// Package audit is the append-only log of administrative and security
// events: one JSON object per line, written with the file opened for append,
// never rewritten, rotated by size.
//
// Recording an event hands it to one writer goroutine over a bounded queue;
// the writer drains whatever is queued and commits it as one write and one
// sync, so the cost of a sync is shared by every event queued behind it and
// no caller waits on the file. Events come in two classes: Record never drops
// (a caller waits for queue space, which the writer frees every batch), and
// RecordLossy drops when the queue is full and counts the drop. The writer
// prefixes the next batch with an "audit.dropped" record naming the count, so
// the log itself says where it is incomplete.
package audit

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Event is one audited action.
type Event struct {
	Time    time.Time         `json:"time"`
	Actor   string            `json:"actor"`
	Action  string            `json:"action"`
	Target  string            `json:"target,omitempty"`
	Outcome string            `json:"outcome"`
	Detail  map[string]string `json:"detail,omitempty"`
}

// Options configure rotation, the queue and where writer errors go.
type Options struct {
	MaxBytes int64 // rotate when the active file passes this; 0 means 64 MiB
	MaxFiles int   // rotated files to keep; 0 means 10
	Queue    int   // events held for the writer; 0 means 8192
	Now      func() time.Time
	Logger   *slog.Logger // writer errors, once per distinct error; nil means silent
}

// Stats are the log's counters since Open.
type Stats struct {
	Written       uint64 // events written to the file
	Dropped       uint64 // lossy events discarded because the queue was full
	WriteFailures uint64 // batches lost to a write or sync error
	Batches       uint64 // writer wake-ups that wrote something
	Depth         int    // events queued and not yet written
}

// ErrClosed is returned by Record and Flush after Close.
var ErrClosed = errors.New("audit: closed")

// Log is the audit log of one node.
type Log struct {
	dir  string
	opts Options

	mu      sync.Mutex
	space   *sync.Cond // broadcast when the queue drains, a flush completes or the log closes
	queue   [][]byte
	spare   [][]byte // the previous batch's backing array, reused for the next queue
	waiters int      // lossless callers waiting for space
	closed  bool     // no new events; the writer exits once the queue and waiters are gone
	paused  chan struct{}
	seq     uint64 // events accepted
	flushed uint64 // events written or lost to a write failure
	stats   Stats
	seen    uint64 // drops already announced by an audit.dropped record
	last    time.Time
	lastErr string

	wake chan struct{}
	done chan struct{}

	f    *os.File // owned by the writer after Open
	size int64
	cerr error
}

const activeName = "audit.log"

// Open creates the directory if needed, opens the active file for append and
// starts the writer.
func Open(dir string, opts Options) (*Log, error) {
	if opts.MaxBytes <= 0 {
		opts.MaxBytes = 64 << 20
	}
	if opts.MaxFiles <= 0 {
		opts.MaxFiles = 10
	}
	if opts.Queue <= 0 {
		opts.Queue = 8192
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("audit: %w", err)
	}
	l := &Log{dir: dir, opts: opts, wake: make(chan struct{}, 1), done: make(chan struct{})}
	l.space = sync.NewCond(&l.mu)
	l.queue = make([][]byte, 0, opts.Queue)
	if err := l.open(); err != nil {
		return nil, err
	}
	l.last = opts.Now()
	go l.run()
	return l, nil
}

func (l *Log) open() error {
	f, err := os.OpenFile(filepath.Join(l.dir, activeName), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o640)
	if err != nil {
		return fmt.Errorf("audit: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("audit: %w", err)
	}
	l.f, l.size = f, info.Size()
	return nil
}

// Record queues one event and never drops it: when the queue is full the
// call waits for the writer to free space. The time is stamped when absent.
func (l *Log) Record(e Event) error { return l.enqueue(e, false) }

// RecordLossy queues one event if there is room and otherwise discards it,
// counting the drop. It never waits. The time is stamped when absent.
func (l *Log) RecordLossy(e Event) error { return l.enqueue(e, true) }

func (l *Log) enqueue(e Event, lossy bool) error {
	if e.Time.IsZero() {
		e.Time = l.opts.Now()
	}
	line, err := json.Marshal(e)
	if err != nil {
		return err
	}
	line = append(line, '\n')
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return ErrClosed
	}
	if len(l.queue) >= l.opts.Queue {
		if lossy {
			l.stats.Dropped++
			l.mu.Unlock()
			return nil
		}
		l.waiters++
		for len(l.queue) >= l.opts.Queue {
			l.space.Wait()
		}
		l.waiters--
	}
	l.queue = append(l.queue, line)
	l.seq++
	l.mu.Unlock()
	select {
	case l.wake <- struct{}{}:
	default:
	}
	return nil
}

// Flush returns once every event accepted before the call has been written
// and synced (or lost to a write failure that has been counted).
func (l *Log) Flush() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	target := l.seq
	for l.flushed < target && !l.closed {
		l.space.Wait()
	}
	if l.flushed < target {
		return ErrClosed
	}
	return nil
}

// Stats reads the counters.
func (l *Log) Stats() Stats {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := l.stats
	s.Depth = len(l.queue)
	return s
}

// Pause holds the writer before its next batch until Resume is called, so a
// test can fill the queue. Calling it twice is the same as once.
func (l *Log) Pause() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.paused == nil && !l.closed {
		l.paused = make(chan struct{})
	}
}

// Resume releases a paused writer.
func (l *Log) Resume() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.paused != nil {
		close(l.paused)
		l.paused = nil
	}
}

// run is the writer: it wakes on the first queued event, takes everything
// queued and commits it as one batch, then exits after Close once nothing
// is queued and no caller is waiting for space.
func (l *Log) run() {
	defer close(l.done)
	for {
		<-l.wake
		l.mu.Lock()
		if p := l.paused; p != nil {
			l.mu.Unlock()
			<-p
			l.mu.Lock()
		}
		batch := l.queue
		if l.spare == nil {
			l.spare = make([][]byte, 0, l.opts.Queue)
		}
		l.queue = l.spare[:0]
		l.spare = nil
		dropped := l.stats.Dropped - l.seen
		l.seen = l.stats.Dropped
		l.space.Broadcast()
		l.mu.Unlock()

		l.commit(dropped, batch)

		l.mu.Lock()
		l.flushed += uint64(len(batch))
		exit := l.closed && len(l.queue) == 0 && l.waiters == 0
		if exit {
			l.cerr = l.f.Close()
			l.f = nil
		}
		clear(batch)
		l.spare = batch[:0]
		l.space.Broadcast()
		l.mu.Unlock()
		if exit {
			return
		}
	}
}

// commit writes one batch: an audit.dropped record first when events were
// discarded since the last batch, then the lines in order, rotating between
// lines as the size passes MaxBytes, and one sync for the lot.
func (l *Log) commit(dropped uint64, batch [][]byte) {
	if dropped > 0 {
		now := l.opts.Now()
		line, _ := json.Marshal(Event{Time: now, Actor: "audit", Action: "audit.dropped", Outcome: strconv.FormatUint(dropped, 10),
			Detail: map[string]string{"count": strconv.FormatUint(dropped, 10), "since": l.last.UTC().Format(time.RFC3339)}})
		batch = append([][]byte{append(line, '\n')}, batch...)
	}
	if len(batch) == 0 {
		return
	}
	var buf []byte
	fail := func(err error) {
		l.mu.Lock()
		l.stats.WriteFailures++
		l.mu.Unlock()
		if msg := err.Error(); msg != l.lastErr {
			l.lastErr = msg
			if l.opts.Logger != nil {
				l.opts.Logger.Warn("audit write failed", "err", err)
			}
		}
	}
	for _, line := range batch {
		if l.size > 0 && l.size+int64(len(line)) > l.opts.MaxBytes {
			if len(buf) > 0 {
				if _, err := l.f.Write(buf); err != nil {
					fail(err)
					return
				}
				buf = buf[:0]
			}
			if err := l.rotate(); err != nil {
				fail(err)
				return
			}
		}
		buf = append(buf, line...)
		l.size += int64(len(line))
	}
	if _, err := l.f.Write(buf); err != nil {
		fail(err)
		return
	}
	if err := l.f.Sync(); err != nil {
		fail(err)
		return
	}
	l.mu.Lock()
	l.stats.Written += uint64(len(batch))
	l.stats.Batches++
	l.mu.Unlock()
	l.last = l.opts.Now()
}

// rotate renames the active file with a timestamp and prunes old ones.
func (l *Log) rotate() error {
	if err := l.f.Close(); err != nil {
		return err
	}
	name := fmt.Sprintf("audit-%d.log", l.opts.Now().UnixNano())
	if err := os.Rename(filepath.Join(l.dir, activeName), filepath.Join(l.dir, name)); err != nil {
		return fmt.Errorf("audit: rotate: %w", err)
	}
	if err := l.open(); err != nil {
		return err
	}
	entries, err := os.ReadDir(l.dir)
	if err != nil {
		return nil
	}
	var rotated []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "audit-") && strings.HasSuffix(e.Name(), ".log") {
			rotated = append(rotated, e.Name())
		}
	}
	sort.Strings(rotated)
	for len(rotated) > l.opts.MaxFiles {
		_ = os.Remove(filepath.Join(l.dir, rotated[0]))
		rotated = rotated[1:]
	}
	return nil
}

// Close stops accepting events, lets the writer drain what was accepted,
// closes the active file and returns the close error, if any.
func (l *Log) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		<-l.done
		return l.cerr
	}
	l.closed = true
	if l.paused != nil {
		close(l.paused)
		l.paused = nil
	}
	l.space.Broadcast()
	l.mu.Unlock()
	select {
	case l.wake <- struct{}{}:
	default:
	}
	<-l.done
	return l.cerr
}

// Dir is the log directory.
func (l *Log) Dir() string { return l.dir }

// Tail returns up to n of the most recent events on disk, oldest first: the
// active file's, then the newest rotated files' when it holds fewer. A line
// the writer has not finished is skipped, and so is one that does not decode.
// It reads only; call Flush first to include events still queued. A rotation
// while Tail reads may repeat or skip events at the files' boundary.
func (l *Log) Tail(n int) ([]Event, error) {
	if n <= 0 {
		return nil, nil
	}
	files := []string{filepath.Join(l.dir, activeName)}
	entries, err := os.ReadDir(l.dir)
	if err != nil {
		return nil, fmt.Errorf("audit: %w", err)
	}
	var rotated []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "audit-") && strings.HasSuffix(e.Name(), ".log") {
			rotated = append(rotated, e.Name())
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(rotated)))
	for _, name := range rotated {
		files = append(files, filepath.Join(l.dir, name))
	}
	var newestFirst []Event
	for _, f := range files {
		need := n - len(newestFirst)
		if need <= 0 {
			break
		}
		// A line that does not decode (a write a crash cut short, now
		// followed by later ones) is not an event: ask the same file for more
		// until it yields need events or has none left.
		for want := need; ; {
			lines, whole, err := lastLines(f, want)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					break
				}
				return nil, fmt.Errorf("audit: %w", err)
			}
			var got []Event
			for i := len(lines) - 1; i >= 0 && len(got) < need; i-- {
				var e Event
				if json.Unmarshal(lines[i], &e) == nil {
					got = append(got, e)
				}
			}
			if len(got) == need || whole {
				newestFirst = append(newestFirst, got...)
				break
			}
			want += need - len(got)
		}
	}
	out := make([]Event, 0, len(newestFirst))
	for i := len(newestFirst) - 1; i >= 0; i-- {
		out = append(out, newestFirst[i])
	}
	return out, nil
}

// lastLines reads up to n complete lines from the end of a file, oldest
// first, reading backwards in chunks, and reports whether they are all the
// file holds. Bytes after the last newline are a line still being written and
// are not returned.
func lastLines(path string, n int) (lines [][]byte, whole bool, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return nil, false, err
	}
	const chunk = 64 << 10
	end := info.Size()
	var buf []byte
	for end > 0 && strings.Count(string(buf), "\n") <= n {
		start := end - chunk
		if start < 0 {
			start = 0
		}
		part := make([]byte, end-start)
		if _, err := f.ReadAt(part, start); err != nil {
			return nil, false, err
		}
		buf = append(part, buf...)
		end = start
	}
	i := strings.LastIndexByte(string(buf), '\n')
	if i < 0 {
		return nil, true, nil
	}
	buf = buf[:i]
	all := strings.Split(string(buf), "\n")
	whole = end == 0 && len(all) <= n
	// The loop read at least n+1 newlines unless it reached the start, so
	// keeping the last n drops any line the first chunk cut.
	if len(all) > n {
		all = all[len(all)-n:]
	}
	for _, line := range all {
		if line != "" {
			lines = append(lines, []byte(line))
		}
	}
	return lines, whole, nil
}
