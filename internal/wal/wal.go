// Package wal is the node's log engine: segmented append-only files with a
// length, type and CRC-32C per record, a versioned segment header, fsync on
// demand, segment-drop retention, and checkpoints. One engine serves several
// named logs (the state log now, the Raft log later).
package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	magic         = "HOARDLOG"
	formatVersion = 1
	headerSize    = 32
	recordHeader  = 4 + 1 + 4 // length, type, crc
	segmentExt    = ".seg"
	checkpointExt = ".ckpt"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// maxRecordBytes bounds a record's payload: Append refuses a larger one, so a
// larger length on disk is damage. A message at MQTT's packet limit is about
// 342 MiB as base64 in JSON.
var maxRecordBytes int64 = 1 << 30

// Options configure a log.
type Options struct {
	// SegmentBytes is the size at which the active segment rolls.
	SegmentBytes int64
	// Sync is the durability call; nil means (*os.File).Sync. Tests inject a
	// barrier or a counter.
	Sync func(*os.File) error
	// Write, when set, is every write to a segment or checkpoint file in place
	// of (*os.File).Write. Tests inject a disk that fills.
	Write func(f *os.File, p []byte) (int, error)
	// Logger receives recovery notes.
	Logger *slog.Logger
	// IgnoreChecksum is the simulation harness's injected defect: a record
	// whose checksum fails is read as if it were sound. Never set from
	// configuration.
	IgnoreChecksum bool
}

// CorruptError reports a record whose checksum failed or whose length is
// damaged. The node must not start over it.
type CorruptError struct {
	Log     string
	Segment string
	Offset  int64
	Index   uint64
	// Reason is what is wrong with the record, "" for a checksum mismatch.
	Reason string
}

func (e *CorruptError) Error() string {
	what := e.Reason
	if what == "" {
		what = "checksum mismatch"
	}
	return fmt.Sprintf("wal %s: %s in %s at offset %d (record %d)", e.Log, what, e.Segment, e.Offset, e.Index)
}

// Record is one entry read back from the log.
type Record struct {
	Index   uint64
	Type    byte
	Payload []byte
}

type segment struct {
	path  string
	first uint64
	last  uint64 // 0 while unknown
	size  int64
}

// empty reports whether the segment holds no record: its last index is 0 when
// it was started in this process and first-1 when Open read it.
func (s *segment) empty() bool { return s.last < s.first }

// Log is one named log.
type Log struct {
	name     string
	dir      string
	opts     Options
	log      *slog.Logger
	segments []*segment
	active   *os.File
	w        *bufio.Writer
	activeSz int64
	last     uint64
	closed   bool

	// rollMu guards the active file against an fsync in flight outside the
	// owner's lock (group commit, WI 1292 round 4): Fsync holds it shared
	// while it syncs, roll and Close hold it exclusively while they close or
	// replace the file. gen counts rolls so a late Fsync can tell its file
	// was already synced and closed by roll.
	rollMu sync.RWMutex
	gen    uint64
	// What Open repaired: torn tails truncated, and segments removed that a
	// crash left before their header reached the disk.
	tornTails, removedSegments int
	// writerFailed is set when a write through w fails: the writer keeps the
	// error, so nothing more reaches the active segment until a new segment
	// gets a new writer. writeFailures counts every such failure.
	writerFailed  bool
	writeFailures atomic.Int64
	// syncMu is held across every sync of the active file. Linux reports a
	// write-back error to one fsync of a file, so two that overlap can see one
	// fail and the other succeed over the pages the failure dropped (WI 1812).
	syncMu sync.Mutex
}

// Open creates or opens the log named name under dir. A torn write at the
// end of the last segment is truncated away; a checksum failure anywhere
// fails open.
func Open(dir, name string, opts Options) (*Log, error) {
	if opts.SegmentBytes <= 0 {
		opts.SegmentBytes = 64 << 20
	}
	if opts.Sync == nil {
		opts.Sync = func(f *os.File) error { return f.Sync() }
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	l := &Log{name: name, dir: filepath.Join(dir, name), opts: opts, log: opts.Logger.With("wal", name)}
	if err := os.MkdirAll(l.dir, 0o750); err != nil {
		return nil, fmt.Errorf("wal %s: %w", name, err)
	}
	restart, err := l.scan()
	if err != nil {
		return nil, err
	}
	if len(l.segments) == 0 {
		l.last = restart - 1
		if err := l.roll(restart); err != nil {
			return nil, err
		}
		return l, nil
	}
	if err := l.openActive(); err != nil {
		return nil, err
	}
	return l, nil
}

// scan lists segments, validates headers, and verifies every record of every
// segment, repairing a torn tail on the last one. It returns the index a log
// left with no segment starts at.
func (l *Log) scan() (uint64, error) {
	entries, err := os.ReadDir(l.dir)
	if err != nil {
		return 0, fmt.Errorf("wal %s: %w", l.name, err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), segmentExt) {
			continue
		}
		first, err := strconv.ParseUint(strings.TrimSuffix(e.Name(), segmentExt), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("wal %s: unexpected file %s", l.name, e.Name())
		}
		l.segments = append(l.segments, &segment{path: filepath.Join(l.dir, e.Name()), first: first})
	}
	sort.Slice(l.segments, func(i, j int) bool { return l.segments[i].first < l.segments[j].first })
	restart := uint64(1)
	if n := len(l.segments); n > 0 {
		s := l.segments[n-1]
		removed, err := l.removeTornSegment(s)
		if err != nil {
			return 0, err
		}
		if removed {
			l.segments = l.segments[:n-1]
			restart = s.first
		}
	}
	for i, s := range l.segments {
		isLast := i == len(l.segments)-1
		last, size, err := l.verify(s, isLast)
		if err != nil {
			return 0, err
		}
		s.last, s.size = last, size
		if last != 0 {
			l.last = last
		} else if s.first > 1 || i > 0 {
			l.last = s.first - 1
		}
	}
	return restart, nil
}

// tornSegment reports whether a segment of size bytes, whose first bytes are
// head, is one a crash left while creating it: shorter than its header, or a
// header of zeros with nothing after it. Only the last segment can be one.
func tornSegment(head []byte, size int64) bool {
	if size > headerSize {
		return false
	}
	if size < headerSize {
		return true
	}
	for _, b := range head {
		if b != 0 {
			return false
		}
	}
	return true
}

// removeTornSegment removes the last segment if a crash left it before its
// header reached the disk. It holds no record.
func (l *Log) removeTornSegment(s *segment) (bool, error) {
	f, err := os.Open(s.path)
	if err != nil {
		return false, fmt.Errorf("wal %s: %w", l.name, err)
	}
	head := make([]byte, headerSize)
	n, err := io.ReadFull(f, head)
	_ = f.Close()
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return false, fmt.Errorf("wal %s: %w", l.name, err)
	}
	size := int64(n)
	if n == headerSize {
		info, err := os.Stat(s.path)
		if err != nil {
			return false, fmt.Errorf("wal %s: %w", l.name, err)
		}
		size = info.Size()
	}
	if !tornSegment(head[:n], size) {
		return false, nil
	}
	l.log.Warn("removing a segment cut short before its header", "segment", filepath.Base(s.path), "bytes", size)
	if err := os.Remove(s.path); err != nil {
		return false, fmt.Errorf("wal %s: %w", l.name, err)
	}
	if err := syncDir(l.dir); err != nil {
		return false, fmt.Errorf("wal %s: %w", l.name, err)
	}
	l.removedSegments++
	return true, nil
}

// OpenRepairs is what Open repaired: torn tails truncated, and segments a
// crash left before their header reached the disk, removed.
func (l *Log) OpenRepairs() (tornTails, removedSegments int) {
	return l.tornTails, l.removedSegments
}

// verify walks a segment, returning its last index and byte size. On the
// last segment a short tail is truncated; elsewhere it is corruption.
func (l *Log) verify(s *segment, isLast bool) (uint64, int64, error) {
	f, err := os.OpenFile(s.path, os.O_RDWR, 0)
	if err != nil {
		return 0, 0, fmt.Errorf("wal %s: %w", l.name, err)
	}
	defer func() { _ = f.Close() }()
	first, err := readHeader(f, l.name, filepath.Base(s.path))
	if err != nil {
		return 0, 0, err
	}
	if first != s.first {
		return 0, 0, fmt.Errorf("wal %s: segment %s header says first index %d", l.name, filepath.Base(s.path), first)
	}
	info, err := f.Stat()
	if err != nil {
		return 0, 0, fmt.Errorf("wal %s: %w", l.name, err)
	}
	r := bufio.NewReader(f)
	offset := int64(headerSize)
	index := s.first - 1
	var hdr [recordHeader]byte
	for {
		n, err := io.ReadFull(r, hdr[:])
		if err == io.EOF {
			break
		}
		if err == io.ErrUnexpectedEOF || (err == nil && n < recordHeader) {
			return l.torn(f, s, isLast, offset, index+1, "record header cut short")
		}
		if err != nil {
			return 0, 0, fmt.Errorf("wal %s: %w", l.name, err)
		}
		length := binary.BigEndian.Uint32(hdr[0:4])
		typ := hdr[4]
		want := binary.BigEndian.Uint32(hdr[5:9])
		left := info.Size() - offset - recordHeader
		if int64(length) > maxRecordBytes {
			return 0, 0, &CorruptError{Log: l.name, Segment: filepath.Base(s.path), Offset: offset, Index: index + 1,
				Reason: fmt.Sprintf("record length %d over the maximum", length)}
		}
		if int64(length) > left {
			// A whole record whose length alone is damaged is not a torn
			// write: its checksum still matches the bytes it was written with.
			if isLast && !l.opts.IgnoreChecksum {
				rest := make([]byte, max(left, 0))
				if _, err := io.ReadFull(r, rest); err != nil {
					return 0, 0, fmt.Errorf("wal %s: %w", l.name, err)
				}
				if lengthDamaged(typ, want, rest) {
					return 0, 0, &CorruptError{Log: l.name, Segment: filepath.Base(s.path), Offset: offset, Index: index + 1,
						Reason: fmt.Sprintf("record length %d damaged (past the end of the segment)", length)}
				}
			}
			return l.torn(f, s, isLast, offset, index+1, "record payload cut short")
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(r, payload); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return l.torn(f, s, isLast, offset, index+1, "record payload cut short")
			}
			return 0, 0, fmt.Errorf("wal %s: %w", l.name, err)
		}
		if crc(typ, payload) != want && !l.opts.IgnoreChecksum {
			return 0, 0, &CorruptError{Log: l.name, Segment: filepath.Base(s.path), Offset: offset, Index: index + 1}
		}
		index++
		offset += int64(recordHeader) + int64(length)
	}
	return index, offset, nil
}

func (l *Log) torn(f *os.File, s *segment, isLast bool, offset int64, index uint64, what string) (uint64, int64, error) {
	if !isLast {
		return 0, 0, &CorruptError{Log: l.name, Segment: filepath.Base(s.path), Offset: offset, Index: index}
	}
	l.log.Warn("truncating torn tail", "segment", filepath.Base(s.path), "offset", offset, "reason", what)
	if err := f.Truncate(offset); err != nil {
		return 0, 0, fmt.Errorf("wal %s: truncate: %w", l.name, err)
	}
	if err := f.Sync(); err != nil {
		return 0, 0, err
	}
	l.tornTails++
	return index - 1, offset, nil
}

// lengthDamaged reports whether a record whose length reaches past the end of
// its segment was nonetheless written whole: its checksum, over its type and
// payload, matches a prefix of rest (the bytes after its header), and that
// prefix ends at the end of rest or where a sound record starts. A record cut
// short by a crash matches only by chance.
func lengthDamaged(typ byte, want uint32, rest []byte) bool {
	c := ^crc32.Update(0, castagnoli, []byte{typ})
	for n := 0; ; n++ {
		if ^c == want && (n == len(rest) || soundRecord(rest[n:])) {
			return true
		}
		if n == len(rest) {
			return false
		}
		c = castagnoli[byte(c)^rest[n]] ^ (c >> 8)
	}
}

// soundRecord reports whether b starts with a whole record whose checksum
// matches.
func soundRecord(b []byte) bool {
	if len(b) < recordHeader {
		return false
	}
	length := int64(binary.BigEndian.Uint32(b[0:4]))
	if length > maxRecordBytes || length > int64(len(b)-recordHeader) {
		return false
	}
	return crc(b[4], b[recordHeader:recordHeader+length]) == binary.BigEndian.Uint32(b[5:9])
}

func readHeader(f *os.File, name, seg string) (uint64, error) {
	var h [headerSize]byte
	if _, err := io.ReadFull(f, h[:]); err != nil {
		return 0, fmt.Errorf("wal %s: segment %s: header: %w", name, seg, err)
	}
	if string(h[:8]) != magic {
		return 0, fmt.Errorf("wal %s: segment %s: bad magic", name, seg)
	}
	if v := binary.BigEndian.Uint16(h[8:10]); v != formatVersion {
		return 0, fmt.Errorf("wal %s: segment %s: format version %d not supported", name, seg, v)
	}
	return binary.BigEndian.Uint64(h[16:24]), nil
}

func writeHeader(w io.Writer, first uint64) error {
	var h [headerSize]byte
	copy(h[:8], magic)
	binary.BigEndian.PutUint16(h[8:10], formatVersion)
	binary.BigEndian.PutUint64(h[16:24], first)
	binary.BigEndian.PutUint64(h[24:32], uint64(time.Now().Unix()))
	_, err := w.Write(h[:])
	return err
}

// fileWriter writes to f through Options.Write.
type fileWriter struct {
	f     *os.File
	write func(*os.File, []byte) (int, error)
}

func (w fileWriter) Write(p []byte) (int, error) { return w.write(w.f, p) }

// writer is what writes to f: f itself, or Options.Write when set.
func (l *Log) writer(f *os.File) io.Writer {
	if l.opts.Write == nil {
		return f
	}
	return fileWriter{f: f, write: l.opts.Write}
}

func crc(typ byte, payload []byte) uint32 {
	c := crc32.Update(0, castagnoli, []byte{typ})
	return crc32.Update(c, castagnoli, payload)
}

// roll starts a new segment whose first index is first, after syncing the
// active one.
func (l *Log) roll(first uint64) error {
	if l.active != nil {
		if err := l.flushAndSync(); err != nil {
			return err
		}
	}
	return l.switchTo(first)
}

// StartSegment moves appends to a new segment after the last record without
// syncing the active one; the caller has made its records durable some other
// way, a checkpoint at the last index. It does nothing when the active segment
// holds no records.
func (l *Log) StartSegment() error {
	if l.closed {
		return errors.New("wal: closed")
	}
	if l.segments[len(l.segments)-1].empty() {
		return nil
	}
	// A writer that failed keeps its error; what it holds is in the caller's
	// checkpoint, so it is left behind with its segment.
	if !l.writerFailed {
		_ = l.wrote(l.w.Flush())
	}
	return l.switchTo(l.last + 1)
}

// ContinueAfter makes the next record's index index+1 when the log ends below
// it, by starting a new segment there; the records between are in the
// caller's checkpoint at index. It does nothing when the log already reaches
// index.
func (l *Log) ContinueAfter(index uint64) error {
	if l.closed {
		return errors.New("wal: closed")
	}
	if index <= l.last {
		return nil
	}
	if !l.writerFailed {
		if err := l.w.Flush(); err != nil {
			return l.wrote(err)
		}
	}
	if err := l.switchTo(index + 1); err != nil {
		return err
	}
	l.last = index
	return nil
}

// switchTo makes a new segment whose first index is first the active one. The
// new file is written and synced before the active one is closed, so a failure
// leaves the log appending where it was.
func (l *Log) switchTo(first uint64) error {
	path := filepath.Join(l.dir, fmt.Sprintf("%020d%s", first, segmentExt))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o640)
	if err != nil {
		return fmt.Errorf("wal %s: %w", l.name, err)
	}
	abandon := func(err error) error {
		_ = f.Close()
		_ = os.Remove(path)
		return err
	}
	if err := writeHeader(l.writer(f), first); err != nil {
		return abandon(err)
	}
	if err := l.opts.Sync(f); err != nil {
		return abandon(err)
	}
	if err := syncDir(l.dir); err != nil {
		return abandon(err)
	}
	if l.active != nil {
		// Exclusive: an Fsync in flight on this file finishes first.
		l.rollMu.Lock()
		_ = l.active.Close()
		l.gen++
		l.rollMu.Unlock()
	}
	s := &segment{path: path, first: first, size: headerSize}
	l.segments = append(l.segments, s)
	l.active = f
	l.w = bufio.NewWriterSize(l.writer(f), 64<<10)
	l.writerFailed = false
	l.activeSz = headerSize
	return nil
}

func (l *Log) openActive() error {
	s := l.segments[len(l.segments)-1]
	f, err := os.OpenFile(s.path, os.O_RDWR|os.O_APPEND, 0)
	if err != nil {
		return fmt.Errorf("wal %s: %w", l.name, err)
	}
	l.active = f
	l.w = bufio.NewWriterSize(l.writer(f), 64<<10)
	l.activeSz = s.size
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}

// Append writes one record and returns its index. The record is durable
// only after Sync.
func (l *Log) Append(typ byte, payload []byte) (uint64, error) {
	if l.closed {
		return 0, errors.New("wal: closed")
	}
	if int64(len(payload)) > maxRecordBytes {
		return 0, fmt.Errorf("wal %s: a record of %d bytes is over the maximum of %d", l.name, len(payload), maxRecordBytes)
	}
	if l.activeSz >= l.opts.SegmentBytes {
		if err := l.roll(l.last + 1); err != nil {
			return 0, err
		}
	}
	var hdr [recordHeader]byte
	binary.BigEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	hdr[4] = typ
	binary.BigEndian.PutUint32(hdr[5:9], crc(typ, payload))
	if _, err := l.w.Write(hdr[:]); err != nil {
		return 0, l.wrote(err)
	}
	if _, err := l.w.Write(payload); err != nil {
		return 0, l.wrote(err)
	}
	l.last++
	l.activeSz += int64(recordHeader) + int64(len(payload))
	s := l.segments[len(l.segments)-1]
	s.last, s.size = l.last, l.activeSz
	return l.last, nil
}

// Sync makes every appended record durable: flush, then fsync. Callers
// that batch their fsyncs use Flush and Fsync separately, so the owner's
// lock covers only the flush.
func (l *Log) Sync() error {
	f, gen, err := l.Flush()
	if err != nil {
		return err
	}
	return l.Fsync(f, gen)
}

// Flush writes the buffered records to the active file and returns that
// file with the current roll generation, for a later Fsync. The caller
// holds whatever lock serializes Append.
func (l *Log) Flush() (*os.File, uint64, error) {
	if l.closed {
		return nil, 0, errors.New("wal: closed")
	}
	if err := l.w.Flush(); err != nil {
		return nil, 0, l.wrote(err)
	}
	return l.active, l.gen, nil
}

// Fsync makes the bytes flushed to f durable, without the owner's lock: a
// roll or Close waits for it before closing f, and no other sync of the
// active file overlaps it. If the log moved past gen meanwhile, f was synced
// by roll, or its records are in the checkpoint written before StartSegment,
// so there is nothing left to do.
func (l *Log) Fsync(f *os.File, gen uint64) error {
	l.rollMu.RLock()
	defer l.rollMu.RUnlock()
	if l.closed || gen != l.gen {
		return nil
	}
	l.syncMu.Lock()
	defer l.syncMu.Unlock()
	return l.opts.Sync(f)
}

func (l *Log) flushAndSync() error {
	if err := l.w.Flush(); err != nil {
		return l.wrote(err)
	}
	l.syncMu.Lock()
	defer l.syncMu.Unlock()
	return l.opts.Sync(l.active)
}

// wrote records a failed write through the segment writer and returns err.
func (l *Log) wrote(err error) error {
	if err != nil {
		l.writerFailed = true
		l.writeFailures.Add(1)
	}
	return err
}

// WriterFailed reports whether a write to the active segment has failed since
// it became active. The caller holds whatever lock serializes Append.
func (l *Log) WriterFailed() bool { return l.writerFailed }

// WriteFailures counts the writes to segment files that failed, lock-free.
func (l *Log) WriteFailures() int64 { return l.writeFailures.Load() }

// LastIndex is the index of the newest record, 0 when empty.
func (l *Log) LastIndex() uint64 { return l.last }

// FirstIndex is the index of the oldest record still held, 0 when empty.
func (l *Log) FirstIndex() uint64 {
	if l.last == 0 {
		return 0
	}
	return l.segments[0].first
}

// Iterate calls fn for every record with index >= from, in order. Records
// not yet synced are visible after Flush; Iterate flushes first.
func (l *Log) Iterate(from uint64, fn func(Record) error) error {
	if l.w != nil && !l.writerFailed {
		if err := l.w.Flush(); err != nil {
			return l.wrote(err)
		}
	}
	for _, s := range l.segments {
		if s.last != 0 && s.last < from {
			continue
		}
		if err := l.iterateSegment(s, from, fn); err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) iterateSegment(s *segment, from uint64, fn func(Record) error) error {
	f, err := os.Open(s.path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	if _, err := readHeader(f, l.name, filepath.Base(s.path)); err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil {
		return err
	}
	r := bufio.NewReader(f)
	index := s.first - 1
	offset := int64(headerSize)
	var hdr [recordHeader]byte
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}
			return err
		}
		length := binary.BigEndian.Uint32(hdr[0:4])
		if int64(length) > maxRecordBytes {
			return &CorruptError{Log: l.name, Segment: filepath.Base(s.path), Offset: offset, Index: index + 1,
				Reason: fmt.Sprintf("record length %d over the maximum", length)}
		}
		if int64(length) > info.Size()-offset-recordHeader {
			return nil // a record not yet whole in the file
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil
		}
		if crc(hdr[4], payload) != binary.BigEndian.Uint32(hdr[5:9]) && !l.opts.IgnoreChecksum {
			return &CorruptError{Log: l.name, Segment: filepath.Base(s.path), Offset: offset, Index: index + 1}
		}
		index++
		offset += int64(recordHeader) + int64(length)
		if index >= from {
			if err := fn(Record{Index: index, Type: hdr[4], Payload: payload}); err != nil {
				return err
			}
		}
	}
}

// Truncate removes every whole segment whose records are all below before.
// The active segment is kept.
func (l *Log) Truncate(before uint64) error {
	keep := l.segments[:0]
	for i, s := range l.segments {
		if i < len(l.segments)-1 && s.last != 0 && s.last < before {
			if err := os.Remove(s.path); err != nil && !os.IsNotExist(err) {
				// keep shares the list's array: what was moved down, then
				// this segment and every one after it.
				l.segments = append(keep, l.segments[i:]...)
				return err
			}
			continue
		}
		keep = append(keep, s)
	}
	l.segments = keep
	return syncDir(l.dir)
}

// Segments is the number of segment files, for tests and metrics.
func (l *Log) Segments() int { return len(l.segments) }

// Close syncs and closes the log.
func (l *Log) Close() error {
	if l.closed {
		return nil
	}
	err := l.flushAndSync()
	// Exclusive: an Fsync in flight finishes before the file goes away, and
	// one arriving afterwards sees closed and returns.
	l.rollMu.Lock()
	defer l.rollMu.Unlock()
	l.closed = true
	if err != nil {
		_ = l.active.Close()
		return err
	}
	return l.active.Close()
}

// WriteCheckpoint stores a checkpoint at index, replacing older ones. The
// file holds a CRC-32C then the payload and is renamed into place after an
// fsync.
func (l *Log) WriteCheckpoint(index uint64, payload []byte) error {
	name := fmt.Sprintf("checkpoint-%020d%s", index, checkpointExt)
	tmp := filepath.Join(l.dir, name+".tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o640)
	if err != nil {
		return err
	}
	var sum [4]byte
	binary.BigEndian.PutUint32(sum[:], crc32.Checksum(payload, castagnoli))
	w := l.writer(f)
	if _, err := w.Write(sum[:]); err != nil {
		_ = f.Close()
		return err
	}
	if _, err := w.Write(payload); err != nil {
		_ = f.Close()
		return err
	}
	if err := l.opts.Sync(f); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(l.dir, name)); err != nil {
		return err
	}
	if err := syncDir(l.dir); err != nil {
		return err
	}
	entries, _ := os.ReadDir(l.dir)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), checkpointExt) && e.Name() != name {
			_ = os.Remove(filepath.Join(l.dir, e.Name()))
		}
	}
	return nil
}

// LatestCheckpoint returns the newest checkpoint whose checksum verifies.
func (l *Log) LatestCheckpoint() (uint64, []byte, bool) {
	idx, payload, name, skipped := l.latestCheckpoint()
	l.logSkipped(skipped)
	return idx, payload, name != ""
}

// CheckpointIndex is the index of the newest checkpoint whose checksum
// verifies, read without logging the files passed over.
func (l *Log) CheckpointIndex() (uint64, bool) {
	idx, _, name, _ := l.latestCheckpoint()
	return idx, name != ""
}

func (l *Log) logSkipped(skipped []SkippedCheckpoint) {
	for _, s := range skipped {
		if s.Reason == skippedUnreadable {
			l.log.Warn("skipping unreadable checkpoint", "file", s.File)
		} else {
			l.log.Warn("skipping checkpoint with a bad checksum", "file", s.File)
		}
	}
}

const (
	skippedUnreadable  = "unreadable"
	skippedBadChecksum = "bad checksum"
)

// SkippedCheckpoint is a checkpoint file passed over because it could not be
// read or its checksum failed.
type SkippedCheckpoint struct {
	File   string
	Reason string
}

// latestCheckpoint is LatestCheckpoint with the file's name, "" when none
// verifies, and the files passed over on the way.
func (l *Log) latestCheckpoint() (uint64, []byte, string, []SkippedCheckpoint) {
	entries, err := os.ReadDir(l.dir)
	if err != nil {
		return 0, nil, "", nil
	}
	var names []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), checkpointExt) {
			names = append(names, e.Name())
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(names)))
	var skipped []SkippedCheckpoint
	for _, name := range names {
		idx, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(name, "checkpoint-"), checkpointExt), 10, 64)
		if err != nil {
			continue
		}
		data, err := os.ReadFile(filepath.Join(l.dir, name))
		if err != nil || len(data) < 4 {
			skipped = append(skipped, SkippedCheckpoint{File: name, Reason: skippedUnreadable})
			continue
		}
		if crc32.Checksum(data[4:], castagnoli) != binary.BigEndian.Uint32(data[:4]) {
			skipped = append(skipped, SkippedCheckpoint{File: name, Reason: skippedBadChecksum})
			continue
		}
		return idx, data[4:], name, skipped
	}
	return 0, nil, "", skipped
}

// GapError reports that recovery would need records the log no longer holds:
// records from the replay index on are missing before a segment that is
// still there, dropped behind a checkpoint that is missing or failed to
// verify, or lost between two segments. The node must not start over it.
type GapError struct {
	Log string
	Dir string // the log's directory
	// From is the index replay would start at: one past the checkpoint
	// recovered, or 1 with none.
	From       uint64
	Checkpoint string // the checkpoint recovered, "" with none
	// Missing and Through are the first and last index not held; Segment
	// is the segment that holds the next one.
	Missing, Through uint64
	Segment          string
	Skipped          []SkippedCheckpoint
}

func (e *GapError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "wal %s: records %d to %d are not held, before segment %s in %s", e.Log, e.Missing, e.Through, e.Segment, e.Dir)
	if e.Checkpoint != "" {
		fmt.Fprintf(&b, "; replay starts at %d, after checkpoint %s", e.From, e.Checkpoint)
	} else {
		fmt.Fprintf(&b, "; replay starts at %d, and no checkpoint verifies", e.From)
	}
	for _, s := range e.Skipped {
		fmt.Fprintf(&b, "; skipped %s (%s)", s.File, s.Reason)
	}
	b.WriteString("; refusing to recover without them")
	return b.String()
}

// span is the run of indexes one segment holds: first to end, end being
// first-1 for a segment with no records.
type span struct{ first, end uint64 }

// replayGap returns the first run of indexes at or after from that the log
// does not hold although a later segment is held, and that segment's
// position in spans, which are in order.
func replayGap(from uint64, spans []span) (missing, through uint64, next int, gap bool) {
	expect := from
	for i, s := range spans {
		if s.end < expect {
			continue
		}
		if s.first > expect {
			return expect, s.first - 1, i, true
		}
		expect = s.end + 1
	}
	return 0, 0, 0, false
}

// spans lists what each segment holds.
func (l *Log) spans() []span {
	out := make([]span, len(l.segments))
	for i, s := range l.segments {
		end := s.first - 1
		if s.last != 0 {
			end = s.last
		}
		out[i] = span{first: s.first, end: end}
	}
	return out
}

// RecoveryCheckpoint returns the newest checkpoint that verifies, as
// LatestCheckpoint does, and a *GapError when the log does not hold every
// record from the one replay would start at to its last.
func (l *Log) RecoveryCheckpoint() (uint64, []byte, bool, error) {
	idx, payload, name, skipped := l.latestCheckpoint()
	l.logSkipped(skipped)
	ok := name != ""
	from := uint64(1)
	if ok {
		from = idx + 1
	}
	if missing, through, next, gap := replayGap(from, l.spans()); gap {
		return idx, payload, ok, &GapError{Log: l.name, Dir: l.dir, From: from, Checkpoint: name, Missing: missing,
			Through: through, Segment: filepath.Base(l.segments[next].path), Skipped: skipped}
	}
	return idx, payload, ok, nil
}
