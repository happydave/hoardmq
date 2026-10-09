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

// Options configure a log.
type Options struct {
	// SegmentBytes is the size at which the active segment rolls.
	SegmentBytes int64
	// Sync is the durability call; nil means (*os.File).Sync. Tests inject a
	// barrier or a counter.
	Sync func(*os.File) error
	// Logger receives recovery notes.
	Logger *slog.Logger
	// IgnoreChecksum is the simulation harness's injected defect: a record
	// whose checksum fails is read as if it were sound. Never set from
	// configuration.
	IgnoreChecksum bool
}

// CorruptError reports a record whose checksum failed. The node must not
// start over it.
type CorruptError struct {
	Log     string
	Segment string
	Offset  int64
	Index   uint64
}

func (e *CorruptError) Error() string {
	return fmt.Sprintf("wal %s: checksum mismatch in %s at offset %d (record %d)", e.Log, e.Segment, e.Offset, e.Index)
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
	if err := l.scan(); err != nil {
		return nil, err
	}
	if len(l.segments) == 0 {
		if err := l.roll(1); err != nil {
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
// segment, repairing a torn tail on the last one.
func (l *Log) scan() error {
	entries, err := os.ReadDir(l.dir)
	if err != nil {
		return fmt.Errorf("wal %s: %w", l.name, err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), segmentExt) {
			continue
		}
		first, err := strconv.ParseUint(strings.TrimSuffix(e.Name(), segmentExt), 10, 64)
		if err != nil {
			return fmt.Errorf("wal %s: unexpected file %s", l.name, e.Name())
		}
		l.segments = append(l.segments, &segment{path: filepath.Join(l.dir, e.Name()), first: first})
	}
	sort.Slice(l.segments, func(i, j int) bool { return l.segments[i].first < l.segments[j].first })
	for i, s := range l.segments {
		isLast := i == len(l.segments)-1
		last, size, err := l.verify(s, isLast)
		if err != nil {
			return err
		}
		s.last, s.size = last, size
		if last != 0 {
			l.last = last
		} else if s.first > 1 || i > 0 {
			l.last = s.first - 1
		}
	}
	return nil
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
	return index - 1, offset, nil
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

func writeHeader(f *os.File, first uint64) error {
	var h [headerSize]byte
	copy(h[:8], magic)
	binary.BigEndian.PutUint16(h[8:10], formatVersion)
	binary.BigEndian.PutUint64(h[16:24], first)
	binary.BigEndian.PutUint64(h[24:32], uint64(time.Now().Unix()))
	_, err := f.Write(h[:])
	return err
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
	if l.segments[len(l.segments)-1].last == 0 {
		return nil
	}
	if err := l.w.Flush(); err != nil {
		return err
	}
	return l.switchTo(l.last + 1)
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
	if err := writeHeader(f, first); err != nil {
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
	l.w = bufio.NewWriterSize(f, 64<<10)
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
	l.w = bufio.NewWriterSize(f, 64<<10)
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
		return 0, err
	}
	if _, err := l.w.Write(payload); err != nil {
		return 0, err
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
		return nil, 0, err
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
		return err
	}
	l.syncMu.Lock()
	defer l.syncMu.Unlock()
	return l.opts.Sync(l.active)
}

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
	if l.w != nil {
		if err := l.w.Flush(); err != nil {
			return err
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
	if _, err := f.Write(sum[:]); err != nil {
		_ = f.Close()
		return err
	}
	if _, err := f.Write(payload); err != nil {
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
	entries, err := os.ReadDir(l.dir)
	if err != nil {
		return 0, nil, false
	}
	var names []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), checkpointExt) {
			names = append(names, e.Name())
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(names)))
	for _, name := range names {
		idx, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(name, "checkpoint-"), checkpointExt), 10, 64)
		if err != nil {
			continue
		}
		data, err := os.ReadFile(filepath.Join(l.dir, name))
		if err != nil || len(data) < 4 {
			l.log.Warn("skipping unreadable checkpoint", "file", name)
			continue
		}
		if crc32.Checksum(data[4:], castagnoli) != binary.BigEndian.Uint32(data[:4]) {
			l.log.Warn("skipping checkpoint with a bad checksum", "file", name)
			continue
		}
		return idx, data[4:], true
	}
	return 0, nil, false
}
