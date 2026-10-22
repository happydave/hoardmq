package etcdraft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// A stored snapshot's data is one of two forms behind the same magic: format
// 1, the machine document inline, which older builds wrote; or format 2, a
// descriptor naming a snapshot file in the snapshot directory. An older
// build meeting format 2 refuses it by number.

// snapDesc names a snapshot file: its position, size and digest.
type snapDesc struct {
	Index  uint64
	Term   uint64
	Size   uint64
	Digest [32]byte
	Name   string
}

func (d snapDesc) encode() []byte {
	out := append([]byte(nil), consensus.SnapshotMagic...)
	out = append(out, consensus.FileFormat)
	out = binary.BigEndian.AppendUint64(out, d.Index)
	out = binary.BigEndian.AppendUint64(out, d.Term)
	out = binary.BigEndian.AppendUint64(out, d.Size)
	out = append(out, d.Digest[:]...)
	out = binary.BigEndian.AppendUint16(out, uint16(len(d.Name)))
	return append(out, d.Name...)
}

// descHeader is the fixed part of an encoded descriptor before its name.
const descHeader = 4 + 1 + 24 + 32 + 2

// decodeDesc reads a descriptor and returns what follows it.
func decodeDesc(data []byte) (snapDesc, []byte, error) {
	var d snapDesc
	if len(data) < descHeader || !bytes.Equal(data[:4], consensus.SnapshotMagic) || data[4] != consensus.FileFormat {
		return d, nil, errors.New("snapshot: not a snapshot descriptor")
	}
	d.Index = binary.BigEndian.Uint64(data[5:13])
	d.Term = binary.BigEndian.Uint64(data[13:21])
	d.Size = binary.BigEndian.Uint64(data[21:29])
	copy(d.Digest[:], data[29:61])
	n := int(binary.BigEndian.Uint16(data[61:63]))
	if len(data) < descHeader+n {
		return d, nil, errors.New("snapshot: descriptor name truncated")
	}
	d.Name = string(data[descHeader : descHeader+n])
	if d.Name == "" || d.Name != filepath.Base(d.Name) || strings.HasPrefix(d.Name, ".") {
		return d, nil, fmt.Errorf("snapshot: descriptor names %q", d.Name)
	}
	return d, data[descHeader+n:], nil
}

// snapshotForm reports which form a stored snapshot's data is in.
func snapshotForm(data []byte) byte {
	if len(data) < 5 || !bytes.Equal(data[:4], consensus.SnapshotMagic) {
		return 0
	}
	return data[4]
}

// File names in the snapshot directory. A built file is named by its index
// and term; a received one also by when it arrived, so it never shares a name
// with one this node built at the same position.
const (
	snapExt   = ".snap"
	buildPfx  = "build-"
	recvPfx   = "recv-"
	partialEx = ".part"
)

func builtName(index, term uint64) string {
	return fmt.Sprintf("%020d-%020d%s", index, term, snapExt)
}

func receivedName(index, term uint64, at time.Time) string {
	return fmt.Sprintf("%020d-%020d-r%d%s", index, term, at.UnixNano(), snapExt)
}

// snapshotFileRemovable decides whether a file in the snapshot directory may
// be deleted: never the file the stored snapshot names, never one a transfer
// reads or a build or stage owns; at start every other file goes, partial
// ones included, and while running only finished snapshot files go.
func snapshotFileRemovable(name, current string, inUse map[string]bool, starting bool) bool {
	if name == "" || name == current || inUse[name] {
		return false
	}
	return starting || strings.HasSuffix(name, snapExt)
}

// syncDir makes a rename or a removal in dir durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}

// storedSnapshotName is the file the stored snapshot names, or "".
func (c *Core) storedSnapshotName() string {
	snap, err := c.o.Store.Snapshot()
	if err != nil || raft.IsEmptySnap(snap) || snapshotForm(snap.Data) != consensus.FileFormat {
		return ""
	}
	d, _, err := decodeDesc(snap.Data)
	if err != nil {
		return ""
	}
	return d.Name
}

// removeSnapshotFiles deletes the files the predicate allows.
func (c *Core) removeSnapshotFiles(starting bool) {
	entries, err := os.ReadDir(c.snapDir)
	if err != nil {
		return
	}
	current := c.storedSnapshotName()
	inUse := c.snapshotFilesInUse()
	removed := false
	for _, e := range entries {
		if e.IsDir() || !snapshotFileRemovable(e.Name(), current, inUse, starting) {
			continue
		}
		if err := os.Remove(filepath.Join(c.snapDir, e.Name())); err == nil {
			removed = true
		}
	}
	if removed {
		_ = syncDir(c.snapDir)
	}
}

// snapshotFilesInUse is every file a build, a transfer or a stage holds.
func (c *Core) snapshotFilesInUse() map[string]bool {
	c.filesMu.Lock()
	defer c.filesMu.Unlock()
	out := make(map[string]bool, len(c.filesInUse))
	for name, n := range c.filesInUse {
		if n > 0 {
			out[name] = true
		}
	}
	return out
}

// pinFile and unpinFile count the holders of a snapshot file.
func (c *Core) pinFile(name string) {
	c.filesMu.Lock()
	c.filesInUse[name]++
	c.filesMu.Unlock()
}

func (c *Core) unpinFile(name string) {
	c.filesMu.Lock()
	if c.filesInUse[name]--; c.filesInUse[name] <= 0 {
		delete(c.filesInUse, name)
	}
	c.filesMu.Unlock()
}

// buildResult is a build's outcome, handed back to the loop.
type buildResult struct {
	desc    snapDesc
	cs      *pb.ConfState
	err     error
	took    time.Duration
	capture time.Duration
}

// startBuild captures the machine at its applied index and writes the
// snapshot file off the loop. Called on the loop.
func (c *Core) startBuild(applied uint64) error {
	began := time.Now()
	capture, err := c.machine.Capture()
	if err != nil {
		return err
	}
	c.mu.Lock()
	cs := c.confState
	c.mu.Unlock()
	if cs == nil {
		cs = &pb.ConfState{}
	}
	cs = proto.Clone(cs).(*pb.ConfState)
	term, err := c.o.Store.Term(applied)
	if err != nil {
		return fmt.Errorf("snapshot term at %d: %w", applied, err)
	}
	csb, err := proto.Marshal(cs)
	if err != nil {
		return err
	}
	captured := time.Since(began)
	c.set("hoardmq_raft_snapshot_capture_seconds", nil, captured.Seconds())
	c.building = true
	name := builtName(applied, term)
	c.pinFile(name)
	gate := c.o.BuildGate
	c.bg.Add(1)
	go func() {
		defer c.bg.Done()
		if gate != nil {
			gate()
		}
		res := buildResult{cs: cs, capture: captured}
		started := time.Now()
		res.desc, res.err = c.writeSnapshotFile(capture, consensus.FileHeader{Index: applied, Term: term, Applied: capture.Applied(), ConfState: csb}, name)
		res.took = time.Since(started)
		select {
		case c.builds <- res:
		case <-c.stop:
			c.unpinFile(name)
			_ = os.Remove(filepath.Join(c.snapDir, name))
		}
	}()
	return nil
}

// errStopped ends a build or a transfer when the core stops.
var errStopped = errors.New("consensus: core stopped")

// writeSnapshotFile writes a capture to a temporary file, makes it durable
// and renames it into place.
func (c *Core) writeSnapshotFile(capture *consensus.Capture, hd consensus.FileHeader, name string) (snapDesc, error) {
	d := snapDesc{Index: hd.Index, Term: hd.Term, Name: name}
	if err := os.MkdirAll(c.snapDir, 0o755); err != nil {
		return d, err
	}
	tmp := filepath.Join(c.snapDir, buildPfx+name+".tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return d, err
	}
	fail := func(err error) (snapDesc, error) {
		_ = f.Close()
		_ = os.Remove(tmp)
		return d, err
	}
	fw, err := consensus.NewFileWriter(&stopWriter{w: f, stop: c.stop}, hd)
	if err != nil {
		return fail(err)
	}
	if err := capture.Write(fw); err != nil {
		return fail(err)
	}
	if d.Size, d.Digest, err = fw.Close(); err != nil {
		return fail(err)
	}
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return d, err
	}
	if err := os.Rename(tmp, filepath.Join(c.snapDir, name)); err != nil {
		_ = os.Remove(tmp)
		return d, err
	}
	return d, syncDir(c.snapDir)
}

// stopWriter fails a write once the core has stopped, so a stop does not
// wait out a large file.
type stopWriter struct {
	w    io.Writer
	stop chan struct{}
}

func (s *stopWriter) Write(p []byte) (int, error) {
	select {
	case <-s.stop:
		return 0, errStopped
	default:
	}
	return s.w.Write(p)
}

// finishBuild records a build's file as the stored snapshot and compacts the
// log behind it. Called on the loop.
func (c *Core) finishBuild(res buildResult) error {
	c.building = false
	defer c.unpinFile(res.desc.Name)
	if res.err != nil {
		c.count("hoardmq_raft_snapshot_builds_total", map[string]string{"outcome": "failed"}, 1)
		c.log.Warn("snapshot build failed", "index", res.desc.Index, "err", res.err)
		return nil
	}
	c.count("hoardmq_raft_snapshot_build_seconds_total", nil, res.took.Seconds())
	if _, err := c.o.Store.CreateSnapshot(res.desc.Index, res.cs, res.desc.encode()); err != nil {
		if errors.Is(err, raft.ErrSnapOutOfDate) {
			c.count("hoardmq_raft_snapshot_builds_total", map[string]string{"outcome": "superseded"}, 1)
			c.log.Info("snapshot build superseded by a newer snapshot", "index", res.desc.Index)
			_ = os.Remove(filepath.Join(c.snapDir, res.desc.Name))
			return nil
		}
		return fmt.Errorf("snapshot: %w", err)
	}
	c.count("hoardmq_raft_snapshot_builds_total", map[string]string{"outcome": "ok"}, 1)
	c.count("hoardmq_raft_snapshots_total", map[string]string{"direction": "out"}, 1)
	c.set("hoardmq_raft_snapshot_bytes", nil, float64(res.desc.Size))
	c.log.Debug("snapshot built", "index", res.desc.Index, "bytes", res.desc.Size, "capture", res.capture, "build", res.took)
	c.removeSnapshotFiles(false)
	return c.compact()
}

// compactTarget is the index the log may be compacted to: the trailing
// window behind the stored snapshot, lowered to the lowest snapshot a
// transfer in flight still sends, so a follower that installs it can catch
// up from the entries after it. Zero means nothing to compact.
func compactTarget(stored, trailing uint64, floors []uint64) uint64 {
	if stored <= trailing {
		return 0
	}
	target := stored - trailing
	for _, f := range floors {
		if f < target {
			target = f
		}
	}
	return target
}

// compact drops the log behind the target. Called on the loop.
func (c *Core) compact() error {
	target := compactTarget(c.o.Store.SnapshotIndex(), c.o.SnapshotTrailing, c.floors())
	c.set("hoardmq_raft_log_pinned_entries", nil, float64(c.pinnedEntries()))
	if target == 0 {
		return nil
	}
	if err := c.o.Store.Compact(target); err != nil && !errors.Is(err, raft.ErrCompacted) {
		return fmt.Errorf("compact: %w", err)
	}
	return nil
}

// stageSnapshot decodes the file a descriptor names into new machine state,
// checking it against the descriptor.
func (c *Core) stageSnapshot(d snapDesc) (*consensus.StagedMachine, error) {
	if h := c.o.transferHooks; h != nil && h.stageDelay != nil {
		h.stageDelay()
	}
	path := filepath.Join(c.snapDir, d.Name)
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("snapshot file %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	if st, err := f.Stat(); err != nil {
		return nil, err
	} else if uint64(st.Size()) != d.Size {
		return nil, fmt.Errorf("snapshot file %s: %d bytes, the descriptor says %d", path, st.Size(), d.Size)
	}
	fr, hd, err := consensus.NewFileReader(f)
	if err != nil {
		return nil, fmt.Errorf("snapshot file %s: %w", path, err)
	}
	if hd.Index != d.Index || hd.Term != d.Term {
		return nil, fmt.Errorf("snapshot file %s: at %d/%d, the descriptor says %d/%d", path, hd.Index, hd.Term, d.Index, d.Term)
	}
	st, err := c.machine.Stage(fr, hd)
	if err != nil {
		return nil, fmt.Errorf("snapshot file %s: %w", path, err)
	}
	if got, ok := fr.Digest(); !ok || got != d.Digest {
		return nil, fmt.Errorf("snapshot file %s: digest does not match the descriptor", path)
	}
	return st, nil
}

// holdReceived pins a received file until the loop has installed it or
// passed its index, so removal cannot take it while its message waits for
// the loop. A newer received file releases the older one.
func (c *Core) holdReceived(d snapDesc) {
	c.filesMu.Lock()
	prev := c.received
	c.received = d
	c.filesInUse[d.Name]++
	if prev.Name != "" {
		if c.filesInUse[prev.Name]--; c.filesInUse[prev.Name] <= 0 {
			delete(c.filesInUse, prev.Name)
		}
	}
	c.filesMu.Unlock()
}

// releaseReceived drops the pin on a received file once the applied index
// has reached it: it is installed, and named by the stored snapshot, or the
// log already held what it carries.
func (c *Core) releaseReceived(applied uint64) {
	c.filesMu.Lock()
	d := c.received
	if d.Name == "" || d.Index > applied {
		c.filesMu.Unlock()
		return
	}
	c.received = snapDesc{}
	if c.filesInUse[d.Name]--; c.filesInUse[d.Name] <= 0 {
		delete(c.filesInUse, d.Name)
	}
	c.filesMu.Unlock()
	c.removeSnapshotFiles(false)
}
