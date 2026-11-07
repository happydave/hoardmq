package etcdraft

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func storedSnap(data []byte) *pb.Snapshot {
	return &pb.Snapshot{Data: data, Metadata: &pb.SnapshotMetadata{Index: proto.Uint64(7), Term: proto.Uint64(2)}}
}

// writeSnapshotFile writes a format 2 snapshot file at index 7, term 2 and
// returns its descriptor.
func writeSnapshotFile(t *testing.T, dir, name string) snapDesc {
	t.Helper()
	f, err := os.Create(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	fw, err := consensus.NewFileWriter(f, consensus.FileHeader{Index: 7, Term: 2})
	if err != nil {
		t.Fatal(err)
	}
	size, digest, err := fw.Close()
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return snapDesc{Index: 7, Term: 2, Size: size, Digest: digest, Name: name}
}

// TestCheckStoredSnapshot: each stored snapshot a start refuses before it
// stages the machine, and those it accepts.
func TestCheckStoredSnapshot(t *testing.T) {
	dir := t.TempDir()
	good := writeSnapshotFile(t, dir, builtName(7, 2))
	wrongIndex := good
	wrongIndex.Index = 8
	short := good
	short.Size++
	missing := good
	missing.Name = builtName(9, 2)
	for _, c := range []struct {
		name string
		snap *pb.Snapshot
		err  string // "" accepts
	}{
		{"empty", &pb.Snapshot{Metadata: &pb.SnapshotMetadata{}}, ""},
		{"inline format 1", storedSnap(wrapSnapshot(7, 2, []byte("machine"))), ""},
		{"a file", storedSnap(good.encode()), ""},
		{"an unknown form", storedSnap([]byte("not a snapshot at all, longer")), "not a HoardMQ snapshot"},
		{"a newer format", storedSnap(append(append([]byte(nil), consensus.SnapshotMagic...), 9, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 2)), "format 9 not supported"},
		{"a descriptor that does not decode", storedSnap(good.encode()[:20]), "not a snapshot descriptor"},
		{"its file missing", storedSnap(missing.encode()), "no such file"},
		{"its size not the descriptor's", storedSnap(short.encode()), "the descriptor says"},
		{"its header at another index", storedSnap(wrongIndex.encode()), "at 7/2, the descriptor says 8/2"},
	} {
		err := CheckStoredSnapshot(dir, c.snap)
		switch {
		case c.err == "" && err != nil:
			t.Errorf("%s: refused: %v", c.name, err)
		case c.err != "" && (err == nil || !strings.Contains(err.Error(), c.err)):
			t.Errorf("%s: %v, want a refusal naming %q", c.name, err, c.err)
		}
	}
}
