package replication

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// durability records the durability calls made while it is installed: each
// file synced, then each directory, with what the target held at the time.
type durability struct {
	calls  []string
	target string
	fail   string // "file" or "dir": that call fails
}

func installDurability(t *testing.T, d *durability) {
	t.Helper()
	savedFile, savedDir := syncFile, syncDir
	t.Cleanup(func() { syncFile, syncDir = savedFile, savedDir })
	syncFile = func(f *os.File) error {
		held, _ := os.ReadFile(d.target)
		d.calls = append(d.calls, "file "+filepath.Base(f.Name())+" target="+string(held))
		if d.fail == "file" {
			return errors.New("injected fsync failure")
		}
		return f.Sync()
	}
	syncDir = func(dir string) error {
		held, _ := os.ReadFile(d.target)
		d.calls = append(d.calls, "dir target="+string(held))
		if d.fail == "dir" {
			return errors.New("injected fsync failure")
		}
		return savedDir(dir)
	}
}

// requireDurableReplace checks that the temporary file was synced while the
// target still held its old content, and the directory after the rename.
func requireDurableReplace(t *testing.T, d *durability, old, now string) {
	t.Helper()
	if len(d.calls) != 2 || !strings.HasSuffix(strings.Fields(d.calls[0])[1], ".tmp") ||
		d.calls[0] != "file "+strings.Fields(d.calls[0])[1]+" target="+old || d.calls[1] != "dir target="+now {
		t.Fatalf("durability calls %q, want the temporary file synced before the rename (target %q), then the directory (target %q)", d.calls, old, now)
	}
}

// TestReconciledVersionIsWrittenDurably: the reconciled map version reaches
// its file through a synced temporary file, a rename and a synced directory;
// a failed sync leaves the old version in place and unacted on.
func TestReconciledVersionIsWrittenDurably(t *testing.T) {
	dir := t.TempDir()
	r := &Replicator{o: Options{ReplicaDir: dir, Logger: quiet}, log: quiet}
	path := filepath.Join(dir, reconciledFile)
	r.writeReconciled(4)
	d := &durability{target: path}
	installDurability(t, d)
	r.writeReconciled(5)
	requireDurableReplace(t, d, `{"version":4}`, `{"version":5}`)

	for _, fail := range []string{"file", "dir"} {
		d.calls, d.fail = nil, fail
		r.writeReconciled(9)
		if r.persisted != 5 {
			t.Errorf("a %s sync failed and the version in memory moved to %d, want 5", fail, r.persisted)
		}
		if fail == "file" {
			if b, _ := os.ReadFile(path); string(b) != `{"version":5}` {
				t.Errorf("the file sync failed and the file holds %s, want version 5", b)
			}
		}
	}
}

// TestKeptRetainedNotesAreWrittenDurably: the kept retained notes reach their
// file the same way.
func TestKeptRetainedNotesAreWrittenDurably(t *testing.T) {
	dir := t.TempDir()
	r := &Replicator{o: Options{ReplicaDir: dir, Logger: quiet}, log: quiet, rmoves: &retainedMoves{kept: map[int]retainedNote{}}}
	path := filepath.Join(dir, keptRetainedFile)
	r.writeKeptRetainedLocked()
	d := &durability{target: path}
	installDurability(t, d)
	r.rmoves.kept[3] = retainedNote{Releaser: "n2", Version: 7}
	want, err := json.Marshal(map[string]retainedNote{"3": r.rmoves.kept[3]})
	if err != nil {
		t.Fatal(err)
	}
	r.writeKeptRetainedLocked()
	requireDurableReplace(t, d, `{}`, string(want))

	d.calls, d.fail = nil, "file"
	r.rmoves.kept[4] = retainedNote{Releaser: "n3", Version: 8}
	r.writeKeptRetainedLocked()
	if b, _ := os.ReadFile(path); string(b) != string(want) {
		t.Errorf("the file sync failed and the notes file holds %s, want the previous notes %s", b, want)
	}
}
