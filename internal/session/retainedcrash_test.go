package session

import (
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/happydave/hoardmq/internal/store"
	"github.com/happydave/hoardmq/internal/store/faultdisk"
)

// TestRetainedInstallWhoseSyncFailedAndACrash: a retained install whose
// journal fsync failed is held in memory; a power cut before the journal is
// made whole again leaves the disk without it, and one after leaves it there.
func TestRetainedInstallWhoseSyncFailedAndACrash(t *testing.T) {
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, c := range []struct {
		name         string
		fail, repair bool
		want         bool
	}{
		{"control: synced", false, false, true},
		{"sync failed, power cut before the next sync", true, false, false},
		{"sync failed, a later sync, then the power cut", true, true, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			d := faultdisk.New()
			st, err := store.Open(dir, store.Options{Logger: quiet, Sync: d.Sync})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			m, _ := newManager(t, func(o *Options) { o.Journal = st })
			if c.fail {
				d.FailNext(1)
			}
			if durable, _ := m.InjectRetained(retained("a")); durable == c.fail {
				t.Fatalf("precondition: the install reported durable=%v", durable)
			}
			inMemory := len(m.router.RetainedFor("default", "r", m.clock.Now())) == 1
			if c.repair {
				if err := st.Sync(); err != nil {
					t.Fatalf("the later sync: %v", err)
				}
			}
			cut := t.TempDir()
			d.CrashCopy(t, filepath.Join(dir, "state"), filepath.Join(cut, "state"))
			after, err := store.Open(cut, store.Options{Logger: quiet})
			if err != nil {
				t.Fatalf("reopen after the power cut: %v", err)
			}
			t.Cleanup(func() { _ = after.Close() })
			state, err := after.Recover()
			if err != nil {
				t.Fatalf("recover after the power cut: %v", err)
			}
			found := false
			for _, r := range state.Retained {
				if r.Topic == "r" && string(r.Payload) == "a" {
					found = true
				}
			}
			t.Logf("in memory before the cut: %v; on the disk after it: %v; journal %+v", inMemory, found, st.JournalStats())
			if found != c.want {
				t.Fatalf("on the disk after the power cut: %v, want %v", found, c.want)
			}
		})
	}
}
