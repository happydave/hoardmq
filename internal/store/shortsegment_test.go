package store

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestStoreOpensOverASegmentCutShort: a journal whose last segment a crash
// left at 5 bytes opens, recovers every session synced before, and logs the
// removal.
func TestStoreOpensOverASegmentCutShort(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir, Options{})
	for _, id := range []string{"a", "b"} {
		upsert(t, s, id)
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "state", "00000000000000000003.seg"), []byte("HOARD"), 0o640); err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	r, err := Open(dir, Options{Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	if err != nil {
		t.Fatalf("open over a segment cut short: %v", err)
	}
	defer func() { _ = r.Close() }()
	st, err := r.Recover()
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b"} {
		if st.Find("ns", id) == nil {
			t.Errorf("%s not recovered", id)
		}
	}
	out := logs.String()
	if !strings.Contains(out, "00000000000000000003.seg") || !strings.Contains(out, "segments_removed=1") {
		t.Errorf("the open did not log the removal:\n%s", out)
	}
}
