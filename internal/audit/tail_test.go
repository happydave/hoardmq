package audit

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func targets(events []Event) string {
	var out []string
	for _, e := range events {
		out = append(out, e.Target)
	}
	return strings.Join(out, ",")
}

// Tail returns the newest events oldest first, crosses into rotated files
// when the active one holds fewer, and skips a line the writer has not
// finished.
func TestTail(t *testing.T) {
	dir := t.TempDir()
	now := time.Unix(1_700_000_000, 0)
	var mu sync.Mutex
	l, err := Open(dir, Options{MaxBytes: 400, MaxFiles: 10, Now: func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		now = now.Add(time.Second)
		return now
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	for i := 1; i <= 12; i++ {
		if err := l.Record(Event{Actor: "admin", Action: "test", Target: fmt.Sprint("e", i), Outcome: "ok"}); err != nil {
			t.Fatal(err)
		}
		if err := l.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	rotated, _ := filepath.Glob(filepath.Join(dir, "audit-*.log"))
	if len(rotated) < 2 {
		t.Fatalf("the fixture must rotate at least twice to exercise crossing files; rotated %d", len(rotated))
	}
	active := readEvents(t, dir)
	if len(active) >= 12 {
		t.Fatalf("the active file holds every event; nothing crosses a rotation")
	}
	if got := targets(mustTail(t, l, 3)); got != "e10,e11,e12" {
		t.Fatalf("tail 3 = %s", got)
	}
	if got := targets(mustTail(t, l, 12)); got != "e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12" {
		t.Fatalf("tail 12 = %s", got)
	}
	if got := len(mustTail(t, l, 50)); got != 12 {
		t.Fatalf("tail 50 = %d events", got)
	}
	// A torn last line is not returned; the complete ones before it are.
	f, err := os.OpenFile(filepath.Join(dir, activeName), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"time":"2026-09-25T00:00:00Z","actor":"torn","act`); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	got := mustTail(t, l, 2)
	if targets(got) != "e11,e12" {
		t.Fatalf("tail with a torn line = %s", targets(got))
	}
	if mustTail(t, l, 0) != nil {
		t.Fatal("tail 0 returned events")
	}
}

// A line that does not decode, followed by complete ones (a write a crash cut
// short, then the writer carried on after the restart), costs that line only:
// the tail still takes the events before it from the same file.
func TestTailSkipsAnUndecodableLine(t *testing.T) {
	dir := t.TempDir()
	var b strings.Builder
	for i := 1; i <= 5; i++ {
		fmt.Fprintf(&b, `{"time":"2026-09-25T00:00:0%dZ","actor":"admin","action":"test","target":"e%d","outcome":"ok"}`+"\n", i, i)
	}
	b.WriteString(`{"time":"2026-09-25T00:00:06Z","actor":"adm` + "\n")
	for i := 6; i <= 7; i++ {
		fmt.Fprintf(&b, `{"time":"2026-09-25T00:00:0%dZ","actor":"admin","action":"test","target":"e%d","outcome":"ok"}`+"\n", i, i)
	}
	if err := os.WriteFile(filepath.Join(dir, activeName), []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	l, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	if got := targets(mustTail(t, l, 3)); got != "e5,e6,e7" {
		t.Fatalf("tail across an undecodable line = %s", got)
	}
	if got := targets(mustTail(t, l, 10)); got != "e1,e2,e3,e4,e5,e6,e7" {
		t.Fatalf("tail of the whole file = %s", got)
	}
}

func mustTail(t *testing.T, l *Log, n int) []Event {
	t.Helper()
	got, err := l.Tail(n)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// lastLines reads across its chunk boundary: lines longer than a chunk and a
// count that needs several chunks.
func TestLastLinesAcrossChunks(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "f")
	var b strings.Builder
	for i := 0; i < 5; i++ {
		fmt.Fprintf(&b, "%d%s\n", i, strings.Repeat("x", 40<<10))
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	lines, _, err := lastLines(path, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != 3 || lines[0][0] != '2' || lines[2][0] != '4' || len(lines[1]) != 1+40<<10 {
		t.Fatalf("got %d lines", len(lines))
	}
}
