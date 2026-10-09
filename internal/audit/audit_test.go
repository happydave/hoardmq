package audit

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// readEvents parses the active file.
func readEvents(t *testing.T, dir string) []Event {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, activeName))
	if err != nil {
		t.Fatal(err)
	}
	var out []Event
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line == "" {
			continue
		}
		var e Event
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("line %q: %v", line, err)
		}
		out = append(out, e)
	}
	return out
}

func TestAppendOnlyAndRotation(t *testing.T) {
	dir := t.TempDir()
	now := time.Unix(1_700_000_000, 0)
	var nowMu sync.Mutex
	l, err := Open(dir, Options{MaxBytes: 300, MaxFiles: 2, Now: func() time.Time {
		nowMu.Lock()
		defer nowMu.Unlock()
		now = now.Add(time.Second)
		return now
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Record(Event{Actor: "c1", Action: "auth.failure", Target: "tcp", Outcome: "Not authorized (0x87)", Detail: map[string]string{"peer": "127.0.0.1:1"}}); err != nil {
		t.Fatal(err)
	}
	_ = l.Flush()
	first, _ := os.ReadFile(filepath.Join(dir, "audit.log"))
	if err := l.Record(Event{Actor: "admin", Action: "module.disable", Target: "auth.file", Outcome: "ok"}); err != nil {
		t.Fatal(err)
	}
	_ = l.Flush()
	second, _ := os.ReadFile(filepath.Join(dir, "audit.log"))
	if !strings.HasPrefix(string(second), string(first)) {
		t.Fatal("earlier line was rewritten")
	}
	events := readEvents(t, dir)
	if len(events) != 2 {
		t.Fatalf("lines %d", len(events))
	}
	if e := events[0]; e.Action != "auth.failure" || e.Detail["peer"] != "127.0.0.1:1" || e.Time.IsZero() {
		t.Fatalf("first event %+v", e)
	}
	// Push past MaxBytes several times: rotated files appear and are pruned.
	for i := 0; i < 12; i++ {
		if err := l.Record(Event{Actor: "admin", Action: "trace.start", Target: strings.Repeat("x", 40), Outcome: "ok"}); err != nil {
			t.Fatal(err)
		}
	}
	_ = l.Flush()
	entries, _ := os.ReadDir(dir)
	var rotated int
	active := false
	for _, en := range entries {
		switch {
		case en.Name() == "audit.log":
			active = true
		case strings.HasPrefix(en.Name(), "audit-"):
			rotated++
		}
	}
	if !active || rotated != 2 {
		t.Fatalf("active=%v rotated=%d", active, rotated)
	}
	if st := l.Stats(); st.Written != 14 || st.Dropped != 0 || st.WriteFailures != 0 || st.Depth != 0 {
		t.Fatalf("stats %+v", st)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := l.Record(Event{Action: "x"}); err != ErrClosed {
		t.Fatalf("record after close: %v", err)
	}
	if err := l.Flush(); err != ErrClosed {
		t.Fatalf("flush after close: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if l.Dir() != dir {
		t.Error("Dir")
	}
	// Reopen appends to the existing active file.
	l2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(filepath.Join(dir, "audit.log"))
	_ = l2.Record(Event{Action: "server.start", Outcome: "ok"})
	_ = l2.Flush()
	after, _ := os.ReadFile(filepath.Join(dir, "audit.log"))
	if !strings.HasPrefix(string(after), string(before)) || len(after) <= len(before) {
		t.Fatal("reopen did not append")
	}
	_ = l2.Close()
}

func TestOpenUnwritable(t *testing.T) {
	file := filepath.Join(t.TempDir(), "notadir")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(filepath.Join(file, "audit"), Options{}); err == nil {
		t.Fatal("opened under a file")
	}
}

// Events queued while the writer is held are committed as one batch: one
// write and one sync for the lot, in order.
func TestGroupCommit(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{Queue: 1000})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	l.Pause()
	for i := 0; i < 500; i++ {
		if err := l.Record(Event{Actor: "admin", Action: "trace.start", Target: strings.Repeat("t", i%7)}); err != nil {
			t.Fatal(err)
		}
	}
	if st := l.Stats(); st.Depth != 500 || st.Written != 0 {
		t.Fatalf("before resume %+v", st)
	}
	l.Resume()
	if err := l.Flush(); err != nil {
		t.Fatal(err)
	}
	events := readEvents(t, dir)
	if len(events) != 500 {
		t.Fatalf("events %d", len(events))
	}
	for i, e := range events {
		if e.Target != strings.Repeat("t", i%7) {
			t.Fatalf("event %d out of order: %+v", i, e)
		}
	}
	st := l.Stats()
	if st.Batches == 0 || st.Batches > 3 || st.Written != 500 || st.Dropped != 0 {
		t.Fatalf("stats %+v", st)
	}
}

// Lossy events beyond the queue are discarded and counted, the call returns
// at once, and the next batch opens with an audit.dropped record whose count
// is the drops since the previous one.
func TestLossyDropsAreCountedAndRecorded(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{Queue: 64})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	l.Pause()
	start := time.Now()
	for i := 0; i < 200; i++ {
		if err := l.RecordLossy(Event{Actor: "peer", Action: "auth.failure", Outcome: "0x86"}); err != nil {
			t.Fatal(err)
		}
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("200 lossy records took %v", d)
	}
	if st := l.Stats(); st.Dropped != 136 || st.Depth != 64 {
		t.Fatalf("before resume %+v", st)
	}
	l.Resume()
	if err := l.Flush(); err != nil {
		t.Fatal(err)
	}
	events := readEvents(t, dir)
	if len(events) != 65 {
		t.Fatalf("events %d", len(events))
	}
	if d := events[0]; d.Actor != "audit" || d.Action != "audit.dropped" || d.Outcome != "136" || d.Detail["count"] != "136" || d.Detail["since"] == "" {
		t.Fatalf("dropped record %+v", d)
	}
	for _, e := range events[1:] {
		if e.Action != "auth.failure" {
			t.Fatalf("unexpected %+v", e)
		}
	}
	// A round with no drops writes no audit.dropped record.
	if err := l.RecordLossy(Event{Actor: "peer", Action: "auth.failure", Outcome: "0x87"}); err != nil {
		t.Fatal(err)
	}
	_ = l.Flush()
	events = readEvents(t, dir)
	if len(events) != 66 || events[65].Action != "auth.failure" {
		t.Fatalf("second round: %d events, last %+v", len(events), events[len(events)-1])
	}
	if st := l.Stats(); st.Dropped != 136 || st.Written != 66 {
		t.Fatalf("stats %+v", st)
	}
}

// A lossless event waits for space rather than dropping, and is written.
func TestLosslessWaitsNeverDrops(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{Queue: 64})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	l.Pause()
	for i := 0; i < 64; i++ {
		_ = l.RecordLossy(Event{Actor: "peer", Action: "auth.failure"})
	}
	done := make(chan error, 1)
	go func() { done <- l.Record(Event{Actor: "admin", Action: "module.disable", Target: "authz.file"}) }()
	select {
	case err := <-done:
		t.Fatalf("lossless record returned %v with the queue full", err)
	case <-time.After(50 * time.Millisecond):
	}
	l.Resume()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("lossless record did not return after resume")
	}
	if err := l.Flush(); err != nil {
		t.Fatal(err)
	}
	events := readEvents(t, dir)
	if len(events) != 65 || events[64].Action != "module.disable" {
		t.Fatalf("events %d, last %+v", len(events), events[len(events)-1])
	}
	if st := l.Stats(); st.Dropped != 0 {
		t.Fatalf("stats %+v", st)
	}
}

// A storm of lossy records from many goroutines completes in bounded time
// with the writer held, and every record is accounted for afterwards.
func TestStormShape(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{Queue: 256})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	l.Pause()
	start := time.Now()
	var wg sync.WaitGroup
	for g := 0; g < 1000; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				_ = l.RecordLossy(Event{Actor: "peer", Action: "auth.failure", Outcome: "0x88"})
			}
		}()
	}
	wg.Wait()
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("storm took %v", d)
	}
	l.Resume()
	if err := l.Flush(); err != nil {
		t.Fatal(err)
	}
	st := l.Stats()
	if st.Dropped+st.Written != 100_000+1 || st.Depth != 0 || st.Written != 257 {
		t.Fatalf("stats %+v", st)
	}
	if got := len(readEvents(t, dir)); got != 257 {
		t.Fatalf("file has %d events", got)
	}
}

// Close drains what was accepted without a Flush.
func TestCloseDrains(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{Queue: 1000})
	if err != nil {
		t.Fatal(err)
	}
	l.Pause()
	for i := 0; i < 100; i++ {
		if err := l.Record(Event{Actor: "server", Action: "server.drain", Outcome: "phase"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if got := len(readEvents(t, dir)); got != 100 {
		t.Fatalf("file has %d events after close", got)
	}
}

// A lossless caller waiting for space when Close runs still has its event
// written: it entered before the close.
func TestCloseWritesWaiter(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{Queue: 64})
	if err != nil {
		t.Fatal(err)
	}
	l.Pause()
	for i := 0; i < 64; i++ {
		_ = l.RecordLossy(Event{Actor: "peer", Action: "auth.failure"})
	}
	done := make(chan error, 1)
	go func() { done <- l.Record(Event{Actor: "admin", Action: "module.disable"}) }()
	time.Sleep(50 * time.Millisecond)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("waiter returned %v", err)
	}
	events := readEvents(t, dir)
	if len(events) != 65 || events[64].Action != "module.disable" {
		t.Fatalf("events %d, last %+v", len(events), events[len(events)-1])
	}
}

// A write failure is counted, logged once, and does not stop later batches.
func TestWriteFailureCounted(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{Queue: 64})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	l.Pause()
	// Close the file behind the writer's back so its next write fails.
	_ = l.f.Close()
	_ = l.Record(Event{Actor: "admin", Action: "trace.start"})
	l.Resume()
	_ = l.Flush()
	if st := l.Stats(); st.WriteFailures != 1 || st.Written != 0 {
		t.Fatalf("stats %+v", st)
	}
	// Reopen the file for it and the next batch succeeds.
	l.Pause()
	if err := l.open(); err != nil {
		t.Fatal(err)
	}
	_ = l.Record(Event{Actor: "admin", Action: "trace.stop"})
	l.Resume()
	_ = l.Flush()
	if st := l.Stats(); st.WriteFailures != 1 || st.Written != 1 {
		t.Fatalf("stats %+v", st)
	}
	if events := readEvents(t, dir); len(events) != 1 || events[0].Action != "trace.stop" {
		t.Fatalf("events %+v", events)
	}
}
