package sim

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestArtifactWriter exercises writeRunArtifact against synthetic evidence
// carrying a violation, so the serialization of every evidence class is
// checked deterministically (a real defect run's violation is
// probabilistic; the integration test below covers the live path).
func TestArtifactWriter(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(artifactEnv, dir)

	now := time.Now()
	led := &Ledger{
		Publications:  []Publication{{Publisher: "pub1", Topic: "a/t", QoS: 1, Seq: 1, AckedAt: now}},
		Deliveries:    []Delivery{{Client: "sub1", Publisher: "pub1", Topic: "a/t", QoS: 1, Seq: 1, Node: "127.0.0.1:1", At: now}},
		Subscriptions: []Subscription{{Client: "sub1", Filter: "a/t", Since: now.Add(-time.Second)}},
		Events: []Event{
			{Client: "sub1", Node: "127.0.0.1:1", Kind: "connack", At: now.Add(-time.Second)},
			{Client: "sub1", Node: "127.0.0.1:1", Kind: "dropped", At: now, Code: 0x88},
			{Client: "<script>alert('x')</script>", Node: "127.0.0.1:1", Kind: "connack", At: now},
		},
	}
	rep := Report{
		Seed:       42,
		Schedule:   []Step{{At: time.Second, Kind: "crash", Node: "n1", Dur: 2 * time.Second}},
		Violations: []Violation{{Property: PropNoAckedLoss, Detail: "sub1: 1 acknowledged message never delivered"}},
		Stats:      map[string]int{"publications": 1, "deliveries": 1},
		Duration:   3 * time.Second,
	}
	e := Evidence{
		Ledger: led,
		Probes: []Probe{{At: now, Node: "n2", Code: 0x88, Err: os.ErrDeadlineExceeded}},
	}
	writeRunArtifact(t, rep, e, now.Add(-2*time.Second), deltaBetween(readRuntime(), readRuntime(), 2))

	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("want one artifact dir, got %d (err=%v)", len(entries), err)
	}
	sub := filepath.Join(dir, entries[0].Name())

	raw, err := os.ReadFile(filepath.Join(sub, "run.json"))
	if err != nil {
		t.Fatal(err)
	}
	var art artifactWithCoverage
	if err := json.Unmarshal(raw, &art); err != nil {
		t.Fatalf("run.json does not parse: %v", err)
	}
	if art.Seed != 42 || len(art.Violations) != 1 || !strings.Contains(art.Violations[0].Detail, "never delivered") {
		t.Fatalf("violation not carried: %+v", art.Violations)
	}
	if len(art.Schedule) != 1 || art.Schedule[0].Kind != "crash" || art.Schedule[0].AtSec != 1 {
		t.Fatalf("schedule not carried: %+v", art.Schedule)
	}
	if art.Probes[0].Err == "" {
		t.Fatal("probe error lost in serialization")
	}
	if !art.Coverage.Exercised["traffic"] || !art.Coverage.Exercised["connections"] || art.Coverage.Exercised["fail-closed"] {
		t.Fatalf("coverage wrong: %+v", art.Coverage.Exercised)
	}
	if art.Ledger == nil || len(art.Ledger.Deliveries) != 1 {
		t.Fatal("ledger not carried")
	}

	page, err := os.ReadFile(filepath.Join(sub, "timeline.html"))
	if err != nil {
		t.Fatal(err)
	}
	html := string(page)
	// Self-contained: no external fetch of any kind, data embedded inline.
	for _, banned := range []string{"http://", "https://", "<script src", "<link "} {
		if strings.Contains(html, banned) {
			t.Fatalf("timeline.html is not self-contained: found %q", banned)
		}
	}
	if !strings.Contains(html, `"seed": 42`) || !strings.Contains(html, "never delivered") {
		t.Fatal("timeline.html does not embed the run data")
	}
	// The hostile client identifier is carried inside a JSON string; the
	// viewer escapes at render time, and the raw payload must not appear as
	// live markup outside the JSON (it is JSON-escaped there or absent).
	if strings.Contains(strings.ReplaceAll(html, `<script>`, ""), "<script>alert") {
		t.Fatal("hostile identifier reached the page as live markup")
	}

	for _, p := range []string{"goroutine-end.pb.gz", "mutex-end.pb.gz", "block-end.pb.gz"} {
		raw, err := os.ReadFile(filepath.Join(sub, p))
		if err != nil {
			t.Fatalf("profile %s: %v", p, err)
		}
		if len(raw) < 2 || raw[0] != 0x1f || raw[1] != 0x8b {
			t.Fatalf("profile %s is not gzip", p)
		}
	}

	// With the env var empty, the writer is a no-op.
	t.Setenv(artifactEnv, "")
	writeRunArtifact(t, rep, e, now, runtimeDelta{})
	entries, _ = os.ReadDir(dir)
	if len(entries) != 1 {
		t.Fatalf("no-env write produced files: %d entries", len(entries))
	}
}

// TestArtifactFromRun covers the live path: a real (fault-free, shortened)
// run with the env var set writes a parseable artifact whose coverage
// reflects real traffic. This is plumbing verification at gate-friendly
// cost — storm-scale behavior is the measurement's business, not this
// test's (a claim carries the task it was measured on).
func TestArtifactFromRun(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(artifactEnv, dir)
	r := Run(t, RunOptions{Seed: 99, NoFaults: true, Window: 3 * time.Second, Settle: 6 * time.Second})
	if len(r.Violations) > 0 {
		t.Fatalf("fault-free baseline violated: %v", r.Violations)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("want one artifact dir, got %d (err=%v)", len(entries), err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, entries[0].Name(), "run.json"))
	if err != nil {
		t.Fatal(err)
	}
	var art artifactWithCoverage
	if err := json.Unmarshal(raw, &art); err != nil {
		t.Fatalf("run.json does not parse: %v", err)
	}
	if !art.Coverage.Exercised["traffic"] || art.Coverage.Deliveries == 0 {
		t.Fatalf("live coverage wrong: %+v", art.Coverage)
	}
	if art.Runtime.End.Goroutines == 0 || art.Runtime.WallSec <= 0 {
		t.Fatalf("runtime delta not captured: %+v", art.Runtime)
	}
	if art.Nodes != 3 || art.TTL == "" {
		t.Fatalf("cluster shape not carried: nodes=%d ttl=%q", art.Nodes, art.TTL)
	}
}

// TestRuntimeSnapshotSeesContention: the mutex-wait counter moves across a
// window of deliberate contention, so the per-run delta is a live signal
// rather than a decorative zero.
func TestRuntimeSnapshotSeesContention(t *testing.T) {
	before := readRuntime()
	var mu sync.Mutex
	var wg sync.WaitGroup
	stop := time.Now().Add(300 * time.Millisecond)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(stop) {
				mu.Lock()
				time.Sleep(time.Millisecond)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	d := deltaBetween(before, readRuntime(), 100)
	if d.MutexWaitSec <= 0 {
		t.Fatalf("mutex wait delta = %v, want > 0 under deliberate contention", d.MutexWaitSec)
	}
	if d.MutexWaitPerOp <= 0 {
		t.Fatalf("per-op figure = %v, want > 0", d.MutexWaitPerOp)
	}
}

// TestProgressLine pins the WI 1285 line format: phase, progress fraction,
// storm counters, and the runtime figures, one line, no locks read.
func TestProgressLine(t *testing.T) {
	got := progressLine("reconnect", 95*time.Second, 3855, 33402, 27105, 12, 51234, 1.75)
	for _, want := range []string{"[sim reconnect]", "1m35s", "3855/33402", "refusals=27105", "fails=12", "goroutines=51234", "mutex-wait=1.75s/s"} {
		if !strings.Contains(got, want) {
			t.Fatalf("progress line %q missing %q", got, want)
		}
	}
	if strings.Contains(got, "\n") {
		t.Fatal("progress line must be one line")
	}
}
