package dispatch

import (
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/session"
	"github.com/happydave/hoardmq/internal/store"
	"github.com/happydave/hoardmq/internal/store/faultdisk"
)

// appendFailures counts the session layer's failed appends.
type appendFailures struct {
	mu sync.Mutex
	n  float64
}

func (a *appendFailures) Add(name string, labels map[string]string, delta float64) {
	if name == "hoardmq_journal_failures_total" && labels["op"] == "append" {
		a.mu.Lock()
		a.n += delta
		a.mu.Unlock()
	}
}
func (a *appendFailures) Set(string, map[string]string, float64) {}
func (a *appendFailures) count() float64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.n
}

// newJournaledHarness is the harness over a store whose segments hold one
// record each (a 64-byte segment past a 32-byte header), so the first fsync
// after a record is asked for is its roll's: a disk told to fail its next
// call fails that record's append.
func newJournaledHarness(t *testing.T) (*harness, *faultdisk.Disk, *appendFailures) {
	t.Helper()
	d := faultdisk.New()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	st, err := store.Open(t.TempDir(), store.Options{Logger: quiet, Sync: d.Sync, SegmentBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	failures := &appendFailures{}
	r := New(RoundRobin)
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	m := session.New(session.Options{Router: r, Authorizers: auth.AuthorizerChain{allow{}}, Logger: quiet, Clock: clock, MaxQueued: 100, Journal: st, Metrics: failures})
	return &harness{t: t, m: m, r: r, clock: clock, conns: map[string]*fakeConn{}}, d, failures
}

// TestInjectGroupReportsAFailedAppend: a shared-group message a dispatch
// owner gave this node, at QoS 1, whose member's record fails, is reported
// so the forward is refused, and the member's copy is withdrawn.
func TestInjectGroupReportsAFailedAppend(t *testing.T) {
	h, d, failures := newJournaledHarness(t)
	y := h.connect("y", false, 3600)
	h.subscribe(y, 0, sub("$share/g/t", 1))
	h.m.Disconnect(y, false, 0, false)
	d.FailNext(1)
	scope, ok, err := h.m.InjectGroup(&message.Message{Namespace: "default", Topic: "t", QoS: 1, Payload: []byte("g")}, "g/t")
	if !ok {
		t.Fatal("precondition: the group had no local member")
	}
	if failures.count() != 1 {
		t.Fatalf("precondition: %v failed appends, want the member's record", failures.count())
	}
	if err == nil {
		t.Fatalf("a group delivery whose record failed reported no error (%v)", scope)
	}
	if rec, _ := h.m.Record("y"); rec.Queued != 0 {
		t.Fatalf("the member's copy whose record failed is still queued (%d)", rec.Queued)
	}
}

// TestRedistributionKeepsACopyWhoseRecordFailed: a departed member's QoS 1
// message offered to another member is accepted work nobody retries; its
// copy stays when its record fails.
func TestRedistributionKeepsACopyWhoseRecordFailed(t *testing.T) {
	h, d, failures := newJournaledHarness(t)
	pub := h.connect("pub", true, 0)
	x := h.connect("x", true, 0) // not persistent: its departure journals nothing
	y := h.connect("y", false, 3600)
	h.subscribe(x, 0, sub("$share/g/t", 1))
	h.subscribe(y, 0, sub("$share/g/t", 1))
	h.m.Disconnect(y, false, 0, false)
	x.quota = 0 // the message queues on x
	h.publish(pub, "t", 1, "r", false)
	if rec, _ := h.m.Record("x"); rec.Queued != 1 {
		t.Fatalf("precondition: x queued %d, want the message", rec.Queued)
	}
	d.FailNext(1)
	h.m.Disconnect(x, false, 0, false)
	if failures.count() != 1 {
		t.Fatalf("precondition: %v failed appends, want y's redistributed record", failures.count())
	}
	if rec, _ := h.m.Record("y"); rec.Queued != 1 {
		t.Fatalf("the redistributed copy whose record failed was withdrawn: y queued %d", rec.Queued)
	}
}
