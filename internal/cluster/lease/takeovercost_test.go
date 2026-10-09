package lease

import (
	"context"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/store"
)

// TestTakeoverCostSplitsByWhetherAClientWasHere is WI 1495's server half.
//
// A lease move costs the client a reconnect only when the losing node still
// had it connected; when the client had already gone elsewhere the move costs
// nothing. The published batches could not tell those apart, and the ratio
// between them is what separates a fast leader-crash recovery from a slow one.
func TestTakeoverCostSplitsByWhetherAClientWasHere(t *testing.T) {
	c := newCluster(t, []string{"n1", "n2", "n3"}, 5*time.Second, nil)
	a, b := c.nodes["n1"], c.nodes["n2"]

	// take moves id from b to a. When liveAge is zero, b has no connection
	// for it — the client had already left — which is the "gone" case.
	take := func(id string, liveAge time.Duration) {
		t.Helper()
		if _, err := acquire(t, b, id, nil, 300, true); err != nil {
			t.Fatal(err)
		}
		b.sessions.put("default", id, &store.SessionState{SessionMeta: store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 1}})
		if liveAge > 0 {
			b.sessions.setConnAge("default", id, liveAge)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		res, err := a.svc.AcquireFrom(ctx, "default", id, nil, 300, false, Requester{})
		if err != nil || res.PrevOwner != b.name {
			t.Fatalf("takeover of %s: %+v %v", id, res, err)
		}
	}

	take("still-here-1", 3*time.Second)
	take("still-here-2", 9*time.Second)
	take("already-gone-1", 0)
	take("already-gone-2", 0)
	take("already-gone-3", 0)

	// Wait on a signal that predates this work item — all five leases have
	// left b — so the new counters are checked by assertions below rather
	// than by the wait itself. A control that breaks one of them must trip
	// the assertion that targets it, not time out here (Test.md step 3).
	waitFor(t, "five leases moved off b", 3*time.Second, func() bool {
		return b.svc.LivenessStats().LostLeases == 5
	})

	st := b.svc.LivenessStats()
	if st.TakeoversServed != 5 {
		t.Errorf("takeovers served %d, want 5", st.TakeoversServed)
	}
	if st.TakeoversLive != 2 {
		t.Errorf("takeovers that detached a live client: %d, want 2", st.TakeoversLive)
	}
	if st.TakeoversGone != 3 {
		t.Errorf("takeovers that found nothing here: %d, want 3", st.TakeoversGone)
	}
	// The partition must hold: a takeover is either live or gone, never both
	// and never neither. Without this the two counters could drift apart and
	// the ratio they exist to produce would be meaningless.
	if st.TakeoversLive+st.TakeoversGone != st.TakeoversServed {
		t.Errorf("live %d + gone %d != served %d — the split does not partition",
			st.TakeoversLive, st.TakeoversGone, st.TakeoversServed)
	}
	// Served must equal the leases this node lost, an equality the published
	// artifacts show in 16 of 16 runs and which is now asserted rather than
	// observed.
	if st.TakeoversServed != st.LostLeases {
		t.Errorf("served %d but lost %d leases", st.TakeoversServed, st.LostLeases)
	}
	if st.DetachedAgeMaxMs < 9000 {
		t.Errorf("detached age max %d ms, want at least the 9 s connection", st.DetachedAgeMaxMs)
	}
	if st.DetachedAgeAvgMs < 3000 || st.DetachedAgeAvgMs > 9000 {
		t.Errorf("detached age avg %.0f ms, want between the 3 s and 9 s connections", st.DetachedAgeAvgMs)
	}
	// None of these leases was adopted — b held them all along — so the
	// adopted subset must be empty. A counter that fired here would be
	// counting every live takeover, not the adopted ones.
	if st.TakeoversLiveAdopt != 0 {
		t.Errorf("takeovers_live_adopted = %d on leases that were never adopted", st.TakeoversLiveAdopt)
	}
}

// TestAvgMsHasNoAverageOfNothing: a run with no live detaches has no average,
// and a NaN in the artifact is a value every reader has to special-case.
func TestAvgMsHasNoAverageOfNothing(t *testing.T) {
	if got := avgMs(0, 0); got != 0 {
		t.Errorf("avgMs(0,0) = %v, want 0", got)
	}
	if got := avgMs(500, 0); got != 0 {
		t.Errorf("avgMs(500,0) = %v, want 0 — a total with no count is not an average", got)
	}
	if got := avgMs(900, 3); got != 300 {
		t.Errorf("avgMs(900,3) = %v, want 300", got)
	}
}

// TestAdoptedTakeoverIntervalAndCap is WI 1495's B3: the interval between
// adopting a lease and losing it is the number that distinguishes an adoption
// racing an arriving client from a re-home that has nothing to do with the
// adoption. Nothing in the published artifacts can tell those apart.
func TestAdoptedTakeoverIntervalAndCap(t *testing.T) {
	s := &Service{}
	k := key{"default", "c1"}

	// Nothing recorded: no average of nothing, and no sample rather than an
	// empty one — an absent field and a zero are different findings.
	if got := s.adoptTakeoverSample(); got != nil {
		t.Errorf("a service that adopted nothing returned a sample: %v", got)
	}

	s.recordAdoptTakeover(k, 120*time.Millisecond, 3*time.Second, true)
	s.recordAdoptTakeover(key{"default", "c2"}, 880*time.Millisecond, 0, false)

	if got := s.adoptTakeN.Load(); got != 2 {
		t.Errorf("adopt takeovers %d, want 2", got)
	}
	if got := s.adoptTakeMaxMs.Load(); got != 880 {
		t.Errorf("max held %d ms, want 880", got)
	}
	if got := avgMs(s.adoptTakeSumMs.Load(), s.adoptTakeN.Load()); got != 500 {
		t.Errorf("avg held %.0f ms, want 500", got)
	}
	sample := s.adoptTakeoverSample()
	if len(sample) != 2 {
		t.Fatalf("sample holds %d, want 2", len(sample))
	}
	if sample[0].ClientID != "c1" || sample[0].HeldMs != 120 || sample[0].ConnAgeMs != 3000 || !sample[0].Live {
		t.Errorf("first event recorded wrong: %+v", sample[0])
	}
	// An adopted lease taken away with nobody connected is still a wasted
	// adoption; Live is what tells the two apart, so it must be recorded and
	// not inferred from the interval.
	if sample[1].Live || sample[1].ConnAgeMs != 0 {
		t.Errorf("the no-client event recorded wrong: %+v", sample[1])
	}

	// The cap holds and says how much it dropped. Silence here is what made
	// WI 1511 invisible until an artifact refused to publish at 937 MB.
	for i := 0; i < adoptTakeSampleMax+500; i++ {
		s.recordAdoptTakeover(k, time.Millisecond, 0, false)
	}
	if got := len(s.adoptTakeoverSample()); got != adoptTakeSampleMax {
		t.Errorf("sample grew to %d, want the cap %d", got, adoptTakeSampleMax)
	}
	if got := s.adoptTakeoverDropped(); got != 502 {
		t.Errorf("dropped %d, want 502 (500 over the cap, plus the 2 recorded first)", got)
	}
}

// TestTakeoverPathRecordsTheAdoptionInterval drives the call site, not the
// recorder. The first version of this work item's B3 tested
// recordAdoptTakeover directly and passed with the call removed from
// serveTakeover entirely — the same shape as WI 1511's control, which proved
// a function worked rather than that anything used it.
func TestTakeoverPathRecordsTheAdoptionInterval(t *testing.T) {
	c := newCluster(t, []string{"n1", "n2", "n3"}, 5*time.Second, nil)
	a, b := c.nodes["n1"], c.nodes["n2"]

	const id = "adopted-then-taken"
	if _, err := acquire(t, b, id, nil, 300, true); err != nil {
		t.Fatal(err)
	}
	b.sessions.put("default", id, &store.SessionState{SessionMeta: store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 1}})
	b.sessions.setConnAge("default", id, 2*time.Second)

	// Mark the lease as one b adopted during this outage. Seeding the record
	// directly is what lets the call site be tested without staging a whole
	// node death; what is under test is that serving a takeover for an
	// adopted lease records the interval, not how the lease came to be
	// adopted.
	b.svc.adoptedAt.Store(key{"default", id}, b.svc.o.Clock().Add(-750*time.Millisecond))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if res, err := a.svc.AcquireFrom(ctx, "default", id, nil, 300, false, Requester{}); err != nil || res.PrevOwner != b.name {
		t.Fatalf("takeover: %+v %v", res, err)
	}

	waitFor(t, "the lease left b", 3*time.Second, func() bool {
		return b.svc.LivenessStats().LostLeases == 1
	})
	st := b.svc.LivenessStats()
	if st.AdoptTakeovers != 1 {
		t.Fatalf("adopt_takeovers = %d, want 1 — the takeover path did not record the interval", st.AdoptTakeovers)
	}
	if st.TakeoversLiveAdopt != 1 {
		t.Errorf("takeovers_live_adopted = %d, want 1", st.TakeoversLiveAdopt)
	}
	if st.AdoptTakeMaxMs < 700 || st.AdoptTakeMaxMs > 5000 {
		t.Errorf("held %d ms, want about 750", st.AdoptTakeMaxMs)
	}
	if len(st.AdoptTakeSample) != 1 || st.AdoptTakeSample[0].ClientID != id || !st.AdoptTakeSample[0].Live {
		t.Errorf("sample did not carry the event: %+v", st.AdoptTakeSample)
	}
	// And the record is cleared once the lease has gone, so a client that is
	// adopted, re-homed, and adopted again is not reported as held since the
	// first time.
	if _, still := b.svc.adoptedAt.Load(key{"default", id}); still {
		t.Error("the adoption record outlived the lease")
	}
}

// TestAdoptionRecordIsDroppedWhenTheLeaseVanishes: a lease that is released or
// expires is never taken over, so nothing consumes its adoption record. The
// record has to be dropped where the lease disappears or it accumulates for
// the life of the process — which an earlier comment here wrongly claimed was
// bounded by the lease table.
func TestAdoptionRecordIsDroppedWhenTheLeaseVanishes(t *testing.T) {
	s := &Service{}
	s.o.Node = "me"
	k := key{"default", "gone"}
	s.adoptedAt.Store(k, time.Now())

	// The lease leaves this node by vanishing entirely: before is ours,
	// after is nil.
	s.changed(&Lease{Namespace: k.ns, ClientID: k.id, Owner: "me"}, nil)

	if _, still := s.adoptedAt.Load(k); still {
		t.Error("the adoption record survived the lease disappearing; nothing else will ever drop it")
	}

	// The complementary case — a lease that moves to another owner must
	// *keep* its record, because the takeover about to be served is what
	// consumes it — is covered by TestTakeoverPathRecordsTheAdoptionInterval,
	// which drives a real cluster. It cannot be driven from a bare Service:
	// that path enqueues onto the lost-lease work queue, which only New
	// builds.
}
