package replication

import (
	"encoding/json"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/lease"
	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

const unappliedMeta = "hoardmq_replication_unapplied_total"

func loneMetaRefusals(sinks map[string]*counters, node string) float64 {
	return sinks[node].get(unappliedMeta, map[string]string{"reason": "no-session", "type": "session-upsert"})
}

// threeAt starts n1–n3 on one shard, n1 its owner and n2, n3 its targets, at
// level 2 unless level1.
func threeAt(t *testing.T, level1 bool, extra func(*Options)) (*harness, map[string]*counters, map[string]*lease.Tokens) {
	t.Helper()
	tokens := map[string]*lease.Tokens{}
	mutate := atLevel2(tokens, extra)
	if level1 {
		mutate = func(o *Options) {
			tokens[o.Node] = lease.NewTokens()
			o.Tokens = tokens[o.Node]
			if extra != nil {
				extra(o)
			}
		}
	}
	h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3"}, 1, 3, mutate)
	h.applyMap(shardmap.Compute(1, h.order, 3, 1))
	waitSettled(h, 1)
	return h, sinks, tokens
}

// loneMeta writes a session's meta record alone at the owner, as a resuming
// connect does.
func loneMeta(t *testing.T, h *harness, owner, id string, epoch uint64) {
	t.Helper()
	n := h.nodes[owner]
	h.setLease("default", id, owner, epoch)
	if err := n.own.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: id, Epoch: epoch, Version: 5, ExpiryInterval: 300}); err != nil {
		t.Fatal(err)
	}
	if err := n.own.Sync(); err != nil {
		t.Fatal(err)
	}
}

// TestALoneMetaRefusedIsNotInTheReplicaLog: at level 2 a meta record finding
// no copy is refused, and it is not in the follower's replica log either, so
// the follower's recovery makes no copy of it. The key's repair is made just
// before, so the refusal's own is a repeat and no later record masks the log.
func TestALoneMetaRefusedIsNotInTheReplicaLog(t *testing.T) {
	h, sinks, _ := threeAt(t, false, nil)
	id := clientIn(1, 0)
	n1 := h.nodes["n1"]
	h.setLease("default", id, "n1", 6)
	if err := n1.own.Enqueue(store.Key{Namespace: "default", ClientID: id, Epoch: 6}, store.Entry{Seq: 1, QoS: 1, Message: &message.Message{Namespace: "default", Topic: "a/b", Payload: []byte("x")}}); err != nil {
		t.Fatal(err)
	}
	if err := n1.own.Sync(); err != nil {
		t.Fatal(err)
	}
	h.waitFor("n1 to repair the key at n2 with its delete", 5*time.Second, func() bool { return repairsAny(sinks, "n1", "delete") >= 2 })
	loneMeta(t, h, "n1", id, 6)
	h.waitFor("n2 to refuse the lone meta record", 5*time.Second, func() bool { return loneMetaRefusals(sinks, "n2") >= 1 })
	if holdsCopy(h, "n2", id) {
		t.Fatal("n2 made a copy from a lone meta record at level 2")
	}
	n2 := h.nodes["n2"]
	if err := n2.replica.Sync(); err != nil {
		t.Fatal(err)
	}
	st, err := n2.replica.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if st.Find("default", id) != nil {
		t.Fatal("n2's replica log replays a copy from the meta record it refused")
	}
}

// TestALoneMetaAtLevelOneStillMakesACopy: the refusal is gated by the frame's
// stamp. An unstamped frame's lone meta record makes a copy as before.
func TestALoneMetaAtLevelOneStillMakesACopy(t *testing.T) {
	h, sinks, _ := threeAt(t, true, nil)
	id := clientIn(1, 0)
	loneMeta(t, h, "n1", id, 6)
	h.waitFor("n2 to make the copy", 5*time.Second, func() bool { return holdsCopy(h, "n2", id) })
	if n := loneMetaRefusals(sinks, "n2"); n != 0 {
		t.Fatalf("n2 refused %v lone meta records from unstamped frames; want 0", n)
	}
}

// TestADropBetweenTheCopyCheckAndTheApplyLeavesNoHollowCopy: a drop of the
// copy that arrives between the follower's check for a copy and the meta
// record's apply waits for the apply, so it leaves no copy rather than a
// hollow one.
func TestADropBetweenTheCopyCheckAndTheApplyLeavesNoHollowCopy(t *testing.T) {
	h, _, _ := threeAt(t, false, nil)
	id := clientIn(1, 0)
	writeSessionWhole(t, h, "n1", id, 5)
	h.waitFor("n2 to hold the copy", 5*time.Second, func() bool { return copyEpochOf(h, "n2", id) == 5 })
	n2 := h.nodes["n2"]
	dropped := make(chan struct{})
	var once sync.Once
	n2.rep.follower.afterCopyCheck = func() {
		once.Do(func() {
			go func() {
				n2.rep.follower.dropCopy("default", id, "test")
				close(dropped)
			}()
			select {
			case <-dropped:
			case <-time.After(300 * time.Millisecond):
			}
		})
	}
	loneMeta(t, h, "n1", id, 6)
	select {
	case <-dropped:
	case <-time.After(5 * time.Second):
		t.Fatal("the drop never ran")
	}
	if ss := n2.rep.ReplicaState().Find("default", id); ss != nil && len(ss.Subscriptions) == 0 {
		t.Fatalf("n2 holds a hollow copy at %d after a drop raced the meta record; want the copy or none", ss.Epoch)
	}
}

// TestAFetchNeverReadsASnapshotHalfApplied: a fetch made while a snapshot's
// records are being applied reads the copy before them or after them, never
// deleted or holding the meta without its subscription.
func TestAFetchNeverReadsASnapshotHalfApplied(t *testing.T) {
	h, _, _ := threeAt(t, false, nil)
	id := clientIn(1, 0)
	writeSessionWhole(t, h, "n1", id, 5)
	h.waitFor("n2 to hold the copy", 5*time.Second, func() bool { return copyEpochOf(h, "n2", id) == 5 })
	n2 := h.nodes["n2"]
	type read struct {
		ss   *store.SessionState
		held bool
	}
	got := make(chan read, 1)
	var once sync.Once
	n2.rep.follower.betweenSnapshotRecords = func() {
		once.Do(func() {
			go func() {
				ss, held := n2.rep.follower.find("default", id)
				got <- read{ss, held}
			}()
			select {
			case r := <-got:
				got <- r
			case <-time.After(300 * time.Millisecond):
			}
		})
	}
	importWhole(t, h, id, 7, nil)
	var r read
	select {
	case r = <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("the fetch never ran")
	}
	if !r.held || len(r.ss.Subscriptions) != 1 {
		t.Fatalf("a fetch during a snapshot's apply read held %v, subscriptions %d; want the copy whole, before or after", r.held, subsIn(r.ss))
	}
}

func TestWholeForm(t *testing.T) {
	for _, c := range []struct {
		snap, head, entry, limit int
		want                     string
	}{
		{100, 100, 0, 100, formSnapshot},
		{101, 60, 50, 100, formHeadAndRecords},
		{101, 101, 50, 100, formTooLarge},
		{101, 60, 101, 100, formTooLarge},
	} {
		if got := wholeForm(c.snap, c.head, c.entry, c.limit); got != c.want {
			t.Errorf("wholeForm(%d, %d, %d, %d) = %s; want %s", c.snap, c.head, c.entry, c.limit, got, c.want)
		}
	}
}

func TestRepairDecisions(t *testing.T) {
	if !coveredBy(4, 5) || coveredBy(5, 5) || coveredBy(6, 5) || coveredBy(4, 0) {
		t.Error("coveredBy: a whole write covers a refusal only when pushed after it")
	}
	if !repairsEarlierGrant("stale-epoch", 7, 12) {
		t.Error("a stale refusal at an earlier epoch is this node's earlier grant")
	}
	if repairsEarlierGrant("stale-epoch", 12, 12) {
		t.Error("a stale refusal at the record's own epoch is a later grant seen; no repair")
	}
	if repairsEarlierGrant("other", 7, 12) || repairsEarlierGrant("unapplied", 7, 12) {
		t.Error("a refusal of another class would be refused again; no repair")
	}
	if !refusesLoneMeta(2, false, false) || !refusesLoneMeta(3, false, false) {
		t.Error("a lone meta record stamped 2 or above is refused when it finds no copy")
	}
	if refusesLoneMeta(0, false, false) || refusesLoneMeta(1, false, false) {
		t.Error("an unstamped frame's lone meta record is applied as before")
	}
	if refusesLoneMeta(2, true, false) || !refusesLoneMeta(0, false, true) {
		t.Error("the older build's pin applies it; the test seam refuses it unstamped")
	}
}

func TestRedecideDueLeavesATrailingDecision(t *testing.T) {
	t0 := time.Unix(1000, 0)
	if run, _ := redecideDue(time.Time{}, t0, time.Second, false); !run {
		t.Fatal("a first shrink decides at once")
	}
	run, after := redecideDue(t0, t0.Add(300*time.Millisecond), time.Second, false)
	if run || after != 700*time.Millisecond {
		t.Fatalf("a shrink inside the interval: run %v after %v; want a trailing decision in 700ms", run, after)
	}
	if run, after := redecideDue(t0, t0.Add(400*time.Millisecond), time.Second, true); run || after != 0 {
		t.Fatalf("a shrink with a trailing decision arranged: run %v after %v; want neither", run, after)
	}
	if run, _ := redecideDue(t0, t0.Add(time.Second), time.Second, true); !run {
		t.Fatal("a shrink past the interval decides at once")
	}
}

func TestWarnLimiterCountsWhatItHeldBack(t *testing.T) {
	var l warnLimiter
	t0 := time.Unix(1000, 0)
	if ok, n := l.due(t0, time.Minute); !ok || n != 1 {
		t.Fatalf("first: %v %d", ok, n)
	}
	for i := 1; i <= 3; i++ {
		if ok, _ := l.due(t0.Add(time.Duration(i)*time.Second), time.Minute); ok {
			t.Fatal("a Warn inside the minute")
		}
	}
	if ok, n := l.due(t0.Add(time.Minute), time.Minute); !ok || n != 4 {
		t.Fatalf("after the minute: %v %d; want true and 4", ok, n)
	}
}

// TestEncodeFrameSendsTheStateItEncoded: a whole write's state encoded once
// is sent as the state member: a reader of any build decodes the same frame
// json.Marshal gives (the member comes last instead of in field order).
func TestEncodeFrameSendsTheStateItEncoded(t *testing.T) {
	st := &store.SessionState{SessionMeta: store.SessionMeta{Namespace: "default", ClientID: "c", Epoch: 7, Version: 5},
		Subscriptions: []store.SubscriptionRecord{{Filter: "a/b", QoS: 1}}, InboundQoS2: []uint16{}, Entries: []store.Entry{}}
	raw, _ := json.Marshal(st)
	f := frame{Kind: kindSnapshot, Seq: 9, Namespace: "default", ClientID: "c", Epoch: 7, Level: 2, State: st}
	want, _ := json.Marshal(f)
	f.stateJSON = raw
	var a, b frame
	if err := json.Unmarshal(encodeFrame(f), &a); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(want, &b); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("encodeFrame decodes to\n%+v\nwant\n%+v", a, b)
	}
	if a.State == nil || len(a.State.Subscriptions) != 1 {
		t.Fatalf("the state member carries %+v; want the session", a.State)
	}
}

// TestMixedVersionWholeCopies: n3 stands in for a build before whole copies
// (records-form whole writes, lone meta records sent and applied as before),
// at level 1. A new owner's import reaches n3 and n2 as one snapshot each,
// applied whole; the pinned owner's session, written in records, makes its
// copy on a new follower from its lone meta record; and where a new follower
// refuses that record (a test seam: no new follower does at level 1), the
// pinned owner repairs the session whole.
func TestMixedVersionWholeCopies(t *testing.T) {
	h, sinks, _ := threeAt(t, true, func(o *Options) {
		o.AckTimeout = 300 * time.Millisecond
		switch o.Node {
		case "n3":
			o.PreWholeCopies = true
		case "n1":
			o.RefuseLoneMetaUnstamped = true
		}
	})
	if h.nodes["n3"].rep.OpenWhole("default", "any", causeImport) != nil {
		t.Fatal("the pinned node opens a whole write")
	}
	a := clientIn(1, 0)
	importWhole(t, h, a, 7, nil)
	for _, n := range []string{"n2", "n3"} {
		h.waitFor(n+" to hold n1's import whole", 5*time.Second, func() bool {
			ss := h.nodes[n].rep.ReplicaState().Find("default", a)
			return ss != nil && ss.Epoch == 7 && len(ss.Subscriptions) == 1
		})
	}

	b := clientIn(1, 0, "b")
	n3 := h.nodes["n3"]
	n3.sessions.mu.Lock()
	n3.sessions.sessions = []*store.SessionState{{SessionMeta: store.SessionMeta{Namespace: "default", ClientID: b, Epoch: 4, Version: 5, ExpiryInterval: 300},
		Subscriptions: []store.SubscriptionRecord{{Filter: "a/b", QoS: 1}}, InboundQoS2: []uint16{}, Entries: []store.Entry{}}}
	n3.sessions.mu.Unlock()
	h.writeSession("n3", b, 4)
	h.waitFor("n2 to make b's copy from the pinned owner's records", 5*time.Second, func() bool {
		ss := h.nodes["n2"].rep.ReplicaState().Find("default", b)
		return ss != nil && len(ss.Subscriptions) == 1
	})
	h.waitFor("n1 to refuse the lone meta record", 5*time.Second, func() bool { return loneMetaRefusals(sinks, "n1") >= 1 })
	h.waitFor("the pinned owner to repair b whole at n1", 5*time.Second, func() bool {
		ss := h.nodes["n1"].rep.ReplicaState().Find("default", b)
		return ss != nil && len(ss.Subscriptions) == 1
	})
}

func TestLockHoldPercentiles(t *testing.T) {
	var c lockHoldCounters
	for range 98 {
		c.waited(holdFind, 3*time.Microsecond)
	}
	c.waited(holdFind, 100*time.Millisecond)
	c.waited(holdFind, 200*time.Millisecond)
	if got := p99Of(&c.waits[holdFind]); got != int64(1)<<bucketOf(int64(100*time.Millisecond))*1000 {
		t.Fatalf("p99 of 98 short waits and 2 long ones: %d ns; want the 99th sample's bucket (100 ms)", got)
	}
	if got := c.waitMax[holdFind].Load(); got != int64(200*time.Millisecond) {
		t.Fatalf("max wait %d", got)
	}
	var one lockHoldCounters
	for range 100 {
		one.waited(holdFind, 3*time.Microsecond)
	}
	if got := p99Of(&one.waits[holdFind]); got != 4000 {
		t.Fatalf("p99 of 3 µs waits: %d ns; want 4000 (the 2–4 µs bucket's bound)", got)
	}
}
