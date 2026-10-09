package session

import (
	"errors"
	"io"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
	"github.com/happydave/hoardmq/internal/store/faultdisk"
)

// ackJournal accepts every write and counts its Syncs. hold makes the next
// Sync signal entered and wait for release; failSync fails every Sync; the
// subscribe and unsubscribe appends of failFilter fail.
type ackJournal struct {
	scopeJournal
	syncs      atomic.Int32
	deletes    atomic.Int32
	hold       atomic.Bool
	failSync   atomic.Bool
	failFilter string
	// failUndo fails the unsubscribe append of that filter only.
	failUndo string
	subs     sync.Map // filter -> *atomic.Int32, Subscribe appends that succeeded
	entered  chan struct{}
	release  chan struct{}
}

func newAckJournal() *ackJournal {
	return &ackJournal{entered: make(chan struct{}, 1), release: make(chan struct{})}
}

func (j *ackJournal) Sync() error {
	j.syncs.Add(1)
	if j.hold.CompareAndSwap(true, false) {
		j.entered <- struct{}{}
		<-j.release
	}
	if j.failSync.Load() {
		return errors.New("injected sync failure")
	}
	return nil
}

func (j *ackJournal) SessionDelete(k store.Key) error {
	j.deletes.Add(1)
	return j.scopeJournal.SessionDelete(k)
}

func (j *ackJournal) Subscribe(k store.Key, sub store.SubscriptionRecord) error {
	if sub.Filter == j.failFilter {
		return errors.New("injected append failure")
	}
	n, _ := j.subs.LoadOrStore(sub.Filter, &atomic.Int32{})
	n.(*atomic.Int32).Add(1)
	return j.scopeJournal.Subscribe(k, sub)
}

// subscribed is how many Subscribe appends of filter succeeded.
func (j *ackJournal) subscribed(filter string) int32 {
	if n, ok := j.subs.Load(filter); ok {
		return n.(*atomic.Int32).Load()
	}
	return 0
}

func (j *ackJournal) Unsubscribe(k store.Key, filter string) error {
	if filter == j.failFilter || filter == j.failUndo {
		return errors.New("injected append failure")
	}
	return j.scopeJournal.Unsubscribe(k, filter)
}

// diskFixture is a manager over a real store whose fsyncs the fault disk can
// fail, with a connected persistent subscriber "sub" (level 5, expiry 600)
// and a connected transient publisher "pub".
type diskFixture struct {
	m     *Manager
	clock *fakeClock
	d     *faultdisk.Disk
	st    *store.Store
	dir   string
	sub   *fakeConn
	pub   *fakeConn
}

func newDiskFixture(t *testing.T, mutate func(*Options)) *diskFixture {
	t.Helper()
	f := &diskFixture{d: faultdisk.New(), dir: t.TempDir()}
	st, err := store.Open(f.dir, store.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Sync: f.d.Sync})
	if err != nil {
		t.Fatal(err)
	}
	f.st = st
	t.Cleanup(func() { _ = st.Close() })
	f.m, f.clock = newManager(t, func(o *Options) {
		o.Journal = st
		if mutate != nil {
			mutate(o)
		}
	})
	f.sub = newFake("sub", packet.V5, 100)
	f.pub = newFake("pub", packet.V5, 100)
	connectV5(f.m, f.sub, true, 600, nil)
	connectV5(f.m, f.pub, true, 0, nil)
	return f
}

// dirty journals a record of another session that no Sync has covered yet.
func dirty(m *Manager) {
	m.writer().record(&Session{persistent: true}, func() error {
		return m.opts.Journal.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: "other", Epoch: 1})
	})
}

// subsOf is a session's subscriptions as a snapshot reports them, by filter.
func subsOf(st *store.State, id string) map[string]store.SubscriptionRecord {
	out := map[string]store.SubscriptionRecord{}
	for _, s := range st.Sessions {
		if s.ClientID != id {
			continue
		}
		for _, sub := range s.Subscriptions {
			out[sub.Filter] = sub
		}
	}
	return out
}

func sameSubs(t *testing.T, where string, got map[string]store.SubscriptionRecord, want ...store.SubscriptionRecord) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: subscriptions %+v, want %+v", where, got, want)
	}
	for _, w := range want {
		if g, ok := got[w.Filter]; !ok || g != w {
			t.Fatalf("%s: subscriptions %+v, want %+v", where, got, want)
		}
	}
}

// TestSubackWaitsForTheSync: a persistent session's SUBACK codes are returned
// only after a Sync that started after the filter's record was appended has
// returned.
func TestSubackWaitsForTheSync(t *testing.T) {
	j := newAckJournal()
	unblock := sync.OnceFunc(func() { close(j.release) })
	t.Cleanup(unblock)
	m, _ := newManager(t, func(o *Options) { o.Journal = j })
	c := newFake("c", packet.V5, 100)
	connectV5(m, c, true, 600, nil)
	j.hold.Store(true)
	done := make(chan []packet.ReasonCode, 1)
	go func() {
		done <- m.Subscribe(c, &packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "t/1", QoS: 1}}})
	}()
	select {
	case <-j.entered:
	case codes := <-done:
		t.Fatalf("Subscribe returned %v without syncing its record", codes)
	case <-time.After(10 * time.Second):
		t.Fatal("Subscribe neither synced nor returned")
	}
	select {
	case codes := <-done:
		t.Fatalf("Subscribe returned %v while the sync covering its record was in flight", codes)
	default:
	}
	unblock()
	if codes := <-done; len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("codes %v, want [1]", codes)
	}
}

// TestUnsubackWaitsForTheSync: the same for an UNSUBSCRIBE of a held filter.
func TestUnsubackWaitsForTheSync(t *testing.T) {
	j := newAckJournal()
	unblock := sync.OnceFunc(func() { close(j.release) })
	t.Cleanup(unblock)
	m, _ := newManager(t, func(o *Options) { o.Journal = j })
	c := newFake("c", packet.V5, 100)
	connectV5(m, c, true, 600, nil)
	subscribe(m, c, 1, "t/1")
	j.hold.Store(true)
	done := make(chan []packet.ReasonCode, 1)
	go func() { done <- m.Unsubscribe(c, &packet.Unsubscribe{PacketID: 2, Filters: []string{"t/1"}}) }()
	select {
	case <-j.entered:
	case codes := <-done:
		t.Fatalf("Unsubscribe returned %v without syncing its record", codes)
	case <-time.After(10 * time.Second):
		t.Fatal("Unsubscribe neither synced nor returned")
	}
	select {
	case codes := <-done:
		t.Fatalf("Unsubscribe returned %v while the sync covering its record was in flight", codes)
	default:
	}
	unblock()
	if codes := <-done; len(codes) != 1 || codes[0] != packet.Success {
		t.Fatalf("codes %v, want [Success]", codes)
	}
}

// TestFailedSyncRefusesAndWithdrawsTheSubscription: a SUBSCRIBE whose sync
// fails is refused with 0x83 for every filter, and each filter goes back to
// what the session held before: in memory, in the router (a publish reaches
// it only as before), in the retained messages sent after the SUBACK, and in
// the journal once it is repaired.
func TestFailedSyncRefusesAndWithdrawsTheSubscription(t *testing.T) {
	for _, tc := range []struct {
		name    string
		prior   []packet.Subscription
		request []packet.Subscription
		// want is what the session holds after the refusal.
		want []store.SubscriptionRecord
	}{
		{"a new filter", nil, []packet.Subscription{{Filter: "t/1", QoS: 2}}, nil},
		{"a filter held at QoS 1, asked at QoS 2", []packet.Subscription{{Filter: "t/1", QoS: 1}}, []packet.Subscription{{Filter: "t/1", QoS: 2}}, []store.SubscriptionRecord{{Filter: "t/1", QoS: 1}}},
		{"two new filters", nil, []packet.Subscription{{Filter: "t/1", QoS: 1}, {Filter: "t/#", QoS: 2}}, nil},
		{"a held filter named twice", []packet.Subscription{{Filter: "t/1", QoS: 1}}, []packet.Subscription{{Filter: "t/1", QoS: 2}, {Filter: "t/1", QoS: 0}}, []store.SubscriptionRecord{{Filter: "t/1", QoS: 1}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newDiskFixture(t, nil)
			if len(tc.prior) > 0 {
				if codes := f.m.Subscribe(f.sub, &packet.Subscribe{PacketID: 1, Subscriptions: tc.prior}); codes[0].IsError() {
					t.Fatalf("precondition: the prior SUBSCRIBE was refused: %v", codes)
				}
				f.m.Subscribed(f.sub)
			}
			if code, _ := f.m.Publish(f.pub, &packet.Publish{Topic: "t/1", QoS: 1, PacketID: 1, Retain: true, Payload: []byte("kept")}); code.IsError() {
				t.Fatalf("precondition: the retained publish was refused: %v", code)
			}
			before := len(f.sub.publishes())
			f.d.FailNext(1)
			codes := f.m.Subscribe(f.sub, &packet.Subscribe{PacketID: 2, Subscriptions: tc.request})
			for i, code := range codes {
				if code != packet.ImplementationSpecificError {
					t.Fatalf("filter %q answered %v, want 0x83: the SUBACK granted a filter whose sync failed", tc.request[i].Filter, code)
				}
			}
			sameSubs(t, "in memory after the refusal", subsOf(f.m.Snapshot(), "sub"), tc.want...)
			f.m.Subscribed(f.sub)
			if got := len(f.sub.publishes()); got != before {
				t.Fatalf("the subscriber was sent %d messages after the refused SUBSCRIBE, want none: a retained message went out for a refused filter", got-before)
			}
			if code, _ := f.m.Publish(f.pub, &packet.Publish{Topic: "t/1", QoS: 1, PacketID: 2, Payload: []byte("after")}); code.IsError() {
				t.Fatalf("the publish after the refusal was refused: %v (the journal's repair failed)", code)
			}
			pubs := f.sub.publishes()[before:]
			switch {
			case len(tc.want) == 0 && len(pubs) != 0:
				t.Fatalf("a publish after the refusal reached the subscriber %d times; the refused filter is still routed", len(pubs))
			case len(tc.want) == 1 && (len(pubs) != 1 || pubs[0].QoS != 1):
				t.Fatalf("a publish after the refusal reached the subscriber as %+v, want once at the prior QoS 1", pubs)
			}
			sameSubs(t, "in the journal after its repair", subsOf(recovered(t, f.dir, f.st), "sub"), tc.want...)
		})
	}
}

// TestFailedAppendRefusesThatFilterOnly: a filter whose record fails to append
// is refused and withdrawn; another filter of the same SUBSCRIBE, appended
// and synced, is granted. The refusing writer leaves the session unmarked, so
// no whole rewrite follows.
func TestFailedAppendRefusesThatFilterOnly(t *testing.T) {
	j := newAckJournal()
	j.failFilter = "bad"
	m, clock := newManager(t, func(o *Options) { o.Journal = j })
	c := newFake("c", packet.V5, 100)
	connectV5(m, c, true, 600, nil)
	syncs := j.syncs.Load()
	codes := m.Subscribe(c, &packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "bad", QoS: 1}, {Filter: "good", QoS: 1}}})
	if codes[0] != packet.ImplementationSpecificError || codes[1] != 1 {
		t.Fatalf("codes %v, want [0x83 1]", codes)
	}
	sameSubs(t, "after the SUBSCRIBE", subsOf(m.Snapshot(), "c"), store.SubscriptionRecord{Filter: "good", QoS: 1})
	if got := j.syncs.Load() - syncs; got != 1 {
		t.Fatalf("Syncs during the SUBSCRIBE = %d, want 1: the granted filter was not synced", got)
	}
	deletes := j.deletes.Load()
	clock.advance(2 * pubrelRetry)
	if got := j.deletes.Load() - deletes; got != 0 {
		t.Fatalf("the session was written whole %d times after a refused filter's failed append, want 0", got)
	}
}

// TestTransientSessionSubscribeWaitsForNothing: a session the journal does
// not record syncs nothing for its SUBSCRIBE or UNSUBSCRIBE, even with
// another session's record waiting, so a failing Sync refuses nothing.
func TestTransientSessionSubscribeWaitsForNothing(t *testing.T) {
	j := newAckJournal()
	m, _ := newManager(t, func(o *Options) { o.Journal = j })
	c := newFake("c", packet.V5, 100)
	connectV5(m, c, true, 0, nil)
	dirty(m)
	j.failSync.Store(true)
	syncs := j.syncs.Load()
	if codes := m.Subscribe(c, &packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "t/1", QoS: 1}}}); codes[0] != 1 {
		t.Fatalf("SUBSCRIBE codes %v, want [1]", codes)
	}
	if codes := m.Unsubscribe(c, &packet.Unsubscribe{PacketID: 2, Filters: []string{"t/1"}}); codes[0] != packet.Success {
		t.Fatalf("UNSUBSCRIBE codes %v, want [Success]", codes)
	}
	if got := j.syncs.Load() - syncs; got != 0 {
		t.Fatalf("Syncs = %d, want 0 for a session the journal does not record", got)
	}
}

// TestFailedSyncRefusesTheUnsubscribeAndRestores: an UNSUBSCRIBE whose sync
// fails is refused with 0x83 for the filter it removed, and the subscription
// comes back with its options, in memory, in the router and in the repaired
// journal; a filter the session never held is still answered 0x11.
func TestFailedSyncRefusesTheUnsubscribeAndRestores(t *testing.T) {
	f := newDiskFixture(t, nil)
	held := &packet.Subscribe{PacketID: 1, Properties: packet.Properties{SubscriptionIdentifiers: []uint32{7}}, Subscriptions: []packet.Subscription{{Filter: "u/1", QoS: 1}}}
	if codes := f.m.Subscribe(f.sub, held); codes[0] != 1 {
		t.Fatalf("precondition: SUBSCRIBE codes %v", codes)
	}
	want := store.SubscriptionRecord{Filter: "u/1", QoS: 1, Identifier: 7}
	f.d.FailNext(1)
	codes := f.m.Unsubscribe(f.sub, &packet.Unsubscribe{PacketID: 2, Filters: []string{"u/1", "never"}})
	if !slices.Equal(codes, []packet.ReasonCode{packet.ImplementationSpecificError, packet.NoSubscriptionExisted}) {
		t.Fatalf("codes %v, want [0x83 0x11]", codes)
	}
	sameSubs(t, "in memory after the refusal", subsOf(f.m.Snapshot(), "sub"), want)
	before := len(f.sub.publishes())
	if code, _ := f.m.Publish(f.pub, &packet.Publish{Topic: "u/1", QoS: 1, PacketID: 1, Payload: []byte("x")}); code.IsError() {
		t.Fatalf("the publish after the refusal was refused: %v", code)
	}
	if pubs := f.sub.publishes()[before:]; len(pubs) != 1 || !slices.Equal(pubs[0].Properties.SubscriptionIdentifiers, []uint32{7}) {
		t.Fatalf("a publish after the refused UNSUBSCRIBE reached the subscriber as %+v, want once with identifier 7", pubs)
	}
	sameSubs(t, "in the journal after its repair", subsOf(recovered(t, f.dir, f.st), "sub"), want)
}

// TestFailedAppendRefusesTheUnsubscribeAndRestores: the same when the
// UNSUBSCRIBE's record fails to append; nothing reached the journal, so
// nothing is synced, not even another session's waiting record, and the
// session is not marked for a whole rewrite.
func TestFailedAppendRefusesTheUnsubscribeAndRestores(t *testing.T) {
	j := newAckJournal()
	m, clock := newManager(t, func(o *Options) { o.Journal = j })
	c := newFake("c", packet.V5, 100)
	connectV5(m, c, true, 600, nil)
	subscribe(m, c, 1, "keep")
	j.failFilter = "keep"
	dirty(m)
	syncs := j.syncs.Load()
	if codes := m.Unsubscribe(c, &packet.Unsubscribe{PacketID: 2, Filters: []string{"keep"}}); codes[0] != packet.ImplementationSpecificError {
		t.Fatalf("codes %v, want [0x83]", codes)
	}
	sameSubs(t, "after the refusal", subsOf(m.Snapshot(), "c"), store.SubscriptionRecord{Filter: "keep", QoS: 1})
	if got := j.syncs.Load() - syncs; got != 0 {
		t.Fatalf("Syncs = %d, want 0: nothing was appended", got)
	}
	deletes := j.deletes.Load()
	clock.advance(2 * pubrelRetry)
	if got := j.deletes.Load() - deletes; got != 0 {
		t.Fatalf("the session was written whole %d times after a refused removal's failed append, want 0", got)
	}
}

// TestFailedSyncRefusesTheConnect: a CONNECT whose sync fails is refused with
// 0x83. The connection is not left attached, the session stays offline with
// its expiry, the CONNECT's Will is discarded rather than armed, and the
// client's next CONNECT finds its session.
func TestFailedSyncRefusesTheConnect(t *testing.T) {
	f := newDiskFixture(t, nil)
	watcher := newFake("watcher", packet.V5, 100)
	connectV5(f.m, watcher, true, 0, nil)
	subscribe(f.m, watcher, 0, "will/c1")
	exp := uint32(600)
	c := newFake("c1", packet.V5, 100)
	pkt := &packet.Connect{ProtocolLevel: packet.V5, ClientID: "c1", CleanStart: true, Will: &packet.Will{Topic: "will/c1", Payload: []byte("gone")}, Properties: packet.Properties{SessionExpiryInterval: &exp}}
	f.d.FailNext(1)
	res := f.m.Connect(c, pkt, 0)
	if res.Reason != packet.ImplementationSpecificError {
		t.Fatalf("CONNECT answered %v, want 0x83: the CONNACK ignored its failed sync", res.Reason)
	}
	if s := f.m.lookup(c); s != nil {
		t.Fatal("the refused connection is still attached to the session")
	}
	rec, ok := f.m.Record("c1")
	if !ok || rec.Online || rec.ExpiryInterval != 600 {
		t.Fatalf("session after the refusal %+v (found %v), want offline with expiry 600", rec, ok)
	}
	f.clock.advance(time.Second)
	if pubs := watcher.publishes(); len(pubs) != 0 {
		t.Fatalf("the refused CONNECT's Will was published %d times", len(pubs))
	}
	again := newFake("c1", packet.V5, 100)
	retry := *pkt
	retry.CleanStart, retry.Will = false, nil
	if res := f.m.Connect(again, &retry, 0); res.Reason != packet.Success || !res.SessionPresent {
		t.Fatalf("the reconnect answered %+v, want Success with Session Present", res)
	}
}

// TestFailedSyncRefusesATransientConnect: with Session Expiry 0 the refused
// CONNECT ends the session, and the ended callback releases it.
func TestFailedSyncRefusesATransientConnect(t *testing.T) {
	var ended atomic.Int32
	f := newDiskFixture(t, func(o *Options) {
		o.Ended = func(_, id string, _ uint64) {
			if id == "c2" {
				ended.Add(1)
			}
		}
	})
	// A record nothing has synced yet, so the transient CONNECT's sync runs.
	dirty(f.m)
	f.d.FailNext(1)
	c := newFake("c2", packet.V5, 100)
	if res := f.m.Connect(c, &packet.Connect{ProtocolLevel: packet.V5, ClientID: "c2", CleanStart: true}, 0); res.Reason != packet.ImplementationSpecificError {
		t.Fatalf("CONNECT answered %v, want 0x83", res.Reason)
	}
	if _, ok := f.m.Record("c2"); ok {
		t.Fatal("the refused transient session is still held")
	}
	if got := ended.Load(); got != 1 {
		t.Fatalf("ended callback ran %d times for c2, want 1", got)
	}
}

// heldRefusal starts a request and waits until it is held in its Sync. The
// function it returns makes that Sync fail, releases it, and returns the
// request's codes.
func heldRefusal(t *testing.T, j *ackJournal, call func() []packet.ReasonCode) func() []packet.ReasonCode {
	t.Helper()
	unblock := sync.OnceFunc(func() { close(j.release) })
	t.Cleanup(unblock)
	j.hold.Store(true)
	done := make(chan []packet.ReasonCode, 1)
	go func() { done <- call() }()
	select {
	case <-j.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("precondition: the request never reached its Sync")
	}
	return func() []packet.ReasonCode {
		j.failSync.Store(true)
		unblock()
		codes := <-done
		j.failSync.Store(false)
		return codes
	}
}

// TestRefusalLeavesAFilterChangedDuringItsWait: a SUBSCRIBE refused after
// another SUBSCRIBE of the same filter was granted while it waited leaves the
// granted subscription alone; so does an UNSUBSCRIBE refused after the filter
// was subscribed again.
func TestRefusalLeavesAFilterChangedDuringItsWait(t *testing.T) {
	t.Run("subscribe", func(t *testing.T) {
		j := newAckJournal()
		m, _ := newManager(t, func(o *Options) { o.Journal = j })
		c := newFake("c", packet.V5, 100)
		connectV5(m, c, true, 600, nil)
		finish := heldRefusal(t, j, func() []packet.ReasonCode {
			return m.Subscribe(c, &packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "t/1", QoS: 2}}})
		})
		if codes := m.Subscribe(c, &packet.Subscribe{PacketID: 2, Subscriptions: []packet.Subscription{{Filter: "t/1", QoS: 1}}}); codes[0] != 1 {
			t.Fatalf("precondition: the second SUBSCRIBE answered %v", codes)
		}
		if codes := finish(); codes[0] != packet.ImplementationSpecificError {
			t.Fatalf("precondition: the held SUBSCRIBE answered %v, want 0x83", codes)
		}
		sameSubs(t, "after the refusal", subsOf(m.Snapshot(), "c"), store.SubscriptionRecord{Filter: "t/1", QoS: 1})
	})
	t.Run("unsubscribe", func(t *testing.T) {
		j := newAckJournal()
		m, _ := newManager(t, func(o *Options) { o.Journal = j })
		c := newFake("c", packet.V5, 100)
		connectV5(m, c, true, 600, nil)
		subscribe(m, c, 1, "t/1")
		finish := heldRefusal(t, j, func() []packet.ReasonCode {
			return m.Unsubscribe(c, &packet.Unsubscribe{PacketID: 2, Filters: []string{"t/1"}})
		})
		if codes := m.Subscribe(c, &packet.Subscribe{PacketID: 3, Subscriptions: []packet.Subscription{{Filter: "t/1", QoS: 2}}}); codes[0] != 2 {
			t.Fatalf("precondition: the SUBSCRIBE during the wait answered %v", codes)
		}
		if codes := finish(); codes[0] != packet.ImplementationSpecificError {
			t.Fatalf("precondition: the held UNSUBSCRIBE answered %v, want 0x83", codes)
		}
		sameSubs(t, "after the refusal", subsOf(m.Snapshot(), "c"), store.SubscriptionRecord{Filter: "t/1", QoS: 2})
	})
}

// TestRefusalLeavesASessionItsConnectionLeft: a SUBSCRIBE refused after its
// connection ended during the wait changes nothing in the session the
// connection left; the journal holds the record and the repair keeps it.
func TestRefusalLeavesASessionItsConnectionLeft(t *testing.T) {
	j := newAckJournal()
	m, _ := newManager(t, func(o *Options) { o.Journal = j })
	c := newFake("c", packet.V5, 100)
	connectV5(m, c, true, 600, nil)
	finish := heldRefusal(t, j, func() []packet.ReasonCode {
		return m.Subscribe(c, &packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "t/1", QoS: 1}}})
	})
	m.Disconnect(c, false, packet.UnspecifiedError, false)
	if codes := finish(); codes[0] != packet.ImplementationSpecificError {
		t.Fatalf("precondition: the held SUBSCRIBE answered %v, want 0x83", codes)
	}
	sameSubs(t, "after the refusal", subsOf(m.Snapshot(), "c"), store.SubscriptionRecord{Filter: "t/1", QoS: 1})
}

// TestFailedUndoIsWrittenWhole: a SUBSCRIBE's sync fails and so does the
// append of the record that undoes it. The session is marked and written
// whole, and the rewrite holds no record of the refused filter.
func TestFailedUndoIsWrittenWhole(t *testing.T) {
	j := newAckJournal()
	m, clock := newManager(t, func(o *Options) { o.Journal = j })
	c := newFake("c", packet.V5, 100)
	connectV5(m, c, true, 600, nil)
	subscribe(m, c, 1, "t")
	j.failUndo = "x"
	j.failSync.Store(true)
	codes := m.Subscribe(c, &packet.Subscribe{PacketID: 2, Subscriptions: []packet.Subscription{{Filter: "x", QoS: 1}}})
	j.failSync.Store(false)
	if codes[0] != packet.ImplementationSpecificError {
		t.Fatalf("precondition: the SUBSCRIBE answered %v, want 0x83", codes)
	}
	deletes, xs := j.deletes.Load(), j.subscribed("x")
	clock.advance(pubrelRetry)
	if got := j.deletes.Load() - deletes; got != 1 {
		t.Fatalf("the session was written whole %d times after its undo record failed, want 1", got)
	}
	if j.subscribed("t") < 2 || j.subscribed("x") != xs {
		t.Fatalf("the rewrite wrote t %d times and x %d more times, want t again and x not at all", j.subscribed("t"), j.subscribed("x")-xs)
	}
}

// TestSessionAcknowledgementsWaitForNoReplica: with the replicas unreachable,
// a SUBSCRIBE, an UNSUBSCRIBE and a CONNECT are still answered once the local
// journal has synced.
func TestSessionAcknowledgementsWaitForNoReplica(t *testing.T) {
	j := newAckJournal()
	m, _ := newManager(t, func(o *Options) { o.Journal = j })
	r := &switchReplication{}
	r.down.Store(true)
	m.SetReplication(r)
	c := newFake("c", packet.V5, 100)
	exp := uint32(600)
	if res := m.Connect(c, &packet.Connect{ProtocolLevel: packet.V5, ClientID: "c", CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, 0); res.Reason != packet.Success {
		t.Fatalf("CONNECT answered %v with the replicas down, want Success", res.Reason)
	}
	syncs := j.syncs.Load()
	if codes := m.Subscribe(c, &packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "t/1", QoS: 1}}}); codes[0] != 1 {
		t.Fatalf("SUBSCRIBE answered %v with the replicas down, want [1]", codes)
	}
	if codes := m.Unsubscribe(c, &packet.Unsubscribe{PacketID: 2, Filters: []string{"t/1"}}); codes[0] != packet.Success {
		t.Fatalf("UNSUBSCRIBE answered %v with the replicas down, want [Success]", codes)
	}
	if got := j.syncs.Load() - syncs; got != 2 {
		t.Fatalf("Syncs = %d, want 2: each acknowledgement waits for the local journal", got)
	}
}
