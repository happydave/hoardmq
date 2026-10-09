package session

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

// fakeJournal records the sequence of calls and lets a test hold Sync.
type fakeJournal struct {
	mu      sync.Mutex
	calls   []string
	state   *store.State
	barrier chan struct{} // when set, Sync blocks until it is closed
	syncs   int
}

func newFakeJournal() *fakeJournal { return &fakeJournal{state: store.NewState()} }

func (j *fakeJournal) log(s string) {
	j.mu.Lock()
	j.calls = append(j.calls, s)
	j.mu.Unlock()
}

func (j *fakeJournal) SessionUpsert(m store.SessionMeta) error {
	j.log("upsert " + m.ClientID)
	return nil
}
func (j *fakeJournal) SessionDelete(k store.Key) error { j.log("delete " + k.ClientID); return nil }
func (j *fakeJournal) Subscribe(k store.Key, sub store.SubscriptionRecord) error {
	j.log("subscribe " + k.ClientID + " " + sub.Filter)
	return nil
}
func (j *fakeJournal) Unsubscribe(k store.Key, filter string) error {
	j.log("unsubscribe " + k.ClientID + " " + filter)
	return nil
}
func (j *fakeJournal) InboundQoS2(k store.Key, pid uint16, add bool) error {
	if add {
		j.log("inbound+ " + k.ClientID)
	} else {
		j.log("inbound- " + k.ClientID)
	}
	return nil
}
func (j *fakeJournal) InboundAccepted(k store.Key, _ uint16) error {
	j.log("inbound accepted " + k.ClientID)
	return nil
}
func (j *fakeJournal) Enqueue(k store.Key, e store.Entry) error {
	// A queued QoS 1 or 2 entry now carries its identifier too, so the
	// state is what says whether it went out.
	if e.State == store.StatePublishSent {
		j.log("enqueue-sent " + k.ClientID)
	} else {
		j.log("enqueue " + k.ClientID)
	}
	return nil
}
func (j *fakeJournal) Sent(k store.Key, seq uint64, pid uint16) error {
	j.log("sent " + k.ClientID)
	return nil
}
func (j *fakeJournal) InflightState(k store.Key, pid uint16, st byte) error {
	j.log("state " + k.ClientID)
	return nil
}
func (j *fakeJournal) Complete(k store.Key, pid uint16) error {
	j.log("complete " + k.ClientID)
	return nil
}
func (j *fakeJournal) Dropped(k store.Key, seq uint64) error {
	j.log("dropped " + k.ClientID)
	return nil
}
func (j *fakeJournal) Retain(m *message.Message) error     { j.log("retain " + m.Topic); return nil }
func (j *fakeJournal) RetainDelete(ns, topic string) error { j.log("retain- " + topic); return nil }
func (j *fakeJournal) Sync() error {
	j.mu.Lock()
	b := j.barrier
	j.syncs++
	j.mu.Unlock()
	if b != nil {
		<-b
	}
	j.log("sync")
	return nil
}

func (j *fakeJournal) sequence() string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return strings.Join(j.calls, "; ")
}

func (j *fakeJournal) count(prefix string) int {
	j.mu.Lock()
	defer j.mu.Unlock()
	n := 0
	for _, c := range j.calls {
		if strings.HasPrefix(c, prefix) {
			n++
		}
	}
	return n
}

func TestAckWaitsForSync(t *testing.T) {
	j := newFakeJournal()
	m, _ := newManager(t, func(o *Options) { o.Journal = j })
	pub := newFake("pub", packet.V5, 100)
	sub := newFake("sub", packet.V5, 100)
	connectV5(m, pub, true, 0, nil)
	connectV5(m, sub, false, 3600, nil)
	subscribe(m, sub, 1, "t")
	j.mu.Lock()
	j.barrier = make(chan struct{})
	j.mu.Unlock()
	done := make(chan packet.ReasonCode, 1)
	go func() { done <- publish(m, pub, "t", 1, 1, "durable") }()
	select {
	case code := <-done:
		t.Fatalf("acknowledged with %s before the journal synced", code)
	case <-time.After(100 * time.Millisecond):
	}
	if len(sub.publishes()) != 1 {
		t.Fatal("delivery must not wait for the sync")
	}
	close(j.barrier)
	if code := <-done; code != packet.Success {
		t.Fatalf("got %s", code)
	}
	// QoS 0 never syncs.
	j.mu.Lock()
	j.barrier = nil
	before := j.syncs
	j.mu.Unlock()
	publish(m, pub, "t", 0, 0, "fire")
	if j.syncs != before {
		t.Error("QoS 0 publish synced")
	}
}

func TestJournalSequenceAndTransientSessions(t *testing.T) {
	j := newFakeJournal()
	m, clock := newManager(t, func(o *Options) { o.Journal = j; o.MaxQueued = 1 })
	pub := newFake("pub", packet.V5, 100)
	connectV5(m, pub, true, 0, nil) // transient: never journaled
	sub := newFake("sub", packet.V5, 1)
	connectV5(m, sub, false, 60, nil)
	subscribe(m, sub, 2, "t")
	publish(m, pub, "t", 2, 1, "a") // sent at once (quota 1)
	publish(m, pub, "t", 1, 2, "b") // queued
	publish(m, pub, "t", 1, 3, "c") // queued, drops b
	id := sub.publishes()[0].PacketID
	m.Pubrec(sub, id, packet.Success)
	sub.ack() // the connection frees the quota before the core sees PUBCOMP
	m.Pubcomp(sub, id, packet.Success)
	sub.ack()
	m.Puback(sub, sub.publishes()[1].PacketID, packet.Success)
	m.Publish(sub, &packet.Publish{Topic: "x", QoS: 2, PacketID: 40})
	m.Pubrel(sub, 40)
	m.Publish(pub, &packet.Publish{Topic: "r", Retain: true, Payload: []byte("v")})
	m.Publish(pub, &packet.Publish{Topic: "r", Retain: true})
	m.Unsubscribe(sub, &packet.Unsubscribe{PacketID: 9, Filters: []string{"t"}})
	m.Disconnect(sub, false, 0, false)
	clock.advance(time.Hour)
	seq := j.sequence()
	for _, want := range []string{
		"upsert sub", "subscribe sub t", "enqueue-sent sub", "sync", "enqueue sub", "dropped sub", "state sub", "complete sub", "sent sub",
		"inbound+ sub", "inbound- sub", "retain r", "retain- r", "unsubscribe sub t", "delete sub",
	} {
		if !strings.Contains(seq, want) {
			t.Errorf("missing %q in\n%s", want, seq)
		}
	}
	if strings.Contains(seq, "pub") {
		t.Errorf("transient session journaled:\n%s", seq)
	}
	if j.count("complete sub") != 2 {
		t.Errorf("completes: %d", j.count("complete sub"))
	}
}

func TestSnapshotAndLoadRoundTrip(t *testing.T) {
	m, clock := newManager(t, nil)
	pub := newFake("pub", packet.V5, 100)
	connectV5(m, pub, true, 0, nil)
	sub := newFake("sub", packet.V5, 1)
	will := &packet.Will{Topic: "w", Payload: []byte("bye"), QoS: 1, Properties: packet.Properties{WillDelayInterval: u32(30)}}
	connectV5(m, sub, false, 600, will)
	m.Subscribe(sub, &packet.Subscribe{PacketID: 1, Properties: packet.Properties{SubscriptionIdentifiers: []uint32{3}}, Subscriptions: []packet.Subscription{{Filter: "t", QoS: 2, RetainAsPublished: true}}})
	publish(m, pub, "t", 2, 1, "inflight")
	publish(m, pub, "t", 1, 2, "queued")
	m.Pubrec(sub, sub.publishes()[0].PacketID, packet.Success)
	m.Publish(sub, &packet.Publish{Topic: "x", QoS: 2, PacketID: 40})
	m.Publish(pub, &packet.Publish{Topic: "r", Retain: true, Payload: []byte("kept")})
	m.Disconnect(sub, false, 0, true) // will armed with delay 30
	snap := m.Snapshot()
	if len(snap.Sessions) != 1 || len(snap.Retained) != 1 {
		t.Fatalf("snapshot %+v", snap)
	}
	ss := snap.Sessions[0]
	if ss.ClientID != "sub" || ss.ExpiryInterval != 600 || ss.Will == nil || !ss.Will.Armed || ss.Will.Delay != 30 || len(ss.Subscriptions) != 1 || len(ss.InboundQoS2) != 1 || len(ss.Entries) != 2 {
		t.Fatalf("session state %+v", ss)
	}
	if ss.Entries[0].State != store.StatePubrecReceived || ss.Entries[1].State != store.StateQueued {
		t.Fatalf("entries %+v", ss.Entries)
	}

	// A fresh manager loads it: the subscriber comes back, gets PUBREL then
	// the queued message, the inbound identifier is known, the retained
	// message is there, and the Will fires after its delay.
	m2, clock2 := newManager(t, nil)
	clock2.now = clock.now
	m2.Load(snap)
	watcher := newFake("watcher", packet.V5, 100)
	connectV5(m2, watcher, true, 0, nil)
	subscribe(m2, watcher, 1, "w")
	if rec, ok := m2.Record("sub"); !ok || rec.Online || rec.Subscriptions != 1 || rec.InFlight != 1 || rec.Queued != 1 {
		t.Fatalf("loaded record %+v %v", rec, ok)
	}
	sub2 := newFake("sub", packet.V5, 100)
	if !connectV5(m2, sub2, false, 600, nil) {
		t.Fatal("session not present after load")
	}
	pk := sub2.packets()
	if len(pk) != 2 {
		t.Fatalf("resume packets %+v", pk)
	}
	if _, ok := pk[0].(*packet.Pubrel); !ok {
		t.Errorf("first resume packet %T", pk[0])
	}
	if p, ok := pk[1].(*packet.Publish); !ok || string(p.Payload) != "queued" || p.Properties.SubscriptionIdentifiers[0] != 3 {
		t.Errorf("second resume packet %+v", pk[1])
	}
	if code, _ := m2.Pubrel(sub2, 40); code != packet.Success {
		t.Error("inbound identifier lost")
	}
	if len(m2.router.RetainedFor("default", "r", clock2.Now())) != 1 {
		t.Error("retained message lost")
	}
	// The Will was suppressed by the reconnect.
	clock2.advance(time.Hour)
	if len(watcher.publishes()) != 0 {
		t.Error("will published despite reconnect")
	}

	// Loading the same snapshot with time advanced past expiry discards the
	// session and publishes its armed Will.
	m3, clock3 := newManager(t, nil)
	clock3.now = clock.now.Add(2 * time.Hour)
	w3 := newFake("watcher", packet.V5, 100)
	connectV5(m3, w3, true, 0, nil)
	subscribe(m3, w3, 1, "w")
	m3.Load(snap)
	if _, ok := m3.Record("sub"); ok {
		t.Error("expired session loaded")
	}
	if p := w3.publishes(); len(p) != 1 || string(p[0].Payload) != "bye" {
		t.Errorf("will at load: %+v", p)
	}
	// Load with the Will delay still pending fires it on the timer.
	m4, clock4 := newManager(t, nil)
	clock4.now = clock.now
	w4 := newFake("watcher", packet.V5, 100)
	connectV5(m4, w4, true, 0, nil)
	subscribe(m4, w4, 1, "w")
	m4.Load(snap)
	clock4.advance(31 * time.Second)
	if len(w4.publishes()) != 1 {
		t.Error("will not fired after load")
	}
}

// TestLoadOnlineSnapshotZeroExpiresAt pins recovery-after-crash expiry
// independently of the cluster tests. A snapshot written while the session
// was online (a crash, or an export during a live takeover) carries no
// expiry deadline; loading it must start the expiry clock at the load —
// neither discarding the session at once nor keeping it forever.
func TestLoadOnlineSnapshotZeroExpiresAt(t *testing.T) {
	m, clock := newManager(t, nil)
	sub := newFake("sub", packet.V5, 1)
	connectV5(m, sub, false, 600, nil)
	m.Subscribe(sub, &packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "t", QoS: 1}}})
	snap := m.Snapshot() // no disconnect: the session is online
	if len(snap.Sessions) != 1 {
		t.Fatalf("snapshot %+v", snap)
	}
	if !snap.Sessions[0].ExpiresAt.IsZero() {
		t.Fatalf("online snapshot carries a deadline: %v", snap.Sessions[0].ExpiresAt)
	}

	m2, clock2 := newManager(t, nil)
	clock2.now = clock.now
	m2.Load(snap)
	if _, ok := m2.Record("sub"); !ok {
		t.Fatal("session with a zero ExpiresAt discarded at load")
	}
	clock2.advance(599 * time.Second)
	if _, ok := m2.Record("sub"); !ok {
		t.Fatal("session expired before its interval, measured from the load")
	}
	clock2.advance(2 * time.Second)
	if _, ok := m2.Record("sub"); ok {
		t.Fatal("session survived past its interval after a zero-ExpiresAt load")
	}
}

// The replication tap stamps the epoch the record carries rather than looking
// it up, so a session's epoch must be authoritative by the time anything is
// journalled for it — not a call later. Connect takes the lease epoch and
// applies it before the upsert; this asserts the record carries it.
func TestConnectJournalsUnderTheLeaseEpoch(t *testing.T) {
	j := &recordingJournal{}
	m, _ := newManager(t, func(o *Options) { o.Journal = j })
	c := newFake("c1", packet.V5, 16)
	expiry := uint32(300)
	m.Connect(c, &packet.Connect{ProtocolLevel: packet.V5, ClientID: "c1", Properties: packet.Properties{SessionExpiryInterval: &expiry}}, 7)
	if got := j.lastUpsertEpoch(); got != 7 {
		t.Fatalf("session journalled under epoch %d, want the lease epoch 7", got)
	}
	// A subsequent write for the same session carries it too: the key the
	// journal writer builds reads the session's epoch.
	subscribe(m, c, 1, "a/b")
	if got := j.lastKeyEpoch(); got != 7 {
		t.Fatalf("subscription journalled under epoch %d, want 7", got)
	}
	// With no lease service (a single node) the manager keeps its own
	// generation and nothing forces an epoch on it.
	c2 := newFake("c2", packet.V5, 16)
	m.Connect(c2, &packet.Connect{ProtocolLevel: packet.V5, ClientID: "c2", Properties: packet.Properties{SessionExpiryInterval: &expiry}}, 0)
	if got := j.lastUpsertEpoch(); got != 0 {
		t.Fatalf("no lease service should not stamp an epoch, got %d", got)
	}
}

type recordingJournal struct {
	mu          sync.Mutex
	upsertEpoch uint64
	keyEpoch    uint64
}

func (r *recordingJournal) lastUpsertEpoch() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.upsertEpoch
}
func (r *recordingJournal) lastKeyEpoch() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.keyEpoch
}
func (r *recordingJournal) SessionUpsert(m store.SessionMeta) error {
	r.mu.Lock()
	r.upsertEpoch = m.Epoch
	r.mu.Unlock()
	return nil
}
func (r *recordingJournal) note(k store.Key) error {
	r.mu.Lock()
	r.keyEpoch = k.Epoch
	r.mu.Unlock()
	return nil
}
func (r *recordingJournal) SessionDelete(k store.Key) error { return r.note(k) }
func (r *recordingJournal) Subscribe(k store.Key, _ store.SubscriptionRecord) error {
	return r.note(k)
}
func (r *recordingJournal) Unsubscribe(k store.Key, _ string) error           { return r.note(k) }
func (r *recordingJournal) InboundQoS2(k store.Key, _ uint16, _ bool) error   { return r.note(k) }
func (r *recordingJournal) InboundAccepted(k store.Key, _ uint16) error       { return r.note(k) }
func (r *recordingJournal) Enqueue(k store.Key, _ store.Entry) error          { return r.note(k) }
func (r *recordingJournal) Sent(k store.Key, _ uint64, _ uint16) error        { return r.note(k) }
func (r *recordingJournal) InflightState(k store.Key, _ uint16, _ byte) error { return r.note(k) }
func (r *recordingJournal) Complete(k store.Key, _ uint16) error              { return r.note(k) }
func (r *recordingJournal) Dropped(k store.Key, _ uint64) error               { return r.note(k) }
func (r *recordingJournal) Retain(_ *message.Message) error                   { return nil }
func (r *recordingJournal) RetainDelete(_, _ string) error                    { return nil }
func (r *recordingJournal) Sync() error                                       { return nil }

// TestImportWritesTheDeleteFirst: an import journals the session whole, and
// whole begins with the delete — so the log, and every replica applying it,
// replaces what it held rather than merging the new state into it (WI 1563).
func TestImportWritesTheDeleteFirst(t *testing.T) {
	j := newFakeJournal()
	m, _ := newManager(t, func(o *Options) { o.Journal = j })
	m.Import(&store.SessionState{
		SessionMeta: store.SessionMeta{Namespace: m.opts.Namespace, ClientID: "imp", Epoch: 2, ExpiryInterval: 600, Version: byte(packet.V5)},
		Entries:     []store.Entry{{Seq: 1, Message: &message.Message{Namespace: m.opts.Namespace, Topic: "t/0", QoS: 1, Payload: []byte("x")}, QoS: 1}},
	})
	j.mu.Lock()
	calls := append([]string(nil), j.calls...)
	j.mu.Unlock()
	var mine []string
	for _, c := range calls {
		if strings.HasSuffix(c, " imp") {
			mine = append(mine, c)
		}
	}
	if len(mine) < 2 || mine[0] != "delete imp" || mine[1] != "upsert imp" {
		t.Fatalf("an import journaled %v; it must begin with the delete, then the upsert", mine)
	}
}
