package session

import (
	"strings"
	"testing"

	"github.com/happydave/hoardmq/internal/conn"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

func connectAt(m *Manager, c *fakeConn, clean bool, expiry uint32, epoch uint64) conn.ConnectResult {
	pkt := &packet.Connect{ProtocolLevel: packet.V5, ClientID: c.id, CleanStart: clean}
	if expiry != 0 {
		pkt.Properties.SessionExpiryInterval = &expiry
	}
	res := m.Connect(c, pkt, epoch)
	if res.Reason == packet.Success {
		m.Resume(c)
	}
	return res
}

// sealedFixture: a persistent session "c" at token 5 with a subscription to
// "t" and one queued message, taken offline, and a publisher.
func sealedFixture(t *testing.T) (*Manager, *fakeJournal, *fakeConn) {
	t.Helper()
	j := newFakeJournal()
	m, _ := newManager(t, func(o *Options) { o.Journal = j })
	pub := newFake("pub", packet.V5, 100)
	connectAt(m, pub, true, 0, 1)
	c := newFake("c", packet.V5, 100)
	connectAt(m, c, true, 600, 5)
	subscribe(m, c, 1, "t")
	m.Disconnect(c, false, packet.NormalDisconnection, false)
	publish(m, pub, "t", 1, 1, "queued")
	return m, j, pub
}

func journalFor(j *fakeJournal, id string) []string {
	j.mu.Lock()
	defer j.mu.Unlock()
	var out []string
	for _, c := range j.calls {
		if strings.HasSuffix(c, " "+id) {
			out = append(out, c)
		}
	}
	return out
}

// TestSealJournalsNothing: the hand-over's delete, journaled at the pass, is
// what reached a follower ahead of the grant and removed the only copy
// (WIs 1575, 1740). A seal writes no record; the old removal writes the delete.
func TestSealJournalsNothing(t *testing.T) {
	m, j, _ := sealedFixture(t)
	before := len(journalFor(j, "c"))
	res := m.Seal(m.opts.Namespace, "c", packet.SessionTakenOver, 9)
	if res.Outcome != SealSealed || res.State == nil || len(res.State.Entries) != 1 || len(res.State.Subscriptions) != 1 || res.Token != 9 {
		t.Fatalf("seal: %+v", res)
	}
	if got := journalFor(j, "c")[before:]; len(got) != 0 {
		t.Fatalf("a seal journaled %v; it must journal nothing", got)
	}
	if _, held := m.Export(m.opts.Namespace, "c"); held {
		t.Fatal("a sealed session is still reported held")
	}
	if st, tok, ok := m.Sealed(m.opts.Namespace, "c"); !ok || tok != 9 || len(st.Entries) != 1 {
		t.Fatalf("sealed copy: %v %d %v", st, tok, ok)
	}

	m2, j2, _ := sealedFixture(t)
	before = len(journalFor(j2, "c"))
	if st, ok := m2.Take(m2.opts.Namespace, "c", packet.SessionTakenOver); !ok || st == nil {
		t.Fatal("take found nothing")
	}
	if got := journalFor(j2, "c")[before:]; len(got) != 1 || got[0] != "delete c" {
		t.Fatalf("the old removal journaled %v, want the delete", got)
	}
}

// TestSealedSessionTakesNoEnqueue: it is in no route, so a local publish does
// not reach it and a forwarded one finds no subscriber here.
func TestSealedSessionTakesNoEnqueue(t *testing.T) {
	m, _, pub := sealedFixture(t)
	if n, _, _ := m.InjectForwarded(&message.Message{Namespace: m.opts.Namespace, Topic: "t", QoS: 1, Payload: []byte("before")}); n != 1 {
		t.Fatalf("before the seal a forwarded publish matched %d sessions, want 1", n)
	}
	m.Seal(m.opts.Namespace, "c", packet.SessionTakenOver, 9)
	if n, _, _ := m.InjectForwarded(&message.Message{Namespace: m.opts.Namespace, Topic: "t", QoS: 1, Payload: []byte("after")}); n != 0 {
		t.Fatalf("a forwarded publish matched %d sessions after the seal, want 0", n)
	}
	publish(m, pub, "t", 1, 2, "local")
	st, _, _ := m.Sealed(m.opts.Namespace, "c")
	if len(st.Entries) != 2 {
		t.Fatalf("the sealed copy has %d entries; it holds what it had when sealed (2: queued and the forward before)", len(st.Entries))
	}
}

func TestSealOutcomes(t *testing.T) {
	m, _, _ := sealedFixture(t)
	ns := m.opts.Namespace
	if res := m.Seal(ns, "c", packet.SessionTakenOver, 4); res.Outcome != SealNewer || res.Token != 5 {
		t.Fatalf("a request below the session's own token: %+v, want newer", res)
	}
	if _, held := m.Export(ns, "c"); !held {
		t.Fatal("a newer session was sealed for an older request")
	}
	m.Seal(ns, "c", packet.SessionTakenOver, 9)
	if res := m.Seal(ns, "c", packet.SessionTakenOver, 9); res.Outcome != SealAlready || res.Raised || res.State == nil {
		t.Fatalf("asked again at the same token: %+v", res)
	}
	if res := m.Seal(ns, "c", packet.SessionTakenOver, 12); res.Outcome != SealAlready || !res.Raised || res.Token != 12 {
		t.Fatalf("asked at a higher token: %+v", res)
	}
	if res := m.Seal(ns, "nobody", packet.SessionTakenOver, 12); res.Outcome != SealNone {
		t.Fatalf("nothing here: %+v", res)
	}
	if m.SealedCount() != 1 {
		t.Fatalf("sealed count %d", m.SealedCount())
	}
}

func TestDropSealed(t *testing.T) {
	m, j, _ := sealedFixture(t)
	ns := m.opts.Namespace
	m.Seal(ns, "c", packet.SessionTakenOver, 9)
	before := len(journalFor(j, "c"))
	if m.DropSealed(ns, "c", 8) {
		t.Fatal("dropped for a token below the one it was sealed for")
	}
	if !m.DropSealed(ns, "c", 9) {
		t.Fatal("not dropped at its token")
	}
	if got := journalFor(j, "c")[before:]; len(got) != 1 || got[0] != "delete c" {
		t.Fatalf("the drop journaled %v, want the delete", got)
	}
	if m.DropSealed(ns, "c", 99) || m.SealedCount() != 0 {
		t.Fatal("dropped twice")
	}
}

func TestEvictOffline(t *testing.T) {
	m, j, _ := sealedFixture(t)
	ns := m.opts.Namespace
	if m.EvictOffline(ns, "c", 4) {
		t.Fatal("evicted a session held at a later token")
	}
	c := newFake("c", packet.V5, 100)
	connectAt(m, c, false, 600, 5)
	if m.EvictOffline(ns, "c", 9) {
		t.Fatal("evicted a connected session")
	}
	m.Disconnect(c, false, packet.NormalDisconnection, false)
	before := len(journalFor(j, "c"))
	if !m.EvictOffline(ns, "c", 9) {
		t.Fatal("did not evict an offline session held below the token")
	}
	if got := journalFor(j, "c")[before:]; len(got) != 1 || got[0] != "delete c" {
		t.Fatalf("the eviction journaled %v", got)
	}
}

func TestSealedConnect(t *testing.T) {
	cases := []struct {
		sealedFor, lease uint64
		clean            bool
		want             sealedVerdict
	}{
		{9, 9, false, sealedRefuse},
		{9, 5, false, sealedRefuse},
		{9, 5, true, sealedRefuse},
		{9, 0, false, sealedRefuse},
		{9, 10, true, sealedDiscard},
		{9, 10, false, sealedResume},
	}
	for _, c := range cases {
		if got := sealedConnect(c.sealedFor, c.lease, c.clean); got != c.want {
			t.Errorf("sealedConnect(%d, %d, %v) = %v, want %v", c.sealedFor, c.lease, c.clean, got, c.want)
		}
	}
}

// TestConnectOverASealedSession: a CONNECT at a grant no newer than the seal's
// is refused; at a later grant it resumes the session, or with Clean Start
// discards it with its delete journaled before the new session.
func TestConnectOverASealedSession(t *testing.T) {
	m, j, pub := sealedFixture(t)
	ns := m.opts.Namespace
	m.Seal(ns, "c", packet.SessionTakenOver, 9)
	if res := connectAt(m, newFake("c", packet.V5, 100), false, 600, 5); res.Reason != packet.ServerBusy {
		t.Fatalf("a CONNECT at this node's older grant: %s, want Server Busy", res.Reason)
	}
	if _, _, ok := m.Sealed(ns, "c"); !ok {
		t.Fatal("a refused CONNECT changed the sealed copy")
	}
	c := newFake("c", packet.V5, 100)
	res := connectAt(m, c, false, 600, 12)
	if res.Reason != packet.Success || !res.SessionPresent {
		t.Fatalf("a CONNECT at a later grant: %+v, want the session resumed", res)
	}
	if p := c.publishes(); len(p) != 1 || string(p[0].Payload) != "queued" {
		t.Fatalf("resumed session delivered %+v", p)
	}
	publish(m, pub, "t", 1, 3, "routed")
	if p := c.publishes(); len(p) != 2 || string(p[1].Payload) != "routed" {
		t.Fatalf("the resumed session's subscription is not routed: %+v", p)
	}
	if at, ok := m.HeldAt(ns, "c"); !ok || at != 12 {
		t.Fatalf("held at %d %v", at, ok)
	}

	m2, j2, _ := sealedFixture(t)
	m2.Seal(ns, "c", packet.SessionTakenOver, 9)
	before := len(journalFor(j2, "c"))
	res = connectAt(m2, newFake("c", packet.V5, 100), true, 600, 12)
	if res.Reason != packet.Success || res.SessionPresent {
		t.Fatalf("a clean start at a later grant: %+v", res)
	}
	got := journalFor(j2, "c")[before:]
	if len(got) < 2 || got[0] != "delete c" || got[1] != "upsert c" {
		t.Fatalf("a clean start over a sealed copy journaled %v; the delete must come first", got)
	}
	if m2.SealedCount() != 0 {
		t.Fatal("the sealed copy outlived the clean start")
	}
	_ = j
}

// TestImportReplacesASealedCopy: the lease came back and a state was imported;
// the import's whole write begins with the delete the sealed records waited for.
func TestImportReplacesASealedCopy(t *testing.T) {
	m, j, _ := sealedFixture(t)
	ns := m.opts.Namespace
	res := m.Seal(ns, "c", packet.SessionTakenOver, 9)
	st := res.State
	st.Epoch = 12
	before := len(journalFor(j, "c"))
	m.Import(st)
	if m.SealedCount() != 0 {
		t.Fatal("an import left the sealed copy")
	}
	if got := journalFor(j, "c")[before:]; len(got) < 1 || got[0] != "delete c" {
		t.Fatalf("the import journaled %v", got)
	}
	if at, ok := m.HeldAt(ns, "c"); !ok || at != 12 {
		t.Fatalf("held at %d %v", at, ok)
	}
}

// TestCheckpointKeepsSealedCopies: a checkpoint replaces the journal, and a
// seal journals nothing, so a checkpoint that left the sealed copy out would
// delete it from disk before the new owner confirmed it; a drained node that
// stopped then restarted could not recover it (WI 1763). The resync snapshot
// still leaves it out: the new owner feeds the replicas.
func TestCheckpointKeepsSealedCopies(t *testing.T) {
	noIndex := func() uint64 { return 0 }
	m, _, _ := sealedFixture(t)
	has := func(sessions []*store.SessionState, id string) *store.SessionState {
		for _, s := range sessions {
			if s.ClientID == id {
				return s
			}
		}
		return nil
	}
	if s := has(m.CheckpointState(noIndex).Sessions, "c"); s == nil || len(s.Entries) != 1 {
		t.Fatalf("checkpoint before the seal: %+v", s)
	}
	if res := m.Seal(m.opts.Namespace, "c", packet.SessionTakenOver, 9); res.Outcome != SealSealed {
		t.Fatalf("seal: %+v", res)
	}
	s := has(m.CheckpointState(noIndex).Sessions, "c")
	if s == nil || len(s.Entries) != 1 || len(s.Subscriptions) != 1 || s.Epoch != 5 {
		t.Fatalf("checkpoint of a sealed session: %+v; want the sealed state at its own epoch", s)
	}
	if s := has(m.Snapshot().Sessions, "c"); s != nil {
		t.Fatalf("the resync snapshot carries a sealed session: %+v", s)
	}
	if !m.DropSealed(m.opts.Namespace, "c", 9) {
		t.Fatal("the sealed copy was not dropped")
	}
	if s := has(m.CheckpointState(noIndex).Sessions, "c"); s != nil {
		t.Fatalf("checkpoint after the drop: %+v", s)
	}
}
