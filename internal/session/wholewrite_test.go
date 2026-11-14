package session

import (
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

// wholeReplication records the whole writes the manager opens, the journal
// calls each covers, and how each closed.
type wholeReplication struct {
	nopReplication
	j      *fakeJournal
	mu     sync.Mutex
	opened []string // cause, per whole write
	inside [][]string
	closed []wholeClose
	left   []string
}

type wholeClose struct {
	st       *store.SessionState
	appended bool
}

func (r *wholeReplication) OpenWhole(ns, id, cause string) func(*store.SessionState, bool) {
	r.mu.Lock()
	r.opened = append(r.opened, cause)
	r.mu.Unlock()
	r.j.mu.Lock()
	from := len(r.j.calls)
	r.j.mu.Unlock()
	return func(st *store.SessionState, appended bool) {
		r.j.mu.Lock()
		calls := slices.Clone(r.j.calls[from:])
		r.j.mu.Unlock()
		r.mu.Lock()
		r.inside = append(r.inside, calls)
		r.closed = append(r.closed, wholeClose{st, appended})
		r.mu.Unlock()
	}
}

func (r *wholeReplication) SessionLeft(_, id string) {
	r.mu.Lock()
	r.left = append(r.left, id)
	r.mu.Unlock()
}

func wholeManager(t *testing.T) (*Manager, *wholeReplication) {
	t.Helper()
	j := newFakeJournal()
	m, _ := newManager(t, func(o *Options) { o.Journal = j })
	r := &wholeReplication{j: j}
	m.SetReplication(r)
	return m, r
}

func connectWith(m *Manager, c *fakeConn, clean bool, expiry uint32, mayHaveCopies bool, epoch uint64) packet.ReasonCode {
	pkt := &packet.Connect{ProtocolLevel: packet.V5, ClientID: c.id, CleanStart: clean}
	if expiry != 0 {
		pkt.Properties.SessionExpiryInterval = &expiry
	}
	if mayHaveCopies {
		return m.ConnectReplacing(c, pkt, epoch).Reason
	}
	return m.Connect(c, pkt, epoch).Reason
}

// TestWhichCreationsAreWholeWrites: a brand-new persistent session (the
// acquire said no earlier session can have copies) and a persistent Clean
// Start are written whole; a session created where copies may exist without
// Clean Start, a resume, and a session with no expiry are not.
func TestWhichCreationsAreWholeWrites(t *testing.T) {
	for _, c := range []struct {
		name                      string
		clean                     bool
		expiry                    uint32
		mayHaveCopies             bool
		want                      []string
		wantInside, wantNotInside string
	}{
		{"brand new", false, 600, false, []string{wholeCreate}, "upsert x", ""},
		{"fresh where copies may exist", false, 600, true, nil, "", ""},
		{"clean start where copies may exist", true, 600, true, []string{wholeCleanStart}, "delete x", ""},
		{"clean start, brand new", true, 600, false, []string{wholeCleanStart}, "upsert x", ""},
		{"clean start, no expiry", true, 0, true, nil, "", ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			m, r := wholeManager(t)
			if got := connectWith(m, newFake("x", packet.V5, 10), c.clean, c.expiry, c.mayHaveCopies, 7); got != packet.Success {
				t.Fatalf("connect: %s", got)
			}
			if !slices.Equal(r.opened, c.want) {
				t.Fatalf("whole writes opened %v; want %v", r.opened, c.want)
			}
			if c.want == nil {
				return
			}
			if len(r.closed) != 1 || !r.closed[0].appended || r.closed[0].st == nil || r.closed[0].st.Epoch != 7 {
				t.Fatalf("closed %+v; want one close, appended, with the state at 7", r.closed)
			}
			inside := r.inside[0]
			if !slices.Contains(inside, c.wantInside) || !slices.Contains(inside, "upsert x") {
				t.Fatalf("records inside the whole write %v; want %q and the meta", inside, c.wantInside)
			}
		})
	}
}

// TestAResumeAndAHeldCleanStart: a resume of a held session writes its meta
// record alone; a Clean Start over a held session writes its delete and the
// new session's meta inside one whole write.
func TestAResumeAndAHeldCleanStart(t *testing.T) {
	m, r := wholeManager(t)
	x := newFake("x", packet.V5, 10)
	connectWith(m, x, false, 600, false, 7)
	m.Disconnect(x, false, packet.NormalDisconnection, false)
	r.opened = nil
	connectWith(m, newFake("x", packet.V5, 10), false, 600, true, 8)
	if len(r.opened) != 0 {
		t.Fatalf("a resume opened %v; want none", r.opened)
	}
	connectWith(m, newFake("x", packet.V5, 10), true, 600, true, 9)
	if !slices.Equal(r.opened, []string{wholeCleanStart}) {
		t.Fatalf("a held Clean Start opened %v; want one clean-start", r.opened)
	}
	inside := r.inside[len(r.inside)-1]
	if !slices.Contains(inside, "delete x") || !slices.Contains(inside, "upsert x") {
		t.Fatalf("records inside the Clean Start's whole write %v; want its delete and its meta", inside)
	}
}

// TestAnImportIsAWholeWrite: an import's records are written inside one
// whole write, closed with the state imported.
func TestAnImportIsAWholeWrite(t *testing.T) {
	m, r := wholeManager(t)
	m.Import(&store.SessionState{SessionMeta: store.SessionMeta{Namespace: m.opts.Namespace, ClientID: "x", Epoch: 7, Version: 5, ExpiryInterval: 600},
		Subscriptions: []store.SubscriptionRecord{{Filter: "a/b", QoS: 1}}})
	if !slices.Equal(r.opened, []string{wholeImport}) || len(r.closed) != 1 || !r.closed[0].appended {
		t.Fatalf("an import opened %v and closed %+v; want one import, appended", r.opened, r.closed)
	}
	if st := r.closed[0].st; st == nil || len(st.Subscriptions) != 1 {
		t.Fatalf("the import's whole write closed with %+v; want the session with its subscription", st)
	}
	if inside := r.inside[0]; len(inside) < 3 || inside[0] != "delete x" {
		t.Fatalf("records inside the import's whole write %v; want the delete first, then the rest", inside)
	}
}

// upsertFails is a journal whose metadata appends fail.
type upsertFails struct{ *fakeJournal }

func (j upsertFails) SessionUpsert(store.SessionMeta) error { return errUpsert }

var errUpsert = errors.New("upsert refused")

// TestAWholeWriteThatDidNotAppendClosesUnwritten: a creation whose meta
// record failed to append closes its whole write as not appended, so the
// replicas are not told the session is whole.
func TestAWholeWriteThatDidNotAppendClosesUnwritten(t *testing.T) {
	j := newFakeJournal()
	m, _ := newManager(t, func(o *Options) { o.Journal = upsertFails{j} })
	r := &wholeReplication{j: j}
	m.SetReplication(r)
	connectWith(m, newFake("x", packet.V5, 10), false, 600, false, 7)
	if len(r.closed) != 1 || r.closed[0].appended {
		t.Fatalf("closed %+v; want one close, not appended", r.closed)
	}
}

// TestARewriteThatFindsTheSessionGoneTellsTheReplicas: a session armed for a
// rewrite that has left this node is reported gone, so its refusal at the
// targets ends.
func TestARewriteThatFindsTheSessionGoneTellsTheReplicas(t *testing.T) {
	m, r := wholeManager(t)
	connectWith(m, newFake("x", packet.V5, 10), false, 600, false, 7)
	k := key{m.opts.Namespace, "x"}
	sh := m.shardFor(k)
	sh.mu.Lock()
	s := sh.sessions[k]
	sh.mu.Unlock()
	s.mu.Lock()
	s.unwritten = true
	s.mu.Unlock()
	if _, ok := m.Take(m.opts.Namespace, "x", packet.SessionTakenOver); !ok {
		t.Fatal("take")
	}
	if got := m.rewrite(s); got != rewriteGone {
		t.Fatalf("rewrite %v; want gone", got)
	}
	if !slices.Equal(r.left, []string{"x"}) {
		t.Fatalf("sessions reported left %v; want [x]", r.left)
	}
}
