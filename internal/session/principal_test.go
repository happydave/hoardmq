package session

import (
	"testing"

	"github.com/happydave/hoardmq/internal/packet"
)

func TestPrincipalRefused(t *testing.T) {
	for _, c := range []struct {
		bind              bool
		owner, connecting string
		refused           bool
	}{
		{true, "alice", "alice", false},
		{true, "alice", "mallory", true},
		{true, "", "", false},
		{true, "alice", "", true},
		{true, "", "alice", true},
		{false, "alice", "mallory", false},
	} {
		if got := principalRefused(c.bind, c.owner, c.connecting); got != c.refused {
			t.Errorf("principalRefused(%v, %q, %q) = %v", c.bind, c.owner, c.connecting, got)
		}
	}
}

// boundFixture is sealedFixture with the binding on: client "c", principal
// "u-c", one queued message, offline.
func boundFixture(t *testing.T) *Manager {
	t.Helper()
	m, _ := newManager(t, func(o *Options) { o.Journal = newFakeJournal(); o.BindPrincipal = true })
	pub := newFake("pub", packet.V5, 100)
	connectAt(m, pub, true, 0, 1)
	c := newFake("c", packet.V5, 100)
	connectAt(m, c, true, 600, 5)
	subscribe(m, c, 1, "t")
	m.Disconnect(c, false, packet.NormalDisconnection, false)
	publish(m, pub, "t", 1, 1, "queued")
	return m
}

func as(principal string) *fakeConn {
	f := newFake("c", packet.V5, 100)
	f.identity.Username = principal
	return f
}

// A sealed copy that a CONNECT at a later grant would resume or discard is
// left as it is for another principal, and resumed by its own.
func TestSealedCopyRefusedToAnotherPrincipal(t *testing.T) {
	m := boundFixture(t)
	ns := m.opts.Namespace
	m.Seal(ns, "c", packet.SessionTakenOver, 9)
	for _, clean := range []bool{false, true} {
		if res := connectAt(m, as("mallory"), clean, 600, 12); res.Reason != packet.NotAuthorized {
			t.Fatalf("mallory over the sealed copy (clean %v): %s", clean, res.Reason)
		}
		if _, _, ok := m.Sealed(ns, "c"); !ok {
			t.Fatalf("mallory's refused CONNECT (clean %v) changed the sealed copy", clean)
		}
	}
	c := as("u-c")
	if res := connectAt(m, c, false, 600, 12); res.Reason != packet.Success || !res.SessionPresent {
		t.Fatalf("the owner over the sealed copy: %+v", res)
	}
	if p := c.publishes(); len(p) != 1 || string(p[0].Payload) != "queued" {
		t.Fatalf("the owner's queue: %+v", p)
	}
}

// A session imported for a CONNECT, as a takeover does, is refused to
// another principal and held offline here; its owner then resumes it.
func TestImportedSessionRefusedToAnotherPrincipal(t *testing.T) {
	src := boundFixture(t)
	st := src.Seal(src.opts.Namespace, "c", packet.SessionTakenOver, 9).State
	st.Epoch = 12
	m, _ := newManager(t, func(o *Options) { o.Journal = newFakeJournal(); o.BindPrincipal = true })
	m.Import(st)
	if res := connectAt(m, as("mallory"), false, 600, 12); res.Reason != packet.NotAuthorized {
		t.Fatalf("mallory after the import: %s", res.Reason)
	}
	if rec, ok := m.Record("c"); !ok || rec.Online {
		t.Fatalf("the imported session after the refusal: %+v %v, want held offline", rec, ok)
	}
	c := as("u-c")
	if res := connectAt(m, c, false, 600, 12); res.Reason != packet.Success || !res.SessionPresent {
		t.Fatalf("the owner after the import: %+v", res)
	}
	if p := c.publishes(); len(p) != 1 || string(p[0].Payload) != "queued" {
		t.Fatalf("the owner's queue: %+v", p)
	}
}
