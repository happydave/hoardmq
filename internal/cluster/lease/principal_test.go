package lease

import (
	"bytes"
	"testing"
	"time"
)

// TestPrincipalRefusedEarlyDecidesOnlyAMismatchUnderBinding: the dangerous
// arguments first, each of which must not refuse, then the one that must.
func TestPrincipalRefusedEarlyDecidesOnlyAMismatchUnderBinding(t *testing.T) {
	alice, mallory, anon := PrincipalDigest("default", "alice"), PrincipalDigest("default", "mallory"), PrincipalDigest("default", "")
	for _, c := range []struct {
		name              string
		bind              bool
		lease, connecting uint64
		want              bool
	}{
		{"binding off", false, alice, mallory, false},
		{"a lease without a digest", true, 0, mallory, false},
		{"a CONNECT without a digest", true, alice, 0, false},
		{"the same principal", true, alice, alice, false},
		{"anonymous resuming anonymous", true, anon, anon, false},
		{"anonymous taking a named principal's", true, alice, anon, true},
		{"a named principal taking anonymous's", true, anon, alice, true},
		{"another principal", true, alice, mallory, true},
	} {
		if got := principalRefusedEarly(c.bind, c.lease, c.connecting); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

// TestPrincipalDigestSeparatesItsParts: the namespace and username are
// length-prefixed, an empty username is a principal of its own, never 0, and
// the digest is stable.
func TestPrincipalDigestSeparatesItsParts(t *testing.T) {
	if PrincipalDigest("a", "bc") == PrincipalDigest("ab", "c") {
		t.Error("the namespace and username run together")
	}
	if d := PrincipalDigest("default", ""); d == 0 || d == PrincipalDigest("default", "x") {
		t.Errorf("an empty username digests to %d", d)
	}
	if a, b := PrincipalDigest("default", "alice"), PrincipalDigest("default", string([]byte("alice"))); a != b {
		t.Error("the digest is not stable")
	}
	if PrincipalDigest("tenant", "alice") == PrincipalDigest("default", "alice") {
		t.Error("the namespace is not in the digest")
	}
}

// TestAcquireRecordsThePrincipalAndMovesKeepIt: an acquire sets the lease's
// digest (0 from an acquire that carries none); a reassign and a hand-off,
// which move the same session, keep it.
func TestAcquireRecordsThePrincipalAndMovesKeepIt(t *testing.T) {
	tb := NewTable()
	now := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	alice := PrincipalDigest("d", "alice")
	apply(t, tb,
		Op{Kind: OpAlive, Node: "n1", Now: now},
		Op{Kind: OpAlive, Node: "n2", Now: now},
		Op{Kind: OpAcquire, Namespace: "d", ClientID: "c1", Node: "n1", ExpiryInterval: 60, Now: now, Principal: alice},
		Op{Kind: OpAcquire, Namespace: "d", ClientID: "c2", Node: "n1", ExpiryInterval: 60, Now: now, Principal: alice})
	if l, _ := tb.Lookup("d", "c1"); l.Principal != alice {
		t.Fatalf("after the acquire: %d, want alice's", l.Principal)
	}
	apply(t, tb, Op{Kind: OpHandoff, Node: "n1", To: "n2", Now: now.Add(time.Second)})
	if l, _ := tb.Lookup("d", "c1"); l.Owner != "n2" || l.Principal != alice {
		t.Fatalf("after the hand-off: %+v", l)
	}
	apply(t, tb, Op{Kind: OpReassign, Node: "n2", To: "n1", SeenAliveIndex: 1, Now: now.Add(time.Minute)})
	if l, _ := tb.Lookup("d", "c1"); l.Owner != "n1" || l.Principal != alice {
		t.Fatalf("after the reassign: %+v", l)
	}
	apply(t, tb, Op{Kind: OpAcquire, Namespace: "d", ClientID: "c2", Node: "n2", ExpiryInterval: 60, Now: now.Add(2 * time.Minute)})
	if l, _ := tb.Lookup("d", "c2"); l.Owner != "n2" || l.Principal != 0 {
		t.Fatalf("after an acquire carrying no digest: %+v", l)
	}
}

// TestCompactLeaseWithoutAPrincipalIsTodaysBytes: a lease with no digest
// encodes exactly as before the field existed, and one with it reads back.
func TestCompactLeaseWithoutAPrincipalIsTodaysBytes(t *testing.T) {
	l := &Lease{Namespace: "default", ClientID: "c1", Owner: "n1", Epoch: 7, Online: true, ExpiryInterval: 3600}
	want := []byte{0x01, 7, 'd', 'e', 'f', 'a', 'u', 'l', 't', 2, 'c', '1', 2, 'n', '1', 7, 0x90, 0x1c}
	if got := encodeCompactLease(l); !bytes.Equal(got, want) {
		t.Fatalf("encoding %x, want today's %x", got, want)
	}
	l.Principal = PrincipalDigest("default", "alice")
	b := encodeCompactLease(l)
	if len(b) != len(want)+8 {
		t.Fatalf("with a digest: %d bytes, want %d", len(b), len(want)+8)
	}
	back, err := decodeCompactLease(b)
	if err != nil || *back != *l {
		t.Fatalf("read back %+v %v, want %+v", back, err, l)
	}
	if _, err := decodeCompactLease(b[:len(b)-1]); err == nil {
		t.Fatal("a truncated digest read back")
	}
}
