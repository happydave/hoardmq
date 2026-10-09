package lease

import (
	"testing"
	"time"
)

// TestAcquireSaysWhetherCopiesMayExist: only an acquire whose lease another
// node held, or which held a session expiry, says an earlier session may be
// on the replicas; a first acquire and a local one with no expiry, as a
// liveness probe's, do not.
func TestAcquireSaysWhetherCopiesMayExist(t *testing.T) {
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	a, b := c.nodes["n1"], c.nodes["n2"]
	res, err := acquire(t, a, "x", nil, 300, true)
	if err != nil || res.MayHaveCopies {
		t.Fatalf("first acquire: %+v %v, want MayHaveCopies false", res, err)
	}
	first := res.Epoch
	waitFor(t, "n2's table shows x on n1", 3*time.Second, func() bool {
		l, ok := b.svc.table.Lookup("default", "x")
		return ok && l.Owner == "n1" && l.Epoch == first
	})
	res, err = acquire(t, b, "x", nil, 300, true)
	if err != nil || !res.MayHaveCopies || res.PrevOwner != "n1" {
		t.Fatalf("clean-start takeover: %+v %v, want MayHaveCopies and PrevOwner n1", res, err)
	}
	second := res.Epoch
	waitFor(t, "n2's table shows x on n2", 3*time.Second, func() bool {
		l, ok := b.svc.table.Lookup("default", "x")
		return ok && l.Owner == "n2" && l.Epoch == second
	})
	res, err = acquire(t, b, "x", nil, 300, true)
	if err != nil || !res.Local || !res.MayHaveCopies {
		t.Fatalf("local acquire with an expiry: %+v %v, want Local and MayHaveCopies", res, err)
	}
	res, err = acquire(t, a, "p", nil, 0, true)
	if err != nil || res.MayHaveCopies {
		t.Fatalf("a probe's first acquire: %+v %v, want MayHaveCopies false", res, err)
	}
	ownedBy(t, a, "p", "n1")
	res, err = acquire(t, a, "p", nil, 0, true)
	if err != nil || !res.Local || res.MayHaveCopies {
		t.Fatalf("a probe's local acquire with no expiry: %+v %v, want Local and not MayHaveCopies", res, err)
	}
}

func TestMayHaveCopies(t *testing.T) {
	for _, c := range []struct {
		found    bool
		owner    string
		expiry   uint32
		prev, me string
		want     bool
	}{
		{false, "", 0, "", "n2", false},
		{false, "", 0, "n2", "n2", false},
		{false, "", 0, "n1", "n2", true},
		{true, "n2", 0, "", "n2", false},
		{true, "n2", 0, "n2", "n2", false},
		{true, "n2", 600, "", "n2", true},
		{true, "n1", 0, "", "n2", true},
		{true, "n1", 0, "n1", "n2", true},
	} {
		if got := mayHaveCopies(c.found, c.owner, c.expiry, c.prev, c.me); got != c.want {
			t.Errorf("mayHaveCopies(%v, %q, %d, %q, %q) = %v, want %v", c.found, c.owner, c.expiry, c.prev, c.me, got, c.want)
		}
	}
}
