package replication

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/store"
)

// refusedScopes reads what a failed wait names as refused, as the lease
// service reads it: through an interface, not the concrete type.
func refusedScopes(err error) []store.Scope {
	var rk interface{ RefusedScopes() []store.Scope }
	if !errors.As(err, &rk) {
		return nil
	}
	return rk.RefusedScopes()
}

// TestNotHeldNamesEveryRefusedKey: a not-held failure carries every key the
// wait named that the peer refused, bare and once each, in the order named,
// on a target's stream and on another named peer's alike, counted not-held.
func TestNotHeldNamesEveryRefusedKey(t *testing.T) {
	fresh := func() *outbound {
		p := &outbound{name: "peer"}
		p.cond = sync.NewCond(&p.mu)
		return p
	}
	sc := func(id string, epoch uint64) store.Scope {
		return store.Scope{Namespace: "default", ClientID: id, Epoch: epoch}
	}
	p := fresh()
	for i, id := range []string{"a", "b", "c"} {
		p.push(frame{Kind: kindAppend, Namespace: "default", ClientID: id, Epoch: 3})
		if id != "b" {
			p.failedAt(uint64(i+1), "epoch 3 is behind lease epoch 4", store.Scope{})
		}
	}
	keys := []store.Scope{sc("c", 3), sc("b", 3), sc("a", 3), sc("c", 5)}
	want := []store.Scope{sc("c", 0), sc("a", 0)}

	up := func(string) bool { return true }
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for name, err := range map[string]error{"a target's wait": p.wait(ctx, up, keys), "another named peer": p.refusal(keys)} {
		if got := refusedScopes(err); !slices.Equal(got, want) {
			t.Errorf("%s: refused %v, want %v", name, got, want)
		}
		if barrierReason(err) != "not-held" || !errors.Is(err, ErrReplicaUnavailable) {
			t.Errorf("%s: %v (reason %q), want not-held wrapping ErrReplicaUnavailable", name, err, barrierReason(err))
		}
	}
	if err := p.refusal([]store.Scope{sc("b", 3)}); err != nil {
		t.Errorf("a wait naming only a key nobody refused: %v", err)
	}
}

// TestOtherFailuresNameNoRefusedKey: a hole and a timeout fail every key
// alike and name none.
func TestOtherFailuresNameNoRefusedKey(t *testing.T) {
	keys := []store.Scope{{Namespace: "default", ClientID: "a"}}
	up := func(string) bool { return true }

	holed := &outbound{name: "peer"}
	holed.cond = sync.NewCond(&holed.mu)
	holed.push(frame{Kind: kindAppend, Namespace: "default", ClientID: "a"})
	holed.hole = true
	err := holed.wait(context.Background(), up, keys)
	if barrierReason(err) != "resync" || refusedScopes(err) != nil {
		t.Errorf("a holed stream: %v (reason %q), refused %v; want resync naming none", err, barrierReason(err), refusedScopes(err))
	}

	slow := &outbound{name: "peer"}
	slow.cond = sync.NewCond(&slow.mu)
	slow.push(frame{Kind: kindAppend, Namespace: "default", ClientID: "a"})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err = slow.wait(ctx, up, keys)
	if barrierReason(err) != "timeout" || refusedScopes(err) != nil {
		t.Errorf("an unacknowledged stream: %v (reason %q), refused %v; want timeout naming none", err, barrierReason(err), refusedScopes(err))
	}
}
