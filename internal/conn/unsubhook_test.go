package conn

import (
	"context"
	"sync"
	"testing"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/hook"
	"github.com/happydave/hoardmq/internal/packet"
)

// unsubHooks answers each unsubscribed filter from outcomes (Continue when
// absent) and records the filters it saw.
type unsubHooks struct {
	pingHooks
	outcomes map[string]hook.Outcome
	mu       *sync.Mutex
	seen     *[]string
}

func (u unsubHooks) Unsubscribe(_ context.Context, _ hook.Info, filter string) hook.Outcome {
	u.mu.Lock()
	*u.seen = append(*u.seen, filter)
	u.mu.Unlock()
	if out, ok := u.outcomes[filter]; ok {
		return out
	}
	return hook.OK
}

// recordingAuthorizer allows everything and records the topics it was asked
// about.
type recordingAuthorizer struct {
	mu     *sync.Mutex
	topics *[]string
}

func (r recordingAuthorizer) Authorize(_ context.Context, _ auth.Action, topic string, _ *auth.Identity) auth.Decision {
	r.mu.Lock()
	*r.topics = append(*r.topics, topic)
	r.mu.Unlock()
	return auth.Allow
}

type unsubCase struct {
	h      *harness
	mu     sync.Mutex
	seen   []string
	authed []string
}

func newUnsubCase(t *testing.T, v packet.Version, outcomes map[string]hook.Outcome) *unsubCase {
	t.Helper()
	c := &unsubCase{}
	o := defaultOptions()
	o.Hooks = unsubHooks{outcomes: outcomes, mu: &c.mu, seen: &c.seen}
	o.Authorizers = auth.AuthorizerChain{recordingAuthorizer{mu: &c.mu, topics: &c.authed}}
	c.h = newHarness(t, o, &fakeCore{})
	c.h.connect(v, nil)
	return c
}

func (c *unsubCase) recorded() (seen, authed []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.seen...), append([]string(nil), c.authed...)
}

// TestUnsubscribeInterceptorAtLevel5: a Drop gives that filter its reason on
// the UNSUBACK, 0x80 when the reason is not an error, before authorization
// is asked; the other filters unsubscribe; an invalid filter never reaches
// the interceptor.
func TestUnsubscribeInterceptorAtLevel5(t *testing.T) {
	c := newUnsubCase(t, packet.V5, map[string]hook.Outcome{
		"refused":   {Action: hook.Drop, Reason: packet.NotAuthorized},
		"no-reason": {Action: hook.Drop},
		"success":   {Action: hook.Drop, Reason: packet.NoSubscriptionExisted},
	})
	c.h.send(&packet.Unsubscribe{PacketID: 3, Filters: []string{"ok", "refused", "no-reason", "success", "bad/#/x"}})
	u, ok := c.h.read().(*packet.Unsuback)
	if !ok {
		t.Fatal("no UNSUBACK")
	}
	want := []packet.ReasonCode{packet.Success, packet.NotAuthorized, packet.UnspecifiedError, packet.UnspecifiedError, packet.TopicFilterInvalid}
	if len(u.ReasonCodes) != len(want) {
		t.Fatalf("UNSUBACK reasons %v, want %v", u.ReasonCodes, want)
	}
	for i := range want {
		if u.ReasonCodes[i] != want[i] {
			t.Fatalf("UNSUBACK reasons %v, want %v", u.ReasonCodes, want)
		}
	}
	seen, authed := c.recorded()
	if len(seen) != 4 || seen[0] != "ok" || seen[1] != "refused" || seen[2] != "no-reason" || seen[3] != "success" {
		t.Fatalf("the interceptor saw %q, want the four valid filters", seen)
	}
	if len(authed) != 1 || authed[0] != "ok" {
		t.Fatalf("authorization was asked about %q, want only the filter the interceptor let through", authed)
	}
	c.h.core.mu.Lock()
	unsubs := c.h.core.unsubs
	c.h.core.mu.Unlock()
	if len(unsubs) != 1 || len(unsubs[0].Filters) != 1 || unsubs[0].Filters[0] != "ok" {
		t.Fatalf("the core unsubscribed %+v, want only ok", unsubs)
	}
}

// TestUnsubscribeInterceptorDisconnect: Disconnect closes with its reason.
func TestUnsubscribeInterceptorDisconnect(t *testing.T) {
	c := newUnsubCase(t, packet.V5, map[string]hook.Outcome{"t": {Action: hook.Disconnect, Reason: packet.AdministrativeAction}})
	c.h.send(&packet.Unsubscribe{PacketID: 3, Filters: []string{"t"}})
	d := c.h.readEnd()
	if d == nil || d.ReasonCode != packet.AdministrativeAction {
		t.Fatalf("DISCONNECT %+v, want 0x98", d)
	}
}

// TestUnsubscribeInterceptorDropBelowLevel5: below 5 an UNSUBACK reads as
// success, so a Drop closes the connection with nothing written, and the
// core unsubscribes nothing.
func TestUnsubscribeInterceptorDropBelowLevel5(t *testing.T) {
	for _, v := range []packet.Version{packet.V3, packet.V4} {
		t.Run(v.String(), func(t *testing.T) {
			c := newUnsubCase(t, v, map[string]hook.Outcome{"t": {Action: hook.Drop, Reason: packet.NotAuthorized}})
			c.h.send(&packet.Unsubscribe{PacketID: 3, Filters: []string{"t"}})
			if d := c.h.readEnd(); d != nil {
				t.Fatalf("a dropped UNSUBSCRIBE at %s was answered %+v; want the connection closed", v, d)
			}
			c.h.core.mu.Lock()
			n := len(c.h.core.unsubs)
			c.h.core.mu.Unlock()
			if n != 0 {
				t.Fatalf("the core unsubscribed %d times", n)
			}
		})
	}
}
