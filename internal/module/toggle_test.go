package module

import (
	"context"
	"sync"
	"testing"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/hook"
)

type authzStub struct {
	stub
	decision auth.Decision
}

func (a *authzStub) SetEnabled(b bool) { a.enabled = b }
func (a *authzStub) Authorize(context.Context, auth.Action, string, *auth.Identity) auth.Decision {
	return a.decision
}

// TestToggleWhileTheChainsRun: a module toggled while other goroutines
// authenticate, authorize, look a mechanism up and describe the registry is
// read and written without a data race (run under -race), and the chain
// decides by the last toggle once it settles.
func TestToggleWhileTheChainsRun(t *testing.T) {
	var log []string
	reg(t, "auth.ta", func() Module {
		return &authStub{stub: stub{id: "auth.ta", caps: CapAuthentication, started: &log}, decision: auth.Allow}
	})
	reg(t, "auth.tb", func() Module {
		return &authStub{stub: stub{id: "auth.tb", caps: CapAuthentication, started: &log}, decision: auth.Deny}
	})
	reg(t, "authz.tz", func() Module {
		return &authzStub{stub: stub{id: "authz.tz", caps: CapAuthorization, started: &log}, decision: auth.Allow}
	})
	reg(t, "store.x", func() Module { return &stub{id: "store.x", caps: 0, started: &log} })
	reg(t, "metrics.x", func() Module { return &stub{id: "metrics.x", caps: 0, started: &log} })
	r, problems := New(cfgWith("auth.ta", "auth.tb", "authz.tz", "store.x", "metrics.x"), ctx(), hook.Policy{})
	if len(problems) != 0 {
		t.Fatalf("problems %v", problems)
	}
	authn, authz := r.Authenticators(), r.Authorizers()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				authn.Authenticate(context.Background(), &auth.Request{})
				authz.Authorize(context.Background(), auth.Publish, "t", &auth.Identity{})
				r.Mechanism("SCRAM-SHA-256")
				r.Describe()
			}
		}()
	}
	for i := range 2000 {
		r.SetEnabled("auth.ta", i%2 == 0)
		r.SetEnabled("authz.tz", i%2 == 0)
	}
	close(stop)
	wg.Wait()

	r.SetEnabled("auth.ta", false)
	r.SetEnabled("authz.tz", false)
	if res := authn.Authenticate(context.Background(), &auth.Request{}); res.Decision != auth.Deny {
		t.Fatalf("auth.ta disabled: %v, want the next module's deny", res.Decision)
	}
	if d := authz.Authorize(context.Background(), auth.Publish, "t", &auth.Identity{}); d != auth.Pass {
		t.Fatalf("authz.tz disabled: %v, want pass", d)
	}
	r.SetEnabled("auth.ta", true)
	r.SetEnabled("authz.tz", true)
	if res := authn.Authenticate(context.Background(), &auth.Request{}); res.Decision != auth.Allow {
		t.Fatalf("auth.ta enabled: %v, want allow", res.Decision)
	}
	if d := authz.Authorize(context.Background(), auth.Publish, "t", &auth.Identity{}); d != auth.Allow {
		t.Fatalf("authz.tz enabled: %v, want allow", d)
	}
}
