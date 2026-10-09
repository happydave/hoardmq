package module

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/hook"
	"github.com/happydave/hoardmq/internal/packet"
)

// validating is a module with a Validate step that records its calls.
type validating struct {
	stub
	err       error
	panics    bool
	validated *int
}

func (v *validating) Validate() error {
	*v.validated++
	if v.panics {
		panic("validator broke")
	}
	return v.err
}

// TestValidatorRunsAfterProvision: Validate runs once a module's Provision has
// succeeded, its error is a problem at the module's pointer beside every
// other problem, a panic is reported rather than raised, and a module whose
// Provision failed is not validated.
func TestValidatorRunsAfterProvision(t *testing.T) {
	var log []string
	var refused, failed, panicked, fine int
	reg(t, "hook.vrefuse", func() Module {
		return &validating{stub: stub{id: "hook.vrefuse", started: &log}, err: errors.New("log is required"), validated: &refused}
	})
	reg(t, "hook.vfail", func() Module {
		return &validating{stub: stub{id: "hook.vfail", started: &log, fail: true}, validated: &failed}
	})
	reg(t, "hook.vpanic", func() Module {
		return &validating{stub: stub{id: "hook.vpanic", started: &log}, panics: true, validated: &panicked}
	})
	reg(t, "hook.vfine", func() Module {
		return &validating{stub: stub{id: "hook.vfine", started: &log}, validated: &fine}
	})
	reg(t, "store.val", func() Module { return &stub{id: "store.val", started: &log} })
	reg(t, "metrics.val", func() Module { return &stub{id: "metrics.val", started: &log} })

	_, problems := New(cfgWith("hook.vrefuse", "hook.vfail", "hook.vpanic", "hook.vfine", "store.val", "metrics.val"), ctx(), hook.Policy{})
	got := map[string]string{}
	for _, p := range problems {
		got[p.Pointer] = p.Message
	}
	want := map[string]string{
		"/modules/hook.vrefuse": "log is required",
		"/modules/hook.vfail":   "bad config",
		"/modules/hook.vpanic":  "validate panicked: validator broke",
	}
	if len(got) != len(want) {
		t.Fatalf("problems %v, want %v", problems, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s: %q, want %q", k, got[k], v)
		}
	}
	if refused != 1 || panicked != 1 || fine != 1 || failed != 0 {
		t.Errorf("validated refuse %d, panic %d, fine %d, failed-provision %d; want 1, 1, 1, 0", refused, panicked, fine, failed)
	}

	r, problems := New(cfgWith("hook.vfine", "store.val", "metrics.val"), ctx(), hook.Policy{})
	if len(problems) != 0 || r == nil {
		t.Fatalf("a module that validates is refused: %v", problems)
	}
}

// TestOrderDecidesAuthentication: the first authenticator to decide wins, so
// the order decides between two that disagree. By identifier auth.ab (deny)
// comes first; hooks.order puts auth.za (allow) first.
func TestOrderDecidesAuthentication(t *testing.T) {
	var log []string
	reg(t, "auth.za", func() Module {
		return &authStub{stub: stub{id: "auth.za", caps: CapAuthentication, started: &log}, decision: auth.Allow}
	})
	reg(t, "auth.ab", func() Module {
		return &authStub{stub: stub{id: "auth.ab", caps: CapAuthentication, started: &log}, decision: auth.Deny}
	})
	reg(t, "store.oauth", func() Module { return &stub{id: "store.oauth", started: &log} })
	reg(t, "metrics.oauth", func() Module { return &stub{id: "metrics.oauth", started: &log} })

	for _, c := range []struct {
		order []string
		want  auth.Decision
	}{
		{nil, auth.Deny},
		{[]string{"auth.za"}, auth.Allow},
	} {
		doc := cfgWith("auth.za", "auth.ab", "store.oauth", "metrics.oauth")
		doc.Hooks.Order = c.order
		r, problems := New(doc, ctx(), hook.Policy{})
		if len(problems) != 0 {
			t.Fatal(problems)
		}
		if got := r.Authenticators().Authenticate(context.Background(), &auth.Request{Username: "u", HasUsername: true}).Decision; got != c.want {
			t.Errorf("order %q: %v, want %v", c.order, got, c.want)
		}
	}
}

// orderedInbound records that it was called and returns its outcome.
type orderedInbound struct {
	stub
	outcome hook.Outcome
	calls   *[]string
}

func (o *orderedInbound) InterceptInbound(context.Context, hook.Info, packet.Packet) hook.Outcome {
	*o.calls = append(*o.calls, string(o.id))
	return o.outcome
}

// TestOrderDecidesInterceptorsAndLifecycle: an interceptor chain stops at its
// first Drop, so with hook.zz ordered first hook.aa is never called; start
// follows the order and stop reverses it.
func TestOrderDecidesInterceptorsAndLifecycle(t *testing.T) {
	var life, calls []string
	reg(t, "hook.aa", func() Module {
		return &orderedInbound{stub: stub{id: "hook.aa", caps: CapInbound, started: &life}, outcome: hook.OK, calls: &calls}
	})
	reg(t, "hook.zz", func() Module {
		return &orderedInbound{stub: stub{id: "hook.zz", caps: CapInbound, started: &life}, outcome: hook.Outcome{Action: hook.Drop}, calls: &calls}
	})
	reg(t, "store.olc", func() Module { return &stub{id: "store.olc", started: &life} })
	reg(t, "metrics.olc", func() Module { return &stub{id: "metrics.olc", started: &life} })

	for _, c := range []struct {
		order     []string
		calls     string
		lifecycle string
	}{
		{nil, "hook.aa hook.zz", "start hook.aa,start hook.zz,start metrics.olc,start store.olc,stop store.olc,stop metrics.olc,stop hook.zz,stop hook.aa"},
		{[]string{"hook.zz"}, "hook.zz", "start hook.zz,start hook.aa,start metrics.olc,start store.olc,stop store.olc,stop metrics.olc,stop hook.aa,stop hook.zz"},
	} {
		life, calls = nil, nil
		doc := cfgWith("hook.aa", "hook.zz", "store.olc", "metrics.olc")
		doc.Hooks.Order = c.order
		r, problems := New(doc, ctx(), hook.Policy{})
		if len(problems) != 0 {
			t.Fatal(problems)
		}
		if out := r.Hooks().Inbound(context.Background(), hook.Info{}, &packet.Publish{}); out.Action != hook.Drop {
			t.Errorf("order %q: outcome %v, want drop", c.order, out.Action)
		}
		if got := strings.Join(calls, " "); got != c.calls {
			t.Errorf("order %q: called %q, want %q", c.order, got, c.calls)
		}
		if err := r.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := r.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got := strings.Join(life, ","); got != c.lifecycle {
			t.Errorf("order %q: lifecycle %q, want %q", c.order, got, c.lifecycle)
		}
	}
}

// TestOrderProblems: an entry naming no module the registry instantiates, and
// a repeated entry, are each a problem at their index.
func TestOrderProblems(t *testing.T) {
	var log []string
	reg(t, "auth.za", func() Module {
		return &authStub{stub: stub{id: "auth.za", caps: CapAuthentication, started: &log}, decision: auth.Allow}
	})
	reg(t, "store.oprob", func() Module { return &stub{id: "store.oprob", started: &log} })
	reg(t, "metrics.oprob", func() Module { return &stub{id: "metrics.oprob", started: &log} })
	for _, c := range []struct {
		order   []string
		pointer string
		message string
	}{
		{[]string{"auth.nope"}, "/hooks/order/0", "not an instantiated module"},
		{[]string{"auth.za", "auth.za"}, "/hooks/order/1", "repeats auth.za"},
	} {
		doc := cfgWith("auth.za", "store.oprob", "metrics.oprob")
		doc.Hooks.Order = c.order
		r, problems := New(doc, ctx(), hook.Policy{})
		if r != nil || len(problems) != 1 || problems[0].Pointer != c.pointer || problems[0].Message != c.message {
			t.Errorf("order %q: problems %v, want %s: %s", c.order, problems, c.pointer, c.message)
		}
	}
}
