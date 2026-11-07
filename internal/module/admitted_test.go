package module

import (
	"context"
	"testing"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/hook"
)

// TestChainsAndMechanismsNameTheModule: the registry's authentication chain
// names the module as registered when it admits, a disabled module passes and
// is never named, and the mechanism the lookup returns names its module.
func TestChainsAndMechanismsNameTheModule(t *testing.T) {
	var log []string
	reg(t, "auth.na", func() Module {
		return &authStub{stub: stub{id: "auth.na", caps: CapAuthentication, started: &log}, decision: auth.Allow}
	})
	reg(t, "auth.nb", func() Module {
		return &authStub{stub: stub{id: "auth.nb", caps: CapAuthentication, started: &log}, decision: auth.Allow}
	})
	reg(t, "auth.nmech", func() Module {
		return &mechStub{stub: stub{id: "auth.nmech", caps: CapMechanism, started: &log}, method: "N-TEST"}
	})
	reg(t, "store.n", func() Module { return &stub{id: "store.n", started: &log} })
	reg(t, "metrics.n", func() Module { return &stub{id: "metrics.n", started: &log} })
	r, problems := New(cfgWith("auth.na", "auth.nb", "auth.nmech", "store.n", "metrics.n"), ctx(), hook.Policy{})
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	if res, by := r.Authenticators().Admit(context.Background(), &auth.Request{}); res.Decision != auth.Allow || by != "auth.na" {
		t.Fatalf("admitted %v by %q, want allow by auth.na", res.Decision, by)
	}
	if !r.SetEnabled("auth.na", false) {
		t.Fatal("toggle refused")
	}
	if _, by := r.Authenticators().Admit(context.Background(), &auth.Request{}); by != "auth.nb" {
		t.Fatalf("with auth.na disabled, admitted by %q, want auth.nb", by)
	}
	mech, ok := r.Mechanism("N-TEST")
	if !ok {
		t.Fatal("N-TEST not found")
	}
	if by := auth.AdmitterOf(mech); by != "auth.nmech" {
		t.Fatalf("the mechanism names %q, want auth.nmech", by)
	}
	if got := mech.Methods(); len(got) != 1 || got[0] != "N-TEST" {
		t.Fatalf("the named mechanism's methods %v", got)
	}
}
