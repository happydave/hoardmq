package auth

import (
	"context"
	"testing"
)

// named answers d with attrs, for the module id.
type named struct {
	id    string
	d     Decision
	attrs map[string]string
}

func (n named) AdmitterID() string { return n.id }
func (n named) Authenticate(context.Context, *Request) Result {
	return Result{Decision: n.d, Attributes: n.attrs}
}

// bare answers d and names no module.
type bare Decision

func (b bare) Authenticate(context.Context, *Request) Result { return Result{Decision: Decision(b)} }

// TestAdmitNamesTheDecider: the chain names the module of the member that
// allowed, and nobody when the members pass, a member denies, or the one
// that allowed names no module. An attribute a member returns, whatever its
// name, does not change it.
func TestAdmitNamesTheDecider(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name  string
		chain AuthenticatorChain
		want  string
	}{
		{"pass, allow, allow", AuthenticatorChain{
			named{id: "auth.a", d: Pass, attrs: map[string]string{"AdmittedBy": "auth.a", "admittedBy": "auth.a"}},
			named{id: "auth.b", d: Allow}, named{id: "auth.c", d: Allow}}, "auth.b"},
		{"all pass", AuthenticatorChain{named{id: "auth.a", d: Pass}, named{id: "auth.b", d: Pass}}, ""},
		{"a deny", AuthenticatorChain{named{id: "auth.a", d: Deny}, named{id: "auth.b", d: Allow}}, ""},
		{"an allow naming no module", AuthenticatorChain{bare(Allow), named{id: "auth.b", d: Allow}}, ""},
		{"empty", AuthenticatorChain{}, ""},
	} {
		r, by := tc.chain.Admit(ctx, &Request{})
		if by != tc.want {
			t.Errorf("%s: admitted by %q, want %q", tc.name, by, tc.want)
		}
		if a := tc.chain.Authenticate(ctx, &Request{}); a.Decision != r.Decision {
			t.Errorf("%s: Authenticate %v, Admit %v", tc.name, a.Decision, r.Decision)
		}
	}
}

// overwriting sets AdmittedBy and Username on the identity it is handed, and
// records the AdmittedBy it saw.
type overwriting struct{ saw *[]string }

func (o overwriting) Authorize(_ context.Context, _ Action, _ string, id *Identity) Decision {
	*o.saw = append(*o.saw, id.AdmittedBy)
	id.AdmittedBy, id.Username = "auth.http", "root"
	return Pass
}

// TestAuthorizersGetTheirOwnCopy: a member that sets AdmittedBy on the
// identity it is handed changes it for neither the next member nor the
// caller.
func TestAuthorizersGetTheirOwnCopy(t *testing.T) {
	var saw []string
	chain := AuthorizerChain{overwriting{&saw}, overwriting{&saw}}
	id := &Identity{Username: "u", AdmittedBy: "auth.file"}
	if d := chain.Authorize(context.Background(), Publish, "t", id); d != Pass {
		t.Fatalf("decision %v", d)
	}
	if len(saw) != 2 || saw[0] != "auth.file" || saw[1] != "auth.file" {
		t.Fatalf("the members saw %q, want auth.file twice", saw)
	}
	if id.AdmittedBy != "auth.file" || id.Username != "u" {
		t.Fatalf("the caller's identity became %+v", *id)
	}
}
