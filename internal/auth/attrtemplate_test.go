package auth

import (
	"context"
	"strings"
	"testing"
)

// TestAttributeTemplatesFillARuleTopic: ${attr.<name>} stands for the
// identity's attribute, so a fleet whose topics follow attributes, not the
// username, is one rule. The identity's own topics are allowed; another
// tenant's are not; and an attribute that is missing, empty or holds a
// wildcard or level separator makes the rule unusable, so the later rule
// decides.
func TestAttributeTemplatesFillARuleTopic(t *testing.T) {
	a, err := LoadACLFile(write(t, "acl.json", `{"rules":[
		{"topic": "${attr.tenant}/v1/${attr.system}/#", "allow": ["publish", "subscribe"]},
		{"topic": "#", "deny": ["publish", "subscribe"]}
	]}`))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	id := func(attrs map[string]string) *Identity {
		return &Identity{ClientID: "c1", Username: "u", Attributes: attrs}
	}
	own := id(map[string]string{"tenant": "acme", "system": "s7"})
	if got := a.Authorize(ctx, Publish, "acme/v1/s7/temp", own); got != Allow {
		t.Fatalf("its own topic: %s, want allow", got)
	}
	if got := a.Authorize(ctx, Subscribe, "acme/v1/s7/#", own); got != Allow {
		t.Fatalf("its own filter: %s, want allow", got)
	}
	if got := a.Authorize(ctx, Publish, "other/v1/s7/temp", own); got != Deny {
		t.Fatalf("another tenant's topic: %s, want the later rule's deny", got)
	}
	for name, attrs := range map[string]map[string]string{
		"no attributes":     nil,
		"one missing":       {"tenant": "acme"},
		"empty":             {"tenant": "", "system": "s7"},
		"a wildcard +":      {"tenant": "+", "system": "s7"},
		"a wildcard #":      {"tenant": "acme", "system": "#"},
		"a level separator": {"tenant": "acme/v1/s7", "system": "s7"},
	} {
		v := id(attrs)
		for _, req := range []string{"acme/v1/s7/temp", "+/v1/s7/temp", "acme/v1/s7/v1/s7/temp"} {
			if got := a.Authorize(ctx, Publish, req, v); got != Deny {
				t.Errorf("%s: publish %q = %s, want the later rule's deny", name, req, got)
			}
		}
	}
}

// TestATemplateValueIsNotReadAsATemplate: a username that looks like an
// attribute template stays literal text.
func TestATemplateValueIsNotReadAsATemplate(t *testing.T) {
	a, err := LoadACLFile(write(t, "acl.json", `{"rules":[{"topic": "users/${username}/#", "allow": ["publish"]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	v := &Identity{ClientID: "c1", Username: "${attr.tenant}", Attributes: map[string]string{"tenant": "acme"}}
	if got := a.Authorize(context.Background(), Publish, "users/acme/x", v); got != Pass {
		t.Fatalf("the username's text was expanded: %s", got)
	}
	if got := a.Authorize(context.Background(), Publish, "users/${attr.tenant}/x", v); got != Allow {
		t.Fatalf("the username as literal text: %s, want allow", got)
	}
}

// TestMalformedAttributeTemplatesAreRefused: a ${attr. that does not form a
// template is a load error, not literal text in the filter.
func TestMalformedAttributeTemplatesAreRefused(t *testing.T) {
	for _, topic := range []string{"${attr.}/#", "${attr.a b}/#", "x/${attr.team"} {
		_, err := LoadACLFile(write(t, "acl.json", `{"rules":[{"topic": "`+topic+`", "allow": ["publish"]}]}`))
		if err == nil || !strings.Contains(err.Error(), "attribute template") {
			t.Errorf("%q: %v, want refused", topic, err)
		}
	}
	if _, err := LoadACLFile(write(t, "acl.json", `{"rules":[{"topic": "${attr.auth.http-x_1}/#", "allow": ["publish"]}]}`)); err != nil {
		t.Errorf("a name with '.', '-' and '_': %v", err)
	}
}
