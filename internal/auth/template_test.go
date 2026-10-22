package auth

import (
	"context"
	"testing"
)

// A template value containing a wildcard or a level separator makes the rule
// unusable for that identity: it authorizes nothing, and in particular not
// another identity's topics. Each rule is first shown to authorize a safe
// identity's own topics, so a Pass below is the rule refusing, not a rule that
// never applied.
func TestTemplateValuesCannotWidenARule(t *testing.T) {
	a, err := LoadACLFile(write(t, "acl.json", `{"rules":[
		{"topic": "devices/${clientid}/#", "allow": ["publish", "subscribe", "unsubscribe"]},
		{"principal": {"clientId": "u-client"}, "topic": "users/${username}/#", "allow": ["publish", "subscribe", "unsubscribe"]},
		{"principal": {"clientId": "g-client"}, "topic": "gw/${cn}/#", "allow": ["publish", "subscribe", "unsubscribe"]},
		{"principal": {"username": "partial"}, "topic": "x/dev-${clientid}/#", "allow": ["publish", "subscribe", "unsubscribe"]}
	]}`))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	actions := []Action{Publish, Subscribe, Unsubscribe}
	templates := []struct {
		name   string
		prefix string
		id     func(v string) *Identity
		safe   string
	}{
		{"clientid", "devices/", func(v string) *Identity { return &Identity{ClientID: v} }, "c1"},
		{"username", "users/", func(v string) *Identity { return &Identity{ClientID: "u-client", Username: v} }, "alice"},
		{"cn", "gw/", func(v string) *Identity { return &Identity{ClientID: "g-client", CertCN: v} }, "gw1"},
	}
	// widened is the filter each value forms when substituted.
	widened := map[string]string{"+": "+/#", "#": "#", "a/b": "a/b/#", "+/#": "+/#"}

	for _, tm := range templates {
		own := tm.id(tm.safe)
		for _, act := range actions {
			req := tm.prefix + tm.safe + "/x"
			if act != Publish {
				req = tm.prefix + tm.safe + "/#"
			}
			if got := a.Authorize(ctx, act, req, own); got != Allow {
				t.Fatalf("%s: safe value %q %s %q = %s, want allow (the rule must be live)", tm.name, tm.safe, act, req, got)
			}
		}
		for v, w := range widened {
			id := tm.id(v)
			reqs := map[Action][]string{
				Publish:     {tm.prefix + "other/x"},
				Subscribe:   {tm.prefix + w, tm.prefix + "other/#"},
				Unsubscribe: {tm.prefix + w, tm.prefix + "other/#"},
			}
			if v == "a/b" {
				// Identity "a"'s topics.
				reqs[Publish] = append(reqs[Publish], tm.prefix+"a/b/x")
			}
			for _, act := range actions {
				for _, req := range reqs[act] {
					if got := a.Authorize(ctx, act, req, id); got != Pass {
						t.Errorf("%s: value %q %s %q = %s, want pass", tm.name, v, act, req, got)
					}
				}
			}
		}
	}

	// A template inside a level: only a separator can widen it there, since
	// '+' and '#' inside a level are not wildcards.
	partial := func(cid string) *Identity { return &Identity{Username: "partial", ClientID: cid} }
	if got := a.Authorize(ctx, Publish, "x/dev-d7/x", partial("d7")); got != Allow {
		t.Fatalf("in-level template: safe value = %s, want allow (the rule must be live)", got)
	}
	for _, c := range []struct {
		act Action
		req string
	}{{Publish, "x/dev-a/b/x"}, {Subscribe, "x/dev-a/b/#"}, {Unsubscribe, "x/dev-a/b/#"}} {
		if got := a.Authorize(ctx, c.act, c.req, partial("a/b")); got != Pass {
			t.Errorf("in-level template: value %q %s %q = %s, want pass", "a/b", c.act, c.req, got)
		}
	}
}

// Each template a pattern uses is checked, and only those: a rule with one
// unusable template is skipped, and a rule without templates still applies.
func TestTemplateUseIsPerTemplate(t *testing.T) {
	a, err := LoadACLFile(write(t, "acl.json", `{"rules":[
		{"principal": {"username": "ops"}, "topic": "#", "allow": ["publish"]},
		{"topic": "t/${username}/${clientid}/#", "allow": ["publish"]}
	]}`))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	cases := []struct {
		id    *Identity
		topic string
		want  Decision
	}{
		{&Identity{Username: "alice", ClientID: "c1"}, "t/alice/c1/x", Allow},
		{&Identity{Username: "alice", ClientID: "+"}, "t/alice/other/x", Pass},
		{&Identity{Username: "+", ClientID: "c1"}, "t/other/c1/x", Pass},
		{&Identity{Username: "ops", ClientID: "+"}, "t/ops/other/x", Allow},
	}
	for _, c := range cases {
		if got := a.Authorize(ctx, Publish, c.topic, c.id); got != c.want {
			t.Errorf("%+v publish %q = %s, want %s", c.id, c.topic, got, c.want)
		}
	}
}

// A templated deny whose value is unusable names no topic of that identity's,
// so it is skipped like a rule with an empty value, and a later rule decides.
func TestTemplatedDenySkippedForUnusableValue(t *testing.T) {
	a, err := LoadACLFile(write(t, "acl.json", `{"rules":[
		{"topic": "devices/${clientid}/cmd", "deny": ["publish"]},
		{"topic": "devices/#", "allow": ["publish"]}
	]}`))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	cases := []struct {
		cid, topic string
		want       Decision
	}{
		{"c1", "devices/c1/cmd", Deny},
		{"c1", "devices/x/cmd", Allow},
		{"a/b", "devices/a/b/cmd", Allow},
		{"+", "devices/x/cmd", Allow},
		{"", "devices/x/cmd", Allow},
	}
	for _, c := range cases {
		if got := a.Authorize(ctx, Publish, c.topic, &Identity{ClientID: c.cid}); got != c.want {
			t.Errorf("client %q publish %q = %s, want %s", c.cid, c.topic, got, c.want)
		}
	}
}

func TestTemplateValueUsable(t *testing.T) {
	for _, v := range []string{"", "+", "#", "/", "a/b", "+/#", "a+", "#b", "/a", "a/"} {
		if templateValueUsable(v) {
			t.Errorf("%q usable, want unusable", v)
		}
	}
	for _, v := range []string{"a", "hoardmq-AbC_-9", "a.b", "a$", "$a", "é"} {
		if !templateValueUsable(v) {
			t.Errorf("%q unusable, want usable", v)
		}
	}
}
