package module

import (
	"context"
	"strings"
	"testing"

	"github.com/happydave/hoardmq/internal/hook"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/packet"
)

// publishHooks implements both publish hooks, records its calls and refuses
// every admission when refuse is set.
type publishHooks struct {
	stub
	refuse bool
	calls  *[]string
}

func (p *publishHooks) SetEnabled(b bool) { p.enabled = b }
func (p *publishHooks) OnPublishAccepted(_ context.Context, source hook.PublishSource, m *message.Message) {
	*p.calls = append(*p.calls, "accepted "+string(p.id)+" "+source.String()+" "+m.Topic)
}
func (p *publishHooks) AdmitPublish(context.Context, hook.Info, *message.Message) hook.Outcome {
	*p.calls = append(*p.calls, "admit "+string(p.id))
	if p.refuse {
		return hook.Outcome{Action: hook.Drop, Reason: packet.QuotaExceeded}
	}
	return hook.OK
}

// acceptedOnly and admissionOnly implement one hook each.
type acceptedOnly struct{ stub }

func (*acceptedOnly) OnPublishAccepted(context.Context, hook.PublishSource, *message.Message) {}

type admissionOnly struct{ stub }

func (*admissionOnly) AdmitPublish(context.Context, hook.Info, *message.Message) hook.Outcome {
	return hook.OK
}

// TestPublishHookCapabilities: each publish hook's bit and method go
// together, both ways.
func TestPublishHookCapabilities(t *testing.T) {
	var log []string
	reg(t, "hook.pa1", func() Module { return &stub{id: "hook.pa1", caps: CapPublishAccepted, started: &log} })
	reg(t, "hook.pa2", func() Module { return &acceptedOnly{stub{id: "hook.pa2", started: &log}} })
	reg(t, "hook.pd1", func() Module { return &stub{id: "hook.pd1", caps: CapPublishAdmission, started: &log} })
	reg(t, "hook.pd2", func() Module { return &admissionOnly{stub{id: "hook.pd2", started: &log}} })
	reg(t, "hook.pok", func() Module {
		return &publishHooks{stub: stub{id: "hook.pok", caps: CapPublishAccepted | CapPublishAdmission, started: &log}, calls: &log}
	})
	reg(t, "store.ph", func() Module { return &stub{id: "store.ph", started: &log} })
	reg(t, "metrics.ph", func() Module { return &stub{id: "metrics.ph", started: &log} })
	want := map[string]string{
		"hook.pa1": "declares the publish-accepted capability without implementing it",
		"hook.pa2": "implements the publish-accepted hooks without declaring the capability",
		"hook.pd1": "declares the publish-admission capability without implementing it",
		"hook.pd2": "implements the publish-admission hooks without declaring the capability",
	}
	for id, msg := range want {
		_, problems := New(cfgWith(id, "store.ph", "metrics.ph"), ctx(), hook.Policy{})
		if len(problems) != 1 || problems[0].String() != "/modules/"+id+": "+msg {
			t.Errorf("%s: %v, want %q", id, problems, msg)
		}
	}
	if r, problems := New(cfgWith("hook.pok", "store.ph", "metrics.ph"), ctx(), hook.Policy{}); r == nil || len(problems) != 0 {
		t.Fatalf("a module declaring and implementing both: %v", problems)
	}
}

// TestPublishHooksFollowTheOrderAndTheToggle: the admission chain stops at the
// first refusal in the module order, and a disabled module's hooks are not
// called.
func TestPublishHooksFollowTheOrderAndTheToggle(t *testing.T) {
	var calls, log []string
	reg(t, "hook.pza", func() Module {
		return &publishHooks{stub: stub{id: "hook.pza", caps: CapPublishAccepted | CapPublishAdmission, started: &log}, refuse: true, calls: &calls}
	})
	reg(t, "hook.pab", func() Module {
		return &publishHooks{stub: stub{id: "hook.pab", caps: CapPublishAccepted | CapPublishAdmission, started: &log}, calls: &calls}
	})
	reg(t, "store.po", func() Module { return &stub{id: "store.po", started: &log} })
	reg(t, "metrics.po", func() Module { return &stub{id: "metrics.po", started: &log} })
	msg := &message.Message{Topic: "t"}
	for _, c := range []struct {
		order []string
		want  string
	}{
		{nil, "admit hook.pab; admit hook.pza"},
		{[]string{"hook.pza"}, "admit hook.pza"},
	} {
		doc := cfgWith("hook.pza", "hook.pab", "store.po", "metrics.po")
		doc.Hooks.Order = c.order
		r, problems := New(doc, ctx(), hook.Policy{})
		if len(problems) != 0 {
			t.Fatal(problems)
		}
		calls = calls[:0]
		out := r.Hooks().AdmitPublish(context.Background(), hook.Info{}, msg)
		if out.Action != hook.Drop || out.Reason != packet.QuotaExceeded {
			t.Errorf("order %q: outcome %+v, want the refusal", c.order, out)
		}
		if got := strings.Join(calls, "; "); got != c.want {
			t.Errorf("order %q: calls %q, want %q", c.order, got, c.want)
		}
		if c.order != nil {
			continue
		}
		calls = calls[:0]
		r.Hooks().OnPublishAccepted(context.Background(), hook.SourceWill, msg)
		if got := strings.Join(calls, "; "); got != "accepted hook.pab will t; accepted hook.pza will t" {
			t.Errorf("accepted calls %q", got)
		}
		if !r.SetEnabled("hook.pza", false) {
			t.Fatal("toggle refused")
		}
		calls = calls[:0]
		if out := r.Hooks().AdmitPublish(context.Background(), hook.Info{}, msg); out.Action != hook.Continue {
			t.Errorf("with the refusing module disabled: %+v", out)
		}
		r.Hooks().OnPublishAccepted(context.Background(), hook.SourceClient, msg)
		if got := strings.Join(calls, "; "); got != "admit hook.pab; accepted hook.pab client t" {
			t.Errorf("a disabled module was called: %q", got)
		}
	}
}
