package module

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/hook"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/session"
	"github.com/happydave/hoardmq/internal/store"
)

// Minimal modules for registry tests; the broker tests exercise the full
// families through the shared test module.
type stub struct {
	id      ID
	caps    Capability
	fail    bool
	started *[]string
	enabled bool
}

func (s *stub) ID() ID                   { return s.id }
func (s *stub) Capabilities() Capability { return s.caps }
func (s *stub) Provision(_ *Context, raw json.RawMessage) error {
	if s.fail {
		return errors.New("bad config")
	}
	s.enabled = true
	return nil
}
func (s *stub) Start(context.Context) error {
	*s.started = append(*s.started, "start "+string(s.id))
	return nil
}
func (s *stub) Stop(context.Context) error {
	*s.started = append(*s.started, "stop "+string(s.id))
	return nil
}

type authStub struct {
	stub
	decision auth.Decision
}

func (a *authStub) SetEnabled(b bool) { a.enabled = b }
func (a *authStub) Authenticate(context.Context, *auth.Request) auth.Result {
	return auth.Result{Decision: a.decision}
}

type inboundStub struct {
	stub
	sleep time.Duration
	panic bool
}

func (i *inboundStub) InterceptInbound(context.Context, hook.Info, packet.Packet) hook.Outcome {
	if i.sleep > 0 {
		time.Sleep(i.sleep)
	}
	if i.panic {
		panic("boom")
	}
	return hook.Outcome{Action: hook.Drop}
}

// A store stub that is also toggleable, which the registry must refuse.
type badStore struct{ stub }

func (badStore) SetEnabled(bool)                  {}
func (badStore) Journal() session.Journal         { return nil }
func (badStore) Recover() (*store.State, error)   { return store.NewState(), nil }
func (badStore) Checkpoint(store.Snapshot) error  { return nil }
func (badStore) NeedsCheckpoint() bool            { return false }
func (badStore) Close() error                     { return nil }
func (badStore) SetTap(func(store.Record) func()) {}

// A store stub with every method a store provider had before the tap was
// required, and not the tap.
type untappedStore struct{ stub }

func (untappedStore) Journal() session.Journal        { return nil }
func (untappedStore) Recover() (*store.State, error)  { return store.NewState(), nil }
func (untappedStore) Checkpoint(store.Snapshot) error { return nil }
func (untappedStore) NeedsCheckpoint() bool           { return false }
func (untappedStore) Close() error                    { return nil }

// TestStoreProviderNeedsTheTap: a store provider that cannot feed
// replication is refused at provision, so no node runs on it.
func TestStoreProviderNeedsTheTap(t *testing.T) {
	var log []string
	reg(t, "store.untapped", func() Module { return &untappedStore{stub{id: "store.untapped", caps: CapStore, started: &log}} })
	reg(t, "metrics.tap", func() Module { return &stub{id: "metrics.tap", caps: 0, started: &log} })
	r, problems := New(cfgWith("store.untapped", "metrics.tap"), ctx(), hook.Policy{})
	if r != nil || len(problems) != 1 || problems[0].String() != "/modules/store.untapped: declares the store capability without implementing it" {
		t.Fatalf("an untapped store provider: %v", problems)
	}
}

var registered = map[ID]bool{}

func reg(t *testing.T, id ID, f Factory) {
	t.Helper()
	if !registered[id] {
		Register(id, f)
		registered[id] = true
	}
}

func cfgWith(ids ...string) config.Document {
	doc := config.Default()
	doc.Modules = map[string]json.RawMessage{}
	for _, id := range ids {
		doc.Modules[id] = json.RawMessage(`{}`)
	}
	return doc
}

func ctx() *Context {
	return &Context{Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Namespace: "default"}
}

func TestRegistryProvisionAndOrder(t *testing.T) {
	var log []string
	reg(t, "auth.a", func() Module {
		return &authStub{stub: stub{id: "auth.a", caps: CapAuthentication, started: &log}, decision: auth.Allow}
	})
	reg(t, "auth.b", func() Module {
		return &authStub{stub: stub{id: "auth.b", caps: CapAuthentication, started: &log}, decision: auth.Deny}
	})
	reg(t, "store.x", func() Module { return &stub{id: "store.x", caps: 0, started: &log} })
	reg(t, "metrics.x", func() Module { return &stub{id: "metrics.x", caps: 0, started: &log} })

	r, problems := New(cfgWith("auth.b", "auth.a", "store.x", "metrics.x"), ctx(), hook.Policy{})
	if len(problems) != 0 {
		t.Fatalf("problems %v", problems)
	}
	if got := strings.Join(idStrings(r.Modules()), ","); got != "auth.a,auth.b,metrics.x,store.x" {
		t.Fatalf("order %s", got)
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := r.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(log, ","); got != "start auth.a,start auth.b,start metrics.x,start store.x,stop store.x,stop metrics.x,stop auth.b,stop auth.a" {
		t.Fatalf("lifecycle %s", got)
	}
	// The chain is in module order: a allows before b denies.
	if res := r.Authenticators().Authenticate(context.Background(), &auth.Request{}); res.Decision != auth.Allow {
		t.Fatalf("chain %v", res.Decision)
	}
	// Disabling a passes to the next: b denies.
	if !r.SetEnabled("auth.a", false) {
		t.Fatal("SetEnabled failed")
	}
	if res := r.Authenticators().Authenticate(context.Background(), &auth.Request{}); res.Decision != auth.Deny {
		t.Fatalf("after disable %v", res.Decision)
	}
	if r.SetEnabled("store.x", false) {
		t.Fatal("non-toggleable module toggled")
	}
	if r.SetEnabled("nope.x", false) {
		t.Fatal("unknown module toggled")
	}
}

func idStrings(ids []ID) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = string(id)
	}
	return out
}

func TestRegistryProblems(t *testing.T) {
	var log []string
	reg(t, "auth.fail", func() Module { return &stub{id: "auth.fail", caps: 0, fail: true, started: &log} })
	reg(t, "auth.mismatch", func() Module { return &stub{id: "auth.mismatch", caps: CapAuthentication, started: &log} })
	reg(t, "hook.undeclared", func() Module { return &inboundStub{stub: stub{id: "hook.undeclared", caps: 0, started: &log}} })
	reg(t, "store.toggle", func() Module { return &badStore{stub{id: "store.toggle", caps: CapStore, started: &log}} })
	reg(t, "store.x", func() Module { return &stub{id: "store.x", caps: 0, started: &log} })
	reg(t, "metrics.x", func() Module { return &stub{id: "metrics.x", caps: 0, started: &log} })

	_, problems := New(cfgWith("auth.fail", "auth.mismatch", "hook.undeclared", "nope.unknown", "store.x", "metrics.x"), ctx(), hook.Policy{})
	var lines []string
	for _, p := range problems {
		lines = append(lines, p.String())
	}
	got := strings.Join(lines, "\n")
	for _, want := range []string{
		"/modules/auth.fail: bad config",
		"/modules/auth.mismatch: declares the authentication capability without implementing it",
		"/modules/hook.undeclared: implements the inbound hooks without declaring the capability",
		"/modules/nope.unknown: unknown module",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in\n%s", want, got)
		}
	}
	if len(problems) != 4 {
		t.Errorf("%d problems:\n%s", len(problems), got)
	}
	// A toggleable store provider is refused, and two store providers too.
	_, problems = New(cfgWith("store.toggle", "store.x", "metrics.x"), ctx(), hook.Policy{})
	got = ""
	for _, p := range problems {
		got += p.String() + "\n"
	}
	if !strings.Contains(got, "a store provider must not be toggleable") {
		t.Errorf("toggleable store accepted:\n%s", got)
	}
}

func TestDefaultsWhenFamilyAbsent(t *testing.T) {
	var log []string
	reg(t, "store.x", func() Module { return &stub{id: "store.x", caps: 0, started: &log} })
	reg(t, "metrics.x", func() Module { return &stub{id: "metrics.x", caps: 0, started: &log} })
	// Defaults name store.wal and metrics.internal, which are not registered
	// in this package's tests: their absence must surface as unknown.
	_, problems := New(cfgWith(), ctx(), hook.Policy{})
	if len(problems) != 2 {
		t.Fatalf("expected the two defaults to be reported unknown here, got %v", problems)
	}
	r, problems := New(cfgWith("store.x", "metrics.x"), ctx(), hook.Policy{})
	if len(problems) != 0 || len(r.Modules()) != 2 {
		t.Fatalf("%v %v", problems, r.Modules())
	}
}

func TestHookTimeoutFallbackAndPanic(t *testing.T) {
	var log []string
	reg(t, "hook.slow", func() Module {
		return &inboundStub{stub: stub{id: "hook.slow", caps: CapInbound, started: &log}, sleep: 50 * time.Millisecond}
	})
	reg(t, "hook.panic", func() Module {
		return &inboundStub{stub: stub{id: "hook.panic", caps: CapInbound, started: &log}, panic: true}
	})
	reg(t, "store.x", func() Module { return &stub{id: "store.x", caps: 0, started: &log} })
	reg(t, "metrics.x", func() Module { return &stub{id: "metrics.x", caps: 0, started: &log} })
	info := hook.Info{ClientID: "c"}
	pub := &packet.Publish{Topic: "t"}

	// No timeout: the slow hook's Drop is honoured.
	r, _ := New(cfgWith("hook.slow", "store.x", "metrics.x"), ctx(), hook.Policy{})
	if out := r.Hooks().Inbound(context.Background(), info, pub); out.Action != hook.Drop {
		t.Fatalf("inline: %v", out)
	}
	// A 10 ms timeout with the default fallback: continue, and a failure counted.
	r, _ = New(cfgWith("hook.slow", "store.x", "metrics.x"), ctx(), hook.Policy{Timeout: 10 * time.Millisecond})
	if out := r.Hooks().Inbound(context.Background(), info, pub); out.Action != hook.Continue {
		t.Fatalf("timeout fallback continue: %v", out)
	}
	if r.Hooks().Failures()["hook.slow"] != 1 {
		t.Fatalf("failures %v", r.Hooks().Failures())
	}
	// Fallback disconnect.
	r, _ = New(cfgWith("hook.slow", "store.x", "metrics.x"), ctx(), hook.Policy{Timeout: 10 * time.Millisecond, Fallback: hook.Disconnect})
	if out := r.Hooks().Inbound(context.Background(), info, pub); out.Action != hook.Disconnect || out.Reason != packet.ImplementationSpecificError {
		t.Fatalf("timeout fallback disconnect: %v", out)
	}
	// A panic is recovered and treated as the fallback, inline and with a timeout.
	for _, pol := range []hook.Policy{{}, {Timeout: time.Second}} {
		r, _ = New(cfgWith("hook.panic", "store.x", "metrics.x"), ctx(), pol)
		if out := r.Hooks().Inbound(context.Background(), info, pub); out.Action != hook.Continue {
			t.Fatalf("panic fallback: %v", out)
		}
		if r.Hooks().Failures()["hook.panic"] != 1 {
			t.Fatalf("panic not counted: %v", r.Hooks().Failures())
		}
	}
	if r.Hooks().Empty() {
		t.Fatal("Empty with an interceptor present")
	}
}

func TestIDFamilyAndKnown(t *testing.T) {
	if ID("auth.file").Family() != "auth" || ID("plain").Family() != "plain" {
		t.Error("Family")
	}
	if len(Known()) == 0 {
		t.Error("Known is empty")
	}
	if !CapInbound.Has(CapInbound) || (CapInbound | CapOutbound).Has(CapStore) {
		t.Error("Has")
	}
}

// certStub is a certificate authenticator.
type certStub struct{ authStub }

func (*certStub) ChecksCertificates() {}

// mechStub provides a method.
type mechStub struct {
	stub
	method string
}

func (m *mechStub) SetEnabled(b bool) { m.enabled = b }
func (m *mechStub) Methods() []string { return []string{m.method} }
func (m *mechStub) Begin(context.Context, string, *auth.Request) auth.Exchange {
	return nil
}

// Certificate authenticators are their own chain, run first by the
// connection; the other chain leaves them out, whatever their identifiers.
func TestCertificateAuthenticatorsApart(t *testing.T) {
	var log []string
	reg(t, "auth.pw", func() Module {
		return &authStub{stub: stub{id: "auth.pw", caps: CapAuthentication, started: &log}, decision: auth.Allow}
	})
	reg(t, "auth.zcert", func() Module {
		return &certStub{authStub{stub: stub{id: "auth.zcert", caps: CapAuthentication | CapCertificate, started: &log}, decision: auth.Deny}}
	})
	reg(t, "store.x", func() Module { return &stub{id: "store.x", caps: 0, started: &log} })
	reg(t, "metrics.x", func() Module { return &stub{id: "metrics.x", caps: 0, started: &log} })
	r, problems := New(cfgWith("auth.pw", "auth.zcert", "store.x", "metrics.x"), ctx(), hook.Policy{})
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	if n := len(r.CertificateAuthenticators()); n != 1 {
		t.Fatalf("%d certificate authenticators", n)
	}
	if res := r.CertificateAuthenticators().Authenticate(context.Background(), &auth.Request{}); res.Decision != auth.Deny {
		t.Fatalf("the certificate chain: %v", res.Decision)
	}
	if n := len(r.Authenticators()); n != 1 {
		t.Fatalf("%d in the chain", n)
	}
	if res := r.Authenticators().Authenticate(context.Background(), &auth.Request{}); res.Decision != auth.Allow {
		t.Fatalf("the chain: %v", res.Decision)
	}
}

func TestMechanismCapability(t *testing.T) {
	var log []string
	reg(t, "auth.mech", func() Module {
		return &mechStub{stub: stub{id: "auth.mech", caps: CapMechanism, started: &log}, method: "X-TEST"}
	})
	reg(t, "auth.mechundeclared", func() Module {
		return &mechStub{stub: stub{id: "auth.mechundeclared", caps: 0, started: &log}, method: "Y"}
	})
	reg(t, "auth.mechmissing", func() Module { return &stub{id: "auth.mechmissing", caps: CapMechanism, started: &log} })
	reg(t, "auth.certmissing", func() Module {
		return &authStub{stub: stub{id: "auth.certmissing", caps: CapAuthentication | CapCertificate, started: &log}}
	})
	reg(t, "store.x", func() Module { return &stub{id: "store.x", caps: 0, started: &log} })
	reg(t, "metrics.x", func() Module { return &stub{id: "metrics.x", caps: 0, started: &log} })
	_, problems := New(cfgWith("auth.mechundeclared", "auth.mechmissing", "auth.certmissing", "store.x", "metrics.x"), ctx(), hook.Policy{})
	got := ""
	for _, p := range problems {
		got += p.String() + "\n"
	}
	for _, want := range []string{
		"/modules/auth.mechundeclared: implements the mechanism hooks without declaring the capability",
		"/modules/auth.mechmissing: declares the mechanism capability without implementing it",
		"/modules/auth.certmissing: declares the certificate capability without implementing it",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in\n%s", want, got)
		}
	}
	r, problems := New(cfgWith("auth.mech", "store.x", "metrics.x"), ctx(), hook.Policy{})
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	if _, ok := r.Mechanism("X-TEST"); !ok {
		t.Fatal("X-TEST not found")
	}
	if _, ok := r.Mechanism("Y"); ok {
		t.Fatal("an unknown method found")
	}
	r.SetEnabled("auth.mech", false)
	if _, ok := r.Mechanism("X-TEST"); ok {
		t.Fatal("a disabled module's method found")
	}
}

type reloadStub struct {
	stub
	reloads *int
	err     error
}

func (r *reloadStub) Reload() error {
	*r.reloads++
	return r.err
}

// Reload asks each module that re-reads its files, and names each that
// failed; a module that does not re-read is not asked.
func TestRegistryReload(t *testing.T) {
	var log []string
	var good, bad int
	reg(t, "metrics.good", func() Module {
		return &reloadStub{stub: stub{id: "metrics.good", started: &log}, reloads: &good}
	})
	reg(t, "metrics.bad", func() Module {
		return &reloadStub{stub: stub{id: "metrics.bad", started: &log}, reloads: &bad, err: errors.New("unreadable")}
	})
	reg(t, "store.plain", func() Module { return &stub{id: "store.plain", started: &log} })
	r, problems := New(cfgWith("metrics.good", "metrics.bad", "store.plain"), ctx(), hook.Policy{})
	if len(problems) != 0 {
		t.Fatalf("problems %v", problems)
	}
	got := r.Reload()
	if good != 1 || bad != 1 || len(got) != 1 || got[0].Pointer != "/modules/metrics.bad" || got[0].Message != "unreadable" {
		t.Fatalf("reloads good=%d bad=%d, problems %+v", good, bad, got)
	}
}
