package session

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/hook"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

// hookLog is one ordered record shared by the journal, the replication layer
// and the observer, so a test can read which step came before which.
type hookLog struct {
	*fakeJournal
	syncErr error
}

func (j *hookLog) Sync() error {
	if j.syncErr != nil {
		j.log("sync failed")
		return j.syncErr
	}
	return j.fakeJournal.Sync()
}

// orderReplication logs each barrier into the shared record and fails it when
// told to.
type orderReplication struct {
	log  *hookLog
	fail error

	mu    sync.Mutex
	named []store.Scope
}

func (r *orderReplication) Barrier(_ context.Context, keys []store.Scope, _ []message.ForwardRef) error {
	r.mu.Lock()
	r.named = append([]store.Scope(nil), keys...)
	r.mu.Unlock()
	r.log.log("barrier")
	return r.fail
}
func (r *orderReplication) Durable(context.Context, []store.Scope) error { return nil }
func (r *orderReplication) StoreRetained(*message.Message) bool          { return true }
func (r *orderReplication) QueryRetained(string, string, time.Time) []*message.Message {
	return nil
}

// recordingObserver logs the accepted and admission calls into the shared
// record, and refuses the admission of a publish whose payload is refuse.
type recordingObserver struct {
	log    *hookLog
	refuse string
	out    hook.Outcome
	// noAdmission stands for a module set with no admission check.
	noAdmission bool

	mu       sync.Mutex
	accepted []*message.Message
	sources  []hook.PublishSource
	admits   int
}

func (o *recordingObserver) OnDelivery(context.Context, hook.DeliveryEvent, string, string, *message.Message, byte) {
}
func (o *recordingObserver) OnRetained(context.Context, *message.Message)             {}
func (o *recordingObserver) OnWill(context.Context, string, string, *message.Message) {}

func (o *recordingObserver) OnPublishAccepted(_ context.Context, source hook.PublishSource, m *message.Message) {
	o.mu.Lock()
	o.accepted = append(o.accepted, m)
	o.sources = append(o.sources, source)
	o.mu.Unlock()
	o.log.log(fmt.Sprintf("accepted %s %s %d", source, m.Topic, m.QoS))
}

func (o *recordingObserver) Admitting() bool { return !o.noAdmission }

func (o *recordingObserver) AdmitPublish(_ context.Context, _ hook.Info, m *message.Message) hook.Outcome {
	o.mu.Lock()
	o.admits++
	o.mu.Unlock()
	o.log.log("admit " + m.Topic)
	if o.refuse != "" && string(m.Payload) == o.refuse {
		return o.out
	}
	return hook.OK
}

func (o *recordingObserver) acceptedCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.accepted)
}

func (o *recordingObserver) admitCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.admits
}

type hookFixture struct {
	m       *Manager
	clock   *fakeClock
	log     *hookLog
	repl    *orderReplication
	obs     *recordingObserver
	metrics *seriesMetrics
}

func newHookFixture(t *testing.T, mutate func(*Options)) *hookFixture {
	t.Helper()
	f := &hookFixture{log: &hookLog{fakeJournal: newFakeJournal()}, metrics: &seriesMetrics{}}
	f.repl = &orderReplication{log: f.log}
	f.obs = &recordingObserver{log: f.log, refuse: "refuse", out: hook.Outcome{Action: hook.Drop, Reason: packet.QuotaExceeded}}
	f.m, f.clock = newManager(t, func(o *Options) {
		o.Journal = f.log
		o.Observer = f.obs
		o.Metrics = f.metrics
		if mutate != nil {
			mutate(o)
		}
	})
	f.m.SetReplication(f.repl)
	return f
}

// order fails unless each step appears in the shared record after the one
// before it, scanning from the record's start.
func (f *hookFixture) order(t *testing.T, steps ...string) {
	t.Helper()
	calls := strings.Split(f.log.sequence(), "; ")
	i := 0
	for _, c := range calls {
		if i < len(steps) && c == steps[i] {
			i++
		}
	}
	if i < len(steps) {
		t.Fatalf("want the steps %q in order; the record was %q", steps, f.log.sequence())
	}
}

func TestAcceptedQoS1AfterSyncAndBarrier(t *testing.T) {
	f := newHookFixture(t, nil)
	pub := newFake("pub", packet.V5, 100)
	sub := newFake("sub", packet.V5, 100)
	connectV5(f.m, pub, true, 0, nil)
	connectV5(f.m, sub, false, 3600, nil)
	subscribe(f.m, sub, 1, "t")
	if code := publish(f.m, pub, "t", 1, 1, "x"); code != packet.Success {
		t.Fatalf("QoS 1 publish: %s", code)
	}
	f.order(t, "admit t", "enqueue-sent sub", "sync", "barrier", "accepted client t 1")
	if n := f.obs.acceptedCount(); n != 1 {
		t.Fatalf("accepted %d times, want 1", n)
	}
}

func TestAcceptedQoS2OnceAfterTheIdentifierIsAccepted(t *testing.T) {
	f := newHookFixture(t, nil)
	pub := newFake("pub", packet.V5, 100)
	sub := newFake("sub", packet.V5, 100)
	// A persistent publisher, so its inbound identifier is journaled.
	connectV5(f.m, pub, false, 3600, nil)
	connectV5(f.m, sub, false, 3600, nil)
	subscribe(f.m, sub, 2, "t")
	if code := publish(f.m, pub, "t", 2, 7, "x"); code != packet.Success {
		t.Fatalf("QoS 2 publish: %s", code)
	}
	f.order(t, "inbound+ pub", "admit t", "sync", "barrier", "inbound accepted pub", "accepted client t 2")
	// The retransmission is answered as the accepted publish it is, and is
	// neither admitted nor reported again; nor is the release.
	if code := publish(f.m, pub, "t", 2, 7, "x"); code != packet.Success {
		t.Fatalf("retransmission: %s", code)
	}
	if code, _ := f.m.Pubrel(pub, 7); code != packet.Success {
		t.Fatalf("PUBREL: %s", code)
	}
	if n, a := f.obs.acceptedCount(), f.obs.admitCount(); n != 1 || a != 1 {
		t.Fatalf("accepted %d and admitted %d times, want 1 and 1; record %q", n, a, f.log.sequence())
	}
}

func TestAcceptedWithNoMatchingSubscribers(t *testing.T) {
	f := newHookFixture(t, func(o *Options) { o.Forwarder = refusingForwarder{} })
	pub := newFake("pub", packet.V5, 100)
	connectV5(f.m, pub, true, 0, nil)
	if code := publish(f.m, pub, "nobody", 1, 1, "x"); code != packet.NoMatchingSubscribers {
		t.Fatalf("want 0x10, got %s", code)
	}
	if n := f.obs.acceptedCount(); n != 1 {
		t.Fatalf("a publish answered No Matching Subscribers is accepted: reported %d times", n)
	}
}

func TestRefusedPublishesAreNotReportedAccepted(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*hookFixture)
		opts   func(*Options)
		qos    byte
	}{
		{"refused forward QoS 1", nil, func(o *Options) { o.Forwarder = refusingForwarder{err: errors.New("routes unknown")} }, 1},
		{"refused forward QoS 2", nil, func(o *Options) { o.Forwarder = refusingForwarder{err: errors.New("routes unknown")} }, 2},
		{"failed barrier QoS 1", func(f *hookFixture) { f.repl.fail = errors.New("a follower refused") }, nil, 1},
		{"failed barrier QoS 2", func(f *hookFixture) { f.repl.fail = errors.New("a follower refused") }, nil, 2},
		{"failed sync QoS 1", func(f *hookFixture) { f.log.syncErr = errors.New("fsync") }, nil, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newHookFixture(t, c.opts)
			pub := newFake("pub", packet.V5, 100)
			sub := newFake("sub", packet.V5, 100)
			connectV5(f.m, pub, true, 0, nil)
			connectV5(f.m, sub, false, 3600, nil)
			subscribe(f.m, sub, 1, "t")
			if c.mutate != nil {
				c.mutate(f)
			}
			if code := publish(f.m, pub, "t", c.qos, 3, "x"); code != packet.ImplementationSpecificError {
				t.Fatalf("want the publish refused with 0x83, got %s", code)
			}
			if n := f.obs.acceptedCount(); n != 0 {
				t.Fatalf("a refused publish was reported accepted %d times; record %q", n, f.log.sequence())
			}
		})
	}
}

func TestQoS0WithARefusedForwardIsAccepted(t *testing.T) {
	f := newHookFixture(t, func(o *Options) { o.Forwarder = refusingForwarder{err: errors.New("routes unknown")} })
	pub := newFake("pub", packet.V5, 100)
	connectV5(f.m, pub, true, 0, nil)
	if code := publish(f.m, pub, "t", 0, 0, "x"); code != packet.Success {
		t.Fatalf("QoS 0: %s", code)
	}
	if n := f.obs.acceptedCount(); n != 1 {
		t.Fatalf("a QoS 0 publish answered as accepted was reported %d times", n)
	}
}

func TestAcceptedRetainedAfterTheBarrierNamesItsTopic(t *testing.T) {
	f := newHookFixture(t, nil)
	pub := newFake("pub", packet.V5, 100)
	connectV5(f.m, pub, true, 0, nil)
	for _, payload := range []string{"on", ""} {
		code, _ := f.m.Publish(pub, &packet.Publish{Topic: "r", QoS: 1, PacketID: 1, Retain: true, Payload: []byte(payload)})
		if code.IsError() {
			t.Fatalf("retained publish %q: %s", payload, code)
		}
	}
	f.order(t, "retain r", "sync", "barrier", "accepted client r 1", "retain- r", "barrier", "accepted client r 1")
	f.repl.mu.Lock()
	named := f.repl.named
	f.repl.mu.Unlock()
	found := false
	for _, s := range named {
		if s.Topic == "r" {
			found = true
		}
	}
	if !found {
		t.Fatalf("the barrier before the accepted call named %v, not the retained topic", named)
	}
	f.obs.mu.Lock()
	defer f.obs.mu.Unlock()
	if len(f.obs.accepted) != 2 || !f.obs.accepted[0].Retain || !f.obs.accepted[1].Retain || len(f.obs.accepted[1].Payload) != 0 {
		t.Fatalf("accepted %d messages; want both retained, the second empty", len(f.obs.accepted))
	}
}

func TestAdmissionRefusesBeforeAnyDelivery(t *testing.T) {
	f := newHookFixture(t, nil)
	pub := newFake("pub", packet.V5, 100)
	sub := newFake("sub", packet.V5, 100)
	connectV5(f.m, pub, true, 0, nil)
	connectV5(f.m, sub, false, 3600, nil)
	subscribe(f.m, sub, 1, "t")
	if code := publish(f.m, pub, "t", 1, 1, "refuse"); code != packet.QuotaExceeded {
		t.Fatalf("want the refusal's reason 0x97, got %s", code)
	}
	if n := len(sub.publishes()); n != 0 {
		t.Fatalf("a refused publish was delivered %d times", n)
	}
	if n := f.obs.acceptedCount(); n != 0 {
		t.Fatalf("a refused publish was reported accepted %d times", n)
	}
	if got := f.metrics.snapshot()[`hoardmq_drops_total{namespace=default,reason=admission}`]; got != 1 {
		t.Fatalf("admission refusals counted %v, want 1: %v", got, f.metrics.snapshot())
	}
	// An admitted publish after it is delivered and accepted.
	if code := publish(f.m, pub, "t", 1, 2, "fine"); code != packet.Success {
		t.Fatalf("admitted publish: %s", code)
	}
	if n, d := f.obs.acceptedCount(), len(sub.publishes()); n != 1 || d != 1 {
		t.Fatalf("accepted %d, delivered %d; want 1 and 1", n, d)
	}
}

func TestAdmissionRefusalReleasesTheQoS2Identifier(t *testing.T) {
	f := newHookFixture(t, nil)
	pub := newFake("pub", packet.V5, 100)
	connectV5(f.m, pub, true, 0, nil)
	if code := publish(f.m, pub, "t", 2, 9, "refuse"); code != packet.QuotaExceeded {
		t.Fatalf("want 0x97, got %s", code)
	}
	// A new message under the same identifier is not answered as a duplicate.
	if code := publish(f.m, pub, "t", 2, 9, "fine"); code.IsError() {
		t.Fatalf("the identifier was not released: %s", code)
	}
	if n, a := f.obs.acceptedCount(), f.obs.admitCount(); n != 1 || a != 2 {
		t.Fatalf("accepted %d, admitted %d; want 1 and 2", n, a)
	}
}

func TestAdmissionRefusalWithoutAnErrorReasonIsUnspecified(t *testing.T) {
	f := newHookFixture(t, nil)
	f.obs.out = hook.Outcome{Action: hook.Disconnect}
	pub := newFake("pub", packet.V5, 100)
	connectV5(f.m, pub, true, 0, nil)
	if code := publish(f.m, pub, "t", 1, 1, "refuse"); code != packet.UnspecifiedError {
		t.Fatalf("want 0x80, got %s", code)
	}
	pub.mu.Lock()
	closed := append([]packet.ReasonCode(nil), pub.closed...)
	pub.mu.Unlock()
	if len(closed) != 1 || closed[0] != packet.UnspecifiedError {
		t.Fatalf("a Disconnect outcome must close the connection with its code: %v", closed)
	}
}

func TestWillAndModulePublishesAreAcceptedAndForwardedCopiesAreNot(t *testing.T) {
	f := newHookFixture(t, nil)
	c := newFake("c", packet.V5, 100)
	connectV5(f.m, c, false, 3600, &packet.Will{Topic: "w", Payload: []byte("gone"), QoS: 1})
	f.m.Disconnect(c, false, packet.KeepAliveTimeout, true)
	f.m.Inject(&message.Message{Topic: "m", Payload: []byte("x")})
	if _, _, err := f.m.InjectForwarded(&message.Message{Topic: "f", Payload: []byte("x"), QoS: 1}); err != nil {
		t.Fatal(err)
	}
	f.obs.mu.Lock()
	defer f.obs.mu.Unlock()
	if len(f.obs.accepted) != 2 || f.obs.sources[0] != hook.SourceWill || f.obs.accepted[0].Topic != "w" ||
		f.obs.sources[1] != hook.SourceModule || f.obs.accepted[1].Topic != "m" {
		t.Fatalf("want the Will then the module's publish, and no forwarded copy; record %q", f.log.sequence())
	}
	if f.obs.admits != 0 {
		t.Fatalf("a Will or module publish was put to the admission check %d times", f.obs.admits)
	}
}

// TestNoAdmissionCheckIsNotCalled: with no module checking admission, a
// publish does not run the chain, so it builds no connection facts for it.
func TestNoAdmissionCheckIsNotCalled(t *testing.T) {
	f := newHookFixture(t, nil)
	f.obs.noAdmission = true
	pub := newFake("pub", packet.V5, 100)
	connectV5(f.m, pub, true, 0, nil)
	if code := publish(f.m, pub, "t", 1, 1, "refuse"); code.IsError() {
		t.Fatalf("with no admission check the publish is taken in: %s", code)
	}
	if n, a := f.obs.acceptedCount(), f.obs.admitCount(); n != 1 || a != 0 {
		t.Fatalf("accepted %d, admission calls %d; want 1 and 0", n, a)
	}
}
