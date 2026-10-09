package session

import (
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
	"github.com/happydave/hoardmq/internal/store/faultdisk"
)

// rollingJournal is a store whose segments hold one record each: past its
// 32-byte header, one record fills a segment of 64, so every append after the
// first rolls, and the first fsync after a record is asked for is the roll's,
// of the segment it leaves. A disk told to fail its next call fails that
// record's append.
func rollingJournal(t *testing.T) (*store.Store, *faultdisk.Disk) {
	t.Helper()
	d := faultdisk.New()
	st, err := store.Open(t.TempDir(), store.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Sync: d.Sync, SegmentBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st, d
}

// journaledManager is a manager over a rolling journal with an offline
// durable subscriber "sub" to t at QoS 1 and a connected publisher "pub"
// whose own session journals nothing.
func journaledManager(t *testing.T, mutate func(*Options)) (*Manager, *faultdisk.Disk, *fakeConn) {
	t.Helper()
	st, d := rollingJournal(t)
	m, _ := newManager(t, func(o *Options) {
		o.Journal = st
		if mutate != nil {
			mutate(o)
		}
	})
	sub := newFake("sub", packet.V5, 100)
	connectV5(m, sub, false, 600, nil)
	subscribe(m, sub, 1, "t")
	m.Disconnect(sub, true, 0, false)
	pub := newFake("pub", packet.V5, 100)
	connectV5(m, pub, true, 0, nil)
	return m, d, pub
}

func recordOf(t *testing.T, m *Manager, id string) Record {
	t.Helper()
	r, ok := m.Record(id)
	if !ok {
		t.Fatalf("no session %q", id)
	}
	return r
}

// failedAppends is how many appends the session layer counted as failed.
func failedAppends(m *seriesMetrics) float64 {
	return m.snapshot()[seriesKey("hoardmq_journal_failures_total", opAppend)]
}

// reservedIDs is how many packet identifiers a session's queue holds.
func reservedIDs(t *testing.T, m *Manager, id string) int {
	t.Helper()
	k := key{m.opts.Namespace, id}
	sh := m.shardFor(k)
	sh.mu.Lock()
	s := sh.sessions[k]
	sh.mu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, held := range s.queuedIDs {
		if held {
			n++
		}
	}
	return n
}

// copiesOf counts the PUBLISHes of payload a connection received.
func copiesOf(c *fakeConn, payload string) int {
	n := 0
	for _, p := range c.publishes() {
		if string(p.Payload) == payload {
			n++
		}
	}
	return n
}

// reconnect resumes sub's session on a new connection and returns it.
func reconnect(t *testing.T, m *Manager, id string) *fakeConn {
	t.Helper()
	c := newFake(id, packet.V5, 100)
	if !connectV5(m, c, false, 600, nil) {
		t.Fatalf("precondition: %s's session was not present on reconnect", id)
	}
	return c
}

// TestWithdrawOnlyWhatARefusedPublishWrote: a copy whose record failed to
// append leaves its session only when the publish that wrote it is refused
// for the failure; a copy nobody retries is never withdrawn.
func TestWithdrawOnlyWhatARefusedPublishWrote(t *testing.T) {
	for _, c := range []struct {
		refused, journaled, want bool
	}{
		{refused: true, journaled: false, want: true},
		{refused: false, journaled: false, want: false}, // a Will, a redistribution, a QoS 0 publish: withdrawing loses it
		{refused: true, journaled: true, want: false},
		{refused: false, journaled: true, want: false},
	} {
		if got := withdraws(c.refused, c.journaled); got != c.want {
			t.Errorf("withdraws(refused %v, journaled %v) = %v, want %v", c.refused, c.journaled, got, c.want)
		}
	}
}

// TestFailedQueuedAppendRefusesThePublishAndWithdrawsTheCopy is WI 1891's
// defect on one node: the append of an offline subscriber's queued record
// fails, the publish is refused, and the copy that is in memory only leaves
// the queue, so the publisher's retry is the one the subscriber receives.
func TestFailedQueuedAppendRefusesThePublishAndWithdrawsTheCopy(t *testing.T) {
	metrics := &seriesMetrics{}
	m, d, pub := journaledManager(t, func(o *Options) { o.Metrics = metrics })
	d.FailNext(1)
	if code := publish(m, pub, "t", 1, 1, "x"); code != packet.ImplementationSpecificError {
		t.Fatalf("a publish whose subscriber's record failed to append answered %v, want 0x83", code)
	}
	if r := recordOf(t, m, "sub"); r.Queued != 0 {
		t.Fatalf("the copy whose record failed is still queued (%d): a crash loses it and the retry duplicates it", r.Queued)
	}
	if n := reservedIDs(t, m, "sub"); n != 0 {
		t.Fatalf("the withdrawn copy's packet identifier is still reserved (%d reserved)", n)
	}
	queuedEvents := seriesKey("hoardmq_deliveries_total", map[string]string{"namespace": "default", "event": "queued"})
	if n := metrics.snapshot()[queuedEvents]; n != 0 {
		t.Fatalf("the withdrawn copy was reported queued (%v)", n)
	}
	if code := publish(m, pub, "t", 1, 2, "x"); code != packet.Success {
		t.Fatalf("the retry answered %v, want Success once the journal is repaired", code)
	}
	sub := reconnect(t, m, "sub")
	if n := copiesOf(sub, "x"); n != 1 {
		t.Fatalf("the subscriber received %d copies of the retried message, want 1", n)
	}
}

// TestQoS2PublishIsRefusedInItsPubrec: at QoS 2 the refusal is the PUBREC's
// reason, the copy is withdrawn, and the exchange ends, so a re-send under
// the same identifier is routed as a new message rather than taken for a
// duplicate.
func TestQoS2PublishIsRefusedInItsPubrec(t *testing.T) {
	m, d, pub := journaledManager(t, nil)
	d.FailNext(1)
	if code := publish(m, pub, "t", 2, 7, "y"); code != packet.ImplementationSpecificError {
		t.Fatalf("PUBREC answered %v, want 0x83", code)
	}
	if r := recordOf(t, m, "sub"); r.Queued != 0 {
		t.Fatalf("the copy whose record failed is still queued (%d)", r.Queued)
	}
	if code := publish(m, pub, "t", 2, 7, "y"); code != packet.Success {
		t.Fatalf("the re-send answered %v", code)
	}
	if r := recordOf(t, m, "sub"); r.Queued != 1 {
		t.Fatalf("the re-send under the refused identifier queued %d copies, want 1: it was taken for a duplicate", r.Queued)
	}
}

// TestFailedSentRecordRefusesThePublishAndKeepsTheInflight: a copy already on
// the wire when its record fails cannot be withdrawn — the client may have
// it — so it stays in flight and completes; the publish is refused all the
// same.
func TestFailedSentRecordRefusesThePublishAndKeepsTheInflight(t *testing.T) {
	m, d, pub := journaledManager(t, nil)
	sub := reconnect(t, m, "sub")
	d.FailNext(1)
	if code := publish(m, pub, "t", 1, 1, "z"); code != packet.ImplementationSpecificError {
		t.Fatalf("a publish whose delivery's record failed answered %v, want 0x83", code)
	}
	ps := sub.publishes()
	if len(ps) != 1 || string(ps[0].Payload) != "z" {
		t.Fatalf("precondition: the delivery did not go on the wire: %+v", ps)
	}
	if r := recordOf(t, m, "sub"); r.InFlight != 1 {
		t.Fatalf("the copy on the wire was withdrawn: %d in flight, want 1", r.InFlight)
	}
	m.Puback(sub, ps[0].PacketID, 0)
	if r := recordOf(t, m, "sub"); r.InFlight != 0 {
		t.Fatalf("the PUBACK did not complete the kept copy: %d in flight", r.InFlight)
	}
}

// TestFailedDroppedRecordRefusesThePublish: a full queue evicts its oldest
// entry to take the new copy; the eviction's record fails and the new copy's
// succeeds. The publish is refused, and the new copy, journaled, stays.
func TestFailedDroppedRecordRefusesThePublish(t *testing.T) {
	m, d, pub := journaledManager(t, func(o *Options) { o.MaxQueued = 1 })
	if code := publish(m, pub, "t", 1, 1, "first"); code != packet.Success {
		t.Fatalf("precondition: the first publish answered %v", code)
	}
	d.FailNext(1)
	if code := publish(m, pub, "t", 1, 2, "second"); code != packet.ImplementationSpecificError {
		t.Fatalf("a publish whose eviction record failed answered %v, want 0x83", code)
	}
	if r := recordOf(t, m, "sub"); r.Queued != 1 {
		t.Fatalf("queued %d, want the journaled new copy", r.Queued)
	}
	sub := reconnect(t, m, "sub")
	if copiesOf(sub, "second") != 1 || copiesOf(sub, "first") != 0 {
		t.Fatalf("the subscriber received %+v, want the new copy alone", sub.publishes())
	}
}

// TestDeliveryFailureDoesNotMarkTheSubscriber: a delivery's failed record is
// the publisher's refusal, not the subscriber's; the subscriber's own next
// publish is answered on its own records.
func TestDeliveryFailureDoesNotMarkTheSubscriber(t *testing.T) {
	m, d, pub := journaledManager(t, nil)
	d.FailNext(1)
	if code := publish(m, pub, "t", 1, 1, "x"); code != packet.ImplementationSpecificError {
		t.Fatalf("precondition: the publish answered %v, want 0x83", code)
	}
	sub := reconnect(t, m, "sub")
	if code := publish(m, sub, "t", 1, 1, "own"); code != packet.Success {
		t.Fatalf("the subscriber's own publish answered %v: another publish's failed delivery was charged to it", code)
	}
}

// TestUnacknowledgedDeliveryKeepsItsCopy: a delivery no publisher waits on
// keeps a copy whose record failed; withdrawing it would lose it. A QoS 0
// publish is answered Success.
func TestUnacknowledgedDeliveryKeepsItsCopy(t *testing.T) {
	t.Run("Will", func(t *testing.T) {
		metrics := &seriesMetrics{}
		m, d, _ := journaledManager(t, func(o *Options) { o.Metrics = metrics })
		c := newFake("mortal", packet.V5, 100)
		connectV5(m, c, true, 0, &packet.Will{Topic: "t", Payload: []byte("gone"), QoS: 1})
		d.FailNext(1)
		m.Disconnect(c, false, packet.KeepAliveTimeout, true)
		if failedAppends(metrics) != 1 {
			t.Fatal("precondition: the Will's delivery record did not fail")
		}
		if r := recordOf(t, m, "sub"); r.Queued != 1 {
			t.Fatalf("the Will's copy whose record failed was withdrawn: %d queued", r.Queued)
		}
	})
	t.Run("QoS 0 publish", func(t *testing.T) {
		metrics := &seriesMetrics{}
		m, d, pub := journaledManager(t, func(o *Options) { o.Metrics = metrics })
		d.FailNext(1)
		if code := publish(m, pub, "t", 0, 0, "q0"); code != packet.Success {
			t.Fatalf("a QoS 0 publish answered %v", code)
		}
		if failedAppends(metrics) != 1 {
			t.Fatal("precondition: the QoS 0 delivery's record did not fail")
		}
		if r := recordOf(t, m, "sub"); r.Queued != 1 {
			t.Fatalf("the QoS 0 copy whose record failed was withdrawn: %d queued", r.Queued)
		}
	})
	t.Run("module message", func(t *testing.T) {
		metrics := &seriesMetrics{}
		m, d, _ := journaledManager(t, func(o *Options) { o.Metrics = metrics })
		d.FailNext(1)
		m.Inject(&message.Message{Topic: "t", QoS: 1, Payload: []byte("mod")})
		if failedAppends(metrics) != 1 {
			t.Fatal("precondition: the module message's record did not fail")
		}
		if r := recordOf(t, m, "sub"); r.Queued != 1 {
			t.Fatalf("the module message's copy whose record failed was withdrawn: %d queued", r.Queued)
		}
	})
	t.Run("retained on subscribe", func(t *testing.T) {
		metrics := &seriesMetrics{}
		m, d, pub := journaledManager(t, func(o *Options) { o.Metrics = metrics })
		if code, _ := m.Publish(pub, &packet.Publish{Topic: "r", QoS: 1, PacketID: 1, Retain: true, Payload: []byte("kept")}); code.IsError() {
			t.Fatalf("precondition: the retained publish answered %v", code)
		}
		sub := newFake("sub", packet.V5, 0) // no send quota: the retained copy is queued
		connectV5(m, sub, false, 600, nil)
		subscribe(m, sub, 1, "r")
		d.FailNext(1)
		m.Subscribed(sub)
		if failedAppends(metrics) != 1 {
			t.Fatal("precondition: the retained copy's record did not fail")
		}
		if r := recordOf(t, m, "sub"); r.Queued != 1 {
			t.Fatalf("the retained copy whose record failed was withdrawn: %d queued", r.Queued)
		}
	})
}

// TestFailedRetainedAppendRefusesThePublish: a retained publish's own record
// is a write its acknowledgement claims.
func TestFailedRetainedAppendRefusesThePublish(t *testing.T) {
	metrics := &seriesMetrics{}
	m, d, pub := journaledManager(t, func(o *Options) { o.Metrics = metrics })
	d.FailNext(1)
	if code, _ := m.Publish(pub, &packet.Publish{Topic: "nobody", QoS: 1, PacketID: 1, Retain: true, Payload: []byte("r")}); code != packet.ImplementationSpecificError {
		t.Fatalf("a retained publish whose record failed to append answered %v, want 0x83", code)
	}
	if n := metrics.snapshot()[seriesKey("hoardmq_journal_failures_total", opAppend)]; n != 1 {
		t.Fatalf("the retained record's failed append counted %v, want 1", n)
	}
}

// TestInjectForwardedReportsAFailedAppend: on the receiving node, a forwarded
// QoS 1 message whose delivery's record failed is reported, so the forward is
// refused, and its copy withdrawn; at QoS 0 nothing waits on the forward, and
// the copy stays.
func TestInjectForwardedReportsAFailedAppend(t *testing.T) {
	metrics := &seriesMetrics{}
	m, d, _ := journaledManager(t, func(o *Options) { o.Metrics = metrics })
	d.FailNext(1)
	n, into, err := m.InjectForwarded(&message.Message{Namespace: "default", Topic: "t", QoS: 1, Payload: []byte("f")})
	if err == nil {
		t.Fatalf("a forward whose delivery's record failed reported no error (%d delivered, %v)", n, into)
	}
	if r := recordOf(t, m, "sub"); r.Queued != 0 {
		t.Fatalf("the forwarded copy whose record failed is still queued (%d)", r.Queued)
	}
	d.FailNext(1)
	if _, _, err := m.InjectForwarded(&message.Message{Namespace: "default", Topic: "t", QoS: 0, Payload: []byte("f0")}); err != nil {
		t.Fatalf("a QoS 0 forward reported %v", err)
	}
	if failedAppends(metrics) != 2 {
		t.Fatal("precondition: the QoS 0 forward's record did not fail")
	}
	if r := recordOf(t, m, "sub"); r.Queued != 1 {
		t.Fatalf("the QoS 0 forwarded copy was withdrawn: %d queued", r.Queued)
	}
}

// TestRetryOfARefusedQoS2PublishIsANewMessage is server-defined behaviour: a
// QoS 2 publish refused after dispatch keeps the copies whose records
// succeeded, and the publisher's retry is a new message, whether or not it
// reuses the packet identifier, so a subscriber that took the refused copy is
// delivered the retry too. A queued copy whose own record failed was
// withdrawn, so its subscriber gets the retry once.
func TestRetryOfARefusedQoS2PublishIsANewMessage(t *testing.T) {
	for _, tc := range []struct {
		name    string
		retryID uint16
		dup     bool
	}{
		{"new identifier", 2, false},
		{"identifier reused", 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, d, pub := journaledManager(t, nil)
			a := newFake("a", packet.V5, 100)
			connectV5(m, a, true, 0, nil) // not persistent: its delivery journals nothing
			subscribe(m, a, 2, "t")
			d.FailNext(1)
			if code := publish(m, pub, "t", 2, 1, "once"); code != packet.ImplementationSpecificError {
				t.Fatalf("precondition: the publish whose other copy failed answered %v, want 0x83", code)
			}
			if n := copiesOf(a, "once"); n != 1 {
				t.Fatalf("a took %d copies of the refused publish before its retry, want 1: a refused publish may already have reached a subscriber", n)
			}
			retry := &packet.Publish{Topic: "t", QoS: 2, PacketID: tc.retryID, Dup: tc.dup, Payload: []byte("once")}
			if code, _ := m.Publish(pub, retry); code != packet.Success {
				t.Fatalf("the retry answered %v, want Success", code)
			}
			var got []*packet.Publish
			for _, p := range a.publishes() {
				if string(p.Payload) == "once" {
					got = append(got, p)
				}
			}
			if len(got) != 2 {
				t.Fatalf("a received the message %d times across the refusal and its retry, want 2", len(got))
			}
			if got[0].PacketID == got[1].PacketID || got[1].Dup {
				t.Fatalf("the retry reached a as a resend of the refused copy (identifiers %d and %d, DUP %v), want a new message", got[0].PacketID, got[1].PacketID, got[1].Dup)
			}
			if n := copiesOf(reconnect(t, m, "sub"), "once"); n != 1 {
				t.Fatalf("sub, whose copy of the refused publish failed to journal, received %d copies, want 1", n)
			}
		})
	}
}

// switchForwarder stands for a routing plane whose forward is refused while
// err is set.
type switchForwarder struct{ err *error }

func (f switchForwarder) Forward(*message.Message) (int, error) { return 1, *f.err }

// TestLocalAndForwardedFailuresAreOneRefusal: a publish whose local copy's
// record fails and whose forward is refused is answered 0x83 once, and
// neither failure carries over to the publisher's next publish.
func TestLocalAndForwardedFailuresAreOneRefusal(t *testing.T) {
	fwdErr := errors.New("injected: the forward was refused")
	m, d, pub := journaledManager(t, func(o *Options) { o.Forwarder = switchForwarder{err: &fwdErr} })
	d.FailNext(1)
	if code := publish(m, pub, "t", 1, 1, "both"); code != packet.ImplementationSpecificError {
		t.Fatalf("a publish whose local record failed and whose forward was refused answered %v, want 0x83", code)
	}
	if r := recordOf(t, m, "sub"); r.Queued != 0 {
		t.Fatalf("the local copy whose record failed is still queued (%d)", r.Queued)
	}
	fwdErr = nil
	if code := publish(m, pub, "t", 1, 2, "both"); code != packet.Success {
		t.Fatalf("the retry answered %v: a failure of the refused publish was carried over", code)
	}
}

// TestClosedJournalRefusesAndWithdraws: once the journal is closed every
// append fails, and a publish into an offline subscriber is refused and its
// copy withdrawn as for any failed append.
func TestClosedJournalRefusesAndWithdraws(t *testing.T) {
	st, _ := rollingJournal(t)
	m, _ := newManager(t, func(o *Options) { o.Journal = st })
	sub := newFake("sub", packet.V5, 100)
	connectV5(m, sub, false, 600, nil)
	subscribe(m, sub, 1, "t")
	m.Disconnect(sub, true, 0, false)
	pub := newFake("pub", packet.V5, 100)
	connectV5(m, pub, true, 0, nil)
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if code := publish(m, pub, "t", 1, 1, "late"); code != packet.ImplementationSpecificError {
		t.Fatalf("a publish over a closed journal answered %v, want 0x83", code)
	}
	if r := recordOf(t, m, "sub"); r.Queued != 0 {
		t.Fatalf("the copy a closed journal could not hold is still queued (%d)", r.Queued)
	}
}

// TestEvictionStandsWhenTheNewCopyIsWithdrawn: a full queue evicts its oldest
// entry for the new copy, whose record then fails. The new copy is withdrawn
// and the eviction is not undone: it is counted as a drop.
func TestEvictionStandsWhenTheNewCopyIsWithdrawn(t *testing.T) {
	m, d, pub := journaledManager(t, func(o *Options) { o.MaxQueued = 1 })
	if code := publish(m, pub, "t", 1, 1, "first"); code != packet.Success {
		t.Fatalf("precondition: the first publish answered %v", code)
	}
	// Every append rolls, syncing the segment it leaves and the one it opens.
	// The eviction's roll opens a segment; the new copy's roll is the first
	// sync of that segment again, as the one it leaves.
	seen := map[string]bool{}
	failed := false
	d.Before = func(_ int, path string) {
		if seen[path] && !failed {
			failed = true
			d.FailNext(1)
		}
		seen[path] = true
	}
	if code := publish(m, pub, "t", 1, 2, "second"); code != packet.ImplementationSpecificError {
		t.Fatalf("a publish whose new copy's record failed answered %v, want 0x83", code)
	}
	r := recordOf(t, m, "sub")
	if r.Drops != 1 {
		t.Fatalf("precondition: %d drops, want the eviction", r.Drops)
	}
	if r.Queued != 0 {
		t.Fatalf("queued %d, want none: the evicted entry is gone and the new copy withdrawn", r.Queued)
	}
	sub := reconnect(t, m, "sub")
	if n := len(sub.publishes()); n != 0 {
		t.Fatalf("the subscriber received %+v, want nothing", sub.publishes())
	}
}

// TestDowngradedCopyIsWithdrawn: a QoS 1 publish delivered at QoS 0 by the
// subscription's maximum is still a write the publish's acknowledgement
// claims.
func TestDowngradedCopyIsWithdrawn(t *testing.T) {
	st, d := rollingJournal(t)
	m, _ := newManager(t, func(o *Options) { o.Journal = st })
	sub := newFake("sub", packet.V5, 100)
	connectV5(m, sub, false, 600, nil)
	subscribe(m, sub, 0, "t")
	m.Disconnect(sub, true, 0, false)
	pub := newFake("pub", packet.V5, 100)
	connectV5(m, pub, true, 0, nil)
	if code := publish(m, pub, "t", 1, 1, "kept"); code != packet.Success {
		t.Fatalf("precondition: a QoS 1 publish to the QoS 0 subscription answered %v", code)
	}
	if r := recordOf(t, m, "sub"); r.Queued != 1 {
		t.Fatalf("precondition: an offline QoS 0 subscription queued %d, want 1", r.Queued)
	}
	d.FailNext(1)
	if code := publish(m, pub, "t", 1, 2, "gone"); code != packet.ImplementationSpecificError {
		t.Fatalf("a publish whose QoS 0 copy's record failed answered %v, want 0x83", code)
	}
	if r := recordOf(t, m, "sub"); r.Queued != 1 {
		t.Fatalf("queued %d, want the earlier copy alone: the failed QoS 0 copy was kept", r.Queued)
	}
}

// TestPublisherSubscribedToItselfIsRefusedOnce: a publisher's delivery to its
// own subscription is a delivery, so its failed record refuses that publish
// and is not left on the publisher's session to refuse the next one.
func TestPublisherSubscribedToItselfIsRefusedOnce(t *testing.T) {
	st, d := rollingJournal(t)
	m, _ := newManager(t, func(o *Options) { o.Journal = st })
	pub := newFake("pub", packet.V5, 100)
	connectV5(m, pub, false, 600, nil)
	subscribe(m, pub, 1, "t")
	d.FailNext(1)
	if code := publish(m, pub, "t", 1, 1, "echo"); code != packet.ImplementationSpecificError {
		t.Fatalf("a publish whose delivery to its own subscription failed to journal answered %v, want 0x83", code)
	}
	if n := copiesOf(pub, "echo"); n != 1 {
		t.Fatalf("precondition: the publisher's own delivery went on the wire %d times, want 1", n)
	}
	if code := publish(m, pub, "t", 1, 2, "echo"); code != packet.Success {
		t.Fatalf("the next publish answered %v: the refused publish's failure was left on the publisher", code)
	}
}
