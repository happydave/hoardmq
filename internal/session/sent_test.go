package session

import (
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

// forwarding is a manager that forwards to other nodes, as every clustered
// node's does.
func forwarding(o *Options) { o.Forwarder = refusingForwarder{} }

// forwardedMsg is a message published on n2 and forwarded here.
func forwardedMsg(seq uint64, payload string) *message.Message {
	return &message.Message{Topic: "t/x", QoS: 1, Payload: []byte(payload), Publisher: message.Publisher{ClientID: "pub", Node: "n2"}, Seq: seq, Arrived: time.Unix(1_700_000_000, int64(seq))}
}

// handedOver seals x on m and returns its state as the takeover answer
// carries it.
func handedOver(t *testing.T, m *Manager) *store.SessionState {
	t.Helper()
	res := m.Seal("default", "x", packet.SessionTakenOver, 1<<40)
	if res.Outcome != SealSealed {
		t.Fatalf("precondition: seal %+v", res)
	}
	b, err := json.Marshal(res.State)
	if err != nil {
		t.Fatal(err)
	}
	var out store.SessionState
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return &out
}

// sentAndAcknowledged connects x on m, subscribed to t/x at QoS 1, gives it
// msg, and has it acknowledge the message.
func sentAndAcknowledged(t *testing.T, m *Manager, msg *message.Message) {
	t.Helper()
	x := newFake("x", packet.V5, 100)
	connectV5(m, x, true, 600, nil)
	subscribe(m, x, 1, "t/x")
	_, _, _ = m.InjectForwarded(msg)
	pubs := x.publishes()
	if len(pubs) != 1 || string(pubs[0].Payload) != string(msg.Payload) {
		t.Fatalf("precondition: x was sent %d messages, want %q", len(pubs), msg.Payload)
	}
	m.Puback(x, pubs[0].PacketID, packet.Success)
	if rec, _ := m.Record("x"); rec.InFlight != 0 || rec.Queued != 0 {
		t.Fatalf("precondition: x still holds the message after acknowledging it: %+v", rec)
	}
}

// TestSentMessageIsHandedOverWithTheSession: a session was sent a message on
// its old owner and acknowledged it, so the state handed over holds no entry
// for it; the new owner still skips a late forward of it, and delivers a
// forward of anything else.
func TestSentMessageIsHandedOverWithTheSession(t *testing.T) {
	a, _ := newManager(t, forwarding)
	sentAndAcknowledged(t, a, forwardedMsg(41, "m41"))
	st := handedOver(t, a)
	if len(st.Entries) != 0 || len(st.Delivered) != 1 || st.Delivered[0].Seq != 41 || st.Delivered[0].Node != "n2" {
		t.Fatalf("handed over entries %d, delivered %+v; want no entry and m41 delivered", len(st.Entries), st.Delivered)
	}
	b, _ := newManager(t, forwarding)
	b.Import(st)
	n, into, _ := b.InjectForwarded(forwardedMsg(41, "m41"))
	if n != 1 || len(into) != 1 || into[0].ClientID != "x" {
		t.Fatalf("the late forward reached %d sessions, naming %v; want it counted as reaching x", n, into)
	}
	if rec, _ := b.Record("x"); rec.Queued != 0 {
		t.Fatalf("x was given the message it had already been sent: %d queued", rec.Queued)
	}
	if got := b.DuplicatesSkipped(); got != 1 {
		t.Fatalf("duplicates skipped %d, want 1", got)
	}
	_, _, _ = b.InjectForwarded(forwardedMsg(42, "m42"))
	if rec, _ := b.Record("x"); rec.Queued != 1 {
		t.Fatalf("x holds %d queued after a new message; want 1", rec.Queued)
	}
}

// TestUnsentMessageIsNotSkipped: a message routed into a session and dropped
// by its queue limit was never sent to the client, so a late forward of it
// is delivered on the new owner; the message the session holds is skipped.
func TestUnsentMessageIsNotSkipped(t *testing.T) {
	a, _ := newManager(t, func(o *Options) { forwarding(o); o.MaxQueued = 1 })
	x := newFake("x", packet.V5, 100)
	connectV5(a, x, true, 600, nil)
	subscribe(a, x, 1, "t/x")
	a.Disconnect(x, false, packet.NormalDisconnection, false)
	_, _, _ = a.InjectForwarded(forwardedMsg(41, "m41"))
	_, _, _ = a.InjectForwarded(forwardedMsg(42, "m42"))
	if rec, _ := a.Record("x"); rec.Queued != 1 || rec.Drops != 1 {
		t.Fatalf("precondition: the queue limit did not drop m41: %+v", rec)
	}
	st := handedOver(t, a)
	b, _ := newManager(t, forwarding)
	b.Import(st)
	_, _, _ = b.InjectForwarded(forwardedMsg(41, "m41"))
	if rec, _ := b.Record("x"); rec.Queued != 2 {
		t.Fatalf("x holds %d queued after the forward of the message it never received; want 2", rec.Queued)
	}
	_, _, _ = b.InjectForwarded(forwardedMsg(42, "m42"))
	if rec, _ := b.Record("x"); rec.Queued != 2 || b.DuplicatesSkipped() != 1 {
		t.Fatalf("x holds %d queued after the forward of the message it holds (skipped %d); want 2 and 1", rec.Queued, b.DuplicatesSkipped())
	}
}

// TestSentListIsBounded: the identities a session was sent age out after the
// window and are capped, and a node that forwards nothing remembers none.
func TestSentListIsBounded(t *testing.T) {
	t.Run("the window", func(t *testing.T) {
		a, clock := newManager(t, forwarding)
		sentAndAcknowledged(t, a, forwardedMsg(41, "m41"))
		clock.advance(sentWindow + time.Second)
		_, _, _ = a.InjectForwarded(forwardedMsg(42, "m42"))
		sh := a.shardFor(key{"default", "x"})
		sh.mu.Lock()
		s := sh.sessions[key{"default", "x"}]
		sh.mu.Unlock()
		s.mu.Lock()
		held := s.sent.held()
		s.mu.Unlock()
		if held != 1 {
			t.Fatalf("the list holds %d identities after the window; want only the one sent since", held)
		}
		st := handedOver(t, a)
		if len(st.Delivered) != 1 || st.Delivered[0].Seq != 42 {
			t.Fatalf("handed over %+v after the window; want only m42", st.Delivered)
		}
	})
	t.Run("an idle session past the window", func(t *testing.T) {
		a, clock := newManager(t, forwarding)
		sentAndAcknowledged(t, a, forwardedMsg(41, "m41"))
		clock.advance(sentWindow + time.Second)
		if st := handedOver(t, a); len(st.Delivered) != 0 {
			t.Fatalf("handed over %+v, sent before the window; want nothing", st.Delivered)
		}
	})
	t.Run("the cap", func(t *testing.T) {
		saved := sentMax
		sentMax = 3
		t.Cleanup(func() { sentMax = saved })
		a, _ := newManager(t, forwarding)
		x := newFake("x", packet.V5, 100)
		connectV5(a, x, true, 600, nil)
		subscribe(a, x, 0, "t/x")
		for seq := uint64(1); seq <= 5; seq++ {
			_, _, _ = a.InjectForwarded(forwardedMsg(seq, "m"))
		}
		st := handedOver(t, a)
		var got []uint64
		for _, id := range st.Delivered {
			got = append(got, id.Seq)
		}
		if len(got) != 3 || got[0] != 3 || got[2] != 5 {
			t.Fatalf("handed over sequences %v; want the last three, [3 4 5]", got)
		}
	})
	t.Run("a single node", func(t *testing.T) {
		a, _ := newManager(t, nil)
		sentAndAcknowledged(t, a, forwardedMsg(41, "m41"))
		if st := handedOver(t, a); len(st.Delivered) != 0 {
			t.Fatalf("a node that forwards nothing handed over %+v", st.Delivered)
		}
	})
}

// TestSentListIsNotCheckpointed: a sealed copy keeps what it hands over, and
// a checkpoint of the node writes the copy without it.
func TestSentListIsNotCheckpointed(t *testing.T) {
	a, _ := newManager(t, forwarding)
	sentAndAcknowledged(t, a, forwardedMsg(41, "m41"))
	handedOver(t, a)
	if st, _, ok := a.Sealed("default", "x"); !ok || len(st.Delivered) != 1 {
		t.Fatalf("precondition: the sealed copy holds %+v", st)
	}
	cp := a.CheckpointState(func() uint64 { return 0 })
	ss := cp.Find("default", "x")
	if ss == nil {
		t.Fatal("precondition: the checkpoint does not hold the sealed copy")
	}
	if len(ss.Delivered) != 0 {
		t.Fatalf("the checkpoint wrote %+v", ss.Delivered)
	}
	if st, _, _ := a.Sealed("default", "x"); len(st.Delivered) != 1 {
		t.Fatalf("the checkpoint cleared the sealed copy's list: %+v", st.Delivered)
	}
}

// TestSentListCarriesOnAcrossASecondHandOver: A sent the message; the session
// moved to B and on to C within the window; C still skips a late forward.
func TestSentListCarriesOnAcrossASecondHandOver(t *testing.T) {
	a, _ := newManager(t, forwarding)
	sentAndAcknowledged(t, a, forwardedMsg(41, "m41"))
	b, _ := newManager(t, forwarding)
	b.Import(handedOver(t, a))
	c, _ := newManager(t, forwarding)
	c.Import(handedOver(t, b))
	_, _, _ = c.InjectForwarded(forwardedMsg(41, "m41"))
	if rec, _ := c.Record("x"); rec.Queued != 0 || c.DuplicatesSkipped() != 1 {
		t.Fatalf("C gave x the message A had sent it: %d queued, %d skipped", rec.Queued, c.DuplicatesSkipped())
	}
}

// TestSentListKeepsItsOrder drives the list through the window, growth below
// the cap after a wrap, and overwriting at the cap: it holds the newest
// entries within the window, oldest first.
func TestSentListKeepsItsOrder(t *testing.T) {
	saved := sentMax
	sentMax = 4
	t.Cleanup(func() { sentMax = saved })
	t0 := time.Unix(1_700_000_000, 0)
	var l sentList
	add := func(seq uint64, at time.Time) { l.add(sentMessage{id: message.ID{Seq: seq}, at: at.UnixNano()}, at) }
	seqs := func() []uint64 {
		var out []uint64
		for i := 0; i < l.held(); i++ {
			out = append(out, l.at(i).id.Seq)
		}
		return out
	}
	add(1, t0)
	add(2, t0)
	add(3, t0.Add(6*time.Second))
	add(4, t0.Add(sentWindow+time.Second))
	if got := seqs(); !slices.Equal(got, []uint64{3, 4}) {
		t.Fatalf("after the window the list holds %v; want [3 4]", got)
	}
	late := t0.Add(sentWindow + 2*time.Second)
	for seq := uint64(5); seq <= 7; seq++ {
		add(seq, late)
	}
	if got := seqs(); !slices.Equal(got, []uint64{4, 5, 6, 7}) {
		t.Fatalf("at the cap the list holds %v; want [4 5 6 7]", got)
	}
	if len(l.e) != sentMax {
		t.Fatalf("the ring holds %d slots; want the cap, %d", len(l.e), sentMax)
	}
}
