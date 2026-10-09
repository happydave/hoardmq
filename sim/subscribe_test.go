package sim

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

// TestSubackError covers the decision alone: which SUBACK reason codes grant a
// subscription and which refuse it. The granted cases come first on purpose —
// a check that refused everything would pass the refusal cases while breaking
// every scenario in the corpus, and that is the shape of mistake this whole
// work item is about (WI 1500).
func TestSubackError(t *testing.T) {
	granted := []struct {
		name  string
		codes []packet.ReasonCode
	}{
		{"QoS 0 granted", []packet.ReasonCode{packet.GrantedQoS0}},
		{"QoS 1 granted", []packet.ReasonCode{packet.GrantedQoS1}},
		{"QoS 2 granted", []packet.ReasonCode{packet.GrantedQoS2}},
	}
	for _, tc := range granted {
		if err := subackError("a/t", tc.codes); err != nil {
			t.Errorf("%s: %v, want nil", tc.name, err)
		}
	}

	refused := []struct {
		name string
		code packet.ReasonCode
	}{
		{"implementation specific error", packet.ImplementationSpecificError},
		{"not authorized", packet.NotAuthorized},
		{"topic filter invalid", packet.TopicFilterInvalid},
	}
	for _, tc := range refused {
		err := subackError("a/t", []packet.ReasonCode{tc.code})
		if err == nil {
			t.Errorf("%s: nil, want an error", tc.name)
			continue
		}
		// The message must name the filter and the code: a refusal a reader
		// cannot attribute to a filter is most of the way back to silence.
		if !strings.Contains(err.Error(), "a/t") || !strings.Contains(err.Error(), tc.code.String()) {
			t.Errorf("%s: %q names neither the filter nor the code", tc.name, err)
		}
	}

	// A SUBACK that says nothing about the filter is not a grant.
	if err := subackError("a/t", nil); err == nil {
		t.Error("an empty SUBACK: nil, want an error")
	}
}

func TestRefusedFilters(t *testing.T) {
	for _, tc := range []struct {
		name  string
		codes []packet.ReasonCode
		want  int
	}{
		{"all granted", []packet.ReasonCode{packet.GrantedQoS0, packet.GrantedQoS2}, 0},
		{"one refused", []packet.ReasonCode{packet.GrantedQoS1, packet.NotAuthorized}, 1},
		{"both refused", []packet.ReasonCode{packet.NotAuthorized, packet.ImplementationSpecificError}, 2},
		{"none at all", nil, 0},
	} {
		if got := refusedFilters(tc.codes); got != tc.want {
			t.Errorf("%s: %d, want %d", tc.name, got, tc.want)
		}
	}
}

// fakeBroker answers a CONNECT and then replies to each SUBSCRIBE with the
// reason code the test chooses for it, so the client's handling of a refusal
// can be driven without one. It speaks only what these tests need.
type fakeBroker struct {
	t  *testing.T
	ln net.Listener
	// reply picks the reason code for the nth subscription this connection
	// receives, and may hold the reply back until release is closed.
	reply   func(n int, filter string) packet.ReasonCode
	release chan struct{}
	// dropAfterSuback ends the connection once it has acknowledged a
	// subscription, so the client reconnects and takes the resubscribe path.
	dropAfterSuback bool

	// presentFrom is the first connection number whose CONNACK reports
	// Session Present true; 0 means never, which is what every test before
	// WI 1618 assumed.
	presentFrom int

	mu         sync.Mutex
	subacked   int
	conns      int
	refusedOn  int // the connection number whose SUBACKs refuse; 0 for none
	subscribes map[int]int
	open       map[int]net.Conn
}

// brokerOpts are set before the accept goroutine starts. Setting them on a
// running broker would race its session goroutines, which -race would catch
// and which would make these tests untrustworthy rather than merely noisy.
type brokerOpts struct {
	hold            bool // do not answer a SUBSCRIBE until release is closed
	dropAfterSuback bool
	refusedOn       int
	presentFrom     int
}

func newFakeBroker(t *testing.T, reply func(n int, filter string) packet.ReasonCode, opts brokerOpts) *fakeBroker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b := &fakeBroker{t: t, ln: ln, reply: reply, release: make(chan struct{}),
		dropAfterSuback: opts.dropAfterSuback, refusedOn: opts.refusedOn,
		presentFrom: opts.presentFrom, subscribes: map[int]int{}, open: map[int]net.Conn{}}
	if !opts.hold {
		close(b.release)
	}
	go b.serve()
	t.Cleanup(func() { _ = ln.Close() })
	return b
}

func (b *fakeBroker) addr() string { return b.ln.Addr().String() }

// subscribesOn is how many SUBSCRIBEs the nth connection carried.
func (b *fakeBroker) subscribesOn(n int) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.subscribes[n]
}

// dropOpen ends every connection open now and leaves later ones alone, so a
// test can assert a client's state before the drop instead of racing a broker
// that closes the connection the moment it has answered.
func (b *fakeBroker) dropOpen() {
	b.mu.Lock()
	conns := make([]net.Conn, 0, len(b.open))
	for k, c := range b.open {
		conns = append(conns, c)
		delete(b.open, k)
	}
	b.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

func (b *fakeBroker) serve() {
	for {
		conn, err := b.ln.Accept()
		if err != nil {
			return
		}
		go b.session(conn)
	}
}

func (b *fakeBroker) session(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	b.mu.Lock()
	b.conns++
	mine := b.conns
	b.open[mine] = conn
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		delete(b.open, mine)
		b.mu.Unlock()
	}()
	enc := &packet.Encoder{Version: packet.V5}
	dec := packet.NewDecoder(conn)
	dec.Version = packet.V5
	n := 0
	for {
		p, err := dec.Read()
		if err != nil {
			return
		}
		switch m := p.(type) {
		case *packet.Connect:
			// Never present: the client then takes the resubscribe path, which
			// is the one a mass reconnect after a crash puts every client
			// through.
			present := b.presentFrom > 0 && mine >= b.presentFrom
			if err := enc.Encode(conn, &packet.Connack{ReasonCode: packet.Success, SessionPresent: present}); err != nil {
				return
			}
		case *packet.Subscribe:
			n++
			filter := ""
			if len(m.Subscriptions) > 0 {
				filter = m.Subscriptions[0].Filter
			}
			code := b.reply(n, filter)
			b.mu.Lock()
			b.subscribes[mine]++
			b.mu.Unlock()
			if b.refusedOn == mine {
				code = packet.ImplementationSpecificError
			}
			<-b.release
			b.mu.Lock()
			b.subacked++
			b.mu.Unlock()
			if err := enc.Encode(conn, &packet.Suback{PacketID: m.PacketID, ReasonCodes: []packet.ReasonCode{code}}); err != nil {
				return
			}
			if b.dropAfterSuback && mine == 1 {
				return
			}
		case *packet.Disconnect:
			return
		}
	}
}

// TestSubscribeReadsTheSuback drives a real client against a broker that
// grants one filter and refuses another. The granted case is asserted first so
// that a Subscribe which failed everything could not pass this test.
func TestSubscribeReadsTheSuback(t *testing.T) {
	b := newFakeBroker(t, func(_ int, filter string) packet.ReasonCode {
		if filter == "refused/t" {
			return packet.ImplementationSpecificError
		}
		return packet.GrantedQoS2
	}, brokerOpts{})
	c := NewClient("sub-1", []string{b.addr()}, 60, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := c.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.Subscribe(ctx, "granted/t", 2); err != nil {
		t.Fatalf("a granted subscribe: %v, want nil", err)
	}
	err := c.Subscribe(ctx, "refused/t", 1)
	if err == nil {
		t.Fatal("a refused subscribe returned nil")
	}
	if !strings.Contains(err.Error(), "refused/t") {
		t.Errorf("the error does not name the filter: %v", err)
	}
	if got := c.SubRefusals.Load(); got != 1 {
		t.Errorf("SubRefusals %d, want 1", got)
	}
	// Only the granted filter is remembered, so a reconnect does not re-ask
	// for one the broker refused.
	c.mu.Lock()
	subs := append([]packet.Subscription(nil), c.subs...)
	c.mu.Unlock()
	if len(subs) != 1 || subs[0].Filter != "granted/t" {
		t.Errorf("remembered subscriptions %+v, want only granted/t", subs)
	}
}

// TestSettleSubackGoesToItsOwnWaiter is the deterministic check that a SUBACK
// reaches the subscribe that asked for it.
//
// It uses many waiters on purpose. With two, an implementation that hands the
// acknowledgement to whichever subscribe is waiting picks the right one half
// the time, so a two-waiter test catches that defect in about one run in two —
// measured at roughly three failures in twenty before this test was written,
// which is not a test, it is a coin. With sixteen, an implementation that
// ignores the packet identifier would have to guess right sixteen times.
func TestSettleSubackGoesToItsOwnWaiter(t *testing.T) {
	const n = 16
	c := NewClient("settle-1", []string{"127.0.0.1:1"}, 60, 5)
	waiters := make(map[uint16]chan *packet.Suback, n)
	c.mu.Lock()
	for i := 1; i <= n; i++ {
		ch := make(chan *packet.Suback, 1)
		waiters[uint16(i)] = ch
		c.subAcks[uint16(i)] = ch
	}
	c.mu.Unlock()

	for i := 1; i <= n; i++ {
		c.settleSuback(&packet.Suback{PacketID: uint16(i), ReasonCodes: []packet.ReasonCode{packet.GrantedQoS1}})
	}
	for id, ch := range waiters {
		select {
		case ack := <-ch:
			if ack.PacketID != id {
				t.Errorf("the waiter for packet %d received packet %d", id, ack.PacketID)
			}
		default:
			t.Errorf("the waiter for packet %d received nothing", id)
		}
	}
	c.mu.Lock()
	left := len(c.subAcks)
	c.mu.Unlock()
	if left != 0 {
		t.Errorf("%d waiters left registered, want 0", left)
	}
}

// TestSubscribeSettlesOnItsOwnSuback holds both acknowledgements back, then
// releases them out of order: the second subscribe's SUBACK is written first.
// With one shared channel each subscribe took whichever arrived, so the
// granted call could return the refusal and the refused call could return nil.
func TestSubscribeSettlesOnItsOwnSuback(t *testing.T) {
	b := newFakeBroker(t, func(_ int, filter string) packet.ReasonCode {
		if filter == "refused/t" {
			return packet.NotAuthorized
		}
		return packet.GrantedQoS1
	}, brokerOpts{hold: true})

	c := NewClient("sub-2", []string{b.addr()}, 60, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := c.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	type result struct {
		filter string
		err    error
	}
	results := make(chan result, 2)
	var started sync.WaitGroup
	started.Add(2)
	for _, f := range []string{"granted/t", "refused/t"} {
		go func() {
			started.Done()
			results <- result{f, c.Subscribe(ctx, f, 1)}
		}()
	}
	started.Wait()
	// Both SUBSCRIBEs are in flight before any SUBACK is written.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		n := len(c.subAcks)
		c.mu.Unlock()
		if n == 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	close(b.release)

	got := map[string]error{}
	for range 2 {
		select {
		case r := <-results:
			got[r.filter] = r.err
		case <-ctx.Done():
			t.Fatal("a subscribe never returned")
		}
	}
	if got["granted/t"] != nil {
		t.Errorf("granted/t: %v, want nil", got["granted/t"])
	}
	if got["refused/t"] == nil {
		t.Error("refused/t returned nil")
	} else if !strings.Contains(got["refused/t"].Error(), "refused/t") {
		t.Errorf("refused/t: %v does not name its own filter", got["refused/t"])
	}
}

// TestRefusedResubscribeIsCounted is the half of this defect that matters most.
// After a reconnect reports no session present, the client re-issues its
// subscriptions on the connect goroutine, before the reader starts — so it
// cannot wait for the answers, and before WI 1500 it did not look at them at
// all. A client whose resubscribe is refused stays online and silently
// receives nothing, which is the state a mass reconnect after a crash would
// leave clients in.
//
// The granted first connection is asserted before the refusal, so the counter
// cannot pass this test by counting everything.
func TestRefusedResubscribeIsCounted(t *testing.T) {
	// The reconnect's resubscribe is refused.
	b := newFakeBroker(t, func(_ int, _ string) packet.ReasonCode { return packet.GrantedQoS1 },
		brokerOpts{dropAfterSuback: true, refusedOn: 2})

	c := NewClient("resub-1", []string{b.addr()}, 60, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := c.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.Subscribe(ctx, "resub/t", 1); err != nil {
		t.Fatalf("the first subscribe was granted and must return nil: %v", err)
	}
	if got := c.SubRefusals.Load(); got != 0 {
		t.Fatalf("SubRefusals %d after a granted subscribe, want 0", got)
	}

	// The broker drops the connection; the client reconnects, is told there is
	// no session, and resubscribes — into a refusal.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if c.SubRefusals.Load() > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := c.SubRefusals.Load(); got == 0 {
		b.mu.Lock()
		conns, subacked := b.conns, b.subacked
		b.mu.Unlock()
		t.Fatalf("a refused resubscribe was not counted: SubRefusals 0 after %d connections and %d SUBACKs", conns, subacked)
	}
}

// TestSubscribeContextEndsCleanly checks the waiter is not left behind when a
// subscribe gives up, which would leak an entry per abandoned call.
func TestSubscribeContextEndsCleanly(t *testing.T) {
	b := newFakeBroker(t, func(_ int, _ string) packet.ReasonCode { return packet.GrantedQoS0 },
		brokerOpts{hold: true}) // never released

	c := NewClient("sub-3", []string{b.addr()}, 60, 3)
	runCtx, runCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer runCancel()
	if err := c.Run(runCtx); err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(runCtx, 300*time.Millisecond)
	defer cancel()
	if err := c.Subscribe(ctx, "slow/t", 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a subscribe that gave up: %v, want a deadline error", err)
	}
	c.mu.Lock()
	n := len(c.subAcks)
	c.mu.Unlock()
	if n != 0 {
		t.Errorf("%d waiters left behind, want 0", n)
	}
}
