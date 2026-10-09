// Package sim is the cluster's fault-injection harness: real nodes in one
// process, a seeded workload and fault schedule, and the design's
// invariants checked as properties. See README.md in this directory.
package sim

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

// Delivery is one PUBLISH a client received.
type Delivery struct {
	Client    string
	Node      string // address of the node that delivered it
	Topic     string
	QoS       byte
	Dup       bool
	PacketID  uint16
	Publisher string
	Seq       uint64
	// AfterComplete marks a QoS 2 message delivered again after this client
	// had already completed its exchange (PUBCOMP sent, identifier released).
	// The DUP flag cannot express it: the sender believes it is
	// retransmitting, while the receiving application sees the message a
	// second time — which is what exactly-once forbids.
	AfterComplete bool
	// SameExchange marks a QoS 2 PUBLISH that arrived under an identifier
	// whose exchange this client had already begun and not finished. That is
	// a retransmission of one delivery, whatever the DUP flag says, and the
	// receiving application sees it once.
	SameExchange bool
	At           time.Time
}

// msgID identifies one published message to the client, independently of
// whatever packet identifier carried it.
type msgID struct {
	pub, topic string
	seq        uint64
}

// Event is a connection-level observation: a CONNACK, a server DISCONNECT
// or a dropped connection, with the node it happened against.
type Event struct {
	Client    string
	Node      string // address dialled
	Kind      string // connack, disconnect, dropped
	Code      packet.ReasonCode
	Reference string
	// Present is the CONNACK's Session Present flag. A durable client that
	// reconnects and is told its session is absent lost its subscriptions
	// and its queue: the checker reports it.
	Present bool
	At      time.Time
}

// pendingSub is one SUBSCRIBE awaiting its SUBACK.
type pendingSub struct {
	id  uint16
	idx int
	gen uint64
}

// Client is a standard-library MQTT 5 client over the project codec. It
// keeps a durable session, follows Server References on redirects, and
// reconnects to the next node after a drop or a refusal with a seeded
// backoff.
type Client struct {
	ID string
	// Source, when set, is the address the client dials from. A hundred
	// thousand clients need more than one: the ephemeral port range is per
	// source and destination pair.
	Source string
	// failures, when set, counts each failed connect by class.
	failures *failureCounts
	nodes    []string
	expiry   uint32
	rng      *rand.Rand
	rngMu    sync.Mutex
	wait     time.Duration // current reconnect backoff, guarded by rngMu

	mu      sync.Mutex
	conn    net.Conn
	closing bool // Close has run; a dial in flight must not install its socket
	enc     *packet.Encoder
	dec     *packet.Decoder
	current string
	nextID  uint16
	waiting map[uint16]chan packet.ReasonCode
	pubrel  map[uint16]struct{} // QoS 2 inbound awaiting PUBREL
	// inbound2 maps an open QoS 2 identifier to the message it carries, and
	// completed remembers every message whose exchange this client finished,
	// so a later delivery of one is recognisable for what it is.
	inbound2  map[uint16]msgID
	completed map[msgID]bool
	subs      []packet.Subscription
	// grantedIn is, per entry of subs, the connection generation the filter
	// was granted in. A grant belongs to one connection: the broker may hold
	// a session across a reconnect or may not, and only its Session Present
	// flag or a fresh SUBACK says which. Parallel slices rather than a map
	// because a hundred thousand clients each carrying a map is memory the
	// measurement cannot spare, as DropCauses below says.
	grantedIn []uint64
	// awaiting is one outstanding SUBSCRIBE per entry: the packet identifier,
	// which entry of subs it carries, and the generation it was sent in.
	// Identifiers are recycled every 65,535 sends, so a SUBACK arriving on a
	// later connection must not be allowed to grant anything.
	awaiting []pendingSub
	// subAcks is one waiter per outstanding SUBSCRIBE, keyed by its packet
	// identifier as waiting is for publishes. A single shared channel let one
	// subscribe settle on another's acknowledgement (WI 1500).
	subAcks map[uint16]chan *packet.Suback
	online  atomic.Bool
	// gen counts accepted CONNACKs. It is the identity of the current
	// connection for everything a grant is bound to.
	gen atomic.Uint64
	// recovered is the failover measurement's per-client reading (WI 1618):
	// every held filter granted in the current generation. It is maintained
	// where it can change so the measurement's pass reads atomics and takes
	// no lock — at a hundred thousand clients every 50 ms, a lock here would
	// be measuring the measurement. Read it through Recovered, which also
	// reads online.
	recovered atomic.Bool

	recMu      sync.Mutex
	deliveries []Delivery
	events     []Event
	Reconnects atomic.Int64
	Redirects  atomic.Int64
	Refusals   atomic.Int64
	Fails      atomic.Int64 // connect attempts that never reached a CONNACK
	Drops      atomic.Int64
	// DropCauses splits Drops by why the connection ended: the reason code of
	// the DISCONNECT that preceded it, or "none" when the peer simply went
	// away. A fixed array rather than a map because a hundred thousand clients
	// each carrying a map is memory the measurement cannot spare, and because
	// record already holds a lock this can be incremented under (WI 1495).
	//
	// Drops itself counts only closes with no Server Reference — a redirect is
	// counted in Redirects instead — so these buckets sum to Drops, never to
	// every connection that ended.
	DropCauses [dropCauseCount]atomic.Int64

	// SubRefusals counts subscriptions the broker refused. It is a counter
	// rather than a returned error because resubscribe, the path that matters
	// most, cannot wait for the answer: it runs on the connect goroutine
	// before the reader starts. A client that is online with a refused
	// subscription receives nothing, so a property that asserts deliveries
	// would pass for the wrong reason (WI 1500).
	SubRefusals atomic.Int64

	// SessionLost counts CONNACKs that reported Session Present false to a
	// client that already held a session in this run. Its queued messages and
	// its subscriptions went with it, and the resubscribe that follows hides
	// that, so without this counter a session rebuilt empty leaves no trace
	// (WI 1653).
	SessionLost atomic.Int64
	// SubDowngraded counts SUBACKs that granted a lower QoS than was asked
	// for. A legal answer, and one that changes what the subscriber is
	// promised, so it is counted rather than ignored or treated as a refusal.
	SubDowngraded atomic.Int64

	// The reconnect summary is maintained as the events happen, so a
	// hundred thousand clients through a refusal storm cost bounded memory:
	// the event slice is capped and the measurement reads these instead.
	sumMu     sync.Mutex
	firstDrop time.Time
	firstUp   time.Time

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewClient prepares a client; Run connects it and keeps it connected.
func NewClient(id string, nodes []string, expiry uint32, seed int64) *Client {
	c := &Client{ID: id, nodes: append([]string(nil), nodes...), expiry: expiry, rng: rand.New(rand.NewSource(seed)), //nolint:gosec // seeded simulation
		waiting: map[uint16]chan packet.ReasonCode{}, pubrel: map[uint16]struct{}{}, inbound2: map[uint16]msgID{}, completed: map[msgID]bool{}, subAcks: map[uint16]chan *packet.Suback{}}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	premint(id)
	return c
}

// Run connects (retrying until the context ends) and returns once the
// first CONNACK accepted the connection. The client then stays connected
// in the background until Close.
func (c *Client) Run(ctx context.Context) error {
	c.wg.Add(1)
	first := make(chan error, 1)
	clientLabels(func() { go c.loop(first) })
	select {
	case err := <-first:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Cancel stops the client's reconnect loop without waiting. Close still
// completes the shutdown; cancelling every client first lets their in-flight
// dials and handshakes — which do not watch the context and can each wait
// seconds against a starved broker — time out concurrently instead of one
// Close at a time (a 100,000-client teardown ran 38 minutes past its window
// before this, WI 1292 round 2).
func (c *Client) Cancel() { c.cancel() }

// Close ends the client with a clean DISCONNECT when connected.
func (c *Client) Close() {
	c.cancel()
	c.mu.Lock()
	c.closing = true
	if c.conn != nil {
		_ = c.enc.Encode(c.conn, &packet.Disconnect{ReasonCode: packet.NormalDisconnection})
		_ = c.conn.Close()
	}
	c.mu.Unlock()
	c.wg.Wait()
}

// Online reports whether the client currently holds an accepted connection.
func (c *Client) Online() bool { return c.online.Load() }

// Node is the address of the node the client is connected to (or last
// tried).
func (c *Client) Node() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current
}

func (c *Client) event(kind string, code packet.ReasonCode, ref string) {
	c.record(Event{Client: c.ID, Node: c.Node(), Kind: kind, Code: code, Reference: ref, At: time.Now()})
}

// maxEvents caps what one client keeps for diagnosis. A corpus client sees
// a handful; a client retrying through a ten-minute connect storm sees tens
// of thousands, and a hundred thousand of those exhaust the machine.
const maxEvents = 64

// Drop causes, in artifact order. dropCauseNone is a connection that ended
// with no DISCONNECT at all — the transport went away — and is deliberately
// index 0 so a client that never saw one reports it without any mapping.
const (
	dropCauseNone = iota
	dropCauseTakenOver
	dropCauseShuttingDown
	dropCauseKeepAlive
	dropCauseUseAnother
	dropCauseOther
	dropCauseCount
)

var dropCauseNames = [dropCauseCount]string{
	"none", "session-taken-over", "server-shutting-down", "keep-alive-timeout", "use-another-server", "other",
}

// dropCause maps a DISCONNECT reason code to its bucket. It is a pure
// function of the code so the mapping can be tested without a connection.
func dropCause(code packet.ReasonCode) int {
	switch code {
	case 0:
		return dropCauseNone
	case packet.SessionTakenOver:
		return dropCauseTakenOver
	case packet.ServerShuttingDown:
		return dropCauseShuttingDown
	case packet.KeepAliveTimeout:
		return dropCauseKeepAlive
	case packet.UseAnotherServer:
		return dropCauseUseAnother
	}
	return dropCauseOther
}

func (c *Client) record(e Event) {
	c.sumMu.Lock()
	switch {
	case e.Kind == "failed":
		c.Fails.Add(1)
	case e.Kind == "dropped":
		c.Drops.Add(1)
		c.DropCauses[dropCause(e.Code)].Add(1)
		if c.firstDrop.IsZero() {
			c.firstDrop = e.At
		}
	case e.Kind == "connack" && e.Code == packet.Success && !c.firstDrop.IsZero() && c.firstUp.IsZero():
		c.firstUp = e.At
	}
	c.sumMu.Unlock()
	c.recMu.Lock()
	if len(c.events) < maxEvents {
		c.events = append(c.events, e)
	}
	c.recMu.Unlock()
}

// Dropped reports whether this client has lost a connection since the last
// TakeEvents, whether or not it came back.
func (c *Client) Dropped() bool {
	c.sumMu.Lock()
	defer c.sumMu.Unlock()
	return !c.firstDrop.IsZero()
}

// Reconnect returns the interval from this client's first dropped
// connection to the first CONNACK that accepted it afterwards, and whether
// both happened, since the last TakeEvents.
func (c *Client) Reconnect() (time.Duration, bool) {
	c.sumMu.Lock()
	defer c.sumMu.Unlock()
	if c.firstDrop.IsZero() || c.firstUp.IsZero() {
		return 0, false
	}
	return c.firstUp.Sub(c.firstDrop), true
}

// TakeDeliveries returns and clears the deliveries recorded so far.
func (c *Client) TakeDeliveries() []Delivery {
	c.recMu.Lock()
	defer c.recMu.Unlock()
	out := c.deliveries
	c.deliveries = nil
	return out
}

// TakeEvents returns and clears the connection events recorded so far, and
// resets the reconnect summary with them: what follows is measured from
// here.
func (c *Client) TakeEvents() []Event {
	c.sumMu.Lock()
	c.firstDrop, c.firstUp = time.Time{}, time.Time{}
	c.sumMu.Unlock()
	c.recMu.Lock()
	defer c.recMu.Unlock()
	out := c.events
	c.events = nil
	return out
}

// Backoff bounds: a client that keeps being refused doubles its wait to a
// cap, with jitter, and starts again from the floor once it is accepted. A
// flat retry is what a hundred thousand clients need least of each other:
// it exhausts the machine's ephemeral ports on its own and measures the
// harness rather than the broker.
const (
	backoffFloor = 50 * time.Millisecond
	backoffCap   = time.Second
)

func (c *Client) backoff() time.Duration {
	c.rngMu.Lock()
	defer c.rngMu.Unlock()
	if c.wait < backoffFloor {
		c.wait = backoffFloor
	}
	d := c.wait
	c.wait = min(c.wait*2, backoffCap)
	return d/2 + time.Duration(c.rng.Int63n(int64(d)))
}

// resetBackoff returns the client to the floor after an accepted CONNACK.
func (c *Client) resetBackoff() {
	c.rngMu.Lock()
	c.wait = backoffFloor
	c.rngMu.Unlock()
}

// loop connects and reads until closed; a drop, refusal or redirect picks
// the next target and reconnects after the backoff.
func (c *Client) loop(first chan error) {
	defer c.wg.Done()
	target := 0
	redirect := ""
	reported := false
	for c.ctx.Err() == nil {
		addr := c.nodes[target%len(c.nodes)]
		if redirect != "" {
			addr = redirect
			redirect = ""
		}
		c.mu.Lock()
		c.current = addr
		c.mu.Unlock()
		ack, err := c.connect(addr)
		switch {
		case err != nil:
			// A connect that never reached a CONNACK — the dial failed, the
			// accept queue was full, or the handshake did not complete. Not
			// the same event as an established connection ending, and at
			// storm scale the two differ by orders of magnitude.
			c.event("failed", 0, "")
			if c.failures != nil {
				c.failures.add(err)
			}
			target++
		case ack.ReasonCode != packet.Success:
			c.Refusals.Add(1)
			c.event("connack", ack.ReasonCode, deref(ack.Properties.ServerReference))
			if ack.Properties.ServerReference != nil {
				redirect = *ack.Properties.ServerReference
			} else {
				target++
			}
		default:
			c.record(Event{Client: c.ID, Node: c.Node(), Kind: "connack", Code: ack.ReasonCode, Present: ack.SessionPresent, At: time.Now()})
			if reported {
				c.Reconnects.Add(1)
			}
			if !reported {
				reported = true
				first <- nil
			}
			c.online.Store(true)
			c.resetBackoff()
			c.newGeneration(ack.SessionPresent)
			if !ack.SessionPresent {
				// The server has no session for this identifier, so the client
				// discards its own (MQTT 5 §4.1): pending QoS 2 exchanges are
				// forgotten, or a reused packet identifier would look like a
				// redelivery of a message never received.
				c.forgetSession()
				c.resubscribe()
			}
			ref := c.read() // blocks until the connection ends
			c.online.Store(false)
			c.recovered.Store(false)
			if ref != "" {
				c.Redirects.Add(1)
				redirect = ref
			} else {
				target++
			}
		}
		select {
		case <-c.ctx.Done():
			return
		case <-time.After(c.backoff()):
		}
	}
	if !reported {
		first <- c.ctx.Err()
	}
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func (c *Client) connect(addr string) (*packet.Connack, error) {
	d := sourceDialer(c.Source)
	nc, err := d.Dial("tcp", addr)
	if err != nil {
		return nil, &connectError{stage: "dial", err: err}
	}
	enc := &packet.Encoder{Version: packet.V5}
	dec := packet.NewDecoder(nc)
	dec.Version = packet.V5
	exp := c.expiry
	_ = nc.SetDeadline(time.Now().Add(5 * time.Second))
	if err := enc.Encode(nc, withCredentials(&packet.Connect{ProtocolLevel: packet.V5, ClientID: c.ID, CleanStart: false, Properties: packet.Properties{SessionExpiryInterval: &exp}})); err != nil {
		_ = nc.Close()
		return nil, &connectError{stage: "handshake", err: err}
	}
	p, err := dec.Read()
	if err != nil {
		_ = nc.Close()
		return nil, &connectError{stage: "handshake", err: err}
	}
	ack, ok := p.(*packet.Connack)
	if !ok {
		_ = nc.Close()
		return nil, fmt.Errorf("expected CONNACK, got %T", p)
	}
	_ = nc.SetDeadline(time.Time{})
	if ack.ReasonCode != packet.Success {
		_ = nc.Close()
		return ack, nil
	}
	c.mu.Lock()
	if c.closing {
		// Close ran while this dial was in flight, so it saw no connection
		// to close and is now waiting on the loop. Installing this one would
		// leave the reader blocked on a socket nobody will shut, which is a
		// teardown that never ends.
		c.mu.Unlock()
		_ = nc.Close()
		return nil, errors.New("client closing")
	}
	c.conn, c.enc, c.dec = nc, enc, dec
	c.mu.Unlock()
	return ack, nil
}

// forgetSession drops the client-side session state after a CONNACK with
// Session Present false.
func (c *Client) forgetSession() {
	c.mu.Lock()
	c.pubrel = map[uint16]struct{}{}
	c.inbound2 = map[uint16]msgID{}
	c.mu.Unlock()
	// completed is deliberately kept: the application saw those messages,
	// whatever the server now believes about the session.
}

// resubscribe re-issues the subscriptions after a session was not present.
//
// It cannot wait for the acknowledgements: it runs on the connect goroutine
// before the reader starts, so nothing would deliver them. The reader counts
// every refused SUBACK into SubRefusals instead, which is how a refusal here
// becomes visible — and this is the path that matters, because a mass
// reconnect after a crash puts every displaced client through it under the
// contention that produces a refusal (WI 1500).
func (c *Client) resubscribe() {
	c.mu.Lock()
	subs := append([]packet.Subscription(nil), c.subs...)
	gen := c.gen.Load()
	c.mu.Unlock()
	for i, s := range subs {
		id := c.packetID()
		c.mu.Lock()
		c.awaiting = append(c.awaiting, pendingSub{id: id, idx: i, gen: gen})
		c.mu.Unlock()
		if err := c.send(&packet.Subscribe{PacketID: id, Subscriptions: []packet.Subscription{s}}); err != nil {
			c.event("resubscribe-failed", 0, "")
			return
		}
	}
}

// newGeneration advances the connection identity and settles what this
// connection grants. Session Present is the broker stating it holds the
// session, subscriptions included, so every filter is granted at once; the
// alternative grants nothing until each SUBACK arrives.
func (c *Client) newGeneration(present bool) {
	c.mu.Lock()
	gen := c.gen.Add(1)
	c.awaiting = c.awaiting[:0]
	if present {
		for i := range c.grantedIn {
			c.grantedIn[i] = gen
		}
	} else if len(c.subs) > 0 {
		c.SessionLost.Add(1)
	}
	c.refreshRecoveredLocked()
	c.mu.Unlock()
}

// refreshRecoveredLocked recomputes the per-client reading. Caller holds mu.
//
// A client holding no filters is recovered when it is online: the failover
// population subscribes every client during the fill, and the measurement
// refuses to publish a figure over a population that subscribed to nothing,
// so this cannot pass vacuously there.
func (c *Client) refreshRecoveredLocked() {
	gen := c.gen.Load()
	ok := c.online.Load() && gen > 0
	for _, in := range c.grantedIn {
		if in != gen {
			ok = false
			break
		}
	}
	c.recovered.Store(ok)
}

// Recovered reports the failover measurement's per-client reading: online,
// with every filter this client holds granted on the connection it is on now.
//
// Online is read here as well as maintained in the flag so that a client
// between connections cannot read as recovered whatever the flag says. Two
// atomic loads, no lock and no allocation: the measurement's pass runs over
// a hundred thousand clients every 50 ms.
func (c *Client) Recovered() bool { return c.online.Load() && c.recovered.Load() }

func (c *Client) packetID() uint16 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextID++
	if c.nextID == 0 {
		c.nextID = 1
	}
	return c.nextID
}

func (c *Client) send(p packet.Packet) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return errors.New("not connected")
	}
	_ = c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	return c.enc.Encode(c.conn, p)
}

// read serves one connection until it ends; it returns the Server
// Reference of a redirecting DISCONNECT, else "".
func (c *Client) read() string {
	c.mu.Lock()
	conn, dec := c.conn, c.dec
	c.mu.Unlock()
	ref := ""
	// The reason code of the last DISCONNECT on this connection, so the drop
	// below can say what ended it. Zero means none arrived (WI 1495).
	var last packet.ReasonCode
	for {
		p, err := dec.Read()
		if err != nil {
			break
		}
		switch m := p.(type) {
		case *packet.Publish:
			c.deliver(m)
		case *packet.Puback:
			c.settle(m.PacketID, m.ReasonCode)
		case *packet.Pubrec:
			if m.ReasonCode >= 0x80 {
				// MQTT 5 §4.3.3: an error in the PUBREC ends the exchange.
				// The publish was refused, not acknowledged, and no PUBREL
				// follows.
				c.settle(m.PacketID, m.ReasonCode)
				break
			}
			_ = c.send(&packet.Pubrel{Ack: packet.Ack{PacketID: m.PacketID}})
		case *packet.Pubcomp:
			c.settle(m.PacketID, m.ReasonCode)
		case *packet.Pubrel:
			c.mu.Lock()
			delete(c.pubrel, m.PacketID)
			if id, ok := c.inbound2[m.PacketID]; ok {
				// The exchange is finished: the application has this message and
				// the identifier is free. Anything delivering it again is a
				// second delivery, whatever flag it carries.
				c.completed[id] = true
				delete(c.inbound2, m.PacketID)
			}
			c.mu.Unlock()
			_ = c.send(&packet.Pubcomp{Ack: packet.Ack{PacketID: m.PacketID}})
		case *packet.Suback:
			c.settleSuback(m)
		case *packet.Disconnect:
			ref = deref(m.Properties.ServerReference)
			last = m.ReasonCode
			c.event("disconnect", m.ReasonCode, ref)
		}
	}
	c.mu.Lock()
	if c.conn == conn {
		c.conn = nil
	}
	_ = conn.Close()
	for id, ch := range c.waiting {
		close(ch)
		delete(c.waiting, id)
	}
	c.mu.Unlock()
	if ref == "" {
		c.event("dropped", last, "")
	}
	return ref
}

func (c *Client) settle(id uint16, code packet.ReasonCode) {
	c.mu.Lock()
	ch := c.waiting[id]
	delete(c.waiting, id)
	c.mu.Unlock()
	if ch != nil {
		ch <- code
	}
}

func (c *Client) deliver(m *packet.Publish) {
	pub, seq := parsePayload(m.Payload)
	d := Delivery{Client: c.ID, Node: c.Node(), Topic: m.Topic, QoS: m.QoS, Dup: m.Dup, PacketID: m.PacketID, Publisher: pub, Seq: seq, At: time.Now()}
	switch m.QoS {
	case 1:
		_ = c.send(&packet.Puback{Ack: packet.Ack{PacketID: m.PacketID}})
	case 2:
		c.mu.Lock()
		_, open := c.pubrel[m.PacketID]
		d.SameExchange = open
		c.pubrel[m.PacketID] = struct{}{}
		id := msgID{pub, m.Topic, seq}
		c.inbound2[m.PacketID] = id
		d.AfterComplete = c.completed[id]
		c.mu.Unlock()
		_ = c.send(&packet.Pubrec{Ack: packet.Ack{PacketID: m.PacketID}})
	}
	// Every PUBLISH is recorded, retransmissions included: the properties
	// read the DUP flag, so the ledger must not hide a delivery behind the
	// client's own idea of which exchange it belongs to.
	c.recMu.Lock()
	c.deliveries = append(c.deliveries, d)
	c.recMu.Unlock()
}

// Subscribe adds a durable subscription and waits for its SUBACK.
// Subscribe sends one SUBSCRIBE and waits for its own SUBACK, returning an
// error when the broker refused the subscription. It reads the reason code
// rather than the packet's type: an acknowledgement that arrived is not an
// acknowledgement that granted anything, and a client that believes it is
// subscribed when it is not makes every assertion about deliveries vacuous —
// a property cannot fail when nobody is listening (WI 1500).
func (c *Client) Subscribe(ctx context.Context, filter string, qos byte) error {
	sub := packet.Subscription{Filter: filter, QoS: qos}
	id := c.packetID()
	ch := make(chan *packet.Suback, 1)
	c.mu.Lock()
	c.subAcks[id] = ch
	// The entry this SUBSCRIBE will occupy once it is granted. Subscribe
	// appends to subs only on success, so the index is one past the end.
	c.awaiting = append(c.awaiting, pendingSub{id: id, idx: len(c.subs), gen: c.gen.Load()})
	c.mu.Unlock()
	forget := func() {
		c.mu.Lock()
		delete(c.subAcks, id)
		c.mu.Unlock()
	}
	if err := c.send(&packet.Subscribe{PacketID: id, Subscriptions: []packet.Subscription{sub}}); err != nil {
		forget()
		return err
	}
	select {
	case ack := <-ch:
		if err := subackError(filter, ack.ReasonCodes); err != nil {
			return err
		}
		if byte(ack.ReasonCodes[0]) < qos {
			c.SubDowngraded.Add(1)
		}
		// Recorded only once granted: c.subs is what resubscribe re-establishes
		// after a reconnect, and a filter the broker refused is not something
		// this client is subscribed to.
		c.mu.Lock()
		c.subs = append(c.subs, sub)
		c.grantedIn = append(c.grantedIn, c.gen.Load())
		c.refreshRecoveredLocked()
		c.mu.Unlock()
		return nil
	case <-ctx.Done():
		forget()
		return ctx.Err()
	}
}

// settleSuback counts a SUBACK's refusals and hands it to the subscribe that
// asked for it.
//
// Every SUBACK is inspected, whoever sent the SUBSCRIBE: Subscribe gets the
// answer back through its waiter, and resubscribe — which cannot wait for one,
// because it runs before the reader starts — is covered by the same count.
// The waiter is found by packet identifier, as publishes are: one subscribe
// settling on another's acknowledgement would report one filter's refusal
// against the other's name.
func (c *Client) settleSuback(m *packet.Suback) {
	if refused := refusedFilters(m.ReasonCodes); refused > 0 {
		c.SubRefusals.Add(int64(refused))
		c.event("suback-refused", firstErrorCode(m.ReasonCodes), "")
	}
	c.mu.Lock()
	c.grantLocked(m)
	ch := c.subAcks[m.PacketID]
	delete(c.subAcks, m.PacketID)
	c.mu.Unlock()
	if ch != nil {
		ch <- m
	}
}

// grantLocked records a SUBACK against the SUBSCRIBE that asked for it and
// recomputes the client's reading. Caller holds mu.
//
// The match is by identifier and generation together. An identifier is
// reused every 65,535 sends and a SUBACK can arrive after the connection it
// was sent on has gone, so an identifier alone would let a dead connection's
// answer grant a filter on a live one.
func (c *Client) grantLocked(m *packet.Suback) {
	gen := c.gen.Load()
	for i, p := range c.awaiting {
		if p.id != m.PacketID {
			continue
		}
		c.awaiting = append(c.awaiting[:i], c.awaiting[i+1:]...)
		if p.gen != gen || p.idx >= len(c.subs) {
			return
		}
		if subackError(c.subs[p.idx].Filter, m.ReasonCodes) != nil {
			return
		}
		if len(m.ReasonCodes) > 0 && byte(m.ReasonCodes[0]) < c.subs[p.idx].QoS {
			// A legal answer that promises less than was asked for. It counts
			// as granted — a downgrade that disqualified the client would let
			// one of them turn a recovered run into "not reached" and hide
			// everything else the run measured — and is reported.
			c.SubDowngraded.Add(1)
		}
		c.grantedIn[p.idx] = gen
		c.refreshRecoveredLocked()
		return
	}
}

// subackError reports why a SUBACK did not grant the subscription, or nil.
// One SUBSCRIBE carries one subscription here, so a SUBACK that carries no
// code is as much a refusal as one that carries an error: it says nothing
// about the filter, and silence is not a grant.
func subackError(filter string, codes []packet.ReasonCode) error {
	if len(codes) == 0 {
		return fmt.Errorf("subscribe %q: the SUBACK carried no reason code", filter)
	}
	for _, code := range codes {
		if code.IsError() {
			return fmt.Errorf("subscribe %q refused: %s", filter, code)
		}
	}
	return nil
}

// refusedFilters counts the refusals in a SUBACK.
func refusedFilters(codes []packet.ReasonCode) int {
	n := 0
	for _, code := range codes {
		if code.IsError() {
			n++
		}
	}
	return n
}

// firstErrorCode is the code a refusal is recorded under.
func firstErrorCode(codes []packet.ReasonCode) packet.ReasonCode {
	for _, code := range codes {
		if code.IsError() {
			return code
		}
	}
	return 0
}

// Publish sends one message and, for QoS 1 and 2, waits for the
// acknowledgement. The payload names the publisher and the sequence so
// subscribers can check ordering and loss. It returns the reason code the
// broker acknowledged with: Success, or No Matching Subscribers (0x10),
// which is an acknowledgement too and is recorded as one.
func (c *Client) Publish(ctx context.Context, topic string, qos byte, seq uint64) (packet.ReasonCode, error) {
	return c.publish(ctx, &packet.Publish{Topic: topic, QoS: qos, Payload: payload(c.ID, seq)})
}

// PublishRetained is Publish with the retain flag set.
func (c *Client) PublishRetained(ctx context.Context, topic string, qos byte, seq uint64) (packet.ReasonCode, error) {
	return c.publish(ctx, &packet.Publish{Topic: topic, QoS: qos, Retain: true, Payload: payload(c.ID, seq)})
}

func (c *Client) publish(ctx context.Context, p *packet.Publish) (packet.ReasonCode, error) {
	qos := p.QoS
	if qos == 0 {
		return packet.Success, c.send(p)
	}
	id := c.packetID()
	p.PacketID = id
	ch := make(chan packet.ReasonCode, 1)
	c.mu.Lock()
	c.waiting[id] = ch
	c.mu.Unlock()
	if err := c.send(p); err != nil {
		c.mu.Lock()
		delete(c.waiting, id)
		c.mu.Unlock()
		return 0, err
	}
	select {
	case code, ok := <-ch:
		if !ok {
			return 0, errors.New("connection ended before the acknowledgement")
		}
		if code >= 0x80 {
			return code, fmt.Errorf("publish refused: %s", code)
		}
		return code, nil
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.waiting, id)
		c.mu.Unlock()
		return 0, ctx.Err()
	}
}

func payload(publisher string, seq uint64) []byte {
	return []byte("pub=" + publisher + ";seq=" + strconv.FormatUint(seq, 10))
}

func parsePayload(b []byte) (string, uint64) {
	s := string(b)
	var pub string
	var seq uint64
	for _, part := range strings.Split(s, ";") {
		if v, ok := strings.CutPrefix(part, "pub="); ok {
			pub = v
		} else if v, ok := strings.CutPrefix(part, "seq="); ok {
			seq, _ = strconv.ParseUint(v, 10, 64)
		}
	}
	return pub, seq
}

// ProbeConnect dials a node once with a throwaway identifier and returns the
// CONNACK's reason code and Server Reference, closing the connection; the
// minority property reads it during partitions.
func ProbeConnect(addr, id string) (packet.ReasonCode, string, error) {
	nc, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = nc.Close() }()
	enc := &packet.Encoder{Version: packet.V5}
	dec := packet.NewDecoder(nc)
	dec.Version = packet.V5
	_ = nc.SetDeadline(time.Now().Add(5 * time.Second))
	if err := enc.Encode(nc, withCredentials(&packet.Connect{ProtocolLevel: packet.V5, ClientID: id, CleanStart: true})); err != nil {
		return 0, "", err
	}
	p, err := dec.Read()
	if err != nil {
		return 0, "", err
	}
	ack, ok := p.(*packet.Connack)
	if !ok {
		return 0, "", fmt.Errorf("expected CONNACK, got %T", p)
	}
	if ack.ReasonCode == packet.Success {
		_ = enc.Encode(nc, &packet.Disconnect{ReasonCode: packet.NormalDisconnection})
	}
	return ack.ReasonCode, deref(ack.Properties.ServerReference), nil
}
