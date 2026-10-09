package conn

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/packet"
)

// fakeCore records calls and answers with configurable results.
type fakeCore struct {
	mu          sync.Mutex
	connects    []*packet.Connect
	publishes   []*packet.Publish
	subscribes  []*packet.Subscribe
	unsubs      []*packet.Unsubscribe
	disconnects []struct {
		graceful bool
		reason   packet.ReasonCode
		will     bool
	}
	connectResult ConnectResult
	publishReason packet.ReasonCode
	acks          []string
	// holdPubcomp answers every PUBREL with send = false, as a session
	// holding the PUBCOMP for its release does.
	holdPubcomp bool
	// holdPubrec answers a refused QoS 2 PUBLISH with send = false, as a
	// session holding the error PUBREC for its release does.
	holdPubrec bool
	// unsubReason answers every filter of every UNSUBSCRIBE.
	unsubReason packet.ReasonCode
}

func (f *fakeCore) Connect(c *Conn, p *packet.Connect) ConnectResult {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.connects = append(f.connects, p)
	return f.connectResult
}
func (f *fakeCore) Connected(*Conn)  {}
func (f *fakeCore) Subscribed(*Conn) {}
func (f *fakeCore) Publish(c *Conn, p *packet.Publish) (packet.ReasonCode, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.publishes = append(f.publishes, p)
	held := f.holdPubrec && p.QoS == 2 && f.publishReason.IsError()
	return f.publishReason, !held
}
func (f *fakeCore) Pubrel(c *Conn, id uint16) (packet.ReasonCode, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return packet.Success, !f.holdPubcomp
}
func (f *fakeCore) Puback(c *Conn, id uint16, r packet.ReasonCode) {
	f.mu.Lock()
	f.acks = append(f.acks, "puback")
	f.mu.Unlock()
}
func (f *fakeCore) Pubrec(c *Conn, id uint16, r packet.ReasonCode) (packet.ReasonCode, bool) {
	f.mu.Lock()
	f.acks = append(f.acks, "pubrec")
	f.mu.Unlock()
	return packet.Success, true
}
func (f *fakeCore) Pubcomp(c *Conn, id uint16, r packet.ReasonCode) {
	f.mu.Lock()
	f.acks = append(f.acks, "pubcomp")
	f.mu.Unlock()
}
func (f *fakeCore) Subscribe(c *Conn, s *packet.Subscribe) []packet.ReasonCode {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.subscribes = append(f.subscribes, s)
	out := make([]packet.ReasonCode, len(s.Subscriptions))
	for i, sub := range s.Subscriptions {
		out[i] = packet.ReasonCode(sub.QoS)
	}
	return out
}
func (f *fakeCore) Unsubscribe(c *Conn, u *packet.Unsubscribe) []packet.ReasonCode {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unsubs = append(f.unsubs, u)
	out := make([]packet.ReasonCode, len(u.Filters))
	for i := range out {
		out[i] = f.unsubReason
	}
	return out
}
func (f *fakeCore) Disconnect(c *Conn, graceful bool, reason packet.ReasonCode, will bool) {
	f.mu.Lock()
	f.disconnects = append(f.disconnects, struct {
		graceful bool
		reason   packet.ReasonCode
		will     bool
	}{graceful, reason, will})
	f.mu.Unlock()
}

type allowAll struct{}

func (allowAll) Authorize(context.Context, auth.Action, string, *auth.Identity) auth.Decision {
	return auth.Allow
}

type denyTopic string

func (d denyTopic) Authorize(_ context.Context, _ auth.Action, t string, _ *auth.Identity) auth.Decision {
	if t == string(d) {
		return auth.Deny
	}
	return auth.Allow
}

func defaultOptions() Options {
	return Options{
		Capabilities:   packet.DefaultCapabilities(),
		MaxPacketSize:  1 << 20,
		ReceiveMaximum: 4,
		ConnectTimeout: 2 * time.Second,
		Anonymous:      true,
		Authorizers:    auth.AuthorizerChain{allowAll{}},
		Namespace:      "default",
	}
}

// harness wires a Conn to a fake client over net.Pipe.
type harness struct {
	t      *testing.T
	core   *fakeCore
	conn   *Conn
	client net.Conn
	dec    *packet.Decoder
	enc    *packet.Encoder
	done   chan struct{}
	cancel context.CancelFunc
}

func newHarness(t *testing.T, opts Options, core *fakeCore) *harness {
	t.Helper()
	server, client := net.Pipe()
	c := New(server, "test", opts, core)
	ctx, cancel := context.WithCancel(context.Background())
	h := &harness{t: t, core: core, conn: c, client: client, dec: packet.NewDecoder(client), done: make(chan struct{}), cancel: cancel}
	go func() {
		c.Serve(ctx)
		close(h.done)
	}()
	t.Cleanup(func() {
		cancel()
		_ = client.Close()
		select {
		case <-h.done:
		case <-time.After(3 * time.Second):
			t.Error("Serve did not return")
		}
	})
	return h
}

func (h *harness) connect(v packet.Version, c *packet.Connect) *packet.Connack {
	h.t.Helper()
	if c == nil {
		c = &packet.Connect{ClientID: "cid", CleanStart: true}
	}
	c.ProtocolLevel = v
	h.enc = &packet.Encoder{Version: v}
	h.dec.Version = v
	h.send(c)
	p := h.read()
	ack, ok := p.(*packet.Connack)
	if !ok {
		h.t.Fatalf("expected CONNACK, got %T", p)
	}
	return ack
}

func (h *harness) send(p packet.Packet) {
	h.t.Helper()
	_ = h.client.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if err := h.enc.Encode(h.client, p); err != nil {
		h.t.Fatalf("send %T: %v", p, err)
	}
}

func (h *harness) read() packet.Packet {
	h.t.Helper()
	_ = h.client.SetReadDeadline(time.Now().Add(2 * time.Second))
	p, err := h.dec.Read()
	if err != nil {
		h.t.Fatalf("read: %v", err)
	}
	return p
}

// readEnd expects the server to end the connection, returning the
// DISCONNECT if one was sent (level 5) before EOF.
func (h *harness) readEnd() *packet.Disconnect {
	h.t.Helper()
	_ = h.client.SetReadDeadline(time.Now().Add(2 * time.Second))
	p, err := h.dec.Read()
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.ErrClosedPipe) {
			return nil
		}
		h.t.Fatalf("read: %v", err)
	}
	d, ok := p.(*packet.Disconnect)
	if !ok {
		h.t.Fatalf("expected DISCONNECT or EOF, got %T", p)
	}
	_, err = h.dec.Read()
	if err == nil {
		h.t.Fatal("connection stayed open after DISCONNECT")
	}
	return d
}

func (h *harness) waitDisconnect() {
	h.t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		h.core.mu.Lock()
		n := len(h.core.disconnects)
		h.core.mu.Unlock()
		if n > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	h.t.Fatal("core.Disconnect not called")
}

func TestConnectAndPingEveryLevel(t *testing.T) {
	for _, v := range []packet.Version{packet.V3, packet.V4, packet.V5} {
		t.Run(v.String(), func(t *testing.T) {
			core := &fakeCore{}
			h := newHarness(t, defaultOptions(), core)
			ack := h.connect(v, &packet.Connect{ClientID: "c1", CleanStart: true, KeepAlive: 30})
			if ack.ReasonCode != packet.Success || ack.SessionPresent {
				t.Fatalf("connack %+v", ack)
			}
			if v == packet.V5 {
				if ack.Properties.TopicAliasMaximum == nil || *ack.Properties.TopicAliasMaximum != 65535 || *ack.Properties.ReceiveMaximum != 4 || *ack.Properties.MaximumPacketSize != 1<<20 {
					t.Errorf("capabilities not advertised: %+v", ack.Properties)
				}
			}
			h.send(&packet.Pingreq{})
			if _, ok := h.read().(*packet.Pingresp); !ok {
				t.Fatal("no PINGRESP")
			}
			if h.conn.ClientID() != "c1" || h.conn.Version() != v || h.conn.Listener() != "test" {
				t.Error("accessors")
			}
			h.send(&packet.Disconnect{})
			h.waitDisconnect()
			if d := core.disconnects[0]; !d.graceful || d.will {
				t.Errorf("disconnect %+v", d)
			}
		})
	}
}

func TestUnsupportedVersionAnsweredInClientFraming(t *testing.T) {
	h := newHarness(t, defaultOptions(), &fakeCore{})
	h.enc = &packet.Encoder{Version: packet.V5}
	// Hand-craft a level 6 CONNECT.
	raw := []byte{0x10, 0x0D, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x06, 0x02, 0x00, 0x0A, 0x00, 0x00, 0x00}
	_ = h.client.SetWriteDeadline(time.Now().Add(time.Second))
	if _, err := h.client.Write(raw); err != nil {
		t.Fatal(err)
	}
	h.dec.Version = packet.V5
	p := h.read()
	if ack, ok := p.(*packet.Connack); !ok || ack.ReasonCode != packet.UnsupportedProtocolVersion {
		t.Fatalf("got %+v", p)
	}
}

func TestFirstPacketNotConnect(t *testing.T) {
	h := newHarness(t, defaultOptions(), &fakeCore{})
	h.enc = &packet.Encoder{Version: packet.V4}
	h.dec.Version = packet.V4
	h.send(&packet.Pingreq{})
	if _, err := h.dec.Read(); err == nil {
		t.Fatal("expected close")
	}
}

func TestConnectTimeout(t *testing.T) {
	opts := defaultOptions()
	opts.ConnectTimeout = 100 * time.Millisecond
	h := newHarness(t, opts, &fakeCore{})
	h.dec.Version = packet.V4
	_ = h.client.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := h.dec.Read(); err == nil {
		t.Fatal("expected close after connect timeout")
	}
}

func TestAssignedClientIdentifier(t *testing.T) {
	h := newHarness(t, defaultOptions(), &fakeCore{})
	ack := h.connect(packet.V5, &packet.Connect{CleanStart: true})
	if ack.Properties.AssignedClientIdentifier == nil || *ack.Properties.AssignedClientIdentifier != h.conn.ClientID() || !h.conn.AssignedClientID() {
		t.Fatalf("assigned id: %+v", ack.Properties)
	}
	h2 := newHarness(t, defaultOptions(), &fakeCore{})
	ack = h2.connect(packet.V4, &packet.Connect{CleanStart: false})
	if ack.ReasonCode != packet.ClientIdentifierNotValid {
		t.Fatalf("empty id with clean session 0 at level 4: %+v", ack)
	}
	h3 := newHarness(t, defaultOptions(), &fakeCore{})
	ack = h3.connect(packet.V4, &packet.Connect{CleanStart: true})
	if ack.ReasonCode != packet.Success || h3.conn.ClientID() == "" {
		t.Fatalf("empty id with clean session 1 at level 4: %+v", ack)
	}
}

func TestEnhancedAuthUnknownMethod(t *testing.T) {
	h := newHarness(t, defaultOptions(), &fakeCore{})
	m := "SCRAM-SHA-256"
	ack := h.connect(packet.V5, &packet.Connect{ClientID: "c", CleanStart: true, Properties: packet.Properties{AuthenticationMethod: &m}})
	if ack.ReasonCode != packet.BadAuthenticationMethod {
		t.Fatalf("got %+v", ack)
	}
}

func TestAuthOutsideExchangeIsProtocolError(t *testing.T) {
	h := newHarness(t, defaultOptions(), &fakeCore{})
	h.connect(packet.V5, nil)
	h.send(&packet.Auth{ReasonCode: packet.ContinueAuthentication})
	if d := h.readEnd(); d == nil || d.ReasonCode != packet.ProtocolError {
		t.Fatalf("got %+v", d)
	}
}

func TestSecondConnectIsProtocolError(t *testing.T) {
	h := newHarness(t, defaultOptions(), &fakeCore{})
	h.connect(packet.V5, nil)
	h.send(&packet.Connect{ProtocolLevel: packet.V5, ClientID: "again"})
	if d := h.readEnd(); d == nil || d.ReasonCode != packet.ProtocolError {
		t.Fatalf("got %+v", d)
	}
}

type fixedAuth struct {
	d     auth.Decision
	attrs map[string]string
}

func (f fixedAuth) Authenticate(context.Context, *auth.Request) auth.Result {
	return auth.Result{Decision: f.d, Attributes: f.attrs}
}

func TestAuthenticationDecisions(t *testing.T) {
	cases := []struct {
		name      string
		anonymous bool
		chain     auth.AuthenticatorChain
		creds     bool
		want      packet.ReasonCode
	}{
		{"anonymous refused by default", false, nil, false, packet.NotAuthorized},
		{"anonymous allowed", true, nil, false, packet.Success},
		{"credentials with no authenticator", true, nil, true, packet.BadUserNameOrPassword},
		{"deny", true, auth.AuthenticatorChain{fixedAuth{d: auth.Deny}}, true, packet.BadUserNameOrPassword},
		{"allow", false, auth.AuthenticatorChain{fixedAuth{d: auth.Allow, attrs: map[string]string{"role": "r"}}}, true, packet.Success},
		{"pass with credentials", true, auth.AuthenticatorChain{fixedAuth{d: auth.Pass}}, true, packet.BadUserNameOrPassword},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			opts := defaultOptions()
			opts.Anonymous = c.anonymous
			opts.Authenticators = c.chain
			h := newHarness(t, opts, &fakeCore{})
			connect := &packet.Connect{ClientID: "c", CleanStart: true}
			if c.creds {
				connect.HasUsername, connect.Username, connect.HasPassword, connect.Password = true, "u", true, []byte("p")
			}
			ack := h.connect(packet.V5, connect)
			if ack.ReasonCode != c.want {
				t.Fatalf("got %s want %s", ack.ReasonCode, c.want)
			}
			if c.want == packet.Success && c.creds && h.conn.Identity().Attributes["role"] != "r" {
				t.Error("attributes not attached")
			}
		})
	}
}

func TestCoreRefusal(t *testing.T) {
	core := &fakeCore{connectResult: ConnectResult{Reason: packet.ServerUnavailable}}
	h := newHarness(t, defaultOptions(), core)
	if ack := h.connect(packet.V4, nil); ack.ReasonCode != packet.ServerUnavailable {
		t.Fatalf("got %+v", ack)
	}
}

func TestKeepAliveTimeout(t *testing.T) {
	h := newHarness(t, defaultOptions(), &fakeCore{})
	h.connect(packet.V5, &packet.Connect{ClientID: "c", KeepAlive: 1})
	start := time.Now()
	_ = h.client.SetReadDeadline(time.Now().Add(4 * time.Second))
	p, err := h.dec.Read()
	if err != nil {
		t.Fatal(err)
	}
	if d, ok := p.(*packet.Disconnect); !ok || d.ReasonCode != packet.KeepAliveTimeout {
		t.Fatalf("got %+v", p)
	}
	if el := time.Since(start); el < 1400*time.Millisecond || el > 2500*time.Millisecond {
		t.Errorf("closed after %v, want about 1.5s", el)
	}
	h.waitDisconnect()
	if d := h.core.disconnects[0]; d.graceful || !d.will || d.reason != packet.KeepAliveTimeout {
		t.Errorf("disconnect %+v", d)
	}
}

func TestServerKeepAlive(t *testing.T) {
	opts := defaultOptions()
	opts.MaxKeepAlive = 20
	h := newHarness(t, opts, &fakeCore{})
	ack := h.connect(packet.V5, &packet.Connect{ClientID: "c", KeepAlive: 300})
	if ack.Properties.ServerKeepAlive == nil || *ack.Properties.ServerKeepAlive != 20 {
		t.Fatalf("server keep alive not advertised: %+v", ack.Properties)
	}
	h2 := newHarness(t, opts, &fakeCore{})
	if ack := h2.connect(packet.V5, &packet.Connect{ClientID: "c", KeepAlive: 10}); ack.Properties.ServerKeepAlive != nil {
		t.Fatal("client value within limit must not be overridden")
	}
}

func TestPublishQoSFlows(t *testing.T) {
	core := &fakeCore{}
	h := newHarness(t, defaultOptions(), core)
	h.connect(packet.V5, nil)
	h.send(&packet.Publish{Topic: "t", Payload: []byte("0")})
	h.send(&packet.Publish{Topic: "t", QoS: 1, PacketID: 1, Payload: []byte("1")})
	if a, ok := h.read().(*packet.Puback); !ok || a.PacketID != 1 {
		t.Fatal("no PUBACK")
	}
	h.send(&packet.Publish{Topic: "t", QoS: 2, PacketID: 2, Payload: []byte("2")})
	if r, ok := h.read().(*packet.Pubrec); !ok || r.PacketID != 2 {
		t.Fatal("no PUBREC")
	}
	h.send(&packet.Pubrel{Ack: packet.Ack{PacketID: 2}})
	if c, ok := h.read().(*packet.Pubcomp); !ok || c.PacketID != 2 {
		t.Fatal("no PUBCOMP")
	}
	core.mu.Lock()
	n := len(core.publishes)
	core.mu.Unlock()
	if n != 3 {
		t.Errorf("core saw %d publishes", n)
	}
}

// TestHeldPubcompIsNotSent: a PUBREL the core answers with send = false gets
// no PUBCOMP from the connection; the session sends it later.
func TestHeldPubcompIsNotSent(t *testing.T) {
	core := &fakeCore{holdPubcomp: true}
	h := newHarness(t, defaultOptions(), core)
	h.connect(packet.V5, nil)
	h.send(&packet.Publish{Topic: "t", QoS: 2, PacketID: 2, Payload: []byte("2")})
	if r, ok := h.read().(*packet.Pubrec); !ok || r.PacketID != 2 {
		t.Fatal("precondition: no PUBREC")
	}
	h.send(&packet.Pubrel{Ack: packet.Ack{PacketID: 2}})
	h.send(&packet.Pingreq{})
	if p := h.read(); p.Type() != packet.PINGRESP {
		t.Fatalf("after a held PUBREL the connection wrote %T before the PINGRESP", p)
	}
}

// TestRefusedPublishClosesBelowLevel5: a 3.1 or 3.1.1 client has no reason
// code on PUBACK or PUBREC, and reads either as acceptance, so a QoS 1 or 2
// publish the core refuses closes the connection with nothing written for it,
// and stays unacknowledged. A level 5 client is answered with the reason.
func TestRefusedPublishClosesBelowLevel5(t *testing.T) {
	for _, v := range []packet.Version{packet.V3, packet.V4, packet.V5} {
		for _, qos := range []byte{1, 2} {
			for _, hold := range []bool{false, true} {
				if hold && qos == 1 {
					continue // the core holds only an error PUBREC
				}
				name := fmt.Sprintf("%s/qos%d/held=%v", v, qos, hold)
				t.Run(name, func(t *testing.T) {
					h := newHarness(t, defaultOptions(), &fakeCore{publishReason: packet.ImplementationSpecificError, holdPubrec: hold})
					h.connect(v, nil)
					h.send(&packet.Publish{Topic: "t", QoS: qos, PacketID: 7, Payload: []byte("x")})
					if v != packet.V5 {
						if d := h.readEnd(); d != nil {
							t.Fatalf("a refused publish at %s was answered %+v; want the connection closed with nothing written", v, d)
						}
						return
					}
					if hold && qos == 2 {
						return // held by the core: nothing to read at level 5 either
					}
					p := h.read()
					var got packet.ReasonCode
					switch a := p.(type) {
					case *packet.Puback:
						got = a.ReasonCode
					case *packet.Pubrec:
						got = a.ReasonCode
					default:
						t.Fatalf("a refused publish at level 5 was answered %T, want its acknowledgement", p)
					}
					if got != packet.ImplementationSpecificError {
						t.Fatalf("a refused publish at level 5 was acknowledged with %v, want 0x83", got)
					}
				})
			}
		}
	}
}

// TestAcceptedPublishIsAcknowledgedAtEveryLevel: a publish the core accepts is
// acknowledged at every level; only a refusal closes.
func TestAcceptedPublishIsAcknowledgedAtEveryLevel(t *testing.T) {
	for _, v := range []packet.Version{packet.V3, packet.V4, packet.V5} {
		for _, qos := range []byte{1, 2} {
			t.Run(fmt.Sprintf("%s/qos%d", v, qos), func(t *testing.T) {
				h := newHarness(t, defaultOptions(), &fakeCore{})
				h.connect(v, nil)
				h.send(&packet.Publish{Topic: "t", QoS: qos, PacketID: 7, Payload: []byte("x")})
				p := h.read()
				switch a := p.(type) {
				case *packet.Puback:
					if qos != 1 || a.PacketID != 7 {
						t.Fatalf("an accepted QoS %d publish was answered %+v", qos, a)
					}
				case *packet.Pubrec:
					if qos != 2 || a.PacketID != 7 {
						t.Fatalf("an accepted QoS %d publish was answered %+v", qos, a)
					}
				default:
					t.Fatalf("an accepted QoS %d publish at %s was answered %T, want its acknowledgement", qos, v, p)
				}
			})
		}
	}
}

// TestHeldPubrecIsNotSent: a refused QoS 2 PUBLISH whose PUBREC the core
// holds is not answered by the connection; one the core does not hold is.
func TestHeldPubrecIsNotSent(t *testing.T) {
	for _, hold := range []bool{true, false} {
		h := newHarness(t, defaultOptions(), &fakeCore{publishReason: packet.ImplementationSpecificError, holdPubrec: hold})
		h.connect(packet.V5, nil)
		h.send(&packet.Publish{Topic: "t", QoS: 2, PacketID: 2, Payload: []byte("2")})
		h.send(&packet.Pingreq{})
		p := h.read()
		if hold && p.Type() != packet.PINGRESP {
			t.Fatalf("after a held error PUBREC the connection wrote %T before the PINGRESP", p)
		}
		if r, ok := p.(*packet.Pubrec); !hold && (!ok || r.PacketID != 2 || r.ReasonCode != packet.ImplementationSpecificError) {
			t.Fatalf("a refusal the core did not hold was answered %#v, want the error PUBREC", p)
		}
	}
}

// TestHeldPubrecFreesTheReceiveMaximumSlot: a refused identifier leaves the
// connection's inbound count at once even when its PUBREC is held, so a
// client at its Receive Maximum is not disconnected for its next message.
func TestHeldPubrecFreesTheReceiveMaximumSlot(t *testing.T) {
	opts := defaultOptions()
	opts.ReceiveMaximum = 1
	h := newHarness(t, opts, &fakeCore{publishReason: packet.ImplementationSpecificError, holdPubrec: true})
	h.connect(packet.V5, nil)
	h.send(&packet.Publish{Topic: "t", QoS: 2, PacketID: 1})
	h.send(&packet.Publish{Topic: "t", QoS: 2, PacketID: 2})
	// Written aside: a connection refusing the second message is writing its
	// DISCONNECT, and the pipe takes nothing until that is read.
	go func() {
		_ = h.client.SetWriteDeadline(time.Now().Add(2 * time.Second))
		_ = h.enc.Encode(h.client, &packet.Pingreq{})
	}()
	if p := h.read(); p.Type() != packet.PINGRESP {
		t.Fatalf("the next message after a held error PUBREC got %#v, want the connection kept and the PINGRESP", p)
	}
}

// TestHeldPubcompFreesTheReceiveMaximumSlot: the identifier leaves the
// connection's inbound count at the PUBREL even when its PUBCOMP is held, so
// a client at its Receive Maximum is not refused its next message for it.
func TestHeldPubcompFreesTheReceiveMaximumSlot(t *testing.T) {
	opts := defaultOptions()
	opts.ReceiveMaximum = 1
	h := newHarness(t, opts, &fakeCore{holdPubcomp: true})
	h.connect(packet.V5, nil)
	h.send(&packet.Publish{Topic: "t", QoS: 2, PacketID: 1})
	if r, ok := h.read().(*packet.Pubrec); !ok || r.PacketID != 1 {
		t.Fatal("precondition: no PUBREC for the first message")
	}
	h.send(&packet.Pubrel{Ack: packet.Ack{PacketID: 1}})
	h.send(&packet.Publish{Topic: "t", QoS: 2, PacketID: 2})
	if r, ok := h.read().(*packet.Pubrec); !ok || r.PacketID != 2 || r.ReasonCode.IsError() {
		t.Fatalf("the next message after a held PUBREL got %+v, want a successful PUBREC", r)
	}
}

func TestReceiveMaximumInbound(t *testing.T) {
	opts := defaultOptions()
	opts.ReceiveMaximum = 2
	h := newHarness(t, opts, &fakeCore{})
	h.connect(packet.V5, nil)
	for id := uint16(1); id <= 2; id++ {
		h.send(&packet.Publish{Topic: "t", QoS: 2, PacketID: id})
		h.read()
	}
	// A duplicate of an in-flight id is not a new message.
	h.send(&packet.Publish{Topic: "t", QoS: 2, PacketID: 2, Dup: true})
	h.read()
	h.send(&packet.Publish{Topic: "t", QoS: 2, PacketID: 3})
	if d := h.readEnd(); d == nil || d.ReasonCode != packet.ReceiveMaximumExceeded {
		t.Fatalf("got %+v", d)
	}
}

func TestOutboundQuotaAndAcks(t *testing.T) {
	core := &fakeCore{}
	h := newHarness(t, defaultOptions(), core)
	rm := uint16(2)
	h.connect(packet.V5, &packet.Connect{ClientID: "c", Properties: packet.Properties{ReceiveMaximum: &rm}})
	deliver := func(id uint16, qos byte) error {
		return h.conn.Deliver(&packet.Publish{Topic: "t", QoS: qos, PacketID: id, Payload: []byte("x")})
	}
	if err := deliver(1, 1); err != nil {
		t.Fatal(err)
	}
	if err := deliver(2, 2); err != nil {
		t.Fatal(err)
	}
	if err := deliver(3, 1); err != ErrQuota {
		t.Fatalf("third QoS 1 delivery: %v", err)
	}
	if err := deliver(0, 0); err != nil {
		t.Fatalf("QoS 0 is not quota-bound: %v", err)
	}
	h.read()
	h.read()
	h.read()
	h.send(&packet.Puback{Ack: packet.Ack{PacketID: 1}})
	h.send(&packet.Pubrec{Ack: packet.Ack{PacketID: 2}})
	if r, ok := h.read().(*packet.Pubrel); !ok || r.PacketID != 2 {
		t.Fatal("no PUBREL after PUBREC")
	}
	h.send(&packet.Pubcomp{Ack: packet.Ack{PacketID: 2}})
	h.send(&packet.Pingreq{})
	h.read()
	if q := h.conn.SendQuota(); q != 2 {
		t.Errorf("quota after acks %d, want 2", q)
	}
	core.mu.Lock()
	acks := core.acks
	core.mu.Unlock()
	if len(acks) != 3 || acks[0] != "puback" || acks[1] != "pubrec" || acks[2] != "pubcomp" {
		t.Errorf("core acks %v", acks)
	}
}

func TestMaximumPacketSize(t *testing.T) {
	opts := defaultOptions()
	opts.MaxPacketSize = 64
	h := newHarness(t, opts, &fakeCore{})
	mps := uint32(40)
	h.connect(packet.V5, &packet.Connect{ClientID: "c", Properties: packet.Properties{MaximumPacketSize: &mps}})
	// Outbound: an oversize PUBLISH is dropped silently; a small one follows.
	if err := h.conn.Deliver(&packet.Publish{Topic: "t", Payload: make([]byte, 100)}); err != nil {
		t.Fatal(err)
	}
	if err := h.conn.Deliver(&packet.Publish{Topic: "t", Payload: []byte("ok")}); err != nil {
		t.Fatal(err)
	}
	if p, ok := h.read().(*packet.Publish); !ok || string(p.Payload) != "ok" {
		t.Fatalf("got %+v", p)
	}
	// Outbound acknowledgement too large with its Reason String is retried without it.
	long := string(make([]byte, 60))
	_ = h.conn.Send(&packet.Puback{Ack: packet.Ack{PacketID: 9, ReasonCode: packet.QuotaExceeded, Properties: packet.Properties{ReasonString: &long}}})
	if a, ok := h.read().(*packet.Puback); !ok || a.Properties.ReasonString != nil || a.ReasonCode != packet.QuotaExceeded {
		t.Fatalf("stripped ack: %+v", a)
	}
	// Inbound: over the server's limit closes with 0x95 before the body is
	// read, so the pipe write must not be waited on.
	go func() {
		_ = h.enc.Encode(h.client, &packet.Publish{Topic: "t", Payload: make([]byte, 200)})
	}()
	if d := h.readEnd(); d == nil || d.ReasonCode != packet.PacketTooLarge {
		t.Fatalf("got %+v", d)
	}
}

func TestTopicAliasInbound(t *testing.T) {
	opts := defaultOptions()
	opts.Capabilities.TopicAliasMaximum = 2
	core := &fakeCore{}
	h := newHarness(t, opts, core)
	h.connect(packet.V5, nil)
	alias := uint16(1)
	h.send(&packet.Publish{Topic: "sensors/a", Properties: packet.Properties{TopicAlias: &alias}, Payload: []byte("1")})
	h.send(&packet.Publish{Topic: "", Properties: packet.Properties{TopicAlias: &alias}, Payload: []byte("2")})
	h.send(&packet.Pingreq{})
	h.read()
	core.mu.Lock()
	pubs := core.publishes
	core.mu.Unlock()
	if len(pubs) != 2 || pubs[1].Topic != "sensors/a" || pubs[1].Properties.TopicAlias != nil {
		t.Fatalf("aliases not resolved: %+v", pubs)
	}
	unknown := uint16(2)
	h.send(&packet.Publish{Topic: "", Properties: packet.Properties{TopicAlias: &unknown}})
	if d := h.readEnd(); d == nil || d.ReasonCode != packet.ProtocolError {
		t.Fatalf("unknown alias: %+v", d)
	}
	h2 := newHarness(t, opts, &fakeCore{})
	h2.connect(packet.V5, nil)
	big := uint16(3)
	h2.send(&packet.Publish{Topic: "t", Properties: packet.Properties{TopicAlias: &big}})
	if d := h2.readEnd(); d == nil || d.ReasonCode != packet.TopicAliasInvalid {
		t.Fatalf("alias above maximum: %+v", d)
	}
}

func TestTopicAliasOutbound(t *testing.T) {
	h := newHarness(t, defaultOptions(), &fakeCore{})
	am := uint16(1)
	h.connect(packet.V5, &packet.Connect{ClientID: "c", Properties: packet.Properties{TopicAliasMaximum: &am}})
	for i := 0; i < 2; i++ {
		if err := h.conn.Deliver(&packet.Publish{Topic: "a/b", Payload: []byte("x")}); err != nil {
			t.Fatal(err)
		}
	}
	if err := h.conn.Deliver(&packet.Publish{Topic: "other", Payload: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	first := h.read().(*packet.Publish)
	second := h.read().(*packet.Publish)
	third := h.read().(*packet.Publish)
	if first.Topic != "a/b" || first.Properties.TopicAlias == nil || *first.Properties.TopicAlias != 1 {
		t.Errorf("first: %+v", first)
	}
	if second.Topic != "" || second.Properties.TopicAlias == nil || *second.Properties.TopicAlias != 1 {
		t.Errorf("second: %+v", second)
	}
	if third.Topic != "other" || third.Properties.TopicAlias != nil {
		t.Errorf("third beyond the client's maximum: %+v", third)
	}
}

func TestCapabilityEnforcement(t *testing.T) {
	opts := defaultOptions()
	opts.Capabilities = packet.Capabilities{MaximumQoS: 1, TopicAliasMaximum: 0}
	t.Run("retain", func(t *testing.T) {
		h := newHarness(t, opts, &fakeCore{})
		h.connect(packet.V5, nil)
		h.send(&packet.Publish{Topic: "t", Retain: true})
		if d := h.readEnd(); d == nil || d.ReasonCode != packet.RetainNotSupported {
			t.Fatalf("%+v", d)
		}
	})
	t.Run("qos", func(t *testing.T) {
		h := newHarness(t, opts, &fakeCore{})
		h.connect(packet.V5, nil)
		h.send(&packet.Publish{Topic: "t", QoS: 2, PacketID: 1})
		if d := h.readEnd(); d == nil || d.ReasonCode != packet.QoSNotSupported {
			t.Fatalf("%+v", d)
		}
	})
	t.Run("qos at level 4 closes", func(t *testing.T) {
		h := newHarness(t, opts, &fakeCore{})
		h.connect(packet.V4, nil)
		h.send(&packet.Publish{Topic: "t", QoS: 2, PacketID: 1})
		if d := h.readEnd(); d != nil {
			t.Fatalf("level 4 must just close, got %+v", d)
		}
	})
	t.Run("subscribe declines", func(t *testing.T) {
		h := newHarness(t, opts, &fakeCore{})
		h.connect(packet.V5, nil)
		h.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "a/#", QoS: 2}, {Filter: "$share/g/a", QoS: 1}, {Filter: "plain", QoS: 2}, {Filter: "bad/#/x"}}})
		ack := h.read().(*packet.Suback)
		want := []packet.ReasonCode{packet.WildcardSubscriptionsNotSupported, packet.SharedSubscriptionsNotSupported, packet.GrantedQoS1, packet.TopicFilterInvalid}
		for i, w := range want {
			if ack.ReasonCodes[i] != w {
				t.Errorf("filter %d: %s want %s", i, ack.ReasonCodes[i], w)
			}
		}
	})
	t.Run("subscription identifiers", func(t *testing.T) {
		h := newHarness(t, opts, &fakeCore{})
		h.connect(packet.V5, nil)
		h.send(&packet.Subscribe{PacketID: 1, Properties: packet.Properties{SubscriptionIdentifiers: []uint32{1}}, Subscriptions: []packet.Subscription{{Filter: "a"}}})
		if d := h.readEnd(); d == nil || d.ReasonCode != packet.SubscriptionIdentifiersNotSupported {
			t.Fatalf("%+v", d)
		}
	})
	t.Run("invalid filter at level 4 closes", func(t *testing.T) {
		h := newHarness(t, opts, &fakeCore{})
		h.connect(packet.V4, nil)
		h.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "a/#/b"}}})
		if d := h.readEnd(); d != nil {
			t.Fatal("expected plain close")
		}
	})
}

func TestAuthorizationDefaultDeny(t *testing.T) {
	opts := defaultOptions()
	opts.Authorizers = nil
	t.Run("qos1 level 5", func(t *testing.T) {
		h := newHarness(t, opts, &fakeCore{})
		h.connect(packet.V5, nil)
		h.send(&packet.Publish{Topic: "t", QoS: 1, PacketID: 1})
		if a := h.read().(*packet.Puback); a.ReasonCode != packet.NotAuthorized {
			t.Fatalf("%+v", a)
		}
		h.send(&packet.Publish{Topic: "t", QoS: 2, PacketID: 2})
		if r := h.read().(*packet.Pubrec); r.ReasonCode != packet.NotAuthorized {
			t.Fatalf("%+v", r)
		}
		h.send(&packet.Subscribe{PacketID: 3, Subscriptions: []packet.Subscription{{Filter: "t"}}})
		if s := h.read().(*packet.Suback); s.ReasonCodes[0] != packet.NotAuthorized {
			t.Fatalf("%+v", s)
		}
		h.send(&packet.Unsubscribe{PacketID: 4, Filters: []string{"t"}})
		if u := h.read().(*packet.Unsuback); u.ReasonCodes[0] != packet.NotAuthorized {
			t.Fatalf("%+v", u)
		}
		h.send(&packet.Publish{Topic: "t"})
		if d := h.readEnd(); d == nil || d.ReasonCode != packet.NotAuthorized {
			t.Fatalf("qos 0: %+v", d)
		}
	})
	t.Run("level 4", func(t *testing.T) {
		h := newHarness(t, opts, &fakeCore{})
		h.connect(packet.V4, nil)
		h.send(&packet.Subscribe{PacketID: 3, Subscriptions: []packet.Subscription{{Filter: "t"}}})
		if s := h.read().(*packet.Suback); s.ReasonCodes[0] != 0x80 {
			t.Fatalf("%+v", s)
		}
		h.send(&packet.Publish{Topic: "t", QoS: 1, PacketID: 1})
		if d := h.readEnd(); d != nil {
			t.Fatal("expected plain close")
		}
	})
	t.Run("partial", func(t *testing.T) {
		o := opts
		o.Authorizers = auth.AuthorizerChain{denyTopic("secret")}
		core := &fakeCore{}
		h := newHarness(t, o, core)
		h.connect(packet.V5, nil)
		h.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "secret", QoS: 1}, {Filter: "open", QoS: 2}}})
		s := h.read().(*packet.Suback)
		if s.ReasonCodes[0] != packet.NotAuthorized || s.ReasonCodes[1] != packet.GrantedQoS2 {
			t.Fatalf("%+v", s)
		}
		core.mu.Lock()
		fwd := core.subscribes[0]
		core.mu.Unlock()
		if len(fwd.Subscriptions) != 1 || fwd.Subscriptions[0].Filter != "open" {
			t.Errorf("core saw %+v", fwd.Subscriptions)
		}
	})
}

func TestServerCloseSendsDisconnect(t *testing.T) {
	core := &fakeCore{}
	h := newHarness(t, defaultOptions(), core)
	h.connect(packet.V5, nil)
	h.conn.Close(packet.SessionTakenOver)
	if d := h.readEnd(); d == nil || d.ReasonCode != packet.SessionTakenOver {
		t.Fatalf("%+v", d)
	}
	h.waitDisconnect()
	if d := core.disconnects[0]; d.graceful || d.reason != packet.SessionTakenOver {
		t.Errorf("%+v", d)
	}
	if err := h.conn.Send(&packet.Pingresp{}); err != ErrClosed {
		t.Errorf("send after close: %v", err)
	}
}

func TestContextCancelSendsServerShuttingDown(t *testing.T) {
	h := newHarness(t, defaultOptions(), &fakeCore{})
	h.connect(packet.V5, nil)
	h.cancel()
	if d := h.readEnd(); d == nil || d.ReasonCode != packet.ServerShuttingDown {
		t.Fatalf("%+v", d)
	}
}

func TestDisconnectWithWillAndExpiryOverride(t *testing.T) {
	core := &fakeCore{}
	h := newHarness(t, defaultOptions(), core)
	h.connect(packet.V5, nil)
	exp := uint32(120)
	h.send(&packet.Disconnect{ReasonCode: packet.DisconnectWithWillMessage, Properties: packet.Properties{SessionExpiryInterval: &exp}})
	h.waitDisconnect()
	d := core.disconnects[0]
	if !d.graceful || !d.will || h.conn.SessionExpiryOverride() == nil || *h.conn.SessionExpiryOverride() != 120 {
		t.Errorf("%+v %v", d, h.conn.SessionExpiryOverride())
	}
}

func TestClientDropWithoutDisconnect(t *testing.T) {
	core := &fakeCore{}
	h := newHarness(t, defaultOptions(), core)
	h.connect(packet.V4, nil)
	_ = h.client.Close()
	h.waitDisconnect()
	if d := core.disconnects[0]; d.graceful || !d.will {
		t.Errorf("%+v", d)
	}
}

func TestVerifiedCertificateAuthenticates(t *testing.T) {
	// Exercised end to end in the broker package; here the rule is checked
	// through the request shape a certificate produces.
	opts := defaultOptions()
	opts.Anonymous = false
	req := &auth.Request{CertCN: "device"}
	if !req.HasCredentials() {
		t.Fatal("certificate must count as a credential for the anonymous rule")
	}
}

// goneConn is a connection whose client reports itself gone.
type goneConn struct {
	net.Conn
	gone bool
}

func (g goneConn) PeerGone() bool { return g.gone }

// TestHandshakeRefusesAnAbandonedConnect is WI 1485: a CONNECT whose client
// has already closed the connection ends the handshake before the core is
// asked to connect it, so no lease moves for a client that cannot hear the
// answer; a client still present is connected as before.
func TestHandshakeRefusesAnAbandonedConnect(t *testing.T) {
	for _, gone := range []bool{true, false} {
		core := &fakeCore{connectResult: ConnectResult{Reason: packet.Success}}
		server, client := net.Pipe()
		c := New(goneConn{Conn: server, gone: gone}, "test", defaultOptions(), core)
		done := make(chan struct{})
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			c.Serve(ctx)
			close(done)
		}()
		enc := &packet.Encoder{Version: packet.V5}
		_ = client.SetWriteDeadline(time.Now().Add(2 * time.Second))
		if err := enc.Encode(client, &packet.Connect{ProtocolLevel: packet.V5, ClientID: "cid", CleanStart: true}); err != nil {
			t.Fatal(err)
		}
		dec := packet.NewDecoder(client)
		dec.Version = packet.V5
		_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
		p, err := dec.Read()
		core.mu.Lock()
		connects := len(core.connects)
		core.mu.Unlock()
		if gone {
			if err == nil {
				t.Errorf("abandoned CONNECT was answered with %T", p)
			}
			if connects != 0 {
				t.Errorf("the core connected a client that had gone")
			}
		} else if _, ok := p.(*packet.Connack); !ok || connects != 1 {
			t.Errorf("a present client was not connected: %T %v, %d connects", p, err, connects)
		}
		cancel()
		_ = client.Close()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("Serve did not return")
		}
	}
}
