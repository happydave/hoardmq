// Package conformance is the black-box conformance suite: it starts a node
// in-process, drives it over TCP with the pinned Paho clients (and the
// project's own codec for hand-crafted frames), and records which normative
// statements each test observes. The coverage report is generated from that
// record; see cmd/coverage.
package conformance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/happydave/hoardmq/internal/broker"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// Node is a running broker under test.
type Node struct {
	t   *testing.T
	Srv *broker.Server
	TCP string
	Dir string
}

// For returns a view of the node whose failures are reported against t, for
// use inside subtests.
func (n *Node) For(t *testing.T) *Node {
	c := *n
	c.t = t
	return &c
}

// nosubscribeTopic is refused on subscribe by the fixture's ACL, for the
// subscribe-failure scenario.
const nosubscribeTopic = "test/nosubscribe"

// StartNode starts a node with anonymous access, an ACL that allows
// everything except subscribing to nosubscribeTopic, a 1 MB packet limit
// and full 5.0 capabilities, then applies mutate.
func StartNode(t *testing.T, mutate func(*config.Document)) *Node {
	t.Helper()
	dir := t.TempDir()
	acl := filepath.Join(dir, "acl.json")
	rules := `{"rules":[{"topic":"` + nosubscribeTopic + `","deny":["subscribe"]},{"topic":"#","allow":["publish","subscribe","unsubscribe"]},{"topic":"$TopicA/#","allow":["publish","subscribe","unsubscribe"]},{"topic":"$SYS/#","allow":["publish","subscribe","unsubscribe"]}]}`
	if err := os.WriteFile(acl, []byte(rules), 0o600); err != nil {
		t.Fatal(err)
	}
	doc := config.Default()
	doc.Node.DataDir = filepath.Join(dir, "data")
	doc.Auth.Anonymous = true
	doc.Limits.ConnectTimeout = 5
	// A fast consensus clock: a one-member group elects itself in about
	// 100 ms instead of a second, and the first connect needs the lease.
	doc.Cluster.Raft.TickMs = 10
	doc.Cluster.Raft.JoinTimeoutMs = 1000
	doc.Listeners = []config.Listener{{Name: "tcp", Type: "tcp", Address: "127.0.0.1:0"}}
	doc.Modules = map[string]json.RawMessage{"authz.file": json.RawMessage(`{"path":"` + acl + `"}`)}
	if mutate != nil {
		mutate(&doc)
	}
	if p := doc.Validate(); len(p) != 0 {
		t.Fatalf("fixture configuration: %v", p)
	}
	srv, err := broker.New(doc, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	n := &Node{t: t, Srv: srv, TCP: srv.Addr("tcp").String(), Dir: dir}
	t.Cleanup(n.Stop)
	return n
}

// WaitOffline blocks until the session's connection has been released by
// the broker, which happens a moment after a client's DISCONNECT returns.
func (n *Node) WaitOffline(clientID string) {
	n.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		rec, ok := n.Srv.Sessions().Record(clientID)
		if !ok || !rec.Online {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	n.t.Fatalf("session %q still online", clientID)
}

// Stop shuts the node down.
func (n *Node) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = n.Srv.Shutdown(ctx)
}

// --- MQTT 5.0 client over paho.golang ---

// Client5 is a paho.golang client with received messages and server
// disconnects collected on channels.
type Client5 struct {
	t        *testing.T
	C        *paho.Client
	conn     net.Conn
	Messages chan *paho.Publish
	Disconn  chan *paho.Disconnect
	mu       sync.Mutex
	received []*paho.Publish
}

// Connect5 describes a CONNECT for Dial5.
type Connect5 struct {
	ClientID      string
	CleanStart    bool
	KeepAlive     uint16
	SessionExpiry *uint32
	ReceiveMax    *uint16
	TopicAliasMax *uint16
	MaxPacketSize *uint32
	Will          *paho.WillMessage
	WillProps     *paho.WillProperties
	Username      string
	Password      []byte
	// ManualAck stops the client acknowledging QoS 1 and 2 messages until
	// Ack is called, for redelivery and flow-control scenarios.
	ManualAck bool
}

// Dial5 connects a 5.0 client and returns it with the CONNACK. A refused
// CONNECT returns the CONNACK and a nil client.
func (n *Node) Dial5(c Connect5) (*Client5, *paho.Connack, error) {
	n.t.Helper()
	conn, err := net.DialTimeout("tcp", n.TCP, 3*time.Second)
	if err != nil {
		return nil, nil, err
	}
	cl := &Client5{t: n.t, conn: conn, Messages: make(chan *paho.Publish, 256), Disconn: make(chan *paho.Disconnect, 8)}
	cl.C = paho.NewClient(paho.ClientConfig{
		Conn:          conn,
		PacketTimeout: 5 * time.Second,
		OnPublishReceived: []func(paho.PublishReceived) (bool, error){func(pr paho.PublishReceived) (bool, error) {
			cl.mu.Lock()
			cl.received = append(cl.received, pr.Packet)
			cl.mu.Unlock()
			cl.Messages <- pr.Packet
			return true, nil
		}},
		OnServerDisconnect:         func(d *paho.Disconnect) { cl.Disconn <- d },
		OnClientError:              func(error) {},
		EnableManualAcknowledgment: c.ManualAck,
	})
	cp := &paho.Connect{ClientID: c.ClientID, CleanStart: c.CleanStart, KeepAlive: c.KeepAlive, WillMessage: c.Will, WillProperties: c.WillProps}
	if c.Username != "" {
		cp.UsernameFlag, cp.Username = true, c.Username
	}
	if c.Password != nil {
		cp.PasswordFlag, cp.Password = true, c.Password
	}
	if c.SessionExpiry != nil || c.ReceiveMax != nil || c.TopicAliasMax != nil || c.MaxPacketSize != nil {
		cp.Properties = &paho.ConnectProperties{SessionExpiryInterval: c.SessionExpiry, ReceiveMaximum: c.ReceiveMax, TopicAliasMaximum: c.TopicAliasMax, MaximumPacketSize: c.MaxPacketSize}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ack, err := cl.C.Connect(ctx, cp)
	if err != nil {
		_ = conn.Close()
		if ack != nil {
			return nil, ack, err
		}
		return nil, nil, err
	}
	n.t.Cleanup(func() { _ = conn.Close() })
	return cl, ack, nil
}

// Must5 dials and fails the test on a refusal.
func (n *Node) Must5(c Connect5) (*Client5, *paho.Connack) {
	n.t.Helper()
	cl, ack, err := n.Dial5(c)
	if err != nil {
		n.t.Fatalf("connect %q: %v (connack %+v)", c.ClientID, err, ack)
	}
	return cl, ack
}

func ctx5() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}

// Subscribe subscribes with options and returns the SUBACK.
func (c *Client5) Subscribe(props *paho.SubscribeProperties, subs ...paho.SubscribeOptions) *paho.Suback {
	c.t.Helper()
	ctx, cancel := ctx5()
	defer cancel()
	ack, err := c.C.Subscribe(ctx, &paho.Subscribe{Subscriptions: subs, Properties: props})
	if err != nil && ack == nil {
		c.t.Fatalf("subscribe: %v", err)
	}
	return ack
}

// Sub subscribes to one filter at a QoS.
func (c *Client5) Sub(filter string, qos byte) *paho.Suback {
	c.t.Helper()
	return c.Subscribe(nil, paho.SubscribeOptions{Topic: filter, QoS: qos})
}

// Publish publishes and waits for the acknowledgement at QoS 1 and 2.
func (c *Client5) Publish(topic string, qos byte, retain bool, payload []byte, props *paho.PublishProperties) (*paho.PublishResponse, error) {
	ctx, cancel := ctx5()
	defer cancel()
	return c.C.Publish(ctx, &paho.Publish{Topic: topic, QoS: qos, Retain: retain, Payload: payload, Properties: props})
}

// Pub publishes and fails the test on error.
func (c *Client5) Pub(topic string, qos byte, retain bool, payload string, props *paho.PublishProperties) {
	c.t.Helper()
	if _, err := c.Publish(topic, qos, retain, []byte(payload), props); err != nil {
		c.t.Fatalf("publish %q: %v", topic, err)
	}
}

// Unsubscribe removes filters.
func (c *Client5) Unsubscribe(filters ...string) *paho.Unsuback {
	c.t.Helper()
	ctx, cancel := ctx5()
	defer cancel()
	ack, err := c.C.Unsubscribe(ctx, &paho.Unsubscribe{Topics: filters})
	if err != nil && ack == nil {
		c.t.Fatalf("unsubscribe: %v", err)
	}
	return ack
}

// Disconnect sends DISCONNECT with optional properties and closes.
func (c *Client5) Disconnect(props *paho.DisconnectProperties) {
	_ = c.C.Disconnect(&paho.Disconnect{ReasonCode: 0, Properties: props})
	_ = c.conn.Close()
}

// Terminate closes the socket without a DISCONNECT (an abnormal end).
func (c *Client5) Terminate() { _ = c.conn.Close() }

// Ack acknowledges a message when ManualAck is on.
func (c *Client5) Ack(p *paho.Publish) {
	c.t.Helper()
	if err := c.C.Ack(p); err != nil {
		c.t.Fatalf("ack: %v", err)
	}
}

// Expect waits for n messages.
func (c *Client5) Expect(n int, timeout time.Duration) []*paho.Publish {
	c.t.Helper()
	var out []*paho.Publish
	deadline := time.After(timeout)
	for len(out) < n {
		select {
		case m := <-c.Messages:
			out = append(out, m)
		case <-deadline:
			c.t.Fatalf("expected %d messages, got %d after %v: %v", n, len(out), timeout, payloads(out))
		}
	}
	return out
}

// ExpectNone fails if a message arrives within the wait.
func (c *Client5) ExpectNone(wait time.Duration) {
	c.t.Helper()
	select {
	case m := <-c.Messages:
		c.t.Fatalf("unexpected message %q on %s", m.Payload, m.Topic)
	case <-time.After(wait):
	}
}

// Drain discards pending messages.
func (c *Client5) Drain() {
	for {
		select {
		case <-c.Messages:
		default:
			return
		}
	}
}

// WaitDisconnect waits for a server DISCONNECT.
func (c *Client5) WaitDisconnect(timeout time.Duration) *paho.Disconnect {
	c.t.Helper()
	select {
	case d := <-c.Disconn:
		return d
	case <-time.After(timeout):
		c.t.Fatalf("no server DISCONNECT within %v", timeout)
		return nil
	}
}

func payloads(ms []*paho.Publish) []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = string(m.Payload)
	}
	return out
}

// --- MQTT 3.1.1 client over paho.mqtt.golang ---

// Client4 is a paho.mqtt.golang client with messages on a channel and the
// underlying socket captured so a test can drop it without a DISCONNECT.
type Client4 struct {
	t        *testing.T
	C        mqtt.Client
	Messages chan mqtt.Message
	conn     net.Conn
	mu       sync.Mutex
	lost     chan error

	sessionPresent bool
}

// Connect4 describes a 3.1.1 CONNECT.
type Connect4 struct {
	ClientID  string
	Clean     bool
	KeepAlive time.Duration
	Will      *WillMessage
	Username  string
	Password  string
}

// WillMessage is a 3.1.1 Will.
type WillMessage struct {
	Topic    string
	Payload  string
	QoS      byte
	Retained bool
}

// Dial4 connects a 3.1.1 client and returns it, or the connect error.
func (n *Node) Dial4(c Connect4) (*Client4, error) {
	n.t.Helper()
	cl := &Client4{t: n.t, Messages: make(chan mqtt.Message, 256), lost: make(chan error, 4)}
	opts := mqtt.NewClientOptions().AddBroker("tcp://" + n.TCP).SetClientID(c.ClientID).SetCleanSession(c.Clean).
		SetProtocolVersion(4).SetAutoReconnect(false).SetConnectRetry(false).SetOrderMatters(false).
		SetKeepAlive(c.KeepAlive).SetConnectTimeout(5 * time.Second).SetWriteTimeout(5 * time.Second)
	opts.SetDefaultPublishHandler(func(_ mqtt.Client, m mqtt.Message) { cl.Messages <- m })
	opts.SetConnectionLostHandler(func(_ mqtt.Client, err error) { cl.lost <- err })
	opts.SetCustomOpenConnectionFn(func(uri *url.URL, _ mqtt.ClientOptions) (net.Conn, error) {
		conn, err := net.DialTimeout("tcp", uri.Host, 3*time.Second)
		if err != nil {
			return nil, err
		}
		cl.mu.Lock()
		cl.conn = conn
		cl.mu.Unlock()
		return conn, nil
	})
	if c.Will != nil {
		opts.SetWill(c.Will.Topic, c.Will.Payload, c.Will.QoS, c.Will.Retained)
	}
	if c.Username != "" {
		opts.SetUsername(c.Username).SetPassword(c.Password)
	}
	cl.C = mqtt.NewClient(opts)
	tok := cl.C.Connect()
	if !tok.WaitTimeout(5 * time.Second) {
		return nil, errors.New("connect timed out")
	}
	if tok.Error() != nil {
		return nil, tok.Error()
	}
	if ct, ok := tok.(*mqtt.ConnectToken); ok {
		cl.sessionPresent = ct.SessionPresent()
	}
	n.t.Cleanup(func() { cl.Terminate() })
	return cl, nil
}

// Must4 dials and fails the test on error.
func (n *Node) Must4(c Connect4) *Client4 {
	n.t.Helper()
	cl, err := n.Dial4(c)
	if err != nil {
		n.t.Fatalf("connect %q: %v", c.ClientID, err)
	}
	return cl
}

// SessionPresent reports the CONNACK flag of the connect.
func (c *Client4) SessionPresent() bool { return c.sessionPresent }

func wait4(t *testing.T, tok mqtt.Token, what string) {
	t.Helper()
	if !tok.WaitTimeout(5 * time.Second) {
		t.Fatalf("%s timed out", what)
	}
	if tok.Error() != nil {
		t.Fatalf("%s: %v", what, tok.Error())
	}
}

// Sub subscribes to one filter.
func (c *Client4) Sub(filter string, qos byte) {
	c.t.Helper()
	wait4(c.t, c.C.Subscribe(filter, qos, nil), "subscribe")
}

// SubResult subscribes and returns the granted QoS or error code.
func (c *Client4) SubResult(filter string, qos byte) (byte, error) {
	c.t.Helper()
	tok := c.C.Subscribe(filter, qos, nil)
	if !tok.WaitTimeout(5 * time.Second) {
		return 0, errors.New("subscribe timed out")
	}
	if st, ok := tok.(*mqtt.SubscribeToken); ok {
		return st.Result()[filter], tok.Error()
	}
	return 0, tok.Error()
}

// Pub publishes and waits for the acknowledgement.
func (c *Client4) Pub(topic string, qos byte, retained bool, payload string) {
	c.t.Helper()
	wait4(c.t, c.C.Publish(topic, qos, retained, payload), "publish")
}

// Unsub removes filters.
func (c *Client4) Unsub(filters ...string) {
	c.t.Helper()
	wait4(c.t, c.C.Unsubscribe(filters...), "unsubscribe")
}

// Disconnect sends DISCONNECT.
func (c *Client4) Disconnect() { c.C.Disconnect(100) }

// Terminate drops the socket without a DISCONNECT.
func (c *Client4) Terminate() {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

// Expect waits for n messages.
func (c *Client4) Expect(n int, timeout time.Duration) []mqtt.Message {
	c.t.Helper()
	var out []mqtt.Message
	deadline := time.After(timeout)
	for len(out) < n {
		select {
		case m := <-c.Messages:
			out = append(out, m)
		case <-deadline:
			c.t.Fatalf("expected %d messages, got %d after %v", n, len(out), timeout)
		}
	}
	return out
}

// ExpectNone fails if a message arrives within the wait.
func (c *Client4) ExpectNone(wait time.Duration) {
	c.t.Helper()
	select {
	case m := <-c.Messages:
		c.t.Fatalf("unexpected message %q on %s", m.Payload(), m.Topic())
	case <-time.After(wait):
	}
}

// --- Raw frames over the project's codec ---

// Raw is a socket with the project's encoder and decoder, for hand-crafted
// and malformed frames at any protocol level.
type Raw struct {
	t    *testing.T
	Conn net.Conn
	Enc  *packet.Encoder
	Dec  *packet.Decoder
}

// DialRaw opens a raw connection at a protocol level (no CONNECT is sent).
func (n *Node) DialRaw(v packet.Version) *Raw {
	n.t.Helper()
	conn, err := net.DialTimeout("tcp", n.TCP, 3*time.Second)
	if err != nil {
		n.t.Fatal(err)
	}
	n.t.Cleanup(func() { _ = conn.Close() })
	dec := packet.NewDecoder(conn)
	dec.Version = v
	return &Raw{t: n.t, Conn: conn, Enc: &packet.Encoder{Version: v}, Dec: dec}
}

// Send encodes and writes one packet.
func (r *Raw) Send(p packet.Packet) {
	r.t.Helper()
	_ = r.Conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if err := r.Enc.Encode(r.Conn, p); err != nil {
		r.t.Fatalf("send %T: %v", p, err)
	}
}

// SendBytes writes raw bytes.
func (r *Raw) SendBytes(b []byte) {
	r.t.Helper()
	_ = r.Conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if _, err := r.Conn.Write(b); err != nil {
		r.t.Fatalf("write: %v", err)
	}
}

// Read reads the next packet or fails.
func (r *Raw) Read() packet.Packet {
	r.t.Helper()
	_ = r.Conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	p, err := r.Dec.Read()
	if err != nil {
		r.t.Fatalf("read: %v", err)
	}
	return p
}

// Connect sends CONNECT and returns the CONNACK.
func (r *Raw) Connect(c *packet.Connect) *packet.Connack {
	r.t.Helper()
	c.ProtocolLevel = r.Enc.Version
	r.Send(c)
	ack, ok := r.Read().(*packet.Connack)
	if !ok {
		r.t.Fatal("no CONNACK")
	}
	return ack
}

// ReadEnd expects the server to end the connection: it returns the
// DISCONNECT when one is sent (level 5) and nil on a plain close.
func (r *Raw) ReadEnd() *packet.Disconnect {
	r.t.Helper()
	_ = r.Conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	p, err := r.Dec.Read()
	if err != nil {
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			r.t.Fatalf("connection stayed open")
		}
		return nil
	}
	d, ok := p.(*packet.Disconnect)
	if !ok {
		r.t.Fatalf("expected DISCONNECT or close, got %T", p)
	}
	if _, err := r.Dec.Read(); err == nil {
		r.t.Fatal("connection stayed open after DISCONNECT")
	}
	return d
}

// ExpectClosed asserts the server closed the connection without a packet.
func (r *Raw) ExpectClosed() {
	r.t.Helper()
	if d := r.ReadEnd(); d != nil {
		r.t.Fatalf("expected a plain close, got DISCONNECT %s", d.ReasonCode)
	}
}

func u16(v uint16) *uint16 { return &v }
func u32(v uint32) *uint32 { return &v }
func u8(v byte) *byte      { return &v }
func intp(v int) *int      { return &v }

var _ = fmt.Sprintf
