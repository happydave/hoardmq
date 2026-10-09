// Package conn runs the per-connection MQTT state machine over the codec:
// the CONNECT handshake with authentication, keep-alive, Receive Maximum in
// both directions, Maximum Packet Size, topic aliases, capability
// enforcement, authorization, and server-initiated DISCONNECT with a reason
// on every error path. It owns nothing durable; the Core it is given owns
// sessions and routing.
package conn

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/hook"
	"github.com/happydave/hoardmq/internal/listener"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/ratelimit"
	"github.com/happydave/hoardmq/internal/topic"
	"github.com/happydave/hoardmq/internal/trace"
)

// Core is what the connection hands packets to once they have passed the
// connection-level rules. Sessions and routing live behind it.
type Core interface {
	// Connect is called after authentication succeeds. It returns whether a
	// session was present and any CONNACK properties; a reason code of
	// Success accepts the connection, anything else refuses it.
	Connect(c *Conn, pkt *packet.Connect) ConnectResult
	// Connected is called once the CONNACK has been queued, so that anything
	// the core sends now (in-flight replays, queued messages) follows it.
	Connected(c *Conn)
	// Publish receives an authorized PUBLISH with its topic alias resolved
	// and returns the reason for PUBACK (QoS 1) or PUBREC (QoS 2), and
	// whether to send it now; an error PUBREC the core holds is sent by the
	// core itself.
	// For QoS 0 the reason is ignored unless it is an error, which closes
	// the connection with it.
	Publish(c *Conn, pkt *packet.Publish) (packet.ReasonCode, bool)
	// Pubrel completes an inbound QoS 2 exchange; the reason goes on PUBCOMP.
	Pubrel(c *Conn, id uint16) (packet.ReasonCode, bool)
	// Puback, Pubrec and Pubcomp report the client's acknowledgements of
	// messages the core delivered. Pubrec's return is the PUBREL reason.
	Puback(c *Conn, id uint16, reason packet.ReasonCode)
	// Pubrec returns the PUBREL's reason and whether to send it at all. A
	// false send holds the exchange at PUBREC: the client must not complete
	// a message the cluster's replicas do not yet know it has, or a session
	// rebuilt from a replica would deliver it a second time.
	Pubrec(c *Conn, id uint16, reason packet.ReasonCode) (packet.ReasonCode, bool)
	Pubcomp(c *Conn, id uint16, reason packet.ReasonCode)
	// Subscribe receives only the filters that passed validation,
	// authorization and capability checks, and returns one reason per filter
	// in the same order.
	Subscribe(c *Conn, pkt *packet.Subscribe) []packet.ReasonCode
	// Subscribed is called once the SUBACK has been queued, so retained
	// messages for the new subscriptions follow it.
	Subscribed(c *Conn)
	Unsubscribe(c *Conn, pkt *packet.Unsubscribe) []packet.ReasonCode
	// Disconnect is called exactly once when an established connection ends.
	// graceful is true for a client DISCONNECT; reason is the client's or the
	// server's code. willWanted reports whether the Will should be published
	// (false after a normal client DISCONNECT).
	Disconnect(c *Conn, graceful bool, reason packet.ReasonCode, willWanted bool)
}

// ConnectResult is the core's answer to a CONNECT.
type ConnectResult struct {
	SessionPresent bool
	Reason         packet.ReasonCode
	Properties     packet.Properties
}

// Options configure every connection of a broker.
type Options struct {
	Capabilities   packet.Capabilities
	MaxPacketSize  uint32 // inbound limit handed to the decoder and advertised
	ReceiveMaximum uint16 // inbound unacknowledged QoS 1 and 2 publishes
	MaxKeepAlive   uint16 // 0 accepts the client's value
	ConnectTimeout time.Duration
	Anonymous      bool
	Authenticators auth.AuthenticatorChain
	// CertificateAuthenticators run before the chain and before any
	// mechanism: a certificate one refuses is refused whatever else the
	// client presents.
	CertificateAuthenticators auth.AuthenticatorChain
	// Mechanisms finds the enhanced-authentication method a CONNECT names;
	// nil offers none.
	Mechanisms func(method string) (auth.Mechanism, bool)
	// DecisionTTL bounds how long an authenticator's allow is held before it
	// is made again; zero leaves only the decision's own expiry.
	DecisionTTL time.Duration
	Authorizers auth.AuthorizerChain
	Namespace   string
	Logger      *slog.Logger
	// Hooks is the module dispatcher; nil means no modules.
	Hooks hook.Interceptor
	// Audit receives authentication failures; nil means no audit log.
	Audit hook.Auditor
	// Metrics receives counters; nil means none.
	Metrics hook.Metrics
	// TraceEvents bounds a per-client trace recording.
	TraceEvents int
	// ConnectGate is asked once per CONNECT that no ban refused, before its
	// credentials are examined; nil admits every one.
	ConnectGate ConnectGate
	// Banned is asked once per CONNECT, before the gate: the kind of the ban
	// the CONNECT meets, if any. Nil bans nothing.
	Banned func(clientID, username string, hasUsername bool, remote string) (kind string, banned bool)
	// ClientLimits gives a namespace's per-client PUBLISH rates; it is asked
	// for every PUBLISH, and a different value than the last rebuilds the
	// connection's buckets. Nil or a nil answer limits nothing.
	ClientLimits func(namespace string) *ClientLimits
	// OnWritable is called, on a goroutine of its own, when a connection that
	// answered a delivery ErrBusy has drained its outbound queue to half.
	OnWritable func(c *Conn)
	// OnEnded is told how every established connection ended.
	OnEnded func(c *Conn, how Ending)
}

// ClientLimits are the per-client PUBLISH rates: packets and topic plus
// payload bytes a second, with their bursts. A rate of 0 is unlimited.
type ClientLimits struct {
	MessageRate, MessageBurst float64
	ByteRate, ByteBurst       float64
}

// Ending is how an established connection ended.
type Ending int

// Endings.
const (
	// EndedGraceful is a client DISCONNECT.
	EndedGraceful Ending = iota
	// EndedByServer is any end the server chose: a takeover, a redirect, an
	// administrative close, a re-decision, shutdown.
	EndedByServer
	// EndedAbnormal is an end on the client's side: the network closed or
	// failed, the keep-alive expired, or the client broke the protocol.
	EndedAbnormal
)

func (e Ending) String() string {
	switch e {
	case EndedGraceful:
		return "graceful"
	case EndedByServer:
		return "server"
	}
	return "abnormal"
}

// ConnectGate decides whether a listener admits one more CONNECT now. limit
// names what refused it ("listener" or "node"), "" when admitted.
type ConnectGate interface {
	AdmitConnect(listener string) (admitted bool, limit string)
}

// ErrQuota is returned by Deliver when the client's Receive Maximum leaves
// no slot for another QoS 1 or 2 message.
var ErrQuota = errors.New("send quota exhausted")

// ErrClosed is returned by Deliver and Send after the connection has ended.
var ErrClosed = errors.New("connection closed")

// ErrBusy is returned by Deliver when the connection's outbound queue is
// full: the caller keeps the message and delivers it after OnWritable.
var ErrBusy = errors.New("outbound queue full")

// Conn is one client connection.
type Conn struct {
	net      net.Conn
	listener string
	opts     Options
	core     Core
	log      *slog.Logger

	dec     *packet.Decoder
	enc     *packet.Encoder
	version packet.Version

	clientID              string
	assignedID            bool
	identity              auth.Identity
	keepAlive             uint16
	connectPkt            *packet.Connect
	established           bool
	sessionExpiryOverride *uint32

	// Peer limits from CONNECT properties.
	clientMaxPacket uint32
	clientAliasMax  uint16
	clientRecvMax   uint16

	mu          sync.Mutex
	quota       int
	inflightIn  map[uint16]struct{}
	aliasesIn   map[uint16]string
	aliasesOut  map[string]uint16
	closed      bool
	closeReason packet.ReasonCode

	tracer atomic.Pointer[trace.Recorder]

	// authReq is what the client presented, kept to re-decide; authMethod is
	// the CONNECT's enhanced-authentication method, "" for none; reauth is a
	// re-authentication in progress, owned by the read loop.
	authReq    *auth.Request
	authMethod string
	reauth     auth.Exchange
	// chainAdmitted says the chain's allow admitted the connection, so a
	// re-decision re-runs the chain as well as the certificate
	// authenticators; recheck is the re-decision's timer, under mu.
	chainAdmitted bool
	recheck       *time.Timer

	out chan packet.Packet
	// writeClosing is closed once the outbound side is shutting down. The
	// outbound channel itself is never closed: senders run on connection,
	// session and cluster goroutines, and closing a channel under a
	// concurrent send panics the process.
	writeClosing   chan struct{}
	writeDone      chan struct{}
	writeErr       error
	closeOnce      sync.Once
	disconnectOnce sync.Once

	// acceptedAt is when the connection was handed to this layer;
	// prebuffered records that its CONNECT was already in the socket when
	// the first read ran — it waited in the listen backlog (WI 1338).
	acceptedAt  time.Time
	prebuffered bool

	// The read loop's own: the per-client buckets and the limits they were
	// built from.
	limits                *ClientLimits
	msgBucket, byteBucket ratelimit.Bucket
	// pubBytes counts the PUBLISH topic and payload bytes read; shed is the
	// pause the overload controller wants before each read, in nanoseconds.
	pubBytes atomic.Int64
	shed     atomic.Int64
	// busy marks a delivery refused for a full outbound queue; clientFault
	// that the connection ended through the client's side.
	busy        atomic.Bool
	clientFault atomic.Bool
}

// prebufferedWithin is how quickly a CONNECT must arrive after the first
// read starts to count as already buffered: a client on the other side of a
// network cannot answer inside it.
const prebufferedWithin = time.Millisecond

// New wraps an accepted connection. Serve runs it.
func New(nc net.Conn, listenerName string, opts Options, core Core) *Conn {
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Conn{
		net:          nc,
		listener:     listenerName,
		opts:         opts,
		core:         core,
		log:          log.With("peer", nc.RemoteAddr().String(), "listener", listenerName),
		inflightIn:   map[uint16]struct{}{},
		out:          make(chan packet.Packet, 32),
		writeClosing: make(chan struct{}),
		writeDone:    make(chan struct{}),
		acceptedAt:   time.Now(),
	}
}

// Age is how long ago the connection was handed to this layer.
func (c *Conn) Age() time.Duration { return time.Since(c.acceptedAt) }

// Prebuffered reports whether the CONNECT was already in the socket when
// the handshake's first read ran.
func (c *Conn) Prebuffered() bool { return c.prebuffered }

// ClientID is the client identifier, assigned or given.
func (c *Conn) ClientID() string { return c.clientID }

// AssignedClientID reports whether the server assigned the identifier.
func (c *Conn) AssignedClientID() bool { return c.assignedID }

// Identity is the authenticated principal.
func (c *Conn) Identity() *auth.Identity { return &c.identity }

// Version is the negotiated protocol level (0 before CONNECT).
func (c *Conn) Version() packet.Version { return c.version }

// RemoteAddr is the peer address.
func (c *Conn) RemoteAddr() net.Addr { return c.net.RemoteAddr() }

// Listener is the name of the listener that accepted the connection.
func (c *Conn) Listener() string { return c.listener }

// SessionExpiryOverride is the Session Expiry Interval a level-5 client sent
// on DISCONNECT, or nil.
func (c *Conn) SessionExpiryOverride() *uint32 { return c.sessionExpiryOverride }

// Serve runs the connection to completion. It returns when the connection
// has ended for any reason; the core's Disconnect has been called by then if
// the connection was established.
func (c *Conn) Serve(ctx context.Context) {
	defer func() { _ = c.net.Close() }()
	hctx, cancel := context.WithTimeout(ctx, c.opts.ConnectTimeout)
	if err := listener.Handshake(hctx, c.net); err != nil {
		cancel()
		c.log.Debug("tls handshake failed", "err", err)
		c.handshakeDone(false)
		return
	}
	cancel()
	ok := c.handshake()
	// The listener's handshake slot returns here whatever the outcome; the
	// accept loop is paused while every slot is taken (pause-accept).
	c.handshakeDone(ok)
	if !ok {
		return
	}
	reason, graceful := c.readLoop(ctx)
	c.finish(reason, graceful)
}

// handshakeDone tells a slot-counting listener the handshake ended; plain
// connections (tests, embedded use) carry no slot and ignore it.
func (c *Conn) handshakeDone(accepted bool) {
	if hd, ok := c.net.(interface{ HandshakeDone(bool) }); ok {
		hd.HandshakeDone(accepted)
	}
}

// handshake reads CONNECT, authenticates and answers CONNACK. It returns
// false when the connection must end. Everything here writes directly,
// before the write loop starts.
func (c *Conn) handshake() bool {
	c.dec = packet.NewDecoder(c.net)
	c.dec.MaxPacketSize = c.opts.MaxPacketSize
	_ = c.net.SetReadDeadline(time.Now().Add(c.opts.ConnectTimeout))
	readStart := time.Now()
	connect, err := c.dec.ReadConnect()
	if err == nil && time.Since(readStart) < prebufferedWithin {
		c.prebuffered = true
		if p, ok := c.net.(interface{ Prebuffered() }); ok {
			p.Prebuffered()
		}
		if c.opts.Metrics != nil {
			c.opts.Metrics.Add("hoardmq_connects_prebuffered_total", map[string]string{"listener": c.listener}, 1)
		}
	}
	if err != nil {
		var perr *packet.Error
		if errors.As(err, &perr) && perr.Reason == packet.UnsupportedProtocolVersion {
			_ = packet.EncodeUnsupportedVersion(c.net, perr.Level)
			c.log.Debug("unsupported protocol version", "level", perr.Level)
		} else if errors.As(err, &perr) && perr.Reason == packet.ClientIdentifierNotValid {
			// Level 3 identifier rule: the codec knows the level was 3.
			c.version = packet.V3
			c.enc = &packet.Encoder{Version: packet.V3}
			c.writeDirect(&packet.Connack{ReasonCode: packet.ClientIdentifierNotValid})
		} else {
			c.log.Debug("connect failed", "err", err)
		}
		return false
	}
	c.version = connect.ProtocolLevel
	c.enc = &packet.Encoder{Version: c.version}
	if c.opts.Banned != nil {
		if kind, banned := c.opts.Banned(connect.ClientID, connect.Username, connect.HasUsername, c.net.RemoteAddr().String()); banned {
			c.refuseBanned(connect.ClientID, kind)
			return false
		}
	}
	if g := c.opts.ConnectGate; g != nil {
		if ok, limit := g.AdmitConnect(c.listener); !ok {
			c.refuseRate(connect.ClientID, limit)
			return false
		}
	}
	c.connectPkt = connect
	p := &connect.Properties
	if p.MaximumPacketSize != nil {
		c.clientMaxPacket = *p.MaximumPacketSize
		c.enc.MaxPacketSize = c.clientMaxPacket
	}
	if p.TopicAliasMaximum != nil {
		c.clientAliasMax = *p.TopicAliasMaximum
	}
	c.clientRecvMax = 65535
	if p.ReceiveMaximum != nil {
		c.clientRecvMax = *p.ReceiveMaximum
	}
	c.quota = int(c.clientRecvMax)

	var mech auth.Mechanism
	if p.AuthenticationMethod != nil {
		m, ok := c.mechanism(*p.AuthenticationMethod)
		if !ok {
			c.refuse(packet.BadAuthenticationMethod)
			return false
		}
		mech, c.authMethod = m, *p.AuthenticationMethod
	}
	if w := connect.Will; w != nil {
		// A Will beyond the declared capabilities is refused at CONNECT.
		if w.Retain && !c.opts.Capabilities.RetainAvailable {
			c.refuse(packet.RetainNotSupported)
			return false
		}
		if w.QoS > c.opts.Capabilities.MaximumQoS {
			c.refuse(packet.QoSNotSupported)
			return false
		}
	}
	if connect.ClientID == "" {
		if c.version == packet.V4 && !connect.CleanStart {
			c.refuse(packet.ClientIdentifierNotValid)
			return false
		}
		connect.ClientID = assignClientID()
		c.assignedID = true
	}
	c.clientID = connect.ClientID

	req := &auth.Request{
		Namespace: c.opts.Namespace, ClientID: c.clientID,
		Username: connect.Username, HasUsername: connect.HasUsername,
		Password: connect.Password, HasPassword: connect.HasPassword,
		PeerAddr: c.net.RemoteAddr().String(), Listener: c.listener,
	}
	if chain := listener.PeerChain(c.net); len(chain) > 0 {
		req.CertChain, req.CertCN = chain, chain[0].Subject.CommonName
	}
	c.authReq = req
	certs := c.opts.CertificateAuthenticators.Authenticate(context.Background(), req)
	var result auth.Result
	var final []byte
	// held is the allow the session holds, re-decided once the connection is
	// established.
	var held *auth.Result
	if mech != nil {
		if certs.Decision == auth.Deny {
			c.refuseAuth(packet.NotAuthorized, certs.Reason, nil)
			return false
		}
		if connect.HasPassword {
			c.refuseAuth(packet.BadUserNameOrPassword, "a password beside an authentication method", &c.authMethod)
			return false
		}
		step, ok := c.exchange(mech, req, p.AuthenticationData)
		if !ok {
			return false
		}
		if connect.HasUsername && connect.Username != step.Username {
			c.refuseAuth(packet.BadUserNameOrPassword, "the username is not the authenticated user", &c.authMethod)
			return false
		}
		result, final = auth.Result{Decision: auth.Allow, Username: step.Username, Attributes: step.Attributes}, step.Data
		if certs.Decision == auth.Allow {
			held = &certs
		}
	} else {
		if certs.Decision == auth.Deny {
			c.refuseAuth(packet.NotAuthorized, certs.Reason, nil)
			return false
		}
		chain := c.opts.Authenticators.Authenticate(context.Background(), req)
		var ok bool
		if result, ok = admission(certs, chain, req, c.opts.Anonymous); !ok {
			c.refuseAuth(refusalCode(req), result.Reason, nil)
			return false
		}
		c.chainAdmitted = chain.Decision == auth.Allow
		if result.Decision == auth.Allow {
			held = &result
		}
	}
	c.identity = auth.Identity{Namespace: c.opts.Namespace, ClientID: c.clientID, Username: connect.Username, CertCN: req.CertCN, Attributes: result.Attributes}
	switch {
	case result.Username != "":
		c.identity.Username = result.Username
	case !connect.HasUsername && req.CertCN != "":
		c.identity.Username = req.CertCN
	}

	// Keep alive.
	c.keepAlive = connect.KeepAlive
	serverKeepAlive := false
	if max := c.opts.MaxKeepAlive; max != 0 && (c.keepAlive == 0 || c.keepAlive > max) {
		c.keepAlive, serverKeepAlive = max, true
	}

	// A client that has already closed the connection cannot receive the
	// CONNACK, and acquiring its lease here would take the lease from the
	// session it may have opened on another node since (WI 1485).
	if g, ok := c.net.(interface{ PeerGone() bool }); ok && g.PeerGone() {
		if c.opts.Metrics != nil {
			c.opts.Metrics.Add("hoardmq_connects_abandoned_total", map[string]string{"listener": c.listener}, 1)
		}
		c.log.Debug("connect abandoned by the client before the session was acquired")
		return false
	}
	res := c.core.Connect(c, connect)
	if res.Reason != packet.Success {
		c.refuseTo(res.Reason, res.Properties.ServerReference)
		return false
	}
	ack := &packet.Connack{SessionPresent: res.SessionPresent, ReasonCode: packet.Success, Properties: res.Properties}
	if c.authMethod != "" {
		method := c.authMethod
		ack.Properties.AuthenticationMethod, ack.Properties.AuthenticationData = &method, final
	}
	if c.version == packet.V5 {
		caps := c.opts.Capabilities
		caps.MaximumPacketSize = c.opts.MaxPacketSize
		caps.ReceiveMaximum = c.opts.ReceiveMaximum
		if serverKeepAlive {
			caps.ServerKeepAlive = c.keepAlive
		}
		merge(&ack.Properties, caps.ConnackProperties())
		if c.assignedID {
			id := c.clientID
			ack.Properties.AssignedClientIdentifier = &id
		}
	}
	// From here on the write loop owns the socket, so a Close from any
	// goroutine queues its DISCONNECT behind the CONNACK.
	c.mu.Lock()
	c.established = true
	c.mu.Unlock()
	go c.writeLoop()
	if err := c.Send(ack); err != nil {
		return false
	}
	if held != nil {
		c.scheduleRecheck(held.Expires)
	}
	c.armKeepAlive()
	c.core.Connected(c)
	if c.opts.Hooks != nil {
		c.opts.Hooks.Connected(context.Background(), c.info())
	}
	return true
}

// HookInfo describes the connection to hooks the session calls.
func (c *Conn) HookInfo() hook.Info { return c.info() }

// info is the connection as hooks see it.
func (c *Conn) info() hook.Info {
	return hook.Info{
		Namespace: c.identity.Namespace, ClientID: c.clientID, Username: c.identity.Username, CertCN: c.identity.CertCN,
		Version: c.version, Listener: c.listener, PeerAddr: c.net.RemoteAddr().String(),
	}
}

func merge(dst *packet.Properties, src packet.Properties) {
	if src.MaximumQoS != nil {
		dst.MaximumQoS = src.MaximumQoS
	}
	if src.RetainAvailable != nil {
		dst.RetainAvailable = src.RetainAvailable
	}
	if src.WildcardSubscriptionAvailable != nil {
		dst.WildcardSubscriptionAvailable = src.WildcardSubscriptionAvailable
	}
	if src.SubscriptionIdentifierAvailable != nil {
		dst.SubscriptionIdentifierAvailable = src.SubscriptionIdentifierAvailable
	}
	if src.SharedSubscriptionAvailable != nil {
		dst.SharedSubscriptionAvailable = src.SharedSubscriptionAvailable
	}
	if src.TopicAliasMaximum != nil {
		dst.TopicAliasMaximum = src.TopicAliasMaximum
	}
	if src.MaximumPacketSize != nil {
		dst.MaximumPacketSize = src.MaximumPacketSize
	}
	if src.ReceiveMaximum != nil {
		dst.ReceiveMaximum = src.ReceiveMaximum
	}
	if src.ServerKeepAlive != nil {
		dst.ServerKeepAlive = src.ServerKeepAlive
	}
}

func assignClientID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("crypto/rand: " + err.Error())
	}
	return "hoardmq-" + base64.RawURLEncoding.EncodeToString(b[:])
}

// refuse answers a CONNACK with the reason (mapped for 3.x) and marks the
// connection refused.
func (c *Conn) refuse(reason packet.ReasonCode) { c.refuseTo(reason, nil) }

// refuseTo is refuse with the Server Reference a redirect carries. Only a
// level 5 CONNACK has anywhere to put it.
func (c *Conn) refuseTo(reason packet.ReasonCode, reference *string) {
	ack := &packet.Connack{ReasonCode: reason}
	if c.version == packet.V5 {
		ack.Properties.ServerReference = reference
	}
	c.writeDirect(ack)
	c.log.Debug("connect refused", "reason", reason.String(), "client", c.clientID)
	c.countRefusal(reason)
	c.auditFailure(reason, "")
}

// refuseRate answers a CONNECT over a connect-rate limit: CONNACK 0x9F at
// level 5; at levels 3 and 4, which have no code for it, the connection is
// closed without a CONNACK (MQTT 3.1.1 § 3.2.2.3).
func (c *Conn) refuseRate(clientID, limit string) {
	reason := packet.ConnectionRateExceeded
	switch {
	case limit == LimitOverload:
		// The node is shedding load: Server busy at level 5, Server
		// unavailable at levels 3 and 4.
		reason = packet.ServerBusy
		c.writeDirect(&packet.Connack{ReasonCode: reason})
	case c.version == packet.V5:
		c.writeDirect(&packet.Connack{ReasonCode: reason})
	}
	c.clientID = clientID
	c.log.Debug("connect refused", "reason", reason.String(), "limit", limit, "client", clientID)
	if c.opts.Metrics != nil {
		c.opts.Metrics.Add("hoardmq_connect_rate_refused_total", map[string]string{"listener": c.listener, "limit": limit}, 1)
	}
	c.countRefusal(reason)
	c.auditFailure(reason, "rate: "+limit)
}

// LimitOverload is the connect gate's limit while the cluster sheds load.
const LimitOverload = "overload"

// refuseBanned answers a CONNECT that meets a ban: CONNACK 0x8A (Banned) at
// level 5, return code 5 (Not authorized) at levels 3 and 4.
func (c *Conn) refuseBanned(clientID, kind string) {
	reason := packet.Banned
	if c.version != packet.V5 {
		reason = packet.NotAuthorized
	}
	c.writeDirect(&packet.Connack{ReasonCode: reason})
	c.clientID = clientID
	c.log.Debug("connect refused", "reason", "banned", "kind", kind, "client", clientID)
	if c.opts.Metrics != nil {
		c.opts.Metrics.Add("hoardmq_connects_banned_total", map[string]string{"listener": c.listener, "kind": kind}, 1)
	}
	c.countRefusal(packet.Banned)
	c.auditFailure(packet.Banned, "ban: "+kind)
}

func (c *Conn) countRefusal(reason packet.ReasonCode) {
	if c.opts.Metrics != nil {
		c.opts.Metrics.Add("hoardmq_auth_failures_total", map[string]string{"listener": c.listener, "reason": reason.String()}, 1)
	}
}

// auditFailure records an authentication failure; why is an authenticator's
// reason, which never holds a credential.
func (c *Conn) auditFailure(reason packet.ReasonCode, why string) {
	if c.opts.Audit == nil {
		return
	}
	actor := c.clientID
	if actor == "" {
		actor = c.net.RemoteAddr().String()
	}
	detail := map[string]string{"peer": c.net.RemoteAddr().String()}
	if why != "" {
		detail["reason"] = why
	}
	c.opts.Audit.Audit(actor, "auth.failure", c.listener, reason.String(), detail)
}

// StartTrace begins recording this connection's packets, replacing any
// recording in progress. A limit of 0 uses the configured default.
func (c *Conn) StartTrace(limit int) {
	if limit <= 0 {
		limit = c.opts.TraceEvents
	}
	if limit <= 0 {
		limit = 1000
	}
	c.tracer.Store(trace.New(limit))
}

// StopTrace ends the recording and returns its events.
func (c *Conn) StopTrace() []trace.Event {
	r := c.tracer.Swap(nil)
	if r == nil {
		return nil
	}
	return r.Events()
}

// Trace returns the events recorded so far, or nil when not recording.
func (c *Conn) Trace() []trace.Event {
	r := c.tracer.Load()
	if r == nil {
		return nil
	}
	return r.Events()
}

func (c *Conn) traceRecord(dir trace.Direction, p packet.Packet) {
	r := c.tracer.Load()
	if r == nil {
		return
	}
	e := trace.Event{Time: time.Now(), Direction: dir, Type: p.Type().String()}
	switch x := p.(type) {
	case *packet.Publish:
		e.PacketID, e.Topic, e.Size = x.PacketID, x.Topic, len(x.Payload)
	case *packet.Puback:
		e.PacketID, e.Reason = x.PacketID, x.ReasonCode.String()
	case *packet.Pubrec:
		e.PacketID, e.Reason = x.PacketID, x.ReasonCode.String()
	case *packet.Pubrel:
		e.PacketID, e.Reason = x.PacketID, x.ReasonCode.String()
	case *packet.Pubcomp:
		e.PacketID, e.Reason = x.PacketID, x.ReasonCode.String()
	case *packet.Subscribe:
		e.PacketID = x.PacketID
		if len(x.Subscriptions) > 0 {
			e.Topic = x.Subscriptions[0].Filter
		}
	case *packet.Suback:
		e.PacketID = x.PacketID
	case *packet.Unsubscribe:
		e.PacketID = x.PacketID
	case *packet.Unsuback:
		e.PacketID = x.PacketID
	case *packet.Disconnect:
		e.Reason = x.ReasonCode.String()
	case *packet.Connack:
		e.Reason = x.ReasonCode.String()
	}
	r.Record(e)
}

// refusalCode is the answer to the chain's refusal: 0x86 when the client
// sent a username or password, which were examined and refused; 0x87
// otherwise. A certificate authenticator's refusal is always 0x87: the
// certificate was refused, not the password.
func refusalCode(req *auth.Request) packet.ReasonCode {
	if req.HasUsername || req.HasPassword {
		return packet.BadUserNameOrPassword
	}
	return packet.NotAuthorized
}

// refuseAuth answers an authentication refusal: the CONNACK (carrying the
// method during an exchange), and the audit record with the authenticator's
// reason, which never holds a credential.
func (c *Conn) refuseAuth(code packet.ReasonCode, reason string, method *string) {
	ack := &packet.Connack{ReasonCode: code}
	if c.version == packet.V5 && method != nil {
		m := *method
		ack.Properties.AuthenticationMethod = &m
	}
	c.writeDirect(ack)
	c.log.Debug("connect refused", "reason", code.String(), "client", c.clientID)
	c.countRefusal(code)
	c.auditFailure(code, reason)
}

// admission is the decision on what the certificate authenticators and the
// chain said: either's deny refuses; the chain's allow admits, then the
// certificate authenticators'; when both pass, a verified certificate alone —
// an authentication the TLS layer performed — admits, and so does anonymous
// access for a client that sent nothing; a username or password no
// authenticator examined does not. The result is what decided.
func admission(certs, chain auth.Result, req *auth.Request, anonymous bool) (auth.Result, bool) {
	switch {
	case certs.Decision == auth.Deny:
		return certs, false
	case chain.Decision == auth.Deny:
		return chain, false
	case chain.Decision == auth.Allow:
		return chain, true
	case certs.Decision == auth.Allow:
		return certs, true
	}
	verifiedCert := req.CertCN != "" && !req.HasUsername && !req.HasPassword
	return chain, verifiedCert || anonymous && !req.HasCredentials()
}

// mechanism finds an offered method.
func (c *Conn) mechanism(method string) (auth.Mechanism, bool) {
	if c.opts.Mechanisms == nil {
		return nil, false
	}
	return c.opts.Mechanisms(method)
}

// exchange runs an enhanced authentication before the CONNACK (§ 4.12): each
// continue is an AUTH 0x18 carrying the method, answered by the client's
// AUTH 0x18 with the same method. Anything else is answered CONNACK 0x82; a
// DISCONNECT or silence ends the exchange without a CONNACK. It returns the
// final step and whether it succeeded; on failure the CONNACK is written.
func (c *Conn) exchange(mech auth.Mechanism, req *auth.Request, data []byte) (auth.Step, bool) {
	method := c.authMethod
	ex := mech.Begin(context.Background(), method, req)
	step := ex.Step(data)
	for step.Kind == auth.Continue {
		m := method
		if !c.writeDirect(&packet.Auth{ReasonCode: packet.ContinueAuthentication, Properties: packet.Properties{AuthenticationMethod: &m, AuthenticationData: step.Data}}) {
			return step, false
		}
		_ = c.net.SetReadDeadline(time.Now().Add(c.opts.ConnectTimeout))
		p, err := c.dec.Read()
		if err != nil {
			c.log.Debug("enhanced authentication ended", "err", err)
			return step, false
		}
		switch x := p.(type) {
		case *packet.Auth:
			if x.ReasonCode != packet.ContinueAuthentication || x.Properties.AuthenticationMethod == nil || *x.Properties.AuthenticationMethod != method {
				c.refuseAuth(packet.ProtocolError, "auth: an AUTH outside the exchange's method or reason", &method)
				return step, false
			}
			step = ex.Step(x.Properties.AuthenticationData)
		case *packet.Disconnect:
			return step, false
		default:
			c.refuseAuth(packet.ProtocolError, "auth: a "+p.Type().String()+" during the exchange", &method)
			return step, false
		}
	}
	c.countExchange(method, step.Kind == auth.Succeed)
	if step.Kind != auth.Succeed {
		code := packet.NotAuthorized
		if step.Checked {
			code = packet.BadUserNameOrPassword
		}
		c.refuseAuth(code, step.Reason, &method)
		return step, false
	}
	return step, true
}

func (c *Conn) countExchange(method string, ok bool) {
	if c.opts.Metrics == nil {
		return
	}
	outcome := "refused"
	if ok {
		outcome = "allowed"
	}
	c.opts.Metrics.Add("hoardmq_auth_exchanges_total", map[string]string{"method": method, "outcome": outcome}, 1)
}

// handleAuth is re-authentication after the CONNACK (§ 4.12.1): AUTH 0x19
// begins an exchange with the connection's method, AUTH 0x18 continues it,
// and the server answers AUTH 0x18 or, on success, AUTH 0x00, through the
// write loop. A failed re-authentication, or one as another user, ends the
// session with 0x87; anything out of order is a Protocol Error.
func (c *Conn) handleAuth(a *packet.Auth) (packet.ReasonCode, bool, bool) {
	m := a.Properties.AuthenticationMethod
	if c.authMethod == "" || m == nil || *m != c.authMethod {
		c.clientFault.Store(true)
		c.Close(packet.ProtocolError)
		return packet.ProtocolError, true, false
	}
	var step auth.Step
	switch {
	case a.ReasonCode == packet.ReAuthenticate && c.reauth == nil:
		mech, ok := c.mechanism(c.authMethod)
		if !ok {
			c.Close(packet.NotAuthorized)
			return packet.NotAuthorized, true, false
		}
		c.reauth = mech.Begin(context.Background(), c.authMethod, c.authReq)
		step = c.reauth.Step(a.Properties.AuthenticationData)
	case a.ReasonCode == packet.ContinueAuthentication && c.reauth != nil:
		step = c.reauth.Step(a.Properties.AuthenticationData)
	default:
		c.clientFault.Store(true)
		c.Close(packet.ProtocolError)
		return packet.ProtocolError, true, false
	}
	method := c.authMethod
	switch step.Kind {
	case auth.Continue:
		_ = c.Send(&packet.Auth{ReasonCode: packet.ContinueAuthentication, Properties: packet.Properties{AuthenticationMethod: &method, AuthenticationData: step.Data}})
		return 0, false, false
	case auth.Succeed:
		c.reauth = nil
		if step.Username != c.identity.Username {
			c.countExchange(method, false)
			c.auditFailure(packet.NotAuthorized, "re-authentication as another user")
			c.Close(packet.NotAuthorized)
			return packet.NotAuthorized, true, false
		}
		c.countExchange(method, true)
		_ = c.Send(&packet.Auth{ReasonCode: packet.Success, Properties: packet.Properties{AuthenticationMethod: &method, AuthenticationData: step.Data}})
		return 0, false, false
	}
	c.reauth = nil
	c.countExchange(method, false)
	c.auditFailure(packet.NotAuthorized, step.Reason)
	c.Close(packet.NotAuthorized)
	return packet.NotAuthorized, true, false
}

// recheckAt is when a held decision is made again: the earlier of its own
// expiry and ttl from now; zero for neither.
func recheckAt(now, expires time.Time, ttl time.Duration) time.Time {
	at := expires
	if ttl > 0 && (at.IsZero() || now.Add(ttl).Before(at)) {
		at = now.Add(ttl)
	}
	return at
}

// scheduleRecheck arms the re-decision for a decision expiring at expires.
func (c *Conn) scheduleRecheck(expires time.Time) {
	at := recheckAt(time.Now(), expires, c.opts.DecisionTTL)
	if at.IsZero() {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	if c.recheck != nil {
		c.recheck.Stop()
	}
	c.recheck = time.AfterFunc(time.Until(at), c.redecide)
}

// redecide makes the held decision again from what the client presented:
// the certificate authenticators, and the chain when its allow admitted the
// connection. A decision that still admits is held again; anything else ends
// the session with 0x87.
func (c *Conn) redecide() {
	ctx, cancel := context.WithTimeout(context.Background(), c.opts.ConnectTimeout)
	defer cancel()
	certs := c.opts.CertificateAuthenticators.Authenticate(ctx, c.authReq)
	result, ok := certs, certs.Decision != auth.Deny
	if ok && c.chainAdmitted {
		result, ok = admission(certs, c.opts.Authenticators.Authenticate(ctx, c.authReq), c.authReq, false)
	}
	if c.opts.Metrics != nil {
		outcome := "refused"
		if ok {
			outcome = "allowed"
		}
		c.opts.Metrics.Add("hoardmq_auth_rechecks_total", map[string]string{"outcome": outcome}, 1)
	}
	if !ok {
		c.auditFailure(packet.NotAuthorized, "re-decided: "+result.Reason)
		c.Close(packet.NotAuthorized)
		return
	}
	if result.Decision == auth.Allow {
		c.scheduleRecheck(result.Expires)
	}
}

// stopRecheck disarms the re-decision when the connection ends.
func (c *Conn) stopRecheck() {
	c.mu.Lock()
	if c.recheck != nil {
		c.recheck.Stop()
	}
	c.mu.Unlock()
}

// writeDirect encodes and writes synchronously, used before the write loop
// exists. It returns false on failure.
func (c *Conn) writeDirect(p packet.Packet) bool {
	_ = c.net.SetWriteDeadline(time.Now().Add(c.opts.ConnectTimeout))
	if err := c.enc.Encode(c.net, p); err != nil {
		c.log.Debug("write failed", "err", err)
		return false
	}
	return true
}

func (c *Conn) armKeepAlive() {
	if c.keepAlive == 0 {
		_ = c.net.SetReadDeadline(time.Time{})
		return
	}
	_ = c.net.SetReadDeadline(time.Now().Add(time.Duration(c.keepAlive) * time.Second * 3 / 2))
}

// readLoop dispatches packets until the connection ends. It returns the
// reason and whether the end was a client DISCONNECT.
func (c *Conn) readLoop(ctx context.Context) (packet.ReasonCode, bool) {
	stop := context.AfterFunc(ctx, func() { c.Close(packet.ServerShuttingDown) })
	defer stop()
	for {
		p, err := c.dec.Read()
		if err != nil {
			return c.readError(err), false
		}
		c.mu.Lock()
		closed := c.closed
		c.mu.Unlock()
		if closed {
			// Draining after a server-side close: nothing is handled.
			continue
		}
		c.armKeepAlive()
		c.traceRecord(trace.In, p)
		if c.opts.Hooks != nil {
			out := c.opts.Hooks.Inbound(context.Background(), c.info(), p)
			switch out.Action {
			case hook.Drop:
				if _, ok := p.(*packet.Publish); ok && c.opts.Metrics != nil {
					c.opts.Metrics.Add("hoardmq_drops_total", map[string]string{"namespace": c.identity.Namespace, "reason": "hook"}, 1)
				}
				if r, done := c.dropInbound(p); done {
					return r, false
				}
				continue
			case hook.Disconnect:
				c.Close(out.Reason)
				return out.Reason, false
			}
		}
		pub, isPub := p.(*packet.Publish)
		if isPub {
			c.pubBytes.Add(int64(len(pub.Topic) + len(pub.Payload)))
		}
		reason, done, graceful := c.dispatch(p)
		if done {
			return reason, graceful
		}
		if isPub {
			c.throttle(pub)
		}
		if d := time.Duration(c.shed.Load()); d > 0 {
			c.wait(d, "shed")
		}
	}
}

// throttle charges a PUBLISH to the client's buckets and, while either is in
// debt, does not read the next packet: TCP carries the back-pressure to the
// client, and nothing it sent is refused.
func (c *Conn) throttle(p *packet.Publish) {
	if c.opts.ClientLimits == nil {
		return
	}
	lim := c.opts.ClientLimits(c.identity.Namespace)
	if lim == nil {
		return
	}
	now := time.Now()
	if lim != c.limits {
		c.limits = lim
		c.msgBucket = ratelimit.New(lim.MessageRate, lim.MessageBurst, now)
		c.byteBucket = ratelimit.New(lim.ByteRate, lim.ByteBurst, now)
	}
	c.msgBucket = c.msgBucket.Charge(now, 1)
	c.byteBucket = c.byteBucket.Charge(now, float64(len(p.Topic)+len(p.Payload)))
	msg, bytes := c.msgBucket.Delay(now, 0), c.byteBucket.Delay(now, 0)
	switch {
	case msg >= bytes && msg > 0:
		c.wait(msg, "message")
	case bytes > 0:
		c.wait(bytes, "byte")
	}
}

// wait pauses the read loop for d, or until the connection closes, then sets
// the keep-alive from the end of the pause: the silence was the broker's.
func (c *Conn) wait(d time.Duration, reason string) {
	if c.opts.Metrics != nil {
		c.opts.Metrics.Add("hoardmq_client_throttled_total", map[string]string{"reason": reason}, 1)
		c.opts.Metrics.Add("hoardmq_client_throttle_seconds_total", map[string]string{"reason": reason}, d.Seconds())
	}
	t := time.NewTimer(d)
	select {
	case <-t.C:
	case <-c.writeClosing:
		t.Stop()
	}
	c.armKeepAlive()
}

// PublishedBytes is the PUBLISH topic and payload bytes read so far.
func (c *Conn) PublishedBytes() int64 { return c.pubBytes.Load() }

// Shed sets the pause the read loop takes before each read; 0 ends it.
func (c *Conn) Shed(d time.Duration) { c.shed.Store(int64(d)) }

// ShedPause is the pause the read loop takes now.
func (c *Conn) ShedPause() time.Duration { return time.Duration(c.shed.Load()) }

// dropInbound discards an intercepted packet. A QoS 1 or 2 PUBLISH is still
// acknowledged, with 0x80, so the client does not resend it for ever.
func (c *Conn) dropInbound(p packet.Packet) (packet.ReasonCode, bool) {
	pub, ok := p.(*packet.Publish)
	if !ok || pub.QoS == 0 {
		return 0, false
	}
	if c.version != packet.V5 {
		return c.refuseClose(packet.UnspecifiedError)
	}
	if pub.QoS == 1 {
		_ = c.Send(&packet.Puback{Ack: packet.Ack{PacketID: pub.PacketID, ReasonCode: packet.UnspecifiedError}})
	} else {
		_ = c.Send(&packet.Pubrec{Ack: packet.Ack{PacketID: pub.PacketID, ReasonCode: packet.UnspecifiedError}})
	}
	return 0, false
}

func (c *Conn) readError(err error) packet.ReasonCode {
	c.mu.Lock()
	closed, reason := c.closed, c.closeReason
	c.mu.Unlock()
	if closed {
		return reason
	}
	c.clientFault.Store(true)
	var perr *packet.Error
	if errors.As(err, &perr) {
		c.Close(perr.Reason)
		return perr.Reason
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		c.Close(packet.KeepAliveTimeout)
		return packet.KeepAliveTimeout
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
		c.markClosed(packet.UnspecifiedError)
		return packet.UnspecifiedError
	}
	c.markClosed(packet.UnspecifiedError)
	return packet.UnspecifiedError
}

// dispatch handles one packet. It returns done=true when the connection is
// over, with the reason and whether it was a graceful client DISCONNECT.
func (c *Conn) dispatch(p packet.Packet) (reason packet.ReasonCode, done, graceful bool) {
	switch x := p.(type) {
	case *packet.Pingreq:
		_ = c.Send(&packet.Pingresp{})
	case *packet.Publish:
		if r, ok := c.handlePublish(x); !ok {
			return r, true, false
		}
	case *packet.Puback:
		c.releaseQuota()
		c.core.Puback(c, x.PacketID, x.ReasonCode)
	case *packet.Pubrec:
		r, send := c.core.Pubrec(c, x.PacketID, x.ReasonCode)
		if x.ReasonCode.IsError() {
			c.releaseQuota()
		} else if send {
			_ = c.Send(&packet.Pubrel{Ack: packet.Ack{PacketID: x.PacketID, ReasonCode: r}})
		}
	case *packet.Pubrel:
		r, send := c.core.Pubrel(c, x.PacketID)
		c.mu.Lock()
		delete(c.inflightIn, x.PacketID)
		c.mu.Unlock()
		if send {
			_ = c.Send(&packet.Pubcomp{Ack: packet.Ack{PacketID: x.PacketID, ReasonCode: r}})
		}
	case *packet.Pubcomp:
		c.releaseQuota()
		c.core.Pubcomp(c, x.PacketID, x.ReasonCode)
	case *packet.Subscribe:
		if r, ok := c.handleSubscribe(x); !ok {
			return r, true, false
		}
	case *packet.Unsubscribe:
		if r, ok := c.handleUnsubscribe(x); !ok {
			return r, true, false
		}
	case *packet.Disconnect:
		if x.Properties.SessionExpiryInterval != nil {
			v := *x.Properties.SessionExpiryInterval
			c.sessionExpiryOverride = &v
		}
		c.markClosed(x.ReasonCode)
		return x.ReasonCode, true, true
	case *packet.Connect:
		c.clientFault.Store(true)
		c.Close(packet.ProtocolError)
		return packet.ProtocolError, true, false
	case *packet.Auth:
		return c.handleAuth(x)
	default:
		// CONNACK, SUBACK, UNSUBACK, PINGRESP from a client are protocol errors.
		c.clientFault.Store(true)
		c.Close(packet.ProtocolError)
		return packet.ProtocolError, true, false
	}
	return 0, false, false
}

func (c *Conn) handlePublish(p *packet.Publish) (packet.ReasonCode, bool) {
	caps := c.opts.Capabilities
	if p.Retain && !caps.RetainAvailable {
		return c.fail(packet.RetainNotSupported)
	}
	if p.QoS > caps.MaximumQoS {
		return c.fail(packet.QoSNotSupported)
	}
	if c.version == packet.V5 {
		if len(p.Properties.SubscriptionIdentifiers) > 0 {
			// Only the server sends Subscription Identifiers in PUBLISH.
			return c.fail(packet.ProtocolError)
		}
		if alias := p.Properties.TopicAlias; alias != nil {
			if *alias == 0 || *alias > caps.TopicAliasMaximum {
				return c.fail(packet.TopicAliasInvalid)
			}
			c.mu.Lock()
			if p.Topic != "" {
				if c.aliasesIn == nil {
					c.aliasesIn = map[uint16]string{}
				}
				c.aliasesIn[*alias] = p.Topic
			} else {
				t, ok := c.aliasesIn[*alias]
				if !ok {
					c.mu.Unlock()
					return c.fail(packet.ProtocolError)
				}
				p.Topic = t
			}
			c.mu.Unlock()
			p.Properties.TopicAlias = nil
		}
	}
	if p.QoS > 0 {
		c.mu.Lock()
		_, dup := c.inflightIn[p.PacketID]
		if !dup && len(c.inflightIn) >= int(c.opts.ReceiveMaximum) {
			c.mu.Unlock()
			return c.fail(packet.ReceiveMaximumExceeded)
		}
		if p.QoS == 2 {
			c.inflightIn[p.PacketID] = struct{}{}
		}
		c.mu.Unlock()
	}
	if c.opts.Authorizers.Authorize(context.Background(), auth.Publish, p.Topic, &c.identity) != auth.Allow {
		if c.opts.Metrics != nil {
			c.opts.Metrics.Add("hoardmq_drops_total", map[string]string{"namespace": c.identity.Namespace, "reason": "refused"}, 1)
		}
		return c.refusePublish(p, packet.NotAuthorized)
	}
	reason, send := c.core.Publish(c, p)
	if p.QoS > 0 && reason.IsError() && c.version != packet.V5 {
		// Levels 3 and 4 have no reason code on PUBACK or PUBREC, and a client
		// reads either as acceptance; closing leaves the publish unacknowledged.
		return c.refuseClose(reason)
	}
	switch p.QoS {
	case 0:
		if reason.IsError() {
			return c.refuseClose(reason)
		}
	case 1:
		if send {
			_ = c.Send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID, ReasonCode: reason}})
		}
	case 2:
		if reason.IsError() {
			c.mu.Lock()
			delete(c.inflightIn, p.PacketID)
			c.mu.Unlock()
		}
		if send {
			_ = c.Send(&packet.Pubrec{Ack: packet.Ack{PacketID: p.PacketID, ReasonCode: reason}})
		}
	}
	return 0, true
}

// refusePublish answers a refused publish: with a reason code on the
// acknowledgement at level 5, and by closing at levels 3 and 4, which have
// no way to say no.
func (c *Conn) refusePublish(p *packet.Publish, reason packet.ReasonCode) (packet.ReasonCode, bool) {
	if c.version != packet.V5 || p.QoS == 0 {
		return c.refuseClose(reason)
	}
	c.mu.Lock()
	delete(c.inflightIn, p.PacketID)
	c.mu.Unlock()
	if p.QoS == 1 {
		_ = c.Send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID, ReasonCode: reason}})
	} else {
		_ = c.Send(&packet.Pubrec{Ack: packet.Ack{PacketID: p.PacketID, ReasonCode: reason}})
	}
	return 0, true
}

func (c *Conn) handleSubscribe(s *packet.Subscribe) (packet.ReasonCode, bool) {
	caps := c.opts.Capabilities
	if len(s.Properties.SubscriptionIdentifiers) > 0 && !caps.SubscriptionIdentifiersAvailable {
		return c.fail(packet.SubscriptionIdentifiersNotSupported)
	}
	codes := make([]packet.ReasonCode, len(s.Subscriptions))
	forward := &packet.Subscribe{PacketID: s.PacketID, Properties: s.Properties}
	var indexes []int
	for i, sub := range s.Subscriptions {
		refused := false
		if c.opts.Hooks != nil && topic.ValidateFilter(sub.Filter) == "" {
			out := c.opts.Hooks.Subscribe(context.Background(), c.info(), &sub)
			switch out.Action {
			case hook.Drop:
				codes[i] = out.Reason
				if codes[i] == 0 {
					codes[i] = packet.UnspecifiedError
				}
				refused = true
			case hook.Disconnect:
				return c.refuseClose(out.Reason)
			}
		}
		switch {
		case refused:
		case topic.ValidateFilter(sub.Filter) != "":
			if c.version != packet.V5 {
				return c.fail(packet.TopicFilterInvalid)
			}
			codes[i] = packet.TopicFilterInvalid
		case topic.IsShared(sub.Filter) && !caps.SharedSubscriptionAvailable:
			codes[i] = packet.SharedSubscriptionsNotSupported
		case topic.HasWildcard(sub.Filter) && !caps.WildcardSubscriptionAvailable:
			codes[i] = packet.WildcardSubscriptionsNotSupported
		case c.opts.Authorizers.Authorize(context.Background(), auth.Subscribe, innerFilter(sub.Filter), &c.identity) != auth.Allow:
			codes[i] = packet.NotAuthorized
		default:
			if sub.QoS > caps.MaximumQoS {
				sub.QoS = caps.MaximumQoS
			}
			forward.Subscriptions = append(forward.Subscriptions, sub)
			indexes = append(indexes, i)
			continue
		}
	}
	if len(forward.Subscriptions) > 0 {
		granted := c.core.Subscribe(c, forward)
		for j, i := range indexes {
			if j < len(granted) {
				codes[i] = granted[j]
			} else {
				codes[i] = packet.UnspecifiedError
			}
		}
	}
	_ = c.Send(&packet.Suback{PacketID: s.PacketID, ReasonCodes: codes})
	if len(forward.Subscriptions) > 0 {
		c.core.Subscribed(c)
	}
	return 0, true
}

func (c *Conn) handleUnsubscribe(u *packet.Unsubscribe) (packet.ReasonCode, bool) {
	codes := make([]packet.ReasonCode, len(u.Filters))
	forward := &packet.Unsubscribe{PacketID: u.PacketID, Properties: u.Properties}
	var indexes []int
	for i, f := range u.Filters {
		switch {
		case topic.ValidateFilter(f) != "":
			codes[i] = packet.TopicFilterInvalid
		case c.opts.Authorizers.Authorize(context.Background(), auth.Unsubscribe, innerFilter(f), &c.identity) != auth.Allow:
			codes[i] = packet.NotAuthorized
		default:
			forward.Filters = append(forward.Filters, f)
			indexes = append(indexes, i)
		}
	}
	if len(forward.Filters) > 0 {
		results := c.core.Unsubscribe(c, forward)
		for j, i := range indexes {
			if j < len(results) {
				codes[i] = results[j]
			} else {
				codes[i] = packet.UnspecifiedError
			}
			if codes[i].IsError() && c.version != packet.V5 {
				// Levels 3 and 4 have no reason code on UNSUBACK, and a client
				// reads it as success; closing leaves the unsubscribe
				// unacknowledged.
				return c.refuseClose(codes[i])
			}
		}
	}
	_ = c.Send(&packet.Unsuback{PacketID: u.PacketID, ReasonCodes: codes})
	return 0, true
}

// innerFilter strips the $share/{name}/ prefix so authorization sees the
// topic filter the group subscribes to, not the share name.
func innerFilter(filter string) string {
	_, inner := topic.SplitShared(filter)
	return inner
}

// fail closes the connection for a packet the client should not have sent
// and reports it to the read loop.
func (c *Conn) fail(reason packet.ReasonCode) (packet.ReasonCode, bool) {
	c.clientFault.Store(true)
	return c.refuseClose(reason)
}

// refuseClose closes the connection for a decision of the server's (a refused
// publish at a level with no way to say no, a hook's verdict) and reports it
// to the read loop. It is not the client's fault, so never a flap.
func (c *Conn) refuseClose(reason packet.ReasonCode) (packet.ReasonCode, bool) {
	c.Close(reason)
	return reason, false
}

func (c *Conn) releaseQuota() {
	c.mu.Lock()
	if c.quota < int(c.clientRecvMax) {
		c.quota++
	}
	c.mu.Unlock()
}

// Send queues a packet for the write loop. It never blocks the caller for
// longer than the queue takes to drain; after Close it returns ErrClosed.
func (c *Conn) Send(p packet.Packet) error {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return ErrClosed
	}
	select {
	case c.out <- p:
		return nil
	case <-c.writeClosing:
		return ErrClosed
	case <-c.writeDone:
		return ErrClosed
	}
}

// Deliver sends a PUBLISH to the client, applying the send quota for QoS 1
// and 2 and the client's topic alias maximum. The packet identifier for QoS
// 1 and 2 is the caller's to choose. ErrQuota means no slot is free; the
// caller should queue and retry when it releases one.
func (c *Conn) Deliver(p *packet.Publish) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClosed
	}
	if p.QoS > 0 && c.quota <= 0 {
		c.mu.Unlock()
		return ErrQuota
	}
	newAlias := ""
	if c.version == packet.V5 && c.clientAliasMax > 0 {
		if alias, ok := c.aliasesOut[p.Topic]; ok {
			cp := *p
			cp.Properties.TopicAlias = &alias
			cp.Topic = ""
			p = &cp
		} else if len(c.aliasesOut) < int(c.clientAliasMax) {
			if c.aliasesOut == nil {
				c.aliasesOut = map[string]uint16{}
			}
			alias := uint16(len(c.aliasesOut) + 1)
			c.aliasesOut[p.Topic], newAlias = alias, p.Topic
			cp := *p
			cp.Properties.TopicAlias = &alias
			p = &cp
		}
	}
	// Never wait for the client's socket: the caller routes on another
	// client's goroutine. A full queue is answered ErrBusy and nothing about
	// the connection changes, the alias just assigned included.
	select {
	case c.out <- p:
		if p.QoS > 0 {
			c.quota--
		}
		c.mu.Unlock()
		return nil
	default:
	}
	if newAlias != "" {
		delete(c.aliasesOut, newAlias)
	}
	c.mu.Unlock()
	c.busy.Store(true)
	if c.opts.Metrics != nil {
		c.opts.Metrics.Add("hoardmq_deliveries_deferred_total", map[string]string{"listener": c.listener}, 1)
	}
	// The write loop may have drained the queue before the mark was set.
	c.writable()
	return ErrBusy
}

// SendQuota is the number of QoS 1 and 2 messages that may still be in
// flight to the client.
func (c *Conn) SendQuota() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.quota
}

func (c *Conn) writeLoop() {
	defer close(c.writeDone)
	for {
		var p packet.Packet
		select {
		case p = <-c.out:
		case <-c.writeClosing:
			// Flush what is already queued — the server's DISCONNECT is
			// usually the last of it — then stop.
			for {
				select {
				case q := <-c.out:
					if err := c.write(q); err != nil {
						c.writeErr = err
						c.writeFailed()
						c.markClosed(packet.UnspecifiedError)
						_ = c.net.Close()
						return
					}
				default:
					return
				}
			}
		}
		if err := c.write(p); err != nil {
			c.writeErr = err
			c.writeFailed()
			c.markClosed(packet.UnspecifiedError)
			_ = c.net.Close()
			return
		}
		c.writable()
	}
}

// writeFailed marks a write error as the client's side failing, unless the
// server had already ended the connection (a hook's disconnect, a takeover).
func (c *Conn) writeFailed() {
	c.mu.Lock()
	if !c.closed {
		c.clientFault.Store(true)
	}
	c.mu.Unlock()
}

// writable calls OnWritable once a connection that refused a delivery for a
// full queue has drained it to half.
func (c *Conn) writable() {
	if c.busy.Load() && len(c.out) <= cap(c.out)/2 && c.busy.CompareAndSwap(true, false) && c.opts.OnWritable != nil {
		go c.opts.OnWritable(c)
	}
}

// write encodes one packet, applying the outbound Maximum Packet Size rule:
// a PUBLISH that does not fit is dropped; another packet is retried without
// Reason String and User Properties; if it still does not fit the connection
// closes with Packet Too Large.
func (c *Conn) write(p packet.Packet) error {
	if c.opts.Hooks != nil {
		out := c.opts.Hooks.Outbound(context.Background(), c.info(), p)
		switch out.Action {
		case hook.Drop:
			if _, ok := p.(*packet.Publish); ok {
				return nil
			}
			c.log.Debug("outbound drop ignored for a control packet", "type", p.Type().String())
		case hook.Disconnect:
			if ack, ok := p.(*packet.Connack); ok {
				p = &packet.Connack{SessionPresent: ack.SessionPresent, ReasonCode: out.Reason}
			} else {
				c.markClosed(out.Reason)
				_ = c.enc.Encode(c.net, &packet.Disconnect{ReasonCode: out.Reason})
				return fmt.Errorf("hook disconnect: %s", out.Reason)
			}
		}
	}
	c.traceRecord(trace.Out, p)
	_ = c.net.SetWriteDeadline(time.Now().Add(30 * time.Second))
	err := c.enc.Encode(c.net, p)
	var perr *packet.Error
	if err == nil || !errors.As(err, &perr) || perr.Reason != packet.PacketTooLarge {
		return err
	}
	if _, ok := p.(*packet.Publish); ok {
		c.log.Debug("publish dropped: exceeds client maximum packet size", "client", c.clientID)
		return nil
	}
	if stripped, ok := stripOptional(p); ok {
		if err := c.enc.Encode(c.net, stripped); err == nil {
			return nil
		}
	}
	c.markClosed(packet.PacketTooLarge)
	return fmt.Errorf("packet too large for client: %w", err)
}

func stripOptional(p packet.Packet) (packet.Packet, bool) {
	strip := func(props *packet.Properties) {
		props.ReasonString = nil
		props.UserProperties = nil
	}
	switch x := p.(type) {
	case *packet.Connack:
		cp := *x
		strip(&cp.Properties)
		return &cp, true
	case *packet.Puback:
		cp := *x
		strip(&cp.Properties)
		return &cp, true
	case *packet.Pubrec:
		cp := *x
		strip(&cp.Properties)
		return &cp, true
	case *packet.Pubrel:
		cp := *x
		strip(&cp.Properties)
		return &cp, true
	case *packet.Pubcomp:
		cp := *x
		strip(&cp.Properties)
		return &cp, true
	case *packet.Suback:
		cp := *x
		strip(&cp.Properties)
		return &cp, true
	case *packet.Unsuback:
		cp := *x
		strip(&cp.Properties)
		return &cp, true
	case *packet.Disconnect:
		cp := *x
		strip(&cp.Properties)
		return &cp, true
	case *packet.Auth:
		cp := *x
		strip(&cp.Properties)
		return &cp, true
	}
	return nil, false
}

// markClosed records that the connection is over without writing anything.
func (c *Conn) markClosed(reason packet.ReasonCode) {
	c.mu.Lock()
	if !c.closed {
		c.closed, c.closeReason = true, reason
	}
	c.mu.Unlock()
}

// Abort drops the connection without a DISCONNECT, as a crashed process
// would; for the simulation harness.
func (c *Conn) Abort() {
	c.markClosed(packet.ServerShuttingDown)
	_ = c.net.Close()
}

// Close ends the connection from the server side: at level 5 a DISCONNECT
// with the reason is sent first; at levels 3 and 4 the socket is closed. It
// is safe to call from any goroutine and more than once.
func (c *Conn) Close(reason packet.ReasonCode) { c.closeWith(&packet.Disconnect{ReasonCode: reason}) }

// Redirect closes the connection telling a level 5 client where to go: a
// DISCONNECT with Use Another Server and the Server Reference. Older
// clients cannot be redirected and are closed.
func (c *Conn) Redirect(reference string) {
	ref := reference
	c.closeWith(&packet.Disconnect{ReasonCode: packet.UseAnotherServer, Properties: packet.Properties{ServerReference: &ref}})
}

func (c *Conn) closeWith(d *packet.Disconnect) {
	reason := d.ReasonCode
	c.closeOnce.Do(func() {
		c.mu.Lock()
		if !c.closed {
			c.closed, c.closeReason = true, reason
		}
		established := c.established
		c.mu.Unlock()
		if established && c.version == packet.V5 {
			select {
			case c.out <- d:
			default:
			}
		}
		// Signalling the write loop lets it flush the DISCONNECT and exit;
		// the socket closes after that.
		close(c.writeClosing)
		go func() {
			select {
			case <-c.writeDone:
			case <-time.After(5 * time.Second):
			}
			c.halfClose()
		}()
	})
}

// drainGrace bounds how long a closed connection keeps reading after the
// server's DISCONNECT. Closing a socket that still holds unread input resets
// the connection, which can destroy the DISCONNECT in flight; a half-close
// followed by a short drain lets the client read it.
const drainGrace = 500 * time.Millisecond

// halfClose stops the outbound side and leaves the inbound side open for
// the drain grace. Transports without a half-close simply close.
func (c *Conn) halfClose() {
	if cw, ok := c.net.(interface{ CloseWrite() error }); ok {
		_ = cw.CloseWrite()
		_ = c.net.SetReadDeadline(time.Now().Add(drainGrace))
		return
	}
	_ = c.net.Close()
}

// drain discards input until the client closes or the grace ends.
func (c *Conn) drain() {
	_ = c.net.SetReadDeadline(time.Now().Add(drainGrace))
	buf := make([]byte, 4096)
	for {
		if _, err := c.net.Read(buf); err != nil {
			return
		}
	}
}

// finish runs after the read loop: it closes the outbound side if the client
// ended the connection, waits for the writer, and reports to the core.
func (c *Conn) finish(reason packet.ReasonCode, graceful bool) {
	if graceful {
		c.closeOnce.Do(func() { close(c.writeClosing) })
	} else {
		c.Close(reason)
	}
	<-c.writeDone
	c.stopRecheck()
	c.disconnectOnce.Do(func() {
		willWanted := !graceful || reason == packet.DisconnectWithWillMessage
		c.core.Disconnect(c, graceful, reason, willWanted)
		if c.opts.Hooks != nil {
			c.opts.Hooks.Disconnected(context.Background(), c.info(), reason, graceful)
		}
		if c.opts.OnEnded != nil {
			c.opts.OnEnded(c, ending(graceful, c.clientFault.Load()))
		}
	})
	if !graceful {
		c.drain()
	}
}

// ending is how a connection ended: a client DISCONNECT is graceful, an end
// through the client's side (the network, the keep-alive, a protocol error)
// abnormal, and anything else the server's choice.
func ending(graceful, clientFault bool) Ending {
	switch {
	case graceful:
		return EndedGraceful
	case clientFault:
		return EndedAbnormal
	}
	return EndedByServer
}
