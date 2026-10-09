package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/hook"
)

// Auth is how peers authenticate: Mode "mtls" with this node's certificate
// and the cluster CA, or Mode "secret" with the shared secret.
type Auth struct {
	Mode   string
	Cert   tls.Certificate
	CA     *x509.CertPool
	Secret []byte
}

// Options configure a transport. Zero values take the defaults noted.
type Options struct {
	Node             string
	ClusterID        string
	Listen           string // empty: accept nothing (valid only with no peers)
	Peers            []config.Peer
	Auth             Auth
	Heartbeat        time.Duration // default 1 s
	HeartbeatMisses  int           // default 3
	MaxFrame         int           // default 16 MiB
	HandshakeTimeout time.Duration // default 10 s
	QueueDepth       int           // frames per peer awaiting the writer; default 1024
	Logger           *slog.Logger
	Metrics          hook.Metrics                                                   // optional
	Audit            func(action, target, outcome string, detail map[string]string) // optional
	// Reorder is the simulation harness's injected defect: writers swap
	// adjacent frames. Never set from configuration.
	Reorder bool

	// MaxVersion lets a node advertise a framing version other than the
	// current one, for negotiation tests. Zero means Version4.
	MaxVersion uint8
}

// urgentDepth is the urgent lane's capacity per link. Renewals are a handful
// per ttl in each direction; a full lane means the writer is wedged, and the
// frame is refused rather than queued behind the wedge.
const urgentDepth = 64

// State of a peer as this node sees it.
type State uint8

// Peer states.
const (
	Down State = iota
	Connecting
	Up
)

func (s State) String() string {
	switch s {
	case Up:
		return "up"
	case Connecting:
		return "connecting"
	}
	return "down"
}

// PeerInfo is a snapshot of one peer.
type PeerInfo struct {
	Name    string
	Address string
	State   State
	Version uint8
	Stress  uint8
	// Reader-side liveness of the current link (WI 1342): how long since a
	// frame was read from the peer, and how long handlers have held the
	// reader — average, worst, and the class of the worst.
	ReadIdleMs   float64
	HandlerAvgMs float64
	HandlerMaxMs float64
	SlowestClass string
	// DispatchWaitMaxMs is the longest a frame waited in its class queue
	// before its handler ran (WI 1340).
	DispatchWaitMaxMs float64
	// DownForMs is how long the peer has not been Up: zero while Up, the
	// time since the last transition out of Up otherwise, or since the
	// transport started for a peer never seen up (WI 1341: a link that
	// resets for seconds under a storm is not a partition).
	DownForMs float64
	// ControlUp is whether the control connection is in place. A peer is Up
	// only once its data connection is too, so a restarted peer can be
	// reachable on the control connection, and asked there, before it is Up.
	ControlUp bool
	// The data connection (WI 1340). On a peer negotiated below version 3
	// it is the same link as the control connection and these repeat it.
	DataState             State
	DataReadIdleMs        float64
	DataHandlerMaxMs      float64
	DataSlowestClass      string
	DataDispatchWaitMaxMs float64
	// Frames admitted to each connection and not yet taken by its writer:
	// the ordered queue and the urgent lane (WI 1474). A frame that never
	// reached the peer is here or in the kernel's send queue; one the peer
	// never read is in its receive queue.
	OutQueued, UrgentQueued         int
	DataOutQueued, DataUrgentQueued int
}

// Handler receives the payload of one frame from a peer, in arrival order
// for that peer.
type Handler func(peer string, payload []byte)

// Observer is told of peer state changes: the control connection's, which
// is the peer's liveness. ObserveData registers for the data connection's,
// which a component whose frames ride it (the forward barrier, the
// replication streams) uses to fail or resync what a reset lost.
type Observer func(peer string, state State)

// Errors returned by Send.
var (
	ErrUnknownPeer = errors.New("unknown peer")
	ErrPeerDown    = errors.New("peer is not connected")
	ErrClosed      = errors.New("transport closed")
	// ErrUnsupportedClass: the link to the peer was negotiated below the
	// version the class needs; the caller falls back to an older path.
	ErrUnsupportedClass = errors.New("class not supported on this link's framing version")
	// ErrUrgentFull: the peer's urgent lane is full, which means its writer
	// is not draining; the frame is refused rather than queued.
	ErrUrgentFull = errors.New("urgent lane full")
)

// Transport is this node's end of the cluster transport.
type Transport struct {
	o          Options
	vmin, vmax uint8
	log        *slog.Logger
	tlsServer  *tls.Config
	tlsClient  *tls.Config

	mu        sync.Mutex
	peers     map[string]*peer
	isolated  map[string]bool // peers cut off for a partition simulation
	heldData  map[string]bool // peers whose data connection this node does not dial, for tests
	handlers  [classCount]Handler
	observers []Observer
	dataObs   []Observer
	ln        net.Listener
	started   bool
	closed    bool
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	stress    atomic.Uint32

	// Test seams. pauseWrites makes every writer stop writing (heartbeats
	// included) until cleared; reorder makes writers swap adjacent frames;
	// dialAll makes this node dial every peer regardless of name order;
	// admitted, when set, is called under the admission lock with each
	// frame's payload so a test can record the admitted order; written, when
	// set, is called by a writer after each frame it writes, heartbeats
	// excepted, so a test can read the order on the wire.
	pauseWrites atomic.Bool
	pauseRole   [roleCount]atomic.Bool // pause the writers of one role only
	reorder     bool
	dialAll     bool
	admitted    func(peer string, payload []byte)
	written     func(peer string, class Class, payload []byte)
}

// New validates the options and prepares the TLS configurations. Nothing is
// bound or dialed until Start.
func New(o Options) (*Transport, error) {
	if o.Node == "" {
		return nil, errors.New("transport: node name required")
	}
	if o.ClusterID == "" {
		o.ClusterID = "hoardmq"
	}
	if o.Heartbeat == 0 {
		o.Heartbeat = time.Second
	}
	if o.HeartbeatMisses == 0 {
		o.HeartbeatMisses = 3
	}
	if o.MaxFrame == 0 {
		o.MaxFrame = 16 << 20
	}
	if o.HandshakeTimeout == 0 {
		o.HandshakeTimeout = 10 * time.Second
	}
	if o.QueueDepth == 0 {
		o.QueueDepth = 1024
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.MaxVersion == 0 {
		o.MaxVersion = Version4
	}
	t := &Transport{o: o, vmin: Version1, vmax: o.MaxVersion, log: o.Logger.With("component", "cluster-transport"), peers: map[string]*peer{}, isolated: map[string]bool{}, heldData: map[string]bool{}}
	if len(o.Peers) > 0 && o.Listen == "" {
		return nil, errors.New("transport: peers configured without a listen address")
	}
	switch o.Auth.Mode {
	case "mtls":
		if o.Auth.CA == nil || len(o.Auth.Cert.Certificate) == 0 {
			return nil, errors.New("transport: mtls requires a certificate and the cluster CA")
		}
		t.tlsServer = &tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{o.Auth.Cert},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    o.Auth.CA,
		}
		t.tlsClient = &tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{o.Auth.Cert},
			RootCAs:      o.Auth.CA,
			// The peer's name is checked against its certificate in the
			// handshake; addresses are operator-chosen and may be IPs.
			InsecureSkipVerify: true, //nolint:gosec // chain is verified below
			VerifyPeerCertificate: func(raw [][]byte, _ [][]*x509.Certificate) error {
				return verifyChain(raw, o.Auth.CA)
			},
		}
	case "secret":
		if len(o.Auth.Secret) < 16 {
			return nil, errors.New("transport: the shared secret must be at least 16 bytes")
		}
	case "":
		if len(o.Peers) > 0 {
			return nil, errors.New("transport: peers configured without an authentication mode")
		}
	default:
		return nil, fmt.Errorf("transport: unknown auth mode %q", o.Auth.Mode)
	}
	for _, p := range o.Peers {
		if p.Name == o.Node {
			return nil, fmt.Errorf("transport: peer %q is this node", p.Name)
		}
		if _, dup := t.peers[p.Name]; dup {
			return nil, fmt.Errorf("transport: duplicate peer %q", p.Name)
		}
		t.peers[p.Name] = newPeer(p)
	}
	t.reorder = o.Reorder
	return t, nil
}

// verifyChain checks the peer's certificate chains to the cluster CA without
// a hostname check; identity is the certificate's common name, matched to
// the claimed node name during the handshake.
func verifyChain(raw [][]byte, ca *x509.CertPool) error {
	if len(raw) == 0 {
		return errors.New("no peer certificate")
	}
	leaf, err := x509.ParseCertificate(raw[0])
	if err != nil {
		return err
	}
	inter := x509.NewCertPool()
	for _, b := range raw[1:] {
		if c, err := x509.ParseCertificate(b); err == nil {
			inter.AddCert(c)
		}
	}
	_, err = leaf.Verify(x509.VerifyOptions{Roots: ca, Intermediates: inter, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny}})
	return err
}

// Start binds the listener (when configured) and starts dialing the peers
// this node is responsible for. A bind failure leaves nothing running.
func (t *Transport) Start() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.started {
		return errors.New("transport already started")
	}
	if t.o.Listen != "" {
		ln, err := net.Listen("tcp", t.o.Listen)
		if err != nil {
			return fmt.Errorf("cluster listener: %w", err)
		}
		t.ln = ln
	}
	t.ctx, t.cancel = context.WithCancel(context.Background())
	t.started = true
	if t.ln != nil {
		t.wg.Add(1)
		go t.acceptLoop()
	}
	for _, p := range t.peers {
		t.startDialer(p)
	}
	return nil
}

// Addr is the bound listener address, or nil.
func (t *Transport) Addr() net.Addr {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ln == nil {
		return nil
	}
	return t.ln.Addr()
}

// Stop closes the listener and every peer connection and waits for the
// goroutines to end.
func (t *Transport) Stop() {
	t.mu.Lock()
	if !t.started || t.closed {
		t.mu.Unlock()
		return
	}
	t.closed = true
	t.cancel()
	if t.ln != nil {
		_ = t.ln.Close()
	}
	peers := make([]*peer, 0, len(t.peers))
	for _, p := range t.peers {
		peers = append(peers, p)
	}
	t.mu.Unlock()
	for _, p := range peers {
		p.closeAll(errors.New("transport stopping"))
	}
	t.wg.Wait()
}

// closing reports whether Stop has begun. adopt checks it either side of
// installing a link, so a link Stop's snapshot missed is one that sees the
// stop itself.
func (t *Transport) closing() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closed
}

// Handle registers the handler for a payload class, replacing any previous
// one. It must be called before frames of that class are expected.
func (t *Transport) Handle(c Class, h Handler) {
	if !c.payloadClass() || c == Heartbeat {
		panic(fmt.Sprintf("transport: %s is not a handler class", c))
	}
	t.mu.Lock()
	t.handlers[c] = h
	t.mu.Unlock()
}

// handler is the registered handler for a class, or nil.
func (t *Transport) handler(c Class) Handler {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.handlers[c]
}

// Observe registers an observer of peer state transitions.
func (t *Transport) Observe(o Observer) {
	t.mu.Lock()
	t.observers = append(t.observers, o)
	t.mu.Unlock()
}

// ObserveData registers an observer of the data connection's transitions.
func (t *Transport) ObserveData(o Observer) {
	t.mu.Lock()
	t.dataObs = append(t.dataObs, o)
	t.mu.Unlock()
}

// SetStress sets the level (0 to 3) this node advertises in heartbeats.
func (t *Transport) SetStress(level uint8) {
	if level > 3 {
		level = 3
	}
	t.stress.Store(uint32(level))
}

// Send admits one frame for a peer. It blocks until the frame is queued for
// the peer's writer or ctx ends; frames from concurrent callers leave in
// admission order. A peer that is not connected fails at once. The payload
// is queued as given, not copied, so the caller must not modify it after
// Send returns. An urgent class (LeaseAlive) goes on the link's urgent lane
// instead: it never blocks, it leaves ahead of the ordered stream, and it
// fails with ErrUrgentFull when the lane is full or ErrUnsupportedClass when
// the link's framing version predates the class.
func (t *Transport) Send(ctx context.Context, peerName string, c Class, payload []byte) error {
	if !c.payloadClass() || c == Heartbeat {
		return fmt.Errorf("transport: %s cannot be sent by callers", c)
	}
	if len(payload) > t.o.MaxFrame {
		return fmt.Errorf("transport: payload of %d bytes exceeds the frame limit of %d", len(payload), t.o.MaxFrame)
	}
	t.mu.Lock()
	p := t.peers[peerName]
	closed := t.closed
	t.mu.Unlock()
	if closed {
		return ErrClosed
	}
	if p == nil {
		return fmt.Errorf("%w: %q", ErrUnknownPeer, peerName)
	}
	if t.isIsolated(peerName) {
		return fmt.Errorf("%w: %q (isolated)", ErrPeerDown, peerName)
	}
	l := p.current(c.lane())
	if l == nil {
		return fmt.Errorf("%w: %q (%s connection)", ErrPeerDown, peerName, c.lane())
	}
	if l.version < c.minVersion() {
		return fmt.Errorf("%w: %s needs framing %d, link to %q is %d", ErrUnsupportedClass, c, c.minVersion(), peerName, l.version)
	}
	if c.urgent() {
		return l.admitUrgent(frame{class: c, payload: payload})
	}
	return l.admit(ctx, frame{class: c, payload: payload})
}

// Peers snapshots every peer.
func (t *Transport) Peers() []PeerInfo {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]PeerInfo, 0, len(t.peers))
	for _, p := range t.peers {
		out = append(out, p.info())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// SetPeers replaces the peer set: new peers are dialed (when this node is
// the dialer for them), removed peers are disconnected. Addresses of kept
// peers are updated for the next dial.
func (t *Transport) SetPeers(peers []config.Peer) {
	t.mu.Lock()
	keep := map[string]bool{}
	var added []*peer
	var removed []*peer
	for _, cp := range peers {
		if cp.Name == t.o.Node {
			continue
		}
		keep[cp.Name] = true
		if p, ok := t.peers[cp.Name]; ok {
			p.setAddress(cp.Address)
			continue
		}
		p := newPeer(cp)
		t.peers[cp.Name] = p
		added = append(added, p)
	}
	for name, p := range t.peers {
		if !keep[name] {
			delete(t.peers, name)
			removed = append(removed, p)
		}
	}
	started := t.started && !t.closed
	if started {
		for _, p := range added {
			t.startDialer(p)
		}
	}
	t.mu.Unlock()
	for _, p := range removed {
		p.removed.Store(true)
		p.closeAll(errors.New("peer removed"))
	}
}

// knownPeer reports whether a name is a configured peer that is not
// isolated.
func (t *Transport) knownPeer(name string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.peers[name]
	return ok && !t.isolated[name]
}

// Reset closes one of a peer's connections, for tests and the simulation
// harness (a link that resets under load is the storm's shape, WI 1342
// round 6); the dialer redials it.
func (t *Transport) Reset(peerName string, role Role, reason string) {
	t.mu.Lock()
	p := t.peers[peerName]
	t.mu.Unlock()
	if p == nil {
		return
	}
	if l := p.current(role); l != nil {
		l.close(errors.New(reason))
	}
}

// Isolate cuts this node off from a peer, or restores it: while isolated,
// frames to the peer fail, its connections are refused and closed, and it
// is not dialed. It simulates a partition for tests and the simulation
// harness; the broker never calls it.
func (t *Transport) Isolate(peerName string, on bool) {
	t.mu.Lock()
	if on {
		t.isolated[peerName] = true
	} else {
		delete(t.isolated, peerName)
	}
	p := t.peers[peerName]
	t.mu.Unlock()
	if on && p != nil {
		p.closeAll(errors.New("isolated"))
	}
}

func (t *Transport) isIsolated(peerName string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.isolated[peerName]
}

// HoldData stops this node dialing a peer's data connection, or lets it dial
// again; the control connection is left alone. Set while the peer is cut off
// (Isolate, or down), at both ends since only the dialing end's hold counts,
// it brings the peer back reachable on the control connection and not Up, as a
// restarted peer is before its data connection is in place. For tests; the
// broker never calls it.
func (t *Transport) HoldData(peerName string, on bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if on {
		t.heldData[peerName] = true
	} else {
		delete(t.heldData, peerName)
	}
}

func (t *Transport) dataHeld(peerName string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.heldData[peerName]
}

// dials reports whether this node is the dialer for a peer: the lower name
// dials, unless the test seam makes every node dial.
func (t *Transport) dials(peerName string) bool {
	return t.dialAll || t.o.Node < peerName
}

// startDialer runs the dial loops for a peer when this node dials it: one
// per connection role. Caller holds t.mu.
func (t *Transport) startDialer(p *peer) {
	if !t.dials(p.name) {
		return
	}
	t.wg.Add(2)
	go t.dialLoop(p, RoleControl)
	go t.dialLoop(p, RoleData)
}

// dialLoop connects a peer's connection of one role, waits for it to end,
// and redials with exponential backoff and jitter for as long as the
// transport runs. The data loop waits for the control connection first,
// and stands aside while that connection is a version-2 link serving both.
func (t *Transport) dialLoop(p *peer, role Role) {
	defer t.wg.Done()
	backoff := 100 * time.Millisecond
	for {
		if t.ctx.Err() != nil || p.removed.Load() {
			return
		}
		if role == RoleData {
			cl := p.current(RoleControl)
			if cl == nil {
				select {
				case <-t.ctx.Done():
					return
				case <-time.After(100 * time.Millisecond):
				}
				continue
			}
			if cl.both {
				<-cl.done
				continue
			}
			if t.dataHeld(p.name) {
				select {
				case <-t.ctx.Done():
					return
				case <-time.After(100 * time.Millisecond):
				}
				continue
			}
		}
		if l := p.current(role); l != nil && l.dialedBy == min(t.o.Node, p.name) {
			// The connection that should win is in place; wait for it to end.
			<-l.done
			continue
		}
		// No connection, or one dialed by the higher-named node that this
		// node's own connection will supersede.
		if role == RoleControl && p.current(role) == nil {
			p.setState(t, Connecting)
		}
		if t.isIsolated(p.name) {
			select {
			case <-t.ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}
		if l, err := t.dial(p, role); err != nil {
			t.log.Debug("dial failed", "peer", p.name, "address", p.address(), "err", err)
		} else if l != nil {
			backoff = 100 * time.Millisecond
			<-l.done
		}
		wait := backoff + time.Duration(rand.Int63n(int64(backoff)/2+1)) //nolint:gosec // jitter, not security
		select {
		case <-t.ctx.Done():
			return
		case <-time.After(wait):
		}
		if backoff < 5*time.Second {
			backoff *= 2
			if backoff > 5*time.Second {
				backoff = 5 * time.Second
			}
		}
	}
}

// dial connects, authenticates and adopts one outbound connection. A nil
// link with a nil error means the connection was superseded on adoption.
func (t *Transport) dial(p *peer, role Role) (*link, error) {
	d := net.Dialer{Timeout: t.o.HandshakeTimeout}
	c, err := d.DialContext(t.ctx, "tcp", p.address())
	if err != nil {
		return nil, err
	}
	if t.tlsClient != nil {
		tc := tls.Client(c, t.tlsClient)
		hctx, cancel := context.WithTimeout(t.ctx, t.o.HandshakeTimeout)
		err := tc.HandshakeContext(hctx)
		cancel()
		if err != nil {
			_ = c.Close()
			t.refused(c, refuse("tls", err))
			return nil, err
		}
		c = tc
	}
	n, err := t.handshake(c, true)
	if err != nil {
		_ = c.Close()
		t.refused(c, err)
		return nil, err
	}
	if n.peer != p.name {
		_ = c.Close()
		err := refuse("unknown-peer", fmt.Errorf("dialed %q but %q answered", p.name, n.peer))
		t.refused(c, err)
		return nil, err
	}
	if n.version >= Version3 {
		// The dialer names the connection's role in its first frame; the
		// hello encoding is frozen, so the role cannot ride there.
		w := newWriter(c)
		_ = c.SetWriteDeadline(time.Now().Add(t.o.HandshakeTimeout))
		err := writeFrame(w, frame{class: ConnectionRole, payload: []byte{byte(role)}})
		if err == nil {
			err = w.Flush()
		}
		_ = c.SetWriteDeadline(time.Time{})
		if err != nil {
			_ = c.Close()
			return nil, err
		}
	} else if role == RoleData {
		// A version-2 peer has one connection; the control loop's link
		// serves both and this dial was a race with it.
		_ = c.Close()
		return nil, nil
	}
	return t.adopt(p, c, n, t.o.Node, role), nil
}

// acceptLoop accepts inbound connections and authenticates each on its own
// goroutine.
func (t *Transport) acceptLoop() {
	defer t.wg.Done()
	for {
		c, err := t.ln.Accept()
		if err != nil {
			if t.ctx.Err() != nil {
				return
			}
			t.log.Warn("cluster accept failed", "err", err)
			select {
			case <-t.ctx.Done():
				return
			case <-time.After(50 * time.Millisecond):
			}
			continue
		}
		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			t.accept(c)
		}()
	}
}

func (t *Transport) accept(c net.Conn) {
	if t.tlsServer != nil {
		tc := tls.Server(c, t.tlsServer)
		hctx, cancel := context.WithTimeout(t.ctx, t.o.HandshakeTimeout)
		err := tc.HandshakeContext(hctx)
		cancel()
		if err != nil {
			_ = c.Close()
			t.refused(c, refuse("tls", err))
			return
		}
		c = tc
	}
	n, err := t.handshake(c, false)
	if err != nil {
		_ = c.Close()
		t.refused(c, err)
		return
	}
	t.mu.Lock()
	p := t.peers[n.peer]
	t.mu.Unlock()
	if p == nil {
		_ = c.Close()
		t.refused(c, refuse("unknown-peer", fmt.Errorf("%q is not a configured peer", n.peer)))
		return
	}
	role := RoleControl
	if n.version >= Version3 {
		// The dialer's first frame names the role; anything else is a
		// protocol error within the handshake timeout.
		_ = c.SetReadDeadline(time.Now().Add(t.o.HandshakeTimeout))
		f, err := readFrame(c, t.o.MaxFrame, false)
		_ = c.SetReadDeadline(time.Time{})
		if err != nil || f.class != ConnectionRole || len(f.payload) != 1 || Role(f.payload[0]) >= roleCount {
			if err == nil {
				err = fmt.Errorf("%w: expected a connection role, got %s", ErrProtocol, f.class)
			}
			_ = c.Close()
			t.refused(c, refuse("protocol", err))
			return
		}
		role = Role(f.payload[0])
	}
	t.adopt(p, c, n, n.peer, role)
}

// adopt installs an authenticated connection as the peer's link, resolving a
// duplicate in favour of the one dialed by the lower-named node and, between
// equals, the newer. It returns the installed link, or nil when the new
// connection lost.
func (t *Transport) adopt(p *peer, c net.Conn, n negotiated, dialedBy string, role Role) *link {
	l := newLink(t, p, c, n.version, dialedBy, role)
	// A connection that finishes its handshake while the transport is
	// stopping must not be started, or its goroutines outlive Stop's wait.
	// Checking before the install is not enough on its own: Stop could set
	// closed and snapshot the peers in between, and never see this link. So
	// the check is made again after the install, and since Stop sets closed
	// before it snapshots, a link Stop missed is a link that sees closed.
	if t.closing() {
		l.close(errors.New("transport stopping"))
		return nil
	}
	slots := []Role{role}
	if l.both {
		slots = []Role{RoleControl, RoleData}
	}
	p.mu.Lock()
	var olds []*link
	for _, r := range slots {
		old := p.links[r]
		if old == nil || old == l {
			continue
		}
		lower := min(t.o.Node, p.name)
		if old.dialedBy == lower && l.dialedBy != lower {
			p.mu.Unlock()
			l.close(errors.New("superseded by the connection dialed by the lower-named node"))
			return nil
		}
		old.superseded = true
		olds = append(olds, old)
	}
	for _, r := range slots {
		p.links[r] = l
	}
	p.version, p.stress = n.version, 0
	p.mu.Unlock()
	for _, old := range olds {
		// The replaced link is no longer current, so its end does not mark
		// the peer down.
		old.close(errors.New("replaced by a newer connection"))
	}
	if t.closing() {
		p.mu.Lock()
		for _, r := range slots {
			if p.links[r] == l {
				p.links[r] = nil
			}
		}
		p.mu.Unlock()
		l.close(errors.New("transport stopping"))
		return nil
	}
	l.start()
	if l.both || role == RoleData {
		p.setDataState(t, Up)
		t.audit("cluster.data.up", p.name, "ok", map[string]string{"address": c.RemoteAddr().String(), "version": fmt.Sprint(n.version)})
	}
	// The peer is up once both its connections are: components act on Up
	// by sending on either lane. A later loss of the data connection alone
	// is a data event, not a peer event (the control connection is the
	// peer's liveness).
	p.mu.Lock()
	ready := p.links[RoleControl] != nil && p.links[RoleData] != nil
	wasUp := p.state == Up
	p.mu.Unlock()
	if ready && !wasUp {
		p.setState(t, Up)
		t.audit("cluster.peer.up", p.name, "ok", map[string]string{"address": c.RemoteAddr().String(), "version": fmt.Sprint(n.version), "connection": role.String()})
		t.gauge()
	}
	return l
}

// refused records a failed handshake.
func (t *Transport) refused(c net.Conn, err error) {
	reason := "connection"
	var r *refusal
	if errors.As(err, &r) {
		reason = r.reason
	}
	level := slog.LevelDebug
	if reason == "cluster-id" || reason == "version" || reason == "certificate" || reason == "mac" {
		level = slog.LevelWarn
	}
	t.log.Log(context.Background(), level, "cluster peer refused", "remote", c.RemoteAddr().String(), "reason", reason, "err", err)
	t.audit("cluster.peer.refused", c.RemoteAddr().String(), reason, map[string]string{"error": err.Error()})
	t.count("hoardmq_cluster_refusals_total", map[string]string{"reason": reason}, 1)
}

func (t *Transport) audit(action, target, outcome string, detail map[string]string) {
	if t.o.Audit != nil {
		t.o.Audit(action, target, outcome, detail)
	}
}

func (t *Transport) count(name string, labels map[string]string, delta float64) {
	if t.o.Metrics != nil {
		t.o.Metrics.Add(name, labels, delta)
	}
}

func (t *Transport) setGauge(name string, labels map[string]string, v float64) {
	if t.o.Metrics != nil {
		t.o.Metrics.Set(name, labels, v)
	}
}

// gauge publishes the connected-peer count.
func (t *Transport) gauge() {
	if t.o.Metrics == nil {
		return
	}
	n := 0
	for _, p := range t.Peers() {
		if p.State == Up {
			n++
		}
	}
	t.o.Metrics.Set("hoardmq_cluster_peers_connected", nil, float64(n))
}

func (t *Transport) notify(peerName string, s State) { t.notifyAll(t.observers, peerName, s) }

func (t *Transport) notifyData(peerName string, s State) { t.notifyAll(t.dataObs, peerName, s) }

func (t *Transport) notifyAll(list []Observer, peerName string, s State) {
	t.mu.Lock()
	obs := append([]Observer(nil), list...)
	t.mu.Unlock()
	for _, o := range obs {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.log.Error("peer observer panicked", "peer", peerName, "panic", r)
				}
			}()
			o(peerName, s)
		}()
	}
}

// peer is one configured peer and its current connections, one per role;
// a version-2 link occupies both slots.
type peer struct {
	name      string
	mu        sync.Mutex
	addr      string
	links     [roleCount]*link
	state     State
	dataState State
	version   uint8
	// downSince is when the peer last left Up; zero while Up.
	downSince time.Time
	stress    uint8
	removed   atomic.Bool
	fault     atomic.Pointer[Fault]
}

// Fault is an injected disturbance on the frames this node writes to one
// peer, for the simulation harness: Drop is the fraction of payload frames
// not written, Delay is added before each write, Reorder swaps adjacent
// frames. Rand must be set when Drop is; heartbeats are never dropped.
// Classes, when set, restricts Drop and Delay to those frame classes, so a
// scenario can silence one plane without taking the node out of the cluster;
// Reorder is decided before the next frame's class is known and always
// applies to the whole link.
type Fault struct {
	Drop    float64
	Delay   time.Duration
	Reorder bool
	Classes []Class
	Rand    *rand.Rand
	randMu  sync.Mutex
}

// affects reports whether the fault applies to a frame's class.
func (f *Fault) affects(c Class) bool {
	if f == nil {
		return false
	}
	if len(f.Classes) == 0 {
		return true
	}
	for _, x := range f.Classes {
		if x == c {
			return true
		}
	}
	return false
}

// SetFault installs (or, with nil, clears) a fault on the frames sent to a
// peer.
func (t *Transport) SetFault(peerName string, f *Fault) {
	t.mu.Lock()
	p := t.peers[peerName]
	t.mu.Unlock()
	if p != nil {
		p.fault.Store(f)
	}
}

// drops decides whether one payload frame is dropped.
func (f *Fault) drops() bool {
	if f == nil || f.Drop <= 0 || f.Rand == nil {
		return false
	}
	f.randMu.Lock()
	defer f.randMu.Unlock()
	return f.Rand.Float64() < f.Drop
}

func newPeer(cp config.Peer) *peer {
	return &peer{name: cp.Name, addr: cp.Address, downSince: time.Now()}
}

func (p *peer) address() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.addr
}

func (p *peer) setAddress(a string) {
	p.mu.Lock()
	p.addr = a
	p.mu.Unlock()
}

// current is the peer's link for a role, or nil.
func (p *peer) current(r Role) *link {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.links[r]
}

// closeAll closes every link the peer has.
func (p *peer) closeAll(err error) {
	p.mu.Lock()
	links := p.links
	p.mu.Unlock()
	for _, l := range links {
		if l != nil {
			l.close(err)
		}
	}
}

func (p *peer) info() PeerInfo {
	p.mu.Lock()
	defer p.mu.Unlock()
	pi := PeerInfo{Name: p.name, Address: p.addr, State: p.state, Version: p.version, Stress: p.stress}
	if p.state != Up && !p.downSince.IsZero() {
		pi.DownForMs = float64(time.Since(p.downSince)) / 1e6
	}
	if l := p.links[RoleControl]; l != nil {
		pi.ControlUp = true
		pi.ReadIdleMs = float64(time.Since(time.Unix(0, l.lastSeen.Load()))) / 1e6
		pi.HandlerMaxMs = float64(l.handlerMaxNs.Load()) / 1e6
		if n := l.handlerCount.Load(); n > 0 {
			pi.HandlerAvgMs = float64(l.handlerNs.Load()) / float64(n) / 1e6
		}
		pi.SlowestClass = Class(l.slowestClass.Load()).String()
		pi.DispatchWaitMaxMs = float64(l.waitMaxNs.Load()) / 1e6
		pi.OutQueued, pi.UrgentQueued = len(l.out), len(l.urgent)
	}
	pi.DataState = p.dataState
	if l := p.links[RoleData]; l != nil {
		pi.DataReadIdleMs = float64(time.Since(time.Unix(0, l.lastSeen.Load()))) / 1e6
		pi.DataHandlerMaxMs = float64(l.handlerMaxNs.Load()) / 1e6
		pi.DataSlowestClass = Class(l.slowestClass.Load()).String()
		pi.DataDispatchWaitMaxMs = float64(l.waitMaxNs.Load()) / 1e6
		pi.DataOutQueued, pi.DataUrgentQueued = len(l.out), len(l.urgent)
	}
	return pi
}

// setDataState records the data connection's transition and tells the
// data observers when it changed.
func (p *peer) setDataState(t *Transport, s State) {
	p.mu.Lock()
	changed := p.dataState != s
	p.dataState = s
	p.mu.Unlock()
	if changed {
		t.notifyData(p.name, s)
	}
}

// setState records a transition and tells the observers when it changed.
func (p *peer) setState(t *Transport, s State) {
	p.mu.Lock()
	changed := p.state != s
	if changed {
		if s == Up {
			p.downSince = time.Time{}
		} else if p.state == Up {
			p.downSince = time.Now()
		}
	}
	p.state = s
	p.mu.Unlock()
	if changed {
		t.notify(p.name, s)
	}
}

// dropLink clears every slot the link holds. It reports whether the link
// was the peer's current control and data connection.
func (p *peer) dropLink(l *link) (control, data bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.links[RoleControl] == l {
		p.links[RoleControl] = nil
		control = true
	}
	if p.links[RoleData] == l {
		p.links[RoleData] = nil
		data = true
	}
	if control {
		p.version = 0
	}
	return control, data
}
