// Package listener opens the client-facing listeners: TCP, TLS, and MQTT over
// WebSocket with or without TLS. Every listener yields net.Conn values that
// carry MQTT bytes; which protocol level a connection speaks is decided by
// the codec afterwards.
package listener

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/happydave/hoardmq/internal/config"
)

// ErrClosed is returned by Accept after Close.
var ErrClosed = errors.New("listener closed")

// Listener is one configured listener.
type Listener struct {
	Config config.Listener

	ln     net.Listener
	certs  *certLoader
	active atomic.Int32
	// maxConns is the connection bound in force, Config's at Open; 0 is none.
	maxConns atomic.Int64
	closed   atomic.Bool
	httpSrv  *http.Server
	upgraded chan net.Conn
	closeCh  chan struct{}
	once     sync.Once
	// MaxFrame bounds a WebSocket frame or reassembled message.
	MaxFrame int

	// slots holds one token per connection in handshake (accepted, no CONNACK
	// yet, not closed). Accept takes a token before accepting and blocks —
	// pause-accept — while none is free; the kernel backlog absorbs the
	// pause. A token returns when the handshake ends either way.
	slots       chan struct{}
	pausedNanos atomic.Int64
	accepted    atomic.Int64 // handshakes that produced a successful CONNACK
	failed      atomic.Int64 // handshakes that ended any other way
	// How long those handshakes held their slots, from accept to the slot's
	// return. Divided by the slot count it is how long every slot would have
	// been taken had they run back to back: a measure of what fills the
	// slots, not of the pause, which depends on how the load bunches.
	acceptedNanos atomic.Int64
	failedNanos   atomic.Int64
	prebuffered   atomic.Int64 // CONNECTs already in the socket at the first read
	abandoned     atomic.Int64 // CONNECTs whose client had closed before the lease was acquired
}

// Open binds the listener. Nothing is accepted until Accept is called; for
// WebSocket listeners the HTTP upgrade loop starts here.
func Open(cfg config.Listener, maxFrame int) (*Listener, error) {
	bound := cfg.MaxHandshakes
	if bound <= 0 {
		bound = config.DefaultMaxHandshakes
	}
	l := &Listener{Config: cfg, MaxFrame: maxFrame, closeCh: make(chan struct{}), slots: make(chan struct{}, bound)}
	l.maxConns.Store(int64(cfg.MaxConnections))
	tcp, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return nil, fmt.Errorf("listener %s: %w", cfg.Name, err)
	}
	l.ln = tcp
	if cfg.IsTLS() {
		tlsCfg, certs, err := tlsConfig(cfg.TLS, cfg.IsWebSocket())
		if err != nil {
			_ = tcp.Close()
			return nil, fmt.Errorf("listener %s: %w", cfg.Name, err)
		}
		l.certs = certs
		l.ln = tls.NewListener(tcp, tlsCfg)
	}
	if cfg.IsWebSocket() {
		l.upgraded = make(chan net.Conn)
		path := cfg.Path
		if path == "" {
			path = config.DefaultWebSocketPath
		}
		mux := http.NewServeMux()
		mux.HandleFunc(path, l.upgrade)
		l.httpSrv = &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
		go func() { _ = l.httpSrv.Serve(l.ln) }()
	}
	return l, nil
}

// Addr is the bound address.
func (l *Listener) Addr() net.Addr { return l.ln.Addr() }

// Accept returns the next connection. When the listener's maxConnections is
// reached the surplus connection is closed at once and Accept waits for the
// next; the specification offers no way to refuse before CONNECT.
func (l *Listener) Accept() (net.Conn, error) {
	for {
		// Pause-accept: wait for a handshake slot before touching the socket
		// queue. Nothing is refused here; connections wait in the kernel.
		select {
		case l.slots <- struct{}{}:
		default:
			start := time.Now()
			select {
			case l.slots <- struct{}{}:
				l.pausedNanos.Add(int64(time.Since(start)))
			case <-l.closeCh:
				return nil, ErrClosed
			}
		}
		var c net.Conn
		var err error
		if l.upgraded != nil {
			select {
			case c = <-l.upgraded:
			case <-l.closeCh:
				return nil, ErrClosed
			}
		} else {
			c, err = l.ln.Accept()
			if err != nil {
				<-l.slots
				if l.closed.Load() {
					return nil, ErrClosed
				}
				var ne net.Error
				if errors.As(err, &ne) && ne.Timeout() {
					continue
				}
				return nil, err
			}
		}
		if max := l.maxConns.Load(); max > 0 && int64(l.active.Load()) >= max {
			_ = c.Close()
			<-l.slots
			continue
		}
		l.active.Add(1)
		return &countedConn{Conn: c, l: l, acceptedAt: time.Now()}, nil
	}
}

// SetMaxConnections changes the connection bound from the next accept; 0 is
// none. Connections over a lowered bound stay.
func (l *Listener) SetMaxConnections(n int) { l.maxConns.Store(int64(n)) }

// MaxConnections is the connection bound in force; 0 is none.
func (l *Listener) MaxConnections() int { return int(l.maxConns.Load()) }

// Active is the number of connections accepted and not yet closed.
func (l *Listener) Active() int { return int(l.active.Load()) }

// Stats is the listener's admission counters, readable without any lock.
type Stats struct {
	Listener      string  `json:"listener"`
	Handshaking   int     `json:"handshaking"`
	MaxHandshakes int     `json:"max_handshakes"`
	PausedSec     float64 `json:"accept_paused_sec"`
	Accepted      int64   `json:"handshakes_accepted"`
	Failed        int64   `json:"handshakes_failed"`
	// AcceptedSlotSec and FailedSlotSec are the slot time those handshakes
	// took, accept to slot return, summed.
	AcceptedSlotSec float64 `json:"handshakes_accepted_slot_sec"`
	FailedSlotSec   float64 `json:"handshakes_failed_slot_sec"`
	Active          int     `json:"active"`
	// Prebuffered counts CONNECTs already in the socket at the first read:
	// sockets that waited in the listen backlog (WI 1338 attribution).
	Prebuffered int64 `json:"connects_prebuffered"`
	// Abandoned counts CONNECTs refused because their client had already
	// closed the connection (WI 1485).
	Abandoned int64 `json:"connects_abandoned"`
}

func (l *Listener) Stats() Stats {
	return Stats{Listener: l.Config.Name, Handshaking: len(l.slots), MaxHandshakes: cap(l.slots),
		PausedSec: float64(l.pausedNanos.Load()) / 1e9, Accepted: l.accepted.Load(), Failed: l.failed.Load(),
		AcceptedSlotSec: float64(l.acceptedNanos.Load()) / 1e9, FailedSlotSec: float64(l.failedNanos.Load()) / 1e9,
		Active: int(l.active.Load()), Prebuffered: l.prebuffered.Load(), Abandoned: l.abandoned.Load()}
}

// Close stops the listener. Accepted connections are not closed.
func (l *Listener) Close() error {
	var err error
	l.once.Do(func() {
		l.closed.Store(true)
		close(l.closeCh)
		if l.httpSrv != nil {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = l.httpSrv.Shutdown(ctx)
		}
		err = l.ln.Close()
	})
	return err
}

type countedConn struct {
	net.Conn
	l    *Listener
	once sync.Once
	done sync.Once // the handshake slot returns exactly once
	// acceptedAt is when Accept returned the connection, its slot already taken.
	acceptedAt time.Time
}

// HandshakeDone returns the connection's handshake slot: the CONNACK went
// out (accepted) or the handshake ended some other way. Close also returns
// it, so a connection that dies before CONNACK cannot leak the slot.
func (c *countedConn) HandshakeDone(accepted bool) {
	c.done.Do(func() {
		held := int64(time.Since(c.acceptedAt))
		if accepted {
			c.l.accepted.Add(1)
			c.l.acceptedNanos.Add(held)
		} else {
			c.l.failed.Add(1)
			c.l.failedNanos.Add(held)
		}
		<-c.l.slots
	})
}

// Prebuffered records that the connection's CONNECT was already buffered
// when the handshake first read the socket.
func (c *countedConn) Prebuffered() { c.l.prebuffered.Add(1) }

// PeerGone reports whether the client has already closed the connection,
// counting it when so. The handshake asks before acquiring the client's
// lease: a CONNECT that waited in the listen backlog while its client gave
// up would otherwise move the lease away from the session that client has
// since opened elsewhere (WI 1485).
func (c *countedConn) PeerGone() bool {
	if !peerGone(c.Conn) {
		return false
	}
	c.l.abandoned.Add(1)
	return true
}

func (c *countedConn) Close() error {
	c.HandshakeDone(false)
	c.once.Do(func() { c.l.active.Add(-1) })
	return c.Conn.Close()
}

// CloseWrite half-closes the underlying connection when it supports that
// (TCP and TLS do); the WebSocket framing does not.
func (c *countedConn) CloseWrite() error {
	if cw, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return errors.ErrUnsupported
}

// PeerCertificate returns the verified TLS client certificate of a
// connection produced by this package, or nil.
func PeerCertificate(c net.Conn) *x509.Certificate {
	if chain := PeerChain(c); len(chain) > 0 {
		return chain[0]
	}
	return nil
}

// PeerChain returns the peer's first verified certificate chain, leaf first,
// or nil when none was verified.
func PeerChain(c net.Conn) []*x509.Certificate {
	for {
		switch x := c.(type) {
		case *countedConn:
			c = x.Conn
		case *wsConn:
			c = x.raw
		case *tls.Conn:
			state := x.ConnectionState()
			if len(state.VerifiedChains) > 0 && len(state.VerifiedChains[0]) > 0 {
				return state.VerifiedChains[0]
			}
			return nil
		default:
			return nil
		}
	}
}

// tlsConfig builds the server configuration. A WebSocket listener negotiates
// http/1.1 through ALPN, because net/http hands a connection whose negotiated
// protocol is anything else to a TLSNextProto handler and closes it when
// there is none; a raw MQTT listener negotiates "mqtt".
func tlsConfig(cfg *config.TLS, websocket bool) (*tls.Config, *certLoader, error) {
	certs, err := newCertLoader(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, nil, err
	}
	tc := &tls.Config{
		GetCertificate: certs.get,
		MinVersion:     tls.VersionTLS12,
		NextProtos:     []string{"mqtt"},
		ClientAuth:     tls.RequestClientCert,
	}
	if websocket {
		tc.NextProtos = []string{"http/1.1"}
	}
	if cfg.MinVersion == "1.3" {
		tc.MinVersion = tls.VersionTLS13
	}
	if cfg.ClientCAFile != "" {
		pem, err := os.ReadFile(cfg.ClientCAFile)
		if err != nil {
			return nil, nil, fmt.Errorf("client CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, nil, fmt.Errorf("client CA %s: no certificates found", cfg.ClientCAFile)
		}
		tc.ClientCAs = pool
		tc.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return tc, certs, nil
}

// AdminTLS is the admin listener's server configuration: the MQTT
// listeners' certificate handling (reloaded when the files change, the
// minimum version) and HTTP/1.1 by ALPN. With a client CA a presented
// certificate must verify at the handshake, but none is required, so a
// bearer token works over TLS without one; without a client CA none is asked.
func AdminTLS(cfg *config.TLS) (*tls.Config, error) {
	tc, _, err := tlsConfig(cfg, true)
	if err != nil {
		return nil, err
	}
	tc.ClientAuth = tls.NoClientCert
	if tc.ClientCAs != nil {
		tc.ClientAuth = tls.VerifyClientCertIfGiven
	}
	return tc, nil
}

// certLoader serves the certificate pair and re-reads the files when either
// changes, checking modification times at most once per second at handshake
// time. A pair that fails to load keeps the previous one in service.
type certLoader struct {
	certFile, keyFile string
	mu                sync.Mutex
	cert              *tls.Certificate
	certMod, keyMod   time.Time
	lastCheck         time.Time
	// LoadError records the last failed reload, for logging by the owner.
	LoadError error
	now       func() time.Time
}

func newCertLoader(certFile, keyFile string) (*certLoader, error) {
	c := &certLoader{certFile: certFile, keyFile: keyFile, now: time.Now}
	if err := c.load(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *certLoader) load() error {
	cert, err := tls.LoadX509KeyPair(c.certFile, c.keyFile)
	if err != nil {
		return fmt.Errorf("certificate: %w", err)
	}
	cs, err := os.Stat(c.certFile)
	if err != nil {
		return err
	}
	ks, err := os.Stat(c.keyFile)
	if err != nil {
		return err
	}
	c.cert, c.certMod, c.keyMod = &cert, cs.ModTime(), ks.ModTime()
	c.lastCheck = c.now()
	return nil
}

func (c *certLoader) get(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.now().Sub(c.lastCheck) >= time.Second {
		c.lastCheck = c.now()
		cs, err1 := os.Stat(c.certFile)
		ks, err2 := os.Stat(c.keyFile)
		if err1 == nil && err2 == nil && (!cs.ModTime().Equal(c.certMod) || !ks.ModTime().Equal(c.keyMod)) {
			prev := c.cert
			if err := c.load(); err != nil {
				c.cert, c.LoadError = prev, err
				// Record the new times so a broken pair is not re-read every second.
				c.certMod, c.keyMod = cs.ModTime(), ks.ModTime()
			} else {
				c.LoadError = nil
			}
		}
	}
	return c.cert, nil
}

// Handshake completes the TLS handshake of a connection produced by this
// package, so that a peer certificate is known before the first MQTT byte is
// read and a handshake failure is distinguishable from a protocol one. It is
// a no-op for plaintext connections.
func Handshake(ctx context.Context, c net.Conn) error {
	for {
		switch x := c.(type) {
		case *countedConn:
			c = x.Conn
		case *wsConn:
			c = x.raw
		case *tls.Conn:
			return x.HandshakeContext(ctx)
		default:
			return nil
		}
	}
}
