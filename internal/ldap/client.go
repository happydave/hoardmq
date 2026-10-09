package ldap

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sync"
	"time"
)

// ErrDisconnected is the directory's notice of disconnection, or a response
// to a message this connection did not send.
var ErrDisconnected = errors.New("ldap: the directory ended the session")

// Endpoint is where and how to reach a directory.
type Endpoint struct {
	// Address is host:port.
	Address string
	// Host is the TLS server name.
	Host string
	// TLS: from the first byte (ldaps), or after StartTLS on a plain
	// connection; neither for a plain connection.
	LDAPS    bool
	StartTLS bool
	// Config is the TLS configuration; nil uses the system roots.
	Config *tls.Config
}

// ParseURL reads ldap://host[:port] or ldaps://host[:port].
func ParseURL(raw string) (Endpoint, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return Endpoint{}, err
	}
	if u.Host == "" || u.Hostname() == "" || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.User != nil {
		return Endpoint{}, fmt.Errorf("url %q: want ldap://host[:port] or ldaps://host[:port]", raw)
	}
	e := Endpoint{Host: u.Hostname()}
	port := u.Port()
	switch u.Scheme {
	case "ldap":
		if port == "" {
			port = "389"
		}
	case "ldaps":
		e.LDAPS = true
		if port == "" {
			port = "636"
		}
	default:
		return Endpoint{}, fmt.Errorf("url %q: the scheme must be ldap or ldaps", raw)
	}
	e.Address = net.JoinHostPort(e.Host, port)
	return e, nil
}

// Loopback reports whether the endpoint's host is a loopback address or
// localhost, the only place a password may go without TLS.
func (e Endpoint) Loopback() bool {
	if e.Host == "localhost" {
		return true
	}
	ip := net.ParseIP(e.Host)
	return ip != nil && ip.IsLoopback()
}

// Encrypted reports whether a connection to the endpoint is TLS.
func (e Endpoint) Encrypted() bool { return e.LDAPS || e.StartTLS }

func (e Endpoint) tlsConfig() *tls.Config {
	c := &tls.Config{MinVersion: tls.VersionTLS12}
	if e.Config != nil {
		c = e.Config.Clone()
		if c.MinVersion < tls.VersionTLS12 {
			c.MinVersion = tls.VersionTLS12
		}
	}
	if c.ServerName == "" {
		c.ServerName = e.Host
	}
	return c
}

// Conn is one connection to a directory, used by one caller at a time.
type Conn struct {
	nc   net.Conn
	r    *bufio.Reader
	next int
	// answers counts the responses read to this connection's own requests,
	// so a caller can tell whether a failure came before the directory
	// answered; a notice of disconnection is not an answer.
	answers int
	tls     bool
}

// Dial connects, with TLS from the first byte or by StartTLS as the endpoint
// says, within ctx.
func Dial(ctx context.Context, e Endpoint) (*Conn, error) {
	var d net.Dialer
	nc, err := d.DialContext(ctx, "tcp", e.Address)
	if err != nil {
		return nil, err
	}
	c := &Conn{nc: nc, r: bufio.NewReader(nc), next: 1}
	if e.LDAPS {
		if err := c.handshake(ctx, e); err != nil {
			_ = nc.Close()
			return nil, err
		}
		return c, nil
	}
	if e.StartTLS {
		r, err := c.roundTrip(ctx, StartTLSRequest(c.id()))
		if err != nil {
			_ = nc.Close()
			return nil, err
		}
		if r.Op != opExtendedResult || r.Code != Success {
			_ = nc.Close()
			return nil, fmt.Errorf("ldap: StartTLS refused (%d)", r.Code)
		}
		if err := c.handshake(ctx, e); err != nil {
			_ = nc.Close()
			return nil, err
		}
	}
	return c, nil
}

func (c *Conn) handshake(ctx context.Context, e Endpoint) error {
	tc := tls.Client(c.nc, e.tlsConfig())
	if err := tc.HandshakeContext(ctx); err != nil {
		return err
	}
	c.nc, c.r, c.tls = tc, bufio.NewReader(tc), true
	return nil
}

// TLS reports whether the connection is encrypted.
func (c *Conn) TLS() bool { return c.tls }

// Answers is how many responses to its own requests this connection has
// read.
func (c *Conn) Answers() int { return c.answers }

func (c *Conn) id() int {
	id := c.next
	c.next++
	if c.next > 1<<30 {
		c.next = 1
	}
	return id
}

// Close sends an unbind, best effort, and closes.
func (c *Conn) Close() {
	_ = c.nc.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
	_, _ = c.nc.Write(UnbindRequest(c.id()))
	_ = c.nc.Close()
}

func (c *Conn) deadline(ctx context.Context) {
	if d, ok := ctx.Deadline(); ok {
		_ = c.nc.SetDeadline(d)
	} else {
		_ = c.nc.SetDeadline(time.Time{})
	}
}

// send writes one request.
func (c *Conn) send(ctx context.Context, msg []byte) error {
	c.deadline(ctx)
	_, err := c.nc.Write(msg)
	return err
}

// read reads the next response for id; a notice of disconnection or a
// response for another message ends the session.
func (c *Conn) read(id int) (Response, error) {
	raw, err := readMessage(c.r, MaxMessage)
	if err != nil {
		return Response{}, err
	}
	r, err := ParseResponse(raw)
	if err != nil {
		return Response{}, err
	}
	if r.ID != id {
		return Response{}, ErrDisconnected
	}
	c.answers++
	return r, nil
}

func (c *Conn) roundTrip(ctx context.Context, msg []byte) (Response, error) {
	id := idOf(msg)
	if err := c.send(ctx, msg); err != nil {
		return Response{}, err
	}
	return c.read(id)
}

// idOf reads a request's message identifier back from its encoding.
func idOf(msg []byte) int {
	env, _, _ := readTLV(msg)
	t, _, _ := readTLV(env.value)
	n, _ := parseInt(t.value)
	return int(n)
}

// Bind is a simple bind; it returns the directory's result code. An empty
// name and password is an anonymous bind.
func (c *Conn) Bind(ctx context.Context, dn string, password []byte) (int, error) {
	r, err := c.roundTrip(ctx, BindRequest(c.id(), dn, password))
	if err != nil {
		return 0, err
	}
	if r.Op != opBindResponse {
		return 0, fmt.Errorf("%w: a bind answered by operation %d", ErrMalformed, r.Op)
	}
	return r.Code, nil
}

// Search returns the DNs of the entries found, up to sizeLimit, and the
// search's final result code. References are ignored.
func (c *Conn) Search(ctx context.Context, base string, scope Scope, sizeLimit int, filter []byte) ([]string, int, error) {
	timeLimit := 0
	if d, ok := ctx.Deadline(); ok {
		timeLimit = max(1, int(time.Until(d)/time.Second))
	}
	id := c.id()
	if err := c.send(ctx, SearchRequest(id, base, scope, sizeLimit, timeLimit, filter)); err != nil {
		return nil, 0, err
	}
	var dns []string
	for {
		r, err := c.read(id)
		if err != nil {
			return nil, 0, err
		}
		switch r.Op {
		case opSearchEntry:
			if len(dns) <= sizeLimit {
				dns = append(dns, r.DN)
			}
		case opSearchRef:
		case opSearchDone:
			return dns, r.Code, nil
		default:
			return nil, 0, fmt.Errorf("%w: a search answered by operation %d", ErrMalformed, r.Op)
		}
	}
}

// Pool holds at most Max connections to one endpoint.
type Pool struct {
	Endpoint Endpoint
	Max      int
	// OnDial is told each dial's outcome.
	OnDial func(ok bool)

	once sync.Once
	sem  chan struct{}
	mu   sync.Mutex
	idle []*Conn
	dial func(ctx context.Context, e Endpoint) (*Conn, error)
}

func (p *Pool) init() {
	p.once.Do(func() {
		if p.Max < 1 {
			p.Max = 1
		}
		p.sem = make(chan struct{}, p.Max)
		if p.dial == nil {
			p.dial = Dial
		}
	})
}

// RetryFresh is the decision to run an operation again on a new connection:
// only when the one that failed was reused from the pool and the directory
// had answered nothing on it for this operation — the failure of a connection
// the server closed while it sat idle, not an answer.
func RetryFresh(reused bool, answersBefore, answersAfter int, err error) bool {
	return err != nil && reused && answersAfter == answersBefore
}

// Do runs op on a pooled connection within ctx: waiting for a free slot,
// dialling when no idle connection is left, closing a connection that failed,
// and retrying once on a fresh one as RetryFresh decides.
func (p *Pool) Do(ctx context.Context, op func(*Conn) error) error {
	p.init()
	select {
	case p.sem <- struct{}{}:
	case <-ctx.Done():
		return fmt.Errorf("ldap: no free connection: %w", ctx.Err())
	}
	defer func() { <-p.sem }()
	c, reused, err := p.take(ctx)
	if err != nil {
		return err
	}
	before := c.answers
	err = op(c)
	if err != nil {
		c.Close()
		if !RetryFresh(reused, before, c.answers, err) || ctx.Err() != nil {
			return err
		}
		c, err = p.fresh(ctx)
		if err != nil {
			return err
		}
		if err = op(c); err != nil {
			c.Close()
			return err
		}
	}
	p.mu.Lock()
	p.idle = append(p.idle, c)
	p.mu.Unlock()
	return nil
}

func (p *Pool) take(ctx context.Context) (*Conn, bool, error) {
	p.mu.Lock()
	if n := len(p.idle); n > 0 {
		c := p.idle[n-1]
		p.idle = p.idle[:n-1]
		p.mu.Unlock()
		return c, true, nil
	}
	p.mu.Unlock()
	c, err := p.fresh(ctx)
	return c, false, err
}

func (p *Pool) fresh(ctx context.Context) (*Conn, error) {
	c, err := p.dial(ctx, p.Endpoint)
	if p.OnDial != nil {
		p.OnDial(err == nil)
	}
	return c, err
}

// Close closes every idle connection.
func (p *Pool) Close() {
	p.mu.Lock()
	idle := p.idle
	p.idle = nil
	p.mu.Unlock()
	for _, c := range idle {
		c.Close()
	}
}
