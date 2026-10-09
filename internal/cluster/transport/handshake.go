package transport

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"time"
)

// hello is the first frame each side sends. Its encoding is frozen: cluster
// identifier and node name as length-prefixed strings, the supported framing
// version range, and a 32-byte nonce.
type hello struct {
	cluster string
	name    string
	vmin    uint8
	vmax    uint8
	nonce   [32]byte
}

func (h hello) encode() []byte {
	out := make([]byte, 0, 4+len(h.cluster)+len(h.name)+2+32)
	out = binary.BigEndian.AppendUint16(out, uint16(len(h.cluster)))
	out = append(out, h.cluster...)
	out = binary.BigEndian.AppendUint16(out, uint16(len(h.name)))
	out = append(out, h.name...)
	out = append(out, h.vmin, h.vmax)
	return append(out, h.nonce[:]...)
}

func decodeHello(b []byte) (hello, error) {
	var h hello
	read := func() (string, bool) {
		if len(b) < 2 {
			return "", false
		}
		n := int(binary.BigEndian.Uint16(b))
		b = b[2:]
		if len(b) < n {
			return "", false
		}
		s := string(b[:n])
		b = b[n:]
		return s, true
	}
	var ok bool
	if h.cluster, ok = read(); !ok {
		return h, fmt.Errorf("%w: hello cluster identifier", ErrProtocol)
	}
	if h.name, ok = read(); !ok {
		return h, fmt.Errorf("%w: hello node name", ErrProtocol)
	}
	if len(b) != 2+32 {
		return h, fmt.Errorf("%w: hello length", ErrProtocol)
	}
	h.vmin, h.vmax = b[0], b[1]
	copy(h.nonce[:], b[2:])
	if h.vmin == 0 || h.vmax < h.vmin || h.name == "" {
		return h, fmt.Errorf("%w: hello fields", ErrProtocol)
	}
	return h, nil
}

// mac binds a side's hello to both nonces under the shared secret. The role
// byte keeps the dialer's and the acceptor's proofs distinct.
func mac(secret []byte, role byte, mine hello, dialerNonce, acceptorNonce [32]byte) []byte {
	m := hmac.New(sha256.New, secret)
	m.Write([]byte{role})
	m.Write(dialerNonce[:])
	m.Write(acceptorNonce[:])
	m.Write(mine.encode())
	return m.Sum(nil)
}

// refusal is why a handshake failed; the reason is a closed vocabulary used
// in audit events and metrics.
type refusal struct {
	reason string
	err    error
}

func (r *refusal) Error() string { return r.reason + ": " + r.err.Error() }
func (r *refusal) Unwrap() error { return r.err }

func refuse(reason string, err error) error { return &refusal{reason: reason, err: err} }

// negotiated is what a completed handshake yields.
type negotiated struct {
	peer    string
	version uint8
}

// handshake runs the exchange on an already-connected transport (TLS
// completed when in use). dialer says which side this is. It returns the
// authenticated peer name and the framing version, or a refusal.
func (t *Transport) handshake(c net.Conn, dialer bool) (negotiated, error) {
	deadline := time.Now().Add(t.o.HandshakeTimeout)
	_ = c.SetDeadline(deadline)
	defer func() { _ = c.SetDeadline(time.Time{}) }()

	mine := hello{cluster: t.o.ClusterID, name: t.o.Node, vmin: t.vmin, vmax: t.vmax}
	if _, err := rand.Read(mine.nonce[:]); err != nil {
		return negotiated{}, refuse("internal", err)
	}
	w := newWriter(c)
	send := func(f frame) error {
		if err := writeFrame(w, f); err != nil {
			return err
		}
		return w.Flush()
	}
	recv := func(want Class) ([]byte, error) {
		f, err := readFrame(c, t.o.MaxFrame, true)
		if err != nil {
			return nil, err
		}
		if f.class != want {
			return nil, fmt.Errorf("%w: expected %s, got %s", ErrProtocol, want, f.class)
		}
		return f.payload, nil
	}
	classify := func(err error) error {
		var r *refusal
		if errors.As(err, &r) {
			return err
		}
		if errors.Is(err, ErrProtocol) {
			return refuse("protocol", err)
		}
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			return refuse("timeout", err)
		}
		return refuse("connection", err)
	}

	var theirs hello
	var dialerNonce, acceptorNonce [32]byte
	if dialer {
		if err := send(frame{class: classHello, payload: mine.encode()}); err != nil {
			return negotiated{}, classify(err)
		}
		body, err := recv(classHello)
		if err != nil {
			return negotiated{}, classify(err)
		}
		if theirs, err = t.checkHello(body, c, true); err != nil {
			return negotiated{}, err
		}
		dialerNonce, acceptorNonce = mine.nonce, theirs.nonce
		if t.o.Auth.Mode == "secret" {
			// The acceptor's hello carries its proof after the fixed fields.
			want := mac(t.o.Auth.Secret, 'A', theirs, dialerNonce, acceptorNonce)
			if !hmac.Equal(body[len(body)-32:], want) {
				return negotiated{}, refuse("mac", errors.New("acceptor proof does not verify"))
			}
		}
		var proof []byte
		if t.o.Auth.Mode == "secret" {
			proof = mac(t.o.Auth.Secret, 'D', mine, dialerNonce, acceptorNonce)
		}
		if err := send(frame{class: classAuth, payload: proof}); err != nil {
			return negotiated{}, classify(err)
		}
	} else {
		body, err := recv(classHello)
		if err != nil {
			return negotiated{}, classify(err)
		}
		if theirs, err = t.checkHello(body, c, false); err != nil {
			return negotiated{}, err
		}
		dialerNonce, acceptorNonce = theirs.nonce, mine.nonce
		reply := mine.encode()
		if t.o.Auth.Mode == "secret" {
			reply = append(reply, mac(t.o.Auth.Secret, 'A', mine, dialerNonce, acceptorNonce)...)
		}
		if err := send(frame{class: classHello, payload: reply}); err != nil {
			return negotiated{}, classify(err)
		}
		proof, err := recv(classAuth)
		if err != nil {
			return negotiated{}, classify(err)
		}
		if t.o.Auth.Mode == "secret" {
			want := mac(t.o.Auth.Secret, 'D', theirs, dialerNonce, acceptorNonce)
			if !hmac.Equal(proof, want) {
				return negotiated{}, refuse("mac", errors.New("dialer proof does not verify"))
			}
		} else if len(proof) != 0 {
			return negotiated{}, refuse("protocol", errors.New("unexpected auth payload in mtls mode"))
		}
	}
	return negotiated{peer: theirs.name, version: min(mine.vmax, theirs.vmax)}, nil
}

// checkHello decodes and verifies the peer's hello: cluster identifier,
// version overlap, that the peer is one we know and not ourselves, and in
// mtls mode that the certificate's common name is the claimed name. In
// secret mode the acceptor's hello (withProof) carries a trailing 32-byte
// proof, which is verified by the caller.
func (t *Transport) checkHello(body []byte, c net.Conn, withProof bool) (hello, error) {
	fixed := body
	if withProof && t.o.Auth.Mode == "secret" {
		if len(body) < 32 {
			return hello{}, refuse("mac", errors.New("acceptor sent no proof"))
		}
		fixed = body[:len(body)-32]
	}
	h, err := decodeHello(fixed)
	if err != nil {
		return h, refuse("protocol", err)
	}
	if h.cluster != t.o.ClusterID {
		return h, refuse("cluster-id", fmt.Errorf("peer %q is in cluster %q, this node is in %q", h.name, h.cluster, t.o.ClusterID))
	}
	if h.name == t.o.Node {
		return h, refuse("self", errors.New("a connection from this node's own name"))
	}
	if h.vmin > t.vmax || t.vmin > h.vmax {
		return h, refuse("version", fmt.Errorf("peer supports framing %d..%d, this node %d..%d", h.vmin, h.vmax, t.vmin, t.vmax))
	}
	if !t.knownPeer(h.name) {
		return h, refuse("unknown-peer", fmt.Errorf("%q is not a configured peer", h.name))
	}
	if t.o.Auth.Mode == "mtls" {
		tc, ok := c.(*tls.Conn)
		if !ok {
			return h, refuse("tls", errors.New("mtls mode on a plaintext connection"))
		}
		state := tc.ConnectionState()
		if len(state.PeerCertificates) == 0 {
			return h, refuse("certificate", errors.New("no peer certificate"))
		}
		if cn := state.PeerCertificates[0].Subject.CommonName; cn != h.name {
			return h, refuse("certificate", fmt.Errorf("certificate is for %q, hello claims %q", cn, h.name))
		}
	}
	return h, nil
}
