package conn

import (
	"context"
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/sha256"
	"encoding/base64"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/packet"
)

// scriptMech answers each step from its script and records what it was sent.
type scriptMech struct {
	method string
	steps  []auth.Step
	mu     sync.Mutex
	begun  int
	got    []string
}

func (m *scriptMech) Methods() []string { return []string{m.method} }
func (m *scriptMech) Begin(context.Context, string, *auth.Request) auth.Exchange {
	m.mu.Lock()
	m.begun++
	m.mu.Unlock()
	return &scriptExchange{m: m}
}

type scriptExchange struct {
	m *scriptMech
	i int
}

func (e *scriptExchange) Step(data []byte) auth.Step {
	e.m.mu.Lock()
	defer e.m.mu.Unlock()
	e.m.got = append(e.m.got, string(data))
	s := e.m.steps[e.i]
	e.i++
	return s
}

func (m *scriptMech) begins() int { m.mu.Lock(); defer m.mu.Unlock(); return m.begun }

func offering(mechs ...auth.Mechanism) func(string) (auth.Mechanism, bool) {
	return func(method string) (auth.Mechanism, bool) {
		for _, m := range mechs {
			for _, name := range m.Methods() {
				if name == method {
					return m, true
				}
			}
		}
		return nil, false
	}
}

func str(s string) *string { return &s }

func twoSteps(final auth.Step) *scriptMech {
	return &scriptMech{method: "X-TEST", steps: []auth.Step{{Kind: auth.Continue, Data: []byte("s1")}, final}}
}

func methodConnect(method, data string) *packet.Connect {
	return &packet.Connect{ClientID: "cid", CleanStart: true, Properties: packet.Properties{AuthenticationMethod: str(method), AuthenticationData: []byte(data)}}
}

// begin sends a CONNECT naming a method and reads the server's first AUTH.
func (h *harness) begin(c *packet.Connect) *packet.Auth {
	h.t.Helper()
	c.ProtocolLevel = packet.V5
	h.enc = &packet.Encoder{Version: packet.V5}
	h.dec.Version = packet.V5
	h.send(c)
	p := h.read()
	a, ok := p.(*packet.Auth)
	if !ok {
		h.t.Fatalf("expected AUTH, got %T %+v", p, p)
	}
	return a
}

func (h *harness) readConnack() *packet.Connack {
	h.t.Helper()
	p := h.read()
	a, ok := p.(*packet.Connack)
	if !ok {
		h.t.Fatalf("expected CONNACK, got %T %+v", p, p)
	}
	return a
}

// expectEOF reads until the server closes, failing on any packet.
func (h *harness) expectEOF() {
	h.t.Helper()
	_ = h.client.SetReadDeadline(time.Now().Add(2 * time.Second))
	if p, err := h.dec.Read(); err == nil {
		h.t.Fatalf("expected the connection closed, got %T %+v", p, p)
	}
}

func methodOf(p packet.Properties) string {
	if p.AuthenticationMethod == nil {
		return ""
	}
	return *p.AuthenticationMethod
}

// Each server step is an AUTH 0x18 carrying the method; the CONNACK carries
// it and the final data.
func TestEnhancedAuthExchange(t *testing.T) {
	m := twoSteps(auth.Step{Kind: auth.Succeed, Data: []byte("s2"), Username: "alice"})
	o := defaultOptions()
	o.Mechanisms = offering(m)
	h := newHarness(t, o, &fakeCore{})
	a := h.begin(methodConnect("X-TEST", "c1"))
	if a.ReasonCode != packet.ContinueAuthentication || methodOf(a.Properties) != "X-TEST" || string(a.Properties.AuthenticationData) != "s1" {
		t.Fatalf("server AUTH %+v", a)
	}
	h.send(&packet.Auth{ReasonCode: packet.ContinueAuthentication, Properties: packet.Properties{AuthenticationMethod: str("X-TEST"), AuthenticationData: []byte("c2")}})
	ack := h.readConnack()
	if ack.ReasonCode != packet.Success || methodOf(ack.Properties) != "X-TEST" || string(ack.Properties.AuthenticationData) != "s2" {
		t.Fatalf("CONNACK %+v", ack)
	}
	if strings.Join(m.got, ",") != "c1,c2" {
		t.Fatalf("the mechanism was sent %v", m.got)
	}
	if h.conn.Identity().Username != "alice" {
		t.Fatalf("identity %q", h.conn.Identity().Username)
	}
}

func TestEnhancedAuthRefused(t *testing.T) {
	for _, c := range []struct {
		checked bool
		want    packet.ReasonCode
	}{{true, packet.BadUserNameOrPassword}, {false, packet.NotAuthorized}} {
		m := twoSteps(auth.Step{Kind: auth.Fail, Checked: c.checked, Reason: "x: no"})
		o := defaultOptions()
		o.Mechanisms = offering(m)
		h := newHarness(t, o, &fakeCore{})
		h.begin(methodConnect("X-TEST", "c1"))
		h.send(&packet.Auth{ReasonCode: packet.ContinueAuthentication, Properties: packet.Properties{AuthenticationMethod: str("X-TEST")}})
		if ack := h.readConnack(); ack.ReasonCode != c.want || methodOf(ack.Properties) != "X-TEST" {
			t.Fatalf("checked %v: %+v", c.checked, ack)
		}
		h.expectEOF()
	}
}

// Anything but the method's AUTH 0x18 during the exchange is a Protocol
// Error before the CONNACK; a DISCONNECT ends it without one.
func TestEnhancedAuthOutOfOrder(t *testing.T) {
	for name, p := range map[string]packet.Packet{
		"a PUBLISH":         &packet.Publish{Topic: "t", Payload: []byte("x")},
		"another method":    &packet.Auth{ReasonCode: packet.ContinueAuthentication, Properties: packet.Properties{AuthenticationMethod: str("Y")}},
		"re-authentication": &packet.Auth{ReasonCode: packet.ReAuthenticate, Properties: packet.Properties{AuthenticationMethod: str("X-TEST")}},
		"no method":         &packet.Auth{ReasonCode: packet.ContinueAuthentication},
	} {
		m := twoSteps(auth.Step{Kind: auth.Succeed})
		o := defaultOptions()
		o.Mechanisms = offering(m)
		h := newHarness(t, o, &fakeCore{})
		h.begin(methodConnect("X-TEST", "c1"))
		h.send(p)
		if ack := h.readConnack(); ack.ReasonCode != packet.ProtocolError {
			t.Errorf("%s: %+v", name, ack)
		}
		h.expectEOF()
	}
	m := twoSteps(auth.Step{Kind: auth.Succeed})
	o := defaultOptions()
	o.Mechanisms = offering(m)
	h := newHarness(t, o, &fakeCore{})
	h.begin(methodConnect("X-TEST", "c1"))
	h.send(&packet.Disconnect{})
	h.expectEOF()
}

// A client that goes quiet mid-exchange is closed at the connect timeout,
// without a CONNACK.
func TestEnhancedAuthSilence(t *testing.T) {
	m := twoSteps(auth.Step{Kind: auth.Succeed})
	o := defaultOptions()
	o.Mechanisms = offering(m)
	o.ConnectTimeout = 200 * time.Millisecond
	h := newHarness(t, o, &fakeCore{})
	h.begin(methodConnect("X-TEST", "c1"))
	start := time.Now()
	h.expectEOF()
	if el := time.Since(start); el > 700*time.Millisecond {
		t.Fatalf("closed after %v", el)
	}
}

// A password beside a method is refused before the mechanism begins; a
// username must be the authenticated user's.
func TestEnhancedAuthCredentialsBeside(t *testing.T) {
	m := twoSteps(auth.Step{Kind: auth.Succeed, Username: "alice"})
	o := defaultOptions()
	o.Mechanisms = offering(m)
	h := newHarness(t, o, &fakeCore{})
	c := methodConnect("X-TEST", "c1")
	c.HasUsername, c.Username, c.HasPassword, c.Password = true, "alice", true, []byte("pw")
	if ack := h.connect(packet.V5, c); ack.ReasonCode != packet.BadUserNameOrPassword || m.begins() != 0 {
		t.Fatalf("a password beside a method: %+v, begun %d", ack, m.begins())
	}
	for user, want := range map[string]packet.ReasonCode{"bob": packet.BadUserNameOrPassword, "alice": packet.Success} {
		m := twoSteps(auth.Step{Kind: auth.Succeed, Username: "alice"})
		o.Mechanisms = offering(m)
		h := newHarness(t, o, &fakeCore{})
		c := methodConnect("X-TEST", "c1")
		c.HasUsername, c.Username = true, user
		h.begin(c)
		h.send(&packet.Auth{ReasonCode: packet.ContinueAuthentication, Properties: packet.Properties{AuthenticationMethod: str("X-TEST")}})
		if ack := h.readConnack(); ack.ReasonCode != want {
			t.Errorf("username %s: %+v", user, ack)
		}
	}
}

// countingAuth answers with next and counts its calls.
type countingAuth struct {
	calls atomic.Int32
	next  func(n int) auth.Result
}

func (a *countingAuth) Authenticate(context.Context, *auth.Request) auth.Result {
	return a.next(int(a.calls.Add(1)))
}

func always(d auth.Decision) *countingAuth {
	return &countingAuth{next: func(int) auth.Result { return auth.Result{Decision: d} }}
}

// A certificate authenticator's deny refuses before the chain and before a
// mechanism, with 0x87 whatever else the client sent.
func TestCertificateAuthenticatorsRunFirst(t *testing.T) {
	certs, chain := always(auth.Deny), always(auth.Allow)
	o := defaultOptions()
	o.CertificateAuthenticators = auth.AuthenticatorChain{certs}
	o.Authenticators = auth.AuthenticatorChain{chain}
	h := newHarness(t, o, &fakeCore{})
	if ack := h.connect(packet.V5, nil); ack.ReasonCode != packet.NotAuthorized || chain.calls.Load() != 0 {
		t.Fatalf("no credentials: %+v, chain called %d", ack, chain.calls.Load())
	}
	h = newHarness(t, o, &fakeCore{})
	if ack := h.connect(packet.V5, &packet.Connect{ClientID: "c", CleanStart: true, HasUsername: true, Username: "u", HasPassword: true, Password: []byte("p")}); ack.ReasonCode != packet.NotAuthorized || chain.calls.Load() != 0 {
		t.Fatalf("with a password: %+v, chain called %d", ack, chain.calls.Load())
	}
	m := twoSteps(auth.Step{Kind: auth.Succeed})
	o.Mechanisms = offering(m)
	h = newHarness(t, o, &fakeCore{})
	if ack := h.connect(packet.V5, methodConnect("X-TEST", "c1")); ack.ReasonCode != packet.NotAuthorized || m.begins() != 0 {
		t.Fatalf("a method: %+v, begun %d", ack, m.begins())
	}
	// A certificate authenticator's allow admits when the chain passes.
	o = defaultOptions()
	o.Anonymous = false
	o.CertificateAuthenticators = auth.AuthenticatorChain{always(auth.Allow)}
	o.Authenticators = auth.AuthenticatorChain{always(auth.Pass)}
	h = newHarness(t, o, &fakeCore{})
	if ack := h.connect(packet.V5, nil); ack.ReasonCode != packet.Success {
		t.Fatalf("a certificate authenticator's allow: %+v", ack)
	}
}

func TestAdmission(t *testing.T) {
	A, D, P := auth.Result{Decision: auth.Allow}, auth.Result{Decision: auth.Deny}, auth.Result{Decision: auth.Pass}
	bare, pw, cert := &auth.Request{}, &auth.Request{HasUsername: true, HasPassword: true}, &auth.Request{CertCN: "d"}
	for _, c := range []struct {
		name        string
		certs, cham auth.Result
		req         *auth.Request
		anonymous   bool
		want        bool
		by          auth.Decision
	}{
		{"certificate deny beats a chain allow", D, A, pw, false, false, auth.Deny},
		{"chain deny beats a certificate allow", A, D, pw, false, false, auth.Deny},
		{"chain allow", P, A, pw, false, true, auth.Allow},
		{"certificate allow, chain pass", A, P, cert, false, true, auth.Allow},
		{"both pass, a verified certificate alone", P, P, cert, false, true, auth.Pass},
		{"both pass, anonymous", P, P, bare, true, true, auth.Pass},
		{"both pass, not anonymous", P, P, bare, false, false, auth.Pass},
		{"both pass, a password nobody examined", P, P, pw, true, false, auth.Pass},
	} {
		got, ok := admission(c.certs, c.cham, c.req, c.anonymous)
		if ok != c.want || got.Decision != c.by {
			t.Errorf("%s: %v %v", c.name, got.Decision, ok)
		}
	}
}

func TestRecheckAt(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	for _, c := range []struct {
		expires time.Time
		ttl     time.Duration
		want    time.Time
	}{
		{time.Time{}, time.Hour, now.Add(time.Hour)},
		{now.Add(10 * time.Minute), time.Hour, now.Add(10 * time.Minute)},
		{now.Add(2 * time.Hour), time.Hour, now.Add(time.Hour)},
		{now.Add(2 * time.Hour), 0, now.Add(2 * time.Hour)},
		{time.Time{}, 0, time.Time{}},
	} {
		if got := recheckAt(now, c.expires, c.ttl); !got.Equal(c.want) {
			t.Errorf("(%v, %v): %v, want %v", c.expires, c.ttl, got, c.want)
		}
	}
}

// waitCalls waits until the authenticator has been called n times.
func waitCalls(t *testing.T, a *countingAuth, n int32, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for a.calls.Load() < n {
		if time.Now().After(deadline) {
			t.Fatalf("%d calls, want %d", a.calls.Load(), n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A held allow is made again at its expiry: refused, the session ends with
// 0x87 (closed at level 4); allowed again, it is held on.
func TestRedecision(t *testing.T) {
	soon := func() time.Time { return time.Now().Add(150 * time.Millisecond) }
	creds := &packet.Connect{ClientID: "c", CleanStart: true, HasUsername: true, Username: "u", HasPassword: true, Password: []byte("p")}
	refusedSecond := &countingAuth{next: func(n int) auth.Result {
		if n == 1 {
			return auth.Result{Decision: auth.Allow, Expires: soon()}
		}
		return auth.Result{Decision: auth.Deny, Reason: "jwt: token expired"}
	}}
	o := defaultOptions()
	o.Authenticators = auth.AuthenticatorChain{refusedSecond}
	h := newHarness(t, o, &fakeCore{})
	if ack := h.connect(packet.V5, creds); ack.ReasonCode != packet.Success {
		t.Fatal(ack)
	}
	start := time.Now()
	if d := h.readEnd(); d == nil || d.ReasonCode != packet.NotAuthorized {
		t.Fatalf("a refused re-decision: %+v", d)
	}
	if el := time.Since(start); el > 600*time.Millisecond || refusedSecond.calls.Load() != 2 {
		t.Fatalf("after %v, %d calls", el, refusedSecond.calls.Load())
	}
	// Level 4: closed without a DISCONNECT.
	refusedSecond.calls.Store(0)
	h = newHarness(t, o, &fakeCore{})
	c4 := *creds
	if ack := h.connect(packet.V4, &c4); ack.ReasonCode != packet.Success {
		t.Fatal(ack)
	}
	if d := h.readEnd(); d != nil {
		t.Fatalf("level 4 sent %+v", d)
	}
	// Allowed again: held on.
	held := &countingAuth{next: func(int) auth.Result { return auth.Result{Decision: auth.Allow, Expires: soon()} }}
	o.Authenticators = auth.AuthenticatorChain{held}
	h = newHarness(t, o, &fakeCore{})
	c5 := *creds
	h.connect(packet.V5, &c5)
	waitCalls(t, held, 3, 2*time.Second)
	h.send(&packet.Pingreq{})
	if _, ok := h.read().(*packet.Pingresp); !ok {
		t.Fatal("the held session was ended")
	}
	// The TTL bounds a decision with no expiry of its own.
	ttl := &countingAuth{next: func(n int) auth.Result {
		if n == 1 {
			return auth.Result{Decision: auth.Allow}
		}
		return auth.Result{Decision: auth.Deny}
	}}
	o.Authenticators = auth.AuthenticatorChain{ttl}
	o.DecisionTTL = 150 * time.Millisecond
	h = newHarness(t, o, &fakeCore{})
	c6 := *creds
	h.connect(packet.V5, &c6)
	if d := h.readEnd(); d == nil || d.ReasonCode != packet.NotAuthorized {
		t.Fatalf("the TTL: %+v", d)
	}
}

// A pass-admitted (anonymous) connection holds no decision; a session that
// ended is not re-decided.
func TestNoRedecision(t *testing.T) {
	pass, certPass := always(auth.Pass), always(auth.Pass)
	o := defaultOptions()
	o.Authenticators = auth.AuthenticatorChain{pass}
	o.CertificateAuthenticators = auth.AuthenticatorChain{certPass}
	o.DecisionTTL = 100 * time.Millisecond
	h := newHarness(t, o, &fakeCore{})
	h.connect(packet.V5, nil)
	if pass.calls.Load() != 1 || certPass.calls.Load() != 1 {
		t.Fatalf("the handshake made %d and %d calls", pass.calls.Load(), certPass.calls.Load())
	}
	time.Sleep(400 * time.Millisecond)
	if n, m := pass.calls.Load(), certPass.calls.Load(); n != 1 || m != 1 {
		t.Fatalf("an anonymous session was re-decided: %d and %d calls", n, m)
	}
	o.CertificateAuthenticators = nil
	allow := always(auth.Allow)
	o.Authenticators = auth.AuthenticatorChain{allow}
	h = newHarness(t, o, &fakeCore{})
	h.connect(packet.V5, &packet.Connect{ClientID: "c", CleanStart: true, HasUsername: true, Username: "u", HasPassword: true, Password: []byte("p")})
	h.send(&packet.Disconnect{})
	h.waitDisconnect()
	n := allow.calls.Load()
	time.Sleep(400 * time.Millisecond)
	if allow.calls.Load() != n {
		t.Fatalf("an ended session was re-decided: %d → %d calls", n, allow.calls.Load())
	}
}

// scramClient is the client side of SCRAM-SHA-256, written from RFC 5802
// here rather than taken from the server's code.
type scramClient struct {
	user, password, nonce string
	first                 string
}

func (s *scramClient) firstMessage() []byte {
	s.first = "n=" + s.user + ",r=" + s.nonce
	return []byte("n,," + s.first)
}

func (s *scramClient) finalMessage(serverFirst string) []byte {
	var salt []byte
	var iter int
	var nonce string
	for _, f := range strings.Split(serverFirst, ",") {
		switch {
		case strings.HasPrefix(f, "r="):
			nonce = f[2:]
		case strings.HasPrefix(f, "s="):
			salt, _ = base64.StdEncoding.DecodeString(f[2:])
		case strings.HasPrefix(f, "i="):
			iter, _ = strconv.Atoi(f[2:])
		}
	}
	salted, _ := pbkdf2.Key(sha256.New, s.password, salt, iter, 32)
	mac := func(k []byte, m string) []byte { h := hmac.New(sha256.New, k); h.Write([]byte(m)); return h.Sum(nil) }
	clientKey := mac(salted, "Client Key")
	stored := sha256.Sum256(clientKey)
	withoutProof := "c=biws,r=" + nonce
	sig := mac(stored[:], s.first+","+serverFirst+","+withoutProof)
	for i := range clientKey {
		clientKey[i] ^= sig[i]
	}
	return []byte(withoutProof + ",p=" + base64.StdEncoding.EncodeToString(clientKey))
}

func scramOptions(t *testing.T) Options {
	t.Helper()
	users := map[string]auth.SCRAMCredential{}
	for _, u := range []string{"user", "other"} {
		c, err := auth.DeriveSCRAM([]byte("pencil"), []byte("salt-"+u), 4096)
		if err != nil {
			t.Fatal(err)
		}
		users[u] = c
	}
	o := defaultOptions()
	o.Mechanisms = offering(auth.NewSCRAMServer(func(u string) (auth.SCRAMCredential, bool) { c, ok := users[u]; return c, ok }))
	return o
}

func scramAuth(h *harness, reason packet.ReasonCode, c *scramClient) {
	h.t.Helper()
	h.send(&packet.Auth{ReasonCode: reason, Properties: packet.Properties{AuthenticationMethod: str(auth.SCRAMMethod), AuthenticationData: c.firstMessage()}})
}

// SCRAM-SHA-256 end to end, then re-authentication with other traffic in
// between.
func TestSCRAMOverTheConnection(t *testing.T) {
	h := newHarness(t, scramOptions(t), &fakeCore{})
	c := &scramClient{user: "user", password: "pencil", nonce: "fyko+d2lbbFgONRv9qkxdawL"}
	a := h.begin(&packet.Connect{ClientID: "cid", CleanStart: true, Properties: packet.Properties{AuthenticationMethod: str(auth.SCRAMMethod), AuthenticationData: c.firstMessage()}})
	h.send(&packet.Auth{ReasonCode: packet.ContinueAuthentication, Properties: packet.Properties{AuthenticationMethod: str(auth.SCRAMMethod), AuthenticationData: c.finalMessage(string(a.Properties.AuthenticationData))}})
	ack := h.readConnack()
	if ack.ReasonCode != packet.Success || methodOf(ack.Properties) != auth.SCRAMMethod || !strings.HasPrefix(string(ack.Properties.AuthenticationData), "v=") {
		t.Fatalf("CONNACK %+v", ack)
	}
	// Re-authentication, a QoS 1 PUBLISH answered in between.
	scramAuth(h, packet.ReAuthenticate, c)
	a, ok := h.read().(*packet.Auth)
	if !ok || a.ReasonCode != packet.ContinueAuthentication || methodOf(a.Properties) != auth.SCRAMMethod {
		t.Fatalf("re-authentication's first AUTH %+v", a)
	}
	h.send(&packet.Publish{Topic: "t", QoS: 1, PacketID: 7, Payload: []byte("x")})
	if pa, ok := h.read().(*packet.Puback); !ok || pa.PacketID != 7 {
		t.Fatalf("the PUBLISH during re-authentication: %+v", pa)
	}
	h.send(&packet.Auth{ReasonCode: packet.ContinueAuthentication, Properties: packet.Properties{AuthenticationMethod: str(auth.SCRAMMethod), AuthenticationData: c.finalMessage(string(a.Properties.AuthenticationData))}})
	done, ok := h.read().(*packet.Auth)
	if !ok || done.ReasonCode != packet.Success || methodOf(done.Properties) != auth.SCRAMMethod || !strings.HasPrefix(string(done.Properties.AuthenticationData), "v=") {
		t.Fatalf("re-authentication's success %+v", done)
	}
}

func TestSCRAMRefusedOverTheConnection(t *testing.T) {
	h := newHarness(t, scramOptions(t), &fakeCore{})
	c := &scramClient{user: "user", password: "wrong", nonce: "abc"}
	a := h.begin(&packet.Connect{ClientID: "cid", CleanStart: true, Properties: packet.Properties{AuthenticationMethod: str(auth.SCRAMMethod), AuthenticationData: c.firstMessage()}})
	h.send(&packet.Auth{ReasonCode: packet.ContinueAuthentication, Properties: packet.Properties{AuthenticationMethod: str(auth.SCRAMMethod), AuthenticationData: c.finalMessage(string(a.Properties.AuthenticationData))}})
	if ack := h.readConnack(); ack.ReasonCode != packet.BadUserNameOrPassword {
		t.Fatalf("a wrong password: %+v", ack)
	}
}

// A failed re-authentication, or one as another user, ends the session with
// 0x87; a re-authentication with another method is a Protocol Error.
func TestReauthenticationRefused(t *testing.T) {
	connected := func() *harness {
		h := newHarness(t, scramOptions(t), &fakeCore{})
		c := &scramClient{user: "user", password: "pencil", nonce: "n1"}
		a := h.begin(&packet.Connect{ClientID: "cid", CleanStart: true, Properties: packet.Properties{AuthenticationMethod: str(auth.SCRAMMethod), AuthenticationData: c.firstMessage()}})
		h.send(&packet.Auth{ReasonCode: packet.ContinueAuthentication, Properties: packet.Properties{AuthenticationMethod: str(auth.SCRAMMethod), AuthenticationData: c.finalMessage(string(a.Properties.AuthenticationData))}})
		if ack := h.readConnack(); ack.ReasonCode != packet.Success {
			t.Fatal(ack)
		}
		return h
	}
	for name, c := range map[string]*scramClient{
		"a wrong password": {user: "user", password: "wrong", nonce: "n2"},
		"another user":     {user: "other", password: "pencil", nonce: "n3"},
	} {
		h := connected()
		scramAuth(h, packet.ReAuthenticate, c)
		a := h.read().(*packet.Auth)
		h.send(&packet.Auth{ReasonCode: packet.ContinueAuthentication, Properties: packet.Properties{AuthenticationMethod: str(auth.SCRAMMethod), AuthenticationData: c.finalMessage(string(a.Properties.AuthenticationData))}})
		if d := h.readEnd(); d == nil || d.ReasonCode != packet.NotAuthorized {
			t.Errorf("%s: %+v", name, d)
		}
	}
	h := connected()
	h.send(&packet.Auth{ReasonCode: packet.ReAuthenticate, Properties: packet.Properties{AuthenticationMethod: str("X-OTHER")}})
	if d := h.readEnd(); d == nil || d.ReasonCode != packet.ProtocolError {
		t.Errorf("another method: %+v", d)
	}
	h = connected()
	h.send(&packet.Auth{ReasonCode: packet.ContinueAuthentication, Properties: packet.Properties{AuthenticationMethod: str(auth.SCRAMMethod)}})
	if d := h.readEnd(); d == nil || d.ReasonCode != packet.ProtocolError {
		t.Errorf("0x18 outside an exchange: %+v", d)
	}
}

// recordingAudit keeps the audit records.
type recordingAudit struct {
	mu      sync.Mutex
	details []map[string]string
}

func (r *recordingAudit) Audit(_, action, _, _ string, detail map[string]string) {
	r.mu.Lock()
	r.details = append(r.details, detail)
	r.mu.Unlock()
}

// A refusal's reason reaches the audit record, and the credential does not.
func TestRefusalReasonAudited(t *testing.T) {
	rec := &recordingAudit{}
	o := defaultOptions()
	o.Audit = rec
	o.Authenticators = auth.AuthenticatorChain{&countingAuth{next: func(int) auth.Result {
		return auth.Result{Decision: auth.Deny, Reason: "jwt: token expired"}
	}}}
	h := newHarness(t, o, &fakeCore{})
	secret := "eyJhbGciOiJSUzI1NiJ9.e30.c2VjcmV0LXRva2Vu"
	if ack := h.connect(packet.V5, &packet.Connect{ClientID: "c", CleanStart: true, HasUsername: true, Username: "u", HasPassword: true, Password: []byte(secret)}); ack.ReasonCode != packet.BadUserNameOrPassword {
		t.Fatal(ack)
	}
	<-h.done
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.details) != 1 || rec.details[0]["reason"] != "jwt: token expired" {
		t.Fatalf("audit %v", rec.details)
	}
	for _, v := range rec.details[0] {
		if strings.Contains(v, secret) {
			t.Fatal("the audit record carries the token")
		}
	}
}

// The connect timeout bounds each read of the exchange, not the whole of it:
// three steps of 150 ms each outlast a 200 ms timeout and succeed.
func TestEnhancedAuthDeadlinePerRead(t *testing.T) {
	m := &scriptMech{method: "X-TEST", steps: []auth.Step{{Kind: auth.Continue}, {Kind: auth.Continue}, {Kind: auth.Continue}, {Kind: auth.Succeed}}}
	o := defaultOptions()
	o.Mechanisms = offering(m)
	o.ConnectTimeout = 200 * time.Millisecond
	h := newHarness(t, o, &fakeCore{})
	h.begin(methodConnect("X-TEST", "c1"))
	for i := range 3 {
		time.Sleep(150 * time.Millisecond)
		h.send(&packet.Auth{ReasonCode: packet.ContinueAuthentication, Properties: packet.Properties{AuthenticationMethod: str("X-TEST")}})
		p := h.read()
		if i < 2 {
			if _, ok := p.(*packet.Auth); !ok {
				t.Fatalf("step %d: %T", i, p)
			}
		} else if ack, ok := p.(*packet.Connack); !ok || ack.ReasonCode != packet.Success {
			t.Fatalf("the last step: %+v", p)
		}
	}
}
