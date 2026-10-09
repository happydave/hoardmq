package auth

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
)

// SCRAMMethod is the enhanced-authentication method of RFC 7677.
const SCRAMMethod = "SCRAM-SHA-256"

// SCRAMServer is the server side of SCRAM-SHA-256 (RFC 5802, RFC 7677)
// without channel binding. Lookup finds a user's credential; a user it does
// not find, or one stored in another form, is answered with a salt derived
// from the username and refused at the final step, so the exchange does not
// tell a missing user from a wrong password.
type SCRAMServer struct {
	Lookup func(user string) (SCRAMCredential, bool)
	// Nonce returns the server's nonce; tests fix it.
	Nonce func() string
	// fakeKey keys the salts of unknown users for this process.
	fakeKey []byte
}

// NewSCRAMServer returns a server over lookup with random server nonces.
func NewSCRAMServer(lookup func(string) (SCRAMCredential, bool)) *SCRAMServer {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		panic("crypto/rand: " + err.Error())
	}
	return &SCRAMServer{Lookup: lookup, Nonce: randomNonce, fakeKey: key}
}

func randomNonce() string {
	b := make([]byte, 18)
	if _, err := rand.Read(b); err != nil {
		panic("crypto/rand: " + err.Error())
	}
	return base64.StdEncoding.EncodeToString(b)
}

// Methods implements Mechanism.
func (s *SCRAMServer) Methods() []string { return []string{SCRAMMethod} }

// Begin implements Mechanism.
func (s *SCRAMServer) Begin(_ context.Context, _ string, _ *Request) Exchange {
	return &scramExchange{srv: s}
}

type scramExchange struct {
	srv         *SCRAMServer
	state       int
	first       clientFirst
	serverFirst string
	cred        SCRAMCredential
	known       bool
}

// Step implements Exchange: the client-first message, then the client-final.
func (e *scramExchange) Step(data []byte) Step {
	switch e.state {
	case 0:
		e.state = 1
		first, err := parseClientFirst(string(data))
		if err != nil {
			return Step{Kind: Fail, Reason: "scram: " + err.Error()}
		}
		e.first = first
		e.cred, e.known = e.srv.Lookup(first.user)
		if !e.known {
			e.cred = e.srv.stand(first.user)
		}
		e.serverFirst = serverFirstMessage(first.nonce+e.srv.Nonce(), e.cred)
		return Step{Kind: Continue, Data: []byte(e.serverFirst)}
	case 1:
		e.state = 2
		final, reason := verifyClientFinal(e.first, e.serverFirst, e.cred, e.known, string(data))
		if reason != "" {
			return Step{Kind: Fail, Checked: true, Reason: "scram: " + reason}
		}
		return Step{Kind: Succeed, Data: []byte(final), Username: e.first.user}
	}
	return Step{Kind: Fail, Reason: "scram: the exchange is over"}
}

// stand is the credential an unknown user is answered with: a salt keyed to
// the username, so two exchanges for one name see one salt.
func (s *SCRAMServer) stand(user string) SCRAMCredential {
	m := hmac.New(sha256.New, s.fakeKey)
	m.Write([]byte("salt:" + user))
	return SCRAMCredential{Salt: m.Sum(nil)[:16], Iterations: MinSCRAMIterations, StoredKey: make([]byte, sha256.Size), ServerKey: make([]byte, sha256.Size)}
}

// clientFirst is a parsed client-first message.
type clientFirst struct {
	header string // the GS2 header, "n,," or "y,,"
	bare   string // client-first-message-bare
	user   string
	nonce  string
}

// parseClientFirst reads RFC 5802 § 7's client-first-message. Channel
// binding ("p=") and an authorization identity are refused; "y" (the client
// could bind, and believes the server cannot) is accepted, since no binding is
// offered.
func parseClientFirst(msg string) (clientFirst, error) {
	var header string
	switch {
	case strings.HasPrefix(msg, "n,"), strings.HasPrefix(msg, "y,"):
		header = msg[:2]
	case strings.HasPrefix(msg, "p="):
		return clientFirst{}, errors.New("channel binding is not supported")
	default:
		return clientFirst{}, errors.New("malformed GS2 header")
	}
	rest := msg[2:]
	if !strings.HasPrefix(rest, ",") {
		return clientFirst{}, errors.New("an authorization identity is not supported")
	}
	header += ","
	bare := rest[1:]
	fields := strings.Split(bare, ",")
	if len(fields) < 2 || !strings.HasPrefix(fields[0], "n=") || !strings.HasPrefix(fields[1], "r=") {
		return clientFirst{}, errors.New("malformed client-first message")
	}
	user, ok := saslName(fields[0][2:])
	if !ok || user == "" {
		return clientFirst{}, errors.New("malformed username")
	}
	nonce := fields[1][2:]
	if !printable(nonce) {
		return clientFirst{}, errors.New("malformed nonce")
	}
	return clientFirst{header: header, bare: bare, user: user, nonce: nonce}, nil
}

// saslName decodes RFC 5802's "=2C" and "=3D"; any other "=" is malformed.
func saslName(s string) (string, bool) {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] != '=' {
			b.WriteByte(s[i])
			continue
		}
		switch {
		case strings.HasPrefix(s[i:], "=2C"):
			b.WriteByte(',')
		case strings.HasPrefix(s[i:], "=3D"):
			b.WriteByte('=')
		default:
			return "", false
		}
		i += 2
	}
	return b.String(), true
}

// printable is RFC 5802's nonce alphabet: printable ASCII except ",".
func printable(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < 0x21 || s[i] > 0x7e || s[i] == ',' {
			return false
		}
	}
	return true
}

// serverFirstMessage is RFC 5802 § 7's server-first-message.
func serverFirstMessage(nonce string, c SCRAMCredential) string {
	return "r=" + nonce + ",s=" + base64.StdEncoding.EncodeToString(c.Salt) + ",i=" + strconv.Itoa(c.Iterations)
}

// verifyClientFinal is the decision on the client-final message: the channel
// binding is the base64 of the GS2 header, the nonce is the combined one, and
// the proof yields a ClientKey whose hash is the StoredKey. It returns the
// server-final message, or the reason for refusing. An unknown user is refused
// here, after the same work.
func verifyClientFinal(first clientFirst, serverFirst string, c SCRAMCredential, known bool, final string) (string, string) {
	i := strings.LastIndex(final, ",p=")
	if i < 0 {
		return "", "malformed client-final message"
	}
	withoutProof, proofB64 := final[:i], final[i+3:]
	fields := strings.Split(withoutProof, ",")
	if len(fields) < 2 || !strings.HasPrefix(fields[0], "c=") || !strings.HasPrefix(fields[1], "r=") {
		return "", "malformed client-final message"
	}
	if fields[0][2:] != base64.StdEncoding.EncodeToString([]byte(first.header)) {
		return "", "channel binding does not match the GS2 header"
	}
	combined := strings.TrimPrefix(serverFirst, "r=")
	combined = combined[:strings.Index(combined, ",")]
	if fields[1][2:] != combined {
		return "", "nonce mismatch"
	}
	proof, err := base64.StdEncoding.DecodeString(proofB64)
	if err != nil || len(proof) != sha256.Size {
		return "", "malformed proof"
	}
	authMessage := []byte(first.bare + "," + serverFirst + "," + withoutProof)
	signature := hmacSHA256(c.StoredKey, authMessage)
	clientKey := make([]byte, sha256.Size)
	for j := range clientKey {
		clientKey[j] = proof[j] ^ signature[j]
	}
	stored := sha256.Sum256(clientKey)
	if subtle.ConstantTimeCompare(stored[:], c.StoredKey) != 1 || !known {
		if !known {
			return "", "unknown user"
		}
		return "", "invalid proof"
	}
	return "v=" + base64.StdEncoding.EncodeToString(hmacSHA256(c.ServerKey, authMessage)), ""
}
