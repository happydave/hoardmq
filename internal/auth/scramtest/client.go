// Package scramtest is the client side of SCRAM-SHA-256 (RFC 5802, RFC
// 7677) for tests that drive the broker's mechanism over the wire. It is not
// used by the broker.
package scramtest

import (
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/sha256"
	"encoding/base64"
	"strconv"
	"strings"
)

// Client is one exchange's client state.
type Client struct {
	User, Password, Nonce string
	bare                  string
	serverKey             []byte
	authMessage           string
}

// First is the client-first message.
func (c *Client) First() []byte {
	c.bare = "n=" + strings.NewReplacer("=", "=3D", ",", "=2C").Replace(c.User) + ",r=" + c.Nonce
	return []byte("n,," + c.bare)
}

func mac(key []byte, msg string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(msg))
	return h.Sum(nil)
}

// Final is the client-final message answering the server-first one.
func (c *Client) Final(serverFirst []byte) []byte {
	var salt []byte
	var iterations int
	var nonce string
	for _, f := range strings.Split(string(serverFirst), ",") {
		switch {
		case strings.HasPrefix(f, "r="):
			nonce = f[2:]
		case strings.HasPrefix(f, "s="):
			salt, _ = base64.StdEncoding.DecodeString(f[2:])
		case strings.HasPrefix(f, "i="):
			iterations, _ = strconv.Atoi(f[2:])
		}
	}
	salted, _ := pbkdf2.Key(sha256.New, c.Password, salt, iterations, sha256.Size)
	clientKey := mac(salted, "Client Key")
	stored := sha256.Sum256(clientKey)
	withoutProof := "c=biws,r=" + nonce
	c.authMessage = c.bare + "," + string(serverFirst) + "," + withoutProof
	signature := mac(stored[:], c.authMessage)
	proof := make([]byte, len(clientKey))
	for i := range proof {
		proof[i] = clientKey[i] ^ signature[i]
	}
	c.serverKey = mac(salted, "Server Key")
	return []byte(withoutProof + ",p=" + base64.StdEncoding.EncodeToString(proof))
}

// Verify reports whether the server-final message proves the server holds
// the credential.
func (c *Client) Verify(serverFinal []byte) bool {
	want := "v=" + base64.StdEncoding.EncodeToString(mac(c.serverKey, c.authMessage))
	return hmac.Equal([]byte(want), serverFinal)
}
