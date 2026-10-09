package sim

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/ldap/ldaptest"
	"github.com/happydave/hoardmq/internal/oidc/oidctest"
	"github.com/happydave/hoardmq/internal/packet"
)

// authEnv selects how the measurement's clients authenticate: unset is
// anonymous, "jwt" gives every client an access token the nodes verify
// against an in-process JWKS, "ldap" a password the nodes bind with to an
// in-process directory, "file" a password the nodes verify against a users
// file of PBKDF2 hashes at the product's iteration count.
const authEnv = "HOARDMQ_SIM_AUTH"

// simAuthMode is the variable's value, or an error naming an unknown one.
func simAuthMode() (string, error) {
	switch v := os.Getenv(authEnv); v {
	case "", "anonymous":
		return "anonymous", nil
	case "jwt", "ldap", "file":
		return v, nil
	default:
		return "", fmt.Errorf("%s=%q: want jwt, ldap, file or unset", authEnv, v)
	}
}

// tokenSource mints and keeps one access token per client identifier.
type tokenSource struct {
	t        testing.TB
	provider *oidctest.Provider
	mu       sync.Mutex
	tokens   map[string]string
}

// credentials is the running cluster's token source, nil when the clients
// are anonymous. The cluster that sets it restores it at cleanup.
var credentials atomic.Pointer[tokenSource]

func (s *tokenSource) token(id string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if tok, ok := s.tokens[id]; ok {
		return tok
	}
	now := time.Now()
	tok := s.provider.SignWith(s.t, "k1-RS256", map[string]any{"typ": "at+jwt"}, map[string]any{
		"iss": s.provider.URL, "sub": id, "aud": "hoardmq", "iat": now.Unix(), "exp": now.Add(2 * time.Hour).Unix()})
	s.tokens[id] = tok
	return tok
}

// jwtModule is the nodes' auth.jwt configuration for the provider.
func jwtModule(p *oidctest.Provider) json.RawMessage {
	return json.RawMessage(`{"issuer":"` + p.URL + `","audiences":["hoardmq"],"jwksUri":"` + p.URL + `/keys"}`)
}

// startTokenSource starts the provider and installs the token source for
// the cluster's lifetime.
func startTokenSource(t testing.TB) *oidctest.Provider {
	t.Helper()
	p := oidctest.New(t, "hoardmq")
	prev := credentials.Swap(&tokenSource{t: t, provider: p, tokens: map[string]string{}})
	t.Cleanup(func() { credentials.Store(prev) })
	return p
}

// withCredentials gives a CONNECT the client's username and token, or its
// directory password, when the clients authenticate, and leaves it anonymous
// otherwise.
func withCredentials(c *packet.Connect) *packet.Connect {
	if s := credentials.Load(); s != nil {
		c.HasUsername, c.Username = true, c.ClientID
		c.HasPassword, c.Password = true, []byte(s.token(c.ClientID))
	} else if directoryOn.Load() {
		c.HasUsername, c.Username = true, c.ClientID
		c.HasPassword, c.Password = true, []byte(directoryPassword(c.ClientID))
	} else if inUsersFile(c.ClientID) {
		c.HasUsername, c.Username = true, c.ClientID
		c.HasPassword, c.Password = true, []byte(directoryPassword(c.ClientID))
	}
	return c
}

// fileUsers is how many of the population's clients (f000000 upwards) a
// users file holds while a cluster's clients authenticate by it; 0 otherwise.
// The probes and every other client connect anonymously beside them.
var fileUsers atomic.Int64

// inUsersFile reports whether id is one of the population's clients the
// running users file holds.
func inUsersFile(id string) bool {
	n := fileUsers.Load()
	if n == 0 || len(id) != 7 || id[0] != 'f' {
		return false
	}
	i, err := strconv.Atoi(id[1:])
	return err == nil && i >= 0 && int64(i) < n
}

// startUsersFile writes a users file of the population's first n clients,
// each with its own password hashed as hoardmq hash-password hashes it, in
// parallel and off the storm path, and turns their passwords on for the
// cluster's lifetime.
func startUsersFile(t testing.TB, dir string, n int) string {
	t.Helper()
	if n <= 0 {
		t.Fatalf("%s=file needs the population's size", authEnv)
	}
	lines := make([]string, n)
	var wg sync.WaitGroup
	var failed atomic.Pointer[error]
	var next atomic.Int64
	for w := 0; w < runtime.GOMAXPROCS(0); w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := int(next.Add(1) - 1); i < n; i = int(next.Add(1) - 1) {
				id := fmt.Sprintf("f%06d", i)
				phc, err := auth.HashPassword([]byte(directoryPassword(id)))
				if err != nil {
					failed.Store(&err)
					return
				}
				lines[i] = `{"username":"` + id + `","password":"` + phc + `"}`
			}
		}()
	}
	wg.Wait()
	if err := failed.Load(); err != nil {
		t.Fatal(*err)
	}
	path := filepath.Join(dir, "users.json")
	if err := os.WriteFile(path, []byte(`{"users":[`+strings.Join(lines, ",")+`]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	prev := fileUsers.Swap(int64(n))
	t.Cleanup(func() { fileUsers.Store(prev) })
	return path
}

// The directory the nodes bind to when the clients authenticate by LDAP:
// every uid under directoryBase exists, with directoryPassword's password.
const (
	directoryBase    = "ou=people,dc=hoardmq,dc=test"
	directoryService = "cn=broker,dc=hoardmq,dc=test"
)

// directoryOn is set while a cluster's clients authenticate by LDAP.
var directoryOn atomic.Bool

func directoryPassword(id string) string { return "pw-" + id }

// startDirectory starts the directory and turns the clients' passwords on
// for the cluster's lifetime; it writes the service account's password file.
func startDirectory(t testing.TB, dir string) (*ldaptest.Server, string) {
	t.Helper()
	s := ldaptest.New(t, directoryBase, ldaptest.Entry{DN: directoryService, Password: "broker-secret"})
	s.SetOpen(func(uid string) (string, bool) { return directoryPassword(uid), uid != "" })
	pw := filepath.Join(dir, "directory-password")
	if err := os.WriteFile(pw, []byte("broker-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	prev := directoryOn.Swap(true)
	t.Cleanup(func() { directoryOn.Store(prev) })
	return s, pw
}

// ldapModule is the nodes' auth.ldap configuration: search then bind, with
// the service account, over loopback.
func ldapModule(s *ldaptest.Server, passwordFile string) json.RawMessage {
	return json.RawMessage(`{"url":"` + s.URL + `","search":{"baseDn":"` + directoryBase +
		`","filter":"(&(objectClass=person)(uid=${username}))","bindDn":"` + directoryService + `","bindPasswordFile":"` + passwordFile + `"}}`)
}

// premint makes the client's token now, off the storm path.
func premint(id string) {
	if s := credentials.Load(); s != nil {
		_ = s.token(id)
	}
}
