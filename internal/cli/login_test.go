package cli

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/oidc/oidctest"
)

// loginRun starts `hoardmq login` against idp with -no-browser, reads the
// authorization URL it prints, and lets browse act as the user's browser on
// it. It returns the exit code and stdout.
func loginRun(t *testing.T, idp *oidctest.Provider, tokenFile string, browse func(authURL string), extra ...string) (int, string, string) {
	t.Helper()
	args := append([]string{"login", "-issuer", idp.URL, "-client-id", idp.ClientID, "-listen", "127.0.0.1:0", "-no-browser", "-timeout", "20s"}, extra...)
	if tokenFile != "" {
		args = append(args, "-token-file", tokenFile)
	}
	pr, pw := io.Pipe()
	var out bytes.Buffer
	done := make(chan int, 1)
	go func() {
		code := Run(args, nil, strings.NewReader(""), &out, pw)
		_ = pw.Close()
		done <- code
	}()
	var errText strings.Builder
	sc := bufio.NewScanner(pr)
	for sc.Scan() {
		line := sc.Text()
		errText.WriteString(line + "\n")
		if strings.HasPrefix(strings.TrimSpace(line), "http") {
			browse(strings.TrimSpace(line))
		}
	}
	select {
	case code := <-done:
		return code, out.String(), errText.String()
	case <-time.After(30 * time.Second):
		t.Fatal("login did not return")
		return 0, "", ""
	}
}

// follow is a browser that follows the provider's redirect to the loopback
// callback.
func follow(t *testing.T) func(string) {
	return func(u string) {
		resp, err := http.Get(u)
		if err != nil {
			t.Errorf("browsing %s: %v", u, err)
			return
		}
		_ = resp.Body.Close()
	}
}

// callbackWith is a browser that goes straight to the loopback callback
// with the given query, the state replaced by the one the URL carries when
// keepState is set.
func callbackWith(t *testing.T, query string, keepState bool) func(string) {
	return func(u string) {
		au, _ := url.Parse(u)
		redirect := au.Query().Get("redirect_uri")
		q, _ := url.ParseQuery(query)
		if keepState {
			q.Set("state", au.Query().Get("state"))
		}
		resp, err := http.Get(redirect + "?" + q.Encode())
		if err != nil {
			t.Errorf("callback: %v", err)
			return
		}
		_ = resp.Body.Close()
	}
}

func claimsOf(t *testing.T, token string) map[string]any {
	t.Helper()
	parts := strings.Split(strings.TrimSpace(token), ".")
	if len(parts) != 3 {
		t.Fatalf("not a JWS: %q", token)
	}
	b, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	var c map[string]any
	if err := json.Unmarshal(b, &c); err != nil {
		t.Fatal(err)
	}
	return c
}

// Workitem AC 2 end to end: the CLI's login yields an ID token, and the
// admin API, given it, answers the identity with its mapped roles.
func TestLoginEndToEnd(t *testing.T) {
	idp := oidctest.New(t, "hoardmq-cli")
	_, addr, opsFile := adminNode(t, func(d *config.Document) {
		d.Admin.Roles = []config.AdminRole{{Name: "operator", Scopes: []string{"read", "clients:write"}}}
		d.Admin.OIDC = &config.AdminOIDC{Issuer: idp.URL, ClientIDs: []string{"hoardmq-cli"},
			RoleMappings: []config.AdminRoleMapping{{Group: "hoardmq-ops", Roles: []string{"operator"}}}}
	})
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "id.token")
	var sentNonce string
	code, out, errOut := loginRun(t, idp, tokenFile, func(u string) {
		au, _ := url.Parse(u)
		sentNonce = au.Query().Get("nonce")
		follow(t)(u)
	})
	if code != ExitOK || out != "" || !strings.Contains(errOut, "logged in as 00u-alice (alice@example.com); groups: everyone, hoardmq-ops") {
		t.Fatalf("login: %d %q %q", code, out, errOut)
	}
	st, err := os.Stat(tokenFile)
	if err != nil || st.Mode().Perm() != 0o600 {
		t.Fatalf("token file: %v %v", st, err)
	}
	raw, _ := os.ReadFile(tokenFile)
	if c := claimsOf(t, string(raw)); sentNonce == "" || c["nonce"] != sentNonce || c["aud"] != "hoardmq-cli" {
		t.Fatalf("the token's claims %v, nonce sent %q", c, sentNonce)
	}
	code, out, errOut = run(t, nil, "whoami", "-admin", addr, "-token-file", tokenFile)
	if code != ExitOK || !strings.Contains(out, "name: oidc:00u-alice") || !strings.Contains(out, "roles: [operator]") || !strings.Contains(out, "clients:write") {
		t.Fatalf("whoami: %d %q %q", code, out, errOut)
	}
	// The same flow for an identity whose groups map to no role: the token
	// is issued, and the admin API refuses it with the reason audited.
	idp.Lock()
	idp.User.Groups = []string{"everyone"}
	idp.Unlock()
	if code, _, errOut := loginRun(t, idp, tokenFile, follow(t)); code != ExitOK {
		t.Fatalf("second login: %d %q", code, errOut)
	}
	if code, _, errOut := run(t, nil, "whoami", "-admin", addr, "-token-file", tokenFile); code != ExitProblems || !strings.Contains(errOut, "401") {
		t.Fatalf("whoami, unmapped: %d %q", code, errOut)
	}
	code, out, _ = run(t, nil, "audit", "tail", "-admin", addr, "-token-file", opsFile, "-json", "-limit", "20")
	var events []struct {
		Action string
		Detail map[string]string
	}
	_ = json.Unmarshal([]byte(out), &events)
	found := false
	for _, e := range events {
		found = found || e.Action == "admin.auth.failure" && e.Detail["reason"] == "oidc: no role mapped"
	}
	if code != ExitOK || !found {
		t.Fatalf("the refusal's audit: %d %s", code, out)
	}
}

// The redirect URI keeps -listen's host, since a provider matching it
// exactly tells localhost from 127.0.0.1.
func TestLoginRedirectKeepsTheHost(t *testing.T) {
	idp := oidctest.New(t, "hoardmq-cli")
	var redirect string
	code, _, errOut := loginRun(t, idp, "", func(u string) {
		au, _ := url.Parse(u)
		redirect = au.Query().Get("redirect_uri")
		follow(t)(u)
	}, "-listen", "localhost:0")
	if code != ExitOK || !strings.HasPrefix(redirect, "http://localhost:") || strings.HasSuffix(redirect, ":0/callback") {
		t.Fatalf("login: %d %q, redirect %q", code, errOut, redirect)
	}
}

// A login that fails writes nothing: another state, the provider's error,
// another nonce.
func TestLoginFailures(t *testing.T) {
	idp := oidctest.New(t, "hoardmq-cli")
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "id.token")
	cases := []struct {
		name   string
		browse func(string)
		setup  func()
		want   string
	}{
		{"another state", callbackWith(t, "code=abc&state=forged", false), nil, "the callback's state"},
		{"the provider's error", callbackWith(t, "error=access_denied", true), nil, "access_denied"},
		{"another nonce", follow(t), func() { idp.Lock(); idp.Nonce = "not-the-one-sent"; idp.Unlock() }, "the ID token's nonce"},
	}
	for _, c := range cases {
		if c.setup != nil {
			c.setup()
		}
		code, out, errOut := loginRun(t, idp, tokenFile, c.browse)
		// The refusal is the line login ends with; the URL it printed first
		// carries "state" and "nonce" too.
		lines := strings.Split(strings.TrimSpace(errOut), "\n")
		last := lines[len(lines)-1]
		if code != ExitProblems || out != "" || !strings.HasPrefix(last, "hoardmq login: ") || !strings.Contains(last, c.want) {
			t.Errorf("%s: %d %q %q", c.name, code, out, errOut)
		}
		if _, err := os.Stat(tokenFile); !os.IsNotExist(err) {
			t.Errorf("%s: a token file was written", c.name)
		}
	}
	if code, _, errOut := run(t, nil, "login", "-issuer", idp.URL, "-client-id", "x", "-listen", "0.0.0.0:0", "-no-browser", "-timeout", "2s"); code != ExitUsage || !strings.Contains(errOut, "loopback") {
		t.Fatalf("a non-loopback -listen: %d %q", code, errOut)
	}
	if code, _, _ := run(t, nil, "login", "-issuer", "http://idp.example", "-client-id", "x"); code != ExitUsage {
		t.Fatalf("an http issuer off loopback: %d", code)
	}
	if code, _, _ := run(t, nil, "login", "-client-id", "x"); code != ExitUsage {
		t.Fatalf("no issuer: %d", code)
	}
}

// API keys through the CLI: create, use, list, revoke; the revoked key is
// refused.
func TestAPIKeyVerbs(t *testing.T) {
	_, addr, opsFile := adminNode(t, nil)
	code, out, errOut := run(t, nil, "apikeys", "create", "-admin", addr, "-token-file", opsFile, "-json", "-name", "ci", "-scopes", "clients:read", "-expires", "1h")
	var k struct {
		ID, Key   string
		ExpiresAt *time.Time
	}
	if code != ExitOK || json.Unmarshal([]byte(out), &k) != nil || k.Key == "" || k.ExpiresAt == nil || !strings.Contains(errOut, "shown only in this answer") {
		t.Fatalf("create: %d %q %q", code, out, errOut)
	}
	keyFile := writeDoc(t, t.TempDir(), "key", k.Key)
	// A node takes API keys once it has renewed its lease since it started.
	deadline := time.Now().Add(15 * time.Second)
	for {
		code, _, errOut := run(t, nil, "clients", "list", "-admin", addr, "-token-file", keyFile)
		if code == ExitOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the key lists clients: %d %q", code, errOut)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if code, out, _ := run(t, nil, "apikeys", "list", "-admin", addr, "-token-file", opsFile); code != ExitOK || !strings.Contains(out, "id="+k.ID) || strings.Contains(out, k.Key) {
		t.Fatalf("list: %d %q", code, out)
	}
	if code, out, errOut := run(t, nil, "apikeys", "revoke", "-admin", addr, "-token-file", opsFile, k.ID); code != ExitOK || !strings.Contains(out, "revokedBy: token:ops") {
		t.Fatalf("revoke: %d %q %q", code, out, errOut)
	}
	if code, _, errOut := run(t, nil, "clients", "list", "-admin", addr, "-token-file", keyFile); code != ExitProblems || !strings.Contains(errOut, "401") {
		t.Fatalf("the revoked key: %d %q", code, errOut)
	}
	if code, _, errOut := run(t, nil, "apikeys", "create", "-admin", addr, "-token-file", opsFile, "-scopes", "clients:read"); code == ExitOK || !strings.Contains(errOut, "-name") {
		t.Fatalf("create without -name: %d %q", code, errOut)
	}
}

// TestLoginCallbackAnswerReachesTheBrowser: the browser gets the callback's
// answer even when the login has its outcome before the handler returns.
func TestLoginCallbackAnswerReachesTheBrowser(t *testing.T) {
	callbackServed = func() { time.Sleep(200 * time.Millisecond) }
	t.Cleanup(func() { callbackServed = nil })
	idp := oidctest.New(t, "hoardmq-cli")
	var status int
	var body string
	browse := func(u string) {
		au, _ := url.Parse(u)
		q := url.Values{"error": {"access_denied"}, "state": {au.Query().Get("state")}}
		resp, err := http.Get(au.Query().Get("redirect_uri") + "?" + q.Encode())
		if err != nil {
			t.Errorf("callback: %v", err)
			return
		}
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		status, body = resp.StatusCode, string(b)
	}
	code, _, errOut := loginRun(t, idp, "", browse)
	if code != ExitProblems || !strings.Contains(errOut, "access_denied") {
		t.Fatalf("login: %d %q", code, errOut)
	}
	if status != http.StatusBadRequest || !strings.Contains(body, "HoardMQ login failed") {
		t.Fatalf("the browser read %d %q", status, body)
	}
}
