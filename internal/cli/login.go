package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/jose"
	"github.com/happydave/hoardmq/internal/oidc"
)

// DefaultLoginListen is where login listens for the provider's redirect.
// Some providers (Okta) match the redirect URI exactly, port included, so
// the default is a fixed port the provider can register.
const DefaultLoginListen = "127.0.0.1:8250"

// openBrowser starts the platform's URL opener; login prints the URL
// whether or not this works.
var openBrowser = func(u string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", u)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", u)
	default:
		cmd = exec.Command("xdg-open", u)
	}
	if cmd.Start() == nil {
		go func() { _ = cmd.Wait() }()
	}
}

// runLogin runs `hoardmq login`: the authorization-code flow with PKCE and a
// loopback redirect against an OpenID Connect provider. The ID token it
// obtains is the bearer credential the admin API verifies.
func runLogin(a *app, args []string) int {
	fs := flag.NewFlagSet("hoardmq login", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	issuer := fs.String("issuer", "", "the provider's issuer URL (required)")
	clientID := fs.String("client-id", "", "the client identifier registered with the provider (required)")
	listen := fs.String("listen", DefaultLoginListen, "loopback host:port for the redirect; register http://<this>/callback with the provider")
	scope := fs.String("scope", "openid profile email groups", "scopes to request, space-separated")
	tokenFile := fs.String("token-file", "", "write the ID token to this file (mode 0600) instead of stdout")
	noBrowser := fs.Bool("no-browser", false, "print the login URL without opening a browser")
	timeout := fs.Duration("timeout", 5*time.Minute, "how long to wait for the login")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fprintln(a.stdout, "usage: hoardmq login -issuer URL -client-id ID [flags]")
			fs.SetOutput(a.stdout)
			fs.PrintDefaults()
			return ExitOK
		}
		fprintf(a.stderr, "hoardmq login: %v\n", err)
		return ExitUsage
	}
	switch {
	case fs.NArg() != 0:
		fprintf(a.stderr, "hoardmq login: unexpected argument %q\n", fs.Arg(0))
		return ExitUsage
	case *issuer == "" || *clientID == "":
		fprintln(a.stderr, "hoardmq login: -issuer and -client-id are required")
		return ExitUsage
	}
	if msg := config.CheckIssuer(*issuer); msg != "" {
		fprintf(a.stderr, "hoardmq login: -issuer %s\n", msg)
		return ExitUsage
	}
	if host, _, err := net.SplitHostPort(*listen); err != nil || !loopback(host) {
		fprintln(a.stderr, "hoardmq login: -listen must be a loopback host:port: the provider redirects the code there")
		return ExitUsage
	}
	token, claims, err := login(a, *issuer, *clientID, *listen, strings.Fields(*scope), !*noBrowser, *timeout)
	if err != nil {
		fprintf(a.stderr, "hoardmq login: %v\n", err)
		return ExitProblems
	}
	if *tokenFile != "" {
		if err := writeSecret(*tokenFile, token); err != nil {
			fprintf(a.stderr, "hoardmq login: %v\n", err)
			return ExitProblems
		}
	} else {
		fprintln(a.stdout, token)
	}
	who, _ := claims.String("sub")
	if email, ok := claims.String("email"); ok {
		who += " (" + email + ")"
	}
	groups, _ := claims.Strings("groups")
	exp, _ := claims.Time("exp")
	fprintf(a.stderr, "logged in as %s; groups: %s; the token expires at %s\n", who, strings.Join(groups, ", "),
		time.Unix(exp, 0).UTC().Format(time.RFC3339))
	return ExitOK
}

func loopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// login runs the flow and returns the checked ID token and its claims.
func login(a *app, issuer, clientID, listen string, scopes []string, browser bool, timeout time.Duration) (string, jose.Claims, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	client, err := jose.HTTPClient("")
	if err != nil {
		return "", nil, err
	}
	m, err := oidc.Discover(ctx, client, issuer)
	if err != nil {
		return "", nil, err
	}
	if m.AuthorizationEndpoint == "" || m.TokenEndpoint == "" {
		return "", nil, errors.New("the provider's discovery document names no authorization or token endpoint")
	}
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return "", nil, err
	}
	// The redirect URI keeps the host as given (a provider that matches it
	// exactly tells localhost from 127.0.0.1) and the port actually bound.
	host, _, _ := net.SplitHostPort(listen)
	_, port, _ := net.SplitHostPort(ln.Addr().String())
	req := oidc.Request{ClientID: clientID, RedirectURI: "http://" + net.JoinHostPort(host, port) + "/callback", Scopes: scopes,
		State: oidc.Random(16), Nonce: oidc.Random(16), Verifier: oidc.Random(32)}
	type answer struct {
		code string
		err  error
	}
	answers := make(chan answer, 1)
	var once sync.Once
	mux := http.NewServeMux()
	mux.HandleFunc("GET /callback", func(w http.ResponseWriter, r *http.Request) {
		code, err := oidc.Callback(r.URL.Query(), req.State)
		if err != nil {
			http.Error(w, "HoardMQ login failed: "+err.Error(), http.StatusBadRequest)
		} else {
			_, _ = io.WriteString(w, "HoardMQ login complete: you can close this window.\n")
		}
		once.Do(func() { answers <- answer{code, err} })
		if callbackServed != nil {
			callbackServed()
		}
	})
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	// Shutdown, not Close: the outcome arrives before the callback has
	// written its answer, and Close would cut the browser's connection.
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if srv.Shutdown(sctx) != nil {
			_ = srv.Close()
		}
		cancel()
	}()
	u := oidc.AuthURL(m, req)
	fprintf(a.stderr, "open this URL to log in (waiting %s):\n  %s\n", timeout, u)
	if browser {
		openBrowser(u)
	}
	var got answer
	select {
	case got = <-answers:
	case <-ctx.Done():
		return "", nil, fmt.Errorf("no login within %s", timeout)
	}
	if got.err != nil {
		return "", nil, got.err
	}
	token, err := oidc.Exchange(ctx, client, m, req, got.code)
	if err != nil {
		return "", nil, err
	}
	claims, err := oidc.CheckIDToken(token, issuer, clientID, req.Nonce)
	if err != nil {
		return "", nil, err
	}
	return token, claims, nil
}

// callbackServed, when a test sets it, runs in the callback handler after the
// outcome is handed over and before the handler returns.
var callbackServed func()

// writeSecret writes a credential to path, readable by its owner only: a
// temporary file in the same directory, renamed over path.
func writeSecret(path, value string) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	_, werr := io.WriteString(tmp, value+"\n")
	cerr := tmp.Close()
	if err := errors.Join(werr, cerr, os.Chmod(name, 0o600)); err != nil {
		_ = os.Remove(name)
		return err
	}
	if err := os.Rename(name, path); err != nil {
		_ = os.Remove(name)
		return err
	}
	return nil
}
