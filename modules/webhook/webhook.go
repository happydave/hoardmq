// Package webhook is HoardMQ's HTTP webhook pair, built on the public
// extension API alone, as a module from outside the broker would be:
//
//   - auth.http asks an HTTP endpoint to decide each CONNECT and each publish,
//     subscribe and unsubscribe, in EMQX's request and response shapes, with
//     VerneMQ's cache-control header for how long an answer holds. An endpoint
//     that cannot answer denies, unless onUnavailable is "pass".
//   - events.http posts the events the extension API carries to an HTTP
//     endpoint: asynchronously through a bounded buffer whose drops are
//     counted and logged, or synchronously on the path that raised them.
//
// Every binary built from the broker's entry point links both; each runs only
// when configured.
package webhook

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// maxResponseBytes bounds what is read of an endpoint's answer.
const maxResponseBytes = 64 << 10

// endpointConfig is what both modules know of their endpoint.
type endpointConfig struct {
	URL             string            `json:"url"`
	TimeoutMs       *int              `json:"timeoutMs"`
	CAFile          string            `json:"caFile"`
	CertFile        string            `json:"certFile"`
	KeyFile         string            `json:"keyFile"`
	BearerTokenFile string            `json:"bearerTokenFile"`
	Headers         map[string]string `json:"headers"`
	FollowRedirects bool              `json:"followRedirects"`
}

// endpoint is a validated endpoint: its URL, client and request headers.
type endpoint struct {
	url     string
	client  *http.Client
	headers http.Header
	timeout time.Duration
}

func decodeStrict(raw json.RawMessage, v any) error {
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = json.RawMessage("{}")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

func intOr(p *int, def int) int {
	if p == nil {
		return def
	}
	return *p
}

func inRange(problems []string, name string, p *int, lo, hi int) []string {
	if p != nil && (*p < lo || *p > hi) {
		problems = append(problems, fmt.Sprintf("%s must be %d to %d", name, lo, hi))
	}
	return problems
}

// loopback reports whether host names this machine.
func loopback(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// build validates c and reads the files it names; it connects to nothing.
// With clearOnlyLoopback, a plain http URL must name a loopback host.
func (c endpointConfig) build(defaultTimeout int, clearOnlyLoopback bool, readFile func(string) ([]byte, error)) (*endpoint, []string) {
	var problems []string
	u, err := url.Parse(c.URL)
	switch {
	case c.URL == "":
		problems = append(problems, "url is required")
	case err != nil || !u.IsAbs() || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https"):
		problems = append(problems, "url must be an absolute http or https URL")
	case u.Scheme == "http" && clearOnlyLoopback && !loopback(u.Hostname()):
		problems = append(problems, "url must be https unless its host is loopback: the requests carry credentials")
	}
	problems = inRange(problems, "timeoutMs", c.TimeoutMs, 100, 30000)
	if (c.CertFile == "") != (c.KeyFile == "") {
		problems = append(problems, "certFile and keyFile go together")
	}
	tc := &tls.Config{MinVersion: tls.VersionTLS12}
	if c.CAFile != "" {
		pem, err := readFile(c.CAFile)
		pool := x509.NewCertPool()
		if err != nil || !pool.AppendCertsFromPEM(pem) {
			problems = append(problems, fmt.Sprintf("caFile %s holds no certificate", c.CAFile))
		}
		tc.RootCAs = pool
	}
	if c.CertFile != "" && c.KeyFile != "" {
		certPEM, err1 := readFile(c.CertFile)
		keyPEM, err2 := readFile(c.KeyFile)
		pair, err := tls.X509KeyPair(certPEM, keyPEM)
		if err1 != nil || err2 != nil || err != nil {
			problems = append(problems, "certFile and keyFile do not hold a certificate and its key")
		} else {
			tc.Certificates = []tls.Certificate{pair}
		}
	}
	headers := http.Header{}
	for k, v := range c.Headers {
		switch {
		case k == "" || strings.ContainsAny(k, " \t\r\n:"):
			problems = append(problems, fmt.Sprintf("headers: %q is not a header name", k))
		case strings.ContainsAny(v, "\r\n"):
			// The value is not quoted: it may be a secret.
			problems = append(problems, fmt.Sprintf("headers: the value of %s holds a line break", k))
		case http.CanonicalHeaderKey(k) == "Authorization" && c.BearerTokenFile != "":
			problems = append(problems, "headers sets Authorization beside bearerTokenFile")
		}
		headers.Set(k, v)
	}
	if c.BearerTokenFile != "" {
		token, err := readFile(c.BearerTokenFile)
		t := strings.TrimSpace(string(token))
		if err != nil || t == "" {
			problems = append(problems, fmt.Sprintf("bearerTokenFile %s is empty or unreadable", c.BearerTokenFile))
		}
		headers.Set("Authorization", "Bearer "+t)
	}
	headers.Set("Content-Type", "application/json")
	headers.Set("User-Agent", "hoardmq")
	if len(problems) > 0 {
		return nil, problems
	}
	timeout := time.Duration(intOr(c.TimeoutMs, defaultTimeout)) * time.Millisecond
	client := &http.Client{
		Timeout:   timeout,
		Transport: &http.Transport{TLSClientConfig: tc, MaxIdleConnsPerHost: 16, IdleConnTimeout: 90 * time.Second},
	}
	if !c.FollowRedirects {
		client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	}
	return &endpoint{url: c.URL, client: client, headers: headers, timeout: timeout}, nil
}

// response is what an endpoint answered.
type response struct {
	status int
	header http.Header
	body   []byte
}

// errTooLarge is an answer over maxResponseBytes.
var errTooLarge = errors.New("the answer is over 64 KiB")

// post sends body with the endpoint's headers and extra ones.
func (e *endpoint) post(ctx context.Context, body []byte, extra map[string]string) (response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.url, bytes.NewReader(body))
	if err != nil {
		return response{}, err
	}
	for k, v := range e.headers {
		req.Header[k] = v
	}
	for k, v := range extra {
		req.Header.Set(k, v)
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return response{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return response{}, err
	}
	if len(data) > maxResponseBytes {
		return response{}, errTooLarge
	}
	return response{status: resp.StatusCode, header: resp.Header, body: data}, nil
}

func readFileOS(name string) ([]byte, error) { return os.ReadFile(name) }

// redact keeps a transport error's kind and drops the URL, whose query may
// carry a secret.
func redact(err error) string {
	var ue *url.Error
	if errors.As(err, &ue) {
		return ue.Err.Error()
	}
	return err.Error()
}
