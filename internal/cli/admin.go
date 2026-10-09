package cli

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/happydave/hoardmq/internal/config"
)

// adminClient is a thin client of a node's admin API, configured by the
// flags every admin verb shares.
type adminClient struct {
	base    string
	token   string
	http    *http.Client
	json    bool
	address *string
	tokenF  *string
	caFile  *string
	cert    *string
	key     *string
	timeout *time.Duration
	jsonOut *bool
}

// DefaultAdminAddress is where the admin verbs look when neither -admin nor
// HOARDMQ_ADMIN_ADDRESS says.
const DefaultAdminAddress = "127.0.0.1:9091"

// adminAddress is the admin verbs' default address.
func (a *app) adminAddress() string {
	if v, ok := a.env(config.EnvAdminAddress); ok && v != "" {
		return v
	}
	return DefaultAdminAddress
}

// adminFlags registers the shared flags on fs, -admin defaulting to addr.
func (a *app) adminFlags(fs *flag.FlagSet, addr string) *adminClient {
	c := &adminClient{}
	c.address = fs.String("admin", addr, "the node's admin API: host:port, or an https:// URL ($"+config.EnvAdminAddress+")")
	c.tokenF = fs.String("token-file", "", "a file holding the bearer token (default: $"+config.EnvAdminToken+")")
	c.caFile = fs.String("cacert", "", "CA certificate to verify the admin listener's certificate (implies https)")
	c.cert = fs.String("cert", "", "client certificate for mutual TLS (implies https)")
	c.key = fs.String("key", "", "client certificate key")
	return c
}

// ready resolves the parsed flags into a base URL, a token and an HTTP
// client.
func (a *app) ready(c *adminClient) error {
	c.json = *c.jsonOut
	scheme := "http"
	addr := *c.address
	if strings.HasPrefix(addr, "https://") || *c.caFile != "" || *c.cert != "" {
		scheme = "https"
	}
	addr = strings.TrimPrefix(strings.TrimPrefix(addr, "https://"), "http://")
	c.base = scheme + "://" + strings.TrimSuffix(addr, "/")
	if *c.tokenF != "" {
		t, err := config.ReadAdminToken(*c.tokenF)
		if err != nil {
			return err
		}
		c.token = string(t)
	} else if v, ok := a.env(config.EnvAdminToken); ok {
		c.token = strings.TrimSpace(v)
	}
	transport := &http.Transport{}
	if scheme == "https" {
		tc := &tls.Config{MinVersion: tls.VersionTLS12}
		if *c.caFile != "" {
			pem, err := os.ReadFile(*c.caFile)
			if err != nil {
				return err
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return fmt.Errorf("%s: no certificate found", *c.caFile)
			}
			tc.RootCAs = pool
		}
		if *c.cert != "" {
			pair, err := tls.LoadX509KeyPair(*c.cert, *c.key)
			if err != nil {
				return err
			}
			tc.Certificates = []tls.Certificate{pair}
		}
		transport.TLSClientConfig = tc
	}
	c.http = &http.Client{Timeout: *c.timeout, Transport: transport}
	return nil
}

// apiError is a non-2xx answer, with the problem document's detail.
type apiError struct {
	status  string
	code    int
	detail  string
	owner   string
	network bool
	// err is the transport error behind a network apiError.
	err error
}

func (e *apiError) Unwrap() error { return e.err }

func (e *apiError) Error() string {
	if e.network {
		return e.detail
	}
	msg := e.status
	if e.detail != "" {
		msg += ": " + e.detail
	}
	if e.owner != "" {
		msg += " (owner " + e.owner + ")"
	}
	return msg
}

// call performs one request and returns the body of a 2xx answer.
func (c *adminClient) call(method, path string, query url.Values, body []byte) ([]byte, error) {
	u := c.base + "/api/v1" + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, u, r)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, &apiError{network: true, detail: err.Error(), err: err}
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		e := &apiError{status: resp.Status, code: resp.StatusCode}
		var p struct {
			Detail string `json:"detail"`
			Owner  string `json:"owner"`
		}
		if json.Unmarshal(data, &p) == nil {
			e.detail, e.owner = p.Detail, p.Owner
		} else {
			e.detail = strings.TrimSpace(string(data))
		}
		return nil, e
	}
	return data, nil
}

// adminVerb is one sub-verb of an admin group: its usage, its own flags,
// the number of positional arguments it takes, and the request it makes.
type adminVerb struct {
	name  string
	args  string
	nargs int
	flags func(fs *flag.FlagSet) func() url.Values
	// request builds the method, path, query and body from the arguments.
	request func(args []string, q url.Values) (method, path string, body []byte, err error)
	// after, when set, is printed to stderr after a successful call.
	after string
	// failed, when set, reads a successful call's answer and says whether
	// the verb failed all the same: exit 1 after printing it.
	failed func(data []byte) bool
}

// escape puts a client or module identifier into one path segment.
func escape(id string) string { return url.PathEscape(id) }

var adminGroups = map[string][]adminVerb{
	"clients": {
		{name: "list", flags: func(fs *flag.FlagSet) func() url.Values {
			search := fs.String("search", "", "substring of the client identifier")
			node := fs.String("node", "", "only the clients this node holds")
			paging := pagingFlags(fs, url.Values{}, "search", search)
			return func() url.Values {
				q := paging()
				if *node != "" {
					q.Set("node", *node)
				}
				return q
			}
		}, request: get(func(_ []string) string { return "/clients" })},
		{name: "inspect", args: "CLIENT-ID", nargs: 1, request: get(func(a []string) string { return "/clients/" + escape(a[0]) })},
		{name: "subscriptions", args: "CLIENT-ID", nargs: 1, request: get(func(a []string) string { return "/clients/" + escape(a[0]) + "/subscriptions" })},
		{name: "matching", args: "TOPIC", nargs: 1, request: func(a []string, q url.Values) (string, string, []byte, error) {
			q.Set("topic", a[0])
			return "GET", "/subscriptions", nil, nil
		}},
		{name: "disconnect", args: "CLIENT-ID", nargs: 1, request: fixed("POST", func(a []string) string { return "/clients/" + escape(a[0]) + "/disconnect" })},
		{name: "invalidate", args: "CLIENT-ID", nargs: 1, request: fixed("DELETE", func(a []string) string { return "/clients/" + escape(a[0]) + "/session" })},
	},
	"retained": {
		{name: "list", flags: func(fs *flag.FlagSet) func() url.Values {
			filter := fs.String("filter", "#", "topic filter")
			return pagingFlags(fs, url.Values{}, "filter", filter)
		}, request: get(func([]string) string { return "/retained" })},
		{name: "delete", args: "TOPIC", nargs: 1, request: func(a []string, q url.Values) (string, string, []byte, error) {
			q.Set("topic", a[0])
			return "DELETE", "/retained", nil, nil
		}},
	},
	"cluster": {
		{name: "status", request: get(func([]string) string { return "/cluster" })},
		{name: "shards", request: get(func([]string) string { return "/cluster/shards" })},
		{name: "lease", args: "CLIENT-ID", nargs: 1, request: get(func(a []string) string { return "/cluster/leases/" + escape(a[0]) })},
	},
	"traces": {
		{name: "start", args: "CLIENT-ID", nargs: 1, flags: func(fs *flag.FlagSet) func() url.Values {
			limit := fs.Int("limit", 0, "events kept (default observability.traceEvents)")
			return func() url.Values { return url.Values{"limit": {strconv.Itoa(*limit)}} }
		}, request: func(a []string, q url.Values) (string, string, []byte, error) {
			n, _ := strconv.Atoi(q.Get("limit"))
			q.Del("limit")
			body, _ := json.Marshal(map[string]int{"limit": n})
			return "POST", "/traces/" + escape(a[0]), body, nil
		}},
		{name: "stop", args: "CLIENT-ID", nargs: 1, request: fixed("DELETE", func(a []string) string { return "/traces/" + escape(a[0]) })},
		{name: "fetch", args: "CLIENT-ID", nargs: 1, request: get(func(a []string) string { return "/traces/" + escape(a[0]) })},
	},
	"modules": {
		{name: "list", request: get(func([]string) string { return "/modules" })},
		{name: "enable", args: "MODULE-ID", nargs: 1, request: fixed("POST", func(a []string) string { return "/modules/" + escape(a[0]) + "/enable" })},
		{name: "disable", args: "MODULE-ID", nargs: 1, request: fixed("POST", func(a []string) string { return "/modules/" + escape(a[0]) + "/disable" })},
	},
	"audit": {
		{name: "tail", flags: func(fs *flag.FlagSet) func() url.Values {
			limit := fs.Int("limit", 100, "events, 1 to 1000")
			return func() url.Values { return url.Values{"limit": {strconv.Itoa(*limit)}} }
		}, request: get(func([]string) string { return "/audit" })},
	},
	"apikeys": {
		{name: "list", request: get(func([]string) string { return "/apikeys" })},
		{name: "create", flags: func(fs *flag.FlagSet) func() url.Values {
			name := fs.String("name", "", "the key's name (required)")
			scopes := fs.String("scopes", "", "comma-separated scopes the key holds")
			roles := fs.String("roles", "", "comma-separated roles whose scopes the key holds")
			expires := fs.Duration("expires", 0, "how long the key lives (0: it does not expire; else at least 1m)")
			return func() url.Values {
				return url.Values{"name": {*name}, "scopes": {*scopes}, "roles": {*roles}, "expires": {strconv.FormatInt(int64(*expires/time.Second), 10)}}
			}
		}, request: func(_ []string, q url.Values) (string, string, []byte, error) {
			req := struct {
				Name             string   `json:"name"`
				Scopes           []string `json:"scopes,omitempty"`
				Roles            []string `json:"roles,omitempty"`
				ExpiresInSeconds int64    `json:"expiresInSeconds,omitempty"`
			}{Name: q.Get("name"), Scopes: list(q.Get("scopes")), Roles: list(q.Get("roles"))}
			req.ExpiresInSeconds, _ = strconv.ParseInt(q.Get("expires"), 10, 64)
			for _, k := range []string{"name", "scopes", "roles", "expires"} {
				q.Del(k)
			}
			if req.Name == "" {
				return "", "", nil, errors.New("-name is required")
			}
			body, err := json.Marshal(req)
			return "POST", "/apikeys", body, err
		}, after: "the key is shown only in this answer: store it now; revoke it with hoardmq apikeys revoke <id>"},
		{name: "revoke", args: "KEY-ID", nargs: 1, request: fixed("DELETE", func(a []string) string { return "/apikeys/" + escape(a[0]) })},
	},
	"credentials": {
		{name: "list", flags: func(fs *flag.FlagSet) func() url.Values {
			none := ""
			return pagingFlags(fs, url.Values{}, "", &none)
		}, request: get(func([]string) string { return "/credentials" })},
		{name: "show", args: "USERNAME", nargs: 1, request: get(func(a []string) string { return "/credentials/" + escape(a[0]) })},
		{name: "add", args: "USERNAME", nargs: 1, flags: credentialFlags(false), request: credentialRequest("add")},
		{name: "update", args: "USERNAME", nargs: 1, flags: credentialFlags(true), request: credentialRequest("update")},
		{name: "remove", args: "USERNAME", nargs: 1, flags: func(fs *flag.FlagSet) func() url.Values {
			ifVersion := fs.Uint64("if-version", 0, "remove only at this version")
			return func() url.Values {
				if *ifVersion == 0 {
					return url.Values{}
				}
				return url.Values{"ifVersion": {strconv.FormatUint(*ifVersion, 10)}}
			}
		}, request: fixed("DELETE", func(a []string) string { return "/credentials/" + escape(a[0]) })},
	},
	"bans": {
		{name: "list", flags: func(fs *flag.FlagSet) func() url.Values {
			none := ""
			return pagingFlags(fs, url.Values{}, "", &none)
		}, request: get(func([]string) string { return "/bans" })},
		{name: "add", args: "KIND VALUE", nargs: 2, flags: func(fs *flag.FlagSet) func() url.Values {
			reason := fs.String("reason", "", "why, kept with the ban")
			expires := fs.Duration("expires", 0, "how long the ban lasts (0: until it is removed)")
			return func() url.Values {
				return url.Values{"reason": {*reason}, "expires": {strconv.FormatInt(int64(*expires/time.Second), 10)}}
			}
		}, request: func(a []string, q url.Values) (string, string, []byte, error) {
			body := map[string]any{"kind": a[0], "value": a[1]}
			if r := q.Get("reason"); r != "" {
				body["reason"] = r
			}
			if n, _ := strconv.ParseInt(q.Get("expires"), 10, 64); n > 0 {
				body["expiresInSeconds"] = n
			}
			q.Del("reason")
			q.Del("expires")
			data, err := json.Marshal(body)
			return "POST", "/bans", data, err
		}},
		{name: "remove", args: "KIND VALUE", nargs: 2, request: fixed("DELETE", func(a []string) string { return "/bans/" + escape(a[0]+":"+a[1]) })},
	},
	"limits": {
		{name: "show", request: get(func([]string) string { return "/limits" })},
		{name: "set", flags: func(fs *flag.FlagSet) func() url.Values {
			names := []string{"client-message-rate", "client-message-burst", "client-byte-rate", "client-byte-burst", "connect-rate", "connect-burst"}
			vals := map[string]*int{}
			for _, n := range names {
				vals[n] = fs.Int(n, -1, "the administered "+n+" (unset: the configuration's)")
			}
			return func() url.Values {
				q := url.Values{}
				for n, v := range vals {
					if *v >= 0 {
						q.Set(n, strconv.Itoa(*v))
					}
				}
				return q
			}
		}, request: func(_ []string, q url.Values) (string, string, []byte, error) {
			members := map[string]string{"client-message-rate": "clientMessageRate", "client-message-burst": "clientMessageBurst",
				"client-byte-rate": "clientByteRate", "client-byte-burst": "clientByteBurst", "connect-rate": "connectRate", "connect-burst": "connectBurst"}
			body := map[string]int{}
			for flagName, member := range members {
				if v := q.Get(flagName); v != "" {
					body[member], _ = strconv.Atoi(v)
					q.Del(flagName)
				}
			}
			data, err := json.Marshal(body)
			return "PUT", "/limits", data, err
		}, after: "the administered limits are replaced whole: a flag not given falls back to the configuration"},
	},
	"config": {
		{name: "show", request: get(func([]string) string { return "/config" })},
		{name: "validate", args: "FILE", nargs: 1, request: func(a []string, _ url.Values) (string, string, []byte, error) {
			data, err := os.ReadFile(a[0])
			if err != nil {
				return "", "", nil, err
			}
			return "POST", "/config/validate", data, nil
		}},
		{name: "reload", request: fixed("POST", func([]string) string { return "/config/reload" }), failed: notApplied},
	},
}

// list splits a comma-separated flag value, dropping empty items.
func list(v string) []string {
	var out []string
	for _, x := range strings.Split(v, ",") {
		if x = strings.TrimSpace(x); x != "" {
			out = append(out, x)
		}
	}
	return out
}

func get(path func([]string) string) func([]string, url.Values) (string, string, []byte, error) {
	return fixed("GET", path)
}

func fixed(method string, path func([]string) string) func([]string, url.Values) (string, string, []byte, error) {
	return func(a []string, _ url.Values) (string, string, []byte, error) { return method, path(a), nil, nil }
}

// pagingFlags adds -after and -limit and returns the query builder.
func pagingFlags(fs *flag.FlagSet, q url.Values, name string, value *string) func() url.Values {
	after := fs.String("after", "", "cursor: the last item of the previous page")
	limit := fs.Int("limit", 100, "items per page, 1 to 1000")
	return func() url.Values {
		if *value != "" {
			q.Set(name, *value)
		}
		if *after != "" {
			q.Set("after", *after)
		}
		q.Set("limit", strconv.Itoa(*limit))
		return q
	}
}

// runAdminGroup runs `hoardmq <group> <verb> [flags] [args]`.
func runAdminGroup(group string) func(a *app, args []string) int {
	return func(a *app, args []string) int {
		verbs := adminGroups[group]
		usage := func(w io.Writer) {
			fprintf(w, "usage: hoardmq %s <verb> [flags] [args]\n\nverbs:\n", group)
			for _, v := range verbs {
				fprintf(w, "  %s %s\n", v.name, v.args)
			}
			fprintln(w, "\nflags go before the arguments; every verb takes -admin, -token-file, -cacert, -cert, -key, -timeout and -json.")
		}
		if len(args) == 0 {
			usage(a.stderr)
			return ExitUsage
		}
		if args[0] == "-h" || args[0] == "-help" || args[0] == "help" {
			usage(a.stdout)
			return ExitOK
		}
		var verb *adminVerb
		for i := range verbs {
			if verbs[i].name == args[0] {
				verb = &verbs[i]
			}
		}
		if verb == nil {
			fprintf(a.stderr, "hoardmq %s: unknown verb %q\n", group, args[0])
			usage(a.stderr)
			return ExitUsage
		}
		return a.runAdminVerb("hoardmq "+group+" "+args[0], verb, args[1:])
	}
}

// runWhoami runs `hoardmq whoami [flags]`: the identity the credential
// authenticates as.
func runWhoami(a *app, args []string) int {
	return a.runAdminVerb("hoardmq whoami", &adminVerb{name: "whoami", request: get(func([]string) string { return "/whoami" })}, args)
}

// runAdminVerb parses one verb's flags and arguments, makes its request and
// prints the answer.
func (a *app) runAdminVerb(name string, verb *adminVerb, args []string) int {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	c := a.adminFlags(fs, a.adminAddress())
	c.timeout = fs.Duration("timeout", 30*time.Second, "how long one request may take")
	c.jsonOut = fs.Bool("json", false, "print the API's JSON instead of text")
	query := func() url.Values { return url.Values{} }
	if verb.flags != nil {
		query = verb.flags(fs)
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fprintf(a.stdout, "usage: %s [flags] %s\n", name, verb.args)
			fs.SetOutput(a.stdout)
			fs.PrintDefaults()
			return ExitOK
		}
		fprintf(a.stderr, "%s: %v\n", name, err)
		return ExitUsage
	}
	if fs.NArg() != verb.nargs {
		fprintf(a.stderr, "%s: want %d argument(s) %s, got %d\n", name, verb.nargs, verb.args, fs.NArg())
		return ExitUsage
	}
	if err := a.ready(c); err != nil {
		fprintf(a.stderr, "%s: %v\n", name, err)
		return ExitProblems
	}
	q := query()
	method, path, body, err := verb.request(fs.Args(), q)
	if err != nil {
		fprintf(a.stderr, "%s: %v\n", name, err)
		if errors.Is(err, errUsage) {
			return ExitUsage
		}
		return ExitProblems
	}
	data, err := c.call(method, path, q, body)
	if err != nil {
		fprintf(a.stderr, "%s: %v\n", name, err)
		return ExitProblems
	}
	if c.json {
		_, _ = a.stdout.Write(data)
	} else if err := render(a.stdout, data); err != nil {
		fprintf(a.stderr, "%s: %v\n", name, err)
		return ExitProblems
	}
	if verb.after != "" {
		fprintln(a.stderr, verb.after)
	}
	if verb.failed != nil && verb.failed(data) {
		return ExitProblems
	}
	return ExitOK
}

// notApplied reads a reload's answer: the node changed nothing.
func notApplied(data []byte) bool {
	var r struct {
		Applied bool `json:"applied"`
	}
	return json.Unmarshal(data, &r) != nil || !r.Applied
}

// errUsage marks a request that the flags make impossible: exit 2.
var errUsage = errors.New("usage")

// attributeFlag collects repeated -attribute name=value flags.
type attributeFlag struct{ values *[]string }

func (f attributeFlag) String() string { return "" }
func (f attributeFlag) Set(v string) error {
	if k, _, ok := strings.Cut(v, "="); !ok || k == "" {
		return errors.New("want name=value")
	}
	*f.values = append(*f.values, v)
	return nil
}

// credentialFlags are add's and update's flags. The password is read from a
// file, so it is never on the command line.
func credentialFlags(update bool) func(fs *flag.FlagSet) func() url.Values {
	return func(fs *flag.FlagSet) func() url.Values {
		passwordFile := fs.String("password-file", "", "a file holding the password; the node stores its hash")
		hashFile := fs.String("hash-file", "", "a file holding a PHC hash hoardmq hash-password wrote")
		scram := fs.Bool("scram", false, "store the password in the SCRAM-SHA-256 form")
		var attrs []string
		fs.Var(attributeFlag{&attrs}, "attribute", "name=value, repeatable; on update, replaces every attribute")
		var ifVersion *uint64
		if update {
			ifVersion = fs.Uint64("if-version", 0, "update only at this version")
		}
		return func() url.Values {
			q := url.Values{"password-file": {*passwordFile}, "hash-file": {*hashFile}, "scram": {strconv.FormatBool(*scram)}, "attribute": attrs}
			if ifVersion != nil && *ifVersion != 0 {
				q.Set("if-version", strconv.FormatUint(*ifVersion, 10))
			}
			return q
		}
	}
}

// credentialRequest builds add's POST or update's PUT from the flags,
// reading the password or the hash from its file.
func credentialRequest(verb string) func(a []string, q url.Values) (string, string, []byte, error) {
	return func(a []string, q url.Values) (string, string, []byte, error) {
		passwordFile, hashFile, attrs, ifVersion := q.Get("password-file"), q.Get("hash-file"), q["attribute"], q.Get("if-version")
		scram := q.Get("scram") == "true"
		for _, k := range []string{"password-file", "hash-file", "scram", "attribute", "if-version"} {
			q.Del(k)
		}
		if passwordFile != "" && hashFile != "" {
			return "", "", nil, fmt.Errorf("%w: -password-file and -hash-file together", errUsage)
		}
		if verb == "add" && passwordFile == "" && hashFile == "" {
			return "", "", nil, fmt.Errorf("%w: -password-file or -hash-file is required", errUsage)
		}
		body := map[string]any{}
		if verb == "add" {
			body["username"] = a[0]
		}
		for flagName, member := range map[string]string{passwordFile: "password", hashFile: "passwordHash"} {
			if flagName == "" {
				continue
			}
			data, err := os.ReadFile(flagName)
			if err != nil {
				return "", "", nil, err
			}
			body[member] = strings.TrimRight(string(data), "\r\n")
		}
		if scram {
			body["scram"] = true
		}
		if len(attrs) > 0 {
			m := map[string]string{}
			for _, kv := range attrs {
				k, v, _ := strings.Cut(kv, "=")
				m[k] = v
			}
			body["attributes"] = m
		}
		if ifVersion != "" {
			n, _ := strconv.ParseUint(ifVersion, 10, 64)
			body["ifVersion"] = n
		}
		data, err := json.Marshal(body)
		if verb == "add" {
			return "POST", "/credentials", data, err
		}
		return "PUT", "/credentials/" + escape(a[0]), data, err
	}
}
