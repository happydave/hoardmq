package cli

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/broker"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/testcert"
)

const cliOpsToken = "cli-test-ops-token-0123456789abc"

// adminNode starts a one-node broker with the admin API on an ephemeral
// loopback port and returns it, its admin address and a token file.
func adminNode(t *testing.T, mutate func(*config.Document)) (*broker.Server, string, string) {
	t.Helper()
	dir := t.TempDir()
	tokenFile := writeDoc(t, dir, "ops.token", cliOpsToken+"\n")
	doc := config.Default()
	doc.Node.DataDir = filepath.Join(dir, "data")
	doc.Auth.Anonymous = true
	doc.Cluster.Raft.TickMs = 10
	doc.Cluster.Readiness.StableMs = 0
	doc.Listeners = []config.Listener{{Name: "tcp", Type: "tcp", Address: "127.0.0.1:0"}}
	acl := writeDoc(t, dir, "acl.json", `{"rules":[{"topic":"#","allow":["publish","subscribe","unsubscribe"]}]}`)
	doc.Modules = map[string]json.RawMessage{"authz.file": json.RawMessage(`{"path":"` + acl + `"}`)}
	doc.Admin.Address = "127.0.0.1:0"
	doc.Admin.Tokens = []config.AdminToken{{Name: "ops", TokenFile: tokenFile, Scopes: []string{"admin"}}}
	if mutate != nil {
		mutate(&doc)
	}
	if p := doc.Validate(); len(p) != 0 {
		t.Fatal(p)
	}
	srv, err := broker.New(doc, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && !srv.Consensus().Ready() {
		time.Sleep(10 * time.Millisecond)
	}
	return srv, srv.AdminAddr().String(), tokenFile
}

// mqttConnect opens an MQTT 5 session and returns the connection.
func mqttConnect(t *testing.T, srv *broker.Server, id string, subscribe string) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", srv.Addr("tcp").String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	enc := &packet.Encoder{Version: packet.V5}
	dec := packet.NewDecoder(c)
	dec.Version = packet.V5
	exp := uint32(600)
	if err := enc.Encode(c, &packet.Connect{ProtocolLevel: packet.V5, ClientID: id, CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}); err != nil {
		t.Fatal(err)
	}
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	if p, err := dec.Read(); err != nil {
		t.Fatal(err)
	} else if ack, ok := p.(*packet.Connack); !ok || ack.ReasonCode != packet.Success {
		t.Fatalf("connack %v", p)
	}
	if subscribe != "" {
		if err := enc.Encode(c, &packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: subscribe, QoS: 1}}}); err != nil {
			t.Fatal(err)
		}
		if _, err := dec.Read(); err != nil {
			t.Fatal(err)
		}
	}
	return c
}

func TestAdminVerbs(t *testing.T) {
	srv, addr, tokenFile := adminNode(t, nil)
	mqttConnect(t, srv, "gw/one", "t/+")
	mqttConnect(t, srv, "other", "")
	common := []string{"-admin", addr, "-token-file", tokenFile}
	runVerb := func(args ...string) (int, string, string) {
		t.Helper()
		// Flags go before the arguments: insert the common ones after the verb.
		full := append(append(append([]string{}, args[:2]...), common...), args[2:]...)
		return run(t, nil, full...)
	}
	cases := []struct {
		args []string
		want []string
	}{
		{[]string{"clients", "list", "-search", "gw"}, []string{"clientId=gw/one", "online=true", "node=node"}},
		{[]string{"clients", "list", "-node", "node", "-search", "gw"}, []string{"clientId=gw/one"}},
		{[]string{"clients", "list", "-node", "elsewhere"}, []string{"(none)"}},
		{[]string{"clients", "inspect", "gw/one"}, []string{"session: {clientId=gw/one", "connection: {listener=tcp"}},
		{[]string{"clients", "subscriptions", "gw/one"}, []string{"filter=t/+ qos=1"}},
		{[]string{"clients", "matching", "t/1"}, []string{"clientId=gw/one filter=t/+"}},
		{[]string{"cluster", "status"}, []string{"ready: true", "members: [{name=node role=voter"}},
		{[]string{"cluster", "shards"}, []string{"version:"}},
		{[]string{"cluster", "lease", "gw/one"}, []string{"owner: node"}},
		{[]string{"modules", "list"}, []string{"id=authz.file", "id=store.wal"}},
		{[]string{"modules", "disable", "authz.file"}, []string{"enabled: false"}},
		{[]string{"modules", "enable", "authz.file"}, []string{"enabled: true"}},
		{[]string{"traces", "start", "-limit", "10", "gw/one"}, []string{"limit: 10"}},
		{[]string{"traces", "fetch", "gw/one"}, []string{"(none)"}},
		{[]string{"traces", "stop", "gw/one"}, []string{"(none)"}},
		{[]string{"config", "show"}, []string{`tokenFile=[redacted]`}},
		{[]string{"retained", "list"}, []string{"(none)"}},
		{[]string{"clients", "disconnect", "gw/one"}, nil},
		{[]string{"clients", "invalidate", "other"}, []string{"wasOnline: true", "durable: true"}},
		{[]string{"audit", "tail", "-limit", "50"}, []string{"actor=token:ops action=client.disconnect target=gw/one outcome=ok", "action=session.invalidate target=other"}},
	}
	for _, c := range cases {
		code, out, errOut := runVerb(c.args...)
		if code != ExitOK {
			t.Errorf("%v: exit %d %q", c.args, code, errOut)
			continue
		}
		for _, w := range c.want {
			if !strings.Contains(out, w) {
				t.Errorf("%v: %q missing from:\n%s", c.args, w, out)
			}
		}
	}
	// -json prints the API's document.
	code, out, _ := runVerb("clients", "list", "-json")
	var page struct {
		Items []struct {
			ClientID string `json:"clientId"`
		} `json:"items"`
	}
	if code != ExitOK || json.Unmarshal([]byte(out), &page) != nil || len(page.Items) != 1 || page.Items[0].ClientID != "gw/one" {
		t.Fatalf("-json: %d %q", code, out)
	}
	// A document to validate.
	bad := writeDoc(t, t.TempDir(), "bad.json", `{"version":2}`)
	code, out, _ = runVerb("config", "validate", bad)
	if code != ExitOK || !strings.Contains(out, "valid: false") || !strings.Contains(out, "pointer=/version") {
		t.Fatalf("validate: %d %q", code, out)
	}
	// A reload the node refuses prints why and exits 1: this node was built
	// in code, with no file to re-read.
	code, out, _ = runVerb("config", "reload")
	if code != ExitProblems || !strings.Contains(out, "applied: false") || !strings.Contains(out, "not started from a configuration file") {
		t.Fatalf("reload: %d %q", code, out)
	}
	// API errors exit 1 with the status and the detail.
	code, _, errOut := runVerb("clients", "inspect", "nobody")
	if code != ExitProblems || !strings.Contains(errOut, "404") {
		t.Fatalf("unknown client: %d %q", code, errOut)
	}
	code, _, errOut = run(t, nil, "clients", "list", "-admin", addr)
	if code != ExitProblems || !strings.Contains(errOut, "401") {
		t.Fatalf("no token: %d %q", code, errOut)
	}
	// The token from the environment.
	code, _, errOut = run(t, []string{config.EnvAdminToken + "=" + cliOpsToken, config.EnvAdminAddress + "=" + addr}, "clients", "list")
	if code != ExitOK {
		t.Fatalf("environment token: %d %q", code, errOut)
	}
	// Usage.
	for _, args := range [][]string{{"clients"}, {"clients", "nope"}, {"clients", "inspect"}, {"clients", "inspect", "a", "b"}, {"clients", "list", "-nope"}} {
		if code, _, _ := run(t, nil, args...); code != ExitUsage {
			t.Errorf("%v: exit %d", args, code)
		}
	}
	if code, out, _ := run(t, nil, "clients", "-h"); code != ExitOK || !strings.Contains(out, "invalidate CLIENT-ID") {
		t.Errorf("help: %d %q", code, out)
	}
	if code, _, errOut := run(t, nil, "clients", "list", "-admin", "127.0.0.1:1", "-token-file", tokenFile); code != ExitProblems || errOut == "" {
		t.Errorf("unreachable: %d %q", code, errOut)
	}
}

// The chart's preStop with the admin API on: drain through the API with a
// token file and wait until the drain completes. The observability door is
// closed meanwhile, and a missing token is refused.
func TestDrainThroughTheAdminAPI(t *testing.T) {
	drainPoll = 20 * time.Millisecond
	// The chart's preStop token: the drain and its status, nothing else.
	prestop := writeDoc(t, t.TempDir(), "drain.token", "prestop-drain-token-0123456789ab")
	srv, addr, _ := adminNode(t, func(d *config.Document) {
		d.Observability.Address = "127.0.0.1:0"
		d.Modules["metrics.prometheus"] = json.RawMessage(`{}`)
		d.Admin.Tokens = append(d.Admin.Tokens, config.AdminToken{Name: "prestop-drain", TokenFile: prestop, Scopes: []string{"cluster:drain", "cluster:read"}})
	})
	obs := srv.ObservabilityAddr().String()
	if code, _, errOut := run(t, nil, "drain", "-address", obs); code != ExitProblems || !strings.Contains(errOut, "403") {
		t.Fatalf("observability door: %d %q", code, errOut)
	}
	if code, _, errOut := run(t, nil, "drain", "-admin", addr); code != ExitProblems || !strings.Contains(errOut, "401") {
		t.Fatalf("no token: %d %q", code, errOut)
	}
	if srv.Draining() {
		t.Fatal("a refused drain started one")
	}
	if code, _, errOut := run(t, nil, "clients", "list", "-admin", addr, "-token-file", prestop); code != ExitProblems || !strings.Contains(errOut, "403") {
		t.Fatalf("the preStop token lists clients: %d %q", code, errOut)
	}
	code, out, errOut := run(t, nil, "drain", "-admin", addr, "-token-file", prestop, "-wait", "-timeout", "30s")
	if code != ExitOK || !strings.Contains(out, "drain started") || !strings.Contains(out, "drain complete") {
		t.Fatalf("drain -admin -wait: %d %q %q", code, out, errOut)
	}
	select {
	case <-srv.Drained():
	default:
		t.Fatal("the node's drain is not complete")
	}
}

func TestRender(t *testing.T) {
	var b strings.Builder
	if err := render(&b, []byte(`{"items":[{"clientId":"a","online":true,"n":3}],"next":"a"}`)); err != nil {
		t.Fatal(err)
	}
	if b.String() != "clientId=a online=true n=3\nnext: a\n" {
		t.Fatalf("page %q", b.String())
	}
	b.Reset()
	_ = render(&b, []byte(`{"node":"n1","members":[{"name":"n1"}],"drain":{"draining":false},"x":null}`))
	if b.String() != "node: n1\nmembers: [{name=n1}]\ndrain: {draining=false}\nx: -\n" {
		t.Fatalf("object %q", b.String())
	}
	b.Reset()
	_ = render(&b, []byte(`[]`))
	if b.String() != "(none)\n" {
		t.Fatalf("empty list %q", b.String())
	}
	if err := render(&b, []byte(`{`)); err == nil {
		t.Fatal("a truncated answer rendered")
	}
}

// Over TLS with a client certificate: -cacert makes the scheme https, and
// the certificate's common name is the identity.
func TestAdminVerbsOverMutualTLS(t *testing.T) {
	dir := t.TempDir()
	ca, err := testcert.NewCA("admin-ca")
	if err != nil {
		t.Fatal(err)
	}
	server, _ := ca.Issue("localhost", 2)
	certFile, keyFile, _ := server.Write(dir, "server")
	caFile, _ := ca.WriteCA(dir)
	ops, _ := ca.Issue("ops", 3)
	opsCert, opsKey, _ := ops.Write(dir, "ops")
	stranger, _ := ca.Issue("stranger", 4)
	strangerCert, strangerKey, _ := stranger.Write(dir, "stranger")
	_, addr, _ := adminNode(t, func(d *config.Document) {
		d.Admin.TLS = &config.TLS{CertFile: certFile, KeyFile: keyFile, ClientCAFile: caFile}
		d.Admin.Certificates = []config.AdminCertificate{{CommonName: "ops", Scopes: []string{"read"}}}
	})
	host := strings.Replace(addr, "127.0.0.1", "localhost", 1)
	code, out, errOut := run(t, nil, "cluster", "status", "-admin", host, "-cacert", caFile, "-cert", opsCert, "-key", opsKey)
	if code != ExitOK || !strings.Contains(out, "ready: true") {
		t.Fatalf("mTLS status: %d %q %q", code, out, errOut)
	}
	code, _, errOut = run(t, nil, "clients", "invalidate", "-admin", host, "-cacert", caFile, "-cert", opsCert, "-key", opsKey, "x")
	if code != ExitProblems || !strings.Contains(errOut, "403") {
		t.Fatalf("a read-scoped certificate's mutation: %d %q", code, errOut)
	}
	code, _, errOut = run(t, nil, "cluster", "status", "-admin", host, "-cacert", caFile, "-cert", strangerCert, "-key", strangerKey)
	if code != ExitProblems || !strings.Contains(errOut, "401") {
		t.Fatalf("an unmapped certificate: %d %q", code, errOut)
	}
	// Plain HTTP against the TLS listener fails.
	if code, _, _ := run(t, nil, "cluster", "status", "-admin", host, "-token-file", writeDoc(t, dir, "t", cliOpsToken)); code != ExitProblems {
		t.Fatalf("plain HTTP against TLS: %d", code)
	}
}

// The credentials verbs: the password comes from a file and goes only into
// the request body; the answers never show it or its hash; both files at
// once is a usage error.
func TestCredentialsVerbs(t *testing.T) {
	srv, addr, tokenFile := adminNode(t, func(d *config.Document) {
		d.Modules["auth.db"] = json.RawMessage(`{}`)
		d.Auth.Anonymous = false
	})
	connectAs := func(user, pw string) packet.ReasonCode {
		t.Helper()
		c, err := net.Dial("tcp", srv.Addr("tcp").String())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close() }()
		enc := &packet.Encoder{Version: packet.V5}
		dec := packet.NewDecoder(c)
		dec.Version = packet.V5
		if err := enc.Encode(c, &packet.Connect{ProtocolLevel: packet.V5, ClientID: "c-" + user, CleanStart: true, HasUsername: true, Username: user, HasPassword: true, Password: []byte(pw)}); err != nil {
			t.Fatal(err)
		}
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
		p, err := dec.Read()
		ack, ok := p.(*packet.Connack)
		if err != nil || !ok {
			t.Fatalf("connect as %s: %v %v", user, p, err)
		}
		return ack.ReasonCode
	}
	dir := t.TempDir()
	pw := writeDoc(t, dir, "pw", "s3cret-cli\n")
	hash := writeDoc(t, dir, "hash", "$pbkdf2-sha256$i=1000$c2FsdHNhbHRzYWx0$aGFzaA\n")
	common := []string{"-admin", addr, "-token-file", tokenFile}
	runVerb := func(args ...string) (int, string, string) {
		t.Helper()
		full := append(append(append([]string{}, args[:2]...), common...), args[2:]...)
		return run(t, nil, full...)
	}
	code, out, errOut := runVerb("credentials", "add", "-password-file", pw, "-attribute", "team=blue", "alice")
	if code != ExitOK || !strings.Contains(out, "form: pbkdf2-sha256") || !strings.Contains(out, "team=blue") {
		t.Fatalf("add: %d %q %q", code, out, errOut)
	}
	if got := connectAs("alice", "s3cret-cli"); got != packet.Success {
		t.Fatalf("alice with the file's password: %s", got)
	}
	code, out, errOut = runVerb("credentials", "update", "-hash-file", hash, "alice")
	if code != ExitOK || !strings.Contains(out, "form: pbkdf2-sha256") {
		t.Fatalf("update with a hash: %d %q %q", code, out, errOut)
	}
	code, out, errOut = runVerb("credentials", "update", "-password-file", pw, "-scram", "alice")
	if code != ExitOK || !strings.Contains(out, "form: scram-sha-256") {
		t.Fatalf("update: %d %q %q", code, out, errOut)
	}
	code, out, _ = runVerb("credentials", "list")
	if code != ExitOK || !strings.Contains(out, "username=alice") || strings.Contains(out, "s3cret-cli") || strings.Contains(out, "$scram") {
		t.Fatalf("list: %d %q", code, out)
	}
	if code, out, _ := runVerb("credentials", "show", "alice"); code != ExitOK || !strings.Contains(out, "username: alice") {
		t.Fatalf("show: %d %q", code, out)
	}
	if code, _, errOut := runVerb("credentials", "add", "-password-file", pw, "-hash-file", hash, "bob"); code != ExitUsage {
		t.Fatalf("both files: %d %q", code, errOut)
	}
	if code, _, errOut := runVerb("credentials", "add", "bob"); code != ExitUsage {
		t.Fatalf("no password: %d %q", code, errOut)
	}
	if code, _, errOut := runVerb("credentials", "remove", "-if-version", "1", "alice"); code != ExitProblems || !strings.Contains(errOut, "409") {
		t.Fatalf("remove at a stale version: %d %q", code, errOut)
	}
	if code, _, errOut := runVerb("credentials", "remove", "alice"); code != ExitOK {
		t.Fatalf("remove: %d %q", code, errOut)
	}
	if code, _, errOut := runVerb("credentials", "show", "alice"); code != ExitProblems || !strings.Contains(errOut, "404") {
		t.Fatalf("show after remove: %d %q", code, errOut)
	}
}

// The ban and limit verbs, against a node: a ban added by the CLI refuses a
// matching CONNECT, lists and is removed; limits set by the CLI are in force.
func TestBansAndLimitsVerbs(t *testing.T) {
	srv, addr, tokenFile := adminNode(t, nil)
	common := []string{"-admin", addr, "-token-file", tokenFile}
	runVerb := func(args ...string) (int, string, string) {
		t.Helper()
		full := append(append(append([]string{}, args[:2]...), common...), args[2:]...)
		return run(t, nil, full...)
	}
	code, out, errOut := runVerb("bans", "add", "-reason", "testing", "-expires", "10m", "clientid", "cli-banned")
	if code != ExitOK || !strings.Contains(out, "cli-banned") || !strings.Contains(out, "expiresAt") || !strings.Contains(out, "reason: testing") {
		t.Fatalf("add: %d %q %q", code, out, errOut)
	}
	connect := func(id string) packet.ReasonCode {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for {
			c, err := net.Dial("tcp", srv.Addr("tcp").String())
			if err != nil {
				t.Fatal(err)
			}
			enc := &packet.Encoder{Version: packet.V5}
			dec := packet.NewDecoder(c)
			dec.Version = packet.V5
			_ = enc.Encode(c, &packet.Connect{ProtocolLevel: packet.V5, ClientID: id, CleanStart: true})
			_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
			p, err := dec.Read()
			_ = c.Close()
			if err != nil {
				t.Fatal(err)
			}
			code := p.(*packet.Connack).ReasonCode
			if code == packet.Banned || time.Now().After(deadline) {
				return code
			}
			time.Sleep(20 * time.Millisecond) // the node's index follows the write
		}
	}
	if got := connect("cli-banned"); got != packet.Banned {
		t.Fatalf("a banned client: %s", got)
	}
	if code, out, _ := runVerb("bans", "list"); code != ExitOK || !strings.Contains(out, "clientid:cli-banned") {
		t.Fatalf("list: %d %q", code, out)
	}
	if code, _, errOut := runVerb("bans", "remove", "clientid", "cli-banned"); code != ExitOK {
		t.Fatalf("remove: %d %q", code, errOut)
	}
	if code, _, errOut := runVerb("bans", "add", "host", "x"); code != ExitProblems || !strings.Contains(errOut, "400") {
		t.Fatalf("a bad kind: %d %q", code, errOut)
	}
	code, out, errOut = runVerb("limits", "set", "-client-message-rate", "7", "-connect-rate", "900")
	if code != ExitOK {
		t.Fatalf("set: %d %q %q", code, out, errOut)
	}
	if code, out, _ := runVerb("limits", "show"); code != ExitOK || !strings.Contains(out, "inForce: {clientMessageRate=7 ") || !strings.Contains(out, "connectRate=900}") {
		t.Fatalf("show: %d %q", code, out)
	}
}
