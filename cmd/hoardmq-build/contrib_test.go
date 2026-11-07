package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/modlicense"
	"github.com/happydave/hoardmq/internal/packet"
)

// authSQLModule is contrib/_authsql's module path.
const authSQLModule = "github.com/happydave/hoardmq-authsql"

// copyAuthSQL copies contrib/_authsql out of the checkout.
func copyAuthSQL(t *testing.T) (root, dir string) {
	t.Helper()
	return copyContrib(t, "_authsql")
}

// copyContrib copies contrib/<name> out of the checkout as a module of its
// own: its module and checksum files renamed, and the broker replaced by the
// checkout so its tests build against this tree.
func copyContrib(t *testing.T, name string) (root, dir string) {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	src := filepath.Join(root, "contrib", name)
	dir = filepath.Join(t.TempDir(), strings.TrimPrefix(name, "_"))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		data, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		name := strings.TrimSuffix(e.Name(), ".txt")
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	goCmd(t, dir, "mod", "edit", "-replace", "github.com/happydave/hoardmq="+root)
	return root, dir
}

// goCmd runs the go command in dir and returns its output, failing the test
// on an error.
func goCmd(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("go", args...)
	cmd.Dir = dir
	cmd.Env = append(binaryEnv(), "GOWORK=off", "GOFLAGS=")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go %s in %s: %v\n%s", strings.Join(args, " "), dir, err, out)
	}
	return string(out)
}

// buildAuthSQL builds a binary carrying auth.sql from the checkout.
func buildAuthSQL(t *testing.T) string {
	t.Helper()
	root, dir := copyAuthSQL(t)
	bin := filepath.Join(t.TempDir(), "hoardmq")
	var out, errOut bytes.Buffer
	if code := run([]string{"--hoardmq-dir", root, "--with", authSQLModule + "=" + dir, "--output", bin}, &out, &errOut); code != exitOK {
		t.Fatalf("hoardmq-build exit %d:\n%s", code, errOut.String())
	}
	return bin
}

// TestAuthSQLModuleTests: the module's own vet and unit tests pass in its
// copy, with no database.
func TestAuthSQLModuleTests(t *testing.T) {
	_, dir := copyAuthSQL(t)
	goCmd(t, dir, "vet", "./...")
	out := goCmd(t, dir, "test", "-count=1", "./...")
	if !strings.Contains(out, "ok  \t"+authSQLModule) {
		t.Fatalf("go test:\n%s", out)
	}
}

// TestAuthSQLBinary: hoardmq-build builds a binary carrying auth.sql; check
// validates its configuration with no database, and refuses a password in
// the connection string.
func TestAuthSQLBinary(t *testing.T) {
	bin := buildAuthSQL(t)
	dir := t.TempDir()
	out, err := command(bin, "version").CombinedOutput()
	if err != nil || !strings.Contains(string(out), " auth.sql") {
		t.Fatalf("version: %v\n%s", err, out)
	}
	doc := func(conn string) map[string]any {
		return map[string]any{
			"version": 1, "node": map[string]any{"dataDir": filepath.Join(dir, "data")},
			"listeners": []any{map[string]any{"name": "t", "type": "tcp", "address": "127.0.0.1:0"}},
			"modules": map[string]any{"auth.sql": map[string]any{
				"connString": conn, "query": "SELECT password_hash FROM mqtt_users WHERE username = ${username}"}},
		}
	}
	good, bad := filepath.Join(dir, "good.json"), filepath.Join(dir, "bad.json")
	// Nothing listens on port 1: check connects to nothing.
	writeJSON(t, good, doc("postgres://hoardmq@127.0.0.1:1/mqtt?sslmode=disable"))
	writeJSON(t, bad, doc("postgres://hoardmq:hunter2@127.0.0.1:1/mqtt"))
	if out, err := command(bin, "check", "-config", good).CombinedOutput(); err != nil {
		t.Fatalf("check of the good configuration: %v\n%s", err, out)
	}
	out, err = command(bin, "check", "-config", bad).CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 1 || !strings.Contains(string(out), "/modules/auth.sql") ||
		!strings.Contains(string(out), "connString holds a password") || strings.Contains(string(out), "hunter2") {
		t.Fatalf("check of the bad configuration: %v\n%s", err, out)
	}
}

// TestAuthSQLLicences: pgx is in the module's build, within the licence
// policy, and in neither the broker's module nor the default binary.
func TestAuthSQLLicences(t *testing.T) {
	root, dir := copyAuthSQL(t)
	ctx := context.Background()
	t.Setenv("GOFLAGS", "")
	broker, err := modlicense.List(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	binary, err := modlicense.ListBuild(ctx, root, "./cmd/hoardmq")
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range append(broker, binary...) {
		if strings.HasPrefix(m.Path, "github.com/jackc/") {
			t.Errorf("the broker's module or default binary carries %s %s", m.Path, m.Version)
		}
	}
	mods, err := modlicense.ListBuild(ctx, dir, "./...")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, m := range mods {
		found = found || m.Path == "github.com/jackc/pgx/v5"
	}
	if !found {
		t.Fatalf("the module's build does not carry pgx: %v", mods)
	}
	if v := modlicense.Violations(modlicense.Classify(mods)); len(v) != 0 {
		t.Fatalf("outside the licence policy: %+v", v)
	}
}

// live is the live test's environment: a Postgres the test may write to, and
// the podman container it runs in.
type live struct {
	url, password, container string
}

func liveEnv(t *testing.T) live {
	l := live{os.Getenv("AUTHSQL_LIVE_URL"), os.Getenv("AUTHSQL_LIVE_PASSWORD"), os.Getenv("AUTHSQL_LIVE_CONTAINER")}
	if l.url == "" || l.container == "" {
		t.Skip("AUTHSQL_LIVE_URL and AUTHSQL_LIVE_CONTAINER are not set")
	}
	return l
}

// psql runs SQL in the container as the postgres user.
func (l live) psql(t *testing.T, sql string) {
	t.Helper()
	cmd := exec.Command("podman", "exec", "-i", l.container, "psql", "-U", "postgres", "-v", "ON_ERROR_STOP=1", "-q")
	cmd.Stdin = strings.NewReader(sql)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("psql: %v\n%s", err, out)
	}
}

func hashPassword(t *testing.T, bin, password string, args ...string) string {
	t.Helper()
	cmd := command(bin, append([]string{"hash-password"}, args...)...)
	cmd.Stdin = strings.NewReader(password + "\n")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("hash-password %v: %v", args, err)
	}
	return strings.TrimSpace(string(out))
}

// node is a running serve.
type node struct {
	addr   string
	log    *lineWatcher
	pid    int
	exited chan struct{}
}

func serve(t *testing.T, bin, config string) *node {
	t.Helper()
	cmd := command(bin, "serve", "-config", config)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdout = io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	n := &node{pid: cmd.Process.Pid, log: &lineWatcher{addr: make(chan string, 1)}, exited: make(chan struct{})}
	readDone := make(chan struct{})
	go func() { n.log.read(stderr); close(readDone) }()
	go func() { <-readDone; _ = cmd.Wait(); close(n.exited) }()
	t.Cleanup(func() {
		select {
		case <-n.exited:
		default:
			_ = syscall.Kill(n.pid, syscall.SIGKILL)
			<-n.exited
		}
	})
	select {
	case n.addr = <-n.log.addr:
	case <-n.exited:
		t.Fatalf("serve exited:\n%s", n.log)
	case <-time.After(60 * time.Second):
		t.Fatalf("no listening line:\n%s", n.log)
	}
	return n
}

// connectAs sends a CONNECT with a username and password and returns the
// connection and its CONNACK's code.
func connectAs(t *testing.T, addr, id, user, password string) (*client, packet.ReasonCode) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		c, code, err := tryConnectAs(addr, id, user, password)
		if err == nil {
			return c, code
		}
		if time.Now().After(deadline) {
			t.Fatalf("connect %s: %v", id, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func tryConnectAs(addr, id, user, password string) (*client, packet.ReasonCode, error) {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, 0, err
	}
	c := &client{conn: conn, enc: packet.Encoder{Version: packet.V5}, dec: packet.NewDecoder(conn)}
	c.dec.Version = packet.V5
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	if err := c.enc.Encode(conn, &packet.Connect{ProtocolName: "MQTT", ProtocolLevel: packet.V5, CleanStart: true, KeepAlive: 30,
		ClientID: id, HasUsername: true, Username: user, HasPassword: true, Password: []byte(password)}); err != nil {
		_ = conn.Close()
		return nil, 0, err
	}
	p, err := c.dec.Read()
	if err != nil {
		_ = conn.Close()
		return nil, 0, err
	}
	ack, ok := p.(*packet.Connack)
	if !ok {
		_ = conn.Close()
		return nil, 0, fmt.Errorf("answered %#v", p)
	}
	// A node not yet serving refuses with Server unavailable: try again.
	if ack.ReasonCode == packet.ServerUnavailable {
		_ = conn.Close()
		return nil, 0, errors.New("not serving yet")
	}
	_ = conn.SetDeadline(time.Time{})
	return c, ack.ReasonCode, nil
}

func scrape(t *testing.T, addr string) string {
	t.Helper()
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	return string(body)
}

// TestAuthSQLLive runs a binary carrying auth.sql against a real Postgres:
// both hash forms hoardmq hash-password writes, a wrong password, an unknown
// user, an injection attempt, two rows, the re-check after a hash changes,
// the database paused, and a wrong database password kept out of the log.
func TestAuthSQLLive(t *testing.T) {
	l := liveEnv(t)
	bin := buildAuthSQL(t)
	dir := t.TempDir()
	t.Setenv("HOARDMQ_SIM_LOG", filepath.Join(dir, "sim.log"))

	table := fmt.Sprintf("mqtt_users_%d", time.Now().UnixNano())
	pbkdf2 := hashPassword(t, bin, "alice-pw")
	scram := hashPassword(t, bin, "bob-pw", "-scram")
	other := hashPassword(t, bin, "rotated-pw")
	if !strings.HasPrefix(pbkdf2, "$pbkdf2-sha256$") || !strings.HasPrefix(scram, "$scram-sha-256$") {
		t.Fatalf("hashes %q %q", pbkdf2, scram)
	}
	l.psql(t, fmt.Sprintf(`CREATE TABLE %[1]s (username text, password_hash text, tenant text);
INSERT INTO %[1]s VALUES ('alice', '%[2]s', 'blue'), ('bob', '%[3]s', NULL), ('carol', '%[2]s', 'red'), ('carol', '%[2]s', 'red');`, table, pbkdf2, scram))
	t.Cleanup(func() { l.psql(t, "DROP TABLE "+table+";") })

	pwFile := filepath.Join(dir, "db.pw")
	if err := os.WriteFile(pwFile, []byte(l.password+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	acl := filepath.Join(dir, "acl.json")
	if err := os.WriteFile(acl, []byte(`{"rules":[{"topic":"#","allow":["publish","subscribe","unsubscribe"]}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	obs := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	doc := func(data, pw string) map[string]any {
		return map[string]any{
			"version": 1, "node": map[string]any{"dataDir": filepath.Join(dir, data)},
			"listeners":     []any{map[string]any{"name": "t", "type": "tcp", "address": "127.0.0.1:0"}},
			"auth":          map[string]any{"decisionTtlSeconds": 2},
			"observability": map[string]any{"address": obs},
			"modules": map[string]any{
				"auth.sql": map[string]any{"connString": l.url, "passwordFile": pw, "timeoutMs": 500,
					"query": "SELECT password_hash, tenant FROM " + table + " WHERE username = ${username}"},
				"authz.file": map[string]any{"path": acl}, "metrics.prometheus": map[string]any{}},
			"log": map[string]any{"format": "json"},
		}
	}
	cfg := filepath.Join(dir, "node.json")
	writeJSON(t, cfg, doc("data", pwFile))
	n := serve(t, bin, cfg)

	for _, c := range []struct {
		user, password string
		want           packet.ReasonCode
	}{
		{"alice", "alice-pw", packet.Success},
		{"bob", "bob-pw", packet.Success},
		{"alice", "wrong", packet.BadUserNameOrPassword},
		{"nobody", "alice-pw", packet.BadUserNameOrPassword},
		{"x' OR 1=1 --", "alice-pw", packet.BadUserNameOrPassword},
		{"carol", "alice-pw", packet.BadUserNameOrPassword},
	} {
		cl, code := connectAs(t, n.addr, "c-"+strings.Fields(c.user)[0], c.user, c.password)
		_ = cl.conn.Close()
		if code != c.want {
			t.Errorf("%q with %q: %s, want %s", c.user, c.password, code, c.want)
		}
	}

	// The re-check: a changed hash ends the session at the decision TTL.
	alice, code := connectAs(t, n.addr, "alice-1", "alice", "alice-pw")
	if code != packet.Success {
		t.Fatalf("alice: %s", code)
	}
	l.psql(t, fmt.Sprintf(`UPDATE %s SET password_hash = '%s' WHERE username = 'alice';`, table, other))
	_ = alice.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	p, err := alice.dec.Read()
	if d, ok := p.(*packet.Disconnect); !ok || d.ReasonCode != packet.NotAuthorized {
		t.Fatalf("after the hash changed: %#v %v", p, err)
	}
	_ = alice.conn.Close()

	// Unavailable: a paused database denies within the timeout, counted.
	if out, err := exec.Command("podman", "pause", l.container).CombinedOutput(); err != nil {
		t.Fatalf("pause: %v\n%s", err, out)
	}
	t.Cleanup(func() { _ = exec.Command("podman", "unpause", l.container).Run() })
	start := time.Now()
	cl, code := connectAs(t, n.addr, "bob-2", "bob", "bob-pw")
	_ = cl.conn.Close()
	if el := time.Since(start); code != packet.BadUserNameOrPassword || el > 2*time.Second {
		t.Fatalf("with the database paused: %s after %v", code, el)
	}
	if m := scrape(t, obs); !strings.Contains(m, `hoardmq_auth_sql_unavailable_total{policy="deny"}`) {
		t.Fatalf("no unavailable count:\n%s", m)
	}
	if out, err := exec.Command("podman", "unpause", l.container).CombinedOutput(); err != nil {
		t.Fatalf("unpause: %v\n%s", err, out)
	}
	if err := syscall.Kill(n.pid, syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	<-n.exited

	// A wrong database password is refused by the database, and the Warn that
	// reports it does not carry it.
	wrongFile := filepath.Join(dir, "wrong.pw")
	if err := os.WriteFile(wrongFile, []byte("wrong-db-password-value\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	wrongCfg := filepath.Join(dir, "wrong.json")
	writeJSON(t, wrongCfg, doc("data-wrong", wrongFile))
	w := serve(t, bin, wrongCfg)
	cl, code = connectAs(t, w.addr, "bob-3", "bob", "bob-pw")
	_ = cl.conn.Close()
	logs := w.log.String()
	if code != packet.BadUserNameOrPassword || !strings.Contains(logs, "auth.sql database unavailable") {
		t.Fatalf("with a wrong database password: %s\n%s", code, logs)
	}
	if strings.Contains(logs, "wrong-db-password-value") || strings.Contains(logs, l.password) && l.password != "" {
		t.Fatalf("a database password in the log:\n%s", logs)
	}
	t.Logf("the refused connection's Warn: %s", warnLine(logs))
}

func warnLine(logs string) string {
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, "auth.sql database unavailable") {
			return line
		}
	}
	return ""
}
