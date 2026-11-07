package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/modlicense"
	"github.com/happydave/hoardmq/internal/packet"
)

// sinkPostgresModule is contrib/_sinkpostgres's module path.
const sinkPostgresModule = "github.com/happydave/hoardmq-sinkpostgres"

// buildContribs builds a binary carrying both contrib modules.
func buildContribs(t *testing.T) string {
	t.Helper()
	root, authsql := copyContrib(t, "_authsql")
	_, sink := copyContrib(t, "_sinkpostgres")
	bin := filepath.Join(t.TempDir(), "hoardmq")
	var out, errOut bytes.Buffer
	if code := run([]string{"--hoardmq-dir", root, "--with", authSQLModule + "=" + authsql, "--with", sinkPostgresModule + "=" + sink, "--output", bin}, &out, &errOut); code != exitOK {
		t.Fatalf("hoardmq-build exit %d:\n%s", code, errOut.String())
	}
	return bin
}

// TestSinkPostgresModuleTests: the module's own vet and unit tests pass in
// its copy, with no database.
func TestSinkPostgresModuleTests(t *testing.T) {
	_, dir := copyContrib(t, "_sinkpostgres")
	goCmd(t, dir, "vet", "./...")
	out := goCmd(t, dir, "test", "-count=1", "./...")
	if !strings.Contains(out, "ok  \t"+sinkPostgresModule) {
		t.Fatalf("go test:\n%s", out)
	}
}

// TestSinkPostgresBinary: a binary carries sink.postgres beside auth.sql;
// check validates its configuration with no database, and refuses a password
// in the connection string.
func TestSinkPostgresBinary(t *testing.T) {
	bin := buildContribs(t)
	dir := t.TempDir()
	out, err := command(bin, "version").CombinedOutput()
	if err != nil || !strings.Contains(string(out), " sink.postgres") || !strings.Contains(string(out), " auth.sql") {
		t.Fatalf("version: %v\n%s", err, out)
	}
	doc := func(conn string) map[string]any {
		return map[string]any{
			"version": 1, "node": map[string]any{"dataDir": filepath.Join(dir, "data")},
			"listeners": []any{map[string]any{"name": "t", "type": "tcp", "address": "127.0.0.1:0"}},
			"modules": map[string]any{"sink.postgres": map[string]any{
				"connString": conn, "routes": []any{map[string]any{"filter": "s/#", "table": "readings"}}}},
		}
	}
	good, bad := filepath.Join(dir, "good.json"), filepath.Join(dir, "bad.json")
	writeJSON(t, good, doc("postgres://hoardmq@127.0.0.1:1/mqtt?sslmode=disable"))
	writeJSON(t, bad, doc("postgres://hoardmq:hunter2@127.0.0.1:1/mqtt"))
	if out, err := command(bin, "check", "-config", good).CombinedOutput(); err != nil {
		t.Fatalf("check of the good configuration: %v\n%s", err, out)
	}
	out, err = command(bin, "check", "-config", bad).CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 1 || !strings.Contains(string(out), "/modules/sink.postgres") ||
		!strings.Contains(string(out), "connString holds a password") || strings.Contains(string(out), "hunter2") {
		t.Fatalf("check of the bad configuration: %v\n%s", err, out)
	}
}

// TestSinkPostgresLicences: pgx is in the module's build, within the policy.
// The broker's side is TestAuthSQLLicences's.
func TestSinkPostgresLicences(t *testing.T) {
	_, dir := copyContrib(t, "_sinkpostgres")
	t.Setenv("GOFLAGS", "")
	mods, err := modlicense.ListBuild(context.Background(), dir, "./...")
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

// psqlOut runs SQL in the container and returns its unaligned output.
func (l live) psqlOut(t *testing.T, sql string) string {
	t.Helper()
	cmd := exec.Command("podman", "exec", "-i", l.container, "psql", "-U", "postgres", "-v", "ON_ERROR_STOP=1", "-tA", "-q")
	cmd.Stdin = strings.NewReader(sql)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("psql: %v\n%s", err, out)
	}
	return strings.TrimSpace(string(out))
}

func publishQoS1(t *testing.T, c *client, topic string, id uint16, payload string) packet.ReasonCode {
	t.Helper()
	_ = c.conn.SetDeadline(time.Now().Add(10 * time.Second))
	defer func() { _ = c.conn.SetDeadline(time.Time{}) }()
	if err := c.enc.Encode(c.conn, &packet.Publish{QoS: 1, Topic: topic, PacketID: id, Payload: []byte(payload)}); err != nil {
		t.Fatal(err)
	}
	p, err := c.dec.Read()
	if err != nil {
		t.Fatal(err)
	}
	ack, ok := p.(*packet.Puback)
	if !ok || ack.PacketID != id {
		t.Fatalf("publish %s answered %#v", topic, p)
	}
	return ack.ReasonCode
}

func metricValue(metrics, series string) float64 {
	for _, line := range strings.Split(metrics, "\n") {
		if strings.HasPrefix(line, series+" ") {
			v, _ := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, series)), 64)
			return v
		}
	}
	return 0
}

// TestSinkPostgresLive runs a binary carrying sink.postgres against a real
// Postgres: the mapping, refuse and drop with the database paused, a table
// that does not exist, and the stop's count.
func TestSinkPostgresLive(t *testing.T) {
	l := liveEnv(t)
	bin := buildContribs(t)
	dir := t.TempDir()
	t.Setenv("HOARDMQ_SIM_LOG", filepath.Join(dir, "sim.log"))
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	readings, alerts := "readings_"+suffix, "alerts_"+suffix
	for _, tbl := range []string{readings, alerts} {
		l.psql(t, fmt.Sprintf(`CREATE TABLE %s (topic text NOT NULL, payload bytea NOT NULL, qos smallint NOT NULL, retain boolean NOT NULL,
  client_id text, namespace text NOT NULL, source text NOT NULL, accepted_at timestamptz NOT NULL);`, tbl))
		t.Cleanup(func() { l.psql(t, "DROP TABLE IF EXISTS "+tbl+";") })
	}
	pwFile := filepath.Join(dir, "db.pw")
	if err := os.WriteFile(pwFile, []byte(l.password+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	acl := filepath.Join(dir, "acl.json")
	if err := os.WriteFile(acl, []byte(`{"rules":[{"topic":"#","allow":["publish","subscribe","unsubscribe"]}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	obs := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	cfg := func(name, whenFull string, routes []any) string {
		p := filepath.Join(dir, name+".json")
		writeJSON(t, p, map[string]any{
			"version": 1, "node": map[string]any{"dataDir": filepath.Join(dir, name)},
			"listeners":     []any{map[string]any{"name": "t", "type": "tcp", "address": "127.0.0.1:0"}},
			"auth":          map[string]any{"anonymous": true},
			"observability": map[string]any{"address": obs},
			"modules": map[string]any{
				"sink.postgres": map[string]any{"connString": l.url, "passwordFile": pwFile, "routes": routes, "timeoutMs": 1000,
					"flushTimeoutMs": 2000, "batch": map[string]any{"lingerMs": 5},
					"buffer": map[string]any{"maxRecords": 20, "whenFull": whenFull, "waitMs": 100}},
				"authz.file": map[string]any{"path": acl}, "metrics.prometheus": map[string]any{}},
			"log": map[string]any{"format": "json"},
		})
		return p
	}
	routes := []any{map[string]any{"filter": "s/#", "table": readings}, map[string]any{"filter": "s/a/#", "table": alerts},
		map[string]any{"filter": "gone/#", "table": "missing_" + suffix}}

	n := serve(t, bin, cfg("refuse", "refuse", routes))
	c := connect(t, n.addr, "pub")
	// A publisher faster than the writer meets the 20-row buffer: a 0x97 is the
	// policy's back-pressure, and the publish is sent again under a new
	// identifier until it is accepted.
	id := uint16(0)
	for i := range 200 {
		topic := "s/x"
		if i%4 == 0 {
			topic = "s/a/y"
		}
		for {
			id++
			code := publishQoS1(t, c, topic, id, "p"+strconv.Itoa(i))
			if code == packet.QuotaExceeded {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			if code.IsError() {
				t.Fatalf("publish %d: %s", i, code)
			}
			break
		}
	}
	waitRows := func(tbl string, want int) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for {
			got, _ := strconv.Atoi(l.psqlOut(t, "SELECT count(*) FROM "+tbl+";"))
			if got == want {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s holds %d rows, want %d", tbl, got, want)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	waitRows(readings, 200)
	waitRows(alerts, 50)
	row := l.psqlOut(t, "SELECT topic, encode(payload,'escape'), qos, retain, client_id, namespace, source, accepted_at > now() - interval '5 minutes' FROM "+alerts+" WHERE payload = 'p4';")
	if row != "s/a/y|p4|1|f|pub|default|client|t" {
		t.Fatalf("the row: %q", row)
	}

	// A table that does not exist: its rows are rejected, counted.
	id++
	publishQoS1(t, c, "gone/1", id, "x")
	deadline := time.Now().Add(10 * time.Second)
	for metricValue(scrape(t, obs), `hoardmq_sink_postgres_dropped_total{reason="rejected",table="missing_`+suffix+`"}`) != 1 {
		if time.Now().After(deadline) {
			t.Fatal("the missing table's row was not counted rejected")
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Refuse: paused, the buffer fills to 20, then QoS 1 is refused 0x97; on
	// unpause the 20 land.
	if out, err := exec.Command("podman", "pause", l.container).CombinedOutput(); err != nil {
		t.Fatalf("pause: %v\n%s", err, out)
	}
	t.Cleanup(func() { _ = exec.Command("podman", "unpause", l.container).Run() })
	accepted, refused := 0, 0
	for i := range 30 {
		id++
		switch code := publishQoS1(t, c, "s/x", id, "r"+strconv.Itoa(i)); code {
		case packet.QuotaExceeded:
			refused++
		default:
			if code.IsError() {
				t.Fatalf("paused publish %d: %s", i, code)
			}
			accepted++
		}
	}
	if accepted != 20 || refused != 10 {
		t.Fatalf("paused: %d accepted, %d refused; want 20 and 10", accepted, refused)
	}
	if out, err := exec.Command("podman", "unpause", l.container).CombinedOutput(); err != nil {
		t.Fatalf("unpause: %v\n%s", err, out)
	}
	waitRows(readings, 220)
	_ = c.conn.Close()
	if err := syscall.Kill(n.pid, syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	<-n.exited

	// Drop and the stop: under drop, paused, publishes past 20 are accepted
	// and counted buffer_full; SIGTERM counts the 20 shutdown.
	d := serve(t, bin, cfg("drop", "drop", routes[:1]))
	dc := connect(t, d.addr, "pub2")
	if out, err := exec.Command("podman", "pause", l.container).CombinedOutput(); err != nil {
		t.Fatalf("pause: %v\n%s", err, out)
	}
	for i := range 30 {
		if code := publishQoS1(t, dc, "s/x", uint16(i+1), "d"+strconv.Itoa(i)); code.IsError() {
			t.Fatalf("drop policy publish %d: %s", i, code)
		}
	}
	m := scrape(t, obs)
	if got := metricValue(m, `hoardmq_sink_postgres_dropped_total{reason="buffer_full",table="`+readings+`"}`); got != 10 {
		t.Fatalf("buffer_full %v, want 10:\n%s", got, m)
	}
	_ = dc.conn.Close()
	start := time.Now()
	if err := syscall.Kill(d.pid, syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	<-d.exited
	if el := time.Since(start); el > 2*time.Second+5*time.Second {
		t.Fatalf("the stop took %v", el)
	}
	if !strings.Contains(d.log.String(), `"reason":"shutdown"`) || !strings.Contains(d.log.String(), `"records":20`) {
		t.Fatalf("no shutdown count of 20 in the log:\n%s", d.log)
	}
	if out, err := exec.Command("podman", "unpause", l.container).CombinedOutput(); err != nil {
		t.Fatalf("unpause: %v\n%s", err, out)
	}
}
