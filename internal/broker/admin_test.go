package broker

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/admin"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

const (
	adminOps    = "broker-test-ops-token-0123456789"
	adminReader = "broker-test-reader-token-012345"
)

// withAdmin configures the admin API on an ephemeral loopback port with an
// ops token (every scope) and a reader token (read), in files under dir.
func withAdmin(t *testing.T, dir string) func(*config.Document) {
	t.Helper()
	write := func(name, value string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	ops, reader := write("ops.token", adminOps), write("reader.token", adminReader)
	return func(d *config.Document) {
		d.Admin.Address = "127.0.0.1:0"
		d.Admin.Tokens = []config.AdminToken{{Name: "ops", TokenFile: ops, Scopes: []string{"admin"}}, {Name: "reader", TokenFile: reader, Scopes: []string{"read"}}}
	}
}

// api calls a node's admin API.
func api(t *testing.T, f *fixture, method, path, token, body string) (int, []byte) {
	t.Helper()
	code, data, _ := apiH(t, f, method, path, token, body)
	return code, data
}

// apiH is api with the answer's headers.
func apiH(t *testing.T, f *fixture, method, path, token, body string) (int, []byte, http.Header) {
	t.Helper()
	addr := f.srv.AdminAddr()
	if addr == nil {
		t.Fatal("no admin listener")
	}
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, "http://"+addr.String()+path, r)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, data, resp.Header
}

func decode[T any](t *testing.T, data []byte) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(data, &v); err != nil {
		t.Fatalf("decode %s: %v", data, err)
	}
	return v
}

func adminWait(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// auditHas reports whether the node's audit tail holds an event matching
// every given field.
func auditHas(t *testing.T, f *fixture, actor, action, target, outcome string) bool {
	t.Helper()
	code, data := api(t, f, "GET", "/api/v1/audit?limit=1000", adminOps, "")
	if code != 200 {
		t.Fatalf("audit tail %d %s", code, data)
	}
	for _, e := range decode[[]admin.AuditEvent](t, data) {
		if e.Actor == actor && e.Action == action && e.Target == target && e.Outcome == outcome {
			return true
		}
	}
	return false
}

func TestAdminOffByDefault(t *testing.T) {
	f := start(t, nil)
	if f.srv.AdminAddr() != nil || f.srv.adminAPI != nil {
		t.Fatal("an admin listener without admin.address")
	}
}

// One node, every resource group, against a live broker.
func TestAdminAPISingleNode(t *testing.T) {
	f := start(t, func(d *config.Document) {
		withAdmin(t, t.TempDir())(d)
		d.Observability.Address = "127.0.0.1:0"
		d.Modules["metrics.prometheus"] = json.RawMessage(`{}`)
	})
	if code, _ := api(t, f, "GET", "/api/v1/clients", "", ""); code != 401 {
		t.Fatalf("no token: %d", code)
	}
	exp := uint32(600)
	watcher, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "watcher", CleanStart: true}, false)
	watcher.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "will/#", QoS: 0}}})
	watcher.read()
	sub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "gw/sub-1", CleanStart: true, Will: &packet.Will{Topic: "will/sub", Payload: []byte("sub-gone")},
		Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	sub.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "t/+", QoS: 1}}})
	sub.read()
	pub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)

	// Search, inspect, subscriptions.
	code, data := api(t, f, "GET", "/api/v1/clients?search=sub", adminReader, "")
	page := decode[admin.ClientPage](t, data)
	if code != 200 || len(page.Items) != 1 || page.Items[0].ClientID != "gw/sub-1" || !page.Items[0].Online ||
		page.Items[0].Session == nil || page.Items[0].Session.Subscriptions != 1 || page.Items[0].Node == "" {
		t.Fatalf("search: %d %s", code, data)
	}
	code, data = api(t, f, "GET", "/api/v1/clients/"+url.PathEscape("gw/sub-1"), adminReader, "")
	detail := decode[admin.ClientDetail](t, data)
	if code != 200 || detail.Connection == nil || detail.Connection.Listener != "tcp" || detail.Lease == nil || detail.Lease.Owner != f.srv.config().Node.Name && detail.Lease.Owner != "node" {
		t.Fatalf("inspect: %d %s", code, data)
	}
	code, data = api(t, f, "GET", "/api/v1/clients/"+url.PathEscape("gw/sub-1")+"/subscriptions", adminReader, "")
	if subs := decode[[]admin.Subscription](t, data); code != 200 || len(subs) != 1 || subs[0].Filter != "t/+" || subs[0].QoS != 1 {
		t.Fatalf("subscriptions: %d %s", code, data)
	}
	code, data = api(t, f, "GET", "/api/v1/subscriptions?topic=t/1", adminReader, "")
	if m := decode[[]admin.Matcher](t, data); code != 200 || len(m) != 1 || m[0].ClientID != "gw/sub-1" {
		t.Fatalf("matchers: %d %s", code, data)
	}
	if code, _ := api(t, f, "GET", "/api/v1/clients/nobody", adminReader, ""); code != 404 {
		t.Fatalf("unknown client: %d", code)
	}

	// Disconnect: the client reads DISCONNECT 0x98, its Will goes out, the
	// session stays, offline.
	if code, data := api(t, f, "POST", "/api/v1/clients/"+url.PathEscape("gw/sub-1")+"/disconnect", adminReader, ""); code != 403 {
		t.Fatalf("reader disconnect: %d %s", code, data)
	}
	if code, data := api(t, f, "POST", "/api/v1/clients/"+url.PathEscape("gw/sub-1")+"/disconnect", adminOps, ""); code != 200 {
		t.Fatalf("disconnect: %d %s", code, data)
	}
	if d, ok := sub.read().(*packet.Disconnect); !ok || d.ReasonCode != packet.AdministrativeAction {
		t.Fatalf("the client did not read DISCONNECT 0x98")
	}
	if w := watcher.readPublish(); w.Topic != "will/sub" || string(w.Payload) != "sub-gone" {
		t.Fatalf("will %s %q", w.Topic, w.Payload)
	}
	adminWait(t, "the session offline", func() bool { r, ok := f.srv.Sessions().Record("gw/sub-1"); return ok && !r.Online })
	if code, _ := api(t, f, "POST", "/api/v1/clients/"+url.PathEscape("gw/sub-1")+"/disconnect", adminOps, ""); code != 409 {
		t.Fatalf("disconnect offline: %d", code)
	}

	// Invalidate a connected client with a Will: it reads 0x98, the Will goes
	// out because the session ends, and the session and its lease are gone.
	doomed, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "doomed", CleanStart: true, Will: &packet.Will{Topic: "will/doomed", Payload: []byte("ended")},
		Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	code, data = api(t, f, "DELETE", "/api/v1/clients/doomed/session", adminOps, "")
	inv := decode[admin.Invalidated](t, data)
	if code != 200 || !inv.WasOnline || !inv.WillPublished || !inv.Durable {
		t.Fatalf("invalidate: %d %s", code, data)
	}
	if d, ok := doomed.read().(*packet.Disconnect); !ok || d.ReasonCode != packet.AdministrativeAction {
		t.Fatal("the invalidated client did not read DISCONNECT 0x98")
	}
	if w := watcher.readPublish(); w.Topic != "will/doomed" {
		t.Fatalf("will %s", w.Topic)
	}
	if _, ok := f.srv.Sessions().Record("doomed"); ok {
		t.Fatal("the invalidated session is still held")
	}
	adminWait(t, "the lease released", func() bool { _, ok := f.srv.Leases().Table().Lookup("default", "doomed"); return !ok })
	// The offline session: gone too.
	if code, data := api(t, f, "DELETE", "/api/v1/clients/"+url.PathEscape("gw/sub-1")+"/session", adminOps, ""); code != 200 {
		t.Fatalf("invalidate offline: %d %s", code, data)
	}
	if code, _ := api(t, f, "DELETE", "/api/v1/clients/nobody/session", adminOps, ""); code != 404 {
		t.Fatalf("invalidate unknown: %d", code)
	}

	// Retained: list, delete one, and nothing reaches a subscriber.
	for _, tp := range []string{"r/1", "r/2"} {
		pub.send(&packet.Publish{Topic: tp, Retain: true, QoS: 1, PacketID: 7, Payload: []byte("v-" + tp)})
		pub.read()
	}
	code, data = api(t, f, "GET", "/api/v1/retained?filter="+url.QueryEscape("r/#"), adminReader, "")
	if rp := decode[admin.RetainedPage](t, data); code != 200 || len(rp.Items) != 2 || rp.Items[0].Topic != "r/1" || string(rp.Items[1].Payload) != "v-r/2" {
		t.Fatalf("retained: %d %s", code, data)
	}
	code, data = api(t, f, "GET", "/api/v1/retained?limit=1&filter="+url.QueryEscape("r/#"), adminReader, "")
	if rp := decode[admin.RetainedPage](t, data); code != 200 || len(rp.Items) != 1 || rp.Items[0].Topic != "r/1" || rp.Next != "r/1" {
		t.Fatalf("retained page 1: %d %s", code, data)
	}
	code, data = api(t, f, "GET", "/api/v1/retained?limit=1&after=r/1&filter="+url.QueryEscape("r/#"), adminReader, "")
	if rp := decode[admin.RetainedPage](t, data); code != 200 || len(rp.Items) != 1 || rp.Items[0].Topic != "r/2" || rp.Next != "" {
		t.Fatalf("retained page 2: %d %s", code, data)
	}
	listener, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "rl", CleanStart: true}, false)
	listener.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "r/#", QoS: 0, RetainHandling: 2}}})
	listener.read()
	if code, data := api(t, f, "DELETE", "/api/v1/retained?topic=r/2", adminOps, ""); code != 200 || !decode[admin.Durability](t, data).Durable {
		t.Fatalf("retained delete: %d %s", code, data)
	}
	listener.expectNoPacket()
	code, data = api(t, f, "GET", "/api/v1/retained?filter="+url.QueryEscape("r/#"), adminReader, "")
	if rp := decode[admin.RetainedPage](t, data); code != 200 || len(rp.Items) != 1 || rp.Items[0].Topic != "r/1" {
		t.Fatalf("retained after delete: %d %s", code, data)
	}

	// Traces.
	if code, data := api(t, f, "POST", "/api/v1/traces/pub", adminOps, `{"limit":20}`); code != 200 {
		t.Fatalf("trace start: %d %s", code, data)
	}
	pub.send(&packet.Publish{Topic: "t/1", QoS: 1, PacketID: 9, Payload: []byte("x")})
	pub.read()
	code, data = api(t, f, "GET", "/api/v1/traces/pub", adminReader, "")
	if evs := decode[[]admin.TraceEvent](t, data); code != 200 || len(evs) < 2 {
		t.Fatalf("trace fetch: %d %s", code, data)
	}
	code, data = api(t, f, "DELETE", "/api/v1/traces/pub", adminOps, "")
	if evs := decode[[]admin.TraceEvent](t, data); code != 200 || len(evs) < 2 || f.srv.Trace("pub") != nil {
		t.Fatalf("trace stop: %d %s", code, data)
	}
	if code, _ := api(t, f, "POST", "/api/v1/traces/nobody", adminOps, ""); code != 404 {
		t.Fatalf("trace unknown: %d", code)
	}

	// Modules.
	if code, data := api(t, f, "POST", "/api/v1/modules/metrics.prometheus/disable", adminOps, ""); code != 200 || decode[admin.Module](t, data).Enabled {
		t.Fatalf("disable: %d %s", code, data)
	}
	if code, data := api(t, f, "POST", "/api/v1/modules/metrics.prometheus/enable", adminOps, ""); code != 200 || !decode[admin.Module](t, data).Enabled {
		t.Fatalf("enable: %d %s", code, data)
	}
	if code, _ := api(t, f, "POST", "/api/v1/modules/store.wal/disable", adminOps, ""); code != 409 {
		t.Fatalf("store.wal: %d", code)
	}
	if code, _ := api(t, f, "POST", "/api/v1/modules/nope.thing/disable", adminOps, ""); code != 404 {
		t.Fatalf("unknown module: %d", code)
	}

	// Configuration: the view redacts the token files, and validation reports
	// what check reports.
	code, data = api(t, f, "GET", "/api/v1/config", adminReader, "")
	view := decode[struct {
		Admin struct {
			Tokens []map[string]any `json:"tokens"`
		} `json:"admin"`
	}](t, data)
	if code != 200 || len(view.Admin.Tokens) != 2 || view.Admin.Tokens[0]["name"] != "ops" || view.Admin.Tokens[0]["tokenFile"] != admin.Redacted || strings.Contains(string(data), "ops.token") {
		t.Fatalf("config view: %d %s", code, data)
	}
	code, data = api(t, f, "POST", "/api/v1/config/validate", adminOps, `{"version":2,"node":{"dataDir":""}}`)
	if v := decode[admin.ConfigValidation](t, data); code != 200 || v.Valid || len(v.Problems) != 2 {
		t.Fatalf("validate: %d %s", code, data)
	}

	// The cluster resource of a one-member cluster.
	code, data = api(t, f, "GET", "/api/v1/cluster", adminReader, "")
	cs := decode[admin.ClusterState](t, data)
	if code != 200 || !cs.Ready || len(cs.Members) != 1 || !cs.Members[0].Alive || cs.Members[0].Role != "voter" || cs.FeatureLevel.Cluster < 1 ||
		cs.ShardMap.Version != f.srv.Replication().Map().Version() || cs.Drain.Draining {
		t.Fatalf("cluster: %d %s", code, data)
	}

	// The observability listener's /drain is closed: the admin API owns it.
	resp, err := http.Post("http://"+f.srv.ObservabilityAddr().String()+"/drain", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 403 || f.srv.Draining() {
		t.Fatalf("observability drain: %d draining=%v", resp.StatusCode, f.srv.Draining())
	}

	// Every mutation above is in the audit log with its actor and outcome.
	for _, want := range [][4]string{
		{"token:reader", "client.disconnect", "gw/sub-1", "denied"},
		{"token:ops", "client.disconnect", "gw/sub-1", "ok"},
		{"token:ops", "client.disconnect", "gw/sub-1", "conflict"},
		{"token:ops", "session.invalidate", "doomed", "ok"},
		{"token:ops", "session.invalidate", "gw/sub-1", "ok"},
		{"token:ops", "retained.delete", "r/2", "ok"},
		{"token:ops", "trace.start", "pub", "ok"},
		{"token:ops", "trace.stop", "pub", "ok"},
		{"token:ops", "module.disable", "metrics.prometheus", "ok"},
		{"token:ops", "module.enable", "metrics.prometheus", "ok"},
		{"token:ops", "config.validate", f.srv.config().Node.Name, "ok"},
		{"anonymous", "admin.auth.failure", "/api/v1/clients", "unauthenticated"},
	} {
		target := want[2]
		if target == "" {
			target = "node"
		}
		if !auditHas(t, f, want[0], want[1], target, want[3]) {
			t.Errorf("no audit record %v", want)
		}
	}

	// The drain: refused to a reader, started for ops, and the node leaves.
	if code, _ := api(t, f, "POST", "/api/v1/cluster/drain", adminReader, ""); code != 403 || f.srv.Draining() {
		t.Fatalf("reader drain: %d", code)
	}
	code, data = api(t, f, "POST", "/api/v1/cluster/drain", adminOps, "")
	if code != 202 || !decode[admin.DrainStatus](t, data).Draining {
		t.Fatalf("drain: %d %s", code, data)
	}
	select {
	case <-f.srv.Drained():
	case <-time.After(10 * time.Second):
		t.Fatal("the drain did not complete")
	}
	// A lone node's hand-off has no target or is refused one to itself;
	// neither is tried again, and the drain counts as making no hand-off.
	if got := metricSum(t, f, "hoardmq_drain_handoff_retries_total", nil); got != 0 {
		t.Fatalf("handoff_retries = %v, want 0", got)
	}
	if got := metricSum(t, f, "hoardmq_drain_placement_total", map[string]string{"outcome": "not-handed-off"}); got != 1 {
		t.Fatalf("placement{not-handed-off} = %v, want 1", got)
	}
}

// Shutdown closes the admin listener.
func TestAdminStopsWithTheServer(t *testing.T) {
	f := start(t, withAdmin(t, t.TempDir()))
	addr := f.srv.AdminAddr().String()
	if code, _ := api(t, f, "GET", "/api/v1/cluster", adminReader, ""); code != 200 {
		t.Fatalf("before shutdown: %d", code)
	}
	f.stop()
	if c, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
		_ = c.Close()
		t.Fatal("the admin listener still accepts after shutdown")
	}
}

// Without the admin API the observability listener's /drain still drains:
// the chart's default preStop.
func TestObservabilityDrainWithoutAdmin(t *testing.T) {
	f := start(t, func(d *config.Document) {
		d.Observability.Address = "127.0.0.1:0"
		d.Modules["metrics.prometheus"] = json.RawMessage(`{}`)
	})
	resp, err := http.Post("http://"+f.srv.ObservabilityAddr().String()+"/drain", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 202 || !f.srv.Draining() {
		t.Fatalf("observability drain without admin: %d draining=%v", resp.StatusCode, f.srv.Draining())
	}
}

// A taken admin port refuses the start with nothing created.
func TestAdminBindIsAllOrNothing(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	blocker := l.Addr().String()
	dir := t.TempDir()
	doc := config.Default()
	doc.Node.DataDir = filepath.Join(dir, "data")
	doc.Listeners = []config.Listener{{Name: "tcp", Type: "tcp", Address: "127.0.0.1:0"}}
	withAdmin(t, dir)(&doc)
	doc.Admin.Address = blocker
	srv, err := New(doc, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(); err == nil || !strings.Contains(err.Error(), "admin listener") {
		t.Fatalf("start with the admin port taken: %v", err)
	}
	if _, err := os.Stat(doc.Node.DataDir); !os.IsNotExist(err) {
		t.Fatal("the data directory was created")
	}
}

// The invalidation predicate, driven with the arguments it exists to refuse.
func TestInvalidateAdmitted(t *testing.T) {
	cases := []struct {
		seen, held uint64
		leased     bool
		owner      string
		want       bool
	}{
		{5, 5, true, "n1", true},
		{0, 5, true, "n1", true},
		{0, 5, false, "", true},
		{6, 5, true, "n1", false},
		{5, 5, true, "n2", false},
		{9, 5, true, "n2", false},
		{6, 5, false, "", false},
	}
	for _, c := range cases {
		if got := invalidateAdmitted(c.seen, c.held, c.leased, c.owner, "n1"); got != c.want {
			t.Errorf("seen %d held %d leased %v owner %q: %v", c.seen, c.held, c.leased, c.owner, got)
		}
	}
}

// Client actions on a live three-node cluster: the node that does not hold
// the client names its owner and changes nothing; the owner acts.
func TestAdminAPIThreeNodes(t *testing.T) {
	dir := t.TempDir()
	nodes := startTrio(t, func(_ string, d *config.Document) {
		withAdmin(t, dir)(d)
		d.Cluster.Shards = 8
		d.Cluster.Readiness.StableMs = 0
	})
	waitMap(t, nodes, 3)
	exp := uint32(600)
	c1, ack := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: "c1", CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("connect %s", ack.ReasonCode)
	}
	for _, n := range []string{"n2", "n3"} {
		adminWait(t, n+" to see c1's lease", func() bool {
			l, ok := nodes[n].srv.Leases().Table().Lookup("default", "c1")
			return ok && l.Owner == "n1"
		})
	}
	n1, n2 := nodes["n1"], nodes["n2"]
	// Search finds c1 from any node, naming n1; only n1's answer carries the
	// session.
	code, data := api(t, n2, "GET", "/api/v1/clients?search=c1", adminReader, "")
	if pg := decode[admin.ClientPage](t, data); code != 200 || len(pg.Items) != 1 || pg.Items[0].Node != "n1" || pg.Items[0].Session != nil || !pg.Items[0].Online {
		t.Fatalf("n2 search: %d %s", code, data)
	}
	code, data = api(t, n1, "GET", "/api/v1/clients?search=c1", adminReader, "")
	if pg := decode[admin.ClientPage](t, data); code != 200 || len(pg.Items) != 1 || pg.Items[0].Node != "n1" || pg.Items[0].Session == nil {
		t.Fatalf("n1 search: %d %s", code, data)
	}
	if code, data := api(t, n2, "GET", "/api/v1/clients?search=c1&node=n2", adminReader, ""); code != 200 || len(decode[admin.ClientPage](t, data).Items) != 0 {
		t.Fatalf("n2 search of its own clients: %d %s", code, data)
	}
	// Reads on n2 are n1's answers.
	code, data, hdr := apiH(t, n2, "GET", "/api/v1/clients/c1", adminOps, "")
	if d := decode[admin.ClientDetail](t, data); code != 200 || hdr.Get(admin.NodeHeader) != "n1" || d.Connection == nil || d.Connection.Listener != "tcp" {
		t.Fatalf("n2 inspect: %d %q %s", code, hdr.Get(admin.NodeHeader), data)
	}
	if code, data, hdr := apiH(t, n2, "GET", "/api/v1/clients/c1/subscriptions", adminOps, ""); code != 200 || hdr.Get(admin.NodeHeader) != "n1" {
		t.Fatalf("n2 subscriptions: %d %s", code, data)
	}
	if code, data := api(t, n2, "GET", "/api/v1/cluster/leases/c1", adminReader, ""); code != 200 || decode[admin.Lease](t, data).Owner != "n1" {
		t.Fatalf("n2 lease: %d %s", code, data)
	}
	code, data = api(t, n2, "GET", "/api/v1/cluster", adminReader, "")
	cs := decode[admin.ClusterState](t, data)
	if code != 200 || len(cs.Members) != 3 || cs.Node != "n2" || cs.ShardMap.Version != n2.srv.Replication().Map().Version() {
		t.Fatalf("n2 cluster: %d %s", code, data)
	}
	for _, m := range cs.Members {
		if !m.Alive || m.Role != "voter" || m.Level != cs.FeatureLevel.Build {
			t.Fatalf("member %+v of %s", m, data)
		}
	}
	code, data = api(t, n2, "GET", "/api/v1/cluster/shards", adminReader, "")
	if sm := decode[admin.ShardMap](t, data); code != 200 || len(sm.Assignments) != 8 || len(sm.Basis) != 3 {
		t.Fatalf("n2 shards: %d %s", code, data)
	}
	// An invalidation called on n2 acts on n1: c1 is closed, every node's
	// table loses the lease, and both nodes audit it as the operator.
	code, data, hdr = apiH(t, n2, "DELETE", "/api/v1/clients/c1/session", adminOps, "")
	if inv := decode[admin.Invalidated](t, data); code != 200 || hdr.Get(admin.NodeHeader) != "n1" || !inv.WasOnline {
		t.Fatalf("n2 invalidate: %d %q %s", code, hdr.Get(admin.NodeHeader), data)
	}
	if d, ok := c1.read().(*packet.Disconnect); !ok || d.ReasonCode != packet.AdministrativeAction {
		t.Fatal("c1 did not read DISCONNECT 0x98")
	}
	for name, f := range nodes {
		adminWait(t, name+" to drop c1's lease", func() bool { _, ok := f.srv.Leases().Table().Lookup("default", "c1"); return !ok })
	}
	forwardAudited(t, n2, n1, "session.invalidate", "c1", "ok")
}

// A session held on a node whose lease has already moved is a copy on its
// way to the new owner; invalidating it there is refused and it stays.
func TestAdminInvalidateRefusesACopyInTransit(t *testing.T) {
	fixtureHooks = Hooks{TakeoverDelay: 3 * time.Second}
	t.Cleanup(func() { fixtureHooks = Hooks{} })
	dir := t.TempDir()
	nodes := startTrio(t, func(_ string, d *config.Document) {
		withAdmin(t, dir)(d)
		d.Cluster.Shards = 8
		d.Cluster.Readiness.StableMs = 0
	})
	waitMap(t, nodes, 3)
	exp := uint32(600)
	c, _ := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: "mover", CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	c.send(&packet.Disconnect{ReasonCode: packet.NormalDisconnection})
	_ = c.c.Close()
	n1 := nodes["n1"]
	adminWait(t, "mover offline on n1", func() bool { r, ok := n1.srv.Sessions().Record("mover"); return ok && !r.Online })
	// The client moves to n2; n1's pass that would seal the session is held
	// back, so n1 keeps the copy after its table names n2.
	nc, err := net.Dial("tcp", nodes["n2"].srv.Addr("tcp").String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nc.Close() })
	enc := &packet.Encoder{Version: packet.V5}
	if err := enc.Encode(nc, &packet.Connect{ProtocolLevel: packet.V5, ClientID: "mover", Properties: packet.Properties{SessionExpiryInterval: &exp}}); err != nil {
		t.Fatal(err)
	}
	adminWait(t, "n1's table to name n2", func() bool {
		l, ok := n1.srv.Leases().Table().Lookup("default", "mover")
		return ok && l.Owner == "n2"
	})
	if _, held := n1.srv.Sessions().Record("mover"); !held {
		t.Fatal("the fixture did not hold the copy on n1 after the move; nothing is tested")
	}
	// n1 refuses to discard its copy and sends the call to n2, the owner by
	// its table, which answers it.
	code, data, hdr := apiH(t, n1, "DELETE", "/api/v1/clients/mover/session", adminOps, "")
	if hdr.Get(admin.NodeHeader) != "n2" {
		t.Fatalf("invalidate a copy in transit: %d %q %s", code, hdr.Get(admin.NodeHeader), data)
	}
	if _, held := n1.srv.Sessions().Record("mover"); !held {
		t.Fatal("the refused invalidation discarded the copy")
	}
	// n1's search names n2 and carries no session: its copy is not the
	// client's session.
	code, data = api(t, n1, "GET", "/api/v1/clients?search=mover", adminReader, "")
	if pg := decode[admin.ClientPage](t, data); code != 200 || len(pg.Items) != 1 || pg.Items[0].Node != "n2" || pg.Items[0].Session != nil {
		t.Fatalf("n1 search for a copy in transit: %d %s", code, data)
	}
}
