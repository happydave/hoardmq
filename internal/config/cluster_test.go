package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/happydave/hoardmq/internal/testcert"
)

func clusterDoc(t *testing.T, cluster string) string {
	t.Helper()
	return `{"version":1,"node":{"name":"n1","dataDir":"d"},"cluster":` + cluster + `}`
}

func TestClusterSecretModeValidates(t *testing.T) {
	dir := t.TempDir()
	secret := filepath.Join(dir, "secret")
	if err := os.WriteFile(secret, []byte("0123456789abcdef0123\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	doc, problems := Parse([]byte(clusterDoc(t, `{"listen":"127.0.0.1:7000","auth":{"mode":"secret","secretFile":"`+secret+`"},"peers":[{"name":"n2","address":"h2:7000"},{"name":"n3","address":"h3:7000"}]}`)), nil)
	if len(problems) != 0 {
		t.Fatalf("problems:\n%s", lines(problems))
	}
	if doc.Cluster.ID != "hoardmq" || doc.Cluster.HeartbeatMs != 1000 || doc.Cluster.HeartbeatMisses != 3 || doc.Cluster.MaxFrameBytes != 16<<20 || len(doc.Cluster.Peers) != 2 || doc.Cluster.Peers[1].Address != "h3:7000" {
		t.Fatalf("defaults or peers wrong: %+v", doc.Cluster)
	}
}

func TestClusterMTLSModeValidates(t *testing.T) {
	dir := t.TempDir()
	ca, err := testcert.NewCA("cluster")
	if err != nil {
		t.Fatal(err)
	}
	caFile, err := ca.WriteCA(dir)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := ca.Issue("n1", 2)
	if err != nil {
		t.Fatal(err)
	}
	certFile, keyFile, err := leaf.Write(dir, "n1")
	if err != nil {
		t.Fatal(err)
	}
	tlsJSON := `{"certFile":"` + certFile + `","keyFile":"` + keyFile + `","caFile":"` + caFile + `"}`
	_, problems := Parse([]byte(clusterDoc(t, `{"listen":"127.0.0.1:7000","auth":{"mode":"mtls","tls":`+tlsJSON+`},"peers":[{"name":"n2","address":"h2:7000"}]}`)), nil)
	if len(problems) != 0 {
		t.Fatalf("problems:\n%s", lines(problems))
	}
	// A key that does not match the certificate is a problem at the pair.
	other, _ := ca.Issue("n9", 3)
	_, otherKey, _ := other.Write(dir, "n9")
	_, problems = Parse([]byte(clusterDoc(t, `{"listen":"127.0.0.1:7000","auth":{"mode":"mtls","tls":{"certFile":"`+certFile+`","keyFile":"`+otherKey+`","caFile":"`+caFile+`"}},"peers":[{"name":"n2","address":"h2:7000"}]}`)), nil)
	if got := lines(problems); !strings.Contains(got, "/cluster/auth/tls/certFile: certificate and key") {
		t.Fatalf("mismatched pair not reported:\n%s", got)
	}
	// A CA file without a certificate.
	empty := filepath.Join(dir, "empty.pem")
	_ = os.WriteFile(empty, []byte("nothing"), 0o600)
	_, problems = Parse([]byte(clusterDoc(t, `{"listen":"127.0.0.1:7000","auth":{"mode":"mtls","tls":{"certFile":"`+certFile+`","keyFile":"`+keyFile+`","caFile":"`+empty+`"}},"peers":[{"name":"n2","address":"h2:7000"}]}`)), nil)
	if got := lines(problems); !strings.Contains(got, "/cluster/auth/tls/caFile: no certificate found") {
		t.Fatalf("empty CA not reported:\n%s", got)
	}
}

func TestClusterRules(t *testing.T) {
	dir := t.TempDir()
	short := filepath.Join(dir, "short")
	_ = os.WriteFile(short, []byte("tiny"), 0o600)
	peers := `"peers":[{"name":"n2","address":"h2:7000"}]`
	cases := []struct{ name, cluster, pointer, want string }{
		{"peers without listen", `{"auth":{"mode":"secret","secretFile":"` + short + `"},` + peers + `}`, "/cluster/listen", "required when peers"},
		{"peers without auth", `{"listen":"127.0.0.1:1",` + peers + `}`, "/cluster/auth/mode", "required when peers"},
		{"duplicate peers", `{"listen":"127.0.0.1:1","peers":[{"name":"n2","address":"a:1"},{"name":"n2","address":"b:1"}]}`, "/cluster/peers/1/name", "duplicate"},
		{"self peer", `{"listen":"127.0.0.1:1","peers":[{"name":"n1","address":"a:1"}]}`, "/cluster/peers/0/name", "cannot be this node"},
		{"bad peer address", `{"listen":"127.0.0.1:1","peers":[{"name":"n2","address":"nohost"}]}`, "/cluster/peers/0/address", "host:port"},
		{"bad listen", `{"listen":"nope"}`, "/cluster/listen", "host:port"},
		{"mtls without tls", `{"auth":{"mode":"mtls"}}`, "/cluster/auth/tls", "required for mtls"},
		{"mtls with secret", `{"auth":{"mode":"mtls","secretFile":"x"}}`, "/cluster/auth/secretFile", "not used in mtls"},
		{"secret without file", `{"auth":{"mode":"secret"}}`, "/cluster/auth/secretFile", "required for secret"},
		{"secret missing file", `{"auth":{"mode":"secret","secretFile":"` + filepath.Join(dir, "none") + `"}}`, "/cluster/auth/secretFile", "no such file"},
		{"secret too short", `{"auth":{"mode":"secret","secretFile":"` + short + `"}}`, "/cluster/auth/secretFile", "at least 16 bytes"},
		{"secret with tls", `{"auth":{"mode":"secret","secretFile":"` + short + `","tls":{"certFile":"a","keyFile":"b","caFile":"c"}}}`, "/cluster/auth/tls", "not used in secret"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, problems := Parse([]byte(clusterDoc(t, c.cluster)), nil)
			found := false
			for _, p := range problems {
				if p.Pointer == c.pointer && strings.Contains(p.Message, c.want) {
					found = true
				}
			}
			if !found {
				t.Fatalf("want %s: %s, got\n%s", c.pointer, c.want, lines(problems))
			}
		})
	}
	// Peers configured but node unnamed.
	_, problems := Parse([]byte(`{"version":1,"node":{"dataDir":"d"},"cluster":{"listen":"127.0.0.1:1","auth":{"mode":"secret","secretFile":"`+short+`"},`+peers+`}}`), nil)
	if got := lines(problems); !strings.Contains(got, "/node/name: required when cluster peers") {
		t.Fatalf("unnamed node not reported:\n%s", got)
	}
}

func TestClusterEnvironmentOverrides(t *testing.T) {
	env := []string{"HOARDMQ_CLUSTER_ID=prod", "HOARDMQ_CLUSTER_LISTEN=127.0.0.1:7100", "HOARDMQ_CLUSTER_AUTH_MODE=mtls", "HOARDMQ_CLUSTER_AUTH_TLS_CERT_FILE=c", "HOARDMQ_CLUSTER_AUTH_TLS_KEY_FILE=k", "HOARDMQ_CLUSTER_AUTH_TLS_CA_FILE=ca", "HOARDMQ_CLUSTER_HEARTBEAT_MS=250", "HOARDMQ_CLUSTER_HEARTBEAT_MISSES=5", "HOARDMQ_CLUSTER_MAX_FRAME_BYTES=131072"}
	_, problems := Parse([]byte(clusterDoc(t, `{}`)), env)
	// The certificate files do not exist, which is the one expected problem;
	// everything else applied.
	got := lines(problems)
	if !strings.Contains(got, "/cluster/auth/tls/certFile") || !strings.Contains(got, "/cluster/auth/tls/caFile") || strings.Count(got, "\n") > 1 {
		t.Fatalf("problems:\n%s", got)
	}
	env = []string{"HOARDMQ_CLUSTER_ID=prod", "HOARDMQ_CLUSTER_HEARTBEAT_MS=250", "HOARDMQ_CLUSTER_HEARTBEAT_MISSES=5", "HOARDMQ_CLUSTER_MAX_FRAME_BYTES=131072"}
	doc, problems := Parse([]byte(clusterDoc(t, `{}`)), env)
	if len(problems) != 0 {
		t.Fatal(lines(problems))
	}
	if doc.Cluster.ID != "prod" || doc.Cluster.HeartbeatMs != 250 || doc.Cluster.HeartbeatMisses != 5 || doc.Cluster.MaxFrameBytes != 131072 {
		t.Fatalf("%+v", doc.Cluster)
	}
}

func TestRaftAndDiscoveryRules(t *testing.T) {
	doc, problems := Parse([]byte(clusterDoc(t, `{}`)), nil)
	if len(problems) != 0 {
		t.Fatal(lines(problems))
	}
	r := doc.Cluster.Raft
	if r.TickMs != 100 || r.ElectionTicks != 10 || r.HeartbeatTicks != 1 || r.SnapshotEntries != 10000 || r.SnapshotTrailing != 1000 || r.PromoteLag != 100 || r.JoinTimeoutMs != 30000 || r.Bootstrap != nil || doc.Cluster.Discovery.Mode != "static" {
		t.Fatalf("raft defaults %+v discovery %+v", r, doc.Cluster.Discovery)
	}
	dir := t.TempDir()
	secret := filepath.Join(dir, "secret")
	_ = os.WriteFile(secret, []byte("0123456789abcdef0123"), 0o600)
	auth := `"auth":{"mode":"secret","secretFile":"` + secret + `"}`
	// A valid dns document.
	valid := `{"version":1,"node":{"name":"hoardmq-1","dataDir":"d"},"cluster":{"listen":"0.0.0.0:7000",` + auth + `,"discovery":{"mode":"dns","dns":{"service":"hoardmq-headless","name":"hoardmq","replicas":3,"port":7000}}}}`
	if _, problems := Parse([]byte(valid), nil); len(problems) != 0 {
		t.Fatalf("valid dns document:\n%s", lines(problems))
	}
	cases := []struct{ name, doc, pointer, want string }{
		{"heartbeat above election", clusterDoc(t, `{"raft":{"heartbeatTicks":10,"electionTicks":10}}`), "/cluster/raft/heartbeatTicks", "below electionTicks"},
		{"trailing above snapshot", clusterDoc(t, `{"raft":{"snapshotEntries":100,"snapshotTrailing":100}}`), "/cluster/raft/snapshotTrailing", "below snapshotEntries"},
		{"dns without dns", clusterDoc(t, `{"discovery":{"mode":"dns"}}`), "/cluster/discovery/dns", "required"},
		{"dns without listen", `{"version":1,"node":{"name":"hoardmq-1","dataDir":"d"},"cluster":{` + auth + `,"discovery":{"mode":"dns","dns":{"service":"s","name":"hoardmq","replicas":3,"port":7000}}}}`, "/cluster/listen", "required for dns"},
		{"dns with peers", `{"version":1,"node":{"name":"hoardmq-1","dataDir":"d"},"cluster":{"listen":":7000",` + auth + `,"peers":[{"name":"x","address":"x:1"}],"discovery":{"mode":"dns","dns":{"service":"s","name":"hoardmq","replicas":3,"port":7000}}}}`, "/cluster/peers", "not used with dns"},
		{"dns bad node name", `{"version":1,"node":{"name":"broker","dataDir":"d"},"cluster":{"listen":":7000",` + auth + `,"discovery":{"mode":"dns","dns":{"service":"s","name":"hoardmq","replicas":3,"port":7000}}}}`, "/node/name", "hoardmq-<ordinal>"},
		{"dns ordinal too high", `{"version":1,"node":{"name":"hoardmq-3","dataDir":"d"},"cluster":{"listen":":7000",` + auth + `,"discovery":{"mode":"dns","dns":{"service":"s","name":"hoardmq","replicas":3,"port":7000}}}}`, "/node/name", "not below"},
		{"unknown mode", clusterDoc(t, `{"discovery":{"mode":"gossip"}}`), "/cluster/discovery/mode", "one of"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, problems := Parse([]byte(c.doc), nil)
			for _, p := range problems {
				if p.Pointer == c.pointer && strings.Contains(p.Message, c.want) {
					return
				}
			}
			t.Fatalf("want %s: %s, got\n%s", c.pointer, c.want, lines(problems))
		})
	}
	env := []string{"HOARDMQ_CLUSTER_RAFT_TICK_MS=50", "HOARDMQ_CLUSTER_DISCOVERY_MODE=static", "HOARDMQ_CLUSTER_RAFT_JOIN_TIMEOUT_MS=5000"}
	doc, problems = Parse([]byte(clusterDoc(t, `{}`)), env)
	if len(problems) != 0 || doc.Cluster.Raft.TickMs != 50 || doc.Cluster.Raft.JoinTimeoutMs != 5000 {
		t.Fatalf("%v %+v", lines(problems), doc.Cluster.Raft)
	}
}

func TestLeaseRules(t *testing.T) {
	doc, problems := Parse([]byte(clusterDoc(t, `{}`)), nil)
	if len(problems) != 0 || doc.Cluster.Leases.TTLMs != 30000 || doc.Cluster.Leases.TakeoverTimeoutMs != 5000 {
		t.Fatalf("%v %+v", lines(problems), doc.Cluster.Leases)
	}
	_, problems = Parse([]byte(clusterDoc(t, `{"leases":{"ttlMs":1000},"raft":{"tickMs":100,"electionTicks":10}}`)), nil)
	if got := lines(problems); !strings.Contains(got, "/cluster/leases/ttlMs: must be at least three election timeouts (3000 ms)") {
		t.Fatalf("ttl rule:\n%s", got)
	}
	_, problems = Parse([]byte(clusterDoc(t, `{"advertise":"nohost"}`)), nil)
	if got := lines(problems); !strings.Contains(got, "/cluster/advertise: must be host:port") {
		t.Fatalf("advertise rule:\n%s", got)
	}
	doc, problems = Parse([]byte(clusterDoc(t, `{}`)), []string{"HOARDMQ_CLUSTER_LEASES_TTL_MS=45000", "HOARDMQ_CLUSTER_ADVERTISE=broker.example:1883"})
	if len(problems) != 0 || doc.Cluster.Leases.TTLMs != 45000 || doc.Cluster.Advertise != "broker.example:1883" {
		t.Fatalf("%v %+v", lines(problems), doc.Cluster)
	}
}

func TestRoutingRules(t *testing.T) {
	doc, problems := Parse([]byte(clusterDoc(t, `{}`)), nil)
	if len(problems) != 0 || doc.Cluster.Routing.SyncMs != 30000 {
		t.Fatalf("%v %+v", lines(problems), doc.Cluster.Routing)
	}
	if _, problems = Parse([]byte(clusterDoc(t, `{"routing":{"syncMs":10}}`)), nil); len(problems) == 0 {
		t.Fatal("syncMs below the minimum accepted")
	}
	doc, problems = Parse([]byte(clusterDoc(t, `{}`)), []string{"HOARDMQ_CLUSTER_ROUTING_SYNC_MS=5000"})
	if len(problems) != 0 || doc.Cluster.Routing.SyncMs != 5000 {
		t.Fatalf("%v %+v", lines(problems), doc.Cluster.Routing)
	}
}

func TestReplicationRules(t *testing.T) {
	doc := Default()
	if doc.Cluster.Replicas != 2 || doc.Cluster.Replication.AckTimeoutMs != 2000 {
		t.Fatalf("defaults %+v", doc.Cluster)
	}
	got, problems := Parse([]byte(`{"version":1,"node":{"dataDir":"d"}}`), []string{"HOARDMQ_CLUSTER_REPLICATION_ACK_TIMEOUT_MS=750"})
	if len(problems) != 0 || got.Cluster.Replication.AckTimeoutMs != 750 {
		t.Fatalf("override: %v %+v", problems, got.Cluster.Replication)
	}
	_, problems = Parse([]byte(`{"version":1,"node":{"dataDir":"d"},"cluster":{"replication":{"ackTimeoutMs":9000},"leases":{"takeoverTimeoutMs":5000}}}`), nil)
	found := false
	for _, p := range problems {
		if p.Pointer == "/cluster/replication/ackTimeoutMs" {
			found = true
		}
	}
	if !found {
		t.Fatalf("ack timeout above the takeover timeout accepted: %v", problems)
	}
}

func TestDrainAndReadinessRules(t *testing.T) {
	doc := Default()
	if doc.Cluster.Drain.RatePerSecond != 100 || doc.Cluster.Drain.TimeoutMs != 30000 || doc.Cluster.Drain.ConfirmTimeoutMs != 30000 || doc.Cluster.Drain.HoldExitMs != 0 || doc.Cluster.Readiness.StableMs != 5000 {
		t.Fatalf("defaults %+v %+v", doc.Cluster.Drain, doc.Cluster.Readiness)
	}
	got, problems := Parse([]byte(`{"version":1,"node":{"dataDir":"d"}}`), []string{"HOARDMQ_CLUSTER_DRAIN_RATE_PER_SECOND=7", "HOARDMQ_CLUSTER_DRAIN_TIMEOUT_MS=4000", "HOARDMQ_CLUSTER_DRAIN_CONFIRM_TIMEOUT_MS=6000", "HOARDMQ_CLUSTER_DRAIN_HOLD_EXIT_MS=15000", "HOARDMQ_CLUSTER_READINESS_STABLE_MS=0"})
	if len(problems) != 0 || got.Cluster.Drain.RatePerSecond != 7 || got.Cluster.Drain.TimeoutMs != 4000 || got.Cluster.Drain.ConfirmTimeoutMs != 6000 || got.Cluster.Drain.HoldExitMs != 15000 || got.Cluster.Readiness.StableMs != 0 {
		t.Fatalf("overrides: %v %+v %+v", problems, got.Cluster.Drain, got.Cluster.Readiness)
	}
	for _, tc := range []struct{ doc, pointer string }{
		{`{"version":1,"node":{"dataDir":"d"},"cluster":{"drain":{"ratePerSecond":0}}}`, "/cluster/drain/ratePerSecond"},
		{`{"version":1,"node":{"dataDir":"d"},"cluster":{"drain":{"timeoutMs":10}}}`, "/cluster/drain/timeoutMs"},
		{`{"version":1,"node":{"dataDir":"d"},"cluster":{"drain":{"confirmTimeoutMs":999}}}`, "/cluster/drain/confirmTimeoutMs"},
		{`{"version":1,"node":{"dataDir":"d"},"cluster":{"drain":{"holdExitMs":-1}}}`, "/cluster/drain/holdExitMs"},
		{`{"version":1,"node":{"dataDir":"d"},"cluster":{"readiness":{"stableMs":-1}}}`, "/cluster/readiness/stableMs"},
	} {
		_, problems := Parse([]byte(tc.doc), nil)
		found := false
		for _, p := range problems {
			if p.Pointer == tc.pointer {
				found = true
			}
		}
		if !found {
			t.Errorf("%s: no problem at %s: %v", tc.doc, tc.pointer, problems)
		}
	}
}
