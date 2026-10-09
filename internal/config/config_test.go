package config

import (
	"github.com/happydave/hoardmq/internal/packet"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

const valid = `{
  "version": 1,
  "node": {"name": "n1", "dataDir": "/var/lib/hoardmq"},
  "listeners": [
    {"name": "plain", "type": "tcp", "address": ":1883"},
    {"name": "secure", "type": "tls", "address": "0.0.0.0:8883", "tls": {"certFile": "c.pem", "keyFile": "k.pem", "minVersion": "1.3"}},
    {"name": "web", "type": "ws", "address": "[::1]:8080", "path": "/ws", "maxConnections": 10},
    {"name": "webs", "type": "wss", "address": ":8443", "tls": {"certFile": "c.pem", "keyFile": "k.pem"}}
  ],
  "auth": {"anonymous": true, "decisionTtlSeconds": 600},
  "cluster": {"replicas": 3, "shards": 128},
  "limits": {"maxPacketSize": 1048576},
  "modules": {"auth.file": {"path": "users.json"}},
  "log": {"level": "debug"},
  "mqtt": {"maximumQoS": 1, "sharedSubscriptionAvailable": false},
  "admin": {"address": "127.0.0.1:9443", "tls": {"certFile": "c.pem", "keyFile": "k.pem", "clientCAFile": "ca.pem"}, "roles": [{"name": "keys", "scopes": ["apikeys:read", "apikeys:write"]}], "certificates": [{"commonName": "ops", "scopes": ["read", "cluster:drain"], "roles": ["keys"]}], "oidc": {"issuer": "https://idp.example", "clientIds": ["cli"], "usernameClaim": "email", "groupsClaim": "grp", "roleMappings": [{"group": "ops", "roles": ["keys"]}], "clockSkewSeconds": 30, "maxTokenLifetimeSeconds": 3600}, "maxBodyBytes": 2048}
}`

func lines(p []Problem) string {
	var out []string
	for _, x := range p {
		out = append(out, x.String())
	}
	return strings.Join(out, "\n")
}

func TestValidDocumentLoadsWithDefaults(t *testing.T) {
	doc, problems := Parse([]byte(valid), nil)
	if len(problems) != 0 {
		t.Fatalf("problems: %s", lines(problems))
	}
	if doc.Limits.ReceiveMaximum != 1024 || doc.Limits.MaxQueuedMessages != 1000 || doc.Log.Format != "text" || doc.Limits.QueueDrop != "oldest" || doc.Node.WALSync != "commit" || doc.Node.WALSegmentBytes != 64<<20 {
		t.Errorf("defaults not applied: %+v", doc)
	}
	if doc.Limits.MaxPacketSize != 1048576 || doc.Cluster.Replicas != 3 || len(doc.Listeners) != 4 || doc.Listeners[1].TLS.MinVersion != "1.3" || doc.Listeners[2].Path != "/ws" || doc.Listeners[2].MaxConnections != 10 || !doc.Auth.Anonymous {
		t.Errorf("values not loaded: %+v", doc)
	}
	if string(doc.Modules["auth.file"]) != `{"path":"users.json"}` {
		t.Errorf("modules not held opaque: %s", doc.Modules["auth.file"])
	}
	if p := doc.Validate(); len(p) != 0 {
		t.Errorf("round-trip Validate: %s", lines(p))
	}
}

func TestObservabilityAddressRules(t *testing.T) {
	for _, ok := range []string{`"127.0.0.1:9100"`, `"localhost:9100"`, `"[::1]:9100"`} {
		if _, p := Parse([]byte(`{"version":1,"node":{"dataDir":"d"},"observability":{"address":`+ok+`}}`), nil); len(p) != 0 {
			t.Errorf("%s: %s", ok, lines(p))
		}
	}
	if _, p := Parse([]byte(`{"version":1,"node":{"dataDir":"d"},"observability":{"address":"0.0.0.0:9100","allowRemote":true}}`), nil); len(p) != 0 {
		t.Errorf("allowRemote: %s", lines(p))
	}
	doc, _ := Parse([]byte(`{"version":1,"node":{"dataDir":"d"}}`), nil)
	if doc.Observability.Address != "" || doc.Observability.MetricsPath != "/metrics" || doc.Observability.TraceEvents != 1000 || !doc.Observability.Audit.Enabled || doc.Observability.Audit.MaxFiles != 10 || doc.Observability.Audit.Queue != 8192 {
		t.Errorf("defaults %+v", doc.Observability)
	}
}

func TestMinimalDocument(t *testing.T) {
	doc, problems := Parse([]byte(`{"version":1,"node":{"dataDir":"d"}}`), nil)
	if len(problems) != 0 {
		t.Fatalf("problems: %s", lines(problems))
	}
	want := Default()
	want.Node.DataDir = "d"
	if !reflect.DeepEqual(doc, want) {
		t.Errorf("got %+v want %+v", doc, want)
	}
}

// Each rule the plan states has a failing fixture; the expected pointer is
// what the operator sees.
func TestEachRuleHasAFailingFixture(t *testing.T) {
	cases := []struct {
		name, doc, pointer, contains string
	}{
		{"version missing", `{"node":{"dataDir":"d"}}`, "/", `"version"`},
		{"version wrong", `{"version":2,"node":{"dataDir":"d"}}`, "/version", "must be 1"},
		{"node missing", `{"version":1}`, "/", `"node"`},
		{"dataDir empty", `{"version":1,"node":{"dataDir":""}}`, "/node/dataDir", "empty"},
		{"unknown top-level", `{"version":1,"node":{"dataDir":"d"},"nope":1}`, "/nope", "unknown member"},
		{"unknown nested", `{"version":1,"node":{"dataDir":"d","x":1}}`, "/node/x", "unknown member"},
		{"walSync", `{"version":1,"node":{"dataDir":"d","walSync":"never"}}`, "/node/walSync", "one of"},
		{"walSegmentBytes small", `{"version":1,"node":{"dataDir":"d","walSegmentBytes":10}}`, "/node/walSegmentBytes", "at least"},
		{"listener type", `{"version":1,"node":{"dataDir":"d"},"listeners":[{"name":"a","type":"udp","address":":1"}]}`, "/listeners/0/type", "one of"},
		{"listener name dup", `{"version":1,"node":{"dataDir":"d"},"listeners":[{"name":"a","type":"tcp","address":":1"},{"name":"a","type":"tcp","address":":2"}]}`, "/listeners/1/name", "duplicate"},
		{"address no port", `{"version":1,"node":{"dataDir":"d"},"listeners":[{"name":"a","type":"tcp","address":"localhost"}]}`, "/listeners/0/address", "host:port"},
		{"address service name", `{"version":1,"node":{"dataDir":"d"},"listeners":[{"name":"a","type":"tcp","address":":mqtt"}]}`, "/listeners/0/address", "numeric"},
		{"address port range", `{"version":1,"node":{"dataDir":"d"},"listeners":[{"name":"a","type":"tcp","address":":70000"}]}`, "/listeners/0/address", "between"},
		{"address port negative", `{"version":1,"node":{"dataDir":"d"},"listeners":[{"name":"a","type":"tcp","address":":-1"}]}`, "/listeners/0/address", "between"},
		{"tls missing", `{"version":1,"node":{"dataDir":"d"},"listeners":[{"name":"a","type":"tls","address":":1"}]}`, "/listeners/0/tls", "required"},
		{"tls on tcp", `{"version":1,"node":{"dataDir":"d"},"listeners":[{"name":"a","type":"tcp","address":":1","tls":{"certFile":"c","keyFile":"k"}}]}`, "/listeners/0/tls", "non-tls"},
		{"tls on ws", `{"version":1,"node":{"dataDir":"d"},"listeners":[{"name":"a","type":"ws","address":":1","tls":{"certFile":"c","keyFile":"k"}}]}`, "/listeners/0/tls", "non-tls"},
		{"wss without tls", `{"version":1,"node":{"dataDir":"d"},"listeners":[{"name":"a","type":"wss","address":":1"}]}`, "/listeners/0/tls", "required"},
		{"path on tcp", `{"version":1,"node":{"dataDir":"d"},"listeners":[{"name":"a","type":"tcp","address":":1","path":"/x"}]}`, "/listeners/0/path", "only meaningful"},
		{"path not absolute", `{"version":1,"node":{"dataDir":"d"},"listeners":[{"name":"a","type":"ws","address":":1","path":"x"}]}`, "/listeners/0/path", "must match"},
		{"maxConnections negative", `{"version":1,"node":{"dataDir":"d"},"listeners":[{"name":"a","type":"tcp","address":":1","maxConnections":-1}]}`, "/listeners/0/maxConnections", "at least 0"},
		{"connectTimeout 0", `{"version":1,"node":{"dataDir":"d"},"limits":{"connectTimeout":0}}`, "/limits/connectTimeout", "at least 1"},
		{"maxKeepAlive high", `{"version":1,"node":{"dataDir":"d"},"limits":{"maxKeepAlive":70000}}`, "/limits/maxKeepAlive", "at most"},
		{"queueDrop", `{"version":1,"node":{"dataDir":"d"},"limits":{"queueDrop":"random"}}`, "/limits/queueDrop", "one of"},
		{"maxSessionExpiry negative", `{"version":1,"node":{"dataDir":"d"},"limits":{"maxSessionExpiry":-1}}`, "/limits/maxSessionExpiry", "at least 0"},
		{"auth unknown", `{"version":1,"node":{"dataDir":"d"},"auth":{"anon":true}}`, "/auth/anon", "unknown member"},
		{"decisionTtlSeconds 0", `{"version":1,"node":{"dataDir":"d"},"auth":{"decisionTtlSeconds":0}}`, "/auth/decisionTtlSeconds", "at least 1"},
		{"argon2MemoryBytes small", `{"version":1,"node":{"dataDir":"d"},"auth":{"argon2MemoryBytes":1024}}`, "/auth/argon2MemoryBytes", "at least"},
		{"decisionTtlSeconds 86401", `{"version":1,"node":{"dataDir":"d"},"auth":{"decisionTtlSeconds":86401}}`, "/auth/decisionTtlSeconds", "at most 86400"},
		{"cache ttl negative", `{"version":1,"node":{"dataDir":"d"},"auth":{"cache":{"ttlSeconds":-1}}}`, "/auth/cache/ttlSeconds", "at least 0"},
		{"cache ttl high", `{"version":1,"node":{"dataDir":"d"},"auth":{"cache":{"ttlSeconds":3601}}}`, "/auth/cache/ttlSeconds", "at most 3600"},
		{"cache entries 0", `{"version":1,"node":{"dataDir":"d"},"auth":{"cache":{"maxEntries":0}}}`, "/auth/cache/maxEntries", "at least 1"},
		{"cache unknown", `{"version":1,"node":{"dataDir":"d"},"auth":{"cache":{"size":1}}}`, "/auth/cache/size", "unknown member"},
		{"listener connectRate negative", `{"version":1,"node":{"dataDir":"d"},"listeners":[{"name":"a","type":"tcp","address":":1","connectRate":-1}]}`, "/listeners/0/connectRate", "at least 0"},
		{"listener burst without rate", `{"version":1,"node":{"dataDir":"d"},"listeners":[{"name":"a","type":"tcp","address":":1","connectBurst":5}]}`, "/listeners/0/connectBurst", "needs a connectRate"},
		{"limits connectRate negative", `{"version":1,"node":{"dataDir":"d"},"limits":{"connectRate":-1}}`, "/limits/connectRate", "at least 0"},
		{"limits burst without rate", `{"version":1,"node":{"dataDir":"d"},"limits":{"connectBurst":5}}`, "/limits/connectBurst", "needs a connectRate"},
		{"client message burst without rate", `{"version":1,"node":{"dataDir":"d"},"limits":{"clientMessageBurst":5}}`, "/limits/clientMessageBurst", "needs a clientMessageRate"},
		{"client byte burst without rate", `{"version":1,"node":{"dataDir":"d"},"limits":{"clientByteBurst":5}}`, "/limits/clientByteBurst", "needs a clientByteRate"},
		{"client byte rate negative", `{"version":1,"node":{"dataDir":"d"},"limits":{"clientByteRate":-1}}`, "/limits/clientByteRate", "at least 0"},
		{"overload lag negative", `{"version":1,"node":{"dataDir":"d"},"overload":{"replicationLag":-1}}`, "/overload/replicationLag", "at least 0"},
		{"overload unknown", `{"version":1,"node":{"dataDir":"d"},"overload":{"cpu":1}}`, "/overload/cpu", "unknown member"},
		{"flapping window 0", `{"version":1,"node":{"dataDir":"d"},"flapping":{"windowSeconds":0}}`, "/flapping/windowSeconds", "at least 1"},
		{"flapping count 1", `{"version":1,"node":{"dataDir":"d"},"flapping":{"maxCount":1}}`, "/flapping/maxCount", "at least 2"},
		{"flapping ban 0", `{"version":1,"node":{"dataDir":"d"},"flapping":{"banSeconds":0}}`, "/flapping/banSeconds", "at least 1"},
		{"hooks fallback", `{"version":1,"node":{"dataDir":"d"},"hooks":{"fallback":"ignore"}}`, "/hooks/fallback", "one of"},
		{"hooks timeout", `{"version":1,"node":{"dataDir":"d"},"hooks":{"timeoutMs":-1}}`, "/hooks/timeoutMs", "at least 0"},
		{"observability remote", `{"version":1,"node":{"dataDir":"d"},"observability":{"address":"0.0.0.0:9100"}}`, "/observability/address", "loopback"},
		{"observability bad address", `{"version":1,"node":{"dataDir":"d"},"observability":{"address":"nope"}}`, "/observability/address", "host:port"},
		{"observability trace low", `{"version":1,"node":{"dataDir":"d"},"observability":{"traceEvents":1}}`, "/observability/traceEvents", "at least 10"},
		{"observability audit files", `{"version":1,"node":{"dataDir":"d"},"observability":{"audit":{"maxFiles":0}}}`, "/observability/audit/maxFiles", "at least 1"},
		{"tls keyFile missing", `{"version":1,"node":{"dataDir":"d"},"listeners":[{"name":"a","type":"tls","address":":1","tls":{"certFile":"c"}}]}`, "/listeners/0/tls", `"keyFile"`},
		{"tls minVersion", `{"version":1,"node":{"dataDir":"d"},"listeners":[{"name":"a","type":"tls","address":":1","tls":{"certFile":"c","keyFile":"k","minVersion":"1.1"}}]}`, "/listeners/0/tls/minVersion", "one of"},
		{"replicas low", `{"version":1,"node":{"dataDir":"d"},"cluster":{"replicas":0}}`, "/cluster/replicas", "at least 1"},
		{"replicas high", `{"version":1,"node":{"dataDir":"d"},"cluster":{"replicas":6}}`, "/cluster/replicas", "at most 5"},
		{"shards high", `{"version":1,"node":{"dataDir":"d"},"cluster":{"shards":5000}}`, "/cluster/shards", "at most 4096"},
		{"peer name empty", `{"version":1,"node":{"dataDir":"d"},"cluster":{"peers":[{"name":"","address":"a:1"}]}}`, "/cluster/peers/0/name", "empty"},
		{"peer without address", `{"version":1,"node":{"dataDir":"d"},"cluster":{"peers":[{"name":"a"}]}}`, "/cluster/peers/0", "address"},
		{"heartbeat low", `{"version":1,"node":{"dataDir":"d"},"cluster":{"heartbeatMs":50}}`, "/cluster/heartbeatMs", "at least 100"},
		{"frame low", `{"version":1,"node":{"dataDir":"d"},"cluster":{"maxFrameBytes":1024}}`, "/cluster/maxFrameBytes", "at least 65536"},
		{"auth mode", `{"version":1,"node":{"dataDir":"d"},"cluster":{"auth":{"mode":"password"}}}`, "/cluster/auth/mode", "one of"},
		{"maxPacketSize low", `{"version":1,"node":{"dataDir":"d"},"limits":{"maxPacketSize":8}}`, "/limits/maxPacketSize", "at least 16"},
		{"maxPacketSize high", `{"version":1,"node":{"dataDir":"d"},"limits":{"maxPacketSize":268435456}}`, "/limits/maxPacketSize", "at most"},
		{"receiveMaximum high", `{"version":1,"node":{"dataDir":"d"},"limits":{"receiveMaximum":65536}}`, "/limits/receiveMaximum", "at most"},
		{"maxQueued negative", `{"version":1,"node":{"dataDir":"d"},"limits":{"maxQueuedMessages":-1}}`, "/limits/maxQueuedMessages", "at least 0"},
		{"integer not float", `{"version":1,"node":{"dataDir":"d"},"limits":{"receiveMaximum":1.5}}`, "/limits/receiveMaximum", "expected integer"},
		{"module id", `{"version":1,"node":{"dataDir":"d"},"modules":{"Auth":{}}}`, "/modules/Auth", "must match"},
		{"module not object", `{"version":1,"node":{"dataDir":"d"},"modules":{"auth.file":1}}`, "/modules/auth.file", "expected object"},
		{"log level", `{"version":1,"node":{"dataDir":"d"},"log":{"level":"loud"}}`, "/log/level", "one of"},
		{"log format", `{"version":1,"node":{"dataDir":"d"},"log":{"format":"yaml"}}`, "/log/format", "one of"},
		{"mqtt qos 3", `{"version":1,"node":{"dataDir":"d"},"mqtt":{"maximumQoS":3}}`, "/mqtt/maximumQoS", "at most 2"},
		{"mqtt alias max", `{"version":1,"node":{"dataDir":"d"},"mqtt":{"topicAliasMaximum":65536}}`, "/mqtt/topicAliasMaximum", "at most"},
		{"mqtt bool", `{"version":1,"node":{"dataDir":"d"},"mqtt":{"retainAvailable":"yes"}}`, "/mqtt/retainAvailable", "expected boolean"},
		{"mqtt unknown", `{"version":1,"node":{"dataDir":"d"},"mqtt":{"qos":1}}`, "/mqtt/qos", "unknown member"},
		{"mqtt strategy", `{"version":1,"node":{"dataDir":"d"},"mqtt":{"sharedSubscriptionStrategy":"fastest"}}`, "/mqtt/sharedSubscriptionStrategy", "one of"},
		{"not an object", `[1]`, "/", "expected object"},
		{"invalid json", `{"version":1,`, "/", "invalid JSON"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, problems := Parse([]byte(c.doc), nil)
			for _, p := range problems {
				if p.Pointer == c.pointer && strings.Contains(p.Message, c.contains) {
					return
				}
			}
			t.Errorf("no problem at %s containing %q; got:\n%s", c.pointer, c.contains, lines(problems))
		})
	}
}

func TestAllProblemsReportedTogether(t *testing.T) {
	doc := `{"version":3,"node":{"dataDir":""},"listeners":[{"name":"a","type":"tls","address":"nope"}],"log":{"level":"x"},"zzz":1}`
	_, problems := Parse([]byte(doc), []string{"HOARDMQ_TYPO=1", "HOARDMQ_CLUSTER_SHARDS=many"})
	want := []string{
		"/: unknown environment override HOARDMQ_TYPO",
		`/cluster/shards: HOARDMQ_CLUSTER_SHARDS must be an integer, got "many"`,
		"/listeners/0/address: must be host:port",
		"/listeners/0/tls: required for a tls listener",
		`/log/level: must be one of "debug", "info", "warn", "error"`,
		"/node/dataDir: must not be empty",
		"/version: must be 1",
		"/zzz: unknown member",
	}
	if got := lines(problems); got != strings.Join(want, "\n") {
		t.Errorf("got:\n%s\nwant:\n%s", got, strings.Join(want, "\n"))
	}
}

func TestEnvironmentOverrides(t *testing.T) {
	env := []string{
		"HOARDMQ_NODE_DATA_DIR=/elsewhere",
		"HOARDMQ_NODE_NAME=n9",
		"HOARDMQ_CLUSTER_REPLICAS=2",
		"HOARDMQ_LIMITS_MAX_PACKET_SIZE=4096",
		"HOARDMQ_LOG_LEVEL=warn",
		"HOARDMQ_LOG_FORMAT=json",
		"HOARDMQ_CONFIG=ignored-here.json",
		"OTHER=1",
	}
	doc, problems := Parse([]byte(`{"version":1,"node":{"dataDir":"d"}}`), env)
	if len(problems) != 0 {
		t.Fatalf("problems: %s", lines(problems))
	}
	if doc.Node.DataDir != "/elsewhere" || doc.Node.Name != "n9" || doc.Cluster.Replicas != 2 || doc.Limits.MaxPacketSize != 4096 || doc.Log.Level != "warn" || doc.Log.Format != "json" {
		t.Errorf("overrides not applied: %+v", doc)
	}
}

func TestEnvironmentOverrideIsValidatedBySchema(t *testing.T) {
	_, problems := Parse([]byte(`{"version":1,"node":{"dataDir":"d"}}`), []string{"HOARDMQ_LOG_LEVEL=loud", "HOARDMQ_CLUSTER_REPLICAS=9"})
	if got := lines(problems); !strings.Contains(got, "/log/level: must be one of") || !strings.Contains(got, "/cluster/replicas: must be at most 5") {
		t.Errorf("got:\n%s", got)
	}
}

func TestEnvironmentOverrideUnderWrongTypeDoesNotPanic(t *testing.T) {
	_, problems := Parse([]byte(`{"version":1,"node":"oops"}`), []string{"HOARDMQ_NODE_DATA_DIR=/x"})
	if got := lines(problems); !strings.Contains(got, "/node: expected object") {
		t.Errorf("got:\n%s", got)
	}
}

func TestLoadMissingFileIsOneProblem(t *testing.T) {
	path := filepath.Join(t.TempDir(), "absent.json")
	_, problems := Load(path, nil)
	if len(problems) != 1 || problems[0].Pointer != "/" || !strings.Contains(problems[0].Message, "absent.json") {
		t.Fatalf("got %s", lines(problems))
	}
}

func TestLoadDirectoryIsOneProblem(t *testing.T) {
	_, problems := Load(t.TempDir(), nil)
	if len(problems) != 1 || problems[0].Pointer != "/" {
		t.Fatalf("got %s", lines(problems))
	}
}

func TestLoadReadsFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hoardmq.json")
	if err := os.WriteFile(path, []byte(valid), 0o600); err != nil {
		t.Fatal(err)
	}
	doc, problems := Load(path, nil)
	if len(problems) != 0 || doc.Node.Name != "n1" {
		t.Fatalf("got %+v %s", doc, lines(problems))
	}
}

func TestSchemaDeclaresDraft2020(t *testing.T) {
	// The embedded schema compiles (package init would panic otherwise); this
	// pins the declared dialect and identifier.
	if !strings.Contains(string(schemaBytes()), `"https://json-schema.org/draft/2020-12/schema"`) {
		t.Error("schema does not declare draft 2020-12")
	}
	if !strings.Contains(string(schemaBytes()), `"$id": "https://hoardmq.dev/schema/v1/hoardmq.schema.json"`) {
		t.Error("schema $id missing")
	}
}

func TestOverridesListIsSorted(t *testing.T) {
	o := Overrides()
	if len(o) != len(overrides) || o[0] != "HOARDMQ_CLUSTER_ADVERTISE" {
		t.Errorf("got %v", o)
	}
}

// The struct and the schema must agree on the member set: a document using
// every schema member decodes strictly.
func TestStructAcceptsEverySchemaMember(t *testing.T) {
	var doc Document
	if err := decodeStrict([]byte(valid), &doc); err != nil {
		t.Fatal(err)
	}
}

// The mqtt object defaults to every capability on (design invariant 6) and
// only the members named are switched.
func TestMQTTCapabilitySwitches(t *testing.T) {
	doc, problems := Parse([]byte(`{"version":1,"node":{"dataDir":"d"}}`), nil)
	if len(problems) != 0 {
		t.Fatal(lines(problems))
	}
	if !reflect.DeepEqual(doc.MQTT.Capabilities(), packet.DefaultCapabilities()) {
		t.Errorf("absent mqtt object must equal the codec default: %+v", doc.MQTT)
	}
	doc, problems = Parse([]byte(valid), nil)
	if len(problems) != 0 {
		t.Fatal(lines(problems))
	}
	c := doc.MQTT.Capabilities()
	if c.MaximumQoS != 1 || c.SharedSubscriptionAvailable || !c.RetainAvailable || !c.WildcardSubscriptionAvailable || !c.SubscriptionIdentifiersAvailable || c.TopicAliasMaximum != 65535 {
		t.Errorf("switches: %+v", c)
	}
}

// The decision TTL's bounds are inclusive; absent, it is an hour.
func TestDecisionTTLBounds(t *testing.T) {
	for _, v := range []string{"1", "86400"} {
		if d, p := Parse([]byte(`{"version":1,"node":{"dataDir":"d"},"auth":{"decisionTtlSeconds":`+v+`}}`), nil); len(p) != 0 || strconv.Itoa(d.Auth.DecisionTTLSeconds) != v {
			t.Errorf("%s: %v %d", v, p, d.Auth.DecisionTTLSeconds)
		}
	}
	if d, _ := Parse([]byte(`{"version":1,"node":{"dataDir":"d"}}`), nil); d.Auth.DecisionTTLSeconds != 3600 {
		t.Errorf("default %d", d.Auth.DecisionTTLSeconds)
	}
}

// The cache's TTL bounds are inclusive and 0 turns it off; absent, it is a
// minute over at most 100,000 entries. The connect rates default to unlimited.
func TestAuthCacheAndConnectRateDefaults(t *testing.T) {
	for _, v := range []string{"0", "3600"} {
		if d, p := Parse([]byte(`{"version":1,"node":{"dataDir":"d"},"auth":{"cache":{"ttlSeconds":`+v+`}}}`), nil); len(p) != 0 || strconv.Itoa(d.Auth.Cache.TTLSeconds) != v {
			t.Errorf("ttlSeconds %s: %v %d", v, p, d.Auth.Cache.TTLSeconds)
		}
	}
	d, p := Parse([]byte(`{"version":1,"node":{"dataDir":"d"},"listeners":[{"name":"a","type":"tcp","address":":1"}]}`), nil)
	if len(p) != 0 {
		t.Fatal(p)
	}
	if d.Auth.Cache.TTLSeconds != 60 || d.Auth.Cache.MaxEntries != 100000 {
		t.Errorf("cache defaults %+v, want ttl 60 and 100000 entries", d.Auth.Cache)
	}
	if d.Limits.ConnectRate != 0 || d.Limits.ConnectBurst != 0 || d.Listeners[0].ConnectRate != 0 || d.Listeners[0].ConnectBurst != 0 {
		t.Errorf("connect rate defaults: node %d/%d, listener %d/%d; want all 0", d.Limits.ConnectRate, d.Limits.ConnectBurst, d.Listeners[0].ConnectRate, d.Listeners[0].ConnectBurst)
	}
	d, p = Parse([]byte(`{"version":1,"node":{"dataDir":"d"},"listeners":[{"name":"a","type":"tcp","address":":1","connectRate":500,"connectBurst":700}],"limits":{"connectRate":9}}`), nil)
	if len(p) != 0 || d.Listeners[0].ConnectRate != 500 || d.Listeners[0].ConnectBurst != 700 || d.Limits.ConnectRate != 9 {
		t.Errorf("rates as set: %v %+v %+v", p, d.Listeners[0], d.Limits)
	}
	if got := Burst(9, 0); got != 18 {
		t.Errorf("Burst(9, 0) = %d, want twice the rate", got)
	}
	if got := Burst(9, 4); got != 4 {
		t.Errorf("Burst(9, 4) = %d, want the burst set", got)
	}
}

// The overload tier's defaults: no client limit, stress limits of 1,000,000
// queued messages and 400,000 frames of backlog, flapping on at 15 in 60 s for a
// 300 s ban.
func TestOverloadDefaults(t *testing.T) {
	d, p := Parse([]byte(`{"version":1,"node":{"dataDir":"d"}}`), nil)
	if len(p) != 0 {
		t.Fatal(p)
	}
	l := d.Limits
	if l.ClientMessageRate != 0 || l.ClientMessageBurst != 0 || l.ClientByteRate != 0 || l.ClientByteBurst != 0 {
		t.Errorf("client limits %+v, want all 0", l)
	}
	if d.Overload != (Overload{QueuedMessages: 1000000, ReplicationLag: 400000}) {
		t.Errorf("overload %+v", d.Overload)
	}
	if d.Flapping != (Flapping{Enabled: true, WindowSeconds: 60, MaxCount: 15, BanSeconds: 300}) {
		t.Errorf("flapping %+v", d.Flapping)
	}
	d, p = Parse([]byte(`{"version":1,"node":{"dataDir":"d"},"flapping":{"enabled":false},"overload":{"memoryBytes":1024}}`), nil)
	if len(p) != 0 || d.Flapping.Enabled || d.Flapping.MaxCount != 15 || d.Overload.MemoryBytes != 1024 || d.Overload.QueuedMessages != 1000000 {
		t.Errorf("set: %v %+v %+v", p, d.Flapping, d.Overload)
	}
}
