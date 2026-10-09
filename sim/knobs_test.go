package sim

import (
	"testing"

	"github.com/happydave/hoardmq/internal/config"
)

func TestParseConnectRate(t *testing.T) {
	for v, want := range map[string][2]int{"500,1000": {500, 1000}, "500": {500, 0}, " 7 , 9": {7, 9}} {
		r, b, err := parseConnectRate(v)
		if err != nil || r != want[0] || b != want[1] {
			t.Errorf("%q: %d %d %v, want %v", v, r, b, err, want)
		}
	}
	for _, v := range []string{"x", "-1,2", "1,2,3", "1,", "1,-2"} {
		if _, _, err := parseConnectRate(v); err == nil {
			t.Errorf("%q accepted", v)
		}
	}
}

// The knobs reach a node's document only when their variables are set.
func TestProductKnobs(t *testing.T) {
	env := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }
	doc := config.Default()
	doc.Listeners = []config.Listener{{Name: "tcp"}}
	k, err := readProductKnobs(env(nil))
	if err != nil {
		t.Fatal(err)
	}
	k.apply(&doc)
	if doc.Listeners[0].ConnectRate != 0 || doc.Auth.Cache.TTLSeconds != 60 || doc.Cluster.Drain.RatePerSecond != 100 {
		t.Fatalf("with nothing set the defaults changed: %+v %+v %+v", doc.Listeners[0], doc.Auth.Cache, doc.Cluster.Drain)
	}
	k, err = readProductKnobs(env(map[string]string{connectRateEnv: "500,1000", authCacheTTLEnv: "0", drainRateEnv: "4000"}))
	if err != nil {
		t.Fatal(err)
	}
	k.apply(&doc)
	if doc.Listeners[0].ConnectRate != 500 || doc.Listeners[0].ConnectBurst != 1000 || doc.Auth.Cache.TTLSeconds != 0 || doc.Cluster.Drain.RatePerSecond != 4000 {
		t.Fatalf("set: %+v %+v %+v", doc.Listeners[0], doc.Auth.Cache, doc.Cluster.Drain)
	}
	for _, bad := range []map[string]string{{connectRateEnv: "fast"}, {authCacheTTLEnv: "3601"}, {authCacheTTLEnv: "-1"}, {drainRateEnv: "0"}, {drainRateEnv: "x"}} {
		if _, err := readProductKnobs(env(bad)); err == nil {
			t.Errorf("%v accepted", bad)
		}
	}
}

// The trace ratio puts traces.otlp on a node's document, pointed at the
// cluster's endpoint, only when set.
func TestTraceRatioKnob(t *testing.T) {
	env := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }
	doc := config.Default()
	k, err := readProductKnobs(env(nil))
	if err != nil {
		t.Fatal(err)
	}
	k.apply(&doc)
	if _, ok := doc.Modules["traces.otlp"]; ok {
		t.Fatal("traces.otlp configured with nothing set")
	}
	k, err = readProductKnobs(env(map[string]string{traceRatioEnv: "0.1"}))
	if err != nil {
		t.Fatal(err)
	}
	k.traceEndpoint = "http://127.0.0.1:9/v1/traces"
	k.apply(&doc)
	if got := string(doc.Modules["traces.otlp"]); got != `{"endpoint":"http://127.0.0.1:9/v1/traces","sampleRatio":0.1}` {
		t.Fatalf("traces.otlp %s", got)
	}
	for _, bad := range []string{"x", "-0.1", "1.5"} {
		if _, err := readProductKnobs(env(map[string]string{traceRatioEnv: bad})); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}
