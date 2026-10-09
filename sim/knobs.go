package sim

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/happydave/hoardmq/internal/config"
)

// The product settings a run may move away from their defaults, each by a
// variable every run's provenance records (WI 1283).
const (
	// connectRateEnv is "<rate>,<burst>" or "<rate>": every node's client
	// listener's connectRate and connectBurst.
	connectRateEnv = "HOARDMQ_SIM_CONNECT_RATE"
	// authCacheTTLEnv is auth.cache.ttlSeconds on every node; 0 turns the
	// password-verification cache off.
	authCacheTTLEnv = "HOARDMQ_SIM_AUTH_CACHE_TTL"
	// traceRatioEnv is traces.otlp's sampleRatio on every node, exporting to
	// an in-process OTLP endpoint the cluster starts (WI 1313).
	traceRatioEnv = "HOARDMQ_SIM_TRACE_RATIO"
	// drainRateEnv is cluster.drain.ratePerSecond on every node: the least
	// clients a drain redirects a second (WI 2251).
	drainRateEnv = "HOARDMQ_SIM_DRAIN_RATE"
)

// productKnobs are the settings read from the environment; each applies only
// when its variable is set.
type productKnobs struct {
	rate, burst int
	rateSet     bool
	cacheTTL    int
	cacheTTLSet bool
	traceRatio  float64
	traceSet    bool
	drainRate   int
	// traceEndpoint is the cluster's OTLP endpoint, set once it runs.
	traceEndpoint string
}

// parseConnectRate reads "<rate>,<burst>" or "<rate>", each a non-negative
// integer.
func parseConnectRate(v string) (rate, burst int, err error) {
	parts := strings.Split(v, ",")
	if len(parts) > 2 {
		return 0, 0, fmt.Errorf("%s=%q: want <rate>,<burst> or <rate>", connectRateEnv, v)
	}
	nums := make([]int, 2)
	for i, p := range parts {
		n, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil || n < 0 {
			return 0, 0, fmt.Errorf("%s=%q: %q is not a non-negative integer", connectRateEnv, v, p)
		}
		nums[i] = n
	}
	return nums[0], nums[1], nil
}

// readProductKnobs reads the variables; a malformed one is an error naming it.
func readProductKnobs(getenv func(string) string) (productKnobs, error) {
	var k productKnobs
	if v := getenv(connectRateEnv); v != "" {
		rate, burst, err := parseConnectRate(v)
		if err != nil {
			return k, err
		}
		k.rate, k.burst, k.rateSet = rate, burst, true
	}
	if v := getenv(authCacheTTLEnv); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 || n > 3600 {
			return k, fmt.Errorf("%s=%q: want 0 to 3600 seconds", authCacheTTLEnv, v)
		}
		k.cacheTTL, k.cacheTTLSet = n, true
	}
	if v := getenv(traceRatioEnv); v != "" {
		r, err := strconv.ParseFloat(v, 64)
		if err != nil || r < 0 || r > 1 {
			return k, fmt.Errorf("%s=%q: want a ratio from 0 to 1", traceRatioEnv, v)
		}
		k.traceRatio, k.traceSet = r, true
	}
	if v := getenv(drainRateEnv); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return k, fmt.Errorf("%s=%q: want a positive number of clients a second", drainRateEnv, v)
		}
		k.drainRate = n
	}
	return k, nil
}

// drainRate is the clients a node's drain redirects a second: the knob's, or
// the product's default.
func (c *Cluster) drainRate() int {
	if c.knobs.drainRate > 0 {
		return c.knobs.drainRate
	}
	return config.Default().Cluster.Drain.RatePerSecond
}

// apply sets the knobs on a node's document.
func (k productKnobs) apply(doc *config.Document) {
	if k.rateSet {
		for i := range doc.Listeners {
			doc.Listeners[i].ConnectRate, doc.Listeners[i].ConnectBurst = k.rate, k.burst
		}
	}
	if k.cacheTTLSet {
		doc.Auth.Cache.TTLSeconds = k.cacheTTL
	}
	if k.drainRate > 0 {
		doc.Cluster.Drain.RatePerSecond = k.drainRate
	}
	if k.traceSet {
		if doc.Modules == nil {
			doc.Modules = map[string]json.RawMessage{}
		}
		endpoint := k.traceEndpoint
		if endpoint == "" {
			endpoint = "http://127.0.0.1:4318/v1/traces"
		}
		doc.Modules["traces.otlp"] = json.RawMessage(`{"endpoint":"` + endpoint + `","sampleRatio":` + strconv.FormatFloat(k.traceRatio, 'g', -1, 64) + `}`)
	}
}
