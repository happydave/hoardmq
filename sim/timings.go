package sim

import (
	"fmt"
	"strings"
	"time"
)

// timingsEnv chooses a recorded failover run's timing set.
const timingsEnv = "HOARDMQ_SIM_TIMINGS"

// failoverTimings resolves the timing set for the failover test. A recorded
// measurement runs at the product's timings unless HOARDMQ_SIM_TIMINGS asks
// for the harness's, which are the stress variant (WI 1475). The gate — no
// HOARDMQ_SIM_FAILOVER — always runs the harness's: at a 30 s ttl it would
// wait out lease expiry on every run.
func failoverTimings(record bool, getenv func(string) string) (TimingSet, error) {
	if !record {
		return TimingsHarness, nil
	}
	switch v := getenv(timingsEnv); v {
	case "", string(TimingsProduct):
		return TimingsProduct, nil
	case string(TimingsHarness):
		return TimingsHarness, nil
	default:
		return "", fmt.Errorf("%s=%q: want %q or %q", timingsEnv, v, TimingsProduct, TimingsHarness)
	}
}

// timingLabel names the timings a run actually used: the set, and any knob
// a per-knob variable or the scenario moved away from the set's value. A run
// is never labelled with a set it did not run.
func timingLabel(set TimingSet, e Effective) string {
	if set == "" {
		set = TimingsHarness
	}
	label := string(set)
	if set == TimingsHarness {
		label += " (stress)"
	}
	ttl := time.Duration(e.LeaseTTLMs) * time.Millisecond
	// The ttl is judged against the set's own; the rest against the set at
	// the run's ttl, since the harness's drain confirmation follows its ttl.
	d, at := timingDefaults(set), setTimings(set, ttl)
	knobs := []struct {
		name, unit string
		got, want  int
	}{
		{"heartbeat", " ms", e.HeartbeatMs, at.HeartbeatMs},
		{"misses", "", e.HeartbeatMisses, at.HeartbeatMisses},
		{"raft tick", " ms", e.RaftTickMs, at.RaftTickMs},
		{"ttl", " ms", e.LeaseTTLMs, int(d.TTL / time.Millisecond)},
		{"takeover", " ms", e.TakeoverTimeoutMs, at.TakeoverTimeoutMs},
		{"ack", " ms", e.AckTimeoutMs, at.AckTimeoutMs},
		{"join", " ms", e.JoinTimeoutMs, at.JoinTimeoutMs},
		{"connect", " s", e.ConnectTimeoutSec, at.ConnectTimeoutSec},
		{"readiness stable", " ms", e.ReadinessStableMs, at.ReadinessStableMs},
		{"drain rate", "/s", e.DrainRatePerSecond, at.DrainRatePerSecond},
		{"drain timeout", " ms", e.DrainTimeoutMs, at.DrainTimeoutMs},
		{"drain confirm", " ms", e.DrainConfirmTimeoutMs, at.DrainConfirmTimeoutMs},
	}
	var moved []string
	for _, k := range knobs {
		if k.got != k.want {
			moved = append(moved, fmt.Sprintf("%s %d%s", k.name, k.got, k.unit))
		}
	}
	if len(moved) > 0 {
		label += ", overridden: " + strings.Join(moved, ", ")
	}
	return label
}
