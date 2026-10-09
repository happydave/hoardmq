package sim

import (
	"strings"
	"testing"
)

func TestClientLimited(t *testing.T) {
	busy := func(v float64) *float64 { return &v }
	window := func(b float64) ClientHostSample {
		return ClientHostSample{Phase: phaseWindow, HostCPUBusy: busy(b), Failures: map[string]int64{}}
	}
	series := func(bs ...float64) []ClientHostSample {
		var out []ClientHostSample
		for _, b := range bs {
			out = append(out, window(b))
		}
		return out
	}
	for _, tc := range []struct {
		name     string
		series   []ClientHostSample
		limited  bool
		measured bool
		reason   string
	}{
		{"clean", series(0.2, 0.3, 0.25), false, true, ""},
		{"clean with one spike", series(0.2, 1.0, 0.3, 0.2), false, true, ""},
		{"two busy seconds, twice", series(0.95, 0.95, 0.1, 0.95, 0.95), false, true, ""},
		{"three busy seconds", series(0.1, 0.9, 0.95, 0.99, 0.1), true, true, "3 consecutive seconds"},
		{"an unavailable second breaks the run", append(append(series(0.95, 0.95), ClientHostSample{Phase: phaseWindow}), series(0.95)...), false, true, ""},
		{"port exhaustion", func() []ClientHostSample {
			s := series(0.1, 0.1)
			s[1].Failures[failAddrNotAvailable] = 3
			return s
		}(), true, true, "want of a local port"},
		{"file exhaustion", func() []ClientHostSample {
			s := series(0.1)
			s[0].Failures[failTooManyFiles] = 1
			return s
		}(), true, true, "want of a file descriptor"},
		{"driver stopped", func() []ClientHostSample {
			s := series(0.1)
			s[0].WorstStopSec = 1.5
			return s
		}(), true, true, "stopped for 1.50 s"},
		{"refusals are not a client limit", func() []ClientHostSample {
			s := series(0.1)
			s[0].Failures[failRefused] = 10000
			s[0].Failures[failHandshakeDeadline] = 5000
			return s
		}(), false, true, ""},
		{"busy outside the window is ignored", []ClientHostSample{
			{Phase: phaseFill, HostCPUBusy: busy(1)}, {Phase: phaseFill, HostCPUBusy: busy(1)}, {Phase: phaseFill, HostCPUBusy: busy(1)},
			window(0.1),
		}, false, true, ""},
		{"no window sample", []ClientHostSample{{Phase: phaseFill, HostCPUBusy: busy(0.1)}}, false, false, ""},
	} {
		v := clientLimited(tc.series)
		if v.Limited != tc.limited || v.Measured != tc.measured {
			t.Errorf("%s: limited %v measured %v (%v), want %v %v", tc.name, v.Limited, v.Measured, v.Reasons, tc.limited, tc.measured)
			continue
		}
		if tc.reason != "" && (len(v.Reasons) != 1 || !strings.Contains(v.Reasons[0], tc.reason)) {
			t.Errorf("%s: reasons %v, want one naming %q", tc.name, v.Reasons, tc.reason)
		}
	}
}

func TestProcParsers(t *testing.T) {
	total, idle, ok := parseCPUStat("cpu  100 5 50 800 20 1 2 3 7 0\ncpu0 1 1 1 1 1 1 1 1 0 0\n")
	if !ok || total != 981 || idle != 820 {
		t.Fatalf("cpu: total %d idle %d ok %v", total, idle, ok)
	}
	ticks, ok := parseSelfTicks("1234 (sim clients) S 1 1234 1234 0 -1 4194560 100 0 0 0 250 50 0 0 20 0 30 0\n")
	if !ok || ticks != 300 {
		t.Fatalf("self ticks %d ok %v", ticks, ok)
	}
	tcp := parseProcTable("Ip: Forwarding\nIp: 1\nTcp: RtoAlgorithm ActiveOpens RetransSegs\nTcp: 1 42 7\n", "Tcp:")
	if tcp["ActiveOpens"] != 42 || tcp["RetransSegs"] != 7 {
		t.Fatalf("tcp table %v", tcp)
	}
}
