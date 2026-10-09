package sim

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// ClientHostSample is one second of the client host in a remote-client run:
// what the machine and the driver process were doing, and how the clients'
// connects failed. A nil figure was not available on the host; it is never
// reported as zero. Counters are deltas over the second.
type ClientHostSample struct {
	// Sec is measured from the driver's ready message.
	Sec   float64 `json:"sec"`
	Phase string  `json:"phase"`

	HostCPUBusy       *float64 `json:"host_cpu_busy"`
	DriverCPUCores    *float64 `json:"driver_cpu_cores"`
	MemAvailableBytes *uint64  `json:"mem_available_bytes"`
	DriverRSSBytes    *uint64  `json:"driver_rss_bytes"`

	TCPInUse    *int64 `json:"tcp_inuse"`
	TCPTimeWait *int64 `json:"tcp_time_wait"`
	TCPOrphan   *int64 `json:"tcp_orphan"`

	RetransSegs  *int64 `json:"retrans_segs"`
	ActiveOpens  *int64 `json:"active_opens"`
	AttemptFails *int64 `json:"attempt_fails"`
	EstabResets  *int64 `json:"estab_resets"`
	SynRetrans   *int64 `json:"syn_retrans"`

	Failures map[string]int64 `json:"connect_failures"`

	Goroutines   int     `json:"goroutines"`
	WorstStopSec float64 `json:"worst_stop_sec"`

	// ServerConntrack is filled on the broker host when the sample arrives:
	// its connection-tracking table's size, which drops new connections
	// silently when full.
	ServerConntrack *int64 `json:"server_conntrack"`
}

// hostCounters is one cumulative reading behind a ClientHostSample.
type hostCounters struct {
	at                     time.Time
	cpuOK                  bool
	cpuTotal, cpuIdle      uint64
	selfOK                 bool
	selfTicks              uint64
	snmpOK                 bool
	retrans, opens, afails int64
	resets                 int64
	synOK                  bool
	synRetrans             int64
	failures               map[string]int64
}

// clockTicks is USER_HZ, the unit of /proc/stat and /proc/self/stat times.
// Linux fixes it at 100 on the architectures this harness runs on.
const clockTicks = 100

func readHostCounters(failures *failureCounts) hostCounters {
	h := hostCounters{at: time.Now()}
	if data, err := os.ReadFile("/proc/stat"); err == nil {
		h.cpuTotal, h.cpuIdle, h.cpuOK = parseCPUStat(string(data))
	}
	if data, err := os.ReadFile("/proc/self/stat"); err == nil {
		h.selfTicks, h.selfOK = parseSelfTicks(string(data))
	}
	if data, err := os.ReadFile("/proc/net/snmp"); err == nil {
		t := parseProcTable(string(data), "Tcp:")
		var ok1, ok2, ok3, ok4 bool
		h.retrans, ok1 = t["RetransSegs"]
		h.opens, ok2 = t["ActiveOpens"]
		h.afails, ok3 = t["AttemptFails"]
		h.resets, ok4 = t["EstabResets"]
		h.snmpOK = ok1 && ok2 && ok3 && ok4
	}
	if data, err := os.ReadFile("/proc/net/netstat"); err == nil {
		h.synRetrans, h.synOK = parseProcTable(string(data), "TcpExt:")["TCPSynRetrans"]
	}
	if failures != nil {
		h.failures = failures.snapshot()
	}
	return h
}

// parseCPUStat reads the aggregate cpu line: total jiffies and idle plus
// iowait.
func parseCPUStat(data string) (total, idle uint64, ok bool) {
	for _, line := range strings.Split(data, "\n") {
		f := strings.Fields(line)
		if len(f) < 6 || f[0] != "cpu" {
			continue
		}
		for i, v := range f[1:] {
			n, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				return 0, 0, false
			}
			// guest and guest_nice are already counted in user and nice.
			if i < 8 {
				total += n
			}
			if i == 3 || i == 4 {
				idle += n
			}
		}
		return total, idle, true
	}
	return 0, 0, false
}

// parseSelfTicks reads utime plus stime from /proc/self/stat. The command
// name can contain spaces, so fields are counted from its closing paren.
func parseSelfTicks(data string) (uint64, bool) {
	i := strings.LastIndexByte(data, ')')
	if i < 0 {
		return 0, false
	}
	f := strings.Fields(data[i+1:])
	// After the paren: state is field 3, utime 14 and stime 15.
	if len(f) < 13 {
		return 0, false
	}
	u, err1 := strconv.ParseUint(f[11], 10, 64)
	s, err2 := strconv.ParseUint(f[12], 10, 64)
	if err1 != nil || err2 != nil {
		return 0, false
	}
	return u + s, true
}

// parseProcTable reads a /proc/net/snmp or netstat table: a header line and a
// value line sharing a prefix.
func parseProcTable(data, prefix string) map[string]int64 {
	lines := strings.Split(data, "\n")
	for i := 0; i+1 < len(lines); i++ {
		if !strings.HasPrefix(lines[i], prefix) || !strings.HasPrefix(lines[i+1], prefix) {
			continue
		}
		names, values := strings.Fields(lines[i])[1:], strings.Fields(lines[i+1])[1:]
		out := make(map[string]int64, len(names))
		for j := 0; j < len(names) && j < len(values); j++ {
			if v, err := strconv.ParseInt(values[j], 10, 64); err == nil {
				out[names[j]] = v
			}
		}
		return out
	}
	return nil
}

// readSockstat reads TCP in use, TIME-WAIT and orphaned from
// /proc/net/sockstat.
func readSockstat() (inuse, tw, orphan int64, ok bool) {
	data, err := os.ReadFile("/proc/net/sockstat")
	if err != nil {
		return 0, 0, 0, false
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "TCP:") {
			continue
		}
		f := strings.Fields(line)
		seen := 0
		for i := 1; i+1 < len(f); i += 2 {
			n, err := strconv.ParseInt(f[i+1], 10, 64)
			if err != nil {
				continue
			}
			switch f[i] {
			case "inuse":
				inuse, seen = n, seen+1
			case "tw":
				tw, seen = n, seen+1
			case "orphan":
				orphan, seen = n, seen+1
			}
		}
		return inuse, tw, orphan, seen == 3
	}
	return 0, 0, 0, false
}

// readMemAvailable returns MemAvailable in bytes.
func readMemAvailable() (uint64, bool) {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, false
	}
	for _, line := range strings.Split(string(data), "\n") {
		if f := strings.Fields(line); len(f) >= 2 && f[0] == "MemAvailable:" {
			kb, err := strconv.ParseUint(f[1], 10, 64)
			return kb * 1024, err == nil
		}
	}
	return 0, false
}

// readSelfRSS returns this process's resident set in bytes.
func readSelfRSS() (uint64, bool) {
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0, false
	}
	f := strings.Fields(string(data))
	if len(f) < 2 {
		return 0, false
	}
	pages, err := strconv.ParseUint(f[1], 10, 64)
	if err != nil {
		return 0, false
	}
	return pages * uint64(os.Getpagesize()), true
}

// readConntrackCount returns the connection-tracking table's current size;
// false where the module is not loaded.
func readConntrackCount() (int64, bool) {
	return readProcInt("/proc/sys/net/netfilter/nf_conntrack_count")
}

func readProcInt(path string) (int64, bool) {
	data, err := os.ReadFile(path) //nolint:gosec // fixed /proc paths
	if err != nil {
		return 0, false
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	return n, err == nil
}

// clientHostSample builds one second's sample from two readings.
func clientHostSample(prev, cur hostCounters, sec float64, phase string, worstStop time.Duration) ClientHostSample {
	s := ClientHostSample{Sec: sec, Phase: phase, Goroutines: runtime.NumGoroutine(), WorstStopSec: worstStop.Seconds()}
	secs := cur.at.Sub(prev.at).Seconds()
	if prev.cpuOK && cur.cpuOK && cur.cpuTotal > prev.cpuTotal && cur.cpuIdle >= prev.cpuIdle {
		busy := 1 - float64(cur.cpuIdle-prev.cpuIdle)/float64(cur.cpuTotal-prev.cpuTotal)
		s.HostCPUBusy = &busy
	}
	if prev.selfOK && cur.selfOK && secs > 0 && cur.selfTicks >= prev.selfTicks {
		cores := float64(cur.selfTicks-prev.selfTicks) / clockTicks / secs
		s.DriverCPUCores = &cores
	}
	if v, ok := readMemAvailable(); ok {
		s.MemAvailableBytes = &v
	}
	if v, ok := readSelfRSS(); ok {
		s.DriverRSSBytes = &v
	}
	if inuse, tw, orphan, ok := readSockstat(); ok {
		s.TCPInUse, s.TCPTimeWait, s.TCPOrphan = &inuse, &tw, &orphan
	}
	if prev.snmpOK && cur.snmpOK {
		r, o, a, e := cur.retrans-prev.retrans, cur.opens-prev.opens, cur.afails-prev.afails, cur.resets-prev.resets
		s.RetransSegs, s.ActiveOpens, s.AttemptFails, s.EstabResets = &r, &o, &a, &e
	}
	if prev.synOK && cur.synOK {
		d := cur.synRetrans - prev.synRetrans
		s.SynRetrans = &d
	}
	if cur.failures != nil {
		s.Failures = make(map[string]int64, len(cur.failures))
		for k, v := range cur.failures {
			s.Failures[k] = v - prev.failures[k]
		}
	}
	return s
}

// ClientVerdict says whether the client host limited a storm, and why.
// Measured is false when no sample covered the window, which is not the same
// as "not limited".
type ClientVerdict struct {
	Measured bool     `json:"measured"`
	Limited  bool     `json:"limited"`
	Reasons  []string `json:"reasons"`
}

// Client-limited thresholds (WI 1480 plan, B5).
const (
	clientBusyLimit   = 0.9
	clientBusySeconds = 3
	clientStopLimit   = time.Second
)

// clientLimited decides the verdict over a run's client series, from the
// samples taken while the storm window was open. Retransmits are deliberately
// not a criterion: the brokers' accept pausing makes the client host
// retransmit SYNs by design, and that is the brokers' behaviour.
func clientLimited(series []ClientHostSample) ClientVerdict {
	v := ClientVerdict{Reasons: make([]string, 0)}
	var ports, files int64
	run, longest := 0, 0
	var worstStop float64
	for _, s := range series {
		if s.Phase != phaseWindow {
			continue
		}
		v.Measured = true
		ports += s.Failures[failAddrNotAvailable]
		files += s.Failures[failTooManyFiles]
		if s.HostCPUBusy != nil && *s.HostCPUBusy >= clientBusyLimit {
			run++
			longest = max(longest, run)
		} else {
			run = 0
		}
		worstStop = max(worstStop, s.WorstStopSec)
	}
	if ports > 0 {
		v.Reasons = append(v.Reasons, fmt.Sprintf("%d connects failed for want of a local port", ports))
	}
	if files > 0 {
		v.Reasons = append(v.Reasons, fmt.Sprintf("%d connects failed for want of a file descriptor", files))
	}
	if longest >= clientBusySeconds {
		v.Reasons = append(v.Reasons, fmt.Sprintf("the client host's CPU was at least %.0f%% busy for %d consecutive seconds", clientBusyLimit*100, longest))
	}
	if worstStop > clientStopLimit.Seconds() {
		v.Reasons = append(v.Reasons, fmt.Sprintf("the driver process was stopped for %.2f s", worstStop))
	}
	v.Limited = len(v.Reasons) > 0
	return v
}
