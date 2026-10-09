package sim

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
)

// A remote-client run starts only on a client host that cannot itself be the
// limit it is meant to rule out. Each condition is a pure predicate over facts
// read beforehand, so its test can hand it the values it exists to refuse.

// clientStartFacts is what the driver reads about its host before a run.
// A *Known flag false means the value could not be read.
type clientStartFacts struct {
	SelfPID int
	SelfExe string
	// Procs maps each process whose executable link could be read to it.
	Procs map[int]string

	TCPInUseTW int64
	TCPKnown   bool

	OpenFiles uint64

	MemAvailableBytes uint64
	MemKnown          bool

	Load1     float64
	LoadKnown bool

	// Addrs are the host's interface addresses.
	Addrs []net.IP

	PortLo, PortHi int
	PortsKnown     bool
}

// clientStartWant is what the run needs.
type clientStartWant struct {
	Clients        int
	Sources        []string
	BytesPerClient uint64
}

const (
	clientSettledSockets = 2000
	clientFileMargin     = 1000
	clientMinMemory      = 16 << 30
	clientMaxLoad1       = 4
)

// strayDrivers returns the other processes running the driver's executable.
// A driver whose binary was replaced by a later deploy still counts: the
// kernel then reports its link with a " (deleted)" suffix.
func strayDrivers(self int, exe string, procs map[int]string) []int {
	var out []int
	for pid, path := range procs {
		if pid != self && strings.TrimSuffix(path, " (deleted)") == exe {
			out = append(out, pid)
		}
	}
	sort.Ints(out)
	return out
}

// sourcePresent reports whether an address is one of the host's. A loopback
// address is present whenever the host has a loopback interface address,
// since the whole loopback network routes to it.
func sourcePresent(source string, addrs []net.IP) bool {
	ip := net.ParseIP(source)
	if ip == nil {
		return false
	}
	for _, a := range addrs {
		if a.Equal(ip) || (ip.IsLoopback() && a.IsLoopback()) {
			return true
		}
	}
	return false
}

// portBudgetOK reports whether the clients sharing one source address fit in
// half the ephemeral range. It is the worst case: every client on a source
// connected to the same node, which a failover approaches as the survivors
// take the crashed node's clients.
func portBudgetOK(clients, sources, lo, hi int) (perSource, budget int, ok bool) {
	if sources <= 0 {
		return clients, (hi - lo + 1) / 2, false
	}
	perSource = (clients + sources - 1) / sources
	budget = (hi - lo + 1) / 2
	return perSource, budget, perSource <= budget
}

// clientStartProblems names every unmet starting condition with the value
// seen. Empty means the run may start.
func clientStartProblems(f clientStartFacts, w clientStartWant) []string {
	var out []string
	if stray := strayDrivers(f.SelfPID, f.SelfExe, f.Procs); len(stray) > 0 {
		out = append(out, fmt.Sprintf("another driver is running: pid %v", stray))
	}
	switch {
	case !f.TCPKnown:
		out = append(out, "TCP socket count unreadable")
	case f.TCPInUseTW > clientSettledSockets:
		out = append(out, fmt.Sprintf("not settled: %d TCP sockets in use or TIME-WAIT (limit %d)", f.TCPInUseTW, clientSettledSockets))
	}
	if need := uint64(w.Clients) + clientFileMargin; f.OpenFiles < need {
		out = append(out, fmt.Sprintf("open-file limit %d, need %d", f.OpenFiles, need))
	}
	switch {
	case !f.MemKnown:
		out = append(out, "MemAvailable unreadable")
	default:
		need := uint64(clientMinMemory)
		if w.BytesPerClient > 0 {
			need = max(need, uint64(w.Clients)*w.BytesPerClient*3)
		}
		if f.MemAvailableBytes < need {
			out = append(out, fmt.Sprintf("MemAvailable %d bytes, need %d", f.MemAvailableBytes, need))
		}
	}
	switch {
	case !f.LoadKnown:
		out = append(out, "load average unreadable")
	case f.Load1 > clientMaxLoad1:
		out = append(out, fmt.Sprintf("1-minute load %.2f, limit %d", f.Load1, clientMaxLoad1))
	}
	for _, s := range w.Sources {
		if !sourcePresent(s, f.Addrs) {
			out = append(out, fmt.Sprintf("source address %s is not on this host", s))
		}
	}
	switch {
	case !f.PortsKnown:
		out = append(out, "ephemeral port range unreadable")
	default:
		if per, budget, ok := portBudgetOK(w.Clients, len(w.Sources), f.PortLo, f.PortHi); !ok {
			out = append(out, fmt.Sprintf("%d clients per source address exceed half the ephemeral range %d-%d (%d)", per, f.PortLo, f.PortHi, budget))
		}
	}
	return out
}

// readClientStartFacts reads the facts on this host. It only reads.
func readClientStartFacts() clientStartFacts {
	f := clientStartFacts{SelfPID: os.Getpid(), Procs: map[int]string{}}
	if exe, err := os.Executable(); err == nil {
		f.SelfExe = exe
	}
	if entries, err := os.ReadDir("/proc"); err == nil {
		for _, e := range entries {
			pid, err := strconv.Atoi(e.Name())
			if err != nil {
				continue
			}
			if path, err := os.Readlink(filepath.Join("/proc", e.Name(), "exe")); err == nil {
				f.Procs[pid] = path
			}
		}
	}
	if inuse, tw, _, ok := readSockstat(); ok {
		f.TCPInUseTW, f.TCPKnown = inuse+tw, true
	}
	var lim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &lim); err == nil {
		f.OpenFiles = lim.Cur
	}
	f.MemAvailableBytes, f.MemKnown = readMemAvailable()
	if data, err := os.ReadFile("/proc/loadavg"); err == nil {
		if fields := strings.Fields(string(data)); len(fields) > 0 {
			if v, err := strconv.ParseFloat(fields[0], 64); err == nil {
				f.Load1, f.LoadKnown = v, true
			}
		}
	}
	if addrs, err := net.InterfaceAddrs(); err == nil {
		for _, a := range addrs {
			if n, ok := a.(*net.IPNet); ok {
				f.Addrs = append(f.Addrs, n.IP)
			}
		}
	}
	f.PortLo, f.PortHi, f.PortsKnown = readEphemeralRange()
	return f
}

func readEphemeralRange() (lo, hi int, ok bool) {
	data, err := os.ReadFile("/proc/sys/net/ipv4/ip_local_port_range")
	if err != nil {
		return 0, 0, false
	}
	if n, _ := fmt.Sscan(string(data), &lo, &hi); n == 2 && lo > 0 && hi >= lo {
		return lo, hi, true
	}
	return 0, 0, false
}
