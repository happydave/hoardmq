package sim

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
)

// The failover measurement's evidence for WI 1470's open questions (WI
// 1474): how the storm's CPU divides between the simulated clients and the
// nodes, what the process was doing before the first fence, and whether a
// peer's frames were stuck unsent or unread.

// nodeLabels runs f with pprof labels naming a node. Goroutines created
// while f runs inherit them, so a node's accept loops, connection handlers
// and background loops are attributed to it in a CPU profile.
func nodeLabels(name string, f func()) {
	pprof.Do(context.Background(), pprof.Labels("sim", "node", "node", name), func(context.Context) { f() })
}

// clientLabels runs f with the label that marks the simulated clients.
func clientLabels(f func()) {
	pprof.Do(context.Background(), pprof.Labels("sim", "client"), func(context.Context) { f() })
}

// firstFence reports the first node, by name, whose fence count is above
// zero. It reads only the counts it is given.
func firstFence(counts map[string]int64) (string, bool) {
	names := make([]string, 0, len(counts))
	for n := range counts {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		if counts[n] > 0 {
			return n, true
		}
	}
	return "", false
}

// CPUProfileRecord says where the storm's CPU profile is, or why there is
// none.
type CPUProfileRecord struct {
	File          string  `json:"file,omitempty"`
	StartAfterSec float64 `json:"start_after_crash_sec"`
	StopAfterSec  float64 `json:"stop_after_crash_sec,omitempty"`
	Unavailable   string  `json:"unavailable,omitempty"`
}

// FenceTraceRecord says whether a trace window was taken at the first fence.
type FenceTraceRecord struct {
	File          string  `json:"file,omitempty"`
	Node          string  `json:"node,omitempty"`
	AfterCrashSec float64 `json:"after_crash_sec,omitempty"`
	Unavailable   string  `json:"unavailable,omitempty"`
}

// SocketSample is one TCP socket on a cluster port as `ss` reported it: the
// bytes in its receive queue the process has not read, and in its send
// queue the peer has not acknowledged.
type SocketSample struct {
	Local  string `json:"local"`
	Peer   string `json:"peer"`
	RecvQ  int    `json:"recv_q"`
	SendQ  int    `json:"send_q"`
	Listen bool   `json:"local_is_cluster_port"`
}

// PeerSample is one peer as a survivor's transport saw it.
type PeerSample struct {
	Name             string  `json:"name"`
	State            string  `json:"state"`
	DataState        string  `json:"data_state"`
	DownForMs        float64 `json:"down_for_ms,omitempty"`
	ReadIdleMs       float64 `json:"read_idle_ms"`
	DataReadIdleMs   float64 `json:"data_read_idle_ms"`
	OutQueued        int     `json:"out_queued"`
	UrgentQueued     int     `json:"urgent_queued"`
	DataOutQueued    int     `json:"data_out_queued"`
	DataUrgentQueued int     `json:"data_urgent_queued"`
}

// PeersSample is one second's reading: each survivor's peers, the cluster
// ports' sockets, and how long each read took. A read that took seconds is
// itself the finding — something held the lock it waited on.
type PeersSample struct {
	AfterCrashSec float64                 `json:"after_crash_sec"`
	Peers         map[string][]PeerSample `json:"peers"`
	PeersReadMs   map[string]float64      `json:"peers_read_ms"`
	Sockets       []SocketSample          `json:"sockets,omitempty"`
	SocketsReadMs float64                 `json:"sockets_read_ms"`
	SocketsError  string                  `json:"sockets_error,omitempty"`
	// SocketsDropped counts rows the cap discarded. A capture that is meant
	// to return a handful and returns thousands is a defect somewhere else,
	// and this is how the artifact says so instead of growing without limit.
	SocketsDropped int `json:"sockets_dropped,omitempty"`
}

// socketsPerSampleMax bounds one sample's socket rows. The filter is supposed
// to return the cluster's own connections — single digits per node — so this
// is generous by three orders of magnitude and only fires when something is
// wrong. It fired once: a drawn cluster port collided with the remote mode's
// fixed client port, the filter matched every client connection, and one
// 250,000-client run wrote 6,421,763 rows into a 937 MB artifact while
// spending 286 ms a second reading them (WI 1511).
const socketsPerSampleMax = 2000

// peerSamplesMax bounds the series at one sample a second over the window.
const peerSamplesMax = 720

// parseSS reads `ss -tnH` output: receive queue, send queue, local and peer
// address per line. A line it cannot read is skipped; the caller records the
// raw error if every line fails.
func parseSS(out string, clusterPorts map[string]bool) []SocketSample {
	var socks []SocketSample
	for _, line := range strings.Split(out, "\n") {
		f := strings.Fields(line)
		if len(f) < 4 {
			continue
		}
		// With a state filter, ss omits the state column: Recv-Q Send-Q Local Peer.
		recv, err1 := strconv.Atoi(f[0])
		send, err2 := strconv.Atoi(f[1])
		if err1 != nil || err2 != nil {
			continue
		}
		local, peer := f[2], f[3]
		socks = append(socks, SocketSample{Local: local, Peer: peer, RecvQ: recv, SendQ: send, Listen: clusterPorts[portOf(local)]})
	}
	return socks
}

func portOf(addr string) string {
	if i := strings.LastIndexByte(addr, ':'); i >= 0 {
		return addr[i+1:]
	}
	return ""
}

// readClusterSockets runs `ss` for established TCP sockets with either end on
// one of the given ports. The filter runs in the kernel, so the output is the
// cluster's handful of sockets however many client sockets exist.
func readClusterSockets(ports []string) ([]SocketSample, error) {
	if len(ports) == 0 {
		return nil, errors.New("no cluster ports")
	}
	var clauses []string
	set := map[string]bool{}
	for _, p := range ports {
		clauses = append(clauses, "sport = :"+p, "dport = :"+p)
		set[p] = true
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	args := []string{"-tnH", "state", "established", "( " + strings.Join(clauses, " or ") + " )"}
	out, err := exec.CommandContext(ctx, "ss", args...).Output() //nolint:gosec // fixed program; ports are the harness's own
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			return nil, fmt.Errorf("ss: %v: %s", err, strings.TrimSpace(string(ee.Stderr)))
		}
		return nil, fmt.Errorf("ss: %w", err)
	}
	return parseSS(string(out), set), nil
}

// capSockets bounds one sample's rows and reports how many it discarded, so a
// capture that has gone wrong says so instead of growing without limit.
func capSockets(socks []SocketSample) (kept []SocketSample, dropped int) {
	if len(socks) <= socketsPerSampleMax {
		return socks, 0
	}
	return socks[:socketsPerSampleMax], len(socks) - socketsPerSampleMax
}

// peerSource is what the sampler reads for one survivor.
type peerSource struct {
	name  string
	peers func() []transport.PeerInfo
}

// peerSampler takes one PeersSample a second until stopped, in its own
// goroutine, so a read that waits on a lock delays only the sampler.
type peerSampler struct {
	mu      sync.Mutex
	samples []PeersSample
	dropped int
	stop    chan struct{}
	done    chan struct{}
}

func startPeerSampler(crashAt time.Time, sources []peerSource, ports []string) *peerSampler {
	s := &peerSampler{stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(s.done)
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-s.stop:
				return
			case <-tick.C:
			}
			sample := PeersSample{AfterCrashSec: time.Since(crashAt).Seconds(), Peers: map[string][]PeerSample{}, PeersReadMs: map[string]float64{}}
			for _, src := range sources {
				began := time.Now()
				infos := src.peers()
				sample.PeersReadMs[src.name] = float64(time.Since(began)) / 1e6
				for _, p := range infos {
					sample.Peers[src.name] = append(sample.Peers[src.name], PeerSample{Name: p.Name, State: p.State.String(), DataState: p.DataState.String(),
						DownForMs: p.DownForMs, ReadIdleMs: p.ReadIdleMs, DataReadIdleMs: p.DataReadIdleMs,
						OutQueued: p.OutQueued, UrgentQueued: p.UrgentQueued, DataOutQueued: p.DataOutQueued, DataUrgentQueued: p.DataUrgentQueued})
				}
			}
			began := time.Now()
			socks, err := readClusterSockets(ports)
			sample.SocketsReadMs = float64(time.Since(began)) / 1e6
			sample.Sockets, sample.SocketsDropped = capSockets(socks)
			if err != nil {
				sample.SocketsError = err.Error()
			}
			s.mu.Lock()
			if len(s.samples) < peerSamplesMax {
				s.samples = append(s.samples, sample)
			} else {
				s.dropped++
			}
			s.mu.Unlock()
		}
	}()
	return s
}

// peerSamplerFinishWait bounds how long finish waits for a sample in
// progress: a read stuck on a lock must not hold the measurement.
const peerSamplerFinishWait = 30 * time.Second

// finish stops the sampler and returns the series, how many samples were not
// kept, and whether a sample was still being read when the wait ran out.
func (s *peerSampler) finish() (samples []PeersSample, dropped int, stuck bool) {
	select {
	case <-s.stop:
	default:
		close(s.stop)
	}
	select {
	case <-s.done:
	case <-time.After(peerSamplerFinishWait):
		stuck = true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]PeersSample(nil), s.samples...), s.dropped, stuck
}

// startCPUProfile starts the storm's CPU profile. The profile is kept in
// memory and written to dir/name when stopped, so a profile that never
// starts leaves no file behind. It never fails the measurement: a profile
// that cannot start or be written is recorded with the reason.
func startCPUProfile(dir, name string, afterCrash time.Duration) (stop func(afterCrash time.Duration) CPUProfileRecord, rec CPUProfileRecord) {
	rec = CPUProfileRecord{StartAfterSec: afterCrash.Seconds()}
	var buf bytes.Buffer
	if err := pprof.StartCPUProfile(&buf); err != nil {
		rec.Unavailable = "cpu profile not started: " + err.Error()
		return nil, rec
	}
	var once sync.Once
	return func(afterCrash time.Duration) CPUProfileRecord {
		once.Do(func() {
			pprof.StopCPUProfile()
			rec.StopAfterSec = afterCrash.Seconds()
			if err := os.WriteFile(filepath.Join(dir, name), buf.Bytes(), 0o640); err != nil { //nolint:gosec // operator-chosen artifact dir
				rec.Unavailable = "cpu profile write: " + err.Error()
				return
			}
			rec.File = name
		})
		return rec
	}, rec
}
