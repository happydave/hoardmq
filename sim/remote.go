package sim

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// The remote-client mode's settings. HOARDMQ_SIM_CLIENT_COMMAND selects the
// mode; the other two are then required.
const (
	clientCommandEnv = "HOARDMQ_SIM_CLIENT_COMMAND"
	nodeAddrsEnv     = "HOARDMQ_SIM_NODE_ADDRS"
	clientSourcesEnv = "HOARDMQ_SIM_CLIENT_SOURCES"
	clientBytesEnv   = "HOARDMQ_SIM_CLIENT_BYTES"
)

// remoteSettings is a remote-client run's configuration.
type remoteSettings struct {
	Command        []string
	Nodes          []string
	Sources        []string
	BytesPerClient uint64
}

// readRemoteSettings returns nil when the remote mode is not selected.
func readRemoteSettings(getenv func(string) string, nodes int) (*remoteSettings, error) {
	command := strings.Fields(getenv(clientCommandEnv))
	if len(command) == 0 {
		return nil, nil
	}
	s := &remoteSettings{Command: command}
	for _, a := range splitList(getenv(nodeAddrsEnv)) {
		host, port, err := net.SplitHostPort(a)
		if err != nil || net.ParseIP(host) == nil || port == "" {
			return nil, fmt.Errorf("%s: %q is not an IP address and port", nodeAddrsEnv, a)
		}
		s.Nodes = append(s.Nodes, a)
	}
	if len(s.Nodes) != nodes {
		return nil, fmt.Errorf("%s: want %d node addresses, got %d", nodeAddrsEnv, nodes, len(s.Nodes))
	}
	seen := map[string]bool{}
	for _, a := range s.Nodes {
		if seen[a] {
			return nil, fmt.Errorf("%s: %s is given twice", nodeAddrsEnv, a)
		}
		seen[a] = true
	}
	for _, a := range splitList(getenv(clientSourcesEnv)) {
		if net.ParseIP(a) == nil {
			return nil, fmt.Errorf("%s: %q is not an IP address", clientSourcesEnv, a)
		}
		s.Sources = append(s.Sources, a)
	}
	if len(s.Sources) == 0 {
		return nil, fmt.Errorf("%s: the remote mode needs at least one source address", clientSourcesEnv)
	}
	if v := getenv(clientBytesEnv); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", clientBytesEnv, err)
		}
		s.BytesPerClient = n
	}
	return s, nil
}

func splitList(v string) []string {
	var out []string
	for _, f := range strings.Split(v, ",") {
		if f = strings.TrimSpace(f); f != "" {
			out = append(out, f)
		}
	}
	return out
}

// destination is the part of the command a report may name: the argument
// that looks like user@host, or the program when there is none. An identity
// file's path is never recorded as the destination.
func (s *remoteSettings) destination() string {
	for _, a := range s.Command[1:] {
		if strings.Contains(a, "@") && !strings.HasPrefix(a, "-") {
			return a
		}
	}
	return s.Command[0]
}

// serverStartProblems are the broker host's own starting conditions for a
// remote-client run: its connection-tracking table must have room, and every
// node address must be on the host before a node binds it.
func serverStartProblems(clients int, conntrackCount, conntrackMax int64, conntrackKnown bool, nodes []string, addrs []net.IP) []string {
	var out []string
	if conntrackKnown {
		if conntrackCount > conntrackStartLimit {
			out = append(out, fmt.Sprintf("connection tracking holds %d entries (limit %d)", conntrackCount, conntrackStartLimit))
		}
		if conntrackMax < 4*int64(clients) {
			out = append(out, fmt.Sprintf("nf_conntrack_max %d is under four times %d clients", conntrackMax, clients))
		}
	}
	for _, n := range nodes {
		host, _, _ := net.SplitHostPort(n)
		if !sourcePresent(host, addrs) {
			out = append(out, fmt.Sprintf("node address %s is not on this host", host))
		}
	}
	return out
}

func readServerStartProblems(clients int, nodes []string) []string {
	count, okCount := readConntrackCount()
	maxEntries, okMax := readProcInt("/proc/sys/net/netfilter/nf_conntrack_max")
	var addrs []net.IP
	if as, err := net.InterfaceAddrs(); err == nil {
		for _, a := range as {
			if n, ok := a.(*net.IPNet); ok {
				addrs = append(addrs, n.IP)
			}
		}
	}
	return serverStartProblems(clients, count, maxEntries, okCount && okMax, nodes, addrs)
}

// driverBuildProblem refuses, for a recorded run, a driver built from another
// commit or from a modified tree: the two halves of one measurement must be
// the same source.
func driverBuildProblem(record bool, tree Tree, info DriverInfo) string {
	if !record {
		return ""
	}
	switch {
	case info.Revision == "":
		return "the driver's build records no commit"
	case info.Revision != tree.Commit:
		return fmt.Sprintf("the driver was built from %s, the test from %s", info.Revision, tree.Commit)
	case info.Modified == nil || *info.Modified:
		return "the driver was built from a modified tree"
	}
	return ""
}

// remoteRecord is what a remote-client run adds to run.json.
type remoteRecord struct {
	Destination    string             `json:"destination"`
	Nodes          []string           `json:"node_addresses"`
	Sources        []string           `json:"sources"`
	Driver         DriverInfo         `json:"driver"`
	Series         []ClientHostSample `json:"series"`
	SeriesDropped  int                `json:"series_dropped,omitempty"`
	BulkDropped    int64              `json:"bulk_dropped"`
	MissingSources []string           `json:"missing_sources"`
	Verdict        ClientVerdict      `json:"client_limited"`
	RecoveryNote   string             `json:"recovery_resolution"`
}

// remoteOptions bound the test side's patience.
type remoteOptions struct {
	// silence is how long the driver may send nothing before it is taken for
	// dead. It sends host figures every second from ready onwards.
	silence time.Duration
	// startWait is how long the driver may wait for its host to settle.
	startWait time.Duration
	// closeWait bounds the driver's teardown.
	closeWait time.Duration
}

func defaultRemoteOptions() remoteOptions {
	return remoteOptions{silence: 30 * time.Second, startWait: 10 * time.Minute, closeWait: 3 * time.Minute}
}

// remotePopulation is the test's side of the driver protocol.
type remotePopulation struct {
	settings remoteSettings
	clients  int
	opts     remoteOptions

	in   io.WriteCloser // the driver's stdin
	enc  *json.Encoder
	cmd  *exec.Cmd // nil when driven over pipes in a test
	tail *lineTail

	reliable chan driverMsg
	readDone chan struct{}
	readErr  atomic.Value // string

	lastMsg   atomic.Int64                 // unix nanoseconds
	completed atomic.Pointer[atomic.Int64] // the test's fill counter, set by fill

	mu        sync.Mutex
	latest    popSample
	haveLast  bool
	series    []ClientHostSample
	seriesOut int

	info          DriverInfo
	failed        int
	perNodeCounts map[string]int
	summary       driverSummary
	closed        bool
}

const remoteSeriesMax = 7200

// lineTail keeps the last lines written to it: the driver's stderr, quoted
// when the driver fails.
type lineTail struct {
	mu    sync.Mutex
	lines []string
	part  string
}

func (l *lineTail) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.part += string(p)
	for {
		i := strings.IndexByte(l.part, '\n')
		if i < 0 {
			break
		}
		l.lines = append(l.lines, l.part[:i])
		if len(l.lines) > 50 {
			l.lines = l.lines[len(l.lines)-50:]
		}
		l.part = l.part[i+1:]
	}
	return len(p), nil
}

func (l *lineTail) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(append(append([]string(nil), l.lines...), l.part), "\n")
}

// startRemotePopulation starts the driver command and says hello. The
// returned error carries the driver's stderr.
func startRemotePopulation(s remoteSettings, clients int, opts remoteOptions) (*remotePopulation, error) {
	cmd := exec.Command(s.Command[0], s.Command[1:]...) //nolint:gosec // operator-chosen command
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	tail := &lineTail{}
	cmd.Stderr = tail
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("starting the client driver: %w", err)
	}
	p := newRemotePopulation(s, clients, opts, stdin, stdout)
	p.cmd, p.tail = cmd, tail
	return p, nil
}

// newRemotePopulation speaks the protocol over the given streams.
func newRemotePopulation(s remoteSettings, clients int, opts remoteOptions, in io.WriteCloser, out io.Reader) *remotePopulation {
	p := &remotePopulation{settings: s, clients: clients, opts: opts, in: in, enc: json.NewEncoder(in), tail: &lineTail{},
		reliable: make(chan driverMsg, 64), readDone: make(chan struct{})}
	p.lastMsg.Store(time.Now().UnixNano())
	go p.read(bufio.NewReaderSize(out, 1<<20))
	return p
}

func (p *remotePopulation) read(r io.Reader) {
	defer close(p.readDone)
	dec := json.NewDecoder(r)
	for {
		var m driverMsg
		if err := dec.Decode(&m); err != nil {
			if !errors.Is(err, io.EOF) {
				p.readErr.Store(err.Error())
			} else {
				p.readErr.Store("the driver closed its output")
			}
			return
		}
		now := time.Now()
		p.lastMsg.Store(now.UnixNano())
		switch m.Type {
		case msgSample, msgCross:
			// A reading's At is when it arrived, and the test's watch only
			// advances when At does — so a driver that has stopped sending
			// cannot complete a hold by having its last reading read again.
			p.mu.Lock()
			p.latest = popSample{Online: m.Online, Recovered: m.Recovered, Refusals: m.Refusals, Fails: m.Fails, At: now}
			p.haveLast = true
			p.mu.Unlock()
		case msgHost:
			if m.Host == nil {
				continue
			}
			if n, ok := readConntrackCount(); ok {
				m.Host.ServerConntrack = &n
			}
			p.mu.Lock()
			if len(p.series) < remoteSeriesMax {
				p.series = append(p.series, *m.Host)
			} else {
				p.seriesOut++
			}
			p.mu.Unlock()
		case msgProgress:
			if c := p.completed.Load(); c != nil {
				c.Store(m.Completed)
			}
		default:
			p.reliable <- m
		}
	}
}

// failure is the driver's reason for failing and its stderr's last lines.
func (p *remotePopulation) failure(what string) error {
	why, _ := p.readErr.Load().(string)
	if why == "" {
		why = "no reason given"
	}
	return fmt.Errorf("client driver: %s (%s); driver stderr:\n%s", what, why, p.tail.String())
}

// await waits for one reliable message of the given type, failing on a
// refusal, a closed stream, another reliable message, or silence.
func (p *remotePopulation) await(want string, limit time.Duration) (driverMsg, error) {
	deadline := time.Now().Add(limit)
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case m := <-p.reliable:
			switch m.Type {
			case want:
				return m, nil
			case msgRefused:
				return m, p.failure("refused: " + strings.Join(m.Reasons, "; "))
			default:
				return m, p.failure(fmt.Sprintf("sent %q while the test waited for %q", m.Type, want))
			}
		case <-p.readDone:
			select {
			case m := <-p.reliable:
				if m.Type == want {
					return m, nil
				}
				return m, p.failure(fmt.Sprintf("ended after %q while the test waited for %q", m.Type, want))
			default:
			}
			return driverMsg{}, p.failure("ended while the test waited for " + want)
		case <-tick.C:
			if err := p.alive(); err != nil {
				return driverMsg{}, err
			}
			if time.Now().After(deadline) {
				return driverMsg{}, p.failure(fmt.Sprintf("no %q within %s", want, limit))
			}
		}
	}
}

// alive fails once the driver has been silent past the bound.
func (p *remotePopulation) alive() error {
	if quiet := time.Since(time.Unix(0, p.lastMsg.Load())); quiet > p.opts.silence {
		return p.failure(fmt.Sprintf("silent for %s", quiet.Round(time.Second)))
	}
	return nil
}

func (p *remotePopulation) send(m driverMsg) error {
	if err := p.enc.Encode(m); err != nil {
		return p.failure("writing " + m.Type + ": " + err.Error())
	}
	return nil
}

// hello introduces the run and waits for the driver's host to be ready.
func (p *remotePopulation) hello() (DriverInfo, error) {
	if err := p.send(driverMsg{Type: msgHello, Clients: p.clients, Nodes: p.settings.Nodes, Sources: p.settings.Sources,
		BytesPerClient: p.settings.BytesPerClient, WaitMs: p.opts.startWait.Milliseconds()}); err != nil {
		return DriverInfo{}, err
	}
	// Before ready the driver sends nothing while it waits, so silence is
	// not failure here; the bound is its own wait plus reachability.
	saved := p.opts.silence
	p.opts.silence = p.opts.startWait + 2*time.Minute
	m, err := p.await(msgReady, p.opts.startWait+2*time.Minute)
	p.opts.silence = saved
	p.lastMsg.Store(time.Now().UnixNano())
	if err != nil {
		return DriverInfo{}, err
	}
	if m.Driver != nil {
		p.info = *m.Driver
	}
	return p.info, nil
}

func (p *remotePopulation) fill(ctx context.Context, completed *atomic.Int64) (int, error) {
	p.completed.Store(completed)
	if err := p.send(driverMsg{Type: msgFill}); err != nil {
		return 0, err
	}
	limit := time.Hour
	if d, ok := ctx.Deadline(); ok {
		limit = time.Until(d) + time.Minute
	}
	m, err := p.await(msgFilled, limit)
	if err != nil {
		return 0, err
	}
	completed.Store(m.Completed)
	p.failed = m.Failed
	p.mu.Lock()
	p.perNodeCounts = m.PerNode
	p.mu.Unlock()
	return m.Failed, nil
}

func (p *remotePopulation) perNode() (map[string]int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[string]int, len(p.perNodeCounts))
	for k, v := range p.perNodeCounts {
		out[k] = v
	}
	return out, nil
}

func (p *remotePopulation) mark() error {
	if err := p.send(driverMsg{Type: msgMark}); err != nil {
		return err
	}
	m, err := p.await(msgMarked, time.Minute)
	if err != nil {
		return err
	}
	p.mu.Lock()
	p.latest = popSample{Online: m.Online, Refusals: m.Refusals, Fails: m.Fails, At: time.Now()}
	p.haveLast = true
	p.mu.Unlock()
	return nil
}

func (p *remotePopulation) crashed() error {
	return p.send(driverMsg{Type: msgCrashed})
}

func (p *remotePopulation) sample() (popSample, error) {
	if err := p.alive(); err != nil {
		return popSample{}, err
	}
	select {
	case m := <-p.reliable:
		return popSample{}, p.failure(fmt.Sprintf("sent %q during the window", m.Type))
	default:
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.haveLast {
		return popSample{}, p.failure("no reading of the clients before the window")
	}
	return p.latest, nil
}

func (p *remotePopulation) finish() (popSummary, error) {
	if err := p.send(driverMsg{Type: msgFinish}); err != nil {
		return popSummary{}, err
	}
	m, err := p.await(msgSummary, 10*time.Minute)
	if err != nil {
		return popSummary{}, err
	}
	if m.Summary == nil {
		return popSummary{}, p.failure("sent a summary with no content")
	}
	p.summary = *m.Summary
	return summaryFromDriver(*m.Summary), nil
}

// summaryFromDriver converts the driver's wire summary into the one the
// measurement reads. It is a separate function so a field can be tested to
// cross the socket: a figure added to popSummary but not to this conversion
// reads zero on exactly the runs the published sweeps use — the remote ones —
// and a zero is indistinguishable from a healthy run (WI 1509).
func summaryFromDriver(ds driverSummary) popSummary {
	out := popSummary{DroppedClients: ds.DroppedClients, Refusals: ds.Refusals, Drops: ds.Drops,
		Fails: ds.Fails, SubRefusals: ds.SubRefusals, SubRefusedClients: ds.SubRefusedClients,
		SessionLost: ds.SessionLost, SessionLostClients: ds.SessionLostClients, SubDowngraded: ds.SubDowngraded}
	// Copied by index rather than assigned, so a driver from an older build
	// sending fewer buckets fills what it has instead of failing to decode.
	for i := 0; i < len(ds.DropCauses) && i < len(out.DropCauses); i++ {
		out.DropCauses[i] = ds.DropCauses[i]
	}
	for _, ns := range ds.ReconnectsNs {
		out.Reconnects = append(out.Reconnects, time.Duration(ns))
	}
	return out
}

// close ends the driver: close, then closed and exit within the bound, then
// the driver's stdin, then the local process. A remote process is never
// signalled; losing its stdin ends it.
func (p *remotePopulation) close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.mu.Unlock()
	if err := p.send(driverMsg{Type: msgClose}); err == nil {
		_, _ = p.await(msgClosed, p.opts.closeWait)
	}
	_ = p.in.Close()
	if p.cmd == nil {
		return
	}
	exited := make(chan struct{})
	go func() { _ = p.cmd.Wait(); close(exited) }()
	select {
	case <-exited:
	case <-time.After(30 * time.Second):
		_ = p.cmd.Process.Kill()
		<-exited
	}
}

// record is the run's remote section.
func (p *remotePopulation) record() *remoteRecord {
	p.mu.Lock()
	defer p.mu.Unlock()
	series := append([]ClientHostSample(nil), p.series...)
	r := &remoteRecord{Destination: p.settings.destination(), Nodes: p.settings.Nodes, Sources: p.settings.Sources, Driver: p.info,
		Series: series, SeriesDropped: p.seriesOut, BulkDropped: p.summary.BulkDropped, MissingSources: p.summary.MissingSources,
		Verdict:      clientLimited(series),
		RecoveryNote: "full recovery is the time the test received the driver's allback (or a sample at target) minus the crash, on the test's clock: one 50 ms sample plus the channel's delay"}
	if r.MissingSources == nil {
		r.MissingSources = make([]string, 0)
	}
	return r
}

// topologyLine is the report's description of where nodes and clients ran.
// The in-process wording is the one every earlier report carries.
func topologyLine(remote *remoteSettings) string {
	if remote == nil {
		return "Three in-process nodes over loopback"
	}
	return fmt.Sprintf("Three in-process nodes listening on %s, clients in a separate driver process at %s", strings.Join(remote.Nodes, ", "), remote.destination())
}

// caveatLine is the report's first caveat, which depends on where the
// clients ran.
func caveatLine(remote *remoteSettings) string {
	if remote == nil {
		return "one machine, one kernel, loopback; the clients share the CPU with\nthe brokers"
	}
	return "the clients run on another host over the network, so their timing\nincludes it; the client host's own limits are reported below"
}

// remoteSection is the report's account of the client host, or nothing for
// an in-process run.
func remoteSection(r *remoteRecord) string {
	if r == nil {
		return ""
	}
	var b strings.Builder
	b.WriteString("\n## Client host\n\n")
	verdict := "not limited"
	switch {
	case !r.Verdict.Measured:
		verdict = "not measured: no client-host sample covered the window"
	case r.Verdict.Limited:
		verdict = "**limited**: " + strings.Join(r.Verdict.Reasons, "; ")
	}
	fmt.Fprintf(&b, "Driver at `%s` (%s, %d CPUs, kernel %s, ephemeral ports %d-%d, open files %d), built from `%s`. Sources %s.\n\n",
		r.Destination, r.Driver.Hostname, r.Driver.NumCPU, r.Driver.Kernel, r.Driver.PortLo, r.Driver.PortHi, r.Driver.OpenFiles, r.Driver.Revision, strings.Join(r.Sources, ", "))
	fmt.Fprintf(&b, "Whether the client host limited the storm: %s.\n\n", verdict)
	var busy, cores, stop float64
	var rss uint64
	var retrans, syn, twPeak int64
	failures := map[string]int64{}
	for _, s := range r.Series {
		if s.Phase != phaseWindow {
			continue
		}
		if s.HostCPUBusy != nil {
			busy = max(busy, *s.HostCPUBusy)
		}
		if s.DriverCPUCores != nil {
			cores = max(cores, *s.DriverCPUCores)
		}
		if s.DriverRSSBytes != nil {
			rss = max(rss, *s.DriverRSSBytes)
		}
		if s.RetransSegs != nil {
			retrans += *s.RetransSegs
		}
		if s.SynRetrans != nil {
			syn += *s.SynRetrans
		}
		if s.TCPTimeWait != nil {
			twPeak = max(twPeak, *s.TCPTimeWait)
		}
		stop = max(stop, s.WorstStopSec)
		for k, v := range s.Failures {
			failures[k] += v
		}
	}
	b.WriteString("| Client host, over the window | Value |\n|---|---|\n")
	fmt.Fprintf(&b, "| Peak CPU busy, all CPUs | %.0f%% |\n", busy*100)
	fmt.Fprintf(&b, "| Peak driver CPU | %.2f cores |\n", cores)
	fmt.Fprintf(&b, "| Worst driver stop | %.2f s |\n", stop)
	fmt.Fprintf(&b, "| Peak driver RSS | %d MiB |\n", rss>>20)
	fmt.Fprintf(&b, "| Peak TIME-WAIT sockets | %d |\n", twPeak)
	fmt.Fprintf(&b, "| Segments retransmitted / SYN retransmits | %d / %d |\n", retrans, syn)
	for _, k := range failureClasses {
		fmt.Fprintf(&b, "| Connects failed: %s | %d |\n", k, failures[k])
	}
	fmt.Fprintf(&b, "\nSamples or host figures dropped on the control channel: %d. Sources missing at finish: %d. %s.\n", r.BulkDropped, len(r.MissingSources), r.RecoveryNote)
	return b.String()
}

// writeReconnects writes every per-client reconnect time, in milliseconds.
func writeReconnects(path string, times []time.Duration) error {
	ms := make([]float64, 0, len(times))
	for _, d := range times {
		ms = append(ms, float64(d.Microseconds())/1000)
	}
	raw, err := json.Marshal(struct {
		Unit       string    `json:"unit"`
		Reconnects []float64 `json:"reconnects"`
	}{"ms", ms})
	if err != nil {
		return err
	}
	return os.WriteFile(path, raw, 0o640) //nolint:gosec // operator-chosen dir
}
