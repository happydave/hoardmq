package sim

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/happydave/hoardmq/internal/testcert"
)

// scaleConfig is one run's settings, from the environment and the layout.
type scaleConfig struct {
	Layout      scaleLayout   `json:"layout"`
	Clients     int           `json:"clients"`
	Queue       queueRequest  `json:"queue"`
	Victim      string        `json:"victim"`
	Hold        time.Duration `json:"-"`
	HoldSec     float64       `json:"hold_sec"`
	NodeBytes   int64         `json:"node_bytes"`
	DriverBytes int64         `json:"driver_bytes"`
	// Window bounds the storm's recovery instant; Settle bounds the idle and
	// queue phases; Rejoin bounds the restarted node's rejoin.
	WindowSec float64 `json:"window_sec"`
	SettleSec float64 `json:"settle_sec"`
	RejoinSec float64 `json:"rejoin_sec"`
	// ScrapeSec is how often the settle and checkpoint phases scrape every
	// node's /metrics (the rejoin twice as often). A scrape walks every
	// session record, so at a million sessions it is part of the load.
	ScrapeSec float64 `json:"scrape_sec"`
	// IdleHoldSec, when set, is the least time the idle phase lasts from the
	// fill's end, settled or not, so the connections stay idle for longer
	// than the transport's keepalive takes to give up on a peer.
	IdleHoldSec float64 `json:"idle_hold_sec,omitempty"`
	// DriverTCPKeepAlive is the clients' TCP keepalive: "default" (Go's, a
	// probe every 15 s on an idle connection) or "off". One client host
	// stands in for many, so its probes and their answers all cross one
	// network card.
	DriverTCPKeepAlive string `json:"driver_tcp_keepalive"`
	// ClientKeepAliveSec is the MQTT keep alive the clients ask for, each
	// sending a PINGREQ that often; 0, by default, asks for none and sends
	// nothing while idle.
	ClientKeepAliveSec int `json:"client_keep_alive_sec"`
	// DeliveryCheck is the delivery check after the storm: "off" (none),
	// "all", or the size of a seeded sample; DeliverySample is that size, 0
	// for every client.
	DeliveryCheck  string `json:"delivery_check"`
	DeliverySample int    `json:"-"`
}

const (
	scaleScrapeEnv    = "HOARDMQ_SIM_SCALE_SCRAPE_SEC"
	scaleIdleHoldEnv  = "HOARDMQ_SIM_SCALE_IDLE_HOLD_SEC"
	scaleKeepAliveEnv = "HOARDMQ_SIM_SCALE_DRIVER_TCP_KEEPALIVE"
	scaleClientKAEnv  = "HOARDMQ_SIM_SCALE_CLIENT_KEEP_ALIVE"
	scaleDeliveryEnv  = "HOARDMQ_SIM_SCALE_DELIVERY_CHECK"
)

// The delivery check's fixed settings, recorded with its result: the sample's
// seed, the negative controls asked for, and the wait for the messages.
const (
	deliverySeed     = 1
	deliveryControls = 10
	deliveryWait     = 30 * time.Second
)

// deliveryOn says whether the run checks delivery after the storm.
func (c scaleConfig) deliveryOn() bool { return c.DeliveryCheck != "off" }

func (c scaleConfig) scrapeEvery() time.Duration {
	return time.Duration(c.ScrapeSec * float64(time.Second))
}

// readScaleConfig reads the run's settings; ok is false when the layout
// variable is unset, which leaves the measurement unselected.
func readScaleConfig(getenv func(string) string) (scaleConfig, bool, error) {
	path := getenv(scaleLayoutEnv)
	if path == "" {
		return scaleConfig{}, false, nil
	}
	c := scaleConfig{Victim: "n2", Hold: holdDefault, NodeBytes: 48 << 10, DriverBytes: 32 << 10,
		WindowSec: 900, SettleSec: 300, RejoinSec: 900, ScrapeSec: 10, DriverTCPKeepAlive: "default", DeliveryCheck: "off"}
	data, err := os.ReadFile(path) //nolint:gosec // the operator's layout
	if err != nil {
		return c, true, err
	}
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c.Layout); err != nil {
		return c, true, fmt.Errorf("%s: %w", path, err)
	}
	if c.Clients, err = strconv.Atoi(getenv(scaleClientsEnv)); err != nil || c.Clients <= 0 {
		return c, true, fmt.Errorf("%s: want a positive client count, got %q", scaleClientsEnv, getenv(scaleClientsEnv))
	}
	c.Queue = queueRequest{Sessions: max(1, c.Clients/10), Depth: 10, PayloadBytes: 64}
	switch q := getenv(scaleQueueEnv); q {
	case "":
	case "off":
		c.Queue = queueRequest{}
	default:
		var s, d int
		if n, _ := fmt.Sscanf(q, "%dx%d", &s, &d); n != 2 || s <= 0 || d <= 0 || s > c.Clients {
			return c, true, fmt.Errorf("%s: want <sessions>x<depth> or off, got %q", scaleQueueEnv, q)
		}
		c.Queue.Sessions, c.Queue.Depth = s, d
	}
	if v := getenv(scaleVictimEnv); v != "" {
		c.Victim = v
	}
	for _, kv := range []struct {
		env string
		to  *int64
	}{{scaleNodeBytesEnv, &c.NodeBytes}, {scaleDrvBytesEnv, &c.DriverBytes}} {
		if v := getenv(kv.env); v != "" {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil || n <= 0 {
				return c, true, fmt.Errorf("%s: %q", kv.env, v)
			}
			*kv.to = n
		}
	}
	if v := getenv("HOARDMQ_SIM_HOLD_SEC"); v != "" {
		s, err := strconv.ParseFloat(v, 64)
		if err != nil || s <= 0 {
			return c, true, fmt.Errorf("HOARDMQ_SIM_HOLD_SEC: %q", v)
		}
		c.Hold = time.Duration(s * float64(time.Second))
	}
	c.HoldSec = c.Hold.Seconds()
	if v := getenv(scaleScrapeEnv); v != "" {
		s, err := strconv.ParseFloat(v, 64)
		if err != nil || s < 1 || s > 60 {
			return c, true, fmt.Errorf("%s: want 1 to 60 seconds, got %q", scaleScrapeEnv, v)
		}
		c.ScrapeSec = s
	}
	if v := getenv(scaleIdleHoldEnv); v != "" {
		s, err := strconv.ParseFloat(v, 64)
		if err != nil || s < 1 || s > 3600 {
			return c, true, fmt.Errorf("%s: want 1 to 3600 seconds, got %q", scaleIdleHoldEnv, v)
		}
		c.IdleHoldSec = s
	}
	switch v := getenv(scaleKeepAliveEnv); v {
	case "", "default":
	case "off":
		c.DriverTCPKeepAlive = v
	default:
		return c, true, fmt.Errorf("%s: want default or off, got %q", scaleKeepAliveEnv, v)
	}
	if v := getenv(scaleClientKAEnv); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 || n > 65535 {
			return c, true, fmt.Errorf("%s: want 0 to 65535 seconds, got %q", scaleClientKAEnv, v)
		}
		c.ClientKeepAliveSec = n
	}
	switch v := getenv(scaleDeliveryEnv); v {
	case "", "off":
	case "all":
		c.DeliveryCheck = v
	default:
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 || n > c.Clients {
			return c, true, fmt.Errorf("%s: want off, all, or a sample of 1 to %d clients, got %q", scaleDeliveryEnv, c.Clients, v)
		}
		c.DeliveryCheck, c.DeliverySample = strconv.Itoa(n), n
	}
	return c, true, nil
}

// The record of one run, written to run.json.
type scaleRecord struct {
	RunID      string               `json:"run_id"`
	Config     scaleConfig          `json:"config"`
	Provenance Provenance           `json:"provenance"`
	Binaries   []binaryInfo         `json:"binaries"`
	Facts      map[string]hostFacts `json:"host_facts"`
	Refused    []string             `json:"refused,omitempty"`
	Phases     []scalePhase         `json:"phases"`
	ReadyMs    int64                `json:"ready_ms"`
	Empty      scaleReading         `json:"empty"`
	Fill       scaleFill            `json:"fill"`
	Idle       scaleReading         `json:"idle"`
	IdleFigs   *idleFigures         `json:"idle_figures,omitempty"`
	Queue      *scaleQueue          `json:"queue,omitempty"`
	Checkpoint scaleCheckpoint      `json:"checkpoint"`
	Storm      *scaleStorm          `json:"storm,omitempty"`
	Delivery   *deliveryResult      `json:"delivery,omitempty"`
	Rejoin     *scaleRejoin         `json:"rejoin,omitempty"`
	Teardown   scaleTeardown        `json:"teardown"`
	Driver     *DriverInfo          `json:"driver,omitempty"`
	DriverHost []ClientHostSample   `json:"driver_host_series,omitempty"`
	Verdict    *ClientVerdict       `json:"client_limited,omitempty"`
	Scrapes    []nodeReading        `json:"scrapes"`
	// LockFetch is every node's lock and fetch histograms before the storm's
	// kill, after its verdict and at the run's end.
	LockFetch []lockReading `json:"lock_fetch,omitempty"`
	// Census is the route census after the rejoin (scale_census.go).
	Census *scaleCensus `json:"route_census,omitempty"`
	// Connacks is the CONNACK record of every client the run named
	// (scale_connacks.go).
	Connacks   *scaleConnacks          `json:"connacks,omitempty"`
	Samples    map[string][]hostSample `json:"host_samples"`
	SamplerBad map[string]int          `json:"sampler_unparsed_lines,omitempty"`
	Errors     []string                `json:"errors,omitempty"`
}

type scalePhase struct {
	Name    string `json:"name"`
	StartMs int64  `json:"start_ms"`
	EndMs   int64  `json:"end_ms"`
	Note    string `json:"note,omitempty"`
}

// scaleReading is every node's resident memory and scraped figures at one
// moment, and each host's available memory.
type scaleReading struct {
	AtMs       int64                         `json:"at_ms"`
	Reached    bool                          `json:"reached"`
	RSSKB      map[string]int64              `json:"rss_kb"`
	Metrics    map[string]map[string]float64 `json:"metrics"`
	MemAvailKB map[string]int64              `json:"mem_available_kb"`
}

type scaleFill struct {
	FillMs  int64          `json:"fill_ms"`
	Failed  int            `json:"failed"`
	PerNode map[string]int `json:"per_node"`
}

// idleFigures are the idle phase's per-session figures.
type idleFigures struct {
	// ClusterBytesPerSession is the rise in the nodes' resident memory from
	// the empty cluster, over the clients.
	ClusterBytesPerSession float64 `json:"cluster_bytes_per_session"`
	// NodeBytesPerCopy is each node's rise over the sessions it owns plus
	// the replica copies it holds.
	NodeBytesPerCopy map[string]float64 `json:"node_bytes_per_copy"`
	// HostMemFallKB is each host's fall in available memory from the empty
	// cluster.
	HostMemFallKB map[string]int64 `json:"host_mem_fall_kb"`
}

type scaleQueue struct {
	Request   queueRequest `json:"request"`
	Offline   queueResult  `json:"offline"`
	Baseline  scaleReading `json:"baseline"`
	Published queueResult  `json:"published"`
	After     scaleReading `json:"after"`
	Resumed   queueResult  `json:"resumed"`
	// BytesPerMessage is the nodes' rise in resident memory over the
	// messages acknowledged; NodeBytesPerQueued is each node's rise over its
	// own queued-messages indicator.
	BytesPerMessage    float64            `json:"bytes_per_message"`
	NodeBytesPerQueued map[string]float64 `json:"node_bytes_per_queued"`
}

type scaleCheckpoint struct {
	Reached bool `json:"reached"`
	// AfterFill is each node's checkpoint count at the idle reading; Taken
	// the counts when the phase ended.
	AfterFill map[string]float64 `json:"after_fill"`
	Taken     map[string]float64 `json:"taken"`
	// Peaks are, per node, every completed checkpoint's resident-memory peak
	// in the 30 s before it over the level at that window's start.
	Peaks map[string][]checkpointPeak `json:"peaks"`
	Files map[string][]string         `json:"files,omitempty"`
}

type checkpointPeak struct {
	CompletedMs int64 `json:"completed_ms"`
	BaseKB      int64 `json:"base_kb"`
	PeakKB      int64 `json:"peak_kb"`
}

type scaleStorm struct {
	Victim          string             `json:"victim"`
	VictimWasLeader bool               `json:"victim_was_leader"`
	Leaders         map[string]float64 `json:"raft_leader_before"`
	CrashMs         int64              `json:"crash_ms"`
	Target          int                `json:"target"`
	RecoverySec     float64            `json:"recovery_sec"`
	Verdict         string             `json:"verdict"`
	NinetyNineSec   float64            `json:"ninety_nine_sec"`
	NineNineNineSec float64            `json:"ninety_nine_nine_sec"`
	PeakPerSec      int                `json:"peak_reconnects_per_sec"`
	Dropped         int                `json:"dropped_clients"`
	Attempts        *attemptFigures    `json:"reconnect_attempts,omitempty"`
	Summary         *driverSummary     `json:"summary,omitempty"`
	Probe           *probeRecord       `json:"probe,omitempty"`
	Readings        int                `json:"readings"`
	// Series keeps one of the driver's readings a second, and the last: how
	// many clients were online and recovered across the window, which the
	// recovery instant alone does not show when it is never reached.
	Series []stormReading `json:"series,omitempty"`
	// Intervals is each survivor's long lock holds and waits, sessions served
	// empty and fetch outcomes between consecutive storm readings, so a hold
	// can be placed against what the clients saw at the same time.
	Intervals []holdInterval `json:"hold_intervals,omitempty"`
}

// stormTickEvery is how often the survivors' lock and fetch counters are
// read through the storm window.
var stormTickEvery = 10 * time.Second

// stormTicks reads every node but the victim at stormTickEvery until the
// returned stop is called; stop, which may be called more than once, adds
// the readings to the record.
func (r *scaleRun) stormTicks(ctx context.Context) (stop func()) {
	var survivors []scaleNode
	for _, n := range r.cfg.Layout.Nodes {
		if n.Name != r.cfg.Victim {
			survivors = append(survivors, n)
		}
	}
	var got []lockReading
	halt := ticking(stormTickEvery, func() {
		got = append(got, readLocksOf(ctx, "storm-tick", survivors)...)
	})
	var once sync.Once
	return func() {
		once.Do(func() {
			halt()
			r.rec.LockFetch = append(r.rec.LockFetch, got...)
		})
	}
}

// stormReading is one of the driver's readings in the storm, dated against
// the crash.
type stormReading struct {
	AtSec     float64 `json:"at_sec"`
	Online    int     `json:"online"`
	Recovered int     `json:"recovered"`
	Refusals  int64   `json:"refusals"`
	Fails     int64   `json:"fails"`
}

// appendReading keeps a reading when a second has passed since the last
// kept one, or when final asks for it.
func appendReading(series []stormReading, crashAt time.Time, s popSample, final bool) []stormReading {
	at := s.At.Sub(crashAt).Seconds()
	if n := len(series); n > 0 && !final && at-series[n-1].AtSec < 1 {
		return series
	}
	return append(series, stormReading{AtSec: at, Online: s.Online, Recovered: s.Recovered, Refusals: s.Refusals, Fails: s.Fails})
}

type scaleRejoin struct {
	Reached          bool                `json:"reached"`
	StartMs          int64               `json:"start_ms"`
	StateRecoveredMs int64               `json:"state_recovered_ms"`
	SessionsLoaded   int64               `json:"sessions_loaded"`
	HealthzMs        int64               `json:"healthz_ms"`
	ReadyzMs         int64               `json:"readyz_ms"`
	ServingzMs       int64               `json:"servingz_ms"`
	RejoinedMs       int64               `json:"rejoined_ms"`
	ReplicasBefore   float64             `json:"replicas_before_crash"`
	ReplicasAfter    float64             `json:"replicas_at_rejoin"`
	HostBytes        map[string][4]int64 `json:"host_bytes"`
	LinkBoundSec     map[string]float64  `json:"link_bound_sec"`
}

type scaleTeardown struct {
	// Stopped names the nodes teardown signalled, and NotRunning those it
	// found not running.
	Stopped    []string         `json:"stopped"`
	NotRunning []string         `json:"not_running,omitempty"`
	LogBytes   map[string]int64 `json:"log_bytes"`
	LogErrors  map[string]int   `json:"log_errors"`
	Removed    []string         `json:"removed"`
	RemoveErrs []string         `json:"remove_errors,omitempty"`
	// Kept is each run directory teardown left in place, with its size in
	// bytes, and KeptWhy what made it keep them.
	Kept    map[string]int64 `json:"kept,omitempty"`
	KeptWhy []string         `json:"kept_why,omitempty"`
}

// scaleLogKeep bounds how much of each node's log a run keeps.
const scaleLogKeep = 64 << 20

// linkBytesPerSec is 1 Gb/s in bytes.
const linkBytesPerSec = 125_000_000

// scaleRun is one run in progress.
type scaleRun struct {
	cfg      scaleConfig
	rec      *scaleRecord
	procs    map[string]*nodeProc
	samplers map[string]*hostSampler
	pop      *remotePopulation
	pr       *probe
	logf     func(string, ...any)
	failed   []string
	// artifactDir receives the node logs at teardown.
	artifactDir string
}

func (r *scaleRun) phase(name string, f func() string) {
	p := scalePhase{Name: name, StartMs: time.Now().UnixMilli()}
	r.logf("phase %s", name)
	p.Note = f()
	p.EndMs = time.Now().UnixMilli()
	r.rec.Phases = append(r.rec.Phases, p)
}

func (r *scaleRun) fail(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	r.failed = append(r.failed, msg)
	r.rec.Errors = append(r.rec.Errors, msg)
	r.logf("FAILED: %s", msg)
}

func (r *scaleRun) scrapeAll(ctx context.Context) map[string]map[string]float64 {
	out := map[string]map[string]float64{}
	for _, n := range r.cfg.Layout.Nodes {
		rd := scrape(ctx, n)
		r.rec.Scrapes = append(r.rec.Scrapes, rd)
		out[n.Name] = rd.Values
	}
	return out
}

func (r *scaleRun) reading(ctx context.Context) scaleReading {
	rd := scaleReading{AtMs: time.Now().UnixMilli(), Reached: true, RSSKB: nodeRSS(r.samplerList()),
		Metrics: r.scrapeAll(ctx), MemAvailKB: map[string]int64{}}
	for host, s := range r.samplers {
		if smp, ok := s.latest(); ok {
			rd.MemAvailKB[host] = smp.MemAvailKB
		}
	}
	return rd
}

func (r *scaleRun) samplerList() []*hostSampler {
	out := make([]*hostSampler, 0, len(r.samplers))
	for _, s := range r.samplers {
		out = append(out, s)
	}
	return out
}

// settle scrapes at the scrape interval until the figure the key function
// reads from all nodes has not changed for 30 s, bounded by the config's
// settle time.
func (r *scaleRun) settle(ctx context.Context, key func(map[string]map[string]float64) float64) bool {
	var series []timedValue
	deadline := time.Now().Add(time.Duration(r.cfg.SettleSec * float64(time.Second)))
	for time.Now().Before(deadline) && ctx.Err() == nil {
		series = append(series, timedValue{At: time.Now(), V: key(r.scrapeAll(ctx))})
		if _, ok := settledAt(series, 30*time.Second); ok {
			return true
		}
		sleepCtx(ctx, r.cfg.scrapeEvery())
	}
	return false
}

func sleepCtx(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

func sumOf(m map[string]map[string]float64, name string) float64 {
	s := 0.0
	for _, v := range m {
		s += v[name]
	}
	return s
}

// runScale runs one measurement. It returns the record, written or not, and
// an error for the harness's own failures; the measured figures are recorded,
// not judged.
func runScale(ctx context.Context, cfg scaleConfig, prov Provenance, artifactDir string, logf func(string, ...any)) (*scaleRecord, error) {
	rec := &scaleRecord{RunID: newScaleRunID(time.Now()), Config: cfg, Provenance: prov, Facts: map[string]hostFacts{},
		Samples: map[string][]hostSample{}}
	r := &scaleRun{cfg: cfg, rec: rec, procs: map[string]*nodeProc{}, samplers: map[string]*hostSampler{}, logf: logf, artifactDir: artifactDir}
	defer func() {
		if artifactDir != "" {
			if err := writeScaleArtifact(artifactDir, rec); err != nil {
				logf("artifact: %v", err)
			}
		}
	}()

	if p := r.startConditions(ctx, prov.Tree); len(p) > 0 {
		rec.Refused = p
		return rec, fmt.Errorf("%w: %s", errScaleRefused, strings.Join(p, "; "))
	}
	if artifactDir != "" {
		if err := os.MkdirAll(artifactDir, 0o750); err != nil {
			return rec, err
		}
	}
	defer r.teardown()

	steps := []struct {
		name string
		run  func() string
		on   bool
	}{
		{"start", func() string { return r.startNodes(ctx) }, true},
		{"empty", func() string {
			sleepCtx(ctx, 10*time.Second)
			rec.Empty = r.reading(ctx)
			return ""
		}, true},
		{"fill", func() string { return r.fill(ctx) }, true},
		{"idle", func() string { return r.idle(ctx) }, true},
		{"queue", func() string { return r.queue(ctx) }, cfg.Queue.Sessions > 0},
		{"checkpoint", func() string { return r.checkpoint(ctx) }, true},
		{"storm", func() string { return r.storm(ctx) }, true},
		{"delivery", func() string { return r.delivery(ctx) }, cfg.deliveryOn()},
		{"rejoin", func() string { return r.rejoin(ctx) }, true},
		{"census", func() string { return r.census(ctx) }, true},
	}
	// A phase that failed leaves the state the next one needs unreached, so
	// the run goes to its teardown.
	for _, st := range steps {
		if !st.on {
			continue
		}
		r.phase(st.name, st.run)
		if len(r.failed) == 0 && ctx.Err() == nil {
			r.checkNodes(ctx)
		}
		if len(r.failed) > 0 || ctx.Err() != nil {
			break
		}
	}
	if len(r.failed) > 0 {
		return rec, errors.New(strings.Join(r.failed, "; "))
	}
	return rec, nil
}

// startConditions reads every host once and names every unmet condition.
func (r *scaleRun) startConditions(ctx context.Context, tree Tree) []string {
	cfg := r.cfg
	out := layoutProblems(cfg.Layout)
	victimKnown := false
	for _, n := range cfg.Layout.Nodes {
		victimKnown = victimKnown || n.Name == cfg.Victim
	}
	if !victimKnown {
		out = append(out, fmt.Sprintf("the victim %q is not a node of the layout", cfg.Victim))
	}
	if len(out) > 0 {
		return out
	}
	reference := map[string]string{}
	paths := []string{cfg.Layout.Driver.Binary}
	for _, n := range cfg.Layout.Nodes {
		paths = append(paths, n.Binary)
	}
	seen := map[string]bool{}
	for _, p := range paths {
		if seen[p] {
			continue
		}
		seen[p] = true
		bi, err := readBinaryInfo(p)
		if err != nil {
			out = append(out, fmt.Sprintf("%s: %v", p, err))
			continue
		}
		r.rec.Binaries = append(r.rec.Binaries, bi)
		reference[p] = bi.SHA256
	}
	out = append(out, buildProblems(tree.Commit, r.rec.Binaries)...)
	needs := scaleNeeds{Clients: cfg.Clients, Nodes: len(cfg.Layout.Nodes), NodeBytes: cfg.NodeBytes, DriverBytes: cfg.DriverBytes}
	hosts := map[string][]scaleNode{"local": nil}
	for _, n := range cfg.Layout.Nodes {
		hosts[n.Host] = append(hosts[n.Host], n)
	}
	for _, host := range sortedNames(hosts) {
		nodes := hosts[host]
		bins := []string{}
		runDir := "/nonexistent-run-dir"
		for _, n := range nodes {
			bins = append(bins, n.Binary)
			runDir = filepath.Join(n.RunRoot, r.rec.RunID)
		}
		facts, err := runScript(ctx, shellFor(host), factsScript(bins, runDir))
		if err != nil {
			out = append(out, fmt.Sprintf("%s: reading the host: %v", host, err))
			continue
		}
		f := parseFacts(facts)
		r.rec.Facts[host] = f
		for _, p := range hostProblems(f, nodes, host == "local", needs, reference) {
			out = append(out, host+": "+p)
		}
	}
	return out
}

func (r *scaleRun) startNodes(ctx context.Context) string {
	ca, err := testcert.NewCA("scale " + r.rec.RunID)
	if err != nil {
		r.fail("cluster CA: %v", err)
		return ""
	}
	for i, n := range r.cfg.Layout.Nodes {
		leaf, err := ca.Issue(n.Name, int64(i+2))
		if err != nil {
			r.fail("%s: certificate: %v", n.Name, err)
			return ""
		}
		p := &nodeProc{node: n, sh: shellFor(n.Host), dir: filepath.Join(n.RunRoot, r.rec.RunID, n.Name),
			files: map[string][]byte{"ca.pem": ca.PEM, "node.pem": leaf.CertPEM, "node-key.pem": leaf.KeyPEM, "acl.json": scaleACL}}
		r.procs[n.Name] = p
		if err := p.launch(ctx, r.cfg.Layout.Nodes); err != nil {
			r.fail("%v; its log ends:\n%s", err, r.logTail(ctx, p, 20))
			return ""
		}
		r.logf("%s: pid %d on %s", n.Name, p.rec.PID, n.Host)
	}
	for _, n := range r.cfg.Layout.Nodes {
		if _, ok := r.samplers[n.Host]; ok {
			continue
		}
		s, err := startSampler(ctx, shellFor(n.Host), filepath.Join(n.RunRoot, r.rec.RunID), 500*time.Millisecond)
		if err != nil {
			r.fail("%s: sampler: %v", n.Host, err)
			return ""
		}
		r.samplers[n.Host] = s
	}
	start := time.Now()
	deadline := start.Add(5 * time.Minute)
	for {
		ready := 0
		for _, n := range r.cfg.Layout.Nodes {
			if probeHealth(ctx, n, "/readyz") {
				ready++
			}
		}
		if ready == len(r.cfg.Layout.Nodes) {
			r.rec.ReadyMs = time.Since(start).Milliseconds()
			return fmt.Sprintf("every node ready after %d ms", r.rec.ReadyMs)
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			r.fail("%d of %d nodes ready after %s", ready, len(r.cfg.Layout.Nodes), time.Since(start).Round(time.Second))
			return ""
		}
		for _, p := range r.procs {
			if _, alive, err := observeProc(ctx, p.sh, p.rec.PID); err == nil && !alive {
				r.fail("%s exited while starting; its log ends:\n%s", p.node.Name, r.logTail(ctx, p, 20))
				return ""
			}
		}
		sleepCtx(ctx, time.Second)
	}
}

// checkNodes fails the run naming each node whose process has gone while
// the run expects it running: every launched node but the victim between
// its kill and its restart. A node that cannot be read is recorded, not
// failed.
func (r *scaleRun) checkNodes(ctx context.Context) {
	for _, name := range sortedNames(r.procs) {
		p := r.procs[name]
		if p.rec.PID == 0 || p.down {
			continue
		}
		now, alive, err := observeProc(ctx, p.sh, p.rec.PID)
		if err != nil {
			r.rec.Errors = append(r.rec.Errors, fmt.Sprintf("checking %s: %v", name, err))
			continue
		}
		if procGone(p.rec, now, alive) {
			r.fail("%s exited outside the crash; its log ends:\n%s", name, r.logTail(ctx, p, 20))
		}
	}
}

// logTail is the last lines of a node's log.
func (r *scaleRun) logTail(ctx context.Context, p *nodeProc, lines int) string {
	out, err := runScript(ctx, p.sh, fmt.Sprintf("tail -n %d %s/node.log", lines, shellQuote(p.dir)))
	if err != nil {
		return err.Error()
	}
	return out
}

func (r *scaleRun) clientAddrs() []string {
	out := make([]string, 0, len(r.cfg.Layout.Nodes))
	for _, n := range r.cfg.Layout.Nodes {
		out = append(out, n.clientAddr())
	}
	return out
}

func (r *scaleRun) fill(ctx context.Context) string {
	d := r.cfg.Layout.Driver
	settings := remoteSettings{Command: []string{"taskset", "-c", d.Cores, d.Binary}, Nodes: r.clientAddrs(), Sources: d.Sources,
		TCPKeepAliveOff: r.cfg.DriverTCPKeepAlive == "off", KeepAlive: uint16(r.cfg.ClientKeepAliveSec)}
	pop, err := startRemotePopulation(settings, r.cfg.Clients, defaultRemoteOptions())
	if err != nil {
		r.fail("driver: %v", err)
		return ""
	}
	r.pop = pop
	info, err := pop.hello()
	if err != nil {
		r.fail("driver: %v", err)
		return ""
	}
	r.rec.Driver = &info
	if problem := driverBuildProblem(true, r.rec.Provenance.Tree, info); problem != "" {
		r.fail("driver: %s", problem)
		return ""
	}
	if problem := driverKeepAliveProblem(r.cfg.DriverTCPKeepAlive, info); problem != "" {
		r.fail("driver: %s", problem)
		return ""
	}
	if problem := driverClientKeepAliveProblem(r.cfg.ClientKeepAliveSec, info); problem != "" {
		r.fail("driver: %s", problem)
		return ""
	}
	start := time.Now()
	var completed atomic.Int64
	failed, err := pop.fill(ctx, &completed)
	r.rec.Fill.FillMs = time.Since(start).Milliseconds()
	r.rec.Fill.Failed = failed
	if err != nil {
		r.fail("fill: %v", err)
		return ""
	}
	r.rec.Fill.PerNode, _ = pop.perNode()
	if failed > 0 {
		r.fail("fill: %d of %d clients failed to connect or subscribe; nothing after it would be measured on the population asked for", failed, r.cfg.Clients)
		return ""
	}
	return fmt.Sprintf("%d clients, %d failed, %d ms", r.cfg.Clients, failed, r.rec.Fill.FillMs)
}

// driverKeepAliveProblem names a driver that applies another TCP keepalive
// than the run asked for.
func driverKeepAliveProblem(asked string, info DriverInfo) string {
	if info.TCPKeepAlive != asked {
		return fmt.Sprintf("the driver applies TCP keepalive %q, the run asked for %q", info.TCPKeepAlive, asked)
	}
	return ""
}

// driverClientKeepAliveProblem names a driver that gives its clients another
// MQTT keep alive than the run asked for.
func driverClientKeepAliveProblem(asked int, info DriverInfo) string {
	if info.KeepAlive != asked {
		return fmt.Sprintf("the driver's clients ask for keep alive %d s, the run asked for %d s", info.KeepAlive, asked)
	}
	return ""
}

// sessionsAndCopies is the cluster's sessions and replica copies as one
// number, unchanged only when both are.
func sessionsAndCopies(m map[string]map[string]float64) float64 {
	return sumOf(m, mSessions)*1e9 + sumOf(m, mReplicas)
}

// connectionsAndSessions is the cluster's connections and sessions as one
// number, unchanged only when both are.
func connectionsAndSessions(m map[string]map[string]float64) float64 {
	return sumOf(m, mConns)*1e9 + sumOf(m, mSessions)
}

func (r *scaleRun) idle(ctx context.Context) string {
	start := time.Now()
	reached := r.settle(ctx, sessionsAndCopies)
	hold := time.Duration(r.cfg.IdleHoldSec * float64(time.Second))
	holdFor(ctx, start, hold, r.cfg.scrapeEvery(), func() { r.scrapeAll(ctx) })
	rd := r.reading(ctx)
	rd.Reached = reached
	r.rec.Idle = rd
	figs := &idleFigures{NodeBytesPerCopy: map[string]float64{}, HostMemFallKB: map[string]int64{}}
	figs.ClusterBytesPerSession = rssDelta(r.rec.Empty.RSSKB, rd.RSSKB, float64(r.cfg.Clients))
	for n, after := range rd.RSSKB {
		copies := rd.Metrics[n][mSessions] + rd.Metrics[n][mReplicas]
		figs.NodeBytesPerCopy[n] = rssDelta(map[string]int64{n: r.rec.Empty.RSSKB[n]}, map[string]int64{n: after}, copies)
	}
	for h, kb := range rd.MemAvailKB {
		figs.HostMemFallKB[h] = r.rec.Empty.MemAvailKB[h] - kb
	}
	r.rec.IdleFigs = figs
	held := ""
	if hold > 0 {
		held = fmt.Sprintf("; held %.0f s", time.Since(start).Seconds())
	}
	if !reached {
		return "not settled within the bound" + held
	}
	return fmt.Sprintf("%.0f bytes a session across the cluster%s", figs.ClusterBytesPerSession, held)
}

// holdFor scrapes every interval until hold has passed since start, so a
// phase that ended early still lasts the hold; it returns how many scrapes it
// took. A hold already passed takes none.
func holdFor(ctx context.Context, start time.Time, hold, every time.Duration, scrape func()) int {
	n := 0
	for ctx.Err() == nil {
		left := time.Until(start.Add(hold))
		if left <= 0 {
			return n
		}
		scrape()
		n++
		sleepCtx(ctx, min(left, every))
	}
	return n
}

func (r *scaleRun) queue(ctx context.Context) string {
	q := &scaleQueue{Request: r.cfg.Queue, NodeBytesPerQueued: map[string]float64{}}
	r.rec.Queue = q
	var err error
	if q.Offline, err = r.pop.queue(ctx, r.cfg.Queue); err != nil {
		r.fail("queue: %v", err)
		return ""
	}
	reached := r.settle(ctx, connectionsAndSessions)
	q.Baseline = r.reading(ctx)
	q.Baseline.Reached = reached
	if q.Published, err = r.pop.publish(ctx); err != nil {
		r.fail("publish: %v", err)
		return ""
	}
	reached = r.settle(ctx, func(m map[string]map[string]float64) float64 { return sumOf(m, mQueued) })
	q.After = r.reading(ctx)
	q.After.Reached = reached
	acked := float64(q.Published.Acked["Success"])
	q.BytesPerMessage = rssDelta(q.Baseline.RSSKB, q.After.RSSKB, acked)
	for n, after := range q.After.RSSKB {
		queued := q.After.Metrics[n][mQueued] - q.Baseline.Metrics[n][mQueued]
		q.NodeBytesPerQueued[n] = rssDelta(map[string]int64{n: q.Baseline.RSSKB[n]}, map[string]int64{n: after}, queued)
	}
	if acked == 0 || sumOf(q.After.Metrics, mQueued) <= sumOf(q.Baseline.Metrics, mQueued) {
		r.fail("queue: nothing measured (acknowledged %v, queued indicator %.0f → %.0f)", q.Published.Acked,
			sumOf(q.Baseline.Metrics, mQueued), sumOf(q.After.Metrics, mQueued))
		return ""
	}
	if q.Resumed, err = r.pop.resume(ctx, 10*time.Minute); err != nil {
		r.fail("resume: %v", err)
		return ""
	}
	return fmt.Sprintf("%.0f bytes a queued message; %d of %d resumed sessions received every message",
		q.BytesPerMessage, q.Resumed.Complete, q.Offline.Offline)
}

func (r *scaleRun) checkpoint(ctx context.Context) string {
	c := &r.rec.Checkpoint
	c.AfterFill, c.Taken, c.Peaks = map[string]float64{}, map[string]float64{}, map[string][]checkpointPeak{}
	for n, m := range r.rec.Idle.Metrics {
		c.AfterFill[n] = m[mCheckpts]
	}
	deadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		m := r.scrapeAll(ctx)
		all := true
		for n := range c.AfterFill {
			c.Taken[n] = m[n][mCheckpts]
			if c.Taken[n] <= c.AfterFill[n] {
				all = false
			}
		}
		if all {
			c.Reached = true
			break
		}
		sleepCtx(ctx, r.cfg.scrapeEvery())
	}
	r.checkpointPeaks()
	if !c.Reached {
		return "a node took no checkpoint after the fill within the bound"
	}
	return "every node checkpointed after the fill"
}

// checkpointPeaks finds, per node, each checkpoint completed since the idle
// reading (its counter's advance between two scrapes) and the resident
// memory's peak in the 30 s before it over the level at that window's start.
func (r *scaleRun) checkpointPeaks() {
	var series []hostSample
	for _, s := range r.samplers {
		series = append(series, s.series()...)
	}
	sort.Slice(series, func(i, j int) bool { return series[i].AtMs < series[j].AtMs })
	prev := map[string]float64{}
	for _, sc := range r.rec.Scrapes {
		v, ok := sc.Values[mCheckpts]
		if !ok || sc.AtMs < r.rec.Idle.AtMs {
			if ok {
				prev[sc.Node] = v
			}
			continue
		}
		if p, had := prev[sc.Node]; had && v > p {
			from := sc.AtMs - 30_000
			pk := checkpointPeak{CompletedMs: sc.AtMs, BaseKB: -1}
			for _, s := range series {
				kb, ok := s.RSSKB[sc.Node]
				if !ok || s.AtMs < from || s.AtMs > sc.AtMs {
					continue
				}
				if pk.BaseKB < 0 {
					pk.BaseKB = kb
				}
				pk.PeakKB = max(pk.PeakKB, kb)
			}
			r.rec.Checkpoint.Peaks[sc.Node] = append(r.rec.Checkpoint.Peaks[sc.Node], pk)
		}
		prev[sc.Node] = v
	}
}

func (r *scaleRun) storm(ctx context.Context) string {
	st := &scaleStorm{Victim: r.cfg.Victim, Leaders: map[string]float64{}}
	r.rec.Storm = st
	for n, m := range r.scrapeAll(ctx) {
		st.Leaders[n] = m[mLeader]
	}
	st.VictimWasLeader = st.Leaders[r.cfg.Victim] == 1
	r.readLocks(ctx, "storm-start")
	names := map[string]string{}
	for _, n := range r.cfg.Layout.Nodes {
		names[n.clientAddr()] = n.Name
	}
	pr, err := newProbeAt(ctx, 100, probeDefaultRate, r.clientAddrs(), func(a string) string { return names[a] })
	r.pr = pr
	if err != nil {
		r.fail("probe: %v", err)
		return ""
	}
	pr.start(ctx)
	if err := r.pop.mark(); err != nil {
		r.fail("mark: %v", err)
		return ""
	}
	victim := r.procs[r.cfg.Victim]
	crashAt := time.Now()
	if sent, err := victim.signal(ctx, "KILL"); err != nil || !sent {
		r.fail("crash of %s: sent %v, %v", r.cfg.Victim, sent, err)
		return ""
	}
	victim.down = true
	st.CrashMs = crashAt.UnixMilli()
	stopTicks := r.stormTicks(ctx)
	defer stopTicks()
	if !victim.waitGone(ctx, 30*time.Second) {
		r.fail("%s still runs 30 s after SIGKILL", r.cfg.Victim)
	}
	if err := r.pop.crashed(); err != nil {
		r.fail("crashed: %v", err)
		return ""
	}
	st.Target = r.cfg.Clients - r.rec.Fill.Failed
	deadline := crashAt.Add(time.Duration(r.cfg.WindowSec * float64(time.Second)))
	w := newHoldWatch(st.Target, r.cfg.Hold, deadline)
	perSec := map[int64]int{}
	lastOnline := -1
	var last popSample
	for ctx.Err() == nil {
		now := time.Now()
		if now.After(deadline) && (w.holdStart().IsZero() || now.After(deadline.Add(r.cfg.Hold))) {
			break
		}
		s, err := r.pop.sample()
		if err != nil {
			r.fail("driver during the storm: %v", err)
			return ""
		}
		st.Readings++
		last = s
		st.Series = appendReading(st.Series, crashAt, s, false)
		if lastOnline >= 0 && s.Online > lastOnline {
			perSec[s.At.Unix()] += s.Online - lastOnline
		}
		lastOnline = s.Online
		if w.observe(s.At, s.Recovered) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if st.Readings > 0 {
		st.Series = appendReading(st.Series, crashAt, last, true)
	}
	instant, verdict := w.verdict()
	st.Verdict = verdict
	if !instant.IsZero() {
		st.RecoverySec = instant.Sub(crashAt).Seconds()
	}
	for _, v := range perSec {
		st.PeakPerSec = max(st.PeakPerSec, v)
	}
	rec := pr.result(crashAt, instant)
	st.Probe = &rec
	sum, err := r.pop.finish()
	if err != nil {
		r.fail("finish: %v", err)
		return ""
	}
	st.Summary = &r.pop.summary
	dateLateDrops(st.Summary.LateDrops, names, st.CrashMs)
	st.Dropped = sum.DroppedClients
	if n := len(sum.Reconnects); n > 0 {
		st.NinetyNineSec = percentile(sum.Reconnects, 0.99).Seconds()
		st.NineNineNineSec = percentile(sum.Reconnects, 0.999).Seconds()
	}
	st.Attempts = attemptStats(sum.Attempts)
	stopTicks()
	r.readLocks(ctx, "storm-end")
	st.Intervals = holdIntervals(r.rec.LockFetch, st.CrashMs)
	if instant.IsZero() {
		return "not recovered: " + verdict
	}
	return fmt.Sprintf("recovered %.2f s after the crash", st.RecoverySec)
}

// delivery runs the delivery check while the victim is still down, so it
// reads the storm's outcome before the restart moves anything, and judges
// only whether the check itself worked.
func (r *scaleRun) delivery(ctx context.Context) string {
	res, err := r.pop.deliver(ctx, deliveryRequest{Sample: r.cfg.DeliverySample, Seed: deliverySeed, Controls: deliveryControls,
		WaitMs: deliveryWait.Milliseconds()})
	if err != nil {
		r.fail("delivery: %v", err)
		return ""
	}
	names := map[string]string{}
	for _, n := range r.cfg.Layout.Nodes {
		names[n.clientAddr()] = n.Name
	}
	nameDeliveryNodes(&res, names)
	r.rec.Delivery = &res
	if p := deliveryProblems(res); len(p) > 0 {
		r.fail("delivery check: %s", strings.Join(p, "; "))
		return ""
	}
	return fmt.Sprintf("%d checked, %d delivered, %d lost sessions, %d not delivered on a held connection; controls %d of %d caught",
		res.Checked, res.Delivered, res.LostSessions, res.HeldNotDelivered, res.ControlsCaught, res.ControlsRemoved)
}

// deliveryProblems names what makes a delivery check's result unusable: no
// control removed, a removed control the check did not count as lost, or no
// checked client receiving its message. Losses are findings, not problems.
func deliveryProblems(res deliveryResult) []string {
	var p []string
	if res.ControlsRemoved == 0 {
		p = append(p, fmt.Sprintf("no negative control could be made (%d asked; one needs a client resumed with Session Present 1)", res.ControlsAsked))
	}
	if res.ControlsCaught < res.ControlsRemoved {
		var missed []string
		for _, c := range res.Controls {
			if c.Class != classNotRemoved && !controlCaught(c.Class) {
				missed = append(missed, c.ID+" "+c.Class)
			}
		}
		p = append(p, fmt.Sprintf("%d of %d negative controls not counted lost: %s", res.ControlsRemoved-res.ControlsCaught, res.ControlsRemoved, strings.Join(missed, ", ")))
	}
	if res.Delivered == 0 {
		p = append(p, fmt.Sprintf("no checked client received its message (%d checked)", res.Checked))
	}
	return p
}

// nameDeliveryNodes names each listed client's node from its address.
func nameDeliveryNodes(res *deliveryResult, names map[string]string) {
	for _, list := range [][]deliveryClient{res.Lost, res.Held, res.ToldNamed, res.NotOnlineNamed, res.NeverAcceptedNamed, res.Controls} {
		for i := range list {
			list[i].NodeName = names[list[i].Node]
		}
	}
}

// stateRecoveredScript prints the time and session count of the node log's
// last "state recovered" line.
const stateRecoveredScript = `grep '"msg":"state recovered"' %s/node.log | tail -n 1`

func (r *scaleRun) rejoin(ctx context.Context) string {
	j := &scaleRejoin{HostBytes: map[string][4]int64{}, LinkBoundSec: map[string]float64{}}
	r.rec.Rejoin = j
	victim := r.procs[r.cfg.Victim]
	for _, sc := range r.rec.Scrapes {
		if sc.Node == r.cfg.Victim && sc.AtMs <= r.rec.Storm.CrashMs && sc.Values != nil {
			j.ReplicasBefore = sc.Values[mReplicas]
		}
	}
	bytesAt := func() map[string][4]int64 {
		out := map[string][4]int64{}
		for h, s := range r.samplers {
			if smp, ok := s.latest(); ok {
				out[h] = smp.Bytes
			}
		}
		return out
	}
	before := bytesAt()
	start := time.Now()
	j.StartMs = start.UnixMilli()
	if err := victim.launch(ctx, r.cfg.Layout.Nodes); err != nil {
		r.fail("restart of %s: %v", r.cfg.Victim, err)
		return ""
	}
	var series []rejoinReading
	deadline := start.Add(time.Duration(r.cfg.RejoinSec * float64(time.Second)))
	var short float64
	var replicas float64
	lastScrape := time.Time{}
	for time.Now().Before(deadline) && ctx.Err() == nil {
		now := time.Now()
		since := now.Sub(start).Milliseconds()
		if j.HealthzMs == 0 && probeHealth(ctx, victim.node, "/healthz") {
			j.HealthzMs = since
		}
		ready := probeHealth(ctx, victim.node, "/readyz")
		serving := probeHealth(ctx, victim.node, "/servingz")
		if ready && j.ReadyzMs == 0 {
			j.ReadyzMs = since
		}
		if serving && j.ServingzMs == 0 {
			j.ServingzMs = since
		}
		if now.Sub(lastScrape) >= r.cfg.scrapeEvery()/2 {
			lastScrape = now
			m := r.scrapeAll(ctx)
			replicas = m[r.cfg.Victim][mReplicas]
			short = 0
			for _, v := range m {
				short = math.Max(short, v[mShort])
			}
		}
		series = append(series, rejoinReading{At: now, Ready: ready, Serving: serving, Replicas: replicas, Short: short})
		if at, ok := rejoinedAt(series, 30*time.Second); ok {
			j.Reached, j.RejoinedMs, j.ReplicasAfter = true, at.Sub(start).Milliseconds(), replicas
			break
		}
		sleepCtx(ctx, time.Second)
	}
	after := bytesAt()
	for h, b := range after {
		d := [4]int64{}
		for i := range d {
			d[i] = b[i] - before[h][i]
		}
		j.HostBytes[h] = d
		j.LinkBoundSec[h] = float64(max(d[2], d[3])) / linkBytesPerSec
	}
	if out, err := runScript(ctx, victim.sh, fmt.Sprintf(stateRecoveredScript, shellQuote(victim.dir))); err == nil {
		var line struct {
			Time     time.Time `json:"time"`
			Sessions int64     `json:"sessions"`
		}
		if json.Unmarshal([]byte(strings.TrimSpace(out)), &line) == nil && !line.Time.IsZero() {
			j.StateRecoveredMs = line.Time.Sub(start).Milliseconds()
			j.SessionsLoaded = line.Sessions
		}
	}
	if !j.Reached {
		return "not rejoined within the bound"
	}
	return fmt.Sprintf("rejoined %d ms after its restart", j.RejoinedMs)
}

// teardown ends the driver, the probe and every node, keeps the node logs,
// and removes the run's directories, or keeps them when keepRunDirs finds
// them evidence.
func (r *scaleRun) teardown() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	td := &r.rec.Teardown
	td.LogBytes, td.LogErrors = map[string]int64{}, map[string]int{}
	if len(r.procs) > 0 {
		r.readLocks(ctx, "end")
	}
	// The driver answers for its clients only once it has sent its summary.
	if r.pop != nil && r.rec.Storm != nil && r.rec.Storm.Summary != nil {
		r.readConnacks(ctx)
	}
	if r.pr != nil {
		r.pr.close()
	}
	if r.pop != nil {
		r.pop.close()
		r.pop.mu.Lock()
		series := append([]ClientHostSample(nil), r.pop.series...)
		r.pop.mu.Unlock()
		r.rec.DriverHost = series
		v := clientLimited(series)
		r.rec.Verdict = &v
	}
	for _, name := range sortedNames(r.procs) {
		p := r.procs[name]
		switch running, err := p.stop(ctx, 30*time.Second); {
		case err != nil:
			r.rec.Errors = append(r.rec.Errors, fmt.Sprintf("stopping %s: %v", name, err))
		case running:
			td.Stopped = append(td.Stopped, name)
		default:
			td.NotRunning = append(td.NotRunning, name)
		}
		if out, err := runScript(ctx, p.sh, fmt.Sprintf("wc -c < %[1]s/node.log; grep -c '\"level\":\"ERROR\"' %[1]s/node.log || true; find %[1]s/data -type f -printf '%%s %%P\\n' | sort -k2 | head -n 500",
			shellQuote(p.dir))); err == nil {
			lines := strings.Split(strings.TrimSpace(out), "\n")
			if len(lines) >= 2 {
				td.LogBytes[name], _ = strconv.ParseInt(strings.TrimSpace(lines[0]), 10, 64)
				td.LogErrors[name], _ = strconv.Atoi(strings.TrimSpace(lines[1]))
				if r.rec.Checkpoint.Files == nil {
					r.rec.Checkpoint.Files = map[string][]string{}
				}
				r.rec.Checkpoint.Files[name] = lines[2:]
			}
		}
	}
	for host, s := range r.samplers {
		s.stop()
		r.rec.Samples[host] = s.series()
		if s.bad > 0 {
			if r.rec.SamplerBad == nil {
				r.rec.SamplerBad = map[string]int{}
			}
			r.rec.SamplerBad[host] = s.bad
		}
	}
	if r.artifactDir != "" {
		for _, name := range sortedNames(r.procs) {
			p := r.procs[name]
			out, err := runScript(ctx, p.sh, fmt.Sprintf("tail -c %d %s/node.log", scaleLogKeep, shellQuote(p.dir)))
			if err == nil {
				err = os.WriteFile(filepath.Join(r.artifactDir, name+".log"), []byte(out), 0o640)
			}
			if err != nil {
				r.rec.Errors = append(r.rec.Errors, fmt.Sprintf("keeping %s's log: %v", name, err))
			}
		}
	}
	var dirs []runDir
	for _, name := range sortedNames(r.procs) {
		p := r.procs[name]
		dirs = append(dirs, runDir{host: p.node.Host, root: p.node.RunRoot, sh: p.sh})
	}
	r.finishDirs(ctx, dirs)
}

// finishDirs ends the run's directories by the run's readings and its storm's
// late drops.
func (r *scaleRun) finishDirs(ctx context.Context, dirs []runDir) {
	var late *lateDrops
	if r.rec.Storm != nil && r.rec.Storm.Summary != nil {
		late = r.rec.Storm.Summary.LateDrops
	}
	finishRunDirs(ctx, &r.rec.Teardown, dirs, r.rec.RunID, r.rec.LockFetch, late)
}

// runDir is one host's run root, as teardown reaches it.
type runDir struct {
	host, root string
	sh         hostShell
}

// finishRunDirs removes the run's directory on each host once, or, when
// keepRunDirs decides from the readings and the late drops that they are
// evidence, keeps every one and records its size.
func finishRunDirs(ctx context.Context, td *scaleTeardown, dirs []runDir, runID string, readings []lockReading, late *lateDrops) {
	keep, why := keepRunDirs(readings, late)
	if keep {
		td.KeptWhy, td.Kept = why, map[string]int64{}
	}
	done := map[string]bool{}
	for _, d := range dirs {
		base := filepath.Join(d.root, runID)
		key := d.host + ":" + base
		if done[key] {
			continue
		}
		done[key] = true
		if keep {
			out, err := runScript(ctx, d.sh, "du -sb "+shellQuote(base))
			size, _ := strconv.ParseInt(strings.Fields(out + " -1")[0], 10, 64)
			if err != nil {
				size = -1
			}
			td.Kept[key] = size
			continue
		}
		if err := removeDir(ctx, d.sh, base, d.root, runID); err != nil {
			td.RemoveErrs = append(td.RemoveErrs, err.Error())
		} else {
			td.Removed = append(td.Removed, key)
		}
	}
}

// writeScaleArtifact writes run.json, report.md and, last, complete.
func writeScaleArtifact(dir string, rec *scaleRecord) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "run.json"), data, 0o640); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "report.md"), []byte(scaleReport(rec)), 0o640); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "complete"), []byte(time.Now().Format(time.RFC3339)+"\n"), 0o640)
}

// scaleReport is the run's summary, result first.
func scaleReport(rec *scaleRecord) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Scale run %s\n\n", rec.RunID)
	if len(rec.Refused) > 0 {
		fmt.Fprintf(&b, "**Refused to start:**\n\n")
		for _, p := range rec.Refused {
			fmt.Fprintf(&b, "- %s\n", p)
		}
		return b.String()
	}
	for _, e := range rec.Errors {
		fmt.Fprintf(&b, "- **Error:** %s\n", e)
	}
	fmt.Fprintf(&b, "\n%d clients, %d nodes, commit %s.\n\n", rec.Config.Clients, len(rec.Config.Layout.Nodes), rec.Provenance.Tree.Commit)
	fmt.Fprintf(&b, "| Phase | Note | Seconds |\n|---|---|---|\n")
	for _, p := range rec.Phases {
		fmt.Fprintf(&b, "| %s | %s | %.1f |\n", p.Name, p.Note, float64(p.EndMs-p.StartMs)/1000)
	}
	if f := rec.IdleFigs; f != nil {
		fmt.Fprintf(&b, "\n## Idle\n\nResident memory a session, cluster-wide: %.0f bytes.\n\n| Node | RSS (KiB) | Sessions | Replica copies | Bytes a copy |\n|---|---|---|---|---|\n",
			f.ClusterBytesPerSession)
		for _, n := range sortedNames(rec.Idle.RSSKB) {
			m := rec.Idle.Metrics[n]
			fmt.Fprintf(&b, "| %s | %d | %.0f | %.0f | %.0f |\n", n, rec.Idle.RSSKB[n], m[mSessions], m[mReplicas], f.NodeBytesPerCopy[n])
		}
	}
	if q := rec.Queue; q != nil {
		fmt.Fprintf(&b, "\n## Queued messages\n\n%d sessions offline, %d messages of %d bytes each: published %d, acknowledged %v; "+
			"%.0f bytes a queued message; on resume %d of %d received every message, %d sessions lost.\n",
			q.Offline.Offline, q.Request.Depth, q.Request.PayloadBytes, q.Published.Published, q.Published.Acked,
			q.BytesPerMessage, q.Resumed.Complete, q.Offline.Offline, q.Resumed.SessionLost)
	}
	if s := rec.Storm; s != nil {
		fmt.Fprintf(&b, "\n## Storm\n\nVictim %s (raft leader: %v). %s; recovery %.2f s; 99%% %.2f s, 99.9%% %.2f s; peak %d reconnects a second.\n",
			s.Victim, s.VictimWasLeader, s.Verdict, s.RecoverySec, s.NinetyNineSec, s.NineNineNineSec, s.PeakPerSec)
		if a := s.Attempts; a != nil {
			fmt.Fprintf(&b, "Connects a reconnecting client made, from its drop to its accepted CONNACK (%d clients): median %d, 90%% %d, 99%% %d, max %d, mean %.1f.\n",
				a.Clients, a.Median, a.P90, a.P99, a.Max, a.Mean)
		}
		if s.Summary != nil {
			fmt.Fprintf(&b, "Sessions lost: %d clients; below-QoS grants: %d; not recovered at the window's end: %d (named in run.json).\n",
				s.Summary.SessionLostClients, s.Summary.SubDowngraded, s.Summary.NotRecovered)
		}
		if s.Probe != nil {
			fmt.Fprintf(&b, "Probe: %d acknowledged, %d acknowledged and not delivered, refused after recovery %v.\n",
				s.Probe.Acked, s.Probe.AckedNotDelivered, s.Probe.RefusedAfterRecovery)
		}
		if s.Summary != nil {
			b.WriteString(lateDropReport(s.Summary.LateDrops))
		}
	}
	if d := rec.Delivery; d != nil {
		fmt.Fprintf(&b, "\n## Delivery check\n\nSetting %s (seed %d): %d clients checked, %d publishers, %d publishes in %d passes over %d ms, wait %d ms. "+
			"Delivered %d; **lost sessions (resumed with Session Present 1, nothing received) %d**; not delivered on a held connection %d; "+
			"told Session Present 0 %d; not online %d; never accepted %v; refusals by code %v. Negative controls: %d asked, %d removed, %d caught.\n",
			rec.Config.DeliveryCheck, d.Seed, d.Checked, d.Publishers, d.Published, d.Passes, d.PublishMs, d.WaitMs,
			d.Delivered, d.LostSessions, d.HeldNotDelivered, d.Told, d.NotOnline, d.NeverAccepted, d.RefusalsByCode,
			d.ControlsAsked, d.ControlsRemoved, d.ControlsCaught)
		for _, c := range d.Lost {
			fmt.Fprintf(&b, "- lost: %s on %s (%s), %s\n", c.ID, c.NodeName, c.Node, c.Reason)
		}
		for _, c := range d.Held {
			fmt.Fprintf(&b, "- not delivered, connection held: %s on %s (%s), %s\n", c.ID, c.NodeName, c.Node, c.Reason)
		}
	}
	if j := rec.Rejoin; j != nil {
		fmt.Fprintf(&b, "\n## Rejoin\n\nReached %v: state recovered %d ms (%d sessions), healthz %d ms, readyz %d ms, servingz %d ms, rejoined %d ms; copies %.0f before the crash, %.0f at rejoin. Link bound: %v s.\n",
			j.Reached, j.StateRecoveredMs, j.SessionsLoaded, j.HealthzMs, j.ReadyzMs, j.ServingzMs, j.RejoinedMs, j.ReplicasBefore, j.ReplicasAfter, j.LinkBoundSec)
	}
	b.WriteString(lockReport(rec.LockFetch))
	if td := rec.Teardown; len(td.Kept) > 0 {
		fmt.Fprintf(&b, "\n## Kept run directories\n\n%s, so teardown kept the brokers' run directories, to be removed by hand once read:\n", strings.Join(td.KeptWhy, "; "))
		for _, k := range sortedKeys(td.Kept) {
			fmt.Fprintf(&b, "- %s (%d bytes)\n", k, td.Kept[k])
		}
	}
	if c := rec.Census; c != nil {
		fmt.Fprintf(&b, "\n## Route census\n\nEach node's subscriptions against its own routes, after the rejoin: %d gaps in the last of %d readings.\n", len(c.Gaps), c.Readings)
		for _, g := range c.Gaps {
			fmt.Fprintf(&b, "- %s: %s (%.0f subscriptions, %.0f own routes)\n", g.Node, g.Reason, g.Subscriptions, g.OwnRoutes)
		}
		b.WriteString(wholeSessionReport(c.Sessions))
		for _, f := range c.Reconciler {
			fmt.Fprintf(&b, "- **Finding:** %s.\n", f)
		}
	}
	b.WriteString(reconcileReport(rec.LockFetch))
	b.WriteString(fetchReport(rec.LockFetch))
	if s := rec.Storm; s != nil {
		b.WriteString(intervalReport(s.Intervals))
	}
	b.WriteString(connackReport(rec.Connacks))
	return b.String()
}

// attemptFigures is the distribution of connects a reconnecting client made
// in the storm, from its drop to its accepted CONNACK. The count measures the
// client's retry policy as much as the broker: sim/client.go's backoff and the
// order it tries the nodes in.
type attemptFigures struct {
	Clients int     `json:"clients"`
	Median  int     `json:"median"`
	P90     int     `json:"p90"`
	P99     int     `json:"p99"`
	Max     int     `json:"max"`
	Mean    float64 `json:"mean"`
}

// attemptStats summarises sorted per-client attempt counts with the failover
// report's percentile rule (the value at index len*p, clamped); nil when no
// client reconnected.
func attemptStats(sorted []int) *attemptFigures {
	n := len(sorted)
	if n == 0 {
		return nil
	}
	at := func(p float64) int { return sorted[min(n-1, int(float64(n)*p))] }
	sum := 0
	for _, v := range sorted {
		sum += v
	}
	return &attemptFigures{Clients: n, Median: at(0.5), P90: at(0.9), P99: at(0.99), Max: sorted[n-1], Mean: float64(sum) / float64(n)}
}
