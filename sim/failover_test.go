package sim

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/trace"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/broker"
	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/module"
)

// TestFailover measures a node loss with many durable clients: the time to
// full reconnection and the lease-proposal batching. The gate runs 1,000
// clients; HOARDMQ_SIM_FAILOVER=<clients> runs the measurement and writes
// its report to the artifact directory when one is set, or to
// results/failover-<date>.md when not (see treeReportPath).
func TestFailover(t *testing.T) {
	clients := 1000
	record := false
	if os.Getenv("HOARDMQ_SIM_REDUCED") != "" && os.Getenv("HOARDMQ_SIM_FAILOVER") == "" {
		// The reduced mode exists so the whole-module race run fits its
		// budget. A thousand clients reconnecting is minutes of that budget
		// and exercises no concurrency the corpus does not, so the race run
		// skips it; the gate runs it without -race.
		t.Skip("reduced mode: the failover gate runs without -race")
	}
	if v := os.Getenv("HOARDMQ_SIM_FAILOVER"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			t.Fatalf("HOARDMQ_SIM_FAILOVER: %v", err)
		}
		clients, record = n, true
	}
	// A load run must start on a settled machine: three recorded runs were
	// invalidated by sockets left over from a previous attempt (the symptom
	// is a fill in tens of minutes instead of ~4.5, clients that cannot
	// connect at all, and lease counters an order of magnitude high before
	// the crash). Refuse to start, naming what was seen, rather than record
	// a number that looks like a finding.
	if used, ok := tcpSockets(); ok {
		// The probe's pairs and groups are clients too, and the limit is
		// derived from how many the run will open.
		limit := settleLimit(clients + probeClients(probePairs(), probeGroupCount(), 3))
		if used > limit {
			t.Fatalf("machine is not settled: %d TCP sockets in use or in TIME-WAIT (limit %d for %d clients); wait for them to drain — see ss -s", used, limit, clients)
		}
	}
	// Evidence capture (WI 1284): with HOARDMQ_SIM_ARTIFACT set, the
	// measurement writes its report, per-phase runtime deltas, profile
	// snapshots (including one taken mid-storm), and a flight-recorder
	// trace window into its own artifact directory. Never fatal: the
	// measurement outlives its instrumentation.
	artifactDir := ""
	if base := os.Getenv(artifactEnv); base != "" {
		artifactDir = filepath.Join(base, fmt.Sprintf("TestFailover-%d-%s", clients, time.Now().Format("150405")))
		if err := os.MkdirAll(artifactDir, 0o750); err != nil {
			t.Logf("artifact: %v", err)
			artifactDir = ""
		}
	}
	// Progress goes to stderr and, when an artifact directory exists, to a
	// progress.log inside it. The file is the reliable channel: `go test`
	// pipes and buffers the binary's stderr along with everything else
	// unless run with -v, which is exactly the silence WI 1285 exists to
	// end — an operator can tail the file regardless of how the test was
	// invoked. (Observed, not theorized: a 20,000-client run under a plain
	// `go test` produced zero live lines on stderr.)
	var progressFile *os.File
	if artifactDir != "" {
		if f, err := os.Create(filepath.Join(artifactDir, "progress.log")); err != nil { //nolint:gosec // operator-chosen dir
			t.Logf("artifact: %v", err)
		} else {
			progressFile = f
			defer func() { _ = progressFile.Close() }()
		}
	}
	emitProgress := func(line string) {
		fmt.Fprintln(os.Stderr, line)
		if progressFile != nil {
			_, _ = fmt.Fprintln(progressFile, line)
		}
	}
	// The ephemeral port range is per source and destination pair, so a
	// hundred thousand clients over three destinations need more than one
	// source address; loopback gives us 127.0.0.1 to 127.0.0.8.
	sources := []string{"127.0.0.1"}
	if clients > 20000 {
		sources = nil
		for i := 1; i <= 8; i++ {
			sources = append(sources, fmt.Sprintf("127.0.0.%d", i))
		}
	}
	// The remote-client mode (WI 1480): the clients run in a driver process,
	// usually on another host, dialling from the given sources at every client
	// count, and the nodes listen on the given addresses.
	remote, err := readRemoteSettings(os.Getenv, 3)
	if err != nil {
		t.Fatal(err)
	}
	if remote != nil {
		sources = remote.Sources
		if problems := readServerStartProblems(clients, remote.Nodes); len(problems) > 0 {
			t.Fatalf("remote-client run cannot start on this host: %s", strings.Join(problems, "; "))
		}
	}
	// Node debug logs are kept only on request: at a hundred thousand
	// clients they are enormous, and the artifact's bounded metrics
	// snapshots are the default answer to "what were the nodes doing".
	// HOARDMQ_SIM_NODELOG=1 (with an artifact dir) opts smaller runs in.
	// A recorded run uses the product's timings, ttl included, and the gate
	// the harness's (WI 1475). HOARDMQ_SIM_LEASE_TTL_MS still overrides the
	// ttl (WI 1292 round 4: a storm-loaded consensus core against a 2 s ttl
	// declared healthy survivors dead).
	set, err := failoverTimings(record, os.Getenv)
	if err != nil {
		t.Fatal(err)
	}
	setTTL := timingDefaults(set).TTL
	opts := Options{TTL: setTTL, Timings: set, FileUsers: clients}
	if remote != nil {
		opts.ListenAddrs = remote.Nodes
	}
	if v := os.Getenv("HOARDMQ_SIM_LEASE_TTL_MS"); v != "" {
		if ms, err := strconv.Atoi(v); err == nil && ms > 0 {
			opts.TTL = time.Duration(ms) * time.Millisecond
		}
	}
	if artifactDir != "" && os.Getenv("HOARDMQ_SIM_NODELOG") != "" {
		if f, err := os.Create(filepath.Join(artifactDir, "nodes.log")); err != nil { //nolint:gosec // operator-chosen dir
			t.Logf("artifact: %v", err)
		} else {
			opts.Log = f
			defer func() { _ = f.Close() }()
		}
	}
	c := New(t, opts)
	// Taken before the fill so git never runs inside the storm, and read
	// again when the artifact is written (WI 1379).
	prov := readProvenance(os.Args, os.Environ(), ".")
	eff := c.effective(clients)
	var pop population
	var remotePop *remotePopulation
	clientMode := "in-process"
	if remote == nil {
		eff.ClientSources, eff.ClientDial = clientDial(sources)
		dialSources := sources
		if len(dialSources) <= 1 {
			dialSources = nil
		}
		pop = newLocalPopulation(clients, c.Addresses(), dialSources, nil)
	} else {
		clientMode = "remote"
		rp, err := startRemotePopulation(*remote, clients, defaultRemoteOptions())
		if err != nil {
			t.Fatal(err)
		}
		// close is idempotent: this covers a failure before the fill, and the
		// teardown after the fill closes it again as a no-op.
		defer rp.close()
		info, err := rp.hello()
		if err != nil {
			emitProgress("client driver failed before the fill: " + err.Error())
			t.Fatal(err)
		}
		if why := driverBuildProblem(record, prov.Tree, info); why != "" {
			t.Fatalf("remote-client run refused: %s", why)
		}
		eff.ClientSources, eff.ClientDial = len(sources), info.Dial
		eff.ClientMode, eff.ClientHost, eff.NodeAddresses = clientMode, remote.destination(), remote.Nodes
		pop, remotePop = rp, rp
	}
	prov.Effective = &eff
	// The fill is bounded separately from everything after it. One context
	// over both means a slow fill silently truncates and the report prints a
	// deadline where a duration belongs.
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Minute)
	defer cancel()
	fillCtx, fillCancel := context.WithTimeout(ctx, 45*time.Minute)
	defer fillCancel()
	rtStart := readRuntime()
	netStart := readNetstat()
	crashHold, err := crashAt(os.Getenv)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	// The direct pause observer and the per-interval series (WI 1352). The
	// runtime's own metrics say what the runtime knows about; this says how
	// long the process was not running, whatever the reason. Elapsed times
	// are measured from the run's start, which the artifact records.
	stalls := newStallWatch(start)
	defer stalls.close()
	var intervals []intervalSample
	var intervalsMu sync.Mutex
	prevRT := rtStart
	// The nodes' stores are under t.TempDir(); the disk figures describe the
	// device that directory is on, resolved rather than assumed (WI 1375).
	prevHost := readHost(os.TempDir())
	// Called from the fill goroutine, from the reconnect loop and once at
	// window end. Closing the fill goroutine's stop channel does not wait
	// for it, so two goroutines can be in here at once: prevRT and the
	// series are both the mutex's, not just the series.
	sampleInterval := func(phase string, at time.Duration, cur runtimeSnapshot) (gc, sched float64, worst time.Duration, host hostDelta) {
		worst = stalls.takeWorst()
		curHost := readHost(os.TempDir())
		intervalsMu.Lock()
		gc, sched, ok := intervalPauses(prevRT, cur)
		host = hostInterval(prevHost, curHost)
		prevRT, prevHost = cur, curHost
		intervals = append(intervals, intervalSample{
			Phase: phase, AfterSec: at.Seconds(), GCPauseSec: gc, SchedMaxSec: sched,
			StallSec: worst.Seconds(), Measured: ok, Host: host,
		})
		intervalsMu.Unlock()
		return gc, sched, worst, host
	}
	snapshotIntervals := func() []intervalSample {
		intervalsMu.Lock()
		defer intervalsMu.Unlock()
		return append([]intervalSample(nil), intervals...)
	}
	// Fill-phase progress (WI 1285): a stderr line every 15 s from a
	// goroutine that reads one atomic and the runtime's own metrics —
	// nothing under a product lock, nothing on the connect path.
	var completed atomic.Int64
	stopFillProgress := make(chan struct{})
	if record {
		go func() {
			tick := time.NewTicker(15 * time.Second)
			defer tick.Stop()
			prev := readRuntime()
			for {
				select {
				case <-stopFillProgress:
					return
				case <-tick.C:
					cur := readRuntime()
					rate := (cur.MutexWaitSec - prev.MutexWaitSec) / cur.At.Sub(prev.At).Seconds()
					elapsed := time.Since(start)
					gc, sched, worst, host := sampleInterval("fill", elapsed, cur)
					emitProgress(progressLine("fill", elapsed, int(completed.Load()), clients, 0, 0, cur.Goroutines, rate) +
						pauseSuffix(worst, gc, sched) + host.suffix())
					prev = cur
				}
			}
		}()
	}
	failed, err := pop.fill(fillCtx, &completed)
	if err != nil {
		emitProgress("client driver failed during the fill: " + err.Error())
		t.Fatal(err)
	}
	connectAll := time.Since(start)
	if record {
		close(stopFillProgress)
	}
	rtFill := readRuntime()
	if artifactDir != "" {
		if d, err := writeProfiles(artifactDir, "fill"); err != nil {
			t.Logf("artifact: fill profiles: %v (%s)", err, d)
		}
	}
	fillTime := connectAll.Round(time.Millisecond).String()
	if fillCtx.Err() != nil {
		// Say so rather than print the ceiling as though it were measured.
		fillTime = fmt.Sprintf("not measured: the fill was cut off at %s with %d of %d connected", connectAll.Round(time.Second), clients-failed, clients)
	}
	t.Logf("%d clients connected and subscribed in %s (%d failed)", clients-failed, connectAll.Round(time.Millisecond), failed)
	defer pop.close()
	// The node with the most clients dies.
	count, err := pop.perNode()
	if err != nil {
		t.Fatal(err)
	}
	// The node with the most clients by default; a victim policy can put the
	// displaced clients' first target on the leader or away from it (WI 1493).
	names := make([]string, len(c.Nodes))
	counts := make([]int, len(c.Nodes))
	for i, n := range c.Nodes {
		names[i], counts[i] = n.Name, count[n.TCP]
	}
	leaderBefore, _ := agreedLeader(leaderViews(c, nil))
	victimPolicy := os.Getenv(victimEnv)
	vi, err := chooseVictim(victimPolicy, names, counts, leaderBefore)
	if err != nil {
		t.Fatal(err)
	}
	victim := c.Nodes[vi]
	target := stormTarget(names, victim.Name)
	profileAt, err := parseProfileAt(os.Getenv(profileAtEnv))
	if err != nil {
		t.Fatal(err)
	}
	var profilesTaken []string
	affected := count[victim.TCP]
	before := leaseCounters(c, victim)
	// The publish probe (WI 1618). It connects after the fill so the fill
	// time — a published figure — is unaffected, and publishes from here
	// until watching ends. A probe that cannot be built is reported and does
	// not fail the run: the measurement outlives its instrumentation.
	var pr *probe
	var probeRec *probeRecord
	if n := probePairs(); n > 0 {
		built, err := newProbe(ctx, n, probeGroupCount(), probeDefaultRate, c)
		if err != nil {
			t.Logf("probe: %v — the run continues without a publish figure", err)
			built.close()
		} else {
			pr = built
			defer pr.close()
			pr.start(ctx)
		}
	}
	// The baseline, taken before the crash: a remote driver replies only once
	// it has it, so the crash cannot precede it.
	if err := pop.mark(); err != nil {
		emitProgress("client driver failed at the mark: " + err.Error())
		t.Fatal(err)
	}
	// The flight recorder (WI 1284, Go 1.25+) keeps a moving window of the
	// execution trace in memory. Snapshots are written when the first
	// survivor fences (WI 1474: the window covers the leaderless stretch
	// before it), when the stall detector fires — reconnects stopped making
	// progress — and at window end if neither happened. The recorder failing
	// never fails the measurement.
	var recorder *trace.FlightRecorder
	if record && artifactDir != "" {
		recorder = trace.NewFlightRecorder(trace.FlightRecorderConfig{MinAge: 45 * time.Second, MaxBytes: 512 << 20})
		if err := recorder.Start(); err != nil {
			t.Logf("flight recorder: %v", err)
			recorder = nil
		} else {
			// writeTrace stops and nils the recorder once a snapshot is
			// taken; the deferred guard covers the paths that never take one.
			defer func() {
				if recorder != nil {
					recorder.Stop()
					recorder = nil
				}
			}()
		}
	}
	// writeTrace writes a snapshot and, unless keepRunning, stops the
	// recorder; a later snapshot is then skipped.
	writeTrace := func(tag string, keepRunning bool) (string, error) {
		if recorder == nil {
			return "", fmt.Errorf("the flight recorder is not running")
		}
		name := "trace-" + tag + ".out"
		f, err := os.Create(filepath.Join(artifactDir, name)) //nolint:gosec // operator-chosen dir
		if err != nil {
			t.Logf("flight recorder: %v", err)
			return "", err
		}
		n, werr := recorder.WriteTo(f)
		_ = f.Close()
		t.Logf("flight recorder: wrote %s (%d bytes, err=%v)", name, n, werr)
		if !keepRunning {
			recorder.Stop()
			recorder = nil
		}
		return name, werr
	}
	if crashHold > 0 {
		wait, err := crashWait(start, time.Now(), crashHold)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(wait)
	}
	// The survivors' counters are cumulative from process start, and the fill
	// goes through the same listeners: what the storm added is the difference
	// from here.
	atCrash := stormStats(c, victim)
	crashAt := time.Now()
	c.Crash(victim)
	if err := pop.crashed(); err != nil {
		emitProgress("client driver failed at the crash: " + err.Error())
		t.Fatal(err)
	}
	// WI 1474's evidence, from the crash: a CPU profile over the storm's first
	// minute, a one-second sampler of each survivor's peers and the cluster
	// sockets, and a trace window taken at the first fence.
	cpuRec := CPUProfileRecord{Unavailable: "no artifact directory"}
	var stopCPU func(time.Duration) CPUProfileRecord
	fenceRec := FenceTraceRecord{Unavailable: "no fence during the window"}
	fenceTraced := false
	var sampler *peerSampler
	if artifactDir != "" {
		stopCPU, cpuRec = startCPUProfile(artifactDir, "cpu-storm.pb.gz", time.Since(crashAt))
		var sources []peerSource
		var ports []string
		for _, n := range c.Nodes {
			ports = append(ports, portOf(n.Cluster))
			if n == victim {
				continue
			}
			if srv := n.Server(); srv != nil && srv.Cluster() != nil {
				sources = append(sources, peerSource{name: n.Name, peers: srv.Cluster().Peers})
			}
		}
		sampler = startPeerSampler(crashAt, sources, ports)
	}
	survivorFences := func() map[string]int64 {
		counts := map[string]int64{}
		for _, n := range c.Nodes {
			if n == victim {
				continue
			}
			if srv := n.Server(); srv != nil {
				counts[n.Name] = srv.LeaseFences()
			}
		}
		return counts
	}
	// Wait for every affected client to be back online. This is a
	// measurement, not a bar: a run where some client never came back
	// records that instead of failing, and the report says so.
	// The loop is also the window's progress reporter and stall detector
	// (WI 1285 / 1284): it already visits every client each pass, so the
	// periodic line and the storm capture add reads of client atomics and
	// runtime metrics only — nothing under a product lock.
	deadline := time.Now().Add(10 * time.Minute)
	// The definition of recovered, owner-decided 2026-09-21 (WI 1618): the
	// instant is the start of the earliest interval of length hold in which
	// every still-trying client is recovered. recWatch decides it; connWatch
	// runs the old reading — the first return of the *online* count — beside
	// it, so one batch publishes both and the difference can be read.
	hold := holdSeconds(record)
	// The target is every client that connected *and* subscribed: the fill
	// counts a client whose Subscribe returned an error as failed, so every
	// client in the target holds a filter and the predicate cannot pass over
	// a population subscribed to nothing. A target of zero would satisfy the
	// definition vacuously, so the run refuses instead.
	if clients-failed == 0 {
		t.Fatalf("no client connected and subscribed (%d of %d failed): there is nothing to recover", failed, clients)
	}
	recWatch := newHoldWatch(clients-failed, hold, deadline)
	// The survivors' counters at the instant: one snapshot each time a hold
	// begins, and the one for the hold that confirmed the instant is kept.
	snaps := newInstantSnapshots(func(forStart time.Time) counterSnapshot {
		return snapshotSurvivors(c, victim, forStart)
	})
	connWatch := newHoldWatch(clients-failed, 0, deadline)
	holdStart, holdMin := time.Time{}, 0
	online, recovered := 0, 0
	// The headline figure is an endpoint; these are the shape it hides. A
	// round that closes its last clients stops being watched and a round
	// that misses them is watched to the end of the window, so the miss case
	// reports where it ended up rather than where it got to — round 12's
	// headline was 88,570 when its peak had been 99,911 (WI 1373).
	shape := newOnlineShape(clients-failed, crashAt)
	onlineSec := &onlineSeries{from: crashAt}
	// The recovered count on the same once-a-second scale, beside the online
	// one. The online series stays as it is: every batch published before
	// this work item reports it, and the two are read against each other.
	recoveredSec := &onlineSeries{from: crashAt}
	prevOnline, lastIncrease := 0, time.Now()
	lastProgress, prevWait := time.Now(), rtFill.MutexWaitSec
	stormCaptured := false
	stallSeen := ""
	// Watching runs to the deadline, and on to deadline+hold while an
	// interval that began inside the window is still completing: the window
	// bounds the instant, not the confirmation of it.
	for {
		now := time.Now()
		if now.After(deadline) && (recWatch.holdStart().IsZero() || now.After(deadline.Add(hold))) {
			break
		}
		s, err := pop.sample()
		if err != nil {
			emitProgress("client driver failed during the window: " + err.Error())
			t.Fatal(err)
		}
		online, recovered = s.Online, s.Recovered
		shape.observe(online, recovered)
		onlineSec.observe(s.At, online)
		recoveredSec.observe(s.At, recovered)
		connWatch.observe(s.At, online)
		if artifactDir != "" && len(profileAt) > 0 && time.Since(crashAt) >= profileAt[0] {
			tag := fmt.Sprintf("at%d", int(profileAt[0]/time.Second))
			profileAt = profileAt[1:]
			if d, err := writeProfiles(artifactDir, tag); err != nil {
				t.Logf("artifact: %s profiles: %v (%s)", tag, err, d)
			} else {
				profilesTaken = append(profilesTaken, tag)
			}
		}
		// The lowest recovered count seen inside the interval now being held,
		// so a run says how nearly it broke the hold it completed.
		if start := recWatch.holdStart(); start != holdStart {
			holdStart, holdMin = start, recovered
		} else if !start.IsZero() && recovered < holdMin {
			holdMin = recovered
		}
		settled := recWatch.observe(s.At, recovered)
		snaps.observe(recWatch.holdStart())
		if settled {
			break
		}
		if stopCPU != nil && now.Sub(crashAt) >= time.Minute {
			cpuRec, stopCPU = stopCPU(now.Sub(crashAt)), nil
		}
		if artifactDir != "" && !fenceTraced {
			if node, ok := firstFence(survivorFences()); ok {
				fenceTraced = true
				fenceRec = FenceTraceRecord{Node: node, AfterCrashSec: now.Sub(crashAt).Seconds()}
				if name, err := writeTrace("fence", !stormCaptured); err != nil {
					fenceRec.Unavailable = err.Error()
				} else {
					fenceRec.File = name
				}
			}
		}
		// Net progress resets the stall clock: at 100k the count oscillates by
		// ±1 as clients drop and return, which "any increase" misread as
		// recovery and the detector never fired (round 2).
		if online >= prevOnline+max(1, affected/1000) {
			prevOnline, lastIncrease = online, now
		}
		if artifactDir != "" && !stormCaptured && (now.Sub(lastIncrease) > 30*time.Second || now.Sub(crashAt) > 2*time.Minute) {
			// The storm capture: contention profiled while it is happening.
			// The stall detector fires first when reconnects have stopped;
			// the two-minute fallback fires mid-window otherwise.
			stormCaptured = true
			if now.Sub(lastIncrease) > 30*time.Second {
				stallSeen = now.Sub(crashAt).Round(time.Second).String()
			}
			if d, err := writeProfiles(artifactDir, "storm"); err != nil {
				t.Logf("artifact: storm profiles: %v (%s)", err, d)
			} else {
				t.Logf("artifact: storm profiles written %s after the crash (cost %s)", now.Sub(crashAt).Round(time.Second), d)
			}
			_, _ = writeTrace("storm", !fenceTraced)
		}
		if record && now.Sub(lastProgress) >= 15*time.Second {
			cur := readRuntime()
			rate := (cur.MutexWaitSec - prevWait) / now.Sub(lastProgress).Seconds()
			gc, sched, worst, host := sampleInterval("reconnect", now.Sub(start), cur)
			emitProgress(progressLine("reconnect", now.Sub(crashAt), online, clients-failed,
				s.Refusals, s.Fails, cur.Goroutines, rate) +
				pauseSuffix(worst, gc, sched) + host.suffix() + stormSuffix(c, victim))
			prevWait, lastProgress = cur.MutexWaitSec, now
		}
		time.Sleep(50 * time.Millisecond)
	}
	// The hold is inside the measurement (WI 1618), so this record describes
	// the interval that confirmed the instant rather than a separate watch
	// kept afterwards: its length, the lowest recovered count seen inside it,
	// and where the counts ended.
	instant, whyNot := recWatch.verdict()
	atInstant, haveAtInstant, lateAtInstant := snaps.finish(instant)
	var instantRec *instantRecord
	if haveAtInstant {
		r := newInstantRecord(atInstant, instant, snaps.begun, lateAtInstant)
		instantRec = &r
	} else {
		instantRec = &instantRecord{Snapshots: snaps.begun, Absent: "no recovery instant: " + whyNot}
	}
	if pr != nil {
		rec := pr.result(crashAt, instant)
		probeRec = &rec
	}
	holdRec := holdRecord{Seconds: int(hold / time.Second), MinRecovered: holdMin,
		RecoveredAtEnd: recovered, OnlineAtEnd: online}
	if whyNot != recoveryReached {
		holdRec.MinRecovered = -1
	}
	if stopCPU != nil {
		cpuRec = stopCPU(time.Since(crashAt))
	}
	var peerSamples []PeersSample
	var peerSamplesDropped int
	peerSamplesNote := ""
	if sampler != nil {
		var stuck bool
		peerSamples, peerSamplesDropped, stuck = sampler.finish()
		if stuck {
			peerSamplesNote = fmt.Sprintf("a sample was still being read %s after the window closed; the series ends before it", peerSamplerFinishWait)
		}
	}
	var fullReconnect string
	if whyNot == recoveryReached {
		fullReconnect = instant.Sub(crashAt).Round(time.Millisecond).String()
	} else {
		t.Logf("%d of %d clients recovered (%d online) when watching ended: %s", recovered, clients-failed, online, whyNot)
		fullReconnect = fmt.Sprintf("not reached in 10m: %d of %d recovered, %d online — %s", recovered, clients-failed, online, whyNot)
	}
	connackAt, connackWhyNot := connWatch.verdict()
	// Per-client reconnect times, and the refusals and drops the storm
	// produced, from the counters each client kept since the snapshot above.
	sum, err := pop.finish()
	if err != nil {
		emitProgress("client driver failed at the finish: " + err.Error())
		t.Fatal(err)
	}
	times := sum.Reconnects
	refusals, drops, fails, droppedClients := sum.Refusals, sum.Drops, sum.Fails, sum.DroppedClients
	pct := func(p float64) time.Duration { return percentile(times, p) }
	var remoteRec *remoteRecord
	if remotePop != nil {
		remoteRec = remotePop.record()
	}
	after := leaseCounters(c, victim)
	leaderAfter, _ := agreedLeader(leaderViews(c, victim))
	// One final interval, so a run shorter than a progress interval still
	// reports its pause figures rather than "not measured" (WI 1352).
	sampleInterval("window-end", time.Since(start), readRuntime())
	series := snapshotIntervals()
	prov.TreeAtEnd = checkTree(".", prov.Tree)
	endRead := time.Now()
	endStats := stormStats(c, victim)
	servedEmpty := servedEmptyTotals(endStats)
	drop := newDropTimeline(crashAt, endRead, victim.Name, leaderAfter, endStats, instant, whyNot == recoveryReached)
	// The run's artifact is assembled before its report, so the report and
	// run.json state the same verdict against the pass bounds (WI 1734).
	art := failoverArtifact{
		Clients: clients, Failed: failed, Victim: victim.Name, Affected: affected,
		FillTime: fillTime, OnlineAtEnd: online, RecoveredAtEnd: recovered,
		Hold: holdOrNil(holdRec), ReconnectedOfDropped: len(times),
		DroppedClients: droppedClients, Refusals: refusals, Drops: drops, Fails: fails,
		SubRefusals: sum.SubRefusals, SubRefusedClients: sum.SubRefusedClients,
		DropCauses:            dropCauseMap(sum.DropCauses),
		StallSeenAfter:        stallSeen,
		StoreDir:              c.Dir(),
		Recovery:              fullReconnect,
		RecoverySec:           recoverySeconds(instant, crashAt),
		RecoveryReason:        whyNot,
		ConnackRecoverySec:    recoverySeconds(connackAt, crashAt),
		ConnackRecoveryReason: connackWhyNot,
		SessionLost:           sum.SessionLost,
		SessionLostClients:    sum.SessionLostClients,
		SessionLostSample:     sum.SessionLostSample,
		ServedEmpty:           servedEmpty,
		DropTimeline:          &drop,
		SubDowngraded:         sum.SubDowngraded,
		Probe:                 probeRec,
		PeakOnline:            shape.peak, PeakAtSec: shape.peakAt.Seconds(),
		TroughOnline: shape.trough, TroughAtSec: shape.troughAt.Seconds(),
		NinetyNineSec: shape.ninetyNineSeconds(), NineNineNineSec: shape.nineNineNineSeconds(),
		// Fill ops = successful connects+subscribes; window ops = the
		// storm's connect outcomes (refusals + fails + reconnections).
		Fill:           deltaBetween(rtStart, rtFill, clients-failed),
		LeaseOpsBefore: sanitizeNaN(before.ops), LeaseOpsAfter: sanitizeNaN(after.ops),
		ProposalsBefore: sanitizeNaN(before.proposals), ProposalsAfter: sanitizeNaN(after.proposals),
		MetricsBefore: before.metrics, MetricsAfter: after.metrics,
		MetricsAtInstant: atInstant.Metrics, MetricsAtInstantTiming: instantRec,
		MetricsTimedOutBefore: before.timedOut, MetricsTimedOutAfter: after.timedOut,
		StormAtCrash: atCrash,
		ClientMode:   clientMode,
		Remote:       remoteRec,

		VictimPolicy:      victimPolicy,
		LeaderBeforeCrash: leaderBefore,
		LeaderAfter:       leaderAfter,
		StormTarget:       target,
		OnlineSeries:      onlineSec.samples,
		RecoveredSeries:   recoveredSec.samples,
		ProfilesAt:        profilesTaken,
	}
	art.Provenance = prov
	verdict := judgeRun(boundsInputOf(&art))
	art.Bounds = &verdict
	t.Logf("pass bounds: %s", boundsVerdictLine(verdict))
	report := fmt.Sprintf(`# Failover measurement — %s

Machine: %d CPUs, %s; Go %s. %s, no TLS, %s timings: lease ttl %s, heartbeat %d ms × %d misses, raft tick %d ms, takeover timeout (the move grace) %d ms, replication ack timeout %d ms, connect timeout %d s, replicas 2, client source addresses %s.

| Quantity | Value |
|---|---|
| Clients (durable, one QoS 1 subscription each) | %d (%d failed to connect) |
| Time to connect and subscribe all | %s |
| Node crashed | %s, holding %d clients |
| Time from crash to recovered (every client connected and subscribed, held %s) | %s |
| The same run under the old reading (first return of the online count, no hold) | %s |
| Runs this report covers | %s |
| Most online at once, and when | %s |
| Fewest online at once, and when | %s |
| Time from crash to 99%% recovered | %s |
| Time from crash to 99.9%% recovered | %s |
| Lowest recovered count inside the hold that confirmed it | %s |
| Sessions the broker did not have on a reconnect, and clients affected | %d across %d clients |
| Sessions the survivors counted serving empty, by reason | %s |
| Subscriptions granted below the QoS asked for | %d |
| Publish probe, through the crash and the hold | %s |
| Survivors' counters split at the recovery instant | %s |
| Crash to the shard map dropping the crashed node, s after the crash | %s |
| Per-client reconnect time p50 / p90 / p99 / max | %s / %s / %s / %s |
| Clients the percentiles cover (dropped, then accepted again) | %d of %d that lost a connection |
| Connect refusals seen (0x88/0x9C) during failover | %d |
| Established connections lost | %d |
| Connect attempts that never reached a CONNACK | %d |
| Subscriptions the broker refused, and clients affected | %d across %d clients |
| Lease operations on the survivors (acquire) before → after | %s |
| Lease proposals (batches) on the survivors before → after | %s |
| Worst the process was not running, in any one interval | %s |
| Moments it was not running for over %s | %s |
| GC pause total across the run / worst scheduler latency in any interval | %s / %s |

Whether a silence was this process or the peer: the four figures above come
from a sampler that sleeps 50 ms and measures how long the sleep took, and
from the runtime's own histograms **subtracted per interval** rather than
read cumulatively — a cumulative quantile says "somewhere in this run", which
is what made WI 1340 round 8's 21.5 s uninterpretable. The GC total is a
bucket-midpoint approximation and the scheduler figure is a bucket's upper
bound, so both are magnitudes, not measurements. The sampler reports how
long the process was stopped and never why. **The host figures beside it now
say why** (WI 1375): Linux pressure stall information gives, per interval,
the time some task was stalled on cpu, io or memory and the time *every*
runnable task was — the latter directly comparable with the stall figure —
together with the data directory's device utilisation and queue depth, this
process's own write rate, iowait, steal, and the dirty and writeback levels.
A stall with io pressure beside it is a disk wait; a stall with cpu pressure
and no io is contention; a stall with neither is something else again, and
that is now a finding rather than the end of the enquiry. Figures the host
does not expose are reported as unavailable, never as zero. Per-interval
figures and the timestamped stalls are in run.json.

Method: clients connect round-robin over the three nodes with Clean Start 0
and a 3600 s expiry, each subscribing to its own topic. The node with the
most clients is crashed (no DISCONNECT, no checkpoint). Each client
reconnects to the next node in its order and follows any Server Reference;
a refused connect (minority or unavailable) tries the next. Between
attempts a client waits a seeded backoff starting at %s and doubling to a
cap of %s, jittered, and returning to the floor once a CONNACK accepts it.
**Recovered means connected with every subscription granted on the current
connection, and the instant is the start of the earliest interval of length
%s throughout which every still-trying client was continuously recovered**
(owner decision, 2026-09-21, WI 1618). The hold is part of the measurement,
not a check after it: a population that comes back and is then closed moves
the instant out to the interval that actually held, which is what WI 1515's
defect did unobserved under the old reading. A run reports an instant only if
the recovered count was seen below the target after the crash. A round that
reaches the finish line is not observed past its hold and a round that does
not is observed to the end of the ten-minute window, so two rounds are not
watched for the same length of time, and the "fewest online" row of a round
that missed is a trough that a round which closed might also have had. Peak,
trough and the two percentile figures are reported for exactly this reason
(WI 1373).
The reconnect time is measured per client from its first
dropped-connection event to the first CONNACK that accepted it afterwards,
so the percentiles cover only clients that came back; those that never did
contribute nothing to them and are counted in the row above. The reconnect percentiles are of the *connection*, not of the definition
above: a client counts there at the CONNACK that accepted it, which is why
the headline and the percentiles can differ. Until WI 1618 the headline was
that same reading — a client whose resubscribe the broker refused was counted
back while receiving nothing, and the refusals row was the only thing that
said so, as an upper bound. It is now exact: such a client is not recovered
and the headline waits for it. The old reading is published beside the new
one for this batch so the two can be compared. **A session the broker did not
have** is the other way a client can be counted back to nothing: it
reconnects, is told Session Present false, loses its queue, and re-subscribes
into a session the broker built empty (WI 1653). That row counts it. Lease
operations and proposals are read from the survivors' metrics: the ratio of
acquire operations to proposals is the batching the design counts on.

Caveats: %s; no connect-rate limiter exists yet (work item 1283), so the
survivors accept the storm as fast as it arrives. The number is a
measurement of this setup, not a bar.
`, time.Now().Format("2006-01-02"), runtime.NumCPU(), memory(), runtime.Version(), topologyLine(remote), eff.TimingSet, c.o.TTL, eff.HeartbeatMs, eff.HeartbeatMisses, eff.RaftTickMs, eff.TakeoverTimeoutMs, eff.AckTimeoutMs, eff.ConnectTimeoutSec, strings.Join(sources, ", "), clients, failed, fillTime,
		victim.Name, affected, hold, fullReconnect, connackLine(connackAt, connackWhyNot, crashAt), runsCovered(), shape.peakLine(), shape.troughLine(),
		shape.toNinetyNine(), shape.toNineNineNine(), holdMinLine(holdRec, whyNot), sum.SessionLost, sum.SessionLostClients, servedEmptyLine(servedEmpty), sum.SubDowngraded, probeLine(probeRec), instantSplitLine(atInstant.Metrics, after.metrics, instantRec), dropLine(&drop),
		pct(0.5).Round(time.Millisecond), pct(0.9).Round(time.Millisecond), pct(0.99).Round(time.Millisecond), pct(1).Round(time.Millisecond),
		len(times), droppedClients, refusals, drops, fails, sum.SubRefusals, sum.SubRefusedClients, deltas(before, after, "ops"), deltas(before, after, "proposals"),
		worstIntervalStall(series), stallThreshold, stallSummary(stalls), gcTotal(series), worstIntervalSched(series),
		hold, backoffFloor, backoffCap, caveatLine(remote)) + boundsSection(verdict) + remoteSection(remoteRec) + placementSection(victimPolicy, leaderBefore, leaderAfter, victim.Name, target) + provenanceSection(prov)
	t.Log("\n" + report)
	if path := treeReportPath(record, artifactDir, time.Now()); path != "" {
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(report), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Logf("written %s", path)
	}
	if artifactDir != "" {
		// A window-end trace only when neither the fence nor the storm took
		// one: otherwise it would add a large file covering the quiet tail.
		if stormCaptured || fenceTraced {
			if recorder != nil {
				recorder.Stop()
				recorder = nil
			}
		} else {
			_, _ = writeTrace("end", false)
		}
		if d, err := writeProfiles(artifactDir, "end"); err != nil {
			t.Logf("artifact: end profiles: %v (%s)", err, d)
		}
		rtEnd := readRuntime()
		// The window's runtime delta ends after the end profiles; the storm
		// and accept-queue counters are read at window end.
		art.Window = deltaBetween(rtFill, rtEnd, int(refusals+fails)+len(times))
		art.Storm, art.Netstat = stormStats(c, victim), netstatDelta(netStart, readNetstat())
		// WI 1352: what the process itself was doing, per interval and at
		// the moments it stopped. Elapsed times are measured from the run's
		// start, and CrashAfterSec is where the crash falls on that scale.
		art.Environment = readEnvironment()
		art.CPUProfile, art.FenceTrace = cpuRec, fenceRec
		art.PeerSamples, art.PeerSamplesDropped, art.PeerSamplesNote = peerSamples, peerSamplesDropped, peerSamplesNote
		art.CrashAfterSec = crashAt.Sub(start).Seconds()
		art.Intervals = snapshotIntervals()
		art.Stalls, art.StallsDropped = stalls.report()
		if raw, err := json.MarshalIndent(art, "", " "); err != nil {
			t.Logf("artifact: marshal: %v", err)
		} else if err := os.WriteFile(filepath.Join(artifactDir, "run.json"), raw, 0o640); err != nil { //nolint:gosec // operator-chosen dir
			t.Logf("artifact: %v", err)
		}
		if err := os.WriteFile(filepath.Join(artifactDir, "report.md"), []byte(report), 0o640); err != nil { //nolint:gosec // operator-chosen dir
			t.Logf("artifact: %v", err)
		}
		if err := writeReconnects(filepath.Join(artifactDir, "reconnects.json"), times); err != nil {
			t.Logf("artifact: %v", err)
		}
		// Last, and only here: the marker is what tells a watcher this
		// measurement is finished. Everything above it is what it certifies,
		// so it must not move earlier — a marker written before the artifact
		// certifies nothing, and a driver that ends the run on seeing it
		// would truncate the files still being written (WI 1374).
		if err := writeComplete(artifactDir, clients, fullReconnect); err != nil {
			t.Logf("artifact: completion marker: %v", err)
		}
		t.Logf("artifact written: %s", artifactDir)
	}
}

// failoverArtifact is the failover measurement's run.json: phases with
// their runtime deltas, the storm counters, and the lease metrics the
// report quotes — the raw evidence beside results/'s prose.
type failoverArtifact struct {
	Clients              int    `json:"clients"`
	Failed               int    `json:"failed"`
	Victim               string `json:"victim"`
	Affected             int    `json:"affected"`
	FillTime             string `json:"fill_time"`
	OnlineAtEnd          int    `json:"online_at_end"`
	ReconnectedOfDropped int    `json:"reconnected_of_dropped"`
	DroppedClients       int    `json:"dropped_clients"`
	Refusals             int64  `json:"refusals"`
	Drops                int64  `json:"drops"`
	Fails                int64  `json:"fails"`
	StallSeenAfter       string `json:"stall_seen_after,omitempty"`
	// Hold is the watch kept after every client was back, when one was asked
	// for: the counters in this artifact then cover it too.
	Hold *holdRecord `json:"hold,omitempty"`
	// StoreDir is the root every node's store is under. A driver that ends a
	// run once its marker appears skips t.TempDir's cleanup, so it needs to
	// be told what to remove rather than deriving it (WI 1377).
	StoreDir string `json:"store_dir,omitempty"`
	// Recovery is the report's headline in both forms, under the definition
	// of WI 1618. RecoverySec is negative when no instant was reached, so
	// "did not reach" can never be read as a duration of zero — two of the
	// four runs in the 2026-09-07 repeat batch did not reach, and a driver
	// that averaged them as zero would report the best result ever recorded
	// (WI 1374).
	Recovery    string  `json:"recovery"`
	RecoverySec float64 `json:"recovery_sec"`
	// RecoveryReason is empty when an instant was reached and otherwise says
	// which way it was not: never fell, never returned, returned after the
	// window closed, or the hold did not complete.
	RecoveryReason string `json:"recovery_reason,omitempty"`
	// ConnackRecoverySec is the *old* definition on the same run — the first
	// return of the online count, with no hold and no subscriptions — so one
	// batch carries both and the difference can be read. Negative on the same
	// terms as RecoverySec.
	ConnackRecoverySec    float64 `json:"connack_recovery_sec"`
	ConnackRecoveryReason string  `json:"connack_recovery_reason,omitempty"`
	// RecoveredAtEnd is the count under the new definition where watching
	// ended, beside OnlineAtEnd under the old one.
	RecoveredAtEnd int `json:"recovered_at_end"`
	// SessionLost counts CONNACKs that told a client with a session that the
	// broker had none, and the clients that suffered one; SubDowngraded the
	// SUBACKs that granted less than was asked for.
	SessionLost        int64 `json:"session_lost"`
	SessionLostClients int   `json:"session_lost_clients"`
	// SessionLostSample names up to sessionLostSampleCap of those clients, so
	// a run can be joined with the nodes' logs; in-process populations only.
	SessionLostSample []string `json:"session_lost_sample,omitempty"`
	// ServedEmpty is the survivors' own count of the same event, by reason
	// (lease.NoteServedEmpty): a CONNECT answered Session Present false
	// although the client's lease showed a session existed.
	ServedEmpty   map[string]int64 `json:"served_empty"`
	SubDowngraded int64            `json:"sub_downgraded"`
	// Probe is the publish probe's record, absent when it did not run.
	Probe *probeRecord `json:"probe,omitempty"`
	// Bounds is the run judged against the owner's pass bounds (WI 1734).
	Bounds *boundsVerdict `json:"bounds,omitempty"`
	// DropTimeline is each step from the crash to the shard map dropping
	// the crashed node, and each survivor's forget, from the crash mark.
	DropTimeline *dropTimeline `json:"drop_timeline,omitempty"`
	// The shape the headline hides (WI 1373), machine readable.
	PeakOnline      int                `json:"peak_online"`
	PeakAtSec       float64            `json:"peak_at_sec"`
	TroughOnline    int                `json:"trough_online"`
	TroughAtSec     float64            `json:"trough_at_sec"`
	NinetyNineSec   float64            `json:"ninety_nine_sec"`
	NineNineNineSec float64            `json:"ninety_nine_nine_sec"`
	Fill            runtimeDelta       `json:"fill"`
	Window          runtimeDelta       `json:"window"`
	LeaseOpsBefore  map[string]float64 `json:"lease_ops_before"`
	LeaseOpsAfter   map[string]float64 `json:"lease_ops_after"`
	ProposalsBefore map[string]float64 `json:"proposals_before"`
	ProposalsAfter  map[string]float64 `json:"proposals_after"`
	// MetricsBefore and MetricsAfter are each survivor's storm metrics just
	// before the crash and at window end, keyed name{label="value",...}. A
	// survivor whose snapshot did not return within its bound is named in
	// the TimedOut list for that read and absent from the map: absent is
	// not zero.
	MetricsBefore         map[string]map[string]float64 `json:"metrics_before"`
	MetricsAfter          map[string]map[string]float64 `json:"metrics_after"`
	MetricsTimedOutBefore []string                      `json:"metrics_timed_out_before,omitempty"`
	// MetricsAtInstant is the same snapshot taken when the hold that
	// confirmed the recovery instant began, so a counter can be split at the
	// instant: what rose after it is MetricsAfter minus this (WI 1734).
	// MetricsAtInstantTiming says when each read began after the instant and
	// how long it took, or why there is no snapshot.
	MetricsAtInstant       map[string]map[string]float64 `json:"metrics_at_instant,omitempty"`
	MetricsAtInstantTiming *instantRecord                `json:"metrics_at_instant_timing,omitempty"`
	MetricsTimedOutAfter   []string                      `json:"metrics_timed_out_after,omitempty"`
	// Storm is each survivor's admission and lease-work picture at window
	// end (WI 1292 round 3), read without the session manager's lock.
	Storm map[string]broker.StormStats `json:"storm"`
	// StormAtCrash is the same block read just before the crash. The counters
	// are cumulative, so the storm's own share is Storm minus this.
	StormAtCrash map[string]broker.StormStats `json:"storm_at_crash"`
	// Netstat holds the kernel's accept-queue counters over the run: under
	// pause-accept, ListenOverflows/ListenDrops are the truth about the queue
	// the broker deliberately lets build.
	Netstat map[string]int64 `json:"netstat_delta"`
	// Environment is what a starvation figure has to be read against
	// (WI 1352).
	Environment Environment `json:"environment"`
	// Provenance is how this run was produced: the command, the settings,
	// the effective timings and the source tree (WI 1379).
	Provenance Provenance `json:"provenance"`
	// WI 1474's evidence: the storm's CPU profile (split by the sim and node
	// pprof labels), the trace window taken at the first fence, and the
	// one-second series of each survivor's peers and the cluster sockets.
	CPUProfile         CPUProfileRecord `json:"cpu_profile"`
	FenceTrace         FenceTraceRecord `json:"fence_trace"`
	PeerSamples        []PeersSample    `json:"peer_samples,omitempty"`
	PeerSamplesDropped int              `json:"peer_samples_dropped,omitempty"`
	PeerSamplesNote    string           `json:"peer_samples_note,omitempty"`
	// CrashAfterSec places the crash on the same scale as Intervals and
	// Stalls, both of which are measured from the run's start.
	CrashAfterSec float64 `json:"crash_after_sec"`
	// Intervals is the per-progress-interval series: each interval's own GC
	// pause total and worst scheduler latency, subtracted out of the
	// histograms rather than read off a cumulative quantile, beside the
	// worst the direct observer saw. This is what says *when*.
	Intervals []intervalSample `json:"intervals,omitempty"`
	// Stalls are the moments this process was not running, over the
	// observer's threshold; StallsDropped is how many were seen past the
	// bound and not kept, so a truncated list cannot read as a quiet run.
	Stalls        []StallEvent `json:"stalls,omitempty"`
	StallsDropped int          `json:"stalls_dropped,omitempty"`
	// ClientMode is where the clients ran: "in-process" or "remote" (WI
	// 1480). Remote carries the driver's build and host, the client host's
	// per-second series and the client-limited verdict.
	ClientMode string        `json:"client_mode"`
	Remote     *remoteRecord `json:"remote_clients,omitempty"`
	// Leader placement (WI 1493): the victim policy when one was set, the
	// leader every node agreed on before the crash and every survivor at
	// window end, the node the displaced clients tried first, the online count
	// once a second from the crash, and the fixed-time profiles written.
	VictimPolicy      string   `json:"victim_policy,omitempty"`
	LeaderBeforeCrash string   `json:"leader_before_crash"`
	LeaderAfter       string   `json:"leader_after"`
	StormTarget       string   `json:"storm_target"`
	OnlineSeries      []int    `json:"online_series,omitempty"`
	RecoveredSeries   []int    `json:"recovered_series,omitempty"`
	ProfilesAt        []string `json:"profiles_at,omitempty"`
	// Subscribe refusals during the window (WI 1509). A client that
	// reconnected but had its resubscribe refused is online, is counted in
	// the recovery figure, and is subscribed to nothing.
	//
	// SubRefusedClients counts clients refused at least once since the mark,
	// so it is an upper bound on how many the recovery figure over-counts,
	// not the exact number: a client refused early and granted on a later
	// reconnect is in it and did recover. An exact figure needs per-connection
	// subscription state on the client, and nothing has yet shown a case that
	// needs it: no batch before this one recorded refusals at all, so how
	// often it happens under a storm is not known. Whether a refused client
	// counts as recovered at all is the owner's to settle.
	SubRefusals       int64 `json:"sub_refusals"`
	SubRefusedClients int   `json:"sub_refused_clients"`
	// DropCauses says what ended each lost connection, keyed by cause name
	// (WI 1495). `drops` alone is a count with no cause, which is why the
	// published batches cannot say whether an excess drop was a takeover
	// detaching a live client or a transport that simply went away.
	DropCauses map[string]int64 `json:"drop_causes,omitempty"`
}

// boundsInputOf is the same record read from a run's artifact in memory, so
// the run judges exactly what a later reading of its run.json would.
func boundsInputOf(art *failoverArtifact) boundsInput {
	in := boundsInput{
		Clients: art.Clients, RecoverySec: art.RecoverySec, RecoveryReason: art.RecoveryReason,
		NinetyNineSec: art.NinetyNineSec, NineNineNineSec: art.NineNineNineSec,
		SubDowngraded: art.SubDowngraded, SessionLost: art.SessionLost, SessionLostClients: art.SessionLostClients,
		Probe: art.Probe, Hold: art.Hold, ClientMode: art.ClientMode,
		StormTarget: art.StormTarget, LeaderAfter: art.LeaderAfter,
	}
	in.Provenance.Host = art.Provenance.Host
	if e := art.Provenance.Effective; e != nil {
		takeover := e.TakeoverTimeoutMs
		in.Provenance.Effective = &boundsEffective{TimingSet: e.TimingSet, TakeoverTimeoutMs: &takeover}
	}
	return in
}

// stormStats reads every survivor's StormStats. Most of it is atomic reads,
// but the lease liveness, consensus status and peer reads take locks a storm
// can hold, which is why each part's read time is in StormStats.ReadMs
// (WI 1474: one progress line's figures arrived about 25 s after its
// timestamp).
// servedEmptyTotals sums the survivors' served-empty counts by reason.
func servedEmptyTotals(stats map[string]broker.StormStats) map[string]int64 {
	out := map[string]int64{}
	for _, st := range stats {
		if st.Lease == nil {
			continue
		}
		for r, n := range st.Lease.ServedEmpty {
			out[r] += n
		}
	}
	return out
}

// servedEmptyLine renders servedEmptyTotals for the report: "0" when none.
func servedEmptyLine(m map[string]int64) string {
	if len(m) == 0 {
		return "0"
	}
	keys := make([]string, 0, len(m))
	var total int64
	for r, n := range m {
		keys = append(keys, r)
		total += n
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, r := range keys {
		parts[i] = fmt.Sprintf("%s %d", r, m[r])
	}
	return fmt.Sprintf("%d (%s)", total, strings.Join(parts, ", "))
}

func stormStats(c *Cluster, victim *Node) map[string]broker.StormStats {
	out := map[string]broker.StormStats{}
	for _, n := range c.Nodes {
		if n == victim {
			continue
		}
		if srv := n.Server(); srv != nil {
			out[n.Name] = srv.StormStats()
		}
	}
	return out
}

// stormSuffix renders the survivors' admission picture for the progress
// line: handshakes in flight over the bound, accept pause so far, and the
// lease work queues' depth and coalescing.
func stormSuffix(c *Cluster, victim *Node) string {
	var hs, bound int
	var paused, waitSec, aliveMax float64
	var accepted, failed, depth, coalesced, syncs, ops, fences, adoptions, lostLeases, aliveFails, deferred int64
	var prebuffered, abandoned, renewOK, renewSkipped, renewFallback, streak, stale, older int64
	var rTimeout, rNoLeader, rLane, rOther, rUnreach int64
	var leaderlessMax float64
	var raft []string
	stats := stormStats(c, victim)
	names := make([]string, 0, len(stats))
	for n := range stats {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		st := stats[n]
		if st.Consensus != nil {
			up := 0
			for _, p := range st.Peers {
				if p.State == transport.Up {
					up++
				}
			}
			var idle, hmax float64
			slowest := ""
			for _, p := range st.Peers {
				if p.State != transport.Up {
					continue
				}
				idle = max(idle, p.ReadIdleMs)
				if p.HandlerMaxMs > hmax {
					hmax, slowest = p.HandlerMaxMs, p.SlowestClass
				}
			}
			var didle, dwait float64
			dataUp := 0
			for _, p := range st.Peers {
				if p.DataState == transport.Up {
					dataUp++
					didle = max(didle, p.DataReadIdleMs)
					dwait = max(dwait, p.DataDispatchWaitMaxMs)
				}
			}
			raft = append(raft, fmt.Sprintf("%s=%s/t%d/L%s/lag%d/ticks%d-of-%d/catchup%d/ctl:up%d/idle%.0fms/hmax%.0fms@%s/data:up%d/idle%.0fms/qwait%.0fms", n, st.Consensus.Role, st.Consensus.Term, st.Consensus.Leader, st.Consensus.Commit-st.Consensus.Applied, st.Consensus.Ticks, st.Consensus.TicksOwed, st.Consensus.TicksMaxRun, up, idle, hmax, slowest, dataUp, didle, dwait))
		}
		for _, l := range st.Listeners {
			hs += l.Handshaking
			bound += l.MaxHandshakes
			paused += l.PausedSec
			accepted += l.Accepted
			failed += l.Failed
			prebuffered += l.Prebuffered
			abandoned += l.Abandoned
		}
		for _, w := range st.LeaseWork {
			depth += w.Depth
			coalesced += w.Coalesced
		}
		if st.Sync != nil {
			syncs += st.Sync.Syncs
			ops += st.Sync.Ops
			waitSec += st.Sync.WaitSec
		}
		if st.Lease != nil {
			fences += st.Lease.Fences
			deferred += st.Lease.FencesDeferred
			adoptions += st.Lease.Adoptions
			lostLeases += st.Lease.LostLeases
			aliveFails += st.Lease.AliveFails
			if st.Lease.AliveMaxMs > aliveMax {
				aliveMax = st.Lease.AliveMaxMs
			}
			renewOK += st.Lease.RenewOK
			renewSkipped += st.Lease.RenewSkipped
			renewFallback += st.Lease.RenewFallback
			streak = max(streak, st.Lease.RenewStreak)
			stale += st.Lease.StaleTakeovers
			older += st.Lease.OlderTakeovers
			rTimeout += st.Lease.RenewTimeouts
			rNoLeader += st.Lease.RenewNoLeader
			rUnreach += st.Lease.RenewUnreachable
			if st.Lease.LeaderlessMaxMs > leaderlessMax {
				leaderlessMax = st.Lease.LeaderlessMaxMs
			}
			rLane += st.Lease.RenewLaneFull
			rOther += st.Lease.RenewOther
		}
	}
	perSync := 0.0
	if syncs > 0 {
		perSync = float64(ops) / float64(syncs)
	}
	return fmt.Sprintf(" | handshaking=%d/%d paused=%.0fs connack=%d/%d prebuffered=%d abandoned=%d queued=%d coalesced=%d fsyncs=%d ops/fsync=%.1f syncwait=%.0fs lease: fences=%d deferred=%d adopted=%d lost=%d alivefail=%d alivemax=%.0fms renew=%d/%d skipped=%d fallback=%d streak=%d fail: timeout=%d noleader=%d unreachable=%d leaderlessmax=%.0fms lane=%d other=%d takeovers: older=%d stale=%d raft: %s",
		hs, bound, paused, accepted, accepted+failed, prebuffered, abandoned, depth, coalesced, syncs, perSync, waitSec, fences, deferred, adoptions, lostLeases, aliveFails, aliveMax,
		renewOK, renewOK+aliveFails, renewSkipped, renewFallback, streak, rTimeout, rNoLeader, rUnreach, leaderlessMax, rLane, rOther, older, stale, strings.Join(raft, " "))
}

// readNetstat reads the TcpExt counters from /proc/net/netstat (Linux);
// an absent or unreadable file yields nil and the delta is omitted.
func readNetstat() map[string]int64 {
	data, err := os.ReadFile("/proc/net/netstat")
	if err != nil {
		return nil
	}
	lines := strings.Split(string(data), "\n")
	for i := 0; i+1 < len(lines); i++ {
		if !strings.HasPrefix(lines[i], "TcpExt:") || !strings.HasPrefix(lines[i+1], "TcpExt:") {
			continue
		}
		names, values := strings.Fields(lines[i])[1:], strings.Fields(lines[i+1])[1:]
		out := map[string]int64{}
		for j := 0; j < len(names) && j < len(values); j++ {
			switch names[j] {
			case "ListenOverflows", "ListenDrops", "TCPBacklogDrop", "SyncookiesSent", "TCPAbortOnTimeout":
				v, _ := strconv.ParseInt(values[j], 10, 64)
				out[names[j]] = v
			}
		}
		return out
	}
	return nil
}

func netstatDelta(before, after map[string]int64) map[string]int64 {
	if before == nil || after == nil {
		return nil
	}
	out := map[string]int64{}
	for k, v := range after {
		out[k] = v - before[k]
	}
	return out
}

// sanitizeNaN replaces NaN (a bounded metrics snapshot that timed out) with
// -1, which JSON can carry; -1 keeps the timeout visible rather than
// smoothing it into a zero.
func sanitizeNaN(m map[string]float64) map[string]float64 {
	out := make(map[string]float64, len(m))
	for k, v := range m {
		if math.IsNaN(v) {
			v = -1
		}
		out[k] = v
	}
	return out
}

type counters struct {
	ops, proposals map[string]float64
	// metrics is each survivor's storm metrics (see stormMetricFamilies);
	// timedOut names the survivors whose snapshot did not return.
	metrics  map[string]map[string]float64
	timedOut []string
}

// leaseCounters reads the survivors' lease acquire operations and proposal
// counts.
func leaseCounters(c *Cluster, victim *Node) counters {
	out := counters{ops: map[string]float64{}, proposals: map[string]float64{}, metrics: map[string]map[string]float64{}}
	for _, n := range c.Nodes {
		if n == victim {
			continue
		}
		srv := n.Server()
		if srv == nil {
			continue
		}
		snap, ok := srv.Registry().Metrics().(interface{ Snapshot() []module.Metric })
		if !ok {
			continue
		}
		// A snapshot walks the session table under its lock; under a storm
		// that lock has been seen held for longer than the measurement, so
		// the wait is bounded and a timeout is reported rather than hung on.
		got := make(chan []module.Metric, 1)
		go func() { got <- snap.Snapshot() }()
		select {
		case metrics := <-got:
			for _, m := range metrics {
				if m.Name == "hoardmq_lease_ops_total" && m.Labels["op"] == "acquire" {
					out.ops[n.Name] += m.Value
				}
			}
			out.metrics[n.Name] = pickStormMetrics(metrics)
		case <-time.After(30 * time.Second):
			out.ops[n.Name] = math.NaN()
			out.timedOut = append(out.timedOut, n.Name)
		}
		out.proposals[n.Name] = float64(srv.Leases().Proposals())
	}
	return out
}

func deltas(before, after counters, which string) string {
	b, a := before.ops, after.ops
	if which == "proposals" {
		b, a = before.proposals, after.proposals
	}
	var names []string
	for n := range a {
		names = append(names, n)
	}
	sort.Strings(names)
	var parts []string
	for _, n := range names {
		parts = append(parts, fmt.Sprintf("%s %s → %s", n, counterValue(b[n]), counterValue(a[n])))
	}
	return strings.Join(parts, "; ")
}

func memory() string {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return "memory unknown"
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "MemTotal:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				kb, _ := strconv.Atoi(fields[1])
				return fmt.Sprintf("%d GiB RAM", kb/1024/1024)
			}
		}
	}
	return "memory unknown"
}
