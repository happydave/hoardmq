package sim

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// The client driver runs a failover measurement's clients in their own
// process, usually on another host, so the brokers do not share a scheduler,
// CPU or socket tables with them. The test starts it over ssh and
// talks to it through its stdin and stdout, one JSON object per line:
//
//	test → driver   hello (clients, node addresses, sources)
//	driver → test   ready (the driver's build and host) or refused (reasons)
//	test → driver   fill
//	driver → test   progress every 15 s, then filled (failed, per-node counts)
//	test → driver   queue (optional: sessions to take offline, messages each)
//	driver → test   queued (how many went offline)
//	test → driver   publish (only after queue)
//	driver → test   published (what was published and acknowledged)
//	test → driver   resume (only after publish)
//	driver → test   resumed (reconnected, sessions lost, messages received)
//	test → driver   mark
//	driver → test   marked; the test crashes a node only after this
//	test → driver   crashed, once the node is down
//	driver → test   sample every 50 ms; allback once, when every client is back
//	                after a reading below that, taken after crashed arrived
//	driver → test   host figures every second, from ready to closed
//	test → driver   finish
//	driver → test   summary (every per-client reconnect time and the counters)
//	test → driver   deliver (optional: one message on each chosen client's
//	                filter, with negative controls)
//	driver → test   delivered (every chosen client's class)
//	test → driver   connacks (optional, any number: client identifiers)
//	driver → test   connack_records (each named client's latest CONNACKs)
//	test → driver   close
//	driver → test   closed, and the driver exits
//
// The driver listens on no port. When its stdin closes, at any step, it ends
// every client and exits: an ssh that dies takes the driver with it.

// reliableReading is how often a reading goes on the reliable channel when
// it is not a crossing: often enough that the hold advances and the driver is
// never silent, rare enough to cost nothing beside the bulk stream.
const reliableReading = time.Second

const (
	msgHello     = "hello"
	msgReady     = "ready"
	msgRefused   = "refused"
	msgFill      = "fill"
	msgProgress  = "progress"
	msgFilled    = "filled"
	msgMark      = "mark"
	msgMarked    = "marked"
	msgCrashed   = "crashed"
	msgSample    = "sample"
	msgCross     = "cross"
	msgHost      = "host"
	msgFinish    = "finish"
	msgSummary   = "summary"
	msgClose     = "close"
	msgClosed    = "closed"
	msgQueue     = "queue"
	msgQueued    = "queued"
	msgPublish   = "publish"
	msgPublished = "published"
	msgResume    = "resume"
	msgResumed   = "resumed"
	msgDeliver   = "deliver"
	msgDelivered = "delivered"
	msgConnacks  = "connacks"
	msgConnackRe = "connack_records"
)

// The driver's phases, as its host figures label them. The client-limited
// verdict reads the window samples only.
const (
	phaseReady    = "ready"
	phaseFill     = "fill"
	phaseFilled   = "filled"
	phaseWindow   = "window"
	phaseFinished = "finished"
	phaseClosing  = "closing"
)

// Driver exit codes.
const (
	driverOK        = 0
	driverRefused   = 1
	driverProtocol  = 2
	driverInputGone = 3
)

// driverMsg is every control message; each type uses its own fields.
type driverMsg struct {
	Type string `json:"type"`

	Clients        int      `json:"clients,omitempty"`
	Nodes          []string `json:"nodes,omitempty"`
	Sources        []string `json:"sources,omitempty"`
	BytesPerClient uint64   `json:"bytes_per_client,omitempty"`
	WaitMs         int64    `json:"wait_ms,omitempty"`
	// TCPKeepAliveOff, in the hello, turns TCP keepalive off on the
	// clients' connections.
	TCPKeepAliveOff bool `json:"tcp_keepalive_off,omitempty"`
	// KeepAlive, in the hello, is the clients' MQTT keep alive in seconds.
	KeepAlive uint16 `json:"keep_alive,omitempty"`

	Driver  *DriverInfo `json:"driver,omitempty"`
	Reasons []string    `json:"reasons,omitempty"`

	Completed int64          `json:"completed,omitempty"`
	Failed    int            `json:"failed,omitempty"`
	FillMs    int64          `json:"fill_ms,omitempty"`
	PerNode   map[string]int `json:"per_node,omitempty"`

	Online int `json:"online,omitempty"`
	// Recovered is the count under the measurement's definition: online with
	// every held filter granted on the connection the client is on now.
	Recovered int   `json:"recovered,omitempty"`
	Refusals  int64 `json:"refusals,omitempty"`
	Fails     int64 `json:"fails,omitempty"`

	Host    *ClientHostSample `json:"host,omitempty"`
	Summary *driverSummary    `json:"summary,omitempty"`

	Queue       *queueRequest `json:"queue,omitempty"`
	QueueResult *queueResult  `json:"queue_result,omitempty"`

	Delivery       *deliveryRequest `json:"delivery,omitempty"`
	DeliveryResult *deliveryResult  `json:"delivery_result,omitempty"`

	ClientIDs []string         `json:"client_ids,omitempty"`
	Connacks  []clientConnacks `json:"connacks,omitempty"`
}

// DriverInfo is the driver's own provenance: its build and its host.
type DriverInfo struct {
	Revision      string `json:"revision"`
	Modified      *bool  `json:"modified"`
	GoVersion     string `json:"go_version"`
	Hostname      string `json:"hostname"`
	NumCPU        int    `json:"num_cpu"`
	MemTotalBytes uint64 `json:"mem_total_bytes"`
	Kernel        string `json:"kernel"`
	PortLo        int    `json:"ephemeral_lo"`
	PortHi        int    `json:"ephemeral_hi"`
	OpenFiles     uint64 `json:"open_files"`
	Dial          string `json:"dial"`
	// TCPKeepAlive is the clients' TCP keepalive as the driver applies it:
	// "default" (Go's) or "off".
	TCPKeepAlive string `json:"tcp_keepalive"`
	// KeepAlive is the clients' MQTT keep alive as the driver applies it,
	// in seconds.
	KeepAlive int     `json:"keep_alive_sec"`
	WaitedSec float64 `json:"waited_for_start_sec"`
}

// driverSummary is the storm's per-client outcome as the driver saw it.
type driverSummary struct {
	ReconnectsNs       []int64  `json:"reconnects_ns"`
	ReconnectAttempts  []int    `json:"reconnect_attempts"`
	NotRecovered       int      `json:"not_recovered"`
	NotRecoveredSample []string `json:"not_recovered_sample"`
	DroppedClients     int      `json:"dropped_clients"`
	Refusals           int64    `json:"refusals"`
	Drops              int64    `json:"drops"`
	Fails              int64    `json:"fails"`
	SubRefusals        int64    `json:"sub_refusals"`
	SubRefusedClients  int      `json:"sub_refused_clients"`
	SessionLost        int64    `json:"session_lost"`
	SessionLostClients int      `json:"session_lost_clients"`
	SubDowngraded      int64    `json:"sub_downgraded"`
	DropCauses         []int64  `json:"drop_causes"`
	BulkDropped        int64    `json:"bulk_dropped"`
	MissingSources     []string `json:"missing_sources"`
	// LateDrops names the clients that dropped after every client was back.
	LateDrops *lateDrops `json:"late_drops,omitempty"`
}

type driverOptions struct {
	facts         func() clientStartFacts
	checkEvery    time.Duration
	sampleEvery   time.Duration
	hostEvery     time.Duration
	progressEvery time.Duration
	// dropBulk drops every sample, host figure and progress message, so a
	// test can show the reliable messages do not depend on them.
	dropBulk bool
}

func defaultDriverOptions() driverOptions {
	return driverOptions{facts: readClientStartFacts, checkEvery: 5 * time.Second, sampleEvery: 50 * time.Millisecond,
		hostEvery: time.Second, progressEvery: 15 * time.Second}
}

// RunDriver serves one measurement over in and out and returns the process
// exit code.
func RunDriver(in io.Reader, out, errOut io.Writer) int {
	return runDriver(in, out, errOut, defaultDriverOptions())
}

// driverWriter owns the driver's stdout. Reliable messages are queued and
// always written; samples, host figures and progress are offered to a bounded
// queue and dropped, counted, when it is full. No client goroutine writes, and
// nothing waits on a slow reader except the protocol and sampler goroutines'
// reliable sends.
type driverWriter struct {
	enc      *json.Encoder
	errOut   io.Writer
	reliable chan driverMsg
	bulk     chan driverMsg
	dropAll  bool
	dropped  atomic.Int64
	quit     chan struct{}
	done     chan struct{}
	failed   bool
}

func newDriverWriter(out, errOut io.Writer, dropAll bool) *driverWriter {
	w := &driverWriter{enc: json.NewEncoder(out), errOut: errOut, reliable: make(chan driverMsg, 64), bulk: make(chan driverMsg, 256),
		dropAll: dropAll, quit: make(chan struct{}), done: make(chan struct{})}
	go w.loop()
	return w
}

func (w *driverWriter) write(m driverMsg) {
	if w.failed {
		return
	}
	if err := w.enc.Encode(m); err != nil {
		w.failed = true
		_, _ = fmt.Fprintf(w.errOut, "driver: writing control channel: %v\n", err)
	}
}

func (w *driverWriter) loop() {
	defer close(w.done)
	for {
		select {
		case m := <-w.reliable:
			w.write(m)
			continue
		default:
		}
		select {
		case m := <-w.reliable:
			w.write(m)
		case m := <-w.bulk:
			w.write(m)
		case <-w.quit:
			for {
				select {
				case m := <-w.reliable:
					w.write(m)
				default:
					return
				}
			}
		}
	}
}

// send queues a message that must arrive.
func (w *driverWriter) send(m driverMsg) { w.reliable <- m }

// offer queues a message that may be dropped.
func (w *driverWriter) offer(m driverMsg) {
	if w.dropAll {
		w.dropped.Add(1)
		return
	}
	select {
	case w.bulk <- m:
	default:
		w.dropped.Add(1)
	}
}

// stop writes every reliable message already sent and ends the writer.
func (w *driverWriter) stop() {
	close(w.quit)
	<-w.done
}

// ticking runs f every period in its own goroutine until the returned stop
// function is called; stop waits for it.
func ticking(period time.Duration, f func()) (stop func()) {
	quit, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(period)
		defer t.Stop()
		for {
			select {
			case <-quit:
				return
			case <-t.C:
				f()
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() { close(quit) })
		<-done
	}
}

func runDriver(in io.Reader, out, errOut io.Writer, o driverOptions) int {
	w := newDriverWriter(out, errOut, o.dropBulk)
	defer w.stop()

	msgs := make(chan driverMsg, 16)
	inputDone := make(chan struct{})
	go func() {
		defer close(inputDone)
		dec := json.NewDecoder(in)
		for {
			var m driverMsg
			if err := dec.Decode(&m); err != nil {
				if !errors.Is(err, io.EOF) {
					_, _ = fmt.Fprintf(errOut, "driver: reading control channel: %v\n", err)
				}
				return
			}
			msgs <- m
		}
	}()

	var (
		phase      atomic.Value
		pop        *localPopulation
		stopHost   = func() {}
		stopSample = func() {}
		failures   = &failureCounts{}
	)
	phase.Store(phaseReady)
	// end stops everything this run started, in order: the sampler reads the
	// clients, so it stops before they close.
	end := func(code int) int {
		stopSample()
		if pop != nil {
			pop.close()
		}
		stopHost()
		return code
	}
	next := func() (driverMsg, bool) {
		select {
		case m := <-msgs:
			return m, true
		case <-inputDone:
			select {
			case m := <-msgs:
				return m, true
			default:
				return driverMsg{}, false
			}
		}
	}
	// expect reads the next message and returns an exit code when it is not
	// the one wanted.
	expect := func(want string) (driverMsg, int, bool) {
		m, ok := next()
		if !ok {
			_, _ = fmt.Fprintf(errOut, "driver: control channel closed while waiting for %s\n", want)
			return m, driverInputGone, false
		}
		if m.Type != want {
			w.send(driverMsg{Type: msgRefused, Reasons: []string{fmt.Sprintf("unexpected message %q while waiting for %q", m.Type, want)}})
			return m, driverProtocol, false
		}
		return m, 0, true
	}

	hello, code, ok := expect(msgHello)
	if !ok {
		return end(code)
	}
	if hello.Clients <= 0 || len(hello.Nodes) == 0 || len(hello.Sources) == 0 {
		w.send(driverMsg{Type: msgRefused, Reasons: []string{"hello needs clients, nodes and sources"}})
		return end(driverProtocol)
	}
	want := clientStartWant{Clients: hello.Clients, Sources: hello.Sources, BytesPerClient: hello.BytesPerClient}
	waitStart := time.Now()
	lastLogged := ""
	for {
		problems := clientStartProblems(o.facts(), want)
		if len(problems) == 0 {
			break
		}
		if joined := strings.Join(problems, "; "); joined != lastLogged {
			_, _ = fmt.Fprintf(errOut, "driver: waiting to start: %s\n", joined)
			lastLogged = joined
		}
		if time.Since(waitStart) >= time.Duration(hello.WaitMs)*time.Millisecond {
			w.send(driverMsg{Type: msgRefused, Reasons: problems})
			return end(driverRefused)
		}
		select {
		case <-inputDone:
			return end(driverInputGone)
		case <-time.After(o.checkEvery):
		}
	}
	dial, unreachable := reachNodes(hello.Sources, hello.Nodes)
	if len(unreachable) > 0 {
		w.send(driverMsg{Type: msgRefused, Reasons: unreachable})
		return end(driverRefused)
	}
	info := readDriverInfo(dial)
	info.TCPKeepAlive = keepAliveName(hello.TCPKeepAliveOff)
	info.KeepAlive = int(hello.KeepAlive)
	info.WaitedSec = time.Since(waitStart).Seconds()
	w.send(driverMsg{Type: msgReady, Driver: &info})

	readyAt := time.Now()
	stalls := newStallWatch(readyAt)
	prev := readHostCounters(failures)
	stopTicker := ticking(o.hostEvery, func() {
		cur := readHostCounters(failures)
		s := clientHostSample(prev, cur, time.Since(readyAt).Seconds(), phase.Load().(string), stalls.takeWorst())
		prev = cur
		w.offer(driverMsg{Type: msgHost, Host: &s})
	})
	stopHost = func() {
		stopTicker()
		stalls.close()
	}

	if _, code, ok := expect(msgFill); !ok {
		return end(code)
	}
	phase.Store(phaseFill)
	pop = newLocalPopulation(hello.Clients, hello.Nodes, hello.Sources, failures)
	pop.keepAliveOff = hello.TCPKeepAliveOff
	pop.keepAlive = hello.KeepAlive
	fillCtx, cancelFill := context.WithTimeout(context.Background(), 45*time.Minute)
	defer cancelFill()
	var completed atomic.Int64
	stopProgress := ticking(o.progressEvery, func() {
		w.offer(driverMsg{Type: msgProgress, Completed: completed.Load()})
	})
	fillStart := time.Now()
	filled := make(chan int, 1)
	go func() {
		failed, _ := pop.fill(fillCtx, &completed)
		filled <- failed
	}()
	var failed int
	select {
	case failed = <-filled:
	case <-inputDone:
		cancelFill()
		<-filled
		stopProgress()
		return end(driverInputGone)
	}
	stopProgress()
	perNode, _ := pop.perNode()
	phase.Store(phaseFilled)
	w.send(driverMsg{Type: msgFilled, Failed: failed, FillMs: time.Since(fillStart).Milliseconds(), PerNode: perNode, Completed: completed.Load()})

	if code, ok := queueSteps(next, w, pop, inputDone, errOut); !ok {
		return end(code)
	}
	_ = pop.mark()
	target := hello.Clients - failed
	phase.Store(phaseWindow)
	// marked carries the first reading, so the test's window never starts
	// from a count it has not received.
	first, _ := pop.sample()
	w.send(driverMsg{Type: msgMarked, Online: first.Online, Refusals: first.Refusals, Fails: first.Fails, Recovered: first.Recovered})
	// The test decides when the cluster recovered, from the readings it is
	// sent. Readings go as bulk, which may be dropped under load —
	// so every reading that *crosses* the target in either direction is sent
	// again on the reliable channel. A dropped reading between two crossings
	// cannot change the interval; a dropped crossing could.
	atTarget := first.Recovered >= target && first.Online >= target
	lastReliable := time.Now()
	// back is read once stopSample has returned; crashed is set once the
	// test has said the crash happened.
	var back backWatch
	var crashed atomic.Bool
	stopSample = ticking(o.sampleEvery, func() {
		s, _ := pop.sample()
		m := driverMsg{Type: msgSample, Online: s.Online, Recovered: s.Recovered, Refusals: s.Refusals, Fails: s.Fails}
		w.offer(m)
		cross := false
		now := s.Recovered >= target && s.Online >= target
		if now != atTarget {
			atTarget, cross = now, true
		}
		back.observe(now, crashed.Load(), s.At)
		// A reading goes reliably when it crosses the target, and otherwise
		// once a second. The test's watch only advances its hold when a
		// reading's arrival time advances, so a run whose bulk was all
		// dropped would otherwise never complete one — the interval rule
		// needs the series to keep arriving, not only its crossings.
		if cross || time.Since(lastReliable) >= reliableReading {
			lastReliable = time.Now()
			if cross {
				m.Type = msgCross
			}
			w.send(m)
		}
	})

	// crashed orders the exchange — finish may not arrive before it. The
	// fell-first rule is the test's watch's, not the driver's.
	if _, code, ok := expect(msgCrashed); !ok {
		return end(code)
	}
	crashed.Store(true)
	if _, code, ok := expect(msgFinish); !ok {
		return end(code)
	}
	stopSample()
	phase.Store(phaseFinished)
	sum, _ := pop.finish()
	ds := driverSummary{ReconnectsNs: make([]int64, 0, len(sum.Reconnects)), ReconnectAttempts: sum.Attempts, NotRecovered: sum.NotRecovered, NotRecoveredSample: sum.NotRecoveredSample, DroppedClients: sum.DroppedClients,
		Refusals: sum.Refusals, Drops: sum.Drops, Fails: sum.Fails, SubRefusals: sum.SubRefusals,
		SubRefusedClients: sum.SubRefusedClients, SessionLost: sum.SessionLost,
		SessionLostClients: sum.SessionLostClients, SubDowngraded: sum.SubDowngraded,
		DropCauses:  sum.DropCauses[:],
		BulkDropped: w.dropped.Load(), MissingSources: make([]string, 0),
		LateDrops: lateDropsOf(pop.clientsSnapshot(), back.at)}
	for _, d := range sum.Reconnects {
		ds.ReconnectsNs = append(ds.ReconnectsNs, int64(d))
	}
	facts := o.facts()
	for _, s := range hello.Sources {
		if !sourcePresent(s, facts.Addrs) {
			ds.MissingSources = append(ds.MissingSources, s)
		}
	}
	w.send(driverMsg{Type: msgSummary, Summary: &ds})

	if code, ok := deliverStep(next, w, pop, inputDone, errOut); !ok {
		return end(code)
	}
	phase.Store(phaseClosing)
	pop.close()
	pop = nil
	stopHost()
	stopHost = func() {}
	w.send(driverMsg{Type: msgClosed})
	return driverOK
}

// reachNodes connects once from every source to every node and closes, and
// reads the dial mode back from the first socket. It returns the mode and a
// reason per pair that could not connect.
func reachNodes(sources, nodes []string) (string, []string) {
	dial := ""
	var problems []string
	for _, s := range sources {
		for _, n := range nodes {
			d := sourceDialer(s)
			nc, err := d.Dial("tcp", n)
			if err != nil {
				problems = append(problems, fmt.Sprintf("cannot reach %s from %s: %v", n, s, err))
				continue
			}
			if dial == "" {
				dial = connDialMode(nc)
			}
			_ = nc.Close()
		}
	}
	return dial, problems
}

func readDriverInfo(dial string) DriverInfo {
	info := DriverInfo{GoVersion: runtime.Version(), NumCPU: runtime.NumCPU(), Dial: dial}
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, s := range bi.Settings {
			switch s.Key {
			case "vcs.revision":
				info.Revision = s.Value
			case "vcs.modified":
				m := s.Value == "true"
				info.Modified = &m
			}
		}
	}
	if h, err := os.Hostname(); err == nil {
		info.Hostname = h
	}
	if data, err := os.ReadFile("/proc/meminfo"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if f := strings.Fields(line); len(f) >= 2 && f[0] == "MemTotal:" {
				var kb uint64
				if _, err := fmt.Sscan(f[1], &kb); err == nil {
					info.MemTotalBytes = kb * 1024
				}
			}
		}
	}
	if data, err := os.ReadFile("/proc/sys/kernel/osrelease"); err == nil {
		info.Kernel = strings.TrimSpace(string(data))
	}
	info.PortLo, info.PortHi, _ = readEphemeralRange()
	info.OpenFiles = readClientStartFacts().OpenFiles
	return info
}

// queueSteps reads the messages between filled and mark: mark alone, or one
// queue and its resume before it. It returns once mark has arrived, or with
// the exit code when the exchange broke.
func queueSteps(next func() (driverMsg, bool), w *driverWriter, pop *localPopulation, inputDone <-chan struct{}, errOut io.Writer) (int, bool) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-inputDone:
			cancel()
		case <-ctx.Done():
		}
	}()
	queued := false
	for {
		m, ok := next()
		if !ok {
			_, _ = fmt.Fprintf(errOut, "driver: control channel closed while waiting for %s\n", msgMark)
			return driverInputGone, false
		}
		switch {
		case m.Type == msgMark:
			return 0, true
		case m.Type == msgQueue && !queued && m.Queue != nil:
			queued = true
			steps := []struct {
				answer string
				run    func() (queueResult, error)
			}{
				{msgQueued, func() (queueResult, error) { return pop.queue(ctx, *m.Queue) }},
				{msgPublished, func() (queueResult, error) { return pop.publish(ctx) }},
				{msgResumed, nil},
			}
			for i, st := range steps {
				run := st.run
				if i > 0 {
					want := []string{"", msgPublish, msgResume}[i]
					r, ok := next()
					if !ok {
						return driverInputGone, false
					}
					if r.Type != want {
						w.send(driverMsg{Type: msgRefused, Reasons: []string{fmt.Sprintf("unexpected message %q while waiting for %q", r.Type, want)}})
						return driverProtocol, false
					}
					if run == nil {
						wait := time.Duration(r.WaitMs) * time.Millisecond
						run = func() (queueResult, error) { return pop.resume(ctx, wait) }
					}
				}
				res, err := run()
				if err != nil {
					w.send(driverMsg{Type: msgRefused, Reasons: []string{err.Error()}})
					return driverProtocol, false
				}
				w.send(driverMsg{Type: st.answer, QueueResult: &res})
			}
		default:
			w.send(driverMsg{Type: msgRefused, Reasons: []string{fmt.Sprintf("unexpected message %q while waiting for %q", m.Type, msgMark)}})
			return driverProtocol, false
		}
	}
}

// deliverStep reads the messages between summary and close: at most one
// deliver, and any number of connacks, before close. It returns once close
// has arrived, or with the exit code when the exchange broke.
func deliverStep(next func() (driverMsg, bool), w *driverWriter, pop *localPopulation, inputDone <-chan struct{}, errOut io.Writer) (int, bool) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-inputDone:
			cancel()
		case <-ctx.Done():
		}
	}()
	delivered := false
	for {
		m, ok := next()
		if !ok {
			_, _ = fmt.Fprintf(errOut, "driver: control channel closed while waiting for %s\n", msgClose)
			return driverInputGone, false
		}
		switch {
		case m.Type == msgClose:
			return 0, true
		case m.Type == msgDeliver && !delivered && m.Delivery != nil:
			delivered = true
			res, err := pop.deliver(ctx, *m.Delivery)
			if err != nil {
				w.send(driverMsg{Type: msgRefused, Reasons: []string{err.Error()}})
				return driverProtocol, false
			}
			w.send(driverMsg{Type: msgDelivered, DeliveryResult: &res})
		case m.Type == msgConnacks:
			res, _ := pop.connacks(m.ClientIDs)
			w.send(driverMsg{Type: msgConnackRe, Connacks: res})
		default:
			w.send(driverMsg{Type: msgRefused, Reasons: []string{fmt.Sprintf("unexpected message %q while waiting for %q", m.Type, msgClose)}})
			return driverProtocol, false
		}
	}
}

// keepAliveName names the clients' TCP keepalive as a run records it.
func keepAliveName(off bool) string {
	if off {
		return "off"
	}
	return "default"
}
