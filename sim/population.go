package sim

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// A failover measurement's clients, wherever they run. The test reads them
// through this interface: in its own process (localPopulation) or in a
// driver process on another host (remotePopulation), so the two modes share
// the test body and the figures are computed by the same code.
type population interface {
	// fill connects and subscribes every client, advancing completed as each
	// finishes, and returns how many failed.
	fill(ctx context.Context, completed *atomic.Int64) (int, error)
	// perNode counts clients by the node address they are connected to or
	// last tried.
	perNode() (map[string]int, error)
	// mark takes the pre-crash baseline: what follows is measured from here.
	mark() error
	// crashed tells the clients' side that the node has been crashed, so a
	// reading taken before it cannot count as recovery.
	crashed() error
	// sample reads the clients now.
	sample() (popSample, error)
	// finish reads the storm's per-client outcome.
	finish() (popSummary, error)
	// close ends every client.
	close()
}

// popSample is one reading of the clients: how many hold an accepted
// connection, how many are recovered (WI 1618 — online with every held filter
// granted on the connection they are on now), and the refusals and failed
// connects since the mark. At is when the reading was known, on the test's
// clock.
//
// There is no "back" flag: whether the cluster has recovered is a statement
// about a series of readings rather than about one of them, and holdWatch
// makes it.
type popSample struct {
	Online    int
	Recovered int
	Refusals  int64
	Fails     int64
	At        time.Time
}

// popSummary is the storm's per-client outcome since the mark. Reconnects is
// sorted.
type popSummary struct {
	Reconnects     []time.Duration
	DroppedClients int
	Refusals       int64
	Drops          int64
	Fails          int64
	// SubRefusals counts subscriptions the broker refused during the window,
	// and SubRefusedClients how many clients suffered at least one. A client
	// whose resubscribe was refused is online and receives nothing, so the two
	// figures are what says whether "recovered" means what the headline
	// implies (WI 1509). One client refused fifty times and fifty clients
	// refused once are different failures and a total cannot separate them.
	SubRefusals       int64
	SubRefusedClients int
	// SessionLost counts CONNACKs that told a client holding a session that
	// the broker had none, since the mark, and SessionLostClients how many
	// clients suffered one. The resubscribe that follows hides the event, so
	// without these a run that rebuilt sessions empty reads exactly like one
	// that did not (WI 1653).
	SessionLost        int64
	SessionLostClients int
	// SessionLostSample names up to sessionLostSampleCap of those clients.
	SessionLostSample []string
	// SubDowngraded counts SUBACKs that granted a lower QoS than was asked
	// for. A grant, and a change to what the subscriber is promised.
	SubDowngraded int64
	// DropCauses splits Drops by what ended the connection, in the same order
	// as dropCauseNames. Without it `drops` is a count with no cause attached,
	// which is what made WI 1495's excess drops uninterpretable from the
	// artifact alone.
	DropCauses [dropCauseCount]int64
}

// percentile is the failover report's percentile: the value at index
// len*p, clamped to the last. Both modes use it.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[min(len(sorted)-1, int(float64(len(sorted))*p))]
}

// localPopulation runs the clients in this process.
type localPopulation struct {
	clients int
	addrs   []string // node client addresses, in node order
	sources []string // nil: clients dial with no source address
	// failures, when set, classifies every failed connect (the driver's
	// client-host figures). The in-process measurement leaves it nil.
	failures *failureCounts

	mu     sync.Mutex // guards all while fill appends and close reads
	all    []*Client
	failed int

	refusalsBefore, dropsBefore, failsBefore int64
	subRefusalsBefore                        int64
	dropCausesBefore                         [dropCauseCount]int64
	// subRefusedAt is each client's refusal count at the mark. The aggregate
	// counters above can be windowed by subtracting one total, but "how many
	// clients were refused during the window" cannot: a client refused while
	// the population was filling would otherwise be counted again as refused
	// during the storm. Per client is the only way to tell those apart.
	subRefusedAt map[*Client]int64
	// sessionLostAt is the same baseline for session losses: a client whose
	// session was absent during the fill must not be counted again as one the
	// storm lost.
	sessionLostAt       map[*Client]int64
	sessionLostBefore   int64
	subDowngradedBefore int64
}

func newLocalPopulation(clients int, addrs, sources []string, failures *failureCounts) *localPopulation {
	return &localPopulation{clients: clients, addrs: addrs, sources: sources, failures: failures, all: make([]*Client, 0, clients)}
}

func (p *localPopulation) clientsSnapshot() []*Client {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.all
}

func (p *localPopulation) fill(ctx context.Context, completed *atomic.Int64) (int, error) {
	sem := make(chan struct{}, 256)
	errs := make(chan error, p.clients)
	for i := 0; i < p.clients; i++ {
		order := append([]string(nil), p.addrs[i%len(p.addrs):]...)
		order = append(order, p.addrs[:i%len(p.addrs)]...)
		cl := NewClient(fmt.Sprintf("f%06d", i), order, 3600, int64(i))
		if len(p.sources) > 0 {
			cl.Source = p.sources[i%len(p.sources)]
		}
		cl.failures = p.failures
		p.mu.Lock()
		p.all = append(p.all, cl)
		p.mu.Unlock()
		sem <- struct{}{}
		go func(cl *Client) {
			defer func() { <-sem }()
			defer completed.Add(1)
			if err := cl.Run(ctx); err != nil {
				errs <- err
				return
			}
			errs <- cl.Subscribe(ctx, "f/"+cl.ID, 1)
		}(cl)
	}
	failed := 0
	for i := 0; i < p.clients; i++ {
		if err := <-errs; err != nil {
			failed++
		}
	}
	p.failed = failed
	return failed, nil
}

func (p *localPopulation) perNode() (map[string]int, error) {
	count := map[string]int{}
	for _, cl := range p.clientsSnapshot() {
		count[cl.Node()]++
	}
	return count, nil
}

func (p *localPopulation) mark() error {
	p.refusalsBefore, p.dropsBefore, p.failsBefore, p.subRefusalsBefore = 0, 0, 0, 0
	p.sessionLostBefore, p.subDowngradedBefore = 0, 0
	p.subRefusedAt = map[*Client]int64{}
	p.sessionLostAt = map[*Client]int64{}
	p.dropCausesBefore = [dropCauseCount]int64{}
	for _, cl := range p.clientsSnapshot() {
		cl.TakeEvents()
		p.refusalsBefore += cl.Refusals.Load()
		sub := cl.SubRefusals.Load()
		p.subRefusalsBefore += sub
		p.subRefusedAt[cl] = sub
		lost := cl.SessionLost.Load()
		p.sessionLostBefore += lost
		p.sessionLostAt[cl] = lost
		p.subDowngradedBefore += cl.SubDowngraded.Load()
		p.dropsBefore += cl.Drops.Load()
		p.failsBefore += cl.Fails.Load()
		for i := range p.dropCausesBefore {
			p.dropCausesBefore[i] += cl.DropCauses[i].Load()
		}
	}
	return nil
}

func (p *localPopulation) sample() (popSample, error) {
	var s popSample
	var refusals, fails int64
	for _, cl := range p.clientsSnapshot() {
		if cl.Online() {
			s.Online++
		}
		if cl.Recovered() {
			s.Recovered++
		}
		refusals += cl.Refusals.Load()
		fails += cl.Fails.Load()
	}
	s.Refusals, s.Fails, s.At = refusals-p.refusalsBefore, fails-p.failsBefore, time.Now()
	return s, nil
}

func (p *localPopulation) crashed() error { return nil }

// sessionLostSampleCap bounds how many session-lost clients a run names.
const sessionLostSampleCap = 1000

func (p *localPopulation) finish() (popSummary, error) {
	var out popSummary
	for _, cl := range p.clientsSnapshot() {
		if d, ok := cl.Reconnect(); ok {
			out.Reconnects = append(out.Reconnects, d)
		}
		out.Refusals += cl.Refusals.Load()
		if cl.Dropped() {
			out.DroppedClients++
		}
		out.Drops += cl.Drops.Load()
		out.Fails += cl.Fails.Load()
		sub := cl.SubRefusals.Load()
		out.SubRefusals += sub
		// Since the mark, so a client refused during the fill is not counted
		// again as a client the storm refused. A client that appeared after
		// the mark has no entry and its whole count is in the window.
		if sub > p.subRefusedAt[cl] {
			out.SubRefusedClients++
		}
		lost := cl.SessionLost.Load()
		out.SessionLost += lost
		if lost > p.sessionLostAt[cl] {
			out.SessionLostClients++
			if len(out.SessionLostSample) < sessionLostSampleCap {
				out.SessionLostSample = append(out.SessionLostSample, cl.ID)
			}
		}
		out.SubDowngraded += cl.SubDowngraded.Load()
		for i := range out.DropCauses {
			out.DropCauses[i] += cl.DropCauses[i].Load()
		}
	}
	for i := range out.DropCauses {
		out.DropCauses[i] -= p.dropCausesBefore[i]
	}
	out.SubRefusals -= p.subRefusalsBefore
	out.SessionLost -= p.sessionLostBefore
	out.SubDowngraded -= p.subDowngradedBefore
	out.Refusals -= p.refusalsBefore
	out.Drops -= p.dropsBefore
	out.Fails -= p.failsBefore
	sort.Slice(out.Reconnects, func(i, j int) bool { return out.Reconnects[i] < out.Reconnects[j] })
	return out, nil
}

// close ends every client. A hundred thousand sequential closes, each
// writing a DISCONNECT and waiting for its reader, is a test run of its own,
// so everything is cancelled first and in-flight connects time out together
// rather than in turn.
func (p *localPopulation) close() {
	all := p.clientsSnapshot()
	for _, cl := range all {
		cl.Cancel()
	}
	var wg sync.WaitGroup
	gate := make(chan struct{}, 256)
	for _, cl := range all {
		wg.Add(1)
		gate <- struct{}{}
		go func(cl *Client) {
			defer wg.Done()
			defer func() { <-gate }()
			cl.Close()
		}(cl)
	}
	wg.Wait()
}

// dropCauseMap turns the fixed bucket array into the artifact's named map.
// Zero buckets are kept: a cause that did not occur is a measurement, and
// dropping it would make an absent field and a zero indistinguishable.
func dropCauseMap(causes [dropCauseCount]int64) map[string]int64 {
	out := make(map[string]int64, dropCauseCount)
	for i, n := range causes {
		out[dropCauseNames[i]] = n
	}
	return out
}
