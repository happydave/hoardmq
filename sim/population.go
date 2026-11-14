package sim

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
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
	// queue takes sessions offline, publish queues messages to them, and
	// resume reconnects them and counts what each received. The three come
	// in that order between fill and mark, or not at all.
	queue(ctx context.Context, q queueRequest) (queueResult, error)
	publish(ctx context.Context) (queueResult, error)
	resume(ctx context.Context, wait time.Duration) (queueResult, error)
	// finish reads the storm's per-client outcome.
	finish() (popSummary, error)
	// deliver, after finish, publishes one message on each chosen client's
	// own filter and classes every client by what it received.
	deliver(ctx context.Context, req deliveryRequest) (deliveryResult, error)
	// connacks, after finish, returns each named client's latest CONNACKs.
	connacks(ids []string) ([]clientConnacks, error)
	// close ends every client.
	close()
}

// popSample is one reading of the clients: how many hold an accepted
// connection, how many are recovered (online with every held filter granted
// on the connection they are on now), and the refusals and failed connects
// since the mark. At is when the reading was known, on the test's clock.
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

// popSummary is the storm's per-client outcome since the mark. Reconnects and
// Attempts are sorted.
type popSummary struct {
	Reconnects []time.Duration
	// Attempts holds, for each client in Reconnects, the connects it made
	// from its drop to its first accepted CONNACK (Client.ReconnectAttempts).
	Attempts       []int
	DroppedClients int
	Refusals       int64
	Drops          int64
	Fails          int64
	// SubRefusals counts subscriptions the broker refused during the window,
	// and SubRefusedClients how many clients suffered at least one. A client
	// whose resubscribe was refused is online and receives nothing, so the two
	// figures are what says whether "recovered" means what the headline
	// implies. One client refused fifty times and fifty clients
	// refused once are different failures and a total cannot separate them.
	SubRefusals       int64
	SubRefusedClients int
	// SessionLost counts CONNACKs that told a client holding a session that
	// the broker had none, since the mark, and SessionLostClients how many
	// clients suffered one. The resubscribe that follows hides the event, so
	// without these a run that rebuilt sessions empty reads exactly like one
	// that did not.
	SessionLost        int64
	SessionLostClients int
	// SessionLostSample names up to sessionLostSampleCap of those clients.
	SessionLostSample []string
	// NotRecovered counts the clients not recovered (online with every held
	// filter granted on the current connection) when the summary was taken,
	// and NotRecoveredSample names up to sessionLostSampleCap of them, so a
	// storm that never returns to its target says who is missing.
	NotRecovered       int
	NotRecoveredSample []string
	// SubDowngraded counts SUBACKs that granted a lower QoS than was asked
	// for. A grant, and a change to what the subscriber is promised.
	SubDowngraded int64
	// DropCauses splits Drops by what ended the connection, in the same order
	// as dropCauseNames. Without it `drops` is a count with no cause attached,
	// which is what made the excess drops after a leader crash
	// uninterpretable from the artifact alone.
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
	// keepAliveOff turns TCP keepalive off on every client's connections.
	keepAliveOff bool
	// keepAlive is every client's MQTT keep alive, in seconds.
	keepAlive uint16

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
	sessionLostAt     map[*Client]int64
	sessionLostBefore int64
	// genAt is each client's accepted CONNACK count at the mark, by index:
	// a client past it has reconnected since.
	genAt               []uint64
	subDowngradedBefore int64

	// offline holds the indexes the queue step took offline, until resume
	// replaces them; queued is that step's request.
	offline []int
	queued  queueRequest
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
		cl.KeepAliveOff = p.keepAliveOff
		cl.KeepAlive = p.keepAlive
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
	all := p.clientsSnapshot()
	p.genAt = make([]uint64, len(all))
	for i, cl := range all {
		p.genAt[i] = cl.gen.Load()
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
		if n, ok := cl.ReconnectAttempts(); ok {
			out.Attempts = append(out.Attempts, n)
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
		if !cl.Recovered() {
			out.NotRecovered++
			if len(out.NotRecoveredSample) < sessionLostSampleCap {
				out.NotRecoveredSample = append(out.NotRecoveredSample, cl.ID)
			}
		}
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
	sort.Ints(out.Attempts)
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

// queueRequest asks for sessions taken offline and messages queued to them:
// Sessions of the population, Depth QoS 1 messages each, of PayloadBytes.
type queueRequest struct {
	Sessions     int `json:"sessions"`
	Depth        int `json:"depth"`
	PayloadBytes int `json:"payload_bytes"`
}

// queueResult is what the queue step did, and on resume what came back. Acked
// counts the publishes' acknowledgements by reason code; an error that reached
// no acknowledgement is counted as "none".
type queueResult struct {
	Offline     int              `json:"offline"`
	Publishers  int              `json:"publishers"`
	Published   int64            `json:"published"`
	Acked       map[string]int64 `json:"acked"`
	PublishMs   int64            `json:"publish_ms"`
	Reconnected int              `json:"reconnected,omitempty"`
	SessionLost int              `json:"session_lost,omitempty"`
	Received    int64            `json:"received,omitempty"`
	// Complete counts resumed clients that received every message queued to
	// them, each sequence number once at least.
	Complete int   `json:"complete,omitempty"`
	ResumeMs int64 `json:"resume_ms,omitempty"`
}

// queuePublishers bounds the connections that publish the queued messages.
const queuePublishers = 64

// queueStride picks every stride-th client, so the offline sessions spread
// over every node the population's order rotates through.
func queueStride(clients, sessions int) int {
	if sessions <= 0 || sessions >= clients {
		return 1
	}
	return clients / sessions
}

// queuePayload is a message of exactly n bytes when n is long enough, carrying
// the publisher and sequence parsePayload reads.
func queuePayload(publisher string, seq uint64, n int) []byte {
	b := payload(publisher, seq)
	if pad := n - len(b) - len(";pad="); pad > 0 {
		b = append(b, ";pad="...)
		b = append(b, strings.Repeat("x", pad)...)
	}
	return b
}

// queue takes the chosen sessions offline with a clean DISCONNECT, which keeps
// each session.
func (p *localPopulation) queue(_ context.Context, q queueRequest) (queueResult, error) {
	all := p.clientsSnapshot()
	if q.Sessions <= 0 || q.Depth <= 0 || q.Sessions > len(all) {
		return queueResult{}, fmt.Errorf("queue: %d sessions of %d clients, depth %d", q.Sessions, len(all), q.Depth)
	}
	stride := queueStride(len(all), q.Sessions)
	p.offline = p.offline[:0]
	for i := 0; i < len(all) && len(p.offline) < q.Sessions; i += stride {
		p.offline = append(p.offline, i)
	}
	var wg sync.WaitGroup
	gate := make(chan struct{}, 256)
	for _, i := range p.offline {
		wg.Add(1)
		gate <- struct{}{}
		go func(cl *Client) {
			defer wg.Done()
			defer func() { <-gate }()
			cl.Close()
		}(all[i])
	}
	wg.Wait()
	p.queued = q
	return queueResult{Offline: len(p.offline), Acked: map[string]int64{}}, nil
}

// publish sends Depth QoS 1 messages to each offline session's topic.
func (p *localPopulation) publish(ctx context.Context) (queueResult, error) {
	all := p.clientsSnapshot()
	q := p.queued
	res := queueResult{Offline: len(p.offline), Acked: map[string]int64{}}
	if len(p.offline) == 0 {
		return res, errors.New("publish: no session was taken offline")
	}
	var wg sync.WaitGroup
	n := min(queuePublishers, len(p.offline))
	res.Publishers = n
	pubs := make([]*Client, n)
	for j := range pubs {
		order := append([]string(nil), p.addrs[j%len(p.addrs):]...)
		order = append(order, p.addrs[:j%len(p.addrs)]...)
		pubs[j] = NewClient(fmt.Sprintf("q%03d", j), order, 0, int64(3_000_000+j))
		if len(p.sources) > 0 {
			pubs[j].Source = p.sources[j%len(p.sources)]
		}
		if err := pubs[j].Run(ctx); err != nil {
			for _, c := range pubs[:j+1] {
				c.Close()
			}
			return res, fmt.Errorf("queue: publisher %d: %w", j, err)
		}
	}
	var mu sync.Mutex
	var published int64
	start := time.Now()
	for j, pub := range pubs {
		wg.Add(1)
		go func(j int, pub *Client) {
			defer wg.Done()
			local := map[string]int64{}
			var sent int64
			var seq uint64
			for k := j; k < len(p.offline); k += n {
				topic := "f/" + all[p.offline[k]].ID
				for d := 0; d < q.Depth; d++ {
					seq++
					code, err := pub.publish(ctx, &packet.Publish{Topic: topic, QoS: 1, Payload: queuePayload(pub.ID, seq, q.PayloadBytes)})
					sent++
					if err != nil {
						local["none"]++
						continue
					}
					local[ackName(code)]++
				}
			}
			mu.Lock()
			published += sent
			for k, v := range local {
				res.Acked[k] += v
			}
			mu.Unlock()
		}(j, pub)
	}
	wg.Wait()
	res.PublishMs = time.Since(start).Milliseconds()
	res.Published = published
	for _, c := range pubs {
		c.Close()
	}
	return res, nil
}

// resume reconnects the offline sessions as new clients holding their filter,
// and waits, bounded, until each has received every message queued to it.
func (p *localPopulation) resume(ctx context.Context, wait time.Duration) (queueResult, error) {
	all := p.clientsSnapshot()
	res := queueResult{Offline: len(p.offline), Acked: map[string]int64{}}
	if len(p.offline) == 0 {
		return res, errors.New("resume: no session was taken offline")
	}
	start := time.Now()
	fresh := make([]*Client, len(p.offline))
	var wg sync.WaitGroup
	var reconnected atomic.Int64
	gate := make(chan struct{}, 256)
	for k, i := range p.offline {
		old := all[i]
		cl := NewClient(old.ID, old.nodes, old.expiry, int64(4_000_000+i))
		cl.Source, cl.failures, cl.KeepAliveOff, cl.KeepAlive = old.Source, p.failures, old.KeepAliveOff, old.KeepAlive
		cl.Hold("f/"+old.ID, 1)
		fresh[k] = cl
		wg.Add(1)
		gate <- struct{}{}
		go func(cl *Client) {
			defer wg.Done()
			defer func() { <-gate }()
			if cl.Run(ctx) == nil {
				reconnected.Add(1)
			}
		}(cl)
	}
	wg.Wait()
	p.mu.Lock()
	for k, i := range p.offline {
		p.all[i] = fresh[k]
	}
	p.mu.Unlock()
	res.Reconnected = int(reconnected.Load())

	seen := make([]map[uint64]bool, len(fresh))
	for k := range seen {
		seen[k] = map[uint64]bool{}
	}
	deadline := time.Now().Add(wait)
	for {
		complete := 0
		for k, cl := range fresh {
			for _, d := range cl.TakeDeliveries() {
				res.Received++
				seen[k][d.Seq] = true
			}
			if len(seen[k]) >= p.queued.Depth {
				complete++
			}
		}
		res.Complete = complete
		if complete == len(fresh) || time.Now().After(deadline) || ctx.Err() != nil {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	for _, cl := range fresh {
		if cl.SessionLost.Load() > 0 {
			res.SessionLost++
		}
	}
	res.ResumeMs = time.Since(start).Milliseconds()
	p.offline = p.offline[:0]
	return res, nil
}

// ackName is a reason code's name without its number, "Success" for 0x00.
func ackName(code packet.ReasonCode) string {
	name, _, _ := strings.Cut(code.String(), " (")
	return name
}
