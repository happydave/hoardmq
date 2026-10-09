package sim

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

// Publication is one acknowledged QoS 1 or 2 publish (or a sent QoS 0 one).
type Publication struct {
	Publisher string
	Topic     string
	QoS       byte
	Seq       uint64
	// Code is the acknowledgement's reason code: Success, or No Matching
	// Subscribers (0x10), which the broker answers when the message reached
	// no local session and is an acknowledgement all the same.
	Code    packet.ReasonCode
	AckedAt time.Time // zero for QoS 0
	// Node is the node that answered. A loss is a statement about what one
	// node knew, and without this the checker can name the message but not
	// the broker that got it wrong (WI 1354).
	Node string
}

// Subscription is a subscriber's durable subscription and when its SUBACK
// arrived.
type Subscription struct {
	Client string
	Filter string
	QoS    byte
	Since  time.Time
}

// Ledger is everything the workload did and saw.
type Ledger struct {
	mu            sync.Mutex
	Publications  []Publication
	Subscriptions []Subscription
	Deliveries    []Delivery
	Events        []Event
	Errors        []string         // publish failures with their cause
	Failures      []PublishFailure // the same failures, attributable to a node
}

// PublishFailure is one publish the broker did not acknowledge, with the node
// that answered it.
//
// The total alone was already reported as publishErrors, which cannot
// distinguish one node refusing everything from every node refusing a little —
// and one node refusing everything is what four defects this month looked
// like (WI 1502).
type PublishFailure struct {
	Client string
	Node   string
	Seq    uint64
	QoS    byte              // the publish's QoS; only 1 and 2 are acknowledged, so only they can be refused
	Code   packet.ReasonCode // the refusal's code; 0 when the send itself failed
	Err    string
}

func (l *Ledger) addPub(p Publication) {
	l.mu.Lock()
	l.Publications = append(l.Publications, p)
	l.mu.Unlock()
}

func (l *Ledger) addErr(f PublishFailure) {
	l.mu.Lock()
	l.Errors = append(l.Errors, fmt.Sprintf("%s seq %d on %s: %s", f.Client, f.Seq, orUnknown(f.Node), f.Err))
	l.Failures = append(l.Failures, f)
	l.mu.Unlock()
}

// nodeNameAt is the name of the node a client is connected to, or "" when it
// is between connections. Client.Node reports an address; the ledger records
// names, so every caller goes through this rather than storing one of each.
func nodeNameAt(c *Cluster, cl *Client) string {
	if c == nil || cl == nil {
		return ""
	}
	if nd := c.NodeAt(cl.Node()); nd != nil {
		return nd.Name
	}
	return ""
}

func orUnknown(node string) string {
	if node == "" {
		return "an unknown node"
	}
	return node
}

// Workload is a seeded set of durable subscribers and publishers.
type Workload struct {
	rng      *rand.Rand
	cluster  *Cluster
	Subs     []*Client
	Pubs     []*Client
	Topics   []string
	Ledger   *Ledger
	rate     time.Duration // between publishes per publisher
	stop     chan struct{}
	done     sync.WaitGroup
	seqs     map[string]*uint64
	pubQoS   map[string]byte
	pubTopic map[string]string
}

// WorkloadOptions size a workload.
type WorkloadOptions struct {
	Subscribers int
	Publishers  int
	Topics      int
	Rate        int // publishes per second per publisher
}

// NewWorkload connects the clients: subscribers spread over the nodes with
// one or two subscriptions each at QoS 1 or 2, publishers spread over the
// nodes each with a fixed topic and QoS.
func NewWorkload(ctx context.Context, c *Cluster, seed int64, o WorkloadOptions) (*Workload, error) {
	rng := rand.New(rand.NewSource(seed)) //nolint:gosec // seeded simulation
	if o.Subscribers == 0 {
		o.Subscribers = 4
	}
	if o.Publishers == 0 {
		o.Publishers = 3
	}
	if o.Topics == 0 {
		o.Topics = 3
	}
	if o.Rate == 0 {
		o.Rate = 20
	}
	w := &Workload{rng: rng, cluster: c, Ledger: &Ledger{}, rate: time.Second / time.Duration(o.Rate), stop: make(chan struct{}),
		seqs: map[string]*uint64{}, pubQoS: map[string]byte{}, pubTopic: map[string]string{}}
	for i := 0; i < o.Topics; i++ {
		w.Topics = append(w.Topics, fmt.Sprintf("t/%d", i))
	}
	addrs := c.Addresses()
	rotate := func(i int) []string {
		out := make([]string, 0, len(addrs))
		for j := range addrs {
			out = append(out, addrs[(i+j)%len(addrs)])
		}
		return out
	}
	for i := 0; i < o.Subscribers; i++ {
		cl := NewClient(fmt.Sprintf("sub%d", i), rotate(i), 600, rng.Int63())
		if err := cl.Run(ctx); err != nil {
			return nil, fmt.Errorf("%s: %w", cl.ID, err)
		}
		w.Subs = append(w.Subs, cl)
		n := 1 + rng.Intn(2)
		perm := rng.Perm(len(w.Topics))
		for _, ti := range perm[:n] {
			qos := byte(1 + rng.Intn(2))
			if err := cl.Subscribe(ctx, w.Topics[ti], qos); err != nil {
				return nil, fmt.Errorf("%s subscribe: %w", cl.ID, err)
			}
			w.Ledger.mu.Lock()
			w.Ledger.Subscriptions = append(w.Ledger.Subscriptions, Subscription{Client: cl.ID, Filter: w.Topics[ti], QoS: qos, Since: time.Now()})
			w.Ledger.mu.Unlock()
		}
	}
	// Routes propagate asynchronously: a publisher must not start before
	// every node knows where the subscribers are, or the first messages are
	// acknowledged with no matching subscriber and never delivered.
	if err := w.waitRoutes(ctx); err != nil {
		return nil, err
	}
	for i := 0; i < o.Publishers; i++ {
		cl := NewClient(fmt.Sprintf("pub%d", i), rotate(i+1), 600, rng.Int63())
		if err := cl.Run(ctx); err != nil {
			return nil, fmt.Errorf("%s: %w", cl.ID, err)
		}
		w.Pubs = append(w.Pubs, cl)
		var seq uint64
		w.seqs[cl.ID] = &seq
		w.pubQoS[cl.ID] = byte(1 + rng.Intn(2))
		w.pubTopic[cl.ID] = w.Topics[rng.Intn(len(w.Topics))]
	}
	return w, nil
}

// Start runs the publishers until Stop.
func (w *Workload) Start() {
	for _, p := range w.Pubs {
		w.done.Add(1)
		go w.publish(p)
	}
}

// Stop ends publishing and waits for the publishers to return.
func (w *Workload) Stop() {
	close(w.stop)
	w.done.Wait()
}

// Close disconnects every client and gathers their deliveries and events.
func (w *Workload) Close() {
	for _, cl := range append(append([]*Client(nil), w.Subs...), w.Pubs...) {
		cl.Close()
	}
	w.Gather()
}

// Gather drains the clients' channels into the ledger.
func (w *Workload) Gather() {
	w.Ledger.mu.Lock()
	defer w.Ledger.mu.Unlock()
	for _, cl := range append(append([]*Client(nil), w.Subs...), w.Pubs...) {
		w.Ledger.Deliveries = append(w.Ledger.Deliveries, cl.TakeDeliveries()...)
		w.Ledger.Events = append(w.Ledger.Events, cl.TakeEvents()...)
	}
	sort.Slice(w.Ledger.Deliveries, func(i, j int) bool { return w.Ledger.Deliveries[i].At.Before(w.Ledger.Deliveries[j].At) })
}

func (w *Workload) publish(p *Client) {
	defer w.done.Done()
	topic, qos := w.pubTopic[p.ID], w.pubQoS[p.ID]
	seq := w.seqs[p.ID]
	tick := time.NewTicker(w.rate)
	defer tick.Stop()
	for {
		select {
		case <-w.stop:
			return
		case <-tick.C:
		}
		*seq++
		n := *seq
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		code, err := p.Publish(ctx, topic, qos, n)
		cancel()
		// Read before branching: the node that answered is as much part of a
		// refusal as of an acknowledgement, and it is the first question
		// asked of a run that refused thousands.
		node := nodeNameAt(w.cluster, p)
		if err != nil {
			// Not acknowledged: the message is not in the ledger and the
			// client did not resend it under a new identifier (a real client
			// would resend with DUP on the same session; the harness keeps
			// the accounting simple and counts the attempt).
			w.Ledger.addErr(PublishFailure{Client: p.ID, Node: node, Seq: n, QoS: qos, Code: code, Err: err.Error()})
			continue
		}
		w.Ledger.addPub(Publication{Publisher: p.ID, Topic: topic, QoS: qos, Seq: n, Code: code, AckedAt: time.Now(), Node: node})
	}
}

// waitRoutes blocks until every running node's route table names, for each
// subscribed topic, every other node that holds a subscriber for it.
func (w *Workload) waitRoutes(ctx context.Context) error {
	want := map[string]map[string]bool{} // topic → nodes holding a subscriber
	for _, s := range w.Ledger.Subscriptions {
		var node string
		for _, cl := range w.Subs {
			if cl.ID == s.Client {
				if n := w.cluster.NodeAt(cl.Node()); n != nil {
					node = n.Name
				}
			}
		}
		if node == "" {
			continue
		}
		if want[s.Filter] == nil {
			want[s.Filter] = map[string]bool{}
		}
		want[s.Filter][node] = true
	}
	for {
		converged := true
		for _, n := range w.cluster.Running() {
			srv := n.Server()
			if srv == nil || srv.Routes() == nil {
				continue
			}
			for topic, holders := range want {
				nodes, _ := srv.Routes().Table().Match("default", topic)
				have := map[string]bool{n.Name: true}
				for _, x := range nodes {
					have[x] = true
				}
				for h := range holders {
					if !have[h] {
						converged = false
					}
				}
			}
		}
		if converged {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("routes did not converge: %w", ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}
