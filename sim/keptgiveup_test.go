package sim

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
)

// TestGiveUpServesAKeptCopyBehindASilentSource, on five brokers, in
// TestEarlierGrantCopyOutlivesAnUndeclaredOwner's placement: the subscriber's
// owner p dies with three acknowledged messages queued, replicated to h. The
// leader l adopts the session; under the map without p its fetch asks t, and h
// keeps its copy from p's grant. Every replication frame from l to t is lost,
// so t never answers l's fetch: the copy h kept is found, but a silent source
// leaves every fetch unanswered. Once the adoption worker's retries are spent,
// the subscriber comes back on l and must be served h's copy: Session Present
// and its three messages.
func TestGiveUpServesAKeptCopyBehindASilentSource(t *testing.T) {
	c := New(t, Options{Nodes: 5, AdoptionDelay: 3 * time.Second, Log: halfLog(t, "keptgiveup")})
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	leaderName, ok := agreedLeader(leaderViews(c, nil))
	if !ok {
		t.Fatal("no agreed leader")
	}
	names := make([]string, 0, len(c.Nodes))
	for _, n := range c.Nodes {
		names = append(names, n.Name)
	}
	rep := c.Node(leaderName).Server().Replication()
	roles, found := findEarlierGrantRoles(names, leaderName, rep.Map().Current().Replicas, rep.Map().Count())
	if !found {
		t.Fatalf("no placement under leader %s", leaderName)
	}
	l, p, h, tn := c.Node(leaderName), c.Node(roles.p), c.Node(roles.h), c.Node(roles.t)
	t.Logf("client %s: owner %s fed %s; adopter %s will ask %s", roles.id, p.Name, h.Name, l.Name, tn.Name)

	ledger := &Ledger{}
	sub := NewClient(roles.id, []string{p.TCP}, 600, 1)
	if err := sub.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := sub.Subscribe(ctx, "keptgiveup/t", 1); err != nil {
		t.Fatal(err)
	}
	ledger.Subscriptions = []Subscription{{Client: sub.ID, Filter: "keptgiveup/t", QoS: 1, Since: time.Now()}}
	sub.Close()
	waitOffline(t, p, roles.id)
	pub := NewClient("pub-keptgiveup", []string{p.TCP}, 0, 2)
	if err := pub.Run(ctx); err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 3; i++ {
		pctx, pcancel := context.WithTimeout(ctx, 5*time.Second)
		code, err := pub.Publish(pctx, "keptgiveup/t", 1, i)
		pcancel()
		if err != nil {
			t.Fatalf("precondition not reached: publish %d: %v (code %v)", i, err, code)
		}
		ledger.addPub(Publication{Publisher: pub.ID, Topic: "keptgiveup/t", QoS: 1, Seq: i, Code: code, AckedAt: time.Now()})
	}
	pub.Close()
	waitReplicaCopy(t, h, roles.id, 3, 15*time.Second)

	silent := &transport.Fault{Drop: 1, Classes: []transport.Class{transport.ReplicationAppend}, Rand: rand.New(rand.NewSource(7))} //nolint:gosec // seeded simulation
	c.Fault(l, tn, silent)
	t.Cleanup(func() { c.Fault(l, tn, nil) })
	c.Crash(p)
	waitBasisWithout(t, c, p.Name, 30*time.Second)
	if replicaCopy(h, roles.id) == nil {
		t.Fatalf("precondition not reached: %s dropped its copy at the map change", h.Name)
	}
	// Past the adoption worker's retries: 500 ms doubling to 8 s, six times,
	// each fetch taking up to one ack timeout, after the adoption delay.
	time.Sleep(45 * time.Second)
	ls := l.Server().Leases().LivenessStats()
	t.Logf("after the retries: %s gave up %d, worker unanswered %d; %s holds a copy %v, %s holds a copy %v",
		l.Name, ls.AdoptGaveUp, ls.AdoptWorkerUnanswered, h.Name, replicaCopy(h, roles.id) != nil, tn.Name, replicaCopy(tn, roles.id) != nil)
	if replicaCopy(h, roles.id) == nil {
		t.Fatalf("precondition not reached: %s no longer holds its copy", h.Name)
	}

	sctx, scancel := context.WithTimeout(ctx, 30*time.Second)
	sub2 := NewClient(roles.id, []string{l.TCP}, 600, 3)
	err := sub2.Run(sctx)
	scancel()
	if err != nil {
		t.Fatalf("the subscriber did not come back on %s: %v", l.Name, err)
	}
	got := drainInto(ledger, sub2, 3, 8*time.Second)
	sub2.Close()
	present, connacks := false, 0
	for _, e := range ledger.Events {
		if e.Client == roles.id && e.Kind == "connack" && e.Code == 0 {
			connacks++
			if connacks == 1 {
				present = e.Present
			}
		}
	}
	if !present {
		t.Fatalf("the subscriber was served a fresh session on %s (delivered %d of 3) although %s kept a copy", l.Name, got, h.Name)
	}
	if vs := Check(Evidence{Ledger: ledger}); hasViolation(vs, PropNoAckedLoss) {
		t.Fatalf("acknowledged messages lost: %v", vs)
	}
}
