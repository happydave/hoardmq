package session

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

// blockingJournal holds SessionUpsert until released, so a test can put a
// connect inside the store's write path and keep it there.
type blockingJournal struct {
	fakeJournal
	inside  atomic.Int64
	peak    atomic.Int64
	hold    chan struct{}
	holding atomic.Bool
}

func newBlockingJournal() *blockingJournal {
	j := &blockingJournal{hold: make(chan struct{})}
	j.state = store.NewState()
	return j
}

func (j *blockingJournal) SessionUpsert(m store.SessionMeta) error {
	n := j.inside.Add(1)
	for {
		peak := j.peak.Load()
		if n <= peak || j.peak.CompareAndSwap(peak, n) {
			break
		}
	}
	if j.holding.Load() {
		<-j.hold
	}
	j.inside.Add(-1)
	return nil
}

// TestASessionLivesInExactlyOneShard: a session, and the connection attached
// to it, are addressed through the same shard, so no operation on one
// session touches two.
func TestASessionLivesInExactlyOneShard(t *testing.T) {
	m, _ := newManager(t, nil)
	c := newFake("client-a", packet.V5, 100)
	connectV5(m, c, true, 600, nil)

	k := key{m.opts.Namespace, "client-a"}
	sh := m.shardFor(k)
	if m.shardForConn(c) != sh {
		t.Fatal("a connection and its session must share a shard")
	}
	found := 0
	for _, other := range m.shards {
		other.mu.Lock()
		if _, ok := other.sessions[k]; ok {
			found++
			if other != sh {
				t.Fatal("the session is not in the shard its key selects")
			}
		}
		if _, ok := other.byConn[c]; ok && other != sh {
			t.Fatal("the connection is not in the shard its key selects")
		}
		other.mu.Unlock()
	}
	if found != 1 {
		t.Fatalf("the session is in %d shards, want 1", found)
	}
}

// TestShardsSpreadSequentialClientIdentifiers is the edge case that would
// otherwise look exactly like "sharding did not help": the measurement's
// clients are named f000000, f000001, … and a hash with poor low-bit
// behaviour over them would put the whole storm in one shard.
func TestShardsSpreadSequentialClientIdentifiers(t *testing.T) {
	m, _ := newManager(t, nil)
	const n = 100000
	counts := make([]int, len(m.shards))
	for i := 0; i < n; i++ {
		counts[shardIndexFor(m, key{m.opts.Namespace, fmt.Sprintf("f%06d", i)})]++
	}
	want := n / len(m.shards)
	for i, got := range counts {
		if got < want/2 || got > want*2 {
			t.Fatalf("shard %d holds %d of %d identifiers (even share %d): the hash does not spread the "+
				"identifiers the measurement uses", i, got, n, want)
		}
	}
}

// twoIdentifiersInDifferentShards finds a pair that lands in different
// shards, and fails naming the search when the manager has only one shard to
// give.
func twoIdentifiersInDifferentShards(t *testing.T, m *Manager) (string, string) {
	t.Helper()
	const first = "aaa"
	target := m.shardFor(key{m.opts.Namespace, first})
	for i := 0; i < 5000; i++ {
		id := fmt.Sprintf("b%04d", i)
		if m.shardFor(key{m.opts.Namespace, id}) != target {
			return first, id
		}
	}
	t.Fatalf("no identifier of 5000 lands outside %q's shard, of %d shards: the pair this test needs cannot be built", first, len(m.shards))
	return "", ""
}

func shardIndexFor(m *Manager, k key) int {
	target := m.shardFor(k)
	for i, sh := range m.shards {
		if sh == target {
			return i
		}
	}
	return -1
}

// TestTwoKeyOperationsInDifferentShardsDoNotDeadlock: the classic mistake in
// this refactor. Two hand-overs in opposite directions, repeatedly.
func TestTwoKeyOperationsInDifferentShardsDoNotDeadlock(t *testing.T) {
	m, _ := newManager(t, nil)
	// Two identifiers in *different* shards, searched for rather than hoped
	// for: the shard of an identifier is a pure function of it, so a search
	// is deterministic and cheap, and a skip here would report a pass for a
	// deadlock this test never drove.
	idA, idB := twoIdentifiersInDifferentShards(t, m)
	a, b := newFake(idA, packet.V5, 100), newFake(idB, packet.V5, 100)
	connectV5(m, a, true, 600, nil)
	connectV5(m, b, true, 600, nil)
	if m.shardForConn(a) == m.shardForConn(b) {
		t.Fatalf("%s and %s share a shard although they were chosen not to", idA, idB)
	}
	done := make(chan struct{})
	var wg sync.WaitGroup
	for _, pair := range [][2]string{{idA, idB}, {idB, idA}} {
		wg.Add(1)
		go func(first, second string) {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				m.Export(m.opts.Namespace, first)
				m.Export(m.opts.Namespace, second)
				m.SetEpoch(m.opts.Namespace, first, 1)
				m.markMoving(m.opts.Namespace, second)
			}
		}(pair[0], pair[1])
	}
	time.Sleep(300 * time.Millisecond)
	close(done)
	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(10 * time.Second):
		t.Fatal("two-key operations in opposite directions deadlocked")
	}
}

// TestWholeManagerReadsSeeAConsistentSet: a checkpoint or a record listing
// must observe a set that existed, and must not deadlock against the
// single-key work running beside it.
func TestWholeManagerReadsSeeAConsistentSet(t *testing.T) {
	m, _ := newManager(t, nil)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("c%d", i)
			for {
				select {
				case <-stop:
					return
				default:
				}
				c := newFake(id, packet.V5, 10)
				m.Connect(c, &packet.Connect{ProtocolLevel: packet.V5, ClientID: id, CleanStart: true}, 0)
				m.Resume(c)
				m.Disconnect(c, true, packet.NormalDisconnection, false)
			}
		}(i)
	}
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if n := m.Count(); n < 0 || n > 8 {
			t.Fatalf("Count saw %d sessions, which never existed", n)
		}
		_ = m.Records()
		_ = m.Snapshot()
	}
	close(stop)
	wg.Wait()
}

// TestStoreAdmissionBoundsWritersToTheStore is I6: the single map lock used
// to bound how many connects reached the store at once as a side effect, and
// sharding removes that. The bound is now explicit and this is what holds it.
func TestStoreAdmissionBoundsWritersToTheStore(t *testing.T) {
	j := newBlockingJournal()
	m, _ := newManager(t, func(o *Options) { o.Journal = j; o.StoreAdmission = 3 })
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("g%03d", i)
			c := newFake(id, packet.V5, 10)
			m.Connect(c, persistentConnect(id), 0)
		}(i)
	}
	wg.Wait()
	if peak := j.peak.Load(); peak > 3 {
		t.Fatalf("%d connects were inside the store's write path at once, bound is 3", peak)
	}
	if peak := j.peak.Load(); peak == 0 {
		t.Fatal("no connect reached the store: the test proves nothing")
	}
}

// TestAdmissionGateIsNotHeldUnderAShardLock: a connect waiting for a gate
// slot must not be holding a shard, or the convoy comes back one shard at a
// time. With the gate full and a connect queued behind it, an unrelated
// read of that same shard still completes.
func TestAdmissionGateIsNotHeldUnderAShardLock(t *testing.T) {
	j := newBlockingJournal()
	j.holding.Store(true)
	m, _ := newManager(t, func(o *Options) { o.Journal = j; o.StoreAdmission = 1 })

	// One connect goes in and stays in the store's write path.
	first := newFake("holder", packet.V5, 10)
	go m.Connect(first, persistentConnect("holder"), 0)
	waitUntil(t, "the first connect to reach the store", func() bool { return j.inside.Load() == 1 })

	// A second connect queues at the gate. Its key is chosen in a *different*
	// shard from the holder's, because the holder legitimately holds its own
	// shard while it is inside the store — the property under test is that
	// the one *waiting* holds nothing.
	holderShard := m.shardFor(key{m.opts.Namespace, "holder"})
	var queuedID string
	var queuedShard *shard
	for i := 0; i < 5000 && queuedID == ""; i++ {
		id := fmt.Sprintf("q%04d", i)
		if sh := m.shardFor(key{m.opts.Namespace, id}); sh != holderShard {
			queuedID, queuedShard = id, sh
		}
	}
	if queuedID == "" {
		t.Fatalf("no identifier of 5000 lands outside the holder's shard, of %d shards: the convoy this test looks for cannot be set up", len(m.shards))
	}
	queued := newFake(queuedID, packet.V5, 10)
	go m.Connect(queued, persistentConnect(queuedID), 0)
	time.Sleep(100 * time.Millisecond)

	// The queued connect's own shard must still be free. If waiting for the
	// gate happened under the shard lock, this would block for as long as the
	// holder stays in the store — the convoy back, one shard at a time.
	done := make(chan struct{})
	go func() {
		queuedShard.mu.Lock()
		queuedShard.mu.Unlock() //nolint:staticcheck // the point is that it can be taken
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("a connect waiting for the admission gate is holding its shard lock")
	}
	j.holding.Store(false)
	close(j.hold)
}

// persistentConnect is a CONNECT whose session is journalled: a session with
// a zero expiry is not persistent and never reaches the store, so a test of
// the store's admission bound would prove nothing with one.
func persistentConnect(id string) *packet.Connect {
	expiry := uint32(600)
	return &packet.Connect{ProtocolLevel: packet.V5, ClientID: id, CleanStart: true,
		Properties: packet.Properties{SessionExpiryInterval: &expiry}}
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
