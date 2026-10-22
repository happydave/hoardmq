package etcdraft

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
	"github.com/happydave/hoardmq/internal/cluster/lease"
	"github.com/happydave/hoardmq/internal/cluster/records"
	"github.com/happydave/hoardmq/internal/cluster/transport"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// xferMetrics keeps every counter and gauge by name and labels.
type xferMetrics struct {
	mu sync.Mutex
	v  map[string]float64
	g  map[string]float64
}

func metricKey(name string, labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(name)
	for _, k := range keys {
		fmt.Fprintf(&b, "|%s=%s", k, labels[k])
	}
	return b.String()
}

func (m *xferMetrics) Add(name string, labels map[string]string, delta float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.v == nil {
		m.v = map[string]float64{}
	}
	m.v[metricKey(name, labels)] += delta
}

func (m *xferMetrics) Set(name string, labels map[string]string, value float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.g == nil {
		m.g = map[string]float64{}
	}
	k := metricKey(name, labels)
	m.g[k] = value
	if value > m.g[k+"|peak"] {
		m.g[k+"|peak"] = value
	}
}

// sum adds every counter of name whose labels include want.
func (m *xferMetrics) sum(name string, want map[string]string) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	total := 0.0
	for k, v := range m.v {
		parts := strings.Split(k, "|")
		if parts[0] != name {
			continue
		}
		have := map[string]string{}
		for _, p := range parts[1:] {
			kv := strings.SplitN(p, "=", 2)
			have[kv[0]] = kv[1]
		}
		ok := true
		for wk, wv := range want {
			if have[wk] != wv {
				ok = false
			}
		}
		if ok {
			total += v
		}
	}
	return total
}

func (m *xferMetrics) gauge(name string) (value, peak float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := metricKey(name, nil)
	return m.g[k], m.g[k+"|peak"]
}

// warnLog keeps every warning's message and attributes.
type warnLog struct {
	mu   sync.Mutex
	recs []map[string]string
	echo slog.Handler // HOARDMQ_TEST_DEBUG: every record to stderr as well
	node string
}

func (w *warnLog) Enabled(_ context.Context, l slog.Level) bool {
	return l >= slog.LevelWarn || w.echo != nil
}
func (w *warnLog) Handle(ctx context.Context, r slog.Record) error {
	if w.echo != nil {
		r2 := r.Clone()
		r2.AddAttrs(slog.String("node", w.node))
		_ = w.echo.Handle(ctx, r2)
	}
	if r.Level < slog.LevelWarn {
		return nil
	}
	rec := map[string]string{"msg": r.Message}
	r.Attrs(func(a slog.Attr) bool { rec[a.Key] = a.Value.String(); return true })
	w.mu.Lock()
	w.recs = append(w.recs, rec)
	w.mu.Unlock()
	return nil
}
func (w *warnLog) WithAttrs(as []slog.Attr) slog.Handler { return w }
func (w *warnLog) WithGroup(string) slog.Handler         { return w }

// count is how many warnings carry msg and, when given, reason.
func (w *warnLog) count(msg, reason string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	for _, r := range w.recs {
		if r["msg"] == msg && (reason == "" || r["reason"] == reason) {
			n++
		}
	}
	return n
}

// snapCluster is three nodes with the broker's lease table and records on
// every machine, the shape a snapshot the size of a fleet takes.
type snapCluster struct {
	t       *testing.T
	c       *tcluster
	names   []string
	mu      sync.Mutex
	tables  map[string]*lease.Table
	metrics map[string]*xferMetrics
	logs    map[string]*warnLog
	hooks   map[string]*transferHooks
	reports map[string][]raft.SnapshotStatus
	tooBig  atomic.Int64 // frames over the cap, or refused for it
}

type snapOpts struct {
	frame      int
	maxVersion map[string]uint8
	hooks      map[string]*transferHooks
}

func newSnapCluster(t *testing.T, so snapOpts) *snapCluster {
	sc := &snapCluster{t: t, names: []string{"n1", "n2", "n3"}, tables: map[string]*lease.Table{}, metrics: map[string]*xferMetrics{},
		logs: map[string]*warnLog{}, hooks: so.hooks, reports: map[string][]raft.SnapshotStatus{}}
	if sc.hooks == nil {
		sc.hooks = map[string]*transferHooks{}
	}
	for _, name := range sc.names {
		sc.metrics[name] = &xferMetrics{}
		sc.logs[name] = &warnLog{node: name}
		if os.Getenv("HOARDMQ_TEST_DEBUG") != "" {
			sc.logs[name].echo = slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})
		}
		if sc.hooks[name] == nil {
			sc.hooks[name] = &transferHooks{}
		}
		name := name
		h := sc.hooks[name]
		prev := h.onReport
		h.onReport = func(to uint64, st raft.SnapshotStatus) {
			sc.mu.Lock()
			sc.reports[name] = append(sc.reports[name], st)
			sc.mu.Unlock()
			if prev != nil {
				prev(to, st)
			}
		}
	}
	sc.c = newTCluster(t, func(o *Options) {
		m := consensus.NewMachine()
		tab := lease.NewTable()
		m.Register(lease.Kind, tab)
		m.Register(records.Kind, records.New(0))
		o.Machine = m
		o.SnapshotEntries = 20
		o.SnapshotTrailing = 10
		o.Metrics = sc.metrics[o.Node]
		o.Logger = slog.New(sc.logs[o.Node])
		o.transferHooks = sc.hooks[o.Node]
		sc.mu.Lock()
		sc.tables[o.Node] = tab
		sc.mu.Unlock()
	})
	sc.c.maxFrame, sc.c.maxVersion = so.frame, so.maxVersion
	for _, name := range sc.names {
		sc.c.addTransport(name, others(name, sc.names))
	}
	for _, name := range sc.names {
		sc.c.startCore(sc.c.nodes[name], sc.names, nil)
		sc.watchFrames(sc.c.nodes[name])
	}
	return sc
}

func (sc *snapCluster) watchFrames(n *tnode) {
	limit := sc.c.maxFrame
	if limit == 0 {
		limit = 16 << 20
	}
	obs := func(_ string, _ transport.Class, size int, err error) {
		if size > limit || (err != nil && strings.Contains(err.Error(), "exceeds the frame limit")) {
			sc.tooBig.Add(1)
		}
	}
	n.observe.Store(&obs)
}

func (sc *snapCluster) table(name string) *lease.Table {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.tables[name]
}

func leaseID(i int) string { return fmt.Sprintf("client-%030d", i) }

// populate proposes n leases in batches and waits until every node holds
// them.
func (sc *snapCluster) populate(n int) {
	sc.t.Helper()
	now := time.Date(2026, 10, 22, 1, 0, 0, 0, time.UTC)
	for start := 0; start < n; start += 200 {
		var ops []lease.Op
		for i := start; i < min(start+200, n); i++ {
			ops = append(ops, lease.Op{Kind: lease.OpAcquire, ClientID: leaseID(i), Node: sc.names[i%3], Now: now})
		}
		proposeOnLeader(sc.t, sc.c, lease.Kind, lease.Encode(ops))
	}
	sc.pad("fill", 25)
	waitFor(sc.t, "every node holds the leases", 60*time.Second, func() bool {
		for _, name := range sc.names {
			if tb := sc.table(name); tb == nil || tb.Count() != n {
				return false
			}
		}
		return true
	})
}

func (sc *snapCluster) pad(prefix string, k int) {
	for i := 0; i < k; i++ {
		proposeOnLeader(sc.t, sc.c, consensus.KindConfig, consensus.ConfigCommand(fmt.Sprintf("%s%d", prefix, i), "v")[1:])
	}
}

// followers are the two nodes that do not lead, the first the victim.
func (sc *snapCluster) followers() (lead, victim, other *tnode) {
	lead = sc.c.leader()
	for _, name := range sc.names {
		if nd := sc.c.nodes[name]; nd != lead {
			if victim == nil {
				victim = nd
			} else {
				other = nd
			}
		}
	}
	return lead, victim, other
}

// needSnapshot stops the victim and leaves it needing a snapshot: wiped, or
// behind the leader's compacted log.
func (sc *snapCluster) needSnapshot(victim *tnode, mode string) {
	sc.t.Helper()
	victim.core.Stop()
	switch mode {
	case "wiped":
		if err := os.RemoveAll(victim.dir); err != nil {
			sc.t.Fatal(err)
		}
	case "behind":
		sc.pad("behind", 45)
		waitFor(sc.t, "the leader's log compacted past the victim", 10*time.Second, func() bool {
			first, _ := sc.c.leader().store.FirstIndex()
			return first > victim.core.Status().Applied+1
		})
	default:
		sc.t.Fatalf("mode %q", mode)
	}
	sc.c.startCore(victim, sc.names, nil)
}

// needSnapshotStopped wipes an already stopped victim and starts it again,
// telling started of its new core before anything is received.
func (sc *snapCluster) needSnapshotStopped(victim *tnode, started func(*Core)) {
	sc.t.Helper()
	if err := os.RemoveAll(victim.dir); err != nil {
		sc.t.Fatal(err)
	}
	sc.c.startCore(victim, sc.names, nil)
	started(victim.core)
}

// rejoined reports whether the victim has caught up: a voter holding every
// lease and, behind, applied past the leader's snapshot.
func (sc *snapCluster) rejoined(victim *tnode, n int, snapAt uint64) bool {
	lead := sc.c.leader()
	role := ""
	for _, m := range lead.core.Members() {
		if m.Name == victim.name {
			role = string(m.Role)
		}
	}
	tb := sc.table(victim.name)
	return tb != nil && tb.Count() == n && role == string(consensus.Voter) && victim.core.Ready() && victim.core.Status().Applied >= snapAt
}

// secondStop stops the other follower and requires a proposal to commit.
func (sc *snapCluster) secondStop(other *tnode) error {
	other.core.Stop()
	deadline := time.Now().Add(5 * time.Second)
	var err error
	for time.Now().Before(deadline) {
		for _, name := range sc.names {
			nd := sc.c.nodes[name]
			if nd == other || nd.core.Status().Role != "leader" {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			_, err = nd.core.Propose(ctx, consensus.KindConfig, consensus.ConfigCommand("after", "v")[1:])
			cancel()
			if err == nil {
				return nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err == nil {
		err = fmt.Errorf("no leader among the two left")
	}
	return err
}

func envInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil {
		return v
	}
	return def
}

// runSnapshotScenario is the follower that needs a snapshot larger than the
// frame: it must rejoin, every frame stays under the cap, and the cluster
// survives the other follower's stop.
func runSnapshotScenario(t *testing.T, mode string, frame, n int) {
	sc := newSnapCluster(t, snapOpts{frame: frame})
	sc.c.settled(3)
	sc.populate(n)
	lead, victim, other := sc.followers()
	if s := lead.core.Status().SnapshotIndex; s == 0 {
		t.Fatal("the leader holds no snapshot")
	}
	size, _ := sc.metrics[lead.name].gauge("hoardmq_raft_snapshot_bytes")
	if int(size) <= frame {
		t.Fatalf("the snapshot (%v B) is not above the frame (%d B), so the scenario shows nothing", size, frame)
	}
	sc.needSnapshot(victim, mode)
	snapAt := sc.c.leader().core.Status().SnapshotIndex
	began := time.Now()
	waitFor(t, "the victim rejoins or a frame goes over the cap", 30*time.Second, func() bool {
		return sc.tooBig.Load() != 0 || sc.rejoined(victim, n, snapAt)
	})
	if k := sc.tooBig.Load(); k != 0 {
		t.Fatalf("%d frames over the cap", k)
	}
	t.Logf("%s victim %s rejoined in %v; snapshot %v B over a %d B frame", mode, victim.name, time.Since(began).Round(time.Millisecond), size, frame)
	if got := sc.metrics[victim.name].sum("hoardmq_raft_snapshot_transfers_total", map[string]string{"direction": "in", "outcome": "ok"}); got < 1 {
		t.Fatalf("the victim counted %v completed transfers in", got)
	}
	if err := sc.secondStop(other); err != nil {
		t.Fatalf("after the second stop: %v", err)
	}
}

// TestSnapshotAboveTheFrameReachesAWipedFollower: a wiped member needing a
// snapshot larger than the frame rejoins as a voter.
func TestSnapshotAboveTheFrameReachesAWipedFollower(t *testing.T) {
	runSnapshotScenario(t, "wiped", 1<<20, 30000)
}

// TestSnapshotAboveTheFrameReachesABehindVoter: a voter behind the leader's
// compacted log, needing a snapshot larger than the frame, catches up.
func TestSnapshotAboveTheFrameReachesABehindVoter(t *testing.T) {
	runSnapshotScenario(t, "behind", 1<<20, 30000)
}

// TestSnapshotAtTheDefaultFrame runs the scenario at the transport's default
// frame with HOARDMQ_SNAPSHOT_SCALE leases (80,000 is above the old ceiling).
func TestSnapshotAtTheDefaultFrame(t *testing.T) {
	n := envInt("HOARDMQ_SNAPSHOT_SCALE", 0)
	if n == 0 {
		t.Skip("set HOARDMQ_SNAPSHOT_SCALE to the lease count")
	}
	for _, mode := range []string{"wiped", "behind"} {
		t.Run(mode, func(t *testing.T) { runSnapshotScenario(t, mode, 16<<20, n) })
	}
}

// failureScenario is a wiped victim needing a snapshot, with the hooks given.
func failureScenario(t *testing.T, so snapOpts) (*snapCluster, *tnode, *tnode) {
	sc := newSnapCluster(t, so)
	sc.c.settled(3)
	sc.populate(3000)
	lead, victim, _ := sc.followers()
	sc.needSnapshot(victim, "wiped")
	return sc, sc.c.nodes[lead.name], victim
}

// TestTransferToAnOlderPeerIsCountedAndLogged: a follower whose link is below
// framing version 5 cannot take a stream; each attempt is counted, logged
// and reported failed.
func TestTransferToAnOlderPeerIsCountedAndLogged(t *testing.T) {
	// Two nodes advertise framing 4, so every link is below 5 whoever leads.
	sc := newSnapCluster(t, snapOpts{frame: 1 << 20, maxVersion: map[string]uint8{"n2": transport.Version4, "n3": transport.Version4}})
	sc.c.settled(3)
	sc.populate(3000)
	lead, victim, _ := sc.followers()
	sc.needSnapshot(victim, "wiped")
	m := sc.metrics[lead.name]
	waitFor(t, "a transfer refused for the peer's version", 20*time.Second, func() bool {
		return m.sum("hoardmq_raft_snapshot_transfers_total", map[string]string{"direction": "out", "reason": reasonPeerVersion}) >= 2
	})
	if sc.logs[lead.name].count("snapshot transfer failed", reasonPeerVersion) < 1 {
		t.Fatal("no warning names the peer-version failure")
	}
	sc.mu.Lock()
	reports := append([]raft.SnapshotStatus(nil), sc.reports[lead.name]...)
	sc.mu.Unlock()
	failed := m.sum("hoardmq_raft_snapshot_transfers_total", map[string]string{"direction": "out", "outcome": "failed"})
	if len(reports) == 0 || float64(len(reports)) > failed {
		t.Fatalf("%d reports for %v failed transfers", len(reports), failed)
	}
	for _, st := range reports {
		if st != raft.SnapshotFailure {
			t.Fatalf("a refused transfer reported %v", st)
		}
	}
}

// TestTransferBrokenByALinkResetIsRetried: the data connection reset during
// a transfer ends it as link-down, counted and logged; the retry after the
// backoff completes and the victim rejoins.
func TestTransferBrokenByALinkResetIsRetried(t *testing.T) {
	var once sync.Once
	var sc *snapCluster
	hooks := map[string]*transferHooks{}
	var leadName atomic.Value
	for _, name := range []string{"n1", "n2", "n3"} {
		me := name
		hooks[me] = &transferHooks{corruptChunk: func(off uint64, data []byte) []byte {
			if off > 0 {
				once.Do(func() {
					if l, ok := leadName.Load().(string); ok {
						sc.c.nodes[me].tr.Reset(l, transport.RoleData, "test reset")
					}
				})
			}
			return data
		}}
	}
	sc = newSnapCluster(t, snapOpts{frame: 1 << 20, hooks: hooks})
	sc.c.settled(3)
	sc.populate(8000)
	lead, victim, _ := sc.followers()
	leadName.Store(lead.name)
	sc.needSnapshot(victim, "wiped")
	m := sc.metrics[lead.name]
	waitFor(t, "a transfer ended by the reset", 20*time.Second, func() bool {
		return m.sum("hoardmq_raft_snapshot_transfers_total", map[string]string{"direction": "out", "reason": reasonLinkDown}) >= 1
	})
	waitFor(t, "the victim rejoins after the retry", 30*time.Second, func() bool { return sc.rejoined(victim, 8000, 0) })
	if sc.logs[lead.name].count("snapshot transfer failed", reasonLinkDown) < 1 {
		t.Fatal("no warning names the link-down failure")
	}
	if got := m.sum("hoardmq_raft_snapshot_transfers_total", map[string]string{"direction": "out", "outcome": "ok"}); got < 1 {
		t.Fatalf("completed transfers %v", got)
	}
}

// TestCorruptChunkIsRefusedOnBothSides: a chunk that arrives damaged fails
// the digest; the receiver refuses it and the sender counts the refusal; the
// next attempt completes.
func TestCorruptChunkIsRefusedOnBothSides(t *testing.T) {
	var once sync.Once
	hooks := map[string]*transferHooks{}
	for _, name := range []string{"n1", "n2", "n3"} {
		hooks[name] = &transferHooks{corruptChunk: func(off uint64, data []byte) []byte {
			once.Do(func() { data[len(data)/2] ^= 0xFF })
			return data
		}}
	}
	sc, lead, victim := failureScenario(t, snapOpts{frame: 1 << 20, hooks: hooks})
	waitFor(t, "the victim rejoins after the refusal", 30*time.Second, func() bool { return sc.rejoined(victim, 3000, 0) })
	if got := sc.metrics[victim.name].sum("hoardmq_raft_snapshot_transfers_total", map[string]string{"direction": "in", "outcome": "refused", "reason": reasonDigest}); got != 1 {
		t.Fatalf("receiver's digest refusals %v", got)
	}
	if got := sc.metrics[lead.name].sum("hoardmq_raft_snapshot_transfers_total", map[string]string{"direction": "out", "outcome": "refused", "reason": reasonDigest}); got != 1 {
		t.Fatalf("sender's counted refusals %v", got)
	}
	if sc.logs[victim.name].count("snapshot transfer refused or aborted", reasonDigest) != 1 || sc.logs[lead.name].count("snapshot transfer failed", refusedPfx+reasonDigest) != 1 {
		t.Fatal("the refusal is not logged on both sides")
	}
	// One report for each transfer: the refused one failed, the next one
	// finished, in that order.
	sc.mu.Lock()
	reports := append([]raft.SnapshotStatus(nil), sc.reports[lead.name]...)
	sc.mu.Unlock()
	if len(reports) != 2 || reports[0] != raft.SnapshotFailure || reports[1] != raft.SnapshotFinish {
		t.Fatalf("reports %v, want one failure then one finish", reports)
	}
}

// TestReceiverRefusesMalformedTransfers: an offer of an unknown encoding, a
// chunk out of order and a chunk past the offered size are refused and
// counted by the receiver.
func TestReceiverRefusesMalformedTransfers(t *testing.T) {
	sc := newSnapCluster(t, snapOpts{})
	sc.c.settled(3)
	lead, victim, _ := sc.followers()
	m := sc.metrics[victim.name]
	send := func(p []byte) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := lead.tr.Send(ctx, victim.name, transport.SnapshotData, p); err != nil {
			t.Fatal(err)
		}
	}
	refused := func(reason string) float64 {
		return m.sum("hoardmq_raft_snapshot_transfers_total", map[string]string{"direction": "in", "outcome": "refused", "reason": reason})
	}
	send(offerMsg{id: 1, size: 10, format: consensus.FileFormat, encodings: []byte{9}}.encode())
	waitFor(t, "an unknown encoding refused", 5*time.Second, func() bool { return refused(reasonEncoding) == 1 })

	send(offerMsg{id: 2, size: 10, format: 3}.encode())
	waitFor(t, "an unknown format refused", 5*time.Second, func() bool { return refused(reasonFormat) == 1 })

	valid := func(id uint64) []byte {
		d := snapDesc{Index: 5, Term: 1, Size: 10, Name: builtName(5, 1)}
		msg, err := proto.Marshal(&pb.Message{Type: pb.MsgSnap.Enum(), To: proto.Uint64(victim.core.ID()), From: proto.Uint64(lead.core.ID()),
			Snapshot: &pb.Snapshot{Data: d.encode(), Metadata: &pb.SnapshotMetadata{Index: proto.Uint64(5), Term: proto.Uint64(1)}}})
		if err != nil {
			t.Fatal(err)
		}
		return offerMsg{id: id, size: 10, digest: d.Digest, format: consensus.FileFormat, encodings: []byte{1, 2}, chunk: snapshotChunk, msg: msg}.encode()
	}
	send(valid(3))
	send(idMsg(xChunk, 3, 4, []byte("abc")))
	waitFor(t, "a chunk out of order refused", 5*time.Second, func() bool { return refused(reasonOrder) == 1 })

	send(valid(4))
	send(idMsg(xChunk, 4, 0, make([]byte, 11)))
	waitFor(t, "a chunk past the offered size refused", 5*time.Second, func() bool { return refused(reasonSize) == 1 })
	entries, _ := os.ReadDir(victim.core.snapDir)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), partialEx) {
			t.Fatalf("a refused transfer left %s", e.Name())
		}
	}
}

// TestFloorIsReleasedAtTheFailureNotTheReport: a follower that never
// acknowledges pins the leader's log at the snapshot it is sent, and only
// until the transfer fails; the compaction that follows does not wait out
// the backoff.
func TestFloorIsReleasedAtTheFailureNotTheReport(t *testing.T) {
	var drop atomic.Bool
	hooks := map[string]*transferHooks{}
	for _, name := range []string{"n1", "n2", "n3"} {
		hooks[name] = &transferHooks{
			dropAcks: func() bool { return drop.Load() },
			backoff:  func(int) time.Duration { return 30 * time.Second },
		}
	}
	sc := newSnapCluster(t, snapOpts{hooks: hooks})
	sc.c.settled(3)
	sc.populate(3000)
	lead, victim, _ := sc.followers()
	drop.Store(true)
	sc.needSnapshot(victim, "wiped")
	m := sc.metrics[lead.name]
	waitFor(t, "a transfer in flight", 10*time.Second, func() bool { return len(lead.core.floors()) == 1 })
	floor := lead.core.floors()[0]
	sc.pad("pinned", 45)
	first, _ := lead.store.FirstIndex()
	if first > floor+1 {
		t.Fatalf("the log was compacted to %d past the floor %d while the transfer was in flight", first, floor)
	}
	if _, peak := m.gauge("hoardmq_raft_log_pinned_entries"); peak <= 0 {
		t.Fatalf("pinned entries never reported above 0 (peak %v)", peak)
	}
	waitFor(t, "the transfer's ack timeout", 5*time.Second, func() bool {
		return m.sum("hoardmq_raft_snapshot_transfers_total", map[string]string{"direction": "out", "reason": reasonAckTimeout}) >= 1
	})
	failedAt := time.Now()
	waitFor(t, "the log compacted past the floor", lead.core.electionTimeout()+time.Second, func() bool {
		f, _ := lead.store.FirstIndex()
		return f > floor+1
	})
	if time.Since(failedAt) > 5*time.Second {
		t.Fatal("the compaction waited for the backoff")
	}
	// The file the transfer read was superseded during the pin; with the
	// transfer ended only the stored snapshot's file is left.
	waitFor(t, "the superseded file removed", time.Second, func() bool {
		files := snapFiles(t, lead.core.snapDir)
		return len(files) == 1 && files[0] == lead.core.storedSnapshotName()
	})
	sc.mu.Lock()
	n := len(sc.reports[lead.name])
	sc.mu.Unlock()
	if n != 0 {
		t.Fatalf("%d reports before the backoff ended", n)
	}
}

// TestBackoffDoublesToItsCap: one election timeout after the first failure,
// doubling, capped at thirty.
func TestBackoffDoublesToItsCap(t *testing.T) {
	c := &Core{o: Options{Tick: 10 * time.Millisecond, ElectionTicks: 10}}
	e := c.electionTimeout()
	for failures, want := range map[int]time.Duration{1: e, 2: 2 * e, 3: 4 * e, 5: 16 * e, 6: 30 * e, 40: 30 * e} {
		if got := c.backoffFor(failures); got != want {
			t.Errorf("backoff after %d failures: %v, want %v", failures, got, want)
		}
	}
}

// TestRemovedMemberCancelsItsTransfer: a follower removed while a transfer
// to it stalls has the transfer cancelled at once, its floor released,
// without waiting out the ack timeout.
func TestRemovedMemberCancelsItsTransfer(t *testing.T) {
	var drop atomic.Bool
	hooks := map[string]*transferHooks{}
	for _, name := range []string{"n1", "n2", "n3"} {
		hooks[name] = &transferHooks{dropAcks: func() bool { return drop.Load() }, backoff: func(int) time.Duration { return 30 * time.Second }}
	}
	sc := newSnapCluster(t, snapOpts{hooks: hooks})
	sc.c.settled(3)
	sc.populate(3000)
	lead, victim, _ := sc.followers()
	drop.Store(true)
	sc.needSnapshot(victim, "wiped")
	waitFor(t, "a transfer in flight", 10*time.Second, func() bool { return len(lead.core.floors()) == 1 })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	removed := time.Now()
	if err := lead.core.RemoveMember(ctx, victim.name); err != nil {
		t.Fatal(err)
	}
	cancelled := func() float64 {
		return sc.metrics[lead.name].sum("hoardmq_raft_snapshot_transfers_total", map[string]string{"direction": "out", "reason": reasonCancelled})
	}
	waitFor(t, "the transfer cancelled", lead.core.ackTimeout()/2, func() bool { return len(lead.core.floors()) == 0 && cancelled() >= 1 })
	if got := cancelled(); got != 1 {
		t.Fatalf("cancelled transfers %v, %v after the removal", got, time.Since(removed))
	}
}

// TestLeaderThatStepsDownCancelsItsTransfers: a leader cut off from the
// other voter loses its lead by check-quorum, and the transfer it was making
// to a learner it still reaches is cancelled, its floor released.
func TestLeaderThatStepsDownCancelsItsTransfers(t *testing.T) {
	var drop atomic.Bool
	hooks := map[string]*transferHooks{}
	for _, name := range []string{"n1", "n2", "n3"} {
		hooks[name] = &transferHooks{dropAcks: func() bool { return drop.Load() }, backoff: func(int) time.Duration { return 30 * time.Second }}
	}
	sc := newSnapCluster(t, snapOpts{hooks: hooks})
	sc.c.settled(3)
	sc.populate(3000)
	lead, victim, other := sc.followers()
	drop.Store(true)
	sc.needSnapshot(victim, "wiped")
	waitFor(t, "a transfer in flight", 10*time.Second, func() bool { return len(lead.core.floors()) == 1 })
	lead.tr.Isolate(other.name, true)
	other.tr.Isolate(lead.name, true)
	cancelled := func() float64 {
		return sc.metrics[lead.name].sum("hoardmq_raft_snapshot_transfers_total", map[string]string{"direction": "out", "reason": reasonCancelled})
	}
	waitFor(t, "the old leader cancels its transfer", lead.core.ackTimeout()/2, func() bool {
		return lead.core.Status().Role != "leader" && len(lead.core.floors()) == 0 && cancelled() >= 1
	})
	if got := cancelled(); got != 1 {
		t.Fatalf("cancelled transfers %v", got)
	}
	lead.tr.Isolate(other.name, false)
	other.tr.Isolate(lead.name, false)
}

// TestWindowBoundsUnacknowledgedChunks: a receiver that acknowledges nothing
// is sent the window's four chunks and no more before the transfer times out.
func TestWindowBoundsUnacknowledgedChunks(t *testing.T) {
	var drop atomic.Bool
	hooks := map[string]*transferHooks{}
	for _, name := range []string{"n1", "n2", "n3"} {
		hooks[name] = &transferHooks{dropAcks: func() bool { return drop.Load() }, backoff: func(int) time.Duration { return 30 * time.Second }}
	}
	sc := newSnapCluster(t, snapOpts{hooks: hooks})
	sc.c.settled(3)
	sc.populate(30000)
	lead, victim, _ := sc.followers()
	if size, _ := sc.metrics[lead.name].gauge("hoardmq_raft_snapshot_bytes"); size <= snapshotWindow*snapshotChunk {
		t.Fatalf("the snapshot (%v B) fits the window, so the test shows nothing", size)
	}
	drop.Store(true)
	sc.needSnapshot(victim, "wiped")
	m := sc.metrics[lead.name]
	waitFor(t, "the transfer's ack timeout", 10*time.Second, func() bool {
		return m.sum("hoardmq_raft_snapshot_transfers_total", map[string]string{"direction": "out", "reason": reasonAckTimeout}) >= 1
	})
	if got := m.sum("hoardmq_raft_snapshot_transfer_bytes_total", map[string]string{"direction": "out"}); got != snapshotWindow*snapshotChunk {
		t.Fatalf("sent %v B unacknowledged, want the window's %d", got, snapshotWindow*snapshotChunk)
	}
}
