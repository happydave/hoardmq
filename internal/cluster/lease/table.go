// Package lease is the client-identifier lease service: leases with owner
// and epoch in the consensus core, time-fenced by per-node alive records,
// batched proposals, cross-node takeover with state pulled from the previous
// owner, the minority-partition rule, and adoption of a dead owner's
// sessions by the leader.
package lease

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
	"github.com/happydave/hoardmq/internal/store"
)

// Kind is the lease component's command kind in the state machine.
const Kind consensus.Kind = consensus.KindReserved

// Lease is one client identifier's ownership record.
type Lease struct {
	Namespace      string            `json:"ns"`
	ClientID       string            `json:"id"`
	Owner          string            `json:"owner"`
	Epoch          uint64            `json:"epoch"`
	Online         bool              `json:"online"`
	ExpiryInterval uint32            `json:"expiryInterval"`
	ExpiresAt      time.Time         `json:"expiresAt,omitempty"`
	DisconnectedAt time.Time         `json:"disconnectedAt,omitempty"`
	Will           *store.WillRecord `json:"will,omitempty"`
}

// NodeInfo is what the table knows about a node: when it last proposed
// that it was alive, on the wall clock of the leader that carried the
// proposal — for display, not for judging it — and the client address it
// advertises for redirects.
type NodeInfo struct {
	Alive     time.Time `json:"alive"`
	Advertise string    `json:"advertise,omitempty"`
	// Level is the highest cluster feature level this node's build
	// understands. A raise waits for every member to advertise the level it
	// would raise to. Zero from a build that predates the field,
	// which reads as level 1 — the baseline the first release tag defines.
	Level uint64 `json:"level,omitempty"`
	// AliveIndex is the log index of this node's last alive record, and
	// DeadAt the index at which the leader declared it dead, zero while it is
	// not. Both come from the log, so every node holds the same values: they
	// are what any node other than the leader reads to learn whether this one
	// is alive, in place of a clock.
	AliveIndex uint64 `json:"aliveIndex,omitempty"`
	DeadAt     uint64 `json:"deadAt,omitempty"`
	// ServedIndex is the highest log index at which this node has said its
	// adoptions were finished, and ServedRoutes how far its own route updates
	// had gone when it said so: the routing plane's batch number, which this
	// table stores and never reads. Zero while it has said nothing.
	ServedIndex  uint64 `json:"servedIndex,omitempty"`
	ServedRoutes uint64 `json:"servedRoutes,omitempty"`
	// CoverIndex is the highest grant through which this node has said the
	// sessions it took in transit are held, and CoverRoutes its route mark
	// when it said so. It says nothing about its own adoptions.
	CoverIndex  uint64 `json:"coverIndex,omitempty"`
	CoverRoutes uint64 `json:"coverRoutes,omitempty"`
	// LeavingAt is the index of the hand-off that marked this node leaving,
	// zero while it is not: the shard map's basis leaves it out while the
	// rest give the copy count. Its next alive record clears it.
	LeavingAt uint64 `json:"leavingAt,omitempty"`
}

// OpKind is an operation within a batched lease command.
type OpKind byte

// Operations.
const (
	OpAcquire      OpKind = 1
	OpDisconnected OpKind = 2
	OpRelease      OpKind = 3
	OpAlive        OpKind = 4
	OpReassign     OpKind = 5
	OpWillDone     OpKind = 6 // the adopter published the Will
	OpHandoff      OpKind = 7 // a draining owner moves every lease it holds to another node
	OpSetLevel     OpKind = 8 // the leader raises the cluster's feature level
	OpServed       OpKind = 9 // a node's adoptions up to an index are finished, and its routes out
)

// Op is one operation. Times come from the proposer so every node applies
// the same values.
type Op struct {
	Kind           OpKind            `json:"k"`
	Namespace      string            `json:"ns,omitempty"`
	ClientID       string            `json:"id,omitempty"`
	Node           string            `json:"node,omitempty"`
	To             string            `json:"to,omitempty"`
	Epoch          uint64            `json:"epoch,omitempty"`
	Will           *store.WillRecord `json:"will,omitempty"`
	ExpiryInterval uint32            `json:"expiryInterval,omitempty"`
	ExpiresAt      time.Time         `json:"expiresAt,omitempty"`
	WillArmed      bool              `json:"willArmed,omitempty"`
	Now            time.Time         `json:"now"`
	Advertise      string            `json:"advertise,omitempty"`
	SeenAliveIndex uint64            `json:"seenAliveIndex,omitempty"` // for reassign: the index of the owner's last alive record when the leader judged it
	Unreached      []string          `json:"unreached,omitempty"`      // for acquire: members the requester has no live link to, none of which it may displace while alive
	Level          uint64            `json:"level,omitempty"`          // for set-level: the cluster level proposed; for alive: the level this node's build understands
	Served         uint64            `json:"served,omitempty"`         // for served: the index up to which the node's adoptions are finished
	Routes         uint64            `json:"routes,omitempty"`         // for served: the node's route mark when it said so
	Cover          uint64            `json:"cover,omitempty"`          // for served: the grant through which the node's takeovers in transit are held
	CoverRoutes    uint64            `json:"coverRoutes,omitempty"`    // for served: the node's route mark when it said so of the cover
}

// Result is one operation's outcome.
type Result struct {
	OK        bool   `json:"ok"`
	Epoch     uint64 `json:"epoch,omitempty"`
	PrevOwner string `json:"prev,omitempty"`
	Reason    string `json:"reason,omitempty"`
	Moved     int    `json:"moved,omitempty"` // reassign: leases moved
	// PrevAlive is whether the previous owner was alive, by the log, when the
	// acquire applied.
	PrevAlive bool `json:"prevAlive,omitempty"`
	// Index is the log index the operation applied at; zero from a build
	// that predates it.
	Index uint64 `json:"index,omitempty"`
}

type key struct{ ns, id string }

// Table is the lease component: the replicated table plus read access.
type Table struct {
	mu     sync.RWMutex
	leases map[key]*Lease
	nodes  map[string]*NodeInfo
	// level is the cluster's feature level: what every member has been shown
	// to understand, and what a gated semantic reads to decide which
	// behaviour to apply. It changes at a log position, so every voter
	// switches at the same point in the history. Zero reads as 1,
	// the baseline the first release tag defines.
	//
	// Written with mu held and read without it: the replication fan-out
	// stamps it on every frame, and mu is the lock a takeover storm starves.
	level atomic.Uint64
	// levelProbe, when a test sets it before the core starts, sees every
	// operation with the log index and the level it is applied under — what
	// a gated semantic would read. Called with mu held.
	levelProbe func(index, level uint64, op Op)
	// hooked is the log index of the last command whose change hooks have
	// returned. It trails the applied state on purpose: a reader pairing it
	// with what the hooks produce — the adoptions a reassignment queues —
	// must not see the index before them.
	hooked atomic.Uint64
	// applied is the log index of the last command applied to the state,
	// stored with mu held before any hook runs: at or past the index of
	// whatever a reader has just seen in the table.
	applied atomic.Uint64
	// lostAt is, per node, the log index of the last command that took a
	// lease away from it — a takeover, a release, a reassignment, a
	// hand-off. It is derived from the log alone, so every node holds the
	// same value: the index a node that owns nothing was emptied at, which
	// peers compare each other's progress against. applying is the index of
	// the command being applied, for applyOne.
	lostAt   map[string]uint64
	applying uint64
	// granted is who each client was granted to by the command being
	// applied. A command carries one node's operations, so two owners for one
	// client at one index cannot happen; if it ever did, both would hold the
	// same token, which is the one thing a token must not allow.
	granted map[key]string
	// hooks run after an operation applies, on the applying node, without
	// the lock; they must not block.
	onChange    func(before *Lease, after *Lease)
	onRecovered func()
	// onAlive is told each node whose alive record a command carried,
	// onDeclared each node a command's reassignment declared dead, and
	// onRestore that a snapshot replaced the state, which carries no record of
	// when anything was last heard. None runs while recovering.
	onAlive    func(node string)
	onDeclared func(node string)
	onServed   func(node string)
	onLeaving  func(node string)
	onRestore  func()
	heard      []string // nodes whose alive records the command being applied carried
	declared   []string // nodes the command being applied declared dead
	served     []string // nodes whose served facts the command being applied recorded
	leaving    []string // nodes a hand-off in the command being applied marked leaving
	recovering bool
}

// NewTable creates an empty table.
func NewTable() *Table {
	return &Table{leases: map[key]*Lease{}, nodes: map[string]*NodeInfo{}, lostAt: map[string]uint64{}}
}

// Encode serialises a batch into the command payload (without the kind byte).
func Encode(ops []Op) []byte {
	b, _ := json.Marshal(ops)
	return b
}

// Apply implements consensus.Component.
func (t *Table) Apply(index uint64, payload []byte) (any, error) {
	var ops []Op
	if err := json.Unmarshal(payload, &ops); err != nil {
		return nil, fmt.Errorf("lease command: %w", err)
	}
	results := make([]Result, len(ops))
	var changes []change
	t.mu.Lock()
	t.applying = index
	t.granted = nil
	t.heard, t.declared, t.served, t.leaving = nil, nil, nil, nil
	for i, op := range ops {
		var cs []change
		if t.levelProbe != nil {
			t.levelProbe(index, t.Level(), op)
		}
		results[i], cs = t.applyOne(op)
		results[i].Index = index
		changes = append(changes, cs...)
	}
	if index > t.applied.Load() {
		t.applied.Store(index)
	}
	hook, onAlive, heard := t.onChange, t.onAlive, t.heard
	onDeclared, declared := t.onDeclared, t.declared
	onServed, served := t.onServed, t.served
	onLeaving, leaving := t.onLeaving, t.leaving
	if t.recovering {
		hook, onAlive, onDeclared, onServed, onLeaving = nil, nil, nil, nil, nil // replayed history is not acted on; Recovered reconciles
	}
	t.mu.Unlock()
	if onAlive != nil {
		for _, node := range heard {
			onAlive(node)
		}
	}
	if onDeclared != nil {
		for _, node := range declared {
			onDeclared(node)
		}
	}
	if onServed != nil {
		for _, node := range served {
			onServed(node)
		}
	}
	if onLeaving != nil {
		for _, node := range leaving {
			onLeaving(node)
		}
	}
	if hook != nil {
		for _, c := range changes {
			hook(c.before, c.after)
		}
	}
	if index > t.hooked.Load() {
		t.hooked.Store(index)
	}
	return results, nil
}

// HookedIndex is the log index of the last command this node has applied
// and run its change hooks for; AppliedIndex is the last applied, hooks run
// or not. HookedIndex never exceeds AppliedIndex.
func (t *Table) HookedIndex() uint64  { return t.hooked.Load() }
func (t *Table) AppliedIndex() uint64 { return t.applied.Load() }

// SetRecovering implements consensus.Recoverable: while recovering the
// table is updated without running the change hook.
func (t *Table) SetRecovering(on bool) {
	t.mu.Lock()
	t.recovering = on
	t.mu.Unlock()
}

// Recovered implements consensus.Recoverable: the service reconciles its
// timers and local sessions against the table as it now stands.
func (t *Table) Recovered() {
	t.mu.RLock()
	hook := t.onRecovered
	t.mu.RUnlock()
	if hook != nil {
		hook()
	}
}

// change is a lease before and after an operation; nil before means created,
// nil after means removed.
type change struct{ before, after *Lease }

func clone(l *Lease) *Lease {
	if l == nil {
		return nil
	}
	c := *l
	return &c
}

// applyOne applies one operation with the lock held and returns the result
// and the leases whose ownership or liveness changed.
func (t *Table) applyOne(op Op) (Result, []change) {
	k := key{op.Namespace, op.ClientID}
	switch op.Kind {
	case OpAcquire:
		// With an epoch the acquire is a renewal by a node that admitted its
		// client on the strength of holding the lease at that epoch: it may
		// arrive long after, and must not take a lease that has moved since,
		// because nobody is waiting on it to fetch the session from the node
		// that holds it now. Without one it takes the lease from whoever has it.
		l := t.leases[k]
		if l == nil {
			if op.Epoch != 0 {
				return Result{Reason: "stale"}, nil
			}
			if !t.grantTo(k, op.Node) {
				return Result{Reason: doubleGrant}, nil
			}
			n := &Lease{Namespace: op.Namespace, ClientID: op.ClientID, Owner: op.Node, Epoch: t.token(0), Online: true, ExpiryInterval: op.ExpiryInterval, Will: op.Will}
			t.leases[k] = n
			return Result{OK: true, Epoch: n.Epoch}, []change{{nil, clone(n)}}
		}
		if op.Epoch != 0 && (l.Owner != op.Node || l.Epoch != op.Epoch) {
			return Result{Reason: "stale"}, nil
		}
		before := clone(l)
		prev, prevAlive := "", false
		if l.Owner != op.Node {
			prevAlive = t.live(l.Owner)
			if prevAlive && contains(op.Unreached, l.Owner) {
				// The owner may still be serving the client and the requester
				// cannot tell it to stop: two live sessions for one client.
				return Result{Reason: ownerAlive}, nil
			}
			if !t.grantTo(k, op.Node) {
				return Result{Reason: doubleGrant}, nil
			}
			prev = l.Owner
			t.lost(prev)
			l.Epoch = t.token(l.Epoch)
			l.Owner = op.Node
		}
		l.Online, l.ExpiresAt, l.DisconnectedAt = true, time.Time{}, time.Time{}
		l.ExpiryInterval, l.Will = op.ExpiryInterval, op.Will
		return Result{OK: true, Epoch: l.Epoch, PrevOwner: prev, PrevAlive: prevAlive}, []change{{before, clone(l)}}
	case OpDisconnected:
		l := t.leases[k]
		if l == nil || l.Owner != op.Node || l.Epoch != op.Epoch {
			return Result{Reason: "stale"}, nil
		}
		before := clone(l)
		l.Online = false
		l.ExpiresAt, l.DisconnectedAt = op.ExpiresAt, op.Now
		if l.Will != nil {
			l.Will.Armed = op.WillArmed
			if !op.WillArmed {
				l.Will = nil
			}
		}
		return Result{OK: true, Epoch: l.Epoch}, []change{{before, clone(l)}}
	case OpRelease:
		l := t.leases[k]
		if l == nil || l.Owner != op.Node || l.Epoch != op.Epoch {
			return Result{Reason: "stale"}, nil
		}
		delete(t.leases, k)
		t.lost(op.Node)
		return Result{OK: true, Epoch: l.Epoch}, []change{{clone(l), nil}}
	case OpWillDone:
		l := t.leases[k]
		if l == nil || l.Owner != op.Node || l.Epoch != op.Epoch {
			return Result{Reason: "stale"}, nil
		}
		l.Will = nil
		return Result{OK: true, Epoch: l.Epoch}, nil
	case OpAlive:
		n := t.nodes[op.Node]
		if n == nil {
			n = &NodeInfo{}
			t.nodes[op.Node] = n
		}
		if op.Now.After(n.Alive) {
			n.Alive = op.Now
		}
		if op.Advertise != "" {
			n.Advertise = op.Advertise
		}
		if op.Level > n.Level {
			n.Level = op.Level
		}
		n.AliveIndex, n.DeadAt, n.LeavingAt = t.applying, 0, 0
		t.heard = append(t.heard, op.Node)
		return Result{OK: true}, nil
	case OpSetLevel:
		// The cluster's feature level, raised by the leader once every member
		// advertises at least this much. It is monotonic: a lower or repeated
		// level is refused, because a node that has already switched cannot
		// switch back.
		if op.Level <= t.level.Load() {
			return Result{Reason: "not a raise"}, nil
		}
		t.level.Store(op.Level)
		return Result{OK: true}, nil
	case OpServed:
		// A node's word that its adoptions begun at or below op.Served are
		// finished, or that the sessions it took in transit through op.Cover
		// are held, or both. Each is recorded when it rises, on the node's
		// record, never creating one: a record is what reads as alive.
		n := t.nodes[op.Node]
		if n == nil {
			return Result{Reason: "no record"}, nil
		}
		raised := false
		if op.Served > n.ServedIndex {
			n.ServedIndex, n.ServedRoutes = op.Served, op.Routes
			raised = true
		}
		if op.Cover > n.CoverIndex {
			n.CoverIndex, n.CoverRoutes = op.Cover, op.CoverRoutes
			raised = true
		}
		if !raised {
			return Result{Reason: "not a raise"}, nil
		}
		t.served = append(t.served, op.Node)
		return Result{OK: true}, nil
	case OpReassign, OpHandoff:
		if op.Kind == OpReassign {
			if n := t.nodes[op.Node]; n != nil && n.AliveIndex > op.SeenAliveIndex {
				// An alive record reached the log after the leader judged: not
				// dead. Positions in the log, not stamps, which two leaders
				// may have written on two clocks.
				return Result{Reason: ownerAlive}, nil
			}
		}
		if op.To == "" || op.To == op.Node {
			return Result{Reason: "no target"}, nil
		}
		if n := t.nodes[op.Node]; op.Kind == OpReassign && n != nil {
			// The leader's verdict, for every node to read. A drain's hand-off
			// declares nothing: the node it empties is alive.
			n.DeadAt = max(t.applying, 1)
			if !contains(t.declared, op.Node) {
				t.declared = append(t.declared, op.Node)
			}
		}
		if n := t.nodes[op.Node]; op.Kind == OpHandoff && n != nil {
			// A drain's hand-off is its word that the node is leaving.
			n.LeavingAt = max(t.applying, 1)
			if !contains(t.leaving, op.Node) {
				t.leaving = append(t.leaving, op.Node)
			}
		}
		var changes []change
		for _, l := range t.sortedLeases() {
			if l.Owner != op.Node {
				continue
			}
			before := clone(l)
			t.lost(op.Node)
			l.Owner = op.To
			l.Epoch = t.token(l.Epoch)
			if l.Online {
				// The client was connected when its owner died: treat the
				// death as its disconnect.
				l.Online = false
				l.DisconnectedAt = op.Now
				if l.ExpiryInterval != 0 && l.ExpiryInterval != ^uint32(0) {
					l.ExpiresAt = op.Now.Add(time.Duration(l.ExpiryInterval) * time.Second)
				}
				if l.Will != nil {
					l.Will.Armed = true
				}
			}
			changes = append(changes, change{before, clone(l)})
		}
		return Result{OK: true, Moved: len(changes)}, changes
	}
	return Result{Reason: "unknown operation"}, nil
}

// ownerAlive refuses a reassignment of, or a conditional acquire from, a node
// the log says is alive.
const ownerAlive = "owner alive"

// live reports whether the log says a node is alive: it has a record and the
// leader has not declared it dead since its last alive record. Callers hold
// mu.
func (t *Table) live(node string) bool {
	n := t.nodes[node]
	return n != nil && n.DeadAt == 0
}

// Live is the only reading of another node's liveness a node that is not the
// leader may act on. A node with no record is not alive: every caller asks
// before taking or discarding something, and uncertainty does neither.
func (t *Table) Live(node string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.live(node)
}

// NodeNames lists every node with a record, and every name in also, sorted.
func (t *Table) NodeNames(also map[string]bool) []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	seen := map[string]bool{}
	for name := range t.nodes {
		seen[name] = true
	}
	for name := range also {
		seen[name] = true
	}
	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// ServedFact is the highest index at which node has said its adoptions were
// finished, and the route mark it gave; zeros while it has said nothing.
func (t *Table) ServedFact(node string) (index, routes uint64) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if n := t.nodes[node]; n != nil {
		return n.ServedIndex, n.ServedRoutes
	}
	return 0, 0
}

// Cover is node's cover as this node's table holds it: the grant through
// which the sessions it took in transit are held, and its route mark then.
func (t *Table) Cover(node string) (grant, routes uint64) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if n := t.nodes[node]; n != nil {
		return n.CoverIndex, n.CoverRoutes
	}
	return 0, 0
}

// Leaving reports whether a hand-off has marked node leaving since its last
// alive record.
func (t *Table) Leaving(node string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	n := t.nodes[node]
	return n != nil && n.LeavingAt != 0
}

// LeavingNodes lists the nodes marked leaving, sorted.
func (t *Table) LeavingNodes() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var out []string
	for name, n := range t.nodes {
		if n.LeavingAt != 0 {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// DeclaredDead lists the nodes the leader has declared dead and that have
// not renewed since, sorted.
func (t *Table) DeclaredDead() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var out []string
	for name, n := range t.nodes {
		if n.DeadAt != 0 {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// doubleGrant refuses the second of two grants of one client to different
// nodes inside one command.
const doubleGrant = "granted to another node by the same command"

// token is a grant's epoch: the log index of the command that makes it, so
// it is the same on every node and never goes backwards — a lease released
// and acquired again is granted above anything it had before. One more than
// the previous epoch when that is higher, which happens only where Apply is
// driven without a log index.
func (t *Table) token(prev uint64) uint64 {
	if t.applying > prev {
		return t.applying
	}
	return prev + 1
}

// grantTo records that the command being applied grants the client to node,
// and reports false when it has already granted it to another.
func (t *Table) grantTo(k key, node string) bool {
	if prev, ok := t.granted[k]; ok && prev != node {
		return false
	}
	if t.granted == nil {
		t.granted = map[key]string{}
	}
	t.granted[k] = node
	return true
}

// each calls fn for every lease, with the read lock held; fn must not call
// back into the table.
func (t *Table) each(fn func(l *Lease)) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, l := range t.leases {
		fn(l)
	}
}

// lost records that the command being applied took a lease from node.
// Callers hold mu.
func (t *Table) lost(node string) {
	if t.applying > t.lostAt[node] {
		t.lostAt[node] = t.applying
	}
}

// LostAt is the log index of the last command that took a lease from node;
// zero when none has, or when the table was restored from a snapshot that
// predates the record.
func (t *Table) LostAt(node string) uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.lostAt[node]
}

func (t *Table) sortedLeases() []*Lease {
	out := make([]*Lease, 0, len(t.leases))
	for _, l := range t.leases {
		out = append(out, l)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].ClientID < out[j].ClientID
	})
	return out
}

// Level is the cluster's feature level, never below 1: a build that has
// never raised it, and a snapshot written before the level existed, are the
// baseline.
func (t *Table) Level() uint64 {
	if l := t.level.Load(); l != 0 {
		return l
	}
	return 1
}

// MinAdvertisedLevel is the lowest level any node record advertises, and
// whether every node named was found. A raise waits for this to reach the
// level it would raise to; a name with no record makes it unknowable, which
// is not the same as low.
func (t *Table) MinAdvertisedLevel(nodes []string) (uint64, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	min := uint64(0)
	for _, name := range nodes {
		n := t.nodes[name]
		if n == nil {
			return 0, false
		}
		l := n.Level
		if l == 0 {
			l = 1
		}
		if min == 0 || l < min {
			min = l
		}
	}
	return min, min > 0
}

// Snapshot implements consensus.Component.
func (t *Table) Snapshot() ([]byte, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	doc := struct {
		Leases []*Lease             `json:"leases"`
		Nodes  map[string]*NodeInfo `json:"nodes"`
		Level  uint64               `json:"level,omitempty"`
		LostAt map[string]uint64    `json:"lostAt,omitempty"`
	}{Leases: t.sortedLeases(), Nodes: t.nodes, Level: t.level.Load(), LostAt: t.lostAt}
	return json.Marshal(doc)
}

// Restore implements consensus.Component.
func (t *Table) Restore(data []byte) error {
	var doc struct {
		Leases []*Lease             `json:"leases"`
		Nodes  map[string]*NodeInfo `json:"nodes"`
		Level  uint64               `json:"level,omitempty"`
		LostAt map[string]uint64    `json:"lostAt,omitempty"`
	}
	if len(data) > 0 {
		if err := json.Unmarshal(data, &doc); err != nil {
			return err
		}
	}
	t.mu.Lock()
	t.leases = map[key]*Lease{}
	for _, l := range doc.Leases {
		t.leases[key{l.Namespace, l.ClientID}] = l
	}
	t.nodes = doc.Nodes
	if t.nodes == nil {
		t.nodes = map[string]*NodeInfo{}
	}
	t.level.Store(doc.Level)
	t.lostAt = doc.LostAt
	if t.lostAt == nil {
		t.lostAt = map[string]uint64{}
	}
	onRestore := t.onRestore
	t.mu.Unlock()
	if onRestore != nil {
		onRestore()
	}
	return nil
}

// Lookup returns a copy of a lease.
func (t *Table) Lookup(ns, id string) (Lease, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	l := t.leases[key{ns, id}]
	if l == nil {
		return Lease{}, false
	}
	return *l, true
}

// Valid reports whether node owns the lease at exactly this epoch.
func (t *Table) Valid(ns, id, node string, epoch uint64) bool {
	l, ok := t.Lookup(ns, id)
	return ok && l.Owner == node && l.Epoch == epoch
}

// Node returns what is known about a node.
func (t *Table) Node(name string) (NodeInfo, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	n := t.nodes[name]
	if n == nil {
		return NodeInfo{}, false
	}
	return *n, true
}

// OwnedBy lists the leases a node owns.
func (t *Table) OwnedBy(node string) []Lease {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var out []Lease
	for _, l := range t.sortedLeases() {
		if l.Owner == node {
			out = append(out, *l)
		}
	}
	return out
}

// CountOwnedBy is how many leases a node owns. It exists beside OwnedBy
// because the routing plane asks this question about every peer on a timer
// and OwnedBy sorts the whole table to answer it (WI 1351).
func (t *Table) CountOwnedBy(node string) int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	n := 0
	for _, l := range t.leases {
		if l.Owner == node {
			n++
		}
	}
	return n
}

// Owners lists every node that owns at least one lease.
func (t *Table) Owners() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	seen := map[string]bool{}
	for _, l := range t.leases {
		seen[l.Owner] = true
	}
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Count is the number of leases.
func (t *Table) Count() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.leases)
}
