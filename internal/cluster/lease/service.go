package lease

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/hook"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

// Sessions is what the service needs from the session manager.
type Sessions interface {
	Export(ns, id string) (*store.SessionState, bool)
	// Take exports and removes the session in one step: the hand-over before
	// the seal, kept for Options.RemoveOnHandOver.
	Take(ns, id string, reason packet.ReasonCode) (*store.SessionState, bool)
	// Seal hands a session to a takeover without deleting it: it leaves the
	// session map and the router, its connection is closed with reason, and
	// its state is kept sealed for token; nothing is journaled.
	Seal(ns, id string, reason packet.ReasonCode, token uint64) SealResult
	// Sealed reads a sealed copy and the token it is sealed for.
	Sealed(ns, id string) (*store.SessionState, uint64, bool)
	// DropSealed ends a seal for a token at or below upTo, journaling the
	// session's delete.
	DropSealed(ns, id string, upTo uint64) bool
	// EvictOffline evicts a session held at an epoch at or below upTo that
	// has no connection.
	EvictOffline(ns, id string, upTo uint64) bool
	// HeldAt reports the token a session held here is served at.
	HeldAt(ns, id string) (uint64, bool)
	// SealedCount is the number of sessions sealed here.
	SealedCount() int
	// Durable returns once the records written so far for the named sessions
	// are acknowledged by their replicas.
	Durable(ctx context.Context, keys []store.Scope) error
	// ImportIf installs a session state when admit, called under the
	// session's lock before anything changes, allows it, and reports whether
	// it did; a takeover pass's Seal waits for an admitted install to finish.
	ImportIf(st *store.SessionState, admit func() bool) bool
	Evict(ns, id string, reason packet.ReasonCode) bool
	// Detach closes the client's connection but keeps the state for a pull.
	Detach(ns, id string, reason packet.ReasonCode)
	SetEpoch(ns, id string, epoch uint64)
	// PublishWill publishes an adopted session's Will.
	PublishWill(w *store.WillRecord)
	// Each calls fn for every session this node holds, for the reconcile at
	// the end of recovery.
	Each(fn func(ns, id string))
	// FenceAll disconnects every client because this node's leases lapsed,
	// with a DISCONNECT reason: Use Another Server when a majority-side node
	// is known, otherwise Server Busy (0x88 is a CONNACK-only code).
	FenceAll(reason packet.ReasonCode)
	// ConnAge reports how long the session's live connection has been
	// accepted; false when the session has no live connection.
	ConnAge(ns, id string) (time.Duration, bool)
}

// Requester describes the connection asking for a lease, carried on a
// takeover request so the previous owner can attribute it (WI 1338): how
// long ago it was accepted, and whether its CONNECT was already buffered at
// accept — a socket that waited in the listen backlog.
type Requester struct {
	ConnAge     time.Duration
	Prebuffered bool
}

// Transport is what the service needs from the cluster transport; nil for a
// node with no peers.
type Transport interface {
	Send(ctx context.Context, peer string, c transport.Class, payload []byte) error
	Handle(c transport.Class, h transport.Handler)
	Peers() []transport.PeerInfo
}

// Options configure the service.
type Options struct {
	Node            string
	Core            consensus.Core
	Machine         *consensus.Machine
	Transport       Transport
	Sessions        Sessions
	TTL             time.Duration // default 30 s
	TakeoverTimeout time.Duration // default 5 s
	Advertise       string        // client address handed out in Server Reference
	// Fallback, when set, fetches a session's replica from the shard's
	// holders for a takeover the previous owner cannot serve, and for an
	// adopted offline lease; nil returns nothing. answered is false when a
	// node that was asked did not reply, or nobody could be asked yet: a nil
	// state is then no evidence that the session is held nowhere. down names
	// the replica holders that were not asked because they were down. Kept is
	// a copy a former holder kept, found while a source did not answer, and
	// KeptFrom that holder; an adoption that gives up serves it.
	Fallback func(ctx context.Context, ns, id, prevOwner string) Fetched
	// FallbackTimeout bounds each call to Fallback, on a context of its own:
	// a pull that went unanswered has spent the CONNECT's (default 2 s; the
	// broker passes cluster.replication.ackTimeoutMs, the fetch's own bound).
	FallbackTimeout time.Duration
	// BuildLevel overrides the cluster feature level this node advertises,
	// so a test can stand a node in for an older build. Zero outside tests.
	BuildLevel uint64
	// AdoptionDelay holds the adoption worker back before it claims an
	// adoption, so a harness can have a client's CONNECT reach the adopting
	// node first. Zero outside tests.
	AdoptionDelay time.Duration
	// TakeoverDelay holds each takeover pass back before it starts, so a
	// test can make this node a previous owner that answers too late. Zero
	// outside tests.
	TakeoverDelay time.Duration
	// ImportGate is called by the adoption worker between its fetch and its
	// import of the state it fetched, so a test can hold an import in that
	// window. Nil outside tests.
	ImportGate func(ns, id string)
	// ImportPastLaterGrant is an injected defect: a fetched session is
	// installed although a later grant for its client has been seen here.
	ImportPastLaterGrant bool
	// AdmitDuringAdoption is an injected defect: a CONNECT is admitted while
	// its lease's adoption is unfinished, so the import that follows evicts it.
	AdmitDuringAdoption bool
	// AdmitUnansweredFetch is an injected defect: a fetch that went unanswered
	// is acted on as if every source had answered "not found".
	AdmitUnansweredFetch bool
	// DiscardLateState is an injected defect: an answer carrying a session
	// that arrives after its pull gave up is dropped.
	DiscardLateState bool
	// RemoveOnHandOver is an injected defect: a takeover pass removes the
	// session and journals its delete at once, and the reclaim evicts, instead
	// of sealing — the hand-over before the seal. No confirmation is sent.
	RemoveOnHandOver bool
	// OmitPendingOwner stands this node in for a build that answers a takeover
	// pull without naming the owner it is waiting on, so a test can pair it
	// with a newer requester. False outside tests.
	OmitPendingOwner bool
	// AcquireGate is called by an acquire just before it proposes (granted
	// false) and just after a grant from another owner (granted true), so a
	// test can hold a takeover there. Nil outside tests.
	AcquireGate func(ns, id string, granted bool)
	// SealTimeout is how long a seal nothing confirms waits before its check,
	// and between checks (default twice TTL).
	SealTimeout time.Duration
	// HeldAtTargets asks owner's replica targets for a session whether one
	// holds it at token or a later grant, and returns the epoch it saw, or
	// zero; a seal's check calls it. Nil finds nothing, so a seal is kept until
	// its confirmation, its use here, or the lease's end.
	HeldAtTargets func(ctx context.Context, ns, id, owner string, token uint64) uint64
	// Unfed lists the nodes besides the previous owner that this node, as a
	// session's owner, sends its confirmation to: the other live members it
	// does not replicate the session to, any of which may keep a copy, a seal
	// or a drain's wait for it. Nil sends to the previous owner only.
	Unfed func(ns, id string) []string
	// Confirmed is told of every confirmation received: from holds the
	// session at token, durable at its targets. Nil outside a cluster.
	Confirmed func(ns, id, from string, token uint64)
	// NextPublishSeq reads the sequence the session manager's next publish
	// will take; every publish takes one before it is routed. A confirmation
	// carries it, so the previous owner can tell this node's forwards routed
	// after the session was held here from those routed before. Nil sends no
	// mark.
	NextPublishSeq func() uint64
	// MoveEnded is told, once this node has dropped or evicted its copy on a
	// confirmation, which node holds the client's session and the mark it
	// reported; the routing plane ends the client's move for the paths that
	// can prove delivery. Nil outside a cluster.
	MoveEnded func(ns, id, owner string, mark uint64)
	// Declared is told, on the apply goroutine, of every node a reassignment
	// this node applied declared dead. It must not block.
	Declared func(node string)
	// Leaving is told, on the apply goroutine, of every node a hand-off this
	// node applied marked leaving. It must not block.
	Leaving func(node string)
	// Served is told, on the apply goroutine, of every node whose served fact
	// this node applied, and AdoptionsIdle each time this node's last
	// unfinished adoption ends. Neither may block.
	Served        func(node string)
	AdoptionsIdle func()
	// TakesEnded is told when takeovers of sessions in transit end, held here
	// or at their bound: a cover may be due (CoverDue). It must not block.
	TakesEnded func()
	// AdoptRetries is how many times the adoption worker fetches again after
	// an unanswered fetch before it gives the adoption up (default 6), and
	// AdoptRetryDelay the wait before the first retry, doubling to
	// maxAdoptRetryDelay (default 500 ms).
	AdoptRetries    int
	AdoptRetryDelay time.Duration
	// MinorityServes is the simulation harness's injected defect: a fenced
	// node keeps granting leases locally instead of refusing. Never set from
	// configuration.
	MinorityServes bool
	Logger         *slog.Logger
	Metrics        hook.Metrics
	Audit          func(action, target, outcome string, detail map[string]string)
	Clock          func() time.Time
	// Steady is this node's own time for measuring how long ago it did or saw
	// something: its last renewal, when it became leader, when it last applied
	// a node's alive record. It must not step. Clock is the wall clock and can:
	// measured on it, a step forward of a ttl reads this node's own renewal as
	// lapsed, which disconnects its clients, and every other node as dead.
	// Defaults to Clock when Clock is set, so a test that moves one moves
	// both, and otherwise to the process's monotonic time.
	Steady func() time.Time
}

// Refusal is why a CONNECT cannot get a lease: the reason code for the
// CONNACK and, when a majority-side node is known, its client address.
type Refusal struct {
	Reason          packet.ReasonCode
	ServerReference string
}

func (r *Refusal) Error() string {
	if r.ServerReference != "" {
		return fmt.Sprintf("lease refused: %s (use %s)", r.Reason, r.ServerReference)
	}
	return "lease refused: " + r.Reason.String()
}

// AcquireResult is the outcome of a successful acquire.
type AcquireResult struct {
	Epoch     uint64
	Local     bool   // renewed on the same owner, no proposal awaited
	PrevOwner string // set on a takeover from another node
	// MayHaveCopies says a session for the client may have been written to
	// the replicas by a node other than this one, or by this one at a
	// previous grant, before this acquire.
	MayHaveCopies bool
	State         *store.SessionState
	// Empty has a reason when the client's lease existed before this acquire
	// and no state was obtained for it: if nothing is held here either, the
	// client is served a fresh session although the lease says it had one.
	Empty EmptyEvidence
}

// mayHaveCopies decides whether an earlier session of the client may be on
// the replicas: the lease was found under another owner, or with a session
// expiry (a session without one journals nothing), or the grant names
// another node as the previous owner, which a lagging table does not show.
// A lease of this node's with no expiry, as a liveness probe's, has none.
func mayHaveCopies(found bool, owner string, expiry uint32, prevOwner, self string) bool {
	return (found && (owner != self || expiry != 0)) || (prevOwner != "" && prevOwner != self)
}

// EmptyEvidence is why an acquire obtained no state for a lease that existed,
// with the lease's last-known owner and the token the client is served at.
type EmptyEvidence struct {
	Reason string
	Owner  string
	Token  uint64
}

// Why an acquire for an existing lease obtained no state. The set is closed:
// these are the values of the reason label.
const (
	// EmptyTakeover: taken over from another owner, which answered with
	// nothing or could not be asked, and every replica asked answered "not
	// found".
	EmptyTakeover = "takeover"
	// EmptyAdoption: this CONNECT fetched its lease's adoption and every
	// source answered "not found".
	EmptyAdoption = "adoption"
	// EmptyAdoptionEnded: the lease is this node's and offline, with no
	// adoption left to fetch — one ended without a state, or the session
	// expired and the lease is not yet released.
	EmptyAdoptionEnded = "adoption-ended"
	// EmptyGrantUnfetched: the lease is this node's and online, with no
	// adoption: an acquire granted it and the CONNECT that asked never got a
	// session here.
	EmptyGrantUnfetched = "grant-unfetched"
	// EmptyRaced: the lease became this node's while this CONNECT's acquire
	// was proposed, and there was no adoption to fetch.
	EmptyRaced = "raced"
	// EmptyNoExpiry: any of the above for a lease that, as this CONNECT looked
	// it up, carried no session expiry. Such a session ends with its
	// connection, so nothing of it is lost; it is counted apart.
	EmptyNoExpiry = "no-expiry"
)

// EmptyReasons lists every reason, for readers that total them.
var EmptyReasons = []string{EmptyTakeover, EmptyAdoption, EmptyAdoptionEnded, EmptyGrantUnfetched, EmptyRaced, EmptyNoExpiry}

// emptyReason is the reason a CONNECT served with nothing is counted under:
// found and expiry are what its lookup saw of the lease.
func emptyReason(reason string, found bool, expiry uint32) string {
	if reason != "" && found && expiry == 0 {
		return EmptyNoExpiry
	}
	return reason
}

// localEmpty is the evidence for a lease this node holds with nothing
// fetched for it: the table's online flag says whether an acquire granted it
// (a reassignment leaves the leases it moves offline).
func localEmpty(l Lease, self string) EmptyEvidence {
	reason := EmptyAdoptionEnded
	if l.Online {
		reason = EmptyGrantUnfetched
	}
	return EmptyEvidence{Reason: reason, Owner: self, Token: l.Epoch}
}

// Service is one node's lease service.
type Service struct {
	o     Options
	log   *slog.Logger
	table *Table
	// tokens is the highest grant token seen per client; see Tokens.
	tokens *Tokens

	mu       sync.Mutex
	pending  []request
	inflight bool
	wake     chan struct{}
	forwards map[uint64]chan forwardResult
	pulls    map[uint64]chan takeoverAck
	timers   map[key]*adopted
	nextReq  atomic.Uint64

	lastAlive atomic.Int64 // unix nanos of the last successful alive
	// contactIndex is the highest log index a successful renewal's alive
	// record applied at, and contactAt the steady time of that success.
	contactMu    sync.Mutex
	contactIndex uint64
	contactAt    time.Time
	fenced       atomic.Bool
	proposals    atomic.Uint64 // proposals made (for the batching test)

	stop   chan struct{}
	done   chan struct{}
	ctx    context.Context // cancelled by Stop, bounds every proposal and forward
	cancel context.CancelFunc

	// Bounded work in place of a goroutine per event (WI 1292 round 3):
	// takeover requests from peers, leases this node lost, adoptions.
	takeovers  *workQueue
	lost       *workQueue
	adopts     *workQueue
	adopting   *adoptionSet
	confirmed  *workQueue
	confirmOut confirmQueue
	// sealChecks runs the checks of seals nothing confirmed at their bound.
	sealChecks *workQueue
	// confirmDone closes when the confirmation loop has returned.
	confirmDone chan struct{}
	// transit counts the sessions of a lost node taken from their adopter
	// before its import (transit.go); transitDone closes when its loop has
	// returned.
	transit     *transitSet
	transitDone chan struct{}
	abandoned   *abandonedAcquires
	firsts      *firstAcquires
	waitNotes   *waitNotes
	race        claimRace

	// Liveness counters, lock-free, for the harness (WI 1292 round 4: a
	// full recovery that collapsed minutes later needed the lease service's
	// own timeline — fences, adoptions, lost leases, failed renewals).
	fences       atomic.Int64
	adoptions    atomic.Int64
	lostLeases   atomic.Int64
	aliveFails   atomic.Int64
	aliveMaxNs   atomic.Int64 // slowest successful renewal
	aliveTotalNs atomic.Int64
	aliveCount   atomic.Int64

	// Renewal path (WI 1337): renewals run on their own lane, several per
	// ttl, bounded in flight; the counters are the plan's observability.
	renewInflight atomic.Int32
	renewSkipped  atomic.Int64 // launches skipped for the in-flight bound
	renewFallback atomic.Int64 // renewals that took the batched path (Version 1 peer)
	renewStreak   atomic.Int64 // consecutive failures, reset on success
	renewWake     chan struct{}
	leaderSince   atomic.Int64 // unix nanos this node became leader; 0 when not leader
	// heard is, per node, when this node last applied its alive record, and
	// observingSince when it began watching, both on the steady clock.
	heard          sync.Map // string -> int64
	observingSince atomic.Int64
	timeline       livenessTimeline
	renewMu        sync.Mutex
	renewCur       *renewal     // the attempt concurrent alive() callers share
	renewFailedAt  atomic.Int64 // unix nanos of the last failed attempt, for the connect path's floor
	// Fence deferrals (WI 1341): a lapsed record does not fence while the
	// group has no leader and this node has a reachable quorum.
	fencesDeferred   atomic.Int64
	deferring        atomic.Bool  // a deferral streak is in progress (one warning per streak)
	leaderKnownSince atomic.Int64 // unix nanos since this node has continuously seen the same leader; 0 while leaderless
	leaderKnown      atomic.Value // string: the leader whose tenure leaderKnownSince measures
	leaderHint       atomic.Value // string: the leader the tick loop last saw, for refusals
	// levelPin is Options.BuildLevel, movable through SetBuildLevel; zero
	// advertises BuildLevel.
	levelPin   atomic.Uint64
	levelBelow atomic.Bool   // noteLevel has reported this node below the cluster's level
	raising    atomic.Uint64 // the level this node has proposed and not yet heard back about; 0 when none
	// Failure causes, so a collapse can be attributed: the attempt timed
	// out, no leader was known, the urgent lane was full, anything else.
	renewTimeouts atomic.Int64
	renewNoLeader atomic.Int64
	// renewUnreachable counts renewals that failed with a leader known and
	// unreachable, which renewNoLeader used to absorb (WI 1357).
	renewUnreachable atomic.Int64
	renewLaneFull    atomic.Int64
	renewOther       atomic.Int64
	// leaderlessSince and leaderlessMax measure how long this node goes
	// without a leader. A handful of renewal failures across a one-second
	// election and the same handful across a twenty-five-second leaderless
	// window are different diagnoses, and the count alone does not separate
	// them.
	leaderlessSince atomic.Int64
	leaderlessMax   atomic.Int64

	// Takeover attribution (WI 1338): requests for a key this node still
	// serves live, where the live connection is older than the requester's
	// (olderTakeovers), and of those, where the requester's CONNECT was
	// prebuffered — the signature of a backlog-delayed socket (stale).
	olderTakeovers atomic.Int64
	staleTakeovers atomic.Int64

	// What a takeover costs, split by whether this node still had the client
	// connected when it served one (WI 1495). The published batches can say
	// how many leases a node lost and how many connections the run dropped,
	// but not whether those are the same events: a lease move costs a live
	// connection only when the losing node still had one, and that ratio is
	// what separates a fast leader-crash recovery from a slow one.
	//
	// takeoversLive and takeoversGone partition takeoversServed. Within the
	// live ones, takeoversLiveAdopted counts those whose lease this node had
	// adopted during this outage rather than held all along — the case that
	// distinguishes an adoption racing a client from an unrelated re-home.
	// The other detach path (WI 1495). lostLease runs asynchronously after a
	// lease has gone and closes the client here; it fires once per lost lease
	// and is a different act from the one serveTakeover performs — a client
	// that arrived after the takeover was served is still connected when this
	// runs. A 1,000-client smoke run found every excess drop arriving through
	// a path serveTakeover's counters could not see, which is what this closes.
	lostDetachLive atomic.Int64
	lostDetachGone atomic.Int64

	// The deferred reclaim after a lost lease (WI 1513). evictDeferrals counts
	// times the eviction was held off because a client was connected here;
	// evictBoundHit counts evictions that went ahead with a client still here
	// because the bound ran out. Without the second, a fix that silently
	// deferred for ever would be indistinguishable from one that worked.
	evictDeferrals atomic.Int64
	evictBoundHit  atomic.Int64
	// Who settled each adoption: the worker, the client's own CONNECT, a clean
	// start that discarded it; and the CONNECTs that waited on one, or were
	// refused when the wait outlasted the handshake.
	adoptedByWorker atomic.Int64
	adoptedInline   atomic.Int64
	adoptDiscarded  atomic.Int64
	adoptWaited     atomic.Int64
	adoptRefused    atomic.Int64
	// adoptLate counts CONNECTs that found their adoption only after their
	// acquire applied: the reassignment overtook them while it was proposed.
	adoptLate atomic.Int64
	// What an adoption costs the handshake that settles it: time fetching the
	// state, and time waiting on a fetch already running.
	adoptInlineNs atomic.Int64
	adoptWaitNs   atomic.Int64
	// The part of adoptInlineNs spent asking the previous owner first, and
	// how many handshakes did; and the in-handshake fetches after which the
	// client is admitted with no state: adoptInlineEmpty because none came
	// back, adoptInlineMoved because the lease moved on during the fetch.
	adoptInlinePulls  atomic.Int64
	adoptInlinePullNs atomic.Int64
	adoptInlineEmpty  atomic.Int64
	adoptInlineMoved  atomic.Int64
	// How adoption and takeover fetches ended, by who made them; LivenessStats
	// says what each counts.
	adoptInlineUnanswered atomic.Int64
	adoptWorkerUnanswered atomic.Int64
	adoptRetries          atomic.Int64
	adoptGaveUp           atomic.Int64
	adoptWorkerEmpty      atomic.Int64
	adoptWorkerMoved      atomic.Int64
	takeoverUnanswered    atomic.Int64
	takeoverEmpty         atomic.Int64

	// Takeovers whose previous owner did not answer the pull, and of those the
	// ones the fallback served a copy.
	takeoverOwnerSilent       atomic.Int64
	takeoverOwnerSilentServed atomic.Int64
	// Previous owners that could not be asked and were not declared dead:
	// takeovers whose fallback ran with one, and of those the ones served a
	// copy; adoption fetches that found nothing and were refused for one; and
	// adoptions the worker gave up at the bound while still waiting on one.
	takeoverOwnerUndeclared       atomic.Int64
	takeoverOwnerUndeclaredServed atomic.Int64
	adoptOwnerUndeclared          atomic.Int64
	adoptOwnerSilent              atomic.Int64
	adoptWaitsPastBound           atomic.Int64
	adoptRedirected               atomic.Int64
	adoptGaveUpKept               atomic.Int64
	// Owners named in takeover answers: answers this node sent naming the
	// owner it was waiting on (of which from a note, and from a takeover's
	// note); takeovers here whose fallback ran with a named owner unaccounted
	// (of which served a copy); adoption fetches that took the wait over from
	// a named owner.
	answeredPending            atomic.Int64
	answeredPendingNoted       atomic.Int64
	answeredPendingTaking      atomic.Int64
	takeoverPendingOwner       atomic.Int64
	takeoverPendingOwnerServed atomic.Int64
	adoptPendingOwner          atomic.Int64
	// Replica holders the fetch skipped as down, not declared dead: takeovers
	// and adoption fetches refused for one. Pulls made on the control
	// connection alone.
	takeoverHolderUndeclared atomic.Int64
	adoptHolderUndeclared    atomic.Int64
	pullsControlOnly         atomic.Int64
	// Acquires whose CONNECT gave up while they were queued or proposed, and
	// the notes they left that a later CONNECT here turned into an adoption.
	acquiresAbandoned  atomic.Int64
	unfetchedAdopted   atomic.Int64
	firstsUnfetched    atomic.Int64
	firstsTakenAdopted atomic.Int64
	// servedEmpty counts, by reason, CONNECTs served a fresh session although
	// the lease showed one existed; keyed by every EmptyReasons value at New.
	servedEmpty map[string]*atomic.Int64
	// Takeover acknowledgements that arrived after their pull had given up,
	// and how many of them carried a session's state.
	orphanedAcks      atomic.Int64
	orphanedAckStates atomic.Int64
	// What became of the states those carried.
	lateStateUsed        atomic.Int64
	lateStateHandedOn    atomic.Int64
	lateStateSuperfluous atomic.Int64
	lateStateLeaseMoved  atomic.Int64
	// Sealed copies used again or sent again, and takeovers that found the
	// session still here; LivenessStats says what each counts.
	givenReused  atomic.Int64
	givenResent  atomic.Int64
	takeoverHeld atomic.Int64
	// The sealed hand-over: seals made, how they ended, and confirmations;
	// LivenessStats says what each counts.
	sealsMade          atomic.Int64
	sealsByReclaim     atomic.Int64
	sealsByRecovery    atomic.Int64
	sealsConfirmed     atomic.Int64
	sealsExpired       atomic.Int64
	sealsHeld          atomic.Int64
	sealsLeaseGone     atomic.Int64
	sealChecksHeld     atomic.Int64
	sealChecksNotHeld  atomic.Int64
	reclaimedOnConfirm atomic.Int64
	confirmsSent       atomic.Int64
	confirmsReceived   atomic.Int64
	confirmsIgnored    atomic.Int64
	confirmsUnheld     atomic.Int64
	confirmsUndurable  atomic.Int64
	confirmsUnsent     atomic.Int64
	// Confirmations sent to a shard's holders the owner does not replicate to.
	confirmsSentHolders atomic.Int64
	// Confirmations sent to a node that stopped being the session's target.
	confirmsSentMoved atomic.Int64
	// renewalsPaused stops this node proposing alive records: a drain sets it
	// before its hand-off, so the record that clears the leaving mark is the
	// next incarnation's.
	renewalsPaused atomic.Bool
	// Confirmations acted on, and their wait from receipt to the act's end.
	confirmsActed     atomic.Int64
	confirmActNsTotal atomic.Int64
	confirmActNsMax   atomic.Int64
	// Acquires refused because the owner is alive and unreachable: those the
	// log refused at apply, with the time they waited for it, and those this
	// node refused without proposing.
	ownerAliveApplied   atomic.Int64
	ownerAliveAppliedNs atomic.Int64
	ownerAliveLocal     atomic.Int64
	// The adoption worker's imports that found the client connected here,
	// which the import then closes: split by whether that connection was
	// accepted before the adoption was recorded or after it, and apart for
	// adoptions queued by recovery.
	importConnBefore    atomic.Int64
	importConnAfter     atomic.Int64
	importConnRecovered atomic.Int64
	importTrace         importTrace
	// Installs of a fetched session refused because a later grant for the
	// client had been seen: the adoption worker's, and a CONNECT's.
	adoptImportRefused   atomic.Int64
	connectImportRefused atomic.Int64

	// When this node first and last gained an offline lease it did not own —
	// the bulk reassignment after a crash landing — and when it first gained
	// an *online* one from another owner, which is a client acquiring its own
	// lease here. Unix milliseconds, so they compare across nodes; the three
	// nodes share a process in the measurement, which is what makes that
	// sound, and a cross-host version of this would need a different basis
	// (WI 1514).
	//
	// Zero means the event did not happen, which is a real outcome for a node
	// that adopts nothing.
	firstAdoptMs atomic.Int64
	lastAdoptMs  atomic.Int64
	// firstClientTakeMs is the first client takeover from *any* previous
	// owner, and firstTakeFrom the first from each owner separately.
	//
	// The aggregate alone cannot answer the question it was added for. A
	// client re-homing between the two survivors trips it, and that happens at
	// about the lease ttl in every run whatever the adoption regime — so it
	// measured which node won the election rather than when the crashed node's
	// leases started moving. The per-owner map is what lets the analysis ask
	// about the crashed node specifically (WI 1514).
	firstClientTakeMs atomic.Int64
	takeFromMu        sync.Mutex
	firstTakeFrom     map[string]int64

	// What a takeover pull waits for, both sides (WI 1691).
	takeoverTimes takeoverCounters

	takeoversServed    atomic.Int64
	takeoversLive      atomic.Int64
	takeoversGone      atomic.Int64
	takeoversLiveAdopt atomic.Int64
	detachedAgeTotalMs atomic.Int64
	detachedAgeMaxMs   atomic.Int64
	// The interval between this node adopting a lease and losing it to a
	// takeover (WI 1495, B3). If a client that arrives while the adoption is
	// still settling is sent elsewhere, these are short; if the re-home has
	// nothing to do with the adoption, they are not. No counter in the
	// published artifacts can tell those apart, which is why this exists.
	adoptTakeN     atomic.Int64
	adoptTakeSumMs atomic.Int64
	adoptTakeMaxMs atomic.Int64
	sampleMu       sync.Mutex
	adoptTakeSeen  []AdoptedTakeover
	adoptTakeDrop  int64

	// adoptedAt records when this node adopted a lease, so a later takeover
	// can report the interval.
	//
	// The entry is consumed by serveTakeover, not by changed(). The consensus
	// change that moves the lease applies *before* the takeover is served, so
	// deleting it in changed() removes the record a fraction before the only
	// reader wants it — which reports zero adopted takeovers in every run,
	// indistinguishable from a build where the case never arises. A test that
	// drives the takeover path caught it; no counter could have.
	//
	// What remains is one entry per lease this node adopted and has not since
	// lost to a served takeover or to the lease disappearing. A re-adoption
	// overwrites rather than adds, so a client that churns repeatedly costs
	// one entry, not one per cycle. The residue is leases handed off while
	// draining, which the drain ends anyway.
	//
	// It is not simply bounded by the lease table: an earlier comment here
	// said it was, and that was wrong.
	adoptedAt sync.Map // key -> time.Time

	// Fence evidence (WI 1474): which condition allowed each fence, the
	// decision records, and the tick loop's own timing.
	fencesLeaderSettled atomic.Int64
	fencesNoQuorum      atomic.Int64
	fenceRecords        atomic.Pointer[[]FenceRecord]
	fenceRecordsTotal   atomic.Int64
	loopLastNs          atomic.Int64 // unix nanos the tick loop last began an iteration
	loopGapNs           atomic.Int64 // the gap before the current iteration
	loopGapMaxNs        atomic.Int64
	statusReadNs        atomic.Int64 // the last Core.Status read in trackLeader
	statusReadMaxNs     atomic.Int64
}

// renewInflightMax bounds concurrent renewals: a slow attempt does not stop
// the next launch, and a wedged peer does not pile them up.
const renewInflightMax = 2

// renewWarnStreak is the consecutive-failure count that logs a warning,
// before the ttl runs out.
const renewWarnStreak = 3

// LivenessStats is the lease service's liveness picture, readable without
// any lock a storm contends for.
type LivenessStats struct {
	Fences       int64   `json:"fences"`
	Adoptions    int64   `json:"adoptions"`
	LostLeases   int64   `json:"lost_leases"`
	AliveFails   int64   `json:"alive_failures"`
	AliveMaxMs   float64 `json:"alive_max_ms"`
	AliveAvgMs   float64 `json:"alive_avg_ms"`
	AliveAgeMs   float64 `json:"alive_age_ms"` // since the last successful renewal
	Fenced       bool    `json:"fenced"`
	LeasesActive int     `json:"leases_active"`
	// Renewal path (WI 1337).
	RenewOK        int64   `json:"renew_ok"`
	RenewInflight  int32   `json:"renew_inflight"`
	RenewSkipped   int64   `json:"renew_skipped"`
	RenewFallback  int64   `json:"renew_fallback"`
	RenewStreak    int64   `json:"renew_fail_streak"`
	LeaderSinceMs  float64 `json:"leader_since_ms"` // 0 when not leader
	FencesDeferred int64   `json:"fences_deferred"` // lapsed records not fenced: no leader, quorum reachable
	RenewTimeouts  int64   `json:"renew_timeouts"`
	RenewNoLeader  int64   `json:"renew_no_leader"`
	// RenewUnreachable is renewals that failed with a leader known and
	// unreachable; LeaderlessMaxMs is the longest this node went with no
	// leader at all (WI 1357).
	RenewUnreachable int64   `json:"renew_leader_unreachable"`
	LeaderlessMaxMs  float64 `json:"leaderless_max_ms"`
	RenewLaneFull    int64   `json:"renew_lane_full"`
	RenewOther       int64   `json:"renew_other_failures"`
	StaleTakeovers   int64   `json:"stale_takeovers"` // WI 1338 attribution
	OlderTakeovers   int64   `json:"older_takeovers"` // live connection older than the requester's
	// WI 1495: what a takeover cost. Served partitions into Live and Gone;
	// LiveAdopted is the subset of Live whose lease this node had adopted.
	TakeoversServed    int64   `json:"takeovers_served"`
	TakeoversLive      int64   `json:"takeovers_live"`
	TakeoversGone      int64   `json:"takeovers_gone"`
	TakeoversLiveAdopt int64   `json:"takeovers_live_adopted"`
	DetachedAgeAvgMs   float64 `json:"detached_age_avg_ms"`
	DetachedAgeMaxMs   int64   `json:"detached_age_max_ms"`
	// How long an adopted lease stayed here before a takeover took it, and a
	// capped sample of the individual intervals (WI 1495).
	AdoptTakeovers   int64             `json:"adopt_takeovers"`
	AdoptTakeAvgMs   float64           `json:"adopt_takeover_avg_ms"`
	AdoptTakeMaxMs   int64             `json:"adopt_takeover_max_ms"`
	AdoptTakeSample  []AdoptedTakeover `json:"adopt_takeover_sample,omitempty"`
	AdoptTakeDropped int64             `json:"adopt_takeover_sample_dropped"`
	// The lost-lease detach, which runs after the lease has already gone and
	// is a separate cost from the takeover's own (WI 1495).
	LostDetachLive int64 `json:"lost_detach_live"`
	LostDetachGone int64 `json:"lost_detach_gone"`
	// The deferred reclaim (WI 1513): evictions held off because a client was
	// here, and evictions that went ahead anyway once the bound ran out.
	EvictDeferrals int64 `json:"evict_deferrals"`
	EvictBoundHit  int64 `json:"evict_bound_hit"`
	// How each adoption was settled, and what it cost a connecting client.
	AdoptedByWorker int64   `json:"adopted_by_worker"`
	AdoptedInline   int64   `json:"adopted_inline"`
	AdoptDiscarded  int64   `json:"adopt_discarded"`
	AdoptWaited     int64   `json:"adopt_waited"`
	AdoptRefused    int64   `json:"adopt_refused"`
	AdoptLate       int64   `json:"adopt_late"`
	AdoptInlineMs   float64 `json:"adopt_inline_ms_total"`
	AdoptWaitMs     float64 `json:"adopt_wait_ms_total"`
	// AdoptInlinePullMs is the part of AdoptInlineMs spent asking the
	// previous owner; AdoptInlineEmpty the in-handshake fetches that brought
	// no state back, and AdoptInlineMoved those whose lease moved on during
	// the fetch.
	AdoptInlinePulls  int64   `json:"adopt_inline_pulls"`
	AdoptInlinePullMs float64 `json:"adopt_inline_pull_ms_total"`
	AdoptInlineEmpty  int64   `json:"adopt_inline_empty"`
	AdoptInlineMoved  int64   `json:"adopt_inline_moved"`
	// Fetches that ended with a request unanswered and no state: in a
	// handshake (the CONNECT refused), by the adoption worker (retried; given
	// up when the retries are spent), and by a takeover's replica fetch (the
	// CONNECT refused and an adoption recorded). AdoptInlineEmpty,
	// AdoptWorkerEmpty and TakeoverEmpty are the answered ones: every node
	// asked said it holds nothing, and a fresh session is served.
	AdoptInlineUnanswered int64 `json:"adopt_inline_unanswered"`
	AdoptWorkerUnanswered int64 `json:"adopt_worker_unanswered"`
	AdoptRetries          int64 `json:"adopt_retries"`
	AdoptGaveUp           int64 `json:"adopt_gave_up"`
	// AdoptGaveUpKept counts the give-ups that served the copy a former
	// holder kept, found by the last fetch while a source did not answer;
	// AdoptGaveUp the rest, after which the client is served fresh.
	AdoptGaveUpKept    int64 `json:"adopt_gave_up_kept_copy"`
	AdoptWorkerEmpty   int64 `json:"adopt_worker_empty"`
	AdoptWorkerMoved   int64 `json:"adopt_worker_moved"`
	TakeoverUnanswered int64 `json:"takeover_unanswered"`
	TakeoverEmpty      int64 `json:"takeover_empty"`

	// TakeoverOwnerSilent counts takeovers whose previous owner was pulled and
	// did not answer, after which the replica fallback ran on its own budget;
	// TakeoverOwnerSilentServed those the fallback served a copy. The rest
	// are refused as unanswered: the owner's answer may still come.
	TakeoverOwnerSilent       int64 `json:"takeover_owner_silent"`
	TakeoverOwnerSilentServed int64 `json:"takeover_owner_silent_served"`
	// TakeoverOwnerUndeclared counts takeovers whose previous owner had no
	// live link from this node and was not declared dead when the fallback
	// ran; TakeoverOwnerUndeclaredServed those the fallback served a copy. The
	// rest are refused as unanswered until the declaration. AdoptOwnerUndeclared
	// counts adoption fetches, by any caller, refused for such an owner alone,
	// and AdoptOwnerSilent those refused for a previous owner alive by the log
	// that was asked and did not answer. Neither wait is given up.
	// AdoptWaitsPastBound counts the adoptions whose wait on such an owner, or
	// on a down replica holder alive by the log, outlasted SealTimeout, once
	// each, and AdoptWaitingNow those unfinished;
	// AdoptRedirected the CONNECTs refused for one of them with a redirect to
	// the leader.
	TakeoverOwnerUndeclared       int64 `json:"takeover_owner_undeclared"`
	TakeoverOwnerUndeclaredServed int64 `json:"takeover_owner_undeclared_served"`
	AdoptOwnerUndeclared          int64 `json:"adopt_owner_undeclared"`
	AdoptOwnerSilent              int64 `json:"adopt_owner_silent"`
	AdoptWaitsPastBound           int64 `json:"adopt_waits_past_bound"`
	AdoptWaitingNow               int   `json:"adopt_waiting_now"`
	AdoptRedirected               int64 `json:"adopt_redirected"`
	// TakeoverAnsweredPending counts takeover answers this node sent with no
	// state, naming the owner it was still waiting on (Noted: taken from a
	// note, left by an adoption that ended because the lease moved or by a
	// takeover of this node's own; Taking: the takeovers' notes;
	// PendingOwnerNotes are the notes held). TakeoverPendingOwner counts
	// takeovers here whose fallback ran with such a named owner unaccounted,
	// TakeoverPendingOwnerServed those served a copy; the rest are refused and
	// wait on that owner.
	// AdoptPendingOwner counts adoption fetches that took the wait over from a
	// named owner.
	TakeoverAnsweredPending       int64 `json:"takeover_answered_pending"`
	TakeoverAnsweredPendingNoted  int64 `json:"takeover_answered_pending_noted"`
	TakeoverAnsweredPendingTaking int64 `json:"takeover_answered_pending_taking"`
	TakeoverPendingOwner          int64 `json:"takeover_pending_owner"`
	TakeoverPendingOwnerServed    int64 `json:"takeover_pending_owner_served"`
	AdoptPendingOwner             int64 `json:"adopt_pending_owner"`
	PendingOwnerNotes             int   `json:"pending_owner_notes"`
	// TakeoverHolderUndeclared and AdoptHolderUndeclared count takeovers and
	// adoption fetches that found nothing and were refused because a replica
	// holder the fetch skipped as down is not declared dead; that wait is not
	// given up either. PullsControlOnly counts pulls made to a previous owner
	// reachable on its control connection alone.
	TakeoverHolderUndeclared int64 `json:"takeover_holder_undeclared"`
	AdoptHolderUndeclared    int64 `json:"adopt_holder_undeclared"`
	PullsControlOnly         int64 `json:"pulls_control_only"`
	// AcquiresAbandoned counts acquires whose CONNECT gave up while they could
	// still be granted; GrantsUnfetchedAdopted the ones a later CONNECT here
	// found granted with no session, and adopted and fetched before it was
	// served; UnfetchedNotes the notes held. FirstGrantsUnfetched counts the
	// CONNECTs here that found the lease a given-up first CONNECT's acquire
	// created, served with no evidence of a lost session;
	// FirstGrantsTakenAdopted the CONNECTs here that found it had taken the
	// lease from another node, and adopted and fetched before they were
	// served; FirstAcquireMarks the first acquires marked.
	AcquiresAbandoned       int64 `json:"acquires_abandoned"`
	GrantsUnfetchedAdopted  int64 `json:"grants_unfetched_adopted"`
	UnfetchedNotes          int   `json:"unfetched_notes"`
	FirstGrantsUnfetched    int64 `json:"first_grants_unfetched"`
	FirstGrantsTakenAdopted int64 `json:"first_grants_taken_adopted"`
	FirstAcquireMarks       int   `json:"first_acquire_marks"`
	// ServedEmpty counts, by reason, CONNECTs answered Session Present false
	// although the client's lease showed a session existed (NoteServedEmpty).
	ServedEmpty map[string]int64 `json:"served_empty,omitempty"`
	// OrphanedAcks are takeover acknowledgements that arrived after the pull
	// waiting for them had given up; OrphanedAckStates those that carried the
	// session's state, which the previous owner had dropped to send it.
	OrphanedAcks      int64 `json:"takeover_acks_orphaned"`
	OrphanedAckStates int64 `json:"takeover_acks_orphaned_with_state"`
	// What became of those states: used for the client here; handed on to the
	// node that took the lease next; dropped because this node already holds
	// a session for the client; dropped because the lease had moved on.
	LateStateUsed        int64 `json:"late_state_used"`
	LateStateHandedOn    int64 `json:"late_state_handed_on"`
	LateStateSuperfluous int64 `json:"late_state_superfluous"`
	LateStateLeaseMoved  int64 `json:"late_state_lease_moved"`
	// GivenReused counts sealed copies this node used when the lease came
	// back and no other node had the session; GivenResent those it sent again
	// to a requester asking a second time. TakeoverHeld counts takeovers that
	// found nothing elsewhere while this node still held the session, which
	// the client then resumes.
	GivenReused  int64 `json:"given_state_reused"`
	GivenResent  int64 `json:"given_state_resent"`
	TakeoverHeld int64 `json:"takeover_held"`
	// The sealed hand-over. Sealed: sessions sealed for a takeover, by a pass,
	// by the reclaim (SealedByReclaim) or at recovery (SealedByRecovery);
	// SealedNow: held sealed at the read.
	// SealsConfirmed ended on the new owner's confirmation, SealsExpired at a
	// bound's check without one: SealsHeld because a target of the lease's
	// owner held the session at its grant, SealsLeaseGone because the lease no
	// longer existed. SealChecksHeld and SealChecksNotHeld count those checks
	// by what they found. ReclaimedOnConfirm are never-sealed offline sessions
	// a confirmation evicted. Confirmations sent (to previous owners and to
	// holders), received from new ones, received and acted on nothing, and
	// owed but dropped: never held here at the token (Unheld), never durable
	// (Undurable), or not delivered (Unsent).
	Sealed             int64 `json:"sealed"`
	SealedByReclaim    int64 `json:"sealed_by_reclaim"`
	SealedByRecovery   int64 `json:"sealed_by_recovery"`
	SealedNow          int   `json:"sealed_now"`
	SealsConfirmed     int64 `json:"seals_confirmed"`
	SealsExpired       int64 `json:"seals_expired"`
	SealsHeld          int64 `json:"seals_held"`
	SealsLeaseGone     int64 `json:"seals_lease_gone"`
	SealChecksHeld     int64 `json:"seal_checks_held"`
	SealChecksNotHeld  int64 `json:"seal_checks_not_held"`
	ReclaimedOnConfirm int64 `json:"reclaimed_on_confirm"`
	ConfirmsSent       int64 `json:"confirms_sent"`
	ConfirmsReceived   int64 `json:"confirms_received"`
	ConfirmsIgnored    int64 `json:"confirms_ignored"`
	ConfirmsUnheld     int64 `json:"confirms_unheld"`
	ConfirmsUndurable  int64 `json:"confirms_undurable"`
	ConfirmsUnsent     int64 `json:"confirms_unsent"`
	// ConfirmsSentHolders counts, of ConfirmsSent, those sent to a node Unfed
	// names that is neither the previous owner nor a moved session's old
	// target.
	ConfirmsSentHolders int64 `json:"confirms_sent_holders"`
	// ConfirmsSentMoved counts, of ConfirmsSent, those sent to a node that
	// stopped being the session's replica target at a shard-map change.
	ConfirmsSentMoved int64 `json:"confirms_sent_moved,omitempty"`
	// ConfirmsActed are the confirmations that dropped a sealed copy or
	// evicted an offline session; ConfirmActMsTotal and ConfirmActMsMax are
	// their time from receipt on the transport's reader to the end of that act.
	ConfirmsActed     int64   `json:"confirms_acted"`
	ConfirmActMsTotal float64 `json:"confirm_act_ms_total"`
	ConfirmActMsMax   float64 `json:"confirm_act_ms_max"`
	// Acquires refused because the lease's owner is alive by the log and
	// unreachable from the requester: OwnerAliveApplied were proposed and
	// refused at apply, and OwnerAliveAppliedMs is the time those CONNECTs
	// waited for the proposal; OwnerAliveLocal were refused from this node's
	// own table with nothing proposed.
	OwnerAliveApplied   int64   `json:"owner_alive_refused_at_apply"`
	OwnerAliveAppliedMs float64 `json:"owner_alive_refused_at_apply_ms_total"`
	OwnerAliveLocal     int64   `json:"owner_alive_refused_locally"`
	// Worker imports that found the client connected: its connection
	// accepted before the adoption was recorded, after it, or the adoption
	// queued by recovery.
	ImportConnBefore    int64 `json:"import_conn_before_adoption"`
	ImportConnAfter     int64 `json:"import_conn_after_adoption"`
	ImportConnRecovered int64 `json:"import_conn_recovered"`
	// Installs of a fetched session refused because a later grant for the
	// client had been seen here: the adoption worker's (the lease moved during
	// its fetch or import), and a CONNECT's (answered Server Busy).
	AdoptImportRefused   int64 `json:"adopt_import_refused"`
	ConnectImportRefused int64 `json:"connect_import_refused"`
	// The race the adoption regime turns on (WI 1514): when the bulk
	// reassignment landed here, and when the first client took its own lease
	// from another owner. Unix ms; 0 means it did not happen.
	FirstAdoptMs int64 `json:"first_adopt_ms"`
	LastAdoptMs  int64 `json:"last_adopt_ms"`
	// AdoptionsIdleNs is when this node's last unfinished adoption last
	// ended, wall-clock nanoseconds; zero if none ever has.
	AdoptionsIdleNs   int64 `json:"adoptions_idle_ns,omitempty"`
	FirstClientTakeMs int64 `json:"first_client_takeover_ms"`
	// Per previous owner, so the analysis can ask about the crashed node
	// rather than about whichever client re-homed first.
	FirstTakeFrom map[string]int64 `json:"first_client_takeover_from_ms,omitempty"`
	// The same race counted rather than timed, per previous owner and as every
	// node sees it: leases its clients claimed, leases moved from it, and the
	// claims when the first was moved, with that instant in Unix ms.
	ClaimsFrom        map[string]int64 `json:"claims_from,omitempty"`
	MovedFrom         map[string]int64 `json:"moved_from,omitempty"`
	ClaimedAtReassign map[string]int64 `json:"claimed_at_reassign,omitempty"`
	ReassignAtMs      map[string]int64 `json:"reassign_at_ms,omitempty"`
	// Timeline is, per node name, when this node last applied its alive
	// record and when it proposed and applied its declaration.
	Timeline map[string]NodeLiveness `json:"timeline,omitempty"`
	FenceEvidence
	ImportTraceStats
	// Transit counts the sessions of a lost node taken from their adopter
	// before its import (WI 1833).
	Transit TransitStats `json:"transit"`
}

func (s *Service) LivenessStats() LivenessStats {
	st := LivenessStats{Fences: s.fences.Load(), Adoptions: s.adoptions.Load(), LostLeases: s.lostLeases.Load(),
		AliveFails: s.aliveFails.Load(), AliveMaxMs: float64(s.aliveMaxNs.Load()) / 1e6, Fenced: s.fenced.Load(),
		LeasesActive: s.table.Count(), RenewOK: s.aliveCount.Load(), RenewInflight: s.renewInflight.Load(),
		RenewSkipped: s.renewSkipped.Load(), RenewFallback: s.renewFallback.Load(), RenewStreak: s.renewStreak.Load(),
		StaleTakeovers: s.staleTakeovers.Load(), OlderTakeovers: s.olderTakeovers.Load(),
		TakeoversServed: s.takeoversServed.Load(), TakeoversLive: s.takeoversLive.Load(),
		TakeoversGone: s.takeoversGone.Load(), TakeoversLiveAdopt: s.takeoversLiveAdopt.Load(),
		DetachedAgeAvgMs:  avgMs(s.detachedAgeTotalMs.Load(), s.takeoversLive.Load()),
		DetachedAgeMaxMs:  s.detachedAgeMaxMs.Load(),
		AdoptTakeovers:    s.adoptTakeN.Load(),
		AdoptTakeAvgMs:    avgMs(s.adoptTakeSumMs.Load(), s.adoptTakeN.Load()),
		AdoptTakeMaxMs:    s.adoptTakeMaxMs.Load(),
		AdoptTakeSample:   s.adoptTakeoverSample(),
		AdoptTakeDropped:  s.adoptTakeoverDropped(),
		LostDetachLive:    s.lostDetachLive.Load(),
		LostDetachGone:    s.lostDetachGone.Load(),
		EvictDeferrals:    s.evictDeferrals.Load(),
		AdoptedByWorker:   s.adoptedByWorker.Load(),
		AdoptedInline:     s.adoptedInline.Load(),
		AdoptDiscarded:    s.adoptDiscarded.Load(),
		AdoptWaited:       s.adoptWaited.Load(),
		AdoptRefused:      s.adoptRefused.Load(),
		AdoptLate:         s.adoptLate.Load(),
		AdoptInlineMs:     float64(s.adoptInlineNs.Load()) / 1e6,
		AdoptWaitMs:       float64(s.adoptWaitNs.Load()) / 1e6,
		AdoptInlinePulls:  s.adoptInlinePulls.Load(),
		AdoptInlinePullMs: float64(s.adoptInlinePullNs.Load()) / 1e6,
		AdoptInlineEmpty:  s.adoptInlineEmpty.Load(),
		AdoptInlineMoved:  s.adoptInlineMoved.Load(),

		AdoptInlineUnanswered: s.adoptInlineUnanswered.Load(),
		AdoptWorkerUnanswered: s.adoptWorkerUnanswered.Load(),
		AdoptRetries:          s.adoptRetries.Load(),
		AdoptGaveUp:           s.adoptGaveUp.Load(),
		AdoptGaveUpKept:       s.adoptGaveUpKept.Load(),
		AdoptWorkerEmpty:      s.adoptWorkerEmpty.Load(),
		AdoptWorkerMoved:      s.adoptWorkerMoved.Load(),
		TakeoverUnanswered:    s.takeoverUnanswered.Load(),
		TakeoverEmpty:         s.takeoverEmpty.Load(),
		OrphanedAcks:          s.orphanedAcks.Load(),
		OrphanedAckStates:     s.orphanedAckStates.Load(),
		LateStateUsed:         s.lateStateUsed.Load(),
		LateStateHandedOn:     s.lateStateHandedOn.Load(),
		LateStateSuperfluous:  s.lateStateSuperfluous.Load(),
		LateStateLeaseMoved:   s.lateStateLeaseMoved.Load(),
		GivenReused:           s.givenReused.Load(),
		GivenResent:           s.givenResent.Load(),
		TakeoverHeld:          s.takeoverHeld.Load(),
		Sealed:                s.sealsMade.Load(),
		SealedByReclaim:       s.sealsByReclaim.Load(),
		SealedByRecovery:      s.sealsByRecovery.Load(),
		SealedNow:             s.sealedNow(),
		SealsConfirmed:        s.sealsConfirmed.Load(),
		SealsExpired:          s.sealsExpired.Load(),
		SealsHeld:             s.sealsHeld.Load(),
		SealsLeaseGone:        s.sealsLeaseGone.Load(),
		SealChecksHeld:        s.sealChecksHeld.Load(),
		SealChecksNotHeld:     s.sealChecksNotHeld.Load(),
		ReclaimedOnConfirm:    s.reclaimedOnConfirm.Load(),
		ConfirmsSent:          s.confirmsSent.Load(),
		ConfirmsSentHolders:   s.confirmsSentHolders.Load(),
		ConfirmsSentMoved:     s.confirmsSentMoved.Load(),
		ConfirmsActed:         s.confirmsActed.Load(),
		ConfirmActMsTotal:     float64(s.confirmActNsTotal.Load()) / 1e6,
		ConfirmActMsMax:       float64(s.confirmActNsMax.Load()) / 1e6,
		ConfirmsReceived:      s.confirmsReceived.Load(),
		ConfirmsIgnored:       s.confirmsIgnored.Load(),
		ConfirmsUnheld:        s.confirmsUnheld.Load(),
		ConfirmsUndurable:     s.confirmsUndurable.Load(),
		ConfirmsUnsent:        s.confirmsUnsent.Load(),
		OwnerAliveApplied:     s.ownerAliveApplied.Load(),
		OwnerAliveAppliedMs:   float64(s.ownerAliveAppliedNs.Load()) / float64(time.Millisecond),
		OwnerAliveLocal:       s.ownerAliveLocal.Load(),

		TakeoverOwnerSilent:       s.takeoverOwnerSilent.Load(),
		TakeoverOwnerSilentServed: s.takeoverOwnerSilentServed.Load(),
		AcquiresAbandoned:         s.acquiresAbandoned.Load(),
		GrantsUnfetchedAdopted:    s.unfetchedAdopted.Load(),
		UnfetchedNotes:            s.abandoned.len(),
		FirstGrantsUnfetched:      s.firstsUnfetched.Load(),
		FirstGrantsTakenAdopted:   s.firstsTakenAdopted.Load(),
		FirstAcquireMarks:         s.firsts.len(),
		ServedEmpty:               s.servedEmptyCounts(),

		TakeoverOwnerUndeclared:       s.takeoverOwnerUndeclared.Load(),
		TakeoverOwnerUndeclaredServed: s.takeoverOwnerUndeclaredServed.Load(),
		AdoptOwnerUndeclared:          s.adoptOwnerUndeclared.Load(),
		AdoptOwnerSilent:              s.adoptOwnerSilent.Load(),
		AdoptWaitsPastBound:           s.adoptWaitsPastBound.Load(),
		AdoptWaitingNow:               s.adopting.pastBound(),
		AdoptRedirected:               s.adoptRedirected.Load(),

		TakeoverAnsweredPending:       s.answeredPending.Load(),
		TakeoverAnsweredPendingNoted:  s.answeredPendingNoted.Load(),
		TakeoverAnsweredPendingTaking: s.answeredPendingTaking.Load(),
		TakeoverPendingOwner:          s.takeoverPendingOwner.Load(),
		TakeoverPendingOwnerServed:    s.takeoverPendingOwnerServed.Load(),
		AdoptPendingOwner:             s.adoptPendingOwner.Load(),
		PendingOwnerNotes:             s.waitNotes.len(),

		TakeoverHolderUndeclared: s.takeoverHolderUndeclared.Load(),
		AdoptHolderUndeclared:    s.adoptHolderUndeclared.Load(),
		PullsControlOnly:         s.pullsControlOnly.Load(),

		ImportConnBefore:     s.importConnBefore.Load(),
		ImportConnAfter:      s.importConnAfter.Load(),
		ImportConnRecovered:  s.importConnRecovered.Load(),
		AdoptImportRefused:   s.adoptImportRefused.Load(),
		ConnectImportRefused: s.connectImportRefused.Load(),
		EvictBoundHit:        s.evictBoundHit.Load(),
		FirstAdoptMs:         s.firstAdoptMs.Load(),
		LastAdoptMs:          s.lastAdoptMs.Load(),
		AdoptionsIdleNs:      s.adopting.idleAt(),
		FirstClientTakeMs:    s.firstClientTakeMs.Load(),
		FirstTakeFrom:        s.takeFromSnapshot(),
		RenewTimeouts:        s.renewTimeouts.Load(), RenewNoLeader: s.renewNoLeader.Load(), RenewLaneFull: s.renewLaneFull.Load(), RenewOther: s.renewOther.Load(),
		RenewUnreachable: s.renewUnreachable.Load(), LeaderlessMaxMs: float64(s.leaderlessWorst()) / float64(time.Millisecond),
		FencesDeferred: s.fencesDeferred.Load(), FenceEvidence: s.FenceEvidence(), ImportTraceStats: s.importTraceStats()}
	st.ClaimsFrom, st.MovedFrom, st.ClaimedAtReassign, st.ReassignAtMs = s.race.snapshot()
	st.Timeline = s.timeline.snapshot()
	st.Transit = s.TransitStats()
	if n := s.aliveCount.Load(); n > 0 {
		st.AliveAvgMs = float64(s.aliveTotalNs.Load()) / float64(n) / 1e6
	}
	now := s.steady()
	st.AliveAgeMs = float64(now.Sub(time.Unix(0, s.lastAlive.Load()))) / 1e6
	if since := s.leaderSince.Load(); since != 0 {
		st.LeaderSinceMs = float64(now.Sub(time.Unix(0, since))) / 1e6
	}
	return st
}

type request struct {
	op   Op
	done chan Result // nil for fire-and-forget
	err  chan error
}

type adopted struct {
	will, expiry *time.Timer
}

// Envelope kinds within the lease-notification class.
const (
	envForward       byte = 1
	envForwardResult byte = 2
	// envForwardCommand carries a command of another component's kind to the
	// leader, answered with envForwardResult carrying the component's result
	// as JSON.
	envForwardCommand byte = 3
)

type forwardMsg struct {
	Req uint64 `json:"req"`
	Ops []Op   `json:"ops"`
}

type forwardResult struct {
	Req     uint64          `json:"req"`
	Results []Result        `json:"results,omitempty"`
	Raw     json.RawMessage `json:"raw,omitempty"`
	Error   string          `json:"error,omitempty"`
}

type commandMsg struct {
	Req     uint64         `json:"req"`
	Kind    consensus.Kind `json:"kind"`
	Payload []byte         `json:"payload"`
}

// ErrNoLeader is returned when no lease decision can be made.
var ErrNoLeader = errors.New("lease: no leader reachable")

// ErrLeaderUnreachable is returned when a leader *is* known and this node
// could not send to it. It wraps ErrNoLeader deliberately: everything that
// tests for ErrNoLeader — including retryable, and therefore the renewal's
// retry — keeps working unchanged, while the two causes become countable
// apart. They were not, and 18 of the 20 renewal failures behind round 10b's
// one fence sat in the same bucket as a genuinely leaderless view while raft
// had a stable leader (WI 1357).
var ErrLeaderUnreachable = fmt.Errorf("%w: the leader is known but this node could not reach it", ErrNoLeader)

// New registers the lease component with the machine and prepares the
// service. Call before the core starts applying entries.
func New(o Options) (*Service, error) {
	if o.Node == "" || o.Core == nil || o.Machine == nil || o.Sessions == nil {
		return nil, errors.New("lease: node, core, machine and sessions are required")
	}
	if o.TTL == 0 {
		o.TTL = 30 * time.Second
	}
	if o.AdoptRetries == 0 {
		o.AdoptRetries = 6
	}
	if o.AdoptRetryDelay == 0 {
		o.AdoptRetryDelay = 500 * time.Millisecond
	}
	if o.TakeoverTimeout == 0 {
		o.TakeoverTimeout = 5 * time.Second
	}
	if o.FallbackTimeout == 0 {
		o.FallbackTimeout = 2 * time.Second
	}
	if o.SealTimeout == 0 {
		o.SealTimeout = 2 * o.TTL
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.Steady == nil {
		if o.Clock != nil {
			o.Steady = o.Clock
		} else {
			base := time.Now()
			o.Steady = func() time.Time { return base.Add(time.Since(base)) }
		}
	}
	if o.Clock == nil {
		o.Clock = time.Now
	}
	s := &Service{o: o, log: o.Logger.With("component", "lease"), table: NewTable(), wake: make(chan struct{}, 1), renewWake: make(chan struct{}, 1),
		forwards: map[uint64]chan forwardResult{}, pulls: map[uint64]chan takeoverAck{}, timers: map[key]*adopted{},
		stop: make(chan struct{}), done: make(chan struct{})}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.tokens = NewTokens()
	s.servedEmpty = map[string]*atomic.Int64{}
	for _, r := range EmptyReasons {
		s.servedEmpty[r] = new(atomic.Int64)
	}
	s.levelPin.Store(o.BuildLevel)
	// A takeover pass detaches the client and takes the session; the session
	// manager is sharded by client identifier and ends the pass with a
	// group-committed journal sync, so passes for different clients overlap
	// rather than serialize, and the worker count is what bounds the queue a
	// storm's takeovers wait in. At 100,000 clients that queue was 88-93% of
	// what a pull waited for, against 2-4 ms for the pass itself (WI 1691).
	// Lost-lease passes are on no handshake's path and keep their handful.
	s.takeovers = s.newQueue("takeover", takeoverWorkers, s.serveTakeover)
	s.lost = s.newQueue("lost-lease", 4, func(k key, _ []any) { s.lostLease(k) })
	s.adopts = s.newQueue("adopt", adoptWorkers, s.serveAdopt)
	s.adopting = newAdoptionSet()
	// A confirmation that acts waits for the journal's group fsync covering
	// its delete; that wait is nearly all of its pass, so the passes waiting
	// at once are how many a fsync releases. A node loss sends the adopter
	// them in a burst (WI 1834).
	s.confirmed = s.newQueue("confirmed", confirmWorkers, s.serveConfirmed)
	s.sealChecks = s.newQueue("seal-check", sealCheckWorkers, s.serveSealCheck)
	s.confirmOut.wake = make(chan struct{}, 1)
	s.confirmDone = make(chan struct{})
	s.transit = newTransitSet()
	s.transitDone = make(chan struct{})
	s.abandoned = newAbandonedAcquires()
	s.firsts = newFirstAcquires()
	s.waitNotes = newWaitNotes()
	// A key the queue gives up on is an adoption nobody will finish: its
	// waiters are released and it stops counting as unfinished.
	s.adopts.onDrop = func(k key) { s.adopting.abandon(k) }
	s.table.onChange = s.changed
	s.table.onRecovered = s.recovered
	s.table.onAlive = s.heardFrom
	s.table.onDeclared = s.declared
	s.table.onLeaving = func(node string) {
		if o.Leaving != nil {
			o.Leaving(node)
		}
	}
	s.table.onServed = func(node string) {
		s.wakeTransit()
		if o.Served != nil {
			o.Served(node)
		}
	}
	s.adopting.onIdle = o.AdoptionsIdle
	s.table.onRestore = s.observeAfresh
	o.Machine.Register(Kind, s.table)
	s.lastAlive.Store(o.Steady().UnixNano())
	s.observeAfresh()
	if o.Transport != nil {
		o.Transport.Handle(transport.LeaseNotification, s.receiveEnvelope(transport.LeaseNotification))
		o.Transport.Handle(transport.LeaseAlive, s.receiveEnvelope(transport.LeaseAlive))
		o.Transport.Handle(transport.TakeoverRequest, s.receiveTakeoverRequest)
		o.Transport.Handle(transport.TakeoverAck, s.receiveTakeoverAck)
	}
	return s, nil
}

// Table exposes the replicated lease table for reads.
func (s *Service) Table() *Table { return s.table }

// Tokens exposes the registry of grant tokens seen, for the replication
// layer's judgement of a record's writer.
func (s *Service) Tokens() *Tokens { return s.tokens }

// Start runs the flusher, the alive tick, the adoption check and the work
// queues.
func (s *Service) Start() {
	go s.flushLoop()
	go s.tickLoop()
	s.takeovers.start()
	s.lost.start()
	s.adopts.start()
	s.confirmed.start()
	s.sealChecks.start()
	go s.confirmLoop()
	go s.transitLoop()
}

// Workers per queue; see each queue's construction for what its count buys.
const (
	takeoverWorkers = 16
	adoptWorkers    = 16
	confirmWorkers  = 64
	// sealCheckWorkers matches the replicator's check slots, which bound what
	// is asked at once.
	sealCheckWorkers = 8
)

// newQueue builds a work queue whose counters flow into the metrics and
// whose passes log through the service.
func (s *Service) newQueue(name string, workers int, run func(key, []any)) *workQueue {
	q := newWorkQueue(name, workers, s.log, run)
	q.onEvent = func(outcome string) {
		s.count("hoardmq_lease_work_total", map[string]string{"queue": name, "outcome": outcome}, 1)
	}
	q.onDepth = func(depth int) {
		if s.o.Metrics != nil {
			s.o.Metrics.Set("hoardmq_lease_work_queued", map[string]string{"queue": name}, float64(depth))
		}
	}
	return q
}

// WorkStats reports the work queues' counters; it takes no lock the storm
// contends for, so a harness can read it mid-storm.
func (s *Service) WorkStats() []WorkStats {
	return []WorkStats{s.takeovers.stats(), s.lost.stats(), s.adopts.stats(), s.confirmed.stats(), s.sealChecks.stats()}
}

// Stop ends the loops and cancels adoption timers.
func (s *Service) Stop() {
	select {
	case <-s.stop:
		return
	default:
	}
	close(s.stop)
	s.cancel()
	<-s.done
	s.takeovers.stopAndWait()
	s.lost.stopAndWait()
	s.adopts.stopAndWait()
	s.confirmed.stopAndWait()
	s.sealChecks.stopAndWait()
	<-s.confirmDone
	<-s.transitDone
	s.mu.Lock()
	for k, a := range s.timers {
		a.cancel()
		delete(s.timers, k)
	}
	s.mu.Unlock()
}

func (a *adopted) cancel() {
	if a.will != nil {
		a.will.Stop()
	}
	if a.expiry != nil {
		a.expiry.Stop()
	}
}

// Proposals counts the lease proposals this node made or forwarded.
func (s *Service) Proposals() uint64 { return s.proposals.Load() }

// Fenced reports whether this node's leases have lapsed.
func (s *Service) Fenced() bool { return s.fenced.Load() }

// Valid reports whether node owns the lease at exactly this epoch.
func (s *Service) Valid(ns, id, node string, epoch uint64) bool {
	return s.table.Valid(ns, id, node, epoch)
}

// Epoch returns a lease's current epoch.
func (s *Service) Epoch(ns, id string) (uint64, bool) {
	l, ok := s.table.Lookup(ns, id)
	return l.Epoch, ok
}

// Acquire takes the lease for a connecting client. The same owner renews
// locally; another owner is taken over and, unless cleanStart, asked for
// the session state.
func (s *Service) Acquire(ctx context.Context, ns, id string, will *store.WillRecord, expiryInterval uint32, cleanStart bool) (AcquireResult, error) {
	return s.AcquireFrom(ctx, ns, id, will, expiryInterval, cleanStart, Requester{})
}

// AcquireFrom is Acquire with the requesting connection described, for the
// previous owner's takeover attribution.
func (s *Service) AcquireFrom(ctx context.Context, ns, id string, will *store.WillRecord, expiryInterval uint32, cleanStart bool, from Requester) (AcquireResult, error) {
	if s.fenced.Load() && s.o.MinorityServes {
		// Defects: grant without the core, the way a broker with no fence
		// would; the harness's one-live-owner property must catch it.
		if l, ok := s.table.Lookup(ns, id); ok {
			return AcquireResult{Epoch: l.Epoch, Local: true}, nil
		}
		return AcquireResult{Epoch: 1, Local: true}, nil
	}
	recovering, below := s.o.Core.Recovering(), s.BelowLevel()
	switch admission(recovering, below, s.fenced.Load()) {
	case admitRefuse:
		outcome := "recovering"
		if !recovering {
			outcome = "level"
			s.count("hoardmq_cluster_feature_level_refusals_total", map[string]string{"where": "connect"}, 1)
		}
		s.count("hoardmq_lease_ops_total", map[string]string{"op": "acquire", "outcome": outcome}, 1)
		return AcquireResult{}, s.refusal()
	case admitRenewFirst:
		// After a partition heals the next connect should not wait for the
		// tick.
		s.alive()
		if s.fenced.Load() {
			return AcquireResult{}, s.refusal()
		}
	}
	op := Op{Kind: OpAcquire, Namespace: ns, ClientID: id, Node: s.o.Node, Will: will, ExpiryInterval: expiryInterval, Now: s.o.Clock()}
	l, existed := s.table.Lookup(ns, id)
	if existed {
		if l.Owner == s.o.Node {
			st, ev, err := s.awaitAdoption(ctx, key{ns, id}, cleanStart)
			var mark firstAcquire
			if err == nil {
				mark, _ = s.firsts.take(key{ns, id})
				first := takenFrom(mark.abandoned, mark.created, mark.taken, l.Epoch, mark.from, "", s.o.Node)
				st, ev, err = s.fetchAbandoned(ctx, key{ns, id}, l.Epoch, cleanStart, st, ev, first)
			}
			if err != nil {
				return AcquireResult{}, err
			}
			if cur, ok := s.table.Lookup(ns, id); ok && cur.Owner == s.o.Node {
				// Renew locally; the table catches up in the background.
				renew := op
				renew.Epoch = cur.Epoch
				s.submitAsync(renew)
				s.count("hoardmq_lease_ops_total", map[string]string{"op": "acquire", "outcome": "local"}, 1)
				out := AcquireResult{Epoch: cur.Epoch, Local: true, MayHaveCopies: mayHaveCopies(true, l.Owner, l.ExpiryInterval, "", s.o.Node), State: st}
				first := createdHere(mark.abandoned, mark.created, cur.Epoch)
				if st == nil && !cleanStart {
					if ev.Reason == "" && first {
						s.firstsUnfetched.Add(1)
						s.log.Debug("lease created here by a first acquire whose CONNECT gave up; nothing to fetch", "client", id, "token", cur.Epoch)
					} else if ev.Reason == "" {
						ev = localEmpty(cur, s.o.Node)
					}
					ev.Reason = emptyReason(ev.Reason, true, l.ExpiryInterval)
					out.Empty = ev
				}
				return out, nil
			}
			// The lease left this node while the adoption was awaited: acquire
			// it as from any other owner.
		}
		// Another node holds the lease, as far as this one knows. Whether
		// that owner is alive is judged where the acquire applies, from the
		// log, against whichever node owns the lease then; this node says
		// whom it could not ask for the session, and proposes nothing when
		// its own table already gives the log's refusal. A CONNECT that finds
		// no lease reads no peer state.
		op.Unreached = s.unreached()
		if refuseUnreachableOwner(l.Owner, s.o.Node, s.table.Live(l.Owner), op.Unreached) {
			s.count("hoardmq_lease_ops_total", map[string]string{"op": "acquire", "outcome": "owner-unreachable-local"}, 1)
			s.ownerAliveLocal.Add(1)
			return AcquireResult{}, s.refusal()
		}
	}
	// Noted before the proposal: a peer can apply the grant and pull this
	// node before this node has applied it.
	if existed && !cleanStart {
		s.noteTaking(key{ns, id}, l.Owner)
	}
	if !existed {
		s.firsts.begin(key{ns, id}, s.steady())
	}
	if g := s.o.AcquireGate; g != nil {
		g(ns, id, false)
	}
	proposed := time.Now()
	res, err := s.submit(ctx, op)
	if !existed && err == nil {
		s.firsts.end(key{ns, id})
	}
	if err != nil {
		// The acquire may still be proposed and granted.
		if existed {
			s.noteAbandoned(key{ns, id}, l.Owner)
		} else {
			s.abandonFirst(key{ns, id})
		}
		s.log.Warn("lease acquire failed", "client", id, "err", err)
		s.count("hoardmq_lease_ops_total", map[string]string{"op": "acquire", "outcome": "refused"}, 1)
		return AcquireResult{}, s.refusal()
	}
	if !res.OK && res.Reason == ownerAlive {
		// The owner may still be serving the client and cannot be told:
		// taking the lease now could give the client two live sessions. It is
		// refused until the leader declares the owner dead or the link returns.
		s.count("hoardmq_lease_ops_total", map[string]string{"op": "acquire", "outcome": "owner-unreachable"}, 1)
		s.ownerAliveApplied.Add(1)
		s.ownerAliveAppliedNs.Add(int64(time.Since(proposed)))
		return AcquireResult{}, s.refusal()
	}
	if !res.OK {
		return AcquireResult{}, &Refusal{Reason: packet.UnspecifiedError}
	}
	// This node is told its token here, and may act on it, before its own
	// table has applied the grant.
	s.tokens.Note(ns, id, res.Epoch)
	out := AcquireResult{Epoch: res.Epoch, PrevOwner: res.PrevOwner, MayHaveCopies: mayHaveCopies(existed, l.Owner, l.ExpiryInterval, res.PrevOwner, s.o.Node)}
	if res.PrevOwner == "" || res.PrevOwner == s.o.Node {
		// The lease was already this node's when the acquire applied. It may
		// have become so while the acquire was proposed: a reassignment that
		// applied first adopted it, and queued an adoption this CONNECT has
		// not consulted, since it found another owner when it looked.
		before := s.adopting.lateFor(key{ns, id})
		st, ev, err := s.awaitAdoption(ctx, key{ns, id}, cleanStart)
		if err == nil {
			seen := ""
			if existed {
				seen = l.Owner
			}
			mark, _ := s.firsts.take(key{ns, id})
			first := takenFrom(mark.abandoned, mark.created, mark.taken, res.Epoch, mark.from, seen, s.o.Node)
			st, ev, err = s.fetchAbandoned(ctx, key{ns, id}, res.Epoch, cleanStart, st, ev, first)
		}
		if err != nil {
			return AcquireResult{}, err
		}
		if before {
			s.adoptLate.Add(1)
		}
		out.State = st
		if st == nil && !cleanStart && existed {
			if ev.Reason == "" {
				ev = EmptyEvidence{Reason: EmptyRaced, Owner: l.Owner, Token: res.Epoch}
			}
			ev.Reason = emptyReason(ev.Reason, existed, l.ExpiryInterval)
			out.Empty = ev
		}
	}
	if res.PrevOwner != "" && res.PrevOwner != s.o.Node {
		if g := s.o.AcquireGate; g != nil {
			g(ns, id, true)
		}
		if !cleanStart {
			s.noteTaking(key{ns, id}, res.PrevOwner)
		}
		kind := "lapsed"
		alive := res.PrevAlive
		if alive {
			kind = "live"
		}
		s.log.Debug("lease taken over", "client", id, "from", res.PrevOwner, "ownerAlive", alive, "cleanStart", cleanStart)
		// This CONNECT fetches for its own grant; an earlier abandoned acquire's
		// note or mark has nothing left to do.
		s.abandoned.take(key{ns, id})
		s.firsts.end(key{ns, id})
		// silent is a pull the previous owner did not answer. Its answer may
		// still come, carrying the session it removed to send it, so finding
		// nothing elsewhere does not make the session held nowhere.
		silent := false
		// The previous owner is asked for the session whenever it can be
		// reached, whatever the verdict on it. Rebuilding the session from a
		// replica that is behind, while an owner that was wrongly judged kept
		// enqueueing into a session it no longer owns, loses every message in
		// between.
		asked := !cleanStart && s.canAsk(res.PrevOwner)
		// pending is the owner the previous owner was itself still waiting on,
		// named in an answer that carried no state.
		pending := ""
		if asked {
			var answered, superseded bool
			out.State, answered, superseded, pending = s.pull(ctx, res.PrevOwner, ns, id, res.Epoch, from)
			silent = !answered
			if out.State == nil && pending != "" && !superseded {
				// The previous owner was still adopting the session: until it is
				// held here, nobody advertises its routes.
				s.noteTaken(key{ns, id}, res.Epoch)
			}
			if superseded {
				// The previous owner has seen a grant above this one: the
				// lease has already moved on, and the session went, or will
				// go, to whoever holds that token.
				s.count("hoardmq_lease_stale_token_total", map[string]string{"where": "acquire"}, 1)
				s.log.Info("takeover superseded by a later grant", "client", id, "token", res.Epoch, "from", res.PrevOwner)
				return AcquireResult{}, s.refusal()
			}
			if out.State == nil && !answered && alive {
				// It could be reached but did not answer: it may still be
				// serving the client until its lease lapses. An owner that
				// answered with no session is holding nothing back, and the
				// wait would only spend the connect budget the replica
				// fallback below needs.
				s.waitLapse(ctx, res.PrevOwner)
			}
		}
		if out.State == nil && !cleanStart && s.o.Fallback != nil {
			// The previous owner is gone or silent: a follower's replica
			// carries the queue and the subscriptions.
			f := s.fallback(ns, id, res.PrevOwner)
			st, answered, down := f.State, f.Answered, f.Down
			answered = answered && !silent
			held := false
			if st == nil {
				_, held = s.o.Sessions.Export(ns, id)
			}
			source := "replica"
			if st == nil && !held {
				// The lease is back and nobody has the session: this node's own
				// sealed copy, if it gave the session away. Importing it ends the
				// seal; so does any other state imported here, or a clean start.
				if sealed, _, ok := s.o.Sessions.Sealed(ns, id); ok {
					st, source = sealed, "given"
					s.givenReused.Add(1)
				}
			}
			if silent {
				s.takeoverOwnerSilent.Add(1)
			}
			// Declared dead at apply, or in this node's table since, is declared.
			unaccounted := ownerUnaccounted(res.PrevOwner, s.o.Node, asked, alive && !asked && s.table.Live(res.PrevOwner))
			if unaccounted {
				s.takeoverOwnerUndeclared.Add(1)
			}
			// An owner the previous owner named was not asked by this node.
			waitOn := res.PrevOwner
			pendingUnaccounted := ownerUnaccounted(pending, s.o.Node, false, pending != "" && s.table.Live(pending))
			if pendingUnaccounted {
				s.takeoverPendingOwner.Add(1)
				waitOn = pending
			}
			// A replica holder the fetch skipped as down may come back with its
			// copy until it is declared dead.
			holder := ""
			if st == nil && !held {
				holder = s.undeclaredHolder(down)
			}
			if holder != "" {
				s.takeoverHolderUndeclared.Add(1)
			}
			answered = heldNowhere(answered, unaccounted || pendingUnaccounted || holder != "")
			switch {
			case held:
				// Not given up yet, and the lease is back before the deferred
				// reclaim: the client resumes what is here.
				s.takeoverHeld.Add(1)
			case st != nil:
				out.State = st
				s.count("hoardmq_lease_takeovers_total", map[string]string{"kind": source}, 1)
				if silent {
					s.takeoverOwnerSilentServed.Add(1)
				}
				if unaccounted {
					s.takeoverOwnerUndeclaredServed.Add(1)
				}
				if pendingUnaccounted {
					s.takeoverPendingOwnerServed.Add(1)
				}
				if silent || unaccounted || pendingUnaccounted {
					s.log.Debug("takeover served without the previous owner's answer", "client", id, "from", res.PrevOwner, "pending", pending, "source", source, "token", res.Epoch)
				}
			case !answered && !s.o.AdmitUnansweredFetch:
				// Nobody answered, which does not say the session is held
				// nowhere. The lease is this node's now, so the client's retry
				// arrives as the same owner: the adoption recorded here is what
				// makes that retry fetch again instead of being served with
				// nothing.
				s.takeoverUnanswered.Add(1)
				s.log.Debug("takeover's replica fetch went unanswered", "client", id, "from", res.PrevOwner, "ownerAlive", alive, "ownerUndeclared", unaccounted, "pending", pending, "holder", holder, "token", res.Epoch)
				s.count("hoardmq_lease_ops_total", map[string]string{"op": "acquire", "outcome": "fetch-unanswered"}, 1)
				k, job := key{ns, id}, adoptJob{epoch: res.Epoch, prev: waitOn, takeover: true}
				s.adopting.begin(k, job)
				s.retryAdoption(k, job, 0)
				return AcquireResult{}, &Refusal{Reason: packet.ServerBusy}
			case !answered:
				s.takeoverUnanswered.Add(1)
			default:
				s.takeoverEmpty.Add(1)
				s.log.Debug("takeover found no state anywhere", "client", id, "from", res.PrevOwner, "ownerAlive", alive, "token", res.Epoch)
			}
			if out.State == nil && !held {
				out.Empty = EmptyEvidence{Reason: emptyReason(EmptyTakeover, existed, l.ExpiryInterval), Owner: res.PrevOwner, Token: res.Epoch}
			}
		}
		s.count("hoardmq_lease_takeovers_total", map[string]string{"kind": kind}, 1)
		s.audit("lease.takeover", ns+"/"+id, kind, map[string]string{"from": res.PrevOwner, "epoch": fmt.Sprint(res.Epoch)})
		s.confirmLater(key{ns, id}, res.Epoch, res.PrevOwner)
	}
	s.count("hoardmq_lease_ops_total", map[string]string{"op": "acquire", "outcome": "ok"}, 1)
	return out, nil
}

// Handoff moves every lease this node owns to another node, for a drain:
// the receiver adopts them (Will and expiry timers, replica import). It
// returns the number moved.
func (s *Service) Handoff(ctx context.Context, to string) (int, error) {
	res, err := s.submit(ctx, Op{Kind: OpHandoff, Node: s.o.Node, To: to, Now: s.o.Clock()})
	if err != nil {
		return 0, err
	}
	if !res.OK {
		return 0, fmt.Errorf("%w: %s", ErrHandoffRefused, res.Reason)
	}
	s.count("hoardmq_lease_ops_total", map[string]string{"op": "handoff", "outcome": "ok"}, 1)
	s.audit("lease.handoff", s.o.Node, "ok", map[string]string{"to": to, "moved": fmt.Sprint(res.Moved)})
	return res.Moved, nil
}

// ErrHandoffRefused wraps the lease table's refusal of a hand-off, which a
// second proposal of the same hand-off would meet again.
var ErrHandoffRefused = errors.New("lease: hand-off refused")

// PauseRenewals stops this node proposing alive records, for a drain about to
// hand off: its next alive record would clear the leaving mark the hand-off
// sets. While paused the node does not fence: a drain pauses them after it has
// closed its listeners and redirected its clients, so there is nobody to
// disconnect. It waits up to wait for the renewals already in flight and
// returns how many were still running.
func (s *Service) PauseRenewals(wait time.Duration) int {
	s.renewalsPaused.Store(true)
	deadline := time.Now().Add(wait)
	for {
		n := int(s.renewInflight.Load())
		if n == 0 || !time.Now().Before(deadline) {
			return n
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// ResumeRenewals undoes PauseRenewals, for a drain whose hand-off did not
// happen.
func (s *Service) ResumeRenewals() {
	if s.renewalsPaused.Swap(false) {
		select {
		case s.renewWake <- struct{}{}:
		default:
		}
	}
}

// Disconnected records that a session went offline with an expiry.
func (s *Service) Disconnected(ns, id string, epoch uint64, expiresAt time.Time, willArmed bool) {
	s.submitAsync(Op{Kind: OpDisconnected, Namespace: ns, ClientID: id, Node: s.o.Node, Epoch: epoch, ExpiresAt: expiresAt, WillArmed: willArmed, Now: s.o.Clock()})
}

// Release drops a lease whose session ended.
func (s *Service) Release(ns, id string, epoch uint64) {
	s.submitAsync(Op{Kind: OpRelease, Namespace: ns, ClientID: id, Node: s.o.Node, Epoch: epoch, Now: s.o.Clock()})
}

// refusal builds the minority-side answer: a redirect when a leader's client
// address is known, otherwise unavailable.
func (s *Service) refusal() error {
	if r := s.leaderRedirect(); r != nil {
		s.count("hoardmq_connect_refusals_total", map[string]string{"reason": "redirect"}, 1)
		return r
	}
	s.count("hoardmq_connect_refusals_total", map[string]string{"reason": "unavailable"}, 1)
	return &Refusal{Reason: packet.ServerUnavailable}
}

// leaderRedirect is Use Another Server naming the leader the tick loop last
// saw, or nil when that is this node, nobody, or a node with no advertised
// address.
func (s *Service) leaderRedirect() *Refusal {
	name, _ := s.leaderHint.Load().(string)
	if name != "" && name != s.o.Node {
		if n, ok := s.table.Node(name); ok && n.Advertise != "" {
			return &Refusal{Reason: packet.UseAnotherServer, ServerReference: n.Advertise}
		}
	}
	return nil
}

// notePastBound reports whether an adoption is waiting on a node alive by
// the log past SealTimeout, and the first time it is, counts and logs the
// wait.
func (s *Service) notePastBound(k key, ad *adoption, wait waitOn) bool {
	age := time.Since(ad.begun)
	if !waitPastBound(wait.kind, age, s.o.SealTimeout) {
		return false
	}
	if s.adopting.markPast(ad) {
		on := "owner"
		switch wait.kind {
		case waitOwnerSilent:
			on = "silent-owner"
		case waitHolder:
			on = "holder"
		}
		s.adoptWaitsPastBound.Add(1)
		s.count("hoardmq_lease_adoption_waits_total", map[string]string{"on": on}, 1)
		s.log.Warn("adoption still waiting on a node alive by the log", "client", k.id, "waitingOn", wait.node, "on", on, "from", ad.job.prev, "token", ad.job.epoch, "age", age.Round(time.Millisecond))
	}
	return true
}

type admissionVerdict int

const (
	admitProceed    admissionVerdict = iota
	admitRenewFirst                  // fenced: try to renew, refuse if still fenced
	admitRefuse
)

// admission is what a CONNECT meets before the lease table is read. A node
// that restarted holds the table and the sessions it had when it stopped
// until recovery ends: the table may name it owner of a lease the cluster has
// since given away, and admitting on that would serve an old copy of the
// session and, by the renewal that follows, take the lease back with no state
// pulled from the node that holds the current one.
//
// A node whose build is below the cluster's feature level admits nobody: the
// cluster has switched to semantics this build would apply differently.
func admission(recovering, belowLevel, fenced bool) admissionVerdict {
	switch {
	case recovering, belowLevel:
		return admitRefuse
	case fenced:
		return admitRenewFirst
	default:
		return admitProceed
	}
}

// refuseUnreachableOwner reports whether an acquire is refused without being
// proposed: by this node's table the lease is another node's and that owner
// is alive, and this node cannot reach it. The log refuses such an acquire
// for the same reason, so a table that is behind costs a retrying client one
// more refusal; only the log grants.
func refuseUnreachableOwner(owner, self string, live bool, unreached []string) bool {
	return owner != self && live && contains(unreached, owner)
}

// unreached lists the peers this node has no live link to. It reads the
// transport once and the consensus core not at all: it runs on the connect
// path, where a storm makes every lock taken per CONNECT a queue.
func (s *Service) unreached() []string {
	if s.o.Transport == nil {
		return nil
	}
	var out []string
	for _, p := range s.o.Transport.Peers() {
		if p.State != transport.Up {
			out = append(out, p.Name)
		}
	}
	return out
}

// askable reports whether a takeover pull can reach a peer: its control
// connection, which the request travels on and an answer falls back to, is
// in place. A restarted peer is reachable there before its data connection
// makes it Up; controlOnly says it is not Up yet.
func (s *Service) askable(node string) (ask, controlOnly bool) {
	if s.o.Transport == nil {
		return false, false
	}
	for _, p := range s.o.Transport.Peers() {
		if p.Name == node {
			return p.State == transport.Up || p.ControlUp, p.State != transport.Up && p.ControlUp
		}
	}
	return false, false
}

// canAsk is askable, counting a pull made on the control connection alone.
func (s *Service) canAsk(node string) bool {
	ask, controlOnly := s.askable(node)
	if controlOnly {
		s.pullsControlOnly.Add(1)
	}
	return ask
}

// waitLapse waits until the leader declares a previous owner dead, or a ttl
// passes, or the caller gives up.
func (s *Service) waitLapse(ctx context.Context, owner string) {
	s.log.Warn("previous owner did not answer; waiting for its lease to lapse", "owner", owner)
	// A ttl of this node's own time is the longest the owner can go on serving
	// without a renewal before it fences itself; the leader's declaration says
	// sooner that it has stopped.
	limit := time.NewTimer(s.o.TTL)
	defer limit.Stop()
	poll := time.NewTicker(max(s.o.TTL/40, 10*time.Millisecond))
	defer poll.Stop()
	for s.table.Live(owner) {
		select {
		case <-poll.C:
		case <-limit.C:
			return
		case <-ctx.Done():
			return
		case <-s.stop:
			return
		}
	}
}

// NoteServedEmpty records a CONNECT the session manager answered Session
// Present false after an acquire that carried evidence: the client's lease
// said a session existed and none was held or obtained here. It is counted by
// reason and logged with the lease's evidence; the client is served, as the
// owner decided for a session found nowhere.
func (s *Service) NoteServedEmpty(ns, id string, ev EmptyEvidence) {
	if n := s.servedEmpty[ev.Reason]; n != nil {
		n.Add(1)
	}
	s.count("hoardmq_sessions_rebuilt_empty_total", map[string]string{"reason": ev.Reason}, 1)
	if ev.Reason == EmptyTakeover {
		s.count("hoardmq_lease_takeovers_total", map[string]string{"kind": "empty"}, 1)
	}
	s.log.Warn("session served empty although its lease says one existed", "client", id, "namespace", ns, "reason", ev.Reason, "owner", ev.Owner, "token", ev.Token)
}

func (s *Service) servedEmptyCounts() map[string]int64 {
	out := map[string]int64{}
	for r, n := range s.servedEmpty {
		if v := n.Load(); v > 0 {
			out[r] = v
		}
	}
	return out
}

// Fetched is what Options.Fallback found: the state, whether every node asked
// answered, the holders that were down, and a copy a former holder kept,
// found while a source did not answer, with that holder.
type Fetched struct {
	State    *store.SessionState
	Answered bool
	Down     []string
	Kept     *store.SessionState
	KeptFrom string
}

// fallback fetches a replica on a context of its own, bounded by
// FallbackTimeout: the pull before it may have spent the caller's, and a fetch
// on a spent context asks no peer, so a copy held on one is never found.
func (s *Service) fallback(ns, id, prev string) Fetched {
	ctx, cancel := context.WithTimeout(s.ctx, s.o.FallbackTimeout)
	defer cancel()
	return s.o.Fallback(ctx, ns, id, prev)
}

// submit queues an operation and waits for its result.
func (s *Service) submit(ctx context.Context, op Op) (Result, error) {
	r := request{op: op, done: make(chan Result, 1), err: make(chan error, 1)}
	s.enqueue(r)
	select {
	case res := <-r.done:
		return res, nil
	case err := <-r.err:
		return Result{}, err
	case <-ctx.Done():
		return Result{}, ctx.Err()
	case <-s.stop:
		return Result{}, consensus.ErrStopped
	}
}

// submitAsync queues an operation whose result nobody waits for.
func (s *Service) submitAsync(op Op) {
	s.enqueue(request{op: op})
}

func (s *Service) enqueue(r request) {
	s.mu.Lock()
	s.pending = append(s.pending, r)
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// flushLoop proposes batches: everything pending goes in one command, and
// a new batch starts only when the previous one has completed.
func (s *Service) flushLoop() {
	defer close(s.done)
	for {
		select {
		case <-s.stop:
			s.failPending(consensus.ErrStopped)
			return
		case <-s.wake:
		}
		for {
			s.mu.Lock()
			if s.inflight || len(s.pending) == 0 {
				s.mu.Unlock()
				break
			}
			batch := s.pending
			s.pending = nil
			s.inflight = true
			s.mu.Unlock()
			s.flush(batch)
			s.mu.Lock()
			s.inflight = false
			s.mu.Unlock()
		}
	}
}

func (s *Service) failPending(err error) {
	s.mu.Lock()
	batch := s.pending
	s.pending = nil
	s.mu.Unlock()
	for _, r := range batch {
		if r.err != nil {
			r.err <- err
		}
	}
}

// flush proposes one batch, retrying once when leadership moved.
func (s *Service) flush(batch []request) {
	ops := make([]Op, len(batch))
	for i, r := range batch {
		ops[i] = r.op
	}
	// A batch waits out a leader election (bootstrap, failover, a partition
	// healing) rather than failing at once: the first attempt that finds a
	// leader wins, and the takeover timeout bounds the wait.
	deadline := s.o.Clock().Add(s.o.TakeoverTimeout)
	results, err := s.propose(ops)
	for err != nil && retryable(err) && s.o.Clock().Before(deadline) && s.ctx.Err() == nil {
		select {
		case <-s.stop:
		case <-time.After(50 * time.Millisecond):
		}
		if s.ctx.Err() != nil {
			break
		}
		results, err = s.propose(ops)
	}
	for i, r := range batch {
		if err != nil {
			if r.err != nil {
				r.err <- err
			}
			if r.op.Kind == OpSetLevel {
				s.noteRaise(r.op.Level, Result{}, err)
			}
			continue
		}
		if r.done != nil {
			r.done <- results[i]
		} else if r.op.Kind == OpAcquire && results[i].Reason == "stale" {
			// A background renewal found the lease moved; the move itself
			// reclaims the session here.
			s.count("hoardmq_lease_ops_total", map[string]string{"op": "renew", "outcome": "stale"}, 1)
		} else if r.op.Kind == OpSetLevel {
			s.noteRaise(r.op.Level, results[i], nil)
		}
	}
	if err != nil {
		s.log.Debug("lease batch failed", "ops", len(ops), "err", err)
	}
}

// retryable reports whether a proposal failed for want of a leader.
func retryable(err error) bool {
	var nl *consensus.NotLeaderError
	return errors.As(err, &nl) || errors.Is(err, ErrNoLeader) || errors.Is(err, consensus.ErrDropped)
}

// propose runs a batch through the core when this node leads, or forwards
// it to the leader over the transport on the ordered lease-notification
// class, within the takeover timeout.
func (s *Service) propose(ops []Op) ([]Result, error) {
	ctx, cancel := context.WithTimeout(s.ctx, s.o.TakeoverTimeout)
	defer cancel()
	return s.proposeVia(ctx, ops, transport.LeaseNotification)
}

// proposeVia proposes directly on the leader, or forwards to the leader on
// the given class and waits for the answer on the same class, until ctx
// ends. LeaseNotification is the ordered lane the batches share with the
// takeover traffic; LeaseAlive is the urgent lane a renewal takes alone.
func (s *Service) proposeVia(ctx context.Context, ops []Op, class transport.Class) ([]Result, error) {
	s.proposals.Add(1)
	s.countProposed(ops)
	if s.o.Core.Status().Role == "leader" || s.o.Transport == nil {
		v, err := s.o.Core.Propose(ctx, Kind, Encode(ops))
		if err != nil {
			return nil, err
		}
		results, ok := v.([]Result)
		if !ok {
			return nil, fmt.Errorf("lease: unexpected apply result %T", v)
		}
		return results, nil
	}
	leader, ok := s.o.Core.Leader()
	if !ok || leader == s.o.Node {
		return nil, ErrNoLeader
	}
	req := s.nextReq.Add(1)
	ch := make(chan forwardResult, 1)
	s.mu.Lock()
	s.forwards[req] = ch
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.forwards, req)
		s.mu.Unlock()
	}()
	body, _ := json.Marshal(forwardMsg{Req: req, Ops: ops})
	if err := s.o.Transport.Send(ctx, leader, class, append([]byte{envForward}, body...)); err != nil {
		if errors.Is(err, transport.ErrUnsupportedClass) || errors.Is(err, transport.ErrUrgentFull) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: %v", ErrLeaderUnreachable, err)
	}
	select {
	case r := <-ch:
		if r.Error != "" {
			if r.Error == "not leader" {
				return nil, &consensus.NotLeaderError{}
			}
			return nil, errors.New(r.Error)
		}
		return r.Results, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("%w: forward timed out", ErrNoLeader)
	case <-s.stop:
		return nil, consensus.ErrStopped
	}
}

// receiveEnvelope builds the handler for one class carrying forwarded
// proposals (served on the leader) and their results; the answer goes back
// on the class the request came in on.
func (s *Service) receiveEnvelope(class transport.Class) transport.Handler {
	return func(peer string, payload []byte) { s.receiveLease(peer, payload, class) }
}

func (s *Service) receiveLease(peer string, payload []byte, class transport.Class) {
	if len(payload) == 0 {
		return
	}
	switch payload[0] {
	case envForward:
		var m forwardMsg
		if json.Unmarshal(payload[1:], &m) != nil {
			return
		}
		go s.serveForward(peer, m, class)
	case envForwardCommand:
		var m commandMsg
		if json.Unmarshal(payload[1:], &m) != nil {
			return
		}
		go s.serveCommand(peer, m, class)
	case envForwardResult:
		var r forwardResult
		if json.Unmarshal(payload[1:], &r) != nil {
			return
		}
		s.mu.Lock()
		ch := s.forwards[r.Req]
		s.mu.Unlock()
		if ch != nil {
			select {
			case ch <- r:
			default:
			}
		}
	}
}

func (s *Service) serveForward(peer string, m forwardMsg, class transport.Class) {
	out := forwardResult{Req: m.Req}
	if s.o.Core.Status().Role != "leader" {
		out.Error = "not leader"
	} else {
		// Times are the leader's, so alive records compare on one clock.
		now := s.o.Clock()
		for i := range m.Ops {
			m.Ops[i].Now = now
		}
		ctx, cancel := context.WithTimeout(context.Background(), s.o.TakeoverTimeout)
		v, err := s.o.Core.Propose(ctx, Kind, Encode(m.Ops))
		cancel()
		if err != nil {
			out.Error = err.Error()
		} else if results, ok := v.([]Result); ok {
			out.Results = results
		} else {
			out.Error = "unexpected apply result"
		}
	}
	body, _ := json.Marshal(out)
	ctx, cancel := context.WithTimeout(context.Background(), s.o.TakeoverTimeout)
	defer cancel()
	_ = s.o.Transport.Send(ctx, peer, class, append([]byte{envForwardResult}, body...))
}

// CommandKindForwardable is the decision on a command of another kind the
// lease lane may carry to the leader: a registered component's, never the
// consensus package's own nor the lease table's, whose operations the leader
// stamps with its clock on their own path.
func CommandKindForwardable(kind consensus.Kind) bool {
	return kind >= consensus.KindReserved && kind != Kind
}

// ProposeCommand puts a command of another component's kind through the
// consensus core: directly on the leader (or with no transport), else
// forwarded to the leader on the lease lane. It retries while no leader is
// known, until ctx ends, and returns the component's result as JSON. A ctx
// that has already ended proposes nothing.
func (s *Service) ProposeCommand(ctx context.Context, kind consensus.Kind, payload []byte) (json.RawMessage, error) {
	if !CommandKindForwardable(kind) {
		return nil, fmt.Errorf("lease: kind %d is not forwarded as a command", kind)
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		raw, err := s.proposeCommand(ctx, kind, payload)
		if err == nil || !retryable(err) {
			return raw, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.stop:
			return nil, consensus.ErrStopped
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (s *Service) proposeCommand(ctx context.Context, kind consensus.Kind, payload []byte) (json.RawMessage, error) {
	if s.o.Core.Status().Role == "leader" || s.o.Transport == nil {
		v, err := s.o.Core.Propose(ctx, kind, payload)
		if err != nil {
			return nil, err
		}
		return json.Marshal(v)
	}
	leader, ok := s.o.Core.Leader()
	if !ok || leader == s.o.Node {
		return nil, ErrNoLeader
	}
	req := s.nextReq.Add(1)
	ch := make(chan forwardResult, 1)
	s.mu.Lock()
	s.forwards[req] = ch
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.forwards, req)
		s.mu.Unlock()
	}()
	body, _ := json.Marshal(commandMsg{Req: req, Kind: kind, Payload: payload})
	if err := s.o.Transport.Send(ctx, leader, transport.LeaseNotification, append([]byte{envForwardCommand}, body...)); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrLeaderUnreachable, err)
	}
	select {
	case r := <-ch:
		if r.Error != "" {
			if r.Error == "not leader" {
				return nil, &consensus.NotLeaderError{}
			}
			return nil, errors.New(r.Error)
		}
		return r.Raw, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.stop:
		return nil, consensus.ErrStopped
	}
}

// serveCommand proposes a forwarded command on the leader and answers with
// its result.
func (s *Service) serveCommand(peer string, m commandMsg, class transport.Class) {
	out := forwardResult{Req: m.Req}
	switch {
	case !CommandKindForwardable(m.Kind):
		out.Error = fmt.Sprintf("kind %d is not forwarded as a command", m.Kind)
	case s.o.Core.Status().Role != "leader":
		out.Error = "not leader"
	default:
		ctx, cancel := context.WithTimeout(context.Background(), s.o.TakeoverTimeout)
		v, err := s.o.Core.Propose(ctx, m.Kind, m.Payload)
		cancel()
		if err != nil {
			out.Error = err.Error()
		} else if raw, err := json.Marshal(v); err != nil {
			out.Error = err.Error()
		} else {
			out.Raw = raw
		}
	}
	body, _ := json.Marshal(out)
	ctx, cancel := context.WithTimeout(context.Background(), s.o.TakeoverTimeout)
	defer cancel()
	_ = s.o.Transport.Send(ctx, peer, class, append([]byte{envForwardResult}, body...))
}

// tickLoop launches this node's renewals — every quarter ttl, at most
// renewInflightMax at a time, sooner after a failure — fences the node when
// its alive record has lapsed, and, on the leader, adopts dead owners'
// leases. A renewal runs on its own goroutine so a slow one never delays
// the next launch or the fence check (WI 1337).
func (s *Service) tickLoop() {
	interval := s.o.TTL / 4
	retry := s.renewRetry()
	next := time.Duration(0)
	for {
		select {
		case <-s.stop:
			return
		case <-time.After(next):
			next = interval
		case <-s.renewWake:
			// A renewal failed: try again after the floor, not a quarter ttl.
			select {
			case <-s.stop:
				return
			case <-time.After(retry):
			}
			next = interval
		}
		s.markLoop(time.Now())
		st := s.trackLeader()
		s.launchRenewal()
		s.checkFence(st)
		s.adopt()
		s.raiseLevel()
		s.noteLevel()
		s.noteDeclared()
	}
}

// markLoop records the gap since the tick loop's previous iteration. It
// uses the wall clock, not Options.Clock: the question is how late the loop
// ran, which a skewed test clock would hide.
func (s *Service) markLoop(now time.Time) {
	if last := s.loopLastNs.Swap(now.UnixNano()); last != 0 {
		gap := now.UnixNano() - last
		s.loopGapNs.Store(gap)
		storeMax(&s.loopGapMaxNs, gap)
	}
}

// renewRetry is the floor between renewal attempts after a failure.
func (s *Service) renewRetry() time.Duration { return max(s.o.TTL/32, 50*time.Millisecond) }

// launchRenewal starts one renewal unless the in-flight bound is reached
// or renewals are paused.
func (s *Service) launchRenewal() {
	if s.renewalsPaused.Load() {
		return
	}
	for {
		n := s.renewInflight.Load()
		if n >= renewInflightMax {
			s.renewSkipped.Add(1)
			return
		}
		if s.renewInflight.CompareAndSwap(n, n+1) {
			break
		}
	}
	go func() {
		defer s.renewInflight.Add(-1)
		if !s.renew() {
			select {
			case s.renewWake <- struct{}{}:
			default:
			}
		}
	}()
}

// trackLeader records when this node became leader, for the adoption
// grace, and caches the leader's name for refusals: a refused CONNECT must
// not take the core's lock for its redirect hint when a storm refuses
// thousands a second.
func (s *Service) trackLeader() consensus.Status {
	began := time.Now()
	st := s.o.Core.Status()
	read := time.Since(began).Nanoseconds()
	s.statusReadNs.Store(read)
	storeMax(&s.statusReadMaxNs, read)
	s.leaderHint.Store(st.Leader)
	// The tenure clock measures one leader: a different name is a new
	// tenure even when the tick never sampled the leaderless gap between
	// them (at the product ttl a tick is 7.5 s apart, an election ~1 s).
	prev, _ := s.leaderKnown.Load().(string)
	now := s.steady()
	if leaderless(st) {
		if s.leaderlessSince.Load() == 0 {
			s.leaderlessSince.Store(now.UnixNano())
		}
	} else if since := s.leaderlessSince.Swap(0); since != 0 {
		if d := now.Sub(time.Unix(0, since)); int64(d) > s.leaderlessMax.Load() {
			s.leaderlessMax.Store(int64(d))
		}
	}
	switch {
	case leaderless(st):
		s.leaderKnownSince.Store(0)
		s.leaderKnown.Store("")
	case st.Leader != prev || s.leaderKnownSince.Load() == 0:
		s.leaderKnownSince.Store(now.UnixNano())
		s.leaderKnown.Store(st.Leader)
	}
	if st.Role == "leader" {
		if s.leaderSince.Load() == 0 {
			s.leaderSince.Store(now.UnixNano())
		}
		return st
	}
	s.leaderSince.Store(0)
	return st
}

// leaderlessWorst is the longest this node has gone without a leader,
// counting a window that is still open. Without that a run that ends while
// leaderless reports zero, which is the opposite of what happened.
func (s *Service) leaderlessWorst() int64 {
	worst := s.leaderlessMax.Load()
	if since := s.leaderlessSince.Load(); since != 0 {
		if d := int64(s.steady().Sub(time.Unix(0, since))); d > worst {
			worst = d
		}
	}
	return worst
}

// leaderless reports whether the group has no leader as this node sees it:
// none named, or an election in progress.
func leaderless(st consensus.Status) bool { return st.Leader == "" || st.Role == "candidate" }

// quorumOf reports whether this node and the peers it can reach form a
// strict majority of the configured membership. A peer counts as reachable
// while its link is up, and for half a ttl after the link last went down:
// under a storm a link resets for seconds and is redialed, whereas at the
// moment a partitioned node's record lapses its links have been down for the
// whole ttl.
func quorumOf(peers []transport.PeerInfo, ttl time.Duration) bool {
	reachable := 0
	for _, p := range peers {
		if reachableAt(p, ttl) {
			reachable++
		}
	}
	return (1+reachable)*2 > 1+len(peers)
}

// reachable is the one reading of a peer's link both sides of the fence
// argument use: up, or down for less than half a ttl. The leader's
// adoption grace and a follower's fence deferral must agree on it, or a
// follower deferring through a link reset could be adopted during it.
func (s *Service) reachable(p transport.PeerInfo) bool { return reachableAt(p, s.o.TTL) }

func reachableAt(p transport.PeerInfo, ttl time.Duration) bool {
	return p.State == transport.Up || time.Duration(p.DownForMs*float64(time.Millisecond)) < ttl/2
}

// peerReachable is reachable by name.
func (s *Service) peerReachable(node string) bool {
	if s.o.Transport == nil {
		return false
	}
	for _, p := range s.o.Transport.Peers() {
		if p.Name == node {
			return s.reachable(p)
		}
	}
	return false
}

// renewal is one attempt shared by every alive() caller that arrives while
// it runs; ok is written before done is closed.
type renewal struct {
	done chan struct{}
	ok   bool
}

// alive renews synchronously, for a fenced node's connect path. Callers
// share one attempt: a fenced node under a storm has every handshake slot
// asking at once, and one proposal answers them all — a proposal per caller
// is itself a storm on the consensus core (seen at 100k: ~600 renewal
// proposals a second kept both survivors fenced). An attempt that has just
// failed is not repeated inside the retry floor either: with no leader an
// attempt fails in microseconds, and a thousand handshakes retrying it
// back to back is a storm on the core's own lock instead (seen at 100k
// round 5c: 8,000 no-leader failures a second, and no election).
func (s *Service) alive() bool {
	if s.renewalsPaused.Load() {
		return false
	}
	if failed := s.renewFailedAt.Load(); failed != 0 && s.o.Clock().Sub(time.Unix(0, failed)) < s.renewRetry() {
		return false
	}
	s.renewMu.Lock()
	cur := s.renewCur
	if cur == nil {
		cur = &renewal{done: make(chan struct{})}
		s.renewCur = cur
		s.renewMu.Unlock()
		cur.ok = s.renew()
		if !cur.ok {
			s.renewFailedAt.Store(s.o.Clock().UnixNano())
		}
		s.renewMu.Lock()
		s.renewCur = nil
		s.renewMu.Unlock()
		close(cur.done)
		return cur.ok
	}
	s.renewMu.Unlock()
	select {
	case <-cur.done:
		return cur.ok
	case <-s.stop:
		return false
	}
}

// renew proposes the node's alive record on its own: directly on the
// leader, on the urgent lane to the leader from a follower, and through the
// batched path only when the link predates the lane. One attempt may take
// up to the ttl: it is background work, not a client's wait, and under a
// storm the core's apply latency is what bounds it; the launches overlap,
// so any success inside the ttl refreshes lastAlive.
func (s *Service) renew() bool {
	ctx, cancel := context.WithTimeout(s.ctx, s.o.TTL)
	defer cancel()
	start := time.Now()
	op := Op{Kind: OpAlive, Node: s.o.Node, Advertise: s.o.Advertise, Now: s.o.Clock(), Level: s.BuildLevel()}
	results, err := s.proposeVia(ctx, []Op{op}, transport.LeaseAlive)
	var res Result
	switch {
	case errors.Is(err, transport.ErrUnsupportedClass):
		s.renewFallback.Add(1)
		res, err = s.submit(ctx, op)
	case err == nil && len(results) == 1:
		res = results[0]
	case err == nil:
		err = fmt.Errorf("lease: renewal returned %d results", len(results))
	}
	if err != nil || !res.OK {
		s.aliveFails.Add(1)
		switch {
		case errors.Is(err, context.DeadlineExceeded) || (err != nil && strings.Contains(err.Error(), "timed out")):
			s.renewTimeouts.Add(1)
		case errors.Is(err, transport.ErrUrgentFull):
			s.renewLaneFull.Add(1)
		// The specific case first: ErrLeaderUnreachable wraps ErrNoLeader, so
		// testing them the other way round compiles, passes, and counts
		// everything as noleader — which is the defect this ordering fixes.
		case errors.Is(err, ErrLeaderUnreachable):
			s.renewUnreachable.Add(1)
		case errors.Is(err, ErrNoLeader) || retryable(err):
			s.renewNoLeader.Add(1)
		default:
			s.renewOther.Add(1)
		}
		if streak := s.renewStreak.Add(1); streak == renewWarnStreak {
			s.log.Warn("lease renewals failing; this node fences when its record lapses", "consecutive", streak, "ttl", s.o.TTL, "err", err)
		}
		return false
	}
	s.renewStreak.Store(0)
	took := int64(time.Since(start))
	s.aliveTotalNs.Add(took)
	s.aliveCount.Add(1)
	for {
		cur := s.aliveMaxNs.Load()
		if took <= cur || s.aliveMaxNs.CompareAndSwap(cur, took) {
			break
		}
	}
	s.lastAlive.Store(s.steady().UnixNano())
	s.noteContact(res.Index)
	if s.fenced.Swap(false) {
		s.log.Info("lease renewals resumed; node unfenced")
		s.audit("lease.unfenced", s.o.Node, "ok", nil)
	}
	return true
}

// noteContact records a successful renewal whose alive record applied at
// index: the highest index, and the time of the success that gave it. An
// older renewal that succeeds after a newer one moves neither.
func (s *Service) noteContact(index uint64) {
	if index == 0 {
		return
	}
	now := s.steady()
	s.contactMu.Lock()
	if index > s.contactIndex {
		s.contactIndex, s.contactAt = index, now
	}
	s.contactMu.Unlock()
}

// Contact is the highest log index a renewal of this node's alive record has
// applied at, and when, on the steady clock, that renewal succeeded; zero
// before the first since the service started.
func (s *Service) Contact() (index uint64, at time.Time) {
	s.contactMu.Lock()
	defer s.contactMu.Unlock()
	return s.contactIndex, s.contactAt
}

// Steady is the service's steady clock.
func (s *Service) Steady() time.Time { return s.steady() }

// TTL is the lease ttl.
func (s *Service) TTL() time.Duration { return s.o.TTL }

// Applied is the last log index this node's state machine has applied,
// across its components. A renewal's index is compared with this: one
// component's applied index moves only on that component's own entries.
func (s *Service) Applied() uint64 { return s.o.Machine.Applied() }

// checkFence fences the node when it has not renewed within the ttl —
// unless this node can reach a quorum and the group has had no leader, or
// a leader for less than a ttl (WI 1341, owner decision): with no leader
// nothing can be adopted or taken over, so no second live owner can appear
// while the fence waits, and a fresh leader gives every reachable owner a
// ttl before adopting (the grace in adopt), so deferring for that same ttl
// is covered by the same argument; fencing would only disconnect every
// client for an election's duration. A leader known for a full ttl that
// this node still could not renew with, or a lost quorum (a partition),
// fences as before.
func (s *Service) checkFence(st consensus.Status) {
	now := s.steady()
	last := time.Unix(0, s.lastAlive.Load())
	if now.Sub(last) <= s.o.TTL || s.fenced.Load() || s.renewalsPaused.Load() {
		s.deferring.Store(false)
		return
	}
	var peers []transport.PeerInfo
	if s.o.Transport != nil {
		peers = s.o.Transport.Peers()
	}
	outcome, rec := decideFence(now, last, s.o.TTL, st, s.leaderKnownSince.Load(), peers, s.o.Transport != nil)
	rec.LoopGapMs = float64(s.loopGapNs.Load()) / 1e6
	rec.StatusReadMs = float64(s.statusReadNs.Load()) / 1e6
	if outcome == FenceDefer {
		s.fencesDeferred.Add(1)
		s.count("hoardmq_lease_fence_deferred_total", nil, 1)
		if !s.deferring.Swap(true) {
			s.recordFence(rec)
			s.log.Warn("this node's record lapsed but the group has no leader; fence deferred while a quorum is reachable", "lastAlive", last, "role", st.Role)
		}
		return
	}
	s.deferring.Store(false)
	s.recordFence(rec)
	if rec.LeaderSettled {
		s.fencesLeaderSettled.Add(1)
	}
	if !rec.QuorumReachable {
		s.fencesNoQuorum.Add(1)
	}
	s.fenced.Store(true)
	s.fences.Add(1)
	s.count("hoardmq_lease_fenced_total", nil, 1)
	s.audit("lease.fenced", s.o.Node, "lapsed", map[string]string{"lastAlive": last.Format(time.RFC3339)})
	s.log.Warn("this node's leases lapsed; disconnecting clients", "lastAlive", last,
		"leaderSettled", rec.LeaderSettled, "quorumReachable", rec.QuorumReachable, "leader", rec.Leader,
		"loopGapMs", rec.LoopGapMs, "statusReadMs", rec.StatusReadMs)
	reason := packet.ServerBusy
	var r *Refusal
	if errors.As(s.refusal(), &r) && r.Reason == packet.UseAnotherServer {
		reason = packet.UseAnotherServer
	}
	s.o.Sessions.FenceAll(reason)
}

// FenceOutcome is what one fence check decided.
type FenceOutcome string

// The outcomes of decideFence.
const (
	FenceNone  FenceOutcome = "none"  // the record has not lapsed, or the node is already fenced
	FenceDefer FenceOutcome = "defer" // lapsed, but no settled leader and a reachable quorum
	FenceNow   FenceOutcome = "fence" // lapsed, and a settled leader or no reachable quorum
)

// FenceRecord is the inputs one fence or deferral was decided on. A
// collapse in which both survivors fence says nothing about which condition
// allowed it without this: a leader this node judged settled, or a quorum it
// judged unreachable (WI 1474, from WI 1470's twelve runs).
type FenceRecord struct {
	AtUnixMs         int64        `json:"at_unix_ms"`
	Outcome          FenceOutcome `json:"outcome"`
	RecordAgeMs      float64      `json:"record_age_ms"`
	Role             string       `json:"role"`
	Term             uint64       `json:"term"`
	Leader           string       `json:"leader"`
	Leaderless       bool         `json:"leaderless"`
	LeaderKnownForMs float64      `json:"leader_known_for_ms"` // 0 while no leader's tenure is being measured
	LeaderSettled    bool         `json:"leader_settled"`
	QuorumReachable  bool         `json:"quorum_reachable"`
	Peers            []PeerReach  `json:"peers,omitempty"`
	// The tick loop's timing when the decision ran: the gap since its
	// previous iteration, and how long its last read of the core's status
	// took.
	LoopGapMs    float64 `json:"loop_gap_ms"`
	StatusReadMs float64 `json:"status_read_ms"`
}

// PeerReach is one peer as the quorum reading saw it.
type PeerReach struct {
	Name      string  `json:"name"`
	State     string  `json:"state"`
	DownForMs float64 `json:"down_for_ms"`
	Reachable bool    `json:"reachable"`
}

// decideFence is checkFence's decision, free of its effects: whether a node
// whose alive record was last renewed at lastAlive fences now, defers, or
// has nothing to decide. The fence disconnects every client on the node, so
// the decision is a function its tests can drive with any inputs. With no
// transport the quorum is taken as reachable.
func decideFence(now, lastAlive time.Time, ttl time.Duration, st consensus.Status, knownSince int64, peers []transport.PeerInfo, hasTransport bool) (FenceOutcome, FenceRecord) {
	rec := FenceRecord{AtUnixMs: now.UnixMilli(), RecordAgeMs: float64(now.Sub(lastAlive)) / 1e6,
		Role: st.Role, Term: st.Term, Leader: st.Leader, Leaderless: leaderless(st)}
	if now.Sub(lastAlive) <= ttl {
		rec.Outcome = FenceNone
		return FenceNone, rec
	}
	if knownSince != 0 {
		rec.LeaderKnownForMs = float64(now.Sub(time.Unix(0, knownSince))) / 1e6
	}
	rec.LeaderSettled = !rec.Leaderless && knownSince != 0 && now.Sub(time.Unix(0, knownSince)) >= ttl
	rec.QuorumReachable = !hasTransport || quorumOf(peers, ttl)
	for _, p := range peers {
		rec.Peers = append(rec.Peers, PeerReach{Name: p.Name, State: p.State.String(), DownForMs: p.DownForMs, Reachable: reachableAt(p, ttl)})
	}
	rec.Outcome = FenceNow
	if !rec.LeaderSettled && rec.QuorumReachable {
		rec.Outcome = FenceDefer
	}
	return rec.Outcome, rec
}

// fenceRecordsMax bounds the records kept; fenceRecordsTotal keeps counting.
const fenceRecordsMax = 16

// recordFence publishes a decision record. Only the tick loop writes, and
// it replaces the slice rather than appending to a shared one, so a reader
// holding an earlier slice never sees it change.
func (s *Service) recordFence(rec FenceRecord) {
	var next []FenceRecord
	if cur := s.fenceRecords.Load(); cur != nil {
		next = append(next, (*cur)[max(0, len(*cur)-(fenceRecordsMax-1)):]...)
	}
	next = append(next, rec)
	s.fenceRecords.Store(&next)
	s.fenceRecordsTotal.Add(1)
}

// Fences is how many times this node has fenced, read from one atomic.
func (s *Service) Fences() int64 { return s.fences.Load() }

// FenceEvidence is the fence decisions and the tick loop's timing, readable
// from atomics alone: LivenessStats also reads the lease table under its
// lock, which a storm can hold for seconds.
type FenceEvidence struct {
	FencesLeaderSettled int64         `json:"fences_leader_settled"`
	FencesNoQuorum      int64         `json:"fences_no_quorum"`
	Records             []FenceRecord `json:"fence_records,omitempty"`
	RecordsTotal        int64         `json:"fence_records_total"`
	LoopGapMaxMs        float64       `json:"loop_gap_max_ms"`
	StatusReadMaxMs     float64       `json:"status_read_max_ms"`
}

// FenceEvidence returns the current evidence.
func (s *Service) FenceEvidence() FenceEvidence {
	ev := FenceEvidence{FencesLeaderSettled: s.fencesLeaderSettled.Load(), FencesNoQuorum: s.fencesNoQuorum.Load(),
		RecordsTotal: s.fenceRecordsTotal.Load(), LoopGapMaxMs: float64(s.loopGapMaxNs.Load()) / 1e6,
		StatusReadMaxMs: float64(s.statusReadMaxNs.Load()) / 1e6}
	if cur := s.fenceRecords.Load(); cur != nil {
		ev.Records = *cur
	}
	return ev
}

// storeMax raises an atomic maximum.
func storeMax(a *atomic.Int64, v int64) {
	for {
		cur := a.Load()
		if v <= cur || a.CompareAndSwap(cur, v) {
			return
		}
	}
}

// adopt, on the leader, declares dead every node it has heard no alive
// record from for a ttl, and moves that node's leases here. The judgement is
// this node's own: how long ago, on its steady clock, it last applied an
// alive record from the node, never a comparison of any clock with the stamp
// in the record, which another leader may have written on another clock and
// which a step of this one would misread. A follower applies the same records,
// so a new leader judges from what it saw itself. It judges nobody until it
// has watched the log for a ttl: a restart, the end of recovery and a snapshot
// restore each begin the watch again.
//
// An owner this node can still reach is given a grace after an election: a
// full ttl from when this node took the lead, so a live node whose renewals
// were lost with the old leader is not judged on records it had no leader to
// send (WI 1337). An owner whose link is down cannot renew with anyone, and
// waiting would only leave its sessions' subscriptions invisible to the rest
// of the cluster for another ttl.
func (s *Service) adopt() {
	if s.o.Core.Status().Role != "leader" {
		return
	}
	if s.BelowLevel() {
		// It serves no client, so what it adopted nobody could reach.
		return
	}
	now := s.steady()
	since := s.leaderSince.Load()
	grace := since == 0 || now.Sub(time.Unix(0, since)) < s.o.TTL
	owns := map[string]bool{}
	for _, owner := range s.table.Owners() {
		owns[owner] = true
	}
	for _, node := range s.table.NodeNames(owns) {
		if node == s.o.Node {
			continue
		}
		info, known := s.table.Node(node)
		if known && info.DeadAt != 0 && !owns[node] {
			continue // declared, and nothing left to move
		}
		if !silent(now, s.observingSince.Load(), s.lastHeard(node), s.o.TTL) {
			continue
		}
		if grace && s.peerReachable(node) {
			continue
		}
		s.timeline.of(node).proposed.Store(wallNs())
		s.submitAsync(Op{Kind: OpReassign, Node: node, To: s.o.Node, SeenAliveIndex: info.AliveIndex, Now: s.o.Clock()})
		s.count("hoardmq_lease_declarations_total", nil, 1)
		s.audit("lease.adopt", node, "proposed", map[string]string{"by": s.o.Node})
	}
}

// noteDeclared publishes how many current members are declared dead. A node
// removed from the membership keeps its record, and its declaration, for
// good; counting it would leave the gauge raised after a decommission.
func (s *Service) noteDeclared() {
	if s.o.Metrics == nil {
		return
	}
	member := map[string]bool{}
	for _, m := range s.o.Core.Members() {
		member[m.Name] = true
	}
	n := 0
	for _, name := range s.table.DeclaredDead() {
		if member[name] {
			n++
		}
	}
	s.o.Metrics.Set("hoardmq_lease_nodes_declared_dead", nil, float64(n))
}

// silent reports whether a node has gone a ttl unheard by a watcher that has
// itself been watching for at least a ttl. heard is zero for a node never
// heard from, which is then judged from when the watch began.
func silent(now time.Time, watchingSince, heard int64, ttl time.Duration) bool {
	from := max(watchingSince, heard)
	return now.Sub(time.Unix(0, from)) > ttl
}

// steady is Options.Steady, or the wall clock for a Service built without
// New.
func (s *Service) steady() time.Time {
	if s.o.Steady != nil {
		return s.o.Steady()
	}
	return s.o.Clock()
}

// heardFrom notes, on this node's steady clock, that an alive record from
// node was applied.
func (s *Service) heardFrom(node string) {
	s.heard.Store(node, s.steady().UnixNano())
	s.timeline.of(node).lastAlive.Store(wallNs())
}

func (s *Service) lastHeard(node string) int64 {
	if v, ok := s.heard.Load(node); ok {
		return v.(int64)
	}
	return 0
}

// observeAfresh begins the watch again: what was heard before says nothing
// about the state now in the table.
func (s *Service) observeAfresh() {
	s.observingSince.Store(s.steady().UnixNano())
}

// changed runs on every node after a lease changes. This node acts when it
// lost a lease it served (evict) or gained an offline lease (adopt).
func (s *Service) changed(before, after *Lease) {
	switch {
	case after != nil:
		s.tokens.Note(after.Namespace, after.ClientID, after.Epoch)
	case before != nil:
		s.tokens.released(before.Namespace, before.ClientID, before.Epoch)
	}
	me := s.o.Node
	s.race.observe(before, after, func() int64 { return s.o.Clock().UnixMilli() })
	if s.o.Metrics != nil {
		s.o.Metrics.Set("hoardmq_leases_active", nil, float64(s.table.Count()))
	}
	var k key
	if after != nil {
		k = key{after.Namespace, after.ClientID}
	} else if before != nil {
		k = key{before.Namespace, before.ClientID}
	}
	wasMine := before != nil && before.Owner == me
	isMine := after != nil && after.Owner == me
	switch {
	case wasMine && !isMine:
		s.cancelTimers(k)
		if after == nil {
			// The lease is gone entirely — released or expired, not taken
			// over — so no takeover will arrive to consume the adoption
			// record and this is the only chance to drop it. Deleting it on
			// the takeover path instead would race the takeover, which is
			// served after this apply (see adoptedAt).
			s.adoptedAt.Delete(k)
		}
		if after != nil {
			// Taken over or reassigned elsewhere: the client is told at once,
			// the state stays for the new owner's pull, and is dropped after
			// the takeover window unless this node owns the lease again.
			// Queued, not spawned: this runs on consensus apply, and a storm
			// produces these by the hundred thousand.
			s.lostLeases.Add(1)
			s.lost.enqueue(k, nil)
		}
	case isMine && !after.Online && (before == nil || before.Owner != me):
		s.startTimers(*after)
		s.adoptions.Add(1)
		now := s.o.Clock().UnixMilli()
		s.firstAdoptMs.CompareAndSwap(0, now)
		s.lastAdoptMs.Store(now)
		s.count("hoardmq_lease_adopted_total", nil, 1)
		s.audit("lease.adopted", k.ns+"/"+k.id, "ok", map[string]string{"epoch": fmt.Sprint(after.Epoch)})
		if s.o.Fallback != nil && before != nil {
			// Adopted from a dead owner: bring the replica here so the
			// queue survives and a reconnecting client can pull it. A pass
			// takes the newest adoption for the key.
			s.adopting.begin(k, adoptJob{epoch: after.Epoch, prev: before.Owner})
			s.adopts.enqueue(k, adoptJob{epoch: after.Epoch, prev: before.Owner})
			s.adoptedAt.Store(k, s.o.Clock())
		}
	case isMine && after.Online:
		if before == nil {
			s.firsts.created(k, after.Epoch)
		}
		if before != nil && before.Owner != me {
			// A client acquired its own lease here, taking it from another
			// owner. Recorded per previous owner as well as in aggregate: only
			// the entry for the *crashed* node is the one the bulk
			// reassignment is racing (WI 1514).
			now := s.o.Clock().UnixMilli()
			s.firstClientTakeMs.CompareAndSwap(0, now)
			s.noteTakeFrom(before.Owner, now)
			s.firstTaken(k, after.Epoch, before.Owner)
		}
		s.cancelTimers(k)
	case isMine && after == nil:
		s.cancelTimers(k)
	}
}

// adoptState is the adoption worker's pass: it installs an adopted session's
// state unless the client's own CONNECT has the adoption already.
func (s *Service) adoptState(k key) {
	if d := s.o.AdoptionDelay; d > 0 {
		select {
		case <-time.After(d):
		case <-s.ctx.Done():
			return
		}
	}
	ad, st := s.adopting.claim(k)
	if st != adoptionQueued {
		return
	}
	ctx, cancel := context.WithTimeout(s.ctx, s.o.TakeoverTimeout)
	defer cancel()
	state, got, wait, unconfirmed := s.fetchAdopted(ctx, k, ad, false)
	checked := time.Now()
	if got == fetchUnanswered {
		s.adoptWorkerUnanswered.Add(1)
	}
	keptAtGiveUp := false
	if got == fetchUnanswered && !s.o.AdmitUnansweredFetch {
		// The claim is this pass's, so the record's counts are its to read.
		attempts, spent := ad.attempts+1, ad.spent+1
		givesUp := adoptionGivesUp(spent, s.o.AdoptRetries, wait.kind)
		switch {
		case givesUp && giveUpServes(wait.kind, unconfirmed.state != nil) == serveKept:
			state, got, keptAtGiveUp = unconfirmed.state, fetchState, true
			state.Epoch = ad.job.epoch
			state.Will = nil
		case givesUp:
			s.endAdoption(k, ad)
			s.adoptGaveUp.Add(1)
			s.count("hoardmq_lease_adoptions_abandoned_total", nil, 1)
			s.log.Warn("adoption given up: its fetches went unanswered and a copy may exist", "client", k.id, "from", ad.job.prev, "token", ad.job.epoch, "attempts", attempts)
			return
		default:
			s.notePastBound(k, ad, wait)
			if attempts, mine := s.adopting.giveBack(k, ad, true, wait.kind == waitNone); mine {
				s.adoptRetries.Add(1)
				s.retryAdoption(k, ad.job, attempts)
			}
			return
		}
	}
	defer s.endAdoption(k, ad)
	switch got {
	case fetchMoved:
		s.adoptWorkerMoved.Add(1)
		s.handOn(k)
	case fetchEmpty:
		s.adoptWorkerEmpty.Add(1)
	}
	if state == nil {
		return
	}
	age, connected := s.o.Sessions.ConnAge(k.ns, k.id)
	if g := s.o.ImportGate; g != nil {
		g(k.ns, k.id)
	}
	seen := s.tokens.Seen(k.ns, k.id)
	began := time.Now()
	if !s.o.Sessions.ImportIf(state, s.admitImport(k.ns, k.id, ad.job.epoch)) {
		// The lease has moved on since the fetch's check. This node's copy of
		// the session, which the import would have dropped, stays for the
		// node that holds the lease now.
		s.adoptImportRefused.Add(1)
		s.noteWaitedOn(k, ad.job.prev)
		s.handOn(k)
		s.log.Debug("adopted state not imported: a later grant was seen", "client", k.id, "token", ad.job.epoch, "seen", s.tokens.Seen(k.ns, k.id))
		return
	}
	s.noteImport(k, ad, "worker", seen, checked, began, time.Now())
	if connected {
		switch importFinds(ad.job.recovered, age, time.Since(ad.begun)) {
		case connRecovered:
			s.importConnRecovered.Add(1)
		case connBeforeAdoption:
			s.importConnBefore.Add(1)
		default:
			s.importConnAfter.Add(1)
		}
	}
	s.adoptedByWorker.Add(1)
	if keptAtGiveUp {
		s.adoptGaveUpKept.Add(1)
		s.count("hoardmq_lease_adoptions_kept_copy_total", nil, 1)
		s.log.Warn("adoption given up with a source silent: served the copy a former holder kept", "client", k.id, "from", ad.job.prev, "holder", unconfirmed.from, "token", ad.job.epoch, "entries", len(state.Entries))
	}
	s.confirmLater(k, ad.job.epoch, ad.job.prev)
}

// importAdmitted decides whether a session fetched for the grant token may be
// installed here: not once a later grant for the client has been seen. It is
// pure; the install it admits drops this node's replica copy of the session,
// which a node holding the later grant may need.
func importAdmitted(seen, token uint64) bool { return seen <= token }

// admitImport is the check an install of state fetched for token passes,
// called under the session's shard lock (Sessions.ImportIf).
func (s *Service) admitImport(ns, id string, token uint64) func() bool {
	return func() bool { return s.o.ImportPastLaterGrant || importAdmitted(s.tokens.Seen(ns, id), token) }
}

// AdmitImport is the check a CONNECT's install of a takeover's state, pulled
// or fetched for token, passes.
func (s *Service) AdmitImport(ns, id string, token uint64) func() bool {
	return s.admitImport(ns, id, token)
}

// NoteImportRefused counts a CONNECT whose install of a takeover's state was
// refused because a later grant for the client had been seen; the CONNECT is
// refused with Server Busy.
func (s *Service) NoteImportRefused(ns, id string, token uint64) {
	s.connectImportRefused.Add(1)
	s.count("hoardmq_lease_ops_total", map[string]string{"op": "acquire", "outcome": "import-refused"}, 1)
	s.log.Debug("takeover state not imported: a later grant was seen", "client", id, "token", token, "seen", s.tokens.Seen(ns, id))
}

type connFound int

const (
	connAfterAdoption connFound = iota
	connBeforeAdoption
	connRecovered
)

// importFinds classifies a connection the worker's import is about to close:
// connAge is how long ago it was accepted, adoptionAge how long ago the
// adoption was recorded.
func importFinds(recovered bool, connAge, adoptionAge time.Duration) connFound {
	switch {
	case recovered:
		return connRecovered
	case connAge >= adoptionAge:
		return connBeforeAdoption
	default:
		return connAfterAdoption
	}
}

// undeclaredWait is what an unanswered fetch of adopted state waits on: a
// node the log says is alive that gave no answer, or nothing.
type undeclaredWait int

const (
	waitNone        undeclaredWait = iota
	waitOwner                      // the previous owner, or the owner a node asked named, not asked
	waitOwnerSilent                // the previous owner, asked, did not answer
	waitHolder                     // a replica holder that was down
)

// waitOn is an unanswered fetch's wait and the node it waits on.
type waitOn struct {
	kind undeclaredWait
	node string
}

// fetchResult is how a fetch of adopted state ended.
type fetchResult int

const (
	fetchState       fetchResult = iota // state came back and the lease is still this node's
	fetchEmpty                          // every node asked answered that it holds nothing
	fetchMoved                          // the lease moved on during the fetch
	fetchUnanswered                     // a node that was asked did not reply, and no state came back
	fetchSuperfluous                    // a late state arrived for a session this node already holds
)

// keptCopy is a copy a former holder kept, found while a source did not
// answer, and that holder.
type keptCopy struct {
	state *store.SessionState
	from  string
}

// fetchAdopted brings an adopted session's state here; the Will stays with
// the lease timers, so it is stripped. The previous owner is asked first when
// it can be reached: this node adopted the lease because it read that owner's
// alive record as stale, which is a judgement about another node's clock, and
// a replica may be behind what a living owner holds. wait names, for an
// unanswered result, the node alive by the log it waits on, in
// noStateVerdict's order. unconfirmed is a copy a former holder kept, found
// while a source did not answer, given only with the unanswered result that
// waits on nothing alive.
func (s *Service) fetchAdopted(ctx context.Context, k key, ad *adoption, inline bool) (_ *store.SessionState, _ fetchResult, wait waitOn, unconfirmed keptCopy) {
	job := ad.job
	// A late acknowledgement's state is the session itself: the previous owner
	// removed it to send it. Nobody is asked while one is in hand.
	st := s.adopting.takeLate(ad)
	late := st != nil
	answered := true
	pending := ""
	var down []string
	asked := st == nil && job.prev != "" && job.prev != s.o.Node && s.canAsk(job.prev)
	if asked {
		began := time.Now()
		st, answered, _, pending = s.pull(ctx, job.prev, k.ns, k.id, job.epoch, Requester{})
		if inline {
			s.adoptInlinePulls.Add(1)
			s.adoptInlinePullNs.Add(int64(time.Since(began)))
		}
	}
	pulled := answered
	if st == nil && s.o.Fallback != nil {
		f := s.fallback(k.ns, k.id, job.prev)
		st, down = f.State, f.Down
		answered = answered && f.Answered
		unconfirmed = keptCopy{state: f.Kept, from: f.KeptFrom}
	}
	if st == nil {
		// It may have come while the others were being asked.
		st = s.adopting.takeLate(ad)
		late = st != nil
	}
	fromGiven := false
	if st == nil {
		if sealed, _, ok := s.o.Sessions.Sealed(k.ns, k.id); ok {
			if _, held := s.o.Sessions.Export(k.ns, k.id); !held {
				st, fromGiven = sealed, true
			}
		}
	}
	// Before anything else is made of the result: a lease that has left needs
	// neither its state nor another attempt.
	if l, ok := s.table.Lookup(k.ns, k.id); !ok || l.Owner != s.o.Node || l.Epoch != job.epoch {
		if (late || job.takeover) && s.tokens.Seen(k.ns, k.id) == job.epoch {
			// This node's table has not applied the grant the job was recorded
			// for: this node learned it from its own acquire's result, which can
			// arrive before its table has applied it. A state in hand waits on
			// the record for the next attempt.
			if st != nil {
				s.adopting.putLate(ad, st)
			}
			return nil, fetchUnanswered, waitOn{}, keptCopy{}
		}
		if late {
			s.lateStateLeaseMoved.Add(1)
			s.log.Warn("a late takeover acknowledgement's state dropped: the lease has moved on", "client", k.id, "token", job.epoch)
		}
		s.noteWaitedOn(k, job.prev)
		return nil, fetchMoved, waitOn{}, keptCopy{}
	}
	if fromGiven {
		s.givenReused.Add(1)
	}
	if late {
		// What this node holds is what the client was, or is being, served
		// from; importing over it would close that connection.
		if _, held := s.o.Sessions.Export(k.ns, k.id); held {
			s.lateStateSuperfluous.Add(1)
			return nil, fetchSuperfluous, waitOn{}, keptCopy{}
		}
		s.lateStateUsed.Add(1)
	}
	if st == nil {
		owner := ownerWait(job.prev, s.o.Node, asked, pulled, job.prev != "" && s.table.Live(job.prev))
		pendingUnaccounted := ownerUnaccounted(pending, s.o.Node, false, pending != "" && s.table.Live(pending))
		holder := s.undeclaredHolder(down)
		got, kind := noStateVerdict(owner, pendingUnaccounted, holder != "", answered)
		switch {
		case kind == waitOwner && owner == waitOwner:
			s.adoptOwnerUndeclared.Add(1)
			s.log.Debug("adopted lease found nowhere, but its previous owner could not be asked and is not declared dead", "client", k.id, "from", job.prev, "token", job.epoch)
			return nil, got, waitOn{kind, job.prev}, keptCopy{}
		case kind == waitOwnerSilent:
			s.adoptOwnerSilent.Add(1)
			s.log.Debug("adopted lease found nowhere, but its previous owner did not answer and is not declared dead", "client", k.id, "from", job.prev, "token", job.epoch)
			return nil, got, waitOn{kind, job.prev}, keptCopy{}
		case kind == waitOwner:
			// The adoption waits on the named owner from now on, as on its own.
			s.adopting.repoint(ad, pending)
			s.adoptPendingOwner.Add(1)
			s.log.Debug("adopted lease found nowhere, but the node asked was waiting on an owner that is not declared dead", "client", k.id, "from", job.prev, "pending", pending, "token", job.epoch)
			return nil, got, waitOn{kind, pending}, keptCopy{}
		case kind == waitHolder:
			s.adoptHolderUndeclared.Add(1)
			s.log.Debug("adopted lease found nowhere, but a replica holder is down and not declared dead", "client", k.id, "from", job.prev, "holder", holder, "answered", answered, "token", job.epoch)
			return nil, got, waitOn{kind, holder}, keptCopy{}
		case got == fetchUnanswered:
			s.log.Debug("adopted lease's fetch went unanswered", "client", k.id, "from", job.prev)
			return nil, got, waitOn{}, unconfirmed
		}
		s.log.Debug("adopted lease has no state anywhere", "client", k.id, "from", job.prev)
		return nil, got, waitOn{}, keptCopy{}
	}
	// The state carries the epoch its previous owner wrote it under. It is
	// imported at the adopted lease's epoch: the import journals it whole,
	// stamped for replication with that epoch, and a follower whose table has
	// applied the reassignment refuses anything older.
	st.Epoch = job.epoch
	st.Will = nil
	s.log.Debug("adopted lease state fetched", "client", k.id, "from", job.prev, "late", late, "entries", len(st.Entries), "subscriptions", len(st.Subscriptions))
	s.count("hoardmq_lease_adopted_state_total", nil, 1)
	s.audit("lease.adopted", k.ns+"/"+k.id, "state-imported", map[string]string{"entries": fmt.Sprint(len(st.Entries))})
	return st, fetchState, waitOn{}, keptCopy{}
}

// endAdoption ends an adoption, and keeps a late state that reached it too
// late for its fetcher: the key gets a fresh adoption for it.
func (s *Service) endAdoption(k key, ad *adoption) {
	if late := s.adopting.end(k, ad); late != nil {
		s.keepLateState(ad.job.prev, late)
	}
}

// maxAdoptRetryDelay caps the wait between the adoption worker's fetches.
const maxAdoptRetryDelay = 8 * time.Second

// retryDelay is the wait before the worker's fetch that follows attempts
// unanswered ones: base, doubling, capped.
func retryDelay(base time.Duration, attempts int) time.Duration {
	d := base
	for i := 1; i < attempts && d < maxAdoptRetryDelay; i++ {
		d *= 2
	}
	return min(d, maxAdoptRetryDelay)
}

// retryAdoption queues the key for the adoption worker after the delay for
// its attempt count. A CONNECT may claim the adoption meanwhile; the pass
// then finds nothing to do.
func (s *Service) retryAdoption(k key, job adoptJob, attempts int) {
	time.AfterFunc(retryDelay(s.o.AdoptRetryDelay, attempts), func() { s.adopts.enqueue(k, job) })
}

// awaitAdoption settles a connecting client's adoption before the client is
// admitted (see adoption.go): it returns the adopted state when this CONNECT
// fetched it, nil when there was nothing to wait for, and a refusal the
// client can retry when the wait outlasts the handshake.
func (s *Service) awaitAdoption(ctx context.Context, k key, cleanStart bool) (*store.SessionState, EmptyEvidence, error) {
	if s.o.AdmitDuringAdoption {
		return nil, EmptyEvidence{}, nil
	}
	for {
		ad, st := s.adopting.claim(k)
		switch decideAdoption(st, cleanStart) {
		case adoptProceed:
			return nil, EmptyEvidence{}, nil
		case adoptDiscard:
			// A clean start discards the session; a sealed copy here goes with
			// the new session's CONNECT.
			s.adopting.end(k, ad)
			s.adoptDiscarded.Add(1)
			return nil, EmptyEvidence{}, nil
		case adoptFetch:
			began := time.Now()
			state, got, wait, _ := s.fetchAdopted(ctx, k, ad, true)
			checked := time.Now()
			switch got {
			case fetchMoved:
				s.adoptInlineMoved.Add(1)
				s.handOn(k)
			case fetchEmpty:
				s.adoptInlineEmpty.Add(1)
			case fetchUnanswered:
				s.adoptInlineUnanswered.Add(1)
				if !s.o.AdmitUnansweredFetch {
					// The adoption stays for this client's retry, and for the
					// worker should the client not come back.
					past := s.notePastBound(k, ad, wait)
					if attempts, mine := s.adopting.giveBack(k, ad, false, false); mine {
						s.retryAdoption(k, ad.job, attempts)
					}
					s.adoptInlineNs.Add(int64(time.Since(began)))
					s.count("hoardmq_lease_ops_total", map[string]string{"op": "acquire", "outcome": "fetch-unanswered"}, 1)
					if past {
						// The leader hears the node this node is waiting on, and
						// a CONNECT there takes the lease and fetches from it.
						if r := s.leaderRedirect(); r != nil {
							s.adoptRedirected.Add(1)
							s.count("hoardmq_lease_ops_total", map[string]string{"op": "acquire", "outcome": "adoption-redirect"}, 1)
							s.count("hoardmq_connect_refusals_total", map[string]string{"reason": "redirect"}, 1)
							return nil, EmptyEvidence{}, r
						}
					}
					return nil, EmptyEvidence{}, &Refusal{Reason: packet.ServerBusy}
				}
			}
			s.endAdoption(k, ad)
			s.adoptedInline.Add(1)
			s.adoptInlineNs.Add(int64(time.Since(began)))
			if state != nil {
				s.noteImport(k, ad, "inline", s.tokens.Seen(k.ns, k.id), checked, time.Now(), time.Now())
				s.confirmLater(k, ad.job.epoch, ad.job.prev)
			}
			var ev EmptyEvidence
			if state == nil && (got == fetchEmpty || got == fetchUnanswered) {
				ev = EmptyEvidence{Reason: EmptyAdoption, Owner: ad.job.prev, Token: ad.job.epoch}
			}
			return state, ev, nil
		case adoptWait:
			began := time.Now()
			select {
			case <-ad.done:
				s.adoptWaited.Add(1)
				s.adoptWaitNs.Add(int64(time.Since(began)))
			case <-ctx.Done():
				s.adoptRefused.Add(1)
				s.count("hoardmq_lease_ops_total", map[string]string{"op": "acquire", "outcome": "adopting"}, 1)
				return nil, EmptyEvidence{}, &Refusal{Reason: packet.ServerBusy}
			case <-s.ctx.Done():
				return nil, EmptyEvidence{}, &Refusal{Reason: packet.ServerUnavailable}
			}
		}
	}
}

// recovered runs once when the core finishes replaying history or
// installing a snapshot: the table is final, so the offline leases this node
// owns get their Will and expiry timers from the lease's recorded times, and
// a local session whose lease belongs to another node — taken over while
// this node was down — is sealed for that node's grant, without a Will.
// Nothing from the replayed history itself is acted on.
func (s *Service) recovered() {
	s.observeAfresh()
	me := s.o.Node
	// Replay ran no change hook, so nothing fed the registry.
	s.table.each(func(l *Lease) { s.tokens.Note(l.Namespace, l.ClientID, l.Epoch) })
	owned, queued := 0, 0
	for _, l := range s.table.OwnedBy(me) {
		if l.Online {
			continue
		}
		s.startTimers(l)
		owned++
		// A reassignment replayed during recovery gave this node the lease and
		// queued nothing, since replay runs no hook. Without this the session
		// is owned here, held nowhere here, and served empty on its next
		// connect — while this node's served fact says it has nothing to adopt.
		if s.o.Fallback == nil {
			continue
		}
		if _, held := s.o.Sessions.Export(l.Namespace, l.ClientID); held {
			continue
		}
		k := key{l.Namespace, l.ClientID}
		job := adoptJob{epoch: l.Epoch, recovered: true}
		s.adopting.begin(k, job)
		s.adopts.enqueue(k, job)
		queued++
	}
	if queued > 0 {
		s.count("hoardmq_lease_adoptions_recovered_total", nil, float64(queued))
		s.log.Info("adoptions queued for leases this node owns and holds no session for", "count", queued)
	}
	evicted, sealed := 0, 0
	s.o.Sessions.Each(func(ns, id string) {
		l, ok := s.table.Lookup(ns, id)
		switch recoveryChoice(ok, l.Owner, me, s.o.RemoveOnHandOver) {
		case evictNow:
			if s.o.Sessions.Evict(ns, id, packet.SessionTakenOver) {
				evicted++
			}
		case evictSeal:
			// The new owner may not have fetched it yet: this node's journal can
			// hold the only copy (a drain whose receiver's pull came after the
			// node stopped). The copy answers that pull, and ends as any sealed
			// copy does (WI 1742).
			if res := s.o.Sessions.Seal(ns, id, packet.SessionTakenOver, l.Epoch); res.Outcome == SealSealed {
				sealed++
				s.sealsMade.Add(1)
				s.sealsByRecovery.Add(1)
				s.log.Debug("session sealed at recovery", "client", id, "owner", l.Owner, "token", l.Epoch)
				s.armSeal(key{ns, id}, l.Epoch)
			}
		}
	})
	if sealed > 0 {
		s.count("hoardmq_lease_seals_total", map[string]string{"by": "recovery"}, float64(sealed))
	}
	s.count("hoardmq_lease_recoveries_total", nil, 1)
	s.audit("lease.recovered", me, "ok", map[string]string{"offlineOwned": fmt.Sprint(owned), "staleEvicted": fmt.Sprint(evicted), "staleSealed": fmt.Sprint(sealed)})
	s.log.Info("leases reconciled after recovery", "offlineOwned", owned, "staleEvicted", evicted, "staleSealed", sealed)
}

// adoptJob is one queued adoption; a pass uses the newest for its key.
type adoptJob struct {
	epoch uint64
	prev  string
	// recovered marks an adoption queued by recovery, not by a change hook.
	recovered bool
	// takeover marks one recorded by a takeover whose replica fetch went
	// unanswered: the lease is online and already this node's.
	takeover bool
}

func (s *Service) serveAdopt(k key, payloads []any) {
	for _, p := range payloads {
		if _, ok := p.(adoptJob); ok {
			// The job to run is the adoption set's: it holds the newest.
			s.adoptState(k)
			return
		}
	}
}

// lostLease detaches the client now and evicts the state later. It runs
// from the lost-lease queue, so the lease may already be back here by the
// time it runs; a client that returned to this node is left alone.
func (s *Service) lostLease(k key) {
	if l, ok := s.table.Lookup(k.ns, k.id); ok && l.Owner == s.o.Node {
		return
	}
	// Whether this detach costs a live connection, the same question
	// serveTakeover answers for its own detach (WI 1495).
	if _, live := s.o.Sessions.ConnAge(k.ns, k.id); live {
		s.lostDetachLive.Add(1)
	} else {
		s.lostDetachGone.Add(1)
	}
	s.o.Sessions.Detach(k.ns, k.id, packet.SessionTakenOver)
	s.scheduleEvict(k, 0)
}

// scheduleEvict arms the deferred reclaim of a session whose lease has gone.
// The decision itself is shouldEvict's; this reads the two facts it needs and
// carries out whichever answer comes back.
//
// It re-arms rather than evicting while a client is connected here. The client
// is one that reconnected to this node after the lease left — lostLease has
// already detached whoever was here at the time — so evicting closes a
// connection made since, and the client has to start again (WI 1513).
func (s *Service) scheduleEvict(k key, deferred int) {
	time.AfterFunc(s.o.TakeoverTimeout*2, func() {
		l, ok := s.table.Lookup(k.ns, k.id)
		ownsLease := ok && l.Owner == s.o.Node
		_, clientHere := s.o.Sessions.ConnAge(k.ns, k.id)
		choice := shouldEvict(ownsLease, !ok, clientHere, deferred)
		if choice == evictKeep {
			return
		}
		if choice == evictDefer {
			s.evictDeferrals.Add(1)
			s.scheduleEvict(k, deferred+1)
			return
		}
		if clientHere {
			// The bound ran out with a client still here: reclaimed anyway,
			// counted apart so a fix that defers for ever cannot look like one
			// that works.
			s.evictBoundHit.Add(1)
		}
		if choice == evictNow || s.o.RemoveOnHandOver {
			s.o.Sessions.Evict(k.ns, k.id, packet.SessionTakenOver)
			return
		}
		// The lease is another node's: sealed, not deleted, until that node
		// confirms it holds the session, the lease comes back, or the bound.
		if res := s.o.Sessions.Seal(k.ns, k.id, packet.SessionTakenOver, l.Epoch); res.Outcome == SealSealed {
			s.sealsMade.Add(1)
			s.sealsByReclaim.Add(1)
			s.count("hoardmq_lease_seals_total", map[string]string{"by": "reclaim"}, 1)
			s.log.Debug("session sealed by the reclaim", "client", k.id, "token", l.Epoch)
			s.armSeal(k, l.Epoch)
		}
	})
}

func (s *Service) cancelTimers(k key) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if a := s.timers[k]; a != nil {
		a.cancel()
		delete(s.timers, k)
	}
}

// startTimers runs an adopted offline lease's Will and expiry.
func (s *Service) startTimers(l Lease) {
	k := key{l.Namespace, l.ClientID}
	now := s.o.Clock()
	a := &adopted{}
	if l.Will != nil && l.Will.Armed {
		at := l.DisconnectedAt.Add(time.Duration(l.Will.Delay) * time.Second)
		if !l.ExpiresAt.IsZero() && l.ExpiresAt.Before(at) {
			at = l.ExpiresAt
		}
		epoch := l.Epoch
		will := l.Will
		a.will = time.AfterFunc(max(at.Sub(now), 0), func() {
			s.o.Sessions.PublishWill(will)
			s.submitAsync(Op{Kind: OpWillDone, Namespace: k.ns, ClientID: k.id, Node: s.o.Node, Epoch: epoch, Now: s.o.Clock()})
			s.audit("lease.will", k.ns+"/"+k.id, "published", nil)
		})
	}
	if !l.ExpiresAt.IsZero() {
		epoch := l.Epoch
		a.expiry = time.AfterFunc(max(l.ExpiresAt.Sub(now), 0), func() {
			s.Release(k.ns, k.id, epoch)
			s.audit("lease.expired", k.ns+"/"+k.id, "released", nil)
		})
	}
	s.mu.Lock()
	if old := s.timers[k]; old != nil {
		old.cancel()
	}
	s.timers[k] = a
	s.mu.Unlock()
}

func (s *Service) count(name string, labels map[string]string, delta float64) {
	if s.o.Metrics != nil {
		s.o.Metrics.Add(name, labels, delta)
	}
}

func (s *Service) audit(action, target, outcome string, detail map[string]string) {
	if s.o.Audit != nil {
		s.o.Audit(action, target, outcome, detail)
	}
}

// avgMs is the mean of a total over a count, reported as 0 rather than NaN
// when nothing was counted: a run with no live detaches has no average, and a
// NaN in the artifact is a value a reader has to special-case (WI 1495).
func avgMs(totalMs, n int64) float64 {
	if n <= 0 {
		return 0
	}
	return float64(totalMs) / float64(n)
}

// AdoptedTakeover is one lease this node adopted and then lost to a takeover,
// with how long it held it. The sample is capped; the count of what did not
// fit is reported beside it, because a truncated capture that says nothing
// reads as a complete one (WI 1511, where a sample reached 937 MB unremarked).
type AdoptedTakeover struct {
	Namespace string `json:"namespace"`
	ClientID  string `json:"client_id"`
	HeldMs    int64  `json:"held_ms"`
	ConnAgeMs int64  `json:"conn_age_ms"`
	Live      bool   `json:"live"`
}

// adoptTakeSampleMax bounds the per-event sample, matching the sim evidence
// sampler's own cap so one run's artifact cannot grow without a stated limit.
const adoptTakeSampleMax = 2000

// recordAdoptTakeover notes that a lease this node adopted has been taken over
// after held, and keeps a bounded sample of the individual events.
func (s *Service) recordAdoptTakeover(k key, held, connAge time.Duration, live bool) {
	ms := held.Milliseconds()
	s.adoptTakeN.Add(1)
	s.adoptTakeSumMs.Add(ms)
	for {
		old := s.adoptTakeMaxMs.Load()
		if ms <= old || s.adoptTakeMaxMs.CompareAndSwap(old, ms) {
			break
		}
	}
	s.sampleMu.Lock()
	if len(s.adoptTakeSeen) < adoptTakeSampleMax {
		s.adoptTakeSeen = append(s.adoptTakeSeen, AdoptedTakeover{
			Namespace: k.ns, ClientID: k.id, HeldMs: ms,
			ConnAgeMs: connAge.Milliseconds(), Live: live,
		})
	} else {
		s.adoptTakeDrop++
	}
	s.sampleMu.Unlock()
}

func (s *Service) adoptTakeoverSample() []AdoptedTakeover {
	s.sampleMu.Lock()
	defer s.sampleMu.Unlock()
	// Appending an empty slice to a nil one yields nil, which is the absent
	// sample the artifact wants. An explicit length check here would be dead
	// code — it was written, and its negative control could not fail.
	return append([]AdoptedTakeover(nil), s.adoptTakeSeen...)
}

func (s *Service) adoptTakeoverDropped() int64 {
	s.sampleMu.Lock()
	defer s.sampleMu.Unlock()
	return s.adoptTakeDrop
}

// noteTakeFrom records the first client takeover this node served for a lease
// whose previous owner was the named node. Bounded by the cluster's size, and
// written once per owner: after that the read finds the key and returns.
func (s *Service) noteTakeFrom(owner string, nowMs int64) {
	s.takeFromMu.Lock()
	if s.firstTakeFrom == nil {
		s.firstTakeFrom = map[string]int64{}
	}
	if _, seen := s.firstTakeFrom[owner]; !seen {
		s.firstTakeFrom[owner] = nowMs
	}
	s.takeFromMu.Unlock()
}

func (s *Service) takeFromSnapshot() map[string]int64 {
	s.takeFromMu.Lock()
	defer s.takeFromMu.Unlock()
	if len(s.firstTakeFrom) == 0 {
		return nil
	}
	out := make(map[string]int64, len(s.firstTakeFrom))
	for k, v := range s.firstTakeFrom {
		out[k] = v
	}
	return out
}

// opNames label proposals by what they are for; most change no ownership.
var opNames = map[OpKind]string{OpAcquire: "acquire", OpDisconnected: "disconnected", OpRelease: "release", OpAlive: "alive",
	OpReassign: "reassign", OpWillDone: "will-done", OpHandoff: "handoff", OpSetLevel: "set-level", OpServed: "served"}

// countProposed counts every operation this node proposes, by kind. An
// acquire that names an epoch is a same-owner renewal and is counted apart:
// it and a disconnect are the proposals that change no ownership.
func (s *Service) countProposed(ops []Op) {
	if s.o.Metrics == nil {
		return
	}
	for _, op := range ops {
		name := opNames[op.Kind]
		if name == "" {
			name = "other"
		}
		if op.Kind == OpAcquire && op.Epoch != 0 {
			name = "renew"
		}
		s.o.Metrics.Add("hoardmq_lease_proposals_total", map[string]string{"op": name}, 1)
	}
}
