package broker

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
	"github.com/happydave/hoardmq/internal/cluster/consensus/etcdraft"
	"github.com/happydave/hoardmq/internal/cluster/consensus/raftlog"
	"github.com/happydave/hoardmq/internal/cluster/discovery"
	"github.com/happydave/hoardmq/internal/cluster/lease"
	"github.com/happydave/hoardmq/internal/cluster/records"
	"github.com/happydave/hoardmq/internal/cluster/replication"
	"github.com/happydave/hoardmq/internal/cluster/routing"
	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/conn"
	"github.com/happydave/hoardmq/internal/hook"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/session"
	storepkg "github.com/happydave/hoardmq/internal/store"
	"github.com/happydave/hoardmq/internal/wal"
)

// newTransport builds the cluster transport from the configuration, reading
// the certificate, CA or secret files it names. It returns nil when no
// transport listener is configured: a one-member cluster with nothing to
// talk to.
func newTransport(cfg config.Document, log *slog.Logger, metrics hook.Metrics, audit func(action, target, outcome string, detail map[string]string), reorder bool, maxVersion uint8) (*transport.Transport, error) {
	c := cfg.Cluster
	if c.Listen == "" {
		return nil, nil
	}
	o := transport.Options{
		Node:             cfg.Node.Name,
		ClusterID:        c.ID,
		Listen:           c.Listen,
		Peers:            c.Peers,
		Heartbeat:        time.Duration(c.HeartbeatMs) * time.Millisecond,
		HeartbeatMisses:  c.HeartbeatMisses,
		MaxFrame:         c.MaxFrameBytes,
		HandshakeTimeout: time.Duration(cfg.Limits.ConnectTimeout) * time.Second,
		Logger:           log,
		Metrics:          metrics,
		Audit:            audit,
		Reorder:          reorder,
		MaxVersion:       maxVersion,
	}
	if o.Node == "" {
		// A listener with no peers still needs a name to introduce itself.
		o.Node = "node"
	}
	switch c.Auth.Mode {
	case "mtls":
		if c.Auth.TLS == nil {
			return nil, errors.New("cluster: mtls requires auth.tls")
		}
		cert, err := tls.LoadX509KeyPair(c.Auth.TLS.CertFile, c.Auth.TLS.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("cluster certificate: %w", err)
		}
		pemBytes, err := os.ReadFile(c.Auth.TLS.CAFile)
		if err != nil {
			return nil, fmt.Errorf("cluster CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pemBytes) {
			return nil, errors.New("cluster CA: no certificate in the file")
		}
		o.Auth = transport.Auth{Mode: "mtls", Cert: cert, CA: pool}
	case "secret":
		secret, err := os.ReadFile(c.Auth.SecretFile)
		if err != nil {
			return nil, fmt.Errorf("cluster secret: %w", err)
		}
		o.Auth = transport.Auth{Mode: "secret", Secret: bytes.TrimSpace(secret)}
	}
	return transport.New(o)
}

// Cluster exposes the transport, for the cluster layers and tests. It is
// nil when no cluster listener is configured.
func (s *Server) Cluster() *transport.Transport { return s.cluster }

// openConsensus opens the Raft log under the data directory and starts the
// consensus core. Every node runs one: with no peers it is a one-member
// group. Called after the data directory exists.
func (s *Server) openConsensus() error {
	peers, err := discovery.Peers(*s.config())
	if err != nil {
		return err
	}
	store, err := raftlog.Open(filepath.Join(s.config().Node.DataDir, "logs"), "raft", wal.Options{SegmentBytes: s.config().Node.WALSegmentBytes, Logger: s.log, Sync: s.hooks.Sync, IgnoreChecksum: s.hooks.Defects.IgnoreChecksum})
	if err != nil {
		return fmt.Errorf("raft log: %w", err)
	}
	r := s.config().Cluster.Raft
	o := etcdraft.Options{
		Node:             s.config().Node.Name,
		Peers:            peers,
		Store:            store,
		Machine:          consensus.NewMachine(),
		Tick:             time.Duration(r.TickMs) * time.Millisecond,
		ElectionTicks:    r.ElectionTicks,
		HeartbeatTicks:   r.HeartbeatTicks,
		SnapshotEntries:  uint64(r.SnapshotEntries),
		SnapshotTrailing: uint64(r.SnapshotTrailing),
		PromoteLag:       uint64(r.PromoteLag),
		JoinTimeout:      time.Duration(r.JoinTimeoutMs) * time.Millisecond,
		Bootstrap:        r.Bootstrap,
		SendBeforeSave:   s.hooks.Defects.SendBeforeSave,
		HoldAdmissions:   s.hooks.HoldAdmissions,
		Logger:           s.log,
		Metrics:          s.metrics,
		Audit: func(action, target, outcome string, detail map[string]string) {
			s.record("cluster", action, target, outcome, detail)
		},
	}
	if o.Node == "" {
		o.Node = "node"
	}
	if s.cluster != nil {
		o.Transport = s.cluster
	}
	rc, err := etcdraft.New(o)
	if err != nil {
		_ = store.Close()
		return err
	}
	// The lease table registers with the machine before the core applies
	// anything. The transport is passed only when the node has peers.
	lo := lease.Options{
		Node: o.Node, Core: rc, Machine: o.Machine, Sessions: leaseSessions{s},
		TTL:                  time.Duration(s.config().Cluster.Leases.TTLMs) * time.Millisecond,
		TakeoverTimeout:      time.Duration(s.config().Cluster.Leases.TakeoverTimeoutMs) * time.Millisecond,
		FallbackTimeout:      time.Duration(s.config().Cluster.Replication.AckTimeoutMs) * time.Millisecond,
		Advertise:            s.config().Cluster.Advertise,
		Clock:                s.hooks.Clock,
		Steady:               s.hooks.Steady,
		MinorityServes:       s.hooks.Defects.MinorityServes,
		AdoptionDelay:        s.hooks.AdoptionDelay,
		TakeoverDelay:        s.hooks.TakeoverDelay,
		ImportGate:           s.hooks.ImportGate,
		AcquireGate:          s.hooks.AcquireGate,
		ImportPastLaterGrant: s.hooks.Defects.ImportPastLaterGrant,
		BuildLevel:           s.hooks.BuildLevel,
		AdmitDuringAdoption:  s.hooks.Defects.AdmitDuringAdoption,
		AdmitUnansweredFetch: s.hooks.Defects.AdmitUnansweredFetch,
		DiscardLateState:     s.hooks.Defects.DiscardLateState,
		RemoveOnHandOver:     s.hooks.Defects.RemoveOnHandOver,
		Logger:               s.log, Metrics: s.metrics,
		Audit: func(action, target, outcome string, detail map[string]string) {
			s.record("lease", action, target, outcome, detail)
		},
	}
	if s.cluster != nil {
		lo.Transport = s.cluster
	}
	// The replication layer registers the shard map with the machine and
	// hands the lease service its replica fallback; both before Start.
	replica, err := storepkg.Open(filepath.Join(s.config().Node.DataDir, "replica"), storepkg.Options{
		SyncPolicy: storepkg.Commit, SegmentBytes: s.config().Node.WALSegmentBytes, Logger: s.log,
		Sync: s.hooks.Sync, IgnoreChecksum: s.hooks.Defects.IgnoreChecksum})
	if err != nil {
		_ = store.Close()
		return fmt.Errorf("replica store: %w", err)
	}
	var svc *lease.Service
	ro := replication.Options{
		Node: o.Node, Map: shardmap.New(s.config().Cluster.Shards), Core: rc, Sessions: s.sessions, Local: s.router, Replica: replica,
		ReplicaDir:               filepath.Join(s.config().Node.DataDir, "replica"),
		Replicas:                 s.config().Cluster.Replicas,
		AckTimeout:               time.Duration(s.config().Cluster.Replication.AckTimeoutMs) * time.Millisecond,
		TTL:                      lo.TTL,
		Clock:                    s.hooks.Clock,
		SkipEpochCheck:           s.hooks.Defects.SkipEpochCheck,
		IgnoreGaps:               s.hooks.Defects.IgnoreReplicationGaps,
		DeleteWithoutHandoverAck: s.hooks.Defects.DeleteWithoutHandoverAck,
		AcknowledgeUnapplied:     s.hooks.Defects.AcknowledgeUnapplied,
		HandOverOwnedCopies:      s.hooks.Defects.HandOverOwnedCopies,
		KeepUnfedCopies:          s.hooks.Defects.KeepUnfedCopies,
		DropMovedRetained:        s.hooks.Defects.DropMovedRetained,
		KeepRetainedNotHandedOn:  s.hooks.Defects.KeepRetainedNotHandedOn,
		IgnoreOwedHandOver:       s.hooks.Defects.IgnoreOwedHandOver,
		NoHandOverRetry:          s.hooks.Defects.NoHandOverRetry,
		NoEarlyReleaseDrop:       s.hooks.Defects.NoEarlyReleaseDrop,
		NoHandOverAtLeaderChange: s.hooks.Defects.NoHandOverAtLeaderChange,
		NoBarrier:                s.hooks.Defects.NoBarrier,
		Logger:                   s.log, Metrics: s.metrics,
		Audit: func(action, target, outcome string, detail map[string]string) {
			s.record("replication", action, target, outcome, detail)
		},
	}
	if s.cluster != nil {
		ro.Transport = s.cluster
	}
	// The lease table exists once the service does; build the service first
	// with a fallback that reaches the replicator through the closure.
	var repl *replication.Replicator
	lo.Fallback = func(ctx context.Context, ns, id, prevOwner string) lease.Fetched {
		if repl == nil {
			return lease.Fetched{}
		}
		f := repl.FetchKept(ctx, ns, id, prevOwner)
		return lease.Fetched{State: f.State, Answered: f.Answered, Down: f.Down, Kept: f.Kept, KeptFrom: f.KeptFrom}
	}
	// A new owner's confirmation also reaches every other live member it does
	// not feed: a holder keeps a copy from an earlier grant, an old target a
	// moved copy and a previous owner a seal until it arrives, and a draining
	// node waits for it before it stops.
	lo.Unfed = func(ns, id string) []string {
		if repl == nil {
			return nil
		}
		return repl.Unfed(ns, id)
	}
	// A previous owner's sealed copy that nothing confirmed ends once one of
	// the new owner's targets holds the session at its grant.
	lo.HeldAtTargets = func(ctx context.Context, ns, id, owner string, token uint64) uint64 {
		if repl == nil {
			return 0
		}
		return repl.HeldAtTargets(ctx, ns, id, owner, token)
	}
	lo.Confirmed = func(ns, id, from string, token uint64) {
		if repl != nil {
			repl.NoteWritten(ns, id, from, token)
		}
		s.drainer.noteConfirmed(ns, id, token)
	}
	lo.NextPublishSeq = func() uint64 { return s.sessions.NextPublishSeq() }
	if s.routes != nil {
		lo.MoveEnded = s.routes.Resolve
	}
	// A declaration takes a node out of the shard map's basis; the leader's
	// driver proposes the map without it now rather than at its next check.
	lo.Declared = func(string) {
		if repl != nil {
			repl.BasisChanged()
		}
	}
	// A drain's hand-off marks its node leaving, which takes it out of the
	// basis the same way.
	lo.Leaving = func(string) {
		if repl != nil {
			repl.BasisChanged()
		}
	}
	// A served fact applied, or this node's last adoption ended: a lost
	// owner's routes may be forgettable now, or this node's own fact due.
	if s.routes != nil {
		lo.Served = func(string) { s.routes.WakeServed() }
		lo.AdoptionsIdle = s.routes.WakeServed
		lo.TakesEnded = s.routes.WakeServed
	}
	recs := records.New(0)
	o.Machine.Register(records.Kind, recs)
	svc, err = lease.New(lo)
	if err != nil {
		_ = store.Close()
		_ = replica.Close()
		return err
	}
	ro.Leases = svc.Table()
	ro.Tokens = svc.Tokens()
	ro.Level, ro.BuildLevel = svc.Table().Level, svc.BuildLevel
	ro.ConfirmMoved = svc.ConfirmMoved
	repl, err = replication.New(ro, o.Machine)
	if err != nil {
		_ = store.Close()
		_ = replica.Close()
		return err
	}
	if err := rc.Start(); err != nil {
		_ = store.Close()
		_ = replica.Close()
		return err
	}
	svc.Start()
	repl.Start()
	if tapper, ok := s.store.(interface {
		SetTap(func(storepkg.Record) func())
	}); ok {
		tapper.SetTap(repl.Tap)
	} else {
		s.log.Warn("store provider has no record tap; sessions are not replicated")
	}
	s.sessions.SetReplication(dataPlane{repl: repl, routes: s.routes, timeout: ro.AckTimeout})
	s.consensus = rc
	s.leases = svc
	s.coreReady.Store(rc)
	s.leasesReady.Store(svc)
	s.records.Store(&records.Store{Component: recs, Propose: svc.ProposeCommand})
	s.replica = replica
	s.repl = repl
	s.core = core{m: s.sessions, leases: svc, node: o.Node, timeout: lo.TakeoverTimeout, importGate: s.hooks.ImportGate}
	return nil
}

// Replication exposes the replication layer, for the cluster layers and
// tests.
func (s *Server) Replication() *replication.Replicator { return s.repl }

// Leases exposes the lease service, for the cluster layers and tests.
func (s *Server) Leases() *lease.Service { return s.leases }

// leaseSessions adapts the session manager and the connection table to what
// the lease service needs.
type leaseSessions struct{ s *Server }

func (l leaseSessions) Export(ns, id string) (*storepkg.SessionState, bool) {
	return l.s.sessions.Export(ns, id)
}
func (l leaseSessions) ImportIf(st *storepkg.SessionState, admit func() bool) bool {
	return l.s.sessions.ImportIf(st, admit)
}
func (l leaseSessions) Each(fn func(ns, id string)) {
	for _, rec := range l.s.sessions.Records() {
		fn(rec.Namespace, rec.ClientID)
	}
}
func (l leaseSessions) Take(ns, id string, r packet.ReasonCode) (*storepkg.SessionState, bool) {
	return l.s.sessions.Take(ns, id, r)
}
func (l leaseSessions) Evict(ns, id string, r packet.ReasonCode) bool {
	return l.s.sessions.Evict(ns, id, r)
}
func (l leaseSessions) Seal(ns, id string, r packet.ReasonCode, token uint64) lease.SealResult {
	res := l.s.sessions.Seal(ns, id, r, token)
	out := lease.SealResult{State: res.State, Token: res.Token, Raised: res.Raised}
	switch res.Outcome {
	case session.SealSealed:
		out.Outcome = lease.SealSealed
	case session.SealAlready:
		out.Outcome = lease.SealAlready
	case session.SealNewer:
		out.Outcome = lease.SealNewer
	}
	return out
}
func (l leaseSessions) Sealed(ns, id string) (*storepkg.SessionState, uint64, bool) {
	return l.s.sessions.Sealed(ns, id)
}
func (l leaseSessions) DropSealed(ns, id string, upTo uint64) bool {
	return l.s.sessions.DropSealed(ns, id, upTo)
}
func (l leaseSessions) EvictOffline(ns, id string, upTo uint64) bool {
	return l.s.sessions.EvictOffline(ns, id, upTo)
}
func (l leaseSessions) HeldAt(ns, id string) (uint64, bool) { return l.s.sessions.HeldAt(ns, id) }
func (l leaseSessions) SealedCount() int                    { return l.s.sessions.SealedCount() }
func (l leaseSessions) Durable(ctx context.Context, keys []storepkg.Scope) error {
	return l.s.sessions.Durable(ctx, keys)
}
func (l leaseSessions) Detach(ns, id string, r packet.ReasonCode) { l.s.sessions.Detach(ns, id, r) }
func (l leaseSessions) SetEpoch(ns, id string, epoch uint64)      { l.s.sessions.SetEpoch(ns, id, epoch) }
func (l leaseSessions) ConnAge(ns, id string) (time.Duration, bool) {
	return l.s.sessions.ConnAge(ns, id)
}

// PublishWill publishes an adopted session's Will as the node itself.
func (l leaseSessions) PublishWill(w *storepkg.WillRecord) {
	if w != nil && w.Message != nil {
		l.s.sessions.Inject(w.Message)
	}
}

// FenceAll closes every client connection: this node's leases lapsed and it
// may not serve until it renews.
func (l leaseSessions) FenceAll(reason packet.ReasonCode) {
	l.s.mu.Lock()
	conns := make([]*conn.Conn, 0, len(l.s.conns))
	for c := range l.s.conns {
		conns = append(conns, c)
	}
	l.s.mu.Unlock()
	for _, c := range conns {
		c.Close(reason)
	}
	l.s.record("lease", "lease.fence", l.s.config().Node.Name, "disconnected", map[string]string{"connections": fmt.Sprint(len(conns))})
}

// Consensus exposes the core, for the cluster layers and tests. It is nil
// until Start has opened it; the field is returned through an explicit nil so
// a caller's nil check sees one, not an interface around a nil pointer.
func (s *Server) Consensus() consensus.Core {
	if s.consensus == nil {
		return nil
	}
	return s.consensus
}

// Routes exposes the routing plane, for the cluster layers and tests. It is
// nil when no cluster listener is configured.
func (s *Server) Routes() *routing.Plane { return s.routes }
