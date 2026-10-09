package broker

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	stdlog "log"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/happydave/hoardmq/internal/admin"
	"github.com/happydave/hoardmq/internal/audit"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/conn"
	"github.com/happydave/hoardmq/internal/listener"
	"github.com/happydave/hoardmq/internal/module"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/session"
	"github.com/happydave/hoardmq/internal/store"
	"github.com/happydave/hoardmq/internal/trace"
)

// adminNamespace is the namespace every admin call acts on until namespaces
// are activated.
const adminNamespace = "default"

// bindAdmin opens the admin listener, TLS when configured, in Start's bind
// phase. It serves nothing until serveAdmin.
func (s *Server) bindAdmin() error {
	if s.adminAPI == nil {
		return nil
	}
	ln, err := net.Listen("tcp", s.config().Admin.Address)
	if err != nil {
		return fmt.Errorf("admin listener: %w", err)
	}
	if s.config().Admin.TLS != nil {
		tc, err := listener.AdminTLS(s.config().Admin.TLS)
		if err != nil {
			_ = ln.Close()
			return fmt.Errorf("admin listener: %w", err)
		}
		ln = tls.NewListener(ln, tc)
	}
	s.adminLn = ln
	return nil
}

func (s *Server) serveAdmin() {
	if s.adminLn == nil {
		return
	}
	s.adminHTTP = &http.Server{Handler: s.adminAPI, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second,
		WriteTimeout: 60 * time.Second, IdleTimeout: 120 * time.Second, ErrorLog: slogErrorLog(s.log)}
	srv, ln := s.adminHTTP, s.adminLn
	go func() { _ = srv.Serve(ln) }()
	s.log.Info("admin API listening", "address", ln.Addr().String(), "tls", s.config().Admin.TLS != nil)
}

// stopAdmin ends the admin API first, so no request reaches a node that is
// shutting down; it closes a listener that never served.
func (s *Server) stopAdmin() {
	if s.adminHTTP != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := s.adminHTTP.Shutdown(ctx); err != nil {
			_ = s.adminHTTP.Close()
		}
		cancel()
		s.adminHTTP = nil
	} else if s.adminLn != nil {
		_ = s.adminLn.Close()
	}
	s.adminLn = nil
}

// AdminAddr is the bound admin listener, or nil.
func (s *Server) AdminAddr() net.Addr {
	if s.adminLn == nil {
		return nil
	}
	return s.adminLn.Addr()
}

// adminBackend is the admin API's view of the server. A read takes at most
// one session shard's lock, one lease-table key or per-member records; the
// retained list is the query a SUBSCRIBE makes.
type adminBackend struct{ s *Server }

func (b adminBackend) Node() string {
	if b.s.config().Node.Name == "" {
		return "node"
	}
	return b.s.config().Node.Name
}

func summary(r session.Record) admin.ClientSummary {
	out := admin.ClientSummary{ClientID: r.ClientID, Online: r.Online, ProtocolVersion: int(r.Version), Token: r.Epoch,
		ExpiryInterval: r.ExpiryInterval, Subscriptions: r.Subscriptions, InFlight: r.InFlight, Queued: r.Queued,
		Drops: r.Drops, Expired: r.Expired, HasWill: r.HasWill}
	if !r.ExpiresAt.IsZero() {
		at := r.ExpiresAt
		out.ExpiresAt = &at
	}
	return out
}

// ScanClients lists clients from the lease table, which every node holds,
// so a search on any node finds a client wherever it is; a client held here
// carries its session summary. One search walks the table at a time. A
// node with no lease table lists its own sessions.
func (b adminBackend) ScanClients(ctx context.Context, search, after, owner string, limit int) ([]admin.ClientListItem, bool, error) {
	self := b.Node()
	if b.s.leases == nil {
		if owner != "" && owner != self {
			return []admin.ClientListItem{}, false, nil
		}
		recs, more := b.s.sessions.ScanRecords(adminNamespace, search, after, limit)
		out := make([]admin.ClientListItem, 0, len(recs))
		for _, r := range recs {
			sum := summary(r)
			out = append(out, admin.ClientListItem{ClientID: r.ClientID, Node: self, Online: r.Online, Token: r.Epoch,
				ExpiresAt: sum.ExpiresAt, HasWill: r.HasWill, Session: &sum})
		}
		return out, more, nil
	}
	select {
	case b.s.searching <- struct{}{}:
	case <-ctx.Done():
		return nil, false, ctx.Err()
	}
	if gate := b.s.hooks.AdminSearchGate; gate != nil {
		gate()
	}
	leases, more := b.s.leases.Table().Scan(adminNamespace, search, after, owner, limit)
	<-b.s.searching
	out := make([]admin.ClientListItem, 0, len(leases))
	for _, l := range leases {
		item := admin.ClientListItem{ClientID: l.ClientID, Node: l.Owner, Online: l.Online, Token: l.Epoch, HasWill: l.Will != nil}
		if !l.ExpiresAt.IsZero() {
			at := l.ExpiresAt
			item.ExpiresAt = &at
		}
		if l.Owner == self {
			if rec, ok := b.s.sessions.Record(l.ClientID); ok {
				sum := summary(rec)
				item.Session = &sum
			}
		}
		out = append(out, item)
	}
	return out, more, nil
}

// Forward sends an admin request to node, which holds the client it acts on.
func (b adminBackend) Forward(ctx context.Context, node string, req admin.ForwardRequest) (admin.ForwardAnswer, error) {
	if b.s.admins == nil {
		return admin.ForwardAnswer{}, &admin.ForwardError{Outcome: admin.ForwardUnreachable, Reason: "this node has no cluster transport"}
	}
	return b.s.admins.forward(ctx, node, req)
}

// lease reads one client's lease from this node's table.
func (b adminBackend) lease(id string) (admin.Lease, bool) {
	if b.s.leases == nil {
		return admin.Lease{}, false
	}
	l, ok := b.s.leases.Table().Lookup(adminNamespace, id)
	if !ok {
		return admin.Lease{}, false
	}
	out := admin.Lease{ClientID: id, Owner: l.Owner, Token: l.Epoch, Online: l.Online, HasWill: l.Will != nil}
	if !l.ExpiresAt.IsZero() {
		at := l.ExpiresAt
		out.ExpiresAt = &at
	}
	return out, true
}

// notHere is the error for a client this node holds no session for: the
// owner when the lease names another node, else not found.
func (b adminBackend) notHere(id string) error {
	if l, ok := b.lease(id); ok && l.Owner != b.Node() {
		return admin.HeldElsewhere(id, l.Owner)
	}
	return admin.NotFound("no session for client " + id + " on this node")
}

func (b adminBackend) Client(id string) (admin.ClientDetail, error) {
	rec, ok := b.s.sessions.Record(id)
	if !ok {
		return admin.ClientDetail{}, b.notHere(id)
	}
	out := admin.ClientDetail{Session: summary(rec)}
	if c, ok := b.s.sessions.Conn(adminNamespace, id); ok {
		if cc, ok := c.(*conn.Conn); ok {
			out.Connection = &admin.Connection{Listener: cc.Listener(), RemoteAddress: cc.RemoteAddr().String(),
				Username: cc.Identity().Username, ProtocolVersion: int(cc.Version()), ConnectedSeconds: cc.Age().Seconds()}
		}
	}
	if l, ok := b.lease(id); ok {
		out.Lease = &l
	}
	return out, nil
}

func (b adminBackend) Subscriptions(id string) ([]admin.Subscription, error) {
	subs, ok := b.s.sessions.Subscriptions(adminNamespace, id)
	if !ok {
		return nil, b.notHere(id)
	}
	out := make([]admin.Subscription, 0, len(subs))
	for _, sub := range subs {
		out = append(out, admin.Subscription{Filter: sub.Filter, QoS: int(sub.QoS), NoLocal: sub.NoLocal,
			RetainAsPublished: sub.RetainAsPublished, RetainHandling: int(sub.RetainHandling), Identifier: sub.Identifier})
	}
	return out, nil
}

func (b adminBackend) Disconnect(id string) error {
	c, online := b.s.sessions.Conn(adminNamespace, id)
	if !online {
		if rec, held := b.s.sessions.Record(id); held {
			return admin.Conflict(fmt.Sprintf("client %s is offline; its session is kept (expiry interval %d s)", id, rec.ExpiryInterval))
		}
		return b.notHere(id)
	}
	c.Close(packet.AdministrativeAction)
	return nil
}

// invalidateAdmitted decides whether an operator's invalidation may discard
// the session this node holds for a client. It is pure. The discard deletes
// the session here and, through the replicated journal delete, on the
// replicas this node feeds, so a copy whose lease has moved must not be
// discarded: it may be the one its new owner is about to fetch. held is the
// token the session is held at and seen the highest grant token this node
// has seen for the client; leased and owner are the lease table's.
func invalidateAdmitted(seen, held uint64, leased bool, owner, self string) bool {
	if seen > held {
		return false
	}
	return !leased || owner == self
}

func (b adminBackend) Invalidate(ctx context.Context, id string) (admin.Invalidated, error) {
	s := b.s
	l, leased := b.lease(id)
	admit := func(token uint64) bool {
		var seen uint64
		if s.leases != nil {
			seen = s.leases.Tokens().Seen(adminNamespace, id)
		}
		return invalidateAdmitted(seen, token, leased, l.Owner, b.Node())
	}
	res := s.sessions.Invalidate(adminNamespace, id, packet.AdministrativeAction, admit)
	switch {
	case !res.Found:
		return admin.Invalidated{}, b.notHere(id)
	case res.Refused && leased && l.Owner != b.Node():
		return admin.Invalidated{}, admin.HeldElsewhere(id, l.Owner)
	case res.Refused:
		return admin.Invalidated{}, admin.Conflict("a later grant of client " + id + " has been seen; its session is moving to another node")
	}
	return admin.Invalidated{ClientID: id, WasOnline: res.WasOnline, WillPublished: res.WillPublished, Durable: b.durable(ctx, res.Scope)}, nil
}

// durable waits for a change's records to be synced here and held by the
// live replicas, for at most the replication acknowledgement timeout.
func (b adminBackend) durable(ctx context.Context, scope store.Scope) bool {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(b.s.config().Cluster.Replication.AckTimeoutMs)*time.Millisecond)
	defer cancel()
	return b.s.sessions.Durable(ctx, []store.Scope{scope}) == nil
}

func (b adminBackend) Matchers(topic string) []admin.Matcher {
	var out []admin.Matcher
	for _, m := range b.s.router.Matchers(adminNamespace, topic) {
		out = append(out, admin.Matcher{ClientID: m.ClientID, Filter: m.Filter, Group: m.Group, QoS: int(m.QoS)})
	}
	return out
}

func (b adminBackend) Retained(filter, after string, limit int) ([]admin.RetainedMessage, bool) {
	msgs := b.s.sessions.QueryRetained(adminNamespace, filter)
	sort.Slice(msgs, func(i, j int) bool { return msgs[i].Topic < msgs[j].Topic })
	var out []admin.RetainedMessage
	for _, m := range msgs {
		if m.Topic <= after {
			continue
		}
		if len(out) == limit {
			return out, true
		}
		rm := admin.RetainedMessage{Topic: m.Topic, QoS: int(m.QoS), Payload: m.Payload, PayloadBytes: len(m.Payload)}
		if m.ContentType != nil {
			rm.ContentType = *m.ContentType
		}
		out = append(out, rm)
	}
	return out, false
}

func (b adminBackend) DeleteRetained(ctx context.Context, topic string) (admin.Durability, error) {
	scope, appended := b.s.sessions.DeleteRetained(adminNamespace, topic)
	if !appended {
		return admin.Durability{}, nil
	}
	return admin.Durability{Durable: b.durable(ctx, scope)}, nil
}

func (b adminBackend) Cluster() admin.ClusterState {
	s := b.s
	ready, reason := s.Readiness()
	serving, servingReason := s.Serving()
	out := admin.ClusterState{Node: b.Node(), Ready: ready, ReadyReason: reason, Serving: serving, ServingReason: servingReason,
		Drain: drainStatus(s.DrainStatus()), Members: []admin.Member{}}
	if s.consensus != nil {
		st := s.consensus.Status()
		out.Consensus = admin.ConsensusView{Role: st.Role, Leader: st.Leader, Term: st.Term, Commit: st.Commit, Applied: st.Applied}
		members := s.consensus.Members()
		sort.Slice(members, func(i, j int) bool { return members[i].Name < members[j].Name })
		for _, m := range members {
			am := admin.Member{Name: m.Name, Role: string(m.Role), CaughtUp: m.CaughtUp}
			if s.leases != nil {
				am.Alive = s.leases.Table().Live(m.Name)
				if n, ok := s.leases.Table().Node(m.Name); ok {
					am.Advertise, am.Level, am.DeclaredDeadAt = n.Advertise, n.Level, n.DeadAt
					am.ServedIndex, am.ServedRoutes = n.ServedIndex, n.ServedRoutes
				}
			}
			out.Members = append(out.Members, am)
		}
	}
	if s.leases != nil {
		out.FeatureLevel.Cluster, out.FeatureLevel.Build, out.FeatureLevel.MinAdvertised = s.leases.Levels()
		out.Leases = s.leases.Table().Count()
		out.Sealed.Held, out.Sealed.Made, out.Sealed.Confirmed, out.Sealed.Expired = s.leases.SealCounts()
	} else if s.sessions != nil {
		out.Sealed.Held = s.sessions.SealedCount()
	}
	out.ShardMap = b.Shards().ShardSummary
	return out
}

func (b adminBackend) Shards() admin.ShardMap {
	if b.s.repl == nil {
		return admin.ShardMap{ShardSummary: admin.ShardSummary{Basis: []string{}}, Assignments: []admin.ShardAssignment{}}
	}
	m := b.s.repl.Map()
	cur := m.Current()
	out := admin.ShardMap{ShardSummary: admin.ShardSummary{Version: cur.Version, Shards: m.Count(), Replicas: cur.Replicas, Basis: append([]string{}, cur.Basis...)},
		Assignments: make([]admin.ShardAssignment, 0, len(cur.Shards))}
	for i, sh := range cur.Shards {
		out.Assignments = append(out.Assignments, admin.ShardAssignment{Shard: i, Leader: sh.Leader, Followers: append([]string(nil), sh.Followers...)})
	}
	return out
}

func (b adminBackend) Lease(id string) (admin.Lease, error) {
	if l, ok := b.lease(id); ok {
		return l, nil
	}
	return admin.Lease{}, admin.NotFound("no lease for client " + id)
}

func drainStatus(d module.DrainStatus) admin.DrainStatus {
	return admin.DrainStatus{Draining: d.Draining, Phase: d.Phase, ClientsRemaining: d.ClientsRemaining, LeasesRemaining: d.LeasesRemaining,
		SessionsAwaiting: d.SessionsAwaiting, SessionsConfirmed: d.SessionsConfirmed, SessionsUnconfirmed: d.SessionsUnconfirmed, CopiesKept: d.CopiesKept, Complete: d.Complete}
}

func (b adminBackend) DrainStatus() admin.DrainStatus { return drainStatus(b.s.DrainStatus()) }

func (b adminBackend) Drain() admin.DrainStatus {
	b.s.Drain()
	return drainStatus(b.s.DrainStatus())
}

func (b adminBackend) RunningConfig() ([]byte, error) { return json.Marshal(b.s.config()) }

func (b adminBackend) ValidateConfig(doc []byte) []admin.ConfigProblem {
	parsed, problems := config.Parse(doc, nil)
	if len(problems) == 0 {
		problems = ValidateModules(parsed, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}
	out := make([]admin.ConfigProblem, 0, len(problems))
	for _, p := range problems {
		out = append(out, admin.ConfigProblem{Pointer: p.Pointer, Message: p.Message})
	}
	return out
}

func (b adminBackend) ReloadConfig() admin.ConfigReload {
	r := b.s.Reload()
	out := admin.ConfigReload{Applied: r.Applied, RestartRequired: r.RestartRequired, Changed: r.Changed, Problems: []admin.ConfigProblem{}}
	if out.Changed == nil {
		out.Changed = []string{}
	}
	for _, p := range r.Problems {
		out.Problems = append(out.Problems, admin.ConfigProblem{Pointer: p.Pointer, Message: p.Message})
	}
	return out
}

// traceConn is the connection a recording is taken on: a client connected
// here, found through its session's shard.
func (b adminBackend) traceConn(id string) (*conn.Conn, error) {
	if c := b.s.connected(id); c != nil {
		return c, nil
	}
	if _, held := b.s.sessions.Record(id); held {
		return nil, admin.Conflict("client " + id + " is not connected")
	}
	return nil, b.notHere(id)
}

func (b adminBackend) StartTrace(id string, limit int) (admin.TraceStarted, error) {
	c, err := b.traceConn(id)
	if err != nil {
		return admin.TraceStarted{}, err
	}
	if limit == 0 {
		limit = b.s.config().Observability.TraceEvents
	}
	c.StartTrace(limit)
	return admin.TraceStarted{ClientID: id, Limit: limit}, nil
}

func traceEvents(c *conn.Conn, stop bool) []admin.TraceEvent {
	var evs []trace.Event
	if stop {
		evs = c.StopTrace()
	} else {
		evs = c.Trace()
	}
	out := make([]admin.TraceEvent, 0, len(evs))
	for _, e := range evs {
		out = append(out, admin.TraceEvent{Time: e.Time, Direction: string(e.Direction), Type: e.Type, PacketID: e.PacketID, Topic: e.Topic, Size: e.Size, Reason: e.Reason})
	}
	return out
}

func (b adminBackend) StopTrace(id string) ([]admin.TraceEvent, error) {
	c, err := b.traceConn(id)
	if err != nil {
		return nil, err
	}
	return traceEvents(c, true), nil
}

func (b adminBackend) Trace(id string) ([]admin.TraceEvent, error) {
	c, err := b.traceConn(id)
	if err != nil {
		return nil, err
	}
	return traceEvents(c, false), nil
}

func (b adminBackend) Modules() []admin.Module {
	failures := b.s.registry.Hooks().Failures()
	var out []admin.Module
	for _, m := range b.s.registry.Describe() {
		out = append(out, admin.Module{ID: string(m.ID), Family: m.ID.Family(), Enabled: m.Enabled, Toggleable: m.Toggleable, HookFailures: failures[m.ID]})
	}
	return out
}

func (b adminBackend) SetModuleEnabled(id string, enabled bool) error {
	for _, m := range b.s.registry.Describe() {
		if string(m.ID) != id {
			continue
		}
		if !m.Toggleable {
			return admin.Conflict("module " + id + " cannot be enabled or disabled while the broker runs")
		}
		b.s.registry.SetEnabled(m.ID, enabled)
		return nil
	}
	return admin.NotFound("module " + id + " is not instantiated")
}

// Records is the replicated records store once the core is open.
func (b adminBackend) Records() admin.RecordStore {
	if st := b.s.records.Load(); st != nil {
		return st
	}
	return nil
}

func (b adminBackend) Limits() admin.LimitsView {
	v := admin.LimitsView{Namespace: config.DefaultLimitsKey, Configured: admin.RatesOf(b.s.config().Limits), InForce: admin.RatesOf(b.s.config().Limits)}
	if st := b.s.records.Load(); st != nil {
		if rec, ok := st.Get(config.LimitsCollection, config.DefaultLimitsKey); ok {
			_ = json.Unmarshal(rec.Value, &v.Administered)
			v.InForce, v.Version = admin.RatesOf(b.s.config().Limits.With(v.Administered)), rec.Version
		}
	}
	return v
}

func (b adminBackend) Overload() admin.OverloadView {
	st := b.s.overload.Stats()
	lim := b.s.overload.lim.Load()
	v := admin.OverloadView{NodeLevel: st.NodeLevel, ClusterLevel: st.ClusterLevel, Shed: st.Shed,
		Indicators: map[string]int64{"queued_messages": st.Indicators.Queued, "replication_lag": st.Indicators.Lag, "heap_bytes": st.Indicators.Heap},
		Limits:     map[string]int64{"queued_messages": lim.Queued, "replication_lag": lim.Lag, "heap_bytes": lim.Heap},
		Peers:      map[string]uint8{}}
	if b.s.cluster != nil {
		for _, p := range b.s.cluster.Peers() {
			v.Peers[p.Name] = p.Stress
		}
	}
	return v
}

func (b adminBackend) AuditTail(n int) ([]admin.AuditEvent, error) {
	a := b.s.audit.Load()
	if a == nil {
		return nil, admin.Conflict("the audit log is off (observability.audit.enabled)")
	}
	if err := a.Flush(); err != nil && !errors.Is(err, audit.ErrClosed) {
		return nil, err
	}
	evs, err := a.Tail(n)
	if err != nil {
		return nil, err
	}
	out := make([]admin.AuditEvent, 0, len(evs))
	for _, e := range evs {
		out = append(out, admin.AuditEvent{Time: e.Time, Actor: e.Actor, Action: e.Action, Target: e.Target, Outcome: e.Outcome, Detail: e.Detail})
	}
	return out, nil
}

func (b adminBackend) Audit(e admin.AuditEvent, lossy bool) {
	a := b.s.audit.Load()
	if a == nil {
		return
	}
	ev := audit.Event{Time: e.Time, Actor: e.Actor, Action: e.Action, Target: e.Target, Outcome: e.Outcome, Detail: e.Detail}
	if lossy {
		_ = a.RecordLossy(ev)
		return
	}
	_ = a.Record(ev)
}

// connected is the connection of a client whose session is held here, found
// through the session's shard: never a walk of every connection under the
// server's mutex, which the accept loop takes for each new connection.
func (s *Server) connected(clientID string) *conn.Conn {
	if s.sessions == nil {
		return nil
	}
	c, ok := s.sessions.Conn(adminNamespace, clientID)
	if !ok {
		return nil
	}
	cc, _ := c.(*conn.Conn)
	return cc
}

// slogErrorLog routes net/http's own error lines (TLS handshake failures,
// malformed requests) to the broker's log at Debug.
func slogErrorLog(log *slog.Logger) *stdlog.Logger { return stdlog.New(&logAdapter{log: log}, "", 0) }

type logAdapter struct{ log *slog.Logger }

func (l *logAdapter) Write(p []byte) (int, error) {
	l.log.Debug("admin http", "msg", strings.TrimSpace(string(p)))
	return len(p), nil
}
