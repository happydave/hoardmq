package broker

import (
	"context"
	"errors"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/lease"
	"github.com/happydave/hoardmq/internal/conn"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/session"
	"github.com/happydave/hoardmq/internal/store"
)

// core forwards the connection layer's callbacks to the session manager,
// narrowing *conn.Conn to the manager's Connection interface.
type core struct {
	m       *session.Manager
	leases  *lease.Service // nil until the consensus core is up
	node    string
	timeout time.Duration
	// importGate is Hooks.ImportGate: nil outside tests.
	importGate func(ns, id string)
}

// Connect takes the client's lease before the session manager's connect
// step: a same-owner reconnect renews locally, another owner is taken over
// and its state imported first, and a node that cannot decide refuses with
// 0x88 or redirects with 0x9C and a Server Reference.
func (k core) Connect(c *conn.Conn, p *packet.Connect) conn.ConnectResult {
	if k.leases == nil {
		return k.m.Connect(c, p, 0)
	}
	ns := c.Identity().Namespace
	var will *store.WillRecord
	if w := p.Will; w != nil {
		pub := &packet.Publish{Topic: w.Topic, Payload: w.Payload, QoS: w.QoS, Retain: w.Retain, Properties: w.Properties}
		delay := uint32(0)
		if w.Properties.WillDelayInterval != nil {
			delay = *w.Properties.WillDelayInterval
		}
		will = &store.WillRecord{Message: message.FromPublish(pub, message.Publisher{Namespace: ns, ClientID: c.ClientID(), Username: c.Identity().Username, Node: k.node}, 0, time.Now()), Delay: delay}
	}
	var expiry uint32
	if p.ProtocolLevel == packet.V5 {
		if p.Properties.SessionExpiryInterval != nil {
			expiry = *p.Properties.SessionExpiryInterval
		}
	} else if !p.CleanStart {
		expiry = session.NeverExpires
	}
	ctx, cancel := context.WithTimeout(context.Background(), k.timeout)
	defer cancel()
	res, err := k.leases.AcquireFrom(ctx, ns, c.ClientID(), will, expiry, p.CleanStart, lease.Requester{ConnAge: c.Age(), Prebuffered: c.Prebuffered()})
	if err != nil {
		var r *lease.Refusal
		if errors.As(err, &r) {
			out := conn.ConnectResult{Reason: r.Reason}
			if r.ServerReference != "" {
				ref := r.ServerReference
				out.Properties.ServerReference = &ref
			}
			return out
		}
		return conn.ConnectResult{Reason: packet.ServerUnavailable}
	}
	if res.State != nil {
		// The state came from the previous owner (or a replica of it) and
		// carries that owner's epoch. It is imported — journaled whole, and
		// replicated with the epoch each record was written under — at the
		// epoch this node now holds the lease at, or every follower whose
		// table has already applied the takeover refuses the whole write and
		// its copy never holds the queue.
		res.State.Epoch = res.Epoch
		if k.importGate != nil {
			k.importGate(ns, c.ClientID())
		}
		// A later grant seen here means the lease moved on during this
		// CONNECT; installing would put the session where its lease is not,
		// after a pass here may have answered its new owner that it holds
		// nothing.
		if !k.m.ImportIf(res.State, k.leases.AdmitImport(ns, c.ClientID(), res.Epoch)) {
			k.leases.NoteImportRefused(ns, c.ClientID(), res.Epoch)
			return conn.ConnectResult{Reason: packet.ServerBusy}
		}
	}
	// The epoch goes in through Connect rather than through SetEpoch after it:
	// Connect journals the session, and that record carries the epoch it was
	// written under for the replication tap to stamp.
	var out conn.ConnectResult
	if p.CleanStart && res.MayHaveCopies {
		out = k.m.ConnectReplacing(c, p, res.Epoch)
	} else {
		out = k.m.Connect(c, p, res.Epoch)
	}
	if res.Empty.Reason != "" && !out.SessionPresent && !p.CleanStart && out.Reason == packet.Success {
		k.leases.NoteServedEmpty(ns, c.ClientID(), res.Empty)
	}
	if out.Reason == packet.ImplementationSpecificError {
		// The manager refused after installing the session and left it as a
		// disconnect would; no Disconnect follows a refused CONNECT.
		k.leftOffline(c)
	}
	return out
}
func (k core) Connected(c *conn.Conn) { k.m.Resume(c) }
func (k core) Publish(c *conn.Conn, p *packet.Publish) (packet.ReasonCode, bool) {
	return k.m.Publish(c, p)
}
func (k core) Pubrel(c *conn.Conn, id uint16) (packet.ReasonCode, bool) { return k.m.Pubrel(c, id) }
func (k core) Puback(c *conn.Conn, id uint16, r packet.ReasonCode)      { k.m.Puback(c, id, r) }
func (k core) Pubrec(c *conn.Conn, id uint16, r packet.ReasonCode) (packet.ReasonCode, bool) {
	return k.m.Pubrec(c, id, r)
}
func (k core) Pubcomp(c *conn.Conn, id uint16, r packet.ReasonCode) { k.m.Pubcomp(c, id, r) }
func (k core) Subscribe(c *conn.Conn, p *packet.Subscribe) []packet.ReasonCode {
	return k.m.Subscribe(c, p)
}
func (k core) Subscribed(c *conn.Conn) { k.m.Subscribed(c) }
func (k core) Unsubscribe(c *conn.Conn, p *packet.Unsubscribe) []packet.ReasonCode {
	return k.m.Unsubscribe(c, p)
}
func (k core) Disconnect(c *conn.Conn, graceful bool, r packet.ReasonCode, will bool) {
	k.m.Disconnect(c, graceful, r, will)
	k.leftOffline(c)
}

// leftOffline records on its lease a session the manager left offline with an
// expiry, so the leader can adopt it if this node dies; one that ended
// releases its lease through the manager's Ended callback.
func (k core) leftOffline(c *conn.Conn) {
	if k.leases == nil {
		return
	}
	if rec, ok := k.m.Record(c.ClientID()); ok && !rec.Online && rec.ExpiryInterval != 0 {
		k.leases.Disconnected(rec.Namespace, rec.ClientID, rec.Epoch, rec.ExpiresAt, rec.HasWill)
	}
}
