package session

import "github.com/happydave/hoardmq/internal/store"

// wholeWriter is a Replication that sends a session's whole write as one
// unit: its records are journaled here as always, and the replicas are sent
// the session whole once the last of them is appended.
type wholeWriter interface {
	// OpenWhole begins a whole write of a session; the close it returns,
	// called before the session's lock is released, takes the state written
	// and whether every record appended. Nil: the records go as records.
	OpenWhole(ns, id, cause string) func(st *store.SessionState, appended bool)
	// SessionLeft says a session whose whole write did not append is no longer
	// this node's.
	SessionLeft(ns, id string)
}

// Why a session is written whole.
const (
	wholeImport     = "import"
	wholeRewrite    = "rewrite"
	wholeCleanStart = "clean-start"
	wholeCreate     = "create"
)

// openWhole opens a whole write of the session at k on the replication
// layer: nil when there is none, or it sends records.
func (m *Manager) openWhole(k key, cause string) func(*store.SessionState, bool) {
	ww, ok := m.Replication().(wholeWriter)
	if !ok {
		return nil
	}
	return ww.OpenWhole(k.ns, k.id, cause)
}

// sessionLeft tells the replication layer a session armed for a rewrite is no
// longer this node's.
func (m *Manager) sessionLeft(k key) {
	if ww, ok := m.Replication().(wholeWriter); ok {
		ww.SessionLeft(k.ns, k.id)
	}
}

// countCreation counts a new persistent session by the form its first write
// reached the replicas in, where there are replicas to reach.
func (m *Manager) countCreation(whole bool) {
	if _, ok := m.Replication().(wholeWriter); !ok {
		return
	}
	form := "lone"
	if whole {
		form = "snapshot"
	}
	m.count("hoardmq_session_creations_total", map[string]string{"form": form}, 1)
}
