package session

import (
	"strconv"

	"github.com/happydave/hoardmq/internal/auth"
)

// QueueLimitAttribute is the identity attribute an authenticator sets to bound
// its principal's session queues below the configured limit.
const QueueLimitAttribute = "hoardmq.maxQueuedMessages"

// queueBound is the queue bound in force for a session: the configured
// limit, lowered to the principal's own when it sets one. A principal's
// limit never raises it; with no configured limit (0) the principal's alone
// bounds the queue.
func queueBound(configured, principal int) int {
	switch {
	case principal <= 0:
		return configured
	case configured <= 0 || principal < configured:
		return principal
	}
	return configured
}

// queueLimitOf reads the principal's queue limit from the session's
// identity: a positive decimal count, or none. A value that is not one is
// counted under the session's namespace, like its other series, and ignored,
// so the configured limit applies.
func (m *Manager) queueLimitOf(s *Session) int {
	v, ok := s.identity.Attributes[QueueLimitAttribute]
	if !ok {
		return 0
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		m.count("hoardmq_session_queue_limit_invalid_total", map[string]string{"namespace": s.key.ns}, 1)
		return 0
	}
	return n
}

// Reidentify gives the session attached to c the identity id, for its Will
// and its queue bound, when a re-decision is about to close c. A session no
// longer attached to c (a takeover came first) is left as it is.
func (m *Manager) Reidentify(c Connection, id auth.Identity) {
	s := m.lookup(c)
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != c {
		return
	}
	s.identity = id
	s.queueLimit = m.queueLimitOf(s)
}
