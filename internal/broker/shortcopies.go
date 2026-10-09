package broker

// shortCopies counts the persistent sessions this node owns, and the shards
// it leads for retained messages, that the current shard map gives fewer
// copies than the configured replica count. A node with no replication has
// neither.
func (s *Server) shortCopies() (sessions, retainedLed int) {
	rep := s.Replication()
	if rep == nil {
		return 0, 0
	}
	short, led := rep.Short()
	if len(short) == 0 {
		return 0, led
	}
	t := rep.Map()
	return s.sessions.CountPersistent(func(ns, id string) bool { return short[t.ShardOf(ns, id)] }), led
}
