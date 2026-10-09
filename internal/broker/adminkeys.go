package broker

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// keysReady is the decision on whether a node may authenticate API keys.
// contactIndex and contactAt are its last successful renewal with the
// leader: the log index its alive record applied at, and when, on its
// steady clock. A node that has not renewed since it started, has not
// renewed within bound, or has not applied the log through its last
// renewal is not ready: a revocation committed before that renewal may not
// have reached it.
func keysReady(contactIndex uint64, contactAt, now time.Time, applied uint64, bound time.Duration) (bool, string) {
	switch {
	case contactIndex == 0:
		return false, "no renewal with the leader since this node started"
	case now.Sub(contactAt) > bound:
		return false, fmt.Sprintf("its last renewal with the leader was %s ago, more than %s", now.Sub(contactAt).Round(time.Millisecond), bound)
	case applied < contactIndex:
		return false, fmt.Sprintf("it has applied the log to %d, short of its last renewal at %d", applied, contactIndex)
	}
	return true, ""
}

// KeysReady applies keysReady to this node's lease service and state
// machine: the bound is half the lease ttl, under the ttl after which the
// leader declares a silent node dead.
func (b adminBackend) KeysReady() (bool, string) {
	s := b.s
	st := s.records.Load()
	if s.leases == nil || st == nil {
		return false, "this node's consensus core is not open"
	}
	index, at := s.leases.Contact()
	return keysReady(index, at, s.leases.Steady(), s.leases.Applied(), s.leases.TTL()/2)
}

// ConfirmApplied asks every member the lease table calls alive, this node
// included, whether it has applied the log through index, and returns those
// that did not say so before ctx ended. A member the log has declared dead
// is not asked; it refuses API keys until it has renewed past the
// declaration. A member whose link takes no admin requests runs a build that
// keeps API keys of its own and knows none of the cluster's, and counts as
// confirmed.
func (b adminBackend) ConfirmApplied(ctx context.Context, index uint64) ([]string, error) {
	s := b.s
	st := s.records.Load()
	if st == nil {
		return nil, fmt.Errorf("this node's consensus core is not open")
	}
	self := b.Node()
	var mu sync.Mutex
	var unconfirmed []string
	var wg sync.WaitGroup
	if err := st.WaitApplied(ctx, index); err != nil {
		unconfirmed = append(unconfirmed, self)
	}
	if s.consensus == nil || s.leases == nil || s.admins == nil {
		return unconfirmed, nil
	}
	for _, m := range s.consensus.Members() {
		if m.Name == self || !s.leases.Table().Live(m.Name) {
			continue
		}
		wg.Add(1)
		go func(node string) {
			defer wg.Done()
			if !s.admins.confirmApplied(ctx, node, index) {
				mu.Lock()
				unconfirmed = append(unconfirmed, node)
				mu.Unlock()
			}
		}(m.Name)
	}
	wg.Wait()
	return unconfirmed, nil
}

// importKeys puts the keys of this node's key file into the cluster's
// collection, retrying until a write is confirmed or the server stops: at
// start the cluster may have no leader yet.
func (s *Server) importKeys() {
	defer s.wg.Done()
	for {
		ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
		imported, kept, err := s.adminAPI.ImportKeys(ctx)
		cancel()
		if err == nil {
			if imported+kept > 0 {
				s.log.Info("API keys imported from the node's key file", "imported", imported, "alreadyPresent", kept)
				s.record("server", "apikey.import", s.config().Node.Name, "ok", map[string]string{"imported": fmt.Sprint(imported), "kept": fmt.Sprint(kept)})
			}
			return
		}
		s.log.Warn("API key import failed; retrying", "err", err)
		select {
		case <-s.ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}
