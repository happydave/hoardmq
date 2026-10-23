package etcdraft

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
)

// heldWarnEvery is how often a held node says again that it holds, with
// how far its raft log has grown: nothing compacts it until the node is
// restarted on a build that can apply what it holds.
const heldWarnEvery = time.Minute

// Held implements consensus.Core. Lock-free.
func (c *Core) Held() (consensus.Hold, bool) {
	if h := c.hold.Load(); h != nil {
		return *h, true
	}
	return consensus.Hold{}, false
}

// Holds is how many times this node has entered the held state: zero or
// one for a process. Lock-free.
func (c *Core) Holds() uint64 { return c.holds.Load() }

// enterHold runs on the loop at the first command the machine refused to
// apply. From here the loop applies no command and moves no applied index;
// it still saves, acknowledges and votes. Every proposal still waiting will
// not be applied here, so each is answered now.
func (c *Core) enterHold(index uint64, err error) {
	if c.hold.Load() != nil {
		return
	}
	h := &consensus.Hold{Index: index, Reason: err.Error()}
	c.hold.Store(h)
	c.holds.Add(1)
	c.mu.Lock()
	commit := c.commit
	waiters := c.waiters
	c.waiters = map[uint64]chan result{}
	c.mu.Unlock()
	for _, w := range waiters {
		w <- result{err: consensus.ErrHeld}
	}
	size := logBytes(c.o.Store.Path())
	c.heldWarnAt, c.heldWarnBytes = time.Now(), size
	c.log.Error("consensus holds: this build cannot apply the log from here; restart it on a build that understands the cluster's feature level",
		"entry", index, "reason", h.Reason, "commit", commit, "logBytes", size)
	c.audit("consensus.held", fmt.Sprint(index), "held", map[string]string{"reason": h.Reason, "commit": fmt.Sprint(commit), "logBytes": fmt.Sprint(size)})
	c.set("hoardmq_cluster_feature_level_held", nil, 1)
	c.noteHeldEntries()
}

// noteHeldEntries publishes how many committed entries this node holds
// unapplied.
func (c *Core) noteHeldEntries() {
	if c.hold.Load() == nil {
		return
	}
	c.mu.Lock()
	commit := c.commit
	c.mu.Unlock()
	applied := c.machine.Applied()
	held := uint64(0)
	if commit > applied {
		held = commit - applied
	}
	c.set("hoardmq_raft_held_entries", nil, float64(held))
}

// warnHeld runs on the loop's tick: once a minute while held it logs the
// held entries and the raft log's size and growth.
func (c *Core) warnHeld(now time.Time) {
	if c.hold.Load() == nil || now.Sub(c.heldWarnAt) < heldWarnEvery {
		return
	}
	size := logBytes(c.o.Store.Path())
	c.mu.Lock()
	commit := c.commit
	c.mu.Unlock()
	c.log.Warn("consensus still holds; its raft log grows until the node is upgraded or removed",
		"heldFrom", c.hold.Load().Index, "applied", c.machine.Applied(), "commit", commit,
		"logBytes", size, "grewBytes", int64(size)-int64(c.heldWarnBytes))
	c.heldWarnAt, c.heldWarnBytes = now, size
}

// logBytes is the size of the files under the raft log's directory, zero
// when it cannot be read.
func logBytes(dir string) uint64 {
	if dir == "" {
		return 0
	}
	var n uint64
	_ = filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if info, err := d.Info(); err == nil {
			n += uint64(info.Size())
		}
		return nil
	})
	return n
}

// TransferLeadership implements consensus.Core over the library's call.
// The library's node is stepped off the loop goroutine, as a campaign is.
func (c *Core) TransferLeadership(ctx context.Context, to string) error {
	var id uint64
	for _, m := range c.members.List() {
		if m.Name == to {
			id = m.ID
		}
	}
	if id == 0 {
		return fmt.Errorf("%w: %s", consensus.ErrNotMember, to)
	}
	c.mu.Lock()
	n := c.node
	c.mu.Unlock()
	if n == nil {
		return consensus.ErrStopped
	}
	n.TransferLeadership(ctx, c.id, id)
	return ctx.Err()
}
