package session

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

// deleteJournal is a fakeJournal that also records the epoch of each delete
// and can fail them.
type deleteJournal struct {
	*fakeJournal
	failDeletes bool
}

func (j *deleteJournal) SessionDelete(k store.Key) error {
	if j.failDeletes {
		return errors.New("disk full")
	}
	j.log(fmt.Sprintf("delete %s @%d", k.ClientID, k.Epoch))
	return nil
}

func (j *deleteJournal) callsFor(id string) []string {
	j.mu.Lock()
	defer j.mu.Unlock()
	var out []string
	for _, c := range j.calls {
		if f := strings.Fields(c); len(f) >= 2 && f[1] == id {
			out = append(out, c)
		}
	}
	return out
}

func cleanConnect(m *Manager, c *fakeConn, clean bool, expiry uint32, epoch uint64, replacing bool) {
	pkt := &packet.Connect{ProtocolLevel: packet.V5, ClientID: c.id, CleanStart: clean}
	if expiry != 0 {
		pkt.Properties.SessionExpiryInterval = &expiry
	}
	if replacing {
		m.ConnectReplacing(c, pkt, epoch)
	} else {
		m.Connect(c, pkt, epoch)
	}
	m.Resume(c)
}

// TestCleanStartReplacingJournalsTheDeleteFirst: a Clean Start for a client
// another node may have written journals the delete at the new token before
// anything of the new session, persistent or not, so a replica holding the
// previous session's copy drops it rather than merging the new one into it.
func TestCleanStartReplacingJournalsTheDeleteFirst(t *testing.T) {
	for _, c := range []struct {
		name   string
		expiry uint32
		want   []string
	}{
		{"persistent", 600, []string{"delete x @7", "upsert x"}},
		{"expiry 0", 0, []string{"delete x @7"}},
	} {
		j := &deleteJournal{fakeJournal: newFakeJournal()}
		m, _ := newManager(t, func(o *Options) { o.Journal = j })
		cleanConnect(m, newFake("x", packet.V5, 10), true, c.expiry, 7, true)
		if got := j.callsFor("x"); strings.Join(got, ", ") != strings.Join(c.want, ", ") {
			t.Fatalf("%s: journaled %v, want %v", c.name, got, c.want)
		}
	}
}

// TestOnlyACleanStartReplacingWithNothingHereJournalsTheDelete: without the
// replacing entry point, without Clean Start, or with the session held here
// (whose own discard writes the delete), no extra delete is written.
func TestOnlyACleanStartReplacingWithNothingHereJournalsTheDelete(t *testing.T) {
	deletes := func(calls []string) int {
		n := 0
		for _, c := range calls {
			if strings.HasPrefix(c, "delete ") {
				n++
			}
		}
		return n
	}
	t.Run("not replacing", func(t *testing.T) {
		j := &deleteJournal{fakeJournal: newFakeJournal()}
		m, _ := newManager(t, func(o *Options) { o.Journal = j })
		cleanConnect(m, newFake("x", packet.V5, 10), true, 600, 7, false)
		if got := j.callsFor("x"); deletes(got) != 0 || len(got) == 0 {
			t.Fatalf("journaled %v, want the upsert and no delete", got)
		}
	})
	t.Run("not a clean start", func(t *testing.T) {
		j := &deleteJournal{fakeJournal: newFakeJournal()}
		m, _ := newManager(t, func(o *Options) { o.Journal = j })
		cleanConnect(m, newFake("x", packet.V5, 10), false, 600, 7, true)
		if got := j.callsFor("x"); deletes(got) != 0 || len(got) == 0 {
			t.Fatalf("journaled %v, want the upsert and no delete", got)
		}
	})
	t.Run("held here", func(t *testing.T) {
		j := &deleteJournal{fakeJournal: newFakeJournal()}
		m, _ := newManager(t, func(o *Options) { o.Journal = j })
		first := newFake("x", packet.V5, 10)
		cleanConnect(m, first, true, 600, 7, false)
		subscribe(m, first, 1, "t/x")
		m.Disconnect(first, true, 0, false)
		j.mu.Lock()
		j.calls = nil
		j.mu.Unlock()
		cleanConnect(m, newFake("x", packet.V5, 10), true, 600, 7, true)
		got := j.callsFor("x")
		if deletes(got) != 1 || len(got) != 2 || !strings.HasPrefix(got[0], "delete x") || got[1] != "upsert x" {
			t.Fatalf("journaled %v, want one delete, then the upsert", got)
		}
	})
}

// TestAFailedReplacingDeleteIsRepairedOrCounted: a persistent session whose
// delete failed is marked to be written whole, which begins with the delete;
// for a session that is not persistent the failure is counted.
func TestAFailedReplacingDeleteIsRepairedOrCounted(t *testing.T) {
	t.Run("persistent", func(t *testing.T) {
		j := &deleteJournal{fakeJournal: newFakeJournal(), failDeletes: true}
		m, _ := newManager(t, func(o *Options) { o.Journal = j })
		cleanConnect(m, newFake("x", packet.V5, 10), true, 600, 7, true)
		k := key{m.opts.Namespace, "x"}
		sh := m.shardFor(k)
		sh.mu.Lock()
		s := sh.sessions[k]
		sh.mu.Unlock()
		s.mu.Lock()
		marked, armed := s.unwritten, s.rewriteArmed
		s.mu.Unlock()
		if !marked || !armed {
			t.Fatalf("unwritten %v, rewrite armed %v: a failed delete must leave the session to be written whole", marked, armed)
		}
	})
	t.Run("expiry 0", func(t *testing.T) {
		j := &deleteJournal{fakeJournal: newFakeJournal(), failDeletes: true}
		metrics := &seriesMetrics{}
		m, _ := newManager(t, func(o *Options) { o.Journal = j; o.Metrics = metrics })
		cleanConnect(m, newFake("x", packet.V5, 10), true, 0, 7, true)
		if n := failedAppends(metrics); n != 1 {
			t.Fatalf("failed appends counted %v, want 1", n)
		}
	})
}
