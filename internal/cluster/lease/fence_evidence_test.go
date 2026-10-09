package lease

import (
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
)

// TestFenceRecordNamesTheDecidingCondition runs the fence rule's matrix
// again and checks, per row, that the record says which condition decided
// it. Without the record, a fence under a leader this node judged settled
// and a fence with no reachable quorum look the same (WI 1474).
func TestFenceRecordNamesTheDecidingCondition(t *testing.T) {
	cases := []struct {
		name            string
		role, leader    string
		up, down, flap  []string
		known           time.Duration
		settled, quorum bool
		outcome         FenceOutcome
	}{
		{"leader known a ttl, quorum up", "follower", "n2", []string{"n2", "n3"}, nil, nil, time.Second, true, true, FenceNow},
		{"leader known a ttl, no quorum", "follower", "n2", nil, []string{"n2", "n3"}, nil, time.Second, true, false, FenceNow},
		{"leader just elected, quorum up", "follower", "n2", []string{"n2", "n3"}, nil, nil, 100 * time.Millisecond, false, true, FenceDefer},
		{"leader just elected, no quorum", "follower", "n2", nil, []string{"n2", "n3"}, nil, 100 * time.Millisecond, false, false, FenceNow},
		{"leader changed since the tenure clock started, quorum up", "follower", "n3", []string{"n2", "n3"}, nil, nil, time.Second, false, true, FenceDefer},
		{"no leader, quorum up", "follower", "", []string{"n2"}, []string{"n3"}, nil, 0, false, true, FenceDefer},
		{"candidate, quorum up", "candidate", "", []string{"n2", "n3"}, nil, nil, 0, false, true, FenceDefer},
		{"no leader, no quorum (partition)", "follower", "", nil, []string{"n2", "n3"}, nil, 0, false, false, FenceNow},
		{"candidate, no quorum", "candidate", "", nil, []string{"n2", "n3"}, nil, 0, false, false, FenceNow},
		{"removed, peers up", "removed", "", []string{"n2", "n3"}, nil, nil, 0, false, true, FenceDefer},
		{"no leader, one peer flapping (link reset)", "candidate", "", nil, []string{"n3"}, []string{"n2"}, 0, false, true, FenceDefer},
		{"no leader, peers long down (partition)", "candidate", "", nil, []string{"n2", "n3"}, nil, 0, false, false, FenceNow},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			sessions := newFakeSessions()
			s, err := New(Options{Node: "me", Core: &fakeCore{role: tc.role, leader: tc.leader}, Machine: consensus.NewMachine(),
				Transport: &fakeTransport{up: tc.up, down: tc.down, flapping: tc.flap}, Sessions: sessions, TTL: time.Second, Logger: quiet,
				Clock: func() time.Time { return now }})
			if err != nil {
				t.Fatal(err)
			}
			s.lastAlive.Store(now.Add(-2 * time.Second).UnixNano())
			if tc.leader != "" {
				s.leaderKnownSince.Store(now.Add(-tc.known).UnixNano())
				s.leaderKnown.Store("n2")
			}
			s.checkFence(s.trackLeader())

			ev := s.FenceEvidence()
			if len(ev.Records) != 1 || ev.RecordsTotal != 1 {
				t.Fatalf("want exactly one record, got %d (total %d)", len(ev.Records), ev.RecordsTotal)
			}
			rec := ev.Records[0]
			if rec.Outcome != tc.outcome || rec.LeaderSettled != tc.settled || rec.QuorumReachable != tc.quorum {
				t.Fatalf("record %+v: want outcome=%s leader_settled=%v quorum_reachable=%v", rec, tc.outcome, tc.settled, tc.quorum)
			}
			if rec.RecordAgeMs < 1999 || rec.Role != tc.role || len(rec.Peers) != len(tc.up)+len(tc.down)+len(tc.flap) {
				t.Fatalf("record lacks its inputs: %+v", rec)
			}
			wantSettled, wantNoQuorum := int64(0), int64(0)
			if tc.outcome == FenceNow && tc.settled {
				wantSettled = 1
			}
			if tc.outcome == FenceNow && !tc.quorum {
				wantNoQuorum = 1
			}
			if ev.FencesLeaderSettled != wantSettled || ev.FencesNoQuorum != wantNoQuorum {
				t.Fatalf("fences_leader_settled=%d fences_no_quorum=%d, want %d and %d", ev.FencesLeaderSettled, ev.FencesNoQuorum, wantSettled, wantNoQuorum)
			}
		})
	}
}

// TestDecideFenceHasNothingToDecideBeforeTheTtl: an unlapsed record is not a
// decision, whatever else holds.
func TestDecideFenceHasNothingToDecideBeforeTheTtl(t *testing.T) {
	now := time.Now()
	out, rec := decideFence(now, now.Add(-999*time.Millisecond), time.Second, consensus.Status{Role: "candidate"}, 0, nil, true)
	if out != FenceNone || rec.Outcome != FenceNone {
		t.Fatalf("an unlapsed record decided %s", out)
	}
}

// TestDecideFenceWithoutTransportTakesTheQuorumAsReachable: a service with no
// transport has no peer list to count, so a lapsed record under no settled
// leader defers.
func TestDecideFenceWithoutTransportTakesTheQuorumAsReachable(t *testing.T) {
	now := time.Now()
	out, rec := decideFence(now, now.Add(-2*time.Second), time.Second, consensus.Status{Role: "follower"}, 0, nil, false)
	if out != FenceDefer || !rec.QuorumReachable {
		t.Fatalf("no transport: outcome %s, quorum_reachable %v", out, rec.QuorumReachable)
	}
}

// TestDeferralStreakRecordsOnce: a deferral streak is one record, and the
// fence that ends it is another; the deferral count keeps counting every
// check, as it did before.
func TestDeferralStreakRecordsOnce(t *testing.T) {
	now := time.Now()
	core := &fakeCore{role: "candidate"}
	tr := &fakeTransport{up: []string{"n2", "n3"}}
	s, err := New(Options{Node: "me", Core: core, Machine: consensus.NewMachine(), Transport: tr, Sessions: newFakeSessions(),
		TTL: time.Second, Logger: quiet, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	s.lastAlive.Store(now.Add(-2 * time.Second).UnixNano())
	for range 5 {
		s.checkFence(s.trackLeader())
	}
	tr.up, tr.down = nil, []string{"n2", "n3"}
	s.checkFence(s.trackLeader())

	ev := s.FenceEvidence()
	if ev.RecordsTotal != 2 || len(ev.Records) != 2 {
		t.Fatalf("five deferrals and a fence: want 2 records, got %d (total %d)", len(ev.Records), ev.RecordsTotal)
	}
	if ev.Records[0].Outcome != FenceDefer || ev.Records[1].Outcome != FenceNow || ev.FencesNoQuorum != 1 {
		t.Fatalf("records %+v, fences_no_quorum %d", ev.Records, ev.FencesNoQuorum)
	}
	if d := s.LivenessStats().FencesDeferred; d != 5 {
		t.Fatalf("fences_deferred=%d, want 5", d)
	}
}

// TestFenceRecordsAreBounded: the ring keeps the last fenceRecordsMax and
// the total keeps counting.
func TestFenceRecordsAreBounded(t *testing.T) {
	s := &Service{}
	for i := range fenceRecordsMax + 5 {
		s.recordFence(FenceRecord{Term: uint64(i)})
	}
	ev := s.FenceEvidence()
	if len(ev.Records) != fenceRecordsMax || ev.RecordsTotal != fenceRecordsMax+5 {
		t.Fatalf("%d records, total %d", len(ev.Records), ev.RecordsTotal)
	}
	if ev.Records[0].Term != 5 || ev.Records[fenceRecordsMax-1].Term != fenceRecordsMax+4 {
		t.Fatalf("the ring kept the wrong records: first term %d, last term %d", ev.Records[0].Term, ev.Records[fenceRecordsMax-1].Term)
	}
}

// TestLoopTimingRecordsTheLongestGap: the tick loop's gap and the status
// read's duration are maxima over the run.
func TestLoopTimingRecordsTheLongestGap(t *testing.T) {
	s := &Service{}
	base := time.Now()
	s.markLoop(base)
	s.markLoop(base.Add(250 * time.Millisecond))
	s.markLoop(base.Add(3 * time.Second))
	s.markLoop(base.Add(3100 * time.Millisecond))
	ev := s.FenceEvidence()
	if ev.LoopGapMaxMs != 2750 {
		t.Fatalf("loop_gap_max_ms=%v, want 2750", ev.LoopGapMaxMs)
	}
	if got := float64(s.loopGapNs.Load()) / 1e6; got != 100 {
		t.Fatalf("current gap %vms, want 100", got)
	}
}

// TestTrackLeaderTimesTheStatusRead: a slow core status read is recorded.
func TestTrackLeaderTimesTheStatusRead(t *testing.T) {
	s, err := New(Options{Node: "me", Core: &slowStatusCore{fakeCore: fakeCore{role: "follower", leader: "n2"}, delay: 30 * time.Millisecond},
		Machine: consensus.NewMachine(), Transport: &fakeTransport{up: []string{"n2"}}, Sessions: newFakeSessions(), TTL: time.Second, Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	s.trackLeader()
	if ms := s.FenceEvidence().StatusReadMaxMs; ms < 30 {
		t.Fatalf("status_read_max_ms=%v, want at least 30", ms)
	}
}

type slowStatusCore struct {
	fakeCore
	delay time.Duration
}

func (c *slowStatusCore) Status() consensus.Status {
	time.Sleep(c.delay)
	return c.fakeCore.Status()
}
