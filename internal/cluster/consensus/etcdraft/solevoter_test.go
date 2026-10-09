package etcdraft

import (
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
)

func TestSoleVoter(t *testing.T) {
	for _, c := range []struct {
		name string
		cs   *pb.ConfState
		want bool
	}{
		{"no state", nil, false},
		{"empty", &pb.ConfState{}, false},
		{"only me", &pb.ConfState{Voters: []uint64{7}}, true},
		{"only someone else", &pb.ConfState{Voters: []uint64{8}}, false},
		{"me and another", &pb.ConfState{Voters: []uint64{7, 8}}, false},
		{"me, with an outgoing set", &pb.ConfState{Voters: []uint64{7}, VotersOutgoing: []uint64{7, 8}}, false},
		{"me as a learner only", &pb.ConfState{Learners: []uint64{7}}, false},
	} {
		if got := soleVoter(c.cs, 7); got != c.want {
			t.Errorf("%s: soleVoter = %v, want %v", c.name, got, c.want)
		}
	}
}
