package replication

import "testing"

// The backlog is what the peers have not acknowledged, summed; a peer that
// has acknowledged everything adds nothing, and an acknowledgement read
// ahead of the push (a resync's mark) is not a negative lag.
func TestBacklog(t *testing.T) {
	r := &Replicator{peers: map[string]*outbound{
		"a": {sent: 10, ackSeq: 4},
		"b": {sent: 3, ackSeq: 3},
		"c": {sent: 5, ackSeq: 7},
	}}
	if got := r.Backlog(); got != 6 {
		t.Fatalf("backlog %d, want 6", got)
	}
}

// TestBacklogByPeer: each peer's own unacknowledged frames, the terms Backlog
// sums.
func TestBacklogByPeer(t *testing.T) {
	r := &Replicator{peers: map[string]*outbound{
		"a": {name: "a", sent: 10, ackSeq: 4},
		"b": {name: "b", sent: 3, ackSeq: 3},
		"c": {name: "c", sent: 5, ackSeq: 7},
	}}
	got := r.BacklogByPeer()
	if len(got) != 3 || got["a"] != 6 || got["b"] != 0 || got["c"] != 0 {
		t.Fatalf("backlog by peer %v, want a 6, b 0, c 0", got)
	}
}
