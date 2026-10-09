package replication

import (
	"testing"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
)

// TestShortShards: a shard is short for this node when its targets, or its
// followers where this node leads, number fewer than replicas-1 under the
// map; a map with every copy placed reports nothing.
func TestShortShards(t *testing.T) {
	cases := []struct {
		name         string
		basis        []string
		replicas     int
		wantSessions int
		wantLed      int
	}{
		{"two copies on two nodes", []string{"n1", "n2"}, 2, 0, 0},
		{"two copies on one node", []string{"n1"}, 2, 4, 4},
		{"three copies on three nodes", []string{"n1", "n2", "n3"}, 3, 0, 0},
		{"three copies on two nodes", []string{"n1", "n2"}, 3, 4, 2},
		{"one copy", []string{"n1"}, 1, 0, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := shardmap.Compute(1, c.basis, c.replicas, 4)
			sessions, led := shortShards(m, "n1")
			if len(sessions) != c.wantSessions || led != c.wantLed {
				t.Fatalf("short sessions on %d shards and %d led, want %d and %d", len(sessions), led, c.wantSessions, c.wantLed)
			}
		})
	}
	if s, led := shortShards(shardmap.Map{}, "n1"); s != nil || led != 0 {
		t.Fatalf("no map yet: %v and %d", s, led)
	}
}
