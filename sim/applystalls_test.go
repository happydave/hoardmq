package sim

import (
	"reflect"
	"testing"
)

// TestApplyStallsSumsEveryRun: a node's stalls are summed over its stopped
// runs, and a node with none has no entry.
func TestApplyStallsSumsEveryRun(t *testing.T) {
	c := &Cluster{}
	c.addStallCount("n1", 2)
	c.addStallCount("n1", 1)
	c.addStallCount("n2", 0)
	if got, want := c.ApplyStalls(), map[string]uint64{"n1": 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("apply stalls %v, want %v", got, want)
	}
}

// TestConsensusHoldsSumsEveryRun: a node's holds are summed over its stopped
// runs, and a node with none has no entry.
func TestConsensusHoldsSumsEveryRun(t *testing.T) {
	c := &Cluster{}
	c.addHoldCount("n1", 1)
	c.addHoldCount("n1", 1)
	c.addHoldCount("n2", 0)
	if got, want := c.ConsensusHolds(), map[string]uint64{"n1": 2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("holds %v, want %v", got, want)
	}
}
