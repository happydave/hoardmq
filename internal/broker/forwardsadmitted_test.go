package broker

import "testing"

// TestForwardsAdmitted: a node takes forwarded QoS 1 and 2 deliveries only
// once its replication is attached and its consensus core has recovered.
func TestForwardsAdmitted(t *testing.T) {
	for _, c := range []struct {
		attached, recovering, want bool
	}{
		{false, false, false},
		{false, true, false},
		{true, true, false},
		{true, false, true},
	} {
		if got := forwardsAdmitted(c.attached, c.recovering); got != c.want {
			t.Errorf("forwardsAdmitted(attached %v, recovering %v) = %v, want %v", c.attached, c.recovering, got, c.want)
		}
	}
}
