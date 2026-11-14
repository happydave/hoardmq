package listener

import "testing"

// TestTCPKeepAliveOffDecision: TCP keepalive goes off only on a TCP socket
// whose client asked for a keep alive other than 0; a keep alive of 0 and a
// connection with no TCP socket are refused whatever the other argument says.
func TestTCPKeepAliveOffDecision(t *testing.T) {
	for _, c := range []struct {
		requested uint16
		tcp       bool
		off       bool
	}{
		{0, true, false},
		{0, false, false},
		{1, false, false},
		{30, false, false},
		{65535, false, false},
		{1, true, true},
		{30, true, true},
		{65535, true, true},
	} {
		if got := tcpKeepAliveOff(c.requested, c.tcp); got != c.off {
			t.Errorf("keep alive %d, tcp %v: off %v, want %v", c.requested, c.tcp, got, c.off)
		}
	}
}
