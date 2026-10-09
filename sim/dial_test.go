package sim

import "testing"

// TestNoSourceDialsAsBefore: a client without a source address gets the
// plain dialer, with no local address and no control hook.
func TestNoSourceDialsAsBefore(t *testing.T) {
	d := sourceDialer("")
	if d.LocalAddr != nil || d.ControlContext != nil {
		t.Fatalf("a client without a source address must dial as before: %+v", d)
	}
	if n, mode := clientDial([]string{"127.0.0.1"}); n != 0 || mode != dialNoSource {
		t.Fatalf("one source address is not given to the clients: got %d, %q", n, mode)
	}
}
