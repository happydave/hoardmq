package sim

import (
	"strconv"
	"testing"
)

// TestListenPortsAreNotDrawnAsClusterPorts is the fix for WI 1511's cause.
//
// freePort checks a port by binding 127.0.0.1, but a listener address given
// through Options.ListenAddrs is on another interface — in the remote-client
// mode every node listens on 10.231.14.x:8001 — so that bind succeeds and the
// port can be handed out as a node's cluster address anyway. Nothing clashes
// on the wire; what breaks is the evidence sampler, which filters `ss` by the
// cluster's ports and then matches every client connection to every node.
func TestListenPortsAreNotDrawnAsClusterPorts(t *testing.T) {
	const port = 8001
	addrs := []string{"10.231.14.1:8001", "10.231.14.2:8001", "10.231.14.3:8001"}

	// Presence first: without the reservation the port is drawable, so the
	// assertion below is about the reservation and not about 8001 being
	// impossible to draw.
	portMu.Lock()
	wasHeld := handedPorts[port]
	delete(handedPorts, port)
	portMu.Unlock()
	t.Cleanup(func() {
		portMu.Lock()
		if wasHeld {
			handedPorts[port] = true
		} else {
			delete(handedPorts, port)
		}
		portMu.Unlock()
	})

	portMu.Lock()
	before := handedPorts[port]
	portMu.Unlock()
	if before {
		t.Fatal("8001 was already reserved before the call, so this test would pass vacuously")
	}

	// Through drawAddresses, the path a cluster actually takes — not by
	// calling reserveListenPorts directly. The first version of this test did
	// that, and passed with the call site removed: it proved the function
	// worked, not that anything used it.
	c := &Cluster{o: Options{ListenAddrs: addrs}, byAddr: map[string]*Node{}}
	c.drawAddresses()

	portMu.Lock()
	after := handedPorts[port]
	portMu.Unlock()
	if !after {
		t.Error("a listener address's port was not kept out of the draw")
	}
}

func TestReserveListenPortsIgnoresJunk(t *testing.T) {
	// A malformed address must not panic or reserve something arbitrary; the
	// harness passes these straight from an environment variable.
	reserveListenPorts([]string{"", "no-colon", "host:notaport", "1.2.3.4:"})
}

// TestSocketsPerSampleCap is the bound: the correctness fix above stops the
// known cause, and this makes the next one cheap instead of a gigabyte.
func TestSocketsPerSampleCap(t *testing.T) {
	// A sample under the cap keeps everything and reports nothing dropped.
	small := make([]SocketSample, 10)
	got, dropped := capSockets(small)
	if len(got) != 10 || dropped != 0 {
		t.Errorf("under the cap: kept %d dropped %d, want 10 and 0", len(got), dropped)
	}

	// Over it, the sample is truncated and says by how much — silence here is
	// what made WI 1511 invisible until a 937 MB file refused to publish.
	big := make([]SocketSample, socketsPerSampleMax+1234)
	got, dropped = capSockets(big)
	if len(got) != socketsPerSampleMax {
		t.Errorf("over the cap: kept %d, want %d", len(got), socketsPerSampleMax)
	}
	if dropped != 1234 {
		t.Errorf("over the cap: dropped %d, want 1234", dropped)
	}

	// The shape WI 1511 actually produced.
	got, dropped = capSockets(make([]SocketSample, 168184))
	if len(got) != socketsPerSampleMax || dropped != 168184-socketsPerSampleMax {
		t.Errorf("WI 1511's sample: kept %d dropped %d", len(got), dropped)
	}
	if strconv.Itoa(dropped) == "0" {
		t.Error("a capture of 168,184 rows reported nothing dropped")
	}
}
