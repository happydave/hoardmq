package routing

import (
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"
)

// At a production owner's size a set composes into parts no larger than the
// part size and applies whole at the receiver. Run with HOARDMQ_ROUTE_SCALE
// set to the entry count (630000 is the largest node of the 1M runs).
func TestSetOfAMillionScaleOwnerComposesAndApplies(t *testing.T) {
	n, _ := strconv.Atoi(os.Getenv("HOARDMQ_ROUTE_SCALE"))
	if n <= 0 {
		t.Skip("set HOARDMQ_ROUTE_SCALE to the entry count")
	}
	send := New(Options{Node: "b", Logger: quiet, MaxFrame: 16 << 20})
	for i := 0; i < n; i++ {
		send.table.MergeOwn(Entry{Namespace: "default", Filter: fmt.Sprintf("f/f%07d", i), Owner: "b", Count: 1}, &send.version)
	}
	recv, m := barePlane(t)
	started := time.Now()
	c := send.compose(7)
	composed := time.Since(started)
	parts, pos, largest := 0, 0, 0
	started = time.Now()
	var encoding time.Duration
	for index := uint32(0); pos < len(c.entries) || index == 0; index++ {
		at := time.Now()
		payload, next := send.encodePart(c, pos, index)
		encoding += time.Since(at)
		largest = max(largest, len(payload))
		if len(payload) > send.partSize {
			t.Fatalf("part %d is %d bytes", index, len(payload))
		}
		recv.receiveRoutes("b", payload)
		parts++
		pos = next
	}
	total := time.Since(started)
	if _, remote := recv.Table().Count(); remote != n {
		t.Fatalf("the receiver holds %d of %d entries", remote, n)
	}
	if !synced(recv, "b") {
		t.Fatal("the set did not complete")
	}
	t.Logf("%d entries: %d parts (largest %d bytes, %d rounds of %d); compose %v, encode %v, receive and apply %v, apply's lock held %.3fs",
		n, parts, largest, (parts+roundParts-1)/roundParts, roundParts, composed, encoding, total-encoding, m.get("hoardmq_route_set_apply_seconds"))
}
