package session

import (
	"testing"

	"github.com/happydave/hoardmq/internal/packet"
)

// TestSealStatsCountResumesAndRefusals: a CONNECT at a grant no later than
// the seal's is refused and counted; one at a later grant resumes the copy and
// is counted.
func TestSealStatsCountResumesAndRefusals(t *testing.T) {
	m, _, _ := sealedFixture(t)
	if res := m.Seal(m.opts.Namespace, "c", packet.SessionTakenOver, 9); res.Outcome != SealSealed {
		t.Fatalf("seal: %+v", res)
	}
	if res := connectAt(m, newFake("c", packet.V5, 100), false, 600, 7); res.Reason != packet.ServerBusy {
		t.Fatalf("a CONNECT at an older grant: %v, want Server Busy", res.Reason)
	}
	if res := connectAt(m, newFake("c", packet.V5, 100), false, 600, 12); !res.SessionPresent {
		t.Fatalf("a CONNECT at a later grant did not resume the copy: %+v", res)
	}
	if refused, resumed := m.SealStats(); refused != 1 || resumed != 1 {
		t.Fatalf("SealStats refused %d resumed %d, want 1 and 1", refused, resumed)
	}
}
