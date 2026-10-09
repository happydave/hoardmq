package routing

import (
	"errors"
	"testing"
	"time"
)

// TestRefusalTraceRecordsTheMovingFilter: a publish refused because a session
// on its filter is moving away is sampled with that filter and how long ago its
// move began, which is what attributes a refusal to the move grace.
func TestRefusalTraceRecordsTheMovingFilter(t *testing.T) {
	p := movingPlane(t, time.Hour)
	p.Observe("default", "t/0", 1)
	handOver(p, "t/0")
	if _, err := p.Forward(qos1On("t/0")); !errors.Is(err, ErrRoutesUnavailable) {
		t.Fatalf("precondition not reached: the publish was not refused: %v", err)
	}
	tr := p.RefusalTrace()
	if got := tr.Counts["local/session-moving"]; got != 1 {
		t.Fatalf("counts %v, want one local session-moving refusal", tr.Counts)
	}
	if len(tr.Samples) != 1 {
		t.Fatalf("samples %+v, want one", tr.Samples)
	}
	s := tr.Samples[0]
	if s.Topic != "t/0" || s.Where != "local" || s.Reason != "session-moving" {
		t.Errorf("sample %+v", s)
	}
	began, ok := s.Moving["t/0"]
	if !ok || began < 0 || began > time.Minute.Milliseconds() {
		t.Errorf("moving %v, want t/0 with its move begun under a minute ago", s.Moving)
	}
}
