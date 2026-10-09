package lease

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
	"github.com/happydave/hoardmq/internal/cluster/transport"
)

// failingTransport names a leader that cannot be reached: every send fails.
// It is the difference this work item exists to make visible — a healthy
// leader this node cannot talk to is not "no leader".
type failingTransport struct{ err error }

func (t failingTransport) Send(context.Context, string, transport.Class, []byte) error { return t.err }
func (failingTransport) Handle(transport.Class, transport.Handler)                     {}
func (failingTransport) Observe(transport.Observer)                                    {}
func (failingTransport) ObserveData(transport.Observer)                                {}
func (failingTransport) Peers() []transport.PeerInfo                                   { return nil }

func renewService(t *testing.T, core consensus.Core, tr Transport) *Service {
	t.Helper()
	s, err := New(Options{Node: "me", Core: core, Machine: consensus.NewMachine(), Transport: tr,
		Sessions: newFakeSessions(), TTL: 1500 * time.Millisecond, TakeoverTimeout: 300 * time.Millisecond,
		Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	s.Start()
	t.Cleanup(s.Stop)
	return s
}

// TestRenewalFailureNamesAnUnreachableLeader: a follower whose core names a
// leader, and whose sends to it fail, counts the failure as unreachable and
// not as leaderless — and the error still reads as ErrNoLeader, so the
// renewal is retried exactly as before.
func TestRenewalFailureNamesAnUnreachableLeader(t *testing.T) {
	core := &fakeCore{role: "follower", leader: "other"}
	s := renewService(t, core, failingTransport{err: errors.New("write: broken pipe")})
	waitFor(t, "a renewal failure", 3*time.Second, func() bool { return s.LivenessStats().AliveFails > 0 })
	st := s.LivenessStats()
	if st.RenewUnreachable == 0 {
		t.Fatalf("a failed send to a known leader must be counted unreachable: %+v", st)
	}
	if st.RenewNoLeader != 0 {
		t.Fatalf("it must not also be counted as leaderless: %+v", st)
	}
	// The retry contract: everything that tested for ErrNoLeader still does.
	if !errors.Is(ErrLeaderUnreachable, ErrNoLeader) || !retryable(ErrLeaderUnreachable) {
		t.Fatal("ErrLeaderUnreachable must remain an ErrNoLeader for retry")
	}
}

// TestRenewalFailureNamesALeaderlessView is the other half: with no leader
// named, the leaderless counter moves and the unreachable one does not.
func TestRenewalFailureNamesALeaderlessView(t *testing.T) {
	core := &fakeCore{role: "follower", leader: ""}
	s := renewService(t, core, failingTransport{err: errors.New("unused")})
	waitFor(t, "a renewal failure", 3*time.Second, func() bool { return s.LivenessStats().AliveFails > 0 })
	st := s.LivenessStats()
	if st.RenewNoLeader == 0 {
		t.Fatalf("a renewal with no leader named must be counted leaderless: %+v", st)
	}
	if st.RenewUnreachable != 0 {
		t.Fatalf("it must not be counted unreachable: %+v", st)
	}
}

// TestLeaderlessClockMeasuresTheWindow: the count of leaderless failures is
// not interpretable without how long the node was leaderless, which is why
// this figure exists. The second half is the control: a service that always
// has a leader must report zero.
func TestLeaderlessClockMeasuresTheWindow(t *testing.T) {
	core := &fakeCore{role: "follower", leader: ""}
	s := renewService(t, core, failingTransport{err: errors.New("unused")})
	const window = 700 * time.Millisecond
	time.Sleep(window)
	if got := s.LivenessStats().LeaderlessMaxMs; got < float64(window/time.Millisecond)*0.7 {
		t.Fatalf("leaderless for %s, reported %.0f ms — an open window must still count", window, got)
	}

	led := &fakeCore{role: "follower", leader: "other"}
	quiet := renewService(t, led, failingTransport{err: errors.New("unused")})
	time.Sleep(window)
	if got := quiet.LivenessStats().LeaderlessMaxMs; got != 0 {
		t.Fatalf("a service that always had a leader reported %.0f ms leaderless", got)
	}
}
