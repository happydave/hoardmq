package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/happydave/hoardmq/internal/admin"
	"github.com/happydave/hoardmq/internal/cluster/transport"
)

// forwardsInFlight bounds the forwarded admin requests a node runs at once;
// one beyond it is answered busy at once.
const forwardsInFlight = 16

// adminForward is a forwarded admin request on the wire, or, when Applied
// is set, a question: has the receiver applied the log through Applied?
type adminForward struct {
	ID      uint64               `json:"id"`
	Req     admin.ForwardRequest `json:"req"`
	Applied uint64               `json:"applied,omitempty"`
}

// adminForwardAnswer is its answer: the owner's answer, or why there is
// none; to a question, whether the receiver has applied the index.
type adminForwardAnswer struct {
	ID      uint64              `json:"id"`
	Outcome string              `json:"outcome,omitempty"`
	Reason  string              `json:"reason,omitempty"`
	Answer  admin.ForwardAnswer `json:"answer"`
	Applied bool                `json:"applied,omitempty"`
}

// adminForwarder sends admin requests for clients another node holds, and
// serves those other nodes send here.
type adminForwarder struct {
	s       *Server
	seq     atomic.Uint64
	mu      sync.Mutex
	pending map[uint64]pendingForward
	running chan struct{}
}

type pendingForward struct {
	node string
	ch   chan adminForwardAnswer
}

func newAdminForwarder(s *Server) *adminForwarder {
	f := &adminForwarder{s: s, pending: map[uint64]pendingForward{}, running: make(chan struct{}, forwardsInFlight)}
	s.cluster.Handle(transport.AdminRequest, f.receiveRequest)
	s.cluster.Handle(transport.AdminAnswer, f.receiveAnswer)
	return f
}

// timeout is how long a caller waits for an answer: past the owner's longest
// wait, an invalidation's durability wait, which the replication
// acknowledgement timeout bounds.
func (f *adminForwarder) timeout() time.Duration {
	if d := f.s.hooks.AdminForwardTimeout; d > 0 {
		return d
	}
	return time.Duration(f.s.config().Cluster.Replication.AckTimeoutMs)*time.Millisecond + 5*time.Second
}

func (f *adminForwarder) node() string { return adminBackend{s: f.s}.Node() }

func (f *adminForwarder) count(outcome string) {
	f.s.count("hoardmq_admin_forwards_total", map[string]string{"outcome": outcome}, 1)
}

// forward sends req to node and waits for its answer.
func (f *adminForwarder) forward(ctx context.Context, node string, req admin.ForwardRequest) (admin.ForwardAnswer, error) {
	id := f.seq.Add(1)
	body, err := json.Marshal(adminForward{ID: id, Req: req})
	if err != nil {
		return admin.ForwardAnswer{}, err
	}
	ch := make(chan adminForwardAnswer, 1)
	f.mu.Lock()
	f.pending[id] = pendingForward{node: node, ch: ch}
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		delete(f.pending, id)
		f.mu.Unlock()
	}()
	if err := f.s.cluster.Send(ctx, node, transport.AdminRequest, body); err != nil {
		outcome, reason := admin.ForwardUnreachable, fmt.Sprintf("node %s: %v", node, err)
		if errors.Is(err, transport.ErrUnsupportedClass) {
			outcome, reason = admin.ForwardUnsupported, "node "+node+"'s build takes no forwarded admin requests"
		}
		f.count(outcome)
		return admin.ForwardAnswer{}, &admin.ForwardError{Outcome: outcome, Reason: reason}
	}
	wait := f.timeout()
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case a := <-ch:
		switch a.Outcome {
		case "":
			f.count("answered")
			return a.Answer, nil
		case "busy":
			f.count("busy")
			return a.Answer, nil
		default:
			f.count(a.Outcome)
			return admin.ForwardAnswer{}, &admin.ForwardError{Outcome: a.Outcome, Reason: a.Reason}
		}
	case <-timer.C:
	case <-ctx.Done():
	}
	f.count(admin.ForwardTimeout)
	return admin.ForwardAnswer{}, &admin.ForwardError{Outcome: admin.ForwardTimeout, Reason: fmt.Sprintf("no answer from node %s in %s", node, wait)}
}

// receiveAnswer hands an answer to the caller waiting for it; one whose
// caller stopped waiting is dropped and counted.
func (f *adminForwarder) receiveAnswer(peer string, payload []byte) {
	var a adminForwardAnswer
	if json.Unmarshal(payload, &a) != nil {
		return
	}
	f.mu.Lock()
	p, ok := f.pending[a.ID]
	f.mu.Unlock()
	if !ok || p.node != peer {
		f.s.count("hoardmq_admin_forward_answers_dropped_total", nil, 1)
		return
	}
	select {
	case p.ch <- a:
	default:
	}
}

// receiveRequest serves a request another node forwarded, off the class's
// drainer: at most forwardsInFlight at once, and none without an admin API.
// A question about the applied index is answered whether or not this node
// has an admin API.
func (f *adminForwarder) receiveRequest(peer string, payload []byte) {
	var req adminForward
	if json.Unmarshal(payload, &req) != nil {
		return
	}
	if req.Applied > 0 {
		go f.answerApplied(peer, req)
		return
	}
	api := f.s.adminAPI
	if api == nil {
		f.answer(peer, adminForwardAnswer{ID: req.ID, Outcome: admin.ForwardAdminOff, Reason: "node " + f.node() + " has no admin API"})
		return
	}
	select {
	case f.running <- struct{}{}:
	default:
		body, _ := json.Marshal(admin.Problem{Type: "about:blank", Title: http.StatusText(http.StatusServiceUnavailable), Status: http.StatusServiceUnavailable,
			Detail: "node " + f.node() + " is running as many forwarded requests as it takes; try again"})
		f.answer(peer, adminForwardAnswer{ID: req.ID, Outcome: "busy",
			Answer: admin.ForwardAnswer{Status: http.StatusServiceUnavailable, ContentType: "application/problem+json", Body: body}})
		return
	}
	go func() {
		defer func() { <-f.running }()
		if gate := f.s.hooks.AdminForwardGate; gate != nil {
			gate(f.node())
		}
		ctx, cancel := context.WithTimeout(context.Background(), f.timeout())
		defer cancel()
		ans := api.ServeForwarded(ctx, peer, req.Req)
		f.answer(peer, adminForwardAnswer{ID: req.ID, Answer: ans})
	}()
}

// answerApplied answers whether this node's records have applied the index,
// waiting for them up to the forward timeout.
func (f *adminForwarder) answerApplied(peer string, req adminForward) {
	if gate := f.s.hooks.AdminAppliedGate; gate != nil {
		gate(f.node())
	}
	ok := false
	if st := f.s.records.Load(); st != nil {
		ctx, cancel := context.WithTimeout(context.Background(), f.timeout())
		ok = st.WaitApplied(ctx, req.Applied) == nil
		cancel()
	}
	f.answer(peer, adminForwardAnswer{ID: req.ID, Applied: ok})
}

// confirmApplied asks node whether it has applied the log through index,
// and reports whether it said so before ctx ended. A node whose link takes
// no admin requests runs a build that keeps API keys of its own, and is
// counted as confirmed.
func (f *adminForwarder) confirmApplied(ctx context.Context, node string, index uint64) bool {
	id := f.seq.Add(1)
	body, err := json.Marshal(adminForward{ID: id, Applied: index})
	if err != nil {
		return false
	}
	ch := make(chan adminForwardAnswer, 1)
	f.mu.Lock()
	f.pending[id] = pendingForward{node: node, ch: ch}
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		delete(f.pending, id)
		f.mu.Unlock()
	}()
	if err := f.s.cluster.Send(ctx, node, transport.AdminRequest, body); err != nil {
		if errors.Is(err, transport.ErrUnsupportedClass) {
			f.s.count("hoardmq_admin_confirmations_total", map[string]string{"outcome": "not-asked"}, 1)
			return true
		}
		f.s.count("hoardmq_admin_confirmations_total", map[string]string{"outcome": "unreachable"}, 1)
		return false
	}
	select {
	case a := <-ch:
		outcome := "confirmed"
		if !a.Applied {
			outcome = "behind"
		}
		f.s.count("hoardmq_admin_confirmations_total", map[string]string{"outcome": outcome}, 1)
		return a.Applied
	case <-ctx.Done():
		f.s.count("hoardmq_admin_confirmations_total", map[string]string{"outcome": "timeout"}, 1)
		return false
	}
}

// answer sends an answer back; one too large for a frame is replaced by
// saying so, since the request has already run.
func (f *adminForwarder) answer(peer string, a adminForwardAnswer) {
	body, err := json.Marshal(a)
	if err != nil {
		return
	}
	limit := f.s.config().Cluster.MaxFrameBytes
	if limit <= 0 {
		limit = 16 << 20
	}
	if l := f.s.hooks.AdminAnswerLimit; l > 0 {
		limit = l
	}
	if len(body) > limit {
		body, _ = json.Marshal(adminForwardAnswer{ID: a.ID, Outcome: admin.ForwardTooLarge, Reason: fmt.Sprintf("an answer of %d bytes", len(body))})
	}
	ctx, cancel := context.WithTimeout(context.Background(), f.timeout())
	defer cancel()
	_ = f.s.cluster.Send(ctx, peer, transport.AdminAnswer, body)
}
