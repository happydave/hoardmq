package replication

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/lease"
	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

// bareStream is an outbound stream with no replicator and no sender: frames
// pushed stay queued, and a test plays the peer's answers.
func bareStream() *outbound {
	p := &outbound{name: "peer"}
	p.cond = sync.NewCond(&p.mu)
	return p
}

var keyK = store.Scope{Namespace: "default", ClientID: "k"}

func at7(k store.Scope) store.Scope { k.Epoch = 7; return k }

func stateOf(k store.Scope, epoch uint64) *store.SessionState {
	return &store.SessionState{SessionMeta: store.SessionMeta{Namespace: k.Namespace, ClientID: k.ClientID, Epoch: epoch, Version: 5, ExpiryInterval: 300},
		Subscriptions: []store.SubscriptionRecord{}, InboundQoS2: []uint16{}, Entries: []store.Entry{}}
}

// appendOf is the frame of a session's meta record alone.
func appendOf(k store.Scope, epoch uint64) frame {
	rec := store.RecordsFor(stateOf(k, epoch))[0]
	return frame{Kind: kindAppend, Type: rec.Type, Namespace: k.Namespace, ClientID: k.ClientID, Epoch: epoch, Payload: rec.Payload}
}

func snapshotOf(k store.Scope, epoch uint64) frame {
	return frame{Kind: kindSnapshot, Namespace: k.Namespace, ClientID: k.ClientID, Epoch: epoch, State: stateOf(k, epoch)}
}

// waiting starts a barrier naming k on p and returns its result channel.
func waiting(p *outbound, k store.Scope) chan error {
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		done <- p.wait(ctx, func(string) bool { return true }, []store.Scope{k})
	}()
	return done
}

func pending(t *testing.T, done chan error, what string) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("%s: the barrier ended (%v); want it waiting", what, err)
	case <-time.After(150 * time.Millisecond):
	}
}

func ended(t *testing.T, done chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("the barrier did not end")
		return nil
	}
}

// TestAPredatedRefusalWaitsForTheWholeWrite: a barrier started between a
// refused record and a later whole write's push, the refusal arriving after
// the push, does not pass on the refused batch's acknowledgement: it waits for
// the whole write's, and fails when that write is refused.
func TestAPredatedRefusalWaitsForTheWholeWrite(t *testing.T) {
	for _, refuseWhole := range []bool{false, true} {
		t.Run(fmt.Sprint("whole write refused ", refuseWhole), func(t *testing.T) {
			p := bareStream()
			p.push(appendOf(keyK, 7)) // 1: the lone meta record
			p.push(appendOf(keyK, 7)) // 2: the publish's record
			done := waiting(p, keyK)
			time.Sleep(50 * time.Millisecond)
			p.push(snapshotOf(keyK, 7)) // 3: the rebalance's whole write
			for _, seq := range []uint64{1, 2} {
				p.failedAt(seq, unappliedReason+": no-session session-upsert", at7(keyK))
				p.refusalArrived()
			}
			p.acked(2)
			pending(t, done, "acknowledged through the refused records")
			if refuseWhole {
				p.failedAt(3, "epoch 7 is behind 9, the highest grant seen for the client", at7(keyK))
				p.refusalArrived()
				if err := ended(t, done); err == nil {
					t.Fatal("the barrier passed with the whole write refused")
				}
				return
			}
			p.acked(3)
			if err := ended(t, done); err != nil {
				t.Fatalf("the barrier failed after the whole write's acknowledgement: %v", err)
			}
		})
	}
}

// TestFormTwoWaitsForItsLastEntry: a whole write sent as a head and entries
// is waited for to its last entry.
func TestFormTwoWaitsForItsLastEntry(t *testing.T) {
	p := bareStream()
	p.push(appendOf(keyK, 7)) // 1: refused, predated
	done := waiting(p, keyK)
	time.Sleep(50 * time.Millisecond)
	head := p.push(snapshotOf(keyK, 7))
	p.push(appendOf(keyK, 7))
	last := p.push(appendOf(keyK, 7))
	p.extendWhole(keyK, head, last)
	p.failedAt(1, unappliedReason+": no-session enqueue", at7(keyK))
	p.refusalArrived()
	p.acked(head)
	pending(t, done, "acknowledged through the head")
	p.acked(last - 1)
	pending(t, done, "acknowledged short of the last entry")
	p.acked(last)
	if err := ended(t, done); err != nil {
		t.Fatalf("the barrier failed after the last entry's acknowledgement: %v", err)
	}
}

// TestAResyncKeepsAnOwnerHeldKeyRefused: a resync's end clears what the peer
// refused, not what this node holds refused on its own verdict.
func TestAResyncKeepsAnOwnerHeldKeyRefused(t *testing.T) {
	p := bareStream()
	p.setHeld(keyK, heldTooLarge)
	p.hole = true
	gen, ok, _ := p.startResync(time.Now(), time.Second)
	if !ok {
		t.Fatal("no resync started")
	}
	p.whole(gen, time.Now())
	if err := p.refusal([]store.Scope{keyK}); err == nil {
		t.Fatal("a resync's end cleared a key held refused for its size")
	}
	p.push(snapshotOf(keyK, 7))
	if err := p.refusal([]store.Scope{keyK}); err != nil {
		t.Fatalf("a whole write of the key did not end it: %v", err)
	}
}

// TestARepairCoveredByALaterWholeWriteIsSkipped: a refusal of a record pushed
// before a whole write of the key to that peer is answered by that write; no
// second whole write is sent.
func TestARepairCoveredByALaterWholeWriteIsSkipped(t *testing.T) {
	h, sinks, _ := threeAt(t, false, quickAck)
	n1 := h.nodes["n1"]
	p := n1.rep.outboundFor("n3")
	k := store.Scope{Namespace: "default", ClientID: "covered"}
	refused := p.push(appendOf(k, 7))
	p.push(snapshotOf(k, 7))
	n1.rep.repair("n3", p, at7(k), refused, repairUnapplied)
	if got := sinks["n1"].get("hoardmq_replication_repairs_total", map[string]string{"cause": repairUnapplied, "outcome": "covered"}); got < 1 {
		t.Fatalf("covered repairs %v; want at least 1", got)
	}
	for _, o := range []string{"snapshot", "delete", "repeat"} {
		if got := repairs(sinks, "n1", o); got != 0 {
			t.Fatalf("repairs %s %v; want 0", o, got)
		}
	}
}

// TestAnEarlierGrantsRefusalIsRepaired: route 2's shape. n1's import at 7
// reaches n2 after n2 noted a grant at 9 and is refused whole; n2 keeps its
// older copy. n1 takes the lease back at 12 and writes the meta record alone.
// n1 repairs the key at n2, so n2 ends with n1's session whole at 12, and a
// barrier naming the client passes.
func TestAnEarlierGrantsRefusalIsRepaired(t *testing.T) {
	h, sinks, tokens := threeAt(t, false, quickAck)
	id := clientIn(1, 0)
	writeSessionWhole(t, h, "n1", id, 5)
	h.waitFor("n2 to hold n1's copy", 5*time.Second, func() bool { return copyEpochOf(h, "n2", id) == 5 })
	importWhole(t, h, id, 7, func() { tokens["n2"].Note("default", id, 9) })
	h.waitFor("n2 to refuse the import", 5*time.Second, func() bool { return staleTokens(sinks, "n2") >= 1 })
	n1 := h.nodes["n1"]
	n1.sessions.mu.Lock()
	n1.sessions.sessions = []*store.SessionState{{SessionMeta: store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 12, Version: 5, ExpiryInterval: 300},
		Subscriptions: []store.SubscriptionRecord{{Filter: "a/b", QoS: 1}, {Filter: "a/c", QoS: 1}}, InboundQoS2: []uint16{}, Entries: []store.Entry{}}}
	n1.sessions.mu.Unlock()
	loneMeta(t, h, "n1", id, 12)
	h.waitFor("n2 to hold n1's session whole at 12", 5*time.Second, func() bool {
		ss := h.nodes["n2"].rep.ReplicaState().Find("default", id)
		return ss != nil && ss.Epoch == 12 && len(ss.Subscriptions) == 2
	})
	if got := sinks["n1"].get("hoardmq_replication_repairs_total", map[string]string{"cause": repairEarlierGrant, "outcome": "snapshot"}); got < 1 {
		t.Fatalf("earlier-grant repairs %v; want at least 1", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := n1.rep.Barrier(ctx, []store.Scope{{Namespace: "default", ClientID: id, Epoch: 12}}); err != nil {
		t.Fatalf("the barrier naming the client: %v", err)
	}
}

// TestARepairRefusedAsStaleLeavesTheCopyAbsent: the repair of a meta record a
// follower refused is judged stale there when it has seen a later grant; the
// follower then holds nothing, and the owner the key refused.
func TestARepairRefusedAsStaleLeavesTheCopyAbsent(t *testing.T) {
	id := clientIn(1, 0)
	var tk map[string]*lease.Tokens
	h, sinks, tokens := threeAt(t, false, func(o *Options) {
		o.AckTimeout = 300 * time.Millisecond
		if o.Node == "n1" {
			// The repair is read and about to be queued when n2 sees a later
			// grant, after it refused the meta record for want of a copy.
			o.WholeWriteGate = func() { tk["n2"].Note("default", id, 20) }
		}
	})
	tk = tokens
	servedAt12(h.nodes["n1"], id)
	loneMeta(t, h, "n1", id, 12)
	h.waitFor("n2 to refuse the meta record", 5*time.Second, func() bool { return loneMetaRefusals(sinks, "n2") >= 1 })
	h.waitFor("n2 to refuse the repair as stale", 5*time.Second, func() bool { return staleTokens(sinks, "n2") >= 1 })
	if holdsCopy(h, "n2", id) {
		t.Fatal("n2 holds a copy after refusing the repair as stale")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := h.nodes["n1"].rep.Barrier(ctx, []store.Scope{{Namespace: "default", ClientID: id, Epoch: 12}}); !errors.Is(err, ErrReplicaUnavailable) {
		t.Fatalf("the barrier naming the client: %v; want refused", err)
	}
}

// TestAFailedWholeWriteHoldsTheKeyUntilTheRewrite: a whole write whose
// records did not all append here holds its key refused at each target, so a
// barrier naming it fails, until a whole write of the key is sent; a session
// that leaves this node ends it too.
func TestAFailedWholeWriteHoldsTheKeyUntilTheRewrite(t *testing.T) {
	h, _, _ := threeAt(t, false, quickAck)
	id := clientIn(1, 0)
	n1 := h.nodes["n1"]
	key := []store.Scope{{Namespace: "default", ClientID: id, Epoch: 7}}
	st := &store.SessionState{SessionMeta: store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 7, Version: 5, ExpiryInterval: 300},
		Subscriptions: []store.SubscriptionRecord{}, InboundQoS2: []uint16{}, Entries: []store.Entry{}}
	n1.rep.OpenWhole("default", id, causeImport)(st, false)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := n1.rep.Barrier(ctx, key); err == nil {
		t.Fatal("a barrier passed naming a key whose whole write did not append")
	}
	n1.rep.OpenWhole("default", id, causeRewrite)(st, true)
	if err := n1.rep.Barrier(ctx, key); err != nil {
		t.Fatalf("after the rewrite: %v", err)
	}
	n1.rep.OpenWhole("default", id, causeImport)(st, false)
	n1.rep.SessionLeft("default", id)
	if err := n1.rep.Barrier(ctx, key); err != nil {
		t.Fatalf("after the session left: %v", err)
	}
}

// bigState is a session whose subscriptions, or entries, pass a small frame
// limit.
func bigState(id string, epoch uint64, subs, entries int) *store.SessionState {
	st := &store.SessionState{SessionMeta: store.SessionMeta{Namespace: "default", ClientID: id, Epoch: epoch, Version: 5, ExpiryInterval: 300},
		Subscriptions: []store.SubscriptionRecord{}, InboundQoS2: []uint16{}, Entries: []store.Entry{}}
	for i := range subs {
		st.Subscriptions = append(st.Subscriptions, store.SubscriptionRecord{Filter: fmt.Sprintf("a/%040d", i), QoS: 1})
	}
	for i := range entries {
		st.Entries = append(st.Entries, store.Entry{Seq: uint64(i + 1), QoS: 1, Message: &message.Message{Namespace: "default", Topic: "a/b", Payload: []byte(strings.Repeat("p", 2000))}})
	}
	return st
}

// TestWholeWritesPastTheFrameLimit: a session whose snapshot passes the limit
// is sent as a head and its entries, and its copy is whole; one whose head
// passes it is held refused, its delete sent and no later record of it; a
// session that shrinks below the limit is sent whole again.
func TestWholeWritesPastTheFrameLimit(t *testing.T) {
	h, sinks, _ := threeAt(t, false, func(o *Options) { o.AckTimeout = 300 * time.Millisecond; o.MaxFrame = 4096 })
	n1 := h.nodes["n1"]
	wholes := func(form string) float64 {
		return sinks["n1"].get("hoardmq_replication_whole_writes_total", map[string]string{"cause": causeImport, "form": form})
	}

	a := clientIn(1, 0)
	h.setLease("default", a, "n1", 7)
	n1.rep.OpenWhole("default", a, causeImport)(bigState(a, 7, 1, 3), true)
	h.waitFor("n2 to hold a's copy with its three entries", 5*time.Second, func() bool {
		ss := h.nodes["n2"].rep.ReplicaState().Find("default", a)
		return ss != nil && len(ss.Entries) == 3 && len(ss.Subscriptions) == 1
	})
	if wholes(formHeadAndRecords) != 1 {
		t.Fatalf("head-and-records writes %v; want 1", wholes(formHeadAndRecords))
	}

	b := clientIn(1, 0, "b")
	h.setLease("default", b, "n1", 7)
	n1.rep.OpenWhole("default", b, causeImport)(bigState(b, 7, 200, 0), true)
	if wholes(formTooLarge) != 1 {
		t.Fatalf("too-large writes %v; want 1", wholes(formTooLarge))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	keyB := []store.Scope{{Namespace: "default", ClientID: b, Epoch: 7}}
	if err := n1.rep.Barrier(ctx, keyB); err == nil {
		t.Fatal("a barrier passed naming a key too large for a frame")
	}
	sent := n1.rep.outboundFor("n2").lastSent()
	if err := n1.own.Subscribe(store.Key{Namespace: "default", ClientID: b, Epoch: 7}, store.SubscriptionRecord{Filter: "x", QoS: 1}); err != nil {
		t.Fatal(err)
	}
	if got := n1.rep.outboundFor("n2").lastSent(); got != sent {
		t.Fatalf("a record of a key held refused for its size was sent (%d → %d)", sent, got)
	}
	n1.rep.outboundFor("n2").broken()
	n1.rep.resync("n2")
	h.waitFor("n1's resync of n2 to end", 5*time.Second, func() bool { return len(n1.rep.HoledStreams()) == 0 })
	if err := n1.rep.Barrier(ctx, keyB); err == nil {
		t.Fatal("a resync's end let a barrier pass naming a key too large for a frame")
	}

	// It shrinks: twice inside one interval, the second bringing it under.
	n1.sessions.mu.Lock()
	n1.sessions.sessions = []*store.SessionState{bigState(b, 7, 100, 0)}
	n1.sessions.mu.Unlock()
	if err := n1.own.Unsubscribe(store.Key{Namespace: "default", ClientID: b, Epoch: 7}, "x"); err != nil {
		t.Fatal(err)
	}
	h.waitFor("b decided again, still too large", 5*time.Second, func() bool {
		return sinks["n1"].get("hoardmq_replication_whole_writes_total", map[string]string{"cause": causeRepair, "form": formTooLarge}) >= 1
	})
	n1.sessions.mu.Lock()
	n1.sessions.sessions = []*store.SessionState{bigState(b, 7, 1, 0)}
	n1.sessions.mu.Unlock()
	if err := n1.own.Unsubscribe(store.Key{Namespace: "default", ClientID: b, Epoch: 7}, "y"); err != nil {
		t.Fatal(err)
	}
	h.waitFor("n2 to hold b whole once it fits", 5*time.Second, func() bool {
		ss := h.nodes["n2"].rep.ReplicaState().Find("default", b)
		return ss != nil && len(ss.Subscriptions) == 1
	})
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	if err := n1.rep.Barrier(ctx2, keyB); err != nil {
		t.Fatalf("after b was sent whole: %v", err)
	}
}

// TestAnOwnerHeldRefusalEndsOffTheTargetsAndOnAFittingWrite: a key this node
// holds refused at a peer stops being refused there once the reconcile worker
// acts on a map under which the peer is not one of its targets, and on every
// stream once a whole write of it that fits is sent anywhere.
func TestAnOwnerHeldRefusalEndsOffTheTargetsAndOnAFittingWrite(t *testing.T) {
	h, _, _ := threeAt(t, false, quickAck)
	n1 := h.nodes["n1"]
	id := clientIn(1, 0)
	k := store.Scope{Namespace: "default", ClientID: id}
	n1.rep.outboundFor("n2").setHeld(k, heldUnwritten)
	n1.rep.outboundFor("n3").setHeld(k, heldUnwritten)
	n1.rep.pushWhole(stateOf(k, 7), 7, []string{"n3"}, causeRepair, nil)
	if _, held := n1.rep.outboundFor("n2").heldWhy(k); held {
		t.Fatal("a whole write that fits, sent to n3, left the key held refused at n2")
	}
	n1.rep.outboundFor("n2").setHeld(k, heldTooLarge)
	h.applyMap(shardmap.Compute(2, []string{"n1", "n3"}, 2, 1))
	h.waitFor("n1 to end the refusal held at n2, no longer a target", 5*time.Second, func() bool {
		_, held := n1.rep.outboundFor("n2").heldWhy(k)
		return !held
	})
}

// TestAnOwnerHeldRefusalAtTheCapBlocksTheStream: an owner-held refusal counts
// against the refused set's cap; one more key at the cap blocks the stream
// whole and asks for a resync, as a peer's refusal at the cap does.
func TestAnOwnerHeldRefusalAtTheCapBlocksTheStream(t *testing.T) {
	p := bareStream()
	p.refused = make(map[store.Scope]refusal, maxRefusedTracked)
	for i := range maxRefusedTracked - 1 {
		p.refused[store.Scope{Namespace: "default", ClientID: fmt.Sprintf("r%d", i)}] = refusal{epoch: 1, class: "other"}
	}
	if p.setHeld(keyK, heldTooLarge) {
		t.Fatal("the last key under the cap blocked the stream")
	}
	other := store.Scope{Namespace: "default", ClientID: "over"}
	if !p.setHeld(other, heldTooLarge) {
		t.Fatal("a key past the cap, owner-held entries counted, did not ask for a resync")
	}
	if err := p.refusal([]store.Scope{{Namespace: "default", ClientID: "unrelated"}}); err != nil {
		t.Fatalf("the refusal check of an unrelated key: %v", err)
	}
	if !p.streamBlocked {
		t.Fatal("the stream is not blocked whole at the cap")
	}
}
