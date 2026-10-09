package store

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/happydave/hoardmq/internal/message"
)

// TestApplyReport: a record that names a session the state does not hold, or
// an entry the session does not hold, is reported; a record that does nothing
// by design is not; and reporting changes nothing about what is applied.
func TestApplyReport(t *testing.T) {
	const ns, id = "d", "c"
	held := func() *State {
		st := &State{}
		st.add(&SessionState{
			SessionMeta:   SessionMeta{Namespace: ns, ClientID: id},
			Subscriptions: []SubscriptionRecord{{Filter: "a/#"}},
			InboundQoS2:   []uint16{7},
			Entries: []Entry{
				{Seq: 1, Message: &message.Message{Topic: "t"}, QoS: 1, State: StateQueued},
				{Seq: 2, Message: &message.Message{Topic: "t"}, QoS: 2, PacketID: 9, State: StatePublishSent},
			},
		})
		return st
	}
	empty := func() *State { return &State{} }
	rec := func(v any) []byte {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	for _, c := range []struct {
		name    string
		state   func() *State
		typ     byte
		payload []byte
		want    Unapplied
	}{
		{"upsert creates", empty, recSessionUpsert, rec(SessionMeta{Namespace: ns, ClientID: id}), ""},
		{"delete of an absent session", empty, recSessionDelete, rec(sessionKey{ns, id}), ""},
		{"subscribe, held", held, recSubscribe, rec(subscribeRecord{ns, id, SubscriptionRecord{Filter: "b"}}), ""},
		{"subscribe, no session", empty, recSubscribe, rec(subscribeRecord{ns, id, SubscriptionRecord{Filter: "b"}}), UnappliedNoSession},
		{"unsubscribe of an absent filter", held, recUnsubscribe, rec(unsubscribeRecord{ns, id, "zz"}), ""},
		{"unsubscribe, no session", empty, recUnsubscribe, rec(unsubscribeRecord{ns, id, "a/#"}), ""},
		{"inbound add, already present", held, recInboundAdd, rec(inboundRecord{Namespace: ns, ClientID: id, PacketID: 7}), ""},
		{"inbound add, no session", empty, recInboundAdd, rec(inboundRecord{Namespace: ns, ClientID: id, PacketID: 7}), UnappliedNoSession},
		{"inbound remove, absent", held, recInboundRemove, rec(inboundRecord{Namespace: ns, ClientID: id, PacketID: 8}), ""},
		{"inbound remove, no session", empty, recInboundRemove, rec(inboundRecord{Namespace: ns, ClientID: id, PacketID: 7}), ""},
		{"enqueue, held", held, recEnqueue, rec(enqueueRecord{ns, id, Entry{Seq: 3}}), ""},
		{"enqueue, no session", empty, recEnqueue, rec(enqueueRecord{ns, id, Entry{Seq: 3}}), UnappliedNoSession},
		{"sent, held", held, recSent, rec(sentRecord{ns, id, 1, 5}), ""},
		{"sent, no entry", held, recSent, rec(sentRecord{ns, id, 99, 5}), UnappliedNoEntry},
		{"sent, no session", empty, recSent, rec(sentRecord{ns, id, 1, 5}), UnappliedNoSession},
		{"inflight state, held", held, recInflightState, rec(stateRecord{ns, id, 9, 2}), ""},
		{"inflight state, no entry", held, recInflightState, rec(stateRecord{ns, id, 99, 2}), UnappliedNoEntry},
		{"inflight state, no session", empty, recInflightState, rec(stateRecord{ns, id, 9, 2}), UnappliedNoSession},
		{"complete, held", held, recComplete, rec(completeRecord{ns, id, 9}), ""},
		{"complete, no entry", held, recComplete, rec(completeRecord{ns, id, 99}), UnappliedNoEntry},
		{"complete, no session", empty, recComplete, rec(completeRecord{ns, id, 9}), UnappliedNoSession},
		{"dropped, held", held, recDropped, rec(droppedRecord{ns, id, 1}), ""},
		{"dropped, no entry", held, recDropped, rec(droppedRecord{ns, id, 99}), UnappliedNoEntry},
		{"dropped, no session", empty, recDropped, rec(droppedRecord{ns, id, 1}), UnappliedNoSession},
		{"retain", empty, recRetain, rec(message.Message{Namespace: ns, Topic: "r", Payload: []byte("x")}), ""},
		{"retain delete of an absent topic", empty, recRetainDelete, rec(retainKey{ns, "r"}), ""},
	} {
		reported, plain := c.state(), c.state()
		got, err := reported.ApplyReport(c.typ, c.payload)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if got != c.want {
			t.Errorf("%s: reported %q, want %q", c.name, got, c.want)
		}
		if err := plain.Apply(c.typ, c.payload); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if !reflect.DeepEqual(reported, plain) {
			t.Errorf("%s: ApplyReport and Apply left different states", c.name)
		}
	}
	if _, err := empty().ApplyReport(recEnqueue, []byte("{")); err == nil {
		t.Error("a record that does not parse was not an error")
	}
}

// TestUpsertAtAHigherEpochKeepsTheCopy: an upsert updates the meta and
// nothing else, whatever its epoch — a client reconnecting to its own owner
// bumps the epoch without the session changing hands. A copy is replaced
// only by the delete an import writes before the session whole.
func TestUpsertAtAHigherEpochKeepsTheCopy(t *testing.T) {
	const ns, id = "d", "c"
	st := &State{}
	rec := func(v any) []byte {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	apply := func(typ byte, v any) {
		if err := st.Apply(typ, rec(v)); err != nil {
			t.Fatal(err)
		}
	}
	seqs := func() []uint64 {
		var out []uint64
		for _, e := range st.find(ns, id).Entries {
			out = append(out, e.Seq)
		}
		return out
	}
	apply(recSessionUpsert, SessionMeta{Namespace: ns, ClientID: id, Epoch: 1})
	apply(recEnqueue, enqueueRecord{ns, id, Entry{Seq: 1}})
	apply(recSessionUpsert, SessionMeta{Namespace: ns, ClientID: id, Epoch: 2}) // the reconnect
	apply(recEnqueue, enqueueRecord{ns, id, Entry{Seq: 2}})
	if got := seqs(); !reflect.DeepEqual(got, []uint64{1, 2}) || st.find(ns, id).Epoch != 2 {
		t.Fatalf("after an upsert at a higher epoch the copy holds %v at epoch %d, want [1 2] at 2", got, st.find(ns, id).Epoch)
	}
	// The import: delete, then whole.
	apply(recSessionDelete, sessionKey{ns, id})
	apply(recSessionUpsert, SessionMeta{Namespace: ns, ClientID: id, Epoch: 3})
	apply(recEnqueue, enqueueRecord{ns, id, Entry{Seq: 2}})
	if got := seqs(); !reflect.DeepEqual(got, []uint64{2}) || st.find(ns, id).Epoch != 3 {
		t.Fatalf("after an import the copy holds %v at epoch %d, want [2] at 3", got, st.find(ns, id).Epoch)
	}
}
