package replication

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/ext/mqtt"
	extstore "github.com/happydave/hoardmq/ext/store"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

// fullMessage is a message with every member set, the ones JSON skips
// included.
func fullMessage(topic, payload string) *message.Message {
	format, ctype, resp := byte(1), "text/plain", "r/"+topic
	return &message.Message{
		Namespace: "ns", Topic: topic, Payload: []byte(payload), QoS: 1, Retain: true,
		PayloadFormat: &format, ContentType: &ctype, ResponseTopic: &resp,
		CorrelationData: []byte("corr-" + topic),
		UserProperties:  []mqtt.UserProperty{{Name: "k", Value: "v-" + topic}},
		Expiry:          time.Date(2026, 11, 12, 18, 0, 0, 5, time.FixedZone("", -7*3600)),
		Publisher:       message.Publisher{Namespace: "ns", ClientID: "pub", Username: "u", Node: "n1"},
		Seq:             9, Arrived: time.Date(2026, 11, 12, 17, 0, 0, 7, time.UTC),
		OriginCluster: "c", Hops: 2,
		Forwarded: true, Forwards: []message.ForwardRef{{Peer: "n2", Seq: 4}},
		Trace: mqtt.SpanContext{TraceID: [16]byte{1}, SpanID: [8]byte{2}, Flags: 1, State: "s"},
	}
}

// populatedFollower is a follower over a replica store whose state was built
// through the apply paths with every record type, plus the members no record
// sets: session a holds subscriptions, inbound identifiers and entries in each
// state, one of them a message with no payload; b held entries and was deleted; c's record was refused for want of a
// copy; d holds nil slices; and two retained topics, one versioned and one
// cleared.
func populatedFollower(t *testing.T) *follower {
	t.Helper()
	f := measureFollower(t)
	f.mu.Lock()
	f.state.Sessions = append(f.state.Sessions, &store.SessionState{
		SessionMeta: store.SessionMeta{Namespace: "ns", ClientID: "d", Epoch: 3}})
	f.mu.Unlock()

	must := func(rec store.Record, err error) store.Record {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return rec
	}
	apply := func(rec store.Record) {
		t.Helper()
		if _, err := f.apply(rec); err != nil {
			t.Fatal(err)
		}
	}
	a := store.Key{Namespace: "ns", ClientID: "a"}
	b := store.Key{Namespace: "ns", ClientID: "b"}
	will := &store.WillRecord{Message: fullMessage("will", "gone"), Delay: 5, Armed: true}
	apply(must(extstore.EncodeSessionUpsert(store.SessionMeta{Namespace: "ns", ClientID: "a", Epoch: 7, Version: 5,
		Username: "u", ExpiryInterval: 3600, ExpiresAt: time.Date(2026, 11, 12, 19, 0, 0, 3, time.FixedZone("", -7*3600)), Will: will})))
	apply(must(extstore.EncodeSubscribe(a, store.SubscriptionRecord{Filter: "x/#", QoS: 1, NoLocal: true, RetainAsPublished: true, RetainHandling: 2, Identifier: 11})))
	apply(must(extstore.EncodeSubscribe(a, store.SubscriptionRecord{Filter: "y", QoS: 2})))
	apply(must(extstore.EncodeSubscribe(a, store.SubscriptionRecord{Filter: "z", QoS: 0})))
	apply(must(extstore.EncodeUnsubscribe(a, "z")))
	apply(must(extstore.EncodeInboundQoS2(a, 21, true)))
	apply(must(extstore.EncodeInboundQoS2(a, 22, true)))
	apply(must(extstore.EncodeInboundAccepted(a, 22)))
	apply(must(extstore.EncodeInboundQoS2(a, 23, true)))
	apply(must(extstore.EncodeInboundQoS2(a, 23, false)))
	for seq := uint64(1); seq <= 4; seq++ {
		apply(must(extstore.EncodeEnqueue(a, store.Entry{Seq: seq, Message: fullMessage(fmt.Sprintf("t%d", seq), fmt.Sprintf("p%d", seq)),
			QoS: 2, Identifiers: []uint32{11, uint32(seq)}, Retain: seq == 1, Group: "g"})))
	}
	bare := &message.Message{Namespace: "ns", Topic: "t5", QoS: 1}
	apply(must(extstore.EncodeEnqueue(a, store.Entry{Seq: 5, Message: bare, QoS: 1})))
	apply(must(extstore.EncodeSent(a, 1, 31)))
	apply(must(extstore.EncodeSent(a, 2, 32)))
	apply(must(extstore.EncodeInflightState(a, 31, store.StatePubrecReceived)))
	apply(must(extstore.EncodeComplete(a, 32)))
	apply(must(extstore.EncodeDropped(a, 3)))

	apply(must(extstore.EncodeSessionUpsert(store.SessionMeta{Namespace: "ns", ClientID: "b", Epoch: 2})))
	apply(must(extstore.EncodeEnqueue(b, store.Entry{Seq: 1, Message: fullMessage("tb", "pb"), QoS: 1})))
	apply(must(extstore.EncodeSessionDelete(b)))

	refused := must(extstore.EncodeSubscribe(store.Key{Namespace: "ns", ClientID: "c"}, store.SubscriptionRecord{Filter: "c", QoS: 1}))
	if un, err := f.applyChecked(refused, nil, holdApplyOther, "", "", true); err != nil || un != store.UnappliedNoSession {
		t.Fatalf("precondition: c's record applied as %q, %v; want refused no-session", un, err)
	}

	apply(must(extstore.EncodeRetain(fullMessage("r1", "kept"), store.Version{E: 1, N: 2})))
	apply(must(extstore.EncodeRetain(fullMessage("r2", "cleared"), store.Version{E: 1, N: 3})))
	apply(must(extstore.EncodeRetainClear("ns", "r2", store.Version{E: 1, N: 4})))

	f.mu.Lock()
	ss := f.state.Find("ns", "a")
	ss.Delivered = []message.ID{{Node: "n1", Seq: 5, Arrived: 6}}
	ss.Entries[0].Message.Forwards = []message.ForwardRef{{Peer: "n3", Seq: 8}}
	f.state.Through = []store.SessionIndex{{Namespace: "ns", ClientID: "a", Index: 99}}
	f.mu.Unlock()
	return f
}

// roundTrip is the copy the follower made through JSON before: encode, decode.
func roundTrip(t *testing.T, st *store.State) *store.State {
	t.Helper()
	data, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	out := store.NewState()
	if err := json.Unmarshal(data, out); err != nil {
		t.Fatal(err)
	}
	return out
}

func encoded(t *testing.T, st *store.State) []byte {
	t.Helper()
	data, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// sameMembers compares every exported member of two states, the ones JSON
// skips included; the unexported index is the copy's own to build.
func sameMembers(a, b *store.State) bool {
	return reflect.DeepEqual(a.Sessions, b.Sessions) && reflect.DeepEqual(a.Retained, b.Retained) &&
		reflect.DeepEqual(a.RetainedVersions, b.RetainedVersions) && reflect.DeepEqual(a.RetainedTombstones, b.RetainedTombstones) &&
		reflect.DeepEqual(a.Through, b.Through)
}

// TestCheckpointCopyEncodesAsTheRoundTrip: the checkpoint's copy and the
// rebalance's encode byte for byte as the JSON round trip of the same state,
// hold every member of it, and the checkpoint's index is read inside the hold.
func TestCheckpointCopyEncodesAsTheRoundTrip(t *testing.T) {
	f := populatedFollower(t)
	f.mu.Lock()
	want := encoded(t, roundTrip(t, f.state))
	live := f.state
	f.mu.Unlock()
	if !bytes.Contains(want, []byte(`"subscriptions":null`)) || !bytes.Contains(want, []byte(`"inboundAccepted":[22]`)) ||
		!bytes.Contains(want, []byte(`"Payload":null`)) ||
		!bytes.Contains(want, []byte(`"retainedTombstones":[`)) || bytes.Contains(want, []byte(`"id":"b"`)) || bytes.Contains(want, []byte(`"id":"c"`)) {
		t.Fatalf("precondition: the populated state is not what the fixture built: %s", want)
	}

	checkpoints, _ := holdsOf(f, holdCheckpoint)
	rebalances, _ := holdsOf(f, holdSnapshotRebalance)
	insideHold := false
	cp := f.checkpointState(func() uint64 {
		insideHold = !f.mu.TryLock()
		if !insideHold {
			f.mu.Unlock()
		}
		return 42
	})
	if cp.At != 42 || !insideHold {
		t.Fatalf("checkpoint index %d (want 42), read inside the hold %v", cp.At, insideHold)
	}
	if got := encoded(t, cp); !bytes.Equal(got, want) {
		t.Fatalf("the checkpoint's copy encodes differently from the round trip:\n got %s\nwant %s", got, want)
	}
	rb := f.snapshot(holdSnapshotRebalance)
	if c, _ := holdsOf(f, holdCheckpoint); c != checkpoints+1 {
		t.Fatalf("the checkpoint's copy was traced as %d checkpoint holds, want 1", c-checkpoints)
	}
	if c, _ := holdsOf(f, holdSnapshotRebalance); c != rebalances+1 {
		t.Fatalf("the rebalance's copy was traced as %d rebalance holds, want 1", c-rebalances)
	}
	f.mu.Lock()
	cpSame, rbSame := sameMembers(cp, live), sameMembers(rb, live)
	f.mu.Unlock()
	if !cpSame {
		t.Fatalf("the checkpoint's copy differs from the live state in a member")
	}
	if got := encoded(t, rb); !bytes.Equal(got, want) || !rbSame {
		t.Fatalf("the rebalance's copy differs from the round trip (%v) or the live state (%v):\n got %s\nwant %s", bytes.Equal(got, want), rbSame, got, want)
	}
}

// TestCheckpointCopyUnchangedByLaterApplies: changes made under the follower's
// mutex after the checkpoint's and the rebalance's copies, as applies make them
// in place, never reach either.
func TestCheckpointCopyUnchangedByLaterApplies(t *testing.T) {
	f := populatedFollower(t)
	cp := f.checkpointState(func() uint64 { return 1 })
	before := encoded(t, cp)
	rb := f.snapshot(holdSnapshotRebalance)
	var forwards []message.ForwardRef
	for _, ss := range cp.Sessions {
		if ss.ClientID == "a" {
			forwards = ss.Entries[0].Message.Forwards
		}
	}

	f.mu.Lock()
	ss := f.state.Find("ns", "a")
	ss.Subscriptions[0].QoS = 2
	ss.InboundQoS2[0] = 99
	ss.InboundAccepted[0] = 98
	ss.Delivered[0].Seq = 97
	ss.Entries[0].Identifiers[0] = 96
	m := ss.Entries[0].Message
	m.Payload[0] = 'Z'
	*m.ContentType = "changed"
	m.CorrelationData[0] = 'Z'
	m.UserProperties[0].Value = "changed"
	m.Forwards[0].Seq = 95
	ss.Will.Message.Payload[0] = 'Z'
	f.state.Retained[0].Payload[0] = 'Z'
	f.state.RetainedVersions[0].RV.N = 94
	f.state.RetainedTombstones[0].RV.N = 93
	f.state.Through[0].Index = 92
	f.mu.Unlock()
	a := store.Key{Namespace: "ns", ClientID: "a"}
	for _, rec := range []func() (store.Record, error){
		func() (store.Record, error) {
			return extstore.EncodeSubscribe(a, store.SubscriptionRecord{Filter: "x/#", QoS: 0})
		},
		func() (store.Record, error) { return extstore.EncodeInflightState(a, 31, store.StatePublishSent) },
		func() (store.Record, error) { return extstore.EncodeSent(a, 4, 34) },
		func() (store.Record, error) {
			return extstore.EncodeSessionUpsert(store.SessionMeta{Namespace: "ns", ClientID: "a", Epoch: 8})
		},
		func() (store.Record, error) {
			return extstore.EncodeRetain(fullMessage("r1", "replaced"), store.Version{E: 2, N: 1})
		},
	} {
		r, err := rec()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.apply(r); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.apply(store.DeleteRecord("ns", "d")); err != nil {
		t.Fatal(err)
	}

	if after := encoded(t, cp); !bytes.Equal(after, before) {
		t.Fatalf("the copy changed with the live state:\nbefore %s\n after %s", before, after)
	}
	if after := encoded(t, rb); !bytes.Equal(after, before) {
		t.Fatalf("the rebalance's copy changed with the live state:\nbefore %s\n after %s", before, after)
	}
	if len(forwards) != 1 || forwards[0].Seq != 8 {
		t.Fatalf("the copy's unencoded forwards changed with the live state: %v", forwards)
	}
}

// TestCheckpointCopyRacesNoApply is a control for the race detector: copies
// are taken and encoded outside the mutex while applies change the same
// sessions in place under it. Run under -race it reports any member the copy
// shares; without it, it checks each copy holds both sessions.
func TestCheckpointCopyRacesNoApply(t *testing.T) {
	f := populatedFollower(t)
	a := store.Key{Namespace: "ns", ClientID: "a"}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			recs := []func() (store.Record, error){
				func() (store.Record, error) {
					return extstore.EncodeSubscribe(a, store.SubscriptionRecord{Filter: "x/#", QoS: byte(i % 3)})
				},
				func() (store.Record, error) { return extstore.EncodeInflightState(a, 31, byte(1+i%2)) },
			}
			for _, rec := range recs {
				r, err := rec()
				if err == nil {
					_, err = f.apply(r)
				}
				if err != nil {
					t.Error(err)
					return
				}
			}
			f.mu.Lock()
			if ss := f.state.Find("ns", "a"); ss != nil {
				ss.Entries[0].Message.Payload[0] = byte('a' + i%26)
				ss.Will.Message.Payload[0] = byte('a' + i%26)
				ss.InboundQoS2[0] = uint16(i)
			}
			f.state.Retained[0].Payload[0] = byte('a' + i%26)
			f.mu.Unlock()
		}
	}()
	for range 200 {
		for _, cp := range []*store.State{f.checkpointState(func() uint64 { return 1 }), f.snapshot(holdSnapshotRebalance)} {
			out := store.NewState()
			if err := json.Unmarshal(encoded(t, cp), out); err != nil || len(out.Sessions) != 2 {
				t.Errorf("a copy decoded to %d sessions, %v; want 2", len(out.Sessions), err)
			}
		}
	}
	close(stop)
	wg.Wait()
}

// TestCopyStateCopiesEveryMember fills every exported member of the state and
// of every type reachable from it, by reflection, and finds the copy equal and
// sharing no slice, map or pointer with the original. A member added to any of
// those types and not handled by the copy fails here: left zero, it fails the
// equality; shared, the walk.
func TestCopyStateCopiesEveryMember(t *testing.T) {
	st := store.NewState()
	fill(t, reflect.ValueOf(st).Elem(), "State")
	cp := copyState(st)
	if !reflect.DeepEqual(cp, st) {
		t.Fatalf("the copy of a filled state differs from it:\n got %+v\nwant %+v", cp, st)
	}
	if shared := sharedRefs(reflect.ValueOf(cp), reflect.ValueOf(st), "State"); len(shared) > 0 {
		t.Fatalf("the copy shares memory with the original at %v", shared)
	}
}

var locationType = reflect.TypeOf((*time.Location)(nil))

// fill sets every settable member under v to a non-zero value: two elements
// for a slice, a fresh value for a pointer.
func fill(t *testing.T, v reflect.Value, path string) {
	t.Helper()
	if v.Type() == reflect.TypeOf(time.Time{}) {
		v.Set(reflect.ValueOf(time.Date(2026, 11, 12, 18, 0, 0, len(path), time.FixedZone("", -7*3600))))
		return
	}
	switch v.Kind() {
	case reflect.String:
		v.SetString(path)
	case reflect.Bool:
		v.SetBool(true)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(int64(1 + len(path)%100))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		v.SetUint(uint64(1 + len(path)%100))
	case reflect.Float32, reflect.Float64:
		v.SetFloat(1.5)
	case reflect.Pointer:
		p := reflect.New(v.Type().Elem())
		fill(t, p.Elem(), path+"*")
		v.Set(p)
	case reflect.Slice:
		s := reflect.MakeSlice(v.Type(), 2, 2)
		for i := range 2 {
			fill(t, s.Index(i), fmt.Sprintf("%s[%d]", path, i))
		}
		v.Set(s)
	case reflect.Array:
		for i := range v.Len() {
			fill(t, v.Index(i), fmt.Sprintf("%s[%d]", path, i))
		}
	case reflect.Map:
		m := reflect.MakeMap(v.Type())
		k, e := reflect.New(v.Type().Key()).Elem(), reflect.New(v.Type().Elem()).Elem()
		fill(t, k, path+".key")
		fill(t, e, path+".value")
		m.SetMapIndex(k, e)
		v.Set(m)
	case reflect.Struct:
		for i := range v.NumField() {
			if v.Type().Field(i).IsExported() {
				fill(t, v.Field(i), path+"."+v.Type().Field(i).Name)
			}
		}
	default:
		t.Fatalf("fill: %s is a %s, which this test does not fill; extend it", path, v.Kind())
	}
}

// sharedRefs lists the paths at which a and b, walked side by side, hold the
// same non-empty slice array, map or pointer. Strings and time zones are
// immutable and may be shared.
func sharedRefs(a, b reflect.Value, path string) []string {
	var out []string
	switch a.Kind() {
	case reflect.Pointer:
		if a.IsNil() || b.IsNil() {
			return nil
		}
		if a.Pointer() == b.Pointer() && a.Type() != locationType {
			out = append(out, path)
		}
		if a.Type() == locationType {
			return out
		}
		out = append(out, sharedRefs(a.Elem(), b.Elem(), path+"*")...)
	case reflect.Slice:
		if a.Cap() > 0 && b.Cap() > 0 && a.Pointer() == b.Pointer() {
			out = append(out, path)
		}
		for i := range min(a.Len(), b.Len()) {
			out = append(out, sharedRefs(a.Index(i), b.Index(i), fmt.Sprintf("%s[%d]", path, i))...)
		}
	case reflect.Map:
		if !a.IsNil() && !b.IsNil() && a.Pointer() == b.Pointer() {
			out = append(out, path)
		}
	case reflect.Array:
		for i := range a.Len() {
			out = append(out, sharedRefs(a.Index(i), b.Index(i), fmt.Sprintf("%s[%d]", path, i))...)
		}
	case reflect.Struct:
		for i := range a.NumField() {
			out = append(out, sharedRefs(a.Field(i), b.Field(i), path+"."+a.Type().Field(i).Name)...)
		}
	case reflect.Interface:
		if !a.IsNil() && !b.IsNil() {
			out = append(out, sharedRefs(a.Elem(), b.Elem(), path)...)
		}
	}
	return out
}

// TestCheckpointCopyOfAnEmptyState: a follower holding nothing, with its lists
// empty or nil, copies to what the round trip gives: [] stays [] and null
// stays null.
func TestCheckpointCopyOfAnEmptyState(t *testing.T) {
	for _, st := range []*store.State{store.NewState(), {}} {
		f := measureFollower(t)
		f.state = st
		want := encoded(t, roundTrip(t, st))
		if got := encoded(t, f.checkpointState(func() uint64 { return 0 })); !bytes.Equal(got, want) {
			t.Fatalf("an empty state's copy encodes as %s, want %s", got, want)
		}
	}
}
