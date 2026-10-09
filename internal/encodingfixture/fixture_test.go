// Package encodingfixture holds the bytes the broker writes for one value of
// each kind it persists or sends: log records, a checkpoint, the message
// envelope, records-store values and packets. The test fails on any byte that
// differs from the golden file, so moving a type between packages cannot
// change an encoding unnoticed.
package encodingfixture

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/records"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

var update = flag.Bool("update", false, "rewrite testdata/encodings.txt from the current encoders")

const golden = "testdata/encodings.txt"

var at = time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)

func ptr[T any](v T) *T { return &v }

// envelope is a message with every member the envelope carries set.
func envelope() *message.Message {
	return &message.Message{
		Namespace:       "default",
		Topic:           "plant/7/temp",
		Payload:         []byte("21.5"),
		QoS:             1,
		Retain:          true,
		PayloadFormat:   ptr(byte(1)),
		ContentType:     ptr("text/plain"),
		ResponseTopic:   ptr("plant/7/reply"),
		CorrelationData: []byte{1, 2, 3},
		UserProperties:  []packet.UserProperty{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}},
		Expiry:          at.Add(time.Hour),
		Publisher:       message.Publisher{Namespace: "default", ClientID: "pub", Username: "u", Node: "n1"},
		Seq:             42,
		Arrived:         at,
		OriginCluster:   "c1",
		Hops:            2,
	}
}

// encodings returns each named value's bytes.
func encodings(t *testing.T) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	var tapped []store.Record
	s, err := store.Open(t.TempDir(), store.Options{Tap: func(r store.Record) func() {
		r.Payload = bytes.Clone(r.Payload)
		tapped = append(tapped, r)
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	k := store.Key{Namespace: "default", ClientID: "sub", Epoch: 7}
	m := envelope()
	steps := []func() error{
		func() error {
			return s.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: "sub", Epoch: 7, Version: 5, Username: "u",
				ExpiryInterval: 3600, ExpiresAt: at.Add(2 * time.Hour), Will: &store.WillRecord{Message: m, Delay: 5, Armed: true}})
		},
		func() error {
			return s.Subscribe(k, store.SubscriptionRecord{Filter: "plant/+/temp", QoS: 2, NoLocal: true, RetainAsPublished: true, RetainHandling: 1, Identifier: 9})
		},
		func() error { return s.Unsubscribe(k, "old/#") },
		func() error { return s.InboundQoS2(k, 11, true) },
		func() error { return s.InboundAccepted(k, 11) },
		func() error { return s.InboundQoS2(k, 11, false) },
		func() error {
			return s.Enqueue(k, store.Entry{Seq: 3, Message: m, QoS: 1, Identifiers: []uint32{9}, Retain: true, Group: "g", State: store.StateQueued})
		},
		func() error { return s.Sent(k, 3, 12) },
		func() error { return s.InflightState(k, 12, store.StatePubrecReceived) },
		func() error { return s.Complete(k, 12) },
		func() error { return s.Dropped(k, 4) },
		func() error { return s.Enqueue(k, store.Entry{Seq: 5, Message: m, QoS: 2, State: store.StateQueued}) },
		func() error { return s.Retain(m) },
		func() error { return s.RetainDelete("default", "plant/7/temp") },
		func() error { return s.SessionDelete(k) },
	}
	for i, step := range steps {
		if err := step(); err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
	}
	for i, r := range tapped {
		name := "record/" + store.RecordName(r.Type)
		if _, dup := out[name]; dup {
			name += "/" + string(rune('a'+i))
		}
		out[name] = append([]byte{r.Type}, r.Payload...)
	}

	// The checkpoint holds the session with its subscription, an open inbound
	// identifier, an inflight entry past PUBREC and a queued one, and a
	// retained message.
	st := store.NewState()
	for _, i := range []int{0, 1, 3, 6, 7, 8, 11, 12} {
		if err := st.Apply(tapped[i].Type, tapped[i].Payload); err != nil {
			t.Fatal(err)
		}
	}
	st.Normalize()
	out["checkpoint/state"] = mustJSON(t, st)

	out["envelope"] = mustJSON(t, m)
	rec := records.Record{Value: json.RawMessage(`{"k":"v"}`), Version: 9, Created: 3, By: "admin", At: at}
	out["records/record"] = mustJSON(t, rec)
	out["records/entry"] = mustJSON(t, records.Entry{Key: "alice", Record: rec})
	out["records/result"] = mustJSON(t, records.Result{OK: true, Index: 10, Version: 10, Previous: &rec})

	enc := packet.Encoder{Version: packet.V5}
	for name, p := range map[string]packet.Packet{
		"packet/publish": m.ToPublish(message.Delivery{QoS: 1, PacketID: 5, SubscriptionIdentifiers: []uint32{9}}, at),
		"packet/subscribe": &packet.Subscribe{PacketID: 6, Properties: packet.Properties{SubscriptionIdentifiers: []uint32{9}},
			Subscriptions: []packet.Subscription{{Filter: "a/#", QoS: 2, NoLocal: true, RetainHandling: 2}}},
		"packet/connack": &packet.Connack{SessionPresent: true, ReasonCode: packet.Success,
			Properties: packet.Properties{ReceiveMaximum: ptr(uint16(100)), MaximumQoS: ptr(byte(1))}},
	} {
		var b bytes.Buffer
		if err := enc.Encode(&b, p); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		out[name] = b.Bytes()
	}
	return out
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func render(m map[string][]byte) []byte {
	names := make([]string, 0, len(m))
	for n := range m {
		names = append(names, n)
	}
	sort.Strings(names)
	var b strings.Builder
	for _, n := range names {
		b.WriteString(n)
		b.WriteByte('\t')
		b.WriteString(hex.EncodeToString(m[n]))
		b.WriteByte('\n')
	}
	return []byte(b.String())
}

func TestEncodingsUnchanged(t *testing.T) {
	got := encodings(t)
	if *update {
		if err := os.MkdirAll(filepath.Dir(golden), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(golden, render(got), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	data, err := os.ReadFile(golden)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		name, h, ok := strings.Cut(line, "\t")
		if !ok {
			t.Fatalf("malformed golden line %q", line)
		}
		want[name] = h
	}
	for name, h := range want {
		b, ok := got[name]
		if !ok {
			t.Errorf("%s: no longer produced", name)
			continue
		}
		if g := hex.EncodeToString(b); g != h {
			wb, _ := hex.DecodeString(h)
			t.Errorf("%s changed:\n got %q\nwant %q", name, b, wb)
		}
	}
	for name := range got {
		if _, ok := want[name]; !ok {
			t.Errorf("%s: produced but not in %s (run with -update deliberately)", name, golden)
		}
	}
}
