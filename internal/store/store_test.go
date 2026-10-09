package store

import (
	"io"
	"log/slog"
	"os"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/message"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func openStore(t *testing.T, dir string, opts Options) *Store {
	t.Helper()
	opts.Logger = quiet()
	s, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func msg(ns, topic, payload string) *message.Message {
	return &message.Message{Namespace: ns, Topic: topic, Payload: []byte(payload), QoS: 1, Arrived: time.Unix(1, 0)}
}

func TestEveryRecordTypeRecovers(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir, Options{})
	meta := SessionMeta{Namespace: "default", ClientID: "c1", Epoch: 2, Version: 5, Username: "u", ExpiryInterval: 3600,
		ExpiresAt: time.Unix(2_000_000_000, 0).UTC(), Will: &WillRecord{Message: msg("default", "w", "bye"), Delay: 5, Armed: true}}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(s.SessionUpsert(meta))
	must(s.SessionUpsert(SessionMeta{Namespace: "default", ClientID: "gone", ExpiryInterval: 10}))
	must(s.SessionDelete(Key{Namespace: "default", ClientID: "gone"}))
	must(s.Subscribe(Key{Namespace: "default", ClientID: "c1"}, SubscriptionRecord{Filter: "a/#", QoS: 1, Identifier: 7}))
	must(s.Subscribe(Key{Namespace: "default", ClientID: "c1"}, SubscriptionRecord{Filter: "b", QoS: 0}))
	must(s.Subscribe(Key{Namespace: "default", ClientID: "c1"}, SubscriptionRecord{Filter: "a/#", QoS: 2, Identifier: 8})) // replace
	must(s.Unsubscribe(Key{Namespace: "default", ClientID: "c1"}, "b"))
	must(s.InboundQoS2(Key{Namespace: "default", ClientID: "c1"}, 11, true))
	must(s.InboundQoS2(Key{Namespace: "default", ClientID: "c1"}, 12, true))
	must(s.InboundQoS2(Key{Namespace: "default", ClientID: "c1"}, 11, false))
	must(s.Enqueue(Key{Namespace: "default", ClientID: "c1"}, Entry{Seq: 1, Message: msg("default", "a/x", "one"), QoS: 1, Identifiers: []uint32{8}}))
	must(s.Enqueue(Key{Namespace: "default", ClientID: "c1"}, Entry{Seq: 2, Message: msg("default", "a/y", "two"), QoS: 2, Group: "g/a/#"}))
	must(s.Enqueue(Key{Namespace: "default", ClientID: "c1"}, Entry{Seq: 3, Message: msg("default", "a/z", "three"), QoS: 1, PacketID: 9, State: StatePublishSent}))
	must(s.Enqueue(Key{Namespace: "default", ClientID: "c1"}, Entry{Seq: 4, Message: msg("default", "a/q", "dropped"), QoS: 1}))
	must(s.Sent(Key{Namespace: "default", ClientID: "c1"}, 2, 5))
	must(s.InflightState(Key{Namespace: "default", ClientID: "c1"}, 5, StatePubrecReceived))
	must(s.Complete(Key{Namespace: "default", ClientID: "c1"}, 9))
	must(s.Dropped(Key{Namespace: "default", ClientID: "c1"}, 4))
	must(s.Retain(msg("default", "r/1", "kept")))
	must(s.Retain(msg("default", "r/2", "temp")))
	must(s.RetainDelete("default", "r/2"))
	must(s.Retain(msg("default", "r/1", "kept-2")))
	// Records for an unknown session are ignored.
	must(s.Enqueue(Key{Namespace: "default", ClientID: "nobody"}, Entry{Seq: 1, Message: msg("default", "x", "y")}))
	must(s.Sync())
	must(s.Close())

	s2 := openStore(t, dir, Options{})
	defer func() { _ = s2.Close() }()
	st, err := s2.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Sessions) != 1 {
		t.Fatalf("sessions %d", len(st.Sessions))
	}
	c1 := st.Sessions[0]
	if !reflect.DeepEqual(c1.SessionMeta, meta) {
		t.Errorf("meta\n got %+v\nwant %+v", c1.SessionMeta, meta)
	}
	if len(c1.Subscriptions) != 1 || c1.Subscriptions[0].Filter != "a/#" || c1.Subscriptions[0].QoS != 2 || c1.Subscriptions[0].Identifier != 8 {
		t.Errorf("subscriptions %+v", c1.Subscriptions)
	}
	if !reflect.DeepEqual(c1.InboundQoS2, []uint16{12}) {
		t.Errorf("inbound %v", c1.InboundQoS2)
	}
	if len(c1.Entries) != 2 {
		t.Fatalf("entries %+v", c1.Entries)
	}
	if e := c1.Entries[0]; e.Seq != 1 || e.State != StateQueued || e.Identifiers[0] != 8 || string(e.Message.Payload) != "one" {
		t.Errorf("entry 1 %+v", e)
	}
	if e := c1.Entries[1]; e.Seq != 2 || e.PacketID != 5 || e.State != StatePubrecReceived || e.Group != "g/a/#" {
		t.Errorf("entry 2 %+v", e)
	}
	if len(st.Retained) != 1 || st.Retained[0].Topic != "r/1" || string(st.Retained[0].Payload) != "kept-2" {
		t.Errorf("retained %+v", st.Retained)
	}
}

// fixed is a snapshot that returns a state built before the checkpoint began,
// for tests that append nothing while it runs.
func fixed(st *State) Snapshot { return func(func() uint64) *State { return st } }

func TestCheckpointThenReplay(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir, Options{SegmentBytes: 1 << 20, CheckpointEvery: 3})
	_ = s.SessionUpsert(SessionMeta{Namespace: "default", ClientID: "a", ExpiryInterval: 1})
	_ = s.Subscribe(Key{Namespace: "default", ClientID: "a"}, SubscriptionRecord{Filter: "x"})
	_ = s.Retain(msg("default", "t", "v"))
	if !s.NeedsCheckpoint() {
		t.Fatal("checkpoint not requested after three records")
	}
	st, err := s.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(fixed(st)); err != nil {
		t.Fatal(err)
	}
	if s.NeedsCheckpoint() {
		t.Fatal("still requested after checkpoint")
	}
	// Records after the checkpoint are replayed on top of it.
	_ = s.Subscribe(Key{Namespace: "default", ClientID: "a"}, SubscriptionRecord{Filter: "y"})
	_ = s.RetainDelete("default", "t")
	_ = s.Close()
	s2 := openStore(t, dir, Options{})
	defer func() { _ = s2.Close() }()
	st2, err := s2.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if len(st2.Sessions) != 1 || len(st2.Sessions[0].Subscriptions) != 2 || len(st2.Retained) != 0 {
		t.Fatalf("%+v", st2)
	}
	// A checkpoint of an empty state recovers empty and marshals arrays.
	if err := s2.Checkpoint(fixed(NewState())); err != nil {
		t.Fatal(err)
	}
	st3, _ := s2.Recover()
	if len(st3.Sessions) != 0 || st3.Sessions == nil || st3.Retained == nil {
		t.Fatalf("%+v", st3)
	}
}

func TestUnknownRecordTypeFailsClosed(t *testing.T) {
	st := NewState()
	if err := st.Apply(200, []byte("{}")); err == nil {
		t.Fatal("unknown type accepted")
	}
	if err := st.Apply(recSessionUpsert, []byte("{not json")); err == nil {
		t.Fatal("bad payload accepted")
	}
}

func TestRelaxedPolicySyncsOnInterval(t *testing.T) {
	var syncs atomic.Int32
	dir := t.TempDir()
	s := openStore(t, dir, Options{SyncPolicy: Relaxed, SyncInterval: 10 * time.Millisecond, Sync: func(f *os.File) error { syncs.Add(1); return f.Sync() }})
	base := syncs.Load()
	_ = s.Retain(msg("default", "t", "v"))
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if syncs.Load() != base {
		t.Fatal("relaxed Sync fsynced synchronously")
	}
	deadline := time.Now().Add(2 * time.Second)
	for syncs.Load() == base && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if syncs.Load() == base {
		t.Fatal("background sync never ran")
	}
	_ = s.Close()
	if _, err := os.Stat(dir + "/state"); err != nil {
		t.Fatal(err)
	}
}

func TestCommitPolicySyncs(t *testing.T) {
	var syncs atomic.Int32
	s := openStore(t, t.TempDir(), Options{Sync: func(f *os.File) error { syncs.Add(1); return f.Sync() }})
	defer func() { _ = s.Close() }()
	_ = s.Retain(msg("default", "t", "v"))
	before := syncs.Load()
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if syncs.Load() != before+1 {
		t.Fatalf("commit Sync: %d -> %d", before, syncs.Load())
	}
	if s.LastIndex() != 1 {
		t.Fatalf("last index %d", s.LastIndex())
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Retain(msg("default", "t", "v")); err == nil {
		t.Fatal("append after close")
	}
}
