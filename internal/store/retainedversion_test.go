package store

import (
	"io"
	"log/slog"
	"testing"

	"github.com/happydave/hoardmq/internal/message"
)

// TestVersionedRetainedRecordsRecover: a stamped message, a clear's tombstone
// and a copy drop written to the disk store come back from its recovery as
// they were held: the message at its version, the tombstone over an older
// message, nothing for the dropped topic, and an older stamped message
// written after a newer one not installed.
func TestVersionedRetainedRecordsRecover(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir, Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	msg := func(tp, p string) *message.Message {
		return &message.Message{Namespace: "default", Topic: tp, Payload: []byte(p), Retain: true}
	}
	for _, step := range []func() error{
		func() error { return st.Retain(msg("m", "new"), Version{E: 2, N: 5}) },
		func() error { return st.Retain(msg("m", "old"), Version{E: 2, N: 4}) },
		func() error { return st.Retain(msg("c", "before"), Version{E: 1, N: 9}) },
		func() error { return st.RetainClear("default", "c", Version{E: 2, N: 6}) },
		func() error { return st.Retain(msg("c", "older"), Version{E: 2, N: 1}) },
		func() error { return st.Retain(msg("d", "x"), Version{E: 2, N: 7}) },
		func() error { return st.RetainDrop("default", "d") },
		func() error { return st.Sync() },
	} {
		if err := step(); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = Open(dir, Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	rec, err := st.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if v, held, tomb := rec.RetainedHeld("default", "m"); !held || tomb || v != (Version{E: 2, N: 5}) {
		t.Fatalf("m recovered at %v (held %v, tombstone %v), want the newer message at 2.5", v, held, tomb)
	}
	for _, r := range rec.Retained {
		if r.Topic == "m" && string(r.Payload) != "new" {
			t.Fatalf("m recovered %q, want new", r.Payload)
		}
	}
	if v, held, tomb := rec.RetainedHeld("default", "c"); !held || !tomb || v != (Version{E: 2, N: 6}) {
		t.Fatalf("c recovered at %v (held %v, tombstone %v), want the clear's tombstone at 2.6", v, held, tomb)
	}
	if _, held, _ := rec.RetainedHeld("default", "d"); held {
		t.Fatal("the copy drop did not survive the restart")
	}
}
