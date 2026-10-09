package records

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
)

func u(v uint64) *uint64 { return &v }

var at = time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)

func apply(t *testing.T, c *Component, index uint64, op Op) Result {
	t.Helper()
	if op.At.IsZero() {
		op.At = at
	}
	payload, err := json.Marshal(op)
	if err != nil {
		t.Fatal(err)
	}
	v, err := c.Apply(index, payload)
	if err != nil {
		t.Fatal(err)
	}
	return v.(Result)
}

func put(key, value string, ifv *uint64) Op {
	return Op{Collection: "auth.users", Key: key, Value: json.RawMessage(value), If: ifv, By: "ops"}
}

func del(key string, ifv *uint64) Op {
	return Op{Delete: true, Collection: "auth.users", Key: key, If: ifv, By: "ops"}
}

func TestApplyDecisions(t *testing.T) {
	c := New(3)
	steps := []struct {
		name    string
		index   uint64
		op      Op
		ok      bool
		reason  string
		version uint64
	}{
		{"put new, if absent", 10, put("alice", `{"a":1}`, u(0)), true, "", 10},
		{"put if absent over an existing record", 11, put("alice", `{"a":2}`, u(0)), false, RefusedExists, 10},
		{"put at a stale version", 12, put("alice", `{"a":2}`, u(9)), false, RefusedVersion, 10},
		{"put at the current version", 13, put("alice", `{"a":3}`, u(10)), true, "", 13},
		{"put unconditionally", 14, put("alice", `{"a":4}`, nil), true, "", 14},
		{"delete a missing record", 15, del("bob", nil), false, RefusedMissing, 0},
		{"delete a missing record at a version", 16, del("bob", u(3)), false, RefusedMissing, 0},
		{"delete at a stale version", 17, del("alice", u(13)), false, RefusedVersion, 14},
		{"a key of 257 bytes", 18, put(strings.Repeat("k", 257), `{}`, nil), false, RefusedInvalid, 0},
		{"a key of 256 bytes", 19, put(strings.Repeat("k", 256), `{}`, nil), true, "", 19},
		{"an empty key", 20, put("", `{}`, nil), false, RefusedInvalid, 0},
		{"a value that is not an object", 22, put("carol", `[1]`, nil), false, RefusedInvalid, 0},
		{"a value of 8,193 bytes", 23, put("carol", `{"x":"`+strings.Repeat("a", 8193-8)+`"}`, nil), false, RefusedInvalid, 0},
		{"a value of 8,192 bytes", 24, put("carol", `{"x":"`+strings.Repeat("a", 8192-8)+`"}`, nil), true, "", 24},
		{"a record beyond the limit", 25, put("dave", `{}`, nil), false, RefusedFull, 0},
		{"a replace in a full collection", 26, put("carol", `{}`, nil), true, "", 26},
		{"delete at the current version", 27, del("alice", u(14)), true, "", 0},
		{"a bad collection name", 28, Op{Collection: "A.b", Key: "x", Value: json.RawMessage(`{}`)}, false, RefusedInvalid, 0},
	}
	for _, s := range steps {
		r := apply(t, c, s.index, s.op)
		if r.OK != s.ok || r.Reason != s.reason || r.Version != s.version || r.Index != s.index {
			t.Errorf("%s: %+v, want ok=%v reason=%q version=%d", s.name, r, s.ok, s.reason, s.version)
		}
	}
	if _, ok := c.Get("auth.users", "alice"); ok {
		t.Fatal("alice survived her delete")
	}
	rec, ok := c.Get("auth.users", "carol")
	if !ok || rec.Version != 26 || rec.Created != 24 || rec.By != "ops" || !rec.At.Equal(at) {
		t.Fatalf("carol %+v", rec)
	}
	if c.Applied() != 28 {
		t.Fatalf("applied %d", c.Applied())
	}
}

// A key that is not UTF-8 cannot survive the operation's JSON, so the rule is
// checked where it decides.
func TestDecideRefusesAKeyThatIsNotUTF8(t *testing.T) {
	if why := Decide(put("\xff", `{}`, nil), Record{}, false, 0, 10); why != RefusedInvalid {
		t.Fatalf("%q", why)
	}
	if why := Decide(put("\u00e9", `{}`, nil), Record{}, false, 0, 10); why != "" {
		t.Fatalf("a UTF-8 key: %q", why)
	}
}

func TestValidName(t *testing.T) {
	for name, want := range map[string]bool{
		"auth.users": true, "admin.apikeys": true, "a": true, "a1.b2": true,
		"": false, "A.b": false, "1a": false, "a.": false, ".a": false, "a..b": false, "a.1b": false, "a-b": false,
		strings.Repeat("a", 64): true, strings.Repeat("a", 65): false,
	} {
		if got := ValidName(name); got != want {
			t.Errorf("%q: %v", name, got)
		}
	}
}

// Two components fed the same log hold the same state.
func TestApplyIsDeterministic(t *testing.T) {
	a, b := New(50), New(50)
	rng := rand.New(rand.NewSource(7))
	for i := uint64(1); i <= 1000; i++ {
		key := fmt.Sprintf("k%d", rng.Intn(80))
		var op Op
		switch rng.Intn(4) {
		case 0:
			op = del(key, nil)
		case 1:
			op = put(key, fmt.Sprintf(`{"n":%d}`, i), u(uint64(rng.Intn(3))*i/2))
		default:
			op = put(key, fmt.Sprintf(`{"n":%d}`, i), nil)
		}
		op.At = at.Add(time.Duration(i) * time.Second)
		ra, rb := apply(t, a, i, op), apply(t, b, i, op)
		if ra.OK != rb.OK || ra.Reason != rb.Reason {
			t.Fatalf("step %d: %+v and %+v", i, ra, rb)
		}
	}
	sa, _ := a.Snapshot()
	sb, _ := b.Snapshot()
	if !bytes.Equal(sa, sb) {
		t.Fatal("two components fed the same log differ")
	}
	if n := a.Counts()["auth.users"]; n == 0 || n > 50 {
		t.Fatalf("%d records", n)
	}
}

func TestSnapshotRestore(t *testing.T) {
	a := New(0)
	apply(t, a, 1, put("alice", `{"a":1}`, nil))
	apply(t, a, 2, put("bob", `{"b":1}`, nil))
	snap, err := a.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	b := New(0)
	apply(t, b, 1, Op{Collection: "other.things", Key: "zed", Value: json.RawMessage(`{}`), At: at})
	if err := b.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if _, ok := b.Get("other.things", "zed"); ok {
		t.Fatal("a record absent from the snapshot survived the restore")
	}
	if r, ok := b.Get("auth.users", "bob"); !ok || r.Version != 2 || b.Applied() != 2 {
		t.Fatalf("bob %+v, applied %d", r, b.Applied())
	}
	items, more := b.List("auth.users", "", 1)
	if len(items) != 1 || items[0].Key != "alice" || !more {
		t.Fatalf("list %+v %v", items, more)
	}
	items, more = b.List("auth.users", "alice", 10)
	if len(items) != 1 || items[0].Key != "bob" || more {
		t.Fatalf("list after alice %+v %v", items, more)
	}
}

func TestWaitApplied(t *testing.T) {
	c := New(0)
	done := make(chan error, 1)
	go func() { done <- c.WaitApplied(context.Background(), 5) }()
	apply(t, c, 4, put("a", `{}`, nil))
	select {
	case err := <-done:
		t.Fatalf("returned before 5 applied: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	apply(t, c, 5, put("b", `{}`, nil))
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("not woken when 5 applied")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := c.WaitApplied(ctx, 9); err == nil {
		t.Fatal("a wait for an index never applied returned nil")
	}
}

// A write returns once this node has applied it, however long after the
// commit that is; a proposal that fails is ErrUnavailable.
func TestStoreWaitsForTheLocalApply(t *testing.T) {
	c := New(0)
	s := &Store{Component: c, Propose: func(_ context.Context, kind consensus.Kind, payload []byte) (json.RawMessage, error) {
		if kind != Kind {
			return nil, errors.New("wrong kind")
		}
		go func() {
			time.Sleep(100 * time.Millisecond)
			_, _ = c.Apply(7, payload)
		}()
		return json.Marshal(Result{OK: true, Index: 7, Version: 7})
	}}
	res, err := s.Put(context.Background(), "auth.users", "alice", json.RawMessage(`{"p":1}`), u(0), "ops", at)
	if err != nil || !res.OK || res.Index != 7 {
		t.Fatalf("put %+v %v", res, err)
	}
	if r, ok := c.Get("auth.users", "alice"); !ok || r.Version != 7 {
		t.Fatalf("the write returned before it applied here: %+v %v", r, ok)
	}
	s.Propose = func(context.Context, consensus.Kind, []byte) (json.RawMessage, error) {
		return nil, errors.New("no leader")
	}
	if _, err := s.Delete(context.Background(), "auth.users", "alice", nil, "ops", at); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("a failed proposal: %v", err)
	}
}
