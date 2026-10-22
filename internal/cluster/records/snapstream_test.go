package records

import (
	"bytes"
	"encoding/json"
	"testing"
)

// TestCapturedRecordsAreUnchangedByLaterApplies: a capture is the
// collections at its index; puts, overwrites and deletes after it leave what
// it writes equal to the component as it was.
func TestCapturedRecordsAreUnchangedByLaterApplies(t *testing.T) {
	c := New(0)
	apply(t, c, 1, Op{Collection: "auth.users", Key: "a", Value: json.RawMessage(`{"v":1}`)})
	apply(t, c, 2, Op{Collection: "auth.users", Key: "b", Value: json.RawMessage(`{"v":2}`)})
	apply(t, c, 3, Op{Collection: "bans", Key: "x", Value: json.RawMessage(`{"r":"y"}`)})
	before, err := c.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	view, err := c.Capture()
	if err != nil {
		t.Fatal(err)
	}
	apply(t, c, 4, Op{Collection: "auth.users", Key: "a", Value: json.RawMessage(`{"v":9}`)})
	apply(t, c, 5, Op{Collection: "auth.users", Key: "c", Value: json.RawMessage(`{"v":3}`)})
	apply(t, c, 6, Op{Delete: true, Collection: "bans", Key: "x"})
	apply(t, c, 7, Op{Delete: true, Collection: "auth.users", Key: "b"})
	if after, _ := c.Snapshot(); bytes.Equal(after, before) {
		t.Fatal("the writes changed nothing, so the test shows nothing")
	}
	var recs [][]byte
	if err := view.WriteRecords(func(b []byte) error { recs = append(recs, append([]byte(nil), b...)); return nil }); err != nil {
		t.Fatal(err)
	}
	i := 0
	fresh := New(0)
	st, err := fresh.Stage(view.Encoding(), func() ([]byte, bool, error) {
		if i == len(recs) {
			return nil, false, nil
		}
		i++
		return recs[i-1], true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	st.Install()
	got, _ := fresh.Snapshot()
	if !bytes.Equal(got, before) {
		t.Fatalf("the captured view changed with later applies:\nwant %s\ngot  %s", before, got)
	}
}
