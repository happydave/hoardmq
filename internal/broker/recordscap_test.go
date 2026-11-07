package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/records"
	"github.com/happydave/hoardmq/internal/config"
)

// TestNodesWithDifferentRecordCapsAgree: n1 writes with a cap of 2 and n2
// with the default. Each node's puts are decided by its own cap, and every
// node ends with the same records.
func TestNodesWithDifferentRecordCapsAgree(t *testing.T) {
	nodes := startTrio(t, func(name string, d *config.Document) {
		d.Cluster.Readiness.StableMs = 0
		if name == "n1" {
			d.Cluster.Records.MaxPerCollection = 2
		}
	})
	waitMap(t, nodes, 3)
	if got := nodes["n1"].srv.records.Load().MaxRecords(); got != 2 {
		t.Fatalf("n1 writes with cap %d, want the configured 2", got)
	}
	if got := nodes["n2"].srv.records.Load().MaxRecords(); got != records.DefaultMaxRecords {
		t.Fatalf("n2 writes with cap %d, want the default", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	put := func(node, key string) records.Result {
		t.Helper()
		res, err := nodes[node].srv.records.Load().Put(ctx, "test.things", key, json.RawMessage(`{}`), nil, "test", time.Now())
		if err != nil {
			t.Fatalf("%s's put of %s: %v", node, key, err)
		}
		return res
	}
	var last uint64
	for i := 0; i < 2; i++ {
		if r := put("n1", fmt.Sprintf("a%d", i)); !r.OK {
			t.Fatalf("n1's put %d under its cap: %+v", i, r)
		}
	}
	if r := put("n1", "a2"); r.OK || r.Reason != records.RefusedFull {
		t.Fatalf("n1's third put past its cap of 2: %+v, want full", r)
	}
	r := put("n2", "b0")
	if !r.OK {
		t.Fatalf("n2's put under the default cap: %+v", r)
	}
	last = r.Index
	for name, f := range nodes {
		adminWait(t, name+" to apply n2's put", func() bool { return f.srv.records.Load().Applied() >= last })
	}
	want, _ := nodes["n1"].srv.records.Load().Snapshot()
	for name, f := range nodes {
		got, _ := f.srv.records.Load().Snapshot()
		if !bytes.Equal(got, want) {
			t.Fatalf("%s's records differ from n1's", name)
		}
		if n := f.srv.records.Load().Counts()["test.things"]; n != 3 {
			t.Fatalf("%s holds %d records, want 3", name, n)
		}
		if n := f.srv.records.Load().FullRefusals()["test.things"]; n != 1 {
			t.Fatalf("%s counted %d full refusals, want 1", name, n)
		}
	}
}
