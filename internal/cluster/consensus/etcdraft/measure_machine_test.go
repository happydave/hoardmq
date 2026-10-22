package etcdraft

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
	"github.com/happydave/hoardmq/internal/cluster/lease"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

// TestSnapshotMeasureMachine attributes the snapshot's memory by side: a
// machine holding N leases captures and writes its file (what the builder
// adds to a leader), then a machine holding N stages that file beside its
// own state (what a follower behind the log adds). One process, one side at
// a time.
func TestSnapshotMeasureMachine(t *testing.T) {
	n, _ := strconv.Atoi(os.Getenv("HOARDMQ_SNAPSHOT_MEASURE"))
	if n == 0 {
		t.Skip("set HOARDMQ_SNAPSHOT_MEASURE to the lease count")
	}
	willBytes, _ := strconv.Atoi(os.Getenv("HOARDMQ_SNAPSHOT_WILL"))
	fill := func() *consensus.Machine {
		m := consensus.NewMachine()
		tab := lease.NewTable()
		m.Register(lease.Kind, tab)
		now := time.Date(2026, 10, 22, 1, 0, 0, 0, time.UTC)
		for start, idx := 0, uint64(1); start < n; start, idx = start+1000, idx+1 {
			ops := make([]lease.Op, 0, 1000)
			for i := start; i < min(start+1000, n); i++ {
				op := lease.Op{Kind: lease.OpAcquire, ClientID: fmt.Sprintf("%036d", i), Node: "n1", ExpiryInterval: 3600, Now: now}
				if willBytes > 0 {
					op.Will = &store.WillRecord{Delay: 5, Message: &message.Message{Topic: "devices/" + op.ClientID + "/status", Payload: make([]byte, willBytes),
						Publisher: message.Publisher{ClientID: op.ClientID, Node: "n1"}, Seq: uint64(i + 1), Arrived: now}}
				}
				ops = append(ops, op)
			}
			if _, err := m.Apply(idx, consensus.Encode(lease.Kind, lease.Encode(ops))); err != nil {
				t.Fatal(err)
			}
		}
		return m
	}
	m := fill()
	heap := startHeapPeaks()
	defer heap.close()
	held := heapNow()
	heap.reset()
	began := time.Now()
	capture, err := m.Capture()
	if err != nil {
		t.Fatal(err)
	}
	captured := time.Since(began)
	dir := t.TempDir()
	c := &Core{snapDir: dir, stop: make(chan struct{})}
	began = time.Now()
	d, err := c.writeSnapshotFile(capture, consensus.FileHeader{Index: m.Applied(), Term: 1, Applied: m.Applied()}, "measure.snap")
	if err != nil {
		t.Fatal(err)
	}
	wrote := time.Since(began)
	buildPeak := heap.reset()

	other := fill()
	c.machine = other
	held2 := heapNow()
	heap.reset()
	began = time.Now()
	st, err := c.stageSnapshot(d)
	if err != nil {
		t.Fatal(err)
	}
	staged := time.Since(began)
	stagePeak := heap.reset()
	afterStage := heapNow()
	began = time.Now()
	if err := st.Install(); err != nil {
		t.Fatal(err)
	}
	installed := time.Since(began)
	fi, _ := os.Stat(filepath.Join(dir, "measure.snap"))
	t.Logf("MEASURE-MACHINE n=%d will=%d file=%dB capture=%v write=%v heldHeap=%d buildPeak=%d (+%d) stage=%v install=%v heldHeap2=%d stagePeak=%d (+%d) afterStage=%d",
		n, willBytes, fi.Size(), captured, wrote.Round(time.Millisecond), held, buildPeak, int64(buildPeak)-int64(held),
		staged.Round(time.Millisecond), installed, held2, stagePeak, int64(stagePeak)-int64(held2), afterStage)
}
