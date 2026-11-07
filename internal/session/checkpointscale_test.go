package session

import (
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"runtime/metrics"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

// TestCheckpointScale measures a checkpoint and a restart at each session
// count HOARDMQ_CHECKPOINT_SCALE lists: n offline persistent sessions, each
// with a subscription and a queued QoS 1 message. It reports the checkpoint's
// time, the heap it adds at its peak (sampled every millisecond), the file it
// writes, and a restart's time to read the checkpoint back and install it in
// a new manager, with the heap the state then holds. It only measures.
func TestCheckpointScale(t *testing.T) {
	spec := os.Getenv("HOARDMQ_CHECKPOINT_SCALE")
	if spec == "" {
		t.Skip("HOARDMQ_CHECKPOINT_SCALE is not set")
	}
	for _, f := range strings.Split(spec, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(f))
		if err != nil || n < 1 {
			t.Fatalf("HOARDMQ_CHECKPOINT_SCALE %q: session counts of at least 1", spec)
		}
		checkpointScale(t, n)
	}
}

var heapSample = []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}

func heapNow() uint64 {
	metrics.Read(heapSample)
	return heapSample[0].Value.Uint64()
}

// settle collects twice, so a buffer a sync.Pool kept (encoding/json's
// encoder among them) is freed before a baseline is read: a pool's contents
// survive one collection as its victim.
func settle() {
	runtime.GC()
	runtime.GC()
}

// peakDuring runs fn while sampling the live heap every millisecond and
// returns the highest sample.
func peakDuring(fn func()) uint64 {
	var peak atomic.Uint64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for {
			if h := heapNow(); h > peak.Load() {
				peak.Store(h)
			}
			select {
			case <-stop:
				return
			case <-tick.C:
			}
		}
	}()
	fn()
	close(stop)
	wg.Wait()
	if h := heapNow(); h > peak.Load() {
		peak.Store(h)
	}
	return peak.Load()
}

func checkpointScale(t *testing.T, n int) {
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	opts := Options{MaxQueued: 100, Authorizers: auth.AuthorizerChain{allow{}}, Logger: quiet, Clock: newFakeClock()}
	m := New(opts)
	pub := newFake("pub", packet.V5, 100)
	connectV5(m, pub, true, 0, nil)
	began := time.Now()
	for i := 0; i < n; i++ {
		id := "client-" + strconv.Itoa(i)
		c := newFake(id, packet.V5, 100)
		connectV5(m, c, true, 3600, nil)
		subscribe(m, c, 1, "devices/"+id+"/state")
		m.Disconnect(c, false, packet.NormalDisconnection, false)
		publish(m, pub, "devices/"+id+"/state", 1, uint16(i%65535+1), "a payload of some tens of bytes")
	}
	if got := len(m.Records()); got != n+1 {
		t.Fatalf("n=%d: %d sessions after the fill", n, got)
	}
	fill := time.Since(began)

	dir := t.TempDir()
	st, err := store.Open(dir, store.Options{Logger: quiet, CheckpointEvery: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	// The decomposition, when asked for: the copy alone, then its JSON on
	// top, both dropped before the checkpoint is measured.
	if os.Getenv("HOARDMQ_CHECKPOINT_DECOMPOSE") != "" {
		decompose(t, m, n)
	}
	settle()
	base := heapNow()
	var took time.Duration
	peak := peakDuring(func() {
		s := time.Now()
		if err := st.Checkpoint(m.CheckpointState); err != nil {
			t.Fatal(err)
		}
		took = time.Since(s)
	})
	// A second checkpoint in the same store, as a running node takes them.
	settle()
	base2 := heapNow()
	var took2 time.Duration
	peak2 := peakDuring(func() {
		s := time.Now()
		if err := st.Checkpoint(m.CheckpointState); err != nil {
			t.Fatal(err)
		}
		took2 = time.Since(s)
	})
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	var size int64
	_ = filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	t.Logf("checkpoint-scale n=%d: fill %s; checkpoint %s, heap +%d MiB at its peak over %d MiB live; files %d MiB",
		n, fill.Round(time.Millisecond), took.Round(time.Millisecond), (peak-base)>>20, base>>20, size>>20)
	t.Logf("checkpoint-scale n=%d: second checkpoint %s, heap +%d MiB at its peak over %d MiB live",
		n, took2.Round(time.Millisecond), (peak2-base2)>>20, base2>>20)
	// The restart, the old manager no longer referenced.
	restart(t, opts, dir, n)
}

// decompose reports the checkpoint's parts: the copy alone, then its JSON on
// top, both dropped after.
func decompose(t *testing.T, m *Manager, n int) {
	settle()
	start := heapNow()
	var copied *store.State
	copyPeak := peakDuring(func() { copied = m.CheckpointState(func() uint64 { return 0 }) })
	var payload []byte
	marshalPeak := peakDuring(func() {
		var err error
		if payload, err = json.Marshal(copied); err != nil {
			t.Fatal(err)
		}
	})
	t.Logf("checkpoint-scale n=%d decomposed: copy +%d MiB, marshal +%d MiB more (payload %d MiB)",
		n, (copyPeak-start)>>20, (marshalPeak-max(copyPeak, start))>>20, len(payload)>>20)
	copied, payload = nil, nil
}

// restart drops the old manager, reads the log back and installs its state
// in a new manager, reporting the time and the heap the state then holds.
func restart(t *testing.T, opts Options, dir string, n int) {
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	settle()
	before := heapNow()
	var open, recover, load time.Duration
	var m2 *Manager
	restartPeak := peakDuring(func() {
		s := time.Now()
		st2, err := store.Open(dir, store.Options{Logger: quiet, CheckpointEvery: 1 << 30})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = st2.Close() }()
		open = time.Since(s)
		s = time.Now()
		state, err := st2.Recover()
		if err != nil {
			t.Fatal(err)
		}
		recover = time.Since(s)
		// The publisher, connected with no session expiry, is not persistent.
		if len(state.Sessions) != n {
			t.Errorf("n=%d: %d sessions recovered", n, len(state.Sessions))
		}
		s = time.Now()
		m2 = New(opts)
		m2.Load(state)
		load = time.Since(s)
	})
	settle()
	after := heapNow()
	if got := len(m2.Records()); got != n {
		t.Fatalf("n=%d: %d sessions after the restart", n, got)
	}
	t.Logf("checkpoint-scale n=%d: restart open %s, read and decode %s, install %s (total %s); heap +%d MiB at its peak, %d MiB held after",
		n, open.Round(time.Millisecond), recover.Round(time.Millisecond), load.Round(time.Millisecond), (open + recover + load).Round(time.Millisecond),
		(restartPeak-before)>>20, (after-before)>>20)
}
