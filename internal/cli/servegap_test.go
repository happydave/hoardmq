package cli

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/broker"
	"github.com/happydave/hoardmq/internal/store"
)

// TestServeRefusesAJournalWithABadCheckpointOverDroppedSegments: a node whose
// journal's checkpoint fails its checksum after the segments before it were
// dropped does not start. serve logs the refusal, naming the checkpoint file,
// and exits non-zero.
func TestServeRefusesAJournalWithABadCheckpointOverDroppedSegments(t *testing.T) {
	dir := t.TempDir()
	data := filepath.Join(dir, "data")
	logs := filepath.Join(data, "logs")
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := store.Open(logs, store.Options{Logger: quiet, SegmentBytes: 256})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b", "c", "d", "e", "f"} {
		if err := s.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 1, ExpiryInterval: 3600}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	st, err := s.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(func(func() uint64) *store.State { return st }); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(logs, "state", "00000000000000000001.seg")); !os.IsNotExist(err) {
		t.Fatalf("precondition: the checkpoint dropped no segment (stat: %v)", err)
	}
	ckpts, err := filepath.Glob(filepath.Join(logs, "state", "*.ckpt"))
	if err != nil || len(ckpts) != 1 {
		t.Fatalf("precondition: checkpoint files %v (%v), want 1", ckpts, err)
	}
	b, err := os.ReadFile(ckpts[0])
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)-1] ^= 0xFF
	if err := os.WriteFile(ckpts[0], b, 0o640); err != nil {
		t.Fatal(err)
	}

	doc := writeDoc(t, dir, "serve.json", `{"version":1,"node":{"dataDir":"`+data+`"},`+
		`"listeners":[{"name":"t","type":"tcp","address":"127.0.0.1:0"}],"log":{"format":"json"}}`)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	prevNotify, prevStarted := notifyContext, started
	notifyContext = func(context.Context) (context.Context, context.CancelFunc) { return ctx, func() {} }
	startedNode := make(chan struct{}, 1)
	started = func(*broker.Server) { startedNode <- struct{}{} }
	t.Cleanup(func() { notifyContext, started = prevNotify, prevStarted })

	type result struct {
		code   int
		errOut string
	}
	done := make(chan result, 1)
	go func() {
		code, _, errOut := run(t, nil, "serve", "-config", doc)
		done <- result{code, errOut}
	}()
	var r result
	select {
	case r = <-done:
	case <-startedNode:
		cancel()
		select {
		case r = <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("serve did not return within 10 s of the stop signal")
		}
		t.Fatalf("serve started over a bad checkpoint whose segments are gone (exit %d)", r.code)
	case <-time.After(30 * time.Second):
		t.Fatal("serve neither started nor returned within 30 s")
	}
	name := filepath.Base(ckpts[0])
	if r.code == ExitOK {
		t.Fatalf("serve exited %d over a bad checkpoint whose segments are gone", r.code)
	}
	var logged bool
	for _, line := range strings.Split(r.errOut, "\n") {
		if strings.HasPrefix(line, "{") && strings.Contains(line, `"level":"ERROR"`) && strings.Contains(line, name) {
			logged = true
		}
	}
	if !logged {
		t.Errorf("no Error log line names the checkpoint %s:\n%s", name, r.errOut)
	}
	if !strings.Contains(r.errOut, "hoardmq serve: ") || !strings.Contains(r.errOut, name) {
		t.Errorf("serve's own error line does not name the checkpoint %s:\n%s", name, r.errOut)
	}
}
