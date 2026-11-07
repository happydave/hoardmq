// Package testlog gives the nodes a test starts one debug-log file, kept and
// named when the test fails and removed when it passes. It is not used by the
// broker itself.
package testlog

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// T is the part of testing.TB the package uses, so a test of the package can
// stand in for a failing test.
type T interface {
	Name() string
	Helper()
	Cleanup(func())
	Failed() bool
	Logf(format string, args ...any)
}

var (
	mu    sync.Mutex
	files = map[T]io.Writer{}
)

// Writer is t's node-log file, created on the first call. It lives beside
// t.TempDir, which is removed even when the test fails.
func Writer(t T) io.Writer {
	t.Helper()
	mu.Lock()
	defer mu.Unlock()
	if w, ok := files[t]; ok {
		return w
	}
	w := open(t)
	files[t] = w
	return w
}

// Logger writes node's debug log to t's node-log file.
func Logger(t T, node string) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(Writer(t), &slog.HandlerOptions{Level: slog.LevelDebug})).With("node", node)
}

// open creates t's file and registers the cleanup that closes it, then keeps
// it if t failed and removes it otherwise. A file that cannot be created
// leaves the test passing, its nodes' logs discarded.
func open(t T) io.Writer {
	f, err := os.CreateTemp(os.Getenv("GOTMPDIR"), "hoardmq-nodes-"+fileName(t.Name())+"-*.log")
	forget := func() {
		mu.Lock()
		delete(files, t)
		mu.Unlock()
	}
	if err != nil {
		t.Logf("node logs discarded: %v", err)
		t.Cleanup(forget)
		return io.Discard
	}
	path := f.Name()
	if abs, err := filepath.Abs(path); err == nil {
		path = abs
	}
	t.Cleanup(func() {
		forget()
		_ = f.Close()
		if t.Failed() {
			t.Logf("node logs kept: %s", path)
			return
		}
		_ = os.Remove(path)
	})
	return f
}

// fileName is a test name made safe for a file name, at most 64 bytes.
func fileName(name string) string {
	s := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
			return r
		}
		return '_'
	}, name)
	if len(s) > 64 {
		s = s[:64]
	}
	return s
}
