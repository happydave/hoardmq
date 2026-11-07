package cli

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/datadir"
	"github.com/happydave/hoardmq/internal/datadir/datadirtest"
	"github.com/happydave/hoardmq/internal/store"
	"github.com/happydave/hoardmq/internal/walcheck"
)

func TestMain(m *testing.M) {
	datadirtest.MaybeHold()
	os.Exit(m.Run())
}

// metric reads one series' line from the node's /metrics.
func metric(t *testing.T, n *servedNode, name string) string {
	t.Helper()
	addr := n.srv.ObservabilityAddr()
	if addr == nil {
		t.Fatal("no observability listener")
	}
	resp, err := http.Get("http://" + addr.String() + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, name+" ") || strings.HasPrefix(line, name+"{") {
			return line
		}
	}
	return ""
}

// A data directory another process holds refuses serve before it opens
// anything, naming the lock file and the holder's PID.
func TestServeRefusesADataDirectoryInUse(t *testing.T) {
	dir := t.TempDir()
	data := filepath.Join(dir, "data")
	pid, _ := datadirtest.Hold(t, data)
	doc := writeDoc(t, dir, "c.json", `{"version":1,"node":{"dataDir":"`+data+`"},"listeners":[{"name":"t","type":"tcp","address":"127.0.0.1:0"}]}`)
	// A serve that started would stop at once and exit 0.
	prev := notifyContext
	notifyContext = func(parent context.Context) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(parent)
		cancel()
		return ctx, cancel
	}
	t.Cleanup(func() { notifyContext = prev })
	code, _, errOut := run(t, nil, "serve", "-config", doc)
	want := "data directory " + data + " is in use: " + filepath.Join(data, datadir.LockName) + " is locked by another process (PID " + strconv.Itoa(pid) + ")"
	if code != ExitProblems || !strings.Contains(errOut, want) {
		t.Fatalf("exit %d, stderr %q; want 1 and %q", code, errOut, want)
	}
	entries, err := os.ReadDir(data)
	if err != nil || len(entries) != 1 || entries[0].Name() != datadir.LockName {
		t.Fatalf("the data directory after the refusal: %v %v; want the lock file alone", entries, err)
	}
}

// serve holds the lock and says so on /metrics; two serves in turn on one
// directory both start.
func TestServeHoldsTheDataDirectory(t *testing.T) {
	data := filepath.Join(t.TempDir(), "data")
	for i := 0; i < 2; i++ {
		n := startServeIn(t, data, "")
		if got := metric(t, n, "hoardmq_data_dir_locked"); got != "hoardmq_data_dir_locked 1" {
			t.Fatalf("run %d: %q, want the lock held", i, got)
		}
		if l := datadir.Shared(data); l.Outcome != datadir.HeldElsewhere {
			t.Fatalf("run %d: a shared attempt beside the node: %v", i, l.Outcome)
		}
		n.stop()
		if r := n.wait(t, 15*time.Second); r.code != ExitOK {
			t.Fatalf("run %d: exit %d %q", i, r.code, r.errOut)
		}
	}
}

// A filesystem that cannot lock does not stop serve: it warns, and the gauge
// reads the lock unheld.
func TestServeOnAFilesystemThatCannotLock(t *testing.T) {
	prev := lockDataDir
	lockDataDir = func(dir string) *datadir.Lock {
		return &datadir.Lock{Outcome: datadir.Unsupported, Path: filepath.Join(dir, datadir.LockName), Err: syscall.ENOLCK}
	}
	t.Cleanup(func() { lockDataDir = prev })
	n := startServe(t, "")
	if got := metric(t, n, "hoardmq_data_dir_locked"); got != "hoardmq_data_dir_locked 0" {
		t.Fatalf("%q, want the lock unheld", got)
	}
	n.stop()
	r := n.wait(t, 15*time.Second)
	if r.code != ExitOK || !strings.Contains(r.errOut, "cannot be locked") || !strings.Contains(r.errOut, "no locks available") {
		t.Fatalf("exit %d, log %q; want a start and the warning naming the error", r.code, r.errOut)
	}
}

// walNode writes a document and a data directory with a state log of 12
// records over several segments; no replica or raft log.
func walNode(t *testing.T) (doc, data string) {
	t.Helper()
	dir := t.TempDir()
	data = filepath.Join(dir, "data")
	st, err := store.Open(filepath.Join(data, "logs"), store.Options{SegmentBytes: 256, Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 12; i++ {
		if err := st.SessionUpsert(store.SessionMeta{Namespace: "ns", ClientID: "c" + strconv.Itoa(i), Epoch: 1}); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	doc = writeDoc(t, dir, "c.json", `{"version":1,"node":{"dataDir":"`+data+`"},"listeners":[{"name":"t","type":"tcp","address":"127.0.0.1:0"}]}`)
	return doc, data
}

func lastStateSegment(t *testing.T, data string) string {
	t.Helper()
	m, _ := filepath.Glob(filepath.Join(data, "logs", "state", "*.seg"))
	sort.Strings(m)
	if len(m) == 0 {
		t.Fatal("no segments")
	}
	return m[len(m)-1]
}

func tearTail(t *testing.T, data string) {
	t.Helper()
	last := lastStateSegment(t, data)
	info, _ := os.Stat(last)
	if err := os.Truncate(last, info.Size()-4); err != nil {
		t.Fatal(err)
	}
}

func damage(t *testing.T, data string) {
	t.Helper()
	m, _ := filepath.Glob(filepath.Join(data, "logs", "state", "*.seg"))
	sort.Strings(m)
	f, err := os.OpenFile(m[0], os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{'X'}, 32+9+1); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
}

func TestWalVerify(t *testing.T) {
	doc, data := walNode(t)
	code, out, errOut := run(t, nil, "wal", "verify", "-config", doc)
	if code != ExitOK || !strings.Contains(out, "log state ("+filepath.Join(data, "logs", "state")+")\n  segments ") ||
		!strings.Contains(out, "  clean\n") || !strings.Contains(out, "log replica ("+filepath.Join(data, "replica", "state")+"): absent") ||
		!strings.HasSuffix(out, "verdict: clean: a start would proceed\n") || errOut != "" {
		t.Fatalf("clean: exit %d\n%s%s", code, out, errOut)
	}
	if _, err := os.Stat(filepath.Join(data, datadir.LockName)); !os.IsNotExist(err) {
		t.Fatalf("verify created the lock file: %v", err)
	}

	tearTail(t, data)
	code, out, _ = run(t, nil, "wal", "verify", "-config", doc)
	if code != ExitOK || !strings.Contains(out, "  start repairs: torn-tail: record 12 cut short") ||
		!strings.HasSuffix(out, "verdict: a start would repair 1 and proceed\n") {
		t.Fatalf("torn tail: exit %d\n%s", code, out)
	}
	code, out, _ = run(t, nil, "wal", "verify", "-config", doc, "-json")
	var r walcheck.Report
	if err := json.Unmarshal([]byte(out), &r); err != nil {
		t.Fatalf("-json: %v\n%s", err, out)
	}
	f := r.Logs[0].Findings
	if code != ExitOK || r.Verdict != walcheck.VerdictStartRepairs || r.DataDir != data || r.Lock.State != "absent" ||
		r.Logs[1].Present || len(f) != 1 || f[0].Kind != walcheck.FindTornTail || f[0].Start != walcheck.StartRepairs ||
		f[0].Repair != walcheck.RepairTruncateTail || f[0].Index != 12 || f[0].Bytes == 0 {
		t.Fatalf("-json: exit %d %+v", code, r)
	}

	damage(t, data)
	code, out, _ = run(t, nil, "wal", "verify", "-config", doc)
	if code != ExitProblems || !strings.Contains(out, "  start refuses: damaged-record: wal state: checksum mismatch") ||
		!strings.Contains(out, "    recovery: not repaired by this command (held for the owner)") {
		t.Fatalf("damage: exit %d\n%s", code, out)
	}
}

func TestWalVerifyRefusesADirectoryInUse(t *testing.T) {
	doc, data := walNode(t)
	pid, _ := datadirtest.Hold(t, data)
	code, out, errOut := run(t, nil, "wal", "verify", "-config", doc)
	if code != ExitProblems || out != "" || !strings.Contains(errOut, "(PID "+strconv.Itoa(pid)+"); stop the node first") {
		t.Fatalf("exit %d out %q err %q", code, out, errOut)
	}
}

func TestWalRepair(t *testing.T) {
	doc, data := walNode(t)
	tearTail(t, data)
	last := lastStateSegment(t, data)
	torn, _ := os.ReadFile(last)
	code, out, _ := run(t, nil, "wal", "repair", "-config", doc)
	if after, _ := os.ReadFile(last); code != ExitOK || !strings.Contains(out, "repair (dry run; -apply makes it):\n  log state: truncate ") || string(after) != string(torn) {
		t.Fatalf("dry run: exit %d\n%s", code, out)
	}
	code, out, _ = run(t, nil, "wal", "repair", "-config", doc, "-apply")
	if code != ExitOK || !strings.Contains(out, "kept what it dropped in "+filepath.Join(data, "wal-repair")) ||
		!strings.HasSuffix(out, "after the repair: clean: a start would proceed\n") {
		t.Fatalf("-apply: exit %d\n%s", code, out)
	}
	if code, out, _ := run(t, nil, "wal", "verify", "-config", doc); code != ExitOK || !strings.Contains(out, "verdict: clean") {
		t.Fatalf("verify after the repair: exit %d\n%s", code, out)
	}

	damage(t, data)
	code, out, _ = run(t, nil, "wal", "repair", "-config", doc, "-apply")
	if code != ExitProblems || !strings.Contains(out, "repair refused: log state: a start refuses on it") {
		t.Fatalf("damage: exit %d\n%s", code, out)
	}
}

func TestWalUsage(t *testing.T) {
	for _, args := range [][]string{{"wal"}, {"wal", "frob"}, {"wal", "verify", "-apply"}, {"wal", "repair", "extra"}} {
		if code, _, _ := run(t, nil, args...); code != ExitUsage {
			t.Errorf("%v: exit %d, want %d", args, code, ExitUsage)
		}
	}
}

// On a filesystem that cannot lock, verify and a dry run run and say so;
// -apply refuses.
func TestWalOnAFilesystemThatCannotLock(t *testing.T) {
	unsupported := func(dir string) *datadir.Lock {
		return &datadir.Lock{Outcome: datadir.Unsupported, Path: filepath.Join(dir, datadir.LockName), Err: syscall.EOPNOTSUPP}
	}
	prevX, prevS := lockDataDir, lockShared
	lockDataDir, lockShared = unsupported, unsupported
	t.Cleanup(func() { lockDataDir, lockShared = prevX, prevS })
	doc, data := walNode(t)
	tearTail(t, data)
	code, out, _ := run(t, nil, "wal", "verify", "-config", doc)
	if code != ExitOK || !strings.Contains(out, "could not lock "+filepath.Join(data, datadir.LockName)+": this filesystem does not support it") {
		t.Fatalf("verify: exit %d\n%s", code, out)
	}
	if code, out, _ = run(t, nil, "wal", "repair", "-config", doc); code != ExitOK || !strings.Contains(out, "dry run") {
		t.Fatalf("dry run: exit %d\n%s", code, out)
	}
	last := lastStateSegment(t, data)
	before, _ := os.ReadFile(last)
	code, out, _ = run(t, nil, "wal", "repair", "-config", doc, "-apply")
	if after, _ := os.ReadFile(last); code != ExitProblems || !strings.Contains(out, "repair refused: the data directory's lock is not held by this command (unsupported)") || string(after) != string(before) {
		t.Fatalf("-apply: exit %d\n%s", code, out)
	}
}

// The state log is found where store.wal's dir puts it, outside the data
// directory.
func TestWalVerifyFollowsTheStoreDirectory(t *testing.T) {
	dir := t.TempDir()
	logs := filepath.Join(dir, "elsewhere")
	st, err := store.Open(logs, store.Options{Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SessionUpsert(store.SessionMeta{Namespace: "ns", ClientID: "c", Epoch: 1}); err != nil {
		t.Fatal(err)
	}
	_ = st.Close()
	data := filepath.Join(dir, "data")
	doc := writeDoc(t, dir, "c.json", `{"version":1,"node":{"dataDir":"`+data+`"},"listeners":[{"name":"t","type":"tcp","address":"127.0.0.1:0"}],"modules":{"store.wal":{"dir":"`+logs+`"}}}`)
	code, out, _ := run(t, nil, "wal", "verify", "-config", doc)
	if code != ExitOK || !strings.Contains(out, "log state ("+filepath.Join(logs, "state")+")\n  segments 1, indexes 1 to 1") {
		t.Fatalf("exit %d\n%s", code, out)
	}
}

// A serve started while verify holds a shared lock refuses, naming the PID the
// file last recorded.
func TestServeRefusesWhileVerifyHoldsTheDirectory(t *testing.T) {
	dir := t.TempDir()
	data := filepath.Join(dir, "data")
	first := datadir.Exclusive(data)
	if first.Outcome != datadir.Held {
		t.Fatalf("precondition: %v", first.Outcome)
	}
	_ = first.Release()
	shared := datadir.Shared(data)
	defer func() { _ = shared.Release() }()
	if shared.Outcome != datadir.Held {
		t.Fatalf("precondition: shared %v", shared.Outcome)
	}
	prev := notifyContext
	notifyContext = func(parent context.Context) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(parent)
		cancel()
		return ctx, cancel
	}
	t.Cleanup(func() { notifyContext = prev })
	doc := writeDoc(t, dir, "c.json", `{"version":1,"node":{"dataDir":"`+data+`"},"listeners":[{"name":"t","type":"tcp","address":"127.0.0.1:0"}]}`)
	code, _, errOut := run(t, nil, "serve", "-config", doc)
	if code != ExitProblems || !strings.Contains(errOut, "is locked by another process (PID "+strconv.Itoa(os.Getpid())+")") {
		t.Fatalf("exit %d, stderr %q", code, errOut)
	}
}
