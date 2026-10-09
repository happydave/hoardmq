package sinkkafka

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

// TestMeasureForwarderCost measures what the forwarder costs the default
// binary: its size, its idle resident set unconfigured and configured, and
// one client's QoS 1 publishing time with and without it. It runs only when
// SINKKAFKA_MEASURE_MAIN names a binary built from main to compare with, and
// the live Kafka is set (liveKafka); it refuses an unsettled host.
func TestMeasureForwarderCost(t *testing.T) {
	mainBin := os.Getenv("SINKKAFKA_MEASURE_MAIN")
	if mainBin == "" {
		t.Skip("SINKKAFKA_MEASURE_MAIN is not set: no measurement for this run")
	}
	brokers, _ := liveKafka(t)
	if load := loadAverage(t); load >= 2 {
		t.Fatalf("precondition: the host's load average is %.2f, want below 2", load)
	}
	bin := buildBroker(t)
	for _, b := range []struct{ name, path string }{{"main", mainBin}, {"branch", bin}} {
		st, err := os.Stat(b.path)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("binary size %s: %d bytes", b.name, st.Size())
	}
	topic := "t1317-measure-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	createTopics(t, brokers, topic)
	sink := map[string]any{"brokers": []string{brokers}, "routes": []any{map[string]any{"filter": "m/#", "topic": topic}}}

	arms := []struct {
		name string
		bin  string
		sink map[string]any
	}{
		{"main", mainBin, nil},
		{"branch-unconfigured", bin, nil},
		{"branch-configured", bin, sink},
	}
	for round := 1; round <= 3; round++ {
		for _, a := range arms {
			p := start(t, a.bin, a.sink)
			// The idle interval is the measurement's own: the resident set
			// is read once the process has settled.
			time.Sleep(30 * time.Second)
			t.Logf("idle rss round %d %s: %d kB", round, a.name, rssKB(t, p.pid))
			p.stop(t)
		}
	}
	for round := 1; round <= 3; round++ {
		for _, a := range arms[1:] {
			p := start(t, a.bin, a.sink)
			c := dial(t, p.addr, "measure")
			began := time.Now()
			for i := 0; i < 10_000; i++ {
				id := uint16(i%65535 + 1)
				if code := c.publish(&packet.Publish{Topic: "m/x", QoS: 1, PacketID: id, Payload: []byte("0123456789abcdef")}); code.IsError() {
					t.Fatalf("%s publish %d: %s", a.name, i, code)
				}
			}
			t.Logf("publish 10000 QoS 1 round %d %s: %v", round, a.name, time.Since(began).Round(time.Millisecond))
			p.stop(t)
		}
	}
}

func loadAverage(t *testing.T) float64 {
	t.Helper()
	b, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		t.Fatal(err)
	}
	f, err := strconv.ParseFloat(strings.Fields(string(b))[0], 64)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func rssKB(t *testing.T, pid int) int {
	t.Helper()
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/status")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(b), "\n") {
		if v, ok := strings.CutPrefix(line, "VmRSS:"); ok {
			n, err := strconv.Atoi(strings.Fields(v)[0])
			if err != nil {
				t.Fatal(err)
			}
			return n
		}
	}
	t.Fatalf("no VmRSS for %d", pid)
	return 0
}

type measured struct {
	pid    int
	addr   string
	exited chan struct{}
}

// start serves a minimal configuration: one loopback listener, anonymous
// clients, an allow-all ACL, and sink.kafka when sinkCfg is not nil.
func start(t *testing.T, bin string, sinkCfg map[string]any) *measured {
	t.Helper()
	dir := t.TempDir()
	acl := filepath.Join(dir, "acl.json")
	if err := os.WriteFile(acl, []byte(`{"rules":[{"topic":"#","allow":["publish","subscribe","unsubscribe"]}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	modules := map[string]any{"authz.file": map[string]any{"path": acl}}
	if sinkCfg != nil {
		modules["sink.kafka"] = sinkCfg
	}
	doc := map[string]any{
		"version":   1,
		"node":      map[string]any{"dataDir": filepath.Join(dir, "data")},
		"listeners": []any{map[string]any{"name": "t", "type": "tcp", "address": "127.0.0.1:0"}},
		"auth":      map[string]any{"anonymous": true},
		"modules":   modules,
		"log":       map[string]any{"format": "json"},
	}
	data, _ := json.Marshal(doc)
	path := filepath.Join(dir, "hoardmq.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, "serve", "-config", path)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdout = io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	m := &measured{pid: cmd.Process.Pid, exited: make(chan struct{})}
	addr := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(stderr)
		sc.Buffer(make([]byte, 64<<10), 1<<20)
		for sc.Scan() {
			var rec struct {
				Msg     string `json:"msg"`
				Address string `json:"address"`
			}
			if json.Unmarshal(sc.Bytes(), &rec) == nil && rec.Msg == "listening" && rec.Address != "" {
				select {
				case addr <- rec.Address:
				default:
				}
			}
		}
		_ = cmd.Wait()
		close(m.exited)
	}()
	t.Cleanup(func() {
		select {
		case <-m.exited:
		default:
			_ = syscall.Kill(m.pid, syscall.SIGKILL)
			<-m.exited
		}
	})
	select {
	case m.addr = <-addr:
	case <-m.exited:
		t.Fatalf("%s exited before listening", bin)
	case <-time.After(60 * time.Second):
		t.Fatalf("%s: no listening line", bin)
	}
	return m
}

func (m *measured) stop(t *testing.T) {
	t.Helper()
	if err := syscall.Kill(m.pid, syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case <-m.exited:
	case <-time.After(30 * time.Second):
		t.Fatalf("pid %d did not exit", m.pid)
	}
}
