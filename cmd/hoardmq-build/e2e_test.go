package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/happydave/hoardmq/ext"
	"github.com/happydave/hoardmq/internal/packet"
)

// copyFixture copies testdata/hello to a temporary directory, its module file
// renamed to go.mod, and returns the directory.
func copyFixture(t *testing.T) string {
	t.Helper()
	dst := filepath.Join(t.TempDir(), "hello")
	if err := os.MkdirAll(dst, 0o755); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Join("testdata", "hello"))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		data, err := os.ReadFile(filepath.Join("testdata", "hello", e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		name := e.Name()
		if name == "go.mod.txt" {
			name = "go.mod"
		}
		if err := os.WriteFile(filepath.Join(dst, name), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dst
}

// buildHello builds a binary carrying hook.hello from the checkout under
// test, with the network off.
func buildHello(t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("go"); err != nil {
		t.Fatalf("the go command is not on PATH: %v", err)
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("GOPROXY", "off")
	t.Setenv("GOSUMDB", "off")
	bin := filepath.Join(t.TempDir(), "hoardmq")
	var out, errOut bytes.Buffer
	start := time.Now()
	code := run([]string{"--hoardmq-dir", root, "--with", "example.com/hoardmq-hello=" + copyFixture(t), "--output", bin}, &out, &errOut)
	if code != exitOK {
		t.Fatalf("hoardmq-build exit %d:\n%s", code, errOut.String())
	}
	t.Logf("built the binary in %s", time.Since(start).Round(time.Millisecond))
	return bin
}

// freePort is a loopback port nothing held a moment ago.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}

func writeJSON(t *testing.T, path string, v any) {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

// lineWatcher keeps a process's log lines and finds the MQTT listener's
// address in them.
type lineWatcher struct {
	mu    sync.Mutex
	lines []string
	addr  chan string
}

func (w *lineWatcher) read(r io.Reader) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		w.mu.Lock()
		w.lines = append(w.lines, line)
		w.mu.Unlock()
		var rec struct {
			Msg     string `json:"msg"`
			Address string `json:"address"`
		}
		if json.Unmarshal([]byte(line), &rec) == nil && rec.Msg == "listening" && rec.Address != "" {
			select {
			case w.addr <- rec.Address:
			default:
			}
		}
	}
}

func (w *lineWatcher) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return strings.Join(w.lines, "\n")
}

// client is a minimal MQTT 5 client over the broker's own codec.
type client struct {
	conn net.Conn
	enc  packet.Encoder
	dec  *packet.Decoder
}

// connect dials until the node accepts the connection: a single node
// refuses clients until its consensus core has elected itself.
func connect(t *testing.T, addr, id string) *client {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		c, err := tryConnect(addr, id)
		if err == nil {
			return c
		}
		if time.Now().After(deadline) {
			t.Fatalf("connect %s: %v", id, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func tryConnect(addr, id string) (*client, error) {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	c := &client{conn: conn, enc: packet.Encoder{Version: packet.V5}, dec: packet.NewDecoder(conn)}
	c.dec.Version = packet.V5
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if err := c.enc.Encode(conn, &packet.Connect{ProtocolName: "MQTT", ProtocolLevel: packet.V5, CleanStart: true, KeepAlive: 30, ClientID: id}); err != nil {
		_ = conn.Close()
		return nil, err
	}
	p, err := c.dec.Read()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	ack, ok := p.(*packet.Connack)
	if !ok || ack.ReasonCode != packet.Success {
		_ = conn.Close()
		return nil, fmt.Errorf("answered %#v", p)
	}
	_ = conn.SetDeadline(time.Time{})
	return c, nil
}

// publish sends a QoS 1 PUBLISH and requires its PUBACK to succeed.
func (c *client) publish(t *testing.T, topic string, id uint16) {
	t.Helper()
	_ = c.conn.SetDeadline(time.Now().Add(10 * time.Second))
	defer func() { _ = c.conn.SetDeadline(time.Time{}) }()
	if err := c.enc.Encode(c.conn, &packet.Publish{QoS: 1, Topic: topic, PacketID: id, Payload: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	p, err := c.dec.Read()
	if err != nil {
		t.Fatal(err)
	}
	if ack, ok := p.(*packet.Puback); !ok || ack.PacketID != id || ack.ReasonCode.IsError() {
		t.Fatalf("publish %s answered %#v", topic, p)
	}
}

// toggle enables or disables a module through the admin API.
func toggle(t *testing.T, adminAddr, token, id string, enable bool) {
	t.Helper()
	verb := "disable"
	if enable {
		verb = "enable"
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "http://"+adminAddr+"/api/v1/modules/"+id+"/"+verb, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s %s: %s %s", verb, id, resp.Status, body)
	}
}

// TestExternalModuleLifecycle builds a binary with the out-of-tree example
// module through the tool, offline, and shows each step of its lifecycle:
// provision and validate at check (refusing a configuration Validate
// rejects, and writing nothing), then provision, validate and start at
// serve, its hooks called, a hot disable that stops them and an enable that
// brings them back, and stop at shutdown.
func TestExternalModuleLifecycle(t *testing.T) {
	bin := buildHello(t)
	dir := t.TempDir()

	out, err := exec.Command(bin, "version").CombinedOutput()
	if err != nil || !strings.Contains(string(out), "extension API "+ext.APIVersion) || !strings.Contains(string(out), " hook.hello") {
		t.Fatalf("version: %v\n%s", err, out)
	}

	logFile := filepath.Join(dir, "hello.log")
	token := "hello-test-ops-token-0123456789ab"
	tokenFile := filepath.Join(dir, "ops.token")
	if err := os.WriteFile(tokenFile, []byte(token), 0o600); err != nil {
		t.Fatal(err)
	}
	acl := filepath.Join(dir, "acl.json")
	if err := os.WriteFile(acl, []byte(`{"rules":[{"topic":"#","allow":["publish","subscribe","unsubscribe"]}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	adminAddr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	doc := func(helloLog string) map[string]any {
		return map[string]any{
			"version":   1,
			"node":      map[string]any{"dataDir": filepath.Join(dir, "data")},
			"listeners": []any{map[string]any{"name": "t", "type": "tcp", "address": "127.0.0.1:0"}},
			"auth":      map[string]any{"anonymous": true},
			"modules":   map[string]any{"hook.hello": map[string]any{"log": helloLog}, "authz.file": map[string]any{"path": acl}},
			"admin": map[string]any{"address": adminAddr,
				"tokens": []any{map[string]any{"name": "ops", "tokenFile": tokenFile, "scopes": []string{"admin"}}}},
			"log": map[string]any{"format": "json"},
		}
	}
	good, bad := filepath.Join(dir, "good.json"), filepath.Join(dir, "bad.json")
	writeJSON(t, good, doc(logFile))
	writeJSON(t, bad, doc(""))

	if out, err := exec.Command(bin, "check", "-config", good).CombinedOutput(); err != nil {
		t.Fatalf("check of the good configuration: %v\n%s", err, out)
	}
	out, err = exec.Command(bin, "check", "-config", bad).CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 1 || !strings.Contains(string(out), "/modules/hook.hello") || !strings.Contains(string(out), "log is required") {
		t.Fatalf("check of the bad configuration: %v\n%s", err, out)
	}
	if _, err := os.Stat(logFile); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("check wrote the module's log: %v", err)
	}

	cmd := exec.Command(bin, "serve", "-config", good)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdout = io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	pid := cmd.Process.Pid
	w := &lineWatcher{addr: make(chan string, 1)}
	readDone := make(chan struct{})
	go func() { w.read(stderr); close(readDone) }()
	// exited closes once the process has exited and been waited for, and
	// waitErr is then its result.
	exited := make(chan struct{})
	var waitErr error
	go func() { <-readDone; waitErr = cmd.Wait(); close(exited) }()
	t.Cleanup(func() {
		select {
		case <-exited:
		default:
			_ = syscall.Kill(pid, syscall.SIGKILL)
			<-exited
		}
	})

	var addr string
	select {
	case addr = <-w.addr:
	case <-exited:
		t.Fatalf("serve exited: %v\n%s", waitErr, w)
	case <-time.After(60 * time.Second):
		t.Fatalf("no listening line:\n%s", w)
	}
	c := connect(t, addr, "c1")
	defer func() { _ = c.conn.Close() }()
	c.publish(t, "t/1", 1)
	toggle(t, adminAddr, token, "hook.hello", false)
	c.publish(t, "t/2", 2)
	toggle(t, adminAddr, token, "hook.hello", true)
	c.publish(t, "t/3", 3)

	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case <-exited:
		if waitErr != nil {
			t.Fatalf("serve exited with %v:\n%s", waitErr, w)
		}
	case <-time.After(60 * time.Second):
		t.Fatalf("serve did not stop:\n%s", w)
	}

	data, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatal(err)
	}
	want := "provision\nvalidate\nstart\nconnected c1\npublish t/1\nenabled false\nenabled true\npublish t/3\nstop\n"
	if string(data) != want {
		t.Fatalf("the module's log:\n%s\nwant:\n%s", data, want)
	}
}
