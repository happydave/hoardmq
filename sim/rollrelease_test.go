package sim

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// The variables naming the two binaries of a roll: the previous release and
// the build under test.
const (
	rrFromEnv = "HOARDMQ_SIM_ROLL_FROM"
	rrToEnv   = "HOARDMQ_SIM_ROLL_TO"
)

const (
	// rrFrameBytes is the schema's least cluster frame limit, given to every
	// process through the environment so that a node's own route set is
	// several frames' worth with a population this test can create quickly.
	rrFrameBytes = 65536
	// rrSyncMs repeats full route sets often enough for a short window to
	// hold several.
	rrSyncMs = 2000
	// rrSessions persistent sessions, each with its own filter, give every
	// node an own route set of about 300 KB at about 90 bytes an entry.
	rrSessions      = 10000
	rrConcurrency   = 64
	rrSessionExpiry = 3600
	rrProbeMessages = 5
	// rrStartBound is how long a started node has for each of /readyz and
	// /servingz; the first start includes the bootstrap's join timeout.
	rrStartBound = 90 * time.Second
	// rrStopBound is how long a node has to exit after SIGTERM before it is
	// killed.
	rrStopBound = 60 * time.Second
	// rrProbeBudget bounds the retries of one node's probe publishes.
	rrProbeBudget = 30 * time.Second
	rrLogTail     = 40
)

// TestRollFromPreviousRelease rolls three broker processes, one at a time,
// from the previous release's binary to the build under test, with the
// cluster frame limit lowered under every node's own route set. The previous
// release sends a full set in one frame, which the transport refuses over the
// limit; after the roll every node must complete full sets from its peers
// (in parts) with no route send refused as too large, and QoS 1 publishes
// must be acknowledged and delivered across nodes.
//
// It runs real processes, so it is skipped unless HOARDMQ_SIM_ROLL_FROM and
// HOARDMQ_SIM_ROLL_TO name the two binaries.
func TestRollFromPreviousRelease(t *testing.T) {
	from, to := os.Getenv(rrFromEnv), os.Getenv(rrToEnv)
	if from == "" || to == "" {
		t.Skipf("set %s to the previous release's hoardmq binary and %s to the build under test", rrFromEnv, rrToEnv)
	}
	for _, bin := range []string{from, to} {
		if st, err := os.Stat(bin); err != nil || st.IsDir() || st.Mode()&0o111 == 0 {
			t.Fatalf("%s is not an executable file: %v", bin, err)
		}
	}
	t.Logf("roll from %s to %s; cluster frame limit %d bytes, route sync every %d ms", from, to, rrFrameBytes, rrSyncMs)

	dir := t.TempDir()
	nodes := rrNodes(t, dir, []string{"n1", "n2", "n3"})
	t.Cleanup(func() {
		for _, n := range nodes {
			n.stop(t)
		}
		if t.Failed() {
			for _, n := range nodes {
				t.Logf("%s: the last %d lines of %s:\n%s", n.name, rrLogTail, n.logPath, n.tail(rrLogTail))
			}
		}
	})

	for _, n := range nodes {
		n.start(t, from)
	}
	for _, n := range nodes {
		n.waitHTTP(t, "/readyz", rrStartBound)
	}
	for _, n := range nodes {
		n.waitHTTP(t, "/servingz", rrStartBound)
	}
	t.Logf("all three nodes ready and serving on %s", from)

	start := time.Now()
	created := rrCreateSessions(t, nodes)
	t.Logf("sessions created: %d of %d in %s (%d a node, each subscribed QoS 1 to r/<i>, then disconnected)",
		created, rrSessions, time.Since(start).Round(time.Millisecond), rrSessions/len(nodes))
	if created != rrSessions {
		t.Fatalf("created %d persistent sessions, want %d", created, rrSessions)
	}

	// A record of the previous release over the same window, not asserted:
	// its full sets are over the frame limit, so they are expected not to
	// complete.
	window := 3 * time.Duration(rrSyncMs) * time.Millisecond
	before := rrScrapeAll(t, nodes)
	time.Sleep(window)
	after := rrScrapeAll(t, nodes)
	for _, n := range nodes {
		b, a := before[n.name], after[n.name]
		t.Logf("control, %s on the previous release over %s: route syncs in %v -> %v, out %v -> %v",
			n.name, window, b.sum("hoardmq_route_syncs_total", rrLabels("direction", "in")), a.sum("hoardmq_route_syncs_total", rrLabels("direction", "in")),
			b.sum("hoardmq_route_syncs_total", rrLabels("direction", "out")), a.sum("hoardmq_route_syncs_total", rrLabels("direction", "out")))
	}

	// A node restarted beside peers on the previous release, whose own sets are
	// over the frame limit, cannot learn their routes until they too are
	// upgraded: it is ready but degraded (routes-unknown) until then. So the
	// roll moves on at readiness, as the chart's probe does, and serving is
	// required of every node once the last has rolled.
	for k, n := range nodes {
		rollStart := time.Now()
		n.stop(t)
		n.start(t, to)
		n.waitHTTP(t, "/readyz", rrStartBound)
		for _, m := range nodes {
			code, body, _ := m.get("/servingz")
			t.Logf("after %s's restart: %s /servingz %d %s", n.name, m.name, code, strings.TrimSpace(body))
		}
		t.Logf("%s rolled to %s in %s", n.name, to, time.Since(rollStart).Round(time.Millisecond))
		levels := rrLevels(t, nodes)
		t.Logf("after %s's restart: cluster feature levels %v", n.name, levels)
		if k < len(nodes)-1 {
			// A member still on the previous release holds the level at 1.
			for name, l := range levels {
				if l != 1 {
					t.Fatalf("%s reads cluster feature level %v with a member still on the previous release; want 1", name, l)
				}
			}
		}
		if k == 0 {
			// The first upgraded node imports sessions made on the others, its
			// targets still on the previous release: each import goes as one
			// snapshot they apply, and the session keeps its subscription. It
			// refuses publishes made on it until its peers' route sets reach it
			// (degraded), so they are made on a node still on the previous
			// release and forwarded.
			rrMoveAndDeliver(t, n, nodes[k+1], []int{1, 2, 4, 5})
		}
	}
	for _, n := range nodes {
		n.waitHTTP(t, "/servingz", rrStartBound)
	}
	rrWaitLevel(t, nodes, 2)
	raises := 0.0
	for _, sc := range rrScrapeAll(t, nodes) {
		raises += sc.sum("hoardmq_cluster_feature_level_raises_total", rrLabels("outcome", "applied"))
	}
	t.Logf("after the roll: the level rose to 2; raises applied, summed over the nodes: %v", raises)
	if raises != 1 {
		t.Fatalf("raises applied %v after the roll; want exactly 1", raises)
	}
	// After the raise a lone meta record finding no copy makes none; imports
	// and resumes still keep every session whole.
	rrMoveAndDeliver(t, nodes[1], nodes[1], []int{0, 3, 6, 9})
	rrMoveAndDeliver(t, nodes[2], nodes[0], []int{0, 1, 4, 7})

	before = rrScrapeAll(t, nodes)
	time.Sleep(window)
	after = rrScrapeAll(t, nodes)
	for _, n := range nodes {
		b, a := before[n.name], after[n.name]
		tooLargeBefore := b.sum("hoardmq_route_send_failures_total", rrLabels("reason", "too-large"))
		tooLargeAfter := a.sum("hoardmq_route_send_failures_total", rrLabels("reason", "too-large"))
		inBefore := b.sum("hoardmq_route_syncs_total", rrLabels("direction", "in"))
		inAfter := a.sum("hoardmq_route_syncs_total", rrLabels("direction", "in"))
		outBefore := b.sum("hoardmq_route_syncs_total", rrLabels("direction", "out"))
		outAfter := a.sum("hoardmq_route_syncs_total", rrLabels("direction", "out"))
		partsBefore := b.sum("hoardmq_route_set_parts_total", rrLabels("direction", "out"))
		partsAfter := a.sum("hoardmq_route_set_parts_total", rrLabels("direction", "out"))
		t.Logf("%s after the roll, over %s: route syncs in %v -> %v, out %v -> %v; parts out %v -> %v; send failures before %s, after %s; own set %v entries in %v parts",
			n.name, window, inBefore, inAfter, outBefore, outAfter, partsBefore, partsAfter,
			b.byLabel("hoardmq_route_send_failures_total", "reason"), a.byLabel("hoardmq_route_send_failures_total", "reason"),
			a.sum("hoardmq_route_set_entries", nil), a.sum("hoardmq_route_set_parts", nil))
		if tooLargeAfter > tooLargeBefore {
			t.Errorf("%s: route sends refused as too large rose from %v to %v after the roll", n.name, tooLargeBefore, tooLargeAfter)
		}
		if inAfter-inBefore < 2 {
			t.Errorf("%s: complete route sets received rose by %v over %s, want at least 2", n.name, inAfter-inBefore, window)
		}
		// Without this a node sending its sets in one frame (a peer link
		// that did not negotiate the parts version) could pass on sets
		// under the limit.
		if partsAfter <= partsBefore {
			t.Errorf("%s: no route set part was sent over %s: the sets did not travel in parts", n.name, window)
		}
	}
	if t.Failed() {
		t.FailNow()
	}

	for i, sub := range nodes {
		pub := nodes[(i+1)%len(nodes)]
		rrProbe(t, sub, pub)
	}
}

// rrNode is one broker process's configuration and, while it runs, the
// process the test started for it. The test stops only the process held
// here.
type rrNode struct {
	name    string
	mqtt    string
	cluster string
	obs     string
	doc     string
	logPath string

	cmd     *exec.Cmd
	pid     int
	bin     string
	exited  chan struct{}
	waitErr error
	logFile *os.File
}

// rrNodes draws every node's addresses and writes its configuration
// document.
func rrNodes(t *testing.T, dir string, names []string) []*rrNode {
	t.Helper()
	secret := filepath.Join(dir, "cluster.secret")
	if err := os.WriteFile(secret, []byte("roll-from-release-cluster-secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	acl := filepath.Join(dir, "acl.json")
	if err := os.WriteFile(acl, []byte(`{"rules":[{"topic":"#","allow":["publish","subscribe","unsubscribe"]}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	nodes := make([]*rrNode, len(names))
	for i, name := range names {
		nodes[i] = &rrNode{name: name, mqtt: freePort(t), cluster: freePort(t), obs: freePort(t),
			doc: filepath.Join(dir, name+".json"), logPath: filepath.Join(dir, name+".log")}
	}
	for _, n := range nodes {
		doc := config.Default()
		doc.Node.Name = n.name
		doc.Node.DataDir = filepath.Join(dir, n.name, "data")
		doc.Listeners = []config.Listener{{Name: "tcp", Type: "tcp", Address: n.mqtt}}
		doc.Auth.Anonymous = true
		aclPath, err := json.Marshal(acl)
		if err != nil {
			t.Fatal(err)
		}
		doc.Modules = map[string]json.RawMessage{
			"authz.file":         json.RawMessage(`{"path":` + string(aclPath) + `}`),
			"metrics.prometheus": json.RawMessage(`{}`),
		}
		doc.Observability.Address = n.obs
		doc.Cluster.Listen = n.cluster
		doc.Cluster.Advertise = n.mqtt
		doc.Cluster.Auth = config.ClusterAuth{Mode: "secret", SecretFile: secret}
		doc.Cluster.Routing.SyncMs = rrSyncMs
		// The lowest name bootstraps once no peer answers within this; the
		// default's 30 s would only lengthen the first start.
		doc.Cluster.Raft.JoinTimeoutMs = 3000
		for _, other := range nodes {
			if other != n {
				doc.Cluster.Peers = append(doc.Cluster.Peers, config.Peer{Name: other.name, Address: other.cluster})
			}
		}
		if p := doc.Validate(); len(p) != 0 {
			t.Fatalf("%s's configuration: %v", n.name, p)
		}
		data, err := json.MarshalIndent(doc, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(n.doc, data, 0o600); err != nil {
			t.Fatal(err)
		}
		t.Logf("%s: mqtt %s, cluster %s, observability %s, document %s", n.name, n.mqtt, n.cluster, n.obs, n.doc)
	}
	return nodes
}

// rrEnv is the test's environment without its HOARDMQ_ variables, with the
// frame limit added: the broker reads every HOARDMQ_ variable as an override
// and refuses one it does not know, such as this test's own.
func rrEnv() []string {
	env := []string{}
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, config.EnvPrefix) {
			env = append(env, kv)
		}
	}
	return append(env, config.EnvPrefix+"CLUSTER_MAX_FRAME_BYTES="+strconv.Itoa(rrFrameBytes))
}

// start runs the node on bin, its output appended to the node's log file.
func (n *rrNode) start(t *testing.T, bin string) {
	t.Helper()
	f, err := os.OpenFile(n.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = fmt.Fprintf(f, "=== %s: hoardmq serve on %s\n", time.Now().Format(time.RFC3339Nano), bin)
	cmd := exec.Command(bin, "serve", "-config", n.doc)
	cmd.Env = rrEnv()
	cmd.Stdout = f
	cmd.Stderr = f
	if err := cmd.Start(); err != nil {
		_ = f.Close()
		t.Fatalf("start %s on %s: %v", n.name, bin, err)
	}
	n.cmd, n.pid, n.bin, n.logFile, n.waitErr = cmd, cmd.Process.Pid, bin, f, nil
	exited := make(chan struct{})
	n.exited = exited
	go func() {
		err := cmd.Wait()
		n.waitErr = err
		close(exited)
	}()
	t.Logf("%s: started pid %d on %s", n.name, n.pid, bin)
}

// running reports whether the process the test started is still up.
func (n *rrNode) running() bool {
	if n.cmd == nil {
		return false
	}
	select {
	case <-n.exited:
		return false
	default:
		return true
	}
}

// stop sends SIGTERM to the process the test started and waits for it to
// exit, killing it if it has not within rrStopBound. It signals only through
// the started command, which refuses once the process has been waited for,
// so it never reaches a process the test did not start.
func (n *rrNode) stop(t *testing.T) {
	t.Helper()
	if n.cmd == nil {
		return
	}
	if n.running() {
		if err := n.cmd.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
			t.Logf("%s: SIGTERM to pid %d: %v", n.name, n.pid, err)
		}
		select {
		case <-n.exited:
		case <-time.After(rrStopBound):
			t.Logf("%s: pid %d had not exited %s after SIGTERM; killing it", n.name, n.pid, rrStopBound)
			if err := n.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
				t.Logf("%s: SIGKILL to pid %d: %v", n.name, n.pid, err)
			}
			select {
			case <-n.exited:
			case <-time.After(10 * time.Second):
				t.Errorf("%s: pid %d did not exit after SIGKILL", n.name, n.pid)
				return
			}
		}
	}
	<-n.exited
	t.Logf("%s: pid %d on %s exited: %v", n.name, n.pid, n.bin, n.waitErr)
	_ = n.logFile.Close()
	n.cmd = nil
}

// tail is the last lines of the node's log file.
func (n *rrNode) tail(lines int) string {
	data, err := os.ReadFile(n.logPath)
	if err != nil {
		return err.Error()
	}
	all := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(all) > lines {
		all = all[len(all)-lines:]
	}
	return strings.Join(all, "\n")
}

var rrHTTP = &http.Client{Timeout: 5 * time.Second}

// get fetches a path from the node's observability listener.
func (n *rrNode) get(path string) (int, string, error) {
	resp, err := rrHTTP.Get("http://" + n.obs + path)
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body), err
}

// waitHTTP waits for path to answer 200, failing the test if the process
// exits first or the bound passes.
func (n *rrNode) waitHTTP(t *testing.T, path string, bound time.Duration) {
	t.Helper()
	start := time.Now()
	deadline := start.Add(bound)
	var last string
	for {
		if !n.running() {
			t.Fatalf("%s: pid %d exited while waiting for %s: %v", n.name, n.pid, path, n.waitErr)
		}
		code, body, err := n.get(path)
		if err == nil && code == http.StatusOK {
			t.Logf("%s: %s 200 after %s", n.name, path, time.Since(start).Round(time.Millisecond))
			return
		}
		if err != nil {
			last = err.Error()
		} else {
			last = fmt.Sprintf("%d %s", code, strings.TrimSpace(body))
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: %s not 200 within %s; last answer: %s", n.name, path, bound, last)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// rrSample is one sample of a metrics scrape.
type rrSample struct {
	name   string
	labels map[string]string
	value  float64
}

// rrScrape is one node's /metrics.
type rrScrape []rrSample

// rrLabels builds a label set from name, value pairs.
func rrLabels(kv ...string) map[string]string {
	m := map[string]string{}
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}
	return m
}

// sum adds the samples of name whose labels include every one of want; a
// family the scrape does not hold sums to 0.
func (s rrScrape) sum(name string, want map[string]string) float64 {
	total := 0.0
	for _, m := range s {
		if m.name != name {
			continue
		}
		match := true
		for k, v := range want {
			if m.labels[k] != v {
				match = false
				break
			}
		}
		if match {
			total += m.value
		}
	}
	return total
}

// byLabel renders a family summed by one label, for the log.
func (s rrScrape) byLabel(name, label string) string {
	sums := map[string]float64{}
	for _, m := range s {
		if m.name == name {
			sums[m.labels[label]] += m.value
		}
	}
	if len(sums) == 0 {
		return "none"
	}
	keys := make([]string, 0, len(sums))
	for k := range sums {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%v", k, sums[k]))
	}
	return "{" + strings.Join(parts, " ") + "}"
}

// rrParse reads the text exposition format the broker writes: a comment
// line, or name{label="value",...} value.
func rrParse(body string) (rrScrape, error) {
	var out rrScrape
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		cut := strings.LastIndexByte(line, ' ')
		if cut < 0 {
			return nil, fmt.Errorf("sample without a value: %q", line)
		}
		value, err := strconv.ParseFloat(line[cut+1:], 64)
		if err != nil {
			return nil, fmt.Errorf("sample %q: %v", line, err)
		}
		series := line[:cut]
		m := rrSample{name: series, labels: map[string]string{}, value: value}
		if open := strings.IndexByte(series, '{'); open >= 0 {
			if !strings.HasSuffix(series, "}") {
				return nil, fmt.Errorf("unterminated label set: %q", line)
			}
			m.name = series[:open]
			if err := rrParseLabels(series[open+1:len(series)-1], m.labels); err != nil {
				return nil, fmt.Errorf("sample %q: %v", line, err)
			}
		}
		out = append(out, m)
	}
	return out, nil
}

// rrParseLabels reads k="v" pairs separated by commas, with the exposition
// format's escapes in values.
func rrParseLabels(s string, into map[string]string) error {
	for len(s) > 0 {
		eq := strings.IndexByte(s, '=')
		if eq < 0 || eq+1 >= len(s) || s[eq+1] != '"' {
			return fmt.Errorf("malformed label set at %q", s)
		}
		key := s[:eq]
		var b strings.Builder
		i := eq + 2
		for ; i < len(s) && s[i] != '"'; i++ {
			if s[i] == '\\' && i+1 < len(s) {
				i++
				switch s[i] {
				case 'n':
					b.WriteByte('\n')
				default:
					b.WriteByte(s[i])
				}
				continue
			}
			b.WriteByte(s[i])
		}
		if i >= len(s) {
			return fmt.Errorf("unterminated label value at %q", s)
		}
		into[key] = b.String()
		s = strings.TrimPrefix(s[i+1:], ",")
	}
	return nil
}

// rrScrapeAll reads every node's /metrics.
func rrScrapeAll(t *testing.T, nodes []*rrNode) map[string]rrScrape {
	t.Helper()
	out := map[string]rrScrape{}
	for _, n := range nodes {
		code, body, err := n.get("/metrics")
		if err != nil || code != http.StatusOK {
			t.Fatalf("%s: /metrics: %d %v", n.name, code, err)
		}
		s, err := rrParse(body)
		if err != nil {
			t.Fatalf("%s: /metrics: %v", n.name, err)
		}
		out[n.name] = s
	}
	return out
}

// rrConn is a minimal MQTT 5 connection over the project codec. Its methods
// return errors rather than failing the test, so it may run on any
// goroutine.
type rrConn struct {
	conn net.Conn
	enc  *packet.Encoder
	dec  *packet.Decoder
	id   uint16
}

// rrDial connects with a CONNECT built by the caller and requires a
// successful CONNACK.
func rrDial(addr string, connect *packet.Connect) (*rrConn, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	c := &rrConn{conn: conn, enc: &packet.Encoder{Version: packet.V5}, dec: packet.NewDecoder(conn)}
	c.dec.Version = packet.V5
	connect.ProtocolName, connect.ProtocolLevel = "MQTT", packet.V5
	if err := c.write(connect); err != nil {
		_ = conn.Close()
		return nil, err
	}
	p, err := c.read(15 * time.Second)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("CONNACK: %w", err)
	}
	if a, ok := p.(*packet.Connack); !ok || a.ReasonCode != packet.Success {
		_ = conn.Close()
		return nil, fmt.Errorf("CONNECT %s answered %#v", connect.ClientID, p)
	}
	return c, nil
}

func (c *rrConn) write(p packet.Packet) error {
	if err := c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}
	return c.enc.Encode(c.conn, p)
}

func (c *rrConn) read(d time.Duration) (packet.Packet, error) {
	if err := c.conn.SetReadDeadline(time.Now().Add(d)); err != nil {
		return nil, err
	}
	return c.dec.Read()
}

func (c *rrConn) nextID() uint16 {
	c.id++
	if c.id == 0 {
		c.id = 1
	}
	return c.id
}

// subscribe sends one QoS 1 subscription and requires it granted at QoS 1.
// A PUBLISH arriving first is acknowledged and otherwise ignored.
func (c *rrConn) subscribe(filter string) error {
	id := c.nextID()
	if err := c.write(&packet.Subscribe{PacketID: id, Subscriptions: []packet.Subscription{{Filter: filter, QoS: 1}}}); err != nil {
		return err
	}
	for {
		p, err := c.read(15 * time.Second)
		if err != nil {
			return fmt.Errorf("SUBACK for %s: %w", filter, err)
		}
		switch m := p.(type) {
		case *packet.Suback:
			if m.PacketID != id || len(m.ReasonCodes) != 1 || m.ReasonCodes[0] != packet.GrantedQoS1 {
				return fmt.Errorf("SUBSCRIBE %s answered %#v", filter, m)
			}
			return nil
		case *packet.Publish:
			if m.QoS == 1 {
				if err := c.write(&packet.Puback{Ack: packet.Ack{PacketID: m.PacketID}}); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("SUBSCRIBE %s answered %#v", filter, p)
		}
	}
}

// disconnect ends the connection normally, which keeps a session with an
// expiry.
func (c *rrConn) disconnect() error {
	err := c.write(&packet.Disconnect{ReasonCode: packet.NormalDisconnection})
	if cerr := c.conn.Close(); err == nil {
		err = cerr
	}
	return err
}

// rrSession makes persistent session i on addr: Clean Start false, an hour's
// expiry, one QoS 1 subscription to its own filter, then a normal
// disconnect.
func rrSession(addr string, i int) error {
	expiry := uint32(rrSessionExpiry)
	c, err := rrDial(addr, &packet.Connect{ClientID: fmt.Sprintf("rr-%d", i), KeepAlive: 60,
		Properties: packet.Properties{SessionExpiryInterval: &expiry}})
	if err != nil {
		return err
	}
	if err := c.subscribe(fmt.Sprintf("r/%d", i)); err != nil {
		_ = c.conn.Close()
		return err
	}
	return c.disconnect()
}

// rrCreateSessions makes rrSessions persistent sessions spread evenly over
// the nodes, rrConcurrency at a time, retrying each a few times, and returns
// how many were made. Every failure is logged up to a bound.
func rrCreateSessions(t *testing.T, nodes []*rrNode) int {
	t.Helper()
	var made atomic.Int64
	var mu sync.Mutex
	var failures []string
	sem := make(chan struct{}, rrConcurrency)
	var wg sync.WaitGroup
	for i := 0; i < rrSessions; i++ {
		sem <- struct{}{}
		wg.Add(1)
		go func(i int) {
			defer func() { <-sem; wg.Done() }()
			addr := nodes[i%len(nodes)].mqtt
			var err error
			for attempt := 0; attempt < 5; attempt++ {
				if attempt > 0 {
					time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
				}
				if err = rrSession(addr, i); err == nil {
					made.Add(1)
					return
				}
			}
			mu.Lock()
			failures = append(failures, fmt.Sprintf("session %d on %s: %v", i, addr, err))
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	for i, f := range failures {
		if i == 20 {
			t.Logf("... and %d more failed sessions", len(failures)-i)
			break
		}
		t.Log(f)
	}
	return int(made.Load())
}

// rrProbe subscribes on sub's node to probe/<sub> and publishes
// rrProbeMessages QoS 1 messages to it from pub's node, requiring each
// publish's final PUBACK to be Success and each message delivered. A publish
// answered 0x83 (delivery not confirmed in time) or 0x10 (the publisher's node
// does not know the route yet) is retried as a new attempt of the same
// message within rrProbeBudget; a message counts as delivered once whichever
// attempt arrives.
func rrProbe(t *testing.T, sub, pub *rrNode) {
	t.Helper()
	topic := "probe/" + sub.name
	s, err := rrDial(sub.mqtt, &packet.Connect{ClientID: "rr-probe-sub-" + sub.name, CleanStart: true, KeepAlive: 60})
	if err != nil {
		t.Fatalf("probe subscriber on %s: %v", sub.name, err)
	}
	defer func() { _ = s.conn.Close() }()
	if err := s.subscribe(topic); err != nil {
		t.Fatalf("probe subscriber on %s: %v", sub.name, err)
	}

	var mu sync.Mutex
	delivered := map[int]bool{}
	got := make(chan struct{}, 1)
	readErr := make(chan error, 1)
	stop := make(chan struct{})
	// The reader blocks until a packet or the connection's close: a read
	// deadline passing mid-packet would leave the decoder out of step.
	go func() {
		for {
			p, err := s.read(rrProbeBudget + time.Minute)
			select {
			case <-stop:
				return
			default:
			}
			if err != nil {
				readErr <- err
				return
			}
			m, ok := p.(*packet.Publish)
			if !ok {
				readErr <- fmt.Errorf("probe subscriber received %#v", p)
				return
			}
			if m.QoS == 1 {
				if err := s.write(&packet.Puback{Ack: packet.Ack{PacketID: m.PacketID}}); err != nil {
					readErr <- err
					return
				}
			}
			seq, _, ok := strings.Cut(string(m.Payload), "/")
			n, err := strconv.Atoi(seq)
			if !ok || err != nil {
				readErr <- fmt.Errorf("probe subscriber received payload %q", m.Payload)
				return
			}
			mu.Lock()
			delivered[n] = true
			mu.Unlock()
			select {
			case got <- struct{}{}:
			default:
			}
		}
	}()
	defer close(stop)

	p, err := rrDial(pub.mqtt, &packet.Connect{ClientID: "rr-probe-pub-" + pub.name + "-to-" + sub.name, CleanStart: true, KeepAlive: 60})
	if err != nil {
		t.Fatalf("probe publisher on %s: %v", pub.name, err)
	}
	defer func() { _ = p.conn.Close() }()

	budget := time.Now().Add(rrProbeBudget)
	retries := map[packet.ReasonCode]int{}
	for msg := 1; msg <= rrProbeMessages; msg++ {
		for attempt := 1; ; attempt++ {
			id := p.nextID()
			payload := fmt.Sprintf("%d/%d", msg, attempt)
			if err := p.write(&packet.Publish{QoS: 1, Topic: topic, PacketID: id, Payload: []byte(payload)}); err != nil {
				t.Fatalf("probe publish from %s to %s: %v", pub.name, topic, err)
			}
			a, err := p.read(15 * time.Second)
			if err != nil {
				t.Fatalf("probe publish from %s to %s: PUBACK: %v", pub.name, topic, err)
			}
			ack, ok := a.(*packet.Puback)
			if !ok || ack.PacketID != id {
				t.Fatalf("probe publish from %s to %s answered %#v", pub.name, topic, a)
			}
			if ack.ReasonCode == packet.Success {
				break
			}
			retryable := ack.ReasonCode == packet.ImplementationSpecificError || ack.ReasonCode == packet.NoMatchingSubscribers
			if !retryable || time.Now().After(budget) {
				t.Fatalf("probe publish %d from %s to %s answered reason 0x%02x on attempt %d (retries so far %v)",
					msg, pub.name, topic, byte(ack.ReasonCode), attempt, retries)
			}
			retries[ack.ReasonCode]++
			time.Sleep(250 * time.Millisecond)
		}
	}

	deadline := time.After(15 * time.Second)
	for {
		mu.Lock()
		n := len(delivered)
		mu.Unlock()
		if n == rrProbeMessages {
			break
		}
		select {
		case <-got:
		case err := <-readErr:
			t.Fatalf("probe subscriber on %s: %v", sub.name, err)
		case <-deadline:
			mu.Lock()
			seen := make([]int, 0, len(delivered))
			for m := range delivered {
				seen = append(seen, m)
			}
			mu.Unlock()
			sort.Ints(seen)
			t.Fatalf("probe from %s to %s: messages %v of 1..%d delivered within 15 s of their PUBACKs", pub.name, sub.name, seen, rrProbeMessages)
		}
	}
	t.Logf("probe from %s to %s: %d of %d QoS 1 publishes acknowledged Success and delivered; publishes retried by reason %v",
		pub.name, sub.name, rrProbeMessages, rrProbeMessages, retries)
}

// rrLevels reads each node's cluster feature level.
func rrLevels(t *testing.T, nodes []*rrNode) map[string]float64 {
	t.Helper()
	out := map[string]float64{}
	for name, sc := range rrScrapeAll(t, nodes) {
		out[name] = sc.sum("hoardmq_cluster_feature_level", nil)
	}
	return out
}

// rrWaitLevel waits for every node to read the cluster feature level want.
func rrWaitLevel(t *testing.T, nodes []*rrNode, want float64) {
	t.Helper()
	deadline := time.Now().Add(rrStartBound)
	for {
		levels := rrLevels(t, nodes)
		done := true
		for _, l := range levels {
			if l != want {
				done = false
			}
		}
		if done {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("cluster feature levels %v after %s; want %v on every node", levels, rrStartBound, want)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// rrMoveAndDeliver reconnects sessions made elsewhere to n, which takes each
// over and imports it, then publishes one QoS 1 message from n to each one's
// filter from via's node: every publish must be acknowledged Success, which
// needs the session's targets to acknowledge its records, and delivered,
// which needs the session to have arrived with its subscription. n's whole
// writes of imports, counted by form, must rise by at least one each.
func rrMoveAndDeliver(t *testing.T, n, via *rrNode, ids []int) {
	t.Helper()
	wholes := func() float64 {
		return rrScrapeAll(t, []*rrNode{n})[n.name].sum("hoardmq_replication_whole_writes_total", rrLabels("cause", "import"))
	}
	before := wholes()
	expiry := uint32(rrSessionExpiry)
	pub, err := rrDial(via.mqtt, &packet.Connect{ClientID: fmt.Sprintf("rr-mover-%s-%d", via.name, time.Now().UnixNano()), CleanStart: true, KeepAlive: 60})
	if err != nil {
		t.Fatalf("publisher on %s: %v", via.name, err)
	}
	defer func() { _ = pub.disconnect() }()
	for _, i := range ids {
		deadline := time.Now().Add(rrProbeBudget)
		var c *rrConn
		for {
			c, err = rrDial(n.mqtt, &packet.Connect{ClientID: fmt.Sprintf("rr-%d", i), KeepAlive: 60, Properties: packet.Properties{SessionExpiryInterval: &expiry}})
			if err == nil || time.Now().After(deadline) {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		if err != nil {
			t.Fatalf("session rr-%d moving to %s: %v", i, n.name, err)
		}
		topic := fmt.Sprintf("r/%d", i)
		reasons := map[byte]int{}
		for acked := false; !acked; {
			if time.Now().After(deadline) {
				t.Fatalf("publish to %s on %s: not acknowledged Success within %s; reasons %v", topic, n.name, rrProbeBudget, reasons)
			}
			id := pub.nextID()
			if err := pub.write(&packet.Publish{Topic: topic, QoS: 1, PacketID: id, Payload: []byte("moved")}); err != nil {
				t.Fatalf("publish to %s on %s: %v", topic, n.name, err)
			}
			p, err := pub.read(15 * time.Second)
			if err != nil {
				t.Fatalf("publish to %s on %s: PUBACK: %v", topic, n.name, err)
			}
			a, ok := p.(*packet.Puback)
			if !ok || a.PacketID != id {
				t.Fatalf("publish to %s on %s answered %#v", topic, n.name, p)
			}
			if acked = a.ReasonCode == packet.Success; !acked {
				reasons[byte(a.ReasonCode)]++
				time.Sleep(200 * time.Millisecond)
			}
		}
		for delivered := false; !delivered; {
			p, err := c.read(15 * time.Second)
			if err != nil {
				t.Fatalf("session rr-%d on %s: the message to %s not delivered: %v", i, n.name, topic, err)
			}
			if m, ok := p.(*packet.Publish); ok {
				if m.QoS == 1 {
					_ = c.write(&packet.Puback{Ack: packet.Ack{PacketID: m.PacketID}})
				}
				delivered = string(m.Payload) == "moved"
			}
		}
		_ = c.disconnect()
		if len(reasons) > 0 {
			t.Logf("publish to %s on %s retried by reason %v", topic, n.name, reasons)
		}
	}
	after := wholes()
	t.Logf("%s imported %d sessions: whole writes of imports %v -> %v (%v)", n.name, len(ids), before, after,
		rrScrapeAll(t, []*rrNode{n})[n.name].byLabel("hoardmq_replication_whole_writes_total", "form"))
	if after-before < float64(len(ids)) {
		t.Fatalf("%s's whole writes of imports rose by %v for %d imports", n.name, after-before, len(ids))
	}
}
