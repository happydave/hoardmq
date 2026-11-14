package sim

import (
	"bufio"
	"context"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The measurement across hosts: broker processes started on the hosts a
// layout names, the clients in the driver process on the test's host, and
// the phases of one run read from the processes, their hosts and the nodes'
// /metrics. sim/README.md, "Across hosts", describes the run.

const (
	scaleLayoutEnv    = "HOARDMQ_SIM_SCALE_LAYOUT"
	scaleClientsEnv   = "HOARDMQ_SIM_SCALE_CLIENTS"
	scaleQueueEnv     = "HOARDMQ_SIM_SCALE_QUEUE"
	scaleVictimEnv    = "HOARDMQ_SIM_SCALE_VICTIM"
	scaleNodeBytesEnv = "HOARDMQ_SIM_SCALE_NODE_BYTES"
	scaleDrvBytesEnv  = "HOARDMQ_SIM_SCALE_DRIVER_BYTES"
)

// scaleNode is one broker process of the layout. Host is "local" or an ssh
// destination.
type scaleNode struct {
	Name        string `json:"name"`
	Host        string `json:"host"`
	Cores       string `json:"cores"`
	Address     string `json:"address"`
	ClientPort  int    `json:"client_port"`
	ClusterPort int    `json:"cluster_port"`
	// ObsAddress is the observability listener's host, the node's address
	// when empty.
	ObsAddress string `json:"observability_address,omitempty"`
	ObsPort    int    `json:"observability_port"`
	Binary     string `json:"binary"`
	RunRoot    string `json:"run_root"`
}

func (n scaleNode) clientAddr() string {
	return net.JoinHostPort(n.Address, strconv.Itoa(n.ClientPort))
}
func (n scaleNode) clusterAddr() string {
	return net.JoinHostPort(n.Address, strconv.Itoa(n.ClusterPort))
}
func (n scaleNode) obsAddr() string {
	h := n.ObsAddress
	if h == "" {
		h = n.Address
	}
	return net.JoinHostPort(h, strconv.Itoa(n.ObsPort))
}

// scaleDriver is the client driver: its binary on the test's host, the cores
// it is pinned to, and the addresses its clients dial from.
type scaleDriver struct {
	Binary  string   `json:"binary"`
	Cores   string   `json:"cores"`
	Sources []string `json:"sources"`
}

type scaleLayout struct {
	Nodes  []scaleNode `json:"nodes"`
	Driver scaleDriver `json:"driver"`
}

// scaleACL is the authorizer's rule file: every topic, every action.
var scaleACL = []byte(`{"rules":[{"topic":"#","allow":["publish","subscribe","unsubscribe"]}]}` + "\n")

var cpuListRE = regexp.MustCompile(`^[0-9]+(-[0-9]+)?(,[0-9]+(-[0-9]+)?)*$`)

// layoutProblems names every problem of a layout. A listener must name an
// address: the client listeners admit anonymous clients, so none binds a
// wildcard.
func layoutProblems(l scaleLayout) []string {
	var out []string
	if len(l.Nodes) < 2 {
		out = append(out, fmt.Sprintf("%d nodes; the measurement needs at least two", len(l.Nodes)))
	}
	names := map[string]bool{}
	bound := map[string]string{}
	bind := func(who, addr string) {
		if prev, ok := bound[addr]; ok {
			out = append(out, fmt.Sprintf("%s and %s both bind %s", prev, who, addr))
			return
		}
		bound[addr] = who
	}
	for _, n := range l.Nodes {
		if n.Name == "" || names[n.Name] {
			out = append(out, fmt.Sprintf("node name %q is empty or repeated", n.Name))
		}
		names[n.Name] = true
		if n.Host == "" {
			out = append(out, n.Name+": no host")
		}
		if !cpuListRE.MatchString(n.Cores) {
			out = append(out, fmt.Sprintf("%s: cores %q is not a CPU list", n.Name, n.Cores))
		}
		for i, a := range []string{n.Address, n.ObsAddress} {
			if i == 1 && a == "" {
				continue
			}
			ip := net.ParseIP(a)
			if ip == nil || ip.IsUnspecified() {
				out = append(out, fmt.Sprintf("%s: address %q is not a specific IP address", n.Name, a))
			}
		}
		for _, p := range []int{n.ClientPort, n.ClusterPort, n.ObsPort} {
			if p <= 0 || p > 65535 {
				out = append(out, fmt.Sprintf("%s: port %d out of range", n.Name, p))
			}
		}
		bind(n.Name+" clients", n.clientAddr())
		bind(n.Name+" cluster", n.clusterAddr())
		bind(n.Name+" observability", n.obsAddr())
		if !filepath.IsAbs(n.Binary) {
			out = append(out, fmt.Sprintf("%s: binary %q is not an absolute path", n.Name, n.Binary))
		}
		if !runRootOK(n.RunRoot) {
			out = append(out, fmt.Sprintf("%s: run root %q must be absolute and at least three directories deep", n.Name, n.RunRoot))
		}
	}
	if !filepath.IsAbs(l.Driver.Binary) {
		out = append(out, fmt.Sprintf("driver: binary %q is not an absolute path", l.Driver.Binary))
	}
	if !cpuListRE.MatchString(l.Driver.Cores) {
		out = append(out, fmt.Sprintf("driver: cores %q is not a CPU list", l.Driver.Cores))
	}
	if len(l.Driver.Sources) == 0 {
		out = append(out, "driver: no source addresses")
	}
	for _, s := range l.Driver.Sources {
		if net.ParseIP(s) == nil {
			out = append(out, fmt.Sprintf("driver: source %q is not an IP address", s))
		}
	}
	return out
}

// runRootOK accepts an absolute, clean path of at least three components, so
// a run's directories never sit directly in /, a top directory or a home.
func runRootOK(root string) bool {
	if !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return false
	}
	return len(strings.Split(strings.Trim(root, "/"), "/")) >= 3
}

// scaleRunIDRE is the form of a run's identifier, the only directory name the
// harness removes under a run root.
var scaleRunIDRE = regexp.MustCompile(`^scale-[0-9]{8}-[0-9]{6}-[0-9a-f]{6}$`)

func newScaleRunID(now time.Time) string {
	var b [3]byte
	h := sha256.Sum256([]byte(fmt.Sprint(now.UnixNano(), os.Getpid())))
	copy(b[:], h[:3])
	return "scale-" + now.Format("20060102-150405") + "-" + hex.EncodeToString(b[:])
}

// removeAllowed decides a removal: only the run's own directory under a valid
// run root, or a directory below it, whose resolved path is the path itself
// (no symlink on the way) and whose components are plain.
func removeAllowed(path, resolved, runRoot, runID string) bool {
	if !runRootOK(runRoot) || !scaleRunIDRE.MatchString(runID) {
		return false
	}
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || resolved != path {
		return false
	}
	base := filepath.Join(runRoot, runID)
	return path == base || strings.HasPrefix(path, base+"/")
}

// procRecord is a process as the harness launched it.
type procRecord struct {
	PID        int    `json:"pid"`
	StartTicks uint64 `json:"start_ticks"`
	Exe        string `json:"exe"`
}

// killAllowed decides a signal: only to the process recorded at launch, read
// again now with the same start time and executable, so a reused PID is never
// signalled.
func killAllowed(recorded, now procRecord) bool {
	return recorded.PID > 1 && now.PID == recorded.PID && recorded.StartTicks != 0 &&
		now.StartTicks == recorded.StartTicks && recorded.Exe != "" && now.Exe == recorded.Exe
}

// hostShell runs scripts on one host: locally, or over ssh.
type hostShell interface {
	command(ctx context.Context, script string) *exec.Cmd
	label() string
}

type localShell struct{}

func (localShell) command(ctx context.Context, script string) *exec.Cmd {
	return exec.CommandContext(ctx, "bash", "-c", script)
}
func (localShell) label() string { return "local" }

type sshShell struct{ dest string }

func (s sshShell) command(ctx context.Context, script string) *exec.Cmd {
	return exec.CommandContext(ctx, "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", s.dest, "bash -c "+shellQuote(script)) //nolint:gosec // the layout's host
}
func (s sshShell) label() string { return s.dest }

func shellFor(host string) hostShell {
	if host == "local" {
		return localShell{}
	}
	return sshShell{dest: host}
}

func shellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }

// runScript runs a script and returns its standard output; stderr is kept
// for the error.
func runScript(ctx context.Context, sh hostShell, script string) (string, error) {
	cmd := sh.command(ctx, script)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return string(out), fmt.Errorf("%s: %w: %s", sh.label(), err, strings.TrimSpace(stderr.String()))
	}
	return string(out), nil
}

// hostFacts is what the harness reads about a host before a run.
type hostFacts struct {
	Hostname        string            `json:"hostname"`
	Kernel          string            `json:"kernel"`
	CPUs            int               `json:"cpus"`
	MemAvailableKB  int64             `json:"mem_available_kb"`
	NofileSoft      int64             `json:"nofile_soft"`
	NofileHard      int64             `json:"nofile_hard"`
	Sysctl          map[string]string `json:"sysctl"`
	Load1           float64           `json:"load1"`
	NTP             string            `json:"ntp"`
	Addrs           []string          `json:"addrs"`
	Listening       []string          `json:"listening"`
	BinarySHA256    map[string]string `json:"binary_sha256"`
	Strays          []int             `json:"strays,omitempty"`
	RunDirExists    bool              `json:"run_dir_exists"`
	ConntrackLoaded bool              `json:"conntrack_loaded"`
	ConntrackMax    int64             `json:"conntrack_max,omitempty"`
	ConntrackCount  int64             `json:"conntrack_count,omitempty"`
}

// factsScript prints one key=value line per fact. It only reads.
func factsScript(binaries []string, runDir string) string {
	var b strings.Builder
	b.WriteString(`echo "hostname=$(hostname)"; echo "kernel=$(uname -r)"; echo "cpus=$(nproc)"
awk '/^MemAvailable:/{print "mem_available_kb="$2}' /proc/meminfo
echo "nofile_soft=$(ulimit -Sn)"; echo "nofile_hard=$(ulimit -Hn)"
for k in net/netfilter/nf_conntrack_max net/netfilter/nf_conntrack_count net/core/somaxconn net/ipv4/tcp_max_syn_backlog net/core/netdev_max_backlog net/ipv4/ip_local_port_range; do
  if [ -r /proc/sys/$k ]; then echo "sysctl.$k=$(tr '\t' ' ' < /proc/sys/$k)"; fi
done
echo "load=$(cut -d' ' -f1 /proc/loadavg)"
echo "ntp=$(timedatectl show -p NTPSynchronized --value 2>/dev/null)"
echo "addrs=$(ip -4 -o addr show | awk '{split($4,a,"/"); printf "%s ", a[1]}')"
echo "listen=$(ss -Htln | awk '{printf "%s ", $4}')"
`)
	for _, bin := range binaries {
		q := shellQuote(bin)
		fmt.Fprintf(&b, "if [ -r %s ]; then echo \"sha256=%s $(sha256sum %s | cut -d' ' -f1)\"; else echo \"sha256=%s missing\"; fi\n", q, bin, q, bin)
		fmt.Fprintf(&b, "for p in /proc/[0-9]*; do e=$(readlink \"$p/exe\" 2>/dev/null) || continue; [ \"${e%% (deleted)}\" = %s ] && echo \"stray=${p#/proc/}\"; done\n", q)
	}
	fmt.Fprintf(&b, "if [ -e %s ]; then echo rundir=exists; else echo rundir=absent; fi\n", shellQuote(runDir))
	return b.String()
}

// parseFacts reads factsScript's output.
func parseFacts(out string) hostFacts {
	f := hostFacts{Sysctl: map[string]string{}, BinarySHA256: map[string]string{}}
	for _, line := range strings.Split(out, "\n") {
		k, v, ok := strings.Cut(strings.TrimSpace(line), "=")
		if !ok {
			continue
		}
		switch {
		case k == "hostname":
			f.Hostname = v
		case k == "kernel":
			f.Kernel = v
		case k == "cpus":
			f.CPUs, _ = strconv.Atoi(v)
		case k == "mem_available_kb":
			f.MemAvailableKB, _ = strconv.ParseInt(v, 10, 64)
		case k == "nofile_soft":
			f.NofileSoft, _ = strconv.ParseInt(v, 10, 64)
		case k == "nofile_hard":
			f.NofileHard, _ = strconv.ParseInt(v, 10, 64)
		case strings.HasPrefix(k, "sysctl."):
			f.Sysctl[strings.TrimPrefix(k, "sysctl.")] = v
		case k == "load":
			f.Load1, _ = strconv.ParseFloat(v, 64)
		case k == "ntp":
			f.NTP = v
		case k == "addrs":
			f.Addrs = strings.Fields(v)
		case k == "listen":
			f.Listening = strings.Fields(v)
		case k == "sha256":
			if path, sum, ok := strings.Cut(v, " "); ok {
				f.BinarySHA256[path] = sum
			}
		case k == "stray":
			if pid, err := strconv.Atoi(v); err == nil {
				f.Strays = append(f.Strays, pid)
			}
		case k == "rundir":
			f.RunDirExists = v == "exists"
		}
	}
	if v, ok := f.Sysctl["net/netfilter/nf_conntrack_max"]; ok {
		f.ConntrackLoaded = true
		f.ConntrackMax, _ = strconv.ParseInt(v, 10, 64)
		f.ConntrackCount, _ = strconv.ParseInt(f.Sysctl["net/netfilter/nf_conntrack_count"], 10, 64)
	}
	return f
}

// scaleNeeds is what a run asks of every host, from the client count.
type scaleNeeds struct {
	Clients     int
	Nodes       int   // in the cluster
	NodeBytes   int64 // memory a client costs the cluster, its copies included
	DriverBytes int64 // memory a client costs the driver
}

const (
	scaleMinMemory   = 16 << 30
	scaleMaxLoad1    = 4
	scaleFileMargin  = 10_000
	scaleMemFactor   = 1.5
	conntrackPerConn = 4
)

// stormShare is the most clients one node holds when another is lost: its
// own and the lost node's, since a client retries the next node in its list.
func (w scaleNeeds) stormShare() int { return (2*w.Clients + w.Nodes - 1) / w.Nodes }

// hostMemoryNeed is the memory a host must have available: its nodes' share
// of the cluster's per-client cost, with one storm target's extra among them,
// and the driver's when the driver runs there.
func hostMemoryNeed(w scaleNeeds, nodesHere int, driverHere bool) int64 {
	need := 0.0
	if nodesHere > 0 {
		need += scaleMemFactor * float64(w.NodeBytes) * float64(w.Clients) * float64(nodesHere+1) / float64(w.Nodes)
	}
	if driverHere {
		need += scaleMemFactor * float64(w.DriverBytes) * float64(w.Clients)
	}
	return max(int64(need), scaleMinMemory)
}

// hostProblems names every unmet starting condition of one host with the
// value seen. nodes are the layout's nodes on this host; reference holds the
// sha256 of each binary as the test's host has it.
func hostProblems(f hostFacts, nodes []scaleNode, driverHere bool, w scaleNeeds, reference map[string]string) []string {
	var out []string
	addrs := make([]net.IP, 0, len(f.Addrs))
	for _, a := range f.Addrs {
		if ip := net.ParseIP(a); ip != nil {
			addrs = append(addrs, ip)
		}
	}
	listening := map[int]string{}
	for _, l := range f.Listening {
		if i := strings.LastIndexByte(l, ':'); i >= 0 {
			if p, err := strconv.Atoi(l[i+1:]); err == nil {
				listening[p] = l
			}
		}
	}
	for _, n := range nodes {
		if !sourcePresent(n.Address, addrs) {
			out = append(out, fmt.Sprintf("%s: address %s is not on the host", n.Name, n.Address))
		}
		for _, p := range []int{n.ClientPort, n.ClusterPort, n.ObsPort} {
			if l, ok := listening[p]; ok {
				out = append(out, fmt.Sprintf("%s: port %d is in use (%s)", n.Name, p, l))
			}
		}
		if ref, have := reference[n.Binary], f.BinarySHA256[n.Binary]; have != ref || ref == "" {
			out = append(out, fmt.Sprintf("%s: binary %s has sha256 %q here, %q on the test's host", n.Name, n.Binary, have, ref))
		}
	}
	if len(f.Strays) > 0 {
		out = append(out, fmt.Sprintf("a broker binary is already running: pid %v", f.Strays))
	}
	if f.RunDirExists {
		out = append(out, "the run's directory already exists")
	}
	if need := hostMemoryNeed(w, len(nodes), driverHere); f.MemAvailableKB*1024 < need {
		out = append(out, fmt.Sprintf("MemAvailable %d bytes, need %d", f.MemAvailableKB*1024, need))
	}
	if len(nodes) > 0 {
		if need := int64(w.stormShare() + scaleFileMargin); f.NofileHard < need {
			out = append(out, fmt.Sprintf("open-file hard limit %d, a node here may hold %d", f.NofileHard, need))
		}
	}
	if f.ConntrackLoaded {
		conns := 0
		if len(nodes) > 0 {
			conns = w.Clients * (len(nodes) + 1) / w.Nodes
		}
		if driverHere {
			conns = w.Clients
		}
		if need := int64(conntrackPerConn * conns); f.ConntrackMax < need {
			out = append(out, fmt.Sprintf("nf_conntrack_max %d is under %d (four times %d connections)", f.ConntrackMax, need, conns))
		}
	}
	if f.NTP != "yes" {
		out = append(out, fmt.Sprintf("clock not NTP-synchronized (%q)", f.NTP))
	}
	if f.Load1 > scaleMaxLoad1 {
		out = append(out, fmt.Sprintf("1-minute load %.2f, limit %d", f.Load1, scaleMaxLoad1))
	}
	return out
}

// binaryInfo is a binary's build as its Go build information records it.
type binaryInfo struct {
	Path     string `json:"path"`
	SHA256   string `json:"sha256"`
	Revision string `json:"revision"`
	Modified *bool  `json:"modified"`
	Go       string `json:"go"`
}

func readBinaryInfo(path string) (binaryInfo, error) {
	bi := binaryInfo{Path: path}
	data, err := os.ReadFile(path) //nolint:gosec // the layout's binary
	if err != nil {
		return bi, err
	}
	sum := sha256.Sum256(data)
	bi.SHA256 = hex.EncodeToString(sum[:])
	info, err := buildinfo.ReadFile(path)
	if err != nil {
		return bi, err
	}
	bi.Go = info.GoVersion
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			bi.Revision = s.Value
		case "vcs.modified":
			m := s.Value == "true"
			bi.Modified = &m
		}
	}
	return bi, nil
}

// buildProblems refuses a binary built from another commit than the test's,
// or from a modified tree.
func buildProblems(commit string, bins []binaryInfo) []string {
	var out []string
	if commit == "" {
		out = append(out, "the test's tree has no commit")
	}
	for _, b := range bins {
		switch {
		case b.Revision == "":
			out = append(out, b.Path+": its build records no commit")
		case b.Revision != commit:
			out = append(out, fmt.Sprintf("%s was built from %s, the test from %s", b.Path, b.Revision, commit))
		case b.Modified == nil || *b.Modified:
			out = append(out, b.Path+" was built from a modified tree")
		}
	}
	return out
}

// nodeConfig is a node's configuration: the broker's defaults with only its
// name, data directory, listeners, cluster addresses and certificates (mtls,
// the default mode for a node with peers), observability address and the
// module that serves it, anonymous clients allowed every topic (a broker
// refuses every subscribe until an authorizer allows it) and the JSON log
// format set.
func nodeConfig(n scaleNode, all []scaleNode, dir string) ([]byte, error) {
	var peers []map[string]string
	for _, p := range all {
		if p.Name != n.Name {
			peers = append(peers, map[string]string{"name": p.Name, "address": p.clusterAddr()})
		}
	}
	obsIP := net.ParseIP(n.ObsAddress)
	if obsIP == nil {
		obsIP = net.ParseIP(n.Address)
	}
	doc := map[string]any{
		"version":   1,
		"node":      map[string]any{"name": n.Name, "dataDir": dir + "/data"},
		"listeners": []map[string]any{{"name": "plain", "type": "tcp", "address": n.clientAddr()}},
		"cluster": map[string]any{"listen": n.clusterAddr(), "advertise": n.clientAddr(), "peers": peers,
			"discovery": map[string]any{"mode": "static"},
			"auth":      map[string]any{"mode": "mtls", "tls": map[string]any{"certFile": dir + "/node.pem", "keyFile": dir + "/node-key.pem", "caFile": dir + "/ca.pem"}}},
		"observability": map[string]any{"address": n.obsAddr(), "allowRemote": obsIP != nil && !obsIP.IsLoopback()},
		"auth":          map[string]any{"anonymous": true},
		"log":           map[string]any{"format": "json"},
		"modules": map[string]any{"metrics.prometheus": map[string]any{},
			"authz.file": map[string]any{"path": dir + "/acl.json"}},
	}
	return json.MarshalIndent(doc, "", "  ")
}

// nodeProc is a running node as the harness knows it; files are written
// beside its configuration at every launch (its certificates).
type nodeProc struct {
	node  scaleNode
	sh    hostShell
	dir   string
	files map[string][]byte
	rec   procRecord
	// down is set when the storm's SIGKILL is sent and cleared by the next
	// launch: the one stretch in which the run expects the node gone.
	down bool
}

// launchScript writes the node's configuration and files and starts it
// detached, pinned to its cores, printing the PID; the PID is also written
// beside the configuration for the sampler.
// The broker reads every HOARDMQ_ variable as an override of its
// configuration and refuses one it does not know, so the measurement's own
// settings are unset first.
func launchScript(n scaleNode, dir string, cfg []byte, files map[string][]byte) string {
	d := shellQuote(dir)
	var w strings.Builder
	for _, name := range sortedNames(files) {
		fmt.Fprintf(&w, "echo %s | base64 -d > %s/%s\nchmod 600 %s/%s\n", base64.StdEncoding.EncodeToString(files[name]), d, name, d, name)
	}
	return fmt.Sprintf(`set -e
umask 077
mkdir -p %[1]s/data
echo %[2]s | base64 -d > %[1]s/config.json
%[5]scd %[1]s
for v in $(compgen -e | grep '^HOARDMQ_' || true); do unset "$v"; done
nohup setsid taskset -c %[3]s %[4]s serve -config %[1]s/config.json >> %[1]s/node.log 2>&1 < /dev/null &
echo $! > %[1]s/pid
echo $!
`, d, base64.StdEncoding.EncodeToString(cfg), shellQuote(n.Cores), shellQuote(n.Binary), w.String())
}

// observeScript prints a process's start time (field 22 of its stat, read
// after the command name, which may hold spaces) and executable.
func observeScript(pid int) string {
	return fmt.Sprintf(`s=$(cat /proc/%[1]d/stat 2>/dev/null) || { echo gone; exit 0; }
echo "start=$(echo "${s##*) }" | cut -d' ' -f20)"
echo "exe=$(readlink /proc/%[1]d/exe)"
`, pid)
}

func parseObserved(pid int, out string) (procRecord, bool) {
	r := procRecord{PID: pid}
	if strings.TrimSpace(out) == "gone" {
		return r, false
	}
	for _, line := range strings.Split(out, "\n") {
		k, v, _ := strings.Cut(strings.TrimSpace(line), "=")
		switch k {
		case "start":
			r.StartTicks, _ = strconv.ParseUint(v, 10, 64)
		case "exe":
			r.Exe = v
		}
	}
	return r, r.StartTicks != 0
}

func observeProc(ctx context.Context, sh hostShell, pid int) (procRecord, bool, error) {
	out, err := runScript(ctx, sh, observeScript(pid))
	if err != nil {
		return procRecord{PID: pid}, false, err
	}
	r, alive := parseObserved(pid, out)
	return r, alive, nil
}

// launch starts the node and records it once its executable is the broker's.
func (p *nodeProc) launch(ctx context.Context, all []scaleNode) error {
	cfg, err := nodeConfig(p.node, all, p.dir)
	if err != nil {
		return err
	}
	out, err := runScript(ctx, p.sh, launchScript(p.node, p.dir, cfg, p.files))
	if err != nil {
		return fmt.Errorf("%s: launch: %w", p.node.Name, err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(out))
	if err != nil {
		return fmt.Errorf("%s: launch printed %q", p.node.Name, out)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		r, alive, err := observeProc(ctx, p.sh, pid)
		if err != nil {
			return err
		}
		if !alive {
			return fmt.Errorf("%s: pid %d exited at start; its log is %s/node.log", p.node.Name, pid, p.dir)
		}
		if r.Exe == p.node.Binary {
			p.rec, p.down = r, false
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s: pid %d still runs %q, not the broker", p.node.Name, pid, r.Exe)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// signal sends sig to the recorded process, after killAllowed has approved
// what is running under its PID now. It reports whether the process was
// there to signal.
func (p *nodeProc) signal(ctx context.Context, sig string) (bool, error) {
	if p.rec.PID == 0 {
		return false, nil
	}
	now, alive, err := observeProc(ctx, p.sh, p.rec.PID)
	if err != nil {
		return false, err
	}
	if !alive {
		return false, nil
	}
	if !killAllowed(p.rec, now) {
		return false, fmt.Errorf("%s: pid %d is not the process launched (recorded %+v, now %+v)", p.node.Name, p.rec.PID, p.rec, now)
	}
	_, err = runScript(ctx, p.sh, fmt.Sprintf("kill -s %s %d", sig, p.rec.PID))
	return err == nil, err
}

// stop ends the node: SIGTERM, then SIGKILL past grace. It reports whether
// the recorded process was running to be stopped.
func (p *nodeProc) stop(ctx context.Context, grace time.Duration) (bool, error) {
	sent, err := p.signal(ctx, "TERM")
	if err != nil || !sent {
		return false, err
	}
	deadline := time.Now().Add(grace)
	for time.Now().Before(deadline) {
		if _, alive, err := observeProc(ctx, p.sh, p.rec.PID); err == nil && !alive {
			return true, nil
		}
		time.Sleep(time.Second)
	}
	_, err = p.signal(ctx, "KILL")
	return true, err
}

// procGone decides whether a recorded process has gone, from a reading of
// its PID now: exited, or the PID reused by another start or executable.
func procGone(recorded, now procRecord, alive bool) bool {
	return !alive || now.StartTicks != recorded.StartTicks || now.Exe != recorded.Exe
}

// waitGone waits, bounded, until the recorded process has exited.
func (p *nodeProc) waitGone(ctx context.Context, bound time.Duration) bool {
	deadline := time.Now().Add(bound)
	for time.Now().Before(deadline) {
		if _, alive, err := observeProc(ctx, p.sh, p.rec.PID); err == nil && !alive {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// removeDir removes a run directory on a host after removeAllowed approves
// the path and its resolved form.
func removeDir(ctx context.Context, sh hostShell, path, runRoot, runID string) error {
	out, err := runScript(ctx, sh, "realpath -e -- "+shellQuote(path)+" 2>/dev/null || true")
	if err != nil {
		return err
	}
	resolved := strings.TrimSpace(out)
	if resolved == "" {
		return nil
	}
	if !removeAllowed(path, resolved, runRoot, runID) {
		return fmt.Errorf("refused to remove %s (resolved %s) under %s for %s", path, resolved, runRoot, runID)
	}
	_, err = runScript(ctx, sh, "rm -rf --one-file-system -- "+shellQuote(path))
	return err
}

// hostSample is one reading of a broker host.
type hostSample struct {
	AtMs       int64            `json:"at_ms"`
	MemAvailKB int64            `json:"mem_available_kb"`
	TCPInUse   int64            `json:"tcp_inuse"`
	TCPTW      int64            `json:"tcp_tw"`
	Conntrack  int64            `json:"conntrack"`
	RSSKB      map[string]int64 `json:"rss_kb"`
	CPUTicks   map[string]int64 `json:"cpu_ticks"`
	// Gone names a node whose process could not be read: exited, never a
	// zero.
	Gone []string `json:"gone,omitempty"`
	// Bytes are the interface counters: lo received and sent, then every
	// other interface's received and sent.
	Bytes [4]int64 `json:"bytes"`
}

// samplerScript prints a reading every interval until killed: the time, the
// host's memory, sockets and connection tracking, its interfaces' byte
// counters, and each node's resident memory and CPU ticks, read from the pid
// files the launches wrote.
func samplerScript(runDir string, interval time.Duration) string {
	return fmt.Sprintf(`while :; do
  l="S $(date +%%s%%3N) mem=$(awk '/^MemAvailable:/{print $2}' /proc/meminfo)"
  l="$l net=$(awk 'NR>2{gsub(":"," "); if($1=="lo"){lr+=$2; lt+=$10} else {nr+=$2; nt+=$10}} END{printf "%%d,%%d,%%d,%%d", lr,lt,nr,nt}' /proc/net/dev)"
  l="$l tcp=$(awk '/^TCP:/{print $3","$7}' /proc/net/sockstat)"
  l="$l ct=$(cat /proc/sys/net/netfilter/nf_conntrack_count 2>/dev/null || echo -)"
  for f in %[1]s/*/pid; do
    [ -r "$f" ] || continue
    n=$(basename "$(dirname "$f")"); p=$(cat "$f")
    r=$(awk '/^VmRSS:/{print $2}' /proc/$p/status 2>/dev/null)
    t=$(awk '{s=$0; sub(/.*\) /,"",s); split(s,a," "); print a[12]+a[13]}' /proc/$p/stat 2>/dev/null)
    l="$l $n=${r:--},${t:--}"
  done
  echo "$l"
  sleep %[2]s
done
`, shellQuote(runDir), strconv.FormatFloat(interval.Seconds(), 'f', 3, 64))
}

// parseSample reads one sampler line; ok is false for any other line.
func parseSample(line string) (hostSample, bool) {
	f := strings.Fields(line)
	if len(f) < 2 || f[0] != "S" {
		return hostSample{}, false
	}
	s := hostSample{RSSKB: map[string]int64{}, CPUTicks: map[string]int64{}, Conntrack: -1}
	var err error
	if s.AtMs, err = strconv.ParseInt(f[1], 10, 64); err != nil {
		return hostSample{}, false
	}
	for _, kv := range f[2:] {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			continue
		}
		parts := strings.Split(v, ",")
		num := func(i int) int64 {
			if i >= len(parts) {
				return -1
			}
			n, err := strconv.ParseInt(parts[i], 10, 64)
			if err != nil {
				return -1
			}
			return n
		}
		switch k {
		case "mem":
			s.MemAvailKB = num(0)
		case "net":
			s.Bytes = [4]int64{num(0), num(1), num(2), num(3)}
		case "tcp":
			s.TCPInUse, s.TCPTW = num(0), num(1)
		case "ct":
			s.Conntrack = num(0)
		default:
			rss, ticks := num(0), num(1)
			if rss < 0 {
				s.Gone = append(s.Gone, k)
				continue
			}
			s.RSSKB[k], s.CPUTicks[k] = rss, ticks
		}
	}
	return s, true
}

// hostSampler streams one host's readings into a series.
type hostSampler struct {
	host string
	cmd  *exec.Cmd
	mu   sync.Mutex
	got  []hostSample
	bad  int
	done chan struct{}
}

func startSampler(ctx context.Context, sh hostShell, runDir string, interval time.Duration) (*hostSampler, error) {
	cmd := sh.command(ctx, samplerScript(runDir, interval))
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	s := &hostSampler{host: sh.label(), cmd: cmd, done: make(chan struct{})}
	go func() {
		defer close(s.done)
		sc := bufio.NewScanner(out)
		sc.Buffer(make([]byte, 64<<10), 1<<20)
		for sc.Scan() {
			smp, ok := parseSample(sc.Text())
			s.mu.Lock()
			if ok {
				s.got = append(s.got, smp)
			} else {
				s.bad++
			}
			s.mu.Unlock()
		}
	}()
	return s, nil
}

func (s *hostSampler) series() []hostSample {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]hostSample(nil), s.got...)
}

func (s *hostSampler) latest() (hostSample, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.got) == 0 {
		return hostSample{}, false
	}
	return s.got[len(s.got)-1], true
}

func (s *hostSampler) stop() {
	if s.cmd.Process != nil {
		_ = s.cmd.Process.Kill()
	}
	<-s.done
	_ = s.cmd.Wait()
}

// The figures read from a node's /metrics.
const (
	mSessions  = "hoardmq_sessions_active"
	mReplicas  = "hoardmq_replicas_held"
	mConns     = "hoardmq_connections_active"
	mShort     = "hoardmq_replication_sessions_short"
	mQueued    = "hoardmq_overload_indicator:queued_messages"
	mHeap      = "hoardmq_overload_indicator:heap_bytes"
	mCheckpts  = "hoardmq_journal_checkpoints_total"
	mLeader    = "hoardmq_raft_leader"
	mRaftApply = "hoardmq_raft_applied_index"
)

var indicatorNameRE = regexp.MustCompile(`name="([^"]+)"`)

var scrapedNames = map[string]bool{mSessions: true, mReplicas: true, mConns: true, mShort: true, mCheckpts: true,
	mLeader: true, mRaftApply: true, "hoardmq_overload_indicator": true}

// parseMetrics sums each wanted family over its label sets; the overload
// indicator is kept per indicator name.
func parseMetrics(body string) map[string]float64 {
	out := map[string]float64{}
	for _, line := range strings.Split(body, "\n") {
		if line == "" || line[0] == '#' {
			continue
		}
		sp := strings.LastIndexByte(line, ' ')
		if sp < 0 {
			continue
		}
		v, err := strconv.ParseFloat(line[sp+1:], 64)
		if err != nil {
			continue
		}
		name, labels := line[:sp], ""
		if i := strings.IndexByte(name, '{'); i >= 0 {
			name, labels = name[:i], name[i:]
		}
		if !scrapedNames[name] {
			continue
		}
		if name == "hoardmq_overload_indicator" {
			m := indicatorNameRE.FindStringSubmatch(labels)
			if m == nil {
				continue
			}
			name += ":" + m[1]
		}
		out[name] += v
	}
	return out
}

// nodeReading is one scrape of a node.
type nodeReading struct {
	AtMs   int64              `json:"at_ms"`
	Node   string             `json:"node"`
	TookMs int64              `json:"took_ms"`
	Values map[string]float64 `json:"values,omitempty"`
	Error  string             `json:"error,omitempty"`
}

var scaleHTTP = &http.Client{Timeout: 60 * time.Second}

func scrape(ctx context.Context, n scaleNode) nodeReading {
	start := time.Now()
	r := nodeReading{AtMs: start.UnixMilli(), Node: n.Name}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+n.obsAddr()+"/metrics", nil)
	if err != nil {
		r.Error = err.Error()
		return r
	}
	resp, err := scaleHTTP.Do(req)
	if err != nil {
		r.Error = err.Error()
		r.TookMs = time.Since(start).Milliseconds()
		return r
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	r.TookMs = time.Since(start).Milliseconds()
	if err != nil {
		r.Error = err.Error()
		return r
	}
	r.Values = parseMetrics(string(body))
	return r
}

// probeHealth reports whether one of a node's health endpoints answers 200.
func probeHealth(ctx context.Context, n scaleNode, path string) bool {
	c, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(c, http.MethodGet, "http://"+n.obsAddr()+path, nil)
	if err != nil {
		return false
	}
	resp, err := scaleHTTP.Do(req)
	if err != nil {
		return false
	}
	_ = resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// timedValue is one reading of a figure at a time.
type timedValue struct {
	At time.Time
	V  float64
}

// settledAt returns the start of the first interval of hold over which the
// value did not change, the series' last reading reaching the interval's
// end; ok is false when there is none yet.
func settledAt(series []timedValue, hold time.Duration) (time.Time, bool) {
	for i := 0; i < len(series); i++ {
		for j := i + 1; j < len(series); j++ {
			if series[j].V != series[i].V {
				break
			}
			if series[j].At.Sub(series[i].At) >= hold {
				return series[i].At, true
			}
		}
	}
	return time.Time{}, false
}

// rejoinReading is one reading during a rejoin: whether the restarted node
// was ready and serving, the copies it held, and the most short sessions on
// any node.
type rejoinReading struct {
	At       time.Time
	Ready    bool
	Serving  bool
	Replicas float64
	Short    float64
}

// rejoinedAt decides the rejoin: the start of the first interval of hold in
// which the node was ready and serving, held more than zero copies, the same
// number throughout, with no node short of copies at its start.
func rejoinedAt(series []rejoinReading, hold time.Duration) (time.Time, bool) {
	for i, r := range series {
		if !r.Ready || !r.Serving || r.Replicas <= 0 || r.Short != 0 {
			continue
		}
		for j := i + 1; j < len(series); j++ {
			s := series[j]
			if !s.Ready || !s.Serving || s.Replicas != r.Replicas {
				break
			}
			if s.At.Sub(r.At) >= hold {
				return r.At, true
			}
		}
	}
	return time.Time{}, false
}

// rssDelta is resident memory per unit: the sum of after minus before over
// the nodes named, divided by units.
func rssDelta(before, after map[string]int64, units float64) float64 {
	if units <= 0 {
		return 0
	}
	var d int64
	for n, a := range after {
		if b, ok := before[n]; ok {
			d += a - b
		}
	}
	return float64(d) * 1024 / units
}

// nodeRSS merges the latest sample of every host into one node → KiB map.
func nodeRSS(samplers []*hostSampler) map[string]int64 {
	out := map[string]int64{}
	for _, s := range samplers {
		if smp, ok := s.latest(); ok {
			for n, v := range smp.RSSKB {
				out[n] = v
			}
		}
	}
	return out
}

var errScaleRefused = errors.New("refused to start")
