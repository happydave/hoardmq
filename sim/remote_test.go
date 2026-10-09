package sim

import (
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

func TestReadRemoteSettings(t *testing.T) {
	env := func(kv map[string]string) func(string) string { return func(k string) string { return kv[k] } }
	good := map[string]string{
		clientCommandEnv: "ssh -i /home/x/.ssh/key -o BatchMode=yes notdave@192.0.2.137",
		nodeAddrsEnv:     "10.231.14.1:8001, 10.231.14.2:8001,10.231.14.3:8001",
		clientSourcesEnv: "10.231.14.11,10.231.14.12",
		clientBytesEnv:   "40000",
	}
	s, err := readRemoteSettings(env(good), 3)
	if err != nil || s == nil {
		t.Fatalf("good settings: %v, %v", s, err)
	}
	if len(s.Nodes) != 3 || len(s.Sources) != 2 || s.BytesPerClient != 40000 || s.destination() != "notdave@192.0.2.137" {
		t.Fatalf("parsed %+v, destination %q", s, s.destination())
	}
	if s, err := readRemoteSettings(env(nil), 3); s != nil || err != nil {
		t.Fatalf("no command selects in-process: %v, %v", s, err)
	}
	for name, change := range map[string]map[string]string{
		"missing nodes":   {nodeAddrsEnv: ""},
		"two nodes":       {nodeAddrsEnv: "10.231.14.1:8001,10.231.14.2:8001"},
		"hostname node":   {nodeAddrsEnv: "ai2:8001,10.231.14.2:8001,10.231.14.3:8001"},
		"no port":         {nodeAddrsEnv: "10.231.14.1,10.231.14.2:8001,10.231.14.3:8001"},
		"duplicate node":  {nodeAddrsEnv: "10.231.14.1:8001,10.231.14.1:8001,10.231.14.3:8001"},
		"missing sources": {clientSourcesEnv: ""},
		"bad source":      {clientSourcesEnv: "gtr"},
		"bad bytes":       {clientBytesEnv: "lots"},
	} {
		kv := map[string]string{}
		for k, v := range good {
			kv[k] = v
		}
		var named string
		for k, v := range change {
			kv[k], named = v, k
		}
		if _, err := readRemoteSettings(env(kv), 3); err == nil || !strings.Contains(err.Error(), named) {
			t.Errorf("%s: error %v does not name %s", name, err, named)
		}
	}
}

func TestServerStartProblems(t *testing.T) {
	addrs := []net.IP{net.ParseIP("192.0.2.126"), net.ParseIP("10.231.14.1"), net.ParseIP("10.231.14.2"), net.ParseIP("10.231.14.3")}
	nodes := []string{"10.231.14.1:8001", "10.231.14.2:8001", "10.231.14.3:8001"}
	if p := serverStartProblems(100000, 76, 1048576, true, nodes, addrs); len(p) != 0 {
		t.Fatalf("a settled host refused: %v", p)
	}
	if p := serverStartProblems(100000, 10001, 1048576, true, nodes, addrs); len(p) != 1 || !strings.Contains(p[0], "10001") {
		t.Fatalf("a full conntrack table: %v", p)
	}
	if p := serverStartProblems(300000, 76, 1048576, true, nodes, addrs); len(p) != 1 || !strings.Contains(p[0], "nf_conntrack_max") {
		t.Fatalf("a small conntrack maximum: %v", p)
	}
	if p := serverStartProblems(100000, 0, 0, false, nodes, addrs); len(p) != 0 {
		t.Fatalf("no conntrack module is no table to fill: %v", p)
	}
	if p := serverStartProblems(100000, 76, 1048576, true, nodes, addrs[:3]); len(p) != 1 || !strings.Contains(p[0], "10.231.14.3") {
		t.Fatalf("a missing node address: %v", p)
	}
}

func TestDriverBuildProblem(t *testing.T) {
	yes, no := true, false
	tree := Tree{Commit: "abc"}
	for _, tc := range []struct {
		name   string
		record bool
		info   DriverInfo
		want   string
	}{
		{"gate run", false, DriverInfo{}, ""},
		{"same clean build", true, DriverInfo{Revision: "abc", Modified: &no}, ""},
		{"no revision", true, DriverInfo{Modified: &no}, "no commit"},
		{"other commit", true, DriverInfo{Revision: "def", Modified: &no}, "def"},
		{"modified", true, DriverInfo{Revision: "abc", Modified: &yes}, "modified"},
		{"modified unknown", true, DriverInfo{Revision: "abc"}, "modified"},
	} {
		got := driverBuildProblem(tc.record, tree, tc.info)
		if (tc.want == "") != (got == "") || !strings.Contains(got, tc.want) {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
}

// TestRemoteNodesDifferOnlyInAddress: invariant 2 of WI 1480. A node built
// for the remote-client mode has exactly the in-process node's configuration
// except for its listener and advertised addresses.
func TestRemoteNodesDifferOnlyInAddress(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "secret"), []byte("simulation-cluster-shared-secret-bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	build := func(listen []string) []config.Document {
		o := Options{Nodes: 3, Replicas: 2, Shards: 16, TTL: 30 * time.Second, Timings: TimingsProduct, ListenAddrs: listen}
		c := &Cluster{t: t, o: o, dir: dir, secret: filepath.Join(dir, "secret"), AckTimeout: time.Second, TakeoverTimeout: time.Second}
		for i, name := range []string{"n1", "n2", "n3"} {
			tcp := "127.0.0.1:" + []string{"1101", "1102", "1103"}[i]
			if listen != nil {
				tcp = listen[i]
			}
			c.Nodes = append(c.Nodes, &Node{Name: name, Dir: filepath.Join(dir, name), c: c, TCP: tcp,
				Cluster: "127.0.0.1:" + []string{"1201", "1202", "1203"}[i], Obs: "127.0.0.1:" + []string{"1301", "1302", "1303"}[i]})
		}
		var docs []config.Document
		for _, n := range c.Nodes {
			docs = append(docs, c.config(n))
		}
		return docs
	}
	local := build(nil)
	remote := build([]string{"10.231.14.1:8001", "10.231.14.2:8001", "10.231.14.3:8001"})
	for i := range local {
		if remote[i].Listeners[0].Address == local[i].Listeners[0].Address || remote[i].Cluster.Advertise != remote[i].Listeners[0].Address {
			t.Fatalf("node %d: remote listener %q, advertise %q", i, remote[i].Listeners[0].Address, remote[i].Cluster.Advertise)
		}
		for _, d := range []*config.Document{&local[i], &remote[i]} {
			for j := range d.Listeners {
				d.Listeners[j].Address = ""
			}
			d.Cluster.Advertise = ""
		}
		if !reflect.DeepEqual(local[i], remote[i]) {
			t.Fatalf("node %d: configurations differ beyond the addresses:\nlocal  %+v\nremote %+v", i, local[i], remote[i])
		}
	}
}

// TestClusterListensOnGivenAddresses: nodes given addresses listen there and
// advertise them.
func TestClusterListensOnGivenAddresses(t *testing.T) {
	var addrs []string
	for i := 0; i < 3; i++ {
		_, port, _ := net.SplitHostPort(freePort(t))
		addrs = append(addrs, net.JoinHostPort("127.0.0."+[]string{"21", "22", "23"}[i], port))
	}
	c := New(t, Options{ListenAddrs: addrs})
	if got := c.Addresses(); !reflect.DeepEqual(got, addrs) {
		t.Fatalf("addresses %v, want %v", got, addrs)
	}
	for _, a := range addrs {
		code, _, err := ProbeConnect(a, "probe-"+a)
		if err != nil || code != packet.Success {
			t.Fatalf("%s: %v, %v", a, code, err)
		}
	}
}
