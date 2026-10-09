package discovery

import (
	"testing"

	"github.com/happydave/hoardmq/internal/config"
)

func TestStaticAndDNS(t *testing.T) {
	doc := config.Default()
	doc.Cluster.Peers = []config.Peer{{Name: "b", Address: "b:1"}}
	peers, err := Peers(doc)
	if err != nil || len(peers) != 1 || peers[0].Name != "b" {
		t.Fatalf("static: %v %v", peers, err)
	}
	doc = config.Default()
	doc.Node.Name = "hoardmq-1"
	doc.Cluster.Discovery = config.Discovery{Mode: "dns", DNS: &config.DNSDiscovery{Service: "hoardmq-headless.ns.svc", Name: "hoardmq", Replicas: 3, Port: 7000}}
	peers, err = Peers(doc)
	if err != nil {
		t.Fatal(err)
	}
	if len(peers) != 2 || peers[0].Name != "hoardmq-0" || peers[0].Address != "hoardmq-0.hoardmq-headless.ns.svc:7000" || peers[1].Name != "hoardmq-2" {
		t.Fatalf("dns peers %+v", peers)
	}
	doc.Node.Name = "other-1"
	if _, err := Peers(doc); err == nil {
		t.Fatal("a node outside the set was accepted")
	}
}
