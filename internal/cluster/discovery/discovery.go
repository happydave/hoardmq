// Package discovery derives the peer set from the configuration: the static
// list, or the ordinals of a StatefulSet behind a headless Service.
package discovery

import (
	"fmt"

	"github.com/happydave/hoardmq/internal/config"
)

// Peers returns the peers this node should know, excluding itself. Address
// resolution is left to the transport, which retries a name that does not
// resolve yet: a DNS miss is not absence.
func Peers(doc config.Document) ([]config.Peer, error) {
	c := doc.Cluster
	switch c.Discovery.Mode {
	case "", "static":
		return append([]config.Peer(nil), c.Peers...), nil
	case "dns":
		d := c.Discovery.DNS
		if d == nil {
			return nil, fmt.Errorf("discovery: dns settings missing")
		}
		self, err := config.Ordinal(d.Name, doc.Node.Name)
		if err != nil {
			return nil, err
		}
		peers := make([]config.Peer, 0, d.Replicas)
		for i := 0; i < d.Replicas; i++ {
			if i == self {
				continue
			}
			name := fmt.Sprintf("%s-%d", d.Name, i)
			peers = append(peers, config.Peer{Name: name, Address: fmt.Sprintf("%s.%s:%d", name, d.Service, d.Port)})
		}
		return peers, nil
	default:
		return nil, fmt.Errorf("discovery: unknown mode %q", c.Discovery.Mode)
	}
}
