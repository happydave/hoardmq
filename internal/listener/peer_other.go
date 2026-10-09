//go:build !linux

package listener

import "net"

// peerGone reports every peer present where the check is not implemented.
func peerGone(net.Conn) bool { return false }
