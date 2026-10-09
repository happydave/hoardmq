// Package transport is the cluster transport: one authenticated, framed,
// multiplexed TCP connection per peer pair, negotiated to a common framing
// version, carrying the cluster protocol's message classes in the order they
// were admitted, with per-peer heartbeats that carry a stress level.
package transport

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Class identifies what a frame carries. The set is closed: an unknown class
// is a protocol error. Classes 0 and 1 are the handshake and are frozen.
type Class uint8

// The message classes of the cluster protocol.
const (
	classHello Class = iota
	classAuth
	Consensus
	ReplicationAppend
	ReplicationAck
	ForwardedPublish
	RouteUpdate
	LeaseNotification
	TakeoverRequest
	TakeoverAck
	DrainAnnouncement
	RedirectHint
	Heartbeat
	// LeaseAlive carries a node's lease renewal and its answer on the urgent
	// lane: queued ahead of every other class so a renewal never waits behind
	// replication traffic (WI 1337). It needs framing Version2 on the link.
	LeaseAlive
	// ConnectionRole is the first frame a version-3 dialer sends after the
	// handshake: which of the peer pair's two connections this one is (WI
	// 1340). ConsensusSnapshot carries a raft snapshot, which is bulk and so
	// travels on the data connection rather than with the other raft
	// messages on the control connection.
	ConnectionRole
	ConsensusSnapshot
	// AdminRequest carries an admin API call to the node that holds the
	// client it acts on, and AdminAnswer its answer. They need framing
	// Version4 on the link.
	AdminRequest
	AdminAnswer
	classCount
)

var classNames = [...]string{
	"hello", "auth", "consensus", "replication-append", "replication-ack",
	"forwarded-publish", "route-update", "lease-notification", "takeover-request",
	"takeover-ack", "drain-announcement", "redirect-hint", "heartbeat", "lease-alive",
	"connection-role", "consensus-snapshot", "admin-request", "admin-answer",
}

// Role is which of a peer pair's connections a link is: control carries
// the liveness and coordination classes, data the bulk classes. A link to
// a peer negotiated below version 3 is both.
type Role uint8

// Connection roles.
const (
	RoleControl Role = iota
	RoleData
	roleCount
)

func (r Role) String() string {
	if r == RoleData {
		return "data"
	}
	return "control"
}

// lane is the connection a class travels on between version-3 peers. It is
// a fixed table: callers do not choose.
func (c Class) lane() Role {
	switch c {
	case Consensus, LeaseNotification, TakeoverRequest, DrainAnnouncement, RedirectHint, Heartbeat, LeaseAlive, ConnectionRole:
		return RoleControl
	}
	return RoleData
}

func (c Class) String() string {
	if int(c) < len(classNames) {
		return classNames[c]
	}
	return fmt.Sprintf("class(%d)", uint8(c))
}

// payloadClass reports whether the class may appear after the handshake.
func (c Class) payloadClass() bool { return c >= Consensus && c < classCount }

// minVersion is the framing version a class first appears in; a class sent
// on a link negotiated below it is a protocol error on the receiving side
// and ErrUnsupportedClass on the sending side.
func (c Class) minVersion() uint8 {
	switch c {
	case LeaseAlive:
		return Version2
	case ConnectionRole, ConsensusSnapshot:
		return Version3
	case AdminRequest, AdminAnswer:
		return Version4
	}
	return Version1
}

// urgent reports whether the class travels on the link's urgent lane, ahead
// of the ordered stream: the liveness signals. A heartbeat queued behind a
// storm's replication frames would have the peer declare this node silent
// exactly when the link is busiest.
func (c Class) urgent() bool { return c == LeaseAlive || c == Heartbeat }

// Framing versions. The handshake advertises a range and peers use the
// highest version both support. Version2 adds the LeaseAlive class;
// Version3 adds the second connection per peer pair (ConnectionRole) and
// the ConsensusSnapshot class; Version4 adds AdminRequest and AdminAnswer.
// The frame encoding is unchanged throughout.
const (
	Version1 uint8 = 1
	Version2 uint8 = 2
	Version3 uint8 = 3
	Version4 uint8 = 4
)

// headerSize is the fixed part of every frame: a 4-byte big-endian length of
// what follows it, one class byte and one flags byte.
const headerSize = 6

// ErrProtocol is the class of error that closes a connection: a frame the
// peer must never have sent.
var ErrProtocol = errors.New("protocol error")

type frame struct {
	class   Class
	payload []byte
}

// writeFrame encodes one frame. Flags are always zero in version 1.
func writeFrame(w *bufio.Writer, f frame) error {
	var hdr [headerSize]byte
	binary.BigEndian.PutUint32(hdr[:4], uint32(len(f.payload)+2))
	hdr[4] = byte(f.class)
	hdr[5] = 0
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(f.payload)
	return err
}

// readFrame decodes one frame, refusing lengths above maxFrame, non-zero
// flags and, when the handshake is over, classes outside the payload set.
func readFrame(r io.Reader, maxFrame int, handshake bool) (frame, error) {
	var hdr [headerSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return frame{}, err
	}
	n := binary.BigEndian.Uint32(hdr[:4])
	if n < 2 {
		return frame{}, fmt.Errorf("%w: length %d below the header", ErrProtocol, n)
	}
	if int(n-2) > maxFrame {
		return frame{}, fmt.Errorf("%w: frame of %d bytes exceeds the limit of %d", ErrProtocol, n-2, maxFrame)
	}
	if hdr[5] != 0 {
		return frame{}, fmt.Errorf("%w: flags 0x%02x are reserved", ErrProtocol, hdr[5])
	}
	c := Class(hdr[4])
	if handshake {
		if c != classHello && c != classAuth {
			return frame{}, fmt.Errorf("%w: %s before the handshake completed", ErrProtocol, c)
		}
	} else if !c.payloadClass() {
		return frame{}, fmt.Errorf("%w: unknown or handshake class %d after the handshake", ErrProtocol, uint8(c))
	}
	payload := make([]byte, n-2)
	if _, err := io.ReadFull(r, payload); err != nil {
		return frame{}, err
	}
	return frame{class: c, payload: payload}, nil
}
