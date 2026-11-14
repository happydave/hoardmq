package sim

import (
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

// A client's last CONNACKs, kept in memory for the whole run, so that a client
// a run names afterwards (served empty by a node, lost by the delivery check,
// corrected by a node's route reconciler) can be read against what it was told
// and when. A broker's log says what it answered; only the client says whether
// the answer arrived.

// connackRing is how many of its latest CONNACKs a client keeps.
const connackRing = 4

// connackRec is one connect's outcome. Fixed-size and pointer-free, so a
// million clients' rings cost a known, small amount of memory and nothing for
// the garbage collector to trace.
type connackRec struct {
	atNs   int64  // when the CONNACK was read, or the handshake gave up
	tookMs uint32 // from the start of the dial to then
	node   uint8  // index into the client's nodes, connackNodeOther when not among them
	code   uint8  // the CONNACK's reason code; for no CONNACK, the index of its failure class
	flags  uint8
}

const (
	connackPresent   uint8 = 1    // the CONNACK's Session Present
	connackNodeOther uint8 = 0xff // a node outside the client's list, reached by a redirect
)

// connackLog is one client's record: its latest CONNACKs, accepted or refused,
// and the latest connect that reached the handshake and read no CONNACK — a
// CONNECT the broker may have answered after the client stopped listening.
type connackLog struct {
	last   [connackRing]connackRec
	noAck  connackRec
	seen   uint32 // CONNACKs read; last holds the latest min(seen, connackRing)
	noAcks uint32 // handshakes that read no CONNACK
}

func (l *connackLog) add(r connackRec) {
	l.last[l.seen%connackRing] = r
	l.seen++
}

func (l *connackLog) addNoAck(r connackRec) {
	l.noAck = r
	l.noAcks++
}

// nodeIndex is addr's position in the client's nodes, which never change
// after NewClient.
func (c *Client) nodeIndex(addr string) uint8 {
	for i, n := range c.nodes {
		if n == addr && i < int(connackNodeOther) {
			return uint8(i)
		}
	}
	return connackNodeOther
}

// noteConnect keeps the outcome of one connect to addr begun at began: the
// CONNACK it read, or, for a connect that failed after its dial, that it read
// none. A failed dial sent no CONNECT and is not kept.
func (c *Client) noteConnect(addr string, began time.Time, ack *packet.Connack, err error) {
	now := time.Now()
	r := connackRec{atNs: now.UnixNano(), tookMs: uint32(min(now.Sub(began).Milliseconds(), math.MaxUint32)), node: c.nodeIndex(addr)}
	if ack == nil {
		var ce *connectError
		if !errors.As(err, &ce) || ce.stage != "handshake" {
			return
		}
		r.code = uint8(failureIndex(err))
	} else {
		r.code = uint8(ack.ReasonCode)
		if ack.SessionPresent {
			r.flags |= connackPresent
		}
	}
	c.sumMu.Lock()
	if ack == nil {
		c.acks.addNoAck(r)
	} else {
		c.acks.add(r)
	}
	c.sumMu.Unlock()
}

// connacks returns a copy of the client's record.
func (c *Client) connacks() connackLog {
	c.sumMu.Lock()
	defer c.sumMu.Unlock()
	return c.acks
}

// failureIndex is the position of err's class in failureClasses.
func failureIndex(err error) int {
	class := classifyFailure(err)
	for i, c := range failureClasses {
		if c == class {
			return i
		}
	}
	return len(failureClasses) - 1
}

// connackEntry is one kept outcome as a run records it. Node is the address;
// NodeName and SinceCrashSec are the test's, which knows the layout and the
// crash.
type connackEntry struct {
	AtMs           int64    `json:"at_ms"`
	TookMs         uint32   `json:"took_ms"`
	Node           string   `json:"node"`
	NodeName       string   `json:"node_name,omitempty"`
	SinceCrashSec  *float64 `json:"since_crash_sec,omitempty"`
	Code           string   `json:"code,omitempty"`
	SessionPresent bool     `json:"session_present"`
	// NoConnack is, for a handshake that read no CONNACK, the class it
	// failed with.
	NoConnack string `json:"no_connack,omitempty"`
}

// clientConnacks is one named client's record, its CONNACKs oldest first.
type clientConnacks struct {
	ID            string         `json:"id"`
	Found         bool           `json:"found"`
	Seen          uint32         `json:"connacks_seen"`
	Last          []connackEntry `json:"last_connacks"`
	NoConnacks    uint32         `json:"no_connack_seen"`
	LastNoConnack *connackEntry  `json:"last_no_connack,omitempty"`
	// Why is every reason the run named the client, filled in by the test.
	Why []string `json:"why,omitempty"`
}

func (l connackLog) entry(r connackRec, nodes []string) connackEntry {
	e := connackEntry{AtMs: r.atNs / int64(time.Millisecond), TookMs: r.tookMs, Node: "other"}
	if int(r.node) < len(nodes) {
		e.Node = nodes[r.node]
	}
	return e
}

// record is the client's record for the run: the kept CONNACKs, oldest
// first, and the last handshake that read none.
func (l connackLog) record(id string, nodes []string) clientConnacks {
	out := clientConnacks{ID: id, Found: true, Seen: l.seen, NoConnacks: l.noAcks, Last: make([]connackEntry, 0, connackRing)}
	n := min(l.seen, connackRing)
	for i := l.seen - n; i < l.seen; i++ {
		r := l.last[i%connackRing]
		e := l.entry(r, nodes)
		e.Code = ackName(packet.ReasonCode(r.code))
		e.SessionPresent = r.flags&connackPresent != 0
		out.Last = append(out.Last, e)
	}
	if l.noAcks > 0 {
		e := l.entry(l.noAck, nodes)
		e.NoConnack = failureClasses[min(int(l.noAck.code), len(failureClasses)-1)]
		out.LastNoConnack = &e
	}
	return out
}

// connackRecords reads the named clients' records, in the order asked; a
// name no client carries is returned not found.
func connackRecords(all []*Client, ids []string) []clientConnacks {
	want := make(map[string]int, len(ids))
	for i, id := range ids {
		want[id] = i
	}
	out := make([]clientConnacks, len(ids))
	for i, id := range ids {
		out[i] = clientConnacks{ID: id, Last: make([]connackEntry, 0)}
	}
	for _, cl := range all {
		if i, ok := want[cl.ID]; ok {
			out[i] = cl.connacks().record(cl.ID, cl.nodes)
		}
	}
	return out
}

func (p *localPopulation) connacks(ids []string) ([]clientConnacks, error) {
	return connackRecords(p.clientsSnapshot(), ids), nil
}

// The node log lines that name a client a run reads the record of: a session
// served empty, and the route reconciler's corrections and the connected
// subscriptions it refused to remove. Each is a Warn with a "client" field.
const (
	logServedEmpty     = "session served empty although its lease says one existed"
	logReconcileFixed  = "route reconciler corrected a difference seen at two passes"
	logReconcileRefuse = "route reconciler left a connected client's subscriber that no map holds"
)

// namedLogMax bounds the lines one node's log may name clients with.
const namedLogMax = 2000

// namedLogScript prints the lines of a node's log naming a client, at most
// namedLogMax of them.
func namedLogScript(dir string) string {
	return "grep -m " + strconv.Itoa(namedLogMax) + " -F -e " + shellQuote(`"msg":"`+logServedEmpty+`"`) +
		" -e " + shellQuote(`"msg":"`+logReconcileFixed+`"`) + " -e " + shellQuote(`"msg":"`+logReconcileRefuse+`"`) +
		" " + shellQuote(dir+"/node.log") + " || true"
}

// logNamedClients reads, from one node's matching log lines, each client they
// name and why, in the order the lines came. A line that is not one of the
// three, or names no client, is skipped.
func logNamedClients(node, lines string) [][2]string {
	var out [][2]string
	for _, line := range strings.Split(lines, "\n") {
		var l struct {
			Msg       string `json:"msg"`
			Client    string `json:"client"`
			Reason    string `json:"reason"`
			Kind      string `json:"kind"`
			Direction string `json:"direction"`
		}
		if json.Unmarshal([]byte(line), &l) != nil || l.Client == "" {
			continue
		}
		var why string
		switch l.Msg {
		case logServedEmpty:
			why = node + " served it empty (" + l.Reason + ")"
		case logReconcileFixed:
			why = node + "'s route reconciler corrected it (" + l.Direction + " " + l.Kind + ")"
		case logReconcileRefuse:
			why = node + "'s route reconciler left its connected subscriber (" + l.Kind + ")"
		default:
			continue
		}
		out = append(out, [2]string{l.Client, why})
	}
	return out
}
