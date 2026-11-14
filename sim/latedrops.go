package sim

import (
	"fmt"
	"strings"
	"time"
)

// A client that drops after every client was back online is a late drop: a
// storm that had recovered loses a client again, and the recovery instant
// moves past the body of the storm. The counts say how many drops there were
// and of what cause, but not which clients; each client therefore keeps its
// latest drops, and the driver names, at the storm's verdict, every client
// with a drop after the first reading that had every client back.

// dropRing is how many of its latest drops a client keeps.
const dropRing = 4

// dropRec is one connection's end as the client saw it. Fixed-size and
// pointer-free, as connackRec is.
type dropRec struct {
	atNs  int64
	node  uint8 // index into the client's nodes, connackNodeOther when not among them
	cause uint8 // index into dropCauseNames
}

// dropLog is one client's latest drops, guarded by the client's sumMu.
type dropLog struct {
	last [dropRing]dropRec
	seen uint32
}

func (l *dropLog) add(r dropRec) {
	l.last[l.seen%dropRing] = r
	l.seen++
}

// since returns the kept drops at or after sinceNs, oldest first. earlier
// reports that every kept drop is that late and the client dropped more often
// than it keeps, so drops in the window may be missing from the list.
func (l dropLog) since(sinceNs int64) (late []dropRec, earlier bool) {
	n := min(l.seen, dropRing)
	for i := l.seen - n; i < l.seen; i++ {
		if r := l.last[i%dropRing]; r.atNs >= sinceNs {
			late = append(late, r)
		}
	}
	return late, uint32(len(late)) == n && l.seen > n
}

// noteDrop keeps a drop of the connection to addr. The caller holds sumMu.
func (c *Client) noteDrop(addr string, at time.Time, cause int) {
	c.drops.add(dropRec{atNs: at.UnixNano(), node: c.nodeIndex(addr), cause: uint8(cause)})
}

// backWatch finds the first reading, after the crash, that has every client
// back. It is fed every reading from the mark on; a reading below the target
// arms it, and the first reading at the target once the crash is known and
// after one below sets the instant. Later dips do not move it.
type backWatch struct {
	down bool
	at   time.Time
}

func (w *backWatch) observe(atTarget, crashed bool, at time.Time) {
	switch {
	case !atTarget:
		w.down = true
	case crashed && w.down && w.at.IsZero():
		w.at = at
	}
}

// lateDropEntry is one drop as a run records it, dated by the test.
type lateDropEntry struct {
	AtMs          int64    `json:"at_ms"`
	Node          string   `json:"node"`
	NodeName      string   `json:"node_name,omitempty"`
	SinceCrashSec *float64 `json:"since_crash_sec,omitempty"`
	Cause         string   `json:"cause"`
}

// lateClient is one client that dropped after every client was back: its
// drops in that window, oldest first, and its CONNACK record as it stood at
// the storm's verdict.
type lateClient struct {
	ID    string          `json:"id"`
	Drops []lateDropEntry `json:"drops"`
	// EarlierNotKept is set when the client dropped more often in the window
	// than it keeps drops.
	EarlierNotKept bool           `json:"earlier_not_kept,omitempty"`
	Connacks       clientConnacks `json:"connacks"`
}

// lateDrops is the storm's late drops. SinceMs is the driver's first reading
// after the crash with every client back, 0 when there was none.
type lateDrops struct {
	SinceMs       int64            `json:"since_ms"`
	SinceCrashSec *float64         `json:"since_crash_sec,omitempty"`
	Clients       int              `json:"clients"`
	ByCause       map[string]int64 `json:"drops_by_cause"`
	Named         []lateClient     `json:"named"`
}

// lateDropMax bounds how many late-dropped clients the summary names.
const lateDropMax = 5000

// lateDropsOf names every client with a kept drop at or after since, in the
// population's order, with its drops and its CONNACK record. With since zero
// (the clients never all came back) it names none.
func lateDropsOf(all []*Client, since time.Time) *lateDrops {
	out := &lateDrops{ByCause: dropCauseMap([dropCauseCount]int64{}), Named: make([]lateClient, 0)}
	if since.IsZero() {
		return out
	}
	out.SinceMs = since.UnixMilli()
	for _, cl := range all {
		cl.sumMu.Lock()
		late, earlier := cl.drops.since(since.UnixNano())
		acks := cl.acks
		cl.sumMu.Unlock()
		if len(late) == 0 {
			continue
		}
		out.Clients++
		for _, r := range late {
			out.ByCause[dropCauseNames[r.cause]]++
		}
		if len(out.Named) == lateDropMax {
			continue
		}
		lc := lateClient{ID: cl.ID, EarlierNotKept: earlier, Connacks: acks.record(cl.ID, cl.nodes), Drops: make([]lateDropEntry, 0, len(late))}
		for _, r := range late {
			e := lateDropEntry{AtMs: r.atNs / int64(time.Millisecond), Node: "other", Cause: dropCauseNames[r.cause]}
			if int(r.node) < len(cl.nodes) {
				e.Node = cl.nodes[r.node]
			}
			lc.Drops = append(lc.Drops, e)
		}
		out.Named = append(out.Named, lc)
	}
	return out
}

// dateLateDrops names each node and dates the window and every drop and
// CONNACK against the crash.
func dateLateDrops(ld *lateDrops, names map[string]string, crashMs int64) {
	if ld == nil {
		return
	}
	if ld.SinceMs > 0 && crashMs > 0 {
		s := float64(ld.SinceMs-crashMs) / 1000
		ld.SinceCrashSec = &s
	}
	for i := range ld.Named {
		lc := &ld.Named[i]
		for j := range lc.Drops {
			d := &lc.Drops[j]
			d.NodeName = names[d.Node]
			if crashMs > 0 {
				s := float64(d.AtMs-crashMs) / 1000
				d.SinceCrashSec = &s
			}
		}
		for j := range lc.Connacks.Last {
			dateEntry(&lc.Connacks.Last[j], names, crashMs)
		}
		if lc.Connacks.LastNoConnack != nil {
			dateEntry(lc.Connacks.LastNoConnack, names, crashMs)
		}
	}
}

// causeList is a cause map's non-zero counts, by name.
func causeList(m map[string]int64) string {
	var parts []string
	for _, k := range sortedKeys(m) {
		if m[k] > 0 {
			parts = append(parts, fmt.Sprintf("%s %d", k, m[k]))
		}
	}
	return strings.Join(parts, ", ")
}

// lateDropReport is the report's account of the late drops.
func lateDropReport(ld *lateDrops) string {
	if ld == nil {
		return ""
	}
	var b strings.Builder
	b.WriteString("\n## Late drops\n\n")
	if ld.SinceMs == 0 {
		b.WriteString("Every client was never back at once, so no drop is late.\n")
		return b.String()
	}
	since := "at an unknown time"
	if ld.SinceCrashSec != nil {
		since = fmt.Sprintf("from %+.3f s", *ld.SinceCrashSec)
	}
	fmt.Fprintf(&b, "Every client was back %s; %d clients dropped after that, before the storm's verdict (drops by cause: %s).\n\n",
		since, ld.Clients, causeList(ld.ByCause))
	for i, lc := range ld.Named {
		if i == connackReportMax {
			fmt.Fprintf(&b, "- … %d more in run.json\n", len(ld.Named)-i)
			break
		}
		drops := make([]string, 0, len(lc.Drops))
		for _, d := range lc.Drops {
			drops = append(drops, fmt.Sprintf("%s %s on %s", sinceCrash(connackEntry{AtMs: d.AtMs, SinceCrashSec: d.SinceCrashSec}), d.Cause, d.NodeName))
		}
		more := ""
		if lc.EarlierNotKept {
			more = " (earlier drops not kept)"
		}
		fmt.Fprintf(&b, "- %s: dropped %s%s; %s\n", lc.ID, strings.Join(drops, "; "), more, connackRecordLine(lc.Connacks))
	}
	return b.String()
}
