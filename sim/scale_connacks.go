package sim

import (
	"context"
	"fmt"
	"strings"
)

// scaleConnacks is the CONNACK records of every client the run named, read
// at teardown while the clients still hold them.
type scaleConnacks struct {
	Asked   int              `json:"asked"`
	Clients []clientConnacks `json:"clients"`
	// LogLines is, per node, the lines of its log that named a client, at
	// most namedLogMax.
	LogLines map[string]int `json:"log_lines"`
	Errors   []string       `json:"errors,omitempty"`
}

// connackAskMax bounds how many clients a run asks the records of.
const connackAskMax = 5000

// namedClients gathers every client the run named, in a fixed order, and
// why: the delivery check's clients that received nothing and its negative
// controls, then those each node's log names, by node. It is pure.
func namedClients(d *deliveryResult, logs map[string]string) (ids []string, why map[string][]string) {
	why = map[string][]string{}
	add := func(id, reason string) {
		if _, ok := why[id]; !ok {
			ids = append(ids, id)
		}
		why[id] = append(why[id], reason)
	}
	if d != nil {
		for _, l := range []struct {
			what string
			list []deliveryClient
		}{{"lost session", d.Lost}, {"not delivered on a held connection", d.Held}, {"told Session Present 0", d.ToldNamed},
			{"not online", d.NotOnlineNamed}, {"never accepted", d.NeverAcceptedNamed}, {"negative control", d.Controls}} {
			for _, c := range l.list {
				add(c.ID, fmt.Sprintf("delivery check: %s (%s on %s)", l.what, c.Class, c.NodeName))
			}
		}
	}
	for _, node := range sortedNames(logs) {
		for _, n := range logNamedClients(node, logs[node]) {
			add(n[0], n[1])
		}
	}
	return ids, why
}

// readConnacks reads the records of every client the run named. The node
// logs are read on their hosts, so it runs while the nodes' directories
// still exist, and before the driver closes.
func (r *scaleRun) readConnacks(ctx context.Context) {
	rec := &scaleConnacks{LogLines: map[string]int{}, Clients: make([]clientConnacks, 0)}
	r.rec.Connacks = rec
	logs := map[string]string{}
	for _, name := range sortedNames(r.procs) {
		p := r.procs[name]
		out, err := runScript(ctx, p.sh, namedLogScript(p.dir))
		if err != nil {
			rec.Errors = append(rec.Errors, fmt.Sprintf("%s's log: %v", name, err))
			continue
		}
		logs[name] = out
		rec.LogLines[name] = len(logNamedClients(name, out))
	}
	ids, why := namedClients(r.rec.Delivery, logs)
	if len(ids) > connackAskMax {
		rec.Errors = append(rec.Errors, fmt.Sprintf("%d clients named; the first %d asked", len(ids), connackAskMax))
		ids = ids[:connackAskMax]
	}
	rec.Asked = len(ids)
	var fromDriver, fromProbe []string
	for _, id := range ids {
		if strings.HasPrefix(id, "probe-") {
			fromProbe = append(fromProbe, id)
		} else {
			fromDriver = append(fromDriver, id)
		}
	}
	got := map[string]clientConnacks{}
	if len(fromDriver) > 0 {
		res, err := r.pop.connacks(fromDriver)
		if err != nil {
			rec.Errors = append(rec.Errors, fmt.Sprintf("the driver's records: %v", err))
		}
		for _, c := range res {
			got[c.ID] = c
		}
	}
	if len(fromProbe) > 0 && r.pr != nil {
		for _, c := range connackRecords(append(append([]*Client(nil), r.pr.pubs...), r.pr.subs...), fromProbe) {
			got[c.ID] = c
		}
	}
	names := map[string]string{}
	for _, n := range r.cfg.Layout.Nodes {
		names[n.clientAddr()] = n.Name
	}
	var crashMs int64
	if r.rec.Storm != nil {
		crashMs = r.rec.Storm.CrashMs
	}
	for _, id := range ids {
		c, ok := got[id]
		if !ok {
			c = clientConnacks{ID: id, Last: make([]connackEntry, 0)}
		}
		c.Why = why[id]
		for i := range c.Last {
			dateEntry(&c.Last[i], names, crashMs)
		}
		if c.LastNoConnack != nil {
			dateEntry(c.LastNoConnack, names, crashMs)
		}
		rec.Clients = append(rec.Clients, c)
	}
}

// dateEntry names an entry's node and dates it against the crash.
func dateEntry(e *connackEntry, names map[string]string, crashMs int64) {
	e.NodeName = names[e.Node]
	if crashMs > 0 {
		s := float64(e.AtMs-crashMs) / 1000
		e.SinceCrashSec = &s
	}
}

// connackReportMax bounds how many named clients the report lists.
const connackReportMax = 50

// connackReport is the report's account of the named clients' CONNACKs.
func connackReport(c *scaleConnacks) string {
	if c == nil {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, "\n## Named clients' CONNACKs\n\n%d clients named (by the delivery check and the node logs: %v lines naming one); each one's latest %d CONNACKs and its last handshake that read none, against the crash.\n\n",
		c.Asked, c.LogLines, connackRing)
	for _, e := range c.Errors {
		fmt.Fprintf(&b, "- **Error:** %s\n", e)
	}
	for i, cl := range c.Clients {
		if i == connackReportMax {
			fmt.Fprintf(&b, "- … %d more in run.json\n", len(c.Clients)-i)
			break
		}
		fmt.Fprintf(&b, "- %s (%s): %s\n", cl.ID, strings.Join(cl.Why, "; "), connackRecordLine(cl))
	}
	return b.String()
}

// connackRecordLine is one client's record as the report gives it.
func connackRecordLine(cl clientConnacks) string {
	if !cl.Found {
		return "no such client"
	}
	parts := make([]string, 0, len(cl.Last)+1)
	for _, e := range cl.Last {
		parts = append(parts, fmt.Sprintf("%s %s %s, Session Present %v, %d ms", sinceCrash(e), e.NodeName, e.Code, e.SessionPresent, e.TookMs))
	}
	line := fmt.Sprintf("%d CONNACKs: %s", cl.Seen, strings.Join(parts, "; "))
	if e := cl.LastNoConnack; e != nil {
		line += fmt.Sprintf("; %d without a CONNACK, the last %s %s %s after %d ms", cl.NoConnacks, sinceCrash(*e), e.NodeName, e.NoConnack, e.TookMs)
	}
	return line
}

func sinceCrash(e connackEntry) string {
	if e.SinceCrashSec == nil {
		return fmt.Sprintf("at %d ms", e.AtMs)
	}
	return fmt.Sprintf("%+.3f s", *e.SinceCrashSec)
}
