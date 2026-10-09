package sim

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// completeFile is written into a run's artifact directory after every other
// artifact, and is the only signal that a measurement has finished.
//
// A watcher that infers completion from the last file it happens to know
// about is guessing: the throwaway driver used before this existed picked
// the newest artifact directory and looked for report.md, which would have
// killed a run that started before its own directory appeared. The
// definition of "finished" belongs to the measurement, which knows.
const completeFile = "complete"

// runComplete is the marker's content. It is small on purpose: enough to
// tell which run it certifies, and nothing a reader might mistake for the
// measurement's own figures, which live in run.json.
type runComplete struct {
	Clients  int    `json:"clients"`
	Recovery string `json:"recovery"`
	At       string `json:"at"`
}

// writeComplete writes the marker. The caller must have written every other
// artifact first.
func writeComplete(dir string, clients int, recovery string) error {
	raw, err := json.MarshalIndent(runComplete{Clients: clients, Recovery: recovery, At: time.Now().Format(time.RFC3339)}, "", " ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, completeFile), raw, 0o640) //nolint:gosec // operator-chosen dir
}

// recoverySeconds is the headline as a number. It is negative when the
// window closed before every client returned, so "did not reach" can never
// be read as a duration of zero: two of the four runs in the 2026-09-07
// repeat batch did not reach, and averaging them as zero would report the
// best result this project has recorded.
func recoverySeconds(instant, crashAt time.Time) float64 {
	if instant.IsZero() {
		return -1
	}
	return instant.Sub(crashAt).Seconds()
}

// ninetyNineSeconds is the time-to-99% as a number, negative when not
// reached, for the same reason.
func (s *onlineShape) ninetyNineSeconds() float64 { return markSeconds(s.seen, s.ninetyNine) }

// nineNineNineSeconds is the 99.9% mark on the same terms.
func (s *onlineShape) nineNineNineSeconds() float64 { return markSeconds(s.seen, s.nineNineNine) }

func markSeconds(seen bool, at time.Duration) float64 {
	if !seen || at == 0 {
		return -1
	}
	return at.Seconds()
}

// treeReportPath is where a failover run writes its report inside the source
// tree, or "" for nowhere. A recorded run with an artifact directory writes
// its report only there: the copy in results/ was untracked, so provenance
// still read the tree as clean, while the measurement checkout collected stray
// reports and a run on a working tree could overwrite a published report of
// the same date (WI 1483). A recorded run with no artifact directory keeps
// writing results/failover-<date>.md, which is then its only copy.
func treeReportPath(record bool, artifactDir string, now time.Time) string {
	if !record || artifactDir != "" {
		return ""
	}
	return filepath.Join("results", "failover-"+now.Format("2006-01-02")+".md")
}

// settleLimit is the socket count above which a run of this size refuses to
// start. It is the measurement's own rule, shared so that a driver waits for
// exactly the condition the measurement will test rather than a second
// opinion about it.
func settleLimit(clients int) int {
	limit := clients / 10
	if limit < 5000 {
		limit = 5000
	}
	return limit
}

// conntrackStartLimit is the number of connection-tracking entries above which
// a remote-client run refuses to start: over it the table is still holding the
// previous run's connections, which expire about two minutes after it ended,
// and the measurement would be taken with the table filling. It is named here
// rather than written into the refusal so the repeat driver's wait and
// serverStartProblems' refusal cannot drift apart.
const conntrackStartLimit = 10000

// settleWait is one condition the repeat driver waits for between runs.
type settleWait struct {
	what  string
	limit int64
	read  func() (int64, bool)
}

// settleWaits are the conditions a run of this size and mode must be under
// before it starts. Each mirrors a starting condition the measurement itself
// refuses on, so the driver waits for exactly what will be tested.
//
// The conntrack condition existed only on the refusing side until WI 1496:
// two 100,000-client remote runs were refused for a table that clears itself
// in about two minutes, which a batch script then had to wait out by hand.
func settleWaits(clients int, remote bool) []settleWait {
	waits := []settleWait{{
		what:  "TCP sockets in use or in TIME-WAIT",
		limit: int64(settleLimit(clients)),
		read:  func() (int64, bool) { n, ok := tcpSockets(); return int64(n), ok },
	}}
	if remote {
		waits = append(waits, settleWait{
			what:  "connection-tracking entries",
			limit: conntrackStartLimit,
			read:  readConntrackCount,
		})
	}
	return waits
}

// tcpSockets reads the machine's TCP socket count (established plus
// TIME-WAIT) from /proc/net/sockstat. The second return is false where the
// file is absent or unreadable, in which case the caller proceeds.
func tcpSockets() (int, bool) {
	data, err := os.ReadFile("/proc/net/sockstat")
	if err != nil {
		return 0, false
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "TCP:") {
			continue
		}
		fields := strings.Fields(line)
		total := 0
		for i := 1; i+1 < len(fields); i += 2 {
			if fields[i] == "inuse" || fields[i] == "tw" {
				n, err := strconv.Atoi(fields[i+1])
				if err != nil {
					return 0, false
				}
				total += n
			}
		}
		return total, true
	}
	return 0, false
}
