package sim

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// A session served empty after a crash is evidence a later reading of the
// brokers' stores may need: what each node's replica copy and journal held
// for that client. Teardown removes the run's directories, so a run whose
// brokers counted one keeps them instead, to be removed by hand once that
// evidence has been read.

// parseEmptyServes reads hoardmq_sessions_rebuilt_empty_total from a /metrics
// body, summed over its reasons. A node that never served a session empty
// does not create the series, so its absence reads 0.
func parseEmptyServes(body string) float64 {
	var sum float64
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, "hoardmq_sessions_rebuilt_empty_total") {
			continue
		}
		sp := strings.LastIndexByte(line, ' ')
		if sp < 0 {
			continue
		}
		name, _, _ := strings.Cut(line[:sp], "{")
		if name != "hoardmq_sessions_rebuilt_empty_total" {
			continue
		}
		if v, err := strconv.ParseFloat(line[sp+1:], 64); err == nil {
			sum += v
		}
	}
	return sum
}

// keepRunDirs decides whether teardown keeps the run's directories: it does
// when any reading of any node counted a session served empty, or when any
// client dropped after every client was back. why names each node's highest
// count and the late drops by cause, so the record says what the kept
// directories are evidence of. It is pure.
func keepRunDirs(readings []lockReading, late *lateDrops) (keep bool, why []string) {
	most := map[string]float64{}
	for _, rd := range readings {
		if rd.EmptyServes > most[rd.Node] {
			most[rd.Node] = rd.EmptyServes
		}
	}
	for node, n := range most {
		if n > 0 {
			why = append(why, fmt.Sprintf("%s served %.0f sessions empty", node, n))
		}
	}
	sort.Strings(why)
	if late != nil && late.Clients > 0 {
		why = append(why, fmt.Sprintf("%d clients dropped after every client was back (%s)", late.Clients, causeList(late.ByCause)))
	}
	return len(why) > 0, why
}
