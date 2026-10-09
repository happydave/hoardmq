package sim

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"testing"
	"time"
)

// TestCorpus runs the fixed seeds; HOARDMQ_SIM_SEED=n runs one seed
// instead, HOARDMQ_SIM_RANDOM=k runs k random seeds (printed first).
func TestCorpus(t *testing.T) {
	if seed := os.Getenv("HOARDMQ_SIM_SEED"); seed != "" {
		n, err := strconv.ParseInt(seed, 10, 64)
		if err != nil {
			t.Fatalf("HOARDMQ_SIM_SEED: %v", err)
		}
		runSeed(t, n, "requested")
		return
	}
	if k := os.Getenv("HOARDMQ_SIM_RANDOM"); k != "" {
		count, err := strconv.Atoi(k)
		if err != nil {
			t.Fatalf("HOARDMQ_SIM_RANDOM: %v", err)
		}
		base := time.Now().UnixNano()
		for i := 0; i < count; i++ {
			seed := base + int64(i)
			t.Logf("random seed %d", seed)
			runSeed(t, seed, "random")
		}
		return
	}
	corpus := Corpus
	if os.Getenv("HOARDMQ_SIM_REDUCED") != "" {
		corpus = corpus[:2]
	}
	acked := 0
	for _, c := range corpus {
		n, summary := runSeed(t, c.Seed, c.What)
		acked += n
		// The description in the corpus is derived from the schedule, so a
		// generator change moves it rather than leaving a label that reads
		// like a finding (WI 1579).
		if summary != "" && summary != c.What {
			t.Errorf("seed %d is described as %q; its schedule is %q", c.Seed, c.What, summary)
		}
	}
	// Over the corpus as a whole, publishes must have been *accepted*:
	// no-acknowledged-loss is a statement about acknowledged publishes only,
	// so a build that refused everything would hold it over an empty ledger
	// every time (WI 1308). `acked` is the count of accepted publishes; a
	// refused one is in the ledger's failures and in no property's subject.
	t.Logf("exemptions fired over the corpus: %d", exemptionsFired)
	if acked == 0 {
		t.Fatal("the corpus accepted no publish, so no-acknowledged-loss held vacuously throughout: a build that refuses its whole workload passes it")
	}
}

// TestScheduleSummaryNamesEveryFaultInOrder: the summary is what the corpus
// is described by, so it has to name each kind of fault, keep the order the
// schedule runs them in, distinguish the two disk treatments of a crash, and
// count repeats rather than list them twice.
func TestScheduleSummaryNamesEveryFaultInOrder(t *testing.T) {
	for _, c := range []struct {
		name  string
		steps []Step
		want  string
	}{
		{"nothing", nil, "no faults"},
		{"one", []Step{{Kind: "partition"}}, "a partition"},
		{"order is the schedule's", []Step{{Kind: "delay"}, {Kind: "partition"}, {Kind: "skew"}, {Kind: "reorder"}},
			"a delay, a partition, a clock skew and a reorder"},
		{"repeats are counted", []Step{{Kind: "reorder"}, {Kind: "reorder"}, {Kind: "skew"}, {Kind: "drain"}},
			"two reorders, a clock skew and a drain"},
		{"a crash carries its disk treatment", []Step{{Kind: "crash", Disk: "truncate"}, {Kind: "crash", Disk: "corrupt"}, {Kind: "crash"}},
			"a crash with a torn tail, a crash with a corrupted record and a crash"},
		{"crashes pluralize", []Step{{Kind: "crash"}, {Kind: "crash"}, {Kind: "drop"}, {Kind: "drop"}, {Kind: "drop"}},
			"two crashes and three drops"},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := scheduleSummary(c.steps); got != c.want {
				t.Errorf("scheduleSummary() = %q, want %q", got, c.want)
			}
		})
	}
}

// runSeed runs one seed and returns how many publishes it acknowledged and
// what its schedule was, summarised.
func runSeed(t *testing.T, seed int64, what string) (int, string) {
	acked := 0
	summary := ""
	t.Run("seed-"+strconv.FormatInt(seed, 10), func(t *testing.T) {
		o := RunOptions{Seed: seed}
		if path := os.Getenv("HOARDMQ_SIM_LOG"); path != "" {
			// Node debug logs for a failing seed, one file per run.
			f, err := os.Create(path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = f.Close() }()
			o.Cluster.Log = f
		}
		r := Run(t, o)
		acked = r.Stats["publications"]
		summary = scheduleSummary(r.Schedule)
		t.Logf("%s\n%s", what, r.Describe())
		var fired []Violation
		r.Violations, fired = exempt(corpusExemptions, seed, r.Violations)
		for _, v := range fired {
			exemptionsFired++
			t.Logf("EXEMPTED (%s): %s", corpusExemptions[seed][v.Property], v)
		}
		if len(r.Violations) > 0 {
			t.Fatalf("seed %d violated %d properties; reproduce with HOARDMQ_SIM_SEED=%d go test ./sim -run TestCorpus", seed, len(r.Violations), seed)
		}
		// The properties hold vacuously over an empty ledger. Per seed the
		// bar is that messages moved at all: a schedule that keeps a node
		// partitioned for half its window may legitimately acknowledge
		// nothing, and failing it would assert an availability bar the
		// design does not set. That acknowledgements happen at all is
		// checked over the corpus as a whole and by the fault-free baseline.
		if r.Stats["deliveries"] == 0 {
			t.Fatalf("seed %d carried no traffic to check: %v", seed, r.Stats)
		}
	})
	return acked, summary
}

// corpusExemptions are the properties a corpus seed is not judged on, each
// naming the work item that removes it. A fired exemption is logged and
// counted, never silent.
var corpusExemptions = map[int64]map[string]string{}

// ownedExemption is the form an exemption's text takes: the work item that
// removes it, then what it covers.
var ownedExemption = regexp.MustCompile(`^WI [0-9]+: .+`)

// unownedExemptions lists the seed/property pairs whose text names no work
// item.
func unownedExemptions(exemptions map[int64]map[string]string) []string {
	var bad []string
	for seed, props := range exemptions {
		for prop, text := range props {
			if !ownedExemption.MatchString(text) {
				bad = append(bad, fmt.Sprintf("seed %d %s: %q", seed, prop, text))
			}
		}
	}
	return bad
}

// exemptionsFired counts exempted violations over one TestCorpus run.
var exemptionsFired int

// exempt splits a seed's violations into those judged and those its
// exemptions cover.
func exempt(exemptions map[int64]map[string]string, seed int64, vs []Violation) (judged, fired []Violation) {
	for _, v := range vs {
		if _, ok := exemptions[seed][v.Property]; ok {
			fired = append(fired, v)
			continue
		}
		judged = append(judged, v)
	}
	return judged, fired
}

// TestWorkloadWithoutFaults: the properties hold with no fault injected,
// which is the baseline the defect injection tests contrast.
func TestWorkloadWithoutFaults(t *testing.T) {
	r := Run(t, RunOptions{Seed: 99, NoFaults: true, Window: 3 * time.Second, Settle: 6 * time.Second})
	t.Log(r.Describe())
	if len(r.Violations) > 0 {
		t.Fatalf("violations without faults: %v", r.Violations)
	}
	if r.Stats["publications"] == 0 || r.Stats["deliveries"] == 0 {
		t.Fatalf("no traffic: %v", r.Stats)
	}
}
