package sim

// Corpus is the fixed seed set the gate runs. Each seed's schedule is derived
// from its number, and so is its description: `What` is what
// `scheduleSummary` says about the schedule the generator produces for that
// seed at the harness timings the gate runs (an 8 s window, four events, a 2 s
// ttl), and `TestCorpus` fails when the two disagree — for every seed in the
// plain gate, and for the first two under HOARDMQ_SIM_REDUCED.
//
// It used to be written by hand, and drifted: WIs 1298 and 1534 were titled
// from labels whose seeds no longer matched, and by 2026-09-21 none of the
// eight descriptions matched its schedule.
var Corpus = []struct {
	Seed int64
	What string
}{
	{1, "a drop, a crash with a corrupted record, a delay and a crash"},
	{2, "a delay, a partition, a clock skew and a reorder"},
	{3, "a drop, a clock skew, a delay and a drain"},
	{4, "a reorder, a delay, a crash with a torn tail and a drain"},
	{5, "a drop, a delay, a partition and a crash"},
	{6, "two reorders, a clock skew and a drain"},
	{7, "three clock skews and a delay"},
	{8, "a drop, a crash with a torn tail, a delay and a partition"},
}
