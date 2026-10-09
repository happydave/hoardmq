package sim

import (
	"context"
	"fmt"

	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The rolling-restart arm's retained probe: one publisher per node
// writes QoS 1 retained messages round-robin over its topics, which cover every
// shard, so a roll's map changes move shards that hold them. Every outcome is
// kept with its time, so refusals can be counted per step and over the roll;
// after the hold a fresh subscriber reads every topic, and a topic served an
// older message than its last acknowledged one, or none, is counted lost.

const rollRetainedEnv = "HOARDMQ_SIM_ROLL_RETAINED"

// rollRetainedTopics is how many topics the retained probe writes: the env's
// value at the product's timings (0, off, when unset), 16 in the gate.
func rollRetainedTopics(getenv func(string) string, record bool) int {
	if v := getenv(rollRetainedEnv); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			return n
		}
	}
	if record {
		return 0
	}
	return 16
}

func retainedProbeTopic(i int) string { return fmt.Sprintf("rprobe/%04d", i) }

// retainedProbe is the probe's state.
type retainedProbe struct {
	topics int
	pubs   []*Client
	rate   time.Duration
	stop   chan struct{}
	wg     sync.WaitGroup

	mu        sync.Mutex
	outcomes  []probeOutcome
	lastAcked map[string]uint64
	published int64
	acked     int64
}

// retainedProbeRecord is the probe in roll.json.
type retainedProbeRecord struct {
	Topics    int              `json:"topics"`
	Published int64            `json:"published"`
	Acked     int64            `json:"acked"`
	Refused   map[string]int64 `json:"refused"`
	Failed    int64            `json:"failed"`
	// Steps counts the probe's outcomes per step, Roll over the roll.
	Steps []windowCounts `json:"steps"`
	Roll  windowCounts   `json:"roll"`
	// Served is how many topics the end check was sent a message for; Lost
	// names the topics served an older message than their last acknowledged
	// one, or none.
	Served      int      `json:"served"`
	AckedTopics int      `json:"acked_topics"`
	Lost        []string `json:"lost"`
}

func newRetainedProbe(ctx context.Context, topics int, rate time.Duration, c *Cluster) (*retainedProbe, error) {
	rp := &retainedProbe{topics: topics, rate: rate, stop: make(chan struct{}), lastAcked: map[string]uint64{}}
	addrs := c.Addresses()
	for j := range addrs {
		order := append([]string(nil), addrs[j:]...)
		order = append(order, addrs[:j]...)
		pub := NewClient(fmt.Sprintf("rprobe-p%02d", j), order, 0, int64(3_000_000+j))
		if err := pub.Run(ctx); err != nil {
			return rp, err
		}
		rp.pubs = append(rp.pubs, pub)
	}
	return rp, nil
}

// start publishes: publisher j writes the topics i with i mod publishers == j,
// one every rate, each topic's sequence counting up.
func (rp *retainedProbe) start(ctx context.Context) {
	for j, pub := range rp.pubs {
		var mine []int
		for i := j; i < rp.topics; i += len(rp.pubs) {
			mine = append(mine, i)
		}
		if len(mine) == 0 {
			continue
		}
		rp.wg.Add(1)
		go func(pub *Client, mine []int) {
			defer rp.wg.Done()
			t := time.NewTicker(rp.rate)
			defer t.Stop()
			seqs := make([]uint64, len(mine))
			for k := 0; ; k = (k + 1) % len(mine) {
				select {
				case <-rp.stop:
					return
				case <-ctx.Done():
					return
				case <-t.C:
				}
				seqs[k]++
				topic := retainedProbeTopic(mine[k])
				pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				code, err := pub.PublishRetained(pctx, topic, 1, seqs[k])
				cancel()
				o := probeOutcome{at: time.Now()}
				switch {
				case err != nil && code >= 0x80:
					o.code = code.String()
				case err != nil:
					o.failed = true
				}
				rp.mu.Lock()
				rp.published++
				rp.outcomes = append(rp.outcomes, o)
				if err == nil {
					rp.acked++
					rp.lastAcked[topic] = seqs[k]
				}
				rp.mu.Unlock()
			}
		}(pub, mine)
	}
}

// result stops publishing and reads every topic from a fresh subscriber.
func (rp *retainedProbe) result(ctx context.Context, c *Cluster, starts []time.Time, end time.Time) retainedProbeRecord {
	close(rp.stop)
	rp.wg.Wait()
	rp.mu.Lock()
	rec := retainedProbeRecord{Topics: rp.topics, Published: rp.published, Acked: rp.acked, Refused: map[string]int64{}, Lost: []string{}}
	outcomes := append([]probeOutcome(nil), rp.outcomes...)
	last := make(map[string]uint64, len(rp.lastAcked))
	for k, v := range rp.lastAcked {
		last[k] = v
	}
	rp.mu.Unlock()
	for _, o := range outcomes {
		switch {
		case o.code != "":
			rec.Refused[o.code]++
		case o.failed:
			rec.Failed++
		}
	}
	rec.Steps, rec.Roll = stepWindows(starts, end, outcomes)
	served := map[string]uint64{}
	sub := NewClient("rprobe-check", c.Addresses(), 0, 3_999_999)
	sctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := sub.Run(sctx); err == nil {
		if err := sub.Subscribe(sctx, "rprobe/#", 1); err == nil {
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) && len(served) < len(last) {
				for _, d := range sub.TakeDeliveries() {
					if d.Seq > served[d.Topic] {
						served[d.Topic] = d.Seq
					}
				}
				time.Sleep(50 * time.Millisecond)
			}
		}
		sub.Close()
	}
	rec.Served, rec.AckedTopics = len(served), len(last)
	for topic, seq := range last {
		if served[topic] < seq {
			rec.Lost = append(rec.Lost, fmt.Sprintf("%s: served %d, acknowledged %d", topic, served[topic], seq))
		}
	}
	sort.Strings(rec.Lost)
	return rec
}

func (rp *retainedProbe) close() {
	for _, p := range rp.pubs {
		p.Close()
	}
}

// retainedLine is the probe's report line.
func retainedLine(r *retainedProbeRecord) string {
	if r == nil {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, "retained probe: %d topics, %d published, %d acknowledged, refused %v, failed %d; over the roll %d of %d refused (%.2f%%); served %d topics, lost %d",
		r.Topics, r.Published, r.Acked, r.Refused, r.Failed, r.Roll.refused(), r.Roll.Attempts, 100*r.Roll.Rate, r.Served, len(r.Lost))
	for i, s := range r.Steps {
		fmt.Fprintf(&b, "\n  step %d: %d of %d refused %v", i+1, s.refused(), s.Attempts, s.Refused)
	}
	return b.String()
}
