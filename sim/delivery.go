package sim

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

// The delivery check, after a storm: one QoS 1 message on each chosen
// client's own filter, and every client classed by what it received. A
// client resumed with Session Present 1 holds every filter as far as it
// knows, so a session that came back without its subscriptions reads
// recovered; only a message that never arrives shows it.

// deliveryRequest asks for the check: Sample clients chosen with Seed (0 or
// the population's size: every client), up to Controls of them made into
// negative controls, and WaitMs for the messages after the last publish.
type deliveryRequest struct {
	Sample   int   `json:"sample"`
	Seed     int64 `json:"seed"`
	Controls int   `json:"controls"`
	WaitMs   int64 `json:"wait_ms"`
}

// deliveryClient names one client in the result: the address of the node it
// is connected to (NodeName is filled in by the test, which knows the
// layout), its class, and the reason (a loss's kind, a refusal's last code).
type deliveryClient struct {
	ID       string `json:"id"`
	Node     string `json:"node"`
	NodeName string `json:"node_name,omitempty"`
	Class    string `json:"class"`
	Reason   string `json:"reason,omitempty"`
}

// deliveryResult is the check's outcome. Every chosen client is in exactly
// one class; controls are counted apart from all of them. Each named list
// holds at most deliveryNameCap clients.
type deliveryResult struct {
	Sample     int   `json:"sample"`
	Seed       int64 `json:"seed"`
	Checked    int   `json:"checked"`
	Publishers int   `json:"publishers"`
	Published  int64 `json:"published"`
	Passes     int   `json:"passes"`
	PublishMs  int64 `json:"publish_ms"`
	WaitMs     int64 `json:"wait_ms"`

	Delivered int `json:"delivered"`
	// LostSessions are clients resumed with Session Present 1 since the mark
	// that received nothing.
	LostSessions int              `json:"lost_sessions"`
	Lost         []deliveryClient `json:"lost"`
	// HeldNotDelivered are clients that kept their connection through the
	// storm and received nothing.
	HeldNotDelivered int              `json:"held_not_delivered"`
	Held             []deliveryClient `json:"held"`
	Told             int              `json:"told_session_present_0"`
	ToldNamed        []deliveryClient `json:"told"`
	NotOnline        int              `json:"not_online"`
	NotOnlineNamed   []deliveryClient `json:"not_online_named"`
	// NeverAccepted counts, by the last reason code, publishes refused on
	// every pass: not losses, since the broker never took the message.
	NeverAccepted      map[string]int   `json:"never_accepted"`
	NeverAcceptedNamed []deliveryClient `json:"never_accepted_named"`
	// RefusalsByCode counts every refused attempt, retried ones included.
	RefusalsByCode map[string]int64 `json:"refusals_by_code"`

	ControlsAsked   int              `json:"controls_asked"`
	ControlsRemoved int              `json:"controls_removed"`
	ControlsCaught  int              `json:"controls_caught"`
	Controls        []deliveryClient `json:"controls"`
}

const (
	classDelivered     = "delivered"
	classNeverAccepted = "never-accepted"
	classNotOnline     = "not-online"
	classTold          = "told-session-present-0"
	classLost          = "lost-session"
	classHeld          = "held-not-delivered"
	// classNotRemoved is a control whose UNSUBACK was not Success: not judged.
	classNotRemoved = "not-removed"

	reasonNoSubscriber = "no-matching-subscribers"
	reasonUndelivered  = "accepted-not-delivered"
)

const (
	// deliveryNameCap bounds how many clients a class names.
	deliveryNameCap = 1000
	// deliveryPublishers bounds the connections that publish the check.
	deliveryPublishers = 64
	// deliveryPasses bounds how often a refused publish is tried, and
	// deliveryPassGap is the pause between passes.
	deliveryPasses  = 30
	deliveryPassGap = 2 * time.Second
	// deliveryUnsubWait bounds a control's wait for its UNSUBACK.
	deliveryUnsubWait = 10 * time.Second
	// deliveryPublisherPrefix starts every check publisher's client
	// identifier, and so its messages' publisher field.
	deliveryPublisherPrefix = "dc"
)

// deliveryFacts is what decides one client's class.
type deliveryFacts struct {
	// Received: the check's message arrived on the client's filter.
	Received bool
	// Accepted: a publish was answered with a reason code below 0x80; Code
	// is that answer, or the last refusal when none was accepted, and NoAck
	// says the last attempt had no answer at all.
	Accepted bool
	Code     packet.ReasonCode
	NoAck    bool
	// Recovered: online with every held filter granted, at the wait's end.
	Recovered bool
	// Told: Session Present 0 seen since the mark. Resumed: an accepted
	// CONNACK since the mark.
	Told    bool
	Resumed bool
}

// classifyDelivery decides a client's class and its reason. The order
// matters: a refused publish says nothing about the session, so it comes
// before every loss class; a client told Session Present 0 was already
// counted lost by the storm; only then does resumed against held decide.
func classifyDelivery(f deliveryFacts) (class, reason string) {
	switch {
	case f.Received:
		return classDelivered, ""
	case !f.Accepted:
		if f.NoAck {
			return classNeverAccepted, "none"
		}
		return classNeverAccepted, ackName(f.Code)
	case !f.Recovered:
		return classNotOnline, ""
	case f.Told:
		return classTold, ""
	}
	reason = reasonUndelivered
	if f.Code == packet.NoMatchingSubscribers {
		reason = reasonNoSubscriber
	}
	if f.Resumed {
		return classLost, reason
	}
	return classHeld, reason
}

// controlCaught says whether a control's class is one the check counts as a
// loss: a control in any other class shows the check would have missed it.
func controlCaught(class string) bool {
	return class == classLost || class == classHeld
}

// deliverySample returns the indexes of the clients to check: every one when
// sample is 0 or at least n, else sample distinct indexes drawn with seed,
// in drawing order.
func deliverySample(n, sample int, seed int64) []int {
	rng := rand.New(rand.NewSource(seed)) //nolint:gosec // a seeded, recorded sample
	if sample <= 0 || sample >= n {
		return rng.Perm(n)
	}
	return rng.Perm(n)[:sample]
}

// deliver runs the check over the clients chosen by req, against the
// baselines the mark took.
func (p *localPopulation) deliver(ctx context.Context, req deliveryRequest) (deliveryResult, error) {
	all := p.clientsSnapshot()
	res := deliveryResult{Sample: req.Sample, Seed: req.Seed, ControlsAsked: req.Controls, NeverAccepted: map[string]int{},
		RefusalsByCode: map[string]int64{}, Lost: make([]deliveryClient, 0), Held: make([]deliveryClient, 0),
		ToldNamed: make([]deliveryClient, 0), NotOnlineNamed: make([]deliveryClient, 0),
		NeverAcceptedNamed: make([]deliveryClient, 0), Controls: make([]deliveryClient, 0)}
	if len(p.genAt) != len(all) || p.sessionLostAt == nil {
		return res, errors.New("deliver: the population was never marked")
	}
	chosen := deliverySample(len(all), req.Sample, req.Seed)
	res.Checked = len(chosen)
	resumed := func(i int) bool { return all[i].gen.Load() > p.genAt[i] }
	told := func(i int) bool { return all[i].SessionLost.Load() > p.sessionLostAt[all[i]] }

	// Controls: clients resumed with Session Present 1 and recovered now,
	// in drawing order, each made to lose its subscription while it still
	// holds the filter.
	control := map[int]bool{}
	for _, i := range chosen {
		if len(control) >= req.Controls {
			break
		}
		cl := all[i]
		if !resumed(i) || told(i) || !cl.Recovered() {
			continue
		}
		uctx, cancel := context.WithTimeout(ctx, deliveryUnsubWait)
		code, err := cl.dropSubscription(uctx, "f/"+cl.ID)
		cancel()
		control[i] = true
		if err != nil || code != packet.Success {
			reason := ackName(code)
			if err != nil {
				reason = err.Error()
			}
			res.Controls = append(res.Controls, deliveryClient{ID: cl.ID, Node: cl.Node(), Class: classNotRemoved, Reason: reason})
			continue
		}
		res.ControlsRemoved++
	}

	for _, i := range chosen {
		all[i].TakeDeliveries()
	}
	n := min(deliveryPublishers, len(chosen))
	res.Publishers = n
	pubs := make([]*Client, n)
	for j := range pubs {
		order := append([]string(nil), p.addrs[j%len(p.addrs):]...)
		order = append(order, p.addrs[:j%len(p.addrs)]...)
		pubs[j] = NewClient(fmt.Sprintf("%s%03d", deliveryPublisherPrefix, j), order, 0, int64(5_000_000+j))
		if len(p.sources) > 0 {
			pubs[j].Source = p.sources[j%len(p.sources)]
		}
		pctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		err := pubs[j].Run(pctx)
		cancel()
		if err != nil {
			for _, c := range pubs[:j+1] {
				c.Close()
			}
			return res, fmt.Errorf("deliver: publisher %d: %w", j, err)
		}
	}
	defer func() {
		for _, c := range pubs {
			c.Close()
		}
	}()

	start := time.Now()
	out := publishPasses(len(chosen), n, func(j, k int) (packet.ReasonCode, error) {
		return pubs[j].publish(ctx, &packet.Publish{Topic: "f/" + all[chosen[k]].ID, QoS: 1, Payload: payload(pubs[j].ID, uint64(chosen[k]))})
	}, func() bool {
		sleepCtx(ctx, deliveryPassGap)
		return ctx.Err() == nil
	})
	accepted, code := out.accepted, out.code
	res.Published, res.Passes, res.RefusalsByCode = out.published, out.passes, out.refusals
	res.PublishMs = time.Since(start).Milliseconds()

	// The wait: every chosen client's deliveries of the check's message,
	// until each publish answered Success has arrived or the bound passes.
	received := make([]bool, len(chosen))
	waitStart := time.Now()
	deadline := waitStart.Add(time.Duration(req.WaitMs) * time.Millisecond)
	for {
		outstanding := 0
		for k, i := range chosen {
			cl := all[i]
			for _, d := range cl.TakeDeliveries() {
				if d.Topic == "f/"+cl.ID && strings.HasPrefix(d.Publisher, deliveryPublisherPrefix) {
					received[k] = true
				}
			}
			if accepted[k] && code[k] == packet.Success && !received[k] {
				outstanding++
			}
		}
		if outstanding == 0 || time.Now().After(deadline) || ctx.Err() != nil {
			break
		}
		sleepCtx(ctx, 250*time.Millisecond)
	}
	res.WaitMs = time.Since(waitStart).Milliseconds()

	name := func(list *[]deliveryClient, c deliveryClient) {
		if len(*list) < deliveryNameCap {
			*list = append(*list, c)
		}
	}
	for k, i := range chosen {
		cl := all[i]
		class, reason := classifyDelivery(deliveryFacts{Received: received[k], Accepted: accepted[k], Code: code[k], NoAck: out.noAck[k],
			Recovered: cl.Recovered(), Told: told(i), Resumed: resumed(i)})
		c := deliveryClient{ID: cl.ID, Node: cl.Node(), Class: class, Reason: reason}
		if control[i] {
			// A control whose removal failed is already listed, unjudged.
			if !containsID(res.Controls, cl.ID) {
				if controlCaught(class) {
					res.ControlsCaught++
				}
				res.Controls = append(res.Controls, c)
			}
			continue
		}
		switch class {
		case classDelivered:
			res.Delivered++
		case classNeverAccepted:
			res.NeverAccepted[reason]++
			name(&res.NeverAcceptedNamed, c)
		case classNotOnline:
			res.NotOnline++
			name(&res.NotOnlineNamed, c)
		case classTold:
			res.Told++
			name(&res.ToldNamed, c)
		case classLost:
			res.LostSessions++
			name(&res.Lost, c)
		case classHeld:
			res.HeldNotDelivered++
			name(&res.Held, c)
		}
	}
	return res, nil
}

// passOutcome is what the publish passes leave for each of the clients
// checked (by position k): whether a publish was answered below 0x80, the
// last answer, and whether the last attempt had no answer at all.
type passOutcome struct {
	accepted  []bool
	code      []packet.ReasonCode
	noAck     []bool
	refusals  map[string]int64
	published int64
	passes    int
}

// publishPasses publishes to each of n clients over the given publishers,
// publisher j taking every publishers-th client of a pass, and publishes
// again in a later pass to each one refused or unanswered, at most
// deliveryPasses passes; pause runs between passes and stops them when it
// returns false.
func publishPasses(n, publishers int, publish func(j, k int) (packet.ReasonCode, error), pause func() bool) passOutcome {
	out := passOutcome{accepted: make([]bool, n), code: make([]packet.ReasonCode, n), noAck: make([]bool, n), refusals: map[string]int64{}}
	pending := make([]int, n)
	for k := range pending {
		pending[k] = k
	}
	var mu sync.Mutex
	for pass := 0; pass < deliveryPasses && len(pending) > 0; pass++ {
		if pass > 0 && !pause() {
			break
		}
		out.passes++
		var wg sync.WaitGroup
		var refused []int
		for j := 0; j < publishers; j++ {
			wg.Add(1)
			go func(j int) {
				defer wg.Done()
				var mine []int
				local := map[string]int64{}
				var sent int64
				for x := j; x < len(pending); x += publishers {
					k := pending[x]
					c, err := publish(j, k)
					sent++
					// Each k is written by one publisher in a pass.
					out.code[k], out.noAck[k] = c, err != nil && c < 0x80
					if err != nil {
						mine = append(mine, k)
						if out.noAck[k] {
							local["none"]++
						} else {
							local[ackName(c)]++
						}
						continue
					}
					out.accepted[k] = true
				}
				mu.Lock()
				out.published += sent
				refused = append(refused, mine...)
				for name, v := range local {
					out.refusals[name] += v
				}
				mu.Unlock()
			}(j)
		}
		wg.Wait()
		sort.Ints(refused)
		pending = refused
	}
	return out
}

func containsID(list []deliveryClient, id string) bool {
	for _, c := range list {
		if c.ID == id {
			return true
		}
	}
	return false
}
