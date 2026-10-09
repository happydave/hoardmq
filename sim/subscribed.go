package sim

import (
	"fmt"
	"time"
)

// subscribeRefusal is one refused SUBACK, as the violation reports it.
type subscribeRefusal struct {
	Client string
	Node   string
	Code   string
	At     time.Time
}

// subscribeRefusals reads the ledger's events for the refusals the harness
// records. Client.Subscribe returns the error to its caller, but
// Client.resubscribe — the path a reconnect takes, and the one a storm puts
// every displaced client through — cannot wait for its acknowledgements, so
// the event is the only trace either path leaves in the ledger (WI 1500).
func subscribeRefusals(events []Event) []subscribeRefusal {
	var out []subscribeRefusal
	for _, e := range events {
		if e.Kind == "suback-refused" {
			out = append(out, subscribeRefusal{Client: e.Client, Node: e.Node, Code: e.Code.String(), At: e.At})
		}
	}
	return out
}

// subscribedViolations reports refused subscribes, and reports a run that
// granted none as a failure rather than a pass.
//
// The non-vacuity check is the point of the second case. A run that
// subscribed to nothing has no refusals either, and would otherwise report a
// clean bill for a property it never exercised — which is how a check comes
// to be trusted without ever having been tested.
//
// One violation per refusal, uncapped, as every other property here reports
// its own: Check runs only on a corpus scenario, whose client count is in the
// hundreds. The failover measurement at 250,000 clients never calls it — it
// reports the two counts in run.json instead.
//
// The two failures are deliberately worded apart. "Nothing subscribed" and
// "everything was refused" both leave the granted list empty, and a message
// that could not tell them apart would send a reader looking in the wrong
// place.
func subscribedViolations(granted int, refusals []subscribeRefusal) []Violation {
	var out []Violation
	for _, r := range refusals {
		out = append(out, Violation{PropSubscribed, fmt.Sprintf(
			"%s was refused a subscription by %s with %s at %s", r.Client, orUnknown(r.Node), r.Code, r.At.Format(time.RFC3339Nano))})
	}
	if granted == 0 {
		switch {
		case len(refusals) > 0:
			out = append(out, Violation{PropSubscribed, fmt.Sprintf(
				"no subscription in this run was granted, and %d were refused: the run had subscribers and none of them was subscribed", len(refusals))})
		default:
			out = append(out, Violation{PropSubscribed,
				"no subscription was granted and none was refused: this run subscribed to nothing, so the property was never exercised and its silence means nothing"})
		}
	}
	return out
}

// refusalsByClient summarises refusals for the failover artifact: the total
// and how many distinct clients were affected. One client refused fifty times
// and fifty clients refused once are different failures and the total alone
// cannot tell them apart.
func refusalsByClient(refusals []subscribeRefusal) (total, clients int) {
	seen := map[string]bool{}
	for _, r := range refusals {
		seen[r.Client] = true
	}
	return len(refusals), len(seen)
}
