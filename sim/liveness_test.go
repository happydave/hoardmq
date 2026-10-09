package sim

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

// TestLivenessViolations covers the decision alone: which end-of-run probes
// mean a node could not recover.
//
// The healthy cases come first on purpose. A checker that flagged everything
// would satisfy the refusal cases while making the corpus useless, and a
// liveness property that cries wolf costs more than it catches.
func TestLivenessViolations(t *testing.T) {
	healthy := []struct {
		name  string
		probe Probe
	}{
		{"success", Probe{Node: "n1", Code: packet.Success}},
		// The probe topic has no subscriber, so this is the expected answer
		// from a working node, not a problem.
		{"no matching subscribers", Probe{Node: "n1", Code: packet.NoMatchingSubscribers}},
		{"granted-style zero code", Probe{Node: "n1", Code: 0}},
	}
	for _, tc := range healthy {
		if v := livenessFindings([]Probe{tc.probe}, nil); len(v) != 0 {
			t.Errorf("%s: %v, want no violation", tc.name, v)
		}
	}

	refusals := []struct {
		name string
		code packet.ReasonCode
	}{
		{"barrier could not pass", packet.ImplementationSpecificError},
		{"not authorized", packet.NotAuthorized},
		{"quota exceeded", packet.QuotaExceeded},
	}
	for _, tc := range refusals {
		v := livenessFindings([]Probe{{Node: "n2", Code: tc.code}}, nil)
		if len(v) != 1 {
			t.Errorf("%s: %d violations, want 1", tc.name, len(v))
			continue
		}
		if v[0].Property != PropRecovers {
			t.Errorf("%s: property %q, want %q", tc.name, v[0].Property, PropRecovers)
		}
		// The node and the code both have to be in the text: "a node refused"
		// without saying which, or with what, is the reconstruction WI 1494
		// had to do by hand.
		if !strings.Contains(v[0].Detail, "n2") || !strings.Contains(v[0].Detail, tc.code.String()) {
			t.Errorf("%s: %q names neither the node nor the code", tc.name, v[0].Detail)
		}
	}

	// A probe that never got an answer is the same failure from the caller's
	// side and is reported.
	v := livenessFindings([]Probe{{Node: "n3", Err: errors.New("connection refused")}}, nil)
	if len(v) != 1 || !strings.Contains(v[0].Detail, "n3") {
		t.Errorf("an unanswered probe: %v, want one violation naming n3", v)
	}

	// Several nodes, only the refusing one reported.
	v = livenessFindings([]Probe{
		{Node: "n1", Code: packet.Success},
		{Node: "n2", Code: packet.ImplementationSpecificError},
		{Node: "n3", Code: packet.NoMatchingSubscribers},
	}, nil)
	if len(v) != 1 || !strings.Contains(v[0].Detail, "n2") {
		t.Fatalf("mixed probes: %v, want exactly one violation naming n2", v)
	}
}

// TestLivenessViolationNamesTheStream checks the barrier counters reach the
// violation, which is what turns "n2 refused" into something actionable.
func TestLivenessViolationNamesTheStream(t *testing.T) {
	counters := map[string]string{"n2": "n3/timeout=20 n3/resync=4"}
	v := livenessFindings([]Probe{{Node: "n2", Code: packet.ImplementationSpecificError}}, counters)
	if len(v) != 1 {
		t.Fatalf("%d violations, want 1", len(v))
	}
	if !strings.Contains(v[0].Detail, "n3/timeout=20") {
		t.Errorf("the violation does not carry the barrier failures: %q", v[0].Detail)
	}
	// A node with no recorded failures must not gain an empty trailer.
	v = livenessFindings([]Probe{{Node: "n1", Code: packet.ImplementationSpecificError}}, counters)
	if strings.Contains(v[0].Detail, "barrier failures") {
		t.Errorf("a node with no counters gained an empty note: %q", v[0].Detail)
	}
}

// TestLivenessNotEvaluatedWhileANodeIsDown is the false positive this property
// would otherwise have shipped with, and it is worth spelling out because the
// first version of the test asserted the opposite and failed.
//
// With a replica down, the survivors refuse a QoS 1 publish with 0x83
// *correctly*: the barrier cannot reach the missing peer. Probing a cluster
// that is not whole therefore reports a violation against two nodes that are
// behaving as designed. So the property is not evaluated at all unless every
// node is running.
func TestLivenessNotEvaluatedWhileANodeIsDown(t *testing.T) {
	c := New(t, Options{})

	// Presence first: whole, the probes are taken and say nothing is wrong.
	whole := livenessProbes(c, "liveness/probe")
	if len(whole) != len(c.Nodes) {
		t.Fatalf("%d probes on a whole cluster, want %d", len(whole), len(c.Nodes))
	}
	if v := livenessFindings(whole, nil); len(v) != 0 {
		t.Fatalf("a whole, healthy cluster produced %v", v)
	}

	// Then with one node down: no probes, so no violation.
	c.Crash(c.Nodes[2])
	if probes := livenessProbes(c, "liveness/probe"); probes != nil {
		v := livenessFindings(probes, nil)
		t.Fatalf("the property was evaluated with a node down: %d probes, violations %v", len(probes), v)
	}
}

// TestLivenessProbeAnswersOnAHealthyCluster is the presence half: the probe
// has to actually reach a broker and get an acknowledgement, or the absence
// asserted everywhere else in this file would be satisfied by a probe that
// never worked.
func TestLivenessProbeAnswersOnAHealthyCluster(t *testing.T) {
	c := New(t, Options{})
	start := time.Now()
	code, err := ProbePublish(c.Nodes[0].TCP, "liveness-test", "liveness/probe")
	if err != nil {
		t.Fatalf("the probe did not complete against a healthy node: %v (%s)", err, refusalDiagnosis(c, time.Since(start)))
	}
	// Nobody subscribes to the probe topic, so a working broker says so.
	if code != packet.NoMatchingSubscribers && code != packet.Success {
		t.Fatalf("a healthy node answered the probe with %s (%s)", code, refusalDiagnosis(c, time.Since(start)))
	}
}

// TestPublishFailuresAreAttributedToANode covers B3: the total was already
// reported, the node was not.
func TestPublishFailuresAreAttributedToANode(t *testing.T) {
	l := &Ledger{}
	l.addErr(PublishFailure{Client: "p1", Node: "n1", Seq: 1, Code: packet.ImplementationSpecificError, Err: "publish refused"})
	l.addErr(PublishFailure{Client: "p2", Node: "n1", Seq: 2, Code: packet.ImplementationSpecificError, Err: "publish refused"})
	l.addErr(PublishFailure{Client: "p3", Node: "n2", Seq: 3, Err: "connection ended"})
	l.addErr(PublishFailure{Client: "p4", Seq: 4, Err: "between connections"})

	st := Stats(Evidence{Ledger: l})
	if st["publishErrors"] != 4 {
		t.Errorf("publishErrors %d, want 4", st["publishErrors"])
	}
	if st["publishErrors.n1"] != 2 {
		t.Errorf("publishErrors.n1 %d, want 2", st["publishErrors.n1"])
	}
	if st["publishErrors.n2"] != 1 {
		t.Errorf("publishErrors.n2 %d, want 1", st["publishErrors.n2"])
	}
	// A failure with no node is still counted somewhere rather than vanishing.
	if st["publishErrors.an unknown node"] != 1 {
		t.Errorf("an unattributed failure was dropped: %v", st)
	}
	// The human-readable line still exists and names the node.
	if len(l.Errors) != 4 || !strings.Contains(l.Errors[0], "n1") {
		t.Errorf("the error strings lost the node: %v", l.Errors)
	}
}

func TestLivenessProbeDeadline(t *testing.T) {
	// A probe against a port nobody is listening on must fail quickly rather
	// than hang the end of every run.
	start := time.Now()
	if _, err := ProbePublish("127.0.0.1:1", "liveness-test", "liveness/probe"); err == nil {
		t.Fatal("a probe against a closed port returned no error")
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Errorf("the probe took %s against a closed port", d)
	}
}
