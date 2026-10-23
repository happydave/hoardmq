package conformance

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/happydave/hoardmq/internal/packet"
)

// TestStatementsAndScope checks the data files: identifiers are unique
// within each specification, and every scope entry names a statement of its
// own specification with a status from the closed set and a reason.
func TestStatementsAndScope(t *testing.T) {
	all := map[Spec]map[string]bool{}
	for spec, name := range specFiles {
		data, err := os.ReadFile(filepath.Join("statements", name))
		if err != nil {
			t.Fatal(err)
		}
		var list []statement
		if err := json.Unmarshal(data, &list); err != nil {
			t.Fatal(err)
		}
		all[spec] = map[string]bool{}
		for _, s := range list {
			if all[spec][s.ID] {
				t.Errorf("%s: duplicate %s", name, s.ID)
			}
			if s.Text == "" {
				t.Errorf("%s: %s has no text", name, s.ID)
			}
			all[spec][s.ID] = true
		}
	}
	data, err := os.ReadFile(filepath.Join("statements", "scope.json"))
	if err != nil {
		t.Fatal(err)
	}
	var scope map[Spec]map[string]struct {
		Status string `json:"status"`
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(data, &scope); err != nil {
		t.Fatal(err)
	}
	for spec, entries := range scope {
		if all[spec] == nil {
			t.Errorf("scope.json: %q is not a specification", spec)
			continue
		}
		for id, e := range entries {
			if !all[spec][id] {
				t.Errorf("scope.json: %s %s is not a statement", spec, id)
			}
			if e.Status != "n/a-client" {
				t.Errorf("scope.json: %s %s has status %q", spec, id, e.Status)
			}
			if e.Reason == "" {
				t.Errorf("scope.json: %s %s has no reason", spec, id)
			}
		}
	}
}

// TestRecorder checks the pass-only-if-every-test-passed rule, that one
// identifier is kept apart per specification, and the unknown-identifier
// guard, without failing a real test.
func TestRecorder(t *testing.T) {
	into := Results{}
	record(into, Spec5, "TestA", false, "MQTT-3.1.0-1", "MQTT-3.1.0-2")
	record(into, Spec5, "TestB", true, "MQTT-3.1.0-2")
	if r := into[Spec5]["MQTT-3.1.0-1"]; !r.Pass || len(r.Tests) != 1 {
		t.Fatalf("one passing test: %+v", r)
	}
	if r := into[Spec5]["MQTT-3.1.0-2"]; r.Pass || len(r.Tests) != 2 {
		t.Fatalf("one of two failed: %+v", r)
	}
	// MQTT-3.2.2-4 is in both specifications with different meanings.
	record(into, Spec311, "TestA311", false, "MQTT-3.2.2-4")
	record(into, Spec5, "TestB5", true, "MQTT-3.2.2-4")
	if r := into[Spec311]["MQTT-3.2.2-4"]; r == nil || !r.Pass || len(r.Tests) != 1 || r.Tests[0] != "TestA311" {
		t.Fatalf("3.1.1 MQTT-3.2.2-4: %+v, want TestA311 alone, passing", r)
	}
	if r := into[Spec5]["MQTT-3.2.2-4"]; r == nil || r.Pass || len(r.Tests) != 1 || r.Tests[0] != "TestB5" {
		t.Fatalf("5.0 MQTT-3.2.2-4: %+v, want TestB5 alone, failing", r)
	}
	regMu.Lock()
	before := len(unknown)
	regMu.Unlock()
	record(into, Spec5, "TestC", false, "MQTT-0.0.0-0")
	// MQTT-2.3.1-2 is a 3.1.1 statement only.
	record(into, Spec5, "TestD", false, "MQTT-2.3.1-2")
	regMu.Lock()
	names := unknown["5.0 MQTT-0.0.0-0"]
	wrongSpec := unknown["5.0 MQTT-2.3.1-2"]
	delete(unknown, "5.0 MQTT-0.0.0-0")
	delete(unknown, "5.0 MQTT-2.3.1-2")
	after := len(unknown)
	regMu.Unlock()
	if len(known) > 0 && (len(names) != 1 || names[0] != "TestC") {
		t.Fatalf("unknown identifier not recorded: %v", names)
	}
	if len(known) > 0 && (len(wrongSpec) != 1 || wrongSpec[0] != "TestD") {
		t.Fatalf("a 3.1.1 identifier recorded under 5.0 not reported: %v", wrongSpec)
	}
	if before != after {
		t.Fatal("unknown map not restored")
	}
}

// A 5.0 connection observes the 5.0 statements; levels 4 and 3 the 3.1.1 ones.
func TestSpecOf(t *testing.T) {
	for v, want := range map[packet.Version]Spec{packet.V5: Spec5, packet.V4: Spec311, packet.V3: Spec311} {
		if got := SpecOf(v); got != want {
			t.Errorf("SpecOf(%v) = %s, want %s", v, got, want)
		}
	}
}
