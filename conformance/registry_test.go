package conformance

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// TestStatementsAndScope checks the data files: identifiers are unique
// within each specification, and every scope entry names a real statement
// with a status from the closed set and a reason.
func TestStatementsAndScope(t *testing.T) {
	all := map[string]bool{}
	for _, name := range []string{"mqtt5.json", "mqtt311.json"} {
		data, err := os.ReadFile(filepath.Join("statements", name))
		if err != nil {
			t.Fatal(err)
		}
		var list []statement
		if err := json.Unmarshal(data, &list); err != nil {
			t.Fatal(err)
		}
		seen := map[string]bool{}
		for _, s := range list {
			if seen[s.ID] {
				t.Errorf("%s: duplicate %s", name, s.ID)
			}
			if s.Text == "" {
				t.Errorf("%s: %s has no text", name, s.ID)
			}
			seen[s.ID] = true
			all[s.ID] = true
		}
	}
	data, err := os.ReadFile(filepath.Join("statements", "scope.json"))
	if err != nil {
		t.Fatal(err)
	}
	var scope map[string]struct {
		Status string `json:"status"`
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(data, &scope); err != nil {
		t.Fatal(err)
	}
	for id, e := range scope {
		if !all[id] {
			t.Errorf("scope.json: %s is not a statement", id)
		}
		if e.Status != "n/a-client" {
			t.Errorf("scope.json: %s has status %q", id, e.Status)
		}
		if e.Reason == "" {
			t.Errorf("scope.json: %s has no reason", id)
		}
	}
}

// TestRecorder checks the pass-only-if-every-test-passed rule and the
// unknown-identifier guard without failing a real test.
func TestRecorder(t *testing.T) {
	into := Results{}
	record(into, "TestA", false, "MQTT-3.1.0-1", "MQTT-3.1.0-2")
	record(into, "TestB", true, "MQTT-3.1.0-2")
	if r := into["MQTT-3.1.0-1"]; !r.Pass || len(r.Tests) != 1 {
		t.Fatalf("one passing test: %+v", r)
	}
	if r := into["MQTT-3.1.0-2"]; r.Pass || len(r.Tests) != 2 {
		t.Fatalf("one of two failed: %+v", r)
	}
	regMu.Lock()
	before := len(unknown)
	regMu.Unlock()
	record(into, "TestC", false, "MQTT-0.0.0-0")
	regMu.Lock()
	names := unknown["MQTT-0.0.0-0"]
	delete(unknown, "MQTT-0.0.0-0")
	after := len(unknown)
	regMu.Unlock()
	if len(known) > 0 && (len(names) != 1 || names[0] != "TestC") {
		t.Fatalf("unknown identifier not recorded: %v", names)
	}
	if before != after {
		t.Fatal("unknown map not restored")
	}
	delete(into, "MQTT-0.0.0-0")
}
