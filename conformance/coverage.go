package conformance

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/happydave/hoardmq/internal/packet"
)

// statement is one row of a statements file.
type statement struct {
	ID   string `json:"id"`
	Text string `json:"text"`
}

// Spec is the specification a statement belongs to. Statement numbers are
// reused between the two with different meanings, so every result and scope
// entry is kept under its specification.
type Spec string

// The specifications. A test at protocol level 3 (MQTT 3.1) observes the
// 3.1.1 statements.
const (
	Spec5   Spec = "5.0"
	Spec311 Spec = "3.1.1"
)

// specFiles names each specification's statements file.
var specFiles = map[Spec]string{Spec5: "mqtt5.json", Spec311: "mqtt311.json"}

// SpecOf is the specification a connection at protocol level v observes.
func SpecOf(v packet.Version) Spec {
	if v == packet.V5 {
		return Spec5
	}
	return Spec311
}

// Result is a statement's outcome across the tests that covered it.
type Result struct {
	Tests []string `json:"tests"`
	Pass  bool     `json:"pass"`
}

// Results is what the test binary writes for the coverage generator: per
// specification, per statement.
type Results map[Spec]map[string]*Result

var (
	resultsPath = flag.String("conformance.results", "", "write per-statement results to this JSON file after the run")

	regMu   sync.Mutex
	results = Results{}
	known   = map[Spec]map[string]bool{}
	unknown = map[string][]string{}
)

// Statement marks the running test as observing the given normative
// statements of spec. The test's outcome is recorded against each of them
// when it completes; a statement passes only if every test that covers it
// passed.
func Statement(t *testing.T, spec Spec, ids ...string) {
	t.Helper()
	name := t.Name()
	t.Cleanup(func() { record(results, spec, name, t.Failed(), ids...) })
}

// record adds one test's outcome to the results for each identifier of spec.
// An identifier spec does not have is reported as "spec id".
func record(into Results, spec Spec, name string, failed bool, ids ...string) {
	regMu.Lock()
	defer regMu.Unlock()
	if into[spec] == nil {
		into[spec] = map[string]*Result{}
	}
	for _, id := range ids {
		if len(known) > 0 && !known[spec][id] {
			key := string(spec) + " " + id
			unknown[key] = append(unknown[key], name)
		}
		r := into[spec][id]
		if r == nil {
			r = &Result{Pass: true}
			into[spec][id] = r
		}
		r.Tests = append(r.Tests, name)
		if failed {
			r.Pass = false
		}
	}
}

// loadStatements reads every statements file and returns each
// specification's identifiers.
func loadStatements(dir string) (map[Spec]map[string]bool, error) {
	ids := map[Spec]map[string]bool{}
	for spec, name := range specFiles {
		data, err := os.ReadFile(filepath.Join(dir, "statements", name))
		if err != nil {
			return nil, err
		}
		var list []statement
		if err := json.Unmarshal(data, &list); err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		ids[spec] = map[string]bool{}
		for _, s := range list {
			ids[spec][s.ID] = true
		}
	}
	return ids, nil
}
