package conformance

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// statement is one row of a statements file.
type statement struct {
	ID   string `json:"id"`
	Text string `json:"text"`
}

// Result is a statement's outcome across the tests that covered it.
type Result struct {
	Tests []string `json:"tests"`
	Pass  bool     `json:"pass"`
}

// Results is what the test binary writes for the coverage generator.
type Results map[string]*Result

var (
	resultsPath = flag.String("conformance.results", "", "write per-statement results to this JSON file after the run")

	regMu   sync.Mutex
	results = Results{}
	known   = map[string]bool{}
	unknown = map[string][]string{}
)

// Statement marks the running test as observing the given normative
// statements. The test's outcome is recorded against each of them when it
// completes; a statement passes only if every test that covers it passed.
func Statement(t *testing.T, ids ...string) {
	t.Helper()
	name := t.Name()
	t.Cleanup(func() { record(results, name, t.Failed(), ids...) })
}

// record adds one test's outcome to the results for each identifier.
func record(into Results, name string, failed bool, ids ...string) {
	regMu.Lock()
	defer regMu.Unlock()
	for _, id := range ids {
		if len(known) > 0 && !known[id] {
			unknown[id] = append(unknown[id], name)
		}
		r := into[id]
		if r == nil {
			r = &Result{Pass: true}
			into[id] = r
		}
		r.Tests = append(r.Tests, name)
		if failed {
			r.Pass = false
		}
	}
}

// loadStatements reads every statements file and returns the identifiers.
func loadStatements(dir string) (map[string]bool, error) {
	ids := map[string]bool{}
	for _, name := range []string{"mqtt5.json", "mqtt311.json"} {
		data, err := os.ReadFile(filepath.Join(dir, "statements", name))
		if err != nil {
			return nil, err
		}
		var list []statement
		if err := json.Unmarshal(data, &list); err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		for _, s := range list {
			ids[s.ID] = true
		}
	}
	return ids, nil
}
