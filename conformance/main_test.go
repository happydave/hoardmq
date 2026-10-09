package conformance

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
)

// TestMain loads the statement identifiers so annotations can be checked,
// runs the suite, and writes the results file when asked.
func TestMain(m *testing.M) {
	flag.Parse()
	if ids, err := loadStatements("."); err == nil {
		known = ids
	}
	code := m.Run()
	regMu.Lock()
	defer regMu.Unlock()
	if len(unknown) > 0 {
		var names []string
		for id := range unknown {
			names = append(names, id)
		}
		sort.Strings(names)
		fmt.Fprintf(os.Stderr, "conformance: %d annotated statement identifiers are not in the statements files: %s\n", len(names), strings.Join(names, ", "))
		if code == 0 {
			code = 1
		}
	}
	if *resultsPath != "" {
		for _, r := range results {
			sort.Strings(r.Tests)
		}
		data, err := json.MarshalIndent(results, "", "  ")
		if err == nil {
			err = os.WriteFile(*resultsPath, append(data, '\n'), 0o644)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "conformance: writing results:", err)
			if code == 0 {
				code = 1
			}
		}
	}
	os.Exit(code)
}
