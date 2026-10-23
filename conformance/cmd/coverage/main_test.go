package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestRenderIsDeterministic(t *testing.T) {
	dir := t.TempDir()
	st := filepath.Join(dir, "statements")
	if err := os.MkdirAll(st, 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("statements/mqtt5.json", `[{"id":"MQTT-1.0.0-1","text":"A | B"},{"id":"MQTT-1.0.0-2","text":"two"},{"id":"MQTT-1.0.0-3","text":"three"},{"id":"MQTT-1.0.0-4","text":"four"}]`)
	write("statements/mqtt311.json", `[{"id":"MQTT-2.0.0-1","text":"one"},{"id":"MQTT-1.0.0-1","text":"the same number, 3.1.1"}]`)
	write("statements/scope.json", `{"5.0":{"MQTT-1.0.0-3":{"status":"n/a-client","reason":"client only"}},"3.1.1":{"MQTT-1.0.0-1":{"status":"n/a-client","reason":"3.1.1 client only"}}}`)
	write("results.json", `{"5.0":{"MQTT-1.0.0-1":{"tests":["TestZ","TestA"],"pass":true},"MQTT-1.0.0-2":{"tests":["TestB"],"pass":false}}}`)
	out := filepath.Join(dir, "COVERAGE.md")
	if err := run(dir, out); err != nil {
		t.Fatal(err)
	}
	first, _ := os.ReadFile(out)
	if err := run(dir, out); err != nil {
		t.Fatal(err)
	}
	second, _ := os.ReadFile(out)
	if string(first) != string(second) {
		t.Fatal("two renders differ")
	}
	body := string(first)
	for _, want := range []string{
		"4 statements: 1 pass, 1 fail, 1 not-covered, 1 n/a-client.",
		"2 statements: 0 pass, 0 fail, 1 not-covered, 1 n/a-client.",
		"| MQTT-1.0.0-1 | n/a-client | 3.1.1 client only | the same number, 3.1.1 |",
		"| MQTT-1.0.0-1 | pass | TestA, TestZ | A \\| B |",
		"| MQTT-1.0.0-2 | fail | TestB | two |",
		"| MQTT-1.0.0-3 | n/a-client | client only | three |",
		"| MQTT-1.0.0-4 | not-covered |  | four |",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in\n%s", want, body)
		}
	}
	if strings.Contains(body, "External suites") {
		t.Error("external section rendered without expected.json")
	}
	// A results file keyed by bare statement identifiers is refused.
	write("results.json", `{"MQTT-1.0.0-1":{"tests":["TestA"],"pass":true}}`)
	if err := run(dir, out); err == nil || !strings.Contains(err.Error(), "results.json") {
		t.Errorf("a bare-keyed results file: %v, want a refusal naming results.json", err)
	}
	// So is one whose member is not a specification.
	write("results.json", `{"5":{"MQTT-1.0.0-1":{"tests":["TestA"],"pass":true}}}`)
	if err := run(dir, out); err == nil || !strings.Contains(err.Error(), `member "5" is not a specification`) {
		t.Errorf("a results file keyed \"5\": %v, want a refusal naming the member", err)
	}
}

// The repository's own COVERAGE.md, rendered from the committed results:
// no MQTT 5.0 statement is credited by a test that runs at 3.1.1 or 3.1.
// Those tests are named for their level: a 311 suffix, a 3.1.1 subtest, the
// level 3 identifier rules.
func TestRepositoryCreditsNo50StatementFrom311Tests(t *testing.T) {
	out := filepath.Join(t.TempDir(), "COVERAGE.md")
	if err := run(filepath.Join("..", ".."), out); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	body := string(data)
	start := strings.Index(body, "## MQTT 5.0")
	end := strings.Index(body, "## MQTT 3.1.1")
	if start < 0 || end < start {
		t.Fatal("no MQTT 5.0 section before the 3.1.1 one")
	}
	level311 := regexp.MustCompile(`311$|/3\.1\.1|at_3\.1\.1|level_3`)
	rows := 0
	for _, line := range strings.Split(body[start:end], "\n") {
		cells := strings.Split(line, " | ")
		if !strings.HasPrefix(line, "| MQTT-") || len(cells) < 3 {
			continue
		}
		rows++
		for _, test := range strings.Split(cells[2], ", ") {
			if level311.MatchString(strings.TrimSpace(test)) {
				t.Errorf("5.0 %s is credited by the 3.1.1 test %s", strings.TrimPrefix(cells[0], "| "), test)
			}
		}
	}
	if rows < 200 {
		t.Fatalf("read %d rows of the 5.0 table; the render is not the repository's", rows)
	}
}
