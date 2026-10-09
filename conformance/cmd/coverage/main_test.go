package main

import (
	"os"
	"path/filepath"
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
	write("statements/mqtt311.json", `[{"id":"MQTT-2.0.0-1","text":"one"}]`)
	write("statements/scope.json", `{"MQTT-1.0.0-3":{"status":"n/a-client","reason":"client only"}}`)
	write("results.json", `{"MQTT-1.0.0-1":{"tests":["TestZ","TestA"],"pass":true},"MQTT-1.0.0-2":{"tests":["TestB"],"pass":false}}`)
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
		"1 statements: 0 pass, 0 fail, 1 not-covered, 0 n/a-client.",
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
}
