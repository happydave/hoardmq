package cli

import (
	"encoding/json"
	"testing"
)

// TestAdminNoWillFlag: -no-will on clients disconnect and clients invalidate
// sends will=false, which the node's audit record notes; without the flag the
// record has no will.
func TestAdminNoWillFlag(t *testing.T) {
	srv, addr, tokenFile := adminNode(t, nil)
	for _, id := range []string{"dropped/d", "dropped/i", "kept/d"} {
		mqttConnect(t, srv, id, "")
	}
	common := []string{"-admin", addr, "-token-file", tokenFile}
	runVerb := func(args ...string) (int, string, string) {
		t.Helper()
		full := append(append(append([]string{}, args[:2]...), common...), args[2:]...)
		return run(t, nil, full...)
	}
	for _, args := range [][]string{
		{"clients", "disconnect", "-no-will", "dropped/d"},
		{"clients", "invalidate", "-no-will", "dropped/i"},
		{"clients", "disconnect", "kept/d"},
	} {
		if code, _, errOut := runVerb(args...); code != ExitOK {
			t.Fatalf("%v: exit %d %q", args, code, errOut)
		}
	}
	code, out, errOut := runVerb("audit", "tail", "-limit", "50", "-json")
	if code != ExitOK {
		t.Fatalf("audit tail: exit %d %q", code, errOut)
	}
	var events []struct {
		Action string            `json:"action"`
		Target string            `json:"target"`
		Detail map[string]string `json:"detail"`
	}
	if err := json.Unmarshal([]byte(out), &events); err != nil {
		t.Fatalf("audit tail -json: %v\n%s", err, out)
	}
	want := map[string]string{"client.disconnect dropped/d": "false", "session.invalidate dropped/i": "false", "client.disconnect kept/d": ""}
	seen := map[string]bool{}
	for _, e := range events {
		k := e.Action + " " + e.Target
		if w, ok := want[k]; ok {
			seen[k] = true
			if e.Detail["will"] != w {
				t.Errorf("%s: audit detail will = %q, want %q", k, e.Detail["will"], w)
			}
		}
	}
	for k := range want {
		if !seen[k] {
			t.Errorf("no audit record for %s in %s", k, out)
		}
	}
}
