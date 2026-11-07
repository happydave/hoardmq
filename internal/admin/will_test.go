package admin

import (
	"encoding/json"
	"testing"
)

// TestWillParameter: both client routes read will — absent or true publishes
// the Will, false drops it and is noted in the audit record, any other value
// is refused before the backend is called.
func TestWillParameter(t *testing.T) {
	for _, rt := range []struct{ method, path, action string }{
		{"POST", "/api/v1/clients/c1/disconnect", "client.disconnect"},
		{"DELETE", "/api/v1/clients/c1/session", "session.invalidate"},
	} {
		for _, c := range []struct {
			query  string
			status int
			called bool
			will   bool
		}{
			{"", 200, true, true},
			{"?will=true", 200, true, true},
			{"?will=false", 200, true, false},
			{"?will=maybe", 400, false, false},
		} {
			f := newFixture(t, 0)
			rec := f.do(rt.method, rt.path+c.query, opsToken, "")
			if rec.Code != c.status {
				t.Fatalf("%s %s%s: %d %s, want %d", rt.method, rt.path, c.query, rec.Code, rec.Body, c.status)
			}
			f.b.mu.Lock()
			wills := append([]bool(nil), f.b.wills...)
			f.b.mu.Unlock()
			if !c.called {
				if len(wills) != 0 {
					t.Fatalf("%s %s%s: the backend was called (%v)", rt.method, rt.path, c.query, wills)
				}
				continue
			}
			if len(wills) != 1 || wills[0] != c.will {
				t.Fatalf("%s %s%s: the backend was given will %v, want [%v]", rt.method, rt.path, c.query, wills, c.will)
			}
			e, _ := f.b.lastAudit(t)
			if e.Action != rt.action {
				t.Fatalf("%s %s%s: last audit %+v", rt.method, rt.path, c.query, e)
			}
			if got, noted := e.Detail["will"]; noted != !c.will || (noted && got != "false") {
				t.Fatalf("%s %s%s: audit detail will = %q (noted %v), want noted only for will=false", rt.method, rt.path, c.query, got, noted)
			}
		}
	}
}

// TestWillParameterDocumented: the OpenAPI document gives both routes a
// boolean will query parameter.
func TestWillParameterDocumented(t *testing.T) {
	f := newFixture(t, 0)
	rec := f.do("GET", "/api/v1/openapi.json", opsToken, "")
	var doc struct {
		Paths map[string]map[string]struct {
			Parameters []struct {
				Name   string `json:"name"`
				In     string `json:"in"`
				Schema struct {
					Type string `json:"type"`
				} `json:"schema"`
			} `json:"parameters"`
		} `json:"paths"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil {
		t.Fatal(err)
	}
	for _, rt := range [][2]string{{"/api/v1/clients/{clientId}/disconnect", "post"}, {"/api/v1/clients/{clientId}/session", "delete"}} {
		found := false
		for _, p := range doc.Paths[rt[0]][rt[1]].Parameters {
			if p.Name == "will" {
				found = p.In == "query" && p.Schema.Type == "boolean"
			}
		}
		if !found {
			t.Errorf("%s %s: no boolean will query parameter in the document", rt[1], rt[0])
		}
	}
}
