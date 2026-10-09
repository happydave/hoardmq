package admin

import (
	"encoding/json"
	"net/http/httptest"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// The document is generated from the table the server dispatches: its
// operations are the table's exactly, the mux resolves every table entry to
// its own pattern, and every reference resolves.
func TestOpenAPIDerivedFromTheTable(t *testing.T) {
	f := newFixture(t, 0)
	rec := f.do("GET", "/api/v1/openapi.json", opsToken, "")
	if rec.Code != 200 {
		t.Fatalf("GET openapi.json: %d", rec.Code)
	}
	var doc struct {
		OpenAPI    string                               `json:"openapi"`
		Paths      map[string]map[string]map[string]any `json:"paths"`
		Components struct {
			Schemas map[string]any `json:"schemas"`
		} `json:"components"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil {
		t.Fatal(err)
	}
	if doc.OpenAPI != "3.1.0" {
		t.Fatalf("openapi %q", doc.OpenAPI)
	}
	var inDoc, inTable []string
	for path, ops := range doc.Paths {
		for method, op := range ops {
			inDoc = append(inDoc, strings.ToUpper(method)+" "+path)
			if _, ok := op["x-hoardmq-scope"]; !ok {
				t.Errorf("%s %s has no scope", method, path)
			}
		}
	}
	routes := f.srv.Routes()
	for _, rt := range routes {
		inTable = append(inTable, rt.Method+" "+Prefix+rt.Path)
		op := doc.Paths[Prefix+rt.Path][strings.ToLower(rt.Method)]
		if op == nil {
			continue
		}
		if op["x-hoardmq-scope"] != string(rt.Scope) || (rt.Action != "") != (op["x-hoardmq-audit"] == rt.Action) {
			t.Errorf("%s %s: document scope %v audit %v, table %q %q", rt.Method, rt.Path, op["x-hoardmq-scope"], op["x-hoardmq-audit"], rt.Scope, rt.Action)
		}
	}
	sort.Strings(inDoc)
	sort.Strings(inTable)
	if strings.Join(inDoc, "\n") != strings.Join(inTable, "\n") {
		t.Fatalf("document and table differ:\ndocument:\n%s\ntable:\n%s", strings.Join(inDoc, "\n"), strings.Join(inTable, "\n"))
	}
	ids := map[string]string{}
	for path, ops := range doc.Paths {
		for method, op := range ops {
			id, _ := op["operationId"].(string)
			if prev, dup := ids[id]; dup || id == "" {
				t.Errorf("operationId %q of %s %s is also %s's", id, method, path, prev)
			}
			ids[id] = method + " " + path
		}
	}
	if _, ok := doc.Paths[Prefix+"/openapi.json"]["get"]["responses"].(map[string]any)["403"]; ok {
		t.Error("the document's own route, which needs no scope, lists 403")
	}
	if _, ok := doc.Paths[Prefix+"/audit"]["get"]["responses"].(map[string]any)["409"]; !ok {
		t.Error("GET /audit does not document its 409")
	}
	if _, ok := doc.Paths[Prefix+"/cluster/drain"]["post"]["responses"].(map[string]any)["403"]; !ok {
		t.Error("POST /cluster/drain does not document its 403")
	}
	if len(inTable) != 39 {
		t.Errorf("the table has %d routes; the plans name 39", len(inTable))
	}
	for _, want := range []string{"GET /api/v1/whoami", "GET /api/v1/apikeys", "POST /api/v1/apikeys", "DELETE /api/v1/apikeys/{keyId}",
		"GET /api/v1/bans", "POST /api/v1/bans", "DELETE /api/v1/bans/{key}", "GET /api/v1/limits", "PUT /api/v1/limits", "GET /api/v1/overload"} {
		if !strings.Contains(strings.Join(inDoc, "\n")+"\n", want+"\n") {
			t.Errorf("the document lacks %s", want)
		}
	}
	// No example values: a credential cannot reach the document through one.
	if strings.Contains(rec.Body.String(), `"example"`) || strings.Contains(rec.Body.String(), `"examples"`) {
		t.Error("the document carries example values")
	}
	if !strings.Contains(rec.Body.String(), `"key"`) {
		t.Error("the created key's schema is missing, so the example check proves nothing")
	}
	param := regexp.MustCompile(`\{[^}]+\}`)
	for _, rt := range routes {
		req := httptest.NewRequest(rt.Method, Prefix+param.ReplaceAllString(rt.Path, "x"), nil)
		if _, pattern := f.srv.Mux().Handler(req); pattern != rt.Method+" "+Prefix+rt.Path {
			t.Errorf("%s %s dispatches to %q", rt.Method, rt.Path, pattern)
		}
	}
	refs := regexp.MustCompile(`"\$ref":\s*"#/components/schemas/([^"]+)"`).FindAllStringSubmatch(rec.Body.String(), -1)
	if len(refs) == 0 {
		t.Fatal("the document refers to no schema")
	}
	for _, m := range refs {
		if _, ok := doc.Components.Schemas[m[1]]; !ok {
			t.Errorf("unresolved $ref %s", m[1])
		}
	}
}

// A path or method the table does not hold is the mux's 404 or 405, after
// authentication.
func TestUnknownRoutes(t *testing.T) {
	f := newFixture(t, 0)
	if rec := f.do("GET", "/api/v1/nope", opsToken, ""); rec.Code != 404 {
		t.Errorf("unknown path %d", rec.Code)
	}
	if rec := f.do("PUT", "/api/v1/clients", opsToken, ""); rec.Code != 405 {
		t.Errorf("unknown method %d", rec.Code)
	}
}
