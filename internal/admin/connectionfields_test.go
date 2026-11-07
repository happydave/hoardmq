package admin

import (
	"encoding/json"
	"testing"
)

// TestConnectionSchemaHasKeepAliveAndCleanStart: the OpenAPI document's
// Connection schema lists the keep alive in force, the one asked for, and
// Clean Start.
func TestConnectionSchemaHasKeepAliveAndCleanStart(t *testing.T) {
	f := newFixture(t, 0)
	rec := f.do("GET", "/api/v1/openapi.json", opsToken, "")
	var doc struct {
		Components struct {
			Schemas map[string]struct {
				Properties map[string]struct {
					Type string `json:"type"`
				} `json:"properties"`
			} `json:"schemas"`
		} `json:"components"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil {
		t.Fatal(err)
	}
	props := doc.Components.Schemas["Connection"].Properties
	for name, typ := range map[string]string{"keepAlive": "integer", "requestedKeepAlive": "integer", "cleanStart": "boolean"} {
		if got := props[name].Type; got != typ {
			t.Errorf("Connection.%s is %q in the document, want %q", name, got, typ)
		}
	}
}
