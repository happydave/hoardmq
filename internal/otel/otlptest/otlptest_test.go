package otlptest

import (
	"net/http"
	"strings"
	"testing"
)

const conformant = `{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"hoardmq"}}]},` +
	`"scopeSpans":[{"scope":{"name":"s"},"spans":[{"traceId":"4bf92f3577b34da6a3ce929d0e0e4736","spanId":"00f067aa0ba902b7",` +
	`"parentSpanId":"00f067aa0ba902b8","name":"publish","kind":2,"startTimeUnixNano":"100","endTimeUnixNano":"200",` +
	`"attributes":[{"key":"q","value":{"intValue":"1"}},{"key":"b","value":{"boolValue":true}}],"status":{"code":2,"message":"x"}}]}]}]}`

// TestCollectorRejectsEachMalformedShape: the conformant body passes; each
// change breaks exactly the rule it is named for.
func TestCollectorRejectsEachMalformedShape(t *testing.T) {
	spans, vs, code := Check(http.MethodPost, "/v1/traces", "application/json", []byte(conformant))
	if len(vs) != 0 || code != 200 || len(spans) != 1 {
		t.Fatalf("conformant body: %v, %d, %d spans", vs, code, len(spans))
	}
	s := spans[0]
	if s.Kind != 2 || s.Start != 100 || s.End != 200 || s.Attributes["q"] != "1" || s.StatusCode != 2 || s.Resource["service.name"] != "hoardmq" {
		t.Fatalf("decoded %+v", s)
	}
	cases := []struct {
		name, method, path, ctype, from, to, rule string
		refused                                   bool
	}{
		{"base64 id", "", "", "", `"spanId":"00f067aa0ba902b7"`, `"spanId":"APBnqguqArc="`, "id", true},
		{"a non-hex character", "", "", "", `"spanId":"00f067aa0ba902b7"`, `"spanId":"00f067aa0ba902bz"`, "id", true},
		{"31-character id", "", "", "", `"traceId":"4bf92f3577b34da6a3ce929d0e0e4736"`, `"traceId":"4bf92f3577b34da6a3ce929d0e0e473"`, "id", true},
		{"upper-case id", "", "", "", `"traceId":"4bf92f3577b34da6a3ce929d0e0e4736"`, `"traceId":"4BF92F3577B34DA6A3CE929D0E0E4736"`, "id", false},
		{"all-zero trace id", "", "", "", `"traceId":"4bf92f3577b34da6a3ce929d0e0e4736"`, `"traceId":"00000000000000000000000000000000"`, "id", false},
		{"kind as a name", "", "", "", `"kind":2`, `"kind":"SPAN_KIND_SERVER"`, "enum", false},
		{"kind 7", "", "", "", `"kind":2`, `"kind":7`, "enum", true},
		{"times as numbers", "", "", "", `"startTimeUnixNano":"100"`, `"startTimeUnixNano":100`, "int64", false},
		{"a snake_case key", "", "", "", `"traceId":`, `"trace_id":`, "member", false},
		{"two value members", "", "", "", `{"intValue":"1"}`, `{"intValue":"1","stringValue":"1"}`, "attribute", false},
		{"end before start", "", "", "", `"endTimeUnixNano":"200"`, `"endTimeUnixNano":"50"`, "times", false},
		{"no name", "", "", "", `"name":"publish"`, `"name":""`, "name", false},
		{"wrong path", "", "/v1/logs", "", "", "", "path", false},
		{"text/plain", "", "", "text/plain", "", "", "media type", true},
		{"GET", http.MethodGet, "", "", "", "", "method", true},
		{"not JSON", "", "", "", `{"resourceSpans"`, `{"resourceSpans"}`, "body", true},
	}
	for _, tc := range cases {
		method, path, ctype := http.MethodPost, "/v1/traces", "application/json; charset=utf-8"
		if tc.method != "" {
			method = tc.method
		}
		if tc.path != "" {
			path = tc.path
		}
		if tc.ctype != "" {
			ctype = tc.ctype
		}
		body := conformant
		if tc.from != "" {
			if !strings.Contains(body, tc.from) {
				t.Fatalf("%s: the conformant body has no %s", tc.name, tc.from)
			}
			body = strings.Replace(body, tc.from, tc.to, 1)
		}
		_, vs, code := Check(method, path, ctype, []byte(body))
		found := false
		for _, v := range vs {
			if v.Rule == tc.rule {
				found = true
				if v.Refused != tc.refused {
					t.Errorf("%s: %v refused=%v, want %v", tc.name, v, v.Refused, tc.refused)
				}
			}
		}
		if !found {
			t.Errorf("%s: no %q violation in %v", tc.name, tc.rule, vs)
		}
		if tc.refused && code == 200 {
			t.Errorf("%s: answered 200", tc.name)
		}
	}
}
