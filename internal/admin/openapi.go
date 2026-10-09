package admin

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"
)

// OpenAPI generates the OpenAPI 3.1 document from the route table: one
// operation per route, its parameters, its request and response schemas
// derived from the Go types by reflection, its scope and whether it is
// audited.
func OpenAPI(table []Route) ([]byte, error) {
	g := &generator{schemas: map[string]any{}}
	paths := map[string]map[string]any{}
	seen := map[string]bool{}
	for _, rt := range table {
		op := map[string]any{
			"summary":         rt.Summary,
			"operationId":     operationID(rt),
			"x-hoardmq-scope": string(rt.Scope),
			"security":        []any{map[string]any{"bearer": []any{}}, map[string]any{"mtls": []any{}}},
		}
		if rt.Action != "" {
			op["x-hoardmq-audit"] = rt.Action
		}
		var params []any
		for _, name := range pathParams(rt.Path) {
			params = append(params, map[string]any{"name": name, "in": "path", "required": true, "schema": map[string]any{"type": "string"},
				"description": "URL-escaped; a / in the value is %2F."})
		}
		for _, q := range rt.Query {
			t := "string"
			if q.Integer {
				t = "integer"
			}
			params = append(params, map[string]any{"name": q.Name, "in": "query", "required": q.Required, "schema": map[string]any{"type": t}, "description": q.Description})
		}
		if len(params) > 0 {
			op["parameters"] = params
		}
		if rt.Body != nil {
			op["requestBody"] = map[string]any{"required": false, "content": map[string]any{"application/json": map[string]any{"schema": g.schema(reflect.TypeOf(rt.Body))}}}
		}
		responses := map[string]any{
			fmt.Sprint(rt.Status): map[string]any{"description": "Success.", "content": map[string]any{"application/json": map[string]any{"schema": g.schema(reflect.TypeOf(rt.Response))}}},
			"401":                 problemResponse("No credential authenticates the request."),
		}
		if rt.Scope != "" {
			responses["403"] = problemResponse("The identity lacks the route's scope.")
		}
		if rt.Body != nil || len(rt.Query) > 0 {
			responses["400"] = problemResponse("A malformed body or parameter.")
		}
		if rt.Body != nil {
			responses["413"] = problemResponse("The body exceeds admin.maxBodyBytes.")
		}
		if len(pathParams(rt.Path)) > 0 {
			responses["404"] = problemResponse("No such client, module or key here.")
			responses["409"] = problemResponse("The client is held by another node (owner names it), or the state refuses the action.")
		}
		if rt.forward {
			responses["409"] = problemResponse("The state refuses the action, or the client is held by another node (owner names it) and the call could not be forwarded there; detail says why.")
			responses["502"] = problemResponse("The owner ran the call but its answer could not be carried back (owner names it).")
			responses["503"] = problemResponse("The owner is running as many forwarded calls as it takes; try again.")
			responses["504"] = problemResponse("The owner did not answer in time (owner names it); the call may have been applied there.")
		}
		for status, desc := range rt.Problems {
			responses[fmt.Sprint(status)] = problemResponse(desc)
		}
		op["responses"] = responses
		p := Prefix + rt.Path
		if paths[p] == nil {
			paths[p] = map[string]any{}
		}
		method := strings.ToLower(rt.Method)
		if _, dup := paths[p][method]; dup {
			return nil, fmt.Errorf("openapi: %s %s appears twice in the route table", rt.Method, p)
		}
		if seen[op["operationId"].(string)] {
			return nil, fmt.Errorf("openapi: operationId %s is not unique", op["operationId"])
		}
		seen[op["operationId"].(string)] = true
		paths[p][method] = op
	}
	g.schemas["Problem"] = g.inline(reflect.TypeOf(Problem{}))
	doc := map[string]any{
		"openapi": "3.1.0",
		"info": map[string]any{
			"title":       "HoardMQ admin API",
			"version":     "1",
			"description": "The admin REST API of one HoardMQ node. Every request is authenticated by a client certificate or a bearer value: a configured token, an API key this node created, or an ID token from the configured OpenID Connect provider (hoardmq login). Every route needs the scope in x-hoardmq-scope (empty: any identity), and every route with x-hoardmq-audit writes that audit record. Client actions act on this node; a client held by another node is answered 409 with the owner.",
		},
		"paths": paths,
		"components": map[string]any{
			"schemas": g.schemas,
			"securitySchemes": map[string]any{
				"bearer": map[string]any{"type": "http", "scheme": "bearer"},
				"mtls":   map[string]any{"type": "mutualTLS"},
			},
		},
	}
	return json.MarshalIndent(doc, "", "  ")
}

var pathParam = regexp.MustCompile(`\{([^}]+)\}`)

func pathParams(path string) []string {
	var out []string
	for _, m := range pathParam.FindAllStringSubmatch(path, -1) {
		out = append(out, m[1])
	}
	return out
}

// operationID is the method and the path's segments, camel-cased, a path
// parameter as By<Name>, so GET /clients and GET /clients/{clientId} differ.
func operationID(rt Route) string {
	var b strings.Builder
	b.WriteString(strings.ToLower(rt.Method))
	for _, seg := range strings.Split(rt.Path, "/") {
		if seg == "" {
			continue
		}
		if strings.HasPrefix(seg, "{") {
			name := strings.Trim(seg, "{}")
			seg = "by" + strings.ToUpper(name[:1]) + name[1:]
		}
		seg = strings.NewReplacer(".", "", "-", "").Replace(seg)
		b.WriteString(strings.ToUpper(seg[:1]) + seg[1:])
	}
	return b.String()
}

func problemResponse(desc string) map[string]any {
	return map[string]any{"description": desc, "content": map[string]any{"application/problem+json": map[string]any{"schema": map[string]any{"$ref": "#/components/schemas/Problem"}}}}
}

type generator struct{ schemas map[string]any }

var (
	timeType = reflect.TypeOf(time.Time{})
	rawType  = reflect.TypeOf(RawDocument{})
	rawMsg   = reflect.TypeOf(json.RawMessage{})
)

// schema returns a JSON Schema for t, registering a named struct under
// components.schemas and referring to it.
func (g *generator) schema(t reflect.Type) any {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() == reflect.Struct && t.Name() != "" && t != timeType && t != rawType {
		if _, ok := g.schemas[t.Name()]; !ok {
			g.schemas[t.Name()] = map[string]any{} // placeholder against recursion
			g.schemas[t.Name()] = g.inline(t)
		}
		return map[string]any{"$ref": "#/components/schemas/" + t.Name()}
	}
	return g.inline(t)
}

func (g *generator) inline(t reflect.Type) any {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t {
	case timeType:
		return map[string]any{"type": "string", "format": "date-time"}
	case rawType, rawMsg:
		return map[string]any{"type": "object"}
	}
	switch t.Kind() {
	case reflect.Bool:
		return map[string]any{"type": "boolean"}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return map[string]any{"type": "integer"}
	case reflect.Float32, reflect.Float64:
		return map[string]any{"type": "number"}
	case reflect.String:
		return map[string]any{"type": "string"}
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return map[string]any{"type": "string", "contentEncoding": "base64"}
		}
		return map[string]any{"type": "array", "items": g.schema(t.Elem())}
	case reflect.Map:
		return map[string]any{"type": "object", "additionalProperties": g.schema(t.Elem())}
	case reflect.Struct:
		props := map[string]any{}
		var required []string
		g.fields(t, props, &required)
		out := map[string]any{"type": "object", "properties": props}
		if len(required) > 0 {
			sort.Strings(required)
			out["required"] = required
		}
		return out
	default:
		return map[string]any{}
	}
}

// fields adds a struct's JSON members, flattening embedded structs as
// encoding/json does. A member without omitempty is always present.
func (g *generator) fields(t reflect.Type, props map[string]any, required *[]string) {
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		tag := f.Tag.Get("json")
		if tag == "-" {
			continue
		}
		name, opts, _ := strings.Cut(tag, ",")
		if f.Anonymous && name == "" && f.Type.Kind() == reflect.Struct {
			g.fields(f.Type, props, required)
			continue
		}
		if name == "" {
			name = f.Name
		}
		props[name] = g.schema(f.Type)
		if !strings.Contains(opts, "omitempty") {
			*required = append(*required, name)
		}
	}
}
