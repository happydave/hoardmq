package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/happydave/hoardmq/internal/cluster/records"
	"github.com/happydave/hoardmq/internal/config"
)

// Prefix is where every route lives.
const Prefix = "/api/v1"

// Param is a query parameter a route reads.
type Param struct {
	Name        string
	Description string
	Required    bool
	Integer     bool
}

// Route is one entry of the table: it dispatches, it is authorized by its
// scope, it is audited when it has an action, and the OpenAPI document is
// generated from it. A route cannot be mounted any other way.
type Route struct {
	Method string
	// Path is below Prefix; {name} segments are path parameters.
	Path string
	// Scope is what the principal needs; empty means any authenticated
	// principal.
	Scope Scope
	// Action names the audit record; every route that is not a GET has one.
	Action  string
	Summary string
	Query   []Param
	// Body is the request type, nil for none; RawDocument takes any JSON
	// object.
	Body any
	// Response is the success body's type; Status the success status.
	Response any
	Status   int
	// Problems documents failures beyond those the generator derives.
	Problems map[int]string
	// forward marks a route that acts on one client: a call for a client
	// another node holds is forwarded there.
	forward bool
	// target names what an audit record is about.
	target func(r *http.Request) string
	handle func(r *http.Request) (any, error)
}

// RawDocument marks a body or response that is any JSON object: a
// configuration document.
type RawDocument struct{}

func pathValue(name string) func(*http.Request) string {
	return func(r *http.Request) string { return r.PathValue(name) }
}

func queryValue(name string) func(*http.Request) string {
	return func(r *http.Request) string { return r.URL.Query().Get(name) }
}

// page reads the after and limit parameters: limit defaults to 100 and must
// be between 1 and 1000.
func page(r *http.Request) (after string, limit int, err error) {
	q := r.URL.Query()
	limit = 100
	if v := q.Get("limit"); v != "" {
		n, convErr := strconv.Atoi(v)
		if convErr != nil || n < 1 || n > 1000 {
			return "", 0, BadRequest("limit must be an integer from 1 to 1000")
		}
		limit = n
	}
	return q.Get("after"), limit, nil
}

// decodeBody decodes a JSON request body into v, refusing unknown members;
// an empty body leaves v as it is.
func decodeBody(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return err
		}
		return BadRequest("request body: " + err.Error())
	}
	return nil
}

var pagingParams = []Param{
	{Name: "after", Description: "Cursor: the last item of the previous page."},
	{Name: "limit", Description: "Items per page, 1 to 1000 (default 100).", Integer: true},
}

// routes is the API surface.
func (s *Server) routes() []Route {
	b := s.backend
	node := func(*http.Request) string { return b.Node() }
	return []Route{
		{Method: "GET", Path: "/openapi.json", Summary: "This API's OpenAPI 3.1 document.", Response: RawDocument{}, Status: 200,
			handle: func(*http.Request) (any, error) { return json.RawMessage(s.openapi), nil }},

		{Method: "GET", Path: "/clients", Scope: ClientsRead, Summary: "The clients the cluster holds, online and offline, by client identifier, each naming the node that holds it; a client held on this node carries its session summary.",
			Query: append([]Param{{Name: "search", Description: "Case-sensitive substring of the client identifier."},
				{Name: "node", Description: "Only the clients this node holds."}}, pagingParams...),
			Response: ClientPage{}, Status: 200,
			handle: func(r *http.Request) (any, error) {
				after, limit, err := page(r)
				if err != nil {
					return nil, err
				}
				q := r.URL.Query()
				items, more, err := b.ScanClients(r.Context(), q.Get("search"), after, q.Get("node"), limit)
				if err != nil {
					return nil, err
				}
				out := ClientPage{Items: items}
				if out.Items == nil {
					out.Items = []ClientListItem{}
				}
				if more && len(items) > 0 {
					out.Next = items[len(items)-1].ClientID
				}
				return out, nil
			}},
		{Method: "GET", Path: "/clients/{clientId}", Scope: ClientsRead, Summary: "A client's session, its connection and its lease, from the node that holds it: a call for a client another node holds is forwarded there, and the answer names that node in X-Hoardmq-Node.",
			Response: ClientDetail{}, Status: 200, forward: true,
			handle: func(r *http.Request) (any, error) { return b.Client(r.PathValue("clientId")) }},
		{Method: "GET", Path: "/clients/{clientId}/subscriptions", Scope: ClientsRead, Summary: "A client's subscriptions.",
			Response: []Subscription{}, Status: 200, forward: true,
			handle: func(r *http.Request) (any, error) { return b.Subscriptions(r.PathValue("clientId")) }},
		{Method: "POST", Path: "/clients/{clientId}/disconnect", Scope: ClientsWrite, Action: "client.disconnect",
			Summary:  "Close a client's connection with 0x98 Administrative action. The session is kept; the Will is published as for any server-initiated close.",
			Response: struct{}{}, Status: 200, target: pathValue("clientId"), forward: true,
			handle: func(r *http.Request) (any, error) { return struct{}{}, b.Disconnect(r.PathValue("clientId")) }},
		{Method: "DELETE", Path: "/clients/{clientId}/session", Scope: ClientsWrite, Action: "session.invalidate",
			Summary:  "End a client's session: close its connection, discard its state, release its lease, and publish its Will, since the session ends. A copy whose lease has moved to another node is not discarded: the call goes to the lease's owner. One whose later grant this node has seen is refused with 409.",
			Response: Invalidated{}, Status: 200, target: pathValue("clientId"), forward: true,
			handle: func(r *http.Request) (any, error) { return b.Invalidate(r.Context(), r.PathValue("clientId")) }},
		{Method: "GET", Path: "/subscriptions", Scope: ClientsRead, Summary: "The subscriptions on this node a topic reaches, plain and shared. A view: no shared group advances.",
			Query:    []Param{{Name: "topic", Description: "A topic name, no wildcards.", Required: true}},
			Response: []Matcher{}, Status: 200,
			handle: func(r *http.Request) (any, error) {
				topic := r.URL.Query().Get("topic")
				if topic == "" || strings.ContainsAny(topic, "+#") {
					return nil, BadRequest("topic is required and must not contain wildcards")
				}
				out := b.Matchers(topic)
				if out == nil {
					out = []Matcher{}
				}
				return out, nil
			}},

		{Method: "GET", Path: "/retained", Scope: RetainedRead, Summary: "Retained messages a filter matches, across the cluster, by topic.",
			Query:    append([]Param{{Name: "filter", Description: "A topic filter (default #)."}}, pagingParams...),
			Response: RetainedPage{}, Status: 200,
			handle: func(r *http.Request) (any, error) {
				after, limit, err := page(r)
				if err != nil {
					return nil, err
				}
				filter := r.URL.Query().Get("filter")
				if filter == "" {
					filter = "#"
				}
				items, more := b.Retained(filter, after, limit)
				out := RetainedPage{Items: items}
				if out.Items == nil {
					out.Items = []RetainedMessage{}
				}
				if more && len(items) > 0 {
					out.Next = items[len(items)-1].Topic
				}
				return out, nil
			}},
		{Method: "DELETE", Path: "/retained", Scope: RetainedWrite, Action: "retained.delete",
			Summary:  "Remove the retained message on a topic, as an empty retained publish does, delivering nothing to subscribers.",
			Query:    []Param{{Name: "topic", Description: "A topic name, no wildcards.", Required: true}},
			Response: Durability{}, Status: 200, target: queryValue("topic"),
			handle: func(r *http.Request) (any, error) {
				topic := r.URL.Query().Get("topic")
				if topic == "" || strings.ContainsAny(topic, "+#") {
					return nil, BadRequest("topic is required and must not contain wildcards")
				}
				return b.DeleteRetained(r.Context(), topic)
			}},

		{Method: "GET", Path: "/cluster", Scope: ClusterRead, Summary: "This node's readiness and its view of the cluster: members, feature level, shard map, drain and sealed copies.",
			Response: ClusterState{}, Status: 200,
			handle: func(*http.Request) (any, error) { return b.Cluster(), nil }},
		{Method: "GET", Path: "/cluster/shards", Scope: ClusterRead, Summary: "The current shard map with every shard's leader and followers.",
			Response: ShardMap{}, Status: 200,
			handle: func(*http.Request) (any, error) { return b.Shards(), nil }},
		{Method: "GET", Path: "/cluster/leases/{clientId}", Scope: ClusterRead, Summary: "A client identifier's lease as this node's lease table holds it.",
			Response: Lease{}, Status: 200,
			handle: func(r *http.Request) (any, error) { return b.Lease(r.PathValue("clientId")) }},
		{Method: "GET", Path: "/cluster/drain", Scope: ClusterRead, Summary: "This node's drain status.",
			Response: DrainStatus{}, Status: 200,
			handle: func(*http.Request) (any, error) { return b.DrainStatus(), nil }},
		{Method: "POST", Path: "/cluster/drain", Scope: ClusterDrain, Action: "cluster.drain",
			Summary:  "Start this node's graceful leave; while one runs, report it. The process exits when the drain completes.",
			Response: DrainStatus{}, Status: 202, target: node,
			handle: func(*http.Request) (any, error) { return b.Drain(), nil }},

		{Method: "GET", Path: "/config", Scope: ConfigRead, Summary: "The running configuration, every member naming a password, secret, token or key redacted.",
			Response: RawDocument{}, Status: 200,
			handle: func(*http.Request) (any, error) {
				doc, err := b.RunningConfig()
				if err != nil {
					return nil, err
				}
				return Redact(doc)
			}},
		{Method: "POST", Path: "/config/validate", Scope: ConfigWrite, Action: "config.validate",
			Summary: "Validate a configuration document as hoardmq check does, reporting every problem. Changes nothing; it reads the files the document names, so it needs config:write.",
			Body:    RawDocument{}, Response: ConfigValidation{}, Status: 200, target: node,
			handle: func(r *http.Request) (any, error) {
				var raw json.RawMessage
				if err := decodeBody(r, &raw); err != nil {
					return nil, err
				}
				problems := b.ValidateConfig(raw)
				if problems == nil {
					problems = []ConfigProblem{}
				}
				return ConfigValidation{Valid: len(problems) == 0, Problems: problems}, nil
			}},
		{Method: "POST", Path: "/config/reload", Scope: ConfigWrite, Action: "config.reload",
			Summary:  "Re-read this node's configuration file and put it into force when every change is reloadable; otherwise change nothing and say why. SIGHUP does the same.",
			Response: ConfigReload{}, Status: 200, target: node,
			handle: func(r *http.Request) (any, error) {
				res := b.ReloadConfig()
				switch {
				case res.Applied:
					note(r).detail = map[string]string{"result": "applied", "changed": strings.Join(res.Changed, ",")}
				case res.RestartRequired:
					note(r).detail = map[string]string{"result": "refused"}
				default:
					note(r).detail = map[string]string{"result": "invalid"}
				}
				return res, nil
			}},

		{Method: "GET", Path: "/traces/{clientId}", Scope: TracesRead, Summary: "The recording in progress of a connected client, from the node it is connected to.",
			Response: []TraceEvent{}, Status: 200, forward: true,
			handle: func(r *http.Request) (any, error) { return b.Trace(r.PathValue("clientId")) }},
		{Method: "POST", Path: "/traces/{clientId}", Scope: TracesWrite, Action: "trace.start",
			Summary: "Start recording a connected client's packets in a ring of limit events (default observability.traceEvents).",
			Body:    TraceStartRequest{}, Response: TraceStarted{}, Status: 200, target: pathValue("clientId"), forward: true,
			handle: func(r *http.Request) (any, error) {
				var req TraceStartRequest
				if err := decodeBody(r, &req); err != nil {
					return nil, err
				}
				if req.Limit < 0 || req.Limit > 100000 {
					return nil, BadRequest("limit must be from 1 to 100000")
				}
				return b.StartTrace(r.PathValue("clientId"), req.Limit)
			}},
		{Method: "DELETE", Path: "/traces/{clientId}", Scope: TracesWrite, Action: "trace.stop",
			Summary:  "Stop a recording and return its events.",
			Response: []TraceEvent{}, Status: 200, target: pathValue("clientId"), forward: true,
			handle: func(r *http.Request) (any, error) { return b.StopTrace(r.PathValue("clientId")) }},

		{Method: "GET", Path: "/modules", Scope: ModulesRead, Summary: "The instantiated modules, their state and hook failures.",
			Response: []Module{}, Status: 200,
			handle: func(*http.Request) (any, error) { return b.Modules(), nil }},
		{Method: "POST", Path: "/modules/{moduleId}/enable", Scope: ModulesWrite, Action: "module.enable", Summary: "Enable a toggleable module.",
			Response: Module{}, Status: 200, target: pathValue("moduleId"),
			handle: func(r *http.Request) (any, error) { return s.toggle(r.PathValue("moduleId"), true) }},
		{Method: "POST", Path: "/modules/{moduleId}/disable", Scope: ModulesWrite, Action: "module.disable", Summary: "Disable a toggleable module; every hook family skips it.",
			Response: Module{}, Status: 200, target: pathValue("moduleId"),
			handle: func(r *http.Request) (any, error) { return s.toggle(r.PathValue("moduleId"), false) }},

		{Method: "GET", Path: "/whoami", Summary: "The identity this request authenticated as: its name, credential kind, roles, scopes, groups and expiry.",
			Response: WhoAmI{}, Status: 200,
			handle: func(r *http.Request) (any, error) { return whoAmI(principal(r)), nil }},
		{Method: "GET", Path: "/apikeys", Scope: APIKeysRead, Summary: "The cluster's API keys, revoked and expired ones included. Never a key or its digest.",
			Response: []APIKey{}, Status: 200, Problems: keyStoreProblems,
			handle: func(*http.Request) (any, error) {
				st, err := s.keyRecords()
				if err != nil {
					return nil, err
				}
				return listKeys(st), nil
			}},
		{Method: "POST", Path: "/apikeys", Scope: APIKeysWrite, Action: "apikey.create",
			Summary: "Create an API key, holding the scopes asked for and those of the roles named, none beyond the creator's; an API key cannot create one. The key is in this answer only. It authenticates on every node once that node has applied it.",
			Body:    APIKeyRequest{}, Response: CreatedAPIKey{}, Status: 201, Problems: keyStoreProblems, target: func(*http.Request) string { return "" },
			handle: s.createKey},
		{Method: "DELETE", Path: "/apikeys/{keyId}", Scope: APIKeysWrite, Action: "apikey.revoke",
			Summary:  "Revoke an API key. 200 means no node authenticates it any more: every node the log calls alive has applied the revocation. 503 names those that have not yet; the revocation is written, and asking again confirms it. Revoking a revoked key writes nothing and confirms again.",
			Response: APIKey{}, Status: 200, Problems: keyStoreProblems, target: pathValue("keyId"),
			handle: s.revokeKey},

		{Method: "GET", Path: "/credentials", Scope: CredentialsRead, Summary: "The client credentials in the in-broker database, by username: form, attributes and version, never the password or its hash. The database is the cluster's: every node holds the same.",
			Query: pagingParams, Response: CredentialPage{}, Status: 200, Problems: credentialProblems, handle: s.listCredentials},
		{Method: "GET", Path: "/credentials/{username}", Scope: CredentialsRead, Summary: "One client credential.",
			Response: Credential{}, Status: 200, Problems: credentialProblems, handle: s.getCredential},
		{Method: "POST", Path: "/credentials", Scope: CredentialsWrite, Action: "credential.create",
			Summary: "Add a client credential: a password the node stores as a PBKDF2 hash (or the SCRAM-SHA-256 form with scram), or a passwordHash hash-password wrote. 409 when the username exists. Answers once the write has committed and applied on this node.",
			Body:    CredentialRequest{}, Response: Credential{}, Status: 201, Problems: credentialProblems, target: func(*http.Request) string { return "" },
			handle: s.createCredential},
		{Method: "PUT", Path: "/credentials/{username}", Scope: CredentialsWrite, Action: "credential.update",
			Summary: "Change a client credential's password, its attributes (replaced whole), or both; with ifVersion, only at that version.",
			Body:    CredentialUpdate{}, Response: Credential{}, Status: 200, Problems: credentialProblems, target: pathValue("username"),
			handle: s.updateCredential},
		{Method: "DELETE", Path: "/credentials/{username}", Scope: CredentialsWrite, Action: "credential.delete",
			Summary:  "Remove a client credential; with ifVersion, only at that version.",
			Query:    []Param{{Name: "ifVersion", Description: "Remove only at this version.", Integer: true}},
			Response: Credential{}, Status: 200, Problems: credentialProblems, target: pathValue("username"),
			handle: s.deleteCredential},

		{Method: "GET", Path: "/bans", Scope: BansRead, Summary: "The ban list, by key (<kind>:<value>): what each ban matches, why, until when and who wrote it. The list is the cluster's: every node enforces the same.",
			Query: pagingParams, Response: BanPage{}, Status: 200, Problems: banProblems, handle: s.listBans},
		{Method: "POST", Path: "/bans", Scope: BansWrite, Action: "ban.create",
			Summary: "Ban a client identifier, username, address or prefix, or a regular expression over identifiers or usernames, until expiresAt, for expiresInSeconds, or for good. A matching CONNECT is refused before its credentials are examined. 409 when the key exists.",
			Body:    BanRequest{}, Response: Ban{}, Status: 201, Problems: banProblems, target: func(*http.Request) string { return "" },
			handle: s.createBan},
		{Method: "DELETE", Path: "/bans/{key}", Scope: BansWrite, Action: "ban.delete",
			Summary:  "Remove a ban by its key, URL-escaped (a prefix's / as %2F); with ifVersion, only at that version.",
			Query:    []Param{{Name: "ifVersion", Description: "Remove only at this version.", Integer: true}},
			Response: Ban{}, Status: 200, Problems: banProblems, target: pathValue("key"),
			handle: s.deleteBan},
		{Method: "GET", Path: "/limits", Scope: LimitsRead, Summary: "The per-client PUBLISH rates and the node's connect rate: as configured, as administered, and in force.",
			Query: []Param{{Name: "namespace", Description: "Only default until namespaces are active."}}, Response: LimitsView{}, Status: 200, handle: s.getLimits},
		{Method: "PUT", Path: "/limits", Scope: LimitsWrite, Action: "limits.update",
			Summary: "Replace the administered limits, cluster-wide: each member set overrides the configuration, each absent one falls back to it. A client's next PUBLISH is paced by the rates in force; the node's connect rate applies at once.",
			Query:   []Param{{Name: "namespace", Description: "Only default until namespaces are active."}},
			Body:    config.LimitsOverride{}, Response: LimitsView{}, Status: 200, Problems: map[int]string{503: "The cluster did not confirm the write."},
			target: func(r *http.Request) string { return config.DefaultLimitsKey }, handle: s.putLimits},
		{Method: "GET", Path: "/overload", Scope: ClusterRead, Summary: "This node's stress level and indicators, the level each peer reports, the cluster level enforced, and how many connections are paused.",
			Response: OverloadView{}, Status: 200, handle: func(*http.Request) (any, error) { return s.backend.Overload(), nil }},

		{Method: "GET", Path: "/audit", Scope: AuditRead, Summary: "The most recent audit events, oldest first, after the writer has flushed.",
			Query:    []Param{{Name: "limit", Description: "Events, 1 to 1000 (default 100).", Integer: true}},
			Response: []AuditEvent{}, Status: 200, Problems: map[int]string{409: "The audit log is off (observability.audit.enabled)."},
			handle: func(r *http.Request) (any, error) {
				_, limit, err := page(r)
				if err != nil {
					return nil, err
				}
				return b.AuditTail(limit)
			}},
	}
}

var (
	errNoKeyStore    = Conflict("API keys are not available until this node's consensus core is open")
	keyStoreProblems = map[int]string{409: "This node's consensus core is not open, or the key changed while it was written.",
		503: "The cluster did not confirm the write, or a node the log calls alive has not confirmed a revocation."}
)

func whoAmI(p Principal) WhoAmI {
	out := WhoAmI{Name: p.Name, Method: p.Method, Roles: p.Roles, Scopes: p.ScopeList(), Groups: p.Groups}
	if !p.Expires.IsZero() {
		e := p.Expires
		out.Expires = &e
	}
	return out
}

// keyRecords is the cluster's records store, or the missing store's answer.
func (s *Server) keyRecords() (RecordStore, error) {
	st := s.backend.Records()
	if st == nil {
		return nil, errNoKeyStore
	}
	return st, nil
}

// createKey makes a key from the request, after mayGrant allows it, and
// writes it only where no key holds its identifier.
func (s *Server) createKey(r *http.Request) (any, error) {
	st, err := s.keyRecords()
	if err != nil {
		return nil, err
	}
	var req APIKeyRequest
	if err := decodeBody(r, &req); err != nil {
		return nil, err
	}
	name := strings.TrimSpace(req.Name)
	switch {
	case name == "" || utf8.RuneCountInString(name) > 64:
		return nil, BadRequest("name must be 1 to 64 characters")
	case len(req.Scopes) == 0 && len(req.Roles) == 0:
		return nil, BadRequest("at least one scope or role is required")
	case req.ExpiresInSeconds < 0 || req.ExpiresInSeconds > 0 && req.ExpiresInSeconds < 60:
		return nil, BadRequest("expiresInSeconds must be 0 (no expiry) or at least 60")
	}
	want, err := s.roles.Grant(req.Scopes, req.Roles)
	if err != nil {
		return nil, BadRequest(err.Error())
	}
	creator := principal(r)
	scopes := Principal{Scopes: want}.ScopeList()
	n := note(r)
	n.detail = map[string]string{"name": name, "scopes": strings.Join(scopes, ",")}
	if msg := mayGrant(creator, want); msg != "" {
		return nil, &Error{Status: http.StatusForbidden, Detail: msg}
	}
	now := s.now().UTC()
	ctx, cancel := context.WithTimeout(r.Context(), s.writeTimeout)
	defer cancel()
	absent := uint64(0)
	for attempt := 0; attempt < 2; attempt++ {
		id, key := newAPIKey()
		rec := APIKey{ID: id, Name: name, Scopes: scopes, Roles: req.Roles, CreatedBy: creator.Name, CreatedAt: now}
		if req.ExpiresInSeconds > 0 {
			at := now.Add(time.Duration(req.ExpiresInSeconds) * time.Second)
			rec.ExpiresAt = &at
		}
		value, err := json.Marshal(storedKey{APIKey: rec, Digest: keyDigest(key)})
		if err != nil {
			return nil, err
		}
		res, err := st.Put(ctx, APIKeysCollection, id, value, &absent, creator.Name, now)
		if err == nil && res.Reason == records.RefusedExists {
			continue
		}
		if err := keyWriteError(res, err, id); err != nil {
			return nil, err
		}
		n.target = id
		return CreatedAPIKey{APIKey: rec, Key: key}, nil
	}
	return nil, errors.New("two new API key identifiers were both taken")
}

// revokeKey writes a key's revocation, unless it is already revoked, and
// answers 200 only once every node the log calls alive has applied it.
func (s *Server) revokeKey(r *http.Request) (any, error) {
	st, err := s.keyRecords()
	if err != nil {
		return nil, err
	}
	id := r.PathValue("keyId")
	k, version, found := getKey(st, id)
	if !found {
		return nil, NotFound("no API key " + id)
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.writeTimeout)
	defer cancel()
	index := version
	if k.RevokedAt == nil {
		at := s.now().UTC()
		k.RevokedAt, k.RevokedBy = &at, principal(r).Name
		value, err := json.Marshal(k)
		if err != nil {
			return nil, err
		}
		res, err := st.Put(ctx, APIKeysCollection, id, value, &version, principal(r).Name, at)
		if err := keyWriteError(res, err, id); err != nil {
			return nil, err
		}
		index = res.Index
	}
	unconfirmed, err := s.backend.ConfirmApplied(ctx, index)
	if err != nil {
		return nil, err
	}
	if len(unconfirmed) > 0 {
		sort.Strings(unconfirmed)
		return nil, &Error{Status: http.StatusServiceUnavailable, Detail: fmt.Sprintf("API key %s's revocation is written at index %d, but %s did not confirm it in time; "+
			"each refuses the key once it applies the revocation or the log declares it dead. Revoke it again to confirm", id, index, strings.Join(unconfirmed, ", "))}
	}
	return k.APIKey, nil
}

func (s *Server) toggle(id string, enabled bool) (any, error) {
	if err := s.backend.SetModuleEnabled(id, enabled); err != nil {
		return nil, err
	}
	for _, m := range s.backend.Modules() {
		if m.ID == id {
			return m, nil
		}
	}
	return nil, NotFound("module " + id + " is not instantiated")
}
