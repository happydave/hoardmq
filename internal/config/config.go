// Package config loads the HoardMQ configuration document: a JSON file
// validated against the published schema, with environment-variable
// overrides applied to the document before validation so that every problem,
// whatever its source, is reported in one pass.
package config

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/happydave/hoardmq/internal/jsonschema"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/schema"
)

// EnvPrefix is the prefix of every environment variable the loader reads.
const EnvPrefix = "HOARDMQ_"

// EnvConfig names the document path when the -config flag is absent.
const EnvConfig = EnvPrefix + "CONFIG"

// DefaultPath is the document path when neither the flag nor EnvConfig is set.
const DefaultPath = "hoardmq.json"

// Document is configuration format version 1.
type Document struct {
	Version       int                        `json:"version"`
	Node          Node                       `json:"node"`
	Listeners     []Listener                 `json:"listeners,omitempty"`
	Cluster       Cluster                    `json:"cluster"`
	Limits        Limits                     `json:"limits"`
	Modules       map[string]json.RawMessage `json:"modules,omitempty"`
	Log           Log                        `json:"log"`
	MQTT          MQTT                       `json:"mqtt"`
	Auth          Auth                       `json:"auth"`
	Hooks         Hooks                      `json:"hooks"`
	Overload      Overload                   `json:"overload"`
	Flapping      Flapping                   `json:"flapping"`
	Observability Observability              `json:"observability"`
	Admin         Admin                      `json:"admin"`
}

// Admin configures the admin REST API. An empty Address binds nothing.
type Admin struct {
	Address      string             `json:"address,omitempty"`
	TLS          *TLS               `json:"tls,omitempty"`
	Roles        []AdminRole        `json:"roles,omitempty"`
	Tokens       []AdminToken       `json:"tokens,omitempty"`
	Certificates []AdminCertificate `json:"certificates,omitempty"`
	OIDC         *AdminOIDC         `json:"oidc,omitempty"`
	MaxBodyBytes int64              `json:"maxBodyBytes"`
}

// AdminRole is a named set of scopes that identities and identity-provider
// groups are granted by name.
type AdminRole struct {
	Name   string   `json:"name"`
	Scopes []string `json:"scopes"`
}

// AdminToken is a bearer-token identity. The token is the trimmed content of
// TokenFile; the document never holds it.
type AdminToken struct {
	Name      string   `json:"name"`
	TokenFile string   `json:"tokenFile"`
	Scopes    []string `json:"scopes,omitempty"`
	Roles     []string `json:"roles,omitempty"`
}

// AdminCertificate is a client-certificate identity, by common name.
type AdminCertificate struct {
	CommonName string   `json:"commonName"`
	Scopes     []string `json:"scopes,omitempty"`
	Roles      []string `json:"roles,omitempty"`
}

// AdminOIDC accepts ID tokens an OpenID Connect provider issued as admin
// identities, their groups mapped to roles.
type AdminOIDC struct {
	Issuer        string             `json:"issuer"`
	ClientIDs     []string           `json:"clientIds"`
	UsernameClaim string             `json:"usernameClaim,omitempty"`
	GroupsClaim   string             `json:"groupsClaim,omitempty"`
	RoleMappings  []AdminRoleMapping `json:"roleMappings"`
	CAFile        string             `json:"caFile,omitempty"`
	// ClockSkewSeconds and MaxTokenLifetimeSeconds are nil when absent:
	// DefaultClockSkewSeconds and DefaultMaxTokenLifetimeSeconds apply.
	ClockSkewSeconds        *int `json:"clockSkewSeconds,omitempty"`
	MaxTokenLifetimeSeconds *int `json:"maxTokenLifetimeSeconds,omitempty"`
}

// AdminRoleMapping grants roles to members of an identity-provider group.
type AdminRoleMapping struct {
	Group string   `json:"group"`
	Roles []string `json:"roles"`
}

// OIDC defaults.
const (
	DefaultUsernameClaim           = "sub"
	DefaultGroupsClaim             = "groups"
	DefaultClockSkewSeconds        = 60
	DefaultMaxTokenLifetimeSeconds = 86400
)

// Username is the claim naming the identity.
func (o AdminOIDC) Username() string {
	if o.UsernameClaim == "" {
		return DefaultUsernameClaim
	}
	return o.UsernameClaim
}

// Groups is the claim listing the identity's groups.
func (o AdminOIDC) Groups() string {
	if o.GroupsClaim == "" {
		return DefaultGroupsClaim
	}
	return o.GroupsClaim
}

// Skew is the clock skew allowed on a token's times.
func (o AdminOIDC) Skew() int {
	if o.ClockSkewSeconds == nil {
		return DefaultClockSkewSeconds
	}
	return *o.ClockSkewSeconds
}

// MaxLifetime is the longest exp - iat accepted.
func (o AdminOIDC) MaxLifetime() int {
	if o.MaxTokenLifetimeSeconds == nil {
		return DefaultMaxTokenLifetimeSeconds
	}
	return *o.MaxTokenLifetimeSeconds
}

// MinAdminTokenBytes is the shortest admin token accepted, the cluster
// secret's floor.
const MinAdminTokenBytes = 16

// Client environment: the admin CLI verbs read these, and the configuration
// loader does not treat them as overrides.
const (
	EnvAdminToken   = EnvPrefix + "ADMIN_TOKEN"
	EnvAdminAddress = EnvPrefix + "ADMIN_ADDRESS"
)

// ReadAdminToken reads a token file and returns its trimmed content.
func ReadAdminToken(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return bytes.TrimSpace(data), nil
}

// Observability configures the metrics listener, the audit log and traces.
type Observability struct {
	Address     string `json:"address,omitempty"`
	AllowRemote bool   `json:"allowRemote"`
	MetricsPath string `json:"metricsPath"`
	TraceEvents int    `json:"traceEvents"`
	Audit       Audit  `json:"audit"`
}

// Audit configures the audit log.
type Audit struct {
	Enabled  bool  `json:"enabled"`
	MaxBytes int64 `json:"maxBytes"`
	MaxFiles int   `json:"maxFiles"`
	Queue    int   `json:"queue"`
}

// Hooks is the module hook policy.
type Hooks struct {
	TimeoutMs int    `json:"timeoutMs"`
	Fallback  string `json:"fallback"`
}

// Auth holds the authentication defaults that are not a module's own.
type Auth struct {
	Anonymous bool `json:"anonymous"`
	// DecisionTTLSeconds bounds how long an authenticator's allow is held
	// before it is made again without the client.
	DecisionTTLSeconds int `json:"decisionTtlSeconds"`
	// Argon2MemoryBytes is the memory the node's Argon2id verifications may
	// hold at once.
	Argon2MemoryBytes int64 `json:"argon2MemoryBytes"`
	// Cache is the node's cache of password verifications, shared by every
	// connection and every password authenticator.
	Cache AuthCache `json:"cache"`
}

// AuthCache bounds the password-verification cache. TTLSeconds 0 turns it
// off; derivations are still counted.
type AuthCache struct {
	TTLSeconds int `json:"ttlSeconds"`
	MaxEntries int `json:"maxEntries"`
}

// Node identifies this node and where it keeps its state.
type Node struct {
	Name              string `json:"name,omitempty"`
	DataDir           string `json:"dataDir"`
	WALSync           string `json:"walSync"`
	WALSyncIntervalMs int    `json:"walSyncIntervalMs"`
	// WALGroupMaxDelayMs is how long a commit-policy sync may hold to let
	// concurrent callers share its fsync; 0 (the default) never holds and
	// groups only the callers that arrive while an fsync is in flight.
	// WALGroupMaxSize fires a held group early once that many callers have
	// joined; it only matters when the delay is non-zero.
	WALGroupMaxDelayMs int   `json:"walGroupMaxDelayMs"`
	WALGroupMaxSize    int   `json:"walGroupMaxSize"`
	WALSegmentBytes    int64 `json:"walSegmentBytes"`
	// SessionShards is how many slices the session map is split into,
	// rounded up to a power of two. One lock for the whole map made every
	// connect, disconnect and inbound packet queue behind every other
	// (WI 1292 round 5).
	SessionShards int `json:"sessionShards"`
	// StoreAdmission bounds how many connects and disconnects are inside the
	// store's write path at once. Before the session map was sharded its
	// single lock bounded this at one as a side effect, and removing that
	// bound entirely was measured catastrophic at 100,000 clients (round 2),
	// so it is set deliberately here.
	StoreAdmission int `json:"storeAdmission"`
}

// Listener is one client-facing listener.
type Listener struct {
	Name           string `json:"name"`
	Type           string `json:"type"`
	Address        string `json:"address"`
	Path           string `json:"path,omitempty"`
	MaxConnections int    `json:"maxConnections"`
	// MaxHandshakes bounds connections accepted but not yet answered with a
	// CONNACK. At the bound the listener stops accepting and the kernel
	// backlog absorbs the rest, so a reconnect storm cannot pile every
	// arrival onto the session manager at once. 0 selects the default (512).
	MaxHandshakes int `json:"maxHandshakes"`
	// ConnectRate is the CONNECTs a second this listener admits, ConnectBurst
	// how many it admits at once; a CONNECT over it is refused before any
	// credential is examined. Rate 0 is unlimited; burst 0 is twice the rate.
	ConnectRate  int  `json:"connectRate"`
	ConnectBurst int  `json:"connectBurst"`
	TLS          *TLS `json:"tls,omitempty"`
}

// DefaultMaxHandshakes is the in-flight handshake bound when a listener
// sets none: enough concurrency to keep the connect path busy, few enough
// waiters that its lock hands off in the regime where it is cheap
// (WI 1292 round 2: the same lock with a million waiters served none).
const DefaultMaxHandshakes = 512

// IsTLS reports whether the listener terminates TLS.
func (l Listener) IsTLS() bool { return l.Type == "tls" || l.Type == "wss" }

// IsWebSocket reports whether the listener speaks MQTT over WebSocket.
func (l Listener) IsWebSocket() bool { return l.Type == "ws" || l.Type == "wss" }

// DefaultWebSocketPath is the upgrade path when a ws or wss listener names none.
const DefaultWebSocketPath = "/mqtt"

// TLS settings for a listener of type "tls".
type TLS struct {
	CertFile     string `json:"certFile"`
	KeyFile      string `json:"keyFile"`
	ClientCAFile string `json:"clientCAFile,omitempty"`
	MinVersion   string `json:"minVersion,omitempty"`
}

// Cluster settings that must agree across nodes, and this node's place in
// the cluster transport.
type Cluster struct {
	ID              string      `json:"id"`
	Listen          string      `json:"listen,omitempty"`
	Replicas        int         `json:"replicas"`
	Shards          int         `json:"shards"`
	Peers           []Peer      `json:"peers,omitempty"`
	Auth            ClusterAuth `json:"auth"`
	HeartbeatMs     int         `json:"heartbeatMs"`
	HeartbeatMisses int         `json:"heartbeatMisses"`
	MaxFrameBytes   int         `json:"maxFrameBytes"`
	Raft            Raft        `json:"raft"`
	Discovery       Discovery   `json:"discovery"`
	Leases          Leases      `json:"leases"`
	Routing         Routing     `json:"routing"`
	Replication     Replication `json:"replication"`
	Drain           Drain       `json:"drain"`
	Readiness       Readiness   `json:"readiness"`
	Advertise       string      `json:"advertise,omitempty"` // client address other nodes hand out in Server Reference
}

// Drain tunes the graceful leave.
type Drain struct {
	RatePerSecond    int  `json:"ratePerSecond"`        // least clients disconnected per second; faster to finish by TimeoutMs less a reserve
	TimeoutMs        int  `json:"timeoutMs"`            // how long to wait for takeovers before handing leases off
	ConfirmTimeoutMs int  `json:"confirmTimeoutMs"`     // how long, after the hand-off, to wait for every session to be confirmed held by its receiver
	OnSignal         bool `json:"onSignal,omitempty"`   // SIGTERM drains before exiting (for setups without a preStop hook)
	HoldExitMs       int  `json:"holdExitMs,omitempty"` // after a drain no signal started, how long to wait for SIGINT or SIGTERM before exiting (for a preStop hook waiting on the drain)
}

// Readiness tunes the readiness signal.
type Readiness struct {
	StableMs int `json:"stableMs"` // topology must be unchanged this long; 0 disables the condition
}

// Replication tunes the data-plane replication layer.
type Replication struct {
	AckTimeoutMs int `json:"ackTimeoutMs"` // how long a QoS 1 or 2 acknowledgement waits for the followers
}

// Routing tunes the routing plane.
type Routing struct {
	SyncMs int `json:"syncMs"` // full route-table exchange interval
}

// Leases tunes the client-identifier lease service.
type Leases struct {
	TTLMs             int `json:"ttlMs"`
	TakeoverTimeoutMs int `json:"takeoverTimeoutMs"`
}

// Raft tunes the consensus core.
type Raft struct {
	TickMs           int   `json:"tickMs"`
	ElectionTicks    int   `json:"electionTicks"`
	HeartbeatTicks   int   `json:"heartbeatTicks"`
	SnapshotEntries  int   `json:"snapshotEntries"`
	SnapshotTrailing int   `json:"snapshotTrailing"`
	PromoteLag       int   `json:"promoteLag"`
	JoinTimeoutMs    int   `json:"joinTimeoutMs"`
	Bootstrap        *bool `json:"bootstrap,omitempty"` // nil: the lowest name bootstraps when nobody answers
}

// Discovery says where the peer set comes from: "static" (cluster.peers) or
// "dns" (a StatefulSet's headless Service).
type Discovery struct {
	Mode string        `json:"mode"`
	DNS  *DNSDiscovery `json:"dns,omitempty"`
}

// DNSDiscovery derives peers <name>-<i>.<service>:<port> for every ordinal
// below replicas; this node's own ordinal comes from node.name.
type DNSDiscovery struct {
	Service  string `json:"service"`
	Name     string `json:"name"`
	Replicas int    `json:"replicas"`
	Port     int    `json:"port"`
}

// Peer is another node of the cluster: its name and transport address.
type Peer struct {
	Name    string `json:"name"`
	Address string `json:"address"`
}

// ClusterAuth says how peers authenticate each other: "mtls" (certificates
// from the cluster CA) or "secret" (a shared secret proved by HMAC; the
// connection is authenticated but not encrypted).
type ClusterAuth struct {
	Mode       string      `json:"mode,omitempty"`
	TLS        *ClusterTLS `json:"tls,omitempty"`
	SecretFile string      `json:"secretFile,omitempty"`
}

// ClusterTLS names this node's certificate and the cluster CA.
type ClusterTLS struct {
	CertFile string `json:"certFile"`
	KeyFile  string `json:"keyFile"`
	CAFile   string `json:"caFile"`
}

// Limits are the per-node protocol limits.
type Limits struct {
	MaxPacketSize     int    `json:"maxPacketSize"`
	ReceiveMaximum    int    `json:"receiveMaximum"`
	MaxQueuedMessages int    `json:"maxQueuedMessages"`
	MaxKeepAlive      int    `json:"maxKeepAlive"`
	ConnectTimeout    int    `json:"connectTimeout"`
	QueueDrop         string `json:"queueDrop"`
	MaxSessionExpiry  int64  `json:"maxSessionExpiry"`
	// ConnectRate and ConnectBurst are the node's connect-rate limit across
	// every listener, as Listener's are for one.
	ConnectRate  int `json:"connectRate"`
	ConnectBurst int `json:"connectBurst"`
	// Each client's PUBLISH rate, in packets and in topic and payload bytes;
	// over it the broker stops reading the client's socket until the rate
	// allows. Rate 0 is unlimited; burst 0 is twice the rate.
	ClientMessageRate  int `json:"clientMessageRate"`
	ClientMessageBurst int `json:"clientMessageBurst"`
	ClientByteRate     int `json:"clientByteRate"`
	ClientByteBurst    int `json:"clientByteBurst"`
}

// LimitsCollection is the replicated records collection administered limits
// live in, one record per namespace; only DefaultLimitsKey exists until
// namespaces do (WI 1322).
const (
	LimitsCollection = "limits"
	DefaultLimitsKey = "default"
)

// LimitsOverride is an administered record over the configured limits; an
// absent member keeps the configured value.
type LimitsOverride struct {
	ClientMessageRate  *int `json:"clientMessageRate,omitempty"`
	ClientMessageBurst *int `json:"clientMessageBurst,omitempty"`
	ClientByteRate     *int `json:"clientByteRate,omitempty"`
	ClientByteBurst    *int `json:"clientByteBurst,omitempty"`
	ConnectRate        *int `json:"connectRate,omitempty"`
	ConnectBurst       *int `json:"connectBurst,omitempty"`
}

// With is the limits in force: l with the override's members laid over it.
func (l Limits) With(o LimitsOverride) Limits {
	for _, p := range []struct {
		dst *int
		src *int
	}{{&l.ClientMessageRate, o.ClientMessageRate}, {&l.ClientMessageBurst, o.ClientMessageBurst}, {&l.ClientByteRate, o.ClientByteRate},
		{&l.ClientByteBurst, o.ClientByteBurst}, {&l.ConnectRate, o.ConnectRate}, {&l.ConnectBurst, o.ConnectBurst}} {
		if p.src != nil {
			*p.dst = *p.src
		}
	}
	return l
}

// CheckOverride is the rule an administered record meets: no member below 0,
// and no burst in force without its rate, given the configured limits.
func CheckOverride(l Limits, o LimitsOverride) string {
	for _, v := range []*int{o.ClientMessageRate, o.ClientMessageBurst, o.ClientByteRate, o.ClientByteBurst, o.ConnectRate, o.ConnectBurst} {
		if v != nil && *v < 0 {
			return "a limit cannot be negative"
		}
	}
	e := l.With(o)
	switch {
	case e.ClientMessageBurst > 0 && e.ClientMessageRate == 0:
		return "a clientMessageBurst needs a clientMessageRate"
	case e.ClientByteBurst > 0 && e.ClientByteRate == 0:
		return "a clientByteBurst needs a clientByteRate"
	case e.ConnectBurst > 0 && e.ConnectRate == 0:
		return "a connectBurst needs a connectRate"
	}
	return ""
}

// Overload holds the limits the node's stress level is measured against; 0
// leaves an indicator out.
type Overload struct {
	QueuedMessages int   `json:"queuedMessages"`
	ReplicationLag int   `json:"replicationLag"`
	MemoryBytes    int64 `json:"memoryBytes"`
}

// Flapping configures the detector whose verdict is a ban by client
// identifier.
type Flapping struct {
	Enabled       bool `json:"enabled"`
	WindowSeconds int  `json:"windowSeconds"`
	MaxCount      int  `json:"maxCount"`
	BanSeconds    int  `json:"banSeconds"`
}

// Burst is the burst a rate is admitted with: the one set, else twice the
// rate.
func Burst(rate, burst int) int {
	if burst > 0 {
		return burst
	}
	return 2 * rate
}

// Log settings.
type Log struct {
	Level  string `json:"level"`
	Format string `json:"format"`
}

// SlogLevel is Level as a log/slog level; an unknown name is Info.
func (l Log) SlogLevel() slog.Level {
	switch l.Level {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	}
	return slog.LevelInfo
}

// MQTT holds the MQTT 5.0 capability switches advertised in CONNACK.
type MQTT struct {
	MaximumQoS                       int    `json:"maximumQoS"`
	RetainAvailable                  bool   `json:"retainAvailable"`
	WildcardSubscriptionAvailable    bool   `json:"wildcardSubscriptionAvailable"`
	SubscriptionIdentifiersAvailable bool   `json:"subscriptionIdentifiersAvailable"`
	SharedSubscriptionAvailable      bool   `json:"sharedSubscriptionAvailable"`
	TopicAliasMaximum                int    `json:"topicAliasMaximum"`
	SharedSubscriptionStrategy       string `json:"sharedSubscriptionStrategy"`
}

// Capabilities converts the switches into what the codec advertises.
func (m MQTT) Capabilities() packet.Capabilities {
	return packet.Capabilities{
		MaximumQoS:                       byte(m.MaximumQoS),
		RetainAvailable:                  m.RetainAvailable,
		WildcardSubscriptionAvailable:    m.WildcardSubscriptionAvailable,
		SubscriptionIdentifiersAvailable: m.SubscriptionIdentifiersAvailable,
		SharedSubscriptionAvailable:      m.SharedSubscriptionAvailable,
		TopicAliasMaximum:                uint16(m.TopicAliasMaximum),
	}
}

// Default returns a document with every defaulted member set and the required
// members empty.
func Default() Document {
	return Document{
		Version: 1,
		Node: Node{WALSync: "commit", WALSyncIntervalMs: 100, WALSegmentBytes: 64 << 20, WALGroupMaxDelayMs: 0, WALGroupMaxSize: 256,
			SessionShards: 64, StoreAdmission: 64},
		Cluster: Cluster{ID: "hoardmq", Replicas: 2, Shards: 64, HeartbeatMs: 1000, HeartbeatMisses: 3, MaxFrameBytes: 16 << 20,
			Raft:        Raft{TickMs: 100, ElectionTicks: 10, HeartbeatTicks: 1, SnapshotEntries: 10000, SnapshotTrailing: 1000, PromoteLag: 100, JoinTimeoutMs: 30000},
			Discovery:   Discovery{Mode: "static"},
			Leases:      Leases{TTLMs: 30000, TakeoverTimeoutMs: 5000},
			Routing:     Routing{SyncMs: 30000},
			Replication: Replication{AckTimeoutMs: 2000},
			Drain:       Drain{RatePerSecond: 100, TimeoutMs: 30000, ConfirmTimeoutMs: 30000},
			Readiness:   Readiness{StableMs: 5000}},
		Limits:        Limits{MaxPacketSize: 1048576, ReceiveMaximum: 1024, MaxQueuedMessages: 1000, ConnectTimeout: 10, QueueDrop: "oldest"},
		Auth:          Auth{DecisionTTLSeconds: 3600, Argon2MemoryBytes: 256 << 20, Cache: AuthCache{TTLSeconds: 60, MaxEntries: 100000}},
		Log:           Log{Level: "info", Format: "text"},
		Hooks:         Hooks{Fallback: "continue"},
		Overload:      Overload{QueuedMessages: 1000000, ReplicationLag: 400000},
		Flapping:      Flapping{Enabled: true, WindowSeconds: 60, MaxCount: 15, BanSeconds: 300},
		Observability: Observability{MetricsPath: "/metrics", TraceEvents: 1000, Audit: Audit{Enabled: true, MaxBytes: 64 << 20, MaxFiles: 10, Queue: 8192}},
		Admin:         Admin{MaxBodyBytes: 1 << 20},
		MQTT: MQTT{
			MaximumQoS:                       2,
			RetainAvailable:                  true,
			WildcardSubscriptionAvailable:    true,
			SubscriptionIdentifiersAvailable: true,
			SharedSubscriptionAvailable:      true,
			TopicAliasMaximum:                65535,
			SharedSubscriptionStrategy:       "round-robin",
		},
	}
}

// Problem is one thing wrong with a document.
type Problem = jsonschema.Problem

var compiled = mustCompile()

func mustCompile() *jsonschema.Schema {
	s, err := jsonschema.Compile(schema.Bytes)
	if err != nil {
		panic(err)
	}
	return s
}

// Schema returns the compiled published schema.
func Schema() *jsonschema.Schema { return compiled }

// Load reads the document at path, applies overrides from environ (in the
// os.Environ form), validates, and returns the document together with every
// problem found. The document is only meaningful when problems is empty. Load
// never creates or modifies anything on the filesystem.
func Load(path string, environ []string) (Document, []Problem) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Document{}, []Problem{{Pointer: "/", Message: readError(err)}}
	}
	return Parse(data, environ)
}

func readError(err error) string {
	var pe *os.PathError
	if errors.As(err, &pe) {
		return pe.Op + " " + pe.Path + ": " + pe.Err.Error()
	}
	return err.Error()
}

// Parse is Load over bytes already in hand.
func Parse(data []byte, environ []string) (Document, []Problem) {
	raw, err := jsonschema.Parse(data)
	if err != nil {
		return Document{}, []Problem{{Pointer: "/", Message: err.Error()}}
	}
	var problems []Problem
	if tree, ok := raw.(map[string]any); ok {
		problems = append(problems, applyEnv(tree, environ)...)
	}
	problems = append(problems, compiled.Validate(raw)...)

	doc := Default()
	merged, err := json.Marshal(raw)
	if err != nil {
		return Document{}, append(problems, Problem{Pointer: "/", Message: err.Error()})
	}
	if err := decodeStrict(merged, &doc); err != nil {
		// The schema has already named every unknown member precisely; a
		// lenient decode lets the semantic rules still run so that all
		// problems arrive together.
		doc = Default()
		if err := json.Unmarshal(merged, &doc); err != nil {
			if len(problems) == 0 {
				problems = append(problems, Problem{Pointer: "/", Message: err.Error()})
			}
			return Document{}, sorted(problems)
		}
	}
	problems = append(problems, doc.semantic()...)
	if len(problems) > 0 {
		return Document{}, sorted(problems)
	}
	return doc, nil
}

func decodeStrict(data []byte, doc *Document) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	return dec.Decode(doc)
}

// Validate checks a document built in code the same way Load checks a file:
// the published schema first, then the rules a schema cannot express.
func (d Document) Validate() []Problem {
	data, err := json.Marshal(d)
	if err != nil {
		return []Problem{{Pointer: "/", Message: err.Error()}}
	}
	raw, err := jsonschema.Parse(data)
	if err != nil {
		return []Problem{{Pointer: "/", Message: err.Error()}}
	}
	problems := compiled.Validate(raw)
	problems = append(problems, d.semantic()...)
	return sorted(problems)
}

// semantic holds the rules the schema does not express.
func (d Document) semantic() []Problem {
	var out []Problem
	if a := d.Observability.Address; a != "" {
		if msg := checkAddress(a); msg != "" {
			out = append(out, Problem{Pointer: "/observability/address", Message: msg})
		} else if host, _, _ := net.SplitHostPort(a); !d.Observability.AllowRemote && !isLoopback(host) {
			out = append(out, Problem{Pointer: "/observability/address", Message: "must bind a loopback address unless allowRemote is true"})
		}
	}
	seen := map[string]bool{}
	for i, l := range d.Listeners {
		at := func(member, msg string) {
			out = append(out, Problem{Pointer: fmt.Sprintf("/listeners/%d/%s", i, member), Message: msg})
		}
		if l.Name != "" {
			if seen[l.Name] {
				at("name", fmt.Sprintf("duplicate listener name %q", l.Name))
			}
			seen[l.Name] = true
		}
		if l.Address != "" {
			if msg := checkAddress(l.Address); msg != "" {
				at("address", msg)
			}
		}
		switch l.Type {
		case "tls", "wss":
			if l.TLS == nil {
				at("tls", "required for a "+l.Type+" listener")
			}
		case "tcp", "ws":
			if l.TLS != nil {
				at("tls", "tls settings on a non-tls listener")
			}
		}
		if l.Path != "" && !l.IsWebSocket() {
			at("path", "path is only meaningful for ws and wss listeners")
		}
		if l.ConnectBurst > 0 && l.ConnectRate == 0 {
			at("connectBurst", "a burst needs a connectRate")
		}
	}
	if d.Limits.ConnectBurst > 0 && d.Limits.ConnectRate == 0 {
		out = append(out, Problem{Pointer: "/limits/connectBurst", Message: "a burst needs a connectRate"})
	}
	if d.Limits.ClientMessageBurst > 0 && d.Limits.ClientMessageRate == 0 {
		out = append(out, Problem{Pointer: "/limits/clientMessageBurst", Message: "a burst needs a clientMessageRate"})
	}
	if d.Limits.ClientByteBurst > 0 && d.Limits.ClientByteRate == 0 {
		out = append(out, Problem{Pointer: "/limits/clientByteBurst", Message: "a burst needs a clientByteRate"})
	}
	out = append(out, d.clusterRules()...)
	out = append(out, d.adminRules()...)
	return out
}

// adminRules are the admin API rules the schema cannot express.
func (d Document) adminRules() []Problem {
	var out []Problem
	a := d.Admin
	at := func(pointer, msg string) { out = append(out, Problem{Pointer: "/admin" + pointer, Message: msg}) }
	if a.Address != "" {
		if msg := checkAddress(a.Address); msg != "" {
			at("/address", msg)
		} else {
			if host, _, _ := net.SplitHostPort(a.Address); !isLoopback(host) && a.TLS == nil {
				at("/tls", "required when the admin address is not loopback: a bearer token must not cross a network in clear text")
			}
			others := []struct{ pointer, addr string }{{"/observability/address", d.Observability.Address}, {"/cluster/listen", d.Cluster.Listen}}
			for i, l := range d.Listeners {
				others = append(others, struct{ pointer, addr string }{fmt.Sprintf("/listeners/%d/address", i), l.Address})
			}
			for _, o := range others {
				if o.addr != "" && addressesCollide(a.Address, o.addr) {
					at("/address", "collides with "+o.pointer)
				}
			}
		}
		if len(a.Tokens) == 0 && len(a.Certificates) == 0 && a.OIDC == nil {
			at("/address", "an admin listener needs at least one token, certificate or oidc identity")
		}
	}
	roles := map[string]bool{}
	for i, r := range a.Roles {
		if r.Name != "" {
			if roles[r.Name] {
				at(fmt.Sprintf("/roles/%d/name", i), fmt.Sprintf("duplicate role name %q", r.Name))
			}
			roles[r.Name] = true
		}
		if len(r.Scopes) == 0 {
			at(fmt.Sprintf("/roles/%d/scopes", i), "at least one scope is required")
		}
	}
	// defined checks that every role named is one of admin.roles.
	defined := func(pointer string, names []string) {
		for j, n := range names {
			if !roles[n] {
				at(fmt.Sprintf("%s/roles/%d", pointer, j), fmt.Sprintf("no role named %q", n))
			}
		}
	}
	// grants checks an identity's scopes and roles: at least one of either,
	// and every role defined.
	grants := func(pointer string, scopes, names []string) {
		if len(scopes) == 0 && len(names) == 0 {
			at(pointer, "at least one scope or role is required")
		}
		defined(pointer, names)
	}
	if len(a.Certificates) > 0 && (a.TLS == nil || a.TLS.ClientCAFile == "") {
		at("/certificates", "certificate identities require tls.clientCAFile")
	}
	names := map[string]bool{}
	values := map[string]int{}
	for i, t := range a.Tokens {
		if t.Name != "" {
			if names[t.Name] {
				at(fmt.Sprintf("/tokens/%d/name", i), fmt.Sprintf("duplicate token name %q", t.Name))
			}
			names[t.Name] = true
		}
		grants(fmt.Sprintf("/tokens/%d", i), t.Scopes, t.Roles)
		if t.TokenFile == "" {
			continue
		}
		token, err := ReadAdminToken(t.TokenFile)
		switch {
		case err != nil:
			at(fmt.Sprintf("/tokens/%d/tokenFile", i), readError(err))
		case len(token) < MinAdminTokenBytes:
			at(fmt.Sprintf("/tokens/%d/tokenFile", i), fmt.Sprintf("the token must be at least %d bytes", MinAdminTokenBytes))
		default:
			if j, dup := values[string(token)]; dup {
				at(fmt.Sprintf("/tokens/%d/tokenFile", i), fmt.Sprintf("the same token as tokens/%d: one credential cannot be two identities", j))
			} else {
				values[string(token)] = i
			}
		}
	}
	for i, c := range a.Certificates {
		grants(fmt.Sprintf("/certificates/%d", i), c.Scopes, c.Roles)
	}
	if o := a.OIDC; o != nil {
		if msg := checkIssuer(o.Issuer); msg != "" {
			at("/oidc/issuer", msg)
		}
		if len(o.ClientIDs) == 0 {
			at("/oidc/clientIds", "at least one client identifier is required: an ID token's aud must name one")
		}
		if len(o.RoleMappings) == 0 {
			at("/oidc/roleMappings", "at least one mapping is required: an identity with no role is refused")
		}
		for i, m := range o.RoleMappings {
			if len(m.Roles) == 0 {
				at(fmt.Sprintf("/oidc/roleMappings/%d/roles", i), "at least one role is required")
			}
			defined(fmt.Sprintf("/oidc/roleMappings/%d", i), m.Roles)
		}
		if o.CAFile != "" {
			if pemBytes, err := os.ReadFile(o.CAFile); err != nil {
				at("/oidc/caFile", readError(err))
			} else if pool := x509.NewCertPool(); !pool.AppendCertsFromPEM(pemBytes) {
				at("/oidc/caFile", "no certificate found in the file")
			}
		}
	}
	return out
}

// checkIssuer is the rule for an OIDC issuer: an absolute https URL, or http
// on a loopback host, with no query or fragment. It returns the problem, or
// "".
func checkIssuer(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || !u.IsAbs() || u.Host == "" {
		return "must be an absolute URL"
	}
	if u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(raw, "#") {
		return "must not carry a query or a fragment"
	}
	switch u.Scheme {
	case "https":
		return ""
	case "http":
		if isLoopback(u.Hostname()) {
			return ""
		}
		return "must be https unless the host is loopback: an ID token must not be fetched or verified over clear text"
	default:
		return "must be https"
	}
}

// CheckIssuer applies the issuer rule to the CLI's -issuer.
func CheckIssuer(raw string) string { return checkIssuer(raw) }

// addressesCollide reports whether two listen addresses would bind the same
// port: the same non-zero port, and hosts that are equal or either
// unspecified.
func addressesCollide(a, b string) bool {
	ha, pa, err1 := net.SplitHostPort(a)
	hb, pb, err2 := net.SplitHostPort(b)
	if err1 != nil || err2 != nil || pa != pb || pa == "0" {
		return false
	}
	unspecified := func(h string) bool { return h == "" || h == "0.0.0.0" || h == "::" }
	return ha == hb || unspecified(ha) || unspecified(hb)
}

// clusterRules are the cluster transport rules the schema cannot express.
func (d Document) clusterRules() []Problem {
	var out []Problem
	c := d.Cluster
	at := func(pointer, msg string) { out = append(out, Problem{Pointer: "/cluster" + pointer, Message: msg}) }
	if c.Listen != "" {
		if msg := checkAddress(c.Listen); msg != "" {
			at("/listen", msg)
		}
	}
	names := map[string]bool{}
	for i, p := range c.Peers {
		if p.Name != "" {
			if names[p.Name] {
				at(fmt.Sprintf("/peers/%d/name", i), fmt.Sprintf("duplicate peer name %q", p.Name))
			}
			names[p.Name] = true
			if p.Name == d.Node.Name {
				at(fmt.Sprintf("/peers/%d/name", i), "a peer cannot be this node")
			}
		}
		if p.Address != "" {
			if _, _, err := net.SplitHostPort(p.Address); err != nil {
				at(fmt.Sprintf("/peers/%d/address", i), "must be host:port")
			}
		}
	}
	if len(c.Peers) > 0 {
		if c.Listen == "" {
			at("/listen", "required when peers are configured")
		}
		if d.Node.Name == "" {
			out = append(out, Problem{Pointer: "/node/name", Message: "required when cluster peers are configured"})
		}
		if c.Auth.Mode == "" {
			at("/auth/mode", "required when peers are configured")
		}
	}
	if c.Raft.HeartbeatTicks >= c.Raft.ElectionTicks {
		at("/raft/heartbeatTicks", "must be below electionTicks")
	}
	if c.Raft.SnapshotTrailing >= c.Raft.SnapshotEntries {
		at("/raft/snapshotTrailing", "must be below snapshotEntries")
	}
	if c.Replication.AckTimeoutMs > c.Leases.TakeoverTimeoutMs {
		at("/replication/ackTimeoutMs", "must not exceed leases.takeoverTimeoutMs: a takeover's pull waits on the barrier")
	}
	if c.Leases.TTLMs < 3*c.Raft.TickMs*c.Raft.ElectionTicks {
		at("/leases/ttlMs", fmt.Sprintf("must be at least three election timeouts (%d ms)", 3*c.Raft.TickMs*c.Raft.ElectionTicks))
	}
	if c.Advertise != "" {
		if msg := checkAddress(c.Advertise); msg != "" {
			at("/advertise", msg)
		}
	}
	if c.Discovery.Mode == "dns" {
		dns := c.Discovery.DNS
		if dns == nil {
			at("/discovery/dns", "required for dns discovery")
		} else {
			if c.Listen == "" {
				at("/listen", "required for dns discovery")
			}
			if c.Auth.Mode == "" {
				at("/auth/mode", "required for dns discovery")
			}
			if dns.Name != "" && dns.Service != "" {
				if ord, err := Ordinal(dns.Name, d.Node.Name); err != nil {
					out = append(out, Problem{Pointer: "/node/name", Message: err.Error()})
				} else if ord >= dns.Replicas {
					out = append(out, Problem{Pointer: "/node/name", Message: fmt.Sprintf("ordinal %d is not below discovery.dns.replicas %d", ord, dns.Replicas)})
				}
			}
		}
		if len(c.Peers) > 0 {
			at("/peers", "not used with dns discovery")
		}
	}
	switch c.Auth.Mode {
	case "mtls":
		if c.Auth.TLS == nil {
			at("/auth/tls", "required for mtls")
		} else {
			t := c.Auth.TLS
			if t.CertFile != "" && t.KeyFile != "" {
				if _, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile); err != nil {
					at("/auth/tls/certFile", "certificate and key: "+readError(err))
				}
			}
			if t.CAFile != "" {
				if pemBytes, err := os.ReadFile(t.CAFile); err != nil {
					at("/auth/tls/caFile", readError(err))
				} else if pool := x509.NewCertPool(); !pool.AppendCertsFromPEM(pemBytes) {
					at("/auth/tls/caFile", "no certificate found in the file")
				}
			}
		}
		if c.Auth.SecretFile != "" {
			at("/auth/secretFile", "not used in mtls mode")
		}
	case "secret":
		if c.Auth.SecretFile == "" {
			at("/auth/secretFile", "required for secret mode")
		} else if secret, err := os.ReadFile(c.Auth.SecretFile); err != nil {
			at("/auth/secretFile", readError(err))
		} else if len(bytes.TrimSpace(secret)) < 16 {
			at("/auth/secretFile", "the secret must be at least 16 bytes")
		}
		if c.Auth.TLS != nil {
			at("/auth/tls", "not used in secret mode")
		}
	}
	return out
}

// Ordinal extracts the StatefulSet ordinal from a pod name of the form
// <set>-<n>.
func Ordinal(set, node string) (int, error) {
	prefix := set + "-"
	if !strings.HasPrefix(node, prefix) {
		return 0, fmt.Errorf("node name %q must be %s<ordinal> for dns discovery", node, prefix)
	}
	n, err := strconv.Atoi(strings.TrimPrefix(node, prefix))
	if err != nil || n < 0 {
		return 0, fmt.Errorf("node name %q must end in a non-negative ordinal", node)
	}
	return n, nil
}

// isLoopback reports whether a host is a loopback name or address.
func isLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func checkAddress(addr string) string {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "must be host:port"
	}
	n, err := strconv.Atoi(port)
	if err != nil {
		return "port must be numeric"
	}
	// Port 0 asks the kernel for an ephemeral port; the bound address is
	// logged at start, which is what tests and single-node trials want.
	if n < 0 || n > 65535 {
		return "port must be between 0 and 65535"
	}
	return ""
}

func sorted(p []Problem) []Problem {
	sort.SliceStable(p, func(i, j int) bool {
		if p[i].Pointer != p[j].Pointer {
			return p[i].Pointer < p[j].Pointer
		}
		return p[i].Message < p[j].Message
	})
	return p
}

// overrides maps an environment suffix to its document path and kind. Arrays
// and module configuration are not overridable from the environment.
var overrides = map[string]struct {
	path    []string
	integer bool
}{
	"NODE_NAME":                          {path: []string{"node", "name"}},
	"NODE_DATA_DIR":                      {path: []string{"node", "dataDir"}},
	"NODE_WAL_SYNC":                      {path: []string{"node", "walSync"}},
	"NODE_WAL_SYNC_INTERVAL_MS":          {path: []string{"node", "walSyncIntervalMs"}, integer: true},
	"NODE_WAL_GROUP_MAX_DELAY_MS":        {path: []string{"node", "walGroupMaxDelayMs"}, integer: true},
	"NODE_SESSION_SHARDS":                {path: []string{"node", "sessionShards"}, integer: true},
	"NODE_STORE_ADMISSION":               {path: []string{"node", "storeAdmission"}, integer: true},
	"NODE_WAL_GROUP_MAX_SIZE":            {path: []string{"node", "walGroupMaxSize"}, integer: true},
	"NODE_WAL_SEGMENT_BYTES":             {path: []string{"node", "walSegmentBytes"}, integer: true},
	"CLUSTER_ID":                         {path: []string{"cluster", "id"}},
	"CLUSTER_LISTEN":                     {path: []string{"cluster", "listen"}},
	"CLUSTER_REPLICAS":                   {path: []string{"cluster", "replicas"}, integer: true},
	"CLUSTER_SHARDS":                     {path: []string{"cluster", "shards"}, integer: true},
	"CLUSTER_AUTH_MODE":                  {path: []string{"cluster", "auth", "mode"}},
	"CLUSTER_AUTH_SECRET_FILE":           {path: []string{"cluster", "auth", "secretFile"}},
	"CLUSTER_AUTH_TLS_CERT_FILE":         {path: []string{"cluster", "auth", "tls", "certFile"}},
	"CLUSTER_AUTH_TLS_KEY_FILE":          {path: []string{"cluster", "auth", "tls", "keyFile"}},
	"CLUSTER_AUTH_TLS_CA_FILE":           {path: []string{"cluster", "auth", "tls", "caFile"}},
	"CLUSTER_HEARTBEAT_MS":               {path: []string{"cluster", "heartbeatMs"}, integer: true},
	"CLUSTER_HEARTBEAT_MISSES":           {path: []string{"cluster", "heartbeatMisses"}, integer: true},
	"CLUSTER_MAX_FRAME_BYTES":            {path: []string{"cluster", "maxFrameBytes"}, integer: true},
	"CLUSTER_RAFT_TICK_MS":               {path: []string{"cluster", "raft", "tickMs"}, integer: true},
	"CLUSTER_RAFT_ELECTION_TICKS":        {path: []string{"cluster", "raft", "electionTicks"}, integer: true},
	"CLUSTER_RAFT_HEARTBEAT_TICKS":       {path: []string{"cluster", "raft", "heartbeatTicks"}, integer: true},
	"CLUSTER_RAFT_SNAPSHOT_ENTRIES":      {path: []string{"cluster", "raft", "snapshotEntries"}, integer: true},
	"CLUSTER_RAFT_SNAPSHOT_TRAILING":     {path: []string{"cluster", "raft", "snapshotTrailing"}, integer: true},
	"CLUSTER_RAFT_PROMOTE_LAG":           {path: []string{"cluster", "raft", "promoteLag"}, integer: true},
	"CLUSTER_RAFT_JOIN_TIMEOUT_MS":       {path: []string{"cluster", "raft", "joinTimeoutMs"}, integer: true},
	"CLUSTER_DISCOVERY_MODE":             {path: []string{"cluster", "discovery", "mode"}},
	"CLUSTER_DISCOVERY_DNS_SERVICE":      {path: []string{"cluster", "discovery", "dns", "service"}},
	"CLUSTER_DISCOVERY_DNS_NAME":         {path: []string{"cluster", "discovery", "dns", "name"}},
	"CLUSTER_DISCOVERY_DNS_REPLICAS":     {path: []string{"cluster", "discovery", "dns", "replicas"}, integer: true},
	"CLUSTER_DISCOVERY_DNS_PORT":         {path: []string{"cluster", "discovery", "dns", "port"}, integer: true},
	"CLUSTER_LEASES_TTL_MS":              {path: []string{"cluster", "leases", "ttlMs"}, integer: true},
	"CLUSTER_LEASES_TAKEOVER_TIMEOUT_MS": {path: []string{"cluster", "leases", "takeoverTimeoutMs"}, integer: true},
	"CLUSTER_ADVERTISE":                  {path: []string{"cluster", "advertise"}},
	"CLUSTER_ROUTING_SYNC_MS":            {path: []string{"cluster", "routing", "syncMs"}, integer: true},
	"CLUSTER_REPLICATION_ACK_TIMEOUT_MS": {path: []string{"cluster", "replication", "ackTimeoutMs"}, integer: true},
	"CLUSTER_DRAIN_RATE_PER_SECOND":      {path: []string{"cluster", "drain", "ratePerSecond"}, integer: true},
	"CLUSTER_DRAIN_TIMEOUT_MS":           {path: []string{"cluster", "drain", "timeoutMs"}, integer: true},
	"CLUSTER_DRAIN_CONFIRM_TIMEOUT_MS":   {path: []string{"cluster", "drain", "confirmTimeoutMs"}, integer: true},
	"CLUSTER_DRAIN_HOLD_EXIT_MS":         {path: []string{"cluster", "drain", "holdExitMs"}, integer: true},
	"CLUSTER_READINESS_STABLE_MS":        {path: []string{"cluster", "readiness", "stableMs"}, integer: true},
	"LIMITS_MAX_PACKET_SIZE":             {path: []string{"limits", "maxPacketSize"}, integer: true},
	"LIMITS_RECEIVE_MAXIMUM":             {path: []string{"limits", "receiveMaximum"}, integer: true},
	"LIMITS_MAX_QUEUED_MESSAGES":         {path: []string{"limits", "maxQueuedMessages"}, integer: true},
	"LIMITS_MAX_KEEP_ALIVE":              {path: []string{"limits", "maxKeepAlive"}, integer: true},
	"LIMITS_CONNECT_TIMEOUT":             {path: []string{"limits", "connectTimeout"}, integer: true},
	"LIMITS_QUEUE_DROP":                  {path: []string{"limits", "queueDrop"}},
	"LIMITS_MAX_SESSION_EXPIRY":          {path: []string{"limits", "maxSessionExpiry"}, integer: true},
	"OBSERVABILITY_ADDRESS":              {path: []string{"observability", "address"}},
	"LOG_LEVEL":                          {path: []string{"log", "level"}},
	"LOG_FORMAT":                         {path: []string{"log", "format"}},
}

// Overrides lists the supported environment variables, for usage text.
func Overrides() []string {
	names := make([]string, 0, len(overrides))
	for k := range overrides {
		names = append(names, EnvPrefix+k)
	}
	sort.Strings(names)
	return names
}

func applyEnv(tree map[string]any, environ []string) []Problem {
	var out []Problem
	keys := make([]string, 0)
	values := map[string]string{}
	for _, kv := range environ {
		name, value, ok := strings.Cut(kv, "=")
		if !ok || !strings.HasPrefix(name, EnvPrefix) || name == EnvConfig || name == EnvAdminToken || name == EnvAdminAddress {
			continue
		}
		keys = append(keys, name)
		values[name] = value
	}
	sort.Strings(keys)
	for _, name := range keys {
		spec, ok := overrides[strings.TrimPrefix(name, EnvPrefix)]
		if !ok {
			out = append(out, Problem{Pointer: "/", Message: "unknown environment override " + name})
			continue
		}
		pointer := "/" + strings.Join(spec.path, "/")
		var v any = values[name]
		if spec.integer {
			n, err := strconv.Atoi(values[name])
			if err != nil {
				out = append(out, Problem{Pointer: pointer, Message: fmt.Sprintf("%s must be an integer, got %q", name, values[name])})
				continue
			}
			v = json.Number(strconv.Itoa(n))
		}
		node := tree
		for _, seg := range spec.path[:len(spec.path)-1] {
			child, ok := node[seg].(map[string]any)
			if !ok {
				if _, present := node[seg]; present {
					// The schema reports the wrong type at this member; the
					// override cannot be applied beneath it.
					node = nil
					break
				}
				child = map[string]any{}
				node[seg] = child
			}
			node = child
		}
		if node != nil {
			node[spec.path[len(spec.path)-1]] = v
		}
	}
	return out
}
