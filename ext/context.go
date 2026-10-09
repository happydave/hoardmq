package ext

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/happydave/hoardmq/ext/mqtt"
	"github.com/happydave/hoardmq/ext/records"
	"github.com/happydave/hoardmq/ext/store"
)

// Services is what modules may ask of the broker.
type Services interface {
	// Publish injects a message as if published by the node itself.
	Publish(m *mqtt.Message)
	// Disconnect closes a client's connection with a reason.
	Disconnect(namespace, clientID string, reason mqtt.ReasonCode) bool
	// Sessions lists session records.
	Sessions() []SessionRecord
	// Retained returns the retained messages a filter matches.
	Retained(namespace, filter string) []*mqtt.Message
	// Ready reports whether the broker is serving; the cluster refines it.
	Ready() bool
	// Serving reports whether the node is ready and can acknowledge a QoS 1
	// or 2 publish, and when not, why: "not-ready: " and the readiness
	// reason, or a degraded state such as "routes-unknown".
	Serving() (bool, string)
	// Drain starts the node's graceful leave (idempotent) and returns at
	// once; DrainStatus reports its progress.
	Drain()
	DrainStatus() DrainStatus
	// Records is the replicated records store, nil until the node's
	// consensus core is open.
	Records() RecordStore
}

// RecordStore is the replicated records store: named collections of keyed
// JSON records that every node applies from the consensus log. Reads are
// this node's applied state; a write returns once it has committed and
// applied here.
type RecordStore interface {
	Get(collection, key string) (records.Record, bool)
	List(collection, after string, limit int) (items []records.Entry, more bool)
	Put(ctx context.Context, collection, key string, value json.RawMessage, ifVersion *uint64, by string, at time.Time) (records.Result, error)
	Delete(ctx context.Context, collection, key string, ifVersion *uint64, by string, at time.Time) (records.Result, error)
}

// DrainStatus is a drain's progress as the endpoint reports it.
type DrainStatus struct {
	Draining         bool   `json:"draining"`
	Phase            string `json:"phase,omitempty"`
	ClientsRemaining int    `json:"clientsRemaining"`
	LeasesRemaining  int    `json:"leasesRemaining"`
	// The sessions the drain handed off and still waits to see confirmed held
	// by their receiver; those confirmed; and those its confirmation timeout
	// ended with unconfirmed.
	SessionsAwaiting    int `json:"sessionsAwaiting"`
	SessionsConfirmed   int `json:"sessionsConfirmed"`
	SessionsUnconfirmed int `json:"sessionsUnconfirmed"`
	// CopiesKept is the replica copies this node keeps because it stopped
	// being their session's target when it left the shard map, until each
	// owner confirms the session held at its new targets.
	CopiesKept int `json:"copiesKept,omitempty"`
	// RetainedShardsKept is the retained shards whose copies this node keeps
	// because it stopped holding them, until each shard's leader releases them.
	RetainedShardsKept int  `json:"retainedShardsKept,omitempty"`
	Complete           bool `json:"complete"`
}

// Observability is the listener configuration the metrics module reads.
// AdminOwnsDrain is set when the admin API is configured: the drain is then
// served there, behind its authentication, and not on this listener.
type Observability struct {
	Address        string
	MetricsPath    string
	AdminOwnsDrain bool
}

// Collector is implemented by a metrics provider that lets the broker set
// at-scrape gauges: each registered function is called before a snapshot.
type Collector interface {
	AddCollector(func(set func(name string, labels map[string]string, value float64)))
}

// Context is what a module receives at provision.
type Context struct {
	Logger    *slog.Logger
	Node      string
	Namespace string
	DataDir   string
	// Store is the node's journal settings, for store providers.
	Store store.Options
	// Observability is the listener the metrics module serves, if any.
	Observability Observability
	// Services is nil during hoardmq check.
	Services Services
	// Passwords verifies a password against a stored credential the way the
	// broker's own authenticators do: through the node's verification cache
	// while serving and uncached during hoardmq check, within the node's
	// Argon2id memory budget, and refusing what the build's FIPS setting
	// refuses. Never nil.
	Passwords PasswordVerifier
}

// PasswordVerifier checks a password against a stored credential in one of
// the PHC forms hoardmq hash-password writes.
type PasswordVerifier interface {
	Verify(stored string, password []byte) (bool, error)
}

// SessionRecord describes one session, as Services.Sessions lists it.
type SessionRecord struct {
	Namespace      string
	ClientID       string
	Epoch          uint64
	Version        mqtt.Version
	Online         bool
	ExpiryInterval uint32
	ExpiresAt      time.Time
	Subscriptions  int
	InFlight       int
	Queued         int
	Drops          uint64
	Expired        uint64
	HasWill        bool
}
