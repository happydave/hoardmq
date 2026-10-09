package admin

import (
	"context"
	"time"
)

// Backend is what the API reads and changes; the broker implements it. Every
// call acts on this node and the default namespace. Errors a caller should
// see are *Error values; anything else is a 500.
type Backend interface {
	Node() string

	// ScanClients lists one page of clients by identifier: every client the
	// lease table holds, each naming its owner, or only owner's when it is
	// not empty.
	ScanClients(ctx context.Context, search, after, owner string, limit int) (items []ClientListItem, more bool, err error)
	Client(id string) (ClientDetail, error)
	Subscriptions(id string) ([]Subscription, error)
	Disconnect(id string) error
	Invalidate(ctx context.Context, id string) (Invalidated, error)
	Matchers(topic string) []Matcher

	Retained(filter, after string, limit int) (items []RetainedMessage, more bool)
	DeleteRetained(ctx context.Context, topic string) (Durability, error)

	Cluster() ClusterState
	Shards() ShardMap
	Lease(id string) (Lease, error)
	DrainStatus() DrainStatus
	Drain() DrainStatus

	// RunningConfig is the running document as JSON, before redaction.
	RunningConfig() ([]byte, error)
	ValidateConfig(doc []byte) []ConfigProblem
	// ReloadConfig re-reads the node's configuration file and applies it when
	// every change is reloadable.
	ReloadConfig() ConfigReload

	StartTrace(id string, limit int) (TraceStarted, error)
	StopTrace(id string) ([]TraceEvent, error)
	Trace(id string) ([]TraceEvent, error)

	Modules() []Module
	SetModuleEnabled(id string, enabled bool) error

	// Records is the replicated records store, nil until this node's
	// consensus core is open.
	Records() RecordStore
	// Limits is the configured limits and those in force; Overload what the
	// node's stress controller sees.
	Limits() LimitsView
	Overload() OverloadView

	// KeysReady reports whether this node may authenticate API keys: it has
	// renewed with the leader recently and applied the log through that
	// renewal. The reason says why not.
	KeysReady() (bool, string)
	// ConfirmApplied asks every member the log calls alive whether it has
	// applied the log through index, and returns those that did not say so
	// before ctx ended.
	ConfirmApplied(ctx context.Context, index uint64) (unconfirmed []string, err error)

	// Forward sends a request to node, the owner of the client it acts on,
	// and returns that node's answer; a *ForwardError says why it could not.
	Forward(ctx context.Context, node string, req ForwardRequest) (ForwardAnswer, error)

	AuditTail(n int) ([]AuditEvent, error)
	// Audit records an event; lossy events may be dropped under load.
	Audit(e AuditEvent, lossy bool)
}

// ClientSummary is one session held on this node.
type ClientSummary struct {
	ClientID        string     `json:"clientId"`
	Online          bool       `json:"online"`
	ProtocolVersion int        `json:"protocolVersion"`
	Token           uint64     `json:"token"`
	ExpiryInterval  uint32     `json:"expiryInterval"`
	ExpiresAt       *time.Time `json:"expiresAt,omitempty"`
	Subscriptions   int        `json:"subscriptions"`
	InFlight        int        `json:"inFlight"`
	Queued          int        `json:"queued"`
	Drops           uint64     `json:"drops"`
	Expired         uint64     `json:"expired"`
	HasWill         bool       `json:"hasWill"`
}

// ClientListItem is one client in the cluster's list, as the lease table
// holds it; Session is its session summary when the answering node holds it.
type ClientListItem struct {
	ClientID  string         `json:"clientId"`
	Node      string         `json:"node"`
	Online    bool           `json:"online"`
	Token     uint64         `json:"token"`
	ExpiresAt *time.Time     `json:"expiresAt,omitempty"`
	HasWill   bool           `json:"hasWill"`
	Session   *ClientSummary `json:"session,omitempty"`
}

// ClientPage is one page of the client list; Next is the cursor for the
// next page, empty on the last.
type ClientPage struct {
	Items []ClientListItem `json:"items"`
	Next  string           `json:"next,omitempty"`
}

// ClientDetail is one client: its session here, its connection if one is
// attached, and its lease as this node's lease table holds it.
type ClientDetail struct {
	Session    ClientSummary `json:"session"`
	Connection *Connection   `json:"connection,omitempty"`
	Lease      *Lease        `json:"lease,omitempty"`
}

// Connection is a client's live connection on this node.
type Connection struct {
	Listener         string  `json:"listener"`
	RemoteAddress    string  `json:"remoteAddress"`
	Username         string  `json:"username,omitempty"`
	ProtocolVersion  int     `json:"protocolVersion"`
	ConnectedSeconds float64 `json:"connectedSeconds"`
}

// Lease is a client identifier's ownership record in the lease table.
type Lease struct {
	ClientID  string     `json:"clientId"`
	Owner     string     `json:"owner"`
	Token     uint64     `json:"token"`
	Online    bool       `json:"online"`
	ExpiresAt *time.Time `json:"expiresAt,omitempty"`
	HasWill   bool       `json:"hasWill"`
}

// Subscription is one of a client's subscriptions.
type Subscription struct {
	Filter            string `json:"filter"`
	QoS               int    `json:"qos"`
	NoLocal           bool   `json:"noLocal"`
	RetainAsPublished bool   `json:"retainAsPublished"`
	RetainHandling    int    `json:"retainHandling"`
	Identifier        uint32 `json:"identifier,omitempty"`
}

// Matcher is one subscription on this node a topic reaches.
type Matcher struct {
	ClientID string `json:"clientId"`
	Filter   string `json:"filter"`
	Group    string `json:"group,omitempty"`
	QoS      int    `json:"qos"`
}

// Invalidated reports an ended session. Durable says the discard was
// acknowledged by this node's journal and its live replicas within the
// replication acknowledgement timeout.
type Invalidated struct {
	ClientID      string `json:"clientId"`
	WasOnline     bool   `json:"wasOnline"`
	WillPublished bool   `json:"willPublished"`
	Durable       bool   `json:"durable"`
}

// RetainedMessage is one retained message.
type RetainedMessage struct {
	Topic        string `json:"topic"`
	QoS          int    `json:"qos"`
	Payload      []byte `json:"payload"`
	PayloadBytes int    `json:"payloadBytes"`
	ContentType  string `json:"contentType,omitempty"`
}

// RetainedPage is one page of retained messages, by topic.
type RetainedPage struct {
	Items []RetainedMessage `json:"items"`
	Next  string            `json:"next,omitempty"`
}

// Durability reports whether a change was acknowledged durable.
type Durability struct {
	Durable bool `json:"durable"`
}

// ClusterState is what an operator reads first about a node and its cluster.
type ClusterState struct {
	Node        string `json:"node"`
	Ready       bool   `json:"ready"`
	ReadyReason string `json:"readyReason,omitempty"`
	// Serving is ready and able to acknowledge a QoS 1 or 2 publish; a
	// ready node that is not serving is degraded, and ServingReason says why.
	Serving       bool          `json:"serving"`
	ServingReason string        `json:"servingReason,omitempty"`
	Consensus     ConsensusView `json:"consensus"`
	Members       []Member      `json:"members"`
	FeatureLevel  FeatureLevel  `json:"featureLevel"`
	ShardMap      ShardSummary  `json:"shardMap"`
	Drain         DrainStatus   `json:"drain"`
	Sealed        SealedCopies  `json:"sealed"`
	Leases        int           `json:"leases"`
}

// ConsensusView is this node's view of the consensus core.
type ConsensusView struct {
	Role    string `json:"role"`
	Leader  string `json:"leader,omitempty"`
	Term    uint64 `json:"term"`
	Commit  uint64 `json:"commit"`
	Applied uint64 `json:"applied"`
}

// Member is one consensus member with its lease-table node record.
type Member struct {
	Name     string `json:"name"`
	Role     string `json:"role"`
	CaughtUp bool   `json:"caughtUp"`
	// Alive is the log's verdict: an alive record and no declaration.
	Alive          bool   `json:"alive"`
	Advertise      string `json:"advertise,omitempty"`
	Level          uint64 `json:"level"`
	DeclaredDeadAt uint64 `json:"declaredDeadAt"`
	ServedIndex    uint64 `json:"servedIndex"`
	ServedRoutes   uint64 `json:"servedRoutes"`
}

// FeatureLevel is the cluster's feature level, this build's, and the lowest
// any member advertises.
type FeatureLevel struct {
	Cluster       uint64 `json:"cluster"`
	Build         uint64 `json:"build"`
	MinAdvertised uint64 `json:"minAdvertised"`
}

// ShardSummary is the current shard map's shape.
type ShardSummary struct {
	Version  uint64   `json:"version"`
	Shards   int      `json:"shards"`
	Replicas int      `json:"replicas"`
	Basis    []string `json:"basis"`
}

// ShardMap is the current shard map with every shard's holders.
type ShardMap struct {
	ShardSummary
	Assignments []ShardAssignment `json:"assignments"`
}

// ShardAssignment is one shard's leader and followers.
type ShardAssignment struct {
	Shard     int      `json:"shard"`
	Leader    string   `json:"leader"`
	Followers []string `json:"followers,omitempty"`
}

// DrainStatus is a drain's progress.
type DrainStatus struct {
	Draining            bool   `json:"draining"`
	Phase               string `json:"phase,omitempty"`
	ClientsRemaining    int    `json:"clientsRemaining"`
	LeasesRemaining     int    `json:"leasesRemaining"`
	SessionsAwaiting    int    `json:"sessionsAwaiting"`
	SessionsConfirmed   int    `json:"sessionsConfirmed"`
	SessionsUnconfirmed int    `json:"sessionsUnconfirmed"`
	CopiesKept          int    `json:"copiesKept,omitempty"`
	Complete            bool   `json:"complete"`
}

// SealedCopies are sessions in transit between owners: held now here, and
// sealed, confirmed and expired since start.
type SealedCopies struct {
	Held      int   `json:"held"`
	Made      int64 `json:"made"`
	Confirmed int64 `json:"confirmed"`
	Expired   int64 `json:"expired"`
}

// ConfigProblem is one thing wrong with a configuration document.
type ConfigProblem struct {
	Pointer string `json:"pointer"`
	Message string `json:"message"`
}

// ConfigValidation is a validation's result.
type ConfigValidation struct {
	Valid    bool            `json:"valid"`
	Problems []ConfigProblem `json:"problems"`
}

// ConfigReload is a reload's result. When applied, problems name only modules
// that could not re-read their files and kept what they had; otherwise
// nothing changed, and problems say why: every validation problem, or with
// restartRequired every member only a restart applies.
type ConfigReload struct {
	Applied         bool            `json:"applied"`
	RestartRequired bool            `json:"restartRequired"`
	Changed         []string        `json:"changed"`
	Problems        []ConfigProblem `json:"problems"`
}

// TraceStartRequest optionally bounds a recording.
type TraceStartRequest struct {
	Limit int `json:"limit,omitempty"`
}

// TraceStarted confirms a recording.
type TraceStarted struct {
	ClientID string `json:"clientId"`
	Limit    int    `json:"limit"`
}

// TraceEvent is one recorded packet.
type TraceEvent struct {
	Time      time.Time `json:"time"`
	Direction string    `json:"direction"`
	Type      string    `json:"type"`
	PacketID  uint16    `json:"packetId,omitempty"`
	Topic     string    `json:"topic,omitempty"`
	Size      int       `json:"size,omitempty"`
	Reason    string    `json:"reason,omitempty"`
}

// Module is one instantiated module.
type Module struct {
	ID           string `json:"id"`
	Family       string `json:"family"`
	Enabled      bool   `json:"enabled"`
	Toggleable   bool   `json:"toggleable"`
	HookFailures int    `json:"hookFailures"`
}

// AuditEvent is one audit record.
type AuditEvent struct {
	Time    time.Time         `json:"time"`
	Actor   string            `json:"actor"`
	Action  string            `json:"action"`
	Target  string            `json:"target,omitempty"`
	Outcome string            `json:"outcome"`
	Detail  map[string]string `json:"detail,omitempty"`
}

// Error is a failure the caller is told about, as a problem document.
type Error struct {
	Status int
	Detail string
	// Owner names the node that holds the client, for a 409.
	Owner string
}

func (e *Error) Error() string { return e.Detail }

// NotFound, Conflict, HeldElsewhere and BadRequest build the errors handlers
// and backends return.
func NotFound(detail string) *Error   { return &Error{Status: 404, Detail: detail} }
func Conflict(detail string) *Error   { return &Error{Status: 409, Detail: detail} }
func BadRequest(detail string) *Error { return &Error{Status: 400, Detail: detail} }
func HeldElsewhere(id, owner string) *Error {
	return &Error{Status: 409, Detail: "client " + id + " is held by node " + owner + "; act on that node", Owner: owner}
}

// Problem is the error body (RFC 9457).
type Problem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail,omitempty"`
	Owner  string `json:"owner,omitempty"`
	Scope  string `json:"scope,omitempty"`
}
