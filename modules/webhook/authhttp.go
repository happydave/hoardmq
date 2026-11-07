package webhook

import (
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/happydave/hoardmq/ext"
	"github.com/happydave/hoardmq/ext/auth"
)

// AuthID is auth.http's identifier.
const AuthID ext.ID = "auth.http"

// SuperuserAttribute is "true" on an identity auth.http admitted with
// is_superuser, and "false" on one it admitted without. Its authorizer
// honours it only on a connection auth.http admitted (superuser).
const SuperuserAttribute = "auth.http.superuser"

func init() {
	ext.Register(AuthID, func() ext.Module { return &authHTTP{readFile: readFileOS, now: time.Now} })
}

const (
	metricAuthRequests    = "hoardmq_auth_http_requests_total"
	metricAuthUnavailable = "hoardmq_auth_http_unavailable_total"
	metricAuthCacheHits   = "hoardmq_auth_http_cache_hits_total"
	metricAuthCacheSize   = "hoardmq_auth_http_cache_entries"
)

type authConfig struct {
	endpointConfig
	Authenticate  *bool  `json:"authenticate"`
	Authorize     *bool  `json:"authorize"`
	OnUnavailable string `json:"onUnavailable"`
	SendPassword  *bool  `json:"sendPassword"`
	Cache         struct {
		MaxEntries *int `json:"maxEntries"`
	} `json:"cache"`
}

func boolOr(p *bool, def bool) bool {
	if p == nil {
		return def
	}
	return *p
}

type authHTTP struct {
	cfg      authConfig
	ep       *endpoint
	node, ns string
	ttl      time.Duration
	log      *slog.Logger
	readFile func(string) ([]byte, error)
	now      func() time.Time

	authenticate, authorize, passOnUnavailable, sendPassword bool

	metrics atomic.Pointer[metricsBox]
	cache   *answerCache
}

type metricsBox struct{ m ext.MetricsProvider }

func (*authHTTP) ID() ext.ID { return AuthID }

func (*authHTTP) Capabilities() ext.Capability { return ext.CapAuthentication | ext.CapAuthorization }

func (a *authHTTP) Provision(ctx *ext.Context, raw json.RawMessage) error {
	if err := decodeStrict(raw, &a.cfg); err != nil {
		return err
	}
	a.node, a.ns, a.ttl, a.log = ctx.Node, ctx.Namespace, ctx.DecisionTTL, ctx.Logger
	if a.log == nil {
		a.log = slog.Default()
	}
	return nil
}

// Validate checks the configuration and reads the files it names.
func (a *authHTTP) Validate() error {
	ep, problems := a.cfg.build(2000, true, a.readFile)
	switch a.cfg.OnUnavailable {
	case "", "deny", "pass":
	default:
		problems = append(problems, `onUnavailable must be "deny" or "pass"`)
	}
	problems = inRange(problems, "cache.maxEntries", a.cfg.Cache.MaxEntries, 0, 1_000_000)
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	a.ep = ep
	a.authenticate, a.authorize = boolOr(a.cfg.Authenticate, true), boolOr(a.cfg.Authorize, true)
	a.passOnUnavailable = a.cfg.OnUnavailable == "pass"
	a.sendPassword = boolOr(a.cfg.SendPassword, true)
	a.cache = newAnswerCache(intOr(a.cfg.Cache.MaxEntries, 100_000))
	return nil
}

func (a *authHTTP) Start(context.Context) error {
	if a.ep == nil {
		return a.Validate()
	}
	return nil
}

func (a *authHTTP) Stop(context.Context) error {
	if a.ep != nil {
		a.ep.client.CloseIdleConnections()
	}
	return nil
}

// UseMetrics receives the node's metrics provider and the cache's gauge.
func (a *authHTTP) UseMetrics(m ext.MetricsProvider) {
	a.metrics.Store(&metricsBox{m: m})
	if c, ok := m.(ext.Collector); ok {
		c.AddCollector(func(set func(string, map[string]string, float64)) {
			if a.cache != nil {
				set(metricAuthCacheSize, nil, float64(a.cache.len()))
			}
		})
	}
}

func (a *authHTTP) count(name string, labels map[string]string) {
	if b := a.metrics.Load(); b != nil {
		b.m.Add(name, labels, 1)
	}
}

// answer is what an endpoint decided, read from its response.
type answer struct {
	decision  auth.Decision
	superuser bool
	attrs     map[string]string
	expireAt  time.Time     // zero: none
	maxAge    time.Duration // zero: not to be cached
}

// errUnavailable marks a response that is no decision.
var errUnavailable = errors.New("unavailable")

// readAnswer reads a response: 204 allows; 200 carries a JSON object whose
// result is allow, deny or ignore. Anything else is no decision.
func readAnswer(r response) (answer, error) {
	var a answer
	a.maxAge = maxAge(r.header.Get("Cache-Control"))
	switch r.status {
	case http.StatusNoContent:
		a.decision = auth.Allow
		return a, nil
	case http.StatusOK:
	default:
		return a, fmt.Errorf("%w: status %d", errUnavailable, r.status)
	}
	var body struct {
		Result      string            `json:"result"`
		IsSuperuser bool              `json:"is_superuser"`
		ClientAttrs map[string]string `json:"client_attrs"`
		ExpireAt    int64             `json:"expire_at"`
	}
	if err := json.Unmarshal(r.body, &body); err != nil {
		return a, fmt.Errorf("%w: the body is not a JSON object of the answer's shape", errUnavailable)
	}
	switch body.Result {
	case "allow":
		a.decision = auth.Allow
	case "deny":
		a.decision = auth.Deny
	case "ignore":
		a.decision = auth.Pass
	default:
		return a, fmt.Errorf("%w: result %q", errUnavailable, body.Result)
	}
	a.superuser, a.attrs = body.IsSuperuser, body.ClientAttrs
	if body.ExpireAt > 0 {
		a.expireAt = time.Unix(body.ExpireAt, 0)
	}
	return a, nil
}

// maxAge reads cache-control's max-age, zero when absent or unreadable.
func maxAge(h string) time.Duration {
	for _, part := range strings.Split(h, ",") {
		k, v, ok := strings.Cut(part, "=")
		if !ok || !strings.EqualFold(strings.TrimSpace(k), "max-age") {
			continue
		}
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil || n <= 0 {
			return 0
		}
		return time.Duration(n) * time.Second
	}
	return 0
}

// expiresAt is when a CONNECT's allow lapses: the earlier of the endpoint's
// expire_at and now plus max-age; zero for neither.
func expiresAt(now time.Time, a answer) time.Time {
	at := a.expireAt
	if a.maxAge > 0 && (at.IsZero() || now.Add(a.maxAge).Before(at)) {
		at = now.Add(a.maxAge)
	}
	return at
}

// cacheFor is how long an authorization answer is held: its max-age, within
// the broker's decision TTL.
func cacheFor(maxAge, ttl time.Duration) time.Duration {
	if ttl > 0 && maxAge > ttl {
		return ttl
	}
	return maxAge
}

func peerHost(addr string) string {
	if h, _, err := net.SplitHostPort(addr); err == nil {
		return h
	}
	return addr
}

// ask posts one request and reads its answer.
func (a *authHTTP) ask(ctx context.Context, action string, body map[string]any) (answer, error) {
	body["action"], body["node"] = action, a.node
	data, err := json.Marshal(body)
	if err != nil {
		return answer{}, err
	}
	resp, err := a.ep.post(ctx, data, map[string]string{"hoardmq-hook": action})
	if err != nil {
		return answer{}, fmt.Errorf("%w: %s", errUnavailable, redact(err))
	}
	return readAnswer(resp)
}

// unavailable is the policy's decision on an endpoint that gave none.
func (a *authHTTP) unavailable(action string, err error) auth.Decision {
	policy := "deny"
	if a.passOnUnavailable {
		policy = "pass"
	}
	a.count(metricAuthUnavailable, map[string]string{"policy": policy})
	a.count(metricAuthRequests, map[string]string{"action": action, "outcome": "unavailable"})
	a.log.Warn("auth.http endpoint unavailable", "action", action, "policy", policy, "err", err)
	if a.passOnUnavailable {
		return auth.Pass
	}
	return auth.Deny
}

// Authenticate asks the endpoint about a CONNECT.
func (a *authHTTP) Authenticate(ctx context.Context, req *auth.Request) auth.Result {
	if !a.authenticate {
		return auth.Result{Decision: auth.Pass}
	}
	body := map[string]any{
		"clientid": req.ClientID, "username": req.Username, "peerhost": peerHost(req.PeerAddr), "peername": req.PeerAddr,
		"cert_common_name": req.CertCN, "listener": req.Listener, "namespace": req.Namespace,
	}
	if req.HasPassword && a.sendPassword {
		body["password"] = string(req.Password)
	}
	ans, err := a.ask(ctx, "connect", body)
	if err != nil {
		d := a.unavailable("connect", err)
		return auth.Result{Decision: d, Reason: "auth.http: endpoint unavailable"}
	}
	a.count(metricAuthRequests, map[string]string{"action": "connect", "outcome": ans.decision.String()})
	switch ans.decision {
	case auth.Deny:
		return auth.Result{Decision: auth.Deny, Reason: "auth.http: denied"}
	case auth.Pass:
		return auth.Result{Decision: auth.Pass}
	}
	now := a.now()
	if !ans.expireAt.IsZero() && !ans.expireAt.After(now) {
		// Held, it would be re-decided at once, in a loop for as long as the
		// endpoint answers so.
		return auth.Result{Decision: auth.Deny, Reason: "auth.http: expire_at has passed"}
	}
	attrs := map[string]string{}
	for k, v := range ans.attrs {
		if k != SuperuserAttribute {
			attrs[k] = v
		}
	}
	// Written on every allow, so a value an earlier member that passed left
	// in the chain's merge never stands for this module's answer.
	attrs[SuperuserAttribute] = strconv.FormatBool(ans.superuser)
	return auth.Result{Decision: auth.Allow, Attributes: attrs, Expires: expiresAt(now, ans)}
}

// superuser decides whether auth.http's authorizer allows id every action
// without asking: only when auth.http admitted the connection and its
// endpoint said is_superuser. An attribute another module or file attached
// grants nothing.
func superuser(id *auth.Identity) bool {
	return id.AdmittedBy == string(AuthID) && id.Attributes[SuperuserAttribute] == "true"
}

// Authorize asks the endpoint about a publish, subscribe or unsubscribe,
// from the cache while an answer holds.
func (a *authHTTP) Authorize(ctx context.Context, action auth.Action, topic string, id *auth.Identity) auth.Decision {
	if !a.authorize {
		return auth.Pass
	}
	name := action.String()
	if superuser(id) {
		a.count(metricAuthRequests, map[string]string{"action": name, "outcome": "superuser"})
		return auth.Allow
	}
	key := strings.Join([]string{id.Namespace, id.ClientID, id.Username, name, topic}, "\x00")
	now := a.now()
	if d, ok := a.cache.get(key, now); ok {
		a.count(metricAuthCacheHits, nil)
		return d
	}
	ans, err := a.ask(ctx, name, map[string]any{
		"clientid": id.ClientID, "username": id.Username, "cert_common_name": id.CertCN, "namespace": id.Namespace, "topic": topic,
	})
	if err != nil {
		return a.unavailable(name, err)
	}
	a.count(metricAuthRequests, map[string]string{"action": name, "outcome": ans.decision.String()})
	if hold := cacheFor(ans.maxAge, a.ttl); hold > 0 {
		a.cache.put(key, ans.decision, now.Add(hold))
	}
	return ans.decision
}

// answerCache holds authorization answers until they expire, the oldest
// dropped first when full.
type answerCache struct {
	max   int
	mu    sync.Mutex
	order *list.List
	items map[string]*list.Element
}

type cached struct {
	key     string
	d       auth.Decision
	expires time.Time
}

func newAnswerCache(max int) *answerCache {
	return &answerCache{max: max, order: list.New(), items: map[string]*list.Element{}}
}

func (c *answerCache) get(key string, now time.Time) (auth.Decision, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.items[key]
	if !ok {
		return auth.Pass, false
	}
	v := e.Value.(*cached)
	if !now.Before(v.expires) {
		c.order.Remove(e)
		delete(c.items, key)
		return auth.Pass, false
	}
	return v.d, true
}

func (c *answerCache) put(key string, d auth.Decision, expires time.Time) {
	if c.max == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.items[key]; ok {
		c.order.Remove(e)
	}
	for c.order.Len() >= c.max {
		oldest := c.order.Front()
		c.order.Remove(oldest)
		delete(c.items, oldest.Value.(*cached).key)
	}
	c.items[key] = c.order.PushBack(&cached{key: key, d: d, expires: expires})
}

func (c *answerCache) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}
