package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/happydave/hoardmq/internal/bans"
	"github.com/happydave/hoardmq/internal/cluster/records"
	"github.com/happydave/hoardmq/internal/config"
)

// Ban is one entry of the ban list.
type Ban struct {
	Key       string     `json:"key"`
	Kind      string     `json:"kind"`
	Value     string     `json:"value"`
	Reason    string     `json:"reason,omitempty"`
	ExpiresAt *time.Time `json:"expiresAt,omitempty"`
	Source    string     `json:"source"`
	Version   uint64     `json:"version"`
	UpdatedAt time.Time  `json:"updatedAt"`
	UpdatedBy string     `json:"updatedBy,omitempty"`
}

// BanPage is a page of bans by key.
type BanPage struct {
	Items []Ban  `json:"items"`
	Next  string `json:"next,omitempty"`
}

// BanRequest adds a ban: the kind and the value it matches, a reason, and an
// expiry — expiresInSeconds from now, or expiresAt; neither is a ban that
// does not expire.
type BanRequest struct {
	Kind             string     `json:"kind"`
	Value            string     `json:"value"`
	Reason           string     `json:"reason,omitempty"`
	ExpiresInSeconds *int64     `json:"expiresInSeconds,omitempty"`
	ExpiresAt        *time.Time `json:"expiresAt,omitempty"`
}

// banProblems documents the failures the ban routes add.
var banProblems = map[int]string{409: "A ban with this key exists.", 503: "The cluster did not confirm the write, or the ban list is not open on this node."}

func banView(key string, r records.Record) Ban {
	var e bans.Entry
	_ = json.Unmarshal(r.Value, &e)
	kind, value, _ := bans.SplitKey(key)
	return Ban{Key: key, Kind: kind, Value: value, Reason: e.Reason, ExpiresAt: e.ExpiresAt, Source: e.Source,
		Version: r.Version, UpdatedAt: r.At, UpdatedBy: r.By}
}

// banEntry is the decision on a request: the key and entry it stores, or
// why it is refused.
func banEntry(req BanRequest, now time.Time) (string, bans.Entry, string) {
	e := bans.Entry{Reason: req.Reason, Source: bans.SourceAdmin}
	switch {
	case req.ExpiresInSeconds != nil && req.ExpiresAt != nil:
		return "", e, "set one of expiresInSeconds and expiresAt, not both"
	case req.ExpiresInSeconds != nil:
		if *req.ExpiresInSeconds < 1 {
			return "", e, "expiresInSeconds must be at least 1"
		}
		t := now.Add(time.Duration(*req.ExpiresInSeconds) * time.Second).UTC()
		e.ExpiresAt = &t
	case req.ExpiresAt != nil:
		if !req.ExpiresAt.After(now) {
			return "", e, "expiresAt must be in the future"
		}
		t := req.ExpiresAt.UTC()
		e.ExpiresAt = &t
	}
	key := bans.Key(req.Kind, req.Value)
	if err := bans.CheckBan(key, e); err != nil {
		return "", e, err.Error()
	}
	return key, e, ""
}

func (s *Server) listBans(r *http.Request) (any, error) {
	st, err := s.records()
	if err != nil {
		return nil, err
	}
	after, limit, err := page(r)
	if err != nil {
		return nil, err
	}
	entries, more := st.List(bans.Collection, after, limit)
	out := BanPage{Items: make([]Ban, 0, len(entries))}
	for _, e := range entries {
		out.Items = append(out.Items, banView(e.Key, e.Record))
	}
	if more && len(out.Items) > 0 {
		out.Next = out.Items[len(out.Items)-1].Key
	}
	return out, nil
}

func (s *Server) createBan(r *http.Request) (any, error) {
	st, err := s.records()
	if err != nil {
		return nil, err
	}
	var req BanRequest
	if err := decodeBody(r, &req); err != nil {
		return nil, err
	}
	now := s.now()
	key, e, msg := banEntry(req, now)
	n := note(r)
	n.target = key
	if msg != "" {
		return nil, BadRequest(msg)
	}
	n.detail = map[string]string{"kind": req.Kind}
	if e.ExpiresAt != nil {
		n.detail["expiresAt"] = e.ExpiresAt.Format(time.RFC3339)
	}
	value, _ := json.Marshal(e)
	absent := uint64(0)
	ctx, cancel := context.WithTimeout(r.Context(), s.writeTimeout)
	defer cancel()
	res, err := st.Put(ctx, bans.Collection, key, value, &absent, principal(r).Name, now.UTC())
	if err := banWriteError(res, err, key); err != nil {
		return nil, err
	}
	rec, _ := st.Get(bans.Collection, key)
	return banView(key, rec), nil
}

func (s *Server) deleteBan(r *http.Request) (any, error) {
	st, err := s.records()
	if err != nil {
		return nil, err
	}
	key := r.PathValue("key")
	var ifVersion *uint64
	if v := r.URL.Query().Get("ifVersion"); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil || n == 0 {
			return nil, BadRequest("ifVersion must be a positive integer")
		}
		ifVersion = &n
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.writeTimeout)
	defer cancel()
	res, err := st.Delete(ctx, bans.Collection, key, ifVersion, principal(r).Name, s.now().UTC())
	if err := banWriteError(res, err, key); err != nil {
		return nil, err
	}
	if res.Previous == nil {
		return Ban{Key: key}, nil
	}
	return banView(key, *res.Previous), nil
}

// banWriteError maps a records write's refusal or failure to the API's answer.
func banWriteError(res records.Result, err error, key string) error {
	switch {
	case err != nil:
		return writeError(res, err, key)
	case res.Reason == records.RefusedExists:
		return Conflict("a ban " + key + " exists")
	case res.Reason == records.RefusedMissing:
		return NotFound("no ban " + key)
	case res.Reason == records.RefusedVersion:
		return Conflict("the ban " + key + " is at version " + strconv.FormatUint(res.Version, 10))
	case res.Reason == records.RefusedFull:
		return Conflict("the ban list is full")
	case res.Reason != "":
		return BadRequest("the ban was refused: " + res.Reason)
	}
	return nil
}

// RateLimits are the administrable limits: the per-client PUBLISH rates and
// the node's connect rate, with their bursts.
type RateLimits struct {
	ClientMessageRate  int `json:"clientMessageRate"`
	ClientMessageBurst int `json:"clientMessageBurst"`
	ClientByteRate     int `json:"clientByteRate"`
	ClientByteBurst    int `json:"clientByteBurst"`
	ConnectRate        int `json:"connectRate"`
	ConnectBurst       int `json:"connectBurst"`
}

// RatesOf reads the administrable limits out of a configuration's.
func RatesOf(l config.Limits) RateLimits {
	return RateLimits{l.ClientMessageRate, l.ClientMessageBurst, l.ClientByteRate, l.ClientByteBurst, l.ConnectRate, l.ConnectBurst}
}

// limits is the configuration's limits carrying these rates.
func (r RateLimits) limits() config.Limits {
	return config.Limits{ClientMessageRate: r.ClientMessageRate, ClientMessageBurst: r.ClientMessageBurst, ClientByteRate: r.ClientByteRate,
		ClientByteBurst: r.ClientByteBurst, ConnectRate: r.ConnectRate, ConnectBurst: r.ConnectBurst}
}

// LimitsView is the configured limits and those in force, which differ by
// the administered record.
type LimitsView struct {
	Namespace    string                `json:"namespace"`
	Configured   RateLimits            `json:"configured"`
	Administered config.LimitsOverride `json:"administered"`
	InForce      RateLimits            `json:"inForce"`
	Version      uint64                `json:"version,omitempty"`
}

// OverloadView is the node's stress level and what it is made of.
type OverloadView struct {
	NodeLevel    uint8            `json:"nodeLevel"`
	ClusterLevel uint8            `json:"clusterLevel"`
	Indicators   map[string]int64 `json:"indicators"`
	Limits       map[string]int64 `json:"limits"`
	Peers        map[string]uint8 `json:"peers"`
	Shed         int              `json:"shedConnections"`
}

// namespaceOf reads the namespace a limits route is for: only the default
// until namespaces are active.
func namespaceOf(r *http.Request) (string, error) {
	ns := r.URL.Query().Get("namespace")
	if ns == "" || ns == config.DefaultLimitsKey {
		return config.DefaultLimitsKey, nil
	}
	return "", BadRequest("namespaces are not active: only the default namespace has limits")
}

func (s *Server) getLimits(r *http.Request) (any, error) {
	if _, err := namespaceOf(r); err != nil {
		return nil, err
	}
	return s.backend.Limits(), nil
}

func (s *Server) putLimits(r *http.Request) (any, error) {
	ns, err := namespaceOf(r)
	if err != nil {
		return nil, err
	}
	st, err := s.records()
	if err != nil {
		return nil, err
	}
	var o config.LimitsOverride
	if err := decodeBody(r, &o); err != nil {
		return nil, err
	}
	if msg := config.CheckOverride(s.backend.Limits().Configured.limits(), o); msg != "" {
		return nil, BadRequest(msg)
	}
	value, _ := json.Marshal(o)
	n := note(r)
	n.detail = map[string]string{"record": string(value)}
	ctx, cancel := context.WithTimeout(r.Context(), s.writeTimeout)
	defer cancel()
	res, err := st.Put(ctx, config.LimitsCollection, ns, value, nil, principal(r).Name, s.now().UTC())
	if err := banWriteError(res, err, ns); err != nil {
		return nil, err
	}
	// The node's own view follows the apply; the answer is the record in
	// force as written.
	v := s.backend.Limits()
	v.Administered, v.InForce, v.Version = o, RatesOf(v.Configured.limits().With(o)), res.Version
	return v, nil
}
