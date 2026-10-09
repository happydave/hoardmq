// Package builtin holds the modules every HoardMQ binary carries: the
// authenticators and the file authorizer, the log-backed store, the metrics
// providers and the OTLP trace exporter. Each registers itself with the
// extension API at init and reaches the broker only through that API, as an
// external module does; the seam tests in the module package hold it there.
package builtin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/happydave/hoardmq/ext"
	extauth "github.com/happydave/hoardmq/ext/auth"
	extstore "github.com/happydave/hoardmq/ext/store"
	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/store"
)

func init() {
	ext.Register("auth.file", func() ext.Module { return &fileAuth{} })
	ext.Register("authz.file", func() ext.Module { return &fileAuthz{} })
	ext.Register("store.wal", func() ext.Module { return &walStore{} })
	ext.Register("metrics.internal", func() ext.Module { return &metrics{} })
}

type pathConfig struct {
	Path string `json:"path"`
}

func decode(raw json.RawMessage, v any) error {
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

// toggle is the enabled flag shared by toggleable built-ins.
type toggle struct{ enabled atomic.Bool }

func (t *toggle) SetEnabled(b bool) { t.enabled.Store(b) }

// fileAuth is auth.file: username and password against a users file.
type fileAuth struct {
	toggle
	inner *auth.FileAuthenticator
}

func (*fileAuth) ID() ext.ID                   { return "auth.file" }
func (*fileAuth) Capabilities() ext.Capability { return ext.CapAuthentication }
func (*fileAuth) Start(context.Context) error  { return nil }
func (*fileAuth) Stop(context.Context) error   { return nil }
func (f *fileAuth) Reload() error              { return f.inner.Reload() }
func (f *fileAuth) Provision(ctx *ext.Context, raw json.RawMessage) error {
	var cfg pathConfig
	if err := decode(raw, &cfg); err != nil {
		return err
	}
	if cfg.Path == "" {
		return errors.New("path is required")
	}
	a, err := auth.LoadUsersFile(cfg.Path)
	if err != nil {
		return err
	}
	a.Cache = ctx.Passwords
	f.inner = a
	f.enabled.Store(true)
	ctx.Logger.Info("users file loaded", "path", cfg.Path)
	return nil
}

func (f *fileAuth) Authenticate(ctx context.Context, req *extauth.Request) extauth.Result {
	return f.inner.Authenticate(ctx, req)
}

// fileAuthz is authz.file: ACL rules from a file.
type fileAuthz struct {
	toggle
	inner *auth.FileAuthorizer
}

func (*fileAuthz) ID() ext.ID                   { return "authz.file" }
func (*fileAuthz) Capabilities() ext.Capability { return ext.CapAuthorization }
func (*fileAuthz) Start(context.Context) error  { return nil }
func (*fileAuthz) Stop(context.Context) error   { return nil }
func (f *fileAuthz) Reload() error              { return f.inner.Reload() }
func (f *fileAuthz) Provision(ctx *ext.Context, raw json.RawMessage) error {
	var cfg pathConfig
	if err := decode(raw, &cfg); err != nil {
		return err
	}
	if cfg.Path == "" {
		return errors.New("path is required")
	}
	a, err := auth.LoadACLFile(cfg.Path)
	if err != nil {
		return err
	}
	f.inner = a
	f.enabled.Store(true)
	ctx.Logger.Info("acl file loaded", "path", cfg.Path)
	return nil
}

func (f *fileAuthz) Authorize(ctx context.Context, action extauth.Action, topic string, id *extauth.Identity) extauth.Decision {
	return f.inner.Authorize(ctx, action, topic, id)
}

// walStore is store.wal: the log-backed store provider. Provision only
// records the directory; Start opens the log, so hoardmq check creates
// nothing.
type walStore struct {
	dir   string
	opts  store.Options
	inner *store.Store
}

// SyncStats exposes the journal's group-commit counters (WI 1292 round 4)
// for the broker's lock-free StormStats; nil-safe before Open.
func (w *walStore) SyncStats() extstore.SyncStats {
	if w.inner == nil {
		return extstore.SyncStats{}
	}
	return w.inner.SyncStats()
}

// AppendStats exposes what the store's lock was held for by appends;
// nil-safe before Open.
func (w *walStore) AppendStats() extstore.AppendStats {
	if w.inner == nil {
		return extstore.AppendStats{}
	}
	return w.inner.AppendStats()
}

type walConfig struct {
	Dir string `json:"dir"`
}

func (*walStore) ID() ext.ID                   { return "store.wal" }
func (*walStore) Capabilities() ext.Capability { return ext.CapStore }
func (w *walStore) Provision(ctx *ext.Context, raw json.RawMessage) error {
	var cfg walConfig
	if err := decode(raw, &cfg); err != nil {
		return err
	}
	w.dir = cfg.Dir
	if w.dir == "" {
		w.dir = filepath.Join(ctx.DataDir, "logs")
	}
	w.opts = store.Options{
		SyncPolicy:     store.SyncPolicy(ctx.Store.SyncPolicy),
		SyncInterval:   ctx.Store.SyncInterval,
		SegmentBytes:   ctx.Store.SegmentBytes,
		GroupMaxDelay:  ctx.Store.GroupMaxDelay,
		GroupMaxSize:   ctx.Store.GroupMaxSize,
		Sync:           ctx.Store.Sync,
		IgnoreChecksum: ctx.Store.IgnoreChecksum,
		Logger:         ctx.Logger,
	}
	if w.dir == "" {
		return errors.New("no data directory")
	}
	if info, err := os.Stat(w.dir); err == nil && !info.IsDir() {
		return fmt.Errorf("%s is not a directory", w.dir)
	}
	return nil
}

func (w *walStore) Start(context.Context) error {
	s, err := store.Open(w.dir, w.opts)
	if err != nil {
		return err
	}
	w.inner = s
	return nil
}

func (w *walStore) Stop(context.Context) error {
	if w.inner == nil {
		return nil
	}
	return w.inner.Close()
}

func (w *walStore) Journal() extstore.Journal {
	if w.inner == nil {
		return nil
	}
	return w.inner
}
func (w *walStore) Recover() (*extstore.State, error) { return w.inner.Recover() }

// SetTap installs the replication layer's record tap on the open store.
func (w *walStore) SetTap(tap func(extstore.Record) func()) {
	if w.inner != nil {
		w.inner.SetTap(tap)
	}
}
func (w *walStore) Checkpoint(snap extstore.Snapshot) error { return w.inner.Checkpoint(snap) }
func (w *walStore) NeedsCheckpoint() bool                   { return w.inner.NeedsCheckpoint() }
func (w *walStore) Close() error                            { return w.Stop(context.Background()) }

// JournalStats exposes the journal's position, checkpoints and fsyncs, read
// without the store's lock; nil-safe before Open.
func (w *walStore) JournalStats() extstore.JournalStats {
	if w.inner == nil {
		return extstore.JournalStats{}
	}
	return w.inner.JournalStats()
}

// LastIndex is the log position, for the metrics collector.
func (w *walStore) LastIndex() uint64 {
	if w.inner == nil {
		return 0
	}
	return w.inner.LastIndex()
}

// metrics is metrics.internal: in-memory counters and gauges.
type metrics struct {
	toggle
	mu         sync.Mutex
	series     map[string]*ext.Metric
	collectors []func(set func(string, map[string]string, float64))
}

// AddCollector implements ext.Collector.
func (m *metrics) AddCollector(f func(set func(string, map[string]string, float64))) {
	m.mu.Lock()
	m.collectors = append(m.collectors, f)
	m.mu.Unlock()
}

func (*metrics) ID() ext.ID                   { return "metrics.internal" }
func (*metrics) Capabilities() ext.Capability { return ext.CapMetrics }
func (m *metrics) Provision(_ *ext.Context, raw json.RawMessage) error {
	var cfg struct{}
	if err := decode(raw, &cfg); err != nil {
		return err
	}
	m.series = map[string]*ext.Metric{}
	m.enabled.Store(true)
	return nil
}
func (*metrics) Start(context.Context) error { return nil }
func (*metrics) Stop(context.Context) error  { return nil }

func key(name string, labels map[string]string) string {
	if len(labels) == 0 {
		return name
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(name)
	for _, k := range keys {
		b.WriteString("{")
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(labels[k])
		b.WriteString("}")
	}
	return b.String()
}

func (m *metrics) get(name string, labels map[string]string) *ext.Metric {
	k := key(name, labels)
	s, ok := m.series[k]
	if !ok {
		cp := make(map[string]string, len(labels))
		for lk, lv := range labels {
			cp[lk] = lv
		}
		s = &ext.Metric{Name: name, Labels: cp}
		m.series[k] = s
	}
	return s
}

func (m *metrics) Add(name string, labels map[string]string, delta float64) {
	m.mu.Lock()
	m.get(name, labels).Value += delta
	m.mu.Unlock()
}

func (m *metrics) Set(name string, labels map[string]string, value float64) {
	m.mu.Lock()
	m.get(name, labels).Value = value
	m.mu.Unlock()
}

func (m *metrics) Snapshot() []ext.Metric {
	m.mu.Lock()
	collectors := m.collectors
	m.mu.Unlock()
	for _, c := range collectors {
		c(m.Set)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]ext.Metric, 0, len(m.series))
	for _, s := range m.series {
		out = append(out, *s)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		return key(out[i].Name, out[i].Labels) < key(out[j].Name, out[j].Labels)
	})
	return out
}
