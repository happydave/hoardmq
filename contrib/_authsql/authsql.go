// Package authsql is auth.sql, a HoardMQ authenticator that checks a client's
// password against a PHC hash a configured PostgreSQL query reads. It is
// built outside the broker's module, on the public extension API, so pgx is
// a dependency of this module alone. Build it into a binary with
//
//	hoardmq-build --with github.com/happydave/hoardmq-authsql=<this directory>
//
// and configure it as {"modules": {"auth.sql": {"connString": …, "query": …}}}.
//
// The password is verified by the broker's own verifier, so every form
// hoardmq hash-password writes is accepted, and the password is never sent
// to the database: the query's parameters are the username, the client
// identifier and the certificate's common name.
package authsql

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/happydave/hoardmq/ext"
	"github.com/happydave/hoardmq/ext/auth"
)

// ID is auth.sql's identifier.
const ID ext.ID = "auth.sql"

// MaxAttributes bounds the attributes one row attaches, as auth.db does.
const MaxAttributes = 32

const (
	metricDecisions   = "hoardmq_auth_sql_decisions_total"
	metricUnavailable = "hoardmq_auth_sql_unavailable_total"
)

func init() {
	ext.Register(ID, func() ext.Module { return &module{readFile: os.ReadFile, open: openPool} })
}

// lookup runs the rewritten query. Each row's first value is the PHC string
// and the rest the attribute columns, in the columns' order; nil is NULL.
type lookup interface {
	Query(ctx context.Context, sql string, args []any) (columns []string, rows [][]*string, err error)
	Close()
}

type module struct {
	cfg       config
	set       *settings
	log       *slog.Logger
	passwords ext.PasswordVerifier
	readFile  func(string) ([]byte, error)
	open      func(*settings) (lookup, error)

	db      atomic.Pointer[lookupBox]
	metrics atomic.Pointer[metricsBox]
}

type lookupBox struct{ l lookup }
type metricsBox struct{ m ext.MetricsProvider }

func (*module) ID() ext.ID { return ID }

func (*module) Capabilities() ext.Capability { return ext.CapAuthentication }

func (m *module) Provision(ctx *ext.Context, raw json.RawMessage) error {
	if err := decodeStrict(raw, &m.cfg); err != nil {
		return err
	}
	m.log, m.passwords = ctx.Logger, ctx.Passwords
	if m.log == nil {
		m.log = slog.Default()
	}
	return nil
}

// Validate checks the configuration and reads passwordFile; it connects to
// nothing.
func (m *module) Validate() error {
	set, problems := m.cfg.validate(m.readFile)
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	m.set = set
	return nil
}

// UseMetrics receives the node's metrics provider.
func (m *module) UseMetrics(p ext.MetricsProvider) { m.metrics.Store(&metricsBox{m: p}) }

// Start builds the pool, which connects on first use, so a database that is
// down at start does not stop the broker.
func (m *module) Start(context.Context) error {
	if m.set == nil {
		if err := m.Validate(); err != nil {
			return err
		}
	}
	l, err := m.open(m.set)
	if err != nil {
		return err
	}
	m.db.Store(&lookupBox{l: l})
	return nil
}

// Stop closes the pool. The close waits for lookups in flight, each bounded
// by timeoutMs.
func (m *module) Stop(context.Context) error {
	if b := m.db.Swap(nil); b != nil {
		b.l.Close()
	}
	return nil
}

func (m *module) count(name string, labels map[string]string) {
	if b := m.metrics.Load(); b != nil {
		b.m.Add(name, labels, 1)
	}
}

// claims is the claim rule auth.ldap has: a non-empty username and a
// non-empty password; anything else passes without a query.
func claims(req *auth.Request) bool {
	return req.HasUsername && req.Username != "" && req.HasPassword && len(req.Password) > 0
}

// Authenticate looks the client up and verifies its password.
func (m *module) Authenticate(ctx context.Context, req *auth.Request) auth.Result {
	if !claims(req) {
		return auth.Result{Decision: auth.Pass}
	}
	ctx, cancel := context.WithTimeout(ctx, m.set.timeout)
	defer cancel()
	var (
		cols []string
		rows [][]*string
		err  = errors.New("the module is stopped")
	)
	if b := m.db.Load(); b != nil {
		cols, rows, err = b.l.Query(ctx, m.set.query, m.set.args(req))
	}
	var res auth.Result
	var outcome string
	if err != nil {
		res, outcome = m.set.unavailable()
		policy := "deny"
		if m.set.passOnUnavailable {
			policy = "pass"
		}
		m.count(metricUnavailable, map[string]string{"policy": policy})
		m.log.Warn("auth.sql database unavailable", "policy", policy, "err", err.Error())
	} else {
		res, outcome = decide(cols, rows, func(stored string) (bool, error) { return m.passwords.Verify(stored, req.Password) })
	}
	m.count(metricDecisions, map[string]string{"outcome": outcome})
	return res
}

// decide is the decision on a lookup's rows: no row passes, one row is
// verified, more than one is refused.
func decide(cols []string, rows [][]*string, verify func(stored string) (bool, error)) (auth.Result, string) {
	switch {
	case len(rows) == 0:
		return auth.Result{Decision: auth.Pass}, "unknown"
	case len(rows) > 1:
		return auth.Result{Decision: auth.Deny, Reason: "sql: ambiguous user"}, "refused"
	}
	row := rows[0]
	if len(row) == 0 || row[0] == nil || *row[0] == "" {
		return auth.Result{Decision: auth.Deny, Reason: "sql: no password hash"}, "refused"
	}
	attrs := map[string]string{}
	for i := 1; i < len(row) && i < len(cols); i++ {
		if row[i] != nil {
			attrs[cols[i]] = *row[i]
		}
	}
	if len(attrs) > MaxAttributes {
		return auth.Result{Decision: auth.Deny, Reason: "sql: more than 32 attributes"}, "refused"
	}
	match, err := verify(*row[0])
	if err != nil {
		return auth.Result{Decision: auth.Deny, Reason: "sql: stored credential unreadable"}, "refused"
	}
	if !match {
		return auth.Result{Decision: auth.Deny, Reason: "sql: wrong password"}, "refused"
	}
	if len(attrs) == 0 {
		attrs = nil
	}
	return auth.Result{Decision: auth.Allow, Attributes: attrs}, "allowed"
}

// unavailable is the policy's decision on a database that gave no answer.
func (s *settings) unavailable() (auth.Result, string) {
	if s.passOnUnavailable {
		return auth.Result{Decision: auth.Pass, Reason: "sql: database unavailable"}, "unavailable"
	}
	return auth.Result{Decision: auth.Deny, Reason: "sql: database unavailable"}, "unavailable"
}

// timeoutOf is timeoutMs as a duration.
func timeoutOf(ms int) time.Duration { return time.Duration(ms) * time.Millisecond }
