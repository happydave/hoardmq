package sinkpostgres

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/happydave/hoardmq/ext/mqtt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type routeConfig struct {
	Filter    string `json:"filter"`
	Table     string `json:"table"`
	Namespace string `json:"namespace"`
}

type config struct {
	ConnString   string        `json:"connString"`
	PasswordFile string        `json:"passwordFile"`
	Routes       []routeConfig `json:"routes"`
	Batch        struct {
		MaxRows  *int `json:"maxRows"`
		LingerMs *int `json:"lingerMs"`
	} `json:"batch"`
	Buffer struct {
		MaxRecords *int64 `json:"maxRecords"`
		MaxBytes   *int64 `json:"maxBytes"`
		WhenFull   string `json:"whenFull"`
		WaitMs     *int   `json:"waitMs"`
	} `json:"buffer"`
	FlushMs   *int `json:"flushTimeoutMs"`
	TimeoutMs *int `json:"timeoutMs"`
}

// Defaults and bounds; the buffer's are sink.kafka's.
const (
	maxRoutes         = 1000
	defaultMaxRows    = 1000
	maxMaxRows        = 10_000
	defaultLingerMs   = 5
	maxLingerMs       = 1000
	defaultMaxRecords = 100_000
	defaultMaxBytes   = 64 << 20
	minMaxBytes       = 1 << 20
	defaultWaitMs     = 1000
	maxWaitMs         = 10_000
	defaultFlushMs    = 5000
	maxFlushMs        = 60_000
	defaultTimeoutMs  = 10_000
	minTimeoutMs      = 100
	maxTimeoutMs      = 60_000
)

var identPart = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)

// route is one validated route.
type route struct {
	filter, namespace string
	table             string // as written, the metric label
	ident             pgx.Identifier
}

// settings is a validated configuration.
type settings struct {
	pool                 *pgxpool.Config
	routes               []route
	maxRows              int
	linger, wait, flush  time.Duration
	timeout              time.Duration
	maxRecords, maxBytes int64
	refuse               bool
}

func intOr(p *int, def int) int {
	if p == nil {
		return def
	}
	return *p
}

func int64Or(p *int64, def int64) int64 {
	if p == nil {
		return def
	}
	return *p
}

func (c config) validate(ns string, readFile func(string) ([]byte, error)) (*settings, []string) {
	var problems []string
	add := func(format string, a ...any) { problems = append(problems, fmt.Sprintf(format, a...)) }
	s := &settings{}
	switch {
	case c.ConnString == "":
		add("connString is required")
	case hasPassword(c.ConnString):
		add("connString holds a password; put it in passwordFile")
	default:
		pc, err := pgxpool.ParseConfig(c.ConnString)
		switch {
		case err != nil:
			add("connString: %v", err)
		case !secure(&pc.ConnConfig.Config):
			add("connString: a host that is not this machine needs sslmode verify-full or verify-ca (or require with sslrootcert): the database password would cross the network in clear or to an unverified server")
		default:
			s.pool = pc
			s.pool.MinConns = 0
			s.pool.MaxConns = 2
		}
	}
	if c.PasswordFile != "" {
		data, err := readFile(c.PasswordFile)
		switch {
		case err != nil:
			add("passwordFile %s is unreadable", c.PasswordFile)
		case s.pool != nil:
			s.pool.ConnConfig.Password = strings.TrimSuffix(strings.TrimSuffix(string(data), "\n"), "\r")
		}
	}
	switch {
	case len(c.Routes) == 0:
		add("routes: at least one route")
	case len(c.Routes) > maxRoutes:
		add("routes: at most %d", maxRoutes)
	}
	for i, rc := range c.Routes {
		at := fmt.Sprintf("routes/%d", i)
		r := route{filter: rc.Filter, namespace: rc.Namespace, table: rc.Table}
		if r.namespace == "" {
			r.namespace = ns
		}
		if why := mqtt.ValidateTopicFilter(rc.Filter); why != "" {
			add("%s/filter: %s", at, why)
		} else if mqtt.IsSharedFilter(rc.Filter) {
			add("%s/filter: a shared filter matches no topic", at)
		}
		parts := strings.Split(rc.Table, ".")
		ok := len(parts) <= 2
		for _, p := range parts {
			ok = ok && identPart.MatchString(p)
		}
		if !ok {
			add("%s/table: name or schema.name, each a letter or _ then up to 62 of letters, digits and _", at)
		}
		r.ident = pgx.Identifier(parts)
		s.routes = append(s.routes, r)
	}
	rangeInt := func(name string, p *int, lo, hi, def int) int {
		v := intOr(p, def)
		if v < lo || v > hi {
			add("%s must be %d to %d", name, lo, hi)
		}
		return v
	}
	s.maxRows = rangeInt("batch.maxRows", c.Batch.MaxRows, 1, maxMaxRows, defaultMaxRows)
	s.linger = time.Duration(rangeInt("batch.lingerMs", c.Batch.LingerMs, 0, maxLingerMs, defaultLingerMs)) * time.Millisecond
	s.wait = time.Duration(rangeInt("buffer.waitMs", c.Buffer.WaitMs, 0, maxWaitMs, defaultWaitMs)) * time.Millisecond
	s.flush = time.Duration(rangeInt("flushTimeoutMs", c.FlushMs, 0, maxFlushMs, defaultFlushMs)) * time.Millisecond
	s.timeout = time.Duration(rangeInt("timeoutMs", c.TimeoutMs, minTimeoutMs, maxTimeoutMs, defaultTimeoutMs)) * time.Millisecond
	if s.maxRecords = int64Or(c.Buffer.MaxRecords, defaultMaxRecords); s.maxRecords < 1 {
		add("buffer.maxRecords must be at least 1")
	}
	if s.maxBytes = int64Or(c.Buffer.MaxBytes, defaultMaxBytes); s.maxBytes < minMaxBytes {
		add("buffer.maxBytes must be at least %d", minMaxBytes)
	}
	switch c.Buffer.WhenFull {
	case "", "refuse":
		s.refuse = true
	case "drop":
	default:
		add(`buffer.whenFull must be "refuse" or "drop"`)
	}
	if len(problems) > 0 {
		return nil, problems
	}
	return s, nil
}

// matches returns the routes a message's topic matches, one per distinct
// table, the first route naming a table deciding.
func (s *settings) matches(m *mqtt.Message) []route {
	var out []route
	seen := map[string]bool{}
	for _, r := range s.routes {
		if r.namespace != m.Namespace || !mqtt.MatchTopic(r.filter, m.Topic) || seen[r.table] {
			continue
		}
		seen[r.table] = true
		out = append(out, r)
	}
	return out
}
