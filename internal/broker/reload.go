package broker

import (
	"io"
	"log/slog"
	"strings"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/session"
)

// ConfigSource is where a node's configuration came from, so a reload reads
// what a restart would: the file, the environment its overrides came from,
// and the level variable the node's loggers share.
type ConfigSource struct {
	Path    string
	Environ []string
	Level   *slog.LevelVar
}

// SetConfigSource names the node's configuration source. A server without
// one refuses a reload.
func (s *Server) SetConfigSource(src ConfigSource) {
	s.reloadMu.Lock()
	s.source = &src
	s.reloadMu.Unlock()
}

// ReloadResult is a reload's outcome. Applied is false when nothing changed
// in the running node; Problems then say why. On an applied reload Problems
// hold only modules that could not re-read their files and kept what they
// had.
type ReloadResult struct {
	Applied bool
	// RestartRequired is set when the document is valid but changes a member
	// only a restart applies; Problems name each.
	RestartRequired bool
	Changed         []string
	Problems        []config.Problem
}

// Outcome is the reload's outcome as the metric and the audit record name it.
func (r ReloadResult) Outcome() string {
	switch {
	case r.Applied:
		return "applied"
	case r.RestartRequired:
		return "refused"
	}
	return "invalid"
}

const restartMessage = "only a restart applies this change; nothing was applied"

// Reload reads the configuration from the node's source, validates it as
// hoardmq check does, and puts it into force when every member it changes is
// reloadable. Otherwise it changes nothing and reports why: every validation
// problem, or every restart-only member.
func (s *Server) Reload() ReloadResult {
	s.reloadMu.Lock()
	defer s.reloadMu.Unlock()
	r := s.reload()
	s.count("hoardmq_config_reloads_total", map[string]string{"outcome": r.Outcome()}, 1)
	if r.Applied {
		s.log.Info("configuration reloaded", "changed", r.Changed)
	}
	for _, p := range r.Problems {
		s.log.Warn("configuration reload", "outcome", r.Outcome(), "pointer", p.Pointer, "problem", p.Message)
	}
	return r
}

// ReloadOnSignal is Reload for a signal, audited as the admin API audits its
// call.
func (s *Server) ReloadOnSignal(signal string) ReloadResult {
	r := s.Reload()
	s.record("signal:"+signal, "config.reload", s.config().Node.Name, r.Outcome(), map[string]string{"changed": strings.Join(r.Changed, ",")})
	return r
}

func (s *Server) reload() ReloadResult {
	fail := func(msg string) ReloadResult {
		return ReloadResult{Problems: []config.Problem{{Pointer: "/", Message: msg}}}
	}
	switch {
	case !s.serving:
		return fail("the node is not serving: it has not started, or is shutting down")
	case s.source == nil:
		return fail("this node was not started from a configuration file")
	}
	doc, problems := config.Load(s.source.Path, s.source.Environ)
	if len(problems) == 0 {
		problems = ValidateModules(doc, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}
	if len(problems) > 0 {
		return ReloadResult{Problems: problems}
	}
	changed, restartOnly := config.Changes(*s.config(), doc)
	if len(restartOnly) > 0 {
		r := ReloadResult{RestartRequired: true, Changed: changed}
		for _, p := range restartOnly {
			r.Problems = append(r.Problems, config.Problem{Pointer: p, Message: restartMessage})
		}
		return r
	}
	s.doc.Store(&doc)
	s.putIntoForce(doc)
	return ReloadResult{Applied: true, Changed: changed, Problems: s.registry.Reload()}
}

// putIntoForce hands each component that reads a reloadable member its new
// value; none of them closes a connection.
func (s *Server) putIntoForce(doc config.Document) {
	if s.source.Level != nil {
		s.source.Level.Set(doc.Log.SlogLevel())
	}
	s.policy.reconfigure()
	s.policy.flap.cfg.Store(&doc.Flapping)
	s.sessions.SetLimits(session.Limits{MaxQueued: doc.Limits.MaxQueuedMessages, DropNewest: doc.Limits.QueueDrop == "newest",
		MaxSessionExpiry: uint32(doc.Limits.MaxSessionExpiry)})
	s.overload.setLimits(doc.Overload)
	for i, l := range s.listeners {
		l.SetMaxConnections(doc.Listeners[i].MaxConnections)
	}
}
