package cli

import (
	"encoding/json"
	"errors"
	"flag"
	"io"
	"path/filepath"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/datadir"
	"github.com/happydave/hoardmq/internal/walcheck"
)

// runWal is the wal verb: verify and repair a stopped node's logs.
func runWal(a *app, args []string) int {
	if len(args) == 0 || (args[0] != "verify" && args[0] != "repair") {
		fprintln(a.stderr, "usage: hoardmq wal verify [-config PATH] [-json]")
		fprintln(a.stderr, "       hoardmq wal repair [-config PATH] [-json] [-apply] [-discard]")
		return ExitUsage
	}
	sub := args[0]
	fs := flag.NewFlagSet("wal "+sub, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	path := fs.String("config", "", "configuration document path")
	asJSON := fs.Bool("json", false, "print the report as JSON")
	applyFlag, discard := new(bool), new(bool)
	if sub == "repair" {
		applyFlag = fs.Bool("apply", false, "make the repairs; without it nothing is written")
		discard = fs.Bool("discard", false, "keep no copy of what the repairs drop")
	}
	if err := fs.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fprintf(a.stdout, "usage: hoardmq wal %s [flags]\n", sub)
			return ExitOK
		}
		fprintf(a.stderr, "hoardmq wal %s: %v\n", sub, err)
		return ExitUsage
	}
	if fs.NArg() > 0 {
		fprintf(a.stderr, "hoardmq wal %s: unexpected argument %q\n", sub, fs.Arg(0))
		return ExitUsage
	}
	if *path == "" {
		if v, ok := a.env(config.EnvConfig); ok && v != "" {
			*path = v
		} else {
			*path = config.DefaultPath
		}
	}
	doc, code := a.loadPath(*path)
	if code >= 0 {
		return code
	}
	paths := walcheck.NodePaths(doc.Node.DataDir, stateParent(doc))
	var lock *datadir.Lock
	if sub == "repair" && *applyFlag {
		lock = lockDataDir(doc.Node.DataDir)
	} else {
		lock = lockShared(doc.Node.DataDir)
	}
	defer func() { _ = lock.Release() }()
	if sub == "verify" && (lock.Outcome == datadir.HeldElsewhere || lock.Outcome == datadir.Failed) {
		fprintf(a.stderr, "hoardmq wal verify: data directory %s: %s; stop the node first\n", doc.Node.DataDir, lock.Describe())
		return ExitProblems
	}
	var r *walcheck.Report
	if sub == "verify" {
		r = walcheck.Verify(paths, lock)
	} else {
		r = walcheck.Repair(paths, lock, walcheck.Options{Apply: *applyFlag, Discard: *discard})
	}
	if *asJSON {
		enc := json.NewEncoder(a.stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(r); err != nil {
			fprintf(a.stderr, "hoardmq wal %s: %v\n", sub, err)
			return ExitProblems
		}
	} else if err := walcheck.WriteText(a.stdout, r); err != nil {
		fprintf(a.stderr, "hoardmq wal %s: %v\n", sub, err)
		return ExitProblems
	}
	return walExit(r)
}

// walExit is the command's status: 1 when a start would refuse, or a
// repair refused, failed or left the log needing more.
func walExit(r *walcheck.Report) int {
	part := r.Repair
	switch {
	case part == nil:
		if r.Verdict == walcheck.VerdictStartRefuses {
			return ExitProblems
		}
	case part.Refused != "", part.Error != "":
		return ExitProblems
	case part.After != nil && part.After.Verdict != walcheck.VerdictClean:
		return ExitProblems
	}
	return ExitOK
}

// stateParent is the directory store.wal opens its state log under: its dir,
// or the data directory's logs.
func stateParent(doc config.Document) string {
	var c struct {
		Dir string `json:"dir"`
	}
	if raw, ok := doc.Modules["store.wal"]; ok {
		_ = json.Unmarshal(raw, &c)
	}
	if c.Dir != "" {
		return c.Dir
	}
	return filepath.Join(doc.Node.DataDir, "logs")
}
