// Package cli is the hoardmq command surface. Run takes its arguments,
// environment and output streams so that every path can be exercised
// in-process, including the rule that a path performing no work performs no
// writes.
package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"runtime/debug"
	"strings"

	"github.com/happydave/hoardmq/ext"
	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/broker"
	"github.com/happydave/hoardmq/internal/config"
)

// Exit codes.
const (
	ExitOK       = 0
	ExitProblems = 1
	ExitUsage    = 2
)

type verb struct {
	name    string
	summary string
	run     func(a *app, args []string) int
}

var verbs = []verb{
	{name: "serve", summary: "validate the configuration and run the broker", run: runServe},
	{name: "drain", summary: "ask a running node to leave gracefully (redirect clients, hand leases off, exit)", run: runDrain},
	{name: "check", summary: "validate the configuration and report every problem", run: runCheck},
	{name: "clients", summary: "list, search, inspect, disconnect and invalidate clients (admin API)", run: runAdminGroup("clients")},
	{name: "retained", summary: "list and delete retained messages (admin API)", run: runAdminGroup("retained")},
	{name: "cluster", summary: "cluster status, the shard map and a client's lease (admin API)", run: runAdminGroup("cluster")},
	{name: "traces", summary: "start, stop and fetch a client's packet trace (admin API)", run: runAdminGroup("traces")},
	{name: "modules", summary: "list, enable and disable modules (admin API)", run: runAdminGroup("modules")},
	{name: "audit", summary: "the most recent audit events (admin API)", run: runAdminGroup("audit")},
	{name: "config", summary: "show the running configuration, validate a document, or reload this node's file (admin API)", run: runAdminGroup("config")},
	{name: "apikeys", summary: "list, create and revoke the cluster's API keys (admin API)", run: runAdminGroup("apikeys")},
	{name: "credentials", summary: "list, show, add, update and remove client credentials in the in-broker database (admin API)", run: runAdminGroup("credentials")},
	{name: "bans", summary: "list, add and remove bans by client identifier, username, address, prefix or pattern (admin API)", run: runAdminGroup("bans")},
	{name: "limits", summary: "show and set the per-client PUBLISH rates and the node's connect rate, cluster-wide (admin API)", run: runAdminGroup("limits")},
	{name: "whoami", summary: "the identity a credential authenticates as (admin API)", run: runWhoami},
	{name: "login", summary: "log in through an OpenID Connect provider and print the ID token the admin API accepts", run: runLogin},
	{name: "hash-password", summary: "read a password from stdin and print its hash for a users file (-scram: the SCRAM form; -argon2id: Argon2id)", run: runHashPassword},
	{name: "version", summary: "print the build version, the extension API's version and the modules compiled in", run: runVersion},
}

type app struct {
	environ []string
	stdin   io.Reader
	stdout  io.Writer
	stderr  io.Writer
}

// Run executes the command line and returns the process exit code. The
// broker's log writes to stderr.
func Run(args, environ []string, stdin io.Reader, stdout, stderr io.Writer) int {
	a := &app{environ: environ, stdin: stdin, stdout: stdout, stderr: stderr}
	fs := flag.NewFlagSet("hoardmq", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			usage(stdout)
			return ExitOK
		}
		fprintf(stderr, "hoardmq: %v\n\n", err)
		usage(stderr)
		return ExitUsage
	}
	rest := fs.Args()
	if len(rest) == 0 {
		usage(stderr)
		return ExitUsage
	}
	name := rest[0]
	if name == "help" {
		usage(stdout)
		return ExitOK
	}
	for _, v := range verbs {
		if v.name == name {
			return v.run(a, rest[1:])
		}
	}
	fprintf(stderr, "hoardmq: unknown command %q\n\n", name)
	usage(stderr)
	return ExitUsage
}

func usage(w io.Writer) {
	fprintln(w, "usage: hoardmq <command> [flags]")
	fprintln(w)
	fprintln(w, "commands:")
	for _, v := range verbs {
		fprintf(w, "  %-8s %s\n", v.name, v.summary)
	}
	fprintln(w)
	fprintln(w, "serve and check take -config PATH (default $"+config.EnvConfig+", else "+config.DefaultPath+").")
	fprintln(w, "the admin API verbs take -admin HOST:PORT (default $"+config.EnvAdminAddress+", else "+DefaultAdminAddress+") and -token-file (default $"+config.EnvAdminToken+").")
	fprintln(w, "exit codes: 0 ok, 1 configuration, start-up or API problem, 2 usage.")
	fprintln(w, "environment overrides applied before validation:")
	for _, name := range config.Overrides() {
		fprintln(w, "  "+name)
	}
}

// configFlags parses the flags shared by serve and check and returns the
// document path.
func (a *app) configFlags(name string, args []string) (string, int) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	path := fs.String("config", "", "configuration document path")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fprintf(a.stdout, "usage: hoardmq %s [-config PATH]\n", name)
			return "", ExitOK
		}
		fprintf(a.stderr, "hoardmq %s: %v\n", name, err)
		return "", ExitUsage
	}
	if fs.NArg() > 0 {
		fprintf(a.stderr, "hoardmq %s: unexpected argument %q\n", name, fs.Arg(0))
		return "", ExitUsage
	}
	if *path != "" {
		return *path, -1
	}
	if v, ok := a.env(config.EnvConfig); ok && v != "" {
		return v, -1
	}
	return config.DefaultPath, -1
}

func (a *app) env(name string) (string, bool) {
	for _, kv := range a.environ {
		if k, v, ok := strings.Cut(kv, "="); ok && k == name {
			return v, true
		}
	}
	return "", false
}

// loadPath reads and validates the document at path, printing problems. It
// returns the document and an exit code, or -1 when the caller should
// continue.
func (a *app) loadPath(path string) (config.Document, int) {
	doc, problems := config.Load(path, a.environ)
	if len(problems) > 0 {
		for _, p := range problems {
			fprintf(a.stderr, "%s: %s: %s\n", path, p.Pointer, p.Message)
		}
		return config.Document{}, ExitProblems
	}
	return doc, -1
}

func runCheck(a *app, args []string) int {
	path, code := a.configFlags("check", args)
	if code >= 0 {
		return code
	}
	doc, problems := config.Load(path, a.environ)
	if len(problems) == 0 {
		// Module configuration is validated read-only through the registry.
		problems = append(problems, broker.ValidateModules(doc, slog.New(slog.NewTextHandler(io.Discard, nil)))...)
	}
	if len(problems) > 0 {
		for _, p := range problems {
			fprintf(a.stderr, "%s: %s: %s\n", path, p.Pointer, p.Message)
		}
		return ExitProblems
	}
	fprintln(a.stdout, "ok")
	return ExitOK
}

func runHashPassword(a *app, args []string) int {
	fs := flag.NewFlagSet("hash-password", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	scram := fs.Bool("scram", false, "write the SCRAM-SHA-256 form, which also serves password login")
	iterations := fs.Int("iterations", auth.DefaultSCRAMIterations, "SCRAM iterations (at least 4096)")
	argon := fs.Bool("argon2id", false, "write the Argon2id form")
	memory := fs.Uint("memory", uint(auth.DefaultArgon2idParams.MemoryKiB), "Argon2id memory in KiB")
	passes := fs.Uint("time", uint(auth.DefaultArgon2idParams.Time), "Argon2id passes")
	lanes := fs.Uint("parallelism", uint(auth.DefaultArgon2idParams.Lanes), "Argon2id lanes")
	const usage = "usage: hoardmq hash-password [-scram [-iterations N] | -argon2id [-memory KiB] [-time N] [-parallelism N]] < password"
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fprintln(a.stdout, usage)
			return ExitOK
		}
		fprintf(a.stderr, "hoardmq hash-password: %v\n%s\n", err, usage)
		return ExitUsage
	}
	if fs.NArg() > 0 {
		fprintf(a.stderr, "hoardmq hash-password: unexpected argument %q (the password is read from stdin)\n", fs.Arg(0))
		return ExitUsage
	}
	if *scram && *iterations < auth.MinSCRAMIterations {
		fprintf(a.stderr, "hoardmq hash-password: -iterations must be at least %d\n", auth.MinSCRAMIterations)
		return ExitUsage
	}
	if *scram && *argon {
		fprintf(a.stderr, "hoardmq hash-password: -scram and -argon2id are different forms; choose one\n%s\n", usage)
		return ExitUsage
	}
	params := auth.Argon2idParams{MemoryKiB: uint32(min(*memory, 1<<32-1)), Time: uint32(min(*passes, 1<<32-1)), Lanes: uint8(min(*lanes, 255))}
	if *argon && !auth.Argon2idAvailable {
		fprintf(a.stderr, "hoardmq hash-password: %v\n", auth.ErrArgon2idUnavailable)
		return ExitProblems
	}
	if *argon {
		if err := params.Check(); err != nil {
			fprintf(a.stderr, "hoardmq hash-password: %v\n", err)
			return ExitUsage
		}
	}
	data, err := io.ReadAll(io.LimitReader(a.stdin, 4096))
	if err != nil {
		fprintf(a.stderr, "hoardmq hash-password: %v\n", err)
		return ExitProblems
	}
	password := strings.TrimRight(string(data), "\r\n")
	if password == "" {
		fprintln(a.stderr, "hoardmq hash-password: empty password")
		return ExitUsage
	}
	var phc string
	switch {
	case *scram:
		phc, err = auth.HashSCRAM([]byte(password), *iterations)
	case *argon:
		phc, err = auth.HashArgon2id([]byte(password), params)
	default:
		phc, err = auth.HashPassword([]byte(password))
	}
	if err != nil {
		fprintf(a.stderr, "hoardmq hash-password: %v\n", err)
		return ExitProblems
	}
	fprintln(a.stdout, phc)
	return ExitOK
}

func runVersion(a *app, args []string) int {
	if len(args) > 0 {
		fprintf(a.stderr, "hoardmq version: unexpected argument %q\n", args[0])
		return ExitUsage
	}
	fprintln(a.stdout, "hoardmq "+Version())
	fprintln(a.stdout, "extension API "+ext.APIVersion)
	ids := ext.Known()
	names := make([]string, len(ids))
	for i, id := range ids {
		names[i] = string(id)
	}
	fprintln(a.stdout, "modules: "+strings.Join(names, " "))
	return ExitOK
}

// brokerModule is the broker's module path: the main module of the default
// binary, a dependency of one hoardmq-build compiled.
const brokerModule = "github.com/happydave/hoardmq"

// Version derives the version string from build information: the broker
// module's version when set, else "devel" with the VCS revision when
// embedded.
func Version() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "devel"
	}
	return buildVersion(info)
}

// buildVersion reads the broker's version from the main module, or from the
// dependency list when hoardmq-build's generated module is the main one.
func buildVersion(info *debug.BuildInfo) string {
	if info.Main.Path == brokerModule || info.Main.Path == "" {
		return versionFrom(info.Main.Version, info.Settings)
	}
	for _, d := range info.Deps {
		if d.Path != brokerModule {
			continue
		}
		if d.Replace != nil && d.Replace.Version == "" {
			return "devel (" + d.Replace.Path + ")"
		}
		return d.Version
	}
	return "devel"
}

func versionFrom(main string, settings []debug.BuildSetting) string {
	if main != "" && main != "(devel)" {
		return main
	}
	rev, modified := "", false
	for _, s := range settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.modified":
			modified = s.Value == "true"
		}
	}
	if rev == "" {
		return "devel"
	}
	if len(rev) > 12 {
		rev = rev[:12]
	}
	if modified {
		rev += "-dirty"
	}
	return "devel (" + rev + ")"
}

// Output helpers discard the unrecoverable write error in one place.
func fprintf(w io.Writer, format string, a ...any) { _, _ = fmt.Fprintf(w, format, a...) }
func fprintln(w io.Writer, a ...any)               { _, _ = fmt.Fprintln(w, a...) }
