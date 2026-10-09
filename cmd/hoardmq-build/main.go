// Command hoardmq-build compiles a HoardMQ binary with extra modules: the
// built-in modules every binary carries, plus each module named with --with.
//
//	hoardmq-build [--hoardmq-version v | --hoardmq-dir dir]
//	              [--with module[@version][=dir]]... [--output file]
//	              [--tags list] [--keep]
//	hoardmq-build --main [--with ...] [--output file]
//
// It writes a throwaway Go module requiring the broker and each named module
// (a local directory through a replace) and a main package that imports them,
// and builds it with go build -mod=mod: go mod tidy would also need every
// module a dependency's tests import. --main prints the main package instead.
// A module registers itself with the extension API
// (github.com/happydave/hoardmq/ext) from its package's init.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"strings"
)

// Exit statuses.
const (
	exitOK    = 0
	exitBuild = 1
	exitUsage = 2
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// withFlags collects repeated --with values.
type withFlags []string

func (w *withFlags) String() string     { return strings.Join(*w, ",") }
func (w *withFlags) Set(v string) error { *w = append(*w, v); return nil }

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("hoardmq-build", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var with withFlags
	fs.Var(&with, "with", "a module to compile in: `module[@version][=dir]`; repeatable")
	version := fs.String("hoardmq-version", "", "the broker's module version to build")
	dir := fs.String("hoardmq-dir", "", "a local broker checkout to build, through a replace")
	output := fs.String("output", "", "the binary to write (default ./hoardmq); with --main, the file for main.go (default standard output)")
	tags := fs.String("tags", "", "build tags, passed to go build")
	keep := fs.Bool("keep", false, "keep the build directory and print where it is")
	mainOnly := fs.Bool("main", false, "print the generated main.go and build nothing")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	if fs.NArg() > 0 {
		fprintf(stderr, "hoardmq-build: unexpected argument %q\n", fs.Arg(0))
		return exitUsage
	}
	mods := make([]module, 0, len(with))
	for _, w := range with {
		m, err := parseWith(w)
		if err != nil {
			fprintf(stderr, "hoardmq-build: --with %q: %v\n", w, err)
			return exitUsage
		}
		mods = append(mods, m)
	}
	if *mainOnly {
		src := mainSource(mods)
		if *output == "" || *output == "-" {
			_, _ = stdout.Write(src)
			return exitOK
		}
		if err := os.WriteFile(*output, src, 0o644); err != nil {
			fprintf(stderr, "hoardmq-build: %v\n", err)
			return exitBuild
		}
		return exitOK
	}
	b, err := newBroker(*version, *dir)
	if err != nil {
		fprintf(stderr, "hoardmq-build: %v\n", err)
		return exitUsage
	}
	for i := range mods {
		if err := mods[i].resolve(); err != nil {
			fprintf(stderr, "hoardmq-build: --with %q: %v\n", with[i], err)
			return exitUsage
		}
	}
	out := *output
	if out == "" {
		out = "hoardmq"
	}
	out, err = filepath.Abs(out)
	if err != nil {
		fprintf(stderr, "hoardmq-build: %v\n", err)
		return exitUsage
	}
	work, err := os.MkdirTemp("", "hoardmq-build-")
	if err != nil {
		fprintf(stderr, "hoardmq-build: %v\n", err)
		return exitBuild
	}
	if *keep {
		fprintf(stderr, "hoardmq-build: build directory %s\n", work)
	} else {
		defer func() { _ = os.RemoveAll(work) }()
	}
	if err := writeModule(work, b, mods); err != nil {
		fprintf(stderr, "hoardmq-build: %v\n", err)
		return exitBuild
	}
	build := []string{"build", "-mod=mod", "-trimpath", "-o", out}
	if *tags != "" {
		build = append(build, "-tags", *tags)
	}
	build = append(build, ".")
	cmd := exec.Command("go", build...)
	cmd.Dir = work
	cmd.Env = append(os.Environ(), "GOWORK=off")
	cmd.Stdout, cmd.Stderr = stderr, stderr
	if err := cmd.Run(); err != nil {
		fprintf(stderr, "hoardmq-build: go %s: %v\n", strings.Join(build, " "), err)
		return exitBuild
	}
	return exitOK
}

// newBroker is the broker to build: a version, a local checkout, or with
// neither the version this tool was built at.
func newBroker(version, dir string) (broker, error) {
	switch {
	case version != "" && dir != "":
		return broker{}, errors.New("--hoardmq-version and --hoardmq-dir are exclusive")
	case dir != "":
		abs, err := filepath.Abs(dir)
		if err != nil {
			return broker{}, err
		}
		path, goLine, err := readGoMod(filepath.Join(abs, "go.mod"))
		if err != nil {
			return broker{}, err
		}
		if path != brokerModule {
			return broker{}, fmt.Errorf("%s is module %s, not %s", dir, path, brokerModule)
		}
		return broker{dir: abs, goLine: goLine}, nil
	case version != "":
		return broker{version: version, goLine: defaultGoLine}, nil
	}
	info, ok := debug.ReadBuildInfo()
	if !ok || info.Main.Version == "" || info.Main.Version == "(devel)" {
		return broker{}, errors.New("this build of hoardmq-build has no version: name --hoardmq-version or --hoardmq-dir")
	}
	return broker{version: info.Main.Version, goLine: defaultGoLine}, nil
}

// fprintf writes a message; a failed write to the terminal has nowhere to go.
func fprintf(w io.Writer, format string, a ...any) { _, _ = fmt.Fprintf(w, format, a...) }
