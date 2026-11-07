// Command breakrun runs a work item's break pass: for each break in a spec it
// applies the break's edits, runs the named tests, restores the files and
// reports the outcome. It never leaves a file changed on any exit it can
// catch, and it refuses to start on a tree whose spec'd files are already
// changed, so a run that was killed is found.
//
//	go run ./tools/breakrun -spec breaks.json -out <dir> [-only id,id]
//
// Run it from the root of the checkout. The spec is a JSON list of breaks:
//
//	[{"id": "B1", "clause": "what the break removes",
//	  "edits": [{"path": "internal/x/x.go", "old": "a", "new": "b"}],
//	  "pkg": "./internal/x/", "run": "^TestX$", "expect": "the assertion's text"}]
//
// "old" must occur exactly once in its file. "expect" is optional: given, a
// break is caught only when the test log holds it; not given, by any failed
// test. Each break's go test output goes to <out>/break-<id>.log and its
// outcome to <out>/summary, one line each.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
)

// Break is one break of a spec.
type Break struct {
	ID     string `json:"id"`
	Clause string `json:"clause"`
	Edits  []Edit `json:"edits"`
	Pkg    string `json:"pkg"`
	Run    string `json:"run"`
	Expect string `json:"expect"`
}

// Edit replaces Old, which must occur once in Path, with New.
type Edit struct {
	Path string `json:"path"`
	Old  string `json:"old"`
	New  string `json:"new"`
}

// The outcomes a break can have.
const (
	Caught          = "caught"
	FailedElsewhere = "failed-elsewhere"
	Survived        = "survived"
	DidNotBuild     = "did-not-build"
	RanNoTest       = "ran-no-test"
	AnchorMiss      = "anchor-miss"
)

// Classify decides a break's outcome from its go test exit status and log:
// a build failure first, then a failure carrying expect (any failed test
// when expect is empty), then a failure elsewhere, then no test run, then a
// survival. A TestMain that fails with expect's text is caught, though no
// test function ran.
func Classify(exit int, log, expect string) string {
	switch {
	case strings.Contains(log, "[build failed]") || strings.Contains(log, "[setup failed]"):
		return DidNotBuild
	case exit != 0 && expect != "" && strings.Contains(log, expect):
		return Caught
	case exit != 0 && expect == "" && strings.Contains(log, "--- FAIL"):
		return Caught
	case exit != 0:
		return FailedElsewhere
	case strings.Contains(log, "[no test files]") || strings.Contains(log, "no tests to run") || !strings.Contains(log, "=== RUN"):
		return RanNoTest
	}
	return Survived
}

// restorer holds the original bytes of every file a break has changed, and
// puts them back once.
type restorer struct {
	mu    sync.Mutex
	saved map[string][]byte
	order []string
}

func (r *restorer) save(path string, data []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.saved[path]; !ok {
		r.saved[path] = data
		r.order = append(r.order, path)
	}
}

// restore writes every saved file back and reports any it could not.
func (r *restorer) restore() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var errs []error
	for _, p := range r.order {
		if err := os.WriteFile(p, r.saved[p], 0o644); err != nil {
			errs = append(errs, err)
			continue
		}
		got, err := os.ReadFile(p)
		if err != nil || !bytes.Equal(got, r.saved[p]) {
			errs = append(errs, fmt.Errorf("%s: not restored", p))
		}
	}
	r.saved, r.order = map[string][]byte{}, nil
	return errors.Join(errs...)
}

// apply makes b's edits, saving each file first. An anchor that does not
// occur exactly once stops it with the edits made so far saved, for the
// caller to restore.
func apply(b Break, r *restorer) error {
	for _, e := range b.Edits {
		data, err := os.ReadFile(e.Path)
		if err != nil {
			return err
		}
		if n := bytes.Count(data, []byte(e.Old)); n != 1 {
			return fmt.Errorf("%s: anchor occurs %d times", e.Path, n)
		}
		r.save(e.Path, data)
		if err := os.WriteFile(e.Path, bytes.Replace(data, []byte(e.Old), []byte(e.New), 1), 0o644); err != nil {
			return err
		}
	}
	return nil
}

// dirty lists the spec's files that git reports changed, so a tree a killed
// run left behind is refused.
func dirty(breaks []Break) ([]string, error) {
	seen := map[string]bool{}
	args := []string{"status", "--porcelain", "--"}
	for _, b := range breaks {
		for _, e := range b.Edits {
			if !seen[e.Path] {
				seen[e.Path] = true
				args = append(args, e.Path)
			}
		}
	}
	out, err := exec.Command("git", args...).Output()
	if err != nil {
		return nil, err
	}
	var paths []string
	for _, l := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if l != "" {
			paths = append(paths, l)
		}
	}
	return paths, nil
}

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	fs := flag.NewFlagSet("breakrun", flag.ContinueOnError)
	spec := fs.String("spec", "", "the spec file")
	out := fs.String("out", "", "the output directory, made if absent")
	only := fs.String("only", "", "comma-separated break identifiers to run, all by default")
	if err := fs.Parse(args); err != nil || *spec == "" || *out == "" {
		fmt.Fprintln(os.Stderr, "usage: breakrun -spec <file.json> -out <dir> [-only id,id]")
		return 2
	}
	data, err := os.ReadFile(*spec)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	var breaks []Break
	if err := json.Unmarshal(data, &breaks); err != nil {
		fmt.Fprintln(os.Stderr, "spec:", err)
		return 2
	}
	if *only != "" {
		want := map[string]bool{}
		for _, id := range strings.Split(*only, ",") {
			want[id] = true
		}
		var kept []Break
		for _, b := range breaks {
			if want[b.ID] {
				kept = append(kept, b)
			}
		}
		breaks = kept
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	if d, err := dirty(breaks); err != nil || len(d) > 0 {
		fmt.Fprintf(os.Stderr, "refusing: the spec's files are not clean (%v): %s\n", err, strings.Join(d, "; "))
		return 2
	}
	summary, err := os.Create(filepath.Join(*out, "summary"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	defer func() { _ = summary.Close() }()

	r := &restorer{saved: map[string][]byte{}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	interrupted := make(chan struct{})
	go func() {
		if _, ok := <-sigs; ok {
			close(interrupted)
			cancel()
		}
	}()
	defer func() {
		signal.Stop(sigs)
		close(sigs)
	}()
	defer func() {
		if p := recover(); p != nil {
			_ = r.restore()
			panic(p)
		}
	}()

	code := 0
	for _, b := range breaks {
		line, err := one(ctx, b, r, *out)
		if rerr := r.restore(); rerr != nil {
			fmt.Fprintln(os.Stderr, "restore:", rerr)
			return 1
		}
		select {
		case <-interrupted:
			_, _ = fmt.Fprintf(summary, "%s interrupted: the tree is restored\n", b.ID)
			return 130
		default:
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			code = 1
		}
		_, _ = fmt.Fprintln(summary, line)
		fmt.Println(line)
	}
	return code
}

// one applies b, runs its tests and classifies them; the caller restores.
func one(ctx context.Context, b Break, r *restorer, out string) (string, error) {
	if err := apply(b, r); err != nil {
		return fmt.Sprintf("%s %s (%v) :: %s", b.ID, AnchorMiss, err, b.Clause), nil
	}
	logPath := filepath.Join(out, "break-"+b.ID+".log")
	f, err := os.Create(logPath)
	if err != nil {
		return "", err
	}
	cmd := exec.CommandContext(ctx, "go", "test", "-count=1", "-timeout", "300s", "-run", b.Run, "-v", b.Pkg)
	cmd.Stdout, cmd.Stderr = f, f
	runErr := cmd.Run()
	_ = f.Close()
	exit := 0
	var ee *exec.ExitError
	if errors.As(runErr, &ee) {
		exit = ee.ExitCode()
	} else if runErr != nil {
		return "", runErr
	}
	log, err := os.ReadFile(logPath)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s %s exit=%d :: %s", b.ID, Classify(exit, string(log), b.Expect), exit, b.Clause), nil
}
