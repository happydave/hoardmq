package gatechain

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// start runs chain.sh over steps with out and dir in a fresh temporary
// directory, and returns the running command.
func start(t *testing.T, steps string) (cmd *exec.Cmd, out, dir string) {
	t.Helper()
	dir = t.TempDir()
	out = filepath.Join(dir, "out")
	file := filepath.Join(dir, "steps")
	if err := os.WriteFile(file, []byte(steps), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd = chainCommand(t, dir, out, file, liveClaims(t, dir))
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	return cmd, out, dir
}

// chainCommand is chain.sh for host h1 and work item 77, asking claims
// for its claim, launched for the commit of a repository made in dir.
func chainCommand(t *testing.T, dir, out, steps, claims string) *exec.Cmd {
	t.Helper()
	script, err := filepath.Abs("chain.sh")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", script, "h1", "77", out, steps, initRepo(t, dir))
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "CHAIN_CLAIMS="+claims)
	return cmd
}

// gitIn runs git in dir and returns its trimmed output.
func gitIn(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir, "-c", "user.name=t", "-c", "user.email=t@t"}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

// initRepo makes dir a repository with one committed file, tracked, unless
// it is one already, and returns its HEAD.
func initRepo(t *testing.T, dir string) string {
	t.Helper()
	if _, err := os.Stat(filepath.Join(dir, ".git")); err != nil {
		gitIn(t, dir, "init", "-q")
		if err := os.WriteFile(filepath.Join(dir, "tracked"), []byte("a\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		gitIn(t, dir, "add", "tracked")
		gitIn(t, dir, "commit", "-q", "-m", "base")
	}
	return gitIn(t, dir, "rev-parse", "HEAD")
}

// fakeClaims writes a claims command that answers body and exits rc.
func fakeClaims(t *testing.T, dir, name, body string, rc int) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte("#!/bin/bash\ncat <<'EOF'\n"+body+"EOF\nexit "+strconv.Itoa(rc)+"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	return p
}

// liveClaims answers as reeve does for work item 77's live claim on h1.
func liveClaims(t *testing.T, dir string) string {
	return fakeClaims(t, dir, "claims-live", "wi-77-1 WI 77 the whole host until 2026-11-02T23:00:00-07:00\n1 live, 0 lapsed\n", 0)
}

func run(t *testing.T, steps string) (code int, out, dir string) {
	t.Helper()
	cmd, out, dir := start(t, steps)
	err := cmd.Wait()
	if ee, ok := err.(*exec.ExitError); ok {
		return ee.ExitCode(), out, dir
	} else if err != nil {
		t.Fatal(err)
	}
	return 0, out, dir
}

func read(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(b))
}

// summary is the summary file's lines with each secs= field dropped.
func summary(t *testing.T, out string) []string {
	t.Helper()
	var lines []string
	for _, l := range strings.Split(read(t, filepath.Join(out, "summary")), "\n") {
		if i := strings.Index(l, " secs="); i >= 0 {
			l = l[:i]
		}
		lines = append(lines, l)
	}
	return lines
}

func waitFor(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s did not appear", path)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestChainRunsEveryStepAndRecordsEach(t *testing.T) {
	code, out, _ := run(t, "# a comment\na true\n\nb exit 3\nc true\n")
	if code != 0 {
		t.Fatalf("exit %d", code)
	}
	got := strings.Join(summary(t, out), "|")
	if got != "a exit=0|b exit=3|c exit=0" {
		t.Fatalf("summary %q", got)
	}
	if d := read(t, filepath.Join(out, "done")); d != "finished 3 steps" {
		t.Fatalf("done %q", d)
	}
}

func TestChainStopsAtTheNextBoundary(t *testing.T) {
	cmd, out, dir := start(t, "a touch started; until test -e release; do sleep 0.01; done; echo step-a-marker >/dev/null\nb true\nc true\n")
	waitFor(t, filepath.Join(dir, "started"))

	cur := strings.Fields(read(t, filepath.Join(out, "current")))
	if len(cur) != 2 || cur[0] != "a" {
		t.Fatalf("current %q while a runs", cur)
	}
	cmdline, err := os.ReadFile("/proc/" + cur[1] + "/cmdline")
	if err != nil || !strings.Contains(string(cmdline), "step-a-marker") {
		t.Fatalf("current names PID %s, whose command line is %q (%v), not step a's", cur[1], cmdline, err)
	}

	if err := os.WriteFile(filepath.Join(out, "stop"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "release"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("a stopped chain: %v", err)
	}
	if got := strings.Join(summary(t, out), "|"); got != "a exit=0|stopped before b" {
		t.Fatalf("summary %q", got)
	}
	if d := read(t, filepath.Join(out, "done")); d != "stopped before b" {
		t.Fatalf("done %q", d)
	}
	if _, err := os.Stat(filepath.Join(out, "b.log")); err == nil {
		t.Fatal("step b ran after the stop")
	}
	if _, err := os.Stat(filepath.Join(out, "stop")); err != nil {
		t.Fatal("the runner removed the stop file")
	}
}

func TestChainStopsBeforeTheFirstStepWhenTheStopFileIsThere(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "out")
	if err := os.MkdirAll(out, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(out, "stop"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(dir, "steps")
	if err := os.WriteFile(file, []byte("a true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := chainCommand(t, dir, out, file, liveClaims(t, dir))
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, b)
	}
	if d := read(t, filepath.Join(out, "done")); d != "stopped before a" {
		t.Fatalf("done %q", d)
	}
	if _, err := os.Stat(filepath.Join(out, "a.log")); err == nil {
		t.Fatal("step a ran with a stop file present at launch")
	}
}

func TestChainRecordsItsPIDAtLaunch(t *testing.T) {
	cmd, out, dir := start(t, "a touch started; until test -e release; do sleep 0.01; done\n")
	waitFor(t, filepath.Join(dir, "started"))
	f := strings.Fields(read(t, filepath.Join(out, "pid")))
	if len(f) != 3 || f[0] != strconv.Itoa(cmd.Process.Pid) {
		t.Fatalf("pid file %q, the runner is %d", f, cmd.Process.Pid)
	}
	stat := strings.Fields(read(t, "/proc/"+f[0]+"/stat"))
	if len(stat) < 22 || stat[21] != f[1] {
		t.Fatalf("the recorded start time %s is not the runner's (%v)", f[1], stat)
	}
	if err := os.WriteFile(filepath.Join(dir, "release"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestChainEndsAtAFailedRequiredStep(t *testing.T) {
	code, out, _ := run(t, "!a exit 4\nb true\n")
	if code != 0 {
		t.Fatalf("exit %d", code)
	}
	if d := read(t, filepath.Join(out, "done")); d != "ended at a (exit 4)" {
		t.Fatalf("done %q", d)
	}
	if got := strings.Join(summary(t, out), "|"); got != "a exit=4" {
		t.Fatalf("summary %q", got)
	}
}

func TestChainRefusesABadStepsFile(t *testing.T) {
	for name, steps := range map[string]string{
		"no command":      "a true\nb\n",
		"duplicate label": "a true\na true\n",
		"slash in label":  "a true\nx/y true\n",
		"no steps":        "# only a comment\n",
	} {
		t.Run(name, func(t *testing.T) {
			code, out, _ := run(t, steps)
			if code != 2 {
				t.Fatalf("exit %d, want 2", code)
			}
			if _, err := os.Stat(filepath.Join(out, "a.log")); err == nil {
				t.Fatal("a step ran from a refused file")
			}
			if _, err := os.Stat(filepath.Join(out, "pid")); err == nil {
				t.Fatal("a refused file was launched")
			}
		})
	}
}

func TestChainTimesEachStep(t *testing.T) {
	_, out, _ := run(t, "a sleep 1.2\n")
	line := read(t, filepath.Join(out, "summary"))
	i := strings.Index(line, " secs=")
	if i < 0 {
		t.Fatalf("summary %q", line)
	}
	if n, err := strconv.Atoi(line[i+len(" secs="):]); err != nil || n < 1 {
		t.Fatalf("a 1.2 s step recorded %q", line)
	}
}

// TestClaimHeldRefusesAllButAWorkItemsLiveClaim: the decision, with the
// answers it exists to refuse.
func TestClaimHeldRefusesAllButAWorkItemsLiveClaim(t *testing.T) {
	script, err := filepath.Abs("chain.sh")
	if err != nil {
		t.Fatal(err)
	}
	live := "wi-77-1 WI 77 the whole host until 2026-11-02T23:00:00-07:00\n1 live, 0 lapsed\n"
	for _, c := range []struct {
		name, wi, rc, out string
		held              bool
	}{
		{"no claim", "77", "1", "0 live, 0 lapsed\n", false},
		{"another work item's claim", "77", "0", "wi-88-1 WI 88 the whole host until 2026-11-02T23:00:00-07:00\n1 live, 0 lapsed\n", false},
		{"a lapsed claim", "77", "0", "wi-77-1 WI 77 the whole host until 2026-11-02T20:00:00-07:00 LAPSED\n0 live, 1 lapsed\n", false},
		{"the line with a failed status", "77", "1", live, false},
		{"a prefix of the work item", "7", "0", live, false},
		{"reeve failing", "77", "2", "reeve claims: no such host\n", false},
		{"the work item's live claim", "77", "0", live, true},
	} {
		cmd := exec.Command("bash", script, "--claim-held", c.wi, c.rc)
		cmd.Stdin = strings.NewReader(c.out)
		err := cmd.Run()
		if held := err == nil; held != c.held {
			t.Errorf("%s: held %v (%v), want %v", c.name, held, err, c.held)
		}
	}
}

// TestChainWithoutAClaimStartsNoStep: no live claim at the first step, so
// nothing runs and done names the step.
func TestChainWithoutAClaimStartsNoStep(t *testing.T) {
	dir := t.TempDir()
	out, file := filepath.Join(dir, "out"), filepath.Join(dir, "steps")
	if err := os.WriteFile(file, []byte("a touch ran-a\nb true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	none := fakeClaims(t, dir, "claims-none", "0 live, 0 lapsed\n", 1)
	err := chainCommand(t, dir, out, file, none).Run()
	if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 3 {
		t.Fatalf("exit %v, want 3", err)
	}
	if d := read(t, filepath.Join(out, "done")); d != "no live claim for WI 77 on h1 before a" {
		t.Fatalf("done %q", d)
	}
	if _, err := os.Stat(filepath.Join(dir, "ran-a")); err == nil {
		t.Fatal("step a ran without a claim")
	}
}

// TestChainStopsWhenItsClaimEnds: the claim is live at the first step and
// gone at the second, so the first runs and the chain stops before the
// second.
func TestChainStopsWhenItsClaimEnds(t *testing.T) {
	dir := t.TempDir()
	out, file := filepath.Join(dir, "out"), filepath.Join(dir, "steps")
	if err := os.WriteFile(file, []byte("a true\nb touch ran-b\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	once := filepath.Join(dir, "claims-once")
	body := "#!/bin/bash\nif [ -e " + filepath.Join(dir, "asked") + " ]; then echo '0 live, 0 lapsed'; exit 1; fi\n" +
		"touch " + filepath.Join(dir, "asked") + "\necho 'wi-77-1 WI 77 the whole host until 2026-11-02T23:00:00-07:00'\necho '1 live, 0 lapsed'\n"
	if err := os.WriteFile(once, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	err := chainCommand(t, dir, out, file, once).Run()
	if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 3 {
		t.Fatalf("exit %v, want 3", err)
	}
	if d := read(t, filepath.Join(out, "done")); d != "no live claim for WI 77 on h1 before b" {
		t.Fatalf("done %q", d)
	}
	if got := strings.Join(summary(t, out), "|"); got != "a exit=0|no live claim for WI 77 on h1 before b" {
		t.Fatalf("summary %q", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "ran-b")); err == nil {
		t.Fatal("step b ran after the claim ended")
	}
}

// TestChainRefusesWithoutAHostOrWorkItem: there is no unchecked mode.
func TestChainRefusesWithoutAHostOrWorkItem(t *testing.T) {
	script, _ := filepath.Abs("chain.sh")
	dir := t.TempDir()
	out, steps := filepath.Join(dir, "out"), filepath.Join(dir, "steps")
	if err := os.WriteFile(steps, []byte("a touch ran-a\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	commit := strings.Repeat("ab", 20)
	for _, args := range [][]string{{out, steps}, {"", "77", out, steps, commit}, {"h1", "", out, steps, commit},
		{"h1", "77", out, steps}, {"h1", "77", out, steps, "abab"}, {"h1", "77", out, steps, strings.ToUpper(commit)}} {
		cmd := exec.Command("bash", append([]string{script}, args...)...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "CHAIN_CLAIMS="+liveClaims(t, dir))
		err := cmd.Run()
		if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 2 {
			t.Errorf("%q: %v, want exit 2", args, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "ran-a")); err == nil {
		t.Fatal("a step ran after a usage error")
	}
}

// TestChainRunsStepsRemotely: with CHAIN_REMOTE each step goes to the ssh
// command as a script on stdin, run from CHAIN_DIR there; its output lands in
// the local log.
func TestChainRunsStepsRemotely(t *testing.T) {
	dir := t.TempDir()
	there := filepath.Join(dir, "there")
	initRepo(t, dir)
	gitIn(t, dir, "clone", "-q", dir, there)
	out, file := filepath.Join(dir, "out"), filepath.Join(dir, "steps")
	if err := os.WriteFile(file, []byte("a echo at $(pwd) as $0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ssh := filepath.Join(dir, "fake-ssh")
	if err := os.WriteFile(ssh, []byte("#!/bin/bash\n[ \"$1\" = remote-host ] && [ \"$2\" = 'bash -s' ] || exit 9\ntouch "+filepath.Join(dir, "via-ssh")+"\nexec bash -s\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	cmd := chainCommand(t, dir, out, file, liveClaims(t, dir))
	cmd.Env = append(cmd.Env, "CHAIN_REMOTE=remote-host", "CHAIN_DIR="+there, "CHAIN_SSH="+ssh)
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, b)
	}
	if got := read(t, filepath.Join(out, "a.log")); got != "at "+there+" as bash" {
		t.Fatalf("remote step's log %q, want it run in %s", got, there)
	}
	if d := read(t, filepath.Join(out, "done")); d != "finished 1 steps" {
		t.Fatalf("done %q", d)
	}
	if _, err := os.Stat(filepath.Join(dir, "via-ssh")); err != nil {
		t.Fatal("the step did not go through the ssh command")
	}
}

// TestTreeAtRefusesAllButTheCleanCommit: the tree decision holds only for a
// read that succeeded, named the chain's commit and listed no change.
func TestTreeAtRefusesAllButTheCleanCommit(t *testing.T) {
	script, err := filepath.Abs("chain.sh")
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Repeat("0a", 20)
	other := strings.Repeat("1b", 20)
	for _, c := range []struct {
		name, commit, rc, out string
		held                  bool
	}{
		{"another commit", want, "0", other + "\n", false},
		{"a modified file", want, "0", want + "\n M tracked\n", false},
		{"a staged file", want, "0", want + "\nM  tracked\n", false},
		{"not a repository", want, "128", "fatal: not a git repository\n", false},
		{"the directory missing", want, "2", "", false},
		{"nothing read", want, "0", "", false},
		{"the commit with a failed status", want, "1", want + "\n", false},
		{"a short commit", want[:12], "0", want[:12] + "\n", false},
		{"the clean commit", want, "0", want + "\n", true},
	} {
		cmd := exec.Command("bash", script, "--tree-at", c.commit, c.rc)
		cmd.Stdin = strings.NewReader(c.out)
		err := cmd.Run()
		if held := err == nil; held != c.held {
			t.Errorf("%s: held %v (%v), want %v", c.name, held, err, c.held)
		}
	}
}

// TestChainRefusesAnotherTree: launched for its repository's commit the chain
// runs; with a tracked file changed, or launched for another commit, it exits
// 4 before the first step and says what it read.
func TestChainRefusesAnotherTree(t *testing.T) {
	for _, c := range []struct {
		name  string
		setup func(t *testing.T, dir string) []string
		want  string
	}{
		{"a modified file", func(t *testing.T, dir string) []string {
			if err := os.WriteFile(filepath.Join(dir, "tracked"), []byte("changed\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			return nil
		}, " M tracked"},
		{"another commit", func(t *testing.T, dir string) []string {
			return []string{strings.Repeat("1b", 20)}
		}, "tree is not " + strings.Repeat("1b", 20) + " clean before a"},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			out, file := filepath.Join(dir, "out"), filepath.Join(dir, "steps")
			if err := os.WriteFile(file, []byte("a touch ran-a\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			cmd := chainCommand(t, dir, out, file, liveClaims(t, dir))
			if args := c.setup(t, dir); args != nil {
				cmd.Args[len(cmd.Args)-1] = args[0]
			}
			err := cmd.Run()
			if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 4 {
				t.Fatalf("exit %v, want 4", err)
			}
			d := read(t, filepath.Join(out, "done"))
			if !strings.HasPrefix(d, "tree is not ") || !strings.Contains(d, " clean before a: ") || !strings.Contains(d, c.want) {
				t.Fatalf("done %q, want the refusal naming %q", d, c.want)
			}
			if _, err := os.Stat(filepath.Join(dir, "ran-a")); err == nil {
				t.Fatal("step a ran on the wrong tree")
			}
		})
	}
}

// TestChainStopsWhenAStepChangesTheTree: a step that changes a tracked file
// runs, and the chain refuses the step after it.
func TestChainStopsWhenAStepChangesTheTree(t *testing.T) {
	code, out, dir := run(t, "a echo more >> tracked\nb touch ran-b\n")
	if code != 4 {
		t.Fatalf("exit %d, want 4", code)
	}
	got := summary(t, out)
	if len(got) != 2 || got[0] != "a exit=0" || !strings.HasPrefix(got[1], "tree is not ") || !strings.Contains(got[1], " clean before b: ") {
		t.Fatalf("summary %q", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "ran-b")); err == nil {
		t.Fatal("step b ran on a changed tree")
	}
}
