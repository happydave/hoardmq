package gatechain

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// recorder writes a command that appends its arguments to log and exits 0.
func recorder(t *testing.T, dir, name, log string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte("#!/bin/bash\necho \"$@\" >> "+log+"\ncat > /dev/null\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	return p
}

// syncRepo makes a repository holding wi-77 one commit past its base, and
// returns the repository and the base commit.
func syncRepo(t *testing.T) (repo, base string) {
	t.Helper()
	repo = t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", repo, "-c", "user.name=t", "-c", "user.email=t@t"}, args...)...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	git("init", "-q")
	if err := os.WriteFile(filepath.Join(repo, "a"), []byte("a\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	git("add", "a")
	git("commit", "-q", "-m", "base")
	base = git("rev-parse", "HEAD")
	git("checkout", "-q", "-b", "wi-77")
	if err := os.WriteFile(filepath.Join(repo, "a"), []byte("b\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	git("commit", "-q", "-am", "change")
	return repo, base
}

func syncCommand(t *testing.T, repo, base, claims, ssh, scp string) *exec.Cmd {
	t.Helper()
	script, err := filepath.Abs("sync.sh")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", script, "h1", "77", base)
	cmd.Dir = repo
	cmd.Env = append(os.Environ(), "CHAIN_CLAIMS="+claims, "CHAIN_SSH="+ssh, "CHAIN_SCP="+scp)
	return cmd
}

// TestSyncMovesNothingWithoutAClaim: with another work item's claim, or a
// claims command that fails, the sync exits 3 and neither copies nor reaches
// the host; with the work item's live claim it does both.
func TestSyncMovesNothingWithoutAClaim(t *testing.T) {
	repo, base := syncRepo(t)
	dir := t.TempDir()
	for _, c := range []struct {
		name, claims string
	}{
		{"another work item's claim", fakeClaims(t, dir, "claims-other", "wi-78-1 WI 78 the whole host until 2026-11-03T10:00:00-07:00\n1 live, 0 lapsed\n", 0)},
		{"a lapsed claim", fakeClaims(t, dir, "claims-lapsed", "wi-77-1 WI 77 the whole host until 2026-11-03T08:00:00-07:00 LAPSED\n0 live, 1 lapsed\n", 0)},
		{"reeve failing", fakeClaims(t, dir, "claims-fail", "refused\n", 1)},
	} {
		log := filepath.Join(dir, strings.ReplaceAll(c.name, " ", "-")+".calls")
		ssh, scp := recorder(t, dir, "ssh", log), recorder(t, dir, "scp", log)
		out, err := syncCommand(t, repo, base, c.claims, ssh, scp).CombinedOutput()
		ee, ok := err.(*exec.ExitError)
		if !ok || ee.ExitCode() != 3 || !strings.Contains(string(out), "no live claim for WI 77 on h1: nothing synced") {
			t.Errorf("%s: %v\n%s", c.name, err, out)
		}
		if _, err := os.Stat(log); !os.IsNotExist(err) {
			t.Errorf("%s: the host was reached: %s", c.name, read(t, log))
		}
		if _, err := os.Stat(filepath.Join(repo, "tmp", "77")); !os.IsNotExist(err) {
			t.Errorf("%s: a bundle was written", c.name)
		}
	}
	log := filepath.Join(dir, "live.calls")
	ssh, scp := recorder(t, dir, "ssh-live", log), recorder(t, dir, "scp-live", log)
	if out, err := syncCommand(t, repo, base, liveClaims(t, dir), ssh, scp).CombinedOutput(); err != nil {
		t.Fatalf("with a live claim: %v\n%s", err, out)
	}
	calls := read(t, log)
	if !strings.Contains(calls, "wi77.bundle h1:hoardmq-wi77.bundle") || !strings.Contains(calls, "h1 bash -s") {
		t.Fatalf("with a live claim the calls were:\n%s", calls)
	}
}

// TestSyncFailsWhenTheHostRefuses: a remote step that fails (a dirty clone
// refusing the checkout) makes the sync exit non-zero.
func TestSyncFailsWhenTheHostRefuses(t *testing.T) {
	repo, base := syncRepo(t)
	dir := t.TempDir()
	claims := fakeClaims(t, dir, "claims-live", "wi-77-1 WI 77 the whole host until 2026-11-02T23:00:00-07:00\n1 live, 0 lapsed\n", 0)
	scp := recorder(t, dir, "scp", filepath.Join(dir, "scp.log"))
	ssh := filepath.Join(dir, "ssh-refuses")
	if err := os.WriteFile(ssh, []byte("#!/bin/bash\ncat > /dev/null\necho 'error: Your local changes would be overwritten by checkout' >&2\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	out, err := syncCommand(t, repo, base, claims, ssh, scp).CombinedOutput()
	if err == nil {
		t.Fatalf("sync exited 0 when the host refused:\n%s", out)
	}
}
