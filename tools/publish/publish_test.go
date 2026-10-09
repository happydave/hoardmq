package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Every test here works in a repository it creates, insulated from the
// host's git configuration, and nothing in it has a remote.

func gitT(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-c", "user.name=test", "-c", "user.email=test@example.invalid", "-c", "commit.gpgsign=false"}, args...)...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

// commitFiles writes files (a nil body removes the file) and commits them on
// the current branch.
func commitFiles(t *testing.T, dir, msg string, files map[string]*string) {
	t.Helper()
	var paths []string
	for p, body := range files {
		full := filepath.Join(dir, p)
		if body == nil {
			if err := os.Remove(full); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := os.MkdirAll(filepath.Dir(full), 0o750); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(full, []byte(*body), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		paths = append(paths, p)
	}
	gitT(t, dir, append([]string{"add", "--"}, paths...)...)
	gitT(t, dir, "commit", "-q", "-m", msg)
}

func s(v string) *string { return &v }

func testRepo(t *testing.T) (dir, terms string) {
	t.Helper()
	t.Setenv("GIT_CONFIG_GLOBAL", os.DevNull)
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	dir = t.TempDir()
	gitT(t, dir, "init", "-q", "-b", "main")
	commitFiles(t, dir, "one", map[string]*string{
		"LICENSE-MIT":       s("mit\n"),
		"LICENSE-APACHE":    s("apache\n"),
		"NOTICE":            s("notice\n"),
		allowedPath:         s("# test\nalpha\nbeta\nnotbeta\n"),
		"README.md":         s("Measured on alpha, by notbeta@192.0.2.1.\n"),
		"sim/keep.go":       s("package sim\n"),
		"sim/results/r1.md": s("host-2025-alpha secret-word\n"),
	})
	terms = filepath.Join(t.TempDir(), "terms.txt")
	if err := os.WriteFile(terms, []byte(testTerms), 0o600); err != nil {
		t.Fatal(err)
	}
	return dir, terms
}

func publishT(t *testing.T, dir, terms string) (int, string) {
	t.Helper()
	var out, errs bytes.Buffer
	code := run([]string{"-terms", terms, "-repo", dir, "-identity", testPublisher.String()}, &out, &errs)
	return code, out.String() + errs.String()
}

func tipOf(t *testing.T, dir string) string {
	t.Helper()
	cmd := exec.Command("git", "rev-parse", "--verify", "--quiet", "refs/heads/public")
	cmd.Dir = dir
	out, _ := cmd.Output()
	return strings.TrimSpace(string(out))
}

func lastLine(out string) string {
	lines := strings.Split(strings.TrimSpace(out), "\n")
	return lines[len(lines)-1]
}

func TestPublishBuildsARootThenOneCommitPerChange(t *testing.T) {
	dir, terms := testRepo(t)
	top := gitT(t, dir, "rev-parse", "--show-toplevel")
	push := "git -C " + top + " push github refs/heads/public:refs/heads/main"

	code, out := publishT(t, dir, terms)
	if code != 0 {
		t.Fatalf("first run: exit %d\n%s", code, out)
	}
	first := tipOf(t, dir)
	main1 := gitT(t, dir, "rev-parse", "main")
	if parents := gitT(t, dir, "rev-list", "--parents", "-n", "1", first); parents != first {
		t.Fatalf("the first public commit has parents: %s", parents)
	}
	files := gitT(t, dir, "ls-tree", "-r", "--name-only", first)
	if strings.Contains(files, "sim/results") || !strings.Contains(files, "sim/keep.go") {
		t.Fatalf("public tree:\n%s", files)
	}
	if got := gitT(t, dir, "log", "-1", "--format=%an <%ae>|%cn <%ce>", first); got != testPublisher.String()+"|"+testPublisher.String() {
		t.Fatalf("identity %q", got)
	}
	if msg := gitT(t, dir, "log", "-1", "--format=%B", first); !strings.Contains(msg, main1) {
		t.Fatalf("message %q does not name main %s", msg, main1)
	}
	if lastLine(out) != push {
		t.Fatalf("last line %q, want %q", lastLine(out), push)
	}

	commitFiles(t, dir, "two", map[string]*string{"README.md": s("Measured on alpha.\n")})
	if code, out := publishT(t, dir, terms); code != 0 || lastLine(out) != push {
		t.Fatalf("second run: exit %d\n%s", code, out)
	}
	second := tipOf(t, dir)
	if parents := gitT(t, dir, "rev-list", "--parents", "-n", "1", second); parents != second+" "+first {
		t.Fatalf("second public commit's parents: %s, want %s", parents, first)
	}
	if got, want := gitT(t, dir, "ls-tree", "-r", second), gitT(t, dir, "ls-tree", "-r", "main", "--", "LICENSE-MIT", "LICENSE-APACHE", "NOTICE", "README.md", "sim/keep.go", allowedPath); got != want {
		t.Fatalf("second public tree:\n%s\nwant:\n%s", got, want)
	}

	if code, out := publishT(t, dir, terms); code != 0 || !strings.Contains(out, "already carries") || tipOf(t, dir) != second {
		t.Fatalf("unchanged main: exit %d, tip %s\n%s", code, tipOf(t, dir), out)
	}
	commitFiles(t, dir, "results only", map[string]*string{"sim/results/r2.md": s("more\n")})
	if code, out := publishT(t, dir, terms); code != 0 || !strings.Contains(out, "already carries") || tipOf(t, dir) != second {
		t.Fatalf("a change under sim/results only: exit %d, tip %s\n%s", code, tipOf(t, dir), out)
	}
	if n := gitT(t, dir, "rev-list", "--count", "public"); n != "2" {
		t.Fatalf("public holds %s commits, want 2", n)
	}
}

func TestPublishRefusesAndLeavesTheBranch(t *testing.T) {
	dir, terms := testRepo(t)
	commitFiles(t, dir, "a term", map[string]*string{"sim/bad.go": s("package sim\n\n// secret-word\n")})
	code, out := publishT(t, dir, terms)
	if code != 1 || tipOf(t, dir) != "" || !strings.Contains(out, "term hit: sim/bad.go:3: term-list line 6") || strings.Contains(out, "secret-word") {
		t.Fatalf("a term on the first run: exit %d, tip %q\n%s", code, tipOf(t, dir), out)
	}
	gitT(t, dir, "reset", "-q", "--hard", "HEAD~1")
	if code, out := publishT(t, dir, terms); code != 0 {
		t.Fatalf("clean run: exit %d\n%s", code, out)
	}
	tip := tipOf(t, dir)

	for _, c := range []struct {
		name   string
		change map[string]*string
		reason string
	}{
		{"a term outside sim/results", map[string]*string{"docs/x.md": s("one\nhost-2025-alpha\n")}, "term hit: docs/x.md:2: term-list line 2"},
		{"a term in a path", map[string]*string{"docs/secret-word.md": s("clean\n")}, "term hit: docs/secret-word.md (path): term-list line 6"},
		{"a personal email", map[string]*string{"docs/y.md": s("mail " + "someone" + "@" + "mail.isp-domain.net\n")}, "identity: docs/y.md:1: email at mail.isp-domain.net"},
		{"a missing licence", map[string]*string{"LICENSE-APACHE": nil}, "licence check: LICENSE-APACHE missing"},
		{"no exceptions file", map[string]*string{allowedPath: nil}, allowedPath + " is not in the source tree"},
		{"an exceptions file widened past a word", map[string]*string{allowedPath: s("alpha\n/home/beta\n")}, "allowed terms line 2 is not a single word"},
	} {
		t.Run(c.name, func(t *testing.T) {
			commitFiles(t, dir, c.name, c.change)
			t.Cleanup(func() { gitT(t, dir, "reset", "-q", "--hard", "HEAD~1") })
			code, out := publishT(t, dir, terms)
			if code != 1 || !strings.Contains(out, c.reason) || !strings.Contains(out, "public not moved") {
				t.Fatalf("exit %d, want 1 with %q\n%s", code, c.reason, out)
			}
			if got := tipOf(t, dir); got != tip {
				t.Fatalf("public moved from %s to %s", tip, got)
			}
			if strings.Contains(out, "push github") {
				t.Fatalf("a refusal printed the push command\n%s", out)
			}
		})
	}

	commitFiles(t, dir, "two", map[string]*string{"README.md": s("Measured on alpha, twice.\n")})
	var stdout, stderr bytes.Buffer
	personal := "someone <" + "someone" + "@" + "mail.isp-domain.net>"
	code = run([]string{"-terms", terms, "-repo", dir, "-identity", personal}, &stdout, &stderr)
	if both := stdout.String() + stderr.String(); code != 1 || !strings.Contains(both, "identity: author: email is not a GitHub noreply address") || tipOf(t, dir) != tip {
		t.Fatalf("a personal identity: exit %d, tip %s\n%s", code, tipOf(t, dir), both)
	}

	commitFiles(t, dir, "a term under sim/results", map[string]*string{"sim/results/r3.md": s("secret-word\n"), "README.md": s("Measured again on alpha.\n")})
	if code, out := publishT(t, dir, terms); code != 0 || tipOf(t, dir) == tip {
		t.Fatalf("a term only under sim/results: exit %d\n%s", code, out)
	}
}

func TestPublishNeedsATermList(t *testing.T) {
	dir, _ := testRepo(t)
	var out, errs bytes.Buffer
	if code := run([]string{"-repo", dir, "-identity", testPublisher.String()}, &out, &errs); code != 2 {
		t.Fatalf("no -terms: exit %d", code)
	}
	if code := run([]string{"-repo", dir, "-terms", "x"}, &out, &errs); code != 2 {
		t.Fatalf("no -identity: exit %d", code)
	}
	empty := filepath.Join(t.TempDir(), "empty.txt")
	if err := os.WriteFile(empty, []byte("# nothing\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if code, out := publishT(t, dir, empty); code != 1 || tipOf(t, dir) != "" {
		t.Fatalf("an empty term list: exit %d\n%s", code, out)
	}
}

func TestSameBlobsRefusesATreeOtherThanTheCheckedList(t *testing.T) {
	dir, _ := testRepo(t)
	entries, omitted, err := published(dir, gitT(t, dir, "rev-parse", "main"), omit)
	if err != nil || omitted != 1 {
		t.Fatalf("published: %v, %d omitted", err, omitted)
	}
	tree, err := treeWithout(dir, gitT(t, dir, "rev-parse", "main^{tree}"), omit)
	if err != nil {
		t.Fatal(err)
	}
	if err := sameBlobs(dir, tree, entries); err != nil {
		t.Fatalf("the tree built from the checked list: %v", err)
	}
	if err := sameBlobs(dir, tree, entries[1:]); err == nil {
		t.Error("a tree with one file more than was checked accepted")
	}
	extra := append(append([]entry{}, entries...), entry{path: "zz.md", mode: "100644", blob: entries[0].blob})
	if err := sameBlobs(dir, tree, extra); err == nil {
		t.Error("a tree missing a file that was checked accepted")
	}
	changed := append([]entry{}, entries...)
	changed[0].blob = changed[1].blob
	if err := sameBlobs(dir, tree, changed); err == nil {
		t.Error("a tree whose file differs from what was checked accepted")
	}
	mode := append([]entry{}, entries...)
	mode[0].mode = "100755"
	if err := sameBlobs(dir, tree, mode); err == nil {
		t.Error("a tree whose file mode differs from what was checked accepted")
	}
	if err := sameBlobs(dir, gitT(t, dir, "rev-parse", "main^{tree}"), entries); err == nil {
		t.Error("the source tree, sim/results and all, accepted")
	}
}
