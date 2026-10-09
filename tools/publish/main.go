// Command publish builds or advances the local branch that is pushed to the
// public repository: the source branch's tree without sim/results, as one new
// commit on that branch, after a term check, an identity check and a licence
// check. It never pushes; its last line is the push command to run.
//
//	go run ./tools/publish -terms <term list> -identity "<name> <noreply address>"
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

const (
	// omit is left out of every public commit: the measurement results
	// stay in the private repository.
	omit = "sim/results"
	// allowedPath is the exceptions file, read from the source commit.
	allowedPath = "tools/publish/allowed-terms.txt"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func fprintf(w io.Writer, format string, a ...any) { _, _ = fmt.Fprintf(w, format, a...) }

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("publish", flag.ContinueOnError)
	fs.SetOutput(stderr)
	terms := fs.String("terms", "", "the site term list (required)")
	source := fs.String("source", "main", "the branch whose tree is published")
	branch := fs.String("branch", "public", "the local branch that receives the commit")
	remote := fs.String("remote", "github", "the remote named in the printed push command")
	repo := fs.String("repo", ".", "the repository")
	as := fs.String("identity", "", `the author and committer, "Name <id+login@users.noreply.github.com>" (required)`)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *terms == "" || *as == "" || fs.NArg() != 0 {
		fprintf(stderr, "usage: publish -terms <term list> -identity \"Name <noreply address>\" [-source main] [-branch public] [-remote github] [-repo .]\n")
		return 2
	}
	if err := publish(*repo, *terms, *source, *branch, *remote, parseIdent(*as), stdout); err != nil {
		fprintf(stderr, "publish: %v\n", err)
		fprintf(stderr, "publish: %s not moved\n", *branch)
		return 1
	}
	return 0
}

// refused is a no-go from the checks; its reasons are already printed.
type refused struct{ stage string }

func (r refused) Error() string { return "refused at " + r.stage }

// publish takes the identity as an input so that the address is not in the
// published tree, where the term list can match it.
func publish(repo, termsPath, source, branch, remote string, publisher identity, out io.Writer) error {
	top, err := gitLine(repo, "rev-parse", "--show-toplevel")
	if err != nil {
		return err
	}
	src, err := resolve(repo, "refs/heads/"+source)
	if err != nil {
		return err
	}
	if src == "" {
		return fmt.Errorf("no branch %s", source)
	}
	ref := "refs/heads/" + branch
	tip, err := resolve(repo, ref)
	if err != nil {
		return err
	}
	fprintf(out, "publish: %s at %s\n", source, src)

	raw, err := os.ReadFile(termsPath) //nolint:gosec // the operator names the term list
	if err != nil {
		return err
	}
	pats, err := parseTerms(raw)
	if err != nil {
		return err
	}
	entries, omitted, err := published(repo, src, omit)
	if err != nil {
		return err
	}
	allowed, err := allowedFrom(entries)
	if err != nil {
		return err
	}
	fprintf(out, "publish: term list %s: %d patterns; allowed terms (%s): %s\n", termsPath, len(pats), allowedPath, strings.Join(sortedKeys(allowed), ", "))
	fprintf(out, "publish: %d files examined; %d under %s/ omitted\n", len(entries), omitted, omit)

	msg := fmt.Sprintf("%s %s, without %s/\n\nBuilt by tools/publish from %s %s.\n", source, src[:7], omit, source, src)
	terms := termCheck(pats, allowed, entries, msg)
	ids := append(identityProblems("author", publisher), identityProblems("committer", publisher)...)
	ids = append(ids, emailProblems(entries, msg)...)
	r := results{patterns: terms.patterns, files: terms.files, hits: terms.hits, identity: ids, missing: missingLicences(entries)}
	fprintf(out, "publish: term check: %d hits; allowed matches: %s\n", len(terms.hits), allowedSummary(terms.allowed))
	fprintf(out, "publish: identity check: %s, %d problems\n", publisher, len(ids))
	fprintf(out, "publish: licence check: %d of %d missing (%s)\n", len(r.missing), len(licences), strings.Join(licences, ", "))
	if err := verdict(out, r, "the checks"); err != nil {
		return err
	}

	root, err := gitLine(repo, "rev-parse", src+"^{tree}")
	if err != nil {
		return err
	}
	tree, err := treeWithout(repo, root, omit)
	if err != nil {
		return err
	}
	if err := sameBlobs(repo, tree, entries); err != nil {
		return err
	}
	if tip != "" {
		cur, err := gitLine(repo, "rev-parse", tip+"^{tree}")
		if err != nil {
			return err
		}
		if cur == tree {
			fprintf(out, "publish: %s at %s already carries this tree; nothing written\n", branch, tip)
			fprintf(out, "git -C %s push %s %s:refs/heads/main\n", top, remote, ref)
			return nil
		}
	}

	commit, err := commitTree(repo, tree, tip, msg, publisher)
	if err != nil {
		return err
	}
	written, err := readCommit(repo, commit)
	if err != nil {
		return err
	}
	r, err = judgeWritten(written, tree, tip, pats, allowed, r)
	if err != nil {
		return err
	}
	if err := verdict(out, r, "the written commit"); err != nil {
		return err
	}

	old := tip
	if old == "" {
		old = strings.Repeat("0", len(commit))
	}
	if _, err := git(repo, nil, nil, "update-ref", "-m", "publish: from "+source+" "+src, ref, commit, old); err != nil {
		return err
	}
	parent := "a root commit"
	if tip != "" {
		parent = "parent " + tip
	}
	fprintf(out, "publish: wrote %s on %s, %s, tree %s\n", commit, branch, parent, tree)
	fprintf(out, "git -C %s push %s %s:refs/heads/main\n", top, remote, ref)
	return nil
}

// verdict prints the decision and returns refused on a no-go.
func verdict(out io.Writer, r results, stage string) error {
	ok, why := decide(r)
	if ok {
		fprintf(out, "publish: go (%s)\n", stage)
		return nil
	}
	fprintf(out, "publish: no-go (%s)\n", stage)
	for _, w := range why {
		fprintf(out, "publish:   %s\n", w)
	}
	return refused{stage: stage}
}

func allowedFrom(entries []entry) (map[string]bool, error) {
	for _, e := range entries {
		if e.path == allowedPath {
			return parseAllowed(e.data)
		}
	}
	return nil, fmt.Errorf("%s is not in the source tree", allowedPath)
}

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func commitTree(repo, tree, parent, msg string, publisher identity) (string, error) {
	env := []string{
		"GIT_AUTHOR_NAME=" + publisher.name, "GIT_AUTHOR_EMAIL=" + publisher.email,
		"GIT_COMMITTER_NAME=" + publisher.name, "GIT_COMMITTER_EMAIL=" + publisher.email,
	}
	args := []string{"commit-tree", "--no-gpg-sign", "-F", "-"}
	if parent != "" {
		args = append(args, "-p", parent)
	}
	out, err := git(repo, env, []byte(msg), append(args, tree)...)
	return strings.TrimSpace(string(out)), err
}
