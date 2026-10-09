package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

// git runs one git command in repo and returns its standard output.
func git(repo string, env []string, stdin []byte, args ...string) ([]byte, error) {
	cmd := exec.Command("git", append([]string{"-C", repo}, args...)...)
	if env != nil {
		cmd.Env = append(os.Environ(), env...)
	}
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return out, nil
}

func gitLine(repo string, args ...string) (string, error) {
	out, err := git(repo, nil, nil, args...)
	return strings.TrimSpace(string(out)), err
}

// resolve returns the commit a revision names, or "" when it names nothing.
func resolve(repo, rev string) (string, error) {
	out, err := git(repo, nil, nil, "rev-parse", "--verify", "--quiet", "--end-of-options", rev+"^{commit}")
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) && exit.ExitCode() == 1 {
			return "", nil
		}
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

// treeEntry is one line of git ls-tree output.
type treeEntry struct {
	mode, kind, oid, path string
}

func lsTree(repo string, recursive bool, tree string) ([]treeEntry, error) {
	args := []string{"ls-tree", "-z", "--full-tree"}
	if recursive {
		args = append(args, "-r")
	}
	out, err := git(repo, nil, nil, append(args, tree)...)
	if err != nil {
		return nil, err
	}
	var entries []treeEntry
	for _, rec := range strings.Split(string(out), "\x00") {
		if rec == "" {
			continue
		}
		meta, path, ok := strings.Cut(rec, "\t")
		f := strings.Fields(meta)
		if !ok || len(f) != 3 {
			return nil, fmt.Errorf("ls-tree: unexpected record %q", rec)
		}
		entries = append(entries, treeEntry{mode: f[0], kind: f[1], oid: f[2], path: path})
	}
	return entries, nil
}

// excluded reports whether path is the omitted directory or inside it.
func excluded(path, omit string) bool {
	return path == omit || strings.HasPrefix(path, omit+"/")
}

// published lists the blobs of commit's tree outside omit, with their bytes,
// and counts the blobs omitted.
func published(repo, commit, omit string) ([]entry, int, error) {
	all, err := lsTree(repo, true, commit)
	if err != nil {
		return nil, 0, err
	}
	var entries []entry
	omitted := 0
	for _, t := range all {
		if excluded(t.path, omit) {
			omitted++
			continue
		}
		if t.kind != "blob" {
			return nil, 0, fmt.Errorf("%s is a %s; only blobs can be published", t.path, t.kind)
		}
		entries = append(entries, entry{path: t.path, mode: t.mode, blob: t.oid})
	}
	if err := readBlobs(repo, entries); err != nil {
		return nil, 0, err
	}
	return entries, omitted, nil
}

// readBlobs fills each entry's data through one git cat-file --batch.
func readBlobs(repo string, entries []entry) error {
	if len(entries) == 0 {
		return nil
	}
	var in bytes.Buffer
	for _, e := range entries {
		in.WriteString(e.blob + "\n")
	}
	out, err := git(repo, nil, in.Bytes(), "cat-file", "--batch")
	if err != nil {
		return err
	}
	r := bufio.NewReader(bytes.NewReader(out))
	for i := range entries {
		header, err := r.ReadString('\n')
		if err != nil {
			return fmt.Errorf("cat-file: %w", err)
		}
		f := strings.Fields(header)
		if len(f) != 3 || f[0] != entries[i].blob || f[1] != "blob" {
			return fmt.Errorf("cat-file: unexpected header %q for %s", strings.TrimSpace(header), entries[i].path)
		}
		size, err := strconv.Atoi(f[2])
		if err != nil {
			return fmt.Errorf("cat-file: size in %q: %w", header, err)
		}
		data := make([]byte, size+1)
		if _, err := io.ReadFull(r, data); err != nil {
			return fmt.Errorf("cat-file: %s: %w", entries[i].path, err)
		}
		entries[i].data = data[:size]
	}
	return nil
}

// treeWithout writes tree with the entry at path removed and returns the new
// tree's id. A directory left empty is removed with it.
func treeWithout(repo, tree, path string) (string, error) {
	first, rest, nested := strings.Cut(path, "/")
	entries, err := lsTree(repo, false, tree)
	if err != nil {
		return "", err
	}
	var kept []treeEntry
	changed := false
	for _, e := range entries {
		if e.path != first || nested && e.kind != "tree" {
			kept = append(kept, e)
			continue
		}
		changed = true
		if !nested {
			continue
		}
		sub, err := treeWithout(repo, e.oid, rest)
		if err != nil {
			return "", err
		}
		if sub == "" {
			continue
		}
		e.oid = sub
		kept = append(kept, e)
	}
	if !changed {
		return tree, nil
	}
	if len(kept) == 0 {
		return "", nil
	}
	var in bytes.Buffer
	for _, e := range kept {
		in.WriteString(e.mode + " " + e.kind + " " + e.oid + "\t" + e.path + "\x00")
	}
	return gitLineIn(repo, in.Bytes(), "mktree", "-z")
}

func gitLineIn(repo string, stdin []byte, args ...string) (string, error) {
	out, err := git(repo, nil, stdin, args...)
	return strings.TrimSpace(string(out)), err
}

// sameBlobs reports whether tree holds exactly the checked entries.
func sameBlobs(repo, tree string, checked []entry) error {
	got, err := lsTree(repo, true, tree)
	if err != nil {
		return err
	}
	if len(got) != len(checked) {
		return fmt.Errorf("the built tree holds %d entries, %d were checked", len(got), len(checked))
	}
	want := map[string]entry{}
	for _, e := range checked {
		want[e.path] = e
	}
	for _, g := range got {
		w, ok := want[g.path]
		if !ok || g.kind != "blob" || g.mode != w.mode || g.oid != w.blob {
			return fmt.Errorf("the built tree's %s differs from what was checked", g.path)
		}
	}
	return nil
}

// commitObject is what a written commit records.
type commitObject struct {
	tree      string
	parents   []string
	author    identity
	committer identity
	message   string
}

func readCommit(repo, id string) (commitObject, error) {
	out, err := git(repo, nil, nil, "cat-file", "commit", id)
	if err != nil {
		return commitObject{}, err
	}
	head, msg, _ := strings.Cut(string(out), "\n\n")
	c := commitObject{message: msg}
	for _, line := range strings.Split(head, "\n") {
		key, val, _ := strings.Cut(line, " ")
		switch key {
		case "tree":
			c.tree = val
		case "parent":
			c.parents = append(c.parents, val)
		case "author":
			c.author = parseIdent(val)
		case "committer":
			c.committer = parseIdent(val)
		}
	}
	return c, nil
}

// parseIdent reads "Name <email> time zone".
func parseIdent(s string) identity {
	open := strings.Index(s, " <")
	closing := strings.LastIndex(s, ">")
	if open < 0 || closing < open {
		return identity{name: s}
	}
	return identity{name: s[:open], email: s[open+2 : closing]}
}
