package main

import (
	"bytes"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// entry is one blob of the tree being published.
type entry struct {
	path string
	mode string
	blob string
	data []byte
}

// pattern is one line of the term list, with its line number in that file.
type pattern struct {
	line int
	re   *regexp.Regexp
}

// parseTerms reads a term list: one extended regular expression per line,
// case-sensitive; blank lines and lines starting with # are skipped. Unlike
// grep -f, a blank line is no pattern: it would match everything.
func parseTerms(data []byte) ([]pattern, error) {
	var out []pattern
	for i, line := range strings.Split(string(data), "\n") {
		line = strings.TrimRight(line, "\r")
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		re, err := regexp.Compile(line)
		if err != nil {
			return nil, fmt.Errorf("term list line %d does not compile: %w", i+1, err)
		}
		out = append(out, pattern{line: i + 1, re: re})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("term list holds no pattern")
	}
	return out, nil
}

// parseAllowed reads the exceptions file: one word per line, # comments.
func parseAllowed(data []byte) (map[string]bool, error) {
	out := map[string]bool{}
	for i, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		for j := 0; j < len(line); j++ {
			if !isWord(line[j]) {
				return nil, fmt.Errorf("allowed terms line %d is not a single word", i+1)
			}
		}
		out[line] = true
	}
	return out, nil
}

func isWord(c byte) bool {
	return c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

// allowedWord reports the word a match stands for and whether it is allowed:
// the match's text trimmed of non-word characters at both ends, then widened
// over the word characters touching it. A match "b@" inside "ab@" stands for
// ab.
func allowedWord(text []byte, start, end int, allowed map[string]bool) (string, bool) {
	for start < end && !isWord(text[start]) {
		start++
	}
	for end > start && !isWord(text[end-1]) {
		end--
	}
	if start == end {
		return "", false
	}
	for start > 0 && isWord(text[start-1]) {
		start--
	}
	for end < len(text) && isWord(text[end]) {
		end++
	}
	w := string(text[start:end])
	return w, allowed[w]
}

// hit is a match the exceptions do not allow. It never carries the matched
// text, so a report of it does not repeat the term.
type hit struct {
	where string
	line  int
	term  int
}

func (h hit) String() string {
	if h.line == 0 {
		return fmt.Sprintf("%s: term-list line %d", h.where, h.term)
	}
	return fmt.Sprintf("%s:%d: term-list line %d", h.where, h.line, h.term)
}

type termResult struct {
	patterns int
	files    int
	hits     []hit
	allowed  map[string]int
}

const messageWhere = "(commit message)"

// termCheck runs every pattern over every entry's bytes and path and over the
// commit message.
func termCheck(pats []pattern, allowed map[string]bool, entries []entry, message string) termResult {
	r := termResult{patterns: len(pats), files: len(entries), allowed: map[string]int{}}
	seen := map[hit]bool{}
	scan := func(where string, text []byte, lines bool) {
		for _, p := range pats {
			for _, m := range p.re.FindAllIndex(text, -1) {
				if w, ok := allowedWord(text, m[0], m[1], allowed); ok {
					r.allowed[w]++
					continue
				}
				h := hit{where: where, term: p.line}
				if lines {
					h.line = bytes.Count(text[:m[0]], []byte{'\n'}) + 1
				}
				if !seen[h] {
					seen[h] = true
					r.hits = append(r.hits, h)
				}
			}
		}
	}
	for _, e := range entries {
		scan(e.path+" (path)", []byte(e.path), false)
		scan(e.path, e.data, true)
	}
	scan(messageWhere, []byte(message), true)
	return r
}

// identity is a commit's author or committer.
type identity struct {
	name  string
	email string
}

func (id identity) String() string { return id.name + " <" + id.email + ">" }

var noreply = regexp.MustCompile(`^([0-9]+\+)?[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?@users\.noreply\.github\.com$`)

// identityProblems refuses any address but a GitHub noreply one, which
// GitHub links to the account without publishing a mailbox.
func identityProblems(role string, id identity) []string {
	var out []string
	if strings.TrimSpace(id.name) == "" {
		out = append(out, role+": empty name")
	}
	if !noreply.MatchString(id.email) {
		out = append(out, role+": email is not a GitHub noreply address")
	}
	return out
}

var emailShape = regexp.MustCompile(`[A-Za-z0-9._%+-]+@([A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,})`)

// reservedDomain is true for the domains reserved for documentation and
// testing (RFC 2606, RFC 6761) and for GitHub's noreply domain.
func reservedDomain(d string) bool {
	d = strings.ToLower(d)
	for _, base := range []string{"example.com", "example.net", "example.org", "users.noreply.github.com"} {
		if d == base || strings.HasSuffix(d, "."+base) {
			return true
		}
	}
	for _, tld := range []string{".example", ".invalid", ".test", ".localhost"} {
		if strings.HasSuffix(d, tld) {
			return true
		}
	}
	return false
}

// emailProblems names every address in the entries and the message whose
// domain is not reserved, by place and domain only.
func emailProblems(entries []entry, message string) []string {
	var out []string
	seen := map[string]bool{}
	scan := func(where string, text []byte) {
		for _, m := range emailShape.FindAllSubmatchIndex(text, -1) {
			domain := string(text[m[2]:m[3]])
			if reservedDomain(domain) {
				continue
			}
			p := fmt.Sprintf("%s:%d: email at %s", where, bytes.Count(text[:m[0]], []byte{'\n'})+1, domain)
			if !seen[p] {
				seen[p] = true
				out = append(out, p)
			}
		}
	}
	for _, e := range entries {
		scan(e.path, e.data)
	}
	scan(messageWhere, []byte(message))
	return out
}

// licences are the files the published tree must carry at its root.
var licences = []string{"LICENSE-MIT", "LICENSE-APACHE", "NOTICE"}

func missingLicences(entries []entry) []string {
	have := map[string]bool{}
	for _, e := range entries {
		if (e.mode == "100644" || e.mode == "100755") && len(e.data) > 0 {
			have[e.path] = true
		}
	}
	var out []string
	for _, l := range licences {
		if !have[l] {
			out = append(out, l)
		}
	}
	return out
}

// results are the three checks' outcomes.
type results struct {
	patterns int
	files    int
	hits     []hit
	identity []string
	missing  []string
}

// decide is the go/no-go for writing the public commit and moving the
// branch: go only when every check examined something and found nothing.
func decide(r results) (bool, []string) {
	var why []string
	if r.patterns == 0 {
		why = append(why, "term check: no pattern read")
	}
	if r.files == 0 {
		why = append(why, "term check: no file examined")
	}
	for _, h := range r.hits {
		why = append(why, "term hit: "+h.String())
	}
	for _, p := range r.identity {
		why = append(why, "identity: "+p)
	}
	for _, m := range r.missing {
		why = append(why, "licence check: "+m+" missing")
	}
	return len(why) == 0, why
}

// judgeWritten checks the commit git recorded: the built tree, the one
// parent expected (none for the first public commit, the previous tip after
// it), and the identity and term checks over what it records. The tree's
// files were checked before it was written, so their results carry over.
func judgeWritten(c commitObject, tree, tip string, pats []pattern, allowed map[string]bool, before results) (results, error) {
	if c.tree != tree {
		return results{}, fmt.Errorf("the written commit's tree %s is not the built tree %s", c.tree, tree)
	}
	want := 0
	if tip != "" {
		want = 1
	}
	if len(c.parents) != want || want == 1 && c.parents[0] != tip {
		return results{}, fmt.Errorf("the written commit's parents %v, want %q", c.parents, tip)
	}
	terms := termCheck(pats, allowed, nil, c.message)
	ids := append(identityProblems("author", c.author), identityProblems("committer", c.committer)...)
	ids = append(ids, emailProblems(nil, c.message)...)
	return results{patterns: terms.patterns, files: before.files, hits: terms.hits, identity: ids, missing: before.missing}, nil
}

// allowedSummary lists the allowed matches per term, in order.
func allowedSummary(counts map[string]int) string {
	if len(counts) == 0 {
		return "none"
	}
	words := make([]string, 0, len(counts))
	for w := range counts {
		words = append(words, w)
	}
	sort.Strings(words)
	parts := make([]string, len(words))
	for i, w := range words {
		parts[i] = fmt.Sprintf("%s %d", w, counts[w])
	}
	return strings.Join(parts, ", ")
}
