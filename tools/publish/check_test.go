package main

import (
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// The terms here are synthetic: these files are published, and the term
// check runs over them.
const testTerms = `# a test term list
host-2025-alpha
\balpha\b
beta@
/home/beta
secret-word
\bgam
@delta
`

// testPublisher is a noreply identity of the form GitHub gives.
var testPublisher = identity{name: "someone", email: "1234+someone@users.noreply.github.com"}

func testAllowed() map[string]bool {
	return map[string]bool{"alpha": true, "beta": true, "notbeta": true, "gam": true, "delta": true}
}

func mustTerms(t *testing.T, list string) []pattern {
	t.Helper()
	p, err := parseTerms([]byte(list))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestDecideRefusesEachCheckAlone(t *testing.T) {
	clear := results{patterns: 13, files: 694}
	if ok, why := decide(clear); !ok {
		t.Fatalf("all clear refused: %v", why)
	}
	for name, r := range map[string]results{
		"a term hit":        {patterns: 13, files: 694, hits: []hit{{where: "sim/x.go", line: 3, term: 6}}},
		"a personal email":  {patterns: 13, files: 694, identity: []string{"author: email is not a GitHub noreply address"}},
		"a missing licence": {patterns: 13, files: 694, missing: []string{"LICENSE-APACHE"}},
		"no pattern read":   {patterns: 0, files: 694},
		"no file examined":  {patterns: 13, files: 0},
	} {
		t.Run(name, func(t *testing.T) {
			ok, why := decide(r)
			if ok || len(why) != 1 {
				t.Fatalf("decide = %v, %v; want a no-go with one reason", ok, why)
			}
		})
	}
}

func TestParseTermsSkipsCommentsAndRefusesBadPatterns(t *testing.T) {
	p := mustTerms(t, testTerms)
	if len(p) != 7 || p[0].line != 2 || p[6].line != 8 {
		t.Fatalf("patterns %d, first line %d, last line %d; want 7 from lines 2 to 8", len(p), p[0].line, p[len(p)-1].line)
	}
	if _, err := parseTerms([]byte("ok\n(unclosed\n")); err == nil || !strings.Contains(err.Error(), "line 2") {
		t.Fatalf("a pattern that does not compile: %v", err)
	}
	if _, err := parseTerms([]byte("# only a comment\n\n")); err == nil {
		t.Fatal("an empty term list was accepted")
	}
}

func TestParseAllowedRefusesMoreThanAWord(t *testing.T) {
	a, err := parseAllowed([]byte("# c\nalpha\n\nbeta\n"))
	if err != nil || !reflect.DeepEqual(a, map[string]bool{"alpha": true, "beta": true}) {
		t.Fatalf("parseAllowed = %v, %v", a, err)
	}
	for _, bad := range []string{"alpha beta\n", "beta@\n", "/home/beta\n"} {
		if _, err := parseAllowed([]byte(bad)); err == nil {
			t.Errorf("%q accepted as an allowed term", bad)
		}
	}
}

func TestTermCheckAllowsOnlyTheWholeAllowedWord(t *testing.T) {
	pats := mustTerms(t, testTerms)
	for _, c := range []struct {
		text    string
		allowed string // the word allowed, or "" for a hit
	}{
		{"run on alpha today", "alpha"},
		{"ssh notbeta@192.0.2.1", "notbeta"},
		{"ssh beta@192.0.2.1", "beta"},
		{"ssh xbeta@192.0.2.1", ""},
		{"host host-2025-alpha", ""},
		{"cd /home/beta/x", ""},
		{"a secret-word here", ""},
		{"a gam ray", "gam"},
		{"a gamma ray", ""},
		{"see @delta now", "delta"},
	} {
		t.Run(c.text, func(t *testing.T) {
			r := termCheck(pats, testAllowed(), []entry{{path: "f.txt", data: []byte("line one\n" + c.text + "\n")}}, "")
			if c.allowed == "" {
				if len(r.hits) == 0 {
					t.Fatalf("no hit for %q", c.text)
				}
				for _, h := range r.hits {
					if h.where != "f.txt" || h.line != 2 {
						t.Errorf("hit %v, want f.txt line 2", h)
					}
					if strings.Contains(h.String(), "alpha") || strings.Contains(h.String(), "beta") || strings.Contains(h.String(), "secret") {
						t.Errorf("hit %q repeats the matched text", h)
					}
				}
				return
			}
			if len(r.hits) != 0 || r.allowed[c.allowed] == 0 {
				t.Fatalf("hits %v, allowed %v; want %s allowed and no hit", r.hits, r.allowed, c.allowed)
			}
		})
	}
}

func TestTermCheckReadsPathsAndTheMessage(t *testing.T) {
	pats := mustTerms(t, testTerms)
	r := termCheck(pats, testAllowed(), []entry{{path: "docs/secret-word.md", data: []byte("clean\n")}}, "subject\n\nbody host-2025-alpha\n")
	var where []string
	for _, h := range r.hits {
		where = append(where, h.String())
	}
	sort.Strings(where)
	want := []string{"(commit message):3: term-list line 2", "docs/secret-word.md (path): term-list line 6"}
	if !reflect.DeepEqual(where, want) {
		t.Fatalf("hits %q, want %q", where, want)
	}
}

func TestIdentityAcceptsOnlyANoreplyAddress(t *testing.T) {
	for _, c := range []struct {
		id   identity
		want int
	}{
		{testPublisher, 0},
		{identity{"someone", "someone@users.noreply.github.com"}, 0},
		{identity{"", testPublisher.email}, 1},
		{identity{"someone", ""}, 1},
		// Built from parts, so the publish check over this file does not
		// read it as an address.
		{identity{"someone", "someone" + "@" + "mail.isp-domain.net"}, 1},
		{identity{"someone", "someone" + "@" + "users.noreply.github.com.isp-domain.net"}, 1},
	} {
		if got := identityProblems("author", c.id); len(got) != c.want {
			t.Errorf("%s: %v, want %d problems", c.id, got, c.want)
		}
	}
}

func TestEmailProblemsNameNonReservedDomains(t *testing.T) {
	text := "alice@example.com\nsim@node.example.invalid\nx@sub.example.org\n" + "someone" + "@" + "mail.isp-domain.net\n"
	got := emailProblems([]entry{{path: "a.md", data: []byte(text)}}, "")
	if !reflect.DeepEqual(got, []string{"a.md:4: email at mail.isp-domain.net"}) {
		t.Fatalf("emailProblems = %q", got)
	}
}

func TestMissingLicences(t *testing.T) {
	full := []entry{
		{path: "LICENSE-MIT", mode: "100644", data: []byte("x")},
		{path: "LICENSE-APACHE", mode: "100644", data: []byte("x")},
		{path: "NOTICE", mode: "100644", data: []byte("x")},
	}
	if got := missingLicences(full); len(got) != 0 {
		t.Fatalf("missing %v from a full set", got)
	}
	empty := append([]entry{}, full[0], full[2], entry{path: "LICENSE-APACHE", mode: "100644"})
	if got := missingLicences(empty); !reflect.DeepEqual(got, []string{"LICENSE-APACHE"}) {
		t.Fatalf("an empty LICENSE-APACHE: missing %v", got)
	}
	nested := append([]entry{}, full[0], full[2], entry{path: "docs/LICENSE-APACHE", mode: "100644", data: []byte("x")})
	if got := missingLicences(nested); !reflect.DeepEqual(got, []string{"LICENSE-APACHE"}) {
		t.Fatalf("LICENSE-APACHE only below the root: missing %v", got)
	}
	link := append([]entry{}, full[0], full[2], entry{path: "LICENSE-APACHE", mode: "120000", data: []byte("LICENSE-MIT")})
	if got := missingLicences(link); !reflect.DeepEqual(got, []string{"LICENSE-APACHE"}) {
		t.Fatalf("LICENSE-APACHE as a symbolic link: missing %v", got)
	}
}

// TestAllowedTermsAreTheOwnersFive pins the committed exceptions file: a
// sixth word is the owner's decision, and changes this test with it.
func TestAllowedTermsAreTheOwnersFive(t *testing.T) {
	data, err := os.ReadFile("allowed-terms.txt")
	if err != nil {
		t.Fatal(err)
	}
	got, err := parseAllowed(data)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"ai": true, "ai2": true, "gtr": true, "dave": true, "notdave": true}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("allowed terms %v, want exactly %v", sortedKeys(got), sortedKeys(want))
	}
}

func TestJudgeWrittenRefusesWhatWasNotChecked(t *testing.T) {
	pats := mustTerms(t, testTerms)
	before := results{patterns: len(pats), files: 7}
	good := commitObject{tree: "t1", parents: []string{"p1"}, author: testPublisher, committer: testPublisher, message: "main abc, without sim/results/\n"}
	r, err := judgeWritten(good, "t1", "p1", pats, testAllowed(), before)
	if ok, why := decide(r); err != nil || !ok {
		t.Fatalf("the commit as checked: %v, %v", err, why)
	}
	root := good
	root.parents = nil
	if r, err := judgeWritten(root, "t1", "", pats, testAllowed(), before); err != nil || r.files != 7 {
		t.Fatalf("a first commit with no parent: %v, %+v", err, r)
	}
	for name, c := range map[string]struct {
		c   commitObject
		tip string
	}{
		"another tree":         {commitObject{tree: "t2", parents: good.parents, author: testPublisher, committer: testPublisher}, "p1"},
		"no parent":            {commitObject{tree: "t1", author: testPublisher, committer: testPublisher}, "p1"},
		"another parent":       {commitObject{tree: "t1", parents: []string{"p2"}, author: testPublisher, committer: testPublisher}, "p1"},
		"a parent on the root": {commitObject{tree: "t1", parents: []string{"p1"}, author: testPublisher, committer: testPublisher}, ""},
	} {
		if _, err := judgeWritten(c.c, "t1", c.tip, pats, testAllowed(), before); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	for name, c := range map[string]commitObject{
		"a personal author":     {tree: "t1", parents: good.parents, author: identity{"someone", "someone" + "@" + "mail.isp-domain.net"}, committer: testPublisher},
		"a personal committer":  {tree: "t1", parents: good.parents, author: testPublisher, committer: identity{"someone", ""}},
		"a term in the message": {tree: "t1", parents: good.parents, author: testPublisher, committer: testPublisher, message: "main abc\n\nsecret-word\n"},
	} {
		r, err := judgeWritten(c, "t1", "p1", pats, testAllowed(), before)
		if ok, _ := decide(r); err != nil || ok {
			t.Errorf("%s: err %v, go %v; want a no-go", name, err, ok)
		}
	}
}
