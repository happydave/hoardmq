// Command astcmp reports whether each named Go file's code, its comments
// removed, is the same at a base revision and in the working tree: the proof
// that a change touched comments only.
//
//	go run ./tools/astcmp -repo <worktree> -base <rev> [-head <rev>] [-strings <list>] <file>...
//
// -head compares against that revision instead of the working tree. -strings
// names string literals allowed to differ, one per line as
// "path:baseLine:headLine" (the line each literal starts on); every string
// literal starting on a named line is masked on both sides, so only those
// may differ, and each named pair must actually differ.
//
// Each file is parsed without comments and its syntax tree dumped with every
// position zeroed, so a comment's removal or a line it no longer fills cannot
// count; the two dumps must be identical. Directive comments (//go:, //nolint,
// //lint:, //line, //export) are code to the toolchain, so their lines must
// also match. It prints one line per file, "same" or "CHANGED", and exits 1 if
// any changed or failed to parse.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
)

var posType = reflect.TypeOf(token.Pos(0))

// zero clears every token.Pos reachable from v.
func zero(v reflect.Value, seen map[uintptr]bool) {
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() || seen[v.Pointer()] {
			return
		}
		seen[v.Pointer()] = true
		zero(v.Elem(), seen)
	case reflect.Interface:
		if !v.IsNil() {
			zero(v.Elem(), seen)
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			f := v.Field(i)
			if f.Type() == posType && f.CanSet() {
				f.SetInt(0)
				continue
			}
			zero(f, seen)
		}
	case reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			zero(v.Index(i), seen)
		}
	}
}

func directives(src []byte) string {
	var out []string
	for _, line := range strings.Split(string(src), "\n") {
		t := strings.TrimSpace(line)
		i := strings.Index(t, "//")
		if i < 0 {
			continue
		}
		c := t[i:]
		for _, p := range []string{"//go:", "//nolint", "//lint:", "//line ", "//export "} {
			if strings.HasPrefix(c, p) {
				out = append(out, c)
			}
		}
	}
	return strings.Join(out, "\n")
}

// mask replaces every string literal starting on one of lines with a
// placeholder naming the line's index and the literal's order on it, and
// returns the literals it replaced, in that order.
func mask(fset *token.FileSet, f *ast.File, lines []int) []string {
	var got []string
	k := map[int]int{}
	ast.Inspect(f, func(n ast.Node) bool {
		lit, ok := n.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return true
		}
		line := fset.Position(lit.Pos()).Line
		for i, l := range lines {
			if l == line {
				got = append(got, lit.Value)
				lit.Value = fmt.Sprintf("ALLOWED_%d_%d", i, k[i])
				k[i]++
			}
		}
		return true
	})
	return got
}

func codeMasked(name string, src []byte, lines []int) ([]byte, []string, error) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, name, src, parser.SkipObjectResolution)
	if err != nil {
		return nil, nil, err
	}
	masked := mask(fset, f, lines)
	f.Comments, f.Scope, f.Unresolved = nil, nil, nil
	zero(reflect.ValueOf(f), map[uintptr]bool{})
	var b bytes.Buffer
	if err := ast.Fprint(&b, nil, f, nil); err != nil {
		return nil, nil, err
	}
	b.WriteString("\ndirectives:\n" + directives(src))
	return b.Bytes(), masked, nil
}

// sameMasked compares with the named literals masked, and fails a named line
// that holds no literal or whose literals did not change.
func sameMasked(name string, before, after []byte, baseLines, headLines []int) (bool, error) {
	a, olds, err := codeMasked(name, before, baseLines)
	if err != nil {
		return false, fmt.Errorf("before: %w", err)
	}
	b, news, err := codeMasked(name, after, headLines)
	if err != nil {
		return false, fmt.Errorf("after: %w", err)
	}
	if len(olds) < len(baseLines) || len(olds) != len(news) {
		return false, fmt.Errorf("named lines hold %d literals before and %d after, for %d names", len(olds), len(news), len(baseLines))
	}
	changed := 0
	for i := range olds {
		if olds[i] != news[i] {
			changed++
		}
	}
	if changed < len(baseLines) {
		return false, fmt.Errorf("%d of %d named literals changed", changed, len(baseLines))
	}
	return bytes.Equal(a, b), nil
}

func fprintf(w io.Writer, format string, a ...any) { _, _ = fmt.Fprintf(w, format, a...) }

func main() {
	repo := flag.String("repo", ".", "the worktree")
	base := flag.String("base", "", "the base revision")
	head := flag.String("head", "", "the revision compared, instead of the working tree")
	strs := flag.String("strings", "", "the list of string literals allowed to differ")
	flag.Parse()
	if *base == "" || flag.NArg() == 0 {
		fprintf(os.Stderr, "usage: astcmp -repo <worktree> -base <rev> [-head <rev>] [-strings <list>] <file>...\n")
		os.Exit(2)
	}
	os.Exit(run(*repo, *base, *head, *strs, flag.Args(), os.Stdout, os.Stderr))
}

// run compares each file and returns the exit status: 0 when every file is
// the same, 1 when any changed or could not be read or parsed, 2 when the
// strings list is unusable.
func run(repo, base, head, strs string, files []string, out, errOut io.Writer) int {
	baseLines, headLines := map[string][]int{}, map[string][]int{}
	if strs != "" {
		data, err := os.ReadFile(strs)
		if err != nil {
			fprintf(errOut, "%v\n", err)
			return 2
		}
		for _, l := range strings.Split(string(data), "\n") {
			l = strings.TrimSpace(l)
			if l == "" || strings.HasPrefix(l, "#") {
				continue
			}
			var bl, hl int
			parts := strings.Split(l, ":")
			if len(parts) != 3 {
				fprintf(errOut, "bad strings line: %s\n", l)
				return 2
			}
			if _, err := fmt.Sscan(parts[1]+" "+parts[2], &bl, &hl); err != nil {
				fprintf(errOut, "bad strings line: %s\n", l)
				return 2
			}
			baseLines[parts[0]] = append(baseLines[parts[0]], bl)
			headLines[parts[0]] = append(headLines[parts[0]], hl)
		}
	}
	bad := 0
	for _, f := range files {
		before, err := exec.Command("git", "-C", repo, "show", base+":"+f).Output()
		if err != nil {
			fprintf(out, "%s: base unreadable: %v\n", f, err)
			bad++
			continue
		}
		var after []byte
		if head != "" {
			after, err = exec.Command("git", "-C", repo, "show", head+":"+f).Output()
		} else {
			after, err = os.ReadFile(filepath.Join(repo, f))
		}
		if err != nil {
			fprintf(out, "%s: %v\n", f, err)
			bad++
			continue
		}
		ok, err := sameMasked(f, before, after, baseLines[f], headLines[f])
		switch {
		case err != nil:
			fprintf(out, "%s: %v\n", f, err)
			bad++
		case ok && len(baseLines[f]) > 0:
			fprintf(out, "%s: same but %d named literals\n", f, len(baseLines[f]))
		case ok:
			fprintf(out, "%s: same\n", f)
		default:
			fprintf(out, "%s: CHANGED\n", f)
			bad++
		}
	}
	if bad > 0 {
		return 1
	}
	return 0
}
