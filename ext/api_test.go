package ext

import (
	"bytes"
	"flag"
	"go/ast"
	"go/doc"
	"go/parser"
	"go/printer"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

var update = flag.Bool("update", false, "rewrite api.txt from the API as it is")

const apiModule = "github.com/happydave/hoardmq/ext"

// apiPackages lists the directories of the API's packages: this one and
// every directory under it holding Go files, testdata aside.
func apiPackages(t *testing.T) []string {
	t.Helper()
	var dirs []string
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && d.Name() == "testdata" {
			return filepath.SkipDir
		}
		if !d.IsDir() && strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go") {
			dir := filepath.Dir(path)
			if len(dirs) == 0 || dirs[len(dirs)-1] != dir {
				dirs = append(dirs, dir)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(dirs)
	return dirs
}

// importPath is the import path of the package in dir.
func importPath(dir string) string {
	if dir == "." {
		return apiModule
	}
	return apiModule + "/" + filepath.ToSlash(dir)
}

func parseDir(t *testing.T, fset *token.FileSet, dir string, mode parser.Mode) []*ast.File {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var files []*ast.File
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, mode)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
	}
	return files
}

// TestAPIImportsOnlyTheStandardLibrary: no file of the API imports anything
// but the standard library and the API's own packages, so no type in a
// public signature can be one of the broker's internals.
func TestAPIImportsOnlyTheStandardLibrary(t *testing.T) {
	fset := token.NewFileSet()
	for _, dir := range apiPackages(t) {
		for _, f := range parseDir(t, fset, dir, parser.ImportsOnly) {
			for _, imp := range f.Imports {
				path, _ := strconv.Unquote(imp.Path.Value)
				first, _, _ := strings.Cut(path, "/")
				if !strings.Contains(first, ".") || path == apiModule || strings.HasPrefix(path, apiModule+"/") {
					continue
				}
				t.Errorf("%s imports %s: the extension API imports only the standard library and itself", fset.Position(imp.Pos()).Filename, path)
			}
		}
	}
}

// renderAPI lists every exported declaration of every API package.
func renderAPI(t *testing.T) []byte {
	t.Helper()
	var b bytes.Buffer
	b.WriteString("hoardmq extension API " + APIVersion + "\n")
	cfg := printer.Config{Mode: printer.UseSpaces | printer.TabIndent, Tabwidth: 8}
	var fset *token.FileSet
	emit := func(node ast.Node) {
		stripComments(node)
		var nb bytes.Buffer
		if err := cfg.Fprint(&nb, fset, node); err != nil {
			t.Fatal(err)
		}
		b.Write(nb.Bytes())
		b.WriteString("\n")
	}
	values := func(vs []*doc.Value) {
		for _, v := range vs {
			emit(v.Decl)
		}
	}
	funcs := func(fs []*doc.Func) {
		for _, f := range fs {
			f.Decl.Body = nil
			emit(f.Decl)
		}
	}
	for _, dir := range apiPackages(t) {
		fset = token.NewFileSet()
		files := parseDir(t, fset, dir, 0)
		pkg, err := doc.NewFromFiles(fset, files, importPath(dir))
		if err != nil {
			t.Fatal(err)
		}
		b.WriteString("\npackage " + importPath(dir) + "\n\n")
		values(pkg.Consts)
		values(pkg.Vars)
		funcs(pkg.Funcs)
		for _, ty := range pkg.Types {
			emit(ty.Decl)
			values(ty.Consts)
			values(ty.Vars)
			funcs(ty.Funcs)
			funcs(ty.Methods)
		}
	}
	return b.Bytes()
}

// stripComments drops the doc and line comments of a declaration, which are
// not part of the API's shape.
func stripComments(node ast.Node) {
	ast.Inspect(node, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.GenDecl:
			x.Doc = nil
		case *ast.FuncDecl:
			x.Doc = nil
		case *ast.TypeSpec:
			x.Doc, x.Comment = nil, nil
		case *ast.ValueSpec:
			x.Doc, x.Comment = nil, nil
		case *ast.Field:
			x.Doc, x.Comment = nil, nil
		}
		return true
	})
}

// TestAPIGolden: the exported API is what api.txt records, at the version
// APIVersion names, and the changelog has a section for that version. A
// change to the API is a deliberate edit of all three (go test -run
// TestAPIGolden -update rewrites api.txt).
func TestAPIGolden(t *testing.T) {
	got := renderAPI(t)
	if *update {
		if err := os.WriteFile("api.txt", got, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile("api.txt")
	if err != nil {
		t.Fatal(err)
	}
	head, _, _ := bytes.Cut(want, []byte("\n"))
	if string(head) != "hoardmq extension API "+APIVersion {
		t.Errorf("api.txt records %q; APIVersion is %s", head, APIVersion)
	}
	if !bytes.Equal(got, want) {
		gl, wl := strings.Split(string(got), "\n"), strings.Split(string(want), "\n")
		for i := 0; i < len(gl) || i < len(wl); i++ {
			var g, w string
			if i < len(gl) {
				g = gl[i]
			}
			if i < len(wl) {
				w = wl[i]
			}
			if g != w {
				t.Errorf("the exported API differs from api.txt at line %d:\n got %q\nwant %q\n(change APIVersion, ext/CHANGELOG.md and api.txt together)", i+1, g, w)
				break
			}
		}
	}
	changelog, err := os.ReadFile("CHANGELOG.md")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(changelog, []byte("\n## "+APIVersion+"\n")) {
		t.Errorf("CHANGELOG.md has no section for %s", APIVersion)
	}
}
