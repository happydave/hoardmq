package module

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Design invariant 9: nothing reachable from a built-in module may be
// unreachable from an external one. These tests hold the built-in modules to
// the extension API, which imports nothing of the broker's, so whatever a
// built-in module gets from the broker an external module gets too.

const (
	brokerModule = "github.com/happydave/hoardmq"
	apiPrefix    = brokerModule + "/ext"
	builtinDir   = "builtin"
)

// implementationLibraries are the internal packages a built-in module may
// import: each is the module's own implementation, which an external module
// replaces with its own code, not a way into the broker.
var implementationLibraries = map[string]string{
	brokerModule + "/internal/auth":  "the authenticators themselves: users and ACL files, JWT, LDAP, the database authenticator, certificate revocation, SCRAM",
	brokerModule + "/internal/jose":  "JSON Web Key sets and token signatures, for auth.jwt",
	brokerModule + "/internal/ldap":  "the LDAP client, for auth.ldap",
	brokerModule + "/internal/store": "the write-ahead log engine store.wal runs on",
	brokerModule + "/internal/otel":  "the OTLP exporter traces.otlp supplies the tracer",
}

// repoDir is a directory of the repository, given relative to its root.
func repoDir(rel string) string { return filepath.Join("..", "..", filepath.FromSlash(rel)) }

// nonTestFiles parses the non-test Go files of dir.
func nonTestFiles(t *testing.T, fset *token.FileSet, dir string, mode parser.Mode) []*ast.File {
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
	if len(files) == 0 {
		t.Fatalf("%s holds no Go files", dir)
	}
	return files
}

func importPathOf(imp *ast.ImportSpec) string {
	p, _ := strconv.Unquote(imp.Path.Value)
	return p
}

// importName is the name a file refers to an import by.
func importName(imp *ast.ImportSpec) string {
	if imp.Name != nil {
		return imp.Name.Name
	}
	p := importPathOf(imp)
	return p[strings.LastIndex(p, "/")+1:]
}

func isStandard(path string) bool {
	first, _, _ := strings.Cut(path, "/")
	return !strings.Contains(first, ".")
}

func isAPI(path string) bool { return path == apiPrefix || strings.HasPrefix(path, apiPrefix+"/") }

// TestBuiltinsImportOnlyTheAPI: a built-in module imports the standard
// library, the extension API and the implementation libraries, and nothing
// else of the broker's.
func TestBuiltinsImportOnlyTheAPI(t *testing.T) {
	fset := token.NewFileSet()
	for _, f := range nonTestFiles(t, fset, builtinDir, parser.ImportsOnly) {
		for _, imp := range f.Imports {
			path := importPathOf(imp)
			if isStandard(path) || isAPI(path) {
				continue
			}
			if _, ok := implementationLibraries[path]; ok {
				continue
			}
			t.Errorf("%s imports %s: a built-in module reaches the broker only through the extension API (%s)", fset.Position(imp.Pos()).Filename, path, apiPrefix)
		}
	}
}

// apiAliases is every name an implementation library declares as an alias of
// an extension API type, or as a constant or variable taken from the API.
func apiAliases(t *testing.T, dir string) map[string]bool {
	t.Helper()
	fset := token.NewFileSet()
	names := map[string]bool{}
	for _, f := range nonTestFiles(t, fset, dir, parser.SkipObjectResolution) {
		api := map[string]bool{}
		for _, imp := range f.Imports {
			if isAPI(importPathOf(imp)) {
				api[importName(imp)] = true
			}
		}
		fromAPI := func(e ast.Expr) bool {
			found := false
			ast.Inspect(e, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok {
					if x, ok := sel.X.(*ast.Ident); ok && api[x.Name] {
						found = true
					}
				}
				return !found
			})
			return found
		}
		for _, d := range f.Decls {
			gd, ok := d.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, spec := range gd.Specs {
				switch s := spec.(type) {
				case *ast.TypeSpec:
					if s.Assign.IsValid() && fromAPI(s.Type) {
						names[s.Name.Name] = true
					}
				case *ast.ValueSpec:
					for i, v := range s.Values {
						if i < len(s.Names) && fromAPI(v) {
							names[s.Names[i].Name] = true
						}
					}
				}
			}
		}
	}
	return names
}

// TestBuiltinsSpellTheSeamFromTheAPI: a built-in module names no API type or
// constant through an implementation library's alias of it, so the seam it
// uses is the one an external module sees.
func TestBuiltinsSpellTheSeamFromTheAPI(t *testing.T) {
	aliases := map[string]map[string]bool{}
	for path := range implementationLibraries {
		aliases[path] = apiAliases(t, repoDir(strings.TrimPrefix(path, brokerModule+"/")))
	}
	fset := token.NewFileSet()
	for _, f := range nonTestFiles(t, fset, builtinDir, 0) {
		byName := map[string]string{}
		for _, imp := range f.Imports {
			if _, ok := implementationLibraries[importPathOf(imp)]; ok {
				byName[importName(imp)] = importPathOf(imp)
			}
		}
		ast.Inspect(f, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			x, ok := sel.X.(*ast.Ident)
			if !ok || x.Obj != nil { // a local name, not a package
				return true
			}
			if path, ok := byName[x.Name]; ok && aliases[path][sel.Sel.Name] {
				t.Errorf("%s: %s.%s is the extension API's; name it from %s", fset.Position(sel.Pos()), x.Name, sel.Sel.Name, apiPrefix)
			}
			return true
		})
	}
}

// TestNoHiddenModuleInterfaces: the broker and the registry assert no module
// value to an interface literal, a method set no module author could find;
// every optional method the broker looks for is a named interface of the
// extension API.
func TestNoHiddenModuleInterfaces(t *testing.T) {
	fset := token.NewFileSet()
	for _, dir := range []string{repoDir("internal/broker"), "."} {
		for _, f := range nonTestFiles(t, fset, dir, parser.SkipObjectResolution) {
			ast.Inspect(f, func(n ast.Node) bool {
				if ta, ok := n.(*ast.TypeAssertExpr); ok {
					if _, literal := ta.Type.(*ast.InterfaceType); literal {
						t.Errorf("%s: a type assertion to an interface literal", fset.Position(ta.Pos()))
					}
				}
				return true
			})
		}
	}
}

// TestImplementationLibrariesExist: every allowlisted library is a package,
// so a renamed one cannot leave the list silently wider than it reads.
func TestImplementationLibrariesExist(t *testing.T) {
	fset := token.NewFileSet()
	for path := range implementationLibraries {
		nonTestFiles(t, fset, repoDir(strings.TrimPrefix(path, brokerModule+"/")), parser.PackageClauseOnly)
	}
}

// moduleDependencies are the third-party modules a package under modules/
// may import from, each with its reason.
var moduleDependencies = map[string]string{
	"github.com/twmb/franz-go": "the Kafka client sink.kafka forwards with",
}

// TestModulesImportOnlyTheAPI: a module kept in the broker's tree under
// modules/ is held to what an outside module could import: the standard
// library, the extension API and its own third-party dependencies, nothing
// else of the broker's. Go does not stop a package of this module importing
// internal/, so this test does.
func TestModulesImportOnlyTheAPI(t *testing.T) {
	root := repoDir("modules")
	fset := token.NewFileSet()
	packages := 0
	err := filepath.WalkDir(root, func(dir string, d os.DirEntry, err error) error {
		if err != nil || !d.IsDir() || dir == root {
			return err
		}
		if strings.Contains(dir, string(filepath.Separator)+"testdata") {
			return filepath.SkipDir
		}
		packages++
		for _, f := range nonTestFiles(t, fset, dir, parser.ImportsOnly) {
			for _, imp := range f.Imports {
				if path := importPathOf(imp); !moduleImportAllowed(path) {
					t.Errorf("%s imports %s: a module under modules/ imports only the standard library, %s and %v", fset.Position(imp.Pos()).Filename, path, apiPrefix, moduleDependencies)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if packages == 0 {
		t.Fatalf("%s holds no module packages", root)
	}
}

func moduleImportAllowed(path string) bool {
	if isStandard(path) || isAPI(path) {
		return true
	}
	for dep := range moduleDependencies {
		if path == dep || strings.HasPrefix(path, dep+"/") {
			return true
		}
	}
	return false
}
