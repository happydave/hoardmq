package modlicense

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestDetect(t *testing.T) {
	cases := map[Family]string{
		MIT:           "MIT License\n\nPermission is hereby granted, free of charge, to any person",
		Apache2:       "                                 Apache License\n                           Version 2.0, January 2004",
		BSD:           "Redistribution and use in source and binary forms, with or without modification",
		ISC:           "Permission to use, copy, modify, and/or distribute this software for any purpose with or without fee is hereby granted",
		MPL2:          "Mozilla Public License Version 2.0\n\n1. Definitions",
		EPL2:          "Eclipse Public License - v 2.0",
		Unlicense:     "This is free and unencumbered software released into the public domain.",
		GPL:           "GNU GENERAL PUBLIC LICENSE\nVersion 3, 29 June 2007",
		LGPL:          "GNU LESSER GENERAL PUBLIC LICENSE\nVersion 3",
		AGPL:          "GNU AFFERO GENERAL PUBLIC LICENSE\nVersion 3",
		SSPL:          "Server Side Public License\nVERSION 1, OCTOBER 16, 2018",
		BSL:           "Business Source License 1.1",
		CommonsClause: "MIT License\n\nPermission is hereby granted, free of charge\n\n\"Commons Clause\" License Condition v1.0",
		Unknown:       "You may do whatever you like.",
	}
	for want, text := range cases {
		if got := Detect(text); got != want {
			t.Errorf("Detect(%q) = %s, want %s", text[:20], got, want)
		}
	}
}

// EPL-2.0 and MPL-2.0 quote the GPL as a possible Secondary License; the
// quotation must not classify the file as GPL.
func TestSecondaryLicenseQuotationIsNotGPL(t *testing.T) {
	epl := "Eclipse Public License - v 2.0\n... Secondary License means either the GNU General Public License, Version 2.0, or any later versions of that license ..."
	if got := Detect(epl); got != EPL2 {
		t.Errorf("EPL text classified as %s", got)
	}
	mpl := "Mozilla Public License Version 2.0\n1.12. Secondary License means either the GNU General Public License, Version 2.0, the GNU Lesser General Public License, Version 2.1 ..."
	if got := Detect(mpl); got != MPL2 {
		t.Errorf("MPL text classified as %s", got)
	}
}

func TestPolicy(t *testing.T) {
	for _, f := range Allowed {
		if !IsAllowed(f) {
			t.Errorf("%s should be allowed", f)
		}
	}
	for _, f := range append(Forbidden, Unknown) {
		if IsAllowed(f) {
			t.Errorf("%s should not be allowed", f)
		}
	}
}

func TestParseList(t *testing.T) {
	in := `{"Path":"github.com/happydave/hoardmq","Main":true,"Dir":"/x"}
{"Path":"b.example/m","Version":"v1.2.0","Dir":"/cache/b"}
{"Path":"a.example/m","Version":"v0.1.0","Dir":"/cache/a"}
`
	mods, err := ParseList(strings.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}
	if len(mods) != 3 || mods[0].Path != "a.example/m" || !mods[2].Main {
		t.Fatalf("got %+v", mods)
	}
	if _, err := ParseList(strings.NewReader(`{"Path":`)); err == nil {
		t.Error("expected parse error")
	}
}

func fakeModule(t *testing.T, name, licenseName, text string) Module {
	t.Helper()
	dir := filepath.Join(t.TempDir(), name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if licenseName != "" {
		if err := os.WriteFile(filepath.Join(dir, licenseName), []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return Module{Path: "example.com/" + name, Version: "v1.0.0", Dir: dir}
}

func TestClassifyNoticeAndViolations(t *testing.T) {
	main := Module{Path: "github.com/happydave/hoardmq", Main: true}
	ok := fakeModule(t, "ok", "LICENSE", "Permission is hereby granted, free of charge")
	copying := fakeModule(t, "copying", "COPYING.txt", "Redistribution and use in source and binary forms")
	bad := fakeModule(t, "bad", "LICENSE.md", "GNU GENERAL PUBLIC LICENSE")
	none := fakeModule(t, "none", "", "")
	odd := fakeModule(t, "odd", "LICENSE", "Do what thou wilt.")

	reports := Classify([]Module{main, ok, copying, bad, none, odd})
	if len(reports) != 5 {
		t.Fatalf("main module should be skipped; got %d reports", len(reports))
	}
	notice := Notice(reports)
	for _, want := range []string{
		"HoardMQ\n", "LICENSE-APACHE", "LICENSE-MIT",
		"example.com/ok v1.0.0 — LICENSE (MIT)",
		"example.com/copying v1.0.0 — COPYING.txt (BSD)",
		"example.com/bad v1.0.0 — LICENSE.md (GPL)",
		"example.com/none v1.0.0 — licence file not found",
		"example.com/odd v1.0.0 — LICENSE (unknown)",
	} {
		if !strings.Contains(notice, want) {
			t.Errorf("NOTICE missing %q:\n%s", want, notice)
		}
	}
	v := Violations(reports)
	var paths []string
	for _, r := range v {
		paths = append(paths, r.Module.Path)
	}
	if got := strings.Join(paths, ","); got != "example.com/bad,example.com/none,example.com/odd" {
		t.Errorf("violations = %s", got)
	}
}

func TestNoticeWithNoModules(t *testing.T) {
	if n := Notice(nil); !strings.Contains(n, "No third-party modules.") {
		t.Errorf("got %q", n)
	}
}

func TestLicenseFilePrefersStableOrder(t *testing.T) {
	dir := t.TempDir()
	for _, n := range []string{"LICENSE.txt", "LICENSE", "COPYING"} {
		if err := os.WriteFile(filepath.Join(dir, n), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if got := filepath.Base(LicenseFile(dir)); got != "COPYING" {
		t.Errorf("got %s", got)
	}
	if LicenseFile("") != "" || LicenseFile(filepath.Join(dir, "missing")) != "" {
		t.Error("missing dir should yield empty")
	}
}

// TestListBuildExcludesTestOnlyModules: the binary's module list has the
// Raft core but not the Paho test clients.
func TestListBuildExcludesTestOnlyModules(t *testing.T) {
	gomod, err := exec.Command("go", "env", "GOMOD").Output()
	if err != nil {
		t.Skip("go env GOMOD unavailable")
	}
	root := filepath.Dir(strings.TrimSpace(string(gomod)))
	mods, err := ListBuild(context.Background(), root, "./cmd/...")
	if err != nil {
		t.Fatal(err)
	}
	paths := map[string]bool{}
	for _, m := range mods {
		paths[m.Path] = true
	}
	if !paths["go.etcd.io/raft/v3"] || paths["github.com/eclipse/paho.golang"] || paths["github.com/eclipse/paho.mqtt.golang"] {
		t.Fatalf("build module list: %v", paths)
	}
	all, err := List(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, m := range all {
		if m.Path == "github.com/eclipse/paho.golang" {
			found = true
		}
	}
	if !found {
		t.Fatal("the full list should include the test clients")
	}
}
