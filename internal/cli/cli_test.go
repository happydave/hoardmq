package cli

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/auth/scramtest"
	"github.com/happydave/hoardmq/internal/broker"
)

const validDoc = `{"version":1,"node":{"dataDir":"data"},"listeners":[{"name":"plain","type":"tcp","address":":1883"}]}`

func run(t *testing.T, environ []string, args ...string) (int, string, string) {
	t.Helper()
	return runWithStdin(t, "", environ, args...)
}

func runWithStdin(t *testing.T, stdin string, environ []string, args ...string) (int, string, string) {
	t.Helper()
	var out, errOut bytes.Buffer
	code := Run(args, environ, strings.NewReader(stdin), &out, &errOut)
	return code, out.String(), errOut.String()
}

func writeDoc(t *testing.T, dir, name, body string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestUsagePaths(t *testing.T) {
	cases := []struct {
		name   string
		args   []string
		code   int
		stream string // "stdout" or "stderr"
	}{
		{"no args", nil, ExitUsage, "stderr"},
		{"help", []string{"help"}, ExitOK, "stdout"},
		{"-h", []string{"-h"}, ExitOK, "stdout"},
		{"bad flag", []string{"-nonsense"}, ExitUsage, "stderr"},
		{"unknown command", []string{"bogus"}, ExitUsage, "stderr"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			code, out, errOut := run(t, nil, c.args...)
			if code != c.code {
				t.Fatalf("exit %d, want %d (stderr %q)", code, c.code, errOut)
			}
			text := out
			if c.stream == "stderr" {
				text = errOut
			}
			for _, want := range []string{"usage: hoardmq", "serve", "check", "version", "HOARDMQ_LOG_LEVEL"} {
				if !strings.Contains(text, want) {
					t.Errorf("%s missing %q:\n%s", c.stream, want, text)
				}
			}
		})
	}
}

func TestVersion(t *testing.T) {
	code, out, _ := run(t, nil, "version")
	if code != ExitOK || !strings.HasPrefix(out, "hoardmq ") {
		t.Fatalf("exit %d out %q", code, out)
	}
	if code, _, errOut := run(t, nil, "version", "extra"); code != ExitUsage || !strings.Contains(errOut, "unexpected argument") {
		t.Fatalf("exit %d err %q", code, errOut)
	}
}

func TestVersionFrom(t *testing.T) {
	cases := []struct {
		main     string
		settings []debug.BuildSetting
		want     string
	}{
		{"v0.1.0", nil, "v0.1.0"},
		{"(devel)", nil, "devel"},
		{"", nil, "devel"},
		{"(devel)", []debug.BuildSetting{{Key: "vcs.revision", Value: "0123456789abcdef"}}, "devel (0123456789ab)"},
		{"(devel)", []debug.BuildSetting{{Key: "vcs.revision", Value: "abc"}, {Key: "vcs.modified", Value: "true"}}, "devel (abc-dirty)"},
	}
	for _, c := range cases {
		if got := versionFrom(c.main, c.settings); got != c.want {
			t.Errorf("versionFrom(%q) = %q, want %q", c.main, got, c.want)
		}
	}
}

func TestCheckValid(t *testing.T) {
	path := writeDoc(t, t.TempDir(), "c.json", validDoc)
	code, out, errOut := run(t, nil, "check", "-config", path)
	if code != ExitOK || out != "ok\n" || errOut != "" {
		t.Fatalf("exit %d out %q err %q", code, out, errOut)
	}
}

func TestCheckReportsEveryProblem(t *testing.T) {
	path := writeDoc(t, t.TempDir(), "c.json", `{"version":2,"node":{"dataDir":""},"log":{"level":"x"}}`)
	code, out, errOut := run(t, []string{"HOARDMQ_TYPO=1"}, "check", "-config", path)
	if code != ExitProblems || out != "" {
		t.Fatalf("exit %d out %q", code, out)
	}
	for _, want := range []string{
		path + ": /: unknown environment override HOARDMQ_TYPO",
		path + ": /log/level: must be one of",
		path + ": /node/dataDir: must not be empty",
		path + ": /version: must be 1",
	} {
		if !strings.Contains(errOut, want) {
			t.Errorf("missing %q in:\n%s", want, errOut)
		}
	}
	if n := strings.Count(errOut, "\n"); n != 4 {
		t.Errorf("expected 4 lines, got %d:\n%s", n, errOut)
	}
}

func TestCheckMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "absent.json")
	code, _, errOut := run(t, nil, "check", "-config", path)
	if code != ExitProblems || !strings.Contains(errOut, "absent.json: /: open") {
		t.Fatalf("exit %d err %q", code, errOut)
	}
}

func TestCheckPathResolution(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	writeDoc(t, dir, "hoardmq.json", validDoc)
	if code, _, errOut := run(t, nil, "check"); code != ExitOK {
		t.Fatalf("default path: exit %d err %q", code, errOut)
	}
	env := writeDoc(t, dir, "env.json", `{"version":1}`)
	if code, _, errOut := run(t, []string{"HOARDMQ_CONFIG=" + env}, "check"); code != ExitProblems || !strings.Contains(errOut, "env.json") {
		t.Fatalf("env path: exit %d err %q", code, errOut)
	}
	if code, _, _ := run(t, []string{"HOARDMQ_CONFIG=" + env}, "check", "-config", "hoardmq.json"); code != ExitOK {
		t.Fatal("flag should win over the environment")
	}
}

func TestCheckUsageErrors(t *testing.T) {
	if code, _, errOut := run(t, nil, "check", "extra"); code != ExitUsage || !strings.Contains(errOut, "unexpected argument") {
		t.Fatalf("exit %d err %q", code, errOut)
	}
	if code, _, _ := run(t, nil, "check", "-bogus"); code != ExitUsage {
		t.Fatalf("exit %d", code)
	}
	if code, out, _ := run(t, nil, "check", "-h"); code != ExitOK || !strings.Contains(out, "usage: hoardmq check") {
		t.Fatalf("exit %d out %q", code, out)
	}
}

func TestServeValidatesThenStarts(t *testing.T) {
	dir := t.TempDir()
	bad := writeDoc(t, dir, "bad.json", `{"version":1}`)
	if code, _, errOut := run(t, nil, "serve", "-config", bad); code != ExitProblems || !strings.Contains(errOut, "/: missing required member") {
		t.Fatalf("exit %d err %q", code, errOut)
	}
	// A listener that cannot bind fails before anything is created.
	blocker, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Close() }()
	data := filepath.Join(dir, "data")
	taken := writeDoc(t, dir, "taken.json", `{"version":1,"node":{"dataDir":"`+data+`"},"listeners":[{"name":"t","type":"tcp","address":"`+blocker.Addr().String()+`"}]}`)
	if code, _, errOut := run(t, nil, "serve", "-config", taken); code != ExitProblems || !strings.Contains(errOut, "address already in use") {
		t.Fatalf("exit %d err %q", code, errOut)
	}
	if _, err := os.Stat(data); !os.IsNotExist(err) {
		t.Error("data directory created despite bind failure")
	}
	// A successful start serves until the stop signal, then exits 0.
	prev := notifyContext
	notifyContext = func(parent context.Context) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(parent)
		cancel()
		return ctx, cancel
	}
	defer func() { notifyContext = prev }()
	good := writeDoc(t, dir, "good.json", `{"version":1,"node":{"dataDir":"`+data+`"},"listeners":[{"name":"t","type":"tcp","address":"127.0.0.1:0"}],"log":{"format":"json"}}`)
	code, _, errOut := run(t, nil, "serve", "-config", good)
	if code != ExitOK || !strings.Contains(errOut, `"listening"`) || !strings.Contains(errOut, `"shutting down"`) {
		t.Fatalf("exit %d err %q", code, errOut)
	}
	if _, err := os.Stat(data); err != nil {
		t.Error("data directory not created by a successful start")
	}
}

// With cluster.drain.onSignal, the stop signal drains before exiting; the
// notifier stub delivers the signal at once, then hands the drain wait a
// live context so the drain, not a second signal, ends the run.
func TestServeDrainOnSignal(t *testing.T) {
	dir := t.TempDir()
	data := filepath.Join(dir, "data")
	prev := notifyContext
	calls := 0
	notifyContext = func(parent context.Context) (context.Context, context.CancelFunc) {
		calls++
		ctx, cancel := context.WithCancel(parent)
		if calls == 1 {
			cancel()
		}
		return ctx, cancel
	}
	defer func() { notifyContext = prev }()
	doc := writeDoc(t, dir, "drain.json", `{"version":1,"node":{"dataDir":"`+data+`"},"listeners":[{"name":"t","type":"tcp","address":"127.0.0.1:0"}],"cluster":{"drain":{"onSignal":true}},"log":{"format":"json"}}`)
	code, _, errOut := run(t, nil, "serve", "-config", doc)
	if code != ExitOK || !strings.Contains(errOut, "draining before exit") || !strings.Contains(errOut, "drain complete") {
		t.Fatalf("exit %d err %q", code, errOut)
	}
}

// The reload signal re-reads the file and applies a reloadable change, and
// the node keeps serving until the stop signal. The notifier stubs rewrite
// the file, deliver one reload, and stop once its result is in. The node
// starts at warn and is reloaded to info, so its Info lines appear only once
// the reload has reached the loggers.
func TestServeReloadsOnSignal(t *testing.T) {
	dir := t.TempDir()
	data := filepath.Join(dir, "data")
	body := func(level string) string {
		return `{"version":1,"node":{"dataDir":"` + data + `"},"listeners":[{"name":"t","type":"tcp","address":"127.0.0.1:0"}],"log":{"format":"json","level":"` + level + `"}}`
	}
	doc := writeDoc(t, dir, "reload.json", body("warn"))
	reloads := make(chan os.Signal, 1)
	done := make(chan broker.ReloadResult, 1)
	prevReload, prevNotify, prevReloaded := notifyReload, notifyContext, reloaded
	notifyReload = func() (<-chan os.Signal, func()) { return reloads, func() {} }
	reloaded = func(r broker.ReloadResult) { done <- r }
	notifyContext = func(parent context.Context) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(parent)
		writeDoc(t, dir, "reload.json", body("info"))
		reloads <- syscall.SIGHUP
		go func() {
			select {
			case r := <-done:
				if !r.Applied {
					t.Errorf("the signal's reload: %+v", r)
				}
			case <-time.After(10 * time.Second):
				t.Error("the reload signal was not acted on within 10 s")
			}
			cancel()
		}()
		return ctx, cancel
	}
	defer func() { notifyReload, notifyContext, reloaded = prevReload, prevNotify, prevReloaded }()
	code, _, errOut := run(t, nil, "serve", "-config", doc)
	if code != ExitOK || !strings.Contains(errOut, `"configuration reloaded"`) || !strings.Contains(errOut, `"changed":["/log/level"]`) {
		t.Fatalf("exit %d err %q", code, errOut)
	}
	if strings.Contains(errOut, `"hoardmq serving"`) || !strings.Contains(errOut, `"shutting down"`) {
		t.Errorf("Info lines before the reload, or none after it: %q", errOut)
	}
}

func TestHashPassword(t *testing.T) {
	code, out, errOut := runWithStdin(t, "s3cret\n", nil, "hash-password")
	if code != ExitOK || !strings.HasPrefix(out, "$pbkdf2-sha256$") {
		t.Fatalf("exit %d out %q err %q", code, out, errOut)
	}
	if ok, err := auth.VerifyPassword(strings.TrimSpace(out), []byte("s3cret")); err != nil || !ok {
		t.Fatalf("hash does not verify: %v %v", ok, err)
	}
	if code, _, _ := runWithStdin(t, "\n", nil, "hash-password"); code != ExitUsage {
		t.Errorf("empty password exit %d", code)
	}
	if code, _, _ := runWithStdin(t, "x", nil, "hash-password", "onthecommandline"); code != ExitUsage {
		t.Errorf("argument exit %d", code)
	}
}

// -scram writes the SCRAM form, which serves password login and a SCRAM
// exchange; below the iteration floor is a usage error.
func TestHashPasswordSCRAM(t *testing.T) {
	code, out, errOut := runWithStdin(t, "pencil\n", nil, "hash-password", "-scram", "-iterations", "4096")
	if code != ExitOK || !strings.HasPrefix(out, "$scram-sha-256$i=4096$") {
		t.Fatalf("exit %d out %q err %q", code, out, errOut)
	}
	stored := strings.TrimSpace(out)
	if ok, err := auth.VerifyPassword(stored, []byte("pencil")); err != nil || !ok {
		t.Fatalf("password login: %v %v", ok, err)
	}
	cred, err := auth.ParseSCRAM(stored)
	if err != nil {
		t.Fatal(err)
	}
	srv := auth.NewSCRAMServer(func(u string) (auth.SCRAMCredential, bool) { return cred, u == "dev" })
	c := &scramtest.Client{User: "dev", Password: "pencil", Nonce: "abc"}
	ex := srv.Begin(context.Background(), auth.SCRAMMethod, &auth.Request{})
	first := ex.Step(c.First())
	if final := ex.Step(c.Final(first.Data)); final.Kind != auth.Succeed || !c.Verify(final.Data) {
		t.Fatalf("SCRAM with the printed credential: %+v", final)
	}
	if code, _, _ := runWithStdin(t, "pencil\n", nil, "hash-password", "-scram", "-iterations", "4095"); code != ExitUsage {
		t.Errorf("-iterations 4095: exit %d", code)
	}
	if code, out, _ := runWithStdin(t, "pencil\n", nil, "hash-password", "-scram"); code != ExitOK || !strings.HasPrefix(out, "$scram-sha-256$i=600000$") {
		t.Errorf("the default iterations: %d %q", code, out)
	}
}

// Design invariant 14: a code path that performs no work performs no writes.
// Every path below runs with the working directory, the configured data
// directory and the document path inside a fresh directory whose listing is
// compared before and after.
func TestNoWorkPathsWriteNothing(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	writeDoc(t, dir, "valid.json", `{"version":1,"node":{"dataDir":"`+filepath.Join(dir, "data")+`"}}`)
	writeDoc(t, dir, "invalid.json", `{"version":1,"node":{"dataDir":""},"nope":true}`)
	blocker, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Close() }()
	writeDoc(t, dir, "taken.json", `{"version":1,"node":{"dataDir":"`+filepath.Join(dir, "data")+`"},"listeners":[{"name":"t","type":"tcp","address":"`+blocker.Addr().String()+`"}]}`)
	// A cluster configuration: check reads the secret and validates; serve
	// binds the client listener, fails to bind the cluster listener on the
	// taken port, and must leave nothing behind.
	writeDoc(t, dir, "secret", "0123456789abcdef0123")
	writeDoc(t, dir, "cluster.json", `{"version":1,"node":{"name":"n1","dataDir":"`+filepath.Join(dir, "data")+`"},"listeners":[{"name":"t","type":"tcp","address":"127.0.0.1:0"}],"cluster":{"listen":"`+blocker.Addr().String()+`","auth":{"mode":"secret","secretFile":"`+filepath.Join(dir, "secret")+`"},"peers":[{"name":"n2","address":"127.0.0.1:1"}],"raft":{"joinTimeoutMs":1000}}}`)
	// An admin block: check reads its token file; serve fails on the taken
	// admin port before anything is created.
	writeDoc(t, dir, "admin.token", "0123456789abcdef0123")
	writeDoc(t, dir, "admin.json", `{"version":1,"node":{"dataDir":"`+filepath.Join(dir, "data")+`"},"listeners":[{"name":"t","type":"tcp","address":"127.0.0.1:0"}],"admin":{"address":"`+blocker.Addr().String()+`","tokens":[{"name":"ops","tokenFile":"`+filepath.Join(dir, "admin.token")+`","scopes":["admin"]}]}}`)
	writeDoc(t, dir, "badjwt.json", `{"version":1,"node":{"dataDir":"`+filepath.Join(dir, "data")+`"},"modules":{"auth.jwt":{"issuer":"i","audiences":["a"],"jwksUri":"http://idp.example/keys"}}}`)
	writeDoc(t, dir, "badldap.json", `{"version":1,"node":{"dataDir":"`+filepath.Join(dir, "data")+`"},"modules":{"auth.ldap":{"url":"ldap://dir.example","bindDn":"uid=${username},dc=x"}}}`)
	env := []string{"HOARDMQ_NODE_DATA_DIR=" + filepath.Join(dir, "envdata")}
	paths := [][]string{
		{},
		{"help"},
		{"-h"},
		{"version"},
		{"-nonsense"},
		{"bogus"},
		{"check"},
		{"check", "-h"},
		{"check", "-config", "absent.json"},
		{"check", "-config", "valid.json"},
		{"check", "-config", "invalid.json"},
		{"check", "-config", "."},
		{"serve"},
		{"serve", "-config", "invalid.json"},
		{"serve", "-config", "taken.json"},
		{"check", "-config", "cluster.json"},
		{"serve", "-config", "cluster.json"},
		{"check", "-config", "admin.json"},
		{"serve", "-config", "admin.json"},
		{"clients", "list", "-admin", "127.0.0.1:1", "-token-file", "admin.token"},
		{"clients", "bogus"},
		{"clients"},
		{"drain", "-admin", "127.0.0.1:1", "-token-file", "admin.token"},
		{"check", "-config", "badjwt.json"},
		{"hash-password", "-scram", "-iterations", "10"},
		{"hash-password", "-bogus"},
		{"hash-password", "-h"},
		{"login", "-issuer", "http://127.0.0.1:1", "-client-id", "c", "-token-file", "id.token", "-no-browser", "-timeout", "5s"},
		{"login", "-issuer", "http://127.0.0.1:1", "-client-id", "c", "-listen", "0.0.0.0:8250", "-token-file", "id.token"},
		{"login", "-token-file", "id.token"},
		{"whoami", "-admin", "127.0.0.1:1", "-token-file", "admin.token"},
		{"apikeys"},
		{"apikeys", "create", "-admin", "127.0.0.1:1", "-token-file", "admin.token"},
		{"apikeys", "revoke", "-admin", "127.0.0.1:1"},
		{"hash-password", "extra"},
		{"version", "extra"},
		{"credentials"},
		{"credentials", "add", "-admin", "127.0.0.1:1", "-token-file", "admin.token", "-password-file", "admin.token", "-hash-file", "admin.token", "alice"},
		{"credentials", "add", "-admin", "127.0.0.1:1", "-token-file", "admin.token", "alice"},
		{"check", "-config", "badldap.json"},
	}
	before := listing(t, dir)
	for _, args := range paths {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			run(t, env, args...)
			if after := listing(t, dir); after != before {
				t.Errorf("filesystem changed:\nbefore %s\nafter  %s", before, after)
			}
		})
	}
}

func listing(t *testing.T, dir string) string {
	t.Helper()
	var names []string
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		names = append(names, path+" "+info.ModTime().String()+" "+info.Mode().String())
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return strings.Join(names, "\n")
}

// check provisions modules read-only and reports their problems beside the
// schema's, in one run, creating nothing.
func TestCheckReportsModuleProblems(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	path := writeDoc(t, dir, "mods.json", `{"version":1,"node":{"dataDir":"data"},"modules":{"auth.file":{"path":"missing-users.json"},"nope.thing":{}}}`)
	code, _, errOut := run(t, nil, "check", "-config", path)
	if code != ExitProblems {
		t.Fatalf("exit %d err %q", code, errOut)
	}
	for _, want := range []string{": /modules/auth.file: ", ": /modules/nope.thing: unknown module"} {
		if !strings.Contains(errOut, want) {
			t.Errorf("missing %q in %q", want, errOut)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "data")); !os.IsNotExist(err) {
		t.Error("check created the data directory")
	}
	// Schema problems win: module validation runs only on a valid document.
	bad := writeDoc(t, dir, "bad.json", `{"version":2,"node":{"dataDir":"data"},"modules":{"nope.thing":{}}}`)
	_, _, errOut = run(t, nil, "check", "-config", bad)
	if !strings.Contains(errOut, "/version: must be 1") || strings.Contains(errOut, "nope.thing") {
		t.Errorf("got %q", errOut)
	}
}
