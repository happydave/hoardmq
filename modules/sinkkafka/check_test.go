package sinkkafka

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/happydave/hoardmq/internal/cli"
)

// TestCheckReportsAtTheModule runs hoardmq check on a sink.kafka
// configuration: a problem only the module can see is reported at
// /modules/sink.kafka, a good one passes, and neither connects to the
// brokers it names, which here is a loopback port nothing listens on.
func TestCheckReportsAtTheModule(t *testing.T) {
	dir := t.TempDir()
	secret := filepath.Join(dir, "secret")
	if err := os.WriteFile(secret, []byte("pw\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name, passwordFile string
		code               int
		want               string
	}{
		{"good", secret, cli.ExitOK, "ok\n"},
		{"bad", filepath.Join(dir, "absent"), cli.ExitProblems, "/modules/sink.kafka: sasl.passwordFile: open "},
	} {
		doc := `{"version":1,"node":{"dataDir":"` + filepath.Join(dir, "data-"+c.name) + `"},` +
			`"listeners":[{"name":"plain","type":"tcp","address":"127.0.0.1:1883"}],` +
			`"modules":{"sink.kafka":{"brokers":["127.0.0.1:9"],"routes":[{"filter":"a/#","topic":"t"}],` +
			`"sasl":{"mechanism":"plain","username":"u","passwordFile":"` + c.passwordFile + `"}}}}`
		path := filepath.Join(dir, c.name+".json")
		if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
			t.Fatal(err)
		}
		var out, errOut bytes.Buffer
		code := cli.Run([]string{"check", "-config", path}, nil, strings.NewReader(""), &out, &errOut)
		if code != c.code || !strings.Contains(out.String()+errOut.String(), c.want) {
			t.Errorf("%s: exit %d, out %q, err %q; want %d and %q", c.name, code, out.String(), errOut.String(), c.code, c.want)
		}
		if _, err := os.Stat(filepath.Join(dir, "data-"+c.name)); !os.IsNotExist(err) {
			t.Errorf("%s: check created the data directory", c.name)
		}
	}
}
