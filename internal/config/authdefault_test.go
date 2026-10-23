package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/happydave/hoardmq/internal/testcert"
)

// TestClusterAuthModeDefaultsToMTLS: a cluster that needs peer
// authentication (peers, or DNS discovery) and names no mode is read as
// mtls, so it asks for the certificate files rather than the mode, and starts
// with them. A node with no peers keeps no mode.
func TestClusterAuthModeDefaultsToMTLS(t *testing.T) {
	dir := t.TempDir()
	ca, err := testcert.NewCA("cluster-ca")
	if err != nil {
		t.Fatal(err)
	}
	leaf, _ := ca.Issue("n1", 2)
	certFile, keyFile, _ := leaf.Write(dir, "n1")
	caFile, _ := ca.WriteCA(dir)
	tlsMember := `"tls":{"certFile":"` + certFile + `","keyFile":"` + keyFile + `","caFile":"` + caFile + `"}`
	peers := `"peers":[{"name":"n2","address":"h2:7000"}]`

	_, problems := Parse([]byte(clusterDoc(t, `{"listen":"127.0.0.1:7000",`+peers+`}`)), nil)
	got := lines(problems)
	if !strings.Contains(got, "/cluster/auth/tls: required for mtls") || strings.Contains(got, "/cluster/auth/mode") {
		t.Errorf("peers with no mode and no certificates: want the certificates asked for as mtls, not the mode; got\n%s", got)
	}

	doc, problems := Parse([]byte(clusterDoc(t, `{"listen":"127.0.0.1:7000","auth":{`+tlsMember+`},`+peers+`}`)), nil)
	if len(problems) != 0 || doc.Cluster.Auth.Mode != "mtls" {
		t.Errorf("peers with certificates and no mode: mode %q, problems\n%s", doc.Cluster.Auth.Mode, lines(problems))
	}

	dns := `{"version":1,"node":{"name":"hoardmq-1","dataDir":"d"},"cluster":{"listen":"0.0.0.0:7000","auth":{` + tlsMember + `},"discovery":{"mode":"dns","dns":{"service":"hoardmq-headless","name":"hoardmq","replicas":3,"port":7000}}}}`
	if doc, problems := Parse([]byte(dns), nil); len(problems) != 0 || doc.Cluster.Auth.Mode != "mtls" {
		t.Errorf("dns discovery with certificates and no mode: mode %q, problems\n%s", doc.Cluster.Auth.Mode, lines(problems))
	}

	for _, single := range []string{`{}`, `{"listen":"127.0.0.1:7000"}`} {
		doc, problems := Parse([]byte(clusterDoc(t, single)), nil)
		if len(problems) != 0 || doc.Cluster.Auth.Mode != "" {
			t.Errorf("no peers (%s): mode %q, problems\n%s", single, doc.Cluster.Auth.Mode, lines(problems))
		}
	}

	secret := filepath.Join(dir, "secret")
	_ = os.WriteFile(secret, []byte("a-shared-secret-of-enough-bytes"), 0o600)
	if doc, problems := Parse([]byte(clusterDoc(t, `{"listen":"127.0.0.1:7000","auth":{"mode":"secret","secretFile":"`+secret+`"},`+peers+`}`)), nil); len(problems) != 0 || doc.Cluster.Auth.Mode != "secret" {
		t.Errorf("an explicit secret mode: mode %q, problems\n%s", doc.Cluster.Auth.Mode, lines(problems))
	}
}
