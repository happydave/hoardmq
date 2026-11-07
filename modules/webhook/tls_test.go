package webhook

import (
	"crypto/tls"
	"crypto/x509"

	"github.com/happydave/hoardmq/internal/testcert"
)

func tlsPair(l *testcert.Leaf) (tls.Certificate, error) { return tls.X509KeyPair(l.CertPEM, l.KeyPEM) }

// serverTLS serves pair and requires a client certificate ca issued.
func serverTLS(pair tls.Certificate, ca *testcert.CA) *tls.Config {
	pool := x509.NewCertPool()
	pool.AddCert(ca.Cert)
	return &tls.Config{Certificates: []tls.Certificate{pair}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool, MinVersion: tls.VersionTLS12}
}
