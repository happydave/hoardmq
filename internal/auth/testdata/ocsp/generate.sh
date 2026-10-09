#!/bin/sh
# Regenerates these fixtures with openssl (3.0.13 made the committed ones), so
# the broker's OCSP and CRL readers are checked against another
# implementation. Run in an empty directory; copy the files that are not keys
# here.
set -e
cat > ca.cnf <<'CNF'
[ ca ]
default_ca = testca
[ testca ]
dir = .
database = index.txt
new_certs_dir = .
certificate = ca.pem
private_key = ca.key
serial = serial
crlnumber = crlnumber
default_md = sha256
default_crl_days = 3650
policy = anything
[ anything ]
commonName = supplied
[ v3_ca ]
basicConstraints = critical,CA:true
keyUsage = critical,keyCertSign,cRLSign,digitalSignature
subjectKeyIdentifier = hash
[ v3_leaf ]
basicConstraints = CA:false
keyUsage = critical,digitalSignature
extendedKeyUsage = clientAuth
[ v3_ocsp ]
basicConstraints = CA:false
keyUsage = critical,digitalSignature
extendedKeyUsage = OCSPSigning
CNF
touch index.txt
echo 1000 > crlnumber
openssl ecparam -name prime256v1 -genkey -noout -out ca.key
openssl req -new -x509 -key ca.key -out ca.pem -days 3650 -subj "/CN=openssl-test-ca" -config ca.cnf -extensions v3_ca
for n in leaf-good:10 leaf-revoked:11 ocsp:20; do
  name=${n%%:*}; ser=${n##*:}; ext=v3_leaf; [ "$name" = ocsp ] && ext=v3_ocsp
  openssl ecparam -name prime256v1 -genkey -noout -out "$name.key"
  openssl req -new -key "$name.key" -out "$name.csr" -subj "/CN=$name"
  echo "$ser" > serial
  openssl ca -batch -config ca.cnf -in "$name.csr" -out "$name.pem" -days 3650 -extensions "$ext" -notext
done
openssl ca -batch -config ca.cnf -revoke leaf-revoked.pem
openssl ca -batch -config ca.cnf -gencrl -out crl.pem
openssl crl -in crl.pem -outform DER -out crl.der
for l in good revoked; do
  openssl ocsp -issuer ca.pem -cert "leaf-$l.pem" -no_nonce -reqout "req-$l.der"
  openssl ocsp -index index.txt -CA ca.pem -rsigner ca.pem -rkey ca.key -reqin "req-$l.der" -respout "resp-$l-ca.der" -ndays 3650
  openssl ocsp -index index.txt -CA ca.pem -rsigner ocsp.pem -rkey ocsp.key -reqin "req-$l.der" -respout "resp-$l-delegated.der" -ndays 3650
done
