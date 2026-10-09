#!/bin/sh
# Regenerates these fixtures with openssl (3.0.13 made the committed ones): a
# CRL whose critical issuing distribution point names one partition and
# user certificates only, so the broker's reading of the extension is checked
# against another implementation. Run in an empty directory; copy the files
# that are not keys here.
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
crl_extensions = crl_idp
[ anything ]
commonName = supplied
[ crl_idp ]
issuingDistributionPoint = critical, @idp
[ idp ]
fullname = URI:http://crl.example/part1.crl
onlyuser = TRUE
[ v3_ca ]
basicConstraints = critical,CA:true
keyUsage = critical,keyCertSign,cRLSign,digitalSignature
[ v3_part1 ]
basicConstraints = CA:false
keyUsage = critical,digitalSignature
extendedKeyUsage = clientAuth
crlDistributionPoints = URI:http://crl.example/part1.crl
[ v3_part2 ]
basicConstraints = CA:false
keyUsage = critical,digitalSignature
extendedKeyUsage = clientAuth
crlDistributionPoints = URI:http://crl.example/part2.crl
CNF
touch index.txt
echo 1000 > crlnumber
openssl ecparam -name prime256v1 -genkey -noout -out ca.key
openssl req -new -x509 -key ca.key -out ca.pem -days 3650 -subj "/CN=openssl-idp-ca" -config ca.cnf -extensions v3_ca
for n in leaf-listed:10:v3_part1 leaf-unlisted:11:v3_part1 leaf-elsewhere:12:v3_part2; do
  name=${n%%:*}; rest=${n#*:}; ser=${rest%%:*}; ext=${rest#*:}
  openssl ecparam -name prime256v1 -genkey -noout -out "$name.key"
  openssl req -new -key "$name.key" -out "$name.csr" -subj "/CN=$name"
  echo "$ser" > serial
  openssl ca -batch -config ca.cnf -in "$name.csr" -out "$name.pem" -days 3650 -extensions "$ext" -notext
done
openssl ca -batch -config ca.cnf -revoke leaf-listed.pem
openssl ca -batch -config ca.cnf -revoke leaf-elsewhere.pem
openssl ca -batch -config ca.cnf -gencrl -out crl.pem
openssl crl -in crl.pem -outform DER -out crl-idp.der
