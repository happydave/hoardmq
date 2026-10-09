#!/bin/sh
# Runs the Eclipse Paho interoperability suites against a node listening on
# localhost:1883 (the 3.1.1 suite accepts no other address). The node needs
# anonymous access, an ACL that refuses subscribe on test/nosubscribe and
# limits.maxKeepAlive 60; conformance/external/paho_test.go starts one
# with exactly that when HOARDMQ_EXTERNAL=1 is set, which is the usual way to
# run this. Usage: run.sh <path to paho.mqtt.testing>
set -eu
suite=${1:?path to a paho.mqtt.testing checkout}
cd "$suite/interoperability"
python3 client_test5.py -p 1883
python3 client_test.py
