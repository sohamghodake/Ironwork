#!/usr/bin/env bash
# Generates the cluster CA and a shared service certificate covering every
# component hostname in the compose network. All service-to-service gRPC is
# mutual TLS — nothing connects until this has run (`make certs`).
set -euo pipefail

cd "$(dirname "$0")/.."
mkdir -p certs

# mkcert creates its local CA on first use. We never run `mkcert -install`:
# the CA reaches services as a mounted file, not via system trust stores.
mkcert -client \
  -cert-file certs/service.pem \
  -key-file certs/service-key.pem \
  gateway observer statemanager \
  scheduler-1 scheduler-2 scheduler-3 \
  worker-1 worker-2 \
  localhost 127.0.0.1 ::1

cp "$(mkcert -CAROOT)/rootCA.pem" certs/ca.pem

echo "mTLS material written to ./certs (gitignored)"
