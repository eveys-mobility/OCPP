#!/usr/bin/env bash
# Generate a self-signed cert for the local Envoy dev listener.
#
# The cert is **dev-only** — committed certs would defeat the
# `secret=True` discipline we use everywhere else, and a self-
# signed root has no real trust anchor anyway. `.gitignore` keeps
# the output out of the repo; rerun this script after a
# `make distclean` or on a fresh clone.
#
# Usage:
#   scripts/gen-dev-certs.sh [days]
#
# The cert is issued for `localhost`, `127.0.0.1`, `::1`, and
# `gateway.local` so a charger sim pointed at any of these
# verifies (or, more honestly, dies the right way — the sim
# needs `--insecure` regardless because nothing trusts our self-
# signed root).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CERT_DIR="$REPO_ROOT/deploy/envoy/certs"
DAYS="${1:-365}"

mkdir -p "$CERT_DIR"

cat > "$CERT_DIR/openssl.cnf" <<'EOF'
[req]
distinguished_name = req_distinguished_name
x509_extensions = v3_req
prompt = no

[req_distinguished_name]
CN = localhost
O = eveys-ocpp dev

[v3_req]
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = gateway.local
IP.1 = 127.0.0.1
IP.2 = ::1
EOF

openssl req -x509 -nodes \
    -newkey rsa:2048 \
    -keyout "$CERT_DIR/server.key" \
    -out "$CERT_DIR/server.crt" \
    -days "$DAYS" \
    -config "$CERT_DIR/openssl.cnf" \
    >/dev/null 2>&1

# openssl.cnf is a build artifact, not config worth keeping.
rm "$CERT_DIR/openssl.cnf"

echo "wrote: $CERT_DIR/server.crt"
echo "wrote: $CERT_DIR/server.key"
echo
echo "Validity: $DAYS days. SANs: localhost, gateway.local, 127.0.0.1, ::1."
echo
echo "These certs are dev-only. Production certs come from cert-manager"
echo "or a vault CSI driver — see deploy/envoy/README.md."
