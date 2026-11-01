#!/usr/bin/env bash
# Starts NATS for the integration tests: a test CA, a server certificate for localhost only (so 127.0.0.1 fails the
# host name check), a client certificate, an NKey user, and four servers (plain, authenticated, TLS, mutual TLS).
set -euo pipefail
cd "$(dirname "$0")"

certs=.certs
if [ ! -f "$certs/ca.pem" ]; then
    mkdir -p "$certs"
    openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -subj "/CN=Kipchak Test CA" \
        -keyout "$certs/ca-key.pem" -out "$certs/ca.pem" 2>/dev/null
    openssl req -newkey rsa:2048 -nodes -subj "/CN=localhost" \
        -keyout "$certs/server-key.pem" -out "$certs/server.csr" 2>/dev/null
    printf 'subjectAltName=DNS:localhost\n' > "$certs/san.ext"
    openssl x509 -req -in "$certs/server.csr" -CA "$certs/ca.pem" -CAkey "$certs/ca-key.pem" -CAcreateserial \
        -days 3650 -extfile "$certs/san.ext" -out "$certs/server.pem" 2>/dev/null
    openssl req -newkey rsa:2048 -nodes -subj "/CN=kipchak-client" \
        -keyout "$certs/client-key.pem" -out "$certs/client.csr" 2>/dev/null
    printf 'extendedKeyUsage=clientAuth\n' > "$certs/client.ext"
    openssl x509 -req -in "$certs/client.csr" -CA "$certs/ca.pem" -CAkey "$certs/ca-key.pem" -CAcreateserial \
        -days 3650 -extfile "$certs/client.ext" -out "$certs/client.pem" 2>/dev/null
    # A CA that did not sign the server's certificate, for the verification tests.
    openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -subj "/CN=Other CA" \
        -keyout "$certs/other-ca-key.pem" -out "$certs/other-ca.pem" 2>/dev/null
    docker run --rm natsio/nats-box:latest nk -gen user -pubout > "$certs/nkey.txt"
    chmod 644 "$certs"/*
fi

nkey_public=$(sed -n 2p "$certs/nkey.txt")
sed "s/NKEY_PUBLIC/$nkey_public/" nats/auth.conf.template > "$certs/auth.conf"

docker compose up -d --force-recreate --wait 2>/dev/null || docker compose up -d --force-recreate

for port in 54222 54223 54224 54225; do
    for _ in $(seq 1 30); do
        if nc -z 127.0.0.1 "$port" 2>/dev/null; then
            break
        fi
        sleep 1
    done
done
echo "NATS is ready."
