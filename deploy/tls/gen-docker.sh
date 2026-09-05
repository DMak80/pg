#!/usr/bin/env bash
# Per-install docker-CA (t03, arch/14 §2.2.1): изолированное от API-пакеты
# доверие docker-хостов. Выпускает: docker-ca.pem/key (CN=pgw-docker-ca),
# клиентскую пару воркера pgworker-docker.* (PGW_DOCKER_TLS_{CERT,KEY}),
# серверный серт демона docker-server.* (SAN по первому аргументу; демоны
# поднимаются с --tlsverify). Идемпотентен по docker-ca.pem.
# Использование: bash gen-docker.sh <host-dns|ip> [доп. SAN через запятую: DNS:x,IP:y]
set -euo pipefail
[ $# -ge 1 ] || { echo "usage: gen-docker.sh <host-dns|ip> [extra-san]"; exit 1; }
HOST="$1"; EXTRA="${2:-}"
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"
DAYS=3650
[ -f docker-ca.pem ] || {
  openssl genrsa -out docker-ca.key 4096 2>/dev/null
  openssl req -x509 -new -nodes -key docker-ca.key -sha256 -days "$DAYS" \
    -subj "/CN=pgw-docker-ca" -out docker-ca.pem
}
SAN="DNS:${HOST},IP:127.0.0.1"
case "$HOST" in *:*) SAN="IP:${HOST},IP:127.0.0.1" ;; esac
[ -n "$EXTRA" ] && SAN="${SAN},${EXTRA}"

issue() { # name cn eku san
  local name="$1" cn="$2" eku="$3" san="$4"
  openssl genrsa -out "$name.key" 2048 2>/dev/null
  openssl req -new -key "$name.key" -subj "/CN=$cn" -out "$name.csr"
  openssl x509 -req -in "$name.csr" -CA docker-ca.pem -CAkey docker-ca.key -CAcreateserial \
    -days "$DAYS" -sha256 -out "$name.crt" 2>/dev/null \
    -extfile <(printf 'basicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=%s\nsubjectAltName=%s\n' "$eku" "$san")
  rm -f "$name.csr"
}
issue pgworker-docker pgworker  clientAuth "DNS:pgworker"
issue docker-server    "$HOST"   serverAuth "$SAN"
chmod 600 docker-ca.key ./*.key 2>/dev/null || true
echo "✓ docker-пакет pgw-docker-ca: docker-ca.pem, pgworker-docker.*, docker-server.* (SAN: $SAN)"
echo "  демон: dockerd --tlsverify --tlscacert=docker-ca.pem --tlscert=docker-server.crt --tlskey=docker-server.key"
echo "  воркер: PGW_DOCKER_TLS_CA_PATH=docker-ca.pem PGW_DOCKER_TLS_{CERT,KEY}_PATH=pgworker-docker.{crt,key}"
