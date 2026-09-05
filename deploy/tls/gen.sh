#!/usr/bin/env bash
# Per-install API TLS-пакет (t03, arch/14 §1.1 / arch/16 §1.1, решение О1):
# ЕДИНАЯ CA kfw-install-ca на оба воркера. Серверные серты: server (kafkaworker),
# pgserver (pgworker); клиентские: panel (мутации панели), seed (стендовый сид),
# prometheus (scrape), healthcheck (docker HEALTHCHECK). Идемпотентен: при
# существующем ca.pem не делает ничего (ротация — вручную: rm ca.* и перезапуск).
# Выпущенные файлы в git не попадают (deploy/tls/.gitignore).
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"
if [ -f ca.pem ]; then echo "TLS-пакет уже есть (ca.pem); ротация — rm ca.* и перезапуск"; exit 0; fi
DAYS=3650

openssl genrsa -out ca.key 4096 2>/dev/null
openssl req -x509 -new -nodes -key ca.key -sha256 -days "$DAYS" \
  -subj "/CN=kfw-install-ca" -out ca.pem

issue() { # name cn eku san
  local name="$1" cn="$2" eku="$3" san="$4"
  openssl genrsa -out "$name.key" 2048 2>/dev/null
  openssl req -new -key "$name.key" -subj "/CN=$cn" -out "$name.csr"
  local ext="basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=$eku"
  [ -n "$san" ] && ext="$ext
subjectAltName=$san"
  openssl x509 -req -in "$name.csr" -CA ca.pem -CAkey ca.key -CAcreateserial \
    -days "$DAYS" -sha256 -out "$name.crt" 2>/dev/null \
    -extfile <(printf '%s\n' "$ext")
  rm -f "$name.csr"
}

# серверные (SAN покрывает compose-DNS, localhost, host-gateway — R13)
issue server     kafkaworker serverAuth "DNS:kafkaworker,DNS:localhost,DNS:host.docker.internal,IP:127.0.0.1"
issue pgserver   pgworker    serverAuth "DNS:pgworker,DNS:localhost,DNS:host.docker.internal,IP:127.0.0.1"
# клиентские (различимость в журналах сервера, независимый отзыв)
issue panel      panel       clientAuth ""
issue seed       seed        clientAuth ""
issue prometheus prometheus  clientAuth ""
issue healthcheck healthcheck clientAuth ""
chmod 600 ca.key ./*.key
echo "✓ TLS-пакет kfw-install-ca: ca.pem, server.*, pgserver.*, panel.*, seed.*, prometheus.*, healthcheck.*"
