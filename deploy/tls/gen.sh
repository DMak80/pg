#!/usr/bin/env bash
# Per-install API TLS-пакет (t03, arch/14 §1.1 / arch/16 §1.1 / arch/21 §1.1):
# ЕДИНАЯ CA kfw-install-ca на воркеров. Серверные серты: server (kafkaworker +
# valkeyworker — один серт, SAN покрывает обоих), pgserver (pgworker); клиентские:
# panel, seed, prometheus, healthcheck. Идемпотентен: при существующем ca.pem не
# делает ничего, КРОМЕ перегенерации server-серта старых пакетов без DNS:valkeyworker
# (t03). Ротация CA — вручную: rm ca.* и перезапуск. Файлы в git не попадают
# (deploy/tls/.gitignore).
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"
SERVER_SAN="DNS:kafkaworker,DNS:valkeyworker,DNS:localhost,DNS:host.docker.internal,IP:127.0.0.1"
DAYS=3650

if [ ! -f ca.pem ]; then
  openssl genrsa -out ca.key 4096 2>/dev/null
  openssl req -x509 -new -nodes -key ca.key -sha256 -days "$DAYS" \
    -subj "/CN=kfw-install-ca" -out ca.pem
fi

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

# t03: старый server-серт без DNS:valkeyworker — перегенерируем (CA жив).
if [ ! -f server.crt ] || ! openssl x509 -in server.crt -noout -text 2>/dev/null | grep -q 'DNS:valkeyworker'; then
  issue server kafkaworker serverAuth "$SERVER_SAN"
fi

if [ ! -f pgserver.crt ]; then
  # серверный pgworker (SAN покрывает compose-DNS, localhost, host-gateway — R13)
  issue pgserver pgworker serverAuth "DNS:pgworker,DNS:localhost,DNS:host.docker.internal,IP:127.0.0.1"
fi

# клиентские (различимость в журналах сервера, независимый отзыв)
[ -f panel.crt ]      || issue panel      panel      clientAuth ""
[ -f seed.crt ]       || issue seed       seed       clientAuth ""
[ -f prometheus.crt ] || issue prometheus prometheus clientAuth ""
[ -f healthcheck.crt ] || issue healthcheck healthcheck clientAuth ""
chmod 600 ca.key ./*.key
echo "✓ TLS-пакет kfw-install-ca: ca.pem, server.* (kafkaworker+valkeyworker), pgserver.*, panel.*, seed.*, prometheus.*, healthcheck.*"
