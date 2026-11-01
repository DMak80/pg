#!/bin/sh
# Декларативное создание кластера ЧЕРЕЗ API PgWorker (arch/14 §1.1; задача
# etcd-via-worker-api): прежний etcdctl-сид стал тонкой curl-обёрткой над
# POST /api/clusters — единственный писатель деклараций теперь воркер.
# Создаёт: config NOT_INITIALIZED + S=2 шарда × replicas=2 + routing/status
# всех N=6 бакетов + заявки request_* (Patroni DCS). После сида PgWorker
# поднимет кластер. Креды bucket_admin воркер берёт из env-fallback
# (PGW_BUCKET_ADMIN_PASSWORD, deploy/.env) — прежние аргументы 3/4 упразднены.
#
# Использование: ./seed.sh [api-url] [кластер]
#   ./seed.sh                                   # shop на https://localhost:8080 (mTLS, seed-серт)
#   ./seed.sh https://localhost:8080 shop2      # ещё один кластер
set -e

# Мутации /api — с клиентским сертом seed (конвенция «чек = стендовый сид»);
# /healthz и /metrics — открытая грань (https без клиентского серта).
TLS_DIR="$(cd "$(dirname "$0")/../deploy/tls" && pwd)"
# Хост-порт публикации pgworker (если URL не передан): env → deploy/.env (00-up) → 8080.
PGW_API_HOST_PORT="${PGW_API_HOST_PORT:-$(awk -F= '$1=="PGW_API_HOST_PORT"{print $2}' "$TLS_DIR/../.env" 2>/dev/null)}"
API="${1:-https://localhost:${PGW_API_HOST_PORT:-8080}}"
C="${2:-shop}"
N=6
SHARDS=2
REPLICAS=2
CPU=2
MEM=2    # Gi
DISK=20  # Gi — в декларативном контракте обязателен (прежде не записывали)

body=$(printf '{"name":"%s","buckets":%d,"shards":%d,"replicas":%d,"requestCpu":%d,"requestMem":%d,"requestDisk":%d}' \
  "$C" "$N" "$SHARDS" "$REPLICAS" "$CPU" "$MEM" "$DISK")
out="$(mktemp)"; trap 'rm -f "$out"' EXIT
code="$(curl -s -o "$out" -w '%{http_code}' --cacert "$TLS_DIR/ca.pem" --cert "$TLS_DIR/seed.crt" --key "$TLS_DIR/seed.key" \
  -X POST "$API/api/clusters" -H 'Content-Type: application/json' -d "$body")"
if [ "$code" = 201 ]; then
  echo "cluster $C declared via API ($N buckets, $SHARDS shards × $REPLICAS replicas)"
elif [ "$code" = 409 ]; then
  echo "cluster $C already declared (claim taken) — skipping"
else
  echo "❌ POST $API/api/clusters = $code: $(cat "$out")" >&2
  exit 1
fi
