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
#   ./seed.sh                                   # shop на localhost:8080
#   ./seed.sh http://localhost:8080 shop2       # ещё один кластер
set -e

API="${1:-http://localhost:8080}"
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
code="$(curl -s -o "$out" -w '%{http_code}' -X POST "$API/api/clusters" \
  -H 'Content-Type: application/json' -d "$body")"
if [ "$code" = 201 ]; then
  echo "кластер $C задекларирован через API ($N бакетов, $SHARDS шарда × $REPLICAS реплик)"
elif [ "$code" = 409 ]; then
  echo "кластер $C уже задекларирован (клэйм занят) — пропускаю"
else
  echo "❌ POST $API/api/clusters = $code: $(cat "$out")" >&2
  exit 1
fi
