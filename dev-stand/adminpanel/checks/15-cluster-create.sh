#!/usr/bin/env bash
# E2E создания кластера: POST /api/clusters -> ключи в etcd -> список/детали
# (spec t12 §3.9). Идемпотентность чека: префиксы smoke-кластера чистятся перед прогоном.
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${ADMINPANEL_URL:-http://localhost:5050}"
JAR="$(mktemp)"; trap 'rm -f "$JAR"' EXIT

# Arrange: панель жива + логин (паттерн 10-smoke-api.sh)
for i in $(seq 1 60); do curl -fsS "$BASE/api/healthz" >/dev/null 2>&1 && break; sleep 1; done
curl -fsS "$BASE/api/healthz" >/dev/null || { echo "❌ панель не отвечает: $BASE"; exit 1; }
curl -fsS -c "$JAR" -o /dev/null -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  || { echo "❌ login admin/admin не прошёл"; exit 1; }

# Чистка прошлых прогонов: только свои ключи (префикс кластера + request_*).
ect() { docker compose exec -T etcd etcdctl --endpoints=http://localhost:2379 "$@"; }
ect del --prefix /clusters/smoke >/dev/null
for k in request_cpu request_mem request_disk; do
  ect del "/service/smoke-shard1/$k" >/dev/null
  ect del "/service/smoke-shard2/$k" >/dev/null
done

# Act: создание (4 бакета, 2 шарда, 2 реплики, 0.5 CPU / 8Gi / 100Gi на ноду)
code="$(curl -s -o /tmp/t12-create.json -w '%{http_code}' -b "$JAR" -X POST "$BASE/api/clusters" \
  -H 'Content-Type: application/json' \
  -d '{"name":"smoke","buckets":4,"shards":2,"replicas":2,"requestCpu":0.5,"requestMem":8,"requestDisk":100}')"
[ "$code" = 201 ] || { echo "❌ POST /api/clusters = $code: $(cat /tmp/t12-create.json)"; exit 1; }
echo "  создан: $(jq -c '{name,bucketsCount,shardsTotal,replicas,requestCpu,requestMem,requestDisk,state}' /tmp/t12-create.json)"

# Assert: ключи контракта в etcd (arch/02 §9.1)
[ "$(ect get /clusters/smoke/config --print-value-only | jq -r '.state')" = "NOT_INITIALIZED" ] \
  || { echo "❌ config.state != NOT_INITIALIZED"; exit 1; }
[ "$(ect get /clusters/smoke/shards/shard1/nodes/shard1b/state --print-value-only)" = "NOT_INITIALIZED" ] \
  || { echo "❌ нода shard1b не NOT_INITIALIZED"; exit 1; }
[ "$(ect get /service/smoke-shard2/request_mem --print-value-only)" = "8Gi" ] \
  || { echo "❌ /service/smoke-shard2/request_mem != 8Gi"; exit 1; }
# etcdctl get --prefix отдаёт значения в порядке ключей (bucket_0..3) — БЕЗ sort,
# чтобы проверить именно round-robin-раскладку: shard1 shard2 shard1 shard2
routing="$(ect get --prefix /clusters/smoke/buckets/routing --print-value-only | tr '\n' ' ')"
[ "$routing" = "shard1 shard2 shard1 shard2 " ] || { echo "❌ routing round-robin: $routing"; exit 1; }
echo "  etcd: config/nodes/request_*/routing — контракт §9.1 соблюдён"

# Assert: повтор — 409 (клэйм)
code="$(curl -s -o /dev/null -w '%{http_code}' -b "$JAR" -X POST "$BASE/api/clusters" \
  -H 'Content-Type: application/json' \
  -d '{"name":"smoke","buckets":4,"shards":2,"replicas":2,"requestCpu":0.5,"requestMem":8,"requestDisk":100}')"
[ "$code" = 409 ] || { echo "❌ повторное создание = $code, ожидался 409"; exit 1; }
echo "  повторное создание -> 409"

# Assert: панель видит кластер (следующий тик ≤ 3 c + polling)
for i in $(seq 1 15); do
  curl -fsS -b "$JAR" "$BASE/api/clusters" | jq -e 'any(.[]; .name=="smoke" and .notInitialized)' >/dev/null && break
  sleep 1
done
curl -fsS -b "$JAR" "$BASE/api/clusters" | jq -e 'any(.[]; .name=="smoke" and .notInitialized)' >/dev/null \
  || { echo "❌ /api/clusters не видит smoke (notInitialized)"; exit 1; }
curl -fsS -b "$JAR" "$BASE/api/clusters/smoke" | jq -e \
  '.state=="NOT_INITIALIZED" and .shards[0].requests.cpu=="0.5" and (.shards[0].nodes|length)==2' >/dev/null \
  || { echo "❌ /api/clusters/smoke: state/requests/nodes"; exit 1; }
echo "  /api/clusters/smoke: NOT_INITIALIZED, заявки и ноды видны"
echo "✓ 15-cluster-create: создание кластера e2e прошло"
