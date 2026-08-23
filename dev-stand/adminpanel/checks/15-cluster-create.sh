#!/usr/bin/env bash
# E2E создания и удаления кластера: POST /api/clusters -> ключи в etcd ->
# список/детали (spec t12 §3.9); DELETE -> config.state=DELETING (arch/02 §9.4).
# Идемпотентность чека: префиксы кластеров чистятся перед прогоном.
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

# Assert: smoke-тело без sharded — заодно регрессия обратной совместимости:
# отсутствующее поле трактуется как sharded=true (arch/02 §9.3)
jq -e '.sharded == true' /tmp/t12-create.json >/dev/null \
  || { echo "❌ ответ без поля sharded не вернул sharded=true"; exit 1; }

# Assert: ключи контракта в etcd (arch/02 §9.1)
[ "$(ect get /clusters/smoke/config --print-value-only | jq -r '.state')" = "NOT_INITIALIZED" ] \
  || { echo "❌ config.state != NOT_INITIALIZED"; exit 1; }
[ "$(ect get /clusters/smoke/shards/shard1/nodes/shard1b/state --print-value-only)" = "NOT_INITIALIZED" ] \
  || { echo "❌ нода shard1b не NOT_INITIALIZED"; exit 1; }
[ "$(ect get /service/smoke-shard2/request_mem --print-value-only)" = "8Gi" ] \
  || { echo "❌ /service/smoke-shard2/request_mem != 8Gi"; exit 1; }
# etcdctl get --prefix отдаёт значения в порядке ключей (bucket_0..3) — БЕЗ sort:
# блочное распределение (arch/02 §9.1.1) — 4×2: бакеты 0,1→shard1; 2,3→shard2
routing="$(ect get --prefix /clusters/smoke/buckets/routing --print-value-only | tr '\n' ' ')"
[ "$routing" = "shard1 shard1 shard2 shard2 " ] || { echo "❌ routing blocks 4×2: $routing"; exit 1; }
echo "  etcd: config/nodes/request_*/routing — контракт §9.1.1 (блоки) соблюдён"

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
  '.sharded == true and .state=="NOT_INITIALIZED" and .shards[0].requests.cpu=="0.5" and (.shards[0].nodes|length)==2' >/dev/null \
  || { echo "❌ /api/clusters/smoke: sharded/state/requests/nodes"; exit 1; }
echo "  /api/clusters/smoke: NOT_INITIALIZED, заявки и ноды видны"

# --- Кейс нешардированной (spec cluster-sharded-toggle §3.6): sharded=false, без buckets/shards ---
ect del --prefix /clusters/solo >/dev/null
for k in request_cpu request_mem request_disk; do
  ect del "/service/solo-shard1/$k" >/dev/null
done

code="$(curl -s -o /tmp/t12-create-solo.json -w '%{http_code}' -b "$JAR" -X POST "$BASE/api/clusters" \
  -H 'Content-Type: application/json' \
  -d '{"name":"solo","sharded":false,"replicas":2,"requestCpu":0.5,"requestMem":8,"requestDisk":100}')"
[ "$code" = 201 ] || { echo "❌ POST /api/clusters (solo, sharded=false) = $code: $(cat /tmp/t12-create-solo.json)"; exit 1; }
echo "  создан solo: $(jq -c '{name,sharded,bucketsCount,shardsTotal,state}' /tmp/t12-create-solo.json)"

# Вырожденная структура 1×1 (arch/02 §9.1): один бакет, один шард, заявки только shard1
[ "$(ect get /clusters/solo/config --print-value-only | jq -r '.buckets')" = "1" ] \
  || { echo "❌ solo config.buckets != 1"; exit 1; }
[ "$(ect get /clusters/solo/buckets/routing/bucket_0 --print-value-only)" = "shard1" ] \
  || { echo "❌ solo routing bucket_0 != shard1"; exit 1; }
[ -z "$(ect get /clusters/solo/buckets/routing/bucket_1 --print-value-only)" ] \
  || { echo "❌ solo: появился лишний bucket_1"; exit 1; }
[ -z "$(ect get /service/solo-shard2/request_cpu --print-value-only)" ] \
  || { echo "❌ solo: появились заявки shard2"; exit 1; }
jq -e '.sharded == false and .bucketsCount == 1 and .shardsTotal == 1' /tmp/t12-create-solo.json >/dev/null \
  || { echo "❌ solo-ответ не вырожденный (sharded/bucketsCount/shardsTotal)"; exit 1; }
echo "  etcd solo: вырожденная структура 1x1 — контракт §9.1 соблюдён"

# Панель видит solo (следующий тик ≤ 3 c + polling): 1 бакет, 1 шард
for i in $(seq 1 15); do
  curl -fsS -b "$JAR" "$BASE/api/clusters/solo" | jq -e \
    '.sharded == false and .state=="NOT_INITIALIZED" and .bucketsCount==1 and (.shards|length)==1' >/dev/null && break
  sleep 1
done
curl -fsS -b "$JAR" "$BASE/api/clusters/solo" | jq -e \
  '.sharded == false and .state=="NOT_INITIALIZED" and .bucketsCount==1 and (.shards|length)==1' >/dev/null \
  || { echo "❌ /api/clusters/solo: sharded=false / вырожденная структура не видна"; exit 1; }
echo "  /api/clusters/solo: 1 бакет × 1 шард, NOT_INITIALIZED"

# --- Кейс канона (spec bucket-block-distribution §2.1): 10×3 → 3+4+3 ---
ect del --prefix /clusters/canon10 >/dev/null
for s in 1 2 3; do
  for k in request_cpu request_mem request_disk; do
    ect del "/service/canon10-shard$s/$k" >/dev/null
  done
done

code="$(curl -s -o /tmp/t15-canon10.json -w '%{http_code}' -b "$JAR" -X POST "$BASE/api/clusters" \
  -H 'Content-Type: application/json' \
  -d '{"name":"canon10","buckets":10,"shards":3,"replicas":2,"requestCpu":0.5,"requestMem":8,"requestDisk":100}')"
[ "$code" = 201 ] || { echo "❌ POST /api/clusters (canon10) = $code: $(cat /tmp/t15-canon10.json)"; exit 1; }

# Значения в порядке bucket_0..9: блоки 3+4+3 — остаток среднему шарду (§9.1.1)
routing="$(ect get --prefix /clusters/canon10/buckets/routing --print-value-only | tr '\n' ' ')"
[ "$routing" = "shard1 shard1 shard1 shard2 shard2 shard2 shard2 shard3 shard3 shard3 " ] \
  || { echo "❌ canon10 routing 3+4+3: $routing"; exit 1; }
echo "  canon10: 10×3 → 3+4+3 — канон §9.1.1 соблюдён"

# --- Кейс удаления (arch/02 §9.4): DELETE → DELETING, ключи не тронуты ---
ect del --prefix /clusters/delme >/dev/null
for k in request_cpu request_mem request_disk; do
  ect del "/service/delme-shard1/$k" >/dev/null
done

code="$(curl -s -o /tmp/t15-del.json -w '%{http_code}' -b "$JAR" -X POST "$BASE/api/clusters" \
  -H 'Content-Type: application/json' \
  -d '{"name":"delme","sharded":false,"replicas":1,"requestCpu":0.5,"requestMem":4,"requestDisk":10}')"
[ "$code" = 201 ] || { echo "❌ POST /api/clusters (delme) = $code: $(cat /tmp/t15-del.json)"; exit 1; }

code="$(curl -s -o /dev/null -w '%{http_code}' -b "$JAR" -X DELETE "$BASE/api/clusters/delme")"
[ "$code" = 204 ] || { echo "❌ DELETE /api/clusters/delme = $code, ожидался 204"; exit 1; }

# config перезаписан: state=DELETING, константы сохранены; ключи кластера НЕ удалены
cfg="$(ect get /clusters/delme/config --print-value-only)"
[ "$(echo "$cfg" | jq -r '.state')" = "DELETING" ] || { echo "❌ delme config.state != DELETING: $cfg"; exit 1; }
[ "$(echo "$cfg" | jq -r '.buckets')" = "1" ] || { echo "❌ delme config.buckets != 1 (константы потеряны): $cfg"; exit 1; }
[ -n "$(ect get /clusters/delme/buckets/routing/bucket_0 --print-value-only)" ] \
  || { echo "❌ delme: ключи кластера удалены (панель их не трогает, §9.4)"; exit 1; }

# Идемпотентность (повтор — тоже 204), неизвестное имя — 404, имя занято — 409
code="$(curl -s -o /dev/null -w '%{http_code}' -b "$JAR" -X DELETE "$BASE/api/clusters/delme")"
[ "$code" = 204 ] || { echo "❌ повторный DELETE = $code, ожидался 204"; exit 1; }
code="$(curl -s -o /dev/null -w '%{http_code}' -b "$JAR" -X DELETE "$BASE/api/clusters/nosuch")"
[ "$code" = 404 ] || { echo "❌ DELETE несуществующего = $code, ожидался 404"; exit 1; }
code="$(curl -s -o /dev/null -w '%{http_code}' -b "$JAR" -X POST "$BASE/api/clusters" \
  -H 'Content-Type: application/json' \
  -d '{"name":"delme","sharded":false,"replicas":1,"requestCpu":0.5,"requestMem":4,"requestDisk":10}')"
[ "$code" = 409 ] || { echo "❌ создание поверх DELETING = $code, ожидался 409 (клэйм §9.2)"; exit 1; }

# Панель видит DELETING (следующий тик ≤ 3 c + polling): детали + сводка deleting
for i in $(seq 1 15); do
  curl -fsS -b "$JAR" "$BASE/api/clusters/delme" | jq -e '.state=="DELETING"' >/dev/null && break
  sleep 1
done
curl -fsS -b "$JAR" "$BASE/api/clusters/delme" | jq -e '.state=="DELETING"' >/dev/null \
  || { echo "❌ /api/clusters/delme: state != DELETING"; exit 1; }
curl -fsS -b "$JAR" "$BASE/api/clusters" | jq -e 'any(.[]; .name=="delme" and .deleting)' >/dev/null \
  || { echo "❌ /api/clusters: delme не помечен deleting"; exit 1; }
echo "  delme: DELETE → DELETING (config сохранён, ключи на месте), 204/404/409 — контракт §9.4"
echo "✓ 15-cluster-create: создание и удаление кластера e2e прошли"
