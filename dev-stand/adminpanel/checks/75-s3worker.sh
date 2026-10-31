#!/usr/bin/env bash
# 75-s3worker.sh (t32, arch/24 §1.1; мерж-гейт задачи): S3-домен против ЖИВОГО
# воркера (s3worker из deploy-контура: /healthz и /metrics — https без клиентского серта, /api — пара seed): демо-кластер
# через API (201 → provisioning воркером ≤ NodeBootSec+тик-запас → Active) →
# валидации (400/422) → sync-probe 202 → снос (202 → чистота etcd/docker/volume).
# RunTag-имена; trap-чистка; set -euo pipefail. Финал: зачистка за собой
# (ассерт чистоты — после teardown не осталось контейнера/ключа префикса).
set -euo pipefail
cd "$(dirname "$0")/.."

ROOT="$(cd ../.. && pwd)"
TAG="s75$(date +%s)"
# Порт API s3worker — из deploy/.env (синхронизация — 00-up.sh).
S3W_PORT="$(grep -E '^S3W_API_HOST_PORT=' "$ROOT/deploy/.env" 2>/dev/null | tail -1 | cut -d= -f2 || true)"
S3W_PORT="${S3W_PORT:-8086}"
BASE="https://localhost:${S3W_PORT}"
HEALTH=(curl -s -m 10 --cacert "$ROOT/deploy/tls/ca.pem")
CURL=(curl -s -m 10 --cacert "$ROOT/deploy/tls/ca.pem" --cert "$ROOT/deploy/tls/seed.crt" --key "$ROOT/deploy/tls/seed.key")
TMP="$(mktemp -d)"
# Own-only зачистка по TAG текущего прогона: падение между create и delete не
# оставляет демо-контейнер/том/ключи etcd (уже снесённые ресурсы — не падаем;
# живость API не требуется — ect дёргает etcd-контейнер стенда).
cleanup() {
  rm -rf "$TMP"
  docker rm -f $(docker ps -a --format '{{.Names}}' | grep "^s3w-$TAG" || true) >/dev/null 2>&1 || true
  docker volume rm $(docker volume ls -q --filter name="s3w-$TAG" || true) >/dev/null 2>&1 || true
  ect del "/s3/$TAG/" --prefix >/dev/null 2>&1 || true
}
trap cleanup EXIT

etcd_key() { docker compose exec -T etcd1 etcdctl get "$1" --print-value-only </dev/null 2>/dev/null; }
ect()      { docker compose exec -T etcd1 etcdctl --endpoints=http://localhost:2379 "$@"; }

# healthz живого s3worker (открытая грань) — Arrange.
for i in $(seq 1 30); do
  "${HEALTH[@]}" -o /dev/null "$BASE/healthz" 2>/dev/null && break
  sleep 1
done
"${HEALTH[@]}" -o /dev/null "$BASE/healthz" 2>/dev/null \
  || { echo "❌ s3worker not responding (:${S3W_PORT}/healthz; did 00-up.sh bring it up?)"; exit 1; }
echo "  s3worker alive: /healthz via https"

# /api без клиентского серта — ровно 401 (mTLS-политика /api, arch/24 §1.1;
# /healthz выше — открытая грань). GET на существующий маршрут мутации:
# middleware замыкает 401 до роутинга — состояние не мутируется.
s3_no_cert_code="$("${HEALTH[@]}" -o /dev/null -w '%{http_code}' "$BASE/api/s3/clusters" 2>/dev/null)"
[ "$s3_no_cert_code" = 401 ] || { echo "❌ s3worker /api without a client cert = $s3_no_cert_code (expected 401)"; exit 1; }

# 1) Демо-кластер через API: 201 → воркер провижинит (RUNNING) ≤ NodeBootSec+запас.
body="{\"name\":\"$TAG\",\"platform\":\"owns3\",\"instances\":[{\"name\":\"node1\",\"placement\":{\"dockerHost\":\"local\"},\"resources\":{\"cpu\":0.5,\"memGi\":1,\"diskGi\":5}}]}"
c="$("${CURL[@]}" -o "$TMP/create.json" -w '%{http_code}' -X POST "$BASE/api/s3/clusters" -H 'Content-Type: application/json' -d "$body")"
[ "$c" = 201 ] || { echo "❌ create = $c, expected 201"; cat "$TMP/create.json"; exit 1; }
echo "  create $TAG -> 201 (request accepted)"

ready=0
for i in $(seq 1 75); do
  [ "$(etcd_key "/s3/clusters/$TAG/nodes/node1/state")" = "RUNNING" ] && [ -z "$(etcd_key "/s3/clusters/$TAG/config" | grep -o '"state"' || true)" ] && { ready=1; break; }
  sleep 2
done
[ "$ready" = 1 ] || { echo "❌ $TAG did not reach RUNNING within 150 s (docker logs deploy-s3worker-1)"; exit 1; }
endpoints="$(etcd_key "/s3/clusters/$TAG/endpoints")"
echo "$endpoints" | grep -qE '^[^:]+:[0-9]+$' \
  || { echo "❌ endpoints '$endpoints' is not in host:port format"; exit 1; }
[ "$(etcd_key "/s3/clusters/$TAG/nodes/node1/placed_on")" = "local" ] \
  || { echo "❌ placed_on ≠ local ($(etcd_key "/s3/clusters/$TAG/nodes/node1/placed_on"))"; exit 1; }
echo "  $TAG: config without state, node1 RUNNING, endpoints=$endpoints, placed_on=local"

# 2) Контейнер/том ноды существуют (bucket создаёт provisioning воркера
#    root-кредом кластера из secrets-ключа; листинг — чек 80/45).
docker ps -a --format '{{.Names}}' | grep -qx "s3w-$TAG-node1" || { echo "❌ container s3w-$TAG-node1 not found"; exit 1; }
docker volume ls -q --filter name="s3w-$TAG-node1-data" | grep -q . || { echo "❌ volume s3w-$TAG-node1-data not found"; exit 1; }

# 3) Валидации: невалидный name → 400; etcdBackup без env-пары спец-ключа → 422
#    (стенд: S3W_ETCD_BACKUP_* не заданы — отсутствие пары = неполная, гвард
#    полноты arch/24 §1.1).
c="$("${CURL[@]}" -o /dev/null -w '%{http_code}' -X POST "$BASE/api/s3/clusters" -H 'Content-Type: application/json' \
  -d "{\"name\":\"Bad-Name\",\"platform\":\"owns3\",\"instances\":[{\"name\":\"node1\",\"placement\":{\"dockerHost\":\"local\"},\"resources\":{\"cpu\":1,\"memGi\":1,\"diskGi\":5}}]}")"
[ "$c" = 400 ] || { echo "❌ invalid name → $c, expected 400"; exit 1; }
c="$("${CURL[@]}" -o /dev/null -w '%{http_code}' -X POST "$BASE/api/s3/clusters" -H 'Content-Type: application/json' \
  -d "{\"name\":\"${TAG}x\",\"platform\":\"owns3\",\"etcdBackup\":true,\"instances\":[{\"name\":\"node1\",\"placement\":{\"dockerHost\":\"local\"},\"resources\":{\"cpu\":1,\"memGi\":1,\"diskGi\":5}}]}")"
[ "$c" = 422 ] || { echo "❌ etcdBackup without env key → $c, expected 422"; exit 1; }
# /metrics домен-счётчики (spec §9): .NET-экспортер выдаёт серию после первого
# наблюдения — проверка ПОСЛЕ операций API выше (create/400/422).
"${HEALTH[@]}" "$BASE/metrics" 2>/dev/null | grep -q '^s3worker_' \
  || { echo "❌ /metrics contains no s3worker_ metrics"; exit 1; }
echo "  validations: name → 400, etcdBackup without env key → 422; /metrics contains s3worker_"

# 4) sync-probe: 202 always; ключи connectivity/sync НЕ появляются (заглушка t33).
c="$("${CURL[@]}" -o /dev/null -w '%{http_code}' -X POST "$BASE/api/s3/clusters/$TAG/sync-probe")"
[ "$c" = 202 ] || { echo "❌ sync-probe → $c, expected 202"; exit 1; }
sleep 3
[ -z "$(ect get "/s3/clusters/$TAG/" --prefix --keys-only 2>/dev/null | grep -E 'connectivity|/sync' || true)" ] \
  || { echo "❌ connectivity/sync keys appeared after sync-probe"; exit 1; }
echo "  sync-probe: 202, t33 keys do not appear"

# 5) Снос: 202 → X0–X3 воркером → чистота etcd + docker + volume.
c="$("${CURL[@]}" -o /dev/null -w '%{http_code}' -X DELETE "$BASE/api/s3/clusters/$TAG")"
[ "$c" = 202 ] || { echo "❌ delete → $c, expected 202"; exit 1; }
clean=0
for i in $(seq 1 60); do
  if [ -z "$(ect get "/s3/clusters/$TAG/" --prefix --keys-only 2>/dev/null)" ] \
     && [ -z "$(docker ps -a --format '{{.Names}}' | grep "s3w-$TAG" || true)" ] \
     && [ -z "$(docker volume ls -q --filter name="s3w-$TAG" || true)" ]; then
    clean=1; break
  fi
  sleep 2
done
[ "$clean" = 1 ] || { echo "❌ traces of $TAG remain after delete (etcd/docker/volume)"; exit 1; }
echo "  delete $TAG: etcd prefix empty, no containers/volumes (cleanliness assert passed)"

echo "✅ 75-s3worker: demo cluster provisioning/validations/sync-probe/teardown — green, cleaned up"
