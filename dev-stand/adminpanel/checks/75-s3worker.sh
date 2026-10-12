#!/usr/bin/env bash
# 75-s3worker.sh (t32, arch/24 §1.1; мерж-гейт задачи): S3-домен против ЖИВОГО
# воркера (s3worker из deploy-контура, mTLS healthcheck-парой): демо-кластер
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
API="https://localhost:${S3W_PORT}"
MTLS=(curl -s -m 10 --cacert "$ROOT/deploy/tls/ca.pem" --cert "$ROOT/deploy/tls/healthcheck.crt" --key "$ROOT/deploy/tls/healthcheck.key")
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

# healthz живого s3worker (mTLS) —Arrange.
for i in $(seq 1 30); do
  "${MTLS[@]}" -o /dev/null "$API/healthz" 2>/dev/null && break
  sleep 1
done
"${MTLS[@]}" -o /dev/null "$API/healthz" 2>/dev/null \
  || { echo "❌ s3worker не отвечает (:${S3W_PORT}/healthz; 00-up.sh поднимал?)"; exit 1; }
echo "  s3worker жив: /healthz по mTLS"

# 1) Демо-кластер через API: 201 → воркер провижинит (RUNNING) ≤ NodeBootSec+запас.
body="{\"name\":\"$TAG\",\"platform\":\"owns3\",\"nodes\":1,\"secretProfile\":\"BACKUPS-MAIN\",\"placements\":[{\"dockerHost\":\"local\"}],\"resources\":{\"cpu\":0.5,\"memGi\":1,\"diskGi\":5}}"
c="$("${MTLS[@]}" -o "$TMP/create.json" -w '%{http_code}' -X POST "$API/api/s3/clusters" -H 'Content-Type: application/json' -d "$body")"
[ "$c" = 201 ] || { echo "❌ create = $c, ожидался 201"; cat "$TMP/create.json"; exit 1; }
echo "  create $TAG -> 201 (заявка принята)"

ready=0
for i in $(seq 1 75); do
  [ "$(etcd_key "/s3/clusters/$TAG/nodes/node1/state")" = "RUNNING" ] && [ -z "$(etcd_key "/s3/clusters/$TAG/config" | grep -o '"state"' || true)" ] && { ready=1; break; }
  sleep 2
done
[ "$ready" = 1 ] || { echo "❌ $TAG не достиг RUNNING за 150 c (docker logs deploy-s3worker-1)"; exit 1; }
endpoints="$(etcd_key "/s3/clusters/$TAG/endpoints")"
echo "$endpoints" | grep -qE '^[^:]+:[0-9]+$' \
  || { echo "❌ endpoints '$endpoints' не формата host:port"; exit 1; }
[ "$(etcd_key "/s3/clusters/$TAG/nodes/node1/placed_on")" = "local" ] \
  || { echo "❌ placed_on ≠ local ($(etcd_key "/s3/clusters/$TAG/nodes/node1/placed_on"))"; exit 1; }
echo "  $TAG: config без state, node1 RUNNING, endpoints=$endpoints, placed_on=local"

# 2) Контейнер/том ноды существуют; bucket создан (листинг root-кредом — mc).
docker ps -a --format '{{.Names}}' | grep -qx "s3w-$TAG-node1" || { echo "❌ контейнер s3w-$TAG-node1 не найден"; exit 1; }
docker volume ls -q --filter name="s3w-$TAG-node1-data" | grep -q . || { echo "❌ том s3w-$TAG-node1-data не найден"; exit 1; }

# 3) Валидации: невалидный name → 400; неизвестный профиль → 422.
c="$("${MTLS[@]}" -o /dev/null -w '%{http_code}' -X POST "$API/api/s3/clusters" -H 'Content-Type: application/json' \
  -d "{\"name\":\"Bad-Name\",\"platform\":\"owns3\",\"nodes\":1,\"secretProfile\":\"BACKUPS-MAIN\",\"placements\":[{\"dockerHost\":\"local\"}],\"resources\":{\"cpu\":1,\"memGi\":1,\"diskGi\":5}}")"
[ "$c" = 400 ] || { echo "❌ невалидный name → $c, ожидался 400"; exit 1; }
c="$("${MTLS[@]}" -o /dev/null -w '%{http_code}' -X POST "$API/api/s3/clusters" -H 'Content-Type: application/json' \
  -d "{\"name\":\"${TAG}x\",\"platform\":\"owns3\",\"nodes\":1,\"secretProfile\":\"no-such-profile\",\"placements\":[{\"dockerHost\":\"local\"}],\"resources\":{\"cpu\":1,\"memGi\":1,\"diskGi\":5}}")"
[ "$c" = 422 ] || { echo "❌ неизвестный профиль → $c, ожидался 422"; exit 1; }
# /metrics домен-счётчики (spec §9): .NET-экспортер выдаёт серию после первого
# наблюдения — проверка ПОСЛЕ операций API выше (create/400/422).
"${MTLS[@]}" "$API/metrics" 2>/dev/null | grep -q '^s3worker_' \
  || { echo "❌ /metrics не содержит метрик s3worker_"; exit 1; }
echo "  валидации: name → 400, профиль → 422; /metrics содержит s3worker_"

# 4) sync-probe: 202 always; ключи connectivity/sync НЕ появляются (заглушка t33).
c="$("${MTLS[@]}" -o /dev/null -w '%{http_code}' -X POST "$API/api/s3/clusters/$TAG/sync-probe")"
[ "$c" = 202 ] || { echo "❌ sync-probe → $c, ожидался 202"; exit 1; }
sleep 3
[ -z "$(ect get "/s3/clusters/$TAG/" --prefix --keys-only 2>/dev/null | grep -E 'connectivity|/sync' || true)" ] \
  || { echo "❌ после sync-probe появились ключи connectivity/sync"; exit 1; }
echo "  sync-probe: 202, ключи t33 не появляются"

# 5) Снос: 202 → X0–X3 воркером → чистота etcd + docker + volume.
c="$("${MTLS[@]}" -o /dev/null -w '%{http_code}' -X DELETE "$API/api/s3/clusters/$TAG")"
[ "$c" = 202 ] || { echo "❌ delete → $c, ожидался 202"; exit 1; }
clean=0
for i in $(seq 1 60); do
  if [ -z "$(ect get "/s3/clusters/$TAG/" --prefix --keys-only 2>/dev/null)" ] \
     && [ -z "$(docker ps -a --format '{{.Names}}' | grep "s3w-$TAG" || true)" ] \
     && [ -z "$(docker volume ls -q --filter name="s3w-$TAG" || true)" ]; then
    clean=1; break
  fi
  sleep 2
done
[ "$clean" = 1 ] || { echo "❌ после delete остались следы $TAG (etcd/docker/volume)"; exit 1; }
echo "  delete $TAG: префикс etcd пуст, контейнеров/томов нет (ассерт чистоты пройден)"

echo "✅ 75-s3worker: демо-кластер provisioning/валидации/sync-probe/снос — зелёный, за собой убран"
