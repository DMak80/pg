#!/usr/bin/env bash
# Грань «Хранилище бэкапов» против ownS3 (t41: контур — owns3:9000 внутри /
# :9010 снаружи; t08, arch/04 §3): идемпотентный mc-налив тестовых объектов
# снаружи → /api/backups/storage (configured/health/дерево demo/shard1),
# on-demand objects, отсутствие секретов в DTO; дерево несут реальные
# бэкапы demo (налив — гарантия минимума до первого тика планировщика).
# Панель — всегда в докере.
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${ADMINPANEL_URL:-http://localhost:5050}"
JAR="$(mktemp)"; trap 'rm -f "$JAR"' EXIT

# Гвард: ownS3 жив (deploy-контур, t41) — без S3 бэкапов грань не проверить;
# порт/root — из deploy/.env (по образцу чека 46).
ROOT="$(cd ../.. && pwd)"
ENV_FILE="$ROOT/deploy/.env"
OWNS3_PORT="$(grep -E '^OWNS3_HOST_PORT=' "$ENV_FILE" 2>/dev/null | tail -1 | cut -d= -f2 || true)"
OWNS3_PORT="${OWNS3_PORT:-9010}"
OWNS3_ROOT_USER="$(grep -E '^OWNS3_ROOT_USER=' "$ENV_FILE" | tail -1 | cut -d= -f2)"
OWNS3_ROOT_PASSWORD="$(grep -E '^OWNS3_ROOT_PASSWORD=' "$ENV_FILE" | tail -1 | cut -d= -f2)"
curl -sf "http://localhost:${OWNS3_PORT}/healthz" >/dev/null 2>&1 \
  || { echo "❌ owns3 не отвечает на :${OWNS3_PORT}/healthz — запусти checks/00-up.sh"; exit 1; }
echo "  owns3 жив (/healthz ok, :${OWNS3_PORT})"

# Панель поднята (до 60 c) + cookie-логин — как 10-smoke-api.sh.
for i in $(seq 1 60); do curl -fsS "$BASE/api/healthz" >/dev/null 2>&1 && break; sleep 1; done
curl -fsS "$BASE/api/healthz" >/dev/null \
  || { echo "❌ панель не отвечает: $BASE/api/healthz (docker compose up -d adminpanel)"; exit 1; }
curl -fsS -c "$JAR" -o /dev/null -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  || { echo "❌ login admin/admin не прошёл"; exit 1; }
api() { curl -fsS -b "$JAR" "$BASE$1"; }
echo "  панель жива ($BASE), login ok"

# Налив тестовых объектов mc-контейнером против ХОСТ-ПУБЛИКАЦИИ ownS3
# (прецедент чека 46: --add-host host-gateway, root-креды из deploy/.env;
# идемпотентно — перепись тех же ключей).
docker run --rm --add-host host.docker.internal:host-gateway \
  --entrypoint /bin/sh minio/mc:RELEASE.2025-08-13T08-35-41Z \
  -c "mc alias set a http://host.docker.internal:${OWNS3_PORT} '$OWNS3_ROOT_USER' '$OWNS3_ROOT_PASSWORD' >/dev/null \
      && echo data | mc pipe a/pgworker-backups/demo/shard1/full/20260913a/base.tar \
      && echo wal  | mc pipe a/pgworker-backups/demo/shard1/wal/000000010000000000000001" \
  || { echo "❌ mc-налив в ownS3/pgworker-backups не прошёл (:${OWNS3_PORT})"; exit 1; }
echo "  налиты demo/shard1/full/20260913a/base.tar и demo/shard1/wal/…01 (ownS3)"

# Ждём инвентарь-тик панели (интервал 60 c — ретрай до 70 c). ВАЖНО: ждать
# надо ИМЕННО налитое дерево — configured/health зелёные сразу от первого
# тика, который мог пройти ещё до налива (по пустому bucket); налитые объекты
# попадают в инвентарь только следующим тиком. env панель получила при
# подъёме, reconfigure не нужна.
tree_ok() {
  api /api/backups/storage | jq -e \
    '.configured == true and .health.apiOk == true and .health.liveOk == true and .health.clusterOk == null
     and any(.clusters[]?; .cluster == "demo")
     and any(.clusters[] | select(.cluster == "demo") | .shards[]; .shard == "shard1" and .fullsCount >= 1)' >/dev/null
}
for i in $(seq 1 70); do tree_ok && break; sleep 1; done
tree_ok || { echo "❌ /api/backups/storage: configured/health/дерево demo не зелёные за 70 c (инвентарь-тик?)"; exit 1; }
storage="$(api /api/backups/storage)"
echo "$storage" | jq -e '.configured == true and .health.apiOk == true and .health.liveOk == true and .health.clusterOk == null' >/dev/null \
  || { echo "❌ /api/backups/storage: configured/health"; exit 1; }
echo "  /api/backups/storage: configured=true, health api/live ok, cluster n/a (ownS3)"

# Дерево: кластер demo налит, шард s1 несёт полный бэкап.
echo "$storage" | jq -e 'any(.clusters[]?; .cluster == "demo")' >/dev/null \
  || { echo "❌ дерево инвентаря: кластера demo нет"; exit 1; }
echo "$storage" | jq -e 'any(.clusters[] | select(.cluster == "demo") | .shards[]; .shard == "shard1" and .fullsCount >= 1)' >/dev/null \
  || { echo "❌ дерево инвентаря: demo/shard1 без полных"; exit 1; }
echo "  дерево: demo/shard1, fullsCount>=1"

# On-demand objects (единственный прямой выход панели в MinIO на запрос).
api '/api/backups/objects?prefix=demo/shard1/&maxKeys=1' | jq -e '(.items | length) == 1' >/dev/null \
  || { echo "❌ /api/backups/objects?prefix=demo/shard1/&maxKeys=1: страница не из 1 объекта"; exit 1; }
echo "  objects: maxKeys=1 → ровно 1 объект"

# Секреты S3 не отдаются никогда (AC8).
api /api/backups/storage | jq -e '((tostring) | contains("SecretKey") or contains("AccessKey")) | not' >/dev/null \
  || { echo "❌ /api/backups/storage: в DTO просочились AccessKey/SecretKey"; exit 1; }
echo "  секретов AccessKey/SecretKey в DTO нет"

echo "✓ backups-storage зелёный"
