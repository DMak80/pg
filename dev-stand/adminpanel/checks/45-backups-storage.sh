#!/usr/bin/env bash
# Грань «Хранилище бэкапов» (t08, arch/04 §3, AC10): налив тестовых объектов
# mc-контейнером → /api/backups/storage (configured/health/дерево),
# on-demand objects, отсутствие секретов в DTO. Панель — всегда в докере.
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${ADMINPANEL_URL:-http://localhost:5050}"
JAR="$(mktemp)"; trap 'rm -f "$JAR"' EXIT

# Гвард: as-minio жив (full-профиль стенда) — без S3 бэкапов грань не проверить.
curl -fsS http://localhost:9000/minio/health/live >/dev/null 2>&1 \
  || { echo "❌ as-minio не отвечает на :9000/minio/health/live — запусти checks/00-up.sh (full-профиль)"; exit 1; }
echo "  as-minio жив (health/live ok)"

# Панель поднята (до 60 c) + cookie-логин — как 10-smoke-api.sh.
for i in $(seq 1 60); do curl -fsS "$BASE/api/healthz" >/dev/null 2>&1 && break; sleep 1; done
curl -fsS "$BASE/api/healthz" >/dev/null \
  || { echo "❌ панель не отвечает: $BASE/api/healthz (docker compose up -d adminpanel)"; exit 1; }
curl -fsS -c "$JAR" -o /dev/null -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  || { echo "❌ login admin/admin не прошёл"; exit 1; }
api() { curl -fsS -b "$JAR" "$BASE$1"; }
echo "  панель жива ($BASE), login ok"

# Налив тестовых объектов mc-контейнером — ТОЧНО по прецеденту 00-up.sh:
# у образа mc ENTRYPOINT ["mc"] — shell зовём через --entrypoint /bin/sh;
# сеть — из inspect as-minio (имя сети не хардкодим); идемпотентно (перепись).
minio_net="$(docker inspect as-minio -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}')"
docker run --rm --entrypoint /bin/sh --network "$minio_net" minio/mc:RELEASE.2025-08-13T08-35-41Z \
  -c "mc alias set a http://as-minio:9000 minioadmin minioadmin >/dev/null \
      && echo data | mc pipe a/pgworker-backups/demo/s1/full/20260913a/base.tar \
      && echo wal  | mc pipe a/pgworker-backups/demo/s1/wal/000000010000000000000001" \
  || { echo "❌ mc-налив в as-minio/pgworker-backups не прошёл"; exit 1; }
echo "  налиты demo/s1/full/20260913a/base.tar и demo/s1/wal/…01"

# Ждём инвентарь-тик панели (интервал 60 c — ретрай до 70 c). ВАЖНО: ждать
# надо ИМЕННО налитое дерево — configured/health зелёные сразу от первого
# тика, который мог пройти ещё до налива (по пустому bucket); налитые объекты
# попадают в инвентарь только следующим тиком. env панель получила при
# подъёме, reconfigure не нужна.
tree_ok() {
  api /api/backups/storage | jq -e \
    '.configured == true and .health.apiOk == true and .health.liveOk == true and .health.clusterOk == true
     and any(.clusters[]?; .cluster == "demo")
     and any(.clusters[] | select(.cluster == "demo") | .shards[]; .shard == "s1" and .fullsCount >= 1)' >/dev/null
}
for i in $(seq 1 70); do tree_ok && break; sleep 1; done
tree_ok || { echo "❌ /api/backups/storage: configured/health/дерево demo не зелёные за 70 c (инвентарь-тик?)"; exit 1; }
storage="$(api /api/backups/storage)"
echo "$storage" | jq -e '.configured == true and .health.apiOk == true and .health.liveOk == true and .health.clusterOk == true' >/dev/null \
  || { echo "❌ /api/backups/storage: configured/health"; exit 1; }
echo "  /api/backups/storage: configured=true, health api/live/cluster ok"

# Дерево: кластер demo налит, шард s1 несёт полный бэкап.
echo "$storage" | jq -e 'any(.clusters[]?; .cluster == "demo")' >/dev/null \
  || { echo "❌ дерево инвентаря: кластера demo нет"; exit 1; }
echo "$storage" | jq -e 'any(.clusters[] | select(.cluster == "demo") | .shards[]; .shard == "s1" and .fullsCount >= 1)' >/dev/null \
  || { echo "❌ дерево инвентаря: demo/s1 без полных"; exit 1; }
echo "  дерево: demo/s1, fullsCount>=1"

# On-demand objects (единственный прямой выход панели в MinIO на запрос).
api '/api/backups/objects?prefix=demo/s1/&maxKeys=1' | jq -e '(.items | length) == 1' >/dev/null \
  || { echo "❌ /api/backups/objects?prefix=demo/s1/&maxKeys=1: страница не из 1 объекта"; exit 1; }
echo "  objects: maxKeys=1 → ровно 1 объект"

# Секреты S3 не отдаются никогда (AC8).
api /api/backups/storage | jq -e '((tostring) | contains("SecretKey") or contains("AccessKey")) | not' >/dev/null \
  || { echo "❌ /api/backups/storage: в DTO просочились AccessKey/SecretKey"; exit 1; }
echo "  секретов AccessKey/SecretKey в DTO нет"

echo "✓ backups-storage зелёный"
