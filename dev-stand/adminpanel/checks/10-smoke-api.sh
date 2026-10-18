#!/usr/bin/env bash
# Дым API: панель против стенда — auth + все зоны инспекции (spec t10 §7.2).
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${ADMINPANEL_URL:-http://localhost:5050}"
JAR="$(mktemp)"; trap 'rm -f "$JAR"' EXIT

# Arrange: панель поднята (до 60 c; запуск руками — arch/04 §5)
for i in $(seq 1 60); do curl -fsS "$BASE/api/healthz" >/dev/null 2>&1 && break; sleep 1; done
curl -fsS "$BASE/api/healthz" >/dev/null \
  || { echo "❌ adminpanel not responding: $BASE/api/healthz (docker compose up -d adminpanel)"; exit 1; }
curl -fsS "$BASE/api/healthz" | jq -e '.status == "ok"' >/dev/null \
  || { echo "❌ /api/healthz: body is not {\"status\":\"ok\"}"; exit 1; }
echo "  adminpanel alive ($BASE, status=ok)"

# Act/Assert: 401 без cookie, login -> cookie
code="$(curl -s -o /dev/null -w '%{http_code}' "$BASE/api/overview")"
[ "$code" = 401 ] || { echo "❌ /api/overview without cookie = $code, expected 401"; exit 1; }
echo "  /api/overview without cookie -> 401"
curl -fsS -c "$JAR" -o /dev/null -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  || { echo "❌ login admin/admin failed"; exit 1; }
echo "  login admin -> cookie"

api() { curl -fsS -b "$JAR" "$BASE$1"; }

api /api/overview | jq -e \
  '.etcd.reachable == true and .alertsCritical >= 0
   and (.clusters | length) == 1 and .clusters[0].buckets == 16' >/dev/null \
  || { echo "❌ /api/overview: etcd.reachable/alertsCritical/clusters"; exit 1; }
echo "  /api/overview: etcd reachable, demo 16 buckets, alertsCritical=$(api /api/overview | jq -r '.alertsCritical')"

api /api/etcd/status | jq -e \
  '.endpoints[0].reachable == true and (.endpoints[0].version | length > 0)' >/dev/null \
  || { echo "❌ /api/etcd/status: endpoint/version"; exit 1; }
echo "  /api/etcd/status: $(api /api/etcd/status | jq -r '.endpoints[0].version') reachable"

api /api/clusters/demo | jq -e \
  '([.shards[] | select(.name=="shard1")][0].masterAddress | length > 0)
  and (.buckets | length) == 16' >/dev/null \
  || { echo "❌ /api/clusters/demo: master/buckets"; exit 1; }
echo "  /api/clusters/demo: master=$(api /api/clusters/demo | jq -r '[.shards[] | select(.name=="shard1")][0].masterAddress'), 16 buckets"

# /api/ha обогащается пробами (тик 15 c): на холодном старте панель могла
# поймать окно «PG ещё поднимается» (replica/stopped) — ждём свежей пробы.
# Адрес нод portalloc-динамичен — гварды без хардкода: leader непуст,
# members 2, у лидера роль master.
ha1_ok() {
  local leader
  leader="$(api /api/ha/demo-shard1 | jq -r '.leaderName // empty')"
  [ -n "$leader" ] \
    && api /api/ha/demo-shard1 | jq -e --arg l "$leader" \
        '(.members | length) == 2
         and ([.members[] | select(.name == $l)][0].role == "leader")' >/dev/null
}
for i in $(seq 1 25); do ha1_ok && break; sleep 1; done
ha1_ok || { echo "❌ /api/ha/demo-shard1: leader/members"; exit 1; }
echo "  /api/ha/demo-shard1: leader $(api /api/ha/demo-shard1 | jq -r '.leaderName'), 2 members"

# Чистый подъём = согласованный кластер (adopt-repair spec §3.6): сид больше
# не сеет аномалий — в алертах нет вечных move-* (нахерачивает и гасит чек 20).
api /api/alerts | jq -e 'all(.[]; (.kind | startswith("move-")) | not)' >/dev/null \
  || { echo "❌ /api/alerts: move-* alerts present on a clean start (does the seed plant anomalies?)"; exit 1; }
echo "  /api/alerts: no move-* alerts (clean start)"

echo "✓ smoke API green"
