#!/usr/bin/env bash
# Дым API: панель против стенда — auth + все зоны инспекции (spec t10 §7.2).
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${ADMINPANEL_URL:-http://localhost:5000}"
JAR="$(mktemp)"; trap 'rm -f "$JAR"' EXIT

# Arrange: панель поднята (до 60 c; запуск руками — arch/04 §5)
for i in $(seq 1 60); do curl -fsS "$BASE/api/healthz" >/dev/null 2>&1 && break; sleep 1; done
curl -fsS "$BASE/api/healthz" >/dev/null \
  || { echo "❌ панель не отвечает: $BASE/api/healthz (dotnet run --project src/AdminPanel.Api)"; exit 1; }
curl -fsS "$BASE/api/healthz" | jq -e '.status == "ok"' >/dev/null \
  || { echo "❌ /api/healthz: тело не {\"status\":\"ok\"}"; exit 1; }
echo "  панель жива ($BASE, status=ok)"

# Act/Assert: 401 без cookie, login -> cookie
code="$(curl -s -o /dev/null -w '%{http_code}' "$BASE/api/overview")"
[ "$code" = 401 ] || { echo "❌ /api/overview без cookie = $code, ожидался 401"; exit 1; }
echo "  без cookie /api/overview -> 401"
curl -fsS -c "$JAR" -o /dev/null -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  || { echo "❌ login admin/admin не прошёл"; exit 1; }
echo "  login admin -> cookie"

api() { curl -fsS -b "$JAR" "$BASE$1"; }

api /api/overview | jq -e \
  '.etcd.reachable == true and .alertsCritical >= 0
   and (.clusters | length) == 1 and .clusters[0].buckets == 16' >/dev/null \
  || { echo "❌ /api/overview: etcd.reachable/alertsCritical/clusters"; exit 1; }
echo "  /api/overview: etcd reachable, demo 16 бакетов, alertsCritical=$(api /api/overview | jq -r '.alertsCritical')"

api /api/etcd/status | jq -e \
  '.endpoints[0].reachable == true and (.endpoints[0].version | length > 0)' >/dev/null \
  || { echo "❌ /api/etcd/status: endpoint/version"; exit 1; }
echo "  /api/etcd/status: $(api /api/etcd/status | jq -r '.endpoints[0].version') reachable"

api /api/clusters/demo | jq -e \
  '([.shards[] | select(.name=="s1")][0].masterAddress == "s1a:5432")
  and (.buckets | length) == 16
  and .heals[0].bucket == "bucket_5"' >/dev/null \
  || { echo "❌ /api/clusters/demo: master/buckets/heals"; exit 1; }
echo "  /api/clusters/demo: master s1a:5432, 16 бакетов, heal bucket_5"

# /api/ha обогащается пробами (тик 15 c): на холодном старте панель могла
# поймать окно «PG ещё поднимается» (replica/stopped) — ждём свежей пробы.
ha1_ok() {
  api /api/ha/demo-s1 | jq -e \
    '.leaderName == "s1a" and (.members | length) == 2
     and ([.members[] | select(.name=="s1a")][0].role == "master")' >/dev/null
}
for i in $(seq 1 25); do ha1_ok && break; sleep 1; done
ha1_ok || { echo "❌ /api/ha/demo-s1: leader/members"; exit 1; }
echo "  /api/ha/demo-s1: leader s1a, 2 члена"

# Сид-аномалии видны в алертах (тик панели 3 c — ждём до 15 c)
for i in $(seq 1 15); do
  api /api/alerts | jq -e 'any(.[]; .kind=="move-stale" and .target=="demo/bucket_11")' >/dev/null && break
  sleep 1
done
api /api/alerts | jq -e \
  'any(.[]; .kind=="move-stale" and .target=="demo/bucket_11")
   and any(.[]; .kind=="move-aborting" and .target=="demo/bucket_7")' >/dev/null \
  || { echo "❌ /api/alerts: seeded move-stale/move-aborting не видны"; exit 1; }
echo "  /api/alerts: move-stale bucket_11, move-aborting bucket_7"

echo "✓ smoke API зелёный"
