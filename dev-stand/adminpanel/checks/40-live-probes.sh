#!/usr/bin/env bash
# Live-пробы панели: Patroni-REST через HostMap + SQL-пробы multi-host
# (spec t10 §7.5). Гоняется ПОСЛЕ 30-failover (мастер s1 = s1b, реплика
# s1a sync; шард 2 нетронут). Только full.
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${ADMINPANEL_URL:-http://localhost:5050}"
JAR="$(mktemp)"; trap 'rm -f "$JAR"' EXIT
curl -fsS -c "$JAR" -o /dev/null -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  || { echo "❌ login (панель на $BASE?)"; exit 1; }
api() { curl -fsS -b "$JAR" "$BASE$1"; }

# 1) HA-скопы: все члены пробиты без ошибок; роли/состояния живые.
#    Тик проб 15 c — ждём до 40 c (spec §7.5).
ha_ok() {
  api /api/ha/"$1" | jq -e '
    all(.members[]; .probeError == null and .probeAtUtc != null)
    and any(.members[]; .state == "running")
    and any(.members[]; .state == "streaming" and .lagBytes != null and .timeline >= 1)' >/dev/null
}
for i in $(seq 1 40); do ha_ok demo-s1 && break; sleep 2; done
ha_ok demo-s1 || { echo "❌ /api/ha/demo-s1: пробы не обогащены (HostMap? эмуляторы?)"; exit 1; }
echo "  demo-s1: мастер running, реплика streaming+lag (Patroni-REST через HostMap)"
for i in $(seq 1 40); do ha_ok demo-s2 && break; sleep 2; done
ha_ok demo-s2 || { echo "❌ /api/ha/demo-s2: пробы не обогащены"; exit 1; }
echo "  demo-s2: то же"

# 2) SQL-пробы: runtime без ошибок, sync-standby, инвентарь 10+6 (все
#    ACTIVE — сид чистый, adopt-repair spec §3.6), lease живы
cl_ok() {
  api /api/clusters/demo | jq -e '
    ([.shards[] | select(.name=="s1")][0] |
      .runtime.error == null and .runtime.standbiesSync >= 1
      and (.runtime.bucketSchemas | length) == 10
      and .masterAddress == "s1b:5432" and .masterLeaseAlive == true)
    and ([.shards[] | select(.name=="s2")][0] |
      .runtime.error == null and .runtime.standbiesSync >= 1
      and (.runtime.bucketSchemas | length) == 6
      and .masterLeaseAlive == true)' >/dev/null
}
for i in $(seq 1 40); do cl_ok && break; sleep 2; done
cl_ok || { echo "❌ /api/clusters/demo: runtime/инвентарь/lease (SQL-проба на 127.0.0.1:5433-5436?)"; exit 1; }
echo "  SQL-пробы: runtime шардов жив, sync-standby есть, инвентарь 10+6"

# 3) никаких ошибок проб и расхождений (spec §7.5 п.4)
# probe-failed info неподнятых кластеров чека 15 (canon10/smoke/solo — никогда
# не инициализировались) — не ошибка живого стенда; важное — warning/critical.
api /api/alerts | jq -e \
  'all(.[]; (.kind != "probe-failed" or .severity == "info")
     and .kind != "inventory-mismatch" and .kind != "shard-no-master")' >/dev/null \
  || { echo "❌ /api/alerts: есть probe-failed(≥warning) / inventory-mismatch / shard-no-master"; exit 1; }
echo "  алертов проб/инвентаря/без-мастера нет"

echo "✓ live-probes зелёный"
