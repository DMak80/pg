#!/usr/bin/env bash
# Live-пробы панели на каноническом кластере (t41): Patroni/SQL-пробы —
# advertised portalloc-адреса нод напрямую (HostMap демонтирован; адреса из
# /api/clusters/demo: patroni :18xxx, pg :15xxx через host.docker.internal).
# Гоняется ПОСЛЕ 30-failover — топология любая: мастер по факту.
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${ADMINPANEL_URL:-http://localhost:5050}"
JAR="$(mktemp)"; trap 'rm -f "$JAR"' EXIT
curl -fsS -c "$JAR" -o /dev/null -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  || { echo "❌ login (панель на $BASE?)"; exit 1; }
api() { curl -fsS -b "$JAR" "$BASE$1"; }

# 1) HA-скопы: все члены пробиты без ошибок; роли/состояния живые.
#    Тик проб 15 c — ждём до 40 c.
ha_ok() {
  api /api/ha/"$1" | jq -e '
    all(.members[]; .probeError == null and .probeAtUtc != null)
    and any(.members[]; .state == "running")
    and any(.members[]; .state == "streaming" and .lagBytes != null and .timeline >= 1)' >/dev/null
}
for scope in demo-shard1 demo-shard2; do
  ok=""
  for i in $(seq 1 40); do ha_ok "$scope" && { ok=1; break; }; sleep 2; done
  [ -n "$ok" ] || { echo "❌ /api/ha/$scope: пробы не обогащены (advertised portalloc-адреса?)"; exit 1; }
  echo "  $scope: мастер running, реплика streaming+lag (Patroni-REST по advertised)"
done

# 2) SQL-пробы: runtime без ошибок, sync-standby есть, инвентарь схем
#    непуст, lease живы; адрес мастера portalloc-динамичен — без хардкода.
cl_ok() {
  api /api/clusters/demo | jq -e '
    ([.shards[] | select(.name=="shard1")][0] |
      .runtime.error == null and .runtime.standbiesSync >= 1
      and (.runtime.bucketSchemas | length) > 0
      and (.masterAddress | length > 0) and .masterLeaseAlive == true)
    and ([.shards[] | select(.name=="shard2")][0] |
      .runtime.error == null and .runtime.standbiesSync >= 1
      and (.runtime.bucketSchemas | length) > 0
      and .masterLeaseAlive == true)' >/dev/null
}
ok=""
for i in $(seq 1 40); do cl_ok && { ok=1; break; }; sleep 2; done
[ -n "$ok" ] || { echo "❌ /api/clusters/demo: runtime/инвентарь/lease (SQL-пробы по advertised portalloc)"; exit 1; }
echo "  SQL-пробы: runtime шардов жив, sync-standby есть, инвентарь схем непуст"

# 3) никаких ошибок проб и расхождений: probe-failed быть не должно вовсе.
api /api/alerts | jq -e \
  'all(.[]; .kind != "probe-failed"
     and .kind != "inventory-mismatch" and .kind != "shard-no-master")' >/dev/null \
  || { echo "❌ /api/alerts: есть probe-failed / inventory-mismatch / shard-no-master"; exit 1; }
echo "  алертов проб/инвентаря/без-мастера нет"

echo "✓ live-probes зелёный"
