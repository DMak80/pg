#!/usr/bin/env bash
# Failover шарда 1 канонического кластера (t41): kill мастера (контейнер
# движка pgw-demo-shard1-*) → Patroni (DCS etcd) сам промотит реплику →
# wait нового мастера → алерты вспыхивают-гаснут (или не вспыхивают при
# быстром промоте) → надзор движка rejoin'ит убитую ноду → финал: оба
# члена RUNNING, master-ключ жив.
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${ADMINPANEL_URL:-http://localhost:5050}"
JAR="$(mktemp)"; trap 'rm -f "$JAR"' EXIT
ect() { docker compose exec -T etcd1 etcdctl --endpoints=http://localhost:2379 "$@"; }

curl -fsS -c "$JAR" -o /dev/null -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  || { echo "❌ login (панель на $BASE?)"; exit 1; }
api() { curl -fsS -b "$JAR" "$BASE$1"; }
has_alert() { api /api/alerts | jq -e --arg k "$1" --arg t "$2" 'any(.[]; .kind==$k and .target==$t)' >/dev/null; }
wait_no_alert() { for i in $(seq 1 120); do has_alert "$1" "$2" || return 0; sleep 1; done; echo "❌ алерт $1 -> $2 не погас за 120 c"; return 1; }

# Arrange: мастер shard1 — по DCS leader-ключу Patroni (имя ноды; master-ключ
# кластера несёт portalloc-формат advertised-хоста); контейнер движка
# pgw-demo-shard1-<node>.
m1_raw="$(ect get /service/demo-shard1/leader --print-value-only 2>/dev/null)"
if [[ "$m1_raw" == {*} ]]; then
  m1_node="$(echo "$m1_raw" | jq -r '.name // empty')"
else
  m1_node="$m1_raw"
fi
[ -n "$m1_node" ] && [ "$m1_node" != "null" ] \
  || { echo "❌ DCS leader demo-shard1 пуст"; exit 1; }
m1=""
for i in $(seq 1 30); do
  m1="$(ect get /clusters/demo/shards/shard1/master --print-value-only 2>/dev/null)"
  [ -n "$m1" ] && break
  sleep 2
done
[ -n "$m1" ] \
  || { echo "❌ master-ключ shard1 пуст (60 c; предыдущий чек 20 останавливал воркеров — ждём MasterKeyReconciler)"; exit 1; }
m1_container="pgw-demo-shard1-${m1_node}"
docker inspect "$m1_container" >/dev/null 2>&1 \
  || { echo "❌ контейнер мастера $m1_container не найден (docker ps | grep pgw-demo)"; exit 1; }
echo "  мастер shard1: $m1 (DCS leader $m1_node, контейнер $m1_container)"

# Act 1: отказ мастера (kill контейнера движка)
echo ">>> [PHASE] docker kill $m1_container ($(date +%H:%M:%S))"
docker kill "$m1_container" >/dev/null

# Act/Assert 2: Patroni промотил реплику — wait НОВОГО мастера по DCS
# leader-ключу (имена нод; master-ключ кластера несёт portalloc-формат и
# может не смениться хостом). Быстрый промоут может закрывать окно «нет
# мастера» раньше тика алертов — вспышка shard-no-master опциональна,
# гашение при живом мастере обязательно.
dcs_leader() {
  local raw
  raw="$(ect get /service/demo-shard1/leader --print-value-only 2>/dev/null || true)"
  if [[ "$raw" == {*} ]]; then echo "$raw" | jq -r '.name // empty'; else echo "$raw"; fi
}
m2_node=""
for i in $(seq 1 120); do
  m2_node="$(dcs_leader)"
  [ -n "$m2_node" ] && [ "$m2_node" != "$m1_node" ] && break
  sleep 2
done
[ -n "$m2_node" ] && [ "$m2_node" != "$m1_node" ] \
  || { echo "❌ новый мастер shard1 не избран за 240 c (DCS leader=$m2_node, был $m1_node; docker logs $m1_container — Patroni?)"; exit 1; }
echo "  Patroni промотил реплику: DCS leader $m1_node -> $m2_node"

wait_no_alert shard-no-master demo/shard1; echo "  shard-no-master -> demo/shard1 погашен (мастер жив)"
wait_no_alert shard-no-leader demo-shard1 30 || echo "  (shard-no-leader demo-shard1 не вспыхнул — быстрый промоут)"

# Assert 3: панель видит нового мастера (HA-грань, тик 15 c).
ha_master_ok() {
  api /api/ha/demo-shard1 | jq -e --arg l "$m2_node" '.leaderName == $l' >/dev/null
}
for i in $(seq 1 25); do ha_master_ok && break; sleep 1; done
ha_master_ok || { echo "❌ /api/ha/demo-shard1 не видит нового мастера ($m2_node)"; exit 1; }
echo "  /api/ha/demo-shard1: leader $m2_node"

# Act/Assert 4: надзор движка rejoin'ит убитую ноду — wait RUNNING обеих.
echo ">>> [PHASE] жду rejoin ${m1_node} надзором движка (бюджет 300 c, $(date +%H:%M:%S))"
rejoin_ok=""
for i in $(seq 1 150); do
  s1="$(ect get /clusters/demo/shards/shard1/nodes/${m1_node}/state --print-value-only 2>/dev/null || true)"
  s2="$(ect get /clusters/demo/shards/shard1/nodes/${m2_node}/state --print-value-only 2>/dev/null || true)"
  [ "$s1" = "RUNNING" ] && [ "$s2" = "RUNNING" ] && { rejoin_ok=1; break; }
  sleep 2
done
[ -n "$rejoin_ok" ] \
  || { echo "❌ rejoin не сошёлся за 300 c (${m1_node}=${s1:-?} ${m2_node}=${s2:-?}; docker logs deploy-pgworker-1)"; exit 1; }
echo "  оба члена shard1 RUNNING (${m1_node} rejoin, ${m2_node} мастер)"

# Финал: master-ключ жив.
m3="$(ect get /clusters/demo/shards/shard1/master --print-value-only)"
[ -n "$m3" ] || { echo "❌ master-ключ shard1 пуст в финале"; exit 1; }
echo "  master-ключ жив: $m3"

echo "✓ failover-цикл зелёный: kill -> Patroni-промоут -> гашение -> rejoin надзором"
