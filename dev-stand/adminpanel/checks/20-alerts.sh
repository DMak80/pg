#!/usr/bin/env bash
# Seeded-аномалии + shard-no-master: появление <=2 тиков и гашение (spec t10 §7.3).
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${ADMINPANEL_URL:-http://localhost:5000}"
JAR="$(mktemp)"; trap 'rm -f "$JAR"' EXIT
ect() { docker compose exec -T etcd etcdctl --endpoints=http://localhost:2379 "$@"; }

curl -fsS -c "$JAR" -o /dev/null -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  || { echo "❌ login (панель на $BASE? запусти dotnet run)"; exit 1; }
api() { curl -fsS -b "$JAR" "$BASE$1"; }

has_alert() { # kind target [severity]
  api /api/alerts | jq -e --arg k "$1" --arg t "$2" --arg s "${3:-}" \
    'any(.[]; .kind==$k and .target==$t and ($s=="" or .severity==$s))' >/dev/null
}
wait_alert() {
  for i in $(seq 1 15); do has_alert "$1" "$2" "${3:-}" && return 0; sleep 1; done
  echo "❌ алерт $1 -> $2${3:+ ($3)} не появился за 15 c"; return 1
}
wait_no_alert() {
  for i in $(seq 1 15); do has_alert "$1" "$2" || return 0; sleep 1; done
  echo "❌ алерт $1 -> $2 не погас за 15 c"; return 1
}

# Assert 1: seeded-аномалии (тик панели 3 c)
wait_alert move-stale demo/bucket_11;   echo "  move-stale -> demo/bucket_11"
wait_alert move-aborting demo/bucket_7; echo "  move-aborting -> demo/bucket_7"

# Act 2: удалить master-ключ s2 (в full сначала стоп эмуляторов — keepalive
# каждые 1 c переписал бы ключ; spec §7.3)
full=0
if docker compose ps --services --filter status=running 2>/dev/null | grep -qx hc2a; then
  full=1
  echo "  (full) стоп эмуляторов s2: hc2a/hc2b"
  docker compose stop hc2a hc2b >/dev/null
fi
ect del /clusters/demo/shards/s2/master >/dev/null
echo "  master-ключ s2 удалён"

# Assert 3: critical-алерт <=2 тиков
wait_alert shard-no-master demo/s2 critical
echo "  shard-no-master -> demo/s2 (critical)"

# Act 4: восстановление
if [ "$full" = 1 ]; then
  docker compose start hc2a hc2b >/dev/null
  echo "  (full) эмуляторы s2 запущены — lease восстановится сам (<=3 c)"
else
  ect put /clusters/demo/shards/s2/master 's2a:5432' >/dev/null
  echo "  (quick) ключ возвращён статично"
fi

# Assert 5: алерт погас
wait_no_alert shard-no-master demo/s2
echo "  shard-no-master -> demo/s2 погас"

echo "✓ alerts-сценарий зелёный"
