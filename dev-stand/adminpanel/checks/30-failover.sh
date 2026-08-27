#!/usr/bin/env bash
# Failover шарда 1: stop мастера -> алерты -> promote s1b -> гашение ->
# rejoin s1a репликой (spec t10 §7.4). Только full-профиль.
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${ADMINPANEL_URL:-http://localhost:5050}"
JAR="$(mktemp)"; trap 'rm -f "$JAR"' EXIT
ect() { docker compose exec -T etcd etcdctl --endpoints=http://localhost:2379 "$@"; }
# Запрос — только через -c: позиционный аргумент psql трактуется как DBNAME
sq()   { docker compose exec -T "$1" psql -U postgres -d postgres -qAt -v ON_ERROR_STOP=1 -c "$2"; }

curl -fsS -c "$JAR" -o /dev/null -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  || { echo "❌ login (панель на $BASE?)"; exit 1; }
api() { curl -fsS -b "$JAR" "$BASE$1"; }
has_alert() { api /api/alerts | jq -e --arg k "$1" --arg t "$2" 'any(.[]; .kind==$k and .target==$t)' >/dev/null; }
wait_alert()    { for i in $(seq 1 15); do has_alert "$1" "$2" && return 0; sleep 1; done; echo "❌ алерт $1 -> $2 не появился за 15 c"; return 1; }
wait_no_alert() { for i in $(seq 1 15); do has_alert "$1" "$2" || return 0; sleep 1; done; echo "❌ алерт $1 -> $2 не погас за 15 c"; return 1; }

m1="$(ect get /clusters/demo/shards/s1/master --print-value-only)"
[ "$m1" = "s1a:5432" ] \
  || { echo "❌ мастер s1 сейчас $m1 — сценарий требует s1a (перезапусти стенд: checks/90-down.sh -v && checks/00-up.sh)"; exit 1; }

# Act 1: отказ мастера
echo ">>> docker stop s1a (мастер s1)"
docker compose stop -t 3 s1a >/dev/null

# Assert 2: lease-ключи гаснут (TTL 5 c — запас 10 c)
for i in $(seq 1 10); do [ -z "$(ect get /clusters/demo/shards/s1/master --print-value-only 2>/dev/null)" ] && break; sleep 1; done
[ -z "$(ect get /clusters/demo/shards/s1/master --print-value-only 2>/dev/null)" ] \
  || { echo "❌ master-ключ s1 не погас (lease эмулятора hc1a жив?)"; exit 1; }
for i in $(seq 1 10); do [ -z "$(ect get /service/demo-s1/leader --print-value-only 2>/dev/null)" ] && break; sleep 1; done
[ -z "$(ect get /service/demo-s1/leader --print-value-only 2>/dev/null)" ] \
  || { echo "  ❌ leader-ключ demo-s1 не погас"; exit 1; }
for i in $(seq 1 10); do [ -z "$(ect get /service/demo-s1/optime/leader --print-value-only 2>/dev/null)" ] && break; sleep 1; done
[ -z "$(ect get /service/demo-s1/optime/leader --print-value-only 2>/dev/null)" ] \
  || { echo "  ❌ optime/leader demo-s1 не погас"; exit 1; }
echo "  lease-ключи s1 погасли: master, leader, optime (<=10 c)"

# Assert 3: панель видит оба алерта
wait_alert shard-no-master demo/s1; echo "  shard-no-master -> demo/s1"
wait_alert shard-no-leader demo-s1; echo "  shard-no-leader -> demo-s1"

# Act 4: promote s1b (+ снятие sync-имён: без реплики коммиты виснут — урок ../pg)
PGD="$(docker compose exec -T -u postgres s1b psql -U postgres -d postgres -tAc 'show data_directory' | tr -d '[:space:]')"
docker compose exec -T -u postgres s1b pg_ctl promote -D "$PGD" >/dev/null 2>&1 || true
for i in $(seq 1 60); do [ "$(sq s1b 'select pg_is_in_recovery()' 2>/dev/null)" = "f" ] && break; sleep 1; done
[ "$(sq s1b 'select pg_is_in_recovery()')" = "f" ] \
  || { echo "  ❌ s1b не вышла из recovery за 60 c (promote?)"; exit 1; }
sq s1b "ALTER SYSTEM SET synchronous_standby_names = ''" >/dev/null
sq s1b "SELECT pg_reload_conf()" >/dev/null
echo "  s1b повышен до мастера (sync-имена сняты)"

# Assert 5: эмулятор s1b взял lease; алерты гаснут; REST показывает нового мастера
for i in $(seq 1 10); do [ "$(ect get /clusters/demo/shards/s1/master --print-value-only 2>/dev/null)" = "s1b:5432" ] && break; sleep 1; done
[ "$(ect get /clusters/demo/shards/s1/master --print-value-only)" = "s1b:5432" ] \
  || { echo "❌ master-ключ не перешёл к s1b"; exit 1; }
ect get /service/demo-s1/leader --print-value-only | jq -e '.name == "s1b"' >/dev/null \
  || { echo "❌ leader не s1b"; exit 1; }
echo "  master-ключ и leader у s1b"
curl -fsS -o /dev/null http://127.0.0.1:8012/primary \
  || { echo "❌ hc1b /primary != 200"; exit 1; }
curl -fsS http://127.0.0.1:8011/cluster | jq -e \
  'any(.members[]; .name=="s1b" and .role=="master")
   and any(.members[]; .name=="s1a" and .state=="stopped")' >/dev/null \
  || { echo "❌ /cluster не показывает s1b-мастера / s1a-stopped"; exit 1; }
echo "  Patroni-REST: s1b master, s1a stopped"
wait_no_alert shard-no-master demo/s1; echo "  shard-no-master погас"
wait_no_alert shard-no-leader demo-s1; echo "  shard-no-leader погас"
# /api/ha обогащается пробами (тик 15 c) — новый мастер появляется в API
# с задержкой после перезаписи etcd-ключей: ждём свежей пробы.
ha_b_master() {
  api /api/ha/demo-s1 | jq -e 'any(.members[]; .name=="s1b" and .role=="master")' >/dev/null
}
for i in $(seq 1 25); do ha_b_master && break; sleep 1; done
ha_b_master || { echo "❌ /api/ha/demo-s1 не видит s1b мастером"; exit 1; }
echo "  /api/ha/demo-s1: s1b master"

# Act 6: rejoin s1a репликой (self-healing: пустой PGDATA -> клон от s1b)
echo ">>> rejoin: пересоздаю s1a репликой s1b"
docker compose rm -sf s1a >/dev/null
sq s1b "select pg_drop_replication_slot('s1a_phys')" >/dev/null 2>&1 || true
docker compose up -d s1a >/dev/null
ok=""
for i in $(seq 1 120); do
  [ "$(docker compose exec -T s1a psql -U postgres -d postgres -tAc 'select pg_is_in_recovery()' 2>/dev/null)" = "t" ] && { ok=1; break; }
  sleep 2
done
[ -n "$ok" ] || { echo "❌ s1a не поднялась репликой за 240 c (docker compose logs s1a)"; exit 1; }
echo "  s1a в recovery (клон s1b)"
sq s1b "ALTER SYSTEM SET synchronous_standby_names = 'FIRST 1 (s1a)'" >/dev/null
sq s1b "SELECT pg_reload_conf()" >/dev/null
st=""
for i in $(seq 1 30); do
  st="$(sq s1b "select sync_state from pg_stat_replication where application_name='s1a'")"
  [ "$st" = "sync" ] && break
  sleep 1
done
[ "$st" = "sync" ] || { echo "❌ s1a не sync-standby у s1b"; exit 1; }
echo "  s1b: sync-standby s1a -> sync"

echo "✓ failover-цикл зелёный: алерт -> promote -> гашение -> rejoin"
