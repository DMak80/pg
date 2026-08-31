#!/usr/bin/env bash
# Подъём полного стенда (профили full + kafka) и приведение в рабочее
# состояние: реплики, sync-standby, инвентарь схем (spec t10 §7.1), живой
# kafkaworker. Управление кафкой входит в стенд всегда (не только e2e-гейтом):
# без воркера kafka-домен панели глух — отсюда «глупые» алерты и разборы.
set -euo pipefail
cd "$(dirname "$0")/.."

# Arrange: инструменты хоста
for bin in docker jq curl; do
  command -v "$bin" >/dev/null || { echo "❌ нет $bin в PATH"; exit 1; }
done

echo ">>> поднимаю стенд (docker compose --profile full --profile kafka up -d --build)"
docker compose --profile full --profile kafka up -d --build 2>&1 | tail -5

ect() { docker compose exec -T etcd etcdctl --endpoints=http://localhost:2379 "$@"; }
# Запрос — только через -c: позиционный аргумент psql трактуется как DBNAME
sq()   { docker compose exec -T "$1" psql -U postgres -d postgres -qAt -v ON_ERROR_STOP=1 -c "$2"; }

# 1) etcd жив и сид на месте (спец: seed идемпотентен, ключи не портит)
for i in $(seq 1 60); do ect endpoint health >/dev/null 2>&1 && break; sleep 1; done
ect endpoint health >/dev/null 2>&1 \
  || { echo "  ❌ etcd не стал здоровым за 60 c (docker compose logs etcd)"; exit 1; }
echo "  etcd ready"
for i in $(seq 1 30); do
  [ -n "$(ect get /clusters/demo/config --print-value-only 2>/dev/null)" ] && break
  sleep 1
done
[ -n "$(ect get /clusters/demo/config --print-value-only 2>/dev/null)" ] \
  || { echo "❌ сид не появился за 30 c (сервис seed: docker compose logs seed)"; exit 1; }
echo "  сид контроль-плейна на месте"

# 2) PG-ноды готовы; hba-replication (нужен basebackup/rejoin — паттерн ../pg).
#    Порядок как в spec §7.1: сначала мастера *a -> patch_hba -> реплики *b
#    (pg_basebackup реплик не пройдёт без replication-строки на мастере).
for c in s1a s2a; do
  for i in $(seq 1 60); do
    docker compose exec -T "$c" pg_isready -U postgres -q 2>/dev/null && break
    sleep 1
  done
  docker compose exec -T "$c" pg_isready -U postgres -q 2>/dev/null \
    || { echo "  ❌ $c не готов за 60 c (docker compose logs $c)"; exit 1; }
  echo "  $c ready"
done
patch_hba() {
  docker compose exec -T "$1" bash -c \
    'grep -q "host replication all all trust" $PGDATA/pg_hba.conf || echo "host replication all all trust" >> $PGDATA/pg_hba.conf;
     psql -U postgres -d postgres -qtAc "select pg_reload_conf()" >/dev/null'
}
patch_hba s1a; patch_hba s2a
echo "  pg_hba: replication-trust добавлен мастерам (s1a, s2a)"
for c in s1b s2b; do
  for i in $(seq 1 90); do
    docker compose exec -T "$c" pg_isready -U postgres -q 2>/dev/null && break
    sleep 1
  done
  docker compose exec -T "$c" pg_isready -U postgres -q 2>/dev/null \
    || { echo "  ❌ $c не готов за 90 c (docker compose logs $c)"; exit 1; }
  echo "  $c ready"
done
patch_hba s1b; patch_hba s2b
echo "  pg_hba: replication-trust добавлен репликам (s1b, s2b)"

# 3) реплики в recovery (базовый basebackup идёт с retry в command-скриптах нод)
for c in s1b s2b; do
  for i in $(seq 1 120); do
    [ "$(sq "$c" 'select pg_is_in_recovery()' 2>/dev/null)" = "t" ] && break
    sleep 2
  done
  [ "$(sq "$c" 'select pg_is_in_recovery()' 2>/dev/null)" = "t" ] \
    || { echo "❌ $c не стала репликой за 240 c (docker compose logs $c)"; exit 1; }
  echo "  $c в recovery (реплика своего шарда)"
done

# 4) эмуляторы зарегистрировались: lease-ключи /cluster/nodes + master шардов
for c in s1a s1b s2a s2b; do
  for i in $(seq 1 30); do
    [ -n "$(ect get "/cluster/nodes/$c" --print-value-only 2>/dev/null)" ] && break
    sleep 1
  done
  [ -n "$(ect get "/cluster/nodes/$c" --print-value-only 2>/dev/null)" ] \
    || { echo "❌ $c не зарегистрирован в /cluster/nodes (эмулятор hc: docker compose logs hc*)"; exit 1; }
done
echo "  эмуляторы: /cluster/nodes/* живы (lease TTL 5 c)"
m1="$(ect get /clusters/demo/shards/s1/master --print-value-only)"
m2="$(ect get /clusters/demo/shards/s2/master --print-value-only)"
[ -n "$m1" ] && [ -n "$m2" ] \
  || { echo "  ❌ master-ключ шарда пуст (s1='$m1' s2='$m2' — эмулятор мастера не зашёл в цикл?)"; exit 1; }
echo "  master s1=$m1 s2=$m2"

# 5) sync-standby: имена ALTER SYSTEM'ом (НЕ флагами -c — ловушка SyncRep,
#    урок ../pg: после promote без реплики коммиты виснут)
set_sync() { # master replica
  docker compose exec -T "$1" psql -U postgres -d postgres -qAt \
    -c "ALTER SYSTEM SET synchronous_standby_names = 'FIRST 1 ($2)'" \
    -c "SELECT pg_reload_conf()" >/dev/null
  st=""
  for i in $(seq 1 30); do
    st="$(sq "$1" "select sync_state from pg_stat_replication where application_name='$2'")"
    [ "$st" = "sync" ] && break
    sleep 1
  done
  [ "$st" = "sync" ] || { echo "❌ $2 не sync-standby у $1 (было: ${st:-нет})"; exit 1; }
  echo "  $1: sync-standby $2 -> sync"
}
master1="${m1%:*}"; rep1=s1b; [ "$master1" = s1b ] && rep1=s1a
master2="${m2%:*}"; rep2=s2b; [ "$master2" = s2b ] && rep2=s2a
set_sync "$master1" "$rep1"
set_sync "$master2" "$rep2"

# 6) инвентарь: схемы bucket_% только ACTIVE-бакетов владельца (spec §6:
#    inventory-mismatch сверяет только ACTIVE; 8 на s1, 5 на s2)
schemas() { # master "список бакетов"
  for b in $2; do
    docker compose exec -T "$1" psql -U postgres -d demo -qAt \
      -c "CREATE SCHEMA IF NOT EXISTS bucket_$b" >/dev/null
  done
}
schemas "$master1" "0 2 4 6 8 10 12 14"
schemas "$master2" "1 5 9 13 15"
echo "  инвентарь: 8 схем на $master1, 5 на $master2"

# 7) kafkaworker жив: heartbeat /kafkaworker/instances/* (lease TTL — ключ
#    исчезает со смертью воркера). Сид (чек 50) с живым воркером несовместим —
#    прогон 50-го сам останавливает воркера перед наливкой сида.
for i in $(seq 1 60); do
  [ -n "$(ect get /kafkaworker/instances/ --prefix --keys-only 2>/dev/null | head -1)" ] && break
  sleep 1
done
[ -n "$(ect get /kafkaworker/instances/ --prefix --keys-only 2>/dev/null | head -1)" ] \
  || { echo "❌ kafkaworker не ожил за 60 c (docker compose logs kafkaworker)"; exit 1; }
echo "  kafkaworker жив (heartbeat /kafkaworker/instances/*)"

echo "✓ стенд поднят (PG + kafka)"
