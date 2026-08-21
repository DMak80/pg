#!/usr/bin/env bash
# Поднятие стенда: шард1 (s1a+s1b+hap1), шард2 (s2a+s2b+hap2), etcd.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p logs

# Arrange: контейнеры
docker compose up -d --build 2>&1 | tail -8

# Act: ждём готовности PG и репликации
until docker exec s1a pg_isready -U postgres -q; do sleep 1; done
echo "  s1a ready"

# trust-метод добавляет только "host all all all trust"; репликации (pg_basebackup
# со стороны реплики) нужна своя строка — патчим и перезагружаем конфиг
patch_hba() {
  docker exec "$1" bash -c \
    'grep -q "host replication all all trust" $PGDATA/pg_hba.conf || echo "host replication all all trust" >> $PGDATA/pg_hba.conf
     psql -U postgres -d postgres -qtAc "select pg_reload_conf()" >/dev/null'
}
patch_hba s1a
echo "  pg_hba s1a: replication-trust добавлен"
until docker exec s2a pg_isready -U postgres -q; do sleep 1; done
echo "  s2a ready"
patch_hba s2a
echo "  pg_hba s2a: replication-trust добавлен"

for c in s1b s2b; do
  until docker exec "$c" pg_isready -U postgres -q; do sleep 1; done
  echo "  $c ready"
done
for c in s1b s2b; do
  until [ "$(docker exec "$c" psql -U postgres -tAc 'select pg_is_in_recovery()')" = "t" ]; do
    sleep 1
  done
  echo "  $c в recovery (реплика своего шарда)"
done

until docker exec etcd etcdctl endpoint health --endpoints=http://localhost:2379 >/dev/null 2>&1; do
  sleep 1
done
echo "  etcd ready (контрол-плейн)"

# Assert: HAProxy ведёт write-трафик на текущих мастерах
ip() { docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$1"; }
for pair in "hap1 s1a $(ip s1a)" "hap2 s2a $(ip s2a)"; do
  set -- $pair
  hap="$1" node="$2" want="$3"
  got=""
  for i in $(seq 1 30); do
    got="$(docker exec s2a psql -h "$hap" -U postgres -tAc 'select inet_server_addr()' 2>/dev/null || true)"
    [ "$got" = "$want" ] && break
    sleep 1
  done
  echo "  $hap:5432 -> $got"
  [ "$got" = "$want" ] || { echo "❌ HAProxy $hap не ведёт на $node"; exit 1; }
done

# Assert/Arrange: sync-standby подключён (предусловие P8: remote_apply у подписок
# переездов). Имена ставим ALTER SYSTEM'ом — НЕ флагами -c (флаг сильнее ALTER
# SYSTEM, и после промоушена без реплики коммиты зависнут в SyncRep навсегда).
for pair in "s1a s1b" "s2a s2b"; do
  set -- $pair
  master="$1" rep="$2"
  docker exec "$master" psql -U postgres -d postgres \
    -c "ALTER SYSTEM SET synchronous_standby_names = 'FIRST 1 ($rep)'" \
    -c "SELECT pg_reload_conf()" >/dev/null
  st=""
  for i in $(seq 1 30); do
    st="$(docker exec "$master" psql -U postgres -d postgres -tAc \
      "select sync_state from pg_stat_replication where application_name='$rep'")"
    [ "$st" = "sync" ] && break
    sleep 1
  done
  echo "  $master: sync-standby $rep → ${st:-нет}"
  [ "$st" = "sync" ] || { echo "❌ $rep не sync-standby у $master (P8-предусловие)"; exit 1; }
done
echo "✓ стенд поднят: $(docker exec s1a psql -U postgres -tAc 'show server_version' | tr -d ' ')"
