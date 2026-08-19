#!/usr/bin/env bash
# Поднятие стенда: шард1 (s1a мастер + s1b реплика + hap1) и шард2 (s2).
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p logs

# Arrange: контейнеры
docker compose up -d --build 2>&1 | tail -8

# Act: ждём готовности PG и репликации
until docker exec s1a pg_isready -U postgres -q; do sleep 1; done
echo "  s1a ready"

# trust-метод добавляет только "host all all all trust"; репликации (pg_basebackup
# со стороны s1b) нужна своя строка — патчим и перезагружаем конфиг
docker exec s1a bash -c \
  'grep -q "host replication all all trust" $PGDATA/pg_hba.conf || echo "host replication all all trust" >> $PGDATA/pg_hba.conf
   psql -U postgres -d postgres -qtAc "select pg_reload_conf()" >/dev/null'
echo "  pg_hba: replication-trust добавлен"

for c in s1b s2; do
  until docker exec "$c" pg_isready -U postgres -q; do sleep 1; done
  echo "  $c ready"
done
until [ "$(docker exec s1b psql -U postgres -tAc 'select pg_is_in_recovery()')" = "t" ]; do
  sleep 1
done
echo "  s1b в recovery (реплика шарда 1)"

# Assert: HAProxy ведёт write-трафик на s1a
ip_s1a="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' s1a)"
got=""
for i in $(seq 1 30); do
  got="$(docker exec s2 psql -h hap1 -U postgres -tAc 'select inet_server_addr()' 2>/dev/null || true)"
  [ "$got" = "$ip_s1a" ] && break
  sleep 1
done
echo "  hap1:5432 -> $got"
[ "$got" = "$ip_s1a" ] || { echo "❌ HAProxy не ведёт на s1a"; exit 1; }
echo "✓ стенд поднят: $(docker exec s1a psql -U postgres -tAc 'show server_version' | tr -d ' ')"
