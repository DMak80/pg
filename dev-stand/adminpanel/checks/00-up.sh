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

# mTLS API kafkaworker (t03, arch/16 §1.1): per-install TLS-пакет — генерируем
# идемпотентно (только если ca.pem отсутствует); panel.crt/ca.pem уходят панели,
# server.* + ca.pem — воркеру (bind ../../deploy/tls в стендовом compose).
if [ ! -f "$ROOT/deploy/tls/ca.pem" ]; then
  echo ">>> генерирую per-install TLS-пакет (deploy/tls/gen.sh)"
  bash "$ROOT/deploy/tls/gen.sh"
fi

echo ">>> поднимаю стенд (docker compose --profile full --profile kafka up -d --build)"
docker compose --profile full --profile kafka up -d --build 2>&1 | tail -5

ect() { docker compose exec -T etcd etcdctl --endpoints=http://localhost:2379 "$@"; }
# Запрос — только через -c: позиционный аргумент psql трактуется как DBNAME
sq()   { docker compose exec -T "$1" psql -U postgres -d postgres -qAt -v ON_ERROR_STOP=1 -c "$2"; }

# 1) etcd жив
for i in $(seq 1 60); do ect endpoint health >/dev/null 2>&1 && break; sleep 1; done
ect endpoint health >/dev/null 2>&1 \
  || { echo "  ❌ etcd не стал здоровым за 60 c (docker compose logs etcd)"; exit 1; }
echo "  etcd ready"

# 1b) PgWorker (стенд = полная система; контур ВСЕГДА один — etcd стенда):
#     воркер из deploy/docker-compose.yml ходит в as-etcd через хост-2379
#     (PGW_ETCD_ENDPOINT=host.docker.internal:2379 — advertise as-etcd);
#     Patroni-ноды, которые он создаёт, ходят в DCS по тому же advertise.
#     Секреты per-install — deploy/.env (нет файла → dev-шаблон .env.example;
#     deploy/.env в .gitignore). Поднимается ДО сида: pg-сид наливается его
#     API POST /api/seed/demo (spec §3.5). force-recreate + --build: контейнер
#     deploy-проекта переживает 90-down (другой compose-проект) и поднимался бы
#     из УСТАРЕВШЕГО образа pgworker:dev (например, сид сеял бы старые аномалии),
#     а его etcd-клиент держит кеш DNS/коннектов умершего etcd — свежий процесс
#     из свежего образа надёжнее.
ROOT="$(cd ../.. && pwd)"
[ -f "$ROOT/deploy/.env" ] || cp "$ROOT/deploy/.env.example" "$ROOT/deploy/.env"
( cd "$ROOT/deploy" && docker compose --env-file "$ROOT/deploy/.env" up -d --build --force-recreate pgworker 2>&1 | tail -2 )
for i in $(seq 1 60); do curl -fsS -m 3 http://localhost:8080/healthz >/dev/null 2>&1 && break; sleep 1; done
curl -fsS -m 3 http://localhost:8080/healthz >/dev/null \
  || { echo "❌ pgworker не ожил за 60 c (:8080/healthz; docker logs deploy-pgworker-1)"; exit 1; }
echo "  pgworker жив (:8080/healthz, общий etcd-контур)"

# 1c) pg-сид — ЧЕРЕЗ API воркера (spec §3.5; прямой etcdctl-сид упразднён):
#     05-seed.sh идемпотентно ждёт /healthz и зовёт POST /api/seed/demo,
#     затем проверяем ключ контроль-плейна как раньше.
"$PWD/checks/05-seed.sh" pg
for i in $(seq 1 30); do
  [ -n "$(ect get /clusters/demo/config --print-value-only 2>/dev/null)" ] && break
  sleep 1
done
[ -n "$(ect get /clusters/demo/config --print-value-only 2>/dev/null)" ] \
  || { echo "❌ сид не появился за 30 c (curl -X POST http://localhost:8080/api/seed/demo)"; exit 1; }
echo "  сид контроль-плейна на месте (налит через API pgworker)"

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

# 6) инвентарь: схемы ВСЕХ бакетов владельца по routing (adopt-repair: сид
#    больше не сеет аномалий — все 16 ACTIVE; 10 на s1, 6 на s2)
schemas() { # master "список бакетов"
  for b in $2; do
    docker compose exec -T "$1" psql -U postgres -d demo -qAt \
      -c "CREATE SCHEMA IF NOT EXISTS bucket_$b" >/dev/null
  done
}
schemas "$master1" "0 2 3 4 6 8 10 11 12 14"
schemas "$master2" "1 5 7 9 13 15"
echo "  инвентарь: 10 схем на $master1, 6 на $master2"

# 7) kafkaworker жив: heartbeat /kafkaworker/instances/* (lease TTL — ключ
#    исчезает со смертью воркера). 50-й наливает kafka-сид ЧЕРЕЗ API живого
#    воркера (05-seed.sh kafka) и останавливает его финальным шагом (spec §3.5).
for i in $(seq 1 60); do
  [ -n "$(ect get /kafkaworker/instances/ --prefix --keys-only 2>/dev/null | head -1)" ] && break
  sleep 1
done
[ -n "$(ect get /kafkaworker/instances/ --prefix --keys-only 2>/dev/null | head -1)" ] \
  || { echo "❌ kafkaworker не ожил за 60 c (docker compose logs kafkaworker)"; exit 1; }
echo "  kafkaworker жив (heartbeat /kafkaworker/instances/*)"

# 8) панель жива: всегда в докере (AGENTS.md), сервис adminpanel сети стенда,
#    /api/healthz опубликован на :5050.
for i in $(seq 1 60); do curl -fsS http://localhost:5050/api/healthz >/dev/null 2>&1 && break; sleep 1; done
curl -fsS http://localhost:5050/api/healthz >/dev/null 2>&1 \
  || { echo "❌ панель не ожила за 60 c на :5050 (docker compose logs adminpanel)"; exit 1; }
echo "  панель жива (http://localhost:5050, docker)"

echo "✓ стенд поднят (полная система: панель + PG + kafka + PgWorker, контур один)"
