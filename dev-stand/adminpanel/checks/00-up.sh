#!/usr/bin/env bash
# Подъём полного стенда (профили full + kafka + valkey) и приведение в рабочее
# состояние: реплики, sync-standby, инвентарь схем (spec t10 §7.1), живой
# kafkaworker, живой valkeyworker + сид demo (t03). Управление кафкой входит в
# стенд всегда (не только e2e-гейтом): без воркера kafka-домен панели глух —
# отсюда «глупые» алерты и разборы.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$(cd ../.. && pwd)"
# Хост-публикация API pgworker параметризуется (коллизии портов на хосте;
# канон 8080). Advertise обязан совпадать с фактической публикацией (панель
# и чеки стучатся по advertise), prometheus-target — файл file_sd.
export PGW_API_HOST_PORT="${PGW_API_HOST_PORT:-8080}"
[ "${PGW_API_ADVERTISE_URL:-}" ] || export PGW_API_ADVERTISE_URL="https://host.docker.internal:${PGW_API_HOST_PORT}"
printf '[{"targets": ["host.docker.internal:%s"]}]\n' "$PGW_API_HOST_PORT" \
  > metrics/prometheus/pgworker-targets.json

# Arrange: инструменты хоста
for bin in docker jq curl; do
  command -v "$bin" >/dev/null || { echo "❌ нет $bin в PATH"; exit 1; }
done

# mTLS API воркеров (t03, arch/16 §1.1 / arch/21 §1.1): per-install TLS-пакет —
# gen.sh идемпотентен ПОФАЙЛОВО (ca.pem жив — клиентские/pgserver не трогаются;
# server-серт без DNS:valkeyworker перегенерируется, t03), поэтому зовём
# безусловно: на свежем хосте создаёт пакет, на живом — только чинит старый
# server-серт; panel.crt/ca.pem уходят панели, server.* + ca.pem — воркерам
# (bind ../../deploy/tls в стендовом compose).
echo ">>> TLS-пакет (deploy/tls/gen.sh — идемпотентно)"
bash "$ROOT/deploy/tls/gen.sh"

# Наполнение deploy-volume pgw-api-tls пакетом (ro-монтирование воркером);
# имя volume — с префиксом compose-проекта deploy (как его создаёт compose).
docker volume create deploy_pgw-api-tls >/dev/null
docker run --rm \
  -v "$ROOT/deploy/tls:/src:ro" -v deploy_pgw-api-tls:/tls alpine:3.20 \
  sh -c "cp /src/ca.pem /src/pgserver.crt /src/pgserver.key /src/healthcheck.crt /src/healthcheck.key /tls/"

echo ">>> поднимаю стенд (docker compose --profile full --profile kafka --profile valkey --profile metrics up -d --build)"
# Docker Desktop отдаёт хост-порт recreated-контейнера с задержкой (com.docke
# держит публикацию после удаления старого контейнера; при пересборке образа
# recreate стабилен — ID меняется метаданными даже на кэшированных слоях) —
# ретрай compose up, иначе подъём падает на «port is already allocated».
compose_up_ok=0
for _ in 1 2 3; do
  if docker compose --profile full --profile kafka --profile valkey --profile metrics up -d --build 2>&1 | tail -5; then
    compose_up_ok=1; break
  fi
  echo "  compose up не удался (порт не отдан после recreate) — пауза 10 c"; sleep 10
done
[ "$compose_up_ok" = 1 ] || { echo "❌ стенд не поднялся за 3 попытки (docker compose logs kafkaworker)"; exit 1; }

ect() { docker compose exec -T etcd etcdctl --endpoints=http://localhost:2379 "$@"; }
# Запрос — только через -c: позиционный аргумент psql трактуется как DBNAME
sq()   { docker compose exec -T "$1" psql -U postgres -d postgres -qAt -v ON_ERROR_STOP=1 -c "$2"; }

# 1) etcd жив
for i in $(seq 1 60); do ect endpoint health >/dev/null 2>&1 && break; sleep 1; done
ect endpoint health >/dev/null 2>&1 \
  || { echo "  ❌ etcd не стал здоровым за 60 c (docker compose logs etcd)"; exit 1; }
echo "  etcd ready"

# 1a) MinIO (S3 бэкапов, arch/19): healthy + стендовый bucket pgworker-backups
#      (идемпотентный сид mc mb --ignore-existing; креды — стендовые дефолты;
#       entrypoint образа mc = mc, shell зовём через --entrypoint /bin/sh).
for i in $(seq 1 60); do curl -fsS http://localhost:9000/minio/health/live >/dev/null 2>&1 && break; sleep 1; done
curl -fsS http://localhost:9000/minio/health/live >/dev/null 2>&1 \
  || { echo "  ❌ as-minio не стал здоровым за 60 c (docker compose logs minio)"; exit 1; }
minio_net="$(docker inspect as-minio -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}')"
#      (mc запинен — как и WAL-агенты E2E; зеркало в локальном registry, см. docs/runbook.md)
docker run --rm --entrypoint /bin/sh --network "$minio_net" minio/mc:RELEASE.2025-08-13T08-35-41Z \
  -c "mc alias set standup http://as-minio:9000 minioadmin minioadmin >/dev/null && mc mb --ignore-existing standup/pgworker-backups >/dev/null" \
  || { echo "  ❌ bucket pgworker-backups не создан (mc против as-minio)"; exit 1; }
echo "  as-minio жив, bucket pgworker-backups готов (:9000 API / :9001 консоль)"

# 1a-2) Образ джоба бэкапов (t02): собирается рядом с pgworker:dev — тег
#       PgWorker:Backups:Job:Image (дефолт pgworker-backup:dev). Прямой docker
#       build (а не compose build) — без env-зависимостей deploy/.env.
docker build -q -f "$ROOT/docker/PgWorker.Backup.Dockerfile" -t pgworker-backup:dev "$ROOT" \
  || { echo "❌ образ pgworker-backup не собрался (docker/PgWorker.Backup.Dockerfile)"; exit 1; }
echo "  образ pgworker-backup:dev готов"

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
[ -f "$ROOT/deploy/.env" ] || cp "$ROOT/deploy/.env.example" "$ROOT/deploy/.env"
# Синхронизация хост-порта API в .env: пересоздания pgworker из чеков
# (05-seed force-recreate) в свежих оболочках интерполируют compose из .env —
# bind обязан совпасть с уже занятой публикацией (чеки порт читают из .env).
if grep -q '^PGW_API_HOST_PORT=' "$ROOT/deploy/.env"; then
  sed -i.bak "s/^PGW_API_HOST_PORT=.*/PGW_API_HOST_PORT=$PGW_API_HOST_PORT/" "$ROOT/deploy/.env" && rm -f "$ROOT/deploy/.env.bak"
else
  printf 'PGW_API_HOST_PORT=%s\n' "$PGW_API_HOST_PORT" >> "$ROOT/deploy/.env"
fi
# тот же race порта (Docker Desktop отдаёт публикацию с задержкой) — ретрай.
pg_up_ok=0
for _ in 1 2 3; do
  if ( cd "$ROOT/deploy" && docker compose --env-file "$ROOT/deploy/.env" up -d --build --force-recreate pgworker 2>&1 | tail -2 ); then
    pg_up_ok=1; break
  fi
  echo "  pgworker up не удался (порт не отдан после recreate) — пауза 15 c"; sleep 15
done
[ "$pg_up_ok" = 1 ] || { echo "❌ pgworker не поднялся за 3 попытки (docker logs deploy-pgworker-1)"; exit 1; }
MTLS="curl -fsS -m 3 --cacert $ROOT/deploy/tls/ca.pem --cert $ROOT/deploy/tls/healthcheck.crt --key $ROOT/deploy/tls/healthcheck.key"
for i in $(seq 1 60); do $MTLS https://localhost:${PGW_API_HOST_PORT:-8080}/healthz >/dev/null 2>&1 && break; sleep 1; done
$MTLS https://localhost:${PGW_API_HOST_PORT:-8080}/healthz >/dev/null \
  || { echo "❌ pgworker не ожил за 60 c (https :${PGW_API_HOST_PORT:-8080}/healthz по mTLS; docker logs deploy-pgworker-1)"; exit 1; }
echo "  pgworker жив (https :${PGW_API_HOST_PORT:-8080}/healthz, mTLS, общий etcd-контур)"

# 1c) pg-сид — ЧЕРЕЗ API воркера (spec §3.5; прямой etcdctl-сид упразднён):
#     05-seed.sh идемпотентно ждёт /healthz и зовёт POST /api/seed/demo,
#     затем проверяем ключ контроль-плейна как раньше.
"$PWD/checks/05-seed.sh" pg
for i in $(seq 1 30); do
  [ -n "$(ect get /clusters/demo/config --print-value-only 2>/dev/null)" ] && break
  sleep 1
done
[ -n "$(ect get /clusters/demo/config --print-value-only 2>/dev/null)" ] \
  || { echo "❌ сид не появился за 30 c (curl -X POST https://localhost:${PGW_API_HOST_PORT:-8080}/api/seed/demo)"; exit 1; }
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

# 7b) valkeyworker жив (t03): heartbeat lease-ключ /valkeyworker/api/* — его
#     ждут панель (WorkerEndpoints) и чек 51 (мутации через панель→воркер).
for i in $(seq 1 60); do
  [ -n "$(ect get /valkeyworker/api/ --prefix --keys-only 2>/dev/null | head -1)" ] && break
  sleep 1
done
[ -n "$(ect get /valkeyworker/api/ --prefix --keys-only 2>/dev/null | head -1)" ] \
  || { echo "❌ valkeyworker не ожил за 60 c (docker compose logs valkeyworker)"; exit 1; }
echo "  valkeyworker жив (heartbeat /valkeyworker/api/*)"

# 7c) valkey-сид (t03): демо-кластер demo наливается ЧЕРЕЗ API живого воркера —
#     метрика spec §8.1: после ПОЛНОГО 00-up.sh панель /valkey уже показывает
#     demo (Active, RUNNING, endpoints, live) — без отдельного запуска чека.
#     05-seed.sh идемпотентен (SeedDemoHandler: живой config → 200 no-op),
#     wait до Active — внутри seed-функции; прецедент — pg-контур (00-up.sh
#     сам наливает pg-сид через API pgworker). Воркер продолжает жить.
"$PWD/checks/05-seed.sh" valkey

# 8) панель жива: всегда в докере (AGENTS.md), сервис adminpanel сети стенда,
#    /api/healthz опубликован на :5050.
for i in $(seq 1 60); do curl -fsS http://localhost:5050/api/healthz >/dev/null 2>&1 && break; sleep 1; done
curl -fsS http://localhost:5050/api/healthz >/dev/null 2>&1 \
  || { echo "❌ панель не ожила за 60 c на :5050 (docker compose logs adminpanel)"; exit 1; }
echo "  панель жива (http://localhost:5050, docker)"

# 9) мониторинг (профиль metrics): Prometheus/Grafana/Alertmanager живы; таргеты
#    прогреваются scrape-интервалом — готовность проверяет 65-metrics.sh.
for i in $(seq 1 60); do curl -fsS -m 3 "http://localhost:${METRICS_PROMETHEUS_PORT:-9090}/-/ready" >/dev/null 2>&1 && break; sleep 1; done
curl -fsS -m 3 "http://localhost:${METRICS_PROMETHEUS_PORT:-9090}/-/ready" >/dev/null \
  || { echo "❌ prometheus не готов за 60 c (docker compose logs prometheus)"; exit 1; }
echo "  prometheus готов (:${METRICS_PROMETHEUS_PORT:-9090})"

echo "✓ стенд поднят (полная система: панель + PG + kafka + valkey + PgWorker + мониторинг, контур один)"
