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
# канон 8080, второй инстанс — 8083, t07). Advertise обязан совпадать с
# фактической публикацией (панель и чеки стучатся по advertise),
# prometheus-target — файл file_sd (оба инстанса).
export PGW_API_HOST_PORT="${PGW_API_HOST_PORT:-8080}"
export PGW_API_HOST_PORT2="${PGW_API_HOST_PORT2:-8083}"
[ "${PGW_API_ADVERTISE_URL:-}" ] || export PGW_API_ADVERTISE_URL="https://host.docker.internal:${PGW_API_HOST_PORT}"
# t15: таргеты джобы pgworker — сетевые адреса deploy-воркеров (aliases в
# pgw-metrics); host-порты PGW_API_HOST_PORT* остаются — advertise/чеки/панель,
# не скрейп (arch/18 §5.2).
printf '[{"targets": ["pgworker:8080", "pgworker-2:8080"]}]\n' \
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
# ca.key — выпуск REST-сертов нод (R16: бэкап пакета — runbook)
docker run --rm \
  -v "$ROOT/deploy/tls:/src:ro" -v deploy_pgw-api-tls:/tls alpine:3.20 \
  sh -c "cp /src/ca.pem /src/ca.key /src/pgserver.crt /src/pgserver.key /src/healthcheck.crt /src/healthcheck.key /tls/"

# SdGenerator (t15): publish НА ХОСТЕ → compose build пакует только вывод
# (канон E2E-образов; сборка не «тихая» — [PHASE] и тайминг).
echo ">>> [PHASE] publish Metrics.SdGenerator ($(date +%H:%M:%S))"
dotnet publish "$ROOT/src/Metrics.SdGenerator/Metrics.SdGenerator.csproj" \
  -c Release -o "$ROOT/artifacts/sd-generator/publish" --nologo \
  || { echo "❌ publish SdGenerator не удался"; exit 1; }
echo ">>> [PHASE] publish SdGenerator готов ($(date +%H:%M:%S))"

# ownS3 (t39, arch/owns3/05 §6): publish НА ХОСТЕ → compose build пакует только
# вывод (канон E2E-образов; сборка не «тихая» — [PHASE] и тайминг).
echo ">>> [PHASE] publish OwnS3.App ($(date +%H:%M:%S))"
dotnet publish "$ROOT/src/OwnS3.App/OwnS3.App.csproj" \
  -c Release -o "$ROOT/artifacts/owns3/publish" --nologo \
  || { echo "❌ publish OwnS3 не удался"; exit 1; }
echo ">>> [PHASE] publish OwnS3 готов ($(date +%H:%M:%S))"

# WAL-агент (t41): publish НА ХОСТЕ → образ publish-COPY (канон E2E-образов;
# сборка не «тихая» — [PHASE] и тайминг).
echo ">>> [PHASE] publish PgWorker.WalReceiver ($(date +%H:%M:%S))"
dotnet publish "$ROOT/src/PgWorker.WalReceiver/PgWorker.WalReceiver.csproj" \
  -c Release -o "$ROOT/artifacts/wal/publish" --nologo \
  || { echo "❌ publish WalReceiver не удался"; exit 1; }
docker build -q -f "$ROOT/docker/PgWorker.Wal.Dockerfile" -t pgworker-wal:dev "$ROOT/artifacts/wal/publish" \
  || { echo "❌ образ pgworker-wal:dev не собрался"; exit 1; }
echo "  образ pgworker-wal:dev готов"

# Секреты per-install — deploy/.env (нет файла → dev-шаблон .env.example;
# deploy/.env в .gitignore; t41: стенд задаёт PGW_CONTOUR_NETWORK/
# METRICS-ключи = стендовая сеть — шаблон уже несёт).
[ -f "$ROOT/deploy/.env" ] || cp "$ROOT/deploy/.env.example" "$ROOT/deploy/.env"
# Синхронизация хост-порта API в .env: пересоздания pgworker из чеков
# (05-seed force-recreate) в свежих оболочках интерполируют compose из .env —
# bind обязан совпасть с уже занятой публикацией (чеки порт читают из .env).
if grep -q '^PGW_API_HOST_PORT=' "$ROOT/deploy/.env"; then
  sed -i.bak "s/^PGW_API_HOST_PORT=.*/PGW_API_HOST_PORT=$PGW_API_HOST_PORT/" "$ROOT/deploy/.env" && rm -f "$ROOT/deploy/.env.bak"
else
  printf 'PGW_API_HOST_PORT=%s\n' "$PGW_API_HOST_PORT" >> "$ROOT/deploy/.env"
fi
# t07: тот же sync для второго инстанса — чеки читают порт из .env.
if grep -q '^PGW_API_HOST_PORT2=' "$ROOT/deploy/.env"; then
  sed -i.bak "s/^PGW_API_HOST_PORT2=.*/PGW_API_HOST_PORT2=$PGW_API_HOST_PORT2/" "$ROOT/deploy/.env" && rm -f "$ROOT/deploy/.env.bak"
else
  printf 'PGW_API_HOST_PORT2=%s\n' "$PGW_API_HOST_PORT2" >> "$ROOT/deploy/.env"
fi
# t15/t41: единая сеть контура = стендовая сеть (adminpanel-stand-net);
# отдельная pgw-metrics на стенде НЕ создаётся (весь стенд в одной сети).
for k in PGW_METRICS_NETWORK PGW_SCRAPE_NETWORK; do
  v="${!k:-adminpanel-stand-net}"
  if grep -q "^$k=" "$ROOT/deploy/.env"; then
    sed -i.bak "s/^$k=.*/$k=$v/" "$ROOT/deploy/.env" && rm -f "$ROOT/deploy/.env.bak"
  else
    printf '%s=%s\n' "$k" "$v" >> "$ROOT/deploy/.env"
  fi
done

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

# deploy-контур (воркеры + owns3) — в стендовую сеть (контур-override t41;
# владелец сети adminpanel-stand-net — стендовый compose выше, deploy
# подключается external; отдельная pgw-metrics не создаётся). force-recreate
# + --build: свежий процесс из свежего образа надёжнее (кеш DNS/коннектов
# умершего etcd); healthz/api-ключи — ниже в §1b.
pg_up_ok=0
for _ in 1 2 3; do
  if ( cd "$ROOT/deploy" && docker compose -f docker-compose.yml -f docker-compose.contour.yml \
       --env-file "$ROOT/deploy/.env" up -d --build --force-recreate pgworker pgworker-2 owns3 2>&1 | tail -2 ); then
    pg_up_ok=1; break
  fi
  echo "  deploy up не удался (порт не отдан после recreate) — пауза 15 c"; sleep 15
done
[ "$pg_up_ok" = 1 ] || { echo "❌ deploy-контур не поднялся за 3 попытки (docker logs deploy-pgworker-1)"; exit 1; }

# ownS3 (t39): готовность — healthz-проба хост-публикации (порт синхронен с
#     deploy/.env, дефолт 9010).
OWNS3_PORT="$(grep -E '^OWNS3_HOST_PORT=' "$ROOT/deploy/.env" 2>/dev/null | tail -1 | cut -d= -f2 || true)"
OWNS3_PORT="${OWNS3_PORT:-9010}"
for i in $(seq 1 30); do curl -sf "http://localhost:${OWNS3_PORT}/healthz" >/dev/null 2>&1 && break; sleep 2; done
curl -sf "http://localhost:${OWNS3_PORT}/healthz" >/dev/null \
  || { echo "❌ owns3 не ожил за 60 c (:${OWNS3_PORT}/healthz; docker compose -p deploy logs owns3)"; exit 1; }
echo "  owns3 жив (http://localhost:${OWNS3_PORT}/healthz)"

# ownS3: bucket бэкапов — идемпотентный сид root-кредами; хост-переход НАРУЖУ
# (bucket-сид — чек-подобная операция с хоста; внутри контура адресация
# owns3:9000 — arch/owns3/05 §2-3:
# CreateBucket — только admin, воркер bucket не создаёт — как и раньше с
# as-minio). Ephemeral mc ходит через хост-публикацию (прецедент чека 46);
# стоит ДО проверок pgworker: подсистема бэкапов стартует против готового
# bucket (S3-недоступность и так transient-ретраи тика, arch/19 §5).
OWNS3_ROOT_USER="$(grep -E '^OWNS3_ROOT_USER=' "$ROOT/deploy/.env" | tail -1 | cut -d= -f2)"
OWNS3_ROOT_PASSWORD="$(grep -E '^OWNS3_ROOT_PASSWORD=' "$ROOT/deploy/.env" | tail -1 | cut -d= -f2)"
docker run --rm --add-host host.docker.internal:host-gateway \
  --entrypoint /bin/sh minio/mc:RELEASE.2025-08-13T08-35-41Z \
  -c "mc alias set own http://host.docker.internal:${OWNS3_PORT} '$OWNS3_ROOT_USER' '$OWNS3_ROOT_PASSWORD' >/dev/null \
      && mc mb --ignore-existing own/pgworker-backups >/dev/null" \
  || { echo "❌ bucket pgworker-backups не создан в ownS3 (:${OWNS3_PORT})"; exit 1; }
echo "  bucket pgworker-backups готов в ownS3 (:${OWNS3_PORT})"

ect() { docker compose exec -T etcd1 etcdctl --endpoints=http://localhost:2379 "$@"; }

# 1) etcd-контур: кворум собран (3 узла одним кластером, arch/04 §8)
ect_ok() { [ "$(ect member list 2>/dev/null | grep -c 'started')" = "3" ]; }
for i in $(seq 1 100); do ect_ok && break; sleep 1; done
ect_ok || { echo "  ❌ кворум as-etcd-1/2/3 не собрался за 100 c (docker compose logs etcd1 etcd2 etcd3)"; exit 1; }
echo "  etcd-контур ready (3/3 started)"

# 1a-2) Образ джоба бэкапов (t02): собирается рядом с pgworker:dev — тег
#       PgWorker:Backups:Job:Image (дефолт pgworker-backup:dev). Прямой docker
#       build (а не compose build) — без env-зависимостей deploy/.env.
docker build -q -f "$ROOT/docker/PgWorker.Backup.Dockerfile" -t pgworker-backup:dev "$ROOT" \
  || { echo "❌ образ pgworker-backup не собрался (docker/PgWorker.Backup.Dockerfile)"; exit 1; }
echo "  образ pgworker-backup:dev готов"

# 1b) PgWorker готовность (стенд = полная система; контур ВСЕГДА один —
#     etcd-кластер стенда): воркеры (2 инстанса, t07) из deploy/docker-compose.yml
#     ходят в as-etcd-1/2/3 через хост (PGW_ETCD_ENDPOINT_0..2=
#     host.docker.internal:2379/2381/2383 — дефолты deploy/docker-compose.yml,
#     t09; advertise as-etcd-1/2/3); Patroni-ноды, которые он создаёт, ходят
#     в DCS по тому же advertise. Порядок подъёма: стендовый компоуз —
#     владелец сети adminpanel-stand-net (выше); deploy-контур подключается
#     к ней external (contour-override). Здесь — проверки живости (нужен
#     собранный etcd-кворум выше); demo создаётся ниже (1c, POST /api/clusters).
MTLS="curl -fsS -m 3 --cacert $ROOT/deploy/tls/ca.pem --cert $ROOT/deploy/tls/healthcheck.crt --key $ROOT/deploy/tls/healthcheck.key"
for i in $(seq 1 60); do $MTLS https://localhost:${PGW_API_HOST_PORT:-8080}/healthz >/dev/null 2>&1 && break; sleep 1; done
$MTLS https://localhost:${PGW_API_HOST_PORT:-8080}/healthz >/dev/null \
  || { echo "❌ pgworker не ожил за 60 c (https :${PGW_API_HOST_PORT:-8080}/healthz по mTLS; docker logs deploy-pgworker-1)"; exit 1; }
for i in $(seq 1 60); do $MTLS https://localhost:${PGW_API_HOST_PORT2:-8083}/healthz >/dev/null 2>&1 && break; sleep 1; done
$MTLS https://localhost:${PGW_API_HOST_PORT2:-8083}/healthz >/dev/null \
  || { echo "❌ pgworker-2 не ожил за 60 c (https :${PGW_API_HOST_PORT2:-8083}/healthz по mTLS; docker logs deploy-pgworker-2-1)"; exit 1; }
# t07: оба инстанса публикуют свой lease-ключ дискавери (InstanceId = GUID,
# уникальный per контейнер) — двухинстансовый надзор доказан ключами.
pgw_api_keys=0
for i in $(seq 1 30); do
  pgw_api_keys="$(ect get /pgworker/api/ --prefix --keys-only 2>/dev/null | grep -c . || true)"
  [ "${pgw_api_keys:-0}" -ge 2 ] && break
  sleep 2
done
[ "${pgw_api_keys:-0}" -ge 2 ] \
  || { echo "❌ /pgworker/api/ содержит ${pgw_api_keys:-0} ключей (<2) — второй инстанс не публикует AdvertiseUrl/keepalive (docker logs deploy-pgworker-2-1)"; exit 1; }
echo "  pgworker жив ×2 (:${PGW_API_HOST_PORT:-8080} + :${PGW_API_HOST_PORT2:-8083}/healthz mTLS; ключей /pgworker/api/: ${pgw_api_keys})"

# 1c) Канонический demo (t41, arch/adminpanel/04 §2.2): декларация ЧЕРЕЗ API
#     воркера (mTLS healthcheck-парой): движок провижинит Spilo-ноды с
#     Patroni в стендовой сети, portalloc-порты, схемы bucket_*, → Active.
#     Идемпотентность: живой config → 409 (перезапуск 00-up на живом стенде —
#     no-op); пересборка demo — полный цикл (90-down.sh -v → 00-up.sh).
echo ">>> [PHASE] создание кластера demo (POST /api/clusters, $(date +%H:%M:%S))"
create_code="$(curl -s -o /tmp/pgw-create-demo.json -w '%{http_code}' -m 10 \
  --cacert "$ROOT/deploy/tls/ca.pem" --cert "$ROOT/deploy/tls/healthcheck.crt" \
  --key "$ROOT/deploy/tls/healthcheck.key" \
  -X POST "https://localhost:${PGW_API_HOST_PORT:-8080}/api/clusters" \
  -H 'Content-Type: application/json' \
  -d '{"name":"demo","buckets":16,"shards":2,"replicas":2,"requestCpu":2,"requestMem":2,"requestDisk":100}')"
case "$create_code" in
  201) echo "  demo: заявка принята (201)";;
  409) echo "  demo уже существует (409) — пропускаю создание";;
  *) { echo "❌ POST /api/clusters → $create_code"; cat /tmp/pgw-create-demo.json; exit 1; };;
esac

# Wait-гейт конвергенции (arch/14: Active = config без NOT_INITIALIZED, все
# ноды RUNNING, status/bucket_* СНЯТЫ): бюджет 600 c (Spilo-инициализация),
# [PHASE]-метки.
echo ">>> [PHASE] жду конвергенцию demo (бюджет 600 c, $(date +%H:%M:%S))"
demo_ready() {
  conf="$(ect get /clusters/demo/config --print-value-only 2>/dev/null || true)"
  [ -n "$conf" ] && ! echo "$conf" | grep -q '"state":"NOT_INITIALIZED"' || return 1
  for s in shard1 shard2; do
    for n in a b; do
      [ "$(ect get "/clusters/demo/shards/$s/nodes/$s$n/state" --print-value-only 2>/dev/null)" = "RUNNING" ] || return 1
    done
  done
  [ -z "$(ect get /clusters/demo/buckets/status/ --prefix --keys-only 2>/dev/null)" ] || return 1
}
for i in $(seq 1 300); do demo_ready && break; sleep 2; done
demo_ready || { echo "❌ demo не сошёлся за 600 c (docker logs deploy-pgworker-1; docker ps | grep pgw-demo)"; exit 1; }
echo "  demo: Active, ноды RUNNING, status-ключи бакетов сняты (shard1/shard2 × a/b)"

# 7) kafkaworker жив ×2 (t07): по два lease-ключа /kafkaworker/api/ и
#    /kafkaworker/instances/ (два инстанса = два ключа каждого вида).
kfw_keys=0
for i in $(seq 1 60); do
  kfw_keys="$(ect get /kafkaworker/api/ --prefix --keys-only 2>/dev/null | grep -c . || true)"
  [ "${kfw_keys:-0}" -ge 2 ] && break
  sleep 1
done
[ "${kfw_keys:-0}" -ge 2 ] \
  || { echo "❌ /kafkaworker/api/ содержит ${kfw_keys:-0} ключей (<2; docker compose logs kafkaworker kafkaworker-2)"; exit 1; }
[ "$(ect get /kafkaworker/instances/ --prefix --keys-only 2>/dev/null | grep -c . || true)" -ge 2 ] \
  || { echo "❌ /kafkaworker/instances/ меньше 2 ключей"; exit 1; }
echo "  kafkaworker жив ×2 (heartbeat /kafkaworker/{api,instances}/*: по ${kfw_keys})"

# 7b) valkeyworker жив ×2 (t03+t07): по два lease-ключа /valkeyworker/api/ и
#     /valkeyworker/instances/ — их ждут панель (WorkerEndpoints) и чек 51.
vwk_keys=0
for i in $(seq 1 60); do
  vwk_keys="$(ect get /valkeyworker/api/ --prefix --keys-only 2>/dev/null | grep -c . || true)"
  [ "${vwk_keys:-0}" -ge 2 ] && break
  sleep 1
done
[ "${vwk_keys:-0}" -ge 2 ] \
  || { echo "❌ /valkeyworker/api/ содержит ${vwk_keys:-0} ключей (<2; docker compose logs valkeyworker valkeyworker-2)"; exit 1; }
[ "$(ect get /valkeyworker/instances/ --prefix --keys-only 2>/dev/null | grep -c . || true)" -ge 2 ] \
  || { echo "❌ /valkeyworker/instances/ меньше 2 ключей"; exit 1; }
echo "  valkeyworker жив ×2 (heartbeat /valkeyworker/{api,instances}/*: по ${vwk_keys})"

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
