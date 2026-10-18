#!/usr/bin/env bash
# Подъём полного стенда (профили full + kafka + valkey + metrics) до рабочего
# состояния, по порядку: ГАРАНТИРОВАННАЯ зачистка контура (90-down.sh -v —
# каждый запуск = новое окружение, базовые правила §13 п.2) → publish-фазы
# образов ([PHASE]: SdGenerator, wal-агент; остальные — compose build;
# owns3 — ВНЕШНИЙ образ из ../OwnS3, fail-fast ниже)
# → стендовый compose (владелец сети adminpanel-stand-net) → deploy-контур
# (contour-override: воркеры + ownS3) → bucket-сид ownS3 → POST /api/clusters
# (канонический demo движком) → панель + прометей. Управление кафкой входит в
# стенд всегда (не только e2e-гейтом): без воркера kafka-домен панели глух —
# отсюда «глупые» алерты и разборы.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$(cd ../.. && pwd)"
# Телеметрия прогона (канон docs/e2e-launch.md — «никакой тихой фазы»):
# [PHASE]-строки дублируются в phases.log (append; разбор прогона — по хвосту).
PHASES_LOG="${PGW_STAND_PHASES_LOG:-/tmp/pgw-stand-phases.log}"
phase() {
  echo ">>> [PHASE] $*"
  echo "[$(date +%H:%M:%S)] [PHASE] $*" >> "$PHASES_LOG"
}
# КАЖДЫЙ ЗАПУСК — НОВОЕ ОКРУЖЕНИЕ (базовые правила §13 п.2): повторный подъём
# поверх живого контура недопустим — первым шагом ГАРАНТИРОВАННАЯ зачистка
# существующего стенда (та же логика, что 90-down.sh -v: стендовый compose down
# с томами, deploy-контур, движковые pgw-*/s3w-* контейнеры, тома, сети;
# own-only по стендовым именам, «не существует» — молча ок). etcd-данные
# стираются: пулы/кластеры/сиды создаются заново на чистом контуре.
phase "cleaning up the contour before startup (90-down.sh -v — every run = fresh environment)"
bash "$PWD/checks/90-down.sh" -v >/dev/null 2>&1 || true
# Хост-публикация API pgworker параметризуется (коллизии портов на хосте;
# канон 8080, второй инстанс — 8083, t07). Advertise обязан совпадать с
# фактической публикацией (панель и чеки стучатся по advertise),
# prometheus-target — файл file_sd (оба инстанса).
export PGW_API_HOST_PORT="${PGW_API_HOST_PORT:-8080}"
export PGW_API_HOST_PORT2="${PGW_API_HOST_PORT2:-8083}"
# Хост-публикации API s3worker (ряд 8086/8089; advertise следует за портом).
export S3W_API_HOST_PORT="${S3W_API_HOST_PORT:-8086}"
export S3W_API_HOST_PORT2="${S3W_API_HOST_PORT2:-8089}"
[ "${PGW_API_ADVERTISE_URL:-}" ] || export PGW_API_ADVERTISE_URL="https://host.docker.internal:${PGW_API_HOST_PORT}"
# t15: таргеты джобы pgworker — сетевые адреса deploy-воркеров (aliases в
# pgw-metrics); host-порты PGW_API_HOST_PORT* остаются — advertise/чеки/панель,
# не скрейп (arch/18 §5.2).
printf '[{"targets": ["pgworker:8080", "pgworker-2:8080"]}]\n' \
  > metrics/prometheus/pgworker-targets.json

# Arrange: инструменты хоста
for bin in docker jq curl; do
  command -v "$bin" >/dev/null || { echo "❌ no $bin in PATH"; exit 1; }
done

# ownS3: ВНЕШНИЙ образ (репозиторий ../OwnS3) — pg его не собирает; нет
# образа — падаем сразу с подсказкой (сборка: ../OwnS3/deploy/build-image.sh).
docker image inspect owns3:dev >/dev/null 2>&1 \
  || { echo "❌ image owns3:dev missing — build it in ../OwnS3: bash ../OwnS3/deploy/build-image.sh"; exit 1; }

# mTLS API воркеров (t03, arch/16 §1.1 / arch/21 §1.1): per-install TLS-пакет —
# gen.sh идемпотентен ПОФАЙЛОВО (ca.pem жив — клиентские/pgserver не трогаются;
# server-серт без DNS:valkeyworker перегенерируется, t03), поэтому зовём
# безусловно: на свежем хосте создаёт пакет, на живом — только чинит старый
# server-серт; panel.crt/ca.pem уходят панели, server.* + ca.pem — воркерам
# (bind ../../deploy/tls в стендовом compose).
echo ">>> TLS package (deploy/tls/gen.sh — idempotent)"
bash "$ROOT/deploy/tls/gen.sh"

# Наполнение deploy-volume pgw-api-tls пакетом (ro-монтирование воркером);
# имя volume — с префиксом compose-проекта deploy (как его создаёт compose).
docker volume create deploy_pgw-api-tls >/dev/null
# ca.key — выпуск REST-сертов нод (R16: бэкап пакета — runbook)
docker run --rm \
  -v "$ROOT/deploy/tls:/src:ro" -v deploy_pgw-api-tls:/tls alpine:3.20 \
  sh -c "cp /src/ca.pem /src/ca.key /src/pgserver.crt /src/pgserver.key /src/server.crt /src/server.key /src/healthcheck.crt /src/healthcheck.key /tls/"

# SdGenerator (t15): publish НА ХОСТЕ → compose build пакует только вывод
# (канон E2E-образов; сборка не «тихая» — [PHASE] и тайминг).
echo ">>> [PHASE] publish Metrics.SdGenerator ($(date +%H:%M:%S))"
dotnet publish "$ROOT/src/Metrics.SdGenerator/Metrics.SdGenerator.csproj" \
  -c Release -o "$ROOT/artifacts/sd-generator/publish" --nologo \
  || { echo "❌ SdGenerator publish failed"; exit 1; }
echo ">>> [PHASE] SdGenerator publish ready ($(date +%H:%M:%S))"

# WAL-агент (t41): publish НА ХОСТЕ → образ publish-COPY (канон E2E-образов;
# сборка не «тихая» — [PHASE] и тайминг).
echo ">>> [PHASE] publish PgWorker.WalReceiver ($(date +%H:%M:%S))"
dotnet publish "$ROOT/src/PgWorker.WalReceiver/PgWorker.WalReceiver.csproj" \
  -c Release -o "$ROOT/artifacts/wal/publish" --nologo \
  || { echo "❌ WalReceiver publish failed"; exit 1; }
docker build -q -f "$ROOT/docker/PgWorker.Wal.Dockerfile" -t pgworker-wal:dev "$ROOT/artifacts/wal/publish" \
  || { echo "❌ image pgworker-wal:dev build failed"; exit 1; }
echo "  image pgworker-wal:dev ready"

# S3Worker (arch/24): publish НА ХОСТЕ → образ publish-COPY (канон
# E2E-образов; сборка не «тихая» — [PHASE] и тайминг).
echo ">>> [PHASE] publish S3Worker.App ($(date +%H:%M:%S))"
dotnet publish "$ROOT/src/S3Worker.App/S3Worker.App.csproj" \
  -c Release -o "$ROOT/artifacts/s3worker/publish" --nologo \
  || { echo "❌ S3Worker publish failed"; exit 1; }
echo ">>> [PHASE] S3Worker publish ready ($(date +%H:%M:%S))"

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
# Синхронизация хост-портов API s3worker в .env (чеки читают порт из .env).
for k in S3W_API_HOST_PORT S3W_API_HOST_PORT2; do
  v="${!k}"
  if grep -q "^$k=" "$ROOT/deploy/.env"; then
    sed -i.bak "s/^$k=.*/$k=$v/" "$ROOT/deploy/.env" && rm -f "$ROOT/deploy/.env.bak"
  else
    printf '%s=%s\n' "$k" "$v" >> "$ROOT/deploy/.env"
  fi
done
# Демо-профиль секретов нод — в .env без блока дописывается
# ЦЕЛИКОМ: compose-плейсхолдеры :?задайте секрет срывают up на интерполяции.
# Свежий .env (копия .env.example) блок уже несёт — дописки нет.
if ! grep -q '^S3W_NODE_SECRETS__BACKUPS-MAIN__ROOT_USER=' "$ROOT/deploy/.env"; then
cat >> "$ROOT/deploy/.env" <<'EOF'

# S3Worker: bucket per-install (fail-fast старта) + демо-профиль
# секретов нод (arch/23 §6) — стендовые примеры (прод — per-install секрет).
S3W_BUCKET=pgworker-backups
S3W_ETCD_ENDPOINT_0=http://host.docker.internal:2379
S3W_ETCD_ENDPOINT_1=http://host.docker.internal:2381
S3W_ETCD_ENDPOINT_2=http://host.docker.internal:2383
S3W_NODE_SECRETS__BACKUPS-MAIN__ROOT_USER=s3w-local-root
S3W_NODE_SECRETS__BACKUPS-MAIN__ROOT_PASSWORD=s3w-local-root-pwd
S3W_NODE_SECRETS__BACKUPS-MAIN__ACCESS_KEYS__0__ACCESSKEY=pgw-backups-writer
S3W_NODE_SECRETS__BACKUPS-MAIN__ACCESS_KEYS__0__SECRETKEY=pgw-backups-writer-pwd
S3W_NODE_SECRETS__BACKUPS-MAIN__ACCESS_KEYS__0__POLICY=read-write
S3W_NODE_SECRETS__BACKUPS-MAIN__ACCESS_KEYS__1__ACCESSKEY=panel-reader
S3W_NODE_SECRETS__BACKUPS-MAIN__ACCESS_KEYS__1__SECRETKEY=panel-reader-pwd
S3W_NODE_SECRETS__BACKUPS-MAIN__ACCESS_KEYS__1__POLICY=read-only
EOF
fi
# Производный файл секретов нод для env_file s3worker — строки KEY=VAL
# едут в контейнер КАК ЕСТЬ (compose-интерполяция ${S3W_NODE_SECRETS__…-…}
# невозможна: дефис в имени переменной парсер читает как оператор дефолта).
grep -E '^S3W_NODE_SECRETS__' "$ROOT/deploy/.env" > "$ROOT/deploy/s3w-node-secrets.env"
[ -s "$ROOT/deploy/s3w-node-secrets.env" ] \
  || { echo "❌ deploy/s3w-node-secrets.env is empty — secrets profile not assembled from .env"; exit 1; }
# t36: env-профили кредов пулов pgworker (arch/19 §7) — в .env без блока
# дописываются ЦЕЛИКОМ (прежний .env от старого шаблона блока не несёт;
# свежий .env — копия .env.example — уже несёт). Имя профиля = secretProfile
# сида пула ниже (BACKUPS-MAIN).
if ! grep -q '^PGW_BACKUP_S3_PROFILES__BACKUPS-MAIN__ACCESS_KEY=' "$ROOT/deploy/.env"; then
cat >> "$ROOT/deploy/.env" <<'EOF'

# Профиль кредов пула бэкапов (t36, arch/19 §7): имя = secretProfile сида
# пула «backups» (00-up.sh); ключ — writer-комплект OWNS3_ACCESS_KEYS__0.
PGW_BACKUP_S3_PROFILES__BACKUPS-MAIN__ACCESS_KEY=pgw-backups-writer
PGW_BACKUP_S3_PROFILES__BACKUPS-MAIN__SECRET_KEY=pgw-backups-writer-pwd
EOF
fi
# t36: производный файл профилей для env_file pgworker — те же строки KEY=VAL
# без интерполяции (дефис в имени профиля; прецедент s3w-node-secrets.env).
grep -E '^PGW_BACKUP_S3_PROFILES__' "$ROOT/deploy/.env" > "$ROOT/deploy/pgw-s3-profiles.env"
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
# Тик лупов воркера — прод-дефолт appsettings (5 c). Стендовый override 1 c
# проверен и отозван: конвергенцию demo не ускоряет (доминирует Spilo-фаза,
# 57-58 c во всех прогонах), а подбор moves-заявок воркером стал обгонять
# панельный тик 3 c — грань панели «очередь заявок» теряла видимость заявки
# (чек 60, rollback bucket_6).
if grep -q '^PgWorker__Loops__ScanIntervalSec=' "$ROOT/deploy/.env"; then
  sed -i.bak '/^PgWorker__Loops__ScanIntervalSec=/d' "$ROOT/deploy/.env" && rm -f "$ROOT/deploy/.env.bak"
fi

echo ">>> bringing up the stand (docker compose --profile full --profile kafka --profile valkey --profile metrics up -d --build)"
# Docker Desktop отдаёт хост-порт recreated-контейнера с задержкой (com.docke
# держит публикацию после удаления старого контейнера; при пересборке образа
# recreate стабилен — ID меняется метаданными даже на кэшированных слоях) —
# ретрай compose up, иначе подъём падает на «port is already allocated».
compose_up_ok=0
for _ in 1 2 3; do
  if docker compose --profile full --profile kafka --profile valkey --profile metrics up -d --build 2>&1 | tail -5; then
    compose_up_ok=1; break
  fi
  echo "  compose up failed (port not released after recreate) — pausing 10 s"; sleep 10
done
[ "$compose_up_ok" = 1 ] || { echo "❌ stand failed to come up after 3 attempts (docker compose logs kafkaworker)"; exit 1; }

# deploy-контур (воркеры + owns3) — в стендовую сеть (контур-override t41;
# владелец сети adminpanel-stand-net — стендовый compose выше, deploy
# подключается external; отдельная pgw-metrics не создаётся). force-recreate
# + --build: свежий процесс из свежего образа надёжнее (кеш DNS/коннектов
# умершего etcd); healthz/api-ключи — ниже в §1b.
pg_up_ok=0
for _ in 1 2 3; do
  if ( cd "$ROOT/deploy" && docker compose -f docker-compose.yml -f docker-compose.contour.yml \
       --env-file "$ROOT/deploy/.env" up -d --build --force-recreate pgworker pgworker-2 owns3 s3worker s3worker-2 2>&1 | tail -2 ); then
    pg_up_ok=1; break
  fi
  echo "  deploy up failed (port not released after recreate) — pausing 15 s"; sleep 15
done
[ "$pg_up_ok" = 1 ] || { echo "❌ deploy contour failed to come up after 3 attempts (docker logs deploy-pgworker-1)"; exit 1; }

# ownS3 (t39): готовность — healthz-проба хост-публикации (порт синхронен с
#     deploy/.env, дефолт 9010).
OWNS3_PORT="$(grep -E '^OWNS3_HOST_PORT=' "$ROOT/deploy/.env" 2>/dev/null | tail -1 | cut -d= -f2 || true)"
OWNS3_PORT="${OWNS3_PORT:-9010}"
for i in $(seq 1 30); do curl -sf "http://localhost:${OWNS3_PORT}/healthz" >/dev/null 2>&1 && break; sleep 2; done
curl -sf "http://localhost:${OWNS3_PORT}/healthz" >/dev/null \
  || { echo "❌ owns3 did not come alive within 60 s (:${OWNS3_PORT}/healthz; docker compose -p deploy logs owns3)"; exit 1; }
echo "  owns3 alive (http://localhost:${OWNS3_PORT}/healthz)"

# ownS3 (t39/t36): bucket СЛЕПКОВ etcd — идемпотентный сид root-кредами
# (спецификация §7: standalone owns3 остаётся целью слепков — отдельная пара
# PGW_SNAPSHOTS_S3_* со своим bucket; bucket пулов создаёт S3Worker, сюда
# бэкапы больше не пишутся). Ephemeral mc ходит через хост-публикацию
# (прецедент чека 46); стоит ДО проверок pgworker.
OWNS3_ROOT_USER="$(grep -E '^OWNS3_ROOT_USER=' "$ROOT/deploy/.env" | tail -1 | cut -d= -f2)"
OWNS3_ROOT_PASSWORD="$(grep -E '^OWNS3_ROOT_PASSWORD=' "$ROOT/deploy/.env" | tail -1 | cut -d= -f2)"
docker run --rm --add-host host.docker.internal:host-gateway \
  --entrypoint /bin/sh minio/mc:RELEASE.2025-08-13T08-35-41Z \
  -c "mc alias set own http://host.docker.internal:${OWNS3_PORT} '$OWNS3_ROOT_USER' '$OWNS3_ROOT_PASSWORD' >/dev/null \
      && mc mb --ignore-existing own/pgworker-etcd-snapshots >/dev/null" \
  || { echo "❌ bucket pgworker-etcd-snapshots not created in ownS3 (:${OWNS3_PORT})"; exit 1; }
echo "  bucket pgworker-etcd-snapshots ready in ownS3 (:${OWNS3_PORT})"

ect() { docker compose exec -T etcd1 etcdctl --endpoints=http://localhost:2379 "$@"; }

# 1) etcd-контур: кворум собран (3 узла одним кластером, arch/04 §8)
ect_ok() { [ "$(ect member list 2>/dev/null | grep -c 'started')" = "3" ]; }
for i in $(seq 1 100); do ect_ok && break; sleep 1; done
ect_ok || { echo "  ❌ as-etcd-1/2/3 quorum not formed within 100 s (docker compose logs etcd1 etcd2 etcd3)"; exit 1; }
echo "  etcd contour ready (3/3 started)"

# 1a-2) Образ джоба бэкапов (t02): собирается рядом с pgworker:dev — тег
#       PgWorker:Backups:Job:Image (дефолт pgworker-backup:dev). Прямой docker
#       build (а не compose build) — без env-зависимостей deploy/.env.
docker build -q -f "$ROOT/docker/PgWorker.Backup.Dockerfile" -t pgworker-backup:dev "$ROOT" \
  || { echo "❌ image pgworker-backup build failed (docker/PgWorker.Backup.Dockerfile)"; exit 1; }
echo "  image pgworker-backup:dev ready"

# 1b) PgWorker готовность (стенд = полная система; контур ВСЕГДА один —
#     etcd-кластер стенда): воркеры (2 инстанса, t07) из deploy/docker-compose.yml
#     ходят в as-etcd-1/2/3 через хост (PGW_ETCD_ENDPOINT_0..2=
#     host.docker.internal:2379/2381/2383 — дефолты deploy/docker-compose.yml,
#     t09; advertise as-etcd-1/2/3); Patroni-ноды, которые он создаёт, ходят
#     в DCS по тому же advertise. Порядок подъёма: стендовый компоуз —
#     владелец сети adminpanel-stand-net (выше); deploy-контур подключается
#     к ней external (contour-override). Здесь — проверки живости (нужен
#     собранный etcd-кворум выше); demo создаётся ниже (1c, POST /api/clusters).
MTLS=(curl -fsS -m 3 --cacert "$ROOT/deploy/tls/ca.pem" --cert "$ROOT/deploy/tls/healthcheck.crt" --key "$ROOT/deploy/tls/healthcheck.key")
for i in $(seq 1 60); do "${MTLS[@]}" https://localhost:${PGW_API_HOST_PORT:-8080}/healthz >/dev/null 2>&1 && break; sleep 1; done
"${MTLS[@]}" https://localhost:${PGW_API_HOST_PORT:-8080}/healthz >/dev/null \
  || { echo "❌ pgworker did not come alive within 60 s (https :${PGW_API_HOST_PORT:-8080}/healthz via mTLS; docker logs deploy-pgworker-1)"; exit 1; }
for i in $(seq 1 60); do "${MTLS[@]}" https://localhost:${PGW_API_HOST_PORT2:-8083}/healthz >/dev/null 2>&1 && break; sleep 1; done
"${MTLS[@]}" https://localhost:${PGW_API_HOST_PORT2:-8083}/healthz >/dev/null \
  || { echo "❌ pgworker-2 did not come alive within 60 s (https :${PGW_API_HOST_PORT2:-8083}/healthz via mTLS; docker logs deploy-pgworker-2-1)"; exit 1; }
# t07: оба инстанса публикуют свой lease-ключ дискавери (InstanceId = GUID,
# уникальный per контейнер) — двухинстансовый надзор доказан ключами.
pgw_api_keys=0
for i in $(seq 1 30); do
  pgw_api_keys="$(ect get /pgworker/api/ --prefix --keys-only 2>/dev/null | grep -c . || true)"
  [ "${pgw_api_keys:-0}" -ge 2 ] && break
  sleep 2
done
[ "${pgw_api_keys:-0}" -ge 2 ] \
  || { echo "❌ /pgworker/api/ contains ${pgw_api_keys:-0} keys (<2) — second instance does not publish AdvertiseUrl/keepalive (docker logs deploy-pgworker-2-1)"; exit 1; }
echo "  pgworker alive ×2 (:${PGW_API_HOST_PORT:-8080} + :${PGW_API_HOST_PORT2:-8083}/healthz mTLS; /pgworker/api/ keys: ${pgw_api_keys})"

# 1c) Канонический demo (t41, arch/adminpanel/04 §2.2): декларация ЧЕРЕЗ API
#     воркера (mTLS healthcheck-парой): движок провижинит Spilo-ноды с
#     Patroni в стендовой сети, portalloc-порты, схемы bucket_*, → Active.
#     Идемпотентность: живой config → 409 (перезапуск 00-up на живом стенде —
#     no-op); пересборка demo — полный цикл (90-down.sh -v → 00-up.sh).
echo ">>> [PHASE] creating cluster demo (POST /api/clusters, $(date +%H:%M:%S))"
conv_t0="$(date +%s)"   # старт конвергенции: ДО POST (пороги — дельты от него)
create_code="$(curl -s -o /tmp/pgw-create-demo.json -w '%{http_code}' -m 10 \
  --cacert "$ROOT/deploy/tls/ca.pem" --cert "$ROOT/deploy/tls/healthcheck.crt" \
  --key "$ROOT/deploy/tls/healthcheck.key" \
  -X POST "https://localhost:${PGW_API_HOST_PORT:-8080}/api/clusters" \
  -H 'Content-Type: application/json' \
  -d '{"name":"demo","buckets":16,"shards":2,"replicas":2,"requestCpu":2,"requestMem":2,"requestDisk":100}')"
case "$create_code" in
  201) echo "  demo: request accepted (201)";;
  409) echo "  demo already exists (409) — skipping creation";;
  *) { echo "❌ POST /api/clusters → $create_code"; cat /tmp/pgw-create-demo.json; exit 1; };;
esac

# Wait-гейт конвергенции (arch/14: Active = config без NOT_INITIALIZED, все
# ноды RUNNING, status/bucket_* СНЯТЫ): бюджет 90 c — общий потолок; ранее
# обнаружение отказа — детектор стагнации 45 c без продвижения НИ ОДНОГО
# порога подфазы (норма: самое длинное «молчание» на живом подъёме ~41 c —
# patroni REST → streaming, pg_basebackup реплик). Пофазовая телеметрия с
# [PHASE]-метками в журнал + phases.log: сеть/контейнеры → Patroni REST
# per-node → leader-ключ → реплики streaming → снятие status-ключей → Active
# + суммарная строка. Отказ (потолок/стагнация) — немедленный abort со
# сбором диагностики (логи воркера, контейнеры нод, последние [PHASE]).
echo ">>> [PHASE] waiting for demo convergence (budget 90 s, stagnation 45 s, $(date +%H:%M:%S))"
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
# Порог подфазы: первый вызов фиксирует метку и печатает [PHASE] с дельтой
# от conv_t0; повторные — no-op (порог уже достигнут). Каждое продвижение
# двигает conv_last_progress — база детектора стагнации.
conv_mark() {
  local var="$1" label="$2"
  [ -n "${!var:-}" ] && return 0
  local now; now="$(date +%s)"
  printf -v "$var" '%s' "$now"
  conv_last_progress="$now"
  phase "demo: $label — $((now - conv_t0)) c"
}
# Дельта порога от conv_t0; незакрытый порог — прочерк (не «отрицательное время»).
conv_delta() {
  local v="$1"
  [ -n "${!v:-}" ] && echo "$(( ${!v} - conv_t0 ))" || echo "-"
}
# Подфаза 1: первый контейнер ноды demo в RUNNING (движок create/start).
demo_containers_up() { [ -n "$(docker ps -q --filter name='^pgw-demo-' 2>/dev/null)" ]; }
# Хост-порт Patroni REST (8008) ноды из её публикаций portalloc (пусто — нет).
patroni_rest_port() {
  docker port "$1" 8008 2>/dev/null | awk -F: '/:/{print $NF}' | tail -1
}
# Подфаза 2: REST каждой ноды отвечает /cluster (TLS per-install; -k —
# телеметрия подъёма, не проверка PKI). Нод ещё нет → порог НЕ достигнут
# (пустой список — не успех).
demo_patroni_ok() {
  local cs c p
  cs="$(docker ps --filter name='^pgw-demo-' --format '{{.Names}}')"
  [ -n "$cs" ] || return 1
  for c in $cs; do
    p="$(patroni_rest_port "$c")"
    [ -n "$p" ] || return 1
    curl -skf -m 2 "https://localhost:${p}/cluster" 2>/dev/null | grep -q '"members"' || return 1
  done
}
# Подфаза 3: leader-ключи обоих шардов (Patroni select leader, DCS etcd).
demo_leader_ok() {
  local s
  for s in shard1 shard2; do
    [ -n "$(ect get "/clusters/demo/shards/$s/master" --print-value-only 2>/dev/null)" ] || return 1
  done
}
# Подфаза 4: у каждого шарда есть стримящая реплика (member state=streaming
# в /cluster; форма ответа Patroni — с пробелом и без, grep-Eq покрывает).
demo_streaming_ok() {
  local s c p found out
  for s in shard1 shard2; do
    found=""
    for c in $(docker ps --filter "name=pgw-demo-$s-" --format '{{.Names}}'); do
      p="$(patroni_rest_port "$c")"
      [ -n "$p" ] || continue
      out="$(curl -sk -m 2 "https://localhost:${p}/cluster" 2>/dev/null || true)"
      if echo "$out" | grep -Eq '"state": ?"streaming"'; then found=1; break; fi
    done
    [ -n "$found" ] || return 1
  done
}
t_containers=""; t_patroni=""; t_leader=""; t_streaming=""; t_status=""
seen_status_keys=""
conv_last_progress="$conv_t0"
conv_deadline=$((conv_t0 + 90))
conv_idle_max=45
# Диагностика отказа: состояние контейнеров нод + хвост лога воркера +
# незакрытые пороги. Печатается ДО любого abort — «почему встало» видно
# без повторного прогона (канон телеметрии e2e-launch).
conv_fail_diag() {
  local why="$1" next
  echo "❌ demo did not converge: $why"
  echo "--- demo node containers:"
  docker ps -a --filter name='pgw-demo-' --format '{{.Names}}\t{{.Status}}' || true
  echo "--- deploy-pgworker-1 log tail:"
  docker logs --tail 25 deploy-pgworker-1 2>&1 | tail -12 || true
  echo "--- unclosed thresholds:"
  for pair in "t_containers:network/containers" "t_patroni:patroni REST" \
              "t_leader:leader keys" "t_streaming:replicas streaming" "t_status:status keys"; do
    next="${pair%%:*}"
    [ -z "${!next:-}" ] && echo "  not reached: ${pair#*:}"
  done
}
while [ "$(date +%s)" -lt "$conv_deadline" ]; do
  [ -n "$t_containers" ] || { demo_containers_up && conv_mark t_containers "network/containers: first pgw-demo-* RUNNING"; }
  [ -n "$t_patroni" ] || { demo_patroni_ok && conv_mark t_patroni "patroni REST: all nodes answer /cluster"; }
  [ -n "$t_leader" ] || { demo_leader_ok && conv_mark t_leader "leader keys shard1+shard2"; }
  [ -n "$t_streaming" ] || { demo_streaming_ok && conv_mark t_streaming "replicas streaming"; }
  status_keys_now="$(ect get /clusters/demo/buckets/status/ --prefix --keys-only 2>/dev/null || true)"
  [ -n "$status_keys_now" ] && seen_status_keys=1
  if [ -z "$t_status" ] && [ -n "$seen_status_keys" ] && [ -z "$status_keys_now" ]; then
    conv_mark t_status "bucket status keys removed"
  fi
  demo_ready && break
  # Детектор стагнации: ни один порог не продвигался дольше conv_idle_max —
  # отказ обнаружен рано, не дожидаясь общего потолка.
  if [ "$(( $(date +%s) - conv_last_progress ))" -gt "$conv_idle_max" ]; then
    conv_fail_diag "stagnation — no subphase progress for more than ${conv_idle_max} s (90 s budget not reached)"
    exit 1
  fi
  sleep 1
done
demo_ready || { conv_fail_diag "90 s budget exhausted"; exit 1; }
[ -n "$t_status" ] || t_status="$(date +%s)"   # сняты быстрее первого опроса — порог = гейт
conv_done="$(date +%s)"
phase "demo: convergence $((conv_done - conv_t0)) s (phases: network/containers $(conv_delta t_containers), patroni $(conv_delta t_patroni), leader $(conv_delta t_leader), streaming $(conv_delta t_streaming), status $(conv_delta t_status))"
echo "  demo: Active, nodes RUNNING, bucket status keys removed (shard1/shard2 × a/b)"

# 7) kafkaworker жив ×2 (t07): по два lease-ключа /kafkaworker/api/ и
#    /kafkaworker/instances/ (два инстанса = два ключа каждого вида).
kfw_keys=0
for i in $(seq 1 60); do
  kfw_keys="$(ect get /kafkaworker/api/ --prefix --keys-only 2>/dev/null | grep -c . || true)"
  [ "${kfw_keys:-0}" -ge 2 ] && break
  sleep 1
done
[ "${kfw_keys:-0}" -ge 2 ] \
  || { echo "❌ /kafkaworker/api/ contains ${kfw_keys:-0} keys (<2; docker compose logs kafkaworker kafkaworker-2)"; exit 1; }
[ "$(ect get /kafkaworker/instances/ --prefix --keys-only 2>/dev/null | grep -c . || true)" -ge 2 ] \
  || { echo "❌ /kafkaworker/instances/ has fewer than 2 keys"; exit 1; }
echo "  kafkaworker alive ×2 (heartbeat /kafkaworker/{api,instances}/*: ${kfw_keys} each)"

# 7b) valkeyworker жив ×2 (t03+t07): по два lease-ключа /valkeyworker/api/ и
#     /valkeyworker/instances/ — их ждут панель (WorkerEndpoints) и чек 51.
vwk_keys=0
for i in $(seq 1 60); do
  vwk_keys="$(ect get /valkeyworker/api/ --prefix --keys-only 2>/dev/null | grep -c . || true)"
  [ "${vwk_keys:-0}" -ge 2 ] && break
  sleep 1
done
[ "${vwk_keys:-0}" -ge 2 ] \
  || { echo "❌ /valkeyworker/api/ contains ${vwk_keys:-0} keys (<2; docker compose logs valkeyworker valkeyworker-2)"; exit 1; }
[ "$(ect get /valkeyworker/instances/ --prefix --keys-only 2>/dev/null | grep -c . || true)" -ge 2 ] \
  || { echo "❌ /valkeyworker/instances/ has fewer than 2 keys"; exit 1; }
echo "  valkeyworker alive ×2 (heartbeat /valkeyworker/{api,instances}/*: ${vwk_keys} each)"

# 7c) s3worker жив ×2 (arch/24): healthz по mTLS healthcheck-парой
#     (общий TLS-пакет pgw-api-tls, server-серт с SAN s3worker*) +
#     дискавери /s3worker/api/ по ключам (по образцу pgworker/kafkaworker).
for i in $(seq 1 60); do "${MTLS[@]}" https://localhost:${S3W_API_HOST_PORT:-8086}/healthz >/dev/null 2>&1 && break; sleep 1; done
"${MTLS[@]}" https://localhost:${S3W_API_HOST_PORT:-8086}/healthz >/dev/null \
  || { echo "❌ s3worker did not come alive within 60 s (https :${S3W_API_HOST_PORT:-8086}/healthz via mTLS; docker logs deploy-s3worker-1)"; exit 1; }
for i in $(seq 1 60); do "${MTLS[@]}" https://localhost:${S3W_API_HOST_PORT2:-8089}/healthz >/dev/null 2>&1 && break; sleep 1; done
"${MTLS[@]}" https://localhost:${S3W_API_HOST_PORT2:-8089}/healthz >/dev/null \
  || { echo "❌ s3worker-2 did not come alive within 60 s (https :${S3W_API_HOST_PORT2:-8089}/healthz via mTLS; docker logs deploy-s3worker-2-1)"; exit 1; }
s3w_keys=0
for i in $(seq 1 60); do
  s3w_keys="$(ect get /s3worker/api/ --prefix --keys-only 2>/dev/null | grep -c . || true)"
  [ "${s3w_keys:-0}" -ge 2 ] && break
  sleep 1
done
[ "${s3w_keys:-0}" -ge 2 ] \
  || { echo "❌ /s3worker/api/ contains ${s3w_keys:-0} keys (<2; docker compose logs s3worker s3worker-2)"; exit 1; }
echo "  s3worker alive ×2 (:${S3W_API_HOST_PORT:-8086} + :${S3W_API_HOST_PORT2:-8089}/healthz mTLS; /s3worker/api/ keys: ${s3w_keys})"

# 7c-2) Сид пула бэкапов (t36, spec §4.6/§7): S3-кластер «backups» из одной
#       ноды через API живого s3worker (mTLS healthcheck-парой, образец 75);
#       идемпотентен: живой Active-кластер → 409 (пропуск). S3Worker сам
#       провижинит ownS3-ноду (порт из ряда S3W), создаст per-install bucket
#       (S3W_BUCKET, P3) и напишет /s3/clusters/backups/{config,nodes,endpoints}.
#       Политика пулов кластерам стенда не задаётся сидом — оператор выбирает
#       пул со страницы (до выбора — алерт backup-pool-missing, канон §7).
echo ">>> [PHASE] seeding backups pool (POST /api/s3/clusters, $(date +%s))"
# DTO CreateS3ClusterRequest (S3Api.cs): инстансы — списком instances (имена
# нод + placement); nodes/placements в запросе не читаются (кластер
# создавался пустым — прецедент мерж-гейта t36).
pool_body='{"name":"backups","platform":"owns3","secretProfile":"BACKUPS-MAIN","instances":[{"name":"node1","placement":{"dockerHost":"local"},"resources":{"cpu":0.5,"memGi":1,"diskGi":5}}]}'
pool_code="$("${MTLS[@]}" -o /tmp/pgw-seed-pool.json -w '%{http_code}' -X POST \
  "https://localhost:${S3W_API_HOST_PORT:-8086}/api/s3/clusters" \
  -H 'Content-Type: application/json' -d "$pool_body")"
case "$pool_code" in
  201) echo "  backups pool: request accepted (201)";;
  409) echo "  backups pool already exists (409) — skipping";;
  *) { echo "❌ POST /api/s3/clusters → $pool_code"; cat /tmp/pgw-seed-pool.json; exit 1; };;
esac
pool_ready=0
for i in $(seq 1 50); do
  [ "$(ect get /s3/clusters/backups/nodes/node1/state --print-value-only 2>/dev/null)" = "RUNNING" ] \
    && [ -n "$(ect get /s3/clusters/backups/endpoints --print-value-only 2>/dev/null)" ] \
    && { pool_ready=1; break; }
  sleep 2
done
[ "$pool_ready" = 1 ] \
  || { echo "❌ backups pool not ready within 100 s (node1 RUNNING + endpoints; docker logs deploy-s3worker-1)"; exit 1; }
echo "  backups pool: node1 RUNNING, endpoints=$(ect get /s3/clusters/backups/endpoints --print-value-only 2>/dev/null)"

# 7d) valkey-сид (t03): демо-кластер demo наливается ЧЕРЕЗ API живого воркера —
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
  || { echo "❌ adminpanel did not come alive within 60 s on :5050 (docker compose logs adminpanel)"; exit 1; }
echo "  adminpanel alive (http://localhost:5050, docker)"

# 9) мониторинг (профиль metrics): Prometheus/Grafana/Alertmanager живы; таргеты
#    прогреваются scrape-интервалом — готовность проверяет 65-metrics.sh.
for i in $(seq 1 60); do curl -fsS -m 3 "http://localhost:${METRICS_PROMETHEUS_PORT:-9090}/-/ready" >/dev/null 2>&1 && break; sleep 1; done
curl -fsS -m 3 "http://localhost:${METRICS_PROMETHEUS_PORT:-9090}/-/ready" >/dev/null \
  || { echo "❌ prometheus not ready within 60 s (docker compose logs prometheus)"; exit 1; }
echo "  prometheus ready (:${METRICS_PROMETHEUS_PORT:-9090})"

echo "✓ stand is up (full system: adminpanel + PG + kafka + valkey + PgWorker + monitoring, single contour)"
