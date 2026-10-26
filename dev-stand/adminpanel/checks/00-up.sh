#!/usr/bin/env bash
# Подъём стенда = ЯДРО + ПАНЕЛЬ на пустом контуре (профили kafka + valkey +
# metrics), по порядку: ГАРАНТИРОВАННАЯ зачистка контура (90-down.sh -v —
# каждый запуск = новое окружение, базовые правила §13 п.2) → publish-фазы
# образов ([PHASE]: SdGenerator, wal-агент, S3Worker; остальные — compose
# build) → стендовый compose (владелец сети adminpanel-stand-net) →
# deploy-контур contour-override (воркеры) → wait-on-healthy (etcd-кворум,
# healthz/lease-ключи воркеров ×2, панель, прометей). Никакие ресурсы НЕ
# создаются и никакие сиды НЕ наливаются — ресурсы (PG-кластеры, S3-пулы,
# valkey/kafka демо) создаёт оператор вручную через панель.
# Управление кафкой входит в стенд всегда (не только e2e-гейтом): без
# воркера kafka-домен панели глух — отсюда «глупые» алерты и разборы.
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

# Образ PG-ноды с приёмником WAL и служебной CLI (t25, arch/19 §3 + arch/14 §2.1):
# publish НА ХОСТЕ self-contained (RID хоста сборки, без trimming) → docker
# build пакует выводы (канон «publish на хосте, в контейнер — артефакты»;
# сборка не «тихая» — [PHASE] и тайминг; оба процесса живут ВНУТРИ контейнера).
case "$(uname -m)" in
  arm64|aarch64) NODE_RID=linux-arm64 ;;
  *)             NODE_RID=linux-x64 ;;
esac
echo ">>> [PHASE] publish PgWorker.WalReceiver self-contained $NODE_RID ($(date +%H:%M:%S))"
dotnet publish "$ROOT/src/PgWorker.WalReceiver/PgWorker.WalReceiver.csproj" \
  -c Release -r "$NODE_RID" --self-contained -o "$ROOT/artifacts/node-wal" --nologo \
  || { echo "❌ WalReceiver publish failed"; exit 1; }
echo ">>> [PHASE] publish PgWorker.NodeTools self-contained $NODE_RID ($(date +%H:%M:%S))"
dotnet publish "$ROOT/src/PgWorker.NodeTools/PgWorker.NodeTools.csproj" \
  -c Release -r "$NODE_RID" --self-contained -o "$ROOT/artifacts/node-tools" --nologo \
  || { echo "❌ NodeTools publish failed"; exit 1; }
echo ">>> [PHASE] pgworker-node:dev build (WAL_PUBLISH_DIR=artifacts/node-wal, NODE_TOOLS_PUBLISH_DIR=artifacts/node-tools) ($(date +%H:%M:%S))"
docker build -q -f "$ROOT/docker/node/Dockerfile" \
  --build-arg "WAL_PUBLISH_DIR=artifacts/node-wal" \
  --build-arg "NODE_TOOLS_PUBLISH_DIR=artifacts/node-tools" -t pgworker-node:dev "$ROOT" \
  || { echo "❌ image pgworker-node:dev build failed"; exit 1; }
echo "  image pgworker-node:dev ready (walreceiver + pgwnodetool inside)"

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
# Синхронизация хост-порта API в .env: пересоздания pgworker из сид-прогонов
# (force-recreate) в свежих оболочках интерполируют compose из .env —
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

echo ">>> bringing up the stand (docker compose --profile kafka --profile valkey --profile metrics up -d --build)"
# Docker Desktop отдаёт хост-порт recreated-контейнера с задержкой (com.docke
# держит публикацию после удаления старого контейнера; при пересборке образа
# recreate стабилен — ID меняется метаданными даже на кэшированных слоях) —
# ретрай compose up, иначе подъём падает на «port is already allocated».
compose_up_ok=0
for _ in 1 2 3; do
  if docker compose --profile kafka --profile valkey --profile metrics up -d --build 2>&1 | tail -5; then
    compose_up_ok=1; break
  fi
  echo "  compose up failed (port not released after recreate) — pausing 10 s"; sleep 10
done
[ "$compose_up_ok" = 1 ] || { echo "❌ stand failed to come up after 3 attempts (docker compose logs kafkaworker)"; exit 1; }

# deploy-контур (воркеры) — в стендовую сеть (контур-override t41; владелец
# сети adminpanel-stand-net — стендовый compose выше, deploy подключается
# external; отдельная pgw-metrics не создаётся). force-recreate + --build:
# свежий процесс из свежего образа надёжнее (кеш DNS/коннектов умершего
# etcd); healthz/api-ключи — ниже в §1b.
pg_up_ok=0
for _ in 1 2 3; do
  if ( cd "$ROOT/deploy" && docker compose -f docker-compose.yml -f docker-compose.contour.yml \
       --env-file "$ROOT/deploy/.env" up -d --build --force-recreate pgworker pgworker-2 s3worker s3worker-2 2>&1 | tail -2 ); then
    pg_up_ok=1; break
  fi
  echo "  deploy up failed (port not released after recreate) — pausing 15 s"; sleep 15
done
[ "$pg_up_ok" = 1 ] || { echo "❌ deploy contour failed to come up after 3 attempts (docker logs deploy-pgworker-1)"; exit 1; }

ect() { docker compose exec -T etcd1 etcdctl --endpoints=http://localhost:2379 "$@"; }

# 1) etcd-контур: кворум собран (3 узла одним кластером, arch/04 §8)
ect_ok() { [ "$(ect member list 2>/dev/null | grep -c 'started')" = "3" ]; }
for i in $(seq 1 100); do ect_ok && break; sleep 1; done
ect_ok || { echo "  ❌ as-etcd-1/2/3 quorum not formed within 100 s (docker compose logs etcd1 etcd2 etcd3)"; exit 1; }
echo "  etcd contour ready (3/3 started)"


# 1b) PgWorker готовность (стенд = полная система; контур ВСЕГДА один —
#     etcd-кластер стенда): воркеры (2 инстанса, t07) из deploy/docker-compose.yml
#     ходят в as-etcd-1/2/3 через хост (PGW_ETCD_ENDPOINT_0..2=
#     host.docker.internal:2379/2381/2383 — дефолты deploy/docker-compose.yml,
#     t09; advertise as-etcd-1/2/3); Patroni-ноды, которые он создаёт, ходят
#     в DCS по тому же advertise. Порядок подъёма: стендовый компоуз —
#     владелец сети adminpanel-stand-net (выше); deploy-контур подключается
#     к ней external (contour-override). Здесь — проверки живости (нужен
#     собранный etcd-кворум выше).
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

echo "✓ stand is up (core + panel; resources are created manually via the panel)"
