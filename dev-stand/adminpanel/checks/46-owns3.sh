#!/usr/bin/env bash
# Smoke-чек стендового ownS3 (t39, arch/owns3/05 §5/§6): /healthz, mc-smoke
# через хост-публикацию, /metrics (каталожные серии; counter-серии OTel
# экспортируются только с точками данных — проверка ПОСЛЕ mc-трафика).
# Стендовые бэкапы остаются на as-minio: чек НЕ трогает PGW_BACKUP_S3_* и
# бакет pgworker-backups. Временный бакет уникален (ts-суффикс); чистка — trap.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$(cd ../.. && pwd)"
ENV_FILE="$ROOT/deploy/.env"
PORT="$(grep -E '^OWNS3_HOST_PORT=' "$ENV_FILE" 2>/dev/null | tail -1 | cut -d= -f2 || true)"
PORT="${PORT:-9010}"
ROOT_USER="$(grep -E '^OWNS3_ROOT_USER=' "$ENV_FILE" | tail -1 | cut -d= -f2)"
ROOT_PASSWORD="$(grep -E '^OWNS3_ROOT_PASSWORD=' "$ENV_FILE" | tail -1 | cut -d= -f2)"
[ -n "$ROOT_USER" ] && [ -n "$ROOT_PASSWORD" ] \
  || { echo "❌ deploy/.env: нет OWNS3_ROOT_USER/OWNS3_ROOT_PASSWORD"; exit 1; }
BASE="http://localhost:${PORT}"
MC="minio/mc:RELEASE.2025-08-13T08-35-41Z"
BUCKET="os3check-$(date +%s)"
# mc ходит через ХОСТ-ПУБЛИКАЦИЮ (канон 05 §6, spec §4.2 п.3): ephemeral
# контейнер mc резолвит host.docker.internal в шлюз docker-хоста и стучит
# на публикуемый порт owns3 — тот же путь, что внешний клиент стенда.
MC_TARGET="http://host.docker.internal:${PORT}"

# 1) healthz — 200 без подписи
curl -sf "$BASE/healthz" >/dev/null || { echo "❌ $BASE/healthz не 200"; exit 1; }
echo "  healthz: 200"

# 1b) кластер-health — 200 без подписи + JSON drives-поля (канон 05 §5;
#     потребитель — ClusterOk-проба панели, adminpanel/02 §2.5)
CLUSTER="$(curl -sf "$BASE/s3/health/cluster")" \
  || { echo "❌ $BASE/s3/health/cluster не 200"; exit 1; }
echo "$CLUSTER" | grep -q '"healthyDrives"' \
  || { echo "❌ /s3/health/cluster: нет healthyDrives"; exit 1; }
echo "  cluster-health: 200 + healthyDrives"

# 2) mc-smoke ЧЕРЕЗ ХОСТ-ПУБЛИКАЦИЮ: alias → mb → cp(128 КиБ /dev/urandom)
#    → ls → cat(sha256 == источник) → stat → скачивание → rm → ls пуст → rb;
#    чистка — trap при любом исходе (mc rb --force). Образ mc урезан
#    (нет grep/sed/cmp): ls/stat — case-матчингом sh, побайтовая сверка —
#    sha256sum (эквивалент cmp).
mc_run() { docker run --rm --add-host host.docker.internal:host-gateway \
  --entrypoint /bin/sh "$MC" -c "$1"; }
cleanup() {
  mc_run "mc alias set c ${MC_TARGET} '$ROOT_USER' '$ROOT_PASSWORD' >/dev/null 2>&1 \
          && mc rb --force c/$BUCKET >/dev/null 2>&1" || true
}
trap cleanup EXIT
mc_run "mc alias set c ${MC_TARGET} '$ROOT_USER' '$ROOT_PASSWORD' >/dev/null \
         && mc mb c/$BUCKET >/dev/null \
         && head -c 131072 /dev/urandom > /tmp/os3check.bin \
         && SRC=\$(sha256sum /tmp/os3check.bin | cut -d' ' -f1) \
         && mc cp /tmp/os3check.bin c/$BUCKET/obj.bin >/dev/null \
         && case \"\$(mc ls c/$BUCKET)\" in *obj.bin*) true;; *) false;; esac \
         && GOT=\$(mc cat c/$BUCKET/obj.bin | sha256sum | cut -d' ' -f1) \
         && case \"\$(mc stat c/$BUCKET/obj.bin)\" in *Size*) true;; *) false;; esac \
         && case \"\$(mc stat c/$BUCKET/obj.bin)\" in *ETag*) true;; *) false;; esac \
         && mc cp c/$BUCKET/obj.bin /tmp/os3check-out.bin >/dev/null \
         && [ \"\$(sha256sum /tmp/os3check-out.bin | cut -d' ' -f1)\" = \"\$SRC\" ] \
         && mc rm c/$BUCKET/obj.bin >/dev/null \
         && [ -z \"\$(mc ls c/$BUCKET 2>/dev/null)\" ] \
         && mc rb c/$BUCKET >/dev/null \
         && [ \"\$SRC\" = \"\$GOT\" ]" \
  || { echo "❌ mc-smoke через хост-публикацию :${PORT} не прошёл"; exit 1; }
trap - EXIT
echo "  mc (host.docker.internal:${PORT}): mb→cp→ls→cat(sha256=источник)→stat→скачивание(cmp ok)→rm→rb"

# 3) /metrics — каталожные серии канона 05 §5 (после mc-трафика: OTel
#    экспортирует counter-серии только при наличии точек данных).
METRICS="$(curl -sf "$BASE/metrics")"
echo "$METRICS" | grep -q 'ownS3_requests_total{' \
  || { echo "❌ /metrics: нет ownS3_requests_total{"; exit 1; }
echo "$METRICS" | grep -q 'ownS3_disk_total_bytes' \
  || { echo "❌ /metrics: нет ownS3_disk_total_bytes"; exit 1; }
echo "  metrics: ownS3_requests_total{…} / ownS3_disk_total_bytes — на месте"

echo "✓ owns3-чек зелёный (healthz + mc-smoke + metrics, :${PORT})"
