#!/usr/bin/env bash
# Smoke-чек стендового ownS3 XL Erasure (t40, arch/owns3/06; модель 46-owns3):
# /healthz, админ-грань GET /admin/storage (SigV4-root на openssl; чужой ключ
# — 403), mc-smoke через хост-публикацию, /metrics (EC-серии + диск).
# Сервис — профиль owns3-ec (поднимает 00-up.sh или вручную
# `docker compose -f deploy/docker-compose.yml --profile owns3-ec up -d owns3-ec`).
# Временный бакет уникален (ts-суффикс); чистка — trap.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$(cd ../.. && pwd)"
ENV_FILE="$ROOT/deploy/.env"
PORT="$(grep -E '^OWNS3_EC_HOST_PORT=' "$ENV_FILE" 2>/dev/null | tail -1 | cut -d= -f2 || true)"
PORT="${PORT:-9011}"
ROOT_USER="$(grep -E '^OWNS3_ROOT_USER=' "$ENV_FILE" | tail -1 | cut -d= -f2)"
ROOT_PASSWORD="$(grep -E '^OWNS3_ROOT_PASSWORD=' "$ENV_FILE" | tail -1 | cut -d= -f2)"
[ -n "$ROOT_USER" ] && [ -n "$ROOT_PASSWORD" ] \
  || { echo "❌ deploy/.env: нет OWNS3_ROOT_USER/OWNS3_ROOT_PASSWORD"; exit 1; }
BASE="http://localhost:${PORT}"
HOST="localhost:${PORT}"
MC="minio/mc:RELEASE.2025-08-13T08-35-41Z"
BUCKET="os3ec-$(date +%s)"
MC_TARGET="http://host.docker.internal:${PORT}"

# 1) healthz — 200 без подписи
curl -sf "$BASE/healthz" >/dev/null || { echo "❌ $BASE/healthz не 200"; exit 1; }
echo "  healthz: 200"

# 2) админ-грань GET /admin/storage: SigV4 в чистом bash на openssl
#    (canonical request + цепочка AWS4-деривации; UNSIGNED-PAYLOAD пустого GET).
#    Статус — в stdout, тело ответа — в /tmp/os3ec-admin-body.
sigv4_get() { # <access> <secret>
  local access="$1" secret="$2"
  local ts amz_date payload_hash canonical scope sts k_date k_region k_service k_signing signature status
  ts="$(date -u +%Y%m%dT%H%M%SZ)"
  amz_date="${ts:0:8}"
  payload_hash="$(printf '' | openssl dgst -sha256 -hex | sed 's/^.* //')"
  canonical="$(printf 'GET\n/admin/storage\n\nhost:%s\nx-amz-content-sha256:%s\nx-amz-date:%s\n\nhost;x-amz-content-sha256;x-amz-date\n%s' \
    "$HOST" "$payload_hash" "$ts" "$payload_hash")"
  scope="${amz_date}/us-east-1/s3/aws4_request"
  sts="$(printf 'AWS4-HMAC-SHA256\n%s\n%s\n%s' "$ts" "$scope" \
    "$(printf '%s' "$canonical" | openssl dgst -sha256 -hex | sed 's/^.* //')")"
  k_date="$(printf '%s' "$amz_date" | openssl dgst -sha256 -hmac "AWS4${secret}" -hex | sed 's/^.* //')"
  k_region="$(printf '%s' us-east-1 | openssl dgst -sha256 -hmac "$k_date" -hex | sed 's/^.* //')"
  k_service="$(printf '%s' s3 | openssl dgst -sha256 -hmac "$k_region" -hex | sed 's/^.* //')"
  k_signing="$(printf '%s' aws4_request | openssl dgst -sha256 -hmac "$k_service" -hex | sed 's/^.* //')"
  signature="$(printf '%s' "$sts" | openssl dgst -sha256 -hmac "$k_signing" -hex | sed 's/^.* //')"
  status="$(curl -s -o /tmp/os3ec-admin-body -w '%{http_code}' \
    -H "x-amz-date: ${ts}" \
    -H "x-amz-content-sha256: ${payload_hash}" \
    -H "Authorization: AWS4-HMAC-SHA256 Credential=${access}/${scope}, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=${signature}" \
    "$BASE/admin/storage")"
  printf '%s' "$status"
}

STATUS="$(sigv4_get "$ROOT_USER" "$ROOT_PASSWORD")"
[ "$STATUS" = "200" ] || { echo "❌ /admin/storage root-подписью: ${STATUS} (ожидался 200)"; exit 1; }
grep -q '"mode":"XL Erasure"' /tmp/os3ec-admin-body \
  || { echo "❌ /admin/storage: нет режима XL Erasure"; cat /tmp/os3ec-admin-body; exit 1; }
grep -q '"online":true' /tmp/os3ec-admin-body \
  || { echo "❌ /admin/storage: нет онлайн-дисков"; cat /tmp/os3ec-admin-body; exit 1; }
echo "  admin/storage (SigV4-root): 200, XL Erasure, диски онлайн"

#    чужой ключ (не-admin: read-write стендовых ключей нет — подпись неизвестным
#    ключом) — 403
STATUS="$(sigv4_get "no-such-key" "no-such-secret")"
[ "$STATUS" = "403" ] || { echo "❌ /admin/storage чужим ключом: ${STATUS} (ожидался 403)"; exit 1; }
echo "  admin/storage (чужой ключ): 403"

# 3) mc-smoke ЧЕРЕЗ ХОСТ-ПУБЛИКАЦИЮ (кейсы чека 46): alias root → mb →
#    cp(128 КиБ /dev/urandom) → cat(sha256 == источник) → stat → скачивание
#    (cmp) → rm → rb; чистка — trap при любом исходе.
mc_run() { docker run --rm --add-host host.docker.internal:host-gateway \
  --entrypoint /bin/sh "$MC" -c "$1"; }
cleanup() {
  mc_run "mc alias set c ${MC_TARGET} '$ROOT_USER' '$ROOT_PASSWORD' >/dev/null 2>&1 \
          && mc rb --force c/$BUCKET >/dev/null 2>&1" || true
}
trap cleanup EXIT
mc_run "mc alias set c ${MC_TARGET} '$ROOT_USER' '$ROOT_PASSWORD' >/dev/null \
         && mc mb c/$BUCKET >/dev/null \
         && head -c 131072 /dev/urandom > /tmp/os3ec.bin \
         && SRC=\$(sha256sum /tmp/os3ec.bin | cut -d' ' -f1) \
         && mc cp /tmp/os3ec.bin c/$BUCKET/obj.bin >/dev/null \
         && case \"\$(mc ls c/$BUCKET)\" in *obj.bin*) true;; *) false;; esac \
         && GOT=\$(mc cat c/$BUCKET/obj.bin | sha256sum | cut -d' ' -f1) \
         && case \"\$(mc stat c/$BUCKET/obj.bin)\" in *Size*) true;; *) false;; esac \
         && mc cp c/$BUCKET/obj.bin /tmp/os3ec-out.bin >/dev/null \
         && [ \"\$(sha256sum /tmp/os3ec-out.bin | cut -d' ' -f1)\" = \"\$SRC\" ] \
         && mc rm c/$BUCKET/obj.bin >/dev/null \
         && [ -z \"\$(mc ls c/$BUCKET 2>/dev/null)\" ] \
         && mc rb c/$BUCKET >/dev/null \
         && [ \"\$SRC\" = \"\$GOT\" ]" \
  || { echo "❌ mc-smoke через хост-публикацию :${PORT} не прошёл"; exit 1; }
trap - EXIT
echo "  mc (host.docker.internal:${PORT}): mb→cp→ls→cat(sha256=источник)→stat→скачивание(cmp ok)→rm→rb"

# 4) /metrics — EC-серии (set readable гейдж; лейблы OTel: otel_scope_name)
#    и каталожная дисковая серия канона 05 §5.
METRICS="$(curl -sf "$BASE/metrics")"
echo "$METRICS" | grep -q 'ownS3_ec_set_readable{' \
  || { echo "❌ /metrics: нет ownS3_ec_set_readable{"; exit 1; }
echo "$METRICS" | grep -q 'set="0"} 1' \
  || { echo "❌ /metrics: сет 0 не readable"; exit 1; }
echo "$METRICS" | grep -q 'ownS3_disk_total_bytes' \
  || { echo "❌ /metrics: нет ownS3_disk_total_bytes"; exit 1; }
echo "  metrics: ownS3_ec_set_readable{…} 1 / ownS3_disk_total_bytes — на месте"

echo "✓ owns3-ec-чек зелёный (healthz + админ-грань SigV4 + mc-smoke + EC-метрики, :${PORT})"
