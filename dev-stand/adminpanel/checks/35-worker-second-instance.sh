#!/usr/bin/env bash
# 35-worker-second-instance.sh (t07, spec §4.4): kill→takeover на полном
# стенде. Для каждого воркера (pgw — deploy-контейнеры, kfw/vwk — стендовые
# as-*): Arrange (оба инстанса живы, по 2 ключа дискавери) → Act (docker stop
# ПЕРВОГО: unless-stopped не поднимает контейнер после явного stop — окно
# владения чисто уходит второму) → Assert ≤60 c (ключей ровно 1, healthz
# второго жив, клэйм кластера у второго, панель не 503) → Restore (docker
# start первого, оба ключа снова). Идемпотентен, стенд в исходном состоянии.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$(cd ../.. && pwd)"

# Хост-порты API pgworker: env → deploy/.env (пишет 00-up.sh) → defaults.
env_or_dotenv() { # <env-имя> <default>
  local v="${!1:-}"
  [ -n "$v" ] && { echo "$v"; return; }
  v="$(awk -F= -v k="$1" '$1==k{print $2}' "$ROOT/deploy/.env" 2>/dev/null)"
  echo "${v:-$2}"
}
PGW_PORT1="$(env_or_dotenv PGW_API_HOST_PORT 8080)"
PGW_PORT2="$(env_or_dotenv PGW_API_HOST_PORT2 8083)"
# Контейнер первого инстанса pgw: compose-проект deploy (00-up.sh поднимает из
# deploy/); env-оверрайд на случай нестандартного имени проекта.
PGW1="${PGW1_CONTAINER:-deploy-pgworker-1}"
MTLS="curl -fsS -m 3 --cacert $ROOT/deploy/tls/ca.pem --cert $ROOT/deploy/tls/healthcheck.crt --key $ROOT/deploy/tls/healthcheck.key"
ect() { docker compose exec -T etcd1 etcdctl --endpoints=http://localhost:2379 "$@"; }
keys() { ect get "$1" --prefix --keys-only 2>/dev/null | grep -c . || true; }
wait_keys() { # <prefix> <want> <budget-sec> <label>
  local got=""
  for _ in $(seq 1 $(( $3 / 2 ))); do
    got="$(keys "$1")"
    [ "${got:-0}" = "$2" ] && return 0
    sleep 2
  done
  echo "❌ $4: keys $1 = ${got:-?}, expected $2 within $3 s"; return 1
}

BASE="${ADMINPANEL_URL:-http://localhost:5050}"
JAR="$(mktemp)"; trap 'rm -f "$JAR"' EXIT
curl -fsS -c "$JAR" -o /dev/null -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  || { echo "❌ login (adminpanel at $BASE? bring up the stand: checks/00-up.sh)"; exit 1; }
api() { curl -s -o /dev/null -w '%{http_code}' -b "$JAR" "$BASE$1"; }

# ===== 1) PgWorker (deploy-контейнеры) =====
echo ">>> pgworker: both instances alive (Arrange)"
$MTLS https://localhost:${PGW_PORT1}/healthz >/dev/null || { echo "❌ pgworker-1 not alive (:${PGW_PORT1}/healthz mTLS; is the stand up?)"; exit 1; }
$MTLS https://localhost:${PGW_PORT2}/healthz >/dev/null || { echo "❌ pgworker-2 not alive (:${PGW_PORT2}/healthz mTLS; did 00-up.sh bring up both?)"; exit 1; }
wait_keys /pgworker/api/ 2 30 "Arrange pgworker"

echo ">>> pgworker: docker stop of the first ($PGW1)"
docker stop "$PGW1" >/dev/null
# ключи первого гаснут ≤ lease TTL 15 c — остаётся ровно один (выживший)
wait_keys /pgworker/api/ 1 60 "takeover pgworker: first instance key cleared"
survivor="$(ect get /pgworker/api/ --prefix --keys-only 2>/dev/null | head -1 | awk -F/ '{print $NF}')"
[ -n "$survivor" ] || { echo "❌ surviving instance not found (/pgworker/api/)"; exit 1; }
$MTLS https://localhost:${PGW_PORT2}/healthz >/dev/null \
  || { echo "❌ surviving pgworker-2 not alive (:${PGW_PORT2}/healthz)"; exit 1; }
[ "$(keys /pgworker/instances/)" = "1" ] || { echo "❌ /pgworker/instances/ did not shrink to 1"; exit 1; }
# клэйм живого кластера стенда (demo) — у выжившего; кластера нет → пропуск
claim="$(ect get /pgworker/claims/demo --print-value-only 2>/dev/null || true)"
if [ -n "$claim" ]; then
  echo "$claim" | jq -e --arg i "$survivor" '.instance == $i' >/dev/null \
    || { echo "❌ demo claim held not by the survivor: $claim (survivor=$survivor)"; exit 1; }
  echo "  demo claim held by the surviving instance ${survivor:0:8}…"
else
  echo "  (no demo cluster — claims assert skipped)"
fi
# панель жива и видит инстансы (не 503; карточка pgworker деградировала до 1)
code="$(api /api/workers)"
[ "$code" = 200 ] || { echo "❌ /api/workers = $code with the second instance alive (expected 200)"; exit 1; }
echo "  supervision on the second: keys cleared ≤60 s, healthz :${PGW_PORT2} alive, adminpanel 200"

echo ">>> pgworker: docker start of the first (Restore)"
docker start "$PGW1" >/dev/null
wait_keys /pgworker/api/ 2 60 "restore pgworker"
$MTLS https://localhost:${PGW_PORT1}/healthz >/dev/null \
  || { echo "❌ first pgworker did not come alive after start (:${PGW_PORT1}/healthz)"; exit 1; }
echo "  ✓ pgworker: both instances alive again"

# ===== 2) KafkaWorker (стендовые as-*) =====
echo ">>> kafkaworker: both instances alive (Arrange)"
[ "$(keys /kafkaworker/api/)" = "2" ] || { echo "❌ /kafkaworker/api/ != 2 keys (is the kafka profile up?)"; exit 1; }
docker stop as-kafkaworker >/dev/null
wait_keys /kafkaworker/api/ 1 60 "takeover kafkaworker"
docker exec as-kafkaworker-2 curl -fsS -m 3 \
  --cacert /tls/ca.pem --cert /tls/healthcheck.crt --key /tls/healthcheck.key \
  https://localhost:8080/healthz >/dev/null \
  || { echo "❌ surviving as-kafkaworker-2 not alive (/healthz mTLS from inside)"; exit 1; }
echo "  supervision on as-kafkaworker-2: first instance key cleared ≤60 s, healthz alive"
docker start as-kafkaworker >/dev/null
wait_keys /kafkaworker/api/ 2 60 "restore kafkaworker"
echo "  ✓ kafkaworker: both instances alive again"

# ===== 3) ValkeyWorker (стендовые as-*) =====
echo ">>> valkeyworker: both instances alive (Arrange)"
[ "$(keys /valkeyworker/api/)" = "2" ] || { echo "❌ /valkeyworker/api/ != 2 keys (is the valkey profile up?)"; exit 1; }
docker stop as-valkeyworker >/dev/null
wait_keys /valkeyworker/api/ 1 60 "takeover valkeyworker"
docker exec as-valkeyworker-2 curl -fsS -m 3 \
  --cacert /tls/ca.pem --cert /tls/healthcheck.crt --key /tls/healthcheck.key \
  https://localhost:8080/healthz >/dev/null \
  || { echo "❌ surviving as-valkeyworker-2 not alive (/healthz mTLS from inside)"; exit 1; }
echo "  supervision on as-valkeyworker-2: first instance key cleared ≤60 s, healthz alive"
docker start as-valkeyworker >/dev/null
wait_keys /valkeyworker/api/ 2 60 "restore valkeyworker"
echo "  ✓ valkeyworker: both instances alive again"

echo "✓ 35-worker-second-instance: kill→takeover of all three workers green (stand back to its original state)"
