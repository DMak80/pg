#!/usr/bin/env bash
# 57-kafka-worker-health.sh (t09; spec §3.5): честность /healthz KafkaWorker и
# единая правда для панели. Transient-стимул — stop etcd ~40 c (≥ 2 тиков
# поллера 15 c): тики/пробы воркера падают при живом процессе. Проверки:
# (1) после подъёма etcd /healthz воркера → 200 БЕЗ рестарта контейнера
#     (сброс sticky-StatusError, живой-Ф7);
# (2) алерт worker-unhealthy:kafkaworker загорается ПЕРВЫМ УСПЕШНЫМ тиком
#     kafka-refresher'а после подъёма etcd (поллер за downtime накопил в стор
#     Degraded; FailTick его не вносит — семантика pg-симметрии) и гаснет
#     ≤ 2 тиков поллера после восстановления;
# (3) worker-api-unreachable не зависает (lease-ключи возвращаются ≤ TTL+тик).
# Профиль: full + kafka (после 55-го, перед 90-down).
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${ADMINPANEL_URL:-http://localhost:5050}"
# /healthz воркера — открытая https-грань без клиентского серта (arch/16 §1.1).
ROOT="$(cd ../.. && pwd)"
TLS_DIR="$ROOT/deploy/tls"
WORKER_HEALTHZ="${KAFKAWORKER_HEALTHZ:-https://localhost:8082/healthz}"
WORKER_API="${KAFKAWORKER_API:-https://localhost:8082/api/kafka/clusters}"
JAR="$(mktemp)"; trap 'rm -f "$JAR"' EXIT

curl -fsS -c "$JAR" -o /dev/null -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  || { echo "❌ login (adminpanel at $BASE?)"; exit 1; }
api() { curl -fsS -b "$JAR" "$BASE$1"; }

# Предикаты (target'ы разные!): worker-unhealthy → target "kafkaworker/<id>"
# (инстансы); worker-api-unreachable → точный target "kafkaworker" (домен
# целиком, без слэша — KafkaAlertEngine).
has_unhealthy() { api /api/alerts | jq -e 'any(.[]; .kind=="worker-unhealthy" and (.target|startswith("kafkaworker/")))' >/dev/null; }
has_api_down()  { api /api/alerts | jq -e 'any(.[]; .kind=="worker-api-unreachable" and .target=="kafkaworker")' >/dev/null; }

# wait_state <предикат> <present|absent> <label>: поллинг 2 c, бюджет 120 c.
wait_state() {
  local fn="$1" want="$2" label="$3"
  for i in $(seq 1 60); do
    if [ "$want" = present ] && "$fn"; then return 0; fi
    if [ "$want" = absent ] && ! "$fn"; then return 0; fi
    sleep 2
  done
  echo "❌ $label not reached within 120 s ($fn/$want)"; return 1
}
wait_healthz() { for i in $(seq 1 30); do curl -fsS -o /dev/null --cacert "$TLS_DIR/ca.pem" "$WORKER_HEALTHZ" && return 0; sleep 1; done; echo "❌ $WORKER_HEALTHZ did not return to 200 within 30 s"; return 1; }
ect() { docker compose exec -T etcd1 etcdctl --endpoints=http://localhost:2379 "$@"; }

# Preconditions: lease-ключи живы, воркер здоров, worker-* алертов нет.
[ -n "$(ect get /kafkaworker/api/ --prefix --keys-only </dev/null 2>/dev/null)" ] \
  || { echo "❌ no live /kafkaworker/api/ — bring up the stand (00-up.sh, full+kafka)"; exit 1; }
curl -fsS -o /dev/null --cacert "$TLS_DIR/ca.pem" "$WORKER_HEALTHZ" || { echo "❌ worker /healthz not 200 before the stimulus"; exit 1; }
# /api без клиентского серта — ровно 401 (mTLS-политика /api, arch/16 §1.1;
# /healthz выше — открытая грань). Без -f: 401 для curl — не ошибка транспорта.
api_no_cert_code="$(curl -s -m 3 -o /dev/null -w '%{http_code}' --cacert "$TLS_DIR/ca.pem" "$WORKER_API")"
[ "$api_no_cert_code" = 401 ] || { echo "❌ worker /api without a client cert = $api_no_cert_code (expected 401)"; exit 1; }
! has_unhealthy || { echo "❌ worker-unhealthy already present — run after a clean 00-up.sh"; exit 1; }
! has_api_down  || { echo "❌ worker-api-unreachable already present — stand inconsistent"; exit 1; }
started_before="$(docker inspect -f '{{.State.StartedAt}}' as-kafkaworker)"

# Act 1: transient — etcd лежит ~40 c (поллер успевает 2+ раза записать Degraded
# в стор; снапшот при этом НЕ обновляется — алерта ещё нет, spec §3.5).
echo ">>> docker compose stop etcd (~40 s)"
docker compose stop -t 3 etcd >/dev/null
sleep 40

# Act 2: подъём etcd.
docker compose start etcd >/dev/null

# Assert 1: алерт загорается первым успешным kafka-тиком после подъёма (≤ 120 c).
wait_state has_unhealthy present "alert worker-unhealthy:kafkaworker" \
  && echo "  alert worker-unhealthy:kafkaworker fired (first successful kafka tick)"

# Assert 2: /healthz → 200 ≤ 30 c, контейнер НЕ рестартован (sticky-сброс).
wait_healthz && echo "  /healthz → 200 after etcd came back"
started_after="$(docker inspect -f '{{.State.StartedAt}}' as-kafkaworker)"
[ "$started_before" = "$started_after" ] \
  || { echo "❌ container as-kafkaworker restarted — sticky-StatusError reset not proven"; exit 1; }
echo "  container as-kafkaworker not restarted (tick error cleared by a successful tick)"

# Assert 3: алерт гаснет после восстановления (≤ 2 тиков поллера).
wait_state has_unhealthy absent "worker-unhealthy:kafkaworker clearing" \
  && echo "  alert worker-unhealthy:kafkaworker cleared"

# Assert 4: эстафета worker-api-unreachable не зависла (lease-ключи вернулись).
wait_state has_api_down absent "worker-api-unreachable absence" \
  && echo "  worker-api-unreachable not stuck (keys /kafkaworker/api/ restored)"

echo "✅ 57-kafka-worker-health: /healthz honest (last tick), panel and docker-health consistent"
