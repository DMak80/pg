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
# mTLS (t03): /healthz воркера — только по https с клиентским сертом healthcheck.
ROOT="$(cd ../.. && pwd)"
TLS_DIR="$ROOT/deploy/tls"
WORKER_HEALTHZ="${KAFKAWORKER_HEALTHZ:-https://localhost:8082/healthz}"
JAR="$(mktemp)"; trap 'rm -f "$JAR"' EXIT

curl -fsS -c "$JAR" -o /dev/null -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  || { echo "❌ login (панель на $BASE?)"; exit 1; }
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
  echo "❌ $label не достигнуто за 120 c ($fn/$want)"; return 1
}
wait_healthz() { for i in $(seq 1 30); do curl -fsS -o /dev/null --cacert "$TLS_DIR/ca.pem" --cert "$TLS_DIR/healthcheck.crt" --key "$TLS_DIR/healthcheck.key" "$WORKER_HEALTHZ" && return 0; sleep 1; done; echo "❌ $WORKER_HEALTHZ не вернулся в 200 за 30 c"; return 1; }
ect() { docker compose exec -T etcd etcdctl --endpoints=http://localhost:2379 "$@"; }

# Preconditions: lease-ключи живы, воркер здоров, worker-* алертов нет.
[ -n "$(ect get /kafkaworker/api/ --prefix --keys-only </dev/null 2>/dev/null)" ] \
  || { echo "❌ нет живых /kafkaworker/api/ — поднимите стенд (00-up.sh, full+kafka)"; exit 1; }
curl -fsS -o /dev/null --cacert "$TLS_DIR/ca.pem" --cert "$TLS_DIR/healthcheck.crt" --key "$TLS_DIR/healthcheck.key" "$WORKER_HEALTHZ" || { echo "❌ /healthz воркера не 200 до стимула"; exit 1; }
! has_unhealthy || { echo "❌ уже есть worker-unhealthy — прогоните после чистого 00-up.sh"; exit 1; }
! has_api_down  || { echo "❌ уже есть worker-api-unreachable — стенд не согласован"; exit 1; }
started_before="$(docker inspect -f '{{.State.StartedAt}}' as-kafkaworker)"

# Act 1: transient — etcd лежит ~40 c (поллер успевает 2+ раза записать Degraded
# в стор; снапшот при этом НЕ обновляется — алерта ещё нет, spec §3.5).
echo ">>> docker compose stop etcd (~40 c)"
docker compose stop -t 3 etcd >/dev/null
sleep 40

# Act 2: подъём etcd.
docker compose start etcd >/dev/null

# Assert 1: алерт загорается первым успешным kafka-тиком после подъёма (≤ 120 c).
wait_state has_unhealthy present "алерт worker-unhealthy:kafkaworker" \
  && echo "  алерт worker-unhealthy:kafkaworker загорелся (первый успешный kafka-тик)"

# Assert 2: /healthz → 200 ≤ 30 c, контейнер НЕ рестартован (sticky-сброс).
wait_healthz && echo "  /healthz → 200 после подъёма etcd"
started_after="$(docker inspect -f '{{.State.StartedAt}}' as-kafkaworker)"
[ "$started_before" = "$started_after" ] \
  || { echo "❌ контейнер as-kafkaworker рестартован — сброс sticky-StatusError не доказан"; exit 1; }
echo "  контейнер as-kafkaworker не рестартован (ошибка тика сброшена успешным тиком)"

# Assert 3: алерт гаснет после восстановления (≤ 2 тиков поллера).
wait_state has_unhealthy absent "гашение worker-unhealthy:kafkaworker" \
  && echo "  алерт worker-unhealthy:kafkaworker погас"

# Assert 4: эстафета worker-api-unreachable не зависла (lease-ключи вернулись).
wait_state has_api_down absent "отсутствие worker-api-unreachable" \
  && echo "  worker-api-unreachable не висит (ключи /kafkaworker/api/ восстановлены)"

echo "✅ 57-kafka-worker-health: /healthz честный (последний тик), панель и docker-health согласованы"
