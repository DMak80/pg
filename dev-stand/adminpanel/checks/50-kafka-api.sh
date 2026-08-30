#!/usr/bin/env bash
# Kafka-API на сиде (план B8): сид активируется первым шагом (профиль seed,
# идемпотентен), панель — хост-процесс. Живой воркер НЕ запускается, контейнеров
# не поднимается. Guard «на брокере есть партиции» панель не проверяет (факта
# в etcd нет) — авторитетно его держит воркер (юнит-тесты B4); демонтаж
# непустого брокера останется ждать roadmap t02-kafka-reassignment.
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${ADMINPANEL_URL:-http://localhost:5050}"
JAR="$(mktemp)"; trap 'rm -f "$JAR"' EXIT

# Arrange: сид kafka-домена (идемпотентен; ошибки «профиль не поднимался» нет).
echo ">>> активирую kafka-сид (docker compose --profile seed run --rm kafka-seed)"
docker compose --profile seed run --rm kafka-seed

# Панель жива.
for i in $(seq 1 60); do curl -fsS "$BASE/api/healthz" >/dev/null 2>&1 && break; sleep 1; done
curl -fsS "$BASE/api/healthz" >/dev/null \
  || { echo "❌ панель не отвечает: $BASE/api/healthz"; exit 1; }
curl -fsS -c "$JAR" -o /dev/null -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  || { echo "❌ login admin/admin не прошёл"; exit 1; }
echo "  панель жива, login ok"

api()   { curl -fsS -b "$JAR" "$BASE$1"; }
code()  { curl -s -o /dev/null -w '%{http_code}' -b "$JAR" "$@"; }

# Kafka-снапшот панели собирается тиком 3 c — даём ему подхватить сид.
wait_clusters() {
  for i in $(seq 1 15); do
    api /api/kafka/clusters | jq -e 'length == 2' >/dev/null 2>&1 && break
    sleep 1
  done
  api /api/kafka/clusters | jq -e 'length == 2' >/dev/null \
    || { echo "❌ /api/kafka/clusters: ожидался 2 кластера сида"; exit 1; }
}
wait_clusters
echo "  GET /api/kafka/clusters: 2 кластера (events, pending)"

# 1) Сводка: events Active + 3/3 брокеров + ротация; pending NOT_INITIALIZED.
api /api/kafka/clusters | jq -e '
  ([.[] | select(.name=="events")][0].state == "ACTIVE")
   and ([.[] | select(.name=="events")][0].brokersRunning == 3)
   and ([.[] | select(.name=="events")][0].rotationPending == true)
   and ([.[] | select(.name=="pending")][0].state == "NOT_INITIALIZED")' >/dev/null \
  || { echo "❌ сводка: state/брокеры/ротация"; exit 1; }
echo "  сводка: events ACTIVE 3/3 + rotationPending, pending NOT_INITIALIZED"

# 2) Детали events: брокеры/topics/заявка ротации.
api /api/kafka/clusters/events | jq -e '
  (.brokersList | length) == 3
  and ([.brokersList[] | select(.name=="broker1")][0].role == "controller")
  and (.topics | length) == 3
  and ([.topics[] | select(.name=="ghost")][0].missing == true)
  and ([.topics[] | select(.name=="payments")][0].desired.partitions == 12)
  and (.rotation.requestedBy == "seed")' >/dev/null \
  || { echo "❌ детали events: brokers/topics/rotation"; exit 1; }
echo "  детали events: 3 брокера controller, 3 топика (desired/missing), ротация seed"

# 3) POST создать events → 409 (клэйм-txn занят).
c="$(code -X POST "$BASE/api/kafka/clusters" -H 'Content-Type: application/json' \
  -d '{"name":"events"}')"
[ "$c" = 409 ] || { echo "❌ POST events (повтор) = $c, ожидался 409"; exit 1; }
echo "  POST /api/kafka/clusters events (занято) -> 409"

# 4) PUT config events (retention) → 200.
curl -fsS -b "$JAR" -X PUT "$BASE/api/kafka/clusters/events/config" \
  -H 'Content-Type: application/json' -d '{"defaultRetentionMs":86400000}' \
  | jq -e '.defaultRetentionMs == 86400000' >/dev/null \
  || { echo "❌ PUT config events: retention не применился"; exit 1; }
echo "  PUT config events (retention 1д) -> 200"

# 5) POST brokers events → 201 broker4 (имя сгенерировано).
curl -fsS -b "$JAR" -X POST "$BASE/api/kafka/clusters/events/brokers" \
  -H 'Content-Type: application/json' -d '{"cpu":1,"memGi":2,"diskGi":20}' \
  | jq -e '.name == "broker4" and .state == "NOT_INITIALIZED"' >/dev/null \
  || { echo "❌ POST brokers: broker4 не сгенерирован"; exit 1; }
echo "  POST brokers events -> 201 broker4"

# 6) DELETE brokers/broker4 → 204 (только что заявленный — пустой по построению).
c="$(code -X DELETE "$BASE/api/kafka/clusters/events/brokers/broker4")"
[ "$c" = 204 ] || { echo "❌ DELETE broker4 = $c, ожидался 204"; exit 1; }
echo "  DELETE brokers/broker4 -> 204"

# 7) DELETE brokers/broker1 → 409 (controller-guard).
c="$(code -X DELETE "$BASE/api/kafka/clusters/events/brokers/broker1")"
[ "$c" = 409 ] || { echo "❌ DELETE broker1 (controller) = $c, ожидался 409"; exit 1; }
echo "  DELETE brokers/broker1 (controller) -> 409"

# 8) DELETE cluster pending → 204; после тика — бейдж TO_REMOVE.
c="$(code -X DELETE "$BASE/api/kafka/clusters/pending")"
[ "$c" = 204 ] || { echo "❌ DELETE pending = $c, ожидался 204"; exit 1; }
for i in $(seq 1 15); do
  api /api/kafka/clusters | jq -e '([.[] | select(.name=="pending")][0].state == "TO_REMOVE")' >/dev/null && break
  sleep 1
done
api /api/kafka/clusters | jq -e '([.[] | select(.name=="pending")][0].state == "TO_REMOVE")' >/dev/null \
  || { echo "❌ pending не получил бейдж TO_REMOVE"; exit 1; }
echo "  DELETE pending -> 204; бейдж TO_REMOVE виден"

# 9) Ротация events: заявка уже стоит (сид) → 409.
c="$(code -X POST "$BASE/api/kafka/clusters/events/app-password/rotate")"
[ "$c" = 409 ] || { echo "❌ POST rotate events = $c, ожидался 409 (заявка сида жива)"; exit 1; }
echo "  POST rotate events (заявка жива) -> 409"

# 10) Алерты: kafka-домен в общей ленте (kafka-rotation-pending: events).
for i in $(seq 1 15); do
  api /api/alerts | jq -e 'any(.[]; .kind=="kafka-rotation-pending" and .target=="events")' >/dev/null && break
  sleep 1
done
api /api/alerts | jq -e 'any(.[]; .kind=="kafka-rotation-pending" and .target=="events")' >/dev/null \
  || { echo "❌ /api/alerts: kafka-rotation-pending events не найден"; exit 1; }
echo "  /api/alerts: kafka-rotation-pending events в общей ленте"

echo "✓ 50-kafka-api: API kafka-домена на сиде — все шаги зелёные"
