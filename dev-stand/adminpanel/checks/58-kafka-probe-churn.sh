#!/usr/bin/env bash
# 58-kafka-probe-churn.sh (t11): kafka-пробы панели не жгут CPU на недоступных
# брокерах. Репро инцидента as-adminpanel 2026-09-02: Active-кластер с
# endpoints на закрытые порты → ~99% ядра (churn Confluent-клиентов «один на
# вызов» + reconnect-шторм librdkafka + блокирующий Dispose). Фикс: кэш
# клиентов KafkaClientCache + backoff'ы librdkafka ≥1 c + экспоненциальный
# backoff недоступных кластеров в KafkaProbeLoop (15→60→300 c, сброс при
# успехе, состояние видно в probeError). Проверки за окно наблюдения
# CHURN_MINUTES (по умолчанию 5; приёмка t11 — 15):
# (1) проба кластера честно failed, probeError несёт backoff-пометку (UI);
# (2) CPU панели (docker stats) ≤ 10% (инцидент ~99%; приёмка ≤5% ядра);
# (3) rdkafka-строк в логе ≤ 1/мин (после фикса — 0: лог уведён на Debug);
# (4) число потоков процесса не растёт.
# Профиль: достаточно quick (etcd + панель), образ панели — с фиксом t11
# (00-up.sh или docker compose build adminpanel && up -d adminpanel).
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${ADMINPANEL_URL:-http://localhost:5050}"
CHURN_MINUTES="${CHURN_MINUTES:-5}"
# Порты репро — закрытые (никто не слушает): не пересекаются со стендом
# (15000-151xx pg, 16000-161xx kafka, 18xxx patroni) и с хост-сервисами.
CHURN_PORTS="${CHURN_PORTS:-24997 24998 24999}"
CLUSTER="churn"

JAR="$(mktemp)"; trap 'rm -f "$JAR"' EXIT
curl -fsS -c "$JAR" -o /dev/null -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  || { echo "❌ login (панель на $BASE? поднимите 00-up.sh)"; exit 1; }
api() { curl -fsS -b "$JAR" "$BASE$1"; }
ect() { docker compose exec -T etcd etcdctl --endpoints=http://localhost:2379 "$@"; }

docker inspect as-adminpanel >/dev/null 2>&1 \
  || { echo "❌ контейнер as-adminpanel не найден — поднимите стенд (00-up.sh)"; exit 1; }

# Preconditions: порты действительно закрыты (иначе «репро» ничего не репает).
for port in $CHURN_PORTS; do
  if (echo > "/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
    echo "❌ порт $port занят — выберите свободные: CHURN_PORTS=\"...\" ./58-kafka-probe-churn.sh"; exit 1
  fi
done
# Чистый слейт от прошлого прогона.
ect del "/kafka/clusters/$CLUSTER" --prefix >/dev/null 2>&1 || true

# Act: сид репро-расклада инцидента — Active-кластер (без config → Active),
# 3 endpoint'а на закрытые порты + app-креды.
bootstrap="$(printf 'host.docker.internal:%s,' $CHURN_PORTS)"; bootstrap="${bootstrap%,}"
ect put "/kafka/clusters/$CLUSTER/endpoints" "$bootstrap" >/dev/null
ect put "/kafka/clusters/$CLUSTER/app_user" "app" >/dev/null
ect put "/kafka/clusters/$CLUSTER/app_password" "deadbeef" >/dev/null
echo ">>> сид: /kafka/clusters/$CLUSTER → $bootstrap (порты закрыты), окно ${CHURN_MINUTES} мин"

# Assert 1: проба честно failed и кластер виден панели (снапшот 3 c + тик 15 c).
probe_error=""
for i in $(seq 1 30); do
  probe_error="$(api "/api/kafka/clusters/$CLUSTER" 2>/dev/null | jq -r '.probeError // empty')" && [ -n "$probe_error" ] && break
  sleep 2
done
[ -n "$probe_error" ] || { echo "❌ кластер $CLUSTER/probeError не появился за 60 c"; ect del "/kafka/clusters/$CLUSTER" --prefix >/dev/null; exit 1; }
echo "  проба failed: ${probe_error:0:80}…"

# Наблюдение: CPU каждые 30 c, стартовые потоки; backoff-пометка ждёт 2 неудач
# и пропущенный тик (≈75 c от сида — внутри окна).
threads_start="$(docker exec as-adminpanel sh -c 'ls /proc/1/task | wc -l')"
cpu_max=0
for i in $(seq 1 "$((CHURN_MINUTES * 2))"); do
  cpu="$(docker stats --no-stream --format '{{.CPUPerc}}' as-adminpanel | tr -d '%\n' | cut -d. -f1)"
  [ "${cpu:-0}" -gt "$cpu_max" ] && cpu_max="$cpu"
  sleep 30
done

probe_error="$(api "/api/kafka/clusters/$CLUSTER" | jq -r '.probeError // empty')"
threads_end="$(docker exec as-adminpanel sh -c 'ls /proc/1/task | wc -l')"
rdkafka_lines="$(docker logs --since "${CHURN_MINUTES}m" as-adminpanel 2>&1 | grep -ci rdkafka || true)"

# Assert 2: backoff-состояние видно в UI-поле probeError (кластер не мерцает).
echo "$probe_error" | grep -qi "backoff" \
  || { echo "❌ probeError без backoff-пометки: $probe_error"; ect del "/kafka/clusters/$CLUSTER" --prefix >/dev/null; exit 1; }
echo "  probeError несёт backoff: ${probe_error: -80}"

# Assert 3: CPU ≤ 10% (инцидент ~99%, приёмка ≤5% ядра).
[ "$cpu_max" -le 10 ] \
  || { echo "❌ CPU панели до ${cpu_max}% (бюджет 10%)"; ect del "/kafka/clusters/$CLUSTER" --prefix >/dev/null; exit 1; }
echo "  CPU панели ≤ ${cpu_max}% за окно"

# Assert 4: rdkafka-лог ≤ 1 события/мин на кластер (после фикса — 0).
allowed="$CHURN_MINUTES"
[ "$rdkafka_lines" -le "$allowed" ] \
  || { echo "❌ rdkafka-строк $rdkafka_lines за ${CHURN_MINUTES} мин (бюджет $allowed)"; ect del "/kafka/clusters/$CLUSTER" --prefix >/dev/null; exit 1; }
echo "  rdkafka-строк в логе: $rdkafka_lines (бюджет ≤$allowed)"

# Assert 5: потоки процесса не растут (churn poll-потоков погашен).
[ "$threads_end" -le "$((threads_start + 10))" ] \
  || { echo "❌ потоки: $threads_start → $threads_end (растут)"; ect del "/kafka/clusters/$CLUSTER" --prefix >/dev/null; exit 1; }
echo "  потоки процесса стабильны: $threads_start → $threads_end"

# Cleanup: демонтаж сида, кластер уходит из панели (снапшот 3 c).
ect del "/kafka/clusters/$CLUSTER" --prefix >/dev/null
for i in $(seq 1 15); do
  api "/api/kafka/clusters/$CLUSTER" >/dev/null 2>&1 || { echo "✅ 58-kafka-probe-churn: мёртвый кластер не жжёт CPU (≤${cpu_max}%), лог тих (${rdkafka_lines}), потоки стабильны"; exit 0; }
  sleep 2
done
echo "⚠️ сид убран из etcd, кластер ещё виден в API — дождитесь тика снапшота"; exit 1
