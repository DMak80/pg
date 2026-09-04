#!/usr/bin/env bash
# 66-kafka-worker-churn.sh (t05): воркер не жжёт CPU на недоступном kafka-
# кластере. Репро инцидента as-kafkaworker 2026-09-04: Active-кластер,
# endpoints на закрытые порты, brokers declared, portalloc ПУСТ (тупик
# «не закреплён» + churn AdminClient ~100% ядра). Фикс: кэш клиентов +
# пины librdkafka >=1 c + backoff 15→60→300 c + лестница E9 portalloc.
# Проверки за окно CHURN_MINUTES (default 5; приёмка t05 — 15):
# (1) тупик лечится: /kafkaworker/portalloc/<C> появляется (лестница E9);
# (2) CPU as-kafkaworker (docker stats) <= 10% (приёмка <=5% ядра);
# (3) rdkafka-строк в логе <= 1/мин (после фикса — 0: Debug);
# (4) число потоков процесса стабильно (+<=10).
# Профиль: full+kafka (as-kafkaworker); образ kafkaworker — с фиксом t05
# (00-up.sh или docker compose build kafkaworker && up -d kafkaworker).
set -euo pipefail
cd "$(dirname "$0")/.."

CHURN_MINUTES="${CHURN_MINUTES:-5}"
# Закрытые порты — вне зон стенда (15000-151xx pg, 16000-161xx kafka,
# 18xxx patroni) и тестовых окон (21xxx+): 24997-24999, как 58-м.
CHURN_PORTS="${CHURN_PORTS:-24997 24998 24999}"
CLUSTER="churnkw"
WORKER="as-kafkaworker"

ect() { docker exec as-etcd etcdctl --endpoints=http://localhost:2379 "$@"; }
cleanup() {
  ect del "/kafka/clusters/$CLUSTER" --prefix >/dev/null 2>&1 || true
  ect del "/kafkaworker/portalloc/$CLUSTER" >/dev/null 2>&1 || true
  ect del "/kafkaworker/claims/$CLUSTER" >/dev/null 2>&1 || true
  ect del "/kafkaworker/work/$CLUSTER" >/dev/null 2>&1 || true
  # Лестница E9 (ветка 3) реально поднимает контейнер чек-кластера —
  # демонтаж без сирот: контейнер + том + per-cluster сеть.
  docker rm -f "kfw-$CLUSTER-broker1" >/dev/null 2>&1 || true
  docker volume rm "kfw-$CLUSTER-broker1-data" >/dev/null 2>&1 || true
  docker network rm "kfw-net-$CLUSTER" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker inspect "$WORKER" >/dev/null 2>&1 \
  || { echo "❌ контейнер $WORKER не найден — поднимите стенд (00-up.sh)"; exit 1; }

# Предусловие: порты действительно закрыты (иначе «репро» ничего не репает).
for port in $CHURN_PORTS; do
  if (echo > "/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
    echo "❌ порт $port занят — выберите свободные: CHURN_PORTS=\"...\" ./66-kafka-worker-churn.sh"; exit 1
  fi
done

# Сид репро-раскладки: Active-кластер (config без state) + broker1 RUNNING
# + endpoints на закрытые порты + креды; portalloc НЕ сидится (утерян).
bootstrap="$(printf 'host.docker.internal:%s,' $CHURN_PORTS)"; bootstrap="${bootstrap%,}"
ect put "/kafka/clusters/$CLUSTER/config" \
  '{"brokers":1,"replication_factor":1,"min_insync_replicas":1,"default_partitions":1,"default_retention_ms":3600000}' >/dev/null
ect put "/kafka/clusters/$CLUSTER/brokers/broker1/state" "RUNNING" >/dev/null
ect put "/kafka/clusters/$CLUSTER/endpoints" "$bootstrap" >/dev/null
ect put "/kafka/clusters/$CLUSTER/app_user" "app" >/dev/null
ect put "/kafka/clusters/$CLUSTER/app_password" "deadbeef" >/dev/null
echo ">>> сид: /kafka/clusters/$CLUSTER → $bootstrap (порты закрыты), portalloc нет; окно ${CHURN_MINUTES} мин"

# Assert 1: тупик лечится — portalloc появился (лестница E9: контейнера нет
# → S7-ветка → новая аллокация + контейнер; endpoints чек-кластера при этом
# обновится — это часть самолечения, cleanup гасит всё).
healed=""
for i in $(seq 1 36); do
  healed="$(ect get "/kafkaworker/portalloc/$CLUSTER" 2>/dev/null || true)"
  [ -n "$healed" ] && break
  sleep 5
done
[ -n "$healed" ] || { echo "❌ portalloc/$CLUSTER не появился за 180 с — лестница E9 не работает"; exit 1; }
echo "  лестница E9: portalloc восстановлен (${healed:0:60}…)"

# Наблюдение: CPU каждые 30 c + стартовые потоки (окно — после сида, чтобы
# тики воркера уже прошли по лежащему кластеру минимум раз).
threads_start="$(docker exec "$WORKER" sh -c 'ls /proc/1/task | wc -l')"
cpu_max=0
for i in $(seq 1 "$((CHURN_MINUTES * 2))"); do
  cpu="$(docker stats --no-stream --format '{{.CPUPerc}}' "$WORKER" | tr -d '%\n' | cut -d. -f1)"
  [ "${cpu:-0}" -gt "$cpu_max" ] && cpu_max="$cpu"
  sleep 30
done
threads_end="$(docker exec "$WORKER" sh -c 'ls /proc/1/task | wc -l')"
rdkafka_lines="$(docker logs --since "${CHURN_MINUTES}m" "$WORKER" 2>&1 | grep -ci rdkafka || true)"

# Assert 2: CPU <= 10% (инцидент ~99%, приёмка <=5% ядра).
[ "$cpu_max" -le 10 ] || { echo "❌ CPU воркера до ${cpu_max}% (бюджет 10%)"; exit 1; }
echo "  CPU воркера ≤ ${cpu_max}% за окно"

# Assert 3: rdkafka-лог <= 1 событие/мин (после фикса — 0: Debug).
allowed="$CHURN_MINUTES"
[ "$rdkafka_lines" -le "$allowed" ] || { echo "❌ rdkafka-строк $rdkafka_lines за ${CHURN_MINUTES} мин (бюджет $allowed)"; exit 1; }
echo "  rdkafka-строк в логе: $rdkafka_lines (бюджет ≤$allowed)"

# Assert 4: потоки процесса не растут (churn poll-потоков погашен).
[ "$threads_end" -le "$((threads_start + 10))" ] || { echo "❌ потоки: $threads_start → $threads_end (растут)"; exit 1; }
echo "  потоки процесса стабильны: $threads_start → $threads_end"

echo "✅ 66-kafka-worker-churn: лежащий кластер не жжёт CPU (≤${cpu_max}%), лог тих (${rdkafka_lines}), потоки стабильны, portalloc самолечится"
