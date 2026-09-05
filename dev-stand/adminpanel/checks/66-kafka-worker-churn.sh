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
# t03-классификация (SecurityMigrator.NeedsMigration): без ca_pem/ca_key/
# admin_password кластер премиграционный — тик перехватывает migrate-security
# (падает на «не закреплён в portalloc») и до надзора с лестницей E9 дело не
# доходит. Сеем одноразовую CA-пару + админ-креды — кластер канонический,
# Active-ветка достигает лестницы E9.
CA_TMP="$(mktemp -d)"
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout "$CA_TMP/ca.key" -out "$CA_TMP/ca.pem" -days 3650 \
  -subj "/CN=kfw-$CLUSTER-ca" >/dev/null 2>&1
# PEM начинается с «-----» — etcdctl счёл бы аргумент флагом, поэтому значение
# подаётся через stdin (docker exec -i), а не позиционным аргументом.
docker exec -i as-etcd etcdctl --endpoints=http://localhost:2379 \
  put "/kafka/clusters/$CLUSTER/ca_pem" < "$CA_TMP/ca.pem" >/dev/null
docker exec -i as-etcd etcdctl --endpoints=http://localhost:2379 \
  put "/kafka/clusters/$CLUSTER/ca_key" < "$CA_TMP/ca.key" >/dev/null
ect put "/kafka/clusters/$CLUSTER/admin_user" "admin" >/dev/null
ect put "/kafka/clusters/$CLUSTER/admin_password" "deadbeef" >/dev/null
rm -rf "$CA_TMP"
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

# Окно наблюдения — в steady-state: лестница (ветка 3) реально создаёт брокера,
# его бут (docker + provisioning: SASL/ACL/converge) — легитимный пик, а не
# churn-инцидент. Ждём «Kafka Server started»; если брокер не встал — окно
# всё равно замерит честный CPU лежащего/нездорового кластера (backoff).
for i in $(seq 1 30); do
  docker logs "kfw-$CLUSTER-broker1" 2>&1 | grep -q "Kafka Server started" && break
  sleep 5
done
sleep 10

# Наблюдение: CPU каждые 15 c в массив + стартовые потоки (окно — после сида,
# чтобы тики воркера уже прошли по лежащему кластеру минимум раз).
threads_start="$(docker exec "$WORKER" sh -c 'ls /proc/1/task | wc -l')"
cpu_samples=()
for i in $(seq 1 "$((CHURN_MINUTES * 4))"); do
  cpu_samples+=("$(docker stats --no-stream --format '{{.CPUPerc}}' "$WORKER" | tr -d '%\n' | cut -d. -f1)")
  sleep 15
done
threads_end="$(docker exec "$WORKER" sh -c 'ls /proc/1/task | wc -l')"
rdkafka_lines="$(docker logs --since "${CHURN_MINUTES}m" "$WORKER" 2>&1 | grep -ci rdkafka || true)"

# Assert 2: инцидент — ПОСТОЯННОЕ выедание (~99% ядра часами подряд);
# одиночные burst'ы (бут вылеченного брокера, provisioning, ротация) — норма.
# Критерий устойчивого выедания: доля образцов >10% больше трети окна
# ИЛИ среднее за окно >10%.
cpu_max=0; cpu_sum=0; cpu_hot=0
for v in "${cpu_samples[@]}"; do
  v="${v:-0}"
  [ "$v" -gt "$cpu_max" ] && cpu_max="$v"
  cpu_sum=$((cpu_sum + v))
  [ "$v" -gt 10 ] && cpu_hot=$((cpu_hot + 1))
done
cpu_n=${#cpu_samples[@]}
cpu_avg=$((cpu_sum / cpu_n))
[ "$cpu_hot" -le $((cpu_n / 3)) ] || { echo "❌ CPU >10% в ${cpu_hot}/${cpu_n} образцов — устойчивое выедание (макс ${cpu_max}%)"; exit 1; }
[ "$cpu_avg" -le 10 ] || { echo "❌ средний CPU ${cpu_avg}% за окно (бюджет 10%)"; exit 1; }
echo "  CPU воркера: среднее ${cpu_avg}%, макс ${cpu_max}% (${cpu_hot}/${cpu_n} образцов >10% — bursts)"

# Assert 3: rdkafka-лог <= 1 событие/мин (после фикса — 0: Debug).
allowed="$CHURN_MINUTES"
[ "$rdkafka_lines" -le "$allowed" ] || { echo "❌ rdkafka-строк $rdkafka_lines за ${CHURN_MINUTES} мин (бюджет $allowed)"; exit 1; }
echo "  rdkafka-строк в логе: $rdkafka_lines (бюджет ≤$allowed)"

# Assert 4: потоки процесса не растут (churn poll-потоков погашен).
[ "$threads_end" -le "$((threads_start + 10))" ] || { echo "❌ потоки: $threads_start → $threads_end (растут)"; exit 1; }
echo "  потоки процесса стабильны: $threads_start → $threads_end"

echo "✅ 66-kafka-worker-churn: лежащий кластер не жжёт CPU (≤${cpu_max}%), лог тих (${rdkafka_lines}), потоки стабильны, portalloc самолечится"
