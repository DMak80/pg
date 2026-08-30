#!/bin/sh
# Идемпотентный сид kafka-домена (план B8): РОВНО 2 кластера —
#   events  — Active: 3 брокера RUNNING (roles controller), endpoints, topics
#             (desired + missing), живая заявка ротации;
#   pending — NOT_INITIALIZED: config с state + 3 брокера-заявки.
# Состояние TO_REMOVE сид НЕ сеет: его создаёт сам чек 50 шагом DELETE.
# Профиль seed изолирован от профиля kafka (живой воркер): смешивание
# превращает сид в заявки для воркера — см. README.
set -eu

: "${ETCDCTL_ENDPOINTS:=http://etcd:2379}"
export ETCDCTL_ENDPOINTS
ECT() { etcdctl "$@"; }

# Идемпотентность: существующий config => уже засеяно — не портим.
if [ -n "$(ECT get /kafka/clusters/events/config --print-value-only 2>/dev/null)" ]; then
  echo "kafka-seed: /kafka/clusters уже засеян — пропускаю"
  exit 0
fi

now=$(date +%s)
put() { ECT put "$1" "$2" >/dev/null; }

echo "kafka-seed: пишу 2 кластера (unix=$now)"

# --- events: Active (state снят — семантика arch/15 §2.1) ---
put /kafka/clusters/events/config \
  '{"brokers":3,"replication_factor":3,"min_insync_replicas":2,"default_partitions":12,"default_retention_ms":604800000,"created_unix":1756500000}'
for k in 1 2 3; do
  put "/kafka/clusters/events/brokers/broker$k/state" 'RUNNING'
  put "/kafka/clusters/events/brokers/broker$k/role" 'controller'
  put "/kafka/clusters/events/brokers/broker$k/resources" '{"cpu":"2","mem":"4Gi","disk":"40Gi"}'
done
put /kafka/clusters/events/endpoints \
  'host.docker.internal:16001,host.docker.internal:16002,host.docker.internal:16003'
put /kafka/clusters/events/app_user 'app'
put /kafka/clusters/events/app_password 'SeEdPaSsWoRd0123456789AbCdEf'

# topics: факт без заявки / desired / missing (архетипы arch/15 §3)
put /kafka/clusters/events/topics/orders \
  '{"partitions":12,"replication_factor":3,"configs":{"retention.ms":"604800000","min.insync.replicas":"2"},"synced_unix":1750000100,"missing":false}'
put /kafka/clusters/events/topics/payments \
  '{"partitions":6,"replication_factor":3,"configs":{"retention.ms":"604800000"},"desired":{"partitions":12},"desired_unix":1750000010,"desired_by":"ops","synced_unix":1750000110,"missing":false}'
put /kafka/clusters/events/topics/ghost \
  '{"partitions":3,"replication_factor":1,"configs":{"retention.ms":"604800000"},"desired":{"configs":{"retention.ms":"86400000"}},"desired_unix":1750000200,"desired_by":"admin","synced_unix":1750000300,"missing":true}'

# lifecycle-заявки (t01, arch/15 §3.1): create без факт-ключа + delete на живой orders
put /kafka/clusters/events/topics/audit/desired.create \
  '{"partitions":12,"replication_factor":3,"configs":{"retention.ms":"86400000"},"requested_unix":1756501200,"requested_by":"seed"}'
put /kafka/clusters/events/topics/orders/desired.delete \
  '{"requested_unix":1756501300,"requested_by":"seed"}'

# Живая заявка ротации (чистится только исполнением/удалением кластера — A10)
put /kafkaworker/rotations/events \
  "{\"requested_unix\":$now,\"requested_by\":\"seed\"}"

# Ребалансировка (t02): живая заявка + drain-прогресс — парсер/UI/алерты
# видны без живого воркера (арх/15 §4).
put /kafkaworker/rebalances/events \
  '{"requested_unix":1756500123,"requested_by":"seed"}'
put /kafkaworker/reassignments/events \
  '{"mode":"drain","drain_broker":"broker2","partitions_total":6,"partitions_remaining":3,"submitted_unix":1756500130,"updated_unix":1756500135,"instance":"seed"}'

# --- pending: заявка NOT_INITIALIZED ---
put /kafka/clusters/pending/config \
  '{"brokers":3,"replication_factor":3,"min_insync_replicas":2,"default_partitions":12,"default_retention_ms":604800000,"created_unix":1756500900,"state":"NOT_INITIALIZED"}'
for k in 1 2 3; do
  put "/kafka/clusters/pending/brokers/broker$k/state" 'NOT_INITIALIZED'
  put "/kafka/clusters/pending/brokers/broker$k/resources" '{"cpu":"2","mem":"2Gi","disk":"20Gi"}'
done

# Самопроверка
[ -n "$(ECT get /kafka/clusters/events/config --print-value-only)" ] || { echo "kafka-seed: ❌ events/config не записан"; exit 1; }
[ -n "$(ECT get /kafka/clusters/pending/config --print-value-only)" ] || { echo "kafka-seed: ❌ pending/config не записан"; exit 1; }
[ -n "$(ECT get /kafkaworker/rotations/events --print-value-only)" ] || { echo "kafka-seed: ❌ заявка ротации не записана"; exit 1; }
[ -n "$(ECT get /kafkaworker/rebalances/events --print-value-only)" ] || { echo "kafka-seed: ❌ заявка ребалансировки не записана"; exit 1; }
[ -n "$(ECT get /kafkaworker/reassignments/events --print-value-only)" ] || { echo "kafka-seed: ❌ прогресс reassignment не записан"; exit 1; }
echo "kafka-seed: ✓ events (Active) + pending (NOT_INITIALIZED) + ротация/ребалансировка events"
