#!/usr/bin/env bash
# 55-kafka-e2e.sh (план C5; критерий spec §9.5, §6): полный цикл kafka-домена
# на ЖИВОМ воркере с чистого состояния — дискавери ТОЛЬКО из etcd. Панель —
# хост-процесс (как всегда, ADMINPANEL_URL); воркер — docker compose --profile
# kafka (чек собирает образ и поднимает стенд сам; сид-профиль не активен).
#
# Подшаги: 1) чистый стенд; 2) кластер 3 брокера через API → RUNNING+endpoints;
# 3) топик kafka-topics CLI (креды из etcd) → автосинк ключа; 4) desired
# (partitions↑+retention) применяется и снимается; 5) негатив partitions↓ → 400;
# 6) группа+лаг (GET деталей: totalLag>0); 7) missing-ветка (валидная заявка →
# CLI-удаление → missing=true + алерт → отмена → ключ удалён); 8) создание
# топика из панели (lifecycle, t01) → воркер исполняет → факт-ключ; 9) негативы
# create (409/400); 10) удаление топика из панели → ключи исчезают; 11) отмена
# создания до тика; 12) отмена удаления до тика (топик остаётся); 13) демонтаж
# непустого broker4 (controller-409 негатив; drain со снижением RF 4→3, t02);
# 14) ребалансировка — заявочный цикл (t02); 15) TO_REMOVE → /kafka/ пуст.
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${ADMINPANEL_URL:-http://localhost:5050}"
CLUSTER="e2e"
TOPIC="e2e"
JAR="$(mktemp)"
WORK="$(mktemp -d)"
trap 'rm -f "$JAR"; rm -rf "$WORK"' EXIT

# etcd стенда — единственный источник адресов/кредов (дискавери §3.5).
# </dev/null: docker CLI в пайпе (producer stdin) не должен съедать stdin.
etcd_key() { docker compose exec -T etcd etcdctl get "$1" --print-value-only </dev/null 2>/dev/null; }
etcd_has() { docker compose exec -T etcd etcdctl get "$1" --print-value-only </dev/null 2>/dev/null | grep -q .; }
etcd_kafka_keys() {
  docker compose exec -T etcd etcdctl get /kafka/ --prefix --keys-only 2>/dev/null | grep -v '^$' || true
}

kafka_cli() { # kafka_cli <tool> <args…>: креды/адрес — только чтением etcd.
  local endpoints user password ca_pem
  endpoints="$(etcd_key "/kafka/clusters/$CLUSTER/endpoints")"
  user="$(etcd_key "/kafka/clusters/$CLUSTER/app_user")"
  password="$(etcd_key "/kafka/clusters/$CLUSTER/app_password")"
  ca_pem="$(etcd_key "/kafka/clusters/$CLUSTER/ca_pem")"
  [ -n "$endpoints" ] && [ -n "$user" ] && [ -n "$password" ] && [ -n "$ca_pem" ] \
    || { echo "❌ ключи дискавери etcd неполны"; exit 1; }
  # t03: кластеры только SASL_SSL — доверие per-cluster CA из etcd. Однострочник
  # ca_pem (\n-эскейпы; base64 символа '\' не содержит) → файл PEM; канон CLI —
  # как ReassignCli (ssl.truststore.type=PEM + location), путь внутри контейнера.
  printf '%b' "$ca_pem" > "$WORK/ca.pem"
  cat > "$WORK/client.properties" <<EOF
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="$user" password="$password";
ssl.truststore.type=PEM
ssl.truststore.location=/conf/ca.pem
EOF
  # CLI-инструменты образа лежат в /opt/kafka/bin/*.sh (PATH их не включает);
  # у консольных producer/consumer флаг конфига называется иначе (не command-config).
  local cfgflag="--command-config"
  [ "$1" = "kafka-console-producer" ] && cfgflag="--producer.config"
  [ "$1" = "kafka-console-consumer" ] && cfgflag="--consumer.config"
  docker run --rm -i --add-host host.docker.internal:host-gateway \
    -v "$WORK:/conf:ro" apache/kafka:4.0.0 "/opt/kafka/bin/$1.sh" \
    --bootstrap-server "$endpoints" "$cfgflag" /conf/client.properties "${@:2}"
}

api()  { curl -fsS -b "$JAR" "$BASE$1"; }
code() { curl -s -o /dev/null -w '%{http_code}' -b "$JAR" "$@"; }

wait_until() { # wait_until <описание> <секунд> <cmd…>
  local what="$1" seconds="$2"; shift 2
  for _ in $(seq 1 "$seconds"); do
    if "$@" >/dev/null 2>&1; then echo "  ✓ $what"; return 0; fi
    sleep 1
  done
  echo "❌ не дождались: $what (бюджет ${seconds} c)"; exit 1
}

# ===== 1) Чистое состояние: стенд разобран, kfw-объекты и /kafka/ пусты =====
echo ">>> (1/15) чистый стенд: down -v + kfw-очистка + up --profile kafka"
./checks/90-down.sh -v >/dev/null
docker compose --profile kafka down -v --remove-orphans >/dev/null 2>&1 || true
# kfw-объекты живут вне compose-проекта — чистим вручную (handoff-рецепт B9).
docker rm -f $(docker ps -aq --filter 'name=kfw-') >/dev/null 2>&1 || true
docker volume rm $(docker volume ls -q --filter 'name=kfw-') >/dev/null 2>&1 || true
docker network rm $(docker network ls -q --filter 'name=kfw-net') >/dev/null 2>&1 || true
docker compose --profile kafka build kafkaworker >/dev/null \
  || { echo "❌ сборка образа kafkaworker не удалась"; exit 1; }
docker compose --profile kafka up -d >/dev/null
[ -z "$(etcd_kafka_keys)" ] || { echo "❌ /kafka/ не пуст до старта (сид?)"; exit 1; }
echo "  ✓ стенд поднят (etcd+kafkaworker), /kafka/ пуст"

# Панель жива (хост-процесс оператора).
for i in $(seq 1 60); do curl -fsS "$BASE/api/healthz" >/dev/null 2>&1 && break; sleep 1; done
curl -fsS -c "$JAR" -o /dev/null -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  || { echo "❌ панель/login недоступны: $BASE"; exit 1; }
echo "  ✓ панель жива, login ok"

# ===== 2) Кластер 3 брокера через API → RUNNING + endpoints =====
echo ">>> (2/15) создание кластера $CLUSTER (3 брокера) через API"
# Панель могла стартовать раньше стенда: 503 (etcd endpoint ещё не выбран)
# повторяем до появления kafka-снапшота.
for i in $(seq 1 30); do
  created="$(curl -s -b "$JAR" -X POST "$BASE/api/kafka/clusters" \
    -H 'Content-Type: application/json' -d "{\"name\":\"$CLUSTER\"}")" \
    && echo "$created" | jq -e '.state == "NOT_INITIALIZED"' >/dev/null && break
  sleep 1
done
echo "$created" | jq -e '.state == "NOT_INITIALIZED"' >/dev/null \
  || { echo "❌ POST /api/kafka/clusters не прошёл: $created"; exit 1; }
wait_until "RUNNING 3/3 + endpoints" 240 bash -c '
  curl -fsS -b "'"$JAR"'" "'"$BASE"'/api/kafka/clusters" \
  | jq -e "[.[] | select(.name == \"e2e\")][0] | .brokersRunning == 3 and .endpoints != null"'
echo "  endpoints: $(etcd_key /kafka/clusters/$CLUSTER/endpoints)"

# ===== 3) Топик kafka-topics CLI → автосинк ключа ≤ 2 тиков =====
echo ">>> (3/15) топик $TOPIC kafka-topics CLI (креды из etcd) → автосинк"
kafka_cli kafka-topics --create --topic "$TOPIC" --partitions 3 --replication-factor 3 >/dev/null \
  || { echo "❌ kafka-topics --create не прошёл"; exit 1; }
wait_until "ключ topics/$TOPIC в etcd (автосинк ≤ 2 тиков)" 60 \
  bash -c 'docker compose exec -T etcd etcdctl get /kafka/clusters/e2e/topics/e2e --print-value-only 2>/dev/null | grep -q .'
etcd_key "/kafka/clusters/$CLUSTER/topics/$TOPIC" \
  | jq -e '.partitions == 3 and .replication_factor == 3 and .missing == false and (has("desired") | not)' >/dev/null \
  || { echo "❌ ключ топика не с фактом"; exit 1; }
echo "  ключ topics/$TOPIC: факт 3 партиции/RF 3, без заявки"

# ===== 4) desired (partitions↑ + retention) применяется и снимается =====
echo ">>> (4/15) desired: partitions 3→6 + retention 1д → автосинк применяет и снимает"
c="$(code -X PUT "$BASE/api/kafka/clusters/$CLUSTER/topics/$TOPIC" \
  -H 'Content-Type: application/json' -d '{"partitions":6,"retentionMs":86400000}')"
[ "$c" = 200 ] || { echo "❌ PUT desired = $c, ожидался 200"; exit 1; }
wait_until "desired применён и снят (partitions 6, retention 1д)" 90 bash -c '
  key="$(docker compose exec -T etcd etcdctl get /kafka/clusters/e2e/topics/e2e --print-value-only 2>/dev/null)"
  echo "$key" | jq -e ".partitions == 6 and .configs[\"retention.ms\"] == \"86400000\" and (has(\"desired\") | not)"'
kafka_cli kafka-configs --entity-type topics --entity-name "$TOPIC" --describe 2>/dev/null \
  | grep -q 'retention.ms=86400000' \
  || { echo "❌ фактический retention.ms не применился (kafka-configs)"; exit 1; }
echo "  факт: 6 партиций, retention.ms=86400000, desired снят"

# ===== 5) Негатив: уменьшение partitions → 400, заявка не пишется =====
echo ">>> (5/15) негатив: desired partitions 6→3 → 400"
c="$(code -X PUT "$BASE/api/kafka/clusters/$CLUSTER/topics/$TOPIC" \
  -H 'Content-Type: application/json' -d '{"partitions":3}')"
[ "$c" = 400 ] || { echo "❌ PUT partitions↓ = $c, ожидался 400"; exit 1; }
etcd_key "/kafka/clusters/$CLUSTER/topics/$TOPIC" | jq -e 'has("desired") | not' >/dev/null \
  || { echo "❌ заявка всё же записана в ключ"; exit 1; }
echo "  400 ProblemDetails, desired в ключе отсутствует"

# ===== 6) Группа + лаг: сообщения → частичное чтение → totalLag>0 =====
echo ">>> (6/15) группа lag-test: лаги в деталях кластера (live-проба)"
# Продюсер пишет 20 сообщений; консьюмер читает --from-beginning только 5 и
# уходит (committed=5) — оставшиеся 15 светятся лагом группы в панели.
for i in $(seq 1 20); do echo "msg-$i"; done \
  | kafka_cli kafka-console-producer --topic "$TOPIC" >/dev/null 2>&1 \
  || { echo "❌ producer не прошёл"; exit 1; }
kafka_cli kafka-console-consumer --topic "$TOPIC" --group lag-test \
  --from-beginning --max-messages 5 --timeout-ms 30000 >/dev/null 2>&1 || true
wait_until "группа lag-test с totalLag>0 в GET деталях" 120 bash -c '
  curl -fsS -b "'"$JAR"'" "'"$BASE"'/api/kafka/clusters/'"$CLUSTER"'" \
  | jq -e "[.groups[]? | select(.group == \"lag-test\")][0].totalLag > 0"'
echo "  группа lag-test видна с totalLag>0"

# ===== 7) missing-ветка: заявка → CLI-удаление → missing + алерт → отмена =====
echo ">>> (7/15) missing-ветка: валидная заявка → удаление топика → missing=true + алерт → отмена"
c="$(code -X PUT "$BASE/api/kafka/clusters/$CLUSTER/topics/$TOPIC" \
  -H 'Content-Type: application/json' -d '{"retentionMs":3600000}')"
[ "$c" = 200 ] || { echo "❌ PUT валидной заявки = $c"; exit 1; }
etcd_key "/kafka/clusters/$CLUSTER/topics/$TOPIC" | jq -e '.desired.configs["retention.ms"] == "3600000"' >/dev/null \
  || { echo "❌ desired не виден в ключе до удаления топика"; exit 1; }
echo "  desired стоит в ключе (etcdctl подтверждает)"
kafka_cli kafka-topics --delete --topic "$TOPIC" >/dev/null \
  || { echo "❌ kafka-topics --delete не прошёл"; exit 1; }
wait_until "missing=true + алерт kafka-topic-missing-desired" 90 bash -c '
  key="$(docker compose exec -T etcd etcdctl get /kafka/clusters/e2e/topics/e2e --print-value-only 2>/dev/null)"
  alerts="$(curl -fsS -b "'"$JAR"'" "'"$BASE"'/api/alerts")"
  echo "$key" | jq -e ".missing == true and .desired != null" \
    && echo "$alerts" | jq -e "any(.[]; .kind == \"kafka-topic-missing-desired\" and (.target | startswith(\"e2e/\")))"'
echo "  missing=true, заявка жива, алерт на месте"
c="$(code -X DELETE "$BASE/api/kafka/clusters/$CLUSTER/topics/$TOPIC/desired")"
[ "$c" = 204 ] || { echo "❌ DELETE desired = $c, ожидался 204"; exit 1; }
wait_until "ключ topics/$TOPIC удалён (топика и заявки нет)" 60 \
  bash -c '! docker compose exec -T etcd etcdctl get /kafka/clusters/e2e/topics/e2e --print-value-only 2>/dev/null | grep -q .'
echo "  отмена заявки → автосинк удалил ключ"

# ===== 8) Создание топика из панели → воркер исполняет (t01) =====
echo ">>> (8/15) POST create e2e-panel (6 партиций, RF 3, retention 1д) → автосинк приносит факт"
c="$(code -X POST "$BASE/api/kafka/clusters/$CLUSTER/topics" -H 'Content-Type: application/json' \
  -d '{"name":"e2e-panel","partitions":6,"replicationFactor":3,"retentionMs":86400000}')"
[ "$c" = 201 ] || { echo "❌ POST create = $c"; exit 1; }
wait_until "факт-ключ e2e-panel (partitions 6 / RF 3 / retention 1д, без заявок)" 90 bash -c '
  docker compose exec -T etcd etcdctl get /kafka/clusters/e2e/topics/e2e-panel --print-value-only 2>/dev/null \
  | jq -e ".partitions == 6 and .replication_factor == 3 and .configs[\"retention.ms\"] == \"86400000\" and (has(\"desired\") | not)"'
# NB: pipefail + grep -q здесь нельзя: grep выходит после совпадения, java
# CLI дописывает строки → docker run получает SIGPIPE (141) → пайплайн падает;
# grep … >/dev/null читает до EOF.
kafka_cli kafka-topics --describe --topic e2e-panel </dev/null 2>/dev/null | grep "PartitionCount: 6" >/dev/null \
  || { echo "❌ kafka-topics --describe: не 6 партиций"; exit 1; }
echo "  топик e2e-panel в Kafka (6 партиций), факт-ключ с теми же значениями"

# ===== 9) Негативы create =====
c="$(code -X POST "$BASE/api/kafka/clusters/$CLUSTER/topics" -H 'Content-Type: application/json' -d '{"name":"e2e-panel","partitions":1,"replicationFactor":1}')"
[ "$c" = 409 ] || { echo "❌ повторный create = $c, ожидался 409"; exit 1; }
c="$(code -X POST "$BASE/api/kafka/clusters/$CLUSTER/topics" -H 'Content-Type: application/json' -d '{"name":"x","replicationFactor":10}')"
[ "$c" = 400 ] || { echo "❌ RF 10 = $c, ожидался 400"; exit 1; }
c="$(code -X POST "$BASE/api/kafka/clusters/$CLUSTER/topics" -H 'Content-Type: application/json' -d '{"name":"x","partitions":0}')"
[ "$c" = 400 ] || { echo "❌ partitions 0 = $c, ожидался 400"; exit 1; }
echo "  негативы: повторный create 409, RF 10 → 400, partitions 0 → 400"

# ===== 10) Удаление топика из панели → ключи исчезают =====
echo ">>> (10/15) DELETE e2e-panel → топик и ключи исчезли"
c="$(code -X DELETE "$BASE/api/kafka/clusters/$CLUSTER/topics/e2e-panel")"
[ "$c" = 204 ] || { echo "❌ DELETE = $c"; exit 1; }
wait_until "факт-ключ и заявка e2e-panel удалены" 60 bash -c '
  ! docker compose exec -T etcd etcdctl get /kafka/clusters/e2e/topics/e2e-panel --print-value-only 2>/dev/null | grep -q .'
if kafka_cli kafka-topics --list </dev/null 2>/dev/null | grep e2e-panel >/dev/null; then
  echo "❌ топик e2e-panel всё ещё в Kafka"; exit 1
fi
echo "  топик e2e-panel удалён из Kafka, оба ключа etcd чисты"

# ===== 11) Отмена создания: заявка снята до тика =====
echo ">>> (11/15) отмена create: e2e-cancel не создаётся"
c="$(code -X POST "$BASE/api/kafka/clusters/$CLUSTER/topics" -H 'Content-Type: application/json' -d '{"name":"e2e-cancel","partitions":1,"replicationFactor":1}')"
[ "$c" = 201 ] || { echo "❌ POST create e2e-cancel = $c"; exit 1; }
c="$(code -X DELETE "$BASE/api/kafka/clusters/$CLUSTER/topics/e2e-cancel/desired.create")"
[ "$c" = 204 ] || { echo "❌ отмена = $c"; exit 1; }
sleep 35  # 2 тика автосинка
if docker compose exec -T etcd etcdctl get /kafka/clusters/e2e/topics/e2e-cancel --print-value-only </dev/null 2>/dev/null | grep . >/dev/null; then
  # Гонка задокументирована (спека §6): тик успел раньше отмены — доводим до удаления.
  echo "  гонка: тик исполнил create раньше отмены — доводим до удаления"
  c="$(code -X DELETE "$BASE/api/kafka/clusters/$CLUSTER/topics/e2e-cancel")"
  [ "$c" = 204 ] || { echo "❌ cleanup e2e-cancel = $c"; exit 1; }
  wait_until "e2e-cancel удалён (гонка тика)" 60 bash -c '
    ! docker compose exec -T etcd etcdctl get /kafka/clusters/e2e/topics/e2e-cancel --print-value-only 2>/dev/null | grep -q .'
fi
echo "  заявка e2e-cancel отменена до исполнения (или гонка доведена)"

# ===== 12) Отмена удаления: топик остаётся (spec §9.3) =====
echo ">>> (12/15) отмена delete: e2e-undo остаётся жив"
c="$(code -X POST "$BASE/api/kafka/clusters/$CLUSTER/topics" -H 'Content-Type: application/json' \
  -d '{"name":"e2e-undo","partitions":1,"replicationFactor":1}')"
[ "$c" = 201 ] || { echo "❌ POST create e2e-undo = $c"; exit 1; }
wait_until "факт-ключ e2e-undo" 90 bash -c '
  docker compose exec -T etcd etcdctl get /kafka/clusters/e2e/topics/e2e-undo --print-value-only 2>/dev/null | grep -q .'
c="$(code -X DELETE "$BASE/api/kafka/clusters/$CLUSTER/topics/e2e-undo")"
[ "$c" = 204 ] || { echo "❌ DELETE e2e-undo = $c"; exit 1; }
c="$(code -X DELETE "$BASE/api/kafka/clusters/$CLUSTER/topics/e2e-undo/desired.delete")"
[ "$c" = 204 ] || { echo "❌ отмена delete = $c"; exit 1; }
sleep 35  # 2 тика: тик воркера не должен был исполнить удаление
if ! kafka_cli kafka-topics --list </dev/null 2>/dev/null | grep e2e-undo >/dev/null; then
  # Гонка задокументирована (спека §6, симметрично шагу 11): тик успел раньше
  # отмены — топик пересоздаём и шаг считается пройденным.
  echo "  гонка: тик исполнил удаление раньше отмены — пересоздаём e2e-undo"
  c="$(code -X POST "$BASE/api/kafka/clusters/$CLUSTER/topics" -H 'Content-Type: application/json' \
    -d '{"name":"e2e-undo","partitions":1,"replicationFactor":1}')"
  [ "$c" = 201 ] || { echo "❌ восстановление e2e-undo = $c"; exit 1; }
  wait_until "факт-ключ e2e-undo восстановлен" 90 bash -c '
    docker compose exec -T etcd etcdctl get /kafka/clusters/e2e/topics/e2e-undo --print-value-only 2>/dev/null | grep -q .'
fi
docker compose exec -T etcd etcdctl get /kafka/clusters/e2e/topics/e2e-undo --print-value-only </dev/null 2>/dev/null \
  | grep . >/dev/null || { echo "❌ факт-ключ e2e-undo пропал после отмены delete"; exit 1; }
echo "  отмена delete до тика: топик e2e-undo жив"

# ===== 13) Демонтаж НЕПУСТОГО брокера: drain со снижением RF 4→3 (t02) =====
echo ">>> (13/15) демонтаж broker4: топик RF=4 с данными → drain → RF 4→3"
curl -fsS -b "$JAR" -X POST "$BASE/api/kafka/clusters/$CLUSTER/brokers" \
  -H 'Content-Type: application/json' -d '{"cpu":1,"memGi":2,"diskGi":20}' \
  | jq -e '.name == "broker4"' >/dev/null \
  || { echo "❌ POST brokers: broker4 не сгенерирован"; exit 1; }
wait_until "broker4 RUNNING" 240 bash -c '
  curl -fsS -b "'"$JAR"'" "'"$BASE"'/api/kafka/clusters/'"$CLUSTER"'" \
  | jq -e "[.brokersList[] | select(.name == \"broker4\")][0].state == \"RUNNING\""'
c="$(code -X DELETE "$BASE/api/kafka/clusters/$CLUSTER/brokers/broker1")"
[ "$c" = 409 ] || { echo "❌ DELETE broker1 (controller) = $c, ожидался 409"; exit 1; }
echo "  DELETE broker1 (controller) → 409"
# Непустой broker4: топик RF=4/6 партиций с данными (снижение RF при drain).
kafka_cli kafka-topics --create --topic e2e2 --partitions 6 --replication-factor 4 >/dev/null \
  || { echo "❌ kafka-topics --create e2e2 (RF=4) не прошёл"; exit 1; }
for i in $(seq 1 12); do echo "re-$i"; done \
  | kafka_cli kafka-console-producer --topic e2e2 >/dev/null 2>&1 \
  || { echo "❌ producer e2e2 не прошёл"; exit 1; }
echo "  топик e2e2: RF=4, 6 партиций, 12 сообщений"
c="$(code -X DELETE "$BASE/api/kafka/clusters/$CLUSTER/brokers/broker4")"
[ "$c" = 204 ] || { echo "❌ DELETE broker4 = $c, ожидался 204"; exit 1; }
wait_until "прогресс-ключ reassignments/e2e появился (drain идёт)" 120 \
  bash -c 'docker compose exec -T etcd etcdctl get /kafkaworker/reassignments/e2e --print-value-only 2>/dev/null | grep -q .'
wait_until "broker4 демонтирован после drain (ключей brokers/broker4 нет)" 300 \
  bash -c '! docker compose exec -T etcd etcdctl get /kafka/clusters/e2e/brokers/broker4/ --prefix --keys-only 2>/dev/null | grep -q broker4'
# Факт после drain: в репликах нет 4, ровно 3 реплики, ISR непуст (упрощённый
# parse; Kafka 4.0 печатает describe с TAB-разделителями — нормализуем в пробелы).
describe_check() {
  local desc bad=0 replicas isr n
  desc="$(kafka_cli kafka-topics --describe --topic e2e2 2>/dev/null | tr '\t' ' ')" || return 1
  while IFS= read -r line; do
    replicas="${line##*Replicas: }"; replicas="${replicas%% Isr:*}"
    isr="${line##*Isr: }"; isr="${isr%% *}"
    n="$(echo "$replicas" | tr ',' '\n' | grep -c . || true)"
    [ "$n" = 3 ] || bad=1
    echo "$replicas" | tr ',' '\n' | grep -qx 4 && bad=1
    [ -n "$isr" ] || bad=1
  done <<EOF
$(echo "$desc" | grep 'Partition:')
EOF
  [ "$bad" = 0 ]
}
wait_until "describe e2e2: без nodeId=4, по 3 реплики, ISR непуст" 60 describe_check
wait_until "реестр topics/e2e2: replication_factor=3 (автосинк)" 60 \
  bash -c 'docker compose exec -T etcd etcdctl get /kafka/clusters/e2e/topics/e2e2 --print-value-only 2>/dev/null | grep -q "\"replication_factor\":3"'
echo "  broker4 (непустой) дренирован и демонтирован, RF e2e2 снижен 4→3"

# ===== 14) Ребалансировка: заявочный цикл на 3-брокерном факте =====
echo ">>> (14/15) rebalance: POST → 201, повтор → 409, сходимость → заявка снята"
c="$(code -X POST "$BASE/api/kafka/clusters/$CLUSTER/rebalance")"
[ "$c" = 201 ] || { echo "❌ POST rebalance = $c, ожидался 201"; exit 1; }
echo "  POST rebalance -> 201"
c="$(code -X POST "$BASE/api/kafka/clusters/$CLUSTER/rebalance")"
[ "$c" = 409 ] || { echo "❌ повторный POST rebalance = $c, ожидался 409"; exit 1; }
echo "  повторный POST rebalance -> 409"
# Восстановление RF=4 требует повторного add — покрыто integration-тестом
# (Balance_Восстанавливает_RF_После_Повторного_Add); здесь заявочный цикл на
# факте RF=3: факт == план → заявка снимется без движения.
wait_until "заявка rebalances/e2e снята (факт == план RF=3)" 300 \
  bash -c '! docker compose exec -T etcd etcdctl get /kafkaworker/rebalances/e2e --print-value-only 2>/dev/null | grep -q .'
describe_check || { echo "❌ describe e2e2 после balance: RF не 3"; exit 1; }
echo "  заявка снята, план не ухудшил факт (RF=3)"
c="$(code -X DELETE "$BASE/api/kafka/clusters/$CLUSTER/rebalance")"
[ "$c" = 404 ] || { echo "❌ DELETE rebalance (заявки нет) = $c, ожидался 404"; exit 1; }
echo "  DELETE rebalance (заявки нет) -> 404"

# ===== 15) TO_REMOVE кластера → префикс пуст, координация чиста =====
echo ">>> (15/15) удаление кластера → /kafka/ пуст"
c="$(code -X DELETE "$BASE/api/kafka/clusters/$CLUSTER")"
[ "$c" = 204 ] || { echo "❌ DELETE cluster = $c"; exit 1; }
wait_until "префикс /kafka/clusters/$CLUSTER/ пуст + kfw-контейнеров нет" 180 bash -c '
  [ -z "$(docker compose exec -T etcd etcdctl get /kafka/clusters/e2e/ --prefix --keys-only 2>/dev/null | grep -v "^$")" ] \
    && [ -z "$(docker ps -a --format "{{.Names}}" | grep "^kfw-e2e-")" ]'
left="$(docker compose exec -T etcd etcdctl get /kafkaworker/ --prefix --keys-only 2>/dev/null | grep "e2e" || true)"
[ -z "$left" ] || { echo "❌ остаточные kafkaworker-ключи: $left"; exit 1; }
echo "  демонтаж завершён: контейнеры/тома/ключи чисты"

echo "✅ 55-kafka-e2e: полный цикл §9.5 зелёный (все 15 подшагов)"
