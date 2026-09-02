#!/usr/bin/env bash
# 59-kafka-regen.sh (t06; критерий spec §10.3/§10.4/§10.8): rolling-регенерация
# брокеров на ЖИВОМ воркере с чистого состояния — мутация №15 (PUT resources
# через API панели) → NodeRegenerator пересоздаёт контейнер (том жив) →
# сходимость лимитов docker inspect == декларации → прогресс-ключ исчезает.
# Панель — compose-сервис adminpanel (AGENTS.md: всегда в докере, :5050 =
# ADMINPANEL_URL); воркер — docker compose --profile kafka (чек собирает
# образы и поднимает стенд сам, сид-профиль не активен).
#
# Подшаги: 1) чистый стенд; 2) кластер 3 брокера через API → RUNNING+endpoints;
# 3) фиксация NanoCPUs ДО; 4) PUT resources cpu 2→3 → 200; 5) поллинг:
# прогресс regen виден → сходимость (ключ исчез, RUNNING, NanoCPUs=3e9);
# 6) негативы (cpu 100 → 400, ghost → 404, broker9 → 404); 7) идемпотентность:
# повтор PUT cpu 3 → 200 без рестарта (Running/StartedAt стабильны); 8) TO_REMOVE
# кластера → /kafka/ пуст.
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${ADMINPANEL_URL:-http://localhost:5050}"
CLUSTER="e2e6"
JAR="$(mktemp)"
trap 'rm -f "$JAR"' EXIT

# etcd стенда — единственный источник правды (дискавери §3.5).
# </dev/null: docker CLI в пайпе не должен съедать stdin.
etcd_key() { docker compose exec -T etcd etcdctl get "$1" --print-value-only </dev/null 2>/dev/null; }
etcd_kafka_keys() {
  docker compose exec -T etcd etcdctl get /kafka/ --prefix --keys-only 2>/dev/null | grep -v '^$' || true
}
# Лимиты контейнера брокера: HostConfig.NanoCpus (тег inspect — «NanoCpus»).
broker_nano() { docker inspect "kfw-$CLUSTER-broker1" --format '{{.HostConfig.NanoCpus}}'; }
broker_running() { docker inspect "kfw-$CLUSTER-broker1" --format '{{.State.Running}}'; }
broker_started_at() { docker inspect "kfw-$CLUSTER-broker1" --format '{{.State.StartedAt}}'; }

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
echo ">>> (1/8) чистый стенд: down -v + kfw-очистка + up --profile kafka"
./checks/90-down.sh -v >/dev/null
docker compose --profile kafka down -v --remove-orphans >/dev/null 2>&1 || true
# kfw-объекты живут вне compose-проекта — чистим вручную (handoff-рецепт B9).
docker rm -f $(docker ps -aq --filter 'name=kfw-') >/dev/null 2>&1 || true
docker volume rm $(docker volume ls -q --filter 'name=kfw-') >/dev/null 2>&1 || true
docker network rm $(docker network ls -q --filter 'name=kfw-net') >/dev/null 2>&1 || true
# Прокси-маршрут №15 живёт в панели — образ панели тоже обязан быть свежим.
docker compose --profile kafka build kafkaworker adminpanel >/dev/null \
  || { echo "❌ сборка образов kafkaworker/adminpanel не удалась"; exit 1; }
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
echo ">>> (2/8) создание кластера $CLUSTER (3 брокера) через API"
for i in $(seq 1 30); do
  created="$(curl -s -b "$JAR" -X POST "$BASE/api/kafka/clusters" \
    -H 'Content-Type: application/json' -d "{\"name\":\"$CLUSTER\"}")" \
    && echo "$created" | jq -e '.state == "NOT_INITIALIZED"' >/dev/null && break
  sleep 1
done
echo "$created" | jq -e '.state == "NOT_INITIALIZED"' >/dev/null \
  || { echo "❌ POST /api/kafka/clusters не прошёл: $created"; exit 1; }
wait_until "RUNNING 3/3 + endpoints" 150 bash -c '
  curl -fsS -b "'"$JAR"'" "'"$BASE"'/api/kafka/clusters" \
  | jq -e "[.[] | select(.name == \"e2e6\")][0] | .brokersRunning == 3 and .endpoints != null"'
echo "  endpoints: $(etcd_key /kafka/clusters/$CLUSTER/endpoints)"

# ===== 3) Фиксация лимитов ДО (сид: cpu 2 → 2000000000) =====
echo ">>> (3/8) docker inspect kfw-$CLUSTER-broker1: NanoCPUs ДО"
nano_before="$(broker_nano)"
[ "$nano_before" = "2000000000" ] \
  || { echo "❌ NanoCPUs до регенерации = $nano_before, ожидался 2000000000 (сид cpu 2)"; exit 1; }
echo "  NanoCPUs до: $nano_before"

# ===== 4) Мутация №15: PUT resources cpu 2→3 → 200 =====
echo ">>> (4/8) PUT /api/kafka/clusters/$CLUSTER/brokers/broker1/resources {\"cpu\":3}"
c="$(code -X PUT "$BASE/api/kafka/clusters/$CLUSTER/brokers/broker1/resources" \
  -H 'Content-Type: application/json' -d '{"cpu":3}')"
[ "$c" = 200 ] || { echo "❌ PUT resources = $c, ожидался 200"; exit 1; }
etcd_key "/kafka/clusters/$CLUSTER/brokers/broker1/resources" \
  | jq -e '.cpu == "3" and .mem == "2Gi" and .disk == "20Gi"' >/dev/null \
  || { echo "❌ канонический JSON resources не записан"; exit 1; }
echo "  200, ключ: cpu=3 (mem/disk унаследованы)"

# ===== 5) Сходимость: прогресс виден → регенерация → ключ исчез, NanoCPUs=3e9 =====
echo ">>> (5/8) поллинг: регенерация broker1 (прогресс → RUNNING → ключ исчез)"
# (а) прогресс regen виден в деталях панели хотя бы раз (воркер снял ключ с
# сходимостью мгновеннее поллера — допустимо, сходимость проверяем ниже).
# NB: инлайн docker inspect — bash -c не наследует функции родительского шелла.
wait_until "прогресс regen виден в GET деталей ИЛИ уже сошёлся (ключ исчез)" 150 bash -c '
  details="$(curl -fsS -b "'"$JAR"'" "'"$BASE"'/api/kafka/clusters/'"$CLUSTER"'")" || exit 1
  echo "$details" | jq -e ".regen != null" && exit 0
  echo "$details" | jq -e ".regen == null and ([.brokersList[] | select(.name == \"broker1\")][0].state == \"RUNNING\")" \
    && [ "$(docker inspect kfw-'"$CLUSTER"'-broker1 --format "{{.HostConfig.NanoCpus}}")" = "3000000000" ]'
# (б) финальная сходимость: ключ исчез, RUNNING, лимиты == декларации.
wait_until "сходимость: regen=null, broker1 RUNNING, NanoCPUs=3000000000" 150 bash -c '
  curl -fsS -b "'"$JAR"'" "'"$BASE"'/api/kafka/clusters/'"$CLUSTER"'" \
  | jq -e ".regen == null and ([.brokersList[] | select(.name == \"broker1\")][0].state == \"RUNNING\")" \
  && [ "$(docker inspect kfw-'"$CLUSTER"'-broker1 --format "{{.HostConfig.NanoCpus}}")" = "3000000000" ] \
  && ! docker compose exec -T etcd etcdctl get /kafkaworker/regens/'"$CLUSTER"' --print-value-only 2>/dev/null | grep -q .'
echo "  лимиты сошлись к декларации: NanoCPUs=3000000000, регенерация завершена"

# ===== 6) Негативы =====
echo ">>> (6/8) негативы: cpu 100 → 400; ghost-кластер → 404; broker9 → 404"
c="$(code -X PUT "$BASE/api/kafka/clusters/$CLUSTER/brokers/broker1/resources" \
  -H 'Content-Type: application/json' -d '{"cpu":100}')"
[ "$c" = 400 ] || { echo "❌ PUT cpu=100 = $c, ожидался 400"; exit 1; }
c="$(code -X PUT "$BASE/api/kafka/clusters/ghost/brokers/broker1/resources" \
  -H 'Content-Type: application/json' -d '{"cpu":3}')"
[ "$c" = 404 ] || { echo "❌ PUT ghost-кластер = $c, ожидался 404"; exit 1; }
c="$(code -X PUT "$BASE/api/kafka/clusters/$CLUSTER/brokers/broker9/resources" \
  -H 'Content-Type: application/json' -d '{"cpu":3}')"
[ "$c" = 404 ] || { echo "❌ PUT broker9 = $c, ожидался 404"; exit 1; }
echo "  негативы: 400 / 404 / 404"

# ===== 7) Идемпотентность: повтор тех же значений — рестарта нет =====
echo ">>> (7/8) повтор PUT {\"cpu\":3} → 200, рестарта нет (15 с наблюдения)"
c="$(code -X PUT "$BASE/api/kafka/clusters/$CLUSTER/brokers/broker1/resources" \
  -H 'Content-Type: application/json' -d '{"cpu":3}')"
[ "$c" = 200 ] || { echo "❌ повторный PUT = $c, ожидался 200"; exit 1; }
started_before="$(broker_started_at)"
[ "$(broker_running)" = "true" ] || { echo "❌ контейнер broker1 не running"; exit 1; }
sleep 15
[ "$(broker_running)" = "true" ] || { echo "❌ контейнер broker1 перезапустился"; exit 1; }
[ "$(broker_started_at)" = "$started_before" ] \
  || { echo "❌ StartedAt изменился — рестарт был (идемпотентность нарушена)"; exit 1; }
echo "  рестарта нет: Running=true, StartedAt стабилен"

# ===== 8) TO_REMOVE кластера → префикс пуст =====
echo ">>> (8/8) удаление кластера → /kafka/ пуст"
c="$(code -X DELETE "$BASE/api/kafka/clusters/$CLUSTER")"
[ "$c" = 204 ] || { echo "❌ DELETE cluster = $c, ожидался 204"; exit 1; }
wait_until "префикс /kafka/clusters/$CLUSTER/ пуст + kfw-контейнеров нет" 180 bash -c '
  [ -z "$(docker compose exec -T etcd etcdctl get /kafka/clusters/e2e6/ --prefix --keys-only 2>/dev/null | grep -v "^$")" ] \
    && [ -z "$(docker ps -a --format "{{.Names}}" | grep "^kfw-e2e6-")" ]'
left="$(docker compose exec -T etcd etcdctl get /kafkaworker/ --prefix --keys-only 2>/dev/null | grep "e2e6" || true)"
[ -z "$left" ] || { echo "❌ остаточные kafkaworker-ключи: $left"; exit 1; }
echo "  демонтаж завершён: контейнеры/тома/ключи чисты"

echo "✅ 59-kafka-regen: rolling-регенерация зелёная (все 8 подшагов)"
