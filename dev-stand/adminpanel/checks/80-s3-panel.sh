#!/usr/bin/env bash
# 80-s3-panel.sh (t34, arch/03 §12; мерж-гейт задачи): s3-домен через ПАНЕЛЬ
# против живого воркера (s3worker deploy-контура): создание кластера REST панели
# (201 → provisioning воркером → Active) → инспекция → PUT links → deny/allow →
# sync-probe → ротация → снос через панель (202 → чистота etcd/docker/volume).
# RunTag-имена; trap-чистка own-only; set -euo pipefail. Финал: зачистка за
# собой (ассерт чистоты — после teardown не осталось контейнера/ключа префикса).
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${ADMINPANEL_URL:-http://localhost:5050}"
TAG="s80$(date +%s)"
JAR="$(mktemp)"; TMP="$(mktemp -d)"
# Own-only зачистка по TAG текущего прогона (порт 75): падение между create и
# delete не оставляет демо-контейнер/том/ключи etcd; снесённое — не падаем.
cleanup() {
  rm -f "$JAR"; rm -rf "$TMP"
  docker rm -f $(docker ps -a --format '{{.Names}}' | grep "^s3w-$TAG" || true) >/dev/null 2>&1 || true
  docker volume rm $(docker volume ls -q --filter name="s3w-$TAG" || true) >/dev/null 2>&1 || true
  ect del "/s3/$TAG/" --prefix >/dev/null 2>&1 || true
}
trap cleanup EXIT

# etcd-хелперы: чтение/удаление фактов мимо панели — для ожиданий и чистки.
etcd_key() { docker compose exec -T etcd1 etcdctl get "$1" --print-value-only </dev/null 2>/dev/null; }
ect()      { docker compose exec -T etcd1 etcdctl --endpoints=http://localhost:2379 "$@"; }

# Arrange: живость панели + cookie-логин admin/admin.
for i in $(seq 1 30); do curl -fsS "$BASE/api/healthz" >/dev/null 2>&1 && break; sleep 1; done
curl -fsS -c "$JAR" -o /dev/null -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  || { echo "❌ login admin/admin не прошёл"; exit 1; }
api()  { curl -fsS -b "$JAR" "$BASE$1"; }
code() { curl -s -o /dev/null -w '%{http_code}' -b "$JAR" "$@"; }

# 1) Создание через панель: 201 (instances[] — новый формат arch/24 §1.1;
#    platform панель подставляет сама).
body="{\"name\":\"$TAG\",\"secretProfile\":\"BACKUPS-MAIN\",\"instances\":[{\"name\":\"node1\",\"placement\":{\"dockerHost\":\"local\"},\"resources\":{\"cpu\":0.5,\"memGi\":1,\"diskGi\":5}}]}"
c="$(code -X POST "$BASE/api/s3/clusters" -H 'Content-Type: application/json' -d "$body")"
[ "$c" = 201 ] || { echo "❌ create = $c, ожидался 201"; exit 1; }
echo "  create $TAG -> 201 (заявка принята панелью)"

# 2) Ожидание Active (≤ 150 × 2 s = 300 c — бюджет 51-чека с запасом на
#    portalloc/загрузку образа; 75-чек ждёт меньше — 75 × 2 c): воркер
#    провижинит node1; endpoints появляется в сводке.
for i in $(seq 1 150); do
  api "/api/s3/clusters/$TAG" | jq -e '.state == "ACTIVE" and .nodesList[0].state == "RUNNING"' >/dev/null 2>&1 && break
  sleep 2
done
api "/api/s3/clusters/$TAG" | jq -e '.state == "ACTIVE" and .nodesList[0].state == "RUNNING"' >/dev/null \
  || { echo "❌ $TAG не достиг ACTIVE/RUNNING за 300 c (docker logs deploy-s3worker-1)"; exit 1; }
api /api/s3/clusters | jq -e "any(.[]; .name == \"$TAG\" and (.endpoints | length > 0))" >/dev/null \
  || { echo "❌ endpoints пуст в сводке"; exit 1; }
echo "  $TAG: ACTIVE, node1 RUNNING, endpoints в сводке"

# 3) Инспекция деталей: secretProfile, placedOn=local, граф и denylist пусты.
api "/api/s3/clusters/$TAG" | jq -e '
  .secretProfile == "BACKUPS-MAIN"
  and .nodesList[0].placedOn == "local"
  and (.links | length == 0)
  and (.denylist | length == 0)' >/dev/null \
  || { echo "❌ детали не сошлись (secretProfile/placedOn/links/denylist)"; exit 1; }
echo "  инспекция: secretProfile/placedOn/links/denylist — как заявлено"

# 4) Граф: с 1 нодой links пуст ([] → 202); добавляем node2 → пары [node1,node2].
c="$(code -X PUT "$BASE/api/s3/clusters/$TAG/links" -H 'Content-Type: application/json' -d '{"links":[]}')"
[ "$c" = 202 ] || { echo "❌ PUT links (пустой) = $c, ожидался 202"; exit 1; }
body2="{\"name\":\"node2\",\"placement\":{\"dockerHost\":\"local\"},\"resources\":{\"cpu\":0.5,\"memGi\":1,\"diskGi\":5}}"
c="$(code -X POST "$BASE/api/s3/clusters/$TAG/nodes" -H 'Content-Type: application/json' -d "$body2")"
[ "$c" = 201 ] || { echo "❌ add node2 = $c, ожидался 201"; exit 1; }
for i in $(seq 1 150); do
  api "/api/s3/clusters/$TAG" | jq -e '.nodesList[1].state == "RUNNING"' >/dev/null 2>&1 && break
  sleep 2
done
api "/api/s3/clusters/$TAG" | jq -e '.nodesList[1].state == "RUNNING"' >/dev/null \
  || { echo "❌ node2 не достиг RUNNING за 300 c"; exit 1; }
c="$(code -X PUT "$BASE/api/s3/clusters/$TAG/links" -H 'Content-Type: application/json' -d '{"links":[{"a":"node1","b":"node2"}]}')"
[ "$c" = 202 ] || { echo "❌ PUT links (пара) = $c, ожидался 202"; exit 1; }
for i in $(seq 1 15); do
  api "/api/s3/clusters/$TAG" | jq -e '(.links | length) == 1' >/dev/null 2>&1 && break
  sleep 2
done
api "/api/s3/clusters/$TAG" | jq -e '(.links | length) == 1' >/dev/null \
  || { echo "❌ links не отразился в деталях (доводка воркера/тики)"; exit 1; }
echo "  links: []→202; node2 RUNNING; пара node1-node2 → 202 и видна в деталях"

# 5) deny/allow: 202 → denied=true → 202 → denied=false (≤ пары тиков).
c="$(code -X POST "$BASE/api/s3/clusters/$TAG/nodes/node2/deny")"
[ "$c" = 202 ] || [ "$c" = 204 ] || { echo "❌ deny = $c, ожидался 202/204"; exit 1; }
for i in $(seq 1 15); do
  api "/api/s3/clusters/$TAG" | jq -e '.nodesList[1].denied == true' >/dev/null 2>&1 && break
  sleep 2
done
api "/api/s3/clusters/$TAG" | jq -e '.nodesList[1].denied == true' >/dev/null \
  || { echo "❌ denied=true не отразился"; exit 1; }
c="$(code -X POST "$BASE/api/s3/clusters/$TAG/nodes/node2/allow")"
[ "$c" = 202 ] || [ "$c" = 204 ] || { echo "❌ allow = $c, ожидался 202/204"; exit 1; }
for i in $(seq 1 15); do
  api "/api/s3/clusters/$TAG" | jq -e '.nodesList[1].denied == false' >/dev/null 2>&1 && break
  sleep 2
done
api "/api/s3/clusters/$TAG" | jq -e '.nodesList[1].denied == false' >/dev/null \
  || { echo "❌ denied=false не отразился"; exit 1; }
echo "  deny/allow: 202; бейдж denied true→false"

# 6) sync-probe: 202 → зонд пишет connectivity/sync (≤ 60 × 2 s; 2 ноды на одном
#    хосте — связность меряется по факту).
c="$(code -X POST "$BASE/api/s3/clusters/$TAG/sync-probe")"
[ "$c" = 202 ] || { echo "❌ sync-probe = $c, ожидался 202"; exit 1; }
for i in $(seq 1 60); do
  api "/api/s3/clusters/$TAG" | jq -e '.connectivity.state != null and .sync.state != null' >/dev/null 2>&1 && break
  sleep 2
done
api "/api/s3/clusters/$TAG" | jq -e '.connectivity.state != null and .sync.state != null' >/dev/null \
  || { echo "❌ connectivity/sync не появились за 120 c (зонд воркера)"; exit 1; }
echo "  sync-probe: 202; connectivity/sync заполнены зондом"

# 7) Ротация: 201 (повтор при живой заявке → 200 — идемпотентность воркера,
#    это ожидаемо); rotation видна в деталях.
c="$(code -X POST "$BASE/api/s3/clusters/$TAG/rotations")"
[ "$c" = 201 ] || [ "$c" = 200 ] || { echo "❌ rotations = $c, ожидался 201/200"; exit 1; }
c2="$(code -X POST "$BASE/api/s3/clusters/$TAG/rotations")"
[ "$c2" = 200 ] || [ "$c2" = 201 ] || { echo "❌ повтор rotations = $c2, ожидался 200/201"; exit 1; }
for i in $(seq 1 15); do
  api "/api/s3/clusters/$TAG" | jq -e '.rotation != null' >/dev/null 2>&1 && break
  sleep 2
done
api "/api/s3/clusters/$TAG" | jq -e '.rotation != null' >/dev/null \
  || { echo "❌ rotation не видна в деталях"; exit 1; }
echo "  ротация: $c/$c2 (201/200 — идемпотентно); rotation в деталях"

# 8) Снос через панель: 202 → демонтаж воркером → чистота (ассерт чистоты —
#    канон docs/e2e-isolation.md): префикс etcd пуст, контейнеров/томов нет.
c="$(code -X DELETE "$BASE/api/s3/clusters/$TAG")"
[ "$c" = 202 ] || [ "$c" = 204 ] || { echo "❌ delete = $c, ожидался 202/204"; exit 1; }
clean=0
for i in $(seq 1 60); do
  if [ -z "$(ect get "/s3/$TAG/" --prefix --keys-only 2>/dev/null)" ] \
     && [ -z "$(docker ps -a --format '{{.Names}}' | grep "s3w-$TAG" || true)" ] \
     && [ -z "$(docker volume ls -q --filter name="s3w-$TAG" || true)" ]; then
    clean=1; break
  fi
  sleep 2
done
[ "$clean" = 1 ] || { echo "❌ после delete остались следы $TAG (etcd/docker/volume)"; exit 1; }
echo "  delete $TAG: префикс etcd пуст, контейнеров/томов нет (ассерт чистоты пройден)"

# Финал.
echo "✅ 80-s3-panel: s3-домен через панель (создание/инспекция/links/deny/зонд/ротация/снос) — зелёный, за собой убран"
