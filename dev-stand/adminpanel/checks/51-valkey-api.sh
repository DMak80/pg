#!/usr/bin/env bash
# 51-valkey-api.sh (t03, arch/04 §3; мерж-гейт задачи): valkey-домен против
# ЖИВОГО воркера (профиль valkey): сид demo (05-seed.sh valkey — заявка,
# доигранная воркером) → панель видит кластер (live-PING) → полный цикл
# мутаций ЧЕРЕЗ панель→прокси→API воркера с RunTag-именем → чистота после
# delete. TLS-проверки t06: RESP-проба по TLS с ca_pem из etcd, plain-порт
# закрыт. Финал: демо-контур остаётся (сид живёт — полная система).
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${ADMINPANEL_URL:-http://localhost:5050}"
TAG="v51$(date +%s)"
JAR="$(mktemp)"; TMP="$(mktemp -d)"
trap 'rm -f "$JAR"; rm -rf "$TMP"' EXIT

# etcd-хелперы (как 55-kafka-e2e.sh): чтение фактов мимо панели — для ожиданий.
etcd_key() { docker compose exec -T etcd1 etcdctl get "$1" --print-value-only </dev/null 2>/dev/null; }
etcd_has() { docker compose exec -T etcd1 etcdctl get "$1" --print-value-only </dev/null 2>/dev/null | grep -q .; }

# Arrange: сид через API живого воркера (поднимает valkeyworker, ждёт demo Active).
"$PWD/checks/05-seed.sh" valkey

for i in $(seq 1 60); do curl -fsS "$BASE/api/healthz" >/dev/null 2>&1 && break; sleep 1; done
curl -fsS -c "$JAR" -o /dev/null -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  || { echo "❌ login admin/admin failed"; exit 1; }
api()  { curl -fsS -b "$JAR" "$BASE$1"; }
code() { curl -s -o /dev/null -w '%{http_code}' -b "$JAR" "$@"; }

# 1) Панель видит сид demo: сводка ACTIVE + нода RUNNING + endpoints (список),
#    детали с live=true (PING-проба по admin-креду из etcd; тики 3 c + 15 c).
# null при отсутствии demo: jq -e на null-входе даст false → цикл ждёт (не пустой вывод!).
demo_summary() { api /api/valkey/clusters | jq -c '[.[] | select(.name == "demo")][0]'; }
for i in $(seq 1 20); do
  demo_summary | jq -e '.state == "ACTIVE" and .nodesRunning == 1 and (.endpoints | length > 0)' >/dev/null 2>&1 && break
  sleep 2
done
demo_summary | jq -e '.state == "ACTIVE" and .nodesRunning == 1' >/dev/null \
  || { echo "❌ demo not ACTIVE/RUNNING in the summary"; exit 1; }
for i in $(seq 1 20); do
  api /api/valkey/clusters/demo | jq -e '.nodesList[0].live == true' >/dev/null 2>&1 && break
  sleep 2
done
api /api/valkey/clusters/demo | jq -e '.nodesList[0].live == true' >/dev/null \
  || { echo "❌ live=true did not appear (PING probe; AdminPanel__Probes__Valkey?)"; exit 1; }
echo "  demo seed: ACTIVE, node RUNNING, live=true (PING)"

# 2) Создание кластера через панель: 201 → NOT_INITIALIZED → RUNNING ≤ бюджета
#    тиков воркера (NodeBootSec 120 c + запас на portalloc/загрузку образа).
body="{\"name\":\"$TAG\",\"maxmemoryBytes\":268435456,\"maxmemoryPolicy\":\"volatile-lru\",\"resources\":{\"cpu\":1,\"memGi\":1,\"diskGi\":10}}"
c="$(code -X POST "$BASE/api/valkey/clusters" -H 'Content-Type: application/json' -d "$body")"
[ "$c" = 201 ] || { echo "❌ create = $c, expected 201"; exit 1; }
for i in $(seq 1 150); do
  api "/api/valkey/clusters/$TAG" | jq -e '.state == "ACTIVE" and .nodesList[0].state == "RUNNING"' >/dev/null 2>&1 && break
  sleep 2
done
api "/api/valkey/clusters/$TAG" | jq -e '.state == "ACTIVE" and .nodesList[0].state == "RUNNING"' >/dev/null \
  || { echo "❌ $TAG did not reach ACTIVE/RUNNING within 300 s (docker compose logs valkeyworker)"; exit 1; }
echo "  create $TAG -> 201; RUNNING reached"

# 2b) TLS-пробы (t06): ca_pem из etcd → TLS-проба app-кредом PONG; plain-порт
#     закрыт (тот же вызов без --tls отклоняется, retry ≤ 30 c суммарно).
etcd_key "/valkey/clusters/$TAG/ca_pem" | sed 's/\\n/\n/g' > "$TMP/ca.pem"
grep -q "BEGIN CERTIFICATE" "$TMP/ca.pem" \
  || { echo "❌ ca_pem missing/corrupt in etcd (TLS discovery t06)"; exit 1; }
endpoints="$(etcd_key "/valkey/clusters/$TAG/endpoints")"
vh="${endpoints%%:*}"; vp="${endpoints##*:}"
app_password="$(etcd_key "/valkey/clusters/$TAG/app_password")"
tls_ok=""
for i in $(seq 1 10); do
  # PING вне прав app (ACL-канон: +@read +@write) — проба SET/GET по TLS.
  [ "$(docker run --rm --network host -v "$TMP/ca.pem:/ca.pem:ro" valkey/valkey:9.1.2 \
      valkey-cli --tls --cacert /ca.pem -h "$vh" -p "$vp" --user app -a "$app_password" \
      --no-auth-warning SET tls:probe t06 2>/dev/null)" = OK ] \
    && [ "$(docker run --rm --network host -v "$TMP/ca.pem:/ca.pem:ro" valkey/valkey:9.1.2 \
      valkey-cli --tls --cacert /ca.pem -h "$vh" -p "$vp" --user app -a "$app_password" \
      --no-auth-warning GET tls:probe 2>/dev/null)" = t06 ] && { tls_ok=1; break; }
  sleep 3
done
[ -n "$tls_ok" ] || { echo "❌ TLS probe with app credentials did not answer PONG"; exit 1; }
plain_rejected=""
for i in $(seq 1 10); do
  if ! docker run --rm --network host valkey/valkey:9.1.2 \
      valkey-cli -h "$vh" -p "$vp" -u app -a "$app_password" \
      --no-auth-warning PING >/dev/null 2>&1; then plain_rejected=1; break; fi
  sleep 3
done
[ -n "$plain_rejected" ] || { echo "❌ plain connection succeeded (rejection expected — --port 0)"; exit 1; }
echo "  TLS: PONG over TLS with ca_pem from etcd; plain port closed"

# 3) Конфиг-мутация: 200; converge D применяет БЕЗ рестарта контейнера
#    (container ID неизменен); панель видит новые значения ≤ пары тиков.
cid_before="$(docker inspect -f '{{.Id}}' "vwk-$TAG-node1" 2>/dev/null || true)"
c="$(code -X PUT "$BASE/api/valkey/clusters/$TAG/config" -H 'Content-Type: application/json' -d '{"maxmemoryBytes":134217728}')"
[ "$c" = 200 ] || { echo "❌ PUT config = $c"; exit 1; }
for i in $(seq 1 15); do
  api "/api/valkey/clusters/$TAG" | jq -e '.maxmemoryBytes == 134217728' >/dev/null 2>&1 && break
  sleep 2
done
api "/api/valkey/clusters/$TAG" | jq -e '.maxmemoryBytes == 134217728' >/dev/null \
  || { echo "❌ maxmemory not applied in the panel data"; exit 1; }
cid_after="$(docker inspect -f '{{.Id}}' "vwk-$TAG-node1" 2>/dev/null || true)"
[ -n "$cid_before" ] && [ "$cid_before" = "$cid_after" ] \
  || { echo "❌ container recreated on config mutation (converge without restart expected)"; exit 1; }
echo "  config mutation: converge without restart, panel sees 128 MiB"

# 4) Ресурсы: 200 → автоконверге пересоздаёт контейнер (PROVISIONING → RUNNING).
c="$(code -X PUT "$BASE/api/valkey/clusters/$TAG/nodes/node1/resources" -H 'Content-Type: application/json' -d '{"cpu":2,"memGi":2,"diskGi":20}')"
[ "$c" = 200 ] || { echo "❌ PUT resources = $c"; exit 1; }
for i in $(seq 1 150); do
  api "/api/valkey/clusters/$TAG" | jq -e '.nodesList[0].state == "RUNNING"' >/dev/null 2>&1 \
    && docker inspect -f '{{.Id}}' "vwk-$TAG-node1" 2>/dev/null | grep -qv "$cid_after" && break
  sleep 2
done
docker inspect "vwk-$TAG-node1" >/dev/null 2>&1 \
  || { echo "❌ container vwk-$TAG-node1 did not come up after resources"; exit 1; }
echo "  resources: container recreated, RUNNING"

# 5) Ротации app+admin: 202, заявка исполняется (ключ /valkeyworker/rotations/$TAG
#    исчезает тиком воркера).
for role in app admin; do
  c="$(code -X POST "$BASE/api/valkey/clusters/$TAG/password/rotate" -H 'Content-Type: application/json' -d "{\"role\":\"$role\"}")"
  [ "$c" = 202 ] || { echo "❌ rotate $role = $c, expected 202"; exit 1; }
  for i in $(seq 1 15); do ! etcd_has "/valkeyworker/rotations/$TAG" && break; sleep 2; done
  etcd_has "/valkeyworker/rotations/$TAG" && { echo "❌ $role rotation request not executed within 30 s"; exit 1; }
done

# 5b) Двойная ротация (409): два ПАРАЛЛЕЛЬНЫХ POST до тика воркера — клэйм-txn
#     version==0 пропускает ровно один: ожидаем пару {202, 409}.
c1="$(mktemp)"; c2="$(mktemp)"
code -X POST "$BASE/api/valkey/clusters/$TAG/password/rotate" -H 'Content-Type: application/json' -d '{"role":"app"}' >"$c1" &
p1=$!
code -X POST "$BASE/api/valkey/clusters/$TAG/password/rotate" -H 'Content-Type: application/json' -d '{"role":"app"}' >"$c2" &
p2=$!
wait "$p1" "$p2"
r1="$(cat "$c1")"; r2="$(cat "$c2")"; rm -f "$c1" "$c2"
{ [ "$r1" = 202 ] && [ "$r2" = 409 ]; } || { [ "$r1" = 409 ] && [ "$r2" = 202 ]; } \
  || { echo "❌ double rotation: $r1/$r2, expected 202+409"; exit 1; }
for i in $(seq 1 15); do ! etcd_has "/valkeyworker/rotations/$TAG" && break; sleep 2; done
etcd_has "/valkeyworker/rotations/$TAG" && { echo "❌ double rotation request not executed"; exit 1; }

# 5c) Негатив роли: role=wrong → 400 (валидирует воркер).
c="$(code -X POST "$BASE/api/valkey/clusters/$TAG/password/rotate" -H 'Content-Type: application/json' -d '{"role":"wrong"}')"
[ "$c" = 400 ] || { echo "❌ rotate role=wrong = $c, expected 400"; exit 1; }
echo "  app+admin rotations: 202 → executed; double → 409; role=wrong → 400"

# 6) 409-ветка создания: дубль имени занят.
c="$(code -X POST "$BASE/api/valkey/clusters" -H 'Content-Type: application/json' -d "{\"name\":\"$TAG\"}")"
[ "$c" = 409 ] || { echo "❌ name duplicate = $c, expected 409"; exit 1; }
echo "  name duplicate $TAG -> 409"

# 7) Алерты домена: worker-api-unreachable нет при живом воркере; alerts содержит
#    только реальные (проверка отсутствия critical worker-api-unreachable valkey).
api /api/alerts | jq -e 'any(.[]; .kind == "worker-api-unreachable" and .target == "valkeyworker") | not' >/dev/null \
  || { echo "❌ spurious worker-api-unreachable with a live worker"; exit 1; }
echo "  /api/alerts: valkey edge clean with a live worker"

# 8) Удаление: 202 → демонтаж воркером → кластер исчезает из панели; чистота:
#    ни контейнера vwk-$TAG-*, ни ключей /valkey/clusters/$TAG/ (TLS-материал
#    доставляется env ноды — per-cluster TLS-тома в модели нет).
c="$(code -X DELETE "$BASE/api/valkey/clusters/$TAG")"
[ "$c" = 202 ] || { echo "❌ delete = $c, expected 202"; exit 1; }
for i in $(seq 1 150); do
  ! docker inspect "vwk-$TAG-node1" >/dev/null 2>&1 \
    && ! etcd_has "/valkey/clusters/$TAG/config" && break
  sleep 2
done
docker inspect "vwk-$TAG-node1" >/dev/null 2>&1 && { echo "❌ container vwk-$TAG-node1 not deleted"; exit 1; }
etcd_has "/valkey/clusters/$TAG/config" && { echo "❌ keys /valkey/clusters/$TAG/ not deleted"; exit 1; }
# Список панели догоняет удаление тиком refresher'а (3 c) — поллинг, не одиночный выстрел.
for i in $(seq 1 15); do
  api /api/valkey/clusters | jq -e "any(.[]; .name == \"$TAG\") | not" >/dev/null 2>&1 && break
  sleep 2
done
api /api/valkey/clusters | jq -e "any(.[]; .name == \"$TAG\") | not" >/dev/null \
  || { echo "❌ $TAG not gone from the panel list"; exit 1; }
echo "  delete -> 202; clean: no container or keys, gone from the panel"

# Финал: демо-контур остаётся (сид живёт; чистка — 90-down.sh).
api /api/valkey/clusters | jq -e 'any(.[]; .name == "demo")' >/dev/null \
  || { echo "❌ demo cluster disappeared"; exit 1; }
echo "✓ 51-valkey-api: full valkey domain cycle via the panel — all steps green"
