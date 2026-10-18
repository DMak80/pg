#!/usr/bin/env bash
# 52-valkey-contour.sh (t31, arch/21 §2 / arch/adminpanel/04 §3; РУЧНАЯ приёмка
# контурного режима сетей ValkeyWorker на стенде): гварды живости ядра →
# логин в панель → создание valkey-кластера с уникальным именем ЧЕРЕЗ панель →
# wait Active/нода RUNNING → ассерты контурного режима (контейнер ноды в
# стендовой сети adminpanel-stand-net; endpoints — advertised host:port,
# portalloc-формат) → удаление через панель → чистота (ни контейнера, ни
# ключей). Самодостаточен и самоочищается: НЕ входит в 00-up.sh и никем
# автоматически не вызывается.
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${ADMINPANEL_URL:-http://localhost:5050}"
TAG="v52$(date +%s)"
JAR="$(mktemp)"
created=""

# Teardown при ЛЮБОМ исходе: удаление созданного кластера через панель + wait
# исчезновения его контейнеров (панельный cookie мог не успеть — прямая
# мутация; повторный DELETE по несуществующему — безопасен).
cleanup() {
  if [ -n "$created" ]; then
    curl -s -o /dev/null -X DELETE -b "$JAR" "$BASE/api/valkey/clusters/$TAG" >/dev/null 2>&1 || true
    for _ in $(seq 1 150); do
      docker ps -a --format '{{.Names}}' 2>/dev/null | grep -q "^vwk-$TAG-" || break
      sleep 2
    done
  fi
  rm -f "$JAR"
}
trap cleanup EXIT

# etcd-хелперы (как 51-valkey-api.sh): чтение фактов мимо панели.
etcd_key() { docker compose exec -T etcd1 etcdctl get "$1" --print-value-only </dev/null 2>/dev/null; }
etcd_has() { docker compose exec -T etcd1 etcdctl get "$1" --print-value-only </dev/null 2>/dev/null | grep -q .; }

# 1) Гварды: панель жива (healthz ≤ 60 c); ядро живо — lease-ключи
#    valkeyworker ×2 (двухинстансовый контур 00-up.sh).
for i in $(seq 1 60); do curl -fsS "$BASE/api/healthz" >/dev/null 2>&1 && break; sleep 1; done
curl -fsS "$BASE/api/healthz" >/dev/null 2>&1 \
  || { echo "❌ panel healthz failed within 60 s (:${BASE#http://}; checks/00-up.sh first)"; exit 1; }
vwk_keys=0
for i in $(seq 1 30); do
  vwk_keys="$(docker compose exec -T etcd1 etcdctl get /valkeyworker/api/ --prefix --keys-only </dev/null 2>/dev/null | grep -c . || true)"
  [ "${vwk_keys:-0}" -ge 2 ] && break
  sleep 2
done
[ "${vwk_keys:-0}" -ge 2 ] \
  || { echo "❌ /valkeyworker/api/ contains ${vwk_keys:-0} keys (<2) — valkey workers are not up (checks/00-up.sh first)"; exit 1; }

# 2) Логин (cookie-сессия панели; хелперы — по образцу 51).
curl -fsS -c "$JAR" -o /dev/null -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  || { echo "❌ login admin/admin failed"; exit 1; }
api()  { curl -fsS -b "$JAR" "$BASE$1"; }
code() { curl -s -o /dev/null -w '%{http_code}' -b "$JAR" "$@"; }

# 3) Создание valkey-кластера с УНИКАЛЬНЫМ именем через панель (тело create —
#    как в 51: maxmemory/policy/resources).
body="{\"name\":\"$TAG\",\"maxmemoryBytes\":268435456,\"maxmemoryPolicy\":\"volatile-lru\",\"resources\":{\"cpu\":1,\"memGi\":1,\"diskGi\":10}}"
c="$(code -X POST "$BASE/api/valkey/clusters" -H 'Content-Type: application/json' -d "$body")"
[ "$c" = 201 ] || { echo "❌ create = $c, expected 201"; exit 1; }
created=1

# 4) Wait: кластер Active + нода RUNNING через панель (бюджет — тики воркера,
#    по образцу 51: ≤ 150 × 2 c).
for i in $(seq 1 150); do
  api "/api/valkey/clusters/$TAG" | jq -e '.state == "ACTIVE" and .nodesList[0].state == "RUNNING"' >/dev/null 2>&1 && break
  sleep 2
done
api "/api/valkey/clusters/$TAG" | jq -e '.state == "ACTIVE" and .nodesList[0].state == "RUNNING"' >/dev/null \
  || { echo "❌ $TAG did not reach ACTIVE/RUNNING within 300 s (docker compose logs valkeyworker valkeyworker-2)"; exit 1; }
echo "  $TAG: ACTIVE, node RUNNING"

# 5) Ассерты контурного режима (arch/21 §2): контейнер ноды — в стендовой
#    сети контура; endpoints — advertised host:port (portalloc; без хардкода
#    порта — только формат).
nets="$(docker inspect "vwk-$TAG-node1" --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}} {{end}}' 2>/dev/null || true)"
echo "$nets" | grep -q 'adminpanel-stand-net' \
  || { echo "❌ vwk-$TAG-node1 networks [$nets] — adminpanel-stand-net not attached (contour network mode, ValkeyWorker__Docker__ContourNetwork)"; exit 1; }
endpoints="$(etcd_key "/valkey/clusters/$TAG/endpoints")"
[ -n "$endpoints" ] || { echo "❌ /valkey/clusters/$TAG/endpoints is empty"; exit 1; }
echo "$endpoints" | grep -Eq '^[^:]+:[0-9]+$' \
  || { echo "❌ endpoints '$endpoints' is not host:port (advertised format, arch/21 §2)"; exit 1; }
echo "  contour: node in adminpanel-stand-net; endpoints=$endpoints"

# 6) Teardown: DELETE через панель → wait исчезновения контейнеров vwk-$TAG-*
#    и ключей /valkey/clusters/$TAG/ (цикл ≤ 150 × 2 c; 404-повтор — no-op).
c="$(code -X DELETE "$BASE/api/valkey/clusters/$TAG")"
[ "$c" = 202 ] || { echo "❌ delete = $c, expected 202"; exit 1; }
created=""
for i in $(seq 1 150); do
  ! docker ps -a --format '{{.Names}}' 2>/dev/null | grep -q "^vwk-$TAG-" \
    && ! etcd_has "/valkey/clusters/$TAG/config" && break
  sleep 2
done

# 7) Финальные ассерты чистоты: ни контейнера, ни ключей (после teardown).
docker ps -a --format '{{.Names}}' 2>/dev/null | grep -q "^vwk-$TAG-" \
  && { echo "❌ containers vwk-$TAG-* not deleted"; exit 1; }
etcd_has "/valkey/clusters/$TAG/config" \
  && { echo "❌ keys /valkey/clusters/$TAG/ not deleted"; exit 1; }
echo "✓ 52-valkey-contour: contour network mode green (node in adminpanel-stand-net, advertised host:port, no traces)"
