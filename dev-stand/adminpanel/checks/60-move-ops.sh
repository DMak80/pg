#!/usr/bin/env bash
# Move-ops на демо-сиде через API панели (t07): rollback/finalize/abort +
# снятие заявок; результаты — etcd-ключи, очередь/статусы/work-журнал деталей
# кластера. Полный docker-цикл move→abort/rollback→finalize покрыт E2e t01 —
# здесь API/UI-слой без поднятия новых PG. Зависшие статусы насыпает чек
# (демо-сид аномалий не сеет — образец 20-alerts.sh): updated_unix=now —
# «свежий» (abort 409), now-300 — «несвежий» (>AbortMinAgeSec=120, <repair 600).
set -euo pipefail
# Корень репо — только от расположения самого скрипта (никаких cwd-зависимостей
# и абсолютных путей): checks → adminpanel → dev-stand → корень.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
cd "$SCRIPT_DIR/.."

BASE="${ADMINPANEL_URL:-http://localhost:5050}"
JAR="$(mktemp)"; trap 'rm -f "$JAR"' EXIT

# Arrange (t41): канонический demo жив (config без NOT_INITIALIZED, ноды
# RUNNING; создаёт 00-up.sh POST /api/clusters) — wait до 60 c; панель отвечает.
demo_live() {
  local conf
  conf="$(docker compose exec -T etcd1 etcdctl get /clusters/demo/config --print-value-only 2>/dev/null || true)"
  [ -n "$conf" ] && ! echo "$conf" | grep -q '"state":"NOT_INITIALIZED"' \
    && [ "$(docker compose exec -T etcd1 etcdctl get /clusters/demo/shards/shard1/nodes/shard1a/state --print-value-only 2>/dev/null)" = "RUNNING" ]
}
for i in $(seq 1 30); do demo_live && break; sleep 2; done
demo_live || { echo "❌ demo not live (config/nodes; run checks/00-up.sh)"; exit 1; }
for i in $(seq 1 60); do curl -fsS "$BASE/api/healthz" >/dev/null 2>&1 && break; sleep 1; done
curl -fsS "$BASE/api/healthz" >/dev/null || { echo "❌ adminpanel not responding: $BASE"; exit 1; }
curl -fsS -c "$JAR" -o /dev/null -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  || { echo "❌ login admin/admin failed"; exit 1; }

api()  { curl -fsS -b "$JAR" "$BASE$1"; }
# POST с телом, возвращающий тело ответа без -f (409 ProblemDetails читаемы).
post() { curl -s -b "$JAR" -X POST "$BASE$1" -H 'Content-Type: application/json' -d "$2"; }
code() { curl -s -o /dev/null -w '%{http_code}' -b "$JAR" "$@"; }
ect()  { docker compose exec -T etcd1 etcdctl --endpoints=http://localhost:2379 "$@"; }
now()  { date +%s; }
# t41: раскладка каноническая (движок) — owner статуса = фактический routing,
# target = другой шард; иначе routing==target → abort отвергается «осознанно».
other() { [ "$1" = shard1 ] && echo shard2 || echo shard1; }
routing_of() { ect get "/clusters/demo/buckets/routing/$1" --print-value-only; }

# Сводная проверка тика панели (детали демо-кластера читаются).
for i in $(seq 1 15); do
  api /api/clusters/demo | jq -e '.name == "demo"' >/dev/null 2>&1 && break; sleep 1;
done
api /api/clusters/demo | jq -e '.name == "demo"' >/dev/null \
  || { echo "❌ /api/clusters/demo unavailable"; exit 1; }
echo "  adminpanel alive, demo cluster readable"

# ── 1) abort: свежий статус без force → 409; с force → 201; отмена заявки → 204/404.
own3="$(routing_of bucket_3)"; tgt3="$(other "$own3")"
ect put /clusters/demo/buckets/status/bucket_3 \
  "{\"bucket\":\"bucket_3\",\"state\":\"SYNCING\",\"owner\":\"$own3\",\"target\":\"$tgt3\",\"started_unix\":$(now),\"updated_unix\":$(now),\"phase\":\"copy\"}" >/dev/null
c="$(code -X POST "$BASE/api/clusters/demo/moves/abort" -H 'Content-Type: application/json' -d '{"bucket":3}')"
[ "$c" = 409 ] || { echo "❌ abort fresh without force = $c, expected 409"; exit 1; }
c="$(code -X POST "$BASE/api/clusters/demo/moves/abort" -H 'Content-Type: application/json' -d '{"bucket":3,"force":true}')"
# force-заявка поставлена этим запросом; повтор идентичной → 201 без записи (§9.7 п.3):
c="$(code -X POST "$BASE/api/clusters/demo/moves/abort" -H 'Content-Type: application/json' -d '{"bucket":3,"force":true}')"
[ "$c" = 201 ] || { echo "❌ repeated abort force = $c, expected 201 (identical request)"; exit 1; }
ect get /pgworker/moves/demo/bucket_3 --print-value-only | grep -q '"op":"abort"' \
  || { echo "❌ abort request key does not contain op=abort"; exit 1; }
ect get /pgworker/moves/demo/bucket_3 --print-value-only | grep -q '"force":true' \
  || { echo "❌ abort request key without force:true"; exit 1; }
c="$(code -X DELETE "$BASE/api/clusters/demo/moves/bucket_3")"
[ "$c" = 204 ] || { echo "❌ abort request removal = $c, expected 204"; exit 1; }
[ -z "$(ect get /pgworker/moves/demo/bucket_3 --print-value-only 2>/dev/null)" ] \
  || { echo "❌ request key not gone after DELETE"; exit 1; }
c="$(code -X DELETE "$BASE/api/clusters/demo/moves/bucket_3")"
[ "$c" = 404 ] || { echo "❌ repeated removal = $c, expected 404 (not idempotent)"; exit 1; }
echo "  abort: fresh 409 → force 201 (identical) → removal 204 → repeat 404"
ect del /clusters/demo/buckets/status/bucket_3 >/dev/null

# ── 2) abort: несвежий статус без force → 201 (force в JSON нет); ACTIVE → 409.
own7="$(routing_of bucket_7)"; tgt7="$(other "$own7")"
ect put /clusters/demo/buckets/status/bucket_7 \
  "{\"bucket\":\"bucket_7\",\"state\":\"ABORTING\",\"owner\":\"$own7\",\"target\":\"$tgt7\",\"started_unix\":$(( $(now) - 300 )),\"updated_unix\":$(( $(now) - 300 )),\"phase\":\"cleanup\"}" >/dev/null
c="$(code -X POST "$BASE/api/clusters/demo/moves/abort" -H 'Content-Type: application/json' -d '{"bucket":7}')"
[ "$c" = 201 ] || { echo "❌ abort stale without force = $c, expected 201"; exit 1; }
ect get /pgworker/moves/demo/bucket_7 --print-value-only | grep -q '"force"' \
  && { echo "❌ stale abort key contains force (must be omitted)"; exit 1; }
c="$(code -X DELETE "$BASE/api/clusters/demo/moves/bucket_7")"
[ "$c" = 204 ] || { echo "❌ stale abort removal = $c"; exit 1; }
c="$(code -X POST "$BASE/api/clusters/demo/moves/abort" -H 'Content-Type: application/json' -d '{"bucket":0}')"
[ "$c" = 409 ] || { echo "❌ abort of an ACTIVE bucket = $c, expected 409"; exit 1; }
ect del /clusters/demo/buckets/status/bucket_7 >/dev/null
echo "  abort: stale 201 (force omitted); ACTIVE → 409"

# ── 3) abort: routing==target без force → 409 «осознанно».
own2="$(routing_of bucket_2)"
ect put /clusters/demo/buckets/status/bucket_2 \
  "{\"bucket\":\"bucket_2\",\"state\":\"FROZEN\",\"owner\":\"$own2\",\"target\":\"$own2\",\"started_unix\":$(( $(now) - 300 )),\"updated_unix\":$(( $(now) - 300 )),\"phase\":\"cutover-wait\"}" >/dev/null
resp="$(post /api/clusters/demo/moves/abort '{"bucket":2}')"
# паттерн "deliberate" — фактический текст ProblemDetails воркера (MoveAlreadyFlippedException)
echo "$resp" | jq -e '.detail // empty' 2>/dev/null | grep -q "deliberate" \
  || { echo "❌ abort routing==target: expected 409 with 'deliberate', got: $resp"; exit 1; }
ect del /clusters/demo/buckets/status/bucket_2 >/dev/null
echo "  abort: routing==target without force → 409 deliberate"

# ── 4) rollback: ACTIVE-бакет → 201 (op=rollback); снятие → 204 (грань мутации;
# воркер подбирает заявку первым тиком — очередь в живой системе эфемерна).
c="$(code -X POST "$BASE/api/clusters/demo/moves/rollback" -H 'Content-Type: application/json' -d '{"buckets":[6]}')"
[ "$c" = 201 ] || { echo "❌ rollback ACTIVE = $c, expected 201"; exit 1; }
ect get /pgworker/moves/demo/bucket_6 --print-value-only | grep -q '"op":"rollback"' \
  || { echo "❌ rollback request key does not contain op=rollback"; exit 1; }
c="$(code -X DELETE "$BASE/api/clusters/demo/moves/bucket_6")"
[ "$c" = 204 ] || { echo "❌ rollback removal = $c"; exit 1; }
echo "  rollback: 201 → key op=rollback → removal 204"

# ── 4б) очередь панели видит rollback-заявку (детерминированно): панель
# показывает очередь снапшотом (тик 3 c), воркер подбирает заявку первым тиком
# (~≤5 c) — на живых исполнителях видимость гоночна. Исполнителей останавливаем
# (панель жива независимо; мутации панели в этой фазе не нужны), заявку кладёт
# чек напрямую в etcd (паттерн сценария 5); после проверки — снятие и возврат
# воркеров (паттерн шага 0).
workers_up() {
  local port="$1"
  curl -fsS -m 3 --cacert "$ROOT/deploy/tls/ca.pem" --cert "$ROOT/deploy/tls/healthcheck.crt" \
    --key "$ROOT/deploy/tls/healthcheck.key" "https://localhost:${port}/healthz" >/dev/null 2>&1
}
pgw1_port="$(grep -E '^PGW_API_HOST_PORT=' "$ROOT/deploy/.env" | tail -1 | cut -d= -f2)"
pgw2_port="$(grep -E '^PGW_API_HOST_PORT2=' "$ROOT/deploy/.env" | tail -1 | cut -d= -f2)"
workers_return() {
  docker start deploy-pgworker-1 deploy-pgworker-2-1 >/dev/null 2>&1 || true
  for i in $(seq 1 60); do
    workers_up "${pgw1_port:-8080}" && workers_up "${pgw2_port:-8083}" && return 0
    sleep 1
  done
  echo "❌ pgworker did not come alive after return (docker logs deploy-pgworker-1 deploy-pgworker-2-1)"; exit 1
}
docker stop deploy-pgworker-1 deploy-pgworker-2-1 >/dev/null
ect put /pgworker/moves/demo/bucket_8 \
  '{"op":"rollback","requested_unix":'"$(now)"',"requested_by":"e2e"}' >/dev/null
seen=""
for i in $(seq 1 30); do
  api /api/clusters/demo | jq -e 'any(.pendingMoves[]; .bucketId == 8 and .op == "rollback")' >/dev/null 2>&1 && { seen=1; break; }
  sleep 1
done
ect del /pgworker/moves/demo/bucket_8 >/dev/null
workers_return
[ "$seen" = 1 ] || { echo "❌ panel queue does not see the rollback request (bucket_8)"; exit 1; }
echo "  panel queue sees the rollback request (snapshot, executors stopped)"

# ── 5) rollback-заявка остаётся: процесс отвергает (нет подписки) → work-журнал.
ect put /pgworker/moves/demo/bucket_4 \
  '{"op":"rollback","requested_unix":'"$(( $(now) + 1 ))"',"requested_by":"e2e"}' >/dev/null
for i in $(seq 1 30); do
  api /api/clusters/demo | jq -e '.work != null and .work.op == "rollback" and .work.phase == "rejected" and (.work.lastError != null)' >/dev/null 2>&1 && break; sleep 2;
done
api /api/clusters/demo | jq -e '.work != null and .work.op == "rollback" and .work.phase == "rejected" and (.work.lastError | test("reverse subscription|re-copy"; "i"))' >/dev/null \
  || { echo "❌ work log did not show the rejected rollback (op/phase/lastError)"; exit 1; }
[ -z "$(ect get /pgworker/moves/demo/bucket_4 --print-value-only 2>/dev/null)" ] \
  || { echo "❌ rejected request bucket_4 not gone from the queue"; exit 1; }
echo "  rejected rollback: request gone, reason is in the worker log"

# ── 6) finalize: oldShard=владельцу → 409; несуществующий → 404; валидный → 201.
own0="$(routing_of bucket_0)"; notown0="$(other "$own0")"
c="$(code -X POST "$BASE/api/clusters/demo/moves/finalize" -H 'Content-Type: application/json' -d "{\"bucket\":0,\"oldShard\":\"$own0\"}")"
[ "$c" = 409 ] || { echo "❌ finalize oldShard=owner($own0) = $c, expected 409"; exit 1; }
c="$(code -X POST "$BASE/api/clusters/demo/moves/finalize" -H 'Content-Type: application/json' -d '{"bucket":0,"oldShard":"shard9"}')"
[ "$c" = 404 ] || { echo "❌ finalize oldShard=shard9 = $c, expected 404"; exit 1; }
c="$(code -X POST "$BASE/api/clusters/demo/moves/finalize" -H 'Content-Type: application/json' -d "{\"bucket\":0,\"oldShard\":\"$notown0\"}")"
[ "$c" = 201 ] || { echo "❌ finalize oldShard=$notown0 = $c, expected 201"; exit 1; }
ect get /pgworker/moves/demo/bucket_0 --print-value-only | grep -q "\"old_shard\":\"$notown0\"" \
  || { echo "❌ finalize key without old_shard"; exit 1; }
c="$(code -X DELETE "$BASE/api/clusters/demo/moves/bucket_0")"
[ "$c" = 204 ] || { echo "❌ finalize removal = $c"; exit 1; }
echo "  finalize: 409 owner / 404 no shard / 201 (old_shard in the key) / removal 204"

# ── 7) негативы: пустой buckets → 400; несуществующий кластер → 404.
c="$(code -X POST "$BASE/api/clusters/demo/moves/rollback" -H 'Content-Type: application/json' -d '{"buckets":[]}')"
[ "$c" = 400 ] || { echo "❌ rollback empty buckets = $c, expected 400"; exit 1; }
c="$(code -X POST "$BASE/api/clusters/nope/moves/abort" -H 'Content-Type: application/json' -d '{"bucket":0}')"
[ "$c" = 404 ] || { echo "❌ abort unknown cluster = $c, expected 404"; exit 1; }
echo "  negatives: empty buckets → 400; unknown cluster → 404"

# Финал: очередь демо-кластера пуста (все заявки сняты/исполнены).
for i in $(seq 1 15); do
  api /api/clusters/demo | jq -e '(.pendingMoves | length) == 0' >/dev/null 2>&1 && break; sleep 1;
done
api /api/clusters/demo | jq -e '(.pendingMoves | length) == 0' >/dev/null \
  || { echo "❌ demo request queue not empty after the check"; exit 1; }
echo "✓ 60-move-ops: move-ops via the panel API — all steps green"
