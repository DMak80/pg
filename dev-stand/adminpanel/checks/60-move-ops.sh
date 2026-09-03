#!/usr/bin/env bash
# Move-ops на демо-сиде через API панели (t07): rollback/finalize/abort +
# снятие заявок; результаты — etcd-ключи, очередь/статусы/work-журнал деталей
# кластера. Полный docker-цикл move→abort/rollback→finalize покрыт E2e t01 —
# здесь API/UI-слой без поднятия новых PG. Зависшие статусы насыпает чек
# (демо-сид аномалий не сеет — образец 20-alerts.sh): updated_unix=now —
# «свежий» (abort 409), now-300 — «несвежий» (>AbortMinAgeSec=120, <repair 600).
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${ADMINPANEL_URL:-http://localhost:5050}"
JAR="$(mktemp)"; trap 'rm -f "$JAR"' EXIT

# Arrange: демо-сид жив (идемпотентен), панель отвечает, etcd под рукой.
[ -n "$(docker compose exec -T etcd etcdctl get /clusters/demo/config --print-value-only 2>/dev/null)" ] \
  || { "$PWD/checks/05-seed.sh" pg; }
for i in $(seq 1 60); do curl -fsS "$BASE/api/healthz" >/dev/null 2>&1 && break; sleep 1; done
curl -fsS "$BASE/api/healthz" >/dev/null || { echo "❌ панель не отвечает: $BASE"; exit 1; }
curl -fsS -c "$JAR" -o /dev/null -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  || { echo "❌ login admin/admin не прошёл"; exit 1; }

api()  { curl -fsS -b "$JAR" "$BASE$1"; }
# POST с телом, возвращающий тело ответа без -f (409 ProblemDetails читаемы).
post() { curl -s -b "$JAR" -X POST "$BASE$1" -H 'Content-Type: application/json' -d "$2"; }
code() { curl -s -o /dev/null -w '%{http_code}' -b "$JAR" "$@"; }
ect()  { docker compose exec -T etcd etcdctl --endpoints=http://localhost:2379 "$@"; }
now()  { date +%s; }

# Сводная проверка тика панели (детали демо-кластера читаются).
for i in $(seq 1 15); do
  api /api/clusters/demo | jq -e '.name == "demo"' >/dev/null 2>&1 && break; sleep 1;
done
api /api/clusters/demo | jq -e '.name == "demo"' >/dev/null \
  || { echo "❌ /api/clusters/demo недоступен"; exit 1; }
echo "  панель жива, демо-кластер читается"

# ── 1) abort: свежий статус без force → 409; с force → 201; отмена заявки → 204/404.
ect put /clusters/demo/buckets/status/bucket_3 \
  "{\"bucket\":\"bucket_3\",\"state\":\"SYNCING\",\"owner\":\"s1\",\"target\":\"s2\",\"started_unix\":$(now),\"updated_unix\":$(now),\"phase\":\"copy\"}" >/dev/null
c="$(code -X POST "$BASE/api/clusters/demo/moves/abort" -H 'Content-Type: application/json' -d '{"bucket":3}')"
[ "$c" = 409 ] || { echo "❌ abort свежий без force = $c, ожидался 409"; exit 1; }
c="$(code -X POST "$BASE/api/clusters/demo/moves/abort" -H 'Content-Type: application/json' -d '{"bucket":3,"force":true}')"
# force-заявка поставлена этим запросом; повтор идентичной → 201 без записи (§9.7 п.3):
c="$(code -X POST "$BASE/api/clusters/demo/moves/abort" -H 'Content-Type: application/json' -d '{"bucket":3,"force":true}')"
[ "$c" = 201 ] || { echo "❌ повторный abort force = $c, ожидался 201 (идентичность)"; exit 1; }
ect get /pgworker/moves/demo/bucket_3 --print-value-only | grep -q '"op":"abort"' \
  || { echo "❌ ключ abort-заявки не содержит op=abort"; exit 1; }
ect get /pgworker/moves/demo/bucket_3 --print-value-only | grep -q '"force":true' \
  || { echo "❌ ключ abort-заявки без force:true"; exit 1; }
c="$(code -X DELETE "$BASE/api/clusters/demo/moves/bucket_3")"
[ "$c" = 204 ] || { echo "❌ снятие abort-заявки = $c, ожидался 204"; exit 1; }
[ -z "$(ect get /pgworker/moves/demo/bucket_3 --print-value-only 2>/dev/null)" ] \
  || { echo "❌ ключ заявки не исчез после DELETE"; exit 1; }
c="$(code -X DELETE "$BASE/api/clusters/demo/moves/bucket_3")"
[ "$c" = 404 ] || { echo "❌ повторное снятие = $c, ожидался 404 (не идемпотентно)"; exit 1; }
echo "  abort: свежий 409 → force 201 (идентичность) → снятие 204 → повтор 404"
ect del /clusters/demo/buckets/status/bucket_3 >/dev/null

# ── 2) abort: несвежий статус без force → 201 (force в JSON нет); ACTIVE → 409.
ect put /clusters/demo/buckets/status/bucket_7 \
  "{\"bucket\":\"bucket_7\",\"state\":\"ABORTING\",\"owner\":\"s2\",\"target\":\"s1\",\"started_unix\":$(( $(now) - 300 )),\"updated_unix\":$(( $(now) - 300 )),\"phase\":\"cleanup\"}" >/dev/null
c="$(code -X POST "$BASE/api/clusters/demo/moves/abort" -H 'Content-Type: application/json' -d '{"bucket":7}')"
[ "$c" = 201 ] || { echo "❌ abort несвежий без force = $c, ожидался 201"; exit 1; }
ect get /pgworker/moves/demo/bucket_7 --print-value-only | grep -q '"force"' \
  && { echo "❌ ключ несвежего abort содержит force (должен опускаться)"; exit 1; }
c="$(code -X DELETE "$BASE/api/clusters/demo/moves/bucket_7")"
[ "$c" = 204 ] || { echo "❌ снятие несвежего abort = $c"; exit 1; }
c="$(code -X POST "$BASE/api/clusters/demo/moves/abort" -H 'Content-Type: application/json' -d '{"bucket":0}')"
[ "$c" = 409 ] || { echo "❌ abort ACTIVE-бакета = $c, ожидался 409"; exit 1; }
ect del /clusters/demo/buckets/status/bucket_7 >/dev/null
echo "  abort: несвежий 201 (force опущен); ACTIVE → 409"

# ── 3) abort: routing==target без force → 409 «осознанно».
ect put /clusters/demo/buckets/status/bucket_2 \
  "{\"bucket\":\"bucket_2\",\"state\":\"FROZEN\",\"owner\":\"s1\",\"target\":\"s1\",\"started_unix\":$(( $(now) - 300 )),\"updated_unix\":$(( $(now) - 300 )),\"phase\":\"cutover-wait\"}" >/dev/null
resp="$(post /api/clusters/demo/moves/abort '{"bucket":2}')"
echo "$resp" | jq -e '.detail // empty' 2>/dev/null | grep -q "осознанно" \
  || { echo "❌ abort routing==target: ожидался 409 с «осознанно», получено: $resp"; exit 1; }
ect del /clusters/demo/buckets/status/bucket_2 >/dev/null
echo "  abort: routing==target без force → 409 «осознанно»"

# ── 4) rollback: ACTIVE-бакет → 201 (op=rollback); очередь панели видит; снятие → 204.
c="$(code -X POST "$BASE/api/clusters/demo/moves/rollback" -H 'Content-Type: application/json' -d '{"buckets":[6]}')"
[ "$c" = 201 ] || { echo "❌ rollback ACTIVE = $c, ожидался 201"; exit 1; }
ect get /pgworker/moves/demo/bucket_6 --print-value-only | grep -q '"op":"rollback"' \
  || { echo "❌ ключ rollback-заявки не содержит op=rollback"; exit 1; }
for i in $(seq 1 10); do
  api /api/clusters/demo | jq -e 'any(.pendingMoves[]; .bucketId == 6 and .op == "rollback")' >/dev/null 2>&1 && break; sleep 1;
done
api /api/clusters/demo | jq -e 'any(.pendingMoves[]; .bucketId == 6 and .op == "rollback")' >/dev/null \
  || { echo "❌ очередь панели не видит rollback-заявку bucket_6"; exit 1; }
c="$(code -X DELETE "$BASE/api/clusters/demo/moves/bucket_6")"
[ "$c" = 204 ] || { echo "❌ снятие rollback = $c"; exit 1; }
echo "  rollback: 201 → очередь панели видит → снятие 204"

# ── 5) rollback-заявка остаётся: процесс отвергает (нет подписки) → work-журнал.
ect put /pgworker/moves/demo/bucket_4 \
  '{"op":"rollback","requested_unix":'"$(( $(now) + 1 ))"',"requested_by":"e2e"}' >/dev/null
for i in $(seq 1 30); do
  api /api/clusters/demo | jq -e '.work != null and .work.op == "rollback" and .work.phase == "rejected" and (.work.lastError != null)' >/dev/null 2>&1 && break; sleep 2;
done
api /api/clusters/demo | jq -e '.work != null and .work.op == "rollback" and .work.phase == "rejected" and (.work.lastError | test("подписк|re-copy"; "i"))' >/dev/null \
  || { echo "❌ work-журнал не показал отвергнутый rollback (op/phase/lastError)"; exit 1; }
[ -z "$(ect get /pgworker/moves/demo/bucket_4 --print-value-only 2>/dev/null)" ] \
  || { echo "❌ отвергнутая заявка bucket_4 не исчезла из очереди"; exit 1; }
echo "  отвергнутый rollback: заявка исчезла, причина — в «Журнале воркера»"

# ── 6) finalize: oldShard=владельцу → 409; несуществующий → 404; валидный → 201.
c="$(code -X POST "$BASE/api/clusters/demo/moves/finalize" -H 'Content-Type: application/json' -d '{"bucket":0,"oldShard":"s1"}')"
[ "$c" = 409 ] || { echo "❌ finalize oldShard=owner = $c, ожидался 409"; exit 1; }
c="$(code -X POST "$BASE/api/clusters/demo/moves/finalize" -H 'Content-Type: application/json' -d '{"bucket":0,"oldShard":"s9"}')"
[ "$c" = 404 ] || { echo "❌ finalize oldShard=s9 = $c, ожидался 404"; exit 1; }
c="$(code -X POST "$BASE/api/clusters/demo/moves/finalize" -H 'Content-Type: application/json' -d '{"bucket":0,"oldShard":"s2"}')"
[ "$c" = 201 ] || { echo "❌ finalize oldShard=s2 = $c, ожидался 201"; exit 1; }
ect get /pgworker/moves/demo/bucket_0 --print-value-only | grep -q '"old_shard":"s2"' \
  || { echo "❌ ключ finalize без old_shard"; exit 1; }
c="$(code -X DELETE "$BASE/api/clusters/demo/moves/bucket_0")"
[ "$c" = 204 ] || { echo "❌ снятие finalize = $c"; exit 1; }
echo "  finalize: 409 владелец / 404 нет шарда / 201 (old_shard в ключе) / снятие 204"

# ── 7) негативы: пустой buckets → 400; несуществующий кластер → 404.
c="$(code -X POST "$BASE/api/clusters/demo/moves/rollback" -H 'Content-Type: application/json' -d '{"buckets":[]}')"
[ "$c" = 400 ] || { echo "❌ rollback пустой buckets = $c, ожидался 400"; exit 1; }
c="$(code -X POST "$BASE/api/clusters/nope/moves/abort" -H 'Content-Type: application/json' -d '{"bucket":0}')"
[ "$c" = 404 ] || { echo "❌ abort неизвестный кластер = $c, ожидался 404"; exit 1; }
echo "  негативы: пустой buckets → 400; неизвестный кластер → 404"

# Финал: очередь демо-кластера пуста (все заявки сняты/исполнены).
for i in $(seq 1 15); do
  api /api/clusters/demo | jq -e '(.pendingMoves | length) == 0' >/dev/null 2>&1 && break; sleep 1;
done
api /api/clusters/demo | jq -e '(.pendingMoves | length) == 0' >/dev/null \
  || { echo "❌ очередь заявок демо не пуста после чека"; exit 1; }
echo "✓ 60-move-ops: move-ops через API панели — все шаги зелёные"
