#!/usr/bin/env bash
# Репарация + усыновление воркером: нахераченные извне ключи → алерты →
# гашение РЕАЛЬНЫМ ремонтом живого PgWorker + полный move на усыновлённом
# кластере + прежний сценарий shard-no-master (adopt-repair spec §3.6).
# Quick (без PgWorker/PG) — только появление алертов.
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${ADMINPANEL_URL:-http://localhost:5050}"
JAR="$(mktemp)"; trap 'rm -f "$JAR"' EXIT
ect() { docker compose exec -T etcd etcdctl --endpoints=http://localhost:2379 "$@"; }

curl -fsS -c "$JAR" -o /dev/null -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  || { echo "❌ login (панель на $BASE? запусти docker compose up -d adminpanel)"; exit 1; }
api() { curl -fsS -b "$JAR" "$BASE$1"; }

has_alert() { # kind target [severity]
  api /api/alerts | jq -e --arg k "$1" --arg t "$2" --arg s "${3:-}" \
    'any(.[]; .kind==$k and .target==$t and ($s=="" or .severity==$s))' >/dev/null
}
wait_alert() {
  for i in $(seq 1 15); do has_alert "$1" "$2" "${3:-}" && return 0; sleep 1; done
  echo "❌ алерт $1 -> $2${3:+ ($3)} не появился за 15 c"; return 1
}
wait_no_alert() { # kind target [timeout_sec]
  local t="${3:-120}"
  for i in $(seq 1 "$t"); do has_alert "$1" "$2" || return 0; sleep 1; done
  echo "❌ алерт $1 -> $2 не погас за ${t} c"; return 1
}
wait_routing() { # bucket owner [timeout_sec]
  local t="${3:-180}" want
  for i in $(seq 1 "$t"); do
    want="$(ect get "/clusters/demo/buckets/routing/$1" --print-value-only 2>/dev/null || true)"
    [ "$want" = "$2" ] && return 0
    sleep 2
  done
  echo "❌ routing $1 не стал '$2' за ${t} c"; return 1
}
wait_request_gone() { # bucket [timeout_sec] — auto-finalize доработал (заявка снята)
  local t="${2:-180}"
  for i in $(seq 1 "$t"); do
    [ -z "$(ect get "/pgworker/moves/demo/$1" --print-value-only 2>/dev/null)" ] && return 0
    sleep 2
  done
  echo "❌ заявка $1 не снята за ${t} c (finalize не доработал)"; return 1
}

# Act 1: нахерачиваем брошенные статусы извне (протухшие: updated_unix=now-3600,
# пороги репарации RepairStaleSec=600/RepairFrozenSec=120 истекли заранее —
# гашение пойдёт в первые же тики воркера, spec §3.6/§9).
# Full-режим (живой воркер): паузим его ДО нахерачивания — иначе репарация
# (тик 5 c) гасит статусы быстрее тика панели (3 c) и Assert 2 «алерты
# появились» становится гонкой; старт воркера после Assert 2 = «появление →
# ремонт → гашение» детерминированно.
full=0
if curl -fsS -m 3 http://localhost:8080/healthz >/dev/null 2>&1; then
  full=1
  echo "  (full) пауза PgWorker (deploy-pgworker-1) до нахерачивания"
  docker stop -t 3 deploy-pgworker-1 >/dev/null
fi
now=$(date +%s); past=$((now - 3600))
ect put /clusters/demo/buckets/status/bucket_3 \
  "{\"bucket\":\"bucket_3\",\"state\":\"SYNCING\",\"owner\":\"s1\",\"target\":\"s2\",\"started_unix\":$past,\"updated_unix\":$past,\"phase\":\"copy\"}" >/dev/null
ect put /clusters/demo/buckets/status/bucket_7 \
  "{\"bucket\":\"bucket_7\",\"state\":\"ABORTING\",\"owner\":\"s2\",\"target\":\"s1\",\"started_unix\":$past,\"updated_unix\":$past,\"phase\":\"cleanup\",\"last_error\":\"receiver went away\"}" >/dev/null
ect put /clusters/demo/buckets/status/bucket_11 \
  "{\"bucket\":\"bucket_11\",\"state\":\"FROZEN\",\"owner\":\"s1\",\"target\":\"s2\",\"started_unix\":$past,\"updated_unix\":$past,\"phase\":\"cutover-wait\"}" >/dev/null
echo "  статусы bucket_3/7/11 нахерачены (протухшие)"

# Assert 2: алерты появились (тик панели 3 c; воркер на паузе в full).
wait_alert move-stale demo/bucket_11;   echo "  move-stale -> demo/bucket_11"
wait_alert move-stale demo/bucket_3;    echo "  move-stale -> demo/bucket_3"
wait_alert move-stale demo/bucket_7;    echo "  move-stale -> demo/bucket_7"
wait_alert move-aborting demo/bucket_7; echo "  move-aborting -> demo/bucket_7"
wait_alert move-frozen-long demo/bucket_11 critical; echo "  move-frozen-long -> demo/bucket_11 (critical)"

# Full-ветка (живой PgWorker, 00-up.sh шаг 9): гашение ремонтом + move-цикл
# + сохранённый сценарий shard-no-master.
if [ "$full" != 1 ]; then
  echo "  (quick) PgWorker не поднят — проверка появления алертов пройдена, выход"
  exit 0
fi
echo "  (full) старт PgWorker — репарация гасит алерты ремонтом"
docker start deploy-pgworker-1 >/dev/null
for i in $(seq 1 60); do curl -fsS -m 3 http://localhost:8080/healthz >/dev/null 2>&1 && break; sleep 1; done
curl -fsS -m 3 http://localhost:8080/healthz >/dev/null 2>&1 \
  || { echo "❌ PgWorker не ожил после старта (:8080/healthz)"; exit 1; }

# Assert 3: гашение = статус-ключи сняты воркером (репарация), routing не тронут.
wait_no_alert move-stale demo/bucket_11;  echo "  move-stale -> demo/bucket_11 погашен"
wait_no_alert move-aborting demo/bucket_7; echo "  move-aborting -> demo/bucket_7 погашен"
wait_no_alert move-frozen-long demo/bucket_11; echo "  move-frozen-long -> demo/bucket_11 погашен"
[ -z "$(ect get /clusters/demo/buckets/status/bucket_3 --print-value-only 2>/dev/null)" ] \
  || { echo "❌ статус bucket_3 не снят"; exit 1; }
[ "$(ect get /clusters/demo/buckets/routing/bucket_3 --print-value-only)" = "s1" ] \
  || { echo "❌ routing bucket_3 изменился (ожидался владелец s1)"; exit 1; }
echo "  статусы сняты репарацией, routing нетронут"

# Assert 3.5: усыновление — portalloc внешнего кластера восстановлен: ВСЕ 4 ноды
# (s1a/s1b/s2a/s2b) с object-контейнерами as-* (spec §3.6 Assert 5 / §7.1).
alloc="$(ect get /pgworker/portalloc/demo --print-value-only 2>/dev/null || true)"
echo "$alloc" | jq -e 'has("s1/s1a") and has("s1/s1b") and has("s2/s2a") and has("s2/s2b")
  and ([.[].object] | all(. != null and startswith("as-")))' >/dev/null \
  || { echo "❌ portalloc/demo: ожидались все 4 ноды с object: as-* — получено: $alloc"; exit 1; }
echo "  portalloc/demo: все 4 ноды усыновлены (object: as-*)"

# Act/Assert 4: полный move на усыновлённом кластере (bucket_5: s2→s1→s2;
# возврат раскладки — чек 40 ждёт инвентарь 10+6, spec §3.6). Между ходами
# ждём завершения auto-finalize (заявка снята = старый шард вычищен) — вторая
# заявка поверх недоделанного finalize затирала бы уборку: остаток схемы на
# старом шарде отверг бы обратный move.
ect put /pgworker/moves/demo/bucket_5 \
  "{\"op\":\"move\",\"to\":\"s1\",\"requested_unix\":$now,\"requested_by\":\"check-20\"}" >/dev/null
wait_routing bucket_5 s1; echo "  bucket_5 переехал s2 → s1 (полный move на усыновлённом)"
wait_request_gone bucket_5 120; echo "  finalize доработал (артефакты s2 вычищены)"
wait_no_alert move-stale demo/bucket_5 60
ect put /pgworker/moves/demo/bucket_5 \
  "{\"op\":\"move\",\"to\":\"s2\",\"requested_unix\":$(date +%s),\"requested_by\":\"check-20\"}" >/dev/null
wait_routing bucket_5 s2; echo "  bucket_5 вернулся s1 → s2 (раскладка исходная)"
wait_request_gone bucket_5 120; echo "  finalize доработал (артефакты s1 вычищены)"

# Assert 5: ни одного move-* алерта перед финальным сценарием.
api /api/alerts | jq -e 'all(.[]; (.kind | startswith("move-")) | not)' >/dev/null \
  || { echo "❌ остались move-* алерты"; exit 1; }
echo "  move-* алертов нет"

# Act 6 / Assert 7: прежний сценарий shard-no-master (сохранён из действующего
# чека, t10 §7.3): после усыновления мастер-ключ s2 пишет внешний HA-контур
# (эмуляторы) — стоп эмуляторов + удаление ключа по-прежнему корректны.
full=0
if docker compose ps --services --filter status=running 2>/dev/null | grep -qx hc2a; then
  full=1
  echo "  (full) стоп эмуляторов s2: hc2a/hc2b"
  docker compose stop hc2a hc2b >/dev/null
fi
ect del /clusters/demo/shards/s2/master >/dev/null
echo "  master-ключ s2 удалён"
wait_alert shard-no-master demo/s2 critical
echo "  shard-no-master -> demo/s2 (critical)"
if [ "$full" = 1 ]; then
  docker compose start hc2a hc2b >/dev/null
  echo "  (full) эмуляторы s2 запущены — lease восстановится сам (<=3 c)"
else
  ect put /clusters/demo/shards/s2/master 's2a:5432' >/dev/null
  echo "  (quick) ключ возвращён статично"
fi
wait_no_alert shard-no-master demo/s2
echo "  shard-no-master -> demo/s2 погас"

echo "✓ alerts/repair-сценарий зелёный (появление → ремонт → гашение; move туда-обратно; shard-no-master)"
