#!/usr/bin/env bash
# Репарация: нахераченные извне ключи → алерты → гашение РЕАЛЬНЫМ ремонтом
# живого PgWorker + полный move на каноническом кластере (demo/shard1|shard2)
# + сценарий shard-no-master с возвратом фактическим мастером (t41).
# Quick (без PgWorker/PG) — только появление алертов.
set -euo pipefail
cd "$(dirname "$0")/.."
# t03: проба живости pgworker — только mTLS (healthcheck-пара).
ROOT="$(cd ../.. && pwd)"
# Хост-порт публикации pgworker: env-оверрайд → .env (пишет 00-up.sh) → 8080.
PGW_API_HOST_PORT="${PGW_API_HOST_PORT:-$(awk -F= '$1=="PGW_API_HOST_PORT"{print $2}' "$ROOT/deploy/.env" 2>/dev/null)}"
PGW_API_HOST_PORT="${PGW_API_HOST_PORT:-8080}"
PG_MTLS="curl -fsS -m 3 --cacert $ROOT/deploy/tls/ca.pem --cert $ROOT/deploy/tls/healthcheck.crt --key $ROOT/deploy/tls/healthcheck.key"

BASE="${ADMINPANEL_URL:-http://localhost:5050}"
JAR="$(mktemp)"; trap 'rm -f "$JAR"' EXIT
ect() { docker compose exec -T etcd1 etcdctl --endpoints=http://localhost:2379 "$@"; }

curl -fsS -c "$JAR" -o /dev/null -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  || { echo "❌ login (adminpanel at $BASE? run docker compose up -d adminpanel)"; exit 1; }
api() { curl -fsS -b "$JAR" "$BASE$1"; }

has_alert() { # kind target [severity]
  api /api/alerts | jq -e --arg k "$1" --arg t "$2" --arg s "${3:-}" \
    'any(.[]; .kind==$k and .target==$t and ($s=="" or .severity==$s))' >/dev/null
}
wait_alert() {
  for i in $(seq 1 15); do has_alert "$1" "$2" "${3:-}" && return 0; sleep 1; done
  echo "❌ alert $1 -> $2${3:+ ($3)} did not appear within 15 s"; return 1
}
wait_no_alert() { # kind target [timeout_sec]
  local t="${3:-120}"
  for i in $(seq 1 "$t"); do has_alert "$1" "$2" || return 0; sleep 1; done
  echo "❌ alert $1 -> $2 did not clear within ${t} s"; return 1
}
wait_routing() { # bucket owner [timeout_sec]
  local t="${3:-180}" want
  for i in $(seq 1 "$t"); do
    want="$(ect get "/clusters/demo/buckets/routing/$1" --print-value-only 2>/dev/null || true)"
    [ "$want" = "$2" ] && return 0
    sleep 2
  done
  echo "❌ routing $1 did not become '$2' within ${t} s"; return 1
}
wait_request_gone() { # bucket [timeout_sec] — auto-finalize доработал (заявка снята)
  local t="${2:-180}"
  for i in $(seq 1 "$t"); do
    [ -z "$(ect get "/pgworker/moves/demo/$1" --print-value-only 2>/dev/null)" ] && return 0
    sleep 2
  done
  echo "❌ request $1 not removed within ${t} s (finalize did not finish)"; return 1
}

# Act 1: нахерачиваем брошенные статусы извне (протухшие: updated_unix=now-3600,
# пороги репарации RepairStaleSec=600/RepairFrozenSec=120 истекли заранее —
# гашение пойдёт в первые же тики воркера, spec §3.6/§9).
# Full-режим (живой воркер): паузим его ДО нахерачивания — иначе репарация
# (тик 5 c) гасит статусы быстрее тика панели (3 c) и Assert 2 «алерты
# появились» становится гонкой; старт воркера после Assert 2 = «появление →
# ремонт → гашение» детерминированно.
full=0
if $PG_MTLS https://localhost:${PGW_API_HOST_PORT:-8080}/healthz >/dev/null 2>&1; then
  full=1
  # t07: паузим ОБА инстанса — репарация идёт у держателя клэйма, живой
  # второй гасил бы статусы быстрее тика панели (гонка Assert 2)
  echo "  (full) pausing PgWorker (deploy-pgworker-1 + deploy-pgworker-2-1) before spoofing"
  docker stop -t 3 deploy-pgworker-1 deploy-pgworker-2-1 >/dev/null
fi
# t41: раскладка каноническая (её строит движок, не фиксированный сид) —
# owner нахераченного статуса = ФАКТИЧЕСКИЙ routing бакета, target = другой
# шард (иначе routing==target выглядит как «flip прошёл» и abort отвергается).
other() { [ "$1" = shard1 ] && echo shard2 || echo shard1; }
spoof_status() { # bucket state extra
  local b="$1" st="$2" extra="$3" own tgt
  own="$(ect get "/clusters/demo/buckets/routing/$b" --print-value-only)"
  [ -n "$own" ] || { echo "❌ routing $b is empty"; exit 1; }
  tgt="$(other "$own")"
  now=$(date +%s); past=$((now - 3600))
  ect put "/clusters/demo/buckets/status/$b" \
    "{\"bucket\":\"$b\",\"state\":\"$st\",\"owner\":\"$own\",\"target\":\"$tgt\",\"started_unix\":$past,\"updated_unix\":$past$extra}" >/dev/null
}
spoof_status bucket_3  SYNCING  ',"phase":"copy"'
spoof_status bucket_7  ABORTING ',"phase":"cleanup","last_error":"receiver went away"'
spoof_status bucket_11 FROZEN   ',"phase":"cutover-wait"'
echo "  bucket_3/7/11 statuses spoofed (stale; owner=actual routing)"

# Assert 2: алерты появились (тик панели 3 c; воркер на паузе в full).
wait_alert move-stale demo/bucket_11;   echo "  move-stale -> demo/bucket_11"
wait_alert move-stale demo/bucket_3;    echo "  move-stale -> demo/bucket_3"
wait_alert move-stale demo/bucket_7;    echo "  move-stale -> demo/bucket_7"
wait_alert move-aborting demo/bucket_7; echo "  move-aborting -> demo/bucket_7"
wait_alert move-frozen-long demo/bucket_11 critical; echo "  move-frozen-long -> demo/bucket_11 (critical)"

# Full-ветка (живой PgWorker, 00-up.sh шаг 9): гашение ремонтом + move-цикл
# + сохранённый сценарий shard-no-master.
if [ "$full" != 1 ]; then
  echo "  (quick) PgWorker is not up — alert appearance check passed, exiting"
  exit 0
fi
echo "  (full) starting PgWorker — repair clears the alerts by fixing"
# t07: оба инстанса (пауза была у обоих)
docker start deploy-pgworker-1 deploy-pgworker-2-1 >/dev/null
for i in $(seq 1 60); do $PG_MTLS https://localhost:${PGW_API_HOST_PORT:-8080}/healthz >/dev/null 2>&1 && break; sleep 1; done
$PG_MTLS https://localhost:${PGW_API_HOST_PORT:-8080}/healthz >/dev/null 2>&1 \
  || { echo "❌ PgWorker did not come alive after start (:${PGW_API_HOST_PORT:-8080}/healthz)"; exit 1; }

# Assert 3: гашение = статус-ключи сняты воркером (репарация), routing не тронут.
wait_no_alert move-stale demo/bucket_11;  echo "  move-stale -> demo/bucket_11 cleared"
wait_no_alert move-aborting demo/bucket_7; echo "  move-aborting -> demo/bucket_7 cleared"
wait_no_alert move-frozen-long demo/bucket_11; echo "  move-frozen-long -> demo/bucket_11 cleared"
[ -z "$(ect get /clusters/demo/buckets/status/bucket_3 --print-value-only 2>/dev/null)" ] \
  || { echo "❌ bucket_3 status not removed"; exit 1; }
own3="$(ect get /clusters/demo/buckets/routing/bucket_3 --print-value-only)"
[ -n "$own3" ] \
  || { echo "❌ routing bucket_3 is empty"; exit 1; }
echo "  statuses removed by repair, routing untouched"

# Act/Assert 4: полный move на каноническом кластере (bucket_5: shard2→shard1→
# shard2; возврат раскладки). Между ходами ждём завершения auto-finalize
# (заявка снята = старый шард вычищен) — вторая заявка поверх недоделанного
# finalize затирала бы уборку: остаток схемы на старом шарде отверг бы
# обратный move.
from5="$(ect get /clusters/demo/buckets/routing/bucket_5 --print-value-only)"; to5="$(other "$from5")"
ect put /pgworker/moves/demo/bucket_5 \
  "{\"op\":\"move\",\"to\":\"$to5\",\"requested_unix\":$now,\"requested_by\":\"check-20\"}" >/dev/null
wait_routing bucket_5 "$to5"; echo "  bucket_5 moved $from5 → $to5 (full move)"
wait_request_gone bucket_5 120; echo "  finalize finished (previous owner artifacts cleaned up)"
wait_no_alert move-stale demo/bucket_5 60
ect put /pgworker/moves/demo/bucket_5 \
  "{\"op\":\"move\",\"to\":\"$from5\",\"requested_unix\":$(date +%s),\"requested_by\":\"check-20\"}" >/dev/null
wait_routing bucket_5 "$from5"; echo "  bucket_5 returned $to5 → $from5 (original layout)"
wait_request_gone bucket_5 120; echo "  finalize finished (previous owner artifacts cleaned up)"

# Assert 5: ни одного move-* алерта перед финальным сценарием.
api /api/alerts | jq -e 'all(.[]; (.kind | startswith("move-")) | not)' >/dev/null \
  || { echo "❌ move-* alerts remain"; exit 1; }
echo "  no move-* alerts"

# Act 6 / Assert 7: shard-no-master (critical) на каноническом кластере:
# удаление master-ключа шарда → алерт → возврат ключа ФАКТИЧЕСКИМ мастером
# (Patroni-lease живых нод перепишет; значение — из HA-грани панели).
ect del /clusters/demo/shards/shard2/master >/dev/null
echo "  shard2 master key deleted"
wait_alert shard-no-master demo/shard2 critical
echo "  shard-no-master -> demo/shard2 (critical)"
shard2_master="$(api /api/ha/demo-shard2 | jq -r '.leaderName + ":5432"')"
[ -n "$shard2_master" ] && [ "$shard2_master" != ":5432" ] \
  || { echo "❌ /api/ha/demo-shard2: empty leaderName for master key restore"; exit 1; }
ect put /clusters/demo/shards/shard2/master "$shard2_master" >/dev/null
echo "  shard2 master key restored by the actual master ($shard2_master)"
wait_no_alert shard-no-master demo/shard2
echo "  shard-no-master -> demo/shard2 cleared"

echo "✓ alerts/repair scenario green (appearance → repair → clearing; move back and forth; shard-no-master)"

# 6) Доступность API воркера (full; spec §9.3/§9.5): ключ жив → мутации идут;
#    остановлен → 503 + алерт worker-api-unreachable; возврат → гашение.
#    Quick-стенд без pgworker (нет :8080/healthz) — шаг пропускается.
if $PG_MTLS https://localhost:${PGW_API_HOST_PORT:-8080}/healthz >/dev/null 2>&1; then
  ect get /pgworker/api/ --prefix --keys-only | grep -q . || { echo "❌ no /pgworker/api/*"; exit 1; }
  # t07: оба инстанса — иначе второй держит ключ /pgworker/api/ (ни 503, ни алерта)
  ( cd ../../deploy && docker compose stop pgworker pgworker-2 >/dev/null 2>&1 )
  code="$(curl -s -o /dev/null -w '%{http_code}' -b "$JAR" -X POST "$BASE/api/clusters" \
    -H 'Content-Type: application/json' -d '{"name":"probeapi"}')"
  [ "$code" = 503 ] || { echo "❌ mutation with a dead worker = $code (expected 503)"; exit 1; }
  echo "  adminpanel mutation with pgworker stopped -> 503 (reads alive)"
  # алерт: тик ≤3 c ×2 + lease ≤15 c
  for i in $(seq 1 20); do
    curl -fsS -b "$JAR" "$BASE/api/alerts" | jq -e 'any(.[]; .kind=="worker-api-unreachable" and .target=="pgworker")' >/dev/null 2>&1 && break
    sleep 2
  done
  curl -fsS -b "$JAR" "$BASE/api/alerts" | jq -e 'any(.[]; .kind=="worker-api-unreachable" and .target=="pgworker")' >/dev/null \
    || { echo "❌ worker-api-unreachable did not appear"; exit 1; }
  echo "  worker-api-unreachable -> pgworker (critical) appeared"
  ( cd ../../deploy && docker compose start pgworker pgworker-2 >/dev/null 2>&1 )
  for i in $(seq 1 30); do $PG_MTLS https://localhost:${PGW_API_HOST_PORT:-8080}/healthz >/dev/null 2>&1 && break; sleep 1; done
  # Гашение: ключ /pgworker/api/ восстановился (keepalive ≤15 c) + 2 тика панели → алерт исчез.
  for i in $(seq 1 20); do
    curl -fsS -b "$JAR" "$BASE/api/alerts" | jq -e 'any(.[]; .kind=="worker-api-unreachable" and .target=="pgworker") | not' >/dev/null 2>&1 && break
    sleep 2
  done
  curl -fsS -b "$JAR" "$BASE/api/alerts" | jq -e 'any(.[]; .kind=="worker-api-unreachable" and .target=="pgworker") | not' >/dev/null \
    || { echo "❌ worker-api-unreachable did not clear after pgworker returned"; exit 1; }
  echo "  worker-api-unreachable: 503 mutations + alert + jq-clearing after recovery — ok"
else
  echo "  (quick: pgworker is not up — worker-api-unreachable step skipped)"
fi

echo "✓ alerts scenario green"
