#!/usr/bin/env bash
# Покрытие ops-скриптов, НЕ задействованных чеками 10–74:
#   1) синтаксис (bash -n) всех arch/scripts/*.sh — patronictl/switchover/
#      rebuild-node на стенде неприменимы (нужны Patroni-контейнеры; их роль —
#      плановая смена мастера и пересоздание ноды — в чеках 30/68/70 руками
#      делают docker stop/promote/pg_basebackup), для них проверяется синтаксис;
#   2) find-leader/get-role/health/cluster-state — Patroni REST API на стенде
#      эмулируют сайдкары hc* (/primary 200 только у мастера, /replica, /):
#      запускаются из opsbox (curl в образе) против шарда 2 (s2a мастер +
#      s2b реплика — «полноценный кластер» после 70-го);
#   3) restore-system.sh (P22, оркестратор порядка etcd → шарды → карта):
#      plan/run на живой системе; при остановленном etcd — plan указывает
#      шаг 1, run --snapshot делегирует restore-cluster.sh restore (на
#      ops-машине без docker печатает прод-процедуру и честно завершается
#      кодом 3 — «подними etcd и повтори run»), после возврата etcd — run
#      доводит до зелёного verify. Физический restore data-dir из снапшота
#      docker-автоматикой с хоста стенда уже покрыт чеком 74.
# Предусловие: после 74-го (s1b мастер s1; s2a мастер s2 + s2b реплика;
# кластеры alpha/gamma в etcd, gamma bucket_0 на s2 после heal).
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p logs snapshots

# --no-deps: opsbox зависит от etcd, а Act 4 проверяет поведение БЕЗ него
ops() { local s="$1"; shift; docker compose run --rm -T --no-deps opsbox bash "/arch/scripts/$s" "$@"; }
# Patroni-REST скрипты (find-leader/get-role/health/cluster-state) читают env,
# а не buckets.env — передаём топологию стенда явно
rest() { local s="$1"; shift; docker compose run --rm -T --no-deps \
           -e ETCD_ENDPOINTS=http://etcd:2379 -e ALL_NODES="s2a s2b" \
           opsbox bash "/arch/scripts/$s" "$@"; }
h2()  { docker compose run --rm -T --no-deps opsbox psql -X -qAt -v ON_ERROR_STOP=1 "host=hap2 port=5432 dbname=postgres user=postgres" "$@"; }
ect() { docker exec etcd etcdctl --endpoints=http://localhost:2379 "$@"; }

# ═══ Arrange 0: предусловие — состояние после 74-го ═══════════════════════════
[ "$(h2 -c 'select pg_is_in_recovery()' 2>/dev/null)" = "f" ] \
  || { echo "❌ hap2 does not route to master s2a — run after check 74"; exit 1; }
[ "$(docker exec s2b psql -U postgres -tAc 'select pg_is_in_recovery()' 2>/dev/null)" = "t" ] \
  || { echo "❌ s2b is not a replica — run after check 74"; exit 1; }
[ "$(ect get /clusters/gamma/buckets/routing/bucket_0 --print-value-only 2>/dev/null)" = "s2" ] \
  || { echo "❌ gamma is not in the post-74 state — run after check 74"; exit 1; }
echo "  preconditions: s2a master + s2b replica, gamma alive (bucket_0 → s2) ✓"

# ═══ Act 1: синтаксис всех ops-скриптов ════════════════════════════════════════
echo ">>> Act 1: bash -n of all arch/scripts/*.sh (patronictl/switchover/rebuild-node do not run on the stand — no Patroni)"
for f in ../scripts/*.sh; do
  bash -n "$f" || { echo "❌ syntax error in $f"; exit 1; }
done
echo "  syntax ok: $(ls ../scripts/*.sh | wc -l | tr -d ' ') scripts ✓"

# ═══ Act 2: Patroni-REST скрипты против сайдкаров (эмуляция Patroni API) ══════
echo ">>> Act 2: find-leader / get-role / health / cluster-state (sidecars = Patroni REST)"

# find-leader: etcd-DCS (/service/* на стенде нет) и Patroni /cluster (сайдкар
# не отдаёт) отпадают — лидер находится опросом /primary (200 только у мастера)
out="$(rest find-leader.sh)"
grep -q 'Leader = s2a' <<<"$out" || { echo "❌ find-leader did not find s2a: $out"; exit 1; }
grep -q 'source: patroni /primary == 200' <<<"$out" || { echo "❌ find-leader did not use /primary: $out"; exit 1; }
echo "  find-leader: Leader = s2a (the fallback chain reached /primary) ✓"

# get-role: мастер rc=0, реплика rc=1 (коды — контракт для скриптов мониторинга)
out="$(rest get-role.sh s2a)"
grep -q 's2a: MASTER (leader)' <<<"$out" || { echo "❌ get-role s2a: $out"; exit 1; }
rc=0; out="$(rest get-role.sh s2b)" || rc=$?
[ "$rc" = 1 ] || { echo "❌ get-role s2b: expected rc=1 (REPLICA), got $rc"; exit 1; }
grep -q 's2b: REPLICA' <<<"$out" || { echo "❌ get-role s2b: $out"; exit 1; }
echo "  get-role: s2a=MASTER (rc 0), s2b=REPLICA (rc 1) ✓"

# health: стендовой etcd ОДИН — кворума 2/3 нет, health честно ругается (ровно
# 1 проблема); лидер один, API отвечают. В проде etcd x3 — эта ветка зелёная.
rc=0; out="$(rest health.sh)" || rc=$?
[ "$rc" = 1 ] || { echo "❌ health: expected rc=1 (a single etcd without quorum), got $rc"; exit 1; }
grep -q 'etcd: NO quorum' <<<"$out" || { echo "❌ health did not notice the single etcd: $out"; exit 1; }
grep -q 'leader: exactly one (s2a)' <<<"$out" || { echo "❌ health: the leader is not single: $out"; exit 1; }
grep -q 'RESULT: PROBLEMS (1)' <<<"$out" || { echo "❌ health: the problem count is not 1? $out"; exit 1; }
echo "  health: exactly 1 problem — etcd quorum (a single-node stand, honestly reported); leader/API ok ✓"

# cluster-state: сводка не падает, лидер и etcd-health в выводе (JSON-ролей
# сайдкар не отдаёт — колонки ролей пусты, ограничение эмуляции; patronictl-
# блок отрабатывает graceful — docker на ops-машине не ставится)
out="$(rest cluster-state.sh)"
grep -q 'Leader = s2a' <<<"$out" || { echo "❌ cluster-state without a leader: $out"; exit 1; }
grep -q 'is healthy' <<<"$out" || { echo "❌ cluster-state: etcd health not shown: $out"; exit 1; }
echo "  cluster-state: leader + etcd health in the summary ✓"

# ═══ Act 3: restore-system.sh (P22) — plan + run на живой системе ══════════════
echo ">>> Act 3: restore-system.sh plan/run (the gamma cluster, everything alive)"
out="$(ops restore-system.sh --cluster gamma plan 2>&1)"
grep -q 'etcd: reachable' <<<"$out" || { echo "❌ plan: etcd not seen: $out"; exit 1; }
grep -q 'initialized (N=4' <<<"$out" || { echo "❌ plan: cluster not seen: $out"; exit 1; }
grep -q "shard 's1': reachable" <<<"$out" || { echo "❌ plan: s1 unreachable: $out"; exit 1; }
grep -q "shard 's2': reachable" <<<"$out" || { echo "❌ plan: s2 unreachable: $out"; exit 1; }
echo "  plan: etcd reachable, cluster initialized, both shards reachable ✓"

ops restore-system.sh --cluster gamma run 2>&1 | tee logs/76-run.log | grep -v "Container\|Creating\|Created" | sed 's/^/  /'
grep -q 'no restore needed' logs/76-run.log || { echo "❌ run: a live etcd must not restore"; exit 1; }
grep -q 'the map is consistent — restore finished' logs/76-run.log \
  || { echo "❌ run: did not reach the end"; exit 1; }
[ "$(ect get /clusters/gamma/buckets/routing/bucket_0 --print-value-only)" = "s2" ] \
  || { echo "❌ run changed routing (it must not)"; exit 1; }
echo "  run: steps 1–3 passed, verify green, routing untouched ✓"

# ═══ Act 4: потеря контрол-плейна — диагностика и отказ-путь ═══════════════════
echo ">>> Act 4: etcd stopped — plan points to step 1; run --snapshot honestly exits with code 3"
ops restore-cluster.sh --cluster gamma snapshot check76 >/dev/null 2>&1
snap="$(ls -t snapshots/snap-gamma-*check76.snapshot | head -1)"
[ -n "$snap" ] || { echo "❌ the check76 snapshot was not taken"; exit 1; }
echo "  a just-in-case snapshot: $(basename "$snap")"

docker compose stop etcd >/dev/null 2>&1
out="$(ops restore-system.sh --cluster gamma plan 2>&1)"
grep -q 'etcd is UNREACHABLE' <<<"$out" || { echo "❌ plan did not notice the etcd loss: $out"; exit 1; }
echo "  plan on a stopped etcd: 'etcd is UNREACHABLE — step 1: bring the layer up + restore' ✓"

# run --snapshot: делегат restore-cluster restore на ops-машине БЕЗ docker
# печатает прод-процедуру и не восстанавливает — run завершается кодом 3
# (в проде: подними etcd по процедуре и повтори run; сам физический restore
# docker-автоматикой с хоста стенда проверен чеком 74)
rc=0; ops restore-system.sh --cluster gamma run --snapshot "/snapshots/$(basename "$snap")" --yes \
      >logs/76-run-snap.log 2>&1 || rc=$?
[ "$rc" = 3 ] || { echo "❌ run --snapshot: expected rc=3, got $rc"; cat logs/76-run-snap.log; exit 1; }
grep -q 'etcd restore procedure' logs/76-run-snap.log \
  || { echo "❌ the delegate did not print the prod procedure"; cat logs/76-run-snap.log; exit 1; }
grep -q 'still unreachable' logs/76-run-snap.log \
  || { echo "❌ run did not refuse after the failed restore"; cat logs/76-run-snap.log; exit 1; }
echo "  run --snapshot: delegation → prod procedure → an honest refusal (rc=3) ✓"

docker compose start etcd >/dev/null 2>&1
ok=""
for _ in $(seq 1 30); do
  docker exec etcd etcdctl --endpoints=http://localhost:2379 endpoint health >/dev/null 2>&1 && { ok=1; break; }
  sleep 1
done
[ -n "$ok" ] || { echo "❌ etcd did not come back after start"; exit 1; }

ops restore-system.sh --cluster gamma run 2>&1 | tee logs/76-run-after.log | grep -v "Container\|Creating\|Created" | sed 's/^/  /'
grep -q 'the map is consistent — restore finished' logs/76-run-after.log \
  || { echo "❌ the repeated run did not reach the end"; exit 1; }
echo "  etcd returned → run carried the restore to a green verify ✓"

echo "✓ ops scripts on the stand: the REST scripts work via sidecars (Patroni API emulation);"
echo "  restore-system.sh (P22): plan/run on a live system, etcd-loss diagnostics,"
echo "  the no-docker delegation refusal path, and completion after etcd returns"
