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
  || { echo "❌ hap2 не ведёт на мастера s2a — запусти после 74-го"; exit 1; }
[ "$(docker exec s2b psql -U postgres -tAc 'select pg_is_in_recovery()' 2>/dev/null)" = "t" ] \
  || { echo "❌ s2b не реплика — запусти после 74-го"; exit 1; }
[ "$(ect get /clusters/gamma/buckets/routing/bucket_0 --print-value-only 2>/dev/null)" = "s2" ] \
  || { echo "❌ gamma не в пост-74 состоянии — запусти после 74-го"; exit 1; }
echo "  предусловия: s2a мастер + s2b реплика, gamma жива (bucket_0 → s2) ✓"

# ═══ Act 1: синтаксис всех ops-скриптов ════════════════════════════════════════
echo ">>> Act 1: bash -n всех arch/scripts/*.sh (patronictl/switchover/rebuild-node на стенде не запускаются — нет Patroni)"
for f in ../scripts/*.sh; do
  bash -n "$f" || { echo "❌ синтаксис $f"; exit 1; }
done
echo "  синтаксис ок: $(ls ../scripts/*.sh | wc -l | tr -d ' ') скриптов ✓"

# ═══ Act 2: Patroni-REST скрипты против сайдкаров (эмуляция Patroni API) ══════
echo ">>> Act 2: find-leader / get-role / health / cluster-state (сайдкары = Patroni REST)"

# find-leader: etcd-DCS (/service/* на стенде нет) и Patroni /cluster (сайдкар
# не отдаёт) отпадают — лидер находится опросом /primary (200 только у мастера)
out="$(rest find-leader.sh)"
grep -q 'Leader = s2a' <<<"$out" || { echo "❌ find-leader не нашёл s2a: $out"; exit 1; }
grep -q 'source: patroni /primary == 200' <<<"$out" || { echo "❌ find-leader не через /primary: $out"; exit 1; }
echo "  find-leader: Leader = s2a (fallback-цепочка дошла до /primary) ✓"

# get-role: мастер rc=0, реплика rc=1 (коды — контракт для скриптов мониторинга)
out="$(rest get-role.sh s2a)"
grep -q 's2a: MASTER (leader)' <<<"$out" || { echo "❌ get-role s2a: $out"; exit 1; }
rc=0; out="$(rest get-role.sh s2b)" || rc=$?
[ "$rc" = 1 ] || { echo "❌ get-role s2b: ждали rc=1 (REPLICA), получили $rc"; exit 1; }
grep -q 's2b: REPLICA' <<<"$out" || { echo "❌ get-role s2b: $out"; exit 1; }
echo "  get-role: s2a=MASTER (rc 0), s2b=REPLICA (rc 1) ✓"

# health: стендовой etcd ОДИН — кворума 2/3 нет, health честно ругается (ровно
# 1 проблема); лидер один, API отвечают. В проде etcd x3 — эта ветка зелёная.
rc=0; out="$(rest health.sh)" || rc=$?
[ "$rc" = 1 ] || { echo "❌ health: ждали rc=1 (одиночный etcd без кворума), получили $rc"; exit 1; }
grep -q 'etcd: кворума НЕТ' <<<"$out" || { echo "❌ health не заметил одиночный etcd: $out"; exit 1; }
grep -q 'лидер: ровно один (s2a)' <<<"$out" || { echo "❌ health: лидер не один: $out"; exit 1; }
grep -q 'RESULT: PROBLEMS (1)' <<<"$out" || { echo "❌ health: проблем не одна? $out"; exit 1; }
echo "  health: ровно 1 проблема — кворум etcd (стенд одиночный — честно); лидер/API ок ✓"

# cluster-state: сводка не падает, лидер и etcd-health в выводе (JSON-ролей
# сайдкар не отдаёт — колонки ролей пусты, ограничение эмуляции; patronictl-
# блок отрабатывает graceful — docker на ops-машине не ставится)
out="$(rest cluster-state.sh)"
grep -q 'Leader = s2a' <<<"$out" || { echo "❌ cluster-state без лидера: $out"; exit 1; }
grep -q 'is healthy' <<<"$out" || { echo "❌ cluster-state: etcd health не показан: $out"; exit 1; }
echo "  cluster-state: лидер + etcd health в сводке ✓"

# ═══ Act 3: restore-system.sh (P22) — plan + run на живой системе ══════════════
echo ">>> Act 3: restore-system.sh plan/run (кластер gamma, всё живо)"
out="$(ops restore-system.sh --cluster gamma plan 2>&1)"
grep -q 'etcd: доступен' <<<"$out" || { echo "❌ plan: etcd не увиден: $out"; exit 1; }
grep -q 'инициализирован (N=4' <<<"$out" || { echo "❌ plan: кластер не увиден: $out"; exit 1; }
grep -q "шард 's1': доступен" <<<"$out" || { echo "❌ plan: s1 недоступен: $out"; exit 1; }
grep -q "шард 's2': доступен" <<<"$out" || { echo "❌ plan: s2 недоступен: $out"; exit 1; }
echo "  plan: etcd доступен, кластер инициализирован, оба шарда доступны ✓"

ops restore-system.sh --cluster gamma run 2>&1 | tee logs/76-run.log | grep -v "Container\|Creating\|Created" | sed 's/^/  /'
grep -q 'restore не требуется' logs/76-run.log || { echo "❌ run: живой etcd не должен ресторить"; exit 1; }
grep -q 'карта согласована — восстановление завершено' logs/76-run.log \
  || { echo "❌ run: не дошёл до конца"; exit 1; }
[ "$(ect get /clusters/gamma/buckets/routing/bucket_0 --print-value-only)" = "s2" ] \
  || { echo "❌ run изменил routing (не должен)"; exit 1; }
echo "  run: шаги 1–3 пройдены, verify зелёный, routing не тронут ✓"

# ═══ Act 4: потеря контрол-плейна — диагностика и отказ-путь ═══════════════════
echo ">>> Act 4: etcd остановлен — plan указывает шаг 1; run --snapshot честно завершается кодом 3"
ops restore-cluster.sh --cluster gamma snapshot check76 >/dev/null 2>&1
snap="$(ls -t snapshots/snap-gamma-*check76.snapshot | head -1)"
[ -n "$snap" ] || { echo "❌ снапшот check76 не снялся"; exit 1; }
echo "  снапшот на случай: $(basename "$snap")"

docker compose stop etcd >/dev/null 2>&1
out="$(ops restore-system.sh --cluster gamma plan 2>&1)"
grep -q 'etcd НЕдоступен' <<<"$out" || { echo "❌ plan не заметил потерю etcd: $out"; exit 1; }
echo "  plan на остановленном etcd: «etcd НЕдоступен — шаг 1: поднять слой + restore» ✓"

# run --snapshot: делегат restore-cluster restore на ops-машине БЕЗ docker
# печатает прод-процедуру и не восстанавливает — run завершается кодом 3
# (в проде: подними etcd по процедуре и повтори run; сам физический restore
# docker-автоматикой с хоста стенда проверен чеком 74)
rc=0; ops restore-system.sh --cluster gamma run --snapshot "/snapshots/$(basename "$snap")" --yes \
      >logs/76-run-snap.log 2>&1 || rc=$?
[ "$rc" = 3 ] || { echo "❌ run --snapshot: ждали rc=3, получили $rc"; cat logs/76-run-snap.log; exit 1; }
grep -q 'процедура восстановления etcd' logs/76-run-snap.log \
  || { echo "❌ делегат не напечатал прод-процедуру"; cat logs/76-run-snap.log; exit 1; }
grep -q 'всё ещё недоступен' logs/76-run-snap.log \
  || { echo "❌ run не отказался после невосстановления"; cat logs/76-run-snap.log; exit 1; }
echo "  run --snapshot: делегирование → прод-процедура → честный отказ (rc=3) ✓"

docker compose start etcd >/dev/null 2>&1
ok=""
for _ in $(seq 1 30); do
  docker exec etcd etcdctl --endpoints=http://localhost:2379 endpoint health >/dev/null 2>&1 && { ok=1; break; }
  sleep 1
done
[ -n "$ok" ] || { echo "❌ etcd не вернулся после start"; exit 1; }

ops restore-system.sh --cluster gamma run 2>&1 | tee logs/76-run-after.log | grep -v "Container\|Creating\|Created" | sed 's/^/  /'
grep -q 'карта согласована — восстановление завершено' logs/76-run-after.log \
  || { echo "❌ повторный run не дошёл до конца"; exit 1; }
echo "  etcd возвращён → run довёл восстановление до зелёного verify ✓"

echo "✓ ops-скрипты на стенде: REST-скрипты работают через сайдкары (эмуляция"
echo "  Patroni API); restore-system.sh (P22): plan/run на живой системе, диагностика"
echo "  потери etcd, отказ-путь делегирования без docker и доведение после возврата"
