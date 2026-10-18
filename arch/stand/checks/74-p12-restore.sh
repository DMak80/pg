#!/usr/bin/env bash
# P12: потеря/restore etcd-контрол-плейна + обязательные снапшоты точек
# переезда. Самодостаточен на собственном кластере gamma (N=4, своя БД).
# Фазы:
#   Arrange: init gamma → Act 1: снапшот «до» (restore-cluster.sh snapshot)
#   Act 2: настоящий move bucket_0 s1→s2 + finalize → снапшоты ТОЧЕК
#          переезда появились (move-start и flip-…): их снимает сам mover
#   Act 3 (P9): etcd остановлен — дата-плейн жив (hap1/hap2 отвечают:
#          hasync держит последние применённые адреса — fail-open)
#   Act 4 (потеря): data-dir etcd уничтожен, поднят ПУСТОЙ etcd →
#          контрол-плейн пуст (карта бакетов потеряна, данные на местах)
#   Act 5 (restore): восстановление из УСТАРЕВШЕГО (до-flip) снапшота —
#          restore-cluster.sh restore с хоста (docker-автоматика)
#   Act 6 (verify → heal → verify): verify ловит расхождение (routing=s1
#          из снапшота, схема после переезда на s2) → heal чинит с журналом
#          /clusters/gamma/heals/bucket_0 → verify зелёный 4/4
# Предусловие: после 72-го (s1b мастер s1; s2a мастер s2 + s2b sync-standby).
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p logs snapshots

# --no-deps: opsbox зависит от etcd, а Act 3 проверяет живость БЕЗ etcd —
# без флага compose run молча поднял бы его
ops() { local s="$1"; shift; docker compose run --rm -T --no-deps opsbox bash "/arch/scripts/$s" "$@"; }
h1()  { docker compose run --rm -T --no-deps opsbox psql -X -qAt -v ON_ERROR_STOP=1 "host=hap1 port=5432 dbname=postgres user=postgres" "$@"; }
h2()  { docker compose run --rm -T --no-deps opsbox psql -X -qAt -v ON_ERROR_STOP=1 "host=hap2 port=5432 dbname=postgres user=postgres" "$@"; }
g1()  { docker compose run --rm -T --no-deps opsbox psql -X -qAt -v ON_ERROR_STOP=1 "host=hap1 port=5432 dbname=gamma user=postgres" "$@"; }
ect() { docker exec etcd etcdctl --endpoints=http://localhost:2379 "$@"; }

# ═══ Arrange 0: предусловие — состояние после 72-го ═══════════════════════════
[ "$(h2 -c 'select pg_is_in_recovery()' 2>/dev/null)" = "f" ] \
  || { echo "❌ hap2 does not route to master s2a — run after check 72"; exit 1; }
[ "$(docker exec s2b psql -U postgres -tAc 'select pg_is_in_recovery()' 2>/dev/null)" = "t" ] \
  || { echo "❌ s2b is not a replica — run after check 72"; exit 1; }
[ "$(ect get /clusters/alpha/buckets/routing/bucket_0 --print-value-only 2>/dev/null)" = "s2" ] \
  || { echo "❌ alpha is not in the post-72 state — run after check 72"; exit 1; }
echo "  preconditions: s2a master + s2b replica, check 72 passed ✓"

# ═══ Arrange 1: чистка хвостов прошлого прогона ════════════════════════════════
echo ">>> Arrange: cleanup (the gamma cluster, the gamma DB, old gamma snapshots)"
ect del /clusters/gamma --prefix >/dev/null 2>&1 || true
for h in h1 h2; do
  $h -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='gamma'" >/dev/null 2>&1 || true
  $h -c "DROP DATABASE IF EXISTS gamma;" >/dev/null 2>&1 || true
  $h -c "CREATE DATABASE gamma;" >/dev/null
done
rm -f snapshots/snap-gamma-*.snapshot

# ═══ Act 1: init gamma + снапшот «до» ══════════════════════════════════════════
echo ">>> Act 1: init-cluster.sh --cluster gamma (N=4, dbname=gamma) + a pre-flip snapshot"
ops init-cluster.sh --cluster gamma --buckets 4 --dbname gamma --replicas 1 --yes \
     --shard "s1=host=hap1 port=5432 dbname=gamma user=postgres" \
     --shard "s2=host=hap2 port=5432 dbname=gamma user=postgres" \
     2>&1 | grep -v "Container\|Creating\|Created" | sed 's/^/  /'
[ "$(ect get /clusters/gamma/buckets/routing/bucket_0 --print-value-only)" = "s1" ] \
  || { echo "❌ routing bucket_0 != s1 after init"; exit 1; }
ops restore-cluster.sh --cluster gamma snapshot pre-move 2>&1 | grep -v "Container\|Creating\|Created" | sed 's/^/  /'
snap_pre="$(ls -t snapshots/snap-gamma-*-pre-move.snapshot | head -1)"
[ -n "$snap_pre" ] || { echo "❌ the pre-move snapshot did not appear in snapshots/"; exit 1; }
echo "  the 'before' snapshot: $(basename "$snap_pre")"

# ═══ Act 2: move + finalize — снапшоты ТОЧЕК переезда снимает сам mover ════════
echo ">>> Act 2: move bucket_0 s1→s2 (the mover must take the P12 move-point snapshots)"
ops move-bucket.sh --cluster gamma move bucket_0 --to s2 --yes --skip-reverse \
     2>&1 | tee logs/74-move.log | grep -v "Container\|Creating\|Created" | sed 's/^/  /'
[ "$(ect get /clusters/gamma/buckets/routing/bucket_0 --print-value-only)" = "s2" ] \
  || { echo "❌ routing bucket_0 != s2 after move"; exit 1; }
# Assert 2a: снапшот точки «после начала» (SYNCING)
snap_start="$(ls -t snapshots/snap-gamma-*-move-bucket_0-start.snapshot 2>/dev/null | head -1 || true)"
[ -n "$snap_start" ] || { echo "❌ the move-bucket_0-start snapshot was not taken (P12: move point)"; exit 1; }
# Assert 2b: снапшот точки «после flip»
snap_flip="$(ls -t snapshots/snap-gamma-*-flip-bucket_0-s2.snapshot 2>/dev/null | head -1 || true)"
[ -n "$snap_flip" ] || { echo "❌ the flip-bucket_0-s2 snapshot was not taken (P12: move point)"; exit 1; }
echo "  move points snapshotted: $(basename "$snap_start"), $(basename "$snap_flip") ✓"
ops move-bucket.sh --cluster gamma finalize bucket_0 --old-shard s1 --yes \
     2>&1 | grep -v "Container\|Creating\|Created" | sed 's/^/  /'
[ "$(g1 -c "SELECT to_regnamespace('bucket_0') IS NOT NULL")" = "f" ] \
  || { echo "❌ the bucket_0 schema remains on s1 after finalize"; exit 1; }
echo "  bucket_0: the schema only on s2 (the owner), the s1 copy removed ✓"

# ═══ Act 3 (P9): etcd остановлен — дата-плейн жив ══════════════════════════════
echo ">>> Act 3 (P9): docker stop etcd → hap1/hap2 keep serving"
docker compose stop etcd >/dev/null 2>&1
[ "$(h1 -c 'SELECT 1' 2>/dev/null)" = "1" ] || { echo "❌ hap1 died with etcd stopped (fail-open?)"; docker compose start etcd >/dev/null 2>&1 || true; exit 1; }
[ "$(h2 -c 'SELECT 1' 2>/dev/null)" = "1" ] || { echo "❌ hap2 died with etcd stopped (fail-open?)"; docker compose start etcd >/dev/null 2>&1 || true; exit 1; }
echo "  writes via hap1/hap2 alive (hasync keeps the last addresses — fail-open, P9) ✓"

# ═══ Act 4 (потеря): data-dir уничтожен, etcd поднят ПУСТЫМ ════════════════════
echo ">>> Act 4: destroying the etcd data-dir → an EMPTY etcd (the control plane lost)"
docker compose rm -sf etcd >/dev/null 2>&1 || true
docker volume rm pgstand_etcd-data >/dev/null
docker compose up -d etcd >/dev/null 2>&1
ok=""
for _ in $(seq 1 30); do
  docker exec etcd etcdctl --endpoints=http://localhost:2379 endpoint health >/dev/null 2>&1 && { ok=1; break; }
  sleep 1
done
[ -n "$ok" ] || { echo "❌ the empty etcd did not come up"; exit 1; }
[ -z "$(ect get /clusters/gamma --prefix --print-value-only 2>/dev/null)" ] \
  || { echo "❌ the gamma control plane is not empty after the loss"; exit 1; }
echo "  etcd is alive and EMPTY: no /clusters/* — the map is lost, the data is on the shards ✓"

# ═══ Act 5 (restore): восстановление из УСТАРЕВШЕГО (до-flip) снапшота ═════════
echo ">>> Act 5: restore-cluster.sh restore <pre-move> (from the host, docker automation)"
# BUCKETS_ENV: скрипт source'ит buckets-common.sh, которому нужен конфиг; с хоста
# передаём стендовую инкарнацию (в проде будет configs/buckets/buckets.env)
BUCKETS_ENV="$PWD/buckets.stand.env" bash ../scripts/restore-cluster.sh restore "$snap_pre" --yes 2>&1 | tee logs/74-restore.log | sed 's/^/  /'
[ "$(ect get /clusters/gamma/buckets/routing/bucket_0 --print-value-only)" = "s1" ] \
  || { echo "❌ restore did not bring back the stale routing (expected s1 from the snapshot)"; exit 1; }
# соседние кластеры вернулись к моменту снапшота (alpha между снапшотом и потерей не менялась)
[ "$(ect get /clusters/alpha/buckets/routing/bucket_0 --print-value-only)" = "s2" ] \
  || { echo "❌ the neighbor alpha was not restored from the shared snapshot"; exit 1; }
# сайдкары: их lease умер вместе со старым etcd — перерегистрировать (новый lease)
for hc in hc1b hc2a hc2b; do docker restart "$hc" >/dev/null 2>&1 || true; done
ok=""
for _ in $(seq 1 60); do
  [ "$(h1 -c 'SELECT 1' 2>/dev/null)" = "1" ] && [ "$(h2 -c 'SELECT 1' 2>/dev/null)" = "1" ] && { ok=1; break; }
  sleep 2
done
[ -n "$ok" ] || { echo "❌ hap1/hap2 did not come back after restore (hasync/sidecars?)"; exit 1; }
echo "  restore brought the map back to the snapshot moment: gamma routing bucket_0=s1 (STALE — the flip happened), haps alive ✓"

# ═══ Act 6: verify → heal → verify ════════════════════════════════════════════
echo ">>> Act 6a: verify MUST catch the mismatch (routing=s1, the schema on s2)"
if ops restore-cluster.sh --cluster gamma verify >logs/74-verify1.log 2>&1; then
  echo "❌ verify did not notice the stale routing"; exit 1
fi
grep -q "bucket_0: routing='s1', no schema there — the schema is on 's2'" logs/74-verify1.log \
  || { echo "❌ verify did not produce the expected diagnosis:"; cat logs/74-verify1.log; exit 1; }
grep "bucket_0:" logs/74-verify1.log | sed 's/^/  /'

echo ">>> Act 6b: heal --yes — auto-fixing the unambiguous mismatch"
ops restore-cluster.sh --cluster gamma heal --yes 2>&1 | tee logs/74-heal.log | grep -v "Container\|Creating\|Created" | sed 's/^/  /'
[ "$(ect get /clusters/gamma/buckets/routing/bucket_0 --print-value-only)" = "s2" ] \
  || { echo "❌ heal did not move routing bucket_0 to s2"; exit 1; }
hj="$(ect get /clusters/gamma/heals/bucket_0 --print-value-only)"
echo "$hj" | jq -e '.was == "s1" and .now == "s2" and .reason == "restore-heal"' >/dev/null \
  || { echo "❌ wrong heal journal: $hj"; exit 1; }
echo "  routing bucket_0 → s2; the journal before the manipulation: was=s1 now=s2 ✓"

echo ">>> Act 6c: verify after heal — green"
ops restore-cluster.sh --cluster gamma verify 2>&1 | grep -v "Container\|Creating\|Created" | sed 's/^/  /'
[ "$(ect get /clusters/gamma/buckets/routing/bucket_0 --print-value-only)" = "s2" ] \
  || { echo "❌ routing changed after verify (it must not)"; exit 1; }

echo "✓ P12: the mover takes move-point snapshots (SYNCING + flip); losing etcd does not touch"
echo "  the data plane (P9 fail-open); restore from a stale snapshot + verify → heal (a journal"
echo "  under /heals/*, unambiguous cases only) → verify 4/4; the shared snapshot restored the neighbor clusters"
