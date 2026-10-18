#!/usr/bin/env bash
# P8: failover ПРИЁМНИКА во время SYNCING. Две фазы на одном и том же сценарии.
#
# RED (без защиты): подписка с дефолтным synchronous_commit=off. W1 применяется
#   на мастере приёмника и подтверждается источнику (confirmed_flush убегает),
#   но не доезжает до standby приёмника. Failover: standby повышен — а walsender
#   отдаёт изменения только от confirmed_flush → срез W1 МОЛЧА ПРОПУСКАЕТСЯ.
#   Стрим при этом «здоров» (лаг 0, новые строки доезжают) — дефект невидим
#   для лаг-метрик. Лечится abort (P7) + перезапуском переезда.
# GREEN (защита): настоящий move-bucket.sh — подписка с synchronous_commit=
#   remote_apply: коммит применённой транзакции ждёт replay на standby приёмника
#   (видно как SyncRep-ожидание apply-воркера) → feedback не убегает вперёд →
#   после failover W2 ПЕРЕСЫЛАЕТСЯ и применяется. Mover переживает обрывы
#   (транзиент-толерантные циклы), initial copy большой таблицы рестартует на
#   новом мастере, cutover проходит со сверкой строк и атомарным etcd-flip.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p logs

h1()  { docker compose run --rm -T opsbox psql -X -qAt -v ON_ERROR_STOP=1 "host=hap1 port=5432 dbname=postgres user=postgres" "$@"; }
h1a() { docker compose run --rm -T opsbox psql -X -qAt -v ON_ERROR_STOP=1 "host=hap1 port=5432 dbname=postgres user=app_role" "$@"; }
h2()  { docker compose run --rm -T opsbox psql -X -qAt -v ON_ERROR_STOP=1 "host=hap2 port=5432 dbname=postgres user=postgres" "$@"; }
h2a() { docker compose run --rm -T opsbox psql -X -qAt -v ON_ERROR_STOP=1 "host=hap2 port=5432 dbname=postgres user=app_role" "$@"; }
ops() { local s="$1"; shift; docker compose run --rm -T opsbox bash "/arch/scripts/$s" "$@"; }
ect() { docker exec etcd etcdctl --endpoints=http://localhost:2379 "$@"; }
ip()  { docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$1"; }

# ═══ RESET: канонический шард2 (s2a мастер, s2b реплика) ══════════════════════
# Чек топологически специфичен (RED: стоп standby → стоп мастер → promote standby;
# GREEN: ребейз s2a репликой s2b), поэтому приводит шард2 к исходной паре сам:
# compose rm стирает PGDATA (без томов) → s2a initdb-мастер, s2b клонируется.
echo ">>> RESET: shard2 → the canonical pair (s2a master, s2b replica)"
docker compose rm -sf hc2b hc2a s2b s2a >/dev/null 2>&1 || true
docker compose up -d s2a hc2a s2b hc2b >/dev/null 2>&1
until docker exec s2a pg_isready -U postgres -q 2>/dev/null; do sleep 2; done
docker exec s2a bash -c \
  'grep -q "host replication all all trust" $PGDATA/pg_hba.conf || echo "host replication all all trust" >> $PGDATA/pg_hba.conf
   psql -U postgres -d postgres -qtAc "select pg_reload_conf()" >/dev/null'
until docker exec s2b pg_isready -U postgres -q 2>/dev/null; do sleep 2; done
until [ "$(docker exec s2b psql -U postgres -tAc 'select pg_is_in_recovery()' 2>/dev/null)" = "t" ]; do sleep 1; done
until [ "$(h2 -c 'select pg_is_in_recovery()' 2>/dev/null)" = "f" ]; do sleep 1; done
echo "  s2a master, s2b replica (fresh clones)"

# ═══ RED: тихий пропуск без защиты ════════════════════════════════════════════
echo ">>> RED / Arrange: a manual subscription (synccommit default = off), target master s2a"
ect del /clusters/legacy/buckets/routing/bucket_46 >/dev/null
ect del /clusters/legacy/buckets/status/bucket_46 >/dev/null
h2 -c "DROP SUBSCRIPTION IF EXISTS sub_bucket_46;" >/dev/null 2>&1 || true
h1 -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'sub_bucket_46%';" >/dev/null 2>&1 || true
h1 -c "DROP PUBLICATION IF EXISTS pub_bucket_46;" >/dev/null 2>&1 || true
h1 -c "DROP SCHEMA IF EXISTS bucket_46 CASCADE;" >/dev/null
h2 -c "DROP SCHEMA IF EXISTS bucket_46 CASCADE;" >/dev/null
h1 -c "CREATE SCHEMA bucket_46;
       CREATE TABLE bucket_46.events(id serial PRIMARY KEY, note text);
       INSERT INTO bucket_46.events(note) SELECT 'e'||g FROM generate_series(1,50) g;
       CREATE PUBLICATION pub_bucket_46 FOR TABLES IN SCHEMA bucket_46;" >/dev/null
h2 -c "CREATE SCHEMA bucket_46;
       CREATE TABLE bucket_46.events(id serial PRIMARY KEY, note text);"
h2 -c "CREATE SUBSCRIPTION sub_bucket_46
         CONNECTION 'host=hap1 port=5432 dbname=postgres user=postgres'
         PUBLICATION pub_bucket_46 WITH (copy_data = true, failover = true);"
# «зависший переезд»: routing + протухший статус (mover мёртв — свежесть не мешает abort'у)
ect put /clusters/legacy/buckets/routing/bucket_46 s1 >/dev/null
ect put /clusters/legacy/buckets/status/bucket_46 "{\"state\":\"SYNCING\",\"target\":\"s2\",\"updated_unix\":$(( $(date +%s) - 3600 ))}" >/dev/null
until [ "$(h2 -c "SELECT count(*) FROM pg_subscription_rel r JOIN pg_subscription s ON s.oid=r.srsubid
                  JOIN pg_class c ON c.oid=r.srrelid
                  WHERE s.subname='sub_bucket_46' AND r.srsubstate='r'")" = "1" ]; do sleep 1; done
echo "  the subscription streams: events on s2a = $(h2 -c 'SELECT count(*) FROM bucket_46.events')"

echo ">>> RED / Act: the standby vanishes → W1 is applied on the master and confirmed → the master dies"
docker stop s2b >/dev/null
h1 -c "INSERT INTO bucket_46.events(note) SELECT 'W1-'||g FROM generate_series(1,100) g;" >/dev/null
until [ "$(h2 -c 'SELECT count(*) FROM bucket_46.events')" = "150" ]; do sleep 1; done
sleep 3   # даём feedback-у подтвердиться (confirmed_flush убегает за физрепликацию)
echo "  W1 applied on s2a (=150), s2b is down — W1 exists only in the source WAL"
docker stop s2a >/dev/null
docker start s2b >/dev/null
# сайдкар hc2b не переподключается к новому netns перезапущенной s2b — рестартуем
docker restart hc2b >/dev/null 2>&1 || true
until docker exec s2b pg_isready -U postgres -q; do sleep 1; done
PGD="$(docker exec s2b psql -U postgres -tAc 'show data_directory' | tr -d '[:space:]')"
docker exec -u postgres s2b pg_ctl promote -D "$PGD" >/dev/null
until [ "$(h2 -c 'select pg_is_in_recovery()')" = "f" ]; do sleep 1; done
# промоушен без второй реплики: снять sync-имена, иначе коммиты на s2b висят в SyncRep
docker exec s2b psql -U postgres -d postgres -c "ALTER SYSTEM SET synchronous_standby_names = ''" -c "SELECT pg_reload_conf()" >/dev/null
until [ "$(h2 -c 'select inet_server_addr()')" = "$(ip s2b)" ]; do sleep 1; done
# s2b теперь мастер — патчим его hba (репликация) для будущего пересоздания s2a
docker exec s2b bash -c \
  'grep -q "host replication all all trust" $PGDATA/pg_hba.conf || echo "host replication all all trust" >> $PGDATA/pg_hba.conf
   psql -U postgres -d postgres -qtAc "select pg_reload_conf()" >/dev/null'
echo "  s2b promoted, hap2 → s2b"

echo ">>> RED / Assert: W1 silently skipped; the stream is 'healthy' — the defect is invisible to lag"
w=""
for i in $(seq 1 60); do
  w="$(h2 -c "SELECT count(*) FROM pg_stat_subscription WHERE subname='sub_bucket_46' AND pid IS NOT NULL" 2>/dev/null || true)"
  [ "$w" = "1" ] && break; sleep 1
done
[ "$w" = "1" ] || { echo "❌ the subscription did not reconnect to s2b"; exit 1; }
r="$(h2 -c 'SELECT count(*) FROM bucket_46.events')"
[ "$r" = "50" ] || { echo "❌ unexpected: events on s2b = $r (expected 50 — a silent skip)"; exit 1; }
echo "  events: source=$(h1 -c 'SELECT count(*) FROM bucket_46.events')  target=$r  ← W1 SKIPPED (silently)"
echo "  slot lag at this moment: $(h1 -c "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) FROM pg_replication_slots WHERE slot_name='sub_bucket_46'") bytes"
h1 -c "INSERT INTO bucket_46.events(note) VALUES ('post-failover-row')" >/dev/null
until [ "$(h2 -c 'SELECT count(*) FROM bucket_46.events')" = "51" ]; do sleep 1; done
echo "  the new row arrived (51) — replication is 'healthy', yet hundreds of rows are missing"

echo ">>> RED / Recovery: abort-move.sh (P7) repairs, a fresh copy rebuilds the data"
ops abort-move.sh abort bucket_46 --yes 2>&1 | tee logs/70-red-abort.log | grep -v "Container\|Creating\|Created" | sed 's/^/  /'
[ "$(h1 -c 'SELECT count(*) FROM bucket_46.events')" = "151" ] || { echo "❌ owner data corrupted"; exit 1; }
[ "$(h2 -c "SELECT count(*) FROM pg_subscription WHERE subname='sub_bucket_46'")" = "0" ] || { echo "❌ subscription not dropped"; exit 1; }
[ "$(h2 -c "SELECT count(*) FROM pg_namespace WHERE nspname='bucket_46'")" = "0" ] || { echo "❌ schema copy not dropped"; exit 1; }
[ -z "$(ect get /clusters/legacy/buckets/status/bucket_46 --print-value-only)" ] || { echo "❌ status key not deleted"; exit 1; }

# ═══ GREEN: remote_apply спасает ══════════════════════════════════════════════
echo ">>> GREEN / Arrange: restoring the shard2 pair (s2a re-bases as a replica of s2b)"
docker compose rm -sf hc2a s2a >/dev/null 2>&1
docker compose up -d s2a hc2a >/dev/null
until docker exec s2a pg_isready -U postgres -q 2>/dev/null; do sleep 2; done
until [ "$(docker exec s2a psql -U postgres -tAc 'select pg_is_in_recovery()')" = "t" ]; do sleep 1; done
# standby снова есть → вернуть sync-имена на s2b (P8-предусловие для move)
docker exec s2b psql -U postgres -d postgres -c "ALTER SYSTEM SET synchronous_standby_names = 'FIRST 1 (s2a)'" -c "SELECT pg_reload_conf()" >/dev/null
syncst=""
for i in $(seq 1 60); do
  syncst="$(h2 -c "SELECT sync_state FROM pg_stat_replication WHERE application_name='s2a'")"
  [ "$syncst" = "sync" ] && break; sleep 1
done
[ "$syncst" = "sync" ] || { echo "❌ s2a did not become a sync-standby of s2b (P8 precondition)"; exit 1; }
echo "  s2b ← sync-standby s2a"

echo ">>> GREEN / Arrange: a big table (initial copy window) and starting move in the background"
h1 -c "CREATE TABLE bucket_46.big(id int PRIMARY KEY, pad text);
       INSERT INTO bucket_46.big SELECT g, 'pad-'||g FROM generate_series(1,150000) g;" >/dev/null
( docker compose run --rm -T -e FREEZE_WAIT_SEC=45 -e POLL_INTERVAL_SEC=1 opsbox \
    bash /arch/scripts/move-bucket.sh move bucket_46 --to s2 --yes \
    >logs/70-move-green.log 2>&1 ) &
mv=$!

# ждём: подписка создана и events синхронизирована (big ещё копируется — окно инжекта)
sub=""
for i in $(seq 1 120); do
  sub="$(h2 -c "SELECT r.srsubstate FROM pg_subscription_rel r JOIN pg_subscription s ON s.oid=r.srsubid
                JOIN pg_class c ON c.oid=r.srrelid
                WHERE s.subname='sub_bucket_46' AND c.relname='events'" 2>/dev/null || true)"
  [ "$sub" = "r" ] && break; sleep 1
done
[ "$sub" = "r" ] || { echo "❌ the subscription/GREEN did not reach events streaming"; kill "$mv" 2>/dev/null || true; exit 1; }
[ "$(h2 -c 'SELECT count(*) FROM bucket_46.events')" = "151" ] \
  || { echo "❌ arrange: the events copy is incomplete before the inject"; kill "$mv" 2>/dev/null || true; exit 1; }
echo "  events streaming (151/151), big copying — injecting the target failure"

echo ">>> GREEN / Act: standby replay pause → W2 → the commit hangs in SyncRep → master dies → promote"
r0="$(h2 -c 'SELECT count(*) FROM bucket_46.events')"
docker exec s2a psql -U postgres -d postgres -c 'select pg_wal_replay_pause();' >/dev/null
base_src="$(h1 -c 'SELECT count(*) FROM bucket_46.events')"
h1 -c "INSERT INTO bucket_46.events(note) SELECT 'W2-'||g FROM generate_series(1,100) g;" >/dev/null
# Assert защиты: W2 не подтверждается на мастере приёмника — apply-воркер ждёт SyncRep
# (воркер доходит до коммита не мгновенно — ждём появления SyncRep-ожидания циклом;
# если защита не действует, W2 применится и счётчик приёмника убежит — поймаем это)
w=0
for i in $(seq 1 60); do
  # PG18: apply-воркер — backend_type 'logical replication apply worker'
  w="$(h2 -c "SELECT count(*) FROM pg_stat_activity WHERE backend_type LIKE 'logical replication%' AND wait_event='SyncRep'" 2>/dev/null || true)"
  [ "$w" -ge 1 ] && break
  sleep 1
done
r="$(h2 -c 'SELECT count(*) FROM bucket_46.events')"
echo "  W2 on the source; on s2b events=$r (not grown), apply workers waiting in SyncRep: $w"
[ "$r" = "$r0" ] || { echo "❌ W2 was applied during the replay pause — the protection did not work"; kill "$mv" 2>/dev/null || true; exit 1; }
[ "$w" -ge 1 ] || { echo "❌ the apply worker is not in SyncRep — remote_apply is not in effect"; kill "$mv" 2>/dev/null || true; exit 1; }
docker stop s2b >/dev/null
docker exec s2a psql -U postgres -d postgres -c 'select pg_wal_replay_resume();' >/dev/null
PGD="$(docker exec s2a psql -U postgres -tAc 'show data_directory' | tr -d '[:space:]')"
docker exec -u postgres s2a pg_ctl promote -D "$PGD" >/dev/null
# промоушен без второй реплики: снять sync-имена, иначе коммиты на s2a висят в SyncRep
docker exec s2a psql -U postgres -d postgres -c "ALTER SYSTEM SET synchronous_standby_names = ''" -c "SELECT pg_reload_conf()" >/dev/null
until [ "$(h2 -c 'select inet_server_addr()')" = "$(ip s2a)" ]; do sleep 1; done
echo "  s2a promoted, hap2 → s2a — the subscription and copy continue on the new master"

# Assert: W2 ПЕРЕСЫЛАЕТСЯ (origin < W2, confirmed_flush тоже < W2 — срез не пропущен)
w2ok=""
for i in $(seq 1 120); do
  w2ok="$(h2 -c 'SELECT count(*) FROM bucket_46.events' 2>/dev/null || true)"
  [ "$w2ok" = "$((base_src + 100))" ] && break; sleep 2
done
[ "$w2ok" = "$((base_src + 100))" ] \
  || { echo "❌ W2 did not arrive after failover ($w2ok != $((base_src + 100)))"; kill "$mv" 2>/dev/null || true; exit 1; }
echo "  W2 arrived: events on s2a = $w2ok — NO skip (remote_apply held the feedback back)"

echo ">>> GREEN / Assert: the mover survived the failure and carried the move to the end"
alive=1
for i in $(seq 1 300); do kill -0 "$mv" 2>/dev/null || { alive=0; break; }; sleep 2; done
if [ "$alive" = 1 ]; then echo "❌ move did not finish within the timeout"; kill "$mv" 2>/dev/null || true; exit 1; fi
move_rc=0; wait "$mv" || move_rc=$?
grep -v "Container\|Creating\|Created" logs/70-move-green.log | tail -25 | sed 's/^/  /'
[ "$move_rc" = 0 ] || { echo "❌ move exited with code $move_rc"; exit 1; }
[ "$(ect get /clusters/legacy/buckets/routing/bucket_46 --print-value-only)" = "s2" ] || { echo "❌ routing != s2"; exit 1; }
[ -z "$(ect get /clusters/legacy/buckets/status/bucket_46 --print-value-only)" ] || { echo "❌ status key not deleted"; exit 1; }
grep -q "row counts match" logs/70-move-green.log || { echo "❌ cutover without the row count check (P8 barrier)"; exit 1; }

src_big="$(h1 -c 'SELECT count(*) FROM bucket_46.big')"
dst_big="$(h2 -c 'SELECT count(*) FROM bucket_46.big')"
src_ev="$(h1 -c 'SELECT count(*) FROM bucket_46.events')"
dst_ev="$(h2 -c 'SELECT count(*) FROM bucket_46.events')"
echo "  parity: events $src_ev/$dst_ev  big $src_big/$dst_big"
[ "$src_big" = "$dst_big" ] && [ "$src_ev" = "$dst_ev" ] || { echo "❌ source/target mismatch"; exit 1; }
if out="$(h1a -c "INSERT INTO bucket_46.events(note) VALUES ('ghost')" 2>&1)"; then
  echo "❌ the ghost wrote to the old shard"; exit 1
fi
echo "$out" | grep -q "permission denied" || { echo "❌ unexpected ghost error: $out"; exit 1; }
h2a -c "INSERT INTO bucket_46.events(note) VALUES ('on-new-owner')" \
  || { echo "❌ app_role does not write on the new owner"; exit 1; }

echo ">>> GREEN / Finale: finalize (including orphaned tablesync slots)"
orphan_before="$(h1 -c "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'sub_bucket_46_sync_%'")"
echo "  orphaned sync slots on s1 before finalize: $orphan_before (copy restart after failover)"
ops move-bucket.sh finalize bucket_46 --old-shard s1 --yes 2>&1 | tee logs/70-finalize.log | grep -v "Container\|Creating\|Created" | sed 's/^/  /'
[ "$(h1 -c "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'sub_bucket_46%'")" = "0" ] \
  || { echo "❌ sub_bucket_46 slots remain on s1"; exit 1; }
[ "$(h1 -c "SELECT count(*) FROM pg_namespace WHERE nspname='bucket_46'")" = "0" ] \
  || { echo "❌ the schema copy remains on s1"; exit 1; }

echo "✓ P8 verified: RED — without the protection the change slice is skipped SILENTLY (lag 0, stream alive);"
echo "  GREEN — the subscription's synchronous_commit=remote_apply keeps the feedback behind the physical"
echo "  replication of the target: SyncRep wait, W2 redelivery after failover, parity, row count check, atomic flip"
