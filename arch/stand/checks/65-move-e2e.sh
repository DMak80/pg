#!/usr/bin/env bash
# E2E переезд НАСТОЯЩИМ скриптом move-bucket.sh (запуск из ops-бокса):
# create-bucket (etcd-регистрация) → move с P1-заморозкой/P6 sequence→sequence/
# P5-сверкой инвентаря/атомарным etcd-flip → P1-призрак → запись на новом
# владельце → rollback через обратную подписку → повторный move → finalize.
# Плюс негатив: move на шард без sync-standby отказывается (P8-предусловие).
# Шард1: мастер после 30-й проверки — s1b (через hap1, s1a не поднимать!).
# Шард2: s2a+s2b (через hap2).
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p logs

ops() { local s="$1"; shift; docker compose run --rm -T opsbox bash "/arch/scripts/$s" "$@"; }
h1()  { docker compose run --rm -T opsbox psql -X -qAt -v ON_ERROR_STOP=1 "host=hap1 port=5432 dbname=postgres user=postgres" "$@"; }
h1a() { docker compose run --rm -T opsbox psql -X -qAt -v ON_ERROR_STOP=1 "host=hap1 port=5432 dbname=postgres user=app_role" "$@"; }
h2()  { docker compose run --rm -T opsbox psql -X -qAt -v ON_ERROR_STOP=1 "host=hap2 port=5432 dbname=postgres user=postgres" "$@"; }
h2a() { docker compose run --rm -T opsbox psql -X -qAt -v ON_ERROR_STOP=1 "host=hap2 port=5432 dbname=postgres user=app_role" "$@"; }
ect() { docker exec etcd etcdctl --endpoints=http://localhost:2379 "$@"; }

# ═══ Arrange: bucket_45 на шарде s1 через create-bucket.sh ═══════════════════
echo ">>> Arrange: чистка хвостов + create-bucket.sh bucket_45 --shard s1"
ect del /clusters/legacy/buckets/routing/bucket_45 >/dev/null
ect del /clusters/legacy/buckets/status/bucket_45 >/dev/null
h2 -c "DROP SUBSCRIPTION IF EXISTS sub_bucket_45;" >/dev/null 2>&1 || true
h1 -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'sub_bucket_45%';" >/dev/null 2>&1 || true
h1 -c "DROP SUBSCRIPTION IF EXISTS sub_bucket_45_rb; DROP PUBLICATION IF EXISTS pub_bucket_45; DROP PUBLICATION IF EXISTS pub_bucket_45_rb;" >/dev/null 2>&1 || true
h1 -c "DROP SCHEMA IF EXISTS bucket_45 CASCADE;" >/dev/null
h2 -c "DROP SCHEMA IF EXISTS bucket_45 CASCADE;" >/dev/null

ops create-bucket.sh bucket_45 --shard s1 2>&1 | grep -v "Container\|Creating\|Created" | sed 's/^/  /'
[ "$(ect get /clusters/legacy/buckets/routing/bucket_45 --print-value-only)" = "s1" ] || { echo "❌ create-bucket не зарегистрировал в etcd"; exit 1; }
# данные: serial + standalone sequence с «сожжёнными» номерами (кейс P6)
h1 -c "CREATE TABLE bucket_45.orders(id serial PRIMARY KEY, note text);
       CREATE SEQUENCE bucket_45.seq_ticket START 1;
       CREATE TABLE bucket_45.tickets(ticket_no bigint UNIQUE NOT NULL DEFAULT nextval('bucket_45.seq_ticket'), note text);
       INSERT INTO bucket_45.orders(note) SELECT 'row'||g FROM generate_series(1,30) g;
       INSERT INTO bucket_45.tickets(ticket_no) VALUES (1),(2),(3),(4),(5),(100),(110);
       SELECT setval('bucket_45.seq_ticket', 5000, true);" >/dev/null
base_orders="$(h1 -c 'SELECT count(*) FROM bucket_45.orders')"
echo "  s1: orders=$base_orders  seq_ticket.last=$(h1 -c 'SELECT last_value FROM bucket_45.seq_ticket')"

# ═══ Act 1: move s1 → s2 ═════════════════════════════════════════════════════
echo ">>> Act 1: move-bucket.sh move bucket_45 --to s2 --yes"
ops move-bucket.sh move bucket_45 --to s2 --yes 2>&1 | tee logs/65-move1.log | grep -v "Container\|Creating\|Created" | sed 's/^/  /'

# ═══ Assert 1: etcd-атомарность + данные + P6 + P1-призрак + запись ═══════════
echo ">>> Assert 1: атомарный flip (routing=s2, статус-ключ удалён)"
[ "$(ect get /clusters/legacy/buckets/routing/bucket_45 --print-value-only)" = "s2" ] || { echo "❌ routing != s2"; exit 1; }
[ -z "$(ect get /clusters/legacy/buckets/status/bucket_45 --print-value-only)" ] || { echo "❌ статус-ключ не удалён"; exit 1; }
grep -q "атомарный flip" logs/65-move1.log || { echo "❌ в логе нет атомарного flip"; exit 1; }

echo ">>> Assert 1: копия на s2 + P6 (следующий ticket > 5000) + обратная подписка"
[ "$(h2 -c 'SELECT count(*) FROM bucket_45.orders')" = "$base_orders" ] || { echo "❌ orders не доехали на s2"; exit 1; }
nxt="$(h2 -c "SELECT CASE WHEN is_called THEN last_value+1 ELSE last_value END FROM bucket_45.seq_ticket")"
[ "$nxt" -gt 5000 ] || { echo "❌ P6: следующее значение seq_ticket на s2 = $nxt (<= 5000)"; exit 1; }
echo "  s2: orders=$(h2 -c 'SELECT count(*) FROM bucket_45.orders')  следующий ticket=$nxt"
[ "$(h1 -c "SELECT count(*) FROM pg_subscription WHERE subname='sub_bucket_45_rb'")" = "1" ] || { echo "❌ обратной подписки нет на s1"; exit 1; }

echo ">>> Assert 1: P1-призрак — app_role НЕ пишет на старый шард"
if out="$(h1a -c "INSERT INTO bucket_45.orders(note) VALUES ('ghost')" 2>&1)"; then
  echo "❌ призрак записался на s1!"; exit 1
fi
echo "$out" | grep -q "permission denied" || { echo "❌ неожиданная ошибка призрака: $out"; exit 1; }

echo ">>> Assert 1: app_role пишет и делает DDL на новом владельце (гранты приёмника)"
h2a -c "INSERT INTO bucket_45.orders(note) VALUES ('on-new-owner')" \
  || { echo "❌ app_role не пишет на s2"; exit 1; }
h2a -c "CREATE TABLE bucket_45.tmp_ddl(id int);" \
  || { echo "❌ app_role не делает DDL на s2 (P5)"; exit 1; }
h2 -c "DROP TABLE bucket_45.tmp_ddl;" >/dev/null

# ═══ Act 2: rollback через обратную подписку ═════════════════════════════════
echo ">>> Act 2: move-bucket.sh rollback bucket_45 --yes"
ops move-bucket.sh rollback bucket_45 --yes 2>&1 | tee logs/65-rollback.log | grep -v "Container\|Creating\|Created" | sed 's/^/  /'

echo ">>> Assert 2: владелец снова s1, разморожен, артефакты обратной репликации срезаны"
[ "$(ect get /clusters/legacy/buckets/routing/bucket_45 --print-value-only)" = "s1" ] || { echo "❌ routing != s1 после rollback"; exit 1; }
[ -z "$(ect get /clusters/legacy/buckets/status/bucket_45 --print-value-only)" ] || { echo "❌ статус-ключ не удалён при rollback"; exit 1; }
h1a -c "INSERT INTO bucket_45.orders(note) VALUES ('after-rollback')" \
  || { echo "❌ s1 не разморожен после rollback (P1)"; exit 1; }
[ "$(h1 -c "SELECT count(*) FROM pg_subscription WHERE subname='sub_bucket_45_rb'")" = "0" ] || { echo "❌ sub_rb не срезана"; exit 1; }
[ "$(h2 -c "SELECT count(*) FROM pg_publication WHERE pubname='pub_bucket_45_rb'")" = "0" ] || { echo "❌ pub_rb не срезана"; exit 1; }
echo "  s1: orders=$(h1 -c 'SELECT count(*) FROM bucket_45.orders') (данные вернулись через обратную подписку)"

# ═══ Act 2.5: finalize остатка отката на s2 ═══════════════════════════════════
# rollback оставляет на s2 схему-копию на момент отката (данные у владельца) —
# повторный move честно откажется работать поверх остатка без подписки; убираем.
echo ">>> Act 2.5: finalize bucket_45 --old-shard s2 (уборка остатка отката)"
ops move-bucket.sh finalize bucket_45 --old-shard s2 --yes 2>&1 | tee logs/65-finalize-rb.log | grep -v "Container\|Creating\|Created" | sed 's/^/  /'
[ "$(h2 -c "SELECT count(*) FROM pg_namespace WHERE nspname='bucket_45'")" = "0" ] || { echo "❌ схема-остаток отката осталась на s2"; exit 1; }
[ "$(h2 -c "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'sub_bucket_45%'")" = "0" ] || { echo "❌ слоты sub_bucket_45 на s2"; exit 1; }
[ "$(h2 -c "SELECT count(*) FROM pg_publication WHERE pubname LIKE 'pub_bucket_45%'")" = "0" ] || { echo "❌ публикации pub_bucket_45 на s2"; exit 1; }

# ═══ Act 3: повторный move (после отката и уборки всё живо) ═══════════════════
echo ">>> Act 3: повторный move bucket_45 --to s2 --yes"
ops move-bucket.sh move bucket_45 --to s2 --yes 2>&1 | tee logs/65-move2.log | grep -v "Container\|Creating\|Created" | sed 's/^/  /'
[ "$(ect get /clusters/legacy/buckets/routing/bucket_45 --print-value-only)" = "s2" ] || { echo "❌ повторный move не дошёл до flip"; exit 1; }

# ═══ Act 4: finalize — уборка старого шарда ═══════════════════════════════════
echo ">>> Act 4: finalize bucket_45 --old-shard s1"
ops move-bucket.sh finalize bucket_45 --old-shard s1 --yes 2>&1 | tee logs/65-finalize.log | grep -v "Container\|Creating\|Created" | sed 's/^/  /'
[ "$(h1 -c "SELECT count(*) FROM pg_namespace WHERE nspname='bucket_45'")" = "0" ] || { echo "❌ схема-копия осталась на s1"; exit 1; }
[ "$(h1 -c "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'sub_bucket_45%'")" = "0" ] || { echo "❌ слоты sub_bucket_45 на s1"; exit 1; }
[ "$(h1 -c "SELECT count(*) FROM pg_publication WHERE pubname LIKE 'pub_bucket_45%'")" = "0" ] || { echo "❌ публикации pub_bucket_45 на s1"; exit 1; }
h2a -c "INSERT INTO bucket_45.orders(note) VALUES ('after-finalize')" >/dev/null \
  || { echo "❌ владелец s2 не пишет после finalize"; exit 1; }

# ═══ Негатив: P8-предусловие — приёмник без sync-standby ══════════════════════
echo ">>> Негатив: move на s1 (после 30-й у s1b нет реплики) должен отказаться"
ops create-bucket.sh bucket_44 --shard s2 2>&1 | grep -v "Container\|Creating\|Created" | sed 's/^/  /'
if ops move-bucket.sh move bucket_44 --to s1 --yes >logs/65-refuse.log 2>&1; then
  echo "❌ move должен отказаться: у приёмника s1 нет sync-standby (P8)"; exit 1
fi
grep -Eq "synchronous_standby_names|sync-standby" logs/65-refuse.log || { echo "❌ неожиданный отказ:"; cat logs/65-refuse.log; exit 1; }
grep -v "Container\|Creating\|Created" logs/65-refuse.log | sed 's/^/  /'
[ "$(ect get /clusters/legacy/buckets/routing/bucket_44 --print-value-only)" = "s2" ] || { echo "❌ routing изменился при отказе"; exit 1; }
[ -z "$(ect get /clusters/legacy/buckets/status/bucket_44 --print-value-only)" ] || { echo "❌ отказ оставил статус-ключ"; exit 1; }
# уборка негативного бакета
ect del /clusters/legacy/buckets/routing/bucket_44 >/dev/null
h2 -c "DROP SCHEMA IF EXISTS bucket_44 CASCADE;" >/dev/null

echo "✓ скрипты: create→move(атомарный flip)→призрак→rollback→move→finalize зелёные;"
echo "  P8-предусловие (sync-standby у приёмника) отсекает переезд в кластер без реплики"
