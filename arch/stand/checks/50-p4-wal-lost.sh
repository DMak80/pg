#!/usr/bin/env bash
# P4: слот «застрявшего переезда» держит WAL → max_slot_wal_keep_size
# инвалидирует СЛОТ (умирает переезд), а не диск шарда.
#
# Нюанс, выявленный на стенде: НЕАКТИВНЫЙ слот при плавной записи не
# инвалидируется — checkpoint'ы подтягивают его restart_lsn. Лимит ловит
# реальный сценарий: слот АКТИВЕН (walsender жив), а потребитель не
# подтверждает применение — его эмулирует slowconsumer.pl (читает поток,
# отвечает на keepalive стоячим feedback'ом: TCP жив, confirmed_flush стоит;
# «не читать вовсе» нельзя — walsender зависает в ClientWrite и блокирует
# CHECKPOINT, который инвалидирует слот только после освобождения walsender'а).
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p logs

q()  { docker exec -i s1b psql -U postgres -d postgres -qAt -v ON_ERROR_STOP=1 "$@"; }
qe() { docker exec -i s1b psql -U postgres -d postgres -qAt -v ON_ERROR_STOP=0 "$@"; }

# Arrange: таблица с несжимаемыми данными (md5-цепочки случайных чисел —
# TOAST их не сжимает, в отличие от repeat(md5(x),400)), публикация, слот
q -c "DROP TABLE IF EXISTS walgen;" >/dev/null
q -c "CREATE TABLE walgen(id int, pad text);" >/dev/null
q -c "DROP PUBLICATION IF EXISTS pub_p4;" >/dev/null
q -c "CREATE PUBLICATION pub_p4 FOR TABLE walgen;" >/dev/null
q -c "SELECT pg_drop_replication_slot('stuck_move');" >/dev/null 2>&1 || true
q -c "SELECT * FROM pg_create_logical_replication_slot('stuck_move', 'pgoutput');" >/dev/null
# стоячая позиция для feedback'а потребителя: confirmed слота на момент создания
SS_LSN="$(q -c "select confirmed_flush_lsn from pg_replication_slots where slot_name='stuck_move'")"
docker exec s1b pkill -f slowconsumer 2>/dev/null || true
docker cp "$PWD/slowconsumer.pl" s1b:/tmp/slowconsumer.pl >/dev/null
docker exec -d s1b sh -c "perl /tmp/slowconsumer.pl stuck_move pub_p4 $SS_LSN > /tmp/sc.log 2>&1"
for i in $(seq 1 15); do
  a="$(q -c "select active from pg_replication_slots where slot_name='stuck_move'")"
  [ "$a" = "t" ] && break; sleep 1
done
echo "— slot created, the 'stuck subscriber' connected (active=$a, no confirmations):"
q -c "select slot_name, active, wal_status from pg_replication_slots where slot_name='stuck_move';"
[ "$a" = "t" ] || { echo "❌ the consumer did not connect: $(docker exec s1b cat /tmp/sc.log 2>/dev/null)"; exit 1; }

# Act: пачки несжимаемого WAL (~28МБ) + checkpoint → ждём wal_status='lost'
st=""
for i in 1 2 3 4 5 6; do
  q -c "INSERT INTO walgen SELECT 600000+$i*10000+g, s.pad
        FROM generate_series(1,8000) g,
             LATERAL (SELECT string_agg(md5(random()::text),'') AS pad FROM generate_series(g, g+79)) s;" >/dev/null
  q -c "CHECKPOINT;" >/dev/null
  st="$(q -c "select wal_status from pg_replication_slots where slot_name='stuck_move'")"
  echo "  iter $i: wal_status=$st"
  [ "$st" = "lost" ] && break
  sleep 1
done

# Assert
echo "— final slot state:"
q -c "select slot_name, active, wal_status, safe_wal_size from pg_replication_slots where slot_name='stuck_move';"
[ "$st" = "lost" ] || { echo "❌ the slot was not invalidated (wal_status=$st)"; exit 1; }
echo "  ✓ wal_status='lost': the MOVE died, not the shard"
echo "— the shard is alive: writes and reads work"
q -c "CREATE TABLE p4_alive(id int); INSERT INTO p4_alive VALUES (1); SELECT count(*) FROM p4_alive; DROP TABLE p4_alive;"
echo "— cleanup (= move-bucket.sh rollback): dropping the slot and artifacts, WAL is released"
docker exec s1b pkill -f slowconsumer 2>/dev/null || true
q -c "SELECT pg_drop_replication_slot('stuck_move');" >/dev/null
q -c "DROP TABLE walgen; DROP PUBLICATION pub_p4;" >/dev/null
echo "✓ P4 verified: WAL blast-radius isolation works"
