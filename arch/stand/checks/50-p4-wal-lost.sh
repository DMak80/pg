#!/usr/bin/env bash
# P4: слот «застрявшего переезда» держит WAL → max_slot_wal_keep_size
# инвалидирует СЛОТ (умирает переезд), а не диск шарда.
#
# Нюанс, выявленный на стенде: НЕАКТИВНЫЙ слот при плавной записи не
# инвалидируется — checkpoint'ы подтягивают его restart_lsn. Лимит ловит
# реальный сценарий: слот АКТИВЕН (walsender жив), а потребитель молчит
# и не подтверждает — его эмулирует slowconsumer.pl.
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
docker exec s1b pkill -f slowconsumer 2>/dev/null || true
docker cp "$PWD/slowconsumer.pl" s1b:/tmp/slowconsumer.pl >/dev/null
docker exec -d s1b sh -c 'perl /tmp/slowconsumer.pl stuck_move pub_p4 > /tmp/sc.log 2>&1'
for i in $(seq 1 15); do
  a="$(q -c "select active from pg_replication_slots where slot_name='stuck_move'")"
  [ "$a" = "t" ] && break; sleep 1
done
echo "— слот создан, «зависший подписчик» подключён (active=$a, подтверждений нет):"
q -c "select slot_name, active, wal_status from pg_replication_slots where slot_name='stuck_move';"
[ "$a" = "t" ] || { echo "❌ потребитель не подключился: $(docker exec s1b cat /tmp/sc.log 2>/dev/null)"; exit 1; }

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
echo "— итоговое состояние слота:"
q -c "select slot_name, active, wal_status, safe_wal_size from pg_replication_slots where slot_name='stuck_move';"
[ "$st" = "lost" ] || { echo "❌ слот не инвалидировался (wal_status=$st)"; exit 1; }
echo "  ✓ wal_status='lost': умер ПЕРЕЕЗД, а не шард"
echo "— шард жив: запись и чтение работают"
q -c "CREATE TABLE p4_alive(id int); INSERT INTO p4_alive VALUES (1); SELECT count(*) FROM p4_alive; DROP TABLE p4_alive;"
echo "— уборка (= move-bucket.sh rollback): срезаем слот и артефакты, WAL освобождается"
docker exec s1b pkill -f slowconsumer 2>/dev/null || true
q -c "SELECT pg_drop_replication_slot('stuck_move');" >/dev/null
q -c "DROP TABLE walgen; DROP PUBLICATION pub_p4;" >/dev/null
echo "✓ P4 подтверждён: изоляция взрыва WAL работает"
