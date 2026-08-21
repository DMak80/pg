#!/usr/bin/env bash
# Данные бакета + подписка приёмника ЧЕРЕЗ HAProxy (P2) + предпосылки P3
# (слот подписки синхронизирован на реплику sync_replication_slots'ом).
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p logs

q() { docker exec -i "$1" psql -U postgres -d postgres -qAt -v ON_ERROR_STOP=1 "$@"; }
via_hap() { docker exec -i s2a psql -h hap1 -U postgres -d postgres -qAt -v ON_ERROR_STOP=1 "$@"; }

echo ">>> Arrange: бакет с «неудобными» sequences (кейс P6)"
# самоочистка артефактов прошлых запусков (идемпотентность)
q s2a  -c "DROP SUBSCRIPTION IF EXISTS sub_bucket_42; DROP SCHEMA IF EXISTS bucket_42 CASCADE;" >/dev/null
q s1a -c "DROP PUBLICATION IF EXISTS pub_bucket_42;" >/dev/null
q s1a <<'SQL'
DROP SCHEMA IF EXISTS bucket_42 CASCADE;
CREATE SCHEMA bucket_42 AUTHORIZATION bucket_migr;
SET ROLE bucket_migr;
-- deptype 'a' (serial): наивная эвристика (поиск по pg_depend) её ВИДИТ
CREATE TABLE bucket_42.customers(id serial PRIMARY KEY, name text);
-- standalone sequence: НЕ привязана к колонке — наивная эвристика её ПРОПУСКАЕТ
CREATE SEQUENCE bucket_42.seq_ticket START 1;
CREATE TABLE bucket_42.tickets(ticket_no bigint UNIQUE NOT NULL DEFAULT nextval('bucket_42.seq_ticket'), note text);
INSERT INTO bucket_42.customers(name) SELECT 'cust-'||g FROM generate_series(1,50) g;
INSERT INTO bucket_42.tickets(ticket_no) VALUES (1),(2),(3),(4),(5),(100),(110);
-- приложение «сожгло» номера билетов (nextval в откаченных tx/своих циклах)
SELECT setval('bucket_42.seq_ticket', 5000, true);
GRANT USAGE ON SCHEMA bucket_42 TO app_role;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA bucket_42 TO app_role;
GRANT USAGE, UPDATE ON ALL SEQUENCES IN SCHEMA bucket_42 TO app_role;
RESET ROLE;
SQL
echo "  customers=$(q s1a -c 'select count(*) from bucket_42.customers')  \
seq_ticket.last_value=$(q s1a -c 'select last_value from bucket_42.seq_ticket')  \
tickets.max=$(q s1a -c 'select max(ticket_no) from bucket_42.tickets')"

echo; echo ">>> Act: перенос DDL + подписка через HAProxy (P2)"
docker exec s1a pg_dump -U postgres -d postgres --schema-only --schema=bucket_42 \
  --no-owner --no-privileges \
  | docker exec -i s2a psql -U postgres -d postgres -q -v ON_ERROR_STOP=1
# роли в dump с --no-privileges не попадают — создаём на приёмнике отдельно
q s2a -c "CREATE ROLE app_role LOGIN;" >/dev/null 2>&1 || true
q s2a -c "GRANT USAGE ON SCHEMA bucket_42 TO app_role;
         GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA bucket_42 TO app_role;
         GRANT USAGE, UPDATE ON ALL SEQUENCES IN SCHEMA bucket_42 TO app_role;"
q s1a -c "CREATE PUBLICATION pub_bucket_42 FOR TABLES IN SCHEMA bucket_42;"
q s2a  -c "CREATE SUBSCRIPTION sub_bucket_42
           CONNECTION 'host=hap1 port=5432 dbname=postgres user=postgres'
           PUBLICATION pub_bucket_42 WITH (copy_data = true, failover = true);"
echo "  подписка создана: conninfo host=hap1 (write-эндпоинт шарда, не нода — P2);"
echo "  failover=true — иначе sync_replication_slots НЕ скопирует слот на реплику"

echo "— жду initial copy..."
st=""
for i in $(seq 1 120); do
  st="$(q s2a -c "select coalesce(sum((srsubstate='r')::int),0)||'/'||count(*)
                 from pg_subscription_rel
                 where srsubid=(select oid from pg_subscription where subname='sub_bucket_42')")"
  [ "$st" = "2/2" ] && break
  sleep 1
done
echo "  initial copy: $st таблиц готово"
[ "$st" = "2/2" ] || { echo "❌ initial copy не завершился"; exit 1; }
echo "  приёмник: customers=$(q s2a -c 'select count(*) from bucket_42.customers')  tickets=$(q s2a -c 'select count(*) from bucket_42.tickets')"

echo "— стриминг: вставка на источнике ЧЕРЕЗ hap1 → появляется на приёмнике"
via_hap -c "INSERT INTO bucket_42.customers(name) VALUES ('stream-check');" >/dev/null
for i in $(seq 1 30); do
  [ "$(q s2a -c 'select count(*) from bucket_42.customers')" = "51" ] && break; sleep 1
done
echo "  s2/customers = $(q s2a -c 'select count(*) from bucket_42.customers')"

echo; echo ">>> Assert: слот подписки СИНХРОНИЗИРОВАН на реплику s1b (P3)"
synced=""
for i in $(seq 1 60); do
  synced="$(q s1b -c "select coalesce(bool_and(synced),false) from pg_replication_slots where slot_name like 'sub_bucket_42%'")"
  # сразу после initial copy catalog_xmin реплики и слота разъезжаются — ждём,
  # пока periodic re-sync пройдёт БЕЗ ошибок (иначе промо потеряет слот)
  if [ "$synced" = "t" ] && ! docker logs s1b --since 6s 2>&1 | grep -q "could not synchronize"; then
    break
  fi
  sleep 1
done
q s1b -c "select slot_name, active, wal_status, failover, synced from pg_replication_slots where slot_name like 'sub_bucket_42%';"
[ "$synced" = "t" ] || { echo "❌ слот не синхронизировался на s1b — failover его потеряет"; exit 1; }
echo "✓ P2 (подписка за HAProxy) и предпосылки P3 (failover slots: synced=t, ре-синк чистый) на месте"
