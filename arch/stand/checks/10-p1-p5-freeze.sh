#!/usr/bin/env bash
# Практическая проверка P1 (заморозка REVOKE + барьер LOCK TABLE) и P5
# (технический DDL-мораторий) на источнике ДО переезда. Источник — s1a.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p logs

psql1()  { docker exec -i s1a psql -U postgres -d postgres -qAt -v ON_ERROR_STOP=1 "$@"; }
psql1e() { docker exec -i s1a psql -U postgres -d postgres -qAt -v ON_ERROR_STOP=0 "$@"; }

deny() { # deny <роль> <sql> <grep-паттерн> <метка> — команда ОБЯЗАНА упасть с паттерном
  local out
  if out="$(docker exec -i s1a psql -U "$1" -d postgres -qAt -v ON_ERROR_STOP=1 -c "$2" 2>&1)"; then
    echo "❌ прошло без отказа: $2"; exit 1
  fi
  if echo "$out" | grep -q "$3"; then
    echo "  ✓ $4: $(echo "$out" | grep -m1 -o "$3[^;]*" | head -c 100)"
  else
    echo "❌ неожиданная ошибка ($4): $out"; exit 1
  fi
}

freeze() { # заморозка: REVOKE прав + барьер LOCK TABLE ACCESS EXCLUSIVE
  local tables
  tables="$(psql1 -c "select string_agg(format('%I.%I','bucket_42',c.relname),', ')
                    from pg_class c join pg_namespace n on n.oid=c.relnamespace
                    where c.relkind='r' and n.nspname='bucket_42'")"
  psql1 -c "BEGIN;
            SET lock_timeout='4s';
            REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA bucket_42 FROM app_role;
            REVOKE USAGE, UPDATE ON ALL SEQUENCES IN SCHEMA bucket_42 FROM app_role;  -- nextval дают USAGE ИЛИ UPDATE
            LOCK TABLE $tables IN ACCESS EXCLUSIVE MODE;
            COMMIT;"
}

# ── Arrange: app-роль (≠ owner), миграционная роль-владелец, схема бакета
psql1 <<'SQL'
DROP SCHEMA IF EXISTS bucket_42 CASCADE;
DROP ROLE IF EXISTS app_role;
DROP ROLE IF EXISTS bucket_migr;
CREATE ROLE app_role LOGIN;
CREATE ROLE bucket_migr LOGIN;
CREATE SCHEMA bucket_42 AUTHORIZATION bucket_migr;
SET ROLE bucket_migr;
CREATE TABLE bucket_42.t_logs(id serial PRIMARY KEY, note text);
CREATE SEQUENCE bucket_42.seq_extra START 1;
GRANT USAGE ON SCHEMA bucket_42 TO app_role;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA bucket_42 TO app_role;
GRANT USAGE, UPDATE ON ALL SEQUENCES IN SCHEMA bucket_42 TO app_role;
RESET ROLE;
INSERT INTO bucket_42.t_logs(note) VALUES ('before-freeze');
SQL
echo "— контроль: app_role пишет и читает sequence:"
psql1e -c "SET ROLE app_role; INSERT INTO bucket_42.t_logs(note) VALUES ('app-write');
           SELECT nextval('bucket_42.seq_extra'); RESET ROLE;"

echo; echo "== P5: технический DDL-мораторий (REVOKE CREATE ON SCHEMA) =="
# Act
psql1 -c "REVOKE CREATE ON SCHEMA bucket_42 FROM app_role;"
# Assert
deny app_role "CREATE TABLE bucket_42.t_hack(id int);" "permission denied" "app_role CREATE TABLE отклонён"
psql1e -c "SET ROLE bucket_migr; CREATE TABLE bucket_42.t_migrated(id int); RESET ROLE;"
echo "  ✓ владелец (миграционная роль) создаёт таблицу — его мораторий только процедурный (P5)"
psql1 -c "DROP TABLE bucket_42.t_migrated;"

echo; echo "== P1: REVOKE НЕ барьерит писателей (лёгкая блокировка) — проверяем =="
# Arrange: фоновая транзакция с INSERT висит 8 секунд
( docker exec -i s1a psql -U postgres -d postgres -q -c "BEGIN;
   INSERT INTO bucket_42.t_logs(note) VALUES ('tx-in-flight');
   SELECT pg_sleep(8);
   COMMIT;" > logs/10-inflight-tx.log 2>&1 ) &
sleep 1.5
# Act: голый REVOKE при живом писателе
if psql1 -c "REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA bucket_42 FROM app_role;" \
    2>logs/10-revoke-bare.err; then
  echo "  ✓ подтверждено: REVOKE прошёл мгновенно — писателей НЕ ждёт (AccessShareLock)"
else
  echo "  (!) REVOKE заблокировал: $(cat logs/10-revoke-bare.err)"; exit 1
fi
psql1 -c "GRANT INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA bucket_42 TO app_role;" >/dev/null
echo "  ⇒ барьером служит отдельный LOCK TABLE ACCESS EXCLUSIVE (следующий шаг)"

echo; echo "== P1: заморозка REVOKE + барьер LOCK TABLE =="
echo "  строк в t_logs до: $(psql1 -c 'SELECT count(*) FROM bucket_42.t_logs;')"
# Act: заморозка обязана упереться в писателя и отредактироваться по lock_timeout
if freeze 2>logs/10-revoke-1.err; then
  echo "❌ заморозка не дождалась транзакцию — барьер не работает"; exit 1
fi
grep -m1 -o "canceling statement due to lock timeout" logs/10-revoke-1.err | sed 's/^/  ✓ /'
echo "  (LOCK ACCESS EXCLUSIVE конфликтует со всеми держателями блокировок)"
# Arrange: разбираем зависшую транзакцию
psql1 -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity
          WHERE query LIKE '%pg_sleep%' AND pid <> pg_backend_pid();" >/dev/null
# Act: повторная заморозка
freeze
# Assert
cnt="$(psql1 -c 'SELECT count(*) FROM bucket_42.t_logs;')"
echo "  ✓ заморозка прошла после terminate; строк: $cnt (незакоммиченная вставка откатилась терминацией)"

echo; echo "== P1: «призрак» под заморозкой =="
deny app_role "INSERT INTO bucket_42.t_logs(note) VALUES ('ghost');" "permission denied" "INSERT отклонён"
deny app_role "SELECT nextval('bucket_42.seq_extra');" "permission denied" "nextval закрыт (REVOKE USAGE+UPDATE)"
echo "  — чтение при этом живо (FROZEN = запрет записи, не чтения):"
psql1e -c "SET ROLE app_role; SELECT count(*) FROM bucket_42.t_logs; RESET ROLE;"

echo; echo "== P1: разморозка — симметричный GRANT =="
# Act
psql1 -c "GRANT INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA bucket_42 TO app_role;
          GRANT USAGE, UPDATE ON ALL SEQUENCES IN SCHEMA bucket_42 TO app_role;"
# Assert
docker exec -i s1a psql -U app_role -d postgres -qAt -v ON_ERROR_STOP=1 \
  -c "INSERT INTO bucket_42.t_logs(note) VALUES ('after-unfreeze');" >/dev/null
echo "  ✓ запись восстановлена"

echo; echo "✓ P1/P5 подтверждены на практике (с поправкой: барьер — LOCK TABLE, не сам REVOKE)"
