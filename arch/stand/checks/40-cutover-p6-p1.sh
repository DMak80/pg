#!/usr/bin/env bash
# Cutover на приёмник: P1 (REVOKE-заморозка источника, призрак после flip)
# и P6 (sequence→sequence + инвариант-проверка; провал наивной эвристики
# поиска sequence через pg_depend deptype 'a'/'i').
# Источник после failover — s1b, приёмник — s2.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p logs

q() { docker exec -i "$1" psql -U postgres -d postgres -qAt -v ON_ERROR_STOP=1 "$@"; }

echo ">>> P1: hard freeze of the source (s1b) — before synchronizing sequences"
# Act: REVOKE прав + барьер LOCK TABLE ACCESS EXCLUSIVE (REVOKE сам по себе
# писателей не ждёт — лёгкая блокировка; выяснено на стенде)
tables="$(q s1b -c "select string_agg(format('%I.%I','bucket_42',c.relname),', ')
                   from pg_class c join pg_namespace n on n.oid=c.relnamespace
                   where c.relkind='r' and n.nspname='bucket_42'")"
q s1b -c "BEGIN;
          SET lock_timeout='5s';
          REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA bucket_42 FROM app_role;
          REVOKE USAGE, UPDATE ON ALL SEQUENCES IN SCHEMA bucket_42 FROM app_role;
          LOCK TABLE $tables IN ACCESS EXCLUSIVE MODE;
          COMMIT;"
echo "  ✓ REVOKE + LOCK barrier on s1b (nextval blocked: REVOKE USAGE+UPDATE — no margin needed)"

echo; echo ">>> P6: the naive heuristic (deptype 'a') SKIPS the standalone sequence"
echo "— the query 'find a sequence via column ownership' (pg_depend) on the source:"
q s1b -c "SELECT format('SELECT setval(%L, (SELECT coalesce(max(%I),0) FROM %I.%I) + 1000);',
                format('%I.%I', n.nspname, s.relname), a.attname, n.nspname, t.relname)
          FROM pg_class s
          JOIN pg_depend d  ON d.objid = s.oid AND d.deptype IN ('a','i')
          JOIN pg_class t   ON t.oid = d.refobjid
          JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = d.refobjsubid
          JOIN pg_namespace n ON n.oid = t.relnamespace
          WHERE s.relkind = 'S' AND n.nspname = 'bucket_42';"
echo "  ↑ only customers/…; seq_ticket is MISSING → on s2 it would stay fresh (1)"
echo "— what that means: ticket_no=1 is already taken, and nextval on the unprepared s2 would return 1:"
n="$(q s2a -c "select nextval('bucket_42.seq_ticket')")"
echo "  nextval on s2 (before synchronization): $n"
q s2a -c 'select ticket_no from bucket_42.tickets where ticket_no = 1;'
echo "  ⇒ an INSERT with the default would hit a duplicate key — the 'silent future conflict' from P6"

echo; echo ">>> P6: sequence→sequence synchronization (by name, ALL sequences of the schema)"
# Act: читаем last_value/is_called источника обычным SELECT (nextval не зовём!)
seqs="$(q s1b -c "select c.relname from pg_class c join pg_namespace n on n.oid=c.relnamespace
                  where c.relkind='S' and n.nspname='bucket_42' order by 1")"
for seq in $seqs; do
  read -r lv ic <<< "$(q s1b -c "select last_value||' '||is_called from bucket_42.$seq")"
  q s2a -c "select setval('bucket_42.$seq', $lv, $ic);" >/dev/null
  echo "  bucket_42.$seq: setval($lv, is_called=$ic)"
done

# Assert: инвариант — следующее выдаваемое на приёмнике СТРОГО > последнего выданного на источнике
echo "— invariant check before the flip:"
fail=0
for seq in $seqs; do
  # issued считаем в SQL: конкатенация boolean с текстом даёт 'true'/'false'
  # (не 't'/'f' дисплея psql) — парсить его в bash нельзя
  issued="$(q s1b -c "select case when is_called then last_value else last_value-1 end from bucket_42.$seq")"
  next_dst="$(q s2a -c "select case when is_called then last_value+1 else last_value end from bucket_42.$seq")"
  if [ "$next_dst" -gt "$issued" ]; then
    echo "  ✓ $seq: next on s2=$next_dst > last issued on s1b=$issued"
  else
    echo "  ❌ $seq: $next_dst <= $issued — cutover refused!"; fail=1
  fi
done
[ "$fail" = "0" ] || exit 1

echo; echo ">>> flip: dropping the subscription (the slot moves to s1b), the owner is s2"
# Act
q s2a -c "DROP SUBSCRIPTION sub_bucket_42;"
echo "  sub_bucket_42 slots on s1b: $(q s1b -c "select count(*) from pg_replication_slots where slot_name like 'sub_bucket_42%'") (WAL released)"

echo; echo ">>> P1 post-flip: a 'ghost' writes to the old shard (s1b)"
if out="$(docker exec -i s1b psql -U app_role -d postgres -qAt -v ON_ERROR_STOP=1 \
           -c "INSERT INTO bucket_42.customers(name) VALUES ('ghost-after-flip');" 2>&1)"; then
  echo "❌ the ghost wrote to the old shard!"; exit 1
fi
echo "$out" | grep -m1 "permission denied" >/dev/null \
  && echo "  ✓ the ghost got: $(echo "$out" | grep -m1 -o 'permission denied[^;]*' | head -c 90)" \
  || { echo "❌ unexpected ghost error: $out"; exit 1; }

echo; echo ">>> writes on the new owner (s2) — no collisions"
q s2a -c "INSERT INTO bucket_42.tickets(note) VALUES ('first-on-new-owner') RETURNING ticket_no;"
q s2a -c "INSERT INTO bucket_42.customers(name) VALUES ('new-owner') RETURNING id;"
echo "✓ P1 (freeze/ghost) and P6 (sequence→sequence + invariant) verified"
