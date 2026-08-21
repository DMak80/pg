#!/usr/bin/env bash
# P2 + P3: отказ мастера источника во время стриминга.
# Ожидаем: HAProxy сам переключает бэкенд, подписка переподключается сама
# (conninfo не менялся), слот пережил failover (synced на s1b), потери нет.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p logs

q() { docker exec -i "$1" psql -U postgres -d postgres -qAt -v ON_ERROR_STOP=1 "$@"; }
via_hap() { docker exec -i s2a psql -h hap1 -U postgres -d postgres -qAt -v ON_ERROR_STOP=1 "$@"; }

ip_s1a="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' s1a)"
ip_s1b="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' s1b)"

# Arrange: пачка W2 через write-эндпоинт
base="$(q s2a -c 'select count(*) from bucket_42.customers')"
exp=$((base + 100))
echo ">>> Arrange: W2 (100 строк) через hap1 (бэкенд сейчас: $(via_hap -c 'select inet_server_addr()')); было $base"
via_hap -c "INSERT INTO bucket_42.customers(name) SELECT 'w2-'||g FROM generate_series(1,100) g;" >/dev/null
for i in $(seq 1 60); do
  [ "$(q s2a -c 'select count(*) from bucket_42.customers')" = "$exp" ] && break; sleep 1
done
echo "  применено на s2: $(q s2a -c 'select count(*) from bucket_42.customers') строк (ожидалось $exp)"
s1b_cnt="$(q s1b -c 'select count(*) from bucket_42.customers')"
echo "  replay на реплике s1b: $s1b_cnt строк (данные физически на standby)"
[ "$s1b_cnt" = "$exp" ] || { echo "❌ standby не догнал — промоушен потерял бы хвост (см. P3 «принятый риск»)"; exit 1; }

# Act: отказ мастера + promote реплики
echo; echo ">>> Act: docker stop s1a + promote s1b"
# предохранитель (находка №4, P3): промоушен теряет слот, если досинк не сошёлся.
# «Тихие логи» мало: synced=t может остаться от прошлого цикла, а ошибки досинка
# идут ретрай-батчами с паузами. Детерминизм: pg_log_standby_snapshot() (PG17+)
# двигает catalog_xmin слота, затем ждём ПОЛОЖИТЕЛЬНОЙ сходимости — копия на
# реплике равна оригиналу (xmin и confirmed LSN) и ошибок нет.
q s1a -c "SELECT pg_log_standby_snapshot();" >/dev/null
conv=""
for i in $(seq 1 60); do
  pa="$(q s1a -c "select catalog_xmin||'/'||confirmed_flush_lsn from pg_replication_slots where slot_name='sub_bucket_42'")"
  pb="$(q s1b -c "select catalog_xmin||'/'||confirmed_flush_lsn from pg_replication_slots where slot_name like 'sub_bucket_42%'")"
  syn="$(q s1b -c "select coalesce(bool_and(synced),false) from pg_replication_slots where slot_name like 'sub_bucket_42%'")"
  if [ "$syn" = "t" ] && [ -n "$pa" ] && [ "$pa" = "$pb" ] \
     && ! docker logs s1b --since 10s 2>&1 | grep -q "could not synchronize"; then
    conv=1; break
  fi
  sleep 1
done
q s1b -c "select slot_name, active, wal_status, failover, synced from pg_replication_slots where slot_name like 'sub_bucket_42%';"
[ -n "$conv" ] || { echo "❌ досинк слота не сошёлся (s1a: $pa, s1b: $pb) — промо потерял бы его"; exit 1; }
echo "  досинк сошёлся: $pa (мастер) == $pb (реплика)"
docker stop -t 3 s1a >/dev/null
PGD="$(docker exec s1b psql -U postgres -tAc 'show data_directory' | tr -d '[:space:]')"
docker exec -u postgres s1b pg_ctl promote -D "$PGD" >/dev/null 2>&1 || true
until [ "$(q s1b -c 'select pg_is_in_recovery()')" = "f" ]; do sleep 1; done
# НАХОДКА: у повышенной ноды остались synchronous_standby_names='FIRST 1 (s1a)',
# а s1a мёртв — при непустых sync-именах и НЕподключённой реплике коммиты ВИСЯТ
# в SyncRep бесконечно (ловушка синхронной репликации). Промоушен без второй
# реплики обязан сопровождаться очисткой sync-имён.
q s1b -c "ALTER SYSTEM SET synchronous_standby_names = ''" -c "SELECT pg_reload_conf()" >/dev/null
echo "  s1b повышен до мастера (sync-имена сняты: реплики под ним нет)"

# Assert 1: HAProxy переключился сам (health-check /primary)
got=""
for i in $(seq 1 60); do
  got="$(via_hap -c 'select inet_server_addr()' 2>/dev/null || true)"
  [ "$got" = "$ip_s1b" ] && break; sleep 1
done
echo "  hap1:5432 -> $got"
[ "$got" = "$ip_s1b" ] || { echo "❌ HAProxy не переключился на s1b"; exit 1; }

# Assert 2: подписка переподключилась сама (P2: conninfo host=hap1 не менялся)
w=""
for i in $(seq 1 60); do
  w="$(q s2a -c "select count(*) from pg_stat_subscription where subname='sub_bucket_42' and pid is not null")"
  [ "$w" = "1" ] && break; sleep 1
done
echo "  apply-воркер подписки активен: $w"
[ "$w" = "1" ] || { echo "❌ подписка не переподключилась"; exit 1; }

# Assert 3: поток продолжился, данные целы, duplicate-key нет (P3: origin пропускает повторную доставку)
via_hap -c "INSERT INTO bucket_42.customers(name) VALUES ('w3-after-failover');" >/dev/null
exp3=$((exp + 1))
for i in $(seq 1 60); do
  [ "$(q s2a -c 'select count(*) from bucket_42.customers')" = "$exp3" ] && break; sleep 1
done
c_s1b="$(q s1b -c 'select count(*) from bucket_42.customers')"
c_s2="$(q s2a  -c 'select count(*) from bucket_42.customers')"
echo "  counts после failover: s1b=$c_s1b  s2=$c_s2"
[ "$c_s1b" = "$c_s2" ] || { echo "❌ расхождение источник/приёмник"; exit 1; }
dups="$(docker logs s2a 2>&1 | grep -c 'duplicate key' || true)"
echo "  ошибок duplicate key в логах приёмника: $dups"
[ "$dups" = "0" ] || { echo "❌ duplicate-key при повторной доставке"; exit 1; }
echo "✓ P2: адресация стабильна, watcher не нужен; P3: слот пережил failover, репликация продолжилась"
