#!/usr/bin/env bash
# P2 + P3: отказ мастера источника во время стриминга.
# Ожидаем: HAProxy сам переключает бэкенд, подписка переподключается сама
# (conninfo не менялся), слот пережил failover (synced на s1b), потери нет.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p logs

q() { docker exec -i "$1" psql -U postgres -d postgres -qAt -v ON_ERROR_STOP=1 "$@"; }
via_hap() { docker exec -i s2 psql -h hap1 -U postgres -d postgres -qAt -v ON_ERROR_STOP=1 "$@"; }

ip_s1a="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' s1a)"
ip_s1b="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' s1b)"

# Arrange: пачка W2 через write-эндпоинт
base="$(q s2 -c 'select count(*) from bucket_42.customers')"
exp=$((base + 100))
echo ">>> Arrange: W2 (100 строк) через hap1 (бэкенд сейчас: $(via_hap -c 'select inet_server_addr()')); было $base"
via_hap -c "INSERT INTO bucket_42.customers(name) SELECT 'w2-'||g FROM generate_series(1,100) g;" >/dev/null
for i in $(seq 1 60); do
  [ "$(q s2 -c 'select count(*) from bucket_42.customers')" = "$exp" ] && break; sleep 1
done
echo "  применено на s2: $(q s2 -c 'select count(*) from bucket_42.customers') строк (ожидалось $exp)"
s1b_cnt="$(q s1b -c 'select count(*) from bucket_42.customers')"
echo "  replay на реплике s1b: $s1b_cnt строк (данные физически на standby)"
[ "$s1b_cnt" = "$exp" ] || { echo "❌ standby не догнал — промоушен потерял бы хвост (см. P3 «принятый риск»)"; exit 1; }

# Act: отказ мастера + promote реплики
echo; echo ">>> Act: docker stop s1a + promote s1b"
# предохранитель: слот должен быть на s1b и synced — иначе промо его теряет
# (после initial copy нужно дать слоту досинхронизироваться — P3, см. 20-й скрипт)
pre="$(q s1b -c "select coalesce(bool_and(synced),false) from pg_replication_slots where slot_name like 'sub_bucket_42%'")"
[ "$pre" = "t" ] || { echo "❌ слот не synced на s1b — промо потеряет его; повтори 20-й скрипт"; exit 1; }
docker stop -t 3 s1a >/dev/null
PGD="$(docker exec s1b psql -U postgres -tAc 'show data_directory' | tr -d '[:space:]')"
docker exec -u postgres s1b pg_ctl promote -D "$PGD" >/dev/null
until [ "$(q s1b -c 'select pg_is_in_recovery()')" = "f" ]; do sleep 1; done
echo "  s1b повышен до мастера"

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
  w="$(q s2 -c "select count(*) from pg_stat_subscription where subname='sub_bucket_42' and pid is not null")"
  [ "$w" = "1" ] && break; sleep 1
done
echo "  apply-воркер подписки активен: $w"
[ "$w" = "1" ] || { echo "❌ подписка не переподключилась"; exit 1; }

# Assert 3: поток продолжился, данные целы, duplicate-key нет (P3: origin пропускает повторную доставку)
via_hap -c "INSERT INTO bucket_42.customers(name) VALUES ('w3-after-failover');" >/dev/null
exp3=$((exp + 1))
for i in $(seq 1 60); do
  [ "$(q s2 -c 'select count(*) from bucket_42.customers')" = "$exp3" ] && break; sleep 1
done
c_s1b="$(q s1b -c 'select count(*) from bucket_42.customers')"
c_s2="$(q s2  -c 'select count(*) from bucket_42.customers')"
echo "  counts после failover: s1b=$c_s1b  s2=$c_s2"
[ "$c_s1b" = "$c_s2" ] || { echo "❌ расхождение источник/приёмник"; exit 1; }
dups="$(docker logs s2 2>&1 | grep -c 'duplicate key' || true)"
echo "  ошибок duplicate key в логах приёмника: $dups"
[ "$dups" = "0" ] || { echo "❌ duplicate-key при повторной доставке"; exit 1; }
echo "✓ P2: адресация стабильна, watcher не нужен; P3: слот пережил failover, репликация продолжилась"
