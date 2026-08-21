#!/usr/bin/env bash
# Жизненный цикл шардированного кластера (§4.5 доки 11, P23) НАСТОЯЩИМИ
# скриптами init/add/remove-shard из ops-бокса:
#   init alpha (N=8, dbname=postgres): константы config, dsn/replicas шардов,
#     все 8 бакетов поровну round-robin 4/4, гранты app_role;
#   повторный init alpha → отказ (константы неизменяемы, P18);
#   init beta (N=4, dbname=beta) на ТОМ ЖЕ etcd → второй независимый кластер:
#     ключи/схемы/БД изолированы, alpha не задета;
#   add-shard alpha s1x → пустой шард (routing не тронут);
#   негатив create-bucket вне диапазона 0..N-1 → отказ;
#   move bucket_0 s1→s2 (пустой бакет, полный cutover с атомарным flip);
#   remove-shard непустого s2 → отказ (инвариант P23);
#   remove-shard пустого s1x → успех.
# Предусловие: после 70-го (s1b мастер s1; s2a мастер s2, s2b остановлена —
# возвращаем её как sync-standby для P8-префлайта move).
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p logs

ops() { local s="$1"; shift; docker compose run --rm -T opsbox bash "/arch/scripts/$s" "$@"; }
h1()  { docker compose run --rm -T opsbox psql -X -qAt -v ON_ERROR_STOP=1 "host=hap1 port=5432 dbname=postgres user=postgres" "$@"; }
h2()  { docker compose run --rm -T opsbox psql -X -qAt -v ON_ERROR_STOP=1 "host=hap2 port=5432 dbname=postgres user=postgres" "$@"; }
hb1() { docker compose run --rm -T opsbox psql -X -qAt -v ON_ERROR_STOP=1 "host=hap1 port=5432 dbname=beta user=postgres" "$@"; }
ect() { docker exec etcd etcdctl --endpoints=http://localhost:2379 "$@"; }
ip()  { docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$1"; }

# ═══ Arrange 0: вернуть s2b sync-standby (P8-предусловие приёмника) ═══════════
# После 70-го s2b — ПОВЫШЕННЫЙ бывший мастер (RED-фаза), просто стартовать её
# нельзя: пересоздаём с нуля как реплику s2a (паттерн 68-го: слот срезать,
# basebackup создаст заново). Пропускаем, если s2b уже живая реплика.
if [ "$(docker inspect -f '{{.State.Running}}' s2b 2>/dev/null)" != "true" ] \
   || [ "$(docker exec s2b psql -U postgres -tAc 'select pg_is_in_recovery()' 2>/dev/null)" != "t" ]; then
  echo ">>> Arrange: пересоздаю s2b (реплика s2a, sync-standby)"
  docker compose rm -sf hc2b s2b >/dev/null 2>&1 || true
  dropped=""
  for i in $(seq 1 15); do
    # слота может уже не быть (не переносится при promote) — это нормально:
    # basebackup создаст заново (-C -S)
    n="$(h2 -c "SELECT count(*) FROM pg_replication_slots WHERE slot_name='s2b_phys'" 2>/dev/null || true)"
    { [ "$n" = "0" ] || h2 -c "SELECT pg_drop_replication_slot('s2b_phys')" >/dev/null 2>&1; } \
      && { dropped=1; break; }
    sleep 1
  done
  [ -n "$dropped" ] || { echo "❌ не срезать слот s2b_phys на s2a"; exit 1; }
  docker compose up -d s2b hc2b >/dev/null 2>&1
  until docker exec s2b pg_isready -U postgres -q 2>/dev/null; do sleep 2; done
  until [ "$(docker exec s2b psql -U postgres -tAc 'select pg_is_in_recovery()' 2>/dev/null)" = "t" ]; do
    sleep 1
  done
fi
h2 -c "ALTER SYSTEM SET synchronous_standby_names = 'FIRST 1 (s2b)'" -c "SELECT pg_reload_conf()" >/dev/null
st=""
for i in $(seq 1 60); do
  st="$(h2 -c "select sync_state from pg_stat_replication where application_name='s2b'" 2>/dev/null || true)"
  [ "$st" = "sync" ] && break; sleep 1
done
[ "$st" = "sync" ] || { echo "❌ s2b не вернулась sync-standby (P8-предусловие)"; exit 1; }
echo "  s2: sync-standby s2b жив"

# ═══ Arrange 1: чистка хвостов прошлого прогона ═══════════════════════════════
echo ">>> Arrange: чистка хвостов (кластеры alpha/beta, схемы bucket_0..7, БД beta)"
ect del /clusters/alpha --prefix >/dev/null 2>&1 || true
ect del /clusters/beta  --prefix >/dev/null 2>&1 || true
h1 -c "DROP SCHEMA IF EXISTS bucket_0 CASCADE; DROP SCHEMA IF EXISTS bucket_1 CASCADE; DROP SCHEMA IF EXISTS bucket_2 CASCADE; DROP SCHEMA IF EXISTS bucket_3 CASCADE;
       DROP SCHEMA IF EXISTS bucket_4 CASCADE; DROP SCHEMA IF EXISTS bucket_5 CASCADE; DROP SCHEMA IF EXISTS bucket_6 CASCADE; DROP SCHEMA IF EXISTS bucket_7 CASCADE;" >/dev/null 2>&1 || true
h2 -c "DROP SCHEMA IF EXISTS bucket_0 CASCADE; DROP SCHEMA IF EXISTS bucket_1 CASCADE; DROP SCHEMA IF EXISTS bucket_2 CASCADE; DROP SCHEMA IF EXISTS bucket_3 CASCADE;
       DROP SCHEMA IF EXISTS bucket_4 CASCADE; DROP SCHEMA IF EXISTS bucket_5 CASCADE; DROP SCHEMA IF EXISTS bucket_6 CASCADE; DROP SCHEMA IF EXISTS bucket_7 CASCADE;" >/dev/null 2>&1 || true
h1 -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='beta'" >/dev/null 2>&1 || true
h1 -c "DROP DATABASE IF EXISTS beta;" >/dev/null 2>&1 || true
h2 -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='beta'" >/dev/null 2>&1 || true
h2 -c "DROP DATABASE IF EXISTS beta;" >/dev/null 2>&1 || true
h1 -c "CREATE DATABASE beta;" >/dev/null
h2 -c "CREATE DATABASE beta;" >/dev/null

# ═══ Act 1: init alpha ════════════════════════════════════════════════════════
echo ">>> Act 1: init-cluster.sh --cluster alpha (2 шарда, N=8, dbname=postgres)"
ops init-cluster.sh --cluster alpha --buckets 8 --dbname postgres --replicas 1 --yes \
     --shard "s1=host=hap1 port=5432 dbname=postgres user=postgres" \
     --shard "s2=host=hap2 port=5432 dbname=postgres user=postgres" \
     2>&1 | grep -v "Container\|Creating\|Created" | sed 's/^/  /'

echo ">>> Assert 1: config (константы), dsn/replicas шардов, routing поровну 4/4"
cfg="$(ect get /clusters/alpha/config --print-value-only)"
echo "$cfg" | jq -e '.buckets == 8 and .dbname == "postgres"' >/dev/null \
  || { echo "❌ config неверен: $cfg"; exit 1; }
[ "$(ect get /clusters/alpha/shards/s1/dsn --print-value-only)" = "host=hap1 port=5432 dbname=postgres user=postgres" ] \
  || { echo "❌ dsn s1 не записан/с паролем"; exit 1; }
[ "$(ect get /clusters/alpha/shards/s1/replicas --print-value-only)" = "1" ] \
  || { echo "❌ replicas s1 != 1"; exit 1; }
dist="$(ect get /clusters/alpha/buckets/routing/ --prefix --print-value-only | sort | uniq -c | awk '{print $2":"$1}' | sort | tr '\n' ' ')"
[ "$dist" = "s1:4 s2:4 " ] || { echo "❌ распределение не 4/4: $dist"; exit 1; }
echo "  routing: $dist"
# round-robin строго чередуется: bucket_0→s1, bucket_1→s2, ...
[ "$(ect get /clusters/alpha/buckets/routing/bucket_0 --print-value-only)" = "s1" ] \
  && [ "$(ect get /clusters/alpha/buckets/routing/bucket_1 --print-value-only)" = "s2" ] \
  || { echo "❌ round-robin нарушен"; exit 1; }
for i in 0 2 4 6; do
  [ "$(h1 -c "SELECT to_regnamespace('bucket_$i') IS NOT NULL")" = "t" ] || { echo "❌ схемы bucket_$i нет на s1"; exit 1; }
done
for i in 1 3 5 7; do
  [ "$(h2 -c "SELECT to_regnamespace('bucket_$i') IS NOT NULL")" = "t" ] || { echo "❌ схемы bucket_$i нет на s2"; exit 1; }
done
echo "  схемы: s1=bucket_0,2,4,6; s2=bucket_1,3,5,7; USAGE app_role: $(h1 -c "SELECT count(*) FROM pg_namespace n WHERE n.nspname ~ '^bucket_[0-7]$' AND has_schema_privilege('app_role', n.oid, 'USAGE')")/4"

# ═══ Act 2: повторный init → отказ ════════════════════════════════════════════
echo ">>> Act 2: повторный init alpha → отказ (константы неизменяемы, P18)"
if ops init-cluster.sh --cluster alpha --buckets 8 --dbname postgres --yes \
     --shard "s1=host=hap1 port=5432 dbname=postgres user=postgres" \
     >logs/72-reinit.log 2>&1; then
  echo "❌ повторный init должен отказаться"; exit 1
fi
grep -q "уже инициализирован" logs/72-reinit.log || { echo "❌ неожиданный отказ:"; cat logs/72-reinit.log; exit 1; }
echo "  отказ: $(grep -o 'уже инициализирован' logs/72-reinit.log | head -1) ✓"

# ═══ Act 3: init beta на том же etcd → изоляция кластеров ═════════════════════
echo ">>> Act 3: init-cluster.sh --cluster beta (N=4, dbname=beta) на том же etcd"
routing_alpha_before="$(ect get /clusters/alpha/buckets/routing/ --prefix | md5)"
ops init-cluster.sh --cluster beta --buckets 4 --dbname beta --replicas 1 --yes \
     --shard "s1=host=hap1 port=5432 dbname=beta user=postgres" \
     --shard "s2=host=hap2 port=5432 dbname=beta user=postgres" \
     2>&1 | grep -v "Container\|Creating\|Created" | sed 's/^/  /'
echo ">>> Assert 3: beta живёт в своей БД, alpha не задета"
[ "$(ect get /clusters/beta/config --print-value-only | jq -r .dbname)" = "beta" ] \
  || { echo "❌ config beta неверен"; exit 1; }
[ "$(hb1 -c "SELECT count(*) FROM pg_namespace WHERE nspname LIKE 'bucket_%'")" = "2" ] \
  || { echo "❌ бакеты beta не в БД beta"; exit 1; }
routing_alpha_after="$(ect get /clusters/alpha/buckets/routing/ --prefix | md5)"
[ "$routing_alpha_before" = "$routing_alpha_after" ] \
  || { echo "❌ routing alpha изменился при init beta"; exit 1; }
[ "$(h1 -c "SELECT count(*) FROM pg_namespace WHERE nspname LIKE 'bucket_%' AND nspname ~ '^bucket_[0-7]$'")" = "4" ] \
  || { echo "❌ схемы alpha в postgres задеты"; exit 1; }
echo "  beta: схемы в БД beta; alpha routing/схемы нетронуты ✓"

# ═══ Act 4: add-shard (пустой) + негатив create-bucket вне диапазона ══════════
echo ">>> Act 4: add-shard.sh s1x → пустой шард"
ops add-shard.sh --cluster alpha s1x --dsn 'host=hap1 port=5432 dbname=postgres user=postgres' \
     2>&1 | grep -v "Container\|Creating\|Created" | sed 's/^/  /'
[ -n "$(ect get /clusters/alpha/shards/s1x/dsn --print-value-only)" ] || { echo "❌ s1x не зарегистрирован"; exit 1; }
dist2="$(ect get /clusters/alpha/buckets/routing/ --prefix --print-value-only | sort | uniq -c | awk '{print $2":"$1}' | sort | tr '\n' ' ')"
[ "$dist2" = "s1:4 s2:4 " ] || { echo "❌ add-shard тронул routing: $dist2"; exit 1; }
echo "  s1x зарегистрирован, бакеты не двигались ✓"
if ops create-bucket.sh --cluster alpha bucket_99 --shard s1 >logs/72-create99.log 2>&1; then
  echo "❌ create-bucket вне диапазона должен отказаться"; exit 1
fi
grep -q "вне диапазона" logs/72-create99.log || { echo "❌ неожиданный отказ create-bucket:"; cat logs/72-create99.log; exit 1; }
echo "  create-bucket bucket_99 (N=8) → отказ ✓"

# ═══ Act 5: move bucket_0 s1 → s2 ═════════════════════════════════════════════
echo ">>> Act 5: move-bucket.sh --cluster alpha move bucket_0 --to s2 (пустой бакет, полный cutover)"
ops move-bucket.sh --cluster alpha move bucket_0 --to s2 --yes --skip-reverse \
     2>&1 | tee logs/72-move.log | grep -v "Container\|Creating\|Created" | sed 's/^/  /'
[ "$(ect get /clusters/alpha/buckets/routing/bucket_0 --print-value-only)" = "s2" ] \
  || { echo "❌ routing bucket_0 != s2"; exit 1; }
[ -z "$(ect get /clusters/alpha/buckets/status/bucket_0 --print-value-only)" ] \
  || { echo "❌ статус-ключ bucket_0 не удалён"; exit 1; }
[ "$(h2 -c "SELECT to_regnamespace('bucket_0') IS NOT NULL")" = "t" ] \
  || { echo "❌ схемы bucket_0 нет на s2"; exit 1; }
echo "  bucket_0: s1 → s2, атомарный flip ✓"

# ═══ Act 6: remove-shard непустого → отказ; пустого → успех ═══════════════════
echo ">>> Act 6: remove-shard.sh s2 (непустой) → отказ (инвариант P23)"
if ops remove-shard.sh --cluster alpha s2 --yes >logs/72-rm-s2.log 2>&1; then
  echo "❌ remove-shard непустого должен отказаться"; exit 1
fi
grep -Eq "есть бакеты" logs/72-rm-s2.log || { echo "❌ неожиданный отказ:"; cat logs/72-rm-s2.log; exit 1; }
grep -v "Container\|Creating\|Created" logs/72-rm-s2.log | sed 's/^/  /'
[ -n "$(ect get /clusters/alpha/shards/s2/dsn --print-value-only)" ] \
  || { echo "❌ отказ удалил регистрацию s2!"; exit 1; }

echo ">>> Act 6b: remove-shard.sh s1x (пустой) → успех"
ops remove-shard.sh --cluster alpha s1x --yes 2>&1 | grep -v "Container\|Creating\|Created" | sed 's/^/  /'
[ -z "$(ect get /clusters/alpha/shards/s1x/dsn --print-value-only)" ] \
  || { echo "❌ регистрация s1x не удалена"; exit 1; }
echo "  alpha: шарды s1 s2 (s1x снят) ✓"

echo "✓ жизненный цикл: init (константы, поровну round-robin) → отказ повторного init →"
echo "  второй кластер на том же etcd изолирован (своя БД, alpha нетронута) → add-shard пустым →"
echo "  move с атомарным flip → remove непустого отказал (P23), пустого снят"
