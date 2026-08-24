#!/bin/sh
# Идемпотентный сид контроль-плейна demo (spec t10 §4).
# Значения = EtcdSeed интеграционных тестов; времена статус-ключей —
# динамические (now-60/-900/-7200), чтобы seeded-аномалии были живыми.
# Запуск: сервис seed (docker compose up) или docker compose run --rm seed.
set -eu

# Эндпоинт передаём только env'ом: etcdctl 3.5.21 падает, когда
# ETCDCTL_ENDPOINTS в окружении дублируется флагом --endpoints.
: "${ETCDCTL_ENDPOINTS:=http://etcd:2379}"
export ETCDCTL_ENDPOINTS
ECT() { etcdctl "$@"; }

# Идемпотентность: существующий config => состояние уже засеяно (в т.ч.
# эмуляторами с lease) — не портим, выходим успешно (spec §4).
if [ -n "$(ECT get /clusters/demo/config --print-value-only 2>/dev/null)" ]; then
  echo "seed: /clusters/demo уже засеян — пропускаю"
  exit 0
fi

now=$(date +%s)
put() { ECT put "$1" "$2" >/dev/null; }

echo "seed: пишу контроль-плейн demo (unix=$now)"
put /clusters/demo/config \
  "{\"buckets\":16,\"dbname\":\"demo\",\"created_unix\":$now}"

# Шарды: dsn/replicas/master (master статично; в full эмулятор перепишет с lease)
put /clusters/demo/shards/s1/dsn 'host=s1a,s1b port=5432 dbname=demo user=postgres'
put /clusters/demo/shards/s1/replicas '1'
put /clusters/demo/shards/s1/master 's1a:5432'
put /clusters/demo/shards/s2/dsn 'host=s2a,s2b port=5432 dbname=demo user=postgres'
put /clusters/demo/shards/s2/replicas '1'
put /clusters/demo/shards/s2/master 's2a:5432'

# Routing 16 бакетов фикс-раскладкой EtcdSeed (s1=10, s2=6; spec §4)
for b in 0 2 3 4 6 8 10 11 12 14; do put "/clusters/demo/buckets/routing/bucket_$b" s1; done
for b in 1 5 7 9 13 15;           do put "/clusters/demo/buckets/routing/bucket_$b" s2; done

# Очередь заявок PgWorker (arch/02 §2.3.1): bucket_13 (принадлежит s2) — «увезти на s1»
put /pgworker/moves/demo/bucket_13 '{"op":"move","to":"s1","requested_unix":1755850000,"requested_by":"ops"}'

# Статусы переездов: bucket_3 свежий; 7/11 протухшие (порог StaleMoveSeconds=600)
put /clusters/demo/buckets/status/bucket_3 \
  "{\"bucket\":\"bucket_3\",\"state\":\"SYNCING\",\"owner\":\"s1\",\"target\":\"s2\",\"started_unix\":$((now-120)),\"updated_unix\":$((now-60)),\"phase\":\"copy\"}"
put /clusters/demo/buckets/status/bucket_7 \
  "{\"bucket\":\"bucket_7\",\"state\":\"ABORTING\",\"owner\":\"s2\",\"target\":\"s1\",\"started_unix\":$((now-1000)),\"updated_unix\":$((now-900)),\"phase\":\"cleanup\",\"last_error\":\"receiver went away\"}"
put /clusters/demo/buckets/status/bucket_11 \
  "{\"bucket\":\"bucket_11\",\"state\":\"FROZEN\",\"owner\":\"s1\",\"target\":\"s2\",\"started_unix\":$((now-7400)),\"updated_unix\":$((now-7200)),\"phase\":\"cutover-wait\"}"

put /clusters/demo/heals/bucket_5 \
  "{\"bucket\":\"bucket_5\",\"was\":\"s2\",\"now\":\"s1\",\"reason\":\"restore-heal\",\"ts\":$((now-86400))}"

# HA-DCS: два scope; статично (в full эмуляторы перепишут members/leader/optime с lease)
for s in s1 s2; do
  a="${s}a"; b="${s}b"
  put "/service/demo-$s/leader" "{\"name\":\"$a\"}"
  put "/service/demo-$s/members/$a" "{\"name\":\"$a\",\"conn_url\":\"$a:5432\",\"role\":\"master\",\"state\":\"running\",\"timeline\":1,\"lag\":0}"
  put "/service/demo-$s/members/$b" "{\"name\":\"$b\",\"conn_url\":\"$b:5432\",\"role\":\"replica\",\"state\":\"streaming\",\"timeline\":1,\"lag\":0}"
done
put /service/demo-s1/optime/leader '738273634528'
put /service/demo-s1/initialize '738273612345678'
put /service/demo-s1/config '{"ttl":5,"loop_wait":2,"retry_timeout":3}'
put /service/demo-s2/optime/leader '738273634001'
put /service/demo-s2/initialize '738273611234567'
put /service/demo-s2/config '{"ttl":5,"loop_wait":2,"retry_timeout":3}'

# Стендовая топология (в full перепишут эмуляторы реальными IP с lease)
put /cluster/nodes/s1a '172.28.0.11'
put /cluster/nodes/s1b '172.28.0.12'
put /cluster/nodes/s2a '172.28.0.21'
put /cluster/nodes/s2b '172.28.0.22'

# Самопроверка: ключи легли (spec §4)
[ -n "$(ECT get /clusters/demo/config --print-value-only)" ] || { echo "seed: ❌config не записан"; exit 1; }
[ -n "$(ECT get /clusters/demo/buckets/routing/bucket_0 --print-value-only)" ] || { echo "seed: ❌routing не записан"; exit 1; }
echo "seed: ✓ контроль-плейн demo засеян"
