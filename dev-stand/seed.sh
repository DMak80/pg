#!/bin/sh
# Сид кластера в стиле панели AdminPanel (02 §9.1; задача 26): config
# NOT_INITIALIZED + S=2 шарда × replicas=2 + routing/status всех N=6 бакетов
# + заявки request_* (Patroni DCS). После сида PgWorker поднимет кластер.
#
# Использование: ./seed.sh [endpoint] [кластер]
set -e

ETCD="${1:-http://localhost:2379}"
C="${2:-shop}"
N=6
SHARDS="shard1 shard2"

put() {
    key_b64=$(printf %s "$1" | base64 | tr -d '\n')
    value_b64=$(printf %s "$2" | base64 | tr -d '\n')
    curl -sf -X POST "$ETCD/v3/kv/put" \
        -H 'Content-Type: application/json' \
        -d "{\"key\":\"$key_b64\",\"value\":\"$value_b64\"}" >/dev/null
}

put "/clusters/$C/config" \
    "{\"buckets\":$N,\"dbname\":\"$C\",\"created_unix\":$(date +%s),\"state\":\"NOT_INITIALIZED\"}"

i=0
for shard in $SHARDS; do
    put "/clusters/$C/shards/$shard/replicas" "2"
    for node in "${shard}a" "${shard}b"; do
        put "/clusters/$C/shards/$shard/nodes/$node/state" "NOT_INITIALIZED"
    done
    put "/service/$C-$shard/request_cpu" "2"
    put "/service/$C-$shard/request_mem" "2G"
    i=$((i + 1))
done

b=0
while [ "$b" -lt "$N" ]; do
    shard=$(printf 'shard%d' $((b % 2 + 1)))
    put "/clusters/$C/buckets/routing/bucket_$b" "$shard"
    put "/clusters/$C/buckets/status/bucket_$b" '{"state":"NOT_INITIALIZED"}'
    b=$((b + 1))
done

echo "сид кластера $C записан ($N бакетов, шарды: $SHARDS)"
