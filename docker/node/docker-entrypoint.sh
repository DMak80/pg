#!/bin/sh
# Entrypoint pgworker-node: конфиги из env (генерирует PgWorker при создании
# контейнера — NodeConfigBuilders) → файлы сервисов, затем supervisord.
#
# ВАЖНО (runit /etc/service/patroni/run вырезает почти весь env): параметры
# lease-скрипта мастер-ключа дублируются в файл — callback Patroni стартует
# с обрезанным окружением и читает их оттуда.
set -e

mkdir -p /etc/haproxy /etc/pg_doorman

if [ -n "$HAPROXY_CONFIG" ]; then
    printf '%s\n' "$HAPROXY_CONFIG" > /etc/haproxy/haproxy.cfg
else
    : > /etc/haproxy/haproxy.cfg
fi

if [ -n "$DOORMAN_CONFIG" ]; then
    printf '%s\n' "$DOORMAN_CONFIG" > /etc/pg_doorman/pg_doorman.ini
else
    : > /etc/pg_doorman/pg_doorman.ini
fi

# ENV lease-скрипта мастер-ключа (P11) — переживает env-фильтр runit.
{
    echo "PGW_ETCD=$(printf '%s' "$PGW_ETCD")"
    echo "PGW_MASTER_KEY=$(printf '%s' "$PGW_MASTER_KEY")"
    echo "PGW_NODE_HOST=$(printf '%s' "$PGW_NODE_HOST")"
    echo "PGW_NODE_NAME=$(printf '%s' "$PGW_NODE_NAME")"
    echo "PGW_DOORMAN_PORT=$(printf '%s' "$PGW_DOORMAN_PORT")"
} > /home/postgres/pgw-node.env
chown postgres:postgres /home/postgres/pgw-node.env
chmod 600 /home/postgres/pgw-node.env

# Patroni запускается под postgres (chpst): каталоги данных должны быть его.
mkdir -p /home/postgres/pgroot /home/postgres/pgdata
chown -R postgres:postgres /home/postgres/pgroot /home/postgres/pgdata

exec /usr/bin/supervisord -n -c /etc/supervisor/supervisord.conf
