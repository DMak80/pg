#!/bin/sh
# Entrypoint pgworker-node: конфиги из env (генерирует PgWorker при создании
# контейнера — NodeConfigBuilders) → файлы сервисов, затем supervisord.
#
# ВАЖНО (runit /etc/service/patroni/run вырезает почти весь env): параметры
# lease-скрипта мастер-ключа дублируются в файл — callback Patroni стартует
# с обрезанным окружением и читает их оттуда.
set -e

# Tool-режим контейнера (spec t26 §3.3): никаких PG-команд при старте —
# только старт контейнера; агент PgNode — главный процесс (PID-1), весь
# жизненный цикл операций — через его HTTP API. Patroni/supervisord и
# генерация нодовых конфигов не запускаются.
if [ "$PGW_NODE_TOOL" = "1" ]; then
    exec /opt/pgw-node/pgnode
fi

mkdir -p /etc/haproxy

if [ -n "$HAPROXY_CONFIG" ]; then
    printf '%s\n' "$HAPROXY_CONFIG" > /etc/haproxy/haproxy.cfg
else
    : > /etc/haproxy/haproxy.cfg
fi

# Пулер pg_doorman обязателен (arch/14 §8): нет бинарника — отказ старта
# контейнера (ненулевой exit ДО supervisord; воркер видит exited-контейнер).
# Молчаливых «not starting» не существует.
if [ ! -x /usr/local/bin/pg_doorman ]; then
    echo "pg_doorman: /usr/local/bin/pg_doorman binary is missing - DOORMAN_IMAGE build-arg is required"
    exit 1
fi

# Шаблон конфига пулера (неконфиденциальный): пароль подставляет агент PgNode
# при рендере в tmpfs (arch/14 §4); пустой шаблон — старт-отказ контейнера.
if [ -z "$DOORMAN_CONFIG_TEMPLATE" ]; then
    echo "pg_doorman: DOORMAN_CONFIG_TEMPLATE env is empty - cannot start the mandatory pooler"
    exit 1
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

# REST-TLS: CA верификации loopback-клиента (https://127.0.0.1:8008/primary,
# t22) материализуется в файл — PEM в KEY=VALUE-файл pgw-node.env не переносится.
if [ -n "$SSL_RESTAPI_CA" ]; then
    printf '%s\n' "$SSL_RESTAPI_CA" > /home/postgres/pgw-node-ca.pem
    chown postgres:postgres /home/postgres/pgw-node-ca.pem
    chmod 600 /home/postgres/pgw-node-ca.pem
    echo "PGW_NODE_CA=/home/postgres/pgw-node-ca.pem" >> /home/postgres/pgw-node.env
fi

# Patroni запускается под postgres (chpst): каталоги данных должны быть его.
mkdir -p /home/postgres/pgroot /home/postgres/pgdata
chown -R postgres:postgres /home/postgres/pgroot /home/postgres/pgdata

exec /usr/bin/supervisord -n -c /etc/supervisor/supervisord.conf
