#!/bin/sh
# Entrypoint pgworker-node: конфиги из env (генерирует PgWorker при создании
# контейнера — NodeConfigBuilders) → файлы сервисов, затем supervisord.
#
# ВАЖНО (runit /etc/service/patroni/run вырезает почти весь env): параметры
# lease-скрипта мастер-ключа дублируются в файл — callback Patroni стартует
# с обрезанным окружением и читает их оттуда.
set -e

# Tool-режим контейнера (spec t26 §3.3): никаких PG-команд при старте —
# только старт контейнера; WalReceiver — главный процесс (PID-1), весь
# жизненный цикл операций — через его HTTP API. Patroni/supervisord и
# генерация нодовых конфигов не запускаются.
if [ "$PGW_NODE_TOOL" = "1" ]; then
    exec /opt/pgw-wal/walreceiver
fi

mkdir -p /etc/haproxy /etc/pg_doorman

if [ -n "$HAPROXY_CONFIG" ]; then
    printf '%s\n' "$HAPROXY_CONFIG" > /etc/haproxy/haproxy.cfg
else
    : > /etc/haproxy/haproxy.cfg
fi

# Пулер pg_doorman обязателен (arch/14 §8): нет бинарника или конфига —
# отказ старта контейнера (ненулевой exit ДО supervisord; воркер видит
# exited-контейнер). Молчаливых «not starting» не существует.
if [ ! -x /usr/local/bin/pg_doorman ]; then
    echo "pg_doorman: /usr/local/bin/pg_doorman binary is missing - DOORMAN_IMAGE build-arg is required"
    exit 1
fi
if [ -n "$DOORMAN_CONFIG" ]; then
    printf '%s\n' "$DOORMAN_CONFIG" > /etc/pg_doorman/pg_doorman.yaml
else
    echo "pg_doorman: DOORMAN_CONFIG env is empty - cannot start the mandatory pooler"
    exit 1
fi

# Self-signed TLS-серт пулера при каждом старте (P17: клиенты ходят
# sslmode=require без верификации цепочки — серт шифрования не несёт
# идентичности); файлы вне volume — ключ не переживает рестарт контейнера,
# это допустимо (минимум движущихся частей).
openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -subj "/CN=pg_doorman" \
    -keyout /etc/pg_doorman/tls.key -out /etc/pg_doorman/tls.crt

# Пулер работает под postgres (supervisord user=postgres): конфиг и TLS-файлы
# его; ключ — 0600 (читает только владелец).
chown postgres:postgres /etc/pg_doorman/pg_doorman.yaml \
    /etc/pg_doorman/tls.key /etc/pg_doorman/tls.crt
chmod 600 /etc/pg_doorman/tls.key

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
