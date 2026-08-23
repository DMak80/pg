#!/bin/sh
# Entrypoint pgworker-node: конфиги из env (генерирует PgWorker при создании
# контейнера — NodeConfigBuilders) → файлы сервисов, затем supervisord.
set -e

mkdir -p /etc/haproxy /etc/pg_doorman

# Конфиг из env генерирует PgWorker (NodeConfigBuilders); без env (ручной
# запуск образа) затираем дефолтный конфиг apt-пакета — обёртки не стартуют.
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

exec /usr/bin/supervisord -n -c /etc/supervisor/supervisord.conf
