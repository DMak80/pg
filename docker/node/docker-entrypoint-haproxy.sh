#!/bin/sh
# Условный запуск haproxy: без сконфигурированного /etc/haproxy/haproxy.cfg
# (ручной запуск образа без env PgWorker) сервис не нужен — тихий выход.
if [ ! -s /etc/haproxy/haproxy.cfg ]; then
    echo "haproxy: конфиг не передан (HAPROXY_CONFIG) — не запускаемся"
    exit 0
fi
exec /usr/sbin/haproxy -db -f /etc/haproxy/haproxy.cfg
