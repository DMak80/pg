# syntax=docker/dockerfile:1

# Образ узла кластера pgworker-node (Д4, spec §5.1, arch/14 §2.1):
# Spilo (PG16+Patroni) + haproxy (P2 write-фронтенд) + pg_doorman (P13–P17,
# опционально) + supervisord + lease-скрипт мастер-ключа (P11).
# Сборка: docker build -f docker/node/Dockerfile -t pgworker-node:dev .
#   --build-arg DOORMAN_URL=<url-бинарника-релиза>  — включает пулер (версия
#   пинится в самом URL); без него узел без doorman (R1: EnableDoorman=false).
FROM ghcr.io/zalando/spilo-16:3.3-p3

ARG DOORMAN_URL=""

RUN apt-get update \
    && apt-get install -y --no-install-recommends haproxy supervisor \
    && rm -rf /var/lib/apt/lists/* \
    && if [ -n "$DOORMAN_URL" ]; then \
        curl -fsSL "$DOORMAN_URL" -o /usr/local/bin/pg_doorman \
        && chmod +x /usr/local/bin/pg_doorman; \
    fi

# Lease-скрипт мастер-ключа — путь совпадает с callback on_role_change
# в SPILO_CONFIGURATION (NodeConfigBuilders, задача 9). Контекст сборки —
# корень репозитория: docker build -f docker/node/Dockerfile -t pgworker-node:dev .
COPY docker/node/master-lease.py /home/postgres/master-lease.py
COPY docker/node/supervisord.conf /etc/supervisor/supervisord.conf
COPY docker/node/docker-entrypoint.sh /docker-entrypoint-pgw.sh
COPY docker/node/docker-entrypoint-haproxy.sh /docker-entrypoint-haproxy.sh
COPY docker/node/docker-entrypoint-doorman.sh /docker-entrypoint-doorman.sh
RUN chmod +x /home/postgres/master-lease.py /docker-entrypoint-pgw.sh \
    /docker-entrypoint-haproxy.sh /docker-entrypoint-doorman.sh

# Порты публикуются PgWorker при создании контейнера (портпаблиши аллокатора).
EXPOSE 5432 8008 6432

CMD ["/docker-entrypoint-pgw.sh"]
