# syntax=docker/dockerfile:1

# Образ узла кластера pgworker-node (Д4, spec §5.1, arch/14 §2.1):
# Spilo (PG18+Patroni; R1/Д11 закрыт: failover-слоты поддерживаются с PG17)
# + haproxy (P2 write-фронтенд) + pg_doorman (P13–P17,
# опционально) + supervisord + pgwnodetool master-lease (P11).
# Сборка: docker build -f docker/node/Dockerfile -t pgworker-node:dev .
#   --build-arg DOORMAN_URL=<url-бинарника-релиза>  — включает пулер (версия
#   пинится в самом URL); без него узел без doorman (R1: EnableDoorman=false).
FROM ghcr.io/zalando/spilo-18:4.1-p2

ARG DOORMAN_URL=""

RUN apt-get update \
    && apt-get install -y --no-install-recommends haproxy supervisor \
    && rm -rf /var/lib/apt/lists/* \
    && if [ -n "$DOORMAN_URL" ]; then \
        curl -fsSL "$DOORMAN_URL" -o /usr/local/bin/pg_doorman \
        && chmod +x /usr/local/bin/pg_doorman; \
    fi

# Приёмник WAL (t25, arch/19 §3 + arch/14 §2.1) и служебная CLI pgwnodetool:
# self-contained publish НА ХОСТЕ (RID хоста сборки, без trimming — рефлексия
# Npgsql/AWSSDK/OTel), в образ — ТОЛЬКО publish-вывод (канон «publish на
# хосте, в контейнер — артефакты»):
#   dotnet publish src/PgWorker.WalReceiver -c Release -r <rid> \
#     --self-contained -o artifacts/node-wal
#   dotnet publish src/PgWorker.NodeTools -c Release -r <rid> \
#     --self-contained -o artifacts/node-tools
#   docker build -f docker/node/Dockerfile \
#     --build-arg WAL_PUBLISH_DIR=artifacts/node-wal \
#     --build-arg NODE_TOOLS_PUBLISH_DIR=artifacts/node-tools -t pgworker-node:dev .
ARG WAL_PUBLISH_DIR=artifacts/node-wal
COPY ${WAL_PUBLISH_DIR} /opt/pgw-wal/
RUN chmod +x /opt/pgw-wal/walreceiver
ARG NODE_TOOLS_PUBLISH_DIR=artifacts/node-tools
COPY ${NODE_TOOLS_PUBLISH_DIR} /opt/pgw-tools/
RUN chmod +x /opt/pgw-tools/pgwnodetool

# Контекст сборки — корень репозитория:
# docker build -f docker/node/Dockerfile -t pgworker-node:dev .
COPY docker/node/supervisord.conf /etc/supervisor/supervisord.conf
COPY docker/node/docker-entrypoint.sh /docker-entrypoint-pgw.sh
COPY docker/node/docker-entrypoint-haproxy.sh /docker-entrypoint-haproxy.sh
COPY docker/node/docker-entrypoint-doorman.sh /docker-entrypoint-doorman.sh
RUN chmod +x /docker-entrypoint-pgw.sh \
    /docker-entrypoint-haproxy.sh /docker-entrypoint-doorman.sh

# Порты публикуются PgWorker при создании контейнера (портпаблиши аллокатора).
EXPOSE 5432 8008 6432

CMD ["/docker-entrypoint-pgw.sh"]
