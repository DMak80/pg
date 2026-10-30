
# Образ узла кластера pgworker-node (Д4, spec §5.1, arch/14 §2.1):
# Spilo (PG18+Patroni; R1/Д11 закрыт: failover-слоты поддерживаются с PG17)
# + pg_doorman (P13–P17, ОБЯЗАТЕЛЕН: клиентский вход :6432, arch/14 §8 —
# узлов без пулера не существует) + supervisord + pgwnodetool master-lease (P11).
# Сборка: docker build -f docker/node/Dockerfile -t pgworker-node:dev .
#   --build-arg DOORMAN_IMAGE=<мульти-арх образ pg_doorman> — ОБЯЗАТЕЛЕН
#   (пин: ghcr.io/ozontech/pg_doorman:v3.11.0, arch/14 §2.1 R1); без него
#   сборка падает fail-fast.
FROM ghcr.io/zalando/spilo-18:4.1-p2

# Пулер обязателен (arch/14 §2.1 R1): бинарник — COPY --from мульти-арх
# контейнер-образа pg_doorman (docker выбирает архитектуру под платформу
# сборки автоматически); отсутствие build-arg — отказ сборки с диагностикой.
ARG DOORMAN_IMAGE=""
RUN test -n "$DOORMAN_IMAGE" || { echo "DOORMAN_IMAGE build-arg is required (pg_doorman is mandatory, arch/14 §2.1 R1)"; exit 1; }

RUN apt-get update \
    && apt-get install -y --no-install-recommends haproxy supervisor openssl \
    && rm -rf /var/lib/apt/lists/*

# Бинарник пулера из официального образа (путь внутри него — /usr/bin/pg_doorman).
COPY --from=$DOORMAN_IMAGE /usr/bin/pg_doorman /usr/local/bin/pg_doorman

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
COPY docker/node/spilo-supervised.sh /spilo-supervised.sh
COPY docker/node/docker-entrypoint.sh /docker-entrypoint-pgw.sh
COPY docker/node/docker-entrypoint-haproxy.sh /docker-entrypoint-haproxy.sh
RUN chmod +x /docker-entrypoint-pgw.sh \
    /docker-entrypoint-haproxy.sh /spilo-supervised.sh

# Порты публикуются PgWorker при создании контейнера (портпаблиши аллокатора).
EXPOSE 5432 8008 6432

CMD ["/docker-entrypoint-pgw.sh"]
