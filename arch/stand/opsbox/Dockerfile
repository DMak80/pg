# «Ops-бокс» стенда: машина запуска v2-скриптов (arch/scripts/abort-move.sh).
# psql и pg_dump уже в образе postgres:18; доставляем etcdctl (контрол-плейн v2)
# и jq (разбор статусов/журналов). Только стенд — образ не для прода.
FROM postgres:18
RUN apt-get update \
 && apt-get install -y --no-install-recommends etcd-client jq \
 && rm -rf /var/lib/apt/lists/*
