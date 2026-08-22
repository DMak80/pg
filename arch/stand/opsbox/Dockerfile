# «Ops-бокс» стенда: машина запуска v2-скриптов (arch/scripts/abort-move.sh).
# psql и pg_dump уже в образе postgres:18; доставляем etcdctl (контрол-плейн v2),
# jq (разбор статусов/журналов) и curl (ops-скрипты find-leader/get-role/
# health/cluster-state ходят по Patroni REST API — на стенде его эмулируют
# сайдкары hc*, чек 76). Только стенд — образ не для прода.
FROM postgres:18
RUN apt-get update \
 && apt-get install -y --no-install-recommends etcd-client jq curl \
 && rm -rf /var/lib/apt/lists/*
