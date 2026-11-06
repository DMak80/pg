# Образ managed-узла etcd-контура (arch/etcdnode/01-agent.md §1): etcd +
# supervisord + self-contained агент EtcdNode — ОДИН контейнер.
# Канонический образ etcd (quay.io/coreos/etcd:v3.5.21 через локальный
# registry) очищен от shell/apt — supervisord ставится в glibc-носитель
# (dotnet/runtime: Debian, тот же RID-мир linux-arm64/x64, что и publish
# агента); бинарники etcd/etcdctl/etcdutl копируются ИЗ канонического
# образа (Go-статик, версия etcd — канон). Сборка НА ХОСТЕ, в образ —
# ТОЛЬКО publish-вывод (канон поставки узлов, прецедент docker/node):
#   dotnet publish src/EtcdNode.App -c Release -r <rid> --self-contained -o artifacts/etcdnode
#   docker build -f docker/etcdnode/Dockerfile \
#     --build-arg ETCDNODE_PUBLISH_DIR=artifacts/etcdnode -t pgworker/etcdnode:dev .
# В registry НЕ класть (локальный образ).

FROM 192.168.0.1:5000/quay.io/coreos/etcd:v3.5.21 AS etcd

FROM mcr.microsoft.com/dotnet/runtime:10.0

RUN apt-get update \
    && apt-get install -y --no-install-recommends supervisor \
    && rm -rf /var/lib/apt/lists/*

# Канонический etcd (версия — внешний образ, прецедент pg_doorman COPY --from).
COPY --from=etcd /usr/local/bin/etcd /usr/local/bin/etcd
COPY --from=etcd /usr/local/bin/etcdctl /usr/local/bin/etcdctl
COPY --from=etcd /usr/local/bin/etcdutl /usr/local/bin/etcdutl

ARG ETCDNODE_PUBLISH_DIR=artifacts/etcdnode
COPY ${ETCDNODE_PUBLISH_DIR} /opt/etcdnode/
RUN chmod +x /opt/etcdnode/EtcdNode.App

COPY docker/etcdnode/supervisord.conf /etc/supervisor/supervisord.conf
COPY docker/etcdnode/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Клиентская грань публикуется воркером (PortRange); новых портов нет.
EXPOSE 2379 2380

VOLUME ["/etcd-data", "/snapshots"]

CMD ["/entrypoint.sh"]
