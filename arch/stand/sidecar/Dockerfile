FROM mcr.microsoft.com/dotnet/runtime:10.0

# pgwnodetool публикуется на хосте self-contained (канон: в контейнер — артефакты):
# dotnet publish src/PgWorker.NodeTools -c Release -r linux-<arch> --self-contained \
#   -o artifacts/node-tools
ARG NODE_TOOLS_PUBLISH_DIR=artifacts/node-tools
COPY ${NODE_TOOLS_PUBLISH_DIR} /opt/pgw-tools/
RUN chmod +x /opt/pgw-tools/pgwnodetool

EXPOSE 8008
ENTRYPOINT ["/opt/pgw-tools/pgwnodetool"]
CMD ["rolecheck", "--node-name", "unknown", "--etcd-endpoints", "http://etcd:2379", "--pg-host", "127.0.0.1", "--pg-port", "5432", "--listen-port", "8008"]
