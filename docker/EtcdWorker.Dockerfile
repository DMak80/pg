# syntax=docker/dockerfile:1
# Образ EtcdWorker (arch/25 §8): dotnet publish НА ХОСТЕ, в контейнер — ТОЛЬКО
# publish-вывод (runtime-слой; канон E2E-образов AGENTS.md — без sdk-стадии).
# CLI: dotnet publish src/EtcdWorker.App -c Release -o artifacts/etcdworker/publish
#      docker build -f docker/EtcdWorker.Dockerfile -t etcdworker:dev artifacts/etcdworker/publish
# Compose: context .., args { ETCDW_PUBLISH_DIR: artifacts/etcdworker/publish }
ARG ETCDW_PUBLISH_DIR=.
FROM mcr.microsoft.com/dotnet/aspnet:10.0
ARG ETCDW_PUBLISH_DIR
WORKDIR /app
COPY ${ETCDW_PUBLISH_DIR} ./
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
ENV ASPNETCORE_HTTP_PORTS=8080
EXPOSE 8080
# /healthz за mTLS (arch/25 §1.1): клиентская пара healthcheck из TLS-пакета.
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -sf --cacert /tls/ca.pem --cert /tls/healthcheck.crt --key /tls/healthcheck.key \
    https://localhost:8080/healthz || exit 1
ENTRYPOINT ["dotnet", "EtcdWorker.App.dll"]
