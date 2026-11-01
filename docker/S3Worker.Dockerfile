# Образ S3Worker (arch/24 §8): dotnet publish НА ХОСТЕ, в контейнер — ТОЛЬКО
# publish-вывод (runtime-слой; канон E2E-образов AGENTS.md — без sdk-стадии).
# CLI: dotnet publish src/S3Worker.App -c Release -o artifacts/s3worker/publish
#      docker build -f docker/S3Worker.Dockerfile -t s3worker:dev artifacts/s3worker/publish
# Compose: context .., args { S3W_PUBLISH_DIR: artifacts/s3worker/publish }
ARG S3W_PUBLISH_DIR=.
FROM mcr.microsoft.com/dotnet/aspnet:10.0
ARG S3W_PUBLISH_DIR
WORKDIR /app
COPY ${S3W_PUBLISH_DIR} ./
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
ENV ASPNETCORE_HTTP_PORTS=8080
EXPOSE 8080
# /healthz — открытая https-грань без клиентского серта (arch/24 §1.1):
# серверный TLS + доверие ca.pem (volume /tls:ro в compose).
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -sf --cacert /tls/ca.pem https://localhost:8080/healthz || exit 1
ENTRYPOINT ["dotnet", "S3Worker.App.dll"]
