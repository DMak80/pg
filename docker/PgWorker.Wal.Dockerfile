# syntax=docker/dockerfile:1
# Образ WAL-агента-приёмника (t27, arch/19 §2/§3): dotnet publish НА ХОСТЕ,
# в контейнер — ТОЛЬКО publish-вывод (runtime-слой, без sdk/исходников;
# канон E2E-образов, по образцу ../OwnS3/docker/OwnS3.Dockerfile). CLI (узкий контекст):
#   dotnet publish src/PgWorker.WalReceiver/PgWorker.WalReceiver.csproj -c Release -o artifacts/wal/publish
#   docker build -f docker/PgWorker.Wal.Dockerfile -t pgworker-wal:dev artifacts/wal/publish
# Compose (контекст — корень репо, ARG указывает путь вывода):
#   build: { context: .., dockerfile: docker/PgWorker.Wal.Dockerfile,
#            args: { WAL_PUBLISH_DIR: artifacts/wal/publish } }
# ENTRYPOINT без shell; параметры воркер передаёт ТОЛЬКО env (§7).
# База aspnet (не runtime): WalReceiver транзитивно (Backups → Shared.Metrics)
# объявляет FrameworkReference Microsoft.AspNetCore.App — runtimeconfig требует
# ASP.NET Core runtime; обычный dotnet/runtime его не содержит (старт агента:
# «No frameworks were found»).
ARG WAL_PUBLISH_DIR=.
FROM mcr.microsoft.com/dotnet/aspnet:10.0
ARG WAL_PUBLISH_DIR
WORKDIR /app
COPY ${WAL_PUBLISH_DIR} ./
ENTRYPOINT ["dotnet", "PgWorker.WalReceiver.dll"]
