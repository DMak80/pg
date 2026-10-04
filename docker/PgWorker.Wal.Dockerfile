# syntax=docker/dockerfile:1

# Образ WAL-агента-приёмника (t27, arch/19 §2/§3): опубликованный бинарь
# PgWorker.WalReceiver, ENTRYPOINT без shell. Контейнерам агентов воркер
# передаёт параметры ТОЛЬКО env (§7). Сборка (контекст — корень репо):
#   docker build -f docker/PgWorker.Wal.Dockerfile -t pgworker-wal:dev .
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS publish
WORKDIR /src
COPY src/ ./src/
RUN dotnet publish src/PgWorker.WalReceiver/PgWorker.WalReceiver.csproj -c Release -o /app --nologo

FROM mcr.microsoft.com/dotnet/runtime:10.0
WORKDIR /app
COPY --from=publish /app ./
ENTRYPOINT ["dotnet", "PgWorker.WalReceiver.dll"]
