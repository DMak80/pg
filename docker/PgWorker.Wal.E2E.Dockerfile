# syntax=docker/dockerfile:1

# E2E-образ WAL-агента (канон AGENTS.md «E2E-образы собираются на хосте»):
# dotnet publish выполняется НА ХОСТЕ (инкрементально, секунды), в контейнер
# упаковывается ТОЛЬКО готовый publish-вывод (runtime-слой, без sdk/исходников).
# Контекст сборки — каталог артефактов (узкий, без «тихой» передачи корня):
#   dotnet publish src/PgWorker.WalReceiver/PgWorker.WalReceiver.csproj \
#     -c Release -o artifacts/e2e/wal
#   docker build -f docker/PgWorker.Wal.E2E.Dockerfile -t pgworker-wal:e2e \
#     artifacts/e2e/wal
FROM mcr.microsoft.com/dotnet/runtime:10.0
WORKDIR /app
COPY . ./
ENTRYPOINT ["dotnet", "PgWorker.WalReceiver.dll"]
