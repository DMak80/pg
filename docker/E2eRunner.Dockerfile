# syntax=docker/dockerfile:1

# Образ тестового раннера E2E-контура (фаза 2, docs/e2e-isolation.md §1):
# тестовая сборка собирается НА ХОСТЕ (dotnet publish
# PgWorker.IntegrationTests → artifacts/e2e/tests), в образ — ТОЛЬКО готовый
# publish-вывод; sdk-стадий и COPY исходников нет.
# Тег: pgw-e2e-runner:e2e (локально собираемый, в registry не кладётся).
# Контекст сборки — каталог publish-вывода тестов (artifacts/e2e/tests).
FROM mcr.microsoft.com/dotnet/runtime:10.0

# docker CLI: раннер управляет контурами (старт SUT-воркеров, mc/psql-пробы,
# инспекция состояний) через смонтированный /var/run/docker.sock.
# Ubuntu noble: docker-cli отдельным пакетом отсутствует — ставим docker.io
# без recommends (демон в контейнере не запускается и не нужен — только CLI).
RUN apt-get update \
    && apt-get install -y --no-install-recommends docker.io \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /tests
COPY . .

# Фильтр и env контура задаёт запуск (docker run pgw-e2e-runner:e2e …);
# xunit.v3 in-process runner: dotnet exec <assembly> -method <FQN>.
ENTRYPOINT ["dotnet", "exec", "/tests/PgWorker.IntegrationTests.dll"]
