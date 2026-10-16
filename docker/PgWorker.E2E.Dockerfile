# syntax=docker/dockerfile:1

# Образ SUT-воркера PgWorker для E2E-контура (канон E2E-образов AGENTS.md):
# сборка НА ХОСТЕ (dotnet publish PgWorker.App → artifacts/e2e/app), в образ —
# ТОЛЬКО готовый publish-вывод; sdk-стадий и COPY исходников нет.
# Тег: pgworker:e2e (локально собираемый, в registry не кладётся).
# Контекст сборки — каталог publish-вывода (artifacts/e2e/app).
# Воркер в E2E-контуре: docker.sock монтируется рантаймом (провижининг нод),
# порты не публикуются — API слушает внутрисетевой :8443 (mTLS).
FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY . .
# mTLS-сервер воркера — внутрисетевой порт контура (задан ASPNETCORE_URLS).
EXPOSE 8443
ENTRYPOINT ["dotnet", "PgWorker.App.dll"]
