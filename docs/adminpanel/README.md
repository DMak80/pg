# AdminPanel

Read-only панель администрирования шардированных HA-кластеров PostgreSQL
(инспектируемая система — репозиторий `../pg`): etcd, шардирование, HA, алерты.
Операций нет — панель ничего не мутирует.

Статус: скелет решения (задача `t01-skeleton`); архитектура и план — в
[`arch/`](arch/01-architecture.md), дорожная карта — в
[`arch/roadmap/`](arch/roadmap/README.md).

## Стек

.NET 10 (C# `LangVersion=latest`, `Nullable=enable`, warnings как ошибки),
ASP.NET Core Minimal API, централизованное версионирование пакетов (CPM),
решение в формате `.slnx`. Каркас (Result-монада, attribute-DI, CQRS-queries)
перенесён из референсного проекта `../Puzzle`.

## Структура

- `src/AdminPanel.Api` — host: модульная композиция, REST-эндпоинты, `/api/healthz`
- `src/AdminPanel.Core` — домен снапшота (наполняется задачами t02+)
- `src/AdminPanel.Etcd` — etcd-клиент и SnapshotRefresher (t03+)
- `src/AdminPanel.Probes` — live-пробы Patroni/SQL (t05+)
- `src/AdminPanel.Infrastructure` — каркас: Result, attribute-DI, CQRS, health-checks
- `src/tests/` — UnitTests (xunit v3 + FluentAssertions), IntegrationTests

## Сборка и запуск

    dotnet build src/AdminPanel.slnx
    dotnet test src/AdminPanel.slnx
    dotnet run --project src/AdminPanel.Api
    curl http://localhost:5000/api/healthz   # {"status":"ok"}

Тесты Docker не требуют. Фронтенд (React+Vite) и поставка в контейнере —
будущие задачи дорожной карты.
