# Выжимка исследования (вход для спецификации)

Факты собраны Explore-агентами 2026-08-27 до планирования. Проверить точечно при необходимости, заново не исследовать.

## Репозиторий pg (/Users/demakaev/ZCodeProject/pg)
- Solution: `src/PgWorker.slnx` (XML-формат), solution-папки: `/common/` (props), `/core/`, `/etcd/`, `/docker/`, `/provisioning/`, `/moves/`, `/app/`, `/tests/`.
- `src/Directory.Build.props`: net10.0, LangVersion=latest, ImplicitUsings=enable, Nullable=enable, TreatWarningsAsErrors=true, IsPackable=false.
- `src/Directory.Packages.props`: CPM (ManagePackageVersionsCentrally=true, EnablePackageVersionOverride=false). Пакеты: M.E.* 10.0.9 (Configuration, DI, Hosting, Http, HealthChecks.Abstractions; Options.ConfigurationExtensions 10.0.0), Npgsql 10.0.3, Polly 8.7.0, Polly.Contrib.WaitAndRetry 1.1.1; тестовые: xunit.v3 3.2.2, Microsoft.NET.Test.Sdk 18.6.0, FluentAssertions 7.2.1, Testcontainers 4.14.0, coverlet.collector 10.0.1.
- `src/NuGet.Config`: только nuget.org + packageSourceMapping (`*` → nuget.org).
- Проекты: PgWorker.Core (база) ← PgWorker.Etcd, PgWorker.Docker ← PgWorker.Provisioning ← PgWorker.Moves ← PgWorker.App (Worker SDK, WebApplication + FrameworkReference AspNetCore.App, только /healthz). Тесты: tests/PgWorker.UnitTests (357), tests/PgWorker.IntegrationTests.
- Поставка: deploy/docker-compose.yml (сервис pgworker, образ pgworker:dev, docker/PgWorker.Dockerfile — multi-stage sdk:10.0→aspnet:10.0, сделан «по образцу AdminPanel/Dockerfile»). Другие compose: dev-stand/compose.yaml (только etcd v3.5.21), arch/stand/*.
- Git: origin=git@github.com:DMak80/pg.git, только main. HEAD на старте задачи: 8c33327.
- `arch/`: 14 нумерованных документов + roadmap/ + configs/ + scripts/ + stand/. AdminPanel фигурирует как владелец etcd-контракта: `arch/14-pgworker.md` (ссылается на репозиторий AdminPanel), `arch/11-bucket-sharding.md`, `arch/roadmap/pgworker.md` (t07-move-bucket-ui — UI переездов в панели). Доки pg ссылаются на `../AdminPanel/arch/02-etcd-contract.md` (напр. docs/superpowers/2026-08-23-pgworker-backend/spec.md).

## Проект AdminPanel (/Users/demakaev/ZCodeProject/AdminPanel)
- Отдельный git-репозиторий, remote ОТСУТСТВУЕТ, ветка main, 190 коммитов, рабочее дерево чистое, HEAD ae25346 (2026-08-26). 308 файлов в git.
- Корень: arch/ (контракт-доки, включая 02-etcd-contract.md — канон), dev-stand/, docs/, frontend/, src/, .dev-flow/, Dockerfile, README.md, AGENTS.md, .gitignore (~453+ строк; wwwroot-билд gitignored).
- src/AdminPanel.slnx; 7 csproj: AdminPanel.Api (Sdk.Web, host), AdminPanel.Core, AdminPanel.Etcd, AdminPanel.Probes, AdminPanel.Infrastructure, tests/AdminPanel.UnitTests, tests/AdminPanel.IntegrationTests. Граф: Api → Core/Etcd/Infrastructure/Probes; Core → Infrastructure; Etcd, Probes → Core.
- Свои src/Directory.Build.props (ИДЕНТИЧЕН pg по настройкам: net10.0, latest, ImplicitUsings, Nullable, TreatWarningsAsErrors, IsPackable=false), src/Directory.Packages.props (CPM), src/NuGet.Config (одинаковый по смыслу).
- Пакеты AdminPanel, отличные от pg: Microsoft.AspNetCore.Mvc.Testing 10.0.9, Microsoft.AspNetCore.OpenApi 10.0.11, xunit.runner.visualstudio 3.1.5. Совпадающие версии: Npgsql 10.0.3, xunit.v3 3.2.2, FluentAssertions 7.2.1, Testcontainers 4.14.0, coverlet 10.0.1, Test.Sdk 18.6.0, M.E.* 10.0.9 / Options.ConfigurationExtensions 10.0.0.
- Фронтенд: frontend/ — React 19 + Vite 8 + TypeScript ~7.0, Mantine 9, TanStack Query 5, react-router 8, Node >= 22.12. Скрипты: dev (vite :5173, proxy /api → localhost:5050), build (tsc --noEmit ×2 + vite build), typecheck. outDir = ../src/AdminPanel.Api/wwwroot (emptyOutDir), билд gitignored. 33 файла в frontend/src (28 tsx + 5 ts), страницы: Overview, Clusters, ClusterDetails, Etcd, HA, HaScopeDetails, Alerts.
- Зависимостей от pg НЕТ (ни ProjectReference, ни using PgWorker) — только через etcd-контракт. Дубли кода: `AdminPanel.Etcd/Client/{EtcdGateway,IEtcdGateway,Kv}.cs` — урезанный аналог `PgWorker.Etcd/Client/` (178 vs 279 строк, без Coordination); Puzzle-каркас Infrastructure (attribute-DI, CQRS, Result, Traces) дублирует по родословной PgWorker.Core.
- Запуск: Program.cs — WebApplication, cookie-auth (fail-closed без пароля), static files + SPA-fallback из wwwroot, /api/healthz, порт 5050. appsettings: AdminPanel:Auth:Username=admin, Etcd:Endpoints, Probes (PatroniEnabled/SqlEnabled, Password, HostMap), Alerts-пороги. appsettings.Development.json ~130 КБ (HostMap docker-стенда + dev-пароль admin/admin).
- Объём: 172 .cs (111 прод + 61 тесты). Тесты: xunit.v3 + FluentAssertions, интеграционные с Mvc.Testing + Testcontainers.
- Компиляционных конфликтов с pg нет: namespace-ы AdminPanel.* vs PgWorker.* не пересекаются, тест-фикстуры EtcdFixtures — в разных сборках.

## Контекст окружения
- Правило: PgWorker всегда в докере (deploy/docker-compose.yml); AdminPanel исторически запускается хост-процессом (npm run build + dotnet run с AdminPanel__Probes__Password). Это НЕ меняем.
- Проверки на docker-стенде и интеграционные тесты (Testcontainers) — только с отдельного согласия пользователя.
- Push в main — только по явному приказу. Коммиты в feature-ветке — свободно.
