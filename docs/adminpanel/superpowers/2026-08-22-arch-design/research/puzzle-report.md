# Исследование референсного проекта Puzzle (PuzzleServer)

Источник: Explore-агент, 2026-08-22. Референс для подходов к реализации AdminPanel (можно копировать код).

## 1. Что за проект

**PuzzleServer** (`/Users/demakaev/ZCodeProject/Puzzle`) — backend игровой/платёжной платформы. Не REST: весь ввод-вывод через единственный HTTP-endpoint `POST /command` с plain-text телом.

- **Стек:** .NET 10 (`net10.0`, `LangVersion=latest`, `Nullable=enable`), C# 14 (extension-блоки, primary constructors).
- **Персистентность:** PostgreSQL 18 (Npgsql) + **Dapper** (micro-ORM, не EF), миграции через **DbUp** (embedded `.sql`).
- **Оркестрация локальной среды:** .NET **Aspire 13.4** (Postgres + Kafka + pgAdmin).
- **Наблюдаемость:** OpenTelemetry (metrics/traces/logs через ServiceDefaults).
- Решение в новом формате **`.slnx`**: `/Users/demakaev/ZCodeProject/Puzzle/src/PuzzleServer.Api.slnx`.

## 2. Структура решения, слои, паттерны

Проекты под `/Users/demakaev/ZCodeProject/Puzzle/src/` (сгруппированы в `.slnx`): `PuzzleServer.Api` (host, Program.cs ~55 строк), `.Api.AppHost` (Aspire-композиция), `.Api.ServiceDefaults` (OTel, health checks, resilience), `Core` (домен: Users/Wallets/Tokens/Tickets c миграциями/репозиториями), `Core.App` (Application-слой, хендлеры команд), `Infrastructure.App` (CQRS, DI, DB, Migrations, Contexts, TypedId, Retry, Traces, HealthChecks, Notifications, Result), `Infrastructure.Models`, `.Bus/.Outbox/.Audit/.Kafka/.TextServer`, `Payments*`, тесты `UnitTests`/`IntegrationTests`.

**Паттерны:**
- **Minimal API, без контроллеров.** `Program.cs` мапит `POST /command` (+ `/health`, `/alive` из ServiceDefaults, OpenAPI в dev).
- **Собственный CQRS, без MediatR.** Маркеры `ICommand`/`IQuery<T>`; хендлеры с `GetContext`; диспетчер `IHandler` открывает scope, Activity, автоматически оборачивает RW-команды в транзакцию. `src/PuzzleServer.Infrastructure.App/CQRS/*`.
- **Result-монада вместо исключений.** `Result`/`Result<T>` (`abstract record`), `Bind/Map/Match/Apply` + async + `CollBind`. `src/PuzzleServer.Infrastructure.App/Result.cs` (~570 строк).
- **Attribute-driven DI с авто-регистрацией.** `[InjectAsScoped/Singleton/Transient]` + `[Config]` для POCO-настроек; `services.AutoRegistration(Assembly)` сканирует сборку. `BackgroundService` с `[InjectAsSingleton]` запускается хостом сам. `src/PuzzleServer.Infrastructure.App/DI/*`.
- **Модульная композиция.** Статические `ModuleExtensions.Add<Module>(services, configuration)` (миграции + AutoRegistration), цепочка из `Api/Program.cs`.
- **Typed IDs.** `readonly record struct` над `Guid` с типом в байте (`Ids.TypeRange`), JSON-конвертеры. `src/PuzzleServer.Infrastructure.App/Ids.cs`, `TypedId/*`.
- **Сквозной контекст операции.** `Contexts/Context.cs` — scoped AsyncLocal контейнер.
- Bus + Outbox для междоменных сайд-эффектов (для AdminPanel избыточно).

## 3. Ключевые подходы из docs/

`/Users/demakaev/ZCodeProject/Puzzle/docs/` — 16 документов + индекс `01-infrastructure.md`. Каждый: «Кратко: как пользоваться» → детали → чек-лист «добавить новое» → «Грабли». Нумерация `01.NN-name.md`.

Полезно для AdminPanel: `01.01-di.md` (attribute-DI, `[Config]`), `01.03-cqrs.md`, `01.04-db.md` (Npgsql+Dapper фабрики), `01.05-migrations.md` (DbUp), `01.06-retry.md` (Polly с jitter), `01.07-traces.md`, `01.08-result.md`, `01.12-health-checks.md` (`HealthCheckAbstract<T>` для hosted-сервисов). Bus/Outbox/Kafka/Audit/TextServer — по потребности (не нужны).

Первый документ для чтения: `/Users/demakaev/ZCodeProject/Puzzle/AGENTS.md` (architecture rules и conventions).

## 4. Фронтенд

**Фронтенда в Puzzle нет.** Чистый JSON/plain-text API. Для AdminPanel паттерн фронта взять неоткуда — только паттерн хоста Minimal API + OpenAPI (`AddOpenApi()`/`MapOpenApi()` в dev).

## 5. Аутентификация

**Аутентификации в Puzzle нет.** Только `[Config]`-POCO для чтения настроек и образец `Account.MakePasswordHash` (доменная логика, не админ-аутентификация). Cookie/JWT для AdminPanel проектировать с нуля.

## 6. Docker / локальная разработка

Dockerfile и docker-compose отсутствуют. Всё через **Aspire AppHost** (`src/PuzzleServer.Api.AppHost/AppHost.cs`): Postgres 18-alpine :6000, pgAdmin, Kafka; connection strings приходят по ключам `ConnectionStrings:*`. Запуск: `dotnet run --project src/PuzzleServer.Api.AppHost`. API: `http://localhost:5172`.

## 7. Directory.Packages.props / Directory.Build.props

- `src/Directory.Build.props`: `LangVersion=latest`, `TargetFramework=net10.0`, `ImplicitUsings=enable`, `Nullable=enable`, `IsPackable=false`. (В AdminPanel добавить `TreatWarningsAsErrors=true`.)
- `src/Directory.Packages.props`: CPM (`ManagePackageVersionsCentrally=true`). Версии: Aspire.Hosting* 13.4.3, Npgsql 10.0.3, Dapper 2.1.79, dbup-postgresql 7.0.1, Polly 8.7.0, OpenTelemetry 1.16.x, Microsoft.Extensions.* 10.0.x; тесты: xunit.v3 3.2.2, Testcontainers.PostgreSql 4.12.0, Testcontainers.Xunit 4.10.0, FluentAssertions 7.2.1, AutoFixture 4.18.1, Microsoft.NET.Test.Sdk 18.6.0, coverlet.collector 10.0.1.
- `src/NuGet.Config`: nuget.org + packageSourceMapping. `src/.editorconfig`: UTF-8, LF, 4 пробела, `var` везде, `I`-префиксы.

## 8. Тестирование

**xunit v3** + FluentAssertions + AutoFixture; **Testcontainers** (postgres:18, tmpfs, случайный порт). Unit-проект: фикстуры по подсистемам (`IAsyncLifetime`), тесты через DI с `services.AutoRegistration(...)`. Integration-проект: реальное приложение через `Aspire.Hosting.Testing` (`DistributedApplicationTestingBuilder`). Primary constructor для инъекции фикстуры: `public class UserTests(CommandRunner runner) : IClassFixture<CommandRunner>`. `InternalsVisibleTo` в Infrastructure.App.

## 9. Код, который стоит скопировать (пути)

- `src/PuzzleServer.Infrastructure.App/Result.cs` — Result-монада.
- `src/PuzzleServer.Infrastructure.App/DI/` — вся авторегистрация.
- `src/PuzzleServer.Infrastructure.App/CQRS/` — интерфейсы и исполнители.
- `src/PuzzleServer.Infrastructure.App/Contexts/` — Context/ContextManager.
- `src/PuzzleServer.Infrastructure.App/DB/` — фабрики, контексты, executors, DbQueries.
- `src/PuzzleServer.Infrastructure.App/Migrations/MigrationRunner.cs` — DbUp-раннер.
- `src/PuzzleServer.Infrastructure.App/Traces/Tracing.cs`, `Retry/`, `HealthChecks/`, `Notifications/`.
- `src/PuzzleServer.Api/Program.cs` — образец композиции; `src/PuzzleServer.Api.ServiceDefaults/Extensions.cs` — Aspire-дефолты.
- Мета: `src/Directory.Build.props`, `src/Directory.Packages.props`, `src/NuGet.Config`, `src/.editorconfig`, `src/PuzzleServer.Api.slnx`, `src/aspire.config.json`.

## 10. Прочее

`arch/` в Puzzle — один ADR `bus.md`. `docs/superpowers/` — брейншторм-документы процесса (концепция «история задач в docs/superpowers»).

## Главные выводы для AdminPanel

1. Переиспользовать каркас: Minimal API host + модульная композиция + attribute-DI + CQRS (`IHandler`) + `Result`-монада + Npgsql/Dapper + Aspire AppHost для локальной среды + Aspire.Hosting.Testing + CPM в `Directory.Packages.props` + .slnx + docs-стиль.
2. Чего нет в референсе: аутентификация (cookie/JWT из настроек), фронтенд и встраивание в .NET, Dockerfile/compose, `TreatWarningsAsErrors`.
3. Перенести формат документации: индекс `docs/NN-name.md` + документы с секциями «как пользоваться / чек-лист / грабли»; conventions из AGENTS.md Puzzle (file-scoped namespaces, `var`, primary constructors).
