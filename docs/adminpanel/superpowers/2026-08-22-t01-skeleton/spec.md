# Спецификация t01-skeleton — скелет решения AdminPanel

Дата: 2026-08-22. Фаза dev-flow: spec. Источники истины: `arch/roadmap/infra.md`
(пункт `t01-skeleton`), `arch/01-architecture.md` (главный), `arch/02-etcd-contract.md`
и `arch/03-panels.md` (ограничители), референс `../Puzzle` (копировать, адаптируя).

## 1. Цель

Создать компилирующийся зелёный скелет решения AdminPanel: `.slnx`, проекты по
`arch/01` §2, общие мета-файлы (`Directory.Build.props`, `Directory.Packages.props`,
`NuGet.Config`, `.editorconfig`), каркас хоста с `GET /api/healthz`, переносы из
Puzzle в `Infrastructure` (`Result`, attribute-DI, CQRS-queries, health-check
базис), тестовые проекты со смоук-покрытием каркаса, `.gitignore` и начальный
`README.md`. Результат: `dotnet build` и `dotnet test` зелёные с
`TreatWarningsAsErrors=true`; пустой API отвечает на `/api/healthz`.

Скелет не содержит бизнес-логики (снапшот, парсеры, auth, etcd-клиент — задачи
`t02+`). Каждый элемент скелета — либо мета-файл, либо перенос из референса,
либо заглушка-модуль для будущих задач.

## 2. Принципы

- Источник истины — `arch/`; всё, что ниже не оговорено arch/, решено в рамках
  его контракта минимальным способом. Расхождение с arch/ запрещено
  (SPEC_DEVIATION).
- Копирование из `../Puzzle` разрешено и поощряется; адаптация = namespace
  `AdminPanel.*` + обрезка до read-only-потребностей + доведение до нуля
  warning'ов (у нас `TreatWarningsAsErrors=true`, в референсе — нет).
- Идентификаторы — английские; комментарии в коде — русские.
- Тесты — xunit v3 + FluentAssertions, комментарии по нотации AAA
  (`// Arrange` / `// Act` / `// Assert`), на русском.
- YAGNI: ничего, что не требуется arch/ на этом этапе (Bus/Outbox/миграции/
  Npgsql/Testcontainers/OTel-пакеты/фронтенд/Dockerfile — не входят).
- Новые зависимости — только через CPM (`Directory.Packages.props`).

## 3. Решения в рамках контракта arch/ (уточнения неоднозначностей)

1. **Тесты в `src/tests/`**, а не в корне репо: единый `src/Directory.Build.props`
   и `src/Directory.Packages.props` покрывают всё решение без дублирования (так
   же устроен референс: тесты внутри `src/`). Имена проектов —
   `AdminPanel.UnitTests` / `AdminPanel.IntegrationTests` (arch/01 §2).
2. **Проект ServiceDefaults не создаётся**: его нет в таблице проектов arch/01
   §2. OTel-пакеты (`OpenTelemetry.*`) в скелет не тянутся; переносится только
   `Tracing.cs` (чистый BCL `System.Diagnostics`) как задел под будущие задачи.
3. **CQRS обрезан до query-ветки** (roadmap: «команды не заводить»):
   `IQuery<T>`, `IQueryHandler<TQ,TR>`, диспетчер `IHandler.HandleQuery`.
   `IHandlerBase.GetContext`/DB-контексты/транзакции/interceptor-перегрузки не
   переносятся — БД у панели нет.
4. **`UseDiBehaviours` живёт в `AdminPanel.Infrastructure`** (в Puzzle — в Api,
   из-за доменных проверок): и Api, и UnitTests используют один и тот же
   способ включения DI-поведений.
5. **`/api/healthz` реализуется через `MapHealthChecks`** с кастомным
   response-writer'ом (`{"status":"ok"}` при Healthy): единая точка здоровья, в
   которую будущие проверки (refresher) добавляются автоматически, тело
   соответствует arch/03 §1.
6. **Пакеты-кандидаты** (Npgsql, Testcontainers, Dapper и т.п.) в
   `Directory.Packages.props` заранее не включаются: в скелете нет кода, их
   использующего. Каждая задача `t02+` добавляет свой `PackageVersion` с
   версией от референса (arch/01 §2: «версии стартуют от референса»). Это не
   сужение arch/, а применение YAGNI к «кандидатам».
7. **`InternalsVisibleTo` не переносится**: тесты каркаса идут через публичный
   API (`IHandler`, `Result`, `IServiceProvider`), internal-классы
   (`Handler`) в прямом доступе тестам не нужны.
8. **launchSettings.json** фиксирует `http://localhost:5000` — адрес из
   arch/01 §5 (vite dev-прокси будет вести на него).

## 4. Состав репозитория (дерево создаваемых файлов)

```
/ (корень репо AdminPanel)
├── .gitignore                          [новый] VS-набор (из Puzzle) + node + .dev-flow/ + .DS_Store
├── README.md                           [новый] начальный: что это, сборка, запуск, структура
└── src/
    ├── AdminPanel.slnx                 [новый] решение (.slnx)
    ├── Directory.Build.props           [новый] по образцу Puzzle + TreatWarningsAsErrors
    ├── Directory.Packages.props        [новый] CPM; версии из референса
    ├── NuGet.Config                    [копия] nuget.org + packageSourceMapping
    ├── .editorconfig                   [копия] из Puzzle как есть
    ├── AdminPanel.Api/
    │   ├── AdminPanel.Api.csproj       Sdk.Web; ref: Infrastructure, Core, Etcd, Probes
    │   ├── Program.cs                  модульная композиция + /api/healthz
    │   ├── HealthzWriter.cs            JSON-writer {"status":"ok"} для health-check
    │   ├── appsettings.json            Logging/AllowedHosts (как в Puzzle)
    │   ├── appsettings.Development.json
    │   └── Properties/launchSettings.json   http://localhost:5000
    ├── AdminPanel.Core/
    │   ├── AdminPanel.Core.csproj      ref: Infrastructure
    │   └── ModuleExtensions.cs         AddCore() → AutoRegistration
    ├── AdminPanel.Etcd/
    │   ├── AdminPanel.Etcd.csproj      ref: Core
    │   └── ModuleExtensions.cs         AddEtcd() → AutoRegistration
    ├── AdminPanel.Probes/
    │   ├── AdminPanel.Probes.csproj    ref: Core
    │   └── ModuleExtensions.cs         AddProbes() → AutoRegistration
    ├── AdminPanel.Infrastructure/
    │   ├── AdminPanel.Infrastructure.csproj
    │   ├── Result.cs                   [копия] монада Result/Result<T> + extensions
    │   ├── ModuleExtensions.cs         [адапт] AddInfrastructure() → AutoRegistration
    │   ├── DI/                         [копия] 7 файлов attribute-DI + [новый] UseDiBehavioursExtensions.cs
    │   ├── CQRS/                       [адапт] 3 файла query-only
    │   ├── Traces/Tracing.cs           [копия] ActivitySource-хелперы (BCL)
    │   ├── Contexts/ServiceProviderHelper.cs [копия] IsGlobal для scope-логики Handler
    │   └── HealthChecks/               [копия] IHealthCheckService + HealthCheckAbstract<T>
    └── tests/
        ├── AdminPanel.UnitTests/
        │   ├── AdminPanel.UnitTests.csproj
        │   ├── ResultTests.cs
        │   ├── AutoRegistrationTests.cs
        │   └── CQRSTests.cs
        └── AdminPanel.IntegrationTests/
            ├── AdminPanel.IntegrationTests.csproj
            └── HealthzTests.cs
```

Существующее (не трогается): `AGENTS.md`, `arch/`, `docs/`, `.dev-flow/`.

## 5. Мета-файлы

### 5.1. `src/Directory.Build.props`

Как в Puzzle плюс строгий режим (roadmap):

```xml
<Project>
    <PropertyGroup>
        <LangVersion>latest</LangVersion>
        <TargetFramework>net10.0</TargetFramework>
        <ImplicitUsings>enable</ImplicitUsings>
        <Nullable>enable</Nullable>
        <TreatWarningsAsErrors>true</TreatWarningsAsErrors>
        <IsPackable>false</IsPackable>
    </PropertyGroup>
</Project>
```

### 5.2. `src/Directory.Packages.props` (CPM)

`ManagePackageVersionsCentrally=true`, `EnablePackageVersionOverride=false`.
Версии — от референса (если конкретный patch недоступен в nuget.org на момент
реализации, берётся ближайшая доступная в той же minor-линейке; фиксируется в
этот же файл без изменения спеки):

| PackageVersion | Версия |
|---|---|
| `coverlet.collector` | 10.0.1 |
| `FluentAssertions` | 7.2.1 |
| `Microsoft.AspNetCore.Mvc.Testing` | 10.0.9 |
| `Microsoft.AspNetCore.OpenApi` | 10.0.9 |
| `Microsoft.Extensions.Configuration` | 10.0.9 |
| `Microsoft.Extensions.Configuration.Abstractions` | 10.0.9 |
| `Microsoft.Extensions.DependencyInjection` | 10.0.9 |
| `Microsoft.Extensions.DependencyInjection.Abstractions` | 10.0.9 |
| `Microsoft.Extensions.Diagnostics.HealthChecks.Abstractions` | 10.0.9 |
| `Microsoft.Extensions.Options.ConfigurationExtensions` | 10.0.0 |
| `Microsoft.NET.Test.Sdk` | 18.6.0 |
| `xunit.runner.visualstudio` | 3.1.5 |
| `xunit.v3` | 3.2.2 |

Распределение по проектам:
- `Infrastructure`: Configuration.Abstractions, DependencyInjection.Abstractions,
  Options.ConfigurationExtensions, Diagnostics.HealthChecks.Abstractions.
- `Api`: Microsoft.AspNetCore.OpenApi (health-checks — из shared framework).
- `UnitTests`: FluentAssertions, Configuration, DependencyInjection,
  DependencyInjection.Abstractions, xunit.v3, xunit.runner.visualstudio,
  Microsoft.NET.Test.Sdk, coverlet.collector.
- `IntegrationTests`: Microsoft.AspNetCore.Mvc.Testing, FluentAssertions,
  xunit.v3, xunit.runner.visualstudio, Microsoft.NET.Test.Sdk, coverlet.collector.

### 5.3. `src/NuGet.Config`

Копия Puzzle: source `nuget.org`, `packageSourceMapping` с паттерном `*` на
nuget.org.

### 5.4. `src/.editorconfig`

Копия Puzzle как есть (UTF-8, LF, 4 пробела, `var` везде, `I`-префиксы
интерфейсов, camelCase для JSON/JS — 2 пробела).

### 5.5. `src/AdminPanel.slnx`

Формат как у Puzzle, папки: `/common/` (Directory.Build.props,
Directory.Packages.props), `/infrastructure/` (Infrastructure), `/core/` (Core),
`/etcd/` (Etcd), `/probes/` (Probes), `/tests/` (UnitTests, IntegrationTests);
`AdminPanel.Api` — в корне решения.

### 5.6. `.gitignore` (корень)

Стандартный VS-набор из Puzzle (`bin/`, `obj/`, `.vs/`, `*.user`, …) плюс:
`.DS_Store`, `node_modules/`, `dist/`, строка `.dev-flow/` (уже есть —
сохранить).

### 5.7. `README.md` (корень, начальный)

Разделы: что это (read-only панель инспекции шардированных HA-кластеров PG,
ссылки на `arch/`), статус (скелет, t01), стек (.NET 10, Minimal API, CPM,
.slnx), структура каталогов, команды (`dotnet build src/AdminPanel.slnx`,
`dotnet test src/AdminPanel.slnx`, `dotnet run --project src/AdminPanel.Api`,
curl healthz). Полный README — задача `t11-finalize`.

## 6. `AdminPanel.Infrastructure` — переносы из Puzzle

Источник: `/Users/demakaev/ZCodeProject/Puzzle/src/PuzzleServer.Infrastructure.App/`.
Единая адаптация для всех файлов: namespace `PuzzleServer.Infrastructure.App*`
→ `AdminPanel.Infrastructure*`, usings соответственно; русские комментарии в
местах, где комментарии добавляются; код доводится до нуля warning'ов
(null-forgiveness и т.п.) — правки минимальные, семантика не меняется.

### 6.1. `Result.cs` — копия целиком (~570 строк)

`Result`/`Result<T>`, `ResultSuccess`/`ResultError`, `ResultExtensions`
(extension-блоки над `ValueTask<Result*>`), `CollBind`/`CollBindAsync`.
Зависимости — только BCL. Namespace: `AdminPanel.Infrastructure`.

### 6.2. `DI/` — копия целиком (7 файлов)

- `InjectAs.cs` — `InjectAsAttribute(lifetime, params interfaces)` +
  `InjectAsSingletonAttribute` / `InjectAsScopedAttribute` /
  `InjectAsTransientAttribute`.
- `ConfigAttribute.cs` — `[Config(name?)]` для POCO-настроек.
- `DiTypeBehaviour.cs` — базовый сканер типов (Filter/Handle, GetAttribute
  через `Result.FromValue`).
- `AutoRegistrationDiTypeBehaviour.cs` — регистрация класса + всех его
  интерфейсов (для generic-типов — через `GetGenericTypeDefinition`).
- `AutoRegistrationConfigDiTypeBehaviour.cs` — `services.Configure<T>(
  configuration.GetSection(name))` для `[Config]`-POCO.
- `ServiceCollectionExtensions.cs` — `AutoRegistration(...)` по сборкам,
  статический набор поведений (`UseBehaviour`).
- `ServiceProviderExtensions.cs` — `GetService<T>(Type)` через `Result`,
  `InvalidTypeException`.

Плюс один новый файл `UseDiBehavioursExtensions.cs` (перенос идеи
`DiBehavioursExtensions` из Api-проекта Puzzle, без доменных проверок):

```csharp
// Включает DI-поведения авто-регистрации для сборок, передаваемых в AutoRegistration.
public static class UseDiBehavioursExtensions
{
    public static IServiceCollection UseDiBehaviours(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        new AutoRegistrationDiTypeBehaviour(services).UseBehaviour();
        new AutoRegistrationConfigDiTypeBehaviour(services, configuration).UseBehaviour();
        return services;
    }
}
```

Namespace `AdminPanel.Infrastructure.DI`.

### 6.3. `CQRS/` — адаптация до query-only (3 файла)

- `IQuery.cs` — `public interface IQuery<T>;` (копия).
- `IQueryHandler.cs` — адаптация: без `IHandlerBase` и `GetContext` (нет БД):

```csharp
namespace AdminPanel.Infrastructure.CQRS;

// Хендлер запроса: чистое чтение, без транзакций и контекста БД.
public interface IQueryHandler<in TQ, TR>
    where TQ : IQuery<TR>
{
    ValueTask<Result<TR>> Handle(TQ query, CancellationToken ct);
}
```

- `IHandler.cs` — адаптация `Handler` из Puzzle: только `HandleQuery<Q,T>`;
  сохранены открытие scope (через `IServiceProviderHelper.IsGlobal`) и Activity
  (через `Tracing`); interceptor-перегрузки и executor-фабрики не переносятся
  (YAGNI):

```csharp
public interface IHandler
{
    ValueTask<Result<T>> HandleQuery<Q, T>(Q query, CancellationToken ct)
        where Q : IQuery<T>;
}

[InjectAsTransient]
internal class Handler(IServiceProviderHelper spHelper, IServiceProvider sp) : IHandler
{
    public async ValueTask<Result<T>> HandleQuery<Q, T>(Q query, CancellationToken ct)
        where Q : IQuery<T>
    {
        Result<T> result = null!;
        await Tracing.ActivityT(
            TypeName<Q>(),
            ActivityKind.Server,
            () => Run(async isp =>
            {
                var handler = isp.GetRequiredService<IQueryHandler<Q, T>>();
                result = await handler.Handle(query, ct);
            }));
        return result;
    }
    // Run/TypeName — как в Puzzle: scope открывается, если провайдер глобальный.
}
```

`ICommand`/`ICommandHandler` и RW/Ro-исполнители не переносятся (команды
заведёны не будут — roadmap).

### 6.4. `Traces/Tracing.cs` — копия

`ActivitySource`-хелперы (`Init`, `Activity`, `ActivityT`, `ActivityVT`).
Чистый BCL, OTel-пакеты не нужны.

### 6.5. `Contexts/ServiceProviderHelper.cs` — копия

`IServiceProviderHelper.IsGlobal(IServiceProvider)` + реализация
`[InjectAsSingleton]`. Полные `Context`/`ContextManager` (Notifications, DB,
Audit) не переносятся.

### 6.6. `HealthChecks/` — копия (2 файла)

`IHealthCheckService` (Inited/Working/StatusError) и
`HealthCheckAbstract<T> : IHealthCheck` — базис для health-проверок будущих
hosted-сервисов (SnapshotRefresher). Пакет:
`Microsoft.Extensions.Diagnostics.HealthChecks.Abstractions`.

### 6.7. `ModuleExtensions.cs` — адаптация

```csharp
public static class ModuleExtensions
{
    private static Assembly Assembly => typeof(ModuleExtensions).Assembly;

    public static IServiceCollection AddInfrastructure(this IServiceCollection services)
        => services.AutoRegistration(Assembly);
}
```

Не переносятся: `DB/`, `Migrations/`, `Retry/`, `Notifications/`, `Ids.cs`,
`TypedId/`, `IdTypeRange.cs`, `UserId.cs`, `JsonExtensions.cs`, `Functional/`,
`Audit/Bus/Outbox/Kafka` (отдельных проектов у панели нет вовсе).

## 7. Пустые доменные проекты

`Core` (ref Infrastructure), `Etcd` (ref Core), `Probes` (ref Core) —
направления зависимостей по arch/01 §1. Каждый содержит один
`ModuleExtensions.cs` (`AddCore`/`AddEtcd`/`AddProbes` →
`services.AutoRegistration(Assembly)`), namespace
`AdminPanel.Core`/`AdminPanel.Etcd`/`AdminPanel.Probes`. Никаких типов в них
не заводится: их наполнение — задачи `t02+` (Etcd — gateway/парсеры/refresher,
Probes — Patroni/SQL-пробы, Core — модель снапшота/AlertEngine).

## 8. `AdminPanel.Api`

### 8.1. `Program.cs` (модульная композиция)

```csharp
using AdminPanel.Api;
using AdminPanel.Core;
using AdminPanel.Etcd;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.DI;
using AdminPanel.Probes;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.Diagnostics.HealthChecks;

// Точка входа панели: сборка хоста и модульная композиция сервисов.
var builder = WebApplication.CreateBuilder(args);

builder
   .Services.UseDiBehaviours(builder.Configuration)
   .AddInfrastructure()
   .AddCore()
   .AddEtcd()
   .AddProbes()
   .AddOpenApi()
   .AddHealthChecks()
   .AddCheck("self", () => HealthCheckResult.Healthy(), ["live"]);

var app = builder.Build();

// OpenAPI-схема — только в dev-окружении.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

// Живость самой панели; без авторизации (auth-модуль — t02).
app.MapHealthChecks(
    "/api/healthz",
    new HealthCheckOptions { ResponseWriter = HealthzWriter.WriteStatus });

app.Run();

// Экспозиция точки входа для WebApplicationFactory в интеграционных тестах.
public partial class Program;
```

### 8.2. `HealthzWriter.cs`

Статический класс, `WriteStatus(HttpContext, HealthReport)`: пишет
`application/json` c телом `{"status":"ok"}` при `Healthy`, `"degraded"` /
`"unhealthy"` иначе (сериализация anonymous-объекта через
`System.Text.Json`). Компактный JSON, camelCase.

### 8.3. `appsettings.json` / `appsettings.Development.json`

Как в Puzzle: `Logging:LogLevel` (Default=Information,
Microsoft.AspNetCore=Warning), `AllowedHosts: "*"`. Секции `AdminPanel:*` не
заводятся — их вводят задачи, которым они нужны (t02 Auth, t03 Etcd, …).

### 8.4. `Properties/launchSettings.json`

Профиль `http`: `applicationUrl=http://localhost:5000` (адрес vite-прокси из
arch/01 §5), `ASPNETCORE_ENVIRONMENT=Development`.

## 9. Тесты

Комментарии в тестах — русские, по нотации AAA.

### 9.1. `AdminPanel.UnitTests` (без Docker)

Общий helper `TestHost` (static, `Lazy<ServiceCollection>`): единожды на тестовую
сборку выполняет `UseDiBehaviours(in-memory IConfiguration)` +
`AutoRegistration(тестовая сборка)`; каждый тест строит свой
`BuildServiceProvider()` из общей коллекции. Причина: `ServiceCollectionExtensions`
в референсе кеширует просканированные сборки в статическом состоянии — повторный
вызов `AutoRegistration` для той же сборки типы не обрабатывает, поэтому скан
должен выполняться ровно один раз на процесс (заметка и для будущих задач).

- `ResultTests.cs` (не требует TestHost — чистые типы):
  - success: `Result.Success().IsSuccess` истинно; `Match` выбирает
    success-ветку;
  - failure: `Result.Failed(ex)` — `IsSuccess` ложно, `Match` возвращает
    ошибку;
  - цепочка `Bind`/`Map`: значение трансформируется на success и
    протаскивается на failure;
  - `Result<T>.From` с бросающим исключение делегатом возвращает ошибку;
  - `CollBind` останавливается на первом неуспехе.
- `AutoRegistrationTests.cs` (через TestHost):
  - класс с `[InjectAsSingleton]` (и интерфейсом) резолвится из построенного
    провайдера по самостоятельному типу и по интерфейсу;
  - `[Config]`-POCO биндится из in-memory `IConfiguration`
    (`AddInMemoryCollection`) через `services.Configure<T>` + `IOptions<T>.Value`
    (in-memory конфиг зашит в TestHost).
- `CQRSTests.cs` (через TestHost):
  - тестовый `TestQuery : IQuery<string>` и
    `TestQueryHandler : IQueryHandler<TestQuery, string>` c `[InjectAsScoped]`;
  - из корневого (singleton) провайдера `IHandler.HandleQuery` возвращает
    success со значением хендлера — заодно проверяет открытие scope
    диспетчером.

### 9.2. `AdminPanel.IntegrationTests` (без Docker)

- `HealthzTests.cs`: `WebApplicationFactory<Program>` → `GET /api/healthz` →
  200, тело десериализуется в JSON с полем `status = "ok"`.

## 10. Ограничения (что НЕ делается)

- Никаких эндпоинтов, кроме `GET /api/healthz` (+ OpenAPI-документ в dev).
- Никакой бизнес-логики: Etcd/Probes/Core пусты; auth отсутствует; модели
  снапшота не заводятся.
- Не переносятся: DB/Dapper/DbUp/миграции, Retry/Polly, OTel-пакеты, Aspire
  (AppHost/ServiceDefaults/aspire.config.json), Bus/Outbox/Kafka/Audit,
  TypedId/Ids, Testcontainers, Dockerfile/compose, frontend.
- `arch/` и AGENTS.md не меняются (кроме roadmap-деливерабла, см. §11).

## 11. Деливерабл roadmap

Тем же мерж-коммитом удалить пункт `t01-skeleton` (строку) из
`arch/roadmap/infra.md`. Зависимость `t02-auth ← t01-skeleton` в
`←`-нотации других пунктов не трогать (по правилам `arch/roadmap/README.md` —
удаляется только строка-пункт; упоминания в зависимостях не очищаются).

## 12. Критерии приёмки

1. `dotnet build src/AdminPanel.slnx` — успех, 0 warnings (warnings как
   ошибки включены и не подавлены).
2. `dotnet test src/AdminPanel.slnx` — все тесты зелёные; Docker не требуется.
3. `dotnet run --project src/AdminPanel.Api` (или с `--launch-profile http`) →
   `curl -s http://localhost:5000/api/healthz` → HTTP 200, тело
   `{"status":"ok"}`.
4. Состав проектов соответствует arch/01 §2: Api, Core, Etcd, Probes,
   Infrastructure, tests/UnitTests, tests/IntegrationTests — и ничего сверх.
5. `grep TreatWarningsAsErrors src/Directory.Build.props` присутствует;
   версии всех пакетов централизованы (`grep -r 'PackageReference' src --include='*.csproj'`
   не возвращает Version-атрибутов).
6. Пункт `t01-skeleton` отсутствует в `arch/roadmap/infra.md`.
7. Мутаций arch/01–03 нет; все решения §3 не противоречат arch (проверка на
   ревью).

## 13. Риски и заметки

- В Puzzle `TreatWarningsAsErrors` не включён: переносимый код может
  давать nullable-warning'и — правится локально (null-forgiveness, явные
  аннотации) без изменения семантики; это единственный ожидаемый источник
  правок при копировании.
- `ServiceCollectionExtensions` хранит behaviors в статическом состоянии — как
  в референсе; для тестов в одном процессе это безопасно (поведения
  идемпотентно добавляются).
- Версии `Microsoft.AspNetCore.Mvc.Testing` и прочих 10.0.x-патчей сверяются с
  nuget.org при первом restore; правило отката — §5.2.
- `public partial class Program;` требует C# 12+ — обеспечено
  `LangVersion=latest` на net10.0.
