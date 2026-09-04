# План: t04-unified-metrics — единое решение Prometheus-метрик (каркас + инструментация + хранение)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Цель:** единый каркас Prometheus-метрик на OpenTelemetry (база в Puzzle — канон, порт `src/Shared.Metrics` + воркер-надстройка в монорепо), инструментация PgWorker/KafkaWorker/AdminPanel по словарю arch/18 §2 и профиль `metrics` dev-станда (Prometheus + Grafana + Alertmanager + rules + дашборды + e2e-чек).

**Архитектура:** пассивные наблюдатели — `System.Diagnostics.Metrics` (Meter API .NET 10) + официальный `OpenTelemetry.Exporter.Prometheus.AspNetCore`; экспозиция `/metrics` на том же Kestrel-порту, что `/healthz`, без ApiKey/cookie. Воркер-серии по единому словарю arch/18 §2.2 (оба воркера пишут в одни имена), источник серий — марк-методы `WorkerMetricsInstrumentation` + подписка на фазовые записи журнала работы; kafka-домен — фоновый коллектор через `IKafkaAdminClientFactory`; PG-репликация — scrape Patroni-эмуляторов `:8008` напрямую. Стек хранения — compose-профиль `metrics` dev-станда, конфиги в репо.

**Тех-стек:** .NET 10 (`net10.0`, `Nullable=enable`, `TreatWarningsAsErrors=true`), OpenTelemetry (пин CPM в `Directory.Packages.props` обоих репо), xunit v3 + `Microsoft.AspNetCore.Mvc.Testing`, docker compose (prometheus/grafana/alertmanager), Prometheus text format.

**Spec:** `docs/superpowers/2026-09-04-t04-unified-metrics/spec.md` (рядом); канон контракта — `arch/18-metrics.md`. План спорит от спеки: исполнитель читает спеку + arch/18 §2 (словарь имён) перед каждым этапом.

## Глобальные ограничения (из spec §2 и AGENTS)

- Работаем ТОЛЬКО в worktree `/Users/demakaev/ZCodeProject/worktrees/feat-t04-unified-metrics` (кроме фазы Ф1 — репо `../Puzzle`).
- .NET 10, `LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true`; сборка — 0 warnings.
- Централизованное версионирование пакетов (CPM, `EnablePackageVersionOverride=false`): версии только в `Directory.Packages.props` (монорепо: `src/Directory.Packages.props`; Puzzle: `../Puzzle/src/Directory.Packages.props`).
- Пины OTel-пакетов (согласованы с уже стоящими в Puzzle `OpenTelemetry.Extensions.Hosting 1.16.0`):
  - `OpenTelemetry` 1.16.0; `OpenTelemetry.Extensions.Hosting` 1.16.0 (только монорепо);
  - `OpenTelemetry.Exporter.Prometheus.AspNetCore` 1.16.0-beta.1 (у экспортёра стабильных релизов нет — официальный пакет OTel в beta-канале; риск M3 закрывает интеграционный тест имён Ф2-Т3);
  - `OpenTelemetry.Instrumentation.AspNetCore` 1.15.2; `OpenTelemetry.Instrumentation.Runtime` 1.15.1;
  - для тестов: `Microsoft.AspNetCore.Mvc.Testing` 10.0.9 (в монорепо уже пин; в Puzzle добавить).
- Язык: комментарии/доки — русские, идентификаторы — английские; тесты — с AAA-комментариями (`// Arrange`, `// Act`, `// Assert`).
- Метрики — пассивные наблюдатели: инструменты и коллектор не бросают исключений наружу, не блокируют циклы; ошибка сбора — лог + собственная метрика свежести, без ретраев (spec §2, arch/18 §9 M2).
- `/metrics` открыт без ApiKey/cookie в доверенной docker-сети (symmetрия `/healthz`); `ApiKeyMiddleware`/guard панели не менять.
- Тестовые порты docker — динамические (никаких хардкод-хост-портов в тестах); порты стенда 9090/3000/9093 — фиксированные публикации с env-override `METRICS_*_PORT` (AGENTS, spec §2).
- Словарь имён — arch/18 §2; новые имена/значения лейблов — только правкой arch/18 тем же коммитом («канон = факт»).
- Коммит — частый, после каждого зелёного шага, в ветке worktree (кроме Ф1 — правила Puzzle). Стиль: `t04: ...`, `feat(metrics): ...` — по образцу `git log`.
- Фаза Ф1 трогает отдельное репо `../Puzzle` (ветка + коммит по правилам Puzzle, remote — GitLab); уведомить main-агента перед её началом.

## Правки по итогам ревью Фазы 4 (учтены в этом плане и в контрактах)

1. Терминальные фазы журнала выведены из фактического словаря (grep по обоим воркерам): `{done, failed, crashed, rejected, cancelled}`; `rejected` — терминальная (MoveProcess.cs:958, AbortSequence.cs:378, TopicSyncProcess.cs:295). `skipped` — промежуточная (AdoptionProcess.cs:128 → далее обязательно `done`:180/`failed`:488), в терминальные НЕ входит — иначе задвоится операция и порвётся живая фаза.
2. Ops без терминальной фазы — `supervise` (стационарные записи, часть через `WriteSupervisionAsync` мимо фазового события) и `evacuate` (только `waiting-*`/`blocked-moving`) — исключены из фазовых серий; живость надзора закрывает `WorkerLoopStalled`. Альтернатива (событие в `WriteSupervisionAsync`) отвергнута: надзор — не процесс с фазами, семантика «кластер в фазе supervise N секунд» бесполезна и даёт вечные серии.
3. Чек 65 стартует с гарантии живости kafkaworker (после 50-го он остановлен) и pgworker.
4. Контракт источника серий переформулирован (arch/18 §1, spec §3.2 — уже внесены в ревью-итерации; шаги Т2.2/Т2.3 сверяют и везут тем же коммитом).
5. Перечень значений лейбла `process` в arch/18 §2.2 приведён к фактическим `op` журналов (внесено); тест Т2.3 фиксирует значения лейблов.
6. Фильтр Active-кластеров коллектора: `Config.State == null` (KafkaDomain.cs:11–18, arch/15 §2.1).
7. Эмулятор экспортирует серию только своей ноды (`NODE_NAME`), не всех членов scope.
8. Гейдж панели — pull над `BuiltAtUtc` (обновляется только успешным тиком) — зафиксировано строкой в Т5.1.

Раунд 2 (все три замечания закрыты в контрактах этой итерацией; Т2.3/Т6.2 везут теми же коммитами):

1. [medium] Перечень лейбла `process` PgWorker дополнен фактическими `rollback` и `finalize` (MoveProcess.cs:592/691 — rollback, :755 — finalize; терминальные `done`:743/:894 и `rejected`:958 — `RejectAsync` принимает op-параметр, т.е. rejected корректно закрывает и их серии). Итог — 11 ops PgWorker в arch/18 §2.2 и тесте Т2.3-2.
2. [low] Отказ от fallback `host.docker.internal` отзеркален в контракты: spec §3.6 — fallback-формулировка убрана (DNS сети стенда `kafkaworker:8080`/`adminpanel:8080`, без fallback; «стенд части» честно виден как down-таргеты); arch/18 §5.2 — альтернатива «`host.docker.internal:8081`» устранена (порт не существует; хост-публикация kafkaworker — 8082, используется только чеками, не Prometheus'ом).
3. [low] arch/18 §2.2, «Смысл» `worker_operation_total`: пример больше не содержит `evacuate` — «(provision/deprovision/rotate/move/rollback/finalize/abort…; подавленные ops — supervise/evacuate — не считаются)» — согласовано с `SuppressedOps` и тестом `OnJournalPhase_SuppressedOps_EmitNoPhaseSeries`.

## Структура файлов (карта изменений)

```
../Puzzle (Ф1, отдельное репо — правила Puzzle)
  src/PuzzleServer.Infrastructure.App.Metrics/        — базовый модуль (канон)
    PuzzleServer.Infrastructure.App.Metrics.csproj
    MetricsOptions.cs                                  — [Config]-опции Enabled/Path
    MetricsModuleExtensions.cs                         — AddAppMetrics/MapAppMetrics
  docs/01.20-metrics.md                                — док модуля + инструкция подключения
  docs/01-infrastructure.md                            — +строка индекса
  src/Directory.Packages.props                         — +OTel-пакеты
  src/PuzzleServer.Api.slnx                            — +проект в /Infrastructure/
  src/PuzzleServer.UnitTests/Metrics/                  — unit-тесты модуля

worktree (Ф2–Ф7)
  src/Directory.Packages.props                         — +OTel-пакеты
  src/Shared.Metrics/                                  — порт базы + надстройка
    Shared.Metrics.csproj
    MetricsOptions.cs                                  — копия Puzzle (namespace Shared.Metrics)
    MetricsModuleExtensions.cs                         — копия Puzzle (namespace Shared.Metrics)
    Worker/WorkerMetricsInstrumentation.cs             — марк-методы + Observable-серии §2.2
  src/PgWorker.slnx                                    — +Shared.Metrics (/common/), +тесты
  src/tests/Shared.Metrics.UnitTests/                  — unit + WAF-интеграционные тесты
    Shared.Metrics.UnitTests.csproj
    WorkerMetricsInstrumentationTests.cs
    MetricsEndpointTests.cs                            — фиксация фактических OTel-имён/лейблов
  src/PgWorker.Etcd/Coordination/WorkJournal.cs        — событие PhaseWritten (seam фаз)
  src/KafkaWorker.Etcd/Coordination/WorkJournal.cs     — то же (метод WriteAsync; дубли t08)
  src/PgWorker.App/Program.cs                          — AddAppMetrics/MapAppMetrics + подписка
  src/PgWorker.App/Options.cs                          — секция PgWorker:Metrics
  src/PgWorker.App/appsettings.json                    — "Metrics": {...}
  src/PgWorker.App/Loops/{Reconcile,Keepalive,Snapshot}Loop.cs — LoopTick/LoopDuration/...
  src/KafkaWorker.App/Program.cs                       — +метрики + регистрация коллектора
  src/KafkaWorker.App/Options.cs                       — KafkaWorkerMetricsOptions
  src/KafkaWorker.App/KafkaMetricsCollector.cs         — hosted-сервис лагов/USR (§4)
  src/KafkaWorker.App/KafkaMetricsState.cs             — Observable-стейт коллектора
  src/KafkaWorker.App/Loops/*.cs                       — LoopTick/LoopDuration/...
  src/KafkaWorker.Provisioning/Kafka/IKafkaAdminClient.cs — +ListGroups/ListOffsets/ListConsumerGroupOffsets
  src/KafkaWorker.Provisioning/Kafka/KafkaAdminClient.cs  — Confluent-адаптер новых методов
  src/KafkaWorker.Etcd/Parsing/KafkaSnapshotParser.cs  — (только чтение — источник коллектора)
  src/AdminPanel.Api/AdminPanel.Api.csproj             — +ProjectReference Shared.Metrics
  src/AdminPanel.Api/Program.cs                        — AddAppMetrics/MapAppMetrics + гейдж refresher
  src/tests/PgWorker.IntegrationTests/Api/MetricsTests.cs (+MetricsApiFactory)
  src/tests/KafkaWorker.UnitTests/KafkaMetricsCollectorTests.cs (+фабрика фейков при нужде)
  src/tests/KafkaWorker.IntegrationTests/Api/MetricsTests.cs
  src/tests/AdminPanel.IntegrationTests/MetricsTests.cs
  dev-stand/adminpanel/docker-compose.yml              — +prometheus/grafana/alertmanager (профиль metrics)
  dev-stand/adminpanel/metrics/prometheus/prometheus.yml, rules.yml
  dev-stand/adminpanel/metrics/alertmanager/alertmanager.null.yml, alertmanager.webhook.yml, entrypoint.sh
  dev-stand/adminpanel/metrics/grafana/provisioning/datasources/prometheus.yml
  dev-stand/adminpanel/metrics/grafana/provisioning/dashboards/provider.yml
  dev-stand/adminpanel/metrics/grafana/dashboards/{workers,kafka,pg}.json
  dev-stand/adminpanel/sidecar/emulator.py             — +GET /metrics (pg_replica_lag_seconds, своя нода)
  dev-stand/adminpanel/checks/00-up.sh                 — +профиль metrics
  dev-stand/adminpanel/checks/65-metrics.sh            — e2e-чек мониторинга + алерт-симуляция
  dev-stand/adminpanel/.env.example                    — дефолты METRICS_*-env
  dev-stand/adminpanel/README.md                       — профиль metrics, порты, env
  arch/18-metrics.md, docs/superpowers/.../spec.md     — контракты ревью-итерации (везутся коммитом Ф2)
  arch/roadmap/pgworker.md, arch/roadmap/kafkaworker.md — удаление тегов t04 (мерж-гейт, Ф7)
```

---

## Фаза Ф1 — Puzzle: базовый модуль каркаса (канон)

Отдельное репо `../Puzzle`. Правила: ветка от актуального main Puzzle, коммит по правилам Puzzle (GitLab). Перед стартом фазы — сообщить main-агенту (трогаем ../Puzzle).

### Task 1.1: Каркас проекта + пакеты

**Files:**
- Create: `../Puzzle/src/PuzzleServer.Infrastructure.App.Metrics/PuzzleServer.Infrastructure.App.Metrics.csproj`
- Create: `../Puzzle/src/PuzzleServer.Infrastructure.App.Metrics/MetricsModuleExtensions.cs` (пустая заготовка)
- Modify: `../Puzzle/src/Directory.Packages.props`
- Modify: `../Puzzle/src/PuzzleServer.Api.slnx`

**Interfaces:**
- Produces: проект `PuzzleServer.Infrastructure.App.Metrics` (namespace `PuzzleServer.Infrastructure.App.Metrics`) в /Infrastructure/ слnx; пакеты-пины для Ф1-Т2.

- [ ] **Шаг 1: Пин версий в CPM**

В `../Puzzle/src/Directory.Packages.props` в `<ItemGroup>` добавить (по алфавиту):

```xml
    <PackageVersion Include="Microsoft.AspNetCore.Mvc.Testing" Version="10.0.9" />
    <PackageVersion Include="OpenTelemetry.Exporter.Prometheus.AspNetCore" Version="1.16.0-beta.1" />
```

(`OpenTelemetry.Extensions.Hosting` 1.16.0, `OpenTelemetry.Instrumentation.AspNetCore` 1.15.2, `OpenTelemetry.Instrumentation.Runtime` 1.15.1 в Puzzle уже запинены.)

- [ ] **Шаг 2: Каркас проекта**

`PuzzleServer.Infrastructure.App.Metrics.csproj` (по образцу соседних модулей, напр. `PuzzleServer.Infrastructure.App.HA.Db`):

```xml
<Project Sdk="Microsoft.NET.Sdk">

    <ItemGroup>
        <PackageReference Include="OpenTelemetry.Exporter.Prometheus.AspNetCore"/>
        <PackageReference Include="OpenTelemetry.Extensions.Hosting"/>
        <PackageReference Include="OpenTelemetry.Instrumentation.AspNetCore"/>
        <PackageReference Include="OpenTelemetry.Instrumentation.Runtime"/>
        <FrameworkReference Include="Microsoft.AspNetCore.App"/>
    </ItemGroup>

</Project>
```

`MetricsModuleExtensions.cs` — временная заготовка (наполнение в Т2):

```csharp
namespace PuzzleServer.Infrastructure.App.Metrics;

// Заготовка модуля метрик (docs/01.20-metrics.md): наполняется в задаче T2.
public static class MetricsModuleExtensions;
```

- [ ] **Шаг 3: Проект в slnx**

В `../Puzzle/src/PuzzleServer.Api.slnx` в `<Folder Name="/Infrastructure/">` добавить:

```xml
        <Project Path="PuzzleServer.Infrastructure.App.Metrics/PuzzleServer.Infrastructure.App.Metrics.csproj" />
```

- [ ] **Шаг 4: Проверка сборки**

Run: `cd /Users/demakaev/ZCodeProject/Puzzle && dotnet build src/PuzzleServer.Api.slnx`
Expected: 0 errors, 0 warnings.

### Task 1.2: `AddAppMetrics`/`MapAppMetrics` — TDD

**Files:**
- Modify: `../Puzzle/src/PuzzleServer.Infrastructure.App.Metrics/MetricsOptions.cs` (Create)
- Modify: `../Puzzle/src/PuzzleServer.Infrastructure.App.Metrics/MetricsModuleExtensions.cs`
- Test: `../Puzzle/src/PuzzleServer.UnitTests/Metrics/MetricsEndpointTests.cs` (Create)
- Test-проект: при отсутствии `Microsoft.AspNetCore.Mvc.Testing` в `PuzzleServer.UnitTests.csproj` — добавить `<PackageReference Include="Microsoft.AspNetCore.Mvc.Testing"/>`.

**Interfaces:**
- Produces (канон, порт в Ф2 слово-в--слово):

```csharp
namespace PuzzleServer.Infrastructure.App.Metrics;

// [Config]-опции модуля: секция "<Service>:Metrics".
public sealed class MetricsOptions
{
    public bool Enabled { get; set; } = true;      // false — модуль полностью выключен
    public string Path { get; set; } = "/metrics"; // путь scrape-эндпоинта
}

public static class MetricsModuleExtensions
{
    // Регистрация OTel-MeterProvider: сервисный Meter(serviceName) + System.Runtime
    // + http-метр ASP.NET; scrape-эндпоинт Prometheus-экспортёра на options.Path.
    // Метрики — пассивные наблюдатели: любые ошибки инструментария не роняют хост.
    public static IServiceCollection AddAppMetrics(
        this IServiceCollection services, string serviceName, IConfiguration metricsSection);

    // Эндпоинт-обёртка Prometheus-экспортёра (учёт Enabled/Path). Вызывать после Build().
    public static TApp MapAppMetrics<TApp>(this TApp app) where TApp : IApplicationBuilder;
}
```

- [ ] **Шаг 1: Написать падающий интеграционный тест (минимальный хост, без WAF-Program)**

`MetricsEndpointTests.cs`:

```csharp
using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using PuzzleServer.Infrastructure.App.Metrics;

namespace PuzzleServer.UnitTests.Metrics;

// Интеграционные тесты базового модуля метрик (docs/01.20-metrics.md): минимальный
// WebApplication-хост, без БД/Kafka. Фиксируют: 200+text-формат, кастомный Path,
// выключение Enabled.
public sealed class MetricsEndpointTests
{
    private static async Task<(HttpStatusCode Code, string Body)> GetMetricsAsync(
        bool enabled = true, string path = "/metrics")
    {
        // Arrange: минимальный хост с модулем метрик (порт 0 — случайный, без коллизий)
        var builder = WebApplication.CreateSlimBuilder();
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Test:Metrics:Enabled"] = enabled ? "true" : "false",
            ["Test:Metrics:Path"] = path,
        });
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Services.AddAppMetrics("TestService", builder.Configuration.GetSection("Test:Metrics"));
        var app = builder.Build();
        app.MapAppMetrics();
        await app.StartAsync();
        using var client = new HttpClient { BaseAddress = new Uri(app.Urls.First()) };

        // Act
        using var response = await client.GetAsync(path);
        var body = await response.Content.ReadAsStringAsync();
        await app.StopAsync();
        return (response.StatusCode, body);
    }

    [Fact]
    public async Task MetricsEndpoint_Responds_200_TextFormat()
    {
        // Act
        var (code, body) = await GetMetricsAsync();

        // Assert: 200 и Prometheus text-формат (dotnet_*-серии рантайма)
        Assert.Equal(HttpStatusCode.OK, code);
        Assert.Contains("dotnet_", body);
    }

    [Fact]
    public async Task MetricsEndpoint_Disabled_Returns404()
    {
        // Act
        var (code, _) = await GetMetricsAsync(enabled: false);

        // Assert: Enabled=false — эндпоинта нет
        Assert.Equal(HttpStatusCode.NotFound, code);
    }

    [Fact]
    public async Task MetricsEndpoint_CustomPath_Works()
    {
        // Act
        var (code, body) = await GetMetricsAsync(path: "/custom-metrics");

        // Assert: путь берётся из MetricsOptions.Path
        Assert.Equal(HttpStatusCode.OK, code);
        Assert.Contains("dotnet_", body);
    }
}
```

- [ ] **Шаг 2: Прогнать — убедиться в падении**

Run: `cd /Users/demakaev/ZCodeProject/Puzzle && dotnet test src/PuzzleServer.Api.slnx --filter "FullyQualifiedName~MetricsEndpointTests"`
Expected: FAIL — `AddAppMetrics` не существует (ошибка компиляции).

- [ ] **Шаг 3: Реализация модуля**

`MetricsOptions.cs` — как в Interfaces выше (с XML-доками на русском).

`MetricsModuleExtensions.cs`:

```csharp
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using OpenTelemetry.Metrics;

namespace PuzzleServer.Infrastructure.App.Metrics;

// Модуль метрик (docs/01.20-metrics.md): DI-регистрация OTel-MeterProvider и
// scrape-эндпоинт. Конвенции: имя Meter = имя системы (dot-нотация инструментов,
// единицы секунды/штуки; финальные имена после экспорта — словарь потребителя,
// например pg/arch/18 §2).
public static class MetricsModuleExtensions
{
    public static IServiceCollection AddAppMetrics(
        this IServiceCollection services, string serviceName, IConfiguration metricsSection)
    {
        var options = new MetricsOptions();
        metricsSection.Bind(options);
        services.AddSingleton(options);

        if (!options.Enabled)
            return services;

        // Сервисный Meter регистрируется в DI: доменные инструменты пишут в него.
        var meter = new Meter(serviceName);
        services.AddSingleton(meter);

        services.AddOpenTelemetry()
            .WithMetrics(b => b
                .AddMeter(serviceName)              // сервисные инструменты
                .AddRuntimeInstrumentation()        // dotnet_* (§2.1)
                .AddAspNetCoreInstrumentation()     // http_server_* (§2.1)
                .AddPrometheusExporter(o => o.ScrapeEndpointPath = options.Path));
        return services;
    }

    public static TApp MapAppMetrics<TApp>(this TApp app) where TApp : IApplicationBuilder
    {
        var options = app.ApplicationServices.GetRequiredService<MetricsOptions>();
        if (!options.Enabled)
            return app;
        app.UseOpenTelemetryPrometheusScrapingEndpoint(); // путь — из опций экспортёра
        return app;
    }
}
```

Если в пин-версии 1.16.0-beta.1 имя метода middleware отличается (проверить intellisense/доку пакета: `UseOpenTelemetryPrometheusScrapingEndpoint` — актуальное; ранее был `MapPrometheusScrapper`), использовать предоставляемое экспортёром API — семантика (учёт Enabled/Path) сохраняется; фактические имена серий зафиксирует тест Ф2-Т3.

- [ ] **Шаг 4: Прогнать тесты — зелёные**

Run: `cd /Users/demakaev/ZCodeProject/Puzzle && dotnet test src/PuzzleServer.Api.slnx --filter "FullyQualifiedName~MetricsEndpointTests"`
Expected: PASS 3/3.

- [ ] **Шаг 5: Полный билд Puzzle + unit-прогон**

Run: `cd /Users/demakaev/ZCodeProject/Puzzle && dotnet build src/PuzzleServer.Api.slnx && dotnet test src/PuzzleServer.Api.slnx --filter "FullyQualifiedName~PuzzleServer.UnitTests"`
Expected: 0 warnings; unit-серия зелёная (DB/Aspire-тесты не трогаем).

### Task 1.3: Док `01.20-metrics.md` + индекс

**Files:**
- Create: `../Puzzle/docs/01.20-metrics.md`
- Modify: `../Puzzle/docs/01-infrastructure.md` (строка в таблицу «Документы»)

**Interfaces:** нет (документация).

- [ ] **Шаг 1: Написать док**

Структура (по образцу `01.12-health-checks.md`; заголовок-ссылки «Назад: 01 — Инфраструктура»):
- Назначение: единый каркас Prometheus-метрик (`Meter` + официальный prometheus-exporter OTel), пассивное наблюдение.
- Типы: `MetricsOptions { Enabled=true, Path="/metrics" }`, `AddAppMetrics(serviceName, configSection)`, `MapAppMetrics()`.
- Конвенции: имя Meter = имя системы; dot-нотация инструментов (`worker.loop.ticks` → `worker_loop_ticks_total` после экспорта); единицы — секунды (`unit:"s"` → суффикс `_seconds`)/штуки; counter-инструменты получают `_total`; лейблы конечны.
- Инструкция подключения нового проекта (зеркало arch/18 §7): `[Config]`-секция `<Service>:Metrics` → `AddAppMetrics`/`MapAppMetrics` → доменные метрики dot-нотацией; для воркер-паттерна — переиспользовать словарь pg/arch/18 §2.2 (монорепо `Shared.Metrics`).
- Что читаем: `dotnet_*` (Runtime), `http_server_request_duration_seconds` (ASP.NET-метр).

- [ ] **Шаг 2: Строка в индекс**

В `01-infrastructure.md`, таблица «Документы», после строки 01.19:

```markdown
| [01.20 — Metrics](01.20-metrics.md) | `PuzzleServer.Infrastructure.App.Metrics` | Единый каркас Prometheus-метрик (OpenTelemetry Meter + официальный exporter): `AddAppMetrics`/`MapAppMetrics`, конвенции имён/лейблов, инструкция подключения. |
```

- [ ] **Шаг 3: Коммит фазы Ф1 (правила Puzzle: ветка в Puzzle-репо)**

```bash
cd /Users/demakaev/ZCodeProject/Puzzle
git checkout -b feat/metrics-module
git add src/PuzzleServer.Infrastructure.App.Metrics docs/01.20-metrics.md docs/01-infrastructure.md src/Directory.Packages.props src/PuzzleServer.Api.slnx src/PuzzleServer.UnitTests
git commit -m "feat: Infrastructure.App.Metrics — базовый модуль Prometheus-метрик (OTel Meter + prometheus-exporter, AddAppMetrics/MapAppMetrics, конвенции, док 01.20)"
```

Пуш/мерж в Puzzle — по правилам Puzzle-репо (main-агент/пользователь решает отдельно; работа t04 не блокируется — Ф2 портирует код файлами).

---

## Фаза Ф2 — Shared.Metrics: порт базы + WorkerMetrics (монорепо, worktree)

### Task 2.1: Пакеты + проект-порт базы

**Files:**
- Modify: `src/Directory.Packages.props` (worktree)
- Create: `src/Shared.Metrics/Shared.Metrics.csproj`, `src/Shared.Metrics/MetricsOptions.cs`, `src/Shared.Metrics/MetricsModuleExtensions.cs`
- Modify: `src/PgWorker.slnx`

**Interfaces:**
- Consumes: код Task 1.2 (порт копией, прецедент `AdminPanel.Infrastructure`).
- Produces: `Shared.Metrics.MetricsOptions`, `Shared.Metrics.MetricsModuleExtensions.AddAppMetrics/MapAppMetrics` — сигнатуры идентичны Task 1.2; DI-синглтон `System.Diagnostics.Metrics.Meter` (имя = serviceName).

- [ ] **Шаг 1: Пины в CPM монорепо**

В `src/Directory.Packages.props` добавить:

```xml
    <PackageVersion Include="OpenTelemetry" Version="1.16.0" />
    <PackageVersion Include="OpenTelemetry.Exporter.Prometheus.AspNetCore" Version="1.16.0-beta.1" />
    <PackageVersion Include="OpenTelemetry.Extensions.Hosting" Version="1.16.0" />
    <PackageVersion Include="OpenTelemetry.Instrumentation.AspNetCore" Version="1.15.2" />
    <PackageVersion Include="OpenTelemetry.Instrumentation.Runtime" Version="1.15.1" />
```

- [ ] **Шаг 2: Порт файлов из Task 1.2**

`Shared.Metrics.csproj` — как Puzzle-проект (те же PackageReference + FrameworkReference). `MetricsOptions.cs`/`MetricsModuleExtensions.cs` — копия кода Task 1.2 с заменой namespace на `Shared.Metrics` и заголовком «Порт Puzzle-модуля Infrastructure.App.Metrics (arch/18 §1; паттерн AdminPanel.Infrastructure — копия осознанная)».

- [ ] **Шаг 3: В slnx (/common/)**

В `src/PgWorker.slnx` в `<Folder Name="/common/">`:

```xml
        <Project Path="Shared.Metrics/Shared.Metrics.csproj" />
```

- [ ] **Шаг 4: Сборка**

Run: `cd /Users/demakaev/ZCodeProject/worktrees/feat-t04-unified-metrics && dotnet build src/PgWorker.slnx`
Expected: 0 errors, 0 warnings.

- [ ] **Шаг 5: Коммит**

```bash
git add src/Directory.Packages.props src/Shared.Metrics src/PgWorker.slnx
git commit -m "t04: Shared.Metrics — порт Puzzle-модуля базовой OTel-обвязки (AddAppMetrics/MapAppMetrics, arch/18 §1)"
```

### Task 2.2: `WorkerMetricsInstrumentation` — TDD (unit) + контракт источника серий

**Files:**
- Create: `src/Shared.Metrics/Worker/WorkerMetricsInstrumentation.cs`
- Create: `src/tests/Shared.Metrics.UnitTests/Shared.Metrics.UnitTests.csproj`, `WorkerMetricsInstrumentationTests.cs`
- Modify: `src/PgWorker.slnx` (+тест-проект в /tests/)
- Modify (сверка, см. Шаг 6): `arch/18-metrics.md` §1/§2.2, `docs/superpowers/2026-09-04-t04-unified-metrics/spec.md` §3.2

**Interfaces:**
- Consumes: `Meter` из Task 2.1.
- Produces (словарь arch/18 §2.2 — оба воркера пишут в одни серии):

```csharp
namespace Shared.Metrics.Worker;

// Типизированные инструменты воркер-паттерна (arch/18 §2.2). Метрики — пассивные
// наблюдатели: марк-методы никогда не бросают исключений, не влияют на циклы.
// Единственный источник серий §2.2 — эти марк-методы (вызовы циклов рядом с
// health.Mark* + подписка на фазовые записи журнала); HealthState — источник
// только /healthz (arch/18 §1).
public sealed class WorkerMetricsInstrumentation : IDisposable
{
    public WorkerMetricsInstrumentation(Meter meter, TimeProvider clock);

    // Тик цикла: counter worker.loop.ticks{loop, ok} → worker_loop_ticks_total;
    // ok=true дополнительно двигает worker.loop.last_success_timestamp_seconds{loop}.
    public void LoopTick(string loop, bool ok);

    // Длительность последнего тика: gauge worker.loop.duration_seconds{loop}.
    public void LoopDuration(string loop, double seconds);

    // Число удерживаемых клэймов: gauge worker.claims.held → worker_claims_held.
    public void ClaimsHeld(int count);

    // Вход кластера в фазу процесса: gauge worker.process.phase.duration_seconds
    // {cluster, process, phase}; value = now - startedAt при observe; повторная
    // запись той же фазы НЕ сбрасывает startedAt (first-seen); ProcessFinished
    // сбрасывает серию (кардинальность, arch/18 §9 M1).
    public void ProcessPhase(string cluster, string process, string phase, DateTimeOffset startedAt);
    public void ProcessFinished(string cluster, string process);

    // Журнальное событие фазы (подписка WorkJournal.PhaseWritten/WriteAsync):
    //  - ops без терминальной фазы (SuppressedOps: supervise, evacuate) — ИГНОР:
    //    стационарные записи (часть — через WriteSupervisionAsync мимо события),
    //    живость надзора закрывает WorkerLoopStalled (решение ревью Ф4-2);
    //  - терминальные фазы (FinalPhases, фактический словарь журналов обоих
    //    воркеров): done, failed, crashed, rejected, cancelled → ProcessFinished +
    //    Operation(process, ok: phase == "done"); skipped — промежуточная
    //    (AdoptionProcess: skipped → далее обязательно done/failed) — НЕ терминальная;
    //  - прочие → ProcessPhase (startedAt контролируется first-seen внутри).
    public void OnJournalPhase(string cluster, string process, string phase);

    // Завершённая операция: counter worker.operation.total{operation, result}
    // → worker_operation_total; result ∈ {"ok","error"}.
    public void Operation(string operation, bool ok);

    // Снапшот снят: источник worker.snapshot.age_seconds (value = now - at).
    public void SnapshotTaken(DateTimeOffset at);

    // Публичные константы контракта (тесты + будущие расширения словаря):
    public static readonly IReadOnlySet<string> FinalPhases =
        new HashSet<string> { "done", "failed", "crashed", "rejected", "cancelled" };
    public static readonly IReadOnlySet<string> SuppressedOps =
        new HashSet<string> { "supervise", "evacuate" };
}
```

Реализация (суть, не каркас): один стейт-объект под `lock` (`_lastSuccess: dict<loop,long unixSec>`, `_lastDuration: dict<loop,double>`, `_claimsHeld: int`, `_phases: dict<(cluster,process),(phase,startedAt)>`, `_lastSnapshotTaken: DateTimeOffset?`); counter-инструменты `worker.loop.ticks`/`worker.operation.total` пишутся напрямую через `meter.CreateCounter`; gauge-серии — через `meter.CreateObservableGauge` (по одному на серию, колбэки читают стейт; длительность фазы вычисляется в колбэке как `clock.GetUtcNow() - startedAt`). Для юнит-проверок значений — internal-снапшот `DebugSnapshot()` (`InternalsVisibleTo Shared.Metrics.UnitTests`) — надёжнее MeterListener'а; фактические имена серий фиксирует Т2.3.

- [ ] **Шаг 1: Тест-проект**

`Shared.Metrics.UnitTests.csproj`: `xunit.v3`+`xunit.runner.visualstudio`+`Microsoft.NET.Test.Sdk`+`FluentAssertions` (по образцу `src/tests/KafkaWorker.UnitTests/KafkaWorker.UnitTests.csproj`), `Microsoft.AspNetCore.Mvc.Testing` (для Т2.3), ProjectReference на `Shared.Metrics`. В `Shared.Metrics.csproj`: `<InternalsVisibleTo Include="Shared.Metrics.UnitTests"/>`. Добавить проект в slnx (/tests/).

- [ ] **Шаг 2: Падающие юнит-тесты семантики (AAA)**

`WorkerMetricsInstrumentationTests.cs` (каждый кейс — с `// Arrange/Act/Assert`):

```csharp
// Собственный FakeTimeProvider (новый пакет НЕ тащим, CPM чистый).
private sealed class FakeTimeProvider : TimeProvider
{
    public DateTimeOffset Now;
    public override DateTimeOffset GetUtcNow() => Now;
}

[Fact]
public void LoopTick_OkTrue_UpdatesLastSuccess()
{
    // Arrange
    var clock = new FakeTimeProvider { Now = DateTimeOffset.UnixEpoch.AddSeconds(1000) };
    using var meter = new Meter("TestWorker");
    using var sut = new WorkerMetricsInstrumentation(meter, clock);

    // Act
    sut.LoopTick("reconcile", ok: true);

    // Assert
    sut.DebugSnapshot().LastSuccess["reconcile"].Should().Be(1000);
}

[Fact]
public void LoopTick_OkFalse_DoesNotMoveLastSuccess()
{
    // Arrange
    var clock = new FakeTimeProvider { Now = DateTimeOffset.UnixEpoch.AddSeconds(1000) };
    using var meter = new Meter("TestWorker");
    using var sut = new WorkerMetricsInstrumentation(meter, clock);
    sut.LoopTick("reconcile", ok: true);

    // Act
    clock.Now = DateTimeOffset.UnixEpoch.AddSeconds(1010);
    sut.LoopTick("reconcile", ok: false);

    // Assert: ошибочный тик не двигает last_success (алерт «цикл умер» честный)
    sut.DebugSnapshot().LastSuccess["reconcile"].Should().Be(1000);
    sut.DebugSnapshot().LoopTicks[("reconcile", false)].Should().Be(1);
}

[Fact]
public void ProcessPhase_SamePhase_KeepsFirstSeen()
{
    // Arrange
    var t0 = DateTimeOffset.UnixEpoch.AddSeconds(5000);
    using var meter = new Meter("TestWorker");
    using var sut = new WorkerMetricsInstrumentation(meter, TimeProvider.System);
    sut.ProcessPhase("demo", "provisioning", "started", t0);

    // Act: повторная запись той же фазы (журнал пишет фазу каждый тик)
    sut.ProcessPhase("demo", "provisioning", "started", t0.AddMinutes(5));

    // Assert: first-seen не сбрасывается — возраст фазы растёт честно
    sut.DebugSnapshot().Phases[("demo", "provisioning")].StartedAt.Should().Be(t0);
}

[Fact]
public void ProcessFinished_RemovesPhaseSeries()
{
    // Arrange
    using var meter = new Meter("TestWorker");
    using var sut = new WorkerMetricsInstrumentation(meter, TimeProvider.System);
    sut.ProcessPhase("demo", "provisioning", "started", DateTimeOffset.UnixEpoch);

    // Act
    sut.ProcessFinished("demo", "provisioning");

    // Assert: серия сброшена — кардинальность только активные кластеры (M1)
    sut.DebugSnapshot().Phases.Should().BeEmpty();
}

[Fact]
public void OnJournalPhase_FinalPhases_FinishAndCountOperation()
{
    // Arrange: терминальные фазы фактического словаря (ревью Ф4-1):
    // done/failed/crashed/rejected/cancelled — все обязаны закрывать серию,
    // иначе вечная серия → ложный ProcessPhaseStuck.
    using var meter = new Meter("TestWorker");
    foreach (var phase in new[] { "done", "failed", "crashed", "rejected", "cancelled" })
    {
        using var sut = new WorkerMetricsInstrumentation(meter, TimeProvider.System);
        sut.OnJournalPhase("demo", "move", "planned");

        // Act
        sut.OnJournalPhase("demo", "move", phase);

        // Assert: серия закрыта; операция посчитана (done → ok, прочие → error)
        sut.DebugSnapshot().Phases.Should().BeEmpty();
        var result = phase == "done" ? "ok" : "error";
        sut.DebugSnapshot().Operations[("move", result)].Should().Be(1);
    }
}

[Fact]
public void OnJournalPhase_Rejected_MoveAndAbort_CloseSeries()
{
    // Arrange: регрессия ревью Ф4-1 — rejected реален в словаре (MoveProcess:958,
    // AbortSequence:378, TopicSync:295): процесс, завершившийся rejected, обязан
    // получить ProcessFinished, иначе серия вечная.
    using var meter = new Meter("TestWorker");
    using var sut = new WorkerMetricsInstrumentation(meter, TimeProvider.System);
    sut.OnJournalPhase("demo", "move", "post-flip");

    // Act
    sut.OnJournalPhase("demo", "move", "rejected");

    // Assert
    sut.DebugSnapshot().Phases.Should().BeEmpty();
    sut.DebugSnapshot().Operations[("move", "error")].Should().Be(1);
}

[Fact]
public void OnJournalPhase_Skipped_IsIntermediate_DoesNotCloseSeries()
{
    // Arrange: skipped у усыновления — ПРОМЕЖУТОЧНАЯ (AdoptionProcess.cs:128:
    // после skipped процесс продолжается и завершается done:180/failed:488).
    // Если объявить skipped терминальной — задвоится операция и порвётся живая фаза.
    using var meter = new Meter("TestWorker");
    using var sut = new WorkerMetricsInstrumentation(meter, TimeProvider.System);
    sut.OnJournalPhase("demo", "adopt", "started");

    // Act
    sut.OnJournalPhase("demo", "adopt", "skipped");
    sut.OnJournalPhase("demo", "adopt", "repaired-portalloc");

    // Assert: серия жива (сменилась фаза, не закрылась); операция не задвоена
    sut.DebugSnapshot().Phases[("demo", "adopt")].Phase.Should().Be("repaired-portalloc");
    sut.DebugSnapshot().Operations.Should().BeEmpty();
}

[Fact]
public void OnJournalPhase_SuppressedOps_EmitNoPhaseSeries()
{
    // Arrange: ревью Ф4-2 — supervise (стационарные записи, часть через
    // WriteSupervisionAsync мимо события) и evacuate (только waiting-*) не имеют
    // терминальной фазы: фазовые серии для них НЕ эмитим — иначе вечно горящий
    // ProcessPhaseStuck; живость надзора закрывает WorkerLoopStalled.
    using var meter = new Meter("TestWorker");
    using var sut = new WorkerMetricsInstrumentation(meter, TimeProvider.System);

    // Act
    sut.OnJournalPhase("demo", "supervise", "dcs-converge");
    sut.OnJournalPhase("demo", "evacuate", "waiting-alive");

    // Assert: подавлены полностью — ни серий, ни операций
    sut.DebugSnapshot().Phases.Should().BeEmpty();
    sut.DebugSnapshot().Operations.Should().BeEmpty();
}

[Fact]
public void SnapshotTaken_AgeComputed_FromTimeProvider()
{
    // Arrange
    var clock = new FakeTimeProvider { Now = DateTimeOffset.UnixEpoch.AddHours(3) };
    using var meter = new Meter("TestWorker");
    using var sut = new WorkerMetricsInstrumentation(meter, clock);
    sut.SnapshotTaken(DateTimeOffset.UnixEpoch.AddHours(1));

    // Act & Assert: возраст от TimeProvider (7200с), а не от времени записи
    sut.DebugSnapshot().SnapshotAgeSeconds.Should().Be(7200);
}

[Fact]
public void ClaimsHeld_LastValueWins()
{
    // Arrange
    using var meter = new Meter("TestWorker");
    using var sut = new WorkerMetricsInstrumentation(meter, TimeProvider.System);

    // Act
    sut.ClaimsHeld(5);
    sut.ClaimsHeld(3);

    // Assert: гейдж хранит последнее значение
    sut.DebugSnapshot().ClaimsHeld.Should().Be(3);
}
```

`DebugSnapshot()` — internal-метод (`InternalsVisibleTo`): immutable-запись стейта `LastSuccess: IReadOnlyDictionary<string,long>`, `LoopTicks: IReadOnlyDictionary<(string loop, bool ok),long>`, `Phases: IReadOnlyDictionary<(string cluster, string process),(string Phase, DateTimeOffset StartedAt)>`, `Operations: IReadOnlyDictionary<(string operation, string result),long>`, `ClaimsHeld: int?`, `SnapshotAgeSeconds: double?` (null — снапшотов не было).

- [ ] **Шаг 3: Прогнать падение**

Run: `dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~Shared.Metrics.UnitTests"`
Expected: FAIL (тип не существует — ошибка компиляции).

- [ ] **Шаг 4: Реализация `WorkerMetricsInstrumentation`**

По Interfaces-контракту выше; все публичные методы — тотальные (ничего не бросают). `OnJournalPhase`: сначала `SuppressedOps.Contains(process)` → выход; затем `FinalPhases.Contains(phase)` → `ProcessFinished` + `Operation(ok: phase=="done")`; иначе `ProcessPhase(cluster, process, phase, clock.GetUtcNow())` с first-seen. `Dispose()` — только освобождение собственных подписок (Meter принадлежит DI — не диспозить).

- [ ] **Шаг 5: Зелёные тесты**

Run: `dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~Shared.Metrics.UnitTests"` → PASS (все кейсы, включая rejected/skipped/suppress).

- [ ] **Шаг 6: Контракт источника серий (arch-first; ревью Ф4-2/Ф4-4)**

Формулировки уже внесены в ревью-итерации плана — сверить и везти тем же коммитом:
- `arch/18-metrics.md` §1 — «серии §2.2 питаются марк-методами WorkerMetricsInstrumentation… HealthState — нетронутый источник только /healthz»; §2.2 — фактические значения `process`, терминальные фазы `{done, failed, crashed, rejected, cancelled}`, `skipped` промежуточная, `supervise`/`evacuate` подавлены.
- `docs/superpowers/2026-09-04-t04-unified-metrics/spec.md` §3.2 — то же примечание.

Если имплементация вскрыла новое терминальное значение/оп (grep по мере добавления процессов) — множества `FinalPhases`/`SuppressedOps` и arch/18 §2.2 правятся в ЭТОМ же коммите (канон = факт).

- [ ] **Шаг 7: Коммит**

```bash
git add src/Shared.Metrics src/tests/Shared.Metrics.UnitTests src/PgWorker.slnx arch/18-metrics.md docs/superpowers/2026-09-04-t04-unified-metrics/spec.md
git commit -m "t04: WorkerMetricsInstrumentation — инструменты воркер-паттерна §2.2; терминальные фазы из фактического словаря (done/failed/crashed/rejected/cancelled), supervise/evacuate подавлены; контракт arch/18 §1/§2.2 + spec §3.2"
```

### Task 2.3: Интеграционный тест `/metrics` — фиксация фактических OTel-имён и лейблов (риск M3/S1, ревью Ф4-5)

**Files:**
- Test: `src/tests/Shared.Metrics.UnitTests/MetricsEndpointTests.cs` (Create)
- Modify (сверка): `arch/18-metrics.md` §2.2

**Interfaces:**
- Consumes: `AddAppMetrics`/`MapAppMetrics` (Т2.1), `WorkerMetricsInstrumentation` (Т2.2).
- Produces: зафиксированный набор фактических экспортируемых имён и значений лейбла `process` — при расхождении со словарём arch/18 §2 правим arch/18 тем же коммитом (spec §8 S1).

- [ ] **Шаг 1: Тест минимального хоста (порт теста Task 1.2 + воркер-серии + лейблы)**

Тесты (минимальный `WebApplication.CreateSlimBuilder`, real `TimeProvider.System`):

1. `MetricsEndpoint_ExportsDictionaryNames`: `AddAppMetrics("TestWorker", ...)` + DI `WorkerMetricsInstrumentation`; из `app.Services` достать instrumentation, вызвать `LoopTick("reconcile", true)`, `LoopTick("reconcile", false)`, `LoopDuration("reconcile", 0.42)`, `ClaimsHeld(3)`, `ProcessPhase("demo", "provision", "started", now)`, `Operation("provision", ok: true)`, `SnapshotTaken(now)`; GET `/metrics` → 200; body содержит ВСЕ канонические имена arch/18 §2.2:

```
worker_loop_ticks_total
worker_loop_last_success_timestamp_seconds
worker_loop_duration_seconds
worker_claims_held
worker_process_phase_duration_seconds
worker_operation_total
worker_snapshot_age_seconds
```

плюс §2.1: `dotnet_` и `http_server_request_duration_seconds` (если фактическое имя гистограммы иное — зафиксировать факт в тесте и СРАЗУ править arch/18 §2.1 тем же коммитом — закрытие риска M3).

2. `MetricsEndpoint_ProcessLabelValues_Canonical` (ревью Ф4-5/Ф4.2-1): прогнать `OnJournalPhase("demo", op, "planned")` по всем фактическим op журналов — PgWorker (11): `provision, deprovision, adopt, add-shard, remove-shard, rotate-app-password, move, rollback, finalize, repair, abort`; KafkaWorker (8): `provision, deprovision, add-broker, remove-broker, reassign, rotate, regen, topicsync`; затем `OnJournalPhase("demo", op, "done")`; GET `/metrics` → все серии `worker_process_phase_duration_seconds` отсутствуют после done, а `worker_operation_total{operation="<op>",result="ok"}` присутствует для каждого op. Канон-значения — из arch/18 §2.2 (перечень уже приведён к факту, включая `rollback`/`finalize` — MoveProcess.cs:592/691/755, ревью Ф4.2-1); при добавлении нового op процессом — тест и §2.2 правятся тем же коммитом.

3. `MetricsEndpoint_Disabled_404` и `MetricsEndpoint_CustomPath` — как в Task 1.2.

- [ ] **Шаг 2: Прогнать; при расхождениях имён/лейблов — правка arch/18 §2 тем же коммитом**

Run: `dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~Shared.Metrics.UnitTests"`
Expected: PASS. Если фактическое имя серии/лейбла отличается от словаря — тест пишет фактическое, а `arch/18-metrics.md` §2 правится в этом же коммите (канон = факт).

- [ ] **Шаг 3: Коммит**

```bash
git add src/tests/Shared.Metrics.UnitTests arch/18-metrics.md
git commit -m "t04: интеграционный тест /metrics фиксирует фактические OTel-имена и значения лейбла process против словаря arch/18 §2 (M3/S1)"
```

---

## Фаза Ф3 — PgWorker: инструментация

### Task 3.1: `Program.cs` + appsettings + интеграционный тест `/metrics`

**Files:**
- Modify: `src/PgWorker.App/PgWorker.App.csproj` (+`<ProjectReference Include="..\Shared.Metrics\Shared.Metrics.csproj"/>`)
- Modify: `src/PgWorker.App/Options.cs` (+секция Metrics)
- Modify: `src/PgWorker.App/appsettings.json` (+`"Metrics"`)
- Modify: `src/PgWorker.App/Program.cs`
- Test: `src/tests/PgWorker.IntegrationTests/Api/MetricsTests.cs` (Create), `MetricsApiFactory.cs` (Create; рядом с `PgWorkerApiFactory.cs`)

**Interfaces:**
- Consumes: `Shared.Metrics` (Т2.1/Т2.2).
- Produces: Meter `"PgWorker"`; `/metrics` на `:8080` без ApiKey; `WorkerMetricsInstrumentation` в DI PgWorker.

- [ ] **Шаг 1: Секция опций**

В `Options.cs` (PgWorkerOptions) добавить:

```csharp
    /// <summary>Экспозиция метрик (arch/18 §3): /metrics на том же порту, что /healthz.</summary>
    public Shared.Metrics.MetricsOptions Metrics { get; set; } = new();
```

В `appsettings.json` внутрь `"PgWorker"`:

```json
    "Metrics": { "Enabled": true, "Path": "/metrics" },
```

- [ ] **Шаг 2: Падающий интеграционный тест**

`MetricsApiFactory.cs` — копия `PgWorkerApiFactory` БЕЗ `RemoveAll<IHostedService>` (OTel-MeterProvider — hosted-сервис; циклы оставляем живыми — etcd-фикстура настоящая, тики по пустому etcd успешны и бесшумны):

```csharp
public sealed class MetricsApiFactory(Etcd.EtcdFixture etcd) : WebApplicationFactory<Program>
{
    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Development");
        builder.ConfigureAppConfiguration((_, cfg) => cfg.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["PgWorker:Etcd:Endpoints:0"] = etcd.Endpoint,
            ["PgWorker:Docker:Hosts:0:Name"] = "local",
            ["PgWorker:Docker:Hosts:0:Endpoint"] = "unix:///var/run/does-not-exist.sock",
            ["PgWorker:Api:AdvertiseUrl"] = "http://localhost:9997",
            ["PgWorker:Api:EnableSeedEndpoint"] = "false",
        }));
        // hosted-сервисы НЕ выключаем: MeterProvider обязан жить для /metrics,
        // а циклы на пустом etcd-фикстуре тикают успешно и тихо.
    }
}
```

Fixture-класс `PgMetricsFixture` — как `PgApiFixture` (env-секреты Д7 до CreateClient, EtcdFixture, тот же паттерн Dispose). Collection: отдельная (`pg-metrics`).

`MetricsTests.cs` (использует `PgMetricsFixture`):

```csharp
[Fact]
public async Task Metrics_Responds_200_WithoutApiKey_EvenWhenApiKeySet()
{
    // Arrange: фабрика-оверрайд с непустым ApiKey (InMemory-конфиг поверх)
    // Act: GET /metrics без X-Api-Key
    // Assert: 200; тело содержит worker_loop_last_success_timestamp_seconds
    //         (циклы живы — серия уже эмитится); /api-401-симметрию НЕ проверяем тут
}

[Fact]
public async Task Metrics_ApiKeySecuredApi_StaysProtected()
{
    // Arrange: непустой ApiKey
    // Act: GET /api/... без ключа
    // Assert: 401 — ApiKeyMiddleware не сломан подключением метрик
}
```

- [ ] **Шаг 3: Прогнать падение**

Run: `dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~PgWorker.IntegrationTests.Api.MetricsTests"`
Expected: FAIL — 404 `/metrics` (эндпоинта нет).

- [ ] **Шаг 4: `Program.cs`**

В `Program.cs` после регистрации `HealthState`:

```csharp
// Метрики (arch/18 §3): /metrics на том же Kestrel-порту, что /healthz;
// ApiKeyMiddleware защищает только /api — scrape-грань открыта (доверенная сеть).
builder.Services.AddAppMetrics("PgWorker", builder.Configuration.GetSection("PgWorker:Metrics"));
builder.Services.AddSingleton(sp => new Shared.Metrics.Worker.WorkerMetricsInstrumentation(
    sp.GetRequiredService<System.Diagnostics.Metrics.Meter>(),
    sp.GetRequiredService<TimeProvider>()));
```

После `app.UseMiddleware<ApiKeyMiddleware>();`:

```csharp
app.MapAppMetrics();
```

- [ ] **Шаг 5: Зелёный тест + коммит**

Run: тот же фильтр → PASS.
```bash
git add src/PgWorker.App src/tests/PgWorker.IntegrationTests
git commit -m "t04: PgWorker — AddAppMetrics/MapAppMetrics, /metrics без ApiKey (arch/18 §3)"
```

### Task 3.2: `WorkJournal.PhaseWritten` — единый seam фаз/операций (S2)

**Files:**
- Modify: `src/PgWorker.Etcd/Coordination/WorkJournal.cs`
- Modify: `src/PgWorker.App/Program.cs` (подписка)
- Test: `src/tests/PgWorker.UnitTests/Writing/WorkJournalPhaseEventTests.cs` (Create; папка `Writing` уже есть)

**Interfaces:**
- Produces:

```csharp
// WorkJournal: событие успешной ФАЗОВОЙ записи (наблюдатели метрик; вызывается
// ПОСЛЕ успешного PutAsync; наблюдатели — пассивные, исключения глотаются).
// ВНИМАНИЕ: WriteSupervisionAsync (стационарные записи надзора) событие НЕ
// эмитит — supervise подавлен в сериях (arch/18 §2.2, решение ревью Ф4-2).
public sealed record WorkPhaseEntry(string Cluster, string Op, string Phase);
public event Action<WorkPhaseEntry>? PhaseWritten;
```

- Эмиттеры фазовых записей журнала (проверено grep'ом): процессы (`WritePhaseAsync`) и `ReconcileLoop.LogCrashAsync` (`"crashed"` — терминальная, событие корректно закроет серию).

- [ ] **Шаг 1: Падающий unit-тест**

`WorkJournalPhaseEventTests.cs` на etcd-фике паттерна `EtcdFixtures` (см. соседние тесты `Writing/`), три кейса (AAA):
1. `WritePhaseAsync` успешен → `PhaseWritten` получил `(cluster, op, phase)`; неудачный Put (недоступный endpoint) → событие НЕ зовётся.
2. `WriteSupervisionAsync` успешен → событие НЕ зовётся (надзор — не фазовый процесс; подавление на стороне потребителя дублируется отсутствием события из этого метода).
3. `WritePhaseAsync(..., "crashed", ...)` → событие зовётся (терминальные фазы приходят тем же путём).

- [ ] **Шаг 2: Прогнать падение** — `dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~WorkJournalPhaseEventTests"` → FAIL (события нет).

- [ ] **Шаг 3: Реализация события**

В `WorkJournal`: record `WorkPhaseEntry(string Cluster, string Op, string Phase)`; поле `private event Action<WorkPhaseEntry>? _phaseWritten;` + публичное событие; в `WritePhaseAsync` после успешного `WithFailoverAsync(...)`:

```csharp
NotifyPhase(new WorkPhaseEntry(cluster, op, phase));
// где NotifyPhase проглатывает исключения наблюдателей (метрики — пассивные):
// try { _phaseWritten?.Invoke(entry); } catch { /* наблюдатель не влияет на журнал */ }
```

`WriteSupervisionAsync` не трогаем (докстринг-комментарий из Interfaces).

- [ ] **Шаг 4: Зелёный тест**

- [ ] **Шаг 5: Подписка в `Program.cs`**

В фабрике `WorkerMetricsInstrumentation` (после создания экземпляра):

```csharp
// Единый seam фаз/операций (S2): метрики — наблюдатель журнала, точки вызова
// процессов не трогаем. Терминальные фазы/first-seen/подавление supervise и
// evacuate — внутри OnJournalPhase (arch/18 §2.2).
sp.GetRequiredService<WorkJournal>().PhaseWritten += e => m.OnJournalPhase(e.Cluster, e.Op, e.Phase);
```

- [ ] **Шаг 6: Сборка + тесты PgWorker.UnitTests + коммит**

```bash
git add src/PgWorker.Etcd src/PgWorker.App src/tests/PgWorker.UnitTests
git commit -m "t04: WorkJournal.PhaseWritten — событие фазовых записей (WriteSupervisionAsync мимо); подписка WorkerMetricsInstrumentation (seam S2)"
```

### Task 3.3: Циклы — LoopTick/LoopDuration/ClaimsHeld/SnapshotTaken

**Files:**
- Modify: `src/PgWorker.App/Loops/ReconcileLoop.cs`, `KeepaliveLoop.cs`, `SnapshotLoop.cs`

**Interfaces:**
- Consumes: `WorkerMetricsInstrumentation` (Т2.2). `loop`-лейблы — `"reconcile"`, `"keepalive"`, `"snapshot"` (канон §2.2; `ok` ∈ {true,false}).

- [ ] **Шаг 1: Параметр DI + марк-вызовы**

В каждый Loop-класс добавить параметр `Shared.Metrics.Worker.WorkerMetricsInstrumentation metrics` (primary constructor). Точки (паттерн одинаков, `ReconcileLoop` показан полностью):

```csharp
// ExecuteAsync: измерение длительности тика + тик-статус в обеих ветках
var started = Stopwatch.GetTimestamp();
var tick = await TickSafelyAsync(stoppingToken);
metrics.LoopDuration("reconcile", Stopwatch.GetElapsedTime(started).TotalSeconds);
if (tick.IsSuccess)
{
    ...
}
else
{
    metrics.LoopTick("reconcile", ok: false);   // рядом с ErrorDelay-веткой
    ...
}

// TickAsync (успешный конец, рядом с health.MarkReconcileTick):
health.MarkReconcileTick(ok: true, claimsHeld: ...);
metrics.LoopTick("reconcile", ok: true);
metrics.ClaimsHeld(parsed.Value.Count(c => claims.IsMine(c.Config.Cluster)));
```

`KeepaliveLoop`/`SnapshotLoop` — то же с лейблами `"keepalive"`/`"snapshot"` по местам их `health.MarkKeepaliveTick()`/`health.MarkSnapshotTick()`; в `SnapshotLoop` рядом с `health.MarkSnapshotTaken()`: `metrics.SnapshotTaken(clock.GetUtcNow());` (если `TimeProvider` не параметр цикла — взять из DI; проверить фактические поля цикла при имплементации: источник тот же, что у `HealthState`-вызовов).

- [ ] **Шаг 2: Интеграционная проверка имён**

Тест `MetricsTests.Metrics_WorkerSeries_AfterFirstTick` (в `MetricsTests.cs`, factory без RemoveAll — циклы живы): дождаться первого тика (retry-цикл до 15 с — тик быстрее ScanIntervalSec=5), GET `/metrics` → содержит `worker_loop_ticks_total{loop="reconcile",ok="true"}` и `worker_claims_held`. AAA-комментарии.

- [ ] **Шаг 3: Прогон интеграционной серии PgWorker**

Run: `dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~PgWorker.IntegrationTests.Api.MetricsTests"` → PASS.

- [ ] **Шаг 4: Коммит**

```bash
git add src/PgWorker.App/Loops src/tests/PgWorker.IntegrationTests
git commit -m "t04: циклы PgWorker — LoopTick/LoopDuration/ClaimsHeld/SnapshotTaken (§2.2)"
```

---

## Фаза Ф4 — KafkaWorker: инструментация + коллектор лагов/USR

### Task 4.1: Расширение seam `IKafkaAdminClient` (группы/оффсеты/watermarks)

**Files:**
- Modify: `src/KafkaWorker.Provisioning/Kafka/IKafkaAdminClient.cs`
- Modify: `src/KafkaWorker.Provisioning/Kafka/KafkaAdminClient.cs` (Confluent-адаптер)

**Interfaces:**
- Produces (новые члены seam — без Confluent-типов, паттерн Puzzle §7):

```csharp
// Группа консьюмеров (ListGroups): id + состояние.
public sealed record KafkaGroupView(string Group, string State);

// Оффсет на партиции: committed группы либо watermark (Latest/Earliest).
public sealed record KafkaTopicPartition(string Topic, int Partition);
public sealed record KafkaTopicPartitionOffset(string Topic, int Partition, long Offset);

public interface IKafkaAdminClient
{
    // ... существующие методы без изменений ...

    // Группы консьюмеров кластера (коллектор лагов, arch/18 §4).
    Task<Result<IReadOnlyList<KafkaGroupView>>> ListGroupsAsync(CancellationToken ct);

    // Committed-оффсеты группы по партициям.
    Task<Result<IReadOnlyList<KafkaTopicPartitionOffset>>> ListConsumerGroupOffsetsAsync(
        string group, CancellationToken ct);

    // Watermark-оффсеты (Latest) набора партиций.
    Task<Result<IReadOnlyList<KafkaTopicPartitionOffset>>> ListOffsetsAsync(
        IReadOnlyList<KafkaTopicPartition> partitions, CancellationToken ct);
}
```

- [ ] **Шаг 1: Типы + методы интерфейса** (код выше, XML-доки на русском — назначение для коллектора arch/18 §4).

- [ ] **Шаг 2: Реализация в `KafkaAdminClient`** (единственное место с Confluent):
  - `ListGroupsAsync` → `adminClient.ListGroups(requestOptions, ct)`; маппинг `ConsumerGroupListing` → `KafkaGroupView(g.Group, g.State.ToString())`; исключения → `Result.Failed` (как существующие методы адаптера — свериться с телом `DescribeClusterAsync` и повторить стиль).
  - `ListConsumerGroupOffsetsAsync` → `ListConsumerGroupOffsets(group, new ListConsumerGroupOffsetsOptions(), ct)` (перегрузку Confluent.Kafka 2.14 проверить по существующему использованию/доке пакета); все партиции группы; offset −1001 (Invalid/нет committed) → пропускать из результата.
  - `ListOffsetsAsync` → `ListOffsets(partitions.Select(p => new TopicPartitionOffsetSpec { TopicPartition = new(p.Topic, p.Partition), Offset = Offset.End }))`; результат → `KafkaTopicPartitionOffset(Offset.Value...)`.
  - Таймауты — от `RequestTimeout` фабрики (10 с, arch/16 §4), без собственных ретраев.

- [ ] **Шаг 3: Сборка + существующие тесты процессов не сломаны**

Run: `dotnet build src/PgWorker.slnx && dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~KafkaWorker.UnitTests"`
Expected: 0 warnings; зелёные (фейки seam в тестах процессов расширить заглушками новых методов — вернуть `Result.Failed`/пусто; компилятор укажет файлы фиков: `tests/KafkaWorker.UnitTests/**/Fake*Kafka*`).

- [ ] **Шаг 4: Коммит**

```bash
git add src/KafkaWorker.Provisioning src/tests/KafkaWorker.UnitTests
git commit -m "t04: seam IKafkaAdminClient + ListGroups/ListConsumerGroupOffsets/ListOffsets (коллектор лагов, arch/18 §4)"
```

### Task 4.2: Опции + `Program.cs` + интеграционный тест `/metrics`

**Files:**
- Modify: `src/KafkaWorker.App/KafkaWorker.App.csproj` (+ProjectReference `Shared.Metrics`)
- Modify: `src/KafkaWorker.App/Options.cs`, `appsettings.json`, `Program.cs`
- Test: `src/tests/KafkaWorker.IntegrationTests/Api/MetricsTests.cs` (+`MetricsApiFactory` — копия `KafkaApiFactory` без `RemoveAll<IHostedService>`, отдельная collection-фикстура)

**Interfaces:**
- Produces: Meter `"KafkaWorker"`; `/metrics` без ApiKey; секция `KafkaWorker:Metrics { Enabled, Path, CollectIntervalSec=30 }`.

- [ ] **Шаг 1: Опции** — в `Options.cs`:

```csharp
/// <summary>Метрики воркера (arch/18 §3–§4): экспозиция + тик коллектора лагов.</summary>
public sealed class KafkaWorkerMetricsOptions : Shared.Metrics.MetricsOptions
{
    /// <summary>Тик коллектора лагов/USR, сек (default 30; arch/18 §4).</summary>
    public int CollectIntervalSec { get; set; } = 30;
}
```

и в `KafkaWorkerOptions`: `public KafkaWorkerMetricsOptions Metrics { get; set; } = new();`. В `appsettings.json` (`"KafkaWorker"`): `"Metrics": { "Enabled": true, "Path": "/metrics", "CollectIntervalSec": 30 }`.

- [ ] **Шаг 2: Падающий тест** — `MetricsTests` (зеркало Т3.1: 200 без ApiKey при непустом ключе; /api остаётся 401). Запустить → FAIL 404.

- [ ] **Шаг 3: `Program.cs`** — как Т3.1 (`AddAppMetrics("KafkaWorker", section "KafkaWorker:Metrics")`, `WorkerMetricsInstrumentation`, `app.MapAppMetrics()`; расширенная секция биндится отдельно через `Configure<KafkaWorkerMetricsOptions>`).

- [ ] **Шаг 4: Зелёный тест + коммит**

```bash
git commit -m "t04: KafkaWorker — AddAppMetrics/MapAppMetrics + секция KafkaWorker:Metrics (arch/18 §3)"
```

### Task 4.3: `KafkaMetricsCollector` — TDD на фейках

**Files:**
- Create: `src/KafkaWorker.App/KafkaMetricsCollector.cs`, `src/KafkaWorker.App/KafkaMetricsState.cs`
- Test: `src/tests/KafkaWorker.UnitTests/App/KafkaMetricsCollectorTests.cs` (+фабрика фейков `IKafkaAdminClientFactory`)

**Interfaces:**
- Consumes: seam Т4.1; `KafkaSnapshotParser` (источник кластеров); `IEtcdGateway.RangeAsync` (префикс `/clusters/` — как в `ReconcileLoop` воркера).
- Produces: серии §2.3 — `kafka_consumer_lag{cluster,group,topic}`, `kafka_under_replicated_partitions{cluster,topic}`, `kafka_collector_last_success_timestamp_seconds` (ObservableGauge над стейтом). Контракты:

```csharp
// Стейт + ObservableGauge-серии коллектора (arch/18 §2.3–§4).
public sealed class KafkaMetricsState(System.Diagnostics.Metrics.Meter meter)
{
    // Снимок стейта для тестов (internal, InternalsVisibleTo KafkaWorker.UnitTests):
    // Lag[(cluster,group,topic)]=long, Usr[(cluster,topic)]=int, LastSuccess=DateTimeOffset?
}

public sealed class KafkaMetricsCollector(
    int collectIntervalSec,
    Func<CancellationToken, Task<Result<IReadOnlyList<KafkaClusterSnapshot>>>> clustersSnapshot,
    IKafkaAdminClientFactory adminFactory,
    KafkaMetricsState state,
    TimeProvider clock,
    ILogger<KafkaMetricsCollector> logger) : BackgroundService
{
    // Ядро тика — публично для unit-тестов без хоста (паттерн RefreshOnceAsync панели).
    public Task CollectOnceAsync(CancellationToken ct);
}
```

- [ ] **Шаг 1: Фейк фабрики + падающие тесты**

Тестовый каркас: `FakeKafkaAdminClientFactory : IKafkaAdminClientFactory` — возвращает фейк с настраиваемыми ответами; источник кластеров — делегат с заготовленным списком `KafkaClusterSnapshot`. Тесты (AAA, все):

```csharp
[Fact]
public async Task Collect_LagComputed_WatermarkMinusCommitted()
{
    // Arrange: кластер c1: группа g1 committed {t1p0=5, t1p1=10}; watermarks {t1p0=20, t1p1=10}
    // Act: CollectOnceAsync
    // Assert: state.Lag[("c1","g1","t1")] == 15 (20-5 + max(0,10-10))
}

[Fact]
public async Task Collect_UnderReplicated_IssrSubsetOfReplicas()
{
    // Arrange: describe: topic t1 — 2 партиции: p0 ISR(2)<replicas(3) → USR; p1 ISR=3 → нет
    // Assert: state.Usr[("c1","t1")] == 1
}

[Fact]
public async Task Collect_ClusterFails_LagsNotUpdated_TickSurvives()
{
    // Arrange: два кластера; второй бросает Failed по всем вызовам
    // Act: CollectOnceAsync → возвращает успех (не бросает)
    // Assert: лаги первого обновлены; стейт второго прежний; LastSuccess == DateTimeOffset.MinValue
    //         (обновляется только при полном успехе всех кластеров — консервативно, алерт §3.7)
}

[Fact]
public async Task Collect_AllOk_UpdatesLastSuccess()
{
    // Assert: state.LastSuccess == fake-время тика (TimeProvider инжектится, паттерн воркера)
}

[Fact]
public async Task Collect_SkipsClustersWithoutBootstrap()
{
    // Arrange: снапшот с Endpoints/AppUser/AppPassword == null (кластер не поднят)
    // Assert: к фабрике не обращался, ошибка не фиксируется (проба невозможна — паттерн NodeSupervisor)
}

[Fact]
public async Task Collect_OnlyActiveClusters_StateNullMeansActive()
{
    // Arrange (ревью Ф4-6): два кластера — Active (Config.State == null) и
    // невыполненная заявка (Config.State == "PROVISIONING"; KafkaDomain.cs:11-18,
    // arch/15 §2.1 — State есть только у невыполненных заявок)
    // Act: CollectOnceAsync
    // Assert: AdminClient создавался только для Active; стейт содержит только его серии
}
```

- [ ] **Шаг 2: Прогнать падение** — FAIL (класса нет).

- [ ] **Шаг 3: Реализация**

`KafkaMetricsState` — стейт под lock (лаги/USR/lastSuccess) + ObservableGauge-регистрация на Meter `"KafkaWorker"` (инструменты `kafka.consumer.lag` без unit, `kafka.under_replicated_partitions`, `kafka.collector.last_success_timestamp_seconds` unit `"s"`). `KafkaMetricsCollector : BackgroundService`: тик `KafkaWorker:Metrics:CollectIntervalSec` (default 30, <=0 → 30 с лог-предупреждением — паттерн `SnapshotRefresher`); тело — `CollectOnceAsync` в try/catch (исключение → лог warning, тик жив); фильтр кластеров — только Active: `snap.Config.State == null` И дискавери-поля не-null (revью Ф4-6); на кластер: `await using var admin = factory.Create(snap.Endpoints!, snap.AppUser!, snap.AppPassword!)` → `ListGroupsAsync` → по группам `ListConsumerGroupOffsetsAsync` + `ListOffsetsAsync` (Latest по committed-партициям) → Σ lag по (group, topic) c clamped `max(0, wm − committed)`; `DescribeTopicsAsync(includeInternal: false)` → USR = `ReplicasPerPartition[p].Count > IsrPerPartition[p].Count` (ISR null → пропуск топика, комментарий); полный успех всех кластеров → `state.LastSuccess = clock.GetUtcNow()`. Один коннект на кластер за тик, без ретраев (M2/S4).

- [ ] **Шаг 4: Зелёные тесты + коммит**

```bash
git add src/KafkaWorker.App src/tests/KafkaWorker.UnitTests
git commit -m "t04: KafkaMetricsCollector — лаги (watermark-committed)/USR/самонаблюдение на seam-фабрике; только Active (State == null) (arch/18 §4)"
```

### Task 4.4: `Program.cs` коллектора + WorkJournal-событие + циклы (зеркало Ф3)

**Files:**
- Modify: `src/KafkaWorker.Etcd/Coordination/WorkJournal.cs` (событие `PhaseWritten` — на фазовом методе `WriteAsync`; `WriteSupervisionAsync` мимо; копия Т3.2: record `WorkPhaseEntry`, `NotifyPhase` после успешного Put)
- Modify: `src/KafkaWorker.App/Program.cs` (подписка + `AddHostedService<KafkaMetricsCollector>` + делегат-источник снапшота кластеров)
- Modify: `src/KafkaWorker.App/Loops/ReconcileLoop.cs`, `KeepaliveLoop.cs`, `SnapshotLoop.cs`
- Test: `src/tests/KafkaWorker.UnitTests/Writing/WorkJournalPhaseEventTests.cs` (Create — зеркало Т3.2 на etcd-фике паттерна KafkaWorker.UnitTests; имена тестов те же + кейс на `WriteSupervisionAsync`)

**Interfaces:** идентичны Т3.2/Т3.3. Лейблы `process` = фактические Op журнала (provision/deprovision/add-broker/remove-broker/reassign/rotate/regen/topicsync — arch/18 §2.2); supervise/evacuate подавлены в `OnJournalPhase`.

- [ ] **Шаг 1: Событие WorkJournal — TDD** (падающий тест → событие на `WriteAsync` → зелёный; код см. Т3.2 Шаги 1–4; отличия KafkaWorker: метод журнала называется `WriteAsync`, а не `WritePhaseAsync`).
- [ ] **Шаг 2: Подписка в Program.cs** — в фабрике `WorkerMetricsInstrumentation` (как Т3.2 Шаг 5).

- [ ] **Шаг 3: Регистрация коллектора**

В `Program.cs` (после `IKafkaAdminClientFactory`; конструкторы — контракты Т4.3):

```csharp
// Коллектор лагов/USR (arch/18 §4): read-only сбор вне клэймов; источник кластеров —
// тот же снапшот /clusters/, что у ReconcileLoop (парсер KafkaSnapshotParser);
// только Active (Config.State == null — arch/15 §2.1, ревью Ф4-6).
builder.Services.AddSingleton(sp => new KafkaMetricsState(
    sp.GetRequiredService<System.Diagnostics.Metrics.Meter>()));
builder.Services.AddHostedService(sp => new KafkaMetricsCollector(
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Metrics.CollectIntervalSec,
    SnapshotClustersAsync,
    sp.GetRequiredService<IKafkaAdminClientFactory>(),
    sp.GetRequiredService<KafkaMetricsState>(),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<ILogger<KafkaMetricsCollector>>()));
```

`SnapshotClustersAsync` — локальная функция `Program.cs`: `RangeAsync` по `/clusters/` c failover по endpoints (паттерн `ReconcileLoop` воркера) → `KafkaSnapshotParser` → список снапшотов; ошибки чтения → `Result.Failed` (коллектор пропустит тик, `KafkaCollectorStalled` сработает по свежести).

- [ ] **Шаг 4: Циклы** — зеркало Т3.3 (`metrics.LoopDuration/LoopTick/ClaimsHeld/SnapshotTaken` в трёх циклах, лейблы те же).

- [ ] **Шаг 5: Прогон KafkaWorker-серий + коммит**

Run: `dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~KafkaWorker.IntegrationTests.Api.MetricsTests|FullyQualifiedName~KafkaWorker.UnitTests"` → PASS.
```bash
git add src/KafkaWorker.Etcd src/KafkaWorker.App src/tests
git commit -m "t04: KafkaWorker — циклы/журнал в метриках, коллектор в host (зеркало PgWorker, §2.2)"
```

---

## Фаза Ф5 — AdminPanel: HTTP-метрики + refresher-гейдж

### Task 5.1: Подключение Shared.Metrics + `/metrics` + гейдж свежести

**Files:**
- Modify: `src/AdminPanel.Api/AdminPanel.Api.csproj` (+`<ProjectReference Include="..\Shared.Metrics\Shared.Metrics.csproj"/>` — прецедент `AdminPanel.Infrastructure`)
- Modify: `src/AdminPanel.Api/Program.cs`, `src/AdminPanel.Api/appsettings.json` (+`"AdminPanel": { "Metrics": {...} }`)
- Test: `src/tests/AdminPanel.IntegrationTests/MetricsTests.cs` (Create; паттерн существующих `HealthzTests.cs` — их WAF-фабрика)

**Interfaces:**
- Consumes: `Shared.Metrics` (Т2.1); `ISnapshotStore.Current.BuiltAtUtc` (панель; `AdminPanel.Etcd.SnapshotStore`).
- Produces: Meter `"AdminPanel"`; серии §2.4: `panel_refresher_last_success_timestamp_seconds` + `http_server_request_duration_seconds` + `dotnet_*`.

Дизайн-гейджа (ревью Ф4-8, фиксируется здесь строкой): реализация — pull (ObservableGauge над `store.Current.BuiltAtUtc`), НЕ марк-метод внутри `SnapshotRefresher`. Эквивалентно spec §3.5 («марк-метод у места обновления снапшота»): `BuiltAtUtc` обновляется ТОЛЬКО успешным тиком сборки (FailTick хранит прежний — `SnapshotRefresher.FailTick`: `previous?.BuiltAtUtc ?? now`), т.е. значение гейджа — ровно «unix-время последнего успешного тика»; при этом вторжение в `SnapshotRefresher` нулевое и нет второго писателя.

- [ ] **Шаг 1: Падающий тест**

`MetricsTests.cs` (по паттерну `HealthzTests` — их фабрика/фикстура; cookie-guard не должен пускать `/metrics` — а он и не должен: guard только `/api/*`):

```csharp
[Fact]
public async Task Metrics_Responds_200_WithoutCookieAuth()
{
    // Arrange: WAF-хост панели (существующая фикстура), клиент без cookie
    // Act: GET /metrics
    // Assert: 200; тело содержит dotnet_ (Runtime-серии) — http-гистограмма
    //         появится после первого запроса (Assert.Contains "dotnet_")
}

[Fact]
public async Task Metrics_ApiGuard_NotAffected()
{
    // Arrange: без cookie
    // Act: GET /api/overview (закрытый эндпоинт)
    // Assert: 401 — guard по-прежнему только /api/*, /metrics мимо него
}
```

Прогнать → FAIL 404.

- [ ] **Шаг 2: `Program.cs`**

После `builder.Services.AddCookieAuth();`:

```csharp
// Метрики (arch/18 §2.4/§3): /metrics без cookie-авторизации (guard — только
// /api/*); refresher-гейдж — пассивное чтение BuiltAtUtc снапшота (обновляется
// только успешным тиком сборки — отказ тика честно стареет, FailTick хранит
// прежний; ревью Ф4-8: pull эквивалентен марк-методу, второго писателя нет).
builder.Services.AddAppMetrics("AdminPanel", builder.Configuration.GetSection("AdminPanel:Metrics"));
builder.Services.AddSingleton(sp =>
{
    var meter = sp.GetRequiredService<System.Diagnostics.Metrics.Meter>();
    var store = sp.GetRequiredService<AdminPanel.Etcd.ISnapshotStore>();
    return meter.CreateObservableGauge(
        "panel.refresher.last_success_timestamp_seconds",
        () => new Measurement<double>(
            store.Current?.BuiltAtUtc.ToUnixTimeSeconds() ?? 0,
        unit: "s",
        description: "unix-время последнего успешного тика etcd-refresher (arch/18 §2.4)");
});
```

После `app.UseApiAuthorization();` (порядок не важен — guard не матчит `/metrics`): `app.MapAppMetrics();`. В `appsettings.json`: `"AdminPanel": { ..., "Metrics": { "Enabled": true, "Path": "/metrics" } }`.

Если `ISnapshotStore` не в DI с этим именем — свериться с `AdminPanel.Etcd/ModuleExtensions.AddEtcd()` (стора регистрируется там; `store.Current` — public getter, см. `SnapshotRefresher`).

- [ ] **Шаг 3: Зелёный тест + полный прогон панели + коммит**

Run: `dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~AdminPanel.IntegrationTests"` → PASS.
```bash
git add src/AdminPanel.Api src/tests/AdminPanel.IntegrationTests
git commit -m "t04: AdminPanel — AddAppMetrics + panel_refresher-гейдж pull над BuiltAtUtc (arch/18 §2.4)"
```

---

## Фаза Ф6 — Стенд хранения: профиль `metrics` (compose + конфиги + эмулятор + чек)

### Task 6.1: `/metrics` Patroni-эмулятора

**Files:**
- Modify: `dev-stand/adminpanel/sidecar/emulator.py`
- Test: ручная проверка docker-сборкой (ниже) + чек 65-го в Т6.3.

**Interfaces:**
- Produces: `GET /metrics` на `:8008` эмулятора: `pg_replica_lag_seconds{scope="<CLUSTER>-<SHARD>", node="<NODE_NAME>"}` — Prometheus text format (arch/18 §2.5). Ревью Ф4-7: экспортируется ТОЛЬКО своя нода (`NODE_NAME`) — каждый эмулятор scope знает всех членов, экспорт всех с каждого инстанса даст дубликаты серий на узел при scrape всех `hc*`; своё состояние (`state` + lock) уже трекается опросом `poll_loop` (S6).

- [ ] **Шаг 1: Эндпоинт в `Handler.do_GET`**

Добавить ветку ПЕРЕД проверкой own/alive (метрики отдаём, пока жив эмулятор; своя нода мертва — серия не эмитится):

```python
if self.path == "/metrics":
    # §2.5 + ревью Ф4-7: ТОЛЬКО своя нода (NODE_NAME) — экспорт всех членов
    # scope с каждого инстанса дублировал бы серии при scrape всех hc*.
    # Мастер: lag=0 (running); реплика: receive-replay diff (state c lock, S6).
    with state_lock:
        own = state.get(NODE)
    lines = [
        "# HELP pg_replica_lag_seconds replication lag of the node (emulator)",
        "# TYPE pg_replica_lag_seconds gauge",
    ]
    if own is not None and own["alive"]:
        lag = 0 if own["role"] == "master" else (own["lag"] or 0)
        lines.append(f'pg_replica_lag_seconds{{scope="{SCOPE}",node="{NODE}"}} {int(lag)}')
    self._send(200, ("\n".join(lines) + "\n").encode())
    return
```

- [ ] **Шаг 2: Ручная проверка**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t04-unified-metrics/dev-stand/adminpanel
docker compose --profile full up -d --build hc1a
sleep 3 && curl -fsS localhost:8011/metrics | grep pg_replica_lag_seconds
docker compose stop hc1a
```
Expected: РОВНО ОДНА серия `pg_replica_lag_seconds{scope="demo-s1",node="s1a"}` (никаких node="s1b" с этого инстанса).

- [ ] **Шаг 3: Коммит**

```bash
git add dev-stand/adminpanel/sidecar/emulator.py
git commit -m "t04: эмулятор Patroni — GET /metrics pg_replica_lag_seconds только своей ноды (arch/18 §2.5)"
```

### Task 6.2: Compose-профиль `metrics` + конфиги Prometheus/Grafana/Alertmanager

**Files:**
- Modify: `dev-stand/adminpanel/docker-compose.yml`
- Create: `dev-stand/adminpanel/metrics/prometheus/prometheus.yml`, `metrics/prometheus/rules.yml`
- Create: `dev-stand/adminpanel/metrics/alertmanager/alertmanager.null.yml`, `alertmanager.webhook.yml`, `entrypoint.sh` (chmod +x)
- Create: `dev-stand/adminpanel/metrics/grafana/provisioning/datasources/prometheus.yml`, `provisioning/dashboards/provider.yml`
- Create: `dev-stand/adminpanel/metrics/grafana/dashboards/workers.json`, `dashboards/kafka.json`, `dashboards/pg.json`
- Create: `dev-stand/adminpanel/.env.example`

**Interfaces:**
- Produces: сервисы `prometheus` (v3.6.1, host `${METRICS_PROMETHEUS_PORT:-9090}:9090`), `grafana` (v12.2.0, `${METRICS_GRAFANA_PORT:-3000}:3000`), `alertmanager` (v0.28.1, `${METRICS_ALERTMANAGER_PORT:-9093}:9093`) — профиль `metrics`; версии пин (последние стабильные на дату исполнения: сверить `docker manifest inspect`/hub; допустимо новее — главное пин, не `latest`).

- [ ] **Шаг 1: Сервисы в compose** (в `docker-compose.yml`, после `kafkaworker`):

```yaml
  # Стек мониторинга (arch/18 §5; профиль metrics — входит в полный подъём
  # 00-up.sh, quick без него). Порты — env-override при коллизиях на хосте (M4).
  prometheus:
    image: prom/prometheus:v3.6.1
    container_name: as-prometheus
    profiles: ["metrics"]
    ports: ["${METRICS_PROMETHEUS_PORT:-9090}:9090"]
    extra_hosts: ["host.docker.internal:host-gateway"]  # scrape pgworker (deploy-проект)
    volumes:
      - ./metrics/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - ./metrics/prometheus/rules.yml:/etc/prometheus/rules.yml:ro
      - prometheus-data:/prometheus
    command: ["--config.file=/etc/prometheus/prometheus.yml", "--storage.tsdb.retention.time=7d"]

  grafana:
    image: grafana/grafana:v12.2.0
    container_name: as-grafana
    profiles: ["metrics"]
    ports: ["${METRICS_GRAFANA_PORT:-3000}:3000"]
    environment:
      GF_SECURITY_ADMIN_USER: admin
      GF_SECURITY_ADMIN_PASSWORD: admin   # стенд: cookie-логин локального мониторинга
    volumes:
      - ./metrics/grafana/provisioning:/etc/grafana/provisioning:ro
      - ./metrics/grafana/dashboards:/var/lib/grafana/dashboards:ro
      - grafana-data:/var/lib/grafana

  # Webhook-ресивер: URL — env METRICS_ALERT_WEBHOOK_URL (пусто — только UI
  # Prometheus/Alertmanager; спека §3.7 Д4). Выбор конфига — entrypoint.
  alertmanager:
    image: prom/alertmanager:v0.28.1
    container_name: as-alertmanager
    profiles: ["metrics"]
    ports: ["${METRICS_ALERTMANAGER_PORT:-9093}:9093"]
    volumes:
      - ./metrics/alertmanager:/etc/alertmanager:ro
    entrypoint: ["/bin/sh", "/etc/alertmanager/entrypoint.sh"]
    environment:
      METRICS_ALERT_WEBHOOK_URL: ${METRICS_ALERT_WEBHOOK_URL:-}
```

`volumes:` добавить `prometheus-data:` и `grafana-data:`. Создать `dev-stand/adminpanel/.env.example` (дефолты env-override, arch/18 §5.1/§8; реальный `.env` — вне git, при наличии — дополнить):

```dotenv
# Порт-override стека мониторинга (профиль metrics; пусто = дефолты compose)
METRICS_PROMETHEUS_PORT=9090
METRICS_GRAFANA_PORT=3000
METRICS_ALERTMANAGER_PORT=9093
# Webhook-ресивер Alertmanager: пусто — алерты только в UI Prometheus/Alertmanager (Д4)
METRICS_ALERT_WEBHOOK_URL=
```

- [ ] **Шаг 2: `prometheus.yml`** (scrape 15 с, таргеты arch/18 §5.2)

Примечание (ревью, осознанное решение): для джоб `kafkaworker`/`adminpanel` fallback `host.docker.internal:8082/5050` НЕ дублируем — в стенде работают compose-DNS-имена сети (это и есть полный подъём); если стенд «части» поднимает Prometheus без этих сервисов — таргеты просто `down`, что честно видно на дашборде `up`. PgWorker — через `host.docker.internal` (deploy-проект, вне сети adminpanel-stand).

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s
rule_files: [/etc/prometheus/rules.yml]
scrape_configs:
  - job_name: pgworker            # deploy-compose, публикация хоста :8080
    static_configs: [{targets: ["host.docker.internal:8080"]}]
  - job_name: kafkaworker         # сеть стенда (профиль kafka)
    static_configs: [{targets: ["kafkaworker:8080"]}]
  - job_name: adminpanel          # сеть стенда
    static_configs: [{targets: ["adminpanel:8080"]}]
  - job_name: patroni             # эмуляторы :8008 (arch/18 §2.5)
    static_configs: [{targets: ["hc1a:8008", "hc1b:8008", "hc2a:8008", "hc2b:8008"]}]
```

- [ ] **Шаг 3: `rules.yml`** — 8 алертов spec §3.7 (группы `workers`, `kafka`, `pg`; annotations `summary`/`description` + runbook-ссылка `arch/18-metrics.md`):

```yaml
groups:
  - name: workers
    rules:
      - alert: ServiceDown
        expr: up{job=~"pgworker|kafkaworker|adminpanel"} == 0
        for: 2m
        labels: {severity: critical}
        annotations:
          summary: "сервис {{ $labels.job }} недоступен ({{ $labels.instance }})"
          description: "scrape падает 2 мин; runbook — arch/18-metrics.md §5.2"
      - alert: WorkerLoopStalled
        expr: time() - worker_loop_last_success_timestamp_seconds{loop=~"reconcile|keepalive"} > 60
        for: 0m
        labels: {severity: critical}
        annotations:
          summary: "цикл {{ $labels.job }}/{{ $labels.loop }} не тикает >60с"
          description: "только быстрые циклы (reconcile/keepalive, 5с; запас на ErrorDelay-бэкофф); snapshot-цикл (тик раз в 6ч) живость — SnapshotStale; runbook — arch/18 §2.2"
      - alert: SnapshotStale
        expr: worker_snapshot_age_seconds > 28800
        for: 0m
        labels: {severity: warning}
        annotations: {summary: "снапшот P12 старше 8ч ({{ $labels.job }})", description: "снапшоты раз в 6ч; runbook — arch/18 §2.2"}
      - alert: ProcessPhaseStuck
        expr: worker_process_phase_duration_seconds > 1800
        for: 0m
        labels: {severity: warning}
        annotations: {summary: "кластер {{ $labels.cluster }} в фазе {{ $labels.process }}/{{ $labels.phase }} >30мин", description: "provision-фазы допускают до 1ч; runbook — arch/18 §2.2"}
  - name: kafka
    rules:
      - alert: KafkaUnderReplicated
        expr: sum by (cluster) (kafka_under_replicated_partitions) > 0
        for: 5m
        labels: {severity: warning}
        annotations: {summary: "кластер {{ $labels.cluster }}: недореплицированные партиции", description: "ISR ⊂ assignment 5мин; runbook — arch/18 §2.3"}
      - alert: KafkaConsumerLagHigh
        expr: kafka_consumer_lag > 1000000
        for: 10m
        labels: {severity: warning}
        annotations: {summary: "лаг {{ $labels.group }}@{{ $labels.topic }} > 1e6 ({{ $labels.cluster }})", description: "порог стартовый, тюнинг по истории; runbook — arch/18 §2.3"}
      - alert: KafkaCollectorStalled
        expr: time() - kafka_collector_last_success_timestamp_seconds > 300
        for: 0m
        labels: {severity: warning}
        annotations: {summary: "коллектор kafka-метрик не собирает >5мин", description: "фиксированный порог ≥3×CollectIntervalSec(30с); runbook — arch/18 §4"}
  - name: pg
    rules:
      - alert: PgReplicaLagHigh
        expr: pg_replica_lag_seconds > 30
        for: 5m
        labels: {severity: warning}
        annotations: {summary: "реплика {{ $labels.node }} ({{ $labels.scope }}) отстаёт >30с", description: "scrape Patroni-эмуляторов; runbook — arch/18 §2.5"}
```

(Пробелы в YAML — двухсимвольная индентация; имена/пороги — дословно из spec §3.7.)

- [ ] **Шаг 4: Alertmanager — два конфига + entrypoint**

`alertmanager.null.yml` (пусто — только UI):

```yaml
route:
  receiver: default
  group_by: [alertname]
  group_wait: 30s
  group_interval: 5m
  repeat_interval: 4h
receivers:
  - name: default
```

`alertmanager.webhook.yml` (внешний канал — URL установки):

```yaml
route:
  receiver: default
  group_by: [alertname]
  group_wait: 30s
  group_interval: 5m
  repeat_interval: 4h
receivers:
  - name: default
    webhook_configs:
      - url: __WEBHOOK_URL__
```

`entrypoint.sh` (chmod +x; подстановка в рантайм-копию — ro-том не трогаем):

```sh
#!/bin/sh
# Д4 (spec §3.7): пустой METRICS_ALERT_WEBHOOK_URL — только UI (null-ресивер);
# непустой — generic webhook. Итоговый конфиг — в /tmp (том смонтирован ro).
set -e
if [ -n "$METRICS_ALERT_WEBHOOK_URL" ]; then
  sed "s|__WEBHOOK_URL__|$METRICS_ALERT_WEBHOOK_URL|" \
    /etc/alertmanager/alertmanager.webhook.yml > /tmp/alertmanager.yml
else
  cp /etc/alertmanager/alertmanager.null.yml /tmp/alertmanager.yml
fi
exec alertmanager --config.file=/tmp/alertmanager.yml --storage.path=/alertmanager
```

- [ ] **Шаг 5: Grafana provisioning + дашборды**

`provisioning/datasources/prometheus.yml`:

```yaml
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
```

`provisioning/dashboards/provider.yml`:

```yaml
apiVersion: 1
providers:
  - name: stand-dashboards
    folder: PgWorker Stand
    type: file
    options: {path: /var/lib/grafana/dashboards}
```

`dashboards/workers.json` — каркас (uid `workers`, datasource `Prometheus`; панели: `up` (stat), staleness per-loop (timeseries), claims (stat), фазы (timeseries), операции (timeseries), возраст снапшота (stat)):

```json
{
  "uid": "workers", "title": "Workers (PgWorker/KafkaWorker)", "schemaVersion": 41,
  "refresh": "30s", "time": {"from": "now-1h", "to": "now"},
  "panels": [
    {"type": "stat", "title": "Services up", "gridPos": {"x":0,"y":0,"w":8,"h":4},
     "targets": [{"expr": "up{job=~\"pgworker|kafkaworker|adminpanel\"}", "refId": "A"}]},
    {"type": "timeseries", "title": "Loop staleness (time() - last success)", "gridPos": {"x":0,"y":4,"w":12,"h":8},
     "targets": [{"expr": "time() - worker_loop_last_success_timestamp_seconds", "legendFormat": "{{job}}/{{loop}}", "refId": "A"}]},
    {"type": "stat", "title": "Claims held", "gridPos": {"x":12,"y":4,"w":6,"h":4},
     "targets": [{"expr": "worker_claims_held", "refId": "A"}]},
    {"type": "stat", "title": "Snapshot age, s", "gridPos": {"x":18,"y":4,"w":6,"h":4},
     "targets": [{"expr": "worker_snapshot_age_seconds", "refId": "A"}]},
    {"type": "timeseries", "title": "Process phases (age, s)", "gridPos": {"x":0,"y":12,"w":12,"h":8},
     "targets": [{"expr": "worker_process_phase_duration_seconds", "legendFormat": "{{cluster}} {{process}}/{{phase}}", "refId": "A"}]},
    {"type": "timeseries", "title": "Operations (increase 15m)", "gridPos": {"x":12,"y":12,"w":12,"h":8},
     "targets": [{"expr": "increase(worker_operation_total[15m])", "legendFormat": "{{operation}} {{result}}", "refId": "A"}]}
  ]
}
```

`dashboards/kafka.json`:

```json
{
  "uid": "kafka", "title": "Kafka (USR / consumer-lag / collector)", "schemaVersion": 41,
  "refresh": "30s", "time": {"from": "now-1h", "to": "now"},
  "panels": [
    {"type": "timeseries", "title": "Consumer lag (by group/topic)", "gridPos": {"x":0,"y":0,"w":16,"h":9},
     "targets": [{"expr": "kafka_consumer_lag", "legendFormat": "{{cluster}} {{group}}@{{topic}}", "refId": "A"}]},
    {"type": "stat", "title": "Under-replicated partitions", "gridPos": {"x":16,"y":0,"w":8,"h":4},
     "targets": [{"expr": "sum by (cluster) (kafka_under_replicated_partitions)", "refId": "A"}]},
    {"type": "stat", "title": "Collector staleness, s (alert > 300)", "gridPos": {"x":16,"y":4,"w":8,"h":5},
     "targets": [{"expr": "time() - kafka_collector_last_success_timestamp_seconds", "refId": "A"}]}
  ]
}
```

`dashboards/pg.json`:

```json
{
  "uid": "pg", "title": "PG replication (Patroni)", "schemaVersion": 41,
  "refresh": "30s", "time": {"from": "now-1h", "to": "now"},
  "panels": [
    {"type": "timeseries", "title": "Replica lag, s (by scope/node)", "gridPos": {"x":0,"y":0,"w":16,"h":9},
     "targets": [{"expr": "pg_replica_lag_seconds", "legendFormat": "{{scope}}/{{node}}", "refId": "A"}]},
    {"type": "stat", "title": "Patroni nodes up", "gridPos": {"x":16,"y":0,"w":8,"h":4},
     "targets": [{"expr": "up{job=\"patroni\"}", "legendFormat": "{{instance}}", "refId": "A"}]},
    {"type": "stat", "title": "Control plane up", "gridPos": {"x":16,"y":4,"w":8,"h":5},
     "targets": [{"expr": "up{job=~\"pgworker|adminpanel\"}", "legendFormat": "{{job}}", "refId": "A"}]}
  ]
}
```

- [ ] **Шаг 6: Проверка конфигов локально**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t04-unified-metrics/dev-stand/adminpanel
docker compose --profile metrics config >/dev/null && echo "compose OK"
docker run --rm --entrypoint /bin/promtool \
  -v "$PWD/metrics/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro" \
  -v "$PWD/metrics/prometheus/rules.yml:/etc/prometheus/rules.yml:ro" \
  prom/prometheus:v3.6.1 check config /etc/prometheus/prometheus.yml
docker run --rm --entrypoint /bin/amtool \
  -v "$PWD/metrics/alertmanager/alertmanager.null.yml:/etc/alertmanager/am.yml:ro" \
  prom/alertmanager:v0.28.1 check-config /etc/alertmanager/am.yml
```
Expected: `compose OK`; `promtool check config` — SUCCESS (включая rules.yml); `amtool check-config` — SUCCESS.

- [ ] **Шаг 7: Коммит**

```bash
git add dev-stand/adminpanel/docker-compose.yml dev-stand/adminpanel/metrics dev-stand/adminpanel/.env.example
git commit -m "t04: профиль metrics стенда — prometheus/grafana/alertmanager, rules §3.7, дашборды §5.3 (arch/18 §5)"
```

### Task 6.3: `00-up.sh` + чек `65-metrics.sh` + README

**Files:**
- Modify: `dev-stand/adminpanel/checks/00-up.sh`
- Create: `dev-stand/adminpanel/checks/65-metrics.sh` (chmod +x)
- Modify: `dev-stand/adminpanel/README.md`

**Interfaces:**
- Produces: полный подъём включает `--profile metrics`; чек 65 — критерий приёмки spec §6.3–6.5.

- [ ] **Шаг 1: Профиль в `00-up.sh`**

Строку `docker compose --profile full --profile kafka up -d --build` заменить на:

```bash
docker compose --profile full --profile kafka --profile metrics up -d --build 2>&1 | tail -5
```

и в конец (после шага 8 «панель жива») добавить шаг 9 — ожидание готовности мониторинга:

```bash
# 9) мониторинг (профиль metrics): Prometheus/Grafana/Alertmanager живы; таргеты
#    прогреваются scrape-интервалом — готовность проверяет 65-metrics.sh.
for i in $(seq 1 60); do curl -fsS -m 3 "http://localhost:${METRICS_PROMETHEUS_PORT:-9090}/-/ready" >/dev/null 2>&1 && break; sleep 1; done
curl -fsS -m 3 "http://localhost:${METRICS_PROMETHEUS_PORT:-9090}/-/ready" >/dev/null \
  || { echo "❌ prometheus не готов за 60 c (docker compose logs prometheus)"; exit 1; }
echo "  prometheus готов (:${METRICS_PROMETHEUS_PORT:-9090})"
```

- [ ] **Шаг 2: Чек `65-metrics.sh`**

Образец стиля — существующие чеки (`set -euo pipefail`, `cd "$(dirname "$0")/.."`, jq/curl). ВАЖНО (ревью Ф4-3): чек запускается и ПОСЛЕ серии чеков, где `50-kafka-api.sh:180-191` штатно ОСТАНАВЛИВАЕТ `as-kafkaworker`, а 30-й может оставить следы failover — поэтому шаг 0 гарантирует живость обоих воркеров ДО любых проверок.

```bash
#!/usr/bin/env bash
# E2E-чек мониторинга (t04, spec §6.3–6.5): /metrics трёх сервисов, все scrape-джобы
# up, rules зарегистрированы, дашборды загружены, Alertmanager жив, алерт-симуляция
# ServiceDown (down kafkaworker → up==0 ≤2мин → ServiceDown firing → восстановление).
set -euo pipefail
cd "$(dirname "$0")/.."
PROM="http://localhost:${METRICS_PROMETHEUS_PORT:-9090}"
GRAFANA="http://localhost:${METRICS_GRAFANA_PORT:-3000}"
AM="http://localhost:${METRICS_ALERTMANAGER_PORT:-9093}"

echo ">>> чек 65: мониторинг (профиль metrics)"
# 0) гарантия живости воркеров (ревью Ф4-3): 50-kafka-api.sh штатно останавливает
#    as-kafkaworker финальным шагом; deploy-pgworker переживает серию чеков, но
#    проверяем оба — чек обязан проходить после ЛЮБОЙ предыстории серии.
docker start as-kafkaworker >/dev/null 2>&1 || true
for i in $(seq 1 60); do curl -fsS -m 3 http://localhost:8082/healthz >/dev/null 2>&1 && break; sleep 1; done
curl -fsS -m 3 http://localhost:8082/healthz >/dev/null \
  || { echo "❌ kafkaworker не ожил за 60 c на :8082 (docker compose logs kafkaworker)"; exit 1; }
for i in $(seq 1 60); do curl -fsS -m 3 http://localhost:8080/healthz >/dev/null 2>&1 && break; sleep 1; done
curl -fsS -m 3 http://localhost:8080/healthz >/dev/null \
  || { echo "❌ pgworker не жив на :8080 — поднимите стенд: checks/00-up.sh (deploy-pgworker-1, docker logs deploy-pgworker-1)"; exit 1; }
echo "  воркеры живы (pgworker :8080, kafkaworker :8082)"

# 1) /metrics трёх сервисов (хост-публикации: deploy 8080, стенд 8082/5050)
for u in http://localhost:8080/metrics http://localhost:8082/metrics http://localhost:5050/metrics; do
  curl -fsS -m 5 "$u" | grep -q "^# HELP" || { echo "  ❌ $u не отдал text-format"; exit 1; }
done
echo "  /metrics трёх сервисов: 200 text-format"

# 2) все scrape-джобы up (ждать прогрева: 15с scrape + оценка rules)
for i in $(seq 1 30); do
  down=$(curl -fsS "$PROM/api/v1/targets" | jq '[.data.activeTargets[] | select(.labels.job!="patroni")] | length')
  [ "$down" -gt 0 ] && break; sleep 2
done
bad=$(curl -fsS "$PROM/api/v1/targets" | jq -r '[.data.activeTargets[] | select(.health!="up")] | .[] | .labels.job+"/"+.labels.instance' | tr '\n' ' ')
[ -z "$bad" ] || { echo "  ❌ таргеты не up: $bad"; exit 1; }
patroni_up=$(curl -fsS "$PROM/api/v1/targets" | jq '[.data.activeTargets[] | select(.labels.job=="patroni" and .health=="up")] | length')
[ "$patroni_up" -ge 2 ] || { echo "  ❌ patroni-эмуляторы: up только $patroni_up (<2)"; exit 1; }
echo "  scrape-джобы up (включая patroni: $patroni_up)"

# 3) серии словаря у живых таргетов (канонические имена arch/18 §2)
for s in worker_loop_ticks_total kafka_collector_last_success_timestamp_seconds pg_replica_lag_seconds; do
  curl -fsS --data-urlencode "query=$s" "$PROM/api/v1/query" | jq -e '.data.result | length > 0' >/dev/null \
    || { echo "  ❌ серия $s не найдена в TSDB"; exit 1; }
done
echo "  серии словаря arch/18 §2 в TSDB"

# 4) rules зарегистрированы (8 алертов §3.7)
rules=$(curl -fsS "$PROM/api/v1/rules" | jq '[.data.groups[].rules[] | select(.type=="alerting")] | length')
[ "$rules" -ge 8 ] || { echo "  ❌ алерт-рулы: $rules < 8"; exit 1; }
echo "  rules: $rules алертов зарегистрировано"

# 5) Grafana: дашборды провиженены (basic admin/admin — стенд)
ds=$(curl -fsS -u admin:admin "$GRAFANA/api/search?type=dash-db" | jq 'length')
[ "$ds" -ge 3 ] || { echo "  ❌ дашборды: $ds < 3"; exit 1; }
echo "  Grafana: $ds дашборда"

# 6) Alertmanager жив
curl -fsS "$AM/api/v2/status" | jq -e '.versionInfo.version' >/dev/null || { echo "  ❌ alertmanager /api/v2/status"; exit 1; }
echo "  Alertmanager жив"

# 7) алерт-симуляция (spec §6.5): stop kafkaworker → up==0 + for:2m → ServiceDown
#    firing → восстановление. Бюджет: scrape 15с + for 2м — ранний выход циклами.
docker stop as-kafkaworker >/dev/null
firing=""
for i in $(seq 1 30); do
  firing=$(curl -fsS "$PROM/api/v1/alerts" | jq -r '[.data.alerts[] | select(.labels.alertname=="ServiceDown" and .state=="firing")] | length')
  [ "$firing" -gt 0 ] && break; sleep 10
done
docker start as-kafkaworker >/dev/null
[ "${firing:-0}" -gt 0 ] || { echo "  ❌ ServiceDown не перешёл в firing ≤~2.5мин после остановки kafkaworker"; exit 1; }
echo "  алерт-симуляция: ServiceDown firing после остановки kafkaworker (восстановлен)"

echo "✓ чек 65: мониторинг жив (prometheus/grafana/alertmanager/rules/серии)"
```

Тайминги симуляции: scrape 15с + `for: 2m` — общий бюджет ожидания ≤ 5 мин, циклы `sleep 10` с ранним выходом; критерий приёмки «срабатывает ≤ 2 мин» отсчитывается от первого пропущенного scrape (лаг ≤15с сверх — фиксируется в выводе чека).

- [ ] **Шаг 3: README стенда**

В `dev-stand/adminpanel/README.md`: строка профиля в таблицу («metrics | + prometheus (:9090), grafana (:3000, admin/admin), alertmanager (:9093) | мониторинг полной системы: дашборды, алерты §3.7; входит в 00-up.sh»); секция «Мониторинг»: env-переменные (`METRICS_PROMETHEUS_PORT`, `METRICS_GRAFANA_PORT`, `METRICS_ALERTMANAGER_PORT`, `METRICS_ALERT_WEBHOOK_URL` — пусто = только UI; дефолты в `.env.example`), чек `checks/65-metrics.sh` (запускается после серии чеков — сам поднимает остановленного kafkaworker'а), путь конфигов `metrics/`, ссылка на arch/18 §5.

- [ ] **Шаг 4: Полный подъём + чек**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t04-unified-metrics/dev-stand/adminpanel
checks/00-up.sh && checks/65-metrics.sh
```
Expected: оба зелёные (стенд поднимает всё, включая metrics; чек 65 проходит все 7 шагов). Дополнительно прогнать «после серии»: `checks/50-kafka-api.sh || true; checks/65-metrics.sh` — шаг 0 гарантирует живость.

- [ ] **Шаг 5: Коммит**

```bash
git add dev-stand/adminpanel/checks dev-stand/adminpanel/README.md
git commit -m "t04: полный подъём с профилем metrics + чек 65 (гарантия живости воркеров, targets/rules/dashboards/ServiceDown-симуляция)"
```

---

## Фаза Ф7 — закрытие: приёмка, E2E-гейт, roadmap-чистка

### Task 7.1: Полная приёмка + roadmap-чистка

**Files:**
- Modify: `arch/roadmap/pgworker.md`, `arch/roadmap/kafkaworker.md` (удалить теги `t04-metrics`/`t04-kafka-metrics`: строки пунктов + упоминания в `←`-зависимостях других пунктов, если есть — `grep -rn "t04-" arch/roadmap/`)
- Modify: `arch/18-metrics.md`, `dev-stand/adminpanel/README.md` — только если имплементация вскрыла отклонения (актуализация канона, spec §6.8)

**Interfaces:** нет (закрытие).

- [ ] **Шаг 1: Полный билд + все тесты монорепо**

Run: `cd /Users/demakaev/ZCodeProject/worktrees/feat-t04-unified-metrics && dotnet build src/PgWorker.slnx && dotnet test src/PgWorker.slnx`
Expected: 0 warnings; unit+integration зелёные, включая `Shared.Metrics.UnitTests` (терминальные фазы/подавления/лейблы) и `/metrics`-тесты трёх сервисов (критерий spec §6.1).

- [ ] **Шаг 2: E2E-гейт AGENTS (трогаем src/PgWorker.App)**

Run: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Scale_AddEmptyShard`
Expected: PASS на свежем Release (критерий spec §6.6; E2eFixture собирает сам).

- [ ] **Шаг 3: Стенд финально**

Run: `cd dev-stand/adminpanel && checks/00-up.sh && checks/65-metrics.sh` → зелёные (§6.4–6.5).

- [ ] **Шаг 4: Roadmap-чистка (мерж-гейт)**

Удалить пункт `t04-metrics` из `arch/roadmap/pgworker.md` и `t04-kafka-metrics` из `arch/roadmap/kafkaworker.md` — вместе с зависимостями `← t04-…` в других пунктах этих файлов, если grep их найдёт. Никаких пометок «сделано» — история в git.

- [ ] **Шаг 5: Финальный коммит**

```bash
git add arch/roadmap
git commit -m "merge: t04 — единые Prometheus-метрики: Puzzle-канон + Shared.Metrics, инструментация PgWorker/KafkaWorker/AdminPanel (словарь arch/18 §2, терминальные фазы из фактического словаря журналов), профиль metrics стенда (prometheus/grafana/alertmanager, rules, чек 65, ServiceDown-симуляция); теги t04-metrics/t04-kafka-metrics удалены (мерж-гейт)"
```

(Мерж в main — по флоу dev-flow, отдельным решением main-агента: ревью + пуш.)

---

## Самопроверка плана (выполнена автором; обновлена по ревью Фазы 4)

1. **Покрытие spec:** §1.1 каркас = Ф1+Ф2; §1.2 инструментация = Ф3–Ф5 (PgWorker циклы/фазы/операции, KafkaWorker + коллектор §2.3–§4, панель §2.4); §1.3 хранение = Ф6 (compose, prometheus/grafana/alertmanager, scrape 15с, Patroni-эмуляторы, чек); §1.4 алертинг = Ф6 Т6.2 Шаг 3–4 (8 rules + webhook env); §3.1–§3.7 — по задачам выше; §4 фазы Ф1→Ф2→{Ф3,Ф4,Ф5}→Ф6→Ф7 — порядок плана; §6 критерии: 6.1=Ф7 Ш1, 6.2=Ф1, 6.3=Ф3.1/Ф4.2/Ф5.1/Ф6.3, 6.4–6.5=Ф6.3 Шаг 2, 6.6=Ф7 Шаг 2, 6.7=Ф7 Шаг 4, 6.8=Ф6.3 Шаг 3/Ф7 Шаг 4; риски S1/M3=Т2.3, S2=Т3.2/Т4.4, S3=Т2.2 (first-seen+ProcessFinished+терминальные фазы), S4=Т4.3, S5/M4=Т6.2 env-порты, S6=Т6.1 (state c lock), S7=Ф1 отдельной фазой.
2. **Ревью Фазы 4:** (1) терминальные фазы — фактический словарь `{done, failed, crashed, rejected, cancelled}` (Т2.2: `FinalPhases` + тесты `OnJournalPhase_FinalPhases…`/`…Rejected…`); `skipped` — промежуточная по коду (AdoptionProcess:128→done:180/failed:488), в финальные не входит, тест фиксирует поведение; (2) `supervise`/`evacuate` подавлены (`SuppressedOps`, тест `…SuppressedOps…`), `WriteSupervisionAsync` событие не эмитит (Т3.2 кейс 2), альтернатива отвергнута с обоснованием; (3) чек 65 — шаг 0 стартует kafkaworker и ждёт `/healthz`, pgworker проверяется (Т6.3); (4) контракт источника серий — arch/18 §1 + spec §3.2 переформулированы (внесено в ревью-итерации, Т2.2 Шаг 6 везёт тем же коммитом); (5) перечень `process` в arch/18 §2.2 = фактические Op (внесено), тест Т2.3-2 фиксирует значения лейблов; (6) фильтр Active `Config.State == null` — Т4.3 Шаг 3 + кейс `Collect_OnlyActiveClusters…`; (7) эмулятор экспортирует только `NODE_NAME` — Т6.1; (8) гейдж панели — pull над `BuiltAtUtc`, обоснование строкой в Т5.1; fallback `host.docker.internal` для kafkaworker/adminpanel — осознанно опущен с обоснованием (Т6.2 Шаг 2). Раунд 2: (1) ops `rollback`/`finalize` добавлены в arch/18 §2.2 и тест Т2.3-2 (итого 11 ops PgWorker; закрытие их серий по done/rejected — `RejectAsync` принимает op); (2) отказ от fallback отзеркален в spec §3.6 и arch/18 §5.2 (несуществующий 8081 устранён, хост-публикация 8082 — только чеки); (3) пример в «Смысле» `worker_operation_total` arch/18 §2.2 согласован с `SuppressedOps` (evacuate убран).
3. **Плейсхолдеры:** шаги содержат конкретный код/YAML/JSON/команды; единственные «свериться при имплементации» — фактические API-имена пин-версий OTel/Confluent (закрыты тестами Т1.2/Т2.3) — осознанный допуск, зафиксированный риском M3.
4. **Типы:** `AddAppMetrics(string, IConfiguration)`/`MapAppMetrics` идентичны в Ф1/Ф2; `WorkerMetricsInstrumentation` API (включая `FinalPhases`/`SuppressedOps`) единообразен в Ф2–Ф4; `OnJournalPhase` согласован с событием `WorkJournal.PhaseWritten`/`WriteAsync` (Т3.2/Т4.4); `KafkaGroupView`/`KafkaTopicPartition(Offset)` объявлены в Т4.1 и потреблены в Т4.3; конструкторы `KafkaMetricsCollector`/`KafkaMetricsState` согласованы Т4.3↔Т4.4.
