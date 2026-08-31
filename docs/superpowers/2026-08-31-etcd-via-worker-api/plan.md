# etcd-via-worker-api: webapi воркеров, ключи доступа, прокси панели, сид через API, объяснимые алерты — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Перенести все записи декларативного etcd-контракта (pg + kafka) от панели и сидов в HTTP API исполнителей — PgWorker и KafkaWorker (ключи доступа `/pgworker/api/<id>`, `/kafkaworker/api/<id>`), панель становится read-only по etcd и проксирует мутации; сиды наливаются через `POST /api/seed/demo`; каждый алерт получает Hint и Remedy-движитель.

**Architecture:** Оба воркера получают minimal-API модуль `/api` на существующей Kestrel-грани (`:8080`) и сами ставят lease-ключ доступа в etcd рядом с `instances/<id>`. Планы записи и валидаторы переезжают из `AdminPanel.Etcd/Writing` в `PgWorker.Core` / `KafkaWorker.Core` (guards переписываются с панельного снапшота на прямые чтения etcd). Панель получает `WorkerApiGateway` (резолв URL по живым ключам из снапшота, failover, `X-Api-Key`) и переписывает command-хендлеры на HTTP-прокси с сохранением фронт-контракта 1:1. Модель `Alert` расширяется `Hint`/`Remedy`/`RemedyText` (движитель worker-auto|operator-api|operator-runbook), добавляется kind `worker-api-unreachable`.

**Tech Stack:** .NET 10, ASP.NET Core Minimal API, xUnit v3 + FluentAssertions + Testcontainers (etcd `quay.io/coreos/etcd:v3.5.21`), WebApplicationFactory, React+TS SPA (Vite).

**Spec:** `docs/superpowers/2026-08-31-etcd-via-worker-api/spec.md` (в этой же папке; канон контрактов уже обновлён в `arch/` — см. spec §8). Читай spec и plan вместе.

## Global Constraints

- Worktree: `/Users/demakaev/ZCodeProject/worktrees/feat-etcd-via-worker-api` (ветка `feat-etcd-via-worker-api`); работай ТОЛЬКО здесь; коммиты — свободно, после каждой задачи.
- Сборка: `TreatWarningsAsErrors=true` — 0 warnings обязательно. Решение: `src/PgWorker.slnx`. Пакеты — централизованно в `src/Directory.Packages.props` (новые пакеты НЕ вводить).
- Тесты: env `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1`; docker-тесты включаются env `PGW_TEST_DOCKER=1` (без него часть тестов `skip-failed` — это норма). Базовые количества на main: PgWorker 388+31, AdminPanel 462+130, KafkaWorker 102+4 (после переноса тестов AdminPanel.UnitTests уменьшится на перенесённые Writing/kafka-команды, KafkaWorker/PgWorker — вырастут; контроль: нет провалившихся и нет потерянных тест-кейсов).
- Воркеры и панель запускаются ТОЛЬКО в docker (никогда `dotnet run`); API-URL из докер-сети панели: `http://kafkaworker:8080` (общая сеть adminpanel-stand), `http://host.docker.internal:8080` (PgWorker из deploy), `http://host.docker.internal:8081` (KafkaWorker из deploy).
- etcd-контур один: as-etcd стенда (`host.docker.internal:2379` advertise).
- Дубли кода панель↔воркеры (в т.ч. зеркальные DTO запросов/ответов API) — осознанные; НЕ унифицировать (roadmap t08).
- Язык: комментарии/доки — русские, идентификаторы — английские.
- Команды сборки/тестов (из корня worktree):
  - `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet build src/PgWorker.slnx` → 0 warnings, 0 errors.
  - `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet test src/tests/<Project>` — быстрый прогон (без docker-тестов).
  - `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 PGW_TEST_DOCKER=1 dotnet test src/tests/<Project>` — полный прогон проекта.
- Фронт: `cd frontend && npm run build` для проверки SPA-сборки (бандл панели).
- Полный стенд: `cd dev-stand/adminpanel && checks/00-up.sh`; сброс: `checks/90-down.sh -v` (том etcd уходит — сиды наливаются заново); kfw-контейнеры чистить вручную (`docker ps -a | grep kfw-`).

---

### Task 1: PgWorker — опции `Api` + ключ доступа `/pgworker/api/<id>` в ClaimStore

**Files:**
- Modify: `src/PgWorker.App/Options.cs` (новый `ApiOptions`)
- Modify: `src/PgWorker.Etcd/Coordination/ClaimStore.cs` (`EnsureInstanceKeyAsync` + параметр `advertiseApiUrl`)
- Modify: `src/PgWorker.App/Program.cs` (fail-fast AdvertiseUrl, передача в ClaimStore)
- Modify: `src/PgWorker.App/appsettings.json` (секция `PgWorker:Api`)
- Test: `src/tests/PgWorker.IntegrationTests/Etcd/EtcdCoordinationTests.cs` (новый кейс)

**Interfaces:**
- Consumes: `ClaimStore(string[] endpoints, IEtcdGateway gateway, TimeProvider clock)` (существующий).
- Produces (для Task 4/6 и зеркала Task 2):
  - `ClaimStore(string[] endpoints, IEtcdGateway gateway, TimeProvider clock, string? advertiseApiUrl = null)` — при непустом `advertiseApiUrl` keepalive-контур ставит lease-ключ `/pgworker/api/<InstanceId>` value `{"url":"<url>","instance":"<id>","since_unix":<unix>}` (тот же lease, что `instances/<id>` — гаснут вместе).
  - `PgWorkerOptions.Api : ApiOptions` где `public sealed class ApiOptions { public string AdvertiseUrl { get; set; } = ""; public bool EnableSeedEndpoint { get; set; } }`.

- [ ] **Step 1: Написать падающий интеграционный тест**

В `EtcdCoordinationTests.cs` добавить кейс (по образцу существующих тестов ClaimStore в этом файле — там уже есть EtcdFixture с реальным etcd; тесты с `[SkippableFact]`/skip-механикой проекта):

```csharp
// AAA: инстанс ClaimStore с advertiseApiUrl ставит два lease-ключа; api-ключ
// содержит url из аргумента; DisposeAsync гасит lease — ключи исчезают.
[SkippableFact]
public async Task StartAsync_WithAdvertiseApiUrl_PutsApiDiscoveryKey()
{
    // Arrange
    Skip.IfNot(Environment.GetEnvironmentVariable("PGW_TEST_DOCKER") is not null,
        "docker-тесты — только с PGW_TEST_DOCKER");
    await using var fixture = new EtcdFixture();
    await fixture.InitializeAsync();
    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
    await using var store = new ClaimStore(
        [fixture.Endpoint], fixture.Gateway, TimeProvider.System,
        advertiseApiUrl: "http://host.docker.internal:8080");

    // Act
    await store.StartAsync(cts.Token);
    await Task.Delay(500, cts.Token); // keepalive-цикл ставит ключи асинхронно

    // Assert
    var api = await fixture.Gateway.RangeAsync(fixture.Endpoint, "/pgworker/api/", cts.Token);
    api.IsSuccess.Should().BeTrue();
    var kv = api.Value.Should().ContainSingle().Subject;
    kv.Key.Should().Be($"/pgworker/api/{store.InstanceId}");
    // Контракт snake_case (arch/02 §2.3.1): {"url","instance","since_unix"} —
    // атрибуты JsonPropertyName обязательны (PayloadJson без naming policy);
    // NotContain-проверки ловят регрессию к PascalCase.
    kv.Value.Should().Contain("\"url\":\"http://host.docker.internal:8080\"")
        .And.Contain($"\"instance\":\"{store.InstanceId}\"")
        .And.Contain("\"since_unix\":")
        .And.NotContain("\"Url\"").And.NotContain("\"Instance\"").And.NotContain("\"SinceUnix\"");
}
```

(Если в файле другой skip-паттерн — используй его; суть: один ключ, value JSON с url+instance.)

- [ ] **Step 2: Прогнать тест — убедиться в падении**

Run: `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet test src/tests/PgWorker.IntegrationTests --filter ApiDiscoveryKey`
Expected: FAIL (компиляция: у ClaimStore нет параметра `advertiseApiUrl`).

- [ ] **Step 3: Реализовать минимально**

`Options.cs` — внутрь `PgWorkerOptions` добавить `public ApiOptions Api { get; set; } = new();` и в конец файла:

```csharp
/// <summary>HTTP API воркера (arch/14 §1.1): advertise-URL в /pgworker/api/&lt;id&gt;
/// + стендовый сид-эндпоинт.</summary>
public sealed class ApiOptions
{
    /// <summary>URL API, достижимый клиентами (панелью); пусто → fail-fast старта.</summary>
    public string AdvertiseUrl { get; set; } = "";

    /// <summary>Демо-сид-эндпоинт POST /api/seed/demo (стенд; default false).</summary>
    public bool EnableSeedEndpoint { get; set; }
}
```

`ClaimStore.cs`:
1. Конструктор: `public sealed class ClaimStore(string[] endpoints, IEtcdGateway gateway, TimeProvider clock, string? advertiseApiUrl = null)` + поле `private readonly string? _advertiseApiUrl = advertiseApiUrl;`.
2. `EnsureInstanceKeyAsync`: после успешного put `instances/<id>` (и до взятия `_instanceLease`) — если `_advertiseApiUrl` непуст, вторым put НА ТОМ ЖЕ lease:

```csharp
if (_advertiseApiUrl is { Length: > 0 } url)
{
    var payload = JsonSerializer.Serialize(
        new ApiDiscoveryPayload(url, InstanceId, Now()), PayloadJson.Json);
    var apiPut = await WithFailoverAsync(endpoint => gateway.PutAsync(
        endpoint, $"/pgworker/api/{InstanceId}", payload, grant.Value, ct));
    if (!apiPut.IsSuccess)
    {
        await RevokeSilentlyAsync(grant.Value);
        return; // оба ключа ставятся на одном lease: отказ = ни одного
    }
}
```

и рядом с `ClaimPayload` (низ файла):

```csharp
// Value ключа /pgworker/api/<id> (arch/14 §1.1): {"url","instance","since_unix"}.
// ВАЖНО: PayloadJson.Json НЕ задаёт PropertyNamingPolicy (дефолт PascalCase) —
// поля маппим атрибутами, как у ClaimPayload, иначе парсер панели (контракт
// arch/02 §2.3.1/§2.3.2 ждёт snake_case) не распарсит значение.
public sealed record ApiDiscoveryPayload(
    [property: JsonPropertyName("url")] string Url,
    [property: JsonPropertyName("instance")] string Instance,
    [property: JsonPropertyName("since_unix")] long SinceUnix);
```

(Значение `since_unix` — `Now()` как у ClaimPayload; сериализация — тем же `PayloadJson.Json`.)

- [ ] **Step 4: Program.cs — fail-fast + передача**

`src/PgWorker.App/Program.cs`:
1. После `builder.Services.Configure<PgWorkerOptions>(...)` добавить валидацию:

```csharp
builder.Services.AddOptions<PgWorkerOptions>()
    .Validate(o => !string.IsNullOrWhiteSpace(o.Api.AdvertiseUrl),
        "PgWorker:Api:AdvertiseUrl не задан (URL API, достижимый панелью; env PGW_API_ADVERTISE_URL)")
    .ValidateOnStart();
```

2. Регистрацию ClaimStore дополнить четвёртым аргументом: `sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Api.AdvertiseUrl`.

`appsettings.json` (PgWorker.App) — добавить секцию с пустым AdvertiseUrl (env обязателен; deploy задаёт):

```json
"Api": { "AdvertiseUrl": "", "EnableSeedEndpoint": false }
```

Внимание: ValidateOnStart упадёт при пустом AdvertiseUrl — это требование spec (fail-fast). Все места, поднимающие хост PgWorker (тесты Task 4+, deploy), обязаны задавать env `PgWorker__Api__AdvertiseUrl`.

- [ ] **Step 5: Прогнать тесты — зелёный + регресс**

Run: `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests --filter EtcdCoordination` затем `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet build src/PgWorker.slnx`
Expected: новый тест PASS, координационные не сломаны, build 0 warnings.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(pgworker): Api-опции + lease-ключ /pgworker/api/<id> (дискавери API, arch/14 §1.1)"
```

---

### Task 2: KafkaWorker — опции `Api` + ключ `/kafkaworker/api/<id>` (зеркал Task 1)

**Files:**
- Modify: `src/KafkaWorker.App/Options.cs` (`KafkaWorkerOptions.Api : ApiOptions` — свой `ApiOptions`-класс в этом файле, неймспейс KafkaWorker.App; поля те же: `AdvertiseUrl`, `EnableSeedEndpoint`)
- Modify: `src/KafkaWorker.Etcd/Coordination/ClaimStore.cs` (та же правка: параметр `advertiseApiUrl = null`, `ApiDiscoveryPayload`, ключ `/kafkaworker/api/{InstanceId}` в `EnsureInstanceKeyAsync`)
- Modify: `src/KafkaWorker.App/Program.cs` (ValidateOnStart `KafkaWorker:Api:AdvertiseUrl` — ВНИМАНИЕ: у KafkaWorker Program уже есть `.AddOptions<KafkaWorkerOptions>().Validate(...Etcd:Endpoints...)` — дописать `.Validate(o => !string.IsNullOrWhiteSpace(o.Api.AdvertiseUrl), "KafkaWorker:Api:AdvertiseUrl не задан (env KFW_API_ADVERTISE_URL)")` в ту же цепочку; передача 4-го аргумента в ClaimStore)
- Modify: `src/KafkaWorker.App/appsettings.json` (секция `"Api": { "AdvertiseUrl": "", "EnableSeedEndpoint": false }`)
- Test: `src/tests/KafkaWorker.IntegrationTests/Etcd/ClaimStoreTests.cs` (новый кейс)

**Interfaces:**
- Produces: `ClaimStore(string[] endpoints, IEtcdGateway gateway, TimeProvider clock, string? advertiseApiUrl = null)` (KafkaWorker.Etcd.Coordination); ключ `/kafkaworker/api/<id>`; `KafkaWorkerOptions.Api`.

- [ ] **Step 1: Падающий тест**

В `ClaimStoreTests.cs` — кейс `StartAsync_WithAdvertiseApiUrl_PutsApiDiscoveryKey` (полный код кейса — см. Task 1 Step 1; отличия: неймспейс `KafkaWorker.Etcd.Coordination`, fixture — как устроен в этом файле: там свой etcd-фикстур или переиспользуется; если файла-фикстуры нет — тесты этого файла уже поднимают etcd Testcontainers — используй их паттерн), advertise URL `http://kafkaworker:8080`, префикс проверки `/kafkaworker/api/`.

- [ ] **Step 2: Прогнать — упасть**

Run: `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet test src/tests/KafkaWorker.IntegrationTests --filter ApiDiscoveryKey`
Expected: FAIL (компиляция).

- [ ] **Step 3: Реализовать (точная копия правок Task 1 Steps 3–4 для KafkaWorker — включая `[property: JsonPropertyName(...)]`-атрибуты `ApiDiscoveryPayload` и snake_case-ассерты теста: `PayloadJson.Json` у KafkaWorker тоже без naming policy)**

- [ ] **Step 4: Прогнать + build**

Run: `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 PGW_TEST_DOCKER=1 dotnet test src/tests/KafkaWorker.IntegrationTests --filter ClaimStore` ; `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet build src/PgWorker.slnx`
Expected: PASS, 0 warnings.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(kafkaworker): Api-опции + lease-ключ /kafkaworker/api/<id> (arch/16 §1.1)"
```

---

### Task 3: `PgWorker.Core.Writing` — перенос планов/валидаторов из панели

Перенос файлов с адаптацией типов: панельный etcd-клиент использует `TxnCompare(Key, Version, ModRevision?)`/`KvPut`, воркерский — `TxnCompare.NotExists(...)`/`TxnOp.Put(Key, Value, Lease)`. Планы делаем нейтральными к клиенту (чистые `(key, value)`-пары), txn строит хендлер (Task 4/5).

**Files:**
- Create: `src/PgWorker.Core/Writing/ValidationError.cs`
- Create: `src/PgWorker.Core/Writing/CreateClusterRequest.cs` (перенос `src/AdminPanel.Etcd/Writing/CreateClusterRequest.cs` дословно, namespace `PgWorker.Core.Writing`)
- Create: `src/PgWorker.Core/Writing/ClusterCreatePlan.cs` (перенос `ClusterCreatePlan.cs` с заменой выходных типов — см. Step 2)
- Create: `src/PgWorker.Core/Writing/ShardScalePlan.cs` (аналогично)
- Test: `src/tests/AdminPanel.UnitTests/CreateClusterPlanTests.cs` → перенести в `src/tests/PgWorker.UnitTests/Writing/ClusterCreatePlanTests.cs` (namespace+usings поменять; фикстуры значений не менять)
- Test: `src/tests/AdminPanel.UnitTests/ShardScalePlanTests.cs` → `src/tests/PgWorker.UnitTests/Writing/ShardScalePlanTests.cs`
- Панельные файлы-оригиналы ПОКА не удалять (панель ещё их использует — удаление в Task 13).

**Interfaces:**
- Produces (для Task 4/5):
  - `namespace PgWorker.Core.Writing; public sealed record ValidationError(string Field, string Message);`
  - `public static class CreateClusterValidator { public static IReadOnlyList<ValidationError> Validate(CreateClusterRequest request); }` (перенос тела из `ClusterCreatePlan.cs`/оригинала панели — валидатор живёт в тех же файлах панели, перенести как есть, заменив только `ValidationError`-тип).
  - `public sealed record PlanPut(string Key, string Value);` (общая для обоих планов)
  - `ClusterCreatePlan`: те же члены, что у панельного (`Build(CreateClusterRequest request, long nowUnix)`, `ConfigKey`, `ConfigValue`, `Puts` → теперь `IReadOnlyList<PlanPut>`, `RequestKeys`, `CanonicalCpu/Mem/Disk`, `static int OwnerShard(int bucket, int buckets, int shards)`, `const string NotInitialized = "NOT_INITIALIZED"`).
  - `ShardScalePlan`: по образцу панельного (`Build(...)`, клэйм-ключ, `Puts : IReadOnlyList<PlanPut>`, `RequestKeys`).

- [ ] **Step 1: Перенести тесты (они упадут — нет типов)**

Скопировать два тест-файла в `src/tests/PgWorker.UnitTests/Writing/`, в usings заменить `AdminPanel.Etcd.Writing` → `PgWorker.Core.Writing`; убрать using AdminPanel-сборок. Прогнать:

Run: `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet test src/tests/PgWorker.UnitTests --filter ClusterCreatePlan`
Expected: FAIL компиляция (типов нет).

- [ ] **Step 2: Перенести модели и планы**

1. `ValidationError.cs` (код в Interfaces выше).
2. `CreateClusterRequest.cs`: перенести дословно из `src/AdminPanel.Etcd/Writing/CreateClusterRequest.cs` (включая `Normalize()`), сменив namespace. Проверь зависимости: там нет ссылок на AdminPanel-типы (чистая модель) — если есть (например, `ValidationError`) — замени на новый.
3. `ClusterCreatePlan.cs`: перенести из панели, изменения:
   - `Puts`-элементы: панельный код строил `new KvPut(key, value)` → `new PlanPut(key, value)`;
   - claim-подготовка: панельный план НЕ строил txn (это делал хендлер) — сохранить разделение: план отдаёт `ConfigKey`/`ConfigValue`/`Puts`/`RequestKeys`, txn строит хендлер Task 4;
   - приватные JSON-записи (`ConfigJson`, `StatusJson`) — дословно.
4. `ShardScalePlan.cs` — по образцу (клэйм-ключ `shards/<X>/replicas`, nodes×R + request_* в `Puts`).
5. Валидатор `CreateClusterValidator` — перенести из панельного `ClusterCreatePlan.cs`/соседних (найди `CreateClusterValidator.Validate` в `src/AdminPanel.Etcd/Writing/` и перенести тело без изменений, кроме типа ошибки).

- [ ] **Step 3: Прогнать перенесённые тесты**

Run: `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~Writing"`
Expected: PASS (все перенесённые кейсы зелёные — фикстуры те же, что были в панели).

- [ ] **Step 4: Build решения + Commit**

Run: `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet build src/PgWorker.slnx` → 0 warnings.

```bash
git add -A && git commit -m "feat(pgworker): Writing-ядро — перенос планов/валидаторов декларативного контракта из панели (значения 1:1)"
```

---

### Task 4: PgWorker API — каркас + `POST /api/clusters` + `DELETE /api/clusters/{c}`

**Files:**
- Create: `src/PgWorker.App/Api/ApiModule.cs` (маппинг эндпоинтов + маппинг исключений в ProblemDetails — порт панельного `OperationsModule` 1:1)
- Create: `src/PgWorker.App/Api/ApiKeyMiddleware.cs` (X-Api-Key)
- Create: `src/PgWorker.App/Api/Operations/WorkerApiExceptions.cs` (перенос панельных исключений: `CreateClusterValidationException`, `ClusterAlreadyExistsException`, `EtcdWriteUnavailableException` — тексты 1:1 из `src/AdminPanel.Api/Operations/CreateClusterCommand.cs`/`DeleteClusterCommand.cs`)
- Create: `src/PgWorker.App/Api/Operations/CreateClusterHandler.cs`
- Create: `src/PgWorker.App/Api/Operations/DeleteClusterHandler.cs`
- Modify: `src/PgWorker.App/Program.cs` (`public partial class Program;` в конец; DI хендлеров; `app.UseApiKey(); app.MapWorkerApi();`; ключ API — поле `public string? ApiKey { get; set; }` в `ApiOptions` Task 1, конфиг-путь `PgWorker:Api:ApiKey`, env-инъекция через compose `PgWorker__Api__ApiKey` — Task 16)
- Modify: `src/PgWorker.App/Options.cs` (`ApiOptions.ApiKey`)
- Test: `src/tests/PgWorker.IntegrationTests/Api/CreateClusterApiTests.cs` (WAF)

**Interfaces:**
- Consumes: `PgWorker.Core.Writing.*` (Task 3), `IEtcdGateway` (`RangeAsync`, `GetAsync`, `PutAsync`, `DeleteAsync`, `TxnAsync(endpoint, TxnRequest.Of([TxnCompare.NotExists(k)], [new TxnOp.Put(k, v, null)]), ct)`), `EtcdFixture` (существующий).
- Produces (для Task 5/6 и панельных стаб-тестов Task 13):
  - `POST /api/clusters` → 201 `ClusterCreatedDto` (поля как панельные: Name, DbName, Sharded, BucketsCount, ShardsTotal, Replicas, RequestCpu, RequestMem, RequestDisk, State) | 400 ProblemDetails c `extensions.errors = {field: [msg]}` | 409 | 503. Ответ БЕЗ Location (Location строит панель).
  - `DELETE /api/clusters/{name}` → 204 | 404 | 503.
  - `ApiKeyMiddleware`: если `PgWorker:Api:ApiKey` непуст — все `/api/*` требуют заголовок `X-Api-Key` равно значению, иначе 401 ProblemDetails `{"title":"Unauthorized"}`; пуст — passthrough.
  - WAF-фабрика тестов: `PgWorkerApiFactory` (в тестах, см. Step 1).

- [ ] **Step 1: WAF-фабрика + падающий тест**

В `src/tests/PgWorker.IntegrationTests/Api/` создать `PgWorkerApiFactory.cs`:

```csharp
// WAF-хост PgWorker с настоящим etcd (fixture) и выключенными фоновыми циклами:
// loops не нужны для API-мутаций, а их тики в тесте — шум.
public sealed class PgWorkerApiFactory(EtcdFixture etcd) : WebApplicationFactory<Program>
{
    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Development");
        builder.ConfigureAppConfiguration((_, cfg) => cfg.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["PgWorker:Etcd:Endpoints:0"] = etcd.Endpoint,
            ["PgWorker:Docker:Hosts:0:Name"] = "local",
            ["PgWorker:Docker:Hosts:0:Endpoint"] = "unix:///var/run/does-not-exist.sock",
            ["PgWorker:Api:AdvertiseUrl"] = "http://localhost:9999",
            ["PGW_PG_SUPERUSER_PASSWORD"] = "x", ["PGW_PG_STANDBY_PASSWORD"] = "x",
            ["PGW_BUCKET_ADMIN_PASSWORD"] = "x", ["PGW_BUCKET_MOVER_PASSWORD"] = "x",
        }));
        builder.ConfigureServices(services =>
            services.RemoveAll<IHostedService>()); // Reconcile/Keepalive/Snapshot не стартуют
    }
}
```

(env-секреты через конфиг-словарь не сработают — `SecretsFromEnv()` читает `Environment.GetEnvironmentVariable`; в тесте задать их `Environment.SetEnvironmentVariable(...)` в конструкторе фабрики ДО `CreateClient` и убрать в Dispose, либо поднять фабрику в collection-fixture с установленными переменными. Выбери второй вариант — stable.)

Тест `CreateClusterApiTests.cs` (порт контракта из `src/tests/AdminPanel.IntegrationTests/CreateClusterApiTests.cs`; ключевые кейсы — happy-path ключей + 409 + 400):

```csharp
// AAA: POST декларации пишет канонический набор ключей (arch/02 §9.1):
// config NOT_INITIALIZED, nodes-декларации, request_*, routing блоками §9.1.1.
[SkippableFact]
public async Task PostCluster_WritesCanonicalKeySet()
{
    // Arrange
    Skip.IfNot(Environment.GetEnvironmentVariable("PGW_TEST_DOCKER") is not null, "needs docker");
    await using var etcd = new EtcdFixture(); await etcd.InitializeAsync();
    await using var factory = new PgWorkerApiFactory(etcd);
    var client = factory.CreateClient();

    // Act
    var resp = await client.PostAsJsonAsync("/api/clusters",
        new { name = "smoke", buckets = 4, shards = 2, replicas = 2,
              requestCpu = 0.5, requestMem = 8, requestDisk = 100 });

    // Assert
    resp.StatusCode.Should().Be(HttpStatusCode.Created);
    (await etcd.Gateway.GetAsync(etcd.Endpoint, "/clusters/smoke/config", TestContext.Current.CancellationToken))
        .Value!.Value.Should().Contain("\"state\":\"NOT_INITIALIZED\"");
    var routing = await etcd.Gateway.RangeAsync(etcd.Endpoint,
        "/clusters/smoke/buckets/routing/", TestContext.Current.CancellationToken);
    string.Join(" ", routing.Value.OrderBy(k => k.Key).Select(k => k.Value))
        .Should().Be("shard1 shard1 shard2 shard2"); // блоки 4×2 (§9.1.1)
}
```

Плюс кейсы: повторный POST → 409; **гонка claim-txn (spec §6)** — два параллельных POST одного имени (`await Task.WhenAll(client.PostAsJsonAsync(...), client.PostAsJsonAsync(...))`) → ровно один `201` и один `409`, в etcd один набор ключей; `buckets=0` → 400 c `errors`-массивом; auth: с заданным `PgWorker:Api:ApiKey=test` запрос без заголовка → 401, с заголовком → 201.

Run: `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet test src/tests/PgWorker.IntegrationTests --filter CreateClusterApi`
Expected: FAIL (нет `/api/clusters` → 404; фабрика не компилируется без `public partial class Program`).

- [ ] **Step 2: Каркас — partial Program, middleware, модуль**

1. `Program.cs` PgWorker: в САМЫЙ низ файла добавить `public partial class Program;`.
2. `ApiKeyMiddleware.cs`:

```csharp
// arch/14 §1.1: X-Api-Key против env-секрета PGW_API_KEY (конфиг PgWorker:Api:ApiKey).
// Пусто — проверка отключена (доверенная docker-сеть). /healthz не трогаем.
public sealed class ApiKeyMiddleware(RequestDelegate next)
{
    public async Task InvokeAsync(HttpContext ctx, IOptions<PgWorkerOptions> options)
    {
        var key = options.Value.Api.ApiKey;
        if (!string.IsNullOrEmpty(key)
            && ctx.Request.Path.StartsWithSegments("/api")
            && !string.Equals(ctx.Request.Headers["X-Api-Key"], key))
        {
            ctx.Response.StatusCode = StatusCodes.Status401Unauthorized;
            await ctx.Response.WriteAsJsonAsync(new { title = "Unauthorized", status = 401 });
            return;
        }
        await next(ctx);
    }
}
```

`Program.cs`: после `var app = builder.Build();` → `app.UseMiddleware<ApiKeyMiddleware>();` (до MapHealthChecks/MapWorkerApi).

3. `ApiModule.cs`: `public static IEndpointRouteBuilder MapWorkerApi(this IEndpointRouteBuilder e)` — внутри `MapPost("/api/clusters", ...)` и `MapDelete("/api/clusters/{name}", ...)`; хендлеры — DI-классы; маппинг исключений скопировать из панельного `src/AdminPanel.Api/Operations/OperationsModule.cs` (ветки 400/409/503 для POST; 204/404/503 для DELETE) без изменений текстов.

4. DI в `Program.cs`:

```csharp
builder.Services.AddSingleton(sp => new CreateClusterHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<TimeProvider>()));
builder.Services.AddSingleton(sp => new DeleteClusterHandler(/* то же */));
```

и `app.MapWorkerApi();` рядом с `app.MapHealthChecks("/healthz");`.

- [ ] **Step 3: Хендлеры (порт панельных, адаптация к прямым чтениям)**

`CreateClusterHandler.cs` — тело = панельный `CreateClusterCommandHandler` (`src/AdminPanel.Api/Operations/CreateClusterCommand.cs`) с заменами:
- `ISnapshotStore`/активный-endpoint-из-снапшота → свой `string[] _endpoints`, первый живой (перебор с failover — как `WithFailoverAsync` в воркерских процессах; если все недоступны → `EtcdWriteUnavailableException` = 503);
- `gateway.TxnAsync(endpoint, [new TxnCompare(plan.ConfigKey, 0)], [new KvPut(...)], ct)` → `gateway.TxnAsync(endpoint, TxnRequest.Of([TxnCompare.NotExists(plan.ConfigKey)], [new TxnOp.Put(plan.ConfigKey, plan.ConfigValue, null)]), ct)`;
- `plan.Puts` теперь `IReadOnlyList<PlanPut>` → `gateway.PutAsync(endpoint, put.Key, put.Value, null, ct)`;
- компенсация — дословно (`DeleteAsync(prefix: true)` + точечные `RequestKeys`).

`DeleteClusterHandler.cs` — порт панельного `DeleteClusterCommand` (он уже читал config напрямую у etcd; заменить источник endpoints, как выше).

- [ ] **Step 4: Прогнать — зелёный + регресс**

Run: `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests --filter FullyQualifiedName~Api` ; `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet build src/PgWorker.slnx`
Expected: все API-кейсы PASS, 0 warnings.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(pgworker): HTTP API воркера — каркас (/api, X-Api-Key) + создание/удаление кластера (порт панельных команд)"
```

---

### Task 5: PgWorker API — shards add/remove, moves, rotate, recreate

**Files:**
- Create: `src/PgWorker.App/Api/Operations/AddShardHandler.cs`, `DeleteShardHandler.cs`, `MoveBucketsHandler.cs`, `RotateAppPasswordHandler.cs`, `RecreateNodeHandler.cs`
- Create: `src/PgWorker.App/Api/Operations/WorkerApiExceptions.cs` — дополнить переносом ВСЕХ `public sealed class *Exception` из `src/AdminPanel.Api/Operations/{AddShardCommand,DeleteShardCommand,MoveBucketsCommand}.cs` (сверь фактический состав по grep `public sealed class.*Exception` в этих файлах; на момент плана это: `ShardNotFoundException`, `ShardRemoveBlockedException`, `ShardPrecheckUnavailableException`, `ClusterNotActiveException`, `AddShardValidationException`, `ShardNameTakenException`, `ShardLimitReachedException`, `NonShardedClusterException`, `MoveBucketsValidationException`, `MoveTargetRemovingException`, `BucketNotOnSourceException`, `MoveRequestConflictException`, `MoveClaimLostException`, `ClusterNotFoundException`, `InvalidClusterConfigException`) плюс из ротации/recreate: `RotationAlreadyRequestedException`, `InvalidRecreateModeException`, `ScopeNotFoundException`, `NodeNotFoundException`, `LastNodeException`, `AllOthersRecreatingException` — имена/тексты 1:1, без выдумывания новых имён
- Modify: `src/PgWorker.App/Api/ApiModule.cs` (+5 эндпоинтов; маппинг ошибок — порт панельных веток)
- Test: `src/tests/PgWorker.IntegrationTests/Api/ShardsApiTests.cs`, `MovesApiTests.cs`, `RecreateRotateApiTests.cs` (порты панельных integration-тестов `ShardsApiTests.cs`, `MovesApiTests.cs`, `RecreateNodeApiTests.cs`, `RotateAppPasswordApiTests.cs` на WAF-фабрику Task 4)

**Interfaces:**
- Consumes: Task 3 (Writing), Task 4 (каркас, фабрика), `IEtcdGateway.RangeAsync` (guards: routing, status, moves, `/service/<scope>/members`).
- Produces (для Task 13 — панельные стаб-тесты сверяют только коды ответов):
  - `POST /api/clusters/{c}/shards` → 201 `{name,state,...}` | 400/404/409/503
  - `DELETE /api/clusters/{c}/shards/{x}` → 204 | 404/409/503 (guard'ы: кластер Active, шард есть, routing>0 → 409, незавершённые переезды → 409, последний шард → 409, QUARANTINED → 409 — порт пред-проверок панели, но данные читаются `RangeAsync` напрямую)
  - `POST /api/clusters/{c}/moves` тело `{from,to,buckets[]}` → 201 `{cluster,from,to,queued[],skipped[]}` | 400/404/409/503 (упорядочивание `requested_unix`: range `/pgworker/moves/` → base = max(now, 1+max) — порт панельного `MoveBucketsCommand`; `requested_by` каждой заявки — из заголовка `X-Requested-By` (панель шлёт имя оператора на всех прокси-мутациях, Task 12/13), fallback `"api"` — тот же источник, что у панели сегодня `user.Identity?.Name ?? "adminpanel"` (OperationsModule.cs:141), значение в etcd НЕ меняется)
  - `POST /api/clusters/{c}/app-password/rotate` → 201 `{cluster,requestedUnix,requestedBy}` | 404/409/503 (`requested_by` — заголовок `X-Requested-By`, fallback `"api"` — у панели сегодня ClaimsPrincipal, OperationsModule.cs:173)
  - `POST /api/ha/{scope}/nodes/{node}/recreate` тело `{mode?}` → 201/204 `{scope,node,state,mode}` | 400/404/409/503 (guards по `/service/<scope>/members/<name>` через `RangeAsync("/service/")`; ставит только `state=TO_RECREATE` + `recreate=soft|hard` — requested_by не участвует, заголовок игнорируется)

- [ ] **Step 1: Падающие тесты (порты панельных)**

Перенести кейсы панельных integration-тестов (файлы перечислены выше; сид-декларации те же — `EtcdSeed`-подобная раскладка делается прямыми put в fixture-Gateway): happy-path + главный негатив каждого эндпоинта (достаточно: add 201 + 409-имя; delete-shard 204 + 409-бакеты; moves 201 + 409-заявка-жива; rotate 201 + 409-повтор; recreate 201 + 404-нода). Дополнительно — идентичность оператора: moves и rotate с заголовком `X-Requested-By: opsuser` → в etcd (`/pgworker/moves/<C>/bucket_<i>` и `/pgworker/rotations/<C>`) значение содержит `"requested_by":"opsuser"`; без заголовка → `"requested_by":"api"` (инвариант spec §3.7: значения etcd не меняются при переходе на прокси). Панельные тесты НЕ удалять (удалятся в Task 13).

Run: `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet test src/tests/PgWorker.IntegrationTests --filter "Shards|Moves|Recreate"`
Expected: FAIL (404 на всех новых путях).

- [ ] **Step 2: Хендлеры — порт с заменой источника данных**

Общее правило переноса (для всех пяти): панельный хендлер брал `store.Current` (снапшот) — воркерский читает etcd сам. Маппинг:
- `snapshot.Clusters` → `RangeAsync("/clusters/")` + разбор НУЖНЫХ ключей (по образцу парсеров панели `AdminPanel.Etcd/Parsing/`, но упрощённо в хендлере: config/shards/routing/status извлекаются regex/`Split`); для guards достаточно тех же полей, что читала панель;
- `snapshot.Etcd.ActiveEndpoint` → перебор `_endpoints`;
- сравнения/тексты ошибок — дословно.
Хендлеры — синглтоны с зависимостями `(IEtcdGateway gateway, string[] endpoints, TimeProvider clock)`.

- [ ] **Step 3: Эндпоинты в ApiModule (маппинг ошибок — порт панельного OperationsModule)**

Для каждого — ветка `result.Error switch` из панельного модуля (`src/AdminPanel.Api/Operations/OperationsModule.cs` строки 80–220) без изменения текстов.

- [ ] **Step 4: Зелёный + build + Commit**

Run: `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests` ; `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet build src/PgWorker.slnx`

```bash
git add -A && git commit -m "feat(pgworker): API — add/remove шарда, заявки переездов, ротация, recreate (guards на прямых чтениях etcd)"
```

---

### Task 6: PgWorker — демо-сид `POST /api/seed/demo`

**Files:**
- Create: `src/PgWorker.Core/Seed/PostgresDemoSeedPlan.cs`
- Create: `src/PgWorker.App/Api/Operations/SeedDemoHandler.cs`
- Modify: `src/PgWorker.App/Api/ApiModule.cs` (эндпоинт за флагом)
- Test: `src/tests/PgWorker.IntegrationTests/Api/SeedApiTests.cs`
- Test fixture-источник значений: `dev-stand/adminpanel/seed.sh` (перечитай его в worktree — перенос 1:1)

**Interfaces:**
- Consumes: Task 4 (каркас), `EtcdFixture`.
- Produces: `POST /api/seed/demo` → 200 `{"seeded":true}` | 200 `{"seeded":false}` (живой `/clusters/demo/config` — no-op); при `EnableSeedEndpoint=false` → 404 ProblemDetails. `PostgresDemoSeedPlan`:

```csharp
namespace PgWorker.Core.Seed;
// Демо-сид pg-контура (arch/14 §1.1.1): 1:1 dev-stand/adminpanel/seed.sh.
public sealed record PostgresDemoSeedPlan(long NowUnix)
{
    public IReadOnlyList<PlanPut> Puts { get; } = Build(NowUnix);
    private static IReadOnlyList<PlanPut> Build(long now) =>
    [
        new("/clusters/demo/config", $"{{\"buckets\":16,\"dbname\":\"demo\",\"created_unix\":{now}}}"),
        new("/clusters/demo/shards/s1/dsn", "host=s1a,s1b port=5432 dbname=demo user=postgres"),
        // ... полный набор по seed.sh: s1/s2 dsn+replicas+master, routing 16
        // (s1: 0,2,3,4,6,8,10,11,12,14; s2: 1,5,7,9,13,15),
        // status bucket_3 (SYNCING, now-120/-60), bucket_7 (ABORTING, now-1000/-900),
        // bucket_11 (FROZEN, now-7400/-7200), heals/bucket_5 (now-86400),
        // /pgworker/moves/demo/bucket_13, /service/demo-s{1,2}/* (leader/members/optime/
        // initialize/config), /cluster/nodes/s1a|s1b|s2a|s2b.
    ];
}
```

(используй `PgWorker.Core.Writing.PlanPut`; значения — скопируй из seed.sh в worktree, включая JSON-структуры статусов с `last_error` у bucket_7.)

- [ ] **Step 1: Падающий тест**

Кейс: `POST /api/seed/demo` на пустом etcd → 200 `{"seeded":true}`; проверка ключей (`/clusters/demo/config`, routing bucket_0→s1, `/pgworker/moves/demo/bucket_13` живы, status bucket_3 SYNCING); повторный вызов → `{"seeded":false}` и значения НЕ перезаписаны; `EnableSeedEndpoint=false` (дефолт фабрики Task 4 — оставь true в фабрике, отдельный кейс со своей фабрикой с false) → 404.

Run: `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet test src/tests/PgWorker.IntegrationTests --filter SeedApi`
Expected: FAIL.

- [ ] **Step 2: Реализация**

`SeedDemoHandler(IEtcdGateway gateway, string[] endpoints, TimeProvider clock, bool enabled)`:
- `enabled=false` → сразу псевдо-404 (кинь `WorkerApiNotFoundException("seed-эндпоинт выключен (PgWorker:Api:EnableSeedEndpoint)")` — добавь этот тип в WorkerApiExceptions, модуль маппит в 404);
- idempotency-check `GetAsync("/clusters/demo/config")` → есть: `{"seeded":false}`;
- иначе пакет `PutAsync` по `plan.Puts` (без txn — как скрипт); ответ `{"seeded":true}`.
`ApiModule`: `MapPost("/api/seed/demo", ...)` — обычный маршрут; флаг проверяет хендлер. DI: `new SeedDemoHandler(gw, endpoints, clock, opts.Api.EnableSeedEndpoint)`.

- [ ] **Step 3: Зелёный + build + Commit**

Run: `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests --filter SeedApi` ; build.

```bash
git add -A && git commit -m "feat(pgworker): демо-сид pg-контура через POST /api/seed/demo (перенос seed.sh 1:1, флаг EnableSeedEndpoint)"
```

---

### Task 7: `KafkaWorker.Core.Writing` — перенос KafkaWriting и команд (unit)

**Files:**
- Create: `src/KafkaWorker.Core/Writing/KafkaWriting.cs` — перенос `src/AdminPanel.Etcd/Writing/KafkaWriting.cs` (445 строк: `CreateKafkaClusterRequest`, `KafkaConfigUpdateRequest`, `AddKafkaBrokerRequest`, `KafkaLimits`, валидаторы, планы `KafkaClusterCreatePlan` и др.) в namespace `KafkaWorker.Core.Writing`; `ValidationError` — свой `KafkaWorker.Core/Writing/ValidationError.cs` (record как в Task 3); выходные типы планов → нейтральные `PlanPut(string Key, string Value)` (зеркал Task 3)
- Create: `src/KafkaWorker.Core/Writing/TopicLifecycleRequests.cs` — перенести request-модели lifecycle/топиков из `src/AdminPanel.Api/Operations/Kafka/KafkaCommands.cs` (те части, что относятся к телам запросов: create-topic-тело и т.п.)
- Test (перенос с правкой usings/namespace): `src/tests/AdminPanel.UnitTests/Kafka/KafkaWritingPlanTests.cs` → `src/tests/KafkaWorker.UnitTests/Writing/KafkaWritingPlanTests.cs`; `Kafka/KafkaCommandTests.cs` → `KafkaWorker.UnitTests/Writing/KafkaCommandTests.cs`; `Kafka/TopicDesiredCommandTests.cs`, `Kafka/TopicLifecycleCommandTests.cs`, `Kafka/KafkaCommandHarness.cs` → туда же (harness адаптировать: панели-специфика IHandler уходит — команды станут прямыми вызовами хендлеров Task 8/9; ЕСЛИ harness глубоко завязан на CQRS — перенести только план-тесты, а командные тесты переписать в Task 8/9 как тесты хендлеров напрямую; решение принимай по факту разбора harness'а, критерий: кейсы сохраняются)
- Панельные оригиналы не удалять (Task 14).

**Interfaces:**
- Produces (для Task 8/9): весь публичный API перенесённых файлов с теми же именами; `PlanPut`; `ValidationError`.

- [ ] **Step 1: Перенести план-тесты (падение)** — скопировать `KafkaWritingPlanTests.cs`, поменять namespace/usings, прогнать `--filter KafkaWritingPlan` → FAIL.
- [ ] **Step 2: Перенести KafkaWriting.cs** (механика = Task 3 Step 2: замена `KvPut`→`PlanPut`, клиентские txn-типы не используются планом).
- [ ] **Step 3: Прогнать план-тесты → PASS; перенести/адаптировать командные тесты по разбору harness'а.**
- [ ] **Step 4: build + Commit**

```bash
git add -A && git commit -m "feat(kafkaworker): Writing-ядро — перенос KafkaWriting/валидаторов/планов из панели (1:1)"
```

---

### Task 8: KafkaWorker API — каркас + кластерные мутации (create/delete/config/brokers/rotate/rebalance)

**Files:**
- Create: `src/KafkaWorker.App/Api/ApiModule.cs`, `ApiKeyMiddleware.cs` (зеркал Task 4; ключ `KafkaWorker:Api:ApiKey`, env `KFW_API_KEY` — добавить поле в ApiOptions Task 2)
- Create: `src/KafkaWorker.App/Api/Operations/` — `KafkaExceptions.cs` (перенос исключений из `src/AdminPanel.Api/Operations/Kafka/KafkaCommands.cs`: `KafkaClusterNotFoundException`, `KafkaClusterNotActiveException`, `InvalidKafkaConfigException`, `KafkaValidationException`, `KafkaConcurrentWriteException`, `KafkaClusterAlreadyExistsException` + специфичные 409-исключения брокеров/ротации — тексты 1:1) и хендлеры: `CreateClusterHandler`, `DeleteClusterHandler`, `UpdateConfigHandler`, `AddBrokerHandler`, `DeleteBrokerHandler`, `RotateAppPasswordHandler`, `RebalanceHandler` (POST+DELETE)
- Modify: `src/KafkaWorker.App/Program.cs` (`public partial class Program;`, DI, `UseMiddleware<ApiKeyMiddleware>()`, `MapWorkerApi()`; ВНИМАНИЕ: у KafkaWorker нет env-секретов — фабрика проще)
- Test: `src/tests/KafkaWorker.IntegrationTests/Api/` — `KafkaApiFactory.cs` (зеркал `PgWorkerApiFactory`, секреты не нужны;.RemoveAll<IHostedService>; конфиг `KafkaWorker:Etcd:Endpoints:0`, `KafkaWorker:Api:AdvertiseUrl`) + `ClusterMutationsApiTests.cs`

**Interfaces:**
- Consumes: Task 7 Writing; `KafkaWorker.Etcd.Client` (типы те же, что у PgWorker: `TxnCompare.NotExists`, `TxnRequest.Of`, `TxnOp.Put/Delete`).
- Produces (Task 14 стаб-тесты): пути/коды/тела 1:1 таблице `arch/adminpanel/02` §10.2 (мутации 1–5, 8, 13, 14):
  - `POST /api/kafka/clusters` 201|400|409|503; `DELETE /api/kafka/clusters/{c}` 204|404|503; `PUT /api/kafka/clusters/{c}/config` 204|400|404|409|503 (RMW-txn по mod_revision; проигрыш → 503 `KafkaConcurrentWriteException`); `POST .../brokers` 201|400|404|409|503 (имя broker<max+1> ≤9); `DELETE .../brokers/{b}` 204|404|409|503 (controller/последний — 409); `POST .../app-password/rotate` 201|404|409|503 и `POST .../rebalance` 201|409|503 — `requested_by` заявки из заголовка `X-Requested-By` (панель шлёт оператора на всех прокси-мутациях, Task 12/14), fallback `"api"` (у панели сегодня ClaimsPrincipal — значения etcd не меняются); `DELETE .../rebalance` 204|404|503.
- Тексты/валидации — порт из `src/AdminPanel.Api/Operations/Kafka/KafkaCommands.cs` и `RebalanceCommands.cs` с единственной заменой: источник данных — прямые чтения etcd (`RangeAsync("/kafka/clusters/")`, `GetAsync(config)`) вместо панельного снапшота/store.

- [ ] **Step 1: WAF-фабрика + падающие тесты** — кейсы: create 201 (ключи: config NOT_INITIALIZED, brokers state+resources × B), повтор 409, **гонка claim-txn (spec §6)** — два параллельных POST одного имени (`Task.WhenAll`) → ровно один `201` и один `409`, DELETE → 204 + config `TO_REMOVE` (идемпотентный повтор 204), PUT config → 204 (значение в etcd изменилось), add broker 201 broker4, delete broker 409-controller, rotate 409-при-живой-заявке (заявку pre-put'нуть в fixture), rebalance POST 409-жива/DELETE 204/повтор 404. Прогнать → FAIL.
- [ ] **Step 2: Каркас + хендлеры (порт по правилу Task 5 Step 2)** — DI-синглтоны `(IEtcdGateway, string[], TimeProvider)`.
- [ ] **Step 3: Зелёный + build**

Run: `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 PGW_TEST_DOCKER=1 dotnet test src/tests/KafkaWorker.IntegrationTests` ; build 0 warnings.

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "feat(kafkaworker): HTTP API — каркас + кластерные мутации (create/delete/config/brokers/rotate/rebalance)"
```

---

### Task 9: KafkaWorker API — мутации топиков (desired / lifecycle)

**Files:**
- Create: `src/KafkaWorker.App/Api/Operations/TopicHandlers.cs` — `UpdateTopicDesiredHandler` (PUT topics/{t}), `DeleteDesiredHandler`, `CreateTopicHandler` (POST topics), `DeleteTopicHandler`, `CancelCreateHandler` (DELETE desired.create), `CancelDeleteHandler` (DELETE desired.delete)
- Modify: `src/KafkaWorker.App/Api/ApiModule.cs` (+6 эндпоинтов; маппинг исключений — порт из `KafkaOperationsModule.cs` строки 250–400)
- Test: `src/tests/KafkaWorker.IntegrationTests/Api/TopicMutationsApiTests.cs` (порт негативов чека `dev-stand/adminpanel/checks/50-kafka-api.sh` шагов 4–5 + панельных `TopicLifecycleCommandTests`-кейсов)

**Interfaces:**
- Consumes: Task 7 (`KafkaTopicDesiredPlan.Build` и др. из KafkaWriting), Task 8 (каркас/фабрика).
- Produces: мутации 6,7,9,10,11,12 из `arch/adminpanel/02` §10.2 — пути/коды/тела 1:1 (все guards таблицы §10.2: «конфликт desired/lifecycle», «partitions только увеличение», идемпотентность DELETE topic 204×2 и т.д.); `desired_by`/`requested_by` (мутации 6, 9, 10) — из заголовка `X-Requested-By`, fallback `"api"` (тот же источник, что ClaimsPrincipal у панели сегодня — значения etcd не меняются).

- [ ] **Step 1: Падающие тесты** — кейсы: PUT desired 204 (etcd: `topics/<t>` содержит desired+desired_unix/by); PUT уменьшение partitions → 400; DELETE desired 404-нет-заявки; POST topics create → 201 (ключ `desired.create` канонический JSON); повтор create 409; DELETE topic → 204 + идемпотентен; cancel create 204/404. FAIL.
- [ ] **Step 2: Хендлеры (порт; RMW через `TxnCompare.ModRevisionEqual` — воркерский клиент имеет эту фабрику; проигрыш → 503 KafkaConcurrentWriteException).**
- [ ] **Step 3: Зелёный + build + Commit**

```bash
git add -A && git commit -m "feat(kafkaworker): API — конфиг-заявки и lifecycle топиков (desired/desired.create/desired.delete)"
```

---

### Task 10: KafkaWorker — демо-сид `POST /api/seed/demo`

**Files:**
- Create: `src/KafkaWorker.Core/Seed/KafkaDemoSeedPlan.cs` (перенос `dev-stand/adminpanel/kafka-seed.sh` 1:1: events Active 3 брокера controller + endpoints + app_user/app_password + topics orders/payments/ghost + desired.create audit + desired.delete orders + `/kafkaworker/rotations|rebalances|reassignments/events`; pending NOT_INITIALIZED; фиксированные unix из скрипта — оставить константами, `now` — только ротации как в скрипте)
- Create: `src/KafkaWorker.App/Api/Operations/SeedDemoHandler.cs` (зеркал Task 6; флаг `KafkaWorker:Api:EnableSeedEndpoint`; идемпотентность по `/kafka/clusters/events/config`)
- Modify: `ApiModule.cs`
- Test: `src/tests/KafkaWorker.IntegrationTests/Api/KafkaSeedApiTests.cs`

**Interfaces:**
- Produces: `POST /api/seed/demo` → 200 `{"seeded":true|false}`; 404 при выключенном флаге.

- [ ] **Step 1: Падающий тест** — наливка на пустой etcd → все ключи скрипта на месте (проверь перечень самопроверки из kafka-seed.sh); повтор → `{"seeded":false}`.
- [ ] **Step 2: Реализация (зеркал Task 6).**
- [ ] **Step 3: Зелёный + build + Commit**

```bash
git add -A && git commit -m "feat(kafkaworker): демо-сид kafka-домена через POST /api/seed/demo (перенос kafka-seed.sh 1:1)"
```

---

### Task 11: Панель — WorkerEndpoint в снапшотах (чтение ключей доступа)

**Files:**
- Create: `src/AdminPanel.Core/WorkerEndpoint.cs`
- Modify: `src/AdminPanel.Core/EtcdSnapshot.cs` (+`IReadOnlyList<WorkerEndpoint> PgWorkerEndpoints` после `MoveTickets`)
- Modify: `src/AdminPanel.Core/Kafka/KafkaSnapshot.cs` (+`IReadOnlyList<WorkerEndpoint> WorkerEndpoints` после `Reassignments`)
- Create: `src/AdminPanel.Etcd/Parsing/WorkerEndpointsParser.cs`
- Modify: `src/AdminPanel.Etcd/SnapshotRefresher.cs` (range `/pgworker/api/` → `Prefixes.PgWorkerApi`; вызов парсера; передача в builder)
- Modify: `src/AdminPanel.Etcd/SnapshotBuilder.cs` (+параметр/прокидка)
- Modify: `src/AdminPanel.Etcd/KafkaSnapshotRefresher.cs` (range `/kafkaworker/api/` → сборка `WorkerEndpoints` — там сборка снапшота инлайн, добавь поле)
- Test: `src/tests/AdminPanel.UnitTests/WorkerEndpointsParserTests.cs`; поправить `SnapshotRefresherTests.cs`/`KafkaRefresherTests.cs`/`SnapshotBuilderTests.cs` (новый параметр/поле — пустой список по умолчанию в тест-фикстурах)

**Interfaces:**
- Produces (для Task 12/15):
  - `namespace AdminPanel.Core; public sealed record WorkerEndpoint(string InstanceId, string Url, long SinceUnix);`
  - `public static class WorkerEndpointsParser { public static (IReadOnlyList<WorkerEndpoint> Endpoints, IReadOnlyList<KeyParseError> Errors) Parse(IReadOnlyList<Kv> kvs); }` — ключи `<prefix>api/<id>` (id = leaf после последнего `/`); value JSON `{"url","instance","since_unix"}`; битый JSON → KeyParseError (не бросает); НЕ-JSON-толерантность как у других парсеров.

- [ ] **Step 1: Падающий unit-тест парсера**

```csharp
// AAA: валидные ключи → записи; битый JSON → parseError; без url — parseError.
[Fact]
public void Parse_ValidAndMalformed()
{
    // Arrange
    var kvs = new List<Kv> {
        new("/pgworker/api/abc123", """{"url":"http://h:8080","instance":"abc123","since_unix":1756000000}""", 1),
        new("/pgworker/api/bad", "{not-json", 2),
    };
    // Act
    var (endpoints, errors) = WorkerEndpointsParser.Parse(kvs);
    // Assert
    endpoints.Should().ContainSingle().Which
        .Should().Be(new WorkerEndpoint("abc123", "http://h:8080", 1756000000));
    errors.Should().ContainSingle(e => e.Key == "/pgworker/api/bad");
}
```

Run: `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet test src/tests/AdminPanel.UnitTests --filter WorkerEndpointsParser` → FAIL.

- [ ] **Step 2: Реализация** (record, парсер, refresher'ы: `var pgApiTask = WithFailoverAsync(... Prefixes.PgWorkerApi ...)` рядом с movesTask; kafka — аналогично `Prefixes.WorkerApi = "/kafkaworker/api/"`; снапшот-поля; SnapshotBuilder-прокидка; в существующих тестах фикстуры EtcdSnapshot/KafkaSnapshot дописать `[]`).
- [ ] **Step 3: Зелёный (unit) + build панели без регресс** — `dotnet test src/tests/AdminPanel.UnitTests` (все, не только новые) + build.
- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "feat(panel): WorkerEndpoint — чтение ключей доступа воркеров в pg/kafka снапшоты"
```

---

### Task 12: Панель — WorkerApiGateway (HTTP-клиент к API воркеров)

**Files:**
- Create: `src/AdminPanel.Etcd/Workers/WorkerApiOptions.cs` + DI-регистрация
- Create: `src/AdminPanel.Etcd/Workers/IWorkerApiGateway.cs` (+ `WorkerApiResult`, `WorkerApiUnavailableException`)
- Create: `src/AdminPanel.Etcd/Workers/WorkerApiGateway.cs`
- Modify: `src/AdminPanel.Etcd/ModuleExtensions.cs` (AddHttpClient("workers") + опции + синглтон шлюза)
- Test: `src/tests/AdminPanel.UnitTests/Workers/WorkerApiGatewayTests.cs` (внутрипроцессный HttpListener-стаб)

**Interfaces:**
- Produces (Task 13/14 — главный интерфейс прокси):

```csharp
namespace AdminPanel.Etcd.Workers;

// Ответ API воркера: статус + сырое тело (ProblemDetails проксируется как есть).
public sealed record WorkerApiResult(int StatusCode, string? Body);

// Живых ключей api нет / все URL недоступны → 503-ветка панели.
public sealed class WorkerApiUnavailableException(string worker) 
    : Exception($"API воркера {worker} недоступен: живых ключей доступа нет или все инстансы не отвечают");

public interface IWorkerApiGateway
{
    // worker: "pgworker" | "kafkaworker"; path — "/api/clusters" и т.п.; body — DTO запроса.
    // requestedBy — имя оператора сессии панели: шлюз шлёт его заголовком
    // X-Requested-By на ВСЕХ мутациях (сквозная идентичность оператора,
    // spec §3.7 — значения etcd не меняются при переходе на прокси);
    // null → заголовок не шлётся (воркерский fallback "api").
    Task<WorkerApiResult> SendAsync(string worker, HttpMethod method, string path,
        object? body, string? requestedBy, CancellationToken ct);
}
```

`WorkerApiOptions` (секция `AdminPanel:Workers`): `public string? PgApiKey { get; set; } public string? KafkaApiKey { get; set; } public int TimeoutSec { get; set; } = 10;`

- [ ] **Step 1: Падающий тест на стаб-сервере** (HttpListener на localhost:random; два «инстанса»: первый мёртвый порт, второй живой — failover; кейсы: 201+тело→результат; 409+ProblemDetails→результат с телом; все недоступны→WorkerApiUnavailableException; нет живых ключей→WorkerApiUnavailableException; ПЛЮС идентичность оператора: `SendAsync(..., requestedBy: "opsuser", ...)` → стаб получил заголовок `X-Requested-By: opsuser`, при `requestedBy: null` заголовка нет). Источник endpoints — `ISnapshotStore`/`KafkaSnapshotStore` заменить тест-двойником (они singleton-классы; в тестах панели есть TestHost.cs — построй мини-DI).

Run: `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet test src/tests/AdminPanel.UnitTests --filter WorkerApiGateway` → FAIL.

- [ ] **Step 2: Реализация**

Скелет `WorkerApiGateway(IOptions<WorkerApiOptions> opts, IHttpClientFactory factory)`:
- выбор endpoints: `worker == "pgworker"` → `_pgStore.Current?.PgWorkerEndpoints` (живые, сортировка по InstanceId), `"kafkaworker"` → `_kafkaStore.Current?.WorkerEndpoints`;
- на каждый URL: `client.SendAsync` c `X-Api-Key` (если задан) и `X-Requested-By: <requestedBy>` (если задан), JSON-сериализация body; сетевой сбой/таймаут → следующий URL; успех (любой HTTP-статус получен) → `new WorkerApiResult((int)resp.StatusCode, await resp.Content.ReadAsStringAsync())`;
- исчерпаны → `throw new WorkerApiUnavailableException(worker)`.
Регистрация: `services.Configure<WorkerApiOptions>(configuration.GetSection("AdminPanel:Workers")); services.AddHttpClient("workers", c => c.Timeout = ...);` + синглтоны шлюза (в Take от ISnapshotStore/KafkaSnapshotStore — оба интерфейса уже в DI панели).

- [ ] **Step 3: Зелёный + build + Commit**

```bash
git add -A && git commit -m "feat(panel): WorkerApiGateway — HTTP-вызовы в API воркеров по живым ключам (failover, X-Api-Key, X-Requested-By)"
```

---

### Task 13: Панель — pg-мутации становятся прокси; удаление Writing pg-части

**Files:**
- Modify: `src/AdminPanel.Api/Operations/{CreateClusterCommand,DeleteClusterCommand,AddShardCommand,DeleteShardCommand,MoveBucketsCommand,RotateAppPasswordCommand,RecreateNodeCommand}.cs` — хендлеры переписать на `IWorkerApiGateway` (тела запросов/DTO ответов и исключения «нет API» — остаются; валидаторы/планы из тел убрать)
- Modify: `src/AdminPanel.Api/Operations/OperationsModule.cs` — success-ветки без изменений; error-ветки: `WorkerApiUnavailableException` → 503 «API воркера недоступен», ProblemDetails-тела воркера проксируются (`Results.Text(body, "application/problem+json", statusCode)`)
- Delete: `src/AdminPanel.Etcd/Writing/{ClusterCreatePlan,CreateClusterRequest,ShardScalePlan}.cs`
- Delete: `src/tests/AdminPanel.UnitTests/{CreateClusterPlanTests,ShardScalePlanTests,CreateClusterCommandHandlerTests,DeleteClusterCommandHandlerTests,AddShardCommandHandlerTests,DeleteShardCommandHandlerTests,MoveBucketsCommandHandlerTests}.cs` (контракт переехал в Task 3/5; вместо них — новые прокси-тесты)
- Test: `src/tests/AdminPanel.UnitTests/Operations/WorkerProxyCommandTests.cs` (стаб IWorkerApiGateway)
- Test (integration): переписать `src/tests/AdminPanel.IntegrationTests/{CreateClusterApiTests,DeleteClusterApiTests,ShardsApiTests,MovesApiTests,RotateAppPasswordApiTests,RecreateNodeApiTests}.cs` на стаб-воркер

**Interfaces:**
- Consumes: Task 11 (WorkerEndpoints в снапшоте), Task 12 (`IWorkerApiGateway.SendAsync("pgworker", method, path, body, requestedBy, ct)`).
- Produces: UI-контракт панели НЕ меняется (пути/коды/тела как в `arch/adminpanel/03-panels` §1). Схема прокси-хендлера (единая для всех семи; оператор — из команды, команду модуль строит с `user.Identity?.Name ?? "adminpanel"` — как сегодня, OperationsModule.cs:141/173):

```csharp
// Прокси: панель не пишет в etcd — команда уходит в API PgWorker (arch/14 §1.1);
// requestedBy передаётся заголовком X-Requested-By (шлюз Task 12) — воркер
// пишет его в requested_by заявок (moves/rotate), значения etcd не меняются.
public async ValueTask<Result<ClusterCreatedDto>> Handle(CreateClusterCommand command, CancellationToken ct)
{
    var resp = await api.SendAsync("pgworker", HttpMethod.Post, "/api/clusters",
        command.Request, command.RequestedBy, ct);
    if (resp.StatusCode is >= 200 and < 300)
        return Result<ClusterCreatedDto>.Success(
            JsonSerializer.Deserialize<ClusterCreatedDto>(resp.Body!, JsonOptions)!);
    return Result<ClusterCreatedDto>.Failed(WorkerProblemDetails.From(resp)); // статус+тело воркера
}
```

(Хендлеры, у которых в команде нет оператора — create/delete cluster, shards, recreate — шлют `requestedBy: null`: их ключи `requested_by` не содержат. MoveBuckets/RotateAppPassword — всегда с оператором.)

`WorkerProblemDetails : Exception` (новый тип в `AdminPanel.Api/Operations/WorkerProblemDetails.cs`): свойства `int StatusCode`, `string Body`; статическая фабрика `public static WorkerProblemDetails From(WorkerApiResult resp) => new(resp.StatusCode, resp.Body ?? "");` — модуль отдаёт `Results.Text(ex.Body, "application/problem+json", statusCode: ex.StatusCode)` (400-ветка с `errors`-массивом приходит от воркера уже в каноническом виде — тест проверит `GetArrayLength("errors", ...)` как старый панельный).

- [ ] **Step 1: Падающие unit-тесты прокси** — стаб `IWorkerApiGateway`: 201+JSON → Success с DTO; 409+ProblemDetails → Failed со статусом 409; throw WorkerApiUnavailableException → Failed → модуль 503; идентичность оператора: стаб-шлюз для moves/rotate ловит `requestedBy == "opsuser"` (панель построила команду с ним), для create — `null`. Прогнать → FAIL (хендлеры ещё пишут etcd).
- [ ] **Step 2: Переписать 7 хендлеров по схеме (оператор — у moves/rotate); модуль — 503-ветка + прокси тел.**
- [ ] **Step 3: Переписать integration-тесты на стаб-воркер**: в тестовом хосте панели (`Program` WAF, паттерн существующих IntegrationTests) заменить DI `IWorkerApiGateway` на стаб, который матчится по path/method и возвращает заготовленные ответы (значения — из прежних тестов: DTO успешных ответов и ProblemDetails-тела ошибок). Проверяемые инварианты: коды/тела ответов панели 1:1 прежним; `IEtcdGateway` из панели НЕ вызывается на мутациях (стаб etcd-клиента — счётчик вызовов = 0 для записи).
- [ ] **Step 4: Удалить Writing pg-файлы и устаревшие тесты; прогнать ВСЁ: unit+integration панели, build решения**

Run: `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet test src/tests/AdminPanel.UnitTests && DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet test src/tests/AdminPanel.IntegrationTests && DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet build src/PgWorker.slnx`
Expected: зелёные, 0 warnings.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(panel)!: pg-мутации — прокси в API PgWorker; панель перестаёт писать в etcd (Writing pg удалён)"
```

---

### Task 14: Панель — kafka-мутации становятся прокси; удаление KafkaWriting

**Files:**
- Modify: `src/AdminPanel.Api/Operations/Kafka/KafkaCommands.cs`, `RebalanceCommands.cs` — все 14 хендлеров по схеме Task 13 (`api.SendAsync("kafkaworker", ..., requestedBy, ...)`, пути — `/api/kafka/...` 1:1 таблице §10.2; `requestedBy` передают мутации с аудитом: 6 desired, 8 rotate, 9 create, 10 delete, 13 rebalance — как сегодня их `requested_by`/`desired_by` из ClaimsPrincipal)
- Modify: `src/AdminPanel.Api/Operations/Kafka/KafkaOperationsModule.cs` — 503-ветка + прокси ProblemDetails
- Delete: `src/AdminPanel.Etcd/Writing/KafkaWriting.cs` (+ каталог Writing становится пуст → удалить)
- Delete: `src/tests/AdminPanel.UnitTests/Kafka/{KafkaCommandTests,TopicDesiredCommandTests,TopicLifecycleCommandTests,KafkaWritingPlanTests,KafkaCommandHarness}.cs` (перенесены/заменены в Task 7–9)
- Test: `src/tests/AdminPanel.UnitTests/Operations/KafkaWorkerProxyTests.cs` (стаб-шлюз; smoke на репрезентативных мутациях: create 201/409, topic lifecycle 204/404, rebalance DELETE 404 — остальное покрыто воркерскими тестами Task 8/9; плюс идентичность оператора: стаб ловит `requestedBy == "opsuser"` у rotate/rebalance/create-topic/desired, `null` — у create кластера)

**Interfaces:**
- Consumes: Task 12.
- Produces: UI-контракт §10.2 без изменений.

- [ ] **Step 1: Падающие прокси-тесты (smoke-набор).**
- [ ] **Step 2: Перепись хендлеров/модуля.**
- [ ] **Step 3: Удаления; полные прогоны панели + build.**
- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "feat(panel)!: kafka-мутации — прокси в API KafkaWorker; AdminPanel.Etcd/Writing удалён (etcd читает только)"
```

---

### Task 15: Алерты — Hint/Remedy у каждого kind + `worker-api-unreachable` + DTO/UI

**Files:**
- Modify: `src/AdminPanel.Core/Alert.cs` (+3 обязательных параметра; enum `AlertRemedy { WorkerAuto, OperatorApi, OperatorRunbook }`)
- Modify: ВСЕ правила `src/AdminPanel.Core/Alerting/Rules/*.cs` (25 шт.) и `src/AdminPanel.Core/Kafka/KafkaAlerting/KafkaAlertEngine.cs` — каждое `new Alert(...)` дополняется Hint/Remedy/RemedyText
- Create: `src/AdminPanel.Core/Alerting/Rules/WorkerApiUnreachableRule.cs` (pg-грань: `EtcdSnapshot.PgWorkerEndpoints` пуст → kind `worker-api-unreachable`, target `pgworker`, severity Critical)
- Modify: `src/AdminPanel.Core/Kafka/KafkaAlerting/KafkaAlertEngine.cs` — та же проверка по `KafkaSnapshot.WorkerEndpoints` (target `kafkaworker`)
- Modify: `src/AdminPanel.Api/Inspection/AlertsQuery.cs` — `AlertDto` + `Hint`, `Remedy` (строка `worker-auto`|`operator-api`|`operator-runbook`), `RemedyText`
- Test: `src/tests/AdminPanel.UnitTests/AlertHintRemedyTests.cs` (все kinds имеют непустые поля)
- Frontend: `frontend/src/api/dto.ts` (тип Alert +3 поля), `frontend/src/pages/AlertsPage.tsx` (раскрытие Hint + бейдж движителя)

**Interfaces:**
- Consumes: Task 11 (WorkerEndpoints).
- Produces: `Alert(Id, Severity, Kind, Target, Message, Details, SinceUnix, string Hint, AlertRemedy Remedy, string RemedyText)` — конструктор ОБНОВИТЬ и все вызовы; `/api/alerts` отдаёт `hint/remedy/remedyText`.

- [ ] **Step 1: Падающий тест-инвариант**

```csharp
// AAA: каждый kind каталога (03-panels §4) даёт непустые Hint/Remedy/RemedyText.
// Перечень kinds собираем прогоном движков по эталонным снапшотам (TestSnapshots.cs)
// + оба worker-api-unreachable.
[Fact]
public void EveryAlert_Kind_HasHintAndRemedy()
{
    // Arrange — эталонные снапшоты из TestSnapshots (аномальные данные) + пустые WorkerEndpoints
    // Act
    var alerts = new AlertEngine(rules).Evaluate(snapshot, prev, now, 3);
    // Assert
    alerts.Should().NotBeEmpty();
    alerts.Should().OnlyContain(a => !string.IsNullOrWhiteSpace(a.Hint)
        && !string.IsNullOrWhiteSpace(a.RemedyText));
}
```

(Если TestSnapshots не покрывает какой-то kind — добавь мини-снапшот с нужной аномалией в сам тест; kafka — аналогично KafkaAlertEngine.) Run → FAIL.

- [ ] **Step 2: Расширить модель + пройтись по всем правилам**

Формат Hint: «что не так; как должно быть; для чего ключ/инвариант». Референсные тексты (остальные — по аналогии, канон `arch/adminpanel/03-panels.md` §4.1):
- `bucket-lost`: Hint «routing указывает на шард, отсутствующий в декларации; routing — единственный авторитет "где бакет", висячие ссылки ломают переезды и SQL-сверку; каждый routing должен указывать на шард с ключом shards/<X>/replicas»; Remedy `OperatorApi`, RemedyText «POST /api/clusters/{c}/moves — перевезти бакеты на живой шард или восстановить декларацию шарда».
- `cluster-incomplete`: Remedy `WorkerAuto` «воркер ждёт доустойчивости ключей (journal /pgworker/work); при вечном висе — дефект воркера».
- `move-stale`/`move-frozen-long`/`move-flipped-status-stuck`/`shard-no-master`: Remedy `WorkerAuto`, RemedyText «репаратор переездов/сверка мастера PgWorker (feat-pgworker-adopt-repair) закроет; висит — дефект воркера».
- `etcd-*`, `snapshot-stale`: Remedy `OperatorRunbook` (текст — проверить кворум/endpoints по arch/09).
- probe-алерты: Remedy `WorkerAuto`/`OperatorRunbook` по смыслу (ha-member-not-streaming → WorkerAuto — надзор rebuild).
- lifecycle-заметки (cluster-not-initialized, kafka-cluster-*, *-pending): Remedy `WorkerAuto`.
- `worker-api-unreachable` (новый): Message «API PgWorker(KafkaWorker) недоступен: живых ключей /pgworker/api/ (/kafkaworker/api/) нет — мутации из панели 503; чтение данных не страдает»; Hint «воркер ставит lease-ключ при старте; ключа нет = воркер не поднялся или умер ≤15 c назад»; Remedy `OperatorRunbook`, RemedyText «запустите контейнер воркера (deploy/docker-compose.yml / профиль kafka), проверьте /healthz и PgWorker:Api:AdvertiseUrl».

- [ ] **Step 3: KafkaAlertEngine — worker-api-unreachable (kafka) + тексты всех kafka-kind'ов.**
- [ ] **Step 4: AlertDto (+remedy-строка: `Remedy.ToString().ToLowerInvariant()` с camel-дефисом — маппинг `worker-auto` и т.д.), фронт: поля в dto.ts; в AlertsPage.tsx под Message — блок `<div className="alert-hint">{a.hint}</div>` + бейдж RemedyText рядом с severity (стили по образцу AlertSeverityBadge.tsx; проверить `npm run build`).**
- [ ] **Step 5: Полный прогон панели + фронт-сборка + build решения; Commit**

```bash
DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet test src/tests/AdminPanel.UnitTests && cd frontend && npm run build && cd .. && DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet build src/PgWorker.slnx
git add -A && git commit -m "feat(panel): алерты с объяснением и движителем (Hint/Remedy) + worker-api-unreachable (pg+kafka)"
```

---

### Task 16: Стенд — deploy/dev-stand/compose/env, 05-seed.sh, чеки, README

**Files:**
- Modify: `deploy/docker-compose.yml` — pgworker env (двойное подчёркивание в env-ключах compose, НЕ двоеточие): `PgWorker__Api__AdvertiseUrl: ${PGW_API_ADVERTISE_URL:-http://host.docker.internal:8080}`, `PgWorker__Api__ApiKey: ${PGW_API_KEY:-}`, `PgWorker__Api__EnableSeedEndpoint: ${PGW_API_ENABLE_SEED:-false}`; kafkaworker env: `KafkaWorker__Api__AdvertiseUrl: ${KFW_API_ADVERTISE_URL:-http://host.docker.internal:8081}`, `KafkaWorker__Api__ApiKey: ${KFW_API_KEY:-}`, `KafkaWorker__Api__EnableSeedEndpoint: ${KFW_API_ENABLE_SEED:-false}`
- Modify: `deploy/.env.example` — добавить `PGW_API_ADVERTISE_URL`, `PGW_API_KEY`, `PGW_API_ENABLE_SEED=true` (стенд), `KFW_API_ADVERTISE_URL`, `KFW_API_KEY`, `KFW_API_ENABLE_SEED=true` с комментариями (прод-комментарий: ключ обязателен, seed выключить)
- Modify: `dev-stand/adminpanel/docker-compose.yml`: kafkaworker — `ports: ["8082:8080"]` (хост-доступ для чеков), env `KafkaWorker__Api__AdvertiseUrl: http://kafkaworker:8080` (панель — по compose-DNS), `KafkaWorker__Api__EnableSeedEndpoint: "true"`; УДАЛИТЬ сервисы `seed` и `kafka-seed` + volume-маунты их скриптов
- Delete: `dev-stand/adminpanel/seed.sh`, `dev-stand/adminpanel/kafka-seed.sh`, каталог `dev-stand/adminpanel/seed/` (образ etcdctl)
- Create: `dev-stand/adminpanel/checks/05-seed.sh`
- Modify: `dev-stand/adminpanel/checks/00-up.sh` (переупорядочивание: PgWorker до сида; шаг сида → `05-seed.sh pg`)
- Modify: `dev-stand/adminpanel/checks/50-kafka-api.sh` (наливка сида → `05-seed.sh kafka`)
- Modify: `dev-stand/adminpanel/checks/20-alerts.sh` (+финальный шаг full-ветки: живость `/pgworker/api/`, 503 мутации при остановленном pgworker, алерт `worker-api-unreachable`, возврат pgworker)
- Modify: `dev-stand/adminpanel/README.md` (quick/full/seed/kafka — новая механика сида; e2e-порядок с 05-seed)
- Modify: `dev-stand/seed.sh` → тонкая curl-обёртка `POST /api/clusters` (параметры прежние: shop, N=6, S=2, R=2, request_* — добери из текущего файла; креды bucket_admin воркер берёт из env-fallback — аргументы 3/4 упраздняются с комментарием)

**Interfaces:**
- Consumes: Task 4–6, 8–10 (API + seed-эндпоинты), Task 13–14 (панель-прокси), Task 15 (алерт).
- Produces: рабочий полный стенд; `05-seed.sh [pg|kafka|all]` (default `all`).

- [ ] **Step 1: 05-seed.sh**

```sh
#!/usr/bin/env bash
# Идемпотентная наливка демо-сидов ЧЕРЕЗ API воркеров (spec §3.5; прямая
# запись etcdctl'ом упразднена). Режимы: pg | kafka | all (default all).
# Скрипт НЕ управляет жизнью воркера ПОСЛЕ наливки (решение пользователя по
# ревью Фазы 4): потребитель сида решает сам — чек 50 после наливки гоняет
# мутации через живой API и останавливает kafkaworker финальным шагом
# (end-state полного прогона «после сида воркер остановлен»).
set -euo pipefail
cd "$(dirname "$0")/.."
MODE="${1:-all}"

seed_pg() {
  ROOT="$(cd ../.. && pwd)"
  [ -f "$ROOT/deploy/.env" ] || cp "$ROOT/deploy/.env.example" "$ROOT/deploy/.env"
  ( cd "$ROOT/deploy" && docker compose up -d pgworker >/dev/null 2>&1 )
  for i in $(seq 1 60); do curl -fsS -m 3 http://localhost:8080/healthz >/dev/null 2>&1 && break; sleep 1; done
  curl -fsS -m 3 http://localhost:8080/healthz >/dev/null || { echo "❌ pgworker не ожил (:8080/healthz)"; exit 1; }
  echo "  pg-сид: $(curl -fsS -X POST http://localhost:8080/api/seed/demo)"
  # живость ключа доступа (arch/14 §1.1)
  docker compose exec -T etcd etcdctl get /pgworker/api/ --prefix --keys-only | grep -q . \
    || { echo "❌ /pgworker/api/ пуст"; exit 1; }
}
seed_kafka() {
  docker compose --profile kafka up -d kafkaworker >/dev/null 2>&1
  for i in $(seq 1 60); do curl -fsS -m 3 http://localhost:8082/healthz >/dev/null 2>&1 && break; sleep 1; done
  curl -fsS -m 3 http://localhost:8082/healthz >/dev/null || { echo "❌ kafkaworker не ожил (:8082/healthz)"; exit 1; }
  echo "  kafka-сид: $(curl -fsS -X POST http://localhost:8082/api/seed/demo)"
  # живой ключ доступа — его ждут и панель (WorkerEndpoints), и последующие
  # мутации чека 50; воркер остаётся Поднятым (безопасно: контейнеров брокеров
  # нет → пробы слепые → сидовые заявки не исполняются, arch/16 §5 C).
  for i in $(seq 1 30); do
    docker compose exec -T etcd etcdctl get /kafkaworker/api/ --prefix --keys-only 2>/dev/null | grep -q . && break
    sleep 1
  done
  docker compose exec -T etcd etcdctl get /kafkaworker/api/ --prefix --keys-only 2>/dev/null | grep -q . \
    || { echo "❌ /kafkaworker/api/ пуст за 30 c (AdvertiseUrl/keepalive?)"; exit 1; }
}
[ "$MODE" = pg ] || [ "$MODE" = kafka ] || [ "$MODE" = all ] || { echo "usage: 05-seed.sh [pg|kafka|all]"; exit 1; }
[ "$MODE" = kafka ] || seed_pg
[ "$MODE" = pg ] || seed_kafka
echo "✓ 05-seed ($MODE): сиды налиты через API воркеров (воркеры подняты — жизнью управляет потребитель)"
```

`chmod +x`. (Проверь: `docker compose exec etcd` в seed_pg/seed_kafka выполняется из каталога dev-stand/adminpanel — да, cd в начале.)

- [ ] **Step 2: 00-up.sh** — шаг 9 (PgWorker) переносится ВВЕРХ сразу после etcd-health (шаг 1); на его прежнем месте — `checks/05-seed.sh pg` (замена блока ожидания сида: было «сид не появился за 30 c» через сервис seed — стало вызовом 05-seed.sh pg + проверка `/clusters/demo/config` как раньше). Комментарий шага обновить (pg-сид — через API воркера). Шаг 7 (kafkaworker heartbeat) — код не меняется, но комментарий обновить: он содержит устаревшее «Сид (чек 50) с живым воркером несовместим — прогон 50-го сам останавливает воркера перед наливкой сида» — заменить на актуальную семантику: «50-й наливает kafka-сид ЧЕРЕЗ API живого воркера (05-seed.sh kafka) и останавливает его финальным шагом (spec §3.5)». Остальные шаги не трогать.

- [ ] **Step 3: 50-kafka-api.sh** — блок наливки сида (строки 17–20: `docker compose --profile kafka stop kafkaworker` + `docker compose --profile seed run --rm kafka-seed`) заменить на новую последовательность (spec §3.5; чек сам управляет жизнью воркера):

```sh
# Arrange: наливка kafka-сида ЧЕРЕЗ API воркера (05-seed поднимает kafkaworker
# и ждёт живой ключ /kafkaworker/api/). Сид без контейнеров брокеров: пробы
# воркера слепые (arch/16 §5 C) → сидовые заявки не исполняются, ожидания
# шагов мутаций ниже — прежние. Воркер остаётся поднятым до финала чека.
"$PWD/05-seed.sh" kafka

# Готовность прокси: панель должна увидеть живой WorkerEndpoint в kafka-снапшоте
# (тик 3 c) — без него kafka-мутации панели вернут 503.
for i in $(seq 1 15); do
  docker compose exec -T etcd etcdctl get /kafkaworker/api/ --prefix --keys-only 2>/dev/null | grep -q . && break
  sleep 1
done
```

(существующий `wait_clusters` шага ниже остаётся — он ждёт кластеры в kafka-снапшоте панели; все шаги мутаций 1–13 НЕ меняются — теперь они физически идут панель→прокси→API живого воркера). В КОНЕЦ чека (после шага 13 «алерты», перед финальным `echo "✓"`) добавить финальный шаг:

```sh
# Финал: kafkaworker больше не нужен (end-state «после сида воркер остановлен»,
# spec §3.5) — мутации прошли через живой API, останавливаем. Сразу проверяем
# kafka-грань worker-api-unreachable (spec §9.5): lease-ключ гаснет ≤15 c,
# тик kafka-снапшота ≤3 c → алерт target=kafkaworker появляется (jq, поллинг).
docker compose --profile kafka stop kafkaworker >/dev/null 2>&1
for i in $(seq 1 20); do
  api /api/alerts 2>/dev/null | jq -e 'any(.[]; .kind=="worker-api-unreachable" and .target=="kafkaworker")' >/dev/null 2>&1 && break
  sleep 2
done
api /api/alerts | jq -e 'any(.[]; .kind=="worker-api-unreachable" and .target=="kafkaworker")' >/dev/null \
  || { echo "❌ worker-api-unreachable (kafkaworker) не появился после stop"; exit 1; }
echo "  kafkaworker остановлен (мутации — через живой API; kafka-грань алерта видна)"
```

- [ ] **Step 4: 20-alerts.sh** — добавить в конец full-ветки (после существующих шагов; quick-ветка — без изменений):

```bash
# Доступность API воркера: ключ жив → мутации идут; остановлен → 503 + алерт.
ect get /pgworker/api/ --prefix --keys-only | grep -q . || { echo "❌ нет /pgworker/api/*"; exit 1; }
( cd ../../deploy && docker compose stop pgworker >/dev/null 2>&1 )
code="$(curl -s -o /dev/null -w '%{http_code}' -b "$JAR" -X POST "$BASE/api/clusters" \
  -H 'Content-Type: application/json' -d '{"name":"probeapi"}')"
[ "$code" = 503 ] || { echo "❌ мутация при мёртвом воркере = $code (ожидался 503)"; exit 1; }
# алерт: тик ≤3 c ×2 + lease ≤15 c
for i in $(seq 1 20); do
  curl -fsS -b "$JAR" "$BASE/api/alerts" | jq -e 'any(.[]; .kind=="worker-api-unreachable" and .target=="pgworker")' >/dev/null 2>&1 && break
  sleep 2
done
curl -fsS -b "$JAR" "$BASE/api/alerts" | jq -e 'any(.[]; .kind=="worker-api-unreachable" and .target=="pgworker")' >/dev/null \
  || { echo "❌ worker-api-unreachable не появился"; exit 1; }
( cd ../../deploy && docker compose start pgworker >/dev/null 2>&1 )
for i in $(seq 1 30); do curl -fsS -m 3 http://localhost:8080/healthz >/dev/null 2>&1 && break; sleep 1; done
# Гашение: ключ /pgworker/api/ восстановился (keepalive ≤15 c) + 2 тика панели → алерт исчез.
for i in $(seq 1 20); do
  curl -fsS -b "$JAR" "$BASE/api/alerts" | jq -e 'any(.[]; .kind=="worker-api-unreachable" and .target=="pgworker") | not' >/dev/null 2>&1 && break
  sleep 2
done
curl -fsS -b "$JAR" "$BASE/api/alerts" | jq -e 'any(.[]; .kind=="worker-api-unreachable" and .target=="pgworker") | not' >/dev/null \
  || { echo "❌ worker-api-unreachable не погас после возврата pgworker"; exit 1; }
echo "  worker-api-unreachable: 503 мутаций + алерт + jq-гашение после восстановления — ok"
```

- [ ] **Step 5: compose/env правки** (перечень в Files; в dev-stand kafkaworker добавить ports 8082:8080 — не конфликтует: 8082 свободен).
- [ ] **Step 6: README (dev-stand/adminpanel) + dev-stand/seed.sh-обёртка; удалить старые сид-файлы/каталог.**
- [ ] **Step 7: Прогон полного стенда**

```bash
cd dev-stand/adminpanel && checks/90-down.sh -v
docker ps -a --format '{{.Names}}' | grep -E '^(kfw|pgw)-' | xargs -r docker rm -f   # почистить ошмётки воркеров
checks/00-up.sh && checks/10-smoke-api.sh && checks/20-alerts.sh
```
Expected: все зелёные; `etcdctl get /clusters/demo/` — прежний набор; `/pgworker/api/` ключ жив.

- [ ] **Step 8: Commit**

```bash
git add -A && git commit -m "feat(stand): сиды через API воркеров — 05-seed.sh, переупорядоченный 00-up.sh, чеки 50/20, env AdvertiseUrl/ApiKey; прямые etcdctl-сид-скрипты удалены"
```

---

### Task 17: Полная верификация и e2e-приёмка

**Files:** без новых правок по коду (только фиксы, найденные прогонами).

- [ ] **Step 1: Полный build + все тесты**

```bash
DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet build src/PgWorker.slnx   # 0 warnings
DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.UnitTests
DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests
DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 PGW_TEST_DOCKER=1 dotnet test src/tests/KafkaWorker.UnitTests
DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 PGW_TEST_DOCKER=1 dotnet test src/tests/KafkaWorker.IntegrationTests
DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 PGW_TEST_DOCKER=1 dotnet test src/tests/AdminPanel.UnitTests
DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 PGW_TEST_DOCKER=1 dotnet test src/tests/AdminPanel.IntegrationTests
cd frontend && npm run build && cd ..
```
Expected: всё зелёное; количества ≥ базовых с учётом переносов (см. Global Constraints).

- [ ] **Step 2: Полный e2e-стенд с чистого состояния**

```bash
cd dev-stand/adminpanel
checks/90-down.sh -v
docker ps -a --format '{{.Names}}' | grep -E '^(kfw|pgw)-' | xargs -r docker rm -f
checks/00-up.sh && checks/10-smoke-api.sh && checks/15-cluster-create.sh && checks/20-alerts.sh \
  && checks/30-failover.sh && checks/40-live-probes.sh && checks/50-kafka-api.sh
```
Expected: чеки 15/50 зелёные — ожидания самих мутаций НЕ меняются (etcd-значения идентичны прежним); 20-й — с новым worker-api-шагом; 50-й: наливка через `05-seed.sh kafka` → мутации через панель→прокси→API живого воркера → финальный `stop kafkaworker` (end-state «после сида воркер остановлен», spec §3.5/§9.3).

- [ ] **Step 3: 55-kafka-e2e.sh — главный e2e панель→прокси→API при живом воркере (spec §9.7)**

Чек сам делает down/up стенда, чистит kfw-объекты и собирает образы — запускать ПОСЛЕ основного прогона (Step 2), он разбирает стенд:

```bash
cd dev-stand/adminpanel
checks/55-kafka-e2e.sh
```
Expected: зелёный (15 подшагов — чек нумерует «(1/15)…15)»: создание кластера через API → автосинк → desired → негативы → missing-ветка → lifecycle из панели → отмены → демонтаж broker-only → ребалансировка → TO_REMOVE). Все панельные мутации чека физически идут через `IWorkerApiGateway` → живой `/kafkaworker/api/<id>` — это сквозная приёмка прокси; сами ожидания чека не меняются.

- [ ] **Step 4: Критерии приёмки spec §9 (прогон вручную по чек-листу)**

1. `grep -rn "PutAsync\|TxnAsync\|DeleteAsync" src/AdminPanel.Etcd src/AdminPanel.Api src/AdminPanel.Core` — только transport-клиент (`EtcdGateway.cs`) и read-пути; Writing-каталога нет.
2. Ключи api живут/гаснут (проверено шагом 20-го чека).
3. Мутации сквозь API (чек 15/50) + 503 при мёртвом воркере (чек 20).
4. Сид через API (00-up/05-seed) + сид-скриптов etcdctl нет (`ls dev-stand/adminpanel/seed*` — только каталог удалён, файлов нет; `git grep -l "etcdctl put" dev-stand/` — пусто для декларативных сидов).
5. Алерты: `/api/alerts` — все kinds с hint/remedy/remedyText (jq по чеку 20 и руками).
6. `/healthz` воркеров жив; процессы не тронуты.
7. 0 warnings, тесты зелёные (шаг 1).
8. Docker-only: хост-запусков не появилось (`git diff --stat` — без новых хост-скриптов запуска воркеров).

- [ ] **Step 5: Финальный коммит (фиксы верификации) + сводка для ревью**

```bash
git add -A && git commit -m "chore: приёмочные прогоны etcd-via-worker-api — все чеки зелёные"
```

---

## Self-Review (выполнен автором плана)

- **Покрытие spec:** §3.1 → Task 1/2/11; §3.2 → Task 3/4/5; §3.3 → Task 7/8/9; §3.4 → Task 11/12/13/14; §3.5 → Task 6/10/16 (+dev-stand/seed.sh в 16); §3.6 → Task 15; §3.7 — инвариант Tasks 4–9/13–14 (фикстуры 1:1); §5 фазы → порядок задач; §6 → тест-шаги задач; §9 критерии → Task 17. Граница с feat-pgworker-adopt-repair: Remedy-тексты без реализации репарации (Task 15).
- **Консистентность имён:** `PlanPut(Key, Value)`, `WorkerEndpoint(InstanceId, Url, SinceUnix)`, `IWorkerApiGateway.SendAsync(worker, method, path, body, requestedBy, ct)` (requestedBy → заголовок `X-Requested-By`, воркерские moves/rotate/kafka-заявки пишут его в `requested_by`/`desired_by`, fallback `"api"`), `WorkerApiResult(StatusCode, Body)`, `WorkerProblemDetails.From(resp)`, `ApiOptions{AdvertiseUrl, ApiKey, EnableSeedEndpoint}` — едины между задачами; env-имена: `PGW_API_ADVERTISE_URL`, `PGW_API_KEY`, `PGW_API_ENABLE_SEED`, `KFW_API_ADVERTISE_URL`, `KFW_API_KEY`, `KFW_API_ENABLE_SEED`; в compose env-ключи — ТОЛЬКО двойное подчёркивание (`PgWorker__Api__AdvertiseUrl`), не двоеточие.
- **Известная ловушка:** у панели `Results.Text(body, contentType, statusCode)` требует `Microsoft.AspNetCore.Http.Results` — уже используется в проектах; ProblemDetails-прокси сохраняет `application/problem+json`.
