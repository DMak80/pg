# t09-kafka-worker-health — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `/healthz` KafkaWorker перестаёт лгать: успешный тик цикла гасит ошибку прошлого (сброс sticky-`StatusError`), активные пробы и чек всегда отдают структуру вместо исключения, etcd-клиент не флейпит DNS (PooledConnectionLifetime + IPv4-first ConnectCallback); панель видит реальное здоровье воркера опросом `/healthz` и алертит `worker-unhealthy` ≤ 2 тиков поллера; e2e-чек 57 доказывает всё на стенде transient-рестартом etcd.

**Architecture:** Три точечных правки циклов воркера (порт «живой-Ф7» из PgWorker-циклов) + catch-all в `ServiceProbes`/`KafkaWorkerHealth` + новый `EtcdConnectCallback` с конфигурацией именованного HttpClient `etcd`. Панель: новый стор `IKafkaWorkerHealthStore` + поле `KafkaSnapshot.WorkerHealth`, расширение существующего `WorkerHealthPoller` на kafka-эндпоинты (один тик, один HttpClient, `/healthz` без X-Api-Key), правило `worker-unhealthy` в `KafkaAlertEngine`; `FailTick` kafka-refresher'а переносит `WorkerHealth` из previous (симметрия pg — свежий стор мерджится только успешным тиком). Стенд: новый чек `57-kafka-worker-health.sh`.

**Tech Stack:** .NET 10, C# (`LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true`), xUnit (AAA-комментарии), bash+jq (стенд-чеки).

**Spec:** `docs/superpowers/2026-09-02-t09-kafka-worker-health/spec.md` (план аргументируется от spec; исполнители читают оба). Канон: `arch/16-kafkaworker.md` §7 (обновлён t09), `arch/adminpanel/02-etcd-contract.md` §2.3.2, `arch/adminpanel/03-panels.md` §4.

**Ревизии:** v3 — правки по повторному ревью Фазы 4: (зам. 1) тест `WorkerUnhealthy_SinceUnix_CarriedFromPrevious` — Alerts вкладываются в **prev** (`ResolveSince` ищет в `previous.Alerts`; прецедент `ExistingAlert_SinceUnixCarried`); (зам. 2) Step 8.4 — явный список чеков 00→57 (glob `[0-9]*.sh` захватывал `90-down.sh` и разбирал стенд); (зам. 3) `EtcdConnectCallbackTests` — явный кейс «IP-литерал — без DNS» (прямой `ConnectToAddressesAsync`, spec §6). v2 — правки по ревью Фазы 4: (зам. 1) `WorkerHealth` в FailTick НЕ мерджится из стора — переносится из previous (симметрия pg `SnapshotRefresher.cs:225`, spec §3.4), тест заменён на «FailTick сохраняет previous», в чеке 57 `wait_alert` перенесён ПОСЛЕ подъёма etcd (алерт загорается первым успешным kafka-тиком — семантика spec §3.5); (зам. 2) чек 57: разделённые предикаты `has_unhealthy` (target `kafkaworker/<id>`) и `has_api_down` (точный target `kafkaworker` — у `worker-api-unreachable` target без слэша); (зам. 3) Task 8: полный прогон серии чеков 00→57; (зам. 4) +тесты «SnapshotLoop не-лидер не трогает» и «бросающая docker-фабрика → per-host Failed»; (мелочь) хелпер `Snapshot` — перегрузка вместо параметра после `params`.

## Global Constraints

- Работа в worktree `/Users/demakaev/ZCodeProject/worktrees/feat-t09-kafka-worker-health` (ветка `feat-t09-kafka-worker-health`); коммит после каждой задачи; **архивные правки spec/arch запрещены** (архив `docs/superpowers/` — только чтение; исключение — сам plan.md при ревью-правках).
- Стиль коммитов — как в `git log`: `feat(kafka): …` / `feat(adminpanel): …` / `test(stand): …`, по-русски, кратко.
- `TreatWarningsAsErrors=true` — новый код без ворнингов; тесты — AAA-комментарии (`// Arrange / // Act / // Assert`) — обязательное правило репозитория.
- Документация/комментарии — русские; идентификаторы — английские.
- Сборка/тесты из корня worktree: воркер — `dotnet build src/KafkaWorker.App/KafkaWorker.App.csproj` (подтягивает весь граф воркера), `dotnet test src/tests/KafkaWorker.UnitTests`; панель — `dotnet build src/AdminPanel.Api/AdminPanel.Api.csproj`, `dotnet test src/tests/AdminPanel.UnitTests`. Полная верификация — `dotnet build src/PgWorker.slnx`.
- Etcd-контракт не менять: heartbeat/дискавери-ключи `/kafkaworker/instances|api/*` не расширяются (решение Д1 spec §9 — канал правды `/healthz`, не etcd).
- Формат алертов: единственный новый id-шаблон `worker-unhealthy:kafkaworker/<instanceId>` (kind `worker-unhealthy` уже в каталоге arch/03 §4 на оба воркера). Target `worker-unhealthy` = `kafkaworker/<id>`; target `worker-api-unreachable` = `kafkaworker` (без слэша — домен целиком, `KafkaAlertEngine.cs:47`).
- Docker HEALTHCHECK и compose-конфиги стенда не меняются.
- **Семантика WorkerHealth в панели (spec §3.4, критично)**: свежий стор поллера мерджится в снапшот ТОЛЬКО успешным тиком kafka-refresher'а; `FailTick` переносит `previous?.WorkerHealth ?? []` (симметрия pg `src/AdminPanel.Etcd/SnapshotRefresher.cs:225`) — никаких «свежих проб при лежащем etcd».

## File Structure (карта изменений)

| Файл | Задача | Ответственность |
|---|---|---|
| `src/KafkaWorker.App/Loops/ReconcileLoop.cs` | T1 | сброс `StatusError` при успешном тике |
| `src/KafkaWorker.App/Loops/SnapshotLoop.cs` | T1 | сброс при успешном `TakeAsync` |
| `src/KafkaWorker.App/Loops/KeepaliveLoop.cs` | T1 | сброс каждым проходом контура |
| `src/tests/KafkaWorker.UnitTests/Provisioning/Fakes.cs` | T1 | хуки `RangeFault`/`SnapshotFault` у FakeEtcd |
| `src/tests/KafkaWorker.UnitTests/App/TestSupport.cs` | T1 (создание), T2 | `FixedOptionsMonitor<KafkaWorkerOptions>`, T2: `ThrowingEtcd` |
| `src/tests/KafkaWorker.UnitTests/App/LoopsHealthResetTests.cs` | T1 (создание) | тесты живой-Ф7 трёх циклов + не-лидер |
| `src/KafkaWorker.App/HealthChecks/ServiceProbes.cs` | T2 | catch-all `EtcdReachableAsync`/`PingDockerHostsAsync`, фикс текста «KafkaWorker:Etcd…» |
| `src/KafkaWorker.App/HealthChecks/KafkaWorkerHealth.cs` | T2 | catch-all чека → Degraded с данными |
| `src/tests/KafkaWorker.UnitTests/App/HealthTests.cs` | T2 (создание) | тесты catch-all проб/чека + бросающая docker-фабрика |
| `src/KafkaWorker.App/EtcdConnectCallback.cs` | T3 (создание) | IPv4-first резолв + фабрика handler'а |
| `src/KafkaWorker.App/Program.cs` | T3 | `AddHttpClient("etcd").ConfigurePrimaryHttpMessageHandler(...)` |
| `src/tests/KafkaWorker.UnitTests/App/EtcdConnectCallbackTests.cs` | T3 (создание) | тесты сортировки/литералов/отказов/конфигурации |
| `src/AdminPanel.Core/Kafka/KafkaSnapshot.cs` | T4 | поле `WorkerHealth` |
| `src/AdminPanel.Core/WorkerHealth.cs` | T4 | интерфейс `IKafkaWorkerHealthStore` |
| `src/AdminPanel.Etcd/Workers/KafkaWorkerHealthStore.cs` | T4 (создание) | стор результатов опроса |
| `src/AdminPanel.Etcd/KafkaSnapshotRefresher.cs` | T4 | мердж `WorkerHealth` успешным тиком; FailTick — перенос из previous |
| `src/tests/AdminPanel.UnitTests/KafkaRefresherTests.cs` | T4 | кейсы мерджа/переноса + починка конструкторов снапшота |
| `src/AdminPanel.Etcd/Workers/WorkerHealthPoller.cs` | T5 | kafka-эндпоинты в том же тике |
| `src/tests/AdminPanel.UnitTests/Workers/WorkerHealthPollerTests.cs` | T5 | kafka-кейсы поллера |
| `src/AdminPanel.Core/Kafka/KafkaAlerting/KafkaAlertEngine.cs` | T6 | правило `worker-unhealthy` |
| `src/tests/AdminPanel.UnitTests/KafkaAlertRulesTests.cs` | T6 | кейсы правила |
| `dev-stand/adminpanel/checks/57-kafka-worker-health.sh` | T7 (создание) | e2e: transient etcd → сброс + алерт + гашение |

Порядок задач = порядок фаз spec §4: T1–T2 (фаза 2 «честный healthz»), T3 (фаза 3 «etcd-клиент»), T4–T6 (фаза 4 «панель»; T5/T6 зависят от T4), T7 (фаза 5 «стенд»), T8 (фаза 6 «верификация»). T1, T2, T3 независимы между собой.

---

### Task 1: Воркер — сброс `StatusError` (живой-Ф7, spec §3.1)

**Files:**
- Modify: `src/KafkaWorker.App/Loops/ReconcileLoop.cs:44-57` (ветка `tick.IsSuccess`)
- Modify: `src/KafkaWorker.App/Loops/SnapshotLoop.cs:48-62` (ветка `shot.IsSuccess`)
- Modify: `src/KafkaWorker.App/Loops/KeepaliveLoop.cs:38-46` (тело цикла)
- Modify: `src/tests/KafkaWorker.UnitTests/Provisioning/Fakes.cs` (хуки отказов FakeEtcd)
- Create: `src/tests/KafkaWorker.UnitTests/App/TestSupport.cs`
- Create: `src/tests/KafkaWorker.UnitTests/App/LoopsHealthResetTests.cs`

**Interfaces:**
- Consumes: `Result.Success()`/`Result.Failed` (KafkaWorker.Core), `FakeEtcd` (internal, KafkaWorker.UnitTests/Provisioning), `IOptionsMonitor<KafkaWorkerOptions>`; конструкторы циклов не меняются.
- Produces: `FixedOptionsMonitor` (internal, TestSupport.cs — переиспользуется T2); `FakeEtcd.RangeFault: Func<string, Exception?>?` и `FakeEtcd.SnapshotFault: Func<Exception?>?`; поведение `IHealthCheckService.StatusError` = «последний тик» (потребитель — `HealthCheckAbstract<T>`, без правок).

- [ ] **Step 1.1: Написать падающие тесты (LoopsHealthResetTests.cs)**

Создать `src/tests/KafkaWorker.UnitTests/App/TestSupport.cs` (порт `PgWorker.UnitTests/App/TestSupport.cs`, только монитор; `ThrowingEtcd` сюда добавит T2):

```csharp
using Microsoft.Extensions.Options;
using KafkaWorker.App;

namespace KafkaWorker.UnitTests.App;

// IOptionsMonitor-дабл (порт PgWorker.UnitTests/App/TestSupport.cs): фиксированные
// настройки KafkaWorkerOptions для тестов циклов/health (t09).
internal sealed class FixedOptionsMonitor(KafkaWorkerOptions value) : IOptionsMonitor<KafkaWorkerOptions>
{
    public KafkaWorkerOptions CurrentValue { get; } = value;

    public IDisposable? OnChange(Action<KafkaWorkerOptions, string?> listener) => null;

    public KafkaWorkerOptions Get(string? name) => value;
}
```

Добавить хуки в `src/tests/KafkaWorker.UnitTests/Provisioning/Fakes.cs`, класс `FakeEtcd` (первая строка `public Action<string>? OnPut` — рядом):

```csharp
// Транспортный отказ range (живой-Ф7-тесты, t09): префикс → исключение (обёрнуто в Failed).
public Func<string, Exception?>? RangeFault { get; set; }

// Отказ снятия снапшота (SnapshotLoop-тесты, t09).
public Func<Exception?>? SnapshotFault { get; set; }
```

В `RangeAsync` — первой строкой тела метода (до чтения Store):

```csharp
if (RangeFault is { } fault && fault(prefix) is { } ex)
    return Task.FromResult(Result<IReadOnlyList<Kv>>.Failed(ex));
```

В `SnapshotSaveAsync` — аналогично перед возвратом:

```csharp
if (SnapshotFault is { } fault && fault() is { } ex)
    return Task.FromResult(Result<byte[]>.Failed(ex));
```

Создать `src/tests/KafkaWorker.UnitTests/App/LoopsHealthResetTests.cs` (порт теста `ExecuteAsync_StatusErrorStickyUntilNextSuccessfulTick` из `PgWorker.UnitTests/App/ReconcileLoopTests.cs:52-92`):

```csharp
using Microsoft.Extensions.Logging.Abstractions;
using KafkaWorker.App;
using KafkaWorker.App.Loops;
using KafkaWorker.Core;
using KafkaWorker.Etcd.Coordination;
using KafkaWorker.Etcd.Parsing;
using KafkaWorker.UnitTests.Provisioning;
using Xunit;

namespace KafkaWorker.UnitTests.App;

// Живой-Ф7 (t09; spec §3.1): StatusError = «последний тик» — провальный тик
// зажигает ошибку healthz, успешный гасит. Липкая ошибка = вечный 503
// «<Loop> service has error» при живых тиках — дефект наблюдаемости 2026-08-31.
public class LoopsHealthResetTests
{
    private static FixedOptionsMonitor Options(
        int scanSec = 0, int errorDelayMs = 200, int snapshotMin = 0, int keepaliveSec = 0) =>
        new(new KafkaWorkerOptions
        {
            Etcd = new EtcdOptions { Endpoints = ["http://etcd:2379"] },
            Loops = new LoopsOptions
            {
                ScanIntervalSec = scanSec, ErrorDelayMs = errorDelayMs,
                SnapshotIntervalMin = snapshotMin, KeepaliveSec = keepaliveSec,
            },
        });

    private static async Task WaitUntilAsync(Func<bool> done)
    {
        for (var i = 0; i < 300 && !done(); i++)
            await Task.Delay(10, TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task ReconcileLoop_StatusErrorStickyUntilNextSuccessfulTick()
    {
        // Arrange: цикл с нулевым интервалом; etcd «падает» на range (RangeFault →
        // тик проваливается, ErrorDelayMs=200 — детерминированное окно), затем оживает.
        var etcd = new Fakes.FakeEtcd();
        etcd.RangeFault = _ => new ApplicationException("etcd недоступен");
        var loop = new ReconcileLoop(
            Options(), etcd,
            new ClaimStore(["http://etcd:2379"], etcd, TimeProvider.System),
            new FakeProcesses(),
            new WorkJournal(etcd, ["http://etcd:2379"]),
            NullLogger<ReconcileLoop>.Instance, new HealthState(TimeProvider.System));
        using var cts = new CancellationTokenSource();
        await loop.StartAsync(cts.Token);

        // Act 1: ждём провального тика.
        await WaitUntilAsync(() => !loop.StatusError.IsSuccess);

        // Assert 1: ошибка последнего тика видна (unhealthy).
        loop.StatusError.IsSuccess.Should().BeFalse();

        // Act 2: etcd оживает → следующий тик успешен.
        etcd.RangeFault = null;
        await WaitUntilAsync(() => loop.StatusError.IsSuccess);

        // Assert 2: успешный тик погасил ошибку (healthz = «последний тик»).
        loop.StatusError.IsSuccess.Should().BeTrue(loop.StatusError.Error?.ToString());
        await loop.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task SnapshotLoop_StatusErrorResetBySuccessfulTake()
    {
        // Arrange: лидер (одиночный инстанс), снапшот-джоб во временной папке;
        // снятие снапшота падает (SnapshotFault), затем оживает.
        var etcd = new Fakes.FakeEtcd();
        etcd.SnapshotFault = () => new ApplicationException("snapshot failed");
        var options = Options(snapshotMin: 0);
        var job = new SnapshotJob(
            etcd, ["http://etcd:2379"],
            Path.Combine(Path.GetTempPath(), $"kfw-health-{Guid.NewGuid():N}"), 10, 60);
        var loop = new SnapshotLoop(
            options, new ClaimStore(["http://etcd:2379"], etcd, TimeProvider.System), job,
            NullLogger<SnapshotLoop>.Instance, new HealthState(TimeProvider.System));
        using var cts = new CancellationTokenSource();
        await loop.StartAsync(cts.Token);

        // Act 1: лидерство захвачено, первый TakeAsync провален.
        await WaitUntilAsync(() => !loop.StatusError.IsSuccess);
        loop.StatusError.IsSuccess.Should().BeFalse();

        // Act 2: снапшот оживает → успешный TakeAsync.
        etcd.SnapshotFault = null;
        await WaitUntilAsync(() => loop.StatusError.IsSuccess);

        // Assert: успешный снимок погасил ошибку (живой-Ф7).
        loop.StatusError.IsSuccess.Should().BeTrue(loop.StatusError.Error?.ToString());
        await loop.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task SnapshotLoop_NotLeader_KeepsStatusErrorSuccess()
    {
        // Arrange: лидерство занято другим инстансом (version>0 у /kafkaworker/leader —
        // txn NotExists проигран) — цикл живёт в ветке не-лидера (MarkSnapshotTick,
        // без TakeAsync; spec §3.1: у не-лидера ошибок взятия не бывает).
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/kafkaworker/leader", """{"instance":"other","since_unix":1}""");
        var job = new SnapshotJob(
            etcd, ["http://etcd:2379"],
            Path.Combine(Path.GetTempPath(), $"kfw-health-{Guid.NewGuid():N}"), 10, 60);
        var loop = new SnapshotLoop(
            Options(), new ClaimStore(["http://etcd:2379"], etcd, TimeProvider.System), job,
            NullLogger<SnapshotLoop>.Instance, new HealthState(TimeProvider.System));
        using var cts = new CancellationTokenSource();
        await loop.StartAsync(cts.Token);

        // Act: не-лидер тикает (попытки захвата проиграны) несколько проходов.
        await WaitUntilAsync(() => loop.Inited && loop.Working);
        await Task.Delay(100, TestContext.Current.CancellationToken);

        // Assert: ошибок нет и не было — StatusError Success (сброс/фейл — только
        // ветка лидера с реальным TakeAsync).
        loop.StatusError.IsSuccess.Should().BeTrue();
        await loop.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task KeepaliveLoop_PassKeepsStatusErrorSuccess()
    {
        // Arrange: у KeepaliveLoop фейлящих тиков нет — контракт цикла: пока проходы
        // контура живы, StatusError остаётся Success (сброс каждым проходом).
        var etcd = new Fakes.FakeEtcd();
        var loop = new KeepaliveLoop(
            Options(keepaliveSec: 0), new ClaimStore(["http://etcd:2379"], etcd, TimeProvider.System),
            NullLogger<KeepaliveLoop>.Instance, new HealthState(TimeProvider.System));
        using var cts = new CancellationTokenSource();

        // Act: цикл жив несколько проходов.
        await loop.StartAsync(cts.Token);
        await WaitUntilAsync(() => loop.Inited && loop.Working);
        await Task.Delay(100, TestContext.Current.CancellationToken);

        // Assert: ошибки нет — healthz цикла Healthy.
        loop.StatusError.IsSuccess.Should().BeTrue();
        await loop.StopAsync(CancellationToken.None);
    }

    // Пустые процессы: тик ReconcileLoop без кластеров — успех.
    private sealed class FakeProcesses : IKafkaClusterProcesses
    {
        public Task<Result> ProvisionAsync(KafkaClusterSnapshot snap, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result> DeprovisionAsync(KafkaClusterSnapshot snap, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result> ActiveAsync(KafkaClusterSnapshot snap, CancellationToken ct)
            => Task.FromResult(Result.Success());
    }
}
```

Примечание: сверить точные пространства имён `IKafkaClusterProcesses` (`KafkaWorker.App.Loops`) и `KafkaClusterSnapshot` (`KafkaWorker.Etcd.Parsing`) по `src/KafkaWorker.App/Loops/KafkaClusterProcesses.cs` и подправить using при необходимости; `using FluentAssertions;` — добавить (при отсутствии в GlobalUsings — явный using).

- [ ] **Step 1.2: Прогнать тесты — убедиться в провале**

Run: `dotnet test src/tests/KafkaWorker.UnitTests --filter "FullyQualifiedName~LoopsHealthResetTests"`
Expected: FAIL — `ReconcileLoop_StatusErrorStickyUntilNextSuccessfulTick` и `SnapshotLoop_StatusErrorResetBySuccessfulTake` виснут до таймаута WaitUntilAsync/теста (ошибка никогда не сбрасывается — sticky) либо падают Assert 2. Тесты `SnapshotLoop_NotLeader…` и `KeepaliveLoop…` могут пройти сразу (фиксация контракта, не сброс).

- [ ] **Step 1.3: Реализовать сброс (минимальные правки циклов)**

`src/KafkaWorker.App/Loops/ReconcileLoop.cs` — в `ExecuteAsync`, первой строкой ветки `if (tick.IsSuccess)` (перед `await Task.Delay(...ScanIntervalSec...)`):

```csharp
// healthz = «последний тик» (живой-Ф7, порт PgWorker ReconcileLoop): успешный
// тик гасит ошибку прошлого — иначе единственный упавший тик = вечный unhealthy.
StatusError = Result.Success();
```

`src/KafkaWorker.App/Loops/SnapshotLoop.cs` — в ветке `if (shot.IsSuccess)`, перед `health.MarkSnapshotTaken()`:

```csharp
// healthz = «последний тик» (живой-Ф7): успешный снимок гасит ошибку
// прошлого — иначе единственный фейл = вечный unhealthy.
StatusError = Result.Success();
```

`src/KafkaWorker.App/Loops/KeepaliveLoop.cs` — в теле `while`-цикла, перед `health.MarkKeepaliveTick()`:

```csharp
// healthz = «последний тик» (живой-Ф7, симметрия остальных циклов):
// проход контура жив — ошибка прошлого тика (если появится) гасится.
StatusError = Result.Success();
```

- [ ] **Step 1.4: Прогнать тесты — убедиться в прохождении**

Run: `dotnet test src/tests/KafkaWorker.UnitTests --filter "FullyQualifiedName~LoopsHealthResetTests"`
Expected: PASS (4/4). Затем весь набор: `dotnet test src/tests/KafkaWorker.UnitTests` — PASS (регрессий нет).

- [ ] **Step 1.5: Сборка без ворнингов**

Run: `dotnet build src/KafkaWorker.App/KafkaWorker.App.csproj`
Expected: 0 errors, 0 warnings (TreatWarningsAsErrors).

- [ ] **Step 1.6: Commit**

```bash
git add src/KafkaWorker.App/Loops/ReconcileLoop.cs src/KafkaWorker.App/Loops/SnapshotLoop.cs \
  src/KafkaWorker.App/Loops/KeepaliveLoop.cs src/tests/KafkaWorker.UnitTests/Provisioning/Fakes.cs \
  src/tests/KafkaWorker.UnitTests/App/TestSupport.cs src/tests/KafkaWorker.UnitTests/App/LoopsHealthResetTests.cs
git commit -m "feat(kafka): живой-Ф7 — успешный тик цикла гасит StatusError прошлого (Reconcile/Snapshot/Keepalive; порт PgWorker-циклов, t09 spec §3.1): transient-сбой ≠ вечный 503; RangeFault/SnapshotFault хуки FakeEtcd + FixedOptionsMonitor + тесты сброса и не-лидера"
```

---

### Task 2: Воркер — catch-all проб и чека (spec §3.2)

**Files:**
- Modify: `src/KafkaWorker.App/HealthChecks/ServiceProbes.cs` (`EtcdReachableAsync`, `PingDockerHostsAsync`, текст исключения)
- Modify: `src/KafkaWorker.App/HealthChecks/KafkaWorkerHealth.cs` (`CheckHealthAsync` → catch-all + `CheckCoreAsync`)
- Modify: `src/tests/KafkaWorker.UnitTests/App/TestSupport.cs` (добавить `ThrowingEtcd`)
- Create: `src/tests/KafkaWorker.UnitTests/App/HealthTests.cs`

**Interfaces:**
- Consumes: `Result`/`Result<T>` (KafkaWorker.Core), `ServiceProbes(IEtcdGateway, IOptionsMonitor<KafkaWorkerOptions>, DockerEngineFactory)`, `KafkaWorkerHealth(ServiceProbes, HealthState, ClaimStore, IOptionsMonitor<KafkaWorkerOptions>, TimeProvider)`, `FixedOptionsMonitor` (T1), `Fakes.FakeEtcd` (T1 расширил `RangeFault`), `DockerEngineFactory.Create` (virtual — переопределяется тестом).
- Produces: контракт «проба возвращает `Result`, чек — `HealthCheckResult` с Data при любых отказах» (потребители: `Program.cs` health-регистрация без правок; T3 не зависит).

- [ ] **Step 2.1: Написать падающие тесты (HealthTests.cs)**

Добавить в `src/tests/KafkaWorker.UnitTests/App/TestSupport.cs`:

```csharp
// Шлюз, бросающий сетевые исключения (t09; spec §3.2): .NET DNS-флейп
// «Name or service not known» летит из HttpClient наружу.
internal sealed class ThrowingEtcd : KafkaWorker.Etcd.Client.IEtcdGateway
{
    public Task<KafkaWorker.Core.Result<IReadOnlyList<KafkaWorker.Etcd.Client.Kv>>> RangeAsync(
        string endpoint, string prefix, CancellationToken ct)
        => throw new HttpRequestException($"Name or service not known ({new Uri(endpoint).Host}:2379)");

    public Task<KafkaWorker.Core.Result<KafkaWorker.Etcd.Client.Kv?>> GetAsync(string endpoint, string key, CancellationToken ct)
        => throw new HttpRequestException("unreachable");

    public Task<KafkaWorker.Core.Result> PutAsync(string endpoint, string key, string value, long? lease, CancellationToken ct)
        => throw new HttpRequestException("unreachable");

    public Task<KafkaWorker.Core.Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
        => throw new HttpRequestException("unreachable");

    public Task<KafkaWorker.Core.Result<KafkaWorker.Etcd.Client.TxnResult>> TxnAsync(
        string endpoint, KafkaWorker.Etcd.Client.TxnRequest req, CancellationToken ct)
        => throw new HttpRequestException("unreachable");

    public Task<KafkaWorker.Core.Result<long>> LeaseGrantAsync(string endpoint, int ttlSec, CancellationToken ct)
        => throw new HttpRequestException("unreachable");

    public Task<KafkaWorker.Core.Result> LeaseRevokeAsync(string endpoint, long lease, CancellationToken ct)
        => throw new HttpRequestException("unreachable");

    public Task<KafkaWorker.Core.Result> LeaseKeepaliveAsync(string endpoint, long lease, CancellationToken ct)
        => throw new HttpRequestException("unreachable");

    public Task<KafkaWorker.Core.Result<byte[]>> SnapshotSaveAsync(string endpoint, CancellationToken ct)
        => throw new HttpRequestException("unreachable");

    public Task<KafkaWorker.Core.Result<long>> StatusAsync(string endpoint, CancellationToken ct)
        => throw new HttpRequestException("unreachable");

    public Task<KafkaWorker.Core.Result> CompactAsync(string endpoint, long revision, CancellationToken ct)
        => throw new HttpRequestException("unreachable");

    public Task<KafkaWorker.Core.Result> DefragmentAsync(string endpoint, CancellationToken ct)
        => throw new HttpRequestException("unreachable");
}
```

(Сверить полный список членов `IEtcdGateway` по `src/KafkaWorker.Etcd/Client/IEtcdGateway.cs` и реализовать все — компилятор подскажет.)

Создать `src/tests/KafkaWorker.UnitTests/App/HealthTests.cs`:

```csharp
using Microsoft.Extensions.Diagnostics.HealthChecks;
using KafkaWorker.App;
using KafkaWorker.App.HealthChecks;
using KafkaWorker.Core;
using KafkaWorker.Etcd.Coordination;
using KafkaWorker.UnitTests.Provisioning;
using Xunit;

namespace KafkaWorker.UnitTests.App;

// Catch-all проб и чека (t09; spec §3.2): сетевое исключение шлюза → Result.Failed
// (Degraded с секциями), чек никогда не падает исключением (DefaultHealthCheckService[103]).
public class HealthTests
{
    private static readonly FixedOptionsMonitor Options = new(new KafkaWorkerOptions
    {
        Etcd = new EtcdOptions { Endpoints = ["http://etcd:2379"] },
        Docker = new DockerOptions { Hosts = [] },
    });

    private static ServiceProbes Probes(KafkaWorker.Etcd.Client.IEtcdGateway etcd)
        => new(etcd, Options, new KafkaWorker.Docker.Engine.DockerEngineFactory());

    [Fact]
    public async Task EtcdProbe_GatewayThrows_ReturnsFailedNotThrows()
    {
        // Arrange: шлюз бросает HttpRequestException (DNS-флейп).
        var probes = Probes(new ThrowingEtcd());

        // Act
        var result = await probes.EtcdReachableAsync(TestContext.Current.CancellationToken);

        // Assert: структура, не исключение — секция etcd отдаст Degraded с данными.
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<Exception>();
        result.Error!.Message.Should().Contain("etcd-проба");
    }

    [Fact]
    public async Task EtcdProbe_HealthyGateway_ReturnsSuccess()
    {
        // Arrange: живой fake-шлюз.
        var probes = Probes(new Fakes.FakeEtcd());

        // Act
        var result = await probes.EtcdReachableAsync(TestContext.Current.CancellationToken);

        // Assert
        result.IsSuccess.Should().BeTrue();
    }

    [Fact]
    public async Task DockerPing_NoHosts_EmptyDictionary()
    {
        // Arrange: plain-режим без хостов (стендовая конфигурация по умолчанию).
        var probes = Probes(new Fakes.FakeEtcd());

        // Act
        var hosts = await probes.PingDockerHostsAsync(TestContext.Current.CancellationToken);

        // Assert: нет хостов — нет записей, не Degraded.
        hosts.Should().BeEmpty();
    }

    // Фабрика docker-клиентов, бросающая при создании (t09; spec §3.2: пер-хостовая
    // проба оборачивает исключение в Failed — структура, не бросок).
    private sealed class ThrowingFactory : KafkaWorker.Docker.Engine.DockerEngineFactory
    {
        public override KafkaWorker.Docker.Engine.IDockerEngine Create(string endpoint, string? hostAlias = null)
            => throw new ApplicationException("docker engine недоступен");
    }

    [Fact]
    public async Task DockerPing_ThrowingFactory_PerHostFailed()
    {
        // Arrange: один настроенный docker-хост; фабрика бросает при создании клиента.
        var options = new FixedOptionsMonitor(new KafkaWorkerOptions
        {
            Etcd = new EtcdOptions { Endpoints = ["http://etcd:2379"] },
            Docker = new DockerOptions
            {
                Hosts = [new DockerHostOptions { Name = "h1", Endpoint = "unix:///var/run/docker.sock" }],
            },
        });
        var probes = new ServiceProbes(new Fakes.FakeEtcd(), options, new ThrowingFactory());

        // Act
        var hosts = await probes.PingDockerHostsAsync(TestContext.Current.CancellationToken);

        // Assert: per-host Failed (catch в PingAsync) — секция docker-hosts отдаст
        // Degraded с именем хоста, не исключение.
        hosts.Should().ContainKey("h1");
        hosts["h1"].IsSuccess.Should().BeFalse();
        hosts["h1"].Error!.Message.Should().Contain("docker h1");
    }

    // Опции, бросающие при чтении — единственный seam, которым можно уронить
    // тело чека целиком (после catch-all проб): KafkaWorkerHealth обязан
    // вернуть Degraded со структурой, а не исключение.
    private sealed class ThrowingOptionsMonitor : Microsoft.Extensions.Options.IOptionsMonitor<KafkaWorkerOptions>
    {
        public KafkaWorkerOptions CurrentValue => throw new ApplicationException("конфигурация недоступна");

        public KafkaWorkerOptions Get(string? name) => throw new ApplicationException("конфигурация недоступна");

        public IDisposable? OnChange(Action<KafkaWorkerOptions, string?> listener) => null;
    }

    [Fact]
    public async Task Check_UnexpectedExceptionInside_DegradedWithStructure()
    {
        // Arrange: любая непредвиденная ошибка тела чека (тут — опции).
        var check = new KafkaWorkerHealth(
            Probes(new Fakes.FakeEtcd()), new HealthState(TimeProvider.System),
            new ClaimStore(["http://etcd:2379"], new Fakes.FakeEtcd(), TimeProvider.System),
            new ThrowingOptionsMonitor(), TimeProvider.System);

        // Act
        var result = await check.CheckHealthAsync(new HealthCheckContext(), TestContext.Current.CancellationToken);

        // Assert: Degraded с данными секции error — не исключение чека.
        result.Status.Should().Be(HealthStatus.Degraded);
        result.Data.Keys.Should().Contain("error");
    }
}
```

- [ ] **Step 2.2: Прогнать тесты — убедиться в провале**

Run: `dotnet test src/tests/KafkaWorker.UnitTests --filter "FullyQualifiedName~HealthTests"`
Expected: FAIL — `EtcdProbe_GatewayThrows…` падает: `HttpRequestException` летит из `EtcdReachableAsync` наружу; `Check_UnexpectedExceptionInside…` падает: исключение из `CheckHealthAsync` (не поймано). `DockerPing_ThrowingFactory…` может пройти сразу (существующий per-host catch в `PingAsync`) — фиксация контракта.

- [ ] **Step 2.3: Реализовать catch-all**

`src/KafkaWorker.App/HealthChecks/ServiceProbes.cs` — метод `EtcdReachableAsync` целиком:

```csharp
/// <summary>etcd жив: хотя бы один endpoint отвечает на range по /kafkaworker/.
/// Catch-all (t09, arch/16 §7): сетевое исключение — тоже Failed-результат,
/// чек получает структуру, а не исключение (DNS-флейп не роняет health).</summary>
public async Task<Result> EtcdReachableAsync(CancellationToken ct)
{
    using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
    timeout.CancelAfter(ProbeTimeout);

    try
    {
        Result? last = null;
        foreach (var endpoint in options.CurrentValue.Etcd.Endpoints)
        {
            var range = await etcd.RangeAsync(endpoint, "/kafkaworker/", timeout.Token);
            if (range.IsSuccess)
                return Result.Success();
            last = Result.Failed(range.Error!);
        }

        return last ?? Result.Failed(new ApplicationException("KafkaWorker:Etcd:Endpoints не заданы"));
    }
    catch (OperationCanceledException) when (ct.IsCancellationRequested)
    {
        throw; // остановка самого запроса health-чека — не «etcd молчит»
    }
    catch (Exception ex)
    {
        return Result.Failed(new ApplicationException($"etcd-проба: {ex.Message}", ex));
    }
}
```

(заодно исправлен копипаст-текст «PgWorker:Etcd:Endpoints» → «KafkaWorker:Etcd:Endpoints»).

Метод `PingDockerHostsAsync` — тело после построения `targets` обернуть:

```csharp
var results = new Dictionary<string, Result>();
try
{
    foreach (var (name, endpoint) in targets)
        results[name] = await PingAsync(name, endpoint, timeout.Token);
}
catch (OperationCanceledException) when (ct.IsCancellationRequested)
{
    throw;
}
catch (Exception ex)
{
    // Catch-all (t09): непредвиденный отказ вне per-host-вызовов — структура, не бросок.
    results["all"] = Result.Failed(new ApplicationException($"docker-проба: {ex.Message}", ex));
}

return results;
```

`src/KafkaWorker.App/HealthChecks/KafkaWorkerHealth.cs` — публичный `CheckHealthAsync` и текущее тело:

```csharp
public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken ct = default)
{
    try
    {
        return await CheckCoreAsync(ct);
    }
    catch (OperationCanceledException) when (ct.IsCancellationRequested)
    {
        throw;
    }
    catch (Exception ex)
    {
        // Catch-all (t09, arch/16 §7): чек ВСЕГДА отдаёт структуру — неожиданный
        // отказ тела превращается в Degraded с данными, а не исключение чека.
        return HealthCheckResult.Degraded(
            $"health-чек выполнился с ошибкой: {ex.Message}",
            data: new Dictionary<string, object> { ["error"] = ex.Message });
    }
}

private async Task<HealthCheckResult> CheckCoreAsync(CancellationToken ct)
{
    // …прежнее тело CheckHealthAsync без изменений (etcd/docker-hosts/loops/claims/snapshot)…
}
```

- [ ] **Step 2.4: Прогнать тесты — убедиться в прохождении**

Run: `dotnet test src/tests/KafkaWorker.UnitTests --filter "FullyQualifiedName~HealthTests"`
Expected: PASS (5/5). Весь набор: `dotnet test src/tests/KafkaWorker.UnitTests` — PASS.

- [ ] **Step 2.5: Сборка + Commit**

Run: `dotnet build src/KafkaWorker.App/KafkaWorker.App.csproj` → 0 warnings.

```bash
git add src/KafkaWorker.App/HealthChecks/ServiceProbes.cs src/KafkaWorker.App/HealthChecks/KafkaWorkerHealth.cs \
  src/tests/KafkaWorker.UnitTests/App/TestSupport.cs src/tests/KafkaWorker.UnitTests/App/HealthTests.cs
git commit -m "feat(kafka): catch-all активных проб и агрегированного чека /healthz (t09 spec §3.2): сетевое исключение (DNS-флейп) → Result.Failed → Degraded с секциями, чек всегда отдаёт структуру; пер-хост Failed при бросающей docker-фабрике; фикс текста KafkaWorker:Etcd:Endpoints"
```

---

### Task 3: Воркер — `EtcdConnectCallback` + конфигурация etcd-HttpClient (spec §3.3)

**Files:**
- Create: `src/KafkaWorker.App/EtcdConnectCallback.cs`
- Modify: `src/KafkaWorker.App/Program.cs:29` (`AddHttpClient("etcd")`)
- Create: `src/tests/KafkaWorker.UnitTests/App/EtcdConnectCallbackTests.cs`

**Interfaces:**
- Consumes: `SocketsHttpHandler`/`SocketsHttpConnectionContext` (System.Net.Http), прецедент `src/KafkaWorker.Docker/Engine/DockerEngine.cs:16-43` (SocketsHttpHandler + PooledConnectionLifetime + ConnectCallback).
- Produces: `EtcdConnectCallback.CreateHandler() → SocketsHttpHandler` (потребляет `Program.cs`), `EtcdConnectCallback.ConnectAsync(SocketsHttpConnectionContext, CancellationToken) → ValueTask<Stream>`, `EtcdConnectCallback.OrderIpv4First(IPAddress[]) → IPAddress[]` (internal, для тестов).

- [ ] **Step 3.1: Написать падающие тесты (EtcdConnectCallbackTests.cs)**

```csharp
using System.Net;
using System.Net.Sockets;
using KafkaWorker.App;
using Xunit;

namespace KafkaWorker.UnitTests.App;

// etcd-клиент против DNS-флейпа Docker embedded DNS (t09; spec §3.3, arch/16 §7):
// PooledConnectionLifetime (пере-резолв после пересоздания etcd-контейнера) +
// IPv4-first последовательный резолв (параллельные A/AAAA флейпят).
public class EtcdConnectCallbackTests
{
    [Fact]
    public void OrderIpv4First_Ipv4BeforeIpv6()
    {
        // Arrange: перемешанные адреса.
        var ipv6 = IPAddress.Parse("fd00::1");
        var ipv4 = IPAddress.Parse("10.0.0.2");
        var mixed = new[] { ipv6, ipv4 };

        // Act
        var ordered = EtcdConnectCallback.OrderIpv4First(mixed);

        // Assert: IPv4 — первый попыткой (Docker embedded DNS держит A-записи).
        ordered[0].Should().Be(ipv4);
        ordered[1].Should().Be(ipv6);
    }

    [Fact]
    public void CreateHandler_ConfiguredWithLifetimeAndCallback()
    {
        // Arrange/Act: фабрика handler'а именованного клиента "etcd".
        var handler = EtcdConnectCallback.CreateHandler();

        // Assert: пул пере-резолвится (5 мин — прецедент DockerEngineFactory),
        // резолв — кастомный IPv4-first.
        handler.PooledConnectionLifetime.Should().Be(TimeSpan.FromMinutes(5));
        handler.ConnectCallback.Should().NotBeNull();
    }

    [Fact]
    public async Task ConnectAsync_AllAddressesDead_ThrowsLast()
    {
        // Arrange: единственный адрес — незанятый порт localhost (connection refused).
        var context = new SocketsHttpConnectionContext(
            new DnsEndPoint("127.0.0.1", 1), null!);

        // Act
        var act = () => EtcdConnectCallback.ConnectAsync(context, TestContext.Current.CancellationToken);

        // Assert: бросок последнего отказа (шлюз обернёт в Result.Failed — проба
        // отдаст структуру), а не «тихое» зависание.
        await act.Should().ThrowAsync<SocketException>();
    }

    [Fact]
    public async Task Connect_IpLiteral_GoesStraightToConnect_NoDns()
    {
        // Arrange: IP-литерал (spec §6 «IP-литерал — без DNS») — прямой вызов
        // внутренней механики ветки IPAddress.TryParse; порт закрыт (refused).
        // Act
        var act = () => EtcdConnectCallback.ConnectToAddressesAsync(
            [IPAddress.Parse("127.0.0.1")], 1, TestContext.Current.CancellationToken);

        // Assert: отказ SocketException без участия DNS-резолва (литерал идёт
        // в коннект напрямую).
        await act.Should().ThrowAsync<SocketException>();
    }
}
```

(Если `SocketsHttpConnectionContext` в текущем TFM не имеет публичного конструктора с `(DnsEndPoint, HttpRequestMessage)` — заменить третий тест на вызов внутренней механики напрямую: `await Assert.ThrowsAsync<SocketException>(() => EtcdConnectCallback.ConnectToAddressesAsync([IPAddress.Parse("127.0.0.1")], 1, TestContext.Current.CancellationToken))`; сигнатура `ConnectToAddressesAsync` — в Step 3.3 в любом случае. Выбрать по компилируемости.)

- [ ] **Step 3.2: Прогнать тесты — убедиться в провале**

Run: `dotnet test src/tests/KafkaWorker.UnitTests --filter "FullyQualifiedName~EtcdConnectCallbackTests"`
Expected: Compile ERROR — `EtcdConnectCallback` не существует.

- [ ] **Step 3.3: Реализовать `EtcdConnectCallback`**

Создать `src/KafkaWorker.App/EtcdConnectCallback.cs`:

```csharp
using System.Net;
using System.Net.Sockets;

namespace KafkaWorker.App;

// Резолв/коннект etcd-клиента против Docker embedded DNS (t09; arch/16 §7):
// 1) PooledConnectionLifetime — пул пере-резолвит DNS (застарелые адреса после
//    пересоздания etcd-контейнера; прецедент DockerEngineFactory);
// 2) последовательный IPv4-first резолв — параллельные A/AAAA-запросы .NET
//    против Docker embedded DNS (127.0.0.11) флейпят «Name or service not known».
public static class EtcdConnectCallback
{
    public static SocketsHttpHandler CreateHandler() => new()
    {
        PooledConnectionLifetime = TimeSpan.FromMinutes(5),
        ConnectCallback = ConnectAsync,
    };

    public static async ValueTask<Stream> ConnectAsync(
        SocketsHttpConnectionContext context, CancellationToken ct)
    {
        var host = context.DnsEndPoint.Host;
        var port = context.DnsEndPoint.Port;

        // IP-литерал — без DNS.
        if (IPAddress.TryParse(host, out var literal))
            return await ConnectToAddressesAsync([literal], port, ct);

        var addresses = await Dns.GetHostAddressesAsync(host, ct);
        return await ConnectToAddressesAsync(OrderIpv4First(addresses), port, ct);
    }

    // IPv4 раньше IPv6: сортировка, не фильтр (IPv6-only окружения не теряются).
    internal static IPAddress[] OrderIpv4First(IPAddress[] addresses)
        => [.. addresses.OrderBy(a => a.AddressFamily == AddressFamily.InterNetwork ? 0 : 1)];

    // Последовательные попытки: первый успех — Stream; все упали — бросок последнего
    // исключения (EtcdGateway обернёт в Result.Failed — проба отдаст структуру).
    internal static async Task<Stream> ConnectToAddressesAsync(
        IPAddress[] addresses, int port, CancellationToken ct)
    {
        Exception? last = null;
        foreach (var address in addresses)
        {
            var socket = new Socket(address.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            try
            {
                await socket.ConnectAsync(address, port, ct);
                return new NetworkStream(socket, ownsSocket: true);
            }
            catch (Exception ex)
            {
                last = ex;
                socket.Dispose();
            }
        }

        throw last ?? new SocketException((int)SocketError.HostNotFound);
    }
}
```

- [ ] **Step 3.4: Подключить handler в Program.cs**

`src/KafkaWorker.App/Program.cs` — заменить строку `builder.Services.AddHttpClient("etcd");` на:

```csharp
// etcd-клиент: HTTP JSON gateway /v3/*; handler против DNS-флейпа Docker
// embedded DNS (t09; arch/16 §7): PooledConnectionLifetime + IPv4-first резолв.
// EtcdGateway-синглтон захвачен HttpClient навсегда — ротация handler'ов фабрики
// на него не действует, поэтому явный SocketsHttpHandler.
builder.Services.AddHttpClient("etcd")
    .ConfigurePrimaryHttpMessageHandler(EtcdConnectCallback.CreateHandler);
```

- [ ] **Step 3.5: Прогнать тесты и сборку**

Run: `dotnet test src/tests/KafkaWorker.UnitTests --filter "FullyQualifiedName~EtcdConnectCallbackTests"` → PASS; `dotnet test src/tests/KafkaWorker.UnitTests` → PASS; `dotnet build src/KafkaWorker.App/KafkaWorker.App.csproj` → 0 warnings.

- [ ] **Step 3.6: Commit**

```bash
git add src/KafkaWorker.App/EtcdConnectCallback.cs src/KafkaWorker.App/Program.cs \
  src/tests/KafkaWorker.UnitTests/App/EtcdConnectCallbackTests.cs
git commit -m "feat(kafka): etcd-клиент на SocketsHttpHandler — PooledConnectionLifetime 5мин + IPv4-first последовательный резолв ConnectCallback (t09 spec §3.3): лечит флейп «Name or service not known» против Docker embedded DNS и застарелый пул после пересоздания etcd-контейнера"
```

---

### Task 4: Панель — `KafkaSnapshot.WorkerHealth` + стор + refresher-мердж (spec §3.4)

**Files:**
- Modify: `src/AdminPanel.Core/Kafka/KafkaSnapshot.cs:16` (новое поле после `WorkerEndpoints`)
- Modify: `src/AdminPanel.Core/WorkerHealth.cs` (интерфейс `IKafkaWorkerHealthStore`)
- Create: `src/AdminPanel.Etcd/Workers/KafkaWorkerHealthStore.cs`
- Modify: `src/AdminPanel.Etcd/KafkaSnapshotRefresher.cs` (конструктор, `RefreshOnceAsync`, `FailTick`)
- Modify: `src/tests/AdminPanel.UnitTests/KafkaRefresherTests.cs` (конструктор-хелперы + новые кейсы)

**Interfaces:**
- Consumes: `WorkerHealth`/`WorkerHealthStatus` (AdminPanel.Core/WorkerHealth.cs — общие с pg), `KafkaSnapshot` (позиционный record), `IWorkerHealthStore`-паттерн (`WorkerHealthStore.cs`), `KafkaSnapshotRefresher` DI-регистрация в `ModuleExtensions.AddKafka`.
- Produces: `KafkaSnapshot.WorkerHealth: IReadOnlyList<WorkerHealth>` (потребляет T6-правило; эндпоинты поллера — T5); `IKafkaWorkerHealthStore { IReadOnlyList<WorkerHealth>? Current; void Replace(IReadOnlyList<WorkerHealth>); }` (пишет T5-поллер, читает refresher успешным тиком); DI: стор подхватывается attribute-DI `AddEtcd`-AutoRegistration (assembly общий). Семантика FailTick — перенос из previous (см. Global Constraints).

- [ ] **Step 4.1: Написать падающие тесты (KafkaRefresherTests + модель)**

В `src/tests/AdminPanel.UnitTests/KafkaRefresherTests.cs` обновить хелперы: конструктор `KafkaSnapshotRefresher` получит новый параметр `IKafkaWorkerHealthStore workerHealthStore` (позиция — см. Step 4.4.1). Существующий `New` — дополнить аргументом `new KafkaWorkerHealthStore()`; плюс перегрузка со стором:

```csharp
// Перегрузка New (рядом с существующей): refresher со стором health-проб.
private static KafkaSnapshotRefresher New(
    KafkaFakeGateway gateway, IKafkaSnapshotStore store,
    AdminPanel.Etcd.Workers.KafkaWorkerHealthStore healthStore, params string[] endpoints)
    => new(
        gateway,
        new KafkaAlertEngine(Options.Create(new KafkaAlertsOptions())),
        store,
        new KafkaSecretsStore(),
        Options.Create(new EtcdOptions { Endpoints = endpoints }),
        Options.Create(new KafkaPanelOptions()),
        new FixedTimeProvider(),
        NullLogger<KafkaSnapshotRefresher>.Instance,
        healthStore);
```

(позиция `healthStore` в конструкторе refresher'а идентична хелперу — Step 4.4.1.)

Новые кейсы:

```csharp
private static readonly DateTimeOffset HealthAt =
    new(2026, 9, 2, 12, 0, 0, TimeSpan.Zero);

[Fact]
public async Task Refresh_HealthStore_MergedIntoSnapshot()
{
    // Arrange: стор поллера содержит Degraded-результат опроса /healthz воркера.
    var gateway = DemoGateway();
    var store = new KafkaSnapshotStore();
    var healthStore = new AdminPanel.Etcd.Workers.KafkaWorkerHealthStore();
    healthStore.Replace([new WorkerHealth("kw1", "http://kafkaworker:8080",
        WorkerHealthStatus.Degraded, HealthAt, "HTTP 503")]);

    // Act
    var result = await New(gateway, store, healthStore, "http://e1")
        .RefreshOnceAsync(CancellationToken.None);

    // Assert: успешный тик вносит свежее состояние поллера (arch/02 §2.3.2;
    // симметрия pg SnapshotRefresher.cs:156).
    result.IsSuccess.Should().BeTrue();
    store.Current!.WorkerHealth.Should().ContainSingle()
        .Which.Status.Should().Be(WorkerHealthStatus.Degraded);
}

[Fact]
public async Task Refresh_EtcdFail_PreservesPreviousWorkerHealth()
{
    // Arrange: успешный тик внёс Degraded из стора; затем все endpoints умирают
    // (FailEndpoints — механика соседних FailTick-тестов), а поллер записывает
    // в стор Healthy (воркер восстановился).
    var gateway = DemoGateway();
    var store = new KafkaSnapshotStore();
    var healthStore = new AdminPanel.Etcd.Workers.KafkaWorkerHealthStore();
    healthStore.Replace([new WorkerHealth("kw1", "http://kafkaworker:8080",
        WorkerHealthStatus.Degraded, HealthAt, "HTTP 503")]);
    var refresher = New(gateway, store, healthStore, "http://e1");
    await refresher.RefreshOnceAsync(CancellationToken.None);
    gateway.FailEndpoints.Add("http://e1");
    healthStore.Replace([new WorkerHealth("kw1", "http://kafkaworker:8080",
        WorkerHealthStatus.Healthy, HealthAt.AddSeconds(5), null)]);

    // Act: отказный тик.
    var result = await refresher.RefreshOnceAsync(CancellationToken.None);

    // Assert: прежний WorkerHealth перенесён из previous (Degraded), свежий стор
    // НЕ мерджится на отказном тике (симметрия pg SnapshotRefresher.cs:225,
    // spec §3.4) — алерт worker-unhealthy загорается только первым УСПЕШНЫМ
    // тиком refresher'а после восстановления etcd.
    result.IsSuccess.Should().BeFalse();
    store.Current!.WorkerHealth.Should().ContainSingle()
        .Which.Status.Should().Be(WorkerHealthStatus.Degraded);
}
```

Все создания `KafkaSnapshot(...)` в тестах — добавить `WorkerHealth: []` (найти: `grep -rn "new KafkaSnapshot(" src/tests/AdminPanel.UnitTests/`).

- [ ] **Step 4.2: Прогнать — убедиться в провале/ошибках компиляции**

Run: `dotnet test src/tests/AdminPanel.UnitTests --filter "FullyQualifiedName~KafkaRefresherTests"`
Expected: Compile ERROR — у `KafkaSnapshot` нет поля `WorkerHealth`.

- [ ] **Step 4.3: Реализовать модель и стор**

`src/AdminPanel.Core/Kafka/KafkaSnapshot.cs` — после строки `WorkerEndpoints`:

```csharp
    IReadOnlyList<WorkerHealth> WorkerHealth,        // опрос /healthz живых инстансов (t09, arch/02 §2.3.2)
```

`src/AdminPanel.Core/WorkerHealth.cs` — в конец файла:

```csharp
/// <summary>
/// Стор результатов опроса /healthz инстансов KafkaWorker (t09; arch/adminpanel/02
/// §2.3.2): poller пишет, kafka-refresher вносит готовым в снапшот — KV-тик
/// не блокируется (симметрия IWorkerHealthStore).
/// </summary>
public interface IKafkaWorkerHealthStore
{
    IReadOnlyList<WorkerHealth>? Current { get; }

    void Replace(IReadOnlyList<WorkerHealth> health);
}
```

Создать `src/AdminPanel.Etcd/Workers/KafkaWorkerHealthStore.cs`:

```csharp
using AdminPanel.Core;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Etcd.Workers;

// Стор результатов опроса /healthz инстансов KafkaWorker (t09; arch/adminpanel/02
// §2.3.2): poller пишет, kafka-refresher вносит готовым в снапшот (паттерн
// WorkerHealthStore: volatile-замена, KV-тик не блокируется).
[InjectAsSingleton(typeof(IKafkaWorkerHealthStore))]
public sealed class KafkaWorkerHealthStore : IKafkaWorkerHealthStore
{
    private volatile IReadOnlyList<WorkerHealth>? _current;

    public IReadOnlyList<WorkerHealth>? Current => _current;

    public void Replace(IReadOnlyList<WorkerHealth> health) => _current = health;
}
```

- [ ] **Step 4.4: Реализовать refresher-мердж**

`src/AdminPanel.Etcd/KafkaSnapshotRefresher.cs`:
1. Конструктор: добавить `IKafkaWorkerHealthStore workerHealthStore` последним обязательным параметром — после `ILogger<KafkaSnapshotRefresher> logger`, ПЕРЕД optional `IKafkaProbeReader? probeReader = null` (позиция идентична хелперу `New` из Step 4.1).
2. В `RefreshOnceAsync` при сборке `built` — после `workerApi.Endpoints` вставить аргумент `workerHealthStore.Current ?? []` (позиция = сразу после `WorkerEndpoints`, симметрия pg `SnapshotRefresher.cs:156`).
3. `FailTick`: пустой конструктор-заготовка `new KafkaSnapshot(now, EtcdReachable: false, ConsecutiveFailures: 0, [], [], [], [], [], [], [], [], 0)` — добавить ещё один `[]` (WorkerHealth — шестая позиция-список: после Clusters/Rotations/Rebalances/Reassignments/WorkerEndpoints, перед Probes: `[], [], [], [], [], /* WorkerHealth */ [], [], [], 0`). В `with`-блок копии previous **поле `WorkerHealth` НЕ трогать** — оно переносится из `previous` автоматически (записи `WorkerHealth = …` в `with` нет), симметрия pg `SnapshotRefresher.cs:225` («health-пробы переживают отказ etcd»):

```csharp
failed = failed with
{
    EtcdReachable = false,
    ConsecutiveFailures = failed.ConsecutiveFailures + 1,
    // WorkerHealth НЕ мерджится из стора на отказном тике (t09; spec §3.4,
    // симметрия pg): переносится из previous автоматически — свежие пробы
    // вносит только успешный тик; алерт worker-unhealthy загорается первым
    // успешным тиком refresher'а после возвращения etcd.
};
```

4. Починить все прочие места конструирования `KafkaSnapshot`: `grep -rn "new KafkaSnapshot(" src/ --include="*.cs"`. Известные: `src/tests/AdminPanel.UnitTests/KafkaRefresherTests.cs:244` (KafkaSnapshotStoreTests — добавить один `[]` шестой позицией-списком), `KafkaAlertRulesTests.Snapshot` (правится в T6). Для каждой находки — `WorkerHealth: []`.

- [ ] **Step 4.5: Прогнать тесты панели**

Run: `dotnet test src/tests/AdminPanel.UnitTests --filter "FullyQualifiedName~KafkaRefresherTests"` → PASS; затем весь набор `dotnet test src/tests/AdminPanel.UnitTests` → PASS (остальные файлы с `new KafkaSnapshot` починены Step 4.4.4).

- [ ] **Step 4.6: Сборка + Commit**

Run: `dotnet build src/AdminPanel.Api/AdminPanel.Api.csproj` → 0 warnings.

```bash
git add src/AdminPanel.Core/Kafka/KafkaSnapshot.cs src/AdminPanel.Core/WorkerHealth.cs \
  src/AdminPanel.Etcd/Workers/KafkaWorkerHealthStore.cs src/AdminPanel.Etcd/KafkaSnapshotRefresher.cs \
  src/tests/AdminPanel.UnitTests/KafkaRefresherTests.cs
git add -u src/tests/AdminPanel.UnitTests/   # прочие конструкторы снапшота в тестах
git commit -m "feat(adminpanel): KafkaSnapshot.WorkerHealth + стор IKafkaWorkerHealthStore + refresher-мердж (t09 spec §3.4): успешный тик вносит свежие пробы поллера, FailTick переносит WorkerHealth из previous (симметрия pg) — алерт загорается первым успешным тиком после возвращения etcd"
```

---

### Task 5: Панель — поллер kafka-эндпоинтов (spec §3.4)

**Files:**
- Modify: `src/AdminPanel.Etcd/Workers/WorkerHealthPoller.cs`
- Modify: `src/tests/AdminPanel.UnitTests/Workers/WorkerHealthPollerTests.cs`

**Interfaces:**
- Consumes: `IKafkaSnapshotReader.Current?.WorkerEndpoints` (поле уже есть), `IKafkaWorkerHealthStore` (T4), `WorkerApiOptions` (`HealthEnabled`/`HealthIntervalSec`/`TimeoutSec`), `WorkerApiGateway.HttpClientName`, `WorkerEndpoint` (Core).
- Produces: `WorkerHealthPoller` constructor `(ISnapshotReader, IWorkerHealthStore, IKafkaSnapshotReader, IKafkaWorkerHealthStore, IHttpClientFactory, IOptions<WorkerApiOptions>, TimeProvider, ILogger<WorkerHealthPoller>)` — потребляет Program панели (DI, правок не требует: все параметры резолвятся).

- [ ] **Step 5.1: Обновить тесты поллера (сначала красные)**

В `src/tests/AdminPanel.UnitTests/Workers/WorkerHealthPollerTests.cs`:
1. Хелпер `Poller(...)` — новая сигнатура (kafka-ридер/стор — необязательные, существующие pg-кейсы зовут без них):

```csharp
private static (WorkerHealthPoller Poller, WorkerHealthStore Store) Poller(
    Func<HttpRequestMessage, HttpResponseMessage> respond,
    EtcdSnapshot? snapshot = null,
    AdminPanel.Etcd.IKafkaSnapshotReader? kafkaReader = null,
    AdminPanel.Etcd.Workers.KafkaWorkerHealthStore? kafkaStore = null)
{
    var store = new WorkerHealthStore();
    var time = new FixedTimeProvider { Utc = Now };
    var poller = new WorkerHealthPoller(
        new StubReader(snapshot ?? TestSnapshots.Healthy(Now)), store,
        kafkaReader ?? new StubKafkaReader(null), kafkaStore ?? new KafkaWorkerHealthStore(),
        new StubFactory(new FakeHandler(respond)),
        Options.Create(new WorkerApiOptions { HealthIntervalSec = 15, TimeoutSec = 3 }),
        time, NullLogger<WorkerHealthPoller>.Instance);
    return (poller, store);
}
```

(kafka-кейсы дополнительно возвращают kafka-стор — читать результат из локальной переменной `kafkaStore`, переданной в хелпер.)

2. Новые кейсы:

```csharp
[Fact]
public async Task RunOnce_KafkaHealthz503_MarkedDegradedInKafkaStore()
{
    // Arrange: kafka-снапшот с живым ключом /kafkaworker/api/kw1; /healthz → 503.
    var kafkaStore = new KafkaWorkerHealthStore();
    var kafka = new StubKafkaReader(KafkaSnapshotWith(new WorkerEndpoint("kw1", "http://kafkaworker:8080", 1)));
    var (poller, _) = Poller(
        _ => new HttpResponseMessage(HttpStatusCode.ServiceUnavailable),
        kafkaReader: kafka, kafkaStore: kafkaStore);

    // Act
    await poller.RunOnceAsync(CancellationToken.None);

    // Assert: kafka-стор получил Degraded (тот же тик/клиент/семантика, что pg).
    kafkaStore.Current!.Should().ContainSingle()
        .Which.Status.Should().Be(WorkerHealthStatus.Degraded);
}

[Fact]
public async Task RunOnce_NoKafkaSnapshot_KafkaStoreEmpty()
{
    // Arrange: kafka-домен ещё не тикал (нет снапшота — нет ключей).
    var kafkaStore = new KafkaWorkerHealthStore();
    var (poller, _) = Poller(
        _ => new HttpResponseMessage(HttpStatusCode.OK),
        kafkaReader: new StubKafkaReader(null), kafkaStore: kafkaStore);

    // Act
    await poller.RunOnceAsync(CancellationToken.None);

    // Assert: пустой список (правило worker-unhealthy молчит).
    kafkaStore.Current.Should().BeEmpty();
}
```

Хелперы файла (внутренние):

```csharp
private sealed class StubKafkaReader(AdminPanel.Core.Kafka.KafkaSnapshot? snapshot)
    : AdminPanel.Etcd.IKafkaSnapshotReader
{
    public AdminPanel.Core.Kafka.KafkaSnapshot? Current { get; } = snapshot;
}

private static AdminPanel.Core.Kafka.KafkaSnapshot KafkaSnapshotWith(
    params WorkerEndpoint[] endpoints) => new(
    Now, EtcdReachable: true, ConsecutiveFailures: 0,
    [], [], [], [], [.. endpoints], WorkerHealth: [], Probes: [], Alerts: [],
    ParseErrors: [], UnknownKeyCount: 0);
```

(Количество `[]` сверить с новым конструктором из T4 — позиции после `WorkerEndpoints` идут `WorkerHealth`, затем прежние.)

- [ ] **Step 5.2: Прогнать — красные**

Run: `dotnet test src/tests/AdminPanel.UnitTests --filter "FullyQualifiedName~WorkerHealthPollerTests"`
Expected: Compile ERROR (конструктор поллера ещё без kafka-параметров).

- [ ] **Step 5.3: Реализовать расширение поллера**

`src/AdminPanel.Etcd/Workers/WorkerHealthPoller.cs`:
1. Конструктор — добавить после `IWorkerHealthStore store`:

```csharp
IKafkaSnapshotReader kafkaSnapshotReader,
IKafkaWorkerHealthStore kafkaStore,
```

2. `RunOnceAsync` — после pg-блока (`store.Replace(...)`):

```csharp
// KafkaWorker-инстансы (t09; arch/adminpanel/02 §2.3.2): тот же тик/клиент/
// семантика — 200 → Healthy, 503 → Degraded, сетевой сбой → Unreachable;
// /healthz не под X-Api-Key (ApiKeyMiddleware проверяет только /api).
var kafkaEndpoints = kafkaSnapshotReader.Current?.WorkerEndpoints ?? [];
var kafkaAt = time.GetUtcNow();
var kafkaResults = await Task.WhenAll(kafkaEndpoints.Select(e => ProbeAsync(e, kafkaAt, ct)));
kafkaStore.Replace([.. kafkaResults.OrderBy(r => r.InstanceId, StringComparer.Ordinal)]);
```

- [ ] **Step 5.4: Прогнать тесты + сборка**

Run: `dotnet test src/tests/AdminPanel.UnitTests --filter "FullyQualifiedName~WorkerHealthPollerTests"` → PASS; `dotnet test src/tests/AdminPanel.UnitTests` → PASS; `dotnet build src/AdminPanel.Api/AdminPanel.Api.csproj` → 0 warnings.

- [ ] **Step 5.5: Commit**

```bash
git add src/AdminPanel.Etcd/Workers/WorkerHealthPoller.cs \
  src/tests/AdminPanel.UnitTests/Workers/WorkerHealthPollerTests.cs
git commit -m "feat(adminpanel): WorkerHealthPoller пробит /healthz инстансов KafkaWorker в том же тике (t09 spec §3.4): kafka-эндпоинты из KafkaSnapshot.WorkerEndpoints → IKafkaWorkerHealthStore, тот же клиент/интервал/семантика что pg"
```

---

### Task 6: Панель — правило `worker-unhealthy` в KafkaAlertEngine (spec §3.4)

**Files:**
- Modify: `src/AdminPanel.Core/Kafka/KafkaAlerting/KafkaAlertEngine.cs` (метод `Enumerate` — после блока `worker-api-unreachable`)
- Modify: `src/tests/AdminPanel.UnitTests/KafkaAlertRulesTests.cs`

**Interfaces:**
- Consumes: `next.WorkerHealth` (T4), `WorkerHealthStatus`/`WorkerHealth` (AdminPanel.Core), `Alert`/`AlertSeverity`/`AlertRemedy` (Core.Alerting), `ResolveSince` — уже в движке.
- Produces: kind `worker-unhealthy`, id `worker-unhealthy:kafkaworker/<instanceId>`, target `kafkaworker/<instanceId>` — потребляет `/api/alerts` (объединение pg+kafka уже в `AlertsQueryHandler.Merge`).

- [ ] **Step 6.1: Написать падающие тесты**

В `src/tests/AdminPanel.UnitTests/KafkaAlertRulesTests.cs`: хелпер `Snapshot` — поправить сигнатуру (параметр после `params` в C# невозможен — перегрузка; существующий делегирует с пустым здоровьем):

```csharp
// Перегрузка со здоровьем воркера (t09): params остаётся последним.
private static KafkaSnapshot Snapshot(
    IReadOnlyList<WorkerHealth> workerHealth, params KafkaClusterInfo[] clusters) => new(
    Now, EtcdReachable: true, ConsecutiveFailures: 0,
    [.. clusters], Rotations: [], Rebalances: [], Reassignments: [],
    WorkerEndpoints: [new WorkerEndpoint("kw1", "http://kafkaworker:8080", 1)],
    WorkerHealth: workerHealth, Probes: [], Alerts: [], ParseErrors: [], UnknownKeyCount: 0);

private static KafkaSnapshot Snapshot(params KafkaClusterInfo[] clusters)
    => Snapshot([], clusters);
```

Новые кейсы:

```csharp
[Fact]
public void WorkerUnhealthy_Degraded_WarningKafkaworkerTarget()
{
    // Arrange: живой ключ /kafkaworker/api/kw1, опрос /healthz → 503.
    var next = Snapshot(
        [new WorkerHealth("kw1", "http://kafkaworker:8080", WorkerHealthStatus.Degraded, Now, "HTTP 503")]);

    // Act
    var alerts = Evaluate(next);

    // Assert: warning worker-unhealthy на kafka-таргет (arch/03 §4).
    var a = alerts.Should().ContainSingle(
        x => x.Kind == "worker-unhealthy" && x.Target == "kafkaworker/kw1").Subject;
    a.Severity.Should().Be(AlertSeverity.Warning);
    a.Message.Should().Contain("не-200");
}

[Fact]
public void WorkerUnhealthy_Unreachable_NetworkErrorText()
{
    // Arrange: lease жив, сетевой сбой опроса.
    var next = Snapshot(
        [new WorkerHealth("kw1", "http://kafkaworker:8080", WorkerHealthStatus.Unreachable, Now, "timeout")]);

    // Act/Assert: «недостижим по URL lease-ключа».
    Evaluate(next).Should().ContainSingle(
        x => x.Kind == "worker-unhealthy" && x.Message.Contains("недостижим"));
}

[Fact]
public void WorkerUnhealthy_Healthy_NoAlert()
{
    // Arrange: все инстансы здоровы.
    var next = Snapshot(
        [new WorkerHealth("kw1", "http://kafkaworker:8080", WorkerHealthStatus.Healthy, Now, null)]);

    // Act/Assert
    Evaluate(next).Should().NotContain(x => x.Kind == "worker-unhealthy");
}

[Fact]
public void WorkerUnhealthy_SinceUnix_CarriedFromPrevious()
{
    // Arrange: алерт уже горел в prev с sinceUnix=100 — Alerts вкладываются
    // в PREVIOUS (ResolveSince ищет в previous.Alerts, KafkaAlertEngine:368-374;
    // прецедент ExistingAlert_SinceUnixCarried), next — свежий снапшот.
    var health = new WorkerHealth("kw1", "http://kafkaworker:8080", WorkerHealthStatus.Degraded, Now, "HTTP 503");
    var baseSnap = Snapshot([health]);
    var first = Evaluate(baseSnap).Single(x => x.Kind == "worker-unhealthy");
    var prev = baseSnap with { Alerts = [first with { SinceUnix = 100 }] };
    var next = Snapshot([health]);

    // Act
    var again = Evaluate(next, prev);

    // Assert: sinceUnix перенесён из prev (механика движка, стабильный id).
    again.Single(x => x.Kind == "worker-unhealthy").SinceUnix.Should().Be(100);
}
```

(При несовпадении с точным видом `Snapshot`-хелпера файла — адаптировать под фактический, сохранив суть.)

- [ ] **Step 6.2: Прогнать — красные**

Run: `dotnet test src/tests/AdminPanel.UnitTests --filter "FullyQualifiedName~KafkaAlertRulesTests"`
Expected: FAIL — правило не эмитит `worker-unhealthy`.

- [ ] **Step 6.3: Реализовать правило**

В `src/AdminPanel.Core/Kafka/KafkaAlerting/KafkaAlertEngine.cs`, метод `Enumerate`, сразу после блока `worker-api-unreachable` (перед `foreach (var cluster in next.Clusters)`):

```csharp
// worker-unhealthy (warning, t09; arch/03 §4, arch/adminpanel/02 §2.3.2): живой
// ключ /kafkaworker/api/<id>, но опрос /healthz ≠ 200 — процесс нездоров ДО
// истечения lease (порт WorkerUnhealthyRule pg-грани; docker-health и панель
// видят одно и то же — расхождений больше нет).
foreach (var w in next.WorkerHealth.Where(w => w.Status != WorkerHealthStatus.Healthy))
{
    var what = w.Status == WorkerHealthStatus.Degraded
        ? $"/healthz отвечает не-200 ({w.Detail ?? "degraded"})"
        : $"недостижим по URL lease-ключа ({w.Detail ?? "network error"})";
    yield return new Alert(
        $"worker-unhealthy:kafkaworker/{w.InstanceId}",
        AlertSeverity.Warning,
        "worker-unhealthy",
        $"kafkaworker/{w.InstanceId}",
        $"инстанс KafkaWorker {w.InstanceId} нездоров: {what}",
        new Dictionary<string, string>
        {
            ["url"] = w.Url,
            ["checked_unix"] = w.CheckedAtUtc.ToUnixTimeSeconds().ToString(),
        },
        null,
        "lease-ключ жив, но health-проба процесса плохая: секции /healthz (etcd/docker-хосты/циклы/снапшот) деградированы; docker-healthcheck гасит контейнер — за этим последует исчезновение lease и critical worker-api-unreachable",
        AlertRemedy.OperatorRunbook,
        "смотрите docker logs kafkaworker и /healthz напрямую (секции etcd-reachable/docker-hosts/loops-alive/snapshot); поднимите зависимость (etcd/docker) или перезапустите контейнер воркера");
}
```

- [ ] **Step 6.4: Прогнать тесты + сборка**

Run: `dotnet test src/tests/AdminPanel.UnitTests --filter "FullyQualifiedName~KafkaAlertRulesTests"` → PASS; весь набор `dotnet test src/tests/AdminPanel.UnitTests` → PASS; `dotnet build src/AdminPanel.Api/AdminPanel.Api.csproj` → 0 warnings.

- [ ] **Step 6.5: Commit**

```bash
git add src/AdminPanel.Core/Kafka/KafkaAlerting/KafkaAlertEngine.cs src/tests/AdminPanel.UnitTests/KafkaAlertRulesTests.cs
git commit -m "feat(adminpanel): правило worker-unhealthy в KafkaAlertEngine (t09 spec §3.4): живой ключ /kafkaworker/api/<id> + /healthz ≠ 200 → warning kafkaworker/<id>, sinceUnix по стабильному id; docker-health и панель больше не расходятся"
```

---

### Task 7: Стенд — чек `57-kafka-worker-health.sh` (spec §3.5)

**Files:**
- Create: `dev-stand/adminpanel/checks/57-kafka-worker-health.sh` (+ `chmod +x`)

**Interfaces:**
- Consumes: панель `:5050` (`/api/auth/login`, `/api/alerts` — объединяет pg+kafka алерты), воркер `:8082/healthz` (compose публикует `8082:8080`), сервисы compose: `etcd`, контейнер `as-kafkaworker`; прецедент механики — `checks/30-failover.sh` (login/api/wait_alert).
- Produces: самодостаточный чек (full-профиль + kafka), возвращает стенд в согласованное состояние.

**Порядок событий (семантика spec §3.5, важно для Assert-ов):** пока etcd лежит, kafka-refresher'а успешных тиков нет — `WorkerHealth` снапшота не обновляется (FailTick переносит previous, T4), алерт НЕ горит, хотя поллер уже записал Degraded в стор. После `docker compose start etcd` первый УСПЕШНЫЙ тик refresher'а (≤3 c после готовности etcd) мерджит стор-Degraded → алерт `worker-unhealthy` загорается; затем тик поллера (≤15 c) видит `/healthz` = 200 (воркер восстановился) → стор Healthy → следующий тик refresher'а → алерт гаснет. Худший случай «поллер перезаписал стор до первого снапшота» исключён практически: refresher-тик (3 c) опережает тик поллера (15 c), а `/healthz` воркера в первые секунды после подъёма etcd ещё 503.

- [ ] **Step 7.1: Написать чек целиком**

Создать `dev-stand/adminpanel/checks/57-kafka-worker-health.sh`:

```bash
#!/usr/bin/env bash
# 57-kafka-worker-health.sh (t09; spec §3.5): честность /healthz KafkaWorker и
# единая правда для панели. Transient-стимул — stop etcd ~40 c (≥ 2 тиков
# поллера 15 c): тики/пробы воркера падают при живом процессе. Проверки:
# (1) после подъёма etcd /healthz воркера → 200 БЕЗ рестарта контейнера
#     (сброс sticky-StatusError, живой-Ф7);
# (2) алерт worker-unhealthy:kafkaworker загорается ПЕРВЫМ УСПЕШНЫМ тиком
#     kafka-refresher'а после подъёма etcd (поллер за downtime накопил в стор
#     Degraded; FailTick его не вносит — семантика pg-симметрии) и гаснет
#     ≤ 2 тиков поллера после восстановления;
# (3) worker-api-unreachable не зависает (lease-ключи возвращаются ≤ TTL+тик).
# Профиль: full + kafka (после 55-го, перед 90-down).
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${ADMINPANEL_URL:-http://localhost:5050}"
WORKER_HEALTHZ="${KAFKAWORKER_HEALTHZ:-http://localhost:8082/healthz}"
JAR="$(mktemp)"; trap 'rm -f "$JAR"' EXIT

curl -fsS -c "$JAR" -o /dev/null -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  || { echo "❌ login (панель на $BASE?)"; exit 1; }
api() { curl -fsS -b "$JAR" "$BASE$1"; }

# Предикаты (target'ы разные!): worker-unhealthy → target "kafkaworker/<id>"
# (инстансы); worker-api-unreachable → точный target "kafkaworker" (домен
# целиком, без слэша — KafkaAlertEngine).
has_unhealthy() { api /api/alerts | jq -e 'any(.[]; .kind=="worker-unhealthy" and (.target|startswith("kafkaworker/")))' >/dev/null; }
has_api_down()  { api /api/alerts | jq -e 'any(.[]; .kind=="worker-api-unreachable" and .target=="kafkaworker")' >/dev/null; }

# wait_state <предикат> <present|absent> <label>: поллинг 2 c, бюджет 120 c.
wait_state() {
  local fn="$1" want="$2" label="$3"
  for i in $(seq 1 60); do
    if [ "$want" = present ] && "$fn"; then return 0; fi
    if [ "$want" = absent ] && ! "$fn"; then return 0; fi
    sleep 2
  done
  echo "❌ $label не достигнуто за 120 c ($fn/$want)"; return 1
}
wait_healthz() { for i in $(seq 1 30); do curl -fsS -o /dev/null "$WORKER_HEALTHZ" && return 0; sleep 1; done; echo "❌ $WORKER_HEALTHZ не вернулся в 200 за 30 c"; return 1; }
ect() { docker compose exec -T etcd etcdctl --endpoints=http://localhost:2379 "$@"; }

# Preconditions: lease-ключи живы, воркер здоров, worker-* алертов нет.
[ -n "$(ect get /kafkaworker/api/ --prefix --keys-only </dev/null 2>/dev/null)" ] \
  || { echo "❌ нет живых /kafkaworker/api/ — поднимите стенд (00-up.sh, full+kafka)"; exit 1; }
curl -fsS -o /dev/null "$WORKER_HEALTHZ" || { echo "❌ /healthz воркера не 200 до стимула"; exit 1; }
! has_unhealthy || { echo "❌ уже есть worker-unhealthy — прогоните после чистого 00-up.sh"; exit 1; }
! has_api_down  || { echo "❌ уже есть worker-api-unreachable — стенд не согласован"; exit 1; }
started_before="$(docker inspect -f '{{.State.StartedAt}}' as-kafkaworker)"

# Act 1: transient — etcd лежит ~40 c (поллер успевает 2+ раза записать Degraded
# в стор; снапшот при этом НЕ обновляется — алерта ещё нет, spec §3.5).
echo ">>> docker compose stop etcd (~40 c)"
docker compose stop -t 3 etcd >/dev/null
sleep 40

# Act 2: подъём etcd.
docker compose start etcd >/dev/null

# Assert 1: алерт загорается первым успешным kafka-тиком после подъёма (≤ 120 c).
wait_state has_unhealthy present "алерт worker-unhealthy:kafkaworker" \
  && echo "  алерт worker-unhealthy:kafkaworker загорелся (первый успешный kafka-тик)"

# Assert 2: /healthz → 200 ≤ 30 c, контейнер НЕ рестартован (sticky-сброс).
wait_healthz && echo "  /healthz → 200 после подъёма etcd"
started_after="$(docker inspect -f '{{.State.StartedAt}}' as-kafkaworker)"
[ "$started_before" = "$started_after" ] \
  || { echo "❌ контейнер as-kafkaworker рестартован — сброс sticky-StatusError не доказан"; exit 1; }
echo "  контейнер as-kafkaworker не рестартован (ошибка тика сброшена успешным тиком)"

# Assert 3: алерт гаснет после восстановления (≤ 2 тиков поллера).
wait_state has_unhealthy absent "гашение worker-unhealthy:kafkaworker" \
  && echo "  алерт worker-unhealthy:kafkaworker погас"

# Assert 4: эстафета worker-api-unreachable не зависла (lease-ключи вернулись).
wait_state has_api_down absent "отсутствие worker-api-unreachable" \
  && echo "  worker-api-unreachable не висит (ключи /kafkaworker/api/ восстановлены)"

echo "✅ 57-kafka-worker-health: /healthz честный (последний тик), панель и docker-health согласованы"
```

`chmod +x dev-stand/adminpanel/checks/57-kafka-worker-health.sh`.

Примечание: порядок Assert 1 → Assert 2 сознателен: алерт короткоживущий (загорается первым успешным тиком ~сразу после подъёма etcd, гаснет через ≤ поллер-тик + refresher-тик ≈ 6–20 c) — ловим его первым опросом, `wait_healthz` к этому моменту почти наверняка уже истинен (healthz восстанавливается первым успешным тиком воркера, 5 c + ErrorDelay). Чтение `/api/alerts` валидно на любом шаге — панель жива независимо от etcd.

- [ ] **Step 7.2: Синтаксическая проверка**

Run: `bash -n dev-stand/adminpanel/checks/57-kafka-worker-health.sh`
Expected: нет вывода (синтаксис OK).

- [ ] **Step 7.3: Живой прогон (если Docker доступен; иначе — отметить в отчёте execute)**

Run: `dev-stand/adminpanel/checks/00-up.sh && dev-stand/adminpanel/checks/57-kafka-worker-health.sh`
Expected: `✅ 57-kafka-worker-health: …` (все Assert зелёные; после чека стенд согласован — worker-* алертов нет).

- [ ] **Step 7.4: Commit**

```bash
git add dev-stand/adminpanel/checks/57-kafka-worker-health.sh
git commit -m "test(stand): чек 57-kafka-worker-health (t09 spec §3.5): stop etcd 40 c → start → алерт worker-unhealthy загорается первым успешным kafka-тиком и гаснет ≤ 2 тиков поллера, /healthz 200 без рестарта контейнера (сброс sticky); раздельные предикаты target kafkaworker/<id> и kafkaworker"
```

---

### Task 8: Финальная верификация (spec §6–7)

**Files:**
- без новых файлов; возможны точечные фиксы по итогам.

**Interfaces:**
- Consumes: всё из T1–T7.
- Produces: зелёная верификация против критериев приёмки spec §7.

- [ ] **Step 8.1: Полная сборка solution**

Run: `dotnet build src/PgWorker.slnx`
Expected: 0 errors, 0 warnings.

- [ ] **Step 8.2: Все unit-наборы**

Run: `dotnet test src/tests/KafkaWorker.UnitTests && dotnet test src/tests/AdminPanel.UnitTests`
Expected: PASS; свежие наборы (`LoopsHealthResetTests` — 4 теста вкл. не-лидера, `HealthTests` — 5 тестов вкл. бросающую docker-фабрику, `EtcdConnectCallbackTests`, `KafkaRefresherTests`-кейсы вкл. `Refresh_EtcdFail_PreservesPreviousWorkerHealth`, `WorkerHealthPollerTests`-kafka, `KafkaAlertRulesTests`-worker-unhealthy) зелёные.

- [ ] **Step 8.3: Интеграционные наборы (Docker жив)**

Run: `dotnet test src/tests/KafkaWorker.IntegrationTests` (WAF-компиляция Program.cs с новым handler'ом) и `dotnet test src/tests/AdminPanel.IntegrationTests`
Expected: PASS.

- [ ] **Step 8.4: Полная серия стенд-чеков 00→57 (Docker жив; иначе — отметить в отчёте execute)**

Явный список (glob `[0-9]*.sh` не использовать — захватывает `90-down.sh` и разбирает стенд):

```bash
for c in 00-up 05-seed 10-smoke-api 15-cluster-create 20-alerts 30-failover \
         40-live-probes 50-kafka-api 55-kafka-e2e 57-kafka-worker-health; do
  echo ">>> $c"; "dev-stand/adminpanel/checks/$c.sh" || exit 1
done
```

Expected: все 10 чеков по порядку номеров зелёные; `90-down.sh` в серию НЕ входит — стенд остаётся поднятым; 57 не ломает остальные сценарии (возвращает стенд в согласованное состояние), регрессий 20/30/40/50/55 нет (spec §6, критерий 7.6).

- [ ] **Step 8.5: Сверка с критериями spec §7 (чеклист исполнителя)**

1. Сброс sticky: `LoopsHealthResetTests` зелёные (кр. 1, unit-часть).
2. Структура вместо исключения: `HealthTests` зелёые (кр. 2).
3. DNS: `EtcdConnectCallbackTests` зелёые (кр. 3, unit-часть).
4. Единая правда: `KafkaAlertRulesTests` + `WorkerHealthPollerTests` + `Refresh_EtcdFail_PreservesPreviousWorkerHealth` зелёые; живой прогон чека 57 (кр. 4–5) — если Docker недоступен на шаге, явно пометить в отчёте execute.
5. Канон arch/16 §7, adminpanel/02 §2.3.2, adminpanel/03 §4 — уже обновлён в spec-фазе; сверить, что код им соответствует (никаких новых etcd-ключей, kind только `worker-unhealthy`, FailTick-семантика = перенос из previous).
6. Итоговый git status чистый; лог коммитов T1→T7 на месте.

- [ ] **Step 8.6: Commit (если были фиксы)**

```bash
git add -A && git commit -m "fix(t09): точечные фиксы финальной верификации (build/тесты/чеки зелёные)"   # только при изменениях
```

---

## Замечания для исполнителя

- **Не менять**: `HealthCheckAbstract<T>`, регистрацию чеков в `Program.cs`, etcd-ключи `/kafkaworker/*`, Dockerfile/HEALTHCHECK, compose стенда, фронтенд, `arch/` (обновлён в spec-фазе).
- **Порядок**: T1→T2→T3 любые (независимы); T4 строго раньше T5/T6 (поллер и правило потребляют стор/поле); T7 после всех; T8 последним.
- **Семантика панели (зам. 1 ревью, критично)**: свежие пробы поллера вносятся ТОЛЬКО успешным тиком kafka-refresher'а; `FailTick` переносит `WorkerHealth` из previous (никакого мерджа стора на отказном тике) — алерт `worker-unhealthy` загорается первым успешным тиком после возвращения etcd.
- **Общий прецедент для «порт паттерна»**: комментируй ссылку на источник («живой-Ф7», arch/16 §7) — как в соседнем коде.
- Тесты с реальными сетевыми действиями: только `ConnectAsync_AllAddressesDead_ThrowsLast` (127.0.0.1:1, connection refused — безопасно); e2e-чеки 57 и серия Task 8 требуют Docker.
