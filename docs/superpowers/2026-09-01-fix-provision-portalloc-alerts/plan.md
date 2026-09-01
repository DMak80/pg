# fix-provision-portalloc-alerts — план исполнения

> **Для агентов-исполнителей:** ОБЯЗАТЕЛЬНЫЙ SUB-SKILL: superpowers:subagent-driven-development (рекомендуется) или superpowers:executing-plans — исполнять задачу-за-задачей. Шаги отмечаются чекбоксами (`- [ ]`).

**Цель:** самолечение provision воркера (усыновление фактических портов, сверка портов контейнера, busy из etcd, бэкофф ретраев) и честный алертинг панели (эскалация age, provision-stuck, worker-unhealthy).

**Архитектура:** PgWorker — хозяин кластера: при расхождении portalloc↔факт каноном становится живой канонический контейнер (в PROVISIONING-фазе), а занятость портов = docker-публикации ∪ portalloc-записи всех кластеров; серия фейлов provision живёт в `/pgworker/work/<C>` (бэкофф), панель читает этот журнал и опрашивает `/healthz` воркера.

**Стек:** .NET 10, C# latest, `Nullable=enable`, `TreatWarningsAsErrors=true`, xUnit v3 + FluentAssertions, centralized packages (`Directory.Packages.props`).

**Spec:** `docs/superpowers/2026-09-01-fix-provision-portalloc-alerts/spec.md` (план аргументируется от spec'а; исполнитель читает оба). Контракты arch/ уже обновлены в этом ветвлении (arch/14 §2.4/§3.3/§5 A/§8, arch/adminpanel/02 §2.3.1/§3/§4, arch/adminpanel/03 §4, arch/09 §11, roadmap t90) — код ниже реализует их.

## Глобальные ограничения

- Рабочая директория ВСЕХ команд: `/Users/demakaev/ZCodeProject/worktrees/fix-provision-portalloc-alerts` (worktree-ветка `fix-provision-portalloc-alerts`).
- Сборка: `dotnet build src/PgWorker.slnx` — обязана проходить БЕЗ warnings (`TreatWarningsAsErrors=true`).
- Тесты: `dotnet test src/tests/PgWorker.UnitTests` и `dotnet test src/tests/AdminPanel.UnitTests`; каждый тест — с AAA-комментариями (`// Arrange`, `// Act`, `// Assert`).
- Документация/комментарии — русский; идентификаторы — английские.
- Стенд НЕ трогать: никаких docker/etcd-мутаций из плана; e2e/деплой — вне плана (отдельный приказ; инструкция верификации — Task 11).
- Коммит после каждой задачи: конвейер `fix(pgworker): …` / `feat(panel): …` / `docs(stand): …` с русским описанием (стиль `git log`).
- Тест-инфраструктура готова (не создавать новую): PgWorker — `Fakes.FakeEtcd/FakeDriver/FakeSql` + `Rig` в `ProvisioningProcessTests`, `FakeEngine` в `ClusterDriverTests`, `FakeGateway` в `CoordinationTests`; AdminPanel — `TestSnapshots`, `FixedTimeProvider`, `FakeEtcdGateway` в `SnapshotRefresherTests`, `AlertTestRules.All()`, локальные `Evaluate`-харнессы в `*AlertRulesTests`.

---

### Task 1: WorkJournal — поля серии ретраев (spec §3.5 E1)

**Files:**
- Modify: `src/PgWorker.Etcd/Coordination/WorkJournal.cs`
- Test: `src/tests/PgWorker.UnitTests/Etcd/CoordinationTests.cs` (дописать тесты в существующий класс)
- Test: `src/tests/PgWorker.IntegrationTests/Etcd/EtcdContractTests.cs` (дописать интеграционный тест; прецедент `WorkJournal_RoundTrip_AgainstRealEtcd` :74 — EtcdFixture/EtcdCollection, Testcontainers)

**Interfaces:**
- Consumes: существующий `WorkState`/`WritePhaseAsync(cluster, op, phase, instance, lastError, ct)`.
- Produces: `public sealed record RetrySeries(int FailCount, long FailFirstUnix, long RetryNotBeforeUnix);` в том же файле/namespace `PgWorker.Etcd.Coordination`; `WritePhaseAsync(..., CancellationToken ct, RetrySeries? series = null)` — опциональный параметр ПОСЛЕ `ct` (существующие вызовы не меняются); `WorkState` += JSON-поля `fail_count`/`fail_first_unix`/`retry_not_before_unix` (int?/long?/long?, сериализация null опускается).

- [ ] **Шаг 1: падающие тесты серии (дописать в CoordinationTests, стиль соседних WorkJournal-тестов, :354-395)**

```csharp
[Fact]
public async Task WorkJournal_WritePhase_WithSeries_CarriesRetryFields()
{
    // Arrange: журнал с контекстом серии ретраев.
    var gateway = new FakeGateway();
    var journal = NewJournal(gateway);
    var series = new RetrySeries(FailCount: 3, FailFirstUnix: 1756000000, RetryNotBeforeUnix: 1756000035);

    // Act: запись фазы с переносом серии.
    var result = await journal.WritePhaseAsync(
        "shop", "provision", "waiting-patroni", "inst-1", "boom", CancellationToken.None, series);

    // Assert: round-trip сохраняет серию (фазы прогресса не стирают контекст неудачи).
    result.IsSuccess.Should().BeTrue();
    var state = await journal.ReadAsync("shop", CancellationToken.None);
    state.Value!.FailCount.Should().Be(3);
    state.Value.FailFirstUnix.Should().Be(1756000000);
    state.Value.RetryNotBeforeUnix.Should().Be(1756000035);
}

[Fact]
public async Task WorkJournal_WritePhase_WithoutSeries_OmitsRetryFields()
{
    // Arrange: серия была; успех пишет фазу без контекста (сброс).
    var gateway = new FakeGateway();
    var journal = NewJournal(gateway);
    await journal.WritePhaseAsync("shop", "provision", "failed", "inst-1", "boom", CancellationToken.None,
        new RetrySeries(2, 1756000000, 1756000010));

    // Act: запись Done без серии.
    await journal.WritePhaseAsync("shop", "provision", "done", "inst-1", null, CancellationToken.None);

    // Assert: поля серии отсутствуют в JSON и в модели.
    var raw = gateway.Store["/pgworker/work/shop"].Value;
    raw.Should().NotContain("fail_count");
    var state = await journal.ReadAsync("shop", CancellationToken.None);
    state.Value!.FailCount.Should().BeNull();
}

[Fact]
public async Task WorkJournal_ReadLegacyFormat_RetryFieldsNull()
{
    // Arrange: журнал старого формата (до полей серии) — честный JSON без них.
    var gateway = new FakeGateway();
    gateway.Store["/pgworker/work/old"] = new FakeGateway.Entry(
        """{"op":"provision","phase":"planned","instance":"i","updated_unix":1756000000}""");
    var journal = NewJournal(gateway);

    // Act
    var state = await journal.ReadAsync("old", CancellationToken.None);

    // Assert: обратная совместимость — поля null, чтение не падает.
    state.Value!.FailCount.Should().BeNull();
    state.Value.RetryNotBeforeUnix.Should().BeNull();
}
```

Примечание: посмотреть, как `FakeGateway` в `CoordinationTests` хранит записи (`Store`-словарь с `Entry`), и пользоваться его фактическим API; при несовпадении имён (`Entry`) — подогнать под реальный (это локальный тест-дабл файла).

- [ ] **Шаг 2: прогон — убедиться, что падают**

Run: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~WorkJournal"`
Expected: FAIL — `RetrySeries` не существует (ошибка компиляции теста).

- [ ] **Шаг 3: реализация в WorkJournal.cs**

`WorkState` — дополнить позиционные параметры в конце (после `Unreachable`):

```csharp
public sealed record WorkState(
    [property: JsonPropertyName("op")] string Op,
    [property: JsonPropertyName("phase")] string Phase,
    [property: JsonPropertyName("instance")] string Instance,
    [property: JsonPropertyName("updated_unix")] long UpdatedUnix,
    [property: JsonPropertyName("last_error")] string? LastError,
    [property: JsonPropertyName("unreachable")] IReadOnlyDictionary<string, long>? Unreachable = null,
    [property: JsonPropertyName("fail_count")] int? FailCount = null,
    [property: JsonPropertyName("fail_first_unix")] long? FailFirstUnix = null,
    [property: JsonPropertyName("retry_not_before_unix")] long? RetryNotBeforeUnix = null);

/// <summary>Серия подряд идущих фейлов процесса (бэкофф ретраев, arch/14 §3.3/§5 A):
/// живёт в /pgworker/work/&lt;C&gt;, пишется фейлом, переносится фазами, сбрасывается Done.</summary>
public sealed record RetrySeries(int FailCount, long FailFirstUnix, long RetryNotBeforeUnix);
```

`WritePhaseAsync` — добавить опциональный параметр и прокинуть в payload:

```csharp
public Task<Result> WritePhaseAsync(
    string cluster, string op, string phase, string instance, string? lastError, CancellationToken ct,
    RetrySeries? series = null)
{
    var payload = new WorkState(op, phase, instance, DateTimeOffset.UtcNow.ToUnixTimeSeconds(), lastError,
        Unreachable: null, series?.FailCount, series?.FailFirstUnix, series?.RetryNotBeforeUnix);
    return WithFailoverAsync(endpoint => gateway.PutAsync(
        endpoint, WorkKey(cluster), JsonSerializer.Serialize(payload, Json), lease: null, ct));
}
```

- [ ] **Шаг 4: прогон — зелёные**

Run: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~WorkJournal"`
Expected: PASS (все WorkJournal-тесты, включая существующие).

- [ ] **Шаг 5: интеграционный round-trip серии через реальный etcd (ревью Ф4-2; spec §6)**

Дописать в `EtcdContractTests` (класс `[Collection(EtcdCollection.Name)]`, ctor `(EtcdFixture fixture)`, свойства `Gateway`/`Endpoint` уже есть):

```csharp
[Fact]
public async Task WorkJournal_RetrySeries_RoundTrip_AgainstRealEtcd()
{
    // Arrange — журнал с контекстом серии ретраев (arch/14 §3.3).
    var ct = TestContext.Current.CancellationToken;
    var journal = new WorkJournal(Gateway, [Endpoint]);
    var series = new RetrySeries(FailCount: 3, FailFirstUnix: 1756005400, RetryNotBeforeUnix: 1756009215);

    // Act — фейл с серией, затем фаза прогресса с переносом той же серии.
    var fail = await journal.WritePhaseAsync("shop", "provision", "shard-provision", "inst-1",
        "Patroni шарда shop-shard1 не поднялся за бюджет 600 с", ct, series);
    var carried = await journal.WritePhaseAsync(
        "shop", "provision", "waiting-patroni", "inst-1", null, ct, series);
    var read = await journal.ReadAsync("shop", ct);

    // Assert — поля серии переживают запись/чтение через реальный etcd.
    fail.IsSuccess.Should().BeTrue();
    carried.IsSuccess.Should().BeTrue();
    read.Value!.FailCount.Should().Be(3);
    read.Value.FailFirstUnix.Should().Be(1756005400);
    read.Value.RetryNotBeforeUnix.Should().Be(1756009215);
}
```

Run: `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet test src/tests/PgWorker.IntegrationTests --filter "FullyQualifiedName~EtcdContractTests"`
Expected: PASS (включая существующие тесты класса).

- [ ] **Шаг 6: коммит**

```bash
git add src/PgWorker.Etcd/Coordination/WorkJournal.cs src/tests/PgWorker.UnitTests/Etcd/CoordinationTests.cs src/tests/PgWorker.IntegrationTests/Etcd/EtcdContractTests.cs
git commit -m "feat(pgworker): серия ретраев в /pgworker/work/<C> (fail_count/fail_first_unix/retry_not_before_unix) — перенос фазами, сброс Done, обратная совместимость + интеграционный round-trip на реальном etcd (spec E1, arch/14 §3.3)"
```

---

### Task 2: ProvisioningProcess — бэкофф ретраев provision (spec §3.5 E2, E4)

**Files:**
- Modify: `src/PgWorker.Provisioning/Processes/IClusterProcess.cs:42` (PlacementOptions)
- Modify: `src/PgWorker.Provisioning/Processes/ProvisioningProcess.cs` (TickAsync-вход, Finish/FailAsync/PlannedAsync)
- Modify: `src/PgWorker.App/Options.cs` (ThresholdsOptions), `src/PgWorker.App/Program.cs:142` (проброс), `src/PgWorker.App/appsettings.json` (Thresholds)
- Test: `src/tests/PgWorker.UnitTests/Provisioning/ProvisioningProcessTests.cs` (дописать)

**Interfaces:**
- Consumes: `RetrySeries` из Task 1; `WorkJournal.ReadAsync` (уже есть).
- Produces: `PlacementOptions(int PortFrom, int PortTo, int PatroniBootSec, int ProvisionRetryBaseSec = 5, int ProvisionRetryMaxSec = 60)` — optional-параметры, существующие вызовы `new(15000, 15100, 600)` не меняются.

- [ ] **Шаг 1: падающие тесты бэкоффа (дописать в ProvisioningProcessTests; Rig/NewRig уже есть; для мгновенного бюджет-фейла использовать `PatroniBootSec: -1`)**

```csharp
[Fact]
public async Task Tick_PatroniBudgetFail_WritesRetrySeriesAndBacksOff()
{
    // Arrange: Patroni мёртв на все пробы, бюджет PatroniBootSec=-1 — первый же тик фейлит ожидание.
    var rig = await NewRig(_ => DeadPatroni(), opts: new PlacementOptions(15000, 15100, PatroniBootSec: -1, 5, 60));

    // Act: первый тик.
    var first = await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

    // Assert: фейл с серией fail_count=1 и retry_not_before=now+5; ноды созданы (P2.1 успел).
    first.IsSuccess.Should().BeFalse();
    var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
    work.Value!.LastError.Should().Contain("не поднялся");
    work.Value.FailCount.Should().Be(1);
    work.Value.RetryNotBeforeUnix.Should().BeGreaterThan(work.Value.UpdatedUnix - 1);
    (work.Value.RetryNotBeforeUnix!.Value - work.Value.UpdatedUnix).Should().Be(5);

    // Act: второй тик до истечения retry_not_before — skip (без новых EnsureNode и без перезаписи журнала).
    var driverCalls = rig.Driver.EnsuredNodes.Count;
    var second = await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

    // Assert: InProgress без мутаций; журнал не тронут (фаза/updated_unix прежние).
    second.IsSuccess.Should().BeTrue();
    second.Value.Should().Be(ProcessOutcome.InProgress);
    rig.Driver.EnsuredNodes.Count.Should().Be(driverCalls);
    var after = await rig.Journal.ReadAsync("shop", CancellationToken.None);
    after.Value!.UpdatedUnix.Should().Be(work.Value.UpdatedUnix);
}

[Fact]
public async Task Tick_AfterRetryDeadline_FailsAgainWithIncrementedSeries()
{
    // Arrange: серия из одного фейла; retry_not_before уже в прошлом (подделано в etcd).
    var rig = await NewRig(_ => DeadPatroni(), opts: new PlacementOptions(15000, 15100, PatroniBootSec: -1, 5, 60));
    await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);
    var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
    var prior = work.Value!;
    rig.Etcd.Store["/pgworker/work/shop"] = new Fakes.FakeEtcd.Entry(
        $$"""{"op":"provision","phase":"planning","instance":"{{prior.Instance}}","updated_unix":{{prior.UpdatedUnix - 100}},"last_error":"boom","fail_count":1,"fail_first_unix":{{prior.FailFirstUnix}},"retry_not_before_unix":{{prior.UpdatedUnix - 50}}}""", prior.UpdatedUnix - 100, 2);

    // Act: тик после дедлайна ретрая.
    var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

    // Assert: снова фейл, серия наросла (fail_count=2, delay=base·2).
    outcome.IsSuccess.Should().BeFalse();
    var state = await rig.Journal.ReadAsync("shop", CancellationToken.None);
    state.Value!.FailCount.Should().Be(2);
    (state.Value.RetryNotBeforeUnix!.Value - state.Value.UpdatedUnix).Should().Be(10);
}

[Fact]
public async Task Tick_InProgressPhasesAfterFail_CarrySeriesUntilNextFail()
{
    // Arrange: серия fail_count=1 с истёкшим retry; Patroni мёртв (тик дойдёт до фейла P2.2).
    var rig = await NewRig(_ => DeadPatroni(), opts: new PlacementOptions(15000, 15100, PatroniBootSec: -1, 5, 60));
    await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);
    var failed = await rig.Journal.ReadAsync("shop", CancellationToken.None);
    var f = failed.Value!;
    rig.Etcd.Store["/pgworker/work/shop"] = new Fakes.FakeEtcd.Entry(
        $$"""{"op":"provision","phase":"planning","instance":"{{f.Instance}}","updated_unix":{{f.UpdatedUnix - 100}},"last_error":"boom","fail_count":1,"fail_first_unix":{{f.FailFirstUnix}},"retry_not_before_unix":{{f.UpdatedUnix - 50}}}""",
        f.UpdatedUnix - 100, 2);
    // Сбор КАЖДОЙ записи work-ключа в тике (FakeEtcd.OnPut): фазы тика
    // обязаны нести серию — включая P0 «started» (ревью Ф4-2: без `, series`
    // optional-параметр молча стирает поля — provision-stuck мигает).
    var workWrites = new List<string>();
    rig.Etcd.OnPut = key =>
    {
        if (key == "/pgworker/work/shop")
            workWrites.Add(rig.Etcd.Store[key].Value);
    };

    // Act: тик после дедлайна (FailAsync снова фейлит P2.2).
    await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);
    rig.Etcd.OnPut = null;

    // Assert: fail_first_unix ПЕРЕЖИЛ промежуточные фазы (started/planned) — серия та же, счётчик 2.
    var state = await rig.Journal.ReadAsync("shop", CancellationToken.None);
    state.Value!.FailFirstUnix.Should().Be(f.FailFirstUnix);
    state.Value.FailCount.Should().Be(2);
    // И каждая промежуточная запись тика несла поля серии (started не стирал).
    workWrites.Should().NotBeEmpty();
    workWrites.Should().OnlyContain(v => v.Contains("\"fail_count\":"));
}
```

Для этого `NewRig` получает опциональный параметр `PlacementOptions? opts = null` (дефолт — текущий `Opts`): изменить локальный helper `NewRig(Func<int, HttpResponseMessage> patroniResponse, List<int>? trace = null, PlacementOptions? opts = null)` и передавать `opts ?? Opts` в конструктор процесса.

- [ ] **Шаг 2: прогон — падают**

Run: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~ProvisioningProcessTests"`
Expected: FAIL (нет параметра `opts` у NewRig / серия не пишется: `FailCount` null).

- [ ] **Шаг 3: реализация**

`IClusterProcess.cs:42`:

```csharp
public sealed record PlacementOptions(
    int PortFrom, int PortTo, int PatroniBootSec,
    int ProvisionRetryBaseSec = 5, int ProvisionRetryMaxSec = 60);
```

`ProvisioningProcess.TickAsync` — после guard'а `HasFullDeclaration` (до P0) вставить:

```csharp
// Бэкофф ретраев (spec §3.5 E2): серия фейлов в журнале — до retry_not_before
// тик процесса пропускается (без записи: журнал несёт последний фейл).
var priorWork = await journal.ReadAsync(cluster, ct);
if (!priorWork.IsSuccess)
    return Result<ProcessOutcome>.Failed(priorWork.Error!);
var series = priorWork.Value is { Op: Op, FailCount: > 0, FailFirstUnix: > 0 } pw
    ? new RetrySeries(pw.FailCount!.Value, pw.FailFirstUnix!.Value, pw.RetryNotBeforeUnix ?? 0)
    : null;
if (series is { RetryNotBeforeUnix: > 0 } s
    && s.RetryNotBeforeUnix > DateTimeOffset.UtcNow.ToUnixTimeSeconds())
    return Result<ProcessOutcome>.Success(ProcessOutcome.InProgress);
```

`Finish`/`PlannedAsync`/`FailAsync` — проброс серии (`series` видна в замыкании `TickAsync`? НЕТ — они отдельные методы; передать параметром):

```csharp
private async Task<Result<ProcessOutcome>> Finish(
    string cluster, string phase, ProcessOutcome outcome, CancellationToken ct, RetrySeries? series = null)
{
    var written = await journal.WritePhaseAsync(cluster, Op, phase, claims.InstanceId, null, ct, series);
    ...
}
```

- все существующие вызовы `Finish(...)` внутри TickAsync получают `, series` (фазы прогресса переносят серию), КРОМЕ `Finish(cluster, "done", ProcessOutcome.Done, ct)` — сброс;
- **P0-запись «started» (:70) — тоже с `, series`** (ревью Ф4-2): с optional-параметром пропуск молча компилируется как null и СТИРАЕТ поля серии из work-ключа на время фаз тика (окно «журнал без серии» → provision-stuck мигает; нарушение spec §3.5 E1/§8.7):

```csharp
var started = await journal.WritePhaseAsync(cluster, Op, "started", claims.InstanceId, null, ct, series);
```

- `FailAsync` — вычисляет новую серию (см. ниже), прежний параметр `prior` не нужен — берёт `series` из параметра:

```csharp
private async Task<Result<ProcessOutcome>> FailAsync(
    string cluster, Exception error, string phase, CancellationToken ct, RetrySeries? prior = null)
{
    // Серия подряд идущих фейлов (без разбора текста — простота; spec §8.8):
    // новая ошибка после успеха начинает серию заново (series=null после Done).
    var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
    var n = prior is null ? 1 : prior.FailCount + 1;
    var shift = Math.Min(n - 1, 20);
    var delay = Math.Min(placementOpts.ProvisionRetryBaseSec * (1L << shift), placementOpts.ProvisionRetryMaxSec);
    var next = new RetrySeries(n, prior?.FailFirstUnix ?? now, now + delay);
    await journal.WritePhaseAsync(cluster, Op, phase, claims.InstanceId, error.Message, ct, next);
    return Result<ProcessOutcome>.Failed(error);
}
```

- все вызовы `FailAsync(...)` в TickAsync получают `, series`; `PlannedAsync` — аналогично `Finish` (параметр `RetrySeries? series = null`, передаётся в WritePhaseAsync).

`Options.cs` (ThresholdsOptions) += два свойства:

```csharp
/// <summary>Бэкофф ретраев provision (arch/14 §5 A): база задержки (n-й фейл подряд → Base·2^(n−1)).</summary>
public int ProvisionRetryBaseSec { get; set; } = 5;

/// <summary>Кап задержки бэкоффа provision (spec §3.5 E4).</summary>
public int ProvisionRetryMaxSec { get; set; } = 60;
```

`Program.cs:142` — проброс: `new PlacementOptions(opts.Docker.PortRange.From, opts.Docker.PortRange.To, opts.Thresholds.PatroniBootSec, opts.Thresholds.ProvisionRetryBaseSec, opts.Thresholds.ProvisionRetryMaxSec)`.

`appsettings.json` — секция `Thresholds` += `"ProvisionRetryBaseSec": 5, "ProvisionRetryMaxSec": 60`.

- [ ] **Шаг 4: прогон — зелёные**

Run: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~ProvisioningProcessTests"`
Expected: PASS — включая существующие (3 новых + все прежние).

- [ ] **Шаг 5: коммит**

```bash
git add src/PgWorker.Provisioning/Processes/IClusterProcess.cs src/PgWorker.Provisioning/Processes/ProvisioningProcess.cs src/PgWorker.App/Options.cs src/PgWorker.App/Program.cs src/PgWorker.App/appsettings.json src/tests/PgWorker.UnitTests/Provisioning/ProvisioningProcessTests.cs
git commit -m "feat(pgworker): бэкофф ретраев provision — skip тика до retry_not_before, счётчик серии в журнале (Base·2^n, кап 60 c), перенос фазами прогресса/сброс Done (spec E2/E4, arch/14 §5 A)"
```

---

### Task 3: Сброс трекера бюджета Patroni при фейле (spec §3.5 E3)

**Files:**
- Modify: `src/PgWorker.Provisioning/Processes/ProvisioningProcess.cs:320-328` (WaitPatroniAsync)
- Modify: `src/PgWorker.Provisioning/Processes/AddShardProcess.cs:283-291` (WaitPatroniAsync)
- Test: `src/tests/PgWorker.UnitTests/Provisioning/ProvisioningProcessTests.cs`

**Interfaces:**
- Consumes: поле `_patroniWaitSince` (ConcurrentDictionary) в обоих процессах.
- Produces: поведение «бюджет-фейл сбрасывает трекер scope'а» — новая попытка получает полный бюджет.

- [ ] **Шаг 1: падающий тест (наблюдение через рефлексию приватного поля — трекер внутренний)**

```csharp
[Fact]
public async Task Tick_PatroniBudgetFail_ResetsWaitTrackerForNextAttempt()
{
    // Arrange: мгновенный бюджет (-1) — первый же тик фейлит ожидание Patroni.
    var rig = await NewRig(_ => DeadPatroni(), opts: new PlacementOptions(15000, 15100, PatroniBootSec: -1));

    // Act
    await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

    // Assert: трекер бюджета очищен — следующая попытка получит полный бюджет,
    // а не мгновенный фейл от протухшего «первого наблюдения» (234 фейла/10 мин на стенде).
    var field = typeof(ProvisioningProcess).GetField("_patroniWaitSince",
        System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
    var tracker = (System.Collections.Concurrent.ConcurrentDictionary<string, long>)field.GetValue(rig.Process)!;
    tracker.Should().BeEmpty();
}
```

- [ ] **Шаг 2: прогон — падает**

Run: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~ResetsWaitTracker"`
Expected: FAIL — трекер содержит `shop-shard1` (запись не удаляется при фейле).

- [ ] **Шаг 3: реализация — в обоих процессах одинаковая правка**

`ProvisioningProcess.WaitPatroniAsync` (блок бюджет-фейла):

```csharp
if (now - since > placementOpts.PatroniBootSec)
{
    // Бюджет исчерпан: сброс трекера — новая попытка (после бэкоффа) получает
    // полный бюджет заново; иначе каждый следующий тик фейлился мгновенно (E3).
    _patroniWaitSince.TryRemove(scope, out _);
    return Result<bool>.Failed(new ApplicationException(
        $"Patroni шарда {scope} не поднялся за бюджет {placementOpts.PatroniBootSec} с"));
}
```

`AddShardProcess.WaitPatroniAsync` (:287-291) — идентичная вставка `TryRemove` перед `return Result<bool>.Failed(...)`.

- [ ] **Шаг 4: прогон — зелёный**

Run: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~ProvisioningProcessTests|FullyQualifiedName~AddShardProcessTests"`
Expected: PASS.

- [ ] **Шаг 5: коммит**

```bash
git add src/PgWorker.Provisioning/Processes/ProvisioningProcess.cs src/PgWorker.Provisioning/Processes/AddShardProcess.cs src/tests/PgWorker.UnitTests/Provisioning/ProvisioningProcessTests.cs
git commit -m "fix(pgworker): сброс трекера бюджета Patroni при фейле — новая попытка получает полный бюджет (мгновенные повторные фейлы после первого таймаута, spec E3)"
```

---

### Task 4: PortAllocIndex — занятость портов из etcd соседей (spec §3.3, пункт C)

**Files:**
- Create: `src/PgWorker.Provisioning/Endpoints/PortAllocIndex.cs`
- Test: Create: `src/tests/PgWorker.UnitTests/Provisioning/PortAllocIndexTests.cs`

**Interfaces:**
- Consumes: `IEtcdGateway.RangeAsync(endpoint, prefix, ct)` (failover-перебор — паттерн `ProvisioningProcess.WithFailoverAsync`), `Portalloc.Parse(cluster, raw)` из `PgWorker.Core.Model`.
- Produces: `public sealed class PortAllocIndex(IEtcdGateway etcd, string[] endpoints, ILogger<PortAllocIndex> logger)` c методом `public Task<Result<IReadOnlySet<(string Host, int Port)>>> ReadBusyAsync(string exceptCluster, CancellationToken ct)` — все три порта каждой записи каждого ЧУЖОГО `/pgworker/portalloc/<C>`; битый JSON ключа — Warning-лог + skip ключа.

- [ ] **Шаг 1: падающие тесты**

```csharp
using Microsoft.Extensions.Logging.Abstractions;
using PgWorker.Core;
using PgWorker.Etcd.Client;
using PgWorker.Provisioning.Endpoints;

namespace PgWorker.UnitTests.Provisioning;

// PortAllocIndex (spec §3.3): busy-множество из portalloc-записей ВСЕХ кластеров,
// кроме своего; битые JSON соседей скипаются без ронирования результата.

public class PortAllocIndexTests
{
    private const string Ep = "http://etcd:2379";

    [Fact]
    public async Task ReadBusy_MixesAllNeighborsExceptOwn()
    {
        // Arrange: portalloc двух кластеров; свой (shop) исключается.
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/pgworker/portalloc/shop",
            """{"shard1/shard1a":{"host":"h1","pg":15000,"patroni":18000,"doorman":16500}}""");
        etcd.Seed("/pgworker/portalloc/canon10",
            """{"shard1/shard1a":{"host":"h1","pg":15004,"patroni":18004,"doorman":16504},
                "shard1/shard1b":{"host":"h2","pg":15005,"patroni":18005,"doorman":16505}}""");
        var index = new PortAllocIndex(etcd, [Ep], NullLogger<PortAllocIndex>.Instance);

        // Act
        var busy = await index.ReadBusyAsync("shop", CancellationToken.None);

        // Assert: только чужая тройка×2 ноды; своих портов нет.
        busy.IsSuccess.Should().BeTrue();
        busy.Value.Should().BeEquivalentTo(
            new (string, int)[] { ("h1", 15004), ("h1", 18004), ("h1", 16504),
                                  ("h2", 15005), ("h2", 18005), ("h2", 16505) });
    }

    [Fact]
    public async Task ReadBusy_MalformedNeighborKey_SkippedNotFailed()
    {
        // Arrange: сосед с битым JSON + валидный сосед.
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/pgworker/portalloc/broken", "{не-json}");
        etcd.Seed("/pgworker/portalloc/good",
            """{"s1/n1":{"host":"h1","pg":15010,"patroni":18010,"doorman":16510}}""");
        var index = new PortAllocIndex(etcd, [Ep], NullLogger<PortAllocIndex>.Instance);

        // Act
        var busy = await index.ReadBusyAsync("shop", CancellationToken.None);

        // Assert: валидный ключ учтён, битый — молча пропущен (лог), Result успешен.
        busy.IsSuccess.Should().BeTrue();
        busy.Value.Should().BeEquivalentTo(new (string, int)[] { ("h1", 15010), ("h1", 18010), ("h1", 16510) });
    }

    [Fact]
    public async Task ReadBusy_ObjectNodeZeroDoorman_NotAdded()
    {
        // Arrange: усыновлённая нода с doorman=0 (внешний контейнер без биндинга).
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/pgworker/portalloc/ext",
            """{"s1/n1":{"host":"h1","pg":15020,"patroni":0,"doorman":0,"object":"foreign"}}""");
        var index = new PortAllocIndex(etcd, [Ep], NullLogger<PortAllocIndex>.Instance);

        // Act
        var busy = await index.ReadBusyAsync("shop", CancellationToken.None);

        // Assert: нулевые порты в занятость не попадают.
        busy.Value.Should().BeEquivalentTo(new (string, int)[] { ("h1", 15020) });
    }
}
```

(`Fakes.FakeEtcd.Seed` — существующий helper, используется в `ProvisioningProcessTests.SeedCluster`.)

Плюс дубль-страховка контракта C на уровне аллокатора (spec §6) — дописать в существующий `Planning/PortAllocatorTests.cs`:

```csharp
[Fact]
public void Allocate_PinnedPortInBusyWithoutExisting_AllocatesNext()
{
    // Arrange: busy-union (docker ∪ portalloc соседей) занял 15000-тройку; existing пуст.
    var plan = new PlacementPlan([new("shard1", "shard1a", "h1")]);
    var busy = new HashSet<(string, int)> { ("h1", 15000), ("h1", 18000), ("h1", 16500) };

    // Act
    var result = PortAllocator.Allocate(plan, new Dictionary<string, NodeAddress>(), busy, 15000, 16000);

    // Assert: база сдвинута — соседская тройка не переиспользуется.
    result.Value["shard1/shard1a"].Ports.Pg.Should().Be(15001);
}
```

- [ ] **Шаг 2: прогон — падает**

Run: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~PortAllocIndexTests"`
Expected: FAIL — тип `PortAllocIndex` не существует.

- [ ] **Шаг 3: реализация**

```csharp
using Microsoft.Extensions.Logging;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Etcd.Client;

namespace PgWorker.Provisioning.Endpoints;

/// <summary>
/// Индекс занятости портов из etcd (spec §3.3): busy = docker-публикации ∪ записи
/// portalloc ВСЕХ кластеров. Свои записи исключает вызывающий (exceptCluster) —
/// свой portalloc переиспользуется аллокатором как закрепление, а не занятость.
/// Битый JSON соседа — Warning-лог + skip ключа: чужой мусор не роняет наш provision.
/// </summary>
public sealed class PortAllocIndex(
    IEtcdGateway etcd, string[] endpoints, ILogger<PortAllocIndex> logger)
{
    private const string Prefix = "/pgworker/portalloc/";

    public async Task<Result<IReadOnlySet<(string Host, int Port)>>> ReadBusyAsync(
        string exceptCluster, CancellationToken ct)
    {
        var range = await WithFailoverAsync(endpoint => etcd.RangeAsync(endpoint, Prefix, ct));
        if (!range.IsSuccess)
            return Result<IReadOnlySet<(string Host, int Port)>>.Failed(range.Error!);

        var busy = new HashSet<(string, int)>();
        foreach (var kv in range.Value)
        {
            var cluster = kv.Key.Split('/')[^1];
            if (cluster == exceptCluster)
                continue;

            var parsed = Portalloc.Parse(cluster, kv.Value);
            if (!parsed.IsSuccess)
            {
                // Не наш ключ — не наша ответственность: лог + skip (spec §2.3-принцип).
                logger.LogWarning("битый portalloc соседа {Cluster}: {Error}", cluster, parsed.Error!.Message);
                continue;
            }

            foreach (var addr in parsed.Value.Values)
            {
                busy.Add((addr.Host, addr.Ports.Pg));
                if (addr.Ports.Patroni > 0)
                    busy.Add((addr.Host, addr.Ports.Patroni));
                if (addr.Ports.Doorman > 0)
                    busy.Add((addr.Host, addr.Ports.Doorman));
            }
        }

        return Result<IReadOnlySet<(string Host, int Port)>>.Success(busy);
    }

    // Failover-обёртка: первый успешный endpoint выигрывает (паттерн процессов).
    private async Task<Result<T>> WithFailoverAsync<T>(Func<string, Task<Result<T>>> call)
    {
        Result<T>? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await call(endpoint);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }
}
```

- [ ] **Шаг 4: прогон — зелёные**

Run: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~PortAllocIndexTests"`
Expected: PASS (3 теста).

- [ ] **Шаг 5: коммит**

```bash
git add src/PgWorker.Provisioning/Endpoints/PortAllocIndex.cs src/tests/PgWorker.UnitTests/Provisioning/PortAllocIndexTests.cs
git commit -m "feat(pgworker): PortAllocIndex — busy из portalloc всех кластеров etcd (кроме своего), битые соседние ключи skip+лог; закрывает последовательную кросс-кластерную коллизию портов (spec C, arch/14 §2.4; в t90 остаётся только параллельная гонка)"
```

---

### Task 5: PlanPortsAsync — усыновление фактических портов + busy-union (spec §3.1, пункт A + C в provision)

**Files:**
- Modify: `src/PgWorker.Provisioning/Processes/ProvisioningProcess.cs` (ctor += `PortAllocIndex portAlloc`; `PlanPortsAsync` — переписать; `PlannedAsync` — skipped-имена)
- Modify: `src/PgWorker.App/Program.cs:137-142` (регистрация `AddSingleton(portAllocIndex)` + передача в ProvisioningProcess)
- Test: `src/tests/PgWorker.UnitTests/Provisioning/ProvisioningProcessTests.cs` (дописать; `Fakes.FakeDriver.InspectResult` уже существует)

**Interfaces:**
- Consumes: `PortAllocIndex.ReadBusyAsync` (Task 4); `IClusterDriver.InspectNodesAsync` → `DiscoveredNode(NodeName, Host, Object, Pg, Patroni, Doorman)`; `NodeAddress(string Host, NodePorts Ports, string? Object = null)`.
- Produces: ctor `ProvisioningProcess(..., PortAllocIndex portAlloc, ...)` — новый параметр после `ISqlExecutor db` (или в конец перед optional `snapshot` — выбрать «в конец перед `snapshot`»); поведение `PlanPortsAsync` по spec §3.1 (шаги 1–7): усыновление выполняется КАЖДЫЙ тик provision — в том числе для ПОЛНОГО portalloc (расхождение полно(portalloc)↔факт — ровно состояние стенда canon10/smoke; ревью Ф4-1); ранний выход без записи только при «wanted ⊆ existing && merge ничего не изменил».

- [ ] **Шаг 1: падающие тесты усыновления (FakeDriver.InspectResult настраивается картой находок)**

```csharp
private static DiscoveredNode Node(string host, string obj, int pg, int patroni, int doorman = 16500)
    => new("ignored", host, obj, pg, patroni, doorman);

[Fact]
public async Task Tick_LostPortalloc_AdoptsActualContainerPorts()
{
    // Arrange: portalloc ПОТЕРЯН (ключа нет), но живые контейнеры есть — инспекция
    // возвращает фактические порты канонических объектов (сценарий стенда canon10).
    var rig = await NewRig(_ => Patroni("shard1a"));
    rig.Driver.InspectResult = new Dictionary<string, DiscoveredNode>
    {
        ["shard1a"] = Node("h1", "pgw-shop-shard1-shard1a", 15004, 18004),
        ["shard1b"] = Node("h2", "pgw-shop-shard1-shard1b", 15005, 18005),
        ["shard2a"] = Node("h1", "pgw-shop-shard2-shard2a", 15006, 18006),
        ["shard2b"] = Node("h2", "pgw-shop-shard2-shard2b", 15007, 18007),
    };

    // Act
    await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

    // Assert: portalloc записан ФАКТОМ контейнеров (не свежей аллокацией 15000+), без object.
    var raw = rig.Etcd.Store["/pgworker/portalloc/shop"].Value;
    raw.Should().Contain("\"shard1/shard1a\":{\"host\":\"h1\",\"pg\":15004,\"patroni\":18004,\"doorman\":16500}");
    raw.Should().NotContain("\"object\"");
}

[Fact]
public async Task Tick_DivergedPortalloc_RewritesRecordWithActualFact()
{
    // Arrange: записи ЕСТЬ, но битые (15014+ — свежая аллокация после потери);
    // факт контейнеров — 15004..15007. Расхождение → каноном становится факт.
    var rig = await NewRig(_ => Patroni("shard1a"));
    rig.Etcd.Seed("/pgworker/portalloc/shop",
        """{"shard1/shard1a":{"host":"h1","pg":15014,"patroni":18014,"doorman":16514},
            "shard1/shard1b":{"host":"h2","pg":15015,"patroni":18015,"doorman":16515},
            "shard2/shard2a":{"host":"h1","pg":15016,"patroni":18016,"doorman":16516},
            "shard2/shard2b":{"host":"h2","pg":15017,"patroni":18017,"doorman":16517}}""");
    rig.Driver.InspectResult = new Dictionary<string, DiscoveredNode>
    {
        ["shard1a"] = Node("h1", "pgw-shop-shard1-shard1a", 15004, 18004),
        ["shard1b"] = Node("h2", "pgw-shop-shard1-shard1b", 15005, 18005),
        ["shard2a"] = Node("h1", "pgw-shop-shard2-shard2a", 15006, 18006),
        ["shard2b"] = Node("h2", "pgw-shop-shard2-shard2b", 15007, 18007),
    };

    // Act
    await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

    // Assert: запись перезаписана фактом (merge «только отсутствующие» дефект не чинит — spec §8.1).
    var raw = rig.Etcd.Store["/pgworker/portalloc/shop"].Value;
    raw.Should().Contain("\"pg\":15004");
    raw.Should().NotContain("\"pg\":15014");
}

[Fact]
public async Task Tick_PartialPortalloc_MergesFactKeepsObjectRecord_WritesByPut()
{
    // Arrange: ЧАСТИЧНЫЙ portalloc (2 записи из 4): shard1a расходится (без
    // object), shard1b — object-запись (усыновлённая ранее); инспекция видит
    // все 4 канонических контейнера. app-секрет уже обеспечен (P1.5 без txn);
    // Patroni мёртв — тик останавливается на waiting-patroni (P3/P4- txn не
    // достигаются — иначе дельта txn-ассерта ниже была бы не нулевой).
    var rig = await NewRig(_ => DeadPatroni());
    rig.Etcd.Seed("/clusters/shop/app_user", "app");
    rig.Etcd.Seed("/clusters/shop/app_password", "pw");
    rig.Etcd.Seed("/pgworker/portalloc/shop",
        """{"shard1/shard1a":{"host":"h1","pg":15014,"patroni":18014,"doorman":16514},
            "shard1/shard1b":{"host":"h2","pg":15015,"patroni":18015,"doorman":16515,"object":"external-1"}}""");
    rig.Driver.InspectResult = new Dictionary<string, DiscoveredNode>
    {
        ["shard1a"] = Node("h1", "pgw-shop-shard1-shard1a", 15004, 18004),
        ["shard1b"] = Node("h2", "pgw-shop-shard1-shard1b", 15005, 18005),
        ["shard2a"] = Node("h1", "pgw-shop-shard2-shard2a", 15006, 18006),
        ["shard2b"] = Node("h2", "pgw-shop-shard2-shard2b", 15007, 18007),
    };
    var txnsBefore = rig.Etcd.Txns.Count; // клэйм NewRig уже записал свои txn

    // Act
    var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

    // Assert: merge — расходящаяся запись перезаписана фактом; object-запись
    // НЕ тронута (порты и object на месте); недобор добран из факта (spec §6).
    outcome.IsSuccess.Should().BeTrue();
    var raw = rig.Etcd.Store["/pgworker/portalloc/shop"].Value;
    raw.Should().Contain("\"shard1/shard1a\":{\"host\":\"h1\",\"pg\":15004");
    raw.Should().Contain("\"shard1/shard1b\":{\"host\":\"h2\",\"pg\":15015");
    raw.Should().Contain("\"object\":\"external-1\"");
    raw.Should().Contain("\"shard2/shard2a\":{\"host\":\"h1\",\"pg\":15006");
    raw.Should().Contain("\"shard2/shard2b\":{\"host\":\"h2\",\"pg\":15007");
    // Ключ существовал → запись put'ом, НЕ txn (ревью Ф4-1): новых txn в тике нет.
    rig.Etcd.Txns.Count.Should().Be(txnsBefore);
}

[Fact]
public async Task Tick_ForeignObjectMatch_SkippedAndAllocatedNormally()
{
    // Arrange: инспекция нашла контейнер с НЕканоническим именем объекта — не наша находка.
    var rig = await NewRig(_ => DeadPatroni());
    rig.Driver.InspectResult = new Dictionary<string, DiscoveredNode>
    {
        ["shard1a"] = Node("h1", "weird-container", 15004, 18004),
    };

    // Act
    await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

    // Assert: shard1a НЕ усыновлён — обычная аллокация (первая свободная база 15000).
    // Фазовая заметка "planned, adopt-skipped: ..." эфемерна (журнал — одна фаза
    // на тик; waiting-patroni допишется следом) — стойкое свидетельство пропуска
    // это сам portalloc: нода на свежей аллокации, расхождение с «чужим фактом»
    // добьёт EnsureNode (Task 7).
    var raw = rig.Etcd.Store["/pgworker/portalloc/shop"].Value;
    raw.Should().Contain("\"shard1/shard1a\":{\"host\":\"h1\",\"pg\":15000");
}

[Fact]
public async Task Tick_FullPortallocMatchingFact_NotRewritten()
{
    // Arrange: полный portalloc, СОВПАДАЮЩИЙ с фактом контейнеров (инспекция
    // выполняется всегда — ревью Ф4-1; сходящийся кластер).
    var rig = await NewRig(_ => Patroni("shard1a"));
    rig.Etcd.Seed("/pgworker/portalloc/shop",
        """{"shard1/shard1a":{"host":"h1","pg":15000,"patroni":18000,"doorman":16500},
            "shard1/shard1b":{"host":"h2","pg":15001,"patroni":18001,"doorman":16501},
            "shard2/shard2a":{"host":"h1","pg":15002,"patroni":18002,"doorman":16502},
            "shard2/shard2b":{"host":"h2","pg":15003,"patroni":18003,"doorman":16503}}""");
    rig.Driver.InspectResult = new Dictionary<string, DiscoveredNode>
    {
        ["shard1a"] = Node("h1", "pgw-shop-shard1-shard1a", 15000, 18000),
        ["shard1b"] = Node("h2", "pgw-shop-shard1-shard1b", 15001, 18001),
        ["shard2a"] = Node("h1", "pgw-shop-shard2-shard2a", 15002, 18002),
        ["shard2b"] = Node("h2", "pgw-shop-shard2-shard2b", 15003, 18003),
    };

    // Act
    await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

    // Assert: запись НЕ перезаписана — version ключа не выросла (ранний выход
    // шага 4 spec §3.1: полный portalloc + merge ничего не изменил).
    var entry = rig.Etcd.Store["/pgworker/portalloc/shop"];
    entry.Version.Should().Be(1);
    entry.Value.Should().Contain("\"pg\":15000");
}
```

Плюс тест коллизии (пункт C через процесс): сосед canon10 закрепил 15004-15007 → свежий provision shop (без контейнеров, `InspectResult` пуст) получает 15008+:

```csharp
[Fact]
public async Task Tick_FreshCluster_AvoidsForeignPortallocRecords()
{
    // Arrange: сосед закрепил 15000-15003 (все 3 порта); наш кластер без контейнеров.
    var rig = await NewRig(_ => Patroni("shard1a"));
    rig.Etcd.Seed("/pgworker/portalloc/canon10",
        """{"s1/n1":{"host":"h1","pg":15000,"patroni":18000,"doorman":16500}}""");

    // Act
    await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

    // Assert: первая нода h1 ушла от занятой тройки соседа (15001 занята doorman=18001? нет —
    // тройка ноды canon10 = 15000/18000/16500; база 15001 даёт 15001/18001/16501 — свободно).
    var raw = rig.Etcd.Store["/pgworker/portalloc/shop"].Value;
    raw.Should().Contain("\"shard1/shard1a\":{\"host\":\"h1\",\"pg\":15001");
}
```

(Rig дорабатывается: `NewRig` конструирует `new PortAllocIndex(etcd, [Ep], NullLogger<PortAllocIndex>.Instance)` и передаёт в процесс — компилятор покажет место.)

- [ ] **Шаг 2: прогон — падают**

Run: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~ProvisioningProcessTests"`
Expected: FAIL — ctor без `PortAllocIndex`/merge не реализован.

- [ ] **Шаг 3: реализация PlanPortsAsync (полностью новая версия метода)**

```csharp
// P1: усыновление факта → busy (docker ∪ чужие portalloc) → аллокация недобора →
// закрепление /pgworker/portalloc/<C> (spec §3.1). Факт над записью: живой
// канонический контейнер — канон записи; нода без контейнера — обычная аллокация.
private async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> PlanPortsAsync(
    ClusterSnapshot snap, RetrySeries? series, CancellationToken ct)
{
    var cluster = snap.Config.Cluster;
    var pinned = await ReadPortAllocAsync(cluster, ct);
    if (!pinned.IsSuccess)
        return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(pinned.Error!);
    var existing = new Dictionary<string, NodeAddress>(pinned.Value);

    var wanted = snap.Shards.SelectMany(s => s.Nodes.Select(n => $"{s.Name}/{n.Name}")).ToList();

    // Усыновление факта — КАЖДЫЙ тик provision (ревью Ф4-1, spec §3.1 шаг 3):
    // расхождение нельзя узнать без инспекции, а ПОЛНЫЙ portalloc может быть
    // расходящимся (потерян и выделен заново — состояние стенда canon10/smoke).
    var adopted = await AdoptRunningContainersAsync(cluster, snap, existing, ct);
    if (!adopted.IsSuccess)
        return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(adopted.Error!);
    var skipped = adopted.Value.Skipped;

    // Ранний выход (идемпотентность, spec §3.1 шаг 4): всё закреплено и merge
    // ничего не изменил — записи portalloc нет.
    if (wanted.All(existing.ContainsKey) && !adopted.Value.Changed)
        return await PlannedAsync(existing, cluster, ct, series, skipped);

    if (wanted.All(existing.ContainsKey))
    {
        var commit = await CommitPortAllocAsync(cluster, existing, pinned.Value.Count > 0, ct);
        if (!commit.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(commit.Error!);
        return await PlannedAsync(existing, cluster, ct, series, skipped);
    }

    var hosts = await driver.GetHostsAsync(ct);
    if (!hosts.IsSuccess)
        return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(hosts.Error!);
    var dockerBusy = await driver.GetBusyPortsAsync(ct);
    if (!dockerBusy.IsSuccess)
        return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(dockerBusy.Error!);
    var foreignBusy = await portAlloc.ReadBusyAsync(cluster, ct);
    if (!foreignBusy.IsSuccess)
        return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(foreignBusy.Error!);
    var busy = new HashSet<(string, int)>(foreignBusy.Value);
    foreach (var p in dockerBusy.Value)
        busy.Add(p);

    var plan = PlacementPlanner.Plan(snap.Shards, hosts.Value);
    var allocated = PortAllocator.Allocate(plan, existing, busy, placementOpts.PortFrom, placementOpts.PortTo);
    if (!allocated.IsSuccess)
        return allocated;

    foreach (var (merged, addr) in allocated.Value)
        existing[merged] = addr;

    var commitAll = await CommitPortAllocAsync(cluster, existing, pinned.Value.Count > 0, ct);
    if (!commitAll.IsSuccess)
        return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(commitAll.Error!);

    return await PlannedAsync(existing, cluster, ct, series, skipped);
}

// Результат усыновления: имена пропущенных находок (journal-заметка) и признак
// «merge изменил existing» (для раннего выхода без записи).
private sealed record Adoption(IReadOnlyList<string> Skipped, bool Changed);

// Инспекция живых канонических контейнеров: фактические public-порты становятся
// каноном записей — добавление отсутствующих, перезапись при расхождении
// (только записей без object), совпадение — не пишем (NodeAddress/NodePorts —
// record, Equals по значению).
private async Task<Result<Adoption>> AdoptRunningContainersAsync(
    string cluster, ClusterSnapshot snap, Dictionary<string, NodeAddress> existing, CancellationToken ct)
{
    var byNode = snap.Shards
        .SelectMany(s => s.Nodes.Select(n => (Key: $"{s.Name}/{n.Name}", Name: n.Name)))
        .ToList();
    var discovered = await driver.InspectNodesAsync(byNode.Select(p => p.Name).Distinct().ToList(), ct);
    if (!discovered.IsSuccess)
        return Result<Adoption>.Failed(discovered.Error!);

    var skipped = new List<string>();
    var changed = false;
    foreach (var (key, nodeName) in byNode)
    {
        if (!discovered.Value.TryGetValue(nodeName, out var node))
            continue; // контейнера нет — аллокация недобором
        var canonicalObject = $"pgw-{cluster}-{key.Replace('/', '-')}";
        if (node.Object != canonicalObject || node.Pg <= 0 || node.Patroni <= 0)
        {
            skipped.Add(key); // чужой/частичная публикация — не наша находка (spec §3.1 guard'ы)
            continue;
        }

        var fact = new NodeAddress(node.Host, new NodePorts(node.Pg, node.Patroni, node.Doorman));
        if (existing.TryGetValue(key, out var current))
        {
            if (current.Object is not null)
                continue; // object-запись (усыновлённая ранее) не перезаписываем
            if (current.Equals(fact))
                continue; // совпадение записи с фактом — не пишем (идемпотентность)
        }

        existing[key] = fact;
        changed = true;
    }

    return Result<Adoption>.Success(new Adoption(skipped, changed));
}

// Закрепление: первый ключ — txn NotExists (конкурент создал → берём его перезаписью
// merge под клэймом); существующий — put (read-modify-write, паттерн AddShard A2).
private async Task<Result> CommitPortAllocAsync(
    string cluster, IReadOnlyDictionary<string, NodeAddress> addresses, bool keyExisted, CancellationToken ct)
{
    var key = PortAllocKey(cluster);
    var value = SerializePortAlloc(addresses);
    if (keyExisted)
    {
        var put = await PutAsync(key, value, ct);
        return put;
    }

    var txn = await TxnAsync(
        TxnRequest.Of(
            [TxnCompare.NotExists(key)],
            [new TxnOp.Put(key, value, null)]),
        ct);
    if (!txn.IsSuccess)
        return txn;
    if (txn.Value.Succeeded)
        return Result.Success();

    // Проигрыш txn (ключ появился после чтения) — канон другой инстанс уже записал:
    // под клэймом безопасно перезаписать нашим merge (факт свежего чтения).
    return await PutAsync(key, value, ct);
}
```

`PlannedAsync` — расширить (параметры `RetrySeries? series`, `IReadOnlyList<string> skipped`), вызовы из прочих мест дополнить `series`/`skipped: []`; фаза:

```csharp
var phase = skipped.Count == 0 ? "planned" : $"planned, adopt-skipped: {string.Join(", ", skipped)}";
var planned = await journal.WritePhaseAsync(cluster, Op, phase, claims.InstanceId, null, ct, series);
```

Вызов в `TickAsync`: `var allocation = await PlanPortsAsync(snap, series, ct);` (series — из вставки Task 2; фаза planned переносит серию).

Ctor: добавить параметр `PortAllocIndex portAlloc` (перед optional `snapshot`). `Program.cs`: перед фабрикой ProvisioningProcess — `builder.Services.AddSingleton(sp => new PortAllocIndex(sp.GetRequiredService<IEtcdGateway>(), opts.Etcd.Endpoints.ToArray(), sp.GetRequiredService<ILogger<PortAllocIndex>>()));` и проброс в оба `new ProvisioningProcess(...)`/`AddShardProcess(...)` места (AddShard — Task 6; в этом шаге только ProvisioningProcess, компилятор укажет).

Существующий тест `Tick_FreshCluster_...` (свежая аллокация 15000) остаётся зелёным: `InspectResult` пуст → adoption тихо пропускает (changed=false, недобор → аллокация). Повторные тики сходящегося кластера не пишут portalloc (ранний выход шага 4).

- [ ] **Шаг 4: прогон — зелёные**

Run: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~ProvisioningProcessTests"`
Expected: PASS (новые + все существующие).

- [ ] **Шаг 5: коммит**

```bash
git add src/PgWorker.Provisioning/Processes/ProvisioningProcess.cs src/PgWorker.App/Program.cs src/tests/PgWorker.UnitTests/Provisioning/ProvisioningProcessTests.cs
git commit -m "feat(pgworker): усыновление фактических портов контейнеров в PlanPortsAsync (факт над записью: канонический контейнер = канон portalloc, перезапись при расхождении, object-записи не трогаем) + busy из portalloc соседей; самолечение стенда без пересоздания контейнеров (spec A, arch/14 §5 A P1)"
```

---

### Task 6: AddShardProcess — busy-union соседей (spec §3.3, пункт C)

**Files:**
- Modify: `src/PgWorker.Provisioning/Processes/AddShardProcess.cs` (ctor += `PortAllocIndex portAlloc`; `PlanShardPortsAsync` — busy-union)
- Modify: `src/PgWorker.App/Program.cs` (проброс в AddShardProcess-фабрику)
- Test: `src/tests/PgWorker.UnitTests/Provisioning/AddShardProcessTests.cs` (дописать один тест;现有 харнесс — посмотреть локальный Rig файла)

**Interfaces:**
- Consumes: `PortAllocIndex.ReadBusyAsync` (Task 4).
- Produces: ctor `AddShardProcess(..., PortAllocIndex portAlloc, ...)` — параметр перед optional `snapshot`.

- [ ] **Шаг 1: падающий тест (харнесс файла: `NewRig(patroniResponse, opts, busyPorts)` сидит Active-кластер shop + add-декларацию shard3 из 2 нод; docker-busy по умолчанию пуст)**

```csharp
[Fact]
public async Task Tick_FreshShard_AvoidsForeignPortallocRecords()
{
    // Arrange: сосед закрепил h1-тройку 15000/18000/16500; наш shard3 заявлен,
    // контейнеров нет (docker-busy пуст) — без foreign-busy новый шард получил бы базу 15000.
    var rig = await NewRig(_ => Patroni("shard3a"));
    rig.Etcd.Seed("/pgworker/portalloc/neighbor",
        """{"s1/n1":{"host":"h1","pg":15000,"patroni":18000,"doorman":16500}}""");

    // Act
    var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), "shard3", CancellationToken.None);

    // Assert: нода h1 нового шарда обошла чужую тройку (база 15001); h2 не затронут соседом.
    outcome.IsSuccess.Should().BeTrue();
    var raw = rig.Etcd.Store["/pgworker/portalloc/shop"].Value;
    raw.Should().Contain("\"shard3/shard3a\":{\"host\":\"h1\",\"pg\":15001");
}
```

(`NewRig` файла дорабатывается: конструирование `AddShardProcess` получает `new PortAllocIndex(etcd, [Ep], NullLogger<PortAllocIndex>.Instance)`; ВСЕ прочие места явного `new AddShardProcess(...)` в файле — компилятор укажет.)

- [ ] **Шаг 2: прогон — падает**

Run: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~AddShardProcessTests"`
Expected: FAIL — ctor без `PortAllocIndex`.

- [ ] **Шаг 3: реализация**

Ctor: добавить `PortAllocIndex portAlloc` (перед optional `snapshot`). В `PlanShardPortsAsync` заменить блок чтения busy:

```csharp
var dockerBusy = await driver.GetBusyPortsAsync(ct);
if (!dockerBusy.IsSuccess)
    return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(dockerBusy.Error!);
// Занятость = docker ∪ portalloc соседей (spec §3.3): свой portalloc уже в existing.
var foreignBusy = await portAlloc.ReadBusyAsync(cluster, ct);
if (!foreignBusy.IsSuccess)
    return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(foreignBusy.Error!);
var busy = new HashSet<(string, int)>(foreignBusy.Value);
foreach (var p in dockerBusy.Value)
    busy.Add(p);
```

(далее по тексту метода `busy.Value` → `busy`.)

- [ ] **Шаг 4: прогон — зелёные**

Run: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~AddShardProcessTests"`
Expected: PASS.

- [ ] **Шаг 5: коммит**

```bash
git add src/PgWorker.Provisioning/Processes/AddShardProcess.cs src/PgWorker.App/Program.cs src/tests/PgWorker.UnitTests/Provisioning/AddShardProcessTests.cs
git commit -m "feat(pgworker): AddShard видит portalloc соседей в busy-множестве — кросс-кластерные коллизии портов закрыты для add-пути тоже (spec C)"
```

---

### Task 7: PlainClusterDriver.EnsureNodeAsync — сверка портов контейнера (spec §3.2, пункт B)

**Files:**
- Modify: `src/PgWorker.Docker/Drivers/ClusterDriver.cs:108-139` (PlainClusterDriver.EnsureNodeAsync + private helper)
- Modify: `src/PgWorker.Docker/Drivers/ClusterDriver.cs` (`SwarmClusterDriver.EnsureNodeAsync` — только комментарий-«swarm: инспект тасков», ревью Ф4-3)
- Test: `src/tests/PgWorker.UnitTests/Docker/ClusterDriverTests.cs` (дописать; приватный `FakeEngine` уже в файле: `Containers`, `Inspects`, `Calls`, `CreatedSpec`)

**Interfaces:**
- Consumes: `IDockerEngine.InspectContainerAsync(id, ct)` → `DockerContainerInspect.Ports` (`PortMap[]`: ContainerPort→HostPort); `enableDoorman`-поле драйвера.
- Produces: поведение EnsureNode — «контейнер есть И биндинги совпадают → return; есть, но биндинги расходятся → stop+rm(force)+create+start (volume не трогаем)»; усыновлённые (`addr.Object != null`) не сверяются.

- [ ] **Шаг 1: падающие тесты (helpers уже есть в файле: `FakeEngine`, `NewPlainDriver(engine)`, `Topology(addr)`, `Addr` = 15432/18008/16432, `Secrets`, `Etcd`; `NewPlainDriver` использует `enableDoorman: true`)**

ВАЖНО — существующий тест `EnsureNode_ExistingContainer_DoesNotRecreate` (:158) сломается новой семантикой (драйвер зовёт `InspectContainerAsync`, а `FakeEngine.Inspects` пуст → Failed → throw). Его сид дополняется инспектом с планом портов:

```csharp
// правка существующего теста EnsureNode_ExistingContainer_DoesNotRecreate:
var engine = new FakeEngine
{
    Containers = [new DockerContainer("id1", ["pgw-shop-shard1-shard1a"], "running", "img")],
    Inspects = new Dictionary<string, DockerContainerInspect>
    {
        ["id1"] = new("id1", "shard1a", [], [],
            [new PortMap(5432, 15432), new PortMap(8008, 18008), new PortMap(6432, 16432)]),
    },
};
```

Новые тесты (добавить рядом):

```csharp
[Fact]
public async Task EnsureNode_PortDrift_RecreatesContainer()
{
    // Arrange: контейнер на ЧУЖИХ портах (сценарий: portalloc потерян и выделен заново).
    var engine = new FakeEngine
    {
        Containers = [new DockerContainer("id1", ["pgw-shop-shard1-shard1a"], "running", "img")],
        Inspects = new Dictionary<string, DockerContainerInspect>
        {
            ["id1"] = new("id1", "shard1a", [], [],
                [new PortMap(5432, 15111), new PortMap(8008, 18111), new PortMap(6432, 16611)]),
        },
    };
    var driver = NewPlainDriver(engine);

    // Act
    var result = await driver.EnsureNodeAsync(Topology(Addr), "shard1a", Addr, Secrets, Etcd, resources: null, ct: CancellationToken.None);

    // Assert: stop → remove → create → start с планом портов (PROVISIONING-фаза, volume жив).
    result.IsSuccess.Should().BeTrue();
    var calls = engine.Calls.Select(c => c.Call).ToList();
    calls.Should().ContainInOrder("stop", "create", "start");
    engine.CreatedSpec!.Ports.Should().Contain(new PortMap(5432, 15432));
}

[Fact]
public async Task EnsureNode_MissingBinding_RecreatesContainer()
{
    // Arrange: контейнер без 5432-биндинга — «бесполезный» контейнер.
    var engine = new FakeEngine
    {
        Containers = [new DockerContainer("id1", ["pgw-shop-shard1-shard1a"], "running", "img")],
        Inspects = new Dictionary<string, DockerContainerInspect>
        {
            ["id1"] = new("id1", "shard1a", [], [], [new PortMap(8008, 18008)]),
        },
    };
    var driver = new PlainClusterDriver([new HostEndpoint("h1", "fake://h1")], new FakeFactory(engine), enableDoorman: false);

    // Act
    await driver.EnsureNodeAsync(Topology(Addr), "shard1a", Addr, Secrets, Etcd, resources: null, ct: CancellationToken.None);

    // Assert: пересоздание (отсутствие ожидаемого биндинга = расхождение).
    engine.Calls.Select(c => c.Call).Should().Contain("create");
}

[Fact]
public async Task EnsureNode_AdoptedObjectNode_NeverTouched()
{
    // Arrange: усыновлённая нода (object) — чужой контейнер, сверка неприменима (R9).
    var engine = new FakeEngine
    {
        Containers = [new DockerContainer("id1", ["foreign-1"], "running", "img")],
        Inspects = new Dictionary<string, DockerContainerInspect>
        {
            ["id1"] = new("id1", "shard1a", [], [], [new PortMap(5432, 15999)]),
        },
    };
    var driver = NewPlainDriver(engine);
    var addr = new NodeAddress("h1", new NodePorts(15432, 18008, 16432), Object: "foreign-1");

    // Act
    var result = await driver.EnsureNodeAsync(Topology(addr), "shard1a", addr, Secrets, Etcd, resources: null, ct: CancellationToken.None);

    // Assert: никаких stop/remove/create.
    result.IsSuccess.Should().BeTrue();
    engine.Calls.Select(c => c.Call).Should().NotContain("stop");
    engine.Calls.Select(c => c.Call).Should().NotContain("create");
}
```

(`NewPlainDriver`/`Topology`/`Addr`/`Secrets`/`Etcd`/`FakeFactory` — существующие helper'ы файла `ClusterDriverTests.cs`.)

- [ ] **Шаг 2: прогон — падают**

Run: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~ClusterDriverTests"`
Expected: FAIL — drift-тест: контейнер не пересоздаётся (только list → return).

- [ ] **Шаг 3: реализация (замена блока идемпотентности :124-129)**

```csharp
// Идемпотентность со сверкой портов (spec §3.2): существующий контейнер
// обязан нести план публичных биндингов; расхождение → пересоздание
// (фаза PROVISIONING — данных нет, volume сохраняется). Без сверки контейнер
// навсегда оставался на чужих портах: WaitPatroni бил в мёртвый порт.
var existing = await engine.ListContainersAsync(name, all: true, ct);
if (!existing.IsSuccess)
    throw existing.Error!;
if (existing.Value.FirstOrDefault(c => c.Names.Contains(name)) is { } container)
{
    if (!string.IsNullOrEmpty(addr.Object))
        return; // усыновлённая (object) — чужой контейнер, не трогаем (R9)

    var inspect = await engine.InspectContainerAsync(container.Id, ct);
    if (!inspect.IsSuccess)
        throw inspect.Error!;
    if (PortsMatchPlan(inspect.Value.Ports, addr))
        return; // контейнер на месте с планом — идемпотентность

    var stopped = await engine.StopContainerAsync(name, timeoutSec: 10, ct);
    if (!stopped.IsSuccess)
        throw stopped.Error!;
    var removed = await engine.RemoveContainerAsync(name, force: true, ct);
    if (!removed.IsSuccess)
        throw removed.Error!;
}

var spec = BuildSpec(topology, nodeName, addr, secrets, etcd, resources);
// ... create/start без изменений ...

// Все ожидаемые public-биндинги контейнера совпадают с планом ноды
// (5432→pg, 8008→patroni, 6432→doorman при enableDoorman).
private bool PortsMatchPlan(IReadOnlyList<PortMap> actual, NodeAddress addr)
{
    var expected = new List<PortMap> { new(5432, addr.Ports.Pg), new(8008, addr.Ports.Patroni) };
    if (enableDoorman)
        expected.Add(new PortMap(6432, addr.Ports.Doorman));
    return expected.All(e => actual.Any(p => p.ContainerPort == e.ContainerPort && p.HostPort == e.HostPort));
}
```

Там же в шаге — комментарий-маркер в `SwarmClusterDriver.EnsureNodeAsync` (только комментарий, поведение swarm не меняется; spec §5 «отметить в коде», ревью Ф4-3):

```csharp
// swarm: сверка портов не реализована (MVP, стенд plain — spec §5):
// при необходимости — ListTasks(service) → ContainerId running-таска →
// InspectContainerAsync и тот же PortsMatchPlan-критерий.
```

- [ ] **Шаг 4: прогон — зелёные**

Run: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~ClusterDriverTests"`
Expected: PASS.

- [ ] **Шаг 5: коммит**

```bash
git add src/PgWorker.Docker/Drivers/ClusterDriver.cs src/tests/PgWorker.UnitTests/Docker/ClusterDriverTests.cs
git commit -m "fix(pgworker): EnsureNode сверяет public-биндинги контейнера с планом — расхождение портов → пересоздание (PROVISIONING-фазы; volume жив; object-ноды не трогаем), вторая линия обороны после усыновления (spec B, arch/14 §5 A P2.1)"
```

---

### Task 8: Панель — чтение /pgworker/work/ в снапшот (spec §3.4 D1)

**Files:**
- Create: `src/AdminPanel.Core/WorkJournalInfo.cs` (модель)
- Create: `src/AdminPanel.Etcd/Parsing/WorkJournalParser.cs`
- Modify: `src/AdminPanel.Core/EtcdSnapshot.cs` (+`PgWorkerWork`), `src/AdminPanel.Etcd/SnapshotBuilder.cs`, `src/AdminPanel.Etcd/SnapshotRefresher.cs` (range + FailTick)
- Test: `src/tests/AdminPanel.UnitTests/WorkJournalParserTests.cs` (Create), фикстуры `src/tests/AdminPanel.UnitTests/EtcdFixtures/work-*.json` (Create); правки `TestSnapshots.cs`, `SnapshotBuilderTests.cs`, `SnapshotRefresherTests.cs` (FakeEtcdGateway += WorkKv)
- Test: `src/tests/AdminPanel.IntegrationTests/EtcdSnapshotIntegrationTests.cs` (дописать интеграционный тест; прецеденты — `EtcdTestHarness.NewRefresher`, `EtcdContainerFixture`)

**Interfaces:**
- Consumes: `Kv` (AdminPanel.Etcd.Client), `KeyParseError` (Core), парсер-прецеденты `WorkerEndpointsParser`.
- Produces: `public sealed record WorkJournalInfo(string Cluster, string Op, string Phase, string Instance, long UpdatedUnix, string? LastError, int? FailCount, long? FailFirstUnix, long? RetryNotBeforeUnix);`; `WorkJournalParseResult(IReadOnlyList<WorkJournalInfo> Items, IReadOnlyList<KeyParseError> Errors)` + `static WorkJournalParseResult Parse(IReadOnlyList<Kv> kvs)`; `EtcdSnapshot`-поле `IReadOnlyList<WorkJournalInfo> PgWorkerWork` (после `PgWorkerEndpoints`).

- [ ] **Шаг 1: фикстуры + падающие тесты парсера**

`EtcdFixtures/work-provision-failed.json` (значение ключа `/pgworker/work/shop`):

```json
{"op":"provision","phase":"shard-provision","instance":"w-42","updated_unix":1756009200,"last_error":"Patroni шарда shop-shard1 не поднялся за бюджет 600 с","fail_count":3,"fail_first_unix":1756005400,"retry_not_before_unix":1756009215}
```

`EtcdFixtures/work-legacy.json`:

```json
{"op":"supervise","phase":"supervising","instance":"w-41","updated_unix":1756009100,"unreachable":{"shard1/shard1a":1756009000}}
```

`WorkJournalParserTests.cs`:

```csharp
public class WorkJournalParserTests
{
    private static Kv WorkKv(string cluster, string json)
        => new($"/pgworker/work/{cluster}", json, 42); // Kv(Key, Value, ulong ModRevision) — AdminPanel.Etcd.Client

    [Fact]
    public void Parse_ProvisionFailed_AllSeriesFields()
    {
        // Arrange: журнал с серией фейлов (канон arch/14 §3.3).
        var json = File.ReadAllText("EtcdFixtures/work-provision-failed.json");

        // Act
        var result = WorkJournalParser.Parse([WorkKv("shop", json)]);

        // Assert
        result.Errors.Should().BeEmpty();
        var w = result.Items.Should().ContainSingle().Subject;
        w.Cluster.Should().Be("shop");
        w.Op.Should().Be("provision");
        w.LastError.Should().Contain("не поднялся");
        w.FailCount.Should().Be(3);
        w.FailFirstUnix.Should().Be(1756005400);
        w.RetryNotBeforeUnix.Should().Be(1756009215);
    }

    [Fact]
    public void Parse_LegacyFormat_NullRetryFields()
    {
        // Arrange: старый формат без полей серии (обратная совместимость).
        var json = File.ReadAllText("EtcdFixtures/work-legacy.json");

        // Act
        var result = WorkJournalParser.Parse([WorkKv("demo", json)]);

        // Assert
        var w = result.Items.Should().ContainSingle().Subject;
        w.Op.Should().Be("supervise");
        w.FailCount.Should().BeNull();
        w.RetryNotBeforeUnix.Should().BeNull();
    }

    [Fact]
    public void Parse_MalformedJson_ParseErrorNotThrow()
    {
        // Arrange: битый JSON ключа — ключ скипается с ParseError (домен воркера, не трогаем).
        // Act
        var result = WorkJournalParser.Parse([WorkKv("bad", "{не-json")]);

        // Assert
        result.Items.Should().BeEmpty();
        result.Errors.Should().ContainSingle().Which.Key.Should().Be("/pgworker/work/bad");
    }
}
```

Плюс юнит refresher'а (митигация R4; дописать в `SnapshotRefresherTests` — прецеденты `DemoGateway()`/`RefresherTestHarness.New`/`SnapshotStore`):

```csharp
[Fact]
public async Task Refresh_WorkRangeTransportFails_PreviousPgWorkerWorkKept()
{
    // Arrange: прежний снапшот несёт PgWorkerWork; gateway валит transport
    // range /pgworker/work/ (FailTick: неполный снапшот хуже прежнего — spec R4).
    var gateway = new FakeEtcdGateway
    {
        ClustersKv = EtcdFixtures.LoadKv("clusters-full.json"),
        ServiceKv = EtcdFixtures.LoadKv("service-full.json"),
        NodesKv = EtcdFixtures.LoadKv("stand-nodes.json"),
        RangeFailPrefixes = ["/pgworker/work/"],
    };
    var store = new SnapshotStore();
    var previous = TestSnapshots.Healthy(new DateTimeOffset(2026, 9, 1, 12, 0, 0, TimeSpan.Zero)) with
    {
        PgWorkerWork = [new WorkJournalInfo("demo", "provision", "planned", "w-1", 1756000000, null, null, null, null)],
    };
    store.Replace(previous);
    var refresher = RefresherTestHarness.New(gateway, store, "http://e1");

    // Act
    var tick = await refresher.RefreshOnceAsync(CancellationToken.None);

    // Assert: тик отказной (FailTick: Reachable=false), прежние work-записи пережили отказ.
    tick.IsSuccess.Should().BeFalse();
    store.Current!.Etcd.Reachable.Should().BeFalse();
    store.Current.PgWorkerWork.Should().ContainSingle().Which.Cluster.Should().Be("demo");
}
```

- [ ] **Шаг 2: прогон — падает**

Run: `dotnet test src/tests/AdminPanel.UnitTests --filter "FullyQualifiedName~WorkJournalParserTests"`
Expected: FAIL — тип не существует.

- [ ] **Шаг 3: реализация**

`WorkJournalInfo.cs` (Core) — record выше. `WorkJournalParser.cs` (Etcd.Parsing) — по прецеденту `WorkerEndpointsParser` (толерантный JSON-parse: несуществующие поля → null; `cluster = key.Split('/')[^1]`; JsonException → `KeyParseError(key, reason)`).

`EtcdSnapshot` — после `PgWorkerEndpoints` вставить `IReadOnlyList<WorkJournalInfo> PgWorkerWork,`. `SnapshotBuilder.Build` — параметр `WorkJournalParseResult work` после `pgWorkerEndpoints` → в позицию. `SnapshotRefresher`:
- `Prefixes` += `public const string PgWorkerWork = "/pgworker/work/";`
- чтение `var workTask = WithFailoverAsync(alive, active, (ep, t) => gateway.RangeAsync(ep, Prefixes.PgWorkerWork, t), ct);` + `if (!workKv.IsSuccess ...) return FailTick(...)` (добавить в существующую проверку через `||`);
- `var workParsed = WorkJournalParser.Parse(workKv.Value);` → в `SnapshotBuilder.Build(...)`; ошибки — в свод `ParseErrors` (по образцу `pgWorkerEndpoints.Errors`);
- `FailTick`: `previous?.PgWorkerWork ?? [],` в конструктор failed-снапшота.

Тестовая инфраструктура: `TestSnapshots.Healthy` — добавить `[],` (PgWorkerWork) в позицию (и `[],` WorkerHealth — зарезервировать в Task 10; СЕЙЧАС добавить только PgWorkerWork, Task 10 добавит своё); `FakeEtcdGateway` — свойство `public IReadOnlyList<Kv> WorkKv { get; set; } = [];` + ветка `"/pgworker/work/" => WorkKv` в switch `RangeAsync`; `SnapshotBuilderTests`/`SnapshotRefresherTests` — компилятор укажет позиции (позиционные конструкторы/параметры).

- [ ] **Шаг 4: прогон — зелёные**

Run: `dotnet test src/tests/AdminPanel.UnitTests`
Expected: PASS (весь проект: правки конструктора снапшота не сломали соседние тесты).

- [ ] **Шаг 5: интеграционный — work-ключи в снапшоте через реальный etcd (ревью Ф4-2; spec §6)**

Дописать в `EtcdSnapshotIntegrationTests` (класс `(EtcdContainerFixture fixture)`, IClassFixture; фикстура изолирована на класс — битый ключ не мешает соседним тестам, они ассертят своё):

```csharp
[Fact]
public async Task Refresher_WorkJournal_ParsedIntoSnapshot()
{
    // Arrange — work-ключи в реальном etcd: валидный с серией + битый JSON.
    var ct = TestContext.Current.CancellationToken;
    var gateway = EtcdTestHarness.NewGateway();
    await gateway.PutAsync(fixture.Endpoint, "/pgworker/work/shop",
        """{"op":"provision","phase":"shard-provision","instance":"w-1","updated_unix":1756009200,"last_error":"boom","fail_count":2,"fail_first_unix":1756005400,"retry_not_before_unix":1756009210}""", ct);
    await gateway.PutAsync(fixture.Endpoint, "/pgworker/work/bad", "{не-json", ct);
    var store = new SnapshotStore();
    var refresher = EtcdTestHarness.NewRefresher(store, fixture.Endpoint);

    // Act
    var tick = await refresher.RefreshOnceAsync(CancellationToken.None);

    // Assert — журнал целиком в снапшоте; битый ключ — ParseError (02 §2.3.1).
    tick.IsSuccess.Should().BeTrue();
    var work = store.Current!.PgWorkerWork.Should().ContainSingle(w => w.Cluster == "shop").Subject;
    work.Op.Should().Be("provision");
    work.LastError.Should().Be("boom");
    work.FailCount.Should().Be(2);
    work.RetryNotBeforeUnix.Should().Be(1756009210);
    store.Current.ParseErrors.Should().Contain(e => e.Key == "/pgworker/work/bad");
}
```

Run: `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet test src/tests/AdminPanel.IntegrationTests --filter "FullyQualifiedName~Refresher_WorkJournal"`
Expected: PASS.

- [ ] **Шаг 6: коммит**

```bash
git add src/AdminPanel.Core/WorkJournalInfo.cs src/AdminPanel.Core/EtcdSnapshot.cs src/AdminPanel.Etcd/Parsing/WorkJournalParser.cs src/AdminPanel.Etcd/SnapshotBuilder.cs src/AdminPanel.Etcd/SnapshotRefresher.cs src/tests/AdminPanel.UnitTests/WorkJournalParserTests.cs src/tests/AdminPanel.UnitTests/EtcdFixtures/work-provision-failed.json src/tests/AdminPanel.UnitTests/EtcdFixtures/work-legacy.json src/tests/AdminPanel.UnitTests/TestSnapshots.cs src/tests/AdminPanel.UnitTests/SnapshotBuilderTests.cs src/tests/AdminPanel.UnitTests/SnapshotRefresherTests.cs src/tests/AdminPanel.IntegrationTests/EtcdSnapshotIntegrationTests.cs
git commit -m "feat(panel): снапшот читает /pgworker/work/<C> (WorkJournalParser → EtcdSnapshot.PgWorkerWork; битый JSON → ParseError, FailTick переносит) — панель видит фазы и серии фейлов воркера + интеграционный прогон на реальном etcd (spec D1, arch/adminpanel/02 §2.3.1)"
```

---

### Task 9: Панель — эскалация cluster-not-initialized + правило provision-stuck (spec §3.4 D2, D3)

**Files:**
- Modify: `src/AdminPanel.Core/Alerting/AlertsOptions.cs` (+2 порога), `src/AdminPanel.Core/Alerting/Rules/ClusterNotInitializedRule.cs`
- Create: `src/AdminPanel.Core/Alerting/Rules/ProvisionStuckRule.cs`
- Modify: `src/tests/AdminPanel.UnitTests/AlertTestRules.cs` (+2 правила), `src/AdminPanel.Api/appsettings.json` (Alerts)
- Test: `src/tests/AdminPanel.UnitTests/ShardingAlertRulesTests.cs` (дописать; локальный `Evaluate`-харнесс; ЗАМЕНА безаргументного вызова в существующем `ClusterNotInitialized_Rule_FiresInfoAlert` :336 — ревью Ф4-1)
- Modify: `src/tests/AdminPanel.IntegrationTests/EtcdSnapshotIntegrationTests.cs:44` (харнесс `EtcdTestHarness.NewRefresher`: безаргументный `new ClusterNotInitializedRule(),` → options-версия — иначе Task 9 ломает компиляцию AdminPanel.IntegrationTests; ревью Ф4-1)

**Interfaces:**
- Consumes: `EtcdSnapshot.PgWorkerWork` (Task 8), `AlertContext.Previous/NowUtc`, `TestSnapshots`.
- Produces: `AlertsOptions.NotInitializedWarnSec` (default 900), `AlertsOptions.ProvisionStuckSec` (default 300); kind `provision-stuck` (Warning, target `<C>`, Message содержит LastError, Details: `op`/`phase`/`fail_count`/`updated_unix`/`retry_not_before_unix`); эскалация существующего kind `cluster-not-initialized` до Warning при возрасте > порога.

- [ ] **Шаг 1: падающие тесты**

В `ShardingAlertRulesTests` (паттерн `Evaluate(rule, snapshot)`) + для эскалации — контекст с previous:

```csharp
[Fact]
public void ClusterNotInitialized_YoungCluster_InfoAlert()
{
    // Arrange: кластер заявлен 100 c назад (created_unix = NowUnix-100).
    var rule = new ClusterNotInitializedRule(Options.Create(new AlertsOptions { NotInitializedWarnSec = 900 }));
    var cluster = TestSnapshots.FullCluster() with
    {
        State = ClusterState.NotInitialized,
        CreatedUnix = NowUnix - 100,
    };

    // Act
    var alerts = Evaluate(rule, Snapshot(cluster));

    // Assert: молодой — info (нормальный жизненный цикл).
    var alert = alerts.Should().ContainSingle().Subject;
    alert.Severity.Should().Be(AlertSeverity.Info);
}

[Fact]
public void ClusterNotInitialized_StuckCluster_EscalatesToWarning()
{
    // Arrange: NOT_INITIALIZED дольше порога 900 c (больше PatroniBootSec воркера 600).
    var rule = new ClusterNotInitializedRule(Options.Create(new AlertsOptions { NotInitializedWarnSec = 900 }));
    var cluster = TestSnapshots.FullCluster() with
    {
        State = ClusterState.NotInitialized,
        CreatedUnix = NowUnix - 901,
    };

    // Act
    var alerts = Evaluate(rule, Snapshot(cluster));

    // Assert: эскалация по возрасту.
    alerts.Single().Severity.Should().Be(AlertSeverity.Warning);
}

[Fact]
public void ClusterNotInitialized_NoCreatedUnix_FallsBackToAlertAge()
{
    // Arrange: created_unix отсутствует (старые init) — возраст по sinceUnix previous-алерта.
    var rule = new ClusterNotInitializedRule(Options.Create(new AlertsOptions { NotInitializedWarnSec = 900 }));
    var cluster = TestSnapshots.FullCluster() with { State = ClusterState.NotInitialized, CreatedUnix = null };
    var previous = Snapshot(cluster) with
    {
        Alerts = [new Alert("cluster-not-initialized:demo", AlertSeverity.Info, "cluster-not-initialized",
            "demo", "…", null, NowUnix - 1000, "hint", AlertRemedy.WorkerAuto, "remedy")],
    };
    var context = new AlertContext(previous, Now, 3);

    // Act
    var alerts = rule.Evaluate(Snapshot(cluster), context).ToList();

    // Assert: previous-алерт старше порога → Warning.
    alerts.Single().Severity.Should().Be(AlertSeverity.Warning);
}

[Fact]
public void ProvisionStuck_LiveErrorSeriesOldEnough_WarningWithLastErrorText()
{
    // Arrange: журнал provision с серией фейлов старше порога 300 c.
    var rule = new ProvisionStuckRule(Options.Create(new AlertsOptions { ProvisionStuckSec = 300 }));
    var snapshot = Snapshot(TestSnapshots.FullCluster() with { State = ClusterState.NotInitialized }) with
    {
        PgWorkerWork = [new WorkJournalInfo("demo", "provision", "shard-provision", "w-1",
            UpdatedUnix: NowUnix - 10, LastError: "Patroni шарда demo-s1 не поднялся за бюджет 600 с",
            FailCount: 3, FailFirstUnix: NowUnix - 400, RetryNotBeforeUnix: NowUnix + 20)],
    };

    // Act
    var alerts = Evaluate(rule, snapshot);

    // Assert: warning с текстом ошибки воркера и деталями серии.
    var alert = alerts.Should().ContainSingle().Subject;
    alert.Kind.Should().Be("provision-stuck");
    alert.Target.Should().Be("demo");
    alert.Severity.Should().Be(AlertSeverity.Warning);
    alert.Message.Should().Contain("не поднялся");
    alert.Details!["fail_count"].Should().Be("3");
}

[Fact]
public void ProvisionStuck_FreshSeriesOrNoError_NoAlert()
{
    // Arrange: (а) серия моложе порога; (б) last_error нет.
    var rule = new ProvisionStuckRule(Options.Create(new AlertsOptions { ProvisionStuckSec = 300 }));
    var fresh = Snapshot(TestSnapshots.FullCluster()) with
    {
        PgWorkerWork = [new WorkJournalInfo("demo", "provision", "planned", "w-1",
            NowUnix - 5, "boom", 1, NowUnix - 5, NowUnix)],
    };
    var healthy = Snapshot(TestSnapshots.FullCluster()) with
    {
        PgWorkerWork = [new WorkJournalInfo("demo", "provision", "waiting-patroni", "w-1",
            NowUnix - 5, null, null, null, null)],
    };

    // Act + Assert
    Evaluate(rule, fresh).Should().BeEmpty();
    Evaluate(rule, healthy).Should().BeEmpty();
}
```

- [ ] **Шаг 2: прогон — падают**

Run: `dotnet test src/tests/AdminPanel.UnitTests --filter "FullyQualifiedName~ShardingAlertRulesTests"`
Expected: FAIL — ctor правил с options не существует / kind отсутствует.

- [ ] **Шаг 3: реализация**

`AlertsOptions` +=

```csharp
// cluster-not-initialized: эскалация info→warning, когда кластер висит в
// NOT_INITIALIZED дольше N секунд (арх/03 §4; 900 > PatroniBootSec=600 —
// здоровый провижининг не эскалируется).
public int NotInitializedWarnSec { get; set; } = 900;

// provision-stuck: серия фейлов provision (fail_first_unix) старше N секунд.
public int ProvisionStuckSec { get; set; } = 300;
```

`ClusterNotInitializedRule` — ctor `(IOptions<AlertsOptions> options)` (полный новый код правила):

```csharp
// cluster-not-initialized (info → warning по возрасту, arch/adminpanel/03 §4):
// кластер заявлен, но ноды не подняты — заметка, пока висит недолго; зависание
// дольше NotInitializedWarnSec (900 c > PatroniBootSec=600) — эскалация.
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class ClusterNotInitializedRule(IOptions<AlertsOptions> options) : IAlertRule
{
    public const string KindName = "cluster-not-initialized";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        var threshold = options.Value.NotInitializedWarnSec;
        var nowUnix = context.NowUtc.ToUnixTimeSeconds();
        foreach (var cluster in snapshot.Clusters.Where(c => c.State == ClusterState.NotInitialized))
        {
            var id = $"{KindName}:{cluster.Name}";
            // Возраст: created_unix (не зависит от рестартов панели), fallback —
            // возраст алерта по previous-снапшоту, иначе «только что увидели».
            var since = cluster.CreatedUnix
                        ?? context.Previous?.Alerts.FirstOrDefault(a => a.Id == id)?.SinceUnix
                        ?? nowUnix;
            var stuckFor = nowUnix - since > threshold;
            yield return new Alert(
                id,
                stuckFor ? AlertSeverity.Warning : AlertSeverity.Info,
                KindName,
                cluster.Name,
                stuckFor
                    ? $"кластер {cluster.Name} висит в NOT_INITIALIZED дольше {threshold} c — provisioning не завершается (причину см. provision-stuck/journal воркера)"
                    : $"кластер {cluster.Name} заявлен (NOT_INITIALIZED): ноды не подняты, схемы не созданы",
                new Dictionary<string, string> { ["dbname"] = cluster.DbName ?? "missing" },
                null,
                "кластер заявлен (config.state=NOT_INITIALIZED), ноды не подняты: это нормальный жизненный цикл — provisioning воркера поднимает ноды и переведёт state в ACTIVE; зависание дольше бюджета Patroni (600 c) — уже не нормальный цикл",
                AlertRemedy.WorkerAuto,
                stuckFor
                    ? "смотрите /pgworker/work/<C> (last_error/fail_count) и логи воркера: вечный provisioning = дефект воркера или окружения"
                    : "дождитесь provisioning (воркер пишет nodes state и снимет NOT_INITIALIZED); висит дольше обычного — смотрите journal воркера");
        }
    }
}
```

`ProvisionStuckRule` (новый, по каталожному шаблону `MoveStaleRule`):

```csharp
// provision-stuck (warning): /pgworker/work/<C> несёт живой last_error и серию
// фейлов provision старше ProvisionStuckSec — воркер сообщил причину, но кластер
// не инициализируется (arch/adminpanel/03 §4; серия живёт с первого фейла до
// успеха — возраст по fail_first_unix, не по updated_unix).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class ProvisionStuckRule(IOptions<AlertsOptions> options) : IAlertRule
{
    public const string KindName = "provision-stuck";
    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        var threshold = options.Value.ProvisionStuckSec;
        var nowUnix = context.NowUtc.ToUnixTimeSeconds();
        foreach (var w in snapshot.PgWorkerWork.Where(w =>
                     w.Op == "provision" && w.LastError is not null
                     && w.FailFirstUnix is { } first && nowUnix - first > threshold))
        {
            yield return new Alert(
                $"{KindName}:{w.Cluster}",
                AlertSeverity.Warning,
                KindName,
                w.Cluster,
                $"provision кластера {w.Cluster} фейлится: {w.LastError}",
                new Dictionary<string, string>
                {
                    ["op"] = w.Op, ["phase"] = w.Phase,
                    ["fail_count"] = w.FailCount?.ToString() ?? "?",
                    ["updated_unix"] = w.UpdatedUnix.ToString(),
                    ["retry_not_before_unix"] = w.RetryNotBeforeUnix?.ToString() ?? "",
                },
                null,
                "воркер сообщает причину фейла provision в /pgworker/work/<C>: серия живёт с первого фейла (fail_first_unix) до успеха; вечная серия = дефект воркера или окружения (порты/образ/etcd)",
                AlertRemedy.WorkerAuto,
                "смотрите журнал /pgworker/work/<C> и логи воркера; воркер сам ретраит с бэкоффом — если причина внешняя (занятые порты, битый образ), действуйте по runbook arch/09");
        }
    }
}
```

Компиляция остальных безаргументных вызовов `ClusterNotInitializedRule()` (ревью Ф4-1 — расширение ctor до `(IOptions<AlertsOptions>)`):

- `src/tests/AdminPanel.IntegrationTests/EtcdSnapshotIntegrationTests.cs:44` (харнесс `EtcdTestHarness.NewRefresher`): заменить `new ClusterNotInitializedRule(),` → `new ClusterNotInitializedRule(Options.Create(new AlertsOptions())),`;
- существующий тест `src/tests/AdminPanel.UnitTests/ShardingAlertRulesTests.cs:336` (`ClusterNotInitialized_Rule_FiresInfoAlert`): заменить безаргументный вызов на options-версию — ассерт Info НЕ ломается (кластер теста строится с `CreatedUnix = null`, fallback sinceUnix == now → возраст 0 < 900).

`AlertTestRules.All()` += `new ClusterNotInitializedRule(Options.Create(new AlertsOptions())),` (заменить существующий безoptions-вызов) и `new ProvisionStuckRule(Options.Create(new AlertsOptions())),`. `appsettings.json` (AdminPanel.Api) — `Alerts` += `"NotInitializedWarnSec": 900, "ProvisionStuckSec": 300`.

- [ ] **Шаг 4: прогон — зелёные**

Run: `dotnet test src/tests/AdminPanel.UnitTests`
Expected: PASS (включая AlertEngineTests/AlertHintRemedyTests — новые поля Hint/Remedy заполнены).

- [ ] **Шаг 5: коммит**

```bash
git add src/AdminPanel.Core/Alerting/AlertsOptions.cs src/AdminPanel.Core/Alerting/Rules/ClusterNotInitializedRule.cs src/AdminPanel.Core/Alerting/Rules/ProvisionStuckRule.cs src/tests/AdminPanel.UnitTests/AlertTestRules.cs src/tests/AdminPanel.UnitTests/ShardingAlertRulesTests.cs src/tests/AdminPanel.IntegrationTests/EtcdSnapshotIntegrationTests.cs src/AdminPanel.Api/appsettings.json
git commit -m "feat(panel): эскалация cluster-not-initialized по возрасту (900 c, info→warning) + правило provision-stuck (last_error + возраст серии fail_first_unix, текст ошибки оператору) (spec D2/D3, arch/adminpanel/03 §4)"
```

---

### Task 10: Панель — WorkerHealth poller + правило worker-unhealthy (spec §3.4 D4)

**Files:**
- Create: `src/AdminPanel.Core/WorkerHealth.cs` (модель + IWorkerHealthStore)
- Create: `src/AdminPanel.Etcd/Workers/WorkerHealthStore.cs`, `src/AdminPanel.Etcd/Workers/WorkerHealthPoller.cs`
- Modify: `src/AdminPanel.Etcd/Workers/WorkerApiOptions.cs` (+HealthEnabled/HealthIntervalSec), `src/AdminPanel.Core/EtcdSnapshot.cs` (+WorkerHealth), `src/AdminPanel.Etcd/SnapshotRefresher.cs` (ctor += IWorkerHealthStore; внесение после Build; FailTick), `src/tests/AdminPanel.UnitTests/AlertTestRules.cs` (+правило)
- Create: `src/AdminPanel.Core/Alerting/Rules/WorkerUnhealthyRule.cs`
- Test: `src/tests/AdminPanel.UnitTests/Workers/WorkerHealthPollerTests.cs` (Create), `HaAlertRulesTests.cs` или новый `WorkerAlertRulesTests.cs` (правило), правки `TestSnapshots.cs`/`SnapshotRefresherTests.cs` (ctor)

**Interfaces:**
- Consumes: `ISnapshotReader.Current` (Core — живые `PgWorkerEndpoints`), `IHttpClientFactory` (именованный `WorkerApiGateway.HttpClientName` = `"workers"`), `FixedTimeProvider`.
- Produces: `sealed record WorkerHealth(string InstanceId, string Url, WorkerHealthStatus Status, DateTimeOffset CheckedAtUtc, string? Detail);` `enum WorkerHealthStatus { Healthy, Degraded, Unreachable }`; `interface IWorkerHealthStore { IReadOnlyList<WorkerHealth>? Current { get; } void Replace(IReadOnlyList<WorkerHealth> health); }`; `WorkerHealthPoller.RunOnceAsync(CancellationToken)` — публичное ядро тика; kind `worker-unhealthy` (Warning, target `pgworker/<instanceId>`).

- [ ] **Шаг 1: падающие тесты poller'а (stub IHttpClientFactory — Moq в проекте НЕТ)**

```csharp
public class WorkerHealthPollerTests
{
    private static readonly DateTimeOffset Now = new(2026, 9, 1, 12, 0, 0, TimeSpan.Zero);

    // Handler-дабл: ответ или исключение на каждый запрос.
    private sealed class FakeHandler(Func<HttpRequestMessage, HttpResponseMessage> responder) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
            => Task.FromResult(responder(request));
    }

    private sealed class StubFactory(HttpMessageHandler handler) : IHttpClientFactory
    {
        public HttpClient CreateClient(string name) => new(handler, disposeHandler: false);
    }

    // ISnapshotReader-дабл: снапшот с одним живым endpoint /pgworker/api/w1.
    private sealed class StubReader(EtcdSnapshot? snapshot) : ISnapshotReader
    {
        public EtcdSnapshot? Current { get; } = snapshot;
    }

    private static (WorkerHealthPoller Poller, WorkerHealthStore Store) Poller(
        Func<HttpRequestMessage, HttpResponseMessage> respond, EtcdSnapshot? snapshot = null)
    {
        var store = new WorkerHealthStore();
        var time = new FixedTimeProvider { Utc = Now };
        var poller = new WorkerHealthPoller(
            new StubReader(snapshot ?? TestSnapshots.Healthy(Now)), store, new StubFactory(new FakeHandler(respond)),
            Options.Create(new WorkerApiOptions { HealthIntervalSec = 15, TimeoutSec = 3 }),
            time, NullLogger<WorkerHealthPoller>.Instance);
        return (poller, store);
    }

    [Fact]
    public async Task RunOnce_Healthz200_MarkedHealthy()
    {
        // Arrange: один живой endpoint /pgworker/api/, /healthz отвечает 200.
        var (poller, store) = Poller(_ => new HttpResponseMessage(HttpStatusCode.OK));

        // Act
        await poller.RunOnceAsync(CancellationToken.None);

        // Assert
        store.Current!.Should().ContainSingle().Which.Status.Should().Be(WorkerHealthStatus.Healthy);
    }

    [Fact]
    public async Task RunOnce_Healthz503_MarkedDegraded()
    {
        // Arrange: /healthz = 503 (Degraded воркера: секции etcd/docker/loops).
        var (poller, store) = Poller(_ => new HttpResponseMessage(HttpStatusCode.ServiceUnavailable));

        // Act
        await poller.RunOnceAsync(CancellationToken.None);

        // Assert
        var w = store.Current!.Should().ContainSingle().Subject;
        w.Status.Should().Be(WorkerHealthStatus.Degraded);
        w.Detail.Should().Contain("503");
    }

    [Fact]
    public async Task RunOnce_NetworkError_MarkedUnreachable()
    {
        // Arrange: lease-ключ жив, но соединение падает (панель не достучалась).
        var (poller, store) = Poller(_ => throw new HttpRequestException("connection refused"));

        // Act
        await poller.RunOnceAsync(CancellationToken.None);

        // Assert
        store.Current!.Should().ContainSingle().Which.Status.Should().Be(WorkerHealthStatus.Unreachable);
    }

    [Fact]
    public async Task RunOnce_NoLiveEndpoints_StoreEmpty()
    {
        // Arrange: живых ключей /pgworker/api/ нет (воркер не поднимался/lease
        // истекли) — домен worker-api-unreachable, не этого poller'а.
        var empty = TestSnapshots.Healthy(Now) with { PgWorkerEndpoints = [] };
        var (poller, store) = Poller(_ => new HttpResponseMessage(HttpStatusCode.OK), empty);

        // Act
        await poller.RunOnceAsync(CancellationToken.None);

        // Assert: пустой список (НЕ null) — правило worker-unhealthy молчит
        // (ревью Ф4-3а: граничный сценарий пустых endpoints).
        store.Current.Should().NotBeNull().And.BeEmpty();
    }
}
```

Тесты правила (в новом `WorkerAlertRulesTests.cs`; харнесс — копия шапки `ShardingAlertRulesTests`: поля `Now`/`NowUnix`, локальный `Evaluate(rule, snapshot)`, `Snapshot(params ClusterInfo[])` поверх `TestSnapshots.Healthy(Now)`):

```csharp
[Fact]
public void WorkerUnhealthy_DegradedInstance_WarningPerInstance()
{
    // Arrange: один инстанс Degraded при живом lease-ключе.
    var rule = new WorkerUnhealthyRule();
    var snapshot = TestSnapshots.Healthy(Now) with
    {
        WorkerHealth = [new WorkerHealth("w1", "http://pgworker:8080", WorkerHealthStatus.Degraded,
            Now, "цикл reconcile не тикал 120 с")],
    };

    // Act
    var alerts = Evaluate(rule, snapshot);

    // Assert: warning на конкретный инстанс, Detail в Message.
    var alert = alerts.Should().ContainSingle().Subject;
    alert.Kind.Should().Be("worker-unhealthy");
    alert.Target.Should().Be("pgworker/w1");
    alert.Severity.Should().Be(AlertSeverity.Warning);
    alert.Message.Should().Contain("reconcile");
}

[Fact]
public void WorkerUnhealthy_AllHealthy_NoAlerts()
{
    // Arrange
    var snapshot = TestSnapshots.Healthy(Now) with
    {
        WorkerHealth = [new WorkerHealth("w1", "http://pgworker:8080", WorkerHealthStatus.Healthy, Now, null)],
    };

    // Act + Assert
    Evaluate(new WorkerUnhealthyRule(), snapshot).Should().BeEmpty();
}
```

- [ ] **Шаг 2: прогон — падают**

Run: `dotnet test src/tests/AdminPanel.UnitTests --filter "FullyQualifiedName~WorkerHealth|FullyQualifiedName~WorkerUnhealthy"`
Expected: FAIL — типы не существуют.

- [ ] **Шаг 3: реализация**

`WorkerHealth.cs` (Core): модель + интерфейс (выше). `WorkerHealthStore.cs`:

```csharp
// Стор результатов опроса /healthz (паттерн ProbeResultsStore): poller пишет,
// refresher вносит готовым в снапшот — KV-тик не блокируется (arch/adminpanel/02 §4).
[InjectAsSingleton(typeof(IWorkerHealthStore))]
public sealed class WorkerHealthStore : IWorkerHealthStore
{
    private volatile IReadOnlyList<WorkerHealth>? _current;
    public IReadOnlyList<WorkerHealth>? Current => _current;
    public void Replace(IReadOnlyList<WorkerHealth> health) => _current = health;
}
```

`WorkerHealthPoller.cs`:

```csharp
// Тик опроса /healthz живых инстансов PgWorker (spec §3.4 D4; arch/adminpanel/02
// §2.3.1): 200 → Healthy, 503 → Degraded, сетевой сбой/таймаут → Unreachable
// (lease жив — панель «недавно видела» воркера). /healthz не под X-Api-Key.
[InjectAsSingleton(typeof(IHostedService))]
public sealed class WorkerHealthPoller(
    ISnapshotReader snapshotReader,
    IWorkerHealthStore store,
    IHttpClientFactory factory,
    IOptions<WorkerApiOptions> options,
    TimeProvider time,
    ILogger<WorkerHealthPoller> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var value = options.Value;
        if (!value.HealthEnabled)
        {
            logger.LogInformation("AdminPanel:Workers:HealthEnabled=false — опрос /healthz не запускается");
            return;
        }

        var seconds = value.HealthIntervalSec > 0 ? value.HealthIntervalSec : 15;
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(seconds));
        do
        {
            await RunOnceAsync(stoppingToken);
        }
        while (await timer.WaitForNextTickAsync(stoppingToken));
    }

    // Ядро тика — публично для unit-тестов (прецедент RefreshOnceAsync).
    public async Task RunOnceAsync(CancellationToken ct)
    {
        var endpoints = snapshotReader.Current?.PgWorkerEndpoints ?? [];
        var at = time.GetUtcNow();
        var results = await Task.WhenAll(endpoints.Select(e => ProbeAsync(e, at, ct)));
        store.Replace([.. results.OrderBy(r => r.InstanceId, StringComparer.Ordinal)]);
    }

    private async Task<WorkerHealth> ProbeAsync(WorkerEndpoint endpoint, DateTimeOffset at, CancellationToken ct)
    {
        using var client = factory.CreateClient(WorkerApiGateway.HttpClientName);
        var timeout = options.Value.TimeoutSec;
        if (timeout > 0)
            client.Timeout = TimeSpan.FromSeconds(timeout);
        try
        {
            using var response = await client.GetAsync(new Uri(new Uri(endpoint.Url), "/healthz"), ct);
            var status = response.IsSuccessStatusCode
                ? WorkerHealthStatus.Healthy
                : WorkerHealthStatus.Degraded; // 503 и прочие — процесс жив, но нездоров
            return new WorkerHealth(endpoint.InstanceId, endpoint.Url, status, at,
                status == WorkerHealthStatus.Healthy ? null : $"HTTP {(int)response.StatusCode}");
        }
        catch (Exception e) when (e is HttpRequestException or TaskCanceledException)
        {
            return new WorkerHealth(endpoint.InstanceId, endpoint.Url, WorkerHealthStatus.Unreachable, at, e.Message);
        }
    }
}
```

`WorkerApiOptions` += `public bool HealthEnabled { get; set; } = true;` / `public int HealthIntervalSec { get; set; } = 15;`. `EtcdSnapshot` — после `PgWorkerWork` вставить `IReadOnlyList<WorkerHealth> WorkerHealth,`. `SnapshotRefresher`: ctor += `IWorkerHealthStore workerHealthStore` (после `IProbeStateStore`); после `ProbeEnricher.Apply(...)` — `built = built with { WorkerHealth = workerHealthStore.Current ?? [] };` (до Evaluate алертов); `FailTick` — `previous?.WorkerHealth ?? [],`. `WorkerUnhealthyRule`:

```csharp
// worker-unhealthy (warning): живой lease-ключ /pgworker/api/<id>, но /healthz ≠ 200 —
// процесс нездоров ДО истечения lease (docker-healthcheck гасит контейнер, ключи
// вот-вот исчезнут → эстафета worker-api-unreachable critical). arch/adminpanel/03 §4.
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class WorkerUnhealthyRule : IAlertRule
{
    public const string KindName = "worker-unhealthy";
    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var w in snapshot.WorkerHealth.Where(w => w.Status != WorkerHealthStatus.Healthy))
        {
            var what = w.Status == WorkerHealthStatus.Degraded
                ? $"/healthz отвечает не-200 ({w.Detail ?? "degraded"})"
                : $"недостижим по URL lease-ключа ({w.Detail ?? "network error"})";
            yield return new Alert(
                $"{KindName}:pgworker/{w.InstanceId}",
                AlertSeverity.Warning,
                KindName,
                $"pgworker/{w.InstanceId}",
                $"инстанс PgWorker {w.InstanceId} нездоров: {what}",
                new Dictionary<string, string> { ["url"] = w.Url, ["checked_unix"] = w.CheckedAtUtc.ToUnixTimeSeconds().ToString() },
                null,
                "lease-ключ жив, но health-проба процесса плохая: секции /healthz (etcd/docker-хосты/циклы/снапшот) деградированы; docker-healthcheck гасит контейнер — за этим последует исчезновение lease и critical worker-api-unreachable",
                AlertRemedy.OperatorRunbook,
                "смотрите docker logs pgworker и /healthz напрямую (секции etcd-reachable/docker-hosts/loops-alive/snapshot); поднимите зависимость (etcd/docker) или перезапустите контейнер воркера (deploy/docker-compose.yml)");
        }
    }
}
```

`AlertTestRules.All()` += `new WorkerUnhealthyRule(),`. Тестовые правки: `TestSnapshots.Healthy` — добавить `[]` для WorkerHealth; `SnapshotRefresherTests` — ctor + `new WorkerHealthStore()` (или settable-дабл); интеграционный харнесс `EtcdTestHarness.NewRefresher` (AdminPanel.IntegrationTests) — дополнить новым аргументом `new WorkerHealthStore()` (ctor refresher'а расширился; компилятор укажет).

- [ ] **Шаг 4: прогон — зелёные**

Run: `dotnet test src/tests/AdminPanel.UnitTests`
Expected: PASS.

- [ ] **Шаг 5: коммит**

```bash
git add src/AdminPanel.Core/WorkerHealth.cs src/AdminPanel.Core/EtcdSnapshot.cs src/AdminPanel.Etcd/Workers/WorkerHealthStore.cs src/AdminPanel.Etcd/Workers/WorkerHealthPoller.cs src/AdminPanel.Etcd/Workers/WorkerApiOptions.cs src/AdminPanel.Etcd/SnapshotRefresher.cs src/AdminPanel.Core/Alerting/Rules/WorkerUnhealthyRule.cs src/tests/AdminPanel.UnitTests/Workers/WorkerHealthPollerTests.cs src/tests/AdminPanel.UnitTests/WorkerAlertRulesTests.cs src/tests/AdminPanel.UnitTests/AlertTestRules.cs src/tests/AdminPanel.UnitTests/TestSnapshots.cs src/tests/AdminPanel.UnitTests/SnapshotRefresherTests.cs src/AdminPanel.Api/appsettings.json
git commit -m "feat(panel): опрос /healthz живых инстансов PgWorker (WorkerHealthPoller/Store, 15 c) + алерт worker-unhealthy (warning per-instance, до истечения lease) — degraded-воркер виден, пока критичный worker-api-unreachable молчит (spec D4в, arch/adminpanel/02 §2.3.1/03 §4)"
```

(appsettings: `AdminPanel:Workers` += `"HealthEnabled": true, "HealthIntervalSec": 15`.)

---

### Task 11: Чек 15 — чистка portalloc + документ верификации стенда (spec §3.6, Ф7)

**Files:**
- Modify: `dev-stand/adminpanel/checks/15-cluster-create.sh` (блок чистки :20-27)
- Create: `docs/superpowers/2026-09-01-fix-provision-portalloc-alerts/stand-verification.md`

**Interfaces:**
- Consumes: contract arch/14 §5 A (самолечение), критерии приёмки spec §7.
- Produces: чек не оставляет `/pgworker/portalloc/smoke` при пересеве; инструкция верификации на живом стенде ПОСЛЕ деплоя (read-only наблюдение).

- [ ] **Шаг 1: правка чека (после `ect del --prefix /clusters/smoke`)**

```bash
# Чистка прошлых прогонов: только свои ключи (префикс кластера + request_* +
# порт-закрепление — без него пересев оставлял portalloc прошлого прогона,
# источник коллизий/усыхающих деклараций; диагностика 2026-09-01).
ect() { docker compose exec -T etcd etcdctl --endpoints=http://localhost:2379 "$@"; }
ect del --prefix /clusters/smoke >/dev/null
ect del /pgworker/portalloc/smoke >/dev/null
for k in request_cpu request_mem request_disk; do
  ...
```

- [ ] **Шаг 2: документ верификации `stand-verification.md`**

```markdown
# Верификация фикса на живом стенде (после деплоя образа pgworker:dev по приказу)

Стенд НЕ перезапускается и руками не трогается: все проверки — чтение
(docker logs/ps, etcdctl get, UI панели :5050). Деплой фикса — отдельный шаг
(пересборка образа + up -d pgworker в deploy/), по приказу пользователя.

## 1. Самолечение canon10/smoke (тик воркера, ожидание — минуты)

- Журнал воркера: `docker logs -f <pgworker>` — фазы provision идут без
  мгновенных повторных фейлов «не поднялся за бюджет» (бэкофф: серия фейлов
  редеет до ≤1/60 c, если проблема остаётся).
- Portalloc = факт: `docker compose exec etcd etcdctl get /pgworker/portalloc/canon10`
  содержит порты ФАКТИЧЕСКИХ контейнеров (15004–15009; сверка с
  `docker ps --filter name=pgw-canon10 --format '{{.Ports}}'`).
- Итог: `/clusters/canon10/config` без поля state (ACTIVE), все
  nodes state=RUNNING, status-ключи бакетов сняты, dsn записаны. Аналогично smoke.
- Контейнеры НЕ пересоздавались: `docker ps --filter name=pgw-` — те же
  контейнеры (CreatedAt/uptime не сброшены).

## 2. Коллизия закрыта (по приказу, опционально)

Создать новый кластер через панель/UI (e2e чек 15 или вручную) при живом
canon10: новый portalloc не пересекается с /pgworker/portalloc/* соседей.

## 3. Алерты панели (UI :5050)

- cluster-not-initialized: после 900 c зависания — Warning (если кластер
  ещё не поднялся); гаснет при ACTIVE.
- provision-stuck: при живом last_error provision + серия старше 300 c —
  Warning с текстом ошибки (проверить details: fail_count).
- worker-unhealthy: пока /healthz воркера 503 — Warning pgworker/<id>;
  после самолечения — гаснет (healthz 200).

## 4. Черепки pgw-solo-*

Не трогать. Диагностика/процедура — arch/09 §11 (только по приказу).
```

- [ ] **Шаг 3: проверка чека синтаксически**

Run: `bash -n dev-stand/adminpanel/checks/15-cluster-create.sh`
Expected: exit 0 (синтаксис; e2e-прогон — НЕ в этом плане, стенд не трогаем).

- [ ] **Шаг 4: коммит**

```bash
git add dev-stand/adminpanel/checks/15-cluster-create.sh docs/superpowers/2026-09-01-fix-provision-portalloc-alerts/stand-verification.md
git commit -m "docs(stand): чек 15 чистит /pgworker/portalloc/smoke при пересеве (источник потери/коллизий portalloc) + инструкция верификации самолечения на живом стенде после деплоя (read-only; spec F/Ф7)"
```

---

### Task 12: Финальная сборка, полные прогоны, self-review (spec Ф6)

**Files:**
- Modify: (по результату self-review — точечные правки кода/тестов/arch)

**Interfaces:**
- Consumes: всё выше.
- Produces: зелёная сборка + все тесты обоих проектов; сверка диффа с контрактами arch/.

- [ ] **Шаг 1: сборка без warnings**

Run: `dotnet build src/PgWorker.slnx`
Expected: Build succeeded, 0 warnings (`TreatWarningsAsErrors` — любой warning = ошибка сборки).

- [ ] **Шаг 2: полные прогоны**

Run: `dotnet test src/tests/PgWorker.UnitTests && dotnet test src/tests/AdminPanel.UnitTests && DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet test src/tests/PgWorker.IntegrationTests && DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 dotnet test src/tests/AdminPanel.IntegrationTests`
Expected: PASS (PgWorker 427+ существующих + новые; AdminPanel 513+ существующих + новые; интеграционные — включая новые WorkJournal-RetrySeries и Refresher_WorkJournal, ревью Ф4-2).

- [ ] **Шаг 3: self-review против контрактов (список-чеклист)**

Проверить по диффу (`git diff main...HEAD` — или от точки ветвления):
- arch/14 §2.4: busy = docker ∪ portalloc всех кластеров — отражено в ProvisioningProcess+AddShardProcess;
- arch/14 §5 A P1/P2.1: adoption + сверка портов — код соответствует тексту контракта;
- arch/14 §3.3: поля серии /pgworker/work — имена JSON совпадают (`fail_count`/`fail_first_unix`/`retry_not_before_unix`);
- arch/adminpanel/02 §2.3.1: панель читает ровно 4 семейства (portalloc/moves/api/work) — лишних range нет;
- arch/adminpanel/03 §4: kind/severity/условия соответствуют каталогу;
- spec §5 ограничения: AddShard НЕ усыновляет и НЕ имеет backoff-skip (только сброс трекера Task 3); swarm без сверки (код SwamClusterDriver не тронут).

- [ ] **Шаг 4: итоговый коммит (если были правки self-review)**

```bash
git add -A
git commit -m "fix(review): точечные правки по self-review фазы Ф6 — сверка с контрактами arch/14 и arch/adminpanel/02/03"
```

(если правок нет — пропуск шага; финальное состояние ветки готово к ревью перед main.)

---

## Порядок зависимостей

```
Task 1 (RetrySeries) ──► Task 2 (backoff provision) ──► Task 3 (сброс трекера)
Task 4 (PortAllocIndex) ──► Task 5 (PlanPortsAsync adoption) ──► Task 6 (AddShard busy)
Task 7 (EnsureNode сверка) — независим (после Task 5 желательно: общий сценарий)
Task 8 (панель: work в снапшоте) ──► Task 9 (правила D2/D3)
Task 10 (WorkerHealth) — независим от 8/9 по коду, НО трогает те же файлы (EtcdSnapshot/SnapshotRefresher/TestSnapshots) — исполнять ПОСЛЕ Task 8
Task 11 (чек + документ) — независим
Task 12 (финал) — после всех
```

Рекомендуемая последовательность: 1 → 2 → 3 → 4 → 5 → 6 → 7 → 8 → 9 → 10 → 11 → 12.
