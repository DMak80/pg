# t06-ha-api Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** HA-зона AdminPanel: live-пробы (Patroni REST + SQL Npgsql) с фон-оркестратором, обогащение снапшота, эндпоинты `GET /api/ha` и `GET /api/ha/{scope}`, 9 HA-правил алертов.

**Architecture:** Пробы пишут своё состояние в `IProbeStateStore` (интерфейс и модель — в Core); `SnapshotRefresher` вносит состояние в каждый новый снапшот чистым `ProbeEnricher` (единственный писатель `ISnapshotStore` сохранён). Правила алертов и API читают только enriched-снапшот. Зависимости: `Etcd → Core`, `Probes → Core`, стык — типы Core (spec §3.1–3.2).

**Tech Stack:** .NET 10 (`TreatWarningsAsErrors=true`), attribute-DI/`[Config]`-POCO каркаса, Npgsql 10.0.3 (CPM, версия референса Puzzle), xunit v3 + FluentAssertions, Testcontainers (etcd v3.5.21, postgres:18).

**Spec:** `docs/superpowers/2026-08-22-t06-ha-api/spec.md` — план аргументирует от спеки; исполнители читают обе. Ссылки «spec §N» — на неё.

## Global Constraints

- `dotnet build src/AdminPanel.slnx` — 0 warnings (`TreatWarningsAsErrors=true` не подавлять).
- Идентификаторы английские, комментарии в коде — русские; AAA-комментарии тестов русские.
- CPM: версии только в `src/Directory.Packages.props`; новые пакеты — только через CPM.
- Направление зависимостей (arch/01 §1): `Probes → Core`, `Etcd → Core`; Probes не ссылается на AdminPanel.Etcd и наоборот.
- Панель read-only: к PG — только SELECT каталога arch/03 §5 (тексты дословно); к etcd — только чтение; `kv/put` — только тесты.
- SQL-строка пробы всегда несёт `TargetSessionAttributes=ReadWrite`, `Options=-c default_transaction_read_only=on`, `Application Name=adminpanel` (spec §3.6).
- Пороги алертов: `ReplicaLagBytes` = 16 МБ (`replica-lag-high` И `slot-lag-high`), `SlotSafeWalSizeBytes` = 1 GiB (spec §3.8); фолбэк `<= 0` → константы правил.
- `AlertEngine`/`IAlertEngine`/`AlertContext`/`SnapshotBuilder`/`Program.cs`/фабрика `"api"` — без правок (spec §15.5).
- Все команды выполнять из корня worktree: `/Users/demakaev/ZCodeProject/worktrees/feat-t06-ha-api`.
- Коммит после каждой задачи (feature-ветка); roadmap-файл `arch/roadmap/ha.md` и `docs/superpowers/2026-08-22-t06-ha-api/` не коммитить до Task 12 (финальный коммит).

---

### Task 1: Core-стык — ProbeState, ISnapshotReader, ProbeEnricher

**Files:**
- Create: `src/AdminPanel.Core/ISnapshotReader.cs`
- Create: `src/AdminPanel.Core/ProbeState.cs`
- Create: `src/AdminPanel.Core/ProbeEnricher.cs`
- Test: `src/tests/AdminPanel.UnitTests/ProbeEnricherTests.cs`

**Interfaces (Produces, для Tasks 2, 6, 10):**
- `AdminPanel.Core.ISnapshotReader { EtcdSnapshot? Current { get; } }`
- `AdminPanel.Core.HaMemberProbe(string? Role, string? State, long? Timeline, long? LagBytes, DateTimeOffset AtUtc, string? Error)`
- `AdminPanel.Core.ProbeState(DateTimeOffset AtUtc, IReadOnlyList<ProbeResult> Probes, IReadOnlyDictionary<string, HaMemberProbe> Members, IReadOnlyDictionary<string, ShardRuntime> Runtimes)` — ключи Members `"<scope>/<member>"`, Runtimes `"<cluster>/<shard>"`
- `AdminPanel.Core.IProbeStateStore { ProbeState? Current { get; } void Replace(ProbeState state); }`
- `AdminPanel.Core.ProbeEnricher.Apply(EtcdSnapshot snapshot, ProbeState? state) → EtcdSnapshot`

**Вход:** t05 в дереве; `EtcdSnapshot`/`HaScope`/`HaMember`/`ShardInfo`/`ShardRuntime`/`ProbeResult` (t03) на месте; `TestSnapshots.Healthy/FixedTimeProvider` в юнит-тестах.

- [ ] **Step 1: Написать failing-тесты (ProbeEnricherTests.cs, 5 тестов)**

```csharp
using AdminPanel.Core;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Слияние состояния проб со снапшотом (spec §4.2, §10.6): обогащение членов,
// runtime шардов, перенос Probes; null-состояние и лишние ключи безопасны.
public class ProbeEnricherTests
{
    private static readonly DateTimeOffset Now = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static HaMember Member(string name, string? role = "replica", string? state = "streaming") =>
        new(name, name, 5432, role, state, null, null, null, null);

    private static EtcdSnapshot SnapshotWithScope() => TestSnapshots.Healthy(Now) with
    {
        HaScopes =
        [
            new HaScope("demo-s1", "demo", "s1", true, "s1a", null, true,
                [Member("s1a", "master", "running"), Member("s1b")], null),
        ],
    };

    [Fact]
    public void Apply_NullState_NoChange()
    {
        // Arrange
        var snapshot = SnapshotWithScope();

        // Act
        var result = ProbeEnricher.Apply(snapshot, null);

        // Assert: тиков проб не было — снапшот не тронут (Probes уже пуст, t03).
        result.Should().BeSameAs(snapshot);
        result.HaScopes.Single().Members.Single(m => m.Name == "s1b").LagBytes.Should().BeNull();
    }

    [Fact]
    public void Apply_SuccessfulProbe_OverridesMemberFields()
    {
        // Arrange
        var snapshot = SnapshotWithScope();
        var state = new ProbeState(
            Now, [],
            new Dictionary<string, HaMemberProbe>
            {
                ["demo-s1/s1b"] = new("replica", "streaming", 2L, 12345L, Now, null),
            },
            []);

        // Act
        var result = ProbeEnricher.Apply(snapshot, state);

        // Assert: REST перекрывает DCS-поля, probeError снят (spec §3.5).
        var member = result.HaScopes.Single().Members.Single(m => m.Name == "s1b");
        member.Timeline.Should().Be(2L);
        member.LagBytes.Should().Be(12345L);
        member.ProbeAtUtc.Should().Be(Now);
        member.ProbeError.Should().BeNull();
    }

    [Fact]
    public void Apply_FailedProbe_KeepsDcsRoleState()
    {
        // Arrange
        var snapshot = SnapshotWithScope();
        var state = new ProbeState(
            Now,
            [new ProbeResult("demo-s1/s1b", "patroni", false, 5.0, "connection refused", Now)],
            new Dictionary<string, HaMemberProbe>
            {
                ["demo-s1/s1b"] = new(null, null, null, null, Now, "connection refused"),
            },
            []);

        // Act
        var result = ProbeEnricher.Apply(snapshot, state);

        // Assert: etcd-часть HA остаётся, лаги не показываем (spec §3.5).
        var member = result.HaScopes.Single().Members.Single(m => m.Name == "s1b");
        member.Role.Should().Be("replica");
        member.State.Should().Be("streaming");
        member.Timeline.Should().BeNull();
        member.LagBytes.Should().BeNull();
        member.ProbeAtUtc.Should().Be(Now);
        member.ProbeError.Should().Be("connection refused");
        result.Probes.Should().ContainSingle().Which.Ok.Should().BeFalse();
    }

    [Fact]
    public void Apply_SetsRuntimeAndProbes()
    {
        // Arrange
        var snapshot = TestSnapshots.Healthy(Now);
        var runtime = new ShardRuntime("s1", [], [], [], ["bucket_0"], false, null);
        var state = new ProbeState(
            Now,
            [new ProbeResult("demo/s1", "sql", true, 12.0, null, Now)],
            [],
            new Dictionary<string, ShardRuntime> { ["demo/s1"] = runtime });

        // Act
        var result = ProbeEnricher.Apply(snapshot, state);

        // Assert: runtime по ключу кластер/шард; Probes = список состояния (spec §4.2).
        result.Clusters.Single().Shards.Single().Runtime.Should().BeSameAs(runtime);
        result.Probes.Should().ContainSingle().Which.Target.Should().Be("demo/s1");
    }

    [Fact]
    public void Apply_StaleTargetsIgnored()
    {
        // Arrange: состояние ссылается на исчезнувшие скоп/шард — лишние ключи не падают.
        var snapshot = SnapshotWithScope();
        var state = new ProbeState(
            Now, [],
            new Dictionary<string, HaMemberProbe> { ["gone-scope/gone"] = new("replica", "streaming", 1L, 0L, Now, null) },
            new Dictionary<string, ShardRuntime> { ["demo/gone"] = new("gone", [], [], [], [], false, null) });

        // Act
        var result = ProbeEnricher.Apply(snapshot, state);

        // Assert: снапшот валиден, поля посторонних ключей не проявились.
        result.HaScopes.Single().Members.Should().OnlyContain(m => m.ProbeAtUtc is null);
        result.Clusters.Single().Shards.Single().Runtime.Should().BeNull();
    }
}
```

- [ ] **Step 2: Прогнать тесты — убедиться в ошибке компиляции**

Run: `dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~AdminPanel.UnitTests.ProbeEnricherTests" --no-restore`
Expected: FAIL (CS0246 `ProbeEnricher`/`ProbeState` не определены).

- [ ] **Step 3: Реализовать ISnapshotReader.cs, ProbeState.cs, ProbeEnricher.cs**

`src/AdminPanel.Core/ISnapshotReader.cs`:

```csharp
namespace AdminPanel.Core;

// Доступ к текущему снапшоту для модулей вне Etcd (пробы t06): Probes → Core, не → Etcd
// (направление зависимостей arch/01 §1; spec §4.3). Реализует Etcd-стор (ISnapshotStore).
public interface ISnapshotReader
{
    EtcdSnapshot? Current { get; }
}
```

`src/AdminPanel.Core/ProbeState.cs`:

```csharp
namespace AdminPanel.Core;

// Результат Patroni-пробы одного члена: обогащение HaMember + статус попытки (spec §4.1).
public sealed record HaMemberProbe(
    string? Role,
    string? State,
    long? Timeline,
    long? LagBytes,
    DateTimeOffset AtUtc,
    string? Error);

// Состояние одного тика проб (arch/02 §4): пишет ProbeOrchestrator, читает SnapshotRefresher.
public sealed record ProbeState(
    DateTimeOffset AtUtc,
    IReadOnlyList<ProbeResult> Probes,                    // все попытки тика, ok и error
    IReadOnlyDictionary<string, HaMemberProbe> Members,   // ключ "<scope>/<member>"
    IReadOnlyDictionary<string, ShardRuntime> Runtimes); // ключ "<cluster>/<shard>"

// Стор состояния проб: атомарная замена ссылки — зеркалит ISnapshotStore (spec §4.9).
public interface IProbeStateStore
{
    ProbeState? Current { get; }

    void Replace(ProbeState state);
}
```

`src/AdminPanel.Core/ProbeEnricher.cs`:

```csharp
namespace AdminPanel.Core;

// Внесение результатов проб в свежий снапшот (arch/02 §4 п.3; spec §4.2): члены HA
// обогащаются REST-полями, шардам ставится Runtime, Probes — последним тиком проб.
// Чистая функция; лишние ключи состояния (цель исчезла из etcd) игнорируются (spec §3.5).
public static class ProbeEnricher
{
    public static EtcdSnapshot Apply(EtcdSnapshot snapshot, ProbeState? state)
    {
        if (state is null)
            return snapshot; // тиков не было — снапшот уже собран с пустыми Probes/Runtime

        var scopes = state.Members.Count == 0
            ? snapshot.HaScopes
            : [.. snapshot.HaScopes.Select(scope => scope with
            {
                Members = [.. scope.Members.Select(member => MergeMember(scope.Scope, member, state))],
            })];

        var clusters = state.Runtimes.Count == 0
            ? snapshot.Clusters
            : [.. snapshot.Clusters.Select(cluster => cluster with
            {
                Shards = [.. cluster.Shards.Select(shard => MergeRuntime(cluster.Name, shard, state))],
            })];

        return snapshot with { HaScopes = scopes, Clusters = clusters, Probes = state.Probes };
    }

    // Успех: REST перекрывает role/state/timeline/lag, ошибка снята; отказ: DCS-часть
    // остаётся, лаги не показываем, фиксируем время и текст ошибки (spec §3.5).
    private static HaMember MergeMember(string scope, HaMember member, ProbeState state)
        => state.Members.TryGetValue($"{scope}/{member.Name}", out var probe)
            ? member with
            {
                Role = probe.Role ?? member.Role,
                State = probe.State ?? member.State,
                Timeline = probe.Timeline,
                LagBytes = probe.LagBytes,
                ProbeAtUtc = probe.AtUtc,
                ProbeError = probe.Error,
            }
            : member;

    private static ShardInfo MergeRuntime(string cluster, ShardInfo shard, ProbeState state)
        => state.Runtimes.TryGetValue($"{cluster}/{shard.Name}", out var runtime)
            ? shard with { Runtime = runtime }
            : shard;
}
```

- [ ] **Step 4: Прогнать тесты — все зелёные**

Run: `dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~AdminPanel.UnitTests.ProbeEnricherTests"`
Expected: PASS, 5 passed.

- [ ] **Step 5: Полная сборка**

Run: `dotnet build src/AdminPanel.slnx`
Expected: успех, 0 warnings.

- [ ] **Step 6: Commit**

```bash
git add src/AdminPanel.Core/ISnapshotReader.cs src/AdminPanel.Core/ProbeState.cs src/AdminPanel.Core/ProbeEnricher.cs src/tests/AdminPanel.UnitTests/ProbeEnricherTests.cs
git commit -m "t06: core — ProbeState/IProbeStateStore/ISnapshotReader + ProbeEnricher (spec §4.1–4.3)"
```

**Выход:** типы стыка Core компилируются, 5 юнит-тестов зелёные.

---

### Task 2: Etcd-стык — SnapshotStore под двумя интерфейсами, refresher обогащает снапшот

**Files:**
- Modify: `src/AdminPanel.Etcd/SnapshotStore.cs` (весь файл — 25 строк)
- Modify: `src/AdminPanel.Etcd/SnapshotRefresher.cs:18-24` (ctor), `:133-141` (enrich), `:196-205` (FailTick Probes)
- Modify: `src/tests/AdminPanel.UnitTests/SnapshotRefresherTests.cs:11-22` (harness + double), `#конец файла` (+2 теста)

**Interfaces:**
- Consumes: `IProbeStateStore`, `ProbeEnricher.Apply` (Task 1).
- Produces: `SnapshotRefresher(IEtcdGateway, IAlertEngine, ISnapshotStore, IProbeStateStore, IOptions<EtcdOptions>, TimeProvider, ILogger<SnapshotRefresher>)` — сигнатура для харнессов Tasks 10–11; `ISnapshotStore : ISnapshotReader`; `RefresherTestHarness.New(gateway, store, probes, params endpoints)`; unit `SettableProbeStateStore`.

**Вход:** Task 1 слит; текущий ctor refresher'а: `(IEtcdGateway gateway, IAlertEngine alertEngine, ISnapshotStore store, IOptions<EtcdOptions> options, TimeProvider time, ILogger<SnapshotRefresher> logger)`; `FailTick` создаёт снапшот с `[],` на позиции Probes.

- [ ] **Step 1: Написать failing-тесты (2 новых в конец SnapshotRefresherTests.cs + double + перегрузка harness)**

В файл `SnapshotRefresherTests.cs`: класс-double на уровне namespace (рядом с `RefresherTestHarness`), перегрузку — внутрь `RefresherTestHarness`; существующий метод `New(FakeEtcdGateway, ISnapshotStore, params string[])` оставить как обёртку — существующие вызовы не ломаются:

```csharp
// Управляемый стор состояния проб (unit-аналог TestSnapshotStore; spec §10.8).
internal sealed class SettableProbeStateStore : IProbeStateStore
{
    public ProbeState? Current { get; set; }

    public void Replace(ProbeState state) => Current = state;
}

internal static class RefresherTestHarness
{
    // Старая сигнатура (обёртка) — существующие вызовы не меняются.
    public static SnapshotRefresher New(FakeEtcdGateway gateway, ISnapshotStore store, params string[] endpoints)
        => New(gateway, store, null, endpoints);

    // Расширенная: с стором проб (spec §10.8 — конструктор refresher'а t06).
    public static SnapshotRefresher New(
        FakeEtcdGateway gateway,
        ISnapshotStore store,
        SettableProbeStateStore? probes,
        params string[] endpoints)
        => new(
            gateway,
            new AlertEngine(AlertTestRules.All()),
            store,
            probes ?? new SettableProbeStateStore(),
            Options.Create(new EtcdOptions { Endpoints = endpoints }),
            new FixedTimeProvider(),
            NullLogger<SnapshotRefresher>.Instance);
}
```

В конец класса `SnapshotRefresherTests` — 2 новых теста и хелпер:

```csharp
    // Минимальный gateway с /service/ demo-s1 и шардем demo/s1 (spec §10.8).
    private static FakeEtcdGateway HaGateway() => new()
    {
        ClustersKv =
        [
            new Kv("/clusters/demo/config", "{\"buckets\":16,\"dbname\":\"demo\",\"created_unix\":1755800000}", 1),
            new Kv("/clusters/demo/shards/s1/dsn", "host=s1a port=5432 dbname=demo user=postgres", 2),
        ],
        ServiceKv =
        [
            new Kv("/service/demo-s1/leader", "{\"name\":\"s1a\"}", 3),
            new Kv("/service/demo-s1/members/s1a", "{\"name\":\"s1a\",\"conn_url\":\"s1a:5432\",\"role\":\"master\",\"state\":\"running\"}", 4),
        ],
    };

    private static readonly DateTimeOffset ProbesAt = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task Refresh_EnrichesFromProbeState()
    {
        // Arrange: стор проб с member-обогащением и runtime шарда.
        var store = new SnapshotStore();
        var probes = new SettableProbeStateStore
        {
            Current = new ProbeState(
                ProbesAt,
                [],
                new Dictionary<string, HaMemberProbe>
                {
                    ["demo-s1/s1a"] = new("master", "running", 2L, 123L, ProbesAt, null),
                },
                new Dictionary<string, ShardRuntime>
                {
                    ["demo/s1"] = new("s1", [], [], [], ["bucket_0"], false, null),
                }),
        };
        var refresher = RefresherTestHarness.New(HaGateway(), store, probes, "http://etcd:2379");

        // Act
        await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert: член обогащён, runtime проставлен (spec §4.2 через refresher).
        var member = store.Current!.HaScopes.Single(s => s.Scope == "demo-s1").Members.Single(m => m.Name == "s1a");
        member.Timeline.Should().Be(2L);
        member.LagBytes.Should().Be(123L);
        member.ProbeAtUtc.Should().Be(ProbesAt);
        var shard = store.Current.Clusters.Single().Shards.Single();
        shard.Runtime.Should().NotBeNull();
        shard.Runtime!.BucketSchemas.Should().ContainSingle().Which.Should().Be("bucket_0");
    }

    [Fact]
    public async Task Refresh_FailTick_PreservesProbes()
    {
        // Arrange: снапшот с живым проб-результатом; все endpoints мертвы.
        var probe = new ProbeResult("demo-s1/s1a", "patroni", true, 5.0, null, ProbesAt);
        var store = new SnapshotStore();
        store.Replace(TestSnapshots.Healthy(ProbesAt) with { Probes = [probe] });
        var gateway = new FakeEtcdGateway();
        gateway.StatusFailEndpoints.Add("http://etcd:2379"); // свойство get-only — наполняется (CS8852 на object initializer)
        var refresher = RefresherTestHarness.New(gateway, store, null, "http://etcd:2379");

        // Act
        await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert: отказ etcd не теряет снапшотные пробы (spec §4.3).
        store.Current!.Probes.Should().ContainSingle().Which.Should().BeSameAs(probe);
    }
```

- [ ] **Step 2: Прогнать — компиляция падает на новом ctor**

Run: `dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~AdminPanel.UnitTests.SnapshotRefresherTests" --no-restore`
Expected: FAIL (CS7036: у `SnapshotRefresher` нет конструктора с 7 аргументами).

- [ ] **Step 3: Правки SnapshotStore.cs**

Заменить интерфейс и атрибут (реализация класса не меняется):

```csharp
// Хранилище текущего снапшота: читатели никогда не блокируются (arch/01 §1).
public interface ISnapshotStore : ISnapshotReader
{
    // До первого тика снапшота нет — потребители (t04) показывают «загрузка» (spec §3.13).
    EtcdSnapshot? Current { get; }

    // Атомарная замена ссылки; писатель один — SnapshotRefresher (arch/01 §1).
    void Replace(EtcdSnapshot snapshot);
}

[InjectAsSingleton(typeof(ISnapshotStore), typeof(ISnapshotReader))]
public sealed class SnapshotStore : ISnapshotStore
{
    private volatile EtcdSnapshot? _current;

    public EtcdSnapshot? Current => _current;

    public void Replace(EtcdSnapshot snapshot) => _current = snapshot;
}
```

- [ ] **Step 4: Правки SnapshotRefresher.cs (3 точки)**

4a. Primary-конструктор — вставить `IProbeStateStore probeStateStore,` после `ISnapshotStore store,`:

```csharp
public sealed class SnapshotRefresher(
    IEtcdGateway gateway,
    IAlertEngine alertEngine,
    ISnapshotStore store,
    IProbeStateStore probeStateStore,
    IOptions<EtcdOptions> options,
    TimeProvider time,
    ILogger<SnapshotRefresher> logger) : BackgroundService, IHealthCheckService
```

4b. Успешный тик — обогащение перед алертами (заменить блок `var built = SnapshotBuilder.Build(...); store.Replace(built with {...})`):

```csharp
        // 6. Сборка + внесение проб (arch/02 §4 п.3) + алерты + атомарная замена
        // (arch/02 §4 п.4–5; Alerts на обоих путях тика, spec §5; spec §3.1).
        var built = ProbeEnricher.Apply(
            SnapshotBuilder.Build(
                time, clustersParsed, serviceParsed, nodes,
                etcd.Members, etcd.Alarms, etcd),
            probeStateStore.Current);
        store.Replace(built with
        {
            Alerts = alertEngine.Evaluate(built, previous, now, EffectiveIntervalSeconds()),
        });
        return Finish(Result.Success(), working: true);
```

4c. `FailTick` — Probes сохраняются (в `new EtcdSnapshot(...)` заменить шестой позиционный аргумент `[]` на `previous?.Probes ?? []`):

```csharp
        var failed = new EtcdSnapshot(
            previous?.BuiltAtUtc ?? now,
            etcd,
            previous?.Clusters ?? [],
            previous?.HaScopes ?? [],
            previous?.StandNodes ?? [],
            previous?.Probes ?? [],   // t06: пробы — часть снапшота, отказ etcd их не теряет (spec §4.3)
            [],
            previous?.ParseErrors ?? [],
            previous?.UnknownKeyCount ?? 0);
```

- [ ] **Step 5: Прогнать тесты**

Run: `dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~AdminPanel.UnitTests.SnapshotRefresherTests"`
Expected: PASS — все существующие (обёртка-перегрузка сохраняет вызовы) + `Refresh_EnrichesFromProbeState`, `Refresh_FailTick_PreservesProbes`.

- [ ] **Step 6: Полная сборка + Commit**

Run: `dotnet build src/AdminPanel.slnx` — успех, 0 warnings.

```bash
git add src/AdminPanel.Etcd/SnapshotStore.cs src/AdminPanel.Etcd/SnapshotRefresher.cs src/tests/AdminPanel.UnitTests/SnapshotRefresherTests.cs
git commit -m "t06: etcd — refresher обогащает снапшот из IProbeStateStore, ISnapshotStore: ISnapshotReader (spec §3.1–3.2, §4.3)"
```

**Выход:** refresher (единственный писатель) вносит состояние проб; FailTick сохраняет Probes; 2 новых теста зелёные.

---

### Task 3: ProbesOptions, HostMapResolver, пакеты, appsettings

**Files:**
- Create: `src/AdminPanel.Probes/ProbesOptions.cs`
- Create: `src/AdminPanel.Probes/HostMapResolver.cs`
- Modify: `src/AdminPanel.Probes/AdminPanel.Probes.csproj` (весь файл)
- Modify: `src/Directory.Packages.props` (+1 строка)
- Modify: `src/AdminPanel.Api/appsettings.json` (секции AdminPanel)
- Test: `src/tests/AdminPanel.UnitTests/HostMapResolverTests.cs`

**Interfaces (Produces, для Tasks 4–6):**
- `AdminPanel.Probes.ProbesOptions { bool PatroniEnabled=true; bool SqlEnabled=true; double IntervalSeconds=15; double TimeoutSeconds=3; string Password=""; Dictionary<string,string> HostMap=[] }`, `[Config("AdminPanel:Probes")]`
- `AdminPanel.Probes.HostMapResolver.Resolve(IReadOnlyDictionary<string,string> hostMap, string host, int port) → string` (полный `host:port`)

**Вход:** Task 1–2 слиты; `AdminPanel.Probes` — только `ModuleExtensions.AddProbes()`; CPM без Npgsql.

- [ ] **Step 1: Написать failing-тесты (HostMapResolverTests.cs, 4 теста)**

```csharp
using AdminPanel.Probes;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Резолвер адресов проб: точное совпадение host:port → override, иначе адрес
// без изменений; порт — часть ключа (arch/02 §6, spec §10.4).
public class HostMapResolverTests
{
    [Fact]
    public void Resolve_ExactMatch_Overrides()
    {
        // Arrange
        var map = new Dictionary<string, string> { ["s1a:8008"] = "127.0.0.1:8011" };

        // Act
        var resolved = HostMapResolver.Resolve(map, "s1a", 8008);

        // Assert
        resolved.Should().Be("127.0.0.1:8011");
    }

    [Fact]
    public void Resolve_NoMatch_Identity()
    {
        // Arrange
        var map = new Dictionary<string, string> { ["s1a:5432"] = "127.0.0.1:5433" };

        // Act
        var resolved = HostMapResolver.Resolve(map, "s1a", 8008);

        // Assert: нет точного совпадения — адрес из etcd используется как есть.
        resolved.Should().Be("s1a:8008");
    }

    [Fact]
    public void Resolve_EmptyMap_Identity()
    {
        // Arrange — прод: HostMap пуст (arch/01 §6).
        // Act
        var resolved = HostMapResolver.Resolve([], "pg1", 5432);

        // Assert
        resolved.Should().Be("pg1:5432");
    }

    [Fact]
    public void Resolve_DifferentPort_NotMatched()
    {
        // Arrange: карта знает другой порт того же хоста — порт часть ключа.
        var map = new Dictionary<string, string> { ["s1a:5432"] = "127.0.0.1:5433" };

        // Act
        var resolved = HostMapResolver.Resolve(map, "s1a", 5433);

        // Assert
        resolved.Should().Be("s1a:5433");
    }
}
```

- [ ] **Step 2: Прогнать — ошибка компиляции**

Run: `dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~AdminPanel.UnitTests.HostMapResolverTests" --no-restore`
Expected: FAIL (CS0234 `AdminPanel.Probes` не содержит `HostMapResolver`).

- [ ] **Step 3: Пакеты и настройки**

3a. `src/Directory.Packages.props` — добавить строку в `<ItemGroup>` (по алфавиту после `Microsoft.NET.Test.Sdk`):

```xml
    <PackageVersion Include="Npgsql" Version="10.0.3" />
```

3b. `src/AdminPanel.Probes/AdminPanel.Probes.csproj` — заменить содержимое целиком:

```xml
<Project Sdk="Microsoft.NET.Sdk">

    <ItemGroup>
        <PackageReference Include="Microsoft.Extensions.Hosting.Abstractions" />
        <PackageReference Include="Microsoft.Extensions.Http" />
        <PackageReference Include="Npgsql" />
        <ProjectReference Include="..\AdminPanel.Core\AdminPanel.Core.csproj"/>
    </ItemGroup>

</Project>
```

3c. `src/AdminPanel.Api/appsettings.json` — внутрь `"AdminPanel"` добавить после `"Alerts"`:

```json
    "Probes": {
      "PatroniEnabled": true,
      "SqlEnabled": true,
      "IntervalSeconds": 15,
      "TimeoutSeconds": 3,
      "Password": "",
      "HostMap": {}
    }
```

- [ ] **Step 4: Реализовать ProbesOptions.cs и HostMapResolver.cs**

`src/AdminPanel.Probes/ProbesOptions.cs`:

```csharp
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Probes;

// [Config]-POCO live-проб: секция AdminPanel:Probes (arch/01 §6, arch/02 §6; spec §4.4).
// Суффикс Seconds — прецедент EtcdOptions (t03 §3.3).
[Config("AdminPanel:Probes")]
public class ProbesOptions
{
    // Patroni REST :8008/cluster — включена по умолчанию (arch/02 §6.1).
    public bool PatroniEnabled { get; set; } = true;

    // SQL-проба Npgsql — включена по умолчанию; в проде — на усмотрение (arch/02 §6.2).
    public bool SqlEnabled { get; set; } = true;

    // Тик оркестратора (arch/02 §4). <= 0 — fallback 15 c с LogWarning.
    public double IntervalSeconds { get; set; } = 15;

    // Таймаут одной пробы: HTTP-запрос / connection+command SQL (arch/01 §6). <= 0 — 3 c.
    public double TimeoutSeconds { get; set; } = 3;

    // Пароль SQL-проб: в DSN из etcd пароля нет никогда (arch/02 §2.1); пусто —
    // ключ не попадает в строку (стенд trust, arch/04 §5). Секрет — env поверх json.
    public string Password { get; set; } = "";

    // «etcd-адрес ноды host:port» → «адрес, достижимый с хоста панели» (arch/02 §6):
    // точное совпадение ключа, иначе адрес без изменений; по умолчанию пуст (прод).
    public Dictionary<string, string> HostMap { get; set; } = [];
}
```

`src/AdminPanel.Probes/HostMapResolver.cs`:

```csharp
namespace AdminPanel.Probes;

// Разрешение адреса цели пробы: адрес из etcd → override при точном совпадении
// host:port → прямое подключение к полученному адресу (arch/02 §6, spec §4.5).
// Значения карты — полные "host:port"; чистая функция — unit-тестируется без сети.
public static class HostMapResolver
{
    public static string Resolve(IReadOnlyDictionary<string, string> hostMap, string host, int port)
        => hostMap.TryGetValue($"{host}:{port}", out var mapped) ? mapped : $"{host}:{port}";
}
```

- [ ] **Step 5: Прогнать тесты + сборку**

Run: `dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~AdminPanel.UnitTests.HostMapResolverTests"`
Expected: PASS, 4 passed.
Run: `dotnet build src/AdminPanel.slnx` — успех, 0 warnings.

- [ ] **Step 6: Commit**

```bash
git add src/Directory.Packages.props src/AdminPanel.Probes/ProbesOptions.cs src/AdminPanel.Probes/HostMapResolver.cs src/AdminPanel.Probes/AdminPanel.Probes.csproj src/AdminPanel.Api/appsettings.json src/tests/AdminPanel.UnitTests/HostMapResolverTests.cs
git commit -m "t06: probes — ProbesOptions/HostMapResolver + Npgsql 10.0.3 в CPM + appsettings (spec §4.4–4.5, §12)"
```

**Выход:** опции и резолвер проб готовы; Npgsql в решении.

---

### Task 4: PatroniClusterParser + PatroniRestProbe + HttpClient "patroni"

**Files:**
- Create: `src/AdminPanel.Probes/PatroniClusterParser.cs`
- Create: `src/AdminPanel.Probes/PatroniRestProbe.cs`
- Modify: `src/AdminPanel.Probes/ModuleExtensions.cs` (весь файл)
- Create: `src/tests/AdminPanel.UnitTests/ProbesFixtures/patroni-cluster.json`
- Modify: `src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj` (+None)
- Test: `src/tests/AdminPanel.UnitTests/PatroniClusterParserTests.cs`

**Interfaces:**
- Consumes: `ProbesOptions`, `HostMapResolver` (Task 3); `HaScope`/`HaMember`/`HaMemberProbe`/`ProbeResult` (Core).
- Produces (для Task 6, 11): `PatroniClusterMember(string? Name, string? Role, string? State, long? Timeline, long? LagBytes)`; `PatroniClusterParser.Parse(string json) → IReadOnlyList<PatroniClusterMember>`; `PatroniMemberResult(HaMemberProbe Enrichment, ProbeResult Result)`; `IPatroniRestProbe.ProbeAsync(HaScope scope, HaMember member, CancellationToken ct) → Task<PatroniMemberResult>`; `PatroniRestProbe.HttpClientName = "patroni"`.

**Вход:** Task 3 слит; typed-HttpClient паттерн — `AdminPanel.Etcd/ModuleExtensions.cs` (t03).

- [ ] **Step 1: Фикстура + csproj**

`src/tests/AdminPanel.UnitTests/ProbesFixtures/patroni-cluster.json` — реальный фрагмент Patroni `/cluster` (pg-report §4; spec §10.3):

```json
{
  "members": [
    { "name": "s1a", "host": "10.0.0.11", "port": 5432, "role": "master", "state": "running", "timeline": 1, "lag": 0 },
    { "name": "s1b", "host": "10.0.0.12", "port": 5432, "role": "replica", "state": "streaming", "timeline": 1, "lag": 52428800 },
    { "name": "s1c", "host": "10.0.0.13", "port": 5432, "role": "replica", "state": "stopped", "timeline": 1, "lag": null }
  ]
}
```

`AdminPanel.UnitTests.csproj` — рядом с существующим None-ItemGroup добавить:

```xml
    <ItemGroup>
        <None Include="ProbesFixtures\**\*.json" CopyToOutputDirectory="PreserveNewest"/>
    </ItemGroup>
```

- [ ] **Step 2: Написать failing-тесты (PatroniClusterParserTests.cs, 3 теста)**

```csharp
using System.Text.Json;
using AdminPanel.Probes;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Парсер ответа Patroni GET /cluster (arch/02 §6.1, §8: реальные фрагменты +
// вырожденные — отсутствующие поля, null-лаг, строковые числа; spec §10.3).
public class PatroniClusterParserTests
{
    private static string LoadFixture()
        => File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "ProbesFixtures", "patroni-cluster.json"));

    [Fact]
    public void Parse_FullFixture_AllMembers()
    {
        // Arrange
        var json = LoadFixture();

        // Act
        var members = PatroniClusterParser.Parse(json);

        // Assert
        members.Should().HaveCount(3);
        var replica = members.Single(m => m.Name == "s1b");
        replica.Role.Should().Be("replica");
        replica.State.Should().Be("streaming");
        replica.Timeline.Should().Be(1L);
        replica.LagBytes.Should().Be(52428800L);
        members.Single(m => m.Name == "s1c").LagBytes.Should().BeNull(); // null-лаг толерантен
    }

    [Fact]
    public void Parse_Tolerant_MissingFieldsAndStringNumbers()
    {
        // Arrange — нет state/timeline/lag, числа строками (строгий Patroni их не шлёт,
        // но шлёт эмулятор стенда; толерантность — arch/02 §8).
        const string json = """
            {"members":[{"name":"x","role":"replica","timeline":"2","lag":"100"}]}
            """;

        // Act
        var members = PatroniClusterParser.Parse(json);

        // Assert
        var member = members.Should().ContainSingle().Subject;
        member.State.Should().BeNull();
        member.Timeline.Should().Be(2L);
        member.LagBytes.Should().Be(100L);
    }

    [Fact]
    public void Parse_BrokenJson_Throws()
    {
        // Arrange — мусор парсер не глотает: ошибку ловит проба (spec §10.3).
        const string json = "not json at all";

        // Act
        var act = () => PatroniClusterParser.Parse(json);

        // Assert
        act.Should().Throw<JsonException>();
    }
}
```

- [ ] **Step 3: Прогнать — ошибка компиляции**

Run: `dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~AdminPanel.UnitTests.PatroniClusterParserTests" --no-restore`
Expected: FAIL (CS0103 `PatroniClusterParser` не найден).

- [ ] **Step 4: Реализовать PatroniClusterParser.cs**

```csharp
using System.Text.Json;
using System.Text.Json.Serialization;

namespace AdminPanel.Probes;

// Распаренный член ответа GET /cluster (Patroni-формат, arch/02 §6.1; spec §4.6).
public sealed record PatroniClusterMember(
    string? Name,
    string? Role,
    string? State,
    long? Timeline,
    long? LagBytes);

// Парсер JSON ответа Patroni /cluster: {"members":[{name,role,state,timeline,lag,…},…]}.
// Толерантен: отсутствующие поля, null-лаг, строковые числа (arch/02 §8).
public static class PatroniClusterParser
{
    // AllowReadingFromString — тот же приём, что EtcdGateway для decimal-строк etcd (t03 §4.2).
    private static readonly JsonSerializerOptions Json = new()
    {
        NumberHandling = JsonNumberHandling.AllowReadingFromString,
        PropertyNameCaseInsensitive = true,
    };

    public static IReadOnlyList<PatroniClusterMember> Parse(string json)
    {
        var response = JsonSerializer.Deserialize<ClusterResponse>(json, Json);
        return response?.Members ?? [];
    }

    private sealed class ClusterResponse
    {
        [JsonPropertyName("members")]
        public List<MemberDto>? Members { get; set; }
    }

    private sealed class MemberDto
    {
        [JsonPropertyName("name")]
        public string? Name { get; set; }

        [JsonPropertyName("role")]
        public string? Role { get; set; }

        [JsonPropertyName("state")]
        public string? State { get; set; }

        [JsonPropertyName("timeline")]
        public long? Timeline { get; set; }

        [JsonPropertyName("lag")]
        public long? Lag { get; set; }
    }
}
```

- [ ] **Step 5: Прогнать тесты парсера**

Run: `dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~AdminPanel.UnitTests.PatroniClusterParserTests"`
Expected: PASS, 3 passed.

- [ ] **Step 6: Реализовать PatroniRestProbe.cs**

```csharp
using System.Diagnostics;
using AdminPanel.Core;
using AdminPanel.Infrastructure.DI;
using Microsoft.Extensions.Options;

namespace AdminPanel.Probes;

// Результат Patroni-пробы одного члена: обогащение HaMember + статус попытки (spec §4.6).
public sealed record PatroniMemberResult(HaMemberProbe Enrichment, ProbeResult Result);

// Проба члена HA-скопа: GET http://<host>:8008/cluster (arch/02 §6.1).
public interface IPatroniRestProbe
{
    Task<PatroniMemberResult> ProbeAsync(HaScope scope, HaMember member, CancellationToken ct);
}

// Запись member'а отсутствует в ответе /cluster — ошибка пробы (spec §3.4).
public sealed class PatroniProbeException(string message) : Exception(message);

// Реализация: typed HttpClient "patroni" (таймаут из ProbesOptions — ModuleExtensions,
// паттерн EtcdGateway t03); адрес host:8008 прогоняется через HostMap (§3.6);
// из ответа берётся запись name == member.Name (§3.4); User-Agent — §3.22.
[InjectAsSingleton(typeof(IPatroniRestProbe))]
public sealed class PatroniRestProbe(
    HttpClient httpClient,
    IOptions<ProbesOptions> options,
    TimeProvider time) : IPatroniRestProbe
{
    public const string HttpClientName = "patroni";

    // Порт Patroni REST — стандарт :8008 (arch/02 §6.1; PG-порт member'а не используется).
    private const int RestPort = 8008;

    public async Task<PatroniMemberResult> ProbeAsync(HaScope scope, HaMember member, CancellationToken ct)
    {
        var url = $"http://{HostMapResolver.Resolve(options.Value.HostMap, member.Host, RestPort)}/cluster";
        var started = Stopwatch.GetTimestamp();
        var at = time.GetUtcNow();
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, url);
            // Идентификация панели в access-логах Patroni/эмуляторов (spec §3.22).
            request.Headers.UserAgent.TryParseAdd("AdminPanel");
            using var response = await httpClient.SendAsync(request, ct);
            response.EnsureSuccessStatusCode();
            var json = await response.Content.ReadAsStringAsync(ct);
            var latency = Stopwatch.GetElapsedTime(started).TotalMilliseconds;
            var entry = PatroniClusterParser.Parse(json).FirstOrDefault(m => m.Name == member.Name)
                ?? throw new PatroniProbeException(
                    $"member {member.Name} не найден в ответе /cluster scope {scope.Scope}");

            return new PatroniMemberResult(
                new HaMemberProbe(entry.Role, entry.State, entry.Timeline, entry.LagBytes, at, null),
                new ProbeResult($"{scope.Scope}/{member.Name}", "patroni", true, latency, null, at));
        }
        catch (Exception e)
        {
            // Любой отказ (транспорт/HTTP/JSON/отсутствие записи) — ошибка пробы этого
            // члена, не тика: DCS-часть HA остаётся (arch/01 §8, spec §3.5).
            return new PatroniMemberResult(
                new HaMemberProbe(null, null, null, null, at, e.Message),
                new ProbeResult(
                    $"{scope.Scope}/{member.Name}", "patroni", false,
                    Stopwatch.GetElapsedTime(started).TotalMilliseconds, e.Message, at));
        }
    }
}
```

- [ ] **Step 7: ModuleExtensions.cs — HttpClient "patroni" (заменить файл целиком)**

```csharp
using System.Reflection;
using AdminPanel.Infrastructure.DI;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AdminPanel.Probes;

// Модуль live-проб (t06): attribute-DI + именованный HttpClient "patroni" с таймаутом
// из настроек — паттерн и порядок регистраций Etcd ModuleExtensions (t03).
public static class ModuleExtensions
{
    private static Assembly Assembly => typeof(ModuleExtensions).Assembly;

    public static IServiceCollection AddProbes(this IServiceCollection services)
    {
        services.AutoRegistration(Assembly);

        // Порядок важен: AddHttpClient после AutoRegistration — typed-фабрика перекрывает
        // дескриптор автоскана, и PatroniRestProbe получал HttpClient из фабрики (t03 §4).
        services
           .AddHttpClient<PatroniRestProbe>(PatroniRestProbe.HttpClientName)
           .ConfigureHttpClient((sp, client) =>
            {
                var seconds = sp.GetRequiredService<IOptions<ProbesOptions>>().Value.TimeoutSeconds;
                if (seconds <= 0)
                {
                    sp.GetRequiredService<ILogger<PatroniRestProbe>>()
                       .LogWarning("AdminPanel:Probes:TimeoutSeconds <= 0 — использую 3 c");
                    seconds = 3;
                }

                client.Timeout = TimeSpan.FromSeconds(seconds);
            });

        return services;
    }
}
```

- [ ] **Step 8: Сборка + все юнит-тесты Probes + Commit**

Run: `dotnet build src/AdminPanel.slnx && dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~AdminPanel.UnitTests" --no-build`
Expected: сборка 0 warnings; все юнит-тесты зелёные (новые: 3 parser).

```bash
git add src/AdminPanel.Probes/PatroniClusterParser.cs src/AdminPanel.Probes/PatroniRestProbe.cs src/AdminPanel.Probes/ModuleExtensions.cs src/tests/AdminPanel.UnitTests/ProbesFixtures src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj src/tests/AdminPanel.UnitTests/PatroniClusterParserTests.cs
git commit -m "t06: probes — PatroniClusterParser + PatroniRestProbe (:8008/cluster, HostMap, User-Agent) + HttpClient patroni (spec §4.6, §4.10)"
```

**Выход:** Patroni-проба с парсером готовы; live-HTTP покроется Task 11.

---

### Task 5: SqlProbe + построитель строки подключения

**Files:**
- Create: `src/AdminPanel.Probes/SqlProbe.cs`
- Test: `src/tests/AdminPanel.UnitTests/SqlConnectionFactoryTests.cs`

**Interfaces:**
- Consumes: `ProbesOptions`, `HostMapResolver` (Task 3); `ClusterInfo`/`ShardInfo`/`ShardRuntime`/`ReplicationSlotInfo`/`StandbyInfo`/`SubscriptionInfo` (Core).
- Produces (для Task 6, 11): `SqlShardResult(ShardRuntime Runtime, ProbeResult Result)`; `ISqlProbe.ProbeAsync(ClusterInfo cluster, ShardInfo shard, CancellationToken ct) → Task<SqlShardResult>`; `SqlProbe.BuildConnectionString(ShardInfo shard, ProbesOptions options) → NpgsqlConnectionStringBuilder` (public static, для unit-тестов).

**Вход:** Task 3–4 слиты; `ShardRuntime`-модель t03; SQL-тексты arch/03 §5 (дословно, spec §4.7).

- [ ] **Step 1: Написать failing-тесты (SqlConnectionFactoryTests.cs, 4 теста)**

```csharp
using AdminPanel.Core;
using AdminPanel.Probes;
using FluentAssertions;
using Npgsql;
using Xunit;

namespace AdminPanel.UnitTests;

// Построитель Npgsql-строки пробы (spec §3.6, §10.5): HostMap по каждому host:port,
// пароль из настроек, read-only + TargetSessionAttributes, толерантность к null-полям.
public class SqlConnectionFactoryTests
{
    private static ShardInfo Shard() => new(
        "s1", "host=s1a,s1b port=5432 dbname=demo user=postgres",
        ["s1a", "s1b"], 5432, "demo", "postgres", 1, "s1a:5432", null);

    [Fact]
    public void Build_MapsHostsPerEndpoint()
    {
        // Arrange: маппится только первый хост — у остальных порт всё равно явный.
        var options = new ProbesOptions
        {
            HostMap = new Dictionary<string, string> { ["s1a:5432"] = "127.0.0.1:5433" },
        };

        // Act
        var builder = SqlProbe.BuildConnectionString(Shard(), options);

        // Assert: эндпоинт-синтаксис Npgsql host:port у каждого хоста (spec §3.6).
        builder.Host.Should().Be("127.0.0.1:5433,s1b:5432");
    }

    [Fact]
    public void Build_MergesPassword()
    {
        // Arrange
        var withPassword = new ProbesOptions { Password = "secret" };
        var withoutPassword = new ProbesOptions { Password = "" };

        // Act
        var present = SqlProbe.BuildConnectionString(Shard(), withPassword);
        var absent = SqlProbe.BuildConnectionString(Shard(), withoutPassword);

        // Assert: пустой пароль — ключа нет (стенд trust, spec §3.6).
        present.Password.Should().Be("secret");
        absent.Password.Should().BeNullOrEmpty();
    }

    [Fact]
    public void Build_ReadOnlyAndSessionAttributes()
    {
        // Arrange
        var options = new ProbesOptions { TimeoutSeconds = 7 };

        // Act
        var builder = SqlProbe.BuildConnectionString(Shard(), options);

        // Assert: двойная защита от записи + теги панели (arch/02 §6.2, spec §3.6).
        builder.TargetSessionAttributes.Should().Be(NpgsqlTargetSessionAttributes.ReadWrite);
        builder.Options.Should().Be("-c default_transaction_read_only=on");
        builder.ApplicationName.Should().Be("adminpanel");
        builder.Timeout.Should().Be(7);
        builder.CommandTimeout.Should().Be(7);
    }

    [Fact]
    public void Build_NullUserAndDb_Omitted()
    {
        // Arrange: битый DSN без dbname/user (DsnParser отдал null).
        var shard = Shard() with { DbName = null, User = null, Port = null };

        // Act
        var builder = SqlProbe.BuildConnectionString(shard, new ProbesOptions());

        // Assert: ключи не ставятся; порт по умолчанию 5432 в каждом эндпоинте.
        builder.Database.Should().BeNullOrEmpty();
        builder.Username.Should().BeNullOrEmpty();
        builder.Host.Should().Be("s1a:5432,s1b:5432");
    }
}
```

- [ ] **Step 2: Прогнать — ошибка компиляции**

Run: `dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~AdminPanel.UnitTests.SqlConnectionFactoryTests" --no-restore`
Expected: FAIL (CS0117 `SqlProbe` не содержит `BuildConnectionString`).

- [ ] **Step 3: Реализовать SqlProbe.cs**

```csharp
using System.Data;
using System.Globalization;
using AdminPanel.Core;
using AdminPanel.Infrastructure.DI;
using Microsoft.Extensions.Options;
using Npgsql;

namespace AdminPanel.Probes;

// Результат SQL-пробы одного шарда: runtime + статус попытки (spec §4.7).
public sealed record SqlShardResult(ShardRuntime Runtime, ProbeResult Result);

// Проба шарда: 5 запросов каталога arch/03 §5 одним подключением (read-only).
public interface ISqlProbe
{
    Task<SqlShardResult> ProbeAsync(ClusterInfo cluster, ShardInfo shard, CancellationToken ct);
}

// Реализация: строка строится из разобранных полей ShardInfo (DSN уже разобран
// DsnParser t03 — повторный парсинг не нужен, spec §3.6); любой отказ — ошибка
// целиком на шард (§3.7). Хосты — эндпоинт-синтаксис Npgsql host:port после HostMap.
[InjectAsSingleton(typeof(ISqlProbe))]
public sealed class SqlProbe(IOptions<ProbesOptions> options, TimeProvider time) : ISqlProbe
{
    // Часовой таймаут фолбэка не нужен:<= 0 → 3 c, как ModuleExtensions "patroni" (spec §4.4).
    private static int TimeoutSeconds(ProbesOptions value)
        => (int)Math.Ceiling(value.TimeoutSeconds <= 0 ? 3 : value.TimeoutSeconds);

    // Строка подключения пробы — публичный static: чистая часть для unit-тестов (spec §10.5).
    public static NpgsqlConnectionStringBuilder BuildConnectionString(ShardInfo shard, ProbesOptions options)
    {
        var port = shard.Port ?? 5432;
        var endpoints = shard.DsnHosts.Select(host => HostMapResolver.Resolve(options.HostMap, host, port));
        var builder = new NpgsqlConnectionStringBuilder
        {
            Host = string.Join(",", endpoints),
            TargetSessionAttributes = NpgsqlTargetSessionAttributes.ReadWrite, // multi-host ведёт на мастер
            ApplicationName = "adminpanel",
            Timeout = TimeoutSeconds(options),
            CommandTimeout = TimeoutSeconds(options), // statement_timeout (arch/02 §6.2)
            Options = "-c default_transaction_read_only=on", // двойная защита от записи
        };
        if (shard.DbName is not null)
            builder.Database = shard.DbName;
        if (shard.User is not null)
            builder.Username = shard.User;
        if (!string.IsNullOrEmpty(options.Password))
            builder.Password = options.Password;
        return builder;
    }

    // Тексты каталога — arch/03 §5 дословно (инвариант документа; семантика неизменна).
    private const string ReplicationSql = """
        select application_name, client_addr, state, sync_state, pg_wal_lsn_diff(
                 pg_current_wal_lsn(), replay_lsn) as lag_bytes
        from pg_stat_replication
        """;

    private const string SlotsSql = """
        select slot_name, slot_type, active, wal_status, safe_wal_size, confirmed_flush_lsn,
               pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) as lag_bytes
        from pg_replication_slots
        """;

    private const string SubscriptionsSql = """
        select subname, received_lsn, latest_end_lsn, latest_end_time
        from pg_stat_subscription
        """;

    private const string SchemasSql = """
        select nspname from pg_namespace where nspname like 'bucket\_%' escape '\'
        """;

    private const string RecoverySql = "select pg_is_in_recovery()";

    public async Task<SqlShardResult> ProbeAsync(ClusterInfo cluster, ShardInfo shard, CancellationToken ct)
    {
        var at = time.GetUtcNow();
        var target = $"{cluster.Name}/{shard.Name}";
        var started = System.Diagnostics.Stopwatch.GetTimestamp();
        try
        {
            await using var connection = new NpgsqlConnection(
                BuildConnectionString(shard, options.Value).ConnectionString);
            await connection.OpenAsync(ct);

            var inRecovery = await ScalarBoolAsync(connection, RecoverySql, ct);
            var standbies = await StandbiesAsync(connection, ct);
            var slots = await SlotsAsync(connection, ct);
            var subscriptions = await SubscriptionsAsync(connection, ct);
            var schemas = await SchemasAsync(connection, ct);

            var runtime = new ShardRuntime(shard.Name, slots, standbies, subscriptions, schemas, inRecovery, null);
            return new SqlShardResult(
                runtime,
                new ProbeResult(
                    target, "sql", true,
                    System.Diagnostics.Stopwatch.GetElapsedTime(started).TotalMilliseconds, null, at));
        }
        catch (Exception e)
        {
            // Отказ пробы — целиком на шард: runtime с Error, списки пустые (spec §3.7);
            // etcd-данные шарда не роняются (arch/02 §6).
            return new SqlShardResult(
                new ShardRuntime(shard.Name, [], [], [], [], null, e.Message),
                new ProbeResult(
                    target, "sql", false,
                    System.Diagnostics.Stopwatch.GetElapsedTime(started).TotalMilliseconds, e.Message, at));
        }
    }

    // numeric (pg_wal_lsn_diff/safe_wal_size) читается decimal → long: разности LSN
    // целочисленны и < 2^53 (spec §3.21); inet — значением + ToString.
    private static async Task<bool?> ScalarBoolAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct) is bool value ? value : null;
    }

    private static async Task<IReadOnlyList<StandbyInfo>> StandbiesAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(ReplicationSql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var result = new List<StandbyInfo>();
        while (await reader.ReadAsync(ct))
        {
            result.Add(new StandbyInfo(
                reader.GetString(0),
                reader.IsDBNull(1) ? null : reader.GetValue(1)?.ToString(),
                reader.GetString(2),
                reader.GetString(3),
                reader.IsDBNull(4) ? null : (long)reader.GetDecimal(4)));
        }

        return result;
    }

    private static async Task<IReadOnlyList<ReplicationSlotInfo>> SlotsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(SlotsSql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var result = new List<ReplicationSlotInfo>();
        while (await reader.ReadAsync(ct))
        {
            result.Add(new ReplicationSlotInfo(
                reader.GetString(0),
                reader.GetString(1),
                reader.GetBoolean(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? null : (long)reader.GetDecimal(4),
                reader.IsDBNull(6) ? null : (long)reader.GetDecimal(6)));
        }

        return result;
    }

    private static async Task<IReadOnlyList<SubscriptionInfo>> SubscriptionsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(SubscriptionsSql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var result = new List<SubscriptionInfo>();
        while (await reader.ReadAsync(ct))
        {
            result.Add(new SubscriptionInfo(
                reader.GetString(0),
                reader.IsDBNull(1) ? null : reader.GetValue(1)?.ToString(),
                reader.IsDBNull(2) ? null : reader.GetValue(2)?.ToString(),
                reader.IsDBNull(3) ? null : reader.GetFieldValue<DateTimeOffset>(3)));
        }

        return result;
    }

    private static async Task<IReadOnlyList<string>> SchemasAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(SchemasSql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var result = new List<string>();
        while (await reader.ReadAsync(ct))
            result.Add(reader.GetString(0));
        return result;
    }
}
```

- [ ] **Step 4: Прогнать тесты + сборку**

Run: `dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~AdminPanel.UnitTests.SqlConnectionFactoryTests"`
Expected: PASS, 4 passed.
Run: `dotnet build src/AdminPanel.slnx` — успех, 0 warnings.

- [ ] **Step 5: Commit**

```bash
git add src/AdminPanel.Probes/SqlProbe.cs src/tests/AdminPanel.UnitTests/SqlConnectionFactoryTests.cs
git commit -m "t06: probes — SqlProbe: строка HostMap+пароль+read-only, каталог 03 §5 одним подключением (spec §4.7)"
```

**Выход:** SQL-проба готова; живой PG — Task 11.

---

### Task 6: ProbeResultsStore + ProbeOrchestrator

**Files:**
- Create: `src/AdminPanel.Probes/ProbeResultsStore.cs`
- Create: `src/AdminPanel.Probes/ProbeOrchestrator.cs`
- Test: `src/tests/AdminPanel.UnitTests/ProbeOrchestratorTests.cs`

**Interfaces:**
- Consumes: `IPatroniRestProbe`/`PatroniMemberResult` (Task 4), `ISqlProbe`/`SqlShardResult` (Task 5), `ISnapshotReader` (Core, Task 1→2 impl), `IProbeStateStore` (Task 1), `ProbesOptions` (Task 3).
- Produces (для Program-хоста и Tasks 10–11): `ProbeOrchestrator(ISnapshotReader, IProbeStateStore, IPatroniRestProbe, ISqlProbe, IOptions<ProbesOptions>, TimeProvider, ILogger<ProbeOrchestrator>) : BackgroundService`, публичное ядро `RunOnceAsync(CancellationToken) → Task`.

**Вход:** Tasks 1–5 слиты; `AddProbes()` в `Program.cs` уже вызван (t01) — DI соберётся сам.

- [ ] **Step 1: Написать failing-тесты (ProbeOrchestratorTests.cs, 6 тестов)**

```csharp
using AdminPanel.Core;
using AdminPanel.Probes;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.UnitTests;

// Оркестратор проб на фейках (spec §10.7): цели из снапшота (matched/Dsn),
// параллельность обеих проб, отключаемость, ошибка цели не роняет тик.
public class ProbeOrchestratorTests
{
    private static readonly DateTimeOffset Now = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private sealed class FixedTime : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => Now;
    }

    // Фейк Patroni-пробы: помнит вызовы, поведение настраивается.
    private sealed class FakePatroniProbe : IPatroniRestProbe
    {
        public List<(string Scope, string Member)> Calls { get; } = [];

        public Func<string, string, PatroniMemberResult>? Respond { get; set; }

        public bool Throw { get; set; }

        public Task<PatroniMemberResult> ProbeAsync(HaScope scope, HaMember member, CancellationToken ct)
        {
            Calls.Add((scope.Scope, member.Name));
            if (Throw)
                throw new HttpRequestException("patroni probe crashed");
            return Task.FromResult(Respond!(scope.Scope, member.Name));
        }
    }

    // Фейк SQL-пробы.
    private sealed class FakeSqlProbe : ISqlProbe
    {
        public List<(string Cluster, string Shard)> Calls { get; } = [];

        public Func<string, string, SqlShardResult>? Respond { get; set; }

        public bool Throw { get; set; }

        public Task<SqlShardResult> ProbeAsync(ClusterInfo cluster, ShardInfo shard, CancellationToken ct)
        {
            Calls.Add((cluster.Name, shard.Name));
            if (Throw)
                throw new Npgsql.NpgsqlException("sql probe crashed");
            return Task.FromResult(Respond!(cluster.Name, shard.Name));
        }
    }

    // Снапшот с целями: matched-скоп demo-s1 (2 члена), unmatched-скоп, кластер demo
    // (s1 с DSN, s2-пустышка без хостов) — как spec §3.3.
    private static EtcdSnapshot Snapshot() => TestSnapshots.Healthy(Now) with
    {
        Clusters =
        [
            TestSnapshots.FullCluster() with
            {
                Shards =
                [
                    TestSnapshots.FullCluster().Shards.Single(),
                    new ShardInfo("empty", "", [], null, null, null, null, null, null),
                ],
            },
        ],
        HaScopes =
        [
            new HaScope("demo-s1", "demo", "s1", true, "s1a", null, true,
                [new HaMember("s1a", "s1a", 5432, "master", "running", null, null, null, null),
                 new HaMember("s1b", "s1b", 5432, "replica", "streaming", null, null, null, null)],
                null),
            new HaScope("other-scope", null, null, false, null, null, false,
                [new HaMember("n1", "n1", 5432, "replica", "streaming", null, null, null, null)],
                null),
        ],
    };

    private static (ProbeOrchestrator Orchestrator, FakePatroniProbe Patroni, FakeSqlProbe Sql, SettableStore Store)
        Orchestrator(ProbesOptions? options = null, EtcdSnapshot? snapshot = null)
    {
        var reader = new SnapshotReaderStub(snapshot);
        var store = new SettableStore();
        var patroni = new FakePatroniProbe
        {
            Respond = (scope, member) => new PatroniMemberResult(
                new HaMemberProbe("replica", "streaming", 1L, 0L, Now, null),
                new ProbeResult($"{scope}/{member}", "patroni", true, 1.0, null, Now)),
        };
        var sql = new FakeSqlProbe
        {
            Respond = (cluster, shard) => new SqlShardResult(
                new ShardRuntime(shard, [], [], [], [], false, null),
                new ProbeResult($"{cluster}/{shard}", "sql", true, 2.0, null, Now)),
        };
        var orchestrator = new ProbeOrchestrator(
            reader, store, patroni, sql,
            Options.Create(options ?? new ProbesOptions()),
            new FixedTime(),
            NullLogger<ProbeOrchestrator>.Instance);
        return (orchestrator, patroni, sql, store);
    }

    private sealed class SnapshotReaderStub(EtcdSnapshot? current) : ISnapshotReader
    {
        public EtcdSnapshot? Current { get; } = current;
    }

    internal sealed class SettableStore : IProbeStateStore
    {
        public ProbeState? Current { get; set; }

        public void Replace(ProbeState state) => Current = state;
    }

    [Fact]
    public async Task RunOnce_BuildsTargetsFromSnapshot()
    {
        // Arrange
        var (orchestrator, patroni, sql, _) = Orchestrator(snapshot: Snapshot());

        // Act
        await orchestrator.RunOnceAsync(CancellationToken.None);

        // Assert: matched-скоп — оба члена; шард с DSN — пробуется; unmatched и
        // шард без хостов — нет (spec §3.3).
        patroni.Calls.Should().BeEquivalentTo([("demo-s1", "s1a"), ("demo-s1", "s1b")]);
        sql.Calls.Should().ContainSingle().Which.Should().Be(("demo", "s1"));
    }

    [Fact]
    public async Task RunOnce_WritesStateWithBothKinds()
    {
        // Arrange
        var (orchestrator, _, _, store) = Orchestrator(snapshot: Snapshot());

        // Act
        await orchestrator.RunOnceAsync(CancellationToken.None);

        // Assert: members + runtimes + probes в одном состоянии, одна замена (spec §3.15).
        store.Current.Should().NotBeNull();
        store.Current!.Members.Keys.Should().BeEquivalentTo(["demo-s1/s1a", "demo-s1/s1b"]);
        store.Current.Runtimes.Keys.Should().BeEquivalentTo(["demo/s1"]);
        store.Current.Probes.Should().HaveCount(3);
        store.Current.AtUtc.Should().Be(Now);
    }

    [Fact]
    public async Task RunOnce_PatroniDisabled_Skipped()
    {
        // Arrange
        var (orchestrator, patroni, sql, store) = Orchestrator(
            new ProbesOptions { PatroniEnabled = false }, Snapshot());

        // Act
        await orchestrator.RunOnceAsync(CancellationToken.None);

        // Assert: sql работает, patroni-части пусты (spec §3.15).
        patroni.Calls.Should().BeEmpty();
        sql.Calls.Should().HaveCount(1);
        store.Current!.Members.Should().BeEmpty();
        store.Current.Runtimes.Should().NotBeEmpty();
        store.Current.Probes.Should().OnlyContain(p => p.Kind == "sql");
    }

    [Fact]
    public async Task RunOnce_SqlDisabled_Skipped()
    {
        // Arrange
        var (orchestrator, patroni, sql, store) = Orchestrator(
            new ProbesOptions { SqlEnabled = false }, Snapshot());

        // Act
        await orchestrator.RunOnceAsync(CancellationToken.None);

        // Assert
        sql.Calls.Should().BeEmpty();
        patroni.Calls.Should().HaveCount(2);
        store.Current!.Runtimes.Should().BeEmpty();
        store.Current.Probes.Should().OnlyContain(p => p.Kind == "patroni");
    }

    [Fact]
    public async Task RunOnce_NoSnapshot_EmptyState()
    {
        // Arrange: до первого тика refresher'а целей нет (spec §8).
        var (orchestrator, patroni, sql, store) = Orchestrator(snapshot: null);

        // Act
        await orchestrator.RunOnceAsync(CancellationToken.None);

        // Assert
        patroni.Calls.Should().BeEmpty();
        sql.Calls.Should().BeEmpty();
        store.Current.Should().NotBeNull();
        store.Current!.Probes.Should().BeEmpty();
    }

    [Fact]
    public async Task RunOnce_ProbeThrows_CapturedAsFailedResult()
    {
        // Arrange: реализации проб сами не бросают, но контракт не гарантирует —
        // оркестратор защищён (spec §3.15).
        var (orchestrator, patroni, sql, store) = Orchestrator(snapshot: Snapshot());
        patroni.Throw = true;
        patroni.Respond = null;
        sql.Throw = true;
        sql.Respond = null;

        // Act
        await orchestrator.Invoking(o => o.RunOnceAsync(CancellationToken.None))
            .Should().NotThrowAsync();
        await orchestrator.RunOnceAsync(CancellationToken.None);

        // Assert: тик не упал, ошибки — в ProbeResult(ok:false).
        store.Current!.Probes.Where(p => !p.Ok).Should().HaveCount(3);
        store.Current.Members.Values.Should().OnlyContain(m => m.Error is not null);
        store.Current.Runtimes.Values.Should().OnlyContain(r => r.Error is not null);
    }
}
```

- [ ] **Step 2: Прогнать — ошибка компиляции**

Run: `dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~AdminPanel.UnitTests.ProbeOrchestratorTests" --no-restore`
Expected: FAIL (CS0246 `ProbeOrchestrator` не найден).

- [ ] **Step 3: Реализовать ProbeResultsStore.cs**

```csharp
using AdminPanel.Core;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Probes;

// Хранилище состояния проб: volatile-замена ссылки — зеркалит SnapshotStore (spec §4.9).
[InjectAsSingleton(typeof(IProbeStateStore))]
public sealed class ProbeResultsStore : IProbeStateStore
{
    private volatile ProbeState? _current;

    public ProbeState? Current => _current;

    public void Replace(ProbeState state) => _current = state;
}
```

- [ ] **Step 4: Реализовать ProbeOrchestrator.cs**

```csharp
using AdminPanel.Core;
using AdminPanel.Infrastructure.DI;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AdminPanel.Probes;

// Фоновый тик проб (arch/02 §4 «отдельный тик Probes.Interval»): цели из текущего
// снапшота, все пробы параллельно, состояние — в IProbeStateStore (spec §4.8).
// Пробы не блокируют тик KV refresher'а — тот берёт состояние готовым (§3.1).
[InjectAsSingleton(typeof(IHostedService))]
public sealed class ProbeOrchestrator(
    ISnapshotReader snapshotReader,
    IProbeStateStore stateStore,
    IPatroniRestProbe patroniProbe,
    ISqlProbe sqlProbe,
    IOptions<ProbesOptions> options,
    TimeProvider time,
    ILogger<ProbeOrchestrator> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var value = options.Value;
        if (!value.PatroniEnabled && !value.SqlEnabled)
        {
            // Обе пробы выключены — цикл не нужен (spec §3.15); hosted-регистрация остаётся.
            logger.LogInformation("AdminPanel:Probes: обе пробы выключены — тик проб не запускается");
            return;
        }

        var seconds = value.IntervalSeconds;
        if (seconds <= 0)
        {
            logger.LogWarning("AdminPanel:Probes:IntervalSeconds <= 0 — использую 15 c");
            seconds = 15;
        }

        // Первый тик сразу (прецедент t03 §7.2), далее по периоду.
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(seconds));
        do
        {
            await RunOnceAsync(stoppingToken);
        }
        while (await timer.WaitForNextTickAsync(stoppingToken));
    }

    // Ядро одного тика — публично для unit/integration-тестов без хоста (прецедент RefreshOnceAsync).
    public async Task RunOnceAsync(CancellationToken ct)
    {
        var value = options.Value;
        var at = time.GetUtcNow();
        var snapshot = snapshotReader.Current;
        var members = new Dictionary<string, HaMemberProbe>();
        var runtimes = new Dictionary<string, ShardRuntime>();
        var results = new List<ProbeResult>();
        var tasks = new List<Task>();

        // Цели — matched-скопы и шарды с DSN (spec §3.3); обе пробы — параллельно (§3.15).
        if (value.PatroniEnabled && snapshot is not null)
        {
            foreach (var scope in snapshot.HaScopes.Where(s => s.Matched))
            foreach (var member in scope.Members)
            {
                var key = $"{scope.Scope}/{member.Name}";
                tasks.Add(Patroni(scope, member, key, at, members, results, ct));
            }
        }

        if (value.SqlEnabled && snapshot is not null)
        {
            foreach (var cluster in snapshot.Clusters)
            foreach (var shard in cluster.Shards.Where(s => s.DsnHosts.Count > 0))
            {
                tasks.Add(Sql(cluster, shard, at, runtimes, results, ct));
            }
        }

        await Task.WhenAll(tasks);

        // Одна атомарная замена состояния; порядок проб стабилен (kind, затем target).
        stateStore.Replace(new ProbeState(
            at,
            [.. results.OrderBy(r => r.Kind, StringComparer.Ordinal).ThenBy(r => r.Target, StringComparer.Ordinal)],
            members,
            runtimes));
        return;

        // Локальная обёртка Patroni-цели: реализация пробы не бросает, но контракт
        // не гарантирует — ошибка цели ловится в failed-результат (spec §3.15).
        async Task Patroni(
            HaScope scope, HaMember member, string key, DateTimeOffset atUtc,
            Dictionary<string, HaMemberProbe> sink, List<ProbeResult> probeSink, CancellationToken token)
        {
            PatroniMemberResult result;
            try
            {
                result = await patroniProbe.ProbeAsync(scope, member, token);
            }
            catch (Exception e)
            {
                result = new PatroniMemberResult(
                    new HaMemberProbe(null, null, null, null, atUtc, e.Message),
                    new ProbeResult(key, "patroni", false, null, e.Message, atUtc));
            }

            lock (sink)
            {
                sink[key] = result.Enrichment;
            }

            lock (probeSink)
            {
                probeSink.Add(result.Result);
            }
        }

        // Локальная обёртка SQL-цели — та же защита от броска реализации.
        async Task Sql(
            ClusterInfo cluster, ShardInfo shard, DateTimeOffset atUtc,
            Dictionary<string, ShardRuntime> sink, List<ProbeResult> probeSink, CancellationToken token)
        {
            SqlShardResult result;
            try
            {
                result = await sqlProbe.ProbeAsync(cluster, shard, token);
            }
            catch (Exception e)
            {
                result = new SqlShardResult(
                    new ShardRuntime(shard.Name, [], [], [], [], null, e.Message),
                    new ProbeResult($"{cluster.Name}/{shard.Name}", "sql", false, null, e.Message, atUtc));
            }

            lock (sink)
            {
                sink[$"{cluster.Name}/{shard.Name}"] = result.Runtime;
            }

            lock (probeSink)
            {
                probeSink.Add(result.Result);
            }
        }
    }
}
```

- [ ] **Step 5: Прогнать тесты + сборку**

Run: `dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~AdminPanel.UnitTests.ProbeOrchestratorTests"`
Expected: PASS, 6 passed.
Run: `dotnet build src/AdminPanel.slnx` — успех, 0 warnings.

- [ ] **Step 6: Commit**

```bash
git add src/AdminPanel.Probes/ProbeResultsStore.cs src/AdminPanel.Probes/ProbeOrchestrator.cs src/tests/AdminPanel.UnitTests/ProbeOrchestratorTests.cs
git commit -m "t06: probes — оркестратор тика 15 c (цели из снапшота, параллельно, отключаемость) + стор состояния (spec §4.8–4.9)"
```

**Выход:** фоновый цикл проб полностью работает в DI (Program уже вызывает `AddProbes()` и `RemoveAll<IHostedService>` в фабрике его не ломает — hosted-регистрация t06 попала под существующий RemoveAll, правок фабрики не нужно, spec §3.16).

---

### Task 7: Пороги AlertsOptions + HA-фикстуры + правила etcd/patroni-источника (4 правила)

**Files:**
- Modify: `src/AdminPanel.Core/Alerting/AlertsOptions.cs` (+2 свойства)
- Modify: `src/AdminPanel.Api/appsettings.json` (Alerts +2 ключа)
- Create: `src/AdminPanel.Core/Alerting/Rules/ShardNoLeaderRule.cs`
- Create: `src/AdminPanel.Core/Alerting/Rules/HaMemberNotStreamingRule.cs`
- Create: `src/AdminPanel.Core/Alerting/Rules/ReplicaLagHighRule.cs`
- Create: `src/AdminPanel.Core/Alerting/Rules/ProbeFailedRule.cs`
- Modify: `src/tests/AdminPanel.UnitTests/TestSnapshots.cs` (+3 хелпера HA)
- Modify: `src/tests/AdminPanel.UnitTests/AlertTestRules.cs` (+4 правила)
- Test: `src/tests/AdminPanel.UnitTests/HaAlertRulesTests.cs` (часть 1)

**Interfaces:**
- Consumes: модель Core (`HaScope`, `ProbeResult`).
- Produces: `AlertsOptions.ReplicaLagBytes` (16 МБ), `AlertsOptions.SlotSafeWalSizeBytes` (1 GiB); правила с `KindName`-константами; `TestSnapshots.HaScopeDemo/UnmatchedNoLeader/ShardRuntimeOf` для Task 8.

**Вход:** Tasks 1–6 слиты; стиль правил — `ShardNoMasterRule`/`MoveStaleRule` (t04/t05).

- [ ] **Step 1: AlertsOptions + appsettings**

`AlertsOptions.cs` — добавить после `FrozenSeconds` (комментарий шапки про t06 убрать):

```csharp
    // replica-lag-high и slot-lag-high: порог лага в байтах (arch/01 §6, каталог 03 §4;
    // один порог лага на оба kind — spec §3.8). <= 0 — дефолт каталога.
    public long ReplicaLagBytes { get; set; } = 16 * 1024 * 1024;

    // slot-invalidation-risk: остаток safe_wal_size ниже порога — риск среза слота
    // (03 §4; отдельная семантика от лага, spec §3.8). <= 0 — дефолт 1 GiB.
    public long SlotSafeWalSizeBytes { get; set; } = 1024L * 1024 * 1024;
```

`appsettings.json` — секция `"Alerts"`:

```json
    "Alerts": {
      "StaleMoveSeconds": 600,
      "FrozenSeconds": 60,
      "ReplicaLagBytes": 16777216,
      "SlotSafeWalSizeBytes": 1073741824
    }
```

- [ ] **Step 2: HA-фикстуры в TestSnapshots.cs (в конец класса)**

```csharp
    // HA-фикстуры t06: matched-скоп с пробами членов и unmatched без лидера (spec §10).
    public static HaScope HaScopeDemo(DateTimeOffset now) => new(
        "demo-s1", "demo", "s1", true, "s1a", 738273634528L, true,
        [
            new HaMember("s1a", "s1a", 5432, "master", "running", 1L, 0L, now, null),
            new HaMember("s1b", "s1b", 5432, "replica", "streaming", 1L, 17L * 1024 * 1024, now, null),
        ],
        "{\"ttl\":5,\"loop_wait\":2}");

    public static HaScope UnmatchedNoLeader(DateTimeOffset now) => new(
        "other-scope", null, null, false, null, null, false,
        [new HaMember("n1", "n1", 5432, "replica", "stopped", null, null, now, "connection refused")],
        null);

    // Снапшот с HA-скопами (без runtime — SQL-часть добавляется через with).
    public static EtcdSnapshot WithHaScopes(DateTimeOffset now) => Healthy(now) with
    {
        HaScopes = [HaScopeDemo(now), UnmatchedNoLeader(now)],
    };

    // Runtime шарда для SQL-правил: слот с лагом/риском, standby async (spec §10.1).
    public static ShardRuntime ShardRuntimeOf(string shard) => new(
        shard,
        [new ReplicationSlotInfo("move_bucket_3", "logical", true, "lost", 512L * 1024 * 1024, 100L)],
        [new StandbyInfo("s1b", "10.0.0.2", "streaming", "async", 100L)],
        [],
        [.. Enumerable.Range(0, 16).Select(i => $"bucket_{i}")],
        false,
        null);
```

- [ ] **Step 3: Написать failing-тесты (HaAlertRulesTests.cs — часть 1, 11 тестов)**

```csharp
using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using AdminPanel.Core.Alerting.Rules;
using FluentAssertions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.UnitTests;

// HA-правила алертов t06 (spec §10.1): источники /service/ и Patroni-проба.
// SQL-правила (slot-*/sync/inventory) — в этом же файле, добавляются следующим таском.
public class HaAlertRulesTests
{
    private static readonly DateTimeOffset Now = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    // Хелпер порогов: имя LagOptions — чтобы не конфликтовать с Microsoft.Extensions.Options.Options.
    private static AlertsOptions LagOptions(long replicaLag = 16L * 1024 * 1024)
        => new() { ReplicaLagBytes = replicaLag };

    [Fact]
    public void ShardNoLeader_MatchedNoLeader_Critical()
    {
        // Arrange: matched-скоп без leader-ключа.
        var snapshot = TestSnapshots.WithHaScopes(Now) with
        {
            HaScopes = [TestSnapshots.HaScopeDemo(Now) with { LeaderName = null }],
        };

        // Act
        var alerts = new ShardNoLeaderRule().Evaluate(snapshot, Context()).ToList();

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Id.Should().Be("shard-no-leader:demo-s1");
        alert.Severity.Should().Be(AlertSeverity.Critical);
        alert.Details!.Keys.Should().Contain(["scope", "cluster", "shard"]);
    }

    [Fact]
    public void ShardNoLeader_WithLeader_NoAlert()
    {
        // Arrange — лидер есть.
        var snapshot = TestSnapshots.WithHaScopes(Now);

        // Act
        var alerts = new ShardNoLeaderRule().Evaluate(snapshot, Context()).ToList();

        // Assert: other-scope без лидера не считается — unmatched не алертится (spec §3.10).
        alerts.Should().BeEmpty();
    }

    [Fact]
    public void HaMemberNotStreaming_ReplicaNotStreaming_Warning()
    {
        // Arrange: реплика в starting с успешной пробой.
        var scopes = new[]
        {
            TestSnapshots.HaScopeDemo(Now) with
            {
                Members =
                [
                    new HaMember("s1a", "s1a", 5432, "master", "running", 1L, 0L, Now, null),
                    new HaMember("s1b", "s1b", 5432, "replica", "starting", 1L, 10L, Now, null),
                ],
            },
        };
        var snapshot = TestSnapshots.Healthy(Now) with { HaScopes = scopes };

        // Act
        var alerts = new HaMemberNotStreamingRule().Evaluate(snapshot, Context()).ToList();

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Id.Should().Be("ha-member-not-streaming:demo-s1/s1b");
        alert.Severity.Should().Be(AlertSeverity.Warning);
        alert.Details!["expected"].Should().Be("streaming");
        alert.Details["state"].Should().Be("starting");
    }

    [Fact]
    public void HaMemberNotStreaming_MasterNotRunning_Warning()
    {
        // Arrange: мастер остановился.
        var scopes = new[]
        {
            TestSnapshots.HaScopeDemo(Now) with
            {
                Members = [new HaMember("s1a", "s1a", 5432, "master", "stopped", 1L, null, Now, null)],
            },
        };
        var snapshot = TestSnapshots.Healthy(Now) with { HaScopes = scopes };

        // Act
        var alerts = new HaMemberNotStreamingRule().Evaluate(snapshot, Context()).ToList();

        // Assert
        alerts.Should().ContainSingle().Which.Details!["expected"].Should().Be("running");
    }

    [Fact]
    public void HaMemberNotStreaming_UnknownRole_Skipped()
    {
        // Arrange: sync_standby — каталожного ожидания нет (spec §3.13).
        var scopes = new[]
        {
            TestSnapshots.HaScopeDemo(Now) with
            {
                Members = [new HaMember("s1c", "s1c", 5432, "sync_standby", "streaming", 1L, 0L, Now, null)],
            },
        };
        var snapshot = TestSnapshots.Healthy(Now) with { HaScopes = scopes };

        // Act
        var alerts = new HaMemberNotStreamingRule().Evaluate(snapshot, Context()).ToList();

        // Assert
        alerts.Should().BeEmpty();
    }

    [Fact]
    public void HaMemberNotStreaming_ProbeErrorOrMissing_Skipped()
    {
        // Arrange: в одном matched-скопе — здоровый мастер, член с упавшей пробой
        // (DCS state "crashed", ProbeError задан) и член до первого тика пробы
        // (ProbeAtUtc null): ни один не должен алертиться этим правилом.
        var scopes = new[]
        {
            TestSnapshots.HaScopeDemo(Now) with
            {
                Members =
                [
                    new HaMember("s1a", "s1a", 5432, "master", "running", 1L, 0L, Now, null),
                    new HaMember("err", "err", 5432, "replica", "crashed", null, null, Now, "connection refused"),
                    new HaMember("cold", "cold", 5432, "replica", null, null, null, null, null),
                ],
            },
        };
        var snapshot = TestSnapshots.Healthy(Now) with { HaScopes = scopes };

        // Act
        var alerts = new HaMemberNotStreamingRule().Evaluate(snapshot, Context()).ToList();

        // Assert: ошибка пробы — зона probe-failed; без пробы — данных нет (spec §3.13).
        alerts.Should().BeEmpty();
    }

    [Fact]
    public void ReplicaLagHigh_AboveThreshold_Warning()
    {
        // Arrange: лаг s1b = 17 МБ > 16 МБ.
        var snapshot = TestSnapshots.WithHaScopes(Now);
        var rule = new ReplicaLagHighRule(Options.Create(LagOptions()));

        // Act
        var alerts = rule.Evaluate(snapshot, Context()).ToList();

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Id.Should().Be("replica-lag-high:demo-s1/s1b");
        alert.Details!["lagBytes"].Should().Be((17L * 1024 * 1024).ToString());
        alert.Details["thresholdBytes"].Should().Be((16L * 1024 * 1024).ToString());
    }

    [Fact]
    public void ReplicaLagHigh_AtThreshold_NoAlert()
    {
        // Arrange: ровно порог — не «больше» (строгие сравнения каталога).
        var scopes = new[]
        {
            TestSnapshots.HaScopeDemo(Now) with
            {
                Members = [new HaMember("s1b", "s1b", 5432, "replica", "streaming", 1L, 16L * 1024 * 1024, Now, null)],
            },
        };
        var snapshot = TestSnapshots.Healthy(Now) with { HaScopes = scopes };

        // Act
        var alerts = new ReplicaLagHighRule(Options.Create(LagOptions())).Evaluate(snapshot, Context()).ToList();

        // Assert
        alerts.Should().BeEmpty();
    }

    [Fact]
    public void ReplicaLagHigh_CustomThreshold_FromOptions()
    {
        // Arrange: порог 100 байт из настроек.
        var snapshot = TestSnapshots.WithHaScopes(Now);
        var rule = new ReplicaLagHighRule(Options.Create(LagOptions(100)));

        // Act
        var alerts = rule.Evaluate(snapshot, Context()).ToList();

        // Assert: лаг 0 мастера не алертится, s1b (17 МБ) — да.
        alerts.Should().ContainSingle().Which.Target.Should().Be("demo-s1/s1b");
    }

    [Fact]
    public void ReplicaLagHigh_NoProbe_Silent()
    {
        // Arrange: член без проб (LagBytes null).
        var scopes = new[]
        {
            TestSnapshots.HaScopeDemo(Now) with
            {
                Members = [new HaMember("s1b", "s1b", 5432, "replica", "streaming", null, null, null, null)],
            },
        };
        var snapshot = TestSnapshots.Healthy(Now) with { HaScopes = scopes };

        // Act
        var alerts = new ReplicaLagHighRule(Options.Create(LagOptions())).Evaluate(snapshot, Context()).ToList();

        // Assert: SQL/Patroni-алерты — только при включённых пробах (03 §4).
        alerts.Should().BeEmpty();
    }

    [Fact]
    public void ProbeFailed_EachFailedResult_Info()
    {
        // Arrange: одна patroni- и одна sql-проба упали.
        var snapshot = TestSnapshots.Healthy(Now) with
        {
            Probes =
            [
                new ProbeResult("demo-s1/s1a", "patroni", false, 3.0, "connection refused", Now),
                new ProbeResult("demo/s1", "sql", false, 4.0, "timeout", Now),
                new ProbeResult("demo-s1/s1b", "patroni", true, 5.0, null, Now),
            ],
        };

        // Act
        var alerts = new ProbeFailedRule().Evaluate(snapshot, Context()).ToList();

        // Assert: id включает kind — уникальность при пересечении имён (spec §3.14).
        alerts.Should().HaveCount(2);
        alerts.Should().OnlyContain(a => a.Severity == AlertSeverity.Info);
        alerts.Select(a => a.Id).Should().BeEquivalentTo(
            ["probe-failed:patroni:demo-s1/s1a", "probe-failed:sql:demo/s1"]);
        alerts.Single(a => a.Kind == "probe-failed" && a.Details!["kind"] == "sql")
            .Details["error"].Should().Be("timeout");
    }

    private static AlertContext Context() => new(null, Now, 3);
}
```

- [ ] **Step 4: Прогнать — ошибка компиляции**

Run: `dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~AdminPanel.UnitTests.HaAlertRulesTests" --no-restore`
Expected: FAIL (CS0246 правила не найдены).

- [ ] **Step 5: Реализовать 4 правила**

`ShardNoLeaderRule.cs`:

```csharp
using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Core.Alerting.Rules;

// shard-no-leader (critical): matched HA-scope без leader-ключа (arch/03 §4; spec §3.10 —
// unmatched-скопы чужого service не алертятся, arch/02 §7).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class ShardNoLeaderRule : IAlertRule
{
    public const string KindName = "shard-no-leader";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var scope in snapshot.HaScopes)
        {
            if (!scope.Matched || scope.LeaderName is not null)
                continue;

            yield return new Alert(
                $"{KindName}:{scope.Scope}",
                AlertSeverity.Critical,
                KindName,
                scope.Scope,
                $"HA-scope {scope.Scope} без leader-ключа (шард {scope.Cluster}/{scope.Shard} без лидера)",
                new Dictionary<string, string>
                {
                    ["scope"] = scope.Scope,
                    ["cluster"] = scope.Cluster!,
                    ["shard"] = scope.Shard!,
                },
                null);
        }
    }
}
```

`HaMemberNotStreamingRule.cs`:

```csharp
using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Core.Alerting.Rules;

// ha-member-not-streaming (warning): Patroni-проба успешна, но состояние члена
// не совпадает с ожиданием по роли: master → running, replica → streaming
// (arch/03 §4; spec §3.13 — прочие роли и упавшие пробы не проверяются).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class HaMemberNotStreamingRule : IAlertRule
{
    public const string KindName = "ha-member-not-streaming";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var scope in snapshot.HaScopes.Where(s => s.Matched))
        foreach (var member in scope.Members)
        {
            if (member.ProbeAtUtc is null || member.ProbeError is not null)
                continue; // данных нет или ошибка пробы (зона probe-failed)

            var expected = member.Role switch
            {
                "master" => "running",
                "replica" => "streaming",
                _ => null,
            };
            if (expected is null || member.State == expected)
                continue;

            yield return new Alert(
                $"{KindName}:{scope.Scope}/{member.Name}",
                AlertSeverity.Warning,
                KindName,
                $"{scope.Scope}/{member.Name}",
                $"член {member.Name} scope {scope.Scope} в состоянии {member.State} (роль {member.Role}, ожидалось {expected})",
                new Dictionary<string, string>
                {
                    ["scope"] = scope.Scope,
                    ["member"] = member.Name,
                    ["role"] = member.Role!,
                    ["state"] = member.State!,
                    ["expected"] = expected,
                },
                null);
        }
    }
}
```

`ReplicaLagHighRule.cs`:

```csharp
using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;
using Microsoft.Extensions.Options;

namespace AdminPanel.Core.Alerting.Rules;

// replica-lag-high (warning): лаг члена по Patroni-пробе > ReplicaLagBytes (arch/03 §4;
// источник — Patroni REST, arch/01 §6; spec §5.1).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class ReplicaLagHighRule(IOptions<AlertsOptions> options) : IAlertRule
{
    public const string KindName = "replica-lag-high";

    // Каталожный дефолт 16 МБ — фолбэк при опечатке конфига (spec §3.8).
    public const long DefaultBytes = 16 * 1024 * 1024;

    public string Kind => KindName;

    private long ThresholdBytes
        => options.Value.ReplicaLagBytes > 0 ? options.Value.ReplicaLagBytes : DefaultBytes;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var scope in snapshot.HaScopes.Where(s => s.Matched))
        foreach (var member in scope.Members)
        {
            if (member.ProbeAtUtc is null || member.ProbeError is not null || member.LagBytes is not > ThresholdBytes)
                continue;

            yield return new Alert(
                $"{KindName}:{scope.Scope}/{member.Name}",
                AlertSeverity.Warning,
                KindName,
                $"{scope.Scope}/{member.Name}",
                $"лаг члена {member.Name} scope {scope.Scope} — {member.LagBytes} байт, порог {ThresholdBytes} байт",
                new Dictionary<string, string>
                {
                    ["lagBytes"] = member.LagBytes.Value.ToString(),
                    ["thresholdBytes"] = ThresholdBytes.ToString(),
                },
                null);
        }
    }
}
```

`ProbeFailedRule.cs`:

```csharp
using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Core.Alerting.Rules;

// probe-failed (info): каждая неудавшаяся проба тика — один алерт на цель
// (arch/03 §4; severity по каталогу — конфликт с обзором arch/01 §8 разрешён
// каталогом, spec §3.9; target "{kind}:{target}" — уникальный id, §3.14).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class ProbeFailedRule : IAlertRule
{
    public const string KindName = "probe-failed";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var probe in snapshot.Probes.Where(p => !p.Ok))
        {
            var target = $"{probe.Kind}:{probe.Target}";
            yield return new Alert(
                $"{KindName}:{target}",
                AlertSeverity.Info,
                KindName,
                target,
                $"проба {probe.Kind} по {probe.Target} не удалась: {probe.Error}",
                new Dictionary<string, string>
                {
                    ["kind"] = probe.Kind,
                    ["target"] = probe.Target,
                    ["error"] = probe.Error ?? string.Empty,
                },
                null);
        }
    }
}
```

- [ ] **Step 6: AlertTestRules.cs — добавить 4 правила (внутрь массива `All()`)**

```csharp
            new ShardNoLeaderRule(),
            new HaMemberNotStreamingRule(),
            new ReplicaLagHighRule(Options.Create(new AlertsOptions())),
            new ProbeFailedRule(),
```

(SQL-правила добавит Task 8; комментарий файла обновить на «t04+t05+t06».)

- [ ] **Step 7: Прогнать тесты + сборку**

Run: `dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~AdminPanel.UnitTests.HaAlertRulesTests"`
Expected: PASS, 11 passed.
Run: `dotnet build src/AdminPanel.slnx && dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~AdminPanel.UnitTests" --no-build`
Expected: все юнит-тесты зелёные — фикстура `WithHaScopes` не входит в чужие тесты, новые правила молчат на старых снапшотах.

- [ ] **Step 8: Commit**

```bash
git add src/AdminPanel.Core/Alerting/AlertsOptions.cs src/AdminPanel.Core/Alerting/Rules/ShardNoLeaderRule.cs src/AdminPanel.Core/Alerting/Rules/HaMemberNotStreamingRule.cs src/AdminPanel.Core/Alerting/Rules/ReplicaLagHighRule.cs src/AdminPanel.Core/Alerting/Rules/ProbeFailedRule.cs src/AdminPanel.Api/appsettings.json src/tests/AdminPanel.UnitTests/TestSnapshots.cs src/tests/AdminPanel.UnitTests/AlertTestRules.cs src/tests/AdminPanel.UnitTests/HaAlertRulesTests.cs
git commit -m "t06: alerts — пороги ReplicaLagBytes/SlotSafeWalSizeBytes + shard-no-leader/ha-member-not-streaming/replica-lag-high/probe-failed (spec §5, §10.1 ч.1)"
```

**Выход:** 4 из 9 HA-правил работают; HA-фикстуры доступны Task 8.

---

### Task 8: SQL-правила (5 правил) + сквозной сценарий AlertEngine

**Files:**
- Create: `src/AdminPanel.Core/Alerting/Rules/SlotLagHighRule.cs`
- Create: `src/AdminPanel.Core/Alerting/Rules/SlotWalLostRule.cs`
- Create: `src/AdminPanel.Core/Alerting/Rules/SlotInvalidationRiskRule.cs`
- Create: `src/AdminPanel.Core/Alerting/Rules/SyncStandbyMissingRule.cs`
- Create: `src/AdminPanel.Core/Alerting/Rules/InventoryMismatchRule.cs`
- Modify: `src/tests/AdminPanel.UnitTests/AlertTestRules.cs` (+5 правил)
- Test: `src/tests/AdminPanel.UnitTests/HaAlertRulesTests.cs` (часть 2, +9 тестов)

**Interfaces:**
- Consumes: `AlertsOptions`-пороги и `TestSnapshots.ShardRuntimeOf` (Task 7); `ShardRuntime`/`ReplicationSlotInfo`/`StandbyInfo` (Core).
- Produces: полный список `AlertTestRules.All()` (24 правила) для Tasks 10–11.

**Вход:** Task 7 слит; правило читает `ShardInfo.Runtime` (появляется после enrichment Task 2).

- [ ] **Step 1: Написать failing-тесты (добавить в HaAlertRulesTests.cs, 8 тестов)**

```csharp
    // ==== SQL-правила (spec §10.1 ч.2) ====

    private static EtcdSnapshot SnapshotWithRuntime(ShardRuntime runtime) => TestSnapshots.Healthy(Now) with
    {
        Clusters =
        [
            TestSnapshots.FullCluster() with
            {
                Shards = [TestSnapshots.FullCluster().Shards.Single() with { Runtime = runtime }],
            },
        ],
    };

    [Fact]
    public void SlotLagHigh_AboveThreshold_Warning()
    {
        // Arrange: слот с лагом 17 МБ (дефолт 16 МБ).
        var runtime = TestSnapshots.ShardRuntimeOf("s1") with
        {
            Slots = [new ReplicationSlotInfo("move_bucket_3", "logical", true, "active", null, 17L * 1024 * 1024)],
        };
        var snapshot = SnapshotWithRuntime(runtime);

        // Act
        var alerts = new SlotLagHighRule(Options.Create(LagOptions())).Evaluate(snapshot, Context()).ToList();

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Id.Should().Be("slot-lag-high:demo/s1/move_bucket_3");
        alert.Details!["thresholdBytes"].Should().Be((16L * 1024 * 1024).ToString());
    }

    [Fact]
    public void SlotWalLost_LostSlot_Critical()
    {
        // Arrange: wal_status=lost — WAL срезан (P4).
        var snapshot = SnapshotWithRuntime(TestSnapshots.ShardRuntimeOf("s1"));

        // Act
        var alerts = new SlotWalLostRule().Evaluate(snapshot, Context()).ToList();

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Severity.Should().Be(AlertSeverity.Critical);
        alert.Id.Should().Be("slot-wal-lost:demo/s1/move_bucket_3");
    }

    [Fact]
    public void SlotInvalidationRisk_BelowThreshold_Warning()
    {
        // Arrange: safe_wal_size 512 МБ < порога 1 GiB (P4, ДО среза).
        var snapshot = SnapshotWithRuntime(TestSnapshots.ShardRuntimeOf("s1"));

        // Act
        var alerts = new SlotInvalidationRiskRule(Options.Create(new AlertsOptions())).Evaluate(snapshot, Context()).ToList();

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Id.Should().Be("slot-invalidation-risk:demo/s1/move_bucket_3");
        alert.Details!["safeWalSizeBytes"].Should().Be((512L * 1024 * 1024).ToString());
    }

    [Fact]
    public void SlotRules_NullSafeWalSizeAndErrorRuntime_Skipped()
    {
        // Arrange: safe_wal_size null (нет max_slot_wal_keep_size) — риска нет;
        // runtime с ошибкой и шард без runtime (пробы выключены) — SQL-алерты молчат
        // (03 §4, spec §3.7; null-runtime — регрессия гварда InventoryMismatchRule).
        var noRisk = TestSnapshots.ShardRuntimeOf("s1") with
        {
            Slots = [new ReplicationSlotInfo("move_bucket_3", "logical", true, "active", null, 100L)],
        };
        var errored = TestSnapshots.ShardRuntimeOf("s1") with { Error = "connect refused" };
        var ruleOptions = Options.Create(new AlertsOptions());

        // Act
        var riskOnNull = new SlotInvalidationRiskRule(ruleOptions).Evaluate(SnapshotWithRuntime(noRisk), Context()).ToList();
        var allOnError = new[]
        {
            new SlotLagHighRule(ruleOptions).Evaluate(SnapshotWithRuntime(errored), Context()),
            new SlotWalLostRule().Evaluate(SnapshotWithRuntime(errored), Context()),
            new SyncStandbyMissingRule().Evaluate(SnapshotWithRuntime(errored), Context()),
            new InventoryMismatchRule().Evaluate(SnapshotWithRuntime(errored), Context()),
        }.SelectMany(a => a).ToList();
        var allOnNoRuntime = new[]
        {
            new SlotLagHighRule(ruleOptions).Evaluate(TestSnapshots.Healthy(Now), Context()),
            new SlotWalLostRule().Evaluate(TestSnapshots.Healthy(Now), Context()),
            new SyncStandbyMissingRule().Evaluate(TestSnapshots.Healthy(Now), Context()),
            new InventoryMismatchRule().Evaluate(TestSnapshots.Healthy(Now), Context()),
        }.SelectMany(a => a).ToList();

        // Assert: Healthy — шард без Runtime (t03-фикстура): правила молчат, не падают.
        riskOnNull.Should().BeEmpty();
        allOnError.Should().BeEmpty();
        allOnNoRuntime.Should().BeEmpty();
    }

    [Fact]
    public void SyncStandbyMissing_MasterWithoutSync_Warning()
    {
        // Arrange: мастер (IsInRecovery false), standby только async (P8).
        var snapshot = SnapshotWithRuntime(TestSnapshots.ShardRuntimeOf("s1"));

        // Act
        var alerts = new SyncStandbyMissingRule().Evaluate(snapshot, Context()).ToList();

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Id.Should().Be("sync-standby-missing:demo/s1");
        alert.Details!["standbiesTotal"].Should().Be("1");
    }

    [Fact]
    public void SyncStandbyMissing_WithQuorum_NoAlert()
    {
        // Arrange: quorum-standby присутствует.
        var runtime = TestSnapshots.ShardRuntimeOf("s1") with
        {
            Standbies = [new StandbyInfo("s1b", "10.0.0.2", "streaming", "quorum", 0L)],
        };

        // Act
        var alerts = new SyncStandbyMissingRule().Evaluate(SnapshotWithRuntime(runtime), Context()).ToList();

        // Assert
        alerts.Should().BeEmpty();
    }

    [Fact]
    public void InventoryMismatch_MissingAndExtraSchemas_Warning()
    {
        // Arrange: routing ждёт bucket_0..15 (схемы фикстуры — 16 шт.), но на шарде
        // нет bucket_15 и есть лишняя bucket_9 (в тесте подменяем инвентарь).
        var runtime = TestSnapshots.ShardRuntimeOf("s1") with
        {
            BucketSchemas = [.. Enumerable.Range(0, 15).Select(i => $"bucket_{i}"), "bucket_99"],
        };
        var snapshot = SnapshotWithRuntime(runtime);

        // Act
        var alerts = new InventoryMismatchRule().Evaluate(snapshot, Context()).ToList();

        // Assert: missing bucket_15, extra bucket_99 (сортировка стабильна).
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Id.Should().Be("inventory-mismatch:demo/s1");
        alert.Details!["missing"].Should().Be("bucket_15");
        alert.Details["extra"].Should().Be("bucket_99");
    }

    [Fact]
    public void InventoryMismatch_MovingBucketExcluded_NoAlert()
    {
        // Arrange: bucket_1 в SYNCING на target s2 — на s1 не ожидается, лишней не считается.
        var cluster = TestSnapshots.FullCluster() with
        {
            Buckets = [.. Enumerable.Range(0, 16).Select(i =>
                i == 1
                    ? new BucketInfo(1, "s2", BucketState.Syncing, new MoveInfo("s1", "s2", null, null, "copy", null))
                    : new BucketInfo(i, "s1", BucketState.Active, null))],
            Shards = [TestSnapshots.FullCluster().Shards.Single() with
            {
                Runtime = TestSnapshots.ShardRuntimeOf("s1") with
                {
                    BucketSchemas = [.. Enumerable.Range(0, 16).Where(i => i != 1).Select(i => $"bucket_{i}")],
                },
            }],
        };
        var snapshot = TestSnapshots.Healthy(Now) with { Clusters = [cluster] };

        // Act
        var alerts = new InventoryMismatchRule().Evaluate(snapshot, Context()).ToList();

        // Assert: переездные бакеты исключены с обеих сторон (spec §3.11).
        alerts.Should().BeEmpty();
    }

    [Fact]
    public void HaRules_FullEngine_Scenario()
    {
        // Arrange: нет лидера + реплика не стримит + слот lost + нет sync-standby + проба падала.
        var runtime = TestSnapshots.ShardRuntimeOf("s1");
        var cluster = TestSnapshots.FullCluster() with
        {
            Shards = [TestSnapshots.FullCluster().Shards.Single() with { Runtime = runtime }],
        };
        var scopes = new[]
        {
            TestSnapshots.HaScopeDemo(Now) with
            {
                LeaderName = null,
                Members =
                [
                    new HaMember("s1a", "s1a", 5432, "master", "running", 1L, 0L, Now, null),
                    new HaMember("s1b", "s1b", 5432, "replica", "starting", 1L, 10L, Now, null),
                ],
            },
        };
        var snapshot = TestSnapshots.Healthy(Now) with
        {
            Clusters = [cluster],
            HaScopes = scopes,
            Probes = [new ProbeResult("demo-s1/s1a", "patroni", false, 1.0, "boom", Now)],
        };
        var engine = new AlertEngine(AlertTestRules.All());

        // Act
        var alerts = engine.Evaluate(snapshot, null, Now, 3).ToList();

        // Assert: сортировка severity → kind (Ordinal): critical (shard-no-leader,
        // slot-wal-lost) → warning (ha-member-not-streaming, slot-invalidation-risk,
        // sync-standby-missing) → info (probe-failed). Слот фикстуры несёт
        // safe_wal_size 512 МБ < 1 GiB — risk-алерт входит в сценарий законно (6-й).
        // t04/t05-правила на этой фикстуре молчат.
        alerts.Select(a => a.Id).Should().ContainInOrder(
            "shard-no-leader:demo-s1",
            "slot-wal-lost:demo/s1/move_bucket_3",
            "ha-member-not-streaming:demo-s1/s1b",
            "slot-invalidation-risk:demo/s1/move_bucket_3",
            "sync-standby-missing:demo/s1",
            "probe-failed:patroni:demo-s1/s1a");
        alerts.Select(a => a.Id).Should().HaveCount(6);
    }
```

- [ ] **Step 2: Прогнать — ошибка компиляции**

Run: `dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~AdminPanel.UnitTests.HaAlertRulesTests" --no-restore`
Expected: FAIL (CS0246 SQL-правила не найдены).

- [ ] **Step 3: Реализовать 5 правил**

`SlotLagHighRule.cs`:

```csharp
using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;
using Microsoft.Extensions.Options;

namespace AdminPanel.Core.Alerting.Rules;

// slot-lag-high (warning): лаг слота > ReplicaLagBytes — один порог лага на
// replica/slot (каталог 03 §4, spec §3.8); источник — SQL-проба (P4).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class SlotLagHighRule(IOptions<AlertsOptions> options) : IAlertRule
{
    public const string KindName = "slot-lag-high";

    public const long DefaultBytes = ReplicaLagHighRule.DefaultBytes;

    public string Kind => KindName;

    private long ThresholdBytes
        => options.Value.ReplicaLagBytes > 0 ? options.Value.ReplicaLagBytes : DefaultBytes;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var (cluster, shard, slot) in Slots(snapshot))
        {
            if (slot.LagBytes is not > ThresholdBytes)
                continue;

            yield return new Alert(
                $"{KindName}:{cluster.Name}/{shard.Name}/{slot.SlotName}",
                AlertSeverity.Warning,
                KindName,
                $"{cluster.Name}/{shard.Name}/{slot.SlotName}",
                $"лаг слота {slot.SlotName} шарда {cluster.Name}/{shard.Name} — {slot.LagBytes} байт, порог {ThresholdBytes} байт",
                new Dictionary<string, string>
                {
                    ["lagBytes"] = slot.LagBytes.Value.ToString(),
                    ["thresholdBytes"] = ThresholdBytes.ToString(),
                },
                null);
        }
    }

    // Общий обход слотов безошибочных runtime — общий хелпер правил slot-* (spec §5.1).
    internal static IEnumerable<(ClusterInfo Cluster, ShardInfo Shard, ReplicationSlotInfo Slot)> Slots(
        EtcdSnapshot snapshot)
    {
        foreach (var cluster in snapshot.Clusters)
        foreach (var shard in cluster.Shards)
        {
            if (shard.Runtime?.Error is not null)
                continue;
            foreach (var slot in shard.Runtime?.Slots ?? [])
                yield return (cluster, shard, slot);
        }
    }
}
```

`SlotWalLostRule.cs`:

```csharp
using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Core.Alerting.Rules;

// slot-wal-lost (critical): wal_status='lost' — WAL срезан, слот догонит только
// пересозданием (P4, arch/03 §4); источник — SQL-проба.
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class SlotWalLostRule : IAlertRule
{
    public const string KindName = "slot-wal-lost";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var (cluster, shard, slot) in SlotLagHighRule.Slots(snapshot))
        {
            if (slot.WalStatus != "lost")
                continue;

            yield return new Alert(
                $"{KindName}:{cluster.Name}/{shard.Name}/{slot.SlotName}",
                AlertSeverity.Critical,
                KindName,
                $"{cluster.Name}/{shard.Name}/{slot.SlotName}",
                $"слот {slot.SlotName} шарда {cluster.Name}/{shard.Name}: wal_status=lost — WAL срезан, источник догонит только пересозданием (P4)",
                new Dictionary<string, string> { ["walStatus"] = "lost" },
                null);
        }
    }
}
```

`SlotInvalidationRiskRule.cs`:

```csharp
using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;
using Microsoft.Extensions.Options;

namespace AdminPanel.Core.Alerting.Rules;

// slot-invalidation-risk (warning): остаток safe_wal_size < порога — риск среза
// слота ДО потери (P4, arch/03 §4); null (max_slot_wal_keep_size не задан) — риска нет.
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class SlotInvalidationRiskRule(IOptions<AlertsOptions> options) : IAlertRule
{
    public const string KindName = "slot-invalidation-risk";

    // Каталожный дефолт 1 GiB — фолбэк при опечатке конфига (spec §3.8).
    public const long DefaultBytes = 1024L * 1024 * 1024;

    public string Kind => KindName;

    private long ThresholdBytes
        => options.Value.SlotSafeWalSizeBytes > 0 ? options.Value.SlotSafeWalSizeBytes : DefaultBytes;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var (cluster, shard, slot) in SlotLagHighRule.Slots(snapshot))
        {
            if (slot.SafeWalSizeBytes is not > 0 || slot.SafeWalSizeBytes >= ThresholdBytes)
                continue;

            yield return new Alert(
                $"{KindName}:{cluster.Name}/{shard.Name}/{slot.SlotName}",
                AlertSeverity.Warning,
                KindName,
                $"{cluster.Name}/{shard.Name}/{slot.SlotName}",
                $"слоту {slot.SlotName} шарда {cluster.Name}/{shard.Name} осталось {slot.SafeWalSizeBytes} байт WAL до среза (порог {ThresholdBytes} байт, P4)",
                new Dictionary<string, string>
                {
                    ["safeWalSizeBytes"] = slot.SafeWalSizeBytes.Value.ToString(),
                    ["thresholdBytes"] = ThresholdBytes.ToString(),
                },
                null);
        }
    }
}
```

`SyncStandbyMissingRule.cs`:

```csharp
using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Core.Alerting.Rules;

// sync-standby-missing (warning): у мастера нет standby с sync_state IN ('sync','quorum')
// — предусловие переездов не выполнено (P8, arch/03 §4; по букве каталога, без
// carve-outs — spec §3.12). Проверяется только на мастере без ошибки пробы.
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class SyncStandbyMissingRule : IAlertRule
{
    public const string KindName = "sync-standby-missing";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var cluster in snapshot.Clusters)
        foreach (var shard in cluster.Shards)
        {
            var runtime = shard.Runtime;
            if (runtime?.Error is not null || runtime?.IsInRecovery != false)
                continue;

            if (runtime.Standbies.Any(s => s.SyncState is "sync" or "quorum"))
                continue;

            yield return new Alert(
                $"{KindName}:{cluster.Name}/{shard.Name}",
                AlertSeverity.Warning,
                KindName,
                $"{cluster.Name}/{shard.Name}",
                $"у мастера шарда {cluster.Name}/{shard.Name} нет sync-standby (sync_state sync/quorum) — предусловие переездов не выполнено (P8)",
                new Dictionary<string, string> { ["standbiesTotal"] = runtime.Standbies.Count.ToString() },
                null);
        }
    }
}
```

`InventoryMismatchRule.cs`:

```csharp
using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Core.Alerting.Rules;

// inventory-mismatch (warning): фактические схемы bucket_% ≠ routing — «тихие»
// расхождения P21/P23 (arch/03 §4). Сверка только по ACTIVE-бакетам: схемы
// переездных бакетов на приёмнике — норма (spec §3.11).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class InventoryMismatchRule : IAlertRule
{
    public const string KindName = "inventory-mismatch";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var cluster in snapshot.Clusters)
        foreach (var shard in cluster.Shards)
        {
            // Runtime нет (пробы выключены/не было тика) — сверки не будет: гвард
            // обязан отсекать и null, и Error (spec §5.1 «Runtime без ошибки»).
            var runtime = shard.Runtime;
            if (runtime is null || runtime.Error is not null)
                continue;

            var expected = cluster.Buckets
               .Where(b => b.Owner == shard.Name && b.State == BucketState.Active)
               .Select(b => $"bucket_{b.Id}")
               .ToHashSet();
            var actual = runtime.BucketSchemas.ToHashSet();
            var missing = expected.Except(actual).Order().ToList(); // Order() без компаратора — Ordinal для строк
            var extra = actual.Except(expected).Order().ToList();
            if (missing.Count == 0 && extra.Count == 0)
                continue;

            yield return new Alert(
                $"{KindName}:{cluster.Name}/{shard.Name}",
                AlertSeverity.Warning,
                KindName,
                $"{cluster.Name}/{shard.Name}",
                $"инвентарь схем шарда {cluster.Name}/{shard.Name} не совпадает с routing: отсутствуют [{string.Join(", ", missing)}], лишние [{string.Join(", ", extra)}]",
                new Dictionary<string, string>
                {
                    ["missing"] = string.Join(", ", missing),
                    ["extra"] = string.Join(", ", extra),
                },
                null);
        }
    }
}
```

- [ ] **Step 4: AlertTestRules.cs — добавить 5 SQL-правил (внутрь `All()`, после t06-части Task 7)**

```csharp
            new SlotLagHighRule(Options.Create(new AlertsOptions())),
            new SlotWalLostRule(),
            new SlotInvalidationRiskRule(Options.Create(new AlertsOptions())),
            new SyncStandbyMissingRule(),
            new InventoryMismatchRule(),
```

- [ ] **Step 5: Прогнать тесты + весь юнит-набор**

Run: `dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~AdminPanel.UnitTests.HaAlertRulesTests"`
Expected: PASS, 20 passed (11 из Task 7 + 9 новых).
Run: `dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~AdminPanel.UnitTests" --no-build`
Expected: все зелёные — 24 правила в `AlertTestRules.All()` не рождают новых алертов на старых фикстурах (healthy-фикстура без HA/runtime).

- [ ] **Step 6: Commit**

```bash
git add src/AdminPanel.Core/Alerting/Rules/SlotLagHighRule.cs src/AdminPanel.Core/Alerting/Rules/SlotWalLostRule.cs src/AdminPanel.Core/Alerting/Rules/SlotInvalidationRiskRule.cs src/AdminPanel.Core/Alerting/Rules/SyncStandbyMissingRule.cs src/AdminPanel.Core/Alerting/Rules/InventoryMismatchRule.cs src/tests/AdminPanel.UnitTests/AlertTestRules.cs src/tests/AdminPanel.UnitTests/HaAlertRulesTests.cs
git commit -m "t06: alerts — slot-*/sync-standby-missing/inventory-mismatch + сквозной AlertEngine-сценарий (spec §5, §10.1 ч.2)"
```

**Выход:** все 9 HA-правил в общем списке; каталог 03 §4 закрыт полностью (24 kind'а).

---

### Task 9: HA-эндпоинты — HaQuery + маршруты + HTTP-тесты

**Files:**
- Create: `src/AdminPanel.Api/Inspection/HaQuery.cs`
- Modify: `src/AdminPanel.Api/Inspection/InspectionModule.cs` (+2 маршрута + исключение)
- Modify: `src/tests/AdminPanel.IntegrationTests/InspectionApiTests.cs` (+`InspectionSnapshots.Ha`)
- Test: `src/tests/AdminPanel.UnitTests/HaMappersTests.cs`
- Test: `src/tests/AdminPanel.IntegrationTests/HaApiTests.cs`

**Interfaces:**
- Consumes: `ISnapshotStore`, `InspectionModule.SnapshotNotReadyException` (t04), Result→HTTP-маппинг t05.
- Produces: `GET /api/ha` → 200 `IReadOnlyList<HaScopeSummaryDto>`; `GET /api/ha/{scope}` → 200 `HaScopeDto` / 404 `Scope not found`; `InspectionModule.ScopeNotFoundException`; `HaMappers.MapSummaries/MapDetails`.

**Вход:** Tasks 1–8 слиты; auth-guard `/api/*` уже закрыт; прецедент 404 — `ClusterNotFoundException` (t05 §6.1).

- [ ] **Step 1: Написать failing-тесты мапперов/хендлеров (HaMappersTests.cs, 5 тестов)**

```csharp
using AdminPanel.Api.Inspection;
using AdminPanel.Core;
using AdminPanel.Etcd;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Мапперы и хендлеры HA-зоны (spec §10.2): сводка (агрегаты) и детали (перенос полей).
public class HaMappersTests
{
    private static readonly DateTimeOffset Now = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public void MapSummaries_CountsAndFlags()
    {
        // Arrange
        var scopes = new[] { TestSnapshots.HaScopeDemo(Now), TestSnapshots.UnmatchedNoLeader(Now) };

        // Act
        var summaries = HaMappers.MapSummaries(scopes);

        // Assert: membersHealthy — running/streaming; lagMax — max LagBytes (spec §3.17).
        summaries.Should().HaveCount(2);
        var demo = summaries.Single(s => s.Scope == "demo-s1");
        demo.Cluster.Should().Be("demo");
        demo.Shard.Should().Be("s1");
        demo.Matched.Should().BeTrue();
        demo.LeaderName.Should().Be("s1a");
        demo.MembersTotal.Should().Be(2);
        demo.MembersHealthy.Should().Be(2);
        demo.LagMaxBytes.Should().Be(17L * 1024 * 1024);
        var other = summaries.Single(s => s.Scope == "other-scope");
        other.Matched.Should().BeFalse();
        other.Cluster.Should().BeNull();
        other.LeaderName.Should().BeNull();
        other.MembersTotal.Should().Be(1);
        other.MembersHealthy.Should().Be(0);
        other.LagMaxBytes.Should().BeNull();
    }

    [Fact]
    public void MapSummaries_EmptyLag_NullLagMaxBytes()
    {
        // Arrange: ни у одного члена лага нет.
        var scope = TestSnapshots.HaScopeDemo(Now) with
        {
            Members = [new HaMember("s1a", "s1a", 5432, "master", "running", 1L, null, Now, null)],
        };

        // Act
        var summary = HaMappers.MapSummaries([scope]).Single();

        // Assert
        summary.LagMaxBytes.Should().BeNull();
    }

    [Fact]
    public void MapDetails_FullTransfer()
    {
        // Arrange
        var scope = TestSnapshots.HaScopeDemo(Now);

        // Act
        var details = HaMappers.MapDetails(scope);

        // Assert: все поля модели → DTO (arch/03 §2 HaScopeDto; Initialized в DTO не входит).
        details.Scope.Should().Be("demo-s1");
        details.Cluster.Should().Be("demo");
        details.Shard.Should().Be("s1");
        details.Matched.Should().BeTrue();
        details.LeaderName.Should().Be("s1a");
        details.OptimeLeader.Should().Be(738273634528L);
        details.RawConfig.Should().Be("{\"ttl\":5,\"loop_wait\":2}");
        var member = details.Members.Should().ContainSingle(m => m.Name == "s1b").Subject;
        member.Host.Should().Be("s1b");
        member.Port.Should().Be(5432);
        member.Role.Should().Be("replica");
        member.State.Should().Be("streaming");
        member.Timeline.Should().Be(1L);
        member.LagBytes.Should().Be(17L * 1024 * 1024);
        member.ProbeAtUtc.Should().Be(Now);
        member.ProbeError.Should().BeNull();
    }

    [Fact]
    public async Task HaScopesHandler_NoSnapshot_ReturnsSnapshotNotReady()
    {
        // Arrange
        var handler = new HaScopesQueryHandler(new SnapshotStore());

        // Act
        var result = await handler.Handle(new HaScopesQuery(), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<InspectionModule.SnapshotNotReadyException>();
    }

    [Fact]
    public async Task HaDetailsHandler_UnknownScope_ReturnsScopeNotFound()
    {
        // Arrange: снапшот есть, скопа нет.
        var store = new SnapshotStore();
        store.Replace(TestSnapshots.WithHaScopes(Now));
        var handler = new HaScopeDetailsQueryHandler(store);

        // Act
        var result = await handler.Handle(new HaScopeDetailsQuery("nope"), CancellationToken.None);

        // Assert: 404-семантика (spec §3.18) — исключение различается эндпоинтом.
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<InspectionModule.ScopeNotFoundException>();
    }
}
```

- [ ] **Step 2: Прогнать — ошибка компиляции**

Run: `dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~AdminPanel.UnitTests.HaMappersTests" --no-restore`
Expected: FAIL (CS0246 `HaMappers`/хендлеры не найдены).

- [ ] **Step 3: Реализовать HaQuery.cs**

```csharp
using AdminPanel.Core;
using AdminPanel.Etcd;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Inspection;

// Запрос сводного списка HA-скопов (arch/03 §1).
public sealed record HaScopesQuery : IQuery<IReadOnlyList<HaScopeSummaryDto>>;

// Сводка скопа — UI-таблица HA (03 §3; spec §3.17): агрегаты по членам.
public sealed record HaScopeSummaryDto(
    string Scope,
    string? Cluster,
    string? Shard,
    bool Matched,
    string? LeaderName,
    int MembersTotal,
    int MembersHealthy,
    long? LagMaxBytes);

// Запрос деталей HA-скопа (arch/03 §1).
public sealed record HaScopeDetailsQuery(string Scope) : IQuery<HaScopeDto>;

// Детали скопа — arch/03 §2 HaScopeDto дословно (spec §3.18); Initialized модели
// в контракт 03 §2 не входит и не отдаётся.
public sealed record HaScopeDto(
    string Scope,
    string? Cluster,
    string? Shard,
    bool Matched,
    string? LeaderName,
    long? OptimeLeader,
    IReadOnlyList<HaMemberDto> Members,
    string? RawConfig);

public sealed record HaMemberDto(
    string Name,
    string Host,
    int? Port,
    string? Role,
    string? State,
    long? Timeline,
    long? LagBytes,
    DateTimeOffset? ProbeAtUtc,
    string? ProbeError);

// Core → DTO: чистые функции; порядок — как в снапшоте (парсер Scope Ordinal, t03).
public static class HaMappers
{
    public static IReadOnlyList<HaScopeSummaryDto> MapSummaries(IReadOnlyList<HaScope> scopes)
        => [.. scopes.Select(scope => new HaScopeSummaryDto(
            scope.Scope,
            scope.Cluster,
            scope.Shard,
            scope.Matched,
            scope.LeaderName,
            scope.Members.Count,
            scope.Members.Count(m => m.State is "running" or "streaming"),
            scope.Members.Any(m => m.LagBytes is not null) ? scope.Members.Max(m => m.LagBytes) : null))];

    public static HaScopeDto MapDetails(HaScope scope)
        => new(
            scope.Scope,
            scope.Cluster,
            scope.Shard,
            scope.Matched,
            scope.LeaderName,
            scope.OptimeLeader,
            [.. scope.Members.Select(m => new HaMemberDto(
                m.Name, m.Host, m.Port, m.Role, m.State, m.Timeline, m.LagBytes, m.ProbeAtUtc, m.ProbeError))],
            scope.RawConfig);
}

[InjectAsScoped]
public sealed class HaScopesQueryHandler(ISnapshotStore store)
    : IQueryHandler<HaScopesQuery, IReadOnlyList<HaScopeSummaryDto>>
{
    public ValueTask<Result<IReadOnlyList<HaScopeSummaryDto>>> Handle(HaScopesQuery query, CancellationToken ct)
    {
        var snapshot = store.Current;
        return ValueTask.FromResult(snapshot is null
            ? Result<IReadOnlyList<HaScopeSummaryDto>>.Failed(new SnapshotNotReadyException())
            : Result<IReadOnlyList<HaScopeSummaryDto>>.Success(HaMappers.MapSummaries(snapshot.HaScopes)));
    }
}

// Хендлер деталей: 503 «снапшота нет» / 404 «скоп не найден» (spec §3.18).
[InjectAsScoped]
public sealed class HaScopeDetailsQueryHandler(ISnapshotStore store)
    : IQueryHandler<HaScopeDetailsQuery, HaScopeDto>
{
    public ValueTask<Result<HaScopeDto>> Handle(HaScopeDetailsQuery query, CancellationToken ct)
    {
        var snapshot = store.Current;
        if (snapshot is null)
            return ValueTask.FromResult(Result<HaScopeDto>.Failed(new SnapshotNotReadyException()));

        var scope = snapshot.HaScopes.FirstOrDefault(s => s.Scope == query.Scope);
        return ValueTask.FromResult(scope is null
            ? Result<HaScopeDto>.Failed(new ScopeNotFoundException(query.Scope))
            : Result<HaScopeDto>.Success(HaMappers.MapDetails(scope)));
    }
}
```

- [ ] **Step 4: InspectionModule.cs — исключение + 2 маршрута**

4a. Рядом с `ClusterNotFoundException` добавить:

```csharp
    // HA-scope отсутствует в снапшоте: 404 — как неизвестный кластер (spec §3.18).
    public sealed class ScopeNotFoundException(string scope)
        : Exception($"HA-scope {scope} не найден в снапшоте");
```

4b. Перед `endpoints.MapGet("/api/alerts", …)` добавить маршруты:

```csharp
        // GET /api/ha — сводный список HA-скопов (arch/03 §1).
        endpoints.MapGet("/api/ha", async (IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleQuery<HaScopesQuery, IReadOnlyList<HaScopeSummaryDto>>(
                new HaScopesQuery(), ct);
            return ResultToHttp(result);
        });

        // GET /api/ha/{scope} — детали скопа (arch/03 §1); ScopeNotFoundException → 404,
        // прочий отказ → 503 — маппинг как у /api/clusters/{cluster} (t05 §6.1).
        endpoints.MapGet("/api/ha/{scope}", async (string scope, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleQuery<HaScopeDetailsQuery, HaScopeDto>(
                new HaScopeDetailsQuery(scope), ct);
            if (result.IsSuccess)
                return Results.Ok(result.Value);
            return result.Error is ScopeNotFoundException
                ? Results.Problem(
                    statusCode: StatusCodes.Status404NotFound,
                    title: "Scope not found",
                    detail: result.Error.Message)
                : Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable,
                    title: "Snapshot not ready",
                    detail: result.Error!.Message);
        });
```

- [ ] **Step 5: Прогнать юнит-тесты**

Run: `dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~AdminPanel.UnitTests.HaMappersTests"`
Expected: PASS, 5 passed.

- [ ] **Step 6: Integration — фикстура + HaApiTests.cs (6 тестов)**

В `InspectionApiTests.cs` внутрь `InspectionSnapshots` добавить фикстуру:

```csharp
    // HA-фикстура HTTP-тестов (spec §9.2): demo-s1 с пробами, other-scope unmatched
    // с упавшей пробой; alerts — руками из Fixture (движок тут не работает).
    public static EtcdSnapshot Ha(DateTimeOffset builtAt, DateTimeOffset now)
    {
        var scopes = new List<AdminPanel.Core.HaScope>
        {
            new("demo-s1", "demo", "s1", true, "s1a", 738273634528L, true,
                [
                    new HaMember("s1a", "s1a", 5432, "master", "running", 1L, 0L, now, null),
                    new HaMember("s1b", "s1b", 5432, "replica", "streaming", 1L, 17L * 1024 * 1024, now, null),
                ],
                "{\"ttl\":5,\"loop_wait\":2}"),
            new("other-scope", null, null, false, null, null, false,
                [new HaMember("n1", "n1", 5432, "replica", "stopped", null, null, now, "connection refused")],
                null),
        };
        return Fixture(builtAt) with { HaScopes = scopes };
    }
```

`src/tests/AdminPanel.IntegrationTests/HaApiTests.cs`:

```csharp
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using AdminPanel.Core;
using FluentAssertions;
using Xunit;

namespace AdminPanel.IntegrationTests;

// HTTP-контракт HA-эндпоинтов (spec §9.2): 401/503/200/404 + probe-поля DTO.
[Collection("api")]
public class HaApiTests
{
    private readonly AuthWebFactory _factory;

    public HaApiTests(AuthWebFactory factory) => _factory = factory;

    private async Task<HttpClient> LoginAsync() => await ApiTestLogin.LoginAsync(_factory);

    private async Task<JsonElement> GetJsonAsync(HttpClient client, string url)
    {
        using var response = await client.GetAsync(url, TestContext.Current.CancellationToken);
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        return await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Ha_WithoutCookie_Return401()
    {
        // Arrange
        using var client = _factory.CreateClient();

        // Act
        var list = await client.GetAsync("/api/ha", TestContext.Current.CancellationToken);
        var details = await client.GetAsync("/api/ha/demo-s1", TestContext.Current.CancellationToken);

        // Assert: default-deny guard закрыл новые эндпоинты без правок auth.
        list.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
        details.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task Ha_NoSnapshot_Return503ProblemDetails()
    {
        // Arrange
        _factory.Snapshot = null;
        using var client = await LoginAsync();

        // Act
        var list = await client.GetAsync("/api/ha", TestContext.Current.CancellationToken);
        var details = await client.GetAsync("/api/ha/demo-s1", TestContext.Current.CancellationToken);

        // Assert
        list.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
        list.Content.Headers.ContentType!.MediaType.Should().Be("application/problem+json");
        var body = await list.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        body.GetProperty("title").GetString().Should().Be("Snapshot not ready");
        details.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
    }

    [Fact]
    public async Task Ha_WithSnapshot_ReturnSummaries()
    {
        // Arrange
        _factory.Snapshot = InspectionSnapshots.Ha(_factory.Time.Utc, _factory.Time.Utc);
        using var client = await LoginAsync();

        // Act
        var summaries = await GetJsonAsync(client, "/api/ha");

        // Assert: порядок Scope Ordinal; агрегаты по членам (spec §3.17).
        summaries.GetArrayLength().Should().Be(2);
        var demo = summaries[0];
        demo.GetProperty("scope").GetString().Should().Be("demo-s1");
        demo.GetProperty("cluster").GetString().Should().Be("demo");
        demo.GetProperty("shard").GetString().Should().Be("s1");
        demo.GetProperty("matched").GetBoolean().Should().BeTrue();
        demo.GetProperty("leaderName").GetString().Should().Be("s1a");
        demo.GetProperty("membersTotal").GetInt32().Should().Be(2);
        demo.GetProperty("membersHealthy").GetInt32().Should().Be(2);
        demo.GetProperty("lagMaxBytes").GetInt64().Should().Be(17L * 1024 * 1024);
        var other = summaries[1];
        other.GetProperty("scope").GetString().Should().Be("other-scope");
        other.GetProperty("matched").GetBoolean().Should().BeFalse();
        other.GetProperty("leaderName").ValueKind.Should().Be(JsonValueKind.Null);
        other.GetProperty("membersHealthy").GetInt32().Should().Be(0);
        other.GetProperty("lagMaxBytes").ValueKind.Should().Be(JsonValueKind.Null);
    }

    [Fact]
    public async Task HaDetails_ReturnsMembersWithProbeFields()
    {
        // Arrange
        _factory.Snapshot = InspectionSnapshots.Ha(_factory.Time.Utc, _factory.Time.Utc);
        using var client = await LoginAsync();

        // Act
        var dto = await GetJsonAsync(client, "/api/ha/demo-s1");

        // Assert
        dto.GetProperty("optimeLeader").GetInt64().Should().Be(738273634528L);
        dto.GetProperty("rawConfig").GetString().Should().Contain("loop_wait");
        var member = dto.GetProperty("members")[1];
        member.GetProperty("name").GetString().Should().Be("s1b");
        member.GetProperty("timeline").GetInt64().Should().Be(1L);
        member.GetProperty("lagBytes").GetInt64().Should().Be(17L * 1024 * 1024);
        member.GetProperty("probeAtUtc").GetString().Should().NotBeNullOrEmpty();
        member.GetProperty("probeError").ValueKind.Should().Be(JsonValueKind.Null);
    }

    [Fact]
    public async Task HaDetails_MemberProbeError_Visible()
    {
        // Arrange
        _factory.Snapshot = InspectionSnapshots.Ha(_factory.Time.Utc, _factory.Time.Utc);
        using var client = await LoginAsync();

        // Act
        var dto = await GetJsonAsync(client, "/api/ha/other-scope");

        // Assert: ошибка пробы видна, DCS role/state остались (spec §3.5).
        var member = dto.GetProperty("members")[0];
        member.GetProperty("role").GetString().Should().Be("replica");
        member.GetProperty("state").GetString().Should().Be("stopped");
        member.GetProperty("timeline").ValueKind.Should().Be(JsonValueKind.Null);
        member.GetProperty("lagBytes").ValueKind.Should().Be(JsonValueKind.Null);
        member.GetProperty("probeError").GetString().Should().Be("connection refused");
    }

    [Fact]
    public async Task HaDetails_UnknownScope_Return404ProblemDetails()
    {
        // Arrange
        _factory.Snapshot = InspectionSnapshots.Ha(_factory.Time.Utc, _factory.Time.Utc);
        using var client = await LoginAsync();

        // Act
        using var response = await client.GetAsync("/api/ha/nope", TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
        response.Content.Headers.ContentType!.MediaType.Should().Be("application/problem+json");
        var body = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        body.GetProperty("title").GetString().Should().Be("Scope not found");
    }
}
```

- [ ] **Step 7: Прогнать integration-класс + Commit**

Run: `dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~AdminPanel.IntegrationTests.HaApiTests"`
Expected: PASS, 6 passed (Docker не нужен — фабрика с TestSnapshotStore).

```bash
git add src/AdminPanel.Api/Inspection/HaQuery.cs src/AdminPanel.Api/Inspection/InspectionModule.cs src/tests/AdminPanel.IntegrationTests/InspectionApiTests.cs src/tests/AdminPanel.IntegrationTests/HaApiTests.cs src/tests/AdminPanel.UnitTests/HaMappersTests.cs
git commit -m "t06: api — GET /api/ha + /api/ha/{scope} (сводка/детали, 404 Scope not found) + мапперы/хендлеры (spec §6, §9.2, §10.2)"
```

**Выход:** таблица эндпоинтов arch/03 §1 закрыта полностью.

---

### Task 10: Живой путь данных — refresher(+пробы) → AlertEngine → API

**Files:**
- Modify: `src/tests/AdminPanel.IntegrationTests/EtcdSnapshotIntegrationTests.cs` (harness: probes-параметр + double; +1 тест)
- Create: `src/tests/AdminPanel.IntegrationTests/InspectionProbeApiTests.cs` (2 теста)
- Modify: `src/tests/AdminPanel.IntegrationTests/InspectionApiTests.cs` (`InspectionEtcdApiTests` — HA-смоук)

**Interfaces:**
- Consumes: `SnapshotRefresher`-ctor t06 (Task 2), `ProbeState` (Task 1), `AlertTestRules` — интеграционный список правил хранится в `EtcdTestHarness` (свой, не из UnitTests).
- Produces: `EtcdTestHarness.NewRefresher(ISnapshotStore store, IProbeStateStore? probes, params string[] endpoints)`; `SettableProbeStateStore` (integration, public для `InspectionProbeApiTests`).

**Вход:** Tasks 1–9 слиты; сид `EtcdSeed.Demo` содержит `/service/demo-s1`, `/service/demo-s2`, шарды `demo/s1`, `demo/s2`; существующие строгие ассерты живого etcd НЕ должны измениться (пустой стор проб → HA-правила молчат, spec §16).

- [ ] **Step 1: Написать failing-тесты (enrich-тест в EtcdSnapshotIntegrationTests + InspectionProbeApiTests.cs — листинг ниже в Step 4)**

В класс `EtcdSnapshotIntegrationTests` добавить тест:

```csharp
    [Fact]
    public async Task Refresher_EnrichesSnapshot_FromProbeState()
    {
        // Arrange: живой etcd с сидом + стор проб (members demo-s1, runtime demo/s1).
        // Инвентарь s1 — чётные bucket_0..14: ровно ожидания routing сида (8/8 round-robin).
        var at = DateTimeOffset.UtcNow;
        var probes = new SettableProbeStateStore
        {
            Current = new ProbeState(
                at,
                [new ProbeResult("demo-s1/s1b", "patroni", true, 1.0, null, at)],
                new Dictionary<string, HaMemberProbe>
                {
                    ["demo-s1/s1a"] = new("master", "running", 1L, 0L, at, null),
                    ["demo-s1/s1b"] = new("replica", "streaming", 2L, 123L, at, null),
                },
                new Dictionary<string, ShardRuntime>
                {
                    ["demo/s1"] = new(
                        "s1", [], [], [],
                        [.. Enumerable.Range(0, 16).Where(i => i % 2 == 0).Select(i => $"bucket_{i}")],
                        false, null),
                }),
        };
        var store = new SnapshotStore();
        var refresher = EtcdTestHarness.NewRefresher(store, probes, fixture.Endpoint);

        // Act
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert: обогащение в снапшоте; инвентарь = routing => inventory-mismatch нет;
        // standbies пусты => sync-standby-missing есть; проб-ошибок нет.
        result.IsSuccess.Should().BeTrue();
        var snapshot = store.Current!;
        var member = snapshot.HaScopes.Single(s => s.Scope == "demo-s1").Members.Single(m => m.Name == "s1b");
        member.Timeline.Should().Be(2L);
        member.LagBytes.Should().Be(123L);
        snapshot.Probes.Should().ContainSingle().Which.Ok.Should().BeTrue();
        var runtime = snapshot.Clusters.Single().Shards.Single(s => s.Name == "s1").Runtime;
        runtime.Should().NotBeNull();
        snapshot.Alerts.Should().NotContain(a => a.Kind == "inventory-mismatch");
        snapshot.Alerts.Should().Contain(a => a.Id == "sync-standby-missing:demo/s1");
        snapshot.Alerts.Should().NotContain(a => a.Kind == "probe-failed");
    }
```

- [ ] **Step 2: Прогнать — падение компиляции**

Run: `dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~AdminPanel.IntegrationTests.EtcdSnapshotIntegrationTests" --no-restore`
Expected: FAIL (CS1501/CS1503: у `EtcdTestHarness.NewRefresher` нет перегрузки с `IProbeStateStore`).

- [ ] **Step 3: Реализовать harness-правки (файл EtcdSnapshotIntegrationTests.cs)**

3a. Публичный double рядом с `EtcdTestHarness`:

```csharp
// Управляемый стор состояния проб для живых сценариев (spec §9.4; unit-аналог —
// SettableProbeStateStore в юнит-сборке).
public sealed class SettableProbeStateStore : IProbeStateStore
{
    public ProbeState? Current { get; set; }

    public void Replace(ProbeState state) => Current = state;
}
```

3b. `NewRefresher` — старая сигнатура обёрткой + расширенная (вставить `IProbeStateStore` после store в ctor — позиция Task 2):

```csharp
    public static SnapshotRefresher NewRefresher(ISnapshotStore store, params string[] endpoints)
        => NewRefresher(store, null, endpoints);

    public static SnapshotRefresher NewRefresher(
        ISnapshotStore store,
        IProbeStateStore? probes,
        params string[] endpoints)
        => new(
            NewGateway(),
            new AlertEngine(
            [
                // t04+t05: 15 правил (список как раньше, без изменений)
                new EtcdUnreachableRule(),
                new EtcdNoQuorumRule(),
                new EtcdEndpointDownRule(),
                new EtcdAlarmRule(),
                new SnapshotStaleRule(),
                new ClusterIncompleteRule(),
                new KeyMalformedRule(),
                new ShardNoMasterRule(),
                new MoveStaleRule(Options.Create(new AlertsOptions())),
                new MoveFrozenLongRule(Options.Create(new AlertsOptions())),
                new MoveAbortingRule(),
                new MoveFlippedStatusStuckRule(),
                new BucketLostRule(),
                new BucketNoRoutingRule(),
                new BucketOutOfRangeRule(),
                // t06: 9 HA-правил (spec §5)
                new ShardNoLeaderRule(),
                new HaMemberNotStreamingRule(),
                new ReplicaLagHighRule(Options.Create(new AlertsOptions())),
                new SlotLagHighRule(Options.Create(new AlertsOptions())),
                new SlotWalLostRule(),
                new SlotInvalidationRiskRule(Options.Create(new AlertsOptions())),
                new SyncStandbyMissingRule(),
                new InventoryMismatchRule(),
                new ProbeFailedRule(),
            ]),
            store,
            probes ?? new SettableProbeStateStore(),
            Options.Create(new EtcdOptions { Endpoints = endpoints }),
            new RealTimeProvider(),
            NullLogger<SnapshotRefresher>.Instance);
```

(`using AdminPanel.Core;` в файле уже есть.)

- [ ] **Step 4: Написать InspectionProbeApiTests.cs (2 теста; harness уже готов из Step 3)**

```csharp
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using AdminPanel.Core;
using AdminPanel.Etcd;
using FluentAssertions;
using Xunit;

namespace AdminPanel.IntegrationTests;

// Живой путь «etcd-сид → refresher(+состояние проб) → API» (spec §9.3): клейка —
// перенос снапшота в TestSnapshotStore фабрики (прецедент t04 §3.17).
[Collection("api")]
public class InspectionProbeApiTests(AuthWebFactory factory, EtcdContainerFixture fixture)
    : IClassFixture<EtcdContainerFixture>
{
    private readonly AuthWebFactory _factory = factory;

    private async Task<EtcdSnapshot> RefreshedAsync(ProbeState? probes)
    {
        var store = new SnapshotStore();
        var probeStore = new SettableProbeStateStore { Current = probes };
        var refresher = EtcdTestHarness.NewRefresher(store, probeStore, fixture.Endpoint);
        (await refresher.RefreshOnceAsync(CancellationToken.None)).IsSuccess.Should().BeTrue();
        return store.Current!;
    }

    [Fact]
    public async Task LiveEtcd_ProbeStateEnriches_HaAndClusterApi()
    {
        // Arrange: проб-состояние с member-обогащением demo-s1 и runtime demo/s1.
        var at = DateTimeOffset.UtcNow;
        var probes = new ProbeState(
            at,
            [],
            new Dictionary<string, HaMemberProbe>
            {
                ["demo-s1/s1a"] = new("master", "running", 1L, 0L, at, null),
                ["demo-s1/s1b"] = new("replica", "streaming", 2L, 4096L, at, null),
            },
            new Dictionary<string, ShardRuntime>
            {
                ["demo/s1"] = new(
                    "s1",
                    [],
                    [new StandbyInfo("s1b", "10.0.0.2", "streaming", "sync", 0L)],
                    [],
                    [.. Enumerable.Range(0, 16).Where(i => i % 2 == 0).Select(i => $"bucket_{i}")],
                    false,
                    null),
            });
        _factory.Snapshot = await RefreshedAsync(probes);
        using var client = await ApiTestLogin.LoginAsync(_factory);

        // Act
        var haList = await client.GetAsync("/api/ha", TestContext.Current.CancellationToken);
        var haDetails = await client.GetAsync("/api/ha/demo-s1", TestContext.Current.CancellationToken);
        var cluster = await client.GetAsync("/api/clusters/demo", TestContext.Current.CancellationToken);
        var failed = await client.GetAsync("/api/alerts?kind=probe-failed", TestContext.Current.CancellationToken);

        // Assert: timeline/lag видны в API; runtime шарда не null; probe-failed пуст.
        haList.StatusCode.Should().Be(HttpStatusCode.OK);
        var list = await haList.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        list.GetArrayLength().Should().Be(2); // demo-s1 + demo-s2 сида
        var details = await haDetails.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        var s1b = details.GetProperty("members")[1];
        s1b.GetProperty("timeline").GetInt64().Should().Be(2L);
        s1b.GetProperty("lagBytes").GetInt64().Should().Be(4096L);
        s1b.GetProperty("probeAtUtc").GetString().Should().NotBeNullOrEmpty();
        var clusterDto = await cluster.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        var runtime = clusterDto.GetProperty("shards")[0].GetProperty("runtime");
        runtime.ValueKind.Should().NotBe(JsonValueKind.Null);
        runtime.GetProperty("standbiesSync").GetInt32().Should().Be(1);
        runtime.GetProperty("bucketSchemas").GetArrayLength().Should().Be(8); // чётные = routing s1 (8/8)
        var failedList = await failed.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        failedList.GetArrayLength().Should().Be(0);
    }

    [Fact]
    public async Task LiveEtcd_FailedProbe_ProducesProbeFailedAlert()
    {
        // Arrange: patroni-проба члена упала (детали ошибки — в details алерта).
        var at = DateTimeOffset.UtcNow;
        var probes = new ProbeState(
            at,
            [new ProbeResult("demo-s1/s1a", "patroni", false, 2.0, "connection refused", at)],
            new Dictionary<string, HaMemberProbe>
            {
                ["demo-s1/s1a"] = new(null, null, null, null, at, "connection refused"),
            },
            []);
        _factory.Snapshot = await RefreshedAsync(probes);
        using var client = await ApiTestLogin.LoginAsync(_factory);

        // Act
        using var response = await client.GetAsync("/api/alerts", TestContext.Current.CancellationToken);

        // Assert: info-алерт probe-failed с kind в target; ha-member-not-streaming
        // по упавшей пробе не вычисляется (spec §3.13/§3.14).
        var alerts = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        var probeAlert = alerts.EnumerateArray().Single(a =>
            a.GetProperty("id").GetString() == "probe-failed:patroni:demo-s1/s1a");
        probeAlert.GetProperty("severity").GetString().Should().Be("info");
        alerts.EnumerateArray().Should().NotContain(a => a.GetProperty("kind").GetString() == "ha-member-not-streaming");
    }
}
```

- [ ] **Step 5: Прогнать живые сценарии (реализация — harness Step 3)**

Run: `dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~AdminPanel.IntegrationTests.InspectionProbeApiTests|FullyQualifiedName~AdminPanel.IntegrationTests.EtcdSnapshotIntegrationTests"`
Expected: PASS — новый enrich-тест + 2 API-теста + все существующие живые etcd-тесты без правок ассертов (нужен Docker).

- [ ] **Step 6: HA-смоук в InspectionEtcdApiTests (правка существующего теста)**

В `LiveEtcd_InspectionEndpoints_ReflectRealSnapshot` после блока `details` добавить:

```csharp
        // t06: HA-эндпоинты против живого сида (без проб — обогащение только через стор, §3.1).
        using var haList = await client.GetAsync("/api/ha", TestContext.Current.CancellationToken);
        var haScopes = await haList.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        haScopes.GetArrayLength().Should().Be(2);
        haScopes[0].GetProperty("scope").GetString().Should().Be("demo-s1");
        haScopes[0].GetProperty("leaderName").GetString().Should().Be("s1a");
        using var haDetails = await client.GetAsync("/api/ha/demo-s1", TestContext.Current.CancellationToken);
        var haDto = await haDetails.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        haDto.GetProperty("members").GetArrayLength().Should().Be(2);
        haDto.GetProperty("members")[0].GetProperty("timeline").ValueKind.Should().Be(JsonValueKind.Null);
```

Run: `dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~AdminPanel.IntegrationTests.InspectionEtcdApiTests"`
Expected: PASS (расширенный тест зелёный, остальные ассерты без правок).

- [ ] **Step 7: Commit**

```bash
git add src/tests/AdminPanel.IntegrationTests/EtcdSnapshotIntegrationTests.cs src/tests/AdminPanel.IntegrationTests/InspectionProbeApiTests.cs src/tests/AdminPanel.IntegrationTests/InspectionApiTests.cs
git commit -m "t06: integration — живой путь refresher(+пробы)→AlertEngine→/api/ha|clusters|alerts + HA-смоук сида (spec §9.3–9.4, §9.7)"
```

**Выход:** enrichment и 9 правил подтверждены на живом etcd-сиде end-to-end.

---

### Task 11: Живые пробы — HTTP-стаб Patroni + Testcontainers postgres:18

**Files:**
- Create: `src/tests/AdminPanel.IntegrationTests/PatroniRestProbeTests.cs` (5 тестов)
- Create: `src/tests/AdminPanel.IntegrationTests/PostgresFixture.cs`
- Create: `src/tests/AdminPanel.IntegrationTests/SqlProbeIntegrationTests.cs` (5 тестов)

**Interfaces:**
- Consumes: `PatroniRestProbe`, `PatroniClusterParser`, `HostMapResolver` (Tasks 3–4); `SqlProbe.BuildConnectionString` (Task 5); `AlertEngine` + правила (Tasks 7–8).
- Produces: `PostgresFixture { int Port }`.

**Вход:** Tasks 1–10 слиты; Npgsql доступен в IntegrationTests транзитивно (Api → Probes); `EtcdContainerFixture` — паттерн контейнера.

Замечание к spec §9.5: кейс `Probe_UnmappedHost_DirectConnection` заменён негативной формой `Probe_UnmappedHost_FailsWithOriginalHost` — REST-порт фиксирован `:8008` (arch/02 §6.1), стаб слушает случайный порт, позитивная identity-проверка без маппинга недостижима; identity покрыт unit-тестами `HostMapResolverTests` (§10.4). Это уточнение реализации, не отклонение spec.

- [ ] **Step 1: PatroniRestProbeTests.cs (5 тестов, HttpListener-стаб)**

```csharp
using System.Net;
using System.Net.Sockets;
using System.Text;
using AdminPanel.Core;
using AdminPanel.Probes;
using FluentAssertions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.IntegrationTests;

// Patroni-проба против локального HTTP-стаба (spec §9.5): HostMap e2e, своя запись
// member'а, ошибки транспорта/отсутствия записи. Стаб — кроссплатформенный
// HttpListener, отдаёт Patroni /cluster JSON на любой GET.
public class PatroniRestProbeTests : IAsyncLifetime
{
    // Инлайн-копия фикстуры patroni-cluster.json (integration-сборка не видит файлы UnitTests).
    private const string ClusterJson = """
        {"members":[
          {"name":"s1a","host":"10.0.0.11","port":5432,"role":"master","state":"running","timeline":1,"lag":0},
          {"name":"s1b","host":"10.0.0.12","port":5432,"role":"replica","state":"streaming","timeline":2,"lag":4096},
          {"name":"s1c","host":"10.0.0.13","port":5432,"role":"replica","state":"stopped","timeline":1,"lag":null}
        ]}
        """;

    private readonly HttpListener _server = new();
    private int _port;

    public async ValueTask InitializeAsync()
    {
        // Свободный порт: захват TcpListener(0), затем HttpListener на нём.
        var probe = new TcpListener(IPAddress.Loopback, 0);
        probe.Start();
        _port = ((IPEndPoint)probe.LocalEndpoint).Port;
        probe.Stop();

        _server.Prefixes.Add($"http://127.0.0.1:{_port}/");
        _server.Start();
        _ = Task.Run(ServeAsync);
    }

    public ValueTask DisposeAsync()
    {
        _server.Stop();
        return ValueTask.CompletedTask;
    }

    private async Task ServeAsync()
    {
        while (_server.IsListening)
        {
            HttpListenerContext context;
            try
            {
                context = await _server.GetContextAsync();
            }
            catch (Exception)
            {
                return; // слушатель остановлен тестом
            }

            try
            {
                var body = Encoding.UTF8.GetBytes(ClusterJson);
                context.Response.ContentType = "application/json";
                context.Response.ContentLength64 = body.Length;
                await context.Response.OutputStream.WriteAsync(body);
            }
            catch (Exception)
            {
                // клиент оборвал соединение — не роняем стаб
            }
            finally
            {
                context.Response.Close();
            }
        }
    }

    private static HaScope Scope() => new(
        "demo-s1", "demo", "s1", true, "s1a", null, true,
        [Member("s1a"), Member("s1b"), Member("zz")],
        null);

    private static HaMember Member(string name)
        => new(name, name, 5432, null, null, null, null, null, null);

    private PatroniRestProbe Probe(Dictionary<string, string>? hostMap = null) => new(
        new HttpClient { Timeout = TimeSpan.FromSeconds(3) },
        Options.Create(new ProbesOptions { HostMap = hostMap ?? [] }),
        TimeProvider.System);

    [Fact]
    public async Task Probe_MapsHostAndParsesSelfEntry()
    {
        // Arrange: s1a:8008 маппится на стаб.
        var probe = Probe(new Dictionary<string, string> { ["s1a:8008"] = $"127.0.0.1:{_port}" });

        // Act
        var result = await probe.ProbeAsync(Scope(), Member("s1a"), CancellationToken.None);

        // Assert: своя запись; latency измерена; target kind по §3.14.
        result.Enrichment.Role.Should().Be("master");
        result.Enrichment.State.Should().Be("running");
        result.Enrichment.Timeline.Should().Be(1L);
        result.Enrichment.Error.Should().BeNull();
        result.Result.Ok.Should().BeTrue();
        result.Result.Target.Should().Be("demo-s1/s1a");
        result.Result.Kind.Should().Be("patroni");
        result.Result.LatencyMs.Should().BePositive();
    }

    [Fact]
    public async Task Probe_AnotherMember_PicksOwnEntry()
    {
        // Arrange
        var probe = Probe(new Dictionary<string, string> { ["s1b:8008"] = $"127.0.0.1:{_port}" });

        // Act
        var result = await probe.ProbeAsync(Scope(), Member("s1b"), CancellationToken.None);

        // Assert: запись s1b — другие timeline/лаг, чем у s1a.
        result.Enrichment.Role.Should().Be("replica");
        result.Enrichment.Timeline.Should().Be(2L);
        result.Enrichment.LagBytes.Should().Be(4096L);
    }

    [Fact]
    public async Task Probe_MemberMissingInResponse_Error()
    {
        // Arrange: member "zz" в ответе стаба нет (spec §3.4).
        var probe = Probe(new Dictionary<string, string> { ["zz:8008"] = $"127.0.0.1:{_port}" });

        // Act
        var result = await probe.ProbeAsync(Scope(), Member("zz"), CancellationToken.None);

        // Assert
        result.Result.Ok.Should().BeFalse();
        result.Enrichment.Error.Should().Contain("не найден");
    }

    [Fact]
    public async Task Probe_DeadPort_ReturnsError()
    {
        // Arrange: HostMap ведёт на закрытый порт.
        var probe = Probe(new Dictionary<string, string> { ["s1a:8008"] = "127.0.0.1:1" });

        // Act
        var result = await probe.ProbeAsync(Scope(), Member("s1a"), CancellationToken.None);

        // Assert: ошибка целиком в результат, enrichment с Error, лагов нет (spec §3.5).
        result.Result.Ok.Should().BeFalse();
        result.Result.Error.Should().NotBeNullOrEmpty();
        result.Enrichment.Timeline.Should().BeNull();
        result.Enrichment.LagBytes.Should().BeNull();
    }

    [Fact]
    public async Task Probe_UnmappedHost_FailsWithOriginalHost()
    {
        // Arrange: хост без записи карты — идёт на исходный адрес :8008 (identity,
        // unit-покрыт HostMapResolverTests); .invalid не резолвится — отказ транспорта.
        var probe = Probe();

        // Act
        var result = await probe.ProbeAsync(Scope(), Member("s1a"), CancellationToken.None);

        // Assert: identity-ветка не падает, даёт штатный failed-результат.
        result.Result.Ok.Should().BeFalse();
        result.Result.Target.Should().Be("demo-s1/s1a");
        result.Enrichment.Error.Should().NotBeNullOrEmpty();
    }
}
```

- [ ] **Step 2: Прогнать стаб-тесты**

Run: `dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~AdminPanel.IntegrationTests.PatroniRestProbeTests"`
Expected: PASS, 5 passed (Docker не нужен).

- [ ] **Step 3: PostgresFixture.cs**

```csharp
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Npgsql;
using Xunit;

namespace AdminPanel.IntegrationTests;

// Testcontainers postgres:18 (spec §9.6): trust-стенд + wal_level=logical ради живых
// логических слотов; готовность — ретрай-подключение Npgsql (паттерн EtcdContainerFixture).
// IClassFixture — контейнер на тестовый класс, изоляция между классами.
public sealed class PostgresFixture : IAsyncLifetime
{
    private readonly IContainer _container = new ContainerBuilder("postgres:18")
        .WithEnvironment("POSTGRES_HOST_AUTH_METHOD", "trust")
        .WithCommand("postgres", "-c", "wal_level=logical")
        .WithPortBinding(5432, assignRandomHostPort: true)
        .Build();

    public int Port { get; private set; }

    public string ConnectionString => $"Host=127.0.0.1;Port={Port};Username=postgres;Timeout=5";

    public async ValueTask InitializeAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        await _container.StartAsync(ct);
        Port = _container.GetMappedPublicPort(5432);

        for (var i = 0; i < 30; i++)
        {
            try
            {
                await using var connection = new NpgsqlConnection(ConnectionString);
                await connection.OpenAsync(ct);
                return;
            }
            catch (NpgsqlException)
            {
                // postgres ещё поднимается — ждём следующую попытку
                await Task.Delay(1000, ct);
            }
        }

        throw new InvalidOperationException("postgres:18 не поднялся за 30 c");
    }

    public async ValueTask DisposeAsync() => await _container.DisposeAsync();
}
```

- [ ] **Step 4: SqlProbeIntegrationTests.cs (5 тестов)**

```csharp
using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using AdminPanel.Core.Alerting.Rules;
using AdminPanel.Probes;
using FluentAssertions;
using Microsoft.Extensions.Options;
using Npgsql;
using Xunit;

namespace AdminPanel.IntegrationTests;

// SQL-проба против живого postgres:18 (spec §9.6): каталог, слоты/лаги, ошибки,
// HA-правила на живом runtime. Хост "pg" в DSN закрывается HostMap на контейнер —
// ровно сценарий стенда (arch/04 §2.3).
public class SqlProbeIntegrationTests(PostgresFixture fixture) : IClassFixture<PostgresFixture>
{
    private static ShardInfo Shard() => new(
        "s1", "host=pg port=5432 dbname=postgres user=postgres",
        ["pg"], 5432, "postgres", "postgres", 1, null, null);

    private SqlProbe Probe(string? password = null)
    {
        var options = new ProbesOptions
        {
            HostMap = new Dictionary<string, string> { ["pg:5432"] = $"127.0.0.1:{fixture.Port}" },
            TimeoutSeconds = 5,
        };
        if (password is not null)
            options.Password = password;
        return new SqlProbe(Options.Create(options), TimeProvider.System);
    }

    private static ClusterInfo DemoCluster(ShardInfo shard) => new(
        "demo", "demo", 16, null, [shard],
        [.. Enumerable.Range(0, 16).Select(i => new BucketInfo(i, "s1", BucketState.Active, null))],
        []);

    // Идемпотентный сид: контейнер один на класс (IClassFixture), Arrange зовётся
    // несколькими тестами — повторное создание слота даст 42710, поэтому guard.
    private async Task SeedSchemasAndSlotAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync(ct);
        await using var schemas = new NpgsqlCommand(
            string.Join(";", Enumerable.Range(0, 16).Select(i => $"create schema if not exists bucket_{i}")),
            connection);
        await schemas.ExecuteNonQueryAsync(ct);
        await using var slotExists = new NpgsqlCommand(
            "select 1 from pg_replication_slots where slot_name = 't06_slot'", connection);
        if (await slotExists.ExecuteScalarAsync(ct) is null)
        {
            await using var slot = new NpgsqlCommand(
                "select pg_create_logical_replication_slot('t06_slot', 'pgoutput')",
                connection);
            await slot.ExecuteScalarAsync(ct);
        }
    }

    private async Task GenerateWalAsync()
    {
        await using var connection = new NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await using var table = new NpgsqlCommand(
            "create table if not exists wal_gen(payload text)", connection);
        await table.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        await using var insert = new NpgsqlCommand(
            "insert into wal_gen select repeat('x', 1000) from generate_series(1, 100)", connection);
        await insert.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task SqlProbe_ReadsCatalogFromLivePostgres()
    {
        // Arrange
        await SeedSchemasAndSlotAsync();

        // Act
        var result = await Probe().ProbeAsync(DemoCluster(Shard()), Shard(), CancellationToken.None);

        // Assert: IsInRecovery false, инвентарь 16, слот виден, реплик нет (spec §9.6).
        result.Result.Ok.Should().BeTrue();
        result.Runtime.Error.Should().BeNull();
        result.Runtime.IsInRecovery.Should().BeFalse();
        result.Runtime.BucketSchemas.Should().HaveCount(16);
        var slot = result.Runtime.Slots.Single(s => s.SlotName == "t06_slot");
        slot.SlotType.Should().Be("logical");
        slot.Active.Should().BeFalse();
        slot.WalStatus.Should().NotBeNullOrEmpty();
        result.Runtime.Standbies.Should().BeEmpty();
        result.Runtime.Subscriptions.Should().BeEmpty();
    }

    [Fact]
    public async Task SqlProbe_GeneratesWal_SlotLagGrows()
    {
        // Arrange: слот есть, WAL генерируется — подтверждённого flush нет.
        await SeedSchemasAndSlotAsync();
        var before = await Probe().ProbeAsync(DemoCluster(Shard()), Shard(), CancellationToken.None);
        await GenerateWalAsync();

        // Act
        var after = await Probe().ProbeAsync(DemoCluster(Shard()), Shard(), CancellationToken.None);

        // Assert: лаг слота появился/вырос (проводка pg_wal_lsn_diff живьём).
        var lagBefore = before.Runtime.Slots.Single(s => s.SlotName == "t06_slot").LagBytes ?? 0;
        var lagAfter = after.Runtime.Slots.Single(s => s.SlotName == "t06_slot").LagBytes;
        lagAfter.Should().BeGreaterThan(lagBefore);
    }

    [Fact]
    public async Task SqlProbe_UnreachableHost_ErrorRuntime()
    {
        // Arrange: HostMap ведёт на закрытый порт — ошибка подключения целиком на шард
        // (категория отказа spec §9.6: проверяется форма Error-runtime, не тип исключения;
        // неверный пароль на trust-стенде не отвергается сервером, поэтому недостижим).
        var options = new ProbesOptions
        {
            HostMap = new Dictionary<string, string> { ["pg:5432"] = "127.0.0.1:1" },
        };
        var probe = new SqlProbe(Options.Create(options), TimeProvider.System);

        // Act
        var result = await probe.ProbeAsync(DemoCluster(Shard()), Shard(), CancellationToken.None);

        // Assert: отказ целиком на шард — Error, списки пустые, IsInRecovery null (spec §3.7).
        result.Result.Ok.Should().BeFalse();
        result.Runtime.Error.Should().NotBeNullOrEmpty();
        result.Runtime.BucketSchemas.Should().BeEmpty();
        result.Runtime.IsInRecovery.Should().BeNull();
    }

    [Fact]
    public async Task AlertRules_OnLiveRuntime()
    {
        // Arrange: снапшот с живым runtime (без реплик, 16/16 схем) + движок t06.
        await SeedSchemasAndSlotAsync();
        var probeResult = await Probe().ProbeAsync(DemoCluster(Shard()), Shard(), CancellationToken.None);
        var runtime = probeResult.Runtime;
        var shard = Shard() with { Runtime = runtime };
        var snapshot = new EtcdSnapshot(
            DateTimeOffset.UtcNow,
            TestSnapshotEtcd(),
            [DemoCluster(shard)],
            [], [], [], [], [], 0);

        // Act
        var alerts = new AlertEngine(
        [
            new SlotWalLostRule(),
            new SlotLagHighRule(Options.Create(new AlertsOptions { ReplicaLagBytes = long.MaxValue })),
            new SyncStandbyMissingRule(),
            new InventoryMismatchRule(),
        ]).Evaluate(snapshot, null, DateTimeOffset.UtcNow, 3).ToList();

        // Assert: sync-standby-missing есть; инвентарь 16/16 — mismatch нет;
        // порог лага maxed-out — лаг-алерта нет (изоляция условий).
        alerts.Should().ContainSingle(a => a.Id == "sync-standby-missing:demo/s1");
        alerts.Should().NotContain(a => a.Kind == "inventory-mismatch");

        // Act-2: схема удалена — появляется inventory-mismatch (missing bucket_15).
        await using (var connection = new NpgsqlConnection(fixture.ConnectionString))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            await using var drop = new NpgsqlCommand("drop schema bucket_15 cascade", connection);
            await drop.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        var afterDrop = await Probe().ProbeAsync(DemoCluster(Shard()), Shard(), CancellationToken.None);
        var snapshot2 = snapshot with
        {
            Clusters = [DemoCluster(shard with { Runtime = afterDrop.Runtime })],
        };
        var alerts2 = new InventoryMismatchRule()
            .Evaluate(snapshot2, new AlertContext(null, DateTimeOffset.UtcNow, 3)).ToList();

        // Assert-2
        alerts2.Should().ContainSingle()
            .Which.Details!["missing"].Should().Be("bucket_15");
    }

    [Fact]
    public async Task AlertRules_SlotLagReproduced_LowThreshold()
    {
        // Arrange: живой лаг + заниженный порог — каталогный алерт воспроизводится
        // без генерации 16 МБ WAL (spec §16).
        await SeedSchemasAndSlotAsync();
        await GenerateWalAsync();
        var probeResult = await Probe().ProbeAsync(DemoCluster(Shard()), Shard(), CancellationToken.None);
        var shard = Shard() with { Runtime = probeResult.Runtime };
        var snapshot = new EtcdSnapshot(
            DateTimeOffset.UtcNow,
            TestSnapshotEtcd(),
            [DemoCluster(shard)],
            [], [], [], [], [], 0);

        // Act
        var alerts = new SlotLagHighRule(Options.Create(new AlertsOptions { ReplicaLagBytes = 1 }))
            .Evaluate(snapshot, new AlertContext(null, DateTimeOffset.UtcNow, 3)).ToList();

        // Assert
        alerts.Should().ContainSingle().Which.Id.Should().Be("slot-lag-high:demo/s1/t06_slot");
    }

    // Минимальный живой EtcdStatus для снапшотов правил (reachable, без alarm'ов).
    private static EtcdStatus TestSnapshotEtcd()
        => new(true, [], [], [], null, false, DateTimeOffset.UtcNow, 0);
}
```

Замечание к spec §9.6: кейс ошибки SQL-пробы реализован как `SqlProbe_UnreachableHost_ErrorRuntime` (закрытый порт вместо неверного пароля) — postgres-стенд с `POSTGRES_HOST_AUTH_METHOD=trust` неверный пароль не отвергает, кейс был бы ложно-зелёным; ассерты (Error, пустые списки, `IsInRecovery=null`) идентичны spec. Уточнение реализации, не отклонение.

- [ ] **Step 5: Прогнать все новые integration-тесты**

Run: `dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~AdminPanel.IntegrationTests.SqlProbeIntegrationTests|FullyQualifiedName~AdminPanel.IntegrationTests.PatroniRestProbeTests"`
Expected: PASS — 5 + 5 passed (нужен Docker: postgres:18).

- [ ] **Step 6: Commit**

```bash
git add src/tests/AdminPanel.IntegrationTests/PatroniRestProbeTests.cs src/tests/AdminPanel.IntegrationTests/PostgresFixture.cs src/tests/AdminPanel.IntegrationTests/SqlProbeIntegrationTests.cs
git commit -m "t06: integration — Patroni-проба против HTTP-стаба (HostMap e2e) + SQL-проба против postgres:18 со слотами (spec §9.5–9.6)"
```

**Выход:** обе пробы подтверждены против реальных протоколов (HTTP PostgreSQL-каталог).

---

### Task 12: Финальный прогон, критерии приёмки, roadmap-коммит

**Files:**
- Modify: ничего нового; проверка дерева и финальный коммит `docs/superpowers/2026-08-22-t06-ha-api/` + `arch/roadmap/ha.md` (правка уже в дереве с Фазы 1).

**Interfaces:** Consumes всё (Tasks 1–11).

**Вход:** все задачи слиты; рабочее дерево содержит только непро-коммиченные `docs/superpowers/2026-08-22-t06-ha-api/` (spec + plan) и `arch/roadmap/ha.md`.

- [ ] **Step 1: Полная сборка**

Run: `dotnet build src/AdminPanel.slnx`
Expected: успех, 0 warnings.

- [ ] **Step 2: Полный прогон тестов**

Run: `dotnet test src/AdminPanel.slnx`
Expected: все зелёные (unit без Docker; integration — etcd/postgres-контейнеры).

- [ ] **Step 3: Критерии приёмки spec §15 (выборочная верификация кода)**

3a. Движок не тронут:

Run: `git diff 029d4bf..HEAD -- src/AdminPanel.Core/Alerting/AlertEngine.cs src/AdminPanel.Core/Alerting/IAlertEngine.cs src/AdminPanel.Etcd/SnapshotBuilder.cs src/AdminPanel.Api/Program.cs`
Expected: пусто.

3b. Пакетная дисциплина:

Run: `grep -rn "PackageReference" src/AdminPanel.Probes/AdminPanel.Probes.csproj && grep -n "Npgsql" src/Directory.Packages.props`
Expected: Hosting.Abstractions, Http, Npgsql; версия 10.0.3.

3c. Roadmap-анкеры (spec §14):

Run: `grep -c "t06-ha-api" arch/roadmap/ha.md`
Expected: `0`.

Run: `grep -n "t06-ha-api" arch/roadmap/stand.md arch/roadmap/frontend.md`
Expected: 2 строки зависимостей `← t06-ha-api` на месте (чистит задача-владелец).

Run: `git status --short arch/`
Expected: только ` M arch/roadmap/ha.md`.

3d. Отключаемость и read-only подтверждены тестами Tasks 6–8 (grep-смоук):

Run: `grep -rn "default_transaction_read_only" src/AdminPanel.Probes/SqlProbe.cs`
Expected: строка Options присутствует.

- [ ] **Step 4: Финальный коммит (spec + plan + roadmap-деливерабл)**

```bash
git add docs/superpowers/2026-08-22-t06-ha-api arch/roadmap/ha.md
git commit -m "t06: spec/plan задачи + roadmap-деливерабл (удаление пункта t06-ha-api)"
```

Run: `git status --short`
Expected: пусто.

Run: `git log --oneline -13`
Expected: 12 коммитов задач + финальный (13 строк от merge-base t05).

**Выход:** t06-ha-api реализован полностью по spec; roadmap-пункт удалён тем же набором коммитов ветки; мерж в main — по гейту ревью dev-flow (вне этого плана).

---

## Self-Review (выполнен при составлении; правки ревью Фазы 4 C1–C4, I1, M1–M3 внесены)

1. **Spec coverage:** §3.1–3.2 → Task 1–2; §3.3–3.16 (пробы) → Tasks 3–6; §4 → Tasks 3–6; §5 (9 правил + пороги) → Tasks 7–8; §6 (API) → Task 9; §9.2 → Task 9; §9.3–9.4, §9.7 → Task 10; §9.5–9.6 → Task 11; §10 → задачи 1, 2, 3, 4, 5, 6, 7, 8, 9; §7 (дерево) покрыто задачами 1–11; §12 → Task 3; §13 → Task 4; §14 → Task 12. Дыр нет.
2. **Placeholder scan:** TBD/TODO/«реализовать позже» нет; каждый шаг кода содержит полный листинг; единственные отклонения от буквы spec — два уточнения реализации, оба обоснованы в тексте плана (Task 11: `Probe_UnmappedHost_FailsWithOriginalHost` и `SqlProbe_UnreachableHost_ErrorRuntime` — недостижимые/ложно-зелёные формы кейсов).
3. **Type consistency:** имена/сигнатуры сверены с фактическим кодом t03–t05: `EtcdSnapshot`/`HaScope`/`HaMember`/`ShardRuntime`/`Alert`/`IAlertRule` (позиционные records), ctor `SnapshotRefresher` (позиция 4 = `IProbeStateStore`), `RefresherTestHarness`/`EtcdTestHarness` (перегрузки-обёртки сохраняют существующие вызовы), `AlertsOptions`, `InspectionModule`-паттерн 404/503, `AuthWebFactory`/`ApiTestLogin`/`TestSnapshotStore`, `EtcdSeed.Demo` (2 скопа, routing 8/8 round-robin — инвентарь в живых фикстурах сверен с ним), `Kv(key, value, modRevision)`. Хелпер порогов назван `LagOptions` (не конфликтует с `Microsoft.Extensions.Options.Options`); счётчики тестов в Проверках посчитаны по листингам (5 + 2 + 4 + 3 + 4 + 6 + 11 + 9 + 5 unit + 6 integration + 1 + 2 + 5 + 5). Правки ревью: get-only-коллекции наполняются `.Add` (C1), гварды правил отсекают `Runtime == null` до обращения к полям + регрессионный кейс (C2), идемпотентный guard создания слота (C3), `AlertContext` вместо `null` для `IAlertRule.Evaluate` (C4), сквозной сценарий ждёт 6 алертов с `slot-invalidation-risk` в каноническом порядке (I1), счётчики Task 8 = 9/20 (M1), кейс «ошибка пробы» исполняется на matched-скопе (M2), несуществующий `using System.StringComparison` убран (M3).
