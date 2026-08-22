# t03-etcd-snapshot: etcd-клиент, парсеры контроль-плейна, снапшот и refresher — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Read-only модуль `AdminPanel.Etcd`: клиент etcd через HTTP JSON gateway `/v3/*`, парсеры `/clusters/`, `/service/`, `/cluster/nodes/` в immutable-модель `EtcdSnapshot` (типы — в Core), `SnapshotStore` (атомарная замена) и `SnapshotRefresher` (BackgroundService, тик 3 c, sticky-failover, отказоустойчивость) + health-check и тесты (unit + Testcontainers-integration).

**Architecture:** Транспорт (`AdminPanel.Etcd.Client`) отделён от домена (`AdminPanel.Core` — модель снапшота целиком по контракту 02 §3); парсеры — чистые статические функции над декодированными `Kv`; refresher — единственный писатель `SnapshotStore` (volatile-замена ссылки). Все тесты конструируют модуль напрямую через `new` (статический кеш attribute-DI не трогается — регистрации Program-хоста t04+ не пострадают).

**Tech Stack:** .NET 10, HttpClient из IHttpClientFactory (CPM: `Microsoft.Extensions.Http`, `Microsoft.Extensions.Hosting.Abstractions`), System.Text.Json, xunit v3 + FluentAssertions, Testcontainers 4.14.0 (`quay.io/coreos/etcd:v3.5.21`).

**Spec:** `docs/superpowers/2026-08-22-t03-etcd-snapshot/spec.md` — план реализует её; исполнители читают обе. Номера § ниже — из spec.

## Global Constraints

- WORKTREE (все пути файлов ниже относительны к нему): `/Users/demakaev/ZCodeProject/worktrees/feat-t03-etcd-snapshot`; ветка `feat-t03-etcd-snapshot`. Команды даются с `cd` — рабочая директория между вызовами не сохраняется.
- .NET 10, `LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true` — код без warning'ов (spec §2).
- Идентификаторы английские, комментарии в коде русские (spec §2).
- Тесты xunit v3 + FluentAssertions; в каждом тестовом методе AAA-комментарии на русском: `// Arrange`, `// Act`, `// Assert` (spec §2).
- Новые NuGet-пакеты только через CPM и только §13: `Microsoft.Extensions.Http`, `Microsoft.Extensions.Hosting.Abstractions` (runtime), `Testcontainers` (тесты). Никакого Polly/gRPC (spec §3.1, §12).
- Никаких вызовов attribute-DI (`UseDiBehaviours`/`AutoRegistration`) в тестах t03: всё через `new` + `Options.Create` — статический кеш `ServiceCollectionExtensions._assemblies` (прецедент t02 §14: второй хост в процессе не получает регистраций) должен остаться чистым для t04+ (spec §3.15).
- Прецедент CS0718: маркер-тип логгера не должен быть static-классом. В `ModuleExtensions` (static) используем `ILogger<EtcdGateway>` (EtcdGateway — не static).
- Прецедент явных generic-аргументов: `Result<T>`-фабрики вызываем с явным `Result<IReadOnlyList<Kv>>.Success(...)`; `Task.FromResult(...)` с явным типом результата; async-методы gateway возвращают `Task<Result<T>>` (не ValueTask — интерфейс по spec §4.1).
- `HealthzTests`/`AuthTests` (коллекция `"api"`) обязаны остаться зелёными: `/api/healthz` — liveness «живость самой панели» (arch/03 §1), поэтому чек `etcd` регистрируется без тега `live`, а healthz-маппинг фильтрует чеки по тегу `live` — поведение зафиксировано в spec §7.3/§8.3/§16.5 (согласующая правка по ревью Фазы 4).
- Сверх spec не добавлять: ни эндпоинтов, ни опций, ни тестов сверх §10/§11.
- `arch/01–04` не мутировать; `arch/roadmap/etcd.md` уже правлен в Фазе 1 (пункт `t03-etcd-snapshot` удалён, зависимость `t04 ← t03` сохранена) — закоммитить финальным коммитом Task 12 (spec §15).
- Коммиты — в ветке `feat-t03-etcd-snapshot`, формат `t03: <слаг> — <русское описание> (unit|integration)` — по прецеденту t01/t02.
- После каждого Task — прогон `dotnet build src/AdminPanel.slnx` (+ целевые `dotnet test --filter …` шага); Docker требуется с Task 11 (Testcontainers) — перед ним проверить `docker info`.

---

## Порядок задач

| Task | Deliverable | Зависит от |
|---|---|---|
| 1 | CPM-пакеты + модель снапшота Core (records по 02 §3) + ссылки UnitTests | — |
| 2 | `ScopeMatcher` (Core) + unit | 1 |
| 3 | `DsnParser` + unit | 1 |
| 4 | `JsonValues` + `ClustersParser` + фикстуры + unit | 1, 3 |
| 5 | `ServiceParser` + фикстуры + unit | 1, 2, 4 |
| 6 | `StandNodesParser` + фикстура + unit | 1 |
| 7 | `IEtcdGateway`/`EtcdGateway` + gateway-фикстуры + unit (fake HttpMessageHandler) | 1 |
| 8 | `SnapshotStore` + `SnapshotBuilder` + unit | 4, 5, 6 |
| 9 | `EtcdOptions` + `ModuleExtensions.AddEtcd` (AddHttpClient) + `SnapshotRefresher` + unit (FakeEtcdGateway) | 7, 8 |
| 10 | `EtcdHealthCheck` + `Program.cs` (AddCheck + Predicate live) + appsettings + unit + smoke-хост | 9 |
| 11 | Integration: Testcontainers etcd + сид + gateway/refresher/health тесты | 7, 9, 10 |
| 12 | Полный прогон + критерии spec §16 + финальный коммит (docs + roadmap) | 1–11 |

---

### Task 1: CPM-пакеты + модель снапшота Core

**Связь со spec:** §5 (модель целиком, включая §3.4–3.5 — ParseErrors, Alert/ProbeResult/ShardRuntime как типы), §9 (дерево), §13 (пакеты), §14 (csproj-правки).

**Files:**
- Modify: `src/Directory.Packages.props`
- Modify: `src/AdminPanel.Etcd/AdminPanel.Etcd.csproj`
- Modify: `src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj`
- Create: `src/AdminPanel.Core/EtcdSnapshot.cs`, `EtcdStatus.cs`, `ClusterInfo.cs`, `HaScope.cs`, `StandNode.cs`, `Alert.cs`, `ProbeResult.cs`, `ShardRuntime.cs`
- Test: `src/tests/AdminPanel.UnitTests/CoreModelTests.cs`

**Interfaces (Produces — используют все последующие задачи):**
- `namespace AdminPanel.Core`: `EtcdSnapshot`, `KeyParseError`, `EtcdStatus`, `EtcdEndpoint`, `EtcdMember`, `EtcdAlarm`, `EtcdAlarmType`, `ClusterInfo`(+computed `Incomplete`), `ShardInfo`(+computed `MasterLeaseAlive`), `BucketInfo`, `BucketState`, `MoveInfo`, `HealRecord`, `HaScope`, `HaMember`, `StandNode`, `Alert`, `AlertSeverity`, `ProbeResult`, `ShardRuntime`, `ReplicationSlotInfo`, `StandbyInfo`, `SubscriptionInfo` — точные сигнатуры в листингах ниже.

- [ ] **Step 1.1: Проверить доступные версии Extensions-пакетов и добавить их в CPM**

Действие. Актуальная минорная версия 10.0.x выясняется локально (CPM не примет несуществующую):

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-etcd-snapshot
dotnet package search Microsoft.Extensions.Http --take 1 --format json
dotnet package search Microsoft.Extensions.Hosting.Abstractions --take 1 --format json
dotnet package search Testcontainers --take 1 --format json
```

Из вывода каждой команды взять значение поля `latestVersion` у единственного результата. В `src/Directory.Packages.props` внутрь `<ItemGroup>` добавить (числа — из вывода команд; строка с несуществующей версией не пройдёт `dotnet restore` на Step 1.5 — это встроенная проверка шага):

```xml
    <PackageVersion Include="Microsoft.Extensions.Hosting.Abstractions" Version="10.0.9" />
    <PackageVersion Include="Microsoft.Extensions.Http" Version="10.0.9" />
    <PackageVersion Include="Testcontainers" Version="4.14.0" />
```

(числа заменить на фактические из вывода — это часть шага, а не placeholder: строка без валидной версии не пройдёт Step 1.7 `dotnet restore`.)

- [ ] **Step 1.2: Ссылки проектов**

`src/AdminPanel.Etcd/AdminPanel.Etcd.csproj` — полностью:

```xml
<Project Sdk="Microsoft.NET.Sdk">

    <ItemGroup>
        <PackageReference Include="Microsoft.Extensions.Hosting.Abstractions"/>
        <PackageReference Include="Microsoft.Extensions.Http"/>
        <ProjectReference Include="..\AdminPanel.Core\AdminPanel.Core.csproj"/>
    </ItemGroup>

</Project>
```

В `src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj` после существующих `ProjectReference` добавить и добавить копирование фикстур (папка появится в Task 4 — Include по маске на пустой папке безопасен):

```xml
    <ProjectReference Include="..\..\AdminPanel.Core\AdminPanel.Core.csproj"/>
    <ProjectReference Include="..\..\AdminPanel.Etcd\AdminPanel.Etcd.csproj"/>
```

и отдельная ItemGroup:

```xml
  <ItemGroup>
    <None Include="EtcdFixtures\**\*.json" CopyToOutputDirectory="PreserveNewest"/>
  </ItemGroup>
```

- [ ] **Step 1.3: Файлы модели Core**

`src/AdminPanel.Core/EtcdSnapshot.cs`:

```csharp
namespace AdminPanel.Core;

// Слепок всего, что панель знает об инспектируемой системе (контракт arch/02 §3).
// Immutable: refresher строит новый и атомарно заменяет в SnapshotStore.
public sealed record EtcdSnapshot(
    DateTimeOffset BuiltAtUtc,
    EtcdStatus Etcd,
    IReadOnlyList<ClusterInfo> Clusters,
    IReadOnlyList<HaScope> HaScopes,
    IReadOnlyList<StandNode> StandNodes,
    IReadOnlyList<ProbeResult> Probes,             // t03: всегда пусто (пробы — t06)
    IReadOnlyList<Alert> Alerts,                   // t03: всегда пусто (AlertEngine — t04)
    IReadOnlyList<KeyParseError> ParseErrors,      // расширение spec §3.4 (arch/02 §7)
    int UnknownKeyCount);

// Ключ, значение которого не удалось разобрать: виден в UI-details, кормит алерт key-malformed (t04).
public sealed record KeyParseError(string Key, string Reason);
```

`src/AdminPanel.Core/EtcdStatus.cs`:

```csharp
namespace AdminPanel.Core;

// Состояние кластера etcd: endpoints, members, alarms + свежесть и счётчик отказов (arch/02 §3, §2.4).
public sealed record EtcdStatus(
    bool Reachable,
    IReadOnlyList<EtcdEndpoint> Endpoints,
    IReadOnlyList<EtcdMember> Members,
    IReadOnlyList<EtcdAlarm> Alarms,
    string? ActiveEndpoint,
    bool QuorumSuspected,
    DateTimeOffset LastRefreshUtc,
    int ConsecutiveFailures);

// Один endpoint из настроек: результат персонального /v3/maintenance/status (или ошибки транспорта).
public sealed record EtcdEndpoint(
    string Url,
    bool Reachable,
    double? LatencyMs,
    string? Version,
    long? DbSizeBytes,
    ulong? LeaderMemberId,
    ulong? RaftIndex,
    ulong? RaftTerm,
    IReadOnlyList<string> Errors);

// Член etcd-кластера из /v3/cluster/member/list (isLeader в DTO вычисляет API t04 по EtcdStatus).
public sealed record EtcdMember(
    ulong Id,
    string? Name,
    IReadOnlyList<string> PeerUrls,
    IReadOnlyList<string> ClientUrls);

// Активная тревога из /v3/maintenance/alarm.
public sealed record EtcdAlarm(ulong MemberId, EtcdAlarmType Type);

// Значения enum-поля alarm в gateway: 0/1/2.
public enum EtcdAlarmType
{
    None = 0,
    NoSpace = 1,
    Corrupt = 2,
}
```

`src/AdminPanel.Core/ClusterInfo.cs`:

```csharp
namespace AdminPanel.Core;

// Кластер <C> из /clusters/<C>/…: константы, шарды, бакеты, журнал heals (arch/02 §2.1).
public sealed record ClusterInfo(
    string Name,
    string? DbName,
    int BucketsCount,
    long? CreatedUnix,
    IReadOnlyList<ShardInfo> Shards,
    IReadOnlyList<BucketInfo> Buckets,
    IReadOnlyList<HealRecord> Heals)
{
    // Пометка «incomplete» (arch/02 §7): префикс есть, config отсутствует/пуст.
    public bool Incomplete => DbName is null || BucketsCount <= 0;
}

// Шард кластера: dsn, декларативные реплики, master-ключ с lease-семантикой (arch/02 §2.1).
public sealed record ShardInfo(
    string Name,
    string Dsn,
    IReadOnlyList<string> DsnHosts,
    int? Port,
    string? DbName,
    string? User,
    int? ReplicasDeclared,
    string? MasterAddress,
    ShardRuntime? Runtime)
{
    // Lease-семантика master-ключа (arch/02 §1): ключ есть = lease жив.
    public bool MasterLeaseAlive => MasterAddress is not null;
}

// Бакет: id, владелец (routing), состояние переезда (arch/02 §2.1).
public sealed record BucketInfo(
    int Id,
    string? Owner,
    BucketState State,
    MoveInfo? Move);

// Статус бакета: отсутствие status-ключа = ACTIVE (arch/02 §2.1).
public enum BucketState
{
    Active,
    Syncing,
    Frozen,
    Aborting,
}

// Поля статус-ключа переезда (значение /clusters/<C>/buckets/status/bucket_<N>).
public sealed record MoveInfo(
    string? Owner,
    string? Target,
    long? StartedUnix,
    long? UpdatedUnix,
    string? Phase,
    string? LastError);

// Запись журнала авто-починки (значение /clusters/<C>/heals/<bucket>).
public sealed record HealRecord(
    string Bucket,
    string? Was,
    string? Now,
    string? Reason,
    long? TsUnix);
```

`src/AdminPanel.Core/HaScope.cs`:

```csharp
namespace AdminPanel.Core;

// Patroni DCS-scope /service/<scope>/ (arch/02 §2.2): leader, members, optime, raw config.
public sealed record HaScope(
    string Scope,
    string? Cluster,
    string? Shard,
    bool Matched,
    string? LeaderName,
    string? OptimeLeader,
    bool Initialized,
    IReadOnlyList<HaMember> Members,
    string? RawConfig);

// Член HA-кластера: что есть в etcd + поля Patroni-пробы (t06 — null).
public sealed record HaMember(
    string Name,
    string Host,
    int? Port,
    string? Role,
    string? State,
    long? Timeline,
    long? LagBytes,
    DateTimeOffset? ProbeAtUtc,
    string? ProbeError);
```

`src/AdminPanel.Core/StandNode.cs`:

```csharp
namespace AdminPanel.Core;

// Стендовая топология /cluster/nodes/<node> → IP (arch/02 §2.3; в проде префикса нет).
public sealed record StandNode(string Name, string? Address);
```

`src/AdminPanel.Core/Alert.cs`:

```csharp
namespace AdminPanel.Core;

// Алерт из AlertEngine (t04): стабильный id "kind:target" (arch/01 §3).
public enum AlertSeverity
{
    Info,
    Warning,
    Critical,
}

public sealed record Alert(
    string Id,
    AlertSeverity Severity,
    string Kind,
    string Target,
    string Message,
    IReadOnlyDictionary<string, string>? Details,
    long? SinceUnix);
```

`src/AdminPanel.Core/ProbeResult.cs`:

```csharp
namespace AdminPanel.Core;

// Результат live-пробы (t06): ok/error/latency по цели (arch/02 §6, минимальный контракт spec §3.5).
public sealed record ProbeResult(
    string Target,
    string Kind,
    bool Ok,
    double? LatencyMs,
    string? Error,
    DateTimeOffset AtUtc);
```

`src/AdminPanel.Core/ShardRuntime.cs`:

```csharp
namespace AdminPanel.Core;

// Runtime-обогащение шарда из SQL-пробы (t06): слоты, standby, подписки, инвентарь бакетов.
public sealed record ShardRuntime(
    string Shard,
    IReadOnlyList<ReplicationSlotInfo> Slots,
    IReadOnlyList<StandbyInfo> Standbies,
    IReadOnlyList<SubscriptionInfo> Subscriptions,
    IReadOnlyList<string> BucketSchemas,
    bool? IsInRecovery,
    string? Error);

// Слот репликации (pg_replication_slots, P4).
public sealed record ReplicationSlotInfo(
    string SlotName,
    string SlotType,
    bool Active,
    string? WalStatus,
    long? SafeWalSizeBytes,
    long? LagBytes);

// Физическая реплика (pg_stat_replication, sync_state! — P8).
public sealed record StandbyInfo(
    string ApplicationName,
    string? ClientAddr,
    string State,
    string SyncState,
    long? LagBytes);

// Подписка логической репликации (pg_stat_subscription — прогресс переездов).
public sealed record SubscriptionInfo(
    string Name,
    string? ReceivedLsn,
    string? LatestEndLsn,
    DateTimeOffset? LatestEndTime);
```

- [ ] **Step 1.4: Тест computed-свойств**

`src/tests/AdminPanel.UnitTests/CoreModelTests.cs`:

```csharp
using AdminPanel.Core;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Вычислимые пометки модели снапшота (spec §3.6).
public class CoreModelTests
{
    [Fact]
    public void ClusterInfo_WithoutConfig_IsIncomplete()
    {
        // Arrange
        var cluster = new ClusterInfo("demo", null, 0, null, [], [], []);

        // Act
        var incomplete = cluster.Incomplete;

        // Assert
        incomplete.Should().BeTrue();
    }

    [Fact]
    public void ClusterInfo_WithConfig_IsComplete()
    {
        // Arrange
        var cluster = new ClusterInfo("demo", "demo", 16, 1755800000, [], [], []);

        // Act
        var incomplete = cluster.Incomplete;

        // Assert
        incomplete.Should().BeFalse();
    }

    [Fact]
    public void ShardInfo_MasterAddressNull_LeaseNotAlive()
    {
        // Arrange
        var shard = new ShardInfo("s1", "dsn", ["s1a"], 5432, "demo", "u", 1, null, null);

        // Act
        var alive = shard.MasterLeaseAlive;

        // Assert
        alive.Should().BeFalse();
    }

    [Fact]
    public void ShardInfo_MasterAddressSet_LeaseAlive()
    {
        // Arrange
        var shard = new ShardInfo("s1", "dsn", ["s1a"], 5432, "demo", "u", 1, "s1a:5432", null);

        // Act
        var alive = shard.MasterLeaseAlive;

        // Assert
        alive.Should().BeTrue();
    }
}
```

- [ ] **Step 1.5: Проверка — сборка и тесты**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-etcd-snapshot
dotnet build src/AdminPanel.slnx
dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~AdminPanel.UnitTests"
```

Ожидание: build — успех, 0 warnings; тесты — все PASS (4 новых `CoreModelTests` + существующие).

- [ ] **Step 1.6: Коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-etcd-snapshot
git add src/Directory.Packages.props src/AdminPanel.Etcd/AdminPanel.Etcd.csproj src/AdminPanel.Core/ src/tests/AdminPanel.UnitTests/
git commit -m "t03: модель снапшота Core по контракту 02 §3 + пакеты CPM (unit)"
```

---

### Task 2: ScopeMatcher (Core)

**Связь со spec:** §5 (ScopeMatcher + правило мэтчинга 02 §2.2: префикс `<C>-` + suffix=имя шарда; чужой scope — норма).

**Files:**
- Create: `src/AdminPanel.Core/ScopeMatcher.cs`
- Test: `src/tests/AdminPanel.UnitTests/ScopeMatcherTests.cs`

**Interfaces:**
- Consumes: `ClusterInfo`, `ShardInfo` (Task 1).
- Produces: `static (string? Cluster, string? Shard, bool Matched) ScopeMatcher.Match(string scope, IReadOnlyList<ClusterInfo> clusters)` — используют Task 5 (ServiceParser) и тесты.

- [ ] **Step 2.1: Тест (сначала — красный)**

`src/tests/AdminPanel.UnitTests/ScopeMatcherTests.cs`:

```csharp
using AdminPanel.Core;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Связь scope "<C>-<X>" → (cluster, shard) по известным кластерам (spec §5, arch/02 §2.2).
public class ScopeMatcherTests
{
    private static readonly IReadOnlyList<ClusterInfo> Clusters =
    [
        new("demo", "demo", 16, null,
            [new ShardInfo("s1", "", [], null, null, null, null, null, null),
             new ShardInfo("s2", "", [], null, null, null, null, null, null)],
            [], []),
        new("shop", "shop", 4, null,
            [new ShardInfo("shard1", "", [], null, null, null, null, null, null)],
            [], []),
    ];

    [Fact]
    public void Match_KnownClusterAndShard_ReturnsMatched()
    {
        // Arrange — scope demo-s1
        // Act
        var (cluster, shard, matched) = ScopeMatcher.Match("demo-s1", Clusters);

        // Assert
        matched.Should().BeTrue();
        cluster.Should().Be("demo");
        shard.Should().Be("s1");
    }

    [Fact]
    public void Match_SuffixNotShard_ClusterWithoutShard()
    {
        // Arrange — s9 не является шардом demo
        // Act
        var (cluster, shard, matched) = ScopeMatcher.Match("demo-s9", Clusters);

        // Assert
        matched.Should().BeFalse();
        cluster.Should().Be("demo");
        shard.Should().BeNull();
    }

    [Fact]
    public void Match_UnknownPrefix_AllNull()
    {
        // Arrange — чужой service в общем etcd — норма (arch/02 §7)
        // Act
        var (cluster, shard, matched) = ScopeMatcher.Match("other-scope", Clusters);

        // Assert
        matched.Should().BeFalse();
        cluster.Should().BeNull();
        shard.Should().BeNull();
    }

    [Fact]
    public void Match_LongerClusterName_NotConfused()
    {
        // Arrange — "shop2" не путается с "shop" из-за дефиса в префиксе
        // Act
        var (cluster, _, matched) = ScopeMatcher.Match("shop2-x", Clusters);

        // Assert
        matched.Should().BeFalse();
        cluster.Should().BeNull();
    }

    [Fact]
    public void Match_NoClusters_AllNull()
    {
        // Arrange — /clusters/ пуст
        // Act
        var (cluster, shard, matched) = ScopeMatcher.Match("demo-s1", []);

        // Assert
        matched.Should().BeFalse();
        cluster.Should().BeNull();
        shard.Should().BeNull();
    }
}
```

- [ ] **Step 2.2: Проверка красного**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-etcd-snapshot
dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~ScopeMatcherTests"
```

Ожидание: FAIL — `CS0103: The name 'ScopeMatcher' does not exist` (ошибка компиляции).

- [ ] **Step 2.3: Реализация**

`src/AdminPanel.Core/ScopeMatcher.cs`:

```csharp
namespace AdminPanel.Core;

// Связь scope "<C>-<X>" с кластером/шардом по известным кластерам /clusters/ (arch/02 §2.2):
// префикс "<C>-", suffix обязан быть именем шарда; иначе scope показывается «как есть» с пометкой unmatched.
public static class ScopeMatcher
{
    public static (string? Cluster, string? Shard, bool Matched) Match(
        string scope,
        IReadOnlyList<ClusterInfo> clusters)
    {
        foreach (var cluster in clusters)
        {
            var prefix = cluster.Name + "-";
            if (!scope.StartsWith(prefix, StringComparison.Ordinal))
                continue;

            var suffix = scope[prefix.Length..];
            return cluster.Shards.Any(sh => sh.Name == suffix)
                ? (cluster.Name, suffix, true)
                : (cluster.Name, null, false);
        }

        return (null, null, false);
    }
}
```

- [ ] **Step 2.4: Проверка зелёного + коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-etcd-snapshot
dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~ScopeMatcherTests"
git add src/AdminPanel.Core/ScopeMatcher.cs src/tests/AdminPanel.UnitTests/ScopeMatcherTests.cs
git commit -m "t03: ScopeMatcher — связь scope <C>-<X> с кластером/шардом (unit)"
```

Ожидание: 5 PASS; коммит создан.

---

### Task 3: DsnParser

**Связь со spec:** §6.4 (libpq keyword-строка, multi-host, толерантность), §2.1 (`/shards/<X>/dsn`).

**Files:**
- Create: `src/AdminPanel.Etcd/Parsing/DsnParser.cs`
- Test: `src/tests/AdminPanel.UnitTests/DsnParserTests.cs`

**Interfaces:**
- Consumes: —
- Produces: `namespace AdminPanel.Etcd.Parsing`: `sealed record DsnInfo(IReadOnlyList<string> Hosts, int? Port, string? DbName, string? User)` и `static DsnInfo DsnParser.Parse(string dsn)` — использует Task 4 (ClustersParser).

- [ ] **Step 3.1: Тест (сначала — красный)**

`src/tests/AdminPanel.UnitTests/DsnParserTests.cs`:

```csharp
using AdminPanel.Etcd.Parsing;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Парсер libpq keyword-строки DSN из /clusters/<C>/shards/<X>/dsn (spec §6.4).
public class DsnParserTests
{
    [Fact]
    public void Parse_MultiHost_SplitsByComma()
    {
        // Arrange
        var dsn = "host=s1a,s1b port=5432 dbname=demo user=postgres";

        // Act
        var info = DsnParser.Parse(dsn);

        // Assert
        info.Hosts.Should().Equal("s1a", "s1b");
        info.Port.Should().Be(5432);
        info.DbName.Should().Be("demo");
        info.User.Should().Be("postgres");
    }

    [Fact]
    public void Parse_MissingKeywords_Nulls()
    {
        // Arrange
        var dsn = "host=n1";

        // Act
        var info = DsnParser.Parse(dsn);

        // Assert
        info.Hosts.Should().Equal("n1");
        info.Port.Should().BeNull();
        info.DbName.Should().BeNull();
        info.User.Should().BeNull();
    }

    [Fact]
    public void Parse_ExtraKeywords_Ignored()
    {
        // Arrange
        var dsn = "host=n1 port=5432 dbname=d user=u sslmode=require application_name=x";

        // Act
        var info = DsnParser.Parse(dsn);

        // Assert
        info.Hosts.Should().Equal("n1");
        info.DbName.Should().Be("d");
        info.User.Should().Be("u");
    }

    [Fact]
    public void Parse_Empty_NoHosts()
    {
        // Arrange
        // Act
        var info = DsnParser.Parse("");

        // Assert
        info.Hosts.Should().BeEmpty();
        info.Port.Should().BeNull();
    }
}
```

- [ ] **Step 3.2: Проверка красного**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-etcd-snapshot
dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~DsnParserTests"
```

Ожидание: FAIL — компиляция (`DsnParser`/`DsnInfo` не существуют).

- [ ] **Step 3.3: Реализация**

`src/AdminPanel.Etcd/Parsing/DsnParser.cs`:

```csharp
namespace AdminPanel.Etcd.Parsing;

// Разобранный libpq keyword-DSN: хосты (multi-host), порт, dbname, user (spec §6.4).
public sealed record DsnInfo(
    IReadOnlyList<string> Hosts,
    int? Port,
    string? DbName,
    string? User);

// Парсер libpq keyword-строки: токены key=value по пробелам; нераспознанное игнорируется
// (DSN пишут init-скрипты ../pg; quoting-синтаксис libpq в системе не используется).
public static class DsnParser
{
    public static DsnInfo Parse(string dsn)
    {
        var hosts = new List<string>();
        int? port = null;
        string? dbName = null;
        string? user = null;

        foreach (var token in dsn.Split(' ', StringSplitOptions.RemoveEmptyEntries))
        {
            var eq = token.IndexOf('=');
            if (eq <= 0)
                continue;

            var key = token[..eq];
            var value = token[(eq + 1)..];
            switch (key)
            {
                case "host":
                    hosts.AddRange(value.Split(',', StringSplitOptions.RemoveEmptyEntries));
                    break;
                case "port" when int.TryParse(value, out var parsed):
                    port = parsed;
                    break;
                case "dbname":
                    dbName = value;
                    break;
                case "user":
                    user = value;
                    break;
            }
        }

        return new DsnInfo(hosts, port, dbName, user);
    }
}
```

- [ ] **Step 3.4: Проверка зелёного + коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-etcd-snapshot
dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~DsnParserTests"
git add src/AdminPanel.Etcd/Parsing/DsnParser.cs src/tests/AdminPanel.UnitTests/DsnParserTests.cs
git commit -m "t03: DsnParser — libpq keyword-строка шарда (unit)"
```

Ожидание: 4 PASS; коммит.

---

### Task 4: JsonValues + ClustersParser + фикстуры

**Связь со spec:** §6.1 (таблица ключей, толерантность, вырожденные случаи), §3.7 (бакеты incomplete), §3.6 (computed-пометки уже в Task 1), §10 преамбула + §10.1 (тесты), §8 (контракт тестирования парсеров — реальные фрагменты).

**Files:**
- Create: `src/AdminPanel.Etcd/Parsing/JsonValues.cs`
- Create: `src/AdminPanel.Etcd/Parsing/ClustersParser.cs`
- Create: `src/tests/AdminPanel.UnitTests/EtcdFixtures/clusters-full.json`
- Create: `src/tests/AdminPanel.UnitTests/EtcdFixtures/clusters-degenerate.json`
- Create: `src/tests/AdminPanel.UnitTests/EtcdFixtures.cs`
- Test: `src/tests/AdminPanel.UnitTests/ClustersParserTests.cs`

**Interfaces:**
- Consumes: `Kv` (Task 7 — ещё не создан!). Внимание: `Kv` создаётся в Task 7; чтобы Task 4 не зависел от Task 7, создайте `Client/Kv.cs` в этом шаге как отдельный файл (см. Step 4.3) — Task 7 добавит к нему интерфейсную пару.
- Produces: `namespace AdminPanel.Etcd.Parsing`: `sealed record ClustersParseResult(IReadOnlyList<ClusterInfo> Clusters, IReadOnlyList<KeyParseError> Errors, int UnknownKeyCount)`, `static ClustersParseResult ClustersParser.Parse(IReadOnlyList<Kv> kvs)`, `internal static class JsonValues { static string? ReadString(JsonElement root, string name); static long? ReadLong(JsonElement root, string name); }` — использует Task 5. `namespace AdminPanel.Etcd.Client`: `sealed record Kv(string Key, string Value, ulong ModRevision)`.

- [ ] **Step 4.1: Фикстуры (реальные форматы значений из ../pg / arch/04 §2.2)**

`src/tests/AdminPanel.UnitTests/EtcdFixtures/clusters-full.json` — полный demo-сид префикса `/clusters/`:

```json
[
  { "key": "/clusters/demo/config", "value": "{\"buckets\":16,\"dbname\":\"demo\",\"created_unix\":1755800000}", "modRevision": 42 },
  { "key": "/clusters/demo/shards/s1/dsn", "value": "host=s1a,s1b port=5432 dbname=demo user=postgres", "modRevision": 43 },
  { "key": "/clusters/demo/shards/s1/replicas", "value": "1", "modRevision": 44 },
  { "key": "/clusters/demo/shards/s1/master", "value": "s1a:5432", "modRevision": 120 },
  { "key": "/clusters/demo/shards/s2/dsn", "value": "host=s2a,s2b port=5432 dbname=demo user=postgres", "modRevision": 45 },
  { "key": "/clusters/demo/shards/s2/replicas", "value": "1", "modRevision": 46 },
  { "key": "/clusters/demo/shards/s2/master", "value": "s2a:5432", "modRevision": 121 },
  { "key": "/clusters/demo/buckets/routing/bucket_0", "value": "s1", "modRevision": 50 },
  { "key": "/clusters/demo/buckets/routing/bucket_1", "value": "s2", "modRevision": 51 },
  { "key": "/clusters/demo/buckets/routing/bucket_2", "value": "s1", "modRevision": 52 },
  { "key": "/clusters/demo/buckets/routing/bucket_3", "value": "s1", "modRevision": 53 },
  { "key": "/clusters/demo/buckets/routing/bucket_4", "value": "s1", "modRevision": 54 },
  { "key": "/clusters/demo/buckets/routing/bucket_5", "value": "s2", "modRevision": 55 },
  { "key": "/clusters/demo/buckets/routing/bucket_6", "value": "s1", "modRevision": 56 },
  { "key": "/clusters/demo/buckets/routing/bucket_7", "value": "s2", "modRevision": 57 },
  { "key": "/clusters/demo/buckets/routing/bucket_8", "value": "s1", "modRevision": 58 },
  { "key": "/clusters/demo/buckets/routing/bucket_9", "value": "s2", "modRevision": 59 },
  { "key": "/clusters/demo/buckets/routing/bucket_10", "value": "s1", "modRevision": 60 },
  { "key": "/clusters/demo/buckets/routing/bucket_11", "value": "s1", "modRevision": 61 },
  { "key": "/clusters/demo/buckets/routing/bucket_12", "value": "s1", "modRevision": 62 },
  { "key": "/clusters/demo/buckets/routing/bucket_13", "value": "s2", "modRevision": 63 },
  { "key": "/clusters/demo/buckets/routing/bucket_14", "value": "s1", "modRevision": 64 },
  { "key": "/clusters/demo/buckets/routing/bucket_15", "value": "s2", "modRevision": 65 },
  { "key": "/clusters/demo/buckets/status/bucket_3", "value": "{\"bucket\":\"bucket_3\",\"state\":\"SYNCING\",\"owner\":\"s1\",\"target\":\"s2\",\"started_unix\":1755900000,\"updated_unix\":1755900600,\"phase\":\"copy\"}", "modRevision": 70 },
  { "key": "/clusters/demo/buckets/status/bucket_7", "value": "{\"bucket\":\"bucket_7\",\"state\":\"ABORTING\",\"owner\":\"s2\",\"target\":\"s1\",\"started_unix\":1755800000,\"updated_unix\":1755800500,\"phase\":\"cleanup\",\"last_error\":\"receiver went away\"}", "modRevision": 71 },
  { "key": "/clusters/demo/buckets/status/bucket_11", "value": "{\"bucket\":\"bucket_11\",\"state\":\"FROZEN\",\"owner\":\"s1\",\"target\":\"s2\",\"started_unix\":1755700000,\"updated_unix\":1755700200,\"phase\":\"cutover-wait\"}", "modRevision": 72 },
  { "key": "/clusters/demo/heals/bucket_5", "value": "{\"bucket\":\"bucket_5\",\"was\":\"s2\",\"now\":\"s1\",\"reason\":\"restore-heal\",\"ts\":1755600000}", "modRevision": 80 }
]
```

`src/tests/AdminPanel.UnitTests/EtcdFixtures/clusters-degenerate.json` — вырожденные случаи 02 §7–8:

```json
[
  { "key": "/clusters/broken/config", "value": "{\"buckets\":\"8\",\"dbname\":\"broken\"}", "modRevision": 1 },
  { "key": "/clusters/broken/shards/x1/dsn", "value": "host=n1 port=5432 dbname=broken user=bucket_admin", "modRevision": 2 },
  { "key": "/clusters/broken/shards/x1/replicas", "value": "two", "modRevision": 3 },
  { "key": "/clusters/broken/heals/bucket_2", "value": "{\"was\":\"s1\",\"now\":\"s2\",\"reason\":\"manual\",\"ts\":1755650000}", "modRevision": 12 },
  { "key": "/clusters/noconfig/shards/y1/dsn", "value": "host=m1 port=5432 dbname=noconfig user=postgres", "modRevision": 4 },
  { "key": "/clusters/demo2/config", "value": "{\"buckets\":4,\"dbname\":\"demo2\",\"created_unix\":1755800099}", "modRevision": 5 },
  { "key": "/clusters/demo2/shards/s1/master", "value": "", "modRevision": 6 },
  { "key": "/clusters/demo2/buckets/routing/bucket_0", "value": "s1", "modRevision": 7 },
  { "key": "/clusters/demo2/buckets/routing/bucket_99", "value": "s9", "modRevision": 8 },
  { "key": "/clusters/demo2/buckets/routing/bucket_abc", "value": "s1", "modRevision": 9 },
  { "key": "/clusters/demo2/buckets/status/bucket_1", "value": "{\"state\":\"WEIRD\"", "modRevision": 10 },
  { "key": "/clusters/demo2/surprise", "value": "?", "modRevision": 11 }
]
```

- [ ] **Step 4.2: Загрузчик фикстур**

`src/tests/AdminPanel.UnitTests/EtcdFixtures.cs`:

```csharp
using System.Text.Json;
using AdminPanel.Etcd.Client;

namespace AdminPanel.UnitTests;

// Загрузчик JSON-фикстур парсеров: массив {"key","value","modRevision"} → декодированные Kv.
// Фикстуры копируются в выходной каталог (None-ItemGroup csproj, spec §14).
public static class EtcdFixtures
{
    // Формат файла фикстуры: [{"key":"/…","value":"…","modRevision":n}, …].
    private sealed record FixtureKv(string Key, string Value, ulong ModRevision);

    private static readonly JsonSerializerOptions Json = new() { PropertyNameCaseInsensitive = true };

    public static IReadOnlyList<Kv> LoadKv(string fileName)
    {
        var path = Path.Combine(AppContext.BaseDirectory, "EtcdFixtures", fileName);
        var items = JsonSerializer.Deserialize<List<FixtureKv>>(File.ReadAllText(path), Json) ?? [];
        return items.Select(i => new Kv(i.Key, i.Value, i.ModRevision)).ToList();
    }

    public static string LoadText(string fileName)
        => File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "EtcdFixtures", fileName));
}
```

- [ ] **Step 4.3: Kv-запись (клиентская, без транспорта)**

`src/AdminPanel.Etcd/Client/Kv.cs`:

```csharp
namespace AdminPanel.Etcd.Client;

// Декодированная пара KV: gateway снял base64, парсеры работают с plain-строками (spec §4.1).
public sealed record Kv(string Key, string Value, ulong ModRevision);
```

- [ ] **Step 4.4: Тест парсера (сначала — красный)**

`src/tests/AdminPanel.UnitTests/ClustersParserTests.cs`:

```csharp
using AdminPanel.Core;
using AdminPanel.Etcd.Parsing;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Парсер /clusters/: полный demo-сид и вырожденные случаи (spec §10.1, arch/02 §7–8).
public class ClustersParserTests
{
    [Fact]
    public void Parse_FullDemoSeed_BuildsClustersShardsBuckets()
    {
        // Arrange
        var kvs = EtcdFixtures.LoadKv("clusters-full.json");

        // Act
        var result = ClustersParser.Parse(kvs);

        // Assert
        var demo = result.Clusters.Should().ContainSingle(c => c.Name == "demo").Subject;
        demo.DbName.Should().Be("demo");
        demo.BucketsCount.Should().Be(16);
        demo.CreatedUnix.Should().Be(1755800000);
        demo.Incomplete.Should().BeFalse();
        var s1 = demo.Shards.Should().ContainSingle(s => s.Name == "s1").Subject;
        s1.Dsn.Should().Be("host=s1a,s1b port=5432 dbname=demo user=postgres");
        s1.DsnHosts.Should().Equal("s1a", "s1b");
        s1.Port.Should().Be(5432);
        s1.DbName.Should().Be("demo");
        s1.User.Should().Be("postgres");
        s1.ReplicasDeclared.Should().Be(1);
        s1.MasterAddress.Should().Be("s1a:5432");
        s1.MasterLeaseAlive.Should().BeTrue();
        demo.Buckets.Should().HaveCount(16);
        demo.Buckets.Single(b => b.Id == 0).Owner.Should().Be("s1");
        demo.Buckets.Single(b => b.Id == 1).Owner.Should().Be("s2");
        result.Errors.Should().BeEmpty();
        result.UnknownKeyCount.Should().Be(0);
    }

    [Fact]
    public void Parse_StatusKeys_MapToMoveInfo()
    {
        // Arrange
        var kvs = EtcdFixtures.LoadKv("clusters-full.json");

        // Act
        var result = ClustersParser.Parse(kvs);

        // Assert
        var demo = result.Clusters.Single();
        var syncing = demo.Buckets.Single(b => b.Id == 3);
        syncing.State.Should().Be(BucketState.Syncing);
        syncing.Move.Should().NotBeNull();
        syncing.Move!.Owner.Should().Be("s1");
        syncing.Move.Target.Should().Be("s2");
        syncing.Move.StartedUnix.Should().Be(1755900000);
        syncing.Move.UpdatedUnix.Should().Be(1755900600);
        syncing.Move.Phase.Should().Be("copy");
        demo.Buckets.Single(b => b.Id == 7).State.Should().Be(BucketState.Aborting);
        demo.Buckets.Single(b => b.Id == 7).Move!.LastError.Should().Be("receiver went away");
        demo.Buckets.Single(b => b.Id == 11).State.Should().Be(BucketState.Frozen);
        // отсутствие status-ключа = ACTIVE (arch/02 §2.1)
        var active = demo.Buckets.Single(b => b.Id == 0);
        active.State.Should().Be(BucketState.Active);
        active.Move.Should().BeNull();
    }

    [Fact]
    public void Parse_HealJournal_Collected()
    {
        // Arrange
        var kvs = EtcdFixtures.LoadKv("clusters-full.json");

        // Act
        var result = ClustersParser.Parse(kvs);

        // Assert
        var heal = result.Clusters.Single().Heals.Should().ContainSingle(h => h.Bucket == "bucket_5").Subject;
        heal.Was.Should().Be("s2");
        heal.Now.Should().Be("s1");
        heal.Reason.Should().Be("restore-heal");
        heal.TsUnix.Should().Be(1755600000);
    }

    [Fact]
    public void Parse_MissingConfig_ClusterIncomplete()
    {
        // Arrange
        var kvs = EtcdFixtures.LoadKv("clusters-degenerate.json");

        // Act
        var result = ClustersParser.Parse(kvs);

        // Assert
        var noconfig = result.Clusters.Should().ContainSingle(c => c.Name == "noconfig").Subject;
        noconfig.Incomplete.Should().BeTrue();
        noconfig.DbName.Should().BeNull();
        noconfig.BucketsCount.Should().Be(0);
        noconfig.Shards.Should().ContainSingle(s => s.Name == "y1");
        // бакеты incomplete-кластера — из фактических ключей (spec §3.7): routing/status-ключей нет → пусто
        noconfig.Buckets.Should().BeEmpty();
    }

    [Fact]
    public void Parse_ConfigWithoutCreatedUnix_NullCreatedUnix()
    {
        // Arrange
        var kvs = EtcdFixtures.LoadKv("clusters-degenerate.json");

        // Act
        var result = ClustersParser.Parse(kvs);

        // Assert
        var broken = result.Clusters.Should().ContainSingle(c => c.Name == "broken").Subject;
        broken.CreatedUnix.Should().BeNull();
        // строковые числа толерантны (arch/02 §8)
        broken.BucketsCount.Should().Be(8);
        broken.DbName.Should().Be("broken");
    }

    [Fact]
    public void Parse_BrokenValues_ParseErrorsRecorded()
    {
        // Arrange
        var kvs = EtcdFixtures.LoadKv("clusters-degenerate.json");

        // Act
        var result = ClustersParser.Parse(kvs);

        // Assert
        // битый JSON статус-ключа: ключ пропущен, ошибка зафиксирована
        result.Errors.Should().Contain(e => e.Key == "/clusters/demo2/buckets/status/bucket_1");
        // replicas не число: ReplicasDeclared=null + ошибка
        result.Errors.Should().Contain(e => e.Key == "/clusters/broken/shards/x1/replicas");
        var x1 = result.Clusters.Single(c => c.Name == "broken").Shards.Single(s => s.Name == "x1");
        x1.ReplicasDeclared.Should().BeNull();
        // пустой master: MasterAddress=null + ошибка (spec §6.1)
        result.Errors.Should().Contain(e => e.Key == "/clusters/demo2/shards/s1/master");
        var m = result.Clusters.Single(c => c.Name == "demo2").Shards.Single(s => s.Name == "s1");
        m.MasterAddress.Should().BeNull();
        // bucket_abc — нечисловой id
        result.Errors.Should().Contain(e => e.Key == "/clusters/demo2/buckets/routing/bucket_abc");
        // heal без поля "bucket": имя — суффикс ключа (spec §6.1)
        var healed = result.Clusters.Single(c => c.Name == "broken").Heals
            .Should().ContainSingle().Subject;
        healed.Bucket.Should().Be("bucket_2");
        healed.Reason.Should().Be("manual");
    }

    [Fact]
    public void Parse_OutOfRangeRouting_StillParsed()
    {
        // Arrange
        var kvs = EtcdFixtures.LoadKv("clusters-degenerate.json");

        // Act
        var result = ClustersParser.Parse(kvs);

        // Assert
        // bucket_99 при N=4 остаётся в списке бакетов — детект out-of-range это алерт t04 (P18)
        var demo2 = result.Clusters.Single(c => c.Name == "demo2");
        demo2.Buckets.Single(b => b.Id == 99).Owner.Should().Be("s9");
        demo2.Buckets.Single(b => b.Id == 0).Owner.Should().Be("s1");
    }

    [Fact]
    public void Parse_UnknownKey_Counted()
    {
        // Arrange
        var kvs = EtcdFixtures.LoadKv("clusters-degenerate.json");

        // Act
        var result = ClustersParser.Parse(kvs);

        // Assert
        result.UnknownKeyCount.Should().Be(1); // /clusters/demo2/surprise
    }

    [Fact]
    public void Parse_EmptyPrefix_EmptyResult()
    {
        // Arrange — /clusters/ не существует (пустой ответ range)
        // Act
        var result = ClustersParser.Parse([]);

        // Assert
        result.Clusters.Should().BeEmpty();
        result.Errors.Should().BeEmpty();
        result.UnknownKeyCount.Should().Be(0);
    }
}
```

- [ ] **Step 4.5: Проверка красного**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-etcd-snapshot
dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~ClustersParserTests"
```

Ожидание: FAIL — компиляция (`ClustersParser`/`ClustersParseResult` не существуют).

- [ ] **Step 4.6: Реализация JsonValues + ClustersParser**

`src/AdminPanel.Etcd/Parsing/JsonValues.cs`:

```csharp
using System.Text.Json;

namespace AdminPanel.Etcd.Parsing;

// Толерантное чтение полей JSON-значений ключей: строки-числа, отсутствующие поля (arch/02 §8).
internal static class JsonValues
{
    public static string? ReadString(JsonElement root, string name)
        => root.ValueKind == JsonValueKind.Object
            && root.TryGetProperty(name, out var element)
            && element.ValueKind is JsonValueKind.String or JsonValueKind.Number
            ? element.ToString()
            : null;

    public static long? ReadLong(JsonElement root, string name)
    {
        if (root.ValueKind != JsonValueKind.Object || !root.TryGetProperty(name, out var element))
            return null;

        return element.ValueKind switch
        {
            JsonValueKind.Number => element.TryGetInt64(out var value) ? value : null,
            JsonValueKind.String when long.TryParse(element.GetString(), out var value) => value,
            _ => null,
        };
    }
}
```

`src/AdminPanel.Etcd/Parsing/ClustersParser.cs`:

```csharp
using System.Globalization;
using System.Text.Json;
using AdminPanel.Core;
using AdminPanel.Etcd.Client;

namespace AdminPanel.Etcd.Parsing;

// Результат разбора префикса /clusters/ (spec §6.1).
public sealed record ClustersParseResult(
    IReadOnlyList<ClusterInfo> Clusters,
    IReadOnlyList<KeyParseError> Errors,
    int UnknownKeyCount);

// Парсер контроль-плейна шардинга /clusters/<C>/…: чистая функция Kv[] → модель,
// битые значения не бросают исключений — порождают KeyParseError (arch/02 §7).
public static class ClustersParser
{
    private sealed class ShardAcc
    {
        public string? Dsn;
        public string? ReplicasRaw;
        public string? Master;
    }

    private sealed class ClusterAcc(string name)
    {
        public readonly string Name = name;
        public string? ConfigRaw;
        public readonly Dictionary<string, ShardAcc> Shards = [];
        public readonly Dictionary<int, string> Routing = [];
        public readonly Dictionary<int, string> StatusRaw = [];
        public readonly List<HealRecord> Heals = [];
    }

    public static ClustersParseResult Parse(IReadOnlyList<Kv> kvs)
    {
        var errors = new List<KeyParseError>();
        var unknown = 0;
        var accs = new Dictionary<string, ClusterAcc>();

        foreach (var kv in kvs)
        {
            // "/clusters/<C>/leaf…" → ["", "clusters", <C>, …]
            var segments = kv.Key.Split('/');
            if (segments.Length < 4 || segments[1] != "clusters" || segments[2].Length == 0)
            {
                unknown++;
                continue;
            }

            var acc = GetOrAdd(accs, segments[2], static name => new ClusterAcc(name));
            switch (segments[3])
            {
                case "config" when segments.Length == 4:
                    acc.ConfigRaw = kv.Value;
                    break;

                case "shards" when segments.Length == 6
                    && segments[4].Length > 0
                    && segments[5] is "dsn" or "replicas" or "master":
                {
                    var shard = GetOrAdd(acc.Shards, segments[4], static _ => new ShardAcc());
                    switch (segments[5])
                    {
                        case "dsn":
                            shard.Dsn = kv.Value;
                            break;
                        case "replicas":
                            shard.ReplicasRaw = kv.Value;
                            break;
                        default:
                            shard.Master = kv.Value;
                            break;
                    }

                    break;
                }

                case "buckets" when segments.Length == 6 && segments[4] == "routing"
                    && segments[5].StartsWith("bucket_", StringComparison.Ordinal):
                {
                    if (TryBucketId(segments[5], out var id))
                        acc.Routing[id] = kv.Value;
                    else
                        errors.Add(new KeyParseError(kv.Key, "нечисловой id бакета в имени ключа"));
                    break;
                }

                case "buckets" when segments.Length == 6 && segments[4] == "status"
                    && segments[5].StartsWith("bucket_", StringComparison.Ordinal):
                {
                    if (TryBucketId(segments[5], out var id))
                        acc.StatusRaw[id] = kv.Value;
                    else
                        errors.Add(new KeyParseError(kv.Key, "нечисловой id бакета в имени ключа"));
                    break;
                }

                case "heals" when segments.Length == 5 && segments[4].Length > 0:
                {
                    var heal = ParseHeal(kv.Key, kv.Value);
                    if (heal is null)
                        errors.Add(new KeyParseError(kv.Key, "битый JSON heal-записи"));
                    else
                        acc.Heals.Add(heal);
                    break;
                }

                default:
                    // система развивается — неизвестный ключ не ошибка, только счётчик (arch/02 §2.1)
                    unknown++;
                    break;
            }
        }

        var clusters = accs.Values
            .OrderBy(c => c.Name, StringComparer.Ordinal)
            .Select(acc => BuildCluster(acc, errors))
            .ToList();

        return new ClustersParseResult(clusters, errors, unknown);
    }

    private static ClusterInfo BuildCluster(ClusterAcc acc, List<KeyParseError> errors)
    {
        var (dbName, bucketsCount, createdUnix) = ParseConfig(acc.Name, acc.ConfigRaw, errors);

        var shards = acc.Shards
            .OrderBy(pair => pair.Key, StringComparer.Ordinal)
            .Select(pair => BuildShard(acc.Name, pair.Key, pair.Value, errors))
            .ToList();

        var buckets = BuildBuckets(bucketsCount, acc, errors);

        return new ClusterInfo(acc.Name, dbName, bucketsCount, createdUnix, shards, buckets, acc.Heals);
    }

    private static (string? DbName, int BucketsCount, long? CreatedUnix) ParseConfig(
        string cluster, string? raw, List<KeyParseError> errors)
    {
        if (raw is null)
            return (null, 0, null); // ключа нет — incomplete, не ошибка (arch/02 §7)

        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            var buckets = JsonValues.ReadLong(root, "buckets");
            return (
                JsonValues.ReadString(root, "dbname"),
                buckets is null ? 0 : (int)buckets.Value,
                JsonValues.ReadLong(root, "created_unix")); // может отсутствовать у старых init (arch/02 §2.1)
        }
        catch (JsonException)
        {
            errors.Add(new KeyParseError($"/clusters/{cluster}/config", "битый JSON config"));
            return (null, 0, null);
        }
    }

    private static ShardInfo BuildShard(string cluster, string name, ShardAcc shard, List<KeyParseError> errors)
    {
        var prefix = $"/clusters/{cluster}/shards/{name}/";
        int? replicas = null;
        if (shard.ReplicasRaw is not null)
        {
            if (int.TryParse(shard.ReplicasRaw.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
                replicas = parsed;
            else
                errors.Add(new KeyParseError(prefix + "replicas", "значение не целое число"));
        }

        if (shard.Master == string.Empty)
            errors.Add(new KeyParseError(prefix + "master", "пустое значение"));

        var dsn = DsnParser.Parse(shard.Dsn ?? "");
        return new ShardInfo(
            name,
            shard.Dsn ?? "",
            dsn.Hosts,
            dsn.Port,
            dsn.DbName,
            dsn.User,
            replicas,
            string.IsNullOrWhiteSpace(shard.Master) ? null : shard.Master.Trim(),
            null); // Runtime — SQL-проба t06
    }

    private static IReadOnlyList<BucketInfo> BuildBuckets(int bucketsCount, ClusterAcc acc, List<KeyParseError> errors)
    {
        // ids: полный диапазон 0..N-1 из config (все N, включая ACTIVE — arch/02 §2.1)
        // ∪ фактические ключи (out-of-range routing вида bucket_99 остаются видимыми для
        // алерта t04 bucket-out-of-range; incomplete-кластер — только фактические, spec §3.7).
        var ids = bucketsCount > 0
            ? Enumerable.Range(0, bucketsCount).Union(acc.Routing.Keys).Union(acc.StatusRaw.Keys)
            : acc.Routing.Keys.Union(acc.StatusRaw.Keys);
        ids = ids.OrderBy(id => id);

        var result = new List<BucketInfo>();
        foreach (var id in ids)
        {
            acc.Routing.TryGetValue(id, out var owner);
            MoveInfo? move = null;
            var state = BucketState.Active;
            if (acc.StatusRaw.TryGetValue(id, out var raw)
                && !TryParseStatus(raw, out state, out move))
            {
                errors.Add(new KeyParseError(
                    $"/clusters/{acc.Name}/buckets/status/bucket_{id}",
                    "битый JSON или неизвестное state"));
            }

            result.Add(new BucketInfo(id, owner, state, move));
        }

        return result;
    }

    private static bool TryParseStatus(string raw, out BucketState state, out MoveInfo? move)
    {
        state = BucketState.Active;
        move = null;
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            state = JsonValues.ReadString(root, "state") switch
            {
                "SYNCING" => BucketState.Syncing,
                "FROZEN" => BucketState.Frozen,
                "ABORTING" => BucketState.Aborting,
                _ => BucketState.Active,
            };
            if (state == BucketState.Active)
                return false; // state отсутствует или неизвестен — считаем ключ битым

            move = new MoveInfo(
                JsonValues.ReadString(root, "owner"),
                JsonValues.ReadString(root, "target"),
                JsonValues.ReadLong(root, "started_unix"),
                JsonValues.ReadLong(root, "updated_unix"),
                JsonValues.ReadString(root, "phase"),
                JsonValues.ReadString(root, "last_error"));
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    // Heal-запись: имя бакета — из поля "bucket", при его отсутствии — суффикс ключа (spec §6.1).
    private static HealRecord? ParseHeal(string key, string raw)
    {
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            return new HealRecord(
                JsonValues.ReadString(root, "bucket") ?? key[(key.LastIndexOf('/') + 1)..],
                JsonValues.ReadString(root, "was"),
                JsonValues.ReadString(root, "now"),
                JsonValues.ReadString(root, "reason"),
                JsonValues.ReadLong(root, "ts"));
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static bool TryBucketId(string leaf, out int id)
        => int.TryParse(leaf["bucket_".Length..], NumberStyles.Integer, CultureInfo.InvariantCulture, out id);

    private static TValue GetOrAdd<TKey, TValue>(Dictionary<TKey, TValue> dictionary, TKey key, Func<TKey, TValue> factory)
        where TKey : notnull
    {
        if (!dictionary.TryGetValue(key, out var value))
        {
            value = factory(key);
            dictionary[key] = value;
        }

        return value;
    }
}
```

Примечание к листингу: метод `TryParseStatus` выше намеренно оставляет `state = BucketState.Active` и `move = null` при неудаче — ветка ошибки в `BuildBuckets` уже не читает их значения, а `out`-параметры всегда назначены до `return`.

- [ ] **Step 4.7: Проверка зелёного + коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-etcd-snapshot
dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~ClustersParserTests"
dotnet build src/AdminPanel.slnx
git add src/AdminPanel.Etcd/Parsing/JsonValues.cs src/AdminPanel.Etcd/Parsing/ClustersParser.cs src/AdminPanel.Etcd/Client/Kv.cs src/tests/AdminPanel.UnitTests/EtcdFixtures/ src/tests/AdminPanel.UnitTests/EtcdFixtures.cs src/tests/AdminPanel.UnitTests/ClustersParserTests.cs
git commit -m "t03: ClustersParser + фикстуры реальных значений /clusters/ (unit)"
```

Ожидание: 9 PASS, build 0 warnings, коммит.

---

### Task 5: ServiceParser + фикстуры

**Связь со spec:** §6.2 (таблица /service/, plain-строка leader, optime, initialize, unmatched), §10.2.

**Files:**
- Create: `src/AdminPanel.Etcd/Parsing/ServiceParser.cs`
- Create: `src/tests/AdminPanel.UnitTests/EtcdFixtures/service-full.json`, `service-unmatched.json`
- Test: `src/tests/AdminPanel.UnitTests/ServiceParserTests.cs`

**Interfaces:**
- Consumes: `Kv`, `JsonValues`, `ScopeMatcher.Match`, `ClusterInfo` (Tasks 1–4).
- Produces: `sealed record ServiceParseResult(IReadOnlyList<HaScope> Scopes, IReadOnlyList<KeyParseError> Errors, int UnknownKeyCount)` и `static ServiceParseResult ServiceParser.Parse(IReadOnlyList<Kv> kvs, IReadOnlyList<ClusterInfo> clusters)` — использует Task 8 (SnapshotBuilder).

- [ ] **Step 5.1: Фикстуры**

`src/tests/AdminPanel.UnitTests/EtcdFixtures/service-full.json`:

```json
[
  { "key": "/service/demo-s1/leader", "value": "{\"name\":\"s1a\"}", "modRevision": 10 },
  { "key": "/service/demo-s1/members/s1a", "value": "{\"name\":\"s1a\",\"conn_url\":\"s1a:5432\",\"role\":\"master\",\"state\":\"running\",\"timeline\":1,\"lag\":0}", "modRevision": 11 },
  { "key": "/service/demo-s1/members/s1b", "value": "{\"name\":\"s1b\",\"conn_url\":\"s1b:5432\",\"role\":\"replica\",\"state\":\"streaming\",\"timeline\":1,\"lag\":0}", "modRevision": 12 },
  { "key": "/service/demo-s1/optime/leader", "value": "738273634528", "modRevision": 13 },
  { "key": "/service/demo-s1/initialize", "value": "738273612345678", "modRevision": 14 },
  { "key": "/service/demo-s1/config", "value": "{\"ttl\":5,\"loop_wait\":2,\"retry_timeout\":3}", "modRevision": 15 },
  { "key": "/service/demo-s2/leader", "value": "{\"name\":\"s2a\"}", "modRevision": 16 },
  { "key": "/service/demo-s2/members/s2a", "value": "{\"name\":\"s2a\",\"conn_url\":\"s2a:5432\",\"role\":\"master\",\"state\":\"running\",\"timeline\":1,\"lag\":0}", "modRevision": 17 },
  { "key": "/service/demo-s2/members/s2b", "value": "{\"name\":\"s2b\",\"conn_url\":\"s2b:5432\",\"role\":\"replica\",\"state\":\"streaming\",\"timeline\":1,\"lag\":0}", "modRevision": 18 },
  { "key": "/service/demo-s2/optime/leader", "value": "738273634001", "modRevision": 19 },
  { "key": "/service/demo-s2/initialize", "value": "738273611234567", "modRevision": 20 },
  { "key": "/service/demo-s2/config", "value": "{\"ttl\":5,\"loop_wait\":2,\"retry_timeout\":3}", "modRevision": 21 }
]
```

`src/tests/AdminPanel.UnitTests/EtcdFixtures/service-unmatched.json`:

```json
[
  { "key": "/service/other-scope/leader", "value": "plain-name", "modRevision": 30 },
  { "key": "/service/other-scope/members/only", "value": "{\"name\":\"only\",\"conn_url\":\"only:5432\",\"role\":\"master\"}", "modRevision": 31 },
  { "key": "/service/demo-s9/leader", "value": "{\"name\":\"m1\"}", "modRevision": 32 },
  { "key": "/service/stray/what/is/this", "value": "x", "modRevision": 33 }
]
```

- [ ] **Step 5.2: Тест (сначала — красный)**

`src/tests/AdminPanel.UnitTests/ServiceParserTests.cs`:

```csharp
using AdminPanel.Etcd.Parsing;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Парсер /service/<scope>/ (Patroni DCS): leader-варианты, members, optime, initialize, unmatched (spec §10.2).
public class ServiceParserTests
{
    // Кластеры для мэтчинга — из реальной фикстуры /clusters/ (связка тика одного снапшота).
    private static readonly IReadOnlyList<AdminPanel.Core.ClusterInfo> DemoClusters =
        ClustersParser.Parse(EtcdFixtures.LoadKv("clusters-full.json")).Clusters;

    [Fact]
    public void Parse_DemoScopes_MatchedToClusters()
    {
        // Arrange
        var kvs = EtcdFixtures.LoadKv("service-full.json");

        // Act
        var result = ServiceParser.Parse(kvs, DemoClusters);

        // Assert
        var s1 = result.Scopes.Should().ContainSingle(s => s.Scope == "demo-s1").Subject;
        s1.Cluster.Should().Be("demo");
        s1.Shard.Should().Be("s1");
        s1.Matched.Should().BeTrue();
        var s2 = result.Scopes.Should().ContainSingle(s => s.Scope == "demo-s2").Subject;
        s2.Matched.Should().BeTrue();
        result.Errors.Should().BeEmpty();
        result.UnknownKeyCount.Should().Be(0);
    }

    [Fact]
    public void Parse_LeaderJson_NameExtracted()
    {
        // Arrange
        var kvs = EtcdFixtures.LoadKv("service-full.json");

        // Act
        var result = ServiceParser.Parse(kvs, DemoClusters);

        // Assert
        result.Scopes.Single(s => s.Scope == "demo-s1").LeaderName.Should().Be("s1a");
    }

    [Fact]
    public void Parse_LeaderPlainString_Tolerated()
    {
        // Arrange — на стенде возможна строка-имя без JSON-обёртки (arch/02 §2.2)
        var kvs = EtcdFixtures.LoadKv("service-unmatched.json");

        // Act
        var result = ServiceParser.Parse(kvs, DemoClusters);

        // Assert
        result.Scopes.Single(s => s.Scope == "other-scope").LeaderName.Should().Be("plain-name");
    }

    [Fact]
    public void Parse_Members_ConnUrlHostPortRoleParsed()
    {
        // Arrange
        var kvs = EtcdFixtures.LoadKv("service-full.json");

        // Act
        var result = ServiceParser.Parse(kvs, DemoClusters);

        // Assert
        var members = result.Scopes.Single(s => s.Scope == "demo-s1").Members;
        var master = members.Should().ContainSingle(m => m.Name == "s1a").Subject;
        master.Host.Should().Be("s1a");
        master.Port.Should().Be(5432);
        master.Role.Should().Be("master");
        master.State.Should().Be("running");
        // probe-поля — t06
        master.Timeline.Should().BeNull();
        master.LagBytes.Should().BeNull();
        members.Should().Contain(m => m.Name == "s1b" && m.Role == "replica");
    }

    [Fact]
    public void Parse_OptimeAndInitialize_Filled()
    {
        // Arrange
        var kvs = EtcdFixtures.LoadKv("service-full.json");

        // Act
        var result = ServiceParser.Parse(kvs, DemoClusters);

        // Assert
        var s1 = result.Scopes.Single(s => s.Scope == "demo-s1");
        s1.OptimeLeader.Should().Be(738273634528); // число-строка LSN
        s1.Initialized.Should().BeTrue();
        s1.RawConfig.Should().Be("{\"ttl\":5,\"loop_wait\":2,\"retry_timeout\":3}");
    }

    [Fact]
    public void Parse_PartialShardSuffix_Unmatched()
    {
        // Arrange — demo-s9: префикс кластера совпал, шарда s9 нет
        var kvs = EtcdFixtures.LoadKv("service-unmatched.json");

        // Act
        var result = ServiceParser.Parse(kvs, DemoClusters);

        // Assert
        var s9 = result.Scopes.Should().ContainSingle(s => s.Scope == "demo-s9").Subject;
        s9.Matched.Should().BeFalse();
        s9.Cluster.Should().Be("demo");
        s9.Shard.Should().BeNull();
        // чужой scope — не ошибка (arch/02 §7)
        result.Errors.Should().BeEmpty();
    }

    [Fact]
    public void Parse_UnknownKey_Counted()
    {
        // Arrange
        var kvs = EtcdFixtures.LoadKv("service-unmatched.json");

        // Act
        var result = ServiceParser.Parse(kvs, DemoClusters);

        // Assert
        result.UnknownKeyCount.Should().Be(1); // /service/stray/what/is/this
    }

    [Fact]
    public void Parse_EmptyPrefix_EmptyResult()
    {
        // Arrange — /service/ не существует
        // Act
        var result = ServiceParser.Parse([], DemoClusters);

        // Assert
        result.Scopes.Should().BeEmpty();
    }
}
```

- [ ] **Step 5.3: Проверка красного**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-etcd-snapshot
dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~ServiceParserTests"
```

Ожидание: FAIL — компиляция (`ServiceParser` не существует).

- [ ] **Step 5.4: Реализация**

`src/AdminPanel.Etcd/Parsing/ServiceParser.cs`:

```csharp
using System.Globalization;
using System.Text.Json;
using AdminPanel.Core;
using AdminPanel.Etcd.Client;

namespace AdminPanel.Etcd.Parsing;

// Результат разбора префикса /service/ (spec §6.2).
public sealed record ServiceParseResult(
    IReadOnlyList<HaScope> Scopes,
    IReadOnlyList<KeyParseError> Errors,
    int UnknownKeyCount);

// Парсер Patroni DCS /service/<scope>/…: leader (JSON или plain-строка стенда), members, optime, initialize.
public static class ServiceParser
{
    private sealed class ScopeAcc(string scope)
    {
        public readonly string Scope = scope;
        public string? LeaderRaw;
        public string? OptimeRaw;
        public string? InitializeRaw;
        public string? RawConfig;
        public readonly List<(string Name, string Raw)> Members = [];
    }

    public static ServiceParseResult Parse(IReadOnlyList<Kv> kvs, IReadOnlyList<ClusterInfo> clusters)
    {
        var unknown = 0;
        var accs = new Dictionary<string, ScopeAcc>();

        foreach (var kv in kvs)
        {
            // "/service/<scope>/…" → ["", "service", <scope>, …]
            var segments = kv.Key.Split('/');
            if (segments.Length < 4 || segments[1] != "service" || segments[2].Length == 0)
            {
                unknown++;
                continue;
            }

            var acc = GetOrAdd(accs, segments[2], static scope => new ScopeAcc(scope));
            switch (segments[3])
            {
                case "leader" when segments.Length == 4:
                    acc.LeaderRaw = kv.Value;
                    break;

                case "config" when segments.Length == 4:
                    acc.RawConfig = kv.Value; // raw-JSON для деталей HA (arch/02 §2.2)
                    break;

                case "initialize" when segments.Length == 4:
                    acc.InitializeRaw = kv.Value;
                    break;

                case "optime" when segments.Length == 5 && segments[4] == "leader":
                    acc.OptimeRaw = kv.Value;
                    break;

                case "members" when segments.Length == 5 && segments[4].Length > 0:
                    acc.Members.Add((segments[4], kv.Value));
                    break;

                default:
                    unknown++;
                    break;
            }
        }

        var scopes = accs.Values
            .OrderBy(a => a.Scope, StringComparer.Ordinal)
            .Select(a =>
            {
                var (cluster, shard, matched) = ScopeMatcher.Match(a.Scope, clusters);
                return new HaScope(
                    a.Scope,
                    cluster,
                    shard,
                    matched,
                    ParseLeader(a.LeaderRaw),
                    ParseOptime(a.OptimeRaw),
                    a.InitializeRaw is { Length: > 0 },
                    a.Members
                        .OrderBy(m => m.Name, StringComparer.Ordinal)
                        .Select(m => ParseMember(m.Name, m.Raw))
                        .ToList(),
                    a.RawConfig);
            })
            .ToList();

        return new ServiceParseResult(scopes, [], unknown);
    }

    // leader: JSON {"name":…} (Patroni) либо plain-строка-имя (стенд) — arch/02 §2.2.
    private static string? ParseLeader(string? raw)
    {
        if (raw is null)
            return null; // нет ключа = нет лидера

        try
        {
            using var doc = JsonDocument.Parse(raw);
            if (doc.RootElement.ValueKind == JsonValueKind.Object)
                return JsonValues.ReadString(doc.RootElement, "name")?.Trim();
        }
        catch (JsonException)
        {
            // не JSON — трактуем как строку-имя
        }

        var trimmed = raw.Trim();
        return trimmed.Length == 0 ? null : trimmed;
    }

    private static long? ParseOptime(string? raw)
        => raw is not null
            && long.TryParse(raw.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var lsn)
            ? lsn
            : null;

    private static HaMember ParseMember(string name, string raw)
    {
        var host = name;
        int? port = null;
        string? role = null;
        string? state = null;
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            role = JsonValues.ReadString(root, "role");
            state = JsonValues.ReadString(root, "state");
            var connUrl = JsonValues.ReadString(root, "conn_url");
            if (connUrl is not null)
            {
                var colon = connUrl.LastIndexOf(':');
                if (colon > 0
                    && int.TryParse(connUrl[(colon + 1)..], NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedPort))
                {
                    host = connUrl[..colon];
                    port = parsedPort;
                }
                else
                    host = connUrl;
            }
        }
        catch (JsonException)
        {
            // толерантно: member без валидного JSON остаётся именем-хостом
        }

        return new HaMember(name, host, port, role, state, null, null, null, null);
    }

    private static TValue GetOrAdd<TKey, TValue>(Dictionary<TKey, TValue> dictionary, TKey key, Func<TKey, TValue> factory)
        where TKey : notnull
    {
        if (!dictionary.TryGetValue(key, out var value))
        {
            value = factory(key);
            dictionary[key] = value;
        }

        return value;
    }
}
```

- [ ] **Step 5.5: Проверка зелёного + коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-etcd-snapshot
dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~ServiceParserTests"
git add src/AdminPanel.Etcd/Parsing/ServiceParser.cs src/tests/AdminPanel.UnitTests/EtcdFixtures/ src/tests/AdminPanel.UnitTests/ServiceParserTests.cs
git commit -m "t03: ServiceParser — Patroni DCS /service/ + мэтчинг scope (unit)"
```

Ожидание: 8 PASS; коммит.

---

### Task 6: StandNodesParser

**Связь со spec:** §6.3, §10.3.

**Files:**
- Create: `src/AdminPanel.Etcd/Parsing/StandNodesParser.cs`
- Create: `src/tests/AdminPanel.UnitTests/EtcdFixtures/stand-nodes.json`
- Test: `src/tests/AdminPanel.UnitTests/StandNodesParserTests.cs`

**Interfaces:**
- Consumes: `Kv`, `StandNode`.
- Produces: `static IReadOnlyList<StandNode> StandNodesParser.Parse(IReadOnlyList<Kv> kvs)` — использует Task 8.

- [ ] **Step 6.1: Фикстура + тест (сначала — красный)**

`src/tests/AdminPanel.UnitTests/EtcdFixtures/stand-nodes.json`:

```json
[
  { "key": "/cluster/nodes/s1a", "value": "172.28.0.11", "modRevision": 90 },
  { "key": "/cluster/nodes/s1b", "value": "172.28.0.12", "modRevision": 91 },
  { "key": "/cluster/nodes/s2a", "value": "172.28.0.21", "modRevision": 92 },
  { "key": "/cluster/nodes/s2b", "value": "", "modRevision": 93 }
]
```

`src/tests/AdminPanel.UnitTests/StandNodesParserTests.cs`:

```csharp
using AdminPanel.Etcd.Parsing;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Парсер стендового топо-реестра /cluster/nodes/<node> → IP (spec §10.3, arch/02 §2.3).
public class StandNodesParserTests
{
    [Fact]
    public void Parse_Nodes_MappedToStandNode()
    {
        // Arrange
        var kvs = EtcdFixtures.LoadKv("stand-nodes.json");

        // Act
        var nodes = StandNodesParser.Parse(kvs);

        // Assert
        nodes.Should().Contain(n => n.Name == "s1a" && n.Address == "172.28.0.11");
        nodes.Should().HaveCount(4);
    }

    [Fact]
    public void Parse_EmptyValue_NullAddress()
    {
        // Arrange
        var kvs = EtcdFixtures.LoadKv("stand-nodes.json");

        // Act
        var nodes = StandNodesParser.Parse(kvs);

        // Assert
        nodes.Should().Contain(n => n.Name == "s2b" && n.Address == null);
    }

    [Fact]
    public void Parse_EmptyPrefix_EmptyResult()
    {
        // Arrange — в проде префикса нет: пустой ответ range
        // Act
        var nodes = StandNodesParser.Parse([]);

        // Assert
        nodes.Should().BeEmpty();
    }
}
```

- [ ] **Step 6.2: Проверка красного**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-etcd-snapshot
dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~StandNodesParserTests"
```

Ожидание: FAIL — компиляция.

- [ ] **Step 6.3: Реализация**

`src/AdminPanel.Etcd/Parsing/StandNodesParser.cs`:

```csharp
using AdminPanel.Core;
using AdminPanel.Etcd.Client;

namespace AdminPanel.Etcd.Parsing;

// Парсер стендовой топологии /cluster/nodes/<node> → <ip> (lease TTL у нод стенда, arch/02 §2.3).
// Реестр однороден: любые ключи под префиксом — узлы; посторонних форм нет.
public static class StandNodesParser
{
    public static IReadOnlyList<StandNode> Parse(IReadOnlyList<Kv> kvs)
    {
        var nodes = new List<StandNode>();
        foreach (var kv in kvs)
        {
            // "/cluster/nodes/<node>" → ["", "cluster", "nodes", <node>]
            var segments = kv.Key.Split('/');
            if (segments.Length != 4 || segments[1] != "cluster" || segments[2] != "nodes" || segments[3].Length == 0)
                continue;

            var address = kv.Value.Trim();
            nodes.Add(new StandNode(segments[3], address.Length == 0 ? null : address));
        }

        return nodes;
    }
}
```

- [ ] **Step 6.4: Проверка зелёного + коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-etcd-snapshot
dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~StandNodesParserTests"
git add src/AdminPanel.Etcd/Parsing/StandNodesParser.cs src/tests/AdminPanel.UnitTests/EtcdFixtures/stand-nodes.json src/tests/AdminPanel.UnitTests/StandNodesParserTests.cs
git commit -m "t03: StandNodesParser — стендовый топо-реестр /cluster/nodes/ (unit)"
```

Ожидание: 3 PASS; коммит.

---

### Task 7: IEtcdGateway + EtcdGateway (HTTP JSON gateway)

**Связь со spec:** §4 (интерфейс, реализация, DTO, base64, PrefixEnd, имена полей, строки-числа), §3.17, §10.6.

**Files:**
- Create: `src/AdminPanel.Etcd/Client/IEtcdGateway.cs`
- Create: `src/AdminPanel.Etcd/Client/EtcdGateway.cs`
- Create: `src/tests/AdminPanel.UnitTests/EtcdFixtures/gateway-range.json`, `gateway-status.json`, `gateway-member-list.json`, `gateway-alarm.json`
- Test: `src/tests/AdminPanel.UnitTests/EtcdGatewayTests.cs`

**Interfaces:**
- Consumes: `Result<T>` (Infrastructure — Etcd ссылается на Core → Infrastructure транзитивно), `Kv`, Core-типы `EtcdMember`/`EtcdAlarm`/`EtcdAlarmType`.
- Produces: `namespace AdminPanel.Etcd.Client`:
  - `interface IEtcdGateway { Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct); Task<Result<EtcdStatusPayload>> StatusAsync(string endpoint, CancellationToken ct); Task<Result<IReadOnlyList<EtcdMember>>> MemberListAsync(string endpoint, CancellationToken ct); Task<Result<IReadOnlyList<EtcdAlarm>>> AlarmAsync(string endpoint, CancellationToken ct); }`
  - `sealed record EtcdStatusPayload(string? Version, long? DbSizeBytes, ulong? LeaderMemberId, ulong? RaftIndex, ulong? RaftTerm)`
  - `sealed class EtcdGateway(HttpClient httpClient) : IEtcdGateway` c `public const string HttpClientName = "etcd"`
  - `public sealed class EtcdHttpException(...)`, `public sealed class EtcdUnreachableException(...)` — используют Task 9 и integration (Task 11).

- [ ] **Step 7.1: Gateway-фикстуры (сырые ответы /v3/*, base64 и строки-числа — как отдаёт etcd 3.5.21)**

`src/tests/AdminPanel.UnitTests/EtcdFixtures/gateway-range.json` (base64: `"/a/b"` → `L2EvYg==`, `"v"` → `dg==`):

```json
{ "header": { "member_id": "13820473277879079085", "raft_term": "3" }, "kvs": [ { "key": "L2EvYg==", "value": "dg==", "mod_revision": "42" } ], "count": "1" }
```

`src/tests/AdminPanel.UnitTests/EtcdFixtures/gateway-status.json`:

```json
{ "header": { "member_id": "13820473277879079085", "raft_term": "3" }, "version": "3.5.21", "dbSize": "20480", "leader": "13820473277879079085", "raftIndex": "17", "raftTerm": "3" }
```

`src/tests/AdminPanel.UnitTests/EtcdFixtures/gateway-member-list.json`:

```json
{ "header": { "member_id": "13820473277879079085" }, "members": [ { "ID": "13820473277879079085", "name": "test", "peerURLs": [ "http://localhost:2380" ], "clientURLs": [ "http://localhost:2379" ] } ] }
```

`src/tests/AdminPanel.UnitTests/EtcdFixtures/gateway-alarm.json`:

```json
{ "header": { "member_id": "13820473277879079085" }, "alarms": [ { "memberID": "13820473277879079085", "alarm": 1 } ] }
```

- [ ] **Step 7.2: Тест (сначала — красный)**

`src/tests/AdminPanel.UnitTests/EtcdGatewayTests.cs`:

```csharp
using System.Net;
using System.Text;
using System.Text.Json;
using AdminPanel.Core;
using AdminPanel.Etcd.Client;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Транспорт etcd /v3/*: base64, range_end, фактические имена полей gateway, ошибки (spec §10.6).
public class EtcdGatewayTests
{
    // Управляемый транспорт: перехватывает запросы и отвечает заготовленным JSON.
    private sealed class FakeHandler(Func<HttpRequestMessage, HttpResponseMessage> responder) : HttpMessageHandler
    {
        public readonly List<(string Url, string Body)> Requests = [];

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            Requests.Add((request.RequestUri!.ToString(), await request.Content!.ReadAsStringAsync(ct)));
            return responder(request);
        }
    }

    private static HttpResponseMessage Json(string body) => new()
    {
        StatusCode = HttpStatusCode.OK,
        Content = new StringContent(body, Encoding.UTF8, "application/json"),
    };

    private static EtcdGateway NewGateway(FakeHandler handler) => new(new HttpClient(handler));

    [Fact]
    public async Task Range_Prefix_RequestHasBase64KeyAndRangeEnd()
    {
        // Arrange
        var handler = new FakeHandler(_ => Json("""{"kvs":[]}"""));
        var gateway = NewGateway(handler);

        // Act
        await gateway.RangeAsync("http://etcd:2379", "/clusters/", CancellationToken.None);

        // Assert
        var request = handler.Requests.Should().ContainSingle().Subject;
        request.Url.Should().Be("http://etcd:2379/v3/kv/range");
        var body = JsonDocument.Parse(request.Body);
        // base64("/clusters/") и range_end = префикс с инкрементированным последним байтом: "/clusters0"
        // (константы выверены: printf '%s' "/clusters/" | base64 → L2NsdXN0ZXJzLw==)
        body.RootElement.GetProperty("key").GetString().Should().Be("L2NsdXN0ZXJzLw==");
        body.RootElement.GetProperty("range_end").GetString().Should().Be("L2NsdXN0ZXJzMA==");
    }

    [Fact]
    public async Task Range_DecodesBase64Kvs()
    {
        // Arrange
        var handler = new FakeHandler(_ => Json(EtcdFixtures.LoadText("gateway-range.json")));
        var gateway = NewGateway(handler);

        // Act
        var result = await gateway.RangeAsync("http://etcd:2379", "/a/", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        var kv = result.Value.Should().ContainSingle().Subject;
        kv.Key.Should().Be("/a/b");
        kv.Value.Should().Be("v");
        kv.ModRevision.Should().Be(42); // mod_revision приходит строкой
    }

    [Fact]
    public async Task Range_MissingKvs_EmptyList()
    {
        // Arrange — пустой префикс: gateway не отдаёт kvs вовсе
        var handler = new FakeHandler(_ => Json("""{"header":{}}"""));
        var gateway = NewGateway(handler);

        // Act
        var result = await gateway.RangeAsync("http://etcd:2379", "/nope/", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().BeEmpty();
    }

    [Fact]
    public async Task Status_ParsesFields()
    {
        // Arrange
        var handler = new FakeHandler(_ => Json(EtcdFixtures.LoadText("gateway-status.json")));
        var gateway = NewGateway(handler);

        // Act
        var result = await gateway.StatusAsync("http://etcd:2379", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Version.Should().Be("3.5.21");
        result.Value.DbSizeBytes.Should().Be(20480);
        result.Value.LeaderMemberId.Should().Be(13820473277879079085UL);
        result.Value.RaftIndex.Should().Be(17);
        result.Value.RaftTerm.Should().Be(3);
        handler.Requests.Single().Url.Should().Be("http://etcd:2379/v3/maintenance/status");
    }

    [Fact]
    public async Task MemberList_ParsesUrls()
    {
        // Arrange
        var handler = new FakeHandler(_ => Json(EtcdFixtures.LoadText("gateway-member-list.json")));
        var gateway = NewGateway(handler);

        // Act
        var result = await gateway.MemberListAsync("http://etcd:2379", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        var member = result.Value.Should().ContainSingle().Subject;
        member.Id.Should().Be(13820473277879079085UL);
        member.Name.Should().Be("test");
        member.PeerUrls.Should().Contain("http://localhost:2380");
        member.ClientUrls.Should().Contain("http://localhost:2379");
    }

    [Fact]
    public async Task Alarm_MapsAlarmType()
    {
        // Arrange
        var handler = new FakeHandler(_ => Json(EtcdFixtures.LoadText("gateway-alarm.json")));
        var gateway = NewGateway(handler);

        // Act
        var result = await gateway.AlarmAsync("http://etcd:2379", CancellationToken.None);

        // Assert
        var alarm = result.Value.Should().ContainSingle().Subject;
        alarm.MemberId.Should().Be(13820473277879079085UL);
        alarm.Type.Should().Be(EtcdAlarmType.NoSpace); // "alarm": 1
    }

    [Fact]
    public async Task HttpError_ReturnsFailed()
    {
        // Arrange — Content задан явно: ответ без тела дал бы null-Content и NRE вместо EtcdHttpException
        var handler = new FakeHandler(_ => new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
        {
            Content = new StringContent(string.Empty),
        });
        var gateway = NewGateway(handler);

        // Act
        var result = await gateway.StatusAsync("http://etcd:2379", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<EtcdHttpException>();
    }

    [Fact]
    public async Task NetworkError_ReturnsFailed()
    {
        // Arrange — HttpClient с недостижимым портом: connection refused мгновенен
        var gateway = new EtcdGateway(new HttpClient { Timeout = TimeSpan.FromSeconds(2) });

        // Act
        var result = await gateway.StatusAsync("http://localhost:1", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
    }
}
```

- [ ] **Step 7.3: Проверка красного**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-etcd-snapshot
dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~EtcdGatewayTests"
```

Ожидание: FAIL — компиляция (`IEtcdGateway`/`EtcdGateway` не существуют).

- [ ] **Step 7.4: Реализация**

`src/AdminPanel.Etcd/Client/IEtcdGateway.cs`:

```csharp
using AdminPanel.Core;
using AdminPanel.Infrastructure;

namespace AdminPanel.Etcd.Client;

// Read-only клиент etcd через HTTP JSON gateway /v3/* (arch/02 §1).
// Методы принимают endpoint явно: выбор/ротация «активного» — задача refresher (arch/02 §4).
// Панель не пишет: put/lease в интерфейсе отсутствуют принципиально.
public interface IEtcdGateway
{
    // Префиксный range: POST /v3/kv/range {"key": b64, "range_end": b64}.
    Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct);

    // POST /v3/maintenance/status — персонально на указанный endpoint (arch/02 §2.4).
    Task<Result<EtcdStatusPayload>> StatusAsync(string endpoint, CancellationToken ct);

    // POST /v3/cluster/member/list.
    Task<Result<IReadOnlyList<EtcdMember>>> MemberListAsync(string endpoint, CancellationToken ct);

    // POST /v3/maintenance/alarm.
    Task<Result<IReadOnlyList<EtcdAlarm>>> AlarmAsync(string endpoint, CancellationToken ct);
}

// Данные status-ответа без контекста endpoint (url/latency добавляет refresher; spec §17).
public sealed record EtcdStatusPayload(
    string? Version,
    long? DbSizeBytes,
    ulong? LeaderMemberId,
    ulong? RaftIndex,
    ulong? RaftTerm);
```

`src/AdminPanel.Etcd/Client/EtcdGateway.cs`:

```csharp
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Net.Http.Json;
using AdminPanel.Core;
using AdminPanel.Infrastructure;

namespace AdminPanel.Etcd.Client;

// HTTP-ошибка gateway: не-2xx от /v3/*.
public sealed class EtcdHttpException(string endpoint, int statusCode, string body)
    : Exception($"etcd {endpoint} ответил {statusCode}: {body}");

// Все живые endpoints не ответили (после failover).
public sealed class EtcdUnreachableException(string message) : Exception(message);

// Реализация IEtcdGateway: HttpClient из IHttpClientFactory (именованный "etcd", ModuleExtensions).
// Таймаут задаётся конфигурацией клиента, не здесь (spec §4.2).
[InjectAsSingleton(typeof(IEtcdGateway))]
public sealed class EtcdGateway(HttpClient httpClient) : IEtcdGateway
{
    public const string HttpClientName = "etcd";

    // etcd gateway сериализует int64 decimal-строками и не приводит proto-имена к camelCase (spec §3.17).
    private static readonly JsonSerializerOptions Json = new()
    {
        NumberHandling = JsonNumberHandling.AllowReadingFromString,
    };

    public async Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
    {
        var body = new { key = ToB64(prefix), range_end = ToB64(PrefixEnd(prefix)) };
        var result = await Result<RangeResponse>.FromAsync(
            () => PostAsync<RangeResponse>(endpoint, "/v3/kv/range", body, ct));
        return result.Map(r => (IReadOnlyList<Kv>)(r.Kvs ?? [])
            .Select(k => new Kv(FromB64(k.Key), FromB64(k.Value), k.ModRevision))
            .ToList());
    }

    public async Task<Result<EtcdStatusPayload>> StatusAsync(string endpoint, CancellationToken ct)
    {
        var result = await Result<StatusResponse>.FromAsync(
            () => PostAsync<StatusResponse>(endpoint, "/v3/maintenance/status", new { }, ct));
        return result.Map(r => new EtcdStatusPayload(r.Version, r.DbSize, r.Leader, r.RaftIndex, r.RaftTerm));
    }

    public async Task<Result<IReadOnlyList<EtcdMember>>> MemberListAsync(string endpoint, CancellationToken ct)
    {
        var result = await Result<MemberListResponse>.FromAsync(
            () => PostAsync<MemberListResponse>(endpoint, "/v3/cluster/member/list", new { }, ct));
        return result.Map(r => (IReadOnlyList<EtcdMember>)(r.Members ?? [])
            .Select(m => new EtcdMember(m.Id, m.Name, m.PeerUrls ?? [], m.ClientUrls ?? []))
            .ToList());
    }

    public async Task<Result<IReadOnlyList<EtcdAlarm>>> AlarmAsync(string endpoint, CancellationToken ct)
    {
        var result = await Result<AlarmResponse>.FromAsync(
            () => PostAsync<AlarmResponse>(endpoint, "/v3/maintenance/alarm", new { }, ct));
        return result.Map(r => (IReadOnlyList<EtcdAlarm>)(r.Alarms ?? [])
            .Select(a => new EtcdAlarm(a.MemberId, a.Type))
            .ToList());
    }

    private async Task<T> PostAsync<T>(string endpoint, string path, object body, CancellationToken ct)
    {
        using var response = await httpClient.PostAsJsonAsync(endpoint + path, body, Json, ct);
        if (!response.IsSuccessStatusCode)
        {
            // null-safe: отдельные серверы/заглушки присылают ответ без тела (Content = null)
            var errorBody = response.Content is null
                ? string.Empty
                : await response.Content.ReadAsStringAsync(ct);
            throw new EtcdHttpException(endpoint, (int)response.StatusCode, errorBody);
        }

        return await response.Content.ReadFromJsonAsync<T>(Json, ct)
            ?? throw new EtcdHttpException(endpoint, (int)response.StatusCode, "пустой ответ");
    }

    private static string ToB64(string value)
        => Convert.ToBase64String(Encoding.UTF8.GetBytes(value));

    private static string FromB64(string value)
        => Encoding.UTF8.GetString(Convert.FromBase64String(value));

    // range_end по префиксу: последний байт +1; переполнение 0xFF переносится влево (spec §4.2).
    private static string PrefixEnd(string prefix)
    {
        var bytes = Encoding.UTF8.GetBytes(prefix);
        for (var i = bytes.Length - 1; i >= 0; i--)
        {
            if (bytes[i] != 0xFF)
            {
                bytes[i]++;
                return Encoding.UTF8.GetString(bytes[..(i + 1)]);
            }
        }

        return string.Empty; // префикс целиком из 0xFF: пустой range_end = «до конца»
    }

    // DTO ответов: имена полей по фактическим proto-именам etcd 3.5 (mod_revision/dbSize/peerURLs…).
    private sealed class RangeResponse
    {
        [JsonPropertyName("kvs")]
        public List<RangeKv>? Kvs { get; set; }
    }

    private sealed class RangeKv
    {
        [JsonPropertyName("key")]
        public string Key { get; set; } = "";

        [JsonPropertyName("value")]
        public string Value { get; set; } = "";

        [JsonPropertyName("mod_revision")]
        public ulong ModRevision { get; set; }
    }

    private sealed class StatusResponse
    {
        [JsonPropertyName("version")]
        public string? Version { get; set; }

        [JsonPropertyName("dbSize")]
        public long? DbSize { get; set; }

        [JsonPropertyName("leader")]
        public ulong? Leader { get; set; }

        [JsonPropertyName("raftIndex")]
        public ulong? RaftIndex { get; set; }

        [JsonPropertyName("raftTerm")]
        public ulong? RaftTerm { get; set; }
    }

    private sealed class MemberListResponse
    {
        [JsonPropertyName("members")]
        public List<MemberDto>? Members { get; set; }
    }

    private sealed class MemberDto
    {
        [JsonPropertyName("ID")]
        public ulong Id { get; set; }

        [JsonPropertyName("name")]
        public string? Name { get; set; }

        [JsonPropertyName("peerURLs")]
        public List<string>? PeerUrls { get; set; }

        [JsonPropertyName("clientURLs")]
        public List<string>? ClientUrls { get; set; }
    }

    private sealed class AlarmResponse
    {
        [JsonPropertyName("alarms")]
        public List<AlarmDto>? Alarms { get; set; }
    }

    private sealed class AlarmDto
    {
        [JsonPropertyName("memberID")]
        public ulong MemberId { get; set; }

        [JsonPropertyName("alarm")]
        public EtcdAlarmType Type { get; set; }
    }
}
```

Примечание к листингу: `Result<T>.FromAsync(Func<ValueTask<T>>)` — сигнатура из t01 (`Result.cs`); лямбда `() => PostAsync<T>(...)` конвертируется в `ValueTask<T>` неявно (async-метод `PostAsync` возвращает `Task<T>` → `ValueTask<T>` implicit-конверсия из `Task<T>` есть в BCL). Если компилятор потребует явную обёртку — прецедент t02: заменить лямбду на `async () => await PostAsync<T>(…)`.

- [ ] **Step 7.5: Проверка зелёного + коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-etcd-snapshot
dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~EtcdGatewayTests"
dotnet build src/AdminPanel.slnx
git add src/AdminPanel.Etcd/Client/ src/tests/AdminPanel.UnitTests/EtcdFixtures/gateway-range.json src/tests/AdminPanel.UnitTests/EtcdFixtures/gateway-status.json src/tests/AdminPanel.UnitTests/EtcdFixtures/gateway-member-list.json src/tests/AdminPanel.UnitTests/EtcdFixtures/gateway-alarm.json src/tests/AdminPanel.UnitTests/EtcdGatewayTests.cs
git commit -m "t03: EtcdGateway — HTTP JSON gateway /v3/*, base64, строки-числа (unit)"
```

Ожидание: 8 PASS, build 0 warnings; коммит.

---

### Task 8: SnapshotStore + SnapshotBuilder

**Связь со spec:** §7.1 (volatile-замена, Current nullable), §6.5 (сборка: BuiltAtUtc, суммирование UnknownKeyCount, конкатенация ParseErrors, Alerts/Probes пусты), §10.7–10.8.

**Files:**
- Create: `src/AdminPanel.Etcd/SnapshotStore.cs`
- Create: `src/AdminPanel.Etcd/SnapshotBuilder.cs`
- Test: `src/tests/AdminPanel.UnitTests/SnapshotStoreTests.cs`, `SnapshotBuilderTests.cs`

**Interfaces:**
- Consumes: `ClustersParseResult`, `ServiceParseResult`, `StandNodesParser.Parse`, Core-типы.
- Produces: `namespace AdminPanel.Etcd`:
  - `interface ISnapshotStore { EtcdSnapshot? Current { get; } void Replace(EtcdSnapshot snapshot); }` + `[InjectAsSingleton(typeof(ISnapshotStore))] sealed class SnapshotStore`
  - `static class SnapshotBuilder { static EtcdSnapshot Build(TimeProvider time, ClustersParseResult clusters, ServiceParseResult service, IReadOnlyList<StandNode> standNodes, IReadOnlyList<EtcdMember> members, IReadOnlyList<EtcdAlarm> alarms, EtcdStatus etcd); }` — использует Task 9.

- [ ] **Step 8.1: Тесты (сначала — красные)**

`src/tests/AdminPanel.UnitTests/SnapshotStoreTests.cs`:

```csharp
using AdminPanel.Core;
using AdminPanel.Etcd;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Хранилище снапшота: атомарная замена ссылки, nullable-Current (spec §10.8).
public class SnapshotStoreTests
{
    private static EtcdSnapshot NewSnapshot()
        => new(
            new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero),
            new EtcdStatus(true, [], [], [], null, false, DateTimeOffset.UtcNow, 0),
            [], [], [], [], [], [], 0);

    [Fact]
    public void Current_NullBeforeFirstReplace()
    {
        // Arrange
        var store = new SnapshotStore();

        // Act
        var current = store.Current;

        // Assert
        current.Should().BeNull();
    }

    [Fact]
    public void Replace_SetsCurrentAtomically()
    {
        // Arrange
        var store = new SnapshotStore();
        var snapshot = NewSnapshot();

        // Act
        store.Replace(snapshot);

        // Assert
        store.Current.Should().BeSameAs(snapshot);
    }
}
```

`src/tests/AdminPanel.UnitTests/SnapshotBuilderTests.cs`:

```csharp
using AdminPanel.Core;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Parsing;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Сборка EtcdSnapshot из частей тика (spec §10.7).
public class SnapshotBuilderTests
{
    // FixedTimeProvider — существующий из t02 (src/tests/AdminPanel.UnitTests/FixedTimeProvider.cs).
    [Fact]
    public void Build_FullParts_AssemblesSnapshot()
    {
        // Arrange
        var time = new FixedTimeProvider { Utc = new(2026, 8, 22, 12, 0, 0, TimeSpan.Zero) };
        var clusters = ClustersParser.Parse(EtcdFixtures.LoadKv("clusters-full.json"));
        var service = ServiceParser.Parse(EtcdFixtures.LoadKv("service-full.json"), clusters.Clusters);
        var nodes = StandNodesParser.Parse(EtcdFixtures.LoadKv("stand-nodes.json"));
        var members = new List<EtcdMember> { new(42, "test", ["http://p"], ["http://c"]) };
        var alarms = new List<EtcdAlarm> { new(42, EtcdAlarmType.NoSpace) };
        var etcd = new EtcdStatus(true, [], members, alarms, "http://e1", false, time.GetUtcNow(), 0);

        // Act
        var snapshot = SnapshotBuilder.Build(time, clusters, service, nodes, members, alarms, etcd);

        // Assert
        snapshot.BuiltAtUtc.Should().Be(time.Utc);
        snapshot.Clusters.Should().ContainSingle(c => c.Name == "demo");
        snapshot.HaScopes.Should().Contain(s => s.Scope == "demo-s1");
        snapshot.StandNodes.Should().HaveCount(4);
        snapshot.Alerts.Should().BeEmpty();   // AlertEngine — t04
        snapshot.Probes.Should().BeEmpty();   // пробы — t06
        snapshot.UnknownKeyCount.Should().Be(0);
        snapshot.ParseErrors.Should().BeEmpty();
    }

    [Fact]
    public void Build_SumsDiagnostics()
    {
        // Arrange — вырожденные фикстуры дают ошибки и unknown-ключи обоих префиксов
        var time = new FixedTimeProvider();
        var clusters = ClustersParser.Parse(EtcdFixtures.LoadKv("clusters-degenerate.json"));
        var service = ServiceParser.Parse(EtcdFixtures.LoadKv("service-unmatched.json"), clusters.Clusters);
        var etcd = new EtcdStatus(true, [], [], [], null, false, time.GetUtcNow(), 0);

        // Act
        var snapshot = SnapshotBuilder.Build(time, clusters, service, [], [], [], etcd);

        // Assert
        snapshot.UnknownKeyCount.Should().Be(2); // surprise (/clusters/) + stray (/service/)
        snapshot.ParseErrors.Should().HaveCount(4); // status-битый, replicas, master-пустой, bucket_abc
    }
}
```

- [ ] **Step 8.2: Проверка красного**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-etcd-snapshot
dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~SnapshotStoreTests|FullyQualifiedName~SnapshotBuilderTests"
```

Ожидание: FAIL — компиляция (`ISnapshotStore`/`SnapshotBuilder` не существуют).

- [ ] **Step 8.3: Реализация**

`src/AdminPanel.Etcd/SnapshotStore.cs`:

```csharp
using AdminPanel.Core;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Etcd;

// Хранилище текущего снапшота: читатели никогда не блокируются (arch/01 §1).
public interface ISnapshotStore
{
    // До первого тика снапшота нет — потребители (t04) показывают «загрузка» (spec §3.13).
    EtcdSnapshot? Current { get; }

    // Атомарная замена ссылки; писатель один — SnapshotRefresher (arch/01 §1).
    void Replace(EtcdSnapshot snapshot);
}

[InjectAsSingleton(typeof(ISnapshotStore))]
public sealed class SnapshotStore : ISnapshotStore
{
    private volatile EtcdSnapshot? _current;

    public EtcdSnapshot? Current => _current;

    public void Replace(EtcdSnapshot snapshot) => _current = snapshot;
}
```

`src/AdminPanel.Etcd/SnapshotBuilder.cs`:

```csharp
using AdminPanel.Core;
using AdminPanel.Etcd.Parsing;

namespace AdminPanel.Etcd;

// Сборка EtcdSnapshot из частей одного тика: чистая функция (spec §6.5).
// Alerts/Probes пусты в t03 (наполняют AlertEngine t04 и пробы t06).
public static class SnapshotBuilder
{
    public static EtcdSnapshot Build(
        TimeProvider time,
        ClustersParseResult clusters,
        ServiceParseResult service,
        IReadOnlyList<StandNode> standNodes,
        IReadOnlyList<EtcdMember> members,
        IReadOnlyList<EtcdAlarm> alarms,
        EtcdStatus etcd)
        => new(
            time.GetUtcNow(),
            etcd,
            clusters.Clusters,
            service.Scopes,
            standNodes,
            [],
            [],
            [.. clusters.Errors, .. service.Errors],
            clusters.UnknownKeyCount + service.UnknownKeyCount);
}
```

- [ ] **Step 8.4: Проверка зелёного + коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-etcd-snapshot
dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~SnapshotStoreTests|FullyQualifiedName~SnapshotBuilderTests"
git add src/AdminPanel.Etcd/SnapshotStore.cs src/AdminPanel.Etcd/SnapshotBuilder.cs src/tests/AdminPanel.UnitTests/SnapshotStoreTests.cs src/tests/AdminPanel.UnitTests/SnapshotBuilderTests.cs
git commit -m "t03: SnapshotStore + SnapshotBuilder — атомарная замена и сборка снапшота (unit)"
```

Ожидание: 4 PASS; коммит.

---

### Task 9: EtcdOptions + ModuleExtensions + SnapshotRefresher

**Связь со spec:** §7.2 (алгоритм тика 02 §4, sticky-failover, сценарий отказа §3.9, QuorumSuspected §3.11, IHealthCheckService), §8.1 (EtcdOptions), §4.2 (AddHttpClient + таймаут с fallback), §3.1/§3.10, §10.9.

**Files:**
- Create: `src/AdminPanel.Etcd/EtcdOptions.cs`
- Create: `src/AdminPanel.Etcd/SnapshotRefresher.cs`
- Modify: `src/AdminPanel.Etcd/ModuleExtensions.cs`
- Test: `src/tests/AdminPanel.UnitTests/SnapshotRefresherTests.cs` (включая общий для Task 10 хелпер `RefresherTestHarness` + `FakeEtcdGateway`; `FixedTimeProvider` — существующий из t02, не создаётся)

**Interfaces:**
- Consumes: `IEtcdGateway`, `ISnapshotStore`, `SnapshotBuilder`, парсеры, `Result`, `TimeProvider` (зарегистрирован в Api-сборке t02 — резолвится в хосте).
- Produces (используют Task 10 и 11):
  - `[Config("AdminPanel:Etcd")] class EtcdOptions { string[] Endpoints; double RefreshIntervalSeconds = 3; double RequestTimeoutSeconds = 2; }`
  - `[InjectAsSingleton(typeof(IHostedService))] sealed class SnapshotRefresher(IEtcdGateway, ISnapshotStore, IOptions<EtcdOptions>, TimeProvider, ILogger<SnapshotRefresher>) : BackgroundService, IHealthCheckService` c `public Task<Result> RefreshOnceAsync(CancellationToken ct)`, `bool Inited`, `bool Working`, `Result StatusError`.

- [ ] **Step 9.1: EtcdOptions**

`src/AdminPanel.Etcd/EtcdOptions.cs`:

```csharp
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Etcd;

// [Config]-POCO etcd-подключения: секция AdminPanel:Etcd (arch/01 §6, spec §8.1).
[Config("AdminPanel:Etcd")]
public class EtcdOptions
{
    // HTTP JSON gateway endpoints, напр. "http://etcd1:2379". Обязателен хотя бы один.
    public string[] Endpoints { get; set; } = [];

    // Тик снапшота (arch/02 §4). <= 0 — fallback 3 c с LogWarning.
    public double RefreshIntervalSeconds { get; set; } = 3;

    // Таймаут HTTP-запроса к одному endpoint (arch/01 §6). <= 0 — fallback 2 c.
    public double RequestTimeoutSeconds { get; set; } = 2;
}
```

- [ ] **Step 9.2: ModuleExtensions.AddEtcd (AddHttpClient)**

`src/AdminPanel.Etcd/ModuleExtensions.cs` — полностью:

```csharp
using System.Reflection;
using AdminPanel.Etcd.Client;
using AdminPanel.Infrastructure.DI;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AdminPanel.Etcd;

// Модуль etcd-клиента: attribute-DI + именованный HttpClient "etcd" с таймаутом из настроек.
public static class ModuleExtensions
{
    private static Assembly Assembly => typeof(ModuleExtensions).Assembly;

    public static IServiceCollection AddEtcd(this IServiceCollection services)
    {
        services.AutoRegistration(Assembly);

        // Порядок важен: AddHttpClient<EtcdGateway> добавляется ПОСЛЕ AutoRegistration,
        // чтобы typed-фабрика (последняя регистрация типа) перекрыла дескриптор AutoRegistration
        // и EtcdGateway получал HttpClient из фабрики, а не из дефолтного резолва.
        // Маркер логгера — EtcdGateway (не static: прецедент CS0718).
        services
           .AddHttpClient<EtcdGateway>(EtcdGateway.HttpClientName)
           .ConfigureHttpClient((sp, client) =>
            {
                var seconds = sp.GetRequiredService<IOptions<EtcdOptions>>().Value.RequestTimeoutSeconds;
                if (seconds <= 0)
                {
                    sp.GetRequiredService<ILogger<EtcdGateway>>()
                       .LogWarning("AdminPanel:Etcd:RequestTimeoutSeconds <= 0 — использую 2 c");
                    seconds = 2;
                }

                client.Timeout = TimeSpan.FromSeconds(seconds);
            });

        return services;
    }
}
```

- [ ] **Step 9.3: SnapshotRefresher**

`src/AdminPanel.Etcd/SnapshotRefresher.cs`:

```csharp
using System.Diagnostics;
using AdminPanel.Core;
using AdminPanel.Etcd.Client;
using AdminPanel.Etcd.Parsing;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AdminPanel.Etcd;

// Единственный писатель снапшота (arch/01 §1): тик RefreshIntervalSeconds по arch/02 §4.
// Отказ тика = обновление только Etcd-части: данные и BuiltAtUtc прежние, возраст растёт (spec §3.9).
[InjectAsSingleton(typeof(IHostedService))]
public sealed class SnapshotRefresher(
    IEtcdGateway gateway,
    ISnapshotStore store,
    IOptions<EtcdOptions> options,
    TimeProvider time,
    ILogger<SnapshotRefresher> logger) : BackgroundService, IHealthCheckService
{
    private string? _activeEndpoint;
    private bool _inited;
    private bool _working;
    private Result _statusError = Result.Success();
    private bool _endpointsWarned;

    public bool Inited => _inited;

    public bool Working => _working;

    public Result StatusError => _statusError;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var seconds = options.Value.RefreshIntervalSeconds;
        if (seconds <= 0)
        {
            logger.LogWarning("AdminPanel:Etcd:RefreshIntervalSeconds <= 0 — использую 3 c");
            seconds = 3;
        }

        // Первый тик сразу: панель набирает данные со старта; далее по периоду (spec §7.2).
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(seconds));
        do
        {
            await RefreshOnceAsync(stoppingToken);
        }
        while (await timer.WaitForNextTickAsync(stoppingToken));
    }

    // Ядро одного тика — публично для unit/integration-тестов без хоста.
    public async Task<Result> RefreshOnceAsync(CancellationToken ct)
    {
        var endpoints = options.Value.Endpoints.Where(IsValidEndpoint).ToArray();
        if (!_endpointsWarned && endpoints.Length == 0)
        {
            // один warning на серии отказов — не шумим в логах каждым тиком (spec §3.12)
            logger.LogWarning("AdminPanel:Etcd:Endpoints не задан или невалиден — etcd-данные недоступны");
            _endpointsWarned = true;
        }

        var now = time.GetUtcNow();
        var previous = store.Current;

        if (endpoints.Length == 0)
            return FailTick(previous, [], now, "AdminPanel:Etcd:Endpoints не задан или невалиден");

        // 1. Статусы персонально по всем endpoints — параллельно (arch/02 §4 п.1).
        var statuses = await Task.WhenAll(endpoints.Select(e => StatusOfAsync(e, ct)));
        var alive = statuses.Where(s => s.Reachable).ToArray();
        if (alive.Length == 0)
            return FailTick(previous, statuses, now, "нет живых endpoints etcd");

        // 2. Активный: sticky с прошлого тика, иначе первый живой (arch/02 §4 п.1).
        var active = alive.FirstOrDefault(e => e.Url == _activeEndpoint) ?? alive[0];
        _activeEndpoint = active.Url;

        // 3. Чтения на активном — параллельно; транспортный провал → failover по кругу живых (spec §3.10).
        var clustersTask = WithFailoverAsync(alive, active, (ep, t) => gateway.RangeAsync(ep, Prefixes.Clusters, t), ct);
        var serviceTask = WithFailoverAsync(alive, active, (ep, t) => gateway.RangeAsync(ep, Prefixes.Service, t), ct);
        var nodesTask = WithFailoverAsync(alive, active, (ep, t) => gateway.RangeAsync(ep, Prefixes.Nodes, t), ct);
        var membersTask = WithFailoverAsync(alive, active, (ep, t) => gateway.MemberListAsync(ep, t), ct);
        var alarmsTask = WithFailoverAsync(alive, active, (ep, t) => gateway.AlarmAsync(ep, t), ct);

        var clustersKv = await clustersTask;
        var serviceKv = await serviceTask;
        var nodesKv = await nodesTask;
        var members = await membersTask;
        var alarms = await alarmsTask;

        // Частичный KV-провал = неполный снапшот: консервативно отказ тика, данные прежние
        // (уточнение к spec §7.2 п.5: пустой префикс — валидные данные, транспортный отказ — нет).
        if (!clustersKv.IsSuccess || !serviceKv.IsSuccess || !nodesKv.IsSuccess)
            return FailTick(previous, statuses, now, "KV-чтения etcd не удались");

        // 4. Парсеры → модель (чистые функции, arch/02 §4 п.3).
        var clustersParsed = ClustersParser.Parse(clustersKv.Value);
        var serviceParsed = ServiceParser.Parse(serviceKv.Value, clustersParsed.Clusters);
        var nodes = StandNodesParser.Parse(nodesKv.Value);

        // 5. Кворум-эвристика (spec §3.11) + мягкие метаданные member/alarm (ошибка не роняет тик).
        var quorumSuspected = alive.All(a => a.LeaderMemberId is not > 0 || a.RaftTerm is not > 0)
            || alive.Any(a => a.Errors.Any(IsRaftError));

        var metaErrors = new List<string>();
        if (!members.IsSuccess)
            metaErrors.Add("member/list: " + members.Error!.Message);
        if (!alarms.IsSuccess)
            metaErrors.Add("alarm: " + alarms.Error!.Message);
        EtcdEndpoint[] patchedStatuses = statuses;
        if (metaErrors.Count > 0)
        {
            patchedStatuses = [.. statuses.Select(s => s.Url == active.Url
                ? s with { Errors = [.. s.Errors, .. metaErrors] }
                : s)];
        }

        var etcd = new EtcdStatus(
            true,
            patchedStatuses,
            members.IsSuccess ? members.Value : [],
            alarms.IsSuccess ? alarms.Value : [],
            active.Url,
            quorumSuspected,
            now,
            0);

        // 6. Сборка + атомарная замена (arch/02 §4 п.4; Alerts — t04).
        store.Replace(SnapshotBuilder.Build(
            time, clustersParsed, serviceParsed, nodes,
            etcd.Members, etcd.Alarms, etcd));
        return Finish(Result.Success(), working: true);
    }

    private async Task<EtcdEndpoint> StatusOfAsync(string endpoint, CancellationToken ct)
    {
        var started = Stopwatch.GetTimestamp();
        var result = await gateway.StatusAsync(endpoint, ct);
        var latencyMs = Stopwatch.GetElapsedTime(started).TotalMilliseconds;
        return result.IsSuccess
            ? new EtcdEndpoint(
                endpoint, true, latencyMs, result.Value.Version, result.Value.DbSizeBytes,
                result.Value.LeaderMemberId, result.Value.RaftIndex, result.Value.RaftTerm, [])
            : new EtcdEndpoint(endpoint, false, null, null, null, null, null, null, [result.Error!.Message]);
    }

    // Failover: один проход по живым endpoints по кругу от активного; смена цели, не повтор
    // (retry внутри тика запрещён arch/02 §4; failover — §3.10).
    private static async Task<Result<T>> WithFailoverAsync<T>(
        EtcdEndpoint[] alive,
        EtcdEndpoint active,
        Func<string, CancellationToken, Task<Result<T>>> call,
        CancellationToken ct)
    {
        var start = Array.IndexOf(alive, active);
        Exception? last = null;
        for (var i = 0; i < alive.Length; i++)
        {
            var endpoint = alive[(start + i) % alive.Length];
            var result = await call(endpoint.Url, ct);
            if (result.IsSuccess)
                return result;
            last = result.Error!;
        }

        return Result<T>.Failed(new EtcdUnreachableException(
            $"все живые endpoints не ответили: {last?.Message}"));
    }

    // Отказ тика: свежий Etcd-статус + прежние данные/BuiltAtUtc (spec §3.9).
    private Result FailTick(
        EtcdSnapshot? previous,
        IReadOnlyList<EtcdEndpoint> statuses,
        DateTimeOffset now,
        string reason)
    {
        var error = Result.Failed(new EtcdUnreachableException(reason));
        var etcd = new EtcdStatus(
            false,
            statuses,
            previous?.Etcd.Members ?? [],
            previous?.Etcd.Alarms ?? [],
            null,
            previous?.Etcd.QuorumSuspected ?? false,
            now,
            (previous?.Etcd.ConsecutiveFailures ?? 0) + 1);
        store.Replace(new EtcdSnapshot(
            previous?.BuiltAtUtc ?? now,
            etcd,
            previous?.Clusters ?? [],
            previous?.HaScopes ?? [],
            previous?.StandNodes ?? [],
            [],
            [],
            previous?.ParseErrors ?? [],
            previous?.UnknownKeyCount ?? 0));
        return Finish(error, working: false);
    }

    private Result Finish(Result status, bool working)
    {
        _inited = true;
        _working = working;
        _statusError = status;
        return status;
    }

    private static bool IsValidEndpoint(string endpoint)
        => Uri.TryCreate(endpoint, UriKind.Absolute, out var uri)
            && uri.Scheme is "http" or "https"
            && !string.IsNullOrEmpty(uri.Host);

    private static bool IsRaftError(string message)
        => message.Contains("raft", StringComparison.OrdinalIgnoreCase)
            || message.Contains("no leader", StringComparison.OrdinalIgnoreCase)
            || message.Contains("quorum", StringComparison.OrdinalIgnoreCase);

    private static class Prefixes
    {
        public const string Clusters = "/clusters/";
        public const string Service = "/service/";
        public const string Nodes = "/cluster/nodes/";
    }
}
```

- [ ] **Step 9.4: FakeEtcdGateway + тесты**

`FixedTimeProvider` — существующий из t02 (`src/tests/AdminPanel.UnitTests/FixedTimeProvider.cs`, public, `Utc` settable) — переиспользуется без изменений.

`src/tests/AdminPanel.UnitTests/SnapshotRefresherTests.cs`:

`src/tests/AdminPanel.UnitTests/SnapshotRefresherTests.cs`:

```csharp
using AdminPanel.Core;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Client;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.UnitTests;

// Общая тест-обвязка refresher'а: FakeEtcdGateway + конструктор с любыми endpoints.
// Используется и EtcdHealthCheckTests (Task 10) — internal на сборку.
internal static class RefresherTestHarness
{
    public static SnapshotRefresher New(FakeEtcdGateway gateway, ISnapshotStore store, params string[] endpoints)
        => new(
            gateway,
            store,
            Options.Create(new EtcdOptions { Endpoints = endpoints }),
            new FixedTimeProvider(),
            NullLogger<SnapshotRefresher>.Instance);
}

// Управляемый gateway: данные/отказы по endpoints, счётчики вызовов.
internal sealed class FakeEtcdGateway : IEtcdGateway
{
    public List<string> StatusFailEndpoints { get; } = [];

    public List<string> RangeFailEndpoints { get; } = [];

    public IReadOnlyList<Kv> ClustersKv { get; init; } = [];

    public IReadOnlyList<Kv> ServiceKv { get; init; } = [];

    public IReadOnlyList<Kv> NodesKv { get; init; } = [];

    public IReadOnlyList<EtcdMember> Members { get; init; } = [];

    public IReadOnlyList<EtcdAlarm> Alarms { get; init; } = [];

    public int RangeCalls { get; private set; }

    public int StatusCalls { get; private set; }

    public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
    {
        RangeCalls++;
        return Task.FromResult(RangeFailEndpoints.Contains(endpoint)
            ? Result<IReadOnlyList<Kv>>.Failed(new EtcdUnreachableException(endpoint))
            : Result<IReadOnlyList<Kv>>.Success(prefix switch
            {
                "/clusters/" => ClustersKv,
                "/service/" => ServiceKv,
                _ => NodesKv,
            }));
    }

    public Task<Result<EtcdStatusPayload>> StatusAsync(string endpoint, CancellationToken ct)
    {
        StatusCalls++;
        return Task.FromResult(StatusFailEndpoints.Contains(endpoint)
            ? Result<EtcdStatusPayload>.Failed(new EtcdUnreachableException(endpoint))
            : Result<EtcdStatusPayload>.Success(new EtcdStatusPayload("3.5.21", 20480, 42, 17, 3)));
    }

    public Task<Result<IReadOnlyList<EtcdMember>>> MemberListAsync(string endpoint, CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyList<EtcdMember>>.Success(Members));

    public Task<Result<IReadOnlyList<EtcdAlarm>>> AlarmAsync(string endpoint, CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyList<EtcdAlarm>>.Success(Alarms));
}

// Refresher: живые/мёртвые endpoints, sticky-failover, отказ с сохранением данных (spec §10.9).
public class SnapshotRefresherTests
{
    private static FakeEtcdGateway DemoGateway() => new()
    {
        ClustersKv = EtcdFixtures.LoadKv("clusters-full.json"),
        ServiceKv = EtcdFixtures.LoadKv("service-full.json"),
        NodesKv = EtcdFixtures.LoadKv("stand-nodes.json"),
        Members = [new EtcdMember(42, "test", ["http://p"], ["http://c"])],
    };

    [Fact]
    public async Task Refresh_AllAlive_BuildsAndStoresSnapshot()
    {
        // Arrange
        var gateway = DemoGateway();
        var store = new SnapshotStore();
        var refresher = RefresherTestHarness.New(gateway, store, "http://e1", "http://e2");

        // Act
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        var snapshot = store.Current.Should().NotBeNull().Subject;
        snapshot.Etcd.Reachable.Should().BeTrue();
        snapshot.Etcd.Endpoints.Should().HaveCount(2);
        snapshot.Etcd.ActiveEndpoint.Should().Be("http://e1"); // sticky: первый по списку
        snapshot.Etcd.ConsecutiveFailures.Should().Be(0);
        snapshot.Clusters.Should().ContainSingle(c => c.Name == "demo");
        snapshot.Etcd.Members.Should().ContainSingle(m => m.Name == "test");
        gateway.StatusCalls.Should().Be(2); // персонально по всем endpoints (arch/02 §2.4)
        refresher.Working.Should().BeTrue();
        refresher.Inited.Should().BeTrue();
    }

    [Fact]
    public async Task Refresh_AllDead_PreservesDataAndCountsFailure()
    {
        // Arrange
        var gateway = DemoGateway();
        var store = new SnapshotStore();
        var refresher = RefresherTestHarness.New(gateway, store, "http://e1", "http://e2");
        await refresher.RefreshOnceAsync(CancellationToken.None);
        var builtAt = store.Current!.BuiltAtUtc;
        var clusters = store.Current.Clusters;
        gateway.StatusFailEndpoints.AddRange(["http://e1", "http://e2"]);

        // Act
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);
        await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        store.Current!.BuiltAtUtc.Should().Be(builtAt);       // возраст данных растёт (spec §3.9)
        store.Current.Clusters.Should().BeSameAs(clusters);   // данные прежние
        store.Current.Etcd.Reachable.Should().BeFalse();
        store.Current.Etcd.ConsecutiveFailures.Should().Be(2);
        store.Current.Etcd.Endpoints.Should().OnlyContain(e => !e.Reachable);
        refresher.Working.Should().BeFalse();
        refresher.StatusError.IsSuccess.Should().BeFalse();
    }

    [Fact]
    public async Task Refresh_Recovery_ResetsFailures()
    {
        // Arrange
        var gateway = DemoGateway();
        var store = new SnapshotStore();
        var refresher = RefresherTestHarness.New(gateway, store, "http://e1");
        await refresher.RefreshOnceAsync(CancellationToken.None);
        gateway.StatusFailEndpoints.Add("http://e1");
        await refresher.RefreshOnceAsync(CancellationToken.None);
        store.Current!.Etcd.ConsecutiveFailures.Should().Be(1);

        // Act — endpoint ожил
        gateway.StatusFailEndpoints.Clear();
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        store.Current.Etcd.ConsecutiveFailures.Should().Be(0);
        store.Current.Etcd.Reachable.Should().BeTrue();
        refresher.Working.Should().BeTrue();
    }

    [Fact]
    public async Task Refresh_StickyFails_OverToNextAlive()
    {
        // Arrange
        var gateway = DemoGateway();
        var store = new SnapshotStore();
        var refresher = RefresherTestHarness.New(gateway, store, "http://e1", "http://e2");
        await refresher.RefreshOnceAsync(CancellationToken.None);
        gateway.StatusFailEndpoints.Add("http://e1"); // активный умер между тиками

        // Act
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        store.Current!.Etcd.ActiveEndpoint.Should().Be("http://e2");
        store.Current.Etcd.Endpoints.Single(e => e.Url == "http://e1").Reachable.Should().BeFalse();
    }

    [Fact]
    public async Task Refresh_MidTickFailure_FailsOverWithoutLosingTick()
    {
        // Arrange — статус жив, но KV-чтения на активном падают: failover внутри тика (spec §3.10)
        var gateway = DemoGateway();
        gateway.RangeFailEndpoints.Add("http://e1");
        var store = new SnapshotStore();
        var refresher = RefresherTestHarness.New(gateway, store, "http://e1", "http://e2");

        // Act
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        store.Current!.Clusters.Should().ContainSingle(c => c.Name == "demo");
    }

    [Fact]
    public async Task Refresh_EmptyEndpoints_FailedTickWithEmptySnapshot()
    {
        // Arrange
        var gateway = DemoGateway();
        var store = new SnapshotStore();
        var refresher = RefresherTestHarness.New(gateway, store);

        // Act
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        store.Current.Should().NotBeNull(); // пустой снапшот с Reachable=false (spec §3.12)
        store.Current!.Etcd.Reachable.Should().BeFalse();
        store.Current.Etcd.ConsecutiveFailures.Should().Be(1);
        store.Current.Clusters.Should().BeEmpty();
        refresher.Inited.Should().BeTrue();
        refresher.Working.Should().BeFalse();
    }
}
```

- [ ] **Step 9.5: Проверка (красный → зелёный) + коммит**

Тесты этого Task пишутся уже вместе с реализацией (реализация велика, TDD-цикл был в Tasks 2–8 для чистых функций); прогон:

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-etcd-snapshot
dotnet build src/AdminPanel.slnx
dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~SnapshotRefresherTests"
git add src/AdminPanel.Etcd/EtcdOptions.cs src/AdminPanel.Etcd/ModuleExtensions.cs src/AdminPanel.Etcd/SnapshotRefresher.cs src/tests/AdminPanel.UnitTests/SnapshotRefresherTests.cs
git commit -m "t03: SnapshotRefresher — тик 3 c, sticky-failover, отказоустойчивость (unit)"
```

Ожидание: build 0 warnings; 6 PASS; коммит.

---

### Task 10: EtcdHealthCheck + композиция Program.cs + appsettings

**Связь со spec:** §7.3 (чек + тег live/Predicate healthz — редакция после ревью Фазы 4: поведение зафиксировано в spec, расхождения spec↔код нет), §8.2–8.3, §10 (unit health-кейсы — часть §10.9-механики).

**Files:**
- Create: `src/AdminPanel.Etcd/EtcdHealthCheck.cs`
- Modify: `src/AdminPanel.Api/Program.cs`
- Modify: `src/AdminPanel.Api/appsettings.json`, `appsettings.Development.json`
- Test: `src/tests/AdminPanel.UnitTests/EtcdHealthCheckTests.cs`

**Interfaces:**
- Consumes: `SnapshotRefresher` (Task 9), `HealthCheckAbstract<T>`/`IHealthCheckService` (t01), `RefresherTestHarness`/`FakeEtcdGateway` (Task 9).
- Produces: `[InjectAsTransient] sealed class EtcdHealthCheck : HealthCheckAbstract<SnapshotRefresher>` — Program.cs регистрирует `.AddCheck<EtcdHealthCheck>("etcd")`.

- [ ] **Step 10.1: Тест (сначала — красный)**

`src/tests/AdminPanel.UnitTests/EtcdHealthCheckTests.cs`:

```csharp
using AdminPanel.Etcd;
using FluentAssertions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Xunit;

namespace AdminPanel.UnitTests;

// EtcdHealthCheck — отражение состояния refresher'а (spec §7.3): старт Degraded, тик ок Healthy, отказ Unhealthy.
public class EtcdHealthCheckTests
{
    [Fact]
    public async Task Check_BeforeFirstTick_Degraded()
    {
        // Arrange
        var refresher = RefresherTestHarness.New(new FakeEtcdGateway(), new SnapshotStore(), "http://e1");
        var check = new EtcdHealthCheck(refresher);

        // Act
        var result = await check.CheckHealthAsync(new HealthCheckContext());

        // Assert
        result.Status.Should().Be(HealthStatus.Degraded); // «service is starting»
    }

    [Fact]
    public async Task Check_AfterSuccessfulTick_Healthy()
    {
        // Arrange
        var gateway = new FakeEtcdGateway
        {
            ClustersKv = EtcdFixtures.LoadKv("clusters-full.json"),
        };
        var refresher = RefresherTestHarness.New(gateway, new SnapshotStore(), "http://e1");
        await refresher.RefreshOnceAsync(CancellationToken.None);
        var check = new EtcdHealthCheck(refresher);

        // Act
        var result = await check.CheckHealthAsync(new HealthCheckContext());

        // Assert
        result.Status.Should().Be(HealthStatus.Healthy);
    }

    [Fact]
    public async Task Check_AfterFailedTick_Unhealthy()
    {
        // Arrange
        var refresher = RefresherTestHarness.New(new FakeEtcdGateway(), new SnapshotStore());
        await refresher.RefreshOnceAsync(CancellationToken.None); // Endpoints пуст → отказ
        var check = new EtcdHealthCheck(refresher);

        // Act
        var result = await check.CheckHealthAsync(new HealthCheckContext());

        // Assert
        result.Status.Should().Be(HealthStatus.Unhealthy);
        result.Exception.Should().NotBeNull();
    }
}
```

- [ ] **Step 10.2: Проверка красного**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-etcd-snapshot
dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~EtcdHealthCheckTests"
```

Ожидание: FAIL — компиляция (`EtcdHealthCheck` не существует).

- [ ] **Step 10.3: Реализация**

`src/AdminPanel.Etcd/EtcdHealthCheck.cs`:

```csharp
using AdminPanel.Infrastructure.DI;
using AdminPanel.Infrastructure.HealthChecks;

namespace AdminPanel.Etcd;

// Чек живости etcd-цикла: без собственной логики — по состоянию refresher (spec §7.3).
// Регистрируется без тега live: /api/healthz — liveness самой панели (arch/03 §1).
// HealthCheckAbstract<T> имеет primary-конструктор T service — наследник обязан пробросить аргумент.
[InjectAsTransient]
public sealed class EtcdHealthCheck(SnapshotRefresher service)
    : HealthCheckAbstract<SnapshotRefresher>(service)
{
}
```

- [ ] **Step 10.4: Program.cs + appsettings**

`src/AdminPanel.Api/Program.cs` — два изменения.

1) Цепочка health-checks (после строки `.AddCheck("self", …)`):

```csharp
   .AddCheck("self", () => HealthCheckResult.Healthy(), ["live"])
   .AddCheck<EtcdHealthCheck>("etcd") // [t03] чек refresher'а; без тега live — healthz не роняет (arch/03 §1)
```

2) Маппинг healthz — добавить `Predicate` (фильтр liveness-чеков):

```csharp
// Живость самой панели (liveness, arch/03 §1): только чеки с тегом live.
// Чек etcd (readiness-семантика) не роняет /api/healthz — его статус отдают t04+ эндпоинты.
app.MapHealthChecks(
    "/api/healthz",
    new HealthCheckOptions
    {
        Predicate = registration => registration.Tags.Contains("live"),
        ResponseWriter = HealthzWriter.WriteStatus,
    });
```

`src/AdminPanel.Api/appsettings.json` — секция `AdminPanel` целиком:

```json
  "AdminPanel": {
    "Auth": {
      "Username": "admin"
    },
    "Etcd": {
      "Endpoints": [],
      "RefreshIntervalSeconds": 3,
      "RequestTimeoutSeconds": 2
    }
  }
```

`src/AdminPanel.Api/appsettings.Development.json` — секция `AdminPanel` целиком:

```json
  "AdminPanel": {
    "Auth": {
      "Username": "admin",
      "Password": "admin",
      "AllowHttp": true
    },
    "Etcd": {
      "Endpoints": [ "http://localhost:2379" ]
    }
  }
```

- [ ] **Step 10.5: Проверка зелёного + регресс auth/healthz + smoke хоста**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-etcd-snapshot
dotnet build src/AdminPanel.slnx
dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~AdminPanel.UnitTests"
```

Ожидание: все unit PASS (включая 3 новых и прежние `HealthzTests`-независимые). Integration-смоук хоста (без Docker: Development endpoints `localhost:2379` недоступны → тики refresher'а неуспешны, hosted-сервис стартует и живёт, healthz остаётся liveness):

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-etcd-snapshot
ASPNETCORE_ENVIRONMENT=Development ASPNETCORE_URLS=http://127.0.0.1:5099 dotnet run --no-launch-profile --project src/AdminPanel.Api &
sleep 8
curl -s http://127.0.0.1:5099/api/healthz; echo
kill %1
```

Ожидание: `{"status":"ok"}` (200) — healthz не пострадал от etcd-чека; хост поднялся с hosted-refresher'ом (в Development Endpoints задан, Docker-etcd нет — тики молча неуспешны, счётчик растёт внутри снапшота; warning «Endpoints не задан» в этом сценарии НЕ появляется — он только для пустого списка). Затем integration-регресс (коллекция `"api"` — WAF поднимает Program с hosted-refresher на пустых Endpoints из `appsettings.json` Production):

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-etcd-snapshot
dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~AdminPanel.IntegrationTests"
```

Ожидание: `HealthzTests`/`AuthTests` PASS (json-конфиг фабрики — Production, `Endpoints: []`, чек etcd Unhealthy, но healthz фильтруется по тегу `live`).

- [ ] **Step 10.6: Коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-etcd-snapshot
git add src/AdminPanel.Etcd/EtcdHealthCheck.cs src/AdminPanel.Api/Program.cs src/AdminPanel.Api/appsettings.json src/AdminPanel.Api/appsettings.Development.json src/tests/AdminPanel.UnitTests/EtcdHealthCheckTests.cs
git commit -m "t03: EtcdHealthCheck + композиция Program.cs — чек etcd, healthz остаётся liveness (unit)"
```

---

### Task 11: Integration — Testcontainers etcd + сид + тесты

**Связь со spec:** §11 (fixture, сид, тесты), §3.16 (сид = arch/04 §2.2), §1 (пакет Testcontainers 4.14.0, generic-контейнер), §3.15 (без WAF/attribute-DI), §14 (csproj).

**Files:**
- Modify: `src/tests/AdminPanel.IntegrationTests/AdminPanel.IntegrationTests.csproj`
- Create: `src/tests/AdminPanel.IntegrationTests/EtcdContainerFixture.cs`
- Create: `src/tests/AdminPanel.IntegrationTests/EtcdSeed.cs`
- Test: `src/tests/AdminPanel.IntegrationTests/EtcdSnapshotIntegrationTests.cs` (включая `EtcdFailureTests`)

**Interfaces:**
- Consumes: `EtcdGateway`, `SnapshotRefresher`, `SnapshotStore`, `EtcdHealthCheck`, `EtcdOptions`, фикстуры-значения demo.
- Produces: `EtcdContainerFixture` (public `string Endpoint`, `Task StopAsync()`), `EtcdSeed.Demo`/`SeedAsync`/`PutAsync` — переиспользуются t04/t05 интеграционными тестами (тот же контейнер-паттерн).

- [ ] **Step 11.1: csproj**

В `src/tests/AdminPanel.IntegrationTests/AdminPanel.IntegrationTests.csproj` к существующим `PackageReference` добавить:

```xml
        <PackageReference Include="Testcontainers"/>
```

и к `ProjectReference`:

```xml
        <ProjectReference Include="..\..\AdminPanel.Core\AdminPanel.Core.csproj"/>
        <ProjectReference Include="..\..\AdminPanel.Etcd\AdminPanel.Etcd.csproj"/>
```

- [ ] **Step 11.2: Сид (значения = фикстурам = будущему seed.sh t10)**

`src/tests/AdminPanel.IntegrationTests/EtcdSeed.cs`:

```csharp
using System.Text;
using System.Text.Json;

namespace AdminPanel.IntegrationTests;

// Сид контроль-плейна demo (arch/04 §2.2) — те же значения, что в EtcdFixtures/*.json;
// скрипт seed.sh dev-станда (t10) обязан использовать те же (spec §3.16).
public static class EtcdSeed
{
    public static readonly IReadOnlyList<(string Key, string Value)> Demo =
    [
        ("/clusters/demo/config", "{\"buckets\":16,\"dbname\":\"demo\",\"created_unix\":1755800000}"),
        ("/clusters/demo/shards/s1/dsn", "host=s1a,s1b port=5432 dbname=demo user=postgres"),
        ("/clusters/demo/shards/s1/replicas", "1"),
        ("/clusters/demo/shards/s1/master", "s1a:5432"),
        ("/clusters/demo/shards/s2/dsn", "host=s2a,s2b port=5432 dbname=demo user=postgres"),
        ("/clusters/demo/shards/s2/replicas", "1"),
        ("/clusters/demo/shards/s2/master", "s2a:5432"),
        ("/clusters/demo/buckets/routing/bucket_0", "s1"),
        ("/clusters/demo/buckets/routing/bucket_1", "s2"),
        ("/clusters/demo/buckets/routing/bucket_2", "s1"),
        ("/clusters/demo/buckets/routing/bucket_3", "s1"),
        ("/clusters/demo/buckets/routing/bucket_4", "s1"),
        ("/clusters/demo/buckets/routing/bucket_5", "s2"),
        ("/clusters/demo/buckets/routing/bucket_6", "s1"),
        ("/clusters/demo/buckets/routing/bucket_7", "s2"),
        ("/clusters/demo/buckets/routing/bucket_8", "s1"),
        ("/clusters/demo/buckets/routing/bucket_9", "s2"),
        ("/clusters/demo/buckets/routing/bucket_10", "s1"),
        ("/clusters/demo/buckets/routing/bucket_11", "s1"),
        ("/clusters/demo/buckets/routing/bucket_12", "s1"),
        ("/clusters/demo/buckets/routing/bucket_13", "s2"),
        ("/clusters/demo/buckets/routing/bucket_14", "s1"),
        ("/clusters/demo/buckets/routing/bucket_15", "s2"),
        ("/clusters/demo/buckets/status/bucket_3", "{\"bucket\":\"bucket_3\",\"state\":\"SYNCING\",\"owner\":\"s1\",\"target\":\"s2\",\"started_unix\":1755900000,\"updated_unix\":1755900600,\"phase\":\"copy\"}"),
        ("/clusters/demo/buckets/status/bucket_7", "{\"bucket\":\"bucket_7\",\"state\":\"ABORTING\",\"owner\":\"s2\",\"target\":\"s1\",\"started_unix\":1755800000,\"updated_unix\":1755800500,\"phase\":\"cleanup\",\"last_error\":\"receiver went away\"}"),
        ("/clusters/demo/buckets/status/bucket_11", "{\"bucket\":\"bucket_11\",\"state\":\"FROZEN\",\"owner\":\"s1\",\"target\":\"s2\",\"started_unix\":1755700000,\"updated_unix\":1755700200,\"phase\":\"cutover-wait\"}"),
        ("/clusters/demo/heals/bucket_5", "{\"bucket\":\"bucket_5\",\"was\":\"s2\",\"now\":\"s1\",\"reason\":\"restore-heal\",\"ts\":1755600000}"),
        ("/service/demo-s1/leader", "{\"name\":\"s1a\"}"),
        ("/service/demo-s1/members/s1a", "{\"name\":\"s1a\",\"conn_url\":\"s1a:5432\",\"role\":\"master\",\"state\":\"running\",\"timeline\":1,\"lag\":0}"),
        ("/service/demo-s1/members/s1b", "{\"name\":\"s1b\",\"conn_url\":\"s1b:5432\",\"role\":\"replica\",\"state\":\"streaming\",\"timeline\":1,\"lag\":0}"),
        ("/service/demo-s1/optime/leader", "738273634528"),
        ("/service/demo-s1/initialize", "738273612345678"),
        ("/service/demo-s1/config", "{\"ttl\":5,\"loop_wait\":2,\"retry_timeout\":3}"),
        ("/service/demo-s2/leader", "{\"name\":\"s2a\"}"),
        ("/service/demo-s2/members/s2a", "{\"name\":\"s2a\",\"conn_url\":\"s2a:5432\",\"role\":\"master\",\"state\":\"running\",\"timeline\":1,\"lag\":0}"),
        ("/service/demo-s2/members/s2b", "{\"name\":\"s2b\",\"conn_url\":\"s2b:5432\",\"role\":\"replica\",\"state\":\"streaming\",\"timeline\":1,\"lag\":0}"),
        ("/service/demo-s2/optime/leader", "738273634001"),
        ("/service/demo-s2/initialize", "738273611234567"),
        ("/service/demo-s2/config", "{\"ttl\":5,\"loop_wait\":2,\"retry_timeout\":3}"),
        ("/cluster/nodes/s1a", "172.28.0.11"),
        ("/cluster/nodes/s1b", "172.28.0.12"),
        ("/cluster/nodes/s2a", "172.28.0.21"),
        ("/cluster/nodes/s2b", "172.28.0.22"),
    ];

    // Запись одного ключа тем же транспортом, что читает панель (kv/put; тест — не панель).
    public static async Task PutAsync(string endpoint, string key, string value, CancellationToken ct)
    {
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(3) };
        var body = JsonSerializer.Serialize(new
        {
            key = Convert.ToBase64String(Encoding.UTF8.GetBytes(key)),
            value = Convert.ToBase64String(Encoding.UTF8.GetBytes(value)),
        });
        using var response = await http.PostAsync(
            endpoint + "/v3/kv/put",
            new StringContent(body, Encoding.UTF8, "application/json"),
            ct);
        response.EnsureSuccessStatusCode();
    }

    public static async Task SeedAsync(string endpoint, CancellationToken ct)
    {
        foreach (var (key, value) in Demo)
            await PutAsync(endpoint, key, value, ct);
    }
}
```

- [ ] **Step 11.3: Fixture (generic-контейнер etcd)**

`src/tests/AdminPanel.IntegrationTests/EtcdContainerFixture.cs`:

```csharp
using System.Net.Http.Json;
using System.Text;
using Testcontainers;                 // ContainerBuilder (если 4.x требует Testcontainers.Builders — см. примечание)
using Xunit;

namespace AdminPanel.IntegrationTests;

// Testcontainers-etcd: generic-контейнер quay.io/coreos/etcd:v3.5.21 (spec §11.1; готовый
// .NET-модуль etcd на NuGet отсутствует). Gateway /v3/* включён в 3.5 по умолчанию.
// Готовность — свой POST-ретрай: встроенные HTTP-wait шлют GET, а /v3/* требует POST.
public sealed class EtcdContainerFixture : IAsyncLifetime
{
    private readonly IContainer _container = new ContainerBuilder()
        .WithImage("quay.io/coreos/etcd:v3.5.21")
        .WithCommand(
            "etcd",
            "--name=test",
            "--data-dir=/etcd-data",
            "--listen-client-urls=http://0.0.0.0:2379",
            "--advertise-client-urls=http://127.0.0.1:2379")
        .WithPortBinding(2379, assignRandomHostPort: true)
        .Build();

    public string Endpoint { get; private set; } = "";

    public async ValueTask InitializeAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        await _container.StartAsync(ct);
        Endpoint = $"http://localhost:{_container.GetMappedPublicPort(2379)}";

        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(3) };
        for (var i = 0; i < 30; i++)
        {
            try
            {
                using var probe = await http.PostAsync(
                    Endpoint + "/v3/maintenance/status",
                    new StringContent("{}", Encoding.UTF8, "application/json"),
                    ct);
                if (probe.IsSuccessStatusCode)
                {
                    await EtcdSeed.SeedAsync(Endpoint, ct);
                    return;
                }
            }
            catch (HttpRequestException)
            {
                // etcd ещё поднимается — ждём следующую попытку
            }

            await Task.Delay(1000, ct);
        }

        throw new InvalidOperationException($"etcd в {Endpoint} не поднялся за 30 c");
    }

    public async ValueTask DisposeAsync() => await _container.DisposeAsync();

    // Тест отказа etcd: тик должен сохранить прежний снапшот (spec §11.2).
    public async Task StopAsync() => await _container.StopAsync();
}
```

Примечание к using'ам Testcontainers 4.x: если компилятор не найдёт `ContainerBuilder`/`IContainer` в `Testcontainers`, заменить на `using Testcontainers.Builders;` + `using Testcontainers.Containers;` (переименование root-namespace в 4.0; компилятор подскажет точные имена). `ContainerBuilder`/`WithPortBinding(2379, true)`/`GetMappedPublicPort` — стабильное API 3.x–4.x.

- [ ] **Step 11.4: Интеграционные тесты**

`src/tests/AdminPanel.IntegrationTests/EtcdSnapshotIntegrationTests.cs`:

```csharp
using AdminPanel.Core;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Client;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.IntegrationTests;

// Конструирование модуля напрямую (без attribute-DI/WAF): статический кеш сборок
// должен остаться чистым для Program-хостов t04+ (spec §3.15).
public static class EtcdTestHarness
{
    private sealed class RealTimeProvider : TimeProvider
    {
    }

    public static EtcdGateway NewGateway()
        => new(new HttpClient { Timeout = TimeSpan.FromSeconds(2) });

    public static SnapshotRefresher NewRefresher(ISnapshotStore store, params string[] endpoints)
        => new(
            NewGateway(),
            store,
            Options.Create(new EtcdOptions { Endpoints = endpoints }),
            new RealTimeProvider(),
            NullLogger<SnapshotRefresher>.Instance);
}

// Gateway + refresher против живого etcd с сидом demo (spec §11.2).
public class EtcdSnapshotIntegrationTests(EtcdContainerFixture fixture) : IClassFixture<EtcdContainerFixture>
{
    [Fact]
    public async Task Gateway_Status_AgainstRealEtcd()
    {
        // Arrange
        var gateway = EtcdTestHarness.NewGateway();

        // Act
        var result = await gateway.StatusAsync(fixture.Endpoint, CancellationToken.None);

        // Assert — подтверждает фактические имена полей gateway (spec §3.17)
        result.IsSuccess.Should().BeTrue();
        result.Value.Version.Should().Be("3.5.21");
        result.Value.LeaderMemberId.Should().BeGreaterThan(0);
        result.Value.RaftTerm.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task Gateway_MemberList_SingleMember()
    {
        // Arrange
        var gateway = EtcdTestHarness.NewGateway();

        // Act
        var result = await gateway.MemberListAsync(fixture.Endpoint, CancellationToken.None);

        // Assert
        var member = result.Value.Should().ContainSingle().Subject;
        member.Name.Should().Be("test");
        member.ClientUrls.Should().NotBeEmpty();
    }

    [Fact]
    public async Task Gateway_Alarm_Empty()
    {
        // Arrange
        var gateway = EtcdTestHarness.NewGateway();

        // Act
        var result = await gateway.AlarmAsync(fixture.Endpoint, CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().BeEmpty();
    }

    [Fact]
    public async Task Gateway_Range_ClustersPrefix_ReturnsSeededKvs()
    {
        // Arrange
        var gateway = EtcdTestHarness.NewGateway();

        // Act
        var result = await gateway.RangeAsync(fixture.Endpoint, "/clusters/", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Contain(kv => kv.Key == "/clusters/demo/config");
    }

    [Fact]
    public async Task Refresher_RefreshOnce_BuildsExpectedSnapshot()
    {
        // Arrange
        var store = new SnapshotStore();
        var refresher = EtcdTestHarness.NewRefresher(store, fixture.Endpoint);

        // Act
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        var snapshot = store.Current.Should().NotBeNull().Subject;
        snapshot.Etcd.Reachable.Should().BeTrue();
        snapshot.Etcd.ActiveEndpoint.Should().Be(fixture.Endpoint);
        snapshot.Etcd.ConsecutiveFailures.Should().Be(0);
        snapshot.Etcd.QuorumSuspected.Should().BeFalse(); // одиночный etcd: leader валиден
        var demo = snapshot.Clusters.Should().ContainSingle(c => c.Name == "demo").Subject;
        demo.DbName.Should().Be("demo");
        demo.BucketsCount.Should().Be(16);
        demo.Buckets.Should().HaveCount(16);
        demo.Shards.Should().Contain(s => s.Name == "s1" && s.MasterAddress == "s1a:5432");
        demo.Buckets.Single(b => b.Id == 3).State.Should().Be(BucketState.Syncing);
        demo.Buckets.Single(b => b.Id == 7).State.Should().Be(BucketState.Aborting);
        demo.Buckets.Single(b => b.Id == 11).State.Should().Be(BucketState.Frozen);
        demo.Buckets.Single(b => b.Id == 0).State.Should().Be(BucketState.Active);
        demo.Heals.Should().ContainSingle(h => h.Bucket == "bucket_5");
        var scope = snapshot.HaScopes.Should().ContainSingle(s => s.Scope == "demo-s1").Subject;
        scope.Matched.Should().BeTrue();
        scope.LeaderName.Should().Be("s1a");
        scope.Members.Should().HaveCount(2);
        snapshot.StandNodes.Should().HaveCount(4);
        snapshot.Etcd.Members.Should().ContainSingle(m => m.Name == "test");
        snapshot.Alerts.Should().BeEmpty();
        snapshot.Probes.Should().BeEmpty();
    }

    [Fact]
    public async Task Refresher_SecondTick_PicksUpChanges()
    {
        // Arrange
        var store = new SnapshotStore();
        var refresher = EtcdTestHarness.NewRefresher(store, fixture.Endpoint);
        await refresher.RefreshOnceAsync(CancellationToken.None);

        // Act — перевладение routing bucket_0 шарду s2
        await EtcdSeed.PutAsync(fixture.Endpoint, "/clusters/demo/buckets/routing/bucket_0", "s2", CancellationToken.None);
        await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert
        store.Current!.Clusters.Single().Buckets.Single(b => b.Id == 0).Owner.Should().Be("s2");
    }

    [Fact]
    public async Task Refresher_Failover_DeadFirstEndpoint()
    {
        // Arrange — localhost:1: connection refused мгновенен
        var store = new SnapshotStore();
        var refresher = EtcdTestHarness.NewRefresher(store, "http://localhost:1", fixture.Endpoint);

        // Act
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        store.Current!.Etcd.ActiveEndpoint.Should().Be(fixture.Endpoint);
        store.Current.Etcd.Endpoints.Should().HaveCount(2);
        store.Current.Etcd.Endpoints[0].Reachable.Should().BeFalse();
    }

    [Fact]
    public async Task HealthCheck_ReflectsRefresherState()
    {
        // Arrange
        var store = new SnapshotStore();
        var refresher = EtcdTestHarness.NewRefresher(store, fixture.Endpoint);
        var check = new EtcdHealthCheck(refresher);
        await refresher.RefreshOnceAsync(CancellationToken.None);

        // Act
        var result = await check.CheckHealthAsync(new Microsoft.Extensions.Diagnostics.HealthChecks.HealthCheckContext());

        // Assert
        result.Status.Should().Be(Microsoft.Extensions.Diagnostics.HealthChecks.HealthStatus.Healthy);
    }
}

// Сценарий отказа etcd: отдельный класс со СВОИМ контейнером — StopAsync ломает fixture,
// порядок тестов внутри коллекции не гарантирован бы (spec §11.1).
// Включает вторую половину HealthCheck-сценария §11.2: Unhealthy после остановки etcd.
public class EtcdFailureTests(EtcdContainerFixture fixture) : IClassFixture<EtcdContainerFixture>
{
    [Fact]
    public async Task Refresher_EtcdStopped_KeepsPreviousSnapshot()
    {
        // Arrange
        var store = new SnapshotStore();
        var refresher = EtcdTestHarness.NewRefresher(store, fixture.Endpoint);
        await refresher.RefreshOnceAsync(CancellationToken.None);
        var builtAt = store.Current!.BuiltAtUtc;
        var clusters = store.Current.Clusters;
        await fixture.StopAsync();

        // Act
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);
        await refresher.RefreshOnceAsync(CancellationToken.None);
        var health = await new EtcdHealthCheck(refresher)
            .CheckHealthAsync(new Microsoft.Extensions.Diagnostics.HealthChecks.HealthCheckContext());

        // Assert
        result.IsSuccess.Should().BeFalse();
        store.Current!.BuiltAtUtc.Should().Be(builtAt);     // данные прежние, возраст растёт (spec §3.9)
        store.Current.Clusters.Should().BeSameAs(clusters);
        store.Current.Etcd.Reachable.Should().BeFalse();
        store.Current.Etcd.ConsecutiveFailures.Should().Be(2);
        health.Status.Should().Be(Microsoft.Extensions.Diagnostics.HealthChecks.HealthStatus.Unhealthy);
    }
}
```

- [ ] **Step 11.5: Проверка (Docker обязателен)**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-etcd-snapshot
docker info >/dev/null && echo "docker ok"
dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~EtcdSnapshotIntegrationTests|FullyQualifiedName~EtcdFailureTests"
```

Ожидание: 9 PASS (8 + 1 failure-класс). Если `Gateway_Status_AgainstRealEtcd` падает на чтении полей — фактические имена полей ответа отличаются от fixture-предположения: сравнить тело реального ответа (`curl -s -X POST .../v3/maintenance/status -d '{}'`) с DTO `EtcdGateway` и поправить `[JsonPropertyName]` — контракт модели не меняется (spec §17).

- [ ] **Step 11.6: Коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-etcd-snapshot
git add src/tests/AdminPanel.IntegrationTests/
git commit -m "t03: integration — Testcontainers etcd, сид demo, снапшот/отказ/failover end-to-end"
```

---

### Task 12: Полный прогон + критерии spec §16 + финальный коммит

**Связь со spec:** §15 (roadmap-деливерабл), §16 (критерии приёмки), §12 (ограничения).

**Files:**
- Modify: `git index` — коммит `docs/superpowers/2026-08-22-t03-etcd-snapshot/` (spec + plan) + `arch/roadmap/etcd.md` (правка Фазы 1).

- [ ] **Step 12.1: Полная сборка и все тесты**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-etcd-snapshot
dotnet build src/AdminPanel.slnx
dotnet test src/AdminPanel.slnx
```

Ожидание: build — 0 warnings; тесты — все зелёные (unit + integration; Docker запущен).

- [ ] **Step 12.2: Критерии spec §16 (grep-анкеры)**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-etcd-snapshot
# §16.6: пакеты — только ожидаемые строки
grep -rn "PackageReference" src --include="*.csproj" | sort
# Ожидание: Infrastructure 4 строки (t01), Etcd 2 новых (Hosting.Abstractions, Http) + ProjectReference,
# UnitTests — прежние + без новых PackageReference, IntegrationTests — прежние + Testcontainers, Api — прежние.

# §16.7: панель шлёт в etcd только /v3/{kv/range, maintenance/status, cluster/member/list, maintenance/alarm}
grep -rn '"/v3/' src/AdminPanel.Etcd/
# Ожидание: ровно 4 пути в EtcdGateway; PutAsync/lease живут только в src/tests/.

grep -rn '"/v3/' src/tests/
# Ожидание: только /v3/kv/put (сид EtcdSeed) и /v3/maintenance/status (wait-ретрай fixture) — это тест, не панель.

# §16.8: roadmap-деливерабл (правка Фазы 1 в рабочем дереве)
grep -n "t03-etcd-snapshot" arch/roadmap/etcd.md
# Ожидание: ровно 1 совпадение — внутри строки t04-etcd-api (зависимость сохранена).
grep -n '^- `t03' arch/roadmap/etcd.md
# Ожидание: пусто (пункт-строка t03 удалена).

# §16.4: секретов в appsettings.json нет
grep -n "Password" src/AdminPanel.Api/appsettings.json
# Ожидание: пусто.

# §16.8: других мутаций arch/ нет
git status --short arch/
# Ожидание: только " M arch/roadmap/etcd.md".
```

- [ ] **Step 12.3: Финальный коммит (docs + roadmap — прецедент t02 «roadmap-деливерабл + spec/plan»)**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-etcd-snapshot
git add docs/superpowers/2026-08-22-t03-etcd-snapshot/ arch/roadmap/etcd.md
git commit -m "t03: spec/plan задачи + roadmap-деливерабл (удаление пункта t03-etcd-snapshot)"
git log --oneline -3
git status --short
```

Ожидание: коммит создан; рабочее дерево чистое; `git log` показывает серию `t03: …` поверх `8970d16` (merge t02).

- [ ] **Step 12.4: Ручная сверка критериев, не покрываемых автотестами**

- §16.5: smoke хоста выполнен в Task 10 Step 10.5 (healthz liveness 200 при недоступном etcd); полноценный стенд-смоук с живыми данными — не выполним до t10 (dev-стенда ещё нет), фиксируется здесь как примечание ревью, не как блокер.
- §16.9: решения spec §3 не противоречат arch/01 §1/§6/§8, arch/02, arch/03 §2 — сверка на ревью (этот план — их реализация без отклонений; Predicate healthz согласован правкой spec §7.3/§8.3/§16.5 по ревью Фазы 4).

---

## Самопроверка плана (выполнена автором)

1. **Spec coverage:** модель §5 → Task 1; ScopeMatcher §5 → Task 2; DsnParser §6.4 → Task 3; ClustersParser §6.1 → Task 4; ServiceParser §6.2 → Task 5; StandNodesParser §6.3 → Task 6; gateway §4 → Task 7; Store/Builder §6.5/§7.1 → Task 8; Refresher/Options/ModuleExtensions §7.2/§8.1/§4.2 → Task 9; health-check/Program/appsettings §7.3/§8.2–8.3 → Task 10; integration §11 → Task 11; пакеты §13/§14 → Tasks 1/11; roadmap §15 + критерии §16 → Task 12. Пропусков нет.
2. **Placeholder scan:** «TBD»/«TODO»/«реализовать позже» — нет; единственные параметрические места (версии CPM 10.0.x, namespace Testcontainers 4.x) снабжены конкретными командами проверки и конкретными альтернативами.
3. **Type consistency:** `Kv` создаётся в Task 4 (Client/Kv.cs) и используется Tasks 5–9 без переопределения; `EtcdStatusPayload`/`IEtcdGateway` (Task 7) ↔ FakeEtcdGateway/RealTime (Tasks 9/11); `SnapshotBuilder.Build`-сигнатура (Task 8) ↔ вызов в Task 9; `EtcdHealthCheck(SnapshotRefresher)` (Task 10) ↔ `HealthCheckAbstract<T>(T)` t01 с явным пробросом base-аргумента; `RefresherTestHarness`/`FakeEtcdGateway` объявлены `internal` в Task 9 и переиспользуются Task 10; `FixedTimeProvider` — существующий из t02, переиспользуется Tasks 8–9 без дубликатов. Сверено.
4. **Прогон листингов (грабли, закрытые при вычитке):** самоприсваивание `move = move` (CS1717 под `TreatWarningsAsErrors`) — устранено инверсией `if`; `new()`-constraint против классов с primary-конструкторами — GetOrAdd переведён на фабрику; наследование `HealthCheckAbstract<T>` — явный base-вызов; тернарник с collection expression — явная типизация `EtcdEndpoint[]`; sync-over-async в FakeHandler — async-override; definite assignment `out var parsed` — упрощённая вложенность; out-of-range routing `bucket_<N≥buckets>` остаётся в `Buckets` (Union диапазона и фактических ключей) — иначе алерт t04 терял бы данные (дыра spec §3.7, закрыта в пользу P18-детекта).
5. **Правки ревью Фазы 4 (внесены):** base64-константы теста `Range_Prefix` выверены утилитой (`L2NsdXN0ZXJzLw==`/`L2NsdXN0ZXJzMA==`); 503-ответ в `HttpError_ReturnsFailed` получил явный `Content`, `PostAsync` читает тело null-safe; `using AdminPanel.Etcd.Client` убран из Task 3 (namespace появляется в Task 4); `FixedTimeProvider` — существующий из t02, локальные дубликаты не создаются; `ParseHeal` берёт имя бакета из поля `bucket` с fallback на суффикс ключа (spec §6.1, покрыто фикстурой+ассертом); интеграционный `HealthCheck_ReflectsRefresherState` дополнен Unhealthy-половиной в `EtcdFailureTests`; Predicate healthz согласован правкой spec §7.3/§8.3/§16.5.
