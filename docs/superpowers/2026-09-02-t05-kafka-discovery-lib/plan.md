# t05 — HA.Kafka (дискавери kafka из etcd) + общий etcd-слой HA.Etcd: план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** В репозитории Puzzle — рефакторинг-вынос etcd-транспорта из HA.Db в общий `PuzzleServer.Infrastructure.App.HA.Etcd` и новая библиотека дискавери kafka-кластеров `PuzzleServer.Infrastructure.App.HA.Kafka` (кэш-снапшот, watch-long-poll/poll, событие `Updated`, fail-open, health).

**Architecture:** Механика 1:1 из HA.Db (шина bootstrap→сигналы→рефетч, сигнальщики по Mode, толерантный парсинг), но транспорт (`IEtcdClient`/ротация/watch/members-монитор) — один общий проект HA.Etcd, не зависящий от опций потребителей. HA.Kafka читает один префикс `/kafka/clusters/<C>/` (контракт pg/arch/15 §5–§6), кластеры заявляются флуент-вызовами при регистрации.

**Tech Stack:** .NET 10, C# (`LangVersion=latest`, `Nullable=enable`), xunit v3 + FluentAssertions, Testcontainers (etcd `quay.io/coreos/etcd:v3.5`). Централизованные версии пакетов (`src/Directory.Packages.props`), новых внешних пакетов нет.

**Spec:** `docs/superpowers/2026-09-02-t05-kafka-discovery-lib/spec.md` (в worktree pg `/Users/demakaev/ZCodeProject/worktrees/feat-t05-kafka-discovery-lib`; далее «спека §N»).

## Global Constraints

- **Два репозитория**: код — в `/Users/demakaev/ZCodeProject/Puzzle` (git-репозиторий, ветка `feat-t05-kafka-discovery-lib`, коммиты по его AGENTS.md: feature-ветки — свободно); spec/plan/roadmap-артефакты — в worktree pg `/Users/demakaev/ZCodeProject/worktrees/feat-t05-kafka-discovery-lib`. Каждый таск ниже помечен `[repo: …]`.
- Идентификаторы — английские; комментарии/документация — русские. Тесты — с комментариями `// Arrange` / `// Act` / `// Assert`.
- `TreatWarningsAsErrors`-режим: сборка без warnings (0 warnings как критерий).
- Никаких `throw` через границы модулей — `Result`/`Result<T>` из `PuzzleServer.Infrastructure.App`.
- Никаких новых внешних пакетов; версии — только centrally в `src/Directory.Packages.props`.
- Модуль HA.Kafka НЕ подключается в `Api/Program.cs` (спека §4.8; интеграция — roadmap `t10`).
- Команды сборки/тестов Puzzle — из корня `/Users/demakaev/ZCodeProject/Puzzle`:
  - `dotnet build src/PuzzleServer.Api.slnx` (0 warnings);
  - unit без Docker: `dotnet test src/PuzzleServer.Api.slnx --filter "FullyQualifiedName~PuzzleServer.UnitTests"`;
  - полные (нужен Docker): `dotnet test src/PuzzleServer.Api.slnx`.

---

### Task 0: Подготовка веток и коммит артефактов фаз

**[repo: Puzzle + repo: pg-worktree]**

**Files:**
- pg-worktree: без новых (уже есть `docs/superpowers/2026-09-02-t05-kafka-discovery-lib/spec.md`, `plan.md`, правка `arch/roadmap/kafkaworker.md`).

- [ ] **Step 1: В репозитории Puzzle создать feature-ветку от main**

```bash
cd /Users/demakaev/ZCodeProject/Puzzle
git switch main && git pull --ff-only || true
git switch -c feat-t05-kafka-discovery-lib
```

Ожидание: ветка создана, рабочее дерево чистое (`git status --short` пусто).

- [ ] **Step 2: В worktree pg закоммитировать артефакты фаз (feature-ветка — коммитить свободно)**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t05-kafka-discovery-lib
git add arch/roadmap/kafkaworker.md docs/superpowers/2026-09-02-t05-kafka-discovery-lib/
git commit -m "t05: spec + plan (HA.Kafka discovery lib, HA.Etcd refactor) + roadmap t10"
```

---

### Task 1: HA.Etcd — вынос общего etcd-слоя из HA.Db (спека §4.1, Фаза Ф1)

**[repo: Puzzle]**

**Files:**
- Create: `src/PuzzleServer.Infrastructure.App.HA.Etcd/PuzzleServer.Infrastructure.App.HA.Etcd.csproj`
- Move (git mv из `src/PuzzleServer.Infrastructure.App.HA.Db/Etcd/`): `IEtcdClient.cs`, `EtcdHttpClient.cs`, `EtcdKv.cs`, `EtcdWatchEvent.cs`, `EtcdHttpException.cs`, `EtcdMember.cs`, `StreamJsonObjectsReader.cs`, `EtcdEndpointRotation.cs`, `EtcdMembersMonitor.cs`
- Create: `src/PuzzleServer.Infrastructure.App.HA.Etcd/EtcdMembersMonitorOptions.cs`
- Modify: `src/PuzzleServer.Infrastructure.App.HA.Db/PuzzleServer.Infrastructure.App.HA.Db.csproj` (+ProjectReference), `src/PuzzleServer.Infrastructure.App.HA.Db/ModuleExtensions.cs`, `src/PuzzleServer.Infrastructure.App.HA.Db/TopologyStore.cs`, `src/PuzzleServer.Infrastructure.App.HA.Db/Refresh/WatchLongPollSignaler.cs`
- Modify: `src/PuzzleServer.Api.slnx` (папка `/Infrastructure/`)
- Test (move): `src/PuzzleServer.UnitTests/HA/Db/{EtcdHttpClientRangeTests,EtcdHttpClientWatchTests,EtcdHttpClientMembersTests,EtcdEndpointRotationTests,EtcdMembersMonitorTests,StreamJsonObjectsReaderTests}.cs` → `src/PuzzleServer.UnitTests/HA/Etcd/`

**Interfaces (produces, используются всеми последующими задачами):**
```csharp
namespace PuzzleServer.Infrastructure.App.HA.Etcd;

public interface IEtcdClient
{
    Task<Result<IReadOnlyList<EtcdKv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct);
    Task<Result<IReadOnlyList<EtcdMember>>> MembersAsync(string endpoint, CancellationToken ct);
    IAsyncEnumerable<EtcdWatchEvent> WatchAsync(string endpoint, string prefix, long? startRevision, CancellationToken ct);
}
public sealed record EtcdKv(string Key, string Value, long ModRevision);
public sealed record EtcdWatchEvent(WatchEventType Type, string? Key, long Revision);
public enum WatchEventType { Put, Delete, Compacted }
public sealed class EtcdHttpClient(HttpClient httpClient, int requestTimeoutMs) : IEtcdClient { … }
public sealed class EtcdEndpointRotation(IReadOnlyList<string> endpoints)
{
    public string GetActive();
    public void ReportFailure(string endpoint);
    public void ReportSuccess(string endpoint);
    public IReadOnlyList<string> Endpoints { get; }
    public event Action<string>? EndpointFailed;
}
public enum EtcdMembersMode { Poll, OnFailure }   // Off остаётся у модуля-потребителя
public sealed record EtcdMembersMonitorOptions(EtcdMembersMode Mode, int PollIntervalMs, int MinIntervalMs);
public sealed class EtcdMembersMonitor(IEtcdClient etcdClient, EtcdEndpointRotation rotation,
    EtcdMembersMonitorOptions options, ILogger<EtcdMembersMonitor> logger) : BackgroundService { … }
```

- [ ] **Step 1: Создать csproj HA.Etcd + перенести файлы git mv**

`src/PuzzleServer.Infrastructure.App.HA.Etcd/PuzzleServer.Infrastructure.App.HA.Etcd.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk">

    <ItemGroup>
        <PackageReference Include="Microsoft.Extensions.DependencyInjection.Abstractions"/>
        <PackageReference Include="Microsoft.Extensions.Hosting.Abstractions"/>
        <PackageReference Include="Microsoft.Extensions.Logging.Abstractions"/>
        <PackageReference Include="Microsoft.Extensions.Options"/>
    </ItemGroup>
    <ItemGroup>
        <ProjectReference Include="..\PuzzleServer.Infrastructure.App\PuzzleServer.Infrastructure.App.csproj"/>
    </ItemGroup>
    <ItemGroup>
        <InternalsVisibleTo Include="PuzzleServer.UnitTests"/>
    </ItemGroup>

</Project>
```

Перенос (сохраняет историю; тела файлов не меняются, кроме правок Step 2–3):

```bash
cd /Users/demakaev/ZCodeProject/Puzzle
mkdir -p src/PuzzleServer.Infrastructure.App.HA.Etcd
for f in IEtcdClient EtcdHttpClient EtcdKv EtcdWatchEvent EtcdHttpException EtcdMember StreamJsonObjectsReader EtcdEndpointRotation EtcdMembersMonitor; do
  git mv "src/PuzzleServer.Infrastructure.App.HA.Db/Etcd/$f.cs" "src/PuzzleServer.Infrastructure.App.HA.Etcd/$f.cs"
done
```

Во всех перенесённых файлах заменить namespace `PuzzleServer.Infrastructure.App.HA.Db.Etcd` → `PuzzleServer.Infrastructure.App.HA.Etcd`.

Добавить в `src/PuzzleServer.Api.slnx` папку `/Infrastructure/` (по алфавиту, между `HA.Db` и `Kafka`):

```xml
        <Project Path="PuzzleServer.Infrastructure.App.HA.Etcd/PuzzleServer.Infrastructure.App.HA.Etcd.csproj" />
```

- [ ] **Step 2: Отвязать EtcdHttpClient от HaDbOptions**

В `EtcdHttpClient.cs` заменить конструктор и оба использования таймаута (в `RangeAsync` и `MembersAsync`):

```csharp
// Было:
public sealed class EtcdHttpClient(HttpClient httpClient, IOptions<HaDbOptions> options) : IEtcdClient
// ...
requestCts.CancelAfter(TimeSpan.FromMilliseconds(options.Value.RequestTimeoutMs));

// Стало (значение передаёт модуль-потребитель из своих опций; watch — без таймаута, окно сигнальщика):
public sealed class EtcdHttpClient(HttpClient httpClient, int requestTimeoutMs) : IEtcdClient
// ...
requestCts.CancelAfter(TimeSpan.FromMilliseconds(requestTimeoutMs));
```

Удалить `using Microsoft.Extensions.Options;` из файла.

- [ ] **Step 3: Отвязать EtcdMembersMonitor от HaDbOptions**

Создать `src/PuzzleServer.Infrastructure.App.HA.Etcd/EtcdMembersMonitorOptions.cs`:

```csharp
namespace PuzzleServer.Infrastructure.App.HA.Etcd;

/// <summary>
/// Режим слежения за составом etcd-кластера для общего members-монитора.
/// Off у модуля-потребителя: монитор вовсе не регистрируется в DI.
/// </summary>
public enum EtcdMembersMode
{
    /// <summary>Периодический member/list + внеплановый опрос при отказе активного endpoint</summary>
    Poll,

    /// <summary>member/list только после отказа активного endpoint</summary>
    OnFailure,
}

/// <summary>
/// Настройки EtcdMembersMonitor — конструируются модулем-потребителем из его опций
/// (общий слой не знает секций конфигурации потребителей).
/// </summary>
public sealed record EtcdMembersMonitorOptions(EtcdMembersMode Mode, int PollIntervalMs, int MinIntervalMs);
```

В `EtcdMembersMonitor.cs` заменить конструктор и все обращения `options.Value.MembersMode` → `options.Mode`, `options.Value.MembersPollIntervalMs` → `options.PollIntervalMs`, `options.Value.MembersMinIntervalMs` → `options.MinIntervalMs`:

```csharp
// Было:
public sealed class EtcdMembersMonitor(
    IEtcdClient etcdClient,
    EtcdEndpointRotation rotation,
    IOptions<HaDbOptions> options,
    ILogger<EtcdMembersMonitor> logger) : BackgroundService

// Стало:
public sealed class EtcdMembersMonitor(
    IEtcdClient etcdClient,
    EtcdEndpointRotation rotation,
    EtcdMembersMonitorOptions options,
    ILogger<EtcdMembersMonitor> logger) : BackgroundService
```

(сравнение `options.Value.MembersMode == HaDbMembersMode.Poll` → `options.Mode == EtcdMembersMode.Poll`; удалить `using Microsoft.Extensions.Options;`).

- [ ] **Step 4: Переключить HA.Db на HA.Etcd**

`PuzzleServer.Infrastructure.App.HA.Db.csproj`: добавить

```xml
    <ItemGroup>
        <ProjectReference Include="..\PuzzleServer.Infrastructure.App.HA.Etcd\PuzzleServer.Infrastructure.App.HA.Etcd.csproj"/>
    </ItemGroup>
```

Заменить using во всех файлах HA.Db, где был `…HA.Db.Etcd` (ModuleExtensions.cs, TopologyStore.cs, Refresh/WatchLongPollSignaler.cs): `using PuzzleServer.Infrastructure.App.HA.Etcd;`.

`ModuleExtensions.cs` (AddHaDb) — две правки:

```csharp
// (1) typed client: таймаут из HaDb-опций, клиент общий
services.AddHttpClient<IEtcdClient, EtcdHttpClient>((sp, http) =>
{
    http.Timeout = Timeout.InfiniteTimeSpan;
    return new EtcdHttpClient(http, sp.GetRequiredService<IOptions<HaDbOptions>>().Value.RequestTimeoutMs);
});

// (2) members-монитор: маппинг HaDbMembersMode → EtcdMembersMode + параметры
if (membersMode != HaDbMembersMode.Off)
{
    services.AddSingleton(sp => new EtcdMembersMonitor(
        sp.GetRequiredService<IEtcdClient>(),
        sp.GetRequiredService<EtcdEndpointRotation>(),
        new EtcdMembersMonitorOptions(
            membersMode == HaDbMembersMode.Poll ? EtcdMembersMode.Poll : EtcdMembersMode.OnFailure,
            sp.GetRequiredService<IOptions<HaDbOptions>>().Value.MembersPollIntervalMs,
            sp.GetRequiredService<IOptions<HaDbOptions>>().Value.MembersMinIntervalMs),
        sp.GetRequiredService<ILogger<EtcdMembersMonitor>>()));
    services.AddHostedService(sp => sp.GetRequiredService<EtcdMembersMonitor>());
}
```

- [ ] **Step 5: Перенести unit-тесты etcd-слоя**

```bash
cd /Users/demakaev/ZCodeProject/Puzzle
mkdir -p src/PuzzleServer.UnitTests/HA/Etcd
for f in EtcdHttpClientRangeTests EtcdHttpClientWatchTests EtcdHttpClientMembersTests EtcdEndpointRotationTests EtcdMembersMonitorTests StreamJsonObjectsReaderTests; do
  git mv "src/PuzzleServer.UnitTests/HA/Db/$f.cs" "src/PuzzleServer.UnitTests/HA/Etcd/$f.cs"
done
```

В перенесённых файлах: namespace → `PuzzleServer.UnitTests.HA.Etcd`, using `…HA.Db.Etcd` → `…HA.Etcd`; в тестах, конструирующих `EtcdHttpClient(http, options)`/`EtcdMembersMonitor(..., options, ...)` напрямую, — новые сигнатуры: `new EtcdHttpClient(http, 2000)` и `new EtcdMembersMonitor(client, rotation, new EtcdMembersMonitorOptions(EtcdMembersMode.Poll, 30_000, 1_000), logger)`.

- [ ] **Step 6: Проверка не-регрессии (спека Ф1: поведение HA.Db не меняется)**

```bash
dotnet build src/PuzzleServer.Api.slnx   # 0 warnings
dotnet test src/PuzzleServer.Api.slnx --filter "FullyQualifiedName~PuzzleServer.UnitTests"
dotnet test src/PuzzleServer.Api.slnx    # с Docker; критично: HA/Db integration зелёные
```

Expected: все тесты PASS; интеграционные `PuzzleServer.IntegrationTests.HA.Db.*` (ClusterDiscovery/ClusterFailover/RefreshModes/AppSecret/ClusterMemberRemove) — зелёные.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "refactor: extract common etcd layer from HA.Db into Infrastructure.App.HA.Etcd (t05 Ф1)"
```

---

### Task 2: HA.Kafka — csproj и модель снапшота (спека §4.1, §4.4, Фаза Ф2)

**[repo: Puzzle]**

**Files:**
- Create: `src/PuzzleServer.Infrastructure.App.HA.Kafka/PuzzleServer.Infrastructure.App.HA.Kafka.csproj`
- Create: `src/PuzzleServer.Infrastructure.App.HA.Kafka/Model/KafkaClusterSnapshot.cs`, `Model/KafkaTopicInfo.cs`, `Model/KafkaAppSecret.cs`, `Model/KafkaClientConfig.cs`
- Create: `src/PuzzleServer.Infrastructure.App.HA.Kafka/HaKafkaException.cs`
- Modify: `src/PuzzleServer.Api.slnx`
- Test: `src/PuzzleServer.UnitTests/HA/Kafka/ModelTests.cs`

**Interfaces (produces):**
```csharp
namespace PuzzleServer.Infrastructure.App.HA.Kafka;
public sealed class HaKafkaException : Exception { public HaKafkaException(string message); }

namespace PuzzleServer.Infrastructure.App.HA.Kafka.Model;
public sealed record KafkaAppSecret(string User, string Password)
{
    public override string ToString();                 // Password = ***
}
public sealed record KafkaTopicInfo(
    string Name, int? Partitions, int? ReplicationFactor,
    IReadOnlyDictionary<string, string>? Configs);
public sealed record KafkaClientConfig(
    string BootstrapServers, string SecurityProtocol, string SaslMechanism,
    string SaslUsername, string SaslPassword)
{
    public const string SecurityProtocolValue = "SASL_PLAINTEXT";   // контракт arch/15 §5 п.2
    public const string SaslMechanismValue = "PLAIN";
    public override string ToString();                 // SaslPassword = ***
}
public sealed record KafkaClusterSnapshot(
    string Cluster, string? State, string? BootstrapServers, KafkaAppSecret? App,
    IReadOnlyList<KafkaTopicInfo> Topics, DateTimeOffset FetchedAtUtc, long Revision)
{
    public bool HasAppSecret { get; }
    public KafkaClientConfig? GetClientConfig();      // null: нет endpoints ИЛИ нет App
    public KafkaTopicInfo? FindTopic(string name);
    public IReadOnlyList<string> TopicNames { get; }  // по возрастанию имени
}

// ВАЖНО (семантика равенства, паттерн ha-db — docs/01.17 «Сравнение снапшотов — только SameContent»):
// record-== у KafkaClusterSnapshot/KafkaTopicInfo сравнивает коллекционные поля
// (Topics, Configs) ПО ССЫЛКЕ. Структурное сравнение содержимого (включая списки
// и словари) выполняет ТОЛЬКО KafkaDiscoveryStore.SameContent (Task 5) — это
// условие корректности события Updated (спека §4.4, §2 п.7).
```

- [ ] **Step 1: Написать падающие тесты модели**

`src/PuzzleServer.UnitTests/HA/Kafka/ModelTests.cs`:

```csharp
using FluentAssertions;
using PuzzleServer.Infrastructure.App.HA.Kafka.Model;

namespace PuzzleServer.UnitTests.HA.Kafka;

// Модель снапшота HA.Kafka: вычислители, редакция секрета и семантика равенства (спека §4.4)
public class ModelTests
{
    // Arrange
    private static readonly KafkaAppSecret Secret = new("app", "pwd1234567890abcdef1234567890ab");

    [Fact]
    public void GetClientConfig_ReturnsConfig_FromEndpointsAndSecret()
    {
        // Arrange
        var snapshot = new KafkaClusterSnapshot(
            "events", null, "h1:9094,h2:9094", Secret,
            [], DateTimeOffset.UtcNow, 42);

        // Act
        var config = snapshot.GetClientConfig();

        // Assert
        config.Should().NotBeNull();
        config!.BootstrapServers.Should().Be("h1:9094,h2:9094");
        config.SecurityProtocol.Should().Be("SASL_PLAINTEXT");
        config.SaslMechanism.Should().Be("PLAIN");
        config.SaslUsername.Should().Be("app");
        config.SaslPassword.Should().Be("pwd1234567890abcdef1234567890ab");
    }

    [Theory]
    [InlineData(null, false)]     // нет endpoints
    [InlineData("h1:9094", true)] // нет секрета
    public void GetClientConfig_Null_WhenEndpointsOrSecretMissing(string? endpoints, bool hasSecret)
    {
        // Arrange
        var snapshot = new KafkaClusterSnapshot(
            "events", null, endpoints, hasSecret ? Secret : null,
            [], DateTimeOffset.UtcNow, 1);

        // Act
        var config = snapshot.GetClientConfig();

        // Assert
        config.Should().BeNull();
    }

    [Fact]
    public void ToString_RedactsPassword_InSecretAndClientConfig()
    {
        // Arrange
        var config = new KafkaClientConfig("h:9094", "SASL_PLAINTEXT", "PLAIN", "app", "supersecret");

        // Act
        var secretText = Secret.ToString();
        var configText = config.ToString();

        // Assert
        secretText.Should().NotContain("pwd1234567890abcdef1234567890ab").And.Contain("***");
        configText.Should().NotContain("supersecret").And.Contain("***");
    }

    [Fact]
    public void FindTopic_AndTopicNames_SortedByName()
    {
        // Arrange
        var topics = new List<KafkaTopicInfo>
        {
            new("zeta", 3, 1, null),
            new("alpha", 12, 3, new Dictionary<string, string> { ["retention.ms"] = "86400000" }),
        };
        var snapshot = new KafkaClusterSnapshot(
            "events", null, "h:9094", Secret, topics, DateTimeOffset.UtcNow, 1);

        // Act
        var found = snapshot.FindTopic("alpha");
        var names = snapshot.TopicNames;

        // Assert
        found.Should().NotBeNull();
        found!.Partitions.Should().Be(12);
        found.Configs!["retention.ms"].Should().Be("86400000");
        names.Should().Equal("alpha", "zeta");
        snapshot.HasAppSecret.Should().BeTrue();
    }

    [Fact]
    public void KafkaAppSecret_ValueEquality_ByStringFields()
    {
        // Arrange — record только со строковыми полями: компиляторный == структурный
        var a = new KafkaAppSecret("app", "pwd");
        var b = new KafkaAppSecret("app", "pwd");

        // Act
        var equal = a == b;

        // Assert
        equal.Should().BeTrue();
    }

    [Fact]
    public void TopicInfo_RecordEquality_IsReferenceLike_ForDictionaryPayload()
    {
        // Arrange — ДОКУМЕНТИРУЕМ семантику: коллекционные поля в record-== сравниваются
        // по ссылке; структурное сравнение снапшота — ТОЛЬКО KafkaDiscoveryStore.SameContent
        // (Task 5; паттерн ha-db — docs/01.17). Без этого события Updated стреляли бы
        // на каждом рефетче — поэтому тест фиксирует ссылочность явно.
        var configsA = new Dictionary<string, string> { ["retention.ms"] = "86400000" };
        var configsB = new Dictionary<string, string> { ["retention.ms"] = "86400000" };

        // Act
        var equal = new KafkaTopicInfo("orders", 3, 1, configsA)
            == new KafkaTopicInfo("orders", 3, 1, configsB);

        // Assert — разные экземпляры словарей: record-== даёт false при равном содержимом
        equal.Should().BeFalse();
    }
}
```

- [ ] **Step 2: Прогнать тесты — убедиться, что падают**

```bash
dotnet test src/PuzzleServer.Api.slnx --filter "FullyQualifiedName~UnitTests.HA.Kafka.ModelTests"
```

Expected: FAIL — типы `PuzzleServer.Infrastructure.App.HA.Kafka` не найдены (проекта нет).

- [ ] **Step 3: Создать csproj + slnx + модель**

`src/PuzzleServer.Infrastructure.App.HA.Kafka/PuzzleServer.Infrastructure.App.HA.Kafka.csproj` (копия набора HA.Db):

```xml
<Project Sdk="Microsoft.NET.Sdk">

    <ItemGroup>
        <PackageReference Include="Microsoft.Extensions.Configuration.Abstractions"/>
        <PackageReference Include="Microsoft.Extensions.DependencyInjection.Abstractions"/>
        <PackageReference Include="Microsoft.Extensions.Diagnostics.HealthChecks"/>
        <PackageReference Include="Microsoft.Extensions.Diagnostics.HealthChecks.Abstractions"/>
        <PackageReference Include="Microsoft.Extensions.Hosting.Abstractions"/>
        <PackageReference Include="Microsoft.Extensions.Http"/>
        <PackageReference Include="Microsoft.Extensions.Logging.Abstractions"/>
        <PackageReference Include="Microsoft.Extensions.Options"/>
        <PackageReference Include="Microsoft.Extensions.Options.ConfigurationExtensions"/>
    </ItemGroup>
    <ItemGroup>
        <ProjectReference Include="..\PuzzleServer.Infrastructure.App.HA.Etcd\PuzzleServer.Infrastructure.App.HA.Etcd.csproj"/>
        <ProjectReference Include="..\PuzzleServer.Infrastructure.App\PuzzleServer.Infrastructure.App.csproj"/>
    </ItemGroup>
    <ItemGroup>
        <InternalsVisibleTo Include="PuzzleServer.UnitTests"/>
    </ItemGroup>

</Project>
```

В `src/PuzzleServer.Api.slnx`, папка `/Infrastructure/` (после строки HA.Etcd):

```xml
        <Project Path="PuzzleServer.Infrastructure.App.HA.Kafka/PuzzleServer.Infrastructure.App.HA.Kafka.csproj" />
```

`HaKafkaException.cs`:

```csharp
namespace PuzzleServer.Infrastructure.App.HA.Kafka;

/// <summary>Ошибки библиотеки HA.Kafka для Result.Failed (спека §4.2)</summary>
public sealed class HaKafkaException(string message) : Exception(message);
```

`Model/KafkaAppSecret.cs`:

```csharp
namespace PuzzleServer.Infrastructure.App.HA.Kafka.Model;

/// <summary>
/// Per-cluster SASL-креды приложения (app_user + app_password, arch/15 §5 п.2).
/// В снапшот попадают только полным набором обоих ключей.
/// </summary>
public sealed record KafkaAppSecret(string User, string Password)
{
    // Редакция секрета: пароль не светится в логах/дампах (спека §2 п.8)
    public override string ToString() => $"{nameof(KafkaAppSecret)} {{ User = {User}, Password = *** }}";
}
```

`Model/KafkaTopicInfo.cs`:

```csharp
namespace PuzzleServer.Infrastructure.App.HA.Kafka.Model;

/// <summary>
/// Факт-топик реестра /kafka/clusters/C/topics/T (arch/15 §2): только фактические
/// поля; desired-заявки и missing-топики в клиентский реестр не входят (спека §3.1).
/// Равенство: record-== сравнивает Configs по ссылке — структурное сравнение
/// содержимого выполняет KafkaDiscoveryStore.SameContent (паттерн ha-db).
/// </summary>
public sealed record KafkaTopicInfo(
    string Name,
    int? Partitions,                    // null = поля нет (битый/неполный ключ, §6)
    int? ReplicationFactor,
    IReadOnlyDictionary<string, string>? Configs);
```

`Model/KafkaClientConfig.cs`:

```csharp
namespace PuzzleServer.Infrastructure.App.HA.Kafka.Model;

/// <summary>
/// Plain-параметры kafka-клиента (спека §4.4): без зависимости от пакетов Kafka —
/// потребитель (Infrastructure.App.Kafka) собирает из них свой ClientConfig.
/// </summary>
public sealed record KafkaClientConfig(
    string BootstrapServers,
    string SecurityProtocol,
    string SaslMechanism,
    string SaslUsername,
    string SaslPassword)
{
    /// <summary>security.protocol контракта (arch/15 §5 п.2)</summary>
    public const string SecurityProtocolValue = "SASL_PLAINTEXT";

    /// <summary>sasl.mechanisms контракта (arch/15 §5 п.2)</summary>
    public const string SaslMechanismValue = "PLAIN";

    // Редакция секрета: пароль не светится (спека §2 п.8)
    public override string ToString()
        => $"{nameof(KafkaClientConfig)} {{ BootstrapServers = {BootstrapServers}, "
           + $"SecurityProtocol = {SecurityProtocol}, SaslMechanism = {SaslMechanism}, "
           + $"SaslUsername = {SaslUsername}, SaslPassword = *** }}";
}
```

`Model/KafkaClusterSnapshot.cs`:

```csharp
namespace PuzzleServer.Infrastructure.App.HA.Kafka.Model;

/// <summary>
/// Иммутабельный снапшот kafka-кластера из etcd (спека §4.4). State — raw-строка
/// config.state (null = Active, §6); Revision — max(mod_revision) ответа,
/// start_revision следующего watch-окна.
/// Равенство: record-== сравнивает Topics по ссылке — структурное сравнение
/// содержимого (условие корректности события Updated, спека §2 п.7) выполняет
/// KafkaDiscoveryStore.SameContent; НЕ меняйте на record-== (паттерн ha-db).
/// </summary>
public sealed record KafkaClusterSnapshot(
    string Cluster,
    string? State,
    string? BootstrapServers,
    KafkaAppSecret? App,
    IReadOnlyList<KafkaTopicInfo> Topics,
    DateTimeOffset FetchedAtUtc,
    long Revision)
{
    public bool HasAppSecret => App is not null;

    // null при отсутствии endpoints ИЛИ секрета (спека §4.4)
    public KafkaClientConfig? GetClientConfig()
        => BootstrapServers is null || App is null
            ? null
            : new KafkaClientConfig(
                BootstrapServers,
                KafkaClientConfig.SecurityProtocolValue,
                KafkaClientConfig.SaslMechanismValue,
                App.User,
                App.Password);

    public KafkaTopicInfo? FindTopic(string name)
        => Topics.FirstOrDefault(t => t.Name == name);

    // По возрастанию имени — парсер отдаёт уже отсортированным (детерминизм)
    public IReadOnlyList<string> TopicNames => Topics.Select(t => t.Name).ToList();
}
```

- [ ] **Step 4: Прогнать тесты — убедиться, что проходят**

```bash
dotnet test src/PuzzleServer.Api.slnx --filter "FullyQualifiedName~UnitTests.HA.Kafka.ModelTests"
```

Expected: PASS (7 кейсов: 5 Facts + Theory с 2 InlineData).

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: HA.Kafka project skeleton + snapshot model with secret redaction (t05 Ф2)"
```

---

### Task 3: HA.Kafka — парсер KafkaClusterParser (спека §4.3, Фаза Ф3)

**[repo: Puzzle]**

**Files:**
- Create: `src/PuzzleServer.Infrastructure.App.HA.Kafka/Parsing/KafkaClusterParser.cs`
- Test: `src/PuzzleServer.UnitTests/HA/Kafka/KafkaClusterParserTests.cs`

**Interfaces (consumes/produces):**
```csharp
// Consumes (Task 1): EtcdKv(string Key, string Value, long ModRevision)
//                   из PuzzleServer.Infrastructure.App.HA.Etcd
// Produces:
namespace PuzzleServer.Infrastructure.App.HA.Kafka.Parsing;

public sealed record KafkaClusterData(
    string? State, string? BootstrapServers, KafkaAppSecret? App,
    IReadOnlyList<KafkaTopicInfo> Topics);

public static class KafkaClusterParser
{
    // Чистая функция разбора range-ответа префикса /kafka/clusters/<C>/.
    // parseErrors — битые значения; unknownKeys — неизвестные ключи внутри /kafka/
    // (спека §2 п.10, счётчик для лога; brokers/* в эти списки НЕ попадают).
    public static KafkaClusterData Parse(
        string cluster, IReadOnlyList<EtcdKv> kvs,
        out IReadOnlyList<string> parseErrors, out IReadOnlyList<string> unknownKeys);
}
```

- [ ] **Step 1: Написать падающие тесты парсера**

`src/PuzzleServer.UnitTests/HA/Kafka/KafkaClusterParserTests.cs` (ключи — канон arch/15 §2.1; EtcdKv-хелпер):

```csharp
using FluentAssertions;
using PuzzleServer.Infrastructure.App.HA.Etcd;
using PuzzleServer.Infrastructure.App.HA.Kafka.Model;
using PuzzleServer.Infrastructure.App.HA.Kafka.Parsing;

namespace PuzzleServer.UnitTests.HA.Kafka;

// Парсер /kafka/clusters/<C>/ — канонические значения arch/15 §2.1 и толерантность §6
public class KafkaClusterParserTests
{
    // Arrange-хелпер: kv по относительному ключу (префикс /kafka/clusters/events/)
    private static EtcdKv Kv(string relativeKey, string value, long modRevision = 1)
        => new($"/kafka/clusters/events/{relativeKey}", value, modRevision);

    [Fact]
    public void Parse_CanonicalActiveCluster_EndpointsSecretAndTopics()
    {
        // Arrange — канон arch/15 §2.1: Active-config без state, endpoints, topics/orders с desired
        var kvs = new[]
        {
            Kv("config", """{"brokers":3,"replication_factor":3,"min_insync_replicas":2,"default_partitions":12,"default_retention_ms":604800000,"created_unix":1756500000}"""),
            Kv("endpoints", "host.docker.internal:16001,host.docker.internal:16002,host.docker.internal:16003"),
            Kv("app_user", "app"),
            Kv("app_password", "abcdefghijklmnopqrstuvwxyz012345"),
            Kv("topics/orders", """{"partitions":12,"replication_factor":3,"configs":{"retention.ms":"604800000","min.insync.replicas":"2"},"desired":{"partitions":16,"configs":{"retention.ms":"86400000"}},"desired_unix":1750000000,"desired_by":"admin","synced_unix":1750000100,"missing":false}"""),
        };

        // Act
        var data = KafkaClusterParser.Parse("events", kvs, out var parseErrors, out var unknownKeys);

        // Assert
        data.State.Should().BeNull(); // нет поля state = Active
        data.BootstrapServers.Should().Be("host.docker.internal:16001,host.docker.internal:16002,host.docker.internal:16003");
        data.App.Should().NotBeNull();
        data.App!.User.Should().Be("app");
        data.App.Password.Should().Be("abcdefghijklmnopqrstuvwxyz012345");
        data.Topics.Should().ContainSingle();
        data.Topics[0].Name.Should().Be("orders");
        data.Topics[0].Partitions.Should().Be(12);       // факт, desired игнорируется
        data.Topics[0].ReplicationFactor.Should().Be(3);
        data.Topics[0].Configs!.Count.Should().Be(2);
        parseErrors.Should().BeEmpty();
        unknownKeys.Should().BeEmpty();
    }

    [Fact]
    public void Parse_ClusterStates_RawString()
    {
        // Arrange
        var notInitialized = new[] { Kv("config", """{"brokers":3,"created_unix":1,"state":"NOT_INITIALIZED"}""") };
        var toRemove = new[] { Kv("config", """{"brokers":3,"created_unix":1,"state":"TO_REMOVE"}""") };
        var future = new[] { Kv("config", """{"brokers":3,"created_unix":1,"state":"SOMETHING_NEW"}""") };

        // Act
        var a = KafkaClusterParser.Parse("events", notInitialized, out _, out _);
        var b = KafkaClusterParser.Parse("events", toRemove, out _, out _);
        var c = KafkaClusterParser.Parse("events", future, out _, out _);

        // Assert — незнакомое state толерантно, raw-строкой (§6)
        a.State.Should().Be("NOT_INITIALIZED");
        b.State.Should().Be("TO_REMOVE");
        c.State.Should().Be("SOMETHING_NEW");
    }

    [Fact]
    public void Parse_IncompleteSecret_AppIsNull()
    {
        // Arrange — неполный набор кредов: секрета нет (спека §3.1)
        var kvs = new[]
        {
            Kv("config", """{"brokers":3,"created_unix":1}"""),
            Kv("app_user", "app"),
        };

        // Act
        var data = KafkaClusterParser.Parse("events", kvs, out _, out _);

        // Assert
        data.App.Should().BeNull();
    }

    [Fact]
    public void Parse_LifecycleAndMissingTopics_ExcludedFromRegistry()
    {
        // Arrange — desired.create/desired.delete — leaf-ключи заявок (7 сегментов);
        // missing:true — топик фактически не существует (канон §2.1 topics/ghost)
        var kvs = new[]
        {
            Kv("topics/audit/desired.create", """{"partitions":12,"replication_factor":3,"requested_unix":1750000000,"requested_by":"admin"}"""),
            Kv("topics/old/desired.delete", """{"requested_unix":1750000100,"requested_by":"admin"}"""),
            Kv("topics/ghost", """{"partitions":3,"replication_factor":1,"desired":{"configs":{"retention.ms":"86400000"}},"desired_unix":1750000200,"desired_by":"admin","synced_unix":1750000300,"missing":true}"""),
            Kv("topics/live", """{"partitions":6,"replication_factor":3,"synced_unix":1750000300,"missing":false}"""),
        };

        // Act
        var data = KafkaClusterParser.Parse("events", kvs, out _, out _);

        // Assert — в реестре только фактически существующие факт-топики
        data.Topics.Select(t => t.Name).Should().Equal("live");
    }

    [Fact]
    public void Parse_BrokersSkippedSilently_InternalTopicSkipped()
    {
        // Arrange — brokers/* — известные ключи вне клиентского подмножества (молча);
        // __-топики в реестр не попадают вовсе
        var kvs = new[]
        {
            Kv("brokers/broker1/state", "RUNNING"),
            Kv("brokers/broker1/role", "controller"),
            Kv("brokers/broker1/resources", """{"cpu":"2","mem":"4Gi","disk":"40Gi"}"""),
            Kv("topics/__consumer_offsets", """{"partitions":50,"replication_factor":3,"synced_unix":1,"missing":false}"""),
            Kv("topics/orders", """{"partitions":12,"replication_factor":3,"synced_unix":1,"missing":false}"""),
        };

        // Act
        var data = KafkaClusterParser.Parse("events", kvs, out var parseErrors, out var unknownKeys);

        // Assert
        data.Topics.Select(t => t.Name).Should().Equal("orders");
        parseErrors.Should().BeEmpty();
        unknownKeys.Should().BeEmpty(); // brokers — не unknown
    }

    [Fact]
    public void Parse_BrokenJson_ParseErrorAndSkip()
    {
        // Arrange — битый JSON: ключ пропускается, парсер не падает (§6)
        var kvs = new[]
        {
            Kv("config", "{not-json"),
            Kv("endpoints", "h:9094"),
            Kv("topics/broken", "{also-not-json"),
            Kv("topics/ok", """{"partitions":3,"synced_unix":1,"missing":false}"""),
        };

        // Act
        var data = KafkaClusterParser.Parse("events", kvs, out var parseErrors, out _);

        // Assert
        data.State.Should().BeNull();          // битый config → Active-ветка
        data.BootstrapServers.Should().Be("h:9094");
        data.Topics.Select(t => t.Name).Should().Equal("ok");
        parseErrors.Should().HaveCount(2);
    }

    [Fact]
    public void Parse_TopicWithoutFactFields_NullFieldsSurvive()
    {
        // Arrange — топик без части факт-полей читается с null-полями (§6)
        var kvs = new[] { Kv("topics/partial", """{"synced_unix":1,"missing":false}""") };

        // Act
        var data = KafkaClusterParser.Parse("events", kvs, out _, out _);

        // Assert
        data.Topics.Should().ContainSingle().Which.Should().Match<KafkaTopicInfo>(
            t => t.Partitions is null && t.ReplicationFactor is null && t.Configs is null);
    }

    [Fact]
    public void Parse_UnknownLeaf_UnknownKeyCounter()
    {
        // Arrange — неизвестный ключ внутри /kafka/: лог + счётчик, парсер не падает (§6)
        var kvs = new[]
        {
            Kv("something/new", "value"),
            Kv("endpoints", "h:9094"),
            Kv("topics/x/unknown-leaf", "value"),
        };

        // Act
        var data = KafkaClusterParser.Parse("events", kvs, out _, out var unknownKeys);

        // Assert
        data.BootstrapServers.Should().Be("h:9094");
        unknownKeys.Should().HaveCount(2);
    }

    [Fact]
    public void Parse_TopicsSortedByName()
    {
        // Arrange
        var kvs = new[]
        {
            Kv("topics/zulu", """{"partitions":1,"synced_unix":1,"missing":false}"""),
            Kv("topics/alpha", """{"partitions":2,"synced_unix":1,"missing":false}"""),
        };

        // Act
        var data = KafkaClusterParser.Parse("events", kvs, out _, out _);

        // Assert
        data.Topics.Select(t => t.Name).Should().Equal("alpha", "zulu");
    }
}
```

- [ ] **Step 2: Прогнать — убедиться, что падают**

```bash
dotnet test src/PuzzleServer.Api.slnx --filter "FullyQualifiedName~UnitTests.HA.Kafka.KafkaClusterParserTests"
```

Expected: FAIL — `KafkaClusterParser` не существует.

- [ ] **Step 3: Реализовать парсер**

`src/PuzzleServer.Infrastructure.App.HA.Kafka/Parsing/KafkaClusterParser.cs`:

```csharp
using System.Text.Json;
using PuzzleServer.Infrastructure.App.HA.Etcd;
using PuzzleServer.Infrastructure.App.HA.Kafka.Model;

namespace PuzzleServer.Infrastructure.App.HA.Kafka.Parsing;

/// <summary>Разобранные данные кластера (без Revision/FetchedAtUtc — их добавляет store)</summary>
public sealed record KafkaClusterData(
    string? State,
    string? BootstrapServers,
    KafkaAppSecret? App,
    IReadOnlyList<KafkaTopicInfo> Topics);

// Чистые функции разбора range-ответа /kafka/clusters/<C>/ (спека §4.3).
// Сегменты: key.Split('/') с ведущим пустым — факт-ключ topics/<T> = 6 сегментов
// (конвенция AdminPanel KafkaParser); desired.* = 7 сегментов — заявки, не факт.
// Разбор — JsonDocument + ручные читатели (ReadInt/ReadBool/ReadConfigs):
// JsonSerializerOptions НЕ заводить без использования — CS0414 под
// TreatWarningsAsErrors=true (числа-строки покрывает ReadInt).
public static class KafkaClusterParser
{
    public static KafkaClusterData Parse(
        string cluster,
        IReadOnlyList<EtcdKv> kvs,
        out IReadOnlyList<string> parseErrors,
        out IReadOnlyList<string> unknownKeys)
    {
        var errors = new List<string>();
        var unknown = new List<string>();
        string? state = null;
        string? bootstrap = null;
        string? appUser = null;
        string? appPassword = null;
        var topics = new List<(string Name, string? Value)>();

        foreach (var kv in kvs)
        {
            var segments = kv.Key.Split('/');
            // ожидаем /kafka/clusters/<C>/...: [0]="", [1]="kafka", [2]="clusters", [3]=C
            if (segments.Length < 5 || segments[1] != "kafka" || segments[2] != "clusters"
                || segments[3] != cluster)
            {
                continue; // чужой ключ в ответе не бывает (range по префиксу), на всякий случай — мимо
            }

            switch (segments[4])
            {
                case "config" when segments.Length == 5:
                    state = ParseConfigState(kv.Value, errors);
                    break;
                case "endpoints" when segments.Length == 5:
                    bootstrap = kv.Value;
                    break;
                case "app_user" when segments.Length == 5:
                    appUser = kv.Value;
                    break;
                case "app_password" when segments.Length == 5:
                    appPassword = kv.Value;
                    break;
                case "brokers":
                    break; // известные ключи вне клиентского подмножества §5 — молча (спека §3.1)
                case "topics" when segments.Length == 6 && segments[5].Length > 0:
                    topics.Add((segments[5], kv.Value));
                    break;
                case "topics" when segments.Length == 7
                    && segments[6] is "desired.create" or "desired.delete":
                    break; // leaf-ключи заявок — не факт-ключи (§5 п.3), пропускаем
                default:
                    unknown.Add(kv.Key); // в т.ч. неизвестный leaf под topics/<T>/ (§6)
                    break;
            }
        }

        parseErrors = errors;
        unknownKeys = unknown;

        return new KafkaClusterData(
            state,
            bootstrap,
            // креды — только полным набором обоих ключей (спека §3.1)
            appUser is not null && appPassword is not null ? new KafkaAppSecret(appUser, appPassword) : null,
            [.. topics
                .Where(t => !t.Name.StartsWith("__", StringComparison.Ordinal)) // internal — мимо
                .Select(t => ParseTopic(t.Name, t.Value, errors))
                .Where(t => t is not null)
                .OrderBy(t => t!.Name)]);
    }

    // config: только state (raw-строкой, §6); остальное клиенту не нужно (спека §3.1)
    private static string? ParseConfigState(string value, List<string> errors)
    {
        try
        {
            using var doc = JsonDocument.Parse(value);
            return doc.RootElement.TryGetProperty("state", out var state)
                && state.ValueKind == JsonValueKind.String
                    ? state.GetString()
                    : null;
        }
        catch (JsonException)
        {
            errors.Add($"config: битый JSON ({Redact(value)})");
            return null; // битый config → Active-ветка, кластер в снапшоте жив (спека §3.1)
        }
    }

    private static KafkaTopicInfo? ParseTopic(string name, string? value, List<string> errors)
    {
        if (value is null)
            return null;
        try
        {
            using var doc = JsonDocument.Parse(value);
            var root = doc.RootElement;
            if (ReadBool(root, "missing") == true)
                return null; // топик фактически не существует — вне клиентского реестра (спека §3.1)
            return new KafkaTopicInfo(
                name,
                ReadInt(root, "partitions"),
                ReadInt(root, "replication_factor"),
                ReadConfigs(root, "configs"));
        }
        catch (JsonException)
        {
            errors.Add($"topics/{name}: битый JSON ({Redact(value)})");
            return null;
        }
    }

    private static int? ReadInt(JsonElement root, string name)
        => root.TryGetProperty(name, out var el) && (el.ValueKind == JsonValueKind.Number
                                                       || el.ValueKind == JsonValueKind.String)
           && int.TryParse(el.ToString(), out var value)
            ? value
            : null;

    private static bool? ReadBool(JsonElement root, string name)
        => root.TryGetProperty(name, out var el) && (el.ValueKind == JsonValueKind.True
                                                      || el.ValueKind == JsonValueKind.False)
            ? el.GetBoolean()
            : null;

    private static IReadOnlyDictionary<string, string>? ReadConfigs(JsonElement root, string name)
    {
        if (!root.TryGetProperty(name, out var configs) || configs.ValueKind != JsonValueKind.Object)
            return null;
        var result = new Dictionary<string, string>();
        foreach (var prop in configs.EnumerateObject())
            result[prop.Name] = prop.Value.ToString();
        return result;
    }

    // Значение ключа в ошибки — укороченное и без подозрительных данных
    private static string Redact(string value)
        => value.Length <= 40 ? value : value[..40] + "…";
}
```

- [ ] **Step 4: Прогнать — убедиться, что проходят**

```bash
dotnet test src/PuzzleServer.Api.slnx --filter "FullyQualifiedName~UnitTests.HA.Kafka.KafkaClusterParserTests"
```

Expected: PASS (9 тестов).

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: HA.Kafka cluster parser (arch/15 §2.1 canon, tolerant, desired/missing filters) (t05 Ф3)"
```

---

### Task 4: HA.Kafka — опции, реестр заявок, ModuleExtensions (спека §4.7, §4.8, Фаза Ф4)

**[repo: Puzzle]**

**Files:**
- Create: `src/PuzzleServer.Infrastructure.App.HA.Kafka/HaKafkaOptions.cs`, `HaKafkaClusterRegistry.cs`, `ModuleExtensions.cs`
- Test: `src/PuzzleServer.UnitTests/HA/Kafka/ModuleRegistrationTests.cs`

**Interfaces (consumes/produces):**
```csharp
// Consumes (Task 1): IEtcdClient, EtcdHttpClient, EtcdEndpointRotation,
//                    EtcdMembersMonitor, EtcdMembersMonitorOptions, EtcdMembersMode
// Produces:
namespace PuzzleServer.Infrastructure.App.HA.Kafka;

public enum HaKafkaRefreshMode { WatchLongPoll, Poll }
public enum HaKafkaMembersMode { Poll, OnFailure, Off }
public class HaKafkaOptions
{
    public HaKafkaRefreshMode Mode { get; set; } = HaKafkaRefreshMode.WatchLongPoll;
    public string[] EtcdEndpoints { get; set; } = [];
    public int RequestTimeoutMs { get; set; } = 2000;
    public int WatchWindowMs { get; set; } = 1000;
    public int WatchReopenDelayMs { get; set; } = 100;
    public int WatchErrorDelayMs { get; set; } = 1000;
    public int PollIntervalMs { get; set; } = 1000;
    public HaKafkaMembersMode MembersMode { get; set; } = HaKafkaMembersMode.Poll;
    public int MembersPollIntervalMs { get; set; } = 30_000;
    public int MembersMinIntervalMs { get; set; } = 1_000;
    public int BootstrapTimeoutSec { get; set; } = 15;
    public string[] Clusters { get; set; } = [];   // наполняется PostConfigure из реестра
}
public sealed class HaKafkaClusterRegistry
{
    public IReadOnlyList<string> Clusters { get; }
    public void Add(string cluster);               // fail-fast: пусто/формат/дубликат
}
public static class ModuleExtensions
{
    public static IServiceCollection AddHaKafka(this IServiceCollection services, IConfiguration configuration);
    public static IServiceCollection AddKafkaCluster(this IServiceCollection services, string cluster);
}
```

- [ ] **Step 1: Написать падающие тесты регистрации**

`src/PuzzleServer.UnitTests/HA/Kafka/ModuleRegistrationTests.cs`:

```csharp
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using PuzzleServer.Infrastructure.App.HA.Etcd;
using PuzzleServer.Infrastructure.App.HA.Kafka;
using PuzzleServer.Infrastructure.App.HA.Kafka.Refresh;

namespace PuzzleServer.UnitTests.HA.Kafka;

// Регистрация модуля HA.Kafka: флуент-заявки кластеров, PostConfigure, fail-fast,
// выбор сигнальщика по Mode (спека §4.8, §8 п.7)
public class ModuleRegistrationTests
{
    private static ServiceCollection Services() => [];

    private static IConfiguration Config(Dictionary<string, string?>? extra = null)
    {
        var data = new Dictionary<string, string?>
        {
            ["HaKafka:EtcdEndpoints:0"] = "http://etcd:2379",
        };
        foreach (var (k, v) in extra ?? [])
            data[k] = v;
        return new ConfigurationBuilder().AddInMemoryCollection(data).Build();
    }

    [Fact]
    public void AddKafkaCluster_FillsOptionsClusters_ThroughPostConfigure()
    {
        // Arrange
        var services = Services();

        // Act
        services.AddHaKafka(Config()).AddKafkaCluster("events").AddKafkaCluster("pending");
        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<HaKafkaOptions>>().Value;

        // Assert
        options.Clusters.Should().Equal("events", "pending");
    }

    [Theory]
    [InlineData("")]              // пустое имя
    [InlineData("Events")]        // заглавная буква
    [InlineData("e-vents")]       // дефис (арх/15: без дефиса)
    [InlineData("events")]        // дубликат (второй AddKafkaCluster("events"))
    public void AddKafkaCluster_FailFast_OnBadName(string cluster)
    {
        // Arrange
        var services = Services();
        services.AddHaKafka(Config()).AddKafkaCluster("events");

        // Act
        var act = () => services.AddKafkaCluster(cluster);

        // Assert
        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void AddKafkaCluster_FailFast_WhenModuleNotRegistered()
    {
        // Arrange
        var services = Services();

        // Act
        var act = () => services.AddKafkaCluster("events");

        // Assert
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*AddHaKafka*");
    }

    [Fact]
    public void AddHaKafka_FailFast_OnSecondCall()
    {
        // Arrange
        var services = Services();
        services.AddHaKafka(Config());

        // Act
        var act = () => services.AddHaKafka(Config());

        // Assert
        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void AddHaKafka_FailFast_WhenNoClustersDeclared()
    {
        // Arrange — старт без единой заявки запрещён (спека §4.8)
        var services = Services();
        services.AddHaKafka(Config());

        // Act — резолв сервиса, чья фабрика валидирует опции (EtcdEndpointRotation)
        using var provider = services.BuildServiceProvider();
        var act = () => provider.GetRequiredService<EtcdEndpointRotation>();

        // Assert
        act.Should().Throw<InvalidOperationException>().WithMessage("*кластер*заявлен*");
    }

    [Fact]
    public void AddHaKafka_FailFast_OnEmptyEndpointsAndBadIntervals()
    {
        // Arrange — секция без endpoints
        var noEndpoints = new ConfigurationBuilder().AddInMemoryCollection(
            new Dictionary<string, string?> { ["HaKafka:PollIntervalMs"] = "1000" }).Build();
        var badInterval = new ConfigurationBuilder().AddInMemoryCollection(
            new Dictionary<string, string?>
            {
                ["HaKafka:EtcdEndpoints:0"] = "http://etcd:2379",
                ["HaKafka:PollIntervalMs"] = "0",
            }).Build();
        var services1 = Services();
        services1.AddHaKafka(noEndpoints).AddKafkaCluster("events");
        var services2 = Services();
        services2.AddHaKafka(badInterval).AddKafkaCluster("events");

        // Act
        using var p1 = services1.BuildServiceProvider();
        using var p2 = services2.BuildServiceProvider();
        var act1 = () => p1.GetRequiredService<EtcdEndpointRotation>();
        var act2 = () => p2.GetRequiredService<EtcdEndpointRotation>();

        // Assert
        act1.Should().Throw<InvalidOperationException>().WithMessage("*EtcdEndpoints*");
        act2.Should().Throw<InvalidOperationException>().WithMessage("*PollIntervalMs*");
    }

    [Fact]
    public void AddHaKafka_AutoRegistration_ResolvesStoreAndRefresher()
    {
        // Arrange
        var services = Services();

        // Act
        services.AddHaKafka(Config()).AddKafkaCluster("events");
        using var provider = services.BuildServiceProvider();

        // Assert — AutoRegistration сборки поднял [InjectAsSingleton]-сервисы (спека §4.8)
        provider.GetRequiredService<IKafkaDiscoveryStore>().Should().NotBeNull();
        provider.GetRequiredService<KafkaDiscoveryRefresher>().Should().NotBeNull();
    }

    [Fact]
    public void Mode_Poll_ResolvesPollSignaler()
    {
        // Arrange
        var services = Services();
        services.AddHaKafka(Config(new() { ["HaKafka:Mode"] = "Poll" })).AddKafkaCluster("events");

        // Act
        using var provider = services.BuildServiceProvider();
        var signaler = provider.GetRequiredService<IHaKafkaRefreshSignaler>();

        // Assert — режим Poll выбирает poll-сигнальщик (спека §8 п.7)
        signaler.Should().BeOfType<HaKafkaPollRefreshSignaler>();
    }

    [Fact]
    public void AddHaKafka_Defaults_WatchLongPollAndIntervals()
    {
        // Arrange
        var services = Services();

        // Act
        services.AddHaKafka(Config()).AddKafkaCluster("events");
        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<HaKafkaOptions>>().Value;

        // Assert — дефолты спеки §4.7
        options.Mode.Should().Be(HaKafkaRefreshMode.WatchLongPoll);
        options.WatchWindowMs.Should().Be(1000);
        options.BootstrapTimeoutSec.Should().Be(15);
        options.MembersMode.Should().Be(HaKafkaMembersMode.Poll);
    }
}
```

(Тест выбора сигнальщика для `Mode=WatchLongPoll` добавляется в Task 6 — после появления `HaKafkaWatchLongPollSignaler` и возврата полной switch-ветки, см. Task 5 Step 6 / Task 6 Step 1.)

- [ ] **Step 2: Прогнать — убедиться, что падают**

```bash
dotnet test src/PuzzleServer.Api.slnx --filter "FullyQualifiedName~UnitTests.HA.Kafka.ModuleRegistrationTests"
```

Expected: FAIL — `AddHaKafka` не существует.

- [ ] **Step 3: Реализовать опции, реестр и регистрацию (health-класс `HaKafkaHealthCheck` создаётся в Task 5 вместе с `KafkaDiscoveryRefresher`; компиляция и тесты Task 4 замыкаются общим циклом с Task 5 — см. Step 4)**

`HaKafkaOptions.cs`:

```csharp
namespace PuzzleServer.Infrastructure.App.HA.Kafka;

/// <summary>Режим фоновой актуализации (спека §4.7)</summary>
public enum HaKafkaRefreshMode
{
    /// <summary>Короткоживущие watch-стримы /v3/watch: событие по префиксу кластера → форс-рефетч</summary>
    WatchLongPoll,

    /// <summary>Периодический полный рефетч всех префиксов</summary>
    Poll,
}

/// <summary>Режим слежения за составом etcd-кластера (спека §4.7)</summary>
public enum HaKafkaMembersMode
{
    /// <summary>Периодический member/list + внеплановый опрос при отказе активного endpoint</summary>
    Poll,

    /// <summary>member/list только после отказа активного endpoint</summary>
    OnFailure,

    /// <summary>Состав фиксирован конфигом — монитора нет в DI вовсе</summary>
    Off,
}

/// <summary>Конфигурация секции "HaKafka" (спека §4.7). Кластеры секцией НЕ задаются — только заявками AddKafkaCluster.</summary>
public class HaKafkaOptions
{
    public HaKafkaRefreshMode Mode { get; set; } = HaKafkaRefreshMode.WatchLongPoll;

    /// <summary>Seed-адреса etcd (HTTP JSON gateway): точка первого соединения и вечный алиас</summary>
    public string[] EtcdEndpoints { get; set; } = [];

    /// <summary>Наполняется PostConfigure из реестра заявок AddKafkaCluster; секцией не задаётся</summary>
    public string[] Clusters { get; set; } = [];

    public int RequestTimeoutMs { get; set; } = 2000;

    // --- WatchLongPoll ---
    public int WatchWindowMs { get; set; } = 1000;
    public int WatchReopenDelayMs { get; set; } = 100;
    public int WatchErrorDelayMs { get; set; } = 1000;

    // --- Poll ---
    public int PollIntervalMs { get; set; } = 1000;

    // --- Members (node discovery) ---
    public HaKafkaMembersMode MembersMode { get; set; } = HaKafkaMembersMode.Poll;
    public int MembersPollIntervalMs { get; set; } = 30_000;
    public int MembersMinIntervalMs { get; set; } = 1_000;

    // --- Общее ---
    public int BootstrapTimeoutSec { get; set; } = 15;
}
```

`HaKafkaClusterRegistry.cs`:

```csharp
using System.Text.RegularExpressions;

namespace PuzzleServer.Infrastructure.App.HA.Kafka;

/// <summary>
/// Реестр заявок kafka-кластеров (паттерн ConfigurationTopologyRegistry, спека §4.8):
/// заявки дают флуент-вызовы AddKafkaCluster после AddHaKafka; PostConfigure
/// переносит их в HaKafkaOptions.Clusters.
/// </summary>
public sealed partial class HaKafkaClusterRegistry
{
    // Имя кластера — ^[a-z][a-z0-9_]{0,62}$ (arch/15)
    [GeneratedRegex("^[a-z][a-z0-9_]{0,62}$")]
    private static partial Regex ClusterName();

    private readonly List<string> _clusters = [];

    public IReadOnlyList<string> Clusters => _clusters;

    public void Add(string cluster)
    {
        if (string.IsNullOrWhiteSpace(cluster))
            throw new InvalidOperationException("HA.Kafka: имя kafka-кластера не задано");
        if (!ClusterName().IsMatch(cluster))
            throw new InvalidOperationException(
                $"HA.Kafka: недопустимое имя кластера '{cluster}' (нужен ^[a-z][a-z0-9_]{{0,62}}$, arch/15)");
        if (_clusters.Contains(cluster))
            throw new InvalidOperationException($"HA.Kafka: кластер '{cluster}' уже заявлен");
        _clusters.Add(cluster);
    }
}
```

`ModuleExtensions.cs`:

```csharp
using System.Reflection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PuzzleServer.Infrastructure.App.DI;
using PuzzleServer.Infrastructure.App.HA.Etcd;

namespace PuzzleServer.Infrastructure.App.HA.Kafka;

public static class ModuleExtensions
{
    private static Assembly Assembly => typeof(ModuleExtensions).Assembly;

    /// <summary>
    /// Регистрирует HA.Kafka-модуль (спека §4.8): опции (секция HaKafka, валидация),
    /// общий etcd-клиент из HA.Etcd (таймаут из HaKafka:RequestTimeoutMs), ротация,
    /// сигнальщик по Mode, health-check "HaKafkaCheck", members-монитор.
    /// Кластеры задаются ЗАЯВКАМИ: .AddKafkaCluster("name") после этого вызова.
    /// </summary>
    public static IServiceCollection AddHaKafka(this IServiceCollection services, IConfiguration configuration)
    {
        if (services.Any(d => d.ServiceType == typeof(HaKafkaClusterRegistry)))
            throw new InvalidOperationException("HA.Kafka: модуль уже зарегистрирован (повторный AddHaKafka)");

        var registry = new HaKafkaClusterRegistry();
        services.AddSingleton(registry);
        services.Configure<HaKafkaOptions>(configuration.GetSection("HaKafka"));
        // Кластеры — из реестра заявок (спека §4.8); пережиток HaKafka:Clusters в конфиге игнорируется
        services.AddOptions<HaKafkaOptions>().PostConfigure(options => options.Clusters = [.. registry.Clusters]);
        services.AddHttpClient<IEtcdClient, EtcdHttpClient>((sp, http) =>
        {
            // Без глобального таймаута: watch-стрим живёт дольше любого фиксированного
            http.Timeout = Timeout.InfiniteTimeSpan;
            return new EtcdHttpClient(http,
                sp.GetRequiredService<IOptions<HaKafkaOptions>>().Value.RequestTimeoutMs);
        });
        services.AddHealthChecks().AddCheck<HaKafkaHealthCheck>("HaKafkaCheck");
        services.AddSingleton(sp =>
        {
            var options = sp.GetRequiredService<IOptions<HaKafkaOptions>>().Value;
            ValidateOptions(options);
            return new EtcdEndpointRotation(options.EtcdEndpoints);
        });

        // Сигнальщик выбирается режимом при старте. Ф4: watch-сигнальщик появится в
        // Ф5/Task 6 — ОБА режима временно работают poll-сигнальщиком (семантика
        // та же: сигнал → полный рефетч; коммит Ф4 помечен "(watch — Task 6)").
        // Task 6 Step 4 заменяет ветку WatchLongPoll на HaKafkaWatchLongPollSignaler.
        services.AddSingleton<IHaKafkaRefreshSignaler>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<HaKafkaOptions>>().Value;
            return options.Mode switch
            {
                HaKafkaRefreshMode.Poll => new HaKafkaPollRefreshSignaler(options),
                HaKafkaRefreshMode.WatchLongPoll => new HaKafkaPollRefreshSignaler(options), // временно (Task 6)
                _ => throw new InvalidOperationException($"HA.Kafka: Mode={options.Mode} не поддерживается"),
            };
        });

        // Members-режим читается прямым биндом секции (IOptions недоступен на этапе регистрации)
        var membersMode = configuration.GetSection("HaKafka").Get<HaKafkaOptions>()?.MembersMode
                          ?? HaKafkaMembersMode.Poll;
        if (membersMode != HaKafkaMembersMode.Off)
        {
            services.AddSingleton(sp => new EtcdMembersMonitor(
                sp.GetRequiredService<IEtcdClient>(),
                sp.GetRequiredService<EtcdEndpointRotation>(),
                new EtcdMembersMonitorOptions(
                    membersMode == HaKafkaMembersMode.Poll ? EtcdMembersMode.Poll : EtcdMembersMode.OnFailure,
                    sp.GetRequiredService<IOptions<HaKafkaOptions>>().Value.MembersPollIntervalMs,
                    sp.GetRequiredService<IOptions<HaKafkaOptions>>().Value.MembersMinIntervalMs),
                sp.GetRequiredService<ILogger<EtcdMembersMonitor>>()));
            services.AddHostedService(sp => sp.GetRequiredService<EtcdMembersMonitor>());
        }

        // AutoRegistration сборки: [InjectAsSingleton] KafkaDiscoveryStore/KafkaDiscoveryRefresher
        // (refresher — как hosted TopologyRefresher в HaDb; спека §4.8)
        return services.AutoRegistration(Assembly);
    }

    /// <summary>
    /// Заявка kafka-кластера (спека §4.8): КАЖДЫЙ вызов добавляет имя в реестр.
    /// Fail-fast: модуль не зарегистрирован, пустое имя, неверный формат (arch/15), дубликат.
    /// </summary>
    public static IServiceCollection AddKafkaCluster(this IServiceCollection services, string cluster)
    {
        var registry = services.FirstOrDefault(d => d.ServiceType == typeof(HaKafkaClusterRegistry))
            ?.ImplementationInstance as HaKafkaClusterRegistry
            ?? throw new InvalidOperationException(
                "HA.Kafka: сначала зарегистрируйте модуль вызовом AddHaKafka(configuration)");
        registry.Add(cluster);
        return services;
    }

    // Полная валидация опций (спека §4.7): endpoints, интервалы, хотя бы один кластер
    private static void ValidateOptions(HaKafkaOptions options)
    {
        if (options.EtcdEndpoints.Length == 0)
            throw new InvalidOperationException("HA.Kafka: HaKafka:EtcdEndpoints не задан (нужен хотя бы один etcd endpoint)");
        if (options.Clusters.Length == 0)
            throw new InvalidOperationException("HA.Kafka: ни один kafka-кластер не заявлен (AddKafkaCluster)");
        foreach (var (name, value) in new[]
                 {
                     ("RequestTimeoutMs", options.RequestTimeoutMs),
                     ("WatchWindowMs", options.WatchWindowMs),
                     ("WatchReopenDelayMs", options.WatchReopenDelayMs),
                     ("WatchErrorDelayMs", options.WatchErrorDelayMs),
                     ("PollIntervalMs", options.PollIntervalMs),
                     ("MembersPollIntervalMs", options.MembersPollIntervalMs),
                     ("MembersMinIntervalMs", options.MembersMinIntervalMs),
                     ("BootstrapTimeoutSec", options.BootstrapTimeoutSec),
                 })
        {
            if (value <= 0)
                throw new InvalidOperationException($"HA.Kafka: HaKafka:{name} должен быть > 0 (сейчас {value})");
        }
    }
}
```

- [ ] **Step 4: Перейти к Task 5 (общий тест-цикл с Task 4, коммит после Task 5 Step 7)**

---

### Task 5: HA.Kafka — store + шина + poll-сигнальщик (спека §4.5, §4.6, Фаза Ф4)

**[repo: Puzzle]**

**Files:**
- Create: `src/PuzzleServer.Infrastructure.App.HA.Kafka/KafkaDiscoveryStore.cs`, `KafkaDiscoveryRefresher.cs`, `HaKafkaHealthCheck.cs`, `Refresh/IHaKafkaRefreshSignaler.cs`, `Refresh/HaKafkaPollRefreshSignaler.cs`
- Test: `src/PuzzleServer.UnitTests/HA/Kafka/KafkaDiscoveryStoreTests.cs`, `KafkaDiscoveryRefresherTests.cs`, `HaKafkaPollRefreshSignalerTests.cs`, `FakeEtcdClient.cs`

**Interfaces (consumes/produces):**
```csharp
// Consumes (Task 1): IEtcdClient, EtcdEndpointRotation, EtcdKv
//                    (Task 3): KafkaClusterParser.Parse, KafkaClusterData
//                    (Task 4): HaKafkaOptions, HaKafkaRefreshMode
// Produces:
namespace PuzzleServer.Infrastructure.App.HA.Kafka;

public interface IKafkaDiscoveryStore
{
    Result<KafkaClusterSnapshot> Get(string cluster);
    event Action<KafkaClusterSnapshot>? Updated;
    Task<Result<KafkaClusterSnapshot>> RefreshAsync(string cluster, CancellationToken ct);
}
namespace PuzzleServer.Infrastructure.App.HA.Kafka.Refresh;

// Сигнальщик HA.Kafka (аналог IRefreshSignaler из HA.Db, спека §4.6)
public interface IHaKafkaRefreshSignaler
{
    Task RunAsync(ChannelWriter<object> signals, CancellationToken ct);
}
```

- [ ] **Step 1: Написать FakeEtcdClient для unit-тестов**

`src/PuzzleServer.UnitTests/HA/Kafka/FakeEtcdClient.cs`:

```csharp
using System.Threading.Channels;
using PuzzleServer.Infrastructure.App;
using PuzzleServer.Infrastructure.App.HA.Etcd;

namespace PuzzleServer.UnitTests.HA.Kafka;

// Fake IEtcdClient для unit-тестов HA.Kafka.
// Range: ответы по префиксу из словаря; счётчик вызовов (проверка «Get не ходит
// в сеть», коалесценция); настраиваемая задержка и два режима отказа
// (одноразовый — fail-open одного прохода; постоянный — bootstrap/health).
// Watch: канал событий (засев EnqueueWatchEvent/EnqueueCompacted) + журнал
// вызовов WatchCalls (ассерты prefix/start_revision).
public sealed class FakeEtcdClient : IEtcdClient
{
    private readonly Dictionary<string, IReadOnlyList<EtcdKv>> _ranges = [];
    private readonly Channel<EtcdWatchEvent> _watchEvents = Channel.CreateUnbounded<EtcdWatchEvent>();
    private bool _failAllRanges;
    private bool _failNextRange;

    /// <summary>Сколько раз звёван RangeAsync (любым префиксом)</summary>
    public int RangeCalls { get; private set; }

    /// <summary>Журнал вызовов WatchAsync: (prefix, startRevision)</summary>
    public List<(string Prefix, long? StartRevision)> WatchCalls { get; } = [];

    /// <summary>Задержка каждого range-ответа, мс (медленный рефетч для коалесценции)</summary>
    public int RangeDelayMs { get; set; }

    public void SetRange(string prefix, IReadOnlyList<EtcdKv> kvs) => _ranges[prefix] = kvs;

    // Одноразовый сбой СЛЕДУЮЩЕГО range (после него — снова успех)
    public void SimulateRangeFailure() => _failNextRange = true;

    // Постоянный отказ range до выключения (bootstrap-бюджет, health-деградация)
    public void FailAllRanges(bool enabled) => _failAllRanges = enabled;

    // Засев watch-события: следующий WatchAsync вернёт его первым элементом и завершится
    public void EnqueueWatchEvent(EtcdWatchEvent evt) => _watchEvents.Writer.TryWrite(evt);

    // Засев compact-маркера (сброс ревизии → форс-рефетч)
    public void EnqueueCompacted(long compactRevision)
        => _watchEvents.Writer.TryWrite(new EtcdWatchEvent(WatchEventType.Compacted, null, compactRevision));

    public async Task<Result<IReadOnlyList<EtcdKv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
    {
        RangeCalls++;
        if (RangeDelayMs > 0)
            await Task.Delay(RangeDelayMs, ct);
        if (_failAllRanges || _failNextRange)
        {
            if (!_failAllRanges)
                _failNextRange = false; // одноразовый режим гасим; постоянный живёт до выключения
            return Result<IReadOnlyList<EtcdKv>>.Failed(new IOException("etcd недоступен (fake)"));
        }

        // Префикс кластера: /kafka/clusters/<C>/ — отдаём заготовленный набор
        return _ranges.TryGetValue(prefix, out var kvs)
            ? Result<IReadOnlyList<EtcdKv>>.Success(kvs)
            : Result<IReadOnlyList<EtcdKv>>.Success([]);
    }

    public Task<Result<IReadOnlyList<EtcdMember>>> MembersAsync(string endpoint, CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyList<EtcdMember>>.Success([]));

    public async IAsyncEnumerable<EtcdWatchEvent> WatchAsync(
        string endpoint, string prefix, long? startRevision,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        WatchCalls.Add((prefix, startRevision));
        // Ждём засеянное событие или отмену окна; поллинг вместо долгого ожидания —
        // отмена окна (ct) бросает OCE наружу: сигнальщик гасит её при закрытии окна
        while (!ct.IsCancellationRequested)
        {
            if (_watchEvents.Reader.TryRead(out var evt))
            {
                yield return evt;
                yield break;
            }
            await Task.Delay(20, ct);
        }
    }
}
```

- [ ] **Step 2: Написать падающие тесты store, шины и poll-сигнальщика**

`src/PuzzleServer.UnitTests/HA/Kafka/KafkaDiscoveryStoreTests.cs`:

```csharp
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using PuzzleServer.Infrastructure.App.HA.Etcd;
using PuzzleServer.Infrastructure.App.HA.Kafka;
using PuzzleServer.Infrastructure.App.HA.Kafka.Model;

namespace PuzzleServer.UnitTests.HA.Kafka;

// KafkaDiscoveryStore: кэш + событие только при ИЗМЕНЕНИИ СОДЕРЖИМОГО + RefreshAsync
// + fail-open (спека §4.5, §2 п.7, §8 п.8)
public class KafkaDiscoveryStoreTests
{
    // Заготовка kv-набора: креды + endpoints + топик с configs (парсер создаёт
    // НОВЫЕ экземпляры List/Dictionary на каждый рефетч — как в бою)
    private static IReadOnlyList<EtcdKv> ClusterKvs(string endpoints = "h1:9094", long modRevision = 10)
        => new[]
        {
            new EtcdKv("/kafka/clusters/events/config", """{"brokers":3,"created_unix":1}""", modRevision),
            new EtcdKv("/kafka/clusters/events/endpoints", endpoints, modRevision),
            new EtcdKv("/kafka/clusters/events/app_user", "app", modRevision),
            new EtcdKv("/kafka/clusters/events/app_password", "abcdefghijklmnopqrstuvwxyz012345", modRevision),
            new EtcdKv("/kafka/clusters/events/topics/orders",
                """{"partitions":12,"replication_factor":3,"configs":{"retention.ms":"604800000"},"synced_unix":1,"missing":false}""", modRevision),
        };

    private static KafkaDiscoveryStore BuildStore(FakeEtcdClient client, params string[] clusters)
    {
        var options = Microsoft.Extensions.Options.Options.Create(new HaKafkaOptions { Clusters = clusters });
        return new KafkaDiscoveryStore(
            client, new EtcdEndpointRotation(["http://etcd:2379"]), options,
            NullLoggerFactory.Instance.CreateLogger<KafkaDiscoveryStore>());
    }

    [Fact]
    public async Task RefreshAsync_ThenGet_ReturnsSnapshotWithoutNetwork()
    {
        // Arrange
        var client = new FakeEtcdClient();
        client.SetRange("/kafka/clusters/events/", ClusterKvs());
        var store = BuildStore(client, "events");

        // Act
        var refresh = await store.RefreshAsync("events", CancellationToken.None);
        var rangeCallsAfterRefresh = client.RangeCalls;
        var get = store.Get("events");

        // Assert
        refresh.IsSuccess.Should().BeTrue();
        get.IsSuccess.Should().BeTrue();
        get.Value.BootstrapServers.Should().Be("h1:9094");
        get.Value.GetClientConfig().Should().NotBeNull();
        get.Value.TopicNames.Should().Equal("orders");
        client.RangeCalls.Should().Be(rangeCallsAfterRefresh); // Get не ходит в сеть (спека §8 п.4)
    }

    [Fact]
    public async Task Get_Failed_ForUndeclaredCluster()
    {
        // Arrange
        var store = BuildStore(new FakeEtcdClient(), "events");

        // Act
        var get = store.Get("other");

        // Assert
        get.IsSuccess.Should().BeFalse();
        (await store.RefreshAsync("other", CancellationToken.None)).IsSuccess.Should().BeFalse();
    }

    [Fact]
    public async Task Updated_FiresOnlyOnContentChange()
    {
        // Arrange
        var client = new FakeEtcdClient();
        client.SetRange("/kafka/clusters/events/", ClusterKvs());
        var store = BuildStore(client, "events");
        var fired = 0;
        store.Updated += _ => fired++;

        // Act
        await store.RefreshAsync("events", CancellationToken.None);   // первое появление → событие
        await store.RefreshAsync("events", CancellationToken.None);   // тот же ответ → НЕТ события
        client.SetRange("/kafka/clusters/events/", ClusterKvs("h9:9094", 11));
        await store.RefreshAsync("events", CancellationToken.None);   // изменение → событие

        // Assert
        fired.Should().Be(2);
        store.Get("events").Value.BootstrapServers.Should().Be("h9:9094");
    }

    [Fact]
    public async Task Updated_NotFired_WhenSameTopicsRecreated_AsNewCollectionInstances()
    {
        // Arrange — ГЛАВНЫЙ тест структурного сравнения: содержимое одинаковое,
        // но каждый рефетч создаёт НОВЫЕ экземпляры List/Dictionary (парсер).
        // record-== для коллекций ссылочный — потому store сравнивает содержимое
        // через SameContent (спека §2 п.7, §4.5; паттерн ha-db docs/01.17)
        var client = new FakeEtcdClient();
        var store = BuildStore(client, "events");
        var fired = 0;
        store.Updated += _ => fired++;

        // Act — два рефетча: kv-набор пересобирается заново (новые EtcdKv-массивы,
        // новые словари configs у topics/orders)
        client.SetRange("/kafka/clusters/events/", ClusterKvs());
        await store.RefreshAsync("events", CancellationToken.None);
        client.SetRange("/kafka/clusters/events/", ClusterKvs());
        await store.RefreshAsync("events", CancellationToken.None);

        // Assert — событие ровно один раз (только первое появление снапшота)
        fired.Should().Be(1);
        store.Get("events").Value.TopicNames.Should().Equal("orders");
    }

    [Fact]
    public async Task RefreshAsync_Failure_KeepsLastSnapshot_FailOpen()
    {
        // Arrange
        var client = new FakeEtcdClient();
        client.SetRange("/kafka/clusters/events/", ClusterKvs());
        var store = BuildStore(client, "events");
        await store.RefreshAsync("events", CancellationToken.None);

        // Act — одноразовый сбой следующего range
        client.SimulateRangeFailure();
        var failed = await store.RefreshAsync("events", CancellationToken.None);
        var get = store.Get("events");

        // Assert — fail-open: кэш живёт (спека §8 п.10)
        failed.IsSuccess.Should().BeFalse();
        get.IsSuccess.Should().BeTrue();
        get.Value.BootstrapServers.Should().Be("h1:9094");
    }

    [Fact]
    public async Task Updated_SignalsPasswordRotation()
    {
        // Arrange — сценарий ротации §16-H фазы B: put NEW → событие, новый пароль
        var client = new FakeEtcdClient();
        client.SetRange("/kafka/clusters/events/", ClusterKvs());
        var store = BuildStore(client, "events");
        await store.RefreshAsync("events", CancellationToken.None);
        KafkaClientConfig? latest = null;
        store.Updated += s => latest = s.GetClientConfig();

        // Act
        client.SetRange("/kafka/clusters/events/", new[]
        {
            new EtcdKv("/kafka/clusters/events/config", """{"brokers":3,"created_unix":1}""", 12),
            new EtcdKv("/kafka/clusters/events/endpoints", "h1:9094", 12),
            new EtcdKv("/kafka/clusters/events/app_user", "app", 12),
            new EtcdKv("/kafka/clusters/events/app_password", "ZYXWVUTSRQPONMLKJIHGFEDCBA987654", 12),
            new EtcdKv("/kafka/clusters/events/topics/orders",
                """{"partitions":12,"replication_factor":3,"configs":{"retention.ms":"604800000"},"synced_unix":1,"missing":false}""", 12),
        });
        await store.RefreshAsync("events", CancellationToken.None);

        // Assert
        latest.Should().NotBeNull();
        latest!.SaslPassword.Should().Be("ZYXWVUTSRQPONMLKJIHGFEDCBA987654");
    }
}
```

`src/PuzzleServer.UnitTests/HA/Kafka/KafkaDiscoveryRefresherTests.cs`:

```csharp
using System.Threading.Channels;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using PuzzleServer.Infrastructure.App.HA.Etcd;
using PuzzleServer.Infrastructure.App.HA.Kafka;
using PuzzleServer.Infrastructure.App.HA.Kafka.Refresh;

namespace PuzzleServer.UnitTests.HA.Kafka;

// Шина KafkaDiscoveryRefresher: bootstrap с бюджетом, коалесценция сигналов,
// health-семантика (StatusError/Working/Inited) — спека §4.6, §7, §8 п.4/п.10
public class KafkaDiscoveryRefresherTests
{
    // Ручной сигнальщик: шина получает сигналы только по команде теста
    private sealed class ManualSignaler : IHaKafkaRefreshSignaler
    {
        private ChannelWriter<object>? _writer;

        public bool Ready => _writer is not null;

        public Task RunAsync(ChannelWriter<object> signals, CancellationToken ct)
        {
            _writer = signals;
            return Task.Delay(Timeout.Infinite, ct);
        }

        public void Signal() => _writer?.TryWrite(new object());
    }

    private static (KafkaDiscoveryRefresher Refresher, FakeEtcdClient Client, ManualSignaler Signaler) Build(
        Action<HaKafkaOptions>? tune = null)
    {
        var client = new FakeEtcdClient();
        var options = new HaKafkaOptions { Clusters = ["events"] };
        tune?.Invoke(options);
        var store = new KafkaDiscoveryStore(
            client, new EtcdEndpointRotation(["http://etcd:2379"]),
            Microsoft.Extensions.Options.Options.Create(options),
            NullLoggerFactory.Instance.CreateLogger<KafkaDiscoveryStore>());
        var signaler = new ManualSignaler();
        var refresher = new KafkaDiscoveryRefresher(
            store, signaler, Microsoft.Extensions.Options.Options.Create(options),
            NullLoggerFactory.Instance.CreateLogger<KafkaDiscoveryRefresher>());
        return (refresher, client, signaler);
    }

    private static IReadOnlyList<EtcdKv> ClusterKvs(string endpoints)
        => new[] { new EtcdKv("/kafka/clusters/events/endpoints", endpoints, 1) };

    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline && !condition())
            await Task.Delay(20);
    }

    [Fact]
    public async Task StartAsync_WhenEtcdDown_BootstrapDegradesButStarts()
    {
        // Arrange — etcd постоянно недоступен: старт не падает, Inited=false,
        // health деградирует (спека §8 п.4/п.10)
        var (refresher, client, _) = Build(o => o.BootstrapTimeoutSec = 1);
        client.FailAllRanges(true);

        // Act
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        await refresher.StartAsync(cts.Token);
        var stop = () => refresher.StopAsync(CancellationToken.None);

        // Assert — bootstrap-цикл не собрал ни одного снапшота, но не бросил
        refresher.Inited.Should().BeFalse();
        refresher.Working.Should().BeFalse();
        refresher.StatusError.IsSuccess.Should().BeFalse("после ≥2 провалов подряд StatusError = Failed");
        await stop.Should().CompleteWithinAsync(TimeSpan.FromSeconds(2));
    }

    [Fact]
    public async Task ExecuteAsync_SignalRefetches_AndSnapshotFollowsChanges()
    {
        // Arrange
        var (refresher, client, signaler) = Build();
        client.SetRange("/kafka/clusters/events/", ClusterKvs("h1:9094"));

        // Act
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var run = refresher.StartAsync(cts.Token);
        await WaitUntilAsync(() => signaler.Ready, TimeSpan.FromSeconds(2));
        await WaitUntilAsync(() => refresher.Inited, TimeSpan.FromSeconds(2)); // bootstrap-проход
        var snapshot1 = refresher.Store.Get("events").Value.BootstrapServers;
        client.SetRange("/kafka/clusters/events/", ClusterKvs("h2:9094"));
        signaler.Signal();
        await WaitUntilAsync(
            () => refresher.Store.Get("events").Value.BootstrapServers == "h2:9094",
            TimeSpan.FromSeconds(2));
        var snapshot2 = refresher.Store.Get("events").Value.BootstrapServers;
        cts.Cancel();
        await run;

        // Assert — сигнал шины → рефетч → новый снапшот (спека §4.6)
        snapshot1.Should().Be("h1:9094");
        snapshot2.Should().Be("h2:9094");
    }

    [Fact]
    public async Task SignalStorm_DuringSlowPass_CoalescesIntoSingleRefetch()
    {
        // Arrange — медленный рефетч (200 мс): пачка сигналов за время прохода
        // должна схлопнуться в ОДИН следующий проход (спека §4.6, §7)
        var (refresher, client, signaler) = Build(o => { });
        client.RangeDelayMs = 200;
        client.SetRange("/kafka/clusters/events/", ClusterKvs("h1:9094"));

        // Act
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(8));
        var run = refresher.StartAsync(cts.Token);
        await WaitUntilAsync(() => signaler.Ready, TimeSpan.FromSeconds(2));
        var afterBootstrap = client.RangeCalls; // bootstrap = 1 проход
        signaler.Signal();                      // проход 2 (200 мс)
        await Task.Delay(50, cts.Token);        // идёт проход 2 — штормим пачкой
        for (var i = 0; i < 5; i++)
            signaler.Signal();
        await WaitUntilAsync(() => client.RangeCalls >= afterBootstrap + 2, TimeSpan.FromSeconds(3));
        await Task.Delay(400, cts.Token);       // если бы пачка НЕ дренажилась — прошли бы ещё 5

        // Assert — ровно один дополнительный проход на всю пачку
        client.RangeCalls.Should().Be(afterBootstrap + 2);
        cts.Cancel();
        await run;
    }

    [Fact]
    public async Task StatusError_ResetsAfterSuccessfulPass()
    {
        // Arrange — два провальных прохода подряд → StatusError Failed;
        // возврат etcd + сигнал → успех сбрасывает здоровье (спека §4.6, §8 п.10)
        var (refresher, client, signaler) = Build();
        client.SetRange("/kafka/clusters/events/", ClusterKvs("h1:9094"));

        // Act — деградация требует ДВУХ провальных проходов подряд (порог >= 2
        // в OnRefreshFailure): первый сигнал — первый провал (дожидаемся его по
        // счётчику RangeCalls), второй сигнал — второй провал → StatusError Failed
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(8));
        var run = refresher.StartAsync(cts.Token);
        await WaitUntilAsync(() => signaler.Ready, TimeSpan.FromSeconds(2));
        await WaitUntilAsync(() => refresher.Inited, TimeSpan.FromSeconds(2)); // bootstrap-проход успешен
        var passesBeforeFailure = client.RangeCalls;
        client.FailAllRanges(true);
        signaler.Signal(); // провальный проход 1: _consecutiveFailures == 1 — порог ещё НЕ достигнут
        await WaitUntilAsync(() => client.RangeCalls > passesBeforeFailure, TimeSpan.FromSeconds(2));
        signaler.Signal(); // провальный проход 2 → StatusError = Failed
        await WaitUntilAsync(() => !refresher.StatusError.IsSuccess, TimeSpan.FromSeconds(2));
        var degraded = refresher.StatusError;
        client.FailAllRanges(false);
        signaler.Signal(); // успешный проход → сброс здоровья
        await WaitUntilAsync(() => refresher.Inited && refresher.StatusError.IsSuccess, TimeSpan.FromSeconds(2));
        var healthy = (refresher.StatusError, refresher.Inited, refresher.Working);
        cts.Cancel();
        await run;

        // Assert
        degraded.IsSuccess.Should().BeFalse();
        healthy.Item1.IsSuccess.Should().BeTrue();
        healthy.Inited.Should().BeTrue();
        healthy.Working.Should().BeTrue();
    }
}
```

`src/PuzzleServer.UnitTests/HA/Kafka/HaKafkaPollRefreshSignalerTests.cs`:

```csharp
using System.Threading.Channels;
using FluentAssertions;
using PuzzleServer.Infrastructure.App.HA.Kafka;
using PuzzleServer.Infrastructure.App.HA.Kafka.Refresh;

namespace PuzzleServer.UnitTests.HA.Kafka;

// Poll-сигнальщик (спека §4.6, §7): тик PollIntervalMs → сигнал; отмена — выход
public class HaKafkaPollRefreshSignalerTests
{
    [Fact]
    public async Task RunAsync_WritesSignals_EveryPollInterval()
    {
        // Arrange
        var options = new HaKafkaOptions { PollIntervalMs = 50 };
        var signaler = new HaKafkaPollRefreshSignaler(options);
        var channel = Channel.CreateUnbounded<object>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

        // Act — ждём первый сигнал и ещё один за бюджетом окна
        var run = signaler.RunAsync(channel.Writer, cts.Token);
        using var readCts = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));
        var first = await channel.Reader.ReadAsync(readCts.Token);
        var second = await channel.Reader.ReadAsync(cts.Token);

        // Assert — сигналы идут с интервалом тика (спека §4.7: PollIntervalMs)
        first.Should().NotBeNull();
        second.Should().NotBeNull();
        cts.Cancel();
        await run;
    }

    [Fact]
    public async Task RunAsync_ExitsPromptly_OnCancellation_WithoutSignals()
    {
        // Arrange
        var options = new HaKafkaOptions { PollIntervalMs = 50 };
        var signaler = new HaKafkaPollRefreshSignaler(options);
        var channel = Channel.CreateUnbounded<object>();
        using var cts = new CancellationTokenSource();

        // Act — отмена ДО первого тика: завершение без зависаний и без сигналов
        cts.Cancel();
        var run = signaler.RunAsync(channel.Writer, cts.Token);
        var completed = await Task.WhenAny(run, Task.Delay(TimeSpan.FromSeconds(1)));

        // Assert
        completed.Should().Be(run, "отмена ct должна завершать сигнальщик немедленно");
        channel.Reader.TryRead(out var signal).Should().BeFalse();
        signal.Should().BeNull();
    }
}
```

- [ ] **Step 3: Прогнать — убедиться, что падают**

```bash
dotnet test src/PuzzleServer.Api.slnx --filter "FullyQualifiedName~UnitTests.HA.Kafka"
```

Expected: FAIL — `KafkaDiscoveryStore`/`KafkaDiscoveryRefresher`/`HaKafkaPollRefreshSignaler` не существуют; ModuleRegistrationTests тоже FAIL (нет типов).

- [ ] **Step 4: Реализовать store, шину, сигнальщик, health**

`Refresh/IHaKafkaRefreshSignaler.cs`:

```csharp
using System.Threading.Channels;

namespace PuzzleServer.Infrastructure.App.HA.Kafka.Refresh;

// Источник сигнала «пора рефрешить» для шины KafkaDiscoveryRefresher.
// Режим (HaKafka:Mode) выбирает реализацию при старте; сигнальщик НЕ делает
// range-рефетчей — только сигналит (рефетч всегда идёт через IKafkaDiscoveryStore).
public interface IHaKafkaRefreshSignaler
{
    Task RunAsync(ChannelWriter<object> signals, CancellationToken ct);
}
```

`Refresh/HaKafkaPollRefreshSignaler.cs`:

```csharp
using System.Threading.Channels;

namespace PuzzleServer.Infrastructure.App.HA.Kafka.Refresh;

// Режим Poll: PeriodicTimer → сигнал на каждый тик (полный рефетч префиксов).
public sealed class HaKafkaPollRefreshSignaler(HaKafkaOptions options) : IHaKafkaRefreshSignaler
{
    public async Task RunAsync(ChannelWriter<object> signals, CancellationToken ct)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(options.PollIntervalMs));
        while (await timer.WaitForNextTickAsync(ct))
            signals.TryWrite(new object());
    }
}
```

`HaKafkaHealthCheck.cs`:

```csharp
using PuzzleServer.Infrastructure.App.HealthChecks;

namespace PuzzleServer.Infrastructure.App.HA.Kafka;

// Health-check шины актуализации (паттерн HaDbCheck: пустой класс-наследник)
public class HaKafkaHealthCheck(KafkaDiscoveryRefresher service)
    : HealthCheckAbstract<KafkaDiscoveryRefresher>(service)
{
}
```

`KafkaDiscoveryStore.cs`:

```csharp
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PuzzleServer.Infrastructure.App.DI;
using PuzzleServer.Infrastructure.App.HA.Etcd;
using PuzzleServer.Infrastructure.App.HA.Kafka.Model;
using PuzzleServer.Infrastructure.App.HA.Kafka.Parsing;

namespace PuzzleServer.Infrastructure.App.HA.Kafka;

// Публичный контракт кэша дискавери (спека §4.5)
public interface IKafkaDiscoveryStore
{
    Result<KafkaClusterSnapshot> Get(string cluster);
    event Action<KafkaClusterSnapshot>? Updated;
    Task<Result<KafkaClusterSnapshot>> RefreshAsync(string cluster, CancellationToken ct);
}

// Кэш снапшотов по кластерам: Get — мгновенно, RefreshAsync — полный рефетч
// с атомарной заменой и событием только при фактическом изменении содержимого.
[InjectAsSingleton]
public sealed class KafkaDiscoveryStore(
    IEtcdClient etcdClient,
    EtcdEndpointRotation rotation,
    IOptions<HaKafkaOptions> options,
    ILogger<KafkaDiscoveryStore> logger) : IKafkaDiscoveryStore
{
    private readonly object _sync = new();
    private readonly SemaphoreSlim _refreshGate = new(1, 1);
    private readonly Dictionary<string, KafkaClusterSnapshot> _snapshots = new();

    public event Action<KafkaClusterSnapshot>? Updated;

    public Result<KafkaClusterSnapshot> Get(string cluster)
    {
        lock (_sync)
        {
            if (!_snapshots.TryGetValue(cluster, out var snapshot))
                return Result<KafkaClusterSnapshot>.Failed(new HaKafkaException(
                    IsDeclared(cluster)
                        ? $"снапшот kafka-кластера {cluster} ещё не готов (bootstrap не завершён)"
                        : $"kafka-кластер {cluster} не заявлен (AddKafkaCluster)"));
            return Result<KafkaClusterSnapshot>.Success(snapshot);
        }
    }

    public async Task<Result<KafkaClusterSnapshot>> RefreshAsync(string cluster, CancellationToken ct)
    {
        if (!IsDeclared(cluster))
            return Result<KafkaClusterSnapshot>.Failed(
                new HaKafkaException($"kafka-кластер {cluster} не заявлен (AddKafkaCluster)"));

        await _refreshGate.WaitAsync(ct);
        try
        {
            var fetched = await FetchAsync(cluster, ct);
            fetched.Apply(snapshot => Publish(cluster, snapshot));
            return fetched;
        }
        finally
        {
            _refreshGate.Release();
        }
    }

    private bool IsDeclared(string cluster)
        => options.Value.Clusters.Contains(cluster);

    // Один проход = один range на одном endpoint (консистентность ревизии), ротация при отказе
    private async Task<Result<KafkaClusterSnapshot>> FetchAsync(string cluster, CancellationToken ct)
    {
        var endpoint = rotation.GetActive();
        var prefix = $"/kafka/clusters/{cluster}/";

        var kvs = await etcdClient.RangeAsync(endpoint, prefix, ct);
        if (!kvs.IsSuccess)
        {
            rotation.ReportFailure(endpoint);
            logger.LogWarning(kvs.Error, "HA.Kafka: range {Prefix} на {Endpoint} провалился", prefix, endpoint);
            return Result<KafkaClusterSnapshot>.Failed(kvs.Error!);
        }

        rotation.ReportSuccess(endpoint);
        var revision = kvs.Value.Count == 0 ? 0 : kvs.Value.Max(kv => kv.ModRevision);
        var data = KafkaClusterParser.Parse(cluster, kvs.Value, out var parseErrors, out var unknownKeys);
        foreach (var error in parseErrors)
            logger.LogWarning("HA.Kafka: кластер {Cluster}: {Error}", cluster, error);
        if (unknownKeys.Count > 0)
            logger.LogWarning("HA.Kafka: кластер {Cluster}: неизвестные ключи ({Count}): {Keys}",
                cluster, unknownKeys.Count, string.Join(", ", unknownKeys));

        return Result<KafkaClusterSnapshot>.Success(new KafkaClusterSnapshot(
            cluster, data.State, data.BootstrapServers, data.App, data.Topics,
            DateTimeOffset.UtcNow, revision));
    }

    // Замена снапшота; Updated стреляет только при изменении содержимого.
    // FetchedAtUtc/Revision обновляются всегда — start_revision следующего watch-окна.
    private void Publish(string cluster, KafkaClusterSnapshot snapshot)
    {
        Action<KafkaClusterSnapshot>? handlers = null;
        lock (_sync)
        {
            var changed = !_snapshots.TryGetValue(cluster, out var old) || !SameContent(old, snapshot);
            _snapshots[cluster] = snapshot;
            if (changed)
                handlers = Updated;
        }

        if (handlers is null)
            return;

        foreach (var handler in handlers.GetInvocationList().Cast<Action<KafkaClusterSnapshot>>())
        {
            try
            {
                handler(snapshot);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "HA.Kafka: подписчик Updated кластера {Cluster} бросил исключение", cluster);
            }
        }
    }

    // СТРУКТУРНОЕ сравнение содержимого без FetchedAtUtc/Revision (спека §4.5, §2 п.7).
    // ВАЖНО: record-== здесь НЕ годится — компилятор сравнивает коллекционные поля
    // (Topics: IReadOnlyList, Configs: IReadOnlyDictionary) через EqualityComparer
    // по ССЫЛКЕ, и каждый рефетч (новые экземпляры от парсера) давал бы «изменение».
    // Паттерн ha-db (docs/01.17): «Сравнение снапшотов — только SameContent,
    // структурное, включая списки». Topics сравниваются поэлементно (реестр
    // отсортирован по имени парсером), Configs — пословно.
    private static bool SameContent(KafkaClusterSnapshot a, KafkaClusterSnapshot b)
    {
        if (a.Cluster != b.Cluster
            || a.State != b.State
            || a.BootstrapServers != b.BootstrapServers
            || a.App != b.App                       // record со строковыми полями — структурно
            || a.Topics.Count != b.Topics.Count)
            return false;

        for (var i = 0; i < a.Topics.Count; i++)
            if (!TopicEquals(a.Topics[i], b.Topics[i]))
                return false;

        return true;
    }

    private static bool TopicEquals(KafkaTopicInfo x, KafkaTopicInfo y)
        => x.Name == y.Name
           && x.Partitions == y.Partitions
           && x.ReplicationFactor == y.ReplicationFactor
           && ConfigsEquals(x.Configs, y.Configs);

    private static bool ConfigsEquals(IReadOnlyDictionary<string, string>? x, IReadOnlyDictionary<string, string>? y)
    {
        if (x is null || y is null)
            return x is null && y is null;
        if (x.Count != y.Count)
            return false;
        foreach (var (key, value) in x)
            if (!y.TryGetValue(key, out var other)
                || !string.Equals(value, other, StringComparison.Ordinal))
                return false;
        return true;
    }
}
```

`KafkaDiscoveryRefresher.cs`:

```csharp
using System.Threading.Channels;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PuzzleServer.Infrastructure.App.DI;
using PuzzleServer.Infrastructure.App.HealthChecks;
using PuzzleServer.Infrastructure.App.HA.Kafka.Model;
using PuzzleServer.Infrastructure.App.HA.Kafka.Refresh;

namespace PuzzleServer.Infrastructure.App.HA.Kafka;

// Общая шина актуализации (спека §4.6): bootstrap при старте + сигналы
// IHaKafkaRefreshSignaler → рефетч всех кластеров через IKafkaDiscoveryStore.
// Коалесценция: сигналы, накопленные во время прохода, дренируются после него.
[InjectAsSingleton]
public sealed class KafkaDiscoveryRefresher(
    IKafkaDiscoveryStore store,
    IHaKafkaRefreshSignaler signaler,
    IOptions<HaKafkaOptions> options,
    ILogger<KafkaDiscoveryRefresher> logger) : BackgroundService, IHealthCheckService
{
    // Для тестов (unit-сценарии шины без DI)
    public IKafkaDiscoveryStore Store => store;

    private DateTimeOffset _lastSuccessUtc;
    private int _consecutiveFailures;
    private bool _firstSuccessLogged;

    // Inited — только после хотя бы одного успешного рефетча (спека §4.6)
    public bool Inited { get; private set; }

    public bool Working =>
        Inited && _consecutiveFailures == 0
        && DateTimeOffset.UtcNow - _lastSuccessUtc < WorkingWindow();

    public Result StatusError { get; private set; } = Result.Success();

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        // Bootstrap: немедленный рефетч всех кластеров с бюджетом BootstrapTimeoutSec;
        // провал не роняет старт приложения (кэш соберут следующие сигналы).
        var deadline = DateTimeOffset.UtcNow.AddSeconds(options.Value.BootstrapTimeoutSec);
        while (DateTimeOffset.UtcNow < deadline && !cancellationToken.IsCancellationRequested)
        {
            if (await RefreshAllAsync(cancellationToken))
                break;
            await Task.Delay(250, cancellationToken);
        }

        if (!Inited)
            logger.LogWarning("HA.Kafka: bootstrap не собрал ни одного снапшота за {Sec} c — продолжим в фоне",
                options.Value.BootstrapTimeoutSec);
        await base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var channel = Channel.CreateUnbounded<object>();
        var signalerTask = signaler.RunAsync(channel.Writer, stoppingToken);
        try
        {
            await foreach (var _ in channel.Reader.ReadAllAsync(stoppingToken))
            {
                await RefreshAllAsync(stoppingToken);
                // Коалесценция: пачка сигналов за время прохода = один проход
                while (channel.Reader.TryRead(out var __))
                { }
            }
        }
        catch (OperationCanceledException)
        {
            // штатная остановка хоста
        }
        finally
        {
            await signalerTask.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        }
    }

    // Окно «здоровья»: 3 интервала режима (спека §4.6)
    private TimeSpan WorkingWindow()
        => options.Value.Mode == HaKafkaRefreshMode.Poll
            ? TimeSpan.FromMilliseconds(3 * Math.Max(options.Value.PollIntervalMs, 1))
            : TimeSpan.FromMilliseconds(3 * (options.Value.WatchWindowMs + options.Value.WatchReopenDelayMs + 1000));

    private async Task<bool> RefreshAllAsync(CancellationToken ct)
    {
        var allOk = true;
        foreach (var cluster in options.Value.Clusters)
        {
            var result = await store.RefreshAsync(cluster, ct);
            if (result.IsSuccess)
                OnRefreshSuccess(cluster, result.Value);
            else
            {
                allOk = false;
                OnRefreshFailure(cluster, result.Error!);
            }
        }
        return allOk;
    }

    private void OnRefreshSuccess(string cluster, KafkaClusterSnapshot snapshot)
    {
        _consecutiveFailures = 0;
        _lastSuccessUtc = DateTimeOffset.UtcNow;
        StatusError = Result.Success();
        var wasInited = Inited;
        Inited = true;
        if (!_firstSuccessLogged)
        {
            _firstSuccessLogged = true;
            logger.LogInformation(
                "HA.Kafka: первый снапшот кластера {Cluster}: топиков {Topics}, ревизия {Revision}",
                cluster, snapshot.Topics.Count, snapshot.Revision);
        }
        else if (!wasInited)
        {
            logger.LogInformation("HA.Kafka: первый успешный снапшот кластера {Cluster} после провалов", cluster);
        }
    }

    private void OnRefreshFailure(string cluster, Exception error)
    {
        _consecutiveFailures++;
        logger.LogWarning(error, "HA.Kafka: рефетч кластера {Cluster} провалился (подряд: {Count})", cluster,
            _consecutiveFailures);
        if (_consecutiveFailures >= 2)
            StatusError = Result.Failed(new HaKafkaException(
                $"последние {_consecutiveFailures} рефетчей провалились; etcd недоступен?"));
    }
}
```

- [ ] **Step 5: Прогнать тесты Task 4 + Task 5 — убедиться, что проходят**

```bash
dotnet build src/PuzzleServer.Api.slnx
dotnet test src/PuzzleServer.Api.slnx --filter "FullyQualifiedName~UnitTests.HA.Kafka"
```

Expected: PASS — ModelTests (7 кейсов), KafkaClusterParserTests (9), ModuleRegistrationTests (включая `Mode_Poll_ResolvesPollSignaler`), KafkaDiscoveryStoreTests (6, включая `Updated_NotFired_WhenSameTopicsRecreated_AsNewCollectionInstances`), KafkaDiscoveryRefresherTests (4), HaKafkaPollRefreshSignalerTests (2).

- [ ] **Step 6: Убедиться, что временная poll-заглушка WatchLongPoll-ветки на месте (из Task 4 Step 3)**

В `ModuleExtensions.AddHaKafka` switch-ветка `HaKafkaRefreshMode.WatchLongPoll` ДОЛЖНА временно возвращать `new HaKafkaPollRefreshSignaler(options)` (комментарий «временно (Task 6)»). Именно в этом виде коммитится Ф4: каждый режим даёт рабочую семантику «сигнал → полный рефетч», watch-оптимизация появляется в Ф5/Task 6 (соответствует спеке §5: Ф4 = шина + Poll-сигнальщик, Ф5 = watch-клиент и режим WatchLongPoll).

- [ ] **Step 7: Commit (общий для Task 4 + Task 5)**

```bash
git add -A
git commit -m "feat: HA.Kafka store/refresher/poll-signaler + module registration with fluent cluster claims (t05 Ф4; watch — Task 6)"
```

---

### Task 6: HA.Kafka — WatchLongPoll-сигнальщик (спека §4.6, Фаза Ф5)

**[repo: Puzzle]**

**Files:**
- Create: `src/PuzzleServer.Infrastructure.App.HA.Kafka/Refresh/HaKafkaWatchLongPollSignaler.cs`
- Modify: `src/PuzzleServer.Infrastructure.App.HA.Kafka/ModuleExtensions.cs` (возврат полной WatchLongPoll-ветки — Step 4)
- Test: `src/PuzzleServer.UnitTests/HA/Kafka/HaKafkaWatchLongPollSignalerTests.cs`
- Test (modify): `src/PuzzleServer.UnitTests/HA/Kafka/ModuleRegistrationTests.cs` (+ тест Mode=WatchLongPoll — Step 1)

**Interfaces (consumes/produces):**
```csharp
// Consumes (Task 1): IEtcdClient.WatchAsync, EtcdEndpointRotation
//                    (Task 5): IKafkaDiscoveryStore (start_revision = snapshot Revision),
//                               FakeEtcdClient.EnqueueWatchEvent/EnqueueCompacted/WatchCalls
// Produces: HaKafkaWatchLongPollSignaler(IEtcdClient, EtcdEndpointRotation,
//           IKafkaDiscoveryStore, IOptions<HaKafkaOptions>, ILogger<...>) : IHaKafkaRefreshSignaler
```

- [ ] **Step 1: Написать падающие тесты сигнальщика**

`src/PuzzleServer.UnitTests/HA/Kafka/HaKafkaWatchLongPollSignalerTests.cs`:

```csharp
using System.Threading.Channels;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using PuzzleServer.Infrastructure.App.HA.Etcd;
using PuzzleServer.Infrastructure.App.HA.Kafka;
using PuzzleServer.Infrastructure.App.HA.Kafka.Refresh;

namespace PuzzleServer.UnitTests.HA.Kafka;

// WatchLongPoll-сигнальщик: событие watch → сигнал; окно истекло → переоткрытие
// с start_revision = Revision снапшота; Compacted → сигнал; сбой → ротация
// активного endpoint + пауза WatchErrorDelayMs (спека §4.6, §8 п.8)
public class HaKafkaWatchLongPollSignalerTests
{
    private static (HaKafkaWatchLongPollSignaler Signaler, FakeEtcdClient Client, KafkaDiscoveryStore Store) Build(
        string[] clusters, Action<HaKafkaOptions>? tune = null)
    {
        var options = new HaKafkaOptions
        {
            Clusters = clusters, WatchWindowMs = 200, WatchReopenDelayMs = 10, WatchErrorDelayMs = 50,
        };
        tune?.Invoke(options);
        var client = new FakeEtcdClient();
        var store = new KafkaDiscoveryStore(
            client, new EtcdEndpointRotation(["http://etcd:2379"]),
            Options.Create(options), NullLoggerFactory.Instance.CreateLogger<KafkaDiscoveryStore>());
        return (new HaKafkaWatchLongPollSignaler(
            client, new EtcdEndpointRotation(["http://etcd:2379"]), store, Options.Create(options),
            NullLoggerFactory.Instance.CreateLogger<HaKafkaWatchLongPollSignaler>()), client, store);
    }

    // Читает один сигнал с бюджетом; при таймауте бросает (тест падает с понятной причиной)
    private static async Task<DateTime> ReadSignalWithTimeAsync(ChannelReader<object> reader, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        await reader.ReadAsync(cts.Token);
        return DateTime.UtcNow;
    }

    private static async Task<bool> WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline && !condition())
            await Task.Delay(20);
        return condition();
    }

    [Fact]
    public async Task WindowTimeout_WithoutEvents_ProducesNoSignals()
    {
        // Arrange — окна крутятся (200 мс) без событий: TimedOut не сигналит (спека §4.6)
        var (signaler, client, _) = Build(["events"]);
        var channel = Channel.CreateUnbounded<object>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var run = signaler.RunAsync(channel.Writer, cts.Token);
        try
        {
            // Act — ждём несколько окон (600 мс) без засеянных событий
            await Task.Delay(600, cts.Token);
            channel.Reader.TryRead(out var signal).Should().BeFalse();

            // Assert — окна реально переоткрывались, но сигналов нет
            client.WatchCalls.Count.Should().BeGreaterThanOrEqualTo(2);
            signal.Should().BeNull();
        }
        finally
        {
            cts.Cancel();
            await run;
        }
    }

    [Fact]
    public async Task WatchEvent_ProducesSignal()
    {
        // Arrange
        var (signaler, client, _) = Build(["events"]);
        client.EnqueueWatchEvent(new EtcdWatchEvent(WatchEventType.Put, "/kafka/clusters/events/endpoints", 42));
        var channel = Channel.CreateUnbounded<object>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

        // Act
        var run = signaler.RunAsync(channel.Writer, cts.Token);
        await ReadSignalWithTimeAsync(channel.Reader, TimeSpan.FromSeconds(1));

        // Assert — первое содержательное событие → сигнал шине (спека §4.6)
        cts.Cancel();
        await run;
    }

    [Fact]
    public async Task WindowReopens_WithSnapshotRevision_AsStartRevision()
    {
        // Arrange — снапшот с Revision=7 (max ModRevision kv); окна без событий
        // переоткрываются с start_revision=7 — пропусков нет по построению (спека §3.2)
        var (signaler, client, store) = Build(["events"]);
        client.SetRange("/kafka/clusters/events/", new[]
        {
            new EtcdKv("/kafka/clusters/events/config", """{"brokers":3,"created_unix":1}""", 7),
            new EtcdKv("/kafka/clusters/events/endpoints", "h1:9094", 7),
        });
        (await store.RefreshAsync("events", CancellationToken.None)).IsSuccess.Should().BeTrue();

        // Act
        var channel = Channel.CreateUnbounded<object>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var run = signaler.RunAsync(channel.Writer, cts.Token);
        await WaitUntilAsync(() => client.WatchCalls.Count >= 2, TimeSpan.FromSeconds(1));

        // Assert — каждое окно кластера открывается с ревизией СВОЕГО снапшота
        client.WatchCalls.Should().OnlyContain(c => c.Prefix == "/kafka/clusters/events/");
        client.WatchCalls.Select(c => c.StartRevision).Should().AllSatisfy(r => r.Should().Be(7));
        cts.Cancel();
        await run;
    }

    [Fact]
    public async Task CompactedEvent_ProducesSignal()
    {
        // Arrange — compact-маркер: инкрементальное продолжение невозможно → форс-сигнал
        var (signaler, client, _) = Build(["events"]);
        client.EnqueueCompacted(5);
        var channel = Channel.CreateUnbounded<object>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

        // Act
        var run = signaler.RunAsync(channel.Writer, cts.Token);
        await ReadSignalWithTimeAsync(channel.Reader, TimeSpan.FromSeconds(1));

        // Assert — Compacted равнозначен событию: сигнал шине на полный рефетч (спека §3.2)
        cts.Cancel();
        await run;
    }

    [Fact]
    public async Task WatchFailure_RotatesEndpoint_AndPausesBeforeNextWindow()
    {
        // Arrange — watch всегда падает: Failed → ReportFailure активного endpoint
        // (ротация на второй) + пауза WatchErrorDelayMs между сигналами (спека §4.6)
        var failing = new FailingWatchEtcdClient();
        var options = Options.Create(new HaKafkaOptions
        {
            Clusters = ["events"], WatchWindowMs = 100, WatchReopenDelayMs = 5, WatchErrorDelayMs = 300,
        });
        var rotation = new EtcdEndpointRotation(["http://e1:2379", "http://e2:2379"]);
        var signaler = new HaKafkaWatchLongPollSignaler(
            failing, rotation, new EmptyStore(), options,
            NullLoggerFactory.Instance.CreateLogger<HaKafkaWatchLongPollSignaler>());
        var channel = Channel.CreateUnbounded<object>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));

        // Act
        var run = signaler.RunAsync(channel.Writer, cts.Token);
        var firstAt = await ReadSignalWithTimeAsync(channel.Reader, TimeSpan.FromSeconds(1));
        var activeAfterFirstFailure = rotation.GetActive(); // ротация происходит В момент Failed
        var secondAt = await ReadSignalWithTimeAsync(channel.Reader, TimeSpan.FromSeconds(2));

        // Assert — сбой → сигнал (рефетч-попытка, суммирование сбоев health);
        // активный endpoint сменился; между сигналами ≥ WatchErrorDelayMs (допуск 50 мс)
        activeAfterFirstFailure.Should().NotBe("http://e1:2379", "Failed обязан ротировать endpoint");
        (secondAt - firstAt).Should().BeGreaterOrEqualTo(TimeSpan.FromMilliseconds(250));
        cts.Cancel();
        await run;
    }

    // Пустой store: Get → Failed (снапшотов нет) — start_revision = null
    private sealed class EmptyStore : IKafkaDiscoveryStore
    {
        public Result<Model.KafkaClusterSnapshot> Get(string cluster)
            => Result<Model.KafkaClusterSnapshot>.Failed(new HaKafkaException("нет снапшота"));
        public event Action<Model.KafkaClusterSnapshot>? Updated { add { } remove { } }
        public Task<Result<Model.KafkaClusterSnapshot>> RefreshAsync(string cluster, CancellationToken ct)
            => Task.FromResult(Get(cluster));
    }

    // Клиент с всегда падающим watch (транспортный сбой)
    private sealed class FailingWatchEtcdClient : IEtcdClient
    {
        public Task<Result<IReadOnlyList<EtcdKv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<EtcdKv>>.Success([]));
        public Task<Result<IReadOnlyList<EtcdMember>>> MembersAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<EtcdMember>>.Success([]));
        public async IAsyncEnumerable<EtcdWatchEvent> WatchAsync(
            string endpoint, string prefix, long? startRevision,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
        {
            await Task.Yield();
            throw new IOException("watch оборван (fake)");
        }
    }
}
```

Дополнить `src/PuzzleServer.UnitTests/HA/Kafka/ModuleRegistrationTests.cs` (после теста `Mode_Poll_ResolvesPollSignaler`):

```csharp
    [Fact]
    public void Mode_WatchLongPoll_ResolvesWatchSignaler()
    {
        // Arrange — дефолтный режим; полная ветка возвращена в Task 6 Step 4
        var services = Services();
        services.AddHaKafka(Config()).AddKafkaCluster("events");

        // Act
        using var provider = services.BuildServiceProvider();
        var signaler = provider.GetRequiredService<IHaKafkaRefreshSignaler>();

        // Assert — режим WatchLongPoll выбирает watch-сигнальщик (спека §8 п.7)
        signaler.Should().BeOfType<HaKafkaWatchLongPollSignaler>();
    }
```

- [ ] **Step 2: Прогнать — убедиться, что падают**

```bash
dotnet test src/PuzzleServer.Api.slnx --filter "FullyQualifiedName~UnitTests.HA.Kafka"
```

Expected: FAIL — `HaKafkaWatchLongPollSignaler` не существует (включая `Mode_WatchLongPoll_ResolvesWatchSignaler`: резолвится временная poll-заглушка).

- [ ] **Step 3: Реализовать сигнальщик (порт WatchLongPollSignaler из HA.Db, отличия: один префикс на кластер, свои опции/логи)**

`src/PuzzleServer.Infrastructure.App.HA.Kafka/Refresh/HaKafkaWatchLongPollSignaler.cs`:

```csharp
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PuzzleServer.Infrastructure.App.HA.Etcd;

namespace PuzzleServer.Infrastructure.App.HA.Kafka.Refresh;

// Режим WatchLongPoll: цикл короткоживущих watch-окон (спека §4.6).
// Окно = параллельные стримы на префиксы всех кластеров (ПО ОДНОМУ на кластер —
// /kafka/clusters/<C>/) с общим CTS окна; первое содержательное
// событие/Compacted → сигнал и досрочное закрытие окна. Короткоживущие
// итерации = тривиальное восстановление после обрывов.
public sealed class HaKafkaWatchLongPollSignaler(
    IEtcdClient etcdClient,
    EtcdEndpointRotation rotation,
    IKafkaDiscoveryStore store,
    IOptions<HaKafkaOptions> options,
    ILogger<HaKafkaWatchLongPollSignaler> logger) : IHaKafkaRefreshSignaler
{
    private enum WindowOutcome { Signaled, TimedOut, Failed }

    public async Task RunAsync(ChannelWriter<object> signals, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            WindowOutcome outcome;
            try
            {
                outcome = await RunWindowAsync(ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return; // остановка хоста
            }
            catch (Exception ex)
            {
                // непредвиденное — не роняем шину: пауза и новое окно
                logger.LogWarning(ex, "HA.Kafka: непредвиденная ошибка watch-окна");
                outcome = WindowOutcome.Failed;
            }

            switch (outcome)
            {
                case WindowOutcome.Signaled:
                    signals.TryWrite(new object());
                    await Task.Delay(options.Value.WatchReopenDelayMs, ct);
                    break;
                case WindowOutcome.TimedOut:
                    await Task.Delay(options.Value.WatchReopenDelayMs, ct);
                    break;
                case WindowOutcome.Failed:
                    // watch-сбой → сигнал: шина сделает рефетч-попытку; её провал
                    // инкрементит общий счётчик health (суммирование сбоев, спека §4.6)
                    signals.TryWrite(new object());
                    await Task.Delay(options.Value.WatchErrorDelayMs, ct);
                    break;
            }
        }
    }

    private async Task<WindowOutcome> RunWindowAsync(CancellationToken ct)
    {
        using var windowCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        windowCts.CancelAfter(options.Value.WatchWindowMs);
        var windowToken = windowCts.Token;

        var endpoint = rotation.GetActive();
        // per-cluster start_revision: стрим кластера C — с ревизией снапшота C
        var watchers = BuildWatches()
           .SelectMany(watch => watch.Prefixes.Select(
                prefix => WatchFirstEventAsync(endpoint, prefix, watch.StartRevision, windowToken)))
           .ToList();
        var timeoutTask = Task.Delay(Timeout.Infinite, windowToken);

        // WhenAny не бросает — классифицируем исход по завершившейся задаче
        var completed = await Task.WhenAny(watchers.Append(timeoutTask).ToList());
        if (ct.IsCancellationRequested)
            throw new OperationCanceledException(ct); // остановка хоста — единственный бросок отсюда

        // Закрываем окно и собираем осадок ПОЗАДАЧНО: исключения закрытия поглощаем —
        // исход уже определён в completed
        windowCts.Cancel();
        foreach (var watcher in watchers)
        {
            try
            {
                await watcher;
            }
            catch (OperationCanceledException) when (windowToken.IsCancellationRequested)
            {
                // штатное закрытие стрима отменой окна
            }
            catch (Exception ex)
            {
                logger.LogDebug(ex, "HA.Kafka: watch-стрим на {Endpoint} закрылся с ошибкой", endpoint);
            }
        }

        if (completed == timeoutTask)
        {
            rotation.ReportSuccess(endpoint);
            return WindowOutcome.TimedOut;
        }

        if (completed.IsFaulted)
        {
            rotation.ReportFailure(endpoint);
            logger.LogWarning(completed.Exception?.GetBaseException(),
                "HA.Kafka: watch-окно на {Endpoint} провалилось", endpoint);
            return WindowOutcome.Failed;
        }

        rotation.ReportSuccess(endpoint);
        return completed is Task<bool> { Status: TaskStatus.RanToCompletion, Result: true }
            ? WindowOutcome.Signaled
            : WindowOutcome.TimedOut; // стрим закрылся без событий — не сигнал
    }

    // Первый содержательный элемент стрима; true — событие получено.
    // Обычные события и Compacted-маркер равнозначны (после Compacted полный
    // рефетч пересоберёт ревизию сам).
    private async Task<bool> WatchFirstEventAsync(
        string endpoint, string prefix, long? startRevision, CancellationToken windowToken)
    {
        await foreach (var _ in etcdClient.WatchAsync(endpoint, prefix, startRevision, windowToken))
            return true; // первое событие достаточно — выходим, отмена окна закроет стрим

        return false; // стрим завершился без событий (сервер закрыл) — окно это не сигналит
    }

    // per-cluster start_revision (пропусков нет по построению, спека §3.2):
    // ОДИН префикс на кластер — /kafka/clusters/<C>/ (отличие от HA.Db с двумя)
    private sealed record ClusterWatch(IReadOnlyList<string> Prefixes, long? StartRevision);

    private List<ClusterWatch> BuildWatches()
        => options.Value.Clusters
           .Select(cluster => new ClusterWatch(
               [$"/kafka/clusters/{cluster}/"],
               store.Get(cluster) is { IsSuccess: true, Value.Revision: > 0 and var revision }
                   ? revision
                   : null))
           .ToList();
}
```

- [ ] **Step 4: Вернуть полную WatchLongPoll-ветку в ModuleExtensions (снять временную заглушку Ф4)**

В `ModuleExtensions.AddHaKafka` заменить switch-регистрацию сигнальщика на финальную:

```csharp
        // Сигнальщик выбирается режимом при старте (спека §4.8)
        services.AddSingleton<IHaKafkaRefreshSignaler>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<HaKafkaOptions>>().Value;
            return options.Mode switch
            {
                HaKafkaRefreshMode.Poll => new HaKafkaPollRefreshSignaler(options),
                HaKafkaRefreshMode.WatchLongPoll => new HaKafkaWatchLongPollSignaler(
                    sp.GetRequiredService<IEtcdClient>(),
                    sp.GetRequiredService<EtcdEndpointRotation>(),
                    sp.GetRequiredService<IKafkaDiscoveryStore>(),
                    sp.GetRequiredService<IOptions<HaKafkaOptions>>(),
                    sp.GetRequiredService<ILogger<HaKafkaWatchLongPollSignaler>>()),
                _ => throw new InvalidOperationException($"HA.Kafka: Mode={options.Mode} не поддерживается"),
            };
        });
```

Прогнать все тесты HA.Kafka:

```bash
dotnet build src/PuzzleServer.Api.slnx
dotnet test src/PuzzleServer.Api.slnx --filter "FullyQualifiedName~UnitTests.HA.Kafka"
```

Expected: PASS — все наборы HA.Kafka, включая `Mode_WatchLongPoll_ResolvesWatchSignaler` и 5 тестов сигнальщика.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: HA.Kafka WatchLongPoll signaler + restore Mode switch (per-cluster single prefix, start_revision, compact, rotation) (t05 Ф5)"
```

---

### Task 7: Интеграционные тесты (Testcontainers etcd) (спека §7, Фаза Ф6)

**[repo: Puzzle]**

**Files:**
- Create: `src/PuzzleServer.IntegrationTests/HA/Kafka/KafkaEtcdFixture.cs`, `src/PuzzleServer.IntegrationTests/HA/Kafka/KafkaDiscoveryIntegrationTests.cs`

**Interfaces (consumes):** всё из Task 1–6; fixture-паттерн `src/PuzzleServer.IntegrationTests/HA/Db/EtcdFixture.cs` (Testcontainers etcd, HTTP-клиент засева `/v3/kv/put` с base64).

- [ ] **Step 1: Написать fixture (порт EtcdFixture под kafka-ключи)**

`src/PuzzleServer.IntegrationTests/HA/Kafka/KafkaEtcdFixture.cs` — по образцу `HA/Db/EtcdFixture.cs`: контейнер `quay.io/coreos/etcd:v3.5` (команда `etcd`, advertised `http://localhost:2379` → endpoint `http://localhost:<mapped>`), плюс члены:

```csharp
// IAsyncLifetime: InitializeAsync — старт контейнера; свойство Endpoint (string).
// Хелперы засева (тот же HTTP JSON gateway; механика base64/JSON — 1:1 из EtcdFixture):
public async Task PutAsync(string key, string value);   // POST /v3/kv/put {key,value} base64
public async Task DeleteAsync(string key);               // POST /v3/kv/deleterange
// Пауза/возобновление для fail-open-сценария (подход — как в HA/Db/ClusterFailoverIntegrationTests):
public Task StopEtcdAsync();                             // остановить контейнер
public Task StartEtcdAsync();                            // снова поднять
```

(при отличиях имён/формата — свериться с `src/PuzzleServer.IntegrationTests/HA/Db/EtcdFixture.cs` и повторить его подход дословно).

- [ ] **Step 2: Написать интеграционные тесты (матрица режимов + read-only трафик)**

`src/PuzzleServer.IntegrationTests/HA/Kafka/KafkaDiscoveryIntegrationTests.cs`:

```csharp
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using PuzzleServer.Infrastructure.App.HA.Etcd;
using PuzzleServer.Infrastructure.App.HA.Kafka;
using PuzzleServer.Infrastructure.App.HA.Kafka.Model;
using PuzzleServer.Infrastructure.App.HA.Kafka.Refresh;

namespace PuzzleServer.IntegrationTests.HA.Kafka;

// Полный цикл дискавери в обоих режимах против реального etcd + фиксация
// read-only трафика (спека §7, §8 п.7–11)
public class KafkaDiscoveryIntegrationTests : IAsyncLifetime
{
    private readonly KafkaEtcdFixture _etcd = new();

    public static TheoryData<string> Modes() => new() { "WatchLongPoll", "Poll" };

    private IConfiguration Config(string mode, Dictionary<string, string?>? extra = null)
    {
        var data = new Dictionary<string, string?>
        {
            ["HaKafka:Mode"] = mode,
            ["HaKafka:EtcdEndpoints:0"] = _etcd.Endpoint,
            ["HaKafka:MembersMode"] = "Off",          // монитору тут нечего открывать
            ["HaKafka:WatchWindowMs"] = "300",
            ["HaKafka:PollIntervalMs"] = "200",
        };
        foreach (var (k, v) in extra ?? [])
            data[k] = v;
        return new ConfigurationBuilder().AddInMemoryCollection(data).Build();
    }

    private async Task<IServiceProvider> StartAsync(string mode, RecordingHandler? recording = null)
    {
        var services = new ServiceCollection()
            .AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        if (recording is not null)
        {
            // Декоратор трафика на typed client IEtcdClient (имя = FullName типа)
            services.AddSingleton(recording);
            services.AddHttpClient(typeof(IEtcdClient).FullName!)
                .AddHttpMessageHandler<RecordingHandler>();
        }
        services.AddHaKafka(Config(mode)).AddKafkaCluster("events");
        var provider = services.BuildServiceProvider();
        // Стартуем ВСЕ hosted-сервисы (KafkaDiscoveryRefresher — bootstrap)
        foreach (var hosted in provider.GetServices<IHostedService>())
            await hosted.StartAsync(CancellationToken.None);
        return provider;
    }

    private async Task SeedClusterAsync(string password = "abcdefghijklmnopqrstuvwxyz012345")
    {
        await _etcd.PutAsync("/kafka/clusters/events/config",
            """{"brokers":3,"replication_factor":3,"min_insync_replicas":2,"default_partitions":12,"default_retention_ms":604800000,"created_unix":1756500000}""");
        await _etcd.PutAsync("/kafka/clusters/events/endpoints",
            "host.docker.internal:16001,host.docker.internal:16002,host.docker.internal:16003");
        await _etcd.PutAsync("/kafka/clusters/events/app_user", "app");
        await _etcd.PutAsync("/kafka/clusters/events/app_password", password);
        await _etcd.PutAsync("/kafka/clusters/events/topics/orders",
            """{"partitions":12,"replication_factor":3,"configs":{"retention.ms":"604800000"},"synced_unix":1750000100,"missing":false}""");
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task FullCycle_SnapshotEventsRotationFailOpen(string mode)
    {
        // Arrange
        await _etcd.InitializeAsync();
        await SeedClusterAsync();

        // Act / Assert — 1. bootstrap: снапшот собран
        var provider = await StartAsync(mode);
        await using var _ = provider;
        var store = provider.GetRequiredService<IKafkaDiscoveryStore>();
        (await WaitUntilAsync(() => store.Get("events").IsSuccess, TimeSpan.FromSeconds(5)))
            .Should().BeTrue("bootstrap должен собрать снапшот");
        var snapshot = store.Get("events").Value;
        snapshot.GetClientConfig()!.BootstrapServers.Should().Contain("16001");
        snapshot.TopicNames.Should().Equal("orders");

        // 2. изменение endpoints → событие Updated (порог: Watch ≤ 2 с, Poll ≤ ~1 с)
        var updated = new TaskCompletionSource<KafkaClusterSnapshot>(TaskCreationOptions.RunContinuationsAsynchronously);
        store.Updated += s => { if (s.Cluster == "events") updated.TrySetResult(s); };
        await _etcd.PutAsync("/kafka/clusters/events/endpoints", "host.docker.internal:16011");
        var winner = await Task.WhenAny(updated.Task, Task.Delay(TimeSpan.FromSeconds(3)));
        winner.Should().Be(updated.Task, $"изменение endpoints должно прийти в режиме {mode}");
        (await updated.Task).BootstrapServers.Should().Be("host.docker.internal:16011");

        // 3. ротация пароля (§16-H фаза B): put NEW → событие → новый пароль в конфиге
        var rotated = new TaskCompletionSource<KafkaClusterSnapshot>(TaskCreationOptions.RunContinuationsAsynchronously);
        store.Updated += s => { if (s.Cluster == "events") rotated.TrySetResult(s); };
        await _etcd.PutAsync("/kafka/clusters/events/app_password", "ZYXWVUTSRQPONMLKJIHGFEDCBA987654");
        await Task.WhenAny(rotated.Task, Task.Delay(TimeSpan.FromSeconds(3)));
        store.Get("events").Value.GetClientConfig()!.SaslPassword
            .Should().Be("ZYXWVUTSRQPONMLKJIHGFEDCBA987654");

        // 4. fail-open: остановка etcd → кэш живёт
        await _etcd.StopEtcdAsync();
        store.Get("events").IsSuccess.Should().BeTrue("кэш живёт при лежащем etcd (fail-open)");
        store.Get("events").Value.BootstrapServers.Should().Be("host.docker.internal:16011");

        // 5. возврат etcd → восстановление первым окном/тиком
        await _etcd.StartEtcdAsync();
        await _etcd.PutAsync("/kafka/clusters/events/endpoints", "host.docker.internal:16012");
        var recovered = new TaskCompletionSource<KafkaClusterSnapshot>(TaskCreationOptions.RunContinuationsAsynchronously);
        store.Updated += s => { if (s.Cluster == "events") recovered.TrySetResult(s); };
        await Task.WhenAny(recovered.Task, Task.Delay(TimeSpan.FromSeconds(8)));
        store.Get("events").Value.BootstrapServers.Should().Be("host.docker.internal:16012");
    }

    [Fact]
    public async Task SameValuePut_DoesNotFireUpdated()
    {
        // Arrange
        await _etcd.InitializeAsync();
        await SeedClusterAsync();
        var provider = await StartAsync("WatchLongPoll");
        await using var _ = provider;
        var store = provider.GetRequiredService<IKafkaDiscoveryStore>();
        await WaitUntilAsync(() => store.Get("events").IsSuccess, TimeSpan.FromSeconds(5));
        var fired = 0;
        store.Updated += _ => fired++;

        // Act — переписываем тот же endpoints тем же значением (в т.ч. topics
        // с configs: событие не стреляет при равном СОДЕРЖИМОМ, новые экземпляры
        // коллекций от парсера не в счёт — SameContent структурный)
        await _etcd.PutAsync("/kafka/clusters/events/endpoints",
            "host.docker.internal:16001,host.docker.internal:16002,host.docker.internal:16003");
        await _etcd.PutAsync("/kafka/clusters/events/topics/orders",
            """{"partitions":12,"replication_factor":3,"configs":{"retention.ms":"604800000"},"synced_unix":1750000100,"missing":false}""");
        await Task.Delay(TimeSpan.FromSeconds(2));

        // Assert — событие по содержимому, не по ревизии (спека §8 п.8)
        fired.Should().Be(0);
    }

    [Fact]
    public async Task Traffic_IsReadOnly_RangeAndWatchOnly()
    {
        // Arrange — декорирующий DelegatingHandler на typed client IEtcdClient
        await _etcd.InitializeAsync();
        await SeedClusterAsync();
        var recording = new RecordingHandler();
        var provider = await StartAsync("WatchLongPoll", recording);
        await using var _ = provider;
        var store = provider.GetRequiredService<IKafkaDiscoveryStore>();
        await WaitUntilAsync(() => store.Get("events").IsSuccess, TimeSpan.FromSeconds(5));
        await Task.Delay(TimeSpan.FromSeconds(1)); // пара watch-окон поверх bootstrap

        // Act — журнал трафика собран RecordingHandler'ом
        var entries = recording.Entries.ToList();

        // Assert — только read-only RPC etcd: range + watch; НИЧЕГО не пишем
        // (put/txn/lease/deleterange исключены по построению OnlyContain; спека §8 п.11)
        entries.Should().NotBeEmpty();
        entries.Should().OnlyContain(e => e is "POST /v3/kv/range" or "POST /v3/watch");
        entries.Should().Contain(e => e == "POST /v3/kv/range");
        entries.Should().Contain(e => e == "POST /v3/watch");
    }

    // Счётчик запросов «METHOD path» сквозь весь typed client IEtcdClient
    private sealed class RecordingHandler : DelegatingHandler
    {
        public List<string> Entries { get; } = [];

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Entries.Add($"{request.Method.Method} {request.RequestUri?.AbsolutePath}");
            return base.SendAsync(request, cancellationToken);
        }
    }

    private static async Task<bool> WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
                return true;
            await Task.Delay(100);
        }
        return condition();
    }

    public Task InitializeAsync() => Task.CompletedTask;
    public Task DisposeAsync() => _etcd.DisposeAsync().AsTask();
}
```

Методы `StopEtcdAsync`/`StartEtcdAsync` fixture — остановка/запуск контейнера Testcontainers (свериться с `HA/Db/ClusterFailoverIntegrationTests.cs` и повторить его подход; `StartEtcdAsync` после остановки требует повторного маппинга порта — если Testcontainers не переживает restart, использовать тот же приём, что в HA/Db failover-тестах: пауза контейнера `docker pause`/`unpause` или сетевая изоляция).

- [ ] **Step 3: Прогнать (нужен Docker)**

```bash
dotnet test src/PuzzleServer.Api.slnx --filter "FullyQualifiedName~IntegrationTests.HA.Kafka"
```

Expected: PASS (4 теста: матрица 2 режимов + same-value + read-only трафик).

- [ ] **Step 4: Полный прогон не-регрессии**

```bash
dotnet build src/PuzzleServer.Api.slnx && dotnet test src/PuzzleServer.Api.slnx
```

Expected: всё зелёное (включая HA.Db integration — контроль рефакторинга).

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "test: HA.Kafka integration tests (both modes, rotation, fail-open, same-value, read-only traffic) (t05 Ф6)"
```

---

### Task 8: Документация (спека §4.9 / Фаза Ф7, критерий §8 п.12)

**[repo: Puzzle]**

**Files:**
- Create: `docs/01.18-ha-etcd.md`, `docs/01.19-ha-kafka.md`
- Modify: `docs/01-infrastructure.md` (таблица/список — две строки), `docs/01.17-ha-db.md` (etcd-слой вынесен)

- [ ] **Step 1: `docs/01.18-ha-etcd.md`** — заголовок-шаблон по образцу `01.17-ha-db.md`: проект `PuzzleServer.Infrastructure.App.HA.Etcd`, namespace; секции: назначение (общий etcd-транспорт HA-модулей), состав (IEtcdClient: range/member-list/watch; EtcdEndpointRotation — sticky-ротация; EtcdMembersMonitor — node discovery, режимы Poll/OnFailure, Off на модуле), подключение модулями (typed-client фабрика с таймаутом из опций модуля; конструирование EtcdMembersMonitorOptions), ключевые решения (не зависит от опций потребителей; per-module монитор — дублирование member/list при сосуществовании модулей допустимо).

- [ ] **Step 2: `docs/01.19-ha-kafka.md`** — по образцу `01.17`: проект `PuzzleServer.Infrastructure.App.HA.Kafka`; секции: назначение (клиентский дискавери kafka-кластеров из etcd, контракт pg/arch/15 §5–§6), подключение (`AddHaKafka(configuration).AddKafkaCluster("events")` — заявки в коде, fail-fast), режимы (таблица WatchLongPoll/Poll с настройками и дефолтами из HaKafkaOptions), модель снапшота (KafkaClusterSnapshot/App/TopicInfo/GetClientConfig — plain-поля SASL_PLAINTEXT/PLAIN, редакция пароля; семантика равенства: событие Updated — по SameContent, структурно, включая Topics/Configs — record-== для коллекций ссылочный, как в ha-db), читаемые ключи (таблица config/endpoints/app_user/app_password/topics + фильтры desired./missing/`__`), грабли (fail-open; неполный набор кредов → App=null; State raw; brokers не читаются; событие только при изменении содержимого; ротация пароля = событие).

- [ ] **Step 3: Правки индексов** — в `docs/01-infrastructure.md` добавить строки-ссылки на 01.18 и 01.19 (по формату существующих строк секции Infrastructure); в `docs/01.17-ha-db.md` — заменить упоминания встроенного etcd-слоя ссылкой «транспорт — общий [01.18-ha-etcd.md](01.18-ha-etcd.md)» (разделы «Node discovery» и ключи: там, где описан EtcdMembersMonitor/EtcdEndpointRotation — указать на общую сборку).

- [ ] **Step 4: Проверка и commit**

```bash
dotnet build src/PuzzleServer.Api.slnx
git add -A
git commit -m "docs: HA.Etcd common layer + HA.Kafka discovery library docs (t05 Ф7)"
```

---

### Task 9: Финальная верификация и сводка

**[repo: Puzzle + repo: pg-worktree]**

- [ ] **Step 1: Полная верификация Puzzle (спека §8 п.1)**

```bash
cd /Users/demakaev/ZCodeProject/Puzzle
dotnet build src/PuzzleServer.Api.slnx          # 0 warnings
dotnet test src/PuzzleServer.Api.slnx           # всё зелёное (Docker запущен)
git log --oneline main..feat-t05-kafka-discovery-lib   # 7 коммитов (Task 1, 2, 3, 4+5 общий, 6, 7, 8)
```

- [ ] **Step 2: Проверка критериев спеки §8 (п.1–12) по чек-листу** — пройтись по каждому пункту: сборка/тесты; проекты в slnx; регистрации/fail-fast; Get без сети; снапшот-модель; парсер-канон §2.1; режимы (включая unit-тесты выбора сигнальщика по Mode — Task 4/6); WatchLongPoll-актуальность; Poll-актуальность+ротация; fail-open; только чтение + редакция пароля (unit + integration read-only трафик); документация. Пробелы — исправить и закоммитить (`fix: …`).

- [ ] **Step 3: Дождаться ревью ветки Puzzle перед main (правила AGENTS.base: ревью перед main, мерж — по явной просьбе). Ветка остаётся открытой до ручки пользователя.**

- [ ] **Step 4: В worktree pg — закоммитить финальные правки (если появились) и подготовить сводку для merge-гейта: roadmap-пункт t05 удаляется из `arch/roadmap/kafkaworker.md` мерж-коммитом в main (правило мерж-гейта roadmap README; исполняется при мерже, НЕ сейчас).**

---

## Самопроверка плана (выполнена)

- **Покрытие спеки:** Ф1→Task 1; Ф2→Task 2; Ф3→Task 3; Ф4→Task 4+5; Ф5→Task 6; Ф6→Task 7; Ф7→Task 8; §8-критерии→Task 9 Step 2; §4.8 (не подключать в Program.cs) — нигде нет шага правки `Api/Program.cs`; §2 п.8 (редакция) → Task 2/7; §6 (мониторы per-module) → Task 1/4; slnx → Task 1/2.
- **Структурное сравнение (спека §2 п.7, §4.4, §8 п.8):** `KafkaDiscoveryStore.SameContent` — ручное структурное сравнение (скаляры через `==`, Topics поэлементно, Configs пословно); record-== для коллекционных полей ссылочный — зафиксировано тестом `TopicInfo_RecordEquality_IsReferenceLike_ForDictionaryPayload` (Task 2) и главным тестом `Updated_NotFired_WhenSameTopicsRecreated_AsNewCollectionInstances` (Task 5: два рефетча, одинаковые topics+configs в новых экземплярах List/Dictionary → событие ровно один раз); интеграционный `SameValuePut_DoesNotFireUpdated` дополнен повторным put topics с configs (Task 7). Документация семантики — комментарии модели и Task 8 Step 2 (док 01.19).
- **Unit-покрытие заявленного (§7, §8 п.7/п.8/п.10):** выбор сигнальщика по Mode — `Mode_Poll_ResolvesPollSignaler` (Task 4) + `Mode_WatchLongPoll_ResolvesWatchSignaler` (Task 6); poll-сигнальщик — `HaKafkaPollRefreshSignalerTests` (Task 5: тик → сигнал, отмена → выход); коалесценция — `SignalStorm_DuringSlowPass_CoalescesIntoSingleRefetch` (Task 5, счётчик RangeCalls + медленный range); health — `StartAsync_WhenEtcdDown_BootstrapDegradesButStarts` + `StatusError_ResetsAfterSuccessfulPass` (Task 5); watch-сценарии — событие→сигнал, переоткрытие с start_revision=Revision снапшота (ассерт WatchCalls), Compacted→сигнал, сбой→ротация endpoint + пауза ≥ WatchErrorDelayMs (Task 6).
- **Placeholder-скан:** TBD/TODO нет; каждый код-шаг содержит код или точный diff; интеграционная fixture описана через точный порт существующих файлов с именами источников (`EtcdFixture.cs`, `ClusterFailoverIntegrationTests.cs`).
- **Компилируемость каждого коммита:** Ф4-коммит (Task 4+5) собирается с временной poll-заглушкой в ветке WatchLongPoll (обязательный Step 6 Task 5, пометка в commit-сообщении); полная ветка возвращается строго в Task 6 Step 4 — вариант согласован со спекой §5 (Ф4 = шина+Poll, Ф5 = watch).
- **Консистентность типов:** `KafkaClusterSnapshot(Cluster, State, BootstrapServers, App, Topics, FetchedAtUtc, Revision)` одинаков в Task 2/3/5/6/7; `IKafkaDiscoveryStore` — Task 5/6/7; `IHaKafkaRefreshSignaler`/`HaKafkaPollRefreshSignaler`/`HaKafkaWatchLongPollSignaler` — Task 4/5/6; `EtcdMembersMonitorOptions(EtcdMembersMode, int, int)` — Task 1/4; `Parse(cluster, kvs, out parseErrors, out unknownKeys)` — Task 3/5; `FakeEtcdClient` (SetRange/SimulateRangeFailure/FailAllRanges/RangeDelayMs/RangeCalls/WatchCalls/EnqueueWatchEvent/EnqueueCompacted) — Task 5/6.
- **Счётчики ожиданий:** Task 2 Step 4 — «PASS (7 кейсов: 5 Facts + Theory с 2 InlineData)»; Task 9 Step 1 — «7 коммитов (Task 1, 2, 3, 4+5 общий, 6, 7, 8)» в репозитории Puzzle (Task 0 коммитит только в pg-worktree).
