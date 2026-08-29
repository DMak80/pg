# app_params + ротация app-пароля: план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** per-node ключ `app_params` (серверные параметры подключения в etcd, пишет PgWorker) + ротация app-пароля всего кластера (заявка из AdminPanel → процесс PgWorker) + кнопка в UI.

**Architecture:** контракт etcd уже обновлён в `arch/` (этим же изменением, что и spec — Фаза 1). Код: PgWriter-ensure `app_params` в provisioning/add-shard/надзоре; новый процесс `AppPasswordRotator` (R0–R4, атомарный txn-коммит put+del); AdminPanel — седьмая мутация `POST /api/clusters/{cluster}/app-password/rotate` (заявка `/pgworker/rotations/<C>`, txn-клэйм) + кнопка.

**Tech Stack:** .NET 10 (`TreatWarningsAsErrors=true`, nullable), xUnit + FluentAssertions, Testcontainers etcd, React+Mantine (frontend/).

**Spec:** `docs/superpowers/2026-08-29-app-params-rotate/spec.md` (контракты: arch/11 §2/§3, arch/14 §3/§5 I/§8, arch/adminpanel/02 §9.8, arch/adminpanel/03 §1.6). Исполнитель читает spec вместе с планом.

## Global Constraints

- Все пути — от корня worktree `/Users/demakaev/ZCodeProject/worktrees/feat-app-params-rotate/`.
- Русский для комментариев/доков, английские идентификаторы (`LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true` — ни одного warning).
- Тесты — с AAA-комментариями (`// Arrange` / `// Act` / `// Assert`).
- Существующие ключи/форматы не меняем: `app_user`="app", `app_password` 32 симв `[A-Za-z0-9]`, dsn-ключ как есть.
- Значение по умолчанию app_params — `"sslmode=require"` (P17), из `PgWorker:AppParams:Default`.
- etcd-операции — с failover по endpoints (паттерн `WithFailoverAsync`, `AppSecretEnsurer`); txn-клэймы — `TxnCompare.NotExists`; мутации `/clusters/` — только держателем клэйма.
- Сборка решения: `dotnet build src/PgWorker.slnx` — 0 warnings (errors). Фронт: `cd frontend && npm run build`.
- Коммит после каждой задачи (стиль `feat:`/`test:`, как в истории); НЕ пушить, НЕ мержить — это делает main-агент.
- Семантика модели: `NodeSpec.AppParams == null` — ключа НЕТ; `""` — ключ есть с пустым значением («нет серверных параметров», не перезаписывается). Миграция ensure — только для `null`.

---

### Task 1: Модель `NodeSpec.AppParams` + парсер PgWorker

**Вход:** контракты arch/11 §2, arch/14 §3.1 уже обновлены; парсер `ClusterSnapshotParser` читает `/clusters/` в `ClusterSnapshot`.

**Files:**
- Modify: `src/PgWorker.Core/Model/Domain.cs` (record `NodeSpec`)
- Modify: `src/PgWorker.Etcd/Parsing/ClusterSnapshotParser.cs` (`ShardAcc`, switch, `BuildShard`)
- Test: `src/tests/PgWorker.UnitTests/Etcd/ClusterSnapshotParserTests.cs`

**Interfaces (Produces):** `NodeSpec(string Shard, string Name, NodeState State, string? AppParams = null)` — дальше используют Provisioning/AddShard/NodeSupervisor-задачи и Rotator.

**Spec:** §3.1 (ключ/семантика), §4.1.

- [ ] **Step 1.1: Failing-тест парсера** — добавить в конец `ClusterSnapshotParserTests.cs` (файл уже использует `Kv(key, value, mod_revision)` и FluentAssertions):

```csharp
// AAA: per-node app_params (spec §3.1): значение на ноду; пустое = "" (ключ есть),
// отсутствие ключа = null (не обеспечен — фильтр миграции надзора)
[Fact]
public void Parse_NodeAppParams_PerNodeValueEmptyStringAndMissing()
{
    // Arrange — ноды шарда с app_params: значение / пустое / отсутствие
    var kvs = new List<Kv>
    {
        new("/clusters/shop/config", """{"buckets":1,"dbname":"shop"}""", 1),
        new("/clusters/shop/shards/shard1/replicas", "2", 2),
        new("/clusters/shop/shards/shard1/nodes/shard1a/state", "RUNNING", 3),
        new("/clusters/shop/shards/shard1/nodes/shard1a/app_params", "sslmode=require", 4),
        new("/clusters/shop/shards/shard1/nodes/shard1b/state", "RUNNING", 5),
        new("/clusters/shop/shards/shard1/nodes/shard1b/app_params", "  ", 6),
        new("/clusters/shop/shards/shard1/nodes/shard1c/state", "RUNNING", 7),
    };

    // Act
    var result = ClusterSnapshotParser.ParseClusters(kvs, out _);

    // Assert — значение на своей ноде; whitespace → ""; нет ключа → null
    var nodes = result.Value.Single().Shards.Single().Nodes;
    nodes.Single(n => n.Name == "shard1a").AppParams.Should().Be("sslmode=require");
    nodes.Single(n => n.Name == "shard1b").AppParams.Should().Be("");
    nodes.Single(n => n.Name == "shard1c").AppParams.Should().BeNull();
}
```

- [ ] **Step 1.2: Прогнать — упасть** — `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~Parse_NodeAppParams"`.
  Ожидание: FAIL (свойства `AppParams` нет — ошибка компиляции теста).

- [ ] **Step 1.3: Реализация.** `Domain.cs`, заменить record `NodeSpec`:

```csharp
/// <summary>Плановая нода шарда: имя = имя шарда + буква ("shard1", "shard1a");
/// AppParams — per-node серверные параметры подключения (libpq-строка; null —
/// ключа nodes/&lt;n&gt;/app_params нет, "" — ключ с пустым значением; spec §3.1).</summary>
public sealed record NodeSpec(string Shard, string Name, NodeState State, string? AppParams = null);
```

`ClusterSnapshotParser.cs`: в `ShardAcc` добавить поле:

```csharp
public readonly Dictionary<string, string?> AppParams = [];
```

В switch `ParseClusters` — новый case сразу ПОСЛЕ case `nodes/<n>/state` (тот же префикс segments):

```csharp
case "shards" when segments.Length == 8
    && segments[4].Length > 0
    && segments[5] == "nodes"
    && segments[6].Length > 0
    && segments[7] == "app_params":
{
    var shard = GetOrAdd(acc.Shards, segments[4], static _ => new ShardAcc());
    shard.AppParams[segments[6]] = kv.Value?.Trim() ?? string.Empty;
    break;
}
```

В `BuildShard` — пронести значение в `NodeSpec`:

```csharp
var nodes = shard.Nodes
    .OrderBy(n => n.Name, StringComparer.Ordinal)
    .Select(n => new NodeSpec(
        name, n.Name, ParseNodeState(n.State),
        shard.AppParams.TryGetValue(n.Name, out var appParams) ? appParams : null))
    .ToList();
```

- [ ] **Step 1.4: Прогнать тест + весь проект** — `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~ClusterSnapshotParserTests"`.
  Ожидание: PASS (весь файл, включая старые случаи — регрессий нет).

- [ ] **Step 1.5: Сборка решения** — `dotnet build src/PgWorker.slnx`. Ожидание: 0 warnings/errors (конструктор `NodeSpec` расширен опциональным параметром — существующие вызовы компилируются).

- [ ] **Step 1.6: Commit** — `git add -A && git commit -m "feat: NodeSpec.AppParams — парсер per-node ключа nodes/<n>/app_params (spec §3.1)"`

**Выход:** модель несёт per-node app_params. **Проверка:** Steps 1.4–1.5.

---

### Task 2: `AppParamsOptions` + `AppParamsEnsurer`

**Вход:** Task 1 (модель не нужна тут напрямую); образец `AppSecretEnsurer` + его тесты.

**Files:**
- Modify: `src/PgWorker.App/Options.cs` (класс `PgWorkerOptions`)
- Modify: `src/PgWorker.App/appsettings.json`
- Create: `src/PgWorker.Provisioning/Processes/AppParamsEnsurer.cs`
- Test: `src/tests/PgWorker.UnitTests/Provisioning/AppParamsEnsurerTests.cs`

**Interfaces (Produces):**

```csharp
public interface IAppParamsEnsurer
{
    /// Ensure per-node app_params (spec §4.2): put-if-absent значения по умолчанию
    /// для перечисленных нод; существующие ключи НЕ перезаписываются.
    Task<Result> EnsureShardAsync(string cluster, string shard, IEnumerable<string> nodes, CancellationToken ct);
}
public sealed class AppParamsEnsurer(IEtcdGateway etcd, string[] endpoints, string defaultValue) : IAppParamsEnsurer
```

**Spec:** §3.1 (put-if-absent, дефолт), §4.2.

- [ ] **Step 2.1: Failing-тест** — создать `src/tests/PgWorker.UnitTests/Provisioning/AppParamsEnsurerTests.cs`:

```csharp
using PgWorker.Core;
using PgWorker.Etcd.Client;
using PgWorker.Provisioning.Processes;
using Xunit;

namespace PgWorker.UnitTests.Provisioning;

// Ensure per-node app_params (spec §3.1/§4.2): put-if-absent дефолта,
// существующее не перезаписывается, пустое значение живёт.
public class AppParamsEnsurerTests
{
    private const string Ep = "http://etcd:2379";

    private static AppParamsEnsurer Sut(Fakes.FakeEtcd etcd)
        => new(etcd, [Ep], "sslmode=require");

    [Fact]
    public async Task Ensure_MissingKeys_PutsDefaultPerNode()
    {
        // Arrange — ключей нет (provisioning P2.5' после dsn)
        var etcd = new Fakes.FakeEtcd();

        // Act
        var result = await Sut(etcd).EnsureShardAsync(
            "shop", "shard1", ["shard1a", "shard1b"], CancellationToken.None);

        // Assert — обе ноды получили дефолт, разными txn (put-if-absent)
        result.IsSuccess.Should().BeTrue();
        etcd.Store["/clusters/shop/shards/shard1/nodes/shard1a/app_params"].Value
            .Should().Be("sslmode=require");
        etcd.Store["/clusters/shop/shards/shard1/nodes/shard1b/app_params"].Value
            .Should().Be("sslmode=require");
    }

    [Fact]
    public async Task Ensure_ExistingKeys_NotOverwritten()
    {
        // Arrange — оператор etcdctl'ом записал своё значение (в т.ч. пустое)
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/clusters/shop/shards/shard1/nodes/shard1a/app_params", "sslmode=verify-full");
        etcd.Seed("/clusters/shop/shards/shard1/nodes/shard1b/app_params", "");

        // Act
        var result = await Sut(etcd).EnsureShardAsync(
            "shop", "shard1", ["shard1a", "shard1b"], CancellationToken.None);

        // Assert — txn проигран compare NotExists, значения нетронуты (spec §3.1)
        result.IsSuccess.Should().BeTrue();
        etcd.Store["/clusters/shop/shards/shard1/nodes/shard1a/app_params"].Value
            .Should().Be("sslmode=verify-full");
        etcd.Store["/clusters/shop/shards/shard1/nodes/shard1b/app_params"].Value
            .Should().Be("");
    }
}
```

- [ ] **Step 2.2: Прогнать — упасть** — `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~AppParamsEnsurerTests"`. Ожидание: FAIL (тип не найден — CS0103/compile error).

- [ ] **Step 2.3: Реализация** — создать `src/PgWorker.Provisioning/Processes/AppParamsEnsurer.cs`:

```csharp
using PgWorker.Core;
using PgWorker.Etcd.Client;

namespace PgWorker.Provisioning.Processes;

/// <summary>
/// Ensure per-node app_params (spec §4.2, arch/14 §5 P2.5'): ключ
/// /clusters/&lt;C&gt;/shards/&lt;X&gt;/nodes/&lt;n&gt;/app_params — put-if-absent ОДНОЙ txn
/// [NotExists]+[put] с дефолтом PgWorker:AppParams:Default. Проигрыш compare —
/// законный исход (ключ есть: ручные правки оператора живы). Txn и чтения —
/// с failover по endpoints до первого живого (паттерн AppSecretEnsurer).
/// Вызывается только держателем клэйма &lt;C&gt; (инвариант мутаций /clusters/).
/// </summary>
public interface IAppParamsEnsurer
{
    Task<Result> EnsureShardAsync(string cluster, string shard, IEnumerable<string> nodes, CancellationToken ct);
}

public sealed class AppParamsEnsurer(IEtcdGateway etcd, string[] endpoints, string defaultValue)
    : IAppParamsEnsurer
{
    public async Task<Result> EnsureShardAsync(
        string cluster, string shard, IEnumerable<string> nodes, CancellationToken ct)
    {
        foreach (var node in nodes)
        {
            var done = await TxnAsync(
                TxnRequest.Of(
                    [TxnCompare.NotExists(Key(cluster, shard, node))],
                    [new TxnOp.Put(Key(cluster, shard, node), defaultValue, null)]),
                ct);
            if (!done.IsSuccess)
                return done; // транспортный сбой всех endpoints; проигрыш compare — не сбой
        }

        return Result.Success();
    }

    private static string Key(string cluster, string shard, string node)
        => $"/clusters/{cluster}/shards/{shard}/nodes/{node}/app_params";

    // Failover-обёртка: первый успешный endpoint выигрывает (образец AppSecretEnsurer).
    private async Task<Result<TxnResult>> TxnAsync(TxnRequest req, CancellationToken ct)
    {
        Result<TxnResult>? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.TxnAsync(endpoint, req, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }
}
```

- [ ] **Step 2.4: Конфиг.** `src/PgWorker.App/Options.cs` — внутрь `PgWorkerOptions` добавить свойство (рядом с `Moves`):

```csharp
    /// <summary>Per-node серверные параметры подключения (app_params, spec §3.1;
    /// P17: doorman tls_mode=require → клиентский sslmode=require).</summary>
    public AppParamsOptions AppParams { get; set; } = new();
```

и в конец файла — класс:

```csharp
/// <summary>Дефолт значения ключа nodes/&lt;n&gt;/app_params (spec §3.1): libpq-строка
/// keyword=value; применяется put-if-absent (P2.5'/A5/надзор-C).</summary>
public sealed class AppParamsOptions
{
    public string Default { get; set; } = "sslmode=require";
}
```

`src/PgWorker.App/appsettings.json` — в `"PgWorker"` после `"Moves": { … }` добавить:

```json
    ,
    "AppParams": { "Default": "sslmode=require" }
```

- [ ] **Step 2.5: Прогнать тесты** — `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~AppParamsEnsurerTests"`. Ожидание: PASS (2 теста).

- [ ] **Step 2.6: Commit** — `git add -A && git commit -m "feat: AppParamsEnsurer — put-if-absent per-node app_params + PgWorker:AppParams:Default (spec §4.2)"`

**Выход:** `IAppParamsEnsurer` + опция. **Проверка:** Step 2.5.

---

### Task 3: ProvisioningProcess — фаза P2.5'

**Вход:** Task 2 (`IAppParamsEnsurer`); `ProvisioningProcess.ProvisionShardSqlAsync` пишет dsn последним.

**Files:**
- Modify: `src/PgWorker.Provisioning/Processes/ProvisioningProcess.cs` (ctor + `ProvisionShardSqlAsync`)
- Modify: `src/PgWorker.App/Program.cs` (DI: регистрация + ctor-аргумент)
- Test: `src/tests/PgWorker.UnitTests/Provisioning/ProvisioningProcessTests.cs`

**Interfaces (Consumes):** `IAppParamsEnsurer.EnsureShardAsync(string, string, IEnumerable<string>, CancellationToken) → Task<Result>`.
**Interfaces (Produces):** ctor `ProvisioningProcess(…, IAppSecretEnsurer appSecret, IAppParamsEnsurer appParams, EtcdEndpoints etcdEndpoints, Func<…>? snapshot = null)` — новый параметр ПОСЛЕ `appSecret`.

**Spec:** §4.2 (P2.5'), arch/14 §5 A.

- [ ] **Step 3.1: Failing-тест** — в `ProvisioningProcessTests.cs`: в `NewRig` (строка ~91) передать новый аргумент — заменить

```csharp
        var process = new ProvisioningProcess(
            etcd, [Ep], driver, sql, Probe(patroniResponse, trace), claims, journal, Opts, Secrets,
            appSecret, EtcdEndp, snapshot: null);
```

на

```csharp
        var process = new ProvisioningProcess(
            etcd, [Ep], driver, sql, Probe(patroniResponse, trace), claims, journal, Opts, Secrets,
            appSecret, new AppParamsEnsurer(etcd, [Ep], "sslmode=require"), EtcdEndp, snapshot: null);
```

и добавить тест в конец класса (паттерн `NewRig(port => Patroni(port == 18000 ? "shard1a" : "shard2a"))` доводит до DONE — см. существующий `Tick_…Dsn`-тест):

```csharp
// AAA: P2.5' — после SQL-фазы шарда у КАЖДОЙ ноды есть app_params дефолта (spec §4.2)
[Fact]
public async Task Tick_SqlPhase_WritesNodeAppParamsForAllShardNodes()
{
    // Arrange — Patroni жив, мастера shard1a/shard2a (доводит тик до DONE)
    var rig = await NewRig(port => Patroni(port == 18000 ? "shard1a" : "shard2a"));

    // Act
    var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

    // Assert — все 4 ноды двух шардов получили ключ-дефолт
    outcome.Value.Should().Be(ProcessOutcome.Done);
    foreach (var (shard, node) in new[]
             { ("shard1", "shard1a"), ("shard1", "shard1b"), ("shard2", "shard2a"), ("shard2", "shard2b") })
        rig.Etcd.Store[$"/clusters/shop/shards/{shard}/nodes/{node}/app_params"].Value
            .Should().Be("sslmode=require");
}
```

- [ ] **Step 3.2: Прогнать — упасть** — `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~ProvisioningProcessTests"`.
  Ожидание: FAIL компиляции (нет ctor-параметра `IAppParamsEnsurer`).

- [ ] **Step 3.3: Реализация.** `ProvisioningProcess.cs`:
  1) первичный ctor: после `IAppSecretEnsurer appSecret,` добавить строку `IAppParamsEnsurer appParams,`;
  2) в `ProvisionShardSqlAsync` ПОСЛЕ блока записи dsn (строки с `return await PutAsync($"/clusters/{cluster}/shards/{shard.Name}/dsn", dsn, ct);`) и ПЕРЕД финальным `return Result.Success();` вставить:

```csharp
        // P2.5' (spec §4.2, arch/14 §5 A): ensure app_params КАЖДОЙ ноды шарда —
        // put-if-absent дефолта; существующие (ручные) значения не трогаем.
        var appParamsEnsured = await appParams.EnsureShardAsync(
            cluster, shard.Name, shard.Nodes.Select(n => n.Name), ct);
        if (!appParamsEnsured.IsSuccess)
            return appParamsEnsured;
```

- [ ] **Step 3.4: DI.** `src/PgWorker.App/Program.cs`: после регистрации `IAppSecretEnsurer` (строка ~140) добавить:

```csharp
// Ensure per-node app_params (spec §4.2): put-if-absent дефолта — общий для
// Provisioning (P2.5')/AddShard (A5)/надзора (миграция C).
builder.Services.AddSingleton<IAppParamsEnsurer>(sp => new AppParamsEnsurer(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.AppParams.Default));
```

и в регистрации `ProvisioningProcess` после `sp.GetRequiredService<IAppSecretEnsurer>(),` добавить:

```csharp
        sp.GetRequiredService<IAppParamsEnsurer>(),
```

- [ ] **Step 3.5: Прогнать + сборка** — `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~ProvisioningProcessTests"` (PASS) и `dotnet build src/PgWorker.slnx` (0 warnings).

- [ ] **Step 3.6: Commit** — `git add -A && git commit -m "feat: P2.5' — ensure app_params нод шарда в ProvisioningProcess (spec §4.2)"`

**Выход:** provisioning пишет per-node app_params. **Проверка:** Step 3.5.

---

### Task 4: AddShardProcess — A5: ensure app_params + свежий re-read кредов

**Вход:** Task 2-3; `AddShardProcess.ProvisionShardSqlAsync(snap, shard, topology, master, app, ct)` получает креды, прочитанные ДО ожидания Patroni (окно гонки с ротацией — spec О3).

**Files:**
- Modify: `src/PgWorker.Provisioning/Processes/AddShardProcess.cs` (ctor; `ProvisionShardSqlAsync`)
- Modify: `src/PgWorker.App/Program.cs` (ctor-аргумент AddShardProcess)
- Test: `src/tests/PgWorker.UnitTests/Provisioning/AddShardProcessTests.cs`

**Interfaces (Produces):** ctor `AddShardProcess(…, InstallSecrets secrets, IAppSecretEnsurer appSecret, IAppParamsEnsurer appParams, EtcdEndpoints etcdEndpoints, Func<…>? snapshot = null)`.

**Spec:** §4.2 (A5), §4.4 (гонка add-shard ↔ ротация), arch/14 §5 G.

- [ ] **Step 4.1: Failing-тест** — в `AddShardProcessTests.cs` `NewRig` заменить

```csharp
        var process = new AddShardProcess(
            etcd, [Ep], driver, sql, Probe(patroniResponse), claims, journal,
            opts ?? new PlacementOptions(15000, 15100, PatroniBootSec: 600),
            Secrets, new AppSecretEnsurer(etcd, [Ep]), EtcdEndp, snapshot: null);
```

на

```csharp
        var process = new AddShardProcess(
            etcd, [Ep], driver, sql, Probe(patroniResponse), claims, journal,
            opts ?? new PlacementOptions(15000, 15100, PatroniBootSec: 600),
            Secrets, new AppSecretEnsurer(etcd, [Ep]),
            new AppParamsEnsurer(etcd, [Ep], "sslmode=require"), EtcdEndp, snapshot: null);
```

и добавить тест (паттерн `port == 18002 ? Patroni("shard3a") : DeadPatroni()` доводит add до SQL — см. строку ~233):

```csharp
// AAA: A5 — SQL-фаза нового шарда ensure'ит app_params его нод и перечитывает
// app-креды непосредственно перед ALTER (гонка с ротацией, spec §4.4/О3)
[Fact]
public async Task Tick_SqlPhase_WritesAppParamsAndUsesFreshAppPassword()
{
    // Arrange — add-shard shard3 доведён до SQL-фазы; app-секрет кластера меняется
    // ПОСЛЕ старта тика (ротация успела закоммитить новый пароль)
    var rig = await NewRig(port => port == 18002 ? Patroni("shard3a") : DeadPatroni());
    await new AppSecretEnsurer(rig.Etcd, [Ep]).EnsureAsync("shop", CancellationToken.None);
    var newPassword = "Rotated00000000000000000000000Z";
    await rig.Etcd.PutAsync(Ep, "/clusters/shop/app_password", newPassword, null, CancellationToken.None);

    // Act
    var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), "shard3", CancellationToken.None);

    // Assert — DONE: ноды нового шарда с app_params; ALTER выравнивает роль
    // по СВЕЖЕМУ паролю (не по креду, прочитанному до ожидания Patroni)
    outcome.Value.Should().Be(ProcessOutcome.Done);
    rig.Etcd.Store["/clusters/shop/shards/shard3/nodes/shard3a/app_params"].Value
        .Should().Be("sslmode=require");
    rig.Etcd.Store["/clusters/shop/shards/shard3/nodes/shard3b/app_params"].Value
        .Should().Be("sslmode=require");
    rig.Sql.Executed.Should().Contain(e =>
        e.Sql.Contains("ALTER ROLE") && e.Sql.Contains(newPassword));
}
```

- [ ] **Step 4.2: Прогнать — упасть** — `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~AddShardProcessTests"`. Ожидание: FAIL (ctor).

- [ ] **Step 4.3: Реализация.** `AddShardProcess.cs`:
  1) ctor: после `IAppSecretEnsurer appSecret,` — добавить `IAppParamsEnsurer appParams,`;
  2) сигнатура `ProvisionShardSqlAsync` — убрать параметр `AppCredentials app`:

```csharp
    private async Task<Result> ProvisionShardSqlAsync(
        ClusterSnapshot snap, ShardSpec shard, ShardTopology topology, NodeAddress master,
        CancellationToken ct)
```

  3) в начале метода (после вычисления `bucketAdminPassword`) — свежий re-read кредов (spec §4.4):

```csharp
        // Свежий re-read app-кредов в SQL-фазе (spec §4.4): пока шард поднимался
        // (минуты ожидания Patroni), ротация §5 I могла сменить app_password.
        var freshCreds = await appSecret.EnsureAsync(cluster, ct);
        if (!freshCreds.IsSuccess)
            return freshCreds;
        var app = freshCreds.Value;
```

  4) после блока записи dsn (`return await PutAsync(…dsn…, ct);`) ПЕРЕД `return Result.Success();` — ensure:

```csharp
        // A5 (spec §4.2): ensure app_params нод НОВОГО шарда — как P2.5'.
        var appParamsEnsured = await appParams.EnsureShardAsync(
            cluster, shard.Name, shard.Nodes.Select(n => n.Name), ct);
        if (!appParamsEnsured.IsSuccess)
            return appParamsEnsured;
```

  5) вызов в `TickAsync` (строка ~135) — убрать аргумент `appCreds.Value`:

```csharp
        var sqlDone = await ProvisionShardSqlAsync(snap, shard, topology, master, ct);
```

- [ ] **Step 4.4: DI.** `Program.cs`, регистрация `AddShardProcess`: после `sp.GetRequiredService<IAppSecretEnsurer>(),` добавить `sp.GetRequiredService<IAppParamsEnsurer>(),`.

- [ ] **Step 4.5: Прогнать + сборка** — `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~AddShardProcessTests"` (PASS, включая новый); `dotnet build src/PgWorker.slnx` (0 warnings).

- [ ] **Step 4.6: Commit** — `git add -A && git commit -m "feat: A5 — ensure app_params нового шарда + свежий re-read app-кредов перед ALTER (spec §4.2/§4.4)"`

**Выход:** add-shard согласован с app_params/ротацией. **Проверка:** Step 4.5.

---

### Task 5: NodeSupervisor — ленивая миграция app_params

**Вход:** Task 1 (`NodeSpec.AppParams`), Task 2 (`IAppParamsEnsurer`); `NodeSupervisor.TickAsync` уже читает portalloc и итерирует шарды.

**Files:**
- Modify: `src/PgWorker.Provisioning/Processes/NodeSupervisor.cs` (ctor; `TickAsync`)
- Modify: `src/PgWorker.App/Program.cs` (ctor-аргумент NodeSupervisor)
- Test: `src/tests/PgWorker.UnitTests/Provisioning/NodeSupervisorTests.cs`

**Interfaces (Produces):** ctor `NodeSupervisor(…, InstallSecrets secrets, IAppParamsEnsurer appParams, MasterKeyReconciler? masterKeys = null, EtcdEndpoints? etcdForNodes = null)` — параметр ПОСЛЕ `secrets` (перед `masterKeys`).

**Spec:** §4.2 (миграция), arch/14 §5 C.

- [ ] **Step 5.1: Failing-тест** — в `NodeSupervisorTests.cs` `NewRig` заменить

```csharp
        var supervisor = new NodeSupervisor(
            etcd, [Ep], driver, probe, claims, journal, Thresholds, TimeProvider.System, Secrets,
            new MasterKeyReconciler(etcd, [Ep], probe));
```

на

```csharp
        var supervisor = new NodeSupervisor(
            etcd, [Ep], driver, probe, claims, journal, Thresholds, TimeProvider.System, Secrets,
            new AppParamsEnsurer(etcd, [Ep], "sslmode=require"),
            new MasterKeyReconciler(etcd, [Ep], probe));
```

и добавить тест:

```csharp
// AAA: миграция app_params (arch/14 §5 C) — ноды шарда с dsn без ключа получают
// дефолт; ручное значение существующего ключа не перезаписывается
[Fact]
public async Task Tick_NodeWithoutAppParams_MigrationPutsDefault()
{
    // Arrange — кластер «до app_params»: у shard1b ключ есть (ручной), у остальных нет
    var rig = await NewRig(_ => Ok());
    rig.Etcd.Seed("/clusters/shop/shards/shard1/nodes/shard1b/app_params", "sslmode=verify-full");

    // Act
    var outcome = await rig.Supervisor.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

    // Assert — отсутствующие дописаны дефолтом, ручное значение живо (put-if-absent)
    outcome.IsSuccess.Should().BeTrue();
    rig.Etcd.Store["/clusters/shop/shards/shard1/nodes/shard1a/app_params"].Value
        .Should().Be("sslmode=require");
    rig.Etcd.Store["/clusters/shop/shards/shard1/nodes/shard1b/app_params"].Value
        .Should().Be("sslmode=verify-full");
}
```

- [ ] **Step 5.2: Прогнать — упасть** — `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~NodeSupervisorTests"`. Ожидание: FAIL (ctor).

- [ ] **Step 5.3: Реализация.** `NodeSupervisor.cs`:
  1) ctor: после `InstallSecrets secrets,` — вставить `IAppParamsEnsurer appParams,`;
  2) в `TickAsync` ПОСЛЕ `EnsureDeclaredNodesAsync`-блока (шаг «1») и ПЕРЕД чтением `unreachable` (шаг «2») вставить:

```csharp
        // 1.5) Миграция app_params (ленивый ensure, arch/14 §5 C): ноды шардов
        // с dsn без ключа (кластеры, созданные до app_params) — put-if-absent
        // дефолта; после первого обеспечения последующие тики — no-op (модель
        // снапшота уже несёт наличие ключа). Шард без dsn — домен AddShardProcess.
        foreach (var shard in snap.Shards)
        {
            if (shard.Dsn is null)
                continue;
            var missing = shard.Nodes.Where(n => n.AppParams is null).Select(n => n.Name).ToList();
            if (missing.Count == 0)
                continue;
            var migrated = await appParams.EnsureShardAsync(cluster, shard.Name, missing, ct);
            if (!migrated.IsSuccess)
                return Fail(migrated.Error!);
        }
```

- [ ] **Step 5.4: DI.** `Program.cs`, регистрация `NodeSupervisor`: после `sp.GetRequiredService<InstallSecrets>(),` добавить `sp.GetRequiredService<IAppParamsEnsurer>(),`.

- [ ] **Step 5.5: Прогнать + сборка** — `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~NodeSupervisorTests"` (PASS); `dotnet build src/PgWorker.slnx` (0 warnings).

- [ ] **Step 5.6: Commit** — `git add -A && git commit -m "feat: надзор C — ленивая миграция app_params нод зарегистрированных шардов (spec §4.2)"`

**Выход:** живые кластеры получают app_params. **Проверка:** Step 5.5.

---

### Task 6: `AppPasswordRotator` (R0–R4)

**Вход:** Tasks 1-2; образцы: `AddShardProcess` (journal/Finish/FailAsync/ReadPortAllocAsync), `AppSecretEnsurer`, `DatabaseProvisioner.BuildAlterAppPasswordSql/BuildAdminDsn`.

**Files:**
- Create: `src/PgWorker.Provisioning/Processes/AppPasswordRotator.cs`
- Test: `src/tests/PgWorker.UnitTests/Provisioning/AppPasswordRotatorTests.cs`

**Interfaces (Produces):**

```csharp
public sealed class AppPasswordRotator(
    IEtcdGateway etcd, string[] endpoints, ISqlExecutor db, ShardProbe probe,
    ClaimStore claims, WorkJournal journal, InstallSecrets secrets,
    IAppSecretEnsurer appSecret, Func<CancellationToken, Task<Result>>? snapshot = null)
{
    public Task<Result<ProcessOutcome>> TickAsync(ClusterSnapshot snap, CancellationToken ct);
}
```

**Spec:** §3.2 (заявка), §4.3 (R0–R4), arch/14 §5 I.

- [ ] **Step 6.1: Failing-тест** — создать `src/tests/PgWorker.UnitTests/Provisioning/AppPasswordRotatorTests.cs`:

```csharp
using System.Net;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Processes;
using Xunit;

namespace PgWorker.UnitTests.Provisioning;

// Ротация app-пароля по заявке /pgworker/rotations/<C> (spec §4.3, arch/14 §5 I):
// ALTER на мастерах всех шардов с dsn → атомарный txn-коммит put+del; transient-отказы.
public class AppPasswordRotatorTests
{
    private const string Ep = "http://etcd:2379";
    private static readonly InstallSecrets Secrets = new("su-pw", "sb-pw", "adm-pw", "mov-pw");

    private sealed class DeadHandler : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken ct)
            => Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable));
    }

    // Сид: Active-кластер, 2 шарда с dsn + master-ключи (мастера по host из portalloc),
    // app-секрет, portalloc (мастера — первые ноды, host h1/pg 15000 и h2/pg 15001).
    private static void SeedCluster(Fakes.FakeEtcd etcd, string cluster = "shop")
    {
        etcd.Seed($"/clusters/{cluster}/config",
            $$"""{"buckets":2,"dbname":"{{cluster}}","created_unix":1755900000}""");
        etcd.Seed("/clusters/shop/app_user", "app");
        etcd.Seed("/clusters/shop/app_password", "OldPassword000000000000000000A");
        foreach (var (shard, host, pg) in new[] { ("shard1", "h1", 15000), ("shard2", "h2", 15001) })
        {
            etcd.Seed($"/clusters/shop/shards/{shard}/replicas", "2");
            etcd.Seed($"/clusters/shop/shards/{shard}/nodes/{shard}a/state", "RUNNING");
            etcd.Seed($"/clusters/shop/shards/{shard}/nodes/{shard}b/state", "RUNNING");
            etcd.Seed($"/clusters/shop/shards/{shard}/dsn",
                $"host={host} port={pg} dbname=shop user=bucket_admin password=x");
            etcd.Seed($"/clusters/shop/shards/{shard}/master", $"{host}:16500");
            etcd.Seed($"/clusters/shop/shards/{shard}/nodes/{shard}a/app_params", "sslmode=require");
            etcd.Seed($"/clusters/shop/shards/{shard}/nodes/{shard}b/app_params", "sslmode=require");
        }

        var alloc = new Dictionary<string, NodeAddress>
        {
            ["shard1/shard1a"] = new("h1", new NodePorts(15000, 18000, 16500)),
            ["shard1/shard1b"] = new("h2", new NodePorts(15002, 18002, 16502)),
            ["shard2/shard2a"] = new("h2", new NodePorts(15001, 18001, 16501)),
            ["shard2/shard2b"] = new("h1", new NodePorts(15003, 18003, 16503)),
        };
        etcd.Seed("/pgworker/portalloc/shop", PgWorker.Core.Model.Portalloc.Serialize(alloc));
        etcd.Seed("/clusters/shop/buckets/routing/bucket_0", "shard1");
        etcd.Seed("/clusters/shop/buckets/routing/bucket_1", "shard2");
    }

    private static async Task<ClusterSnapshot> Snapshot(Fakes.FakeEtcd etcd)
    {
        var range = await etcd.RangeAsync(Ep, "/clusters/", CancellationToken.None);
        var parsed = ClusterSnapshotParser.ParseClusters(range.Value, out _);
        return parsed.Value.Single(c => c.Config.Cluster == "shop");
    }

    private sealed record Rig(Fakes.FakeEtcd Etcd, Fakes.FakeSql Sql, ClaimStore Claims,
        WorkJournal Journal, AppPasswordRotator Rotator);

    private static async Task<Rig> NewRig(Fakes.FakeEtcd? etcd = null, Fakes.FakeSql? sql = null)
    {
        var store = etcd ?? new Fakes.FakeEtcd();
        if (etcd is null)
            SeedCluster(store);
        var usedSql = sql ?? new Fakes.FakeSql();
        var claims = new ClaimStore([Ep], store, TimeProvider.System);
        await claims.TryClaimClusterAsync("shop", CancellationToken.None);
        var journal = new WorkJournal(store, [Ep]);
        var probe = new ShardProbe(new HttpClient(new DeadHandler()));
        var rotator = new AppPasswordRotator(
            store, [Ep], usedSql, probe, claims, journal, Secrets,
            new AppSecretEnsurer(store, [Ep]), snapshot: null);
        return new Rig(store, usedSql, claims, journal, rotator);
    }
```

(в using-блоках файла — `using PgWorker.Provisioning.Probes;` для `ShardProbe`)

Тесты:

```csharp
    private static void SeedTicket(Fakes.FakeEtcd etcd, string raw =
        """{"requested_unix":1755900100,"requested_by":"admin"}""")
        => etcd.Seed("/pgworker/rotations/shop", raw);

    [Fact]
    public async Task Tick_NoTicket_NoOp()
    {
        // Arrange — заявки нет
        var rig = await NewRig();

        // Act
        var outcome = await rig.Rotator.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert — no-op: пароль нетронут, SQL/txn не было
        outcome.IsSuccess.Should().BeTrue();
        outcome.Value.Should().Be(ProcessOutcome.Done);
        rig.Etcd.Store["/clusters/shop/app_password"].Value
            .Should().Be("OldPassword000000000000000000A");
        rig.Sql.Executed.Should().BeEmpty();
        rig.Etcd.Txns.Should().BeEmpty();
    }

    [Fact]
    public async Task Tick_Ticket_AltersAllShardsAndCommitsAtomically()
    {
        // Arrange — заявка стоит; оба шарда с dsn, мастера известны из master-ключей
        var rig = await NewRig();
        SeedTicket(rig.Etcd);

        // Act
        var outcome = await rig.Rotator.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert — ALTER на мастерах ОБЕИХ шардов; одна txn: compare OLD + put NEW + del заявки
        outcome.IsSuccess.Should().BeTrue();
        var sqlTexts = rig.Sql.Executed.Select(e => e.Sql).ToList();
        sqlTexts.Should().HaveCount(2).And.OnlyContain(s => s.Contains("ALTER ROLE \"app\" PASSWORD"));
        rig.Etcd.Store["/clusters/shop/app_password"].Value
            .Should().MatchRegex("^[A-Za-z0-9]{32}$").And.NotBe("OldPassword000000000000000000A");
        rig.Etcd.Store.Should().NotContainKey("/pgworker/rotations/shop");
        var commit = rig.Etcd.Txns.Single(t => t.Success.Any(op => op is TxnOp.Put));
        commit.Compare.Should().Contain(c =>
            c.Key == "/clusters/shop/app_password" && c.Arg == "OldPassword000000000000000000A");
        commit.Success.Should().ContainSingle(op =>
            op is TxnOp.Delete d && d.Key == "/pgworker/rotations/shop");
        (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.Op
            .Should().Be("rotate-app-password");
    }

    [Fact]
    public async Task Tick_TicketShardWithoutMaster_PasswordUntouchedTicketAlive()
    {
        // Arrange — у shard2 нет master-ключа и Patroni мёртв (transient, spec §4.3 R2)
        var rig = await NewRig();
        rig.Etcd.Store.Remove("/clusters/shop/shards/shard2/master");
        SeedTicket(rig.Etcd);

        // Act
        var outcome = await rig.Rotator.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert — Failed: app_password прежний, заявка жива (ретрай тиком с начала)
        outcome.IsSuccess.Should().BeFalse();
        rig.Etcd.Store["/clusters/shop/app_password"].Value
            .Should().Be("OldPassword000000000000000000A");
        rig.Etcd.Store.Should().ContainKey("/pgworker/rotations/shop");
    }

    [Fact]
    public async Task Tick_TicketExternalPasswordChange_CompareLostRetriable()
    {
        // Arrange — внешний etcdctl меняет app_password между чтением и коммитом:
        // инъекция — перезапись ключа при ВТОРОМ ALTER (spec §4.3 R3)
        var rig = await NewRig();
        SeedTicket(rig.Etcd);
        var alters = 0;
        rig.Sql.OnExecute = _ =>
        {
            if (++alters == 2)
                rig.Etcd.Seed("/clusters/shop/app_password", "External0000000000000000000000X");
        };

        // Act
        var outcome = await rig.Rotator.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert — compare проигран: заявка жива, значение = внешнее (ретрай тиком)
        outcome.IsSuccess.Should().BeFalse();
        rig.Etcd.Store["/clusters/shop/app_password"].Value
            .Should().Be("External0000000000000000000000X");
        rig.Etcd.Store.Should().ContainKey("/pgworker/rotations/shop");
    }

    [Fact]
    public async Task Tick_MalformedTicket_RemovedAsGarbage()
    {
        // Arrange — битая заявка-мусор (не-JSON, spec §4.3 R0/arch §5 I)
        var rig = await NewRig();
        SeedTicket(rig.Etcd, "not-json");

        // Act
        var outcome = await rig.Rotator.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert — удалена с journal-записью; пароль/SQL не тронуты
        outcome.IsSuccess.Should().BeTrue();
        rig.Etcd.Store.Should().NotContainKey("/pgworker/rotations/shop");
        rig.Sql.Executed.Should().BeEmpty();
        (await rig.Journal.ReadAsync("shop", CancellationToken.None)).Value!.Phase
            .Should().Be("malformed-ticket-removed");
    }

    [Fact]
    public async Task Tick_ClaimNotMine_MutationsForbidden()
    {
        // Arrange — заявка есть, клэйм не взят (инвариант мутаций /clusters/)
        var etcd = new Fakes.FakeEtcd();
        SeedCluster(etcd);
        SeedTicket(etcd);
        var journal = new WorkJournal(etcd, [Ep]);
        var probe = new ShardProbe(new HttpClient(new DeadHandler()));
        var rotator = new AppPasswordRotator(
            etcd, [Ep], new Fakes.FakeSql(), probe,
            new ClaimStore([Ep], etcd, TimeProvider.System), journal, Secrets,
            new AppSecretEnsurer(etcd, [Ep]), snapshot: null);

        // Act
        var outcome = await rotator.TickAsync(await Snapshot(etcd), CancellationToken.None);

        // Assert — отказ до любых мутаций
        outcome.IsSuccess.Should().BeFalse();
        etcd.Txns.Should().BeEmpty();
        etcd.Store["/clusters/shop/app_password"].Value
            .Should().Be("OldPassword000000000000000000A");
    }
}
```

- [ ] **Step 6.2: Прогнать — упасть** — `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~AppPasswordRotatorTests"`. Ожидание: FAIL (тип не существует).

- [ ] **Step 6.3: Реализация** — создать `src/PgWorker.Provisioning/Processes/AppPasswordRotator.cs`:

```csharp
using System.Text.Json;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.Provisioning.Probes;
using PgWorker.Provisioning.Sql;

namespace PgWorker.Provisioning.Processes;

/// <summary>
/// Ротация per-cluster app-пароля по заявке /pgworker/rotations/&lt;C&gt;
/// (arch/14 §5 I, spec §4.3): R1 ensure app-секрета → R2 ALTER ROLE на мастере
/// каждого шарда с dsn (реплики получают pg_authid физической репликацией) →
/// R3 атомарный txn [compare value==OLD][put app_password=NEW; del заявки] →
/// R4 снапшот P12. transient-сбой → заявка жива, пароль в etcd НЕ меняется,
/// следующий тик повторяет с начала со свежим NEW (ALTER идемпотентен
/// перезаписью). Вызывается только держателем клэйма &lt;C&gt;.
/// </summary>
public sealed class AppPasswordRotator(
    IEtcdGateway etcd,
    string[] endpoints,
    ISqlExecutor db,
    ShardProbe probe,
    ClaimStore claims,
    WorkJournal journal,
    InstallSecrets secrets,
    IAppSecretEnsurer appSecret,
    Func<CancellationToken, Task<Result>>? snapshot = null)
{
    private const string Op = "rotate-app-password";

    public async Task<Result<ProcessOutcome>> TickAsync(ClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;

        // Мутации — только держателем живого клэйма (инвариант arch/14 §3.3).
        if (!claims.IsMine(cluster))
            return Result<ProcessOutcome>.Failed(new ApplicationException(
                $"{Op} {cluster}: клэйм не наш (или потерян) — мутации запрещены"));

        // R0: заявка (цикл префикс /pgworker/ не читает — читаем ключ сами).
        var ticket = await GetAsync(TicketKey(cluster), ct);
        if (!ticket.IsSuccess)
            return Result<ProcessOutcome>.Failed(ticket.Error!);
        if (ticket.Value is null)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done); // нет заявки — no-op

        // Битая заявка (не-JSON/без requested_unix) — мусор: удалить с journal-записью.
        if (!IsWellFormed(ticket.Value.Value))
        {
            var cleaned = await DeleteAsync(TicketKey(cluster), ct);
            if (!cleaned.IsSuccess)
                return Result<ProcessOutcome>.Failed(cleaned.Error!);
            await journal.WritePhaseAsync(
                cluster, Op, "malformed-ticket-removed", claims.InstanceId, ticket.Value.Value, ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }

        var started = await journal.WritePhaseAsync(cluster, Op, "started", claims.InstanceId, null, ct);
        if (!started.IsSuccess)
            return Result<ProcessOutcome>.Failed(started.Error!);

        // R1: ensure app-секрета (P1.5) — OLD после этого существует.
        var creds = await appSecret.EnsureAsync(cluster, ct);
        if (!creds.IsSuccess)
            return await FailAsync(cluster, creds.Error!, "ensure-app-secret", ct);

        // R2: ALTER ROLE на мастере каждого ПОДНЯТОГО шарда (dsn есть; шард без
        // dsn — домен AddShardProcess: роль создастся/выравнивается по свежему
        // app_password, spec §4.3 R2).
        var newSecret = AppSecretGenerator.Generate();
        var addresses = await ReadPortAllocAsync(cluster, ct);
        if (!addresses.IsSuccess)
            return await FailAsync(cluster, addresses.Error!, "portalloc", ct);

        foreach (var shard in snap.Shards.Where(s => s.Dsn is not null))
        {
            var master = await ResolveMasterAsync(shard, addresses.Value, ct);
            if (master is null)
                return await FailAsync(cluster,
                    new ApplicationException(
                        $"шард {shard.Name}: мастер недоступен (master-ключ/Patroni REST) — ретрай тиком"),
                    $"waiting-master/{shard.Name}", ct);

            var dsn = DatabaseProvisioner.BuildAdminDsn(master.Host, master.Ports.Pg, snap.Config.DbName, secrets);
            var altered = await db.ExecuteAsync(
                dsn,
                DatabaseProvisioner.BuildAlterAppPasswordSql(new AppCredentials(creds.Value.User, newSecret)),
                ct);
            if (!altered.IsSuccess)
                return await FailAsync(cluster, altered.Error!, $"alter/{shard.Name}", ct);
        }

        // R3: атомарный коммит — put нового пароля + снятие заявки ОДНОЙ txn
        // (нет двойной ротации из-за сбоя между put и del); compare по OLD —
        // внешняя запись etcdctl между R1 и R3 → ретрай тиком со свежим OLD.
        var commit = await TxnAsync(
            TxnRequest.Of(
                [TxnCompare.ValueEqual(PasswordKey(cluster), creds.Value.Password)],
                [
                    new TxnOp.Put(PasswordKey(cluster), newSecret, null),
                    new TxnOp.Delete(TicketKey(cluster), Prefix: false),
                ]),
            ct);
        if (!commit.IsSuccess)
            return await FailAsync(cluster, commit.Error!, "committing", ct);
        if (!commit.Value.Succeeded)
            return await FailAsync(cluster,
                new ApplicationException(
                    "app_password изменился с момента чтения (внешняя запись?) — ретрай тиком"),
                "commit-conflict", ct);

        // R4: снапшот P12 (точка изменения, best-effort делегат) + journal done.
        if (snapshot is not null)
        {
            var shot = await snapshot(ct);
            if (!shot.IsSuccess)
                return await FailAsync(cluster, shot.Error!, "snapshot", ct);
        }

        return await Finish(cluster, "done", ProcessOutcome.Done, ct);
    }

    private static string TicketKey(string cluster) => $"/pgworker/rotations/{cluster}";

    private static string PasswordKey(string cluster) => $"/clusters/{cluster}/app_password";

    // Валидная заявка: JSON с числовым requested_unix (панель §9.8 п.3).
    private static bool IsWellFormed(string raw)
    {
        try
        {
            using var doc = JsonDocument.Parse(raw);
            return doc.RootElement.TryGetProperty("requested_unix", out var unix)
                   && unix.ValueKind == JsonValueKind.Number;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    // Мастер шарда: host из master-ключа (по portalloc) → fallback Patroni REST
    // (паттерн ProvisioningProcess.ResolveMasterAsync, упрощённо для чтения).
    private async Task<NodeAddress?> ResolveMasterAsync(
        ShardSpec shard, IReadOnlyDictionary<string, NodeAddress> addresses, CancellationToken ct)
    {
        var byKey = shard.Master?.Split(':')[0];
        foreach (var (key, addr) in addresses.Where(p =>
                     p.Key.StartsWith($"{shard.Name}/", StringComparison.Ordinal)))
        {
            var node = key.Split('/')[1];
            if (byKey is { Length: > 0 } && (byKey == addr.Host || byKey == node))
                return addr;
        }

        foreach (var pair in addresses.Where(p =>
                     p.Key.StartsWith($"{shard.Name}/", StringComparison.Ordinal)))
        {
            var members = await probe.GetClusterAsync(pair.Value, ct);
            if (!members.IsSuccess)
                continue;
            var master = members.Value.FirstOrDefault(m =>
                m.Role is "master" or "leader" or "primary" && m.State == "running");
            if (master is not null && addresses.TryGetValue($"{shard.Name}/{master.Name}", out var addr))
                return addr;
        }

        return null;
    }

    private async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> ReadPortAllocAsync(
        string cluster, CancellationToken ct)
    {
        var result = await GetAsync($"/pgworker/portalloc/{cluster}", ct);
        if (!result.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(result.Error!);
        if (result.Value is not { } kv)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(
                (IReadOnlyDictionary<string, NodeAddress>)new Dictionary<string, NodeAddress>());

        return Portalloc.Parse(cluster, kv.Value);
    }

    private async Task<Result<ProcessOutcome>> Finish(
        string cluster, string phase, ProcessOutcome outcome, CancellationToken ct)
    {
        var written = await journal.WritePhaseAsync(cluster, Op, phase, claims.InstanceId, null, ct);
        return written.IsSuccess
            ? Result<ProcessOutcome>.Success(outcome)
            : Result<ProcessOutcome>.Failed(written.Error!);
    }

    private async Task<Result<ProcessOutcome>> FailAsync(
        string cluster, Exception error, string phase, CancellationToken ct)
    {
        await journal.WritePhaseAsync(cluster, Op, phase, claims.InstanceId, error.Message, ct);
        return Result<ProcessOutcome>.Failed(error);
    }

    // Failover-обёртки: первый успешный endpoint выигрывает (образец AddShardProcess).
    private async Task<Result<Kv?>> GetAsync(string key, CancellationToken ct)
        => await WithFailoverAsync(endpoint => etcd.GetAsync(endpoint, key, ct));

    private async Task<Result> DeleteAsync(string key, CancellationToken ct)
    {
        Result? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.DeleteAsync(endpoint, key, prefix: false, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }

    private async Task<Result<TxnResult>> TxnAsync(TxnRequest req, CancellationToken ct)
        => await WithFailoverAsync(endpoint => etcd.TxnAsync(endpoint, req, ct));

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

- [ ] **Step 6.4: Прогнать** — `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~AppPasswordRotatorTests"`. Ожидание: PASS (6 тестов). Если `TxnOp.Delete` в тесте недоступен как pattern-match (`op is TxnOp.Delete d`) — свойство `Key`/`Prefix` публичны (record), проверка компиляции.

- [ ] **Step 6.5: Commit** — `git add -A && git commit -m "feat: AppPasswordRotator — R0-R4, атомарный txn-коммит app_password+заявка (spec §4.3)"`

**Выход:** процесс ротации. **Проверка:** Step 6.4.

---

### Task 7: Интеграция Rotator: `IClusterProcesses` + ReconcileLoop + DI + D2

**Вход:** Task 6 (`AppPasswordRotator`); `ReconcileLoop` default-ветка; `DeprovisioningProcess` D2.

**Files:**
- Modify: `src/PgWorker.App/Loops/ClusterProcesses.cs` (интерфейс + реализация)
- Modify: `src/PgWorker.App/Loops/ReconcileLoop.cs` (default-ветка)
- Modify: `src/PgWorker.App/Program.cs` (DI Rotator + ClusterProcesses)
- Modify: `src/PgWorker.Provisioning/Processes/DeprovisioningProcess.cs` (D2)
- Test: `src/tests/PgWorker.UnitTests/App/ReconcileLoopTests.cs` (FakeProcesses + новый тест)
- Test: `src/tests/PgWorker.UnitTests/Provisioning/DeprovisioningProcessTests.cs`

**Interfaces (Produces):** `IClusterProcesses.RotateAppPasswordAsync(ClusterSnapshot snap, CancellationToken ct) → Task<Result<ProcessOutcome>>`.

**Spec:** §4.3 (порядок в цикле), §3.2 (D2), arch/14 §5 I.

- [ ] **Step 7.1: Failing-тест цикла** — в `ReconcileLoopTests.cs`: в `FakeProcesses` добавить (по образцу `Moved`):

```csharp
        public List<string> Rotated { get; } = [];
```

и метод:

```csharp
        public Task<Result<ProcessOutcome>> RotateAppPasswordAsync(ClusterSnapshot snap, CancellationToken ct)
        {
            using var _ = Track(snap.Config.Cluster, Rotated, callName: "rotate-app-password");
            return Task.FromResult(Result<ProcessOutcome>.Success(ProcessOutcome.Done));
        }
```

новый тест:

```csharp
// AAA: ротация app-пароля — после scale-прохода, до moves (spec §4.3)
[Fact]
public async Task Tick_ActiveCluster_RotatesAfterScaleBeforeMoves()
{
    // Arrange — Active-кластер: надзор → scale → rotate → moves
    SeedCluster("shop", null);
    var processes = new FakeProcesses();
    var loop = CreateLoop(processes);

    // Act
    var tick = await loop.TickAsync(TestContext.Current.CancellationToken);

    // Assert — ротация вызвана между scale и moves (порядок §4.3)
    tick.IsSuccess.Should().BeTrue();
    processes.Rotated.Should().Equal("shop");
    processes.Calls.Should().ContainInOrder("supervise/shop", "rotate-app-password/shop", "moves/shop");
}
```

- [ ] **Step 7.2: Прогнать — упасть** — `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~ReconcileLoopTests"`.
  Ожидание: FAIL компиляции (метода интерфейса нет).

- [ ] **Step 7.3: Реализация.** `ClusterProcesses.cs`: в `IClusterProcesses` после `ScaleShardsAsync` добавить:

```csharp
    /// <summary>Ротация app-пароля по заявке /pgworker/rotations/&lt;C&gt; (spec §4.3,
    /// arch/14 §5 I); no-op без заявки.</summary>
    Task<Result<ProcessOutcome>> RotateAppPasswordAsync(ClusterSnapshot snap, CancellationToken ct);
```

реализация `ClusterProcesses`: ctor-параметр `AppPasswordRotator rotator` (после `removeShards`) + метод:

```csharp
    public Task<Result<ProcessOutcome>> RotateAppPasswordAsync(ClusterSnapshot snap, CancellationToken ct)
        => rotator.TickAsync(snap, ct);
```

`ReconcileLoop.cs`, default-ветка: ПОСЛЕ `scale-shards`-вызова и ДО цикла `evacuate` вставить:

```csharp
                    // Ротация app-пароля (spec §4.3, arch/14 §5 I): короткая плановая
                    // операция — до эвакуаций/переездов, не ждёт длинных moves.
                    await RunClusterOpAsync(cluster, "rotate-app-password",
                        () => processes.RotateAppPasswordAsync(snap, ct), ct);
```

- [ ] **Step 7.4: DI.** `Program.cs`: `ClusterProcesses` регистрируется автоди (`AddSingleton<IClusterProcesses, ClusterProcesses>()` — все процессы резолвятся через ctor), поэтому достаточно ПЕРЕД этой строкой зарегистрировать сам Rotator:

```csharp
// Ротация app-пароля (spec §4.3, arch/14 §5 I): заявка /pgworker/rotations/<C>;
// Active-ветка цикла зовёт через ClusterProcesses (scale → rotate → evacuate → moves).
builder.Services.AddSingleton(sp => new AppPasswordRotator(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<ISqlExecutor>(),
    sp.GetRequiredService<ShardProbe>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<InstallSecrets>(),
    sp.GetRequiredService<IAppSecretEnsurer>(),
    SnapshotDelegate(sp.GetRequiredService<SnapshotJob>())));
```

- [ ] **Step 7.5: D2.** `DeprovisioningProcess.cs`, метод D2 — ПОСЛЕДНИЙ оператор сейчас `return await DeleteAsync($"/pgworker/moves/{cluster}/", prefix: true, ct);`. Заменить на:

```csharp
        // Заявка ротации app-пароля (spec §3.2/D2): точечно — не переживает удаление.
        var delRotation = await DeleteAsync($"/pgworker/rotations/{cluster}", prefix: false, ct);
        if (!delRotation.IsSuccess)
            return delRotation;

        // Заявки переездов (t01, spec §5.3 D2): префикс /pgworker/moves/<C>/ целиком.
        return await DeleteAsync($"/pgworker/moves/{cluster}/", prefix: true, ct);
```

- [ ] **Step 7.6: Тест D2** — в `DeprovisioningProcessTests` в существующий `Tick_FullRemoval_RemovesNodesKeysAndScope` после `// Act`-блока добавить сид (в Arrange) и ассерт:

```csharp
        // Arrange-добавка (внутрь теста, после NewRig): живая заявка ротации
        rig.Etcd.Seed("/pgworker/rotations/shop", """{"requested_unix":1755900100,"requested_by":"admin"}""");
```

и в Assert-часть:

```csharp
        rig.Etcd.Store.Should().NotContainKey("/pgworker/rotations/shop");
```

- [ ] **Step 7.7: Прогнать всё + сборка** — `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~ReconcileLoopTests|FullyQualifiedName~DeprovisioningProcessTests"` (PASS); `dotnet build src/PgWorker.slnx` (0 warnings).

- [ ] **Step 7.8: Commit** — `git add -A && git commit -m "feat: ротация в ReconcileLoop (scale→rotate→evacuate→moves) + D2-чистка заявки (spec §4.3/§3.2)"`

**Выход:** цикл исполняет ротацию; D2 чистит заявку. **Проверка:** Step 7.7.

---

### Task 8: AdminPanel — парсер expected-skip `app_params`

**Вход:** `ClustersParser` уже скипает `app_user/app_password` (строка ~134).

**Files:**
- Modify: `src/AdminPanel.Etcd/Parsing/ClustersParser.cs`
- Test: `src/tests/AdminPanel.UnitTests/ClustersParserTests.cs`

**Spec:** §3.1 (панель не читает), §4.5; arch/adminpanel/02 §2.1.

- [ ] **Step 8.1: Failing-тест** — в `ClustersParserTests.cs` рядом с `Parse_AppSecretKeys_SkippedWithoutUnknown` добавить:

```csharp
    [Fact]
    public void Parse_NodeAppParams_SkippedWithoutUnknown()
    {
        // Arrange — per-node app_params в префиксе нод (spec §3.1: панель не читает)
        var kvs = new List<Kv>
        {
            Kv("/clusters/demo/config", "{\"buckets\":1,\"dbname\":\"demo\"}"),
            Kv("/clusters/demo/shards/shard1/replicas", "2"),
            Kv("/clusters/demo/shards/shard1/nodes/shard1a/state", "RUNNING"),
            Kv("/clusters/demo/shards/shard1/nodes/shard1a/app_params", "sslmode=require"),
            Kv("/clusters/demo/shards/shard1/nodes/shard1b/app_params", "sslmode=verify-full"),
        };

        // Act
        var result = ClustersParser.Parse(kvs);

        // Assert — expected-skip: не unknown, значение не в модели
        result.UnknownKeyCount.Should().Be(0);
        result.Errors.Should().BeEmpty();
        var nodes = result.Clusters.Single().Shards.Single().Nodes;
        nodes.Should().HaveCount(2);
        nodes.Should().OnlyContain(n => n.GetType().GetProperty("AppParams") is null);
    }
```

Если у `NodeInfo` нет и не планируется `AppParams`-поля — последний ассерт избыточен; тогда заменить на:

```csharp
        nodes.Should().HaveCount(2); // app_params не влияет на ноды
```

(использовать упрощённый вариант).

- [ ] **Step 8.2: Прогнать — упасть** — `dotnet test src/tests/AdminPanel.UnitTests --filter "FullyQualifiedName~Parse_NodeAppParams"`. Ожидание: FAIL (`UnknownKeyCount == 2`).

- [ ] **Step 8.3: Реализация** — `ClustersParser.cs`: после case `nodes/<n>/state` добавить:

```csharp
                // Per-node серверные параметры подключения (spec §3.1; ведёт PgWorker,
                // панель не читает): expected-skip без unknownKeys-счётчика.
                case "shards" when segments.Length == 8
                    && segments[4].Length > 0
                    && segments[5] == "nodes"
                    && segments[6].Length > 0
                    && segments[7] == "app_params":
                    break;
```

- [ ] **Step 8.4: Прогнать** — `dotnet test src/tests/AdminPanel.UnitTests --filter "FullyQualifiedName~ClustersParserTests"` (PASS, весь файл).

- [ ] **Step 8.5: Commit** — `git add -A && git commit -m "feat(adminpanel): expected-skip per-node app_params в ClustersParser (spec §4.5)"`

**Выход:** снапшот панели не ломается новыми ключами. **Проверка:** Step 8.4.

---

### Task 9: AdminPanel — команда + эндпоинт `POST …/app-password/rotate`

**Вход:** Task 8; образцы: `RecreateNodeCommand` (guard-цепочка), `OperationsModule` (маппинг ответов), `RecreateNodeApiTests` (integration).

**Files:**
- Create: `src/AdminPanel.Api/Operations/RotateAppPasswordCommand.cs`
- Modify: `src/AdminPanel.Api/Operations/OperationsModule.cs`
- Test: `src/tests/AdminPanel.IntegrationTests/RotateAppPasswordApiTests.cs`

**Interfaces (Produces):**
- `public sealed record RotateAppPasswordCommand(string Cluster) : ICommand<AppPasswordRotatedDto>;`
- `public sealed record AppPasswordRotatedDto(string Cluster, long RequestedUnix, string RequestedBy);`
- `public sealed class RotationAlreadyRequestedException(string cluster) : Exception($"ротация app-пароля {cluster} уже запрошена — дождитесь исполнения (заявка /pgworker/rotations/{cluster})");`
- HTTP: `POST /api/clusters/{cluster}/app-password/rotate` → 201 `AppPasswordRotatedDto` | 404 | 409 | 503.

**Spec:** §4.5; arch/adminpanel/02 §9.8, arch/adminpanel/03 §1.6.

- [ ] **Step 9.1: Реализация команды** — создать `src/AdminPanel.Api/Operations/RotateAppPasswordCommand.cs`:

```csharp
using System.Text.Json;
using AdminPanel.Core;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Client;
using AdminPanel.Etcd.Writing;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Operations;

// Заявка ротации app-пароля (арх-канон arch/02 §9.8, spec §4.5): панель ставит
// /pgworker/rotations/<C> txn-клэймом [version==0]+[put]; выполняет PgWorker
// (AppPasswordRotator): ALTER ROLE на всех шардах + атомарная замена app_password.
// Панель сама в SQL нод не ходит и app_password не пишет/не читает.
public sealed record RotateAppPasswordCommand(string Cluster, string RequestedBy)
    : ICommand<AppPasswordRotatedDto>;

public sealed record AppPasswordRotatedDto(string Cluster, long RequestedUnix, string RequestedBy);

// Живая заявка уже стоит: панель не перезаписывает (отмена — runbook/etcdctl).
public sealed class RotationAlreadyRequestedException(string cluster)
    : Exception($"ротация app-пароля {cluster} уже запрошена — дождитесь исполнения (ключ /pgworker/rotations/{cluster})");

[InjectAsScoped]
public sealed partial class RotateAppPasswordCommandHandler(ISnapshotStore store, IEtcdGateway gateway)
    : ICommandHandler<RotateAppPasswordCommand, AppPasswordRotatedDto>
{
    // Канон тела заявки PgWorker: snake_case (образец TicketBody MoveBucketsCommand).
    private static readonly JsonSerializerOptions TicketJson = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    private sealed record TicketBody(
        [property: JsonPropertyName("requested_unix")] long RequestedUnix,
        [property: JsonPropertyName("requested_by")] string RequestedBy);

    // Имя кластера панели: ^[a-z][a-z0-9_]{0,62}$ (02 §9.3).
    [GeneratedRegex("^[a-z][a-z0-9_]{0,62}$")]
    private static partial Regex ClusterPattern();

    public async ValueTask<Result<AppPasswordRotatedDto>> Handle(
        RotateAppPasswordCommand command, CancellationToken ct)
    {
        var cluster = command.Cluster;

        // 1) Каноническое имя.
        if (!ClusterPattern().IsMatch(cluster))
            return Result<AppPasswordRotatedDto>.Failed(new ClusterNotFoundException(cluster));

        // 2) Активный endpoint.
        var snapshot = store.Current;
        if (snapshot?.Etcd.ActiveEndpoint is not { } endpoint)
            return Result<AppPasswordRotatedDto>.Failed(new EtcdWriteUnavailableException());

        // 3) Config напрямую (снапшот отстаёт до тика): нет → 404; state не Active → 409.
        var config = await ReadKeyAsync(endpoint, $"/clusters/{cluster}/config", ct);
        if (!config.IsSuccess)
            return Result<AppPasswordRotatedDto>.Failed(config.Error!);
        if (config.Value is null)
            return Result<AppPasswordRotatedDto>.Failed(new ClusterNotFoundException(cluster));
        string? state;
        try
        {
            state = ReadState(config.Value);
        }
        catch (JsonException)
        {
            return Result<AppPasswordRotatedDto>.Failed(new InvalidClusterConfigException(cluster));
        }
        if (state is not null)
            return Result<AppPasswordRotatedDto>.Failed(new ClusterNotActiveException(cluster, state));

        // 4) Живая заявка → 409 (после исполнения PgWorker ключ исчезает — POST валиден).
        var key = $"/pgworker/rotations/{cluster}";
        var ticket = await ReadKeyAsync(endpoint, key, ct);
        if (!ticket.IsSuccess)
            return Result<AppPasswordRotatedDto>.Failed(ticket.Error!);
        if (ticket.Value is not null)
            return Result<AppPasswordRotatedDto>.Failed(new RotationAlreadyRequestedException(cluster));

        // 5) Клэйм-txn: compare version==0 + put (образец §9.7 п.5; API панели —
        // TxnAsync(endpoint, compares, puts)). Проигрыш → 409.
        var requestedUnix = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var payload = JsonSerializer.Serialize(
            new TicketBody(requestedUnix, command.RequestedBy), TicketJson);
        var txn = await gateway.TxnAsync(
            endpoint, [new TxnCompare(key, 0)], [new KvPut(key, payload)], ct);
        if (!txn.IsSuccess)
            return Result<AppPasswordRotatedDto>.Failed(
                new EtcdWriteUnavailableException()); // транспортный сбой — 503
        if (!txn.Value.Succeeded)
            return Result<AppPasswordRotatedDto>.Failed(new RotationAlreadyRequestedException(cluster));

        return Result<AppPasswordRotatedDto>.Success(
            new AppPasswordRotatedDto(cluster, requestedUnix, command.RequestedBy));
    }

    private async Task<Result<string?>> ReadKeyAsync(string endpoint, string key, CancellationToken ct)
    {
        var range = await gateway.RangeAsync(endpoint, key, ct);
        if (!range.IsSuccess)
            return Result<string?>.Failed(range.Error!);
        return Result<string?>.Success(range.Value.FirstOrDefault(kv => kv.Key == key)?.Value);
    }

    private static string? ReadState(string raw)
    {
        using var doc = JsonDocument.Parse(raw);
        return doc.RootElement.TryGetProperty("state", out var s) && s.ValueKind == JsonValueKind.String
            ? s.GetString()
            : null;
    }
}
```

(using-блок — по образцу `RecreateNodeCommand.cs`: `System.Text.Json`, `System.Text.Json.Serialization` (для `JsonIgnoreCondition`), `System.Text.RegularExpressions`, `AdminPanel.Core`, `AdminPanel.Etcd`, `AdminPanel.Etcd.Client`, `AdminPanel.Etcd.Writing`, `AdminPanel.Infrastructure`, `AdminPanel.Infrastructure.CQRS`, `AdminPanel.Infrastructure.DI`.)

- [ ] **Step 9.2: Эндпоинт** — `OperationsModule.cs`, ПОСЛЕ moves-эндпоинта добавить:

```csharp
        // POST /api/clusters/{cluster}/app-password/rotate — заявка ротации app-пароля
        // (02 §9.8): панель только клэймит заявку; выполнение — PgWorker.
        endpoints.MapPost("/api/clusters/{cluster}/app-password/rotate", async (
            string cluster, ClaimsPrincipal user, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<RotateAppPasswordCommand, AppPasswordRotatedDto>(
                new RotateAppPasswordCommand(cluster, user.Identity?.Name ?? "adminpanel"), ct);
            if (result.IsSuccess)
                return Results.Created($"/api/clusters/{cluster}", result.Value);

            return result.Error switch
            {
                ClusterNotFoundException => Results.Problem(
                    statusCode: StatusCodes.Status404NotFound, title: "Cluster not found",
                    detail: result.Error.Message),
                ClusterNotActiveException or RotationAlreadyRequestedException => Results.Problem(
                    statusCode: StatusCodes.Status409Conflict, title: "Rotation rejected",
                    detail: result.Error.Message),
                _ => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed",
                    detail: result.Error!.Message),
            };
        });
```

- [ ] **Step 9.3: Integration-тест** — создать `src/tests/AdminPanel.IntegrationTests/RotateAppPasswordApiTests.cs` (по образцу `RecreateNodeApiTests`: `AuthWebFactory`, `EtcdContainerFixture`, `SetLiveSnapshot`, `ApiTestLogin`):

```csharp
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using AdminPanel.Core;
using AdminPanel.Etcd;
using FluentAssertions;
using Xunit;

namespace AdminPanel.IntegrationTests;

// POST /api/clusters/{cluster}/app-password/rotate против реального etcd (arch/02 §9.8):
// клэйм-txn заявки /pgworker/rotations/<C>, 409 на живую заявку/не-Active, 404/503.
[Collection("api")]
public class RotateAppPasswordApiTests(AuthWebFactory factory, EtcdContainerFixture fixture)
    : IClassFixture<EtcdContainerFixture>
{
    private readonly AuthWebFactory _factory = factory;

    private void SetLiveSnapshot(string cluster)
    {
        var etcd = new EtcdStatus(
            true,
            [new EtcdEndpoint(fixture.Endpoint, true, 1, "3.5.21", null, null, null, null, [])],
            [], [], fixture.Endpoint, false, _factory.Time.GetUtcNow(), 0);
        _factory.Snapshot = InspectionSnapshots.Fixture(_factory.Time.GetUtcNow()) with
        {
            Etcd = etcd,
            Clusters =
            [
                new ClusterInfo(cluster, cluster, 6, 1755900000, ClusterState.Active,
                [
                    new ShardInfo("s1", $"host=s1a port=5432 dbname={cluster} user=bucket_admin",
                        ["s1a"], 5432, cluster, "bucket_admin", 2, null,
                        [new NodeInfo("s1a", "RUNNING")], null),
                ],
                [.. Enumerable.Range(0, 6).Select(i => new BucketInfo(i, "s1", BucketState.Active, null))],
                []),
            ],
        };
    }

    private async Task<HttpClient> LoginAsync() => await ApiTestLogin.LoginAsync(_factory);

    private async Task SeedAsync(params (string Key, string Value)[] kvs)
    {
        foreach (var (key, value) in kvs)
            await EtcdSeed.PutAsync(fixture.Endpoint, key, value, TestContext.Current.CancellationToken);
    }

    private async Task<string?> ReadKeyAsync(string key)
    {
        var gateway = EtcdTestHarness.NewGateway();
        var range = await gateway.RangeAsync(fixture.Endpoint, key, TestContext.Current.CancellationToken);
        return range.Value.FirstOrDefault(kv => kv.Key == key)?.Value;
    }

    private async Task SeedActiveConfigAsync(string cluster)
        => await SeedAsync(($"/clusters/{cluster}/config",
            $$"""{"buckets":6,"dbname":"{{cluster}}","created_unix":1755900000}"""));

    [Fact]
    public async Task Rotate_ActiveCluster_ClaimsTicketWithAudit()
    {
        // Arrange — Active-кластер в снапшоте и в etcd; заявки нет
        const string cluster = "rot1";
        SetLiveSnapshot(cluster);
        await SeedActiveConfigAsync(cluster);
        using var client = await LoginAsync();

        // Act
        var response = await client.PostAsync($"/api/clusters/{cluster}/app-password/rotate", null);

        // Assert — 201 с телом; заявка в etcd с аудполями панели (§9.8 п.3)
        response.StatusCode.Should().Be(HttpStatusCode.Created);
        var dto = await response.Content.ReadFromJsonAsync<JsonElement>();
        dto.GetProperty("cluster").GetString().Should().Be(cluster);
        dto.GetProperty("requestedBy").GetString().Should().Be("admin");
        var ticket = await ReadKeyAsync($"/pgworker/rotations/{cluster}");
        ticket.Should().NotBeNull();
        ticket.Should().Contain("admin").And.Contain("requested_unix");
    }

    [Fact]
    public async Task Rotate_LiveTicket_Conflict()
    {
        // Arrange — заявка уже стоит (повтор до исполнения → 409, §9.8 п.2)
        const string cluster = "rot2";
        SetLiveSnapshot(cluster);
        await SeedActiveConfigAsync(cluster);
        await SeedAsync(($"/pgworker/rotations/{cluster}",
            """{"requested_unix":1755900100,"requested_by":"someone"}"""));
        using var client = await LoginAsync();

        // Act
        var response = await client.PostAsync($"/api/clusters/{cluster}/app-password/rotate", null);

        // Assert — 409, значение заявки НЕ перезаписано
        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
        (await ReadKeyAsync($"/pgworker/rotations/{cluster}"))
            .Should().Contain("someone");
    }

    [Fact]
    public async Task Rotate_NotActiveCluster_Conflict()
    {
        // Arrange — config с state=NOT_INITIALIZED (§9.8 п.1)
        const string cluster = "rot3";
        SetLiveSnapshot(cluster);
        await SeedAsync(($"/clusters/{cluster}/config",
            $$"""{"buckets":6,"dbname":"{{cluster}}","created_unix":1755900000,"state":"NOT_INITIALIZED"}"""));
        using var client = await LoginAsync();

        // Act
        var response = await client.PostAsync($"/api/clusters/{cluster}/app-password/rotate", null);

        // Assert — 409, заявки нет
        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
        (await ReadKeyAsync($"/pgworker/rotations/{cluster}")).Should().BeNull();
    }

    [Fact]
    public async Task Rotate_UnknownCluster_NotFound()
    {
        // Arrange — имени нет в etcd (404 по §9.8 п.1)
        SetLiveSnapshot("rot4");
        using var client = await LoginAsync();

        // Act
        var response = await client.PostAsync("/api/clusters/nosuch/app-password/rotate", null);

        // Assert — 404
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Rotate_NoSnapshot_ServiceUnavailable()
    {
        // Arrange — снапшота нет (etcd недоступен) → 503
        _factory.Snapshot = null;
        using var client = await LoginAsync();

        // Act
        var response = await client.PostAsync("/api/clusters/rot5/app-password/rotate", null);

        // Assert — 503
        response.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
    }
}
```

(сверить хелперы `InspectionSnapshots`/`EtcdTestHarness`/`ApiTestLogin`/`AuthWebFactory.Snapshot` по фактическим `RecreateNodeApiTests`/`MovesApiTests`; конструкторы записей `ClusterInfo`/`ShardInfo`/`EtcdStatus` — по фактическим сигнатурам, при расхождении скопировать точный вызов из `RecreateNodeApiTests.SetLiveSnapshot`.)

- [ ] **Step 9.4: Прогнать** — сначала build: `dotnet build src/PgWorker.slnx`; затем `dotnet test src/tests/AdminPanel.IntegrationTests --filter "FullyQualifiedName~RotateAppPasswordApiTests"`. Ожидание: PASS (5 тестов; etcd поднимется Testcontainers'ом).

- [ ] **Step 9.5: Commit** — `git add -A && git commit -m "feat(adminpanel): POST /api/clusters/{c}/app-password/rotate — заявка ротации txn-клэймом (arch/02 §9.8, spec §4.5)"`

**Выход:** API-мутация панели. **Проверка:** Step 9.4.

---

### Task 10: Фронтенд — нотификации + кнопка «Сменить app-пароль»

**Вход:** Task 9 (эндпоинт); образцы: `DeleteClusterButton.tsx`, `queries.ts` (`recreateNode`), `ClusterDetailsPage.tsx` (кнопки в шапке). Notification-инфраструктуры во фронте нет (только `@mantine/core` + `@mantine/hooks` ^9.5.2; провайдеры — `frontend/src/main.tsx`) — подключаем `@mantine/notifications` (спецпакет Mantine, версия синхронна core).

**Files:**
- Modify: `frontend/package.json` + `frontend/package-lock.json` (новая зависимость `@mantine/notifications`)
- Modify: `frontend/src/main.tsx` (стили + `<Notifications />`)
- Modify: `frontend/src/api/dto.ts` (новый DTO)
- Modify: `frontend/src/api/queries.ts` (функция)
- Create: `frontend/src/pages/cluster-details/RotateAppPasswordButton.tsx`
- Modify: `frontend/src/pages/ClusterDetailsPage.tsx` (кнопка в шапке)

**Spec:** §4.6 (в т.ч. success-нотификация «заявка отправлена; выполняет PgWorker»); arch/adminpanel/03 §1.6/§3.

- [ ] **Step 10.1: DTO** — в `frontend/src/api/dto.ts` рядом с `NodeRecreatedDto` (или в конец) добавить:

```ts
// POST /api/clusters/{cluster}/app-password/rotate — заявка ротации app-пароля
// (arch/03 §1.6, протокол arch/02 §9.8): панель пароль не знает — только факт заявки.
export interface AppPasswordRotatedDto {
  cluster: string;
  requestedUnix: number;
  requestedBy: string;
}
```

- [ ] **Step 10.2: API-функция** — в `frontend/src/api/queries.ts` (импорт `AppPasswordRotatedDto` в существующий import-блок из `./dto`) в конец добавить:

```ts
// POST /api/clusters/{cluster}/app-password/rotate — заявка ротации app-пароля
// (arch/02 §9.8): ставит /pgworker/rotations/<C>; выполняет PgWorker (AppPasswordRotator).
export function rotateAppPassword(cluster: string): Promise<AppPasswordRotatedDto> {
  return apiFetch<AppPasswordRotatedDto>(
    `/api/clusters/${encodeURIComponent(cluster)}/app-password/rotate`,
    { method: 'POST' });
}
```

- [ ] **Step 10.3: Нотификации @mantine/notifications** (spec §4.6 — success-нотификация после 201; инфраструктуры во фронте нет):

```bash
cd frontend && npm install @mantine/notifications@^9.5.2
```

(npm обновит `package.json` + `package-lock.json`; версия синхронна `@mantine/core` ^9.5.2.)

Затем `frontend/src/main.tsx`: импорт стилей рядом с `@mantine/core/styles.css` и компонент-провайдер внутри `MantineProvider` (перед `QueryClientProvider`):

```tsx
import '@mantine/core/styles.css';
import '@mantine/notifications/styles.css';
import { Notifications } from '@mantine/notifications';
```

```tsx
  <StrictMode>
    <MantineProvider defaultColorScheme="dark">
      <Notifications />
      <QueryClientProvider client={queryClient}>
```

- [ ] **Step 10.4: Кнопка** — создать `frontend/src/pages/cluster-details/RotateAppPasswordButton.tsx` (после 201 — success-нотификация «заявка отправлена; выполняет PgWorker», spec §4.6):

```tsx
// Кнопка «Сменить app-пароль» в шапке деталей кластера: подтверждение →
// POST /api/clusters/{cluster}/app-password/rotate → заявка /pgworker/rotations/<C>
// (arch/02 §9.8). Выполняет PgWorker (ALTER ROLE на всех шардах + новый пароль в
// etcd); после применения подключения со старым паролем отвергаются, пока
// приложение не перечитает app_password — предупреждение в модалке (spec О2) и
// success-нотификация после 201 (spec §4.6).
import { useMutation } from '@tanstack/react-query';
import { Alert, Button, Group, List, Modal, Stack, Text } from '@mantine/core';
import { notifications } from '@mantine/notifications';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { rotateAppPassword } from '../../api/queries';

export function RotateAppPasswordButton({ name }: { name: string }) {
  const [opened, setOpened] = useState(false);
  const mutation = useMutation({
    mutationFn: () => rotateAppPassword(name),
    onSuccess: () => {
      setOpened(false);
      notifications.show({
        color: 'green',
        title: 'Заявка отправлена',
        message: 'Смену app-пароля выполнит PgWorker (фоновые тики). После применения '
          + 'приложение должно перечитать app_password из etcd.',
        autoClose: 8000,
      });
    },
  });

  // Ошибка сервера: 409 «уже запрошена» / 503 etcd / прочие ProblemDetails.
  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  return (
    <>
      <Button variant="light" onClick={() => setOpened(true)}>Сменить app-пароль</Button>
      <Modal opened={opened} onClose={() => setOpened(false)} title="Сменить app-пароль" centered>
        <Stack gap="sm">
          <Text>
            Кластер <b>{name}</b>: PgWorker сменит пароль роли <b>app</b> на всех нодах и
            обновит ключ <b>app_password</b> в etcd.
          </Text>
          <Alert color="yellow" variant="light" title="Внимание">
            После применения (секунды) подключения со старым паролем начнут отвергаться,
            пока приложение не перечитает app_password из etcd. Выполняйте в тихое окно.
            <List size="sm" mt={4}>
              <List.Item>заявка ставится в очередь и выполняется фоново (тики PgWorker)</List.Item>
              <List.Item>при недоступном шарде ротация повторяется автоматически</List.Item>
            </List>
          </Alert>
          {serverError ? <Alert color="red" variant="light">{serverError.message}</Alert> : null}
          <Group justify="flex-end">
            <Button variant="default" onClick={() => setOpened(false)}>Отмена</Button>
            <Button loading={mutation.isPending} onClick={() => mutation.mutate()}>
              Сменить пароль
            </Button>
          </Group>
        </Stack>
      </Modal>
    </>
  );
}
```

- [ ] **Step 10.5: Страница** — `ClusterDetailsPage.tsx`: импорт `import { RotateAppPasswordButton } from './cluster-details/RotateAppPasswordButton';` и заменить строку шапки

```tsx
          {toRemove ? null : <DeleteClusterButton name={data.name} />}
```

на

```tsx
          {/* Ротация — только Active (у NOT_INITIALIZED пароль ещё не используется);
              у TO_REMOVE обе кнопки скрыты (обратного перехода нет, arch/02 §9.4). */}
          {toRemove ? null : (
            <Group gap="sm">
              {data.state === 'ACTIVE' ? <RotateAppPasswordButton name={data.name} /> : null}
              <DeleteClusterButton name={data.name} />
            </Group>
          )}
```

- [ ] **Step 10.6: Сборка фронтенда** — `cd frontend && npm run build` (после `npm install` из Step 10.3; полный чистый прогон — `npm ci && npm run build`: lock обновлён на предыдущем шаге). Ожидание: exit 0, SPA-бандл собран в `src/AdminPanel.Api/wwwroot`, tsc без ошибок.

- [ ] **Step 10.7: Commit** — `git add -A && git commit -m "feat(adminpanel-ui): @mantine/notifications + кнопка «Сменить app-пароль» с нотификацией заявки (spec §4.6)"`

**Выход:** UI-мутация с success-нотификацией. **Проверка:** Step 10.6 (сборка) + ручной smoke при желании (`dev-stand/adminpanel` — опционально, вне обязательных критериев).

---

### Task 11: E2E (PgWorker) + финальный полный прогон

**Вход:** Tasks 1-10; образец `E2eAppSecretScenarios` (сид кластера, `StartHostAsync`, `WaitForAsync`, коннект-проба по фрагментам DSN).

**Files:**
- Create: `src/tests/PgWorker.IntegrationTests/E2e/E2eAppParamsScenarios.cs`
- Create: `src/tests/PgWorker.IntegrationTests/E2e/E2eRotateScenarios.cs`

**Spec:** §7.2, §7.3 (критерии), §7.5 (ротация e2e), §7.10.

- [ ] **Step 11.1: E2E app_params** — создать `E2eAppParamsScenarios.cs` (сид = копия `SeedClusterAsync` из `E2eAppSecretScenarios`, кластер `appparams`):

```csharp
using System.Text.RegularExpressions;
using Npgsql;
using PgWorker.Etcd.Client;
using PgWorker.IntegrationTests.Docker;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// E2E per-node app_params (spec §7.2/§7.3): provisioning обеспечивает ключ
// каждой ноды дефолтом; значение стабильно между тиками; ручная правка не
// перезаписывается (put-if-absent, миграция надзора).
[Collection(E2eCollection.Name)]
public class E2eAppParamsScenarios(E2eFixture fixture)
{
    private const string Cluster = "appparams";

    private string Endpoint => fixture.EtcdEndpoint;

    private EtcdGateway G => fixture.Gateway;

    [Fact]
    public async Task AppParams_Provisioning_AllNodesGetDefaultAndStable()
    {
        // Arrange — сид NOT_INITIALIZED без app_params-ключей (генерирует PgWorker)
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await SeedClusterAsync(Cluster);
        await using var app = await fixture.StartHostAsync("appparams", ct: ct);

        // Act — ждать SQL-фазы обоих шардов (dsn записан)
        var provisioned = await E2eFixture.WaitForAsync(async () =>
            await GetOrNullAsync($"/clusters/{Cluster}/shards/shard1/dsn") is not null
            && await GetOrNullAsync($"/clusters/{Cluster}/shards/shard2/dsn") is not null,
            TimeSpan.FromSeconds(360), ct);

        // Assert 1 (критерий 2): у КАЖДОЙ ноды обоих шардов app_params = дефолт
        // конфига ("sslmode=require"), без user/password в значении.
        provisioned.Should().BeTrue("provisioning дошёл до SQL-фазы (dsn обоих шардов)");
        foreach (var shard in new[] { "shard1", "shard2" })
            foreach (var node in new[] { "a", "b" })
            {
                var kv = await GetOrNullAsync($"/clusters/{Cluster}/shards/{shard}/nodes/{shard}{node}/app_params");
                kv.Should().NotBeNull($"app_params узла {shard}{node} обеспечен");
                kv!.Value.Should().Be("sslmode=require");
                kv.Value.Should().NotContainAny(["user=", "password="], "клиентские параметры не входят (spec §3.1)");
            }

        // Assert 2 (критерий 2): стабильность между тиками + ручная правка жива
        // (миграция/ensure не перезаписывают существующее — put-if-absent).
        await G.PutAsync(Endpoint,
            $"/clusters/{Cluster}/shards/shard1/nodes/shard1a/app_params",
            "sslmode=verify-full", null, ct);
        await Task.Delay(TimeSpan.FromSeconds(5), ct); // ≥2 тика scan
        (await GetOrNullAsync($"/clusters/{Cluster}/shards/shard1/nodes/shard1a/app_params"))!.Value
            .Should().Be("sslmode=verify-full", "ручное значение не перезаписано (spec §3.1)");
        (await GetOrNullAsync($"/clusters/{Cluster}/shards/shard1/nodes/shard1b/app_params"))!.Value
            .Should().Be("sslmode=require", "прочие ноды стабильны");
    }

    // Сид кластера в стиле панели (копия E2eAppSecretScenarios.SeedClusterAsync).
    private async Task SeedClusterAsync(string cluster)
    {
        var ct = TestContext.Current.CancellationToken;
        var config = $$"""
            {"buckets":2,"dbname":"{{cluster}}","created_unix":1755800000,"state":"NOT_INITIALIZED","bucket_admin_password":"{{E2eFixture.BucketAdminPassword}}"}
            """;
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/config", config, null, ct);
        foreach (var shard in new[] { "shard1", "shard2" })
        {
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/replicas", "2", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/nodes/{shard}a/state", "NOT_INITIALIZED", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/nodes/{shard}b/state", "NOT_INITIALIZED", null, ct);
        }

        for (var i = 0; i < 2; i++)
        {
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/buckets/routing/bucket_{i}", $"shard{i + 1}", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/buckets/status/bucket_{i}",
                """{"state":"NOT_INITIALIZED"}""", null, ct);
        }
    }

    private async Task<Kv?> GetOrNullAsync(string key)
        => (await G.GetAsync(Endpoint, key, TestContext.Current.CancellationToken)).Value;
}
```

- [ ] **Step 11.2: E2E ротации** — создать `E2eRotateScenarios.cs`:

```csharp
using System.Text.RegularExpressions;
using Npgsql;
using PgWorker.Etcd.Client;
using PgWorker.IntegrationTests.Docker;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// E2E ротации app-пароля (spec §7.5): заявка etcdctl-формой → заявка исчезла,
// app_password изменился, новый пароль подключается, старый отвергается.
[Collection(E2eCollection.Name)]
public class E2eRotateScenarios(E2eFixture fixture)
{
    private const string Cluster = "rotate";

    private string Endpoint => fixture.EtcdEndpoint;

    private EtcdGateway G => fixture.Gateway;

    [Fact]
    public async Task Rotate_TicketRotatesPasswordOnAllShards()
    {
        // Arrange — рабочий кластер (provisioning завершён), известен старый пароль
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await SeedClusterAsync(Cluster);
        await using var app = await fixture.StartHostAsync("rotate", ct: ct);
        var provisioned = await E2eFixture.WaitForAsync(async () =>
            await GetOrNullAsync($"/clusters/{Cluster}/shards/shard1/dsn") is not null
            && await GetOrNullAsync($"/clusters/{Cluster}/shards/shard2/dsn") is not null,
            TimeSpan.FromSeconds(360), ct);
        provisioned.Should().BeTrue("кластер поднялся");
        var oldPassword = await fixture.GetAppPasswordAsync(Cluster, ct);

        // Act — заявка ротации (формат панели §9.8)
        await G.PutAsync(Endpoint, $"/pgworker/rotations/{Cluster}",
            $$"""{"requested_unix":{{DateTimeOffset.UtcNow.ToUnixTimeSeconds()}},"requested_by":"e2e"}""",
            null, ct);

        // Assert 1 (критерий 5а/5б): заявка исполнена и удалена; пароль сменился
        var rotated = await E2eFixture.WaitForAsync(async () =>
        {
            var password = await fixture.GetAppPasswordAsync(Cluster, ct);
            return password != oldPassword
                && await GetOrNullAsync($"/pgworker/rotations/{Cluster}") is null;
        }, TimeSpan.FromSeconds(120), ct);
        rotated.Should().BeTrue("заявка исполнена: пароль изменён, ключ заявки удалён");
        var newPassword = await fixture.GetAppPasswordAsync(Cluster, ct);
        Regex.IsMatch(newPassword, "^[A-Za-z0-9]{32}$").Should().BeTrue();

        // Assert 2 (критерий 5в/5г): новый пароль подключается, старый отвергается
        // (проба по фрагментам multi-host dsn — образец E2eAppSecretScenarios).
        var dsn = (await GetOrNullAsync($"/clusters/{Cluster}/shards/shard1/dsn"))!.Value;
        var hosts = Regex.Match(dsn, "host=([^ ]+)").Groups[1].Value.Split(',');
        var ports = Regex.Match(dsn, "port=([^ ]+)").Groups[1].Value.Split(',');
        var newWorks = false;
        foreach (var (host, port) in hosts.Zip(ports))
            newWorks |= await E2eFixture.WaitForAsync(async () =>
            {
                try
                {
                    await using var con = new NpgsqlConnection(
                        $"Host={host};Port={port};Database={Cluster};Username=app;" +
                        $"Password={newPassword};Timeout=5;SSL Mode=Require;Trust Server Certificate=true");
                    await con.OpenAsync(ct);
                    await using var cmd = new NpgsqlCommand("SELECT 1", con);
                    return await cmd.ExecuteScalarAsync(ct) is 1;
                }
                catch (NpgsqlException)
                {
                    return false;
                }
            }, TimeSpan.FromSeconds(60), ct);
        newWorks.Should().BeTrue("новый пароль подключается user=app");

        var oldRejected = false;
        foreach (var (host, port) in hosts.Zip(ports))
            oldRejected |= await E2eFixture.WaitForAsync(async () =>
            {
                try
                {
                    await using var con = new NpgsqlConnection(
                        $"Host={host};Port={port};Database={Cluster};Username=app;" +
                        $"Password={oldPassword};Timeout=5;SSL Mode=Require;Trust Server Certificate=true");
                    await con.OpenAsync(ct);
                    return false; // старый пароль всё ещё работает — ждём отвержения
                }
                catch (NpgsqlException)
                {
                    return true; // отвергнут — ожидаемо
                }
            }, TimeSpan.FromSeconds(60), ct);
        oldRejected.Should().BeTrue("старый пароль отвергается (auth fail)");
    }

    // Сид кластера в стиле панели (копия E2eAppSecretScenarios.SeedClusterAsync).
    private async Task SeedClusterAsync(string cluster)
    {
        var ct = TestContext.Current.CancellationToken;
        var config = $$"""
            {"buckets":2,"dbname":"{{cluster}}","created_unix":1755800000,"state":"NOT_INITIALIZED","bucket_admin_password":"{{E2eFixture.BucketAdminPassword}}"}
            """;
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/config", config, null, ct);
        foreach (var shard in new[] { "shard1", "shard2" })
        {
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/replicas", "2", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/nodes/{shard}a/state", "NOT_INITIALIZED", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/nodes/{shard}b/state", "NOT_INITIALIZED", null, ct);
        }

        for (var i = 0; i < 2; i++)
        {
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/buckets/routing/bucket_{i}", $"shard{i + 1}", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/buckets/status/bucket_{i}",
                """{"state":"NOT_INITIALIZED"}""", null, ct);
        }
    }

    private async Task<Kv?> GetOrNullAsync(string key)
        => (await G.GetAsync(Endpoint, key, TestContext.Current.CancellationToken)).Value;
}
```

- [ ] **Step 11.3: Полный прогон (критерий 10)** — из корня worktree:

```bash
dotnet build src/PgWorker.slnx
dotnet test src/tests/PgWorker.UnitTests
dotnet test src/tests/AdminPanel.UnitTests
dotnet test src/tests/AdminPanel.IntegrationTests
dotnet test src/tests/PgWorker.IntegrationTests
```

Ожидание: всё зелёное (e2e с docker: выполняются; без docker — skip через `DockerTrait.SkipIfUnavailable`, это допустимый исход стенда без docker).

- [ ] **Step 11.4: Commit** — `git add -A && git commit -m "test(e2e): app_params per-node дефолт/стабильность + ротация app-пароля end-to-end (spec §7.2/§7.3/§7.5)"`

**Выход:** критерии приёмки §7 закрыты. **Проверка:** Step 11.3.

---

## Решения, принятые в плане (не выводится из spec напрямую — фиксирую)

1. **Пустое значение app_params в модели = `""`** (не null): различие «ключа нет» (null → миграция допишет) и «ключ есть, но пустой» (не трогаем) нужно надзору; в spec §3.1 оба случая описаны, здесь зафиксировано кодирование.
2. **`AppParamsEnsurer` без предварительного GET**: сразу txn `[NotExists]+[put]` — существующий ключ корректно разрешается проигрышем compare (дешевле и атомарнее GET-then-put).
3. **Rotator не трогает панельные guard-проверки заявки** (`op`-поля нет): формат заявки минимален (`requested_unix`/`requested_by`), «валидность» = JSON с числовым `requested_unix`; мусор удаляется с journal-записью (spec §4.3/arch §5 I).
4. **Порядок задач**: модель/ensure раньше ротации — каждая задача независимо тестируема и компилирует решение (Program.cs правится в той же задаче, где меняется ctor).
5. **e2e-кластеры с уникальными именами** (`appparams`, `rotate`) — общий etcd коллекции E2eCollection; сиды — копия проверенного `E2eAppSecretScenarios.SeedClusterAsync`.
6. **Фронт-кнопка для `NOT_INITIALIZED` скрыта** (не только TO_REMOVE): у неподнятого кластера пароль ещё не используется клиентами — ротация бессмысленна (в spec §4.6 «только ACTIVE»).
7. **Success-нотификация после 201 — реализуется, а не отклоняется** (ревью Фазы 4): spec §4.6 формулирует утвердительно; notification-инфраструктуры во фронте не было — подключён минимальный `@mantine/notifications` (версия синхронна `@mantine/core` ^9.5.2, `<Notifications />` в `main.tsx` внутри `MantineProvider`), `notifications.show(...)` в `onSuccess` кнопки. Ошибки мутации остаются инлайн-Alert'ом модалки (ProblemDetails-паттерн `DeleteClusterButton`) — не дублируем их тостами.

## Self-review (выполнен после написания)

- **Покрытие spec:** §3.1 → Tasks 1-5, 8; §3.2 → Tasks 6-7; §4.1 → Task 1; §4.2 → Tasks 2-5; §4.3 → Tasks 6-7; §4.4 → Task 4; §4.5 → Tasks 8-9; §4.6 → Task 10 (включая success-нотификацию через `@mantine/notifications` — Step 10.3/10.4, правка ревью Фазы 4); §4.7 (не меняется) — подтверждено составом правок; фазы §5 = Tasks 1-11; критерии §7.1-7.10 → Tasks 1-11 (7.4 — Tasks 1/8, 7.8 — Task 9, 7.9 — Task 10, 7.10 — Task 11).
- **Плейсхолдеры:** шаги содержат код; пометки «сверить по фактическим …» — только там, где план не может знать точный namespace/сигнатуру чужого тест-хелпера, с указанием точного файла-образца.
- **Типы:** `IAppParamsEnsurer.EnsureShardAsync(string, string, IEnumerable<string>, CancellationToken)→Task<Result>` единообразен в Tasks 2-5; `RotateAppPasswordAsync(ClusterSnapshot, CancellationToken)→Task<Result<ProcessOutcome>>` в Tasks 6-7; DTO `AppPasswordRotatedDto(Cluster, RequestedUnix, RequestedBy)` — Tasks 9-10.
