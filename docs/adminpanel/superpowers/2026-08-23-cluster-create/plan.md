# t12-cluster-create — план реализации

> **Для агентных исполнителей:** ОБЯЗАТЕЛЬНЫЙ SUB-SKILL: superpowers:subagent-driven-development
> (рекомендуется) или superpowers:executing-plans — исполнять по задачам; шаги
> отмечаются чекбоксами (`- [ ]`).

**Цель:** создание шардированного кластера из вкладки «Кластеры» — форма +
единственная мутация панели: запись структуры кластера в etcd (txn-клэйм
имени, состояния `NOT_INITIALIZED`, заявки `request_*` на ноду).

**Архитектура:** read-only конвейер не меняется; добавляется CQRS-команда
(`ICommand`/`ICommandHandler` из Puzzle, без БД-контекстов) поверх
минимальных write-методов того же `IEtcdGateway` (`/v3/kv/txn|put|delete-range`);
клэйм-txn гарантирует уникальность имени, остальное пишется пакетом PUT с
компенсацией при сбое. Снапшот подхватывает новые ключи очередным тиком
refresher'а; DTO чтения/алерты учат `NOT_INITIALIZED` и `request_*`.

**Стек:** .NET 10 Minimal API (xunit v3 + FluentAssertions + Testcontainers),
React+Vite+TS (Mantine + TanStack Query), etcd 3.5 HTTP JSON gateway.

**Spec:** `docs/superpowers/2026-08-23-cluster-create/spec.md` — план
аргументируется от spec; исполнители читают оба файла. Контракт —
`arch/02-etcd-contract.md` §9, `arch/03-panels.md` §1–4 (уже обновлены).

## Глобальные ограничения

- `TreatWarningsAsErrors=true`, `net10.0`, `LangVersion=latest`,
  `Nullable=enable` — как в `src/Directory.Build.props`; новые пакеты не
  добавляются (CPM не трогаем).
- Идентификаторы английские; комментарии и тексты UI русские; тесты — с
  комментариями по AAA (`// Arrange`, `// Act`, `// Assert`).
- Все пути — от корня worktree
  `/Users/demakaev/ZCodeProject/worktrees/feat-cluster-create`.
- Работа в ветке `feat-cluster-create` (уже текущая в worktree). Коммиты —
  свободно, по шагам; финальный шаг — удаление roadmap-тега (Задача 13).
- Проверки бэкенда: `dotnet build src/AdminPanel.slnx` и
  `dotnet test src/AdminPanel.slnx` (нужен Docker для Testcontainers).
- Проверки фронта: `cd frontend && npm run build` (включает `tsc`).
- Мутация панели строго одна: `POST /api/clusters`; никаких других
  write-методов/эндпоинтов не появляется.

## Карта файлов

```
src/AdminPanel.Infrastructure/CQRS/
  ICommand.cs (new), ICommandHandler.cs (new), IHandler.cs (mod: HandleCommand)
src/AdminPanel.Core/
  ClusterInfo.cs (mod: ClusterState, NodeInfo, BucketState.NotInitialized, поля)
  HaScope.cs (mod: Request* поля)
  Alerting/Rules/ClusterNotInitializedRule.cs (new)
  Alerting/Rules/MoveStaleRule.cs (mod: скип NotInitialized)
  Alerting/Rules/ShardNoLeaderRule.cs (mod: скип NotInitialized-кластеров)
src/AdminPanel.Etcd/
  Client/IEtcdGateway.cs (mod: TxnAsync/PutAsync/DeleteAsync + records)
  Client/EtcdGateway.cs (mod: реализация)
  Writing/CreateClusterRequest.cs (new: request + limits + validator)
  Writing/ClusterCreatePlan.cs (new: план ключей)
  Parsing/ClustersParser.cs (mod: state/nodes/NOT_INITIALIZED)
  Parsing/ServiceParser.cs (mod: request_*)
src/AdminPanel.Api/
  Operations/OperationsModule.cs (new: POST /api/clusters + исключения + DTO)
  Operations/CreateClusterCommand.cs (new: команда + хендлер)
  Program.cs (mod: MapOperationsApi)
  Inspection/ClustersQuery.cs (mod: NotInitialized, activeMoves-семантика)
  Inspection/ClusterDetailsQuery.cs (mod: State/Nodes/Requests, BucketStates)
  Inspection/HaQuery.cs (mod: Requests)
  Inspection/OverviewQuery.cs (mod: notInitialized, masterless, activeMoves)
frontend/src/
  api/dto.ts (mod), api/queries.ts (mod)
  pages/clusters/ClusterCreateModal.tsx (new)
  pages/ClustersPage.tsx (mod), pages/OverviewPage.tsx (mod)
  pages/cluster-details/ShardsTab.tsx (mod), MovesTab.tsx (mod),
  BucketsTab.tsx (mod)
  pages/HaScopeDetailsPage.tsx (mod), components/BucketStateBadge.tsx (mod)
dev-stand/checks/15-cluster-create.sh (new)
arch/roadmap/sharding.md (mod — только Задача 13)
tests/AdminPanel.UnitTests/ (mod/new: по задачам)
tests/AdminPanel.IntegrationTests/ (mod/new: по задачам)
```

---

## Задача 1: CQRS-команды в Infrastructure

**Файлы:**
- Create: `src/AdminPanel.Infrastructure/CQRS/ICommand.cs`
- Create: `src/AdminPanel.Infrastructure/CQRS/ICommandHandler.cs`
- Modify: `src/AdminPanel.Infrastructure/CQRS/IHandler.cs`
- Test: `src/tests/AdminPanel.UnitTests/CQRSTests.cs`

**Интерфейсы:**
- Consumes: существующий `IHandler` (dispatcher c `Tracing.ActivityT` +
  scope из `spHelper.IsGlobal`), `Result<T>` из `AdminPanel.Infrastructure`.
- Produces: `ICommand<T>` (маркер), `ICommandHandler<in TC, TR>` с
  `ValueTask<Result<TR>> Handle(TC, CancellationToken)`,
  `IHandler.HandleCommand<C, T>(C command, CancellationToken ct)` при
  `where C : ICommand<T>`. Используют Задачи 5, 6.

- [ ] **Шаг 1.1: падающий тест диспётчера команд**

В конец `src/tests/AdminPanel.UnitTests/CQRSTests.cs` (внутрь класса
`CQRSTests` добавить факт, вне — маркер и хендлер):

```csharp
    [Fact]
    public async Task HandleCommand_FromRootProvider_ReturnsHandlerValue()
    {
        // Arrange
        var provider = TestHost.BuildProvider();
        var handler = provider.GetRequiredService<IHandler>();

        // Act
        var result = await handler.HandleCommand<TestCommand, string>(new TestCommand("hi"), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be("hi");
    }
```

```csharp
// Тестовая команда (spec t12 §3.4).
public sealed record TestCommand(string Value) : ICommand<string>;

// Тестовый хендлер команды: scoped — как query, диспётчер резолвит из scope.
[InjectAsScoped]
public class TestCommandHandler : ICommandHandler<TestCommand, string>
{
    public ValueTask<Result<string>> Handle(TestCommand command, CancellationToken ct)
        => new(Result<string>.Success(command.Value));
}
```

- [ ] **Шаг 1.2: проверить, что тест падает**

Run: `dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~CQRSTests"`
Ожидание: ошибка компиляции — `ICommand`/`HandleCommand` не определены.

- [ ] **Шаг 1.3: реализация**

`src/AdminPanel.Infrastructure/CQRS/ICommand.cs`:

```csharp
namespace AdminPanel.Infrastructure.CQRS;

// Маркер команды (мутация); в панели команда одна — создание кластера (arch/01 §2,
// паттерн Puzzle docs/01.03-cqrs.md без DB-слоя — spec t12 §3.4, решение §8.9).
public interface ICommand<T>;
```

`src/AdminPanel.Infrastructure/CQRS/ICommandHandler.cs`:

```csharp
namespace AdminPanel.Infrastructure.CQRS;

// Хендлер команды: без GetContext/IDbContext из референса — у панели нет БД,
// роль транзакции выполняет etcd-txn клэйма (spec t12 §3.4).
public interface ICommandHandler<in TC, TR>
    where TC : ICommand<TR>
{
    ValueTask<Result<TR>> Handle(TC command, CancellationToken ct);
}
```

В `src/AdminPanel.Infrastructure/CQRS/IHandler.cs`: добавить метод в
интерфейс и реализацию в `Handler` (зеркало `HandleQuery`):

```csharp
    ValueTask<Result<T>> HandleCommand<C, T>(C command, CancellationToken ct)
        where C : ICommand<T>;
```

```csharp
    public async ValueTask<Result<T>> HandleCommand<C, T>(C command, CancellationToken ct)
        where C : ICommand<T>
    {
        Result<T> result = null!;
        await Tracing.ActivityT(
            TypeName<C>(),
            ActivityKind.Server,
            () => Run(async isp =>
            {
                var handler = isp.GetRequiredService<ICommandHandler<C, T>>();
                result = await handler.Handle(command, ct);
            }));
        return result;
    }
```

- [ ] **Шаг 1.4: тесты зелёные**

Run: `dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~CQRSTests"`
Ожидание: PASS (2 факта).

- [ ] **Шаг 1.5: коммит**

```bash
git add src/AdminPanel.Infrastructure/CQRS/ src/tests/AdminPanel.UnitTests/CQRSTests.cs
git commit -m "feat(t12): CQRS-команды Infrastructure (ICommand/ICommandHandler/HandleCommand)"
```

---

## Задача 2: Core-модель + парсеры (state/nodes/NOT_INITIALIZED/request_*)

**Файлы:**
- Modify: `src/AdminPanel.Core/ClusterInfo.cs`
- Modify: `src/AdminPanel.Core/HaScope.cs`
- Modify: `src/AdminPanel.Etcd/Parsing/ClustersParser.cs`
- Modify: `src/AdminPanel.Etcd/Parsing/ServiceParser.cs`
- Modify (фикстуры компиляции): `src/tests/AdminPanel.UnitTests/TestSnapshots.cs`,
  `CoreModelTests.cs`, `ScopeMatcherTests.cs`, `ShardingAlertRulesTests.cs`,
  `ClustersMappersTests.cs`, `ProbeEnricherTests.cs`, `ProbeOrchestratorTests.cs`,
  `HaMappersTests.cs`, `SnapshotBuilderTests.cs`, `SnapshotRefresherTests.cs`,
  `AlertEngineTests.cs`, `AlertTestRules.cs`
  (править только те, где компилятор укажет позиционные конструкторы
  `ClusterInfo`/`ShardInfo`/`HaScope`)
- Test: `src/tests/AdminPanel.UnitTests/ClustersParserTests.cs`,
  `ServiceParserTests.cs`

**Интерфейсы:**
- Produces (используют задачи 4, 5, 6, 7, 8):
  - `enum ClusterState { Active, NotInitialized }`;
  - `record NodeInfo(string Name, string? State)`;
  - `ClusterInfo(…, long? CreatedUnix, ClusterState State, IReadOnlyList<ShardInfo> Shards, …)`;
  - `ShardInfo(…, string? MasterAddress, IReadOnlyList<NodeInfo> Nodes, ShardRuntime? Runtime)`;
  - `BucketState.NotInitialized`;
  - `HaScope(…, bool Initialized, string? RequestCpu, string? RequestMem, string? RequestDisk, IReadOnlyList<HaMember> Members, string? RawConfig)`.

- [ ] **Шаг 2.1: падающие тесты ClustersParser**

В `ClustersParserTests.cs` добавить (по образцу существующих фактов класса;
Kv-хелпер там уже есть — `Kv(key, value)`):

```csharp
    [Fact]
    public void Parse_ConfigStateNotInitialized_MapsToClusterState()
    {
        // Arrange
        var kvs = new[]
        {
            Kv("/clusters/fresh/config",
                """{"buckets":4,"dbname":"fresh","created_unix":1755900000,"state":"NOT_INITIALIZED"}"""),
        };

        // Act
        var result = ClustersParser.Parse(kvs);

        // Assert: state из config; отсутствие поля = Active (arch/02 §2.1).
        result.Clusters.Should().ContainSingle().Which.State.Should().Be(ClusterState.NotInitialized);
    }

    [Fact]
    public void Parse_BucketStatusNotInitialized_MapsStateAndOwner()
    {
        // Arrange
        var kvs = new[]
        {
            Kv("/clusters/fresh/config", """{"buckets":1,"dbname":"fresh","state":"NOT_INITIALIZED"}"""),
            Kv("/clusters/fresh/buckets/routing/bucket_0", "shard1"),
            Kv("/clusters/fresh/buckets/status/bucket_0",
                """{"bucket":"bucket_0","state":"NOT_INITIALIZED","owner":"shard1","updated_unix":1755900000}"""),
        };

        // Act
        var bucket = ClustersParser.Parse(kvs).Clusters.Single().Buckets.Single();

        // Assert: NOT_INITIALIZED — не ACTIVE-по-умолчанию и не ошибка; owner сохранён.
        bucket.State.Should().Be(BucketState.NotInitialized);
        bucket.Owner.Should().Be("shard1");
        bucket.Move!.Owner.Should().Be("shard1");
        bucket.Move.Target.Should().BeNull();
        bucket.Move.UpdatedUnix.Should().Be(1755900000);
    }

    [Fact]
    public void Parse_ShardNodesState_MapsToNodeInfo()
    {
        // Arrange
        var kvs = new[]
        {
            Kv("/clusters/fresh/config", """{"buckets":1,"dbname":"fresh"}"""),
            Kv("/clusters/fresh/shards/shard1/replicas", "2"),
            Kv("/clusters/fresh/shards/shard1/nodes/shard1a/state", "NOT_INITIALIZED"),
            Kv("/clusters/fresh/shards/shard1/nodes/shard1b/state", "NOT_INITIALIZED"),
        };

        // Act
        var shard = ClustersParser.Parse(kvs).Clusters.Single().Shards.Single();

        // Assert: плановые ноды отсортированы по имени (arch/02 §9.1).
        shard.Nodes.Select(n => n.Name).Should().Equal("shard1a", "shard1b");
        shard.Nodes.Should().OnlyContain(n => n.State == "NOT_INITIALIZED");
    }
```

- [ ] **Шаг 2.2: падающие тесты ServiceParser**

В `ServiceParserTests.cs`:

```csharp
    [Fact]
    public void Parse_RequestResources_MapsRawStrings()
    {
        // Arrange
        var kvs = new[]
        {
            Kv("/service/fresh-shard1/request_cpu", "0.5"),
            Kv("/service/fresh-shard1/request_mem", "8Gi"),
            Kv("/service/fresh-shard1/request_disk", "100Gi"),
        };

        // Act
        var scope = ServiceParser.Parse(kvs, []).Scopes.Single();

        // Assert: заявки — raw-строки на ноду (arch/02 §2.2); пустое значение = null.
        scope.RequestCpu.Should().Be("0.5");
        scope.RequestMem.Should().Be("8Gi");
        scope.RequestDisk.Should().Be("100Gi");
    }

    [Fact]
    public void Parse_RequestKeysEmptyValues_MapsToNulls()
    {
        // Arrange
        var kvs = new[] { Kv("/service/fresh-shard1/request_cpu", "  ") };

        // Act
        var scope = ServiceParser.Parse(kvs, []).Scopes.Single();

        // Assert
        scope.RequestCpu.Should().BeNull();
    }
```

- [ ] **Шаг 2.3: проверить падение**

Run: `dotnet build src/AdminPanel.slnx`
Ожидание: FAIL — тесты не скомпилируются (нет `ClusterState`/`Nodes`/
`RequestCpu`/`NotInitialized` в модели).

- [ ] **Шаг 2.4: модель Core**

`src/AdminPanel.Core/ClusterInfo.cs` — заменить записи (порядок позиционных
полей важен: State после CreatedUnix; Nodes после MasterAddress):

```csharp
// Состояние кластера: config.state (arch/02 §9); отсутствие = Active (старые init).
public enum ClusterState
{
    Active,
    NotInitialized,
}
```

`ClusterInfo` — вставить `ClusterState State,` между `long? CreatedUnix,` и
`IReadOnlyList<ShardInfo> Shards,`; сразу после record `ShardInfo` добавить:

```csharp
// Плановая нода шарда: /clusters/<C>/shards/<X>/nodes/<n>/state (arch/02 §9.1);
// State — raw-строка (толерантно к будущим состояниям provisioning'а).
public sealed record NodeInfo(string Name, string? State);
```

В `ShardInfo` — вставить `IReadOnlyList<NodeInfo> Nodes,` между
`string? MasterAddress,` и `ShardRuntime? Runtime`. В `enum BucketState`
добавить член `NotInitialized`.

`src/AdminPanel.Core/HaScope.cs` — вставить в `HaScope` после `bool Initialized,`:

```csharp
    string? RequestCpu,                    // /service/<scope>/request_cpu (arch/02 §9.1)
    string? RequestMem,
    string? RequestDisk,
```

(комментарий над record'ом дополнить: «+ заявка ресурсов на ноду»).

- [ ] **Шаг 2.5: парсеры**

`ClustersParser.cs`:
1. `ClusterAcc` — поле `ClusterState State = ClusterState.Active;` не нужно:
   state живёт в config-JSON, читается в `ParseConfig`.
2. `ParseConfig` — вернуть и state: сигнатура
   `(string? DbName, int BucketsCount, long? CreatedUnix, ClusterState State)`;
   `return (JsonValues.ReadString(root, "dbname"), buckets…, created…,
   JsonValues.ReadString(root, "state") == "NOT_INITIALIZED"
       ? ClusterState.NotInitialized : ClusterState.Active);` (отсутствие
   `state` → ReadString null → Active; ветка `raw is null` возвращает
   `(null, 0, null, ClusterState.Active)`).
3. `BuildCluster` — прокинуть state в `new ClusterInfo(acc.Name, dbName,
   bucketsCount, createdUnix, state, shards, buckets, acc.Heals)`.
4. `ShardAcc` — добавить `public readonly List<(string Name, string? State)> Nodes = [];`.
5. В switch новый case (до `default`, рядом с «shards»; путь
   `/clusters/<C>/shards/<X>/nodes/<n>/state` даёт сегменты
   `["", "clusters", C, "shards", X, "nodes", n, "state"]` → Length==8):

```csharp
                case "shards" when segments.Length == 8
                    && segments[4].Length > 0
                    && segments[5] == "nodes"
                    && segments[6].Length > 0
                    && segments[7] == "state":
                {
                    var shard = GetOrAdd(acc.Shards, segments[4], static _ => new ShardAcc());
                    shard.Nodes.Add((segments[6], string.IsNullOrWhiteSpace(kv.Value) ? null : kv.Value.Trim()));
                    break;
                }
```

6. `BuildShard` — после master-обработки:

```csharp
        var nodes = shard.Nodes
            .OrderBy(n => n.Name, StringComparer.Ordinal)
            .Select(n => new NodeInfo(n.Name, n.State))
            .ToList();
```

и передать `nodes` в `new ShardInfo(..., masterAddress, nodes, null)`.

7. `TryParseStatus` — добавить ветку:

```csharp
                "NOT_INITIALIZED" => BucketState.NotInitialized,
```

и для NotInitialized собрать `MoveInfo` без target/phase:
в общем виде после switch — если `state == BucketState.NotInitialized`,
`move = new MoveInfo(JsonValues.ReadString(root, "owner"), null, null,
JsonValues.ReadLong(root, "updated_unix"), null, null); return true;`
(существующая сборка MoveInfo для SYNCING/FROZEN/ABORTING не меняется;
проверка «state отсутствует или неизвестен — битый ключ» сохраняется для
прочих значений).

`ServiceParser.cs`:
1. `ScopeAcc` — поля `public string? RequestCpu; public string? RequestMem;
   public string? RequestDisk;`.
2. В switch (секция `"/service/<scope>/…"`):

```csharp
                case "request_cpu" when segments.Length == 4:
                    acc.RequestCpu = NullIfBlank(kv.Value);
                    break;

                case "request_mem" when segments.Length == 4:
                    acc.RequestMem = NullIfBlank(kv.Value);
                    break;

                case "request_disk" when segments.Length == 4:
                    acc.RequestDisk = NullIfBlank(kv.Value);
                    break;
```

3. `NullIfBlank`-хелпер и три поля — в конструктор `HaScope` после
   `a.InitializeRaw is { Length: > 0 },`:

```csharp
                    NullIfBlank(a.RequestCpu), NullIfBlank(a.RequestMem), NullIfBlank(a.RequestDisk),
```

```csharp
    private static string? NullIfBlank(string? raw)
        => string.IsNullOrWhiteSpace(raw) ? null : raw.Trim();
```

- [ ] **Шаг 2.6: починить компиляцию фикстур**

`dotnet build src/AdminPanel.slnx` — каждый позиционный вызов
`new ClusterInfo(…)`, `new ShardInfo(…)`, `new HaScope(…)` дополнить
каноническими значениями:
- `ClusterInfo`: `ClusterState.Active` после `CreatedUnix`;
- `ShardInfo`: `[]` (пустые Nodes) после `MasterAddress`;
- `HaScope`: `null, null, null` (request'ы) после `Initialized`.
Известные места (по grep `new ClusterInfo\|new ShardInfo\|new HaScope`):
`src/tests/AdminPanel.UnitTests/{TestSnapshots,CoreModelTests,ScopeMatcherTests,
ShardingAlertRulesTests,ClustersMappersTests,ProbeEnricherTests,
ProbeOrchestratorTests,HaMappersTests,SnapshotBuilderTests,SnapshotRefresherTests,
AlertEngineTests,AlertTestRules}.cs`,
`src/tests/AdminPanel.IntegrationTests/{InspectionApiTests,ClustersApiTests}.cs`
(InspectionSnapshots). Значения — канонические выше; логика фикстур не
меняется. В `InspectionSnapshots.Clustered` кластер `demo` получает
`ClusterState.Active`, шарды — `Nodes: []`.

- [ ] **Шаг 2.7: все unit-тесты зелёные**

Run: `dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~AdminPanel.UnitTests"`
Ожидание: PASS (включая новые 5 фактов и все прежние).

- [ ] **Шаг 2.8: коммит**

```bash
git add -A src/AdminPanel.Core src/AdminPanel.Etcd/Parsing src/tests/AdminPanel.UnitTests src/tests/AdminPanel.IntegrationTests
git commit -m "feat(t12): Core-модель ClusterState/NodeInfo/NotInitialized + request_* в парсерах"
```

---

## Задача 3: write-методы IEtcdGateway (txn/put/delete)

**Файлы:**
- Modify: `src/AdminPanel.Etcd/Client/IEtcdGateway.cs`
- Modify: `src/AdminPanel.Etcd/Client/EtcdGateway.cs`
- Test: `src/tests/AdminPanel.UnitTests/EtcdGatewayTests.cs`

**Интерфейсы:**
- Consumes: `PostAsync<T>`, `ToB64`, `PrefixEnd` (уже в `EtcdGateway`);
  паттерн `FakeHandler` из `EtcdGatewayTests`.
- Produces (использует Задача 5):
  - `record TxnCompare(string Key, long Version)`; `record KvPut(string Key,
    string Value)`; `record TxnResult(bool Succeeded)` (в `IEtcdGateway.cs`);
  - `Task<Result<TxnResult>> TxnAsync(string endpoint, IReadOnlyList<TxnCompare> compares, IReadOnlyList<KvPut> puts, CancellationToken ct)`;
  - `Task<Result> PutAsync(string endpoint, string key, string value, CancellationToken ct)`;
  - `Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)`.

- [ ] **Шаг 3.1: падающие тесты**

В `EtcdGatewayTests.cs` (класс уже имеет `FakeHandler`/`Json`/`NewGateway`):

```csharp
    [Fact]
    public async Task Txn_CompareAndPuts_RequestHasBase64Bodies()
    {
        // Arrange
        var handler = new FakeHandler(_ => Json("""{"succeeded":true}"""));
        var gateway = NewGateway(handler);

        // Act
        var result = await gateway.TxnAsync(
            "http://etcd:2379",
            [new TxnCompare("/clusters/shop/config", 0)],
            [new KvPut("/clusters/shop/config", "{}")],
            CancellationToken.None);

        // Assert: compare version=0 + request_put; base64("/clusters/shop/config") = L2NsdXN0ZXJzL3Nob3AvY29uZmln
        result.IsSuccess.Should().BeTrue();
        result.Value.Succeeded.Should().BeTrue();
        var request = handler.Requests.Should().ContainSingle().Subject;
        request.Url.Should().Be("http://etcd:2379/v3/kv/txn");
        var body = JsonDocument.Parse(request.Body).RootElement;
        body.GetProperty("compare")[0].GetProperty("key").GetString().Should().Be("L2NsdXN0ZXJzL3Nob3AvY29uZmln");
        body.GetProperty("compare")[0].GetProperty("version").GetInt32().Should().Be(0);
        body.GetProperty("success")[0].GetProperty("request_put").GetProperty("key").GetString().Should().Be("L2NsdXN0ZXJzL3Nob3AvY29uZmln");
    }

    [Fact]
    public async Task Txn_CompareFailed_MapsSucceededFalse()
    {
        // Arrange
        var handler = new FakeHandler(_ => Json("""{"succeeded":false,"responses":[]}"""));
        var gateway = NewGateway(handler);

        // Act
        var result = await gateway.TxnAsync(
            "http://etcd:2379", [new TxnCompare("/k", 0)], [new KvPut("/k", "v")], CancellationToken.None);

        // Assert: отказ compare — не исключение, а Succeeded=false (клэйм имени занят, arch/02 §9.2).
        result.IsSuccess.Should().BeTrue();
        result.Value.Succeeded.Should().BeFalse();
    }

    [Fact]
    public async Task Put_RequestHasBase64KeyValue()
    {
        // Arrange
        var handler = new FakeHandler(_ => Json("""{"header":{}}"""));
        var gateway = NewGateway(handler);

        // Act
        var result = await gateway.PutAsync("http://etcd:2379", "/a/b", "v", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        var request = handler.Requests.Should().ContainSingle().Subject;
        request.Url.Should().Be("http://etcd:2379/v3/kv/put");
        var body = JsonDocument.Parse(request.Body).RootElement;
        body.GetProperty("key").GetString().Should().Be("L2EvYg==");
        body.GetProperty("value").GetString().Should().Be("dg==");
    }

    [Fact]
    public async Task Delete_Prefix_RequestHasKeyAndRangeEnd()
    {
        // Arrange
        var handler = new FakeHandler(_ => Json("""{"deleted":3}"""));
        var gateway = NewGateway(handler);

        // Act
        await gateway.DeleteAsync("http://etcd:2379", "/clusters/shop/", prefix: true, CancellationToken.None);
        await gateway.DeleteAsync("http://etcd:2379", "/service/shop-shard1/request_cpu", prefix: false, CancellationToken.None);

        // Assert: prefix=true → key+range_end (префиксный deleterange); точечный — только key.
        var bodies = handler.Requests.Select(r => JsonDocument.Parse(r.Body).RootElement).ToList();
        bodies[0].TryGetProperty("range_end", out _).Should().BeTrue();
        bodies[1].TryGetProperty("range_end", out _).Should().BeFalse();
    }
```

- [ ] **Шаг 3.2: проверить падение**

Run: `dotnet build src/AdminPanel.slnx`
Ожидание: FAIL компиляции — методов/рекордов нет.

- [ ] **Шаг 3.3: реализация**

В `IEtcdGateway.cs` (интерфейс + записи; комментарий «Панель не пишет…» в
шапке файла заменить на «Единственная запись — создание кластера (§9)»):

```csharp
    // POST /v3/kv/txn: compare + success-puts. compare не сошёлся → Succeeded=false (arch/02 §9.2).
    Task<Result<TxnResult>> TxnAsync(
        string endpoint, IReadOnlyList<TxnCompare> compares, IReadOnlyList<KvPut> puts, CancellationToken ct);

    // POST /v3/kv/put — одиночная запись (пакет создания кластера, arch/02 §9.2).
    Task<Result> PutAsync(string endpoint, string key, string value, CancellationToken ct);

    // POST /v3/kv/deleterange — точечное (prefix=false) или префиксное удаление (компенсация, arch/02 §9.2).
    Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct);
```

```csharp
// Compare-условие txn: версия ключа (0 = ключа нет).
public sealed record TxnCompare(string Key, long Version);

// Один put внутри txn либо самостоятельный.
public sealed record KvPut(string Key, string Value);

// Итог txn: сошёлся ли compare.
public sealed record TxnResult(bool Succeeded);
```

В `EtcdGateway.cs`:

```csharp
    public async Task<Result<TxnResult>> TxnAsync(
        string endpoint, IReadOnlyList<TxnCompare> compares, IReadOnlyList<KvPut> puts, CancellationToken ct)
    {
        var body = new
        {
            compare = compares.Select(c => new { key = ToB64(c.Key), version = c.Version }),
            success = puts.Select(p => new
            {
                request_put = new { key = ToB64(p.Key), value = ToB64(p.Value) },
            }),
        };
        var result = await Result<TxnResponse>.FromAsync(
            async () => await PostAsync<TxnResponse>(endpoint, "/v3/kv/txn", body, ct));
        return result.Map(r => new TxnResult(r.Succeeded));
    }

    public async Task<Result> PutAsync(string endpoint, string key, string value, CancellationToken ct)
    {
        var body = new { key = ToB64(key), value = ToB64(value) };
        return await Result.FromAsync(
            async () => await PostAsync<StatusResponse>(endpoint, "/v3/kv/put", body, ct));
    }

    public async Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
    {
        object body = prefix
            ? new { key = ToB64(keyOrPrefix), range_end = ToB64(PrefixEnd(keyOrPrefix)) }
            : new { key = ToB64(keyOrPrefix) };
        return await Result.FromAsync(
            async () => await PostAsync<StatusResponse>(endpoint, "/v3/kv/deleterange", body, ct));
    }
```

плюс DTO ответа рядом с остальными приватными классами:

```csharp
    private sealed class TxnResponse
    {
        [JsonPropertyName("succeeded")]
        public bool Succeeded { get; set; }
    }
```

- [ ] **Шаг 3.4: тесты зелёные**

Run: `dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~EtcdGatewayTests"`
Ожидание: PASS.

- [ ] **Шаг 3.5: коммит**

```bash
git add src/AdminPanel.Etcd/Client src/tests/AdminPanel.UnitTests/EtcdGatewayTests.cs
git commit -m "feat(t12): etcd-gateway write-методы txn/put/delete-range"
```

---

## Задача 4: валидатор + план ключей создания (AdminPanel.Etcd.Writing)

**Файлы:**
- Create: `src/AdminPanel.Etcd/Writing/CreateClusterRequest.cs`
- Create: `src/AdminPanel.Etcd/Writing/ClusterCreatePlan.cs`
- Test: Create `src/tests/AdminPanel.UnitTests/CreateClusterPlanTests.cs`

**Интерфейсы:**
- Consumes: `KvPut` (Задача 3).
- Produces (используют задачи 5, 6):
  - `record CreateClusterRequest(string Name, int Buckets, int Shards, int Replicas, decimal RequestCpu, int RequestMem, int RequestDisk)`;
  - `record ValidationError(string Field, string Message)`;
  - `static class CreateClusterLimits` (константы `NamePattern`, `MinBuckets/MaxBuckets`, `MinShards/MaxShards`, `MinReplicas/MaxReplicas`, `MinCpu/MaxCpu`, `MinGiB/MaxGiB`);
  - `static class CreateClusterValidator { static IReadOnlyList<ValidationError> Validate(CreateClusterRequest request); static string CanonicalCpu(decimal cpu); static string CanonicalGiB(int gib); }`;
  - `record ClusterCreatePlan(string ConfigKey, string ConfigValue, IReadOnlyList<KvPut> Puts, IReadOnlyList<string> RequestKeys, string CanonicalCpu, string CanonicalMem, string CanonicalDisk)` и
    `static ClusterCreatePlan Build(CreateClusterRequest request, long nowUnix)`.

- [ ] **Шаг 4.1: падающие тесты валидатора**

`src/tests/AdminPanel.UnitTests/CreateClusterPlanTests.cs` (новый файл):

```csharp
using AdminPanel.Etcd.Writing;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Валидация создания кластера: arch/02 §9.3 — сервер источник истины (spec t12 §3.3).
public class CreateClusterValidatorTests
{
    private static readonly CreateClusterRequest Valid =
        new("shop", 4, 2, 2, 0.5m, 8, 100);

    [Fact]
    public void Validate_ValidRequest_NoErrors()
    {
        // Arrange/Act/Assert
        CreateClusterValidator.Validate(Valid).Should().BeEmpty();
    }

    [Theory]
    [InlineData("Shop")]       // верхний регистр
    [InlineData("1shop")]      // начинается с цифры
    [InlineData("shop-x")]     // дефис: коллизии ScopeMatcher (spec t12 §8.5)
    [InlineData("шоп")]        // не [a-z0-9_]
    [InlineData("")]           // пустое
    public void Validate_BadNames_Rejected(string name)
    {
        // Arrange
        var request = Valid with { Name = name };

        // Act
        var errors = CreateClusterValidator.Validate(request);

        // Assert
        errors.Should().Contain(e => e.Field == "name");
    }

    [Fact]
    public void Validate_NameTooLong_Rejected()
    {
        // Arrange: 64 символа — больше 63 (максимум {1,63} после первого)
        var request = Valid with { Name = new string('a', 64) };

        // Act/Assert
        CreateClusterValidator.Validate(request).Should().Contain(e => e.Field == "name");
    }

    [Fact]
    public void Validate_BucketsOutOfRange_Rejected()
    {
        // Arrange/Act/Assert: 0 и 8193 вне 1..8192
        CreateClusterValidator.Validate(Valid with { Buckets = 0 }).Should().Contain(e => e.Field == "buckets");
        CreateClusterValidator.Validate(Valid with { Buckets = 8193 }).Should().Contain(e => e.Field == "buckets");
    }

    [Fact]
    public void Validate_ShardsWithoutBuckets_Rejected()
    {
        // Arrange: шардов больше бакетов — задание пользователя (spec t12 §1)
        var request = Valid with { Shards = 5 };

        // Act
        var errors = CreateClusterValidator.Validate(request);

        // Assert
        errors.Should().Contain(e => e.Field == "shards" && e.Message.Contains("бакетов"));
    }

    [Fact]
    public void Validate_ReplicasOutOfRange_Rejected()
    {
        // Arrange/Act/Assert: 0 и 27 вне 1..26 (буквы нод a..z)
        CreateClusterValidator.Validate(Valid with { Replicas = 0 }).Should().Contain(e => e.Field == "replicas");
        CreateClusterValidator.Validate(Valid with { Replicas = 27 }).Should().Contain(e => e.Field == "replicas");
    }

    [Fact]
    public void Validate_ResourcesOutOfRange_Rejected()
    {
        // Arrange/Act/Assert
        CreateClusterValidator.Validate(Valid with { RequestCpu = 0.001m }).Should().Contain(e => e.Field == "requestCpu");
        CreateClusterValidator.Validate(Valid with { RequestCpu = 65m }).Should().Contain(e => e.Field == "requestCpu");
        CreateClusterValidator.Validate(Valid with { RequestMem = 0 }).Should().Contain(e => e.Field == "requestMem");
        CreateClusterValidator.Validate(Valid with { RequestDisk = 65537 }).Should().Contain(e => e.Field == "requestDisk");
    }

    [Fact]
    public void Canonical_Strings_AreInvariant()
    {
        // Arrange/Act/Assert: cpu — десятичные ядра без хвостовых нулей; GiB — "<n>Gi"
        CreateClusterValidator.CanonicalCpu(2.0m).Should().Be("2");
        CreateClusterValidator.CanonicalCpu(0.50m).Should().Be("0.5");
        CreateClusterValidator.CanonicalGiB(8).Should().Be("8Gi");
    }
}
```

- [ ] **Шаг 4.2: падающие тесты плана**

Туда же (второй класс):

```csharp
// План ключей одного создания: arch/02 §9.1 — конфиг, шарды, ноды, routing round-robin, request_*.
public class ClusterCreatePlanTests
{
    [Fact]
    public void Build_FullPlan_MatchesContract()
    {
        // Arrange
        var request = new CreateClusterRequest("shop", 4, 2, 2, 0.5m, 8, 100);

        // Act
        var plan = ClusterCreatePlan.Build(request, nowUnix: 1755900000);

        // Assert: клэйм — конфиг со state NOT_INITIALIZED
        plan.ConfigKey.Should().Be("/clusters/shop/config");
        plan.ConfigValue.Should().Be(
            """{"buckets":4,"dbname":"shop","created_unix":1755900000,"state":"NOT_INITIALIZED"}""");

        // Порядок ключей пакета: shards → nodes → routing+status → request_* (детерминированный).
        var keys = plan.Puts.Select(p => p.Key).ToList();
        keys.Should().BeInAscendingOrder(); // отсортирован — стабильные повторы/диагностика
        keys.Should().Contain(
        [
            "/clusters/shop/shards/shard1/replicas",
            "/clusters/shop/shards/shard2/replicas",
            "/clusters/shop/shards/shard1/nodes/shard1a/state",
            "/clusters/shop/shards/shard1/nodes/shard1b/state",
            "/clusters/shop/buckets/routing/bucket_0",
            "/clusters/shop/buckets/status/bucket_0",
            "/service/shop-shard1/request_cpu",
            "/service/shop-shard2/request_disk",
        ]);

        // round-robin: bucket_i → shard_(i % S + 1) — как init-cluster.sh
        plan.Puts.Single(p => p.Key == "/clusters/shop/buckets/routing/bucket_0").Value.Should().Be("shard1");
        plan.Puts.Single(p => p.Key == "/clusters/shop/buckets/routing/bucket_1").Value.Should().Be("shard2");
        plan.Puts.Single(p => p.Key == "/clusters/shop/buckets/routing/bucket_2").Value.Should().Be("shard1");

        // статус бакета: NOT_INITIALIZED + owner + updated_unix, без target/phase
        plan.Puts.Single(p => p.Key == "/clusters/shop/buckets/status/bucket_3").Value.Should().Be(
            """{"bucket":"bucket_3","state":"NOT_INITIALIZED","owner":"shard2","updated_unix":1755900000}""");

        // ноды: state NOT_INITIALIZED; replicas в etcd — строкой
        plan.Puts.Single(p => p.Key == "/clusters/shop/shards/shard1/nodes/shard1a/state").Value.Should().Be("NOT_INITIALIZED");
        plan.Puts.Single(p => p.Key == "/clusters/shop/shards/shard1/replicas").Value.Should().Be("2");

        // request_* — на каждый шард, канонические строки
        plan.Puts.Single(p => p.Key == "/service/shop-shard1/request_cpu").Value.Should().Be("0.5");
        plan.Puts.Single(p => p.Key == "/service/shop-shard1/request_mem").Value.Should().Be("8Gi");
        plan.Puts.Single(p => p.Key == "/service/shop-shard2/request_disk").Value.Should().Be("100Gi");

        // компенсационный список — ровно request-ключи (префикс кластера удаляется целиком)
        plan.RequestKeys.Should().BeEquivalentTo(
        [
            "/service/shop-shard1/request_cpu",
            "/service/shop-shard1/request_mem",
            "/service/shop-shard1/request_disk",
            "/service/shop-shard2/request_cpu",
            "/service/shop-shard2/request_mem",
            "/service/shop-shard2/request_disk",
        ]);

        plan.CanonicalCpu.Should().Be("0.5");
        plan.CanonicalMem.Should().Be("8Gi");
        plan.CanonicalDisk.Should().Be("100Gi");
    }

    [Fact]
    public void Build_NodeNames_AscendingLetters_UpTo26()
    {
        // Arrange
        var request = new CreateClusterRequest("big", 26, 1, 26, 1m, 8, 100);

        // Act
        var plan = ClusterCreatePlan.Build(request, 1);

        // Assert: буквы a..z; мастер — <X>a (spec t12 §8.4)
        plan.Puts.Where(p => p.Key.StartsWith("/clusters/big/shards/shard1/nodes/"))
            .Should().HaveCount(26);
        plan.Puts.Should().Contain(p => p.Key == "/clusters/big/shards/shard1/nodes/shard1z/state");
    }

    [Fact]
    public void Build_RoundRobinUneven_FirstShardsGetExtra()
    {
        // Arrange: 5 бакетов, 2 шарда — как init-cluster.sh (первые rem шардов по +1)
        var request = new CreateClusterRequest("u", 5, 2, 1, 1m, 1, 1);

        // Act
        var plan = ClusterCreatePlan.Build(request, 1);

        // Assert: i % S: 0→shard1,1→shard2,2→shard1,3→shard2,4→shard1
        plan.Puts.Single(p => p.Key == "/clusters/u/buckets/routing/bucket_4").Value.Should().Be("shard1");
    }
}
```

- [ ] **Шаг 4.3: проверить падение**

Run: `dotnet build src/AdminPanel.slnx`
Ожидание: FAIL — namespace `AdminPanel.Etcd.Writing` не существует.

- [ ] **Шаг 4.4: реализация CreateClusterRequest + limits + validator**

`src/AdminPanel.Etcd/Writing/CreateClusterRequest.cs`:

```csharp
using System.Globalization;
using System.Text.RegularExpressions;

namespace AdminPanel.Etcd.Writing;

// Тело POST /api/clusters (arch/03 §1.1): биндится Minimal API как JSON.
public sealed record CreateClusterRequest(
    string Name,
    int Buckets,
    int Shards,
    int Replicas,
    decimal RequestCpu,
    int RequestMem,
    int RequestDisk);

// Ошибка валидации одного поля (ProblemDetails errors, arch/03 §1.1).
public sealed record ValidationError(string Field, string Message);

// Границы создания кластера — arch/02 §9.3; константы кода, не конфиг (spec t12 §8.15).
public static partial class CreateClusterLimits
{
    // Без дефиса: scope <C>-<X> и ScopeMatcher однозначны; dbname = <C> (spec t12 §8.5).
    [GeneratedRegex("^[a-z][a-z0-9_]{0,62}$")]
    public static partial Regex NamePattern();

    public const int MinBuckets = 1;
    public const int MaxBuckets = 8192;
    public const int MinShards = 1;
    public const int MaxShards = 128;
    public const int MinReplicas = 1;   // 1 = только мастер <X>a (spec t12 §8.4)
    public const int MaxReplicas = 26;  // буквы нод a..z
    public const decimal MinCpu = 0.01m;
    public const decimal MaxCpu = 64m;
    public const int MinGiB = 1;
    public const int MaxGiB = 65536;
}

// Чистая функция валидации: сервер — источник истины (spec t12 §2).
public static class CreateClusterValidator
{
    public static IReadOnlyList<ValidationError> Validate(CreateClusterRequest request)
    {
        var errors = new List<ValidationError>();
        if (!CreateClusterLimits.NamePattern().IsMatch(request.Name ?? ""))
            errors.Add(new("name", "имя: ^[a-z][a-z0-9_]{0,62}$ (без дефиса)"));
        if (request.Buckets is < CreateClusterLimits.MinBuckets or > CreateClusterLimits.MaxBuckets)
            errors.Add(new("buckets", $"бакеты: целое {CreateClusterLimits.MinBuckets}..{CreateClusterLimits.MaxBuckets}"));
        if (request.Shards is < CreateClusterLimits.MinShards or > CreateClusterLimits.MaxShards
            || request.Shards > request.Buckets)
            errors.Add(new("shards", $"шарды: целое {CreateClusterLimits.MinShards}..{CreateClusterLimits.MaxShards} и не больше бакетов"));
        if (request.Replicas is < CreateClusterLimits.MinReplicas or > CreateClusterLimits.MaxReplicas)
            errors.Add(new("replicas", $"реплики: целое {CreateClusterLimits.MinReplicas}..{CreateClusterLimits.MaxReplicas}"));
        if (request.RequestCpu < CreateClusterLimits.MinCpu || request.RequestCpu > CreateClusterLimits.MaxCpu)
            errors.Add(new("requestCpu", $"CPU (ядра): {CreateClusterLimits.MinCpu}..{CreateClusterLimits.MaxCpu}"));
        if (request.RequestMem is < CreateClusterLimits.MinGiB or > CreateClusterLimits.MaxGiB)
            errors.Add(new("requestMem", $"память (GiB): {CreateClusterLimits.MinGiB}..{CreateClusterLimits.MaxGiB}"));
        if (request.RequestDisk is < CreateClusterLimits.MinGiB or > CreateClusterLimits.MaxGiB)
            errors.Add(new("requestDisk", $"диск (GiB): {CreateClusterLimits.MinGiB}..{CreateClusterLimits.MaxGiB}"));
        return errors;
    }

    // Канонические строки etcd (arch/02 §9.1): cpu invariant-десятичное без хвостовых нулей.
    public static string CanonicalCpu(decimal cpu)
        => cpu.ToString("0.########", CultureInfo.InvariantCulture);

    public static string CanonicalGiB(int gib)
        => $"{gib}Gi";
}
```

- [ ] **Шаг 4.5: реализация ClusterCreatePlan**

`src/AdminPanel.Etcd/Writing/ClusterCreatePlan.cs`:

```csharp
using System.Text.Json;
using System.Text.Json.Serialization;
using AdminPanel.Etcd.Client;

namespace AdminPanel.Etcd.Writing;

// План ключей одного создания (arch/02 §9.1): чистая функция запрос+время → ключи.
// Вызывается ТОЛЬКО после CreateClusterValidator (невалидный запрос здесь не проверяется).
public sealed record ClusterCreatePlan(
    string ConfigKey,
    string ConfigValue,
    IReadOnlyList<KvPut> Puts,           // всё кроме config (пакет PUT после клэйма)
    IReadOnlyList<string> RequestKeys,   // компенсация: точечные del (пространство Patroni не трогаем)
    string CanonicalCpu,
    string CanonicalMem,
    string CanonicalDisk)
{
    public const string NotInitialized = "NOT_INITIALIZED";

    public static ClusterCreatePlan Build(CreateClusterRequest request, long nowUnix)
    {
        var cpu = CreateClusterValidator.CanonicalCpu(request.RequestCpu);
        var mem = CreateClusterValidator.CanonicalGiB(request.RequestMem);
        var disk = CreateClusterValidator.CanonicalGiB(request.RequestDisk);

        var config = new ConfigJson(request.Buckets, request.Name, nowUnix, NotInitialized);
        var puts = new List<KvPut>();
        var requestKeys = new List<string>();

        for (var s = 0; s < request.Shards; s++)
        {
            var shard = $"shard{s + 1}";
            puts.Add(new($"/clusters/{request.Name}/shards/{shard}/replicas",
                request.Replicas.ToString()));
            for (var r = 0; r < request.Replicas; r++)
                puts.Add(new(
                    $"/clusters/{request.Name}/shards/{shard}/nodes/{shard}{(char)('a' + r)}/state",
                    NotInitialized));

            // Заявка ресурсов на КАЖДУЮ ноду scope (arch/02 §9.1)
            foreach (var (leaf, value) in (
                     ("request_cpu", cpu), ("request_mem", mem), ("request_disk", disk)))
            {
                var key = $"/service/{request.Name}-{shard}/{leaf}";
                puts.Add(new(key, value));
                requestKeys.Add(key);
            }
        }

        for (var i = 0; i < request.Buckets; i++)
        {
            // round-robin по шардам — как init-cluster.sh bucket_shard(): i % S
            var owner = $"shard{i % request.Shards + 1}";
            puts.Add(new($"/clusters/{request.Name}/buckets/routing/bucket_{i}", owner));
            puts.Add(new(
                $"/clusters/{request.Name}/buckets/status/bucket_{i}",
                JsonSerializer.Serialize(new StatusJson($"bucket_{i}", NotInitialized, owner, nowUnix))));
        }

        puts.Sort((a, b) => string.CompareOrdinal(a.Key, b.Key)); // детерминированный порядок
        return new ClusterCreatePlan(
            $"/clusters/{request.Name}/config",
            JsonSerializer.Serialize(config),
            puts,
            requestKeys,
            cpu,
            mem,
            disk);
    }

    // config-JSON: имена полей — канон init-cluster.sh (snake_case).
    private sealed record ConfigJson(
        [property: JsonPropertyName("buckets")] int Buckets,
        [property: JsonPropertyName("dbname")] string DbName,
        [property: JsonPropertyName("created_unix")] long CreatedUnix,
        [property: JsonPropertyName("state")] string State);

    // Статус-ключ бакета: без target/started_unix/phase — это не переезд (arch/02 §2.1).
    private sealed record StatusJson(
        [property: JsonPropertyName("bucket")] string Bucket,
        [property: JsonPropertyName("state")] string State,
        [property: JsonPropertyName("owner")] string Owner,
        [property: JsonPropertyName("updated_unix")] long UpdatedUnix);
}
```

Проверка формата JSON: `JsonSerializer.Serialize` по умолчанию не
экранирует кириллицу (не нужен UnsafeRelaxedJsonEscaping — все значения
ASCII); числа сериализуются без суффиксов — ожидаемые строки тестов
(Шаг 4.2) совпадут.

- [ ] **Шаг 4.6: тесты зелёные**

Run: `dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~CreateClusterPlanTests|FullyQualifiedName~CreateClusterValidatorTests"`
Ожидание: PASS.

- [ ] **Шаг 4.7: коммит**

```bash
git add src/AdminPanel.Etcd/Writing src/tests/AdminPanel.UnitTests/CreateClusterPlanTests.cs
git commit -m "feat(t12): валидатор и план etcd-ключей создания кластера"
```

---

## Задача 5: команда CreateCluster + хендлер

**Файлы:**
- Create: `src/AdminPanel.Api/Operations/CreateClusterCommand.cs`
- Test: Create `src/tests/AdminPanel.UnitTests/CreateClusterCommandHandlerTests.cs`

**Интерфейсы:**
- Consumes: `ICommand<T>`/`ICommandHandler<…>` (Задача 1), `IEtcdGateway`
  write-методы + `KvPut`/`TxnCompare` (Задача 3), `CreateClusterRequest`/
  `CreateClusterValidator`/`ClusterCreatePlan` (Задача 4), `ISnapshotStore`
  (текущий `EtcdStatus.ActiveEndpoint`), `TimeProvider`.
- Produces (использует Задача 6):
  - `record CreateClusterCommand(CreateClusterRequest Request) : ICommand<ClusterCreatedDto>`;
  - `record ClusterCreatedDto(string Name, string DbName, int BucketsCount, int ShardsTotal, int Replicas, string RequestCpu, string RequestMem, string RequestDisk, string State)`;
  - `class CreateClusterValidationException(IReadOnlyList<ValidationError> Errors) : Exception`;
  - `class ClusterAlreadyExistsException(string name) : Exception`;
  - `class EtcdWriteUnavailableException() : Exception("нет активного etcd-endpoint'а (снапшот пуст или etcd недоступен)")`;
  - `[InjectAsScoped] CreateClusterCommandHandler`.

- [ ] **Шаг 5.1: падающие тесты хендлера (фейковый gateway)**

`src/tests/AdminPanel.UnitTests/CreateClusterCommandHandlerTests.cs`:

```csharp
using AdminPanel.Core;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Client;
using AdminPanel.Etcd.Writing;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Хендлер создания: клэйм-txn → пакет PUT → компенсация при сбое (arch/02 §9.2).
public class CreateClusterCommandHandlerTests
{
    private const string Endpoint = "http://etcd:2379";

    private sealed class FakeGateway : IEtcdGateway
    {
        public bool TxnSucceeded = true;
        public int FailPutAtIndex = -1;             // -1 = пакет проходит целиком
        public readonly List<string> Puts = [];
        public readonly List<string> DeletedPrefixes = [];
        public readonly List<string> DeletedKeys = [];

        public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<Kv>>.Success([]));

        public Task<Result<EtcdStatusPayload>> StatusAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<EtcdStatusPayload>.Success(new(null, null, null, null, null)));

        public Task<Result<IReadOnlyList<EtcdMember>>> MemberListAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<EtcdMember>>.Success([]));

        public Task<Result<IReadOnlyList<EtcdAlarm>>> AlarmAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<EtcdAlarm>>.Success([]));

        public Task<Result<TxnResult>> TxnAsync(
            string endpoint, IReadOnlyList<TxnCompare> compares, IReadOnlyList<KvPut> puts, CancellationToken ct)
            => Task.FromResult(TxnSucceeded
                ? Result<TxnResult>.Success(new(true))
                : Result<TxnResult>.Success(new(false)));

        public Task<Result> PutAsync(string endpoint, string key, string value, CancellationToken ct)
        {
            Puts.Add(key);
            return Task.FromResult(Puts.Count - 1 == FailPutAtIndex
                ? Result.Failed(new InvalidOperationException("put failed"))
                : Result.Success());
        }

        public Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
        {
            if (prefix)
                DeletedPrefixes.Add(keyOrPrefix);
            else
                DeletedKeys.Add(keyOrPrefix);
            return Task.FromResult(Result.Success());
        }
    }

    private static (CreateClusterCommandHandler Handler, FakeGateway Gateway, SnapshotStore Store) NewHandler()
    {
        var gateway = new FakeGateway();
        var store = new SnapshotStore();
        // Healthy-базис + один живой endpoint: ActiveEndpoint решает, куда пишет хендлер.
        store.Replace(TestSnapshots.Healthy(DateTimeOffset.UnixEpoch) with
        {
            Etcd = new EtcdStatus(
                true, [new EtcdEndpoint(Endpoint, true, 1, "3.5.21", null, null, null, null, [])],
                [], [], Endpoint, false, DateTimeOffset.UnixEpoch, 0),
        });
        return (new CreateClusterCommandHandler(store, gateway, TimeProvider.System), gateway, store);
    }

    private static CreateClusterCommand Command() => new(new("shop", 4, 2, 2, 0.5m, 8, 100));

    [Fact]
    public async Task Handle_ValidRequest_ClaimsThenPutsAndReturnsDto()
    {
        // Arrange
        var (handler, gateway, _) = NewHandler();

        // Act
        var result = await handler.Handle(Command(), CancellationToken.None);

        // Assert: DTO с каноническими строками и state NOT_INITIALIZED; пакет = план минус config
        result.IsSuccess.Should().BeTrue();
        result.Value.Name.Should().Be("shop");
        result.Value.State.Should().Be("NOT_INITIALIZED");
        result.Value.RequestCpu.Should().Be("0.5");
        result.Value.RequestMem.Should().Be("8Gi");
        result.Value.RequestDisk.Should().Be("100Gi");
        gateway.Puts.Should().HaveCountGreaterThan(0);
        gateway.Puts.Should().NotContain("/clusters/shop/config"); // конфиг — в txn-клэйме
        gateway.DeletedPrefixes.Should().BeEmpty();
    }

    [Fact]
    public async Task Handle_ClaimFailed_ReturnsAlreadyExists()
    {
        // Arrange
        var (handler, gateway, _) = NewHandler();
        gateway.TxnSucceeded = false;

        // Act
        var result = await handler.Handle(Command(), CancellationToken.None);

        // Assert: ничего не писано после несошедшегося compare
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<ClusterAlreadyExistsException>();
        gateway.Puts.Should().BeEmpty();
    }

    [Fact]
    public async Task Handle_InvalidRequest_ReturnsValidationErrors()
    {
        // Arrange
        var (handler, _, _) = NewHandler();

        // Act: шардов больше бакетов
        var result = await handler.Handle(new CreateClusterCommand(new("shop", 1, 2, 2, 1m, 1, 1)), CancellationToken.None);

        // Assert: до etcd дело не дошло
        result.Error.Should().BeOfType<CreateClusterValidationException>()
            .Which.Errors.Should().NotBeEmpty();
    }

    [Fact]
    public async Task Handle_NoSnapshot_ReturnsWriteUnavailable()
    {
        // Arrange: снапшота нет — активный endpoint неизвестен (spec t12 §8.12)
        var gateway = new FakeGateway();
        var handler = new CreateClusterCommandHandler(new SnapshotStore(), gateway, TimeProvider.System);

        // Act
        var result = await handler.Handle(Command(), CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<EtcdWriteUnavailableException>();
        gateway.Puts.Should().BeEmpty();
    }

    [Fact]
    public async Task Handle_PutFailsMidway_CompensatesClusterPrefixAndRequestKeys()
    {
        // Arrange
        var (handler, gateway, _) = NewHandler();
        gateway.FailPutAtIndex = 2;

        // Act
        var result = await handler.Handle(Command(), CancellationToken.None);

        // Assert: отказ исходной ошибки + компенсация — префикс кластера и ТОЧЕЧНЫЕ request_*
        result.IsSuccess.Should().BeFalse();
        gateway.DeletedPrefixes.Should().ContainSingle().Which.Should().Be("/clusters/shop/");
        gateway.DeletedKeys.Should().BeEquivalentTo(
        [
            "/service/shop-shard1/request_cpu", "/service/shop-shard1/request_mem", "/service/shop-shard1/request_disk",
            "/service/shop-shard2/request_cpu", "/service/shop-shard2/request_mem", "/service/shop-shard2/request_disk",
        ]);
    }
}
```

Примечание: `TestSnapshots.Healthy(builtAt)` и `SnapshotStore.Replace` —
существующие хелперы (см. `TestSnapshots.cs`, `SnapshotStore.cs`).

- [ ] **Шаг 5.2: проверить падение**

Run: `dotnet build src/AdminPanel.slnx`
Ожидание: FAIL — `CreateClusterCommandHandler` не существует.

- [ ] **Шаг 5.3: реализация**

`src/AdminPanel.Api/Operations/CreateClusterCommand.cs`:

```csharp
using AdminPanel.Etcd;
using AdminPanel.Etcd.Client;
using AdminPanel.Etcd.Writing;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Operations;

// Команда создания кластера — единственная мутация панели (arch/01 §1; spec t12 §3.5).
public sealed record CreateClusterCommand(CreateClusterRequest Request) : ICommand<ClusterCreatedDto>;

// Ответ 201 POST /api/clusters (arch/03 §1.1).
public sealed record ClusterCreatedDto(
    string Name,
    string DbName,
    int BucketsCount,
    int ShardsTotal,
    int Replicas,
    string RequestCpu,
    string RequestMem,
    string RequestDisk,
    string State);

// Валидация не прошла: 400 с errors по полям (arch/03 §1.1).
public sealed class CreateClusterValidationException(IReadOnlyList<ValidationError> errors)
    : Exception("параметры создания кластера некорректны")
{
    public IReadOnlyList<ValidationError> Errors { get; } = errors;
}

// Клэйм-txn не сошёлся: имя занято (arch/02 §9.2) — 409.
public sealed class ClusterAlreadyExistsException(string name)
    : Exception($"кластер {name} уже существует (config-ключ присутствует)");

// Нет снапшота/активного endpoint'а — писать некуда (spec t12 §8.12) — 503.
public sealed class EtcdWriteUnavailableException()
    : Exception("нет активного etcd-endpoint'а (снапшот пуст или etcd недоступен)");

// Клэйм имени → пакет PUT → компенсация при сбое (arch/02 §9.2). Без ретраев:
// повтор = новый POST от пользователя (spec t12 §8.13).
[InjectAsScoped]
public sealed class CreateClusterCommandHandler(
    ISnapshotStore store,
    IEtcdGateway gateway,
    TimeProvider time) : ICommandHandler<CreateClusterCommand, ClusterCreatedDto>
{
    public async ValueTask<Result<ClusterCreatedDto>> Handle(CreateClusterCommand command, CancellationToken ct)
    {
        var request = command.Request;

        // 1) Валидация (сервер — источник истины, spec t12 §2)
        var errors = CreateClusterValidator.Validate(request);
        if (errors.Count > 0)
            return Result<ClusterCreatedDto>.Failed(new CreateClusterValidationException(errors));

        // 2) Активный endpoint из снапшота — его выбирает/ротирует refresher (spec t12 §8.12)
        var snapshot = store.Current;
        if (snapshot?.Etcd.ActiveEndpoint is not { } endpoint)
            return Result<ClusterCreatedDto>.Failed(new EtcdWriteUnavailableException());

        // 3) Клэйм имени: compare version==0 + put config (атомарная уникальность, arch/02 §9.2)
        var plan = ClusterCreatePlan.Build(request, time.GetUtcNow().ToUnixTimeSeconds());
        var claim = await gateway.TxnAsync(
            endpoint, [new TxnCompare(plan.ConfigKey, 0)], [new KvPut(plan.ConfigKey, plan.ConfigValue)], ct);
        if (!claim.IsSuccess)
            return Result<ClusterCreatedDto>.Failed(claim.Error);
        if (!claim.Value.Succeeded)
            return Result<ClusterCreatedDto>.Failed(new ClusterAlreadyExistsException(request.Name));

        // 4) Пакет PUT (без txn: max-txn-ops=128 не вмещает 2N+ ключей — arch/02 §9.2)
        foreach (var put in plan.Puts)
        {
            var putResult = await gateway.PutAsync(endpoint, put.Key, put.Value, ct);
            if (putResult.IsSuccess)
                continue;

            await CompensateAsync(endpoint, plan, ct);
            return Result<ClusterCreatedDto>.Failed(putResult.Error);
        }

        return Result<ClusterCreatedDto>.Success(new ClusterCreatedDto(
            request.Name, request.Name, request.Buckets, request.Shards, request.Replicas,
            plan.CanonicalCpu, plan.CanonicalMem, plan.CanonicalDisk, ClusterCreatePlan.NotInitialized));
    }

    // Компенсация best-effort: префикс кластера целиком + точечные request_*
    // (пространство Patroni не трогаем — arch/02 §9.2). Ошибка компенсации не
    // маскирует исходную: частичный кластер безопасен (повтор создания → 409).
    private async Task CompensateAsync(string endpoint, ClusterCreatePlan plan, CancellationToken ct)
    {
        await gateway.DeleteAsync(endpoint, $"/clusters/{plan.ConfigKey.Split('/')[2]}/", prefix: true, ct);
        foreach (var key in plan.RequestKeys)
            await gateway.DeleteAsync(endpoint, key, prefix: false, ct);
    }
}
```

- [ ] **Шаг 5.4: тесты зелёные**

Run: `dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~CreateClusterCommandHandlerTests"`
Ожидание: PASS (5 фактов).

- [ ] **Шаг 5.5: коммит**

```bash
git add src/AdminPanel.Api/Operations/CreateClusterCommand.cs src/tests/AdminPanel.UnitTests/CreateClusterCommandHandlerTests.cs
git commit -m "feat(t12): команда CreateCluster — клэйм, пакет PUT, компенсация"
```

---

## Задача 6: OperationsModule — POST /api/clusters + интеграция

**Файлы:**
- Create: `src/AdminPanel.Api/Operations/OperationsModule.cs`
- Modify: `src/AdminPanel.Api/Program.cs` (одна строка)
- Test: Create `src/tests/AdminPanel.IntegrationTests/CreateClusterApiTests.cs`
- Modify: `src/tests/AdminPanel.IntegrationTests/EtcdTestHarness.cs`-хостинга
  не требуется (gateway реальный уже в хосте).

**Интерфейсы:**
- Consumes: `CreateClusterCommand`/`ClusterCreatedDto`/исключения (Задача 5),
  `IHandler.HandleCommand` (Задача 1), `EtcdTestHarness.NewGateway`/
  `EtcdContainerFixture`/`ApiTestLogin`/`AuthWebFactory` (инфраструктура
  тестов), `IEtcdGateway.RangeAsync` для сверки ключей.
- Produces: `POST /api/clusters` (201/400/409/503/401) — использует фронт
  (Задача 9); `MapOperationsApi()`.

- [ ] **Шаг 6.1: падающий интеграционный тест**

`src/tests/AdminPanel.IntegrationTests/CreateClusterApiTests.cs`:

```csharp
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using AdminPanel.Core;
using FluentAssertions;
using Xunit;

namespace AdminPanel.IntegrationTests;

// POST /api/clusters против реального etcd: свой контейнер (мутация сида —
// прецедент InspectionSeededAnomaliesApiTests), снапшот хоста указывает на него.
[Collection("api")]
public class CreateClusterApiTests(AuthWebFactory factory, EtcdContainerFixture fixture)
    : IClassFixture<EtcdContainerFixture>
{
    private readonly AuthWebFactory _factory = factory;

    // Снапшот «живого etcd»: единственный endpoint = контейнер, ActiveEndpoint на него.
    private void SetLiveSnapshot()
    {
        var etcd = new EtcdStatus(
            true,
            [new EtcdEndpoint(fixture.Endpoint, true, 1, "3.5.21", null, null, null, null, [])],
            [], [], fixture.Endpoint, false, _factory.Time.GetUtcNow(), 0);
        _factory.Snapshot = InspectionSnapshots.Fixture(_factory.Time.GetUtcNow()) with { Etcd = etcd };
    }

    [Fact]
    public async Task Create_WithoutCookie_Returns401()
    {
        // Arrange
        using var client = _factory.CreateClient();

        // Act
        using var response = await client.PostAsJsonAsync(
            "/api/clusters", new { name = "x", buckets = 1, shards = 1, replicas = 1, requestCpu = 1, requestMem = 1, requestDisk = 1 },
            TestContext.Current.CancellationToken);

        // Assert: default-deny guard закрывает мутацию как все /api/*
        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task Create_Valid_WritesContractKeysToEtcd()
    {
        // Arrange
        SetLiveSnapshot();
        using var client = await ApiTestLogin.LoginAsync(_factory);

        // Act
        using var response = await client.PostAsJsonAsync(
            "/api/clusters",
            new { name = "shop", buckets = 4, shards = 2, replicas = 2, requestCpu = 0.5m, requestMem = 8, requestDisk = 100 },
            TestContext.Current.CancellationToken);

        // Assert: 201 + Location + DTO канона (arch/03 §1.1)
        response.StatusCode.Should().Be(HttpStatusCode.Created);
        response.Headers.Location!.ToString().Should().Be("/api/clusters/shop");
        var dto = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        dto.GetProperty("state").GetString().Should().Be("NOT_INITIALIZED");
        dto.GetProperty("requestCpu").GetString().Should().Be("0.5");
        dto.GetProperty("requestMem").GetString().Should().Be("8Gi");

        // Ключи в etcd — ровно контракт arch/02 §9.1 (через реальный gateway)
        var gateway = EtcdTestHarness.NewGateway();
        var range = await gateway.RangeAsync(fixture.Endpoint, "/clusters/shop/", TestContext.Current.CancellationToken);
        range.Value.Select(kv => kv.Key).Should().BeEquivalentTo(
        [
            "/clusters/shop/config",
            "/clusters/shop/shards/shard1/replicas",
            "/clusters/shop/shards/shard1/nodes/shard1a/state",
            "/clusters/shop/shards/shard1/nodes/shard1b/state",
            "/clusters/shop/shards/shard2/replicas",
            "/clusters/shop/shards/shard2/nodes/shard2a/state",
            "/clusters/shop/shards/shard2/nodes/shard2b/state",
            "/clusters/shop/buckets/routing/bucket_0", "/clusters/shop/buckets/routing/bucket_1",
            "/clusters/shop/buckets/routing/bucket_2", "/clusters/shop/buckets/routing/bucket_3",
            "/clusters/shop/buckets/status/bucket_0", "/clusters/shop/buckets/status/bucket_1",
            "/clusters/shop/buckets/status/bucket_2", "/clusters/shop/buckets/status/bucket_3",
        ]);
        range.Value.Single(kv => kv.Key == "/clusters/shop/config").Value.Should().Contain("\"state\":\"NOT_INITIALIZED\"");
        var requests = await gateway.RangeAsync(fixture.Endpoint, "/service/shop-", TestContext.Current.CancellationToken);
        requests.Value.Select(kv => kv.Key).Should().BeEquivalentTo(
        [
            "/service/shop-shard1/request_cpu", "/service/shop-shard1/request_mem", "/service/shop-shard1/request_disk",
            "/service/shop-shard2/request_cpu", "/service/shop-shard2/request_mem", "/service/shop-shard2/request_disk",
        ]);
    }

    [Fact]
    public async Task Create_Duplicate_Returns409()
    {
        // Arrange
        SetLiveSnapshot();
        using var client = await ApiTestLogin.LoginAsync(_factory);
        var body = new { name = "dup", buckets = 1, shards = 1, replicas = 1, requestCpu = 1m, requestMem = 1, requestDisk = 1 };

        // Act
        using var first = await client.PostAsJsonAsync("/api/clusters", body, TestContext.Current.CancellationToken);
        using var second = await client.PostAsJsonAsync("/api/clusters", body, TestContext.Current.CancellationToken);

        // Assert: клэйм атомарен — второй POST не прошёл
        first.StatusCode.Should().Be(HttpStatusCode.Created);
        second.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var problem = await second.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        problem.GetProperty("title").GetString().Should().Be("Cluster already exists");
    }

    [Theory]
    [InlineData("Bad-Name", 4, 2, 2, 0.5, 8, 100, "name")]
    [InlineData("ok", 0, 1, 2, 0.5, 8, 100, "buckets")]
    [InlineData("ok", 4, 8, 2, 0.5, 8, 100, "shards")]      // шардов больше бакетов
    [InlineData("ok", 4, 2, 0, 0.5, 8, 100, "replicas")]
    [InlineData("ok", 4, 2, 2, 0.001, 8, 100, "requestCpu")]
    [InlineData("ok", 4, 2, 2, 0.5, 0, 100, "requestMem")]
    [InlineData("ok", 4, 2, 2, 0.5, 8, 0, "requestDisk")]
    public async Task Create_Invalid_Returns400WithFieldErrors(
        string name, int buckets, int shards, int replicas, decimal cpu, int mem, int disk, string field)
    {
        // Arrange
        SetLiveSnapshot();
        using var client = await ApiTestLogin.LoginAsync(_factory);

        // Act
        using var response = await client.PostAsJsonAsync(
            "/api/clusters",
            new { name, buckets, shards, replicas, requestCpu = cpu, requestMem = mem, requestDisk = disk },
            TestContext.Current.CancellationToken);

        // Assert: ProblemDetails 400, errors содержит провалившееся поле (arch/03 §1.1)
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        var problem = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        problem.GetProperty("errors").GetProperty(field).GetArrayLength().Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task Create_NoSnapshot_Returns503()
    {
        // Arrange
        _factory.Snapshot = null;
        using var client = await ApiTestLogin.LoginAsync(_factory);

        // Act
        using var response = await client.PostAsJsonAsync(
            "/api/clusters",
            new { name = "x", buckets = 1, shards = 1, replicas = 1, requestCpu = 1m, requestMem = 1, requestDisk = 1 },
            TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
    }

    [Fact]
    public async Task Create_RefresherNextTick_PicksUpNewCluster()
    {
        // Arrange
        SetLiveSnapshot();
        using var client = await ApiTestLogin.LoginAsync(_factory);
        using var created = await client.PostAsJsonAsync(
            "/api/clusters",
            new { name = "fresh", buckets = 2, shards = 1, replicas = 2, requestCpu = 2m, requestMem = 8, requestDisk = 100 },
            TestContext.Current.CancellationToken);
        created.StatusCode.Should().Be(HttpStatusCode.Created);

        // Act: «следующий тик» — RefreshOnce реального refresher'а (spec t12 §3.10)
        var store = new SnapshotStore();
        var refresher = EtcdTestHarness.NewRefresher(store, fixture.Endpoint);
        (await refresher.RefreshOnceAsync(CancellationToken.None)).IsSuccess.Should().BeTrue();

        // Assert: кластер распознан (NOT_INITIALIZED), заявки видны в scope
        var cluster = store.Current!.Clusters.Single(c => c.Name == "fresh");
        cluster.State.Should().Be(ClusterState.NotInitialized);
        cluster.Shards.Single().Nodes.Should().HaveCount(2);
        cluster.Buckets.Should().OnlyContain(b => b.State == BucketState.NotInitialized);
        var scope = store.Current.HaScopes.Single(s => s.Scope == "fresh-shard1");
        scope.RequestCpu.Should().Be("2");
        scope.RequestMem.Should().Be("8Gi");
        scope.RequestDisk.Should().Be("100Gi");
    }
}
```

Примечание: имена кластеров в фактах (`shop`, `dup`, `fresh`) уникальны —
внутри одного контейнера-на-класс тесты не конфликтуют по клэйму. xunit
запускает факты одного класса последовательно.

- [ ] **Шаг 6.2: проверить падение**

Run: `dotnet build src/AdminPanel.slnx`
Ожидание: FAIL — эндпоинт не существует (тесты упадут 404 при запуске;
сборка упадёт на отсутствии namespace, если файл не создан).

- [ ] **Шаг 6.3: реализация OperationsModule + Program**

`src/AdminPanel.Api/Operations/OperationsModule.cs`:

```csharp
using AdminPanel.Etcd.Writing;
using AdminPanel.Infrastructure.CQRS;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace AdminPanel.Api.Operations;

// Модуль операций (мутирующие эндпоинты): единственный — POST /api/clusters
// (arch/03 §1.1). InspectionModule остаётся read-only (spec t12 §8.16).
public static class OperationsModule
{
    public static IEndpointRouteBuilder MapOperationsApi(this IEndpointRouteBuilder endpoints)
    {
        // POST /api/clusters — создание кластера (auth-guard /api/* уже закрыл, arch/03 §1).
        endpoints.MapPost("/api/clusters", async (
            CreateClusterRequest request, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<CreateClusterCommand, ClusterCreatedDto>(
                new CreateClusterCommand(request), ct);
            if (result.IsSuccess)
                return Results.Created($"/api/clusters/{result.Value.Name}", result.Value);

            return result.Error switch
            {
                CreateClusterValidationException validation => Results.Problem(
                    statusCode: StatusCodes.Status400BadRequest,
                    title: "Validation failed",
                    detail: result.Error.Message,
                    // Канон ProblemDetails (RFC 9457): errors.<field> — МАССИВ сообщений
                    // (как Mvc ValidationProblemDetails); тест 6.1 читает GetArrayLength().
                    extensions: new Dictionary<string, object?>
                    {
                        ["errors"] = validation.Errors.ToDictionary(e => e.Field, e => new[] { e.Message }),
                    }),
                ClusterAlreadyExistsException => Results.Problem(
                    statusCode: StatusCodes.Status409Conflict,
                    title: "Cluster already exists",
                    detail: result.Error.Message),
                _ => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable,
                    title: "Etcd write failed",
                    detail: result.Error.Message),
            };
        });

        return endpoints;
    }
}
```

`src/AdminPanel.Api/Program.cs` — после `app.MapInspectionApi();`:

```csharp
app.MapOperationsApi(); // [t12] единственная мутация: POST /api/clusters (arch/02 §9)
```

(+ `using AdminPanel.Api.Operations;` в шапке Program.cs.)

- [ ] **Шаг 6.4: интеграционные тесты зелёные**

Run: `dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~CreateClusterApiTests"`
Ожидание: PASS (нужен Docker).

- [ ] **Шаг 6.5: коммит**

```bash
git add src/AdminPanel.Api/Operations/OperationsModule.cs src/AdminPanel.Api/Program.cs src/tests/AdminPanel.IntegrationTests/CreateClusterApiTests.cs
git commit -m "feat(t12): POST /api/clusters — 201/400/409/503 + интеграция против реального etcd"
```

---

## Задача 7: DTO чтения (state/nodes/requests/activeMoves/masterless)

**Файлы:**
- Modify: `src/AdminPanel.Api/Inspection/ClustersQuery.cs`
- Modify: `src/AdminPanel.Api/Inspection/ClusterDetailsQuery.cs`
- Modify: `src/AdminPanel.Api/Inspection/HaQuery.cs`
- Modify: `src/AdminPanel.Api/Inspection/OverviewQuery.cs`
- Test: `src/tests/AdminPanel.UnitTests/ClustersMappersTests.cs`,
  `InspectionMappersTests.cs`, `HaMappersTests.cs`;
  `src/tests/AdminPanel.IntegrationTests/InspectionApiTests.cs` (InspectionSnapshots + ассерты)

**Интерфейсы:**
- Consumes: Core-модель Задачи 2.
- Produces (использует фронт — задачи 9, 10):
  - `ClusterSummaryDto` + `bool NotInitialized`;
  - `ClusterDto` + `string State`; `static class ClusterStates { string Name(ClusterState) }`;
  - `NodeDto(string Name, string? State)`; `NodeRequestsDto(string Cpu, string Mem, string Disk)`;
  - `ShardDto` + `IReadOnlyList<NodeDto> Nodes` + `NodeRequestsDto? Requests`;
  - `HaScopeDto` + `NodeRequestsDto? Requests`;
  - `OverviewClusterDto` + `bool NotInitialized`;
  - `BucketStates.Name/TryParse` знают `NOT_INITIALIZED`.

- [ ] **Шаг 7.1: падающие unit-тесты мапперов**

В `ClustersMappersTests.cs` (сводка) — новый факт (снапшот кластера строится
по образцу существующих фикстур файла):

```csharp
    [Fact]
    public void Map_NotInitializedCluster_SetsFlagAndCountsMovesAsRealOnly()
    {
        // Arrange: все бакеты NOT_INITIALIZED + один SYNCING; шард без master и dsn
        var shard = new ShardInfo("shard1", "", [], null, null, null, 2, null,
            [new NodeInfo("shard1a", "NOT_INITIALIZED"), new NodeInfo("shard1b", "NOT_INITIALIZED")], null);
        var cluster = new ClusterInfo("fresh", "fresh", 4, 1755900000, ClusterState.NotInitialized,
            [shard],
            [
                new BucketInfo(0, "shard1", BucketState.NotInitialized, null),
                new BucketInfo(1, "shard1", BucketState.NotInitialized, null),
                new BucketInfo(2, "shard1", BucketState.NotInitialized, null),
                new BucketInfo(3, "shard1", BucketState.Syncing,
                    new MoveInfo("shard1", "shard1", 1, 2, "copy", null)),
            ], []);

        // Act
        var dto = ClustersMapper.Map([cluster]).Single();

        // Assert: бейдж-флаг есть; activeMoves = только реальные переезды (spec t12 §3.6);
        // «без мастера» у не поднятого кластера — не деградация (arch/03 §2)
        dto.NotInitialized.Should().BeTrue();
        dto.ActiveMoves.Should().Be(1);
        dto.ShardsWithMaster.Should().Be(0);
    }
```

В `InspectionMappersTests.cs` (детали кластера) — новый факт:

```csharp
    [Fact]
    public void Map_NotInitializedCluster_StateNodesAndRequests()
    {
        // Arrange: снапшот с HaScope-заявкой (join по <C>-<X>)
        var shard = new ShardInfo("shard1", "", [], null, null, null, 2, null,
            [new NodeInfo("shard1a", "NOT_INITIALIZED")], null);
        var cluster = new ClusterInfo("fresh", "fresh", 1, null, ClusterState.NotInitialized,
            [shard], [new BucketInfo(0, "shard1", BucketState.NotInitialized, null)], []);
        var scopes = new List<HaScope>
        {
            new("fresh-shard1", "fresh", "shard1", true, null, null, false,
                "2", "8Gi", "100Gi", [], null),
        };

        // Act
        var dto = ClusterDetailsMapper.Map(cluster, nowUnix: 100, null, null, [], scopes);

        // Assert
        dto.State.Should().Be("NOT_INITIALIZED");
        dto.Shards.Single().Nodes.Single().Name.Should().Be("shard1a");
        dto.Shards.Single().Requests!.Cpu.Should().Be("2");
        dto.Shards.Single().Requests.Mem.Should().Be("8Gi");
        dto.Buckets.Single().State.Should().Be("NOT_INITIALIZED");
    }
```

Сигнатура `ClusterDetailsMapper.Map` меняется: последний параметр
`IReadOnlyList<HaScope> scopes` (handler передаёт `snapshot.HaScopes`) —
существующие вызовы в тестах дополнить `[]`.

В `HaMappersTests.cs` — новый факт:

```csharp
    [Fact]
    public void MapDetails_WithRequests_MapsRequests()
    {
        // Arrange
        var scope = new HaScope("fresh-shard1", "fresh", "shard1", true, null, null, false,
            "0.5", "8Gi", "100Gi", [], null);

        // Act
        var dto = HaMappers.MapDetails(scope);

        // Assert
        dto.Requests!.Cpu.Should().Be("0.5");
        dto.Requests.Disk.Should().Be("100Gi");
    }
```

И факт Overview (в `InspectionMappersTests.cs` или где живёт OverviewMapper-тест):

```csharp
    [Fact]
    public void Map_NotInitializedCluster_ZeroMasterlessAndNotInActiveMovesList()
    {
        // Arrange: 2 шарда без мастера, бакеты NOT_INITIALIZED
        var shard = new ShardInfo("shard1", "", [], null, null, null, 1, null, [], null);
        var cluster = new ClusterInfo("fresh", "fresh", 2, null, ClusterState.NotInitialized,
            [shard],
            [new BucketInfo(0, "shard1", BucketState.NotInitialized, null),
             new BucketInfo(1, "shard1", BucketState.NotInitialized, null)], []);
        var snapshot = TestSnapshots.Healthy(DateTimeOffset.UnixEpoch) with { Clusters = [cluster] };

        // Act
        var dto = OverviewMapper.Map(snapshot, DateTimeOffset.UnixEpoch, 3);

        // Assert: masterless=0 (ожидаемо), notInitialized=true; в activeMoves не попали
        dto.Clusters.Single().MasterlessShards.Should().Be(0);
        dto.Clusters.Single().NotInitialized.Should().BeTrue();
        dto.ActiveMoves.Should().BeEmpty();
    }
```

- [ ] **Шаг 7.2: проверить падение**

Run: `dotnet build src/AdminPanel.slnx`
Ожидание: FAIL компиляции (новых полей DTO/параметра scopes нет).

- [ ] **Шаг 7.3: реализация DTO и мапперов**

`ClustersQuery.cs`:
- `ClusterSummaryDto` — вставить `bool NotInitialized,` после `bool Incomplete,`.
- `ClustersMapper.Map`: `c.Incomplete` → после него
  `c.State == ClusterState.NotInitialized`; `ActiveMoves` заменить на
  `c.Buckets.Count(b => b.State is BucketState.Syncing or BucketState.Frozen or BucketState.Aborting)`.

`ClusterDetailsQuery.cs`:
- `ClusterDto` — вставить `string State,` после `bool Incomplete,`.
- `ShardDto` — вставить `IReadOnlyList<NodeDto> Nodes,` и `NodeRequestsDto? Requests,`
  перед `ShardRuntimeDto? Runtime`.
- Новые записи и хелпер (в тот же файл):

```csharp
// Плановая нода шарда (arch/02 §9.1); state — raw-строка.
public sealed record NodeDto(string Name, string? State);

// Заявка ресурсов на ноду scope /service/<C>-<X>/request_* (arch/02 §2.2, §9.1).
public sealed record NodeRequestsDto(string Cpu, string Mem, string Disk);

// Канон state кластера (arch/03 §2).
public static class ClusterStates
{
    public static string Name(ClusterState state)
        => state == ClusterState.NotInitialized ? "NOT_INITIALIZED" : "ACTIVE";
}
```

- `BucketStates.Name`: добавить `BucketState.NotInitialized => "NOT_INITIALIZED",`;
  `TryParse`: добавить `case "NOT_INITIALIZED": state = BucketState.NotInitialized; return true;`.
- `ClusterDetailsMapper.Map` — сигнатура
  `Map(ClusterInfo cluster, long nowUnix, string? owner, BucketState? state, IReadOnlyList<StandNode> standNodes, IReadOnlyList<HaScope> haScopes)`;
  в `ClusterDto` передать `ClusterStates.Name(cluster.State)`; шарды:

```csharp
            [.. cluster.Shards.Select(s =>
            {
                // Заявка шарда — join scope "<C>-<X>" (все три ключа обязательны)
                var requests = haScopes
                    .Where(h => h.Matched && h.Cluster == cluster.Name && h.Shard == s.Name
                        && h.RequestCpu is not null && h.RequestMem is not null && h.RequestDisk is not null)
                    .Select(h => new NodeRequestsDto(h.RequestCpu!, h.RequestMem!, h.RequestDisk!))
                    .FirstOrDefault();
                return new ShardDto(
                    s.Name, s.Dsn, s.DsnHosts, s.ReplicasDeclared, s.MasterAddress, s.MasterLeaseAlive,
                    [.. s.Nodes.Select(n => new NodeDto(n.Name, n.State))],
                    requests,
                    s.Runtime is null ? null : MapRuntime(s.Runtime));
            })],
```

- `ClusterDetailsQueryHandler.Handle` — передать `snapshot.HaScopes` последним
  аргументом маппера.

`HaQuery.cs`: `HaScopeDto` — вставить `NodeRequestsDto? Requests,` после
`long? OptimeLeader,`; в `HaMappers.MapDetails`:

```csharp
            scope.RequestCpu is null || scope.RequestMem is null || scope.RequestDisk is null
                ? null
                : new NodeRequestsDto(scope.RequestCpu, scope.RequestMem, scope.RequestDisk),
```

`OverviewQuery.cs`:
- `OverviewClusterDto` — вставить `bool NotInitialized` последним полем.
- `OverviewMapper.Map`: кластеры —

```csharp
            [.. snapshot.Clusters.Select(c => new OverviewClusterDto(
                c.Name,
                c.Shards.Count,
                c.BucketsCount,
                c.Buckets.Count(b => b.State is BucketState.Syncing or BucketState.Frozen or BucketState.Aborting),
                c.State == ClusterState.NotInitialized
                    ? 0 // без мастера у не поднятого кластера — норма (arch/03 §2)
                    : c.Shards.Count(s => s.MasterAddress is null),
                c.State == ClusterState.NotInitialized))],
```

- список `ActiveMoves` — фильтр `Where(b => b.State is BucketState.Syncing
  or BucketState.Frozen or BucketState.Aborting)` вместо `!= Active`.

- [ ] **Шаг 7.4: починить существующие тесты/фикстуры компиляцией**

`dotnet build` → обновить вызовы `ClusterDetailsMapper.Map(...)` в
`InspectionMappersTests`/`ClustersMappersTests` (добавить последний аргумент
`[]` или scope-фикстуру). В `InspectionSnapshots.Clustered` (integration)
кластер `demo` уже получил канонические значения в Задаче 2.

- [ ] **Шаг 7.5: интеграционные ассерты чтения**

В `ClustersApiTests` добавить факт (снапшот собрать прямо в тесте):

```csharp
    [Fact]
    public async Task Clusters_NotInitializedCluster_FlaggedInSummaryAndDetails()
    {
        // Arrange: fresh — 1 шард (nodes a/b), бакеты NOT_INITIALIZED, scope с заявкой
        using var client = await LoginAsync();
        var unix = _factory.Time.Utc.ToUnixTimeSeconds();
        var cluster = new ClusterInfo("fresh", "fresh", 2, 1755900000, ClusterState.NotInitialized,
            [new ShardInfo("shard1", "", [], null, null, null, 2, null,
                [new NodeInfo("shard1a", "NOT_INITIALIZED"), new NodeInfo("shard1b", "NOT_INITIALIZED")], null)],
            [
                new BucketInfo(0, "shard1", BucketState.NotInitialized,
                    new MoveInfo("shard1", null, null, unix - 100, null, null)),
                new BucketInfo(1, "shard1", BucketState.NotInitialized,
                    new MoveInfo("shard1", null, null, unix - 100, null, null)),
            ], []);
        var scope = new AdminPanel.Core.HaScope("fresh-shard1", "fresh", "shard1", true, null, null, false,
            "2", "8Gi", "100Gi", [], null);
        _factory.Snapshot = InspectionSnapshots.Fixture(_factory.Time.Utc) with
        {
            Clusters = [cluster],
            HaScopes = [scope],
        };

        // Act
        var summary = await GetJsonAsync(client, "/api/clusters");
        var details = await GetJsonAsync(client, "/api/clusters/fresh");
        var filtered = await GetJsonAsync(client, "/api/clusters/fresh?state=NOT_INITIALIZED");

        // Assert: сводка (notInitialized, activeMoves=0), детали (state/nodes/requests), фильтр
        summary[0].GetProperty("notInitialized").GetBoolean().Should().BeTrue();
        summary[0].GetProperty("activeMoves").GetInt32().Should().Be(0);
        details.GetProperty("state").GetString().Should().Be("NOT_INITIALIZED");
        var shard = details.GetProperty("shards")[0];
        shard.GetProperty("nodes").GetArrayLength().Should().Be(2);
        shard.GetProperty("requests").GetProperty("cpu").GetString().Should().Be("2");
        details.GetProperty("buckets")[0].GetProperty("state").GetString().Should().Be("NOT_INITIALIZED");
        filtered.GetProperty("buckets").GetArrayLength().Should().Be(2);
    }
```

- [ ] **Шаг 7.6: все тесты зелёные**

Run: `dotnet test src/AdminPanel.slnx`
Ожидание: PASS целиком (unit + integration; Docker нужен).

- [ ] **Шаг 7.7: коммит**

```bash
git add -A src/AdminPanel.Api/Inspection src/tests
git commit -m "feat(t12): DTO чтения — state/nodes/requests, activeMoves/masterless семантика NOT_INITIALIZED"
```

---

## Задача 8: алерты (cluster-not-initialized + подавления)

**Файлы:**
- Create: `src/AdminPanel.Core/Alerting/Rules/ClusterNotInitializedRule.cs`
- Modify: `src/AdminPanel.Core/Alerting/Rules/MoveStaleRule.cs`
- Modify: `src/AdminPanel.Core/Alerting/Rules/ShardNoLeaderRule.cs`
- Modify (списки правил): `src/tests/AdminPanel.IntegrationTests/EtcdSnapshotIntegrationTests.cs`
  (EtcdTestHarness), `src/tests/AdminPanel.UnitTests/AlertEngineTests.cs`
- Test: `src/tests/AdminPanel.UnitTests/ShardingAlertRulesTests.cs` (+ новые
  факты), при необходимости новый `ClusterNotInitializedRuleTests.cs`

**Интерфейсы:**
- Consumes: `IAlertRule`/`AlertContext` (существующие), Core-модель Задачи 2.
- Produces: kind `cluster-not-initialized` (info, target `<C>`); правило
  регистрируется DI автоматически (`[InjectAsSingleton(typeof(IAlertRule))]`,
  `AddCore` → AutoRegistration).

- [ ] **Шаг 8.1: падающие тесты правил**

В `ShardingAlertRulesTests.cs` — используются СУЩЕСТВУЮЩИЕ хелперы файла
(`Now`, `DefaultOptions`, `Evaluate(rule, snapshot)`, `Snapshot(params
ClusterInfo[])`; `AlertContext(null, Now, 3)` собран внутри `Evaluate`):

```csharp
    [Fact]
    public void ClusterNotInitialized_Rule_FiresInfoAlert()
    {
        // Arrange
        var cluster = new ClusterInfo("fresh", "fresh", 1, null, ClusterState.NotInitialized, [], [], []);

        // Act
        var alerts = Evaluate(new ClusterNotInitializedRule(), Snapshot(cluster));

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Severity.Should().Be(AlertSeverity.Info);
        alert.Kind.Should().Be("cluster-not-initialized");
        alert.Target.Should().Be("fresh");
    }

    [Fact]
    public void MoveStale_DoesNotFire_ForNotInitializedBuckets()
    {
        // Arrange: NOT_INITIALIZED со штампом старше порога (600 c)
        var cluster = new ClusterInfo("fresh", "fresh", 1, null, ClusterState.Active, [],
            [new BucketInfo(0, "shard1", BucketState.NotInitialized,
                new MoveInfo("shard1", null, null, 1, 1, null))], []);

        // Act
        var alerts = Evaluate(new MoveStaleRule(DefaultOptions), Snapshot(cluster));

        // Assert: NOT_INITIALIZED — не переезд (arch/03 §4)
        alerts.Should().BeEmpty();
    }

    [Fact]
    public void ShardNoLeader_DoesNotFire_ForNotInitializedClusterScope()
    {
        // Arrange: matched scope без leader, кластер fresh — NOT_INITIALIZED
        var cluster = new ClusterInfo("fresh", "fresh", 1, null, ClusterState.NotInitialized,
            [new ShardInfo("shard1", "", [], null, null, null, 2, null, [], null)], [], []);
        var scope = new HaScope("fresh-shard1", "fresh", "shard1", true, null, null, false,
            null, null, null, [], null);
        var snapshot = TestSnapshots.Healthy(Now) with { Clusters = [cluster], HaScopes = [scope] };

        // Act
        var alerts = Evaluate(new ShardNoLeaderRule(), snapshot);

        // Assert: лидера нет потому, что ноды не подняты (spec t12 §3.7)
        alerts.Should().BeEmpty();
    }
```

(для HaScope-факта снапшот собран `with` поверх `Healthy` — хелпер
`Snapshot(...)` принимает только кластеры; HaScope-конструктор уже с
request-полями из Задачи 2.)

- [ ] **Шаг 8.2: проверить падение**

Run: `dotnet build src/AdminPanel.slnx`
Ожидание: FAIL — `ClusterNotInitializedRule` не существует; два других
факта упадут (alерты сейчас стреляют).

- [ ] **Шаг 8.3: реализация правил**

`src/AdminPanel.Core/Alerting/Rules/ClusterNotInitializedRule.cs`:

```csharp
using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Core.Alerting.Rules;

// cluster-not-initialized (info): кластер заявлен, но ноды не подняты (arch/03 §4;
// arch/02 §9) — заметка вместо critical-шумa не поднятого кластера (spec t12 §8.11).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class ClusterNotInitializedRule : IAlertRule
{
    public const string KindName = "cluster-not-initialized";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var cluster in snapshot.Clusters.Where(c => c.State == ClusterState.NotInitialized))
            yield return new Alert(
                $"{KindName}:{cluster.Name}",
                AlertSeverity.Info,
                KindName,
                cluster.Name,
                $"кластер {cluster.Name} заявлен (NOT_INITIALIZED): ноды не подняты, схемы не созданы",
                new Dictionary<string, string> { ["dbname"] = cluster.DbName ?? "missing" },
                null);
    }
}
```

`MoveStaleRule.Evaluate` — после `if (bucket.State == BucketState.Active)
continue;` добавить:

```csharp
            if (bucket.State == BucketState.NotInitialized)
                continue; // не переезд: начальное состояние создаваемого кластера (arch/03 §4)
```

`ShardNoLeaderRule.Evaluate` — перед циклом построить множество:

```csharp
        // Не поднятые кластеры: лидера нет потому, что нод нет (spec t12 §3.7)
        var notInitialized = snapshot.Clusters
            .Where(c => c.State == ClusterState.NotInitialized)
            .Select(c => c.Name)
            .ToHashSet();
```

и в цикле первым делом `if (scope.Cluster is not null && notInitialized.Contains(scope.Cluster))
continue;`.

- [ ] **Шаг 8.4: тестовый набор правил EtcdTestHarness**

`AlertEngine` в DI-хосте собирает правила автособором
(`[InjectAsSingleton(typeof(IAlertRule))]` + `AddCore` → AutoRegistration) —
новое правило подхватится само. Ручной список правил существует только в
интеграционном `EtcdTestHarness` (`EtcdSnapshotIntegrationTests.cs`):
добавить `new ClusterNotInitializedRule(),` рядом с `new
ClusterIncompleteRule(),`. `AlertEngineTests` трогать не нужно (там
`ConstRule`-заглушки, реальных правил нет).

- [ ] **Шаг 8.5: тесты зелёные**

Run: `dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~Alert"`
Ожидание: PASS (включая `AlertEngineTests`).

- [ ] **Шаг 8.6: коммит**

```bash
git add src/AdminPanel.Core/Alerting src/tests
git commit -m "feat(t12): алерт cluster-not-initialized + подавления move-stale/shard-no-leader"
```

---

## Задача 9: фронтенд — API-слой + форма создания

**Файлы:**
- Modify: `frontend/src/api/dto.ts`
- Modify: `frontend/src/api/queries.ts`
- Create: `frontend/src/pages/clusters/ClusterCreateModal.tsx`
- Modify: `frontend/src/pages/ClustersPage.tsx`

**Интерфейсы:**
- Consumes: `apiFetch` (POST+body), `queryKeys.clusters`, эндпоинт Задачи 6,
  DTO Задачи 7 (camelCase: `notInitialized`, `state`, `nodes`, `requests`).
- Produces: `createCluster(request: CreateClusterRequestDto): Promise<ClusterCreatedDto>`
  (использует Задача 10 для отображения, но модалка самодостаточна).

- [ ] **Шаг 9.1: DTO и query-функция**

`frontend/src/api/dto.ts` — новые/обновлённые типы:

```typescript
// Канон состояния кластера (arch/03 §2): отсутствие записи о state = ACTIVE.
export type ClusterStateName = 'ACTIVE' | 'NOT_INITIALIZED';

// POST /api/clusters — тело и ответ (arch/03 §1.1).
export interface CreateClusterRequestDto {
  name: string;
  buckets: number;
  shards: number;
  replicas: number;
  requestCpu: number;
  requestMem: number;
  requestDisk: number;
}

export interface ClusterCreatedDto {
  name: string;
  dbName: string;
  bucketsCount: number;
  shardsTotal: number;
  replicas: number;
  requestCpu: string;
  requestMem: string;
  requestDisk: string;
  state: ClusterStateName;
}
```

Правки существующих: `BucketStateName` добавить `| 'NOT_INITIALIZED'`;
`ClusterSummaryDto` + `notInitialized: boolean`; `ClusterDto` +
`state: ClusterStateName`; `ShardDto` + `nodes: NodeDto[]` +
`requests: NodeRequestsDto | null`; `HaScopeDto` + `requests:
NodeRequestsDto | null`; `OverviewClusterDto` + `notInitialized: boolean`;

```typescript
// Плановая нода шарда (arch/02 §9.1).
export interface NodeDto {
  name: string;
  state: string | null;
}

// Заявка ресурсов на ноду scope /service/<C>-<X>/request_* (arch/02 §9.1).
export interface NodeRequestsDto {
  cpu: string;
  mem: string;
  disk: string;
}
```

`frontend/src/api/queries.ts`:

```typescript
export function createCluster(request: CreateClusterRequestDto): Promise<ClusterCreatedDto> {
  return apiFetch<ClusterCreatedDto>('/api/clusters', { method: 'POST', body: request });
}
```

(+ импорт типов в шапке.)

- [ ] **Шаг 9.2: модалка**

`frontend/src/pages/clusters/ClusterCreateModal.tsx` (новый):

```tsx
// Форма создания кластера — единственная мутация панели (spec t12 §3.8).
// Клиентская валидация — зеркало серверной (arch/02 §9.3); сервер — истина.
import { useMutation, useQueryClient } from '@tanstack/react-query';
import {
  Alert,
  Button,
  Group,
  Modal,
  NumberInput,
  Stack,
  Text,
  TextInput,
} from '@mantine/core';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { createCluster } from '../../api/queries';
import type { CreateClusterRequestDto } from '../../api/dto';

// Границы — зеркало CreateClusterLimits (arch/02 §9.3).
const NAME_RE = /^[a-z][a-z0-9_]{0,62}$/;

const EMPTY: CreateClusterRequestDto = {
  name: '',
  buckets: 16,
  shards: 2,
  replicas: 2,
  requestCpu: 2,
  requestMem: 8,
  requestDisk: 100,
};

export function ClusterCreateModal({ opened, onClose }: { opened: boolean; onClose: () => void }) {
  const queryClient = useQueryClient();
  const [form, setForm] = useState<CreateClusterRequestDto>(EMPTY);
  const [fieldErrors, setFieldErrors] = useState<Record<string, string>>({});
  const set = <K extends keyof CreateClusterRequestDto>(key: K, value: CreateClusterRequestDto[K]) =>
    setForm((f) => ({ ...f, [key]: value }));

  const mutation = useMutation({
    mutationFn: createCluster,
    onSuccess: async () => {
      // Список кластеров обновит следующий тик refresher'а — инвалидация ключа
      onClose();
      setForm(EMPTY);
      await queryClient.invalidateQueries({ queryKey: ['clusters'] });
    },
  });

  // Зеркало серверной валидации: по полям, до отправки (spec t12 §2).
  function validate(): boolean {
    const errors: Record<string, string> = {};
    if (!NAME_RE.test(form.name)) errors.name = 'a-z, 0-9, _; начинается с буквы; без дефиса';
    if (!Number.isInteger(form.buckets) || form.buckets < 1 || form.buckets > 8192)
      errors.buckets = 'целое 1..8192';
    if (!Number.isInteger(form.shards) || form.shards < 1 || form.shards > 128 || form.shards > form.buckets)
      errors.shards = 'целое 1..128 и не больше бакетов';
    if (!Number.isInteger(form.replicas) || form.replicas < 1 || form.replicas > 26)
      errors.replicas = 'целое 1..26 (1 = только мастер)';
    if (form.requestCpu < 0.01 || form.requestCpu > 64) errors.requestCpu = '0.01..64';
    if (!Number.isInteger(form.requestMem) || form.requestMem < 1 || form.requestMem > 65536)
      errors.requestMem = 'целое 1..65536';
    if (!Number.isInteger(form.requestDisk) || form.requestDisk < 1 || form.requestDisk > 65536)
      errors.requestDisk = 'целое 1..65536';
    setFieldErrors(errors);
    return Object.keys(errors).length === 0;
  }

  function submit() {
    if (validate()) mutation.mutate(form);
  }

  // Ошибка сервера: 409 «имя занято» / 400 по полям / 503 «etcd» (ProblemDetails).
  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  return (
    <Modal opened={opened} onClose={onClose} title="Создать кластер" centered>
      <Stack gap="sm">
        <TextInput
          label="Имя кластера"
          description="уникально; dbname = имя"
          placeholder="shop"
          value={form.name}
          error={fieldErrors.name}
          onChange={(e) => set('name', e.currentTarget.value)}
        />
        <Group grow gap="sm">
          <NumberInput label="Бакеты" min={1} max={8192} value={form.buckets}
            error={fieldErrors.buckets} onChange={(v) => set('buckets', Number(v ?? 0))} />
          <NumberInput label="Шарды" min={1} max={128} value={form.shards}
            error={fieldErrors.shards} onChange={(v) => set('shards', Number(v ?? 0))} />
          <NumberInput label="Реплики" min={1} max={26} value={form.replicas}
            description="2 = мастер + реплика"
            error={fieldErrors.replicas} onChange={(v) => set('replicas', Number(v ?? 0))} />
        </Group>
        <Text size="sm" c="dimmed">Ресурсы нод (заявка, на каждую ноду)</Text>
        <Group grow gap="sm">
          <NumberInput label="CPU (ядра)" min={0.01} max={64} step={0.1} decimalScale={2}
            value={form.requestCpu} error={fieldErrors.requestCpu}
            onChange={(v) => set('requestCpu', Number(v ?? 0))} />
          <NumberInput label="Память (GiB)" min={1} max={65536} value={form.requestMem}
            error={fieldErrors.requestMem} onChange={(v) => set('requestMem', Number(v ?? 0))} />
          <NumberInput label="Диск (GiB)" min={1} max={65536} value={form.requestDisk}
            error={fieldErrors.requestDisk} onChange={(v) => set('requestDisk', Number(v ?? 0))} />
        </Group>
        {serverError !== null ? (
          <Alert color={serverError.status === 409 ? 'yellow' : 'red'} variant="light">
            {serverError.status === 409
              ? 'Имя уже занято — выберите другое'
              : serverError.status === 400
                ? (serverError.detail ?? 'Проверьте параметры')
                : 'etcd недоступен — повторите позже'}
          </Alert>
        ) : null}
        <Group justify="flex-end" mt="xs">
          <Button variant="default" onClick={onClose}>Отмена</Button>
          <Button loading={mutation.isPending} onClick={submit}>Создать</Button>
        </Group>
      </Stack>
    </Modal>
  );
}
```

- [ ] **Шаг 9.3: ClustersPage — кнопка и бейдж**

В `ClustersPage.tsx`: импорты `useState` и `ClusterCreateModal`; в начале
компонента `const [createOpened, setCreateOpened] = useState(false);`;
заголовок обернуть в Group с кнопкой:

```tsx
      <Group justify="space-between">
        <Title order={2}>Кластеры</Title>
        <Button onClick={() => setCreateOpened(true)}>Создать кластер</Button>
      </Group>
      <ClusterCreateModal opened={createOpened} onClose={() => setCreateOpened(false)} />
```

(импорт `Button` из `@mantine/core`). В `ClusterRow` — бейдж после
incomplete:

```tsx
        {cluster.notInitialized ? (
          <Tooltip label="кластер заявлен, ноды не подняты">
            <Badge color="gray" variant="light" ml={5}>не инициализирован</Badge>
          </Tooltip>
        ) : null}
```

(+ `Tooltip` к импортам mantine.)

- [ ] **Шаг 9.4: сборка фронта**

Run: `cd frontend && npm run build`
Ожидание: `tsc` + vite build без ошибок.

- [ ] **Шаг 9.5: коммит**

```bash
git add frontend/src/api frontend/src/pages/ClustersPage.tsx frontend/src/pages/clusters
git commit -m "feat(t12): форма создания кластера в панели Кластеры"
```

---

## Задача 10: фронтенд — отображение NOT_INITIALIZED и заявок

**Файлы:**
- Modify: `frontend/src/components/BucketStateBadge.tsx`
- Modify: `frontend/src/pages/cluster-details/ShardsTab.tsx`
- Modify: `frontend/src/pages/cluster-details/MovesTab.tsx`
- Modify: `frontend/src/pages/cluster-details/BucketsTab.tsx`
- Modify: `frontend/src/pages/HaScopeDetailsPage.tsx`
- Modify: `frontend/src/pages/OverviewPage.tsx`

**Интерфейсы:**
- Consumes: DTO Задачи 9 (`NodeDto`, `NodeRequestsDto`, `notInitialized`,
  `state`, `BucketStateName` c `NOT_INITIALIZED`).

- [ ] **Шаг 10.1: BucketStateBadge**

В `STATE_META` добавить:

```typescript
  NOT_INITIALIZED: { color: 'gray', label: 'не инициализирован' },
```

- [ ] **Шаг 10.2: ShardsTab — ноды и ресурсы**

После колонки «Мастер» добавить две ячейки в `<Table.Tr>` заголовка:

```tsx
              <Table.Th>Ноды</Table.Th>
              <Table.Th>Ресурсы на ноду</Table.Th>
```

и в `ShardRow`:

```tsx
      <Table.Td>
        {shard.nodes.length === 0 ? '—' : (
          <Group gap={4}>
            {shard.nodes.map((n) => (
              <Tooltip key={n.name} label={n.state ?? '—'}>
                <Badge color={n.state === 'NOT_INITIALIZED' ? 'gray' : 'teal'} variant="light">
                  {n.name}
                </Badge>
              </Tooltip>
            ))}
          </Group>
        )}
      </Table.Td>
      <Table.Td>
        {shard.requests === null ? '—' : (
          <Text ff="monospace" size="sm">
            {shard.requests.cpu} CPU · {shard.requests.mem} · {shard.requests.disk}
          </Text>
        )}
      </Table.Td>
```

(+ `Group` к импортам mantine; `minWidth` таблицы поднять до 1200.)

- [ ] **Шаг 10.3: MovesTab — фильтр только переездов**

```typescript
  const moves = buckets.filter((b) => b.state === 'SYNCING' || b.state === 'FROZEN' || b.state === 'ABORTING');
```

(комментарий: NOT_INITIALIZED — не переезд, spec t12 §3.8; текст пустого
состояния не меняется).

- [ ] **Шаг 10.4: BucketsTab — нейтральная подсветка NOT_INITIALIZED и фильтр**

`frontend/src/pages/cluster-details/BucketsTab.tsx`:

1) В `STATE_FILTERS` (после `ABORTING`) добавить опцию:

```typescript
  { value: 'NOT_INITIALIZED', label: 'NOT_INITIALIZED' },
```

(фильтр `non-active` покрывает NOT_INITIALIZED автоматически —
`b.state !== 'ACTIVE'` в `byState`; каноническое значение фильтруется
селектом напрямую через `stateFilter as BucketStateName`).

2) В `BucketRow` заменить `const nonActive = bucket.state !== 'ACTIVE';`
(сейчас красит жёлтым и NOT_INITIALIZED-бакеты нового кластера) на
разделение «реальный переезд» / «не инициализирован»:

```tsx
  // Жёлтый фон — только реальные переезды (SYNCING/FROZEN/ABORTING);
  // NOT_INITIALIZED — нейтральный серый: это начальное состояние
  // создаваемого кластера, не деградация (spec t12 §3.8).
  const moveRow = bucket.state === 'SYNCING' || bucket.state === 'FROZEN' || bucket.state === 'ABORTING';
  const notInitialized = bucket.state === 'NOT_INITIALIZED';
```

и в `<Table.Tr>` заменить фон:

```tsx
    <Table.Tr
      style={{
        backgroundColor: moveRow
          ? 'var(--mantine-color-yellow-light)'
          : notInitialized
            ? 'var(--mantine-color-gray-light)'
            : undefined,
      }}
    >
```

(комментарий в строке 1 файла «подсветка не-ACTIVE» обновить на
«подсветка переездов + нейтральная для NOT_INITIALIZED»).

- [ ] **Шаг 10.5: HaScopeDetailsPage — блок заявки**

После `MembersTable` (перед Accordion) — блок при наличии `requests`:

```tsx
      {data.requests === null ? null : (
        <Group gap="sm">
          <Text c="dimmed" size="sm">Заявленные ресурсы нод:</Text>
          <Badge variant="light" color="gray">{data.requests.cpu} CPU</Badge>
          <Badge variant="light" color="gray">{data.requests.mem}</Badge>
          <Badge variant="light" color="gray">{data.requests.disk}</Badge>
        </Group>
      )}
```

- [ ] **Шаг 10.6: OverviewPage — приглушение неинициализированных**

В `ClustersCard` (строка кластера — где рендерятся `c.name`/`c.shards`):
рядом с именем кластера вывести серый бейдж при `c.notInitialized`, а строку
приглушить (`opacity` или `c="dimmed"` для числа шардов). Минимальная правка
по месту (файл использует `data.clusters`):

```tsx
          <Group gap="xs" wrap="nowrap">
            <Anchor component={Link} to={`/clusters/${c.name}`} size="sm">{c.name}</Anchor>
            {c.notInitialized ? (
              <Badge color="gray" variant="light">не инициализирован</Badge>
            ) : null}
          </Group>
```

- [ ] **Шаг 10.7: сборка фронта**

Run: `cd frontend && npm run build`
Ожидание: без ошибок.

- [ ] **Шаг 10.8: коммит**

```bash
git add frontend/src
git commit -m "feat(t12): отображение NOT_INITIALIZED, плановых нод и заявок ресурсов"
```

---

## Задача 11: dev-stand e2e-чек создания

**Файлы:**
- Create: `dev-stand/checks/15-cluster-create.sh`

**Интерфейсы:**
- Consumes: стенд `dev-stand` (etcd-сервис `etcd`, панель `:5050`, логин
  admin/admin — как `10-smoke-api.sh`), эндпоинт Задачи 6.

- [ ] **Шаг 11.1: скрипт**

`dev-stand/checks/15-cluster-create.sh`:

```bash
#!/usr/bin/env bash
# E2E создания кластера: POST /api/clusters -> ключи в etcd -> список/детали
# (spec t12 §3.9). Идемпотентность чека: префиксы smoke-кластера чистятся перед прогоном.
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${ADMINPANEL_URL:-http://localhost:5050}"
JAR="$(mktemp)"; trap 'rm -f "$JAR"' EXIT

# Arrange: панель жива + логин (паттерн 10-smoke-api.sh)
for i in $(seq 1 60); do curl -fsS "$BASE/api/healthz" >/dev/null 2>&1 && break; sleep 1; done
curl -fsS "$BASE/api/healthz" >/dev/null || { echo "❌ панель не отвечает: $BASE"; exit 1; }
curl -fsS -c "$JAR" -o /dev/null -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  || { echo "❌ login admin/admin не прошёл"; exit 1; }

# Чистка прошлых прогонов: только свои ключи (префикс кластера + request_*).
ect() { docker compose exec -T etcd etcdctl --endpoints=http://localhost:2379 "$@"; }
ect del --prefix /clusters/smoke >/dev/null
for k in request_cpu request_mem request_disk; do
  ect del "/service/smoke-shard1/$k" >/dev/null
  ect del "/service/smoke-shard2/$k" >/dev/null
done

# Act: создание (4 бакета, 2 шарда, 2 реплики, 0.5 CPU / 8Gi / 100Gi на ноду)
code="$(curl -s -o /tmp/t12-create.json -w '%{http_code}' -b "$JAR" -X POST "$BASE/api/clusters" \
  -H 'Content-Type: application/json' \
  -d '{"name":"smoke","buckets":4,"shards":2,"replicas":2,"requestCpu":0.5,"requestMem":8,"requestDisk":100}')"
[ "$code" = 201 ] || { echo "❌ POST /api/clusters = $code: $(cat /tmp/t12-create.json)"; exit 1; }
echo "  создан: $(jq -c '{name,bucketsCount,shardsTotal,replicas,requestCpu,requestMem,requestDisk,state}' /tmp/t12-create.json)"

# Assert: ключи контракта в etcd (arch/02 §9.1)
[ "$(ect get /clusters/smoke/config --print-value-only | jq -r '.state')" = "NOT_INITIALIZED" ] \
  || { echo "❌ config.state != NOT_INITIALIZED"; exit 1; }
[ "$(ect get /clusters/smoke/shards/shard1/nodes/shard1b/state --print-value-only)" = "NOT_INITIALIZED" ] \
  || { echo "❌ нода shard1b не NOT_INITIALIZED"; exit 1; }
[ "$(ect get /service/smoke-shard2/request_mem --print-value-only)" = "8Gi" ] \
  || { echo "❌ /service/smoke-shard2/request_mem != 8Gi"; exit 1; }
# etcdctl get --prefix отдаёт значения в порядке ключей (bucket_0..3) — БЕЗ sort,
# чтобы проверить именно round-robin-раскладку: shard1 shard2 shard1 shard2
routing="$(ect get --prefix /clusters/smoke/buckets/routing --print-value-only | tr '\n' ' ')"
[ "$routing" = "shard1 shard2 shard1 shard2 " ] || { echo "❌ routing round-robin: $routing"; exit 1; }
echo "  etcd: config/nodes/request_*/routing — контракт §9.1 соблюдён"

# Assert: повтор — 409 (клэйм)
code="$(curl -s -o /dev/null -w '%{http_code}' -b "$JAR" -X POST "$BASE/api/clusters" \
  -H 'Content-Type: application/json' \
  -d '{"name":"smoke","buckets":4,"shards":2,"replicas":2,"requestCpu":0.5,"requestMem":8,"requestDisk":100}')"
[ "$code" = 409 ] || { echo "❌ повторное создание = $code, ожидался 409"; exit 1; }
echo "  повторное создание -> 409"

# Assert: панель видит кластер (следующий тик ≤ 3 c + polling)
for i in $(seq 1 15); do
  curl -fsS -b "$JAR" "$BASE/api/clusters" | jq -e 'any(.[]; .name=="smoke" and .notInitialized)' >/dev/null && break
  sleep 1
done
curl -fsS -b "$JAR" "$BASE/api/clusters" | jq -e 'any(.[]; .name=="smoke" and .notInitialized)' >/dev/null \
  || { echo "❌ /api/clusters не видит smoke (notInitialized)"; exit 1; }
curl -fsS -b "$JAR" "$BASE/api/clusters/smoke" | jq -e \
  '.state=="NOT_INITIALIZED" and .shards[0].requests.cpu=="0.5" and (.shards[0].nodes|length)==2' >/dev/null \
  || { echo "❌ /api/clusters/smoke: state/requests/nodes"; exit 1; }
echo "  /api/clusters/smoke: NOT_INITIALIZED, заявки и ноды видны"
echo "✓ 15-cluster-create: создание кластера e2e прошло"
```

```bash
chmod +x dev-stand/checks/15-cluster-create.sh
```

- [ ] **Шаг 11.2: прогон на стенде (если Docker доступен)**

Стенд и панель поднимаются по `arch/04-local-stand.md` §5 и README dev-станда
(quick-профиль: etcd+seed; панель — `dotnet run --project src/AdminPanel.Api`
с `AdminPanel__Etcd__Endpoints__0` на стенд). Затем:

```bash
cd dev-stand && ./checks/15-cluster-create.sh
```

Ожидание: `✓ 15-cluster-create: создание кластера e2e прошло`.
Если Docker/стенд недоступен в окружении исполнителя — пометить шаг
отложенным (его повторит Задача 12, шаг 12.3).

- [ ] **Шаг 11.3: коммит**

```bash
git add dev-stand/checks/15-cluster-create.sh
git commit -m "feat(t12): dev-stand e2e-чек создания кластера"
```

---

## Задача 12: финальная зелёная сборка целиком

**Файлы:** без новых изменений (только исправление найденного).

- [ ] **Шаг 12.1: бэкенд**

Run:
```bash
dotnet build src/AdminPanel.slnx -c Release && dotnet test src/AdminPanel.slnx -c Release
```
Ожидание: build 0 warnings/errors (TreatWarningsAsErrors), все тесты PASS
(нужен Docker для Testcontainers; без Docker —
`dotnet test --filter "FullyQualifiedName~AdminPanel.UnitTests"` и явно
зафиксировать в отчёте, что integration пропущены по среде).

- [ ] **Шаг 12.2: фронтенд**

Run: `cd frontend && npm run build`
Ожидание: tsc + vite build без ошибок.

- [ ] **Шаг 12.3: e2e стенда (если Docker доступен)**

Полный прогон `checks/` по канону README dev-станда (`dev-stand/README.md`:
`00-up → 10 → 20 → 30 → 40`; порядок важен — 30-й делает failover s1,
40-й рассчитан на топологию после него; повторный прогон — только с чистого
состояния `90-down.sh -v`). Наш `15-cluster-create` встаёт по номеру между
10 и 20: он не мешает 20-alerts (точечные `any(...)`-проверки; появление
info-алерта `cluster-not-initialized:smoke` ожидаемо и не сверяется) и не
трогает топологию PG. Финал — очистка стенда:

```bash
cd dev-stand && ./checks/00-up.sh && ./checks/10-smoke-api.sh \
  && ./checks/15-cluster-create.sh && ./checks/20-alerts.sh \
  && ./checks/30-failover.sh && ./checks/40-live-probes.sh \
  && ./checks/90-down.sh -v
```

Ожидание: все чеки зелёные (состав и порядок — как в README dev-станда,
дополненные 15-м).

- [ ] **Шаг 12.4: сверка критериев приёмки spec §6**

Пройти по 9 пунктам `spec.md` §6 и убедиться, что каждый закрыт (1 — форма и
409-гонка покрыта клэйм-тестом; 2–3 — CreateClusterApiTests/план; 4 — Theory
400; 5 — polling и DTO; 6 — алерт-тесты; 7 — прежние тесты зелёные; 8 —
compensation-тест; 9 — шаги 12.1–12.3). Расхождения — исправить и вернуться
к соответствующей задаче.

- [ ] **Шаг 12.5: коммит (если были правки)**

```bash
git add -A && git commit -m "fix(t12): правки финальной сборки" || true
```

---

## Задача 13: мерж-гейт — удалить тег из roadmap (последний коммит ветки)

**Файлы:**
- Modify: `arch/roadmap/sharding.md`

Правило AGENTS.md: задача слита в `main` → пункт удаляется из roadmap тем же
коммитом. Задача будет влита этим же dev-flow, поэтому последним коммитом
ветки возвращаем файл к каноническому виду (пункт `t12-cluster-create`
исполнен; история — в git и `docs/superpowers/`).

- [ ] **Шаг 13.1: вернуть roadmap к виду без задачи**

Заменить содержимое `arch/roadmap/sharding.md` на:

```markdown
# Трек: sharding (инспекция кластеров/шардов/бакетов)

Контекст: [../02-etcd-contract.md](../02-etcd-contract.md) §2.1,
[../03-panels.md](../03-panels.md) (DTO, алерты).

## Задачи
```

(ссылка §9 в строке контекста убирается вместе с пунктом — трек-файл
возвращается к прежнему канону; сам §9 остаётся в `arch/02` навсегда.)

- [ ] **Шаг 13.2: коммит — последний в ветке**

```bash
git add arch/roadmap/sharding.md
git commit -m "chore(t12): мерж-гейт — убрать t12-cluster-create из roadmap (задача исполнена)"
```

После этого ветка `feat-cluster-create` готова к ревью и мержу в `main`
(флоу dev-flow: ревью → мерж; правок кода больше не предполагается).

---

## Самопроверка плана (выполнена при составлении + после ревью Фазы 4)

- **Покрытие spec:** §3.1 → задачи 3–5; §3.2 → 2; §3.3 → 3–4; §3.4 → 1;
  §3.5 → 5–6; §3.6 → 7; §3.7 → 8; §3.8 → 9–10 (включая BucketsTab);
  §3.9 → 11; §3.10 → 2/3/4/5/6/8; фазы §4 → задачи 1–11; критерии §6 →
  задача 12.4; roadmap-правило → задача 13. Пробелов нет.
- **Типы:** `CreateClusterRequest` (Etcd.Writing) используется и как тело
  POST (Задача 6); `NodeRequestsDto` один и тот же в ShardDto/HaScopeDto;
  `ClusterCreatePlan.NotInitialized` — константа канона. Имена сверены
  между задачами.
- **Известная тонкость:** Задача 2 ломает позиционные конструкторы
  фикстур — правки каноническими значениями включены в задачу (шаг 2.6),
  список файлов по grep приведён.
- **Правки ревью Фазы 4:** (1) `errors.<field>` в ProblemDetails — массив
  сообщений `new[] { e.Message }` (канон RFC 9457; тест 6.1 читает
  `GetArrayLength`) — Задача 6, шаг 6.3; (2) `BucketsTab.tsx` добавлен в
  Задачу 10 (шаг 10.4: серый фон NOT_INITIALIZED вместо жёлтого, опция
  фильтра) и в карту файлов; (3) Задача 12.3 — полный прогон чеков с
  `30-failover.sh` по канону README (порядок 00→10→15→20→30→40→90-down -v),
  с обоснованием безопасности 15-го перед 20-м.
