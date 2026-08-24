# UI запуска переноса бакетов (заявки /pgworker/moves/) — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Панель ставит очередь заявок на переезды бакетов (`/pgworker/moves/<C>/bucket_<i>`), показывает её во вкладке «Переезды», выполнение — PgWorker (последовательно, одна заявка кластера за раз).

**Architecture:** Пятая мутация панели по образцу add-shard (CQRS-команда → активный endpoint из снапшота → прямые чтения → txn-клэйм per key, без компенсации); чтение очереди — новый range в тике `SnapshotRefresher` (образец portalloc); инспекция отдаёт `pendingMoves` в деталях кластера; фронт — модал «Перенести бакеты» + блок очереди.

**Tech Stack:** .NET 10 Minimal API + CQRS (`Result`), xUnit + FluentAssertions, Testcontainers (etcd v3.5.21), React 19 + Mantine 9 + TanStack Query + Vite.

**Spec:** `docs/superpowers/2026-08-24-move-buckets-ui/spec.md` (в этом же worktree; все design-решения — spec §3, arch/02 §9.7, arch/03 §1.5/§3.3). arch-файлы уже правлены (Фаза 1) — код не должен им противоречить.

## Global Constraints

- Русский — комментарии/сообщения об ошибках; идентификаторы — английские.
- .NET 10, `Nullable=enable`, `TreatWarningsAsErrors=true` — сборка без warning'ов.
- Тесты — с AAA-комментариями (`// Arrange` / `// Act` / `// Assert`).
- Фикстуры парсеров — реальные фрагменты значений (формат заявки — JSON `MoveRequest` из `../pg`: `{"op","to","old_shard","skip_reverse","resume","force","requested_unix","requested_by"}`).
- Команды сборки/проверок: backend — `cd src && dotnet build AdminPanel.slnx`; тесты — `cd src && dotnet test AdminPanel.slnx` (интеграционные помечены «[Docker]» — Testcontainers поднимает `quay.io/coreos/etcd:v3.5.21`, Docker на стенде доступен); фронт — `cd frontend && npm run build`.
- Коммитов в шагах НЕТ — их делает execute-агент по своему графику.
- Все пути ниже — от корня worktree `/Users/demakaev/ZCodeProject/worktrees/feat-move-buckets-ui`.
- Решения, принятые при планировании (в дополнение к spec §3), помечены «РП:» и обязательны.

---

### Task 1: Модель `MoveTicket` + `EtcdSnapshot.MoveTickets`

**Вход:** arch/02 §2.3.1/§3 (формат заявки и поле снапшота); текущий `EtcdSnapshot` без очереди.

**Действие:** новый record в Core; новое поле в снапшоте после `StandNodes`; починить все конструкторы (передать `[]`/прежние данные), сборку и тесты — компилятор перечислит места.

**Выход:** компилирующееся ядро модели; поведение не изменилось.

**Проверка:** `cd src && dotnet build AdminPanel.slnx` — 0 errors/warnings; `cd src && dotnet test AdminPanel.slnx` (юнит, без Docker-коллекций — при недоступном Docker прогнать юнит-проект) — зелёные.

**Связь со spec:** §4.2 (модель), arch/02 §3.

**Files:**
- Create: `src/AdminPanel.Core/MoveTicket.cs`
- Modify: `src/AdminPanel.Core/EtcdSnapshot.cs`
- Modify: `src/AdminPanel.Etcd/SnapshotBuilder.cs`
- Modify: `src/AdminPanel.Etcd/SnapshotRefresher.cs` (только `FailTick`)
- Modify (компилятор покажет все): `src/tests/AdminPanel.UnitTests/TestSnapshots.cs`, `src/tests/AdminPanel.IntegrationTests/InspectionApiTests.cs` (`InspectionSnapshots.Fixture`)

**Interfaces (produces):** `public sealed record MoveTicket(string Cluster, string Bucket, int? BucketId, string Op, string? To, long RequestedUnix, string? RequestedBy);` — namespace `AdminPanel.Core`. `EtcdSnapshot` получает поле `IReadOnlyList<MoveTicket> MoveTickets` **после** `StandNodes` (позиционно 6-й параметр конструктора).

- [ ] **Step 1.1: Создать `src/AdminPanel.Core/MoveTicket.cs`**

```csharp
namespace AdminPanel.Core;

// Заявка на переезд — значение /pgworker/moves/<C>/<bucket> (arch/02 §2.3.1,
// формат PgWorker MoveRequest). Op — raw-строка канона op (move|rollback|finalize|abort);
// BucketId — id из leaf'а "bucket_<i>" (null у неканонического leaf'а).
public sealed record MoveTicket(
    string Cluster, string Bucket, int? BucketId,
    string Op, string? To, long RequestedUnix, string? RequestedBy);
```

- [ ] **Step 1.2: Добавить поле в `EtcdSnapshot`** (`src/AdminPanel.Core/EtcdSnapshot.cs`)

В record `EtcdSnapshot` после `StandNodes` добавить строку:

```csharp
    IReadOnlyList<MoveTicket> MoveTickets,           // очередь заявок /pgworker/moves/ (arch/02 §2.3.1)
```

- [ ] **Step 1.3: Починить все `new EtcdSnapshot(...)`**

В каждом позиционном вызове после аргумента `StandNodes` вставить `[]` (или прежние данные):

- `src/AdminPanel.Etcd/SnapshotBuilder.cs` — `SnapshotBuilder.Build`: во временной версии передать `[]` на новой позиции (Task 3 заменит на параметр).
- `src/AdminPanel.Etcd/SnapshotRefresher.cs` — `FailTick`: передать `previous?.MoveTickets ?? []` (очередь не теряется на отказном тике — как `Clusters`).
- `src/tests/AdminPanel.UnitTests/TestSnapshots.cs` — `Healthy`: вставить `[]` между `StandNodes`-аргументом и `Probes`.
- `src/tests/AdminPanel.IntegrationTests/InspectionApiTests.cs` — `InspectionSnapshots.Fixture`: аналогично (после 5-го `[]` — StandNodes — идёт новый `[]`, потом прежние `[]`-Probes и `[alerts]`).
- Прогнать сборку: компилятор перечислит прочие места (если есть) — везде вставить `[]` на новую позицию.

- [ ] **Step 1.4: Проверка**

`cd src && dotnet build AdminPanel.slnx` → без ошибок; `cd src && dotnet test AdminPanel.slnx` → зелёные (юнит; [Docker]-коллекции по умолчанию входят в `dotnet test` — прогнать полностью).

---

### Task 2: `MovesQueueParser` + фикстура реальных заявок (TDD)

**Вход:** Task 1 (`MoveTicket`); формат заявки PgWorker (spec §6).

**Действие:** фикстура `moves-queue.json` с реальными фрагментами тел заявок → падающий тест парсера → реализация чистой функции.

**Выход:** `MovesQueueParser.Parse(IReadOnlyList<Kv>) → MovesParseResult(Tickets, Errors)`.

**Проверка:** новый тест красный до реализации, зелёный после; вся сборка зелёная.

**Связь со spec:** §4.2 (парсер), §7.2 (критерий фикстур); arch/02 §2.3.1, §7 (битая заявка → ParseError).

**Files:**
- Create: `src/tests/AdminPanel.UnitTests/EtcdFixtures/moves-queue.json`
- Create: `src/tests/AdminPanel.UnitTests/MovesQueueParserTests.cs`
- Create: `src/AdminPanel.Etcd/Parsing/MovesQueueParser.cs`

**Interfaces (produces):**

```csharp
// namespace AdminPanel.Etcd.Parsing
public sealed record MovesParseResult(
    IReadOnlyList<MoveTicket> Tickets,
    IReadOnlyList<KeyParseError> Errors);
public static class MovesQueueParser
{
    public const string Prefix = "/pgworker/moves/";
    public static MovesParseResult Parse(IReadOnlyList<Kv> kvs);
}
```

(`Kv` — `AdminPanel.Etcd.Client.Kv`, `KeyParseError` — `AdminPanel.Core`.)

- [ ] **Step 2.1: Фикстура `moves-queue.json`** (формат загрузчика — массив `{"key","value","modRevision"}`)

```json
[
  {"key": "/pgworker/moves/demo/bucket_3", "value": "{\"op\":\"move\",\"to\":\"shard2\",\"requested_unix\":1755850000,\"requested_by\":\"ops\"}", "modRevision": 41},
  {"key": "/pgworker/moves/demo/bucket_5", "value": "{\"op\":\"rollback\",\"to\":\"shard1\",\"old_shard\":\"shard2\",\"skip_reverse\":true,\"requested_unix\":1755850060,\"requested_by\":\"etcdctl\"}", "modRevision": 42},
  {"key": "/pgworker/moves/demo/bucket_7", "value": "{\"op\":\"finalize\",\"old_shard\":\"shard1\",\"force\":true,\"requested_unix\":1755850120}", "modRevision": 43},
  {"key": "/pgworker/moves/demo/bucket_9", "value": "{\"op\":\"abort\",\"requested_unix\":1755850180,\"requested_by\":\"ops\"}", "modRevision": 44},
  {"key": "/pgworker/moves/demo/weird", "value": "{\"op\":\"move\",\"to\":\"shard2\",\"requested_unix\":1755850200}", "modRevision": 45},
  {"key": "/pgworker/moves/shop/bucket_1", "value": "{\"op\":\"move\",\"to\":\"shard9\",\"requested_unix\":1755850240,\"requested_by\":\"ops\"}", "modRevision": 46},
  {"key": "/pgworker/moves/demo/bucket_11", "value": "{\"op\":\"dance\",\"to\":\"shard2\",\"requested_unix\":1755850260}", "modRevision": 47},
  {"key": "/pgworker/moves/demo/bucket_13", "value": "{oops", "modRevision": 48},
  {"key": "/pgworker/moves/demo/bucket_15", "value": "{\"to\":\"shard2\",\"requested_unix\":1755850320}", "modRevision": 49},
  {"key": "/pgworker/moves/broken", "value": "{\"op\":\"move\",\"to\":\"x\",\"requested_unix\":1}", "modRevision": 50}
]
```

- [ ] **Step 2.2: Тест `MovesQueueParserTests.cs`** (AAA; запустить, убедиться в ошибке компиляции «MovesQueueParser не найден» — это «красный»)

```csharp
using AdminPanel.Core;
using AdminPanel.Etcd.Client;
using AdminPanel.Etcd.Parsing;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Парсер очереди заявок /pgworker/moves/ на реальных фрагментах тел заявок
// (формат MoveRequest из ../pg; arch/02 §2.3.1, §7).
public class MovesQueueParserTests
{
    [Fact]
    public void Parse_RealFixture_TicketsAndErrors()
    {
        // Arrange
        var kv = EtcdFixtures.LoadKv("moves-queue.json");

        // Act
        var result = MovesQueueParser.Parse(kv);

        // Assert: 6 заявок — все канонические op, поля прочитаны толерантно
        result.Tickets.Should().HaveCount(6);
        result.Tickets.Should().Contain(new MoveTicket(
            "demo", "bucket_3", 3, "move", "shard2", 1755850000L, "ops"));
        result.Tickets.Should().Contain(t => t.Cluster == "demo" && t.Bucket == "bucket_5"
            && t.Op == "rollback" && t.To == "shard1" && t.RequestedUnix == 1755850060L);
        result.Tickets.Should().Contain(t => t.Bucket == "bucket_7" && t.Op == "finalize"
            && t.To is null && t.RequestedBy is null);        // to/requested_by отсутствуют
        result.Tickets.Should().Contain(t => t.Bucket == "bucket_9" && t.Op == "abort");
        result.Tickets.Should().Contain(t => t.Bucket == "weird" && t.BucketId is null); // неканонический leaf
        result.Tickets.Should().Contain(t => t.Cluster == "shop" && t.Bucket == "bucket_1");

        // Assert: 4 ошибки разбора — ключи названы, тикет не создан
        result.Errors.Should().HaveCount(4);
        result.Errors.Select(e => e.Key).Should().BeEquivalentTo(
        [
            "/pgworker/moves/demo/bucket_11", // неизвестный op "dance"
            "/pgworker/moves/demo/bucket_13", // битый JSON
            "/pgworker/moves/demo/bucket_15", // нет поля op
            "/pgworker/moves/broken",         // не /pgworker/moves/<C>/<bucket> (4 сегмента)
        ]);
        result.Errors.Should().OnlyContain(e => e.Reason.Length > 0);
    }

    [Fact]
    public void Parse_Empty_NoTicketsNoErrors()
    {
        // Arrange / Act / Assert
        MovesQueueParser.Parse([]).Tickets.Should().BeEmpty();
        MovesQueueParser.Parse([]).Errors.Should().BeEmpty();
    }
}
```

- [ ] **Step 2.3: Реализация `src/AdminPanel.Etcd/Parsing/MovesQueueParser.cs`**

```csharp
using System.Text.Json;
using AdminPanel.Core;
using AdminPanel.Etcd.Client;

namespace AdminPanel.Etcd.Parsing;

// Результат разбора очереди заявок /pgworker/moves/ (arch/02 §2.3.1).
public sealed record MovesParseResult(
    IReadOnlyList<MoveTicket> Tickets,
    IReadOnlyList<KeyParseError> Errors);

// Чистая функция: KV префикса /pgworker/moves/<C>/<bucket> → заявки. Битый JSON,
// неизвестный/отсутствующий op, неканонический ключ — KeyParseError (тик не роняют;
// ключ не трогаем — его отвергнет и удалит процесс PgWorker, arch/02 §7).
public static class MovesQueueParser
{
    public const string Prefix = "/pgworker/moves/";

    public static MovesParseResult Parse(IReadOnlyList<Kv> kvs)
    {
        var tickets = new List<MoveTicket>();
        var errors = new List<KeyParseError>();
        foreach (var kv in kvs)
        {
            // "/pgworker/moves/<C>/<bucket>" → ["", "pgworker", "moves", <C>, <bucket>]
            var segments = kv.Key.Split('/');
            if (segments.Length != 5 || segments[3].Length == 0 || segments[4].Length == 0)
            {
                errors.Add(new(kv.Key, "ожидается /pgworker/moves/<cluster>/<bucket>"));
                continue;
            }

            var (cluster, leaf) = (segments[3], segments[4]);
            var bucketId = leaf.StartsWith("bucket_", StringComparison.Ordinal)
                           && int.TryParse(leaf["bucket_".Length..], out var id)
                ? id
                : (int?)null;
            try
            {
                using var doc = JsonDocument.Parse(kv.Value);
                var root = doc.RootElement;
                if (root.ValueKind != JsonValueKind.Object
                    || !root.TryGetProperty("op", out var op)
                    || op.ValueKind != JsonValueKind.String)
                {
                    errors.Add(new(kv.Key, "нет поля op"));
                    continue;
                }

                var opName = op.GetString()!;
                if (opName is not ("move" or "rollback" or "finalize" or "abort"))
                {
                    errors.Add(new(kv.Key, $"неизвестный op: '{opName}'"));
                    continue;
                }

                tickets.Add(new MoveTicket(
                    cluster, leaf, bucketId, opName,
                    GetString(root, "to"),
                    root.TryGetProperty("requested_unix", out var unix)
                        && unix.ValueKind == JsonValueKind.Number
                        ? unix.GetInt64()
                        : 0,
                    GetString(root, "requested_by")));
            }
            catch (JsonException e)
            {
                errors.Add(new(kv.Key, $"битый JSON: {e.Message}"));
            }
        }

        return new(tickets, errors);
    }

    private static string? GetString(JsonElement root, string name)
        => root.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String
            ? value.GetString()
            : null;
}
```

- [ ] **Step 2.4: Проверка**

`cd src && dotnet test AdminPanel.slnx` → `MovesQueueParserTests` зелёные, прочее не сломано.

---

### Task 3: Чтение `/pgworker/moves/` в тике `SnapshotRefresher` (TDD)

**Вход:** Task 1–2; текущий тик читает 4 KV-префикса (последний — portalloc).

**Действие:** расширить `FakeEtcdGateway` (юнит-харнесс) данными/точечным отказом по префиксу → падающие тесты → range в тике + `SnapshotBuilder.Build` с заявками.

**Выход:** снапшот содержит `MoveTickets` (и `ParseErrors` от битых заявок); транспортный провал нового range роняет тик.

**Проверка:** новые тесты красные до, зелёные после; вся сборка зелёная.

**Связь со spec:** §4.2 (refresher/builder), Д10–Д11; arch/02 §4 п.2, §7.

**Files:**
- Modify: `src/tests/AdminPanel.UnitTests/SnapshotRefresherTests.cs` (FakeEtcdGateway + тесты)
- Modify: `src/AdminPanel.Etcd/SnapshotRefresher.cs`
- Modify: `src/AdminPanel.Etcd/SnapshotBuilder.cs`
- Modify (если компилятор укажет): `src/tests/AdminPanel.UnitTests/SnapshotBuilderTests.cs`

**Interfaces (produces):** `SnapshotBuilder.Build(TimeProvider, ClustersParseResult, ServiceParseResult, IReadOnlyList<StandNode>, MovesParseResult, IReadOnlyList<EtcdMember>, IReadOnlyList<EtcdAlarm>, EtcdStatus)` — новый позиционный параметр после `standNodes`. `FakeEtcdGateway` получает `IReadOnlyList<Kv> MovesKv { get; init; } = []` и `List<string> RangeFailPrefixes { get; } = []`.

- [ ] **Step 3.1: Расширить `FakeEtcdGateway`** (в `SnapshotRefresherTests.cs`)

Добавить свойства:

```csharp
    public IReadOnlyList<Kv> MovesKv { get; set; } = [];

    public List<string> RangeFailPrefixes { get; } = [];
```

`RangeAsync` — точечный отказ по префиксу и ветка данных (итоговый вид метода):

```csharp
    public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
    {
        RangeCalls++;
        return Task.FromResult(RangeFailEndpoints.Contains(endpoint) || RangeFailPrefixes.Contains(prefix)
            ? Result<IReadOnlyList<Kv>>.Failed(new EtcdUnreachableException(endpoint))
            : Result<IReadOnlyList<Kv>>.Success(prefix switch
            {
                "/clusters/" => ClustersKv,
                "/service/" => ServiceKv,
                "/pgworker/moves/" => MovesKv,
                _ => NodesKv,
            }));
    }
```

- [ ] **Step 3.2: Тесты (в класс `SnapshotRefresherTests`)**

```csharp
    [Fact]
    public async Task Refresh_WithMovesQueue_StoresTickets()
    {
        // Arrange: тик с непустой очередью заявок (арх/02 §2.3.1)
        var gateway = DemoGateway();
        gateway.MovesKv = EtcdFixtures.LoadKv("moves-queue.json");
        var store = new SnapshotStore();

        // Act
        await RefresherTestHarness.New(gateway, store, "http://etcd1:2379")
            .RefreshOnceAsync(CancellationToken.None);

        // Assert: валидные заявки — в MoveTickets, битые — в ParseErrors (Д11)
        var snapshot = store.Current!;
        snapshot.MoveTickets.Should().HaveCount(6);
        snapshot.MoveTickets.Should().Contain(t =>
            t.Cluster == "demo" && t.Bucket == "bucket_3" && t.Op == "move" && t.To == "shard2");
        snapshot.ParseErrors.Should().Contain(e => e.Key == "/pgworker/moves/demo/bucket_13");
    }

    [Fact]
    public async Task Refresh_MovesRangeFails_FailsTickKeepsPrevious()
    {
        // Arrange: точечный отказ чтения очереди — неполный снапшот хуже прежнего (Д10)
        var gateway = DemoGateway();
        var store = new SnapshotStore();
        var refresher = RefresherTestHarness.New(gateway, store, "http://etcd1:2379");
        await refresher.RefreshOnceAsync(CancellationToken.None); // успешный тик ДО поломки
        var before = store.Current;
        gateway.RangeFailPrefixes.Add("/pgworker/moves/");        // ломаем только новый range

        // Act
        var result = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert: отказ тика; прежний снапшот (и его MoveTickets) на месте
        result.IsSuccess.Should().BeFalse();
        store.Current.Should().BeSameAs(before);
    }
```

(Если `DemoGateway()` уже используется с `FixedTimeProvider` — время фиксировано, оба тика идентичны по `BuiltAtUtc` — это не важно для `BeSameAs`.)

Запустить: `cd src && dotnet test AdminPanel.slnx --filter "FullyQualifiedName~SnapshotRefresherTests"` → новые тесты FAIL (MoveKv не читается: первый — `MoveTickets` пуст; второй — тик успешен).

- [ ] **Step 3.3: Реализация в `SnapshotRefresher.cs`**

1. В `RefreshOnceAsync` рядом с `portAllocTask` добавить:

```csharp
        var movesTask = WithFailoverAsync(alive, active, (ep, t) => gateway.RangeAsync(ep, Prefixes.Moves, t), ct);
```

и после `var portAllocKv = await portAllocTask;` — `var movesKv = await movesTask;`.

2. В условие частичного провала добавить `movesKv`:

```csharp
        if (!clustersKv.IsSuccess || !serviceKv.IsSuccess || !nodesKv.IsSuccess
            || !portAllocKv.IsSuccess || !movesKv.IsSuccess)
            return FailTick(previous, statuses, now, "KV-чтения etcd не удались");
```

3. Разбор и сборка:

```csharp
        var movesParsed = MovesQueueParser.Parse(movesKv.Value);
```

вызов `SnapshotBuilder.Build(...)` — вставить `movesParsed` после `nodes`:

```csharp
            SnapshotBuilder.Build(
                time, clustersParsed, serviceParsed, nodes, movesParsed,
                etcd.Members, etcd.Alarms, etcd),
```

4. В `Prefixes` добавить: `public const string Moves = "/pgworker/moves/";`

- [ ] **Step 3.4: `SnapshotBuilder.cs` — параметр вместо временного `[]`**

```csharp
    public static EtcdSnapshot Build(
        TimeProvider time,
        ClustersParseResult clusters,
        ServiceParseResult service,
        IReadOnlyList<StandNode> standNodes,
        MovesParseResult moves,
        IReadOnlyList<EtcdMember> members,
        IReadOnlyList<EtcdAlarm> alarms,
        EtcdStatus etcd)
        => new(
            time.GetUtcNow(),
            etcd,
            clusters.Clusters,
            service.Scopes,
            standNodes,
            moves.Tickets,
            [],
            [],
            [.. clusters.Errors, .. service.Errors, .. moves.Errors],
            clusters.UnknownKeyCount + service.UnknownKeyCount);
```

(`using AdminPanel.Etcd.Parsing;` уже есть.) Если `SnapshotBuilderTests` вызывает `Build` напрямую — добавить `MovesQueueParser.Parse([])` на новую позицию.

- [ ] **Step 3.5: Проверка**

`cd src && dotnet build AdminPanel.slnx && dotnet test AdminPanel.slnx` → зелёные, включая новые.

---

### Task 4: `MoveBucketsCommand` — валидатор + handler (TDD, юнит)

**Вход:** Task 1–3; образец `AddShardCommand`/`DeleteShardCommand` (guard'ы по снапшоту, `ReadKeyAsync`, `ClusterNotActiveException` и др. — реюз).

**Действие:** падающие юнит-тесты handler'а (FakeGateway — скопировать образец из `AddShardCommandHandlerTests`) → реализация команды/валидатора/исключений/handler'а.

**Выход:** `MoveBucketsCommand`/`MoveBucketsCommandHandler` — полная мутация §9.7 без HTTP-слоя.

**Проверка:** `MoveBucketsCommandHandlerTests` зелёные (матрица 400/404/409/503/201 из spec §7.3); сборка чистая.

**Связь со spec:** §3 Д2–Д9, §4.3 (полный порядок handler'а); arch/02 §9.7.

**Files:**
- Create: `src/AdminPanel.Api/Operations/MoveBucketsCommand.cs`
- Create: `src/tests/AdminPanel.UnitTests/MoveBucketsCommandHandlerTests.cs`

**Interfaces (produces):**

```csharp
// namespace AdminPanel.Api.Operations
public sealed record MoveBucketsRequest(string From, string To, IReadOnlyList<int>? Buckets);
public sealed record MovesQueuedDto(string Cluster, string From, string To,
    IReadOnlyList<int> Queued, IReadOnlyList<int> Skipped);
public sealed record MoveBucketsCommand(string Cluster, string From, string To,
    IReadOnlyList<int> Buckets, string RequestedBy) : ICommand<MovesQueuedDto>;
public sealed class MoveBucketsValidationException(IReadOnlyList<ValidationError> errors);
public sealed class MoveTargetRemovingException(string cluster, string shard);
public sealed class BucketNotOnSourceException(int bucket, string? owner, string state);
public sealed class MoveRequestConflictException(string bucket, string op, string? to);
public sealed class MoveClaimLostException(int bucket);
public sealed class MoveBucketsCommandHandler(ISnapshotStore, IEtcdGateway, TimeProvider)
    : ICommandHandler<MoveBucketsCommand, MovesQueuedDto>;
```

Реюз существующих: `ClusterNotFoundException`, `ShardNotFoundException`, `ShardPrecheckUnavailableException`, `ClusterNotActiveException`, `NonShardedClusterException`, `EtcdWriteUnavailableException`, `InvalidClusterConfigException`, `ValidationError`, `CreateClusterLimits.NamePattern()`.

РП-1: `Buckets` в request — nullable: JSON `null`/отсутствие поля ловит валидатор (400), а не NRE.
РП-2: бакет вне `0..N-1` → `BucketNotOnSourceException(id, null, "OUT_OF_RANGE")` (единый 409-guard бакета).

- [ ] **Step 4.1: Тест-файл `MoveBucketsCommandHandlerTests.cs`**

```csharp
using System.Text.Json;
using AdminPanel.Api.Operations;
using AdminPanel.Core;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Client;
using AdminPanel.Infrastructure;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Хендлер постановки заявок на переезды: валидация → config → guard'ы по снапшоту →
// очередь напрямую → txn-клэйм per key; сбой без компенсации (arch/02 §9.7, spec §4.3).
public class MoveBucketsCommandHandlerTests
{
    private const string Endpoint = "http://etcd:2379";
    private static readonly DateTimeOffset Now = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    // FakeGateway — копия образца AddShardCommandHandlerTests (пул kv + счётчики txn).
    private sealed class FakeGateway : IEtcdGateway
    {
        public List<Kv> All = [];
        public readonly List<(IReadOnlyList<TxnCompare> Compares, IReadOnlyList<KvPut> Puts)> Txns = [];
        public bool SucceedTxn = true;
        public Func<string, bool>? FailRangeByPrefix;
        public Func<string, bool>? FailTxnWhen;

        public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
        {
            if (FailRangeByPrefix?.Invoke(prefix) == true)
                return Task.FromResult(Result<IReadOnlyList<Kv>>.Failed(
                    new InvalidOperationException($"range failed: {prefix}")));
            return Task.FromResult(Result<IReadOnlyList<Kv>>.Success(
                [.. All.Where(kv => kv.Key.StartsWith(prefix, StringComparison.Ordinal))]));
        }

        public Task<Result<EtcdStatusPayload>> StatusAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<EtcdStatusPayload>.Success(new(null, null, null, null, null)));

        public Task<Result<IReadOnlyList<EtcdMember>>> MemberListAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<EtcdMember>>.Success([]));

        public Task<Result<IReadOnlyList<EtcdAlarm>> AlarmAsync(string endpoint, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<EtcdAlarm>>.Success([]));

        public Task<Result<TxnResult>> TxnAsync(
            string endpoint, IReadOnlyList<TxnCompare> compares, IReadOnlyList<KvPut> puts, CancellationToken ct)
        {
            Txns.Add((compares, puts));
            return Task.FromResult(FailTxnWhen?.Invoke(puts[0].Key) == true
                ? Result<TxnResult>.Failed(new InvalidOperationException($"txn failed: {puts[0].Key}"))
                : Result<TxnResult>.Success(new(SucceedTxn)));
        }

        public Task<Result> PutAsync(string endpoint, string key, string value, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
            => Task.FromResult(Result.Success());
    }

    // Снапшот: Active-кластер shop, 2 шарда, 6 бакетов (0,2,4 — shard1; 1,3,5 — shard2).
    private static ClusterInfo ShopCluster() => new(
        "shop", "shop", 6, 1755900000, ClusterState.Active,
        [
            new ShardInfo("shard1", "", [], null, null, null, 2, null, [], null),
            new ShardInfo("shard2", "", [], null, null, null, 2, null, [], null),
        ],
        [.. Enumerable.Range(0, 6).Select(i => new BucketInfo(i, i % 2 == 0 ? "shard1" : "shard2", BucketState.Active, null))],
        []);

    private static (MoveBucketsCommandHandler Handler, FakeGateway Gateway) NewHandler(ClusterInfo? cluster = null)
    {
        var gateway = new FakeGateway();
        var store = new SnapshotStore();
        var time = new FixedTimeProvider { Utc = Now };
        store.Replace(TestSnapshots.Healthy(Now) with
        {
            Etcd = new EtcdStatus(
                true, [new EtcdEndpoint(Endpoint, true, 1, "3.5.21", null, null, null, null, [])],
                [], [], Endpoint, false, Now, 0),
            Clusters = [cluster ?? ShopCluster()],
        });
        return (new MoveBucketsCommandHandler(store, gateway, time), gateway);
    }

    // etcd-сид: config Active-кластера shop.
    private static void Seed(FakeGateway gateway)
    {
        gateway.All =
        [
            new Kv("/clusters/shop/config", """{"buckets":6,"dbname":"shop","created_unix":1755900000}""", 1),
        ];
    }

    private static MoveBucketsCommand Command(string from = "shard1", string to = "shard2",
        int[]? buckets = null, string by = "admin") =>
        new("shop", from, to, buckets ?? [0, 2, 4], by);
```

Далее — сами тесты (добавить в тот же класс):

```csharp
    [Fact]
    public async Task Handle_EmptyOrDuplicateBuckets_Returns400()
    {
        // Arrange / Act
        var (handler, gateway) = NewHandler();
        Seed(gateway);
        var empty = await handler.Handle(Command(buckets: []), CancellationToken.None);
        var dup = await handler.Handle(Command(buckets: [0, 0]), CancellationToken.None);

        // Assert: errors по полю buckets; в etcd не ходили
        empty.Error.Should().BeOfType<MoveBucketsValidationException>();
        dup.Error.Should().BeOfType<MoveBucketsValidationException>();
        gateway.Txns.Should().BeEmpty();
    }

    [Fact]
    public async Task Handle_FromEqualsTo_Returns400()
    {
        // Arrange / Act
        var (handler, gateway) = NewHandler();
        Seed(gateway);
        var result = await handler.Handle(Command(from: "shard1", to: "shard1"), CancellationToken.None);

        // Assert
        var error = result.Error.Should().BeOfType<MoveBucketsValidationException>().Subject;
        error.Errors.Should().Contain(e => e.Field == "to");
    }

    [Fact]
    public async Task Handle_NoConfig_Returns404()
    {
        // Arrange: config-ключа нет
        var (handler, _) = NewHandler();
        // Act
        var result = await handler.Handle(Command(), CancellationToken.None);
        // Assert
        result.Error.Should().BeOfType<ClusterNotFoundException>();
    }

    [Fact]
    public async Task Handle_NotActiveCluster_Returns409()
    {
        // Arrange
        var (handler, gateway) = NewHandler();
        gateway.All = [new Kv("/clusters/shop/config",
            """{"buckets":6,"dbname":"shop","state":"TO_REMOVE"}""", 1)];
        // Act
        var result = await handler.Handle(Command(), CancellationToken.None);
        // Assert
        result.Error.Should().BeOfType<ClusterNotActiveException>();
    }

    [Fact]
    public async Task Handle_NonSharded_Returns409()
    {
        // Arrange: 1 бакет и единственный шард (arch/03 §2)
        var (handler, gateway) = NewHandler(new ClusterInfo("shop", "shop", 1, null, ClusterState.Active,
            [new ShardInfo("shard1", "", [], null, null, null, 2, null, [], null)],
            [new BucketInfo(0, "shard1", BucketState.Active, null)], []));
        Seed(gateway);
        // Act
        var result = await handler.Handle(Command(), CancellationToken.None);
        // Assert
        result.Error.Should().BeOfType<NonShardedClusterException>();
    }

    [Fact]
    public async Task Handle_UnknownShard_Returns404()
    {
        // Arrange / Act
        var (handler, gateway) = NewHandler();
        Seed(gateway);
        var result = await handler.Handle(Command(to: "shard9"), CancellationToken.None);
        // Assert
        result.Error.Should().BeOfType<ShardNotFoundException>();
    }

    [Fact]
    public async Task Handle_TargetToRemove_Returns409()
    {
        // Arrange: приёмник в демонтаже (Д9)
        var (handler, gateway) = NewHandler(ShopCluster() with
        {
            Shards =
            [
                new ShardInfo("shard1", "", [], null, null, null, 2, null, [], null),
                new ShardInfo("shard2", "", [], null, null, null, 2, null, [], null, ShardState.ToRemove),
            ],
        });
        Seed(gateway);
        // Act
        var result = await handler.Handle(Command(), CancellationToken.None);
        // Assert
        result.Error.Should().BeOfType<MoveTargetRemovingException>();
        gateway.Txns.Should().BeEmpty();
    }

    [Fact]
    public async Task Handle_BucketNotOnSource_Returns409()
    {
        // Arrange: бакет 1 принадлежит shard2, везём «с shard1»
        var (handler, gateway) = NewHandler();
        Seed(gateway);
        // Act
        var result = await handler.Handle(Command(buckets: [0, 1]), CancellationToken.None);
        // Assert: сообщение называет фактического владельца
        result.Error.Should().BeOfType<BucketNotOnSourceException>()
            .Which.Message.Should().Contain("shard2");
    }

    [Fact]
    public async Task Handle_SyncingBucket_Returns409()
    {
        // Arrange: бакет 2 в незавершённом переезде (статус-ключ)
        var cluster = ShopCluster() with
        {
            Buckets =
            [
                .. Enumerable.Range(0, 6).Select(i => new BucketInfo(
                    i, i % 2 == 0 ? "shard1" : "shard2",
                    i == 2 ? BucketState.Syncing : BucketState.Active,
                    i == 2 ? new MoveInfo("shard1", "shard2", 1, 2, "copy", null) : null)),
            ],
        };
        var (handler, gateway) = NewHandler(cluster);
        Seed(gateway);
        // Act
        var result = await handler.Handle(Command(), CancellationToken.None);
        // Assert
        result.Error.Should().BeOfType<BucketNotOnSourceException>()
            .Which.Message.Should().Contain("SYNCING");
    }

    [Fact]
    public async Task Handle_ConflictingTicket_Returns409BeforeWrites()
    {
        // Arrange: на bucket_0 уже стоит ИНАЯ заявка (op=move, to=shard3) (Д7)
        var (handler, gateway) = NewHandler();
        gateway.All =
        [
            new Kv("/clusters/shop/config", """{"buckets":6,"dbname":"shop"}""", 1),
            new Kv("/pgworker/moves/shop/bucket_0",
                """{"op":"move","to":"shard3","requested_unix":10,"requested_by":"etcdctl"}""", 2),
        ];
        // Act
        var result = await handler.Handle(Command(), CancellationToken.None);
        // Assert: отказ до любых txn; бакет назван
        result.Error.Should().BeOfType<MoveRequestConflictException>()
            .Which.Message.Should().Contain("bucket_0");
        gateway.Txns.Should().BeEmpty();
    }

    [Fact]
    public async Task Handle_IdenticalTicket_GoesToSkippedWithoutTxn()
    {
        // Arrange: на bucket_0 уже стоит ТА ЖЕ заявка move→shard2 (Д6)
        var (handler, gateway) = NewHandler();
        gateway.All =
        [
            new Kv("/clusters/shop/config", """{"buckets":6,"dbname":"shop"}""", 1),
            new Kv("/pgworker/moves/shop/bucket_0",
                """{"op":"move","to":"shard2","requested_unix":1755850000,"requested_by":"ops"}""", 2),
        ];
        // Act
        var result = await handler.Handle(Command(), CancellationToken.None);
        // Assert: bucket_0 skipped, остальные 2 — txn с base = maxUnix+1 > now (Д2)
        result.IsSuccess.Should().BeTrue();
        result.Value.Skipped.Should().BeEquivalentTo([0]);
        result.Value.Queued.Should().BeEquivalentTo([2, 4]);
        var unixes = gateway.Txns.Select(t => ParseUnix(t.Puts[0].Value)).ToList();
        unixes.Should().BeInAscendingOrder().And.OnlyContain(u => u >= 1755850001L);
    }

    [Fact]
    public async Task Handle_Success_QueuesAscendingUnixWithCanonicalBody()
    {
        // Arrange: очередь пуста → base = now (FixedTimeProvider)
        var (handler, gateway) = NewHandler();
        Seed(gateway);
        // Act
        var result = await handler.Handle(Command(by: "ops"), CancellationToken.None);
        // Assert: по возрастанию id, requested_unix = base+0/+1/+2 (Д2/Д3)
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be(new MovesQueuedDto("shop", "shard1", "shard2", [0, 2, 4], []));
        gateway.Txns.Select(t => t.Puts[0].Key).Should().BeEquivalentTo(
        [
            "/pgworker/moves/shop/bucket_0",
            "/pgworker/moves/shop/bucket_2",
            "/pgworker/moves/shop/bucket_4",
        ]);
        gateway.Txns.Select(t => t.Puts[0].Value).Should().BeEquivalentTo(
        [
            """{"op":"move","to":"shard2","requested_unix":""" + Now.ToUnixTimeSeconds() + ""","requested_by":"ops"}""",
            """{"op":"move","to":"shard2","requested_unix":""" + (Now.ToUnixTimeSeconds() + 1) + ""","requested_by":"ops"}""",
            """{"op":"move","to":"shard2","requested_unix":""" + (Now.ToUnixTimeSeconds() + 2) + ""","requested_by":"ops"}""",
        ]);
        gateway.Txns.Should().OnlyContain(t => t.Compares.Count == 1
            && t.Compares[0].Key.StartsWith("/pgworker/moves/shop/bucket_") && t.Compares[0].Version == 0);
    }

    [Fact]
    public async Task Handle_OrderedByAscendingIdRegardlessOfRequestBody()
    {
        // Arrange: массив в обратном порядке — обработка всё равно по id (Д3)
        var (handler, gateway) = NewHandler();
        Seed(gateway);
        // Act
        await handler.Handle(Command(buckets: [4, 2, 0]), CancellationToken.None);
        // Assert
        gateway.Txns.Select(t => ParseBucket(t.Puts[0].Key)).Should().BeInAscendingOrder();
    }

    [Fact]
    public async Task Handle_ClaimLost_Returns409()
    {
        // Arrange: txn-compare не сошёлся — конкурентная заявка (Д4)
        var (handler, gateway) = NewHandler();
        Seed(gateway);
        gateway.SucceedTxn = false;
        // Act
        var result = await handler.Handle(Command(), CancellationToken.None);
        // Assert
        result.Error.Should().BeOfType<MoveClaimLostException>();
    }

    [Fact]
    public async Task Handle_TxnEtcdFailsMiddle_NoCompensation()
    {
        // Arrange: etcd-сбой на 2-й заявке → 503, поставленные НЕ откатываем (Д5)
        var (handler, gateway) = NewHandler();
        Seed(gateway);
        gateway.FailTxnWhen = key => key.EndsWith("bucket_2", StringComparison.Ordinal);
        // Act
        var result = await handler.Handle(Command(), CancellationToken.None);
        // Assert: ошибка наверх; первая заявка (bucket_0) осталась поставленной
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<InvalidOperationException>();
        gateway.Txns.Should().HaveCount(2);
    }

    [Fact]
    public async Task Handle_ConfigReadFails_ReturnsEtcdError()
    {
        // Arrange: чтение config по prefix-ключу не удалось → 503-путь
        var (handler, gateway) = NewHandler();
        gateway.FailRangeByPrefix = p => p == "/clusters/shop/config";
        // Act
        var result = await handler.Handle(Command(), CancellationToken.None);
        // Assert
        result.Error.Should().BeOfType<InvalidOperationException>();
    }

    // requested_unix из JSON-тела заявки.
    private static long ParseUnix(string body)
    {
        using var doc = JsonDocument.Parse(body);
        return doc.RootElement.GetProperty("requested_unix").GetInt64();
    }

    private static int ParseBucket(string key) => int.Parse(key.Split('/')[^1]["bucket_".Length..]);
}
```

Запустить: `cd src && dotnet test AdminPanel.slnx --filter "FullyQualifiedName~MoveBucketsCommandHandlerTests"` → ошибка компиляции (типы не существуют) — это «красный».

- [ ] **Step 4.2: Реализация `src/AdminPanel.Api/Operations/MoveBucketsCommand.cs`**

```csharp
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using AdminPanel.Api.Inspection; // BucketStates (канон имён состояний бакета)
using AdminPanel.Core;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Client;
using AdminPanel.Etcd.Parsing;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Operations;

// Тело POST /api/clusters/{cluster}/moves (arch/03 §1.5). Buckets nullable:
// null/отсутствие поля ловит валидатор (400), а не NRE (решение при планировании).
public sealed record MoveBucketsRequest(string From, string To, IReadOnlyList<int>? Buckets);

// Ответ 201: queued поставлены сейчас, skipped — идентичные уже стояли (arch/03 §1.5).
public sealed record MovesQueuedDto(
    string Cluster, string From, string To,
    IReadOnlyList<int> Queued, IReadOnlyList<int> Skipped);

// Пятая мутация панели — заявки на переезды бакетов (arch/02 §9.7).
public sealed record MoveBucketsCommand(
    string Cluster, string From, string To, IReadOnlyList<int> Buckets, string RequestedBy)
    : ICommand<MovesQueuedDto>;

public sealed class MoveBucketsValidationException(IReadOnlyList<ValidationError> errors)
    : Exception("параметры переноса бакетов некорректны")
{
    public IReadOnlyList<ValidationError> Errors { get; } = errors;
}

// Приёмник в демонтаже: на удаляемый шард везть нельзя (arch/02 §9.7 п.2; источник
// TO_REMOVE допустим — эвакуация перед демонтажем, spec Д9).
public sealed class MoveTargetRemovingException(string cluster, string shard)
    : Exception($"шард-приёмник {cluster}/{shard} удаляется (TO_REMOVE) — выберите другой приёмник");

// Бакет не годен для переезда с источника: не его владелец / не ACTIVE / вне диапазона.
public sealed class BucketNotOnSourceException(int bucket, string? owner, string state)
    : Exception($"бакет {bucket} не доступен для переезда (владелец: {owner ?? "—"}, состояние: {state})");

// На бакете уже стоит иная заявка — панель чужие не перезаписывает (arch/02 §9.7 п.3).
public sealed class MoveRequestConflictException(string bucket, string op, string? to)
    : Exception($"на {bucket} уже стоит заявка (op={op}, to={to ?? "—"}) — дождитесь её обработки или уберите ключ");

// Txn-клэйм не сошёлся: конкурентная заявка заняла ключ между чтением и записью.
public sealed class MoveClaimLostException(int bucket)
    : Exception($"конкурентная заявка заняла bucket_{bucket} между чтением и записью — повторите запрос");

// Валидация тела (arch/02 §9.7 п.2): 400 с errors по полям.
public static class MoveBucketsValidator
{
    public static IReadOnlyList<ValidationError> Validate(MoveBucketsRequest request)
    {
        var errors = new List<ValidationError>();
        if (string.IsNullOrWhiteSpace(request.From))
            errors.Add(new("from", "шард-источник обязателен"));
        if (string.IsNullOrWhiteSpace(request.To))
            errors.Add(new("to", "шард-приёмник обязателен"));
        if (request.From == request.To && request.From.Length > 0)
            errors.Add(new("to", "приёмник должен отличаться от источника"));
        if (request.Buckets is null || request.Buckets.Count == 0)
            errors.Add(new("buckets", "выберите хотя бы один бакет"));
        else if (request.Buckets.Distinct().Count() != request.Buckets.Count)
            errors.Add(new("buckets", "дубликаты бакетов не допускаются"));
        return errors;
    }
}

// Guard'ы по снапшоту + очередь напрямую + txn-клэйм per key (arch/02 §9.7;
// spec §4.3). Сбой посередине — БЕЗ компенсации: частичная очередь валидна,
// повтор досдаст остаток (spec Д5). Без ретраев: повтор = новый POST.
[InjectAsScoped]
public sealed partial class MoveBucketsCommandHandler(
    ISnapshotStore store, IEtcdGateway gateway, TimeProvider time)
    : ICommandHandler<MoveBucketsCommand, MovesQueuedDto>
{
    // Паттерн имени шарда (как DeleteShardCommand: без дефиса).
    [GeneratedRegex("^[a-z][a-z0-9_]{0,30}$")]
    private static partial Regex ShardNamePattern();

    // Канон тела заявки PgWorker: только нужные поля, snake_case (spec §4.3 шаг 6).
    private static readonly JsonSerializerOptions TicketJson = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    private sealed record TicketBody(
        [property: JsonPropertyName("op")] string Op,
        [property: JsonPropertyName("to")] string To,
        [property: JsonPropertyName("requested_unix")] long RequestedUnix,
        [property: JsonPropertyName("requested_by")] string RequestedBy);

    public async ValueTask<Result<MovesQueuedDto>> Handle(MoveBucketsCommand command, CancellationToken ct)
    {
        var (cluster, from, to) = (command.Cluster, command.From, command.To);

        // 1) Валидация тела (400) и каноничность имён (404 — панель такие не создавала).
        var errors = MoveBucketsValidator.Validate(new MoveBucketsRequest(from, to, command.Buckets));
        if (errors.Count > 0)
            return Result<MovesQueuedDto>.Failed(new MoveBucketsValidationException(errors));
        if (!CreateClusterLimits.NamePattern().IsMatch(cluster)
            || !ShardNamePattern().IsMatch(from) || !ShardNamePattern().IsMatch(to))
            return Result<MovesQueuedDto>.Failed(new ClusterNotFoundException(cluster));

        // 2) Активный endpoint из снапшота.
        var snapshot = store.Current;
        if (snapshot?.Etcd.ActiveEndpoint is not { } endpoint)
            return Result<MovesQueuedDto>.Failed(new EtcdWriteUnavailableException());

        // 3) Config напрямую: сбой → 503; нет → 404; state не null → 409; битый → 503.
        var config = await ReadKeyAsync(endpoint, $"/clusters/{cluster}/config", ct);
        if (!config.IsSuccess)
            return Result<MovesQueuedDto>.Failed(config.Error!);
        if (config.Value is null)
            return Result<MovesQueuedDto>.Failed(new ClusterNotFoundException(cluster));
        string? state;
        try
        {
            using var doc = JsonDocument.Parse(config.Value);
            state = doc.RootElement.TryGetProperty("state", out var s) && s.ValueKind == JsonValueKind.String
                ? s.GetString()
                : null;
        }
        catch (JsonException)
        {
            return Result<MovesQueuedDto>.Failed(new InvalidClusterConfigException(cluster));
        }

        if (state is not null)
            return Result<MovesQueuedDto>.Failed(new ClusterNotActiveException(cluster, state));

        // 4) Guard'ы по снапшоту (Д4-паттерн DeleteShard: быстро оператору,
        //    авторитетно перепроверит PgWorker).
        var info = snapshot.Clusters.FirstOrDefault(c => c.Name == cluster);
        if (info is null)
            return Result<MovesQueuedDto>.Failed(new ShardPrecheckUnavailableException());
        if (info.BucketsCount == 1 && info.Shards.Count <= 1)
            return Result<MovesQueuedDto>.Failed(new NonShardedClusterException(cluster));
        if (info.Shards.All(s => s.Name != from))
            return Result<MovesQueuedDto>.Failed(new ShardNotFoundException(cluster, from));
        if (info.Shards.FirstOrDefault(s => s.Name == to) is not { } target)
            return Result<MovesQueuedDto>.Failed(new ShardNotFoundException(cluster, to));
        if (target.State == ShardState.ToRemove)
            return Result<MovesQueuedDto>.Failed(new MoveTargetRemovingException(cluster, to));

        var ordered = command.Buckets.Distinct().OrderBy(id => id).ToList();
        foreach (var id in ordered)
        {
            var bucket = info.Buckets.FirstOrDefault(b => b.Id == id);
            if (id < 0 || id >= info.BucketsCount || bucket is null)
                return Result<MovesQueuedDto>.Failed(new BucketNotOnSourceException(id, null, "OUT_OF_RANGE"));
            if (bucket.Owner != from)
                return Result<MovesQueuedDto>.Failed(
                    new BucketNotOnSourceException(id, bucket.Owner, BucketStates.Name(bucket.State)));
            if (bucket.State != BucketState.Active)
                return Result<MovesQueuedDto>.Failed(
                    new BucketNotOnSourceException(id, bucket.Owner, BucketStates.Name(bucket.State)));
        }

        // 5) Очередь напрямую, один range по всему префиксу (arch/02 §9.7 п.3):
        //    идентичная заявка → skipped; иная → 409 до записей; база — глобальный max.
        var movesRange = await gateway.RangeAsync(endpoint, MovesQueueParser.Prefix, ct);
        if (!movesRange.IsSuccess)
            return Result<MovesQueuedDto>.Failed(movesRange.Error!);
        var parsed = MovesQueueParser.Parse(movesRange.Value);
        var mine = parsed.Tickets
            .Where(t => t.Cluster == cluster)
            .ToDictionary(t => t.Bucket);
        var maxUnix = parsed.Tickets.Count == 0 ? 0 : parsed.Tickets.Max(t => t.RequestedUnix);

        var skipped = new List<int>();
        var toQueue = new List<int>();
        foreach (var id in ordered)
        {
            if (mine.TryGetValue($"bucket_{id}", out var existing))
            {
                if (existing.Op == "move" && existing.To == to)
                    skipped.Add(id);
                else
                    return Result<MovesQueuedDto>.Failed(
                        new MoveRequestConflictException($"bucket_{id}", existing.Op, existing.To));
            }
            else
                toQueue.Add(id);
        }

        // 6) base = max(now, maxUnix+1), заявка k-я по порядку — base+k (arch/02 §9.7 п.4);
        //    txn-клэйм per key: compare version==0 + put (защита от перезаписи чужой).
        var now = time.GetUtcNow().ToUnixTimeSeconds();
        var unixBase = Math.Max(now, maxUnix + 1);
        var queued = new List<int>();
        foreach (var (id, k) in toQueue.Select((b, i) => (b, i)))
        {
            var key = $"/pgworker/moves/{cluster}/bucket_{id}";
            var body = JsonSerializer.Serialize(
                new TicketBody("move", to, unixBase + k, command.RequestedBy), TicketJson);
            var claim = await gateway.TxnAsync(endpoint, [new TxnCompare(key, 0)], [new KvPut(key, body)], ct);
            if (!claim.IsSuccess)
                return Result<MovesQueuedDto>.Failed(claim.Error!); // 503, без компенсации (Д5)
            if (!claim.Value.Succeeded)
                return Result<MovesQueuedDto>.Failed(new MoveClaimLostException(id));
            queued.Add(id);
        }

        return Result<MovesQueuedDto>.Success(new MovesQueuedDto(cluster, from, to, queued, skipped));
    }

    // Точечное чтение ключа через range (образец AddShardCommand):
    // Failed → 503 у эндпоинта; Success(null) — ровно «ключа нет».
    private async Task<Result<string?>> ReadKeyAsync(string endpoint, string key, CancellationToken ct)
    {
        var range = await gateway.RangeAsync(endpoint, key, ct);
        if (!range.IsSuccess)
            return Result<string?>.Failed(range.Error!);
        return Result<string?>.Success(range.Value.FirstOrDefault(kv => kv.Key == key)?.Value);
    }
}
```

- [ ] **Step 4.3: Проверка**

`cd src && dotnet build AdminPanel.slnx && dotnet test AdminPanel.slnx --filter "FullyQualifiedName~MoveBucketsCommandHandlerTests"` → все зелёные; затем полный `dotnet test AdminPanel.slnx` — ничего не сломано.

---

### Task 5: Эндпоинт `POST /api/clusters/{cluster}/moves` (OperationsModule)

**Вход:** Task 4 (команда/исключения); образец маппинга в `OperationsModule.MapOperationsApi`.

**Действие:** добавить маршрут с `ClaimsPrincipal` (username → `RequestedBy`) и маппингом исключений в HTTP-коды.

**Выход:** HTTP-контракт arch/03 §1.5 (коды 201/400/404/409/503).

**Проверка:** сборка чистая; HTTP-поведение закрывается интеграцией Task 7 (юнит-прогона эндпоинта нет — как у прочих мутаций).

**Связь со spec:** §4.3 (OperationsModule), arch/03 §1.5.

**Files:**
- Modify: `src/AdminPanel.Api/Operations/OperationsModule.cs`

РП-3: `Results.Created($"/api/clusters/{cluster}", dto)` — Location ведёт на кластер (ресурсного GET очереди нет).

- [ ] **Step 5.1: Эндпоинт** — в `MapOperationsApi`, после блока DELETE шардов, добавить:

```csharp
        // POST /api/clusters/{cluster}/moves — заявки на переезды бакетов (02 §9.7, 03 §1.5):
        // txn-клэйм per key; сбой посередине без компенсации — повтор досдаст остаток.
        endpoints.MapPost("/api/clusters/{cluster}/moves", async (
            string cluster, MoveBucketsRequest request, ClaimsPrincipal user, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<MoveBucketsCommand, MovesQueuedDto>(
                new MoveBucketsCommand(
                    cluster, request.From, request.To, request.Buckets ?? [],
                    user.Identity?.Name ?? "adminpanel"), ct);
            if (result.IsSuccess)
                return Results.Created($"/api/clusters/{cluster}", result.Value);

            return result.Error switch
            {
                MoveBucketsValidationException validation => Results.Problem(
                    statusCode: StatusCodes.Status400BadRequest,
                    title: "Validation failed",
                    detail: result.Error.Message,
                    extensions: new Dictionary<string, object?>
                    {
                        ["errors"] = validation.Errors.ToDictionary(e => e.Field, e => new[] { e.Message }),
                    }),
                ClusterNotFoundException or ShardNotFoundException => Results.Problem(
                    statusCode: StatusCodes.Status404NotFound, title: "Not found", detail: result.Error.Message),
                ClusterNotActiveException or NonShardedClusterException or MoveTargetRemovingException
                    or BucketNotOnSourceException or MoveRequestConflictException or MoveClaimLostException => Results.Problem(
                        statusCode: StatusCodes.Status409Conflict, title: "Moves rejected", detail: result.Error.Message),
                EtcdWriteUnavailableException or ShardPrecheckUnavailableException => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write unavailable", detail: result.Error.Message),
                _ => Results.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed", detail: result.Error!.Message),
            };
        });
```

В шапку файла добавить `using System.Security.Claims;` (если нет). Обновить комментарий-заголовок класса: «…добавление и демонтаж шарда (arch/03 §1.3/§1.4, t06), заявки на переезды (arch/03 §1.5)».

- [ ] **Step 5.2: Проверка**

`cd src && dotnet build AdminPanel.slnx` → чисто; `cd src && dotnet test AdminPanel.slnx` → зелёные (существующие).

---

### Task 6: `pendingMoves` в деталях кластера (TDD)

**Вход:** Task 1 (`MoveTickets` в снапшоте); `ClusterDetailsQuery`/`ClusterDetailsMapper`.

**Действие:** падающий тест маппера → DTO `MoveTicketDto` + `PendingMoves` в `ClusterDto` + маппинг с сортировкой.

**Выход:** `GET /api/clusters/{c}` отдаёт очередь заявок кластера (spec §7.5).

**Проверка:** новый тест зелёный; сборка/тесты чистые.

**Связь со spec:** §4.4; arch/03 §2 (`ClusterDto.pendingMoves`).

**Files:**
- Modify: `src/AdminPanel.Api/Inspection/ClusterDetailsQuery.cs`
- Modify: `src/tests/AdminPanel.UnitTests/InspectionMappersTests.cs` (добавить тест; при компиляции существующих вызовов `Map` — добавить аргумент)

**Interfaces (produces):**

```csharp
public sealed record MoveTicketDto(int? BucketId, string Bucket, string Op, string? To,
    long RequestedUnix, string? RequestedBy);
// ClusterDto: + IReadOnlyList<MoveTicketDto> PendingMoves (после Buckets, перед Heals)
// ClusterDetailsMapper.Map(+ параметр IReadOnlyList<MoveTicket> moveTickets)
```

- [ ] **Step 6.1: Тест** (в `InspectionMappersTests`; сигнатуру существующих вызовов `Map` временно не менять — тест не скомпилируется, это «красный»):

```csharp
    [Fact]
    public void Map_PendingMoves_FilteredByClusterSortedByUnixThenBucket()
    {
        // Arrange: очередь demo — перемешанные requested_unix и чужой кластер
        var cluster = TestSnapshots.FullCluster();
        var tickets = new[]
        {
            new MoveTicket("demo", "bucket_5", 5, "move", "s2", 300, "ops"),
            new MoveTicket("demo", "bucket_2", 2, "move", "s2", 100, "ops"),
            new MoveTicket("shop", "bucket_1", 1, "move", "shard9", 50, "ops"), // чужой кластер
            new MoveTicket("demo", "bucket_3", 3, "move", "s2", 100, "etcdctl"), // tie unix → по bucket
        };

        // Act
        var dto = ClusterDetailsMapper.Map(cluster, 0, null, null, [], [], tickets);

        // Assert: только demo, по requestedUnix затем bucket (ordinal)
        dto.PendingMoves.Should().BeEquivalentTo(
        [
            new MoveTicketDto(2, "bucket_2", "move", "s2", 100, "ops"),
            new MoveTicketDto(3, "bucket_3", "move", "s2", 100, "etcdctl"),
            new MoveTicketDto(5, "bucket_5", "move", "s2", 300, "ops"),
        ], o => o.WithStrictOrdering());
    }
```

- [ ] **Step 6.2: Реализация в `ClusterDetailsQuery.cs`**

1. `ClusterDto` — добавить поле `IReadOnlyList<MoveTicketDto> PendingMoves` после `Buckets` (перед `Heals`) — позиционно в конструкторе и в вызове маппера.
2. Рядом с прочими DTO:

```csharp
// Строка очереди заявок кластера: /pgworker/moves/<C>/<bucket> (arch/03 §2).
public sealed record MoveTicketDto(
    int? BucketId, string Bucket, string Op, string? To, long RequestedUnix, string? RequestedBy);
```

3. `ClusterDetailsMapper.Map` — добавить последний параметр `IReadOnlyList<MoveTicket> moveTickets`, в конструктор `ClusterDto` на новую позицию:

```csharp
            [.. moveTickets
                .Where(t => t.Cluster == cluster.Name)
                .OrderBy(t => t.RequestedUnix)
                .ThenBy(t => t.Bucket, StringComparer.Ordinal)
                .Select(t => new MoveTicketDto(t.BucketId, t.Bucket, t.Op, t.To, t.RequestedUnix, t.RequestedBy))],
```

4. `ClusterDetailsQueryHandler.Handle`: передать `snapshot.MoveTickets` (последним аргументом `Map`, после `snapshot.HaScopes`).
5. Все прочие вызовы `ClusterDetailsMapper.Map` (юнит-тесты — компилятор перечислит): добавить `[]` или осмысленный список.

- [ ] **Step 6.3: Проверка**

`cd src && dotnet build AdminPanel.slnx && dotnet test AdminPanel.slnx` → зелёные.

---

### Task 7: Интеграционные тесты [Docker] + сиды

**Вход:** Tasks 3–6; `EtcdContainerFixture`/`AuthWebFactory`/`ApiTestLogin`/`EtcdTestHarness`; образец `ShardsApiTests`.

**Действие:** сид заявок в `EtcdSeed` + dev-stand `seed.sh`; интеграционный набор `MovesApiTests` (матрица spec §7.4).

**Выход:** e2e-проверка мутации против реального etcd; dev-стенд показывает очередь «из коробки».

**Проверка:** `cd src && dotnet test AdminPanel.slnx` — интеграционные зелёные (Docker поднят). Прогнать ВЕСЬ набор: сид `/pgworker/moves/demo/bucket_13` мог изменить ожидания тестов, сравнивающих снапшот demo целиком (например `EtcdSnapshotIntegrationTests`) — обновить ожидания (добавить заявку/`MoveTickets`).

**Связь со spec:** §5 фазы D/E, §7.4; arch/02 §8, arch/04 (сид = EtcdSeed).

**Files:**
- Modify: `src/tests/AdminPanel.IntegrationTests/EtcdSeed.cs`
- Modify: `dev-stand/seed.sh`
- Create: `src/tests/AdminPanel.IntegrationTests/MovesApiTests.cs`
- Modify (если сломаются ожидания): `src/tests/AdminPanel.IntegrationTests/EtcdSnapshotIntegrationTests.cs`

- [ ] **Step 7.1: Сид заявки** — в `EtcdSeed.Demo` добавить строку (bucket_13 принадлежит s2 по сиду — заявка «увезти на s1» консистентна):

```csharp
        ("/pgworker/moves/demo/bucket_13", "{\"op\":\"move\",\"to\":\"s1\",\"requested_unix\":1755850000,\"requested_by\":\"ops\"}"),
```

В `dev-stand/seed.sh` после блока routing-ключей добавить:

```bash
put /pgworker/moves/demo/bucket_13 '{"op":"move","to":"s1","requested_unix":1755850000,"requested_by":"ops"}'
```

- [ ] **Step 7.2: `MovesApiTests.cs`** [Docker]

```csharp
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using AdminPanel.Core;
using AdminPanel.Etcd;
using FluentAssertions;
using Xunit;

namespace AdminPanel.IntegrationTests;

// POST /api/clusters/{c}/moves против реального etcd: постановка заявок с возрастающими
// requested_unix, идемпотентность повтора, конфликтная заявка, матрица 400/404/409,
// чтение очереди refresher'ом (arch/02 §9.7, arch/03 §1.5; spec 2026-08-24 §7.4).
[Collection("api")]
public class MovesApiTests(AuthWebFactory factory, EtcdContainerFixture fixture)
    : IClassFixture<EtcdContainerFixture>
{
    private readonly AuthWebFactory _factory = factory;

    // Снапшот «живого etcd» + кластер для пред-проверок (паттерн ShardsApiTests).
    private void SetLiveSnapshot(ClusterInfo? cluster = null)
    {
        var etcd = new EtcdStatus(
            true,
            [new EtcdEndpoint(fixture.Endpoint, true, 1, "3.5.21", null, null, null, null, [])],
            [], [], fixture.Endpoint, false, _factory.Time.GetUtcNow(), 0);
        _factory.Snapshot = InspectionSnapshots.Fixture(_factory.Time.GetUtcNow()) with
        {
            Etcd = etcd,
            Clusters = cluster is null ? [] : [cluster],
        };
    }

    private async Task<HttpClient> LoginAsync() => await ApiTestLogin.LoginAsync(_factory);

    private async Task SeedAsync(params (string Key, string Value)[] kvs)
    {
        foreach (var (key, value) in kvs)
            await EtcdSeed.PutAsync(fixture.Endpoint, key, value, TestContext.Current.CancellationToken);
    }

    // Кластер Active с двумя шардами (shard2 может быть ToRemove — параметр).
    private static ClusterInfo TwoShardCluster(bool targetRemoving = false) => new(
        "shop", "shop", 6, 1755900000, ClusterState.Active,
        [
            new ShardInfo("shard1", "host=shard1a port=5432 dbname=shop user=bucket_admin",
                ["shard1a"], 5432, "shop", "bucket_admin", 2, "shard1a:5432",
                [new NodeInfo("shard1a", "RUNNING")], null),
            new ShardInfo("shard2", "host=shard2a port=5432 dbname=shop user=bucket_admin",
                ["shard2a"], 5432, "shop", "bucket_admin", 2, "shard2a:5432",
                [new NodeInfo("shard2a", "RUNNING")], null,
                targetRemoving ? ShardState.ToRemove : ShardState.Active),
        ],
        [.. Enumerable.Range(0, 6).Select(i => new BucketInfo(i, i % 2 == 0 ? "shard1" : "shard2", BucketState.Active, null))],
        []);

    // etcd-сид Active-кластера: config + 2 шарда + routing (0,2,4 — shard1).
    private async Task SeedShopAsync()
    {
        var kvs = new List<(string, string)>
        {
            ("/clusters/shop/config", """{"buckets":6,"dbname":"shop","created_unix":1755900000}"""),
            ("/clusters/shop/shards/shard1/replicas", "2"),
            ("/clusters/shop/shards/shard1/nodes/shard1a/state", "RUNNING"),
            ("/clusters/shop/shards/shard2/replicas", "2"),
            ("/clusters/shop/shards/shard2/nodes/shard2a/state", "RUNNING"),
        };
        for (var i = 0; i < 6; i++)
            kvs.Add(($"/clusters/shop/buckets/routing/bucket_{i}", i % 2 == 0 ? "shard1" : "shard2"));
        await SeedAsync([.. kvs]);
    }

    private async Task<Dictionary<string, string>> ReadMovesAsync()
    {
        var gateway = EtcdTestHarness.NewGateway();
        var range = await gateway.RangeAsync(fixture.Endpoint, "/pgworker/moves/shop/", TestContext.Current.CancellationToken);
        return range.Value.ToDictionary(kv => kv.Key, kv => kv.Value);
    }

    [Fact]
    public async Task Moves_WithoutCookie_Returns401()
    {
        // Arrange
        SetLiveSnapshot();
        using var client = _factory.CreateClient();

        // Act
        using var response = await client.PostAsJsonAsync("/api/clusters/shop/moves",
            new { from = "shard1", to = "shard2", buckets = new[] { 0 } },
            TestContext.Current.CancellationToken);

        // Assert: default-deny закрывает мутацию как все /api/*
        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task Moves_QueueBuckets_WritesAscendingUnixAndCanonicalBody()
    {
        // Arrange
        SetLiveSnapshot(TwoShardCluster());
        await SeedShopAsync();
        using var client = await LoginAsync();

        // Act: порядок в массиве обратный — обработка всё равно по возрастанию id
        using var response = await client.PostAsJsonAsync("/api/clusters/shop/moves",
            new { from = "shard1", to = "shard2", buckets = new[] { 4, 0, 2 } },
            TestContext.Current.CancellationToken);

        // Assert: 201; в etcd 3 заявки с строго возрастающими requested_unix (Д2/Д3)
        response.StatusCode.Should().Be(HttpStatusCode.Created);
        var dto = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        dto.GetProperty("queued").EnumerateArray().Select(e => e.GetInt32()).Should().BeInAscendingOrder();

        var moves = await ReadMovesAsync();
        moves.Keys.Should().BeEquivalentTo(
        [
            "/pgworker/moves/shop/bucket_0", "/pgworker/moves/shop/bucket_2", "/pgworker/moves/shop/bucket_4",
        ]);
        var unixes = moves.Values
            .Select(v => JsonDocument.Parse(v).RootElement.GetProperty("requested_unix").GetInt64())
            .ToList();
        unixes.Should().OnlyHaveUniqueItems().And.BeInAscendingOrder();
        moves["/pgworker/moves/shop/bucket_0"].Should().Contain("\"op\":\"move\"")
            .And.Contain("\"to\":\"shard2\"").And.Contain("\"requested_by\":\"admin\"");
    }

    [Fact]
    public async Task Moves_Repeat_IdempotentAllSkippedWithoutRewrite()
    {
        // Arrange: первый POST ставит заявки
        SetLiveSnapshot(TwoShardCluster());
        await SeedShopAsync();
        using var client = await LoginAsync();
        using var first = await client.PostAsJsonAsync("/api/clusters/shop/moves",
            new { from = "shard1", to = "shard2", buckets = new[] { 0, 2 } },
            TestContext.Current.CancellationToken);
        first.StatusCode.Should().Be(HttpStatusCode.Created);
        var before = (await ReadMovesAsync())["/pgworker/moves/shop/bucket_0"];

        // Act: повтор того же тела
        using var second = await client.PostAsJsonAsync("/api/clusters/shop/moves",
            new { from = "shard1", to = "shard2", buckets = new[] { 0, 2 } },
            TestContext.Current.CancellationToken);

        // Assert: 201, всё в skipped; значение ключа НЕ перезаписано (Д6)
        second.StatusCode.Should().Be(HttpStatusCode.Created);
        var dto = await second.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        dto.GetProperty("queued").GetArrayLength().Should().Be(0);
        dto.GetProperty("skipped").EnumerateArray().Select(e => e.GetInt32())
            .Should().BeEquivalentTo([0, 2]);
        (await ReadMovesAsync())["/pgworker/moves/shop/bucket_0"].Should().Be(before);
    }

    [Fact]
    public async Task Moves_ConflictingExistingTicket_Returns409BeforeWrites()
    {
        // Arrange: на bucket_0 стоит иная заявка (to=shard9)
        SetLiveSnapshot(TwoShardCluster());
        await SeedShopAsync();
        await SeedAsync(("/pgworker/moves/shop/bucket_0",
            """{"op":"move","to":"shard9","requested_unix":10,"requested_by":"etcdctl"}"""));
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsJsonAsync("/api/clusters/shop/moves",
            new { from = "shard1", to = "shard2", buckets = new[] { 0, 2 } },
            TestContext.Current.CancellationToken);

        // Assert: 409; НИ одной новой заявки (Д7)
        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
        (await ReadMovesAsync()).Keys.Should().BeEquivalentTo(["/pgworker/moves/shop/bucket_0"]);
    }

    [Fact]
    public async Task Moves_BucketNotOnSource_Returns409()
    {
        // Arrange: бакет 1 принадлежит shard2
        SetLiveSnapshot(TwoShardCluster());
        await SeedShopAsync();
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsJsonAsync("/api/clusters/shop/moves",
            new { from = "shard1", to = "shard2", buckets = new[] { 1 } },
            TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
        (await ReadMovesAsync()).Should().BeEmpty();
    }

    [Fact]
    public async Task Moves_TargetToRemove_Returns409()
    {
        // Arrange: приёмник в демонтаже (Д9)
        SetLiveSnapshot(TwoShardCluster(targetRemoving: true));
        await SeedShopAsync();
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsJsonAsync("/api/clusters/shop/moves",
            new { from = "shard1", to = "shard2", buckets = new[] { 0 } },
            TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }

    [Fact]
    public async Task Moves_EmptyBuckets_Returns400()
    {
        // Arrange
        SetLiveSnapshot(TwoShardCluster());
        await SeedShopAsync();
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsJsonAsync("/api/clusters/shop/moves",
            new { from = "shard1", to = "shard2", buckets = Array.Empty<int>() },
            TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task Moves_FromEqualsTo_Returns400()
    {
        // Arrange
        SetLiveSnapshot(TwoShardCluster());
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsJsonAsync("/api/clusters/shop/moves",
            new { from = "shard1", to = "shard1", buckets = new[] { 0 } },
            TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task Moves_UnknownShard_Returns404()
    {
        // Arrange
        SetLiveSnapshot(TwoShardCluster());
        await SeedShopAsync();
        using var client = await LoginAsync();

        // Act
        using var response = await client.PostAsJsonAsync("/api/clusters/shop/moves",
            new { from = "shard1", to = "shard9", buckets = new[] { 0 } },
            TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Moves_RefresherPicksUpQueueTickets()
    {
        // Arrange: сид demo уже содержит заявку bucket_13 (EtcdSeed)
        var store = new SnapshotStore();
        var refresher = EtcdTestHarness.NewRefresher(store, fixture.Endpoint);

        // Act
        (await refresher.RefreshOnceAsync(CancellationToken.None)).IsSuccess.Should().BeTrue();

        // Assert: заявка в снапшоте (Д10); детали кластера отдают её (spec §7.5)
        var ticket = store.Current!.MoveTickets.Single(t => t.Cluster == "demo");
        ticket.Bucket.Should().Be("bucket_13");
        ticket.Op.Should().Be("move");
        ticket.To.Should().Be("s1");
        ticket.RequestedBy.Should().Be("ops");
    }
}
```

- [ ] **Step 7.3: Полный прогон и починка ожиданий** [Docker]

`cd src && dotnet test AdminPanel.slnx` → всё зелёное. Если тесты, сравнивающие demo-снапшот целиком (`EtcdSnapshotIntegrationTests` и пр.), упали из-за нового ключа сида — обновить их ожидания (добавить заявку в ожидаемые `MoveTickets`/KV-наборы), НЕ удаляя сам сид.

---

### Task 8: Фронт — DTO и API-функция

**Вход:** arch/03 §2 (DTO); Tasks 5–6 (бэкенд-контракт).

**Действие:** типы `dto.ts` + `moveBuckets` в `queries.ts`.

**Выход:** типизированный клиент пятой мутации.

**Проверка:** `cd frontend && npm run build` — без ошибок TS.

**Связь со spec:** §4.5; arch/03 §1.5/§2.

**Files:**
- Modify: `frontend/src/api/dto.ts`
- Modify: `frontend/src/api/queries.ts`

- [ ] **Step 8.1: `dto.ts`** — добавить (после `ShardAddedDto`):

```ts
// POST /api/clusters/{cluster}/moves — тело и ответ (arch/03 §1.5, 02 §9.7).
export interface MoveBucketsRequestDto {
  from: string;
  to: string;
  buckets: number[];
}

export interface MovesQueuedDto {
  cluster: string;
  from: string;
  to: string;
  queued: number[];
  skipped: number[];
}

// Строка очереди заявок кластера (arch/03 §2): /pgworker/moves/<C>/<bucket>.
export interface MoveTicketDto {
  bucketId: number | null;
  bucket: string;
  op: string;
  to: string | null;
  requestedUnix: number;
  requestedBy: string | null;
}
```

В `ClusterDto` добавить поле после `buckets` (перед `heals`): `pendingMoves: MoveTicketDto[];` и комментарий `// очередь заявок переездов (arch/02 §2.3.1)`.

- [ ] **Step 8.2: `queries.ts`** — импортировать `MoveBucketsRequestDto, MovesQueuedDto` в блок `import type {...}` и добавить в конец (перед `logoutRequest`):

```ts
// POST /api/clusters/{cluster}/moves — пятая мутация панели (arch/02 §9.7):
// заявки в очередь /pgworker/moves/; выполнение — PgWorker (последовательно).
export function moveBuckets(cluster: string, request: MoveBucketsRequestDto): Promise<MovesQueuedDto> {
  return apiFetch<MovesQueuedDto>(`/api/clusters/${encodeURIComponent(cluster)}/moves`,
    { method: 'POST', body: request });
}
```

- [ ] **Step 8.3: Проверка**

`cd frontend && npm run build` → без ошибок.

---

### Task 9: Фронт — модал «Перенести бакеты» + кнопка на вкладке «Бакеты»

**Вход:** Task 8; образец `AddShardModal.tsx` (мутация, инвалидация, ProblemDetails-Alert).

**Действие:** новый `MoveBucketsModal.tsx`; кнопка в заголовке `BucketsTab` (при `canScale`); проброс props из `ClusterDetailsPage`.

**Выход:** UI-запуск заявок по arch/03 §3.3.

**Проверка:** `cd frontend && npm run build` — без ошибок TS.

**Связь со spec:** §4.5 (модал), §7.6; arch/03 §3.3.

**Files:**
- Create: `frontend/src/pages/cluster-details/MoveBucketsModal.tsx`
- Modify: `frontend/src/pages/cluster-details/BucketsTab.tsx`
- Modify: `frontend/src/pages/ClusterDetailsPage.tsx`

РП-4: вместо тоста (spec упоминал «тост») — результат-Alert в открытом модале с кнопкой «Готово»: в проекте нет notification-библиотеки, тянуть зависимость сверх минимума (spec §2 YAGNI); инвалидация запросов выполняется сразу.

- [ ] **Step 9.1: `MoveBucketsModal.tsx`**

```tsx
// Форма «Перенести бакеты» (arch/03 §3.3): источник/приёмник/чекбоксы бакетов;
// заявки ставит POST /api/clusters/{c}/moves (02 §9.7) — переезды выполняет
// PgWorker последовательно, порядок — по возрастанию id.
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Badge, Button, Checkbox, Group, Modal, ScrollArea, Select, Stack, Text } from '@mantine/core';
import { useMemo, useState } from 'react';
import { ApiError } from '../../api/client';
import { moveBuckets, queryKeys } from '../../api/queries';
import type { BucketDto, MoveTicketDto, MovesQueuedDto, ShardDto } from '../../api/dto';

interface Props {
  cluster: string;
  shards: ShardDto[];
  buckets: BucketDto[];
  pendingMoves: MoveTicketDto[];
  opened: boolean;
  onClose: () => void;
}

export function MoveBucketsModal({ cluster, shards, buckets, pendingMoves, opened, onClose }: Props) {
  const queryClient = useQueryClient();
  const [from, setFrom] = useState<string | null>(null);
  const [to, setTo] = useState<string | null>(null);
  const [selected, setSelected] = useState<Set<number>>(new Set());
  const [result, setResult] = useState<MovesQueuedDto | null>(null);
  const [error, setError] = useState<string | null>(null);

  // Кандидаты источника: все шарды (TO_REMOVE допустим — эвакуация, Д9).
  const bucketCounts = useMemo(
    () => Object.fromEntries(shards.map((s) => [s.name, buckets.filter((b) => b.owner === s.name).length])),
    [shards, buckets],
  );
  const fromData = shards.map((s) => ({
    value: s.name,
    label: `${s.name} (${bucketCounts[s.name] ?? 0} бакетов)${s.state === 'TO_REMOVE' ? ' · к удалению' : ''}`,
  }));
  // Приёмники: кроме источника и не TO_REMOVE (Д9).
  const toData = shards
    .filter((s) => s.name !== from && s.state !== 'TO_REMOVE')
    .map((s) => ({ value: s.name, label: s.name }));

  const sourceBuckets = useMemo(
    () => buckets.filter((b) => b.owner === from).sort((a, b) => a.id - b.id),
    [buckets, from],
  );
  // Бакеты с уже стоящей заявкой — disabled с бейджем (arch/03 §3.3).
  const claimed = useMemo(
    () => new Set(pendingMoves.map((t) => t.bucketId).filter((id): id is number => id !== null)),
    [pendingMoves],
  );

  const mutation = useMutation({
    mutationFn: (body: { from: string; to: string; buckets: number[] }) => moveBuckets(cluster, body),
    onSuccess: async (data) => {
      setResult(data);
      setError(null);
      setSelected(new Set());
      await queryClient.invalidateQueries({ queryKey: ['clusters'] });
      await queryClient.invalidateQueries({ queryKey: queryKeys.cluster(cluster) });
    },
    onError: (e) => setError(e instanceof ApiError ? (e.detail ?? e.message) : 'Неизвестная ошибка'),
  });

  function toggle(id: number) {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id); else next.add(id);
      return next;
    });
  }

  function submit() {
    if (from === null || to === null || selected.size === 0) return;
    mutation.mutate({ from, to, buckets: [...selected].sort((a, b) => a - b) });
  }

  // Успех: сводка результата вместо тоста (решение при планировании — РП-4).
  if (result !== null) {
    return (
      <Modal opened={opened} onClose={() => { setResult(null); onClose(); }} title="Перенести бакеты" centered>
        <Stack gap="sm">
          <Alert color="teal" variant="light">
            Поставлено в очередь: {result.queued.length}
            {result.skipped.length > 0 ? ` (уже стояли: ${result.skipped.length})` : ''}.
            Переезды начнёт PgWorker — смотрите вкладку «Переезды».
          </Alert>
          <Group justify="flex-end">
            <Button onClick={() => { setResult(null); onClose(); }}>Готово</Button>
          </Group>
        </Stack>
      </Modal>
    );
  }

  return (
    <Modal opened={opened} onClose={onClose} title="Перенести бакеты" centered size="lg">
      <Stack gap="sm">
        <Group grow gap="sm">
          <Select label="Шард-источник" data={fromData} value={from}
            onChange={(v) => { setFrom(v); setTo(null); setSelected(new Set()); }}
            nothingFoundMessage="Нет шардов" />
          <Select label="Шард-приёмник" data={toData} value={to} onChange={setTo}
            nothingFoundMessage="Выберите другой источник" />
        </Group>
        {from !== null ? (
          sourceBuckets.length === 0 ? (
            <Text size="sm" c="dimmed">На источнике нет бакетов</Text>
          ) : (
            <ScrollArea.Autosize mah={260}>
              <Stack gap={4}>
                {sourceBuckets.map((b) => {
                  const busy = claimed.has(b.id);
                  const active = b.state === 'ACTIVE';
                  return (
                    <Checkbox key={b.id}
                      label={<Group gap={6}><span>{`bucket_${b.id}`}</span>
                        {busy ? <Badge color="grape" variant="light">в очереди</Badge> : null}
                        {!active ? <Badge color="yellow" variant="light">{b.state}</Badge> : null}
                      </Group>}
                      checked={selected.has(b.id)}
                      disabled={!active || busy}
                      onChange={() => toggle(b.id)} />
                  );
                })}
              </Stack>
            </ScrollArea.Autosize>
          )
        ) : null}
        {from !== null && sourceBuckets.length > 0 ? (
          <Group gap="xs">
            <Button size="xs" variant="subtle"
              onClick={() => setSelected(new Set(sourceBuckets
                .filter((b) => b.state === 'ACTIVE' && !claimed.has(b.id)).map((b) => b.id)))}>
              выбрать все
            </Button>
            <Button size="xs" variant="subtle" onClick={() => setSelected(new Set())}>снять</Button>
          </Group>
        ) : null}
        <Text size="sm" c="dimmed">
          Переезды выполняются последовательно, по одному бакету за раз (обрабатывает
          PgWorker); порядок — по возрастанию id.
        </Text>
        {error !== null ? <Alert color="red" variant="light">{error}</Alert> : null}
        <Group justify="flex-end" mt="xs">
          <Button variant="default" onClick={onClose}>Отмена</Button>
          <Button loading={mutation.isPending} disabled={from === null || to === null || selected.size === 0}
            onClick={submit}>
            Поставить в очередь
          </Button>
        </Group>
      </Stack>
    </Modal>
  );
}
```

- [ ] **Step 9.2: `BucketsTab.tsx`** — расширить props и добавить заголовок с кнопкой (образец `ShardsTab`):

Сигнатура:

```tsx
export function BucketsTab({ cluster, canScale, shards, buckets, pendingMoves }: {
  cluster: string; canScale: boolean; shards: ShardDto[];
  buckets: BucketDto[]; pendingMoves: MoveTicketDto[];
}) {
```

Добавить `const [moveOpened, setMoveOpened] = useState(false);`, импорт `useState`, `Button`, `MoveBucketsModal`, типы `ShardDto`, `MoveTicketDto`. Существующее содержимое обернуть: перед фильтрами-гридом добавить

```tsx
      <Group justify="space-between">
        <Text fw={500}>Бакеты</Text>
        {canScale ? (
          <Group gap="xs">
            <Button size="xs" variant="light" onClick={() => setMoveOpened(true)}>Перенести бакеты</Button>
            <MoveBucketsModal cluster={cluster} shards={shards} buckets={buckets}
              pendingMoves={pendingMoves} opened={moveOpened} onClose={() => setMoveOpened(false)} />
          </Group>
        ) : null}
      </Group>
```

(корневой `Stack gap="xs"` уже есть — блок ставится первым ребёнком).

- [ ] **Step 9.3: `ClusterDetailsPage.tsx`** — проброс:

```tsx
        {data.sharded ? (
          <Tabs.Panel value="buckets" pt="sm">
            <BucketsTab cluster={data.name} canScale={canScale} shards={data.shards}
              buckets={data.buckets} pendingMoves={data.pendingMoves} />
          </Tabs.Panel>
        ) : null}
```

- [ ] **Step 9.4: Проверка**

`cd frontend && npm run build` → без ошибок TS/vite.

---

### Task 10: Фронт — «Очередь заявок» во вкладке «Переезды» + финальный прогон

**Вход:** Task 8 (`MoveTicketDto` в `ClusterDto`); `MovesTab.tsx`.

**Действие:** блок очереди в `MovesTab`; финальная полная проверка всего стека.

**Выход:** видимость очереди (кто/куда/возраст/порядок) — spec §1.

**Проверка:** `npm run build` + полный backend-прогон `dotnet build`/`dotnet test` + `npm run build` — всё зелёное (закрытие критериев spec §7).

**Связь со spec:** §4.5 (MovesTab), §7.6–7.7; arch/03 §3 (вкладка Переезды).

**Files:**
- Modify: `frontend/src/pages/cluster-details/MovesTab.tsx`
- Modify: `frontend/src/pages/ClusterDetailsPage.tsx`

- [ ] **Step 10.1: `MovesTab.tsx`** — расширить сигнатуру и добавить блок:

```tsx
export function MovesTab({ buckets, pendingMoves }: {
  buckets: BucketDto[]; pendingMoves: MoveTicketDto[];
}) {
```

ВАЖНО: заменить существующий early return `if (moves.length === 0) return <Text c="dimmed">Активных переездов нет</Text>;` на условный рендер — иначе очередь не отрисуется при пустых переездах:

```tsx
      {moves.length === 0 ? (
        <Text c="dimmed">Активных переездов нет</Text>
      ) : (
        /* существующая Table.ScrollContainer с таблицей переездов — без изменений */
      )}
```

Импорты: `Badge, Group` в существующий `@mantine/core`-импорт, `MoveTicketDto` в `api/dto`. После блока переездов (в конце корневого стека) добавить:

```tsx
      <Group justify="space-between" mt="md">
        <Text fw={500}>Очередь заявок</Text>
      </Group>
      {pendingMoves.length === 0 ? (
        <Text c="dimmed">Очередь заявок пуста</Text>
      ) : (
        <>
          <Table.ScrollContainer minWidth={700}>
            <Table highlightOnHover>
              <Table.Thead>
                <Table.Tr>
                  <Table.Th>Бакет</Table.Th>
                  <Table.Th>Операция</Table.Th>
                  <Table.Th>Куда</Table.Th>
                  <Table.Th>Возраст заявки</Table.Th>
                  <Table.Th>Кем</Table.Th>
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {pendingMoves.map((t) => (
                  <Table.Tr key={t.bucket}>
                    <Table.Td>{t.bucketId === null ? t.bucket : `bucket_${t.bucketId}`}</Table.Td>
                    <Table.Td>
                      <Badge color={t.op === 'move' ? 'blue' : 'grape'} variant="light">{t.op}</Badge>
                    </Table.Td>
                    <Table.Td>{t.to ?? '—'}</Table.Td>
                    <Table.Td>{formatUnixAge(t.requestedUnix)}</Table.Td>
                    <Table.Td>{t.requestedBy ?? '—'}</Table.Td>
                  </Table.Tr>
                ))}
              </Table.Tbody>
            </Table>
          </Table.ScrollContainer>
          <Text size="sm" c="dimmed">
            Переезды выполняются по одному бакету за раз — старейшая заявка берётся первой.
          </Text>
        </>
      )}
```

Комментарий-заголовок файла дополнить: «…+ очередь заявок /pgworker/moves/ (arch/02 §2.3.1)».

- [ ] **Step 10.2: `ClusterDetailsPage.tsx`** — вкладка Переезды:

```tsx
        <Tabs.Panel value="moves" pt="sm">
          <MovesTab buckets={data.buckets} pendingMoves={data.pendingMoves} />
        </Tabs.Panel>
```

- [ ] **Step 10.3: Финальная проверка (критерии spec §7)**

1. `cd src && dotnet build AdminPanel.slnx` → 0 ошибок/warning.
2. `cd src && dotnet test AdminPanel.slnx` [Docker] → все зелёные (юнит + интеграционные).
3. `cd frontend && npm run build` → без ошибок.
4. Санити-обзор diff: код не противоречит arch/02 §9.7 и arch/03 §1.5/§3.3 (порядок handler'а, коды ответов, формулировки guard'ов).
5. (Опционально, если dev-стенд поднят — spec §7.6, ручная проверка): `bash dev-stand/seed.sh` пересеивает demo с заявкой `bucket_13`; в UI кластера demo: кнопка «Перенести бакеты» на вкладке Бакеты, в модале бакеты с заявкой помечены «в очереди», вкладка Переезды показывает очередь.

---

## Порядок исполнения и зависимости

```
Task 1 → Task 2 → Task 3 ─┐
Task 1 → Task 4 (нужен Task 2: MovesQueueParser) → Task 5 ─┤→ Task 7 [Docker] → (фронт независим от 7)
Task 1 → Task 6 ─────────────────────────────────────────┘
Task 8 → Task 9 → Task 10 (Task 10.3 — после ВСЕХ задач)
```

Задачи 3, 4(после 2), 6 и 8 взаимно параллелимы. Task 10.3 выполняется последним.
