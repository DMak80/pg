# t07 — UI явных переездов бакетов: план реализации

> **Для агентов-исполнителей:** ОБЯЗАТЕЛЬНЫЙ SUB-SKILL: superpowers:subagent-driven-development (рекомендуется) или superpowers:executing-plans — исполнять план задача-за-задачей. Шаги — чекбоксы (`- [ ]`) для трекинга.

**Цель:** полный цикл операторских операций над переездами бакетов из панели: мутации `rollback`/`finalize`/`abort` + отмена стоящих заявок (прокси в API PgWorker, per-op эндпоинты) и чтение результатов заявок (блок «Журнал воркера»).

**Архитектура:** панель не пишет в etcd — все 4 мутации идут прокси (`WorkerProxy` → HTTP API PgWorker, паттерн etcd-via-worker-api); воркер ставит/удаляет ключи `/pgworker/moves/<C>/bucket_<i>` (txn-клэйм `version==0`, порядок `requested_unix`); guard'ы — быстрые пред-проверки по прямым чтениям etcd (`ClusterGuardData`), авторитетно перепроверяет `MoveProcess`. Frontend — per-row кнопки по состоянию бакета + 3 модалки + блок журнала из поля `work` деталей кластера (источник уже в снапшоте панели).

**Стек:** .NET 10 (C# latest, `Nullable=enable`, `TreatWarningsAsErrors=true`), ASP.NET Minimal API, xUnit + FluentAssertions, etcd-фикстуры (docker), React 19 + Mantine 9 + TanStack Query.

**Спека:** [`docs/superpowers/2026-09-03-t07-move-bucket-ui/spec.md`](spec.md) — план аргументируется от спеки, исполнители читают обе. Канон-контракты уже обновлены спекой (arch-first): arch/02 §9.7.1–§9.7.5, arch/03 §1.5–§1.9/§2/§3.4–§3.6, arch/14 §1.1.

## Глобальные ограничения

- Сборка: `dotnet build src/PgWorker.slnx` — **0 warnings** (`TreatWarningsAsErrors=true`); после каждой волны — зелёный `dotnet test`.
- Тесты: комментарии по **AAA** (Arrange/Act/Assert); docker-порты в тестах — динамические (для этой задачи фикстуры etcd-only, `BrokerBootSec` не используется); таймауты короткие.
- Существующий `POST /api/clusters/{c}/moves` и фронт-контракт «Перенести бакеты» — БЕЗ изменений поведения: `MovesApiTests` зелёные без правок ожиданий (спека §10.7).
- Идемпотентность постановки: стоящая **идентичная** заявка → ответ 201 без записи (`skipped`-семантика §9.7 п.3); иная → 409 `MoveRequestConflictException`. Отмена заявки НЕ идемпотентна: повтор → 404 «заявки нет».
- Канонический JSON заявок: snake_case, null-поля опускаются (`WhenWritingNull`); `force: true` пишется, `false` — опускается (парсится `MoveRequest.Parse` как `false`).
- `requested_by` — заголовок `X-Requested-By` (панель шлёт username сессии), fallback `"api"` — как у move.
- Язык: документация/тексты ошибок — русский; идентификаторы — английские.
- Работа в worktree `feat-t07-move-bucket-ui`; коммит после каждой задачи; в `main` не мержим (мерж — отдельный гейт dev-flow).
- Волны строго по порядку: A (PgWorker API) → B (панель) → C (стенд e2e).

---

## Волна A — PgWorker API

### Task A1: `MoveTickets` — вынос общей логики постановки заявок

Рефакторинг без изменения поведения: чтение очереди + txn-клэйм из `MoveBucketsHandler` → общий внутренний класс. Регресс — существующие `MovesApiTests` без правок ожиданий.

**Files:**
- Create: `src/PgWorker.App/Api/Operations/MoveTickets.cs`
- Modify: `src/PgWorker.App/Api/Operations/MoveBucketsHandler.cs` (удалить `TicketBody`/`ParseTickets`/`AllTicketsMaxUnix`/`TicketJson`, перейти на `MoveTickets`)

**Interfaces:**
- Produces (для задач A4–A7):
  - `internal static class MoveTickets` с:
    - `internal sealed record Existing(string Op, string? To, string? OldShard, bool Force)` — живая заявка (поля идентичности §9.7 п.3);
    - `internal sealed record Queue(IReadOnlyDictionary<string, Existing> Mine, long MaxUnix)`;
    - `internal sealed record TicketBody(...)` — общее каноническое тело (op/to?/old_shard?/force?/requested_unix/requested_by);
    - `internal static readonly JsonSerializerOptions TicketJson` (`WhenWritingNull`);
    - `Task<Result<Queue>> ReadQueueAsync(IEtcdGateway gateway, string[] endpoints, string cluster, CancellationToken ct)`;
    - `Task<Result<TxnResult>> ClaimAsync(IEtcdGateway gateway, string[] endpoints, string key, string json, CancellationToken ct)`.
- Consumes: `EtcdFailover.CallAsync`, `IEtcdGateway` (`RangeAsync`/`TxnAsync`), `TxnRequest.Of`/`TxnCompare.NotExists`/`TxnOp.Put` — как в текущем `MoveBucketsHandler`.

- [ ] **Шаг 1. Создать `MoveTickets.cs`**

```csharp
using System.Text.Json;
using System.Text.Json.Serialization;
using PgWorker.Core;
using PgWorker.Etcd.Client;

namespace PgWorker.App.Api.Operations;

// Общая логика постановки заявок переездов (t07, arch/02 §9.7 п.3–5): чтение
// очереди напрямую у etcd, база requested_unix, txn-клэйм per key. Портировано
// из MoveBucketsHandler без изменения поведения (регресс — MovesApiTests).
internal static class MoveTickets
{
    // Живая заявка нашего кластера: поля для проверки идентичности (§9.7 п.3).
    internal sealed record Existing(string Op, string? To, string? OldShard, bool Force);

    // Снимок очереди: заявки кластера по leaf'ам + глобальный max requested_unix.
    internal sealed record Queue(IReadOnlyDictionary<string, Existing> Mine, long MaxUnix);

    // Канон тела заявки (arch/14 §3.3, snake_case): только заполненные поля
    // пишутся в JSON (WhenWritingNull; force:true пишется, false — опускается).
    internal sealed record TicketBody(
        [property: JsonPropertyName("op")] string Op,
        [property: JsonPropertyName("to")] string? To,
        [property: JsonPropertyName("old_shard")] string? OldShard,
        [property: JsonPropertyName("force")] bool? Force,
        [property: JsonPropertyName("requested_unix")] long RequestedUnix,
        [property: JsonPropertyName("requested_by")] string RequestedBy);

    internal static readonly JsonSerializerOptions TicketJson = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    // Чтение префикса /pgworker/moves/ одним range (§9.7 п.3): заявки кластера
    // + глобальный max requested_unix (база упорядочивания, п.4). Битый JSON
    // скипаем — его отвергнет и удалит процесс переездов (arch/02 §7).
    public static async Task<Result<Queue>> ReadQueueAsync(
        IEtcdGateway gateway, string[] endpoints, string cluster, CancellationToken ct)
    {
        var range = await EtcdFailover.CallAsync(endpoints,
            endpoint => gateway.RangeAsync(endpoint, "/pgworker/moves/", ct));
        if (!range.IsSuccess)
            return Result<Queue>.Failed(range.Error!);

        var mine = new Dictionary<string, Existing>();
        long maxUnix = 0;
        foreach (var kv in range.Value)
        {
            try
            {
                using var doc = JsonDocument.Parse(kv.Value);
                var root = doc.RootElement;
                if (!root.TryGetProperty("op", out var op) || op.ValueKind != JsonValueKind.String)
                    continue; // заявка без op — не наша
                if (root.TryGetProperty("requested_unix", out var unix)
                    && unix.ValueKind == JsonValueKind.Number)
                    maxUnix = Math.Max(maxUnix, unix.GetInt64());

                var segments = kv.Key.Split('/');
                if (segments.Length != 5 || segments[3] != cluster || segments[4].Length == 0)
                    continue;
                string? ReadString(string name) => root.TryGetProperty(name, out var el)
                    && el.ValueKind == JsonValueKind.String
                    ? el.GetString()
                    : null;
                var force = root.TryGetProperty("force", out var f) && f.ValueKind == JsonValueKind.True;
                mine[segments[4]] = new Existing(op.GetString()!, ReadString("to"), ReadString("old_shard"), force);
            }
            catch (JsonException)
            {
                // битая заявка не участвует ни в идентичности, ни в базе
            }
        }

        return Result<Queue>.Success(new Queue(mine, maxUnix));
    }

    // Txn-клэйм per key (§9.7 п.5): compare NotExists + put — защита от
    // перезаписи чужой заявки между чтением и записью.
    public static Task<Result<TxnResult>> ClaimAsync(
        IEtcdGateway gateway, string[] endpoints, string key, string json, CancellationToken ct)
        => EtcdFailover.CallAsync(endpoints, endpoint => gateway.TxnAsync(endpoint,
            TxnRequest.Of([TxnCompare.NotExists(key)], [new TxnOp.Put(key, json, null)]), ct));
}
```

- [ ] **Шаг 2. Перевести `MoveBucketsHandler` на `MoveTickets`**

В `HandleAsync` заменить блоки 4–5:
- `ParseTickets(movesRange.Value, cluster)` + `AllTicketsMaxUnix(...)` → `var queue = await MoveTickets.ReadQueueAsync(gateway, endpoints, cluster, ct);` (проверять `queue.IsSuccess`, использовать `queue.Value.Mine` / `queue.Value.MaxUnix`);
- идентичность: `existing.Op == "move" && existing.To == to` → `skipped`;
- тело заявки: `JsonSerializer.Serialize(new MoveTickets.TicketBody("move", to, null, null, unixBase + k, requestedBy), MoveTickets.TicketJson)`;
- клэйм: `MoveTickets.ClaimAsync(gateway, endpoints, key, body, ct)`.
Удалить ставшие неиспользуемыми приватные `TicketBody`, `TicketJson`, `ParseTickets`, `AllTicketsMaxUnix`.

- [ ] **Шаг 3. Регресс: сборка + MovesApiTests зелёные без правок ожиданий**

Run: `dotnet build src/PgWorker.slnx` → 0 warnings; `dotnet test src/tests/PgWorker.IntegrationTests --filter "FullyQualifiedName~MovesApiTests"` → PASS.
Критерий: спека §5.1 («рефакторинг с сохранением текстов ошибок; тесты MovesApiTests — зелёные без правок ожиданий») и §10.7 (регресс).

- [ ] **Шаг 4. Commit**

```bash
git add src/PgWorker.App/Api/Operations/MoveTickets.cs src/PgWorker.App/Api/Operations/MoveBucketsHandler.cs
git commit -m "refactor(t07): вынос общей постановки заявок в MoveTickets (регресс: MovesApiTests без правок)"
```

---

### Task A2: `ClusterGuardData.UpdatedUnix` — возраст статус-ключа для abort

**Files:**
- Modify: `src/PgWorker.App/Api/Operations/ClusterGuardData.cs`
- Test: `src/tests/PgWorker.UnitTests/Api/ClusterGuardDataTests.cs` (новый)
- Test: `src/tests/PgWorker.UnitTests/Api/FakeEtcdGateway.cs` (новый мини-fake, общий для юнит-тестов API)

**Interfaces:**
- Produces: `Status: IReadOnlyDictionary<int, (string? State, string? Owner, string? Target, long? UpdatedUnix)>` — `UpdatedUnix` из JSON `updated_unix`; поле отсутствует/битое → `null` (пред-проверка свежести пропускается, авторитетно решит процесс — спека §5.3).
- Consumes: `IEtcdGateway` (методы `RangeAsync`/`GetAsync`/`PutAsync`/`DeleteAsync`/`TxnAsync`).

- [ ] **Шаг 1. Написать failing-тесты (новые файлы)**

`FakeEtcdGateway.cs` — мини-имитация etcd в памяти (порт приватного fake из `Etcd/CoordinationTests.cs`, только нужное):

```csharp
using PgWorker.Core;
using PgWorker.Etcd.Client;

namespace PgWorker.UnitTests.Api;

// Мини-имитация etcd в памяти для юнит-тестов API-хендлеров (t07): range/get/
// put/delete + txn-compare version==0. Порт fake из Etcd/CoordinationTests.
internal sealed class FakeEtcdGateway : IEtcdGateway
{
    public Dictionary<string, string> Store { get; } = [];

    public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyList<Kv>>.Success(
            Store.Where(p => p.Key.StartsWith(prefix, StringComparison.Ordinal))
                .Select(p => new Kv(p.Key, p.Value, 1)).ToList()));

    public Task<Result<Kv?>> GetAsync(string endpoint, string key, CancellationToken ct)
        => Task.FromResult(Result<Kv?>.Success(Store.TryGetValue(key, out var v) ? new Kv(key, v, 1) : null));

    public Task<Result> PutAsync(string endpoint, string key, string value, long? lease, CancellationToken ct)
    {
        Store[key] = value;
        return Task.FromResult(Result.Success());
    }

    public Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
    {
        foreach (var key in Store.Keys.Where(k => prefix
                     ? k.StartsWith(keyOrPrefix, StringComparison.Ordinal)
                     : k == keyOrPrefix).ToList())
            Store.Remove(key);
        return Task.FromResult(Result.Success());
    }

    public Task<Result<TxnResult>> TxnAsync(string endpoint, TxnRequest req, CancellationToken ct)
        => Task.FromResult(Result<TxnResult>.Success(new TxnResult(
            req.Compare.All(c => c.Target == TxnTarget.Version
                && (!Store.ContainsKey(c.Key) && c.Num == 0 || Store.ContainsKey(c.Key) && c.Num != 0)))));
}
```

`ClusterGuardDataTests.cs`:

```csharp
using PgWorker.App.Api.Operations;
using FluentAssertions;
using Xunit;

namespace PgWorker.UnitTests.Api;

// ClusterGuardData.Status.UpdatedUnix (t07, спека §5.3): возраст статус-ключа
// для пред-проверки свежести abort; отсутствие поля → null (проверка пропускается).
public class ClusterGuardDataTests
{
    private const string Ep = "http://etcd";

    [Fact]
    public async Task ReadAsync_StatusWithUpdatedUnix_Parsed()
    {
        // Arrange
        var gw = new FakeEtcdGateway();
        await gw.PutAsync(Ep, "/clusters/c/config", """{"buckets":2,"dbname":"c"}""", null, CancellationToken.None);
        await gw.PutAsync(Ep, "/clusters/c/buckets/routing/bucket_0", "s1", null, CancellationToken.None);
        await gw.PutAsync(Ep, "/clusters/c/buckets/status/bucket_0",
            """{"state":"SYNCING","owner":"s1","target":"s2","updated_unix":1756000100}""", null, CancellationToken.None);

        // Act
        var data = await ClusterGuardData.ReadAsync(gw, [Ep], "c", CancellationToken.None);

        // Assert
        data.IsSuccess.Should().BeTrue();
        var status = data.Value.Status[0];
        status.State.Should().Be("SYNCING");
        status.Owner.Should().Be("s1");
        status.Target.Should().Be("s2");
        status.UpdatedUnix.Should().Be(1756000100);
    }

    [Fact]
    public async Task ReadAsync_StatusWithoutUpdatedUnix_Null()
    {
        // Arrange — старый формат ключа без updated_unix (толерантность §5.3)
        var gw = new FakeEtcdGateway();
        await gw.PutAsync(Ep, "/clusters/c/config", """{"buckets":2,"dbname":"c"}""", null, CancellationToken.None);
        await gw.PutAsync(Ep, "/clusters/c/buckets/status/bucket_1",
            """{"state":"FROZEN","owner":"s1","target":"s2"}""", null, CancellationToken.None);

        // Act
        var data = await ClusterGuardData.ReadAsync(gw, [Ep], "c", CancellationToken.None);

        // Assert
        data.Value.Status[1].UpdatedUnix.Should().BeNull();
    }
}
```

- [ ] **Шаг 2. Запустить — убедиться в FAIL**

Run: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~ClusterGuardDataTests"` 
Expected: FAIL — `Status[...]` не содержит `UpdatedUnix` (ошибка компиляции: кортеж 3-элементный).

- [ ] **Шаг 3. Реализовать**

В `ClusterGuardData.cs`:
- тип `Status` → `IReadOnlyDictionary<int, (string? State, string? Owner, string? Target, long? UpdatedUnix)>`;
- `ParseStatus` → читать `updated_unix` (Number → `GetInt64()`, иначе `null`), вернуть 4-элементный кортеж; битый JSON → `(ActiveState, null, null, null)`.
(Лишний приватный хелпер `SeedAsync` из шага 1 в тест не включать — сид делается прямыми `PutAsync`.)

- [ ] **Шаг 4. Запустить — PASS + билд**

Run: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~ClusterGuardDataTests"` → PASS; `dotnet build src/PgWorker.slnx` → 0 warnings (компилятор подсветит места, деструктурирующие старый 3-элементный кортеж — их нет, `Status` используется только через `TryGetValue` + обращение к полям).

- [ ] **Шаг 5. Commit**

```bash
git add src/PgWorker.App/Api/Operations/ClusterGuardData.cs src/tests/PgWorker.UnitTests/Api/
git commit -m "feat(t07): ClusterGuardData.Status.UpdatedUnix — возраст статус-ключа для пред-проверки abort"
```

---

### Task A3: Исключения + `RollbackBucketsHandler` + маршрут rollback

Первая мутация волны A: `POST /api/clusters/{c}/moves/rollback` (arch/02 §9.7.2, arch/03 §1.7). Здесь же — общие исключения новых ops.

**Files:**
- Modify: `src/PgWorker.App/Api/Operations/WorkerApiExceptions.cs` (новые классы + `MoveOpValidationException`)
- Create: `src/PgWorker.App/Api/Operations/RollbackBucketsHandler.cs`
- Modify: `src/PgWorker.App/Api/ApiModule.cs` (маршрут)
- Modify: `src/PgWorker.App/Program.cs` (DI)
- Test: `src/tests/PgWorker.IntegrationTests/Api/MoveOpsApiTests.cs` (новый; rollback-кейсы)
- Modify: `src/tests/PgWorker.IntegrationTests/Api/ApiTestSeed.cs` (хелперы сида статусов/заявок)

**Interfaces:**
- Consumes: `MoveTickets.ReadQueueAsync/ClaimAsync/TicketBody/TicketJson` (Task A1), `ClusterGuardData` (A2), существующие исключения `ClusterNotFoundException`/`ClusterNotActiveException`/`NonShardedClusterException`/`MoveRequestConflictException`/`MoveClaimLostException`/`InvalidClusterConfigException`.
- Produces:
  - `public sealed record RollbackBucketsRequest(IReadOnlyList<int>? Buckets)`;
  - `public sealed record RollbackQueuedDto(string Cluster, IReadOnlyList<int> Queued, IReadOnlyList<int> Skipped)`;
  - `public sealed class RollbackBucketsHandler(IEtcdGateway gateway, string[] endpoints, TimeProvider time)` с `Task<Result<RollbackQueuedDto>> HandleAsync(string cluster, RollbackBucketsRequest command, string requestedBy, CancellationToken ct)`;
  - исключения (см. код шага 1) — используются также задачами A4/A5.

- [ ] **Шаг 1. Добавить исключения в `WorkerApiExceptions.cs`**

```csharp
// Валидация тел move-ops (rollback/finalize/abort) не прошла: 400 с errors
// по полям (arch/02 §9.7.2–§9.7.4) — тот же маппинг, что у MoveBucketsValidationException.
public sealed class MoveOpValidationException(IReadOnlyList<PgWorker.Core.Writing.ValidationError> errors)
    : Exception("параметры операции переездов некорректны")
{
    public IReadOnlyList<PgWorker.Core.Writing.ValidationError> Errors { get; } = errors;
}

// Бакет не в состоянии, требуемом операцией (02 §9.7.2–§9.7.4): тексты по op —
// как у процесса (rollback/finalize «только из ACTIVE»; abort — см. фабрики).
public sealed class BucketNotActiveForMoveOpException(string message) : Exception(message)
{
    public static BucketNotActiveForMoveOpException RollbackOrFinalize(string op, int bucket, string? owner, string state)
        => new($"{op} bucket_{bucket} возможен только из ACTIVE (владелец: {owner ?? "—"}, состояние: {state})");

    public static BucketNotActiveForMoveOpException AbortActive(int bucket)
        => new($"бакет bucket_{bucket} ACTIVE — отменять нечего; пост-flip артефакты убирает finalize");

    public static BucketNotActiveForMoveOpException AbortNotInitialized(int bucket)
        => new($"бакет bucket_{bucket} NOT_INITIALIZED — начальное состояние создаваемого кластера, не переезд");

    public static BucketNotActiveForMoveOpException OutOfRange(string op, int bucket)
        => new($"бакет {bucket} вне диапазона или без routing — операция {op} невозможна");
}

// Finalize: выбранный шард — текущий владелец, убирать нечего (02 §9.7.3) — 409.
public sealed class FinalizeTargetIsOwnerException(string cluster, int bucket, string shard)
    : Exception($"шард {cluster}/{shard} — текущий владелец bucket_{bucket}, убирать нечего");

// Abort: статус-ключ свежий, mover возможно жив (02 §9.7.4; порт текста AbortSequence) — 409.
public sealed class MoveStatusFreshException(long ageSec, long thresholdSec)
    : Exception($"статус обновлён {ageSec}с назад (< AbortMinAgeSec={thresholdSec}с) — переезд, возможно, ещё жив; если mover точно мёртв — force");

// Abort: routing уже указывает на target — доведение осознанно (02 §9.7.4) — 409.
public sealed class MoveAlreadyFlippedException(string target)
    : Exception($"routing уже указывает на target '{target}' — похоже, flip прошёл, а статус-ключ остался; такой abort станет уборкой СТАРОГО шарда (как finalize) — осознанно: force");

// Отмена заявки: ключа /pgworker/moves/<C>/<bucket> нет (02 §9.7.5) — 404.
public sealed class MoveTicketNotFoundException(string cluster, string bucket)
    : Exception($"заявки /pgworker/moves/{cluster}/{bucket} нет");
```

- [ ] **Шаг 2. Написать failing интеграционные тесты (rollback) + сид-хелперы**

В `ApiTestSeed.cs` добавить:

```csharp
/// <summary>Статус-ключ переезда /clusters/<C>/buckets/status/bucket_<N> (канон arch/02 §2.1).</summary>
public static Task SeedBucketStatusAsync(
    EtcdFixture etcd, string cluster, int bucket, string state,
    string owner, string target, long updatedUnix, string? lastError = null)
    => etcd.Gateway.PutAsync(etcd.Endpoint, $"/clusters/{cluster}/buckets/status/bucket_{bucket}",
        $$"""{"bucket":"bucket_{{bucket}}","state":"{{state}}","owner":"{{owner}}","target":"{{target}}","started_unix":{{updatedUnix}},"updated_unix":{{updatedUnix}},"phase":"copy"{{(lastError is null ? "" : $",\"last_error\":\"{lastError}\"")}}}""",
        null, TestContext.Current.CancellationToken);

/// <summary>Заявка произвольного op в очереди /pgworker/moves/<C>/bucket_<N> (канон arch/14 §3.3).</summary>
public static Task SeedTicketAsync(
    EtcdFixture etcd, string cluster, int bucket, string op,
    string? to = null, string? oldShard = null, bool? force = null, long unix = 100, string by = "seed")
{
    var fields = new List<string> { $"\"op\":\"{op}\"" };
    if (to is not null) fields.Add($"\"to\":\"{to}\"");
    if (oldShard is not null) fields.Add($"\"old_shard\":\"{oldShard}\"");
    if (force == true) fields.Add("\"force\":true");
    fields.Add($"\"requested_unix\":{unix}");
    fields.Add($"\"requested_by\":\"{by}\"");
    return etcd.Gateway.PutAsync(etcd.Endpoint, $"/pgworker/moves/{cluster}/bucket_{bucket}",
        $"{{{string.Join(",", fields)}}}", null, TestContext.Current.CancellationToken);
}
```

Новый `MoveOpsApiTests.cs` (rollback-часть; класс пополняется задачами A4/A5):

```csharp
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using PgWorker.IntegrationTests.Etcd;
using Xunit;

namespace PgWorker.IntegrationTests.Api;

// POST /api/clusters/{c}/moves/rollback|finalize|abort + DELETE .../moves/{bucket}
// (t07, arch/02 §9.7.2–§9.7.5): постановка заявок op≠move и отмена стоящих.
[Collection(PgApiCollection.Name)]
public class MoveOpsApiTests(PgApiFixture fixture)
{
    private HttpClient Client => fixture.Factory.CreateClient();

    private EtcdFixture Etcd => fixture.Etcd;

    // ===== rollback (§9.7.2) =====

    // AAA: rollback-заявки ставятся по одному ключу на бакет (op=rollback,
    // requested_by из X-Requested-By, requested_unix в конец очереди).
    [Fact]
    public async Task Rollback_QueuesTickets_WithOperatorAndOrder()
    {
        // Arrange — 4×2, бакеты 0,1 на shard1; в очереди чужая заявка unix=100
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "rb", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;
        await ApiTestSeed.SeedTicketAsync(Etcd, "rb", 3, "move", to: "shard2", unix: 100);
        var client = Client;
        client.DefaultRequestHeaders.Add("X-Requested-By", "opsuser");

        // Act
        var resp = await client.PostAsJsonAsync("/api/clusters/rb/moves/rollback",
            new { buckets = new[] { 1, 0 } }, ct);

        // Assert — 201: queued по возрастанию id; ключ op=rollback без to;
        // requested_by из заголовка; requested_unix > сида (в конец очереди).
        resp.StatusCode.Should().Be(HttpStatusCode.Created);
        var body = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        body.GetProperty("queued").EnumerateArray().Select(v => v.GetInt32()).Should().Equal(0, 1);
        var ticket = await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/pgworker/moves/rb/bucket_0", ct);
        ticket.Value!.Value.Should().Contain("\"op\":\"rollback\"")
            .And.Contain("\"requested_by\":\"opsuser\"")
            .And.NotContain("\"to\"");
        using var doc = JsonDocument.Parse(ticket.Value.Value);
        doc.RootElement.GetProperty("requested_unix").GetInt64().Should().BeGreaterThan(100);
    }

    // AAA: повтор идентичной rollback-заявки → skipped (без перезаписи, Д6).
    [Fact]
    public async Task Rollback_Repeat_AllSkipped()
    {
        // Arrange — живая op=rollback заявка на bucket_0
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "rbrpt", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;
        await ApiTestSeed.SeedTicketAsync(Etcd, "rbrpt", 0, "rollback");

        // Act
        var resp = await Client.PostAsJsonAsync("/api/clusters/rbrpt/moves/rollback",
            new { buckets = new[] { 0 } }, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.Created);
        var body = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        body.GetProperty("skipped").EnumerateArray().Select(v => v.GetInt32()).Should().Equal(0);
        body.GetProperty("queued").GetArrayLength().Should().Be(0);
    }

    // AAA: живая иная заявка на бакете → 409 (панель не перезаписывает чужие).
    [Fact]
    public async Task Rollback_ConflictingMoveTicket_409()
    {
        // Arrange
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "rbcf", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;
        await ApiTestSeed.SeedTicketAsync(Etcd, "rbcf", 0, "move", to: "shard2");

        // Act
        var resp = await Client.PostAsJsonAsync("/api/clusters/rbcf/moves/rollback",
            new { buckets = new[] { 0 } }, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var problem = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        problem.GetProperty("title").GetString().Should().Be("Move ops rejected");
    }

    // AAA: не-ACTIVE бакет (SYNCING) → 409 «возможен только из ACTIVE».
    [Fact]
    public async Task Rollback_SyncingBucket_409()
    {
        // Arrange
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "rbsync", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;
        await ApiTestSeed.SeedBucketStatusAsync(Etcd, "rbsync", 0, "SYNCING", "shard1", "shard2",
            DateTimeOffset.UtcNow.AddSeconds(-300).ToUnixTimeSeconds());

        // Act
        var resp = await Client.PostAsJsonAsync("/api/clusters/rbsync/moves/rollback",
            new { buckets = new[] { 0 } }, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var problem = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
        problem.GetProperty("detail").GetString().Should().Contain("только из ACTIVE");
    }

    // AAA: пустой массив → 400; нешардированный кластер → 409.
    [Fact]
    public async Task Rollback_EmptyBuckets_400()
    {
        // Arrange
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "rb400", buckets: 4, shards: 2);
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await Client.PostAsJsonAsync("/api/clusters/rb400/moves/rollback",
            new { buckets = Array.Empty<int>() }, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task Rollback_NonSharded_409()
    {
        // Arrange — вырожденный 1×1
        await ApiTestSeed.SeedActiveClusterAsync(Etcd, "rb1x1", buckets: 1, shards: 1);
        var ct = TestContext.Current.CancellationToken;

        // Act
        var resp = await Client.PostAsJsonAsync("/api/clusters/rb1x1/moves/rollback",
            new { buckets = new[] { 0 } }, ct);

        // Assert
        resp.StatusCode.Should().Be(HttpStatusCode.Conflict);
    }
}
```

- [ ] **Шаг 3. Запустить — FAIL (404: маршрута нет)**

Run: `dotnet test src/tests/PgWorker.IntegrationTests --filter "FullyQualifiedName~MoveOpsApiTests"` 
Expected: FAIL — все rollback-кейсы падают (эндпоинт не замаплен → 404 вместо 201/409/400).

- [ ] **Шаг 4. Реализовать `RollbackBucketsHandler.cs`**

```csharp
using PgWorker.Core;
using PgWorker.Core.Writing;
using PgWorker.Etcd.Client;

namespace PgWorker.App.Api.Operations;

// Тело POST /api/clusters/{cluster}/moves/rollback (arch/02 §9.7.2). Buckets
// nullable: null/отсутствие поля ловит валидатор (400), а не NRE.
public sealed record RollbackBucketsRequest(IReadOnlyList<int>? Buckets);

// Ответ 201: queued поставлены сейчас, skipped — идентичные op=rollback стояли.
public sealed record RollbackQueuedDto(
    string Cluster, IReadOnlyList<int> Queued, IReadOnlyList<int> Skipped);

// Валидация тела (§9.7.2): 400 с errors по полям.
public static class RollbackBucketsValidator
{
    public static IReadOnlyList<ValidationError> Validate(RollbackBucketsRequest request)
    {
        var errors = new List<ValidationError>();
        if (request.Buckets is null || request.Buckets.Count == 0)
            errors.Add(new("buckets", "выберите хотя бы один бакет"));
        else if (request.Buckets.Distinct().Count() != request.Buckets.Count)
            errors.Add(new("buckets", "дубликаты бакетов не допускаются"));
        return errors;
    }
}

// Заявки на откат (t07): откат возвращает бакет на прежний шард по живой
// обратной подписке — куда, определяет воркер (SQL-факт). Общий протокол
// постановки — MoveTickets (§9.7 п.3–5); guard'ы — Д4 (перепроверит процесс).
public sealed class RollbackBucketsHandler(IEtcdGateway gateway, string[] endpoints, TimeProvider time)
{
    public async Task<Result<RollbackQueuedDto>> HandleAsync(
        string cluster, RollbackBucketsRequest command, string requestedBy, CancellationToken ct)
    {
        // 1) Валидация тела (400) и каноничность кластера (404).
        var errors = RollbackBucketsValidator.Validate(command);
        if (errors.Count > 0)
            return Result<RollbackQueuedDto>.Failed(new MoveOpValidationException(errors));
        if (!CreateClusterLimits.NamePattern().IsMatch(cluster))
            return Result<RollbackQueuedDto>.Failed(new ClusterNotFoundException(cluster));

        // 2) Guard-данные кластера одним range: сбой → 503; нет config → 404;
        //    state не null → 409; битый → 503.
        var data = await ClusterGuardData.ReadAsync(gateway, endpoints, cluster, ct);
        if (!data.IsSuccess)
            return Result<RollbackQueuedDto>.Failed(data.Error!);
        var info = data.Value;
        if (info.ConfigRaw is null)
            return Result<RollbackQueuedDto>.Failed(new ClusterNotFoundException(cluster));
        string? state;
        int bucketsCount;
        try
        {
            state = ReadState(info.ConfigRaw);
            bucketsCount = ReadBuckets(info.ConfigRaw);
        }
        catch (JsonException)
        {
            return Result<RollbackQueuedDto>.Failed(new InvalidClusterConfigException(cluster));
        }
        if (state is not null)
            return Result<RollbackQueuedDto>.Failed(new ClusterNotActiveException(cluster, state));

        // 3) Guard'ы (Д4): нешардированный; каждый бакет в диапазоне, с routing,
        //    в ACTIVE — rollback возможен только из ACTIVE (§9.7.2).
        if (bucketsCount == 1 && info.Shards.Count <= 1)
            return Result<RollbackQueuedDto>.Failed(new NonShardedClusterException(cluster));
        var ordered = command.Buckets!.Distinct().OrderBy(id => id).ToList();
        foreach (var id in ordered)
        {
            if (id < 0 || id >= bucketsCount || !info.Routing.TryGetValue(id, out var owner))
                return Result<RollbackQueuedDto>.Failed(
                    BucketNotActiveForMoveOpException.OutOfRange("rollback", id));
            var bucketState = info.Status.TryGetValue(id, out var st)
                ? st.State ?? ClusterGuardData.ActiveState
                : ClusterGuardData.ActiveState;
            if (bucketState != ClusterGuardData.ActiveState)
                return Result<RollbackQueuedDto>.Failed(
                    BucketNotActiveForMoveOpException.RollbackOrFinalize("rollback", id, owner, bucketState));
        }

        // 4) Очередь напрямую (§9.7 п.3): идентичная op=rollback → skipped;
        //    иная → 409; база — глобальный max (п.4).
        var queue = await MoveTickets.ReadQueueAsync(gateway, endpoints, cluster, ct);
        if (!queue.IsSuccess)
            return Result<RollbackQueuedDto>.Failed(queue.Error!);
        var skipped = new List<int>();
        var toQueue = new List<int>();
        foreach (var id in ordered)
        {
            if (queue.Value.Mine.TryGetValue($"bucket_{id}", out var existing))
            {
                if (existing.Op == "rollback")
                    skipped.Add(id);
                else
                    return Result<RollbackQueuedDto>.Failed(
                        new MoveRequestConflictException($"bucket_{id}", existing.Op, existing.To));
            }
            else
            {
                toQueue.Add(id);
            }
        }

        // 5) base = max(now, maxUnix+1), k-я заявка — base+k; txn-клэйм per key.
        var now = time.GetUtcNow().ToUnixTimeSeconds();
        var unixBase = Math.Max(now, queue.Value.MaxUnix + 1);
        var queued = new List<int>();
        foreach (var (id, k) in toQueue.Select((b, i) => (b, i)))
        {
            var key = $"/pgworker/moves/{cluster}/bucket_{id}";
            var json = System.Text.Json.JsonSerializer.Serialize(
                new MoveTickets.TicketBody("rollback", null, null, null, unixBase + k, requestedBy),
                MoveTickets.TicketJson);
            var claim = await MoveTickets.ClaimAsync(gateway, endpoints, key, json, ct);
            if (!claim.IsSuccess)
                return Result<RollbackQueuedDto>.Failed(claim.Error!); // 503, без компенсации (Д5)
            if (!claim.Value.Succeeded)
                return Result<RollbackQueuedDto>.Failed(new MoveClaimLostException(id));
            queued.Add(id);
        }

        return Result<RollbackQueuedDto>.Success(new RollbackQueuedDto(cluster, queued, skipped));
    }

    private static string? ReadState(string raw)
    {
        using var doc = System.Text.Json.JsonDocument.Parse(raw);
        return doc.RootElement.TryGetProperty("state", out var s) && s.ValueKind == System.Text.Json.JsonValueKind.String
            ? s.GetString()
            : null;
    }

    private static int ReadBuckets(string raw)
    {
        using var doc = System.Text.Json.JsonDocument.Parse(raw);
        return doc.RootElement.TryGetProperty("buckets", out var b) && b.ValueKind == System.Text.Json.JsonValueKind.Number
            ? b.GetInt32()
            : 0;
    }
}
```

(Форматирование `using` — по канону проекта: вынести `System.Text.Json` в using сверху, как у `MoveBucketsHandler`.)

- [ ] **Шаг 5. Зарегистрировать маршрут + DI**

`ApiModule.cs` (после существующего `POST .../moves`):

```csharp
// POST /api/clusters/{cluster}/moves/rollback — заявки на откат (t07, 02 §9.7.2):
// направление определяет воркер по обратной подписке; общий протокол §9.7 п.1–5.
endpoints.MapPost("/api/clusters/{cluster}/moves/rollback", async (
    string cluster, RollbackBucketsRequest request, HttpRequest http, RollbackBucketsHandler handler, CancellationToken ct) =>
{
    var requestedBy = http.Headers.TryGetValue("X-Requested-By", out var by)
        && !string.IsNullOrWhiteSpace(by)
        ? by.ToString()
        : "api";
    var result = await handler.HandleAsync(cluster, request, requestedBy, ct);
    if (result.IsSuccess)
        return Results.Created((string?)null, result.Value);

    return result.Error switch
    {
        MoveOpValidationException validation => Results.Problem(
            statusCode: StatusCodes.Status400BadRequest,
            title: "Validation failed",
            detail: result.Error.Message,
            extensions: new Dictionary<string, object?>
            {
                ["errors"] = validation.Errors.ToDictionary(e => e.Field, e => new[] { e.Message }),
            }),
        ClusterNotFoundException => Results.Problem(
            statusCode: StatusCodes.Status404NotFound, title: "Not found", detail: result.Error.Message),
        ClusterNotActiveException or NonShardedClusterException or BucketNotActiveForMoveOpException
            or MoveRequestConflictException or MoveClaimLostException => Results.Problem(
            statusCode: StatusCodes.Status409Conflict, title: "Move ops rejected", detail: result.Error.Message),
        _ => Results.Problem(
            statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed", detail: result.Error!.Message),
    };
});
```

`Program.cs` (рядом с `MoveBucketsHandler`):

```csharp
builder.Services.AddSingleton(sp => new RollbackBucketsHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<TimeProvider>()));
```

- [ ] **Шаг 6. Запустить — PASS + регресс**

Run: `dotnet test src/tests/PgWorker.IntegrationTests --filter "FullyQualifiedName~MoveOpsApiTests"` → PASS; `--filter "FullyQualifiedName~MovesApiTests"` → PASS; `dotnet build src/PgWorker.slnx` → 0 warnings.

- [ ] **Шаг 7. Commit**

```bash
git add src/PgWorker.App/Api/Operations/ src/PgWorker.App/Api/ApiModule.cs src/PgWorker.App/Program.cs src/tests/PgWorker.IntegrationTests/Api/
git commit -m "feat(t07): POST /moves/rollback — заявки отката (guard'ы ACTIVE, идентичность op=rollback, 400/404/409/503)"
```

---

### Task A4: `FinalizeBucketHandler` + маршрут finalize

`POST /api/clusters/{c}/moves/finalize` (arch/02 §9.7.3, arch/03 §1.8): одиночная заявка уборки артефактов на `oldShard` (DROP SCHEMA СО ДАННЫМИ).

**Files:**
- Create: `src/PgWorker.App/Api/Operations/FinalizeBucketHandler.cs`
- Modify: `src/PgWorker.App/Api/ApiModule.cs`, `src/PgWorker.App/Program.cs`
- Test: `src/tests/PgWorker.IntegrationTests/Api/MoveOpsApiTests.cs` (добавить finalize-кейсы)

**Interfaces:**
- Consumes: `MoveTickets.*` (A1), `ClusterGuardData` (A2), исключения A3 (`MoveOpValidationException`, `BucketNotActiveForMoveOpException.RollbackOrFinalize/OutOfRange`, `FinalizeTargetIsOwnerException`), `ShardNotFoundException`, `MoveRequestConflictException`, `MoveClaimLostException`.
- Produces: `public sealed record FinalizeBucketRequest(int? Bucket, string? OldShard)`; `public sealed record BucketFinalizeQueuedDto(string Cluster, int Bucket, string OldShard)`; `public sealed class FinalizeBucketHandler(IEtcdGateway gateway, string[] endpoints, TimeProvider time)` с `HandleAsync(string cluster, FinalizeBucketRequest command, string requestedBy, CancellationToken ct)`.
- Семантика идентичности (спека §4.2): стоящая `op=finalize` с тем же `old_shard` → 201 без записи (DTO тот же); иная заявка → 409.

- [ ] **Шаг 1. Failing-тесты (добавить в `MoveOpsApiTests.cs`)**

```csharp
// ===== finalize (§9.7.3) =====

// AAA: finalize-заявка ставится с old_shard; ключ каноничен.
[Fact]
public async Task Finalize_QueuesTicket_WithOldShard()
{
    // Arrange — 4×2, bucket_0 на shard1; убираем артефакты на shard2
    await ApiTestSeed.SeedActiveClusterAsync(Etcd, "fin", buckets: 4, shards: 2);
    var ct = TestContext.Current.CancellationToken;

    // Act
    var resp = await Client.PostAsJsonAsync("/api/clusters/fin/moves/finalize",
        new { bucket = 0, oldShard = "shard2" }, ct);

    // Assert
    resp.StatusCode.Should().Be(HttpStatusCode.Created);
    var body = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
    body.GetProperty("oldShard").GetString().Should().Be("shard2");
    var ticket = await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/pgworker/moves/fin/bucket_0", ct);
    ticket.Value!.Value.Should().Contain("\"op\":\"finalize\"")
        .And.Contain("\"old_shard\":\"shard2\"");
}

// AAA: oldShard = текущему владельцу → 409 «убирать нечего».
[Fact]
public async Task Finalize_OldShardIsOwner_409()
{
    // Arrange — bucket_0 на shard1
    await ApiTestSeed.SeedActiveClusterAsync(Etcd, "finown", buckets: 4, shards: 2);
    var ct = TestContext.Current.CancellationToken;

    // Act
    var resp = await Client.PostAsJsonAsync("/api/clusters/finown/moves/finalize",
        new { bucket = 0, oldShard = "shard1" }, ct);

    // Assert
    resp.StatusCode.Should().Be(HttpStatusCode.Conflict);
    var problem = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
    problem.GetProperty("detail").GetString().Should().Contain("убирать нечего");
}

// AAA: oldShard не существует → 404; TO_REMOVE-приёмник допустим → 201.
[Fact]
public async Task Finalize_UnknownShard_409()
{
    // Arrange
    await ApiTestSeed.SeedActiveClusterAsync(Etcd, "fin404", buckets: 4, shards: 2);
    var ct = TestContext.Current.CancellationToken;

    // Act
    var resp = await Client.PostAsJsonAsync("/api/clusters/fin404/moves/finalize",
        new { bucket = 0, oldShard = "shard9" }, ct);

    // Assert
    resp.StatusCode.Should().Be(HttpStatusCode.NotFound);
}

[Fact]
public async Task Finalize_OldShardToRemove_201()
{
    // Arrange — shard2 в демонтаже: финализация перед удалением допустима
    await ApiTestSeed.SeedActiveClusterAsync(Etcd, "finrm", buckets: 4, shards: 2);
    var ct = TestContext.Current.CancellationToken;
    await Etcd.Gateway.PutAsync(Etcd.Endpoint, "/clusters/finrm/shards/shard2/state",
        "TO_REMOVE", null, ct);

    // Act
    var resp = await Client.PostAsJsonAsync("/api/clusters/finrm/moves/finalize",
        new { bucket = 0, oldShard = "shard2" }, ct);

    // Assert
    resp.StatusCode.Should().Be(HttpStatusCode.Created);
}
```

- [ ] **Шаг 2. Запустить — FAIL (404: маршрута нет)**

Run: `dotnet test src/tests/PgWorker.IntegrationTests --filter "FullyQualifiedName~MoveOpsApiTests"` → finalize-кейсы FAIL.

- [ ] **Шаг 3. Реализовать `FinalizeBucketHandler.cs`**

Порт структуры `RollbackBucketsHandler` (шаг 4 задачи A3), отличия:

```csharp
// Тело POST /api/clusters/{cluster}/moves/finalize (arch/02 §9.7.3).
public sealed record FinalizeBucketRequest(int? Bucket, string? OldShard);

// Ответ 201: одиночная заявка уборки артефактов на oldShard.
public sealed record BucketFinalizeQueuedDto(string Cluster, int Bucket, string OldShard);

// Валидация тела (§9.7.3): bucket обязателен, oldShard непустой.
public static class FinalizeBucketValidator
{
    public static IReadOnlyList<ValidationError> Validate(FinalizeBucketRequest request)
    {
        var errors = new List<ValidationError>();
        if (request.Bucket is null)
            errors.Add(new("bucket", "укажите бакет"));
        if (string.IsNullOrWhiteSpace(request.OldShard))
            errors.Add(new("oldShard", "шард обязателен"));
        return errors;
    }
}
```

В `HandleAsync` (результат `Result<BucketFinalizeQueuedDto>`):
1) `FinalizeBucketValidator.Validate` → `MoveOpValidationException`; каноничность кластера → 404;
2) guard-данные (как A3 шаг 4 п.2);
3) нешардированный → 409; `var id = command.Bucket!.Value;`: вне диапазона/без routing → `BucketNotActiveForMoveOpException.OutOfRange("finalize", id)`; state ≠ ACTIVE → `BucketNotActiveForMoveOpException.RollbackOrFinalize("finalize", id, owner, state)`;
4) `!info.Shards.Contains(command.OldShard!)` → `ShardNotFoundException(cluster, command.OldShard!)`; `command.OldShard == routing owner` → `FinalizeTargetIsOwnerException(cluster, id, command.OldShard!)`. TO_REMOVE-приёмник допустим (guard не ставится);
5) очередь: `existing.Op == "finalize" && existing.OldShard == command.OldShard` → 201 без записи (DTO тот же); иная → `MoveRequestConflictException($"bucket_{id}", existing.Op, existing.To)`;
6) клэйм: `new MoveTickets.TicketBody("finalize", null, command.OldShard, null, unixBase, requestedBy)` (одна заявка, `k=0`);
7) `Result.Success(new BucketFinalizeQueuedDto(cluster, id, command.OldShard!))`.

- [ ] **Шаг 4. Маршрут + DI**

`ApiModule.cs` — тот же паттерн, что у rollback (шаг 5 задачи A3): `MapPost("/api/clusters/{cluster}/moves/finalize", ...)`; switch: `MoveOpValidationException` → 400+errors; `ClusterNotFoundException or ShardNotFoundException` → 404; `ClusterNotActiveException or NonShardedClusterException or BucketNotActiveForMoveOpException or FinalizeTargetIsOwnerException or MoveRequestConflictException or MoveClaimLostException` → 409 `"Move ops rejected"`; `_` → 503.
`Program.cs`: `AddSingleton(sp => new FinalizeBucketHandler(gateway, endpoints, time))` — по образцу A3.

- [ ] **Шаг 5. Запустить — PASS + регресс**

Run: `dotnet test src/tests/PgWorker.IntegrationTests --filter "FullyQualifiedName~MoveOpsApiTests"` → PASS; `dotnet build src/PgWorker.slnx` → 0 warnings.

- [ ] **Шаг 6. Commit**

```bash
git add src/PgWorker.App/Api/Operations/FinalizeBucketHandler.cs src/PgWorker.App/Api/ApiModule.cs src/PgWorker.App/Program.cs src/tests/PgWorker.IntegrationTests/Api/MoveOpsApiTests.cs
git commit -m "feat(t07): POST /moves/finalize — заявка уборки oldShard (409 владелец, 404 шард, TO_REMOVE допустим)"
```

---

### Task A5: `AbortBucketHandler` + маршрут abort

`POST /api/clusters/{c}/moves/abort` (arch/02 §9.7.4, arch/03 §1.9): отмена незавершённого переезда с быстрыми пред-проверками `force` (свежесть `AbortMinAgeSec`, routing==target). Порог — из DI `MovesRuntimeOptions` (единый источник с процессом).

**Files:**
- Create: `src/PgWorker.App/Api/Operations/AbortBucketHandler.cs`
- Modify: `src/PgWorker.App/Api/ApiModule.cs`, `src/PgWorker.App/Program.cs`
- Test: `src/tests/PgWorker.IntegrationTests/Api/MoveOpsApiTests.cs` (abort-кейсы)

**Interfaces:**
- Consumes: `MoveTickets.*`, `ClusterGuardData` (вкл. `UpdatedUnix` из A2), `MovesRuntimeOptions` (`PgWorker.Moves`), исключения A3 (`AbortActive`/`AbortNotInitialized`/`OutOfRange`, `MoveStatusFreshException`, `MoveAlreadyFlippedException`).
- Produces: `public sealed record AbortBucketRequest(int? Bucket, bool? Force)`; `public sealed record BucketAbortQueuedDto(string Cluster, int Bucket, bool Force)`; `public sealed class AbortBucketHandler(IEtcdGateway gateway, string[] endpoints, TimeProvider time, MovesRuntimeOptions moves)`.

- [ ] **Шаг 1. Failing-тесты (добавить в `MoveOpsApiTests.cs`)**

```csharp
// ===== abort (§9.7.4) =====

// AAA: abort ставит заявку с force:true только при force; иначе force в JSON нет.
[Fact]
public async Task Abort_QueuesTicket_ForceOnlyWhenTrue()
{
    // Arrange — зависший SYNCING-статус (несвежий: updated_unix = now-300)
    await ApiTestSeed.SeedActiveClusterAsync(Etcd, "ab", buckets: 4, shards: 2);
    var ct = TestContext.Current.CancellationToken;
    await ApiTestSeed.SeedBucketStatusAsync(Etcd, "ab", 0, "SYNCING", "shard1", "shard2",
        DateTimeOffset.UtcNow.AddSeconds(-300).ToUnixTimeSeconds());

    // Act
    var resp = await Client.PostAsJsonAsync("/api/clusters/ab/moves/abort",
        new { bucket = 0 }, ct);

    // Assert — force не пишется (null-поле опускается, канон §4.2)
    resp.StatusCode.Should().Be(HttpStatusCode.Created);
    var body = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
    body.GetProperty("force").GetBoolean().Should().BeFalse();
    var ticket = await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/pgworker/moves/ab/bucket_0", ct);
    ticket.Value!.Value.Should().Contain("\"op\":\"abort\"").And.NotContain("\"force\"");
}

// AAA: свежий статус без force → 409 (текст AbortMinAgeSec); с force → 201.
[Fact]
public async Task Abort_FreshStatus_409ThenForce_201()
{
    // Arrange — SYNCING, updated_unix = now (свежий)
    await ApiTestSeed.SeedActiveClusterAsync(Etcd, "abfr", buckets: 4, shards: 2);
    var ct = TestContext.Current.CancellationToken;
    await ApiTestSeed.SeedBucketStatusAsync(Etcd, "abfr", 0, "SYNCING", "shard1", "shard2",
        DateTimeOffset.UtcNow.ToUnixTimeSeconds());

    // Act
    var noForce = await Client.PostAsJsonAsync("/api/clusters/abfr/moves/abort",
        new { bucket = 0 }, ct);

    // Assert — 409, текст — порт процесса
    noForce.StatusCode.Should().Be(HttpStatusCode.Conflict);
    var problem = await noForce.Content.ReadFromJsonAsync<JsonElement>(ct);
    problem.GetProperty("detail").GetString().Should().Contain("AbortMinAgeSec").And.Contain("force");

    // Act — с force
    var forced = await Client.PostAsJsonAsync("/api/clusters/abfr/moves/abort",
        new { bucket = 0, force = true }, ct);

    // Assert — 201, в ключе force:true
    forced.StatusCode.Should().Be(HttpStatusCode.Created);
    var ticket = await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/pgworker/moves/abfr/bucket_0", ct);
    ticket.Value!.Value.Should().Contain("\"force\":true");
}

// AAA: routing==target без force → 409 «осознанно: force»; с force → 201.
[Fact]
public async Task Abort_RoutingEqualsTarget_409ThenForce_201()
{
    // Arrange — SYNCING, владелец shard1 == target (flip прошёл, статус завис)
    await ApiTestSeed.SeedActiveClusterAsync(Etcd, "abfl", buckets: 4, shards: 2);
    var ct = TestContext.Current.CancellationToken;
    await ApiTestSeed.SeedBucketStatusAsync(Etcd, "abfl", 0, "SYNCING", "shard1", "shard1",
        DateTimeOffset.UtcNow.AddSeconds(-300).ToUnixTimeSeconds());

    // Act
    var noForce = await Client.PostAsJsonAsync("/api/clusters/abfl/moves/abort",
        new { bucket = 0 }, ct);

    // Assert
    noForce.StatusCode.Should().Be(HttpStatusCode.Conflict);
    var problem = await noForce.Content.ReadFromJsonAsync<JsonElement>(ct);
    problem.GetProperty("detail").GetString().Should().Contain("осознанно");

    // Act / Assert — с force
    var forced = await Client.PostAsJsonAsync("/api/clusters/abfl/moves/abort",
        new { bucket = 0, force = true }, ct);
    forced.StatusCode.Should().Be(HttpStatusCode.Created);
}

// AAA: ACTIVE-бакет (нет статуса) → 409 «пост-flip артефакты убирает finalize»;
// NOT_INITIALIZED → 409 «не переезд».
[Fact]
public async Task Abort_ActiveBucket_409FinalizeHint()
{
    // Arrange — bucket_0 без статус-ключа = ACTIVE
    await ApiTestSeed.SeedActiveClusterAsync(Etcd, "abact", buckets: 4, shards: 2);
    var ct = TestContext.Current.CancellationToken;

    // Act
    var resp = await Client.PostAsJsonAsync("/api/clusters/abact/moves/abort",
        new { bucket = 0 }, ct);

    // Assert
    resp.StatusCode.Should().Be(HttpStatusCode.Conflict);
    var problem = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
    problem.GetProperty("detail").GetString().Should().Contain("finalize");
}

[Fact]
public async Task Abort_NotInitializedBucket_409NotAMove()
{
    // Arrange
    await ApiTestSeed.SeedActiveClusterAsync(Etcd, "abni", buckets: 4, shards: 2);
    var ct = TestContext.Current.CancellationToken;
        // target для NOT_INITIALIZED не важен — ветка «не переезд» раньше проверок target
        await ApiTestSeed.SeedBucketStatusAsync(Etcd, "abni", 0, "NOT_INITIALIZED", "shard1", "",
            DateTimeOffset.UtcNow.AddSeconds(-300).ToUnixTimeSeconds());

    // Act
    var resp = await Client.PostAsJsonAsync("/api/clusters/abni/moves/abort",
        new { bucket = 0 }, ct);

    // Assert
    resp.StatusCode.Should().Be(HttpStatusCode.Conflict);
    var problem = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
    problem.GetProperty("detail").GetString().Should().Contain("не переезд");
}
```

- [ ] **Шаг 2. Запустить — FAIL**

Run: `dotnet test src/tests/PgWorker.IntegrationTests --filter "FullyQualifiedName~MoveOpsApiTests"` → abort-кейсы FAIL (маршрута нет).

- [ ] **Шаг 3. Реализовать `AbortBucketHandler.cs`**

```csharp
using System.Text.Json;
using PgWorker.Core;
using PgWorker.Core.Writing;
using PgWorker.Etcd.Client;
using PgWorker.Moves;

namespace PgWorker.App.Api.Operations;

// Тело POST /api/clusters/{cluster}/moves/abort (arch/02 §9.7.4): force —
// nullable (null = false; в JSON пишется только true).
public sealed record AbortBucketRequest(int? Bucket, bool? Force);

// Ответ 201.
public sealed record BucketAbortQueuedDto(string Cluster, int Bucket, bool Force);

// Валидация тела (§9.7.4): bucket обязателен.
public static class AbortBucketValidator
{
    public static IReadOnlyList<ValidationError> Validate(AbortBucketRequest request)
    {
        var errors = new List<ValidationError>();
        if (request.Bucket is null)
            errors.Add(new("bucket", "укажите бакет"));
        return errors;
    }
}

// Заявка на отмену переезда (t07): быстрые пред-проверки семантики force по
// прямым чтениям etcd (Д4: свежесть AbortMinAgeSec по updated_unix,
// routing==target); авторитетно перепроверит AbortSequence. Порог — из
// MovesRuntimeOptions (единый источник с процессом, appsettings PgWorker:Moves).
public sealed class AbortBucketHandler(
    IEtcdGateway gateway, string[] endpoints, TimeProvider time, MovesRuntimeOptions moves)
{
    private static readonly HashSet<string> MoveStates = ["SYNCING", "FROZEN", "ABORTING"];

    public async Task<Result<BucketAbortQueuedDto>> HandleAsync(
        string cluster, AbortBucketRequest command, string requestedBy, CancellationToken ct)
    {
        // 1) Валидация тела (400) и каноничность кластера (404).
        var errors = AbortBucketValidator.Validate(command);
        if (errors.Count > 0)
            return Result<BucketAbortQueuedDto>.Failed(new MoveOpValidationException(errors));
        if (!CreateClusterLimits.NamePattern().IsMatch(cluster))
            return Result<BucketAbortQueuedDto>.Failed(new ClusterNotFoundException(cluster));

        // 2) Guard-данные кластера (как rollback/finalize).
        var data = await ClusterGuardData.ReadAsync(gateway, endpoints, cluster, ct);
        if (!data.IsSuccess)
            return Result<BucketAbortQueuedDto>.Failed(data.Error!);
        var info = data.Value;
        if (info.ConfigRaw is null)
            return Result<BucketAbortQueuedDto>.Failed(new ClusterNotFoundException(cluster));
        string? state;
        int bucketsCount;
        try
        {
            using var doc = JsonDocument.Parse(info.ConfigRaw);
            state = doc.RootElement.TryGetProperty("state", out var s) && s.ValueKind == JsonValueKind.String
                ? s.GetString() : null;
            bucketsCount = doc.RootElement.TryGetProperty("buckets", out var b) && b.ValueKind == JsonValueKind.Number
                ? b.GetInt32() : 0;
        }
        catch (JsonException)
        {
            return Result<BucketAbortQueuedDto>.Failed(new InvalidClusterConfigException(cluster));
        }
        if (state is not null)
            return Result<BucketAbortQueuedDto>.Failed(new ClusterNotActiveException(cluster, state));

        // 3) Guard'ы (§9.7.4): бакет в диапазоне и с routing; статус жив и state
        //    ∈ SYNCING/FROZEN/ABORTING (ACTIVE/NOT_INITIALIZED — 409 с подсказкой).
        var id = command.Bucket!.Value;
        var force = command.Force == true;
        if (id < 0 || id >= bucketsCount || !info.Routing.TryGetValue(id, out var owner))
            return Result<BucketAbortQueuedDto>.Failed(
                BucketNotActiveForMoveOpException.OutOfRange("abort", id));
        if (!info.Status.TryGetValue(id, out var status) || status.State == ClusterGuardData.ActiveState)
            return Result<BucketAbortQueuedDto>.Failed(BucketNotActiveForMoveOpException.AbortActive(id));
        if (status.State == "NOT_INITIALIZED")
            return Result<BucketAbortQueuedDto>.Failed(BucketNotActiveForMoveOpException.AbortNotInitialized(id));
        if (!MoveStates.Contains(status.State!))
            return Result<BucketAbortQueuedDto>.Failed(
                BucketNotActiveForMoveOpException.AbortActive(id));

        // 4) Пред-проверки force (порт AbortSequence; отсутствие updated_unix у
        //    старого ключа — пропускаем, авторитетно решит процесс, спека §5.3):
        //    свежесть статуса и routing==target.
        if (!force && status.UpdatedUnix is { } updated)
        {
            var age = time.GetUtcNow().ToUnixTimeSeconds() - updated;
            if (age < moves.AbortMinAgeSec)
                return Result<BucketAbortQueuedDto>.Failed(
                    new MoveStatusFreshException(age, moves.AbortMinAgeSec));
        }
        if (!force && status.Target is { } target && target == owner)
            return Result<BucketAbortQueuedDto>.Failed(new MoveAlreadyFlippedException(target));

        // 5) Очередь: идентичная (op=abort + тот же force) → 201 без записи;
        //    иная → 409 (панель не перезаписывает чужие заявки, §9.7).
        var queue = await MoveTickets.ReadQueueAsync(gateway, endpoints, cluster, ct);
        if (!queue.IsSuccess)
            return Result<BucketAbortQueuedDto>.Failed(queue.Error!);
        if (queue.Value.Mine.TryGetValue($"bucket_{id}", out var existing))
        {
            if (existing.Op == "abort" && existing.Force == force)
                return Result<BucketAbortQueuedDto>.Success(new BucketAbortQueuedDto(cluster, id, force));
            return Result<BucketAbortQueuedDto>.Failed(
                new MoveRequestConflictException($"bucket_{id}", existing.Op, existing.To));
        }

        // 6) Клэйм одной заявки: force:true пишется, false — опускается (§4.2).
        var now = time.GetUtcNow().ToUnixTimeSeconds();
        var unix = Math.Max(now, queue.Value.MaxUnix + 1);
        var key = $"/pgworker/moves/{cluster}/bucket_{id}";
        var json = JsonSerializer.Serialize(
            new MoveTickets.TicketBody("abort", null, null, force ? true : null, unix, requestedBy),
            MoveTickets.TicketJson);
        var claim = await MoveTickets.ClaimAsync(gateway, endpoints, key, json, ct);
        if (!claim.IsSuccess)
            return Result<BucketAbortQueuedDto>.Failed(claim.Error!); // 503, без компенсации (Д5)
        if (!claim.Value.Succeeded)
            return Result<BucketAbortQueuedDto>.Failed(new MoveClaimLostException(id));

        return Result<BucketAbortQueuedDto>.Success(new BucketAbortQueuedDto(cluster, id, force));
    }
}
```

- [ ] **Шаг 4. Маршрут + DI**

`ApiModule.cs`: `MapPost("/api/clusters/{cluster}/moves/abort", ...)` — switch как у finalize, плюс `MoveStatusFreshException or MoveAlreadyFlippedException` в ветке 409 `"Move ops rejected"`.
`Program.cs` (runtime-опции — как у `MoveProcess`, через `ToRuntime`):

```csharp
builder.Services.AddSingleton(sp => new AbortBucketHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Moves.ToRuntime(
        sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Thresholds)));
```

- [ ] **Шаг 5. Запустить — PASS + регресс**

Run: `dotnet test src/tests/PgWorker.IntegrationTests --filter "FullyQualifiedName~MoveOpsApiTests"` → PASS; `dotnet build src/PgWorker.slnx` → 0 warnings.

- [ ] **Шаг 6. Commit**

```bash
git add src/PgWorker.App/Api/Operations/AbortBucketHandler.cs src/PgWorker.App/Api/ApiModule.cs src/PgWorker.App/Program.cs src/tests/PgWorker.IntegrationTests/Api/MoveOpsApiTests.cs
git commit -m "feat(t07): POST /moves/abort — заявка отмены переезда (свежесть AbortMinAgeSec, routing==target, force из DI MovesRuntimeOptions)"
```

---

### Task A6: `CancelMoveHandler` + DELETE-маршрут отмена заявки

`DELETE /api/clusters/{c}/moves/{bucket}` (arch/02 §9.7.5, arch/03 §1.9): удаляет ключ стоящей заявки; НЕ останавливает взятую в работу.

**Files:**
- Create: `src/PgWorker.App/Api/Operations/CancelMoveHandler.cs`
- Modify: `src/PgWorker.App/Api/ApiModule.cs`, `src/PgWorker.App/Program.cs`
- Test: `src/tests/PgWorker.IntegrationTests/Api/MoveOpsApiTests.cs` (отмена-кейсы)

**Interfaces:**
- Consumes: `EtcdFailover.CallAsync`, `IEtcdGateway.GetAsync/DeleteAsync`, `CreateClusterLimits.NamePattern`, `ClusterNotFoundException`, `MoveTicketNotFoundException` (A3).
- Produces: `public sealed class CancelMoveHandler(IEtcdGateway gateway, string[] endpoints)` с `Task<Result> HandleAsync(string cluster, string bucket, CancellationToken ct)`.

- [ ] **Шаг 1. Failing-тесты (добавить в `MoveOpsApiTests.cs`)**

```csharp
// ===== отмена заявки (§9.7.5) =====

// AAA: отмена удаляет ключ заявки (204); повтор → 404 (не идемпотентна).
[Fact]
public async Task Cancel_DeletesTicket_204Then404()
{
    // Arrange — живая move-заявка на bucket_0
    await ApiTestSeed.SeedActiveClusterAsync(Etcd, "cn", buckets: 4, shards: 2);
    var ct = TestContext.Current.CancellationToken;
    await ApiTestSeed.SeedTicketAsync(Etcd, "cn", 0, "move", to: "shard2");

    // Act
    var resp = await Client.DeleteAsync("/api/clusters/cn/moves/bucket_0", ct);

    // Assert — 204, ключ исчез
    resp.StatusCode.Should().Be(HttpStatusCode.NoContent);
    var ticket = await Etcd.Gateway.GetAsync(Etcd.Endpoint, "/pgworker/moves/cn/bucket_0", ct);
    ticket.Value.Should().BeNull();

    // Act — повтор
    var again = await Client.DeleteAsync("/api/clusters/cn/moves/bucket_0", ct);

    // Assert — 404 «заявки нет»
    again.StatusCode.Should().Be(HttpStatusCode.NotFound);
    var problem = await again.Content.ReadFromJsonAsync<JsonElement>(ct);
    problem.GetProperty("detail").GetString().Should().Contain("заявки");
}

// AAA: чужой кластер и битый leaf → 404.
[Fact]
public async Task Cancel_UnknownClusterOrBadLeaf_404()
{
    // Arrange
    var ct = TestContext.Current.CancellationToken;

    // Act / Assert — каноничное имя, ключа нет
    var other = await Client.DeleteAsync("/api/clusters/nowhere/moves/bucket_0", ct);
    other.StatusCode.Should().Be(HttpStatusCode.NotFound);

    // Act / Assert — неканонический leaf
    await ApiTestSeed.SeedActiveClusterAsync(Etcd, "cnbad", buckets: 4, shards: 2);
    var badLeaf = await Client.DeleteAsync("/api/clusters/cnbad/moves/not-a-bucket", ct);
    badLeaf.StatusCode.Should().Be(HttpStatusCode.NotFound);
}
```

- [ ] **Шаг 2. Запустить — FAIL**

Run: `dotnet test src/tests/PgWorker.IntegrationTests --filter "FullyQualifiedName~MoveOpsApiTests"` → отмена-кейсы FAIL.

- [ ] **Шаг 3. Реализовать `CancelMoveHandler.cs`**

```csharp
using System.Text.RegularExpressions;
using PgWorker.Core;
using PgWorker.Etcd.Client;

namespace PgWorker.App.Api.Operations;

// Отмена стоящей заявки: DELETE /api/clusters/{cluster}/moves/{bucket}
// (t07, arch/02 §9.7.5). Удаление НЕ останавливает взятую в работу заявку —
// процесс ведёт фазы по статус-ключу и доедет до конца; остановка начатого —
// только abort. State кластера не проверяется (TO_REMOVE: заявки чистит D2 —
// ручная отмена безвредна). Идемпотентностью не обладает (повтор → 404).
public sealed partial class CancelMoveHandler(IEtcdGateway gateway, string[] endpoints)
{
    // Канонический leaf заявки: bucket_<int> без ведущих нулей.
    [GeneratedRegex("^bucket_(0|[1-9][0-9]*)$")]
    private static partial Regex BucketLeafPattern();

    public async Task<Result> HandleAsync(string cluster, string bucket, CancellationToken ct)
    {
        // 1) Имена канонические — иначе 404 (§9.7.5).
        if (!CreateClusterLimits.NamePattern().IsMatch(cluster))
            return Result.Failed(new ClusterNotFoundException(cluster));
        if (!BucketLeafPattern().IsMatch(bucket))
            return Result.Failed(new MoveTicketNotFoundException(cluster, bucket));

        // 2) Чтение ключа напрямую одним get: нет → 404 «заявки нет».
        var key = $"/pgworker/moves/{cluster}/{bucket}";
        var existing = await EtcdFailover.CallAsync(endpoints,
            endpoint => gateway.GetAsync(endpoint, key, ct));
        if (!existing.IsSuccess)
            return Result.Failed(existing.Error!);
        if (existing.Value is null)
            return Result.Failed(new MoveTicketNotFoundException(cluster, bucket));

        // 3) del ключа → успех (204 ставит маршрут).
        var deleted = await EtcdFailover.CallAsync(endpoints,
            endpoint => gateway.DeleteAsync(endpoint, key, prefix: false, ct));
        return deleted.IsSuccess ? Result.Success() : Result.Failed(deleted.Error!);
    }
}
```

- [ ] **Шаг 4. Маршрут + DI**

`ApiModule.cs`:

```csharp
// DELETE /api/clusters/{cluster}/moves/{bucket} — отмена стоящей заявки (t07,
// 02 §9.7.5): 204; ключа нет/имена неканонические → 404; сбой etcd → 503.
endpoints.MapDelete("/api/clusters/{cluster}/moves/{bucket}", async (
    string cluster, string bucket, CancelMoveHandler handler, CancellationToken ct) =>
{
    var result = await handler.HandleAsync(cluster, bucket, ct);
    if (result.IsSuccess)
        return Results.NoContent();

    return result.Error switch
    {
        ClusterNotFoundException or MoveTicketNotFoundException => Results.Problem(
            statusCode: StatusCodes.Status404NotFound, title: "Not found", detail: result.Error.Message),
        _ => Results.Problem(
            statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed", detail: result.Error!.Message),
    };
});
```

`Program.cs`: `AddSingleton(sp => new CancelMoveHandler(gateway, endpoints))`.

- [ ] **Шаг 5. Запустить — PASS + регресс Moves/MovesApiTests**

Run: `dotnet test src/tests/PgWorker.IntegrationTests --filter "FullyQualifiedName~Move"` → PASS (оба класса: MovesApiTests + MoveOpsApiTests); `dotnet build src/PgWorker.slnx` → 0 warnings.

- [ ] **Шаг 6. Commit**

```bash
git add src/PgWorker.App/Api/Operations/CancelMoveHandler.cs src/PgWorker.App/Api/ApiModule.cs src/PgWorker.App/Program.cs src/tests/PgWorker.IntegrationTests/Api/MoveOpsApiTests.cs
git commit -m "feat(t07): DELETE /moves/{bucket} — отмена стоящей заявки (404 «заявки нет», без остановки начатого)"
```

---

### Task A7: Юнит-тесты: валидаторы + guard-логика на моках + roundtrip канонического JSON

Спека §7: валидаторы тел (rollback/finalize/abort), guard-логика новых handler'ов на моках gateway (быстрее интеграционных, ловят тексты) + канонический JSON заявок (тело, которое пишет handler, парсится процессом).

**Files:**
- Test: `src/tests/PgWorker.UnitTests/Api/MoveOpsHandlersTests.cs` (новый)
- Test: `src/tests/PgWorker.UnitTests/Moves/MoveOpsTicketJsonTests.cs` (новый)

**Interfaces:**
- Consumes: `RollbackBucketsValidator`/`FinalizeBucketValidator`/`AbortBucketValidator`, `RollbackBucketsHandler`, `FinalizeBucketHandler`, `AbortBucketHandler`, `CancelMoveHandler` (A3–A6), `FakeEtcdGateway` (A2), `MoveRequest.Parse` (`PgWorker.Moves`), `MoveTickets.TicketBody/TicketJson` (A1).

- [ ] **Шаг 1. Написать `MoveOpsHandlersTests.cs`**

Общий сид-хелпер внутри класса: `SeedActiveClusterAsync(FakeEtcdGateway gw, string name, int buckets, int shards)` (порт `ApiTestSeed.SeedActiveClusterAsync` на fake: config/shards/routing). Кейсы (все AAA):

```csharp
using PgWorker.App.Api.Operations;
using PgWorker.Moves;
using FluentAssertions;
using Xunit;

namespace PgWorker.UnitTests.Api;

// Guard-логика move-ops handler'ов на моках gateway (t07, спека §7): тексты
// и ветки 409/404 без docker; авторитетные перепроверки — у процесса (t01).
public class MoveOpsHandlersTests
{
    private const string Ep = "http://etcd";

    private static async Task<FakeEtcdGateway> SeedClusterAsync(string name, int buckets = 4, int shards = 2)
    {
        var gw = new FakeEtcdGateway();
        await gw.PutAsync(Ep, $"/clusters/{name}/config",
            $$"""{"buckets":{{buckets}},"dbname":"{{name}}","created_unix":1756000000}""", null, CancellationToken.None);
        for (var s = 1; s <= shards; s++)
        {
            await gw.PutAsync(Ep, $"/clusters/{name}/shards/shard{s}/replicas", "1", null, CancellationToken.None);
            for (var i = 0; i < buckets; i++)
                if (i % shards == s - 1)
                    await gw.PutAsync(Ep, $"/clusters/{name}/buckets/routing/bucket_{i}", $"shard{s}", null, CancellationToken.None);
        }
        return gw;
    }

    [Fact]
    public async Task Rollback_NotActiveBucket_409WithStateText()
    {
        // Arrange — SYNCING-статус на bucket_0 (возраст не важен для rollback)
        var gw = await SeedClusterAsync("c");
        await gw.PutAsync(Ep, "/clusters/c/buckets/status/bucket_0",
            """{"state":"FROZEN","owner":"shard1","target":"shard2","updated_unix":100}""", null, CancellationToken.None);
        var handler = new RollbackBucketsHandler(gw, [Ep], TimeProvider.System);

        // Act
        var result = await handler.HandleAsync("c",
            new RollbackBucketsRequest([0]), "ops", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<BucketNotActiveForMoveOpException>()
            .Which.Message.Should().Contain("только из ACTIVE").And.Contain("FROZEN");
    }

    [Fact]
    public async Task Finalize_TargetIsOwner_409()
    {
        // Arrange — bucket_0 принадлежит shard1
        var gw = await SeedClusterAsync("c");
        var handler = new FinalizeBucketHandler(gw, [Ep], TimeProvider.System);

        // Act
        var result = await handler.HandleAsync("c",
            new FinalizeBucketRequest(0, "shard1"), "ops", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<FinalizeTargetIsOwnerException>();
    }

    [Fact]
    public async Task Abort_FreshStatus_409WithThreshold()
    {
        // Arrange — updated_unix = now-10 (< AbortMinAgeSec=120 дефолта)
        var gw = await SeedClusterAsync("c");
        await gw.PutAsync(Ep, "/clusters/c/buckets/status/bucket_0",
            $$"""{"state":"SYNCING","owner":"shard1","target":"shard2","updated_unix":{{DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 10}}}""", null, CancellationToken.None);
        var handler = new AbortBucketHandler(gw, [Ep], TimeProvider.System, new MovesRuntimeOptions());

        // Act
        var result = await handler.HandleAsync("c", new AbortBucketRequest(0, null), "ops", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<MoveStatusFreshException>()
            .Which.Message.Should().Contain("AbortMinAgeSec=120");
    }

    [Fact]
    public async Task Abort_NoUpdatedUnix_FreshnessSkipped_201()
    {
        // Arrange — старый формат ключа без updated_unix: пред-проверка
        // пропускается (спека §5.3), заявка ставится
        var gw = await SeedClusterAsync("c");
        await gw.PutAsync(Ep, "/clusters/c/buckets/status/bucket_0",
            """{"state":"ABORTING","owner":"shard1","target":"shard2"}""", null, CancellationToken.None);
        var handler = new AbortBucketHandler(gw, [Ep], TimeProvider.System, new MovesRuntimeOptions());

        // Act
        var result = await handler.HandleAsync("c", new AbortBucketRequest(0, null), "ops", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        gw.Store["/pgworker/moves/c/bucket_0"].Should().Contain("\"op\":\"abort\"");
    }

    [Fact]
    public async Task Abort_RoutingEqualsTarget_409()
    {
        // Arrange — target == routing.owner (bucket_0 на shard1)
        var gw = await SeedClusterAsync("c");
        await gw.PutAsync(Ep, "/clusters/c/buckets/status/bucket_0",
            """{"state":"SYNCING","owner":"shard1","target":"shard1","updated_unix":100}""", null, CancellationToken.None);
        var handler = new AbortBucketHandler(gw, [Ep], TimeProvider.System, new MovesRuntimeOptions());

        // Act
        var result = await handler.HandleAsync("c", new AbortBucketRequest(0, null), "ops", CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<MoveAlreadyFlippedException>();
    }

    [Fact]
    public async Task Cancel_MissingTicket_404()
    {
        // Arrange
        var gw = await SeedClusterAsync("c");
        var handler = new CancelMoveHandler(gw, [Ep]);

        // Act
        var result = await handler.HandleAsync("c", "bucket_0", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<MoveTicketNotFoundException>();
    }

    [Fact]
    public async Task Cancel_LiveTicket_Deleted()
    {
        // Arrange
        var gw = await SeedClusterAsync("c");
        await gw.PutAsync(Ep, "/pgworker/moves/c/bucket_0",
            """{"op":"move","to":"shard2","requested_unix":100}""", null, CancellationToken.None);
        var handler = new CancelMoveHandler(gw, [Ep]);

        // Act
        var result = await handler.HandleAsync("c", "bucket_0", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        gw.Store.Should().NotContainKey("/pgworker/moves/c/bucket_0");
    }

    // ===== валидаторы тел (спека §7): 400-ветки без etcd =====

    [Fact]
    public void RollbackValidator_EmptyAndDuplicates_Errors()
    {
        // Arrange / Act / Assert — пустой массив и дубликаты ловит валидатор
        RollbackBucketsValidator.Validate(new RollbackBucketsRequest(null))
            .Should().ContainSingle(e => e.Field == "buckets");
        RollbackBucketsValidator.Validate(new RollbackBucketsRequest([]))
            .Should().ContainSingle(e => e.Field == "buckets");
        RollbackBucketsValidator.Validate(new RollbackBucketsRequest([0, 0]))
            .Should().ContainSingle(e => e.Message.Contains("дубликаты"));
        RollbackBucketsValidator.Validate(new RollbackBucketsRequest([0, 1]))
            .Should().BeEmpty();
    }

    [Fact]
    public void FinalizeValidator_MissingBucketOrShard_Errors()
    {
        // Arrange / Act / Assert
        FinalizeBucketValidator.Validate(new FinalizeBucketRequest(null, "s2"))
            .Should().ContainSingle(e => e.Field == "bucket");
        FinalizeBucketValidator.Validate(new FinalizeBucketRequest(0, null))
            .Should().ContainSingle(e => e.Field == "oldShard");
        FinalizeBucketValidator.Validate(new FinalizeBucketRequest(0, "s2"))
            .Should().BeEmpty();
    }

    [Fact]
    public void AbortValidator_MissingBucket_Error()
    {
        // Arrange / Act / Assert — force nullable: false не мешает
        AbortBucketValidator.Validate(new AbortBucketRequest(null, null))
            .Should().ContainSingle(e => e.Field == "bucket");
        AbortBucketValidator.Validate(new AbortBucketRequest(0, false))
            .Should().BeEmpty();
    }
}
```

- [ ] **Шаг 2. Написать `MoveOpsTicketJsonTests.cs` (roundtrip канонического JSON)**

```csharp
using System.Text.Json;
using PgWorker.App.Api.Operations;
using PgWorker.Moves;
using FluentAssertions;
using Xunit;

namespace PgWorker.UnitTests.Moves;

// Канонический JSON заявок (t07, спека §7): тело, которое пишет handler,
// парсится процессом (MoveRequest.Parse) — контракт постановки ↔ исполнения.
public class MoveOpsTicketJsonTests
{
    private static readonly JsonSerializerOptions Json = new()
    {
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull,
    };

    [Fact]
    public void RollbackTicketBody_ParsesAsMoveRequest()
    {
        // Arrange — тело, которое пишет RollbackBucketsHandler
        var json = JsonSerializer.Serialize(
            new MoveTickets.TicketBody("rollback", null, null, null, 1756000100, "ops"), Json);

        // Act
        var parsed = MoveRequest.Parse("bucket_0", json);

        // Assert
        parsed.IsSuccess.Should().BeTrue();
        parsed.Value.Op.Should().Be(MoveOp.Rollback);
        parsed.Value.To.Should().BeNull();
        parsed.Value.OldShard.Should().BeNull();
        parsed.Value.Force.Should().BeFalse();
        parsed.Value.RequestedUnix.Should().Be(1756000100);
        parsed.Value.RequestedBy.Should().Be("ops");
    }

    [Fact]
    public void FinalizeTicketBody_ParsesAsMoveRequest()
    {
        // Arrange
        var json = JsonSerializer.Serialize(
            new MoveTickets.TicketBody("finalize", null, "shard2", null, 1756000101, "ops"), Json);

        // Act
        var parsed = MoveRequest.Parse("bucket_0", json);

        // Assert
        parsed.Value.Op.Should().Be(MoveOp.Finalize);
        parsed.Value.OldShard.Should().Be("shard2");
    }

    [Fact]
    public void AbortTicketBody_ForceTrueWrites_ForceFalseOmitted()
    {
        // Arrange / Act — force:true пишется; false/null опускается (§4.2)
        var forced = JsonSerializer.Serialize(
            new MoveTickets.TicketBody("abort", null, null, true, 1756000102, "ops"), Json);
        var calm = JsonSerializer.Serialize(
            new MoveTickets.TicketBody("abort", null, null, false, 1756000103, "ops"), Json);

        // Assert
        forced.Should().Contain("\"force\":true");
        calm.Should().NotContain("\"force\"");
        MoveRequest.Parse("bucket_0", calm).Value.Force.Should().BeFalse();
        MoveRequest.Parse("bucket_0", forced).Value.Force.Should().BeTrue();
    }
}
```

Примечание: `MoveTickets` — `internal` → доступно через `InternalsVisibleTo("PgWorker.UnitTests")` (уже есть в `PgWorker.App.csproj`).

- [ ] **Шаг 3. Запустить — PASS (или добить расхождения текстов handler'ов)**

Run: `dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~MoveOps"` → PASS. Если тест ловит несоответствие — править handler (не тест), кроме случаев опечатки в тесте.

- [ ] **Шаг 4. Финал волны A: полная сборка + все тесты**

Run:
```bash
dotnet build src/PgWorker.slnx
dotnet test src/tests/PgWorker.UnitTests
dotnet test src/tests/PgWorker.IntegrationTests --filter "FullyQualifiedName~Api"
```
Expected: 0 warnings; unit зелёные без docker; интеграционные API — зелёные (etcd-фикстура).

- [ ] **Шаг 5. Commit**

```bash
git add src/tests/PgWorker.UnitTests/Api/MoveOpsHandlersTests.cs src/tests/PgWorker.UnitTests/Moves/MoveOpsTicketJsonTests.cs
git commit -m "test(t07): юнит guard-логики move-ops на моках gateway + roundtrip канонического JSON заявок"
```

---

## Волна B — панель (backend-прокси + чтение work + фронтенд)

### Task B1: 4 команды-прокси + маршруты `OperationsModule`

**Files:**
- Create: `src/AdminPanel.Api/Operations/MoveOpsCommands.cs`
- Modify: `src/AdminPanel.Api/Operations/OperationsModule.cs`
- Test: `src/tests/AdminPanel.UnitTests/Operations/MoveOpsProxyCommandTests.cs` (новый)

**Interfaces:**
- Consumes: `WorkerProxy.SendAsync<T>` (существующий), `IWorkerApiGateway`, `ICommand`/`ICommandHandler`, `[InjectAsScoped]`, `WorkerProblemDetails`.
- Produces (маршруты arch/03 §1.7–§1.9, camelCase тела):
  - `POST /api/clusters/{cluster}/moves/rollback` → 201 `RollbackQueuedDto`;
  - `POST /api/clusters/{cluster}/moves/finalize` → 201 `BucketFinalizeQueuedDto`;
  - `POST /api/clusters/{cluster}/moves/abort` → 201 `BucketAbortQueuedDto`;
  - `DELETE /api/clusters/{cluster}/moves/{bucket}` → 204 (DTO не читается — образец `DeleteClusterCommand`).

- [ ] **Шаг 1. Failing-тесты `MoveOpsProxyCommandTests.cs`**

Порт `WorkerProxyCommandTests.cs`: приватный `StubWorkerApi : IWorkerApiGateway` (копия из существующего файла). Кейсы (AAA):

```csharp
using System.Text.Json;
using AdminPanel.Api.Operations;
using AdminPanel.Etcd.Workers;
using AdminPanel.Infrastructure;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests.Operations;

// Прокси-команды move-ops (t07): панель не пишет в etcd — команды уходят в API
// PgWorker; ответы/ошибки проксируются 1:1 (ProblemDetails как есть).
public class MoveOpsProxyCommandTests
{
    private sealed class StubWorkerApi : IWorkerApiGateway
    {
        public sealed record Call(string Worker, HttpMethod Method, string Path, object? Body, string? RequestedBy);
        public List<Call> Calls { get; } = [];
        public Func<Call, WorkerApiResult>? Respond { get; set; }
        public Task<WorkerApiResult> SendAsync(
            string worker, HttpMethod method, string path, object? body, string? requestedBy, CancellationToken ct)
        {
            var call = new Call(worker, method, path, body, requestedBy);
            Calls.Add(call);
            return Task.FromResult(Respond is not null ? Respond(call) : new WorkerApiResult(204, null));
        }
    }

    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    [Fact]
    public async Task Rollback_201_ReturnsDtoAndSendsOperator()
    {
        // Arrange
        var api = new StubWorkerApi
        {
            Respond = _ => new WorkerApiResult(201,
                """{"cluster":"c","queued":[0],"skipped":[]}"""),
        };
        var handler = new RollbackBucketsCommandHandler(api);

        // Act
        var result = await handler.Handle(new RollbackBucketsCommand("c", [0], "admin"), CancellationToken.None);

        // Assert — DTO 1:1 + путь/оператор
        result.IsSuccess.Should().BeTrue();
        result.Value.Queued.Should().Equal(0);
        api.Calls.Should().ContainSingle().Which.Should().Match<StubWorkerApi.Call>(c =>
            c.Worker == "pgworker" && c.Method == HttpMethod.Post
            && c.Path == "/api/clusters/c/moves/rollback" && c.RequestedBy == "admin");
    }

    [Fact]
    public async Task Finalize_409ProblemDetails_FailedWithStatus()
    {
        // Arrange
        var api = new StubWorkerApi
        {
            Respond = _ => new WorkerApiResult(409,
                """{"title":"Move ops rejected","status":409,"detail":"шард c/shard1 — текущий владелец bucket_0, убирать нечего"}"""),
        };
        var handler = new FinalizeBucketCommandHandler(api);

        // Act
        var result = await handler.Handle(
            new FinalizeBucketCommand("c", 0, "shard1", "admin"), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        var problem = result.Error.Should().BeOfType<WorkerProblemDetails>().Subject;
        problem.StatusCode.Should().Be(409);
        problem.Body.Should().Contain("убирать нечего");
    }

    [Fact]
    public async Task Abort_SendsForceOnlyWhenTrue()
    {
        // Arrange — сериализованное тело проверяем через прокси-вызов
        var api = new StubWorkerApi { Respond = _ => new WorkerApiResult(201,
            """{"cluster":"c","bucket":0,"force":true}""") };
        var handler = new AbortBucketCommandHandler(api);

        // Act
        var result = await handler.Handle(new AbortBucketCommand("c", 0, true, "admin"), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Force.Should().BeTrue();
        var body = JsonSerializer.Serialize(api.Calls.Single().Body, Json);
        body.Should().Contain("\"force\":true");
    }

    [Fact]
    public async Task Cancel_204_NoEtcdWrites()
    {
        // Arrange — воркер отвечает 204 без тела (образец delete-мутаций)
        var api = new StubWorkerApi();
        var handler = new CancelMoveTicketCommandHandler(api);

        // Act
        var result = await handler.Handle(new CancelMoveTicketCommand("c", "bucket_0", "admin"), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        api.Calls.Should().ContainSingle().Which.Should().Match<StubWorkerApi.Call>(c =>
            c.Method == HttpMethod.Delete && c.Path == "/api/clusters/c/moves/bucket_0");
    }
}
```

- [ ] **Шаг 2. Запустить — FAIL (типов нет)**

Run: `dotnet test src/tests/AdminPanel.UnitTests --filter "FullyQualifiedName~MoveOpsProxyCommandTests"` → FAIL (компиляция: классы не определены).

- [ ] **Шаг 3. Реализовать `MoveOpsCommands.cs`**

```csharp
using AdminPanel.Etcd.Workers;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Operations;

// ===== rollback (arch/03 §1.7; протокол 02 §9.7.2) =====

// Тело POST: buckets nullable — null/отсутствие поля ловит валидатор воркера.
public sealed record RollbackBucketsRequest(IReadOnlyList<int>? Buckets);

// Ответ 201: queued поставлены сейчас, skipped — идентичные op=rollback стояли.
public sealed record RollbackQueuedDto(
    string Cluster, IReadOnlyList<int> Queued, IReadOnlyList<int> Skipped);

public sealed record RollbackBucketsCommand(
    string Cluster, IReadOnlyList<int> Buckets, string RequestedBy)
    : ICommand<RollbackQueuedDto>;

// Прокси: панель не пишет в etcd — команда уходит в API PgWorker (arch/14 §1.1).
[InjectAsScoped]
public sealed class RollbackBucketsCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<RollbackBucketsCommand, RollbackQueuedDto>
{
    public async ValueTask<Result<RollbackQueuedDto>> Handle(RollbackBucketsCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<RollbackQueuedDto>(
            api, "pgworker", HttpMethod.Post, $"/api/clusters/{command.Cluster}/moves/rollback",
            new RollbackBucketsRequest(command.Buckets), command.RequestedBy, ct);
}

// ===== finalize (arch/03 §1.8; протокол 02 §9.7.3) =====

public sealed record FinalizeBucketRequest(int Bucket, string OldShard);

public sealed record BucketFinalizeQueuedDto(string Cluster, int Bucket, string OldShard);

public sealed record FinalizeBucketCommand(
    string Cluster, int Bucket, string OldShard, string RequestedBy)
    : ICommand<BucketFinalizeQueuedDto>;

[InjectAsScoped]
public sealed class FinalizeBucketCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<FinalizeBucketCommand, BucketFinalizeQueuedDto>
{
    public async ValueTask<Result<BucketFinalizeQueuedDto>> Handle(FinalizeBucketCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<BucketFinalizeQueuedDto>(
            api, "pgworker", HttpMethod.Post, $"/api/clusters/{command.Cluster}/moves/finalize",
            new FinalizeBucketRequest(command.Bucket, command.OldShard), command.RequestedBy, ct);
}

// ===== abort (arch/03 §1.9; протокол 02 §9.7.4) =====

// force nullable: false — не шлём (воркер трактует отсутствие как false).
public sealed record AbortBucketRequest(int Bucket, bool? Force);

public sealed record BucketAbortQueuedDto(string Cluster, int Bucket, bool Force);

public sealed record AbortBucketCommand(
    string Cluster, int Bucket, bool Force, string RequestedBy)
    : ICommand<BucketAbortQueuedDto>;

[InjectAsScoped]
public sealed class AbortBucketCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<AbortBucketCommand, BucketAbortQueuedDto>
{
    public async ValueTask<Result<BucketAbortQueuedDto>> Handle(AbortBucketCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<BucketAbortQueuedDto>(
            api, "pgworker", HttpMethod.Post, $"/api/clusters/{command.Cluster}/moves/abort",
            new AbortBucketRequest(command.Bucket, command.Force ? true : null), command.RequestedBy, ct);
}

// ===== отмена заявки (arch/03 §1.9; протокол 02 §9.7.5) =====

// Воркер отвечает 204 без тела — DTO не читается (образец DeleteClusterCommand).
public sealed record MoveTicketCancelledDto(string Cluster, string Bucket);

public sealed record CancelMoveTicketCommand(
    string Cluster, string Bucket, string RequestedBy)
    : ICommand<MoveTicketCancelledDto>;

[InjectAsScoped]
public sealed class CancelMoveTicketCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<CancelMoveTicketCommand, MoveTicketCancelledDto>
{
    public async ValueTask<Result<MoveTicketCancelledDto>> Handle(CancelMoveTicketCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<MoveTicketCancelledDto>(
            api, "pgworker", HttpMethod.Delete, $"/api/clusters/{command.Cluster}/moves/{command.Bucket}",
            body: null, command.RequestedBy, ct);
}
```

- [ ] **Шаг 4. Маршруты в `OperationsModule.cs`** (после существующего moves; `Error(result)` — существующий хелпер модуля)

```csharp
// POST /api/clusters/{cluster}/moves/rollback — заявки на откат (t07, 02 §9.7.2).
endpoints.MapPost("/api/clusters/{cluster}/moves/rollback", async (
    string cluster, RollbackBucketsRequest request, ClaimsPrincipal user, IHandler handler, CancellationToken ct) =>
{
    var result = await handler.HandleCommand<RollbackBucketsCommand, RollbackQueuedDto>(
        new RollbackBucketsCommand(cluster, request.Buckets ?? [], user.Identity?.Name ?? "adminpanel"), ct);
    if (result.IsSuccess)
        return Results.Created($"/api/clusters/{cluster}", result.Value);

    return Error(result);
});

// POST /api/clusters/{cluster}/moves/finalize — заявка уборки старого шарда
// (t07, 02 §9.7.3): DROP SCHEMA СО ДАННЫМИ — необратимо (UI предупреждает).
endpoints.MapPost("/api/clusters/{cluster}/moves/finalize", async (
    string cluster, FinalizeBucketRequest request, ClaimsPrincipal user, IHandler handler, CancellationToken ct) =>
{
    var result = await handler.HandleCommand<FinalizeBucketCommand, BucketFinalizeQueuedDto>(
        new FinalizeBucketCommand(cluster, request.Bucket, request.OldShard, user.Identity?.Name ?? "adminpanel"), ct);
    if (result.IsSuccess)
        return Results.Created($"/api/clusters/{cluster}", result.Value);

    return Error(result);
});

// POST /api/clusters/{cluster}/moves/abort — заявка отмены переезда (t07,
// 02 §9.7.4): force ломает защиты свежести/routing==target.
endpoints.MapPost("/api/clusters/{cluster}/moves/abort", async (
    string cluster, AbortBucketRequest request, ClaimsPrincipal user, IHandler handler, CancellationToken ct) =>
{
    var result = await handler.HandleCommand<AbortBucketCommand, BucketAbortQueuedDto>(
        new AbortBucketCommand(cluster, request.Bucket, request.Force == true, user.Identity?.Name ?? "adminpanel"), ct);
    if (result.IsSuccess)
        return Results.Created($"/api/clusters/{cluster}", result.Value);

    return Error(result);
});

// DELETE /api/clusters/{cluster}/moves/{bucket} — отмена стоящей заявки (t07,
// 02 §9.7.5): не останавливает взятую в работу; повтор не идемпотентен (404).
endpoints.MapDelete("/api/clusters/{cluster}/moves/{bucket}", async (
    string cluster, string bucket, ClaimsPrincipal user, IHandler handler, CancellationToken ct) =>
{
    var result = await handler.HandleCommand<CancelMoveTicketCommand, MoveTicketCancelledDto>(
        new CancelMoveTicketCommand(cluster, bucket, user.Identity?.Name ?? "adminpanel"), ct);
    if (result.IsSuccess)
        return Results.NoContent();

    return Error(result);
});
```

- [ ] **Шаг 5. Запустить — PASS + сборка**

Run: `dotnet test src/tests/AdminPanel.UnitTests --filter "FullyQualifiedName~MoveOpsProxyCommandTests"` → PASS; `dotnet build src/PgWorker.slnx` → 0 warnings.

- [ ] **Шаг 6. Commit**

```bash
git add src/AdminPanel.Api/Operations/MoveOpsCommands.cs src/AdminPanel.Api/Operations/OperationsModule.cs src/tests/AdminPanel.UnitTests/Operations/MoveOpsProxyCommandTests.cs
git commit -m "feat(t07): панель — 4 прокси-команды move-ops (rollback/finalize/abort/cancel) + маршруты, ProblemDetails 1:1"
```

---

### Task B2: Поле `work` в `ClusterDto` (журнал воркера)

Чтение результатов заявок (спека §4.3/§3.4): маппинг `snapshot.PgWorkerWork` по кластеру; новых etcd-чтений нет.

**Files:**
- Modify: `src/AdminPanel.Api/Inspection/ClusterDetailsQuery.cs`
- Test: `src/tests/AdminPanel.UnitTests/ClustersMappersTests.cs` (обновить вызовы + новый кейс)
- Test: `src/tests/AdminPanel.IntegrationTests/ClusterDetailsWorkApiTests.cs` (новый)

**Interfaces:**
- Consumes: `WorkJournalInfo` (`AdminPanel.Core`: `Cluster, Op, Phase, Instance, UpdatedUnix, LastError, ...`), `EtcdSnapshot.PgWorkerWork`.
- Produces: `public sealed record WorkDto(string Op, string Phase, long UpdatedUnix, string? LastError)`; `ClusterDto` получает поле `WorkDto? Work` (последним); `ClusterDetailsMapper.Map(..., WorkJournalInfo? work)` — 8-й параметр.

- [ ] **Шаг 1. Failing integration-тест**

`ClusterDetailsWorkApiTests.cs` (порт структуры `InspectionApiTests`: `[Collection("api")]`, `AuthWebFactory`, login-хелпер):

```csharp
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using AdminPanel.Core;
using FluentAssertions;
using Xunit;

namespace AdminPanel.IntegrationTests;

// GET /api/clusters/{c}: поле work — журнал /pgworker/work/<C> (t07, спека
// §4.3): последняя запись процесса воркера; null при отсутствии журнала.
[Collection("api")]
public class ClusterDetailsWorkApiTests(AuthWebFactory factory)
{
    [Fact]
    public async Task ClusterDetails_WorkJournal_MappedToDto()
    {
        // Arrange — кластерный снапшот + одна запись work-журнала
        factory.Snapshot = InspectionSnapshots.Clustered(
                factory.Time.GetUtcNow(), factory.Time.GetUtcNow())
            with
        {
            PgWorkerWork =
            [
                new WorkJournalInfo("demo", "rollback", "rejected", "i-1",
                    factory.Time.GetUtcNow().ToUnixTimeSeconds() - 30,
                    "нет обратной подписки bucket_0 — откат только полным re-copy",
                    null, null, null),
            ],
        };
        using var client = await ApiTestLogin.LoginAsync(factory);

        // Act
        using var response = await client.GetAsync("/api/clusters/demo",
            TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var dto = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        dto.GetProperty("work").ValueKind.Should().Be(JsonValueKind.Object);
        dto.GetProperty("work").GetProperty("op").GetString().Should().Be("rollback");
        dto.GetProperty("work").GetProperty("phase").GetString().Should().Be("rejected");
        dto.GetProperty("work").GetProperty("lastError").GetString().Should().Contain("re-copy");
    }

    [Fact]
    public async Task ClusterDetails_NoWorkJournal_WorkNull()
    {
        // Arrange — снапшот без записей work
        factory.Snapshot = InspectionSnapshots.Clustered(
            factory.Time.GetUtcNow(), factory.Time.GetUtcNow());
        using var client = await ApiTestLogin.LoginAsync(factory);

        // Act
        using var response = await client.GetAsync("/api/clusters/demo",
            TestContext.Current.CancellationToken);

        // Assert
        var dto = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        dto.GetProperty("work").ValueKind.Should().Be(JsonValueKind.Null);
    }
}
```

(Если `ApiTestLogin` — internal-хелпер другого файла коллекции — он уже доступен внутри сборки; иначе поднять логин по образцу `MovesApiTests.LoginAsync`.)

- [ ] **Шаг 2. Запустить — FAIL (поля work нет)**

Run: `dotnet test src/tests/AdminPanel.IntegrationTests --filter "FullyQualifiedName~ClusterDetailsWorkApiTests"` → FAIL.

- [ ] **Шаг 3. Реализовать в `ClusterDetailsQuery.cs`**

```csharp
// Журнал последнего процесса воркера кластера /pgworker/work/<C> (t07, arch/03 §2):
// op/phase/возраст/lastError — результат исполненной/отвергнутой заявки.
public sealed record WorkDto(string Op, string Phase, long UpdatedUnix, string? LastError);
```

- `ClusterDto` — добавить параметр `WorkDto? Work` последним;
- `ClusterDetailsMapper.Map(...)` — добавить параметр `WorkJournalInfo? work` последним; в конце конструктора `ClusterDto`:
  ```csharp
  work is null ? null : new WorkDto(work.Op, work.Phase, work.UpdatedUnix, work.LastError)
  ```
- `ClusterDetailsQueryHandler.Handle`:
  ```csharp
  var work = snapshot.PgWorkerWork.FirstOrDefault(w => w.Cluster == query.Cluster);
  ...
  : Result<ClusterDto>.Success(ClusterDetailsMapper.Map(
      cluster, time.GetUtcNow().ToUnixTimeSeconds(), query.Owner, query.State, snapshot.StandNodes,
      snapshot.HaScopes, snapshot.MoveTickets, work)));
  ```
- Обновить ВСЕ вызовы `ClusterDetailsMapper.Map` в тестах (`ClustersMappersTests.cs`, `InspectionMappersTests.cs`): добавить последний аргумент `null` (кроме новых кейсов).
- Добавить юнит-кейс в `ClustersMappersTests.cs` (AAA):

```csharp
[Fact]
public void Map_WorkJournal_MappedAndNullSafe()
{
    // Arrange — кластер из ExistingCluster() + журнальная запись
    var cluster = ExistingCluster();
    var work = new AdminPanel.Core.WorkJournalInfo(
        "demo", "abort", "done", "i-1", 1756000123, null, null, null, null);

    // Act
    var with = ClusterDetailsMapper.Map(cluster, NowUnix, null, null, [], [], [], work);
    var without = ClusterDetailsMapper.Map(cluster, NowUnix, null, null, [], [], [], null);

    // Assert
    with.Work.Should().NotBeNull();
    with.Work!.Op.Should().Be("abort");
    with.Work.Phase.Should().Be("done");
    with.Work.UpdatedUnix.Should().Be(1756000123);
    without.Work.Should().BeNull();
}
```

(Имя фабрики кластера заменить на фактическое из файла — `TestSnapshots.*`/локальный хелпер.)

- [ ] **Шаг 4. Запустить — PASS**

Run: `dotnet test src/tests/AdminPanel.IntegrationTests --filter "FullyQualifiedName~ClusterDetailsWorkApiTests"` → PASS; `dotnet test src/tests/AdminPanel.UnitTests --filter "FullyQualifiedName~ClustersMappersTests"` → PASS; `dotnet build src/PgWorker.slnx` → 0 warnings.

- [ ] **Шаг 5. Commit**

```bash
git add src/AdminPanel.Api/Inspection/ClusterDetailsQuery.cs src/tests/AdminPanel.UnitTests/ClustersMappersTests.cs src/tests/AdminPanel.UnitTests/InspectionMappersTests.cs src/tests/AdminPanel.IntegrationTests/ClusterDetailsWorkApiTests.cs
git commit -m "feat(t07): ClusterDto.work — журнал последнего процесса воркера из PgWorkerWork (null-safe)"
```

---

### Task B3: Frontend API-слой — DTO + query-функции

**Files:**
- Modify: `frontend/src/api/dto.ts`
- Modify: `frontend/src/api/queries.ts`

**Interfaces:**
- Consumes: `apiFetch` (`client.ts`), канон camelCase (минимальный API сериализует Web-camelCase).
- Produces (для B4/B5):
  - типы: `RollbackBucketsRequestDto`, `RollbackQueuedDto`, `FinalizeBucketRequestDto`, `BucketFinalizeQueuedDto`, `AbortBucketRequestDto`, `BucketAbortQueuedDto`, `ClusterWorkDto`; `ClusterDto.work?: ClusterWorkDto | null`;
  - функции: `rollbackBuckets(cluster, request)`, `finalizeBucket(cluster, request)`, `abortMove(cluster, request)`, `cancelMoveTicket(cluster, bucket)`.

- [ ] **Шаг 1. Добавить в `dto.ts`** (рядом с `MoveTicketDto`/`ClusterDto`)

```typescript
// POST /api/clusters/{cluster}/moves/rollback — тело и ответ (t07, arch/03 §1.7).
export interface RollbackBucketsRequestDto {
  buckets: number[];
}

export interface RollbackQueuedDto {
  cluster: string;
  queued: number[];
  skipped: number[];
}

// POST /api/clusters/{cluster}/moves/finalize — тело и ответ (arch/03 §1.8).
export interface FinalizeBucketRequestDto {
  bucket: number;
  oldShard: string;
}

export interface BucketFinalizeQueuedDto {
  cluster: string;
  bucket: number;
  oldShard: string;
}

// POST /api/clusters/{cluster}/moves/abort — тело и ответ (arch/03 §1.9);
// force?: true — только когда включён (false не шлём).
export interface AbortBucketRequestDto {
  bucket: number;
  force?: boolean;
}

export interface BucketAbortQueuedDto {
  cluster: string;
  bucket: number;
  force: boolean;
}

// Журнал последнего процесса воркера кластера /pgworker/work/<C>
// (arch/03 §2): результат исполненной/отвергнутой заявки переездов.
export interface ClusterWorkDto {
  op: string;
  phase: string;
  updatedUnix: number;
  lastError: string | null;
}
```

И в `ClusterDto` добавить поле (после `pendingMoves`):

```typescript
  pendingMoves: MoveTicketDto[]; // очередь заявок переездов (arch/02 §2.3.1)
  // Журнал последнего процесса воркера (t07, arch/03 §2); null — журнала нет.
  work?: ClusterWorkDto | null;
```

- [ ] **Шаг 2. Добавить в `queries.ts`** (рядом с `moveBuckets`)

```typescript
// POST /api/clusters/{cluster}/moves/rollback — заявки на откат (t07, 02 §9.7.2):
// направление определяет воркер по обратной подписке.
export function rollbackBuckets(
  cluster: string, request: RollbackBucketsRequestDto,
): Promise<RollbackQueuedDto> {
  return apiFetch<RollbackQueuedDto>(
    `/api/clusters/${encodeURIComponent(cluster)}/moves/rollback`,
    { method: 'POST', body: request });
}

// POST /api/clusters/{cluster}/moves/finalize — заявка уборки старого шарда
// (t07, 02 §9.7.3): DROP SCHEMA СО ДАННЫМИ — необратимо.
export function finalizeBucket(
  cluster: string, request: FinalizeBucketRequestDto,
): Promise<BucketFinalizeQueuedDto> {
  return apiFetch<BucketFinalizeQueuedDto>(
    `/api/clusters/${encodeURIComponent(cluster)}/moves/finalize`,
    { method: 'POST', body: request });
}

// POST /api/clusters/{cluster}/moves/abort — заявка отмены переезда
// (t07, 02 §9.7.4): force ломает защиты свежести/routing==target.
export function abortMove(
  cluster: string, request: AbortBucketRequestDto,
): Promise<BucketAbortQueuedDto> {
  return apiFetch<BucketAbortQueuedDto>(
    `/api/clusters/${encodeURIComponent(cluster)}/moves/abort`,
    { method: 'POST', body: request });
}

// DELETE /api/clusters/{cluster}/moves/{bucket} — отмена стоящей заявки
// (t07, 02 §9.7.5): начатый переезд доедет; остановка начатого — только abort.
export function cancelMoveTicket(cluster: string, bucket: string): Promise<void> {
  return apiFetch<void>(
    `/api/clusters/${encodeURIComponent(cluster)}/moves/${encodeURIComponent(bucket)}`,
    { method: 'DELETE' });
}
```

(Плюс импорты новых типов в существующий `import type { ... } from './dto'`.)

- [ ] **Шаг 3. Проверка — typecheck**

Run: `cd frontend && npm run typecheck` → без ошибок.

- [ ] **Шаг 4. Commit**

```bash
git add frontend/src/api/dto.ts frontend/src/api/queries.ts
git commit -m "feat(t07): фронт — DTO и query-функции move-ops (rollback/finalize/abort/cancel) + work журнала"
```

---

### Task B4: Вкладка «Бакеты» — колонка действий + модалки rollback/finalize

Per-row кнопки у ACTIVE-бакетов при `canScale` (arch/03 §3/§3.4/§3.5): «Откатить» (light) и «Финализировать» (red light); у занятого заявкой бакета — бейдж «в очереди: `<op>`».

**Files:**
- Create: `frontend/src/pages/cluster-details/RollbackBucketModal.tsx`
- Create: `frontend/src/pages/cluster-details/FinalizeBucketModal.tsx`
- Modify: `frontend/src/pages/cluster-details/BucketsTab.tsx`

**Interfaces:**
- Consumes: `rollbackBuckets`/`finalizeBucket` (B3), `ApiError` (409 → yellow, 400/503 → red — порт `MoveBucketsModal`), `ShardDto.runtime.subscriptions` (подсказки), `queryKeys.cluster`.
- Produces: `RollbackBucketModal({ cluster, bucketId, shards, opened, onClose })`, `FinalizeBucketModal({ cluster, bucketId, owner, shards, opened, onClose })`.

- [ ] **Шаг 1. `RollbackBucketModal.tsx`**

```tsx
// Форма «Откатить бакет» (t07, arch/03 §3.4): направление определяет воркер
// по живой обратной подписке sub_bucket_<i>_rb; подсказка — best-effort по
// SQL-пробе (shards[].runtime.subscriptions); заявка — POST .../moves/rollback.
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Button, Group, Modal, Stack, Text } from '@mantine/core';
import { ApiError } from '../../api/client';
import { queryKeys, rollbackBuckets } from '../../api/queries';
import type { ShardDto } from '../../api/dto';

interface Props {
  cluster: string;
  bucketId: number;
  shards: ShardDto[];
  opened: boolean;
  onClose: () => void;
}

export function RollbackBucketModal({ cluster, bucketId, shards, opened, onClose }: Props) {
  const queryClient = useQueryClient();
  const mutation = useMutation({
    mutationFn: () => rollbackBuckets(cluster, { buckets: [bucketId] }),
    onSuccess: async () => {
      // Заявка появится в очереди вкладки «Переезды» со следующего тика.
      onClose();
      await queryClient.invalidateQueries({ queryKey: ['clusters'] });
      await queryClient.invalidateQueries({ queryKey: queryKeys.cluster(cluster) });
    },
  });

  // Подсказка направления: шард с живой обратной подпиской бакета (SQL-проба).
  const rbSub = `sub_bucket_${bucketId}_rb`;
  const hintShard = shards.find((s) => (s.runtime?.subscriptions ?? []).includes(rbSub));

  // Ошибка сервера: 409 guard'ы (yellow) / 400/503 (red) — ProblemDetails.
  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  return (
    <Modal opened={opened} onClose={onClose} title={`Откатить bucket_${bucketId}`} centered>
      <Stack gap="sm">
        <Text>
          Откатить <b>{`bucket_${bucketId}`}</b> на прежний шард — направление определяет
          воркер по живой обратной подписке <b>{rbSub}</b>.
        </Text>
        <Text size="sm" c="dimmed">
          {hintShard !== undefined
            ? `Вернётся на ${hintShard.name} (живая подписка видна SQL-пробой).`
            : 'Куда — определит воркер по обратной подписке (проба выключена или не видит её).'}
        </Text>
        <Alert color="yellow" variant="light" title="Внимание">
          Откат — зеркальный cutover с секундной заморозкой записи. Если обратной
          подписки нет — воркер отвергнет заявку (откат только полным re-copy).
        </Alert>
        {serverError !== null ? (
          <Alert color={serverError.status === 409 ? 'yellow' : 'red'} variant="light">
            {serverError.status === 409
              ? (serverError.detail ?? 'Откат отклонён')
              : serverError.status === 400
                ? (serverError.detail ?? 'Проверьте параметры')
                : 'etcd недоступен — повторите позже'}
          </Alert>
        ) : null}
        <Group justify="flex-end">
          <Button variant="default" onClick={onClose}>Отмена</Button>
          <Button variant="light" loading={mutation.isPending} onClick={() => mutation.mutate()}>
            Поставить в очередь
          </Button>
        </Group>
      </Stack>
    </Modal>
  );
}
```

- [ ] **Шаг 2. `FinalizeBucketModal.tsx`**

```tsx
// Форма «Финализировать бакет» (t07, arch/03 §3.5): выбор шарда ≠ владельца,
// где убрать артефакты (DROP SCHEMA СО ДАННЫМИ — необратимо); подсказки по
// живым подпискам SQL-пробы; TO_REMOVE допустим (финализация перед демонтажем).
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Button, Group, Modal, Select, Stack, Text } from '@mantine/core';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { finalizeBucket, queryKeys } from '../../api/queries';
import type { ShardDto } from '../../api/dto';

interface Props {
  cluster: string;
  bucketId: number;
  owner: string;
  shards: ShardDto[];
  opened: boolean;
  onClose: () => void;
}

export function FinalizeBucketModal({ cluster, bucketId, owner, shards, opened, onClose }: Props) {
  const queryClient = useQueryClient();
  const [oldShard, setOldShard] = useState<string | null>(null);
  const mutation = useMutation({
    mutationFn: (shard: string) => finalizeBucket(cluster, { bucket: bucketId, oldShard: shard }),
    onSuccess: async () => {
      onClose();
      await queryClient.invalidateQueries({ queryKey: ['clusters'] });
      await queryClient.invalidateQueries({ queryKey: queryKeys.cluster(cluster) });
    },
  });

  // Кандидаты: шарды ≠ текущего владельца; метки — живая подписка / к удалению.
  const sub = `sub_bucket_${bucketId}`;
  const subRb = `sub_bucket_${bucketId}_rb`;
  const shardData = shards
    .filter((s) => s.name !== owner)
    .map((s) => {
      const labels: string[] = [];
      if ((s.runtime?.subscriptions ?? []).some((n) => n === sub || n === subRb))
        labels.push('живая подписка');
      if (s.state === 'TO_REMOVE') labels.push('к удалению');
      return { value: s.name, label: labels.length > 0 ? `${s.name} (${labels.join(', ')})` : s.name };
    });

  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  return (
    <Modal opened={opened} onClose={onClose} title={`Финализировать bucket_${bucketId}`} centered>
      <Stack gap="sm">
        <Select
          label="Убрать артефакты на шарде"
          placeholder="Выберите шард"
          data={shardData}
          value={oldShard}
          onChange={setOldShard}
          nothingFoundMessage="Нет других шардов"
        />
        <Alert color="red" variant="light" title="Необратимо">
          На выбранном шарде будет DROP SCHEMA <b>{`bucket_${bucketId}`}</b> СО ДАННЫМИ
          (необратимо); подписки/публикации/слоты срезаются; владелец <b>{owner}</b> не трогается.
        </Alert>
        {serverError !== null ? (
          <Alert color={serverError.status === 409 ? 'yellow' : 'red'} variant="light">
            {serverError.status === 409
              ? (serverError.detail ?? 'Финализация отклонена')
              : serverError.status === 400
                ? (serverError.detail ?? 'Проверьте параметры')
                : 'etcd недоступен — повторите позже'}
          </Alert>
        ) : null}
        <Group justify="flex-end">
          <Button variant="default" onClick={onClose}>Отмена</Button>
          <Button color="red" variant="light" disabled={oldShard === null}
            loading={mutation.isPending}
            onClick={() => oldShard !== null && mutation.mutate(oldShard)}>
            Убрать артефакты
          </Button>
        </Group>
      </Stack>
    </Modal>
  );
}
```

- [ ] **Шаг 3. Колонка «Действия» в `BucketsTab.tsx`**

- прокинуть в `BucketRow` новые пропсы: `cluster`, `shards`, `canScale`, `ticketOp: string | null` (op стоящей заявки);
- в `BucketsTab` построить `claimed`-map (порт из `MoveBucketsModal`):
  ```tsx
  // Бакет со стоящей заявкой: вместо кнопок — бейдж «в очереди: <op>» (arch/03 §3).
  const claimed = useMemo(() => {
    const map = new Map<number, string>();
    for (const t of pendingMoves) {
      if (t.bucketId !== null) map.set(t.bucketId, t.op);
    }
    return map;
  }, [pendingMoves]);
  ```
- заголовок таблицы: `{canScale ? <Table.Th>Действия</Table.Th> : null}` (колонка только при canScale);
- в строке (`BucketRow`):
  - `{ticketOp !== null ? <Badge color="grape" variant="light">{`в очереди: ${ticketOp}`}</Badge>`
    : `bucket.state === 'ACTIVE'` ? кнопки `<Button size="xs" variant="light">Откатить</Button>` + `<Button size="xs" color="red" variant="light">Финализировать</Button>` (открывают соответствующие модалки — состояние `rollbackId`/`finalizeId: number | null` в `BucketsTab`);
    : `null}` — не-ACTIVE строки — пустая ячейка;
- модалки — в корне `BucketsTab`:
  ```tsx
  <RollbackBucketModal cluster={cluster} bucketId={rollbackId ?? 0} shards={shards}
    opened={rollbackId !== null} onClose={() => setRollbackId(null)} />
  <FinalizeBucketModal cluster={cluster} bucketId={finalizeId ?? 0}
    owner={buckets.find((b) => b.id === finalizeId)?.owner ?? ''} shards={shards}
    opened={finalizeId !== null} onClose={() => setFinalizeId(null)} />
  ```

- [ ] **Шаг 4. Проверка — typecheck**

Run: `cd frontend && npm run typecheck` → без ошибок.

- [ ] **Шаг 5. Commit**

```bash
git add frontend/src/pages/cluster-details/RollbackBucketModal.tsx frontend/src/pages/cluster-details/FinalizeBucketModal.tsx frontend/src/pages/cluster-details/BucketsTab.tsx
git commit -m "feat(t07): вкладка Бакеты — per-row «Откатить»/«Финализировать» (модалы §3.4/§3.5, бейдж «в очереди»)"
```

---

### Task B5: Вкладка «Переезды» — abort/снятие заявки/журнал + прокидка в `ClusterDetailsPage`

**Files:**
- Create: `frontend/src/pages/cluster-details/AbortMoveModal.tsx`
- Modify: `frontend/src/pages/cluster-details/MovesTab.tsx`
- Modify: `frontend/src/pages/ClusterDetailsPage.tsx`

**Interfaces:**
- Consumes: `abortMove`/`cancelMoveTicket` (B3), `ClusterWorkDto`, `formatUnixAge`/`formatUnix` (utils), `ApiError`.
- Produces: `AbortMoveModal({ cluster, bucket: BucketDto, opened, onClose })`; `MovesTab({ cluster, canScale, buckets, pendingMoves, work })`.

- [ ] **Шаг 1. `AbortMoveModal.tsx`**

```tsx
// Форма «Отменить переезд» (t07, arch/03 §3.6): abort незавершённого переезда;
// чекбокс force ломает защиты свежести и routing==target; серверные 409 —
// текстом ProblemDetails в теле формы.
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Box, Checkbox, Group, Modal, Stack, Text } from '@mantine/core';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { abortMove, queryKeys } from '../../api/queries';
import { formatUnixAge } from '../../utils/format';
import type { BucketDto } from '../../api/dto';

interface Props {
  cluster: string;
  bucket: BucketDto;
  opened: boolean;
  onClose: () => void;
}

export function AbortMoveModal({ cluster, bucket, opened, onClose }: Props) {
  const queryClient = useQueryClient();
  const [force, setForce] = useState(false);
  const mutation = useMutation({
    mutationFn: () => abortMove(cluster, { bucket: bucket.id, force: force || undefined }),
    onSuccess: async () => {
      onClose();
      await queryClient.invalidateQueries({ queryKey: ['clusters'] });
      await queryClient.invalidateQueries({ queryKey: queryKeys.cluster(cluster) });
    },
  });

  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  return (
    <Modal opened={opened} onClose={onClose} title={`Отменить переезд bucket_${bucket.id}`} centered>
      <Stack gap="sm">
        <Text>
          Маршрут: <b>{bucket.move?.owner ?? '—'} → {bucket.move?.target ?? '—'}</b>,
          фаза <b>{bucket.move?.phase ?? '—'}</b>, статус обновлён{' '}
          <b>{bucket.move?.updatedUnix != null ? formatUnixAge(bucket.move.updatedUnix) : '—'}</b>.
        </Text>
        <Alert color="yellow" variant="light" title="Внимание">
          Артефакты переезда убираются, бакет возвращается владельцу.
        </Alert>
        <Checkbox
          checked={force}
          onChange={(e) => setForce(e.currentTarget.checked)}
          label="force — ломает защиту свежести (переезд, возможно, ещё жив) и разрешает
            доведение перевода, когда flip уже прошёл (уборка старого шарда, как
            finalize); включайте только если mover точно мёртв"
        />
        {serverError !== null ? (
          <Alert color="yellow" variant="light">{serverError.detail ?? serverError.message}</Alert>
        ) : null}
        <Group justify="flex-end">
          <Button variant="default" onClick={onClose}>Отмена</Button>
          <Button color="red" loading={mutation.isPending} onClick={() => mutation.mutate()}>
            Отменить переезд
          </Button>
        </Group>
      </Stack>
    </Modal>
  );
}
```

- [ ] **Шаг 2. `MovesTab.tsx` — колонки действий, снятие заявки, журнал воркера**

- сигнатура: `MovesTab({ cluster, canScale, buckets, pendingMoves, work }: { cluster: string; canScale: boolean; buckets: BucketDto[]; pendingMoves: MoveTicketDto[]; work?: ClusterWorkDto | null })`;
- таблица переездов: колонка `{canScale ? <Table.Th>Действия</Table.Th> : null}`; в строке — красная кнопка «Отменить переезд» (`<Button color="red" variant="light" size="xs">`) → `abortId: number | null` → `AbortMoveModal`;
- очередь заявок: колонка «Снять заявку» (при canScale): мутация `cancelMoveTicket` c подтверждением (порт `RemoveShardButton`):
  ```tsx
  // Снятие заявки: подтверждение «начатый доедет»; 404 «заявки нет» — тихо
  // инвалидировать (оператора опередил тик воркера, arch/03 §3.6).
  const cancel = useMutation({
    mutationFn: (bucket: string) => cancelMoveTicket(cluster, bucket),
    onSuccess: async () => {
      setConfirmTicket(null);
      await queryClient.invalidateQueries({ queryKey: ['clusters'] });
      await queryClient.invalidateQueries({ queryKey: queryKeys.cluster(cluster) });
    },
    onError: async (error) => {
      if (error instanceof ApiError && error.status === 404) {
        setConfirmTicket(null);
        await queryClient.invalidateQueries({ queryKey: ['clusters'] });
        await queryClient.invalidateQueries({ queryKey: queryKeys.cluster(cluster) });
        return;
      }
      // прочие (503) — текстом в подтверждении
    },
  });
  ```
  подтверждение — Modal с текстом: «Заявка `<op> bucket_<i>` будет удалена из очереди. Если переезд уже начат — он доедет до конца; остановка начатого переезда — только „Отменить переезд" (abort)»;
- блок «Журнал воркера» (после очереди, при `work != null`):
  ```tsx
  {work != null ? (
    <>
      <Group justify="space-between" mt="md">
        <Text fw={500}>Журнал воркера</Text>
      </Group>
      <Group gap="sm">
        <Badge color="blue" variant="light">{work.op}</Badge>
        <Text size="sm">{work.phase}</Text>
        <Text size="sm" c="dimmed">обновлён {formatUnixAge(work.updatedUnix)}</Text>
        {work.lastError !== null ? (
          <Tooltip label={work.lastError}>
            <Text size="sm" c="red">{truncateText(work.lastError, 40)}</Text>
          </Tooltip>
        ) : null}
      </Group>
      <Text size="sm" c="dimmed">
        Последний процесс воркера кластера; отвергнутые заявки — с причиной.
      </Text>
    </>
  ) : null}
  ```

- [ ] **Шаг 3. `ClusterDetailsPage.tsx` — прокинуть в `MovesTab`**

```tsx
<Tabs.Panel value="moves" pt="sm">
  <MovesTab cluster={data.name} canScale={canScale} buckets={data.buckets}
    pendingMoves={data.pendingMoves} work={data.work} />
</Tabs.Panel>
```

- [ ] **Шаг 4. Проверка — typecheck + build фронта**

Run: `cd frontend && npm run typecheck && npm run build` → без ошибок (SPA-бандл собирается).

- [ ] **Шаг 5. Финал волны B: сборка + тесты**

Run:
```bash
dotnet build src/PgWorker.slnx
dotnet test src/tests/AdminPanel.UnitTests
dotnet test src/tests/AdminPanel.IntegrationTests --filter "FullyQualifiedName~MovesApiTests|FullyQualifiedName~ClusterDetailsWorkApiTests"
```
Expected: 0 warnings; тесты зелёные.

- [ ] **Шаг 6. Commit**

```bash
git add frontend/src/pages/cluster-details/AbortMoveModal.tsx frontend/src/pages/cluster-details/MovesTab.tsx frontend/src/pages/ClusterDetailsPage.tsx
git commit -m "feat(t07): вкладка Переезды — per-row «Отменить переезд» (force), «Снять заявку», блок «Журнал воркера»"
```

---

## Волна C — стенд e2e

### Task C1: e2e-чек `60-move-ops.sh`

Мутации через API панели на демо-сиде; зависшие статусы чек насыпает сам (образец `20-alerts.sh`; демо-сид аномалий не сеет — принцип «согласованного сида»). PG-шарды стенда реальны → отвергнутые rollback-заявки доедают до `work`-журнала (критерий приёмки 6).

**Files:**
- Create: `dev-stand/adminpanel/checks/60-move-ops.sh` (следующий свободный номер)

**Interfaces:**
- Consumes: поднятый стенд (`00-up.sh`: панель :5050, etcd `docker compose exec etcd etcdctl`, pgworker :8080 живой, демо-сид налит); login admin/admin (образец `50-kafka-api.sh`).

Ключевые timing-решения (детерминизм):
- «свежий» статус — `updated_unix = now` (repair не трогает: < 600 c; abort-guard: < 120 c → 409);
- «несвежий» — `updated_unix = now-300` (> 120 abort-guard проходит; < 600 repair-процесс молчит);
- заявки после 201 снимаются сразу (`DELETE`), окно против тика скана воркера (5 c) минимально; единственная «долгая» заявка (rollback на bucket_4) остаётся — её отвергает процесс (нет обратной подписки на демо-PG) — это и проверяем.

- [ ] **Шаг 1. Написать чек (порт каркаса `50-kafka-api.sh`)**

```bash
#!/usr/bin/env bash
# Move-ops на демо-сиде через API панели (t07): rollback/finalize/abort +
# снятие заявок; результаты — etcd-ключи, очередь/статусы/work-журнал деталей
# кластера. Полный docker-цикл move→abort/rollback→finalize покрыт E2e t01 —
# здесь API/UI-слой без поднятия новых PG. Зависшие статусы насыпает чек
# (демо-сид аномалий не сеет — образец 20-alerts.sh): updated_unix=now —
# «свежий» (abort 409), now-300 — «несвежий» (>AbortMinAgeSec=120, <repair 600).
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${ADMINPANEL_URL:-http://localhost:5050}"
JAR="$(mktemp)"; trap 'rm -f "$JAR"' EXIT

# Arrange: демо-сид жив (идемпотентен), панель отвечает, etcd под рукой.
[ -n "$(docker compose exec -T etcd etcdctl get /clusters/demo/config --print-value-only 2>/dev/null)" ] \
  || { "$PWD/checks/05-seed.sh" pg; }
for i in $(seq 1 60); do curl -fsS "$BASE/api/healthz" >/dev/null 2>&1 && break; sleep 1; done
curl -fsS "$BASE/api/healthz" >/dev/null || { echo "❌ панель не отвечает: $BASE"; exit 1; }
curl -fsS -c "$JAR" -o /dev/null -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  || { echo "❌ login admin/admin не прошёл"; exit 1; }

api()  { curl -fsS -b "$JAR" "$BASE$1"; }
code() { curl -s -o /dev/null -w '%{http_code}' -b "$JAR" "$@"; }
ect()  { docker compose exec -T etcd etcdctl --endpoints=http://localhost:2379 "$@"; }
now()  { date +%s; }

# Сводная проверка тика панели (детали демо-кластера читаются).
for i in $(seq 1 15); do
  api /api/clusters/demo | jq -e '.name == "demo"' >/dev/null 2>&1 && break; sleep 1;
done
api /api/clusters/demo | jq -e '.name == "demo"' >/dev/null \
  || { echo "❌ /api/clusters/demo недоступен"; exit 1; }
echo "  панель жива, демо-кластер читается"

# ── 1) abort: свежий статус без force → 409; с force → 201; отмена заявки → 204/404.
ect put /clusters/demo/buckets/status/bucket_3 \
  "{\"bucket\":\"bucket_3\",\"state\":\"SYNCING\",\"owner\":\"s1\",\"target\":\"s2\",\"started_unix\":$(now),\"updated_unix\":$(now),\"phase\":\"copy\"}" >/dev/null
c="$(code -X POST "$BASE/api/clusters/demo/moves/abort" -H 'Content-Type: application/json' -d '{"bucket":3}')"
[ "$c" = 409 ] || { echo "❌ abort свежий без force = $c, ожидался 409"; exit 1; }
api -X POST "$BASE/api/clusters/demo/moves/abort" -H 'Content-Type: application/json' -d '{"bucket":3,"force":true}' >/dev/null # проверка кода ниже
c="$(code -X POST "$BASE/api/clusters/demo/moves/abort" -H 'Content-Type: application/json' -d '{"bucket":3,"force":true}')" || true
# force-заявка уже стоит (идентичная → 201 без записи, §9.7 п.3):
[ "$c" = 201 ] || { echo "❌ повторный abort force = $c, ожидался 201 (идентичность)"; exit 1; }
ect get /pgworker/moves/demo/bucket_3 --print-value-only | grep -q '"op":"abort"' \
  || { echo "❌ ключ abort-заявки не содержит op=abort"; exit 1; }
ect get /pgworker/moves/demo/bucket_3 --print-value-only | grep -q '"force":true' \
  || { echo "❌ ключ abort-заявки без force:true"; exit 1; }
c="$(code -X DELETE "$BASE/api/clusters/demo/moves/bucket_3")"
[ "$c" = 204 ] || { echo "❌ снятие abort-заявки = $c, ожидался 204"; exit 1; }
[ -z "$(ect get /pgworker/moves/demo/bucket_3 --print-value-only 2>/dev/null)" ] \
  || { echo "❌ ключ заявки не исчез после DELETE"; exit 1; }
c="$(code -X DELETE "$BASE/api/clusters/demo/moves/bucket_3")"
[ "$c" = 404 ] || { echo "❌ повторное снятие = $c, ожидался 404 (не идемпотентно)"; exit 1; }
echo "  abort: свежий 409 → force 201 (идентичность) → снятие 204 → повтор 404"
ect del /clusters/demo/buckets/status/bucket_3 >/dev/null

# ── 2) abort: несвежий статус без force → 201 (force в JSON нет); ACTIVE → 409.
ect put /clusters/demo/buckets/status/bucket_7 \
  "{\"bucket\":\"bucket_7\",\"state\":\"ABORTING\",\"owner\":\"s2\",\"target\":\"s1\",\"started_unix\":$(( $(now) - 300 )),\"updated_unix\":$(( $(now) - 300 )),\"phase\":\"cleanup\"}" >/dev/null
c="$(code -X POST "$BASE/api/clusters/demo/moves/abort" -H 'Content-Type: application/json' -d '{"bucket":7}')"
[ "$c" = 201 ] || { echo "❌ abort несвежий без force = $c, ожидался 201"; exit 1; }
ect get /pgworker/moves/demo/bucket_7 --print-value-only | grep -q '"force"' \
  && { echo "❌ ключ несвежего abort содержит force (должен опускаться)"; exit 1; }
c="$(code -X DELETE "$BASE/api/clusters/demo/moves/bucket_7")"
[ "$c" = 204 ] || { echo "❌ снятие несвежего abort = $c"; exit 1; }
c="$(code -X POST "$BASE/api/clusters/demo/moves/abort" -H 'Content-Type: application/json' -d '{"bucket":0}')"
[ "$c" = 409 ] || { echo "❌ abort ACTIVE-бакета = $c, ожидался 409"; exit 1; }
ect del /clusters/demo/buckets/status/bucket_7 >/dev/null
echo "  abort: несвежий 201 (force опущен); ACTIVE → 409"

# ── 3) abort: routing==target без force → 409 «осознанно».
ect put /clusters/demo/buckets/status/bucket_2 \
  "{\"bucket\":\"bucket_2\",\"state\":\"FROZEN\",\"owner\":\"s1\",\"target\":\"s1\",\"started_unix\":$(( $(now) - 300 )),\"updated_unix\":$(( $(now) - 300 )),\"phase\":\"cutover-wait\"}" >/dev/null
resp="$(api -X POST "$BASE/api/clusters/demo/moves/abort" -H 'Content-Type: application/json' -d '{"bucket":2}')" || true
echo "$resp" | jq -e '.detail // empty' 2>/dev/null | grep -q "осознанно" \
  || { echo "❌ abort routing==target: ожидался 409 с «осознанно», получено: $resp"; exit 1; }
ect del /clusters/demo/buckets/status/bucket_2 >/dev/null
echo "  abort: routing==target без force → 409 «осознанно»"

# ── 4) rollback: ACTIVE-бакет → 201 (op=rollback); очередь панели видит; снятие → 204.
c="$(code -X POST "$BASE/api/clusters/demo/moves/rollback" -H 'Content-Type: application/json' -d '{"buckets":[6]}')"
[ "$c" = 201 ] || { echo "❌ rollback ACTIVE = $c, ожидался 201"; exit 1; }
ect get /pgworker/moves/demo/bucket_6 --print-value-only | grep -q '"op":"rollback"' \
  || { echo "❌ ключ rollback-заявки не содержит op=rollback"; exit 1; }
for i in $(seq 1 10); do
  api /api/clusters/demo | jq -e 'any(.pendingMoves[]; .bucketId == 6 and .op == "rollback")' >/dev/null 2>&1 && break; sleep 1;
done
api /api/clusters/demo | jq -e 'any(.pendingMoves[]; .bucketId == 6 and .op == "rollback")' >/dev/null \
  || { echo "❌ очередь панели не видит rollback-заявку bucket_6"; exit 1; }
c="$(code -X DELETE "$BASE/api/clusters/demo/moves/bucket_6")"
[ "$c" = 204 ] || { echo "❌ снятие rollback = $c"; exit 1; }
echo "  rollback: 201 → очередь панели видит → снятие 204"

# ── 5) rollback-заявка остаётся: процесс отвергает (нет подписки) → work-журнал.
ect put /pgworker/moves/demo/bucket_4 \
  '{"op":"rollback","requested_unix":'"$(( $(now) + 1 ))"',"requested_by":"e2e"}' >/dev/null
for i in $(seq 1 30); do
  api /api/clusters/demo | jq -e '.work != null and .work.op == "rollback" and .work.phase == "rejected" and (.work.lastError != null)' >/dev/null 2>&1 && break; sleep 2;
done
api /api/clusters/demo | jq -e '.work != null and .work.op == "rollback" and .work.phase == "rejected" and (.work.lastError | test("подписк|re-copy"; "i"))' >/dev/null \
  || { echo "❌ work-журнал не показал отвергнутый rollback (op/phase/lastError)"; exit 1; }
[ -z "$(ect get /pgworker/moves/demo/bucket_4 --print-value-only 2>/dev/null)" ] \
  || { echo "❌ отвергнутая заявка bucket_4 не исчезла из очереди"; exit 1; }
echo "  отвергнутый rollback: заявка исчезла, причина — в «Журнале воркера»"

# ── 6) finalize: oldShard=владельцу → 409; несуществующий → 404; валидный → 201.
c="$(code -X POST "$BASE/api/clusters/demo/moves/finalize" -H 'Content-Type: application/json' -d '{"bucket":0,"oldShard":"s1"}')"
[ "$c" = 409 ] || { echo "❌ finalize oldShard=owner = $c, ожидался 409"; exit 1; }
c="$(code -X POST "$BASE/api/clusters/demo/moves/finalize" -H 'Content-Type: application/json' -d '{"bucket":0,"oldShard":"s9"}')"
[ "$c" = 404 ] || { echo "❌ finalize oldShard=s9 = $c, ожидался 404"; exit 1; }
c="$(code -X POST "$BASE/api/clusters/demo/moves/finalize" -H 'Content-Type: application/json' -d '{"bucket":0,"oldShard":"s2"}')"
[ "$c" = 201 ] || { echo "❌ finalize oldShard=s2 = $c, ожидался 201"; exit 1; }
ect get /pgworker/moves/demo/bucket_0 --print-value-only | grep -q '"old_shard":"s2"' \
  || { echo "❌ ключ finalize без old_shard"; exit 1; }
c="$(code -X DELETE "$BASE/api/clusters/demo/moves/bucket_0")"
[ "$c" = 204 ] || { echo "❌ снятие finalize = $c"; exit 1; }
echo "  finalize: 409 владелец / 404 нет шарда / 201 (old_shard в ключе) / снятие 204"

# ── 7) негативы: пустой buckets → 400; несуществующий кластер → 404.
c="$(code -X POST "$BASE/api/clusters/demo/moves/rollback" -H 'Content-Type: application/json' -d '{"buckets":[]}')"
[ "$c" = 400 ] || { echo "❌ rollback пустой buckets = $c, ожидался 400"; exit 1; }
c="$(code -X POST "$BASE/api/clusters/nope/moves/abort" -H 'Content-Type: application/json' -d '{"bucket":0}')"
[ "$c" = 404 ] || { echo "❌ abort неизвестный кластер = $c, ожидался 404"; exit 1; }
echo "  негативы: пустой buckets → 400; неизвестный кластер → 404"

# Финал: очередь демо-кластера пуста (все заявки сняты/исполнены).
for i in $(seq 1 15); do
  api /api/clusters/demo | jq -e '(.pendingMoves | length) == 0' >/dev/null 2>&1 && break; sleep 1;
done
api /api/clusters/demo | jq -e '(.pendingMoves | length) == 0' >/dev/null \
  || { echo "❌ очередь заявок демо не пуста после чека"; exit 1; }
echo "✓ 60-move-ops: move-ops через API панели — все шаги зелёные"
```

- [ ] **Шаг 2. Хроматость + права**

Run: `chmod +x dev-stand/adminpanel/checks/60-move-ops.sh && bash -n dev-stand/adminpanel/checks/60-move-ops.sh` → синтаксис ок.

- [ ] **Шаг 3. Прогон на живом стенде**

Run (стенд должен быть поднят; если нет — `dev-stand/adminpanel/checks/00-up.sh`):
```bash
cd dev-stand/adminpanel && ./checks/60-move-ops.sh
```
Expected: `✓ 60-move-ops: move-ops через API панели — все шаги зелёные`. Если шаг 5 (отвергнутый rollback) не дожидается журнала за 60 c — смотреть `docker logs deploy-pgworker-1` и текст `work.lastError`; при расхождении текста процесса поправить jq-регэксп шага 5 на фактический текст (guard: проверка «rejected + причина», не конкретная формулировка).

- [ ] **Шаг 4. Commit**

```bash
git add dev-stand/adminpanel/checks/60-move-ops.sh
git commit -m "test(stand): t07 — e2e-чек 60 move-ops (abort guard'ы свежести/flip, rollback, finalize, снятие заявок, work-журнал)"
```

---

### Task C2: Финальная верификация задачи

- [ ] **Шаг 1. Полный прогон**

```bash
dotnet build src/PgWorker.slnx
dotnet test src/tests/PgWorker.UnitTests
dotnet test src/tests/PgWorker.IntegrationTests --filter "FullyQualifiedName~Api"
dotnet test src/tests/AdminPanel.UnitTests
dotnet test src/tests/AdminPanel.IntegrationTests --filter "FullyQualifiedName~MovesApiTests|FullyQualifiedName~ClusterDetailsWorkApiTests"
cd frontend && npm run typecheck && npm run build
```
Expected: всё зелёное, 0 warnings (критерий приёмки 1).

- [ ] **Шаг 2. Самопроверка критериев спеки (§10)** — чек-лист по пунктам 2–7: коды/тексты новых эндпоинтов; идемпотентность/конфликты; guard'ы abort; UI-элементы на месте (вкладки/модалки/журнал); отвергнутая заявка видна в work ≤ 2 тиков; регресс move без изменений.

- [ ] **Шаг 3. Commit (если остались правки по итогам прогона)**

```bash
git add -A && git commit -m "chore(t07): финальная верификация — сборки/тесты/e2e зелёные"
```

---

## Самопроверка плана (выполнена при написании)

- **Покрытие спеки:** §5.1 → A1; §5.3 → A2; §5.2/§5.4 → A3–A6 (rollback/finalize/abort/cancel + исключения + маршруты + DI); §7 unit (валидаторы + guard'ы + roundtrip) → A3–A7, integration → `MoveOpsApiTests.cs` (A3–A6); §6.1 → B1–B2; §6.2 фронтенд → B3–B5; §7 стенд → C1; §8 волны = порядок задач; §10 критерии → C2.
- **Типы/имены сверены:** `MoveTickets.TicketBody(...)` одинаков в A1/A3/A4/A5/A7; `RollbackQueuedDto`/`BucketFinalizeQueuedDto`/`BucketAbortQueuedDto`/`WorkDto` — едины между воркером, панелью и фронтом (camelCase на фронте — сериализация Web-канона); `AbortBucketHandler` получает `MovesRuntimeOptions` через `Moves.ToRuntime(Thresholds)` — тот же источник, что у `MoveProcess` (Program.cs:295); `Kv(Key, Value, ModRevision)`/`TxnResult(Succeeded)` — сигнатуры сверены с `PgWorker.Etcd.Client`.
- **Плейсхолдеров нет** — каждый шаг содержит конкретный код/команду/критерий.
