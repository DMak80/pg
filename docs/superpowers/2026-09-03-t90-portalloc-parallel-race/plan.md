# t90-portalloc-parallel-race Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Устранить гонку параллельного выделения портов глобальным lease-клэймом `/pgworker/locks/portalloc` на секцию довыделения «чтение busy → выбор троек → запись portalloc».

**Architecture:** Новый класс `PortAllocLock` (паттерн ClaimStore: txn `version==0` + put-with-lease TTL 15 с, release = del под `ValueEqual` + revoke). Три процесса (Provisioning/AddShard/Adoption) выполняют довыделение только под локом; «не взял» → InProgress с журнальной фазой `waiting-portalloc-lock`. Быстрый пред-выход до лока — `PortPlanConvergence.AllConfirmed` (записи подтверждены фактом/object — busy-чтение ничего бы не изменило): в ProvisioningProcess — по `wanted` при `!adopted.Changed`; в AdoptionProcess — по полному множеству ключей-кандидатов `"{shard}/{name}"` при `changed=false` (недобор кандидата без записи → false → лок обязателен). Инвариант (spec §3.2): порты, выбранные под локом (allocate-ветка), публикуются ДО release — блок `if (changed) { put portalloc }` всегда внутри критической секции; вне её — только записи, не являющиеся portalloc (dsn, EnsureNode).

**Tech Stack:** .NET 10, C# (`Nullable=enable`, `TreatWarningsAsErrors=true`), etcd HTTP JSON gateway `/v3/*` (txn/lease), xUnit + FluentAssertions, Testcontainers (реальный etcd).

**Spec:** `docs/superpowers/2026-09-03-t90-portalloc-parallel-race/spec.md` (в этом же каталоге). Контракт уже обновлён: `arch/14-pgworker.md` §2.4/§3.3, `arch/roadmap/kafkaworker.md` (t91).

## Global Constraints

- Язык документации и комментариев — русский; идентификаторы — английские (AGENTS.md).
- `TreatWarningsAsErrors=true` — код без ворнингов (`src/Directory.Build.props`).
- Порты в тестах — динамические: никаких хардкод-хост-портов; Testcontainers `assignRandomHostPort: true` (EtcdFixture уже так устроена).
- Таймауты интеграционных фикстур короткие: `BrokerBootSec`-подобные бюджеты ≤ 100 с; инт-тест лока не ждёт TTL — берёт/освобождает явно.
- Решение `src/PgWorker.slnx`; все команды — из корня worktree `/Users/demakaev/ZCodeProject/worktrees/fix-t90-portalloc-parallel-race`.
- Формат `/pgworker/portalloc/<C>` НЕ меняется; модель аллокации «первый свободный» НЕ меняется.
- KafkaWorker не трогаем (отдельная задача `t91-kafka-portalloc-race`).
- Комментарии тестов — по AAA (Arrange/Act/Assert).

Все пути ниже — от корня worktree: `/Users/demakaev/ZCodeProject/worktrees/fix-t90-portalloc-parallel-race`.

---

### Task 1: PortAllocLock + PortLockBusyException + юнит-тесты

**Files:**
- Modify: `src/tests/PgWorker.UnitTests/Provisioning/Fakes.cs` (FakeEtcd: инъекция сбоя txn)
- Create: `src/PgWorker.Etcd/Coordination/PortAllocLock.cs`
- Test: `src/tests/PgWorker.UnitTests/Etcd/PortAllocLockTests.cs`

**Interfaces:**
- Consumes: `IEtcdGateway` (`TxnAsync`, `LeaseGrantAsync`, `LeaseRevokeAsync`), `TxnCompare.NotExists/ValueEqual`, `TxnOp.Put/Delete`, `Result<T>` — всё уже есть.
- Produces (для задач 3–7):
  - `PgWorker.Etcd.Coordination.PortAllocLock(string[] endpoints, IEtcdGateway gateway, TimeProvider clock, string instanceId)`
  - `Task<Result<bool>> TryAcquireAsync(CancellationToken ct)` — `true`=взял, `false`=занят другим (не ошибка), `Failed`=сбой etcd.
  - `Task ReleaseAsync()` — идемпотентен; без захвата — no-op.
  - `const string PortAllocLock.Key = "/pgworker/locks/portalloc"`.
  - `PgWorker.Etcd.Coordination.PortLockBusyException` (наследник `Exception`).

- [ ] **Step 1: Написать падающий тест (захват/занятость/release)**

Создать `src/tests/PgWorker.UnitTests/Etcd/PortAllocLockTests.cs`:

```csharp
using FluentAssertions;
using PgWorker.Etcd.Coordination;
using PgWorker.UnitTests.Provisioning;
using Xunit;

namespace PgWorker.UnitTests.Etcd;

// PortAllocLock (t90, arch/14 §2.4/§3.3): глобальный portalloc-клэйм —
// взаимоисключение секции довыделения портов между кластерами/инстансами.
public class PortAllocLockTests
{
    private const string Ep = "http://etcd:2379";

    // AAA: первый захват проходит и пишет ключ с instance держателя;
    // второй (другой инстанс) получает false — не ошибка, не перезаписывает.
    [Fact]
    public async Task TryAcquire_SecondInstance_GetsFalse()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd();
        var first = new PortAllocLock([Ep], etcd, TimeProvider.System, "inst-1");
        var second = new PortAllocLock([Ep], etcd, TimeProvider.System, "inst-2");

        // Act
        var firstAcquired = await first.TryAcquireAsync(CancellationToken.None);
        var secondAcquired = await second.TryAcquireAsync(CancellationToken.None);

        // Assert
        firstAcquired.IsSuccess.Should().BeTrue();
        firstAcquired.Value.Should().BeTrue();
        secondAcquired.IsSuccess.Should().BeTrue();
        secondAcquired.Value.Should().BeFalse();
        etcd.Store[PortAllocLock.Key].Value.Should().Contain("inst-1");
    }

    // AAA: release (del + revoke) освобождает — повторный захват другим инстансом проходит;
    // повторный ReleaseAsync — no-op.
    [Fact]
    public async Task Release_AllowsTakeover_AndIsIdempotent()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd();
        var first = new PortAllocLock([Ep], etcd, TimeProvider.System, "inst-1");
        var second = new PortAllocLock([Ep], etcd, TimeProvider.System, "inst-2");
        (await first.TryAcquireAsync(CancellationToken.None)).Value.Should().BeTrue();

        // Act
        await first.ReleaseAsync();
        await first.ReleaseAsync(); // повтор — no-op
        var reclaimed = await second.TryAcquireAsync(CancellationToken.None);

        // Assert
        reclaimed.Value.Should().BeTrue();
        etcd.Store[PortAllocLock.Key].Value.Should().Contain("inst-2");
    }

    // AAA: повторный TryAcquire тем же объектом при живом захвате — true без нового txn.
    [Fact]
    public async Task TryAcquire_AlreadyHeld_ReturnsTrue()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd();
        var locks = new PortAllocLock([Ep], etcd, TimeProvider.System, "inst-1");
        (await locks.TryAcquireAsync(CancellationToken.None)).Value.Should().BeTrue();
        var txnsBefore = etcd.Txns.Count;

        // Act
        var again = await locks.TryAcquireAsync(CancellationToken.None);

        // Assert
        again.Value.Should().BeTrue();
        etcd.Txns.Count.Should().Be(txnsBefore); // без нового txn — захват уже наш
    }

    // AAA: лок перехвачен (lease истёк, ключ перезаписан чужим value) —
    // release НЕ удаляет чужой ключ (del под compare ValueEqual).
    [Fact]
    public async Task Release_AfterTakeover_DoesNotDeleteForeignKey()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd();
        var mine = new PortAllocLock([Ep], etcd, TimeProvider.System, "inst-1");
        (await mine.TryAcquireAsync(CancellationToken.None)).Value.Should().BeTrue();
        // имитация истечения TTL и перехвата: ключ перезаписан чужим value
        etcd.Seed(PortAllocLock.Key, """{"instance":"inst-2","since_unix":1}""");

        // Act
        await mine.ReleaseAsync();

        // Assert: чужой ключ жив — del под ValueEqual(наш value) не сошёлся
        etcd.Store[PortAllocLock.Key].Value.Should().Contain("inst-2");
    }

    // AAA: сбой etcd на txn → Result.Failed (процесс пойдёт в обычный бэкофф, не InProgress-тихо).
    [Fact]
    public async Task TryAcquire_EtcdTxnFailure_ReturnsFailed()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd
        {
            TxnFault = _ => Result<PgWorker.Etcd.Client.TxnResult>.Failed(
                new ApplicationException("etcd: connection refused")),
        };
        var locks = new PortAllocLock([Ep], etcd, TimeProvider.System, "inst-1");

        // Act
        var acquired = await locks.TryAcquireAsync(CancellationToken.None);

        // Assert
        acquired.IsSuccess.Should().BeFalse();
    }
}
```

- [ ] **Step 2: Добавить TxnFault в FakeEtcd**

В `src/tests/PgWorker.UnitTests/Provisioning/Fakes.cs`, в класс `FakeEtcd` — рядом с `RangeFault` (строка ~29):

```csharp
        // Сбой-инъекция txn (t90: ошибка захвата PortAllocLock → Result.Failed).
        public Func<TxnRequest, Result<TxnResult>>? TxnFault { get; set; }
```

В начале метода `TxnAsync` (перед `lock (_gate)`):

```csharp
            if (TxnFault?.Invoke(req) is { } failed)
                return Task.FromResult(failed);
```

- [ ] **Step 3: Запустить тест — убедиться, что падает (нет класса)**

Run: `dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj --filter "FullyQualifiedName~PortAllocLockTests"`
Expected: ошибка компиляции `PortAllocLock` не существует.

- [ ] **Step 4: Реализовать PortAllocLock**

Создать `src/PgWorker.Etcd/Coordination/PortAllocLock.cs`:

```csharp
using System.Text.Json;
using System.Text.Json.Serialization;
using PgWorker.Core;
using PgWorker.Etcd.Client;

namespace PgWorker.Etcd.Coordination;

/// <summary>
/// Глобальный portalloc-клэйм (t90, arch/14 §2.4/§3.3): взаимоисключение секции
/// довыделения портов «чтение занятости → выбор троек → запись portalloc» —
/// пер-кластерные клэймы кросс-кластерную гонку не закрывают (два параллельно
/// сеемых кластера читают /pgworker/portalloc/* до первой записи соседа и
/// выбирают одинаковые порты). Захват — txn version==0 + put-with-lease TTL 15 с
/// (паттерн /pgworker/leader); keepalive не нужен — секция короткая (единицы
/// секунд ≪ TTL). Освобождение — явное: del под compare ValueEqual(наш value;
/// lease истёк и лок перехвачен — чужой ключ не трогаем) + revoke lease.
/// «Занят другим» — не ошибка: вызывающий возвращает InProgress, следующий тик
/// (~5 с) повторяет; смерть держателя гасит TTL ≤ 15 с — takeover без оператора.
/// </summary>
public sealed class PortAllocLock(
    string[] endpoints, IEtcdGateway gateway, TimeProvider clock, string instanceId)
{
    public const string Key = "/pgworker/locks/portalloc";
    private const int TtlSec = 15;

    private readonly object _sync = new();
    private long? _lease;
    private string? _payload; // наш value — compare «чужой-не-трогаем» при release

    /// <summary>Захват: true — держим; false — занят другим инстансом (НЕ ошибка).</summary>
    public async Task<Result<bool>> TryAcquireAsync(CancellationToken ct)
    {
        lock (_sync)
        {
            if (_lease is not null)
                return Result<bool>.Success(true); // уже наш — секция ещё не отпущена
        }

        var grant = await WithFailoverAsync(endpoint => gateway.LeaseGrantAsync(endpoint, TtlSec, ct));
        if (!grant.IsSuccess)
            return Result<bool>.Failed(grant.Error!);

        var payload = JsonSerializer.Serialize(new LockPayload(instanceId, Now()));
        var txn = await WithFailoverAsync(endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of(
                [TxnCompare.NotExists(Key)],
                [new TxnOp.Put(Key, payload, grant.Value)]),
            ct));
        if (!txn.IsSuccess)
        {
            await RevokeSilentlyAsync(grant.Value);
            return Result<bool>.Failed(txn.Error!);
        }

        if (!txn.Value.Succeeded)
        {
            await RevokeSilentlyAsync(grant.Value);
            return Result<bool>.Success(false); // занят другим инстансом — не ошибка
        }

        lock (_sync)
        {
            _lease = grant.Value;
            _payload = payload;
        }

        return Result<bool>.Success(true);
    }

    /// <summary>Освобождение: del под compare ValueEqual(наш value) + revoke lease.
    /// Отказ del — best-effort (ключ гаснет по TTL); повтор/без захвата — no-op.</summary>
    public async Task ReleaseAsync()
    {
        long lease;
        string payload;
        lock (_sync)
        {
            if (_lease is not { } l)
                return;
            lease = l;
            payload = _payload!;
            _lease = null;
            _payload = null;
        }

        // Чужой лок не трогаем: compare не сошёлся (lease истёк, ключ перезаписан) → del не выполнится.
        _ = await WithFailoverAsync(endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of(
                [TxnCompare.ValueEqual(Key, payload)],
                [new TxnOp.Delete(Key, Prefix: false)]),
            CancellationToken.None));
        await RevokeSilentlyAsync(lease);
    }

    private async Task RevokeSilentlyAsync(long lease)
    {
        try
        {
            await WithFailoverAsync(endpoint => gateway.LeaseRevokeAsync(endpoint, lease, CancellationToken.None));
        }
        catch
        {
            // best-effort: истечёт по TTL
        }
    }

    private long Now() => clock.GetUtcNow().ToUnixTimeSeconds();

    // Failover по endpoints: первый успешный ответ выигрывает (паттерн ClaimStore).
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

    // Value ключа /pgworker/locks/portalloc (arch/14 §3.3).
    private sealed record LockPayload(
        [property: JsonPropertyName("instance")] string Instance,
        [property: JsonPropertyName("since_unix")] long SinceUnix);
}

/// <summary>Сигнал «глобальный portalloc-клэйм занят другим инстансом» (t90):
/// НЕ фейл — без бэкоффа; процесс возвращает InProgress (waiting-portalloc-lock),
/// следующий тик повторяет. Маркер-тип для ветки обработки рядом с FailAsync.</summary>
public sealed class PortLockBusyException() : Exception(
    $"{PortAllocLock.Key}: занят другим инстансом — повторить следующим тиком");
```

- [ ] **Step 5: Прогнать тесты лока**

Run: `dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj --filter "FullyQualifiedName~PortAllocLockTests"`
Expected: 5 PASS.

- [ ] **Step 6: Прогнать сборку проекта Etcd (ворнинги = ошибки)**

Run: `dotnet build src/PgWorker.Etcd/PgWorker.Etcd.csproj`
Expected: Build succeeded, 0 warnings.

- [ ] **Step 7: Commit**

```bash
git add src/PgWorker.Etcd/Coordination/PortAllocLock.cs src/tests/PgWorker.UnitTests/Provisioning/Fakes.cs src/tests/PgWorker.UnitTests/Etcd/PortAllocLockTests.cs
git commit -m "t90: PortAllocLock — глобальный portalloc-клэйм (txn NotExists + lease TTL 15с, release под ValueEqual) + юнит-тесты"
```

---

### Task 2: PortPlanConvergence.AllConfirmed + юнит-тест

**Files:**
- Modify: `src/PgWorker.Core/Planning/PortPlanConvergence.cs` (добавить метод после `ConfirmedFact`)
- Test: `src/tests/PgWorker.UnitTests/Planning/PortAllocatorTests.cs` (дописать класс тестов в конец файла ИЛИ создать рядом; ниже — создание отдельного класса в том же файле)

**Interfaces:**
- Consumes: `NodeAddress`/`NodePorts` (`PgWorker.Core.Model`), приватный `MatchesFact` уже в классе.
- Produces (для задач 3, 5): `static bool PortPlanConvergence.AllConfirmed(IReadOnlyDictionary<string, NodeAddress> existing, IReadOnlyDictionary<string, IReadOnlySet<(string Host, int Port)>> selfFactByNode, IReadOnlyCollection<string> wanted)`.

- [ ] **Step 1: Написать падающий тест**

В конец `src/tests/PgWorker.UnitTests/Planning/PortAllocatorTests.cs` добавить (usings файла уже содержат `PgWorker.Core.Model`, `PgWorker.Core.Planning`; при необходимости дозировать):

```csharp
// t90: быстрый пред-выход до portalloc-клэйма — всё закреплено и detach ничего
// бы не снял (object не трогается R9; прочее подтверждено фактом своей ноды).
public class PortPlanConvergenceAllConfirmedTests
{
    private static NodeAddress Addr(string host = "h1", int pg = 15000)
        => new(host, new NodePorts(pg, pg + 3000, pg + 1500));

    private static IReadOnlySet<(string, int)> Fact(NodeAddress a) => new HashSet<(string, int)>
    {
        (a.Host, a.Ports.Pg), (a.Host, a.Ports.Patroni), (a.Host, a.Ports.Doorman),
    };

    [Fact]
    public void AllConfirmed_AllMatchedFact_True()
    {
        // Arrange
        var addr = Addr();
        var existing = new Dictionary<string, NodeAddress> { ["s1/n1"] = addr };
        var facts = new Dictionary<string, IReadOnlySet<(string, int)>> { ["s1/n1"] = Fact(addr) };

        // Act
        var confirmed = PortPlanConvergence.AllConfirmed(existing, facts, ["s1/n1"]);

        // Assert
        confirmed.Should().BeTrue();
    }

    [Fact]
    public void AllConfirmed_NodeWithoutFact_False()
    {
        // Arrange — контейнер ноды не найден инспекцией: detach мог бы снять коллизию
        var existing = new Dictionary<string, NodeAddress> { ["s1/n1"] = Addr() };

        // Act
        var confirmed = PortPlanConvergence.AllConfirmed(
            existing, new Dictionary<string, IReadOnlySet<(string, int)>>(), ["s1/n1"]);

        // Assert
        confirmed.Should().BeFalse();
    }

    [Fact]
    public void AllConfirmed_MissingRecord_False()
    {
        // Arrange — недобор: записи нет вовсе — нужна секция под клэймом
        // Act
        var confirmed = PortPlanConvergence.AllConfirmed(
            new Dictionary<string, NodeAddress>(),
            new Dictionary<string, IReadOnlySet<(string, int)>>(), ["s1/n1"]);

        // Assert
        confirmed.Should().BeFalse();
    }

    [Fact]
    public void AllConfirmed_ObjectRecord_TrueWithoutFact()
    {
        // Arrange — object-запись (усыновлённая): detach не трогает (R9)
        var existing = new Dictionary<string, NodeAddress>
            { ["s1/n1"] = Addr() with { Object = "foreign-container" } };

        // Act
        var confirmed = PortPlanConvergence.AllConfirmed(
            existing, new Dictionary<string, IReadOnlySet<(string, int)>>(), ["s1/n1"]);

        // Assert
        confirmed.Should().BeTrue();
    }

    [Fact]
    public void AllConfirmed_FactDiverges_False()
    {
        // Arrange — факт контейнера на других портах: запись не подтверждена
        var existing = new Dictionary<string, NodeAddress> { ["s1/n1"] = Addr() };
        var facts = new Dictionary<string, IReadOnlySet<(string, int)>>
            { ["s1/n1"] = Fact(Addr(pg: 15005)) };

        // Act
        var confirmed = PortPlanConvergence.AllConfirmed(existing, facts, ["s1/n1"]);

        // Assert
        confirmed.Should().BeFalse();
    }
}
```

- [ ] **Step 2: Запустить — убедиться, что падает**

Run: `dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj --filter "FullyQualifiedName~PortPlanConvergenceAllConfirmedTests"`
Expected: ошибка компиляции `AllConfirmed` не существует.

- [ ] **Step 3: Реализовать метод**

В `src/PgWorker.Core/Planning/PortPlanConvergence.cs` после метода `ConfirmedFact`:

```csharp
    /// <summary>Быстрый пред-выход (t90): все wanted-записи закреплены и detach
    /// их снять не может — object-записи не трогаются (R9), прочие подтверждены
    /// фактом своего живого контейнера (MatchesFact). Чтение busy под глобальным
    /// portalloc-клэймом ничего бы не изменило — лок можно не брать (тики
    /// waiting-patroni не соперничают за клэйм, arch/14 §2.4).</summary>
    public static bool AllConfirmed(
        IReadOnlyDictionary<string, NodeAddress> existing,
        IReadOnlyDictionary<string, IReadOnlySet<(string Host, int Port)>> selfFactByNode,
        IReadOnlyCollection<string> wanted)
        => wanted.All(key =>
            existing.TryGetValue(key, out var addr)
            && (addr.Object is not null
                || (selfFactByNode.TryGetValue(key, out var own) && MatchesFact(addr, own))));
```

- [ ] **Step 4: Прогнать**

Run: `dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj --filter "FullyQualifiedName~PortPlanConvergenceAllConfirmedTests"`
Expected: 5 PASS.

- [ ] **Step 5: Commit**

```bash
git add src/PgWorker.Core/Planning/PortPlanConvergence.cs src/tests/PgWorker.UnitTests/Planning/PortAllocatorTests.cs
git commit -m "t90: PortPlanConvergence.AllConfirmed — пред-выход без portalloc-клэйма для подтверждённых записей"
```

---

### Task 3: ProvisioningProcess под portalloc-клэймом

**Files:**
- Modify: `src/PgWorker.Provisioning/Processes/ProvisioningProcess.cs` (ctor ~строка 40; `TickAsync` ~строка 94; `PlanPortsAsync` ~строки 225–296)
- Modify: `src/tests/PgWorker.UnitTests/Provisioning/ProvisioningProcessTests.cs` (ДВЕ конструкции процесса: `NewRig` ~строка 149 и прямая конструкция ~строка 287 в тесте `Tick_NoRoutingKeys_WaitingKeys_NoDocker`; новый тест)

**Interfaces:**
- Consumes: `PortAllocLock.TryAcquireAsync/ReleaseAsync`, `PortLockBusyException`, `PortPlanConvergence.AllConfirmed` (задачи 1–2).
- Produces: ctor `ProvisioningProcess` получает новый параметр `PortAllocLock portLock` (после `PortAllocIndex portAlloc`, перед `snapshot`) — правки задачи 6 (Program.cs) и ОБЕИХ конструкций в тестах.

- [ ] **Step 1: Написать падающие тесты**

В `src/tests/PgWorker.UnitTests/Provisioning/ProvisioningProcessTests.cs`:

1a. Обновить `NewRig` (~строка 149) — в конструкцию `ProvisioningProcess` после `portAlloc` добавить `new PortAllocLock([Ep], etcd, TimeProvider.System, claims.InstanceId),`:

```csharp
        var process = new ProvisioningProcess(
            etcd, [Ep], driver, sql, Probe(patroniResponse, trace, identityByEndpoint), claims, journal,
            opts ?? Opts, Secrets,
            appSecret, new AppParamsEnsurer(etcd, [Ep], "sslmode=require"), EtcdEndp, portAlloc,
            new PortAllocLock([Ep], etcd, TimeProvider.System, claims.InstanceId), snapshot: null);
```

1b. Обновить вторую прямую конструкцию (~строка 287, тест `Tick_NoRoutingKeys_WaitingKeys_NoDocker`) — тот же параметр после `new PortAllocIndex(...)` (в этом тесте локальная переменная `claims` уже есть — ~строка 283):

```csharp
        var process = new ProvisioningProcess(
            etcd, [Ep], driver, new Fakes.FakeSql(), Probe(_ => Patroni("shard1a")),
            claims, journal, Opts, Secrets, new AppSecretEnsurer(etcd, [Ep]),
            new AppParamsEnsurer(etcd, [Ep], "sslmode=require"), EtcdEndp,
            new PortAllocIndex(etcd, [Ep], NullLogger<PortAllocIndex>.Instance),
            new PortAllocLock([Ep], etcd, TimeProvider.System, claims.InstanceId), snapshot: null);
```

1c. Добавить два теста (в конец класса):

```csharp
    // AAA (t90): глобальный portalloc-клэйм занят чужим инстансом — тик
    // возвращает InProgress (waiting-portalloc-lock), portalloc НЕ пишется,
    // контейнеры не создаются; после освобождения следующий тик проходит.
    [Fact]
    public async Task Tick_PortAllocLockBusy_WaitsWithoutMutations()
    {
        // Arrange — свежий кластер (порт-недобор), лок держит «другой инстанс»
        var rig = await NewRig(_ => DeadPatroni(), identityByEndpoint: EmptyIdentity);
        var holder = new PortAllocLock([Ep], rig.Etcd, TimeProvider.System, "other");
        (await holder.TryAcquireAsync(CancellationToken.None)).Value.Should().BeTrue();

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: не фейл, не мутации — ждём лок следующим тиком
        outcome.IsSuccess.Should().BeTrue();
        outcome.Value.Should().Be(ProcessOutcome.InProgress);
        rig.Etcd.Store.Should().NotContainKey("/pgworker/portalloc/shop");
        rig.Driver.EnsuredNodes.Should().BeEmpty();
        var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
        work.Value!.Phase.Should().Be("waiting-portalloc-lock");

        // Act-2: держатель освободил — тик доводит планирование
        await holder.ReleaseAsync();
        var second = await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert-2: порты закреплены, контейнеры создаются
        second.Value.Should().Be(ProcessOutcome.InProgress);
        rig.Etcd.Store.Should().ContainKey("/pgworker/portalloc/shop");
        rig.Driver.EnsuredNodes.Should().NotBeEmpty();
    }

    // AAA (t90): всё закреплено и каждая запись подтверждена живым фактом своей
    // ноды — быстрый пред-выход ДО лока: занятый чужим лок не мешает тику.
    [Fact]
    public async Task Tick_AllConfirmedByFact_SkipsLock()
    {
        // Arrange — первый тик аллоцировал порты; подкладываем факты контейнеров
        var rig = await NewRig(_ => DeadPatroni(), identityByEndpoint: EmptyIdentity);
        await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);
        var alloc = Portalloc.Parse("shop", rig.Etcd.Store["/pgworker/portalloc/shop"].Value);
        rig.Driver.InspectResult = alloc.Value.ToDictionary(
            p => p.Key.Split('/')[1],
            p => new DiscoveredNode(
                p.Key.Split('/')[1], p.Value.Host, $"pgw-shop-{p.Key.Replace('/', '-')}",
                p.Value.Ports.Pg, p.Value.Ports.Patroni, p.Value.Ports.Doorman));
        var holder = new PortAllocLock([Ep], rig.Etcd, TimeProvider.System, "other");
        (await holder.TryAcquireAsync(CancellationToken.None)).Value.Should().BeTrue();

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), CancellationToken.None);

        // Assert: не застряли на локе — тик идёт дальше штатного цикла
        outcome.Value.Should().Be(ProcessOutcome.InProgress);
        var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
        work.Value!.Phase.Should().Be("waiting-patroni");
    }
```

Если файл ещё не имеет `using PgWorker.Etcd.Coordination;` — добавить (для `PortAllocLock`).

- [ ] **Step 2: Запустить — оба новых теста падают**

Run: `dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj --filter "FullyQualifiedName~ProvisioningProcessTests.Tick_PortAllocLockBusy_WaitsWithoutMutations|FullyQualifiedName~ProvisioningProcessTests.Tick_AllConfirmedByFact_SkipsLock"`
Expected: ошибка компиляции (у процесса нет параметра `portLock`). После правки ctor (Step 3) — `Tick_PortAllocLockBusy` падает на `waiting-patroni`≠`waiting-portalloc-lock`, `Tick_AllConfirmedByFact` падает на `waiting-portalloc-lock`≠`waiting-patroni`.

- [ ] **Step 3: Реализовать (ctor + пред-выход + лок в PlanPortsAsync + ветка в TickAsync)**

3a. Ctor (`src/PgWorker.Provisioning/Processes/ProvisioningProcess.cs`, параметры первичного конструктора) — после `PortAllocIndex portAlloc,` перед `Func<CancellationToken, Task<Result>>? snapshot`:

```csharp
    PortAllocLock portLock,
```

3b. В `TickAsync` заменить блок P1:

```csharp
        // P1: план placement + порты, закрепление portalloc.
        var allocation = await PlanPortsAsync(snap, series, ct);
        if (!allocation.IsSuccess)
            return await FailAsync(cluster, allocation.Error!, "planning", ct, series);
```

на:

```csharp
        // P1: план placement + порты, закрепление portalloc.
        var allocation = await PlanPortsAsync(snap, series, ct);
        if (allocation.Error is PortLockBusyException)
            return await Finish(cluster, "waiting-portalloc-lock", ProcessOutcome.InProgress, ct, series);
        if (!allocation.IsSuccess)
            return await FailAsync(cluster, allocation.Error!, "planning", ct, series);
```

3c. В `PlanPortsAsync` — после `var skipped = adopted.Value.Skipped;` и перед комментарием «Д1 (spec §3.7, живой-Ф7): занятость = ...» и `var dockerBusy = await driver.GetBusyPortsAsync(ct);` вставить:

```csharp
        // t90: быстрый пред-выход ДО глобального portalloc-клэйма — всё
        // закреплено, adoption не изменил и detach ничего бы не снял
        // (AllConfirmed: object не трогается R9, прочее подтверждено фактом):
        // тики waiting-patroni не соперничают за глобальный клэйм (§2.4).
        if (wanted.All(existing.ContainsKey)
            && !adopted.Value.Changed
            && PortPlanConvergence.AllConfirmed(existing, adopted.Value.SelfFactByNode, wanted))
            return await PlannedAsync(existing, cluster, ct, series, skipped);

        // t90: глобальный portalloc-клэйм — секция «busy → detach → allocate →
        // commit portalloc» взаимоисключающа между кластерами/инстансами
        // (гонка параллельного посева, arch/14 §2.4/§3.3). Сбой захвата —
        // обычный фейл (бэкофф); занят — PortLockBusyException → тик-ретрай.
        var acquired = await portLock.TryAcquireAsync(ct);
        if (!acquired.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(acquired.Error!);
        if (!acquired.Value)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(new PortLockBusyException());
        try
        {
```

и обернуть весь остаток метода до конца (от `var dockerBusy = ...` до финального `return await PlannedAsync(existing, cluster, ct, series, skipped);` включительно) в `try { ... }`, закрыв после последнего return:

```csharp
        }
        finally
        {
            await portLock.ReleaseAsync();
        }
```

Внутри try ничего не менять: `dockerBusy → foreignAlloc → busy → DetachColliding → ранний выход → commit существующего → GetHosts → taken → Plan → Allocate → merge → CommitPortAllocAsync → PlannedAsync`.

- [ ] **Step 4: Прогнать новые тесты**

Run: `dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj --filter "FullyQualifiedName~ProvisioningProcessTests.Tick_PortAllocLockBusy_WaitsWithoutMutations|FullyQualifiedName~ProvisioningProcessTests.Tick_AllConfirmedByFact_SkipsLock"`
Expected: 2 PASS.

- [ ] **Step 5: Прогнать весь файл ProvisioningProcessTests (регрессия)**

Run: `dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj --filter "FullyQualifiedName~ProvisioningProcessTests"`
Expected: все PASS (существующее поведение P1 не изменилось: в одиночном режиме лок всегда берётся свободным).

- [ ] **Step 6: Commit**

```bash
git add src/PgWorker.Provisioning/Processes/ProvisioningProcess.cs src/tests/PgWorker.UnitTests/Provisioning/ProvisioningProcessTests.cs
git commit -m "t90: ProvisioningProcess P1 — довыделение портов под глобальным portalloc-клэймом, пред-выход AllConfirmed"
```

---

### Task 4: AddShardProcess под portalloc-клэймом

**Files:**
- Modify: `src/PgWorker.Provisioning/Processes/AddShardProcess.cs` (ctor ~строка 29; `TickAsync` блок A2 ~строки 89–93; `PlanShardPortsAsync` ~строки 172–220)
- Modify: `src/tests/PgWorker.UnitTests/Provisioning/AddShardProcessTests.cs` (3 конструкции процесса: строки ~112, ~138, ~321; новый тест)

**Interfaces:**
- Consumes: `PortAllocLock`, `PortLockBusyException` (задача 1).
- Produces: ctor `AddShardProcess` получает `PortAllocLock portLock` после `PortAllocIndex portAlloc` (задача 6 — Program.cs).

- [ ] **Step 1: Написать падающий тест**

В `src/tests/PgWorker.UnitTests/Provisioning/AddShardProcessTests.cs` (при необходимости добавить `using PgWorker.Etcd.Coordination;`):

```csharp
    // AAA (t90): глобальный portalloc-клэйм занят — add-shard ждёт
    // (waiting-portalloc-lock) без мутаций portalloc; после release — доводит.
    // Сид: SeedActiveCluster уже записал /pgworker/portalloc/shop (шарды 1–2),
    // новый шард — shard3 (недобор записей shard3/*).
    [Fact]
    public async Task Tick_PortAllocLockBusy_WaitsWithoutMutations()
    {
        // Arrange — полная декларация нового шарда, лок держит «другой инстанс»
        var rig = await NewRig(_ => DeadPatroni());
        var holder = new PortAllocLock([Ep], rig.Etcd, TimeProvider.System, "other");
        (await holder.TryAcquireAsync(CancellationToken.None)).Value.Should().BeTrue();

        // Act
        var outcome = await rig.Process.TickAsync(await Snapshot(rig.Etcd), "shard3", CancellationToken.None);

        // Assert: не фейл — ждём; portalloc НЕ дописан нодами shard3
        outcome.Value.Should().Be(ProcessOutcome.InProgress);
        rig.Etcd.Store["/pgworker/portalloc/shop"].Value.Should().NotContain("shard3/");
        rig.Driver.EnsuredNodes.Should().BeEmpty();
        var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
        work.Value!.Phase.Should().Be("waiting-portalloc-lock");

        // Act-2 / Assert-2: освобождение — планирование проходит
        await holder.ReleaseAsync();
        var second = await rig.Process.TickAsync(await Snapshot(rig.Etcd), "shard3", CancellationToken.None);
        second.Value.Should().Be(ProcessOutcome.InProgress);
        rig.Etcd.Store["/pgworker/portalloc/shop"].Value.Should().Contain("shard3/");
    }
```

(хелпер `Snapshot(Fakes.FakeEtcd)` и имя нового шарда `shard3` — фактические: `SeedAddDeclaration` сеет `/clusters/shop/shards/shard3/...`).

- [ ] **Step 2: Обновить все конструкции AddShardProcess в тестах**

В каждой из трёх конструкций (`NewRig` и два прямых места) добавить после `new PortAllocIndex(...)`:

```csharp
            new PortAllocLock([Ep], etcd, TimeProvider.System, claims.InstanceId),
```

(в `NewRig` — через переменную `claims`, уже создаваемую хелпером; в прямых конструкциях — по локальной `claims`.)

- [ ] **Step 3: Запустить — тест падает**

Run: `dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj --filter "FullyQualifiedName~AddShardProcessTests.Tick_PortAllocLockBusy_WaitsWithoutMutations"`
Expected: компиляция падает (нет параметра `portLock`) → после Step 4 — падение на фазе.

- [ ] **Step 4: Реализовать**

4a. Ctor — после `PortAllocIndex portAlloc,` добавить `PortAllocLock portLock,`.

4b. В `TickAsync` блок A2 заменить:

```csharp
        var planned = await PlanShardPortsAsync(cluster, shard, ct);
        if (!planned.IsSuccess)
            return await FailAsync(cluster, planned.Error!, "planning", ct);
```

на:

```csharp
        var planned = await PlanShardPortsAsync(cluster, shard, ct);
        if (planned.Error is PortLockBusyException)
            return await Finish(cluster, "waiting-portalloc-lock", ProcessOutcome.InProgress, ct);
        if (!planned.IsSuccess)
            return await FailAsync(cluster, planned.Error!, "planning", ct);
```

4c. В `PlanShardPortsAsync` — после раннего выхода `if (wanted.All(existing.ContainsKey)) return ...;` и перед `var hosts = await driver.GetHostsAsync(ct);` вставить:

```csharp
        // t90: глобальный portalloc-клэйм — секция «hosts → busy → allocate →
        // put» взаимоисключающа между кластерами/инстансами (arch/14 §2.4).
        var acquired = await portLock.TryAcquireAsync(ct);
        if (!acquired.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(acquired.Error!);
        if (!acquired.Value)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(new PortLockBusyException());
        try
        {
```

обернуть остаток метода (от `var hosts = ...` до put включительно:

```csharp
            var put = await PutAsync(PortAllocKey(cluster), Portalloc.Serialize(existing), ct);
            if (!put.IsSuccess)
                return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(put.Error!);
```

) в `try { ... }` и закрыть:

```csharp
        }
        finally
        {
            await portLock.ReleaseAsync();
        }
```

Журнальную запись `plannedPhase` оставить ПОСЛЕ finally (вне try — она не часть критической секции).

- [ ] **Step 5: Прогнать тесты AddShard**

Run: `dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj --filter "FullyQualifiedName~AddShardProcessTests"`
Expected: все PASS.

- [ ] **Step 6: Commit**

```bash
git add src/PgWorker.Provisioning/Processes/AddShardProcess.cs src/tests/PgWorker.UnitTests/Provisioning/AddShardProcessTests.cs
git commit -m "t90: AddShardProcess — порт-аллокация нового шарда под глобальным portalloc-клэймом"
```

---

### Task 5: AdoptionProcess под portalloc-клэймом

**Files:**
- Modify: `src/PgWorker.Provisioning/Processes/AdoptionProcess.cs` (ctor ~строка 25; `TickAsync` ~строки 58–60; `ReconcileAddressesAsync` ~строки 200–361, критическая секция busy → put ~строки 256–295)
- Modify: `src/tests/PgWorker.UnitTests/Provisioning/AdoptionProcessTests.cs` (хелпер `NewAdoption` ~строки 58–78; новый тест)

**Interfaces:**
- Consumes: `PortAllocLock`, `PortLockBusyException`, `PortPlanConvergence.AllConfirmed` (задачи 1–2).
- Produces: ctor `AdoptionProcess` получает `PortAllocLock portLock` после `PortAllocIndex portAlloc` (задача 6 — Program.cs).

**Ключевой инвариант (spec §3.2):** в `ReconcileAddressesAsync` блок `if (changed) { put /pgworker/portalloc/<C>; journal "repaired-portalloc" }` выполняется ТОЛЬКО внутри критической секции под локом — порты allocate-ветки выбираются под локом и публикуются до release (вынос put за finally воспроизводил бы гонку t90: инстанс A выделил порты и отпустил лок, инстанс B берёт лок, не видит записи A, выбирает те же порты). Вне критической секции остаются только операции, не являющиеся записью portalloc: dsn-инвариант (`put /clusters/.../dsn`) и EnsureNode-блок («Дефект-B», `recreated-node`). AD2-merge в `TickAsync` (усыновление недобора по missing, строки ~125–135) кросс-кластерную занятость не читает — вне лока, не меняется.

- [ ] **Step 1: Написать падающий тест**

В `src/tests/PgWorker.UnitTests/Provisioning/AdoptionProcessTests.cs` (сид — фактический хелпер `SnapshotActive(etcd, shards, membersShards)`: кластер `demo`, dsn-шард s1, HA-members сеются парами `{s1a,s1b}` → кандидаты репарации = nodes ∪ members = `{s1/s1a, s1/s1b}`; записи portalloc нет — недобор):

```csharp
    // AAA (t90): репарация адресов с недобором требует глобальный portalloc-клэйм;
    // занят — усыновление ждёт тик (waiting-portalloc-lock) без записи portalloc.
    // Кандидаты = {s1/s1a, s1/s1b} (HA-members), инспекция видит только s1a:
    // merge кладёт факт s1/s1a (changed=true), s1/s1b остаётся недобором —
    // пред-выход AllConfirmed обязан дать false → лок берётся → «не взял» →
    // InProgress БЕЗ каких-либо мутаций portalloc (в т.ч. merge-факта s1a).
    [Fact]
    public async Task Tick_PortAllocLockBusy_WaitsWithoutPortallocWrite()
    {
        // Arrange — Active-кластер demo: dsn-шард s1 с members {s1a, s1b},
        // записи portalloc нет (недобор); инспекция видит канонический контейнер
        // только s1a; глобальный portalloc-клэйм держит «другой инстанс»
        var etcd = new Fakes.FakeEtcd();
        var snap = await SnapshotActive(etcd, ["s1"], ["s1"]);
        var holder = new PortAllocLock([Ep], etcd, TimeProvider.System, "other");
        (await holder.TryAcquireAsync(CancellationToken.None)).Value.Should().BeTrue();
        var (adoption, _, _) = await NewAdoption(
            etcd,
            new Dictionary<string, DiscoveredNode>
            {
                ["s1a"] = new("s1a", "local", "pgw-demo-s1-s1a", 15432, 18008, 16432),
            },
            new PortAllocLock([Ep], etcd, TimeProvider.System, "inst"));

        // Act
        var outcome = await adoption.TickAsync(snap, CancellationToken.None);

        // Assert: не фейл — ждём; portalloc не записан вовсе (недобор не доведён,
        // merge-факт s1a тоже не опубликован — любая запись только под локом)
        outcome.IsSuccess.Should().BeTrue();
        outcome.Value.Should().Be(ProcessOutcome.InProgress);
        (await GetValueAsync(etcd, "/pgworker/portalloc/demo")).Should().BeNull();
    }
```

Фазу журнала можно проверить существующим хелпером `RecordingJournal` (attach на etcd): `Entries` содержит `("adopt", "waiting-portalloc-lock", "")` — по желанию, основной assert выше.

- [ ] **Step 2: Обновить NewAdoption (optional-параметр — существующие вызовы не трогаем)**

В хелпере `NewAdoption` добавить третий параметр с дефолтом (после `IReadOnlyDictionary<string, DiscoveredNode> inspect`):

```csharp
    private static async Task<(AdoptionProcess Process, Fakes.FakeSql Sql, Fakes.FakeDriver Driver)> NewAdoption(
        Fakes.FakeEtcd etcd,
        IReadOnlyDictionary<string, DiscoveredNode> inspect,
        PortAllocLock? portLock = null)
    {
        var claims = new ClaimStore([Ep], etcd, TimeProvider.System);
        await claims.TryClaimClusterAsync("demo", CancellationToken.None);
        // t90: по умолчанию — свежий свободный лок (все существующие тесты
        // исполняются в одиночном режиме, лок всегда берётся); занятый лок
        // передаёт только новый тест Tick_PortAllocLockBusy_WaitsWithoutPortallocWrite.
        portLock ??= new PortAllocLock([Ep], etcd, TimeProvider.System, claims.InstanceId);
```

и передать в конструкцию `AdoptionProcess` после `new PortAllocIndex(...)`:

```csharp
            portLock,
```

(11 существующих вызовов `NewAdoption(etcd, inspect)` в файле НЕ меняются — дефолт покрывает их; поиском `NewAdoption(` убедиться, что явный третий аргумент передаёт только новый тест.)

- [ ] **Step 3: Запустить — падает**

Run: `dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj --filter "FullyQualifiedName~AdoptionProcessTests.Tick_PortAllocLockBusy_WaitsWithoutPortallocWrite"`
Expected: ошибка компиляции (хелпер передаёт `portLock` в ctor, у которого ещё нет параметра — Step 2 сломает сборку до реализации) → после Step 4 тест зелёный.

- [ ] **Step 4: Реализовать**

4a. Ctor — после `PortAllocIndex portAlloc,` добавить `PortAllocLock portLock,`.

4b. В `TickAsync` после `var reconciled = await ReconcileAddressesAsync(snap, existing.Value, ct);` заменить:

```csharp
        if (!reconciled.IsSuccess)
            return await FailAsync(cluster, reconciled.Error!, ct);
```

на:

```csharp
        if (reconciled.Error is PortLockBusyException)
        {
            await journal.WritePhaseAsync(cluster, Op, "waiting-portalloc-lock", claims.InstanceId, null, ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.InProgress);
        }
        if (!reconciled.IsSuccess)
            return await FailAsync(cluster, reconciled.Error!, ct);
```

4c. В `ReconcileAddressesAsync` — секция реплана. Сразу после merge-блока (после закрывающей `}` цикла `foreach (var (shardName, names) in candidatesByShard) ...`) и ДО комментария «Перепланирование занятых (Д1-механика для Active, живой-Ф7)...» / `var dockerBusy = await driver.GetBusyPortsAsync(ct);` вставить:

```csharp
        // t90: пред-выход ДО глобального portalloc-клэйма (spec §3.2 п.3 «без
        // изменений (changed=false) — до лока»): merge ничего не изменил И каждый
        // ключ-кандидат "{shard}/{name}" (candidatesByShard: nodes ∪ members) имеет
        // запись, подтверждённую фактом своей ноды либо object (AllConfirmed).
        // Недобор кандидата без записи даёт false — лок обязателен; расхождение
        // записи с фактом (changed=true) — тоже. Все подтверждено → detach пуст,
        // allocate не сработает, changed останется false — секция была бы no-op,
        // лок не берём (симметрия пред-выхода P1: тики здорового Active-кластера
        // не соперничают за глобальный клэйм, arch/14 §2.4).
        var allWanted = candidatesByShard
            .SelectMany(kv => kv.Value.Select(name => $"{kv.Key}/{name}"))
            .ToList();
        if (changed || !PortPlanConvergence.AllConfirmed(merged, selfFactByNode, allWanted))
        {
            // t90: глобальный portalloc-клэйм — чтение кросс-картины занятости,
            // пере-allocate и ЗАПИСЬ portalloc взаимоисключающи между кластерами/
            // инстансами (инвариант arch/14 §2.4: порты выбираются под локом —
            // публикация до release, иначе конкурент успевает выбрать те же порты).
            // Сбой захвата — обычный фейл (бэкофф); занят — PortLockBusyException
            // → waiting-portalloc-lock, следующий тик повторяет.
            var acquired = await portLock.TryAcquireAsync(ct);
            if (!acquired.IsSuccess)
                return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(acquired.Error!);
            if (!acquired.Value)
                return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(new PortLockBusyException());
            try
            {
```

Затем ОБЕРНУТЬ в этот `try` всю секцию реплана — от `var dockerBusy = await driver.GetBusyPortsAsync(ct);` (с её комментарием «Перепланирование занятых...») до КОНЦА блока `if (changed) { ... }` включительно — содержимое не менять (только отступ +4):

```csharp
                var dockerBusy = await driver.GetBusyPortsAsync(ct);
                if (!dockerBusy.IsSuccess)
                    return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(dockerBusy.Error!);
                var foreignAlloc = await portAlloc.ReadBusyAsync(cluster, ct);
                if (!foreignAlloc.IsSuccess)
                    return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(foreignAlloc.Error!);
                var busy = new HashSet<(string, int)>(foreignAlloc.Value);
                foreach (var p in dockerBusy.Value)
                    busy.Add(p);
                if (PortPlanConvergence.DetachColliding(merged, selfFactByNode, busy))
                {
                    // Недобор адресов снятых нод: переаллокация (паттерн P1-недобора);
                    // taken = busy − факты подтверждённых записей (переиспользование валидных).
                    var hosts = await driver.GetHostsAsync(ct);
                    if (!hosts.IsSuccess)
                        return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(hosts.Error!);
                    var taken = new HashSet<(string, int)>(busy);
                    foreach (var p in PortPlanConvergence.ConfirmedFact(merged, selfFactByNode))
                        taken.Remove(p);
                    var plan = PlacementPlanner.Plan(dsnShards, hosts.Value);
                    var allocated = PortAllocator.Allocate(plan, merged, taken, placementOpts.PortFrom, placementOpts.PortTo);
                    if (!allocated.IsSuccess)
                        return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(allocated.Error!);
                    foreach (var (k, addr) in allocated.Value)
                        merged[k] = addr;
                    changed = true;
                }

                // put portalloc — СТРОГО ВНУТРИ критической секции (до release):
                // changed=true возникает и в allocate-ветке выше — публикация
                // выбранных под локом портов после release воспроизводила бы гонку
                // t90 («port is already allocated»).
                if (changed)
                {
                    var put = await PutAsync($"/pgworker/portalloc/{cluster}", Portalloc.Serialize(merged), ct);
                    if (!put.IsSuccess)
                        return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(put.Error!);
                    await journal.WritePhaseAsync(cluster, Op, "repaired-portalloc", claims.InstanceId, null, ct);
                }
```

и закрыть try/finally и ветку пред-выхода:

```csharp
            }
            finally
            {
                await portLock.ReleaseAsync();
            }
        }
```

ПОСЛЕ закрытия (вне критической секции, без изменений) остаются: dsn-инвариант (`put /clusters/.../dsn`, `repaired-dsn`) и EnsureNode-блок («Дефект-B», `recreated-node`) — они не являются записью portalloc и не читают кросс-кластерную занятость. Итоговый скелет секции:

```
merge-блок (без изменений, вне лока)
→ пред-выход / захват лока (if changed || !AllConfirmed(allWanted))
    → try: dockerBusy → ReadBusy → DetachColliding → пере-Allocate →
           if (changed) { put portalloc; journal "repaired-portalloc" }
    → finally: release
→ dsn-инвариант (без изменений)
→ EnsureNode-блок (без изменений)
```

- [ ] **Step 5: Прогнать тесты Adoption**

Run: `dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj --filter "FullyQualifiedName~AdoptionProcessTests"`
Expected: все PASS. Существующие тесты не трогают лок-конфликтов (лок в одиночном режиме всегда свободен):
- `TickAsync_FullPortalloc_NoOpButRolesEnsured` — до лока вообще не доходит: кандидатов нет (nodes ∪ members пусты — `SnapshotActive(..., [])` не сеет members) → ранний выход «кандидатов нет» в начале `ReconcileAddressesAsync`;
- `TickAsync_AddressesMatchFact_NoRepairMutations` — пред-выход `changed=false + AllConfirmed(allWanted)` без лока: put не выполняется, version portalloc не растёт (ассерт `Version == 1` зелёный);
- тесты с репарацией (`DivergedPortalloc`, `ExternalObjectShard`, `RunningNodeWithDeadContainer`, `ExternalShard`) — changed=true/AllConfirmed=false → лок берётся свободным → put под локом → прежние ассерты (`repaired-portalloc`, version, object) зелёные.

- [ ] **Step 6: Commit**

```bash
git add src/PgWorker.Provisioning/Processes/AdoptionProcess.cs src/tests/PgWorker.UnitTests/Provisioning/AdoptionProcessTests.cs
git commit -m "t90: AdoptionProcess — реплан коллизий/недобор под глобальным portalloc-клэймом (put только в критической секции)"
```

---

### Task 6: Program.cs — DI-регистрация PortAllocLock

**Files:**
- Modify: `src/PgWorker.App/Program.cs` (после регистрации `PortAllocIndex` ~строка 147; три конструкции процессов)

**Interfaces:**
- Consumes: задачи 3–5 (новые ctor-параметры).
- Produces: собранный `PgWorker.App` с локом во всех трёх процессах.

- [ ] **Step 1: Зарегистрировать PortAllocLock**

После блока регистрации `PortAllocIndex` (строки `builder.Services.AddSingleton(sp => new PortAllocIndex(...))`) добавить:

```csharp
// Глобальный portalloc-клэйм (t90, arch/14 §2.4/§3.3): взаимоисключение секции
// довыделения портов между кластерами/инстансами; instance = InstanceId ClaimStore.
builder.Services.AddSingleton(sp => new PortAllocLock(
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints.ToArray(),
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<ClaimStore>().InstanceId));
```

- [ ] **Step 2: Прокинуть в три процесса**

В конструкциях `ProvisioningProcess`, `AdoptionProcess`, `AddShardProcess` добавить после `sp.GetRequiredService<PortAllocIndex>(),`:

```csharp
        sp.GetRequiredService<PortAllocLock>(),
```

- [ ] **Step 3: Собрать всё решение**

Run: `dotnet build src/PgWorker.slnx`
Expected: Build succeeded, 0 warnings (TreatWarningsAsErrors).

- [ ] **Step 4: Commit**

```bash
git add src/PgWorker.App/Program.cs
git commit -m "t90: DI PortAllocLock — прокидывание в Provisioning/AddShard/Adoption"
```

---

### Task 7: Интеграционный тест гонки (реальный etcd)

**Files:**
- Create: `src/tests/PgWorker.IntegrationTests/Etcd/PortAllocLockRaceTests.cs`

**Interfaces:**
- Consumes: `EtcdFixture`/`EtcdCollection` (существующая коллекционная фикстура), `EtcdGateway` напрямую, `PortAllocator.Allocate`, `Portalloc.Serialize/Parse`, `PortAllocLock` (задача 1).
- Produces: критерий приёмки spec №5.

- [ ] **Step 1: Написать тест**

Создать `src/tests/PgWorker.IntegrationTests/Etcd/PortAllocLockRaceTests.cs`:

```csharp
using PgWorker.Core.Model;
using PgWorker.Core.Planning;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
using Xunit;

namespace PgWorker.IntegrationTests.Etcd;

// t90: гонка ПАРАЛЛЕЛЬНОГО provisioning на реальном etcd — две критические
// секции «ReadBusy → Allocate → put portalloc» под глобальным клэймом дают
// НЕПЕРЕСЕКАЮЩИЕСЯ порты; без клэйма обе читали бы пустой префикс (воспроизведение
// dev-стенда 2026-08-25: «port is already allocated»).
[Collection(EtcdCollection.Name)]
public class PortAllocLockRaceTests(EtcdFixture fixture)
{
    private EtcdGateway Gateway => fixture.Gateway;

    private string Endpoint => fixture.Endpoint;

    // Критическая секция довыделения (мини-P1): под PortAllocLock читает busy
    // из префикса portalloc (кроме своего кластера), аллоцирует тройку, пишет ключ.
    // Ретрай-цикл «пока не acquired» имитирует тики (~200 мс) с бюджетом 10 с.
    private async Task<Result> CriticalSectionAsync(PortAllocLock portLock, string cluster, CancellationToken ct)
    {
        var deadline = DateTimeOffset.UtcNow.AddSeconds(10);
        while (DateTimeOffset.UtcNow < deadline)
        {
            var acquired = await portLock.TryAcquireAsync(ct);
            if (!acquired.IsSuccess)
                return acquired;
            if (!acquired.Value)
            {
                await Task.Delay(200, ct); // «занят другим» — следующий тик
                continue;
            }

            try
            {
                // busy = portalloc-записи ВСЕХ чужих кластеров (как PortAllocIndex)
                var range = await Gateway.RangeAsync(Endpoint, "/pgworker/portalloc/", ct);
                if (!range.IsSuccess)
                    return range;
                var busy = new HashSet<(string, int)>();
                foreach (var kv in range.Value)
                {
                    if (kv.Key.EndsWith($"/{cluster}", StringComparison.Ordinal))
                        continue;
                    var parsed = Portalloc.Parse(kv.Key.Split('/')[^1], kv.Value);
                    if (!parsed.IsSuccess)
                        continue;
                    foreach (var addr in parsed.Value.Values)
                    {
                        busy.Add((addr.Host, addr.Ports.Pg));
                        busy.Add((addr.Host, addr.Ports.Patroni));
                        busy.Add((addr.Host, addr.Ports.Doorman));
                    }
                }

                // Аллокация одной ноды (тройка pg/patroni/doorman)
                var plan = new PlacementPlan([new NodePlacement("shard1", "n1", "h1")]);
                var allocated = PortAllocator.Allocate(
                    plan, new Dictionary<string, NodeAddress>(), busy, 15000, 15100);
                if (!allocated.IsSuccess)
                    return allocated;
                var put = await Gateway.PutAsync(
                    Endpoint, $"/pgworker/portalloc/{cluster}", Portalloc.Serialize(allocated.Value), null, ct);
                if (!put.IsSuccess)
                    return put;
                return Result.Success();
            }
            finally
            {
                await portLock.ReleaseAsync();
            }
        }

        return Result.Failed(new ApplicationException("порт-клэйм не освобождался 10 с — гонка/дедлок"));
    }

    private static HashSet<(string, int)> PortsOf(IReadOnlyDictionary<string, NodeAddress> addresses)
    {
        var ports = new HashSet<(string, int)>();
        foreach (var addr in addresses.Values)
        {
            ports.Add((addr.Host, addr.Ports.Pg));
            ports.Add((addr.Host, addr.Ports.Patroni));
            ports.Add((addr.Host, addr.Ports.Doorman));
        }
        return ports;
    }

    // AAA: две параллельные секции (барьер одновременного старта) — порты двух
    // кластеров не пересекаются; ключ клэйма исчезает после release обеих.
    [Fact]
    public async Task ParallelSections_AllocateDisjointPorts()
    {
        // Arrange — «два инстанса» с независимыми клэймами
        var ct = TestContext.Current.CancellationToken;
        var first = new PortAllocLock([Endpoint], Gateway, TimeProvider.System, "inst-1");
        var second = new PortAllocLock([Endpoint], Gateway, TimeProvider.System, "inst-2");
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var task1 = Task.Run(async () => { await start.Task; return await CriticalSectionAsync(first, "shop1", ct); });
        var task2 = Task.Run(async () => { await start.Task; return await CriticalSectionAsync(second, "shop2", ct); });

        // Act — одновременный старт
        start.SetResult();
        var results = await Task.WhenAll(task1, task2);

        // Assert: обе секции дошли до конца
        results.Should().OnlyContain(r => r.IsSuccess);

        // Порты двух кластеров НЕ пересекаются (без клэйма обе получили бы 15000)
        var firstAlloc = Portalloc.Parse("shop1",
            (await Gateway.GetAsync(Endpoint, "/pgworker/portalloc/shop1", ct)).Value!.Value);
        var secondAlloc = Portalloc.Parse("shop2",
            (await Gateway.GetAsync(Endpoint, "/pgworker/portalloc/shop2", ct)).Value!.Value);
        var intersection = PortsOf(firstAlloc.Value).Intersect(PortsOf(secondAlloc.Value)).ToList();
        intersection.Should().BeEmpty("клэйм сериализует выбор троек — повторная секция видит запись соседа");

        // Ключ клэйма исчез после release обеих секций
        var lockKey = await Gateway.GetAsync(Endpoint, PortAllocLock.Key, ct);
        lockKey.Value.Should().BeNull();
    }

    // AAA: захват/занятость/release на реальном txn-примитиве etcd.
    [Fact]
    public async Task TryAcquire_MutualExclusionAndRelease()
    {
        // Arrange
        var ct = TestContext.Current.CancellationToken;
        var first = new PortAllocLock([Endpoint], Gateway, TimeProvider.System, "inst-1");
        var second = new PortAllocLock([Endpoint], Gateway, TimeProvider.System, "inst-2");

        // Act
        var firstAcquired = await first.TryAcquireAsync(ct);
        var secondAcquired = await second.TryAcquireAsync(ct);
        await first.ReleaseAsync();
        var reclaimed = await second.TryAcquireAsync(ct);
        await second.ReleaseAsync();

        // Assert
        firstAcquired.Value.Should().BeTrue();
        secondAcquired.Value.Should().BeFalse();
        reclaimed.Value.Should().BeTrue();
        (await Gateway.GetAsync(Endpoint, PortAllocLock.Key, ct)).Value.Should().BeNull();
    }
}
```

Примечание: тесты коллекции `[Collection(EtcdCollection.Name)]` делят один etcd — ключи `portalloc/shop1|shop2` уникальны для этих тестов; существующие соседние тесты EtcdCoordinationTests свой `/pgworker/claims/...` не пересекают.

- [ ] **Step 2: Запустить инт-тест**

Run: `dotnet test src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj --filter "FullyQualifiedName~PortAllocLockRaceTests"`
Expected: 2 PASS (поднимет testcontainers-etcd; при недоступности docker — проверить `docker info`).

- [ ] **Step 3: Commit**

```bash
git add src/tests/PgWorker.IntegrationTests/Etcd/PortAllocLockRaceTests.cs
git commit -m "t90: инт-тест гонки — параллельные секции под клэймом дают непересекающиеся порты (реальный etcd)"
```

---

### Task 8: Финальный прогон и сверка критериев приёмки

**Files:**
- Без новых файлов; при необходимости — точечные фиксы.

- [ ] **Step 1: Полная сборка решения**

Run: `dotnet build src/PgWorker.slnx`
Expected: Build succeeded, 0 warnings, 0 errors.

- [ ] **Step 2: Полный прогон юнит-тестов**

Run: `dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj`
Expected: все PASS.

- [ ] **Step 3: Полный прогон инт-тестов**

Run: `dotnet test src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj`
Expected: все PASS (включая существующие контракты — формат portalloc не изменился).

- [ ] **Step 4: Сверка критериев приёмки spec (§7)**

- [ ] arch/14 §2.4 + §3.3, roadmap t91 — уже в ветке (Фаза 1).
- [ ] `PortAllocLock`: txn `version==0` + put-with-lease TTL 15 с; release del-under-`ValueEqual(instance)` + revoke (Task 1).
- [ ] Provisioning/AddShard/Adoption довыделяют только под локом; «не взял» → InProgress `waiting-portalloc-lock` без мутаций `/pgworker/portalloc/*` (Tasks 3–5).
- [ ] Инвариант spec §3.2: чтение кросс-кластерной занятости с последующей записью portalloc — только под клэймом; в AdoptionProcess put portalloc целиком внутри критической секции (Task 5, Step 4c).
- [ ] Юнит-тесты захвата/занятости/release/ошибок + инварианты процессов (Tasks 1, 3–5).
- [ ] Инт-тест: непересекающиеся порты, ключ исчезает после release (Task 7).
- [ ] build/test зелёные (настоящий шаг).
- [ ] Параллельный посев больше не даёт «port is already allocated» (Task 7, `ParallelSections_AllocateDisjointPorts`).

- [ ] **Step 5: Commit при фиксах**

```bash
git add -A
git commit -m "t90: финальный прогон — фиксы по итогам полной сборки/тестов"
```

(пропустить, если фиксов не было)

---

## Замечания для исполнителя

- Порядок задач строгий: 1–2 (компоненты) → 3–5 (процессы, каждый независимо тестируем) → 6 (DI) → 7 (инт) → 8 (прогон).
- Задачи 3–5 можно исполнять субагентами последовательно (каждая правит свой процесс); задача 6 требует завершения 3–5 (соберётся только с тремя новыми параметрами).
- Если в существующих тестах процессов риги создаются не только в перечисленных местах (поиск `new ProvisioningProcess(`/`new AddShardProcess(`/`new AdoptionProcess(` по всему тестовому проекту) — обновить ВСЕ конструкции: новый параметр обязателен (компиляция). В ProvisioningProcessTests их ДВЕ: `NewRig` (~149) и прямая в `Tick_NoRoutingKeys_WaitingKeys_NoDocker` (~287) — обе перечислены в Task 3 Step 1 (1a/1b).
- Инвариант t90 при исполнении: ЛЮБАЯ запись `/pgworker/portalloc/<C>` в секциях, читавших кросс-кластерную занятость (busy), — строго до release лока (внутри try). Вне критической секции допустимы только merge-факты БЕЗ чтения busy (AD2-путь AdoptionProcess) и записи не-portalloc (dsn, nodes-ключи, EnsureNode).
- Имена сидов/хелперов в тестах (`SeedCluster`, `SeedActiveCluster`, `SeedAddDeclaration`, `SeedDsnClusterAsync`, `Snapshot`) — сверить с фактическими файлами тестов перед вставкой; при расхождении использовать фактические имена (код тестов написан по образцу реальных хелперов, прочитанных на фазе плана).
- Спец-правка spec (Фаза 3): пред-выход уточнён условием `AllConfirmed` (записи подтверждены фактом/object) — без него пред-выход пропускал бы detach-реплан коллизий; spec.md уже обновлён, план соответствует. Для AdoptionProcess пред-выход — по полному множеству ключей-кандидатов при `changed=false` (ревью Фазы 4: проверка по `merged.Keys` пропускала недобор — запись portalloc без лока).
