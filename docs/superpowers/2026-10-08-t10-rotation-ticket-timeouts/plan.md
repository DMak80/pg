# t10-rotation-ticket-timeouts — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Цель:** возрастная самозачистка зависших не-начатых ротационных заявок kafka/valkey (пароли/CA/rebalance) самим воркером + видимость панели (stale/expired-алерты, ca_rotations kafka в снапшоте).

**Архитектура:** общий helper экспирации в `Shared.Etcd` (тройной гвард «не начато» → journal-фаза `expired` → ОДНА txn `[compare Exists][del заявку][put ticket_outcomes]`), вызываемый в waiting-точках процессов обоих воркеров ТОЛЬКО при прошедшем гварде; K kafka зеркалит valkey K0.3 — window-open-детект первым уводит в доигрывание мимо экспирационных точек; терминальная фаза `expired` закрывает фазовую серию и считает `worker_operation_total{result=expired}`; панель читает `ticket_outcomes/` + `ca_rotations/` (kafka) и алертит 8 новых kinds; дедлоки разрываются структурно (приоритет H перед K по staging-окну, сужение M0-guard'ов).

**Стек:** .NET 10, C# `LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true`; etcd HTTP JSON gateway (`IEtcdGateway`); React/Mantine SPA панели.

**Spec:** `docs/superpowers/2026-10-08-t10-rotation-ticket-timeouts/spec.md` (пороги подтверждены пользователем: `RotationTicketTimeoutSec=3600`, `RotationStaleSeconds=1800`).

## Глобальные ограничения

- Все исходники/комментарии/документация — на русском (идентификаторы/API — английские).
- `TreatWarningsAsErrors=true`: код обязан компилироваться без warnings.
- Тесты — AAA-комментарии (`// Arrange`, `// Act`, `// Assert`).
- Контракт `ticket_outcomes` (arch/15 §4, arch/20 §3): value `{"kind","outcome":"expired"|"done","reason"?,"requested_unix","requested_by","finished_unix"}`, camelCase, null-поля опускаются; kind ∈ {password-app, password-admin, ca, rebalance} (kafka) / без rebalance (valkey); пишет только воркер.
- **Журнал-фаза экспирации — всегда непрефиксованная строка `"expired"`** (без role-префиксов вида `admin:`): терминальная фаза обязана точно совпадать со словарём `FinalPhases` метрики (Задача 2). Различие ролей app/admin — только в `kind` исхода и `reason`. Исполнитель НЕ «улучшает» это префиксацией.
- **Гвард экспирации (spec §3.1) — тройной предикат «не начато», и waiting-точка сама по себе его НЕ доказывает**: (1) заявка жива; (2) окно ротации не открыто — staging `ca_next_{key,pem}` отсутствует; (3) журнал/стейт/прогресс процесса не в незавершённой мутационной фазе (kafka H — фаза роли `phase-a`/`rotated-commit`/`phase-c`, включая `admin:`-префиксы; kafka K — журнал `rotate-ca` вне `{done, waiting-*}`; valkey E — стейт `work/<C>/rotation`; rebalances — прогресс-ключ `mode=balance`). Любой предикат ложен → обычный waiting-исход БЕЗ экспирации (стагнация начатого — зона панельных stale-алертов). Экспирация — только в дооконных ветках процессов.
- Пороги: `RotationTicketTimeoutSec=3600` (воркеры, `Thresholds`-секция appsettings + env `KafkaWorker__Thresholds__RotationTicketTimeoutSec` / `ValkeyWorker__Thresholds__RotationTicketTimeoutSec`), `RotationStaleSeconds=1800` (панель, `AdminPanel:KafkaAlerts` / `AdminPanel:ValkeyAlerts`).
- Битый/отсутствующий `requested_unix` → возраст 0, снятие НЕ выполняется.
- Docker-тесты: порты динамические (`WithPortBinding(..., assignRandomHostPort: true)` / зонд свободных портов), никаких хардкодов `:16000`; `BrokerBootSec`/бюджеты фикстур ≤ 100 с; после КАЖДОЙ тестовой серии — зачистка контейнеров/сетей (`docker network prune -f` как страховочный гейт); каждый сценарий полностью чистит за собой, чистота — ассерт теста.
- arch/-контракты уже обновлены (spec-фаза) — план НЕ трогает `arch/**`, кроме roadmap-тега t10 в мерж-коммите (Задача 14).
- Все пути ниже — относительно корня worktree `/Users/demakaev/ZCodeProject/worktrees/feat-t10-rotation-ticket-timeouts`.

---

### Task 1: Shared.Etcd — TicketOutcomes + TicketExpirator (механизм экспирации §3.1 с тройным гвардом)

**Files:**
- Create: `src/Shared.Etcd/Coordination/TicketOutcomes.cs`
- Modify: `src/Shared.Etcd/Client/IEtcdGateway.cs` (фабрика `TxnCompare.Exists`)
- Modify: `src/tests/Shared.Etcd.UnitTests/Coordination/FakeCoordinationGateway.cs` (честный `TxnPredicate.Greater` по Version)
- Test: `src/tests/Shared.Etcd.UnitTests/Coordination/TicketExpiratorTests.cs`

**Interfaces (потребители — Задачи 3–8):**
```csharp
namespace Shared.Etcd.Coordination;

public sealed record TicketRequestAudit(long RequestedUnix, string? RequestedBy);

public static class TicketOutcomes
{
    public const string KindPasswordApp = "password-app";
    public const string KindPasswordAdmin = "password-admin";
    public const string KindCa = "ca";
    public const string KindRebalance = "rebalance";

    // /<workerPrefix>/ticket_outcomes/<C>
    public static string Key(string workerPrefix, string cluster);

    // (requested_unix, requested_by) из payload заявки; null — битый JSON
    // или нет поля requested_unix (возраст = 0, NOT expiry — параноидальный отказ).
    public static TicketRequestAudit? ParseAudit(string payload);

    // camelCase-JSON исхода; reason/requested_by null — опускаются.
    public static string OutcomeJson(
        string kind, string outcome, string? reason, long requestedUnix, string? requestedBy, long finishedUnix);
}

public sealed class TicketExpirator(IEtcdGateway gateway, string[] endpoints)
{
    // §3.1, тройной гвард «не начато»: предикаты 1 (заявка жива — payload не
    // null) и 2 (окно не открыто — вызывающий обязан вызывать ТОЛЬКО из
    // дооконной ветки, staging за вызывающим) + предикат 3 — параметр
    // mutationLive (незавершённая мутационная фаза процесса: журнал роли H /
    // rotate-ca K / стейт E / прогресс balance I / живое окно чужой ротации).
    // true — заявка снята (journal expired + txn del+put исход);
    // false — заявка жива/битый payload/mutationLive (вызывающий пишет waiting);
    // Failed — ошибка journal/etcd (ретрай тиком).
    // Фаза журнала — ВСЕГДА "expired" (без role-префиксов): точное совпадение
    // с FinalPhases метрики; различие ролей — в kind исхода и reason.
    public Task<Result<bool>> TryExpireAsync(
        WorkJournal journal, string cluster, string op, string instanceId,
        string outcomeKey, string kind, string ticketKey, string ticketPayload,
        string waitingReason, int timeoutSec, long nowUnix, bool mutationLive,
        CancellationToken ct);

    // Финал операции: put исхода done (идемпотентен; null-аудит — рестарт-хвост:
    // requestedUnix = nowUnix, requestedBy = null).
    public Task<Result> WriteDoneAsync(
        string outcomeKey, string kind, TicketRequestAudit? audit, long nowUnix, CancellationToken ct);
}
```
В `IEtcdGateway.cs` рядом с `TxnCompare.NotExists`:
```csharp
// Ключ существует (version > 0) — примитив условного снятия заявок (t10).
public static TxnCompare Exists(string key)
    => new(key, TxnTarget.Version, TxnPredicate.Greater, string.Empty, 0);
```

- [ ] **Step 1: Написать падающие тесты** `TicketExpiratorTests.cs` (фигура — `FakeCoordinationGateway` + `WorkJournal` из этого же проекта тестов; AAA). Сначала — фейк: `FakeCoordinationGateway.TxnAsync` сейчас игнорирует `TxnPredicate` (только Equal-семантика Version), а `TxnCompare.Exists` = `Version Greater 0` — заменить ветку `TxnTarget.Version` на:
```csharp
TxnTarget.Version => c.Pred == TxnPredicate.Greater
    ? Store.ContainsKey(c.Key) // version > Num (Num=0) ⇔ ключ существует — Exists (t10)
    : !Store.ContainsKey(c.Key) && c.Num == 0
      || (Store.ContainsKey(c.Key) && c.Num != 0),
```
Тела тестов:

```csharp
namespace Shared.Etcd.UnitTests;

// TicketExpirator (t10, arch/15 §4 / arch/20 §3): возрастная экспирация
// не-начатой заявки под тройным гвардом §3.1 — journal expired
// (journal-before-manipulations) → ОДНА txn [compare Exists][del заявку][put ticket_outcomes].
public class TicketExpiratorTests
{
    private const string Ep = "http://etcd:2379";
    private const long Now = 1_757_000_000;

    private static (TicketExpirator Expirator, FakeCoordinationGateway Gateway, WorkJournal Journal) Rig()
    {
        var gateway = new FakeCoordinationGateway();
        return (new TicketExpirator(gateway, [Ep]), gateway, new WorkJournal("/kafkaworker", gateway, [Ep]));
    }

    [Fact]
    public void ParseAudit_ValidPayload_ExtractsUnixAndBy()
    {
        // Arrange: payload заявки с аудитом.
        var payload = """{"requested_unix":1757000100,"requested_by":"admin"}""";

        // Act
        var audit = TicketOutcomes.ParseAudit(payload);

        // Assert
        audit.Should().Be(new TicketRequestAudit(1757000100, "admin"));
    }

    [Theory]
    [InlineData("""{"requested_by":"admin"}""")]        // нет requested_unix
    [InlineData("""{"requested_unix":"soon"}""")]       // не число
    [InlineData("""{oops""")]                           // битый JSON
    [InlineData("""null""")]                            // не объект
    public void ParseAudit_BrokenPayload_ReturnsNull(string payload)
    {
        // Act: любой битый payload. Assert: null — возраст 0, NOT expiry.
        TicketOutcomes.ParseAudit(payload).Should().BeNull();
    }

    [Fact]
    public async Task TryExpire_AgeAboveTimeout_JournalExpiredAndTxnDelPutOutcome()
    {
        // Arrange: заявка возрастом 100 с при пороге 60; исходов нет; гварды пройдены.
        var (expirator, gateway, journal) = Rig();
        gateway.Seed("/kafkaworker/rotations/events",
            $$"""{"requested_unix":{{Now - 100}},"requested_by":"admin"}""");

        // Act
        var result = await expirator.TryExpireAsync(
            journal, "events", "rotate", "inst1",
            TicketOutcomes.Key("/kafkaworker", "events"), TicketOutcomes.KindPasswordApp,
            "/kafkaworker/rotations/events", gateway.Store["/kafkaworker/rotations/events"],
            "waiting-cluster", 60, Now, mutationLive: false, CancellationToken.None);

        // Assert: снята; journal expired с возрастом и причиной; txn удалила
        // заявку и поставила исход (camelCase).
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().BeTrue();
        var state = await journal.ReadAsync("events", CancellationToken.None);
        state.Value!.Phase.Should().Be("expired");
        state.Value.LastError.Should().Be("ticket age=100s reason=waiting-cluster");
        gateway.Store.Should().NotContainKey("/kafkaworker/rotations/events");
        var outcome = gateway.Store[TicketOutcomes.Key("/kafkaworker", "events")];
        outcome.Should().Contain("\"kind\":\"password-app\"");
        outcome.Should().Contain("\"outcome\":\"expired\"");
        outcome.Should().Contain("\"reason\":\"waiting-cluster\"");
        outcome.Should().Contain($"\"requested_unix\":{Now - 100}");
        outcome.Should().Contain("\"requested_by\":\"admin\"");
        outcome.Should().Contain($"\"finished_unix\":{Now}");
    }

    [Fact]
    public async Task TryExpire_AgeBelowTimeout_ReturnsFalseWithoutMutations()
    {
        // Arrange: заявка возрастом 10 с при пороге 60.
        var (expirator, gateway, journal) = Rig();
        gateway.Seed("/kafkaworker/rotations/events", $$"""{"requested_unix":{{Now - 10}}}""");

        // Act
        var result = await expirator.TryExpireAsync(
            journal, "events", "rotate", "inst1",
            TicketOutcomes.Key("/kafkaworker", "events"), TicketOutcomes.KindPasswordApp,
            "/kafkaworker/rotations/events", gateway.Store["/kafkaworker/rotations/events"],
            "waiting-cluster", 60, Now, mutationLive: false, CancellationToken.None);

        // Assert: заявка жива — обычный waiting (вызывающий пишет фазу сам);
        // txn не подавалась, исхода нет.
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().BeFalse();
        gateway.Txns.Should().BeEmpty();
        gateway.Store.Should().ContainKey("/kafkaworker/rotations/events");
        gateway.Store.Should().NotContainKey(TicketOutcomes.Key("/kafkaworker", "events"));
    }

    [Fact]
    public async Task TryExpire_MutationLive_ReturnsFalseWithoutMutationsAtAnyAge()
    {
        // Arrange: заявка возрастом 5000 с (далеко за порогом), но третий
        // предикат гварда ложен — процесс в незавершённой мутационной фазе.
        var (expirator, gateway, journal) = Rig();
        gateway.Seed("/kafkaworker/rotations/events", $$"""{"requested_unix":{{Now - 5000}}}""");

        // Act
        var result = await expirator.TryExpireAsync(
            journal, "events", "rotate", "inst1",
            TicketOutcomes.Key("/kafkaworker", "events"), TicketOutcomes.KindPasswordApp,
            "/kafkaworker/rotations/events", gateway.Store["/kafkaworker/rotations/events"],
            "waiting-cluster", 60, Now, mutationLive: true, CancellationToken.None);

        // Assert: начатое не снимается никогда — заявка жива, journal чист,
        // txn не подавалась (вызывающий пишет обычный waiting).
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().BeFalse();
        (await journal.ReadAsync("events", CancellationToken.None)).Value.Should().BeNull();
        gateway.Txns.Should().BeEmpty();
        gateway.Store.Should().ContainKey("/kafkaworker/rotations/events");
    }

    [Fact]
    public async Task TryExpire_BrokenPayload_ReturnsFalseWithoutJournal()
    {
        // Arrange: payload без requested_unix.
        var (expirator, gateway, journal) = Rig();
        gateway.Seed("/kafkaworker/rotations/events", """{"requested_by":"admin"}""");

        // Act
        var result = await expirator.TryExpireAsync(
            journal, "events", "rotate", "inst1",
            TicketOutcomes.Key("/kafkaworker", "events"), TicketOutcomes.KindPasswordApp,
            "/kafkaworker/rotations/events", gateway.Store["/kafkaworker/rotations/events"],
            "waiting-cluster", 60, Now, mutationLive: false, CancellationToken.None);

        // Assert: параноидальный отказ от снятия — заявка жива, journal чист.
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().BeFalse();
        (await journal.ReadAsync("events", CancellationToken.None)).Value.Should().BeNull();
        gateway.Store.Should().ContainKey("/kafkaworker/rotations/events");
    }

    [Fact]
    public async Task WriteDone_PutsOutcomeDone()
    {
        // Arrange: экспиратор + аудит заявки (+ null-аудит рестарт-хвоста).
        var (expirator, gateway, _) = Rig();

        // Act
        var done = await expirator.WriteDoneAsync(
            TicketOutcomes.Key("/kafkaworker", "events"), TicketOutcomes.KindCa,
            new TicketRequestAudit(1757000100, "it"), Now, CancellationToken.None);
        var tail = await expirator.WriteDoneAsync(
            TicketOutcomes.Key("/kafkaworker", "events"), TicketOutcomes.KindCa,
            null, Now, CancellationToken.None);

        // Assert: outcome=done перезаписью; null-аудит — requested_unix = finishedUnix,
        // requested_by опущен.
        done.IsSuccess.Should().BeTrue();
        tail.IsSuccess.Should().BeTrue();
        gateway.Store[TicketOutcomes.Key("/kafkaworker", "events")]
            .Should().Contain("\"kind\":\"ca\"").And.Contain("\"outcome\":\"done\"")
            .And.Contain($"\"requested_unix\":{Now}").And.NotContain("\"requested_by\"");
    }
}
```

- [ ] **Step 2: Прогнать, убедиться в падении** — `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter FullyQualifiedName~TicketExpiratorTests` → FAIL (типы не существуют).

- [ ] **Step 3: Реализовать** `TicketOutcomes.cs` целиком:

```csharp
using System.Text.Json;
using System.Text.Json.Serialization;
using Shared.Core;
using Shared.Etcd.Client;

namespace Shared.Etcd.Coordination;

// Аудит заявки из payload (t10): возраст — из requested_unix (канон §9.8-протокола,
// переживает рестарты), не из mod_revision etcd.
public sealed record TicketRequestAudit(long RequestedUnix, string? RequestedBy);

// Исходы заявок воркеров (t10, arch/15 §4 / arch/20 §3): пишет только воркер,
// перезаписывается каждым новым исходом, чистится демонтажом X2.
public static class TicketOutcomes
{
    public const string KindPasswordApp = "password-app";
    public const string KindPasswordAdmin = "password-admin";
    public const string KindCa = "ca";
    public const string KindRebalance = "rebalance";

    public static string Key(string workerPrefix, string cluster)
        => $"{workerPrefix}/ticket_outcomes/{cluster}";

    // null — битый JSON или нет requested_unix: возраст считается нулём,
    // снятие НЕ выполняется (параноидальный отказ от снятия).
    public static TicketRequestAudit? ParseAudit(string payload)
    {
        try
        {
            using var doc = JsonDocument.Parse(payload);
            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object
                || !root.TryGetProperty("requested_unix", out var unix)
                || unix.ValueKind != JsonValueKind.Number
                || !unix.TryGetInt64(out var requested))
                return null;
            var by = root.TryGetProperty("requested_by", out var b)
                     && b.ValueKind == JsonValueKind.String ? b.GetString() : null;
            return new TicketRequestAudit(requested, by);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static readonly JsonSerializerOptions Json = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    public static string OutcomeJson(
        string kind, string outcome, string? reason,
        long requestedUnix, string? requestedBy, long finishedUnix)
        => JsonSerializer.Serialize(new TicketOutcomeValue(kind, outcome, reason, requestedUnix, requestedBy, finishedUnix), Json);

    private sealed record TicketOutcomeValue(
        [property: JsonPropertyName("kind")] string Kind,
        [property: JsonPropertyName("outcome")] string Outcome,
        [property: JsonPropertyName("reason")] string? Reason,
        [property: JsonPropertyName("requested_unix")] long RequestedUnix,
        [property: JsonPropertyName("requested_by")] string? RequestedBy,
        [property: JsonPropertyName("finished_unix")] long FinishedUnix);
}

// Возрастная экспирация не-начатой заявки (t10, arch/16 §5 / arch/21 §5).
// Гвард экспирации (spec §3.1) — тройной предикат «не начато»:
//  1) заявка жива (ticketPayload != null — за вызывающим);
//  2) окно ротации не открыто (staging ca_next_* отсутствует — за вызывающим:
//     вызов ТОЛЬКО из дооконной ветки процесса);
//  3) mutationLive=false — журнал/стейт/прогресс процесса не в незавершённой
//     мутационной фазе (журнал роли H / rotate-ca K / стейт E / прогресс
//     balance I; сюда же вызывающий передаёт true при живом окне чужой
//     ротации — точка принципиально не экспирационная).
// Waiting-точка процесса сама по себе «не начато» НЕ доказывает.
public sealed class TicketExpirator(IEtcdGateway gateway, string[] endpoints)
{
    public async Task<Result<bool>> TryExpireAsync(
        WorkJournal journal, string cluster, string op, string instanceId,
        string outcomeKey, string kind, string ticketKey, string ticketPayload,
        string waitingReason, int timeoutSec, long nowUnix, bool mutationLive,
        CancellationToken ct)
    {
        // Предикат 3 гварда: незавершённая мутация жива — снятие запрещено
        // при любом возрасте (spec §3.1/§5).
        if (mutationLive)
            return Result<bool>.Success(false);
        var audit = TicketOutcomes.ParseAudit(ticketPayload);
        if (audit is null)
            return Result<bool>.Success(false); // битый payload — NOT expiry
        var age = nowUnix - audit.RequestedUnix;
        if (age <= timeoutSec)
            return Result<bool>.Success(false); // обычный waiting

        // journal-before-manipulations: терминальная фаза expired закрывает
        // фазовую серию и считает worker_operation_total{result=expired}
        // (подписка WorkJournal.PhaseWritten → WorkerMetricsInstrumentation).
        // Фаза — ВСЕГДА непрефиксованная "expired" (role-префиксы вида admin:
        // сломали бы совпадение с FinalPhases метрики); различие ролей —
        // в kind исхода и reason.
        var expired = await journal.WritePhaseAsync(
            cluster, op, "expired", instanceId,
            $"ticket age={age}s reason={waitingReason}", ct);
        if (!expired.IsSuccess)
            return Result<bool>.Failed(expired.Error!);

        // ОДНА txn [compare Exists][del заявку][put исход]: атомарна; проигрыш
        // compare (заявки уже нет) — no-op-успех; краш до txn — повтор тиком.
        var txn = await TxnAsync(TxnRequest.Of(
            [TxnCompare.Exists(ticketKey)],
            [
                new TxnOp.Delete(ticketKey, Prefix: false),
                new TxnOp.Put(outcomeKey, TicketOutcomes.OutcomeJson(
                    kind, "expired", waitingReason, audit.RequestedUnix, audit.RequestedBy, nowUnix), null),
            ]), ct);
        if (!txn.IsSuccess)
            return Result<bool>.Failed(txn.Error!);
        return Result<bool>.Success(true);
    }

    // Финал операции (H/K/E/I): успешная заявка гасит expired-алерт панели
    // перезаписью исхода. Идемпотентный put — безопасен ДО снятия заявки/журнала
    // (порядок вызова — за вызывающим, см. задачи-потребители).
    public async Task<Result> WriteDoneAsync(
        string outcomeKey, string kind, TicketRequestAudit? audit, long nowUnix, CancellationToken ct)
    {
        var value = TicketOutcomes.OutcomeJson(
            kind, "done", null, audit?.RequestedUnix ?? nowUnix, audit?.RequestedBy, nowUnix);
        var put = await PutAsync(outcomeKey, value, ct);
        return put;
    }

    private async Task<Result<TxnResult>> TxnAsync(TxnRequest req, CancellationToken ct)
    {
        Result<TxnResult>? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await gateway.TxnAsync(endpoint, req, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }
        return last!;
    }

    private async Task<Result> PutAsync(string key, string value, CancellationToken ct)
    {
        Result? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await gateway.PutAsync(endpoint, key, value, null, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }
        return last!;
    }
}
```
В `IEtcdGateway.cs` добавить фабрику `TxnCompare.Exists` (код выше).

- [ ] **Step 4: Прогнать тесты — PASS**: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter FullyQualifiedName~TicketExpiratorTests`.

- [ ] **Step 5: Commit**
```bash
git add src/Shared.Etcd/Coordination/TicketOutcomes.cs src/Shared.Etcd/Client/IEtcdGateway.cs src/tests/Shared.Etcd.UnitTests/Coordination/FakeCoordinationGateway.cs src/tests/Shared.Etcd.UnitTests/Coordination/TicketExpiratorTests.cs
git commit -m "feat(t10): общий механизм экспирации заявок TicketExpirator (тройной гвард) в Shared.Etcd"
```

**Вход:** чистая ветка задачи. **Выход:** переиспользуемый механизм §3.1. **Проверка:** зелёные `TicketExpiratorTests` (вкл. mutationLive-кейс). **Spec:** §2.2, §3.1 (гвард + механизм), §3.4 (формат ключей), §2.7, §5 (битый payload).

---

### Task 2: Shared.Metrics — терминальная фаза `expired` (метрика §3.6)

**Files:**
- Modify: `src/Shared.Metrics/Worker/WorkerMetricsInstrumentation.cs`
- Test: `src/tests/Shared.Metrics.UnitTests/WorkerMetricsInstrumentationTests.cs`

**Interfaces:** поведение `OnJournalPhase(cluster, process, "expired")` → `ProcessFinished` + счёт `worker_operation_total{operation, result="expired"}`; `FinalPhases` содержит `"expired"` (непрефиксованная строка — см. глобальные ограничения). Потребитель — неявный (подписка `WorkJournal.PhaseWritten` из Program.cs воркеров уже существует).

- [ ] **Step 1: Тест (AAA)** в `WorkerMetricsInstrumentationTests.cs` рядом с `OnJournalPhase_FinalPhases_FinishAndCountOperation`:

```csharp
[Theory]
[InlineData("rotate")]
[InlineData("reassign")]
public void OnJournalPhase_Expired_ClosesSeriesAndCountsExpiredResult(string op)
{
    // Arrange: фазовая серия открыта (phase-a жива).
    // Act: OnJournalPhase("demo", op, "expired").
    // Assert: DebugSnapshot.Operations содержит ((op, "expired"), 1);
    // серия фаз (demo, op) закрыта (Phases не содержит).
}
```

- [ ] **Step 2: Прогон — FAIL** (`--filter FullyQualifiedName~WorkerMetricsInstrumentationTests`): expired сейчас НЕ в `FinalPhases` → серия не закрыта, счёта `"expired"` нет.

- [ ] **Step 3: Реализация**:
1. `FinalPhases`: добавить `"expired"` в HashSet (комментарий — «возрастное снятие не-начатой заявки, t10»).
2. Перегрузка счёта:
```csharp
// Завершённая операция: counter worker_operation_total{operation, result};
// result ∈ {"ok","error","expired"} (expired — снята возрастным таймаутом, t10).
public void Operation(string operation, string result)
{
    try
    {
        OperationMark(operation, result);
        lock (_lock)
        {
            var key = (operation, result);
            _operations[key] = _operations.TryGetValue(key, out var n) ? n + 1 : 1;
        }
    }
    catch { /* Пассивный наблюдатель. */ }
}

public void Operation(string operation, bool ok) => Operation(operation, ok ? "ok" : "error");
```
3. В `OnJournalPhase` терминальной ветке заменить `Operation(process, ok: phase == "done")` на:
```csharp
Operation(process, phase == "done" ? "ok" : phase == "expired" ? "expired" : "error");
```
4. Обновить XML-комментарии метода `OnJournalPhase` (терминальные фазы + expired) и счётчика (result-множество).

- [ ] **Step 4: Прогон — PASS** (`--filter FullyQualifiedName~WorkerMetricsInstrumentationTests`), затем все метрики: `--filter FullyQualifiedName~Shared.Metrics`.

- [ ] **Step 5: Commit**
```bash
git add src/Shared.Metrics/Worker/WorkerMetricsInstrumentation.cs src/tests/Shared.Metrics.UnitTests/WorkerMetricsInstrumentationTests.cs
git commit -m "feat(t10): терминальная фаза expired и result=expired в worker_operation_total"
```

**Вход:** Task 1 слит. **Выход:** метрика §3.6 (arch/18 §2.2). **Проверка:** тесты метрик зелёные. **Spec:** §3.6, AC1 (метрика result=expired).

---

### Task 3: KafkaWorker — опция порога + PasswordRotator H (waiting-ca-window, приоритет H, гвард журнала роли, экспирация, done-исход)

**Files:**
- Modify: `src/KafkaWorker.App/Options.cs` (`ThresholdsOptions` + `RotationTicketTimeoutSec`)
- Modify: `src/KafkaWorker.App/appsettings.json` (`Thresholds` + `"RotationTicketTimeoutSec": 3600`)
- Modify: `src/KafkaWorker.Provisioning/Processes/ProcessCommon.cs` (`ProvisioningOptions` + поле)
- Modify: `src/KafkaWorker.App/Program.cs` (`ToProvisioningOptions`)
- Modify: `src/KafkaWorker.Provisioning/Processes/PasswordRotator.cs`
- Test: `src/tests/KafkaWorker.UnitTests/Provisioning/PasswordRotatorTests.cs`

**Interfaces:**
- `ProvisioningOptions(..., int RotationTicketTimeoutSec = 3600)` (optional — существующие вызовы/фикстуры не ломаются); `Default` обновить явным `3600`.
- `ThresholdsOptions.RotationTicketTimeoutSec { get; set; } = 3600`.
- Поведение H: guard «живая ca-заявка» заменён на «живой staging `ca_next_{key,pem}`» → journal `waiting-ca-window`; экспирация ТОЛЬКО в waiting-точках `waiting-cluster` (обе: нет endpoints/кредов и слепой преф-чек) под гвардом журнала роли (третий предикат §3.1); точка `waiting-ca-window` принципиально НЕ экспирационная (staging жив по построению точки — предикат 2 гварда ложен: K доиграет окно, H продолжит; вечное окно — зона `kafka-ca-rotation-stale`); финал — put `ticket_outcomes/<C>` outcome=done.
- `WaitOrExpireAsync(KafkaClusterSnapshot snap, RotationRole role, Kv? ticket, string phase, bool mutationLive, CancellationToken ct)` — **`Kv?`**: null-заявка (afterCommit-хвост: заявка удалена фазой B, C не доиграна) → ТОЛЬКО journal-waiting без экспирации (экспирировать нечего); `mutationLive` — третий предикат гварда.

- [ ] **Step 1: Опции и тестовый фейк** (без тест-цикла — покрываются тестами шагов ниже):
1. `Options.cs` → `ThresholdsOptions`:
```csharp
/// <summary>Возраст не-начатой заявки (ротации app/admin/CA, ребалансировка),
/// после которого воркер снимает её в waiting-точке (t10, arch/16 §5).</summary>
public int RotationTicketTimeoutSec { get; set; } = 3600;
```
2. `appsettings.json` → `"Thresholds": { ..., "RotationTicketTimeoutSec": 3600 }`.
3. `ProcessCommon.cs` → `ProvisioningOptions` последний параметр `int RotationTicketTimeoutSec = 3600`; `Default` → `new(16000, 16999, 600, 90, null, "apache/kafka:4.0.0", 3600)`.
4. `Program.cs` `ToProvisioningOptions` → последний аргумент `opts.Thresholds.RotationTicketTimeoutSec`.
5. `src/tests/KafkaWorker.UnitTests/Provisioning/Fakes.cs` (`FakeEtcd.TxnAsync`, ветка `TxnTarget.Version`) — поддержать `TxnPredicate.Greater` (иначе новый `TxnCompare.Exists` проигрывает compare на фейке):
```csharp
TxnTarget.Version => c.Pred == TxnPredicate.Greater
    ? Store.TryGetValue(c.Key, out var e) && e.Version > c.Num // Exists (t10)
    : Store.TryGetValue(c.Key, out var e) ? e.Version == c.Num : c.Num == 0,
```

- [ ] **Step 2: Тесты (AAA)** — дополнить `PasswordRotatorTests.cs` (риг `NewRig` — как в существующих; для малого порога строить `ProvisioningOptions` вручную или `ProvisioningOptions.Default with { RotationTicketTimeoutSec = 60 }`):

```csharp
// ===== t10: приоритет H перед K + экспирация под гвардом =====

[Fact]
public async Task Run_CaTicketWithoutStaging_HExecutesOwnTicket()
{
    // Arrange: живая ca-заявка (ca_rotations/events), staging ОТСУТСТВУЕТ,
    // живая заявка пароль-ротации; кластер готов.
    // Act: тик PasswordRotator.
    // Assert: H НЕ уходит в waiting — ротация доиграна: пароль NEW, заявка
    // rotations удалена, journal done; ca-заявка НЕ тронута (её снимет K).
}

[Fact]
public async Task Run_OpenCaWindow_WaitsCaWindow()
{
    // Arrange: живые ca_next_key+ca_next_pem (окно открыто) + заявка ротации.
    // Act + Assert: journal-фаза waiting-ca-window; пароль OLD; брокеры не тронуты.
}

[Fact]
public async Task Run_OpenCaWindow_OldTicket_WaitsWithoutExpiry()
{
    // Arrange: staging жив (окно K открыто) + заявка requested_unix = UtcNow-3700.
    // Act + Assert: точка waiting-ca-window НЕ экспирационная (тройной гвард §3.1:
    // предикат 2 ложен — staging жив) — journal waiting-ca-window, заявка ЖИВА,
    // ticket_outcomes не создаётся. Окно K доиграется, H продолжит; вечное
    // окно — зона kafka-ca-rotation-stale панели.
}

[Fact]
public async Task Run_ClusterDown_TicketOlderThanTimeout_Expired()
{
    // Arrange: staging отсутствует (дооконная ветка); endpoints удалены из etcd
    // (кластер «не поднят»); журнал роли не в мутационной фазе; заявка старая.
    // Act + Assert: Success; заявка снята; journal-фаза expired; исход
    // {"kind":"password-app","outcome":"expired","reason":"waiting-cluster",...}.
}

[Fact]
public async Task Run_PhaseAInProgress_BlindCluster_OldTicket_WaitsWithoutExpiry()
{
    // Arrange (AC3c): журнал роли phase-a (WritePhaseAsync руками), endpoints жив,
    // DescribeCluster слепой (ClusterView не задан/пуст — преф-чек не отвечает),
    // заявка старая (requested_unix = UtcNow-3700).
    // Act + Assert: третий предикат гварда ложен (мутационная фаза роли жива —
    // брокеры несут JAAS [OLD, NEW]) — заявка ЖИВА, исход тика waiting-cluster
    // БЕЗ экспирации (слепота в середине A — передержка до сходимости).
}

[Fact]
public async Task Run_AfterCommitTailWithoutEndpoints_NoOpWithoutExpiry()
{
    // Arrange: afterCommit-хвост — заявки НЕТ (del фазой B), journal
    // rotate/phase-c; endpoints удалены (кластер «не поднят»).
    // Act + Assert: Success, роль НЕ обрабатывалась (исход функции как раньше —
    // передержка хвоста); НИ journal-записи, НИ экспирации, НИ NRE;
    // исхода ticket_outcomes нет.
}

[Fact]
public async Task Run_AfterCommitTailAndOpenCaWindow_WaitingWithoutExpiry()
{
    // Arrange: afterCommit-хвост (заявки нет) + живой staging ca_next_*.
    // Act + Assert: journal-фаза waiting-ca-window (Kv?-ветка — экспирации нет);
    // ticket_outcomes не создаётся.
}

[Fact]
public async Task Run_ClusterDownFreshTicket_WaitingCluster()
{
    // Arrange: staging нет; endpoints нет; заявка свежая (age < порога).
    // Act + Assert: journal waiting-cluster; заявка жива; исхода нет.
}

[Fact]
public async Task Run_ClusterDownBrokenPayload_NoExpiry()
{
    // Arrange: staging нет; endpoints нет; payload заявки {"requested_by":"x"}
    // (без requested_unix) — возраст «неизвестен».
    // Act + Assert: waiting-cluster; заявка жива (параноидальный отказ).
}

[Fact]
public async Task Run_FullRotation_WritesDoneOutcome()
{
    // Arrange: живой заявкой (существующий кейс FullRotation) + чтение исхода.
    // Act + Assert: после done в /kafkaworker/ticket_outcomes/events исход
    // {"kind":"password-app","outcome":"done","requested_unix":1750000200,...}.
}
```

- [ ] **Step 3: Прогон — FAIL** (`--filter FullyQualifiedName~PasswordRotatorTests`): новые кейсы падают (guard ещё по ca-заявке, экспирации нет).

- [ ] **Step 4: Реализация `PasswordRotator.cs`:**
1. Поле helper + аудит (primary-ctor зависимости доступны):
```csharp
// t10: экспирация не-начатых заявок + исходы финалов.
private readonly TicketExpirator _tickets = new(etcd, endpoints);

// Аудит последней живой заявки роли (для исхода done на финале после del
// заявки фазой B; рестарт в окне B→done теряет аудит — исход пишется с
// фактическим временем финала).
private readonly ConcurrentDictionary<(string Cluster, string Role), TicketRequestAudit?> _ticketAudit = new();
```
2. Заменить guard (строки чтения `ca_rotations` + `waiting-ca-rotation`):
```csharp
// Guard: открытое окно CA-ротации K (staging жив) — rolling-ы не смешиваются.
// Живая, но НЕ начатая ca-заявка H НЕ гейтит (приоритет H перед K, t10):
// при обеих живых заявках H доигрывает свою (del в фазе B), затем K — свою.
var nextKey = await GetAsync($"/kafka/clusters/{cluster}/ca_next_key", ct);
if (!nextKey.IsSuccess)
    return Result<bool>.Failed(nextKey.Error!);
var nextPem = await GetAsync($"/kafka/clusters/{cluster}/ca_next_pem", ct);
if (!nextPem.IsSuccess)
    return Result<bool>.Failed(nextPem.Error!);
if (nextKey.Value is not null || nextPem.Value is not null)
    return await WaitOrExpireAsync(
        snap, role, ticket.Value, "waiting-ca-window", mutationLive: true, ct);
```
(в ca-window точке `mutationLive: true` — окно K живо по построению точки: точка принципиально не экспирационная.)
3. Третий предикат гварда — детектор мутационной фазы роли:
```csharp
// Третий предикат гварда экспирации (§3.1): незавершённая мутационная фаза
// роли (phase-a/rotated-commit/phase-c, включая admin:-префиксы через
// role.Phase) — брокеры уже несут JAAS [OLD, NEW] либо окно C не закрыто:
// снятие заявки запрещено при любом возрасте.
private static bool RoleMutationLive(WorkState? journalState, RotationRole role)
    => journalState is { Op: Op } j
       && (j.Phase == role.Phase("phase-a")
           || j.Phase == role.Phase(PhaseCommitted)
           || j.Phase == role.Phase("phase-c"));
```
4. Новый приватный метод — nullable-заявка + mutationLive (после `ticket.Value is null && !afterCommit → return` в `RunRoleAsync` заявка может быть null при живом afterCommit-хвосте: фаза C доигрывается, экспирировать нечего):
```csharp
// Waiting-исход роли с экспирацией под гвардом (t10): живая заявка старее
// порога И гвард пройден (staging отсутствует — по построению ветки, т.к.
// ca-window guard стоит РАНЬШЕ обеих waiting-cluster точек; mutationLive —
// третий предикат) → снятие (journal expired + txn [del][put исход]),
// иначе обычный journal-waiting. ticket = null (afterCommit-хвост C без
// заявки) — ТОЛЬКО waiting, без экспирации. Фаза expired — БЕЗ role-префикса
// (терминальная, совпадение с FinalPhases метрики); префикс admin: — только
// у обычных waiting-фаз ниже.
private async Task<Result<bool>> WaitOrExpireAsync(
    KafkaClusterSnapshot snap, RotationRole role, Kv? ticket, string phase, bool mutationLive, CancellationToken ct)
{
    var cluster = snap.Cluster;
    if (ticket is not null)
    {
        var kind = role == RotationRole.Admin
            ? TicketOutcomes.KindPasswordAdmin : TicketOutcomes.KindPasswordApp;
        var expired = await _tickets.TryExpireAsync(
            journal, cluster, Op, claims.InstanceId,
            TicketOutcomes.Key("/kafkaworker", cluster), kind, ticket.Key, ticket.Value,
            phase, options.RotationTicketTimeoutSec,
            DateTimeOffset.UtcNow.ToUnixTimeSeconds(), mutationLive, ct);
        if (!expired.IsSuccess)
            return Result<bool>.Failed(expired.Error!);
        if (expired.Value)
            return Result<bool>.Success(true); // заявка снята штатно — тик не ошибка
    }

    var waiting = await journal.WritePhaseAsync(cluster, Op, role.Phase(phase), claims.InstanceId, null, ct);
    return waiting.IsSuccess ? Result<bool>.Success(true) : Result<bool>.Failed(waiting.Error!);
}
```
5. Точки `waiting-cluster` — с сохранением прежней null-семантики:
   - Точка «нет endpoints/кредов» (сегодня строки 122–130) — null-заявка остаётся no-op, живая заявка идёт в гвард+экспирацию/waiting (mutationLive — журнал роли):
```csharp
if (snap.Endpoints is null || snap.AppPassword is null || snap.AdminPassword is null)
{
    // Кластер не поднят. AfterCommit-хвост без заявки — no-op (экспирировать
    // нечего); живая заявка — waiting/экспирация под гвардом журнала роли (t10).
    if (ticket.Value is null)
        return Result<bool>.Success(false);
    return await WaitOrExpireAsync(snap, role, ticket.Value, "waiting-cluster",
        RoleMutationLive(journalState.Value, role), ct);
}
```
   - Точка слепого преф-чека `WaitForBrokersAsync` (сегодня строки 140–146; сюда доходит и afterCommit-хвост фазы C, и середина фазы A — преф-чек проходится каждым тиком):
```csharp
if (!alive.Value)
    return await WaitOrExpireAsync(snap, role, ticket.Value, "waiting-cluster",
        RoleMutationLive(journalState.Value, role), ct);
```
6. Аудит: в `RunRoleAsync` после успешного чтения заявки, если `ticket.Value is not null`:
```csharp
_ticketAudit[(cluster, role.Name)] = TicketOutcomes.ParseAudit(ticket.Value.Value);
```
7. Финал (перед `journal.WritePhaseAsync(..., role.Phase(PhaseDone), ...)`) — исход done ДО записи done (провал put → тик Failed → финал повторится по afterCommit-хвосту; исход не теряется):
```csharp
var nowUnix = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
var kind = role == RotationRole.Admin ? TicketOutcomes.KindPasswordAdmin : TicketOutcomes.KindPasswordApp;
var outcomeDone = await _tickets.WriteDoneAsync(
    TicketOutcomes.Key("/kafkaworker", cluster), kind, _ticketAudit.GetValueOrDefault((cluster, role.Name)), nowUnix, ct);
if (!outcomeDone.IsSuccess)
    return Result<bool>.Failed(outcomeDone.Error!);
_ticketAudit.TryRemove((cluster, role.Name), out _);
```
8. XML-доккласса (guard'ы: окно вместо заявки; приоритет H; тройной гвард экспирации; afterCommit-хвост без экспирации).

- [ ] **Step 5: Прогон — PASS**: `--filter FullyQualifiedName~PasswordRotatorTests` (новые + все существующие кейсы H).

- [ ] **Step 6: Commit**
```bash
git add src/KafkaWorker.App/Options.cs src/KafkaWorker.App/appsettings.json src/KafkaWorker.Provisioning/Processes/ProcessCommon.cs src/KafkaWorker.App/Program.cs src/KafkaWorker.Provisioning/Processes/PasswordRotator.cs src/tests/KafkaWorker.UnitTests/Provisioning/PasswordRotatorTests.cs src/tests/KafkaWorker.UnitTests/Provisioning/Fakes.cs
git commit -m "feat(t10): PasswordRotator H — waiting-ca-window, приоритет H, экспирация под гвардом журнала роли"
```

**Вход:** Tasks 1–2. **Выход:** §3.2 H полностью. **Проверка:** юниты H зелёные (вкл. AC3c и afterCommit-хвост без NRE). **Spec:** §2.4, §3.1 (гвард), §3.2 H, AC1/2/3c/4/5/8.

---

### Task 4: KafkaWorker — CaRotator K: window-open-детект первым, экспирация дооконных waiting-точек, done-исход

**Files:**
- Modify: `src/KafkaWorker.Provisioning/Processes/CaRotator.cs`
- Test: `src/tests/KafkaWorker.UnitTests/Provisioning/CaRotatorTests.cs`

**Interfaces:** структура `RunAsync` зеркалит valkey K0.2→K0.5 (arch/21 §5 K):
```
K0.2 хвост committed (заявка снята, done не записан) → FinishAsync            [как было]
K0.3 window-open: staging ca_next_* жив ИЛИ журнал rotate-ca вне {done, waiting-*}
     → аномалия-защита (CaPem/CaKey null → Failed) → PhasesAsync (P→D→R→C→финал)
       БЕЗ ждущих guard'ов и БЕЗ экспирации                                    [новое]
K0.4 заявки нет → no-op                                                        [как было]
K0.5 дооконные ждущие guard'ы (экспирационные под тройным гвардом §3.1 —
     staging-предикат истинен по построению ветки, журнал — явной проверкой):
     waiting-cluster (нет полей ИЛИ слепой преф-чек) / waiting-password-rotation /
     waiting-reassignment (живые reassignments ИЛИ regens) → PhasesAsync
```
`WaitAsync(cluster, phase, ticketPayload, mutationLive, ct)`; финал K4 — done-исход. Существующий хвост фаз (EnsureStaging → D → R → C → финал) выделяется в приватный `PhasesAsync(snap, ct)` без изменения логики фаз; `WaitForBrokersAsync` становится толерантным к отсутствию endpoints (передержка окна: `snap.Endpoints is null → Success(false)` — не создаёт admin-клиент).

- [ ] **Step 1: Тесты (AAA)** в `CaRotatorTests.cs`:
```csharp
[Fact]
public async Task Run_ClusterDown_OldCaTicket_Expired()           // дооконная ветка: endpoints нет; заявка старая → del + исход kind=ca reason=waiting-cluster; Success
[Fact]
public async Task Run_PasswordRotationAlive_OldCaTicket_Expired() // дооконная ветка: живой /kafkaworker/rotations/<C> → expired reason=waiting-password-rotation
[Fact]
public async Task Run_ReassignmentAlive_OldCaTicket_Expired()     // дооконная ветка: живой reassignments-прогресс → expired reason=waiting-reassignment
[Fact]
public async Task Run_BlindPrecheck_OldCaTicket_Expired()         // дооконная ветка: поля живы, DescribeCluster слепой → expired reason=waiting-cluster
[Fact]
public async Task Run_StagingAliveNoEndpoints_OldCaTicket_NotExpired()  // AC3a: staging жив ∧ endpoints НЕТ ∧ заявка старая → window-open ветка: заявка НЕ снята (доигрывание/передержка, мимо экспирационных точек), ticket_outcomes нет, journal ≠ expired
[Fact]
public async Task Run_StagingAlivePasswordTicket_OldCaTicket_NotExpired() // AC3b: staging жив ∧ живая пароль-заявка ∧ ca-заявка старая → window-open ветка: ca-заявка НЕ снята, ticket_outcomes нет
[Fact]
public async Task Run_FreshCaTicket_WaitingCluster()              // дооконная ветка: возраст < порога → journal waiting-cluster, заявка жива
[Fact]
public async Task Run_FullRotation_WritesDoneOutcome()            // полная ротация → ticket_outcomes outcome=done kind=ca
```
Существующие кейсы K, где ждущие guard'ы достигались при живом staging (если есть) — пересмотреть: окно уводит в доигрывание, guard'ы дооконные.

- [ ] **Step 2: Прогон — FAIL** (`--filter FullyQualifiedName~CaRotatorTests`).

- [ ] **Step 3: Реализация `CaRotator.cs`:**
1. Поля:
```csharp
private readonly TicketExpirator _tickets = new(etcd, endpoints);
private readonly ConcurrentDictionary<string, TicketRequestAudit?> _ticketAudit = new();
```
2. Третий предикат гварда + детектор окна (одна функция для K0.3 и K0.5):
```csharp
// Третий предикат гварда экспирации (§3.1) и он же — детектор открытого
// окна (K0.3): журнал rotate-ca в мутационной фазе — вне {done, waiting-*}
// (staging ставится в P и живёт до C; waiting-фазы — дооконные передержки).
private static bool CaMutationLive(WorkState? journalState)
    => journalState is { Op: Op } j
       && j.Phase != PhaseDone
       && !j.Phase.StartsWith("waiting-", StringComparison.Ordinal);
```
3. `RunAsync` — перестройка порядка (после существующего клэйм-гварда и чтения заявки/журнала):
   - K0.2 хвост `afterCommit && ticket.Value is null` → `FinishAsync` — остаётся первым (как сегодня).
   - K0.3 (новое, сразу после K0.2):
```csharp
// K0.3 (t10, зеркалит valkey K0.3): окно открыто — staging жив ИЛИ журнал
// rotate-ca в мутационной фазе (вне {done, waiting-*}) — доигрывание
// P→D→R→C БЕЗ ждущих guard'ов и БЕЗ экспирации: слепой кластер посреди R —
// передержка (rolling стоит на ожидании сходимости); потеря endpoints или
// появление пароль-заявки при живом staging в waiting-точки НЕ уводят —
// окно не сиротеет, заявка жива.
var stagingKey = await GetAsync(NextKeyKey(cluster), ct);
if (!stagingKey.IsSuccess)
    return Result.Failed(stagingKey.Error!);
var stagingPem = await GetAsync(NextPemKey(cluster), ct);
if (!stagingPem.IsSuccess)
    return Result.Failed(stagingPem.Error!);
var stagingLive = stagingKey.Value is not null || stagingPem.Value is not null;
if (stagingLive || CaMutationLive(journalState.Value))
{
    if (snap.CaPem is null || snap.CaKey is null)
        return Result.Failed(new ApplicationException(
            $"rotate-ca {cluster}: окно открыто, но ca_pem/ca_key отсутствуют — внешняя порча, ретрай тиком"));
    return await PhasesAsync(snap, ct);
}
```
   - K0.4: `if (ticket.Value is null) return Result.Success();` (как сегодня).
   - K0.5 — дооконные ждущие guard'ы (экспирационные; mutationLive — явная проверка журнала, по построению ветки всегда false):
     - нет полей: `return await WaitAsync(cluster, "waiting-cluster", ticket.Value.Value, CaMutationLive(journalState.Value), ct);`
     - живая пароль-ротация (заявка `rotations/<C>` ИЛИ журнал `rotate` не done — как в текущем коде): `WaitAsync(cluster, "waiting-password-rotation", ...)`; добавить проверку `admin_rotations/<C>` (заявка админ-ротации — та же семья H: rolling-ы не смешиваются) — читается тем же паттерном `GetAsync`;
     - живые `reassignments/<C>` ИЛИ `regens/<C>` (по spec §3.2 K regen тоже гейтит — rolling не смешивается; фаза едина `waiting-reassignment`, прецедент — SecurityMigrator M0): `WaitAsync(cluster, "waiting-reassignment", ...)`;
     - слепой преф-чек `WaitForBrokersAsync(snap, 1, ct)` (переносится сюда, в дооконную ветку): `if (!alive.Value) → WaitAsync(cluster, "waiting-cluster", ...)`.
   - Хвост: `return await PhasesAsync(snap, ct);`
4. Выделить `PhasesAsync(KafkaClusterSnapshot snap, CancellationToken ct)` из текущего кода после guard'ов (EnsureStaging → D → R → C → `FinishAsync`) — логика фаз без изменений, кроме п.5.
5. `WaitForBrokersAsync` — толерантность к отсутствию endpoints (window-open ветка: потеря endpoints посреди R — передержка, не NRE):
```csharp
// Передержка окна (t10): DescribeCluster невозможен без endpoints —
// считаем «не сошлось» (rolling стоит, следующий тик повторит).
if (snap.Endpoints is null)
    return Result<bool>.Success(false);
```
6. `WaitAsync` — новый контракт:
```csharp
// Ждущий исход с экспирацией под гвардом (t10): ca-заявка старее порога и
// гвард пройден (дооконная ветка: staging отсутствует по построению K0.3;
// mutationLive — явная проверка журнала) → снятие (kind=ca), иначе
// journal-waiting. Фаза expired — непрефиксованная (терминальная, FinalPhases).
private async Task<Result> WaitAsync(
    string cluster, string phase, string ticketPayload, bool mutationLive, CancellationToken ct)
{
    var expired = await _tickets.TryExpireAsync(
        journal, cluster, Op, claims.InstanceId,
        TicketOutcomes.Key("/kafkaworker", cluster), TicketOutcomes.KindCa,
        TicketKey(cluster), ticketPayload, phase,
        options.RotationTicketTimeoutSec, DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
        mutationLive, ct);
    if (!expired.IsSuccess)
        return Result.Failed(expired.Error!);
    if (expired.Value)
        return Result.Success(); // заявка снята штатно

    var waiting = await journal.WritePhaseAsync(cluster, Op, phase, claims.InstanceId, null, ct);
    return waiting.IsSuccess ? Result.Success() : Result.Failed(waiting.Error!);
}
```
7. Аудит в `RunAsync` после чтения заявки: `if (ticket.Value is not null) _ticketAudit[cluster] = TicketOutcomes.ParseAudit(ticket.Value.Value);`
8. `FinishAsync` — до journal done (хвост afterCommit доиграет финал повторно — put идемпотентен):
```csharp
var outcomeDone = await _tickets.WriteDoneAsync(
    TicketOutcomes.Key("/kafkaworker", cluster), TicketOutcomes.KindCa,
    _ticketAudit.GetValueOrDefault(cluster), DateTimeOffset.UtcNow.ToUnixTimeSeconds(), ct);
if (!outcomeDone.IsSuccess)
    return Result.Failed(outcomeDone.Error!);
_ticketAudit.TryRemove(cluster, out _);
```
9. XML-доккласса: структура K0.2→K0.5, экспирация дооконных точек, доигрывание окна без экспирации (t10).

- [ ] **Step 4: Прогон — PASS** (новые + существующие кейсы K).

- [ ] **Step 5: Commit**
```bash
git add src/KafkaWorker.Provisioning/Processes/CaRotator.cs src/tests/KafkaWorker.UnitTests/Provisioning/CaRotatorTests.cs
git commit -m "feat(t10): CaRotator K — window-open-детект первым, экспирация дооконных waiting-точек"
```

**Вход:** Tasks 1–3. **Выход:** §3.2 K (обновлённая структура). **Проверка:** юниты K зелёные (вкл. AC3a/3b). **Spec:** §2.2/§3.1 (гвард), §3.2 K, §5 («by construction»), AC1/3a/3b/8.

---

### Task 5: KafkaWorker — PartitionReassigner I (экспирация rebalances + done-исход)

**Files:**
- Modify: `src/KafkaWorker.Provisioning/Processes/ProcessCommon.cs` (`ReassignOptions` + `RotationTicketTimeoutSec`)
- Modify: `src/KafkaWorker.App/Program.cs` (wiring `ReassignOptions`)
- Modify: `src/KafkaWorker.Provisioning/Processes/PartitionReassignerProcess.cs`
- Test: `src/tests/KafkaWorker.UnitTests/Provisioning/PartitionReassignerProcessTests.cs`

**Interfaces:** `ReassignOptions(int IntervalSec, int BatchPartitions, int ExecSec, int RetrySubmitSec, int RotationTicketTimeoutSec = 3600)`; экспирация заявки `rebalances/<C>` (kind=`rebalance`) в трёх передержках под тройным гвардом §3.1 (прогресс `mode=balance` не жив ∧ staging `ca_next_*` отсутствует — оба предиката проверяются явно; идущая ребалансировка не снимается); чтение прогресс-ключа переносится ВЫШЕ проверки endpoints; финал balance — done-исход ДО del заявки.

- [ ] **Step 1: Опции**: `ReassignOptions` + optional `int RotationTicketTimeoutSec = 3600`, `Default` → `(15, 10, 180, 120, 3600)`; `Program.cs` → `new ReassignOptions(..., opts.Thresholds.ReassignExecSec, opts.Thresholds.ReassignRetrySubmitSec, opts.Thresholds.RotationTicketTimeoutSec)`.

- [ ] **Step 2: Тесты (AAA)** в `PartitionReassignerProcessTests.cs`:
```csharp
[Fact]
public async Task Run_EternalDrain_OldRebalanceTicket_Expired()        // drain-кандидат TO_REMOVE + заявка старая → del + исход kind=rebalance reason=waiting-drain; drain-прогресс не тронут
[Fact]
public async Task Run_ClusterDown_OldRebalanceTicket_Expired()         // endpoints нет + заявка старая, прогресс-ключ НЕ жив, staging нет → expired reason=waiting-cluster
[Fact]
public async Task Run_ClusterDownWithLiveBalance_TicketNotExpired()    // AC3: endpoints нет + прогресс mode=balance жив + заявка старая → заявка НЕ снята (гвард previous работает и в endpoints-точке), waiting-cluster
[Fact]
public async Task Run_BlindProbe_OldRebalanceTicket_Expired()          // DescribeTopicsAsync слепой + заявка старая, прогресс-ключ balance НЕ жив, staging нет → expired
[Fact]
public async Task Run_LiveBalanceProgress_OldTicket_NotExpired()       // AC3: прогресс mode=balance жив + заявка старая → заявка НЕ снята (батчи идут)
[Fact]
public async Task Run_OpenCaWindow_OldRebalanceTicket_NotExpired()     // AC3: staging ca_next_* жив + заявка старая (прогресс не жив) → заявка НЕ снята (гвард: окно ротации открыто), waiting
[Fact]
public async Task Run_FreshTicket_WaitingDrain()                       // возраст < порога → waiting-drain, заявка жива
[Fact]
public async Task Run_BalanceConverged_WritesDoneOutcome()             // сходимость → исход done kind=rebalance
```

- [ ] **Step 3: Прогон — FAIL** (`--filter FullyQualifiedName~PartitionReassignerProcessTests`).

- [ ] **Step 4: Реализация `PartitionReassignerProcess.cs`:**
1. Поле `private readonly TicketExpirator _tickets = new(etcd, endpoints);` + аудит `private readonly ConcurrentDictionary<string, TicketRequestAudit?> _ticketAudit = new();` (заполнение при `hasTicket` в `RunAsync`: `_ticketAudit[cluster] = TicketOutcomes.ParseAudit(ticket.Value!.Value);`).
2. **Переставить чтение `progressKv`/`previous` ВЫШЕ проверки «кластер не поднят»** (сегодня progressKv читается после слепой пробы D1): решение «идущая balance» нужно во всех трёх точках экспирации, включая самую раннюю — endpoints-точку.
3. Единый приватный метод — тройной гвард (предикаты 2 и 3 явно; предикат 1 — caller гарантирует hasTicket):
```csharp
// Экспирация заявки rebalances под тройным гвардом §3.1 (t10): заявка старее
// порога и НЕ начата — прогресс-ключ balance не жив (предикат 3: батчи не
// подаются) И окно ротации не открыто (предикат 2: staging ca_next_* —
// CA-окно K не смешивается с решением о заявке, гвард общий). Идущая
// ребалансировка (mode=balance жив) не снимается никогда — её стагнацию
// закрывает панельный kafka-reassignment-stale. Гвард по previous
// обязателен во ВСЕХ точках (включая «кластер не поднят»).
private async Task<Result<bool>> TryExpireRebalanceAsync(
    string cluster, string ticketPayload, string reason, ReassignProgress? previous, long now, CancellationToken ct)
{
    var stagingKey = await GetAsync(CaNextKeyKey(cluster), ct);
    if (!stagingKey.IsSuccess)
        return Result<bool>.Failed(stagingKey.Error!);
    var stagingPem = await GetAsync(CaNextPemKey(cluster), ct);
    if (!stagingPem.IsSuccess)
        return Result<bool>.Failed(stagingPem.Error!);
    var mutationLive = previous is { Mode: "balance" }
        || stagingKey.Value is not null
        || stagingPem.Value is not null;
    return await _tickets.TryExpireAsync(
        journal, cluster, Op, claims.InstanceId,
        TicketOutcomes.Key("/kafkaworker", cluster), TicketOutcomes.KindRebalance,
        RebalanceKey(cluster), ticketPayload, reason,
        options.RotationTicketTimeoutSec, now, mutationLive, ct);
}
```
(ключи-хелперы: `private static string CaNextKeyKey(string c) => $"/kafka/clusters/{c}/ca_next_key";` и симметричный `CaNextPemKey`.)
4. Три точки (при `hasTicket`; каждая передаёт фактический `previous`):
   - endpoints нет (`waiting-cluster`): `TryExpireRebalanceAsync(..., "waiting-cluster", previous, now, ct)` → expired → `_lastOk[cluster] = now; return Result.Success();` иначе прежний `JournalAsync`.
   - слепая проба (`waiting-cluster` от describe-all): `TryExpireRebalanceAsync(..., previous, ...)` → expired → Success (с `_lastOk`), иначе прежнее поведение.
   - drain-кандидат (`waiting-drain`): та же схема с reason=`waiting-drain` (прогресс дрейна — не balance → заявка не начата).
5. Финал balance (`RunBalanceAsync`, ветка сходимости) — **исход done ДО del заявки**: после del повтора ветки финала нет (заявки нет → B1 → journal `cancelled`), провал put между del и исходом терял бы done навсегда; put идемпотентен — повтор ветки сходимости до del перезапишет:
```csharp
// Исход done ДО del заявки (t10): после del ветка сходимости недостижима
// (B1-cancelled) — провал etcd между del и исходом терял бы done навсегда;
// put идемпотентен, повтор ветки перезапишет.
var outcomeDone = await _tickets.WriteDoneAsync(
    TicketOutcomes.Key("/kafkaworker", cluster), TicketOutcomes.KindRebalance,
    _ticketAudit.GetValueOrDefault(cluster), now, ct);
if (!outcomeDone.IsSuccess)
    return Fail(cluster, outcomeDone.Error!, "writing-outcome");
var delTicket = await DeleteAsync(RebalanceKey(cluster), prefix: false, ct);
if (!delTicket.IsSuccess)
    return Fail(cluster, delTicket.Error!, "deleting-ticket");
var delProgress = await DeleteAsync(ProgressKey(cluster), prefix: false, ct);
if (!delProgress.IsSuccess)
    return Fail(cluster, delProgress.Error!, "deleting-progress");
```
6. XML-доккласса: t10 (тройной гвард).

- [ ] **Step 5: Прогон — PASS** (новые + существующие).

- [ ] **Step 6: Commit**
```bash
git add src/KafkaWorker.Provisioning/Processes/ProcessCommon.cs src/KafkaWorker.App/Program.cs src/KafkaWorker.Provisioning/Processes/PartitionReassignerProcess.cs src/tests/KafkaWorker.UnitTests/Provisioning/PartitionReassignerProcessTests.cs
git commit -m "feat(t10): PartitionReassigner I — экспирация rebalances под тройным гвардом, done-исход"
```

**Вход:** Tasks 1–2. **Выход:** §3.2 I. **Проверка:** юниты I зелёные (вкл. «endpoints нет + живой balance → не снята», «живой staging → не снята»). **Spec:** §3.1 (гвард), §3.2 I, §5, AC1/3/8.

---

### Task 6: KafkaWorker — SecurityMigrator M (сужение guard'ов) + X2: полный набор заявок/исходов

**Files:**
- Modify: `src/KafkaWorker.Provisioning/Processes/SecurityMigrator.cs`
- Modify: `src/KafkaWorker.Provisioning/Processes/DeprovisioningProcess.cs`
- Test: `src/tests/KafkaWorker.UnitTests/Provisioning/SecurityMigratorTests.cs`, `src/tests/KafkaWorker.UnitTests/Provisioning/DeprovisioningProcessTests.cs`

**Interfaces:** `GuardAliveOperationsAsync` гейтится только живыми `reassignments/<C>`, `regens/<C>` и staging `ca_next_{key,pem}` (фаза `waiting-ca-window`); фаза `waiting-rotation` удаляется; X2-список демонтажа закрывает инвариант «заявка не переживает кластер»: + `admin_rotations/<C>`, `ca_rotations/<C>`, `ticket_outcomes/<C>` (сейчас отсутствуют в списке — сиротские заявки попадали бы в снапшот панели, Задача 9).

- [ ] **Step 1: Тесты (AAA):**
```csharp
// SecurityMigratorTests.cs
[Fact]
public async Task Run_LiveRotationTickets_DoNotGateMigration()  // AC6: живые rotations/admin_rotations/ca_rotations/rebalances → M идёт (journal НЕ waiting-rotation; M1-секреты созданы)
[Fact]
public async Task Run_LiveReassignment_StillGates()             // живой reassignments-прогресс → waiting-reassignment
[Fact]
public async Task Run_LiveRegen_StillGates()                    // живой regens-прогресс → waiting-reassignment
[Fact]
public async Task Run_OpenCaWindow_StillGates()                 // живой staging ca_next_* → waiting-ca-window

// DeprovisioningProcessTests.cs
[Fact]
public async Task Run_CleansAllRotationTicketsAndOutcomes()     // сид rotations/admin_rotations/ca_rotations/rebalances + ticket_outcomes → после демонтажа НИ ОДНОГО из этих ключей нет
```

- [ ] **Step 2: Прогон — FAIL** (`--filter FullyQualifiedName~SecurityMigratorTests` и `~DeprovisioningProcessTests`).

- [ ] **Step 3: Реализация:**
1. `SecurityMigrator.cs`: константу `PhaseWaitingRotation` удалить; добавить `private const string PhaseWaitingCaWindow = "waiting-ca-window";`. `GuardAliveOperationsAsync` целиком:
```csharp
// M0: передёргивают только НАЧАТЫЕ операции (t10): живые прогресс-ключи
// (reassignments/regens) и открытое CA-окно (staging ca_next_*). Живые ЗАЯВКИ
// (rotations/admin_rotations/ca_rotations/rebalances) миграцию НЕ гейтят:
// на премиграционном кластере их исполнение не могло начаться (H/K сами
// уходят в waiting-cluster до миграции) — после M заявки исполнятся;
// взаимное ожидание M↔заявка невозможно.
private async Task<(string Phase, string Key)?> GuardAliveOperationsAsync(string cluster, CancellationToken ct)
{
    foreach (var prefix in new[] { "/kafkaworker/reassignments/", "/kafkaworker/regens/" })
    {
        var progress = await GetAsync($"{prefix}{cluster}", ct);
        if (progress.IsSuccess && progress.Value is not null)
            return (PhaseWaitingReassignment, $"{prefix}{cluster}");
    }

    foreach (var key in new[] { $"/kafka/clusters/{cluster}/ca_next_key", $"/kafka/clusters/{cluster}/ca_next_pem" })
    {
        var staging = await GetAsync(key, ct);
        if (staging.IsSuccess && staging.Value is not null)
            return (PhaseWaitingCaWindow, key);
    }

    return null;
}
```
2. XML-доккласса M: обновить описание M0.
3. `DeprovisioningProcess.cs` `CleanKeysAsync` — в массив deletions добавить (кроме существующих):
```csharp
($"/kafkaworker/admin_rotations/{cluster}", false), // заявка admin-ротации не переживает кластер (инвариант «заявка не переживает кластер», t10)
($"/kafkaworker/ca_rotations/{cluster}", false),    // заявка CA-ротации не переживает кластер (t10)
($"/kafkaworker/ticket_outcomes/{cluster}", false), // исходы заявок не переживают кластер (t10) — иначе вечный kafka-ticket-expired
```
и комментарий класса X2 дополнить (полный набор заявок + исходы).

- [ ] **Step 4: Прогон — PASS** (SecurityMigrator + Deprovisioning + весь Provisioning-набор kafka).

- [ ] **Step 5: Commit**
```bash
git add src/KafkaWorker.Provisioning/Processes/SecurityMigrator.cs src/KafkaWorker.Provisioning/Processes/DeprovisioningProcess.cs src/tests/KafkaWorker.UnitTests/Provisioning/SecurityMigratorTests.cs src/tests/KafkaWorker.UnitTests/Provisioning/DeprovisioningProcessTests.cs
git commit -m "feat(t10): M-гварды только начатых операций; X2 чистит все заявки и исходы (kafka)"
```

**Вход:** Tasks 1–5. **Выход:** §2.5 (разрыв M↔заявки), §3.2 M, §3.4 X2-чистка. **Проверка:** юниты зелёные (вкл. чистка admin_rotations/ca_rotations/ticket_outcomes). **Spec:** §3.2 M, §5 (X2), AC6.

---

### Task 7: ValkeyWorker — опция порога + PasswordRotator E (waiting-cluster-семантика, экспирация, done)

**Files:**
- Modify: `src/ValkeyWorker.App/Options.cs` (`ThresholdsOptions` + `RotationTicketTimeoutSec = 3600`)
- Modify: `src/ValkeyWorker.App/appsettings.json`
- Modify: `src/ValkeyWorker.Provisioning/Processes/ProcessCommon.cs` (`ValkeyProvisioningOptions` + optional поле, `Default`)
- Modify: `src/ValkeyWorker.App/Program.cs` (`ToProvisioningOptions`; ctor `PasswordRotator`)
- Modify: `src/ValkeyWorker.Provisioning/Processes/PasswordRotator.cs`
- Test: `src/tests/ValkeyWorker.UnitTests/Provisioning/PasswordRotatorTests.cs`

**Interfaces:** `ValkeyProvisioningOptions(..., int RotationTicketTimeoutSec = 3600)`; `PasswordRotator(..., Func<string>? generator = null, int rotationTicketTimeoutSec = 3600)`; семантика: нет endpoints/admin-креда/ca_pem при живой заявке БЕЗ стейта → `waiting-cluster` + Success (вместо `Result.Failed`) + экспирация kind=`password-<role>` (тройной гвард §3.1 по построению точки: предикат 1 — заявка жива; предикат 2 — окно CA valkey закрыто, т.к. при открытом окне вентиль Active-ветки не вызывает E вообще, arch/21 §5 K; предикат 3 — стейт `work/<C>/rotation` отсутствует = ветка `state is null`); финал E3 — done-исход.

- [ ] **Step 1: Опции и тестовый фейк** (аналогично Task 3): `Options.cs` `ThresholdsOptions` + `RotationTicketTimeoutSec { get; set; } = 3600;` (XML-комментарий — arch/21 §8); `appsettings.json` `"Thresholds": { ..., "RotationTicketTimeoutSec": 3600 }`; `ProcessCommon.cs` `ValkeyProvisioningOptions` + optional `int RotationTicketTimeoutSec = 3600` и `Default` → `new(17000, 17999, 120, 90, null, "valkey/valkey:9.1.2", 3600)`; `Program.cs`: `ToProvisioningOptions` + последний аргумент, `PasswordRotator` → `..., rotationTicketTimeoutSec: opts.Thresholds.RotationTicketTimeoutSec`; `src/tests/ValkeyWorker.UnitTests/Fakes/Fakes.cs` (`FakeEtcd.TxnAsync`, ветка `TxnTarget.Version`) — поддержать `TxnPredicate.Greater` (правка идентична Task 3 Step 1 п.5).

- [ ] **Step 2: Тесты (AAA)** в `PasswordRotatorTests.cs` (valkey):
```csharp
[Fact]
public async Task Tick_ClusterDownNoState_OldTicket_Expired()     // AC7: нет endpoints + нет стейта + заявка старая → Success; del заявки; исход kind=password-app reason=waiting-cluster
[Fact]
public async Task Tick_ClusterDownNoState_FreshTicket_WaitsWithSuccess() // AC7: Success (НЕ Failed); journal waiting-cluster; заявка жива
[Fact]
public async Task Tick_LiveState_NoEndpoints_FailsAndKeepsTicket() // AC3: стейт e1-added жив + нет endpoints → Failed (доигрывание — третий предикат гварда ложен), заявка/стейт живы, исхода нет
[Fact]
public async Task Tick_BrokenPayload_NoExpiry()                   // AC4: нет endpoints + нет стейта + payload без requested_unix → не снимается
[Fact]
public async Task Tick_FullRotation_WritesDoneOutcome()           // AC8: полная ротация → исход done kind=password-app
```

- [ ] **Step 3: Прогон — FAIL** (`--filter FullyQualifiedName~ValkeyWorker.UnitTests.Provisioning.PasswordRotatorTests`).

- [ ] **Step 4: Реализация `PasswordRotator.cs` (valkey):**
1. ctor: `..., Func<string>? generator = null, int rotationTicketTimeoutSec = 3600)`; поля:
```csharp
private readonly TicketExpirator _tickets = new(gateway, endpoints);
private readonly int _timeoutSec = rotationTicketTimeoutSec;
```
2. В `TickAsync`, ветка `state is null` — ПЕРЕД валидацией роли добавить (а проверку «нет endpoints» из `StartRotationAsync` удалить):
```csharp
// Кластер не поднят при ОТСУТСТВУЮЩЕМ стейте — заявка не начата: waiting-cluster
// + Success (t10: тик кластера не фейлится, миграция T доведёт кластер,
// ротация исполнится после; стейт жив — ротация НАЧАТА, доигрывание ниже).
// Тройной гвард §3.1 по построению точки: заявка жива (предикат 1); окно CA
// закрыто — при открытом окне вентиль Active-ветки E не вызывает (arch/21
// §5 K, предикат 2); стейт отсутствует — ветка state-is-null (предикат 3).
if (snap.Endpoints is null || snap.AdminUser is null || snap.AdminPassword is null || snap.CaPem is null)
{
    var role2 = role is "app" or "admin" ? role : "app"; // битая роль — экспирация с каноничным kind
    var expired = await _tickets.TryExpireAsync(
        journal, cluster, Op, claims.InstanceId,
        TicketOutcomes.Key("/valkeyworker", cluster), $"password-{role2}",
        ProcessCommon.RotationKey(cluster), requestPayload, "waiting-cluster",
        _timeoutSec, DateTimeOffset.UtcNow.ToUnixTimeSeconds(), mutationLive: false, ct);
    if (!expired.IsSuccess)
        return expired.Error!;
    if (expired.Value)
        return Result.Success();
    var waiting = await journal.WritePhaseAsync(
        cluster, Op, "waiting-cluster", claims.InstanceId, null, ct);
    return waiting;
}
```
Порядок веток: сначала «нет endpoints» (для валидной И битой роли — waiting), затем битая роль → мусор (del с journal invalid-request — существующий путь), затем старт. Точка экспирации ЕДИНСТВЕННАЯ — только при `state is null` (стейт жив = начатая, см. `Tick_LiveState_NoEndpoints_FailsAndKeepsTicket`).
3. Финал E3 (`ResumeAsync`, после `DeleteStateAsync`, перед journal done):
```csharp
// Исход финала (t10): done гасит expired-алерт панели перезаписью.
// Аудит — из payload заявки в стейте; рестарт-хвост без стейта — фактическое время.
var audit = state.Request is { } payload ? TicketOutcomes.ParseAudit(payload) : null;
var outcomeDone = await _tickets.WriteDoneAsync(
    TicketOutcomes.Key("/valkeyworker", cluster), $"password-{role}", audit,
    DateTimeOffset.UtcNow.ToUnixTimeSeconds(), ct);
if (!outcomeDone.IsSuccess)
    return outcomeDone.Error!;
```
4. XML-доккласса: обновить (waiting-cluster-семантика t10, экспирация под гвардом, done).

- [ ] **Step 5: Прогон — PASS** (новые + существующие кейсы E).

- [ ] **Step 6: Commit**
```bash
git add src/ValkeyWorker.App/Options.cs src/ValkeyWorker.App/appsettings.json src/ValkeyWorker.Provisioning/Processes/ProcessCommon.cs src/ValkeyWorker.App/Program.cs src/ValkeyWorker.Provisioning/Processes/PasswordRotator.cs src/tests/ValkeyWorker.UnitTests/Provisioning/PasswordRotatorTests.cs src/tests/ValkeyWorker.UnitTests/Fakes/Fakes.cs
git commit -m "feat(t10): ValkeyWorker E — waiting-cluster-семантика, экспирация, done-исход"
```

**Вход:** Tasks 1–2. **Выход:** §3.3 E. **Проверка:** юниты E зелёные. **Spec:** §3.1 (гвард), §3.3 E, AC1/2/3/4/7/8.

---

### Task 8: ValkeyWorker — CaRotator K (экспирация K0.5-точек под гвардом) + X2 ticket_outcomes

**Files:**
- Modify: `src/ValkeyWorker.Provisioning/Processes/CaRotator.cs`
- Modify: `src/ValkeyWorker.Provisioning/Processes/DeprovisioningProcess.cs`
- Test: `src/tests/ValkeyWorker.UnitTests/Provisioning/CaRotatorTests.cs`, `src/tests/ValkeyWorker.UnitTests/Provisioning/DeprovisioningProcessTests.cs`

**Interfaces:** K0.5-точки `waiting-cluster`/`waiting-password-rotation` — экспирация kind=`ca` (payload заявки + `mutationLive` передаются в `WaitAsync`); K0.3 `WindowOpenAsync` уже уводит открытое окно в доигрывание мимо ждущих точек — staging-предикат гварда истинен по построению ветки, журнал — явной проверкой (вынести `CaMutationLive`, переиспользовать в `WindowOpenAsync`); финал K4 — done-исход; X2 + `/valkeyworker/ticket_outcomes/<C>`.

- [ ] **Step 1: Тесты (AAA):**
```csharp
// CaRotatorTests.cs (valkey)
[Fact]
public async Task Run_ClusterDown_OldCaTicket_Expired()           // K0.5-точка: нет endpoints + заявка старая → del + исход kind=ca reason=waiting-cluster; Waiting→Success
[Fact]
public async Task Run_PasswordRotationAlive_OldCaTicket_Expired() // K0.5-точка: живая ротация кредов → expired reason=waiting-password-rotation
[Fact]
public async Task Run_WindowOpen_OldCaTicket_NotExpired()         // AC3: staging жив (K0.3 → доигрывание) → заявка не снимается
[Fact]
public async Task Run_FreshCaTicket_WaitingCluster()              // свежая заявка → Waiting, жива
[Fact]
public async Task Run_FullRotation_WritesDoneOutcome()            // финал K4 → исход done kind=ca

// DeprovisioningProcessTests.cs (valkey)
[Fact]
public async Task Run_CleansTicketOutcomes()                      // X2 удаляет /valkeyworker/ticket_outcomes/<C>
```

- [ ] **Step 2: Прогон — FAIL** (`--filter FullyQualifiedName~ValkeyWorker.UnitTests.Provisioning.CaRotatorTests` / `~ValkeyWorker.UnitTests.Provisioning.DeprovisioningProcessTests`).

- [ ] **Step 3: Реализация:**
1. `CaRotator.cs` (valkey): поля `private readonly TicketExpirator _tickets = new(gateway, endpoints);` + `_ticketAudit` (ConcurrentDictionary<string, TicketRequestAudit?>; заполнение при живой заявке в `RunAsync`).
2. Третий предикат гварда — из существующей journal-части `WindowOpenAsync` (вынести, переиспользовать в обеих):
```csharp
// Третий предикат гварда экспирации (§3.1): журнал rotate-ca в мутационной
// фазе — вне {done, waiting-*}; K0.3 WindowOpenAsync использует то же
// условие как детектор открытого окна.
private static bool CaMutationLive(WorkState? journalState)
    => journalState is { Op: Op } j
       && j.Phase != PhaseDone
       && !j.Phase.StartsWith("waiting-", StringComparison.Ordinal);
```
3. `WaitAsync` — расширить (паттерн Task 4; сигнатура `WaitAsync(cluster, phase, ticketPayload, mutationLive, ct)`; при экспирации вернуть `RotationOutcome.Waiting` — тик не ошибка):
```csharp
// Ждущий исход с экспирацией под гвардом (t10): ca-заявка старее порога и
// гвард пройден (K0.5 достижима ТОЛЬКО при закрытом окне — K0.3 WindowOpenAsync
// уводит открытое в доигрывание: staging-предикат истинен по построению;
// mutationLive — явная проверка журнала) → снятие (kind=ca), иначе
// journal-waiting. Фаза expired — непрефиксованная (терминальная).
```
4. Точки K0.5 (`waiting-cluster`, `waiting-password-rotation`) передают `ticket.Value?.Value` и `CaMutationLive(journalState.Value)`; вызовы в `RunAsync`: `WaitAsync(cluster, "waiting-cluster", ticket.Value?.Value, CaMutationLive(journalState.Value), ct)` и т.д.
5. `FinishAsync`: до journal done — `WriteDoneAsync(TicketOutcomes.Key("/valkeyworker", cluster), TicketOutcomes.KindCa, _ticketAudit.GetValueOrDefault(cluster), ...)` (идемпотентно — хвост afterCommit повторит).
6. `DeprovisioningProcess.cs` (valkey) X2: массив строк ключей (формат — `foreach (var key in new[] { ... })`) дополнить строкой:
```csharp
                     $"/valkeyworker/ticket_outcomes/{cluster}", // исходы заявок не переживают кластер (t10)
```
7. XML-докклассов — t10 (K0.5-точки экспирационные под гвардом).

- [ ] **Step 4: Прогон — PASS** (CaRotator + Deprovisioning valkey).

- [ ] **Step 5: Commit**
```bash
git add src/ValkeyWorker.Provisioning/Processes/CaRotator.cs src/ValkeyWorker.Provisioning/Processes/DeprovisioningProcess.cs src/tests/ValkeyWorker.UnitTests/Provisioning/CaRotatorTests.cs src/tests/ValkeyWorker.UnitTests/Provisioning/DeprovisioningProcessTests.cs
git commit -m "feat(t10): ValkeyWorker K — экспирация ca-заявок под гвардом, done-исход; X2 чистит исходы"
```

**Вход:** Tasks 1–2, 7. **Выход:** §3.3 K, §3.4 X2. **Проверка:** юниты зелёные. **Spec:** §3.1 (гвард), §3.3 K, §3.4, AC1/2/3/8.

---

### Task 9: AdminPanel — снапшоты, парсеры, refreshers (ca_rotations kafka + ticket_outcomes)

**Files:**
- Modify: `src/AdminPanel.Core/Kafka/KafkaSnapshot.cs` (+`KafkaCaRotationTicket`, +`KafkaTicketOutcome`, optional-поля снапшота)
- Modify: `src/AdminPanel.Core/Valkey/ValkeySnapshot.cs` (+`ValkeyTicketOutcome`, optional-поле)
- Modify: `src/AdminPanel.Etcd/Parsing/KafkaParser.cs` (+`ParseCaRotations`, +`ParseTicketOutcomes`)
- Modify: `src/AdminPanel.Etcd/Parsing/ValkeyParser.cs` (+`ParseTicketOutcomes`)
- Modify: `src/AdminPanel.Etcd/KafkaSnapshotRefresher.cs` (префиксы, чтения, сборка)
- Modify: `src/AdminPanel.Etcd/ValkeySnapshotRefresher.cs` (префикс, чтение, сборка)
- Test: `src/tests/AdminPanel.UnitTests/KafkaParserTests.cs`, `src/tests/AdminPanel.UnitTests/ValkeyParserTests.cs`, `src/tests/AdminPanel.UnitTests/KafkaRefresherTests.cs`, `src/tests/AdminPanel.UnitTests/ValkeyRefresherTests.cs`

**Interfaces (потребители — Tasks 10–11):**
```csharp
// KafkaSnapshot.cs
public sealed record KafkaCaRotationTicket(string Cluster, long RequestedUnix, string? RequestedBy);
public sealed record KafkaTicketOutcome(
    string Cluster, string Kind, string Outcome, string? Reason,
    long RequestedUnix, string? RequestedBy, long FinishedUnix);
// KafkaSnapshot: дополнительные optional-параметры хвоста (после AdminRotations):
//   IReadOnlyList<KafkaCaRotationTicket>? CaRotations = null,
//   IReadOnlyList<KafkaTicketOutcome>? TicketOutcomes = null

// ValkeySnapshot.cs
public sealed record ValkeyTicketOutcome(... те же поля ...);
// ValkeySnapshot: IReadOnlyList<ValkeyTicketOutcome>? TicketOutcomes = null (после CaRotations)

// KafkaParser.ParseCaRotations(IReadOnlyList<Kv>) → KafkaCaRotationsParseResult (формат ParseAdminRotations);
// KafkaParser.ParseTicketOutcomes(IReadOnlyList<Kv>) → KafkaTicketOutcomesParseResult;
// ValkeyParser.ParseTicketOutcomes(IReadOnlyList<Kv>) → ValkeyTicketOutcomesParseResult.
```
Парсер исходов: обязательны `kind`/`outcome`/`requested_unix`/`finished_unix` (`outcome` толерантно-строковое, без enum — развивается); `reason`/`requested_by` опциональны; битое → `KeyParseError` (порт `ParseRotations`).

- [ ] **Step 1: Тесты парсеров (AAA)** — по образцу существующих `ParseAdminRotations`-кейсов: валидный ключ → тикет; нет обязательного поля/битый JSON → parseError; kafka ca_rotations ключ `/kafkaworker/ca_rotations/<C>`; исходы `/kafkaworker/ticket_outcomes/<C>` и `/valkeyworker/ticket_outcomes/<C>`.

- [ ] **Step 2: Прогон — FAIL** (`--filter FullyQualifiedName~KafkaParserTests` / `~ValkeyParserTests`).

- [ ] **Step 3: Реализация** записей + парсеров (копия паттерна `ParseAdminRotations` с другим префиксом; для исходов — чтение пяти полей через `JsonValues.Read*`).

- [ ] **Step 4: Тесты refreshers (AAA)**: сид ключей через существующие фейковые гейтвеи тестов → тик `RefreshOnceAsync` → снапшот содержит `CaRotations`/`TicketOutcomes`; битый ключ исхода → ParseErrors снапшота.

- [ ] **Step 5: Реализация refreshers:**
1. `KafkaSnapshotRefresher`: `Prefixes` + `CaRotations = "/kafkaworker/ca_rotations/"`, `TicketOutcomes = "/kafkaworker/ticket_outcomes/"`; чтения-рэнжи + проверки успеха; парсинг; в конструктор снапшота — `CaRotations: caRotations.Tickets, TicketOutcomes: outcomes.Tickets`; ParseErrors дополнить errors обоих.
2. `ValkeySnapshotRefresher`: симметрично `TicketOutcomes = "/valkeyworker/ticket_outcomes/"`.

- [ ] **Step 6: Прогон всех панельных юнитов — PASS**: `--filter FullyQualifiedName~AdminPanel.UnitTests`.

- [ ] **Step 7: Commit**
```bash
git add src/AdminPanel.Core/Kafka/KafkaSnapshot.cs src/AdminPanel.Core/Valkey/ValkeySnapshot.cs src/AdminPanel.Etcd/Parsing/KafkaParser.cs src/AdminPanel.Etcd/Parsing/ValkeyParser.cs src/AdminPanel.Etcd/KafkaSnapshotRefresher.cs src/AdminPanel.Etcd/ValkeySnapshotRefresher.cs src/tests/AdminPanel.UnitTests/
git commit -m "feat(t10): панель читает ca_rotations kafka и ticket_outcomes в снапшоты"
```

**Вход:** Tasks 1–8 (воркеры пишут исходы; панель читает независимо — можно параллельно с 3–8 после Task 1–2, но для порядка после). **Выход:** §3.5 снапшоты. **Проверка:** юниты парсеров/refresher'ов. **Spec:** §3.5, AC9 (ca_rotations парсится).

---

### Task 10: AdminPanel — alert-движки (8 новых kinds: kafka 5 + valkey 3, пороги RotationStaleSeconds)

**Files:**
- Modify: `src/AdminPanel.Core/Kafka/KafkaAlerting/KafkaAlertsOptions.cs` (+`RotationStaleSeconds = 1800`)
- Modify: `src/AdminPanel.Core/Valkey/ValkeyAlerting/ValkeyAlertsOptions.cs` (+`RotationStaleSeconds = 1800`)
- Modify: `src/AdminPanel.Core/Kafka/KafkaAlerting/KafkaAlertEngine.cs`
- Modify: `src/AdminPanel.Core/Valkey/ValkeyAlerting/ValkeyAlertEngine.cs`
- Test: `src/tests/AdminPanel.UnitTests/KafkaAlertRulesTests.cs`, `src/tests/AdminPanel.UnitTests/ValkeyAlertRulesTests.cs`

**Interfaces:** новые kinds движков (чистые функции; sinceUnix — по стабильному id через существующий `ResolveSince`) — 8 новых kinds (kafka 5 + valkey 3):
- kafka: `kafka-ca-rotation-pending` (info), `kafka-rotation-stale` / `kafka-ca-rotation-stale` / `kafka-rebalance-stale` (warning), `kafka-ticket-expired` (warning);
- valkey: `valkey-rotation-stale` / `valkey-ca-rotation-stale` (warning), `valkey-ticket-expired` (warning).

- [ ] **Step 1: Тесты (AAA)** — по одному на kind + порог/sinceUnix:
```csharp
// KafkaAlertRulesTests.cs
RotationStale_BeyondThreshold_Warning()          // заявка старше 1800 → warning kafka-rotation-stale (и для AdminRotations)
RotationStale_BelowThreshold_NoAlert()
CaRotationPending_Info()                         // живая ca-заявка → info kafka-ca-rotation-pending
CaRotationStale_BeyondThreshold_Warning()
RebalanceStale_BeyondThreshold_Warning()
TicketExpired_WarningWithReasonHint()            // исход expired → warning kafka-ticket-expired; Hint упоминает reason
TicketOutcome_Done_NoExpiredAlert()              // исход done → алерта нет (AC13: гашение перезаписью)
StaleAlert_SinceUnix_CarriedFromPrevious()       // стабильный id: prev с алертом → sinceUnix перенесён

// ValkeyAlertRulesTests.cs — valkey-rotation-stale / valkey-ca-rotation-stale / valkey-ticket-expired (аналогично)
```

- [ ] **Step 2: Прогон — FAIL** (`--filter FullyQualifiedName~KafkaAlertRulesTests` / `~ValkeyAlertRulesTests`).

- [ ] **Step 3: Реализация:**
1. Options: `public int RotationStaleSeconds { get; set; } = 1800;` (комментарий: половина `RotationTicketTimeoutSec` воркеров — полчаса видимости до снятия; связность stale < timeout).
2. `KafkaAlertEngine.Enumerate` — после блока rebalance-pending (живые кластеры через существующий `alive`):
   - `kafka-ca-rotation-pending` по `next.CaRotations ?? []` (порт kafka-rotation-pending);
   - stale-алерты: age = `nowUnix - RequestedUnix > _options.RotationStaleSeconds` по `Rotations`+`AdminRotations` (один алерт на кластер, age = max), `CaRotations`, `Rebalances`; id `kafka-{rotation|ca-rotation|rebalance}-stale:<C>`; Hint — «заявка не начата дольше N c — до возрастного снятия воркером осталось M c; проверьте journal (waiting-фаза)»;
   - `kafka-ticket-expired` по `next.TicketOutcomes ?? []` где `Outcome == "expired"` и `alive.Contains(Cluster)`; Hint — «устранить причину reason и повторить заявку»; поля Message/Attributes по канону (kind/reason/finishedUnix).
3. `ValkeyAlertEngine.Enumerate` — симметрично: `valkey-rotation-stale` (Rotations), `valkey-ca-rotation-stale` (`CaRotations ?? []`), `valkey-ticket-expired` (`TicketOutcomes ?? []`).
4. XML-комментарии обоих движков: дополнить каталог (t10).

- [ ] **Step 4: Прогон панельных юнитов — PASS** (`--filter FullyQualifiedName~AdminPanel.UnitTests`).

- [ ] **Step 5: Commit**
```bash
git add src/AdminPanel.Core/Kafka/KafkaAlerting/ src/AdminPanel.Core/Valkey/ValkeyAlerting/ src/tests/AdminPanel.UnitTests/KafkaAlertRulesTests.cs src/tests/AdminPanel.UnitTests/ValkeyAlertRulesTests.cs
git commit -m "feat(t10): stale/expired-алерты ротаций и ca-rotation-pending в панели (8 kinds)"
```

**Вход:** Task 9. **Выход:** §3.5 alert-движки. **Проверка:** юниты движков. **Spec:** §1(3), §3.5, §2.6, AC9/13.

---

### Task 11: AdminPanel — DTO/API + фронтенд (бейджи CA-ротации, строка исхода)

**Files:**
- Modify: `src/AdminPanel.Api/Inspection/KafkaQuery.cs` (+`CaRotationPending`, +`CaRotation`, +`TicketOutcome` DTO/мапперы/хендлеры)
- Modify: `src/AdminPanel.Api/Inspection/ValkeyQuery.cs` (+`TicketOutcome`)
- Modify: `frontend/src/api/dto.ts`
- Modify: `frontend/src/pages/KafkaClustersPage.tsx`, `frontend/src/pages/kafka-cluster/KafkaClusterDetailsPage.tsx`, `frontend/src/pages/valkey-cluster/ValkeyClusterDetailsPage.tsx`
- Test: `src/tests/AdminPanel.UnitTests/InspectionMappersTests.cs` (или `KafkaModelTests.cs` — где живут кейсы мапперов kafka) + `ValkeyModelTests.cs`

**Interfaces (JSON camelCase — зеркало arch/adminpanel/03 §7.2/§8.2):**
```csharp
// KafkaQuery.cs
public sealed record KafkaCaRotationTicketDto(long RequestedUnix, string? RequestedBy);
public sealed record TicketOutcomeDto(
    string Kind, string Outcome, string? Reason, long RequestedUnix, string? RequestedBy, long FinishedUnix);
// KafkaClusterSummaryDto + bool CaRotationPending;
// KafkaClusterDto + KafkaCaRotationTicketDto? CaRotation, + TicketOutcomeDto? TicketOutcome;
// ValkeyClusterDto + TicketOutcomeDto? TicketOutcome;
```

- [ ] **Step 1: Тесты мапперов (AAA)**: сводка kafka с ca-заявкой → `CaRotationPending=true`; детали kafka: caRotation/ticketOutcome проставлены по кластеру, null при отсутствии; valkey: ticketOutcome проставлен/null.

- [ ] **Step 2: Прогон — FAIL** (`--filter FullyQualifiedName~InspectionMappersTests` / `~ValkeyModelTests`).

- [ ] **Step 3: Реализация API:**
1. `KafkaClusterSummaryDto` + поле `bool CaRotationPending` (обновить `MapSummaries`/`MapSummary` — параметр `caRotationPending`, вызов из `MapSummaries` по `snapshot.CaRotations`).
2. `KafkaClusterDto` + `KafkaCaRotationTicketDto? CaRotation = null, TicketOutcomeDto? TicketOutcome = null`; `MapDetails` — параметры `caRotations`, `ticketOutcomes` (дефолт null → `?? []`); хендлер `KafkaClusterDetailsQueryHandler` передаёт `snapshot.CaRotations, snapshot.TicketOutcomes`.
3. `ValkeyClusterDto` + `TicketOutcomeDto? TicketOutcome = null`; `MapDetails(cluster, ticketOutcome)` — хендлер передаёт из снапшота.

- [ ] **Step 4: Фронтенд:**
1. `dto.ts`: `KafkaClusterSummary.caRotationPending: boolean`; `KafkaClusterDetails.caRotation: { requestedUnix, requestedBy } | null; ticketOutcome: TicketOutcomeDto | null` (общий `TicketOutcomeDto { kind, outcome, reason?, requestedUnix, requestedBy?, finishedUnix }`); `ValkeyClusterDetails.ticketOutcome` — по существующему стилю файла.
2. `KafkaClustersPage.tsx`: бейдж «CA-ротация» рядом с существующим `rotationPending` (существующий паттерн бейджа).
3. `KafkaClusterDetailsPage.tsx`: бейдж живой CA-ротации (по `caRotation`); строка «Последний исход заявки: kind → outcome (finished)» с подсветкой `expired` (цвет warning; текст причины `reason`).
4. `ValkeyClusterDetailsPage.tsx`: та же строка исхода.
- [ ] **Step 5: Проверки:**
```bash
cd frontend && npm run typecheck
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~AdminPanel.UnitTests"
```

- [ ] **Step 6: Commit**
```bash
git add src/AdminPanel.Api/Inspection/KafkaQuery.cs src/AdminPanel.Api/Inspection/ValkeyQuery.cs frontend/src/api/dto.ts frontend/src/pages/ src/tests/AdminPanel.UnitTests/
git commit -m "feat(t10): DTO/API и UI — бейдж CA-ротации kafka, строка последнего исхода заявки"
```

**Вход:** Task 9. **Выход:** §3.5 DTO/UI. **Проверка:** typecheck фронтенда + юниты панели. **Spec:** §3.5 DTO/UI, AC13-видимость.

---

### Task 12: KafkaWorker — интеграционные docker-тесты (дедлок, вечный waiting-cluster, rebalances)

**Files:**
- Create: `src/tests/KafkaWorker.IntegrationTests/Kafka/RotationTimeoutTests.cs`

**Interfaces:** использует `KafkaClusterFixture` (коллекция `KafkaCollection`), паттерн `CaRotationTests` (provisioning до канонического кластера, tick-циклы, диагностика в сообщении ассерта). Все заявки с `requested_unix` в прошлом (`now - 3700`) — порог 3600 срабатывает первым тиком, без реального ожидания.

- [ ] **Step 1: Сценарий AC10 — дедлок H↔K разорван** (`BothTicketsPasswordAndCa_PlayThroughWithoutExpiry`):
  - Arrange: канонический кластер (1 брокер, образец CaRotationTests); клэйм; ОДНОВРЕМЕННО ставятся заявки `/kafkaworker/rotations/<C>` и `/kafkaworker/ca_rotations/<C>` (обе `requested_unix = now`).
  - Act: цикл до дедлайна (~360 с): тик `PasswordRotator.RunAsync`, затем тик `CaRotator.RunAsync`, пока обе заявки живы; задержка 3 с.
  - Assert: обе заявки удалены; journal `rotate` = done и `rotate-ca` = done; НИ ОДНА фаза `expired` в journal; `ticket_outcomes/<C>` существует и `outcome != "expired"` (последний исход done — H затем K); `app_password` и `ca_pem` сменены. Диагностика — по образцу `CaRotationTests` (journal/стейджинг/DescribeCluster в сообщении ассерта).

- [ ] **Step 2: Сценарий AC11 — вечный waiting-cluster** (`PasswordTicketOnUnvalidatedCluster_ExpiredAndRetriable`):
  - Arrange: кластер поднят; `PutAsync("/kafka/clusters/<C>/endpoints", "")` (слепой кластер: снапшот без endpoints; staging отсутствует — дооконная ветка); заявка ротации `requested_unix = now - 3700`.
  - Act: один-два тика `PasswordRotator.RunAsync`.
  - Assert: заявка удалена; `/kafkaworker/ticket_outcomes/<C>` = `{"kind":"password-app","outcome":"expired","reason":"waiting-cluster",...}`; повторная `PutAsync` заявки проходит (ключа нет → 409-ситуация невозможна); journal-фаза `expired`.
  - Teardown: восстановить `endpoints` (фикстура чистит контур per-cluster; ассерт чистоты — по канону фикстуры).

- [ ] **Step 3: Сценарий AC12 — rebalances** (`RebalanceTicket_BlindClusterExpired_LiveBalanceKept`):
  - Arrange: кластер 2 брокера; `docker stop` контейнера broker1 (том жив; DescribeTopics слепой — bootstrap недостижим; staging отсутствует); заявка `/kafkaworker/rebalances/<C>` `requested_unix = now - 3700`.
  - Act/Assert 1: тик `PartitionReassignerProcess.RunAsync` → заявка снята, исход `{"kind":"rebalance","outcome":"expired",...}`, прогресс-ключ НЕ создан.
  - Act/Assert 2 (идущая не снимается): `docker start` broker1, дождаться DescribeCluster; свежая заявка rebalance → tick-цикл до появления прогресс-ключа `mode=balance`; затем `PutAsync` заявки с `requested_unix = now - 3700` (имитация возраста при идущей операции) → тик → заявка ЖИВА (экспирации нет — гвард: прогресс balance жив), `partitions_remaining` двигается.
  - Teardown: возврат контейнера, чистота фикстуры.

- [ ] **Step 4: Прогон серии (docker)**:
```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~RotationTimeoutTests"
```
Дождаться финальной строки; зачистить контуры серии (фикстура сама + страховочно):
```bash
docker ps -aq --filter name=kfw- | xargs -r docker rm -f; docker network prune -f
```

- [ ] **Step 5: Commit**
```bash
git add src/tests/KafkaWorker.IntegrationTests/Kafka/RotationTimeoutTests.cs
git commit -m "test(t10): интеграция — дедлок H+K доигрывается, экспирация waiting-cluster/rebalances (kafka)"
```

**Вход:** Tasks 3–6. **Выход:** AC10–12 интеграционно. **Проверка:** серия зелёная, контуры зачищены. **Spec:** §6 п.10–12, §1(1).

---

### Task 13: ValkeyWorker + панель — интеграционные тесты (экспирация valkey, исходы в снапшоте, алерты)

**Files:**
- Create: `src/tests/ValkeyWorker.IntegrationTests/Valkey/RotationTimeoutTests.cs`
- Modify: `src/tests/AdminPanel.IntegrationTests/ValkeySnapshotIntegrationTests.cs` (или новый `RotationOutcomeSnapshotTests.cs` рядом, по образцу)

**Interfaces:** valkey — `ValkeyClusterFixture` (паттерн `RotationTests.cs`); панель — etcd-контейнер фикстуры `ValkeyEtcdFixture`/`EtcdContainerFixture` (guid-префиксы, teardown-ассерт чистоты).

- [ ] **Step 1: ValkeyWorker-сценарий AC11-аналог** (`PasswordTicketOnUnraisedCluster_ExpiredAndRetriable`):
  - Arrange: поднятый кластер; `endpoints` затёрт (пустое значение); заявка `/valkeyworker/rotations/<C>` `{"role":"app","requested_unix":now-3700,"requested_by":"it"}`; стейта `work/<C>/rotation` нет.
  - Act: тик `PasswordRotator.TickAsync`.
  - Assert: Result Success (не Failed); заявка удалена; `/valkeyworker/ticket_outcomes/<C>` outcome=expired kind=password-app reason=waiting-cluster; повторная заявка ставится; полный цикл ротации на восстановленном кластере пишет исход done (перезапись исхода — AC13-тройка «expired → done»).
  - Зачистка per-fixture.

- [ ] **Step 2: Панель-сценарий AC13** (`RotationOutcomeAlerts_StaleThenExpiredThenDone`):
  - Arrange (живой etcd, guid-кластер `t10<guid>`): сид `/valkey/clusters/<C>/config` (Active) + endpoints; заявка `requested_unix = now - 2000` (> 1800); `ValkeySnapshotRefresher.RefreshOnceAsync`.
  - Assert 1: снапшот `TicketOutcomes` пуст; алерты содержат `valkey-rotation-stale` (warning).
  - Act 2: `EtcdSeed.PutAsync` `/valkeyworker/ticket_outcomes/<C>` outcome=expired (reason=waiting-cluster); del заявки; тик.
  - Assert 2: заявки нет → stale погас; алерт `valkey-ticket-expired` (warning).
  - Act 3: put заявки свежей + put исхода outcome=done; тик.
  - Assert 3: expired погашен перезаписью (done не алертится).
  - Симметрично kafka-часть (`KafkaSnapshotRefresher`): сид заявки старше порога + `ca_rotations` + исхода expired → `kafka-rotation-stale`/`kafka-ca-rotation-pending`/`kafka-ticket-expired`.
  - Teardown: prefix-del guid-ключей + ассерт чистоты (канон фикстуры).

- [ ] **Step 3: Прогон серий с зачисткой между** (valkey-интеграция → панель-интеграция):
```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~ValkeyWorker.IntegrationTests.Valkey.RotationTimeoutTests"
docker network prune -f
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~RotationOutcomeSnapshotTests"
docker network prune -f
```

- [ ] **Step 4: Commit**
```bash
git add src/tests/ValkeyWorker.IntegrationTests/Valkey/RotationTimeoutTests.cs src/tests/AdminPanel.IntegrationTests/
git commit -m "test(t10): интеграция — экспирация valkey-заявок и исходы/алерты панели"
```

**Вход:** Tasks 7–11. **Выход:** AC11/13 интеграционно. **Проверка:** серии зелёные, зачистка между сериями. **Spec:** §6 п.11/13, §2.6.

---

### Task 14: Мерж-гейт (канон AGENTS.md) + roadmap

**Files:**
- Modify: `arch/roadmap/reliability.md` (удалить пункт `t10-rotation-ticket-timeouts`), `arch/roadmap/reliability-report.md` (удалить строку таблицы)

**Interfaces:** — (процессуальный шаг; roadmap-правка — тем же мерж-коммитом, без пометок «закрыта»).

- [ ] **Step 1: Полная сборка + ВСЕ юниты** (одна серия, без docker):
```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~UnitTests"
```
Ожидание: 0 failed. Убедиться: нет осиротевших сетей `docker network ls | grep -c 'kfw-net\|vwk-net\|pgw-'` при нуле контейнеров → `docker network prune -f` при необходимости.

- [ ] **Step 2: Интеграционные серии ПО ОДНОЙ, с зачисткой между сериями** (после КАЖДОЙ — финальная строка прогона, затем зачистка):
```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~KafkaWorker.IntegrationTests"
docker ps -aq --filter name=kfw- | xargs -r docker rm -f; docker network prune -f
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~ValkeyWorker.IntegrationTests"
docker ps -aq --filter name=vwk- | xargs -r docker rm -f; docker network prune -f
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Debug --filter "FullyQualifiedName~AdminPanel.IntegrationTests"
docker network prune -f
```

- [ ] **Step 3: docker-E2E PgWorker на свежем Release** (код воркеров меняется — обязательный гейт):
```bash
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Scale_AddEmptyShard
```
E2eFixture собирает Release сам; никаких `PGW_TEST_E2E_NOBUILD`. После — зачистка pgw-контуров по канону.

- [ ] **Step 4: Roadmap-тег t10 удалить** (мерж-коммит):
1. `arch/roadmap/reliability.md`: удалить пункт `- **t10-rotation-ticket-timeouts** — возрастные таймауты/самозачистка зависших ротационных заявок kafka/valkey ...` (проверить: тег не участвует в `←`-зависимостях других пунктов — поиском по файлам `arch/roadmap/*.md`; при вхождении — удалить из зависимости тем же коммитом).
2. `arch/roadmap/reliability-report.md`: удалить строку `| t10-rotation-ticket-timeouts | ... |`.
3. Никаких пометок «реализована/закрыта» — только удаление.

- [ ] **Step 5: Финальный self-check перед гейтом code-review/user-review:**
- Все AC spec §6 покрыты (юниты: 1–9, вкл. 3a/3b/3c; интеграция: 10–13).
- `git grep -n "t10" -- arch/` — только допустимые контракты (arch/15/16/20/21/18, adminpanel/02/03 — обновлены spec-фазой, НЕ трогать).
- Сборка Release без warnings: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release`.

- [ ] **Step 6: Commit roadmap** (мерж-коммит):
```bash
git add arch/roadmap/reliability.md arch/roadmap/reliability-report.md
git commit -m "chore(t10): удалить тег задачи из roadmap (мерж-гейт)"
```

**Вход:** Tasks 1–13. **Выход:** мерж-гейт пройден, тег удалён. **Проверка:** все серии зелёные + E2E-маркер. **Spec:** §4 п.5, §6 «Критерии готовности».

---

## Порядок исполнения и зависимости

```
Task 1 (Shared.Etcd helper) ─┬─→ Task 2 (метрики)
                              ├─→ Task 3 (kafka H) → Task 4 (kafka K) → Task 5 (kafka I) → Task 6 (kafka M+X2)
                              ├─→ Task 7 (valkey E) → Task 8 (valkey K+X2)
                              └─→ Task 9 (панель: снапшоты) → Task 10 (панель: алерты)
                                                              → Task 11 (панель: DTO/UI)
Task 3–6 → Task 12 (kafka-интеграция)
Task 7–11 → Task 13 (valkey+панель-интеграция)
все → Task 14 (мерж-гейт + roadmap)
```

## Ключевые решения плана (сводка для ревью)

- **Helper общий** (`Shared.Etcd.Coordination.TicketExpirator`): дублирование протокола у двух воркеров заметное — критерий spec §4.1 выполнен; `TxnCompare.Exists` — новый примитив условного снятия (compare по существованию, spec §3.1).
- **Тройной гвард экспирации — контракт API helper**: предикаты 1 (заявка жива — payload не null) и 2 (окно не открыто — вызов только из дооконной ветки, staging за вызывающим) + предикат 3 — параметр `mutationLive` (журнал роли H / rotate-ca K / стейт E / прогресс balance I / живое окно чужой ротации). `mutationLive=true` → немедленный `false` без мутаций — начатое не снимается никогда. Waiting-точка сама по себе «не начато» не доказывает (spec §2.2).
- **K kafka: window-open-детект ПЕРВЫМ** (K0.3, зеркалит valkey): staging жив ИЛИ журнал `rotate-ca` вне `{done, waiting-*}` → доигрывание P→D→R→C без ждущих guard'ов и без экспирации; `WaitForBrokersAsync` толерантен к отсутствию endpoints (передержка окна, не NRE); аномалия-защита «окно открыто, а канона нет» — Failed. Дооконные waiting-точки экспирационные; guard'ы дополнены `admin_rotations` и `regens` (спека §3.2 K).
- **Точка `waiting-ca-window` H — принципиально не экспирационная**: staging жив по построению точки (предикат 2 гварда ложен) → `mutationLive: true` → чистый waiting. Экспирация H — только в `waiting-cluster`-точках (staging исключён guard'ом выше) под гвардом журнала роли (`phase-a`/`rotated-commit`/`phase-c`, вкл. `admin:*` — AC3c: слепой преф-чек посреди фазы A не экспирит).
- **Фаза `expired` — непрефиксованная** во всех журналах (и для admin-роли kafka H, где обычные фазы несут префикс `admin:`): терминальная фаза обязана точно совпадать со словарём `FinalPhases` метрики; различие app/admin — только в `kind` исхода и `reason`.
- **Экспирация только при живой заявке**: null-заявка = afterCommit-хвост (заявка удалена фазой B/C, доигрывается) — в `WaitOrExpireAsync` (H) параметр `Kv?`: null → чистый journal-waiting без экспирации; в точке H «нет endpoints» null-заявка сохраняет прежний no-op `Success(false)`.
- **Порядок исхода done — до терминального события, после которого ретрая нет**: для H/K kafka и E/K valkey — до journal done (провал put → тик Failed → финал повторится: H/K — afterCommit-хвостом, valkey K — хвостом committed, valkey E — стейтом доигрывания); для I — до del заявки (после del ветка сходимости недостижима — B1-cancelled; put идемпотентен, повтор ветки перезапишет).
- **Гвард «идущая balance» — по фактическому прогресс-ключу во ВСЕХ точках экспирации I** (включая самую раннюю «endpoints нет»): чтение progressKv перенесено выше endpoints-проверки; плюс staging-предикат (живое CA-окно тоже запрещает снятие — тройной гвард по букве §3.1).
- **Аудит заявки для done**: kafka H/K и valkey K — in-memory словарь (payload заявки недоступен после её del; рестарт в окне до финала — исход с фактическим временем `finishedUnix`, `requestedBy` опущен); valkey E — из payload в стейте доигрывания.
- **X2 kafka закрывает полный набор**: `admin_rotations/` и `ca_rotations/` добавлены в del-список вместе с `ticket_outcomes/` — инвариант «заявка не переживает кластер» (иначе сиротские заявки демонтрированного кластера попадают в новый снапшот панели, Задача 9).
- **Порог через existing-options**: `ProvisioningOptions`/`ReassignOptions`/`ValkeyProvisioningOptions` + optional-поле (существующие вызовы/фикстуры не ломаются), `PasswordRotator` valkey — отдельный ctor-параметр (опций в ctor нет).
