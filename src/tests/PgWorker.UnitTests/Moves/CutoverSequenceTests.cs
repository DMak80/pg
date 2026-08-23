using PgWorker.Core;
using PgWorker.Moves;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Probes;
using PgWorker.UnitTests.Provisioning;
using Xunit;

namespace PgWorker.UnitTests.Moves;

// CutoverSequence (t01 задача 12, spec §6.2 — точный перенос cutover_flip):
// заморозка P1/P5 с ретраями → FROZEN → LSN → ожидание слота → sequences P6 →
// сверка строк P8 → атомарный flip. Классификация отказов: transient
// (freeze/lsn/catchup/sequences — разморозка + FailState) vs permanent
// (CutoverPermanentException: verify-failed с разморозкой, flip-conflict с
// ОСТАВЛЕННОЙ заморозкой) — ревью №1.
public class CutoverSequenceTests
{
    private static readonly MovesRuntimeOptions FastOptions = new(PollIntervalSec: 0, FreezeWaitSec: 0);

    // Контекст стенда: топология MoveRig (shard1 → shard2), статус SYNCING/ddl, routing.
    private sealed record Ctx(
        Fakes.FakeEtcd Etcd, FakeMoveSql Sql, MoveStatusStore Status, CutoverSequence Sequence,
        ShardEndpoints Shards, List<MoveStatus> StatusHistory, List<int> Snapshots);

    private static Ctx NewCtx(FakeMoveSql sql, string routing = "shard1", string statusPhase = "ddl")
    {
        var etcd = new Fakes.FakeEtcd();
        MoveRig.SeedTopology(etcd);
        etcd.Seed(MoveNames.RoutingKey("shop", "bucket_42"), routing);
        if (statusPhase is not null)
            etcd.Seed(MoveNames.StatusKey("shop", "bucket_42"), new MoveStatus(
                "bucket_42", MoveStates.Syncing, "shard1", "shard2", 111, 122, statusPhase).Serialize());

        // История статусов — через OnPut-хук FakeEtcd (сами ключи flip стирает).
        var history = new List<MoveStatus>();
        etcd.OnPut = key =>
        {
            if (key == MoveNames.StatusKey("shop", "bucket_42"))
                history.Add(MoveStatus.Parse(etcd.Store[key].Value).Value!);
        };

        var status = new MoveStatusStore(etcd, [MoveRig.Ep]);
        var shards = new ShardEndpoints(etcd, [MoveRig.Ep], new ShardProbe(new HttpClient()));
        var snapshots = new List<int>();
        return new Ctx(etcd, sql, status,
            new CutoverSequence(sql, status, MoveRig.Secrets), shards, history, snapshots);
    }

    // Полный happy-резолвер cutover: одна таблица items, одна sequence, слот догнал.
    private static FakeMoveSql CutoverSql(
        string? lsn = "0/A000123",
        bool caughtUp = true,
        long? issued = 100,
        long? next = 101,
        long srcRows = 50,
        long dstRows = 50)
    {
        var fake = new FakeMoveSql();
        fake.ScalarResolver = sql => sql switch
        {
            var s when s.Contains("string_agg(format('%I.%I'") => "bucket_42.\"items\"",
            var s when s.Contains("pg_current_wal_lsn") => lsn ?? throw new ApplicationException("LSN не читается"),
            var s when s.Contains("bool_and(active") => caughtUp,
            var s when s.Contains("ELSE last_value - 1 END") => issued ?? throw new ApplicationException("issued не читается"),
            var s when s.Contains("THEN last_value + 1") => next ?? throw new ApplicationException("sequence на приёмнике отсутствует"),
            var s when s.Contains("count(*) FROM bucket_42.") =>
                fake.LastDsn == MoveRig.SrcDsn ? srcRows : dstRows,
            _ => 0L,
        };
        fake.ListResolver = sql => sql.Contains("s.relkind = 'S'") ? ["seq1"] : [];
        return fake;
    }

    private static CutoverContext Move(bool dropStatusOnFail = false) => new(
        "shop", "bucket_42", "shard1", "shard2", "sub_bucket_42", MoveStates.Syncing, dropStatusOnFail);

    // AAA: happy path — транзакционная заморозка на cur-DSN, FROZEN/frozen, LSN+слот,
    //      sequences не трогаются (next > issued), сверки равны, flip прошёл, разморозки НЕТ
    [Fact]
    public async Task HappyPath_FreezesFrozenFlipsUnfreezesNothing()
    {
        // Arrange
        var ctx = NewCtx(CutoverSql());

        // Act
        var flip = await ctx.Sequence.RunAsync(ctx.Shards, MoveRig.Snap(), Move(), FastOptions,
            CancellationToken.None, ct => { ctx.Snapshots.Add(1); return Task.FromResult(Result.Success()); });

        // Assert
        flip.Value.Should().BeTrue("полный cutover прошёл");
        ctx.Sql.Calls.Should().Contain(c => c.Dsn == MoveRig.SrcDsn && c.Sql.Contains("LOCK TABLE"),
            "заморозка — транзакционный батч с барьером LOCK на источнике");
        ctx.Sql.Calls.Should().NotContain(c => c.Sql.Contains("GRANT INSERT"),
            "успешный cutover НЕ размораживает: старый шард остаётся замороженным (P1-призраки)");
        ctx.Sql.Calls.Should().NotContain(c => c.Sql.Contains("setval"),
            "next (101) > issued (100) — setval не нужен");
        ctx.StatusHistory.Should().Contain(s => s.State == MoveStates.Frozen && s.Phase == "frozen",
            "статус FROZEN/frozen фиксировал паузу роутера");
        ctx.StatusHistory.Should().Contain(s => s.Phase == "verify", "сверке строк предшествовала фаза verify");
        ctx.StatusHistory.Should().Contain(s => s.Phase == "flip", "перед flip — фаза flip");
        ctx.Etcd.Store[MoveNames.RoutingKey("shop", "bucket_42")].Value.Should().Be("shard2",
            "атомарный flip перевёл routing");
        ctx.Etcd.Store.Should().NotContainKey(MoveNames.StatusKey("shop", "bucket_42"),
            "flip удалил статус-ключ той же txn (нет ключа = ACTIVE)");
        ctx.Snapshots.Should().HaveCount(1, "снапшот-точка после flip");
    }

    // AAA: lock_timeout во всех попытках заморозки — transient freeze-failed, не permanent
    [Fact]
    public async Task LockTimeoutRetries_ThenGivesUp_Transient()
    {
        // Arrange — FreezeLockTries=3 (дефолт), пауза 0
        var sql = CutoverSql();
        sql.TransactionalResult = _ => Result.Failed(new ApplicationException("lock_timeout: писатель держит таблицу"));
        var ctx = NewCtx(sql);

        // Act
        var flip = await ctx.Sequence.RunAsync(ctx.Shards, MoveRig.Snap(), Move(), FastOptions, CancellationToken.None);

        // Assert
        flip.IsSuccess.Should().BeFalse("заморозка не удалась — cutover сорван");
        flip.Error.Should().NotBeOfType<CutoverPermanentException>(
            "живой писатель — transient: заявка жива, тики повторят");
        ctx.Sql.Calls.Count(c => c.Sql.Contains("REVOKE INSERT")).Should().Be(3,
            "ровно FreezeLockTries попыток заморозки");
        var status = await ctx.Status.GetAsync("shop", "bucket_42", CancellationToken.None);
        status.Value!.State.Should().Be(MoveStates.Syncing);
        status.Value.Phase.Should().Be("freeze-failed", "фаза отказа — freeze-failed");
        ctx.Etcd.Store[MoveNames.RoutingKey("shop", "bucket_42")].Value.Should().Be("shard1",
            "routing не тронут — flip не было");
    }

    // AAA: слот не догоняет за CutoverTimeoutSec — разморозка + transient catchup-timeout
    [Fact]
    public async Task SlotNeverCatchesUp_Unfreezes_Transient()
    {
        // Arrange — таймаут 1с, поллинг 1с (реальная секунда ожидания)
        var ctx = NewCtx(CutoverSql(caughtUp: false));
        var slow = new MovesRuntimeOptions(PollIntervalSec: 1, FreezeWaitSec: 0, CutoverTimeoutSec: 1);

        // Act
        var flip = await ctx.Sequence.RunAsync(ctx.Shards, MoveRig.Snap(), Move(), slow, CancellationToken.None);

        // Assert
        flip.IsSuccess.Should().BeFalse("слот не подтвердил LSN — cutover сорван");
        flip.Error.Should().NotBeOfType<CutoverPermanentException>("репликация продолжает догонять — transient");
        ctx.Sql.Calls.Should().Contain(c => c.Dsn == MoveRig.SrcDsn && c.Sql.Contains("GRANT INSERT"),
            "источник разморожен (GRANT-симметрия)");
        var status = await ctx.Status.GetAsync("shop", "bucket_42", CancellationToken.None);
        status.Value!.Phase.Should().Be("catchup-timeout");
        status.Value!.State.Should().Be(MoveStates.Syncing, "переезд жив: заявка остаётся");
    }

    // AAA: чтение LSN упало — разморозка + transient lsn-failed
    [Fact]
    public async Task LsnReadFails_Unfreezes_Transient()
    {
        // Arrange
        var ctx = NewCtx(CutoverSql(lsn: null));

        // Act
        var flip = await ctx.Sequence.RunAsync(ctx.Shards, MoveRig.Snap(), Move(), FastOptions, CancellationToken.None);

        // Assert
        flip.IsSuccess.Should().BeFalse();
        flip.Error.Should().NotBeOfType<CutoverPermanentException>();
        ctx.Sql.Calls.Should().Contain(c => c.Sql.Contains("GRANT INSERT"), "заморозка снята");
        var status = await ctx.Status.GetAsync("shop", "bucket_42", CancellationToken.None);
        status.Value!.Phase.Should().Be("lsn-failed");
    }

    // AAA: sequence отсутствует на приёмнике (дрейф P5) — разморозка + transient sequences-failed
    [Fact]
    public async Task SequenceMissingOnDst_Unfreezes_Transient()
    {
        // Arrange
        var ctx = NewCtx(CutoverSql(next: null));

        // Act
        var flip = await ctx.Sequence.RunAsync(ctx.Shards, MoveRig.Snap(), Move(), FastOptions, CancellationToken.None);

        // Assert
        flip.IsSuccess.Should().BeFalse("setval невозможен без sequence на приёмнике");
        flip.Error.Should().NotBeOfType<CutoverPermanentException>("отсутствие sequence — transient (починят и повторят)");
        ctx.Sql.Calls.Should().Contain(c => c.Sql.Contains("GRANT INSERT"), "заморозка снята");
        var status = await ctx.Status.GetAsync("shop", "bucket_42", CancellationToken.None);
        status.Value!.Phase.Should().Be("sequences-failed");
    }

    // AAA: сверка строк не сошлась — дефектная копия (P8): разморозка + PERMANENT с подсказкой abort
    [Fact]
    public async Task RowCountsMismatch_Unfreezes_Permanent()
    {
        // Arrange — источник 50 строк, приёмник 49 (failover приёмника потерял срез)
        var ctx = NewCtx(CutoverSql(srcRows: 50, dstRows: 49));

        // Act
        var flip = await ctx.Sequence.RunAsync(ctx.Shards, MoveRig.Snap(), Move(), FastOptions, CancellationToken.None);

        // Assert
        flip.IsSuccess.Should().BeFalse("лаг 0 не гарантирует полноту копии (P8)");
        flip.Error.Should().BeOfType<CutoverPermanentException>(
            "дефектная копия — переезд не вылечить ретраями");
        flip.Error!.Message.Should().Contain("abort", "подсказка оператору — abort + повторный move");
        ctx.Sql.Calls.Should().Contain(c => c.Sql.Contains("GRANT INSERT"),
            "разморозка при verify-failed сделана (репликация жива)");
        var status = await ctx.Status.GetAsync("shop", "bucket_42", CancellationToken.None);
        status.Value!.Phase.Should().Be("verify-failed");
        status.Value!.State.Should().Be(MoveStates.Syncing, "ключ остаётся: переезд живёт до abort");
    }

    // AAA: routing изменился под руками — flip-conflict: PERMANENT, заморозка ОСТАВЛЕНА
    [Fact]
    public async Task FlipCompareFails_Permanent_FreezeLeft()
    {
        // Arrange — конкурент уже перевёл routing
        var ctx = NewCtx(CutoverSql(), routing: "shard9");

        // Act
        var flip = await ctx.Sequence.RunAsync(ctx.Shards, MoveRig.Snap(), Move(), FastOptions, CancellationToken.None);

        // Assert
        flip.IsSuccess.Should().BeFalse("compare по routing=cur обязан не сойтись");
        flip.Error.Should().BeOfType<CutoverPermanentException>("конфликт контрол-плейна — разбор вручную");
        ctx.Sql.Calls.Should().NotContain(c => c.Sql.Contains("GRANT INSERT"),
            "заморозка НЕ снята: P1-призраки не должны ожить до разбора");
        ctx.Etcd.Store[MoveNames.RoutingKey("shop", "bucket_42")].Value.Should().Be("shard9",
            "чужое значение не перезатёрто");
    }

    // AAA: sequence приёмника отстаёт — setval ТОЛЬКО вперёд, до выданного на источнике
    [Fact]
    public async Task SequenceBackward_SetvalForward()
    {
        // Arrange — issued=100, next=5: приёмник отстаёт на 95
        var ctx = NewCtx(CutoverSql(issued: 100, next: 5));

        // Act
        var flip = await ctx.Sequence.RunAsync(ctx.Shards, MoveRig.Snap(), Move(), FastOptions, CancellationToken.None);

        // Assert
        flip.Value.Should().BeTrue("setval довёл счётчик — cutover продолжился");
        ctx.Sql.Calls.Should().Contain(c => c.Dsn == MoveRig.DstDsn && c.Sql == """SELECT setval('bucket_42."seq1"', 100, true)""",
            "setval на приёмнике со значением источника (P6: только вперёд)");
    }

    // AAA: DropStatusOnFail (rollback) — fail-путь удаляет статус-ключ (нет ключа = ACTIVE)
    [Fact]
    public async Task DropStatusOnFail_DeletesStatusInsteadOfPut()
    {
        // Arrange — sequences-fail + rollback-семантика исхода
        var ctx = NewCtx(CutoverSql(next: null));

        // Act
        var flip = await ctx.Sequence.RunAsync(ctx.Shards, MoveRig.Snap(), Move(dropStatusOnFail: true),
            FastOptions, CancellationToken.None);

        // Assert
        flip.IsSuccess.Should().BeFalse();
        ctx.Etcd.Store.Should().NotContainKey(MoveNames.StatusKey("shop", "bucket_42"),
            "rollback-откат до flip = бакет снова ACTIVE (нет ключа)");
    }
}
