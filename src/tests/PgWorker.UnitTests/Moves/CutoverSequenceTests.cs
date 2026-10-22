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
        long dstRows = 50,
        IReadOnlyList<string>? sequences = null,
        IReadOnlyList<string>? tables = null)
    {
        var fake = new FakeMoveSql();
        fake.ScalarResolver = sql => sql switch
        {
            var s when s.Contains("string_agg(format('%I.%I'") => "bucket_42.\"items\"",
            var s when s.Contains("pg_current_wal_lsn") => lsn ?? throw new ApplicationException("LSN is not readable"),
            var s when s.Contains("bool_and(active") => caughtUp,
            var s when s.Contains("ELSE last_value - 1 END") => issued ?? throw new ApplicationException("issued is not readable"),
            var s when s.Contains("THEN last_value + 1") => next ?? throw new ApplicationException("sequence missing on the target"),
            var s when s.Contains("count(*) FROM bucket_42.") =>
                fake.LastDsn == MoveRig.SrcDsn ? srcRows : dstRows,
            _ => 0L,
        };
        fake.ListResolver = sql => sql.Contains("s.relkind = 'S'")
            ? sequences ?? ["seq1"]
            : sql.Contains("c.relkind IN ('r','p')")
                ? tables ?? ["items"]
                : [];
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
        flip.Value.Should().BeTrue("the full cutover passed");
        ctx.Sql.Calls.Should().Contain(c => c.Dsn == MoveRig.SrcDsn && c.Sql.Contains("LOCK TABLE"),
            "the freeze is a transactional batch with a LOCK barrier on the source");
        ctx.Sql.Calls.Should().NotContain(c => c.Sql.Contains("GRANT INSERT"),
            "a successful cutover does NOT unfreeze: the old shard stays frozen (P1-ghosts)");
        ctx.Sql.Calls.Should().NotContain(c => c.Sql.Contains("setval"),
            "next (101) > issued (100) — setval is not needed");
        ctx.StatusHistory.Should().Contain(s => s.State == MoveStates.Frozen && s.Phase == "frozen",
            "the FROZEN/frozen status recorded the router pause");
        ctx.StatusHistory.Should().Contain(s => s.Phase == "verify", "the row verification was preceded by the verify phase");
        ctx.StatusHistory.Should().Contain(s => s.Phase == "flip", "the flip phase precedes flip");
        ctx.Etcd.Store[MoveNames.RoutingKey("shop", "bucket_42")].Value.Should().Be("shard2",
            "the atomic flip switched routing");
        ctx.Etcd.Store.Should().NotContainKey(MoveNames.StatusKey("shop", "bucket_42"),
            "flip deleted the status key in the same txn (no key = ACTIVE)");
        ctx.Snapshots.Should().HaveCount(1, "snapshot point after flip");
    }

    // AAA: lock_timeout во всех попытках заморозки — transient freeze-failed, не permanent
    [Fact]
    public async Task LockTimeoutRetries_ThenGivesUp_Transient()
    {
        // Arrange — FreezeLockTries=3 (дефолт), пауза 0
        var sql = CutoverSql();
        sql.TransactionalResult = _ => Result.Failed(new ApplicationException("lock_timeout: a writer holds the table"));
        var ctx = NewCtx(sql);

        // Act
        var flip = await ctx.Sequence.RunAsync(ctx.Shards, MoveRig.Snap(), Move(), FastOptions, CancellationToken.None);

        // Assert
        flip.IsSuccess.Should().BeFalse("the freeze failed — the cutover is broken off");
        flip.Error.Should().NotBeOfType<CutoverPermanentException>(
            "a live writer is transient: the request is alive, ticks will retry");
        ctx.Sql.Calls.Count(c => c.Sql.Contains("REVOKE INSERT")).Should().Be(3,
            "exactly FreezeLockTries freeze attempts");
        var status = await ctx.Status.GetAsync("shop", "bucket_42", CancellationToken.None);
        status.Value!.State.Should().Be(MoveStates.Syncing);
        status.Value.Phase.Should().Be("freeze-failed", "the failure phase is freeze-failed");
        ctx.Etcd.Store[MoveNames.RoutingKey("shop", "bucket_42")].Value.Should().Be("shard1",
            "routing untouched — no flip happened");
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
        flip.IsSuccess.Should().BeFalse("the slot never confirmed the LSN — the cutover is broken off");
        flip.Error.Should().NotBeOfType<CutoverPermanentException>("replication is still catching up — transient");
        ctx.Sql.Calls.Should().Contain(c => c.Dsn == MoveRig.SrcDsn && c.Sql.Contains("GRANT INSERT"),
            "the source is unfrozen (GRANT symmetry)");
        var status = await ctx.Status.GetAsync("shop", "bucket_42", CancellationToken.None);
        status.Value!.Phase.Should().Be("catchup-timeout");
        status.Value!.State.Should().Be(MoveStates.Syncing, "the move is alive: the request remains");
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
        ctx.Sql.Calls.Should().Contain(c => c.Sql.Contains("GRANT INSERT"), "the freeze is released");
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
        flip.IsSuccess.Should().BeFalse("setval is impossible without the sequence on the target");
        flip.Error.Should().NotBeOfType<CutoverPermanentException>("a missing sequence is transient (fix and retry)");
        ctx.Sql.Calls.Should().Contain(c => c.Sql.Contains("GRANT INSERT"), "the freeze is released");
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
        flip.IsSuccess.Should().BeFalse("lag 0 does not guarantee copy completeness (P8)");
        flip.Error.Should().BeOfType<CutoverPermanentException>(
            "a defective copy cannot be fixed by retries");
        flip.Error!.Message.Should().Contain("abort", "operator hint — abort + a new move");
        ctx.Sql.Calls.Should().Contain(c => c.Sql.Contains("GRANT INSERT"),
            "the unfreeze on verify-failed is done (replication is alive)");
        var status = await ctx.Status.GetAsync("shop", "bucket_42", CancellationToken.None);
        status.Value!.Phase.Should().Be("verify-failed");
        status.Value!.State.Should().Be(MoveStates.Syncing, "the key remains: the move lives until abort");
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
        flip.IsSuccess.Should().BeFalse("compare on routing=cur must not converge");
        flip.Error.Should().BeOfType<CutoverPermanentException>("a control-plane conflict — manual investigation");
        ctx.Sql.Calls.Should().NotContain(c => c.Sql.Contains("GRANT INSERT"),
            "the freeze is NOT released: P1-ghosts must not come alive before the investigation");
        ctx.Etcd.Store[MoveNames.RoutingKey("shop", "bucket_42")].Value.Should().Be("shard9",
            "the foreign value is not overwritten");
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
        flip.Value.Should().BeTrue("setval advanced the counter — the cutover continued");
        ctx.Sql.Calls.Should().Contain(c => c.Dsn == MoveRig.DstDsn && c.Sql == """SELECT setval('bucket_42."seq1"', 100, true)""",
            "setval on the target with the source value (P6: forward only)");
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
            "a rollback before flip = the bucket is ACTIVE again (no key)");
    }

    // AAA: каталог источника с именами вне алфавита Ident ("Orders", a"b, "a, b",
    //      "select"; sequence "Orders_seq") — cutover проходит: имена квочатся,
    //      ArgumentException не возникает (до фикса — синхронный крах на шаге 5/6)
    [Fact]
    public async Task ExoticCatalogNames_CutoverFlips()
    {
        // Arrange
        var ctx = NewCtx(CutoverSql(
            issued: 100, next: 5,
            sequences: ["Orders_seq"],
            tables: ["Orders", "a\"b", "a, b", "select"]));

        // Act
        var flip = await ctx.Sequence.RunAsync(ctx.Shards, MoveRig.Snap(), Move(), FastOptions, CancellationToken.None);

        // Assert
        flip.Value.Should().BeTrue("quoting handles any catalog name — no exception, no freeze loop");
        ctx.Sql.Calls.Should().Contain(c => c.Dsn == MoveRig.SrcDsn
            && c.Sql.Contains("count(*) FROM bucket_42.\"Orders\""), "the mixed-case table is verified quoted");
        ctx.Sql.Calls.Should().Contain(c => c.Dsn == MoveRig.SrcDsn
            && c.Sql.Contains("count(*) FROM bucket_42.\"a\"\"b\""), "an embedded double quote is doubled");
        ctx.Sql.Calls.Should().Contain(c => c.Dsn == MoveRig.SrcDsn
            && c.Sql.Contains("count(*) FROM bucket_42.\"a, b\""), "a name with ', ' survives the row listing");
        ctx.Sql.Calls.Should().Contain(c => c.Dsn == MoveRig.DstDsn
            && c.Sql == "SELECT setval('bucket_42.\"Orders_seq\"', 100, true)",
            "setval forward for the quoted sequence (P6)");
    }

    // AAA: экзотическое имя + падение setval на первом заходе — transient
    //      sequences-failed с разморозкой; повторный заход с теми же именами —
    //      flip (идемпотентность resume: заморозка повторяема, setval только вперёд)
    [Fact]
    public async Task ExoticNames_SetvalFailThenRetry_Flips()
    {
        // Arrange — setval падает ровно один раз (первый тик)
        var sql = CutoverSql(issued: 100, next: 5, sequences: ["Orders_seq"]);
        var setvalFailedOnce = false;
        sql.ExecuteResult = s =>
        {
            if (s.Contains("setval") && !setvalFailedOnce)
            {
                setvalFailedOnce = true;
                return Result.Failed(new ApplicationException("setval failed (first tick)"));
            }
            return Result.Success();
        };
        var ctx = NewCtx(sql);

        // Act
        var first = await ctx.Sequence.RunAsync(ctx.Shards, MoveRig.Snap(), Move(), FastOptions, CancellationToken.None);
        // статус первого захода снимаем ДО второго: успешный flip повторного
        // захода удаляет статус-ключ (нет ключа = ACTIVE) — после second его нет
        var statusAfterFirst = await ctx.Status.GetAsync("shop", "bucket_42", CancellationToken.None);
        var second = await ctx.Sequence.RunAsync(ctx.Shards, MoveRig.Snap(), Move(), FastOptions, CancellationToken.None);

        // Assert
        first.IsSuccess.Should().BeFalse("the first run died on setval — transient sequences-failed");
        ctx.Sql.Calls.Should().Contain(c => c.Dsn == MoveRig.SrcDsn && c.Sql.Contains("GRANT INSERT"),
            "the failed run unfroze the source");
        statusAfterFirst.Value!.State.Should().Be(MoveStates.Syncing, "the move is alive: the request remains");
        statusAfterFirst.Value!.Phase.Should().Be("sequences-failed");
        second.Value.Should().BeTrue("the retry with the same exotic names completes the flip");
    }
}
