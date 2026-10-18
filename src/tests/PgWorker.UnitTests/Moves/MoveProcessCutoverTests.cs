using PgWorker.Core;
using PgWorker.Moves;
using PgWorker.Provisioning.Processes;
using Xunit;

namespace PgWorker.UnitTests.Moves;

// MoveProcess M4–M6 (t01 задача 14, spec §6.1/§6.2): cutover-блок с классификацией
// отказов (transient — заявка жива, ретраи тиками; permanent verify-failed — del
// заявки + статус SYNCING/verify-failed жив + подсказка «abort + повторный move»;
// flip-conflict — permanent, заморозка ОСТАВЛЕНА), срез прямой подписки ДО
// обратной (анти-петля), done + снапшот flip. Сид — SYNCING/cutover-wait
// (initial copy завершён) либо FROZEN (resume после сбоя cutover).
public class MoveProcessCutoverTests
{
    private static MoveStatus CutoverWaitStatus() => new(
        "bucket_42", MoveStates.Syncing, "shard1", "shard2", 111, 122, MovePhases.CutoverWait);

    // Быстрые опции тика: без пауз заморозки/поллинга (таймаут cutover — по тесту).
    private static readonly MovesRuntimeOptions Fast = new(PollIntervalSec: 0, FreezeWaitSec: 0);

    // AAA: happy cutover — flip прошёл, прямая подписка срезана, обратная pub/sub
    //      создана (copy_data=false — без re-copy), заявка удалена, Done
    [Fact]
    public async Task M4_CutoverFlipSuccess_DropsSubCreatesReverse_Done()
    {
        // Arrange — initial copy завершён, сверка строк сойдётся
        var rig = await MoveRig.NewAsync(seededStatus: CutoverWaitStatus(), runtime: Fast);
        MoveRig.CutoverLayer(rig.Sql);

        // Act
        var tick = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert
        tick.Value.Should().Be(ProcessOutcome.Done, "the move finished, auto-finalize is queued");
        rig.Etcd.Store[MoveNames.RoutingKey("shop", "bucket_42")].Value.Should().Be("shard2",
            "the atomic flip switched routing to the target");
        rig.Etcd.Store.Should().NotContainKey(MoveNames.StatusKey("shop", "bucket_42"),
            "flip deleted the status key in the same txn (no key = ACTIVE)");
        // Auto-finalize: заявка заменена на finalize (не удалена)
        rig.Etcd.Store.Should().ContainKey(MoveNames.MoveKey("shop", "bucket_42"),
            "auto-finalize: the request is replaced with op=finalize, not deleted");
        rig.Etcd.Store[MoveNames.MoveKey("shop", "bucket_42")].Value.Should().Contain("\"op\":\"finalize\"",
            "the request is rewritten as finalize");
        rig.Etcd.Store[MoveNames.MoveKey("shop", "bucket_42")].Value.Should().Contain("\"old_shard\":\"shard1\"",
            "finalize knows the old shard");
        rig.Sql.Calls.Should().Contain(c => c.Dsn == MoveRig.DstDsn
            && c.Sql == "DROP SUBSCRIPTION sub_bucket_42",
            "the direct subscription is cut on the new owner");
        rig.Sql.Calls.Should().Contain(c => c.Dsn == MoveRig.DstDsn
            && c.Sql == "CREATE PUBLICATION pub_bucket_42_rb FOR TABLES IN SCHEMA bucket_42",
            "the reverse publication is on the new owner");
        rig.Sql.Calls.Should().Contain(c => c.Dsn == MoveRig.SrcDsn && c.Sql ==
            "CREATE SUBSCRIPTION sub_bucket_42_rb CONNECTION 'host=h1,h2 port=15002,15003 dbname=shop user=bucket_mover password=mov-pw sslmode=require target_session_attrs=read-write' PUBLICATION pub_bucket_42_rb " +
            "WITH (copy_data = false, failover = true, synchronous_commit = remote_apply)",
            "the reverse subscription is on the old owner: target conninfo, WITHOUT initial copy");
    }

    // AAA: transient-отказ cutover (слот не догоняет) — заявка ЖИВА, статус
    //      SYNCING/catchup-timeout, ретраи тиками (ревью №1)
    [Fact]
    public async Task M4_CutoverTransientFail_RequestSurvives()
    {
        // Arrange — слот никогда не подтверждает LSN, таймаут 0 (мгновенный)
        var rig = await MoveRig.NewAsync(seededStatus: CutoverWaitStatus(),
            runtime: new MovesRuntimeOptions(PollIntervalSec: 0, FreezeWaitSec: 0, CutoverTimeoutSec: 0));
        MoveRig.CutoverLayer(rig.Sql, caughtUp: false);

        // Act
        var tick = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert
        tick.IsSuccess.Should().BeFalse("the cutover is broken off — the tick Failed");
        tick.Error.Should().NotBeOfType<CutoverPermanentException>(
            "replication is still catching up — transient, tick retries");
        rig.Etcd.Store.Should().ContainKey(MoveNames.MoveKey("shop", "bucket_42"),
            "transient does NOT delete the request");
        var status = await rig.Status.GetAsync("shop", "bucket_42", CancellationToken.None);
        status.Value!.State.Should().Be(MoveStates.Syncing, "the move is alive: state SYNCING");
        status.Value.Phase.Should().Be("catchup-timeout", "the failure phase is recorded by the cutover");
    }

    // AAA: verify-failed (сверка строк не сошлась) — PERMANENT: заявка удалена,
    //      журнал rejected с подсказкой «abort», статус SYNCING/verify-failed жив
    [Fact]
    public async Task M4_VerifyFailed_RequestRejectedWithHint()
    {
        // Arrange — приёмник потерял строку (failover приёмника, P8)
        var rig = await MoveRig.NewAsync(seededStatus: CutoverWaitStatus(), runtime: Fast);
        MoveRig.CutoverLayer(rig.Sql, srcRows: 50, dstRows: 49);

        // Act
        var tick = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert
        tick.IsSuccess.Should().BeFalse("a defective copy cannot be fixed by retries");
        rig.Etcd.Store.Should().NotContainKey(MoveNames.MoveKey("shop", "bucket_42"),
            "a permanent refusal deletes the request");
        var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
        work.Value!.Phase.Should().Be("rejected");
        work.Value.LastError.Should().Contain("abort",
            "operator hint: abort + a new move");
        var status = await rig.Status.GetAsync("shop", "bucket_42", CancellationToken.None);
        status.Value!.State.Should().Be(MoveStates.Syncing, "the key remains: the move lives until abort");
        status.Value.Phase.Should().Be("verify-failed");
    }

    // AAA: flip-conflict (routing изменился под руками) — PERMANENT: заявка
    //      удалена, заморозка ОСТАВЛЕНА (Unfreeze не вызывался, ревью №1)
    [Fact]
    public async Task M4_FlipConflict_RequestRejected_FreezeLeft()
    {
        // Arrange — конкурент уже перевёл routing
        var rig = await MoveRig.NewAsync(seededStatus: CutoverWaitStatus(), runtime: Fast);
        MoveRig.CutoverLayer(rig.Sql);
        rig.Etcd.Seed(MoveNames.RoutingKey("shop", "bucket_42"), "shard9");

        // Act
        var tick = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert
        tick.IsSuccess.Should().BeFalse("compare on routing must not converge");
        rig.Etcd.Store.Should().NotContainKey(MoveNames.MoveKey("shop", "bucket_42"),
            "a permanent refusal deletes the request");
        var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
        work.Value!.Phase.Should().Be("rejected");
        work.Value.LastError.Should().Contain("freeze left in place",
            "hint: the freeze is not released — manual investigation");
        rig.Sql.Calls.Should().NotContain(c => c.Sql.Contains("GRANT INSERT"),
            "the freeze is NOT released: P1-ghosts must not come alive before the investigation");
        rig.Etcd.Store[MoveNames.RoutingKey("shop", "bucket_42")].Value.Should().Be("shard9",
            "the foreign routing value is not overwritten");
    }

    // AAA: сбой DROP SUBSCRIPTION на M5 НЕ отменяет flip — last_error в журнал,
    //      обратная подписка НЕ ставится (прямая жива — петля), Done
    [Fact]
    public async Task M5_DropSubFails_StillDoneWithError()
    {
        // Arrange — источник прямой подписки недоступен (DROP не проходит)
        var rig = await MoveRig.NewAsync(seededStatus: CutoverWaitStatus(), runtime: Fast);
        MoveRig.CutoverLayer(rig.Sql);
        rig.Sql.ExecuteResult = _ => Result.Failed(new ApplicationException("subscription source unavailable"));

        // Act
        var tick = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert
        tick.Value.Should().Be(ProcessOutcome.Done, "flip happened — the move is finished, auto-finalize is queued");
        var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
        work.Value!.Phase.Should().Be("done");
        work.Value.LastError.Should().Contain("finalize",
            "hint: finalize will clean the direct subscription leftovers");
        rig.Sql.Calls.Should().NotContain(c => c.Sql.Contains("CREATE PUBLICATION pub_bucket_42_rb")
            || c.Sql.Contains("CREATE SUBSCRIPTION sub_bucket_42_rb"),
            "the reverse subscription is NOT created while the direct one is not cut (anti-loop)");
    }

    // AAA: skip_reverse — обратной подписки нет (откат только полным re-copy)
    [Fact]
    public async Task M5_SkipReverse_NoReverseArtifacts()
    {
        // Arrange
        var rig = await MoveRig.NewAsync(
            seededStatus: CutoverWaitStatus(), runtime: Fast,
            requestJson: """{"op":"move","to":"shard2","skip_reverse":true,"requested_unix":100}""");
        MoveRig.CutoverLayer(rig.Sql);

        // Act
        var tick = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert
        tick.Value.Should().Be(ProcessOutcome.Done);
        rig.Sql.Calls.Should().Contain(c => c.Sql == "DROP SUBSCRIPTION sub_bucket_42",
            "the direct subscription is cut regardless of skip_reverse");
        rig.Sql.Calls.Should().NotContain(c => c.Sql.Contains("CREATE PUBLICATION pub_bucket_42_rb")
            || c.Sql.Contains("CREATE SUBSCRIPTION sub_bucket_42_rb"),
            "skip_reverse: pub_rb/sub_rb are not created");
    }

    // AAA: resume из FROZEN/flip (смерть инстанса посреди cutover) — повтор
    //      cutover с начала безопасен: freeze идемпотентен, flip проходит
    [Fact]
    public async Task M4_ResumeFromFrozen_RepeatsCutoverSafely()
    {
        // Arrange — прошлый тик умер перед flip: FROZEN/flip, routing ещё старый
        var rig = await MoveRig.NewAsync(seededStatus: new MoveStatus(
            "bucket_42", MoveStates.Frozen, "shard1", "shard2", 111, 122, "flip"), runtime: Fast);
        MoveRig.CutoverLayer(rig.Sql);

        // Act
        var tick = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert
        tick.Value.Should().Be(ProcessOutcome.Done, "the cutover retry finished the move, auto-finalize is queued");
        rig.Sql.Calls.Should().Contain(c => c.Dsn == MoveRig.SrcDsn && c.Sql.Contains("REVOKE INSERT"),
            "the freeze was idempotently repeated");
        rig.Etcd.Store[MoveNames.RoutingKey("shop", "bucket_42")].Value.Should().Be("shard2",
            "flip switched routing");
        rig.Etcd.Store.Should().NotContainKey(MoveNames.StatusKey("shop", "bucket_42"),
            "the status key was deleted by flip");
    }
}
