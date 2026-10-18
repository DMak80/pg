using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Moves;
using PgWorker.Provisioning.Processes;
using Xunit;

namespace PgWorker.UnitTests.Moves;

// MoveProcess M0 (t01 задача 11, spec §6.1 M0): выбор старейшей заявки, гвард
// клэйма, валидация заявки/статус-ключа, SQL-префлайт источника/приёмника с
// классификацией permanent (del заявки + журнал rejected) / transient (заявка
// жива), пробы mover-роли по mover-DSN (ревью №2), SYNCING + обязательный
// стартовый снапшот (ревью №5 — схема на источнике).
public class MoveProcessPreflightTests
{
    // AAA: заявок нет — тик процесса завершён (циклу нечего делать)
    [Fact]
    public async Task NoRequests_Done()
    {
        // Arrange
        var rig = await MoveRig.NewAsync(seedRequest: false);

        // Act
        var tick = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert
        tick.Value.Should().Be(ProcessOutcome.Done, "empty request prefix — nothing to do");
    }

    // AAA: клэйм кластера не наш — мутации запрещены (инвариант spec §4.3)
    [Fact]
    public async Task ClaimNotMine_RefusesMutation()
    {
        // Arrange
        var rig = await MoveRig.NewAsync(claim: false);

        // Act
        var tick = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert
        tick.IsSuccess.Should().BeFalse("mutations are forbidden without a claim");
        tick.Error!.Message.Should().Contain("claim", "the refusal must be understandable to an operator");
        rig.Etcd.Store.Should().ContainKey(MoveNames.MoveKey("shop", "bucket_42"), "request untouched");
        rig.Etcd.Store.Should().NotContainKey(MoveNames.StatusKey("shop", "bucket_42"), "status not created");
        rig.Etcd.Store.Should().NotContainKey("/pgworker/work/shop", "journal not written");
        rig.SnapshotCalls.Should().BeEmpty("no snapshot taken");
    }

    // AAA: зелёный префлайт — SYNCING/ddl + обязательный стартовый снапшот (P12)
    [Fact]
    public async Task Move_BasicPreflightOk_PutsSyncingAndTakesSnapshot()
    {
        // Arrange
        var rig = await MoveRig.NewAsync();

        // Act
        var tick = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert
        tick.Value.Should().Be(ProcessOutcome.InProgress, "the move started, continues in ticks");
        rig.SnapshotCalls.Should().HaveCount(1, "the start snapshot move-<bucket>-start is mandatory (P12)");
        var status = await rig.Status.GetAsync("shop", "bucket_42", CancellationToken.None);
        status.Value!.State.Should().Be(MoveStates.Syncing, "bucket is being moved");
        status.Value.Phase.Should().Be("ddl", "the first phase after preflight is DDL transfer");
        status.Value.Owner.Should().Be("shard1");
        status.Value.Target.Should().Be("shard2");
        status.Value.StartedUnix.Should().BeGreaterThan(0, "start time is recorded");
    }

    // AAA: цель = текущий владелец — перманентный отказ, заявка удаляется
    [Fact]
    public async Task Move_WrongTargetPermanent_RejectsAndDeletesRequest()
    {
        // Arrange
        var rig = await MoveRig.NewAsync(
            requestJson: """{"op":"move","to":"shard1","requested_unix":100}""");

        // Act
        var tick = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert
        tick.IsSuccess.Should().BeFalse("a move to itself is a ticket error");
        tick.Error!.Message.Should().Contain("shard1", "the refusal reason names the target");
        rig.Etcd.Store.Should().NotContainKey(MoveNames.MoveKey("shop", "bucket_42"),
            "a permanent refusal deletes the request (spec §4.1)");
        var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
        work.Value!.Phase.Should().Be("rejected", "refusal recorded in the work journal");
        work.Value.LastError.Should().Contain("shard1");
    }

    // AAA: ABORTING в статус-ключе — сначала заверши abort (перманентный отказ)
    [Fact]
    public async Task Move_AbortingStatus_Rejects()
    {
        // Arrange
        var rig = await MoveRig.NewAsync(seededStatus: new MoveStatus(
            "bucket_42", MoveStates.Aborting, "shard1", "shard2", 111, 122, "db-cleanup"));

        // Act
        var tick = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert
        tick.IsSuccess.Should().BeFalse("a move does not start on top of an unfinished abort");
        tick.Error!.Message.Should().Contain("abort", "operator hint — finish the cleanup");
        rig.Etcd.Store.Should().NotContainKey(MoveNames.MoveKey("shop", "bucket_42"), "request deleted");
    }

    // AAA: уже идёт переезд в ДРУГУЮ цель — перманентный отказ (один переезд на бакет)
    [Fact]
    public async Task Move_OtherTargetInProgress_Rejects()
    {
        // Arrange
        var rig = await MoveRig.NewAsync(seededStatus: new MoveStatus(
            "bucket_42", MoveStates.Syncing, "shard1", "shard3", 111, 122, "copy-wait"));

        // Act
        var tick = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert
        tick.IsSuccess.Should().BeFalse("the target of an active move is not changed by a ticket");
        tick.Error!.Message.Should().Contain("shard3", "the reason names the target of the active move");
        rig.Etcd.Store.Should().NotContainKey(MoveNames.MoveKey("shop", "bucket_42"), "request deleted");
    }

    // AAA: resume того же переезда — started_unix наследуется (возраст переезда сохраняется)
    [Fact]
    public async Task Move_ResumeSameTarget_ContinuesAndKeepsStartedUnix()
    {
        // Arrange
        var rig = await MoveRig.NewAsync(seededStatus: new MoveStatus(
            "bucket_42", MoveStates.Syncing, "shard1", "shard2", 111, 122, "ddl"));

        // Act
        var tick = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert
        tick.Value.Should().Be(ProcessOutcome.InProgress, "the move continues");
        var status = await rig.Status.GetAsync("shop", "bucket_42", CancellationToken.None);
        status.Value!.State.Should().Be(MoveStates.Syncing);
        status.Value.Target.Should().Be("shard2");
        status.Value.StartedUnix.Should().Be(111, "started_unix is inherited on resume");
    }

    // AAA: wal_level ≠ logical на источнике — факт-несоответствие, перманентный отказ
    [Fact]
    public async Task Move_WalLevelNotLogical_RejectsPermanent()
    {
        // Arrange
        var rig = await MoveRig.NewAsync(new MoveRig.PreflightSql(WalLevel: "replica"));

        // Act
        var tick = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert
        tick.IsSuccess.Should().BeFalse("without logical replication a move is impossible");
        tick.Error!.Message.Should().Contain("wal_level", "the reason names the setting");
        rig.Etcd.Store.Should().NotContainKey(MoveNames.MoveKey("shop", "bucket_42"), "request deleted");
        var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
        work.Value!.Phase.Should().Be("rejected");
    }

    // AAA: у мастера приёмника нет живого sync-standby — remote_apply вырожден (P8)
    [Fact]
    public async Task Move_NoSyncStandby_RejectsPermanent()
    {
        // Arrange — synchronous_standby_names пуст
        var rig = await MoveRig.NewAsync(new MoveRig.PreflightSql(SyncStandbyNames: ""));

        // Act
        var names = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert
        names.IsSuccess.Should().BeFalse("empty sync names — remote_apply degenerates");
        names.Error!.Message.Should().Contain("synchronous_standby_names");

        // Arrange — имена есть, но sync/quorum-реплик нет
        var rig2 = await MoveRig.NewAsync(new MoveRig.PreflightSql(SyncStandbyCount: 0));

        // Act
        var count = await rig2.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert
        count.IsSuccess.Should().BeFalse("no live sync-standby — P8 is not satisfied");
        rig2.Etcd.Store.Should().NotContainKey(MoveNames.MoveKey("shop", "bucket_42"), "request deleted");
    }

    // AAA: источник недоступен — transient: заявка живёт, тики повторят
    [Fact]
    public async Task Move_ShardUnreachable_TransientKeepsRequest()
    {
        // Arrange
        var rig = await MoveRig.NewAsync(new MoveRig.PreflightSql(SrcAdminDown: true));

        // Act
        var tick = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert
        tick.IsSuccess.Should().BeFalse("unreachability is not a ticket refusal but a tick failure");
        rig.Etcd.Store.Should().ContainKey(MoveNames.MoveKey("shop", "bucket_42"),
            "a transient failure does not delete the request");
        rig.Etcd.Store.Should().NotContainKey(MoveNames.StatusKey("shop", "bucket_42"),
            "move not started — no status");
        var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
        work.Value!.LastError.Should().NotBeNullOrEmpty("failure recorded in work.last_error");
    }

    // AAA: стартовый снапшот не удался — переезд не начинается, тик повторяет пробу
    [Fact]
    public async Task Move_SnapshotFails_WaitsWithoutPhases()
    {
        // Arrange — первая проба снапшота падает, вторая succeeds
        var rig = await MoveRig.NewAsync(
            snapshotResults: Result.Failed(new ApplicationException("snapshot failed")));

        // Act
        var first = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert
        first.IsSuccess.Should().BeFalse("without a start snapshot the move does not begin (P12)");
        var status = await rig.Status.GetAsync("shop", "bucket_42", CancellationToken.None);
        status.Value!.State.Should().Be(MoveStates.Syncing, "status key remained SYNCING");
        status.Value.Phase.Should().Be("waiting-snapshot", "the phase waits for the snapshot point");

        // Act — повторный тик снова пробует снапшот (теперь успешно)
        var second = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert
        second.Value.Should().Be(ProcessOutcome.InProgress, "after the snapshot the move began");
        rig.SnapshotCalls.Should().HaveCount(2, "a repeated tick tries the snapshot again");
        var after = await rig.Status.GetAsync("shop", "bucket_42", CancellationToken.None);
        after.Value!.Phase.Should().Be("ddl", "the phase advanced past the snapshot point");
    }

    // AAA: схема на приёмнике без подписки — только resume и только ПУСТАЯ схема
    [Fact]
    public async Task Move_NonEmptySchemaWithoutResume_Rejects()
    {
        // Arrange — схема-остаток на приёмнике, заявка без resume
        var rig = await MoveRig.NewAsync(new MoveRig.PreflightSql(SchemaOnDst: true));

        // Act
        var noResume = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert
        noResume.IsSuccess.Should().BeFalse("a leftover of a broken run — do not continue silently");
        noResume.Error!.Message.Should().Contain("resume", "hint — resume or DROP SCHEMA");

        // Arrange — resume=true, но схема НЕ пустая (остатки данных)
        var rig2 = await MoveRig.NewAsync(new MoveRig.PreflightSql(
            SchemaOnDst: true,
            EmptySchemaGen: """SELECT (SELECT count(*) FROM bucket_42."items")""",
            EmptySchemaRows: 7),
            requestJson: """{"op":"move","to":"shard2","resume":true,"requested_unix":100}""");

        // Act
        var nonEmpty = await rig2.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert
        nonEmpty.IsSuccess.Should().BeFalse("copy_data=true into a non-empty schema will produce duplicates");
        nonEmpty.Error!.Message.Should().Contain("not empty");
        rig2.Etcd.Store.Should().NotContainKey(MoveNames.MoveKey("shop", "bucket_42"),
            "a permanent refusal deletes the request");
    }

    // AAA: свой статус SYNCING/ddl + схема на приёмнике (M1 успел, M2 упал transient) —
    // НЕ перманентный отказ: resume собственного переезда неявное, пустота схемы проверяется
    [Fact]
    public async Task Move_OwnStatus_SchemaOnDst_ImplicitResume_Proceeds()
    {
        // Arrange — статус нашего переезда (shard1→shard2), DDL уже применён на приёмник
        var rig = await MoveRig.NewAsync(
            new MoveRig.PreflightSql(
                SchemaOnDst: true,
                EmptySchemaGen: """SELECT (SELECT count(*) FROM bucket_42."items")""",
                EmptySchemaRows: 0),
            seededStatus: new MoveStatus(
                "bucket_42", MoveStates.Syncing, "shard1", "shard2", 111, 122, "ddl"));

        // Act — заявка БЕЗ resume (её писал пользователь до первого тика)
        var tick = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert
        tick.Value.Should().Be(ProcessOutcome.InProgress,
            "the schema on the target is a product of our M1: a transient M2 failure must not kill the move");
        rig.Etcd.Store.Should().ContainKey(MoveNames.MoveKey("shop", "bucket_42"), "the request is alive");
        rig.Sql.Calls.Should().Contain(c => c.Sql.Contains("(SELECT count(*)"),
            "target schema emptiness was checked before continuing");
    }

    // AAA: свой статус + НЕ пустая схема на приёмнике (данные прошлого запуска) —
    // перманентный отказ с подсказкой abort
    [Fact]
    public async Task Move_OwnStatus_NonEmptySchemaOnDst_RejectsWithAbortHint()
    {
        // Arrange
        var rig = await MoveRig.NewAsync(
            new MoveRig.PreflightSql(
                SchemaOnDst: true,
                EmptySchemaGen: """SELECT (SELECT count(*) FROM bucket_42."items")""",
                EmptySchemaRows: 9),
            seededStatus: new MoveStatus(
                "bucket_42", MoveStates.Syncing, "shard1", "shard2", 111, 122, "ddl"));

        // Act
        var tick = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert
        tick.IsSuccess.Should().BeFalse("copy_data=true into a non-empty schema will produce duplicates");
        tick.Error!.Message.Should().Contain("abort", "hint — abort and a new move");
        rig.Etcd.Store.Should().NotContainKey(MoveNames.MoveKey("shop", "bucket_42"), "request deleted");
    }

    // AAA: схемы бакета нет на источнике — перманентный отказ (ревью №5)
    [Fact]
    public async Task Move_SchemaMissingOnSource_RejectsPermanent()
    {
        // Arrange
        var rig = await MoveRig.NewAsync(new MoveRig.PreflightSql(SchemaOnSource: false));

        // Act
        var tick = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert
        tick.IsSuccess.Should().BeFalse("nothing to transfer — the source has no bucket schema");
        tick.Error!.Message.Should().Contain("shard1", "the reason names the owner");
        rig.Etcd.Store.Should().NotContainKey(MoveNames.MoveKey("shop", "bucket_42"), "request deleted");
        var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
        work.Value!.Phase.Should().Be("rejected", "refusal in the journal");
    }

    // AAA: пробы mover-роли идут по mover-DSN; роль без REPLICATION — перманентный отказ (ревью №2)
    [Fact]
    public async Task Move_MoverRoleProbeUsesMoverDsn_RejectsWithoutReplication()
    {
        // Arrange — SELECT 1 по mover-DSN ок, но роль без REPLICATION
        var rig = await MoveRig.NewAsync(new MoveRig.PreflightSql(MoverRoleOk: false));

        // Act
        var tick = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert
        tick.IsSuccess.Should().BeFalse("a mover without REPLICATION will not be able to subscribe");
        tick.Error!.Message.Should().Contain("REPLICATION", "the reason names the role attribute");
        var probes = rig.Sql.Calls.Where(c => c.Sql is "SELECT 1" || c.Sql.Contains("rolsuper")).ToList();
        probes.Should().HaveCountGreaterThanOrEqualTo(2, "two probes: reachability + role attribute");
        probes.Should().OnlyContain(c => c.Dsn == MoveRig.MoverDsn,
            "both probes must go over the bucket_mover role DSN, not the admin DSN");
        rig.Etcd.Store.Should().NotContainKey(MoveNames.MoveKey("shop", "bucket_42"), "request deleted");
    }

    // ---------- Отказы M0 t06 (spec §5.5) ----------

    // Снапшот с параметризованными шардами (bucket_42 у shard1 — как в MoveRig.Snap).
    private static ClusterSnapshot SnapWith(params ShardSpec[] shards) => new(
        new ClusterConfig("shop", 6, "shop", 1755900000, ClusterState.Active),
        shards,
        [new BucketRoute(42, "shard1", null)]);

    private static ShardSpec LiveShard(string name, string dsn) => new(
        name, 2, dsn, name + "a:18000",
        [new NodeSpec(name, name + "a", NodeState.Running), new NodeSpec(name, name + "b", NodeState.Running)]);

    // AAA: цель переезда помечена TO_REMOVE — перманентный отказ с подсказкой (t06 §5.5)
    [Fact]
    public async Task Move_ToMarkedShard_RejectedPermanently()
    {
        // Arrange — цель shard2 поднята (dsn есть), но помечена к демонтажу
        var rig = await MoveRig.NewAsync();
        var snap = SnapWith(
            LiveShard("shard1", "host=h1,h2 port=15000,15001 dbname=shop user=bucket_admin"),
            LiveShard("shard2", MoveRig.DstDsnKey) with { ToRemove = true });

        // Act
        var tick = await rig.Process.TickAsync(snap, CancellationToken.None);

        // Assert — Reject: заявка удалена, SQL не звался, подсказка оператору
        tick.IsSuccess.Should().BeFalse();
        tick.Error!.Message.Should().Contain("marked for removal");
        rig.Etcd.Store.Should().NotContainKey(MoveNames.MoveKey("shop", "bucket_42"));
        var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
        work.Value!.Phase.Should().Be("rejected");
        work.Value.LastError.Should().Contain("marked for removal");
        rig.Sql.Calls.Should().BeEmpty("refusal before any SQL probes");
    }

    // AAA: цель без dsn (add-shard не завершён) — уточнённый отказ с подсказкой
    [Fact]
    public async Task Move_ToShardWithoutDsn_RejectedWithAddHint()
    {
        // Arrange — цель shard2 declared (ноды есть), dsn НЕТ, маркера нет
        var rig = await MoveRig.NewAsync();
        var declared = new ShardSpec("shard2", 2, null, null,
        [
            new NodeSpec("shard2", "shard2a", NodeState.NotInitialized),
            new NodeSpec("shard2", "shard2b", NodeState.NotInitialized),
        ]);
        var snap = SnapWith(
            LiveShard("shard1", "host=h1,h2 port=15000,15001 dbname=shop user=bucket_admin"),
            declared);

        // Act
        var tick = await rig.Process.TickAsync(snap, CancellationToken.None);

        // Assert — заявка удалена; причина называет незавершённый add-shard
        tick.IsSuccess.Should().BeFalse();
        tick.Error!.Message.Should().Contain("not up yet (add-shard not finished)");
        rig.Etcd.Store.Should().NotContainKey(MoveNames.MoveKey("shop", "bucket_42"));
    }

    // AAA: finalize удалённого шарда (нет dsn) — убирать нечего, артефакты исчезли
    [Fact]
    public async Task Finalize_OldShardRemoved_RejectedNothingToClean()
    {
        // Arrange — заявка finalize с old_shard=shard2; шарда уже нет (dsn-ключа нет)
        var rig = await MoveRig.NewAsync(
            requestJson: """{"op":"finalize","old_shard":"shard2","requested_unix":100}""");
        var snap = SnapWith(LiveShard("shard1", "host=h1,h2 port=15000,15001 dbname=shop user=bucket_admin"));

        // Act
        var tick = await rig.Process.TickAsync(snap, CancellationToken.None);

        // Assert — перманентный отказ с подсказкой; заявка удалена
        tick.IsSuccess.Should().BeFalse();
        tick.Error!.Message.Should().Contain("removed — nothing to clean up");
        rig.Etcd.Store.Should().NotContainKey(MoveNames.MoveKey("shop", "bucket_42"));
        rig.Sql.Calls.Should().BeEmpty("no SQL cleanup happened — no artifacts");
    }
}
