using PgWorker.Core;
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
        tick.Value.Should().Be(ProcessOutcome.Done, "пустой префикс заявок — работы нет");
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
        tick.IsSuccess.Should().BeFalse("без клэйма мутации запрещены");
        tick.Error!.Message.Should().Contain("клэйм", "отказ обязан быть понятным оператору");
        rig.Etcd.Store.Should().ContainKey(MoveNames.MoveKey("shop", "bucket_42"), "заявка не тронута");
        rig.Etcd.Store.Should().NotContainKey(MoveNames.StatusKey("shop", "bucket_42"), "статус не создан");
        rig.Etcd.Store.Should().NotContainKey("/pgworker/work/shop", "журнал не писался");
        rig.SnapshotCalls.Should().BeEmpty("снапшот не брался");
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
        tick.Value.Should().Be(ProcessOutcome.InProgress, "переезд начался, продолжится тиками");
        rig.SnapshotCalls.Should().HaveCount(1, "стартовый снапшот move-<bucket>-start обязателен (P12)");
        var status = await rig.Status.GetAsync("shop", "bucket_42", CancellationToken.None);
        status.Value!.State.Should().Be(MoveStates.Syncing, "бакет в переезде");
        status.Value.Phase.Should().Be("ddl", "первая фаза после префлайта — перенос DDL");
        status.Value.Owner.Should().Be("shard1");
        status.Value.Target.Should().Be("shard2");
        status.Value.StartedUnix.Should().BeGreaterThan(0, "стартовое время зафиксировано");
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
        tick.IsSuccess.Should().BeFalse("переезд в себя — ошибка заявки");
        tick.Error!.Message.Should().Contain("shard1", "причина отказа называет цель");
        rig.Etcd.Store.Should().NotContainKey(MoveNames.MoveKey("shop", "bucket_42"),
            "перманентный отказ удаляет заявку (spec §4.1)");
        var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
        work.Value!.Phase.Should().Be("rejected", "отказ зафиксирован в work-журнале");
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
        tick.IsSuccess.Should().BeFalse("move не стартует поверх незавершённого abort");
        tick.Error!.Message.Should().Contain("abort", "подсказка оператору — завершить уборку");
        rig.Etcd.Store.Should().NotContainKey(MoveNames.MoveKey("shop", "bucket_42"), "заявка удалена");
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
        tick.IsSuccess.Should().BeFalse("цель действующего переезда не меняется заявкой");
        tick.Error!.Message.Should().Contain("shard3", "причина называет цель идущего переезда");
        rig.Etcd.Store.Should().NotContainKey(MoveNames.MoveKey("shop", "bucket_42"), "заявка удалена");
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
        tick.Value.Should().Be(ProcessOutcome.InProgress, "переезд продолжается");
        var status = await rig.Status.GetAsync("shop", "bucket_42", CancellationToken.None);
        status.Value!.State.Should().Be(MoveStates.Syncing);
        status.Value.Target.Should().Be("shard2");
        status.Value.StartedUnix.Should().Be(111, "started_unix наследуется при продолжении");
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
        tick.IsSuccess.Should().BeFalse("без logical-репликации переезд невозможен");
        tick.Error!.Message.Should().Contain("wal_level", "причина называет настройку");
        rig.Etcd.Store.Should().NotContainKey(MoveNames.MoveKey("shop", "bucket_42"), "заявка удалена");
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
        names.IsSuccess.Should().BeFalse("пустые sync-имена — remote_apply вырождается");
        names.Error!.Message.Should().Contain("synchronous_standby_names");

        // Arrange — имена есть, но sync/quorum-реплик нет
        var rig2 = await MoveRig.NewAsync(new MoveRig.PreflightSql(SyncStandbyCount: 0));

        // Act
        var count = await rig2.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert
        count.IsSuccess.Should().BeFalse("нет живого sync-standby — P8 не выполняется");
        rig2.Etcd.Store.Should().NotContainKey(MoveNames.MoveKey("shop", "bucket_42"), "заявка удалена");
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
        tick.IsSuccess.Should().BeFalse("недоступность — не отказ заявки, а сбой тика");
        rig.Etcd.Store.Should().ContainKey(MoveNames.MoveKey("shop", "bucket_42"),
            "transient-сбой заявку не удаляет");
        rig.Etcd.Store.Should().NotContainKey(MoveNames.StatusKey("shop", "bucket_42"),
            "переезд не начат — статуса нет");
        var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
        work.Value!.LastError.Should().NotBeNullOrEmpty("сбой зафиксирован в work.last_error");
    }

    // AAA: стартовый снапшот не удался — переезд не начинается, тик повторяет пробу
    [Fact]
    public async Task Move_SnapshotFails_WaitsWithoutPhases()
    {
        // Arrange — первая проба снапшота падает, вторая succeeds
        var rig = await MoveRig.NewAsync(
            snapshotResults: Result.Failed(new ApplicationException("снапшот не удался")));

        // Act
        var first = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert
        first.IsSuccess.Should().BeFalse("без стартового снапшота переезд не начинается (P12)");
        var status = await rig.Status.GetAsync("shop", "bucket_42", CancellationToken.None);
        status.Value!.State.Should().Be(MoveStates.Syncing, "статус-ключ остался SYNCING");
        status.Value.Phase.Should().Be("waiting-snapshot", "фаза ждёт снапшот-точку");

        // Act — повторный тик снова пробует снапшот (теперь успешно)
        var second = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert
        second.Value.Should().Be(ProcessOutcome.InProgress, "после снапшота переезд начался");
        rig.SnapshotCalls.Should().HaveCount(2, "повтор тика снова пробует снапшот");
        var after = await rig.Status.GetAsync("shop", "bucket_42", CancellationToken.None);
        after.Value!.Phase.Should().Be("ddl", "фаза продвинулась за точку снапшота");
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
        noResume.IsSuccess.Should().BeFalse("остаток сорванного запуска — не молча продолжать");
        noResume.Error!.Message.Should().Contain("resume", "подсказка — resume или DROP SCHEMA");

        // Arrange — resume=true, но схема НЕ пустая (остатки данных)
        var rig2 = await MoveRig.NewAsync(new MoveRig.PreflightSql(
            SchemaOnDst: true,
            EmptySchemaGen: """SELECT (SELECT count(*) FROM bucket_42."items")""",
            EmptySchemaRows: 7),
            requestJson: """{"op":"move","to":"shard2","resume":true,"requested_unix":100}""");

        // Act
        var nonEmpty = await rig2.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert
        nonEmpty.IsSuccess.Should().BeFalse("copy_data=true в непустую схему даст дубликаты");
        nonEmpty.Error!.Message.Should().Contain("не пустая");
        rig2.Etcd.Store.Should().NotContainKey(MoveNames.MoveKey("shop", "bucket_42"),
            "перманентный отказ удаляет заявку");
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
        tick.IsSuccess.Should().BeFalse("нечего переносить — источник без схемы бакета");
        tick.Error!.Message.Should().Contain("shard1", "причина называет владельца");
        rig.Etcd.Store.Should().NotContainKey(MoveNames.MoveKey("shop", "bucket_42"), "заявка удалена");
        var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
        work.Value!.Phase.Should().Be("rejected", "отказ в журнале");
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
        tick.IsSuccess.Should().BeFalse("mover без REPLICATION не сможет подписаться");
        tick.Error!.Message.Should().Contain("REPLICATION", "причина называет атрибут роли");
        var probes = rig.Sql.Calls.Where(c => c.Sql is "SELECT 1" || c.Sql.Contains("rolsuper")).ToList();
        probes.Should().HaveCountGreaterThanOrEqualTo(2, "две пробы: доступность + атрибут роли");
        probes.Should().OnlyContain(c => c.Dsn == MoveRig.MoverDsn,
            "обе пробы обязаны идти по DSN роли bucket_mover, а не по admin-DSN");
        rig.Etcd.Store.Should().NotContainKey(MoveNames.MoveKey("shop", "bucket_42"), "заявка удалена");
    }
}
