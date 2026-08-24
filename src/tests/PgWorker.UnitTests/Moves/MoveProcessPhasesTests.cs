using PgWorker.Core;
using PgWorker.Moves;
using PgWorker.Provisioning.Processes;
using Xunit;

namespace PgWorker.UnitTests.Moves;

// MoveProcess M1–M3 (t01 задача 13, spec §6.1): DDL-перенос (pg_dump через exec,
// применение, гранты, сверка инвентаря P5), pub/sub идемпотентно (copy_data=true,
// failover-флаг конфигурируем, remote_apply), copy-wait с перезаписью статус-ключа
// КАЖДЫЙ тик (updated_unix — фундамент защиты abort, ревью №4) и логом готовности
// с лагом слота (ревью №7). Сид — статус SYNCING/ddl (M0 пройден, снапшот взят).
public class MoveProcessPhasesTests
{
    private static MoveStatus DdlStatus() => new(
        "bucket_42", MoveStates.Syncing, "shard1", "shard2", 111, 122, MovePhases.Ddl);

    // AAA: схемы на приёмнике нет — pg_dump из мастер-контейнера источника, применение,
    //      гранты app-роли; тик доезжает до copy-wait (M2-резолверы зелёные)
    [Fact]
    public async Task M1_SchemaMissing_DumpsAppliesGrants()
    {
        // Arrange — default: схема на приёмнике нет, подписки нет, инвентарь пуст
        var rig = await MoveRig.NewAsync(
            new MoveRig.PreflightSql(SubSyncReady: "1/3"), seededStatus: DdlStatus());

        // Act
        var tick = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert
        tick.Value.Should().Be(ProcessOutcome.InProgress, "после M3 copy ещё идёт");
        rig.Driver.Executed.Should().ContainSingle().Which.Cmd.Should().BeEquivalentTo(
            ["su", "postgres", "-c", "pg_dump --schema-only --no-owner --no-privileges --schema=bucket_42 shop"],
            "DDL берётся pg_dump'ом из мастер-контейнера источника");
        rig.Driver.Executed[0].Node.Should().Be("shard1/shard1a", "exec идёт в мастер-ноду источника");
        rig.Sql.Calls.Should().Contain(c => c.Dsn == MoveRig.DstDsn && c.Sql == "-- ddl",
            "DDL применён батчем на приёмнике");
        rig.Sql.Calls.Should().Contain(c => c.Dsn == MoveRig.DstDsn && c.Sql.Contains("GRANT USAGE ON SCHEMA bucket_42 TO app"),
            "базовые гранты app-роли выданы на приёмнике");
        var status = await rig.Status.GetAsync("shop", "bucket_42", CancellationToken.None);
        status.Value!.Phase.Should().Be(MovePhases.CopyWait, "тик доехал до ожидания initial copy");
        status.Value.StartedUnix.Should().Be(111, "started_unix наследуется сквозь фазы");
    }

    // AAA: инвентарь источник/приёмник расходится — мораторий DDL (P5) нарушен: permanent
    [Fact]
    public async Task M1_InventoryMismatch_RejectsPermanent()
    {
        // Arrange — схема и подписка на приёмнике есть (dump skip), инвентарь неполный
        var rig = await MoveRig.NewAsync(new MoveRig.PreflightSql(
            SchemaOnDst: true, SubOnDstCount: 1,
            InventorySrc: ["r|items"],
            InventoryDst: ["r|items", "S|seq1"]), seededStatus: DdlStatus());

        // Act
        var tick = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert
        tick.IsSuccess.Should().BeFalse("дефектную копию нельзя доверять cutover'у");
        tick.Error!.Message.Should().Contain("inventory-mismatch", "причина — расходление инвентаря P5");
        rig.Etcd.Store.Should().NotContainKey(MoveNames.MoveKey("shop", "bucket_42"),
            "перманентный отказ удаляет заявку");
        var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
        work.Value!.Phase.Should().Be("rejected");
        work.Value.LastError.Should().Contain("inventory-mismatch");
    }

    // AAA: pub/sub отсутствуют — создаются: pub на источнике, sub на приёмнике с
    //      mover-conninfo, copy_data=true, remote_apply; failover=true только при
    //      конфиге (false опускает опцию — PG16 не знает параметра, e2e-факт t01)
    [Theory]
    [InlineData(true, "failover = true, ")]
    [InlineData(false, "")]
    public async Task M2_PubMissing_Created_SubMissing_CreatedWithFailoverOption(
        bool failover, string failoverOption)
    {
        // Arrange — схемы/подписки на приёмнике нет (полный путь M1→M2), pub нет
        var rig = await MoveRig.NewAsync(
            new MoveRig.PreflightSql(SubSyncReady: "1/3"),
            seededStatus: DdlStatus(), failoverSlots: failover);

        // Act
        var tick = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert
        tick.Value.Should().Be(ProcessOutcome.InProgress);
        rig.Sql.Calls.Should().Contain(c => c.Dsn == MoveRig.SrcDsn
            && c.Sql == "CREATE PUBLICATION pub_bucket_42 FOR TABLES IN SCHEMA bucket_42",
            "публикация создаётся на источнике");
        rig.Sql.Calls.Should().Contain(c => c.Dsn == MoveRig.DstDsn && c.Sql ==
            "CREATE SUBSCRIPTION sub_bucket_42 CONNECTION 'host=h1,h2 port=15000,15001 dbname=shop user=bucket_mover password=mov-pw' PUBLICATION pub_bucket_42 " +
            $"WITH (copy_data = true, {failoverOption}synchronous_commit = remote_apply)",
            "подписка на приёмнике: mover-conninfo источника, remote_apply и конфигурируемый failover");
    }

    // AAA: подписка уже есть (resume) — CREATE SUBSCRIPTION не выполняется
    [Fact]
    public async Task M2_Resume_SubExists_SkipsCreate()
    {
        // Arrange
        var rig = await MoveRig.NewAsync(new MoveRig.PreflightSql(
            SchemaOnDst: true, SubOnDstCount: 1, SubSyncReady: "1/3"), seededStatus: DdlStatus());

        // Act
        var tick = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert
        tick.Value.Should().Be(ProcessOutcome.InProgress);
        rig.Sql.Calls.Should().NotContain(c => c.Sql.Contains("CREATE SUBSCRIPTION sub_bucket_42"),
            "подписка существует — создание идемпотентно пропущено");
        var status = await rig.Status.GetAsync("shop", "bucket_42", CancellationToken.None);
        status.Value!.Phase.Should().Be(MovePhases.CopyWait);
    }

    // AAA: подписка не готова («1/3») — тик ждёт: InProgress, статус SYNCING/copy-wait
    [Fact]
    public async Task M3_NotReady_InProgress()
    {
        // Arrange
        var rig = await MoveRig.NewAsync(new MoveRig.PreflightSql(
            SchemaOnDst: true, SubOnDstCount: 1, SubSyncReady: "1/3"), seededStatus: DdlStatus());

        // Act
        var tick = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert
        tick.Value.Should().Be(ProcessOutcome.InProgress, "initial copy идёт — ждём тиками");
        var status = await rig.Status.GetAsync("shop", "bucket_42", CancellationToken.None);
        status.Value!.State.Should().Be(MoveStates.Syncing);
        status.Value.Phase.Should().Be(MovePhases.CopyWait);
    }

    // AAA: подписка готова («3/3») — фаза cutover-wait (M4 продолжит следующим тиком)
    [Fact]
    public async Task M3_Ready_SetsCutoverWaitPhase()
    {
        // Arrange
        var rig = await MoveRig.NewAsync(new MoveRig.PreflightSql(
            SchemaOnDst: true, SubOnDstCount: 1, SubSyncReady: "3/3"), seededStatus: DdlStatus());

        // Act
        var tick = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert
        tick.Value.Should().Be(ProcessOutcome.InProgress);
        var status = await rig.Status.GetAsync("shop", "bucket_42", CancellationToken.None);
        status.Value!.Phase.Should().Be(MovePhases.CutoverWait,
            "initial copy завершён — ждём cutover-тик");
    }

    // AAA: каждый тик copy-wait переписывает статус-ключ с обновлённым updated_unix
    //      (ревью №4: защита abort по updated_unix) — state/phase неизменны
    [Fact]
    public async Task M3_EachTickRewritesStatus_UpdatedUnixAdvances()
    {
        // Arrange — детерминированные часы: тик 1 в T0, тик 2 в T0+1s
        var clock = new StepClock();
        var rig = await MoveRig.NewAsync(new MoveRig.PreflightSql(
            SchemaOnDst: true, SubOnDstCount: 1, SubSyncReady: "1/3"),
            seededStatus: DdlStatus(), clock: clock);

        // Act
        var first = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);
        var afterFirst = await rig.Status.GetAsync("shop", "bucket_42", CancellationToken.None);
        clock.Advance(TimeSpan.FromSeconds(1));
        var second = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);
        var afterSecond = await rig.Status.GetAsync("shop", "bucket_42", CancellationToken.None);

        // Assert
        first.Value.Should().Be(ProcessOutcome.InProgress);
        second.Value.Should().Be(ProcessOutcome.InProgress);
        afterSecond.Value!.UpdatedUnix.Should().BeGreaterThan(afterFirst.Value!.UpdatedUnix,
            "updated_unix обязан двигаться каждым тиком — по нему abort отличает живой mover");
        afterSecond.Value.State.Should().Be(afterFirst.Value!.State);
        afterSecond.Value.Phase.Should().Be(afterFirst.Value.Phase, "фаза не менялась — всё ещё copy");
        afterSecond.Value.StartedUnix.Should().Be(111, "старт переезда не сдвигается");
    }

    // Сдвигаемые часы для детерминированного updated_unix (TimeProvider-хук процесса).
    private sealed class StepClock : TimeProvider
    {
        private DateTimeOffset _now = DateTimeOffset.FromUnixTimeSeconds(1770000000);

        public void Advance(TimeSpan delta) => _now += delta;

        public override DateTimeOffset GetUtcNow() => _now;
    }
}
