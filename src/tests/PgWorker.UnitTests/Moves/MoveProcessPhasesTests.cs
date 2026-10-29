using Microsoft.Extensions.Logging;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Moves;
using PgWorker.Provisioning.Processes;
using Xunit;

namespace PgWorker.UnitTests.Moves;

// MoveProcess M1–M3 (spec §6.1): DDL-перенос (pgdump-команда
// приёмнику источника, применение, гранты, сверка инвентаря P5), pub/sub
// идемпотентно (copy_data=true,
// failover-флаг конфигурируем, remote_apply), copy-wait с перезаписью статус-ключа
// КАЖДЫЙ тик (updated_unix — фундамент защиты abort, ревью №4) и логом готовности
// с лагом слота (ревью №7). Сид — статус SYNCING/ddl (M0 пройден, снапшот взят).
public class MoveProcessPhasesTests
{
    private static MoveStatus DdlStatus() => new(
        "bucket_42", MoveStates.Syncing, "shard1", "shard2", 111, 122, MovePhases.Ddl);

    // AAA: схемы на приёмнике нет — pgdump-команда приёмнику ноды-источника,
    //      применение, гранты app-роли; тик доезжает до copy-wait (M2-резолверы зелёные)
    [Fact]
    public async Task M1_SchemaMissing_DumpsAppliesGrants()
    {
        // Arrange — default: схема на приёмнике нет, подписки нет, инвентарь пуст
        var rig = await MoveRig.NewAsync(
            new MoveRig.PreflightSql(SubSyncReady: "1/3"), seededStatus: DdlStatus());

        // Act
        var tick = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert
        tick.Value.Should().Be(ProcessOutcome.InProgress, "after M3 the copy is still running");
        rig.Receiver.Dumps.Should().ContainSingle();
        rig.Receiver.Dumps[0].Url.Should().Be("http://pgw-shop-shard1-shard1a:8080",
            "the dump is taken by the receiver of the source master node");
        rig.Receiver.Dumps[0].Body.Schema.Should().Be("bucket_42");
        rig.Receiver.Dumps[0].Body.DbName.Should().Be("shop");
        rig.Sql.Calls.Should().Contain(c => c.Dsn == MoveRig.DstDsn && c.Sql == "-- ddl",
            "DDL is applied as a batch on the target");
        rig.Sql.Calls.Should().Contain(c => c.Dsn == MoveRig.DstDsn && c.Sql.Contains("GRANT USAGE ON SCHEMA bucket_42 TO app"),
            "basic app-role grants are issued on the target");
        var status = await rig.Status.GetAsync("shop", "bucket_42", CancellationToken.None);
        status.Value!.Phase.Should().Be(MovePhases.CopyWait, "the tick reached the initial copy wait");
        status.Value.StartedUnix.Should().Be(111, "started_unix is inherited across phases");
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
        tick.IsSuccess.Should().BeFalse("a defective copy cannot be trusted by the cutover");
        tick.Error!.Message.Should().Contain("inventory-mismatch", "the reason is the P5 inventory divergence");
        rig.Etcd.Store.Should().NotContainKey(MoveNames.MoveKey("shop", "bucket_42"),
            "a permanent refusal deletes the request");
        var work = await rig.Journal.ReadAsync("shop", CancellationToken.None);
        work.Value!.Phase.Should().Be("rejected");
        work.Value.LastError.Should().Contain("inventory-mismatch");
    }

    // AAA: pub/sub отсутствуют — создаются: pub на источнике, sub на приёмнике с
    //      mover-conninfo, copy_data=true, remote_apply; failover=true только при
    //      конфиге (false опускает опцию — PG16 не знает параметра, факт стенда)
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
            "the publication is created on the source");
        rig.Sql.Calls.Should().Contain(c => c.Dsn == MoveRig.DstDsn && c.Sql ==
            "CREATE SUBSCRIPTION sub_bucket_42 CONNECTION 'host=h1,h2 port=15000,15001 dbname=shop user=bucket_mover password=mov-pw sslmode=require target_session_attrs=read-write' PUBLICATION pub_bucket_42 " +
            $"WITH (copy_data = true, {failoverOption}synchronous_commit = remote_apply)",
            "the subscription on the target: source mover-conninfo, remote_apply and configurable failover");
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
            "the subscription exists — its creation is idempotently skipped");
        var status = await rig.Status.GetAsync("shop", "bucket_42", CancellationToken.None);
        status.Value!.Phase.Should().Be(MovePhases.CopyWait);
    }

    // AAA (adopt-repair §3.3, advertised-правило): приёмник с object-нодами —
    // внешний исполнитель compose-сети видит адреса dsn-ключа напрямую,
    // подмена host.docker.internal НЕ применяется
    [Fact]
    public async Task M2_AdoptedReceiver_NoAdvertisedSubstitution()
    {
        // Arrange — приёмник shard2 = object-ноды (усыновленный кластер),
        // конфиг содержит advertised-хост (для канонических он бы подменял).
        var rig = await MoveRig.NewAsync(
            new MoveRig.PreflightSql(SubSyncReady: "1/3"), seededStatus: DdlStatus(),
            runtime: new MovesRuntimeOptions(AdvertisedPublisherHost: "host.docker.internal"));
        rig.Etcd.Seed("/pgworker/portalloc/shop", Portalloc.Serialize(new Dictionary<string, NodeAddress>
        {
            ["shard1/shard1a"] = new("h1", new NodePorts(15000, 18000, 16500)),
            ["shard1/shard1b"] = new("h2", new NodePorts(15001, 18001, 16501)),
            ["shard2/shard2a"] = new("h1", new NodePorts(15002, 0, 0), "as-shard2a"),
            ["shard2/shard2b"] = new("h2", new NodePorts(15003, 0, 0), "as-shard2b"),
        }));

        // Act
        var tick = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert: conninfo содержит хосты dsn-ключа источника как есть.
        tick.Value.Should().Be(ProcessOutcome.InProgress);
        rig.Sql.Calls.Should().Contain(c => c.Dsn == MoveRig.DstDsn && c.Sql ==
            "CREATE SUBSCRIPTION sub_bucket_42 CONNECTION 'host=h1,h2 port=15000,15001 dbname=shop user=bucket_mover password=mov-pw sslmode=require target_session_attrs=read-write' PUBLICATION pub_bucket_42 " +
            "WITH (copy_data = true, failover = true, synchronous_commit = remote_apply)",
            "the external target gets the dsn-key addresses without the advertised substitution");
    }

    // AAA (adopt-repair §3.3): канонический приёмник — подмена advertised-хоста
    // применяется как раньше (single-host стенды: подписка из контейнера)
    [Fact]
    public async Task M2_CanonicalReceiver_AdvertisedSubstituted()
    {
        // Arrange — обычная топология (без object), advertised задан конфигом.
        var rig = await MoveRig.NewAsync(
            new MoveRig.PreflightSql(SubSyncReady: "1/3"), seededStatus: DdlStatus(),
            runtime: new MovesRuntimeOptions(AdvertisedPublisherHost: "host.docker.internal"));

        // Act
        var tick = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert: hosts издателя заменены на advertised (поэлементно).
        tick.Value.Should().Be(ProcessOutcome.InProgress);
        rig.Sql.Calls.Should().Contain(c => c.Dsn == MoveRig.DstDsn && c.Sql.Contains(
            "CONNECTION 'host=host.docker.internal,host.docker.internal port=15000,15001"),
            "the canonical target — the publisher advertised substitution works");
    }

    // AAA (adopt-repair §3.3): усыновлённый источник — mover-пробы M0 идут по
    // адресу мастера из portalloc (внутренние имена dsn-ключа из воркера не
    // резолвимы), а не по multi-host dsn-ключу.
    [Fact]
    public async Task M0_AdoptedSource_MoverProbeByMasterAddress()
    {
        // Arrange — источник shard1 = object-ноды, приёмник канонический.
        var rig = await MoveRig.NewAsync(new MoveRig.PreflightSql(SubSyncReady: "1/3"), seededStatus: DdlStatus());
        rig.Etcd.Seed("/pgworker/portalloc/shop", Portalloc.Serialize(new Dictionary<string, NodeAddress>
        {
            ["shard1/shard1a"] = new("h1", new NodePorts(15000, 0, 0), "as-shard1a"),
            ["shard1/shard1b"] = new("h2", new NodePorts(15001, 0, 0), "as-shard1b"),
            ["shard2/shard2a"] = new("h1", new NodePorts(15002, 18002, 16502)),
            ["shard2/shard2b"] = new("h2", new NodePorts(15003, 18003, 16503)),
        }));

        // Act
        var tick = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert: проба SELECT 1 ушла по адресу мастера (Host=h1;Port=15000,
        // bucket_mover), multi-host dsn-ключ не использовался.
        tick.Value.Should().Be(ProcessOutcome.InProgress);
        rig.Sql.Calls.Should().Contain(c => c.Sql == "SELECT 1" && c.Dsn ==
            "Host=h1;Port=15000;Database=shop;Username=bucket_mover;Password=mov-pw;SSL Mode=Require;Trust Server Certificate=true",
            "adopted source: the mover probe goes to the master address from portalloc");
        rig.Sql.Calls.Should().NotContain(c => c.Dsn == MoveRig.MoverDsn,
            "the multi-host dsn key is not resolvable from the worker for object shards");
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
        tick.Value.Should().Be(ProcessOutcome.InProgress, "the initial copy is running — we wait in ticks");
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
            "the initial copy is finished — waiting for the cutover tick");
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
            "updated_unix must advance with every tick — abort uses it to tell a live mover");
        afterSecond.Value.State.Should().Be(afterFirst.Value!.State);
        afterSecond.Value.Phase.Should().Be(afterFirst.Value.Phase, "the phase did not change — still copy");
        afterSecond.Value.StartedUnix.Should().Be(111, "the move start does not shift");
    }

    // AAA: битая заявка в сторе — тик НЕ падает: warning с именем ключа попадает
    //      в лог («исправь или удали ключ»), валидные заявки обрабатываются
    //      (план Task 3 Step 3, ревью №2)
    [Fact]
    public async Task TickAsync_BrokenRequestKey_WarnsAndProcessesValidRequests()
    {
        // Arrange — валидная заявка bucket_42 + битый JSON на bucket_9
        var logger = new RecordingLogger();
        var rig = await MoveRig.NewAsync(logger: logger);
        rig.Etcd.Seed(MoveNames.MoveKey("shop", "bucket_9"), "not-json");

        // Act
        var tick = await rig.Process.TickAsync(MoveRig.Snap(), CancellationToken.None);

        // Assert
        tick.Value.Should().Be(ProcessOutcome.InProgress,
            "a broken request is not a tick failure; the valid request was processed (M0 passed)");
        logger.Entries.Should().Contain(e =>
            e.Level == LogLevel.Warning && e.Text.Contains("/pgworker/moves/shop/bucket_9"),
            "the warning names the broken key");
        logger.Entries.Should().Contain(e =>
            e.Level == LogLevel.Warning && e.Text.Contains("fix or delete the key"),
            "operator hint — what to do with the broken key");
        rig.Etcd.Store.ContainsKey(MoveNames.MoveKey("shop", "bucket_9")).Should().BeTrue(
            "the key is not deleted automatically — the operator fixes the broken request");
    }

    // Сдвигаемые часы для детерминированного updated_unix (TimeProvider-хук процесса).
    private sealed class StepClock : TimeProvider
    {
        private DateTimeOffset _now = DateTimeOffset.FromUnixTimeSeconds(1770000000);

        public void Advance(TimeSpan delta) => _now += delta;

        public override DateTimeOffset GetUtcNow() => _now;
    }

    // Записывающий логгер (ревью №2): фиксирует уровень и текст — ассерты по факту.
    private sealed class RecordingLogger : ILogger<MoveProcess>
    {
        public readonly List<(LogLevel Level, string Text)> Entries = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
            => Entries.Add((logLevel, formatter(state, exception)));
    }
}
