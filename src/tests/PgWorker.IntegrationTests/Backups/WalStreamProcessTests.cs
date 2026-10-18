using FluentAssertions;
using PgWorker.Backups;
using PgWorker.Backups.Sql;
using PgWorker.Core.Model;
using Shared.Etcd.Client;
using PgWorker.Etcd.Parsing;
using PgWorker.IntegrationTests.E2e;
using PgWorker.IntegrationTests.Etcd;
using PgWorker.Core.Templates;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Probes;
using Xunit;

namespace PgWorker.IntegrationTests.Backups;

// Интеграции WalStreamProcess (t03 spec Ф3): реальный etcd (статусы/журнал) +
// фейки docker-агентов/SQL/S3. Снапшот кластера строится руками.
[Collection(NonE2eCollection.Name)]
public class WalStreamProcessTests(EtcdFixture fixture)
{
    private readonly ClaimStore _claims = new("/pgworker", [fixture.Endpoint], fixture.Gateway, TimeProvider.System);

    // ── Хелперы Arrange ──

    // DSN источника-ноды (как ShardEndpoints.AdminDsn): сид слотов per-DSN (t27).
    private static string SourceDsn(int pgPort, string cluster) => ShardEndpoints.AdminDsn(
        new NodeAddress("127.0.0.1", new NodePorts(pgPort, 18001, 17001)),
        cluster, new InstallSecrets("su-pw", "sb-pw", "adm-pw", "mov-pw"));

    // Активный кластер c1/shard1 с нодой shard1a (master-ключ → shard1a:17001).
    private static ClusterSnapshot BuildSnap(string cluster = "c1") => new(
        new ClusterConfig(cluster, 2, cluster, null, ClusterState.Active),
        [new ShardSpec("shard1", 1, $"host=shard1a dbname={cluster}", "shard1a:17001",
            [new NodeSpec("shard1", "shard1a", NodeState.Running)])],
        []);

    // Сид etcd под конкретный тест-кластер: portalloc + backup_password; чистка
    // wal-ключей — чтобы тесты не зависели от порядка исполнения.
    private async Task SeedAsync(string cluster, string password = "pw")
    {
        var ct = TestContext.Current.CancellationToken;
        await fixture.Gateway.DeleteAsync(fixture.Endpoint, $"/pgworker/backups/{cluster}/", prefix: true, ct);
        // клэйм прошлых тестов (lease ещё жив, TTL 15 c > тест) — снимаем принудительно
        await fixture.Gateway.DeleteAsync(fixture.Endpoint, $"/pgworker/claims/{cluster}", prefix: false, ct);
        await fixture.Gateway.PutAsync(fixture.Endpoint, $"/pgworker/portalloc/{cluster}",
            Portalloc.Serialize(new Dictionary<string, NodeAddress>
            {
                ["shard1/shard1a"] = new("127.0.0.1", new NodePorts(16001, 18001, 17001)),
            }), null, ct);
        await fixture.Gateway.PutAsync(fixture.Endpoint, $"/clusters/{cluster}/backup_password",
            password, null, ct);
    }

    private WalStreamProcess BuildProcess(
        BackupsRuntimeOptions? options,
        FakeWalSqlExecutor sql,
        FakeBackupS3 s3,
        StubScaleDriver driver,
        TimeProvider? clock = null,
        Action<string, string, long?>? uploadedAgeObserver = null,
        Action<string, string, long?>? lagObserver = null,
        TimeSpan? watchdogWindow = null,
        Shared.Core.Hosting.ILoopProgress? progress = null)
        => new(
            fixture.Gateway, [fixture.Endpoint], driver,
            new ShardEndpoints(fixture.Gateway, [fixture.Endpoint], new ShardProbe(FakePatroni.CreateProbeClient())),
            sql, FakePoolFactory.Single(s3),
            new WalStatusWriter(fixture.Gateway, [fixture.Endpoint]),
            _claims, new WorkJournal("/pgworker", fixture.Gateway, [fixture.Endpoint]),
            () => options,
            new InstallSecrets("su-pw", "sb-pw", "adm-pw", "mov-pw"),
            clock ?? TimeProvider.System,
            lagObserver,
            null,
            uploadedAgeObserver,
            watchdogWindow,
            progress);

    // Счётчик прогресс-отметок (аудит долгих фаз, код-ревью B3).
    private sealed class MarkCounter : Shared.Core.Hosting.ILoopProgress
    {
        public int Marks;
        public void Mark() => Marks++;
    }

    // AAA (код-ревью B3): аудируемая фаза create агента передаёт progress —
    // итерации поллинга дают Mark (окно без progress = молчание для watchdog).
    [Fact]
    public async Task AuditableAgentCreateIterationsGiveMark()
    {
        // Arrange — «медленный daemon отпускает»: первые 2 вызова висят 2.5 c
        // (дольше таймаута итерации — пол 1 c из Math.Max(TicksPerSecond, window/2)
        // при окне 0.5 c), 3-й — мгновенный успех
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("ap9");
        (await _claims.TryClaimClusterAsync("ap9", ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor();
        sql.SlotsByDsn.GetOrAdd(SourceDsn(16001, "ap9"), _ => []).Add("pgw_bkp_ap9_shard1");
        var s3 = new FakeBackupS3();
        var driver = new StubScaleDriver
        {
            AgentEnsureDelay = TimeSpan.FromSeconds(2.5),
            SlowAgentEnsures = 2,
        };
        var marks = new MarkCounter();
        var process = BuildProcess(Options(), sql, s3, driver,
            progress: marks, watchdogWindow: TimeSpan.FromSeconds(0.5));

        // Act — тик: create агента → 2 таймаут-итерации + успешная третья
        (await process.TickAsync(BuildSnap("ap9"), null, ct)).IsSuccess.Should().BeTrue();

        // Assert — Mark рос с итерациями: 2 таймаут-итерации + успешная = ≥ 3
        // (не «один на весь вызов» и не ноль)
        marks.Marks.Should().BeGreaterThanOrEqualTo(3,
            "polling iterations of an audited phase give Mark (spec §1.2 item 4)");
        driver.EnsuredBackupAgents.Should().Contain("pgw-backup-wal-ap9-shard1-shard1a");
    }

    // Тест-опции: VerifyIntervalSec=0 — контроль выполняется КАЖДЫМ тиком (AAA).
    private static BackupsRuntimeOptions Options(int verify = 0, int lag = 1024, int stale = 300) => new(
        Enabled: true,
        S3Bucket: "bkt",
        JobImage: "pgworker-backup:test",
        StagingDir: "/backup-staging",
        WalVerifyIntervalSec: verify,
        WalLagMaxSegments: lag,
        WalStaleSec: stale);

    // Полный COMPLETED с wal_start_segment (для chain_start от полного);
    // t18: якорь chain_start — только verify-OK полные, поэтому по умолчанию
    // полный валиден (Ok), не-OK задаётся явно параметром.
    private static ShardBackups FullShard(
            string walStart, WalStreamState? wal = null, BackupVerify? verify = null) => new(
        [new FullBackupState("20260910120000Z", FullBackupStatus.Completed, "shard1a",
            BackupSourceRole.Replica, 1757500000, 1757500300, walStart, 1024, null,
            verify ?? new BackupVerify(BackupVerifyStatus.Ok, 1757500400))],
        wal);

    [Fact]
    public async Task TickEnsuresAgentAndSlotAndWaitsForFirstObject()
    {
        // Arrange — полных нет, S3 пуст, ключ wal нет; клэйм кластера наш
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("c1");
        (await _claims.TryClaimClusterAsync("c1", ct)).Value.Should().BeTrue("claim is a tick precondition");
        var sql = new FakeWalSqlExecutor();
        var s3 = new FakeBackupS3();
        var driver = new StubScaleDriver();
        var process = BuildProcess(Options(), sql, s3, driver);

        // Act — первый тик: слот создан, контейнер агента создан+запущен,
        //       ключ wal НЕ пишется (нет объектов — spec §3.2 шаг 8)
        var result = await process.TickAsync(BuildSnap(), null, ct);

        // Assert
        result.IsSuccess.Should().BeTrue();
        driver.EnsuredBackupAgents.Should().Contain("pgw-backup-wal-c1-shard1-shard1a");
        // Контракт (ревью Ф7 №1): процесс НЕ назначает сеть — драйвер владеет
        // per-cluster сетью pgw-net-<C> и проставляет её при create (юнит-тест
        // ClusterDriverTests).
        driver.EnsuredAgentSpecs.Should().ContainSingle().Which.Network.Should().BeNull();
        // Рестарт-политики у агента нет: docker не лупит — супервиз тика
        // пересоздаёт exited-агента (иначе луп молотит на снесённом мастере)
        driver.EnsuredAgentSpecs.Should().ContainSingle().Which.RestartPolicy.Should().Be("no");
        // Слот ensured на DSN мастера (адресация ShardEndpoints.AdminDsn)
        sql.SlotsByDsn.Values.Should().Contain(s => s.Contains("pgw_bkp_c1_shard1"));
        var wal = await fixture.Gateway.GetAsync(
            fixture.Endpoint, "/pgworker/backups/c1/shard1/wal", ct);
        wal.Value.Should().BeNull("the key is not written until the first observation (spec §3.2 item 8)");
    }

    [Fact]
    public async Task TickIsIdempotentAgentNotRecreated()
    {
        // Arrange — агент уже создан (первый тик)
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("c1");
        (await _claims.TryClaimClusterAsync("c1", ct)).Value.Should().BeTrue("claim is a tick precondition");
        var sql = new FakeWalSqlExecutor();
        var s3 = new FakeBackupS3();
        var driver = new StubScaleDriver();
        var process = BuildProcess(Options(), sql, s3, driver);
        (await process.TickAsync(BuildSnap(), null, ct)).IsSuccess.Should().BeTrue();

        // Act — второй тик
        (await process.TickAsync(BuildSnap(), null, ct)).IsSuccess.Should().BeTrue();

        // Assert — EnsuredBackupAgents без дублей; ключ wal не пишется
        driver.EnsuredBackupAgents.Should().ContainSingle(n => n == "pgw-backup-wal-c1-shard1-shard1a");
        var wal = await fixture.Gateway.GetAsync(
            fixture.Endpoint, "/pgworker/backups/c1/shard1/wal", ct);
        wal.Value.Should().BeNull();
    }

    [Fact]
    public async Task TickWithoutCredentialsSkipsShardWithJournalNote()
    {
        // Arrange — backup_password отсутствует (t02 не смержена/ensure не прошёл)
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("c1", password: "");
        await fixture.Gateway.DeleteAsync(
            fixture.Endpoint, "/clusters/c1/backup_password", prefix: false, ct);
        (await _claims.TryClaimClusterAsync("c1", ct)).Value.Should().BeTrue("claim is a tick precondition");
        var sql = new FakeWalSqlExecutor();
        var driver = new StubScaleDriver();
        var process = BuildProcess(Options(), sql, new FakeBackupS3(), driver);

        // Act
        var result = await process.TickAsync(BuildSnap(), null, ct);

        // Assert — transient-пропуск: ни слота, ни агента
        result.IsSuccess.Should().BeTrue();
        sql.SlotsByDsn.Should().BeEmpty("agent/slot are not raised without credentials (spec §3.2 item 1)");
        driver.EnsuredBackupAgents.Should().BeEmpty();
        var journal = await fixture.Gateway.GetAsync(
            fixture.Endpoint, "/pgworker/work/c1", ct);
        journal.Value!.Value.Should().Contain("waiting-backup-password");
    }

    [Fact]
    public async Task TickWithEnabledFalseStopsAgentsAndWritesStopped()
    {
        // Arrange — живой агент + ключ wal ACTIVE от прошлого прохода
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("c1");
        (await _claims.TryClaimClusterAsync("c1", ct)).Value.Should().BeTrue("claim is a tick precondition");
        var sql = new FakeWalSqlExecutor();
        var s3 = new FakeBackupS3();
        var driver = new StubScaleDriver();
        var writer = new WalStatusWriter(fixture.Gateway, [fixture.Endpoint]);
        await writer.WriteIfChangedAsync("c1", "shard1", new WalStreamState(
            WalStreamStatus.Active, "pgw_bkp_c1_shard1", "shard1a",
            "000000010000000000000001", "000000010000000000000002",
            "000000010000000000000002", 1757500000, 1, null), ct);
        // первый тик с ВКЛЮЧЁННОЙ подсистемой — агент поднят
        var process = BuildProcess(Options(), sql, s3, driver);
        (await process.TickAsync(BuildSnap(), null, ct)).IsSuccess.Should().BeTrue();
        driver.EnsuredBackupAgents.Should().Contain("pgw-backup-wal-c1-shard1-shard1a");

        // Act — тик с ВЫКЛЮЧЕННОЙ подсистемой (Enabled=false → runtime()=null)
        var stopped = BuildProcess(null, sql, s3, driver);
        var result = await stopped.TickAsync(BuildSnap(), null, ct);

        // Assert — агенты вниз, ключ финально STOPPED
        result.IsSuccess.Should().BeTrue();
        driver.RemovedBackupAgents.Should().Contain("pgw-backup-wal-c1-shard1-shard1a");
        var wal = await writer.ReadAsync("c1", "shard1", ct);
        wal.Value!.State.Should().Be(WalStreamStatus.Stopped);
    }

    // ── Контроль цепочки + lag (Task 10, AC4/AC5) ──

    // Фиксированные часы: управление now() для расписания/тишины (AAA).
    private sealed class MutableClock : TimeProvider
    {
        public DateTimeOffset Now { get; set; } = new(2026, 9, 10, 12, 0, 0, TimeSpan.Zero);

        public override DateTimeOffset GetUtcNow() => Now;
    }

    // Чтение wal-ключа тестом (через каркасный путь — WalStatusWriter.ReadAsync).
    private async Task<WalStreamState?> ReadWal(string cluster)
    {
        var reader = new WalStatusWriter(fixture.Gateway, [fixture.Endpoint]);
        var read = await reader.ReadAsync(cluster, "shard1", TestContext.Current.CancellationToken);
        read.IsSuccess.Should().BeTrue();
        return read.Value;
    }

    // Сплошная цепочка сегментов [from..to] в FakeS3 (имена 24-hex).
    private static void SeedSegments(FakeBackupS3 s3, string cluster, int from, int to)
    {
        for (var i = from; i <= to; i++)
            s3.Objects.Add((cluster, "shard1", $"0000000100000000000000{i:x2}"));
    }

    // t05 §3.4 гвард: шард с активной restore-заявкой — агент не ensure,
    // wal-статус не пишется (контуры не трогают шард во время restore).
    [Fact]
    public async Task TickSkipsShardWithActiveRestore()
    {
        // Arrange — full COMPLETED + цепочка (due-условия есть) + PLANNED restore
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("cr9");
        (await _claims.TryClaimClusterAsync("cr9", ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor { Current = ("0/3000000", 1) };
        var s3 = new FakeBackupS3();
        SeedSegments(s3, "cr9", 1, 3);
        var driver = new StubScaleDriver();
        var lags = new List<(string Cluster, string Shard, long? Lag)>();
        var process = new WalStreamProcess(
            fixture.Gateway, [fixture.Endpoint], driver,
            new ShardEndpoints(fixture.Gateway, [fixture.Endpoint], new ShardProbe(FakePatroni.CreateProbeClient())),
            sql, FakePoolFactory.Single(s3), new WalStatusWriter(fixture.Gateway, [fixture.Endpoint]),
            _claims, new WorkJournal("/pgworker", fixture.Gateway, [fixture.Endpoint]),
            () => Options(), new InstallSecrets("su", "sb", "adm", "mv"),
            TimeProvider.System, (c, s, l) => lags.Add((c, s, l)));
        var restoring = FullShard("000000010000000000000001") with
        {
            Restores = [new RestoreOperationState("20260911120000Z", RestoreStatus.Planned,
                "", "cr9/shard1", "latest", "shard1a", 1760000000, "operator")],
        };
        var backups = new ClusterBackups("cr9", null,
            new Dictionary<string, ShardBackups> { ["shard1"] = restoring });

        // Act
        var result = await process.TickAsync(BuildSnap("cr9"), backups, ct);

        // Assert — агент не поднят, wal-ключа нет, слот не создавался
        result.IsSuccess.Should().BeTrue();
        driver.EnsuredBackupAgents.Should().BeEmpty();
        sql.SlotsByDsn.Should().BeEmpty();
        var wal = await fixture.Gateway.GetAsync(
            fixture.Endpoint, "/pgworker/backups/cr9/shard1/wal", ct);
        wal.Value.Should().BeNull("shard is in restore — backup contours stay silent");
    }

    [Fact]
    public async Task VerifyContinuousChainWritesActiveAndChainStartFromFull()
    {
        // Arrange — full COMPLETED wal_start=..01; S3: сегменты 1..3
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("cc1");
        (await _claims.TryClaimClusterAsync("cc1", ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor { Current = ("0/3000000", 1) };
        var s3 = new FakeBackupS3();
        SeedSegments(s3, "cc1", 1, 3);
        var driver = new StubScaleDriver();
        var lags = new List<(string Cluster, string Shard, long? Lag)>();
        var process = new WalStreamProcess(
            fixture.Gateway, [fixture.Endpoint], driver,
            new ShardEndpoints(fixture.Gateway, [fixture.Endpoint], new ShardProbe(FakePatroni.CreateProbeClient())),
            sql, FakePoolFactory.Single(s3), new WalStatusWriter(fixture.Gateway, [fixture.Endpoint]),
            _claims, new WorkJournal("/pgworker", fixture.Gateway, [fixture.Endpoint]),
            () => Options(), new InstallSecrets("su", "sb", "adm", "mv"),
            TimeProvider.System, (c, s, l) => lags.Add((c, s, l)));
        var backups = new ClusterBackups("cc1", null,
            new Dictionary<string, ShardBackups> { ["shard1"] = FullShard("000000010000000000000001") });

        // Act — контроль due (VerifyIntervalSec=0)
        (await process.TickAsync(BuildSnap("cc1"), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — ACTIVE, chain_start от полного, last_uploaded по S3, lag посчитан
        var wal = await ReadWal("cc1");
        wal.Should().NotBeNull();
        wal!.State.Should().Be(WalStreamStatus.Active);
        wal.ChainStartSegment.Should().Be("000000010000000000000001");
        wal.LastUploadedSegment.Should().Be("000000010000000000000003");
        wal.LagSegments.Should().NotBeNull();
        lags.Should().Contain(t => t.Cluster == "cc1" && t.Shard == "shard1" && t.Lag != null);
    }

    [Fact]
    public async Task VerifyGapWritesBrokenBoundariesStopsAndBlocksAgentAc4a()
    {
        // Arrange — full wal_start=..01; S3: 1,3 (дыра на ..02)
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("cc2");
        (await _claims.TryClaimClusterAsync("cc2", ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor();
        var s3 = new FakeBackupS3();
        SeedSegments(s3, "cc2", 1, 1);
        s3.Objects.Add(("cc2", "shard1", "000000010000000000000003"));
        var driver = new StubScaleDriver();
        var process = BuildProcess(Options(), sql, s3, driver);
        var backups = new ClusterBackups("cc2", null,
            new Dictionary<string, ShardBackups> { ["shard1"] = FullShard("000000010000000000000001") });

        // Act — тик контроля (дыра ловится)
        (await process.TickAsync(BuildSnap("cc2"), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — BROKEN с границами; повторный тик агент НЕ поднимает
        var wal = await ReadWal("cc2");
        wal!.State.Should().Be(WalStreamStatus.Broken);
        wal.Error.Should().Contain("000000010000000000000002").And.Contain("000000010000000000000003");
        (await process.TickAsync(BuildSnap("cc2"), backups, ct)).IsSuccess.Should().BeTrue();
        driver.EnsuredBackupAgents.Should().BeEmpty("ChainBroken blocks agent raising (review F4-2 #1)");
    }

    [Fact]
    public async Task VerifySlotInvalidationWritesBrokenAndStopsAgentAc4b()
    {
        // Arrange — ключ wal ACTIVE; FakeSql.Slots пуст (слот исчез)
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("cc3");
        (await _claims.TryClaimClusterAsync("cc3", ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor();
        var s3 = new FakeBackupS3();
        SeedSegments(s3, "cc3", 1, 2);
        var driver = new StubScaleDriver();
        driver.BackupAgentObjects.Add(new DockerContainer(
            "id-agent-cc3", ["/pgw-backup-wal-cc3-shard1"], "running", "img"));
        var writer = new WalStatusWriter(fixture.Gateway, [fixture.Endpoint]);
        var liveWal = new WalStreamState(
            WalStreamStatus.Active, "pgw_bkp_cc3_shard1", "shard1a",
            "000000010000000000000001", "000000010000000000000002",
            "000000010000000000000002", 1757500000, 1, null);
        await writer.WriteIfChangedAsync("cc3", "shard1", liveWal, ct);
        var process = BuildProcess(Options(), sql, s3, driver);
        var backups = new ClusterBackups("cc3", null,
            new Dictionary<string, ShardBackups>
            {
                ["shard1"] = new(FullShard("000000010000000000000001").Full, liveWal),
            });

        // Act
        (await process.TickAsync(BuildSnap("cc3"), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — BROKEN с error про слот; агент остановлен; слот пересоздан
        var wal = await ReadWal("cc3");
        wal!.State.Should().Be(WalStreamStatus.Broken);
        wal.Error.Should().Contain("slot");
        wal.ChainStartSegment.Should().Be("000000010000000000000002", "gap boundary — last_uploaded at detection time");
        driver.RemovedBackupAgents.Should().Contain("pgw-backup-wal-cc3-shard1");
        sql.SlotsByDsn.Values.Should().Contain(s => s.Contains("pgw_bkp_cc3_shard1"), "the slot is recreated at BROKEN (spec §3.2)");
    }

    [Fact]
    public async Task VerifyNewFullAboveGapRestoresActiveOnSameTickAc4c()
    {
        // Arrange — DEGRADED (дыра ..02) в ключе; появляется full wal_start=..05; S3: 5,6
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("cc4");
        (await _claims.TryClaimClusterAsync("cc4", ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor();
        var s3 = new FakeBackupS3();
        SeedSegments(s3, "cc4", 5, 6);
        var driver = new StubScaleDriver();
        var writer = new WalStatusWriter(fixture.Gateway, [fixture.Endpoint]);
        await writer.WriteIfChangedAsync("cc4", "shard1", new WalStreamState(
            WalStreamStatus.Degraded, "pgw_bkp_cc4_shard1", "shard1a",
            "000000010000000000000001", "000000010000000000000001",
            "000000010000000000000001", 1757500000, null, "WAL chain gap"), ct);
        var process = BuildProcess(Options(), sql, s3, driver);
        var backups = new ClusterBackups("cc4", null,
            new Dictionary<string, ShardBackups> { ["shard1"] = FullShard("000000010000000000000005") });

        // Act — ОДИН тик (контроль due)
        (await process.TickAsync(BuildSnap("cc4"), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — ACTIVE с новым chain_start; агент поднят ТЕМ ЖЕ тиком
        var wal = await ReadWal("cc4");
        wal!.State.Should().Be(WalStreamStatus.Active);
        wal.ChainStartSegment.Should().Be("000000010000000000000005");
        driver.EnsuredBackupAgents.Should().Contain("pgw-backup-wal-cc4-shard1-shard1a");
    }

    [Fact]
    public async Task TransientStaleDegradedDoesNotBlockAgentRecreation()
    {
        // Arrange — агент exited; цепочка сплошная, но тишина загрузок 3600 c (порог 60)
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("cc5");
        (await _claims.TryClaimClusterAsync("cc5", ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor();
        var s3 = new FakeBackupS3 { LastModified = DateTimeOffset.UtcNow.AddSeconds(-3600) };
        SeedSegments(s3, "cc5", 1, 2);
        var driver = new StubScaleDriver();
        driver.BackupAgentObjects.Add(new DockerContainer(
            "id-agent-cc5", ["/pgw-backup-wal-cc5-shard1"], "exited", "img"));
        var process = BuildProcess(Options(stale: 60), sql, s3, driver);
        var backups = new ClusterBackups("cc5", null,
            new Dictionary<string, ShardBackups> { ["shard1"] = FullShard("000000010000000000000001") });

        // Act — тик контроля
        (await process.TickAsync(BuildSnap("cc5"), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — DEGRADED «тишина»; агент ПЕРЕСОЗДАН (remove → ensure)
        var wal = await ReadWal("cc5");
        wal!.State.Should().Be(WalStreamStatus.Degraded);
        wal.Error.Should().Contain("upload silence");
        driver.RemovedBackupAgents.Should().Contain("pgw-backup-wal-cc5-shard1");
        driver.EnsuredBackupAgents.Should().Contain("pgw-backup-wal-cc5-shard1-shard1a");
    }

    [Fact]
    public async Task SupervisorRunningAgentCanonicalNameWithoutSlashUntouched()
    {
        // Arrange — живой агент в списке движка: канон ListContainersAsync — имена
        // БЕЗ ведущего "/"; раньше матч "/"+name всегда промахивался (t05-регресс
        // 2026-09-13) — супервиз трактовал живого агента отсутствующим и тикал
        // ensure каждый проход
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("cc8");
        (await _claims.TryClaimClusterAsync("cc8", ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor();
        var s3 = new FakeBackupS3();
        SeedSegments(s3, "cc8", 1, 2);
        var driver = new StubScaleDriver();
        driver.BackupAgentObjects.Add(new DockerContainer(
            "id-agent-cc8", ["pgw-backup-wal-cc8-shard1-shard1a"], "running", "img"));
        var process = BuildProcess(Options(), sql, s3, driver);
        var backups = new ClusterBackups("cc8", null,
            new Dictionary<string, ShardBackups> { ["shard1"] = FullShard("000000010000000000000001") });

        // Act
        (await process.TickAsync(BuildSnap("cc8"), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — живой агент найден и не пересоздаётся (ни remove, ни ensure)
        driver.RemovedBackupAgents.Should().BeEmpty();
        driver.EnsuredBackupAgents.Should().BeEmpty();
    }

    // AAA (AC1): дыра → wal.state=BROKEN с границей разрыва в chain_start,
    // агент не поднимается повторными тиками (маркер — ключ, не память)
    [Fact]
    public async Task VerifyGapWritesBrokenAndKeepsAgentDown()
    {
        // Arrange — full wal_start=..01; S3: 1,3 (дыра на ..02)
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("cb1");
        (await _claims.TryClaimClusterAsync("cb1", ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor();
        var s3 = new FakeBackupS3();
        SeedSegments(s3, "cb1", 1, 1);
        s3.Objects.Add(("cb1", "shard1", "000000010000000000000003"));
        var driver = new StubScaleDriver();
        var process = BuildProcess(Options(), sql, s3, driver);
        var backups = new ClusterBackups("cb1", null,
            new Dictionary<string, ShardBackups> { ["shard1"] = FullShard("000000010000000000000001") });

        // Act — тик контроля + повторный тик
        (await process.TickAsync(BuildSnap("cb1"), backups, ct)).IsSuccess.Should().BeTrue();
        (await process.TickAsync(BuildSnap("cb1"), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — BROKEN с границей (последний непрерывный = ..01); агент не поднимается
        var wal = await ReadWal("cb1");
        wal!.State.Should().Be(WalStreamStatus.Broken);
        wal.Error.Should().Contain("000000010000000000000002");
        wal.ChainStartSegment.Should().Be("000000010000000000000001", "gap boundary — last continuous segment");
        driver.EnsuredBackupAgents.Should().BeEmpty("BROKEN keeps the agent down (key, not memory)");
    }

    // AAA (AC2): слот исчез при живой цепочке → BROKEN + слот ПЕРЕСОЗДАН, агент вниз
    [Fact]
    public async Task SlotDisappearedWritesBrokenAndRecreatesSlot()
    {
        // Arrange — ключ wal ACTIVE (цепочка ..01-..02), слота в FakeSql нет, агент жив
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("cb2");
        (await _claims.TryClaimClusterAsync("cb2", ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor();
        var s3 = new FakeBackupS3();
        SeedSegments(s3, "cb2", 1, 2);
        var driver = new StubScaleDriver();
        driver.BackupAgentObjects.Add(new DockerContainer(
            "id-agent-cb2", ["/pgw-backup-wal-cb2-shard1"], "running", "img"));
        var writer = new WalStatusWriter(fixture.Gateway, [fixture.Endpoint]);
        var liveWal = new WalStreamState(
            WalStreamStatus.Active, "pgw_bkp_cb2_shard1", "shard1a",
            "000000010000000000000001", "000000010000000000000002",
            "000000010000000000000002", 1757500000, 1, null);
        await writer.WriteIfChangedAsync("cb2", "shard1", liveWal, ct);
        var process = BuildProcess(Options(), sql, s3, driver);
        var backups = new ClusterBackups("cb2", null,
            new Dictionary<string, ShardBackups> { ["shard1"] = new(FullShard("000000010000000000000001").Full, liveWal) });

        // Act
        (await process.TickAsync(BuildSnap("cb2"), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — BROKEN (chain_start = last_uploaded на момент обнаружения);
        // агент снят; слот пересоздан immediate+reserved (держит позицию к пересъёму)
        var wal = await ReadWal("cb2");
        wal!.State.Should().Be(WalStreamStatus.Broken);
        wal.Error.Should().Contain("slot");
        wal.ChainStartSegment.Should().Be("000000010000000000000002");
        driver.RemovedBackupAgents.Should().Contain("pgw-backup-wal-cb2-shard1");
        sql.SlotsByDsn.Values.Should().Contain(s => s.Contains("pgw_bkp_cb2_shard1"), "the slot is recreated at BROKEN (spec §3.2)");
    }

    // AAA (t13 AC1): инвариант писателя нарушен (ACTIVE с last_uploaded_unix=null
    // — ручная правка/иной писатель) + слот исчез: тик НЕ падает в shard-error —
    // BROKEN (запись из живого ключа, now()-фолбэк в неё не попадает) + слот
    // пересоздан + агент остановлен + journal-заметка wal-key-invalid/<X>.
    // Битый wal в etcd не сидируется: снапшот-объект передан тику напрямую,
    // парсеры не задействованы (spec §3.3); чтение ключа — raw (парсер отверг
    // бы ключ без last_uploaded_unix — гвард остаётся первой линией, spec §3.4).
    [Fact]
    public async Task SlotDisappearedWithBrokenLastUploadedUnixDoesNotFailTick()
    {
        // Arrange — wal ACTIVE с LastUploadedUnix = null (инвариант писателя
        // нарушен), слота в FakeSql нет, агент жив; цепочка объектов в S3 есть
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("cb5");
        (await _claims.TryClaimClusterAsync("cb5", ct)).Value.Should().BeTrue("claim is a tick precondition");
        var sql = new FakeWalSqlExecutor();
        var s3 = new FakeBackupS3();
        SeedSegments(s3, "cb5", 1, 2);
        var driver = new StubScaleDriver();
        driver.BackupAgentObjects.Add(new DockerContainer(
            "id-agent-cb5", ["/pgw-backup-wal-cb5-shard1"], "running", "img"));
        var invalidWal = new WalStreamState(
            WalStreamStatus.Active, "pgw_bkp_cb5_shard1", "shard1a",
            "000000010000000000000001", "000000010000000000000002",
            "000000010000000000000002", null, 1, null); // LastUploadedUnix = null!
        var process = BuildProcess(Options(), sql, s3, driver);
        var backups = new ClusterBackups("cb5", null,
            new Dictionary<string, ShardBackups>
            {
                ["shard1"] = new(FullShard("000000010000000000000001").Full, invalidWal),
            });

        // Act
        var result = await process.TickAsync(BuildSnap("cb5"), backups, ct);

        // Assert — тик успешен (не shard-error); ключ BROKEN с error про слот и
        // границей = last_uploaded_segment; now()-фолбэк в запись не попал
        // (last_uploaded_unix в JSON отсутствует); слот пересоздан; агент
        // остановлен; журнал — wal-key-invalid, НЕ shard-error
        result.IsSuccess.Should().BeTrue();
        var walKv = await fixture.Gateway.GetAsync(
            fixture.Endpoint, "/pgworker/backups/cb5/shard1/wal", ct);
        walKv.Value.Should().NotBeNull("BROKEN is written even from a broken key");
        walKv.Value!.Value.Should().Contain("\"state\":\"BROKEN\"").And.Contain("slot");
        walKv.Value.Value.Should().Contain(
            "\"chain_start_segment\":\"000000010000000000000002\"",
            "gap boundary — last_uploaded_segment at detection time");
        walKv.Value.Value.Should().NotContain("\"last_uploaded_unix\"",
            "the record is built from the live wal: the now() fallback does not reach the key (spec §2)");
        sql.SlotsByDsn.Values.Should().Contain(s => s.Contains("pgw_bkp_cb5_shard1"), "the slot is recreated at BROKEN (spec §3.2)");
        driver.RemovedBackupAgents.Should().Contain("pgw-backup-wal-cb5-shard1");
        var journal = await fixture.Gateway.GetAsync(fixture.Endpoint, "/pgworker/work/cb5", ct);
        journal.Value!.Value.Should().Contain("wal-key-invalid/shard1");
        journal.Value.Value.Should().NotContain("shard-error");
    }

    // AAA (AC1/заживление): новый COMPLETED-полный ≥ границы + непрерывная цепь →
    // ACTIVE + агент ТЕМ ЖЕ тиком (даже без прошедшего VerifyIntervalSec)
    [Fact]
    public async Task VerifyAtBrokenEveryTickHealsInOneTick()
    {
        // Arrange — BROKEN-ключ (граница ..01, дыра ..02); VerifyIntervalSec=3600;
        // появился full wal_start=..05; S3: 5,6 (цепь от нового полного непрерывна —
        // закрытые сегменты набора -X stream дублируются в wal/)
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("cb3");
        (await _claims.TryClaimClusterAsync("cb3", ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor();
        var s3 = new FakeBackupS3();
        SeedSegments(s3, "cb3", 5, 6);
        var driver = new StubScaleDriver();
        var writer = new WalStatusWriter(fixture.Gateway, [fixture.Endpoint]);
        await writer.WriteIfChangedAsync("cb3", "shard1", new WalStreamState(
            WalStreamStatus.Broken, "pgw_bkp_cb3_shard1", "shard1a",
            "000000010000000000000001", "000000010000000000000001",
            "000000010000000000000001", 1757500000, null, "WAL chain gap"), ct);
        var process = BuildProcess(Options(verify: 3600), sql, s3, driver, clock: TimeProvider.System);
        var backups = new ClusterBackups("cb3", null,
            new Dictionary<string, ShardBackups> { ["shard1"] = FullShard("000000010000000000000005") });

        // Act — ОДИН тик (расписание 3600 с НЕ наступило — BROKEN контролирует каждый тик)
        (await process.TickAsync(BuildSnap("cb3"), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — ACTIVE с chain_start от нового полного; агент поднят тем же тиком
        var wal = await ReadWal("cb3");
        wal!.State.Should().Be(WalStreamStatus.Active);
        wal.ChainStartSegment.Should().Be("000000010000000000000005");
        driver.EnsuredBackupAgents.Should().Contain("pgw-backup-wal-cb3-shard1-shard1a");
    }

    // AAA (AC4 — рестарт-устойчивость): BROKEN живёт в etcd — пересоздание
    // процесса (новая фабрика = «рестарт воркера») не поднимает агента
    [Fact]
    public async Task BrokenSurvivesProcessRestart()
    {
        // Arrange — доводим до BROKEN первым процессом (дыра ..02)
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("cb4");
        (await _claims.TryClaimClusterAsync("cb4", ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor();
        var s3 = new FakeBackupS3();
        SeedSegments(s3, "cb4", 1, 1);
        s3.Objects.Add(("cb4", "shard1", "000000010000000000000003"));
        var driver = new StubScaleDriver();
        var first = BuildProcess(Options(), sql, s3, driver);
        var backups = new ClusterBackups("cb4", null,
            new Dictionary<string, ShardBackups> { ["shard1"] = FullShard("000000010000000000000001") });
        (await first.TickAsync(BuildSnap("cb4"), backups, ct)).IsSuccess.Should().BeTrue();
        (await ReadWal("cb4"))!.State.Should().Be(WalStreamStatus.Broken);

        // Act — «рестарт»: НОВАЯ инстанция процесса (in-memory словарей нет),
        // backups перечитан из etcd (backups-аргумент тика — как в ReconcileLoop)
        var restarted = BuildProcess(Options(), sql, s3, driver);
        var reader = new WalStatusWriter(fixture.Gateway, [fixture.Endpoint]);
        var reread = await reader.ReadAsync("cb4", "shard1", ct);
        var freshBackups = new ClusterBackups("cb4", null,
            new Dictionary<string, ShardBackups>
            {
                ["shard1"] = new(FullShard("000000010000000000000001").Full, reread.Value),
            });
        (await restarted.TickAsync(BuildSnap("cb4"), freshBackups, ct)).IsSuccess.Should().BeTrue();

        // Assert — агент не поднимается: маркер разрыва — ключ wal, не память
        driver.EnsuredBackupAgents.Should().BeEmpty("BROKEN in etcd survives a worker restart");
    }

    // AAA (AC9-регресс): смена мастера при живом ключе — агент пересоздаётся с нового
    // (masterChanged-ветка работает с новым ControlOutcome: ключ не BROKEN).
    // Тик 1 — контроль по расписанию (штатно пишет master_node нового резолва);
    // тик 2 — контроль вне интервала (VerifyIntervalSec=3600): EnsureAgent сверяет
    // master_node ПРЕЖНЕГО статуса с новым резолвом → remove+ensure с нового мастера.
    [Fact]
    public async Task MasterChangedAgentRecreatedFromNewMaster()
    {
        // Arrange — ключ wal ACTIVE master_node=shard1a; агент running на «shard1a»;
        // portalloc + ShardSpec.Master переводят мастера на shard1b; цепочка сплошная
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("cm1");
        (await _claims.TryClaimClusterAsync("cm1", ct)).Value.Should().BeTrue();
        await fixture.Gateway.PutAsync(fixture.Endpoint, "/pgworker/portalloc/cm1",
            Portalloc.Serialize(new Dictionary<string, NodeAddress>
            {
                ["shard1/shard1a"] = new("127.0.0.1", new NodePorts(16001, 18001, 17001)),
                ["shard1/shard1b"] = new("localhost", new NodePorts(16002, 18002, 17002)),
            }), null, ct);
        var sql = new FakeWalSqlExecutor();
        // Слот жив на DSN ОБОИХ нод (источники shard1a/shard1b — независимые инстансы)
        sql.SlotsByDsn.GetOrAdd(SourceDsn(16001, "cm1"), _ => []).Add("pgw_bkp_cm1_shard1");
        sql.SlotsByDsn.GetOrAdd(SourceDsn(16002, "cm1"), _ => []).Add("pgw_bkp_cm1_shard1");
        var s3 = new FakeBackupS3();
        SeedSegments(s3, "cm1", 1, 2);
        var driver = new StubScaleDriver();
        driver.BackupAgentObjects.Add(new DockerContainer(
            "id-agent-cm1", ["/pgw-backup-wal-cm1-shard1"], "running", "img"));
        var writer = new WalStatusWriter(fixture.Gateway, [fixture.Endpoint]);
        var liveWal = new WalStreamState(
            WalStreamStatus.Active, "pgw_bkp_cm1_shard1", "shard1a",
            "000000010000000000000001", "000000010000000000000002",
            "000000010000000000000002", 1757500000, 1, null);
        await writer.WriteIfChangedAsync("cm1", "shard1", liveWal, ct);
        var process = BuildProcess(Options(verify: 3600), sql, s3, driver);
        // снапшот с двумя нодами: мастер резолвится в shard1b (Master-ключ шарда)
        var snapTwoNodes = new ClusterSnapshot(
            new ClusterConfig("cm1", 2, "cm1", null, ClusterState.Active),
            [new ShardSpec("shard1", 1, "host=shard1b dbname=cm1", "shard1b:17002",
            [
                new NodeSpec("shard1", "shard1a", NodeState.Running),
                new NodeSpec("shard1", "shard1b", NodeState.Running),
            ])],
            []);
        var backups = new ClusterBackups("cm1", null,
            new Dictionary<string, ShardBackups> { ["shard1"] = new(FullShard("000000010000000000000001").Full, liveWal) });

        // Act — тик 1: контроль (пишет ACTIVE); тик 2: контроль вне расписания —
        // супервиз агента сверяет master_node прежнего статуса с новым резолвом
        (await process.TickAsync(snapTwoNodes, backups, ct)).IsSuccess.Should().BeTrue();
        (await process.TickAsync(snapTwoNodes, backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — старый агент снят, новый поднят (пересоздание с нового мастера)
        driver.RemovedBackupAgents.Should().Contain("pgw-backup-wal-cm1-shard1");
        driver.EnsuredBackupAgents.Should().Contain("pgw-backup-wal-cm1-shard1-shard1b");
    }

    [Fact]
    public async Task VerifyGapWithoutPreviousKeyWritesBrokenAc4Totality()
    {
        // Arrange — ключа нет; full wal_start=..01; S3: 1,3 (дыра при первом наблюдении)
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("cc6");
        (await _claims.TryClaimClusterAsync("cc6", ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor();
        var s3 = new FakeBackupS3();
        SeedSegments(s3, "cc6", 1, 1);
        s3.Objects.Add(("cc6", "shard1", "000000010000000000000003"));
        var driver = new StubScaleDriver();
        var process = BuildProcess(Options(), sql, s3, driver);
        var backups = new ClusterBackups("cc6", null,
            new Dictionary<string, ShardBackups> { ["shard1"] = FullShard("000000010000000000000001") });

        // Act
        (await process.TickAsync(BuildSnap("cc6"), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — ключ создан BROKEN; агент не поднимается
        var wal = await ReadWal("cc6");
        wal!.State.Should().Be(WalStreamStatus.Broken);
        wal.Error.Should().Contain("000000010000000000000002");
        driver.EnsuredBackupAgents.Should().BeEmpty();
    }

    [Fact]
    public async Task VerifyNoSegmentObjectsKeyNotWrittenUntilFirstObservation()
    {
        // Arrange — full wal_start=..01 (chain anchored); S3 ПУСТ; ключа нет
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("cc7");
        (await _claims.TryClaimClusterAsync("cc7", ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor();
        var s3 = new FakeBackupS3();
        var driver = new StubScaleDriver();
        var process = BuildProcess(Options(), sql, s3, driver);
        var backups = new ClusterBackups("cc7", null,
            new Dictionary<string, ShardBackups> { ["shard1"] = FullShard("000000010000000000000001") });

        // Act
        (await process.TickAsync(BuildSnap("cc7"), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — ключ НЕ пишется (ревью Ф4-2 №2); агент при этом работает
        var wal = await ReadWal("cc7");
        wal.Should().BeNull();
        driver.EnsuredBackupAgents.Should().Contain("pgw-backup-wal-cc7-shard1-shard1a");
    }

    [Fact]
    public async Task VerifyObjectsGoneStalenessFromLastFactNotFromNow()
    {
        // Arrange — ключ ACTIVE с last_uploaded_unix = now-3600 (порог 300); S3 пуст
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("cc8");
        (await _claims.TryClaimClusterAsync("cc8", ct)).Value.Should().BeTrue();
        var clock = new MutableClock();
        var staleUnix = clock.Now.AddHours(-1).ToUnixTimeSeconds();
        var sql = new FakeWalSqlExecutor();
        // Слот жив на DSN мастера (источник один — shard1a, pg 16001)
        sql.SlotsByDsn.GetOrAdd(SourceDsn(16001, "cc8"), _ => []).Add("pgw_bkp_cc8_shard1");
        var s3 = new FakeBackupS3();
        var driver = new StubScaleDriver();
        var writer = new WalStatusWriter(fixture.Gateway, [fixture.Endpoint]);
        var liveWal = new WalStreamState(
            WalStreamStatus.Active, "pgw_bkp_cc8_shard1", "shard1a",
            "000000010000000000000001", "000000010000000000000001",
            "000000010000000000000001", staleUnix, 0, null);
        await writer.WriteIfChangedAsync("cc8", "shard1", liveWal, ct);
        var process = BuildProcess(Options(), sql, s3, driver, clock);
        var backups = new ClusterBackups("cc8", null,
            new Dictionary<string, ShardBackups>
            {
                ["shard1"] = new(FullShard("000000010000000000000001").Full, liveWal),
            });

        // Act — тик контроля (объекты пропали)
        (await process.TickAsync(BuildSnap("cc8"), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — DEGRADED «тишина»; last_uploaded_unix — прежний факт (не now)
        var wal = await ReadWal("cc8");
        wal!.State.Should().Be(WalStreamStatus.Degraded);
        wal.Error.Should().Contain("upload silence");
        wal.LastUploadedUnix.Should().Be(staleUnix,
            "fact over the record: the now() factory would hide a dead stream (review F4-2 #2)");
    }

    [Fact]
    public async Task VerifyLagAboveThresholdDegradedAc5a()
    {
        // Arrange — цепочка ..01; мастер далеко впереди (0/10000000 → сегмент 16)
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("cc9");
        (await _claims.TryClaimClusterAsync("cc9", ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor { Current = ("0/10000000", 1) };
        var s3 = new FakeBackupS3();
        SeedSegments(s3, "cc9", 1, 1);
        var driver = new StubScaleDriver();
        var process = BuildProcess(Options(lag: 2), sql, s3, driver);
        var backups = new ClusterBackups("cc9", null,
            new Dictionary<string, ShardBackups> { ["shard1"] = FullShard("000000010000000000000001") });

        // Act
        (await process.TickAsync(BuildSnap("cc9"), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — DEGRADED про отставание; агент ЖИВ (transient-деградация)
        var wal = await ReadWal("cc9");
        wal!.State.Should().Be(WalStreamStatus.Degraded);
        wal.Error.Should().Contain("WAL stream lag");
        driver.EnsuredBackupAgents.Should().Contain("pgw-backup-wal-cc9-shard1-shard1a");
        driver.RemovedBackupAgents.Should().NotContain("pgw-backup-wal-cc9-shard1-shard1a");
    }

    [Fact]
    public async Task FirstObjectAnchorsChainStartWithoutFulls()
    {
        // Arrange — полных нет, ключа нет; S3: 2..3
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("cca");
        (await _claims.TryClaimClusterAsync("cca", ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor();
        var s3 = new FakeBackupS3();
        SeedSegments(s3, "cca", 2, 3);
        var driver = new StubScaleDriver();
        var process = BuildProcess(Options(), sql, s3, driver);

        // Act
        (await process.TickAsync(BuildSnap("cca"), null, ct)).IsSuccess.Should().BeTrue();

        // Assert — ключ: chain_start = min-объект, state=ACTIVE
        var wal = await ReadWal("cca");
        wal!.State.Should().Be(WalStreamStatus.Active);
        wal.ChainStartSegment.Should().Be("000000010000000000000002");
    }

    // ---- t18 (AC4): контроль от wal_start новейшего verify-OK полного ----

    // AC4: после среза WAL ниже cutoff контроль стартует от wal_start
    // новейшего OK-полного — ложный BROKEN отсутствует. Дискриминация
    // старой min-семантики: от старого OK (..03) при объектах {5,6} контроль
    // дал бы дыру «ожидался ..04» и BROKEN.
    [Fact]
    public async Task VerifyStartsFromNewestOkFullWithoutFalseBroken()
    {
        // Arrange — два OK-полных: старый wal_start=..03 (сегменты ..03/..04
        // срезаны ретенцией), новый wal_start=..05; S3: 5,6; ключа wal нет
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("cv1");
        (await _claims.TryClaimClusterAsync("cv1", ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor { Current = ("0/6000000", 1) };
        var s3 = new FakeBackupS3();
        SeedSegments(s3, "cv1", 5, 6);
        var driver = new StubScaleDriver();
        var process = BuildProcess(Options(), sql, s3, driver);
        var oldOk = FullShard("000000010000000000000003").Full[0];
        var newOk = oldOk with
        {
            Id = "20260911120000Z", StartedUnix = 1757600000, FinishedUnix = 1757600300,
            WalStartSegment = "000000010000000000000005",
        };
        var backups = new ClusterBackups("cv1", null,
            new Dictionary<string, ShardBackups> { ["shard1"] = new([oldOk, newOk], null) });

        // Act — контроль due
        (await process.TickAsync(BuildSnap("cv1"), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — chain_start от ..05 (новейший OK), ACTIVE — не BROKEN
        var wal = await ReadWal("cv1");
        wal!.State.Should().Be(WalStreamStatus.Active);
        wal.ChainStartSegment.Should().Be("000000010000000000000005");
    }

    // AC4: ratchet — новый OK-полный со стартом НИЖЕ записанной границы
    // (verify-перепроверка старшего OK→FAILED откатила точку) не понижает
    // chain_start.
    [Fact]
    public async Task RatchetChainStartNotLoweredByOlderOkFull()
    {
        // Arrange — ключ ACTIVE с chain_start=..05; OK-полный wal_start=..03;
        // S3: 5,6,7 (непрерывны от ..05)
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("cv2");
        (await _claims.TryClaimClusterAsync("cv2", ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor { Current = ("0/7000000", 1) };
        // Слот жив на DSN мастера (живой ключ без слота = «слот исчез» → BROKEN
        // раньше проверки цепочки; паттерн соседних тестов cc8/cm1)
        sql.SlotsByDsn.GetOrAdd(SourceDsn(16001, "cv2"), _ => []).Add("pgw_bkp_cv2_shard1");
        var s3 = new FakeBackupS3();
        SeedSegments(s3, "cv2", 5, 7);
        var driver = new StubScaleDriver();
        var writer = new WalStatusWriter(fixture.Gateway, [fixture.Endpoint]);
        await writer.WriteIfChangedAsync("cv2", "shard1", new WalStreamState(
            WalStreamStatus.Active, "pgw_bkp_cv2_shard1", "shard1a",
            "000000010000000000000005", "000000010000000000000007",
            "000000010000000000000007", 1757500000, 0, null), ct);
        var process = BuildProcess(Options(), sql, s3, driver);
        var walKey = (await writer.ReadAsync("cv2", "shard1", ct)).Value;
        var backups = new ClusterBackups("cv2", null,
            new Dictionary<string, ShardBackups>
            {
                ["shard1"] = new(FullShard("000000010000000000000003").Full, walKey),
            });

        // Act — контроль due
        (await process.TickAsync(BuildSnap("cv2"), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — chain_start остался ..05 (ratchet не понижается), ACTIVE
        var wal = await ReadWal("cv2");
        wal!.State.Should().Be(WalStreamStatus.Active);
        wal.ChainStartSegment.Should().Be("000000010000000000000005");
    }

    // ---- t27: два источника, per-node супервиз, миграция (Ф3, spec §3.3/§3.4/§3.6) ----

    private const string SyncClusterJson =
        """{"members":[{"name":"shard1a","role":"master","state":"running","timeline":1},""" +
        """{"name":"shard1b","role":"replica","state":"running","timeline":1,"lag":0,"sync":true}]}""";

    // Сид двухнодового portalloc (patroni-порты обеих нод — фейк Patroni).
    private async Task<(FakePatroni Patroni, string Cluster)> SeedTwoNodeAsync(
        FakeWalSqlExecutor sql, FakePatroni? patroni, string cluster, string password = "pw")
    {
        var ct = TestContext.Current.CancellationToken;
        await fixture.Gateway.DeleteAsync(fixture.Endpoint, $"/pgworker/backups/{cluster}/", prefix: true, ct);
        await fixture.Gateway.DeleteAsync(fixture.Endpoint, $"/pgworker/claims/{cluster}", prefix: false, ct);
        var patroniPort = patroni?.Port ?? 1; // без фейка — порт-заглушка (sync не резолвится)
        await fixture.Gateway.PutAsync(fixture.Endpoint, $"/pgworker/portalloc/{cluster}",
            Portalloc.Serialize(new Dictionary<string, NodeAddress>
            {
                ["shard1/shard1a"] = new("127.0.0.1", new NodePorts(16001, patroniPort, 17001)),
                ["shard1/shard1b"] = new("127.0.0.1", new NodePorts(16002, patroniPort, 17002)),
            }), null, ct);
        await fixture.Gateway.PutAsync(fixture.Endpoint, $"/clusters/{cluster}/backup_password",
            password, null, ct);
        // Слоты per-instance: ensured на ОБОИХ DSN (тик создаст недостающие).
        return (patroni!, cluster);
    }

    // Двухнодовый снапшот (master-ключ → shard1a).
    private static ClusterSnapshot BuildTwoNodeSnap(string cluster = "c1") => new(
        new ClusterConfig(cluster, 2, cluster, null, ClusterState.Active),
        [new ShardSpec("shard1", 2, $"host=shard1a dbname={cluster}", "shard1a:17001",
        [
            new NodeSpec("shard1", "shard1a", NodeState.Running),
            new NodeSpec("shard1", "shard1b", NodeState.Running),
        ])],
        []);

    // AAA (AC3-контур): два источника (мастер + sync-standby) → ДВА per-node агента,
    // слоты ensured на ОБОИХ DSN, ключ wal содержит agents с двумя running-нодами.
    [Fact]
    public async Task DualArchivingTwoAgentsSlotsOnBothNodes()
    {
        // Arrange — двухнодовый кластер, Patroni-фейк с sync на shard1b
        var ct = TestContext.Current.CancellationToken;
        var patroni = await FakePatroni.StartAsync(SyncClusterJson, ct);
        await using var patroniOwner = patroni;
        var cluster = $"da1{Guid.NewGuid().ToString("N")[..6]}";
        await SeedTwoNodeAsync(new FakeWalSqlExecutor(), patroni, cluster);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor();
        var s3 = new FakeBackupS3();
        SeedSegments(s3, cluster, 1, 2);
        var driver = new StubScaleDriver();
        var process = BuildProcess(Options(), sql, s3, driver);

        // Act — тик
        var result = await process.TickAsync(BuildTwoNodeSnap(cluster), null, ct);

        // Assert — два per-node агента (мастер + sync)
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        driver.EnsuredBackupAgents.Should().Contain($"pgw-backup-wal-{cluster}-shard1-shard1a");
        driver.EnsuredBackupAgents.Should().Contain($"pgw-backup-wal-{cluster}-shard1-shard1b");
        // Слоты ensured на ОБОИХ DSN (инстансы независимы)
        sql.SlotsByDsn.GetOrAdd(SourceDsn(16001, cluster), _ => [])
            .Should().Contain("pgw_bkp_" + cluster + "_shard1");
        sql.SlotsByDsn.GetOrAdd(SourceDsn(16002, cluster), _ => [])
            .Should().Contain("pgw_bkp_" + cluster + "_shard1");
        // Ключ wal содержит agents с двумя running-нодами
        var wal = await ReadWal(cluster);
        wal.Should().NotBeNull("segments in S3 — verify writes the key");
        wal!.Agents.Should().NotBeNull();
        wal.Agents.Should().BeEquivalentTo(
        [
            new WalAgentState("shard1a", WalAgentPresence.Running),
            new WalAgentState("shard1b", WalAgentPresence.Running),
        ]);
    }

    // AAA (миграция §3.6 / AC7-сценарий): агент старого формата (имя без ноды,
    // running) → тик: старый снесён, per-node поднят; повторный тик идемпотентен.
    [Fact]
    public async Task MigrationOldAgentRemovedPerNodeRaised()
    {
        // Arrange — живой агент старого формата pgw-backup-wal-<C>-shard1
        var ct = TestContext.Current.CancellationToken;
        var patroni = await FakePatroni.StartAsync(SyncClusterJson, ct);
        await using var patroniOwner = patroni;
        var cluster = $"mg1{Guid.NewGuid().ToString("N")[..6]}";
        await SeedTwoNodeAsync(new FakeWalSqlExecutor(), patroni, cluster);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor();
        var s3 = new FakeBackupS3();
        SeedSegments(s3, cluster, 1, 2);
        var driver = new StubScaleDriver();
        driver.BackupAgentObjects.Add(new DockerContainer(
            "id-old", [$"/pgw-backup-wal-{cluster}-shard1"], "running", "img"));
        var process = BuildProcess(Options(), sql, s3, driver);

        // Act — тик миграции
        (await process.TickAsync(BuildTwoNodeSnap(cluster), null, ct)).IsSuccess.Should().BeTrue();

        // Assert — старый снесён, per-node поднят
        driver.RemovedBackupAgents.Should().Contain($"pgw-backup-wal-{cluster}-shard1");
        driver.EnsuredBackupAgents.Should().Contain($"pgw-backup-wal-{cluster}-shard1-shard1a");
        driver.EnsuredBackupAgents.Should().Contain($"pgw-backup-wal-{cluster}-shard1-shard1b");

        // Act/Assert — повторный тик идемпотентен (новых ensured нет)
        var ensuredBefore = driver.EnsuredBackupAgents.Count;
        (await process.TickAsync(BuildTwoNodeSnap(cluster), null, ct)).IsSuccess.Should().BeTrue();
        driver.EnsuredBackupAgents.Skip(ensuredBefore).Should().BeEmpty();
    }

    // AAA (смена источника): агент per-node мастера -shard1a; мастер сменился на
    // shard1b → агент -shard1a снесён (чужое имя), -shard1b поднят.
    [Fact]
    public async Task MasterChangedPerNodeAgentMoved()
    {
        // Arrange — тик 1: мастер shard1a, агенты per-node
        var ct = TestContext.Current.CancellationToken;
        var patroni = await FakePatroni.StartAsync(SyncClusterJson, ct);
        await using var patroniOwner = patroni;
        var cluster = $"sw1{Guid.NewGuid().ToString("N")[..6]}";
        await SeedTwoNodeAsync(new FakeWalSqlExecutor(), patroni, cluster);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor();
        var s3 = new FakeBackupS3();
        SeedSegments(s3, cluster, 1, 2);
        var driver = new StubScaleDriver();
        var process = BuildProcess(Options(), sql, s3, driver);
        (await process.TickAsync(BuildTwoNodeSnap(cluster), null, ct)).IsSuccess.Should().BeTrue();

        // Act — master-ключ шарда → shard1b: второй тик
        var switched = new ClusterSnapshot(
            new ClusterConfig(cluster, 2, cluster, null, ClusterState.Active),
            [new ShardSpec("shard1", 2, $"host=shard1b dbname={cluster}", "shard1b:17002",
            [
                new NodeSpec("shard1", "shard1a", NodeState.Running),
                new NodeSpec("shard1", "shard1b", NodeState.Running),
            ])],
            []);
        driver.EnsuredBackupAgents.Clear();
        (await process.TickAsync(switched, null, ct)).IsSuccess.Should().BeTrue();

        // Assert — старый агент снесён, поднят на новом мастере
        driver.RemovedBackupAgents.Should().Contain($"pgw-backup-wal-{cluster}-shard1-shard1a");
        driver.EnsuredBackupAgents.Should().Contain($"pgw-backup-wal-{cluster}-shard1-shard1b");
    }

    // AAA («слот исчез» — правило всех нод, t27 §3.3 п.3): слот жив только на
    // sync-ноде → тик НЕ пишет BROKEN (ensure на мастере); слот исчез на обеих при
    // живой цепочке → BROKEN + recreateSlot.
    [Fact]
    public async Task SlotDisappearedAllNodesRule()
    {
        // Arrange — два источника; слот жив ТОЛЬКО на sync-ноде (16002)
        var ct = TestContext.Current.CancellationToken;
        var patroni = await FakePatroni.StartAsync(SyncClusterJson, ct);
        await using var patroniOwner = patroni;
        var cluster = $"sg1{Guid.NewGuid().ToString("N")[..6]}";
        await SeedTwoNodeAsync(new FakeWalSqlExecutor(), patroni, cluster);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor();
        sql.SlotsByDsn.GetOrAdd(SourceDsn(16002, cluster), _ => [])
            .Add($"pgw_bkp_{cluster}_shard1");
        var s3 = new FakeBackupS3();
        SeedSegments(s3, cluster, 1, 2);
        var writer = new WalStatusWriter(fixture.Gateway, [fixture.Endpoint]);
        var liveWal = new WalStreamState(
            WalStreamStatus.Active, $"pgw_bkp_{cluster}_shard1", "shard1a",
            "000000010000000000000001", "000000010000000000000002",
            "000000010000000000000002", 1757500000, 0, null);
        await writer.WriteIfChangedAsync(cluster, "shard1", liveWal, ct);
        var driver = new StubScaleDriver();
        var process = BuildProcess(Options(verify: 3600), sql, s3, driver);
        var backups = new ClusterBackups(cluster, null,
            new Dictionary<string, ShardBackups>
            {
                ["shard1"] = new(FullShard("000000010000000000000001").Full, liveWal),
            });

        // Act — тик при живом sync-слоте
        (await process.TickAsync(BuildTwoNodeSnap(cluster), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — не BROKEN; слот ensured на мастере (16001)
        var walAfterSyncAlive = await ReadWal(cluster);
        walAfterSyncAlive!.State.Should().NotBe(WalStreamStatus.Broken,
            "a live slot on any node — not BROKEN (t27 §3.3 item 3)");
        sql.SlotsByDsn.GetOrAdd(SourceDsn(16001, cluster), _ => [])
            .Should().Contain($"pgw_bkp_{cluster}_shard1", "ensure on the source where the slot is absent");

        // Act 2 — слот исчез на ОБОИХ (границы словаря чистим) при живой цепочке
        sql.SlotsByDsn.Clear();
        driver.EnsuredBackupAgents.Clear();
        (await process.TickAsync(BuildTwoNodeSnap(cluster), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — BROKEN + recreateSlot (слот пересоздан на мастере)
        var walGone = await ReadWal(cluster);
        walGone!.State.Should().Be(WalStreamStatus.Broken);
        walGone.Error.Should().Contain("slot");
        sql.SlotsByDsn.Values.Should().Contain(s => s.Contains($"pgw_bkp_{cluster}_shard1"),
            "recreateSlot immediate+reserved on the master (t07)");
    }

    // AAA (t19 AC1): lost-слот мастера при живом sync — тик НЕ пишет BROKEN,
    // слот мастера пересоздан (drop+create), журнальная фаза slot-recreate/<X>.
    [Fact]
    public async Task LostMasterSlotWithLiveSyncNotBrokenRecreateJournal()
    {
        // Arrange — два источника (мастер shard1a + sync shard1b); слот мастера
        // существует, но lost; слот sync жив; ключ wal ACTIVE (цепочка жива)
        var ct = TestContext.Current.CancellationToken;
        var patroni = await FakePatroni.StartAsync(SyncClusterJson, ct);
        await using var patroniOwner = patroni;
        var cluster = $"sl1{Guid.NewGuid().ToString("N")[..6]}";
        await SeedTwoNodeAsync(new FakeWalSqlExecutor(), patroni, cluster);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor();
        var masterDsn = SourceDsn(16001, cluster);
        sql.SlotsByDsn.GetOrAdd(masterDsn, _ => []).Add($"pgw_bkp_{cluster}_shard1");
        sql.LostByDsn.GetOrAdd(masterDsn, _ => []).Add($"pgw_bkp_{cluster}_shard1");
        sql.SlotsByDsn.GetOrAdd(SourceDsn(16002, cluster), _ => []).Add($"pgw_bkp_{cluster}_shard1");
        var s3 = new FakeBackupS3();
        SeedSegments(s3, cluster, 1, 2);
        var driver = new StubScaleDriver();
        var writer = new WalStatusWriter(fixture.Gateway, [fixture.Endpoint]);
        var liveWal = new WalStreamState(
            WalStreamStatus.Active, $"pgw_bkp_{cluster}_shard1", "shard1a",
            "000000010000000000000001", "000000010000000000000002",
            "000000010000000000000002", 1757500000, 0, null);
        await writer.WriteIfChangedAsync(cluster, "shard1", liveWal, ct);
        var process = BuildProcess(Options(verify: 3600), sql, s3, driver);
        var backups = new ClusterBackups(cluster, null,
            new Dictionary<string, ShardBackups>
            {
                ["shard1"] = new(FullShard("000000010000000000000001").Full, liveWal),
            });

        // Act
        (await process.TickAsync(BuildTwoNodeSnap(cluster), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — не BROKEN; слот мастера пересоздан (drop+create); журнал пишет
        // фазу ДО мутации; агент мастера не снимался
        var wal = await ReadWal(cluster);
        wal!.State.Should().NotBe(WalStreamStatus.Broken,
            "lost on one source while the other is alive — not BROKEN (spec §3.2)");
        sql.Calls.Should().Contain(c => c.Op == "drop" && c.Dsn == masterDsn,
            "FakeSql records the drop of the lost slot");
        sql.Calls.Should().Contain(c => c.Op == "create" && c.Dsn == masterDsn,
            "FakeSql records the create of the recreated slot");
        sql.LostByDsn[masterDsn].Should().NotContain($"pgw_bkp_{cluster}_shard1",
            "after recreate the slot is alive");
        var journal = await fixture.Gateway.GetAsync(
            fixture.Endpoint, $"/pgworker/work/{cluster}", ct);
        journal.Value!.Value.Should().Contain("slot-recreate/shard1");
        journal.Value.Value.Should().Contain("wal_status=lost");
        driver.RemovedBackupAgents.Should().NotContain(
            n => n.Contains("shard1", StringComparison.Ordinal), "agents are not touched");
    }

    // AAA (фаза A2 — «повторный тик на здоровом слоте — нулевые мутации»):
    // тик на здоровых (не lost) слотах — нулевые мутации слотов
    // и никаких журнальных фаз slot-recreate.
    [Fact]
    public async Task TickOnHealthySlotsZeroMutations()
    {
        // Arrange — два источника, оба слота живы; ключ ACTIVE; цепочка сплошная
        var ct = TestContext.Current.CancellationToken;
        var patroni = await FakePatroni.StartAsync(SyncClusterJson, ct);
        await using var patroniOwner = patroni;
        var cluster = $"sl2{Guid.NewGuid().ToString("N")[..6]}";
        await SeedTwoNodeAsync(new FakeWalSqlExecutor(), patroni, cluster);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor();
        sql.SlotsByDsn.GetOrAdd(SourceDsn(16001, cluster), _ => []).Add($"pgw_bkp_{cluster}_shard1");
        sql.SlotsByDsn.GetOrAdd(SourceDsn(16002, cluster), _ => []).Add($"pgw_bkp_{cluster}_shard1");
        var s3 = new FakeBackupS3();
        SeedSegments(s3, cluster, 1, 2);
        var driver = new StubScaleDriver();
        var process = BuildProcess(Options(), sql, s3, driver);
        var backups = new ClusterBackups(cluster, null,
            new Dictionary<string, ShardBackups>
            {
                ["shard1"] = FullShard("000000010000000000000001"),
            });

        // Act — два тика (контроль + супервиз)
        (await process.TickAsync(BuildTwoNodeSnap(cluster), backups, ct)).IsSuccess.Should().BeTrue();
        (await process.TickAsync(BuildTwoNodeSnap(cluster), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — ни одного вызова мутации слота; журнал без slot-recreate
        sql.Calls.Should().BeEmpty("a healthy slot is not touched (phase A2)");
        var journal = await fixture.Gateway.GetAsync(
            fixture.Endpoint, $"/pgworker/work/{cluster}", ct);
        (journal.Value?.Value ?? "").Should().NotContain("slot-recreate");
    }

    // AAA (t19 AC2): lost на ВСЕХ источниках при живом (ACTIVE) ключе — BROKEN,
    // recreateSlot на мастере, error различает lost от «исчез».
    [Fact]
    public async Task LostSlotsOnAllSourcesWithLiveChainBroken()
    {
        // Arrange — оба слота существуют и lost; ключ ACTIVE; агент жив
        var ct = TestContext.Current.CancellationToken;
        var patroni = await FakePatroni.StartAsync(SyncClusterJson, ct);
        await using var patroniOwner = patroni;
        var cluster = $"sl3{Guid.NewGuid().ToString("N")[..6]}";
        await SeedTwoNodeAsync(new FakeWalSqlExecutor(), patroni, cluster);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor();
        foreach (var port in new[] { 16001, 16002 })
        {
            var dsn = SourceDsn(port, cluster);
            sql.SlotsByDsn.GetOrAdd(dsn, _ => []).Add($"pgw_bkp_{cluster}_shard1");
            sql.LostByDsn.GetOrAdd(dsn, _ => []).Add($"pgw_bkp_{cluster}_shard1");
        }
        var s3 = new FakeBackupS3();
        SeedSegments(s3, cluster, 1, 2);
        var driver = new StubScaleDriver();
        driver.BackupAgentObjects.Add(new DockerContainer(
            $"id-agent-{cluster}", [$"/pgw-backup-wal-{cluster}-shard1-shard1a"], "running", "img"));
        var writer = new WalStatusWriter(fixture.Gateway, [fixture.Endpoint]);
        var liveWal = new WalStreamState(
            WalStreamStatus.Active, $"pgw_bkp_{cluster}_shard1", "shard1a",
            "000000010000000000000001", "000000010000000000000002",
            "000000010000000000000002", 1757500000, 0, null);
        await writer.WriteIfChangedAsync(cluster, "shard1", liveWal, ct);
        var process = BuildProcess(Options(), sql, s3, driver);
        var backups = new ClusterBackups(cluster, null,
            new Dictionary<string, ShardBackups>
            {
                ["shard1"] = new(FullShard("000000010000000000000001").Full, liveWal),
            });

        // Act
        (await process.TickAsync(BuildTwoNodeSnap(cluster), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — BROKEN, error содержит lost; слот мастера пересоздан (recreateSlot)
        var wal = await ReadWal(cluster);
        wal!.State.Should().Be(WalStreamStatus.Broken);
        wal.Error.Should().Contain("lost", "the error distinguishes lost from disappeared (spec §3.2)");
        sql.Calls.Should().Contain(c => c.Op == "drop" && c.Dsn == SourceDsn(16001, cluster),
            "BreakAsync recreateSlot: the master's lost slot is recreated immediate+reserved");
        driver.RemovedBackupAgents.Should().NotBeEmpty("BROKEN stops the agents");
    }

    // AAA (t19 AC3): ensure-ветка (ключ wal = BROKEN) + существующий lost-слот —
    // EnsureSlotAlive recreates (не пропускает по идемпотентности), новый BROKEN не пишется.
    [Fact]
    public async Task EnsureBranchBrokenKeyLostSlotRecreate()
    {
        // Arrange — однонодовый кластер; wal-состояние BROKEN передано тиком (как
        // в cb-кейсах: и ключ в etcd, и backups-аргумент); слот есть, но lost
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("sl4");
        (await _claims.TryClaimClusterAsync("sl4", ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor();
        var masterDsn = SourceDsn(16001, "sl4");
        sql.SlotsByDsn.GetOrAdd(masterDsn, _ => []).Add("pgw_bkp_sl4_shard1");
        sql.LostByDsn.GetOrAdd(masterDsn, _ => []).Add("pgw_bkp_sl4_shard1");
        var s3 = new FakeBackupS3(); // пустой префикс wal/
        var driver = new StubScaleDriver();
        var brokenWal = new WalStreamState(
            WalStreamStatus.Broken, "pgw_bkp_sl4_shard1", "shard1a",
            "000000010000000000000001", "000000010000000000000001",
            "000000010000000000000001", 1757500000, null, "WAL chain gap");
        var writer = new WalStatusWriter(fixture.Gateway, [fixture.Endpoint]);
        await writer.WriteIfChangedAsync("sl4", "shard1", brokenWal, ct);
        var process = BuildProcess(Options(), sql, s3, driver);
        var backups = new ClusterBackups("sl4", null,
            new Dictionary<string, ShardBackups> { ["shard1"] = new([], brokenWal) });

        // Act
        var result = await process.TickAsync(BuildSnap("sl4"), backups, ct);

        // Assert — ensure-ветка полечила lost (drop+create), тик успешен
        result.IsSuccess.Should().BeTrue();
        sql.Calls.Should().Contain(c => c.Op == "drop" && c.Dsn == masterDsn,
            "lost in the ensure branch is healed by recreate, not skipped (criterion 3)");
        sql.Calls.Should().Contain(c => c.Op == "create" && c.Dsn == masterDsn);
        sql.LostByDsn[masterDsn].Should().NotContain("pgw_bkp_sl4_shard1");

        // Новый BROKEN не пишется лечением слота: BreakAsync-ветка не стреляла —
        // агенты не снимались, error-текст lost-BROKEN не появился. (Сам ключ при
        // этом переписывается ШТАТНЫМ контролем BROKEN-ключа — контроль каждый тик,
        // наблюдение из фактов ключа (ratchet chain_start + last_uploaded); это
        // pre-existing семантика контроля, не ветки слота.)
        driver.RemovedBackupAgents.Should().BeEmpty(
            "BreakAsync did not fire — the ensure branch heals lost without a BROKEN record");
        var walAfter = await ReadWal("sl4");
        walAfter!.Error.Should().NotContain("retake the full backup",
            "no new BROKEN record from the slot branch — healing is silent (criterion 3)");
    }

    // AAA («sync появился»): тик 1 single-node (sync не резолвится) → один агент;
    // тик 2 с sync → второй агент поднят, первый жив (не пересоздан).
    [Fact]
    public async Task SyncAppearedSecondAgentFirstStaysAlive()
    {
        // Arrange — patroni-порт заглушка (sync не резолвится): тик 1
        var ct = TestContext.Current.CancellationToken;
        var cluster = $"ap1{Guid.NewGuid().ToString("N")[..6]}";
        await SeedTwoNodeAsync(new FakeWalSqlExecutor(), null, cluster);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor();
        var s3 = new FakeBackupS3();
        SeedSegments(s3, cluster, 1, 2);
        var driver = new StubScaleDriver();
        var process = BuildProcess(Options(), sql, s3, driver);
        (await process.TickAsync(BuildTwoNodeSnap(cluster), null, ct)).IsSuccess.Should().BeTrue();

        // Assert тика 1 — только агент мастера
        driver.EnsuredBackupAgents.Should().Contain($"pgw-backup-wal-{cluster}-shard1-shard1a");
        driver.EnsuredBackupAgents.Should().NotContain(n => n.EndsWith("-shard1b", StringComparison.Ordinal));

        // Act — patroni с sync появился: тик 2
        var patroni = await FakePatroni.StartAsync(SyncClusterJson, ct);
        await using var patroniOwner = patroni;
        await fixture.Gateway.PutAsync(fixture.Endpoint, $"/pgworker/portalloc/{cluster}",
            Portalloc.Serialize(new Dictionary<string, NodeAddress>
            {
                ["shard1/shard1a"] = new("127.0.0.1", new NodePorts(16001, patroni.Port, 17001)),
                ["shard1/shard1b"] = new("127.0.0.1", new NodePorts(16002, patroni.Port, 17002)),
            }), null, ct);
        driver.EnsuredBackupAgents.Clear();
        (await process.TickAsync(BuildTwoNodeSnap(cluster), null, ct)).IsSuccess.Should().BeTrue();

        // Assert — второй агент поднят; первый жив (не снесён, не пересоздан)
        driver.EnsuredBackupAgents.Should().Contain($"pgw-backup-wal-{cluster}-shard1-shard1b");
        driver.RemovedBackupAgents.Should().NotContain(
            n => n.StartsWith($"pgw-backup-wal-{cluster}-shard1-shard1a", StringComparison.Ordinal));
        driver.BackupAgentObjects.Should().Contain(c =>
            c.State == "running" && c.Names.Contains($"/pgw-backup-wal-{cluster}-shard1-shard1a"));
    }

    // AAA (регресс t27-E2E AC3): агент СОСЕДНЕГО шарда кластера (листинг по
    // кластеру) не триггерит recreate-цикл этого шарда: foreign-детект —
    // только в рамках шарда.
    [Fact]
    public async Task NeighborShardAgentDoesNotTriggerRecreate()
    {
        // Arrange — двухнодовый шард1 (sync на shard1b) + ЖИВОЙ агент шарда shard2
        var ct = TestContext.Current.CancellationToken;
        var patroni = await FakePatroni.StartAsync(SyncClusterJson, ct);
        await using var patroniOwner = patroni;
        var cluster = $"nb1{Guid.NewGuid().ToString("N")[..6]}";
        await SeedTwoNodeAsync(new FakeWalSqlExecutor(), patroni, cluster);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor();
        var s3 = new FakeBackupS3();
        SeedSegments(s3, cluster, 1, 2);
        var driver = new StubScaleDriver();
        driver.BackupAgentObjects.Add(new DockerContainer(
            "id-nb-shard2", [$"/pgw-backup-wal-{cluster}-shard2-shard2a"], "running", "img"));
        var process = BuildProcess(Options(), sql, s3, driver);
        // Снапшот с ОБОИМИ шардами (shard2 — как в реальном кластере, dsn задан)
        var snap = new ClusterSnapshot(
            new ClusterConfig(cluster, 2, cluster, null, ClusterState.Active),
            [
                new ShardSpec("shard1", 2, $"host=shard1a dbname={cluster}", "shard1a:17001",
                [
                    new NodeSpec("shard1", "shard1a", NodeState.Running),
                    new NodeSpec("shard1", "shard1b", NodeState.Running),
                ]),
                new ShardSpec("shard2", 2, $"host=shard2a dbname={cluster}", "shard2a:17011",
                [
                    new NodeSpec("shard2", "shard2a", NodeState.Running),
                    new NodeSpec("shard2", "shard2b", NodeState.Running),
                ]),
            ],
            []);

        // Act — два тика (второй — после стабилизации: agents обеих нод shard1)
        (await process.TickAsync(snap, null, ct)).IsSuccess.Should().BeTrue();
        (await process.TickAsync(snap, null, ct)).IsSuccess.Should().BeTrue();

        // Assert — shard1-агенты per-node созданы; shard2-агент НЕ снесён
        driver.EnsuredBackupAgents.Should().Contain($"pgw-backup-wal-{cluster}-shard1-shard1a");
        driver.EnsuredBackupAgents.Should().Contain($"pgw-backup-wal-{cluster}-shard1-shard1b");
        driver.RemovedBackupAgents.Should().NotContain(
            n => n.Contains("shard2", StringComparison.Ordinal),
            "a neighbor shard's agent is not foreign to this shard (regression of the eternal recreate)");
    }

    // AAA (t27 Task 13, канон §3 fallback): контроль видит TLI-переход без
    // history-объекта → docker-exec ноды (base64) → put wal/<tli>.history в S3;
    // повторный тик — идемпотентен (exec не повторяется).
    [Fact]
    public async Task HistoryFallbackVerifyReportsHistoryOnTliSwitch()
    {
        // Arrange — сегменты TLI 1 (…0001, …0002) и TLI 2 (…0002); history НЕТ;
        // exec ноды возвращает base64 синтетики history-файла
        var ct = TestContext.Current.CancellationToken;
        var cluster = $"hb1{Guid.NewGuid().ToString("N")[..6]}";
        await SeedTwoNodeAsync(new FakeWalSqlExecutor(), null, cluster);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor();
        var s3 = new FakeBackupS3();
        s3.Objects.Add((cluster, "shard1", "000000010000000000000001"));
        s3.Objects.Add((cluster, "shard1", "000000010000000000000002"));
        s3.Objects.Add((cluster, "shard1", "000000020000000000000002"));
        var driver = new StubScaleDriver();
        driver.ExecNodeResult = Convert.ToBase64String("1\t0/20000060\n"u8.ToArray());
        var process = BuildProcess(Options(), sql, s3, driver);

        // Act — тик 1: контроль находит TLI-переход без history → докладывает
        (await process.TickAsync(BuildTwoNodeSnap(cluster), null, ct)).IsSuccess.Should().BeTrue();

        // Assert — history-объект в S3
        var execCalls1 = driver.ExecNodeCalls;
        execCalls1.Should().BeGreaterThan(0, "node exec for the history file");
        var listed = await s3.ListWalAsync(cluster, "shard1", ct: ct);
        listed.Value.Should().Contain(o => o.Name == "00000002.history",
            "wal/00000002.history is reported by verify (AC5 fallback)");

        // Act/Assert — повторный тик: history уже в S3 — exec не повторяется
        (await process.TickAsync(BuildTwoNodeSnap(cluster), null, ct)).IsSuccess.Should().BeTrue();
        driver.ExecNodeCalls.Should().Be(execCalls1, "idempotency: history already uploaded");
    }

    // AAA (t27, AC5): префикс wal/ БЕЗ сегментов TLI 1 (контур стартовал уже на
    // TLI 2: промотка до первого наблюдения) — history МИНИМАЛЬНОГО TLI набора
    // (>1) обязана докладываться: для TLI>1 history-файл существует и нужен
    // склейке (не бывает только у TLI 1). Прежнее правило «все TLI кроме
    // минимального» молча пропускало этот случай — AC5-факт E2E.
    [Fact]
    public async Task HistoryFallbackMinimalTliAbove1ReportsHistory()
    {
        // Arrange — сегменты ТОЛЬКО TLI 2; history НЕТ; exec возвращает синтетику
        var ct = TestContext.Current.CancellationToken;
        var cluster = $"hb2{Guid.NewGuid().ToString("N")[..6]}";
        await SeedTwoNodeAsync(new FakeWalSqlExecutor(), null, cluster);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor();
        var s3 = new FakeBackupS3();
        s3.Objects.Add((cluster, "shard1", "000000020000000000000023"));
        s3.Objects.Add((cluster, "shard1", "000000020000000000000024"));
        var driver = new StubScaleDriver();
        driver.ExecNodeResult = Convert.ToBase64String("2\t0/25000060\n"u8.ToArray());
        var process = BuildProcess(Options(), sql, s3, driver);

        // Act — тик: минимальный TLI набора = 2 (>1) без history → доклад
        (await process.TickAsync(BuildTwoNodeSnap(cluster), null, ct)).IsSuccess.Should().BeTrue();

        // Assert — history-объект в S3
        driver.ExecNodeCalls.Should().BeGreaterThan(0, "node exec for the history file");
        var listed = await s3.ListWalAsync(cluster, "shard1", ct: ct);
        listed.Value.Should().Contain(o => o.Name == "00000002.history",
            "history of the minimal TLI>1 stitches the switch — it is reported (AC5)");
    }

    // AAA: t14 — контрольный проход отдаёт ОБЕИМ наблюдателям факт одного
    // прохода: lag (сегменты) и uploaded-age (now − last_uploaded_unix ≥ 0)
    [Fact]
    public async Task VerifyObserversReceiveLagAndUploadAge()
    {
        // Arrange — полный COMPLETED wal_start=..01; S3: сегменты 1..3; мастер 0/3000000
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("cw1");
        (await _claims.TryClaimClusterAsync("cw1", ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor { Current = ("0/3000000", 1) };
        var s3 = new FakeBackupS3();
        SeedSegments(s3, "cw1", 1, 3);
        var lags = new List<(string C, string S, long? Lag)>();
        var ages = new List<(string C, string S, long? Age)>();
        var process = BuildProcess(Options(), sql, s3, new StubScaleDriver(),
            uploadedAgeObserver: (c, s, a) => ages.Add((c, s, a)),
            lagObserver: (c, s, l) => lags.Add((c, s, l)));
        var backups = new ClusterBackups("cw1", null,
            new Dictionary<string, ShardBackups> { ["shard1"] = FullShard("000000010000000000000001") });

        // Act — контроль due (VerifyIntervalSec=0)
        (await process.TickAsync(BuildSnap("cw1"), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — по одному вызову на наблюдателя: lag посчитан, возраст ≥ 0
        lags.Should().ContainSingle(t => t.C == "cw1" && t.S == "shard1" && t.Lag.HasValue);
        ages.Should().ContainSingle(t => t.C == "cw1" && t.S == "shard1")
            .Which.Age.Should().BeGreaterThanOrEqualTo(0);
    }

    // AAA: t14 — BROKEN (дыра цепочки) снимает обе серии: наблюдатели получают null
    [Fact]
    public async Task VerifyGapBrokenDropsSeriesOfBothObservers()
    {
        // Arrange — wal_start=..01; S3: 1 и 3 (дыра на ..02)
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("cw2");
        (await _claims.TryClaimClusterAsync("cw2", ct)).Value.Should().BeTrue();
        var s3 = new FakeBackupS3();
        SeedSegments(s3, "cw2", 1, 1);
        s3.Objects.Add(("cw2", "shard1", "000000010000000000000003"));
        var lags = new List<(string C, string S, long? Lag)>();
        var ages = new List<(string C, string S, long? Age)>();
        var process = BuildProcess(Options(), new FakeWalSqlExecutor(), s3, new StubScaleDriver(),
            uploadedAgeObserver: (c, s, a) => ages.Add((c, s, a)),
            lagObserver: (c, s, l) => lags.Add((c, s, l)));
        var backups = new ClusterBackups("cw2", null,
            new Dictionary<string, ShardBackups> { ["shard1"] = FullShard("000000010000000000000001") });

        // Act
        (await process.TickAsync(BuildSnap("cw2"), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — BROKEN: ровно один вызов на наблюдателя, оба null (серии исчезают)
        (await ReadWal("cw2"))!.State.Should().Be(WalStreamStatus.Broken);
        lags.Should().ContainSingle(t => t.C == "cw2" && t.Lag == null);
        ages.Should().ContainSingle(t => t.C == "cw2" && t.Age == null);
    }
}
