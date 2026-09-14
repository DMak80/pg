using FluentAssertions;
using PgWorker.Backups;
using PgWorker.Backups.Sql;
using PgWorker.Core.Model;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.Etcd.Parsing;
using PgWorker.IntegrationTests.Etcd;
using PgWorker.Core.Templates;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Probes;
using Xunit;

namespace PgWorker.IntegrationTests.Backups;

// Интеграции WalStreamProcess (t03 spec Ф3): реальный etcd (статусы/журнал) +
// фейки docker-агентов/SQL/S3. Снапшот кластера строится руками.
[Collection(EtcdCollection.Name)]
public class WalStreamProcessTests(EtcdFixture fixture)
{
    private readonly ClaimStore _claims = new([fixture.Endpoint], fixture.Gateway, TimeProvider.System);

    // ── Хелперы Arrange ──

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
                ["shard1/shard1a"] = new("localhost", new NodePorts(16001, 18001, 17001)),
            }), null, ct);
        await fixture.Gateway.PutAsync(fixture.Endpoint, $"/clusters/{cluster}/backup_password",
            password, null, ct);
    }

    private WalStreamProcess BuildProcess(
        BackupsRuntimeOptions? options,
        FakeWalSqlExecutor sql,
        FakeBackupS3 s3,
        StubScaleDriver driver,
        TimeProvider? clock = null)
        => new(
            fixture.Gateway, [fixture.Endpoint], driver,
            new ShardEndpoints(fixture.Gateway, [fixture.Endpoint], new ShardProbe(new HttpClient())),
            sql, s3,
            new WalStatusWriter(fixture.Gateway, [fixture.Endpoint]),
            _claims, new WorkJournal(fixture.Gateway, [fixture.Endpoint]),
            () => options,
            new InstallSecrets("su-pw", "sb-pw", "adm-pw", "mov-pw"),
            clock ?? TimeProvider.System);

    // Тест-опции: VerifyIntervalSec=0 — контроль выполняется КАЖДЫМ тиком (AAA).
    private static BackupsRuntimeOptions Options(int verify = 0, int lag = 1024, int stale = 300) => new(
        Enabled: true,
        S3Endpoint: "http://minio",
        S3AdvertisedEndpoint: "http://minio-agent",
        S3Bucket: "bkt",
        S3AccessKey: "ak",
        S3SecretKey: "sk",
        S3PathStyle: true,
        JobImage: "pgworker-backup:test",
        StagingDir: "/backup-staging",
        WalVerifyIntervalSec: verify,
        WalLagMaxSegments: lag,
        WalStaleSec: stale);

    // Полный COMPLETED с wal_start_segment (для chain_start от полного).
    private static ShardBackups FullShard(string walStart, WalStreamState? wal = null) => new(
        [new FullBackupState("20260910120000Z", FullBackupStatus.Completed, "shard1a",
            BackupSourceRole.Replica, 1757500000, 1757500300, walStart, 1024, null, null)],
        wal);

    [Fact]
    public async Task Тик_поднимает_агента_ensure_слот_и_ждет_первый_объект()
    {
        // Arrange — полных нет, S3 пуст, ключ wal нет; клэйм кластера наш
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("c1");
        (await _claims.TryClaimClusterAsync("c1", ct)).Value.Should().BeTrue("клэйм — предусловие тика");
        var sql = new FakeWalSqlExecutor();
        var s3 = new FakeBackupS3();
        var driver = new StubScaleDriver();
        var process = BuildProcess(Options(), sql, s3, driver);

        // Act — первый тик: слот создан, контейнер агента создан+запущен,
        //       ключ wal НЕ пишется (нет объектов — spec §3.2 шаг 8)
        var result = await process.TickAsync(BuildSnap(), null, ct);

        // Assert
        result.IsSuccess.Should().BeTrue();
        driver.EnsuredBackupAgents.Should().Contain("pgw-backup-wal-c1-shard1");
        // Контракт (ревью Ф7 №1): процесс НЕ назначает сеть — драйвер владеет
        // pgw-net и проставляет её при create (юнит-тест ClusterDriverTests).
        driver.EnsuredAgentSpecs.Should().ContainSingle().Which.Network.Should().BeNull();
        // Рестарт-политики у агента нет: docker не лупит — супервиз тика
        // пересоздаёт exited-агента (иначе луп молотит на снесённом мастере)
        driver.EnsuredAgentSpecs.Should().ContainSingle().Which.RestartPolicy.Should().Be("no");
        sql.Slots.Should().ContainKey("pgw_bkp_c1_shard1");
        var wal = await fixture.Gateway.GetAsync(
            fixture.Endpoint, "/pgworker/backups/c1/shard1/wal", ct);
        wal.Value.Should().BeNull("ключ не пишется до первого наблюдения (spec §3.2 п.8)");
    }

    [Fact]
    public async Task Тик_идемпотентен_агент_не_пересоздается()
    {
        // Arrange — агент уже создан (первый тик)
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("c1");
        (await _claims.TryClaimClusterAsync("c1", ct)).Value.Should().BeTrue("клэйм — предусловие тика");
        var sql = new FakeWalSqlExecutor();
        var s3 = new FakeBackupS3();
        var driver = new StubScaleDriver();
        var process = BuildProcess(Options(), sql, s3, driver);
        (await process.TickAsync(BuildSnap(), null, ct)).IsSuccess.Should().BeTrue();

        // Act — второй тик
        (await process.TickAsync(BuildSnap(), null, ct)).IsSuccess.Should().BeTrue();

        // Assert — EnsuredBackupAgents без дублей; ключ wal не пишется
        driver.EnsuredBackupAgents.Should().ContainSingle(n => n == "pgw-backup-wal-c1-shard1");
        var wal = await fixture.Gateway.GetAsync(
            fixture.Endpoint, "/pgworker/backups/c1/shard1/wal", ct);
        wal.Value.Should().BeNull();
    }

    [Fact]
    public async Task Тик_без_кредов_t02_пропускает_шард_с_journal_заметкой()
    {
        // Arrange — backup_password отсутствует (t02 не смержена/ensure не прошёл)
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("c1", password: "");
        await fixture.Gateway.DeleteAsync(
            fixture.Endpoint, "/clusters/c1/backup_password", prefix: false, ct);
        (await _claims.TryClaimClusterAsync("c1", ct)).Value.Should().BeTrue("клэйм — предусловие тика");
        var sql = new FakeWalSqlExecutor();
        var driver = new StubScaleDriver();
        var process = BuildProcess(Options(), sql, new FakeBackupS3(), driver);

        // Act
        var result = await process.TickAsync(BuildSnap(), null, ct);

        // Assert — transient-пропуск: ни слота, ни агента
        result.IsSuccess.Should().BeTrue();
        sql.Slots.Should().BeEmpty("агент/слот не поднимаются без кредов (spec §3.2 п.1)");
        driver.EnsuredBackupAgents.Should().BeEmpty();
        var journal = await fixture.Gateway.GetAsync(
            fixture.Endpoint, "/pgworker/work/c1", ct);
        journal.Value!.Value.Should().Contain("waiting-backup-password");
    }

    [Fact]
    public async Task Тик_Enabled_false_останавливает_агентов_и_пишет_STOPPED()
    {
        // Arrange — живой агент + ключ wal ACTIVE от прошлого прохода
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("c1");
        (await _claims.TryClaimClusterAsync("c1", ct)).Value.Should().BeTrue("клэйм — предусловие тика");
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
        driver.EnsuredBackupAgents.Should().Contain("pgw-backup-wal-c1-shard1");

        // Act — тик с ВЫКЛЮЧЕННОЙ подсистемой (Enabled=false → runtime()=null)
        var stopped = BuildProcess(null, sql, s3, driver);
        var result = await stopped.TickAsync(BuildSnap(), null, ct);

        // Assert — агенты вниз, ключ финально STOPPED
        result.IsSuccess.Should().BeTrue();
        driver.RemovedBackupAgents.Should().Contain("pgw-backup-wal-c1-shard1");
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
    public async Task Тик_скипает_шард_с_активным_restore()
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
            new ShardEndpoints(fixture.Gateway, [fixture.Endpoint], new ShardProbe(new HttpClient())),
            sql, s3, new WalStatusWriter(fixture.Gateway, [fixture.Endpoint]),
            _claims, new WorkJournal(fixture.Gateway, [fixture.Endpoint]),
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
        sql.Slots.Should().BeEmpty();
        var wal = await fixture.Gateway.GetAsync(
            fixture.Endpoint, "/pgworker/backups/cr9/shard1/wal", ct);
        wal.Value.Should().BeNull("шард в restore — контуры бэкапов молчат");
    }

    [Fact]
    public async Task Контроль_сплошная_цепочка_пишет_ACTIVE_и_chain_start_от_полного()
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
            new ShardEndpoints(fixture.Gateway, [fixture.Endpoint], new ShardProbe(new HttpClient())),
            sql, s3, new WalStatusWriter(fixture.Gateway, [fixture.Endpoint]),
            _claims, new WorkJournal(fixture.Gateway, [fixture.Endpoint]),
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
    public async Task Контроль_дыра_пишет_BROKEN_границы_стоп_и_блокировка_подъема_AC4a()
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
        driver.EnsuredBackupAgents.Should().BeEmpty("ChainBroken блокирует подъём (ревью Ф4-2 №1)");
    }

    [Fact]
    public async Task Контроль_инвалидация_слота_пишет_BROKEN_стоп_агента_AC4b()
    {
        // Arrange — ключ wal ACTIVE; FakeSql.Slots пуст (слот исчез)
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("cc3");
        (await _claims.TryClaimClusterAsync("cc3", ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor();
        var s3 = new FakeBackupS3();
        SeedSegments(s3, "cc3", 1, 2);
        var driver = new StubScaleDriver();
        driver.BackupAgentObjects.Add(new PgWorker.Docker.Engine.DockerContainer(
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
        wal.Error.Should().Contain("слот");
        wal.ChainStartSegment.Should().Be("000000010000000000000002", "граница разрыва — last_uploaded на момент обнаружения");
        driver.RemovedBackupAgents.Should().Contain("pgw-backup-wal-cc3-shard1");
        sql.Slots.Should().ContainKey("pgw_bkp_cc3_shard1", "слот пересоздаётся при BROKEN (spec §3.2)");
    }

    [Fact]
    public async Task Контроль_новый_полный_выше_дыры_восстанавливает_ACTIVE_тем_же_тиком_AC4c()
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
            "000000010000000000000001", 1757500000, null, "дыра WAL-цепочки"), ct);
        var process = BuildProcess(Options(), sql, s3, driver);
        var backups = new ClusterBackups("cc4", null,
            new Dictionary<string, ShardBackups> { ["shard1"] = FullShard("000000010000000000000005") });

        // Act — ОДИН тик (контроль due)
        (await process.TickAsync(BuildSnap("cc4"), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — ACTIVE с новым chain_start; агент поднят ТЕМ ЖЕ тиком
        var wal = await ReadWal("cc4");
        wal!.State.Should().Be(WalStreamStatus.Active);
        wal.ChainStartSegment.Should().Be("000000010000000000000005");
        driver.EnsuredBackupAgents.Should().Contain("pgw-backup-wal-cc4-shard1");
    }

    [Fact]
    public async Task Transient_stale_DEGRADED_не_блокирует_пересоздание_агента()
    {
        // Arrange — агент exited; цепочка сплошная, но тишина загрузок 3600 c (порог 60)
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("cc5");
        (await _claims.TryClaimClusterAsync("cc5", ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor();
        var s3 = new FakeBackupS3 { LastModified = DateTimeOffset.UtcNow.AddSeconds(-3600) };
        SeedSegments(s3, "cc5", 1, 2);
        var driver = new StubScaleDriver();
        driver.BackupAgentObjects.Add(new PgWorker.Docker.Engine.DockerContainer(
            "id-agent-cc5", ["/pgw-backup-wal-cc5-shard1"], "exited", "img"));
        var process = BuildProcess(Options(stale: 60), sql, s3, driver);
        var backups = new ClusterBackups("cc5", null,
            new Dictionary<string, ShardBackups> { ["shard1"] = FullShard("000000010000000000000001") });

        // Act — тик контроля
        (await process.TickAsync(BuildSnap("cc5"), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — DEGRADED «тишина»; агент ПЕРЕСОЗДАН (remove → ensure)
        var wal = await ReadWal("cc5");
        wal!.State.Should().Be(WalStreamStatus.Degraded);
        wal.Error.Should().Contain("тишина");
        driver.RemovedBackupAgents.Should().Contain("pgw-backup-wal-cc5-shard1");
        driver.EnsuredBackupAgents.Should().Contain("pgw-backup-wal-cc5-shard1");
    }

    [Fact]
    public async Task Супервиз_running_агент_канон_имён_без_слеша_не_трогается()
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
        driver.BackupAgentObjects.Add(new PgWorker.Docker.Engine.DockerContainer(
            "id-agent-cc8", ["pgw-backup-wal-cc8-shard1"], "running", "img"));
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
    public async Task Контроль_дыра_пишет_BROKEN_и_держит_агента_внизу()
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
        wal.ChainStartSegment.Should().Be("000000010000000000000001", "граница разрыва — последний непрерывный сегмент");
        driver.EnsuredBackupAgents.Should().BeEmpty("BROKEN держит агента внизу (ключ, не память)");
    }

    // AAA (AC2): слот исчез при живой цепочке → BROKEN + слот ПЕРЕСОЗДАН, агент вниз
    [Fact]
    public async Task Слот_исчез_пишет_BROKEN_и_пересоздает_слот()
    {
        // Arrange — ключ wal ACTIVE (цепочка ..01-..02), слота в FakeSql нет, агент жив
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("cb2");
        (await _claims.TryClaimClusterAsync("cb2", ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor();
        var s3 = new FakeBackupS3();
        SeedSegments(s3, "cb2", 1, 2);
        var driver = new StubScaleDriver();
        driver.BackupAgentObjects.Add(new PgWorker.Docker.Engine.DockerContainer(
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
        wal.Error.Should().Contain("слот");
        wal.ChainStartSegment.Should().Be("000000010000000000000002");
        driver.RemovedBackupAgents.Should().Contain("pgw-backup-wal-cb2-shard1");
        sql.Slots.Should().ContainKey("pgw_bkp_cb2_shard1", "слот пересоздаётся при BROKEN (spec §3.2)");
    }

    // AAA (t13 AC1): инвариант писателя нарушен (ACTIVE с last_uploaded_unix=null
    // — ручная правка/иной писатель) + слот исчез: тик НЕ падает в shard-error —
    // BROKEN (запись из живого ключа, now()-фолбэк в неё не попадает) + слот
    // пересоздан + агент остановлен + journal-заметка wal-key-invalid/<X>.
    // Битый wal в etcd не сидируется: снапшот-объект передан тику напрямую,
    // парсеры не задействованы (spec §3.3); чтение ключа — raw (парсер отверг
    // бы ключ без last_uploaded_unix — гвард остаётся первой линией, spec §3.4).
    [Fact]
    public async Task Слот_исчез_при_битом_last_uploaded_unix_не_роняет_тик()
    {
        // Arrange — wal ACTIVE с LastUploadedUnix = null (инвариант писателя
        // нарушен), слота в FakeSql нет, агент жив; цепочка объектов в S3 есть
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("cb5");
        (await _claims.TryClaimClusterAsync("cb5", ct)).Value.Should().BeTrue("клэйм — предусловие тика");
        var sql = new FakeWalSqlExecutor();
        var s3 = new FakeBackupS3();
        SeedSegments(s3, "cb5", 1, 2);
        var driver = new StubScaleDriver();
        driver.BackupAgentObjects.Add(new PgWorker.Docker.Engine.DockerContainer(
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
        walKv.Value.Should().NotBeNull("BROKEN пишется даже из битого ключа");
        walKv.Value!.Value.Should().Contain("\"state\":\"BROKEN\"").And.Contain("слот");
        walKv.Value.Value.Should().Contain(
            "\"chain_start_segment\":\"000000010000000000000002\"",
            "граница разрыва — last_uploaded_segment на момент обнаружения");
        walKv.Value.Value.Should().NotContain("\"last_uploaded_unix\"",
            "запись строится из живого wal: now()-фолбэк в ключ не попадает (spec §2)");
        sql.Slots.Should().ContainKey("pgw_bkp_cb5_shard1", "слот пересоздаётся при BROKEN (spec §3.2)");
        driver.RemovedBackupAgents.Should().Contain("pgw-backup-wal-cb5-shard1");
        var journal = await fixture.Gateway.GetAsync(fixture.Endpoint, "/pgworker/work/cb5", ct);
        journal.Value!.Value.Should().Contain("wal-key-invalid/shard1");
        journal.Value.Value.Should().NotContain("shard-error");
    }

    // AAA (AC1/заживление): новый COMPLETED-полный ≥ границы + непрерывная цепь →
    // ACTIVE + агент ТЕМ ЖЕ тиком (даже без прошедшего VerifyIntervalSec)
    [Fact]
    public async Task Контроль_при_BROKEN_каждый_тик_заживляет_одним_тиком()
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
            "000000010000000000000001", 1757500000, null, "дыра WAL-цепочки"), ct);
        var process = BuildProcess(Options(verify: 3600), sql, s3, driver, clock: TimeProvider.System);
        var backups = new ClusterBackups("cb3", null,
            new Dictionary<string, ShardBackups> { ["shard1"] = FullShard("000000010000000000000005") });

        // Act — ОДИН тик (расписание 3600 с НЕ наступило — BROKEN контролирует каждый тик)
        (await process.TickAsync(BuildSnap("cb3"), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — ACTIVE с chain_start от нового полного; агент поднят тем же тиком
        var wal = await ReadWal("cb3");
        wal!.State.Should().Be(WalStreamStatus.Active);
        wal.ChainStartSegment.Should().Be("000000010000000000000005");
        driver.EnsuredBackupAgents.Should().Contain("pgw-backup-wal-cb3-shard1");
    }

    // AAA (AC4 — рестарт-устойчивость): BROKEN живёт в etcd — пересоздание
    // процесса (новая фабрика = «рестарт воркера») не поднимает агента
    [Fact]
    public async Task BROKEN_переживает_рестарт_процесса()
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
        driver.EnsuredBackupAgents.Should().BeEmpty("BROKEN в etcd переживает рестарт воркера");
    }

    // AAA (AC9-регресс): смена мастера при живом ключе — агент пересоздаётся с нового
    // (masterChanged-ветка работает с новым ControlOutcome: ключ не BROKEN).
    // Тик 1 — контроль по расписанию (штатно пишет master_node нового резолва);
    // тик 2 — контроль вне интервала (VerifyIntervalSec=3600): EnsureAgent сверяет
    // master_node ПРЕЖНЕГО статуса с новым резолвом → remove+ensure с нового мастера.
    [Fact]
    public async Task Смена_мастера_агент_пересоздается_с_нового()
    {
        // Arrange — ключ wal ACTIVE master_node=shard1a; агент running на «shard1a»;
        // portalloc + ShardSpec.Master переводят мастера на shard1b; цепочка сплошная
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("cm1");
        (await _claims.TryClaimClusterAsync("cm1", ct)).Value.Should().BeTrue();
        await fixture.Gateway.PutAsync(fixture.Endpoint, "/pgworker/portalloc/cm1",
            Portalloc.Serialize(new Dictionary<string, NodeAddress>
            {
                ["shard1/shard1a"] = new("localhost", new NodePorts(16001, 18001, 17001)),
                ["shard1/shard1b"] = new("localhost", new NodePorts(16002, 18002, 17002)),
            }), null, ct);
        var sql = new FakeWalSqlExecutor();
        sql.Slots["pgw_bkp_cm1_shard1"] = true;
        var s3 = new FakeBackupS3();
        SeedSegments(s3, "cm1", 1, 2);
        var driver = new StubScaleDriver();
        driver.BackupAgentObjects.Add(new PgWorker.Docker.Engine.DockerContainer(
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
        driver.EnsuredBackupAgents.Should().Contain("pgw-backup-wal-cm1-shard1");
    }

    [Fact]
    public async Task Контроль_дыра_без_прошлого_ключа_пишет_BROKEN_AC4_тотальность()
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
    public async Task Контроль_без_сегментных_объектов_ключ_не_пишется_до_первого_наблюдения()
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
        driver.EnsuredBackupAgents.Should().Contain("pgw-backup-wal-cc7-shard1");
    }

    [Fact]
    public async Task Контроль_пропажа_объектов_staleness_от_прошлого_факта_не_от_now()
    {
        // Arrange — ключ ACTIVE с last_uploaded_unix = now-3600 (порог 300); S3 пуст
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("cc8");
        (await _claims.TryClaimClusterAsync("cc8", ct)).Value.Should().BeTrue();
        var clock = new MutableClock();
        var staleUnix = clock.Now.AddHours(-1).ToUnixTimeSeconds();
        var sql = new FakeWalSqlExecutor();
        sql.Slots["pgw_bkp_cc8_shard1"] = true; // слот жив — деградация от тишины
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
        wal.Error.Should().Contain("тишина");
        wal.LastUploadedUnix.Should().Be(staleUnix,
            "факт над записью: фабрика now() прятала бы мёртвый поток (ревью Ф4-2 №2)");
    }

    [Fact]
    public async Task Контроль_lag_выше_порога_DEGRADED_AC5a()
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
        wal.Error.Should().Contain("отставание");
        driver.EnsuredBackupAgents.Should().Contain("pgw-backup-wal-cc9-shard1");
        driver.RemovedBackupAgents.Should().NotContain("pgw-backup-wal-cc9-shard1");
    }

    [Fact]
    public async Task Первый_объект_закрепляет_chain_start_без_полных()
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
}
