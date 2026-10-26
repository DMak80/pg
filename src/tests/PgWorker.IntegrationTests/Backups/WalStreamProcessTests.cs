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
        // t25: приёмник по умолчанию «жив» (ключ lease есть) — контроль не
        // деградирует not-running; конкретные тесты удаляют/сидируют факты.
        await SeedAgentAsync(cluster, "shard1a", AgentJson("shard1a"));
    }

    // t25: сид статус-ключа приёмника agent/<N> (формат arch/19 §4; факты
    // tli_jump/pool_exhausted сидируются конкретными тестами через AgentJson).
    private async Task SeedAgentAsync(string cluster, string node, string json)
        => await fixture.Gateway.PutAsync(
            fixture.Endpoint, $"/pgworker/backups/{cluster}/shard1/agent/{node}", json, null,
            TestContext.Current.CancellationToken);

    private async Task DeleteAgentAsync(string cluster, string node)
        => await fixture.Gateway.DeleteAsync(
            fixture.Endpoint, $"/pgworker/backups/{cluster}/shard1/agent/{node}", prefix: false,
            TestContext.Current.CancellationToken);

    // Значение статус-ключа приёмника: минимум node/state + опциональные факты.
    private static string AgentJson(string node, bool poolExhausted = false, WalGapState? jump = null)
    {
        var jumpJson = jump is null
            ? ""
            : $@",""tli_jump"":{{""from"":""{jump.From}"",""to"":""{jump.To}"",""tli"":{jump.Tli}}}";
        return $@"{{""node"":""{node}"",""state"":""streaming"",""pool_exhausted"":{(poolExhausted ? "true" : "false")}{jumpJson}}}";
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

    // t25: аудируемая фаза create агента упразднена вместе с агентами
    // (LongCallPolling-прогрев джобов покрыт тестами джобов).

    // Тест-опции: VerifyIntervalSec=0 — контроль выполняется КАЖДЫМ тиком (AAA).
    private static BackupsRuntimeOptions Options(int verify = 0, int lag = 1024, int stale = 300) => new(
        Enabled: true,
        S3Bucket: "bkt",
        StagingDir: "/home/postgres/pgdata/backup-staging",
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
    public async Task TickEnsuresSlotAndWaitsForFirstObject()
    {
        // Arrange — полных нет, S3 пуст, ключ wal нет; клэйм кластера наш
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("c1");
        (await _claims.TryClaimClusterAsync("c1", ct)).Value.Should().BeTrue("claim is a tick precondition");
        var sql = new FakeWalSqlExecutor();
        var s3 = new FakeBackupS3();
        var driver = new StubScaleDriver();
        var process = BuildProcess(Options(), sql, s3, driver);

        // Act — первый тик: слот создан, ключ wal НЕ пишется
        //       (нет объектов — spec §3.2 шаг 8; агентов-контейнеров нет — t25)
        var result = await process.TickAsync(BuildSnap(), null, ct);

        // Assert
        result.IsSuccess.Should().BeTrue();
        // Слот ensured на DSN мастера (адресация ShardEndpoints.AdminDsn)
        sql.SlotsByDsn.Values.Should().Contain(s => s.Contains("pgw_bkp_c1_shard1"));
        // t25: контрольный тик БЕЗ docker-exec (history-доклад удалён)
        driver.ExecNodeCalls.Should().Be(0);
        var wal = await fixture.Gateway.GetAsync(
            fixture.Endpoint, "/pgworker/backups/c1/shard1/wal", ct);
        wal.Value.Should().BeNull("the key is not written until the first observation (spec §3.2 item 8)");
    }

    [Fact]
    public async Task TickIsIdempotent()
    {
        // Arrange — первый тик прошёл
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

        // Assert — слот на месте (идемпотентный ensure); ключ wal не пишется
        sql.SlotsByDsn.Values.Should().Contain(s => s.Contains("pgw_bkp_c1_shard1"));
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
        sql.SlotsByDsn.Should().BeEmpty("slot is not ensured without credentials (spec §3.2 item 1)");
        var journal = await fixture.Gateway.GetAsync(
            fixture.Endpoint, "/pgworker/work/c1", ct);
        journal.Value!.Value.Should().Contain("waiting-backup-password");
    }

    // t25 (стоп-семантика): Enabled=false тик — wal-ключ STOPPED; СТОП-ПУТЬ сам
    // не делает docker-сносов (санитарный sweep начала тика — отдельный механизм:
    // без legacy-контейнеров ничего не удаляет). Disabled-ключ ставит лидер
    // BackupOrphanSweeperLoop (кейс BackupsDisabledGate ниже).
    [Fact]
    public async Task TickWithEnabledFalseWritesStopped()
    {
        // Arrange — ключ wal ACTIVE от прошлого прохода
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

        // Act — тик с ВЫКЛЮЧЕННОЙ подсистемой (Enabled=false → runtime()=null)
        var stopped = BuildProcess(null, sql, s3, driver);
        var result = await stopped.TickAsync(BuildSnap(), null, ct);

        // Assert — ключ финально STOPPED; стоп-ветка ничего не удалила
        // (legacy-контейнеров нет — sweep начала тика no-op)
        result.IsSuccess.Should().BeTrue();
        var wal = await writer.ReadAsync("c1", "shard1", ct);
        wal.Value!.State.Should().Be(WalStreamStatus.Stopped);
        driver.RemovedBackupAgents.Should().BeEmpty("no legacy containers — nothing removed");
    }

    // t25 (стоп-семантика): глобальный лидер при Enabled=false поддерживает
    // /pgworker/backups-disabled (put), инстанс при старте с Enabled=true
    // снимает (del; отсутствие ключа — успех). Юнит через РЕАЛЬНЫЙ etcd fixture.
    [Fact]
    public async Task BackupsDisabledGate_PutAndDelete()
    {
        // Arrange — gate на реальном etcd fixture
        var ct = TestContext.Current.CancellationToken;
        var gate = new BackupsDisabledGate(fixture.Gateway);

        // Act/Assert — put: ключ жив с отметкой updated_unix
        (await gate.PutDisabledAsync([fixture.Endpoint], 1760000000, ct)).IsSuccess.Should().BeTrue();
        var kv = await fixture.Gateway.GetAsync(fixture.Endpoint, BackupsDisabledGate.Key, ct);
        kv.Value.Should().NotBeNull();
        kv.Value!.Value.Should().Contain("\"updated_unix\":1760000000");

        // Act/Assert — del: ключ снят; повторный del (нет ключа) — успех
        (await gate.DeleteAsync([fixture.Endpoint], ct)).IsSuccess.Should().BeTrue();
        (await fixture.Gateway.GetAsync(fixture.Endpoint, BackupsDisabledGate.Key, ct))
            .Value.Should().BeNull();
        (await gate.DeleteAsync([fixture.Endpoint], ct)).IsSuccess.Should().BeTrue();
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
    public async Task VerifyGapWritesBrokenBoundaries()
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

        // Assert — BROKEN с границами (агентов-контейнеров больше нет — t25)
        var wal = await ReadWal("cc2");
        wal!.State.Should().Be(WalStreamStatus.Broken);
        wal.Error.Should().Contain("000000010000000000000002").And.Contain("000000010000000000000003");
    }

    [Fact]
    public async Task VerifySlotInvalidationWritesBrokenAndRecreatesSlot()
    {
        // Arrange — ключ wal ACTIVE; FakeSql.Slots пуст (слот исчез)
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("cc3");
        (await _claims.TryClaimClusterAsync("cc3", ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor();
        var s3 = new FakeBackupS3();
        SeedSegments(s3, "cc3", 1, 2);
        var driver = new StubScaleDriver();
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

        // Assert — BROKEN с error про слот; слот пересоздан (t25: агентов нет)
        var wal = await ReadWal("cc3");
        wal!.State.Should().Be(WalStreamStatus.Broken);
        wal.Error.Should().Contain("slot");
        wal.ChainStartSegment.Should().Be("000000010000000000000002", "gap boundary — last_uploaded at detection time");
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

        // Assert — ACTIVE с новым chain_start одним тиком
        var wal = await ReadWal("cc4");
        wal!.State.Should().Be(WalStreamStatus.Active);
        wal.ChainStartSegment.Should().Be("000000010000000000000005");
    }

    [Fact]
    public async Task TransientStaleDegradedWritesSilence()
    {
        // Arrange — цепочка сплошная, но тишина загрузок 3600 c (порог 60)
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("cc5");
        (await _claims.TryClaimClusterAsync("cc5", ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor();
        var s3 = new FakeBackupS3 { LastModified = DateTimeOffset.UtcNow.AddSeconds(-3600) };
        SeedSegments(s3, "cc5", 1, 2);
        var driver = new StubScaleDriver();
        var process = BuildProcess(Options(stale: 60), sql, s3, driver);
        var backups = new ClusterBackups("cc5", null,
            new Dictionary<string, ShardBackups> { ["shard1"] = FullShard("000000010000000000000001") });

        // Act — тик контроля
        (await process.TickAsync(BuildSnap("cc5"), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — DEGRADED «тишина» (transient-деградация; t25: агентов нет)
        var wal = await ReadWal("cc5");
        wal!.State.Should().Be(WalStreamStatus.Degraded);
        wal.Error.Should().Contain("upload silence");
    }

    // ── t25-ревью п.16: компактные покрытия живых контрактов контроля ──

    // AAA (факт поверх записи, ревью Ф4-2 №2): прогресс не наблюдаем (S3 пуст —
    // list ничего не вернул, chain_start от verify-OK полного) —
    // last_uploaded_unix НЕ перезаписывается now(): носится из прошлой записи.
    // now()-фолбэк маскировал бы мёртвый стрим свежим фактом.
    [Fact]
    public async Task UnobservedProgress_KeepsLastUploadedFactFromPreviousKey()
    {
        // Arrange — full COMPLETED (wal_start ..0001, verify Ok), wal-ключ ACTIVE
        // с фактом 1757500000 (давно), S3 пуст, слот жив
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("cf1");
        (await _claims.TryClaimClusterAsync("cf1", ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor();
        sql.SlotsByDsn[SourceDsn(16001, "cf1")] = ["pgw_bkp_cf1_shard1"];
        var s3 = new FakeBackupS3();
        var process = BuildProcess(Options(), sql, s3, new StubScaleDriver());
        var walSeed = new WalStreamState(
            WalStreamStatus.Active, "pgw_bkp_cf1_shard1", "shard1a",
            "000000010000000000000001", "000000010000000000000001",
            "000000010000000000000001", 1757500000, null, null);
        var backups = new ClusterBackups("cf1", null,
            new Dictionary<string, ShardBackups>
            {
                ["shard1"] = FullShard("000000010000000000000001", wal: walSeed),
            });

        // Act
        (await process.TickAsync(BuildSnap("cf1"), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — факт прошлой записи сохранён (не now(), не chain_start)
        var wal = await ReadWal("cf1");
        wal!.LastUploadedUnix.Should().Be(1757500000,
            "the fact is carried from the previous record — now() would mask a dead stream");
        wal.LastUploadedSegment.Should().Be("000000010000000000000001");
    }

    // AAA (lag-DEGRADED, AC5a): зонд pg_current_wal_lsn мастера ушёл вперёд
    // хвоста S3 больше порога — DEGRADED «WAL stream lag N segments».
    [Fact]
    public async Task LagBeyondThreshold_Degraded()
    {
        // Arrange — цепочка 1..2 жива, мастер на сегменте 9 (лаг 7 > порог 3)
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("cl1");
        (await _claims.TryClaimClusterAsync("cl1", ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor { Current = ("0/9000000", 1) };
        sql.SlotsByDsn[SourceDsn(16001, "cl1")] = ["pgw_bkp_cl1_shard1"];
        var s3 = new FakeBackupS3();
        SeedSegments(s3, "cl1", 1, 2);
        var process = BuildProcess(Options(lag: 3), sql, s3, new StubScaleDriver());
        var backups = new ClusterBackups("cl1", null,
            new Dictionary<string, ShardBackups> { ["shard1"] = FullShard("000000010000000000000001") });

        // Act
        (await process.TickAsync(BuildSnap("cl1"), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert
        var wal = await ReadWal("cl1");
        wal!.State.Should().Be(WalStreamStatus.Degraded);
        wal.Error.Should().Contain("WAL stream lag 7 segments (threshold 3)");
    }

    // AAA (t18, интеграционный путь): chain_start от verify-OK полного с
    // ratchet — кандидат НИЖЕ записанной границы точку не понижает.
    [Fact]
    public async Task ChainStart_DoesNotLowerBelowRecorded()
    {
        // Arrange — wal-ключ с recorded chain_start ..0005; verify-OK full ниже
        // (..0001); S3: сегменты 5,6 (цепь от 0005 сплошная)
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("cr1");
        (await _claims.TryClaimClusterAsync("cr1", ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor();
        sql.SlotsByDsn[SourceDsn(16001, "cr1")] = ["pgw_bkp_cr1_shard1"];
        var s3 = new FakeBackupS3();
        SeedSegments(s3, "cr1", 5, 6);
        var process = BuildProcess(Options(), sql, s3, new StubScaleDriver());
        var walSeed = new WalStreamState(
            WalStreamStatus.Active, "pgw_bkp_cr1_shard1", "shard1a",
            "000000010000000000000005", "000000010000000000000006",
            "000000010000000000000005", DateTimeOffset.UtcNow.ToUnixTimeSeconds(), null, null);
        var backups = new ClusterBackups("cr1", null,
            new Dictionary<string, ShardBackups>
            {
                ["shard1"] = FullShard("000000010000000000000001", wal: walSeed),
            });

        // Act
        (await process.TickAsync(BuildSnap("cr1"), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — ratchet: точка осталась ..0005, ACTIVE
        var wal = await ReadWal("cr1");
        wal!.ChainStartSegment.Should().Be("000000010000000000000005");
        wal.State.Should().Be(WalStreamStatus.Active);
    }

    // ── t25: факты приёмников из статус-ключей etcd (arch/19 §4) ──

    // AAA: сид agent-ключей → контроль пишет agents wal-ключа по нодам portalloc
    // шарда: ключ жив (lease) → running; exited новыми записями не пишется
    // (легаси-значение панельного парсера).
    [Fact]
    public async Task AgentFactsFromStatusKeys_WriteRunning()
    {
        // Arrange — сид agent-ключа мастера (SeedAsync); full COMPLETED + цепочка 1..2
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("af1");
        (await _claims.TryClaimClusterAsync("af1", ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor { Current = ("0/3000000", 1) };
        var s3 = new FakeBackupS3();
        SeedSegments(s3, "af1", 1, 2);
        var process = BuildProcess(Options(), sql, s3, new StubScaleDriver());
        var backups = new ClusterBackups("af1", null,
            new Dictionary<string, ShardBackups> { ["shard1"] = FullShard("000000010000000000000001") });

        // Act — тик 1: ключ wal создаётся контролем (backups.Wal=null — без
        // фактов; факты только ДОБАВКА к живому ключу, spec п.8)
        (await process.TickAsync(BuildSnap("af1"), backups, ct)).IsSuccess.Should().BeTrue();

        // Arrange — перечитанный wal-ключ (как ReconcileLoop тика: backups-аргумент
        // несёт свежее состояние из etcd)
        var reader = new WalStatusWriter(fixture.Gateway, [fixture.Endpoint]);
        var reread = await reader.ReadAsync("af1", "shard1", ct);
        var fresh = new ClusterBackups("af1", null,
            new Dictionary<string, ShardBackups>
            {
                ["shard1"] = new(FullShard("000000010000000000000001").Full, reread.Value),
            });

        // Act — тик 2: факты приёмника пишутся в живой ключ
        (await process.TickAsync(BuildSnap("af1"), fresh, ct)).IsSuccess.Should().BeTrue();

        // Assert — agents: shard1a running (статус-ключ приёмника жив)
        var wal = await ReadWal("af1");
        wal!.Agents.Should().ContainSingle(
            a => a.Node == "shard1a" && a.State == WalAgentPresence.Running);
    }

    // AAA: ключа приёмника нет (lease истёк/процесс мёртв) → absent в agents и
    // DEGRADED «источник без живого приёмника» при живом wal-ключе (transient,
    // приоритет после gap, до lag/тишины); exited новыми записями не пишется.
    [Fact]
    public async Task AgentKeyMissing_DegradedNotRunning_AbsentFact()
    {
        // Arrange — wal-ключ ACTIVE; ключ agent/shard1a удалён
        var ct = TestContext.Current.CancellationToken;
        var cluster = "af2";
        await SeedAsync(cluster);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        await DeleteAgentAsync(cluster, "shard1a");
        var sql = new FakeWalSqlExecutor();
        SeedSlot(sql, cluster);
        var s3 = new FakeBackupS3();
        SeedSegments(s3, cluster, 1, 3);
        var driver = new StubScaleDriver();
        var process = BuildProcess(Options(), sql, s3, driver);
        var walSeed = LiveWal(cluster);
        var backups = new ClusterBackups(cluster, null,
            new Dictionary<string, ShardBackups>
            {
                ["shard1"] = new(FullShard("000000010000000000000001").Full, walSeed),
            });

        // Act — тик контроля
        (await process.TickAsync(BuildSnap(cluster), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — DEGRADED с именем ноды; agents: shard1a absent
        var wal = await ReadWal(cluster);
        wal!.State.Should().Be(WalStreamStatus.Degraded);
        wal.Error.Should().Be("wal receiver not running on shard1a");
        wal.Agents.Should().ContainSingle(
            a => a.Node == "shard1a" && a.State == WalAgentPresence.Absent);
    }

    // t25 (решение spec §7 №11): санитарный снос legacy wal-контейнеров
    // pgw-backup-wal-<C>-* в начале тика (один list на тик кластера,
    // идемпотентно): найдены — stop/remove; повторный тик по чистому листингу —
    // no-op (удалений больше нет; вызов list — каждый тик кластера).
    [Fact]
    public async Task SanitarySweepRemovesLegacyContainers_NoOpOnCleanTick()
    {
        // Arrange — legacy-контейнеры обоих форматов имён (shard-формат и per-node)
        var ct = TestContext.Current.CancellationToken;
        var cluster = "sw1";
        await SeedAsync(cluster);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        var driver = new StubScaleDriver();
        driver.BackupAgentObjects.Add(new DockerContainer(
            "id-sw-old", [$"/pgw-backup-wal-{cluster}-shard1"], "running", "img"));
        driver.BackupAgentObjects.Add(new DockerContainer(
            "id-sw-node", [$"/pgw-backup-wal-{cluster}-shard1-shard1a"], "running", "img"));
        var process = BuildProcess(Options(), new FakeWalSqlExecutor(), new FakeBackupS3(), driver);

        // Act — первый тик
        (await process.TickAsync(BuildSnap(cluster), null, ct)).IsSuccess.Should().BeTrue();
        var removedFirst = driver.RemovedBackupAgents.Count;

        // Assert — legacy-контейнеры снесены (оба имени кластерного префикса)
        removedFirst.Should().Be(2, "both legacy agent containers are swept at tick start");
        driver.BackupAgentObjects.Should().BeEmpty("the sweep removes legacy agents");

        // Act/Assert — повторный тик: листинг чист — удалений больше нет (no-op)
        (await process.TickAsync(BuildSnap(cluster), null, ct)).IsSuccess.Should().BeTrue();
        driver.RemovedBackupAgents.Count.Should().Be(removedFirst, "a clean listing removes nothing");
        driver.RemoveBackupAgentsCalls.Should().Be(2, "one list per cluster tick");
    }

    // t25 (spec §3.4 п.4): docker-exec-доклад history УДАЛЁН — history-файлы
    // докладывает приёмник из ФС своей ноды (PGDATA/pg_wal, факт history_missing
    // в статус-ключе). Прежние HistoryFallback-тесты упразднены вместе с кодом.

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

    // ---- t24/t25 (arch/19 §3/§4): tli-зазор из факта tli_jump мастер-приёмника ----

    // Сид факт tli_jump в статус-ключе мастер-приёмника (формат arch/19 §4;
    // факт живёт в etcd до следующего прыжка — docker-лог агента не читается).
    private async Task SeedTliJumpAsync(string cluster, string from, string to, uint tli)
        => await SeedAgentAsync(cluster, "shard1a",
            AgentJson("shard1a", jump: new WalGapState(from, to, tli)));

    // Wal-ключ ACTIVE с хвостом …003 (свежий прогресс: последний сегмент — now);
    // chainStart — записанная граница ratchet (сценарии среза профиля сидируют
    // актуальную точку: ratchet не понижается).
    private static WalStreamState LiveWal(string cluster, WalGapState? gap = null,
        WalStreamStatus state = WalStreamStatus.Active,
        string chainStart = "000000010000000000000001",
        string lastUploaded = "000000010000000000000003") => new(
        state, $"pgw_bkp_{cluster}_shard1", "shard1a",
        chainStart, lastUploaded,
        lastUploaded, DateTimeOffset.UtcNow.ToUnixTimeSeconds(), 0, null,
        Gap: gap);

    // Слоты: сидированный слот мастера (живой wal-ключ — иначе «слот исчез» BROKEN).
    private static void SeedSlot(FakeWalSqlExecutor sql, string cluster)
        => sql.SlotsByDsn.GetOrAdd(SourceDsn(16001, cluster), _ => [])
            .Add($"pgw_bkp_{cluster}_shard1");

    private async Task<string?> ReadRawWal(string cluster)
    {
        var kv = await fixture.Gateway.GetAsync(
            fixture.Endpoint, $"/pgworker/backups/{cluster}/shard1/wal", TestContext.Current.CancellationToken);
        kv.Value.Should().NotBeNull("the wal key must exist after the tick");
        return kv.Value!.Value;
    }

    // AAA (t25, spec §3.4 п.2): факт tli_jump в статус-ключе мастер-приёмника →
    // DEGRADED с error-префиксом tli-gap:, границами зазора и TLI-переходом 1→2;
    // поле Gap персистентно в ключе; journal-фаза wal-tli-gap/<X>.
    [Fact]
    public async Task TliJumpFact_WritesDegradedGap()
    {
        // Arrange — полный COMPLETED (wal_start ниже), wal-ключ ACTIVE, цепочка
        // 1..3 (профиль зазора есть), факт tli_jump 1→2 в agent-ключе мастера
        var ct = TestContext.Current.CancellationToken;
        var cluster = "tg1";
        await SeedAsync(cluster);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        await SeedTliJumpAsync(cluster,
            "000000010000000000000003", "000000020000000000000007", 2);
        var sql = new FakeWalSqlExecutor();
        SeedSlot(sql, cluster);
        var s3 = new FakeBackupS3();
        SeedSegments(s3, cluster, 1, 3);
        var driver = new StubScaleDriver();
        var process = BuildProcess(Options(), sql, s3, driver);
        var walSeed = LiveWal(cluster);
        var backups = new ClusterBackups(cluster, null,
            new Dictionary<string, ShardBackups> { ["shard1"] = new(FullShard("000000010000000000000001").Full, walSeed) });

        // Act — тик контроля
        (await process.TickAsync(BuildSnap(cluster), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — ключ DEGRADED, error построен из границ и TLI-перехода,
        // поле Gap персистентно, journal-фаза wal-tli-gap/shard1
        var wal = await ReadWal(cluster);
        wal!.State.Should().Be(WalStreamStatus.Degraded);
        wal.Error.Should().StartWith("tli-gap:");
        wal.Error.Should().Contain("000000010000000000000003")
            .And.Contain("000000020000000000000007")
            .And.Contain("1→2");
        wal.Gap.Should().BeEquivalentTo(new WalGapState(
            "000000010000000000000003", "000000020000000000000007", 2));
        var journal = await fixture.Gateway.GetAsync(
            fixture.Endpoint, $"/pgworker/work/{cluster}", ct);
        journal.Value!.Value.Should().Contain("wal-tli-gap/shard1");
    }

    // AAA: живой Gap + COMPLETED-полный с wal_start ≥ gap.to → Gap снят,
    // state ACTIVE (прогресс жив), error без tli-gap.
    [Fact]
    public async Task TliJumpGap_CoveredByFull_Lifted()
    {
        // Arrange — Gap (3→7) в сид-ключе; полный wal_start=..07 покрывает зазор
        var ct = TestContext.Current.CancellationToken;
        var cluster = "tg2";
        await SeedAsync(cluster);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor();
        SeedSlot(sql, cluster);
        var s3 = new FakeBackupS3();
        SeedSegments(s3, cluster, 1, 3);
        var driver = new StubScaleDriver();
        var process = BuildProcess(Options(), sql, s3, driver);
        var walSeed = LiveWal(cluster, gap: new WalGapState(
            "000000010000000000000003", "000000020000000000000007", 2));
        var backups = new ClusterBackups(cluster, null,
            new Dictionary<string, ShardBackups> { ["shard1"] = new(FullShard("000000020000000000000007").Full, walSeed) });

        // Act — тик контроля
        (await process.TickAsync(BuildSnap(cluster), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — заживление покрытием: Gap снят, ACTIVE, error без tli-gap
        var wal = await ReadWal(cluster);
        wal!.Gap.Should().BeNull();
        wal.State.Should().Be(WalStreamStatus.Active);
        wal.Error.Should().NotContain("tli-gap");
    }

    // AAA (AC4): внутри-TLI прыжок (from_tli == to_tli) — gap НЕ ставится
    // (обычная дыра цепочки → BROKEN-путь контроля по объектам).
    [Fact]
    public async Task TliJumpSameTli_NoGap()
    {
        // Arrange — факт tli_jump с from_tli == to_tli == 1 в agent-ключе;
        // полный ниже; цепочка цела
        var ct = TestContext.Current.CancellationToken;
        var cluster = "tg3";
        await SeedAsync(cluster);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        await SeedTliJumpAsync(cluster,
            "000000010000000000000003", "000000010000000000000007", 1);
        var sql = new FakeWalSqlExecutor();
        SeedSlot(sql, cluster);
        var s3 = new FakeBackupS3();
        SeedSegments(s3, cluster, 1, 3);
        var driver = new StubScaleDriver();
        var process = BuildProcess(Options(), sql, s3, driver);
        var walSeed = LiveWal(cluster);
        var backups = new ClusterBackups(cluster, null,
            new Dictionary<string, ShardBackups> { ["shard1"] = new(FullShard("000000010000000000000001").Full, walSeed) });

        // Act — тик контроля
        (await process.TickAsync(BuildSnap(cluster), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — Gap нет; state по общим правилам (прогресс жив — ACTIVE)
        var wal = await ReadWal(cluster);
        wal!.Gap.Should().BeNull();
        wal.State.Should().Be(WalStreamStatus.Active);
        wal.Error.Should().NotContain("tli-gap");
    }

    // AAA (WriteIfChanged, идемпотентность): два тика с тем же фактом tli_jump —
    // значение ключа стабильно; второй тик НЕ переписывает journal-фазу
    // (установка gap — один раз; удержание переносится без мутаций).
    [Fact]
    public async Task TliGap_WriteIfChanged_Idempotent()
    {
        // Arrange — фиксированные часы: тик 1 при T, тик 2 при T+600
        var clock = new MutableClock { Now = DateTimeOffset.UtcNow };
        var ct = TestContext.Current.CancellationToken;
        var cluster = "tg4";
        await SeedAsync(cluster);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        await SeedTliJumpAsync(cluster,
            "000000010000000000000003", "000000020000000000000007", 2);
        var sql = new FakeWalSqlExecutor();
        SeedSlot(sql, cluster);
        var s3 = new FakeBackupS3();
        SeedSegments(s3, cluster, 1, 3);
        var driver = new StubScaleDriver();
        var process = BuildProcess(Options(), sql, s3, driver, clock);
        var walSeed = LiveWal(cluster);
        var backups = new ClusterBackups(cluster, null,
            new Dictionary<string, ShardBackups> { ["shard1"] = new(FullShard("000000010000000000000001").Full, walSeed) });

        // Act — два тика с тем же маркером (второй — на 600 c позже)
        (await process.TickAsync(BuildSnap(cluster), backups, ct)).IsSuccess.Should().BeTrue();
        var raw1 = await ReadRawWal(cluster);
        var journal1 = await fixture.Gateway.GetAsync(
            fixture.Endpoint, $"/pgworker/work/{cluster}", ct);
        var journalUnix1 = System.Text.Json.JsonDocument.Parse(journal1.Value!.Value)
            .RootElement.GetProperty("updated_unix").GetInt64();
        clock.Now = clock.Now.AddSeconds(600);
        (await process.TickAsync(BuildSnap(cluster), backups, ct)).IsSuccess.Should().BeTrue();
        var raw2 = await ReadRawWal(cluster);
        var journal2 = await fixture.Gateway.GetAsync(
            fixture.Endpoint, $"/pgworker/work/{cluster}", ct);
        var journalUnix2 = System.Text.Json.JsonDocument.Parse(journal2.Value!.Value)
            .RootElement.GetProperty("updated_unix").GetInt64();

        // Assert — второй тик не переписал ключ (WriteIfChanged) и не написал
        // journal-фазу повторно (updated_unix журнала — время первого тика)
        raw1.Should().Be(raw2);
        journalUnix1.Should().Be(journalUnix2);
    }

    // AAA: Gap жив, но профиль зазора исчез из листинга (ретенция срезала
    // нижележащие TLI) → Gap снят даже без покрывающего полного.
    [Fact]
    public async Task TliGap_ProfileGone_Lifted()
    {
        // Arrange — Gap (3→5); полный COMPLETED wal_start=..04 (НЕ покрывает: 4 < 5,
        // verify PENDING — chain_start строится от min-объекта, а не от …04: объекта
        // …04 нет — ретенция срезала); листинг 5..6 — сегментов ниже gap.to нет
        var ct = TestContext.Current.CancellationToken;
        var cluster = "tg5";
        await SeedAsync(cluster);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor();
        SeedSlot(sql, cluster);
        var s3 = new FakeBackupS3();
        SeedSegments(s3, cluster, 5, 6);
        var driver = new StubScaleDriver();
        var process = BuildProcess(Options(), sql, s3, driver);
        var walSeed = LiveWal(cluster, gap: new WalGapState(
            "000000010000000000000003", "000000010000000000000005", 2),
            chainStart: "000000010000000000000005",
            lastUploaded: "000000010000000000000006");
        var pendingFull = FullShard("000000010000000000000004",
            verify: new BackupVerify(BackupVerifyStatus.Pending, 1757500400));
        var backups = new ClusterBackups(cluster, null,
            new Dictionary<string, ShardBackups> { ["shard1"] = new(pendingFull.Full, walSeed) });

        // Act — тик контроля
        (await process.TickAsync(BuildSnap(cluster), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — профиль срезан → Gap снят (покрытия не было — дискриминация пути)
        var wal = await ReadWal(cluster);
        wal!.Gap.Should().BeNull();
        wal.State.Should().Be(WalStreamStatus.Active);
        wal.Error.Should().NotContain("tli-gap");
    }

    // AAA (удержание): wal-ключ с Gap, факт tli_jump персистентен в etcd
    // (живёт в статус-ключе до следующего прыжка — «окно вымывания лога» ушло,
    // t25) — ключ остаётся DEGRADED; текст error построен из ПОЛЕЙ записи
    // (стабилен между тиками — WriteIfChanged), Gap перенесён.
    [Fact]
    public async Task TliGap_HeldPersistent()
    {
        // Arrange — Gap (3→7, tli 2) в сид-ключе; лог агента пуст; профиль есть (1..3)
        var ct = TestContext.Current.CancellationToken;
        var cluster = "tg6";
        await SeedAsync(cluster);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor();
        SeedSlot(sql, cluster);
        var s3 = new FakeBackupS3();
        SeedSegments(s3, cluster, 1, 3);
        var driver = new StubScaleDriver();
        var process = BuildProcess(Options(), sql, s3, driver);
        var gapSeed = new WalGapState(
            "000000010000000000000003", "000000020000000000000007", 2);
        var walSeed = LiveWal(cluster, gap: gapSeed, state: WalStreamStatus.Degraded);
        await new WalStatusWriter(fixture.Gateway, [fixture.Endpoint])
            .WriteIfChangedAsync(cluster, "shard1", walSeed, ct);
        var backups = new ClusterBackups(cluster, null,
            new Dictionary<string, ShardBackups> { ["shard1"] = new(FullShard("000000010000000000000001").Full, walSeed) });

        // Act — два тика без маркера в логе
        (await process.TickAsync(BuildSnap(cluster), backups, ct)).IsSuccess.Should().BeTrue();
        var raw1 = await ReadRawWal(cluster);
        (await process.TickAsync(BuildSnap(cluster), backups, ct)).IsSuccess.Should().BeTrue();
        var raw2 = await ReadRawWal(cluster);

        // Assert — зазор удержан: DEGRADED, error из полей записи содержит 1→2,
        // Gap перенесён; текст стабилен между тиками
        var wal = await ReadWal(cluster);
        wal!.State.Should().Be(WalStreamStatus.Degraded);
        wal.Error.Should().StartWith("tli-gap:");
        wal.Error.Should().Contain("1→2");
        wal.Gap.Should().BeEquivalentTo(gapSeed);
        raw1.Should().Be(raw2, "the held-gap text is built from the record fields (WriteIfChanged stable)");
    }

    // AAA (Review Focus — цикл «снятие ↔ повторная установка»): факт tli_jump
    // в agent-ключе, НО полный с wal_start ≥ to_segment уже есть → Gap НЕ ставится.
    [Fact]
    public async Task TliJumpFact_CoveredByFull_NotSet()
    {
        // Arrange — полный wal_start=..07 (покрывает to-границу факта);
        // факт прыжка 1→2 в agent-ключе мастера; цепочка 1..3
        var ct = TestContext.Current.CancellationToken;
        var cluster = "tg7";
        await SeedAsync(cluster);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        await SeedTliJumpAsync(cluster,
            "000000010000000000000003", "000000020000000000000007", 2);
        var sql = new FakeWalSqlExecutor();
        SeedSlot(sql, cluster);
        var s3 = new FakeBackupS3();
        SeedSegments(s3, cluster, 1, 3);
        var driver = new StubScaleDriver();
        var process = BuildProcess(Options(), sql, s3, driver);
        var walSeed = LiveWal(cluster);
        var backups = new ClusterBackups(cluster, null,
            new Dictionary<string, ShardBackups> { ["shard1"] = new(FullShard("000000020000000000000007").Full, walSeed) });

        // Act — тик контроля
        (await process.TickAsync(BuildSnap(cluster), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — покрыто полным: Gap не ставится (нет цикла снятие↔установка)
        var wal = await ReadWal(cluster);
        wal!.Gap.Should().BeNull();
        wal.State.Should().Be(WalStreamStatus.Active);
        wal.Error.Should().NotContain("tli-gap");
    }

    // ---- t02/t25 (arch/19 §3): исчерпание write-списка — флаг pool_exhausted
    // статус-ключа мастер-приёмника (детектор хвоста docker-лога удалён:
    // факт персистентен в etcd до первого успешного put) ----

    // AAA: флаг pool_exhausted=true в agent-ключе мастера → wal DEGRADED с
    // маркером s3-pool-exhausted; пока флаг жив — DEGRADED удерживается; флаг
    // снят (первый успешный put приёмника) → контроль возвращает ACTIVE с
    // очищенным error (self-heal одним контрольным проходом). Между тиками
    // ключ ПЕРЕЧИТЫВАЕТСЯ из etcd (контроль берёт wal из backups-аргумента
    // тика — паттерн BrokenSurvivesProcessRestart); VerifyIntervalSec=3600
    // дискриминирует расписание: без правила «exhaustion-DEGRADED каждый тик»
    // тики 2-3 были бы пропущены.
    [Fact]
    public async Task ExhaustionFlagDegraded_ThenFlagClearedHealsActive()
    {
        // Arrange — полный wal_start=..01, цепочка 1..3, ключ ACTIVE, слот
        // жив; agent-ключ мастера несёт pool_exhausted=true (put ещё не прошёл)
        var ct = TestContext.Current.CancellationToken;
        var cluster = "pe1";
        await SeedAsync(cluster);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue("claim is a tick precondition");
        await SeedAgentAsync(cluster, "shard1a", AgentJson("shard1a", poolExhausted: true));
        var sql = new FakeWalSqlExecutor();
        SeedSlot(sql, cluster);
        var s3 = new FakeBackupS3();
        SeedSegments(s3, cluster, 1, 3);
        var driver = new StubScaleDriver();
        var process = BuildProcess(Options(verify: 3600), sql, s3, driver);
        var walSeed = LiveWal(cluster);
        var backups = new ClusterBackups(cluster, null,
            new Dictionary<string, ShardBackups>
            {
                ["shard1"] = new(FullShard("000000010000000000000001").Full, walSeed),
            });

        // Свежий backups из ПЕРЕЧИТАННОГО ключа (как ReconcileLoop тика)
        var reader = new WalStatusWriter(fixture.Gateway, [fixture.Endpoint]);
        async Task<ClusterBackups> FreshBackupsAsync()
        {
            var reread = await reader.ReadAsync(cluster, "shard1", ct);
            return new ClusterBackups(cluster, null,
                new Dictionary<string, ShardBackups>
                {
                    ["shard1"] = new(FullShard("000000010000000000000001").Full, reread.Value),
                });
        }

        // Act — тик 1 (первый контроль): флаг актуален → DEGRADED с маркером
        (await process.TickAsync(BuildSnap(cluster), backups, ct)).IsSuccess.Should().BeTrue();
        var degraded = await ReadWal(cluster);
        degraded!.State.Should().Be(WalStreamStatus.Degraded);
        degraded.Error.Should().Contain("s3-pool-exhausted");

        // Act — тик 2 (ключ перечитан — exhaustion-DEGRADED, расписание 3600 с
        // НЕ наступило, но контроль выполняется): флаг в etcd не снялся
        // (доставки нет) — DEGRADED удерживается
        (await process.TickAsync(BuildSnap(cluster), await FreshBackupsAsync(), ct)).IsSuccess.Should().BeTrue();
        var held = await ReadWal(cluster);
        held!.State.Should().Be(WalStreamStatus.Degraded);
        held.Error.Should().Contain("s3-pool-exhausted");

        // Act — тик 3: флаг в agent-ключе мастера снялся (первый успешный put
        // приёмника — единственный знак снятия)
        await SeedAgentAsync(cluster, "shard1a", AgentJson("shard1a"));
        (await process.TickAsync(BuildSnap(cluster), await FreshBackupsAsync(), ct)).IsSuccess.Should().BeTrue();

        // Assert — self-heal одним контрольным проходом: ACTIVE, error очищен
        var healed = await ReadWal(cluster);
        healed!.State.Should().Be(WalStreamStatus.Active);
        healed.Error.Should().BeNull();
    }

    // AAA (расписание, образец VerifyAtBrokenEveryTickHealsInOneTick): ключ
    // exhaustion-DEGRADED (DEGRADED с маркером в error) + VerifyIntervalSec
    // =3600 — контроль выполняется ТЕМ ЖЕ тиком (флаг в agent-ключе уже снят):
    // заживление ACTIVE без ожидания интервала. Дискриминация bypass:
    // прогревочный тик при ACTIVE-ключе заполняет _lastVerifyUnix — на целевом
    // тике расписание (3600 с) НЕ наступило, и без правила «exhaustion-DEGRADED
    // каждый тик» контроль был бы ПРОПУЩЕН (ключ остался бы DEGRADED).
    [Fact]
    public async Task ExhaustionDegradedEveryTickHealsInOneTick()
    {
        // Arrange — сид: полный wal_start=..01; цепочка 1..3; слот жив; ключ
        // ACTIVE; флаг pool_exhausted в agent-ключе мастера снят (SeedAsync)
        var ct = TestContext.Current.CancellationToken;
        var cluster = "pe2";
        await SeedAsync(cluster);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue("claim is a tick precondition");
        var sql = new FakeWalSqlExecutor();
        SeedSlot(sql, cluster);
        var s3 = new FakeBackupS3();
        SeedSegments(s3, cluster, 1, 3);
        var driver = new StubScaleDriver();
        var writer = new WalStatusWriter(fixture.Gateway, [fixture.Endpoint]);
        var process = BuildProcess(Options(verify: 3600), sql, s3, driver);
        var warmWal = LiveWal(cluster);
        var warmBackups = new ClusterBackups(cluster, null,
            new Dictionary<string, ShardBackups>
            {
                ["shard1"] = new(FullShard("000000010000000000000001").Full, warmWal),
            });

        // Act — прогревочный тик: первый контроль (расписание ещё пусто)
        // заполняет _lastVerifyUnix; ключ остаётся ACTIVE (флаг снят)
        (await process.TickAsync(BuildSnap(cluster), warmBackups, ct)).IsSuccess.Should().BeTrue();
        (await ReadWal(cluster))!.State.Should().Be(WalStreamStatus.Active);

        // Arrange — ПОСЛЕ прогрева ключ перезаписывается exhaustion-DEGRADED
        var seed = LiveWal(cluster, state: WalStreamStatus.Degraded) with
        {
            Error = "s3-pool-exhausted: write list exhausted by master receiver (receiver retry loop)",
        };
        await writer.WriteIfChangedAsync(cluster, "shard1", seed, ct);
        var backups = new ClusterBackups(cluster, null,
            new Dictionary<string, ShardBackups>
            {
                ["shard1"] = new(FullShard("000000010000000000000001").Full, seed),
            });

        // Act — ОДИН тик: расписание 3600 с НЕ наступило (_lastVerifyUnix только
        // что заполнен прогревом) — exhaustion-DEGRADED контролирует каждый тик
        (await process.TickAsync(BuildSnap(cluster), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — ACTIVE, error очищен
        var wal = await ReadWal(cluster);
        wal!.State.Should().Be(WalStreamStatus.Active);
        wal.Error.Should().BeNull();
    }
}
