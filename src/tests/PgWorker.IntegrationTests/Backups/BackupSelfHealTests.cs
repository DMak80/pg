using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using PgWorker.Backups;
using PgWorker.Backups.Job;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Planning;
using PgWorker.Core.Templates;
using PgWorker.Core.Tuning;
using PgWorker.Docker.Drivers;
using PgWorker.Docker.Engine;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.Etcd.Parsing;
using PgWorker.IntegrationTests.Etcd;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Processes;
using PgWorker.Provisioning.Probes;
using PgWorker.Provisioning.Sql;
using Xunit;

namespace PgWorker.IntegrationTests.Backups;

// Сквозной контур самолечения (t07 AC1, spec §3.2): дыра цепочки → WalStream
// пишет BROKEN → BackupProcess планирует пересъём (IsDue walChainBroken) →
// симуляция COMPLETED переснятого полного → WalStream контроль заживляет
// (ACTIVE + агент тем же тиком). Реальный etcd, фейки docker/SQL/S3.
[Collection(EtcdCollection.Name)]
public class BackupSelfHealTests(EtcdFixture fixture)
{
    private readonly ClaimStore _claims = new([fixture.Endpoint], fixture.Gateway, TimeProvider.System);

    // ── Хелперы Arrange (копия паттерна WalStreamProcessTests) ──

    private static ClusterSnapshot BuildSnap(string cluster) => new(
        new ClusterConfig(cluster, 2, cluster, null, ClusterState.Active),
        [new ShardSpec("shard1", 1, $"host=shard1a dbname={cluster}", "shard1a:17001",
            [new NodeSpec("shard1", "shard1a", NodeState.Running)])],
        []);

    // Сид etcd: portalloc + backup_password (WalStream) + config/shards/dsn/master
    // (BackupProcess G2/источник); чистка своих префиксов — изоляция прогонов.
    private async Task SeedAsync(string cluster, string password = "pw")
    {
        var ct = TestContext.Current.CancellationToken;
        await fixture.Gateway.DeleteAsync(fixture.Endpoint, $"/pgworker/backups/{cluster}/", prefix: true, ct);
        await fixture.Gateway.DeleteAsync(fixture.Endpoint, $"/pgworker/claims/{cluster}", prefix: false, ct);
        await fixture.Gateway.PutAsync(fixture.Endpoint, $"/pgworker/portalloc/{cluster}",
            Portalloc.Serialize(new Dictionary<string, NodeAddress>
            {
                ["shard1/shard1a"] = new("localhost", new NodePorts(16001, 18001, 17001)),
            }), null, ct);
        await fixture.Gateway.PutAsync(fixture.Endpoint, $"/clusters/{cluster}/backup_password",
            password, null, ct);
        await fixture.Gateway.PutAsync(fixture.Endpoint, $"/clusters/{cluster}/config",
            """{"buckets":1,"dbname":"sh1","created_unix":1755900000}""", null, ct);
        await fixture.Gateway.PutAsync(fixture.Endpoint, $"/clusters/{cluster}/shards/shard1/replicas",
            "2", null, ct);
        await fixture.Gateway.PutAsync(fixture.Endpoint,
            $"/clusters/{cluster}/shards/shard1/nodes/shard1a/state", "RUNNING", null, ct);
        await fixture.Gateway.PutAsync(fixture.Endpoint, $"/clusters/{cluster}/shards/shard1/dsn",
            $"host=shard1a port=5432 dbname={cluster} user=bucket_admin password=x", null, ct);
        await fixture.Gateway.PutAsync(fixture.Endpoint, $"/clusters/{cluster}/shards/shard1/master",
            "shard1a:17001", null, ct);
    }

    private WalStreamProcess BuildWalProcess(
        BackupsRuntimeOptions options, FakeWalSqlExecutor sql, FakeBackupS3 s3, StubScaleDriver driver)
        => new(
            fixture.Gateway, [fixture.Endpoint], driver,
            new ShardEndpoints(fixture.Gateway, [fixture.Endpoint], new ShardProbe(new HttpClient())),
            sql, s3,
            new WalStatusWriter(fixture.Gateway, [fixture.Endpoint]),
            _claims, new WorkJournal(fixture.Gateway, [fixture.Endpoint]),
            () => options,
            new InstallSecrets("su-pw", "sb-pw", "adm-pw", "mov-pw"),
            TimeProvider.System);

    // Тест-опции (позиционник BackupsRuntimeOptions — паттерн WalStreamProcessTests).
    private static BackupsRuntimeOptions Options() => new(
        Enabled: true,
        S3Endpoint: "http://minio",
        S3AdvertisedEndpoint: "http://minio-agent",
        S3Bucket: "bkt",
        S3AccessKey: "ak",
        S3SecretKey: "sk",
        S3PathStyle: true,
        JobImage: "pgworker-backup:test",
        StagingDir: "/backup-staging",
        WalVerifyIntervalSec: 0,
        WalLagMaxSegments: 1024,
        WalStaleSec: 300);

    private static ShardBackups FullShard(string walStart, WalStreamState? wal = null) => new(
        [new FullBackupState("20260910120000Z", FullBackupStatus.Completed, "shard1a",
            BackupSourceRole.Replica, 1757500000, 1757500300, walStart, 1024, null, null)],
        wal);

    private static void SeedSegments(FakeBackupS3 s3, string cluster, int from, int to)
    {
        for (var i = from; i <= to; i++)
            s3.Objects.Add((cluster, "shard1", $"0000000100000000000000{i:x2}"));
    }

    private async Task<WalStreamState?> ReadWalAsync(string cluster)
    {
        var reader = new WalStatusWriter(fixture.Gateway, [fixture.Endpoint]);
        var read = await reader.ReadAsync(cluster, "shard1", TestContext.Current.CancellationToken);
        read.IsSuccess.Should().BeTrue();
        return read.Value;
    }

    // Снапшот из etcd (BackupProcess читает /clusters/ — как ReconcileLoop).
    private async Task<ClusterSnapshot> SnapshotAsync(string cluster)
    {
        var range = await fixture.Gateway.RangeAsync(
            fixture.Endpoint, "/clusters/", TestContext.Current.CancellationToken);
        var parsed = ClusterSnapshotParser.ParseClusters(range.Value, out _);
        return parsed.Value.Single(c => c.Config.Cluster == cluster);
    }

    // AAA (AC1 сквозной): дыра → BROKEN → планировщик PLANNED/RUNNING → симуляция
    // завершения пересъёма (etcd-ключ COMPLETED с wal_start выше границы + объекты
    // S3) → WalStream контроль → ACTIVE + агент поднят
    [Fact]
    public async Task Дыра_BROKEN_пересъём_и_заживление_сквозным_циклом()
    {
        var ct = TestContext.Current.CancellationToken;
        await SeedAsync("sh1");
        (await _claims.TryClaimClusterAsync("sh1", ct)).Value.Should().BeTrue();
        var sql = new FakeWalSqlExecutor();
        var s3 = new FakeBackupS3();
        var driver = new StubScaleDriver();

        // Arrange 1 — WalStream тик на дырной цепочке (full ..01; S3: 1,2,3,5 —
        // дыра ..04 внутри цепочки; граница разрыва — ..03 > wal_start полного,
        // как в E2E-сценарии «дыра в середине»)
        SeedSegments(s3, "sh1", 1, 3);
        s3.Objects.Add(("sh1", "shard1", "000000010000000000000005"));
        var walProcess = BuildWalProcess(Options(), sql, s3, driver);
        var backups1 = new ClusterBackups("sh1", null,
            new Dictionary<string, ShardBackups> { ["shard1"] = FullShard("000000010000000000000001") });
        (await walProcess.TickAsync(BuildSnap("sh1"), backups1, ct)).IsSuccess.Should().BeTrue();

        // Assert 1 — wal=BROKEN с границей разрыва ..03
        var broken = await ReadWalAsync("sh1");
        broken!.State.Should().Be(WalStreamStatus.Broken);
        broken.ChainStartSegment.Should().Be("000000010000000000000003");

        // Arrange 2 — BackupProcess тик: джобов нет, BROKEN → due → PLANNED →
        // create/start фейка → RUNNING (планировщик отреагировал)
        var engine = new FakeBackupEngine();
        var backupProcess = new BackupProcess(
            fixture.Gateway, [fixture.Endpoint],
            new SelfHealDriver(driver, engine),
            new ShardEndpoints(fixture.Gateway, [fixture.Endpoint], new ShardProbe(new HttpClient())),
            new StubDb(), new StubSecrets(), _claims,
            new WorkJournal(fixture.Gateway, [fixture.Endpoint]),
            new InstallSecrets("su-pw", "sb-pw", "adm-pw", "mov-pw"),
            Options(), TimeProvider.System, NullLogger<BackupProcess>.Instance);
        (await backupProcess.TickAsync(await SnapshotAsync("sh1"), [backups1], ct))
            .IsSuccess.Should().BeTrue();
        engine.Created.Should().NotBeEmpty("BROKEN лечится пересъёмом — джоб запущен");

        // Arrange 3 — симуляция завершения пересъёма: etcd-ключ COMPLETED нового id
        // с wal_start=..05 + объекты S3 5..6 (цепь от нового полного непрерывна)
        const string newId = "20260910130000Z";
        var recompleted = new FullBackupState(newId, FullBackupStatus.Completed, "shard1a",
            BackupSourceRole.Replica, 1757501000, 1757501300, "000000010000000000000005",
            2048, null, null);
        await fixture.Gateway.PutAsync(fixture.Endpoint,
            BackupNames.FullKey("sh1", "shard1", newId),
            BackupStatusJson.Serialize(recompleted), null, ct);
        s3.Objects.Add(("sh1", "shard1", "000000010000000000000006"));

        // Act — WalStream тик (контроль при BROKEN — каждый тик; процесс новый —
        // «рестарт воркера»: маркер разрыва читается из ключа, не из памяти)
        var rereadWal = await ReadWalAsync("sh1");
        var backups2 = new ClusterBackups("sh1", null,
            new Dictionary<string, ShardBackups>
            {
                ["shard1"] = new(
                [
                    FullShard("000000010000000000000001").Full[0],
                    recompleted,
                ], rereadWal),
            });
        var walProcess2 = BuildWalProcess(Options(), sql, s3, driver);
        (await walProcess2.TickAsync(BuildSnap("sh1"), backups2, ct)).IsSuccess.Should().BeTrue();

        // Assert — wal.state=ACTIVE, chain_start от нового полного, агент поднят
        var healed = await ReadWalAsync("sh1");
        healed!.State.Should().Be(WalStreamStatus.Active);
        healed.ChainStartSegment.Should().Be("000000010000000000000005");
        driver.EnsuredBackupAgents.Should().Contain("pgw-backup-wal-sh1-shard1");
    }

    // Драйвер-композит (паттерн RestoreProcessTests.TestDriver): агентные вызовы —
    // StubScaleDriver, движок джобов — FakeBackupEngine.
    private sealed class SelfHealDriver(IClusterDriver inner, IDockerEngine engine) : IClusterDriver
    {
        public bool SupportsRunningInspection => inner.SupportsRunningInspection;
        public IDockerEngine? EngineFor(string host) => engine;
        public Task<Result<IReadOnlyList<HostInfo>>> GetHostsAsync(CancellationToken ct) => inner.GetHostsAsync(ct);
        public Task<Result<IReadOnlySet<(string Host, int Port)>>> GetBusyPortsAsync(CancellationToken ct) => inner.GetBusyPortsAsync(ct);
        public Task<Result> EnsureNodeAsync(ShardTopology t, string n, NodeAddress a, InstallSecrets s, EtcdEndpoints e, NodeResources? r, PgTuneResult? tuning, CancellationToken ct) => inner.EnsureNodeAsync(t, n, a, s, e, r, tuning, ct);
        public Task<Result> RemoveNodeAsync(string c, string sh, string node, CancellationToken ct) => inner.RemoveNodeAsync(c, sh, node, ct);
        public Task<Result> StopNodeAsync(string c, string sh, string node, CancellationToken ct) => inner.StopNodeAsync(c, sh, node, ct);
        public Task<Result<DataPresence>> NodeDataPresenceAsync(string c, string sh, string node, CancellationToken ct) => inner.NodeDataPresenceAsync(c, sh, node, ct);
        public Task<Result<string>> ExecNodeAsync(string c, string sh, string node, IReadOnlyList<string> cmd, CancellationToken ct) => inner.ExecNodeAsync(c, sh, node, cmd, ct);
        public Task<Result<string>> ExecContainerAsync(string container, IReadOnlyList<string> cmd, CancellationToken ct) => inner.ExecContainerAsync(container, cmd, ct);
        public Task<Result<IReadOnlyDictionary<string, DiscoveredNode>>> InspectNodesAsync(string c, IReadOnlyCollection<string> names, CancellationToken ct) => inner.InspectNodesAsync(c, names, ct);
        public Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(string c, CancellationToken ct) => inner.ListNodeObjectsAsync(c, ct);
        public Task<Result> EnsureBackupAgentAsync(string c, string sh, ContainerSpec spec, string host, CancellationToken ct) => inner.EnsureBackupAgentAsync(c, sh, spec, host, ct);
        public Task<Result> RemoveBackupAgentsAsync(string c, string? sh, CancellationToken ct) => inner.RemoveBackupAgentsAsync(c, sh, ct);
        public Task<Result<IReadOnlyList<DockerContainer>>> ListBackupAgentsAsync(string c, CancellationToken ct) => inner.ListBackupAgentsAsync(c, ct);
        public Task<Result> RemoveBackupJobsAsync(string c, CancellationToken ct) => inner.RemoveBackupJobsAsync(c, ct);
        public Task<Result> RemoveRestoreJobsAsync(string c, string sh, CancellationToken ct) => inner.RemoveRestoreJobsAsync(c, sh, ct);
    }

    // SQL-двойник: скаляр-гвард «роль уже есть» (паттерн BackupProcessTests.FakeSql).
    private sealed class StubDb : ISqlExecutor
    {
        public Task<Result> ExecuteAsync(string dsn, string sql, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result<object?>> ExecuteScalarAsync(string dsn, string sql, CancellationToken ct)
            => Task.FromResult(Result<object?>.Success(null));

        public Task<Result> EnsureDatabaseAsync(string dsn, string dbname, CancellationToken ct)
            => Task.FromResult(Result.Success());
    }

    // Секрет-стаб: пер-кластерные креды «уже есть» (паттерн RestoreProcessTests).
    private sealed class StubSecrets : IClusterSecretEnsurer
    {
        public Task<Result<ClusterCredentials>> EnsureAsync(
            string cluster, ClusterConfig config, CancellationToken ct)
            => Task.FromResult(Result<ClusterCredentials>.Success(new ClusterCredentials(
                new AppCredentials("app", "pw"), "moverpw000000000000000000000000A",
                new AppCredentials("bucket_admin", "bapw"),
                "backuppw00000000000000000000000A")));
    }
}
