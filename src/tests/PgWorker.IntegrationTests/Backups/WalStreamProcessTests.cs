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
        "pgworker-backup:test", "http://minio", "http://minio-agent", null,
        "bkt", "ak", "sk", true,
        "/backup-staging", null, null, null,
        verify, lag, stale);

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
}
