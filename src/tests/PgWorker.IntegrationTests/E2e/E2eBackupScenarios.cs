using System.Text.Json;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using FluentAssertions;
using Npgsql;
using PgWorker.Backups;
using PgWorker.Core.Templates;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Parsing;
using PgWorker.IntegrationTests.Docker;
using PgWorker.Provisioning.Sql;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// E2E t03 (AC1/AC2/AC7): кластер provisioned + MinIO → INSERT-нагрузка с
// pg_switch_wal → сегменты в MinIO, ключ wal ACTIVE, цепочка непрерывна.
[Collection(E2eCollection.Name)]
public class E2eBackupScenarios(E2eFixture fixture)
{
    private string Endpoint => fixture.EtcdEndpoint;

    private EtcdGateway G => fixture.Gateway;

    [Fact]
    public async Task WalStream_UploadsSegmentsContinuously()
    {
        // Arrange — гейты: docker + образ t02 (до мержа t02 — явный skip)
        DockerTrait.SkipIfUnavailable();
        if (fixture.BackupAgentImage is null)
            Assert.Skip("docker/pgworker-backup.Dockerfile отсутствует — образ агента приходит из t02 (мерж-порядок t03 ← t02)");
        var ct = TestContext.Current.CancellationToken;

        // bucket per-install — ПРЯМЫМ AWSSDK-клиентом (создание bucket — не операция
        // подсистемы, spec §3.1)
        using var s3client = new AmazonS3Client(
            new BasicAWSCredentials("minioadmin", "minioadmin"),
            new AmazonS3Config { ServiceURL = fixture.MinioHostEndpoint, ForcePathStyle = true });
        await s3client.PutBucketAsync(new PutBucketRequest { BucketName = "pgworker-backups-e2e" }, ct);

        // сид кластера "shopb" (как SeedClusterAsync E2eScenarios) → StartHostAsync
        await SeedClusterAsync("shopb");
        await using var p1 = await fixture.StartHostAsync("p1b", ct: ct, backups: true);

        var provisioned = await E2eFixture.WaitForAsync(
            () => ProvisionedAsync("shopb"), TimeSpan.FromSeconds(360), ct);
        provisioned.Should().BeTrue("provisioning должен дойти до DONE до WAL-нагрузки");

        // Мастер shard1 из portalloc (host-порт) + app-пароль из etcd
        var (host, port) = await MasterAsync("shopb", "shard1", ct);
        var appPassword = await fixture.GetAppPasswordAsync("shopb", ct);
        var dsn = $"Host={host};Port={port};Database=shopb;Username=app_user;Password={appPassword}";
        // pg_switch_wal по умолчанию superuser-only (ревью Ф7 №2) — admin-DSN
        // собирается тем же билдером, что и прод-путь воркера.
        var adminDsn = DatabaseProvisioner.BuildAdminDsn(host, port, "postgres",
            new InstallSecrets(E2eFixture.SuPassword, "", "", ""));

        // Act — INSERT-нагрузка: большие строки + pg_switch_wal форсируют закрытие
        // сегментов (без таймаутов, spec §5); switch — через admin-соединение
        await GenerateWalAsync(dsn, adminDsn, ct);

        // Assert 1 — бюджет 120 с: сегменты в MinIO, все без .partial (AC1)
        var backupS3 = new BackupS3(new BackupsRuntimeOptions(
            fixture.BackupAgentImage!, fixture.MinioHostEndpoint, fixture.MinioAgentEndpoint, null,
            "pgworker-backups-e2e", "minioadmin", "minioadmin", true,
            "/backup-staging", null, null, null,
            WalVerifyIntervalSec: 2, WalLagMaxSegments: 100000, WalStaleSec: 600));
        List<WalObject> listed = [];
        var uploaded = await E2eFixture.WaitForAsync(async () =>
        {
            var list = await backupS3.ListWalAsync("shopb", "shard1", ct: ct);
            if (!list.IsSuccess)
                return false;
            listed = [.. list.Value];
            return listed.Count >= 2;
        }, TimeSpan.FromSeconds(120), ct);
        uploaded.Should().BeTrue("сегменты обязаны появиться в MinIO за бюджет (AC1)");
        listed.Select(o => o.Name).Should().OnlyContain(n => !n.EndsWith(".partial"));

        // Assert 2 — etcd-ключ wal: ACTIVE, slot/master, last_uploaded свежий (AC2)
        var writer = new WalStatusWriter(G, [Endpoint]);
        var walRead = await E2eFixture.WaitForAsync(async () =>
        {
            var read = await writer.ReadAsync("shopb", "shard1", ct);
            return read.IsSuccess && read.Value is { State: WalStreamStatus.Active };
        }, TimeSpan.FromSeconds(120), ct);
        walRead.Should().BeTrue("ключ wal обязан перейти в ACTIVE (AC2)");
        var wal = (await writer.ReadAsync("shopb", "shard1", ct)).Value!;
        wal.Slot.Should().Be("pgw_bkp_shopb_shard1");
        wal.MasterNode.Should().NotBeEmpty();
        wal.ChainStartSegment.Should().NotBeEmpty();
        listed.Select(o => o.Name).Should().Contain(wal.LastUploadedSegment,
            "last_uploaded_segment — наблюдаемый факт S3");
        (DateTimeOffset.UtcNow.ToUnixTimeSeconds() - wal.LastUploadedUnix!.Value)
            .Should().BeLessThan(120, "last_uploaded_unix свежий (поток жив)");

        // Assert 3 — цепочка непрерывна (AC3-факт)
        var chainStart = WalFileName.TryParse(wal.ChainStartSegment)!.Value;
        var chain = WalChain.Check(chainStart, listed.Select(o => o.Name));
        chain.IsContinuous.Should().BeTrue($"дыр быть не должно: {chain.GapError}");

        // Assert 4 — контейнер агента running (docker ps) + слот на мастере (AC1)
        var agents = await fixture.RunDockerAsync(
            ["ps", "--filter", "name=pgw-backup-wal-shopb-shard1", "--format", "{{.Names}} {{.State}}"], ct);
        agents.Should().Contain("pgw-backup-wal-shopb-shard1");
        await using var conn = new NpgsqlConnection(dsn);
        await conn.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand(
            "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'pgw_bkp_shopb_shard1')", conn);
        ((bool)(await cmd.ExecuteScalarAsync(ct))!).Should().BeTrue("слот создан воркером на мастере (AC1)");

        // Assert 5 — каркасный парсер t01 читает ключ без parseErrors (AC2)
        var kvs = (await G.RangeAsync(Endpoint, "/pgworker/backups/shopb/", ct)).Value;
        var parsed = BackupsParser.Parse(kvs, out var parseErrors);
        parseErrors.Should().BeEmpty();
        parsed.Value.Should().Contain(b => b.Cluster == "shopb");
    }

    // ===== Хелперы (по образцу E2eScenarios) =====

    // Сид кластера в стиле панели: config NOT_INITIALIZED, 1 шард × replicas=1,
    // routing/status всех N, заявки request_*.
    private async Task SeedClusterAsync(string cluster)
    {
        var ct = TestContext.Current.CancellationToken;
        var config = $$"""
            {"buckets":4,"dbname":"{{cluster}}","created_unix":1755800000,"state":"NOT_INITIALIZED","bucket_admin_password":"{{E2eFixture.BucketAdminPassword}}"}
            """;
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/config", config, null, ct);
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/shard1/replicas", "1", null, ct);
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/shard1/nodes/shard1a/state", "NOT_INITIALIZED", null, ct);
        await G.PutAsync(Endpoint, $"/service/{cluster}-shard1/request_cpu", "1", null, ct);
        await G.PutAsync(Endpoint, $"/service/{cluster}-shard1/request_mem", "2Gi", null, ct);
        for (var i = 0; i < 4; i++)
        {
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/buckets/routing/bucket_{i}", "shard1", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/buckets/status/bucket_{i}",
                """{"state":"NOT_INITIALIZED"}""", null, ct);
        }
    }

    private async Task<bool> ProvisionedAsync(string cluster)
    {
        var config = await G.GetAsync(Endpoint, $"/clusters/{cluster}/config",
            TestContext.Current.CancellationToken);
        if (config.Value is null)
            return false;
        var doc = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(config.Value.Value);
        if (doc is null || doc.ContainsKey("state"))
            return false;
        var dsn = await G.GetAsync(Endpoint, $"/clusters/{cluster}/shards/shard1/dsn",
            TestContext.Current.CancellationToken);
        var node = await G.GetAsync(Endpoint,
            $"/clusters/{cluster}/shards/shard1/nodes/shard1a/state", TestContext.Current.CancellationToken);
        return dsn.Value is not null && node.Value is { Value: "RUNNING" };
    }

    // Мастер шарда: host-порт pg из portalloc (Npgsql с хоста).
    private async Task<(string Host, int Port)> MasterAsync(string cluster, string shard, CancellationToken ct)
    {
        var kv = await G.GetAsync(Endpoint, $"/pgworker/portalloc/{cluster}", ct);
        kv.Value.Should().NotBeNull("portalloc пишется при provisioning");
        var entries = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(kv.Value!.Value)!;
        var master = await G.GetAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/master", ct);
        master.Value.Should().NotBeNull("master-ключ пишется при provisioning");
        var masterName = JsonSerializer.Deserialize<JsonElement>(master.Value!.Value).GetString()!.Split(':')[0];
        var entry = entries[$"{shard}/{masterName}"];
        return (entry.GetProperty("host").GetString()!, entry.GetProperty("pg").GetInt32());
    }

    // Нагрузка: CREATE TABLE + 30 циклов INSERT больших строк (app_user) +
    // pg_switch_wal через admin/superuser-соединение (ревью Ф7 №2: app_user без
    // GRANT вызовет permission denied) — форсированное закрытие сегментов 16 МБ.
    private static async Task GenerateWalAsync(string dsn, string adminDsn, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(dsn);
        await conn.OpenAsync(ct);
        await using (var create = new NpgsqlCommand(
            "CREATE TABLE IF NOT EXISTS wal_load(id bigserial, payload text)", conn))
            await create.ExecuteNonQueryAsync(ct);
        await using var admin = new NpgsqlConnection(adminDsn);
        await admin.OpenAsync(ct);
        for (var i = 0; i < 30; i++)
        {
            await using var insert = new NpgsqlCommand(
                "INSERT INTO wal_load(payload) SELECT repeat('x', 1048576) FROM generate_series(1, 8)", conn);
            await insert.ExecuteNonQueryAsync(ct);
            await using var switchWal = new NpgsqlCommand("SELECT pg_switch_wal()", admin);
            await switchWal.ExecuteScalarAsync(ct);
        }
    }
}
