using System.Text.Json;
using Npgsql;
using PgWorker.Core.Templates;
using Shared.Etcd.Client;
using PgWorker.Etcd.Parsing;
using PgWorker.IntegrationTests.Docker;
using PgWorker.Provisioning.Sql;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// E2E отказа S3 (t24/ревизия 8 — разбиение E2eBackupScenarios): монолитный
// одиночный факт (критический путь ~7 мин, §7) — отдельный класс.
// E2E полных бэкапов (t02, spec §7.1–7.4): изолированное окружение E2eEnvironment
// (своя docker-сеть, свой etcd, СВОЙ MinIO — withMinio:true; порт динамический)
// + образ pgworker-backup:e2e + живой кластер воркером с Backups:Enabled=true.
// COMPLETED с полными полями и объектами в S3; FAILED по недоступному S3 с
// переснятием новым id; deprovisioning чистит джобы и префикс; ротация
// backup_password включает backup_exec. Каждый Fact — своё окружение: методы
// оставляют после себя Active-кластеры, и без per-method изоляции воркер
// следующего Fact'а подхватывал бы чужие джобы (инцидент Release). Имя кластера сценария —
// {slug}{Fx.ClusterTag} (уникально на прогон, docs/e2e-isolation.md §1):
// движковые контейнеры/тома pgw-*-<C>-* опознаются teardown'ом окружения.
public class E2eBackupFailsOnBadS3Scenarios
{
    private const string Bucket = "pgworker-backups";

    // Окружение Fact'а (своя сеть/etcd/MinIO); создаётся в начале каждого сценария.
    private E2eEnvironment Fx = null!;

    private string Endpoint => Fx.EtcdEndpoint;

    private EtcdGateway G => Fx.Gateway;

    // AAA: недоступный S3 → FAILED с error; переснятие НОВЫМ id после бэкоффа
    [Fact]
    public async Task Backup_FailsOnBadS3_RetriesWithNewId()
    {
        // Arrange — S3-endpoint на заведомо закрытом порт 1 (tcpmux; НЕ тестовый
        // хардкод: фиксированный протокольный «всегда закрыт»)
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await E2eEnvironment.StartAsync("bk-bads3", withMinio: true, ct: ct);
        Fx = fx;
        var cluster = $"bkbads3{Fx.ClusterTag}";
        await SeedClusterAsync(cluster);
        await using var app = await StartBackupHostAsync(
            "bkbads3", ct, s3EndpointOverride: "http://host.docker.internal:1");

        // Act/Assert 1 — первая попытка FAILED с error ≤ 480 c: окно включает
        // provisioning (4 ноды, минуты) + джоб (pg_basebackup со spread-чекпоинтом
        // может ждать ближайший чекпоинт) + мгновенный mc-отказ (connection refused
        // mc не ретраит). 300 c на загруженном хосте не хватает (факт t04-гейта).
        var failed = await E2eFixture.WaitForAsync(
            async () => (await FullKeysAsync(cluster, "shard1")).Any(f => f.Value.Contains("FAILED")),
            TimeSpan.FromSeconds(480), ct);
        failed.Should().BeTrue("попытка с недоступным S3 должна упасть в FAILED");
        var failedKv = (await FullKeysAsync(cluster, "shard1")).Single(f => f.Value.Contains("FAILED"));
        var status = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(failedKv.Value)!;
        status["error"].GetString().Should().NotBeNullOrEmpty("причина фиксируется в статусе");

        // Assert 2 — переснятие: вторая попытка с ДРУГИМ id (Retry BaseSec=2)
        var retried = await E2eFixture.WaitForAsync(
            async () => (await FullKeysAsync(cluster, "shard1")).Count >= 2,
            TimeSpan.FromSeconds(240), ct);
        retried.Should().BeTrue("бэкофф 2 c должен запустить переснятие новым id");
    }

    // Воркер с включённой подсистемой бэкапов: S3 на локальный MinIO,
    // образ pgworker-backup:e2e, ускоренный бэкофф.
    private Task<HostInstance> StartBackupHostAsync(
        string name, CancellationToken ct, string? s3EndpointOverride = null,
        IReadOnlyDictionary<string, string>? extraEnv = null)
    {
        var env = new Dictionary<string, string>
        {
            ["PgWorker__Backups__Enabled"] = "true",
            ["PgWorker__Backups__S3__Endpoint"] = s3EndpointOverride ?? Fx.S3Endpoint,
            ["PgWorker__Backups__S3__Bucket"] = Bucket,
            ["PgWorker__Backups__S3__AccessKey"] = "minioadmin",
            ["PgWorker__Backups__S3__SecretKey"] = "minioadmin",
            ["PgWorker__Backups__Job__Image"] = E2eEnvironment.JobImage,
            ["PgWorker__Backups__Wal__AgentImage"] = E2eEnvironment.WalImage,
            ["PgWorker__Backups__Retry__BaseSec"] = "2",
            ["PgWorker__Backups__Retry__MaxSec"] = "4",
        };
        foreach (var (key, value) in extraEnv ?? new Dictionary<string, string>())
            env[key] = value;
        return Fx.StartHostAsync(name, extraEnv: env, ct: ct);
    }

    private async Task<IReadOnlyList<Kv>> FullKeysAsync(string cluster, string shard)
        => (await G.RangeAsync(Endpoint, $"/pgworker/backups/{cluster}/{shard}/full/",
            TestContext.Current.CancellationToken)).Value;

    private async Task SeedClusterAsync(string cluster)
    {
        var ct = TestContext.Current.CancellationToken;
        var config = $$"""
            {"buckets":2,"dbname":"{{cluster}}","created_unix":1755800000,"state":"NOT_INITIALIZED","bucket_admin_password":"{{E2eFixture.BucketAdminPassword}}"}
            """;
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/config", config, null, ct);
        foreach (var shard in new[] { "shard1", "shard2" })
        {
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/replicas", "2", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/nodes/{shard}a/state", "NOT_INITIALIZED", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/nodes/{shard}b/state", "NOT_INITIALIZED", null, ct);
            await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_cpu", "2", null, ct);
            await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_mem", "8Gi", null, ct);
        }

        for (var i = 0; i < 2; i++)
        {
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/buckets/routing/bucket_{i}", $"shard{i + 1}", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/buckets/status/bucket_{i}",
                """{"state":"NOT_INITIALIZED"}""", null, ct);
        }
    }
}
