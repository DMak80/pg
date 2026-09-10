using System.Text.Json;
using Npgsql;
using PgWorker.Etcd.Client;
using PgWorker.IntegrationTests.Docker;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// E2E полных бэкапов (t02, spec §7.1–7.4): изолированное окружение E2eEnvironment
// (своя docker-сеть, свой etcd, СВОЙ MinIO — withMinio:true; порт динамический)
// + образ pgworker-backup:e2e + живой кластер воркером с Backups:Enabled=true.
// COMPLETED с полными полями и объектами в S3; FAILED по недоступному S3 с
// переснятием новым id; deprovisioning чистит джобы и префикс; ротация
// backup_password включает backup_exec. Каждый Fact — своё окружение: методы
// оставляют после себя Active-кластеры, и без per-method изоляции воркер
// следующего Fact'а подхватывал бы чужие джобы (инцидент Release).
public class E2eBackupScenarios
{
    private const string Bucket = "pgworker-backups";

    // Окружение Fact'а (своя сеть/etcd/MinIO); создаётся в начале каждого сценария.
    private E2eEnvironment Fx = null!;

    private string Endpoint => Fx.EtcdEndpoint;

    private EtcdGateway G => Fx.Gateway;

    // AAA: полный суточный цикл — PLANNED→RUNNING→UPLOADING→COMPLETED,
    // поля канона, объекты full/<id>/ + wal/ в S3, чистка контейнера/volume
    [Fact]
    public async Task Backup_FullDaily_Completes()
    {
        // Arrange — кластер bkshop + policy (verify on_create) + воркер с бэкап-комплектом
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await E2eEnvironment.StartAsync("bk-full", withMinio: true, ct: ct);
        Fx = fx;
        const string cluster = "bkshop";
        await SeedClusterAsync(cluster);
        await G.PutAsync(Endpoint, $"/pgworker/backups/{cluster}/policy",
            """{"full_max_age_sec":3600,"verify":{"on_create":true}}""", null, ct);
        await using var app = await StartBackupHostAsync("bkfull", ct);

        // Act/Assert 1 — COMPLETED на shard1 за ≤ 300 c
        var completed = await E2eFixture.WaitForAsync(
            async () => (await FullKeysAsync(cluster, "shard1")).Any(f => f.Value.Contains("COMPLETED")),
            TimeSpan.FromSeconds(300), ct);
        completed.Should().BeTrue("полный бэкап должен дойти до COMPLETED");

        var done = (await FullKeysAsync(cluster, "shard1")).Single(f => f.Value.Contains("COMPLETED"));
        var status = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(done.Value)!;
        status["state"].GetString().Should().Be("COMPLETED");
        var walSeg = status["wal_start_segment"].GetString();
        walSeg.Should().NotBeNullOrEmpty("wal_start_segment заполняется с UPLOADING");
        status["node"].GetString().Should().NotBeNullOrEmpty();
        status["role"].GetString().Should().BeOneOf("replica", "master");
        status["started_unix"].GetInt64().Should().BeGreaterThan(0);
        status["finished_unix"].GetInt64().Should().BeGreaterThanOrEqualTo(status["started_unix"].GetInt64());
        status["size_bytes"].GetInt64().Should().BeGreaterThan(0);
        status["verify"].GetProperty("state").GetString().Should().Be("PENDING");

        // Assert 2 — объекты в S3: full/<id>/ с manifest и pg_wal/ + wal/<seg>
        var id = done.Key.Split('/').Last();
        var listing = await McLsAsync($"{cluster}/shard1/");
        listing.Should().Contain($"full/{id}/backup_manifest");
        listing.Should().Contain($"full/{id}/PG_VERSION");
        listing.Should().Contain($"full/{id}/pg_wal/");
        listing.Should().Contain($"wal/{walSeg}", "закрытый сегмент набора -X stream дублируется в wal/-префикс");

        // Assert 3 — чистка: контейнера и staging volume нет. COMPLETED пишется
        // в etcd ДО CleanupJobAsync (BackupProcess.PutAsync → CleanupJobAsync) —
        // между итогом и удалением джоба/volume есть окно, поэтому чистка ждётся
        // ограниченно (паттерн Backup_Deprovision_CleansPrefix), а не мгновенно.
        var cleaned = await E2eFixture.WaitForAsync(async () =>
        {
            var jobContainers = await Fx.RunDockerAsync(
            ["ps", "-a", "--format", "{{.Names}}", "--filter", $"name=pgw-backup-full-{cluster}-"], ct);
            if (jobContainers.Length > 0)
                return false;
            var stagingVolumes = await Fx.RunDockerAsync(
            ["volume", "ls", "-q", "--filter", $"name=pgw-backup-{cluster}-"], ct);
            return stagingVolumes.Length == 0;
        }, TimeSpan.FromSeconds(60), ct);
        cleaned.Should().BeTrue("ephemeral-джоб и staging volume удаляются после итога");
    }

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
        const string cluster = "bkbads3";
        await SeedClusterAsync(cluster);
        await using var app = await StartBackupHostAsync(
            "bkbads3", ct, s3EndpointOverride: "http://host.docker.internal:1");

        // Act/Assert 1 — первая попытка FAILED с error ≤ 300 c
        var failed = await E2eFixture.WaitForAsync(
            async () => (await FullKeysAsync(cluster, "shard1")).Any(f => f.Value.Contains("FAILED")),
            TimeSpan.FromSeconds(300), ct);
        failed.Should().BeTrue("попытка с недоступным S3 должна упасть в FAILED");
        var failedKv = (await FullKeysAsync(cluster, "shard1")).Single(f => f.Value.Contains("FAILED"));
        var status = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(failedKv.Value)!;
        status["error"].GetString().Should().NotBeNullOrEmpty("причина фиксируется в статусе");

        // Assert 2 — переснятие: вторая попытка с ДРУГИМ id (Retry BaseSec=2)
        var retried = await E2eFixture.WaitForAsync(
            async () => (await FullKeysAsync(cluster, "shard1")).Count >= 2,
            TimeSpan.FromSeconds(120), ct);
        retried.Should().BeTrue("бэкофф 2 c должен запустить переснятие новым id");
    }

    // AAA: deprovisioning не переживают джобы и префикс /pgworker/backups/<C>/
    [Fact]
    public async Task Backup_Deprovision_CleansPrefix()
    {
        // Arrange — кластер bkclean; ждём появления первой попытки (джоб жив)
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await E2eEnvironment.StartAsync("bk-clean", withMinio: true, ct: ct);
        Fx = fx;
        const string cluster = "bkclean";
        await SeedClusterAsync(cluster);
        await using var app = await StartBackupHostAsync("bkclean", ct);
        var started = await E2eFixture.WaitForAsync(
            async () => (await FullKeysAsync(cluster, "shard1")).Count > 0,
            TimeSpan.FromSeconds(300), ct);
        started.Should().BeTrue("подсистема должна начать первую попытку");

        // Act — state=TO_REMOVE (панель-семантика §4.2)
        var config = await G.GetAsync(Endpoint, $"/clusters/{cluster}/config", ct);
        config.Value.Should().NotBeNull();
        var doc = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(config.Value!.Value)!;
        doc["state"] = JsonSerializer.SerializeToElement("TO_REMOVE");
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/config",
            JsonSerializer.Serialize(doc), null, ct);

        // Assert — префикс бэкапов пуст; джобов и staging volume нет
        var cleaned = await E2eFixture.WaitForAsync(async () =>
        {
            if ((await FullKeysAsync(cluster, "shard1")).Count > 0
                || (await FullKeysAsync(cluster, "shard2")).Count > 0)
                return false;
            var containers = await Fx.RunDockerAsync(
            ["ps", "-a", "--format", "{{.Names}}", "--filter", $"name=pgw-backup-full-{cluster}-"], ct);
            if (containers.Length > 0)
                return false;
            var volumes = await Fx.RunDockerAsync(
            ["volume", "ls", "-q", "--filter", $"name=pgw-backup-{cluster}-"], ct);
            return volumes.Length == 0;
        }, TimeSpan.FromSeconds(180), ct);
        cleaned.Should().BeTrue("deprovisioning должен убрать джобы, volume и префикс бэкапов");
    }

    // AAA: ротация per-cluster секретов включает backup_exec: NEW-пароль
    // подключается к мастеру ролью backup_exec (R2-гвард, spec §7.4)
    [Fact]
    public async Task Backup_Rotator_IncludesBackupExec()
    {
        // Arrange — кластер bkrot с завершённым полным; OLD backup_password
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await E2eEnvironment.StartAsync("bk-rot", withMinio: true, ct: ct);
        Fx = fx;
        const string cluster = "bkrot";
        await SeedClusterAsync(cluster);
        await using var app = await StartBackupHostAsync("bkrot", ct);
        var completed = await E2eFixture.WaitForAsync(
            async () => (await FullKeysAsync(cluster, "shard1")).Any(f => f.Value.Contains("COMPLETED")),
            TimeSpan.FromSeconds(300), ct);
        completed.Should().BeTrue("до ротации должен быть завершённый полный (G2 жил каждый тик)");

        var oldPassword = (await G.GetAsync(Endpoint, $"/clusters/{cluster}/backup_password", ct)).Value!.Value;

        // Act — заявка ротации (формат панели §9.8)
        await G.PutAsync(Endpoint, $"/pgworker/rotations/{cluster}",
            $$"""{"requested_unix":{{DateTimeOffset.UtcNow.ToUnixTimeSeconds()}},"requested_by":"e2e"}""",
            null, ct);

        // Assert 1 — backup_password сменился, заявка удалена
        var rotated = await E2eFixture.WaitForAsync(async () =>
        {
            var current = await G.GetAsync(Endpoint, $"/clusters/{cluster}/backup_password", ct);
            return current.Value is { } kv && kv.Value != oldPassword
                && await GetOrNullAsync($"/pgworker/rotations/{cluster}") is null;
        }, TimeSpan.FromSeconds(120), ct);
        rotated.Should().BeTrue("ротация должна перезаписать backup_password и закрыть заявку");
        var newPassword = (await G.GetAsync(Endpoint, $"/clusters/{cluster}/backup_password", ct)).Value!.Value;

        // Assert 2 — NEW-пароль подключается user=backup_exec к мастеру shard1:
        // DSN-ключ — multi-host с чужими кредами (bucket_admin), дубликаты
        // User/Password в строке недопустимы — собираем параметры явно
        // (пары host:port из ключа, Target Session Attributes=read-write).
        var dsn = (await GetOrNullAsync($"/clusters/{cluster}/shards/shard1/dsn"))!.Value;
        dsn.Should().Contain(",", "multi-host DSN");
        var hosts = System.Text.RegularExpressions.Regex.Match(dsn, "host=([^ ]+)").Groups[1].Value.Split(',');
        var ports = System.Text.RegularExpressions.Regex.Match(dsn, "port=([^ ]+)").Groups[1].Value.Split(',');
        var multiHost = string.Join(",", hosts.Zip(ports, (h, p) => $"{h}:{p}"));
        var connected = await E2eFixture.WaitForAsync(async () =>
        {
            try
            {
                await using var con = new NpgsqlConnection(
                    $"Host={multiHost};Database={cluster};Username=backup_exec;Password={newPassword};" +
                    "Timeout=5;SSL Mode=Require;Trust Server Certificate=true;Target Session Attributes=read-write");
                await con.OpenAsync(ct);
                await using var cmd = new NpgsqlCommand("SELECT 1", con);
                return await cmd.ExecuteScalarAsync(ct) is 1;
            }
            catch (NpgsqlException)
            {
                return false; // failover-окно/рестарт — повторим
            }
        }, TimeSpan.FromSeconds(60), ct);
        connected.Should().BeTrue("роль backup_exec принимает новый пароль (гвард R2)");
    }

    // ===== Хелперы =====

    // Воркер с включённой подсистемой бэкапов: S3 на локальный MinIO,
    // образ pgworker-backup:e2e, ускоренный бэкофф.
    private Task<HostInstance> StartBackupHostAsync(
        string name, CancellationToken ct, string? s3EndpointOverride = null)
        => Fx.StartHostAsync(name, extraEnv: new Dictionary<string, string>
        {
            ["PgWorker__Backups__Enabled"] = "true",
            ["PgWorker__Backups__S3__Endpoint"] = s3EndpointOverride ?? Fx.S3Endpoint,
            ["PgWorker__Backups__S3__Bucket"] = Bucket,
            ["PgWorker__Backups__S3__AccessKey"] = "minioadmin",
            ["PgWorker__Backups__S3__SecretKey"] = "minioadmin",
            ["PgWorker__Backups__Job__Image"] = E2eEnvironment.JobImage,
            ["PgWorker__Backups__Retry__BaseSec"] = "2",
            ["PgWorker__Backups__Retry__MaxSec"] = "4",
        }, ct: ct);

    private async Task<IReadOnlyList<Kv>> FullKeysAsync(string cluster, string shard)
        => (await G.RangeAsync(Endpoint, $"/pgworker/backups/{cluster}/{shard}/full/",
            TestContext.Current.CancellationToken)).Value;

    private async Task<Kv?> GetOrNullAsync(string key)
        => (await G.GetAsync(Endpoint, key, TestContext.Current.CancellationToken)).Value;

    // mc ls --recursive s3-пути кластера (mc в сети MinIO окружения — по алиасу).
    private async Task<string> McLsAsync(string path)
        => await Fx.RunDockerAsync(
        [
            "run", "--rm", "--network", Fx.NetName, "--entrypoint", "/bin/sh", E2eEnvironment.McImage,
            "-c", $"mc alias set t http://e2e-minio:9000 minioadmin minioadmin >/dev/null && mc ls --recursive t/{Bucket}/{path}",
        ], TestContext.Current.CancellationToken);

    // Сид кластера в стиле панели (копия E2eRotateScenarios.SeedClusterAsync).
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
