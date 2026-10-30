using System.Text.Json;
using Npgsql;
using PgWorker.Core.Templates;
using Shared.Etcd.Client;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Sql;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// E2E джобов полных бэкапов (t24/ревизия 8 — разбиение E2eBackupScenarios;
// прецедент копирования хелперов — E2eRestoreScenarios.cs): суточный полный,
// deprovisioning-чистка, ротация backup_exec (~3,9 мин суммарно). Факты внутри
// класса последовательны (collection-per-class), класс — параллельная единица.
// E2E полных бэкапов (t02, spec §7.1–7.4): изолированное окружение E2eEnvironment
// (своя docker-сеть, свой etcd, СВОЯ ownS3-нода — withS3Node:true; порт динамический)
// + операции бэкапов в приёмниках WalReceiver нод (arch/19 §5) + живой кластер воркером с Backups:Enabled=true.
// COMPLETED с полными полями и объектами в S3; FAILED по недоступному S3 с
// переснятием новым id; deprovisioning чистит джобы и префикс; ротация
// backup_password включает backup_exec. Каждый Fact — своё окружение: методы
// оставляют после себя Active-кластеры, и без per-method изоляции воркер
// следующего Fact'а подхватывал бы чужие джобы (инцидент Release). Имя кластера сценария —
// {slug}{Fx.ClusterTag} (уникально на прогон, docs/e2e-isolation.md §1):
// движковые контейнеры/тома pgw-*-<C>-* опознаются teardown'ом окружения.
public sealed class E2eBackupJobsScenarios(E2eBackupJobsScenariosFx Ctx) : IClassFixture<E2eBackupJobsScenariosFx>
{
    private E2eEnvironment Fx => Ctx.Fx;

    private const string Bucket = "pgworker-backups";

    // t36: пул E2E (однонодовый — ownS3-ноды окружения; etcd у каждого Fact'а свой).
    private const string Pool = "bkpool";


    private string Endpoint => Fx.EtcdEndpoint;

    private EtcdGateway G => Fx.Gateway;

    // AAA: полный суточный цикл — PLANNED→RUNNING→UPLOADING→COMPLETED,
    // поля канона, объекты full/<id>/ + wal/ в S3, чистка контейнера/volume
    [Fact]
    public async Task Backup_FullDaily_Completes()
    {
        // Arrange — кластер bkshop<тег прогона> + policy (verify on_create) + воркер
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromMinutes(3)); // потолок теста 3 мин: неверное ожидание не может длиться дольше (приказ владельца)
        var ct = cts.Token;
        var cluster = $"bkshop{Fx.ClusterTag}";
        await SeedClusterAsync(cluster);
        // t36: пул + policy с секцией s3 (без неё подсистема кластера молчит).
        await Fx.SeedS3PoolAsync(Pool, ct);
        await G.PutAsync(Endpoint, $"/pgworker/backups/{cluster}/policy",
            $$"""{"full_max_age_sec":3600,"verify":{"on_create":true},{{E2eEnvironment.PoolS3(Pool)}}}""", null, ct);
        await using var app = await StartBackupHostAsync("bkfull", ct);

        // Act/Assert 1 — COMPLETED на shard1 за ≤ 300 c
        var completed = await E2eFixture.WaitForAsync(
            async () => (await FullKeysAsync(cluster, "shard1")).Any(f => f.Value.Contains("COMPLETED")),
            TimeSpan.FromSeconds(90), ct);
        completed.Should().BeTrue("the full backup must reach COMPLETED");

        var done = (await FullKeysAsync(cluster, "shard1")).Single(f => f.Value.Contains("COMPLETED"));
        var status = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(done.Value)!;
        status["state"].GetString().Should().Be("COMPLETED");
        var walSeg = status["wal_start_segment"].GetString();
        walSeg.Should().NotBeNullOrEmpty("wal_start_segment is filled from UPLOADING");
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
        listing.Should().Contain($"wal/{walSeg}", "the closed segment of the -X stream set is duplicated into the wal/ prefix");

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
        cleaned.Should().BeTrue("the ephemeral job and staging volume are removed after the outcome");
    }

    // AAA: deprovisioning не переживают джобы и префикс /pgworker/backups/<C>/
    [Fact]
    public async Task Backup_Deprovision_CleansPrefix()
    {
        // Arrange — кластер bkclean; ждём появления первой попытки (джоб жив)
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromMinutes(3)); // потолок теста 3 мин: неверное ожидание не может длиться дольше (приказ владельца)
        var ct = cts.Token;
        var cluster = $"bkclean{Fx.ClusterTag}";
        await SeedClusterAsync(cluster);
        // t36: policy с секцией s3 — планирование полных стартует только с пулом.
        await Fx.SeedS3PoolAsync(Pool, ct);
        await G.PutAsync(Endpoint, $"/pgworker/backups/{cluster}/policy",
            $"{{\"full_max_age_sec\":3600,{E2eEnvironment.PoolS3(Pool)}}}", null, ct);
        await using var app = await StartBackupHostAsync("bkclean", ct);
        // Полл с защитой транзиента (паттерн остальных сценариев): чтение fulls
        // на старте контура может транзиентно отказать → .Value null → NRE
        // вместо честного повтора полла.
        var started = await E2eFixture.WaitForAsync(async () =>
        {
            try
            {
                return (await FullKeysAsync(cluster, "shard1")).Count > 0;
            }
            catch (Exception)
            {
                return false; // транзиент чтения — попытка повторится поллом
            }
        }, TimeSpan.FromSeconds(90), ct);
        started.Should().BeTrue("the subsystem must start the first attempt");

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
        }, TimeSpan.FromSeconds(90), ct);
        cleaned.Should().BeTrue("deprovisioning must remove the jobs, volumes and the backup prefix");
    }

    // AAA: ротация per-cluster секретов включает backup_exec: NEW-пароль
    // подключается к мастеру ролью backup_exec (R2-гвард, spec §7.4)
    [Fact]
    public async Task Backup_Rotator_IncludesBackupExec()
    {
        // Arrange — кластер bkrot с завершённым полным; OLD backup_password
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromMinutes(3)); // потолок теста 3 мин: неверное ожидание не может длиться дольше (приказ владельца)
        var ct = cts.Token;
        var cluster = $"bkrot{Fx.ClusterTag}";
        await SeedClusterAsync(cluster);
        // t36: policy с секцией s3 — полный (до ротации) пишется только с пулом.
        await Fx.SeedS3PoolAsync(Pool, ct);
        await G.PutAsync(Endpoint, $"/pgworker/backups/{cluster}/policy",
            $"{{\"full_max_age_sec\":3600,{E2eEnvironment.PoolS3(Pool)}}}", null, ct);
        await using var app = await StartBackupHostAsync("bkrot", ct);
        var completed = await E2eFixture.WaitForAsync(
            async () => (await FullKeysAsync(cluster, "shard1")).Any(f => f.Value.Contains("COMPLETED")),
            TimeSpan.FromSeconds(90), ct);
        completed.Should().BeTrue("before the rotation there must be a completed full (G2 alive on every tick)");

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
        }, TimeSpan.FromSeconds(90), ct); // t03: фон тяжелее (WAL-агенты) — 120 c тесно
        if (!rotated)
        {
            // диагностика: журнал ротации (last_error) + живая заявка + пароль
            var workKv = await GetOrNullAsync($"/pgworker/work/{cluster}");
            var ticketKv = await GetOrNullAsync($"/pgworker/rotations/{cluster}");
            var pwKv = await GetOrNullAsync($"/clusters/{cluster}/backup_password");
            throw new ApplicationException(
                $"the rotation did not finish: journal=[{workKv?.Value[..Math.Min(400, workKv.Value.Length)]}] " +
                $"ticket=[{ticketKv?.Value ?? "-"}] password_changed={(pwKv!.Value != oldPassword)}");
        }
        rotated.Should().BeTrue("the rotation must overwrite backup_password and close the request");
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
        connected.Should().BeTrue("the backup_exec role accepts the new password (the R2 guard)");
    }

    // Воркер с включённой подсистемой бэкапов: пул — policy.s3 (etcd), bucket и
    // env-профиль кредов — env (t36); операции — в приёмниках нод (arch/19 §5), ускоренный бэкофф.
    private Task<HostInstance> StartBackupHostAsync(
        string name, CancellationToken ct,
        IReadOnlyDictionary<string, string>? extraEnv = null)
    {
        var env = new Dictionary<string, string>
        {
            ["PgWorker__Backups__Enabled"] = "true",
            ["PgWorker__Backups__S3__Bucket"] = Bucket,
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

    private async Task<Kv?> GetOrNullAsync(string key)
        => (await G.GetAsync(Endpoint, key, TestContext.Current.CancellationToken)).Value;

    // mc ls --recursive s3-пути кластера (mc в сети ownS3-ноды окружения — по алиасу).
    private async Task<string> McLsAsync(string path)
        => await Fx.RunDockerAsync(
        [
            "run", "--rm", "--network", Fx.NetName, "--entrypoint", "/bin/sh", E2eEnvironment.McImage,
            "-c", $"mc alias set t http://e2e-owns3:9000 {E2eEnvironment.S3RootUser} {E2eEnvironment.S3RootPassword} >/dev/null && mc ls --recursive t/{Bucket}/{path}",
        ], TestContext.Current.CancellationToken);

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
            await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_mem", "2Gi", null, ct);
        }

        for (var i = 0; i < 2; i++)
        {
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/buckets/routing/bucket_{i}", $"shard{i + 1}", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/buckets/status/bucket_{i}",
                """{"state":"NOT_INITIALIZED"}""", null, ct);
        }
    }
}

/// <summary>Компаньон-фикстура окружения класса (полный контур на класс:
/// сеть/etcd/CA/окно портов; docs/e2e-isolation.md §1).</summary>
public sealed class E2eBackupJobsScenariosFx : E2eContourFixture
{
    protected override string Slug => "bk-full";
    protected override bool WithS3Node => true;
}
