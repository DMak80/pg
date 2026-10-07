using System.Text.Json;
using Npgsql;
using PgWorker.Core.Templates;
using Shared.Etcd.Client;
using PgWorker.Etcd.Parsing;
using PgWorker.IntegrationTests.Docker;
using PgWorker.Provisioning.Sql;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// E2E verify-джобов (t24/ревизия 8 — разбиение E2eBackupScenarios): пара
// Verify_Ok/Verify_Corruption (~2,5 мин суммарно — под порогом класса).
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
public class E2eBackupVerifyScenarios
{
    private const string Bucket = "pgworker-backups";

    // Окружение Fact'а (своя сеть/etcd/MinIO); создаётся в начале каждого сценария.
    private E2eEnvironment Fx = null!;

    private string Endpoint => Fx.EtcdEndpoint;

    private EtcdGateway G => Fx.Gateway;

    // pgw-backup-verify-* → verify.state=OK + checked_unix; парсеры без parseErrors
    [Fact]
    public async Task Backup_Verify_Ok_OnCreate()
    {
        // Arrange — окружение с MinIO; кластер bkvrfy<тег прогона>; policy on_create=true
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await E2eEnvironment.StartAsync("bk-verify", withMinio: true, ct: ct);
        Fx = fx;
        var cluster = $"bkvrfy{Fx.ClusterTag}";
        await SeedClusterAsync(cluster);
        await G.PutAsync(Endpoint, $"/pgworker/backups/{cluster}/policy",
            """{"full_max_age_sec":86400,"verify":{"on_create":true,"interval_sec":0}}""", null, ct);
        await using var app = await StartBackupHostAsync("bkverify", ct);

        // Act 1 — фаза PENDING пройдена: в ключе PENDING (t02 пишет при COMPLETED)
        // и/или жив контейнер pgw-backup-verify-<C>-* (PENDING держится в ключе
        // до итога — стабильное условие; контейнер — свидетельство джоба)
        var sawPending = await E2eFixture.WaitForAsync(async () =>
        {
            if ((await FullKeysAsync(cluster, "shard1"))
                .Any(f => f.Value.Contains(""""verify":{"state":"PENDING"""")))
                return true;
            var containers = await Fx.RunDockerAsync(
                ["ps", "-a", "--format", "{{.Names}}", "--filter", $"name=pgw-backup-verify-{cluster}-"], ct);
            return containers.Length > 0;
        }, TimeSpan.FromSeconds(180), ct);
        sawPending.Should().BeTrue("on_create: verify обязан стартовать (PENDING в ключе / контейнер pgw-backup-verify-*)");

        // Act 2 — ждём verify.state=OK (бюджет 300 c: полный ~минуты + verify-скачивание)
        var verified = await E2eFixture.WaitForAsync(async () =>
            (await FullKeysAsync(cluster, "shard1")).Any(f => f.Value.Contains(""""verify":{"state":"OK"""")),
            TimeSpan.FromSeconds(300), ct);

        // Assert 1 — OK + checked_unix (фаза PENDING зафиксирована Act 1)
        verified.Should().BeTrue("on_create: verify должен дойти до OK (AC1)");
        var done = (await FullKeysAsync(cluster, "shard1")).Single(f => f.Value.Contains("COMPLETED"));
        var status = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(done.Value)!;
        status["verify"].GetProperty("state").GetString().Should().Be("OK");
        status["verify"].GetProperty("checked_unix").GetInt64().Should().BeGreaterThan(0);

        // Assert 2 — verify-джоб отработал и снесён (контейнер/volume-префиксы чисты)
        var cleaned = await E2eFixture.WaitForAsync(async () =>
        {
            var containers = await Fx.RunDockerAsync(
                ["ps", "-a", "--format", "{{.Names}}", "--filter", $"name=pgw-backup-verify-{cluster}-"], ct);
            var volumes = await Fx.RunDockerAsync(
                ["volume", "ls", "-q", "--filter", $"name=pgw-backup-verify-{cluster}-"], ct);
            return containers.Length == 0 && volumes.Length == 0;
        }, TimeSpan.FromSeconds(60), ct);
        cleaned.Should().BeTrue("verify-джоб и volume сносятся после итога (AC8)");

        // Assert 3 — воркерный и панельный парсеры читают без parseErrors (AC1);
        // Kv общий (Shared.Etcd.Client, t08) — тот же набор подаётся в оба парсера
        var kvs = (await G.RangeAsync(Endpoint, $"/pgworker/backups/{cluster}/", ct)).Value;
        var parsed = BackupsParser.Parse(kvs, out var parseErrors);
        parseErrors.Should().BeEmpty();
        var panel = AdminPanel.Etcd.Parsing.BackupsParser.Parse(kvs);
        panel.Errors.Should().BeEmpty();
        panel.Clusters.Single(c => c.Cluster == cluster).ShardVerifyFailures.Should().BeEmpty();
    }

    // AAA: порча набора (AC2): удалить объект из full/<id>/ в MinIO → policy
    // interval_sec мал → перепроверка → verify.state=FAILED + error; переснятия
    // в окне теста нет (full_max_age_sec велик)

    // в окне теста нет (full_max_age_sec велик)
    [Fact]
    public async Task Backup_Verify_Corruption_Fails()
    {
        // Arrange — окружение + кластер; interval_sec=5 форсирует перепроверку
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await E2eEnvironment.StartAsync("bk-corrupt", withMinio: true, ct: ct);
        Fx = fx;
        var cluster = $"bkcrpt{Fx.ClusterTag}";
        await SeedClusterAsync(cluster);
        await G.PutAsync(Endpoint, $"/pgworker/backups/{cluster}/policy",
            """{"full_max_age_sec":86400,"verify":{"on_create":true,"interval_sec":5}}""", null, ct);
        await using var app = await StartBackupHostAsync("bkcorrupt", ct);

        // Assert 1 — первый verify OK (как в маркере)
        var firstOk = await E2eFixture.WaitForAsync(async () =>
            (await FullKeysAsync(cluster, "shard1")).Any(f => f.Value.Contains(""""verify":{"state":"OK"""")),
            TimeSpan.FromSeconds(300), ct);
        firstOk.Should().BeTrue("исходный набор валиден");
        var done = (await FullKeysAsync(cluster, "shard1")).Single(f => f.Value.Contains("COMPLETED"));
        var id = done.Key.Split('/').Last();

        // Act — портим: mc rm один объект из full/<id>/ (PG_VERSION из листинга набора)
        await Fx.RunDockerAsync(
            ["run", "--rm", "--network", Fx.NetName, "--entrypoint", "/bin/sh", E2eEnvironment.McImage,
                "-c", $"mc alias set t http://e2e-minio:9000 minioadmin minioadmin >/dev/null"
                      + $" && mc rm t/{Bucket}/{cluster}/shard1/full/{id}/PG_VERSION"], ct);

        // Assert 2 — перепроверка по interval → FAILED + error (бюджет 180 c:
        // interval 5 c + тик + джоб со скачиванием)
        var failed = await E2eFixture.WaitForAsync(async () =>
            (await FullKeysAsync(cluster, "shard1")).Any(f => f.Value.Contains(""""verify":{"state":"FAILED"""")),
            TimeSpan.FromSeconds(180), ct);
        failed.Should().BeTrue("порча набора обязана дать verify FAILED (AC2)");
        var corrupted = (await FullKeysAsync(cluster, "shard1")).Single(f => f.Value.Contains("FAILED"));
        var corruptedStatus = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(corrupted.Value)!;
        corruptedStatus["verify"].GetProperty("error").GetString().Should().NotBeNullOrEmpty("причина pg_verifybackup — в verify.error");

        // Assert 3 — в момент провала verify переснятие не УСПЕЛО завершиться.
        // Точная механика (задача 11): после verify FAILED валидных полных нет →
        // IsDue=true СРАЗУ; переснятие сдерживает только BackoffPassed (n растёт
        // и от verify-фейлов; окно Retry.BaseSec=2 c из StartBackupHostAsync) —
        // новая ПОПЫТКА (PLANNED/RUNNING-ключ) допустима, но полный снимается
        // минуты → COMPLETED в момент этого ассерта обязан быть один.
        (await FullKeysAsync(cluster, "shard1")).Count(f => f.Value.Contains("COMPLETED"))
            .Should().Be(1, "новый COMPLETED-полный не успевает появиться в момент провала verify");
    }

    // ===== Дрилл восстановимости (reliability t02, spec Фаза 5) =====

    // Минимальный снапшот панели для правил дрилла («панельный алерт» в E2E:

    // Минимальный снапшот панели для правил дрилла («панельный алерт» в E2E:
    // фактические kvs кластера кормят правило — образец панельных юнитов).
    private static AdminPanel.Core.EtcdSnapshot SnapshotWith(
        AdminPanel.Core.ClusterBackupsInfo panelCluster, string activeCluster, string shard)
        => new(
            DateTimeOffset.UtcNow,
            new AdminPanel.Core.EtcdStatus(true, [], [], [], null, false, DateTimeOffset.UtcNow, 0),
            [new AdminPanel.Core.ClusterInfo(
                activeCluster, activeCluster, 2, 1755800000, AdminPanel.Core.ClusterState.Active,
                [new AdminPanel.Core.ShardInfo(
                    shard, "", [""], 0, null, null, 1, null, [], null)],
                [], [])],
            [], [], [], [panelCluster], [], [], [], [], [], [], 0);

    private static AdminPanel.Core.Alerting.AlertContext DefaultAlertContext()
        => new(null, DateTimeOffset.UtcNow, 3);

    // AAA (AC1/AC3/AC4): первый дрилл стартует сам после COMPLETED-полного,

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
