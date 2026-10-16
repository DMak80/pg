using System.Security.Cryptography;
using PgWorker.Backups;
using PgWorker.IntegrationTests.Docker;
using Shared.Etcd.Client;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// E2E экспорта etcd-снапшотов (t08): изолированное окружение E2eEnvironment
// (своя сеть/etcd/ownS3 — withS3Node:true) + хост-воркер с включённым
// Snapshots:Export. Каждый Fact — своё окружение (per-method изоляция,
// e2e-isolation §1/§3): метрики/ключи/объекты сценария не пересекаются.
public class E2eEtcdSnapshotExportScenarios
{
    private const string Bucket = "pgworker-backups";

    // t36: bucket слепочной пары (не пуловый pgworker-backups).
    private const string SnapshotBucket = "pgworker-etcd-snapshots";
    private E2eEnvironment Fx = null!;
    private string Endpoint => Fx.EtcdEndpoint;
    private EtcdGateway G => Fx.Gateway;

    // Хост-воркер с включённым экспортом (t36: ОТДЕЛЬНАЯ слепочная пара
    // PgWorker:Snapshots:Export:S3 — свой bucket pgworker-etcd-snapshots,
    // префикс etcd/; Endpoint — localhost-вид воркера: sink ходит из процесса).
    private async Task<HostInstance> StartExportHostAsync(string name, CancellationToken ct,
        int intervalMin = 1, int retrySec = 5, int retention = 28, string? s3Override = null)
    {
        // bucket слепков (t36): отдельный от пулового — создаёт окружение сценария
        // (прод: per-install, arch/19 §7; воркер bucket не создаёт).
        await Fx.RunDockerAsync(
        [
            "run", "--rm", "--network", Fx.NetName, "--entrypoint", "/bin/sh", E2eEnvironment.McImage,
            "-c", $"mc alias set t http://e2e-owns3:9000 {E2eEnvironment.S3RootUser} {E2eEnvironment.S3RootPassword} >/dev/null"
                  + $" && mc mb --ignore-existing t/{SnapshotBucket}",
        ], ct);
        return await Fx.StartHostAsync(name, snapshotIntervalMin: intervalMin, extraEnv: new Dictionary<string, string>
        {
            ["PgWorker__Snapshots__Export__Enabled"] = "true",
            ["PgWorker__Snapshots__Export__RetryIntervalSec"] = retrySec.ToString(),
            ["PgWorker__Snapshots__Export__RetentionObjects"] = retention.ToString(),
            ["PgWorker__Snapshots__Export__S3__Endpoint"] = s3Override ?? Fx.S3Endpoint,
            ["PgWorker__Snapshots__Export__S3__Bucket"] = SnapshotBucket,
            ["PgWorker__Snapshots__Export__S3__AccessKey"] = "minioadmin",
            ["PgWorker__Snapshots__Export__S3__SecretKey"] = "minioadmin",
        }, ct: ct);
    }

    // S3-цель окружения Fact'а (хост-клиент; t36 — S3ClientOptions).
    private PgWorker.Backups.S3ClientOptions MinioS3() => new(
        Fx.S3Endpoint,
        SnapshotBucket, E2eEnvironment.S3RootUser, E2eEnvironment.S3RootPassword);

    // Runtime-опции подсистемы на окружение Fact'а (bucket per-install).
    private BackupsRuntimeOptions MinioRuntimeForHost() => new(
        Enabled: true,
        S3Bucket: Bucket,
        JobImage: E2eEnvironment.JobImage,
        StagingDir: "/backup-staging",
        WalVerifyIntervalSec: 30,
        WalLagMaxSegments: 1024,
        WalStaleSec: 300);

    // mc ls --recursive префикса etcd/ (mc в сети ownS3-ноды окружения — по алиасу);
    // пути в выводе — ОТНОСИТЕЛЬНО префикса (образец E2eBackupScenarios).
    private async Task<string> McLsAsync(string path)
        => await Fx.RunDockerAsync(
        [
            "run", "--rm", "--network", Fx.NetName, "--entrypoint", "/bin/sh", E2eEnvironment.McImage,
            "-c", $"mc alias set t http://e2e-owns3:9000 {E2eEnvironment.S3RootUser} {E2eEnvironment.S3RootPassword} >/dev/null && mc ls --recursive t/{SnapshotBucket}/{path}",
        ], TestContext.Current.CancellationToken);

    // Имена ключей из mc-листинга (последний токен строки — путь).
    private static IReadOnlyList<string> McKeys(string listing)
        => listing.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(l => l[(l.LastIndexOf(' ') + 1)..])
            .ToList();

    private static string Sha256Hex(byte[] data)
        => Convert.ToHexString(SHA256.HashData(data)).ToLowerInvariant();

    // Чтение объекта bucket'a через AWSSDK-клиент теста (факт содержимого S3).
    private async Task<byte[]> DownloadAsync(string key, CancellationToken ct)
    {
        // t36: креды/endpoint — литералы окружения Fact'а (S3-полей в runtime нет).
        using var client = new Amazon.S3.AmazonS3Client(
            new Amazon.Runtime.BasicAWSCredentials(E2eEnvironment.S3RootUser, E2eEnvironment.S3RootPassword),
            new Amazon.S3.AmazonS3Config { ServiceURL = MinioS3().Endpoint, ForcePathStyle = true });
        var response = await client.GetObjectAsync(new Amazon.S3.Model.GetObjectRequest
        {
            BucketName = SnapshotBucket,
            Key = key,
        }, ct);
        using var ms = new MemoryStream();
        await response.ResponseStream.CopyToAsync(ms, ct);
        return ms.ToArray();
    }

    private async Task<string> StatusValueAsync(CancellationToken ct)
        => (await G.GetAsync(Endpoint, "/pgworker/etcd-snapshots", ct)).Value!.Value;

    // AAA: включённый экспорт — слепок лидера уезжает парой db+meta в MinIO,
    // ключ OK с фактом, sha256 совпадает, слепок разворачиваем (etcdctl).
    [Fact]
    public async Task Export_плановый_слепок_уезжает_в_S3_ключ_OK()
    {
        // Arrange — своё окружение (сеть/etcd/ownS3) + хост-воркер с экспортом
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await E2eEnvironment.StartAsync("snap-exp", withS3Node: true, ct: ct);
        Fx = fx;
        await using var app = await StartExportHostAsync("snapexp", ct, intervalMin: 1);

        // Act — ждём первой успешной выгрузки (первый тик лидера снимает сразу)
        var okKey = await E2eFixture.WaitForAsync(async () =>
            (await G.GetAsync(Endpoint, "/pgworker/etcd-snapshots", ct)).Value?.Value.Contains("\"state\":\"OK\"") == true,
            TimeSpan.FromSeconds(120), ct);

        // Assert — ключ OK с интервалом и фактом
        okKey.Should().BeTrue("первый тик лидера снимает слепок и выгружает его");
        var kv = await StatusValueAsync(ct);
        kv.Should().Contain("\"interval_min\":1").And.Contain("\"last_object\":\"etcd/snapshot-");
        // Assert — объекты в MinIO: пара db+meta (имена относительно etcd/).
        // Ретрай-поллинг до УСТОЙЧИВОГО листинга (t24, фейлы №14/№16/№17):
        // intervalMin=1 — в окне ожидания OK воркер успевает снять ЕЩЁ один
        // слепок, и счёт пар в листинге — зона РЕТЕНЦИЯ-факта (Export_ретенция
        // ниже), не этого. Устойчивость здесь = в листинге присутствует пара
        // ПОСЛЕДНЕГО выгруженного объекта (db + его meta), за вычетом гонки
        // «снимок снят, но пара ещё не долетела».
        var stablePair = await E2eFixture.WaitForAsync(async () =>
        {
            var keysNow = McKeys(await McLsAsync("etcd/"));
            var last = PgWorker.Backups.EtcdExport.EtcdSnapshotStatusJson.Parse(
                await StatusValueAsync(ct))!;
            var dbKey = last.LastObject!["etcd/".Length..];
            var metaKey = dbKey.EndsWith(".db")
                ? dbKey[..^".db".Length] + ".meta.json"
                : dbKey;
            return keysNow.Contains(dbKey) && keysNow.Contains(metaKey);
        }, TimeSpan.FromSeconds(120), ct);
        stablePair.Should().BeTrue("пара последнего выгруженного слепка (db+meta) устойчиво в листинге");
        var listing = await McLsAsync("etcd/");
        var keys = McKeys(listing);
        // Assert — sha256 выгрузки == sha256 содержимого S3-объекта (целостность)
        var status = PgWorker.Backups.EtcdExport.EtcdSnapshotStatusJson.Parse(kv)!;
        // db-ключ из статуса (не Single по листингу: в окне факта воркер мог
        // снять следующий слепок — счёт пар в листинге не инвариант этого факта)
        var lastDb = status.LastObject!["etcd/".Length..];
        keys.Should().Contain(lastDb, "последний выгруженный db-объект в листинге");
        var bytes = await DownloadAsync(status.LastObject!, ct);
        Sha256Hex(bytes).Should().Be(status.LastSha256, "sha256 статуса = sha256 содержимого S3-объекта");
        // Assert — verify слепка (AC5): etcdctl snapshot status на ВАЛИДНОМ слепке
        // в контейнере etcd окружения — положительный вердикт структуры/ревизии
        // (etcd 3.5.x печатает hash, но при status его НЕ сверяет — порча байта
        // в E2E не проверяется, покрыта интеграциями T5 Fact 6)
        var tempFile = Path.Combine(Path.GetTempPath(), $"pgw-e2e-snap-{Guid.NewGuid():N}.db");
        await File.WriteAllBytesAsync(tempFile, bytes, ct);
        try
        {
            await Fx.RunDockerAsync(["cp", tempFile, $"{Fx.EtcdContainerName}:/tmp/snap.db"], ct);
            var verdict = await Fx.RunDockerAsync(
                ["exec", Fx.EtcdContainerName, "etcdctl", "snapshot", "status", "/tmp/snap.db"], ct);
            // etcdctl печатает hash БЕЗ ведущих нулей (напр. «9367b56» — 7 hex) —
            // {8} ловил только полный паддинг; диапазон 1..8 по механике вывода.
            verdict.Should().MatchRegex("^[0-9a-f]{1,8}, \\d+, \\d+, .+",
                "валидный слепок из S3 проходит etcdctl snapshot status (AC5): hash/keys/size");
        }
        finally
        {
            File.Delete(tempFile);
        }
    }

    // AAA (AC4 сквозно): ретенция держит ровно N пар — старейшая снесена.
    [Fact]
    public async Task Export_ретенция_держит_N_пар()
    {
        // Arrange — хост с retention: 1 и минутным интервалом
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await E2eEnvironment.StartAsync("snap-ret", withS3Node: true, ct: ct);
        Fx = fx;
        await using var app = await StartExportHostAsync("snapret", ct, intervalMin: 1, retention: 1);

        // Act — ждём первый OK, затем OK со СВЕЖИМ last_object (каждый плановый
        // слепок уезжает новой парой; интервал 1 мин; бюджет ≤ 180 c)
        var first = await E2eFixture.WaitForAsync(async () =>
            (await G.GetAsync(Endpoint, "/pgworker/etcd-snapshots", ct)).Value?.Value.Contains("\"state\":\"OK\"") == true,
            TimeSpan.FromSeconds(300), ct);
        first.Should().BeTrue("первая выгрузка");
        var firstName = PgWorker.Backups.EtcdExport.EtcdSnapshotStatusJson.Parse(
            await StatusValueAsync(ct))!.LastObject;
        var second = await E2eFixture.WaitForAsync(async () =>
        {
            var raw = (await G.GetAsync(Endpoint, "/pgworker/etcd-snapshots", ct)).Value?.Value;
            var obj = raw is null ? null : PgWorker.Backups.EtcdExport.EtcdSnapshotStatusJson.Parse(raw)!.LastObject;
            return obj is not null && obj != firstName;
        }, TimeSpan.FromSeconds(180), ct);

        // Assert — ровно 2 ключа (одна пара), старейшего id нет
        second.Should().BeTrue("каждый плановый слепок уезжает новой парой — ретенция 1 сносит старейшую");
        var keys = McKeys(await McLsAsync("etcd/"));
        keys.Should().HaveCount(2, "ретенция держит ровно N=1 пар (db+meta)");
        keys.Should().Contain(k => k.EndsWith(".db")).And.Contain(k => k.EndsWith(".meta.json"));
        keys.Should().NotContain(k => $"etcd/{k}" == firstName, "старейший id снесён (AC4)");
    }

    // AAA (AC3+AC7 сквозно): S3 недоступен — слепок снят, ключ FAILED с error,
    // цикл жив, панель зажигает critical-алерт на фактическом значении ключа.
    [Fact]
    public async Task Export_негатив_S3_недоступен_слепок_снят_ключ_FAILED_алерт()
    {
        // Arrange — хост с экспортом на закрытый порт (протокольный «всегда закрыт»)
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await E2eEnvironment.StartAsync("snap-neg", withS3Node: true, ct: ct);
        Fx = fx;
        await using var app = await StartExportHostAsync(
            "snapneg", ct, s3Override: "http://e2e-broken-s3:1");

        // Act — ждём ключ FAILED (транзиент S3 не роняет снятие; статус пишет sink)
        var failed = await E2eFixture.WaitForAsync(async () =>
            (await G.GetAsync(Endpoint, "/pgworker/etcd-snapshots", ct)).Value?.Value.Contains("\"state\":\"FAILED\"") == true,
            TimeSpan.FromSeconds(120), ct);

        // Assert — ключ FAILED + непустая error
        failed.Should().BeTrue("сбой выгрузки фиксируется статус-ключом, не снятием");
        var kv = await StatusValueAsync(ct);
        var status = PgWorker.Backups.EtcdExport.EtcdSnapshotStatusJson.Parse(kv)!;
        status.State.Should().Be("FAILED");
        status.Error.Should().NotBeNullOrEmpty();
        // Assert — слепок снят в локальный том (истина снятия), цикл жив
        Directory.GetFiles(app.SnapshotsDir, "snapshot-*.db").Should().NotBeEmpty(
            "сбой выгрузки не роняет снятие (spec §2.3)");
        var api = (await G.RangeAsync(Endpoint, "/pgworker/api/", ct)).Value;
        api.Should().NotBeEmpty("цикл жив — дискавери-ключ инстанса существует");
        // Assert — панельный алерт: фактическое значение ключа кормит правило
        var parsed = AdminPanel.Etcd.Parsing.EtcdSnapshotsParser.Parse(
            new Shared.Etcd.Client.Kv("/pgworker/etcd-snapshots", kv, 1));
        var snapshot = new AdminPanel.Core.EtcdSnapshot(
            DateTimeOffset.UtcNow,
            new AdminPanel.Core.EtcdStatus(true, [], [], [], null, false, DateTimeOffset.UtcNow, 0),
            [], [], [], [], [], [], [], [], [], [], [], 0,
            EtcdSnapshots: parsed.Info);
        var alerts = new AdminPanel.Core.Alerting.Rules.EtcdSnapshotExportFailedRule()
            .Evaluate(snapshot, new AdminPanel.Core.Alerting.AlertContext(null, DateTimeOffset.UtcNow, 3)).ToList();
        alerts.Should().ContainSingle(a => a.Kind == "etcd-snapshot-export-failed"
            && a.Severity == AdminPanel.Core.AlertSeverity.Critical,
            "провал выгрузки — critical-алерт (AC7)");
    }
}
