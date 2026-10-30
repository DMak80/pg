using System.Security.Cryptography;
using PgWorker.Backups;
using Shared.Etcd.Client;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// E2E экспорта etcd-снапшотов : изолированное окружение E2eEnvironment
// (своя сеть/etcd/ownS3 — withS3Node:true) + хост-воркер с включённым
// Snapshots:Export. Каждый Fact — своё окружение (per-method изоляция,
// e2e-isolation §1/§3): метрики/ключи/объекты сценария не пересекаются.
public sealed class E2eEtcdSnapshotExportScenarios(E2eEtcdSnapshotExportScenariosFx Ctx) : IClassFixture<E2eEtcdSnapshotExportScenariosFx>
{
    private E2eEnvironment Fx => Ctx.Fx;

    private const string Bucket = "pgworker-backups";

    // bucket слепочной пары (не пуловый pgworker-backups).
    private const string SnapshotBucket = "pgworker-etcd-snapshots";
    private string Endpoint => Fx.EtcdEndpoint;
    private EtcdGateway G => Fx.Gateway;

    // Хост-воркер с включённым экспортом (: ОТДЕЛЬНАЯ слепочная пара
    // PgWorker:Snapshots:Export:S3 — свой bucket pgworker-etcd-snapshots,
    // префикс etcd/; Endpoint — localhost-вид воркера: sink ходит из процесса).
    private async Task<HostInstance> StartExportHostAsync(string name, CancellationToken ct,
        int intervalMin = 1, int retrySec = 5, int retention = 28, string? s3Override = null)
    {
        // bucket слепков : отдельный от пулового — создаёт окружение сценария
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
            ["PgWorker__Snapshots__Export__S3__Endpoints"] = s3Override ?? Fx.S3EndpointForContainers, // env воркера — alias(ы)
            ["PgWorker__Snapshots__Export__S3__Bucket"] = SnapshotBucket,
            // Креды ownS3-ноды контура (дефолт «minioadmin» ownS3 отвергает
            // ownS3 — выгрузка слепков молча не ехала, полный прогон 2026-10-16:
            // snap-* 3 факта «первая выгрузка False»).
            ["PgWorker__Snapshots__Export__S3__AccessKey"] = E2eEnvironment.S3RootUser,
            ["PgWorker__Snapshots__Export__S3__SecretKey"] = E2eEnvironment.S3RootPassword,
        }, ct: ct);
    }

    // S3-цель окружения Fact'а (хост-клиент; — S3ClientOptions).
    private PgWorker.Backups.S3ClientOptions OwnS3() => new(
        Fx.S3Endpoint,
        SnapshotBucket, E2eEnvironment.S3RootUser, E2eEnvironment.S3RootPassword);

    // Runtime-опции подсистемы на окружение Fact'а (bucket per-install).
    private BackupsRuntimeOptions OwnS3RuntimeForHost() => new(
        Enabled: true,
        S3Bucket: Bucket,
        StagingDir: "/home/postgres/pgdata/backup-staging",
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
        // креды/endpoint — литералы окружения Fact'а (S3-полей в runtime нет).
        using var client = new Amazon.S3.AmazonS3Client(
            new Amazon.Runtime.BasicAWSCredentials(E2eEnvironment.S3RootUser, E2eEnvironment.S3RootPassword),
            new Amazon.S3.AmazonS3Config { ServiceURL = OwnS3().Endpoint, ForcePathStyle = true });
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

    // AAA: включённый экспорт — слепок лидера уезжает парой db+meta в ownS3,
    // ключ OK с фактом, sha256 совпадает, слепок разворачиваем (etcdctl).
    [Fact]
    public async Task ExportPlannedSnapshotLeavesToS3KeyOk()
    {
        // Arrange — своё окружение (сеть/etcd/ownS3) + хост-воркер с экспортом
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromMinutes(3)); // потолок теста 3 мин: неверное ожидание не может длиться дольше (приказ владельца)
        var ct = cts.Token;
        await using var app = await StartExportHostAsync("snapexp", ct, intervalMin: 1);

        // Act — ждём первой успешной выгрузки (первый тик лидера снимает сразу)
        var okKey = await E2eFixture.WaitForAsync(async () =>
            (await G.GetAsync(Endpoint, "/pgworker/etcd-snapshots", ct)).Value?.Value.Contains("\"state\":\"OK\"") == true,
            TimeSpan.FromSeconds(90), ct);

        // Assert — ключ OK с интервалом и фактом
        okKey.Should().BeTrue("the first leader tick takes a snapshot and uploads it");
        var kv = await StatusValueAsync(ct);
        kv.Should().Contain("\"interval_min\":1").And.Contain("\"last_object\":\"etcd/snapshot-");
        // Assert — объекты в ownS3: пара db+meta (имена относительно etcd/).
        // Ретрай-поллинг до УСТОЙЧИВОГО листинга (под нагрузкой серии листинг
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
        }, TimeSpan.FromSeconds(90), ct);
        stablePair.Should().BeTrue("the pair of the last uploaded snapshot (db+meta) is stably in the listing");
        var listing = await McLsAsync("etcd/");
        var keys = McKeys(listing);
        // Assert — sha256 выгрузки == sha256 содержимого S3-объекта (целостность)
        var status = PgWorker.Backups.EtcdExport.EtcdSnapshotStatusJson.Parse(kv)!;
        // db-ключ из статуса (не Single по листингу: в окне факта воркер мог
        // снять следующий слепок — счёт пар в листинге не инвариант этого факта)
        var lastDb = status.LastObject!["etcd/".Length..];
        keys.Should().Contain(lastDb, "the last uploaded db object is in the listing");
        var bytes = await DownloadAsync(status.LastObject!, ct);
        Sha256Hex(bytes).Should().Be(status.LastSha256, "the status sha256 = the sha256 of the S3 object content");
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
                "a valid snapshot from S3 passes etcdctl snapshot status (AC5): hash/keys/size");
        }
        finally
        {
            File.Delete(tempFile);
        }
    }

    // AAA (AC4 сквозно): ретенция держит ровно N пар — старейшая снесена.
    [Fact]
    public async Task ExportRetentionKeepsNPairs()
    {
        // Arrange — хост с retention: 1 и минутным интервалом
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromMinutes(3)); // потолок теста 3 мин: неверное ожидание не может длиться дольше (приказ владельца)
        var ct = cts.Token;
        await using var app = await StartExportHostAsync("snapret", ct, intervalMin: 1, retention: 1);

        // Act — ждём первый OK, затем OK со СВЕЖИМ last_object (каждый плановый
        // слепок уезжает новой парой; интервал 1 мин; бюджет ≤ 180 c)
        var first = await E2eFixture.WaitForAsync(async () =>
            (await G.GetAsync(Endpoint, "/pgworker/etcd-snapshots", ct)).Value?.Value.Contains("\"state\":\"OK\"") == true,
            TimeSpan.FromSeconds(90), ct);
        first.Should().BeTrue("the first upload");
        var firstName = PgWorker.Backups.EtcdExport.EtcdSnapshotStatusJson.Parse(
            await StatusValueAsync(ct))!.LastObject;
        var second = await E2eFixture.WaitForAsync(async () =>
        {
            var raw = (await G.GetAsync(Endpoint, "/pgworker/etcd-snapshots", ct)).Value?.Value;
            var obj = raw is null ? null : PgWorker.Backups.EtcdExport.EtcdSnapshotStatusJson.Parse(raw)!.LastObject;
            return obj is not null && obj != firstName;
        }, TimeSpan.FromSeconds(90), ct);

        // Assert — ровно 2 ключа (одна пара), старейшего id нет
        second.Should().BeTrue("every planned snapshot leaves as a new pair — retention 1 removes the oldest");
        var keys = McKeys(await McLsAsync("etcd/"));
        keys.Should().HaveCount(2, "the retention keeps exactly N=1 pairs (db+meta)");
        keys.Should().Contain(k => k.EndsWith(".db")).And.Contain(k => k.EndsWith(".meta.json"));
        keys.Should().NotContain(k => $"etcd/{k}" == firstName, "the oldest id is removed (AC4)");
    }

    // AAA (AC3+AC7 сквозно): S3 недоступен — слепок снят, ключ FAILED с error,
    // цикл жив, панель зажигает critical-алерт на фактическом значении ключа.
    [Fact]
    public async Task ExportNegativeS3UnavailableSnapshotTakenKeyFailedAlert()
    {
        // Arrange — хост с экспортом на закрытый порт (протокольный «всегда закрыт»)
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromMinutes(3)); // потолок теста 3 мин: неверное ожидание не может длиться дольше (приказ владельца)
        var ct = cts.Token;
        await using var app = await StartExportHostAsync(
            "snapneg", ct, s3Override: "http://e2e-broken-s3:1");

        // Act — ждём ключ FAILED (транзиент S3 не роняет снятие; статус пишет sink)
        var failed = await E2eFixture.WaitForAsync(async () =>
            (await G.GetAsync(Endpoint, "/pgworker/etcd-snapshots", ct)).Value?.Value.Contains("\"state\":\"FAILED\"") == true,
            TimeSpan.FromSeconds(90), ct);

        // Assert — ключ FAILED + непустая error
        failed.Should().BeTrue("an upload failure is recorded by the status key, not by the snapshot");
        var kv = await StatusValueAsync(ct);
        var status = PgWorker.Backups.EtcdExport.EtcdSnapshotStatusJson.Parse(kv)!;
        status.State.Should().Be("FAILED");
        status.Error.Should().NotBeNullOrEmpty();
        // Assert — слепок снят в локальный том (истина снятия), цикл жив
        Directory.GetFiles(app.SnapshotsDir, "snapshot-*.db").Should().NotBeEmpty(
            "an upload failure does not fail the snapshot (spec §2.3)");
        var api = (await G.RangeAsync(Endpoint, "/pgworker/api/", ct)).Value;
        api.Should().NotBeEmpty("the loop is alive — the instance discovery key exists");
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
            "an upload failure — a critical alert (AC7)");
    }
}

/// <summary>Компаньон-фикстура окружения класса (полный контур на класс:
/// сеть/etcd/CA/окно портов; docs/e2e-isolation.md §1).</summary>
public sealed class E2eEtcdSnapshotExportScenariosFx : E2eContourFixture
{
    protected override string Slug => "snap-exp";
    protected override bool WithS3Node => true;
}

// E2E отказоустойчивости слепочной пары (t33): Endpoints — запятая-список из
// ДВУХ ownS3-нод контура; docker stop ПЕРВОЙ ноды → следующий плановый слепок
// (интервал 1 мин) с доводкой (RetryIntervalSec=5) уезжает на ВТОРУЮ ноду:
// статус-ключ снова OK с новым last_object, свежая пара реально на второй
// ноде (mc-листинг). Изоляция — своё окружение класса (per-class контур,
// e2e-isolation §1/§3); teardown при любом исходе — фикстура.
public sealed class E2eEtcdSnapshotFailoverScenarios(E2eEtcdSnapshotFailoverScenariosFx Ctx)
    : IClassFixture<E2eEtcdSnapshotFailoverScenariosFx>
{
    private E2eEnvironment Fx => Ctx.Fx;

    // bucket слепочной пары (не пуловый pgworker-backups).
    private const string SnapshotBucket = "pgworker-etcd-snapshots";

    // mc-скрипт: alias t → нода, bucket слепков. Retry-цикл — extra-нода
    // стартует асинхронно, бакет ждёт готовности S3 (прецедент
    // E2eEnvironment.EnsureOwnS3BucketAsync).
    private static string McMakeBucketScript(string alias)
        => $"for i in $(seq 1 30); do mc alias set t http://{alias}:9000 {E2eEnvironment.S3RootUser}"
           + $" {E2eEnvironment.S3RootPassword} >/dev/null 2>&1"
           + $" && mc mb --ignore-existing t/{SnapshotBucket} 2>/dev/null && exit 0; sleep 2; done; exit 1";

    // mc ls --recursive префикса etcd/ на ноде-алиасе (mc в инструментальном
    // pgw-em контейнера контура — прецедент RunMcAsync).
    private Task<string> McLsOnNodeAsync(string alias, CancellationToken ct)
        => Fx.RunMcAsync(
            $"mc alias set t http://{alias}:9000 {E2eEnvironment.S3RootUser} {E2eEnvironment.S3RootPassword} >/dev/null"
            + $" && mc ls --recursive t/{SnapshotBucket}/etcd/", ct);

    private async Task<string> StatusValueAsync(CancellationToken ct)
        => (await Fx.Gateway.GetAsync(Fx.EtcdEndpoint, "/pgworker/etcd-snapshots", ct)).Value!.Value;

    // Имена ключей из mc-листинга (последний токен строки — путь).
    private static IReadOnlyList<string> McKeys(string listing)
        => listing.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(l => l[(l.LastIndexOf(' ') + 1)..])
            .ToList();

    // AAA (t33): отказ первой цели пары не останавливает выкладку слепков —
    // перебор целей (PoolBackupS3, arch/19 §5) уводит следующий слепок на
    // вторую ноду: статус OK с новым last_object, пара db+meta на ней.
    [Fact]
    public async Task FirstTargetDown_NextSnapshotOkOnSecondNode()
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromMinutes(3)); // потолок теста 3 мин: неверное ожидание не может длиться дольше (приказ владельца)
        var ct = cts.Token;
        try
        {
            // Arrange — вторая ownS3-нода контура + bucket слепков на ОБОИХ нодах
            // (два ОТДЕЛЬНЫХ exec: каждый скрипт заканчивается exit — склейка «&&»
            // обрывала бы второй скрипт первым exit'ом; не-нулевой exit mc
            // бросает исключение — тест падает сразу, не молча)
            await E2eFixture.TimedAsync("snap-fail extra-s3-node",
                () => Fx.StartExtraS3NodeAsync("e2e-owns3-2", ct));
            await E2eFixture.TimedAsync("snap-fail snapshot-bucket-node1",
                () => Fx.RunMcAsync(McMakeBucketScript("e2e-owns3"), ct));
            await E2eFixture.TimedAsync("snap-fail snapshot-bucket-node2",
                () => Fx.RunMcAsync(McMakeBucketScript("e2e-owns3-2"), ct));

            // воркер: Endpoints — обе ноды (alias'ы сети контура), интервал 1 мин
            await using var app = await Fx.StartHostAsync("snapfail", snapshotIntervalMin: 1,
                extraEnv: new Dictionary<string, string>
                {
                    ["PgWorker__Snapshots__Export__Enabled"] = "true",
                    ["PgWorker__Snapshots__Export__RetryIntervalSec"] = "5",
                    ["PgWorker__Snapshots__Export__S3__Endpoints"] =
                        "http://e2e-owns3:9000,http://e2e-owns3-2:9000",
                    ["PgWorker__Snapshots__Export__S3__Bucket"] = SnapshotBucket,
                    ["PgWorker__Snapshots__Export__S3__AccessKey"] = E2eEnvironment.S3RootUser,
                    ["PgWorker__Snapshots__Export__S3__SecretKey"] = E2eEnvironment.S3RootPassword,
                }, ct: ct);

            // Wait — первая выгрузка OK (пара уходит на ПЕРВУЮ ноду)
            var firstOk = await E2eFixture.TimedAsync("snap-fail wait-first-ok",
                () => E2eFixture.WaitForAsync(async () =>
                    (await Fx.Gateway.GetAsync(Fx.EtcdEndpoint, "/pgworker/etcd-snapshots", ct))
                        .Value?.Value.Contains("\"state\":\"OK\"") == true,
                    TimeSpan.FromSeconds(90), ct));
            firstOk.Should().BeTrue("the first snapshot uploads to the FIRST node");
            var firstObject = PgWorker.Backups.EtcdExport.EtcdSnapshotStatusJson.Parse(
                await StatusValueAsync(ct))!.LastObject;

            // Act — стоп ПЕРВОЙ ноды (контейнер pgw-eo-{runId}, alias e2e-owns3)
            await E2eFixture.TimedAsync("snap-fail stop-first-node",
                () => Fx.RunDockerAsync(["stop", Fx.S3NodeContainerName], ct));

            // Wait — следующий слепок/доводка уехали на ВТОРУЮ ноду: снова OK,
            // last_object свежий (промежуточный FAILED — не помеха поллингу)
            var nextOk = await E2eFixture.TimedAsync("snap-fail wait-next-ok",
                () => E2eFixture.WaitForAsync(async () =>
                {
                    var raw = (await Fx.Gateway.GetAsync(Fx.EtcdEndpoint, "/pgworker/etcd-snapshots", ct))
                        .Value?.Value;
                    if (raw is null)
                        return false;
                    var status = PgWorker.Backups.EtcdExport.EtcdSnapshotStatusJson.Parse(raw)!;
                    return status.State == "OK" && status.LastObject != firstObject;
                }, TimeSpan.FromSeconds(90), ct));
            nextOk.Should().BeTrue(
                "the next snapshot (or CatchUp with RetryIntervalSec=5) lands on the SECOND node");

            // Assert — свежая пара реально на ВТОРОЙ ноде (mc-листинг по alias)
            var status2 = PgWorker.Backups.EtcdExport.EtcdSnapshotStatusJson.Parse(
                await StatusValueAsync(ct))!;
            var dbKey = status2.LastObject!["etcd/".Length..];
            var metaKey = dbKey[..^".db".Length] + ".meta.json";
            var keys = McKeys(await E2eFixture.TimedAsync("snap-fail mc-ls-node2",
                () => McLsOnNodeAsync("e2e-owns3-2", ct)));
            keys.Should().Contain(dbKey, "the fresh .db is on the SECOND node");
            keys.Should().Contain(metaKey, "the fresh .meta.json is on the SECOND node");
        }
        catch
        {
            Fx.MarkFailed(); // teardown остановит контейнеры, но не удалит — разбор по логам
            throw;
        }
    }
}

/// <summary>Компаньон-фикстура окружения класса (полный контур на класс:
/// сеть/etcd/ownS3/CA/окно портов; docs/e2e-isolation.md §1).</summary>
public sealed class E2eEtcdSnapshotFailoverScenariosFx : E2eContourFixture
{
    protected override string Slug => "snap-fail";
    protected override bool WithS3Node => true;
}
