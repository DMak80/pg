using System.Security.Cryptography;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using FluentAssertions;
using PgWorker.App;
using PgWorker.Backups;
using PgWorker.Backups.EtcdExport;
using PgWorker.IntegrationTests.E2e;
using Shared.Etcd.Maintenance;
using Xunit;

namespace PgWorker.IntegrationTests.Backups;

// S3-sink снапшотов etcd : OwnEtcd + OwnS3 на Fact (своё окружение,
// динамические порты, teardown при любом исходе — e2e-isolation §1/§3).
// Сценарии: экспорт после TakeAsync, каждый слепок новой парой, ретенция,
// транзиент → FAILED → доводка, верификация разворачиваемости (etcdctl
// snapshot status), takeover.
[Collection(NonE2eCollection.Name)]
public class EtcdSnapshotSinkTests
{
    // Прямой клиент-помощник для сида/чтения объектов (AWSSDK, не тестируемый код).
    private static AmazonS3Client SeedClient(OwnS3 s3Node) => new(
        new BasicAWSCredentials(OwnS3.AccessKey, OwnS3.SecretKey),
        new AmazonS3Config { ServiceURL = s3Node.HostEndpoint, ForcePathStyle = true });

    private static S3ClientOptions BadS3Runtime(OwnS3 s3Node)
        => s3Node.S3() with { Endpoint = $"http://localhost:{E2eFixture.FreePort()}" }; // закрытый порт — транзиент

    private static async Task<EtcdSnapshotSink> SinkAsync(OwnEtcd etcd, OwnS3 s3Node, string instance,
        int retention = 28, CancellationToken ct = default)
    {
        var s3 = new BackupS3(s3Node.S3());
        return new EtcdSnapshotSink(s3, etcd.Gateway, [etcd.Endpoint], retention, 5, instance, 360);
    }

    // Пул целей слепочной пары через ПРОД-фабрику (EtcdSnapshotSinkFactory):
    // правда сборки Program.cs — перебор целей пары (arch/19 §5).
    private static EtcdSnapshotSink FactorySink(OwnEtcd etcd, string endpoints, string instance,
        int retention = 28)
    {
        var opts = new PgWorkerOptions();
        opts.Snapshots.Export.S3 = new SnapshotExportS3Options
        {
            Endpoints = endpoints,
            Bucket = OwnS3.Bucket,
            AccessKey = OwnS3.AccessKey,
            SecretKey = OwnS3.SecretKey,
        };
        return new EtcdSnapshotSink(
            EtcdSnapshotSinkFactory.Create(opts), etcd.Gateway, [etcd.Endpoint], retention, 5, instance, 360);
    }

    // Чтение объекта bucket'a напрямую (проверка факта «реально лежит в S3»).
    private static async Task<byte[]> GetObjectBytesAsync(OwnS3 s3Node, string key, CancellationToken ct)
    {
        using var client = SeedClient(s3Node);
        var response = await client.GetObjectAsync(new GetObjectRequest { BucketName = OwnS3.Bucket, Key = key }, ct);
        using var ms = new MemoryStream();
        await response.ResponseStream.CopyToAsync(ms, ct);
        return ms.ToArray();
    }

    private static string Sha256Hex(byte[] data)
        => Convert.ToHexString(SHA256.HashData(data)).ToLowerInvariant();

    // AAA: снятие вызывает sink — в S3 пара db+meta по канону нейминга, статус
    // OK с фактом, sha256 совпадает; объекты etcd/* видны list'ом всего bucket.
    [Fact]
    public async Task ExportAfterTakeAsyncObjectsMetaStatusOk()
    {
        // Arrange — своё etcd + ownS3-нода; sink; SnapshotJob с sink'ом
        var ct = TestContext.Current.CancellationToken;
        await using var etcdFx = await OwnEtcd.StartAsync("sink1", ct);
        await using var s3NodeFx = await OwnS3.StartAsync("sink1", ct);
        var dir = Directory.CreateTempSubdirectory("sink-export-").FullName;
        var sink = await SinkAsync(etcdFx, s3NodeFx, "inst-A", ct: ct);
        var job = new SnapshotJob(etcdFx.Gateway, [etcdFx.Endpoint], dir, 10, 60, sink);

        // Act
        var shot = await job.TakeAsync(ct);

        // Assert — снятие успешно; в S3 ровно пара db+meta; статус OK с фактом
        shot.IsSuccess.Should().BeTrue();
        var s3 = new BackupS3(s3NodeFx.S3());
        var listed = (await s3.ListPrefixAsync("etcd/", ct: ct)).Value!;
        listed.Should().HaveCount(2).And.Contain(k => k.Key.EndsWith(".db")).And.Contain(k => k.Key.EndsWith(".meta.json"));
        // Assert — канон нейминга §3.2 (фиксация от регрессии): мета-ключ
        // etcd/snapshot-<id>.meta.json — БЕЗ «.db»; ключ «.db.meta.json» ретенция
        // (EtcdExportRetention) сочла бы сиротой-метой и снесла бы первым проходом.
        listed.Should().Contain(k => k.Key.EndsWith(".meta.json"))
            .And.NotContain(k => k.Key.EndsWith(".db.meta.json"));
        var kv = (await etcdFx.Gateway.GetAsync(etcdFx.Endpoint, EtcdSnapshotStatusJson.Key, ct)).Value!;
        var status = EtcdSnapshotStatusJson.Parse(kv.Value)!;
        status.State.Should().Be("OK");
        status.IntervalMin.Should().Be(360);
        status.LastSha256.Should().Be(Sha256Hex(await File.ReadAllBytesAsync(shot.Value, ct)));
        // Assert (AC8-хвост): объекты etcd/* видны list'ом ВСЕГО bucket (префикс "")
        // — база used_bytes ключа storage (считает list ""; ср. BackupS3Tests:189)
        var wholeBucket = (await s3.ListPrefixAsync("", ct: ct)).Value!;
        wholeBucket.Select(o => o.Key).Should().Contain(listed.Select(o => o.Key),
            "the etcd/ objects are counted in used_bytes — a whole-bucket list sees them");
        // meta: taken <= uploaded, instance, ревизия best-effort есть (OwnEtcd жив)
        var metaKey = listed.Single(k => k.Key.EndsWith(".meta.json")).Key;
        var meta = EtcdSnapshotMetaJson.Parse(System.Text.Encoding.UTF8.GetString(
            await GetObjectBytesAsync(s3NodeFx, metaKey, ct)))!;
        meta.TakenUnix.Should().BeLessThanOrEqualTo(meta.UploadedUnix);
        meta.Instance.Should().Be("inst-A");
        meta.Revision.Should().NotBeNull();
    }

    // AAA (AC2): каждый слепок — НОВАЯ пара db+meta (дедупа нет); покрытие
    // продвигается вторым слепком; IsBehind=false — сон лидера остаётся плановым.
    [Fact]
    public async Task EachSnapshotIsUploadedByNewPair()
    {
        // Arrange — своё окружение; sink; SnapshotJob с sink'ом
        var ct = TestContext.Current.CancellationToken;
        await using var etcdFx = await OwnEtcd.StartAsync("sink2", ct);
        await using var s3NodeFx = await OwnS3.StartAsync("sink2", ct);
        var dir = Directory.CreateTempSubdirectory("sink-pairs-").FullName;
        var sink = await SinkAsync(etcdFx, s3NodeFx, "inst-A", ct: ct);
        var job = new SnapshotJob(etcdFx.Gateway, [etcdFx.Endpoint], dir, 10, 60, sink);

        // Act — два снятия с паузой 1.1 c (имена расходятся по секундам)
        var shot1 = await job.TakeAsync(ct);
        await Task.Delay(1100, ct);
        var shot2 = await job.TakeAsync(ct);

        // Assert — (а) в S3 4 ключа: ДВЕ пары; локальных файлов 2
        shot1.IsSuccess.Should().BeTrue();
        shot2.IsSuccess.Should().BeTrue();
        var s3 = new BackupS3(s3NodeFx.S3());
        var listed = (await s3.ListPrefixAsync("etcd/", ct: ct)).Value!;
        listed.Should().HaveCount(4, "the second snapshot leaves as a new pair — no dedup (AC2)");
        Directory.GetFiles(dir, "snapshot-*.db").Should().HaveCount(2);
        // Assert — (б) статус OK, покрытие = метка ВТОРОГО слепка, факт — его
        var secondName = Path.GetFileName(shot2.Value);
        var firstName = Path.GetFileName(shot1.Value);
        var taken1 = EtcdSnapshotStatus.TakenUnixFromName(firstName)!.Value;
        var taken2 = EtcdSnapshotStatus.TakenUnixFromName(secondName)!.Value;
        taken2.Should().BeGreaterThan(taken1);
        var kv = (await etcdFx.Gateway.GetAsync(etcdFx.Endpoint, EtcdSnapshotStatusJson.Key, ct)).Value!;
        var status = EtcdSnapshotStatusJson.Parse(kv.Value)!;
        status.State.Should().Be("OK");
        status.LastUploadedUnix.Should().Be(taken2, "the coverage is advanced by the second snapshot (§3.1)");
        status.LastObject.Should().Be($"etcd/{secondName}");
        status.LastSha256.Should().Be(Sha256Hex(await File.ReadAllBytesAsync(shot2.Value, ct)));
        status.SizeBytes.Should().Be((await File.ReadAllBytesAsync(shot2.Value, ct)).LongLength);
        // Assert — (в) отставания нет: сон лидера остаётся SnapshotIntervalMin
        EtcdSnapshotStatus.IsBehind(status, taken2).Should().BeFalse(
            "every successful sink pass advances the coverage (§3.5 item 1 invariant)");
    }

    // AAA (AC4): ретенция — ровно N пар, старейшие снесены; сирота-meta — мусор.
    [Fact]
    public async Task RetentionOverLimitOldestPairsRemoved()
    {
        // Arrange — sink с ретенцией 2; три снятия с паузами
        var ct = TestContext.Current.CancellationToken;
        await using var etcdFx = await OwnEtcd.StartAsync("sink3", ct);
        await using var s3NodeFx = await OwnS3.StartAsync("sink3", ct);
        var dir = Directory.CreateTempSubdirectory("sink-retain-").FullName;
        var sink = await SinkAsync(etcdFx, s3NodeFx, "inst-A", retention: 2, ct: ct);
        var job = new SnapshotJob(etcdFx.Gateway, [etcdFx.Endpoint], dir, 10, 60, sink);
        await job.TakeAsync(ct);
        await Task.Delay(1100, ct);
        await job.TakeAsync(ct);
        await Task.Delay(1100, ct);
        await job.TakeAsync(ct);

        // Assert — ровно 2 пары (4 ключа); старейший id отсутствует
        var s3 = new BackupS3(s3NodeFx.S3());
        var listed = (await s3.ListPrefixAsync("etcd/", ct: ct)).Value!;
        listed.Should().HaveCount(4, "the retention keeps exactly N=2 pairs");
        var oldestId = listed.Select(k => k.Key).Where(k => k.EndsWith(".db"))
            .Select(k => k[(EtcdExportRetention.Prefix.Length)..][..^".db".Length])
            .OrderBy(id => id, StringComparer.Ordinal).First();
        // Arrange/Act — сид сироты-meta без .db → четвёртое снятие сносит её
        using var client = SeedClient(s3NodeFx);
        await client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = OwnS3.Bucket,
            Key = "etcd/snapshot-20260101-000000.meta.json",
            ContentBody = """{"sha256":"x"}""",
        }, ct);
        await Task.Delay(1100, ct);
        var shot4 = await job.TakeAsync(ct);

        // Assert — сирота снесена ретенцией; пар по-прежнему 2
        shot4.IsSuccess.Should().BeTrue();
        var after = (await s3.ListPrefixAsync("etcd/", ct: ct)).Value!;
        after.Should().NotContain(k => k.Key == "etcd/snapshot-20260101-000000.meta.json",
            "a lone meta without .db — retention garbage (AC4)");
        after.Should().HaveCount(4);
        after.Should().NotContain(k => k.Key.EndsWith($"{oldestId}.db"));
    }

    // AAA (AC3): S3-транзиент не роняет снятие; статус FAILED + непустая ошибка.
    [Fact]
    public async Task TransientS3DoesNotFailSnapshotStatusFailed()
    {
        // Arrange — sink на закрытый порт (S3 недоступен); SnapshotJob с ним
        var ct = TestContext.Current.CancellationToken;
        await using var etcdFx = await OwnEtcd.StartAsync("sink4", ct);
        await using var s3NodeFx = await OwnS3.StartAsync("sink4", ct);
        var dir = Directory.CreateTempSubdirectory("sink-transient-").FullName;
        var badS3 = new BackupS3(BadS3Runtime(s3NodeFx));
        var sink = new EtcdSnapshotSink(badS3, etcdFx.Gateway, [etcdFx.Endpoint], 28, 5, "inst-A", 360);
        var job = new SnapshotJob(etcdFx.Gateway, [etcdFx.Endpoint], dir, 10, 60, sink);

        // Act
        var shot = await job.TakeAsync(ct);

        // Assert — снятие успешно (локальный файл — истина снятия), статус FAILED
        shot.IsSuccess.Should().BeTrue("an upload failure is transient, the snapshot does not fail (spec §2.3)");
        File.Exists(shot.Value).Should().BeTrue();
        var kv = (await etcdFx.Gateway.GetAsync(etcdFx.Endpoint, EtcdSnapshotStatusJson.Key, ct)).Value!;
        var status = EtcdSnapshotStatusJson.Parse(kv.Value)!;
        status.State.Should().Be("FAILED");
        status.Error.Should().NotBeNullOrEmpty();
    }

    // AAA (AC3): доводка — новейший локальный слепок доезжает в S3, статус OK.
    [Fact]
    public async Task CatchUpFinalizationUploadsNewestLocal()
    {
        // Arrange — слепок снят при недоступном S3 (FAILED-статус), затем живой sink
        var ct = TestContext.Current.CancellationToken;
        await using var etcdFx = await OwnEtcd.StartAsync("sink5", ct);
        await using var s3NodeFx = await OwnS3.StartAsync("sink5", ct);
        var dir = Directory.CreateTempSubdirectory("sink-catchup-").FullName;
        var badS3 = new BackupS3(BadS3Runtime(s3NodeFx));
        var brokenSink = new EtcdSnapshotSink(badS3, etcdFx.Gateway, [etcdFx.Endpoint], 28, 5, "inst-A", 360);
        var brokenJob = new SnapshotJob(etcdFx.Gateway, [etcdFx.Endpoint], dir, 10, 60, brokenSink);
        (await brokenJob.TakeAsync(ct)).IsSuccess.Should().BeTrue();

        var liveSink = await SinkAsync(etcdFx, s3NodeFx, "inst-A", ct: ct);

        // Act — доводка отстающей выгрузки
        var catchUp = await liveSink.CatchUpAsync(dir, ct);

        // Assert — новейший локальный доехал: объекты есть, статус OK, sha совпадает
        catchUp.IsSuccess.Should().BeTrue();
        catchUp.Value.Should().BeTrue();
        var latest = Directory.GetFiles(dir, "snapshot-*.db").OrderByDescending(f => f, StringComparer.Ordinal).First();
        var s3 = new BackupS3(s3NodeFx.S3());
        var listed = (await s3.ListPrefixAsync("etcd/", ct: ct)).Value!;
        listed.Should().Contain(k => k.Key == $"etcd/{Path.GetFileName(latest)}");
        var kv = (await etcdFx.Gateway.GetAsync(etcdFx.Endpoint, EtcdSnapshotStatusJson.Key, ct)).Value!;
        var status = EtcdSnapshotStatusJson.Parse(kv.Value)!;
        status.State.Should().Be("OK");
        status.LastSha256.Should().Be(Sha256Hex(await File.ReadAllBytesAsync(latest, ct)));
    }

    // AAA (AC5; круг 7 — по обновлённой букве spec): (а) валидный слепок —
    // положительный вердикт etcdctl snapshot status (структура/размер/ревизия);
    // (б) порча байта — sha256 файла расходится с sha256 из .meta.json (ручная
    // сверка — операторский барьер runbook-шага 2) И провал etcdutl snapshot
    // restore (restore верифицирует sha256 при разворачивании — финальный
    // барьер).
    [Fact]
    public async Task IntegrityVerificationStatusAndRestore()
    {
        // Arrange — экспорт слепка в S3; скачивание .db и .meta.json
        var ct = TestContext.Current.CancellationToken;
        await using var etcdFx = await OwnEtcd.StartAsync("sink6", ct);
        await using var s3NodeFx = await OwnS3.StartAsync("sink6", ct);
        var dir = Directory.CreateTempSubdirectory("sink-verify-").FullName;
        var sink = await SinkAsync(etcdFx, s3NodeFx, "inst-A", ct: ct);
        var job = new SnapshotJob(etcdFx.Gateway, [etcdFx.Endpoint], dir, 10, 60, sink);
        var shot = await job.TakeAsync(ct);
        shot.IsSuccess.Should().BeTrue();
        var s3 = new BackupS3(s3NodeFx.S3());
        var listed = (await s3.ListPrefixAsync("etcd/", ct: ct)).Value!;
        var dbKey = listed.Single(k => k.Key.EndsWith(".db")).Key;
        var metaKey = listed.Single(k => k.Key.EndsWith(".meta.json")).Key;
        var data = await GetObjectBytesAsync(s3NodeFx, dbKey, ct);
        var meta = EtcdSnapshotMetaJson.Parse(System.Text.Encoding.UTF8.GetString(
            await GetObjectBytesAsync(s3NodeFx, metaKey, ct)))!;
        var tempFile = Path.Combine(Path.GetTempPath(), $"pgw-snap-{Guid.NewGuid():N}.db");
        await File.WriteAllBytesAsync(tempFile, data, ct);
        try
        {
            // Act (а) — cp слепка в контейнер etcd + snapshot status (etcdctl в
            // образе — /usr/local/bin/etcdctl)
            await E2eFixture.RunDockerAsync(["cp", tempFile, $"{etcdFx.ContainerName}:/tmp/snap.db"], ct);
            var verdict = await E2eFixture.RunDockerAsync(
                ["exec", etcdFx.ContainerName, "etcdctl", "snapshot", "status", "/tmp/snap.db"], ct);

            // Assert (а) — положительный вердикт etcdctl 3.5.x: строка
            // «<hash>, <revision>, <ключи>, <размер>». Эмпирика 3.5.x: status
            // hash печатает, но НЕ сверяет — целостность им не доказывается.
            verdict.Should().MatchRegex("^[0-9a-f]{8}, \\d+, \\d+, .+",
                "a valid snapshot from S3 passes etcdctl snapshot status (AC5): hash/keys/size");

            // Act (б) — порча байта; перезаписать temp → cp поверх
            data[100] ^= 0xFF;
            await File.WriteAllBytesAsync(tempFile, data, ct);
            await E2eFixture.RunDockerAsync(["cp", tempFile, $"{etcdFx.ContainerName}:/tmp/bad.db"], ct);

            // Assert (б-1) — ручная сверка sha256 с .meta.json: расхождение
            // (операторский барьер runbook-шага 2 — ловит порчу ДО restore)
            Sha256Hex(data).Should().NotBe(meta.Sha256,
                "a corrupted file must diverge from the sha256 in .meta.json (AC5)");

            // Assert (б-2) — restore-верификация: провал разворачивания. Нюанс etcd
            // 3.5.x: snapshot status hash только ПЕЧАТАЕТ (проверено на живом
            // 3.5.21: порченный байт данных не меняет вердикт), sha256 сверяет
            // restore (etcdutl): «expected sha256 … got …» — финальный барьер
            // runbook-шага «файл с битым хешем не разворачивать» (AC5).
            // Примечание среды: обёртка sh -c "… && echo OK" невыполнима — в
            // образе etcd 3.5.x НЕТ shell (проверено: «sh: executable file not
            // found»); провал фиксируется исключением RunProcessAsync (exit ≠ 0),
            // эквивалент «отсутствия OK» — текст «expected sha256» в stderr.
            var corrupt = await FluentActions.Awaiting(() => E2eFixture.RunDockerAsync(
                ["exec", etcdFx.ContainerName, "etcdutl", "snapshot", "restore", "/tmp/bad.db",
                 "--data-dir", "/tmp/restore-bad"], ct))
                .Should().ThrowAsync<ApplicationException>(
                    "a corrupted snapshot must fail the restore sha256 verification (AC5)");
            corrupt.Which.Message.Should().Contain("sha256",
                "restore rejects a snapshot by a sha256 divergence — the final integrity barrier (AC5)");
        }
        finally
        {
            File.Delete(tempFile);
        }
    }

    // AAA (AC6): takeover — статус-выгрузку пишет инстанс-исполнитель (в мете).
    [Fact]
    public async Task TakeoverStatusWrittenByExecutingInstance()
    {
        // Arrange — своё окружение; два sink'а с разными instance на тех же etcd+S3
        var ct = TestContext.Current.CancellationToken;
        await using var etcdFx = await OwnEtcd.StartAsync("sink7", ct);
        await using var s3NodeFx = await OwnS3.StartAsync("sink7", ct);
        var dirA = Directory.CreateTempSubdirectory("sink-takeover-a-").FullName;
        var dirB = Directory.CreateTempSubdirectory("sink-takeover-b-").FullName;
        var sinkA = await SinkAsync(etcdFx, s3NodeFx, "inst-A", ct: ct);
        var sinkB = await SinkAsync(etcdFx, s3NodeFx, "inst-B", ct: ct);
        var jobA = new SnapshotJob(etcdFx.Gateway, [etcdFx.Endpoint], dirA, 10, 60, sinkA);
        var jobB = new SnapshotJob(etcdFx.Gateway, [etcdFx.Endpoint], dirB, 10, 60, sinkB);

        // Act — слепок A, пауза 1.1 c (имена расходятся), слепок B
        var shotA = await jobA.TakeAsync(ct);
        await Task.Delay(1100, ct);
        var shotB = await jobB.TakeAsync(ct);

        // Assert — две пары; мета новейшего объекта — inst-B; статус OK
        shotA.IsSuccess.Should().BeTrue();
        shotB.IsSuccess.Should().BeTrue();
        var s3 = new BackupS3(s3NodeFx.S3());
        var listed = (await s3.ListPrefixAsync("etcd/", ct: ct)).Value!;
        listed.Should().HaveCount(4);
        var newestMetaKey = $"etcd/{Path.GetFileName(shotB.Value)[..^".db".Length]}.meta.json";
        var meta = EtcdSnapshotMetaJson.Parse(System.Text.Encoding.UTF8.GetString(
            await GetObjectBytesAsync(s3NodeFx, newestMetaKey, ct)))!;
        meta.Instance.Should().Be("inst-B", "the status is written by the executing instance (AC6)");
        var kv = (await etcdFx.Gateway.GetAsync(etcdFx.Endpoint, EtcdSnapshotStatusJson.Key, ct)).Value!;
        EtcdSnapshotStatusJson.Parse(kv.Value)!.State.Should().Be("OK");
    }

    // AAA (t33 + п.5 ревью): обе цели ЖИВЫ → пара уходит ТОЛЬКО на ПЕРВУЮ
    // (приоритет первой цели — arch/19 §5 «перебор всегда с первой»); листинг
    // второй живой цели пуст. Ловит и реверс порядка целей, и skip-first —
    // свойство, которое юниты фабрики до пула не наблюдают.
    [Fact]
    public async Task BothTargetsAlive_PairLandsOnlyOnFirst()
    {
        // Arrange — своё etcd + ДВЕ живые ownS3-ноды; цели [нода1, нода2]
        var ct = TestContext.Current.CancellationToken;
        await using var etcdFx = await OwnEtcd.StartAsync("sink-fo3", ct);
        await using var s3Node1Fx = await OwnS3.StartAsync("sink-fo3", ct);
        await using var s3Node2Fx = await OwnS3.StartAsync("sink-fo3", ct);
        var dir = Directory.CreateTempSubdirectory("sink-first-prio-").FullName;
        var sink = FactorySink(etcdFx, $"{s3Node1Fx.HostEndpoint},{s3Node2Fx.HostEndpoint}", "inst-A");
        var job = new SnapshotJob(etcdFx.Gateway, [etcdFx.Endpoint], dir, 10, 60, sink);

        // Act
        var shot = await job.TakeAsync(ct);

        // Assert — снятие OK; пара db+meta ровно на ПЕРВОЙ цели…
        shot.IsSuccess.Should().BeTrue();
        var s3First = new BackupS3(s3Node1Fx.S3());
        var onFirst = (await s3First.ListPrefixAsync("etcd/", ct: ct)).Value!;
        onFirst.Should().HaveCount(2)
            .And.Contain(k => k.Key.EndsWith(".db"))
            .And.Contain(k => k.Key.EndsWith(".meta.json"));
        // Assert — …вторая живая цель не тронута (листинг пуст).
        var s3Second = new BackupS3(s3Node2Fx.S3());
        var onSecond = (await s3Second.ListPrefixAsync("etcd/", ct: ct)).Value!;
        onSecond.Should().BeEmpty(
            "the pair goes ONLY to the FIRST target (arch/19 §5: always from the first)");
    }

    // AAA (t33): перебор целей пары — первая цель закрытый порт (прецедент
    // BadS3Runtime + E2eFixture.FreePort()), вторая — живая ownS3: выгрузка
    // доезжает до второй цели; пара db+meta реально лежит на живой ноде
    // (креды/bucket пары применились к НЕ-первой цели), статус-ключ OK.
    [Fact]
    public async Task FirstTargetDownSecondAliveUploadsToAlive()
    {
        // Arrange — своё etcd + ownS3; цели [закрытый порт, живая нода]
        var ct = TestContext.Current.CancellationToken;
        await using var etcdFx = await OwnEtcd.StartAsync("sink-fo1", ct);
        await using var s3NodeFx = await OwnS3.StartAsync("sink-fo1", ct);
        var dir = Directory.CreateTempSubdirectory("sink-failover-").FullName;
        var bad = s3NodeFx.S3() with { Endpoint = $"http://localhost:{E2eFixture.FreePort()}" };
        var sink = FactorySink(etcdFx, $"{bad.Endpoint},{s3NodeFx.HostEndpoint}", "inst-A");
        var job = new SnapshotJob(etcdFx.Gateway, [etcdFx.Endpoint], dir, 10, 60, sink);

        // Act
        var shot = await job.TakeAsync(ct);

        // Assert — снятие и выгрузка OK: пара на ЖИВОЙ (второй) цели, статус OK
        shot.IsSuccess.Should().BeTrue();
        var s3 = new BackupS3(s3NodeFx.S3());
        var listed = (await s3.ListPrefixAsync("etcd/", ct: ct)).Value!;
        listed.Should().HaveCount(2)
            .And.Contain(k => k.Key.EndsWith(".db"))
            .And.Contain(k => k.Key.EndsWith(".meta.json"));
        var kv = (await etcdFx.Gateway.GetAsync(etcdFx.Endpoint, EtcdSnapshotStatusJson.Key, ct)).Value!;
        EtcdSnapshotStatusJson.Parse(kv.Value)!.State.Should().Be("OK");
    }

    // AAA (t33): обе цели закрытые порты → снятие живо, выгрузка FAILED с
    // маркером s3-pool-exhausted в error, поля последнего успеха сохранены;
    // доводка живым пулом (CatchUpAsync) — статус OK (семантика arch/19 §5:
    // исчерпание — transient, догоняет тик лидера).
    [Fact]
    public async Task BothTargetsDown_StatusFailedFieldsKept_CatchUpOk()
    {
        // Arrange — успех на живой ноде (база полей последнего успеха)
        var ct = TestContext.Current.CancellationToken;
        await using var etcdFx = await OwnEtcd.StartAsync("sink-fo2", ct);
        await using var s3NodeFx = await OwnS3.StartAsync("sink-fo2", ct);
        var dir = Directory.CreateTempSubdirectory("sink-exhaust-").FullName;
        var liveJob = new SnapshotJob(etcdFx.Gateway, [etcdFx.Endpoint], dir, 10, 60,
            FactorySink(etcdFx, s3NodeFx.HostEndpoint, "inst-A"));
        (await liveJob.TakeAsync(ct)).IsSuccess.Should().BeTrue();
        var okKv = (await etcdFx.Gateway.GetAsync(etcdFx.Endpoint, EtcdSnapshotStatusJson.Key, ct)).Value!;
        var okStatus = EtcdSnapshotStatusJson.Parse(okKv.Value)!;

        // Act — обе цели закрытые порты: снятие живо, выгрузка FAILED.
        // bad1/bad2 обязаны различаться (п.2 ревью): FreePort — зонд bind(0)→stop
        // без удержания, повторный эфемерный порт теоретически возможен —
        // совпадение после дедупа схлопнуло бы цели в одну и исказило сценарий.
        var bad1 = $"http://localhost:{E2eFixture.FreePort()}";
        var bad2 = $"http://localhost:{E2eFixture.FreePort()}";
        while (bad2 == bad1)
            bad2 = $"http://localhost:{E2eFixture.FreePort()}";
        var brokenJob = new SnapshotJob(etcdFx.Gateway, [etcdFx.Endpoint], dir, 10, 60,
            FactorySink(etcdFx, $"{bad1},{bad2}", "inst-A"));
        var shot = await brokenJob.TakeAsync(ct);

        // Assert — снятие OK; статус FAILED + маркер; поля успеха сохранены
        shot.IsSuccess.Should().BeTrue("an upload failure is transient, the snapshot does not fail");
        var failedKv = (await etcdFx.Gateway.GetAsync(etcdFx.Endpoint, EtcdSnapshotStatusJson.Key, ct)).Value!;
        var failed = EtcdSnapshotStatusJson.Parse(failedKv.Value)!;
        failed.State.Should().Be("FAILED");
        failed.Error.Should().Contain("s3-pool-exhausted");
        failed.LastObject.Should().Be(okStatus.LastObject, "the fields of the last success are kept");
        failed.LastUploadedUnix.Should().Be(okStatus.LastUploadedUnix);

        // Act/Assert — доводка живым пулом: новейший локальный доезжает, статус OK
        var catchUp = await FactorySink(etcdFx, s3NodeFx.HostEndpoint, "inst-A").CatchUpAsync(dir, ct);
        catchUp.IsSuccess.Should().BeTrue();
        var finalKv = (await etcdFx.Gateway.GetAsync(etcdFx.Endpoint, EtcdSnapshotStatusJson.Key, ct)).Value!;
        EtcdSnapshotStatusJson.Parse(finalKv.Value)!.State.Should().Be("OK");
    }
}
