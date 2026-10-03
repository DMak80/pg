using System.Security.Cryptography;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using FluentAssertions;
using PgWorker.Backups;
using PgWorker.Backups.EtcdExport;
using PgWorker.IntegrationTests.E2e;
using Shared.Etcd.Maintenance;
using Xunit;

namespace PgWorker.IntegrationTests.Backups;

// S3-sink снапшотов etcd (t08): OwnEtcd + OwnMinio на Fact (своё окружение,
// динамические порты, teardown при любом исходе — e2e-isolation §1/§3).
// Сценарии: экспорт после TakeAsync, каждый слепок новой парой, ретенция,
// транзиент → FAILED → доводка, верификация разворачиваемости (etcdctl
// snapshot status), takeover.
public class EtcdSnapshotSinkTests
{
    // Прямой клиент-помощник для сида/чтения объектов (AWSSDK, не тестируемый код).
    private static AmazonS3Client SeedClient(OwnMinio minio) => new(
        new BasicAWSCredentials(OwnMinio.AccessKey, OwnMinio.SecretKey),
        new AmazonS3Config { ServiceURL = minio.HostEndpoint, ForcePathStyle = true });

    private static BackupsRuntimeOptions BadS3Runtime(OwnMinio minio)
        => minio.Runtime() with { S3Endpoint = $"http://localhost:{E2eFixture.FreePort()}" }; // закрытый порт — транзиент

    private static async Task<EtcdSnapshotSink> SinkAsync(OwnEtcd etcd, OwnMinio minio, string instance,
        int retention = 28, CancellationToken ct = default)
    {
        var s3 = new BackupS3(minio.Runtime());
        return new EtcdSnapshotSink(s3, etcd.Gateway, [etcd.Endpoint], retention, 5, instance, 360);
    }

    // Чтение объекта bucket'a напрямую (проверка факта «реально лежит в S3»).
    private static async Task<byte[]> GetObjectBytesAsync(OwnMinio minio, string key, CancellationToken ct)
    {
        using var client = SeedClient(minio);
        var response = await client.GetObjectAsync(new GetObjectRequest { BucketName = OwnMinio.Bucket, Key = key }, ct);
        using var ms = new MemoryStream();
        await response.ResponseStream.CopyToAsync(ms, ct);
        return ms.ToArray();
    }

    private static string Sha256Hex(byte[] data)
        => Convert.ToHexString(SHA256.HashData(data)).ToLowerInvariant();

    // AAA: снятие вызывает sink — в S3 пара db+meta по канону нейминга, статус
    // OK с фактом, sha256 совпадает; объекты etcd/* видны list'ом всего bucket.
    [Fact]
    public async Task Export_после_TakeAsync_объекты_мета_статус_OK()
    {
        // Arrange — своё etcd + MinIO; sink; SnapshotJob с sink'ом
        var ct = TestContext.Current.CancellationToken;
        await using var etcdFx = await OwnEtcd.StartAsync("sink1", ct);
        await using var minioFx = await OwnMinio.StartAsync("sink1", ct);
        var dir = Directory.CreateTempSubdirectory("sink-export-").FullName;
        var sink = await SinkAsync(etcdFx, minioFx, "inst-A", ct: ct);
        var job = new SnapshotJob(etcdFx.Gateway, [etcdFx.Endpoint], dir, 10, 60, sink);

        // Act
        var shot = await job.TakeAsync(ct);

        // Assert — снятие успешно; в S3 ровно пара db+meta; статус OK с фактом
        shot.IsSuccess.Should().BeTrue();
        var s3 = new BackupS3(minioFx.Runtime());
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
        // — база used_bytes ключа storage (t06 считает list ""; ср. BackupS3Tests:189)
        var wholeBucket = (await s3.ListPrefixAsync("", ct: ct)).Value!;
        wholeBucket.Select(o => o.Key).Should().Contain(listed.Select(o => o.Key),
            "объекты etcd/ входят в used_bytes — list всего bucket их видит");
        // meta: taken <= uploaded, instance, ревизия best-effort есть (OwnEtcd жив)
        var metaKey = listed.Single(k => k.Key.EndsWith(".meta.json")).Key;
        var meta = EtcdSnapshotMetaJson.Parse(System.Text.Encoding.UTF8.GetString(
            await GetObjectBytesAsync(minioFx, metaKey, ct)))!;
        meta.TakenUnix.Should().BeLessThanOrEqualTo(meta.UploadedUnix);
        meta.Instance.Should().Be("inst-A");
        meta.Revision.Should().NotBeNull();
    }

    // AAA (AC2): каждый слепок — НОВАЯ пара db+meta (дедупа нет); покрытие
    // продвигается вторым слепком; IsBehind=false — сон лидера остаётся плановым.
    [Fact]
    public async Task Каждый_слепок_выгружается_новой_парой()
    {
        // Arrange — своё окружение; sink; SnapshotJob с sink'ом
        var ct = TestContext.Current.CancellationToken;
        await using var etcdFx = await OwnEtcd.StartAsync("sink2", ct);
        await using var minioFx = await OwnMinio.StartAsync("sink2", ct);
        var dir = Directory.CreateTempSubdirectory("sink-pairs-").FullName;
        var sink = await SinkAsync(etcdFx, minioFx, "inst-A", ct: ct);
        var job = new SnapshotJob(etcdFx.Gateway, [etcdFx.Endpoint], dir, 10, 60, sink);

        // Act — два снятия с паузой 1.1 c (имена расходятся по секундам)
        var shot1 = await job.TakeAsync(ct);
        await Task.Delay(1100, ct);
        var shot2 = await job.TakeAsync(ct);

        // Assert — (а) в S3 4 ключа: ДВЕ пары; локальных файлов 2
        shot1.IsSuccess.Should().BeTrue();
        shot2.IsSuccess.Should().BeTrue();
        var s3 = new BackupS3(minioFx.Runtime());
        var listed = (await s3.ListPrefixAsync("etcd/", ct: ct)).Value!;
        listed.Should().HaveCount(4, "второй слепок уезжает новой парой — дедупа нет (AC2)");
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
        status.LastUploadedUnix.Should().Be(taken2, "покрытие продвинуто вторым слепком (§3.1)");
        status.LastObject.Should().Be($"etcd/{secondName}");
        status.LastSha256.Should().Be(Sha256Hex(await File.ReadAllBytesAsync(shot2.Value, ct)));
        status.SizeBytes.Should().Be((await File.ReadAllBytesAsync(shot2.Value, ct)).LongLength);
        // Assert — (в) отставания нет: сон лидера остаётся SnapshotIntervalMin
        EtcdSnapshotStatus.IsBehind(status, taken2).Should().BeFalse(
            "каждый успешный проход sink'а продвигает покрытие (инвариант §3.5 п.1)");
    }

    // AAA (AC4): ретенция — ровно N пар, старейшие снесены; сирота-meta — мусор.
    [Fact]
    public async Task Ретенция_сверх_лимита_старейшие_пары_снесены()
    {
        // Arrange — sink с ретенцией 2; три снятия с паузами
        var ct = TestContext.Current.CancellationToken;
        await using var etcdFx = await OwnEtcd.StartAsync("sink3", ct);
        await using var minioFx = await OwnMinio.StartAsync("sink3", ct);
        var dir = Directory.CreateTempSubdirectory("sink-retain-").FullName;
        var sink = await SinkAsync(etcdFx, minioFx, "inst-A", retention: 2, ct: ct);
        var job = new SnapshotJob(etcdFx.Gateway, [etcdFx.Endpoint], dir, 10, 60, sink);
        await job.TakeAsync(ct);
        await Task.Delay(1100, ct);
        await job.TakeAsync(ct);
        await Task.Delay(1100, ct);
        await job.TakeAsync(ct);

        // Assert — ровно 2 пары (4 ключа); старейший id отсутствует
        var s3 = new BackupS3(minioFx.Runtime());
        var listed = (await s3.ListPrefixAsync("etcd/", ct: ct)).Value!;
        listed.Should().HaveCount(4, "ретенция держит ровно N=2 пары");
        var oldestId = listed.Select(k => k.Key).Where(k => k.EndsWith(".db"))
            .Select(k => k[(EtcdExportRetention.Prefix.Length)..][..^".db".Length])
            .OrderBy(id => id, StringComparer.Ordinal).First();
        // Arrange/Act — сид сироты-meta без .db → четвёртое снятие сносит её
        using var client = SeedClient(minioFx);
        await client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = OwnMinio.Bucket,
            Key = "etcd/snapshot-20260101-000000.meta.json",
            ContentBody = """{"sha256":"x"}""",
        }, ct);
        await Task.Delay(1100, ct);
        var shot4 = await job.TakeAsync(ct);

        // Assert — сирота снесена ретенцией; пар по-прежнему 2
        shot4.IsSuccess.Should().BeTrue();
        var after = (await s3.ListPrefixAsync("etcd/", ct: ct)).Value!;
        after.Should().NotContain(k => k.Key == "etcd/snapshot-20260101-000000.meta.json",
            "одиночная meta без .db — мусор ретенции (AC4)");
        after.Should().HaveCount(4);
        after.Should().NotContain(k => k.Key.EndsWith($"{oldestId}.db"));
    }

    // AAA (AC3): S3-транзиент не роняет снятие; статус FAILED + непустая ошибка.
    [Fact]
    public async Task Транзиент_S3_не_роняет_снятие_статус_FAILED()
    {
        // Arrange — sink на закрытый порт (S3 недоступен); SnapshotJob с ним
        var ct = TestContext.Current.CancellationToken;
        await using var etcdFx = await OwnEtcd.StartAsync("sink4", ct);
        await using var minioFx = await OwnMinio.StartAsync("sink4", ct);
        var dir = Directory.CreateTempSubdirectory("sink-transient-").FullName;
        var badS3 = new BackupS3(BadS3Runtime(minioFx));
        var sink = new EtcdSnapshotSink(badS3, etcdFx.Gateway, [etcdFx.Endpoint], 28, 5, "inst-A", 360);
        var job = new SnapshotJob(etcdFx.Gateway, [etcdFx.Endpoint], dir, 10, 60, sink);

        // Act
        var shot = await job.TakeAsync(ct);

        // Assert — снятие успешно (локальный файл — истина снятия), статус FAILED
        shot.IsSuccess.Should().BeTrue("сбой выгрузки — транзиент, снятие не роняется (spec §2.3)");
        File.Exists(shot.Value).Should().BeTrue();
        var kv = (await etcdFx.Gateway.GetAsync(etcdFx.Endpoint, EtcdSnapshotStatusJson.Key, ct)).Value!;
        var status = EtcdSnapshotStatusJson.Parse(kv.Value)!;
        status.State.Should().Be("FAILED");
        status.Error.Should().NotBeNullOrEmpty();
    }

    // AAA (AC3): доводка — новейший локальный слепок доезжает в S3, статус OK.
    [Fact]
    public async Task Доводка_CatchUp_выгружает_новейший_локальный()
    {
        // Arrange — слепок снят при недоступном S3 (FAILED-статус), затем живой sink
        var ct = TestContext.Current.CancellationToken;
        await using var etcdFx = await OwnEtcd.StartAsync("sink5", ct);
        await using var minioFx = await OwnMinio.StartAsync("sink5", ct);
        var dir = Directory.CreateTempSubdirectory("sink-catchup-").FullName;
        var badS3 = new BackupS3(BadS3Runtime(minioFx));
        var brokenSink = new EtcdSnapshotSink(badS3, etcdFx.Gateway, [etcdFx.Endpoint], 28, 5, "inst-A", 360);
        var brokenJob = new SnapshotJob(etcdFx.Gateway, [etcdFx.Endpoint], dir, 10, 60, brokenSink);
        (await brokenJob.TakeAsync(ct)).IsSuccess.Should().BeTrue();

        var liveSink = await SinkAsync(etcdFx, minioFx, "inst-A", ct: ct);

        // Act — доводка отстающей выгрузки
        var catchUp = await liveSink.CatchUpAsync(dir, ct);

        // Assert — новейший локальный доехал: объекты есть, статус OK, sha совпадает
        catchUp.IsSuccess.Should().BeTrue();
        catchUp.Value.Should().BeTrue();
        var latest = Directory.GetFiles(dir, "snapshot-*.db").OrderByDescending(f => f, StringComparer.Ordinal).First();
        var s3 = new BackupS3(minioFx.Runtime());
        var listed = (await s3.ListPrefixAsync("etcd/", ct: ct)).Value!;
        listed.Should().Contain(k => k.Key == $"etcd/{Path.GetFileName(latest)}");
        var kv = (await etcdFx.Gateway.GetAsync(etcdFx.Endpoint, EtcdSnapshotStatusJson.Key, ct)).Value!;
        var status = EtcdSnapshotStatusJson.Parse(kv.Value)!;
        status.State.Should().Be("OK");
        status.LastSha256.Should().Be(Sha256Hex(await File.ReadAllBytesAsync(latest, ct)));
    }

    // AAA (AC5): скачанный из S3 слепок проходит etcdctl snapshot status;
    // порченный байт — провал проверки (runbook-шаг «проверка до восстановления»).
    [Fact]
    public async Task Разворачиваемость_etcdctl_snapshot_status()
    {
        // Arrange — экспорт слепка в S3; скачивание .db в temp-файл
        var ct = TestContext.Current.CancellationToken;
        await using var etcdFx = await OwnEtcd.StartAsync("sink6", ct);
        await using var minioFx = await OwnMinio.StartAsync("sink6", ct);
        var dir = Directory.CreateTempSubdirectory("sink-verify-").FullName;
        var sink = await SinkAsync(etcdFx, minioFx, "inst-A", ct: ct);
        var job = new SnapshotJob(etcdFx.Gateway, [etcdFx.Endpoint], dir, 10, 60, sink);
        var shot = await job.TakeAsync(ct);
        shot.IsSuccess.Should().BeTrue();
        var s3 = new BackupS3(minioFx.Runtime());
        var listed = (await s3.ListPrefixAsync("etcd/", ct: ct)).Value!;
        var dbKey = listed.Single(k => k.Key.EndsWith(".db")).Key;
        var data = await GetObjectBytesAsync(minioFx, dbKey, ct);
        var tempFile = Path.Combine(Path.GetTempPath(), $"pgw-snap-{Guid.NewGuid():N}.db");
        await File.WriteAllBytesAsync(tempFile, data, ct);
        try
        {
            // Act — cp слепка в контейнер etcd + snapshot status (etcdctl в
            // образе — /usr/local/bin/etcdctl; shell в образе НЕТ — без sh -c)
            await E2eFixture.RunDockerAsync(["cp", tempFile, $"{etcdFx.ContainerName}:/tmp/snap.db"], ct);
            var verdict = await E2eFixture.RunDockerAsync(
                ["exec", etcdFx.ContainerName, "etcdctl", "snapshot", "status", "/tmp/snap.db"], ct);

            // Assert — вердикт etcdctl 3.5.x: строка «<hash>, <revision>, <ключи>,
            // <размер>» (Hash+размер в одной строке; таблица — формат etcdutl)
            verdict.Should().MatchRegex("^[0-9a-f]{8}, \\d+, \\d+, .+",
                "слепок из S3 проходит etcdctl snapshot status (AC5): hash/keys/size");

            // Act/Assert — порча байта → разворачивание отклонено. Нюанс etcd
            // 3.5.x: snapshot status hash только ПЕЧАТАЕТ (проверено на живом
            // 3.5.21: порченный байт данных не меняет вердикт), верификация
            // sha256 — в restore (etcdutl): «expected sha256 … got …» — ровно
            // runbook-шаг «файл с битым хешем не разворачивать» (AC5).
            data[100] ^= 0xFF;
            await File.WriteAllBytesAsync(tempFile, data, ct);
            await E2eFixture.RunDockerAsync(["cp", tempFile, $"{etcdFx.ContainerName}:/tmp/bad.db"], ct);
            var corrupt = await FluentActions.Awaiting(() => E2eFixture.RunDockerAsync(
                ["exec", etcdFx.ContainerName, "etcdutl", "snapshot", "restore", "/tmp/bad.db",
                 "--data-dir", "/tmp/rest-bad"], ct))
                .Should().ThrowAsync<ApplicationException>(
                    "порченный слепок обязан провалить restore-верификацию sha256 (AC5)");
            corrupt.Which.Message.Should().Contain("sha256");
        }
        finally
        {
            File.Delete(tempFile);
        }
    }

    // AAA (AC6): takeover — статус-выгрузку пишет инстанс-исполнитель (в мете).
    [Fact]
    public async Task Takeover_статус_пишет_инстанс_исполнитель()
    {
        // Arrange — своё окружение; два sink'а с разными instance на тех же etcd+minio
        var ct = TestContext.Current.CancellationToken;
        await using var etcdFx = await OwnEtcd.StartAsync("sink7", ct);
        await using var minioFx = await OwnMinio.StartAsync("sink7", ct);
        var dirA = Directory.CreateTempSubdirectory("sink-takeover-a-").FullName;
        var dirB = Directory.CreateTempSubdirectory("sink-takeover-b-").FullName;
        var sinkA = await SinkAsync(etcdFx, minioFx, "inst-A", ct: ct);
        var sinkB = await SinkAsync(etcdFx, minioFx, "inst-B", ct: ct);
        var jobA = new SnapshotJob(etcdFx.Gateway, [etcdFx.Endpoint], dirA, 10, 60, sinkA);
        var jobB = new SnapshotJob(etcdFx.Gateway, [etcdFx.Endpoint], dirB, 10, 60, sinkB);

        // Act — слепок A, пауза 1.1 c (имена расходятся), слепок B
        var shotA = await jobA.TakeAsync(ct);
        await Task.Delay(1100, ct);
        var shotB = await jobB.TakeAsync(ct);

        // Assert — две пары; мета новейшего объекта — inst-B; статус OK
        shotA.IsSuccess.Should().BeTrue();
        shotB.IsSuccess.Should().BeTrue();
        var s3 = new BackupS3(minioFx.Runtime());
        var listed = (await s3.ListPrefixAsync("etcd/", ct: ct)).Value!;
        listed.Should().HaveCount(4);
        var newestMetaKey = $"etcd/{Path.GetFileName(shotB.Value)[..^".db".Length]}.meta.json";
        var meta = EtcdSnapshotMetaJson.Parse(System.Text.Encoding.UTF8.GetString(
            await GetObjectBytesAsync(minioFx, newestMetaKey, ct)))!;
        meta.Instance.Should().Be("inst-B", "статус пишет инстанс-исполнитель (AC6)");
        var kv = (await etcdFx.Gateway.GetAsync(etcdFx.Endpoint, EtcdSnapshotStatusJson.Key, ct)).Value!;
        EtcdSnapshotStatusJson.Parse(kv.Value)!.State.Should().Be("OK");
    }
}
