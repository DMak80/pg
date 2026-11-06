using EtcdNode.Core;
using Shared.Etcd.Maintenance;
using Xunit;

namespace EtcdNode.IntegrationTests;

// Интеграции конвейера выгрузки DR-слепков (arch/etcdnode/02-snapshots.md):
// экспортёр на живом контуре окружения (client-грань node1) + ownS3-нода
// цели. Перенос механики SnapshotExportTests EtcdWorker-интеграций: пара
// db+meta в фиксированном bucket, статус-ключ /etcdnode/snapshots, транзиент
// цели FAILED → CatchUp, ретенция пар. Кейсы делят контур с ЖИВЫМ агентом
// (CatchUp агента идемпотентен, помехи допустимы): assertions толерантны к
// лишним объектам, порядок кейсов не фиксируется.
[Collection(EtcdNodeCollection.Name)]
public sealed class SnapshotExporterTests(EtcdNodeEnvironment env)
{
    // СВОЙ tempdir выгрузки (не том агента): локальный кэш кейса.
    private static string TempDir() => Directory.CreateTempSubdirectory("etcdn-snap-").FullName;

    // S3-endpoint хостового кейса: из списка цели берём host-порт (localhost);
    // первый элемент списка — сетевой alias owns3:9000 для АГЕНТА в контейнере
    // (хостом не резолвится).
    private static string HostEndpoint(EtcdBackupTarget target)
        => EtcdNodeEnvironment.HostS3Endpoint(target);

    private static SnapshotExporter NewExporter(
        EtcdNodeEnvironment env, string dir, int retentionObjects = 28, int timeoutSec = 30)
        => new(env.Gateway, [env.Endpoint],
            new EtcdBackupTargetResolver(env.Gateway, [env.Endpoint]),
            t => new EtcdSnapshotS3(HostEndpoint(t), t.Bucket, t.AccessKey, t.SecretKey,
                requestTimeout: TimeSpan.FromSeconds(timeoutSec)),
            retentionObjects, timeoutSec, instanceId: "it-instance", intervalMin: 360);

    private static async Task ResetStatusKeyAsync(EtcdNodeEnvironment env)
        => await env.Gateway.DeleteAsync(
            env.Endpoint, EtcdSnapshotStatusJson.Key, prefix: false,
            TestContext.Current.CancellationToken);

    // Снятие+выгрузка одним шагом (хелп кейсов): файл, байты, revision из
    // статуса etcd (best-effort, как цикл агента).
    private static async Task<(string File, byte[] Data, Result Exported)> TakeAndExportAsync(
        EtcdNodeEnvironment env, SnapshotJob job, SnapshotExporter exporter, string dir)
    {
        var ct = TestContext.Current.CancellationToken;
        var shot = await job.TakeAsync(ct);
        shot.IsSuccess.Should().BeTrue(shot.Error?.Message);
        var data = await File.ReadAllBytesAsync(shot.Value, ct);
        var revision = (await env.Gateway.StatusAsync(env.Endpoint, ct)).Value?.Revision;
        var exported = await exporter.ExportAsync(Path.GetFileName(shot.Value), data, (long?)revision, ct);
        return (shot.Value, data, exported);
    }

    // AAA: плановая выгрузка — пара db+meta в etcd-backups, статус OK
    [Fact]
    public async Task Planned_Export_UploadsPair_StatusOk()
    {
        var ct = TestContext.Current.CancellationToken;
        await env.SeedEtcdBackupClusterAsync();
        await ResetStatusKeyAsync(env);
        var dir = TempDir();
        var job = new SnapshotJob(env.Gateway, [env.Endpoint], dir, 10, 60);
        var exporter = NewExporter(env, dir);

        // Act
        var (file, _, exported) = await TakeAndExportAsync(env, job, exporter, dir);
        var dbKey = $"{EtcdExportRetention.Prefix}{Path.GetFileName(file)}";
        var metaKey = $"{EtcdExportRetention.Prefix}{Path.GetFileName(file)[..^".db".Length]}.meta.json";

        // Assert: моя пара лежит в bucket; статус-ключ домена — OK
        exported.IsSuccess.Should().BeTrue(exported.Error?.Message);
        var target = await new EtcdBackupTargetResolver(env.Gateway, [env.Endpoint]).ResolveAsync(ct);
        await using var s3 = new EtcdSnapshotS3(
            EtcdNodeEnvironment.HostS3Endpoint(target.Value), target.Value.Bucket,
            target.Value.AccessKey, target.Value.SecretKey);
        var objects = await s3.ListPrefixAsync(EtcdExportRetention.Prefix, ct);
        objects.IsSuccess.Should().BeTrue(objects.Error?.Message);
        objects.Value.Should().Contain(o => o.Key == dbKey);
        objects.Value.Should().Contain(o => o.Key == metaKey);
        var status = await exporter.ReadStatusAsync(ct);
        status.IsSuccess.Should().BeTrue();
        status.Value!.State.Should().Be("OK");
    }

    // AAA: транзиент цели (битые endpoints) — выгрузка FAILED; восстановление
    // → CatchUp догоняет, статус OK
    [Fact]
    public async Task TransientTarget_FailedThenCatchUp()
    {
        var ct = TestContext.Current.CancellationToken;
        await env.SeedEtcdBackupClusterAsync();
        await ResetStatusKeyAsync(env);
        var dir = TempDir();
        var job = new SnapshotJob(env.Gateway, [env.Endpoint], dir, 10, 60);
        var exporter = NewExporter(env, dir, timeoutSec: 3);
        (await TakeAndExportAsync(env, job, exporter, dir)).Exported.IsSuccess.Should().BeTrue();

        // Act-1: ломаем endpoints — цель резолвится, но недостижима (транспорт);
        // свежий слепок не выгружается, статус FAILED.
        await env.SeedEtcdBackupClusterBrokenEndpointsAsync();
        await Task.Delay(1100, ct); // новое имя файла (метки — секунды)
        var (_, _, failed) = await TakeAndExportAsync(env, job, exporter, dir);

        // Assert-1: FAILED с транспортной ошибкой, поля прошлого успеха
        // сохранены (маркер etcd-backup-missing случая нерезолвимой цели —
        // юниты резолвера).
        failed.IsSuccess.Should().BeFalse();
        var broken = await exporter.ReadStatusAsync(ct);
        broken.Value!.State.Should().BeOneOf("FAILED", "OK",
            "live agent may race its own retry; FAILED asserted via the exported failure");
        if (broken.Value.State == "FAILED")
        {
            broken.Value.Error.Should().NotBeNullOrEmpty("транспортная ошибка недостижимой цели");
            broken.Value.LastUploadedUnix.Should().NotBeNull("поля прошлого успеха сохраняются");
        }

        // Act-2: восстановили цель — CatchUp доводит отстающий слепок.
        await env.SeedEtcdBackupClusterAsync();
        var caughtUp = await exporter.CatchUpAsync(dir, ct);

        // Assert-2: доведено, статус OK.
        caughtUp.IsSuccess.Should().BeTrue(caughtUp.Error?.Message);
        caughtUp.Value.Should().BeTrue("отстающий слепок должен быть довыгружен");
        var recovered = await exporter.ReadStatusAsync(ct);
        recovered.Value!.State.Should().Be("OK");
    }

    // AAA: ретенция — RetentionObjects=2 и три выгрузки: старейшая пара
    // удалена (ассерт по факту отсутствия старейшего .db; точное число
    // толерантно к агенту живого контура)
    [Fact]
    public async Task Retention_RemovesOldestPair()
    {
        var ct = TestContext.Current.CancellationToken;
        await env.SeedEtcdBackupClusterAsync();
        await ResetStatusKeyAsync(env);
        var dir = TempDir();
        var job = new SnapshotJob(env.Gateway, [env.Endpoint], dir, 10, 60);
        var exporter = NewExporter(env, dir, retentionObjects: 2);
        string? oldest = null;
        for (var i = 0; i < 3; i++)
        {
            var (file, _, exported) = await TakeAndExportAsync(env, job, exporter, dir);
            exported.IsSuccess.Should().BeTrue(exported.Error?.Message);
            if (oldest is null)
                oldest = Path.GetFileName(file);
            await Task.Delay(1100, ct); // различные имена (метки — секунды)
        }

        // Act
        var target = await new EtcdBackupTargetResolver(env.Gateway, [env.Endpoint]).ResolveAsync(ct);
        await using var s3 = new EtcdSnapshotS3(
            EtcdNodeEnvironment.HostS3Endpoint(target.Value), target.Value.Bucket,
            target.Value.AccessKey, target.Value.SecretKey);
        var objects = await s3.ListPrefixAsync(EtcdExportRetention.Prefix, ct);

        // Assert: старейшая пара удалена ретенцией; что-то свежее осталось.
        objects.Value.Should().NotContain(o => o.Key == $"{EtcdExportRetention.Prefix}{oldest}");
        objects.Value.Should().Contain(o => o.Key.EndsWith(".db", StringComparison.Ordinal));
    }

    // AAA: обслуживание контура (compact/defrag) — успех; статус etcd читаем
    [Fact]
    public async Task Maintenance_Succeeds_StatusReadable()
    {
        // Arrange: SnapshotJob обслуживания с полным списком endpoints тика
        // (client-грань node1; сам агент узла обслуживает со своего localhost)
        var job = new SnapshotJob(env.Gateway, [env.Endpoint], TempDir(), 10, 60);

        // Act
        var maintenance = await job.MaintainAsync(TestContext.Current.CancellationToken);
        var status = await env.Gateway.StatusAsync(env.Endpoint, TestContext.Current.CancellationToken);

        // Assert
        maintenance.IsSuccess.Should().BeTrue(maintenance.Error?.Message);
        status.IsSuccess.Should().BeTrue();
        status.Value!.Revision.Should().NotBeNull();
        status.Value.DbSizeBytes.Should().NotBeNull();
    }
}
