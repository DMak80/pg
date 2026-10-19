using System.Security.Cryptography;
using EtcdWorker.Core;
using EtcdWorker.IntegrationTests.Docker;
using EtcdWorker.Provisioning;
using Shared.Etcd.Maintenance;
using Xunit;

namespace EtcdWorker.IntegrationTests.Snapshots;

// Интеграции экспорта DR-слепков (arch/25 §4, шаг 9 t32): экспортёр и
// расписание на живом контуре — etcd окружения + ownS3-нода цели (лениво,
// засев /s3/clusters/eb с признаком etcd_backup). Пара db+meta в фиксированном
// bucket etcd-backups, статус /etcdworker/snapshots, транзиент цели —
// FAILED+маркер → CatchUp, ретенция пар, maintenance, детекция изменений.
[Collection(EtcdWorkerCollection.Name)]
public sealed class SnapshotExportTests(EtcdWorkerEnvironment env)
{
    private static string TempDir() => Directory.CreateTempSubdirectory("etcdw-snap-").FullName;

    private static SnapshotExporter NewExporter(
        EtcdWorkerEnvironment env, string dir, int retentionObjects = 28, int timeoutSec = 30)
        => new(env.Gateway, [env.Endpoint],
            new EtcdBackupTargetResolver(env.Gateway, [env.Endpoint]),
            t => new EtcdSnapshotS3(t.Endpoints[0], t.Bucket, t.AccessKey, t.SecretKey,
                requestTimeout: TimeSpan.FromSeconds(timeoutSec)),
            retentionObjects, timeoutSec, instanceId: "it-instance", intervalMin: 360);

    private static async Task ResetStatusKeyAsync(EtcdWorkerEnvironment env)
        => await env.Gateway.DeleteAsync(
            env.Endpoint, EtcdWorker.Etcd.EtcdSnapshotStatusJson.Key, prefix: false,
            TestContext.Current.CancellationToken);

    // Снятие+выгрузка одним шагом (хелп кейсов): файл, байты, revision из
    // статуса etcd (best-effort, как цикл).
    private static async Task<(string File, byte[] Data, Result Exported)> TakeAndExportAsync(
        EtcdWorkerEnvironment env, SnapshotJob job, SnapshotExporter exporter, string dir)
    {
        var ct = TestContext.Current.CancellationToken;
        var shot = await job.TakeAsync(ct);
        shot.IsSuccess.Should().BeTrue(shot.Error?.Message);
        var data = await File.ReadAllBytesAsync(shot.Value, ct);
        var revision = (await env.Gateway.StatusAsync(env.Endpoint, ct)).Value?.Revision;
        var exported = await exporter.ExportAsync(Path.GetFileName(shot.Value), data, (long?)revision, ct);
        return (shot.Value, data, exported);
    }

    // AAA: плановая выгрузка — пара db+meta в etcd-backups, статус OK,
    // sha256 статуса совпадает с байтами слепка
    [Fact]
    public async Task Planned_Export_UploadsPair_StatusOk()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await env.SeedEtcdBackupClusterAsync();
        await ResetStatusKeyAsync(env);
        var dir = TempDir();
        var job = new SnapshotJob(env.Gateway, [env.Endpoint], dir, 10, 60);
        var exporter = NewExporter(env, dir);

        // Act
        var (_, data, exported) = await TakeAndExportAsync(env, job, exporter, dir);

        // Assert
        exported.IsSuccess.Should().BeTrue(exported.Error?.Message);
        var status = await exporter.ReadStatusAsync(ct);
        status.IsSuccess.Should().BeTrue();
        status.Value!.State.Should().Be("OK");
        status.Value.LastSha256.Should().Be(
            Convert.ToHexString(SHA256.HashData(data)).ToLowerInvariant());
        var target = await new EtcdBackupTargetResolver(env.Gateway, [env.Endpoint]).ResolveAsync(ct);
        target.IsSuccess.Should().BeTrue();
        await using var s3 = new EtcdSnapshotS3(
            target.Value.Endpoints[0], target.Value.Bucket, target.Value.AccessKey, target.Value.SecretKey);
        var objects = await s3.ListPrefixAsync("etcd/", ct);
        objects.IsSuccess.Should().BeTrue(objects.Error?.Message);
        objects.Value.Should().Contain(o => o.Key.EndsWith(".db", StringComparison.Ordinal));
        objects.Value.Should().Contain(o => o.Key.EndsWith(".meta.json", StringComparison.Ordinal));
    }

    // AAA: транзиент цели (битые endpoints) — выгрузка FAILED с маркером
    // etcd-backup-missing; восстановление → CatchUp догоняет, статус OK
    [Fact]
    public async Task TransientTarget_FailedThenCatchUp()
    {
        DockerTrait.SkipIfUnavailable();
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

        // Assert-1: FAILED с транспортной ошибкой (маркер etcd-backup-missing —
        // случай нерезолвимой цели, покрыт юнитами резолвера), поля прошлого
        // успеха сохранены.
        failed.IsSuccess.Should().BeFalse();
        var broken = await exporter.ReadStatusAsync(ct);
        broken.Value!.State.Should().Be("FAILED");
        broken.Value.Error.Should().NotBeNullOrEmpty("транспортная ошибка недостижимой цели");
        broken.Value.LastUploadedUnix.Should().NotBeNull("поля прошлого успеха сохраняются");

        // Act-2: восстановили цель — CatchUp доводит отстающий слепок.
        await env.SeedEtcdBackupClusterAsync();
        var caughtUp = await exporter.CatchUpAsync(dir, ct);

        // Assert-2: доведено, статус OK.
        caughtUp.IsSuccess.Should().BeTrue(caughtUp.Error?.Message);
        caughtUp.Value.Should().BeTrue("отстающий слепок должен быть довыгружен");
        var recovered = await exporter.ReadStatusAsync(ct);
        recovered.Value!.State.Should().Be("OK");
    }

    // AAA: ретенция — RetentionObjects=2, три выгрузки → в bucket ровно
    // 2 пары (старейшая удалена)
    [Fact]
    public async Task Retention_KeepsTwoPairs()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await env.SeedEtcdBackupClusterAsync();
        await ResetStatusKeyAsync(env);
        var dir = TempDir();
        var job = new SnapshotJob(env.Gateway, [env.Endpoint], dir, 10, 60);
        var exporter = NewExporter(env, dir, retentionObjects: 2);
        for (var i = 0; i < 3; i++)
        {
            (await TakeAndExportAsync(env, job, exporter, dir)).Exported.IsSuccess.Should().BeTrue();
            await Task.Delay(1100, ct); // различные имена (метки — секунды)
        }

        // Act
        var target = await new EtcdBackupTargetResolver(env.Gateway, [env.Endpoint]).ResolveAsync(ct);
        await using var s3 = new EtcdSnapshotS3(
            target.Value.Endpoints[0], target.Value.Bucket, target.Value.AccessKey, target.Value.SecretKey);
        var objects = await s3.ListPrefixAsync("etcd/", ct);

        // Assert: 2 пары = 4 объекта (db+meta каждая).
        objects.Value.Should().HaveCount(4);
        objects.Value.Count(o => o.Key.EndsWith(".db", StringComparison.Ordinal)).Should().Be(2);
        objects.Value.Count(o => o.Key.EndsWith(".meta.json", StringComparison.Ordinal)).Should().Be(2);
    }

    // AAA: maintenance (compact/defrag) — успех; статус etcd читаем
    // (revision/DbSize фактические)
    [Fact]
    public async Task Maintenance_Succeeds_StatusReadable()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        var dir = TempDir();
        var job = new SnapshotJob(env.Gateway, [env.Endpoint], dir, 10, 60);

        // Act
        var maintenance = await job.MaintainAsync(ct);
        var status = await env.Gateway.StatusAsync(env.Endpoint, ct);

        // Assert
        maintenance.IsSuccess.Should().BeTrue(maintenance.Error?.Message);
        status.IsSuccess.Should().BeTrue();
        status.Value!.Revision.Should().NotBeNull();
        status.Value.DbSizeBytes.Should().NotBeNull();
    }

    // AAA: детекция изменений — put-ключ растит revision etcd; ShouldTake с
    // выросшей revision и несвежей выгрузкой → внеочередной; без изменений —
    // нет (фактические revision живого контура)
    [Fact]
    public async Task ChangeDetection_RevisionGrowth_TriggersExtraSnapshot()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        var beforeRev = (long)(await env.Gateway.StatusAsync(env.Endpoint, ct)).Value!.Revision!;

        // Act: изменение в etcd (put своего ключа префикса сценария).
        await env.PutAsync("/change-detection", "1");
        var afterRev = (long)(await env.Gateway.StatusAsync(env.Endpoint, ct)).Value!.Revision!;
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

        // Assert: revision вырос; расписание видит изменение при несвежей
        // выгрузке; тишина (revision == последней выгрузке) — плановый каденс.
        afterRev.Should().BeGreaterThan(beforeRev);
        SnapshotSchedule.ShouldTake(
            now, lastTakeUnix: now - 60, lastUploadedUnix: now - 301,
            currentRevision: afterRev, lastExportedRevision: beforeRev,
            intervalMin: 360, minIntervalSec: 300).Should().BeTrue();
        SnapshotSchedule.ShouldTake(
            now, lastTakeUnix: now - 60, lastUploadedUnix: now - 301,
            currentRevision: afterRev, lastExportedRevision: afterRev,
            intervalMin: 360, minIntervalSec: 300).Should().BeFalse();
    }
}
