using FluentAssertions;
using PgWorker.Backups;
using PgWorker.Backups.Supervisor;
using PgWorker.Core.Model;
using Shared.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.Etcd.Parsing;
using PgWorker.IntegrationTests.Etcd;
using Xunit;

namespace PgWorker.IntegrationTests.Backups;

// Интеграции BackupSupervisorProcess (t07, spec §3.4): реальный etcd (клэйм/
// журнал) + FakeBackupS3 (префиксы/удаления). Снапшот кластера строится руками
// (паттерн WalStreamProcessTests). Isolation — чистка своих префиксов в Arrange.
[Collection(EtcdCollection.Name)]
public class BackupSupervisorProcessTests(EtcdFixture fixture) : IAsyncLifetime
{
    // Per-class guid-тег (канон docs/e2e-isolation.md §1): имена кластеров уникальны
    // per-class-запуск — пересечение с ShardScaleContractTests (sc1..sc6) и любым
    // будущим классом EtcdCollection механически невозможно (инцидент t07: клэйм
    // sc3 этого класса жил 15с по TTL и ронял TryClaim жертвы).
    private static readonly string Tag = Guid.NewGuid().ToString("N")[..8];

    private readonly ClaimStore _claims = new([fixture.Endpoint], fixture.Gateway, TimeProvider.System);

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    // Own-only teardown клэйма (t12 spec §4.2 п.3): DisposeAsync отзывает lease —
    // живых /pgworker/claims/<C> после тестов класса не остаётся, без TTL-ожидания.
    public ValueTask DisposeAsync() => _claims.DisposeAsync();

    // ── Хелперы Arrange (копия паттерна WalStreamProcessTests) ──

    private static ClusterSnapshot BuildSnap(string cluster = "c1") => new(
        new ClusterConfig(cluster, 2, cluster, null, ClusterState.Active),
        [new ShardSpec("shard1", 1, $"host=shard1a dbname={cluster}", "shard1a:17001",
            [new NodeSpec("shard1", "shard1a", NodeState.Running)])],
        []);

    private async Task SeedAsync(string cluster)
    {
        var ct = TestContext.Current.CancellationToken;
        await fixture.Gateway.DeleteAsync(fixture.Endpoint, $"/pgworker/backups/{cluster}/", prefix: true, ct);
        await fixture.Gateway.DeleteAsync(fixture.Endpoint, $"/pgworker/claims/{cluster}", prefix: false, ct);
        await fixture.Gateway.DeleteAsync(fixture.Endpoint, $"/pgworker/work/{cluster}", prefix: false, ct);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue("клэйм — предусловие тика");
    }

    private BackupSupervisorProcess BuildProcess(
        BackupsRuntimeOptions? options, FakeBackupS3 s3) => new(
        fixture.Gateway, [fixture.Endpoint], s3, _claims,
        new WorkJournal(fixture.Gateway, [fixture.Endpoint]),
        () => options, TimeProvider.System);

    private static BackupsRuntimeOptions Options(int supervisorInterval = 0) => new(
        Enabled: true,
        S3Endpoint: "http://minio",
        S3Bucket: "bkt",
        SupervisorIntervalSec: supervisorInterval);

    private static ShardBackups FullsOf(params FullBackupState[] fulls) => new(fulls, null);

    // AAA (AC6): мусор full/<id>/ живого шарда удаляется первым проходом, journal
    // пишет supervisor/swept-full/<X>/<id>; повторный проход — no-op
    [Fact]
    public async Task Проход_мусор_живого_шарда_удаляется_идемпотентно()
    {
        // Arrange — клэйм c1; объекты c1/shard1/full/A/... и full/B/...;
        // etcd-ключ только для A (COMPLETED); S3 wal-объекты — живы
        var ct = TestContext.Current.CancellationToken;
        var cluster = $"sc1{Tag}";
        await SeedAsync(cluster);
        var s3 = new FakeBackupS3();
        s3.PrefixObjects.Add(($"{cluster}/shard1/full/20260911110000Z/backup_manifest", 100));
        s3.PrefixObjects.Add(($"{cluster}/shard1/full/20260911110000Z/backup_label", 50));
        s3.PrefixObjects.Add(($"{cluster}/shard1/full/20260911120000Z/backup_manifest", 100));
        s3.Objects.Add((cluster, "shard1", "000000010000000000000001"));
        var owned = new FullBackupState("20260911110000Z", FullBackupStatus.Completed, "shard1a",
            BackupSourceRole.Replica, 1757500000, 1757500300, "000000010000000000000001", 150, null, null);
        var process = BuildProcess(Options(), s3);
        var backups = new ClusterBackups(cluster, null,
            new Dictionary<string, ShardBackups> { ["shard1"] = FullsOf(owned) });

        // Act 1 — проход супервизора
        (await process.TickAsync(BuildSnap(cluster), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert 1 — объекты B удалены, A и wal/ живы; journal-факт swept-full
        s3.DeletedKeys.Should().Contain($"{cluster}/shard1/full/20260911120000Z/backup_manifest");
        s3.DeletedKeys.Should().HaveCount(1, "владеемый префикс и wal/ не трогаются");
        s3.PrefixObjects.Should().Contain(o => o.Key == $"{cluster}/shard1/full/20260911110000Z/backup_manifest");
        var journal = await fixture.Gateway.GetAsync(fixture.Endpoint, $"/pgworker/work/{cluster}", ct);
        journal.Value!.Value.Should().Contain("swept-full/shard1/20260911120000Z");

        // Act 2 — повторный проход
        (await process.TickAsync(BuildSnap(cluster), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert 2 — новых удалений нет (list подтверждает пустоту — no-op)
        s3.DeletedKeys.Should().HaveCount(1, "идемпотентность: повторный проход ничего не удаляет");
    }

    // AAA (AC9): шард в активном restore — супервизор его не трогает
    [Fact]
    public async Task Проход_скипает_шард_в_активном_restore()
    {
        // Arrange — мусорный префикс есть, но у шарда активная (PLANNED) заявка
        var ct = TestContext.Current.CancellationToken;
        var cluster = $"sc2{Tag}";
        await SeedAsync(cluster);
        var s3 = new FakeBackupS3();
        s3.PrefixObjects.Add(($"{cluster}/shard1/full/20260911120000Z/backup_manifest", 100));
        var process = BuildProcess(Options(), s3);
        var restoring = new ShardBackups([], null,
            [new RestoreOperationState("20260911130000Z", RestoreStatus.Planned,
                "", $"{cluster}/shard1", "latest", "shard1a", 1760000000, "operator")]);
        var backups = new ClusterBackups(cluster, null,
            new Dictionary<string, ShardBackups> { ["shard1"] = restoring });

        // Act
        (await process.TickAsync(BuildSnap(cluster), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — объекты живы (гвард владельца), журнал-фактов нет
        s3.DeletedKeys.Should().BeEmpty("restore владеет шардом — сверка не выполняется");
        s3.PrefixObjects.Should().ContainSingle();
    }

    // AAA: гварды — Enabled=false (runtime()==null) / не-Active кластер / чужой
    // клэйм: no-op либо отказ, мутаций нет (по образцу RetentionProcess-тестов)
    [Fact]
    public async Task Гварды_Enabled_и_клэйм()
    {
        // Arrange 1 — Enabled=false: проход не выполняется
        var ct = TestContext.Current.CancellationToken;
        var cluster = $"sc3{Tag}";
        await SeedAsync(cluster);
        var s3 = new FakeBackupS3();
        s3.PrefixObjects.Add(($"{cluster}/shard1/full/20260911120000Z/backup_manifest", 100));
        var disabled = BuildProcess(null, s3);
        (await disabled.TickAsync(BuildSnap(cluster), null, ct)).IsSuccess.Should().BeTrue();
        s3.DeletedKeys.Should().BeEmpty("Enabled=false — no-op");

        // Arrange 2 — не-Active кластер: Done без list
        var s3b = new FakeBackupS3();
        s3b.PrefixObjects.Add(($"{cluster}/shard1/full/20260911120000Z/backup_manifest", 100));
        var process = BuildProcess(Options(), s3b);
        var notActive = new ClusterSnapshot(
            new ClusterConfig(cluster, 2, cluster, null, ClusterState.NotInitialized),
            [new ShardSpec("shard1", 1, $"host=shard1a dbname={cluster}", "shard1a:17001",
                [new NodeSpec("shard1", "shard1a", NodeState.Running)])],
            []);
        (await process.TickAsync(notActive, null, ct)).IsSuccess.Should().BeTrue();
        s3b.DeletedKeys.Should().BeEmpty("не-Active — no-op");

        // Arrange 3 — чужой клэйм: отказ до любых мутаций
        var s3c = new FakeBackupS3();
        s3c.PrefixObjects.Add(($"{cluster}/shard1/full/20260911120000Z/backup_manifest", 100));
        await using var claims2 = new ClaimStore([fixture.Endpoint], fixture.Gateway, TimeProvider.System);
        var other = new BackupSupervisorProcess(
            fixture.Gateway, [fixture.Endpoint], s3c, claims2,
            new WorkJournal(fixture.Gateway, [fixture.Endpoint]),
            () => Options(), TimeProvider.System);
        (await other.TickAsync(BuildSnap(cluster), null, ct)).IsSuccess.Should().BeFalse(
            "мутации без клэйма запрещены");
        s3c.DeletedKeys.Should().BeEmpty();
    }
}
