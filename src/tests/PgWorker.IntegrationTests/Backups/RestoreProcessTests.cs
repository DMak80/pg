using FluentAssertions;
using PgWorker.Backups;
using PgWorker.Backups.Process;
using PgWorker.Backups.Restore;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.Etcd.Parsing;
using PgWorker.IntegrationTests.Etcd;
using PgWorker.Core.Templates;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Processes;
using PgWorker.Provisioning.Probes;
using Xunit;

namespace PgWorker.IntegrationTests.Backups;

// Интеграции RestoreProcess (t05 spec Ф2): реальный etcd (статусы/журнал) +
// фейки docker/S3. Снапшот кластера и бэкапы строятся руками. PLANNED-фаза:
// валидация (усыновление/полный/manifest/цепочка) и гвард дублей.
[Collection(EtcdCollection.Name)]
public class RestoreProcessTests(EtcdFixture fixture)
{
    private readonly ClaimStore _claims = new([fixture.Endpoint], fixture.Gateway, TimeProvider.System);

    // ── Хелперы Arrange ──

    // Активный кластер c1/shard1 с канонической нодой shard1a.
    private static ClusterSnapshot BuildSnap(string cluster = "c1", string shard = "shard1") => new(
        new ClusterConfig(cluster, 2, cluster, null, ClusterState.Active),
        [new ShardSpec(shard, 1, $"host=shard1a dbname={cluster}", "shard1a:17001",
            [new NodeSpec(shard, "shard1a", NodeState.Running)])],
        []);

    private static BackupsRuntimeOptions Options() => new(
        Enabled: true,
        S3Endpoint: "http://minio",
        S3Bucket: "bkt",
        S3AccessKey: "ak",
        S3SecretKey: "sk",
        JobImage: "pgworker-backup:test",
        RestoreRecoveryTimeoutSec: 1800);

    // Сид portalloc под конкретный тест-кластер + чистка чужих следов
    // (own-only: только префиксы этого кластера).
    private async Task SeedAsync(string cluster, string shard = "shard1",
        Dictionary<string, NodeAddress>? alloc = null)
    {
        var ct = TestContext.Current.CancellationToken;
        await fixture.Gateway.DeleteAsync(fixture.Endpoint, $"/pgworker/backups/{cluster}/", prefix: true, ct);
        await fixture.Gateway.DeleteAsync(fixture.Endpoint, $"/pgworker/claims/{cluster}", prefix: false, ct);
        await fixture.Gateway.DeleteAsync(fixture.Endpoint, $"/pgworker/work/{cluster}", prefix: false, ct);
        await fixture.Gateway.PutAsync(fixture.Endpoint, $"/pgworker/portalloc/{cluster}",
            Portalloc.Serialize(alloc ?? new Dictionary<string, NodeAddress>
            {
                [$"{shard}/shard1a"] = new("localhost", new NodePorts(16001, 18001, 17001)),
            }), null, ct);
    }

    private RestoreProcess BuildProcess(FakeBackupS3 s3, StubScaleDriver driver, TimeProvider? clock = null)
        => new(fixture.Gateway, [fixture.Endpoint], driver, s3, _claims,
            new WorkJournal(fixture.Gateway, [fixture.Endpoint]), Options(),
            new InstallSecrets("su-pw", "sb-pw", "adm-pw", "mov-pw"),
            new EtcdEndpoints([fixture.Endpoint]), new StubAppSecret(),
            new ShardProbe(new HttpClient()), new ThresholdsOptions(600, 1800),
            clock ?? TimeProvider.System);

    // Заявка restore: PUT PLANNED-ключа в etcd + тождественный объект в
    // backsupply-параметре тика (ReconcileLoop парсит префикс один раз).
    private async Task<RestoreOperationState> SeedRestoreAsync(
        string cluster, string shard, string id, string backupId = "", string source = "",
        string target = "latest")
    {
        var ct = TestContext.Current.CancellationToken;
        var op = new RestoreOperationState(id, RestoreStatus.Planned, backupId,
            source.Length == 0 ? $"{cluster}/{shard}" : source, target, "shard1a",
            TimeProvider.System.GetUtcNow().ToUnixTimeSeconds(), "operator");
        await fixture.Gateway.PutAsync(fixture.Endpoint,
            BackupNames.RestoreKey(cluster, shard, id), RestoreStatusJson.Serialize(op), null, ct);
        return op;
    }

    // Чтение статусов restore шарда из etcd (истина — ключ, не параметр тика).
    private async Task<IReadOnlyList<RestoreOperationState>> ReadRestoresAsync(
        string cluster, string shard)
    {
        var ct = TestContext.Current.CancellationToken;
        var range = await fixture.Gateway.RangeAsync(
            fixture.Endpoint, $"/pgworker/backups/{cluster}/{shard}/restore/", ct);
        var parsed = BackupsParser.Parse(range.Value, out var errors);
        errors.Should().BeEmpty();
        return parsed.Value.Count == 0
            ? (IReadOnlyList<RestoreOperationState>)[]
            : parsed.Value[0].Shards.TryGetValue(shard, out var sb)
                ? sb.Restores
                : (IReadOnlyList<RestoreOperationState>)[];
    }

    // Бэкапы-параметр тика: парс префикса etcd (ReconcileLoop парсит тот же
    // префикс один раз на кластер и передаёт процессам).
    private async Task<IReadOnlyList<ClusterBackups>> BackupsFromEtcdAsync(string cluster)
    {
        var ct = TestContext.Current.CancellationToken;
        var range = await fixture.Gateway.RangeAsync(
            fixture.Endpoint, $"/pgworker/backups/{cluster}/", ct);
        var parsed = BackupsParser.Parse(range.Value, out var errors);
        errors.Should().BeEmpty();
        return parsed.Value;
    }

    // COMPLETED-полный в etcd (wal_start → сегмент 000000010000000000000002).
    private async Task SeedFullAsync(string cluster, string shard, string id)
    {
        var ct = TestContext.Current.CancellationToken;
        await fixture.Gateway.PutAsync(fixture.Endpoint,
            BackupNames.FullKey(cluster, shard, id),
            """{"state":"COMPLETED","node":"shard1a","role":"replica","started_unix":1757500000,"finished_unix":1757500300,"wal_start_segment":"000000010000000000000002"}""",
            null, ct);
    }

    // Непрерывная цепочка от 000000010000000000000002 (сегменты 2–4).
    private static void SeedChain(FakeBackupS3 s3, string cluster, string shard,
        string fullId, bool withManifest = true, string[]? segments = null)
    {
        var prefix = $"{cluster}/{shard}";
        s3.Texts[$"{prefix}/full/{fullId}/backup_label"] =
            "START WAL LOCATION: 0/2000028 (file 000000010000000000000002)\n";
        if (withManifest)
            s3.Texts[$"{prefix}/full/{fullId}/backup_manifest"] = "{}";
        foreach (var seg in segments ?? ["000000010000000000000002", "000000010000000000000003", "000000010000000000000004"])
            s3.Objects.Add((cluster, shard, seg));
    }

    // Секрет-стаб: пер-кластерные креды «уже есть» (прецедент AdoptionContractTests).
    private sealed class StubAppSecret : IClusterSecretEnsurer
    {
        public Task<Result<ClusterCredentials>> EnsureAsync(
            string cluster, ClusterConfig config, CancellationToken ct)
            => Task.FromResult(Result<ClusterCredentials>.Success(new ClusterCredentials(
                new AppCredentials("app", "pw"), "moverpw000000000000000000000000A",
                new AppCredentials("bucket_admin", "bapw"),
                "backuppw00000000000000000000000A")));
    }

    // ── PLANNED: happy-path ──

    [Fact]
    public async Task Валидация_проходит_фиксирует_backup_id_и_started_unix_переходя_в_RUNNING()
    {
        // Arrange — свой COMPLETED-полный в etcd + непрерывная цепочка + манифест
        const string fullId = "20260910120000Z";
        await SeedAsync("c1");
        await SeedFullAsync("c1", "shard1", fullId);
        var s3 = new FakeBackupS3();
        SeedChain(s3, "c1", "shard1", fullId);
        var driver = new StubScaleDriver();
        var process = BuildProcess(s3, driver);
        var op = await SeedRestoreAsync("c1", "shard1", "20260911120000Z", backupId: "");
        (await _claims.TryClaimClusterAsync("c1", TestContext.Current.CancellationToken))
            .Value.Should().BeTrue("клэйм — предусловие тика");

        // Act
        var result = await process.TickAsync(
            BuildSnap(), await BackupsFromEtcdAsync("c1"), TestContext.Current.CancellationToken);

        // Assert — ключ в etcd: RUNNING, backup_id зафиксирован, started_unix
        result.IsSuccess.Should().BeTrue(result.Error?.ToString());
        var restores = await ReadRestoresAsync("c1", "shard1");
        var running = restores.Single(r => r.Id == op.Id);
        running.State.Should().Be(RestoreStatus.Running);
        running.BackupId.Should().Be(fullId);
        running.StartedUnix.Should().BeGreaterThan(0);
        (await fixture.Gateway.GetAsync(fixture.Endpoint, "/pgworker/work/c1",
            TestContext.Current.CancellationToken)).Value!.Value
            .Should().Contain($"validated/shard1/{op.Id}");
    }

    // ── PLANNED: permanent-отказы валидации ──

    [Fact]
    public async Task Валидация_полный_без_backup_manifest_permanent_FAILED()
    {
        // Arrange — префикс полного есть (etcd-статус + backup_label), манифеста
        // НЕТ: имитация упавшего на середине upload t02
        const string fullId = "20260910120000Z";
        await SeedAsync("c1");
        await SeedFullAsync("c1", "shard1", fullId);
        var s3 = new FakeBackupS3();
        SeedChain(s3, "c1", "shard1", fullId, withManifest: false);
        var process = BuildProcess(s3, new StubScaleDriver());
        var op = await SeedRestoreAsync("c1", "shard1", "20260911120001Z");
        (await _claims.TryClaimClusterAsync("c1", TestContext.Current.CancellationToken)).Value.Should().BeTrue();

        // Act
        (await process.TickAsync(BuildSnap(), await BackupsFromEtcdAsync("c1"),
            TestContext.Current.CancellationToken)).IsSuccess.Should().BeTrue();

        // Assert — permanent FAILED с причиной
        var failed = (await ReadRestoresAsync("c1", "shard1")).Single(r => r.Id == op.Id);
        failed.State.Should().Be(RestoreStatus.Failed);
        failed.Error.Should().Contain("без backup_manifest");
        failed.FinishedUnix.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task Валидация_дубль_заявки_младшие_permanent_FAILED_старейшая_исполняется()
    {
        // Arrange — два PLANNED одного шарда; валидные данные под старейшую
        const string fullId = "20260910120000Z";
        await SeedAsync("c1");
        await SeedFullAsync("c1", "shard1", fullId);
        var s3 = new FakeBackupS3();
        SeedChain(s3, "c1", "shard1", fullId);
        var process = BuildProcess(s3, new StubScaleDriver());
        var oldest = await SeedRestoreAsync("c1", "shard1", "20260911120000Z");
        var younger = await SeedRestoreAsync("c1", "shard1", "20260911120100Z");
        (await _claims.TryClaimClusterAsync("c1", TestContext.Current.CancellationToken)).Value.Should().BeTrue();

        // Act — один тик с обеими заявками (старейшая по Id исполняется)
        (await process.TickAsync(BuildSnap(),
                await BackupsFromEtcdAsync("c1"),
                TestContext.Current.CancellationToken)).IsSuccess.Should().BeTrue();

        // Assert — младший FAILED «дубль заявки», старейший дошёл до RUNNING
        var restores = await ReadRestoresAsync("c1", "shard1");
        restores.Single(r => r.Id == younger.Id).State.Should().Be(RestoreStatus.Failed);
        restores.Single(r => r.Id == younger.Id).Error.Should().Contain("дубль заявки");
        restores.Single(r => r.Id == oldest.Id).State.Should().Be(RestoreStatus.Running);
    }

    [Fact]
    public async Task Валидация_усыновлённый_шард_permanent_FAILED()
    {
        // Arrange — portalloc с object-нодами (усвоенный стендовый шард)
        await SeedAsync("c1", alloc: new Dictionary<string, NodeAddress>
        {
            ["shard1/shard1a"] = new("local", new NodePorts(15433, 18011, 0), Object: "as-shard1a"),
        });
        var s3 = new FakeBackupS3();
        var process = BuildProcess(s3, new StubScaleDriver());
        var op = await SeedRestoreAsync("c1", "shard1", "20260911120002Z");
        (await _claims.TryClaimClusterAsync("c1", TestContext.Current.CancellationToken)).Value.Should().BeTrue();

        // Act
        (await process.TickAsync(BuildSnap(), await BackupsFromEtcdAsync("c1"),
            TestContext.Current.CancellationToken)).IsSuccess.Should().BeTrue();

        // Assert
        var failed = (await ReadRestoresAsync("c1", "shard1")).Single(r => r.Id == op.Id);
        failed.State.Should().Be(RestoreStatus.Failed);
        failed.Error.Should().Contain("усыновлённых шардов");
    }

    [Fact]
    public async Task Валидация_дыра_цепочки_permanent_FAILED_с_границами()
    {
        // Arrange — сегмент 3 пропущен: 2, 4 (ожидается 000000010000000000000003)
        const string fullId = "20260910120000Z";
        await SeedAsync("c1");
        await SeedFullAsync("c1", "shard1", fullId);
        var s3 = new FakeBackupS3();
        SeedChain(s3, "c1", "shard1", fullId,
            segments: ["000000010000000000000002", "000000010000000000000004"]);
        var process = BuildProcess(s3, new StubScaleDriver());
        var op = await SeedRestoreAsync("c1", "shard1", "20260911120003Z");
        (await _claims.TryClaimClusterAsync("c1", TestContext.Current.CancellationToken)).Value.Should().BeTrue();

        // Act
        (await process.TickAsync(BuildSnap(), await BackupsFromEtcdAsync("c1"),
            TestContext.Current.CancellationToken)).IsSuccess.Should().BeTrue();

        // Assert — границы дыры в error
        var failed = (await ReadRestoresAsync("c1", "shard1")).Single(r => r.Id == op.Id);
        failed.State.Should().Be(RestoreStatus.Failed);
        failed.Error.Should().Contain("дыра WAL-цепочки: ожидался");
    }

    [Fact]
    public async Task Валидация_полных_нет_в_S3_permanent_FAILED()
    {
        // Arrange — DR-ветка: source-override на пустой префикс
        await SeedAsync("c1");
        var s3 = new FakeBackupS3(); // Fulls пусты
        var process = BuildProcess(s3, new StubScaleDriver());
        var op = await SeedRestoreAsync("c1", "shard1", "20260911120004Z", source: "c9/shard1");
        (await _claims.TryClaimClusterAsync("c1", TestContext.Current.CancellationToken)).Value.Should().BeTrue();

        // Act
        (await process.TickAsync(BuildSnap(), await BackupsFromEtcdAsync("c1"),
            TestContext.Current.CancellationToken)).IsSuccess.Should().BeTrue();

        // Assert
        var failed = (await ReadRestoresAsync("c1", "shard1")).Single(r => r.Id == op.Id);
        failed.State.Should().Be(RestoreStatus.Failed);
        failed.Error.Should().Contain("полные в c9/shard1 не найдены");
    }

    // ── PLANNED: transient (статус не меняем, тик Ok) ──

    [Fact]
    public async Task Валидация_S3_недоступен_статус_не_меняется()
    {
        // Arrange — list S3 падает (FailList), заявка через source-override
        await SeedAsync("c1");
        var s3 = new FakeBackupS3 { FailList = true };
        var process = BuildProcess(s3, new StubScaleDriver());
        var op = await SeedRestoreAsync("c1", "shard1", "20260911120005Z", source: "c9/shard1");
        (await _claims.TryClaimClusterAsync("c1", TestContext.Current.CancellationToken)).Value.Should().BeTrue();

        // Act
        var result = await process.TickAsync(BuildSnap(),
            await BackupsFromEtcdAsync("c1"), TestContext.Current.CancellationToken);

        // Assert — тик Ok (InProgress), заявка осталась PLANNED, журнал-факт
        result.IsSuccess.Should().BeTrue(result.Error?.ToString());
        result.Value.Should().Be(ProcessOutcome.InProgress);
        (await ReadRestoresAsync("c1", "shard1")).Single(r => r.Id == op.Id)
            .State.Should().Be(RestoreStatus.Planned);
        (await fixture.Gateway.GetAsync(fixture.Endpoint, "/pgworker/work/c1",
            TestContext.Current.CancellationToken)).Value!.Value.Should().Contain("s3-unavailable");
    }

    // ── Каркас тика ──

    [Fact]
    public async Task Тик_без_клэйма_и_без_активных_заявок_молчит()
    {
        // Arrange — клэйма нет, заявок нет
        await SeedAsync("c1");
        var process = BuildProcess(new FakeBackupS3(), new StubScaleDriver());

        // Act — тик без клэйма
        var refused = await process.TickAsync(BuildSnap(), [], TestContext.Current.CancellationToken);

        // Assert — отказ клэйм-гварда
        refused.IsSuccess.Should().BeFalse("мутации без клэйма запрещены");
        refused.Error!.Message.Should().Contain("клэйм не наш");

        // Act — клэйм есть, заявок нет
        (await _claims.TryClaimClusterAsync("c1", TestContext.Current.CancellationToken)).Value.Should().BeTrue();
        var done = await process.TickAsync(BuildSnap(), [], TestContext.Current.CancellationToken);

        // Assert — Done без мутаций
        done.IsSuccess.Should().BeTrue();
        done.Value.Should().Be(ProcessOutcome.Done);
    }

    [Fact]
    public async Task Тик_шард_убран_из_декларации_заявка_не_исполняется()
    {
        // Arrange — заявка на shard9, которого в снапшоте нет
        await SeedAsync("c1");
        var s3 = new FakeBackupS3();
        var process = BuildProcess(s3, new StubScaleDriver());
        var op = await SeedRestoreAsync("c1", "shard9", "20260911120006Z");
        (await _claims.TryClaimClusterAsync("c1", TestContext.Current.CancellationToken)).Value.Should().BeTrue();

        // Act
        var result = await process.TickAsync(BuildSnap(),
            await BackupsFromEtcdAsync("c1"), TestContext.Current.CancellationToken);

        // Assert — Done, статус не изменился (закроет remove-shard ветка)
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be(ProcessOutcome.Done);
        (await ReadRestoresAsync("c1", "shard9")).Single(r => r.Id == op.Id)
            .State.Should().Be(RestoreStatus.Planned);
    }
}
