using AdminPanel.Core;
using AdminPanel.Core.S3;
using AdminPanel.Etcd;
using AdminPanel.Probes.S3;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.UnitTests;

// Инвентарь-S3-тик: цели инвентаря резолвятся из снапшота S3 перед тиком
// (S3TargetsResolver → client.UpdateTargets). Успех заполняет стор, сбой копит
// ConsecutiveFailures с живым прежним инвентарём, целей нет — no-op без
// configured-маркера, маркер configured до первого тика (Решение 4).
public class S3InventoryLoopTests
{
    private static readonly DateTimeOffset Now = new(2026, 10, 15, 12, 0, 0, TimeSpan.Zero);

    private static S3Snapshot Snapshot(params S3ClusterInfo[] clusters) => new(
        Now, EtcdReachable: true, ConsecutiveFailures: 0,
        clusters, [], [], [], [], [], 0);

    // Active-кластер с endpoints и panel-ключом — валидный источник целей
    // (креды уникальны имени — база ассертов).
    private static S3ClusterInfo BackupCluster(string name = "demo") => new(
        name, S3ClusterState.Active, "owns3", 2, false, 1760156800,
        "s3-a:9000,s3-b:9000", [], [], [],
        Secrets: new S3ClusterSecretsInfo("root",
            [new S3AccessKeyInfo("panel", "ak-" + name, "sk-" + name, "read-only")]));

    // Options без кредов и endpoints: цели — из снапшота, не из конфига.
    private static S3InventoryLoop Loop(StubS3 client, IS3InventoryStore store, S3Snapshot? snapshot) =>
        new(client, store,
            Options.Create(new BackupsS3Options { S3 = { Bucket = "pgworker-backups" } }),
            new StubReader(snapshot), new FixedTimeProvider(),
            NullLogger<S3InventoryLoop>.Instance);

    // AAA: успешный тик → стор заполнен (Configured, ApiOk, дерево, failures=0);
    // перед тиком клиенту поданы цели резолвера (Active-кластер снапшота).
    [Fact]
    public async Task RunOnce_Success_PopulatesStore()
    {
        // Arrange — снапшот с Active-кластером; стаб отдаёт buckets + страницу объектов demo/s1
        var client = new StubS3
        {
            Objects = [new S3ObjectInfo("demo/s1/full/b1/base.tar", 100, 1000)],
        };
        var store = new S3InventoryStore();

        // Act
        await Loop(client, store, Snapshot(BackupCluster())).RunOnceAsync(CancellationToken.None);

        // Assert
        var current = store.Current;
        current.Should().NotBeNull();
        current!.Configured.Should().BeTrue();
        current.Health.Should().NotBeNull();
        current.Health!.ApiOk.Should().BeTrue();
        current.Health.LiveOk.Should().BeTrue();
        current.Buckets.Should().Contain("pgworker-backups");
        current.Clusters.Should().ContainSingle().Which.Cluster.Should().Be("demo");
        current.UsedBytes.Should().Be(100);
        current.ConsecutiveFailures.Should().Be(0);
        current.UpdatedAtUnix.Should().BeGreaterThan(0);
        current.LastError.Should().BeNull();

        // Цели поданы клиенту до тика — резолвер отдал Active-кластер снапшота.
        client.UpdatedTargets.Should().NotBeNull();
        client.UpdatedTargets!.Cluster.Should().Be("demo");
        client.UpdatedTargets.Endpoints.Should().Equal("http://s3-a:9000", "http://s3-b:9000");
        client.UpdatedTargets.AccessKey.Should().Be("ak-demo");
        client.UpdatedTargets.SecretKey.Should().Be("sk-demo");
    }

    // AAA: сбой после успеха → прежний инвентарь живёт, failures растут (1 → 2)
    [Fact]
    public async Task RunOnce_Failure_KeepsPreviousAndCounts()
    {
        // Arrange — первый успешный тик, затем стаб бросает
        var client = new StubS3
        {
            Objects = [new S3ObjectInfo("demo/s1/full/b1/base.tar", 100, 1000)],
        };
        var store = new S3InventoryStore();
        var loop = Loop(client, store, Snapshot(BackupCluster()));
        await loop.RunOnceAsync(CancellationToken.None);
        var previous = store.Current!;
        client.Throw = new ApplicationException("s3 down");

        // Act — два сбойных тика подряд
        await loop.RunOnceAsync(CancellationToken.None);
        await loop.RunOnceAsync(CancellationToken.None);

        // Assert — данные и штамп прежние, счётчик 2, ошибка заполнена
        store.Current!.UsedBytes.Should().Be(previous.UsedBytes);
        store.Current.UpdatedAtUnix.Should().Be(previous.UpdatedAtUnix);
        store.Current.ConsecutiveFailures.Should().Be(2);
        store.Current.LastError.Should().Contain("s3 down");
    }

    // AAA: после сбоев первый успешный тик сбрасывает счётчик в 0
    [Fact]
    public async Task RunOnce_Success_ResetsFailures()
    {
        // Arrange — один успех, один сбой
        var client = new StubS3 { Objects = [] };
        var store = new S3InventoryStore();
        var loop = Loop(client, store, Snapshot(BackupCluster()));
        await loop.RunOnceAsync(CancellationToken.None);
        client.Throw = new ApplicationException("down");
        await loop.RunOnceAsync(CancellationToken.None);
        store.Current!.ConsecutiveFailures.Should().Be(1);

        // Act — хранилище ожило
        client.Throw = null;
        await loop.RunOnceAsync(CancellationToken.None);

        // Assert
        store.Current!.ConsecutiveFailures.Should().Be(0);
        store.Current.LastError.Should().BeNull();
    }

    // AAA: резолвер не отдал целей (единственный кластер пула TO_REMOVE) — тик
    // no-op: клиент не зовётся, стор без configured-маркера (остаётся null).
    [Fact]
    public async Task RunOnce_NoTargets_ClientNotCalled_NoStoreMarker()
    {
        // Arrange — снапшот без подходящего кластера
        var client = new StubS3();
        var store = new S3InventoryStore();

        // Act
        await Loop(client, store,
                Snapshot(BackupCluster() with { State = S3ClusterState.ToRemove }))
            .RunOnceAsync(CancellationToken.None);

        // Assert
        client.Calls.Should().Be(0);
        store.Current.Should().BeNull();
    }

    // AAA: цели есть + клиент бросает → маркер Configured=true, UpdatedAtUnix=0,
    // failures=1 (Решение 4)
    [Fact]
    public async Task RunOnce_WritesConfiguredMarker_BeforeFirstInventory()
    {
        // Arrange — стаб падает с первого вызова
        var client = new StubS3 { Throw = new ApplicationException("boom") };
        var store = new S3InventoryStore();

        // Act
        await Loop(client, store, Snapshot(BackupCluster())).RunOnceAsync(CancellationToken.None);

        // Assert — маркер «настроено, инвентарь ещё не собран» + учтён сбой
        store.Current!.Configured.Should().BeTrue();
        store.Current.UpdatedAtUnix.Should().Be(0);
        store.Current.ConsecutiveFailures.Should().Be(1);
    }

    // Стаб читателя снапшота S3 (порт S3QueryHandlerTests.StubReader).
    private sealed class StubReader(S3Snapshot? snapshot) : IS3SnapshotReader
    {
        public S3Snapshot? Current { get; } = snapshot;
    }

    // Стаб IS3StorageClient: счётчик list/health-вызовов, цели, поданные
    // UpdateTargets'ом, управляемый сбой, одна страница объектов.
    private sealed class StubS3 : IS3StorageClient
    {
        public int Calls;
        public Exception? Throw;
        public S3StorageTargets? UpdatedTargets { get; private set; }
        public List<S3ObjectInfo> Objects { get; init; } = [];

        public void UpdateTargets(S3StorageTargets? targets) => UpdatedTargets = targets;

        public Task<Result<IReadOnlyList<string>>> ListBucketsAsync(CancellationToken ct)
        {
            Calls++;
            if (Throw is not null)
                throw Throw;
            return Task.FromResult(Result<IReadOnlyList<string>>.Success(["pgworker-backups"]));
        }

        public Task<Result<S3Page>> ListPageAsync(
            string? prefix, string? continuationToken, int maxKeys, CancellationToken ct)
        {
            Calls++;
            if (Throw is not null)
                throw Throw;
            return Task.FromResult(Result<S3Page>.Success(new S3Page(Objects, null)));
        }

        public Task<S3Health> GetHealthAsync(CancellationToken ct)
        {
            Calls++;
            if (Throw is not null)
                throw Throw;
            return Task.FromResult(new S3Health(true, null, true, true, null));
        }
    }
}
