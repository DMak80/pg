using AdminPanel.Core;
using AdminPanel.Probes.S3;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.UnitTests;

// Инвентарь-S3-инвентарь-тик (t08): успех заполняет стор, сбой копит ConsecutiveFailures
// с живым прежним инвентарём, выключенная грань — no-op, маркер configured до
// первого тика (Решение 4).
public class S3InventoryLoopTests
{
    private static BackupsS3Options Configured() => new()
    {
        S3 =
        {
            Endpoint = "http://s3:9000",
            Bucket = "pgworker-backups",
            AccessKey = "apm-owns3-root",
            SecretKey = "apm-owns3-root-pwd",
        },
    };

    private static S3InventoryLoop Loop(StubS3 client, IS3InventoryStore store, BackupsS3Options options) =>
        new(client, store, Options.Create(options), new FixedTimeProvider(),
            NullLogger<S3InventoryLoop>.Instance);

    // AAA: успешный тик → стор заполнен (Configured, ApiOk, дерево, failures=0)
    [Fact]
    public async Task RunOnce_Success_PopulatesStore()
    {
        // Arrange — стаб отдаёт buckets + страницу объектов demo/s1
        var client = new StubS3
        {
            Objects = [new S3ObjectInfo("demo/s1/full/b1/base.tar", 100, 1000)],
        };
        var store = new S3InventoryStore();

        // Act
        await Loop(client, store, Configured()).RunOnceAsync(CancellationToken.None);

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
        var loop = Loop(client, store, Configured());
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
        var loop = Loop(client, store, Configured());
        await loop.RunOnceAsync(CancellationToken.None);
        client.Throw = new ApplicationException("down");
        await loop.RunOnceAsync(CancellationToken.None);
        store.Current!.ConsecutiveFailures.Should().Be(1);

        // Act — ownS3-нода ожил
        client.Throw = null;
        await loop.RunOnceAsync(CancellationToken.None);

        // Assert
        store.Current!.ConsecutiveFailures.Should().Be(0);
        store.Current.LastError.Should().BeNull();
    }

    // AAA: пустой Endpoint — RunOnce не зовёт клиент, стор остаётся null (AC1)
    [Fact]
    public async Task RunOnce_NotConfigured_Noop()
    {
        // Arrange — грань выключена
        var client = new StubS3();
        var store = new S3InventoryStore();

        // Act
        await Loop(client, store, new BackupsS3Options()).RunOnceAsync(CancellationToken.None);

        // Assert
        client.Calls.Should().Be(0);
        store.Current.Should().BeNull();
    }

    // AAA: свежий стор + конфигурация задана + клиент бросает → маркер
    // Configured=true, UpdatedAtUnix=0, failures=1 (Решение 4)
    [Fact]
    public async Task RunOnce_WritesConfiguredMarker_BeforeFirstInventory()
    {
        // Arrange — стаб падает с первого вызова
        var client = new StubS3 { Throw = new ApplicationException("boom") };
        var store = new S3InventoryStore();

        // Act
        await Loop(client, store, Configured()).RunOnceAsync(CancellationToken.None);

        // Assert — маркер «настроено, инвентарь ещё не собран» + учтён сбой
        store.Current!.Configured.Should().BeTrue();
        store.Current.UpdatedAtUnix.Should().Be(0);
        store.Current.ConsecutiveFailures.Should().Be(1);
    }

    // Стаб IS3StorageClient: счётчик вызовов, управляемый сбой, одна страница объектов.
    private sealed class StubS3 : IS3StorageClient
    {
        public int Calls;
        public Exception? Throw;
        public List<S3ObjectInfo> Objects { get; init; } = [];

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
