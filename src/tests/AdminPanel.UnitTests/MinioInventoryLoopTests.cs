using AdminPanel.Core;
using AdminPanel.Probes.S3;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.UnitTests;

// Инвентарь-тик MinIO (t08): успех заполняет стор, сбой копит ConsecutiveFailures
// с живым прежним инвентарём, выключенная грань — no-op, маркер configured до
// первого тика (Решение 4).
public class MinioInventoryLoopTests
{
    private static MinioOptions Configured() => new()
    {
        S3 =
        {
            Endpoint = "http://minio:9000",
            Bucket = "pgworker-backups",
            AccessKey = "minioadmin",
            SecretKey = "minioadmin",
        },
    };

    private static MinioInventoryLoop Loop(StubS3 client, IMinioInventoryStore store, MinioOptions options) =>
        new(client, store, Options.Create(options), new FixedTimeProvider(),
            NullLogger<MinioInventoryLoop>.Instance);

    // AAA: успешный тик → стор заполнен (Configured, ApiOk, дерево, failures=0)
    [Fact]
    public async Task RunOnce_Success_PopulatesStore()
    {
        // Arrange — стаб отдаёт buckets + страницу объектов demo/s1
        var client = new StubS3
        {
            Objects = [new MinioObjectInfo("demo/s1/full/b1/base.tar", 100, 1000)],
        };
        var store = new MinioInventoryStore();

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
            Objects = [new MinioObjectInfo("demo/s1/full/b1/base.tar", 100, 1000)],
        };
        var store = new MinioInventoryStore();
        var loop = Loop(client, store, Configured());
        await loop.RunOnceAsync(CancellationToken.None);
        var previous = store.Current!;
        client.Throw = new ApplicationException("minio down");

        // Act — два сбойных тика подряд
        await loop.RunOnceAsync(CancellationToken.None);
        await loop.RunOnceAsync(CancellationToken.None);

        // Assert — данные и штамп прежние, счётчик 2, ошибка заполнена
        store.Current!.UsedBytes.Should().Be(previous.UsedBytes);
        store.Current.UpdatedAtUnix.Should().Be(previous.UpdatedAtUnix);
        store.Current.ConsecutiveFailures.Should().Be(2);
        store.Current.LastError.Should().Contain("minio down");
    }

    // AAA: после сбоев первый успешный тик сбрасывает счётчик в 0
    [Fact]
    public async Task RunOnce_Success_ResetsFailures()
    {
        // Arrange — один успех, один сбой
        var client = new StubS3 { Objects = [] };
        var store = new MinioInventoryStore();
        var loop = Loop(client, store, Configured());
        await loop.RunOnceAsync(CancellationToken.None);
        client.Throw = new ApplicationException("down");
        await loop.RunOnceAsync(CancellationToken.None);
        store.Current!.ConsecutiveFailures.Should().Be(1);

        // Act — MinIO ожил
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
        var store = new MinioInventoryStore();

        // Act
        await Loop(client, store, new MinioOptions()).RunOnceAsync(CancellationToken.None);

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
        var store = new MinioInventoryStore();

        // Act
        await Loop(client, store, Configured()).RunOnceAsync(CancellationToken.None);

        // Assert — маркер «настроено, инвентарь ещё не собран» + учтён сбой
        store.Current!.Configured.Should().BeTrue();
        store.Current.UpdatedAtUnix.Should().Be(0);
        store.Current.ConsecutiveFailures.Should().Be(1);
    }

    // Стаб IMinioS3: счётчик вызовов, управляемый сбой, одна страница объектов.
    private sealed class StubS3 : IMinioS3
    {
        public int Calls;
        public Exception? Throw;
        public List<MinioObjectInfo> Objects { get; init; } = [];

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

        public Task<MinioHealth> GetHealthAsync(CancellationToken ct)
        {
            Calls++;
            if (Throw is not null)
                throw Throw;
            return Task.FromResult(new MinioHealth(true, null, true, true, null));
        }
    }
}
