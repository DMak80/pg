using FluentAssertions;
using PgWorker.Backups;
using PgWorker.Etcd.Parsing;
using PgWorker.Etcd.S3;
using Shared.Core;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Кэш пул-клиентов фабрики (arch/19 §5 «Кэш пул-клиентов»): клиент создаётся
// ОДИН РАЗ на контекст пула — circuit-breaker-стейт отказных целей переживает
// тики (per-тик клиент платил полный S3-таймаут мёртвой ноды в каждом тике —
// тик воркера растягивался 5 c → ~35 c). Смена любого входа контекста
// (endpoints/креды/bucket/CA) — новый клиент. AAA-комментарии.
public class BackupS3FactoryCacheTests
{
    private static BackupPoolContext Ctx(
        string read = "h1:9000", string ak = "ak", string bucket = "bkt")
        => new("backups", ak, "sk",
            [read], [read], [read]);

    private static BackupS3Factory Factory(Func<string>? bucket = null, Func<string?>? ca = null)
        => new(
            new BackupPoolResolver(new FixedPoolStore(null),
                policyReader: (_, _) => Task.FromResult<BackupPolicy?>(null)),
            new FixedPoolStore(null),
            bucket ?? (() => "bkt"), ca ?? (() => null),
            TimeSpan.FromSeconds(5),
            targetFactory: _ => throw new NotSupportedException("no S3 calls in the cache test"));

    private sealed class FixedPoolStore(S3Pool? pool) : IS3PoolStore
    {
        public Task<Result<S3Pool?>> ReadAsync(string name, CancellationToken ct)
            => Task.FromResult(Result<S3Pool?>.Success(pool));

        public Task<Result<IReadOnlyList<string>>> ListPoolNamesAsync(CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<string>>.Success([]));
    }

    [Fact]
    public void SameContext_ReturnsSameInstance()
    {
        // Arrange: фабрика + контекст пула.
        var factory = Factory();

        // Act: два Create на идентичных контекстах.
        var first = factory.Create(Ctx());
        var second = factory.Create(Ctx());

        // Assert: один инстанс — breaker-стейт переживает вызовы/тики.
        ReferenceEquals(first, second).Should().BeTrue();
    }

    [Fact]
    public void ChangedEndpoints_NewInstance()
    {
        // Arrange: фабрика + контекст.
        var factory = Factory();
        var first = factory.Create(Ctx(read: "h1:9000"));

        // Act: смена списка нод пула (пересборка portalloc).
        var second = factory.Create(Ctx(read: "h2:9000"));

        // Assert: новый клиент на новом контексте; прежний не тронут.
        ReferenceEquals(first, second).Should().BeFalse();
    }

    [Fact]
    public void ChangedCredentialsOrBucketOrCa_NewInstance()
    {
        // Arrange.
        var factory = Factory();
        var baseClient = factory.Create(Ctx());

        // Act/Assert: смена креды — новый клиент.
        factory.Create(Ctx(ak: "ak2")).Should().NotBeSameAs(baseClient);

        // Act/Assert: смена bucket — новый клиент.
        var otherBucket = Factory(bucket: () => "bkt2");
        otherBucket.Create(Ctx()).Should().NotBeSameAs(baseClient);

        // Act/Assert: смена CA — новый клиент.
        var otherCa = Factory(ca: () => "/tmp/ca.pem");
        otherCa.Create(Ctx()).Should().NotBeSameAs(baseClient);
    }
}
