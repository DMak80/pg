using PgWorker.Backups;
using PgWorker.Etcd.Parsing;
using PgWorker.Etcd.S3;
using Shared.Core;

namespace PgWorker.UnitTests.Backups;

// BackupPoolResolver (t36, arch/19 §4): policy.s3 + пул (Secrets) →
// контекст списков; null — «пул не выбран», Failed — transient-диагностика.
public class BackupPoolResolverTests
{
    // Пул из двух нод с адресами (east1/east2) — base Arrange-набор
    // (withSecrets=false — негативный кейс «пул без кредов»).
    private static S3Pool TwoNodePool(bool toRemove = false, bool withSecrets = true) => new(
        "backups", toRemove,
        [
            new S3PoolNode("east1", "RUNNING", "h1", "h1", 18001),
            new S3PoolNode("east2", "RUNNING", "h2", "h2", 18002),
        ],
        ["h1:18001", "h2:18002"],
        Secrets: withSecrets ? new("ak", "sk") : null);

    private static BackupPoolResolver Resolver(
        S3Pool? pool, BackupPolicy? policy)
        => new(
            new FakePoolStore(pool),
            policyReader: (_, _) => Task.FromResult(policy));

    private static BackupPolicy Policy(BackupPolicyS3? s3) => new(
        1, 1, 1, 86400, true, S3: s3);

    // AAA: полный резолв — списки по роли, ReadAll в порядке endpoints.
    [Fact]
    public async Task Resolve_FullPolicy_ListsByRole()
    {
        // Arrange — пул 2 ноды, policy с write_master=[east1,east2], write_replica=[east2].
        var resolver = Resolver(TwoNodePool(), Policy(
            new BackupPolicyS3("backups", ["east1", "east2"], ["east2"])));

        // Act
        var result = await resolver.ResolveAsync("demo", CancellationToken.None);

        // Assert — креды пула, адреса резолвлены, ReadAll обе ноды.
        result.IsSuccess.Should().BeTrue();
        var ctx = result.Value!;
        ctx.Pool.Should().Be("backups");
        ctx.AccessKey.Should().Be("ak");
        ctx.SecretKey.Should().Be("sk");
        ctx.WriteMaster.Should().Equal("http://h1:18001", "http://h2:18002");
        ctx.WriteReplica.Should().Equal("http://h2:18002");
        ctx.ReadAll.Should().Equal("http://h1:18001", "http://h2:18002");
    }

    // AAA: policy без s3 → null (не Failed — кластер без выбора, «молчание»).
    [Fact]
    public async Task Resolve_PolicyWithoutS3_ReturnsNull()
    {
        // Arrange — policy без секции.
        var resolver = Resolver(TwoNodePool(), Policy(null));

        // Act
        var result = await resolver.ResolveAsync("demo", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().BeNull();
    }

    // AAA: пул не найден → Failed с именем пула в диагностике.
    [Fact]
    public async Task Resolve_PoolMissing_Failed()
    {
        // Arrange — store вернёт null (config отсутствует).
        var resolver = Resolver(null, Policy(new BackupPolicyS3("ghost", ["n1"], ["n1"])));

        // Act
        var result = await resolver.ResolveAsync("demo", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error!.Message.Should().Contain("ghost");
    }

    // AAA: пул в TO_REMOVE → Failed.
    [Fact]
    public async Task Resolve_PoolToRemove_Failed()
    {
        // Arrange
        var resolver = Resolver(TwoNodePool(toRemove: true), Policy(
            new BackupPolicyS3("backups", ["east1"], ["east1"])));

        // Act
        var result = await resolver.ResolveAsync("demo", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error!.Message.Should().Contain("TO_REMOVE");
    }

    // AAA: пул без Secrets → Failed с «no readable secrets» (transient —
    // диагностика без значений кредов).
    [Fact]
    public async Task Resolve_PoolWithoutSecrets_Failed()
    {
        // Arrange — пул есть, кредов пула нет.
        var resolver = Resolver(TwoNodePool(withSecrets: false), Policy(
            new BackupPolicyS3("backups", ["east1"], ["east1"])));

        // Act
        var result = await resolver.ResolveAsync("demo", CancellationToken.None);

        // Assert — значения не утекают.
        result.IsSuccess.Should().BeFalse();
        result.Error!.Message.Should().Contain("no readable secrets");
        result.Error!.Message.Should().NotContain("ak").And.NotContain("sk");
    }

    // AAA: пустой write-список при заданной секции → transient-диагностика
    // («не выбрано» — это отсутствие секции целиком).
    [Fact]
    public async Task Resolve_EmptyWriteList_Failed()
    {
        // Arrange
        var resolver = Resolver(TwoNodePool(), Policy(
            new BackupPolicyS3("backups", [], ["east1"])));

        // Act
        var result = await resolver.ResolveAsync("demo", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error!.Message.Should().Contain("write_master");
    }

    // AAA: имя не в пуле (нет nodes/<I>/placement) → «нода не в пуле» —
    // transient, оператор правит policy.
    [Fact]
    public async Task Resolve_UnknownNodeName_Failed()
    {
        // Arrange
        var resolver = Resolver(TwoNodePool(), Policy(
            new BackupPolicyS3("backups", ["ghost1"], ["east1"])));

        // Act
        var result = await resolver.ResolveAsync("demo", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error!.Message.Should().Contain("ghost1").And.Contain("not in pool");
    }

    // AAA: нода в Nodes, но portalloc-записи нет (Host=null) → Failed «пул
    // поднимается» — ретраи тиком.
    [Fact]
    public async Task Resolve_NodeWithoutAddress_TransientFailed()
    {
        // Arrange — east2 заявлена, адреса нет.
        var pool = new S3Pool("backups", false,
        [
            new S3PoolNode("east1", "RUNNING", "h1", "h1", 18001),
            new S3PoolNode("east2", "PROVISIONING", "h2", null, null),
        ], ["h1:18001"], Secrets: new("ak", "sk"));
        var resolver = Resolver(pool, Policy(
            new BackupPolicyS3("backups", ["east2"], ["east1"])));

        // Act
        var result = await resolver.ResolveAsync("demo", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error!.Message.Should().Contain("east2").And.Contain("pool is starting up");
    }

    // Фейк store: заранее собранный пул (или null — «пула нет»).
    private sealed class FakePoolStore(S3Pool? pool) : IS3PoolStore
    {
        public Task<Result<S3Pool?>> ReadAsync(string name, CancellationToken ct)
            => Task.FromResult(Result<S3Pool?>.Success(pool));

        public Task<Result<IReadOnlyList<string>>> ListPoolNamesAsync(CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<string>>.Success(pool is null ? [] : [pool.Name]));
    }
}
