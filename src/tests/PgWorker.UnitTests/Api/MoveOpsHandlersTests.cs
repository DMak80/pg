using PgWorker.App.Api.Operations;
using PgWorker.Moves;
using Xunit;

namespace PgWorker.UnitTests.Api;

// Guard-логика move-ops handler'ов на моках gateway (t07, спека §7): тексты
// и ветки 409/404 без docker; авторитетные перепроверки — у процесса (t01).
public class MoveOpsHandlersTests
{
    private const string Ep = "http://etcd";

    private static async Task<FakeEtcdGateway> SeedClusterAsync(string name, int buckets = 4, int shards = 2)
    {
        var gw = new FakeEtcdGateway();
        await gw.PutAsync(Ep, $"/clusters/{name}/config",
            $$"""{"buckets":{{buckets}},"dbname":"{{name}}","created_unix":1756000000}""", null, CancellationToken.None);
        for (var s = 1; s <= shards; s++)
        {
            await gw.PutAsync(Ep, $"/clusters/{name}/shards/shard{s}/replicas", "1", null, CancellationToken.None);
            for (var i = 0; i < buckets; i++)
                if (i % shards == s - 1)
                    await gw.PutAsync(Ep, $"/clusters/{name}/buckets/routing/bucket_{i}", $"shard{s}", null, CancellationToken.None);
        }
        return gw;
    }

    [Fact]
    public async Task Rollback_NotActiveBucket_409WithStateText()
    {
        // Arrange — SYNCING-статус на bucket_0 (возраст не важен для rollback)
        var gw = await SeedClusterAsync("c");
        await gw.PutAsync(Ep, "/clusters/c/buckets/status/bucket_0",
            """{"state":"FROZEN","owner":"shard1","target":"shard2","updated_unix":100}""", null, CancellationToken.None);
        var handler = new RollbackBucketsHandler(gw, [Ep], TimeProvider.System);

        // Act
        var result = await handler.HandleAsync("c",
            new RollbackBucketsRequest([0]), "ops", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<BucketNotActiveForMoveOpException>()
            .Which.Message.Should().Contain("только из ACTIVE").And.Contain("FROZEN");
    }

    [Fact]
    public async Task Finalize_TargetIsOwner_409()
    {
        // Arrange — bucket_0 принадлежит shard1
        var gw = await SeedClusterAsync("c");
        var handler = new FinalizeBucketHandler(gw, [Ep], TimeProvider.System);

        // Act
        var result = await handler.HandleAsync("c",
            new FinalizeBucketRequest(0, "shard1"), "ops", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<FinalizeTargetIsOwnerException>();
    }

    [Fact]
    public async Task Abort_FreshStatus_409WithThreshold()
    {
        // Arrange — updated_unix = now-10 (< AbortMinAgeSec=120 дефолта)
        var gw = await SeedClusterAsync("c");
        await gw.PutAsync(Ep, "/clusters/c/buckets/status/bucket_0",
            $$"""{"state":"SYNCING","owner":"shard1","target":"shard2","updated_unix":{{DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 10}}}""", null, CancellationToken.None);
        var handler = new AbortBucketHandler(gw, [Ep], TimeProvider.System, new MovesRuntimeOptions());

        // Act
        var result = await handler.HandleAsync("c", new AbortBucketRequest(0, null), "ops", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<MoveStatusFreshException>()
            .Which.Message.Should().Contain("AbortMinAgeSec=120");
    }

    [Fact]
    public async Task Abort_NoUpdatedUnix_FreshnessSkipped_201()
    {
        // Arrange — старый формат ключа без updated_unix: пред-проверка
        // пропускается (спека §5.3), заявка ставится
        var gw = await SeedClusterAsync("c");
        await gw.PutAsync(Ep, "/clusters/c/buckets/status/bucket_0",
            """{"state":"ABORTING","owner":"shard1","target":"shard2"}""", null, CancellationToken.None);
        var handler = new AbortBucketHandler(gw, [Ep], TimeProvider.System, new MovesRuntimeOptions());

        // Act
        var result = await handler.HandleAsync("c", new AbortBucketRequest(0, null), "ops", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        gw.Store["/pgworker/moves/c/bucket_0"].Should().Contain("\"op\":\"abort\"");
    }

    [Fact]
    public async Task Abort_RoutingEqualsTarget_409()
    {
        // Arrange — target == routing.owner (bucket_0 на shard1)
        var gw = await SeedClusterAsync("c");
        await gw.PutAsync(Ep, "/clusters/c/buckets/status/bucket_0",
            """{"state":"SYNCING","owner":"shard1","target":"shard1","updated_unix":100}""", null, CancellationToken.None);
        var handler = new AbortBucketHandler(gw, [Ep], TimeProvider.System, new MovesRuntimeOptions());

        // Act
        var result = await handler.HandleAsync("c", new AbortBucketRequest(0, null), "ops", CancellationToken.None);

        // Assert
        result.Error.Should().BeOfType<MoveAlreadyFlippedException>();
    }

    [Fact]
    public async Task Cancel_MissingTicket_404()
    {
        // Arrange
        var gw = await SeedClusterAsync("c");
        var handler = new CancelMoveHandler(gw, [Ep]);

        // Act
        var result = await handler.HandleAsync("c", "bucket_0", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<MoveTicketNotFoundException>();
    }

    [Fact]
    public async Task Cancel_LiveTicket_Deleted()
    {
        // Arrange
        var gw = await SeedClusterAsync("c");
        await gw.PutAsync(Ep, "/pgworker/moves/c/bucket_0",
            """{"op":"move","to":"shard2","requested_unix":100}""", null, CancellationToken.None);
        var handler = new CancelMoveHandler(gw, [Ep]);

        // Act
        var result = await handler.HandleAsync("c", "bucket_0", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        gw.Store.Should().NotContainKey("/pgworker/moves/c/bucket_0");
    }

    // ===== валидаторы тел (спека §7): 400-ветки без etcd =====

    [Fact]
    public void RollbackValidator_EmptyAndDuplicates_Errors()
    {
        // Arrange / Act / Assert — пустой массив и дубликаты ловит валидатор
        RollbackBucketsValidator.Validate(new RollbackBucketsRequest(null))
            .Should().ContainSingle(e => e.Field == "buckets");
        RollbackBucketsValidator.Validate(new RollbackBucketsRequest([]))
            .Should().ContainSingle(e => e.Field == "buckets");
        RollbackBucketsValidator.Validate(new RollbackBucketsRequest([0, 0]))
            .Should().ContainSingle(e => e.Message.Contains("дубликаты"));
        RollbackBucketsValidator.Validate(new RollbackBucketsRequest([0, 1]))
            .Should().BeEmpty();
    }

    [Fact]
    public void FinalizeValidator_MissingBucketOrShard_Errors()
    {
        // Arrange / Act / Assert
        FinalizeBucketValidator.Validate(new FinalizeBucketRequest(null, "s2"))
            .Should().ContainSingle(e => e.Field == "bucket");
        FinalizeBucketValidator.Validate(new FinalizeBucketRequest(0, null))
            .Should().ContainSingle(e => e.Field == "oldShard");
        FinalizeBucketValidator.Validate(new FinalizeBucketRequest(0, "s2"))
            .Should().BeEmpty();
    }

    [Fact]
    public void AbortValidator_MissingBucket_Error()
    {
        // Arrange / Act / Assert — force nullable: false не мешает
        AbortBucketValidator.Validate(new AbortBucketRequest(null, null))
            .Should().ContainSingle(e => e.Field == "bucket");
        AbortBucketValidator.Validate(new AbortBucketRequest(0, false))
            .Should().BeEmpty();
    }
}
