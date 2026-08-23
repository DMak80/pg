using PgWorker.Core.Model;
using PgWorker.Core.Planning;

namespace PgWorker.UnitTests.Planning;

// EvacuationPlanner: план аварийной эвакуации бакетов умершего шарда
// (spec §6.4 D, Д6): поровну round-robin по живым шардам, guard'ы на
// незавершённые переезды и пустой список живых.

public class EvacuationPlannerTests
{
    private static BucketRoute Bucket(int id, string? owner, BucketMoveState? status = null) =>
        new(id, owner, status);

    [Fact]
    public void Plan_DeadShardBuckets_SpreadRoundRobinEvenly()
    {
        // Arrange: 4 бакета умершего shard2, живые — shard1 и shard3.
        var routing = new List<BucketRoute>
        {
            Bucket(0, "shard2"), Bucket(1, "shard2"),
            Bucket(2, "shard2"), Bucket(3, "shard2"),
        };
        var alive = new List<string> { "shard3", "shard1" };

        // Act: строим план эвакуации.
        var result = EvacuationPlanner.Plan(routing, "shard2", alive);

        // Assert: поровну round-robin по возрастанию id; целевые шарды детерминированы.
        result.IsSuccess.Should().BeTrue();
        var plan = result.Value;
        plan.Should().HaveCount(4);
        plan.Should().OnlyContain(a => a.FromShard == "shard2");
        plan.Select(a => a.ToShard).Should().Equal("shard1", "shard3", "shard1", "shard3");
        plan.Select(a => a.BucketId).Should().BeInAscendingOrder();
    }

    [Fact]
    public void Plan_MovingBucket_BlocksEvacuation()
    {
        // Arrange: один из бакетов кластера в статусе FROZEN — незавершённый переезд.
        var routing = new List<BucketRoute>
        {
            Bucket(0, "shard2"), Bucket(1, "shard1", BucketMoveState.Frozen),
        };

        // Act: пытаемся строить план эвакуации shard2.
        var result = EvacuationPlanner.Plan(routing, "shard2", ["shard1"]);

        // Assert: эвакуация заблокирована (guard из spec §6.4 D).
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().NotBeNull();
    }

    [Fact]
    public void Plan_NoAliveShards_ReturnsFailed()
    {
        // Arrange: умер единственный шард с бакетами, живых нет.
        var routing = new List<BucketRoute> { Bucket(0, "shard1") };

        // Act: строим план без живых шардов.
        var result = EvacuationPlanner.Plan(routing, "shard1", []);

        // Assert: эвакуировать некуда — отказ.
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().NotBeNull();
    }

    [Fact]
    public void Plan_OwnerlessBucketHole_IsSkipped()
    {
        // Arrange: в карте дыра — бакет без владельца; рядом обычные бакеты dead-шарда.
        var routing = new List<BucketRoute>
        {
            Bucket(0, "shard2"), Bucket(1, null), Bucket(2, "shard2"),
        };

        // Act: строим план эвакуации shard2.
        var result = EvacuationPlanner.Plan(routing, "shard2", ["shard1"]);

        // Assert: дыра карты пропущена, эвакуируются только бакеты dead-шарда.
        result.IsSuccess.Should().BeTrue();
        result.Value.Select(a => a.BucketId).Should().Equal(0, 2);
    }
}
