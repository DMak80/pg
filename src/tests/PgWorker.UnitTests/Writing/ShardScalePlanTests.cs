using PgWorker.Core.Writing;

namespace PgWorker.UnitTests.Writing;

// План ключей add-shard и валидатор запроса (arch/02 §9.5, t06).
// Перенос из AdminPanel.UnitTests (task etcd-via-worker-api): фиксстуры значений 1:1.
public class ShardScalePlanTests
{
    [Fact]
    public void Build_FullKeySet_MatchesContractSection9_5()
    {
        // Arrange — запрос на 2 реплики (валидный)
        var request = new AddShardRequest(2, 0.5m, 8, 100);

        // Act
        var plan = ShardScalePlan.Build("shop", "shard3", request);

        // Assert — ровно контракт 02 §9.5 (1:1 §4.1 spec): клэйм-ключ + пакет
        plan.ReplicasKey.Should().Be("/clusters/shop/shards/shard3/replicas");
        plan.ReplicasValue.Should().Be("2");
        plan.Puts.Select(p => p.Key).Should().BeEquivalentTo(
        [
            "/clusters/shop/shards/shard3/nodes/shard3a/state",
            "/clusters/shop/shards/shard3/nodes/shard3b/state",
            "/service/shop-shard3/request_cpu",
            "/service/shop-shard3/request_mem",
            "/service/shop-shard3/request_disk",
        ]);
        plan.Puts.Single(p => p.Key.EndsWith("shard3a/state")).Value.Should().Be("NOT_INITIALIZED");
        plan.RequestKeys.Should().BeEquivalentTo(
        [
            "/service/shop-shard3/request_cpu", "/service/shop-shard3/request_mem", "/service/shop-shard3/request_disk",
        ]);
        plan.CanonicalCpu.Should().Be("0.5");
        plan.CanonicalMem.Should().Be("8Gi");
        plan.CanonicalDisk.Should().Be("100Gi");
    }

    [Fact]
    public void Validator_Boundaries_TableOf409And400()
    {
        // Arrange / Act / Assert — границы §9.3: replicas 0/27 → ошибка; cpu
        // 0.009/64.1 → ошибка; mem 0/65537 → ошибка; валидный (2, 2, 8, 100) → пусто
        AddShardValidator.Validate(new(0, 2, 8, 100)).Should().Contain(e => e.Field == "replicas");
        AddShardValidator.Validate(new(27, 2, 8, 100)).Should().Contain(e => e.Field == "replicas");
        AddShardValidator.Validate(new(2, 0.009m, 8, 100)).Should().Contain(e => e.Field == "requestCpu");
        AddShardValidator.Validate(new(2, 64.1m, 8, 100)).Should().Contain(e => e.Field == "requestCpu");
        AddShardValidator.Validate(new(2, 2, 0, 100)).Should().Contain(e => e.Field == "requestMem");
        AddShardValidator.Validate(new(2, 2, 8, 65537)).Should().Contain(e => e.Field == "requestDisk");
        AddShardValidator.Validate(new(2, 2, 8, 100)).Should().BeEmpty();
    }
}
