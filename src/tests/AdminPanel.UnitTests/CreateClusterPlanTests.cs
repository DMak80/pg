using AdminPanel.Etcd.Writing;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Валидация создания кластера: arch/02 §9.3 — сервер источник истины (spec t12 §3.3).
public class CreateClusterValidatorTests
{
    private static readonly CreateClusterRequest Valid =
        new("shop", 4, 2, 2, 0.5m, 8, 100);

    [Fact]
    public void Validate_ValidRequest_NoErrors()
    {
        // Arrange/Act/Assert
        CreateClusterValidator.Validate(Valid).Should().BeEmpty();
    }

    [Theory]
    [InlineData("Shop")]       // верхний регистр
    [InlineData("1shop")]      // начинается с цифры
    [InlineData("shop-x")]     // дефис: коллизии ScopeMatcher (spec t12 §8.5)
    [InlineData("шоп")]        // не [a-z0-9_]
    [InlineData("")]           // пустое
    public void Validate_BadNames_Rejected(string name)
    {
        // Arrange
        var request = Valid with { Name = name };

        // Act
        var errors = CreateClusterValidator.Validate(request);

        // Assert
        errors.Should().Contain(e => e.Field == "name");
    }

    [Fact]
    public void Validate_NameTooLong_Rejected()
    {
        // Arrange: 64 символа — больше 63 (максимум {1,63} после первого)
        var request = Valid with { Name = new string('a', 64) };

        // Act/Assert
        CreateClusterValidator.Validate(request).Should().Contain(e => e.Field == "name");
    }

    [Fact]
    public void Validate_BucketsOutOfRange_Rejected()
    {
        // Arrange/Act/Assert: 0 и 8193 вне 1..8192
        CreateClusterValidator.Validate(Valid with { Buckets = 0 }).Should().Contain(e => e.Field == "buckets");
        CreateClusterValidator.Validate(Valid with { Buckets = 8193 }).Should().Contain(e => e.Field == "buckets");
    }

    [Fact]
    public void Validate_ShardsWithoutBuckets_Rejected()
    {
        // Arrange: шардов больше бакетов — задание пользователя (spec t12 §1)
        var request = Valid with { Shards = 5 };

        // Act
        var errors = CreateClusterValidator.Validate(request);

        // Assert
        errors.Should().Contain(e => e.Field == "shards" && e.Message.Contains("бакетов"));
    }

    [Fact]
    public void Validate_ReplicasOutOfRange_Rejected()
    {
        // Arrange/Act/Assert: 0 и 27 вне 1..26 (буквы нод a..z)
        CreateClusterValidator.Validate(Valid with { Replicas = 0 }).Should().Contain(e => e.Field == "replicas");
        CreateClusterValidator.Validate(Valid with { Replicas = 27 }).Should().Contain(e => e.Field == "replicas");
    }

    [Fact]
    public void Validate_ResourcesOutOfRange_Rejected()
    {
        // Arrange/Act/Assert
        CreateClusterValidator.Validate(Valid with { RequestCpu = 0.001m }).Should().Contain(e => e.Field == "requestCpu");
        CreateClusterValidator.Validate(Valid with { RequestCpu = 65m }).Should().Contain(e => e.Field == "requestCpu");
        CreateClusterValidator.Validate(Valid with { RequestMem = 0 }).Should().Contain(e => e.Field == "requestMem");
        CreateClusterValidator.Validate(Valid with { RequestDisk = 65537 }).Should().Contain(e => e.Field == "requestDisk");
    }

    [Fact]
    public void Canonical_Strings_AreInvariant()
    {
        // Arrange/Act/Assert: cpu — десятичные ядра без хвостовых нулей; GiB — "<n>Gi"
        CreateClusterValidator.CanonicalCpu(2.0m).Should().Be("2");
        CreateClusterValidator.CanonicalCpu(0.50m).Should().Be("0.5");
        CreateClusterValidator.CanonicalGiB(8).Should().Be("8Gi");
    }
}

// План ключей одного создания: arch/02 §9.1 — конфиг, шарды, ноды, routing round-robin, request_*.
public class ClusterCreatePlanTests
{
    [Fact]
    public void Build_FullPlan_MatchesContract()
    {
        // Arrange
        var request = new CreateClusterRequest("shop", 4, 2, 2, 0.5m, 8, 100);

        // Act
        var plan = ClusterCreatePlan.Build(request, nowUnix: 1755900000);

        // Assert: клэйм — конфиг со state NOT_INITIALIZED
        plan.ConfigKey.Should().Be("/clusters/shop/config");
        plan.ConfigValue.Should().Be(
            """{"buckets":4,"dbname":"shop","created_unix":1755900000,"state":"NOT_INITIALIZED"}""");

        // Порядок ключей пакета: shards → nodes → routing+status → request_* (детерминированный).
        var keys = plan.Puts.Select(p => p.Key).ToList();
        keys.Should().BeInAscendingOrder(); // отсортирован — стабильные повторы/диагностика
        keys.Should().Contain(
        [
            "/clusters/shop/shards/shard1/replicas",
            "/clusters/shop/shards/shard2/replicas",
            "/clusters/shop/shards/shard1/nodes/shard1a/state",
            "/clusters/shop/shards/shard1/nodes/shard1b/state",
            "/clusters/shop/buckets/routing/bucket_0",
            "/clusters/shop/buckets/status/bucket_0",
            "/service/shop-shard1/request_cpu",
            "/service/shop-shard2/request_disk",
        ]);

        // round-robin: bucket_i → shard_(i % S + 1) — как init-cluster.sh
        plan.Puts.Single(p => p.Key == "/clusters/shop/buckets/routing/bucket_0").Value.Should().Be("shard1");
        plan.Puts.Single(p => p.Key == "/clusters/shop/buckets/routing/bucket_1").Value.Should().Be("shard2");
        plan.Puts.Single(p => p.Key == "/clusters/shop/buckets/routing/bucket_2").Value.Should().Be("shard1");

        // статус бакета: NOT_INITIALIZED + owner + updated_unix, без target/phase
        plan.Puts.Single(p => p.Key == "/clusters/shop/buckets/status/bucket_3").Value.Should().Be(
            """{"bucket":"bucket_3","state":"NOT_INITIALIZED","owner":"shard2","updated_unix":1755900000}""");

        // ноды: state NOT_INITIALIZED; replicas в etcd — строкой
        plan.Puts.Single(p => p.Key == "/clusters/shop/shards/shard1/nodes/shard1a/state").Value.Should().Be("NOT_INITIALIZED");
        plan.Puts.Single(p => p.Key == "/clusters/shop/shards/shard1/replicas").Value.Should().Be("2");

        // request_* — на каждый шард, канонические строки
        plan.Puts.Single(p => p.Key == "/service/shop-shard1/request_cpu").Value.Should().Be("0.5");
        plan.Puts.Single(p => p.Key == "/service/shop-shard1/request_mem").Value.Should().Be("8Gi");
        plan.Puts.Single(p => p.Key == "/service/shop-shard2/request_disk").Value.Should().Be("100Gi");

        // компенсационный список — ровно request-ключи (префикс кластера удаляется целиком)
        plan.RequestKeys.Should().BeEquivalentTo(
        [
            "/service/shop-shard1/request_cpu",
            "/service/shop-shard1/request_mem",
            "/service/shop-shard1/request_disk",
            "/service/shop-shard2/request_cpu",
            "/service/shop-shard2/request_mem",
            "/service/shop-shard2/request_disk",
        ]);

        plan.CanonicalCpu.Should().Be("0.5");
        plan.CanonicalMem.Should().Be("8Gi");
        plan.CanonicalDisk.Should().Be("100Gi");
    }

    [Fact]
    public void Build_NodeNames_AscendingLetters_UpTo26()
    {
        // Arrange
        var request = new CreateClusterRequest("big", 26, 1, 26, 1m, 8, 100);

        // Act
        var plan = ClusterCreatePlan.Build(request, 1);

        // Assert: буквы a..z; мастер — <X>a (spec t12 §8.4)
        plan.Puts.Where(p => p.Key.StartsWith("/clusters/big/shards/shard1/nodes/"))
            .Should().HaveCount(26);
        plan.Puts.Should().Contain(p => p.Key == "/clusters/big/shards/shard1/nodes/shard1z/state");
    }

    [Fact]
    public void Build_RoundRobinUneven_FirstShardsGetExtra()
    {
        // Arrange: 5 бакетов, 2 шарда — как init-cluster.sh (первые rem шардов по +1)
        var request = new CreateClusterRequest("u", 5, 2, 1, 1m, 1, 1);

        // Act
        var plan = ClusterCreatePlan.Build(request, 1);

        // Assert: i % S: 0→shard1,1→shard2,2→shard1,3→shard2,4→shard1
        plan.Puts.Single(p => p.Key == "/clusters/u/buckets/routing/bucket_4").Value.Should().Be("shard1");
    }
}
