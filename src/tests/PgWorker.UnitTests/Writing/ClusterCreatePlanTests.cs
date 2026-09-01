using PgWorker.Core.Writing;

namespace PgWorker.UnitTests.Writing;

// Валидация создания кластера: arch/02 §9.3 — сервер источник истины (spec t12 §3.3).
// Перенос из AdminPanel.UnitTests (task etcd-via-worker-api): фиксстуры значений 1:1.
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

// План ключей одного создания: arch/02 §9.1 — конфиг, шарды, ноды, routing блоками (§9.1.1), request_*.
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

        // блочное распределение (arch/02 §9.1.1): 4×2 → бакеты 0,1=shard1; 2,3=shard2
        plan.Puts.Single(p => p.Key == "/clusters/shop/buckets/routing/bucket_0").Value.Should().Be("shard1");
        plan.Puts.Single(p => p.Key == "/clusters/shop/buckets/routing/bucket_1").Value.Should().Be("shard1");
        plan.Puts.Single(p => p.Key == "/clusters/shop/buckets/routing/bucket_2").Value.Should().Be("shard2");

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
    public void Build_BlockUneven_RemainderToLaterShards()
    {
        // Arrange: 5 бакетов, 2 шарда — блоки 2+3, остаток у ПОСЛЕДНЕГО (spec §2.1)
        var request = new CreateClusterRequest("u", 5, 2, 1, 1m, 1, 1);

        // Act
        var plan = ClusterCreatePlan.Build(request, 1);

        // Assert: floor((2i+1)·2/10): b0,b1→shard1; b2,b3,b4→shard2
        plan.Puts.Single(p => p.Key == "/clusters/u/buckets/routing/bucket_4").Value.Should().Be("shard2");
    }

    [Fact]
    public void Build_NormalizedSingle_DegenerateStructure()
    {
        // Arrange: нешардированная — нормализованный запрос (мусор перезаписан в 1×1)
        var request = new CreateClusterRequest("solo", 999, 99, 2, 0.5m, 8, 100, Sharded: false).Normalize();

        // Act
        var plan = ClusterCreatePlan.Build(request, nowUnix: 1755900000);

        // Assert: config.buckets=1; единственный shard1 (ноды a/b); единственный
        // bucket_0 → shard1; заявки только /service/solo-shard1/* (arch/02 §9.1)
        plan.ConfigValue.Should().Contain("\"buckets\":1");
        var keys = plan.Puts.Select(p => p.Key).ToList();
        keys.Should().Contain(
        [
            "/clusters/solo/shards/shard1/replicas",
            "/clusters/solo/shards/shard1/nodes/shard1a/state",
            "/clusters/solo/shards/shard1/nodes/shard1b/state",
            "/clusters/solo/buckets/routing/bucket_0",
            "/clusters/solo/buckets/status/bucket_0",
        ]);
        keys.Where(k => k.Contains("shard2")).Should().BeEmpty();
        keys.Where(k => k.Contains("bucket_1")).Should().BeEmpty();
        plan.Puts.Single(p => p.Key == "/clusters/solo/buckets/routing/bucket_0").Value.Should().Be("shard1");
        plan.RequestKeys.Should().BeEquivalentTo(
        [
            "/service/solo-shard1/request_cpu",
            "/service/solo-shard1/request_mem",
            "/service/solo-shard1/request_disk",
        ]);
    }

    // Канон распределения (arch/02 §9.1.1): непрерывные блоки, «бакет к ближайшему
    // центру отрезка» — floor((2i+1)·S/(2N)); таблица и свойства — spec §2.1.
    [Fact]
    public void OwnerShard_CanonicalTenByThree_BlocksThreeFourThree()
    {
        // Arrange: канон пользователя — 10×3, остаток СРЕДНЕМУ шарду (spec §2.1)
        // Act
        var owners = Enumerable.Range(0, 10)
            .Select(i => ClusterCreatePlan.OwnerShard(i, 10, 3)).ToArray();

        // Assert: shard1={0,1,2}, shard2={3,4,5,6}, shard3={7,8,9} — расклад 3+4+3
        owners.Should().Equal(1, 1, 1, 2, 2, 2, 2, 3, 3, 3);
    }

    [Theory]
    [InlineData(4, 2, new[] { 1, 1, 2, 2 })]
    [InlineData(5, 2, new[] { 1, 1, 2, 2, 2 })]
    [InlineData(7, 3, new[] { 1, 1, 2, 2, 2, 3, 3 })]
    [InlineData(8, 3, new[] { 1, 1, 1, 2, 2, 3, 3, 3 })]
    [InlineData(9, 4, new[] { 1, 1, 2, 2, 3, 3, 3, 4, 4 })]
    [InlineData(3, 3, new[] { 1, 2, 3 })]
    [InlineData(1, 1, new[] { 1 })]
    public void OwnerShard_Table_MatchesSpec(int buckets, int shards, int[] expected)
    {
        // Arrange: строки таблицы распределений spec §2.1
        // Act
        var owners = Enumerable.Range(0, buckets)
            .Select(i => ClusterCreatePlan.OwnerShard(i, buckets, shards));

        // Assert
        owners.Should().Equal(expected);
    }

    [Theory]
    [InlineData(10, 3)]
    [InlineData(4, 2)]
    [InlineData(5, 2)]
    [InlineData(7, 3)]
    [InlineData(8, 3)]
    [InlineData(9, 4)]
    [InlineData(16, 3)]
    [InlineData(100, 7)]
    [InlineData(3, 3)]
    [InlineData(1, 1)]
    [InlineData(8192, 128)]
    public void OwnerShard_Properties_ContinuousBalancedNonEmpty(int buckets, int shards)
    {
        // Arrange: свойства формулы §9.1.1 при допустимых N ≥ S ≥ 1
        // Act
        var owners = Enumerable.Range(0, buckets)
            .Select(i => ClusterCreatePlan.OwnerShard(i, buckets, shards)).ToArray();

        // Assert: размеры шардов — сумма N, размах не более 1
        var sizes = Enumerable.Range(1, shards)
            .Select(k => owners.Count(o => o == k)).ToArray();
        sizes.Sum().Should().Be(buckets);
        (sizes.Max() - sizes.Min()).Should().BeLessThanOrEqualTo(1);

        // Assert: каждый шард непуст, его бакеты — непрерывный диапазон (блок)
        foreach (var k in Enumerable.Range(1, shards))
        {
            var ids = Enumerable.Range(0, buckets).Where(i => owners[i] == k).ToArray();
            ids.Should().NotBeEmpty();
            (ids.Last() - ids.First() + 1).Should().Be(ids.Length);
        }
    }

    [Fact]
    public void OwnerShard_LargeSizes_ExactSplits()
    {
        // Arrange/Act/Assert: точные расклады больших N×S из таблицы spec §2.1
        BlockSizes(16, 3).Should().Equal(5, 6, 5);
        BlockSizes(100, 7).Should().Equal(14, 15, 14, 14, 14, 15, 14);
        BlockSizes(8192, 128).Should().Match(l => l.Count() == 128 && l.All(s => s == 64));

        static int[] BlockSizes(int buckets, int shards)
            => Enumerable.Range(1, shards)
                .Select(k => Enumerable.Range(0, buckets)
                    .Count(i => ClusterCreatePlan.OwnerShard(i, buckets, shards) == k))
                .ToArray();
    }
}

// Нормализация запроса создания: arch/02 §9.3 — sharded=false → buckets/shards
// игнорируются и перезаписываются в 1/1; отсутствующий sharded = true.
public class CreateClusterNormalizeTests
{
    [Fact]
    public void Normalize_ShardedAbsent_TrueAndValuesKept()
    {
        // Arrange: легаси-запрос без sharded (null) — обратная совместимость
        var request = new CreateClusterRequest("shop", 4, 2, 2, 0.5m, 8, 100);

        // Act
        var normalized = request.Normalize();

        // Assert: sharded=true, buckets/shards без изменений
        normalized.Sharded.Should().BeTrue();
        normalized.Buckets.Should().Be(4);
        normalized.Shards.Should().Be(2);
    }

    [Fact]
    public void Normalize_ShardedFalse_OverwritesToOneAndOne()
    {
        // Arrange: мусорные buckets/shards при нешардированной — игнорируются
        var request = new CreateClusterRequest("solo", 9999, -5, 2, 1m, 8, 100, Sharded: false);

        // Act
        var normalized = request.Normalize();

        // Assert: вырожденный случай 1×1 (arch/02 §9.1)
        normalized.Sharded.Should().BeFalse();
        normalized.Buckets.Should().Be(1);
        normalized.Shards.Should().Be(1);
    }

    [Fact]
    public void Normalize_ShardedTrue_KeepsValues()
    {
        // Arrange
        var request = new CreateClusterRequest("shop", 8, 4, 2, 1m, 8, 100, Sharded: true);

        // Act
        var normalized = request.Normalize();

        // Assert
        normalized.Sharded.Should().BeTrue();
        normalized.Buckets.Should().Be(8);
        normalized.Shards.Should().Be(4);
    }

    [Fact]
    public void Normalize_Idempotent()
    {
        // Arrange
        var request = new CreateClusterRequest("solo", 7, 3, 2, 1m, 8, 100, Sharded: false);

        // Act/Assert: повторная нормализация ничего не меняет
        request.Normalize().Normalize().Should().Be(request.Normalize());
    }

    [Fact]
    public void Validate_AfterNormalizeSingleWithGarbage_NoErrors()
    {
        // Arrange: невалидные buckets/shards нормализованы ДО валидации
        var normalized = new CreateClusterRequest("solo", 0, 999, 2, 1m, 8, 100, Sharded: false).Normalize();

        // Act/Assert: ошибок по buckets/shards нет — сервер нормализовал (arch/02 §9.3)
        CreateClusterValidator.Validate(normalized).Should().BeEmpty();
    }
}
