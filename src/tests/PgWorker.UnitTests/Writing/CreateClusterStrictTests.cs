using System.Text.Json;
using PgWorker.Core.Writing;

namespace PgWorker.UnitTests.Writing;

// t06 (spec §3.2/§6.2): опция strict в создании — нормализация null→true,
// валидация strict→replicas≥2, wire-имя synchronousModeStrict, ConfigJson.
public class CreateClusterStrictTests
{
    // Minimal API биндит camelCase (JsonSerializerDefaults.Web) — сериализуем
    // теми же опциями: wire-имя обязано быть synchronousModeStrict (spec §2.7).
    private static readonly JsonSerializerOptions Wire = new(JsonSerializerDefaults.Web);

    private static CreateClusterRequest Valid(bool? syncStrict = null) => new(
        "shop", 4, 2, 2, 2m, 8, 100, Sharded: true, SyncStrict: syncStrict);

    // AAA: Normalize — отсутствующая опция становится true (дефолт strict).
    [Fact]
    public void Normalize_NullSyncStrict_BecomesTrue()
    {
        // Arrange
        var request = Valid();

        // Act
        var normalized = request.Normalize();

        // Assert
        normalized.SyncStrict.Should().BeTrue();
    }

    // AAA: wire-имя поля — synchronousModeStrict (не syncStrict): биндинг
    // Minimal API и прокси-панель проходят только при точном имени.
    [Fact]
    public void Serialize_WireName_IsSynchronousModeStrict()
    {
        // Arrange
        var request = Valid(syncStrict: false);

        // Act
        var json = JsonSerializer.Serialize(request, Wire);

        // Assert
        json.Should().Contain("\"synchronousModeStrict\":false")
            .And.NotContain("\"syncStrict\"");
    }

    // AAA: strict (true или null) + replicas=1 — ошибка по полю syncStrict.
    [Theory]
    [InlineData(null)]
    [InlineData(true)]
    public void Validate_StrictWithSingleReplica_Fails(bool? syncStrict)
    {
        // Arrange
        var request = Valid(syncStrict) with { Replicas = 1 };

        // Act
        var errors = CreateClusterValidator.Validate(request.Normalize());

        // Assert
        errors.Should().ContainSingle(e => e.Field == "syncStrict")
            .Which.Message.Should().Contain("replicas ≥ 2");
    }

    // AAA: strict=false + replicas=1 — валидно (ограничений нет).
    [Fact]
    public void Validate_StrictOffWithSingleReplica_Passes()
    {
        // Arrange / Act
        var errors = CreateClusterValidator.Validate(
            (Valid(syncStrict: false) with { Replicas = 1 }).Normalize());

        // Assert
        errors.Should().BeEmpty();
    }

    // AAA: план пишет synchronous_mode_strict в ConfigJson.
    [Theory]
    [InlineData(null, "\"synchronous_mode_strict\":true")]
    [InlineData(false, "\"synchronous_mode_strict\":false")]
    public void Build_ConfigJsonCarriesStrict(bool? syncStrict, string expected)
    {
        // Arrange / Act
        var plan = ClusterCreatePlan.Build(Valid(syncStrict).Normalize(), 1_700_000_000);

        // Assert
        plan.ConfigValue.Should().Contain(expected);
    }
}
