using AdminPanel.Api.Inspection;
using AdminPanel.Core.Valkey;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Модель valkey-домена: толерантный state-маппинг (arch/20 §5); маппер
// деталей API (t07): CaRotation Core-тикета → API-DTO (null-пропagation).
public sealed class ValkeyModelTests
{
    // Arrange: сырые значения state. Act: Parse. Assert: канон + незнакомое → Active.
    [Theory]
    [InlineData("NOT_INITIALIZED", ValkeyClusterState.NotInitialized)]
    [InlineData("TO_REMOVE", ValkeyClusterState.ToRemove)]
    [InlineData(null, ValkeyClusterState.Active)]
    [InlineData("", ValkeyClusterState.Active)]
    [InlineData("SOMETHING_NEW", ValkeyClusterState.Active)]
    public void Parse_State_MapsTolerantly(string? raw, ValkeyClusterState expected)
    {
        var actual = ValkeyClusterStates.Parse(raw);
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void MapDetails_CaRotationTicket_MapsToDto()
    {
        // Arrange — Core-модель с живой CA-заявкой (Rotation = null)
        var cluster = new ValkeyClusterInfo(
            "live", ValkeyClusterState.Active, 1, 536870912L, "allkeys-lru",
            1756500000, "localhost:17001", [],
            CaRotation: new ValkeyCaRotationTicket("live", 1756500123, "seed"));

        // Act
        var dto = ValkeyMappers.MapDetails(cluster);

        // Assert — поле проброшено в API-DTO (JSON caRotation — бейдж Task 9)
        dto.CaRotation.Should().NotBeNull();
        dto.CaRotation!.RequestedUnix.Should().Be(1756500123);
        dto.CaRotation.RequestedBy.Should().Be("seed");
    }

    [Fact]
    public void MapDetails_NoCaRotation_NullDtoField()
    {
        // Arrange — заявки нет (Rotation и CaRotation = null)
        var cluster = new ValkeyClusterInfo(
            "live", ValkeyClusterState.Active, 1, 536870912L, "allkeys-lru",
            1756500000, "localhost:17001", []);

        // Act
        var dto = ValkeyMappers.MapDetails(cluster);

        // Assert — null (не undefined): фронтенд-бейдж не рендерится
        dto.CaRotation.Should().BeNull();
    }
}
