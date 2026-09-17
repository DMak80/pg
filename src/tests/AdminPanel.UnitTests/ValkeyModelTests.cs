using AdminPanel.Core.Valkey;
using Xunit;

namespace AdminPanel.UnitTests;

// Модель valkey-домена: толерантный state-маппинг (arch/20 §5).
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
}
