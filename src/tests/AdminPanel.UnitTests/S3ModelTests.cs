using AdminPanel.Core.S3;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Маппинг состояний S3-домена (arch/23 §2: отсутствие state = Active).
public sealed class S3ModelTests
{
    // Arrange: raw-строки state. Act: S3ClusterStates.Parse. Assert: канон §2.
    [Theory]
    [InlineData(null, S3ClusterState.Active)]
    [InlineData("", S3ClusterState.Active)]
    [InlineData("NOT_INITIALIZED", S3ClusterState.NotInitialized)]
    [InlineData("TO_REMOVE", S3ClusterState.ToRemove)]
    [InlineData("WEIRD_FUTURE", S3ClusterState.Active)] // толерантность к новым
    public void Parse_State_MapsCanonically(string? raw, S3ClusterState expected)
    {
        S3ClusterStates.Parse(raw).Should().Be(expected);
    }
}
