using AdminPanel.Core;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Возраст не-ACTIVE статуса: единая формула правил move-* и ClusterDetailsMapper (spec §3.7, §4.4).
public class MoveAgeTests
{
    private static readonly long Now = 1_800_000_000;

    [Fact]
    public void Seconds_FromUpdatedUnix()
    {
        // Arrange: SYNCING с обоими штампами — база updated_unix (roadmap t05).
        var bucket = new BucketInfo(1, "s1", BucketState.Syncing,
            new MoveInfo("s1", "s2", Now - 700, Now - 30, "copy", null));

        // Act
        var age = MoveAge.Seconds(bucket, Now);

        // Assert
        age.Should().Be(30);
    }

    [Fact]
    public void Seconds_FallsBackToStartedUnix()
    {
        // Arrange: updated отсутствует — толерантный fallback на started (spec §3.7).
        var bucket = new BucketInfo(1, "s1", BucketState.Syncing,
            new MoveInfo("s1", "s2", Now - 700, null, "copy", null));

        // Act
        var age = MoveAge.Seconds(bucket, Now);

        // Assert
        age.Should().Be(700);
    }

    [Fact]
    public void Seconds_ActiveBucket_Null()
    {
        // Arrange / Act
        var age = MoveAge.Seconds(new BucketInfo(1, "s1", BucketState.Active, null), Now);

        // Assert: возраст только для не-ACTIVE (arch/03 §2).
        age.Should().BeNull();
    }

    [Fact]
    public void Seconds_NoTimestamps_Null()
    {
        // Arrange: оба штампа отсутствуют — меры возраста нет (битые данные видит key-malformed).
        var bucket = new BucketInfo(1, "s1", BucketState.Frozen, new MoveInfo("s1", "s2", null, null, null, null));

        // Act
        var age = MoveAge.Seconds(bucket, Now);

        // Assert
        age.Should().BeNull();
    }

    [Fact]
    public void Stamp_MatchesSecondsBase()
    {
        // Arrange: штамп-база — тот же fallback, что у Seconds (кормит details правил).
        var bucket = new BucketInfo(1, "s1", BucketState.Aborting,
            new MoveInfo("s2", "s1", Now - 45, null, "cleanup", "err"));

        // Act / Assert
        MoveAge.Stamp(bucket).Should().Be(Now - 45);
    }
}
