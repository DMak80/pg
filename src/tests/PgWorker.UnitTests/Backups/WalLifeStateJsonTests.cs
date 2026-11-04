using FluentAssertions;
using PgWorker.Backups;
using PgWorker.Etcd.Backups;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Сериализация значения ключа wal-life (канон backups/05 §1, spec §4.1):
// roundtrip writer/reader, канонические snake_case-поля и reason-строки.
public class WalLifeStateJsonTests
{
    [Fact]
    public void Serialize_CanonicalSnakeCaseFields()
    {
        // Arrange / Act
        var json = WalLifeStateJson.Serialize(new(3, 1757500000, WalLifeReason.Restore));

        // Assert
        json.Should().Contain("\"chain_life\":3")
            .And.Contain("\"bumped_unix\":1757500000")
            .And.Contain("\"reason\":\"restore\"");
        json.Should().NotContain("chainLife").And.NotContain("bumpedUnix"); // camelCase запрещён
    }

    [Theory]
    [InlineData(WalLifeReason.ShardRebuilt, "shard-rebuilt")]
    [InlineData(WalLifeReason.Restore, "restore")]
    [InlineData(WalLifeReason.ForeignChain, "foreign-chain")]
    public void Roundtrip_AllReasons(WalLifeReason reason, string wire)
    {
        // Arrange
        var json = WalLifeStateJson.Serialize(new(7, 42, reason));

        // Act
        var parsed = WalLifeStateJson.TryParse(json);

        // Assert
        parsed.Should().Be(new WalLifeState(7, 42, reason));
        json.Should().Contain($"\"reason\":\"{wire}\"");
    }

    [Fact]
    public void TryParse_BrokenJson_ReturnsNull()
    {
        // Arrange / Act / Assert
        WalLifeStateJson.TryParse("{oops").Should().BeNull();
        WalLifeStateJson.TryParse(null).Should().BeNull();
    }

    [Fact]
    public void WalLifeKey_CanonicalPath()
    {
        // Arrange / Act / Assert
        BackupNames.WalLifeKey("c1", "shard1")
            .Should().Be("/pgworker/backups/c1/shard1/wal-life");
    }
}
