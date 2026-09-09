using FluentAssertions;
using PgWorker.Backups;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Разбор/сравнение имён WAL-файлов (arch/19 §3: TLI(8)+log(8)+seg(8) hex).
public class WalFileNameTests
{
    [Theory]
    [InlineData("000000010000000000000001", 1u, 0u, 1u)]
    [InlineData("0000000200000001000000FE", 2u, 1u, 0xFEu)]
    public void TryParse_корректные_сегменты(string name, uint tli, uint log, uint seg)
    {
        // Arrange / Act
        var parsed = WalFileName.TryParse(name);

        // Assert
        parsed.Should().NotBeNull();
        parsed!.Value.Tli.Should().Be(tli);
        parsed.Value.Log.Should().Be(log);
        parsed.Value.Seg.Should().Be(seg);
        parsed.Value.Name.Should().Be(name.ToLowerInvariant());
    }

    [Theory]
    [InlineData("000000010000000000000001.partial")]  // незакрытый сегмент
    [InlineData("00000002.history")]                  // history-файл
    [InlineData("0000000100000000000000")]            // 23 символа
    [InlineData("0000000100000000000000ZZ")]          // не hex
    [InlineData("backup_manifest")]
    [InlineData("")]
    public void TryParse_не_сегменты_дает_null(string name)
    {
        // Arrange / Act / Assert
        WalFileName.TryParse(name).Should().BeNull();
    }

    [Fact]
    public void TryParse_регистронезависим()
    {
        // Arrange / Act
        var parsed = WalFileName.TryParse("0000000100000000000000AB");

        // Assert — pg_receivewal пишет lowercase, S3 может вернуть иначе
        parsed!.Value.Seg.Should().Be(0xAB);
        parsed.Value.Name.Should().Be("0000000100000000000000ab");
    }

    [Theory]
    [InlineData("000000010000000000000001.partial", true)]
    [InlineData("000000010000000000000001", false)]
    [InlineData("00000002.history", false)]
    public void IsPartial_только_partial_суффикс(string name, bool expected)
    {
        // Arrange / Act / Assert
        WalFileName.IsPartial(name).Should().Be(expected);
    }

    [Theory]
    [InlineData("00000002.history", 2u)]
    [InlineData("00000010.history", 16u)]
    public void TryParseHistory_валидные(string name, uint tli)
    {
        // Arrange / Act
        var parsed = WalFileName.TryParseHistory(name);

        // Assert
        parsed.Should().Be(tli);
    }

    [Theory]
    [InlineData("000000010000000000000001")]
    [InlineData("00000002.histories")]
    [InlineData("2.history")]
    public void TryParseHistory_невалидные_дает_null(string name)
    {
        // Arrange / Act / Assert
        WalFileName.TryParseHistory(name).Should().BeNull();
    }

    [Fact]
    public void Next_инкремент_сегмента()
    {
        // Arrange
        var segment = WalFileName.TryParse("000000010000000000000001")!.Value;

        // Act / Assert
        segment.Next().Name.Should().Be("000000010000000000000002");
    }

    [Fact]
    public void Next_переход_0xFF_переносит_log()
    {
        // Arrange — seg=0xFF: следующий = log+1, seg=0 (arch/19 §3)
        var segment = WalFileName.TryParse("0000000100000000000000FF")!.Value;

        // Act / Assert
        segment.Next().Name.Should().Be("000000010000000100000000");
    }

    [Fact]
    public void DistanceTo_разница_в_сегментах_через_log()
    {
        // Arrange — (log=0,seg=254) → (log=2,seg=1): 2*256+1-254 = 259
        var from = WalFileName.TryParse("0000000100000000000000FE")!.Value;
        var to = WalFileName.TryParse("000000010000000200000001")!.Value;

        // Act / Assert
        from.DistanceTo(to).Should().Be(259);
        to.DistanceTo(from).Should().Be(-259);
    }

    [Fact]
    public void FromLsn_сегмент_16MB()
    {
        // Arrange — LSN 0/3000000 при wal_segment_size=16MB: segId=3 → log=0, seg=3
        // Act
        var segment = WalFileName.FromLsn(1, "0/3000000");

        // Assert
        segment.Name.Should().Be("000000010000000000000003");
    }
}
