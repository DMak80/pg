using PgWorker.Backups.Job;

namespace PgWorker.UnitTests.Backups;

// Парсер stdout джоба (arch/19 §2): маркеры фаз/result JSON, незнакомые
// строки лога игнорируются (толерантность протокола).
public class BackupJobLogTests
{
    // AAA: полный лог — фаза uploading + result ok
    [Fact]
    public void Parse_PhasesAndResult()
    {
        // Arrange — реальный шум pg_basebackup + маркеры
        var logs = """
            {"phase":"basebackup"}
            12345678/99999999 kB (100%), tablespace 0
            {"phase":"uploading","wal_start_segment":"000000010000000000000042"}
            mc: copied 12 objects
            {"ok":true,"wal_start_segment":"000000010000000000000042","size_bytes":1048576}
            """;

        // Act
        var markers = BackupJobLog.Parse(logs);

        // Assert
        markers.Phase.Should().Be("uploading");
        markers.WalStartSegment.Should().Be("000000010000000000000042");
        markers.Result.Should().NotBeNull();
        markers.Result!.Ok.Should().BeTrue();
        markers.Result.WalStartSegment.Should().Be("000000010000000000000042");
        markers.Result.SizeBytes.Should().Be(1048576);
        markers.Result.Error.Should().BeNull();
    }

    // AAA: провал — result ok=false с ошибкой
    [Fact]
    public void Parse_FailureResult()
    {
        // Arrange / Act
        var markers = BackupJobLog.Parse("{\"ok\":false,\"error\":\"staging: no space left on device\"}\n");

        // Assert
        markers.Result.Should().NotBeNull();
        markers.Result!.Ok.Should().BeFalse();
        markers.Result.Error.Should().Be("staging: no space left on device");
    }

    // AAA: мусорные/незнакомые строки — не ошибка, маркеров нет
    [Fact]
    public void Parse_OnlyNoise_NoMarkers()
    {
        // Arrange / Act
        var markers = BackupJobLog.Parse("not json\n{\"unknown\":1}\nplain text");

        // Assert
        markers.Phase.Should().BeNull();
        markers.Result.Should().BeNull();
        markers.WalStartSegment.Should().BeNull();
    }
}
