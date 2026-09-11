using PgWorker.Backups.Restore;

namespace PgWorker.UnitTests.Backups;

// Разбор backup_label полного бэкапа (t05 §3.6): стартовый WAL-сегмент —
// из строки START WAL LOCATION (sed-эквивалент entrypoint t02).
public class BackupLabelTests
{
    // AAA: сегмент из START WAL LOCATION; мусор → null.
    [Fact]
    public void WalStartSegment_FromBackupLabel()
    {
        // Arrange — реальный формат backup_label pg_basebackup.
        const string raw = """
            START WAL LOCATION: 0/2000028 (file 000000010000000000000002)
            CHECKPOINT LOCATION: 0/2000028
            BACKUP METHOD: streamed
            """;

        // Act / Assert
        BackupLabel.WalStartSegment(raw).Should().Be("000000010000000000000002");
        BackupLabel.WalStartSegment("no label here").Should().BeNull();
        BackupLabel.WalStartSegment("").Should().BeNull();
    }
}
