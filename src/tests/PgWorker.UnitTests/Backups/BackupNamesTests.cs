namespace PgWorker.UnitTests.Backups;

// Канонические имена restore-подсистемы (arch/backups/01-full-backup.md/§4): ключи etcd —
// детерминированы; docker-имён джобов бэкапов больше нет (+: операции
// исполняют приёмники WalReceiver нод).
public class BackupNamesTests
{
    // AAA: restore-ключ etcd канона arch/backups/README.md §4.
    [Fact]
    public void RestoreKey_CanonicalPath()
    {
        // Act
        var key = BackupNames.RestoreKey("c1", "shard1", "20260911120000Z");

        // Assert
        key.Should().Be("/pgworker/backups/c1/shard1/restore/20260911120000Z");
    }

    // AAA: drill-ключ etcd канона arch/backups/README.md §4.
    [Fact]
    public void DrillKey_CanonicalPath()
    {
        // Act
        var key = BackupNames.DrillKey("c1", "shard1");

        // Assert
        key.Should().Be("/pgworker/backups/c1/shard1/drill");
    }
}
