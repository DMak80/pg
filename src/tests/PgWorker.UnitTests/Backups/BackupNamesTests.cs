namespace PgWorker.UnitTests.Backups;

// Канонические имена restore-подсистемы (t05, arch/19 §2/§4): ключи etcd —
// детерминированы; docker-имён джобов бэкапов больше нет (t26+: операции
// исполняют приёмники WalReceiver нод).
public class BackupNamesTests
{
    // AAA: restore-ключ etcd канона arch/19 §4.
    [Fact]
    public void RestoreKey_CanonicalPath()
    {
        // Act
        var key = BackupNames.RestoreKey("c1", "shard1", "20260911120000Z");

        // Assert
        key.Should().Be("/pgworker/backups/c1/shard1/restore/20260911120000Z");
    }

    // AAA: drill-ключ etcd канона arch/19 §4.
    [Fact]
    public void DrillKey_CanonicalPath()
    {
        // Act
        var key = BackupNames.DrillKey("c1", "shard1");

        // Assert
        key.Should().Be("/pgworker/backups/c1/shard1/drill");
    }
}
