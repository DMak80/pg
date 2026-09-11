namespace PgWorker.UnitTests.Backups;

// Канонические имена restore-подсистемы (t05, arch/19 §2/§4): ключ etcd и
// имя контейнера джоба — детерминированы (takeover по именам).
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

    // AAA: имя restore-джоба — D1-префикс pgw-backup-restore-<C>-<X>-<id>.
    [Fact]
    public void RestoreContainerName_D1Prefix()
    {
        // Act
        var name = BackupNames.RestoreContainerName("c1", "shard1", "20260911120000Z");

        // Assert — начинается с pgw-backup-restore- (чистка D1/pgw-backup-*).
        name.Should().Be("pgw-backup-restore-c1-shard1-20260911120000Z");
    }
}
