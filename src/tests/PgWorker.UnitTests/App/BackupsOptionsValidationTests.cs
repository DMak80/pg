using PgWorker.App;

namespace PgWorker.UnitTests.App;

// Валидация старта S3-конфигурации бэкапов (t36, arch/19 §7/§9):
// Enabled=true → непустой Bucket (env PGW_BACKUP_S3_BUCKET) + хотя бы один
// env-профиль кредов (PGW_BACKUP_S3_PROFILES__*); слепки — собственная полная
// пара PgWorker:Snapshots:Export:S3 (env PGW_SNAPSHOTS_S3_*).
public class BackupsOptionsValidationTests
{
    // AAA: Enabled=true без Bucket — невалидно (независимо от профилей).
    [Fact]
    public void Backups_EnabledWithoutBucket_Invalid()
    {
        // Arrange — подсистема включена, bucket пуст.
        var options = new BackupsOptions { Enabled = true };

        // Act / Assert
        options.IsValid(1).Should().BeFalse();
        options.IsValid(0).Should().BeFalse();
    }

    // AAA: Enabled=true без env-профилей — невалидно (bucket задан, профилей 0).
    [Fact]
    public void Backups_EnabledWithoutProfiles_Invalid()
    {
        // Arrange — bucket задан, профилей нет.
        var options = new BackupsOptions
        {
            Enabled = true,
            S3 = new BackupsS3Options { Bucket = "pgworker-backups" },
        };

        // Act / Assert — 0 профилей = мусорный конфиг; ≥1 — валидно.
        options.IsValid(0).Should().BeFalse();
        options.IsValid(1).Should().BeTrue();
    }

    // AAA: слепки Enabled=true без пары — невалидно; полная пара — валидно.
    [Fact]
    public void Snapshots_EnabledWithoutPair_Invalid()
    {
        // Arrange — Enabled=true, секция S3 дефолтная (Endpoint пуст).
        var options = new SnapshotExportOptions { Enabled = true };

        // Act / Assert
        options.IsValid().Should().BeFalse();
        options.S3 = new SnapshotExportS3Options
        {
            Endpoint = "http://owns3:9000",
            Bucket = "pgworker-etcd-snapshots",
            AccessKey = "k",
            SecretKey = "s",
        };
        options.IsValid().Should().BeTrue();
    }

    // AAA: слепки выключены — секция S3 произвольная, валидно.
    [Fact]
    public void Snapshots_Disabled_AnyPairValid()
    {
        // Arrange — Enabled=false при пустой паре.
        var options = new SnapshotExportOptions { Enabled = false };

        // Act / Assert
        options.IsValid().Should().BeTrue();
    }
}
