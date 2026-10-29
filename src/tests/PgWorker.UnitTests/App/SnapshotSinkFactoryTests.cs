using PgWorker.App;
using PgWorker.Backups.EtcdExport;

namespace PgWorker.UnitTests.App;

// Фабрика цели слепков (arch/14 §6/§8): S3ClientOptions собирается из
// ОТДЕЛЬНОЙ пары Snapshots:Export:S3 (env PGW_SNAPSHOTS_S3_*) — не из
// Backups:S3 (источник правды о DR-слепках не живёт в восстанавливаемом etcd).
public class SnapshotSinkFactoryTests
{
    // Тестовая пара S3-ключей слепков (литералы — только в константах).
    private const string TestAccessKey = "snap-ak";
    private const string TestSecretKey = "snap-sk";

    // AAA: собранные S3ClientOptions равны полям слепочной пары.
    [Fact]
    public void Create_UsesOwnPair_NotBackupsSection()
    {
        // Arrange — разные значения пары слепков и секции бэкапов.
        var opts = new PgWorkerOptions();
        opts.Snapshots.Export.S3 = new SnapshotExportS3Options
        {
            Endpoint = "http://owns3:9000",
            Bucket = "pgworker-etcd-snapshots",
            AccessKey = TestAccessKey,
            SecretKey = TestSecretKey,
            Region = "us-east-1",
            CaBundlePath = "/tls/ca.pem",
        };
        opts.Backups.S3 = new BackupsS3Options { Bucket = "pgworker-backups" };

        // Act
        var target = EtcdSnapshotSinkFactory.Create(opts);

        // Assert — цель слепков, не бэкапов.
        target.Endpoint.Should().Be("http://owns3:9000");
        target.Bucket.Should().Be("pgworker-etcd-snapshots");
        target.AccessKey.Should().Be(TestAccessKey);
        target.SecretKey.Should().Be(TestSecretKey);
        target.Region.Should().Be("us-east-1");
        target.CaBundlePath.Should().Be("/tls/ca.pem");
    }
}
