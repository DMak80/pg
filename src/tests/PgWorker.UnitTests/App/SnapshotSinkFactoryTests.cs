using PgWorker.App;
using PgWorker.Backups.EtcdExport;

namespace PgWorker.UnitTests.App;

// Фабрика цели слепков (t36, arch/14 §6/§8): S3ClientOptions собирается из
// ОТДЕЛЬНОЙ пары Snapshots:Export:S3 (env PGW_SNAPSHOTS_S3_*) — не из
// Backups:S3 (источник правды о DR-слепках не живёт в восстанавливаемом etcd).
public class SnapshotSinkFactoryTests
{
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
            AccessKey = "snap-ak",
            SecretKey = "snap-sk",
            Region = "us-east-1",
            CaBundlePath = "/tls/ca.pem",
        };
        opts.Backups.S3 = new BackupsS3Options { Bucket = "pgworker-backups" };

        // Act
        var target = EtcdSnapshotSinkFactory.Create(opts);

        // Assert — цель слепков, не бэкапов.
        target.Endpoint.Should().Be("http://owns3:9000");
        target.Bucket.Should().Be("pgworker-etcd-snapshots");
        target.AccessKey.Should().Be("snap-ak");
        target.SecretKey.Should().Be("snap-sk");
        target.Region.Should().Be("us-east-1");
        target.CaBundlePath.Should().Be("/tls/ca.pem");
    }
}
