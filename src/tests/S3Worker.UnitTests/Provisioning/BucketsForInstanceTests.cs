using FluentAssertions;
using S3Worker.Provisioning.Processes;
using Xunit;

namespace S3Worker.UnitTests.Provisioning;

// BucketsForInstance: состав бакетов инстанса S3 — per-install всегда,
// etcd-backup бакет только для etcd-backup кластеров.
public class BucketsForInstanceTests
{
    [Fact]
    public void OrdinaryCluster_OnlyPerInstall()
    {
        // Act: обычный кластер (etcdBackup = false).
        var buckets = S3ProcessCommon.BucketsForInstance("pgworker-backups", etcdBackup: false);

        // Assert: ровно один бакет — per-install.
        buckets.Should().Equal("pgworker-backups");
    }

    [Fact]
    public void EtcdBackupCluster_BothBuckets()
    {
        // Act: etcd-backup кластер.
        var buckets = S3ProcessCommon.BucketsForInstance("pgworker-backups", etcdBackup: true);

        // Assert: per-install первым, etcd-слепковый вторым (имя — из Shared.Core).
        buckets.Should().Equal("pgworker-backups", EtcdSnapshotBucket.Name);
    }

    [Fact]
    public void EmptyPerInstall_StillListed()
    {
        // Act: пустая строка per-install-бакета.
        var buckets = S3ProcessCommon.BucketsForInstance("", etcdBackup: false);

        // Assert: возвращается как есть — валидация имени вне функции.
        buckets.Should().Equal("");
    }
}
