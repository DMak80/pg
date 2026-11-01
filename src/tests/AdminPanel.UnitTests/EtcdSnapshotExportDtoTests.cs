using AdminPanel.Api.Inspection;
using AdminPanel.Core;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Маппер статуса DR-выгрузки etcd-снапшотов в DTO грани (ключ
// /etcdworker/snapshots, поля enabled в формате нет): 1:1 перенос полей;
// null → null (ключа нет).
public class EtcdSnapshotExportDtoTests
{
    // AAA: маппер статуса выгрузки — 1:1; null → null (ключа нет)
    [Fact]
    public void MapEtcdSnapshots_Full_OneToOne()
    {
        // Arrange
        var info = new EtcdSnapshotExportInfo("OK", 1759330000, "etcd/snapshot-x.db", "abc123", 2048, 360, null);

        // Act
        var dto = BackupStorageMappers.MapEtcdSnapshots(info);

        // Assert
        dto.Should().BeEquivalentTo(new EtcdSnapshotsDto("OK", 1759330000, "etcd/snapshot-x.db", "abc123", 2048, 360, null));
    }

    [Fact]
    public void MapEtcdSnapshots_Null_NoKey()
    {
        // Act/Assert
        BackupStorageMappers.MapEtcdSnapshots(null).Should().BeNull();
    }
}
