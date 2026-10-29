using AdminPanel.Api.Inspection;
using AdminPanel.Core;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Маппер статуса выгрузки etcd-снапшотов в DTO грани (spec §3.7):
// 1:1 перенос полей; null → null (карточка «не включена»).
public class EtcdSnapshotExportDtoTests
{
    // AAA: маппер статуса выгрузки — 1:1; null → null (карточка «не включена»)
    [Fact]
    public void MapEtcdSnapshots_Full_OneToOne()
    {
        // Arrange
        var info = new EtcdSnapshotExportInfo(true, "OK", 1759330000, "etcd/snapshot-x.db", "abc123", 2048, 360, null);

        // Act
        var dto = BackupStorageMappers.MapEtcdSnapshots(info);

        // Assert
        dto.Should().BeEquivalentTo(new EtcdSnapshotsDto(true, "OK", 1759330000, "etcd/snapshot-x.db", "abc123", 2048, 360, null));
    }

    [Fact]
    public void MapEtcdSnapshots_Null_NoKey()
    {
        // Act/Assert
        BackupStorageMappers.MapEtcdSnapshots(null).Should().BeNull();
    }
}
