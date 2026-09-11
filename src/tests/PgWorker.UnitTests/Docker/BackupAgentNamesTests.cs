using FluentAssertions;
using PgWorker.Docker.Drivers;
using Xunit;

namespace PgWorker.UnitTests.Docker;

// Имена docker-объектов агентов (arch/19 §3) — контракт between драйвера и процесса.
public class BackupAgentNamesTests
{
    [Fact]
    public void Имена_канонические()
    {
        // Arrange / Act / Assert
        BackupAgentNames.Container("shop", "shard1").Should().Be("pgw-backup-wal-shop-shard1");
        BackupAgentNames.Volume("shop", "shard1").Should().Be("pgw-backup-wal-shop-shard1-staging");
        BackupAgentNames.Prefix("shop").Should().Be("pgw-backup-wal-shop-");
    }

    [Fact]
    public void Имена_начинаются_с_pgw_и_отличимы_от_нод()
    {
        // Arrange / Act
        var container = BackupAgentNames.Container("shop", "shard1");

        // Assert — ListNodeObjectsAsync(“pgw-shop-”) фильтрует их (не ноды кластера)
        container.StartsWith("pgw-", StringComparison.Ordinal).Should().BeTrue();
        container.StartsWith("pgw-shop-", StringComparison.Ordinal).Should().BeFalse();
    }
}
