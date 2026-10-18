using FluentAssertions;
using PgWorker.Docker.Drivers;
using Xunit;

namespace PgWorker.UnitTests.Docker;

// Имена docker-объектов агентов (t27, arch/19 §3) — контракт between драйвера и
// процесса: per-node имена pgw-backup-wal-<C>-<X>-<N>, staging-тома больше нет.
public class BackupAgentNamesTests
{
    [Fact]
    public void Names_Canonical_PerNode()
    {
        // Arrange / Act / Assert
        BackupAgentNames.Container("shop", "shard1", "shard1a")
            .Should().Be("pgw-backup-wal-shop-shard1-shard1a");
        BackupAgentNames.Prefix("shop").Should().Be("pgw-backup-wal-shop-");
    }

    [Fact]
    public void Names_StartWithPgw_AndDistinctFromNodes()
    {
        // Arrange / Act
        var container = BackupAgentNames.Container("shop", "shard1", "shard1a");

        // Assert — ListNodeObjectsAsync(“pgw-shop-”) фильтрует их (не ноды кластера)
        container.StartsWith("pgw-", StringComparison.Ordinal).Should().BeTrue();
        container.StartsWith("pgw-shop-", StringComparison.Ordinal).Should().BeFalse();
    }
}
