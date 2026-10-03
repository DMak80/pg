using PgWorker.Backups;
using PgWorker.Backups.EtcdExport;
using FluentAssertions;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Ретенция S3-выгрузки etcd (t08, arch/19 §5): пары .db+.meta.json сортируются
// по id (таймстемп лексикографически = хронологически), старше N последних —
// к удалению; одиночный .meta.json без .db — мусор; guard ≥1 полной пары.
public class EtcdExportRetentionTests
{
    private static S3ObjectInfo Obj(string key, long size = 100) => new(key, size, DateTimeOffset.UnixEpoch);

    // AAA: 3 пары при N=2 — старейшая пара (db+meta) к удалению, ровно 2 остаются
    [Fact]
    public void Select_сверх_лимита_старейшая_пара_к_удалению()
    {
        // Arrange
        var objects = new[] { Obj("etcd/snapshot-20261001-010000.db"), Obj("etcd/snapshot-20261001-010000.meta.json"),
            Obj("etcd/snapshot-20261001-020000.db"), Obj("etcd/snapshot-20261001-020000.meta.json"),
            Obj("etcd/snapshot-20261001-030000.db"), Obj("etcd/snapshot-20261001-030000.meta.json") };

        // Act
        var stale = EtcdExportRetention.Select(objects, retentionObjects: 2);

        // Assert
        stale.Should().BeEquivalentTo(new[] { "etcd/snapshot-20261001-010000.db", "etcd/snapshot-20261001-010000.meta.json" });
    }

    // AAA: пар не больше лимита — ничего не удаляется (guard ≥1 по построению)
    [Fact]
    public void Select_в_пределах_лимита_пусто()
    {
        // Arrange
        var objects = new[] { Obj("etcd/snapshot-20261001-010000.db"), Obj("etcd/snapshot-20261001-010000.meta.json") };

        // Act
        var stale = EtcdExportRetention.Select(objects, 28);

        // Assert
        stale.Should().BeEmpty();
    }

    // AAA: одиночный .meta.json без .db — мусор ретенции, сносится даже в пределах лимита
    [Fact]
    public void Select_одиночная_meta_мусор()
    {
        // Arrange
        var objects = new[] { Obj("etcd/snapshot-20261001-010000.db"), Obj("etcd/snapshot-20261001-010000.meta.json"),
            Obj("etcd/snapshot-20261001-003000.meta.json") };

        // Act
        var stale = EtcdExportRetention.Select(objects, 28);

        // Assert — пара цела, сирота-meta снесена
        stale.Should().BeEquivalentTo(new[] { "etcd/snapshot-20261001-003000.meta.json" });
    }

    // AAA: посторонние ключи префикса не трогаются
    [Fact]
    public void Select_чужие_ключи_не_трогает()
    {
        // Arrange
        var objects = new[] { Obj("etcd/README"), Obj("etcd/snapshot-20261001-010000.db"), Obj("etcd/snapshot-20261001-010000.meta.json") };

        // Act
        var stale = EtcdExportRetention.Select(objects, 1);

        // Assert
        stale.Should().NotContain("etcd/README");
    }
}
