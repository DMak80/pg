using AdminPanel.Core;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Агрегация list-v2 bucket в дерево <C>/<X> (t08, AC2): полные full/<id>/,
// WAL-сегменты и .history, корни вне формы <C>/<X>/… — foreign-факты.
public class MinioInventoryTests
{
    // AAA: полный (2 объекта) + WAL (2 сегмента, 1 история) → дерево и суммы
    [Fact]
    public void Build_FullsWalHistory_AggregatesTree()
    {
        // Arrange — полный с pg_wal-набором, два сегмента и одна история
        var objects = new List<MinioObject>
        {
            new("demo/s1/full/20260913a/base.tar", 100, 1000),
            new("demo/s1/full/20260913a/pg_wal/seg", 16, 1001),
            new("demo/s1/wal/000000010000000000000001", 16, 1002),
            new("demo/s1/wal/000000010000000000000002", 16, 1003),
            new("demo/s1/wal/00000002.history", 4, 1004),
        };

        // Act
        var inv = MinioInventory.Build(objects);

        // Assert — шард s1: полный 116 байт/2 объекта, WAL 36 = 2+1, суммы bucket
        inv.Clusters.Should().ContainSingle();
        var cluster = inv.Clusters[0];
        cluster.Cluster.Should().Be("demo");
        var shard = cluster.Shards.Should().ContainSingle().Subject;
        shard.Cluster.Should().Be("demo");
        shard.Shard.Should().Be("s1");
        shard.SizeBytes.Should().Be(152);
        var full = shard.Fulls.Should().ContainSingle().Subject;
        full.Id.Should().Be("20260913a");
        full.SizeBytes.Should().Be(116);
        full.ObjectCount.Should().Be(2);
        shard.Wal.Should().NotBeNull();
        shard.Wal!.SegmentCount.Should().Be(2);
        shard.Wal.HistoryCount.Should().Be(1);
        shard.Wal.SizeBytes.Should().Be(36);
        inv.UsedBytes.Should().Be(152);
        inv.ObjectCount.Should().Be(5);
        inv.ForeignPrefixes.Should().BeEmpty();
    }

    // AAA: корень вне формы <C>/<X>/… → foreign-префикс (факт, без вердикта)
    [Fact]
    public void Build_ForeignPrefix_CountedWithoutVerdict()
    {
        // Arrange — объект в корне bucket, короче <C>/<X>/…
        var objects = new List<MinioObject> { new("loose/file.bin", 10, 500) };

        // Act
        var inv = MinioInventory.Build(objects);

        // Assert — кластеров нет, «loose» в ForeignPrefixes, место посчитано
        inv.Clusters.Should().BeEmpty();
        inv.ForeignPrefixes.Should().ContainSingle().Which.Should().Be("loose");
        inv.UsedBytes.Should().Be(10);
        inv.ObjectCount.Should().Be(1);
    }

    // AAA: два кластера → раздельные размеры, сортировка кластеров/шардов Ordinal
    [Fact]
    public void Build_TwoClustersShards_SortedOrdinal()
    {
        // Arrange — вставка в «неправильном» порядке (b раньше a, s2 раньше s1)
        var objects = new List<MinioObject>
        {
            new("b/s1/full/f1/x", 10, 1),
            new("a/s2/full/f1/x", 20, 1),
            new("a/s1/full/f1/x", 30, 1),
            new("b/s2/full/f1/x", 40, 1),
        };

        // Act
        var inv = MinioInventory.Build(objects);

        // Assert — Ordinal-порядок, размеры по кластерам раздельно
        inv.Clusters.Select(c => c.Cluster).Should().Equal("a", "b");
        inv.Clusters[0].Shards.Select(s => s.Shard).Should().Equal("s1", "s2");
        inv.Clusters[0].SizeBytes.Should().Be(50);
        inv.Clusters[1].Shards.Select(s => s.Shard).Should().Equal("s1", "s2");
        inv.Clusters[1].SizeBytes.Should().Be(50);
        inv.UsedBytes.Should().Be(100);
    }

    // AAA: LastModifiedUnix полного/WAL — максимум по объектам префикса
    [Fact]
    public void Build_LastModified_IsMaxOverObjects()
    {
        // Arrange — у второго объекта полного самый свежий lastModified
        var objects = new List<MinioObject>
        {
            new("demo/s1/full/f1/a", 10, 100),
            new("demo/s1/full/f1/b", 10, 300),
            new("demo/s1/wal/000000010000000000000001", 10, 200),
        };

        // Act
        var inv = MinioInventory.Build(objects);

        // Assert — full → 300, wal → 200
        var shard = inv.Clusters[0].Shards[0];
        shard.Fulls[0].LastModifiedUnix.Should().Be(300);
        shard.Wal!.LastModifiedUnix.Should().Be(200);
    }

    // AAA: объект внутри шарда вне full/wal — в SizeBytes шарда, не в Fulls/Wal
    [Fact]
    public void Build_ShardExtras_CountedInShardSizeOnly()
    {
        // Arrange — «посторонний» файл рядом с полным
        var objects = new List<MinioObject>
        {
            new("demo/s1/other.txt", 7, 1),
            new("demo/s1/full/f1/a", 10, 1),
        };

        // Act
        var inv = MinioInventory.Build(objects);

        // Assert — extra в размере шарда, в full/WAL не попал (без вердикта)
        var shard = inv.Clusters[0].Shards[0];
        shard.SizeBytes.Should().Be(17);
        shard.Fulls.Should().ContainSingle().Which.SizeBytes.Should().Be(10);
        shard.Wal.Should().BeNull();
    }

    // AAA (ревью Fix 2): S3-«directory marker»-ключи (пустые сегменты, завершающий
    // '/') — не агрегируются: без ложного foreign «demo» и полного с пустым Id
    [Fact]
    public void Build_DirectoryMarkers_Skipped()
    {
        // Arrange — маркеры «папок» MinIO (консоль/mc) + один реальный объект
        var objects = new List<MinioObject>
        {
            new("demo/", 0, 1),
            new("demo/s1/full/", 0, 1),
            new("demo/s1/full/b1/base.tar", 10, 2),
            new("loose/", 0, 1),
        };

        // Act
        var inv = MinioInventory.Build(objects);

        // Assert — маркеры не считаются ни объектами, ни foreign-корнями;
        // full ровно один, с Id «b1»
        inv.ForeignPrefixes.Should().BeEmpty();
        var shard = inv.Clusters.Should().ContainSingle().Subject
            .Shards.Should().ContainSingle().Subject;
        shard.Fulls.Select(f => f.Id).Should().Equal("b1");
        inv.UsedBytes.Should().Be(10);
        inv.ObjectCount.Should().Be(1);
    }
}
