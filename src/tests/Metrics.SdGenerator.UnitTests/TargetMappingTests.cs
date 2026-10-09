using FluentAssertions;
using Metrics.SdGenerator;
using Shared.Etcd.Client;

namespace Metrics.SdGenerator.UnitTests;

// Маппинг portalloc → file_sd-группы: лейблы/таргеты, толерантность к битым
// записям, детерминизм порядка (spec §3.2, §6.1-1).
public class TargetMappingTests
{
    private static Kv KvOf(string key, string value) => new(key, value, ModRevision: 1);

    private static (List<SdTargetGroup> Groups, List<string> Warnings) MapOf(params Kv[] kvs)
    {
        // Arrange
        var warnings = new List<string>();

        // Act
        var groups = TargetMapping.Map(kvs, (cluster, error) => warnings.Add($"{cluster}: {error}"));

        // Assert (формы возвращает вызывающему — проверяет сам тест)
        return (groups.ToList(), warnings);
    }

    [Fact]
    public void Map_SingleNode_BuildsTargetGroup()
    {
        // Arrange
        var kv = KvOf("/pgworker/portalloc/c1",
            """{"shard1/shard1a":{"host":"h1","pg":5432,"patroni":8008,"doorman":6432}}""");

        // Act
        var (groups, warnings) = MapOf(kv);

        // Assert
        groups.Should().ContainSingle()
            .Which.Should().Be(new SdTargetGroup("h1:8008", "c1", "shard1", "shard1a"));
        warnings.Should().BeEmpty();
    }

    [Fact]
    public void Map_SuppressedNode_SkipsWithoutWarning()
    {
        // Arrange: вторая запись словаря усыптлена (patroni=0 — штатная семантика arch/14 §2.4)
        var kv = KvOf("/pgworker/portalloc/c1",
            """{"shard1/shard1a":{"host":"h1","pg":5432,"patroni":8008,"doorman":6432},"shard1/shard1b":{"host":"h2","pg":5432,"patroni":0,"doorman":6432}}""");

        // Act
        var (groups, warnings) = MapOf(kv);

        // Assert
        groups.Should().ContainSingle().Which.Node.Should().Be("shard1a");
        warnings.Should().BeEmpty();
    }

    [Fact]
    public void Map_BrokenJson_SkipsWholeKeyWithWarning()
    {
        // Arrange
        var kv = KvOf("/pgworker/portalloc/c1", """{"shard1/""");

        // Act
        var (groups, warnings) = MapOf(kv);

        // Assert
        groups.Should().BeEmpty();
        warnings.Should().ContainSingle().Which.Should().Contain("c1");
    }

    [Fact]
    public void Map_UnknownFields_Tolerated()
    {
        // Arrange: поле object (усытовление arch/14 §2.4) и незнакомые поля — игнорируются
        var kv = KvOf("/pgworker/portalloc/c1",
            """{"shard1/shard1a":{"host":"h1","pg":5432,"patroni":8008,"doorman":6432,"object":"pgw-x","extra":true}}""");

        // Act
        var (groups, warnings) = MapOf(kv);

        // Assert
        groups.Should().ContainSingle().Which.Target.Should().Be("h1:8008");
        warnings.Should().BeEmpty();
    }

    [Fact]
    public void Map_EmptyPrefix_SerializesEmptyArray()
    {
        // Arrange
        var kvs = Array.Empty<Kv>();

        // Act
        var (groups, warnings) = MapOf(kvs);
        var json = TargetMapping.Serialize(groups);

        // Assert
        groups.Should().BeEmpty();
        warnings.Should().BeEmpty();
        json.Should().Be("[]");
    }

    [Fact]
    public void Map_MixedClusters_IsDeterministicSorted()
    {
        // Arrange: 2 кластера × 2 шарда × 2 ноды в перемешанном (захардкоженном) порядке
        static string Entry(string host) => $$"""{"host":"{{host}}","pg":5432,"patroni":8008,"doorman":6432}""";
        var kvs = new[]
        {
            KvOf("/pgworker/portalloc/c2",
                $$"""{"s2/n2b":{{Entry("h-c2-s2-b")}},"s1/n1a":{{Entry("h-c2-s1-a")}}}"""),
            KvOf("/pgworker/portalloc/c1",
                $$"""{"s2/n2a":{{Entry("h-c1-s2-a")}},"s1/n1b":{{Entry("h-c1-s1-b")}}}"""),
            KvOf("/pgworker/portalloc/c2",
                $$"""{"s1/n1b":{{Entry("h-c2-s1-b")}},"s2/n2a":{{Entry("h-c2-s2-a")}}}"""),
            KvOf("/pgworker/portalloc/c1",
                $$"""{"s1/n1a":{{Entry("h-c1-s1-a")}},"s2/n2b":{{Entry("h-c1-s2-b")}}}"""),
        };

        // Act
        var groups = TargetMapping.Map(kvs, (_, _) => { });
        var again = TargetMapping.Map(kvs, (_, _) => { });

        // Assert
        var expected = new[]
        {
            new SdTargetGroup("h-c1-s1-a:8008", "c1", "s1", "n1a"),
            new SdTargetGroup("h-c1-s1-b:8008", "c1", "s1", "n1b"),
            new SdTargetGroup("h-c1-s2-a:8008", "c1", "s2", "n2a"),
            new SdTargetGroup("h-c1-s2-b:8008", "c1", "s2", "n2b"),
            new SdTargetGroup("h-c2-s1-a:8008", "c2", "s1", "n1a"),
            new SdTargetGroup("h-c2-s1-b:8008", "c2", "s1", "n1b"),
            new SdTargetGroup("h-c2-s2-a:8008", "c2", "s2", "n2a"),
            new SdTargetGroup("h-c2-s2-b:8008", "c2", "s2", "n2b"),
        };
        groups.Should().Equal(expected);
        again.Should().Equal(groups);
    }

    [Fact]
    public void Serialize_SingleGroup_HasExactFileSdFormat()
    {
        // Arrange
        var groups = new[] { new SdTargetGroup("h1:8008", "c1", "shard1", "shard1a") };

        // Act
        var json = TargetMapping.Serialize(groups);

        // Assert: порядок полей зафиксирован — targets, labels{cluster,shard,node}
        json.Should().Be(
            """[{"targets":["h1:8008"],"labels":{"cluster":"c1","shard":"shard1","node":"shard1a"}}]""");
    }

    [Fact]
    public void Map_ForeignKeyPrefix_IgnoredSilently()
    {
        // Arrange: ключ вне префикса portalloc (чужой контур) — не наша запись
        var kv = KvOf("/pgworker/portallocX/c1",
            """{"shard1/shard1a":{"host":"h1","pg":5432,"patroni":8008,"doorman":6432}}""");

        // Act
        var (groups, warnings) = MapOf(kv);

        // Assert
        groups.Should().BeEmpty();
        warnings.Should().BeEmpty();
    }

    // AAA (t15 ревизия 3, arch/18 §2.5): запись с alias → сетевой таргет
    // <alias>:8008 (контейнерный порт Patroni REST); порты host-публикации
    // записи НЕ участвуют (patroni=18008, таргет — :8008).
    [Fact]
    public void Map_WithAlias_NetworkTarget()
    {
        // Arrange: запись единой сети контура — alias = docker-имя ноды.
        var kv = KvOf("/pgworker/portalloc/c1",
            """{"s1/s1a":{"host":"h1","pg":15432,"patroni":18008,"doorman":16432,"alias":"pgw-c1-s1-s1a"}}""");

        // Act
        var (groups, warnings) = MapOf(kv);

        // Assert: сетевой таргет на контейнерном порту, не host-публикация.
        groups.Should().ContainSingle()
            .Which.Should().Be(new SdTargetGroup("pgw-c1-s1-s1a:8008", "c1", "s1", "s1a"));
        warnings.Should().BeEmpty();
    }

    // AAA (t15 ревизия 3): запись без alias (легаси/усыновлённая) — advertised
    // host:patroni по host-публикации (деградационная ветка той же функции).
    [Fact]
    public void Map_WithoutAlias_AdvertisedTarget()
    {
        // Arrange: та же запись без alias.
        var kv = KvOf("/pgworker/portalloc/c1",
            """{"s1/s1a":{"host":"h1","pg":15432,"patroni":18008,"doorman":16432}}""");

        // Act
        var (groups, warnings) = MapOf(kv);

        // Assert: advertised-таргет по записи portalloc.
        groups.Should().ContainSingle().Which.Target.Should().Be("h1:18008");
        warnings.Should().BeEmpty();
    }

    // AAA (t15 ревизия 3): смешанный контур — per-node правило покомпонентно
    // (alias-запись сетевая, без-alias — advertised).
    [Fact]
    public void Map_MixedContour_PerNode()
    {
        // Arrange: один ключ — запись с alias и запись без (переходный контур).
        var kv = KvOf("/pgworker/portalloc/c1",
            """{"s1/s1a":{"host":"h1","pg":15432,"patroni":18008,"doorman":16432,"alias":"pgw-c1-s1-s1a"},"s1/s1b":{"host":"h2","pg":15433,"patroni":18009,"doorman":16433}}""");

        // Act
        var (groups, warnings) = MapOf(kv);

        // Assert: каждая запись своей веткой.
        groups.Should().HaveCount(2);
        groups.Should().Contain(g => g.Target == "pgw-c1-s1-s1a:8008" && g.Node == "s1a");
        groups.Should().Contain(g => g.Target == "h2:18009" && g.Node == "s1b");
        warnings.Should().BeEmpty();
    }

    // AAA (t15 ревизия 3): фильтр patroni <= 0 (усыплены) действует и в сетевой
    // ветке — alias не оживляет усыпленную ноду, warn не вызывается.
    [Fact]
    public void Map_AliasSuspended_Skipped()
    {
        // Arrange: усыпленная нода с alias.
        var kv = KvOf("/pgworker/portalloc/c1",
            """{"s1/s1a":{"host":"h1","pg":15432,"patroni":0,"doorman":0,"alias":"pgw-c1-s1-s1a"}}""");

        // Act
        var (groups, warnings) = MapOf(kv);

        // Assert: группы нет, тихий skip.
        groups.Should().BeEmpty();
        warnings.Should().BeEmpty();
    }

    // AAA (t15 ревизия 3): пустой alias (""/whitespace) — отсутствие:
    // advertised-ветка (запись не несёт сетевой идентичности).
    [Fact]
    public void Map_AliasEmpty_FallsBack()
    {
        // Arrange: alias — пустая строка.
        var kv = KvOf("/pgworker/portalloc/c1",
            """{"s1/s1a":{"host":"h1","pg":15432,"patroni":18008,"doorman":16432,"alias":""}}""");

        // Act
        var (groups, warnings) = MapOf(kv);

        // Assert: advertised-таргет.
        groups.Should().ContainSingle().Which.Target.Should().Be("h1:18008");
        warnings.Should().BeEmpty();
    }

    // AAA (t15 ревизия 3): сетевой таргет сериализуется тем же file_sd-форматом.
    [Fact]
    public void Serialize_NetworkTarget_Format()
    {
        // Arrange: группа с сетевым таргетом.
        var groups = new[] { new SdTargetGroup("pgw-c1-s1-s1a:8008", "c1", "s1", "s1a") };

        // Act
        var json = TargetMapping.Serialize(groups);

        // Assert: формат не изменился — targets, labels{cluster,shard,node}.
        json.Should().Be(
            """[{"targets":["pgw-c1-s1-s1a:8008"],"labels":{"cluster":"c1","shard":"s1","node":"s1a"}}]""");
    }
}
