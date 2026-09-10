using AdminPanel.Core;
using AdminPanel.Etcd.Client;
using AdminPanel.Etcd.Parsing;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Панельный парсер /pgworker/backups/ (t02): never-семантика — шард с ключами
// полных, но без COMPLETED, попадает в словарь со значением null (не молчит:
// молчание правила — только для ПУСТОГО префикса кластера).
public class BackupsParserTests
{
    // AAA: ключи есть, COMPLETED нет → ShardLastCompletedUnix[s1] = null
    [Fact]
    public void Parse_ShardWithKeysButNoCompleted_NullLastCompleted()
    {
        // Arrange — RUNNING + FAILED (первое включение подсистемы), COMPLETED нет
        var kvs = new List<Kv>
        {
            new("/pgworker/backups/demo/s1/full/20260910030000Z",
                "{\"state\":\"RUNNING\",\"node\":\"n\",\"role\":\"replica\",\"started_unix\":1757463600}", 1),
            new("/pgworker/backups/demo/s1/full/20260910030100Z",
                "{\"state\":\"FAILED\",\"node\":\"n\",\"role\":\"replica\",\"started_unix\":1757463660}", 2),
        };

        // Act
        var result = BackupsParser.Parse(kvs);

        // Assert — шард в словаре (never-алерт реализуем), значение null
        result.Errors.Should().BeEmpty();
        var cluster = result.Clusters.Should().ContainSingle().Subject;
        cluster.Cluster.Should().Be("demo");
        cluster.FullMaxAgeSec.Should().BeNull("policy-ключа нет — панельный дефолт в правиле");
        cluster.ShardLastCompletedUnix.Should().ContainKey("s1")
            .WhoseValue.Should().BeNull();
    }

    // AAA: несколько COMPLETED — в словаре максимум finished_unix
    [Fact]
    public void Parse_MultipleCompleted_LatestFinishedUnix()
    {
        // Arrange — два COMPLETED с разными finished_unix
        var kvs = new List<Kv>
        {
            new("/pgworker/backups/demo/s1/full/20260908030000Z",
                "{\"state\":\"COMPLETED\",\"node\":\"n\",\"role\":\"replica\",\"started_unix\":1757290800,\"finished_unix\":1757294400}", 1),
            new("/pgworker/backups/demo/s1/full/20260909030000Z",
                "{\"state\":\"COMPLETED\",\"node\":\"n\",\"role\":\"replica\",\"started_unix\":1757377200,\"finished_unix\":1757380800}", 2),
        };

        // Act
        var result = BackupsParser.Parse(kvs);

        // Assert
        result.Clusters.Single().ShardLastCompletedUnix["s1"].Should().Be(1757380800);
    }

    // AAA: COMPLETED без finished_unix (битый) — шард регистрируется, значение null
    [Fact]
    public void Parse_CompletedWithoutFinishedUnix_ShardKeptValueNull()
    {
        // Arrange
        var kvs = new List<Kv>
        {
            new("/pgworker/backups/demo/s1/full/20260908030000Z",
                "{\"state\":\"COMPLETED\",\"node\":\"n\",\"role\":\"replica\",\"started_unix\":1757290800}", 1),
        };

        // Act
        var result = BackupsParser.Parse(kvs);

        // Assert — шард в словаре со значением null (never-ветка правила)
        result.Clusters.Single().ShardLastCompletedUnix.Should().ContainKey("s1")
            .WhoseValue.Should().BeNull();
    }

    // AAA: policy кластера парсится в FullMaxAgeSec; без него — null
    [Fact]
    public void Parse_PolicyKey_FullMaxAgeSec()
    {
        // Arrange
        var kvs = new List<Kv>
        {
            new("/pgworker/backups/demo/policy",
                "{\"retention\":{\"days\":7,\"weeks\":4,\"months\":6},\"full_max_age_sec\":43200,\"verify\":{\"on_create\":true}}", 1),
            new("/pgworker/backups/demo/s1/full/20260908030000Z",
                "{\"state\":\"COMPLETED\",\"node\":\"n\",\"role\":\"replica\",\"started_unix\":1757290800,\"finished_unix\":1757294400}", 2),
        };

        // Act
        var result = BackupsParser.Parse(kvs);

        // Assert
        result.Errors.Should().BeEmpty();
        result.Clusters.Single().FullMaxAgeSec.Should().Be(43200);
    }

    // AAA: битый JSON — KeyParseError, запись пропущена (тик не роняет)
    [Fact]
    public void Parse_MalformedJson_KeyParseError()
    {
        // Arrange
        var kvs = new List<Kv>
        {
            new("/pgworker/backups/demo/s1/full/20260908030000Z", "not-json", 1),
        };

        // Act
        var result = BackupsParser.Parse(kvs);

        // Assert
        result.Errors.Should().ContainSingle().Which.Key.Should().Be("/pgworker/backups/demo/s1/full/20260908030000Z");
        result.Clusters.Should().BeEmpty();
    }

    // AAA: пустой префикс — Clusters пуст (правило молчит: подсистема не включена)
    [Fact]
    public void Parse_EmptyKvs_NoClusters()
    {
        // Arrange / Act
        var result = BackupsParser.Parse([]);

        // Assert
        result.Clusters.Should().BeEmpty();
        result.Errors.Should().BeEmpty();
    }
}
