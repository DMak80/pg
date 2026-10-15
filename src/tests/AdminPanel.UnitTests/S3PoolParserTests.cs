using AdminPanel.Etcd.S3;
using FluentAssertions;
using Xunit;
using Shared.Etcd.Client;

namespace AdminPanel.UnitTests;

// Парсер домена /s3/clusters/ (t36, adminpanel/02 §12): config/nodes/endpoints
// → S3PoolInfo; битый config — parseError; отсутствие state — Active.
public class S3PoolParserTests
{
    // AAA: config + ноды + endpoints → пул собран (Active, порядок endpoints).
    [Fact]
    public void Parse_ConfigNodesEndpoints_BuildsPool()
    {
        // Arrange — Active-пул из двух нод (одна RUNNING, одна заявлена).
        var kvs = new List<Kv>
        {
            new("/s3/clusters/backups/config",
                """{"platform":"owns3","nodes":2,"secret_profile":"backups-main","created_unix":1}""", 1),
            new("/s3/clusters/backups/nodes/east1/state", "\"RUNNING\"", 2),
            new("/s3/clusters/backups/nodes/east1/placement", """{"docker_host":"h1"}""", 3),
            new("/s3/clusters/backups/nodes/east2/placement", """{"docker_host":"h2"}""", 4),
            new("/s3/clusters/backups/endpoints", "h1:18001,h2:18002", 5),
        };

        // Act
        var (pools, errors, unknown) = S3PoolParser.Parse(kvs);

        // Assert
        errors.Should().BeEmpty();
        unknown.Should().Be(0);
        var pool = pools.Should().ContainSingle().Subject;
        pool.Name.Should().Be("backups");
        pool.SecretProfile.Should().Be("backups-main");
        pool.Active.Should().BeTrue();
        pool.Nodes.Should().HaveCount(2);
        pool.Nodes[0].Name.Should().Be("east1");
        pool.Nodes[0].State.Should().Be("RUNNING");
        pool.Nodes[1].State.Should().BeNull("placement есть, state нет — заявлена");
        pool.Endpoints.Should().Equal("h1:18001", "h2:18002");
    }

    // AAA: битый config — parseError-запись, пул пропущен (тик жив).
    [Fact]
    public void Parse_BitConfig_SkipWithParseError()
    {
        // Arrange
        var kvs = new List<Kv>
        {
            new("/s3/clusters/broken/config", "{не json", 1),
            new("/s3/clusters/good/config",
                """{"platform":"owns3","nodes":0,"secret_profile":"p","created_unix":1}""", 2),
        };

        // Act
        var (pools, errors, _) = S3PoolParser.Parse(kvs);

        // Assert
        errors.Should().Contain(e => e.Contains("/s3/clusters/broken/config"));
        pools.Select(p => p.Name).Should().Equal("good");
    }

    // AAA: config с state=TO_REMOVE — не Active.
    [Fact]
    public void Parse_ToRemove_NotActive()
    {
        // Arrange
        var kvs = new List<Kv>
        {
            new("/s3/clusters/doomed/config",
                """{"platform":"owns3","nodes":0,"secret_profile":"p","created_unix":1,"state":"TO_REMOVE"}""", 1),
        };

        // Act
        var (pools, _, _) = S3PoolParser.Parse(kvs);

        // Assert
        pools.Single().Active.Should().BeFalse();
    }

    // AAA: endpoints-строка → массив по порядку; битые элементы — долой.
    [Fact]
    public void Parse_EndpointsOrderAndGarbage()
    {
        // Arrange
        var kvs = new List<Kv>
        {
            new("/s3/clusters/p/config", "{}", 1),
            new("/s3/clusters/p/endpoints", "h2:18002,,bad,h1:18001", 2),
        };

        // Act
        var (pools, _, _) = S3PoolParser.Parse(kvs);

        // Assert — порядок строки сохранён (порядок чтения ReadAll, arch/19 §5).
        pools.Single().Endpoints.Should().Equal("h2:18002", "h1:18001");
    }
}
