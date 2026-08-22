using AdminPanel.Etcd.Parsing;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Парсер /service/<scope>/ (Patroni DCS): leader-варианты, members, optime, initialize, unmatched (spec §10.2).
public class ServiceParserTests
{
    // Кластеры для мэтчинга — из реальной фикстуры /clusters/ (связка тика одного снапшота).
    private static readonly IReadOnlyList<AdminPanel.Core.ClusterInfo> DemoClusters =
        ClustersParser.Parse(EtcdFixtures.LoadKv("clusters-full.json")).Clusters;

    [Fact]
    public void Parse_DemoScopes_MatchedToClusters()
    {
        // Arrange
        var kvs = EtcdFixtures.LoadKv("service-full.json");

        // Act
        var result = ServiceParser.Parse(kvs, DemoClusters);

        // Assert
        var s1 = result.Scopes.Should().ContainSingle(s => s.Scope == "demo-s1").Subject;
        s1.Cluster.Should().Be("demo");
        s1.Shard.Should().Be("s1");
        s1.Matched.Should().BeTrue();
        var s2 = result.Scopes.Should().ContainSingle(s => s.Scope == "demo-s2").Subject;
        s2.Matched.Should().BeTrue();
        result.Errors.Should().BeEmpty();
        result.UnknownKeyCount.Should().Be(0);
    }

    [Fact]
    public void Parse_LeaderJson_NameExtracted()
    {
        // Arrange
        var kvs = EtcdFixtures.LoadKv("service-full.json");

        // Act
        var result = ServiceParser.Parse(kvs, DemoClusters);

        // Assert
        result.Scopes.Single(s => s.Scope == "demo-s1").LeaderName.Should().Be("s1a");
    }

    [Fact]
    public void Parse_LeaderPlainString_Tolerated()
    {
        // Arrange — на стенде возможна строка-имя без JSON-обёртки (arch/02 §2.2)
        var kvs = EtcdFixtures.LoadKv("service-unmatched.json");

        // Act
        var result = ServiceParser.Parse(kvs, DemoClusters);

        // Assert
        result.Scopes.Single(s => s.Scope == "other-scope").LeaderName.Should().Be("plain-name");
    }

    [Fact]
    public void Parse_Members_ConnUrlHostPortRoleParsed()
    {
        // Arrange
        var kvs = EtcdFixtures.LoadKv("service-full.json");

        // Act
        var result = ServiceParser.Parse(kvs, DemoClusters);

        // Assert
        var members = result.Scopes.Single(s => s.Scope == "demo-s1").Members;
        var master = members.Should().ContainSingle(m => m.Name == "s1a").Subject;
        master.Host.Should().Be("s1a");
        master.Port.Should().Be(5432);
        master.Role.Should().Be("master");
        master.State.Should().Be("running");
        // probe-поля — t06
        master.Timeline.Should().BeNull();
        master.LagBytes.Should().BeNull();
        members.Should().Contain(m => m.Name == "s1b" && m.Role == "replica");
    }

    [Fact]
    public void Parse_OptimeAndInitialize_Filled()
    {
        // Arrange
        var kvs = EtcdFixtures.LoadKv("service-full.json");

        // Act
        var result = ServiceParser.Parse(kvs, DemoClusters);

        // Assert
        var s1 = result.Scopes.Single(s => s.Scope == "demo-s1");
        s1.OptimeLeader.Should().Be(738273634528); // число-строка LSN
        s1.Initialized.Should().BeTrue();
        s1.RawConfig.Should().Be("{\"ttl\":5,\"loop_wait\":2,\"retry_timeout\":3}");
    }

    [Fact]
    public void Parse_PartialShardSuffix_Unmatched()
    {
        // Arrange — demo-s9: префикс кластера совпал, шарда s9 нет
        var kvs = EtcdFixtures.LoadKv("service-unmatched.json");

        // Act
        var result = ServiceParser.Parse(kvs, DemoClusters);

        // Assert
        var s9 = result.Scopes.Should().ContainSingle(s => s.Scope == "demo-s9").Subject;
        s9.Matched.Should().BeFalse();
        s9.Cluster.Should().Be("demo");
        s9.Shard.Should().BeNull();
        // чужой scope — не ошибка (arch/02 §7)
        result.Errors.Should().BeEmpty();
    }

    [Fact]
    public void Parse_UnknownKey_Counted()
    {
        // Arrange
        var kvs = EtcdFixtures.LoadKv("service-unmatched.json");

        // Act
        var result = ServiceParser.Parse(kvs, DemoClusters);

        // Assert
        result.UnknownKeyCount.Should().Be(1); // /service/stray/what/is/this
    }

    [Fact]
    public void Parse_EmptyPrefix_EmptyResult()
    {
        // Arrange — /service/ не существует
        // Act
        var result = ServiceParser.Parse([], DemoClusters);

        // Assert
        result.Scopes.Should().BeEmpty();
    }
}
