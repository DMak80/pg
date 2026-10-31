using AdminPanel.Core;
using Shared.Etcd.Client;
using AdminPanel.Etcd.Parsing;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Парсер координации воркера (u14, adminpanel/02 §3): все ключи есть; пусто
// (нет ключей → пустая координация, НЕ ошибка); битый leader → parseError;
// claims/instances с since_unix и без. AAA.
public class CoordinationParserTests
{
    private const string Root = "/pgworker";

    [Fact]
    public void Parse_AllKeysPresent_LeaderClaimsInstances()
    {
        // Arrange — leader + claims двух кластеров + instance с since
        var leaderKv = new Kv($"{Root}/leader", """{"instance":"pgw-1","since_unix":1761900000}""", 1);
        var claims = new[]
        {
            new Kv($"{Root}/claims/demo", """{"instance":"pgw-1","since_unix":1761900100}""", 2),
            new Kv($"{Root}/claims/other", """{"instance":"pgw-2"}""", 3),
        };
        var instances = new[]
        {
            new Kv($"{Root}/instances/pgw-1", """{"since_unix":1761900000}""", 4),
            new Kv($"{Root}/instances/pgw-2", """{"since_unix":1761900050}""", 5),
        };

        // Act
        var (coordination, errors) = CoordinationParser.Parse(Root, leaderKv, claims, instances);

        // Assert
        errors.Should().BeEmpty();
        coordination!.LeaderInstance.Should().Be("pgw-1");
        coordination.Claims.Should().HaveCount(2);
        coordination.Claims.First(c => c.Cluster == "demo").Instance.Should().Be("pgw-1");
        coordination.Claims.First(c => c.Cluster == "demo").SinceUnix.Should().Be(1761900100);
        coordination.Claims.First(c => c.Cluster == "other").SinceUnix.Should().BeNull();
        coordination.Instances.Select(i => i.InstanceId).Should().Equal("pgw-1", "pgw-2");
    }

    [Fact]
    public void Parse_NoKeys_EmptyCoordination_NoErrors()
    {
        // Arrange / Act — префиксы пусты (воркер ещё не поднимался)
        var (coordination, errors) = CoordinationParser.Parse(Root, null, [], []);

        // Assert — пустая координация, не ошибка (u14-толерантность)
        errors.Should().BeEmpty();
        coordination.Should().NotBeNull();
        coordination!.LeaderInstance.Should().BeNull();
        coordination.Claims.Should().BeEmpty();
        coordination.Instances.Should().BeEmpty();
    }

    [Fact]
    public void Parse_BrokenLeader_ParseError_LeaderNull()
    {
        // Arrange — leader жив, но JSON битый
        var leaderKv = new Kv($"{Root}/leader", "{broken", 1);
        var claims = new[]
        {
            new Kv($"{Root}/claims/demo", """{"instance":"pgw-1"}""", 2),
        };

        // Act
        var (coordination, errors) = CoordinationParser.Parse(Root, leaderKv, claims, []);

        // Assert — тик не роняет: leader=null, claims разобраны
        errors.Should().ContainSingle().Which.Key.Should().Be($"{Root}/leader");
        coordination!.LeaderInstance.Should().BeNull();
        coordination.Claims.Should().ContainSingle();
    }

    [Fact]
    public void Parse_InstanceWithoutBody_SinceNull_NoError()
    {
        // Arrange — значение instances-ключа может быть пустым (канон допускает)
        var instances = new[]
        {
            new Kv($"{Root}/instances/pgw-1", "", 1),
        };

        // Act
        var (coordination, errors) = CoordinationParser.Parse(Root, null, [], instances);

        // Assert — инстанс учтён без since
        errors.Should().BeEmpty();
        var instance = coordination!.Instances.Should().ContainSingle().Subject;
        instance.InstanceId.Should().Be("pgw-1");
        instance.SinceUnix.Should().BeNull();
    }

    [Fact]
    public void Parse_BrokenClaim_ParseError()
    {
        // Arrange — claim без instance
        var claims = new[]
        {
            new Kv($"{Root}/claims/demo", """{"since_unix":1}""", 1),
        };

        // Act
        var (coordination, errors) = CoordinationParser.Parse(Root, null, claims, []);

        // Assert
        errors.Should().ContainSingle().Which.Key.Should().Be($"{Root}/claims/demo");
        coordination!.Claims.Should().BeEmpty();
    }
}
