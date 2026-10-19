using AdminPanel.Api.Inspection;
using AdminPanel.Core.S3;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Проекция пулов для грани «Хранилище бэкапов» (t36, adminpanel/02 §12.4):
// S3Snapshot → S3PoolDto (единый домен S3 — второго парсера/тика нет).
// Active — по канону arch/23 §2: только отсутствие state (= S3ClusterState.Active).
public class S3PoolMappingTests
{
    // AAA: Active-кластер из двух нод (одна RUNNING, одна заявлена без state)
    // + endpoints-строка → пул собран, порядок нод/endpoints сохранён.
    [Fact]
    public void MapPools_ActiveClusterWithNodesAndEndpoints_BuildsPool()
    {
        // Arrange — кластер без state (Active), ноды отсортированы парсером по имени.
        var snapshot = Snapshot(Cluster("backups", S3ClusterState.Active,
            endpoints: "h1:18001,h2:18002",
            nodes:
            [
                Node("east1", "RUNNING"),
                Node("east2", null),
            ]));

        // Act
        var pools = S3Mappers.MapPools(snapshot.Clusters);

        // Assert
        var pool = pools.Should().ContainSingle().Subject;
        pool.Name.Should().Be("backups");
        pool.Active.Should().BeTrue();
        pool.Nodes.Should().HaveCount(2);
        pool.Nodes[0].Name.Should().Be("east1");
        pool.Nodes[0].State.Should().Be("RUNNING");
        pool.Nodes[1].State.Should().BeNull("placement present, state absent — declared");
        pool.Endpoints.Should().Equal("h1:18001", "h2:18002");
    }

    // AAA: NOT_INITIALIZED/TO_REMOVE — не Active (отсутствие state = Active, arch/23 §2;
    // read-минимум t36 считал Active любой не-TO_REMOVE — унификация выравнивает на канон).
    [Theory]
    [InlineData(S3ClusterState.NotInitialized)]
    [InlineData(S3ClusterState.ToRemove)]
    public void MapPools_NonActiveState_NotActive(S3ClusterState state)
    {
        // Arrange
        var snapshot = Snapshot(Cluster("p", state, endpoints: null, nodes: []));

        // Act
        var pools = S3Mappers.MapPools(snapshot.Clusters);

        // Assert
        pools.Single().Active.Should().BeFalse();
    }

    // AAA: endpoints-строка → массив по порядку; элементы без валидного host:port — долой.
    [Fact]
    public void MapPools_EndpointsOrderAndGarbage()
    {
        // Arrange
        var snapshot = Snapshot(Cluster("p", S3ClusterState.Active,
            endpoints: "h2:18002,,bad,h1:18001", nodes: []));

        // Act
        var pools = S3Mappers.MapPools(snapshot.Clusters);

        // Assert — порядок строки сохранён (порядок чтения ReadAll, arch/19 §5).
        pools.Single().Endpoints.Should().Equal("h2:18002", "h1:18001");
    }

    // AAA: endpoints-ключа нет — пустой массив (не null).
    [Fact]
    public void MapPools_NoEndpoints_EmptyArray()
    {
        // Arrange
        var snapshot = Snapshot(Cluster("p", S3ClusterState.Active, endpoints: null, nodes: []));

        // Act
        var pools = S3Mappers.MapPools(snapshot.Clusters);

        // Assert
        pools.Single().Endpoints.Should().BeEmpty();
    }

    // Минимальный снапшот с одним кластером (прочие поля проекции пулов не читаются).
    private static S3Snapshot Snapshot(params S3ClusterInfo[] clusters)
        => new(
            BuiltAtUtc: DateTimeOffset.UtcNow,
            EtcdReachable: true,
            ConsecutiveFailures: 0,
            Clusters: clusters,
            Rotations: [],
            WorkerEndpoints: [],
            WorkerHealth: [],
            Alerts: [],
            ParseErrors: [],
            UnknownKeyCount: 0);

    private static S3ClusterInfo Cluster(
        string name, S3ClusterState state, string? endpoints, S3NodeInfo[] nodes)
        => new(
            Name: name,
            State: state,
            Platform: "owns3",
            NodesDeclared: nodes.Length,
            EtcdBackup: false,
            CreatedUnix: 1,
            Endpoints: endpoints,
            NodesList: nodes,
            Links: [],
            DenyList: []);

    private static S3NodeInfo Node(string name, string? state)
        => new(Name: name, State: state, PlacedOn: null, Placement: null,
            Resources: null, Usage: null);
}
