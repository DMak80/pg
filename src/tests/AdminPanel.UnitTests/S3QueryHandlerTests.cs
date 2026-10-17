using AdminPanel.Core.S3;
using AdminPanel.Api.Inspection;
using AdminPanel.Etcd;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Хендлеры инспекции s3-домена (arch/03 §12.1): сводка, детали, 503-семантика.
public sealed class S3QueryHandlerTests
{
    private static readonly DateTimeOffset Now = new(2026, 10, 15, 12, 0, 0, TimeSpan.Zero);

    private sealed class StubReader(S3Snapshot? snapshot) : IS3SnapshotReader
    {
        public S3Snapshot? Current { get; } = snapshot;
    }

    private static S3Snapshot Snapshot(IReadOnlyList<S3ClusterInfo> clusters) => new(
        Now, EtcdReachable: true, ConsecutiveFailures: 0,
        clusters, [], [], [], [], [], 0);

    private static S3ClusterInfo DemoCluster() => new(
        "demo", S3ClusterState.Active, "owns3", 2, "backups-main", 1760156800,
        "h1:18001",
        [
            new S3NodeInfo("east1", "RUNNING", "h1",
                new S3NodePlacement("h1", null, null),
                new S3NodeResources(2m, 4, 200),
                new S3NodeUsage(10, 200, 1760156860)),
            new S3NodeInfo("east2", "PROVISIONING", null, null, null, null),
        ],
        [new S3ReplicationLink("east1", "east2")],
        ["west1"],
        new S3ConnectivityInfo("OK", 1760156900, new Dictionary<string, IReadOnlyDictionary<string, string>>(), null),
        new S3SyncInfo("SYNCED", 1760156900,
            [new S3SyncPair("east1", "east2", 0, 0, null, null)], null),
        new S3RotationTicket("demo", 1760157000, "admin"));

    // Arrange: стор с одним Active-кластером (nodes/endpoints/connectivity/sync/
    // rotation из TestSnapshots-подобного хелпера). Act: S3ClustersQueryHandler.
    // Assert: сводка с nodesRunning, connectivityState, syncState, rotationPending.
    [Fact]
    public async Task Summaries_MapFromSnapshot()
    {
        // Arrange: снапшот с кластером demo (2 ноды, 1 RUNNING, ротация жива).
        var store = new StubReader(Snapshot([DemoCluster()]));

        // Act
        var result = await new S3ClustersQueryHandler(store)
            .Handle(new S3ClustersQuery(), CancellationToken.None);

        // Assert: поля сводки — из снапшота.
        result.Value.Should().ContainSingle().Which.Should().BeEquivalentTo(
            new S3ClusterSummaryDto("demo", "ACTIVE", 2, 1, "h1:18001", "OK", "SYNCED", true));
    }

    // Arrange: тот же стор. Act: S3ClusterDetailsQuery("demo"/"nope").
    // Assert: детали со всеми секциями; "nope" → Failed(S3ClusterNotFound).
    [Fact]
    public async Task Details_FoundAndNotFound()
    {
        // Arrange: снапшот с кластером demo.
        var store = new StubReader(Snapshot([DemoCluster()]));

        // Act: существующий кластер.
        var found = await new S3ClusterDetailsQueryHandler(store)
            .Handle(new S3ClusterDetailsQuery("demo"), CancellationToken.None);

        // Assert: все секции деталей на месте.
        found.Value.Name.Should().Be("demo");
        found.Value.NodesList.Should().HaveCount(2);
        found.Value.NodesList[0].Denied.Should().BeFalse();
        found.Value.Links.Should().ContainSingle().Which.Should().Be(new S3LinkDto("east1", "east2"));
        found.Value.Denylist.Should().Equal("west1");
        found.Value.Connectivity!.State.Should().Be("OK");
        found.Value.Sync!.State.Should().Be("SYNCED");
        found.Value.Rotation!.RequestedBy.Should().Be("admin");
        found.Value.SecretProfile.Should().Be("backups-main");

        // Act: несуществующий кластер.
        var missing = await new S3ClusterDetailsQueryHandler(store)
            .Handle(new S3ClusterDetailsQuery("nope"), CancellationToken.None);

        // Assert: 404-семантика.
        missing.Error.Should().BeOfType<S3ClusterNotFound>();
    }

    // Arrange: null-стор. Act: хендлеры. Assert: Failed(SnapshotNotReadyException).
    [Fact]
    public async Task NoSnapshot_503Semantics()
    {
        // Arrange: снапшота ещё нет (до первого тика).
        var store = new StubReader(null);

        // Act: оба хендлера.
        var summaries = await new S3ClustersQueryHandler(store)
            .Handle(new S3ClustersQuery(), CancellationToken.None);
        var details = await new S3ClusterDetailsQueryHandler(store)
            .Handle(new S3ClusterDetailsQuery("demo"), CancellationToken.None);

        // Assert: 503-семантика pg/kafka/valkey.
        summaries.Error.Should().BeOfType<InspectionModule.SnapshotNotReadyException>();
        details.Error.Should().BeOfType<InspectionModule.SnapshotNotReadyException>();
    }
}
