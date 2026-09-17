using AdminPanel.Core;
using AdminPanel.Core.Valkey;
using AdminPanel.Core.Valkey.ValkeyAlerting;
using FluentAssertions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.UnitTests;

// Каталог valkey-алертов (arch/03 §8.4): все 8 kinds, fresh-PROVISIONING-окно,
// гашение, sinceUnix по стабильному id kind:target.
public class ValkeyAlertRulesTests
{
    private static readonly DateTimeOffset Now = new(2026, 9, 17, 12, 0, 0, TimeSpan.Zero);
    private static readonly long NowUnix = Now.ToUnixTimeSeconds();

    private readonly ValkeyAlertEngine _engine =
        new(Options.Create(new ValkeyAlertsOptions()));

    // Оценка: снапшот + необязательный предыдущий (механика refresher).
    private IReadOnlyList<Alert> Evaluate(ValkeySnapshot next, ValkeySnapshot? prev = null)
        => [.. _engine.Evaluate(next, prev)];

    private static ValkeySnapshot Snapshot(params ValkeyClusterInfo[] clusters)
        => Snapshot([], clusters);

    // Перегрузка со здоровьем воркера (params остаётся последним).
    private static ValkeySnapshot Snapshot(
        IReadOnlyList<WorkerHealth> workerHealth, params ValkeyClusterInfo[] clusters) => new(
        Now, EtcdReachable: true, ConsecutiveFailures: 0,
        [.. clusters], Rotations: [],
        WorkerEndpoints: [new WorkerEndpoint("vwk1", "https://valkeyworker:8080", 1)],
        WorkerHealth: workerHealth, Probes: [], Alerts: [], ParseErrors: [], UnknownKeyCount: 0);

    // Active-кластер с одной нодой (по умолчанию RUNNING node1 + endpoints).
    private static ValkeyClusterInfo ActiveCluster(
        string name = "live",
        string? endpoints = "host.docker.internal:17001",
        ValkeyNodeInfo[]? nodes = null)
        => new(
            name, ValkeyClusterState.Active,
            Nodes: 1, MaxmemoryBytes: 536870912, MaxmemoryPolicy: "allkeys-lru",
            CreatedUnix: 1756500000, Endpoints: endpoints,
            NodesList: nodes ?? [Node("node1")]);

    private static ValkeyClusterInfo NotInitializedCluster(string name = "cache")
        => new(
            name, ValkeyClusterState.NotInitialized, 1, 536870912, "allkeys-lru",
            1756500000, null, [Node("node1", "NOT_INITIALIZED")]);

    private static ValkeyClusterInfo ToRemoveCluster(string name = "dying")
        => new(
            name, ValkeyClusterState.ToRemove, 1, 536870912, "allkeys-lru",
            1756500000, "host.docker.internal:17002", [Node("node1", "TO_REMOVE")]);

    private static ValkeyNodeInfo Node(string name, string? state = "RUNNING")
        => new(name, state, 1m, 1, 10);

    // ===== valkey-cluster-not-initialized / valkey-cluster-to-remove (info) =====

    // Arrange: NOT_INITIALIZED-кластер. Act: Evaluate. Assert: kind
    // valkey-cluster-not-initialized, severity info.
    [Fact]
    public void NotInitialized_Info()
    {
        // Arrange
        var next = Snapshot(NotInitializedCluster());

        // Act
        var alerts = Evaluate(next);

        // Assert
        var a = alerts.Should().ContainSingle(
            x => x.Kind == "valkey-cluster-not-initialized" && x.Target == "cache").Subject;
        a.Severity.Should().Be(AlertSeverity.Info);
    }

    // Arrange: TO_REMOVE-кластер. Assert: valkey-cluster-to-remove, info.
    [Fact]
    public void ToRemove_Info()
    {
        // Arrange
        var next = Snapshot(ToRemoveCluster());

        // Act/Assert
        var a = Evaluate(next).Should().ContainSingle(
            x => x.Kind == "valkey-cluster-to-remove" && x.Target == "dying").Subject;
        a.Severity.Should().Be(AlertSeverity.Info);
    }

    // ===== valkey-node-not-running (critical) + fresh-окно =====

    // Arrange: Active-кластер, нода UNREACHABLE. Assert: valkey-node-not-running, critical.
    [Fact]
    public void NodeNotRunning_Critical()
    {
        // Arrange
        var next = Snapshot(ActiveCluster(nodes: [Node("node1", "UNREACHABLE")]));

        // Act/Assert
        var a = Evaluate(next).Should().ContainSingle(
            x => x.Kind == "valkey-node-not-running" && x.Target == "live/node1").Subject;
        a.Severity.Should().Be(AlertSeverity.Critical);
    }

    // Arrange: Active, нода PROVISIONING; prev-снапшот тик назад тоже PROVISIONING,
    // разница BuiltAtUtc < FreshProvisioningSeconds. Assert: алерта НЕТ (fresh-окно).
    [Fact]
    public void NodeFreshProvisioning_Suppressed()
    {
        // Arrange: PROVISIONING наблюдался тик назад, окно 60 c не истекло (тик 3 c).
        var provisioning = new[] { Node("node1", "PROVISIONING") };
        var prev = Snapshot(ActiveCluster(nodes: provisioning));
        var next = Snapshot(ActiveCluster(nodes: provisioning)) with
        {
            BuiltAtUtc = Now.AddSeconds(3),
        };

        // Act/Assert: штатный подъём — critical-шум неуместен.
        Evaluate(next, prev).Should().NotContain(x => x.Kind == "valkey-node-not-running");
    }

    // Arrange: то же, но prev старше окна. Assert: алерт ЕСТЬ.
    [Fact]
    public void NodeProvisioningStale_Raises()
    {
        // Arrange: PROVISIONING тянется дольше окна (prev старше 60 c).
        var provisioning = new[] { Node("node1", "PROVISIONING") };
        var prev = Snapshot(ActiveCluster(nodes: provisioning));
        var next = Snapshot(ActiveCluster(nodes: provisioning)) with
        {
            BuiltAtUtc = Now.AddSeconds(90),
        };

        // Act/Assert
        Evaluate(next, prev).Should().ContainSingle(
            x => x.Kind == "valkey-node-not-running" && x.Target == "live/node1");
    }

    // Arrange: нода стала RUNNING. Assert: алерта нет (гашение).
    [Fact]
    public void NodeRecovered_Clears()
    {
        // Arrange: в prev алерт горел; в next нода RUNNING.
        var prev = Snapshot(ActiveCluster(nodes: [Node("node1", "UNREACHABLE")]));
        var next = Snapshot(ActiveCluster());

        // Act/Assert
        Evaluate(next, prev).Should().NotContain(x => x.Kind == "valkey-node-not-running");
    }

    // ===== valkey-endpoints-missing (critical) =====

    // Arrange: Active без endpoints (null). Assert: valkey-endpoints-missing, critical.
    [Fact]
    public void EndpointsMissing_Critical()
    {
        // Arrange
        var next = Snapshot(ActiveCluster(endpoints: null));

        // Act/Assert
        var a = Evaluate(next).Should().ContainSingle(
            x => x.Kind == "valkey-endpoints-missing" && x.Target == "live").Subject;
        a.Severity.Should().Be(AlertSeverity.Critical);
    }

    // ===== valkey-rotation-pending (info) =====

    // Arrange: живая заявка ротации кластера live. Assert: valkey-rotation-pending,
    // info, details role/requestedBy.
    [Fact]
    public void RotationPending_Info()
    {
        // Arrange: заявка в Rotations + джойн в кластере (механика refresher).
        var ticket = new ValkeyRotationTicket("live", "app", NowUnix - 30, "seed");
        var next = Snapshot(ActiveCluster() with { Rotation = ticket }) with
        {
            Rotations = [ticket],
        };

        // Act
        var a = Evaluate(next).Should().ContainSingle(
            x => x.Kind == "valkey-rotation-pending" && x.Target == "live").Subject;

        // Assert
        a.Severity.Should().Be(AlertSeverity.Info);
        a.Details!["role"].Should().Be("app");
        a.Details["requestedBy"].Should().Be("seed");
    }

    // Arrange: заявка исчезла. Assert: алерт погас.
    [Fact]
    public void RotationGone_Clears()
    {
        // Arrange
        var prev = Snapshot(ActiveCluster() with
        {
            Rotation = new ValkeyRotationTicket("live", "app", NowUnix - 30, "seed"),
        });
        var next = Snapshot(ActiveCluster());

        // Act/Assert
        Evaluate(next, prev).Should().NotContain(x => x.Kind == "valkey-rotation-pending");
    }

    // ===== valkey-key-malformed (warning) =====

    // Arrange: ParseErrors с ключом. Assert: valkey-key-malformed, warning.
    [Fact]
    public void KeyMalformed_Warning()
    {
        // Arrange
        var next = Snapshot() with
        {
            ParseErrors = [new KeyParseError("/valkey/clusters/bad/config", "битый JSON config")],
        };

        // Act/Assert
        var a = Evaluate(next).Should().ContainSingle(
            x => x.Kind == "valkey-key-malformed"
                && x.Target == "/valkey/clusters/bad/config").Subject;
        a.Severity.Should().Be(AlertSeverity.Warning);
    }

    // ===== worker-api-unreachable (critical) / worker-unhealthy (warning) =====

    // Arrange: WorkerEndpoints пуст. Assert: worker-api-unreachable, critical,
    // target "valkeyworker".
    [Fact]
    public void WorkerApiUnreachable_Critical()
    {
        // Arrange: живых ключей /valkeyworker/api/ нет.
        var next = Snapshot() with { WorkerEndpoints = [] };

        // Act/Assert
        var a = Evaluate(next).Should().ContainSingle(
            x => x.Kind == "worker-api-unreachable" && x.Target == "valkeyworker").Subject;
        a.Severity.Should().Be(AlertSeverity.Critical);
    }

    // Arrange: WorkerHealth [{InstanceId=vwk1, Degraded}]. Assert: worker-unhealthy,
    // warning, target "valkeyworker/vwk1".
    [Fact]
    public void WorkerUnhealthy_Warning()
    {
        // Arrange
        var next = Snapshot(
            [new WorkerHealth("vwk1", "https://valkeyworker:8080", WorkerHealthStatus.Degraded, Now, "HTTP 503")]);

        // Act/Assert
        var a = Evaluate(next).Should().ContainSingle(
            x => x.Kind == "worker-unhealthy" && x.Target == "valkeyworker/vwk1").Subject;
        a.Severity.Should().Be(AlertSeverity.Warning);
    }

    // ===== sinceUnix по стабильному id =====

    // Arrange: алерт жил в prev (SinceUnix=T). Act: Evaluate с prev.
    // Assert: SinceUnix перенесён (стабильный id kind:target).
    [Fact]
    public void SinceUnix_StableAcrossTicks()
    {
        // Arrange: endpoints-missing горел в prev с sinceUnix=100.
        var baseSnap = Snapshot(ActiveCluster(endpoints: null));
        var first = Evaluate(baseSnap).Single(x => x.Kind == "valkey-endpoints-missing");
        var prev = baseSnap with { Alerts = [first with { SinceUnix = 100 }] };
        var next = Snapshot(ActiveCluster(endpoints: null));

        // Act
        var again = Evaluate(next, prev);

        // Assert
        again.Single(x => x.Kind == "valkey-endpoints-missing").SinceUnix.Should().Be(100);
    }
}
