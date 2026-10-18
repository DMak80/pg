using AdminPanel.Core;
using AdminPanel.Core.S3;
using AdminPanel.Core.S3.S3Alerting;
using FluentAssertions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.UnitTests;

// Каталог s3-алертов (arch/03 §12.4): таблицы правил условие → kind/severity.
public sealed class S3AlertRulesTests
{
    private static readonly S3AlertsOptions Options = new()
    {
        FreshProvisioningSeconds = 60,
        RotationStaleSeconds = 1800,
        SyncStaleSec = 300,
        DiskUsagePercent = 85,
    };

    private static S3Snapshot Evaluate(S3Snapshot next, S3Snapshot? prev = null)
        => next with { Alerts = new S3AlertEngine(OptionsOf()).Evaluate(next, prev) };

    private static IOptions<S3AlertsOptions> OptionsOf() => Microsoft.Extensions.Options.Options.Create(Options);

    private static S3Snapshot Snapshot(
        IReadOnlyList<S3ClusterInfo>? clusters = null,
        IReadOnlyList<S3RotationTicket>? rotations = null,
        IReadOnlyList<WorkerEndpoint>? endpoints = null,
        IReadOnlyList<WorkerHealth>? health = null,
        IReadOnlyList<KeyParseError>? parseErrors = null,
        DateTimeOffset? at = null)
        => new(
            at ?? DateTimeOffset.FromUnixTimeSeconds(1760160000),
            EtcdReachable: true, ConsecutiveFailures: 0,
            clusters ?? [], rotations ?? [], endpoints ?? [], health ?? [],
            [], parseErrors ?? [], 0);

    private static S3ClusterInfo Cluster(
        string name = "demo",
        S3ClusterState state = S3ClusterState.Active,
        IReadOnlyList<S3NodeInfo>? nodes = null,
        string? endpoints = "h1:18001",
        IReadOnlyList<S3ReplicationLink>? links = null,
        S3ConnectivityInfo? connectivity = null,
        S3SyncInfo? sync = null)
        => new(name, state, "owns3", nodes?.Count ?? 0, "backups-main", 1760156800L,
            endpoints, nodes ?? [], links ?? [], [], connectivity, sync);

    private static S3NodeInfo Node(
        string name = "east1",
        string? state = "RUNNING",
        string? placedOn = "h1",
        S3NodeResources? resources = null,
        S3NodeUsage? usage = null)
        => new(name, state, placedOn, new S3NodePlacement("h1", null, null), resources, usage);

    // Arrange: кластеры в NOT_INITIALIZED/TO_REMOVE. Act: Evaluate.
    // Assert: info-алерты lifecycle (§12.4 строки 1–2).
    [Fact]
    public void LifecycleStates_ProduceInfoAlerts()
    {
        var alerts = Evaluate(Snapshot([
            Cluster("claim", S3ClusterState.NotInitialized),
            Cluster("dying", S3ClusterState.ToRemove),
        ])).Alerts;

        Assert.Contains(alerts, a => a.Kind == "s3-cluster-not-initialized"
            && a.Severity == AlertSeverity.Info && a.Target == "claim");
        Assert.Contains(alerts, a => a.Kind == "s3-cluster-to-remove"
            && a.Severity == AlertSeverity.Info && a.Target == "dying");
    }

    // Arrange: Active-кластер, нода не RUNNING (несвежий PROVISIONING — тик назад
    // тоже PROVISIONING, окно истекло) и свежий PROVISIONING (prev на 50 c раньше).
    // Act: Evaluate. Assert: critical только у несвежей (строка 3).
    [Fact]
    public void NodeNotRunning_Critical_ExceptFreshProvisioning()
    {
        var prev = Snapshot([Cluster(nodes: [Node("booting", "PROVISIONING")])],
            at: DateTimeOffset.FromUnixTimeSeconds(1760159950));
        var next = Snapshot([Cluster(nodes: [
            Node("stuck", "UNREACHABLE"),
            Node("booting", "PROVISIONING"),
        ])]);
        var alerts = Evaluate(next, prev).Alerts;

        Assert.Contains(alerts, a => a.Kind == "s3-node-not-running"
            && a.Severity == AlertSeverity.Critical && a.Target == "demo/stuck");
        Assert.DoesNotContain(alerts, a => a.Target == "demo/booting");
    }

    // Arrange: Active с нодами без endpoints / DEGRADED / DOWN / LAGGING / DIVERGED.
    // Act: Evaluate. Assert: строки 4, 5–8 каталога.
    [Fact]
    public void ObservabilityRules_Table()
    {
        var alerts = Evaluate(Snapshot([
            Cluster("noep", nodes: [Node()], endpoints: null),
            Cluster("deg", connectivity: new S3ConnectivityInfo("DEGRADED", 1760159900, new Dictionary<string, IReadOnlyDictionary<string, string>>(), null)),
            Cluster("down", connectivity: new S3ConnectivityInfo("DOWN", 1760159900, new Dictionary<string, IReadOnlyDictionary<string, string>>(), null)),
            Cluster("lag", sync: new S3SyncInfo("LAGGING", 1760159900, [], null)),
            Cluster("div", sync: new S3SyncInfo("DIVERGED", 1760159900, [], null)),
        ])).Alerts;

        Assert.Contains(alerts, a => a.Kind == "s3-endpoints-missing"
            && a.Severity == AlertSeverity.Critical && a.Target == "noep");
        Assert.Contains(alerts, a => a.Kind == "s3-connectivity-degraded"
            && a.Severity == AlertSeverity.Warning);
        Assert.Contains(alerts, a => a.Kind == "s3-connectivity-down"
            && a.Severity == AlertSeverity.Critical);
        Assert.Contains(alerts, a => a.Kind == "s3-sync-lagging"
            && a.Severity == AlertSeverity.Warning);
        Assert.Contains(alerts, a => a.Kind == "s3-sync-diverged"
            && a.Severity == AlertSeverity.Critical);
    }

    // Arrange: Active с RUNNING-нодами: зонд молчит (ключей нет) и протухший
    // checked_unix (1760156900 при now=1760160000 > 300 c). Act: Evaluate.
    // Assert: s3-sync-stale warning оба кластера (строка 9).
    [Fact]
    public void SyncStale_Warning_WhenSilentOrOld()
    {
        var alerts = Evaluate(Snapshot([
            Cluster("silent", nodes: [Node()]),
            Cluster("stale", nodes: [Node()],
                connectivity: new S3ConnectivityInfo("OK", 1760156900, new Dictionary<string, IReadOnlyDictionary<string, string>>(), null)),
        ])).Alerts;

        var stale = alerts.Where(a => a.Kind == "s3-sync-stale").ToList();
        Assert.Equal(2, stale.Count);
        Assert.All(stale, a => Assert.Equal(AlertSeverity.Warning, a.Severity));
    }

    // Arrange: usage 90% от total и used > декларации disk (200 GiB = 214748364800).
    // Act: Evaluate. Assert: s3-node-disk-usage-high warning оба (строка 10).
    [Fact]
    public void DiskUsageHigh_Warning()
    {
        var alerts = Evaluate(Snapshot([
            Cluster(nodes: [
                Node("pct", usage: new S3NodeUsage(90, 100, 1760159900)),
                Node("over", resources: new S3NodeResources(2m, 4, 200),
                    usage: new S3NodeUsage(300_000_000_000, 400_000_000_000, 1760159900)),
            ]),
        ])).Alerts;

        var disk = alerts.Where(a => a.Kind == "s3-node-disk-usage-high").ToList();
        Assert.Equal(2, disk.Count);
    }

    // Arrange: две ноды на одном placed_on; нода без единой связи в links.
    // Act: Evaluate. Assert: s3-nodes-same-host и s3-node-not-in-mesh warning (11–12).
    [Fact]
    public void AntiAffinityAndMeshRules()
    {
        var alerts = Evaluate(Snapshot([
            Cluster(links: [new S3ReplicationLink("east1", "east2")], nodes: [
                Node("east1"), Node("east2", placedOn: "h1"), Node("lonely"),
            ]),
        ])).Alerts;

        Assert.Contains(alerts, a => a.Kind == "s3-nodes-same-host" && a.Severity == AlertSeverity.Warning);
        Assert.Contains(alerts, a => a.Kind == "s3-node-not-in-mesh" && a.Target == "demo/lonely");
    }

    // Arrange: живая заявка ротации свежая и старше порога; parseError.
    // Act: Evaluate. Assert: pending info, stale warning, malformed warning (13, 14, 15).
    [Fact]
    public void RotationAndMalformedRules()
    {
        var alerts = Evaluate(Snapshot(
            rotations: [new S3RotationTicket("demo", 1760158000, "admin")],
            parseErrors: [new KeyParseError("/s3/clusters/bad/config", "broken JSON")])).Alerts;

        Assert.Contains(alerts, a => a.Kind == "s3-rotation-pending" && a.Severity == AlertSeverity.Info);
        Assert.Contains(alerts, a => a.Kind == "s3-rotation-stale" && a.Severity == AlertSeverity.Warning);
        Assert.Contains(alerts, a => a.Kind == "s3-key-malformed" && a.Severity == AlertSeverity.Warning);
    }

    // Arrange: нет живых /s3worker/api/; живой ключ с Degraded-health.
    // Act: Evaluate. Assert: worker-api-unreachable critical / worker-unhealthy warning (16–17).
    [Fact]
    public void WorkerAvailabilityRules()
    {
        var unreachable = Evaluate(Snapshot()).Alerts;
        Assert.Contains(unreachable, a => a.Kind == "worker-api-unreachable"
            && a.Severity == AlertSeverity.Critical && a.Target == "s3worker");

        var unhealthy = Evaluate(Snapshot(
            endpoints: [new WorkerEndpoint("i1", "https://s3worker:8080", 1760150000)],
            health: [new WorkerHealth("i1", "https://s3worker:8080",
                WorkerHealthStatus.Degraded,
                DateTimeOffset.FromUnixTimeSeconds(1760159900), "HTTP 503")])).Alerts;
        Assert.Contains(unhealthy, a => a.Kind == "worker-unhealthy"
            && a.Severity == AlertSeverity.Warning && a.Target == "s3worker/i1");
    }

    // Arrange: алерт живёт два тика. Act: Evaluate(next, prev). Assert:
    // sinceUnix стабильный id=kind:target (механика §2 03-panels).
    [Fact]
    public void SinceUnix_StableAcrossTicks()
    {
        var first = Evaluate(Snapshot([Cluster("down2",
            connectivity: new S3ConnectivityInfo("DOWN", 1760159900, new Dictionary<string, IReadOnlyDictionary<string, string>>(), null))]));
        var prevAlert = Assert.Single(first.Alerts, a => a.Kind == "s3-connectivity-down");
        var firstSince = prevAlert.SinceUnix;

        var second = Evaluate(Snapshot([Cluster("down2",
            connectivity: new S3ConnectivityInfo("DOWN", 1760159950, new Dictionary<string, IReadOnlyDictionary<string, string>>(), null))]), first);
        var nextAlert = Assert.Single(second.Alerts, a => a.Kind == "s3-connectivity-down");
        Assert.Equal(firstSince, nextAlert.SinceUnix);
    }

    // Arrange: все алерты. Act: проверка Hint/Remedy. Assert: пустых нет (§4.1).
    [Fact]
    public void EveryAlert_CarriesHintAndRemedy()
    {
        var alerts = Evaluate(Snapshot(
            clusters: [Cluster("x", S3ClusterState.NotInitialized)],
            rotations: [new S3RotationTicket("x", 1, null)],
            parseErrors: [new KeyParseError("/s3/clusters/b/config", "broken")])).Alerts;

        Assert.All(alerts, a =>
        {
            Assert.False(string.IsNullOrWhiteSpace(a.Hint));
            Assert.False(string.IsNullOrWhiteSpace(a.RemedyText));
        });
    }
}
