using AdminPanel.Core.S3;
using AdminPanel.Probes.S3;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Резолвер целей S3-инвентаря из снапшота (t31): первый по Ordinal Active-
// кластер с непустыми endpoints и валидными секретами (ключ panel, read-only,
// непустые креды) → цели http://<e>-списком; подходящего кластера нет → null
// (грань «Хранилище бэкапов» не настроена).
public class S3TargetsResolverTests
{
    private static readonly DateTimeOffset Now = new(2026, 10, 15, 12, 0, 0, TimeSpan.Zero);

    private static S3Snapshot Snapshot(params S3ClusterInfo[] clusters) => new(
        Now, EtcdReachable: true, ConsecutiveFailures: 0,
        clusters, [], [], [], [], [], 0);

    // Active-кластер с endpoints и panel-ключом (креды уникальны имени — база
    // ассертов «цели именно из этого кластера»); state/endpoints переопределяются
    // в кейсах-пропусках.
    private static S3ClusterInfo Cluster(
        string name,
        S3ClusterState state = S3ClusterState.Active,
        string? endpoints = "h1:9000,h2:9000")
        => new(name, state, "owns3", 2, false, 1760156800, endpoints,
            [], [], [],
            Secrets: new S3ClusterSecretsInfo("root",
                [new S3AccessKeyInfo("panel", "ak-" + name, "sk-" + name, "read-only")]));

    // AAA: Active-кластер с endpoints и panel-ключом → цели из него: http://-
    // префикс, порядок списка сохранён; креды — именно panel-ключа (соседний
    // ключ с другой политикой не берётся).
    [Fact]
    public void Resolve_QualifyingCluster_TargetsFromIt()
    {
        // Arrange — panel-ключ не первый в списке ключей.
        var snapshot = Snapshot(new S3ClusterInfo(
            "demo", S3ClusterState.Active, "owns3", 2, false, 1760156800,
            "h1:9000,h2:9000", [], [], [],
            Secrets: new S3ClusterSecretsInfo("root",
            [
                new S3AccessKeyInfo("backups", "ak-other", "sk-other", "read-write"),
                new S3AccessKeyInfo("panel", "ak-panel", "sk-panel", "read-only"),
            ])));

        // Act
        var targets = S3TargetsResolver.Resolve(snapshot);

        // Assert
        targets.Should().NotBeNull();
        targets!.Cluster.Should().Be("demo");
        targets.Endpoints.Should().Equal("http://h1:9000", "http://h2:9000");
        targets.AccessKey.Should().Be("ak-panel");
        targets.SecretKey.Should().Be("sk-panel");
    }

    // AAA: единственный кластер пула TO_REMOVE — пропущен → null.
    [Fact]
    public void Resolve_ToRemoveCluster_Skipped()
    {
        // Arrange
        var snapshot = Snapshot(Cluster("demo", S3ClusterState.ToRemove));

        // Act
        var targets = S3TargetsResolver.Resolve(snapshot);

        // Assert
        targets.Should().BeNull();
    }

    // AAA: у Active-кластера пусты endpoints — пропущен → null.
    [Fact]
    public void Resolve_ClusterWithoutEndpoints_Skipped()
    {
        // Arrange
        var snapshot = Snapshot(Cluster("demo", endpoints: null));

        // Act
        var targets = S3TargetsResolver.Resolve(snapshot);

        // Assert
        targets.Should().BeNull();
    }

    // AAA: в секретах нет ключа panel (только другой ключ) — пропущен → null.
    [Fact]
    public void Resolve_ClusterWithoutPanelKey_Skipped()
    {
        // Arrange
        var snapshot = Snapshot(new S3ClusterInfo(
            "demo", S3ClusterState.Active, "owns3", 2, false, 1760156800,
            "h1:9000", [], [], [],
            Secrets: new S3ClusterSecretsInfo("root",
                [new S3AccessKeyInfo("backups", "ak-other", "sk-other", "read-write")])));

        // Act
        var targets = S3TargetsResolver.Resolve(snapshot);

        // Assert
        targets.Should().BeNull();
    }

    // AAA: у кластера нет секретов вовсе — пропущен → null.
    [Fact]
    public void Resolve_ClusterWithoutSecrets_Skipped()
    {
        // Arrange
        var snapshot = Snapshot(new S3ClusterInfo(
            "demo", S3ClusterState.Active, "owns3", 2, false, 1760156800,
            "h1:9000", [], [], [], Secrets: null));

        // Act
        var targets = S3TargetsResolver.Resolve(snapshot);

        // Assert
        targets.Should().BeNull();
    }

    // AAA: берётся ПЕРВЫЙ по Ordinal подходящий кластер: "s10" < "s2" по Ordinal
    // ('1' < '2'), хотя культурно/числово "s2" идёт раньше; входной список —
    // в порядке "s2", затем "s10".
    [Fact]
    public void Resolve_PicksFirstByOrdinal()
    {
        // Arrange — оба кластера валидны; порядок в списке "s2" → "s10".
        var snapshot = Snapshot(Cluster("s2"), Cluster("s10"));

        // Act
        var targets = S3TargetsResolver.Resolve(snapshot);

        // Assert
        targets.Should().NotBeNull();
        targets!.Cluster.Should().Be("s10");
        targets.Endpoints.Should().Equal("http://h1:9000", "http://h2:9000");
    }
}
