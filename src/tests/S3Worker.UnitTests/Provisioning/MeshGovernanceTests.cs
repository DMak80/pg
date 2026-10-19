using System.Diagnostics.Metrics;
using FluentAssertions;
using S3Worker.Core.Model;
using S3Worker.Provisioning;
using S3Worker.Provisioning.Processes;
using Xunit;

namespace S3Worker.UnitTests.Provisioning;

// Процесс G — MeshGovernance (arch/24 §5 G, spec §8.1.5): недостающая пара →
// join (endpoint b из portalloc, AdvertisedHost-приоритет); лишняя запись →
// delete на обеих сторонах; deny-инстанс — изоляция (∅, снос записей); 409 →
// delete конфликтной стороны + повтор; не-RUNNING сосед — ожидание; deny-перезапись
// всем RUNNING; одна структурная операция за тик; битый links — не применяется.
public class MeshGovernanceTests
{
    private const string Prefix = "/s3worker";

    // getenv-профиль store'а: спец-ключ etcd-backup (тестами не используется).
    private static readonly Dictionary<string, string?> Env = [];

    // Валидный secrets-ключ кластера (канон arch/23 §6): root-пара +
    // backups(read-write) и panel(read-only); значения — фиксированные
    // несекретные литералы.
    private static string SecretsJson(string cluster) => ClusterSecretsStore.Serialize(
        new S3Worker.Core.OwnS3.ClusterSecrets(
            $"s3w-{cluster}-root",
            "root-password-fixed",
            [
                new S3Worker.Core.OwnS3.ClusterAccessKey("backups", "backups-ak-fixed", "backups-sk-fixed", "read-write"),
                new S3Worker.Core.OwnS3.ClusterAccessKey("panel", "panel-ak-fixed", "panel-sk-fixed", "read-only"),
            ]));

    private sealed class Rig
    {
        public Fakes.FakeEtcd Etcd = new();
        public Fakes.FakeNodeClient NodeClient = new();
        public FixedTimeProvider Clock = new();
        public PkiStore Pki = null!;
        public MeshGovernanceProcess Mesh = null!;
        public WorkJournal Journal = null!;

        public static Rig New(string? advertisedHost = null, bool withSecrets = true)
        {
            var rig = new Rig();
            var endpoints = new[] { "http://etcd:2379" };
            rig.Pki = new PkiStore(rig.Etcd, endpoints, rig.Clock);
            rig.Journal = new WorkJournal(Prefix, rig.Etcd, endpoints);
            var claims = new ClaimStore(Prefix, endpoints, rig.Etcd, rig.Clock);
            claims.TryClaimClusterAsync("demo", TestContext.Current.CancellationToken).GetAwaiter().GetResult();
            if (withSecrets)
                rig.Etcd.Seed("/s3/clusters/demo/secrets", SecretsJson("demo"));
            var secrets = new ClusterSecretsStore(rig.Etcd, endpoints, env => env is null ? null : Env.GetValueOrDefault(env));
            using var meter = new Meter("S3Worker");
            rig.Mesh = new MeshGovernanceProcess(
                rig.Etcd, endpoints, claims, rig.Journal, secrets, rig.Pki, rig.NodeClient,
                new S3ProvisioningOptions(18000, 18999, 100, 90, 15, advertisedHost, "owns3:dev", "pgworker-backups"),
                meter);
            return rig;
        }
    }

    // Active-кластер: portalloc east1/east2(/west1), state-набор из аргумента.
    private static S3ClusterSnapshot Snap(
        IReadOnlyList<S3ReplicationLink>? links = null,
        IReadOnlyList<string>? denyList = null,
        (string Name, string State)[]? nodes = null,
        string? parseError = null)
    {
        var nodeStates = nodes ?? [("east1", S3NodeStates.Running), ("east2", S3NodeStates.Running)];
        return new S3ClusterSnapshot(
            "demo",
            new S3ClusterConfig("owns3", nodeStates.Length, null, 1760156800, null),
            nodeStates.Select(n => new S3NodeSnapshot(n.Name, n.State, "h1",
                new S3NodePlacement("h1", null, null), null, null, null)).ToList(),
            "h1:18001,h1:18002",
            links,
            denyList,
            parseError);
    }

    private static void SeedPortAlloc(Fakes.FakeEtcd etcd, params string[] names)
    {
        var entries = names.Select((n, i) => $@"""{n}"":{{""host"":""h1"",""s3"":{18001 + i}}}");
        etcd.Seed($"/s3worker/portalloc/demo", $"{{{string.Join(",", entries)}}}");
    }

    // AAA: недостающая пара → join на a с endpoint b из portalloc.
    [Fact]
    public async Task MissingPair_JoinWithEndpointFromPortalloc()
    {
        // Arrange: links {east1,east2}, фактические составы пусты.
        var rig = Rig.New();
        SeedPortAlloc(rig.Etcd, "east1", "east2");
        rig.NodeClient.Status = new S3Worker.Core.OwnS3.OwnS3ReplicationStatus(true, []);

        // Act
        var result = await rig.Mesh.TickAsync(
            Snap(links: [new S3ReplicationLink("east1", "east2")]), TestContext.Current.CancellationToken);

        // Assert: join вызван на east1 (h1:18001) c endpoint east2 (h1:18002).
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        var join = rig.NodeClient.MeshCalls.Should().ContainSingle(c => c.Op == "join").Which;
        join.Host.Should().Be("h1");
        join.Port.Should().Be(18001);
        join.Arg.Should().Be("http://h1:18002");
    }

    // AAA: AdvertisedHost приоритетнее host из portalloc в endpoint join.
    [Fact]
    public async Task Join_AdvertisedHostPriority()
    {
        // Arrange
        var rig = Rig.New(advertisedHost: "s3.example");
        SeedPortAlloc(rig.Etcd, "east1", "east2");
        rig.NodeClient.Status = new S3Worker.Core.OwnS3.OwnS3ReplicationStatus(true, []);

        // Act
        await rig.Mesh.TickAsync(
            Snap(links: [new S3ReplicationLink("east1", "east2")]), TestContext.Current.CancellationToken);

        // Assert
        rig.NodeClient.MeshCalls.Should().Contain(c => c.Op == "join" && c.Arg == "http://s3.example:18002");
    }

    // AAA: лишняя запись (links пуст — полный разрыв) → delete на ОБЕИХ сторонах.
    [Fact]
    public async Task ExtraEntry_DeleteBothSides()
    {
        // Arrange: фактические соседи есть, декларация links отсутствует (null).
        var rig = Rig.New();
        SeedPortAlloc(rig.Etcd, "east1", "east2");
        rig.NodeClient.Status = new S3Worker.Core.OwnS3.OwnS3ReplicationStatus(true, ["east2"]);

        // Act
        var result = await rig.Mesh.TickAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert: два delete — (east1→east2) и (east2→east1).
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.NodeClient.MeshCalls.Where(c => c.Op == "delete-target").Should().HaveCount(2);
        rig.NodeClient.MeshCalls.Should().Contain(c => c.Op == "delete-target" && c.Port == 18001 && c.Arg == "east2");
        rig.NodeClient.MeshCalls.Should().Contain(c => c.Op == "delete-target" && c.Port == 18002 && c.Arg == "east1");
    }

    // AAA: deny-инстанс — полная изоляция: join с ним не зовётся, его записи у
    // соседей и свои сносятся одной delete-парой (arch/24 §5 G п.3, §9.2).
    [Fact]
    public async Task DenyInstance_Isolation_RecordsRemoved()
    {
        // Arrange: east2 в denylist, связь декларирована, факт — связь жива
        // (запись east2 у east1; у east2 — запись east1).
        var rig = Rig.New();
        SeedPortAlloc(rig.Etcd, "east1", "east2");
        rig.NodeClient.StatusByPort[18001] = new S3Worker.Core.OwnS3.OwnS3ReplicationStatus(true, ["east2"]);
        rig.NodeClient.StatusByPort[18002] = new S3Worker.Core.OwnS3.OwnS3ReplicationStatus(true, ["east1"]);

        // Act
        var result = await rig.Mesh.TickAsync(
            Snap(links: [new S3ReplicationLink("east1", "east2")], denyList: ["east2"]),
            TestContext.Current.CancellationToken);

        // Assert: join нет; delete-пара (обе стороны); фаза cleaned-targets.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.NodeClient.MeshCalls.Should().NotContain(c => c.Op == "join");
        rig.NodeClient.MeshCalls.Where(c => c.Op == "delete-target").Should().HaveCount(2);
        var journal = await rig.Journal.ReadAsync("demo", TestContext.Current.CancellationToken);
        journal.Value!.Phase.Should().Be("cleaned-targets");
    }

    // AAA: join → 409 → delete конфликтной стороны; повторным тиком join повторяется.
    [Fact]
    public async Task Join409_DeleteConflictingSide_RetriedNextTick()
    {
        // Arrange: первый тик — Conflict, второй — Joined.
        var rig = Rig.New();
        SeedPortAlloc(rig.Etcd, "east1", "east2");
        rig.NodeClient.Status = new S3Worker.Core.OwnS3.OwnS3ReplicationStatus(true, []);
        rig.NodeClient.JoinOutcome = new S3Worker.Core.OwnS3.OwnS3JoinOutcome(S3Worker.Core.OwnS3.OwnS3JoinKind.Conflict);
        var snap = Snap(links: [new S3ReplicationLink("east1", "east2")]);

        // Act 1
        var first = await rig.Mesh.TickAsync(snap, TestContext.Current.CancellationToken);

        // Assert 1: конфликт → delete east2 на стороне east1 (конфликтующей).
        first.IsSuccess.Should().BeTrue(first.Error?.Message);
        rig.NodeClient.MeshCalls.Should().Contain(c => c.Op == "delete-target" && c.Port == 18001 && c.Arg == "east2");

        // Act 2: исходы статусов пусты (delete-фейк не меняет Status) → join снова.
        rig.NodeClient.JoinOutcome = new S3Worker.Core.OwnS3.OwnS3JoinOutcome(S3Worker.Core.OwnS3.OwnS3JoinKind.Joined);
        var second = await rig.Mesh.TickAsync(snap, TestContext.Current.CancellationToken);

        // Assert 2: второй join.
        second.IsSuccess.Should().BeTrue();
        rig.NodeClient.MeshCalls.Count(c => c.Op == "join").Should().Be(2);
    }

    // AAA: не-RUNNING сосед — ожидание (нет join); после RUNNING — join.
    [Fact]
    public async Task NotRunningNeighbor_Wait_ThenJoin()
    {
        // Arrange: east2 PROVISIONING — связи с ним не доводятся.
        var rig = Rig.New();
        SeedPortAlloc(rig.Etcd, "east1", "east2");
        rig.NodeClient.Status = new S3Worker.Core.OwnS3.OwnS3ReplicationStatus(true, []);
        var snap = Snap(links: [new S3ReplicationLink("east1", "east2")],
            nodes: [("east1", S3NodeStates.Running), ("east2", S3NodeStates.Provisioning)]);

        // Act 1
        var waiting = await rig.Mesh.TickAsync(snap, TestContext.Current.CancellationToken);

        // Assert 1: join нет.
        waiting.IsSuccess.Should().BeTrue(waiting.Error?.Message);
        rig.NodeClient.MeshCalls.Should().NotContain(c => c.Op == "join");

        // Act 2: east2 стал RUNNING.
        var ready = Snap(links: [new S3ReplicationLink("east1", "east2")]);
        await rig.Mesh.TickAsync(ready, TestContext.Current.CancellationToken);

        // Assert 2
        rig.NodeClient.MeshCalls.Should().Contain(c => c.Op == "join");
    }

    // AAA: deny-перезапись отправлена ВСЕМ RUNNING — полный entries с publicKeyPem.
    [Fact]
    public async Task DenyPush_ToAllRunning_WithPublicKeyPem()
    {
        // Arrange: west1 в denylist, материал pki west1 выпущен воркером.
        var rig = Rig.New();
        SeedPortAlloc(rig.Etcd, "east1", "east2", "west1");
        var clusterPki = await rig.Pki.EnsureClusterPkiAsync("demo", TestContext.Current.CancellationToken);
        var west1 = await rig.Pki.EnsureNodePkiAsync("demo", "west1", clusterPki.Value.Ca, TestContext.Current.CancellationToken);
        rig.NodeClient.Status = new S3Worker.Core.OwnS3.OwnS3ReplicationStatus(true, []);

        // Act
        var result = await rig.Mesh.TickAsync(
            Snap(denyList: ["west1"],
                nodes: [("east1", S3NodeStates.Running), ("east2", S3NodeStates.Running), ("west1", S3NodeStates.Running)]),
            TestContext.Current.CancellationToken);

        // Assert: denylist у RUNNING, КРОМЕ отозванного (arch/24 §5 G п.1):
        // east1 и east2 получают перезапись, west1 (DenyList-имя) — нет.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.NodeClient.MeshCalls.Count(c => c.Op == "denylist").Should().Be(2);
        rig.NodeClient.MeshCalls.Where(c => c.Op == "denylist")
            .Select(c => c.Port).Should().NotContain(18003);
        rig.NodeClient.LastDenyEntries.Should().NotBeNull();
        rig.NodeClient.LastDenyEntries.Should().ContainSingle()
            .Which.Name.Should().Be("west1");
        rig.NodeClient.LastDenyEntries![0].PublicKeyPem.Should().Be(west1.Value.Certificate.PublicKeyPem);
        var journal = await rig.Journal.ReadAsync("demo", TestContext.Current.CancellationToken);
        journal.Value!.Phase.Should().Be("deny-pushed");
    }

    // AAA: одна структурная операция за тик (два расхождения → первая по Ordinal).
    [Fact]
    public async Task OneStructuralOperationPerTick()
    {
        // Arrange: два недостающих соседа у east1 (east2, west1), составы пусты.
        var rig = Rig.New();
        SeedPortAlloc(rig.Etcd, "east1", "east2", "west1");
        rig.NodeClient.Status = new S3Worker.Core.OwnS3.OwnS3ReplicationStatus(true, []);
        var snap = Snap(links: [new S3ReplicationLink("east1", "east2"), new S3ReplicationLink("east1", "west1")],
            nodes: [("east1", S3NodeStates.Running), ("east2", S3NodeStates.Running), ("west1", S3NodeStates.Running)]);

        // Act
        var result = await rig.Mesh.TickAsync(snap, TestContext.Current.CancellationToken);

        // Assert: ровно один join (east1→east2 — первая по Ordinal).
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.NodeClient.MeshCalls.Where(c => c.Op == "join").Should().ContainSingle()
            .Which.Port.Should().Be(18001);
    }

    // AAA: битый links (parseError снапшота) — применения нет, диагностика в journal.
    [Fact]
    public async Task BrokenLinks_NotApplied_DiagnosticsInJournal()
    {
        // Arrange: Links null (битое значение), parseError от парсера.
        var rig = Rig.New();
        SeedPortAlloc(rig.Etcd, "east1", "east2");
        rig.NodeClient.Status = new S3Worker.Core.OwnS3.OwnS3ReplicationStatus(true, ["east2"]);
        var snap = Snap(parseError: "/s3/clusters/demo/links: invalid JSON");

        // Act: лишних записей при пустом (не применённом) графе НЕ сносим —
        // битая часть не применяется вовсе (ни join, ни delete).
        var result = await rig.Mesh.TickAsync(snap, TestContext.Current.CancellationToken);

        // Assert: mesh-операций нет; warning о битом links в last_error журнала.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.NodeClient.MeshCalls.Should().NotContain(c => c.Op == "join");
        rig.NodeClient.MeshCalls.Should().NotContain(c => c.Op == "delete-target");
        var journal = await rig.Journal.ReadAsync("demo", TestContext.Current.CancellationToken);
        journal.Value!.LastError.Should().Contain("invalid JSON");
    }

    // AAA: 0 RUNNING — no-op (idle).
    [Fact]
    public async Task ZeroRunning_NoOp()
    {
        // Arrange: инстансы NOT_INITIALIZED.
        var rig = Rig.New();
        SeedPortAlloc(rig.Etcd, "east1", "east2");
        rig.NodeClient.Status = new S3Worker.Core.OwnS3.OwnS3ReplicationStatus(true, []);

        // Act
        var result = await rig.Mesh.TickAsync(
            Snap(nodes: [("east1", S3NodeStates.NotInitialized), ("east2", S3NodeStates.NotInitialized)]),
            TestContext.Current.CancellationToken);

        // Assert
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.NodeClient.MeshCalls.Should().BeEmpty();
    }

    // AAA: secrets-ключа нет → диагностический skip (invalid-secrets),
    // mesh-операций нет.
    [Fact]
    public async Task MissingSecrets_Skip_NoMeshOps()
    {
        // Arrange: ключ /s3/clusters/demo/secrets не записан.
        var rig = Rig.New(withSecrets: false);
        SeedPortAlloc(rig.Etcd, "east1", "east2");
        rig.NodeClient.Status = new S3Worker.Core.OwnS3.OwnS3ReplicationStatus(true, []);

        // Act
        var result = await rig.Mesh.TickAsync(
            Snap(links: [new S3ReplicationLink("east1", "east2")]), TestContext.Current.CancellationToken);

        // Assert: Success-грань тика (skip), mesh-вызовов нет, фаза
        // invalid-secrets с диагностикой по имени ключа.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.NodeClient.MeshCalls.Should().BeEmpty();
        var journal = await rig.Journal.ReadAsync("demo", TestContext.Current.CancellationToken);
        journal.Value!.Phase.Should().Be("invalid-secrets");
        journal.Value.LastError.Should().Contain("secrets /s3/clusters/demo/secrets not found");
    }
}
