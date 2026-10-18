using FluentAssertions;
using S3Worker.Core.Model;
using S3Worker.Docker.Drivers;
using S3Worker.Provisioning.Processes;
using Xunit;

namespace S3Worker.UnitTests.Provisioning;

// Deprovisioning X0–X3 (arch/24 §5 B): маркеры REMOVING, порядок «docker →
// etcd» (сбой docker оставляет etcd нетронутым), del координации, явное
// снятие клэйма.
public class DeprovisioningProcessTests
{
    private const string Prefix = "/s3worker";

    private static (DeprovisioningProcess Process, Fakes.FakeEtcd Etcd, Fakes.FakeS3Driver Driver, ClaimStore Claims) NewRig()
    {
        var etcd = new Fakes.FakeEtcd();
        var driver = new Fakes.FakeS3Driver();
        var claims = new ClaimStore(Prefix, ["http://etcd:2379"], etcd, TimeProvider.System);
        var process = new DeprovisioningProcess(
            etcd, ["http://etcd:2379"], driver, claims,
            new WorkJournal(Prefix, etcd, ["http://etcd:2379"]),
            new BootWaitBudget(TimeProvider.System));
        return (process, etcd, driver, claims);
    }

    private static async Task SeedActiveAsync(Fakes.FakeEtcd etcd, Fakes.FakeS3Driver driver, ClaimStore claims, string cluster = "demo")
    {
        etcd.Seed($"/s3/clusters/{cluster}/config",
            $$"""{"platform":"owns3","nodes":2,"secret_profile":"backups-main","created_unix":1760156800,"state":"TO_REMOVE"}""");
        etcd.Seed($"/s3/clusters/{cluster}/nodes/node1/state", S3NodeStates.Running);
        etcd.Seed($"/s3/clusters/{cluster}/nodes/node2/state", S3NodeStates.Running);
        etcd.Seed($"/s3/clusters/{cluster}/endpoints", "h1:18000,h2:18001");
        etcd.Seed($"{Prefix}/portalloc/{cluster}", """{"node1":{"host":"h1","s3":18000},"node2":{"host":"h2","s3":18001}}""");
        etcd.Seed(S3ProcessCommon.ProbeRequestKey(cluster), """{"requested_unix":1760156800,"requested_by":"e2e-op"}""");
        driver.Nodes[$"{cluster}/node1"] = new Fakes.FakeS3Driver.NodeFact("h1", 18000);
        driver.Nodes[$"{cluster}/node2"] = new Fakes.FakeS3Driver.NodeFact("h2", 18001);
        await claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken);
    }

    private static S3ClusterSnapshot Snap(string cluster = "demo") => new(
        cluster,
        new S3ClusterConfig("owns3", 2, "backups-main", 1760156800, S3NodeStates.ToRemove),
        [
            new S3NodeSnapshot("node1", S3NodeStates.Running, "h1", new S3NodePlacement("h1", null, null),
                new S3NodeResources("1", "1Gi", "5Gi"), null, null),
            new S3NodeSnapshot("node2", S3NodeStates.Running, "h2", new S3NodePlacement("h2", null, null),
                new S3NodeResources("1", "1Gi", "5Gi"), null, null),
        ],
        null, null);

    // AAA: X0–X3 happy-путь: REMOVING маркеры, docker снос, чистка etcd,
    // journal done, клэйм снят явно.
    [Fact]
    public async Task X0_X3_FullTeardown()
    {
        // Arrange: Active-кластер под TO_REMOVE.
        var (process, etcd, driver, claims) = NewRig();
        await SeedActiveAsync(etcd, driver, claims);

        // Act
        var result = await process.TickAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert: домен и координация вычищены; docker снесён; клэйм отпущен
        // (release — claims-ключа нет: второй инстанс может захватить).
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        etcd.Store.Keys.Where(k => k.StartsWith("/s3/clusters/demo", StringComparison.Ordinal))
            .Should().BeEmpty();
        etcd.Store.Keys.Should().NotContain($"{Prefix}/portalloc/demo");
        etcd.Store.Keys.Should().NotContain($"{Prefix}/rotations/demo");
        etcd.Store.ContainsKey(S3ProcessCommon.ProbeRequestKey("demo")).Should().BeFalse(); // заявка зонда t33 снесена X3
        driver.Removed.Should().Contain(["demo/node1", "demo/node2"]);
        etcd.Store.Keys.Where(k => k.StartsWith($"{Prefix}/claims/", StringComparison.Ordinal))
            .Should().NotContain($"{Prefix}/claims/demo");
        var journal = await new WorkJournal(Prefix, etcd, ["http://etcd:2379"])
            .ReadAsync("demo", TestContext.Current.CancellationToken);
        journal.Value!.Op.Should().Be("deprovision");
        journal.Value.Phase.Should().Be("done");
    }

    // AAA: X1 — маркеры REMOVING стоят ДО docker-фазы; docker-сбой → etcd
    // НЕ тронут (порядок «docker → etcd»), ретрай тиком.
    [Fact]
    public async Task DockerPhaseFails_MarkersSet_EtcdUntouched()
    {
        // Arrange: remove-фаза ломается обёрткой (docker-хост недоступен).
        var (process, etcd, driver, claims) = NewRig();
        await SeedActiveAsync(etcd, driver, claims);
        var breaking = new RemovingFaultDriver(driver);
        var brokenProcess = new DeprovisioningProcess(
            etcd, ["http://etcd:2379"], breaking, claims,
            new WorkJournal(Prefix, etcd, ["http://etcd:2379"]),
            new BootWaitBudget(TimeProvider.System));

        // Act
        var result = await brokenProcess.TickAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert: Failed; X1-маркеры REMOVING у обеих нод; etcd-домен и
        // portalloc НЕ тронуты (etcd после docker).
        result.IsSuccess.Should().BeFalse();
        etcd.Store["/s3/clusters/demo/nodes/node1/state"].Value.Should().Be(S3NodeStates.Removing);
        etcd.Store["/s3/clusters/demo/nodes/node2/state"].Value.Should().Be(S3NodeStates.Removing);
        etcd.Store.Keys.Should().Contain("/s3/clusters/demo/config");
        etcd.Store.Keys.Should().Contain($"{Prefix}/portalloc/demo");
    }

    // Драйвер-обёртка с ломким remove (docker-фаза Failed).
    private sealed class RemovingFaultDriver(Fakes.FakeS3Driver inner) : IS3ClusterDriver
    {
        public int S3ContainerPort => inner.S3ContainerPort;

        public Task<Result<IReadOnlyList<S3HostAttributes>>> GetHostsAsync(CancellationToken ct)
            => inner.GetHostsAsync(ct);

        public Task<Result<IReadOnlySet<(string Host, int Port)>>> GetBusyPortsAsync(CancellationToken ct)
            => inner.GetBusyPortsAsync(ct);

        public Task<Result> EnsureNodeAsync(S3NodeSpec spec, string? contourNetwork, CancellationToken ct)
            => inner.EnsureNodeAsync(spec, contourNetwork, ct);

        public Task<Result> RemoveNodeAsync(string cluster, string node, bool removeVolume, CancellationToken ct)
            => Task.FromResult(Result.Failed(new ApplicationException("docker host unavailable")));

        public Task<Result<Shared.Docker.NodeLimits?>> NodeResourcesAsync(string cluster, string node, CancellationToken ct)
            => inner.NodeResourcesAsync(cluster, node, ct);

        public Task<Result<S3NodeInspection?>> InspectNodeAsync(string cluster, string node, CancellationToken ct)
            => inner.InspectNodeAsync(cluster, node, ct);

        public Task<Result<IReadOnlyDictionary<string, string>?>> NodeEnvAsync(string cluster, string node, CancellationToken ct)
            => inner.NodeEnvAsync(cluster, node, ct);

        public Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(string cluster, CancellationToken ct)
            => inner.ListNodeObjectsAsync(cluster, ct);
    }

    // AAA: без клэйма — никаких записей.
    [Fact]
    public async Task NoClaim_NoWrites()
    {
        // Arrange: демонтажная заявка, клэйм НЕ наш.
        var (process, etcd, driver, _) = NewRig();
        await SeedActiveAsync(etcd, driver, new ClaimStore(
            Prefix, ["http://etcd:2379"], etcd, TimeProvider.System));

        // Act
        var result = await process.TickAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert: Failed-грань; домен жив.
        result.IsSuccess.Should().BeFalse();
        etcd.Store.Keys.Should().Contain("/s3/clusters/demo/config");
        driver.Removed.Should().BeEmpty();
    }
}
