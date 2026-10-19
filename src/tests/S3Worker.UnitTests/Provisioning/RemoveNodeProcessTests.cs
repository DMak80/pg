using FluentAssertions;
using S3Worker.Core.Model;
using S3Worker.Docker.Drivers;
using S3Worker.Provisioning.Processes;
using Xunit;

namespace S3Worker.UnitTests.Provisioning;

// RemoveNode E (arch/24 §5 E): гварды ДО деструктива (последняя нода; нет
// живых RUNNING кроме выводимой), REMOVING → снос (контейнер+volume) → del
// ключей ноды + portalloc без неё → RMW endpoints + config.nodes → done.
public class RemoveNodeProcessTests
{
    private const string Prefix = "/s3worker";

    private static (RemoveNodeProcess Process, Fakes.FakeEtcd Etcd, Fakes.FakeS3Driver Driver, ClaimStore Claims) NewRig()
    {
        var etcd = new Fakes.FakeEtcd();
        var driver = new Fakes.FakeS3Driver();
        var claims = new ClaimStore(Prefix, ["http://etcd:2379"], etcd, TimeProvider.System);
        var process = new RemoveNodeProcess(
            etcd, ["http://etcd:2379"], driver, claims,
            new WorkJournal(Prefix, etcd, ["http://etcd:2379"]),
            new PortAllocLock(Prefix, ["http://etcd:2379"], etcd, TimeProvider.System, "inst-1"),
            S3ProvisioningOptions.Default);
        return (process, etcd, driver, claims);
    }

    // Active-кластер N=2, node2 с маркером TO_REMOVE.
    private static async Task SeedAsync(Fakes.FakeEtcd etcd, ClaimStore claims, string node2State = S3NodeStates.ToRemove)
    {
        etcd.Seed("/s3/clusters/demo/config",
            """{"platform":"owns3","nodes":2,"created_unix":1760156800}""");
        etcd.Seed("/s3/clusters/demo/nodes/node1/state", S3NodeStates.Running);
        etcd.Seed("/s3/clusters/demo/nodes/node2/state", node2State);
        etcd.Seed("/s3/clusters/demo/nodes/node2/placed_on", "h2");
        etcd.Seed("/s3/clusters/demo/nodes/node2/usage", """{"used_bytes":1,"total_bytes":2,"updated_unix":1760156800}""");
        etcd.Seed("/s3/clusters/demo/endpoints", "h1:18000,h2:18001");
        etcd.Seed($"{Prefix}/portalloc/demo", """{"node1":{"host":"h1","s3":18000},"node2":{"host":"h2","s3":18001}}""");
        await claims.TryClaimClusterAsync("demo", TestContext.Current.CancellationToken);
    }

    private static S3ClusterSnapshot Snap(string node2State = S3NodeStates.ToRemove, string node1State = S3NodeStates.Running) => new(
        "demo",
        new S3ClusterConfig("owns3", 2, null, 1760156800, null),
        [
            new S3NodeSnapshot("node1", node1State, "h1", new S3NodePlacement("h1", null, null),
                new S3NodeResources("1", "1Gi", "5Gi"), null, null),
            new S3NodeSnapshot("node2", node2State, "h2", new S3NodePlacement("h2", null, null),
                new S3NodeResources("1", "1Gi", "5Gi"), null, null),
        ],
        null, null);

    // AAA: happy-путь: REMOVING → снос (контейнер+volume) → del ключей ноды →
    // portalloc без node2 → endpoints = 1 адрес → config.nodes = 1 → done.
    [Fact]
    public async Task NodeRemoval_FullCycle()
    {
        // Arrange
        var (process, etcd, driver, claims) = NewRig();
        await SeedAsync(etcd, claims);

        // Act
        var result = await process.TickAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        driver.Removed.Should().Contain("demo/node2");
        etcd.Store.Keys.Where(k => k.StartsWith("/s3/clusters/demo/nodes/node2", StringComparison.Ordinal))
            .Should().BeEmpty();
        var portalloc = S3ProcessCommon.ParsePortAlloc(etcd.Store[$"{Prefix}/portalloc/demo"].Value);
        portalloc.Keys.Should().BeEquivalentTo(["node1"]);
        etcd.Store["/s3/clusters/demo/endpoints"].Value.Should().Be("h1:18000");
        etcd.Store["/s3/clusters/demo/config"].Value
            .Should().Be("""{"platform":"owns3","nodes":1,"created_unix":1760156800}""");
        var journal = await new WorkJournal(Prefix, etcd, ["http://etcd:2379"])
            .ReadAsync("demo", TestContext.Current.CancellationToken);
        journal.Value!.Phase.Should().Be("done");
    }

    // AAA: гвард «последняя нода» → отказ фазы (journal), без мутаций.
    [Fact]
    public async Task LastNode_Rejected()
    {
        // Arrange: кластер из одной ноды с маркером TO_REMOVE.
        var (process, etcd, driver, claims) = NewRig();
        etcd.Seed("/s3/clusters/demo/config",
            """{"platform":"owns3","nodes":1,"created_unix":1760156800}""");
        etcd.Seed("/s3/clusters/demo/nodes/node1/state", S3NodeStates.ToRemove);
        etcd.Seed($"{Prefix}/portalloc/demo", """{"node1":{"host":"h1","s3":18000}}""");
        driver.Nodes["demo/node1"] = new Fakes.FakeS3Driver.NodeFact("h1", 18000);
        await claims.TryClaimClusterAsync("demo", TestContext.Current.CancellationToken);
        var single = new S3ClusterSnapshot("demo",
            new S3ClusterConfig("owns3", 1, null, 1760156800, null),
            [new S3NodeSnapshot("node1", S3NodeStates.ToRemove, "h1",
                new S3NodePlacement("h1", null, null), new S3NodeResources("1", "1Gi", "5Gi"), null, null)],
            null, null, null);

        // Act
        var result = await process.TickAsync(single, TestContext.Current.CancellationToken);

        // Assert: отказ; docker и etcd не тронуты (гварды до деструктива).
        result.IsSuccess.Should().BeFalse();
        driver.Removed.Should().BeEmpty();
        etcd.Store.Keys.Should().Contain("/s3/clusters/demo/nodes/node1/state");
        var journal = await new WorkJournal(Prefix, etcd, ["http://etcd:2379"])
            .ReadAsync("demo", TestContext.Current.CancellationToken);
        journal.Value!.Phase.Should().Be("rejected");
    }

    // AAA: вывод инстанса чистит pki-ключ (имя освобождается — §3.2) и имя из
    // denylist кластера (иначе deny-доведение потеряет publicKeyPem — §3.4).
    [Fact]
    public async Task Removal_CleansPki_AndDenylist()
    {
        // Arrange: node2 заявлен TO_REMOVE; в etcd pki node2 и denylist с node2.
        var (process, etcd, driver, claims) = NewRig();
        await SeedAsync(etcd, claims);
        etcd.Seed("/s3/clusters/demo/nodes/node2/pki",
            """{"certificate":"{}","private_key_pem":"k"}""");
        etcd.Seed("/s3/clusters/demo/denylist", """["node2"]""");

        // Act
        var result = await process.TickAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert: pki-ключ удалён; denylist пуст (node2 убран); остальное нетронуто.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        etcd.Store.Keys.Should().NotContain("/s3/clusters/demo/nodes/node2/pki");
        etcd.Store["/s3/clusters/demo/denylist"].Value.Should().Be("[]");
        etcd.Store.Keys.Should().Contain("/s3/clusters/demo/nodes/node1/state");
    }

    // AAA: гвард «мёртвые остальные»: живых RUNNING кроме выводимой нет → отказ.
    [Fact]
    public async Task DeadRemainder_Rejected()
    {
        // Arrange: node1 UNREACHABLE (не RUNNING), node2 TO_REMOVE.
        var (process, etcd, driver, claims) = NewRig();
        await SeedAsync(etcd, claims);
        etcd.Seed("/s3/clusters/demo/nodes/node1/state", S3NodeStates.Unreachable);

        // Act
        var result = await process.TickAsync(Snap(node1State: S3NodeStates.Unreachable), TestContext.Current.CancellationToken);

        // Assert: отказ; docker не тронут.
        result.IsSuccess.Should().BeFalse();
        driver.Removed.Should().BeEmpty();
        etcd.Store.Keys.Should().Contain("/s3/clusters/demo/nodes/node2/state");
    }

    // AAA: повторный прогон при актуальном nodes — no-op (config не переписан).
    [Fact]
    public async Task RepeatRun_NoOp()
    {
        // Arrange: вывод node2 уже отработан (config.nodes=1, node2 удалён);
        // заявки нет.
        var (process, etcd, driver, claims) = NewRig();
        etcd.Seed("/s3/clusters/demo/config",
            """{"platform":"owns3","nodes":1,"created_unix":1760156800}""");
        etcd.Seed("/s3/clusters/demo/nodes/node1/state", S3NodeStates.Running);
        etcd.Seed($"{Prefix}/portalloc/demo", """{"node1":{"host":"h1","s3":18000}}""");
        await claims.TryClaimClusterAsync("demo", TestContext.Current.CancellationToken);
        var modRevisionBefore = etcd.Store["/s3/clusters/demo/config"].ModRevision;
        var snap = new S3ClusterSnapshot("demo",
            new S3ClusterConfig("owns3", 1, null, 1760156800, null),
            [new S3NodeSnapshot("node1", S3NodeStates.Running, "h1",
                new S3NodePlacement("h1", null, null), new S3NodeResources("1", "1Gi", "5Gi"), null, null)],
            null, null, null);

        // Act
        var result = await process.TickAsync(snap, TestContext.Current.CancellationToken);

        // Assert: Success (заявок нет — no-op), config не переписан.
        result.IsSuccess.Should().BeTrue();
        driver.Removed.Should().BeEmpty();
        etcd.Store["/s3/clusters/demo/config"].ModRevision.Should().Be(modRevisionBefore);
    }
}
