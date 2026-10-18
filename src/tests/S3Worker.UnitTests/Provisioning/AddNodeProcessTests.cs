using FluentAssertions;
using S3Worker.Core.Model;
using S3Worker.Docker.Drivers;
using S3Worker.Provisioning;
using S3Worker.Provisioning.Processes;
using Xunit;

namespace S3Worker.UnitTests.Provisioning;

// AddNode D (arch/24 §5 D): mini-A для NOT_INITIALIZED-ноды (placement +
// portalloc + контейнер + bucket + RUNNING) + завершающее окно RMW endpoints
// + актуализация config.nodes (txn mod_revision; актуален — no-op).
public class AddNodeProcessTests
{
    private const string Prefix = "/s3worker";

    private static (AddNodeProcess Process, Fakes.FakeEtcd Etcd, Fakes.FakeS3Driver Driver, Fakes.FakeNodeClient Nodes, ClaimStore Claims) NewRig()
    {
        var etcd = new Fakes.FakeEtcd();
        var driver = new Fakes.FakeS3Driver
        {
            Hosts =
            [
                new S3HostAttributes("h1", 0, new Dictionary<string, string>()),
                new S3HostAttributes("h2", 0, new Dictionary<string, string>()),
            ],
        };
        var nodeClient = new Fakes.FakeNodeClient();
        var claims = new ClaimStore(Prefix, ["http://etcd:2379"], etcd, TimeProvider.System);
        // Занятость docker-публикаций: ноды кластера уже сидят на 18000/18001.
        driver.BusyPorts.Add(("h1", 18000));
        driver.BusyPorts.Add(("h2", 18001));
        var secrets = new SecretProfileStore(
            name => name == $"S3W_NODE_SECRETS__backups-main__ROOT_USER" ? "root"
                : name == $"S3W_NODE_SECRETS__backups-main__ROOT_PASSWORD" ? "pw" : null,
            ["S3W_NODE_SECRETS__backups-main__ROOT_USER", "S3W_NODE_SECRETS__backups-main__ROOT_PASSWORD"]);
        var process = new AddNodeProcess(
            etcd, ["http://etcd:2379"], driver, claims,
            new WorkJournal(Prefix, etcd, ["http://etcd:2379"]),
            new PortAllocLock(Prefix, ["http://etcd:2379"], etcd, TimeProvider.System, "inst-1"),
            new PortAllocIndex(etcd, ["http://etcd:2379"],
                Microsoft.Extensions.Logging.Abstractions.NullLogger<PortAllocIndex>.Instance),
            secrets, new PkiStore(etcd, ["http://etcd:2379"], TimeProvider.System), new InstanceEnvComposer(),
            nodeClient,
            new S3ProvisioningOptions(18000, 18999, 100, 90, 15, null, "owns3:dev", "pgworker-backups"));
        return (process, etcd, driver, nodeClient, claims);
    }

    // Active-кластер N=2 + заявка node3 NOT_INITIALIZED.
    private static async Task<Fakes.FakeEtcd> SeedAsync(
        Fakes.FakeEtcd etcd, ClaimStore claims, int configNodes = 2, bool withNode3 = true)
    {
        etcd.Seed("/s3/clusters/demo/config",
            $$"""{"platform":"owns3","nodes":{{configNodes}},"secret_profile":"backups-main","created_unix":1760156800}""");
        etcd.Seed("/s3/clusters/demo/nodes/node1/state", S3NodeStates.Running);
        etcd.Seed("/s3/clusters/demo/nodes/node2/state", S3NodeStates.Running);
        etcd.Seed("/s3/clusters/demo/nodes/node1/placed_on", "h1");
        etcd.Seed("/s3/clusters/demo/nodes/node2/placed_on", "h2");
        etcd.Seed($"{Prefix}/portalloc/demo", """{"node1":{"host":"h1","s3":18000},"node2":{"host":"h2","s3":18001}}""");
        if (withNode3)
            etcd.Seed("/s3/clusters/demo/nodes/node3/state", S3NodeStates.NotInitialized);
        await claims.TryClaimClusterAsync("demo", TestContext.Current.CancellationToken);
        return etcd;
    }

    private static S3ClusterSnapshot Snap(int declaredNodes = 2) => new(
        "demo",
        new S3ClusterConfig("owns3", declaredNodes, "backups-main", 1760156800, null),
        [
            new S3NodeSnapshot("node1", S3NodeStates.Running, "h1", new S3NodePlacement("h1", null, null),
                new S3NodeResources("1", "1Gi", "5Gi"), null, null),
            new S3NodeSnapshot("node2", S3NodeStates.Running, "h2", new S3NodePlacement("h2", null, null),
                new S3NodeResources("1", "1Gi", "5Gi"), null, null),
            new S3NodeSnapshot("node3", S3NodeStates.NotInitialized, null, new S3NodePlacement("h2", null, null),
                new S3NodeResources("1", "1Gi", "5Gi"), null, null),
        ],
        null, null);

    // AAA: mini-A + завершающее окно: node3 RUNNING, endpoints с третьим
    // адресом, config.nodes актуализирован (txn mod_revision).
    [Fact]
    public async Task NodeRequest_MiniA_AndTopology()
    {
        // Arrange
        var (process, etcd, driver, nodeClient, claims) = NewRig();
        await SeedAsync(etcd, claims);

        // Act
        var result = await process.TickAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert: нода поднята; portalloc с node3; endpoints — 3 адреса
        // (прежние на месте); config.nodes = 3; bucket создан.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        etcd.Store["/s3/clusters/demo/nodes/node3/state"].Value.Should().Be(S3NodeStates.Running);
        etcd.Store[$"{Prefix}/portalloc/demo"].Value.Should().Contain("node3");
        etcd.Store["/s3/clusters/demo/endpoints"].Value.Should().Be("h1:18000,h2:18001,h2:18002");
        etcd.Store["/s3/clusters/demo/config"].Value
            .Should().Be("""{"platform":"owns3","nodes":3,"secret_profile":"backups-main","created_unix":1760156800}""");
        driver.Ensured.Should().ContainSingle().Which.NodeName.Should().Be("node3");
        nodeClient.Buckets.Should().ContainSingle()
            .Which.Bucket.Should().Be("pgworker-backups");
    }

    // AAA: идемпотентность — nodes уже равен фактическому составу →
    // config-txn не выполняется (no-op; mod_revision не меняется).
    [Fact]
    public async Task ConfigAlreadyActual_NoOp()
    {
        // Arrange: config.nodes=3 уже; заявка node3 есть (повторный тик).
        var (process, etcd, driver, _, claims) = NewRig();
        await SeedAsync(etcd, claims, configNodes: 3);
        var modRevisionBefore = etcd.Store["/s3/clusters/demo/config"].ModRevision;

        // Act
        var result = await process.TickAsync(Snap(declaredNodes: 3), TestContext.Current.CancellationToken);

        // Assert: config не переписан (mod_revision тот же).
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        etcd.Store["/s3/clusters/demo/config"].ModRevision.Should().Be(modRevisionBefore);
    }

    // AAA: mini-A — env инстанса собирается композером (OWNS3_REPLICATION_*),
    // имя берётся из декларации (не node<k>).
    [Fact]
    public async Task AddNode_EnvWithReplication_NameFromDeclaration()
    {
        // Arrange: заявка node3 (имя произвольное «west1») на Active-кластере.
        var (process, etcd, driver, nodeClient, claims) = NewRig();
        await SeedAsync(etcd, claims);
        etcd.Seed("/s3/clusters/demo/nodes/west1/state", S3NodeStates.NotInitialized);
        etcd.Seed("/s3/clusters/demo/nodes/west1/placement", """{"docker_host":"h1"}""");
        etcd.Seed("/s3/clusters/demo/nodes/west1/resources", """{"cpu":"1","mem":"1Gi","disk":"5Gi"}""");
        await claims.TryClaimClusterAsync("demo", TestContext.Current.CancellationToken);
        nodeClient.Health = true;
        var snap = new S3ClusterSnapshot("demo",
            new S3ClusterConfig("owns3", 3, "backups-main", 1760156800, null),
            [
                new S3NodeSnapshot("node1", S3NodeStates.Running, "h1", new S3NodePlacement("h1", null, null),
                    new S3NodeResources("1", "1Gi", "5Gi"), null, null),
                new S3NodeSnapshot("node2", S3NodeStates.Running, "h1", new S3NodePlacement("h1", null, null),
                    new S3NodeResources("1", "1Gi", "5Gi"), null, null),
                new S3NodeSnapshot("west1", S3NodeStates.NotInitialized, null, new S3NodePlacement("h1", null, null),
                    new S3NodeResources("1", "1Gi", "5Gi"), null, null),
            ],
            null, null, null);

        // Act
        var result = await process.TickAsync(snap, TestContext.Current.CancellationToken);

        // Assert: контейнер west1 с PKI-материалом в env; pki-ключ записан.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        var ensured = driver.Ensured.Should().ContainSingle(e => e.NodeName == "west1").Subject;
        ensured.Env.Keys.Should().Contain("OWNS3_REPLICATION_CERT");
        ensured.Env.Keys.Should().Contain("OWNS3_REPLICATION_TRUST_CA");
        etcd.Store.Keys.Should().Contain("/s3/clusters/demo/nodes/west1/pki");
    }

    // AAA: без клэйма — никаких docker-мутаций.
    [Fact]
    public async Task NoClaim_NoMutations()
    {
        // Arrange: клэйм чужой.
        var (process, etcd, driver, _, claims) = NewRig();
        await SeedAsync(etcd, new ClaimStore(Prefix, ["http://etcd:2379"], etcd, TimeProvider.System));

        // Act
        var result = await process.TickAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert
        result.IsSuccess.Should().BeFalse();
        driver.Ensured.Should().BeEmpty();
        etcd.Store.Keys.Should().NotContain("/s3/clusters/demo/nodes/node3/placed_on");
    }
}
