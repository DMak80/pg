using EtcdWorker.Core;

namespace EtcdWorker.UnitTests.Core;

// Сборка спецификации managed-узла etcd (EtcdNodeSpecBuilder.Build): контейнер
// образа EtcdNode (etcd + supervisord + агент) — флаги etcd уходят env
// ETCD_ARGS одной строкой (Cmd пуст: контейнер стартует CMD образа),
// конфигурация агента — env ETCDNODE_* (arch/etcdnode/01-agent.md §3).
public class EtcdNodeSpecBuilderTests
{
    private static (string[] Cmd, IReadOnlyDictionary<string, string> Env) Build(
        int k, string? advertisedHost, int clientHostPort, string? s3CaPem = null)
    {
        // Arrange/Act — сборка спеки узла с заданными параметрами
        var opts = new EtcdProvisioningOptions(
            "Plain", "etcdw-net", "img", advertisedHost,
            18100, 18199, "local", 100, 300, 15,
            360, 300, 300, 30, 10, 28, 60, s3CaPem);
        return EtcdNodeSpecBuilder.Build(k, opts, clientHostPort);
    }

    private static string Args(int k, string? advertisedHost, int clientHostPort, string? s3CaPem = null)
        => Build(k, advertisedHost, clientHostPort, s3CaPem).Env["ETCD_ARGS"];

    [Fact]
    public void EnvCarriesEtcdArgs()
    {
        // Arrange/Act — узел 1 без advertised host
        var args = Args(1, null, 23791);

        // Assert — флаги etcd уходят одной env-строкой
        args.Should().Contain("--name=etcdw-node-1");
        args.Should().Contain("--initial-cluster-state=existing");
        args.Should().Contain("--advertise-client-urls=http://etcdw-node-1:2379");
    }

    [Fact]
    public void WithAdvertisedHost_ClientUrlsInArgs()
    {
        // Arrange — узел 1 с публичным хостом и динамическим клиент-портом
        const string expectedUrls =
            "--advertise-client-urls=http://etcdw-node-1:2379,http://10.0.0.5:23791";

        // Act
        var args = Args(1, "10.0.0.5", 23791);

        // Assert — client-список из двух URL ровно в порядке «имя, host:порт»
        args.Should().Contain(expectedUrls);
    }

    [Fact]
    public void InitialClusterPassed()
    {
        // Arrange/Act — join с полным составом
        var opts = new EtcdProvisioningOptions(
            "Plain", "etcdw-net", "img", null, 18100, 18199, "local", 100, 300, 15,
            360, 300, 300, 30, 10, 28, 60, null);
        var (_, env) = EtcdNodeSpecBuilder.Build(2, opts, 23792, "seed=http://seed:2380");

        // Assert — initial-cluster уходит в ETCD_ARGS
        env["ETCD_ARGS"].Should().Contain("--initial-cluster=seed=http://seed:2380");
    }

    [Fact]
    public void HeartbeatElectionQuota_ExactValues()
    {
        // Arrange/Act — сборка спеки узла
        var args = Args(3, null, 23793);

        // Assert — heartbeat 250 мс, election 2000 мс, квота 8 ГБ — точные значения
        args.Should().Contain("--heartbeat-interval=250");
        args.Should().Contain("--election-timeout=2000");
        args.Should().Contain("--quota-backend-bytes=8589934592");
    }

    [Fact]
    public void CmdEmpty_ImageCmdUsed()
    {
        // Arrange/Act — сборка спеки узла
        var (cmd, _) = Build(1, null, 23791);

        // Assert — Cmd НЕ передаётся: контейнер стартует CMD образа (supervisord)
        cmd.Should().BeEmpty();
    }

    [Fact]
    public void AgentEnvPresent()
    {
        // Arrange/Act — сборка спеки без CA-бандла
        var (_, env) = Build(1, null, 23791);

        // Assert — конфигурация агента EtcdNode в env контейнера
        env["ETCDNODE_NODE_NAME"].Should().Be("etcdw-node-1");
        env["ETCDNODE_SNAPSHOTS__INTERVAL_MIN"].Should().Be("360");
        env["ETCDNODE_SNAPSHOTS__RETENTION_OBJECTS"].Should().Be("28");
        env.Should().NotContainKey("ETCDNODE_S3_CA_PEM");
    }

    [Fact]
    public void AgentEnvCarriesCaPem()
    {
        // Arrange/Act — сборка спеки с содержимым PEM per-install CA
        var (_, env) = Build(1, null, 23791, "PEM");

        // Assert — содержимое бандла уходит в env узла (агент материализует
        // его в temp-файл на старте)
        env["ETCDNODE_S3_CA_PEM"].Should().Be("PEM");
    }
}
