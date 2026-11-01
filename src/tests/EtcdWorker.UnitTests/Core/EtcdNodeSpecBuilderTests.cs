using EtcdWorker.Core;

namespace EtcdWorker.UnitTests.Core;

// Сборка спецификации узла etcd (EtcdNodeSpecBuilder.Build): аргументы запуска
// etcdw-node-<k> — advertise-client-urls (имя + опциональный host:порт),
// initial-cluster-state=existing, фиксированные heartbeat/election/quota.
public class EtcdNodeSpecBuilderTests
{
    private const string ClientUrlsWithoutHost = "--advertise-client-urls=http://etcdw-node-1:2379";

    private static string[] Cmd(int k, string? advertisedHost, int clientHostPort)
    {
        // Arrange/Act — сборка спеки узла с заданными параметрами
        return EtcdNodeSpecBuilder.Build(k, advertisedHost, clientHostPort).Cmd;
    }

    [Fact]
    public void WithAdvertisedHost_ClientUrls_NameFirstThenHostPort()
    {
        // Arrange — узел 1 с публичным хостом и динамическим клиент-портом
        const string expectedUrls =
            "--advertise-client-urls=http://etcdw-node-1:2379,http://10.0.0.5:23791";

        // Act
        var cmd = Cmd(1, "10.0.0.5", 23791);

        // Assert — client-список из двух URL ровно в порядке «имя, host:порт»
        cmd.Should().Contain(expectedUrls);
    }

    [Fact]
    public void WithoutAdvertisedHost_ClientUrls_SingleNameUrl()
    {
        // Arrange — узел 1 без advertised host

        // Act
        var cmd = Cmd(1, null, 23791);

        // Assert — один URL по имени узла, хвост «,http://...» отсутствует
        cmd.Should().Contain(ClientUrlsWithoutHost);
        cmd.Should().NotContain(s => s.Contains(",http://"));
    }

    [Fact]
    public void InitialClusterState_IsExisting()
    {
        // Arrange/Act — узел присоединяется к существующему контуру
        var cmd = Cmd(2, null, 23792);

        // Assert — состояние кластера строго existing
        cmd.Should().Contain("--initial-cluster-state=existing");
    }

    [Fact]
    public void HeartbeatElectionQuota_ExactValues()
    {
        // Arrange/Act — сборка спеки узла
        var cmd = Cmd(3, null, 23793);

        // Assert — heartbeat 250 мс, election 2000 мс, квота 8 ГБ — точные значения
        cmd.Should().Contain("--heartbeat-interval=250");
        cmd.Should().Contain("--election-timeout=2000");
        cmd.Should().Contain("--quota-backend-bytes=8589934592");
    }

    [Fact]
    public void Env_IsEmpty()
    {
        // Arrange/Act — сборка спеки узла
        var (_, env) = EtcdNodeSpecBuilder.Build(1, "10.0.0.5", 23791);

        // Assert — переменные окружения не используются
        env.Should().BeEmpty();
    }
}
