using PgWorker.Core.Model;
using PgWorker.Core.Templates;

namespace PgWorker.UnitTests.Templates;

// NodeConfigBuilders: детерминированные конфиги контейнера pgworker-node
// (Spilo env, pg_doorman, haproxy) из arch/configs + параметров spec (P2/P3/P11/P13/P14/P15/P17, Д4).

public class NodeConfigBuildersTests
{
    private static readonly ShardTopology Topology = new(
        "shop", "shard1", "shop-shard1",
        new Dictionary<string, NodeAddress>
        {
            ["shard1a"] = new("h1", new NodePorts(15432, 18008, 16432)),
            ["shard1b"] = new("h2", new NodePorts(15433, 18009, 16433)),
        });

    private static readonly EtcdEndpoints Etcd = new(["http://e1:2379", "http://e2:2379"]);

    private static readonly InstallSecrets Secrets = new(
        "su-secret", "standby-secret", "app-secret", "admin-secret", "mover-secret");

    [Fact]
    public void SpiloEnv_ContainsPatroniAndPgParameters()
    {
        // Arrange: топология шарда из 2 нод и адреса etcd.

        // Act: генерируем env контейнера ноды.
        var env = SpiloEnvBuilder.Build(Topology, Etcd, Secrets);

        // Assert: SPILO_CONFIGURATION несёт P11 (ttl/loop_wait), P3 (wal_level),
        // P15 (max_connections) и callback мастер-ключа.
        var spilo = env["SPILO_CONFIGURATION"];
        spilo.Should().Contain("ttl: 5");
        spilo.Should().Contain("loop_wait: 2");
        spilo.Should().Contain("wal_level: logical");
        spilo.Should().Contain("max_connections: \"60\"");
        spilo.Should().Contain("sync_replication_slots: \"on\"");
        spilo.Should().Contain("on_role_change");
    }

    [Fact]
    public void SpiloEnv_ContainsScopeAndEtcdHosts()
    {
        // Arrange: scope = "<C>-<X>" и список etcd-эндпоинтов.

        // Act: генерируем env контейнера ноды.
        var env = SpiloEnvBuilder.Build(Topology, Etcd, Secrets);

        // Assert: идентификация Patroni-кластера и адреса DCS на месте.
        env["SCOPE"].Should().Be("shop-shard1");
        env["ETCD3_HOSTS"].Should().Be("e1:2379,e2:2379"); // Patroni: host:port без scheme (etcd v3)
        env["PGW_ETCD"].Should().Be("http://e1:2379,http://e2:2379");
        env["PGW_MASTER_KEY"].Should().Be("/clusters/shop/shards/shard1/master");
    }

    [Fact]
    public void Secrets_AreInEnv_ButNeverInDoormanOrHaproxy()
    {
        // Arrange: секреты установки (Д7).

        // Act: генерируем все три конфига.
        var env = SpiloEnvBuilder.Build(Topology, Etcd, Secrets);
        var doorman = DoormanConfigBuilder.Build("shop");
        var haproxy = HaproxyConfigBuilder.Build(Topology);

        // Assert: секреты прокинуты в env контейнера (Spilo/bootstrap),
        // но не попадают в тексты doorman/haproxy (их читаютvolume-маунты).
        env.Values.Should().Contain(new[] { "su-secret", "standby-secret", "app-secret", "admin-secret", "mover-secret" });
        doorman.Should().NotContainAny("su-secret", "standby-secret", "app-secret", "admin-secret", "mover-secret");
        haproxy.Should().NotContainAny("su-secret", "standby-secret", "app-secret", "admin-secret", "mover-secret");
    }

    [Fact]
    public void Doorman_SingleTransactionPoolWithBudget()
    {
        // Arrange: dbname = имя кластера.

        // Act: генерируем конфиг пулера.
        var config = DoormanConfigBuilder.Build("shop");

        // Assert: единственный пул shop → 127.0.0.1:5432, transaction-режим,
        // бюджет 55 серверных соединений (P13/P14/P15).
        config.Should().Contain("pool_mode = \"transaction\"");
        config.Should().Contain("max_db_connections = 55");
        config.Should().Contain("shop = host=127.0.0.1 port=5432 dbname=shop");
    }

    [Fact]
    public void Haproxy_WriteFrontendWithAllNodesAndPatroniChecks()
    {
        // Arrange: шард из 2 нод на разных хостах с выделенными портами.

        // Act: генерируем конфиг HAProxy.
        var config = HaproxyConfigBuilder.Build(Topology);

        // Assert: write-фронтенд :5432, health-check Patroni /primary (P2),
        // все ноды шарда в бэкенде с check-port Patroni.
        config.Should().Contain("bind *:5432");
        config.Should().Contain("option httpchk GET /primary");
        config.Should().Contain("http-check expect status 200");
        config.Should().Contain("server shard1a h1:15432 check port 18008");
        config.Should().Contain("server shard1b h2:15433 check port 18009");
    }
}
