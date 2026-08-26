using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Probes;
using PgWorker.UnitTests.Provisioning;
using Xunit;

namespace PgWorker.UnitTests.Moves;

// ShardEndpoints (t01 задача 9): адресация мастеров (перенос ResolveMasterAsync
// из BucketEvacuator) + DSN-билдеры — admin-Npgsql, mover-libpq-conninfo,
// mover-Npgsql-DSN для SQL-проб роли bucket_mover (spec §6.1 M0, ревью №2).
public class ShardEndpointsTests
{
    private const string Ep = "http://etcd:2379";

    private static ShardEndpoints EndpointsOf(Fakes.FakeEtcd etcd)
        => new(etcd, [Ep], new ShardProbe(new HttpClient()));

    private static void SeedPortalloc(Fakes.FakeEtcd etcd) => etcd.Seed(
        "/pgworker/portalloc/shop",
        Portalloc.Serialize(new Dictionary<string, NodeAddress>
        {
            ["shard1/shard1a"] = new("h1", new NodePorts(15000, 18000, 16500)),
            ["shard1/shard1b"] = new("h2", new NodePorts(15001, 18001, 16501)),
            ["shard2/shard2a"] = new("h1", new NodePorts(15002, 18002, 16502)),
        }));

    private static ShardSpec Shard1(string? master) => new(
        "shard1", 2, Dsn: null, Master: master,
        Nodes:
        [
            new NodeSpec("shard1", "shard1a", NodeState.Running),
            new NodeSpec("shard1", "shard1b", NodeState.Running),
        ]);

    // AAA: mover-conninfo — user подменён, пароль добавлен, host-часть сохранена (P2/P17)
    [Fact]
    public void MoverConninfo_SwapsUserAddsPassword()
    {
        // Arrange
        var dsnKey = "host=n1,n2,n3 port=15432,15433,15434 dbname=shop user=bucket_admin";
        var secrets = new InstallSecrets("su", "sb", "app", "adm", "moverpw");

        // Act
        var conninfo = ShardEndpoints.MoverConninfo(dsnKey, secrets);

        // Assert
        conninfo.Should().Be("host=n1,n2,n3 port=15432,15433,15434 dbname=shop user=bucket_mover password=moverpw sslmode=require target_session_attrs=read-write");
    }

    // AAA: dsn без user= — user добавляется (не теряем вход)
    [Fact]
    public void MoverConninfo_AppendsUserIfMissing()
    {
        // Act
        var conninfo = ShardEndpoints.MoverConninfo("host=n1 dbname=shop", new InstallSecrets("s", "s", "s", "s", "moverpw"));

        // Assert
        conninfo.Should().Be("host=n1 dbname=shop user=bucket_mover password=moverpw sslmode=require target_session_attrs=read-write");
    }

    // AAA: advertisedHost — подмена хостов издателя для контейнеров приёмника
    // (single-host стенды: подписка из контейнера по localhost бьёт в сам контейнер);
    // ПОЭЛЕМЕНТНО — libpq требует соответствия числа host и port
    [Fact]
    public void MoverConninfo_AdvertisedHost_ReplacesHosts()
    {
        // Act
        var conninfo = ShardEndpoints.MoverConninfo(
            "host=n1,n2 port=1,2 dbname=shop user=bucket_admin",
            new InstallSecrets("s", "s", "s", "s", "pw"), "host.docker.internal");

        // Assert
        conninfo.Should().Be(
            "host=host.docker.internal,host.docker.internal port=1,2 dbname=shop user=bucket_mover password=pw sslmode=require target_session_attrs=read-write");
    }

    // AAA: multi-host conninfo ЦЕЛИТся в писателя (add-кластер, 2026-08-26):
    // libpq без target_session_attrs берёт ПЕРВЫЙ доступный хост — слот failover
    // создавался на стендбае источника («cannot enable failover for a replication
    // slot created on the standby»); read-write — семантический эквивалент
    // HAProxy-входа скриптов, переживающий failover источника (переподключение
    // заново выбирает писателя)
    [Fact]
    public void MoverConninfo_MultiHost_TargetsReadWritePrimary()
    {
        // Arrange — источник из двух нод, лидер — вторая
        var dsnKey = "host=standby,primary port=15006,15007 dbname=add user=bucket_admin";

        // Act
        var conninfo = ShardEndpoints.MoverConninfo(dsnKey, new InstallSecrets("s", "s", "s", "s", "pw"));

        // Assert
        conninfo.Should().EndWith("target_session_attrs=read-write");
    }

    // AAA: mover-Npgsql-DSN — libpq→Npgsql конвертация для SQL-проб роли (spec §6.1 M0);
    // разные порты нод — только парами host:port (список портов в Port= Npgsql отвергает)
    [Fact]
    public void MoverNpgsqlDsn_ConvertsLibpqToNpgsql()
    {
        // Arrange
        var dsnKey = "host=n1,n2,n3 port=15432,15433,15434 dbname=shop user=bucket_admin";
        var secrets = new InstallSecrets("su", "sb", "app", "adm", "moverpw");

        // Act
        var dsn = ShardEndpoints.MoverNpgsqlDsn(dsnKey, secrets);

        // Assert
        dsn.Should().Be("Host=n1:15432,n2:15433,n3:15434;Database=shop;Username=bucket_mover;Password=moverpw;SSL Mode=Require;Trust Server Certificate=true;Target Session Attributes=read-write");
    }

    // AAA: Npgsql-DSN без user= — Username добавляется, пароль всегда
    [Fact]
    public void MoverNpgsqlDsn_MissingUser_AddsUsername()
    {
        // Act
        var dsn = ShardEndpoints.MoverNpgsqlDsn("host=n1 port=1 dbname=d", new InstallSecrets("s", "s", "s", "s", "pw"));

        // Assert
        dsn.Should().Be("Host=n1;Port=1;Database=d;Username=bucket_mover;Password=pw;SSL Mode=Require;Trust Server Certificate=true;Target Session Attributes=read-write");
    }

    // AAA: admin-DSN мастера — postgres + пароль Д7 (паттерн BuildAdminDsn)
    [Fact]
    public void AdminDsn_UsesPgPortOfMaster()
    {
        // Arrange
        var master = new NodeAddress("h1", new NodePorts(15000, 18000, 16500));
        var secrets = new InstallSecrets("su-pw", "sb", "app", "adm", "mov");

        // Act
        var dsn = ShardEndpoints.AdminDsn(master, "shop", secrets);

        // Assert
        dsn.Should().Be("Host=h1;Port=15000;Database=shop;Username=postgres;Password=su-pw;SSL Mode=Require;Trust Server Certificate=true");
    }

    // AAA: portalloc читается префиксом кластера, ключа нет → пустой словарь
    [Fact]
    public async Task ReadPortAllocAsync_ParsesClusterAddresses()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd();
        SeedPortalloc(etcd);

        // Act
        var addresses = await EndpointsOf(etcd).ReadPortAllocAsync("shop", CancellationToken.None);
        var empty = await EndpointsOf(new Fakes.FakeEtcd()).ReadPortAllocAsync("shop", CancellationToken.None);

        // Assert
        addresses.Value.Should().HaveCount(3);
        addresses.Value["shard1/shard1a"].Host.Should().Be("h1");
        addresses.Value["shard1/shard1a"].Ports.Pg.Should().Be(15000);
        empty.Value.Should().BeEmpty("нет ключа portalloc — адресов нет");
    }

    // AAA: master-ключ → адрес ноды по ИМЕНИ (перенос ResolveMasterAsync из эвакуатора)
    [Fact]
    public async Task ResolveMasterAsync_MasterKeyByNodeName_ResolvesAddress()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd();
        SeedPortalloc(etcd);
        etcd.Seed("/clusters/shop/shards/shard1/master", "shard1a:18000");
        var endpoints = EndpointsOf(etcd);
        var addresses = await endpoints.ReadPortAllocAsync("shop", CancellationToken.None);

        // Act
        var master = await endpoints.ResolveMasterAsync(Shard1("shard1a:18000"), addresses.Value, CancellationToken.None);

        // Assert
        master.Value.Should().NotBeNull();
        master.Value!.Host.Should().Be("h1", "master-ключ назвал ноду shard1a — она на хосте h1");
        master.Value.Ports.Pg.Should().Be(15000);
    }

    // AAA: master-ключ → адрес по HOST (host неуникален: на нём ноды разных шардов —
    // поиск только среди нод этого шарда)
    [Fact]
    public async Task ResolveMasterAsync_MasterKeyByHost_LooksWithinShardOnly()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd();
        SeedPortalloc(etcd);
        etcd.Seed("/clusters/shop/shards/shard1/master", "h2:16501");
        var endpoints = EndpointsOf(etcd);
        var addresses = await endpoints.ReadPortAllocAsync("shop", CancellationToken.None);

        // Act
        var master = await endpoints.ResolveMasterAsync(Shard1("h2:16501"), addresses.Value, CancellationToken.None);

        // Assert
        master.Value.Should().NotBeNull();
        master.Value!.Host.Should().Be("h2", "host h2 в master-ключе — единственная нода шарда на нём: shard1b");
        master.Value.Ports.Pg.Should().Be(15001);
    }

    // AAA: master-ключа нет и Patroni-фолбэк недоступен — null (ждём, e2e закрывает ветку)
    [Fact]
    public async Task ResolveMasterAsync_NoMasterNoPatroni_ReturnsNull()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd();
        SeedPortalloc(etcd);
        var endpoints = EndpointsOf(etcd);
        var addresses = await endpoints.ReadPortAllocAsync("shop", CancellationToken.None);

        // Act
        var master = await endpoints.ResolveMasterAsync(Shard1(null), addresses.Value, CancellationToken.None);

        // Assert
        master.Value.Should().BeNull("ни master-ключа, ни Patroni-ответа — мастера нет");
    }
}
