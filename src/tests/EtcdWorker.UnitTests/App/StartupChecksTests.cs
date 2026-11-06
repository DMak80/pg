using EtcdWorker.App;

namespace EtcdWorker.UnitTests.App;

// Fail-fast валидации старта — чистые предикаты EtcdStartupChecks:
// для каждого — валидная конфигурация → true и граничные негативы
// (Program применяет их на ValidateOnStart).
public class StartupChecksTests
{
    // Валидный объект собираем мутацией известных свойств: валидны дефолтные
    // Mode/Network/Thresholds/NodeSnapshot-интервалы, значимое задаём явно.
    private static EtcdWorkerOptions Valid()
    {
        var o = new EtcdWorkerOptions();

        // etcd-шлюз доступен
        o.Etcd.Endpoints = ["http://localhost:2379"];

        // plain-топология: один docker-хост, порт-диапазон и образ заданы
        o.Docker.Hosts = [new DockerHostOptions { Name = "local", Endpoint = "unix:///var/run/docker.sock" }];
        o.Docker.PortRange.From = 23000;
        o.Docker.PortRange.To = 23999;
        o.Docker.Images.EtcdNode = "pgworker/etcdnode:dev";

        // API достижим клиентами по https
        o.Api.AdvertiseUrl = "https://localhost:8080";

        return o;
    }

    [Fact]
    public void EtcdEndpoints_Valid_True_Empty_False()
    {
        // Arrange — валидная конфигурация
        var valid = Valid();

        // Arrange — пустой список endpoints
        var empty = Valid();
        empty.Etcd.Endpoints = [];

        // Assert
        EtcdStartupChecks.EtcdEndpoints(valid).Should().BeTrue();
        EtcdStartupChecks.EtcdEndpoints(empty).Should().BeFalse();
    }

    [Theory]
    [InlineData("https://localhost:8080", false, true)]
    [InlineData("http://localhost:8080", false, false)]
    public void AdvertiseUrl_Https_OrInsecureFlag(string url, bool allowInsecure, bool expected)
    {
        // Arrange
        var o = Valid();
        o.Api.AdvertiseUrl = url;
        o.Api.Tls.AllowInsecureHttp = allowInsecure;

        // Assert — plain http разрешён только с AllowInsecureHttp
        EtcdStartupChecks.AdvertiseUrl(o).Should().Be(expected);
    }

    [Fact]
    public void DockerTopology_Plain_RequiresSingleHostAndNetwork()
    {
        // Arrange — валидная конфигурация (Plain, один хост, сеть)
        var valid = Valid();

        // Arrange — Plain без хостов
        var noHosts = Valid();
        noHosts.Docker.Hosts = [];

        // Arrange — Plain с двумя хостами
        var twoHosts = Valid();
        twoHosts.Docker.Hosts =
        [
            new DockerHostOptions { Name = "local", Endpoint = "unix:///var/run/docker.sock" },
            new DockerHostOptions { Name = "remote", Endpoint = "tcp://remote:2375" },
        ];

        // Arrange — Plain без сети
        var noNetwork = Valid();
        noNetwork.Docker.Network = "";

        // Assert
        EtcdStartupChecks.DockerTopology(valid).Should().BeTrue();
        EtcdStartupChecks.DockerTopology(noHosts).Should().BeFalse();
        EtcdStartupChecks.DockerTopology(twoHosts).Should().BeFalse();
        EtcdStartupChecks.DockerTopology(noNetwork).Should().BeFalse();
    }

    [Fact]
    public void DockerTopology_Swarm_RequiresManager()
    {
        // Arrange — Swarm без manager
        var o = Valid();
        o.Docker.Mode = "Swarm";
        o.Docker.SwarmManager = null;

        // Assert
        EtcdStartupChecks.DockerTopology(o).Should().BeFalse();

        // с manager — валидно
        o.Docker.SwarmManager = "tcp://mgr:2376";
        EtcdStartupChecks.DockerTopology(o).Should().BeTrue();
    }

    [Fact]
    public void Thresholds_NodeBootSec_AtOrBelowCeiling()
    {
        // Arrange — дефолтные пороги (NodeBootSec=100)
        var valid = Valid();

        // Arrange — бюджет готовности выше потолка
        var over = Valid();
        over.Thresholds.NodeBootSec = 101;

        // Assert — NodeBootSec ≤ 100
        EtcdStartupChecks.Thresholds(valid).Should().BeTrue();
        EtcdStartupChecks.Thresholds(over).Should().BeFalse();
    }

    [Theory]
    [InlineData(23000, 23999, true)]
    [InlineData(23999, 23999, false)]
    [InlineData(24000, 23999, false)]
    public void PortRange_FromLessThanTo(int from, int to, bool expected)
    {
        // Arrange
        var o = Valid();
        o.Docker.PortRange.From = from;
        o.Docker.PortRange.To = to;

        // Assert — From ≥ To недопустим
        EtcdStartupChecks.PortRange(o).Should().Be(expected);
    }

    [Fact]
    public void NodeSnapshot_PositiveRetentionAndInterval()
    {
        // Arrange — валидные ретеншн и интервалы (дефолты)
        var valid = Valid();

        // Arrange — нулевая объектная ретеншн
        var noRetention = Valid();
        noRetention.NodeSnapshot.RetentionObjects = 0;

        // Arrange — нулевой интервал снапшотов
        var noInterval = Valid();
        noInterval.NodeSnapshot.IntervalMin = 0;

        // Assert
        EtcdStartupChecks.NodeSnapshot(valid).Should().BeTrue();
        EtcdStartupChecks.NodeSnapshot(noRetention).Should().BeFalse();
        EtcdStartupChecks.NodeSnapshot(noInterval).Should().BeFalse();
    }
}
