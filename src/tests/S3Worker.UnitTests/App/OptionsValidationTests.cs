using S3Worker.App;

namespace S3Worker.UnitTests.App;

// Fail-fast валидации старта (arch/24 §8) — чистые предикаты S3StartupChecks:
// каждая — граничные кейсы (Program применяет их на ValidateOnStart).
public class OptionsValidationTests
{
    private static S3WorkerOptions Valid() => new()
    {
        Etcd = new EtcdOptions { Endpoints = ["http://localhost:2379"] },
        Bucket = "pgworker-backups",
        Api = new ApiOptions { AdvertiseUrl = "https://localhost:8080" },
    };

    [Fact]
    public void EtcdEndpoints_Empty_Invalid()
    {
        // Arrange
        var o = Valid();
        o.Etcd.Endpoints = [];

        // Assert
        S3StartupChecks.EtcdEndpoints(o).Should().BeFalse();
        S3StartupChecks.EtcdEndpoints(Valid()).Should().BeTrue();
    }

    [Theory]
    [InlineData("https://host:8080", false, true)]
    [InlineData("", false, false)]
    [InlineData("   ", false, false)]
    [InlineData("http://host:8080", false, false)]
    [InlineData("http://host:8080", true, true)]
    public void AdvertiseUrl_HttpsOrInsecureFlag(string url, bool allowInsecure, bool expected)
    {
        // Arrange
        var o = Valid();
        o.Api.AdvertiseUrl = url;
        o.Api.Tls.AllowInsecureHttp = allowInsecure;

        // Assert — https:// обязателен (mTLS-only, arch/24 §1.1); AllowInsecureHttp — WAF-лазейка
        S3StartupChecks.AdvertiseUrl(o).Should().Be(expected);
    }

    [Fact]
    public void DockerTopology_Plain_RequiresDistinctNonEmptyHosts()
    {
        // Arrange — корректная plain-таблица
        var o = Valid();
        o.Docker.Hosts = [new DockerHostOptions { Name = "h1", Endpoint = "tcp://h1:2375" }];

        // Assert
        S3StartupChecks.DockerTopology(o).Should().BeTrue();

        // пустая таблица — нет
        var empty = Valid();
        empty.Docker.Hosts = [];
        S3StartupChecks.DockerTopology(empty).Should().BeFalse();

        // дубликат имён — нет (имя — атрибут docker_host, arch/23 §4)
        var dup = Valid();
        dup.Docker.Hosts =
        [
            new DockerHostOptions { Name = "h1", Endpoint = "tcp://h1:2375" },
            new DockerHostOptions { Name = "h1", Endpoint = "tcp://h1b:2375" },
        ];
        S3StartupChecks.DockerTopology(dup).Should().BeFalse();
    }

    [Fact]
    public void DockerTopology_Swarm_RequiresManager()
    {
        // Arrange — swarm без manager
        var o = Valid();
        o.Docker.Mode = "Swarm";
        o.Docker.Hosts = [];
        o.Docker.SwarmManager = null;

        // Assert
        S3StartupChecks.DockerTopology(o).Should().BeFalse();

        // с manager — да
        o.Docker.SwarmManager = "tcp://mgr:2376";
        S3StartupChecks.DockerTopology(o).Should().BeTrue();
    }

    [Theory]
    [InlineData(100, 90, 15, true)]
    [InlineData(101, 90, 15, false)]
    [InlineData(0, 90, 15, false)]
    [InlineData(100, 0, 15, false)]
    [InlineData(100, 90, 0, false)]
    public void Thresholds_PositiveAndBootCeiling(int boot, int dead, int health, bool expected)
    {
        // Arrange
        var o = Valid();
        o.Thresholds.NodeBootSec = boot;
        o.Thresholds.NodeDeadSec = dead;
        o.Thresholds.HealthIntervalSec = health;

        // Assert — NodeBootSec ≤ 100 (spec §14.12)
        S3StartupChecks.Thresholds(o).Should().Be(expected);
    }

    [Theory]
    [InlineData("pgworker-backups", true)]
    [InlineData("a", true)]
    [InlineData("a.b_c-9", true)]
    [InlineData("", false)]
    [InlineData("Upper", false)]
    [InlineData("-lead", false)]
    [InlineData("with space", false)]
    [InlineData("111111111111111111111111111111111111111111111111111111111111111", true)]
    [InlineData("1111111111111111111111111111111111111111111111111111111111111111", false)]
    public void Bucket_Pattern(string bucket, bool expected)
    {
        // Arrange
        var o = Valid();
        o.Bucket = bucket;

        // Assert — `^[a-z0-9][a-z0-9_.-]{0,62}$`
        S3StartupChecks.Bucket(o).Should().Be(expected);
    }

    [Theory]
    [InlineData(18000, 18999, true)]
    [InlineData(0, 100, false)]
    [InlineData(100, 100, false)]
    [InlineData(200, 100, false)]
    [InlineData(100, 65536, false)]
    [InlineData(100, 65535, true)]
    public void PortRange_OrderAndBounds(int from, int to, bool expected)
    {
        // Arrange
        var o = Valid();
        o.Docker.PortRange.From = from;
        o.Docker.PortRange.To = to;

        // Assert
        S3StartupChecks.PortRange(o).Should().Be(expected);
    }
}
