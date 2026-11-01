using System.Text.Json;
using EtcdWorker.App;
using EtcdWorker.App.Api.Operations;
using FluentAssertions;
using Xunit;

namespace EtcdWorker.UnitTests.App;

// Эффективный конфиг (arch/25 §1.1 GET /api/config): состав групп ровно по
// канону, значения — из EtcdWorkerOptions; секретные секции (Etcd-endpoints,
// Api/TLS, Docker Tls/Ssh, Snapshots.Dir/Hosts) в тело НЕ попадают.
public class EffectiveConfigTests
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    // Не-дефолтные значения: перепутанные поля ловятся сравнением.
    private static EtcdWorkerOptions Sample() => new()
    {
        Etcd = new EtcdOptions { Endpoints = ["http://secret-etcd:2379"] },
        Api = new ApiOptions { AdvertiseUrl = "https://secret-api:8080" },
        Thresholds = new ThresholdsOptions { NodeBootSec = 55, NodeDeadSec = 666, HealthIntervalSec = 7 },
        Loops = new LoopsOptions
        {
            ScanIntervalSec = 3,
            KeepaliveSec = 4,
            Watchdog = new Shared.Core.Hosting.WatchdogOptions
                { Enabled = true, Multiplier = 5, CheckIntervalSec = 6 },
        },
        Snapshots = new SnapshotOptions
        {
            Dir = "/secret-snapshots",
            IntervalMin = 11,
            MinIntervalSec = 22,
            MaintenanceIntervalMin = 33,
            RetentionFiles = 44,
            RetentionObjects = 55,
            RetryIntervalSec = 66,
            TimeoutSec = 77,
        },
        Docker = new DockerOptions
        {
            Mode = "Plain",
            Network = "etcdw-net",
            AdvertisedHost = "host.docker.internal",
            PortRange = new PortRangeOptions { From = 18001, To = 18002 },
            Images = new DockerImagesOptions { Etcd = "etcd-img" },
            Hosts = [new DockerHostOptions { Name = "local", Endpoint = "unix:///var/run/docker.sock" }],
        },
    };

    [Fact]
    public void From_CanonicalOptions_MapsAllGroups()
    {
        // Arrange
        var o = Sample();

        // Act
        using var doc = JsonDocument.Parse(
            JsonSerializer.Serialize(EtcdWorkerEffectiveConfig.From(o), Json));

        // Assert — каждое поле равно значению Options (не-дефолты ловят путаницу полей)
        var root = doc.RootElement;
        root.GetProperty("source").GetString().Should().Be("env");

        var thresholds = root.GetProperty("thresholds");
        thresholds.GetProperty("nodeBootSec").GetInt32().Should().Be(55);
        thresholds.GetProperty("nodeDeadSec").GetInt32().Should().Be(666);
        thresholds.GetProperty("healthIntervalSec").GetInt32().Should().Be(7);

        var loops = root.GetProperty("loops");
        loops.GetProperty("scanIntervalSec").GetInt32().Should().Be(3);
        loops.GetProperty("keepaliveSec").GetInt32().Should().Be(4);
        var watchdog = loops.GetProperty("watchdog");
        watchdog.GetProperty("enabled").GetBoolean().Should().Be(true);
        watchdog.GetProperty("multiplier").GetInt32().Should().Be(5);
        watchdog.GetProperty("checkIntervalSec").GetInt32().Should().Be(6);

        var snapshots = root.GetProperty("snapshots");
        snapshots.GetProperty("intervalMin").GetInt32().Should().Be(11);
        snapshots.GetProperty("minIntervalSec").GetInt32().Should().Be(22);
        snapshots.GetProperty("maintenanceIntervalMin").GetInt32().Should().Be(33);
        snapshots.GetProperty("retentionFiles").GetInt32().Should().Be(44);
        snapshots.GetProperty("retentionObjects").GetInt32().Should().Be(55);
        snapshots.GetProperty("retryIntervalSec").GetInt32().Should().Be(66);
        snapshots.GetProperty("timeoutSec").GetInt32().Should().Be(77);

        var docker = root.GetProperty("docker");
        docker.GetProperty("mode").GetString().Should().Be("Plain");
        docker.GetProperty("network").GetString().Should().Be("etcdw-net");
        docker.GetProperty("advertisedHost").GetString().Should().Be("host.docker.internal");
        var portRange = docker.GetProperty("portRange");
        portRange.GetProperty("from").GetInt32().Should().Be(18001);
        portRange.GetProperty("to").GetInt32().Should().Be(18002);
        docker.GetProperty("images").GetProperty("etcd").GetString().Should().Be("etcd-img");
    }

    [Fact]
    public void From_SerializedBody_HasNoSecretKeys()
    {
        // Arrange
        var o = Sample();

        // Act
        using var doc = JsonDocument.Parse(
            JsonSerializer.Serialize(EtcdWorkerEffectiveConfig.From(o), Json));

        // Assert — ровно 5 ключей верхнего уровня канона (arch/25 §1.1):
        // любой ЛИШНИЙ ( unmapped-поле Options, добавленное «для диагностики»)
        // валит тест — секретные секции наружу не выходят по whitelist, а здесь
        // лочится сам состав тела.
        var topKeys = doc.RootElement.EnumerateObject().Select(p => p.Name).OrderBy(n => n).ToList();
        topKeys.Should().Equal("docker", "loops", "snapshots", "source", "thresholds");

        // Секретные секции не выходят и по JSON-тексту (защита в глубину).
        var text = doc.RootElement.GetRawText();
        text.Should().NotContain("\"tls\"");
        text.Should().NotContain("\"ssh\"");
        text.Should().NotContain("\"endpoints\"");
        text.Should().NotContain("\"dir\"");
        text.Should().NotContain("\"hosts\"");
        text.Should().NotContain("secret");
    }
}
