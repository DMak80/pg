using FluentAssertions;
using S3Worker.Core.Model;
using S3Worker.Provisioning.Processes;
using Xunit;

namespace S3Worker.UnitTests.Provisioning;

// Общие хелперы процессов: ключи etcd точны (arch/23 §8), ресурсы/roundtrip
// portalloc по канону.
public class S3ProcessCommonTests
{
    [Fact]
    public void КлючиПутиТочны()
    {
        // Arrange + Act + Assert: пути arch/23 §8 один в один.
        S3ProcessCommon.ConfigKey("c").Should().Be("/s3/clusters/c/config");
        S3ProcessCommon.EndpointsKey("c").Should().Be("/s3/clusters/c/endpoints");
        S3ProcessCommon.NodeStateKey("c", "node2").Should().Be("/s3/clusters/c/nodes/node2/state");
        S3ProcessCommon.NodePlacementKey("c", "node1").Should().Be("/s3/clusters/c/nodes/node1/placement");
        S3ProcessCommon.NodeResourcesKey("c", "node1").Should().Be("/s3/clusters/c/nodes/node1/resources");
        S3ProcessCommon.NodePlacedOnKey("c", "node1").Should().Be("/s3/clusters/c/nodes/node1/placed_on");
        S3ProcessCommon.NodeUsageKey("c", "node1").Should().Be("/s3/clusters/c/nodes/node1/usage");
        S3ProcessCommon.PortAllocKey("c").Should().Be("/s3worker/portalloc/c");
        S3ProcessCommon.RotationKey("c").Should().Be("/s3worker/rotations/c");
        S3ProcessCommon.WorkKey("c").Should().Be("/s3worker/work/c");
        S3ProcessCommon.ClusterPrefix("c").Should().Be("/s3/clusters/c/");
    }

    [Fact]
    public void ParseResources_КанонИМусор()
    {
        // Arrange: каноническая декларация и мусорная строка.
        var canonical = new S3NodeResources("2", "4Gi", "200Gi");
        var junk = new S3NodeResources("abc", "far", "1Pi");

        // Act
        var parsed = S3ProcessCommon.ParseResources(canonical);
        var junkParsed = S3ProcessCommon.ParseResources(junk);
        var nullParsed = S3ProcessCommon.ParseResources(null);

        // Assert: cpu decimal, mem → bytes; мусор → null-лимиты (толерантность);
        // disk не парсится (декларация, arch/23 §2).
        parsed.Should().NotBeNull();
        parsed!.Value.Cpu.Should().Be(2m);
        parsed.Value.MemBytes.Should().Be(4L * 1024 * 1024 * 1024);
        junkParsed.Should().NotBeNull();
        junkParsed!.Value.Cpu.Should().BeNull();
        junkParsed.Value.MemBytes.Should().BeNull();
        nullParsed.Should().BeNull();
    }

    [Fact]
    public void ParseResources_FractionalCpu()
    {
        // Arrange: cpu "0.5".
        var resources = new S3NodeResources("0.5", "1Gi", "5Gi");

        // Act
        var parsed = S3ProcessCommon.ParseResources(resources);

        // Assert
        parsed!.Value.Cpu.Should().Be(0.5m);
        parsed.Value.MemBytes.Should().Be(1024L * 1024 * 1024);
    }

    [Fact]
    public void PortAlloc_RoundtripКаноническогоФормата()
    {
        // Arrange: формат arch/23 §8 {"node<k>":{"host":"h","s3":18001}}.
        var alloc = new Dictionary<string, NodeAddress>
        {
            ["node1"] = new("h1", 18001),
            ["node2"] = new("h2", 18002),
        };

        // Act
        var json = S3ProcessCommon.SerializePortAlloc(alloc);
        var parsed = S3ProcessCommon.ParsePortAlloc(json);

        // Assert: roundtrip без потерь; ключи в JSON отсортированы Ordinal.
        parsed.Should().BeEquivalentTo(alloc);
        json.Should().Be("""{"node1":{"host":"h1","s3":18001},"node2":{"host":"h2","s3":18002}}""");
    }

    [Fact]
    public void ParseEndpoint_ПерваяПараИДефолтПорта()
    {
        // Arrange: канонический endpoints и адрес без порта.
        const string endpoints = "h1:18001,h2:18002";

        // Act
        var first = S3ProcessCommon.ParseEndpoint(endpoints);
        var noPort = S3ProcessCommon.ParseEndpoint("h1");

        // Assert: первый адрес; порт без числа — канон-дефолт 9000.
        first.Should().Be(("h1", 18001));
        noPort.Should().Be(("h1", 9000));
    }

    // ============ зонды t33: ключи/сериализаторы/парсеры (arch/23 §3/§8) ============

    [Fact]
    public void КлючиЗондаТочны()
    {
        // Arrange + Act + Assert: пути arch/23 §3/§8 один в один.
        S3ProcessCommon.ConnectivityKey("c").Should().Be("/s3/clusters/c/connectivity");
        S3ProcessCommon.SyncKey("c").Should().Be("/s3/clusters/c/sync");
        S3ProcessCommon.ProbeRequestKey("c").Should().Be("/s3worker/probes/c");
    }

    [Fact]
    public void Connectivity_КаноническийПорядокПолей()
    {
        // Arrange: те же данные в словарях, узлы/таргеты в ОБРАТНОМ порядке —
        // проверка Ordinal-сортировки сериализатора.
        var links = new Dictionary<string, IReadOnlyDictionary<string, string>>
        {
            ["east2"] = new Dictionary<string, string> { ["east1"] = "ok" },
            ["east1"] = new Dictionary<string, string> { ["east2"] = "offline" },
        };

        // Act
        var json = S3ProcessCommon.SerializeConnectivity("DEGRADED", 1760156900, links, "east3: таймаут");

        // Assert: строгая строка канона (порядок полей/узлов, probe_error последним).
        json.Should().Be(
            """{"state":"DEGRADED","checked_unix":1760156900,"links":{"east1":{"east2":"offline"},"east2":{"east1":"ok"}},"probe_error":"east3: таймаут"}""");
    }

    [Fact]
    public void Sync_КаноническийПорядокПолей()
    {
        // Arrange: пара с лагами (обратный порядок списка — сортировка (From,To))
        // и два SYNCED-варианта без опциональных полей.
        var pairs = new List<S3ProcessCommon.SyncPairValue>
        {
            new("east1", "east2", 3, 0, 1048576, 3),
        };

        // Act
        var lagging = S3ProcessCommon.SerializeSync("LAGGING", 1760156900, pairs, "листинг недоступен");
        var synced = S3ProcessCommon.SerializeSync("SYNCED", 1760156900,
            [new S3ProcessCommon.SyncPairValue("east1", "east2", 0, 0, null, 0)]);
        var noPairs = S3ProcessCommon.SerializeSync("SYNCED", 1760156900, []);

        // Assert: строгие строки канона; lag_objects=0 не пишется; last_error только непустой.
        lagging.Should().Be(
            """{"state":"LAGGING","checked_unix":1760156900,"pairs":[{"from":"east1","to":"east2","missing":3,"extra":0,"lag_bytes":1048576,"lag_objects":3}],"last_error":"листинг недоступен"}""");
        synced.Should().Be(
            """{"state":"SYNCED","checked_unix":1760156900,"pairs":[{"from":"east1","to":"east2","missing":0,"extra":0}]}""");
        noPairs.Should().Be(
            """{"state":"SYNCED","checked_unix":1760156900,"pairs":[]}""");
    }

    [Fact]
    public void ParseCheckedUnix_Гейт()
    {
        // Arrange: валидное значение, битый JSON, значение без поля, пустота.
        const string valid = """{"state":"OK","checked_unix":1760156900}""";
        const string broken = "{не-json";
        const string noField = """{"state":"OK"}""";

        // Act
        var parsed = S3ProcessCommon.ParseCheckedUnix(valid);
        var brokenParsed = S3ProcessCommon.ParseCheckedUnix(broken);
        var noFieldParsed = S3ProcessCommon.ParseCheckedUnix(noField);
        var nullParsed = S3ProcessCommon.ParseCheckedUnix(null);

        // Assert: только валидный случай даёт число (гейт «ключа нет» → прогон).
        parsed.Should().Be(1760156900);
        brokenParsed.Should().BeNull();
        noFieldParsed.Should().BeNull();
        nullParsed.Should().BeNull();
    }

    [Fact]
    public void ParseSyncDiscrepancies_Прогресс()
    {
        // Arrange: пары с missing/extra, пара без полей (пропускается), битый JSON.
        const string valid = """
            {"state":"LAGGING","checked_unix":1,"pairs":[{"from":"east1","to":"east2","missing":5,"extra":1},{"from":"a","to":"b"}]}
            """;
        const string broken = "{не-json";

        // Act
        var parsed = S3ProcessCommon.ParseSyncDiscrepancies(valid);
        var brokenParsed = S3ProcessCommon.ParseSyncDiscrepancies(broken);
        var nullParsed = S3ProcessCommon.ParseSyncDiscrepancies(null);

        // Assert: сумма missing+extra полной пары; неполная пропущена; битость → null.
        parsed.Should().NotBeNull();
        parsed![("east1", "east2")].Should().Be(6);
        parsed.Keys.Should().BeEquivalentTo([("east1", "east2")]);
        brokenParsed.Should().BeNull();
        nullParsed.Should().BeNull();
    }
}
