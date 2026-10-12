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
}
