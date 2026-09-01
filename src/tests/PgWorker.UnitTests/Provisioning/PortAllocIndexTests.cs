using Microsoft.Extensions.Logging.Abstractions;
using PgWorker.Etcd.Client;
using PgWorker.Provisioning.Endpoints;

namespace PgWorker.UnitTests.Provisioning;

// PortAllocIndex (spec §3.3): busy-множество из portalloc-записей ВСЕХ кластеров,
// кроме своего; битые JSON соседей скипаются без ронирования результата.

public class PortAllocIndexTests
{
    private const string Ep = "http://etcd:2379";

    [Fact]
    public async Task ReadBusy_MixesAllNeighborsExceptOwn()
    {
        // Arrange: portalloc двух кластеров; свой (shop) исключается.
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/pgworker/portalloc/shop",
            """{"shard1/shard1a":{"host":"h1","pg":15000,"patroni":18000,"doorman":16500}}""");
        etcd.Seed("/pgworker/portalloc/canon10",
            """
            {"shard1/shard1a":{"host":"h1","pg":15004,"patroni":18004,"doorman":16504},
            "shard1/shard1b":{"host":"h2","pg":15005,"patroni":18005,"doorman":16505}}
            """);
        var index = new PortAllocIndex(etcd, [Ep], NullLogger<PortAllocIndex>.Instance);

        // Act
        var busy = await index.ReadBusyAsync("shop", CancellationToken.None);

        // Assert: только чужая тройка×2 ноды; своих портов нет.
        busy.IsSuccess.Should().BeTrue();
        busy.Value.Should().BeEquivalentTo(
            new (string, int)[] { ("h1", 15004), ("h1", 18004), ("h1", 16504),
                                  ("h2", 15005), ("h2", 18005), ("h2", 16505) });
    }

    [Fact]
    public async Task ReadBusy_MalformedNeighborKey_SkippedNotFailed()
    {
        // Arrange: сосед с битым JSON + валидный сосед.
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/pgworker/portalloc/broken", "{не-json}");
        etcd.Seed("/pgworker/portalloc/good",
            """{"s1/n1":{"host":"h1","pg":15010,"patroni":18010,"doorman":16510}}""");
        var index = new PortAllocIndex(etcd, [Ep], NullLogger<PortAllocIndex>.Instance);

        // Act
        var busy = await index.ReadBusyAsync("shop", CancellationToken.None);

        // Assert: валидный ключ учтён, битый — молча пропущен (лог), Result успешен.
        busy.IsSuccess.Should().BeTrue();
        busy.Value.Should().BeEquivalentTo(new (string, int)[] { ("h1", 15010), ("h1", 18010), ("h1", 16510) });
    }

    [Fact]
    public async Task ReadBusy_ObjectNodeZeroDoorman_NotAdded()
    {
        // Arrange: усыновлённая нода с doorman=0 (внешний контейнер без биндинга).
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/pgworker/portalloc/ext",
            """{"s1/n1":{"host":"h1","pg":15020,"patroni":0,"doorman":0,"object":"foreign"}}""");
        var index = new PortAllocIndex(etcd, [Ep], NullLogger<PortAllocIndex>.Instance);

        // Act
        var busy = await index.ReadBusyAsync("shop", CancellationToken.None);

        // Assert: нулевые порты в занятость не попадают.
        busy.Value.Should().BeEquivalentTo(new (string, int)[] { ("h1", 15020) });
    }
}
