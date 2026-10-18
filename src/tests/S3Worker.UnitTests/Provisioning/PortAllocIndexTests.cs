using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using S3Worker.Provisioning.Processes;
using Xunit;

namespace S3Worker.UnitTests.Provisioning;

// Индекс занятости portalloc (arch/23 §8): union чужих записей (поле "s3");
// битый JSON соседа — игнор с логом, не ошибка тика.
public class PortAllocIndexTests
{
    private static S3Worker.Provisioning.Processes.PortAllocIndex NewIndex(Fakes.FakeEtcd etcd)
        => new(etcd, ["http://etcd:2379"], NullLogger<S3Worker.Provisioning.Processes.PortAllocIndex>.Instance);

    [Fact]
    public async Task TwoClusters_ForeignPortMerge()
    {
        // Arrange: два portalloc, свой — exceptCluster.
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/s3worker/portalloc/other1", """{"node1":{"host":"h1","s3":18001}}""");
        etcd.Seed("/s3worker/portalloc/other2", """{"node1":{"host":"h2","s3":18002},"node2":{"host":"h2","s3":18003}}""");
        etcd.Seed("/s3worker/portalloc/mine", """{"node1":{"host":"h1","s3":18999}}""");
        var index = NewIndex(etcd);

        // Act
        var result = await index.ForeignAllocatedPortsAsync("mine", TestContext.Current.CancellationToken);

        // Assert: union портов чужих (18001–18003), свой 18999 исключён.
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().BeEquivalentTo([18001, 18002, 18003]);
    }

    [Fact]
    public async Task NeighborBrokenJson_IgnoredWithLog()
    {
        // Arrange: валидный сосед + битый сосед.
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/s3worker/portalloc/good", """{"node1":{"host":"h1","s3":18001}}""");
        etcd.Seed("/s3worker/portalloc/bad", "{not json");
        var index = NewIndex(etcd);

        // Act
        var result = await index.ForeignAllocatedPortsAsync("mine", TestContext.Current.CancellationToken);

        // Assert: битый ключ пропущен, валидный учтён, без ошибки.
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().BeEquivalentTo([18001]);
    }
}
