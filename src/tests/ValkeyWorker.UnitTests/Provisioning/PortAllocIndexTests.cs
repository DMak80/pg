using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using ValkeyWorker.UnitTests.Provisioning;
using Xunit;

namespace ValkeyWorker.UnitTests.Provisioning;

// Индекс занятости portalloc (arch/21 §2): union чужих записей; битый JSON
// соседа — игнор с логом, не ошибка тика.
public class PortAllocIndexTests
{
    private static ValkeyWorker.Provisioning.Processes.PortAllocIndex NewIndex(Fakes.FakeEtcd etcd)
        => new(etcd, ["http://etcd:2379"], NullLogger<ValkeyWorker.Provisioning.Processes.PortAllocIndex>.Instance);

    [Fact]
    public async Task ДваКластера_ОбъединениеПортовЧужих()
    {
        // Arrange: два portalloc, свой — exceptCluster.
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/valkeyworker/portalloc/other1", """{"node1":{"host":"h1","client":17001}}""");
        etcd.Seed("/valkeyworker/portalloc/other2", """{"node1":{"host":"h2","client":17002},"node2":{"host":"h2","client":17003}}""");
        etcd.Seed("/valkeyworker/portalloc/mine", """{"node1":{"host":"h1","client":17999}}""");
        var index = NewIndex(etcd);

        // Act
        var result = await index.ForeignAllocatedPortsAsync("mine", TestContext.Current.CancellationToken);

        // Assert: union портов чужих (17001–17003), свой 17999 исключён.
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().BeEquivalentTo([17001, 17002, 17003]);
    }

    [Fact]
    public async Task БитыйJsonСоседа_ИгнорСЛогом()
    {
        // Arrange: валидный сосед + битый сосед.
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/valkeyworker/portalloc/good", """{"node1":{"host":"h1","client":17001}}""");
        etcd.Seed("/valkeyworker/portalloc/bad", "{not json");
        var index = NewIndex(etcd);

        // Act
        var result = await index.ForeignAllocatedPortsAsync("mine", TestContext.Current.CancellationToken);

        // Assert: битый ключ пропущен, валидный учтён, без ошибки.
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().BeEquivalentTo([17001]);
    }
}
