using FluentAssertions;
using S3Worker.Provisioning.Processes;
using Xunit;

namespace S3Worker.UnitTests.Provisioning;

// Лестница E9 (arch/17): реконструкция portalloc из inspect живого контейнера
// (put-if-absent под locks/portalloc, проигрыш → re-read); контейнера нет →
// Failed «новая аллокация» (S7); inspect недоступен → Failed (вслепую нет).
public class PortAllocHealerTests
{
    private static (PortAllocHealer Healer, Fakes.FakeEtcd Etcd, Fakes.FakeS3Driver Driver, ClaimStore Claims) NewRig()
    {
        var etcd = new Fakes.FakeEtcd();
        var driver = new Fakes.FakeS3Driver();
        var claims = new ClaimStore("/s3worker", ["http://etcd:2379"], etcd, TimeProvider.System);
        var healer = new PortAllocHealer(
            etcd, ["http://etcd:2379"], driver, claims,
            new WorkJournal("/s3worker", etcd, ["http://etcd:2379"]),
            new PortAllocLock("/s3worker", ["http://etcd:2379"], etcd, TimeProvider.System, "inst-1"),
            new PortAllocIndex(
                etcd, ["http://etcd:2379"], Microsoft.Extensions.Logging.Abstractions.NullLogger<PortAllocIndex>.Instance),
            new S3ProvisioningOptions(18000, 18999, 100, 90, 15, "host.docker.internal", "owns3:dev", "pgworker-backups"));
        return (healer, etcd, driver, claims);
    }

    [Fact]
    public async Task НодаБезPortalloc_ЖивойКонтейнер_РеконструкцияPutIfAbsent()
    {
        // Arrange: portalloc-ключа нет; контейнер жив на published-порту.
        var (healer, etcd, driver, _) = NewRig();
        driver.Nodes["demo/node1"] = new Fakes.FakeS3Driver.NodeFact("h1", 18042);

        // Act
        var result = await healer.HealNodeAsync("demo", "node1", TestContext.Current.CancellationToken);

        // Assert: адрес из inspect; portalloc записан; journal reconstructed.
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be(new S3Worker.Core.Model.NodeAddress("h1", 18042));
        etcd.Store["/s3worker/portalloc/demo"].Value.Should().Contain("18042");
        // txns: захват locks/portalloc (lease), put-if-absent portalloc,
        // journal-фаза (WorkJournal) — portalloc-put второй по порядку.
        etcd.Txns.Should().HaveCount(3);
        etcd.Txns[1].Compare.Should().ContainSingle()
            .Which.Key.Should().Be("/s3worker/portalloc/demo");
    }

    [Fact]
    public async Task ЗаписьУжеЕсть_ВеткаБезТранзакций()
    {
        // Arrange: portalloc-запись есть — advertise стабилен.
        var (healer, etcd, driver, _) = NewRig();
        etcd.Seed("/s3worker/portalloc/demo", """{"node1":{"host":"h2","s3":18100}}""");

        // Act
        var result = await healer.HealNodeAsync("demo", "node1", TestContext.Current.CancellationToken);

        // Assert: чужой (закреплённый) адрес возвращён без docker и без txn.
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be(new S3Worker.Core.Model.NodeAddress("h2", 18100));
        etcd.Txns.Should().BeEmpty();
    }

    [Fact]
    public async Task КонтейнераНет_FailedПортНеВыдумываем()
    {
        // Arrange: ни portalloc, ни контейнера (S7-свидетельство) —
        // новая аллокация — дело вызывающего (P1), не healer'а.
        var (healer, etcd, _, _) = NewRig();

        // Act
        var result = await healer.HealNodeAsync("demo", "node1", TestContext.Current.CancellationToken);

        // Assert: Failed; ключ portalloc не появился.
        result.IsSuccess.Should().BeFalse();
        etcd.Store.Should().NotContainKey("/s3worker/portalloc/demo");
    }

    [Fact]
    public async Task InspectНедоступен_FailedБезРеконструкции()
    {
        // Arrange: docker-хост молчит (слепой inspect).
        var (healer, _, driver, _) = NewRig();
        driver.InspectFault = true;

        // Act
        var result = await healer.HealNodeAsync("demo", "node1", TestContext.Current.CancellationToken);

        // Assert: Failed, portalloc не записан (вслепую нет).
        result.IsSuccess.Should().BeFalse();
    }
}
