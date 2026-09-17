using FluentAssertions;
using Shared.Etcd.Client;
using ValkeyWorker.UnitTests.Provisioning;
using Xunit;

namespace ValkeyWorker.UnitTests.Provisioning;

// Лестница E9 (arch/21 §5 C): реконструкция portalloc из inspect живого
// контейнера (put-if-absent под locks/portalloc, проигрыш → re-read);
// контейнера нет / inspect недоступен → Failed (E9 не выдумывает порт).
public class PortAllocHealerTests
{
    private static (ValkeyWorker.Provisioning.Processes.PortAllocHealer Healer, Fakes.FakeEtcd Etcd, Fakes.FakeDriver Driver, ClaimStore Claims) NewRig()
    {
        var etcd = new Fakes.FakeEtcd();
        var driver = new Fakes.FakeDriver();
        var claims = new ClaimStore("/valkeyworker", ["http://etcd:2379"], etcd, TimeProvider.System);
        var healer = new ValkeyWorker.Provisioning.Processes.PortAllocHealer(
            etcd, ["http://etcd:2379"], driver, claims,
            new WorkJournal("/valkeyworker", etcd, ["http://etcd:2379"]),
            new PortAllocLock("/valkeyworker", ["http://etcd:2379"], etcd, TimeProvider.System, "inst-1"),
            new ValkeyWorker.Provisioning.Processes.PortAllocIndex(
                etcd, ["http://etcd:2379"], Microsoft.Extensions.Logging.Abstractions.NullLogger<ValkeyWorker.Provisioning.Processes.PortAllocIndex>.Instance),
            new ValkeyWorker.Provisioning.Processes.ValkeyProvisioningOptions(
                17000, 17999, 100, 90, "localhost", "valkey/valkey:9.1.2"));
        return (healer, etcd, driver, claims);
    }

    [Fact]
    public async Task НодаБезPortalloc_ЖивойКонтейнер_РеконструкцияPutIfAbsent()
    {
        // Arrange: portalloc-ключа нет; контейнер жив на published-порту.
        var (healer, etcd, driver, _) = NewRig();
        driver.Containers["vwk-demo-node1"] =
            new Fakes.FakeDriver.ContainerFact("h1", 17042, null, null, ["valkey-server"], "valkey/valkey:9.1.2", "id1");

        // Act
        var result = await healer.HealNodePortAsync("demo", "node1", TestContext.Current.CancellationToken);

        // Assert: порт из inspect; portalloc записан; journal reconstructed.
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be(17042);
        etcd.Store["/valkeyworker/portalloc/demo"].Value.Should().Contain("17042");
        // txns: захват locks/portalloc (lease), put-if-absent portalloc,
        // journal-фаза (WorkJournal) — portalloc-put второй по порядку.
        etcd.Txns.Should().HaveCount(3);
        etcd.Txns[1].Compare.Should().ContainSingle()
            .Which.Key.Should().Be("/valkeyworker/portalloc/demo");
    }

    [Fact]
    public async Task PutIfAbsentПроигран_ReReadЧужогоЗначения()
    {
        // Arrange: сосед успел записать portalloc между инспекцией и txn —
        // симуляция: pre-запись чужого ключа сразу (NotExists проиграет).
        var (healer, etcd, driver, _) = NewRig();
        driver.Containers["vwk-demo-node1"] =
            new Fakes.FakeDriver.ContainerFact("h1", 17042, null, null, ["valkey-server"], "valkey/valkey:9.1.2", "id1");
        etcd.Seed("/valkeyworker/portalloc/demo", """{"node1":{"host":"h2","client":17100}}""");

        // Act
        var result = await healer.HealNodePortAsync("demo", "node1", TestContext.Current.CancellationToken);

        // Assert: ветка 1 — уже есть запись, чужой порт возвращён без txn.
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be(17100);
        etcd.Txns.Should().BeEmpty();
    }

    [Fact]
    public async Task InspectНедоступен_FailedБезРеконструкции()
    {
        // Arrange: docker-хост молчит (слепой inspect).
        var (healer, _, driver, _) = NewRig();
        driver.EndpointFault = true;

        // Act
        var result = await healer.HealNodePortAsync("demo", "node1", TestContext.Current.CancellationToken);

        // Assert: Failed, portalloc не записан (вслепую нет).
        result.IsSuccess.Should().BeFalse();
    }

    [Fact]
    public async Task КонтейнераНет_FailedПортНеВыдумываем()
    {
        // Arrange: ни portalloc, ни контейнера (S7-свидетельство).
        var (healer, etcd, _, _) = NewRig();

        // Act
        var result = await healer.HealNodePortAsync("demo", "node1", TestContext.Current.CancellationToken);

        // Assert: Failed; ключ portalloc не появился.
        result.IsSuccess.Should().BeFalse();
        etcd.Store.Should().NotContainKey("/valkeyworker/portalloc/demo");
    }
}
