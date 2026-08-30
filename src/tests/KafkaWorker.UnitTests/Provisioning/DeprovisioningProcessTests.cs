using FluentAssertions;
using KafkaWorker.Core;
using KafkaWorker.Etcd.Coordination;
using KafkaWorker.Provisioning.Processes;

namespace KafkaWorker.UnitTests.Provisioning;

// DeprovisioningProcess X0–X3 (arch/16 §5 B): docker→etcd порядок, полная
// очистка координации (вкл. заявки ротации), идемпотентность, снапшоты «до/после».

public class DeprovisioningProcessTests
{
    private const string Ep = "http://etcd:2379";

    private sealed record Rig(
        Fakes.FakeEtcd Etcd,
        Fakes.FakeKafkaDriver Driver,
        ClaimStore Claims,
        DeprovisioningProcess Process,
        List<string> SnapshotPoints);

    private static async Task<Rig> NewRig(Action<Fakes.FakeEtcd, Fakes.FakeKafkaDriver>? setup = null)
    {
        var etcd = new Fakes.FakeEtcd();
        // Живой Active-кластер: config, брокеры, endpoints, креды, координация.
        etcd.Seed("/kafka/clusters/events/config",
            """{"brokers":2,"replication_factor":2,"min_insync_replicas":1,"default_partitions":12,"default_retention_ms":604800000,"created_unix":1756500000,"state":"TO_REMOVE"}""");
        etcd.Seed("/kafka/clusters/events/brokers/broker1/state", "RUNNING");
        etcd.Seed("/kafka/clusters/events/brokers/broker2/state", "RUNNING");
        etcd.Seed("/kafka/clusters/events/endpoints", "h1:16000,h1:16001");
        etcd.Seed("/kafka/clusters/events/app_user", "app");
        etcd.Seed("/kafka/clusters/events/app_password", "AbCdEf0123456789AbCdEf0123456789");
        etcd.Seed("/kafka/clusters/events/topics/orders", """{"partitions":3,"replication_factor":2,"synced_unix":1,"missing":false}""");
        etcd.Seed("/kafkaworker/portalloc/events", """{"broker1":{"host":"h1","client":16000},"broker2":{"host":"h1","client":16001}}""");
        etcd.Seed("/kafkaworker/rotations/events", """{"requested_unix":1756500100,"requested_by":"admin"}""");

        var claims = new ClaimStore([Ep], etcd, TimeProvider.System);
        await claims.TryClaimClusterAsync("events", CancellationToken.None);
        var journal = new WorkJournal(etcd, [Ep]);
        etcd.Seed("/kafkaworker/work/events", """{"op":"deprovision","phase":"started","instance":"x","updated_unix":1}""");

        var driver = new Fakes.FakeKafkaDriver
        {
            NodeObjects = ["kfw-events-broker1", "kfw-events-broker2"],
        };
        var snapshotPoints = new List<string>();
        var process = new DeprovisioningProcess(
            etcd, [Ep], driver, claims, journal,
            snapshot: ct =>
            {
                snapshotPoints.Add($"n{snapshotPoints.Count}");
                return Task.FromResult(Result.Success());
            });
        setup?.Invoke(etcd, driver);
        return new Rig(etcd, driver, claims, process, snapshotPoints);
    }

    [Fact]
    public async Task Run_RemovesContainersVolumesAndAllKeys()
    {
        // Arrange: TO_REMOVE-кластер с контейнерами и заявкой ротации.
        var rig = await NewRig();

        // Act: демонтаж.
        var result = await rig.Process.RunAsync("events", ["broker1", "broker2"], CancellationToken.None);

        // Assert: контейнеры+тома удалены (removeVolume=true), весь префикс
        // /kafka/clusters/events/ пуст, координация (claims/work/portalloc) и
        // заявка ротации удалены; клэйм снят явно.
        result.IsSuccess.Should().BeTrue();
        rig.Driver.Removed.Should().BeEquivalentTo(
            [("broker1", true), ("broker2", true)]);
        rig.Etcd.Store.Keys.Where(k => k.StartsWith("/kafka/clusters/events/", StringComparison.Ordinal))
            .Should().BeEmpty();
        rig.Etcd.Store.Should().NotContainKey("/kafkaworker/portalloc/events");
        rig.Etcd.Store.Should().NotContainKey("/kafkaworker/rotations/events");
        rig.Claims.IsMine("events").Should().BeFalse();
    }

    [Fact]
    public async Task Run_SnapshotDelegate_BeforeAndAfter()
    {
        // Arrange: полный демонтаж с snapshot-делегатом.
        var rig = await NewRig();

        // Act.
        var result = await rig.Process.RunAsync("events", ["broker1", "broker2"], CancellationToken.None);

        // Assert: делегат вызван «до» (старт) и «после» (финал).
        result.IsSuccess.Should().BeTrue();
        rig.SnapshotPoints.Should().HaveCount(2);
    }

    [Fact]
    public async Task Run_OrphanContainers_Removed()
    {
        // Arrange: docker видит сироту kfw-events-broker9 (ключа нет) и чужой кластер.
        var rig = await NewRig((etcd, driver) =>
        {
            driver.NodeObjects.Add("kfw-events-broker9");
            driver.NodeObjects.Add("kfw-other-broker1");
        });

        // Act.
        var result = await rig.Process.RunAsync("events", ["broker1", "broker2"], CancellationToken.None);

        // Assert: сирота удалена; чужой кластер не тронут; префикс пуст.
        result.IsSuccess.Should().BeTrue();
        rig.Driver.Removed.Should().Contain(("broker9", true));
        rig.Driver.NodeObjects.Should().Contain("kfw-other-broker1");
    }

    [Fact]
    public async Task Run_MissingContainers_404IsOk()
    {
        // Arrange: docker-объектов уже нет (сбое-хвост повторного тика).
        var rig = await NewRig((_, driver) => driver.NodeObjects.Clear());

        // Act.
        var result = await rig.Process.RunAsync("events", ["broker1", "broker2"], CancellationToken.None);

        // Assert: 404 от docker = ок — etcd всё равно вычищен.
        result.IsSuccess.Should().BeTrue();
        rig.Etcd.Store.Keys.Where(k => k.StartsWith("/kafka/clusters/events/", StringComparison.Ordinal))
            .Should().BeEmpty();
    }

    [Fact]
    public async Task Run_DockerFailure_EtcdUntouched()
    {
        // Arrange: docker-хост недоступен (первое удаление падает).
        var rig = await NewRig((_, driver) => driver.RemoveFailsOnce = true);

        // Act.
        var result = await rig.Process.RunAsync("events", ["broker1", "broker2"], CancellationToken.None);

        // Assert: Failed; порядок docker→etcd — префикс кластера ЖИВ (мёртвые
        // ключи при сбое безвредны, повторный тик продолжит).
        result.IsSuccess.Should().BeFalse();
        rig.Etcd.Store.Should().ContainKey("/kafka/clusters/events/config");
        rig.Etcd.Store.Should().ContainKey("/kafkaworker/rotations/events");
        rig.Claims.IsMine("events").Should().BeTrue();
    }

    [Fact]
    public async Task Run_AfterPartialFailure_RetryCompletes()
    {
        // Arrange: первый прогон падает на docker-удалении (сбое-хвост: etcd жив,
        // клэйм жив); повтор должен довести демонтаж до конца.
        var rig = await NewRig((_, driver) => driver.RemoveFailsOnce = true);
        (await rig.Process.RunAsync("events", ["broker1", "broker2"], CancellationToken.None))
            .IsSuccess.Should().BeFalse("первый прогон упал на docker");

        // Act: повторный тик.
        var second = await rig.Process.RunAsync("events", ["broker1", "broker2"], CancellationToken.None);

        // Assert: успех; префикс пуст, координация вычищена, клэйм снят.
        second.IsSuccess.Should().BeTrue();
        rig.Etcd.Store.Keys.Where(k => k.StartsWith("/kafka/clusters/events/", StringComparison.Ordinal))
            .Should().BeEmpty();
        rig.Etcd.Store.Should().NotContainKey("/kafkaworker/rotations/events");
        rig.Claims.IsMine("events").Should().BeFalse();
    }

    [Fact]
    public async Task Run_NotClaimed_Refuses()
    {
        // Arrange: клэйм не захвачен.
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/kafka/clusters/events/config", """{"brokers":1,"state":"TO_REMOVE"}""");
        var claims = new ClaimStore([Ep], etcd, TimeProvider.System);
        var process = new DeprovisioningProcess(
            etcd, [Ep], new Fakes.FakeKafkaDriver(), claims, new WorkJournal(etcd, [Ep]));

        // Act.
        var result = await process.RunAsync("events", ["broker1"], CancellationToken.None);

        // Assert: отказ до любых мутаций; ключи живы.
        result.IsSuccess.Should().BeFalse();
        result.Error!.Message.Should().Contain("клэйм не наш");
        etcd.Store.Should().ContainKey("/kafka/clusters/events/config");
    }
}
