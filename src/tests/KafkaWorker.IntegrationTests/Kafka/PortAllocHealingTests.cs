using FluentAssertions;
using KafkaWorker.Provisioning.Processes;

namespace KafkaWorker.IntegrationTests.Kafka;

// Лестница E9 интеграционно (t05, spec §7.6): утеря portalloc живого
// кластера → реконструкция из inspect (ветка 2, без пересозданий); утеря
// portalloc + снос контейнеров → новая аллокация + RMW endpoints + подъём
// (ветка 3). Порты — окно фикстуры (FreePortWindow, литералов нет).
[Collection(KafkaCollection.Name)]
public class PortAllocHealingTests(KafkaClusterFixture fixture)
{
    [Fact]
    public async Task LostPortAlloc_Heals_ClusterStaysAlive()
    {
        var cluster = fixture.Cluster("heal1");
        var ct = TestContext.Current.CancellationToken;

        // Arrange: 1-брокерный кластер поднимается provisioning'ом (паттерн
        // ProvisioningTests: тики до Config.State == null), portalloc/endpoints
        // записаны воркером.
        await fixture.SeedClusterAsync(cluster, brokers: 1);
        var provision = await fixture.ProvisionRigAsync(cluster);
        var deadline = DateTimeOffset.UtcNow.AddSeconds(200);
        while (DateTimeOffset.UtcNow < deadline)
        {
            var snap = await fixture.SnapshotAsync(cluster);
            if (snap!.Config.State is null)
                break; // done: кластер поднят, ключи факта на месте

            var tick = await provision.RunAsync(snap, ct);
            tick.IsSuccess.Should().BeTrue(
                $"тик provisioning не должен падать (waiting-brokers — успех): {tick.Error?.Message}");
            await Task.Delay(3000, ct);
        }

        var supervisor = await fixture.SupervisorRigAsync(cluster);
        var endpointsBefore = await fixture.GetAsync($"/kafka/clusters/{cluster}/endpoints");
        endpointsBefore.Should().NotBeNullOrEmpty("кластер поднят provisioning'ом");

        // Act 1 (ветка 2): утеря журнала при живых контейнерах.
        await fixture.DelAsync($"/kafkaworker/portalloc/{cluster}");
        var snap1 = await fixture.SnapshotAsync(cluster);
        var tick1 = await supervisor.RunAsync(snap1!, ct);
        tick1.IsSuccess.Should().BeTrue($"ветка 2 (реконструкция) не должна падать: {tick1.Error?.Message}");

        // Assert 1: portalloc восстановлен, порты прежние, endpoints не менялся,
        // контейнер остался тем же объектом и отвечает на exec (пересоздания нет).
        var portAlloc = await fixture.GetAsync($"/kafkaworker/portalloc/{cluster}");
        portAlloc.Should().NotBeNull("реконструкция из inspect (ветка 2)");
        portAlloc.Should().ContainAny(endpointsBefore!.Split(',').Select(p => p[(p.LastIndexOf(':') + 1)..]).ToArray(),
            "прежние порты сохранены (advertise стабилен)");
        (await fixture.GetAsync($"/kafka/clusters/{cluster}/endpoints")).Should().Be(endpointsBefore);
        var objectsAfterBranch2 = await fixture.Driver.ListNodeObjectsAsync(cluster, ct);
        objectsAfterBranch2.Value.Should().Contain($"kfw-{cluster}-broker1");
        (await fixture.Driver.ExecNodeAsync(cluster, "broker1", ["true"], ct))
            .IsSuccess.Should().BeTrue("контейнер жив (не пересоздавался)");
        var admin2 = fixture.AdminFactory.Create(endpointsBefore, "app",
            (await fixture.GetAsync($"/kafka/clusters/{cluster}/app_password"))!,
            (await fixture.GetAsync($"/kafka/clusters/{cluster}/ca_pem")));
        (await admin2.DescribeClusterAsync(ct)).IsSuccess.Should().BeTrue("кластер жив");

        // Act 2 (ветка 3): снос контейнеров + повторная утеря журнала.
        await fixture.DelAsync($"/kafkaworker/portalloc/{cluster}");
        await fixture.RemoveBrokerContainersAsync(cluster);
        var snap2 = await fixture.SnapshotAsync(cluster);
        var tick2 = await supervisor.RunAsync(snap2!, ct);
        tick2.IsSuccess.Should().BeTrue($"ветка 3 (реаллокация) не должна падать: {tick2.Error?.Message}");

        // Assert 2: ветка 3 исполнилась (Recreated=true → supervisor пишет
        // PROVISIONING), portalloc перезаписан, контейнер поднят. Порты МОГУТ
        // совпасть с прежними — старый порт свободен, аллокатор берёт первый
        // свободный (S7-реаллокация не обязана менять адрес).
        (await fixture.GetAsync($"/kafka/clusters/{cluster}/brokers/broker1/state"))
            .Should().Be("PROVISIONING", "ветка 3 пересоздала контейнер (Recreated=true)");
        var portAlloc2 = await fixture.GetAsync($"/kafkaworker/portalloc/{cluster}");
        portAlloc2.Should().NotBeNull("portalloc перезаписан аллокацией");
        var endpointsAfter = await fixture.GetAsync($"/kafka/clusters/{cluster}/endpoints");
        endpointsAfter.Should().NotBeNullOrEmpty("endpoints RMW-обновлён (клиенты перечитают тиком)");

        // Готовность — по ДВУМ фактам (ревью F9): DescribeCluster отвечает И
        // воркер довёл state до RUNNING (поллинг тиков надзора + пробы,
        // потолок 120 с — пол гейта ≤ 100 c + запас).
        var ready = false;
        var pollDeadline = DateTimeOffset.UtcNow.AddSeconds(120);
        while (DateTimeOffset.UtcNow < pollDeadline)
        {
            var snap = await fixture.SnapshotAsync(cluster);
            var tick = await supervisor.RunAsync(snap!, ct);
            tick.IsSuccess.Should().BeTrue($"тик надзора не должен падать: {tick.Error?.Message}");

            var view = await fixture.AdminFactory.Create(endpointsAfter!, "app",
                (await fixture.GetAsync($"/kafka/clusters/{cluster}/app_password"))!,
                (await fixture.GetAsync($"/kafka/clusters/{cluster}/ca_pem"))).DescribeClusterAsync(ct);
            var state = await fixture.GetAsync($"/kafka/clusters/{cluster}/brokers/broker1/state");
            if (view.IsSuccess && state == "RUNNING")
            {
                ready = true;
                break;
            }

            await Task.Delay(3000, ct);
        }

        ready.Should().BeTrue("кластер поднялся воркером после утери portalloc: DescribeCluster жив + state=RUNNING (ветка 3)");
    }
}
