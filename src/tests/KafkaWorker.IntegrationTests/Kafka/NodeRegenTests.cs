using Confluent.Kafka.Admin;
using FluentAssertions;
using KafkaWorker.Core.Planning;
using KafkaWorker.Etcd.Coordination;
using KafkaWorker.Provisioning;
using KafkaWorker.Provisioning.Processes;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace KafkaWorker.IntegrationTests.Kafka;

// Полный цикл регенерации (t06, spec §7): 1-брокерный кластер (рестарт
// единственного брокера — бюджет бут-времени фикстуры), сходимость лимитов,
// сохранность данных, отсутствие лишних рестартов. Кластер доводится до
// Active ЦИКЛОМ Provision-тиков (порт UpAsync ReassignmentTests) — один тик
// не поднимает кластер, а дискавери-креды требуют endpoints из K5
// (ревью Фазы 4, замечания 1–2).
[Collection(KafkaCollection.Name)]
public class NodeRegenTests(KafkaClusterFixture fixture)
{
    private sealed record Rig(
        ClaimStore Claims, WorkJournal Journal,
        ProvisioningProcess Provision,
        AddBrokerProcess Add, NodeRegenerator Regen);

    private Rig BuildRig()
    {
        // Порт NewRigAsync ReassignmentTests (реальные зависимости, без null!):
        var claims = new ClaimStore([fixture.Endpoint], fixture.Gateway, TimeProvider.System);
        var journal = new WorkJournal(fixture.Gateway, [fixture.Endpoint]);
        return new Rig(
            claims, journal,
            new ProvisioningProcess(
                fixture.Gateway, [fixture.Endpoint], fixture.Driver, claims, journal,
                new PortAllocLock([fixture.Endpoint], fixture.Gateway, TimeProvider.System, claims.InstanceId),
                new PortAllocIndex(fixture.Gateway, [fixture.Endpoint], NullLogger<PortAllocIndex>.Instance),
                new ClusterSecretEnsurer(fixture.Gateway, [fixture.Endpoint]),
                fixture.AdminFactory, new ClusterConfigConverger(fixture.AdminFactory),
                fixture.Options, snapshot: null),
            new AddBrokerProcess(
                fixture.Gateway, [fixture.Endpoint], fixture.Driver, claims, journal,
                new PortAllocLock([fixture.Endpoint], fixture.Gateway, TimeProvider.System, claims.InstanceId),
                new PortAllocIndex(fixture.Gateway, [fixture.Endpoint], NullLogger<PortAllocIndex>.Instance),
                fixture.AdminFactory, fixture.Options),
            new NodeRegenerator(
                fixture.Gateway, [fixture.Endpoint], fixture.Driver, claims, journal, fixture.Options));
    }

    // Порт UpAsync (ReassignmentTests): цикл Provision-тиков до Active
    // (config без state) — K4 транзиентно waiting-brokers = успех.
    private async Task UpAsync(Rig rig, string cluster, int budgetSec)
    {
        var ct = TestContext.Current.CancellationToken;
        var deadline = DateTimeOffset.UtcNow.AddSeconds(budgetSec);
        while (DateTimeOffset.UtcNow < deadline)
        {
            var snap = await fixture.SnapshotAsync(cluster);
            if (snap!.Config.State is null)
                break;

            var tick = await rig.Provision.RunAsync(snap, ct);
            tick.IsSuccess.Should().BeTrue(
                $"тик provisioning не должен падать (waiting-brokers — успех): {tick.Error?.Message}");
            await Task.Delay(3000, ct);
        }

        (await fixture.SnapshotAsync(cluster))!.Config.State.Should().BeNull(
            $"кластер {cluster} не поднялся за {budgetSec} с");
    }

    // Доведение до RUNNING (после UpAsync брокеры уже RUNNING по K4, но
    // цикл гарантирует; Add идемпотентен на RUNNING — no-op).
    private async Task BringToRunningAsync(Rig rig, string cluster, int budgetSec)
    {
        var ct = TestContext.Current.CancellationToken;
        var deadline = DateTimeOffset.UtcNow.AddSeconds(budgetSec);
        while (DateTimeOffset.UtcNow < deadline)
        {
            var snap = await fixture.SnapshotAsync(cluster);
            if (snap!.Brokers.All(b => b.State == "RUNNING"))
                return;

            var tick = await rig.Add.RunAsync(snap, ct);
            tick.IsSuccess.Should().BeTrue($"тик add не должен падать: {tick.Error?.Message}");
            await Task.Delay(3000, ct);
        }

        Assert.Fail("брокеры не достигли RUNNING в бюджет");
    }

    [Fact]
    public async Task PutResources_LimitsDiverge_RollingRegenConvergesWithSameVolume()
    {
        // Arrange — 1-брокерный кластер: ЦИКЛ Provision до Active (endpoints
        // появятся в K5 — только тогда доступен дискавери-клиент), затем
        // топик «keep» (том должен пережить регенерацию)
        var cluster = fixture.Cluster("rg1");
        await fixture.SeedClusterAsync(cluster, 1);
        var rig = BuildRig();
        var ct = TestContext.Current.CancellationToken;
        await rig.Claims.TryClaimClusterAsync(cluster, ct);
        await UpAsync(rig, cluster, budgetSec: 120);
        await BringToRunningAsync(rig, cluster, budgetSec: 60);

        var topicBuilder = await fixture.DiscoveryAdminBuilderAsync(cluster);
        using (var admin = topicBuilder.Build())
            await admin.CreateTopicsAsync([new TopicSpecification { Name = "keep", NumPartitions = 1 }]);

        // Act — декларация меняется (cpu 1→2, mem 1Gi→2Gi): как мутация №15
        await fixture.Gateway.PutAsync(fixture.Endpoint,
            $"/kafka/clusters/{cluster}/brokers/broker1/resources",
            """{"cpu":"2","mem":"2Gi","disk":"10Gi"}""", null, ct);

        // Assert — сходимость: RUNNING + новые лимиты + прогресс-ключ исчез.
        // Пересоздание доказывается сменой лимитов (docker меняет лимиты
        // только пересозданием) и наблюдаемым PROVISIONING-циклом.
        var inspected = await fixture.Driver.NodeResourcesAsync(cluster, "broker1", ct);
        var deadline = DateTimeOffset.UtcNow.AddSeconds(180);
        while (DateTimeOffset.UtcNow < deadline)
        {
            var snap = await fixture.SnapshotAsync(cluster);
            await rig.Regen.RunAsync(snap!, ct);
            if (snap!.Brokers.Single(b => b.Name == "broker1").State != "RUNNING")
            {
                await rig.Add.RunAsync(snap, ct); // доводка PROVISIONING → RUNNING (F)
                continue;
            }

            inspected = await fixture.Driver.NodeResourcesAsync(cluster, "broker1", ct);
            if (inspected.IsSuccess
                && inspected.Value == new NodeLimits(2_000_000_000L, 2L << 30)
                && await fixture.GetAsync($"/kafkaworker/regens/{cluster}") is null)
                break;
            await Task.Delay(2000, ct);
        }

        inspected.Value.Should().Be(new NodeLimits(2_000_000_000L, 2L << 30),
            "лимиты контейнера обязаны сойтись к декларации");
        (await fixture.GetAsync($"/kafkaworker/regens/{cluster}")).Should().BeNull(
            "прогресс-ключ удаляется по сходимости");

        // Том пережил пересоздание: топик жив в метаданных кластера
        // (производственный produce/consume-цикл — вне объёма; spec §7).
        var metaBuilder = await fixture.DiscoveryAdminBuilderAsync(cluster);
        using (var admin = metaBuilder.Build())
        {
            var metadata = admin.GetMetadata(TimeSpan.FromSeconds(10));
            metadata.Topics.Should().Contain(t => t.Topic == "keep");
        }
    }

    [Fact]
    public async Task PutSameResources_NoRecreate()
    {
        // Arrange — сошедшийся кластер: ЦИКЛ Provision до Active, затем
        // доводка до RUNNING (ревью Фазы 4, замечание 2: в цикле обязаны
        // тикать процессы — одного чтения снапшота недостаточно)
        var cluster = fixture.Cluster("rg2");
        await fixture.SeedClusterAsync(cluster, 1);
        var rig = BuildRig();
        var ct = TestContext.Current.CancellationToken;
        await rig.Claims.TryClaimClusterAsync(cluster, ct);
        await UpAsync(rig, cluster, budgetSec: 120);
        await BringToRunningAsync(rig, cluster, budgetSec: 60);

        // Act — серия Regen-тиков при сошедшихся ресурсах (JSON сида не менялся)
        for (var i = 0; i < 3; i++)
        {
            var tick = await rig.Regen.RunAsync((await fixture.SnapshotAsync(cluster))!, ct);
            tick.IsSuccess.Should().BeTrue();
            await Task.Delay(1000, ct);
        }

        // Assert — рестарта нет: прогресс-ключ не ставился, брокер остался
        // RUNNING (любое пересоздание обязано поставить PROVISIONING и
        // живой regens-ключ — наблюдаемо; spec §10.3 «совпадающие — без
        // рестарта», доказательство без container-Id — см. шапку задачи)
        (await fixture.GetAsync($"/kafkaworker/regens/{cluster}")).Should().BeNull();
        (await fixture.SnapshotAsync(cluster))!.Brokers.Single(b => b.Name == "broker1")
            .State.Should().Be("RUNNING");
    }
}
