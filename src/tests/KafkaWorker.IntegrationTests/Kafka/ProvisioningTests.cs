using System.Globalization;
using Confluent.Kafka.Admin;
using FluentAssertions;
using KafkaWorker.Core;
using KafkaWorker.Etcd.Coordination;
using KafkaWorker.Provisioning.Processes;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace KafkaWorker.IntegrationTests.Kafka;

// E2E волны A (арх-план A14; Docker required): заявка в etcd → provisioning
// 1-брокерного кластера → ключи факта → дискавери-подключение ТОЛЬКО по ключам
// etcd (endpoints + app_*) → заявка ротации + TO_REMOVE → полный демонтаж с
// очисткой координации (вкл. rotations). Готовность брокера ≤ 120 с.
[Collection(KafkaCollection.Name)]
public class ProvisioningTests(KafkaClusterFixture fixture)
{
    [Fact]
    public async Task FullLifecycle_ProvisionDiscoveryDeprovision()
    {
        var cluster = fixture.Cluster("it1");
        var ct = TestContext.Current.CancellationToken;

        // Arrange: заявка 1-брокерного кластера (config NOT_INITIALIZED + broker1).
        await fixture.SeedClusterAsync(cluster, brokers: 1);
        var claims = new ClaimStore([fixture.Endpoint], fixture.Gateway, TimeProvider.System);
        await claims.TryClaimClusterAsync(cluster, ct);
        var journal = new WorkJournal(fixture.Gateway, [fixture.Endpoint]);
        var provision = new ProvisioningProcess(
            fixture.Gateway, [fixture.Endpoint], fixture.Driver, claims, journal,
            new PortAllocLock([fixture.Endpoint], fixture.Gateway, TimeProvider.System, claims.InstanceId),
            new PortAllocIndex(fixture.Gateway, [fixture.Endpoint], NullLogger<PortAllocIndex>.Instance),
            new AppSecretEnsurer(fixture.Gateway, [fixture.Endpoint]),
            fixture.AdminFactory, new ClusterConfigConverger(fixture.AdminFactory),
            fixture.Options, snapshot: null);
        var deprovision = new DeprovisioningProcess(
            fixture.Gateway, [fixture.Endpoint], fixture.Driver, claims, journal, snapshot: null);

        // Act 1: provisioning до готовности (поллинг-ретраи; потолок 200 с —
        // с запасом над воркерным BrokerBootSec=100, зелёный прогон не замедляет).
        var deadline = DateTimeOffset.UtcNow.AddSeconds(200);
        while (DateTimeOffset.UtcNow < deadline)
        {
            var snap = await fixture.SnapshotAsync(cluster);
            if (snap!.Config.State is null)
                break; // done: config без state

            var tick = await provision.RunAsync(snap, ct);
            tick.IsSuccess.Should().BeTrue(
                $"тик provisioning не должен падать (waiting-brokers — успех): {tick.Error?.Message}");
            await Task.Delay(3000, ct);
        }

        // Assert 1: контейнер Running, ключи факта на месте.
        var config = await fixture.GetAsync($"/kafka/clusters/{cluster}/config");
        config.Should().NotContain("state", "provisioning снимает state у config");
        var endpoints = await fixture.GetAsync($"/kafka/clusters/{cluster}/endpoints");
        // Порты — из динамического окна фикстуры (литералов «:16000» больше нет:
        // выдача сквозная по кластерам коллекции, it1 не обязательно первый).
        // Проверяем принадлежность фактическому окну [PortFrom, PortTo].
        endpoints.Should().StartWith("localhost:");
        var clientPorts = endpoints!.Split(',')
            .Select(e => int.Parse(e[(e.LastIndexOf(':') + 1)..], CultureInfo.InvariantCulture));
        clientPorts.Should().OnlyContain(p => p >= fixture.PortFrom && p <= fixture.PortTo);
        var password = await fixture.GetAsync($"/kafka/clusters/{cluster}/app_password");
        password.Should().HaveLength(32).And.MatchRegex("^[A-Za-z0-9]{32}$");
        (await fixture.GetAsync($"/kafka/clusters/{cluster}/brokers/broker1/state")).Should().Be("RUNNING");
        (await fixture.GetAsync($"/kafka/clusters/{cluster}/brokers/broker1/role")).Should().Be("controller");
        var objects = await fixture.Driver.ListNodeObjectsAsync(cluster, ct);
        objects.Value.Should().Contain($"kfw-{cluster}-broker1");

        // Assert 2 (дискавери, spec §9.2): AdminClient с bootstrap из endpoints-ключа
        // и SASL из app_*-ключей успешно DescribeCluster.
        var builder = await fixture.DiscoveryAdminBuilderAsync(cluster);
        using (var admin = builder.Build())
        {
            var metadata = admin.GetMetadata(TimeSpan.FromSeconds(15));
            metadata.Brokers.Should().HaveCount(1, "1-брокерный кластер заявлен");
        }

        // Act 2: заявка ротации (остаточная — её должна подчистить очистка X2)
        // + перевод в TO_REMOVE + демонтаж.
        await fixture.Gateway.PutAsync(fixture.Endpoint, $"/kafkaworker/rotations/{cluster}",
            """{"requested_unix":1756500900,"requested_by":"test"}""", lease: null, ct);
        var raw = await fixture.GetAsync($"/kafka/clusters/{cluster}/config");
        await fixture.Gateway.PutAsync(fixture.Endpoint, $"/kafka/clusters/{cluster}/config",
            raw!.Replace("}", ",\"state\":\"TO_REMOVE\"}", StringComparison.Ordinal), lease: null, ct);
        var dying = await fixture.SnapshotAsync(cluster);
        var removed = await deprovision.RunAsync(cluster, dying!.Brokers.Select(b => b.Name).ToList(), ct);

        // Assert 3: контейнер/том удалены, префикс пуст, rotations-заявка очищена.
        removed.IsSuccess.Should().BeTrue($"демонтаж не должен падать: {removed.Error?.Message}");
        (await fixture.Driver.ListNodeObjectsAsync(cluster, ct)).Value.Should().BeEmpty();
        var rest = await fixture.Gateway.RangeAsync(fixture.Endpoint, $"/kafka/clusters/{cluster}/", ct);
        rest.Value.Should().BeEmpty("префикс /kafka/clusters/<C>/ пуст после демонтажа");
        (await fixture.GetAsync($"/kafkaworker/rotations/{cluster}")).Should().BeNull(
            "заявка ротации не переживает удаление кластера (A10-очистка)");
        (await fixture.GetAsync($"/kafkaworker/portalloc/{cluster}")).Should().BeNull();

        await claims.DisposeAsync();
    }
}
