using FluentAssertions;
using KafkaWorker.App.Loops;
using KafkaWorker.Core;
using KafkaWorker.Etcd.Coordination;
using KafkaWorker.Provisioning.Kafka;
using KafkaWorker.Provisioning.Processes;
using Microsoft.Extensions.Logging.Abstractions;

namespace KafkaWorker.IntegrationTests.Kafka;

// Гейт Active-ветки (t05, spec §7.5/§3.2, ревью F1): кластер в активном
// backoff-окне — ActiveAsync (надзор-гейт + skip E–J/D) исполняется
// хост-процессом без kafka-контакта: тик Success, фабрика не зовётся.
[Collection(KafkaCollection.Name)]
public class KafkaActiveGateTests(KafkaClusterFixture fixture)
{
    [Fact]
    public async Task ActiveAsync_BackoffWindow_SkipsKafkaSteps()
    {
        var ct = TestContext.Current.CancellationToken;
        var cluster = fixture.Cluster("gate1");

        // Arrange: Active-кластер с endpoints на закрытый порт (зонд), БЕЗ
        // brokers-ключей; backoff-окно активно заранее.
        var port = KafkaClusterFixture.ReserveClosedPort();
        await fixture.PutAsync($"/kafka/clusters/{cluster}/config",
            """{"brokers":1,"replication_factor":1,"min_insync_replicas":1,"default_partitions":1,"default_retention_ms":3600000}""");
        await fixture.PutAsync($"/kafka/clusters/{cluster}/endpoints", $"127.0.0.1:{port}");
        await fixture.PutAsync($"/kafka/clusters/{cluster}/app_user", "app");
        await fixture.PutAsync($"/kafka/clusters/{cluster}/app_password", "deadbeefdeadbeefdeadbeefdeadbeef");

        var claims = new ClaimStore([fixture.Endpoint], fixture.Gateway, TimeProvider.System);
        await claims.TryClaimClusterAsync(cluster, ct);
        var journal = new WorkJournal(fixture.Gateway, [fixture.Endpoint]);
        var factory = new KafkaAdminClientFactory(TimeSpan.FromSeconds(3));
        var backoff = new KafkaClusterBackoff(TimeProvider.System);
        backoff.RecordFailure(cluster, "connection refused"); // окно 15 c активно

        var ep = new[] { fixture.Endpoint };
        var processes = new KafkaClusterProcesses(
            new ProvisioningProcess(fixture.Gateway, ep, fixture.Driver, claims, journal,
                new PortAllocLock(ep, fixture.Gateway, TimeProvider.System, claims.InstanceId),
                new PortAllocIndex(fixture.Gateway, ep, NullLogger<PortAllocIndex>.Instance),
                new AppSecretEnsurer(fixture.Gateway, ep),
                factory, new ClusterConfigConverger(factory), fixture.Options, snapshot: null),
            new DeprovisioningProcess(fixture.Gateway, ep, fixture.Driver, claims, journal, snapshot: null),
            new NodeSupervisor(fixture.Gateway, ep, fixture.Driver, claims, journal,
                factory, fixture.Options, backoff),
            new ClusterConfigConverger(factory),
            new PartitionReassignerProcess(fixture.Gateway, ep, fixture.Driver, claims, journal,
                factory, new ReassignOptions(15, 10, 180, 120), TimeProvider.System),
            new RemoveBrokerProcess(fixture.Gateway, ep, fixture.Driver, claims, journal,
                factory, fixture.Options),
            new AddBrokerProcess(fixture.Gateway, ep, fixture.Driver, claims, journal,
                new PortAllocLock(ep, fixture.Gateway, TimeProvider.System, claims.InstanceId),
                new PortAllocIndex(fixture.Gateway, ep, NullLogger<PortAllocIndex>.Instance),
                factory, fixture.Options),
            new AppPasswordRotator(fixture.Gateway, ep, fixture.Driver, claims, journal,
                factory, fixture.Options, snapshot: null),
            new NodeRegenerator(fixture.Gateway, ep, fixture.Driver, claims, journal, fixture.Options),
            new TopicSyncProcess(fixture.Gateway, ep, claims, journal,
                factory, TimeProvider.System, intervalSec: 15),
            backoff);

        // Act: один полный тик Active-ветки в активном backoff-окне.
        var snap = await fixture.SnapshotAsync(cluster);
        var result = await processes.ActiveAsync(snap!, ct);

        // Assert: тик успех; kafka-контакт не выполнялся (гейт до converger'а
        // и TopicSync; supervise-проба тоже гейтится — без brokers это
        // единственный путь к фабрике); брокеры (их нет) не тронуты.
        result.IsSuccess.Should().BeTrue("skip по backoff — не ошибка тика");
        factory.CreatedClients.Should().Be(0,
            $"ActiveAsync в окне не должен создавать клиентов (E–J/D skip, проба надзора гейтится); фактически {factory.CreatedClients}");
    }
}
