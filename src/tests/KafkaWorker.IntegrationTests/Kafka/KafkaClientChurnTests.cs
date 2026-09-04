using System.Diagnostics;
using FluentAssertions;
using KafkaWorker.Core;
using KafkaWorker.Docker.Drivers;
using KafkaWorker.Docker.Engine;
using KafkaWorker.Etcd.Coordination;
using KafkaWorker.Etcd.Parsing;
using KafkaWorker.IntegrationTests.Etcd;
using KafkaWorker.Core.Templates;
using KafkaWorker.Provisioning.Kafka;
using KafkaWorker.Provisioning.Processes;

namespace KafkaWorker.IntegrationTests.Kafka;

// Churn-интеграция (t05, spec §7.5): Active-кластер с endpoints на ЗАКРЫТЫЙ
// порт (зонд свободных — рантайм) — воркер-цикл (supervise-проба) не churn'ит
// клиентов: кэш + backoff держат CreatedClients ≤ 2, потоки процесса стабильны.
// Вehicle — проба надзора (DescribeCluster): ListConsumerGroups на недоступном
// bootstrap роняет процесс нативно (дефект librdkafka 2.14.2, репро 2026-09-04),
// поэтому коллектор против лежащего кластера интеграционно не гоняется — его
// гейт/пре-гейт покрыты юнитами (KafkaMetricsCollectorTests). Литералов :16000 нет.
[Collection(EtcdCollection.Name)] // etcd-only фикстура (контейнеров kafka нет)
public class KafkaClientChurnTests(EtcdFixture etcd)
{
    [Fact]
    public async Task UnreachableCluster_DoesNotChurnClients()
    {
        // Arrange: Active-кластер, endpoints на закрытый порт + креды, БЕЗ
        // brokers-ключей (лестница/пересоздания вне сценария — гейт пробы
        // изолирован, паттерн KafkaActiveGateTests).
        var port = EtcdFixture.ReserveHostPort();
        var cluster = $"churn{Guid.NewGuid().ToString("N")[..8]}"; // уникально в etcd фикстуры
        await etcd.PutAsync($"/kafka/clusters/{cluster}/config",
            """{"brokers":1,"replication_factor":1,"min_insync_replicas":1,"default_partitions":1,"default_retention_ms":3600000}""");
        await etcd.PutAsync($"/kafka/clusters/{cluster}/endpoints", $"127.0.0.1:{port}");
        await etcd.PutAsync($"/kafka/clusters/{cluster}/app_user", "app");
        await etcd.PutAsync($"/kafka/clusters/{cluster}/app_password", "deadbeefdeadbeefdeadbeefdeadbeef");

        var ep = new[] { etcd.Endpoint };
        var claims = new ClaimStore(ep, etcd.Gateway, TimeProvider.System);
        await claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken);
        var journal = new WorkJournal(etcd.Gateway, ep);
        var driver = new PlainClusterDriver(
            [new HostEndpoint("local", "unix:///var/run/docker.sock")],
            new DockerEngineFactory());
        var factory = new KafkaAdminClientFactory(TimeSpan.FromSeconds(3));
        var backoff = new KafkaClusterBackoff(TimeProvider.System);
        var supervisor = new NodeSupervisor(
            etcd.Gateway, ep, driver, claims, journal, factory,
            new ProvisioningOptions(21000, 21100, 100, 90, null, "apache/kafka:4.0.0"),
            new BrokerCertificateCache(),
            backoff: backoff);

        var range = await etcd.Gateway.RangeAsync(
            etcd.Endpoint, "/kafka/clusters/", TestContext.Current.CancellationToken);
        var snapshot = KafkaSnapshotParser.Parse(range.Value).Value.Single(c => c.Cluster == cluster);

        var threadsBefore = Process.GetCurrentProcess().Threads.Count;

        // Act: 6 тиков надзора (первый — реальный probe-контакт: клиент #1, фейл,
        // RecordFailure; остальные — skip по backoff-окну 15 c). Без задержек.
        for (var i = 0; i < 6; i++)
        {
            var tick = await supervisor.RunAsync(snapshot, TestContext.Current.CancellationToken);
            tick.IsSuccess.Should().BeTrue($"слепая проба — не ошибка тика: {tick.Error?.Message}");
        }

        // Assert: ≤ 2 нативных клиента (первый + не более одного
        // unhealthy-пересоздания), потоки не растут (churn погашен).
        factory.CreatedClients.Should().BeInRange(1, 2,
            $"кэш+backoff: 6 тиков = 1 клиент (+≤1 пересоздание), не 5–7 на тик; фактически {factory.CreatedClients}");
        var threadsAfter = Process.GetCurrentProcess().Threads.Count;
        (threadsAfter - threadsBefore).Should().BeLessThanOrEqualTo(10);
    }
}
