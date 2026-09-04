using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using AdminPanel.Core;
using AdminPanel.Core.Kafka;
using AdminPanel.Etcd;
using AdminPanel.Infrastructure;
using AdminPanel.Probes;
using AdminPanel.Probes.Kafka;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.IntegrationTests;

// Репро churn-инцидента 2026-09-02 (t11): Active-кластер с endpoints на
// закрытые порты — connection refused за миллисекунды. До фикса каждый тик
// плодил 5–7 нативных клиентов (rd_kafka-инстансы + poll-потоки, ~99% ядра);
// после — клиент один на попытку, а частоту попыток держит backoff loop'а.
// Порты динамические (AGENTS.md), контейнеров не нужно — «закрытый порт»
// и есть носитель отказа.
public class KafkaProbeClosedPortsTests : IDisposable
{
    // Динамический порт без слушателя: мгновенный connection refused.
    private static int ClosedPort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private readonly KafkaClientCache _cache = new();

    // Валидный PEM CA (t03): librdkafka парсит ssl.ca.pem при Build() — строка-
    // заглушка роняла бы создание клиента до сетевого отказа (в тесте нужен
    // честный connection refused).
    private static string CaPem()
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            "CN=probe-test-ca", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        using var cert = request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(1));
        return cert.ExportCertificatePem();
    }

    // Репро-расклад инцидента: три брокера на закрытых портах (16003–16005).
    private static string DeadBootstrap()
        => string.Join(",", Enumerable.Range(0, 3).Select(_ => $"127.0.0.1:{ClosedPort()}"));

    public void Dispose() => _cache.Dispose();

    private static KafkaSnapshot Snapshot(string endpoints) => new(
        new DateTimeOffset(2026, 9, 2, 12, 0, 0, TimeSpan.Zero),
        EtcdReachable: true, ConsecutiveFailures: 0,
        [new KafkaClusterInfo(
            "churn", KafkaClusterState.Active, 3, 3, 2, 12, 604800000, 1756500000,
            endpoints, [], [])],
        Rotations: [], Rebalances: [], Reassignments: [], Regens: [],
        WorkerEndpoints: [], WorkerHealth: [], Probes: [], Alerts: [], ParseErrors: [], UnknownKeyCount: 0);

    [Fact]
    public async Task DeadCluster_FailsHonestly_BoundedClients_BackoffSkipsTicks()
    {
        // Arrange: реальный Confluent-адаптер на кэше, loop на управляемом
        // времени; короткий RequestTimeout — отказ наблюдается быстро.
        var bootstrap = DeadBootstrap();
        var snapshotStore = new KafkaSnapshotStore();
        snapshotStore.Replace(Snapshot(bootstrap));
        var secrets = new KafkaSecretsStore();
        secrets.Replace(new Dictionary<string, KafkaClusterSecrets>
        {
            ["churn"] = new("churn", "admin", "SecretPassword0123456789", CaPem()),
        });
        var probeStore = new KafkaProbeStore();
        var clock = new FixedTimeProvider();
        var loop = new KafkaProbeLoop(
            snapshotStore, secrets, new ConfluentKafkaProbeClient(_cache), probeStore,
            Options.Create(new KafkaProbeOptions { IntervalSeconds = 15, TimeoutSeconds = 2 }),
            Options.Create(new ProbesOptions()),
            clock,
            NullLogger<KafkaProbeLoop>.Instance);

        // Act/Assert: попытка 1 (t0) — честный отказ, ОДИН нативный клиент.
        await loop.RunOnceAsync(TestContext.Current.CancellationToken);
        _cache.CreatedClients.Should().Be(1);
        var failed = probeStore.Current!.Results.Single();
        failed.Ok.Should().BeFalse();
        failed.Target.Should().Be("churn");
        failed.Error.Should().Contain("DescribeCluster");
        probeStore.Current.Clusters.Should().BeEmpty();

        // Попытка 2 (t0+15) — ещё один клиент (фейл → Invalidate → пересоздание).
        clock.Utc = clock.Utc.AddSeconds(15);
        await loop.RunOnceAsync(TestContext.Current.CancellationToken);
        _cache.CreatedClients.Should().Be(2);

        // Тики t0+30 и t0+60 — окно backoff (60 c): клиентов не создаётся,
        // состояние несёт отказ с пометкой backoff.
        foreach (var advance in new[] { 15, 30 })
        {
            clock.Utc = clock.Utc.AddSeconds(advance);
            await loop.RunOnceAsync(TestContext.Current.CancellationToken);
        }

        _cache.CreatedClients.Should().Be(2);
        var skipped = probeStore.Current!.Results.Single();
        skipped.Ok.Should().BeFalse();
        skipped.Error.Should().Contain("backoff");
        skipped.Error.Should().NotContain("SecretPassword");

        // Попытка 3 (t0+75): окно истекло — новый клиент, снова отказ;
        // дальше окно 300 c: до t0+375 новых клиентов нет.
        clock.Utc = clock.Utc.AddSeconds(15);
        await loop.RunOnceAsync(TestContext.Current.CancellationToken);
        _cache.CreatedClients.Should().Be(3);
        clock.Utc = clock.Utc.AddSeconds(200);
        await loop.RunOnceAsync(TestContext.Current.CancellationToken);
        _cache.CreatedClients.Should().Be(3);

        // Итог: 4 нативных клиента за ~4 виртуальные минуты (1 на попытку,
        // не 5–7 на тик) — churn-бюджет приёмки t11 выдержан.
    }
}
