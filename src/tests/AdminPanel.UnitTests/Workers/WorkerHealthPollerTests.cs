using System.Net;
using AdminPanel.Core;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Workers;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.UnitTests.Workers;

// WorkerHealthPoller (spec §3.4 D4; arch/adminpanel/02 §2.3.1): опрос /healthz
// живых инстансов PgWorker — 200 → Healthy, 503 → Degraded, сетевой сбой →
// Unreachable; пустые endpoints → пустой список (правило молчит).
public class WorkerHealthPollerTests
{
    private static readonly DateTimeOffset Now = new(2026, 9, 1, 12, 0, 0, TimeSpan.Zero);

    // Handler-дабл: ответ или исключение на каждый запрос.
    private sealed class FakeHandler(Func<HttpRequestMessage, HttpResponseMessage> responder) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
            => Task.FromResult(responder(request));
    }

    private sealed class StubFactory(HttpMessageHandler handler) : IHttpClientFactory
    {
        public HttpClient CreateClient(string name) => new(handler, disposeHandler: false);
    }

    // ISnapshotReader-дабл: снапшот с одним живым endpoint /pgworker/api/w1.
    private sealed class StubReader(EtcdSnapshot? snapshot) : ISnapshotReader
    {
        public EtcdSnapshot? Current { get; } = snapshot;
    }

    private static (WorkerHealthPoller Poller, WorkerHealthStore Store) Poller(
        Func<HttpRequestMessage, HttpResponseMessage> respond,
        EtcdSnapshot? snapshot = null,
        AdminPanel.Etcd.IKafkaSnapshotReader? kafkaReader = null,
        AdminPanel.Etcd.Workers.KafkaWorkerHealthStore? kafkaStore = null)
    {
        var store = new WorkerHealthStore();
        var time = new FixedTimeProvider { Utc = Now };
        var poller = new WorkerHealthPoller(
            new StubReader(snapshot ?? TestSnapshots.Healthy(Now)), store,
            kafkaReader ?? new StubKafkaReader(null), kafkaStore ?? new KafkaWorkerHealthStore(),
            new StubFactory(new FakeHandler(respond)),
            Options.Create(new WorkerApiOptions { HealthIntervalSec = 15, TimeoutSec = 3 }),
            time, NullLogger<WorkerHealthPoller>.Instance);
        return (poller, store);
    }

    [Fact]
    public async Task RunOnce_Healthz200_MarkedHealthy()
    {
        // Arrange: один живой endpoint /pgworker/api/, /healthz отвечает 200.
        var (poller, store) = Poller(_ => new HttpResponseMessage(HttpStatusCode.OK));

        // Act
        await poller.RunOnceAsync(CancellationToken.None);

        // Assert
        store.Current!.Should().ContainSingle().Which.Status.Should().Be(WorkerHealthStatus.Healthy);
    }

    [Fact]
    public async Task RunOnce_Healthz503_MarkedDegraded()
    {
        // Arrange: /healthz = 503 (Degraded воркера: секции etcd/docker/loops).
        var (poller, store) = Poller(_ => new HttpResponseMessage(HttpStatusCode.ServiceUnavailable));

        // Act
        await poller.RunOnceAsync(CancellationToken.None);

        // Assert
        var w = store.Current!.Should().ContainSingle().Subject;
        w.Status.Should().Be(WorkerHealthStatus.Degraded);
        w.Detail.Should().Contain("503");
    }

    [Fact]
    public async Task RunOnce_NetworkError_MarkedUnreachable()
    {
        // Arrange: lease-ключ жив, но соединение падает (панель не достучалась).
        var (poller, store) = Poller(_ => throw new HttpRequestException("connection refused"));

        // Act
        await poller.RunOnceAsync(CancellationToken.None);

        // Assert
        store.Current!.Should().ContainSingle().Which.Status.Should().Be(WorkerHealthStatus.Unreachable);
    }

    [Fact]
    public async Task RunOnce_NoLiveEndpoints_StoreEmpty()
    {
        // Arrange: живых ключей /pgworker/api/ нет (воркер не поднимался/lease
        // истекли) — домен worker-api-unreachable, не этого poller'а.
        var empty = TestSnapshots.Healthy(Now) with { PgWorkerEndpoints = [] };
        var (poller, store) = Poller(_ => new HttpResponseMessage(HttpStatusCode.OK), empty);

        // Act
        await poller.RunOnceAsync(CancellationToken.None);

        // Assert: пустой список (НЕ null) — правило worker-unhealthy молчит.
        store.Current.Should().NotBeNull().And.BeEmpty();
    }

    // IKafkaSnapshotReader-дабл: kafka-снапшот домена (null — домен не тикал).
    private sealed class StubKafkaReader(AdminPanel.Core.Kafka.KafkaSnapshot? snapshot)
        : AdminPanel.Etcd.IKafkaSnapshotReader
    {
        public AdminPanel.Core.Kafka.KafkaSnapshot? Current { get; } = snapshot;
    }

    private static AdminPanel.Core.Kafka.KafkaSnapshot KafkaSnapshotWith(
        params WorkerEndpoint[] endpoints) => new(
        Now, EtcdReachable: true, ConsecutiveFailures: 0,
        [], [], [], [], [], [.. endpoints], WorkerHealth: [], Probes: [], Alerts: [],
        ParseErrors: [], UnknownKeyCount: 0);

    [Fact]
    public async Task RunOnce_KafkaHealthz503_MarkedDegradedInKafkaStore()
    {
        // Arrange: kafka-снапшот с живым ключом /kafkaworker/api/kw1; /healthz → 503.
        var kafkaStore = new KafkaWorkerHealthStore();
        var kafka = new StubKafkaReader(KafkaSnapshotWith(new WorkerEndpoint("kw1", "http://kafkaworker:8080", 1)));
        var (poller, _) = Poller(
            _ => new HttpResponseMessage(HttpStatusCode.ServiceUnavailable),
            kafkaReader: kafka, kafkaStore: kafkaStore);

        // Act
        await poller.RunOnceAsync(CancellationToken.None);

        // Assert: kafka-стор получил Degraded (тот же тик/клиент/семантика, что pg).
        kafkaStore.Current!.Should().ContainSingle()
            .Which.Status.Should().Be(WorkerHealthStatus.Degraded);
    }

    [Fact]
    public async Task RunOnce_NoKafkaSnapshot_KafkaStoreEmpty()
    {
        // Arrange: kafka-домен ещё не тикал (нет снапшота — нет ключей).
        var kafkaStore = new KafkaWorkerHealthStore();
        var (poller, _) = Poller(
            _ => new HttpResponseMessage(HttpStatusCode.OK),
            kafkaReader: new StubKafkaReader(null), kafkaStore: kafkaStore);

        // Act
        await poller.RunOnceAsync(CancellationToken.None);

        // Assert: пустой список (правило worker-unhealthy молчит).
        kafkaStore.Current.Should().BeEmpty();
    }
}
