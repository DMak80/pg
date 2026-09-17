using System.Net;
using System.Net.Sockets;
using System.Text;
using AdminPanel.Core;
using AdminPanel.Core.Valkey;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Workers;
using AdminPanel.Api.Operations;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.UnitTests.Workers;

// valkeyworker в общем каркасе воркеров (t03; arch/adminpanel/02 §2.3.3):
// грань «Воркеры» — третья карточка, gateway резолвит /valkeyworker/api/,
// health-поллер пробует valkey-инстансы тем же тиком.
public class ValkeyWorkersInfraTests
{
    private static readonly DateTimeOffset Now = new(2026, 9, 17, 12, 0, 0, TimeSpan.Zero);

    // ===== Двойники сторов (шлюз/хендлер читают только Current) =====

    private sealed class SettablePgStore : ISnapshotStore
    {
        public EtcdSnapshot? Current { get; set; }

        public void Replace(EtcdSnapshot snapshot) => Current = snapshot;
    }

    private sealed class StubKafkaReader : IKafkaSnapshotReader
    {
        public AdminPanel.Core.Kafka.KafkaSnapshot? Current { get; init; }
    }

    private sealed class SettableKafkaStore : IKafkaSnapshotStore
    {
        public AdminPanel.Core.Kafka.KafkaSnapshot? Current { get; set; }

        public void Replace(AdminPanel.Core.Kafka.KafkaSnapshot snapshot) => Current = snapshot;
    }

    private sealed class SettableValkeyStore : IValkeySnapshotStore
    {
        public ValkeySnapshot? Current { get; set; }

        public void Replace(ValkeySnapshot snapshot) => Current = snapshot;
    }

    private sealed class StubValkeyReader : IValkeySnapshotReader
    {
        public ValkeySnapshot? Current { get; init; }
    }

    // Снапшоты-заготовки: пустые pg/kafka + valkey с живым ключом.
    private static SettablePgStore EmptyPg() => new()
    {
        Current = TestSnapshots.Healthy(Now) with { PgWorkerEndpoints = [] },
    };

    private static ValkeySnapshot ValkeySnapshotWith(
        params WorkerEndpoint[] endpoints) => new(
        Now, EtcdReachable: true, ConsecutiveFailures: 0,
        [], [], [.. endpoints], [], [], [], [], 0);

    // ===== Стаб-инстанс API воркера (паттерн WorkerApiGatewayTests.WorkerStub) =====

    private sealed class WorkerStub : IDisposable
    {
        private readonly HttpListener _listener = new();
        private readonly Task _loop;

        public string Url { get; }

        public string? LastPath { get; private set; }

        public string? LastMethod { get; private set; }

        public string? LastBody { get; private set; }

        public int StatusCode { get; set; } = 200;

        public WorkerStub()
        {
            var port = FreePort();
            Url = $"http://127.0.0.1:{port}/";
            _listener.Prefixes.Add(Url);
            _listener.Start();
            _loop = Task.Run(ServeAsync);
        }

        public void Dispose()
        {
            _listener.Close();
            try
            {
                _loop.Wait(TimeSpan.FromSeconds(2));
            }
            catch (AggregateException)
            {
                // слушатель закрыт — цикл завершён
            }
        }

        private async Task ServeAsync()
        {
            while (_listener.IsListening)
            {
                try
                {
                    var ctx = await _listener.GetContextAsync();
                    LastPath = ctx.Request.Url?.PathAndQuery;
                    LastMethod = ctx.Request.HttpMethod;
                    using var reader = new StreamReader(ctx.Request.InputStream, Encoding.UTF8);
                    LastBody = await reader.ReadToEndAsync();
                    ctx.Response.StatusCode = StatusCode;
                    ctx.Response.Close();
                }
                catch (ObjectDisposedException)
                {
                    break;
                }
                catch (HttpListenerException)
                {
                    break;
                }
            }
        }

        private static int FreePort()
        {
            using var probe = new TcpListener(IPAddress.Loopback, 0);
            probe.Start();
            var port = ((IPEndPoint)probe.LocalEndpoint).Port;
            probe.Stop();
            return port;
        }
    }

    private static IWorkerApiGateway NewGateway(
        ISnapshotStore? pg = null,
        IKafkaSnapshotStore? kafka = null,
        IValkeySnapshotStore? valkey = null)
    {
        var services = new ServiceCollection();
        services.AddHttpClient(WorkerApiGateway.HttpClientName);
        var provider = services.BuildServiceProvider();
        return new WorkerApiGateway(
            Options.Create(new WorkerApiOptions()),
            provider.GetRequiredService<IHttpClientFactory>(),
            pg ?? new SettablePgStore(),
            kafka ?? new SettableKafkaStore(),
            valkey ?? new SettableValkeyStore());
    }

    // ===== 1. Грань «Воркеры»: третья карточка =====

    // Arrange: снапшок valkey с живым WorkerEndpoints(https://vwk:8080, id "i1").
    // Act: GetWorkersQueryHandler.Handle. Assert: третья карточка Worker=="valkeyworker",
    // инстанс i1, TargetCert из WorkerApiCert снапшота.
    [Fact]
    public async Task GetWorkers_ValkeySnapshot_AddsThirdCard()
    {
        var cert = new WorkerApiCert("abc123", "CN=vwt", "CN=ca", ["vwk"],
            Now, Now.AddYears(1), 1756500000, "admin");
        var valkey = new SettableValkeyStore
        {
            Current = ValkeySnapshotWith(new WorkerEndpoint("i1", "https://vwk:8080", 5)) with
            {
                WorkerApiCert = cert,
            },
        };
        var handler = new GetWorkersQueryHandler(
            EmptyPg(), new StubKafkaReader(), valkey);

        // Act
        var result = await handler.Handle(new GetWorkersQuery(), CancellationToken.None);

        // Assert: третья карточка с инстансом и целевым сертом.
        result.IsSuccess.Should().BeTrue();
        var card = result.Value.Workers.Single(w => w.Worker == "valkeyworker");
        card.Instances.Should().ContainSingle().Which.Instance.Should().Be("i1");
        card.TargetCert.Should().NotBeNull();
        card.TargetCert!.Thumbprint.Should().Be("abc123");
        // kafka-карточка не пострадала (порядок pg→kafka→valkey).
        result.Value.Workers.Select(w => w.Worker).Should()
            .Equal("pgworker", "kafkaworker", "valkeyworker");
    }

    // ===== 2. Gateway: резолв endpoints по /valkeyworker/api/ =====

    // Arrange: gateway с IValkeySnapshotStore(живой ключ) и пустыми pg/kafka.
    // Act: SendAsync("valkeyworker",...). Assert: не кидает ArgumentOutOfRange,
    // запрос уходит на URL живого ключа (стаб-хендлер фиксирует).
    [Fact]
    public async Task Gateway_Valkeyworker_ResolvesEndpoints()
    {
        using var stub = new WorkerStub();
        var valkey = new SettableValkeyStore
        {
            Current = ValkeySnapshotWith(new WorkerEndpoint("i1", stub.Url, 5)),
        };
        var gateway = NewGateway(valkey: valkey);

        // Act: мутация-прокси на valkeyworker.
        var result = await gateway.SendAsync(
            "valkeyworker", HttpMethod.Post, "/api/valkey/clusters",
            body: null, requestedBy: null, CancellationToken.None);

        // Assert: запрос ушёл на URL живого ключа (не ArgumentOutOfRange).
        result.StatusCode.Should().Be(200);
        stub.LastPath.Should().Be("/api/valkey/clusters");
        stub.LastMethod.Should().Be("POST");
    }

    // Arrange: пустой IValkeySnapshotStore. Act: SendAsync("valkeyworker",...).
    // Assert: WorkerApiUnavailableException (→ 503 панели).
    [Fact]
    public async Task Gateway_Valkeyworker_NoEndpoints_ThrowsUnavailable()
    {
        var gateway = NewGateway(valkey: new SettableValkeyStore());

        // Act
        var act = async () => await gateway.SendAsync(
            "valkeyworker", HttpMethod.Post, "/api/valkey/clusters",
            body: null, requestedBy: null, CancellationToken.None);

        // Assert
        await act.Should().ThrowAsync<WorkerApiUnavailableException>();
    }

    // ===== 3. Health-поллер: valkey-блок тика =====

    // Arrange: valkey-снапшот с живым ключом i1; /healthz стаба отвечает 503.
    // Act: WorkerHealthPoller.RunOnceAsync. Assert: IValkeyWorkerHealthStore.Current
    // содержит i1 с Degraded (по образцу kafka-блока — стаб HttpClient).
    [Fact]
    public async Task HealthPoller_ValkeyEndpoints_ProbesValkeyWorker()
    {
        using var stub = new WorkerStub { StatusCode = 503 };
        var valkeyReader = new StubValkeyReader
        {
            Current = ValkeySnapshotWith(new WorkerEndpoint("i1", stub.Url, 5)),
        };
        var valkeyStore = new ValkeyWorkerHealthStore();
        var poller = NewPoller(valkeyReader, valkeyStore, _ =>
            new HttpResponseMessage(HttpStatusCode.ServiceUnavailable));

        // Act
        await poller.RunOnceAsync(CancellationToken.None);

        // Assert: valkey-стор получил Degraded (тот же тик/клиент/семантика).
        valkeyStore.Current.Should().NotBeNull();
        valkeyStore.Current!.Should().ContainSingle()
            .Which.Status.Should().Be(WorkerHealthStatus.Degraded);
    }

    // Поллер с valkey-парой (паттерн WorkerHealthPollerTests.Poller).
    private static WorkerHealthPoller NewPoller(
        IValkeySnapshotReader valkeyReader,
        IValkeyWorkerHealthStore valkeyStore,
        Func<HttpRequestMessage, HttpResponseMessage> respond)
    {
        var handler = new StubHttpHandler(respond);
        return new WorkerHealthPoller(
            new StubPgReader(TestSnapshots.Healthy(Now) with { PgWorkerEndpoints = [] }),
            new WorkerHealthStore(),
            new StubKafkaReader(),
            new KafkaWorkerHealthStore(),
            valkeyReader,
            valkeyStore,
            new StubHttpClientFactory(handler),
            Options.Create(new WorkerApiOptions { HealthIntervalSec = 15, TimeoutSec = 3 }),
            new FixedTimeProvider { Utc = Now },
            NullLogger<WorkerHealthPoller>.Instance);
    }

    private sealed class StubPgReader : ISnapshotReader
    {
        public StubPgReader(EtcdSnapshot? snapshot) => Current = snapshot;

        public EtcdSnapshot? Current { get; }
    }

    private sealed class StubHttpHandler(Func<HttpRequestMessage, HttpResponseMessage> responder)
        : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
            => Task.FromResult(responder(request));
    }

    private sealed class StubHttpClientFactory(HttpMessageHandler handler) : IHttpClientFactory
    {
        // Свой handler перекрывает именованный клиент (стаб /healthz).
        public HttpClient CreateClient(string name) => new(handler, disposeHandler: false);
    }
}
