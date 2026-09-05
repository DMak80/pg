using System.Net;
using System.Net.Sockets;
using System.Text;
using AdminPanel.Core;
using AdminPanel.Core.Kafka;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Workers;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.UnitTests.Workers;

// WorkerApiGateway (arch/01 §1: панель — прокси мутаций): резолв URL по живым
// ключам снапшота, failover, X-Api-Key/X-Requested-By; на внутрипроцессном
// HttpListener-стабе.
public class WorkerApiGatewayTests
{
    // Сеттабельные двойники сторов: шлюз читает только Current.
    private sealed class SettableSnapshotStore : ISnapshotStore
    {
        public EtcdSnapshot? Current { get; set; }

        public void Replace(EtcdSnapshot snapshot) => Current = snapshot;
    }

    private sealed class SettableKafkaStore : IKafkaSnapshotStore
    {
        public KafkaSnapshot? Current { get; set; }

        public void Replace(KafkaSnapshot snapshot) => Current = snapshot;
    }

    // Стаб-инстанс API: слушает порт, помнит последний запрос (заголовки/тело),
    // отвечает заготовленным статусом/телом. Dead-инстанс — порт без слушателя.
    private sealed class WorkerStub : IDisposable
    {
        private readonly HttpListener _listener = new();
        private readonly Task _loop;

        public string Url { get; }

        public string? LastRequestedBy { get; private set; }

        public string? LastApiKey { get; private set; }

        public string? LastBody { get; private set; }

        public int StatusCode { get; set; } = 201;

        public string ResponseBody { get; set; } = """{"name":"smoke"}""";

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
                // слушатель закрыт — цикл завершён; ждать нечего
            }
        }

        private async Task ServeAsync()
        {
            while (_listener.IsListening)
            {
                try
                {
                    var ctx = await _listener.GetContextAsync();
                    LastRequestedBy = ctx.Request.Headers["X-Requested-By"];
                    LastApiKey = ctx.Request.Headers["X-Api-Key"];
                    using var reader = new StreamReader(ctx.Request.InputStream, Encoding.UTF8);
                    LastBody = await reader.ReadToEndAsync();
                    ctx.Response.StatusCode = StatusCode;
                    var bytes = Encoding.UTF8.GetBytes(ResponseBody);
                    ctx.Response.ContentType = "application/json";
                    await ctx.Response.OutputStream.WriteAsync(bytes);
                    ctx.Response.Close();
                }
                catch (ObjectDisposedException)
                {
                    break; // Dispose закрыл слушатель
                }
                catch (HttpListenerException)
                {
                    break;
                }
            }
        }

        // Свободный порт: занять TcpListener(0) и отпустить.
        private static int FreePort()
        {
            using var probe = new TcpListener(IPAddress.Loopback, 0);
            probe.Start();
            var port = ((IPEndPoint)probe.LocalEndpoint).Port;
            probe.Stop();
            return port;
        }
    }

    // Мёртвый URL: порт свободен, никто не слушает → соединение отклонено.
    private static string DeadUrl()
    {
        using var probe = new TcpListener(IPAddress.Loopback, 0);
        probe.Start();
        var port = ((IPEndPoint)probe.LocalEndpoint).Port;
        probe.Stop();
        return $"http://127.0.0.1:{port}/";
    }

    private static IWorkerApiGateway NewGateway(
        ISnapshotStore? pg = null,
        IKafkaSnapshotStore? kafka = null,
        WorkerApiOptions? options = null)
    {
        var services = new ServiceCollection();
        services.AddHttpClient("workers");
        var provider = services.BuildServiceProvider();
        return new WorkerApiGateway(
            Options.Create(options ?? new WorkerApiOptions()),
            provider.GetRequiredService<IHttpClientFactory>(),
            pg ?? new SettableSnapshotStore(),
            kafka ?? new SettableKafkaStore());
    }

    // Снапшот с pg-ключами доступа (остальное — пустое; важен только список).
    private static ISnapshotStore PgStore(params WorkerEndpoint[] endpoints)
    {
        var snapshot = TestSnapshots.Healthy(DateTimeOffset.UtcNow) with { PgWorkerEndpoints = [.. endpoints] };
        return new SettableSnapshotStore { Current = snapshot };
    }

    private static IKafkaSnapshotStore KafkaStore(params WorkerEndpoint[] endpoints)
        => new SettableKafkaStore
        {
            Current = new KafkaSnapshot(
                DateTimeOffset.UtcNow, EtcdReachable: true, ConsecutiveFailures: 0,
                [], Rotations: [], Rebalances: [], Reassignments: [], Regens: [],
                WorkerEndpoints: [.. endpoints],
                WorkerHealth: [],
                Probes: [], Alerts: [], ParseErrors: [], UnknownKeyCount: 0),
        };

    [Fact]
    public async Task SendAsync_201WithBody_ReturnsResult()
    {
        // Arrange
        using var stub = new WorkerStub();
        var gateway = NewGateway(PgStore(new WorkerEndpoint("i1", stub.Url, 1)));

        // Act
        var result = await gateway.SendAsync(
            "pgworker", HttpMethod.Post, "/api/clusters",
            new { name = "smoke" }, requestedBy: "opsuser", CancellationToken.None);

        // Assert
        result.StatusCode.Should().Be(201);
        result.Body.Should().Contain("\"name\":\"smoke\"");
        stub.LastRequestedBy.Should().Be("opsuser"); // идентичность оператора (spec §3.7)
        stub.LastApiKey.Should().BeNull(); // t03-pg: X-Api-Key удалён — только mTLS
    }

    [Fact]
    public async Task SendAsync_PgMutation_NoApiKeyHeader_Sent()
    {
        // Arrange: живой pgworker-эндпоинт (стаб) в снапшоте; дефолтные опции.
        using var stub = new WorkerStub();
        var gateway = NewGateway(PgStore(new WorkerEndpoint("i1", stub.Url, 1)));

        // Act: мутация через шлюз (контракт t03-pg: mTLS-only у обоих воркеров).
        var result = await gateway.SendAsync(
            "pgworker", HttpMethod.Post, "/api/clusters",
            new { name = "smoke" }, requestedBy: "opsuser", CancellationToken.None);

        // Assert: X-Api-Key в исходящем запросе НЕТ; X-Requested-By сохранён.
        result.StatusCode.Should().Be(201);
        stub.LastApiKey.Should().BeNull();
        stub.LastRequestedBy.Should().Be("opsuser");
    }

    [Fact]
    public async Task SendAsync_409ProblemDetails_PassesBodyThrough()
    {
        // Arrange
        using var stub = new WorkerStub
        {
            StatusCode = 409,
            ResponseBody = """{"title":"cluster exists","status":409}""",
        };
        var gateway = NewGateway(PgStore(new WorkerEndpoint("i1", stub.Url, 1)));

        // Act
        var result = await gateway.SendAsync(
            "pgworker", HttpMethod.Post, "/api/clusters", null, null, CancellationToken.None);

        // Assert
        result.StatusCode.Should().Be(409);
        result.Body.Should().Contain("cluster exists");
        stub.LastRequestedBy.Should().BeNull(); // null → заголовок не шлётся
    }

    [Fact]
    public async Task SendAsync_FirstDead_FailsOverToSecond()
    {
        // Arrange: два живых ключа, первый (по InstanceId) — мёртвый порт
        using var alive = new WorkerStub();
        var gateway = NewGateway(PgStore(
            new WorkerEndpoint("a-first", DeadUrl(), 1),
            new WorkerEndpoint("z-second", alive.Url, 2)));

        // Act
        var result = await gateway.SendAsync(
            "pgworker", HttpMethod.Post, "/api/clusters", null, "ops", CancellationToken.None);

        // Assert
        result.StatusCode.Should().Be(201);
        alive.LastRequestedBy.Should().Be("ops");
    }

    [Fact]
    public async Task SendAsync_AllDead_ThrowsUnavailable()
    {
        // Arrange
        var gateway = NewGateway(PgStore(
            new WorkerEndpoint("a", DeadUrl(), 1),
            new WorkerEndpoint("b", DeadUrl(), 2)));

        // Act
        var act = () => gateway.SendAsync(
            "pgworker", HttpMethod.Delete, "/api/clusters/x", null, null, CancellationToken.None);

        // Assert
        (await act.Should().ThrowAsync<WorkerApiUnavailableException>())
            .Which.Message.Should().Contain("pgworker");
    }

    [Fact]
    public async Task SendAsync_NoLiveKeys_ThrowsUnavailable()
    {
        // Arrange: снапшот есть, ключей доступа нет (воркер не поднялся)
        var gateway = NewGateway(PgStore());

        // Act
        var act = () => gateway.SendAsync(
            "pgworker", HttpMethod.Post, "/api/clusters", null, null, CancellationToken.None);

        // Assert
        await act.Should().ThrowAsync<WorkerApiUnavailableException>();
    }

    [Fact]
    public async Task SendAsync_KafkaWorker_ResolvesKafkaStore()
    {
        // Arrange
        using var stub = new WorkerStub();
        var gateway = NewGateway(kafka: KafkaStore(new WorkerEndpoint("k1", stub.Url, 3)));

        // Act
        var result = await gateway.SendAsync(
            "kafkaworker", HttpMethod.Post, "/api/kafka/clusters", null, null, CancellationToken.None);

        // Assert
        result.StatusCode.Should().Be(201);
    }
}
