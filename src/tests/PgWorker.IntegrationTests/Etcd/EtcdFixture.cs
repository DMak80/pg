using System.Net;
using System.Net.Sockets;
using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using PgWorker.Etcd.Client;
using Xunit;

namespace PgWorker.IntegrationTests.Etcd;

// Testcontainers-etcd: generic-контейнер quay.io/coreos/etcd:v3.5.21 (задача 13; образ
// как в AdminPanel-тестах). Gateway /v3/* включён в 3.5 по умолчанию. Готовность —
// свой POST-ретрай: встроенные HTTP-wait шлют GET, а /v3/* требует POST.
public sealed class EtcdFixture : IAsyncLifetime
{
    private readonly IContainer _container;

    private readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(10) };

    public string Endpoint { get; private set; } = "";

    // Готов и до InitializeAsync: на фиксированном порту доступен «мёртвому» endpoint.
    public EtcdGateway Gateway { get; }

    // hostPort (тесты недоступности, ревью Ф7): фиксированный порт публикации —
    // endpoint известен ДО старта контейнера, ClaimStore можно стартовать раньше
    // etcd. Случайный порт не подходит: docker выделяет его заново на каждый старт.
    // CTOR internal: у фикстуры коллекции xUnit требует единственный публичный CTOR.
    public EtcdFixture()
        : this(null)
    {
    }

    internal EtcdFixture(int? hostPort)
    {
        var builder = new ContainerBuilder("quay.io/coreos/etcd:v3.5.21")
            .WithCommand(
                "etcd",
                "--name=test",
                "--data-dir=/etcd-data",
                "--listen-client-urls=http://0.0.0.0:2379",
                "--advertise-client-urls=http://127.0.0.1:2379");
        _container = (hostPort is { } port
                ? builder.WithPortBinding(port, 2379)
                : builder.WithPortBinding(2379, assignRandomHostPort: true))
            .Build();
        Endpoint = hostPort is { } p ? $"http://localhost:{p}" : "";
        Gateway = new EtcdGateway(_http);
    }

    public async ValueTask InitializeAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        await _container.StartAsync(ct);
        Endpoint = $"http://localhost:{_container.GetMappedPublicPort(2379)}";
        await WaitReadyAsync(ct);
    }

    public async ValueTask DisposeAsync()
    {
        _http.Dispose();
        await _container.DisposeAsync();
    }

    // Свободный порт для фиксированной публикации: слушаем 0 → отдаём; docker
    // заберёт его при старте контейнера (окно гонки между release и bind ничтожно).
    public static int ReserveHostPort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint!).Port;
        listener.Stop();
        return port;
    }

    private async Task WaitReadyAsync(CancellationToken ct)
    {
        using var probeClient = new HttpClient { Timeout = TimeSpan.FromSeconds(3) };
        for (var i = 0; i < 30; i++)
        {
            try
            {
                using var probe = await probeClient.PostAsync(
                    Endpoint + "/v3/maintenance/status",
                    new StringContent("{}", Encoding.UTF8, "application/json"),
                    ct);
                if (probe.IsSuccessStatusCode)
                    return;
            }
            catch (HttpRequestException)
            {
                // etcd ещё поднимается — ждём следующую попытку
            }

            await Task.Delay(1000, ct);
        }

        throw new InvalidOperationException($"etcd в {Endpoint} не поднялся за 30 c");
    }
}

// Один etcd-контейнер на оба contract/coordination-класса (ключи не пересекаются).
[CollectionDefinition(Name)]
public sealed class EtcdCollection : ICollectionFixture<EtcdFixture>
{
    public const string Name = "etcd";
}
