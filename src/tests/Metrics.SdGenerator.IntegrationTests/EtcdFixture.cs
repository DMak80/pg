using System.Net;
using System.Net.Sockets;
using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Shared.Etcd.Client;

namespace Metrics.SdGenerator.IntegrationTests;

// Testcontainers-etcd (паттерн PgWorker.IntegrationTests/Etcd/EtcdFixture):
// generic-контейнер quay.io/coreos/etcd:v3.5.21, динамический порт публикации,
// готовность — POST-ретрай (встроенные HTTP-wait шлют GET, а /v3/* требует POST).
// АДАПТАЦИЯ t15: старт отложен в явный StartAsync (сценарий «мёртвый порт → ожил»:
// тест недоступности тикает на порт ДО старта контейнера).
public sealed class EtcdFixture : IAsyncDisposable
{
    private readonly IContainer _container;

    private readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(10) };

    public string Endpoint { get; private set; } = "";

    public EtcdGateway Gateway { get; }

    // hostPort (сценарий «мёртвый порт → ожил»): фиксированный порт публикации —
    // endpoint известен ДО старта контейнера. Динамический сценарий — StartAsync.
    public EtcdFixture(int? hostPort = null)
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

    // Старт контейнера + ожидание готовности (отложен из IAsyncLifetime — адаптация t15).
    public async Task StartAsync(CancellationToken ct)
    {
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

    // Готовность POST-ретраем (30×1 c); транзиентные транспортные сбои: клиентский
    // таймаут пробы (3 c) — «ещё не готов», отмена теста продолжает всплывать.
    private async Task WaitReadyAsync(CancellationToken ct)
    {
        using var probeClient = new HttpClient { Timeout = TimeSpan.FromSeconds(3) };
        var attempts = 0;
        for (var i = 0; i < 30; i++)
        {
            attempts++;
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
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                // Клиентский таймаут пробы при холодном старте контейнера.
            }

            await Task.Delay(1000, ct);
        }

        throw new InvalidOperationException(
            $"etcd в {Endpoint} не поднялся за 30 c (попыток {attempts})");
    }
}
