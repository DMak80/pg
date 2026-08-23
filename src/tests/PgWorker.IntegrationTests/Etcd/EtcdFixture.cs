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
    private readonly IContainer _container = new ContainerBuilder("quay.io/coreos/etcd:v3.5.21")
        .WithCommand(
            "etcd",
            "--name=test",
            "--data-dir=/etcd-data",
            "--listen-client-urls=http://0.0.0.0:2379",
            "--advertise-client-urls=http://127.0.0.1:2379")
        .WithPortBinding(2379, assignRandomHostPort: true)
        .Build();

    private readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(10) };

    public string Endpoint { get; private set; } = "";

    public EtcdGateway Gateway { get; private set; } = null!;

    public async ValueTask InitializeAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        await _container.StartAsync(ct);
        Endpoint = $"http://localhost:{_container.GetMappedPublicPort(2379)}";
        Gateway = new EtcdGateway(_http);

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

    public async ValueTask DisposeAsync()
    {
        _http.Dispose();
        await _container.DisposeAsync();
    }
}

// Один etcd-контейнер на оба contract/coordination-класса (ключи не пересекаются).
[CollectionDefinition(Name)]
public sealed class EtcdCollection : ICollectionFixture<EtcdFixture>
{
    public const string Name = "etcd";
}
