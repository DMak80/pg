using System.Net;
using System.Net.Sockets;
using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Shared.Etcd.Client;
using Xunit;

namespace ValkeyWorker.IntegrationTests.Etcd;

// Testcontainers-etcd для etcd-only тестов ValkeyWorker (порт EtcdFixture
// KafkaWorker.IntegrationTests): generic-контейнер quay.io/coreos/etcd:v3.5.21,
// Gateway /v3/* включён в 3.5 по умолчанию. Готовность — свой POST-ретрай
// (встроенные HTTP-wait шлют GET, /v3/* требует POST). Порт публикации —
// динамический (AGENTS.md).
public sealed class ValkeyEtcdFixture : IAsyncLifetime
{
    private readonly IContainer _container;

    private readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(10) };

    public string Endpoint { get; private set; } = "";

    public EtcdGateway Gateway { get; }

    public ValkeyEtcdFixture()
    {
        var builder = new ContainerBuilder("quay.io/coreos/etcd:v3.5.21")
            .WithCommand(
                "etcd",
                "--name=test",
                "--data-dir=/etcd-data",
                "--listen-client-urls=http://0.0.0.0:2379",
                "--advertise-client-urls=http://127.0.0.1:2379");
        _container = builder
            .WithPortBinding(2379, assignRandomHostPort: true)
            .Build();
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

    // Put-хелпер etcd-only тестов: ключ → значение.
    public Task PutAsync(string key, string value)
        => Gateway.PutAsync(Endpoint, key, value, null, TestContext.Current.CancellationToken);

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

// Один etcd-контейнер на etcd-only тест-классы сборки (порт EtcdCollection
// KafkaWorker): контейнер поднимается один, dispose фикстуры — зачистка серии.
[CollectionDefinition(Name)]
public sealed class ValkeyEtcdCollection : ICollectionFixture<ValkeyEtcdFixture>
{
    public const string Name = "valkey-etcd";
}
