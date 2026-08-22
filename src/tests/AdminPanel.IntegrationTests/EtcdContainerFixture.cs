using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Xunit;

namespace AdminPanel.IntegrationTests;

// Testcontainers-etcd: generic-контейнер quay.io/coreos/etcd:v3.5.21 (spec §11.1; готовый
// .NET-модуль etcd на NuGet отсутствует). Gateway /v3/* включён в 3.5 по умолчанию.
// Готовность — свой POST-ретрай: встроенные HTTP-wait шлют GET, а /v3/* требует POST.
public sealed class EtcdContainerFixture : IAsyncLifetime
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

    public string Endpoint { get; private set; } = "";

    public async ValueTask InitializeAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        await _container.StartAsync(ct);
        Endpoint = $"http://localhost:{_container.GetMappedPublicPort(2379)}";

        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(3) };
        for (var i = 0; i < 30; i++)
        {
            try
            {
                using var probe = await http.PostAsync(
                    Endpoint + "/v3/maintenance/status",
                    new StringContent("{}", Encoding.UTF8, "application/json"),
                    ct);
                if (probe.IsSuccessStatusCode)
                {
                    await EtcdSeed.SeedAsync(Endpoint, ct);
                    return;
                }
            }
            catch (HttpRequestException)
            {
                // etcd ещё поднимается — ждём следующую попытку
            }

            await Task.Delay(1000, ct);
        }

        throw new InvalidOperationException($"etcd в {Endpoint} не поднялся за 30 c");
    }

    public async ValueTask DisposeAsync() => await _container.DisposeAsync();

    // Тест отказа etcd: тик должен сохранить прежний снапшот (spec §11.2).
    public async Task StopAsync() => await _container.StopAsync();
}
