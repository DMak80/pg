using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Xunit;

namespace EtcdWorker.IntegrationTests.Docker;

// Окружение EtcdWorker-интеграций (порт S3EtcdFixture/S3ClusterFixture):
// testcontainers-сеть etcdw-<guid>-net + единственный seed-узел etcd 3.5
// (initial-cluster-state=new). Клиентская грань — динамический host-порт
// (AGENTS.md); peer-грань — имена в сети контура, host-публикаций нет
// (arch/25 §2). Готовность — POST-ретрай (встроенные HTTP-wait шлют GET,
// /v3/* требует POST). Полный teardown при любом исходе: свои ключи →
// контейнер → сеть; успех DisposeAsync сети/контейнера — ассерт чистоты
// (удаление несуществующих объектов testcontainers падает).
public sealed class EtcdWorkerEnvironment : IAsyncLifetime
{
    // Уникальный тег прогона: имя сети уникально на каждый запуск.
    public string RunTag { get; } = Guid.NewGuid().ToString("N")[..8];

    public string NetworkName => $"etcdw-{RunTag}-net";

    private readonly INetwork _network;

    private readonly IContainer _etcd;

    private readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(10) };

    // Уникальный префикс сценария в etcd (изоляция ключей серий).
    public string TestPrefix { get; } = "/etcdworker-it-" + Guid.NewGuid().ToString("N")[..8];

    public string Endpoint { get; private set; } = "";

    public EtcdGateway Gateway { get; }

    public EtcdWorkerEnvironment()
    {
        _network = new NetworkBuilder()
            .WithName(NetworkName)
            .Build();
        _etcd = new ContainerBuilder("quay.io/coreos/etcd:v3.5.21")
            .WithNetwork(_network)
            .WithNetworkAliases("seed")
            .WithCommand(
                "etcd",
                "--name=seed",
                "--data-dir=/etcd-data",
                "--listen-client-urls=http://0.0.0.0:2379",
                "--advertise-client-urls=http://localhost:2379",
                "--listen-peer-urls=http://0.0.0.0:2380",
                "--initial-advertise-peer-urls=http://127.0.0.1:2380",
                "--initial-cluster=seed=http://127.0.0.1:2380",
                "--initial-cluster-state=new")
            .WithPortBinding(2379, assignRandomHostPort: true)
            .Build();
        Gateway = new EtcdGateway(_http);
    }

    public async ValueTask InitializeAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        await _network.CreateAsync(ct);
        await _etcd.StartAsync(ct);
        Endpoint = $"http://localhost:{_etcd.GetMappedPublicPort(2379)}";
        await WaitReadyAsync(ct);
    }

    public async ValueTask DisposeAsync()
    {
        // Teardown при любом исходе: свои ключи → контейнер → сеть
        // (docs/e2e-isolation.md; порядок — лучшее усилие, падение dispose
        // сети/контейнера валит тест — это ассерт чистоты окружения).
        if (Endpoint.Length > 0)
            await Gateway.DeleteAsync(Endpoint, TestPrefix, prefix: true, CancellationToken.None);
        _http.Dispose();
        await _etcd.DisposeAsync();
        await _network.DeleteAsync();
    }

    // Put-хелпер интеграций: ключ → значение (свой префикс сценария).
    public Task PutAsync(string key, string value)
        => Gateway.PutAsync(Endpoint, TestPrefix + key, value, null, TestContext.Current.CancellationToken);

    // Get-хелпер интеграций: ключ префикса сценария → Kv?.
    public Task<Result<Kv?>> GetAsync(string key)
        => Gateway.GetAsync(Endpoint, TestPrefix + key, TestContext.Current.CancellationToken);

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

        throw new InvalidOperationException($"etcd at {Endpoint} did not come up within 30 s");
    }
}

// Одно окружение на EtcdWorker-интеграции сборки: сеть+seed поднимаются один,
// dispose фикстуры — полная зачистка серии. Parallelization ВЫКЛЮЧЕНА: состав
// кластера — глобальное состояние, etcd 3.5 допускает ровно ОДИН unstarted
// learner (сверка шага 2 t32: второй add → "too many learner members").
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class EtcdWorkerCollection : ICollectionFixture<EtcdWorkerEnvironment>
{
    public const string Name = "etcdworker-env";
}
