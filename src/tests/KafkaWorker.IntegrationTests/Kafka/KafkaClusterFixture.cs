using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using KafkaWorker.Core.Model;
using KafkaWorker.Docker.Drivers;
using KafkaWorker.Docker.Engine;
using KafkaWorker.Etcd.Client;
using KafkaWorker.Etcd.Coordination;
using KafkaWorker.Etcd.Parsing;
using KafkaWorker.Provisioning.Kafka;
using KafkaWorker.Provisioning.Processes;
using Xunit;

namespace KafkaWorker.IntegrationTests.Kafka;

// Фикстура волны A (арх-план A14): Testcontainers-etcd + ЛОКАЛЬНЫЙ docker-хост
// (сокет хоста — воркер в тесте хост-процессом управляет docker, как
// PgWorker.IntegrationTests). AdvertisedClientHost = host.docker.internal
// (endpoints резолвимы из контейнеров; хост-процесс теста подключается
// через localhost-маппинг — симметрия HostMap панели). Фикстура переиспользуется
// TopicSync-тестами волны C (C1).
public sealed class KafkaClusterFixture : IAsyncLifetime
{
    private readonly IContainer _etcd = new ContainerBuilder("quay.io/coreos/etcd:v3.5.21")
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

    public IClusterDriver Driver { get; private set; } = null!;

    public IKafkaAdminClientFactory AdminFactory { get; } =
        new KafkaAdminClientFactory(TimeSpan.FromSeconds(10));

    // Advertised-хост CLIENT-listener для ХОСТ-процесса теста: localhost
    // (host.docker.internal с macOS-хоста не резолвится; воркер и клиенты
    // теста — хост-процессы, порт опубликован на localhost). Контейнерный
    // стенд волн B/C использует host.docker.internal (compose B8).
    public const string AdvertisedClientHost = "localhost";

    // Окно хост-портов публикации брокеров — динамическое (AGENTS.md «порты
    // docker-контейнеров в тестах — динамические»): свободное окно зондится на
    // рантайме ВНЕ порт-зоны dev-станда (стендовые kfw-брокеры 16xxx) — тесты
    // не конфликтуют с поднятым стендом и параллельными прогонами.
    private static readonly (int From, int To) HostPorts = FreePortWindow.Find();

    // Фактически выбранное окно — для ассертов тестов (литералов «:16000» нет).
    public int PortFrom => HostPorts.From;

    public int PortTo => HostPorts.To;

    // Уникальный тег прогона: имена кластеров (и контейнеров kfw-<C>-broker<N>)
    // уникальны на каждый запуск — осиротевшие контейнеры прошлых прогонов (упавший
    // teardown) не пересекаются с новым по имени; креды нового etcd фикстуры не
    // конфликтуют со старыми (SASL «Invalid username or password» у живого старого
    // брокера с паролем прошлого прогона).
    public string RunTag { get; } = Guid.NewGuid().ToString("N")[..8];

    // Кластеры, созданные тестами этого прогона (регистрирует Cluster) —
    // DisposeAsync демонтирует их целиком: per-cluster сети не копятся,
    // пул subnet'ов docker-хоста конечен (t09-фикс).
    private readonly List<string> _clusters = [];

    // Имя кластера с тегом прогона: fixture.Cluster("re1") → "re1a1b2c3d4".
    public string Cluster(string name)
    {
        var cluster = $"{name}{RunTag}";
        _clusters.Add(cluster);
        return cluster;
    }

    // BrokerBootSec <= 100 (AGENTS.md): тестовый бюджет загрузки брокеров короткий —
    // зависание кластера фейлится быстро, а не мучается минутами.
    public ProvisioningOptions Options { get; } =
        new(HostPorts.From, HostPorts.To, BrokerBootSec: 100, NodeDeadSec: 90, AdvertisedClientHost, "apache/kafka:4.0.0");

    public async ValueTask InitializeAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        await _etcd.StartAsync(ct);
        Endpoint = $"http://localhost:{_etcd.GetMappedPublicPort(2379)}";
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
                    break;
            }
            catch (HttpRequestException)
            {
                // etcd ещё поднимается — ждём следующую попытку
            }

            await Task.Delay(1000, ct);
        }

        // docker-хост — локальный сокет хост-метода (Docker required).
        Driver = new PlainClusterDriver(
            [new HostEndpoint("local", "unix:///var/run/docker.sock")],
            new DockerEngineFactory());
    }

    public async ValueTask DisposeAsync()
    {
        // Демонтаж кластеров прогона (вкл. тесты без собственного teardown):
        // контейнеры/тома — силами драйвера, per-cluster сеть удалит сам драйвер
        // после последнего брокера; 404/ошибки — лучшее усилие (прогон завершён).
        var ct = CancellationToken.None;
        foreach (var cluster in _clusters)
        {
            try
            {
                var objects = await Driver.ListNodeObjectsAsync(cluster, ct);
                if (!objects.IsSuccess)
                    continue;
                foreach (var name in objects.Value)
                {
                    // kfw-<C>-<broker> → имя узла после префикса кластера.
                    var node = name[($"kfw-{cluster}-").Length..];
                    await Driver.RemoveNodeAsync(cluster, node, removeVolume: true, ct);
                }
            }
            catch
            {
                // чистка на выходе — ошибки не всплывают (прогон уже завершён)
            }
        }

        _http.Dispose();
        await _etcd.DisposeAsync();
    }

    // Сид заявки кластера (config NOT_INITIALIZED + brokers/state+resources).
    public async Task SeedClusterAsync(string cluster, int brokers)
    {
        var ct = TestContext.Current.CancellationToken;
        await Gateway.PutAsync(Endpoint, $"/kafka/clusters/{cluster}/config",
            $$"""{"brokers":{{brokers}},"replication_factor":{{Math.Min(brokers, 3)}},"min_insync_replicas":{{Math.Min(brokers, 2)}},"default_partitions":3,"default_retention_ms":604800000,"created_unix":1756500000,"state":"NOT_INITIALIZED"}""",
            lease: null, ct);
        for (var k = 1; k <= brokers; k++)
        {
            await Gateway.PutAsync(Endpoint, $"/kafka/clusters/{cluster}/brokers/broker{k}/state",
                "NOT_INITIALIZED", lease: null, ct);
            await Gateway.PutAsync(Endpoint, $"/kafka/clusters/{cluster}/brokers/broker{k}/resources",
                """{"cpu":"1","mem":"1Gi","disk":"10Gi"}""", lease: null, ct);
        }
    }

    // Снапшот кластера из etcd (как ReconcileLoop).
    public async Task<KafkaClusterSnapshot?> SnapshotAsync(string cluster)
    {
        var range = await Gateway.RangeAsync(Endpoint, "/kafka/clusters/", TestContext.Current.CancellationToken);
        return KafkaSnapshotParser.Parse(range.Value).Value.FirstOrDefault(c => c.Cluster == cluster);
    }

    public async Task<string?> GetAsync(string key)
    {
        var kv = await Gateway.GetAsync(Endpoint, key, TestContext.Current.CancellationToken);
        return kv.Value?.Value;
    }

    // Дискавери-клиент (spec §9.2): bootstrap/endpoints и креды — ТОЛЬКО из ключей etcd.
    public async Task<AdminClientBuilder> DiscoveryAdminBuilderAsync(string cluster)
    {
        var endpoints = await GetAsync($"/kafka/clusters/{cluster}/endpoints");
        var user = await GetAsync($"/kafka/clusters/{cluster}/app_user");
        var password = await GetAsync($"/kafka/clusters/{cluster}/app_password");
        return new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = endpoints!.Replace("host.docker.internal", "localhost", StringComparison.Ordinal),
            SecurityProtocol = SecurityProtocol.SaslPlaintext,
            SaslMechanism = SaslMechanism.Plain,
            SaslUsername = user,
            SaslPassword = password,
        });
    }
}

// Один etcd + docker-хост на все kafka-интеграционные классы (кластеры изолированы именами).
[CollectionDefinition(Name)]
public sealed class KafkaCollection : ICollectionFixture<KafkaClusterFixture>
{
    public const string Name = "kafka-e2e";
}
