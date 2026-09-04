using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace KafkaWorker.IntegrationTests.Api;

// WAF-хост KafkaWorker с настоящим etcd (fixture) и выключенными фоновыми
// циклами (task etcd-via-worker-api): loops не нужны для API-мутаций, а их
// тики в тесте — шум. Env-секретов per-install у KafkaWorker нет (arch/16 §8) —
// фабрика проще зеркала PgWorkerApiFactory.
// Не sealed: кейсы с оверрайдом конфигурации наследуются (ApiKey-кейс).
public class KafkaApiFactory(Etcd.EtcdFixture etcd) : WebApplicationFactory<Program>
{
    // AllowInsecureHttp — env-ом процесса: WAF-конфиг применяется при Build(),
    // а mTLS-конфигурация Kestrel исполняется раньше (TlsEndpoints.ConfigureMtls,
    // до builder.Build() — этап хоста). mTLS — MtlsApiTests на реальном сокете.
    static KafkaApiFactory()
        => Environment.SetEnvironmentVariable("KafkaWorker__Api__Tls__AllowInsecureHttp", "true");

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Development");
        builder.ConfigureAppConfiguration((_, cfg) => cfg.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["KafkaWorker:Etcd:Endpoints:0"] = etcd.Endpoint,
            ["KafkaWorker:Docker:Hosts:0:Name"] = "local",
            ["KafkaWorker:Docker:Hosts:0:Endpoint"] = "unix:///var/run/does-not-exist.sock",
            ["KafkaWorker:Api:AdvertiseUrl"] = "https://localhost:9998",
            // mTLS — MtlsApiTests на реальном сокете; WAF-фабрика — insecure HTTP.
            ["KafkaWorker:Api:Tls:AllowInsecureHttp"] = "true",
            // Seed-эндпоинт включён для кейсов наливки (Task 10); выключенный
            // флаг проверяется отдельной фабрикой-оверрайдом.
            ["KafkaWorker:Api:EnableSeedEndpoint"] = "true",
        }));
        builder.ConfigureServices(services =>
            services.RemoveAll<IHostedService>()); // Keepalive/Snapshot/Reconcile не стартуют
    }
}

// Collection-fixture API-тестов: один etcd + одна WAF-фабрика на все Api-классы
// (Task 8/9/10).
public sealed class KafkaApiFixture : IAsyncLifetime
{
    public Etcd.EtcdFixture Etcd { get; } = new();

    public KafkaApiFactory Factory { get; }

    public KafkaApiFixture()
    {
        Factory = new KafkaApiFactory(Etcd);
    }

    public async ValueTask InitializeAsync() => await Etcd.InitializeAsync();

    public async ValueTask DisposeAsync()
    {
        await Factory.DisposeAsync();
        await Etcd.DisposeAsync();
    }
}

[CollectionDefinition(Name)]
public sealed class KafkaApiCollection : ICollectionFixture<KafkaApiFixture>
{
    public const string Name = "kafka-api";
}
