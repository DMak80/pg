using AdminPanel.Core;
using AdminPanel.Core.Kafka;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Workers;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace AdminPanel.IntegrationTests;

// Фабрика грани «Воркеры» (spec §3.3 п.4): РЕАЛЬНЫЙ etcd-контейнер (панель —
// прямой писатель ключей сертов) + TestSnapshotStore (статусы applied/pending)
// + стаб WorkerApi (рестарт) + стаб kafka-кредов (kafka-ветка правила 3 §4.3).
// Hosted сняты; build строго через PanelHostBuilder.
public sealed class WorkersWebFactory : WebApplicationFactory<Program>
{
    public string EtcdEndpoint { get; set; } = "";
    public TestWorkerApi WorkerApi { get; } = new();
    public FixedTimeProvider Time { get; } = new();
    public StubKafkaSecrets KafkaSecrets { get; } = new();

    private bool _built;
    public void EnsureBuilt()
    {
        if (_built) return;
        PanelHostBuilder.BuildExclusive(this);
        _built = true;
    }

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseSetting("AdminPanel:Auth:Username", "admin");
        builder.UseSetting("AdminPanel:Auth:Password", "adminpw");
        builder.UseSetting("AdminPanel:Auth:AllowHttp", "true");
        if (EtcdEndpoint.Length > 0)
            builder.UseSetting("AdminPanel:Etcd:Endpoints:0", EtcdEndpoint);
        builder.ConfigureTestServices(services =>
        {
            services.RemoveAll<IHostedService>();
            services.Replace(new ServiceDescriptor(typeof(TimeProvider), Time));
            services.Replace(new ServiceDescriptor(typeof(ISnapshotStore), new TestSnapshotStore()));
            services.Replace(new ServiceDescriptor(typeof(IKafkaSnapshotStore), new TestKafkaSnapshotStore()));
            services.Replace(new ServiceDescriptor(typeof(IWorkerApiGateway), WorkerApi));
            // Kafka-креды (правило 3 §4.3): подмена singleton'а ДО build — тот же
            // приём, что и гейтвей выше (прецедент AuthTests.cs:137-138).
            services.Replace(new ServiceDescriptor(typeof(IKafkaSecretsStore), KafkaSecrets));
        });
    }

    public EtcdSnapshot? Snapshot { set => ((TestSnapshotStore)Services.GetRequiredService<ISnapshotStore>()).Current = value; }
    public KafkaSnapshot? KafkaSnapshot { set => ((TestKafkaSnapshotStore)Services.GetRequiredService<IKafkaSnapshotStore>()).Current = value; }
}

// Стаб kafka-кредов интеграционных кейсов (зеркало StubSecrets из юнитов):
// тест кладёт ca_pem кластера → WorkerCertService видит его известным
// материалом исходящих (spec §4.3 правило 3, kafka-ветка).
public sealed class StubKafkaSecrets : IKafkaSecretsStore
{
    public IReadOnlyDictionary<string, KafkaClusterSecrets> Current { get; set; } =
        new Dictionary<string, KafkaClusterSecrets>();
    public void Replace(IReadOnlyDictionary<string, KafkaClusterSecrets> secrets) => Current = secrets;
}

// Мини-стор kafka-снапшота файла (аналог TestSnapshotStore, поле Current).
public sealed class TestKafkaSnapshotStore : IKafkaSnapshotStore
{
    public KafkaSnapshot? Current { get; set; }
    public void Replace(KafkaSnapshot snapshot) => Current = snapshot;
}

// Фикстура сценария «серты API воркеров»: свой etcd-контейнер + фабрика
// (EnsureBuilt ПОСЛЕ инициализации etcd) — teardown при любом исходе.
public sealed class WorkersCertFixture : IAsyncLifetime
{
    public EtcdContainerFixture Etcd { get; } = new();
    public WorkersWebFactory Factory { get; } = new();

    public async ValueTask InitializeAsync()
    {
        await Etcd.InitializeAsync();
        Factory.EtcdEndpoint = Etcd.Endpoint;
        // Часы панели = реальное время: TestPki выпускает серты от реального
        // «сейчас» (правила срока §4.3 считают от него). LoginAsync двигает
        // время на +61 c — изоляция окна rate-limiter'а сохраняется.
        Factory.Time.Utc = DateTimeOffset.UtcNow;
        Factory.EnsureBuilt();
    }

    public async ValueTask DisposeAsync()
    {
        await Factory.DisposeAsync();
        await Etcd.DisposeAsync();
    }
}

[CollectionDefinition("workers-cert")]
public sealed class WorkersCertCollection : ICollectionFixture<WorkersCertFixture>;
