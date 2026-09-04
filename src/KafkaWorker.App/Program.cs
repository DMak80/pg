using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using KafkaWorker.App;
using Shared.Metrics;
using KafkaWorker.App.Api;
using KafkaWorker.App.Api.Operations;
using KafkaWorker.App.HealthChecks;
using KafkaWorker.App.Loops;
using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Core.Templates;
using KafkaWorker.Docker.Drivers;
using KafkaWorker.Docker.Engine;
using KafkaWorker.Etcd;
using KafkaWorker.Etcd.Client;
using KafkaWorker.Etcd.Parsing;
using KafkaWorker.Etcd.Coordination;
using KafkaWorker.Provisioning.Kafka;
using KafkaWorker.Provisioning.Processes;

// Точка входа KafkaWorker (arch/16 §8): host-builder с mTLS-гранью HTTP API
// (вкл. /healthz), конфигурация appsettings+env, DI всех слоёв (etcd →
// координация → docker → процессы → циклы). Per-install env-секреты — только
// TLS HTTP API (arch/16 §4); per-cluster секреты (app/admin/CA) — в etcd.
// Fail-fast: пустые Etcd:Endpoints/Hosts, не-https AdvertiseUrl.

var builder = WebApplication.CreateBuilder(args);

// Конфигурация: appsettings.json + env-оверрайды KafkaWorker__*.
builder.Services.Configure<KafkaWorkerOptions>(builder.Configuration.GetSection("KafkaWorker"));
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<HealthState>();

// Метрики (arch/18 §3): /metrics на том же Kestrel-порту, что /healthz;
// ApiKeyMiddleware защищает только /api — scrape-грань открыта (доверенная сеть).
builder.Services.AddAppMetrics("KafkaWorker", builder.Configuration.GetSection("KafkaWorker:Metrics"));
builder.Services.AddSingleton(sp =>
{
    var m = new Shared.Metrics.Worker.WorkerMetricsInstrumentation(
        sp.GetRequiredService<System.Diagnostics.Metrics.Meter>(),
        sp.GetRequiredService<TimeProvider>());
    // Единый seam фаз/операций (S2, зеркало PgWorker): терминальные фазы/
    // first-seen/подавление supervise и evacuate — внутри OnJournalPhase.
    sp.GetRequiredService<KafkaWorker.Etcd.Coordination.WorkJournal>().PhaseWritten
        += e => m.OnJournalPhase(e.Cluster, e.Op, e.Phase);
    return m;
});

// etcd-клиент: HTTP JSON gateway /v3/*; handler против DNS-флейпа Docker
// embedded DNS (t09; arch/16 §7): PooledConnectionLifetime + IPv4-first резолв.
// EtcdGateway-синглтон захвачен HttpClient навсегда — ротация handler'ов фабрики
// на него не действует, поэтому явный SocketsHttpHandler.
builder.Services.AddHttpClient("etcd")
    .ConfigurePrimaryHttpMessageHandler(EtcdConnectCallback.CreateHandler);

// Fail-fast при старте: без etcd-endpoints воркер бессмысленен (hosts — в DI-фабрике драйвера);
// ключ доступа /kafkaworker/api/<id> без URL бессмысленен (arch/16 §1.1).
builder.Services.AddOptions<KafkaWorkerOptions>()
    .Validate(o => o.Etcd.Endpoints is { Length: > 0 }, "KafkaWorker:Etcd:Endpoints не заданы")
    .Validate(o => !string.IsNullOrWhiteSpace(o.Api.AdvertiseUrl),
        "KafkaWorker:Api:AdvertiseUrl не задан (env KFW_API_ADVERTISE_URL)")
    .Validate(o => o.Api.Tls.AllowInsecureHttp
        || o.Api.AdvertiseUrl.StartsWith("https://", StringComparison.Ordinal),
        "AdvertiseUrl обязан быть https:// (mTLS-only API, arch/16 §1.1)")
    .ValidateOnStart();

// mTLS HTTP API (arch/16 §1.1, t03): env-секреты TLS → конфиг, Kestrel c
// серверным сертом и требованием клиентского серта per-install API-CA.
TlsEndpoints.ApplyEnvOverrides(builder.Configuration);
TlsEndpoints.ConfigureMtls(builder, port: 8080);

// etcd-клиент (HTTP JSON gateway /v3/*) + координация (клэймы/лидерство, журнал).
builder.Services.AddSingleton<IEtcdGateway>(sp =>
    new EtcdGateway(sp.GetRequiredService<IHttpClientFactory>().CreateClient("etcd")));
builder.Services.AddSingleton(sp => new ClaimStore(
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Api.AdvertiseUrl));
// t91: глобальный portalloc-клэйм (arch/15 §4 / arch/16 §2.1) — DI-синглтон,
// InstanceId единый с ClaimStore (сквозная диагностика держателя).
builder.Services.AddSingleton(sp => new PortAllocLock(
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<ClaimStore>().InstanceId));
builder.Services.AddSingleton(sp => new WorkJournal(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints));

// HTTP API воркера (arch/16 §1.1): мутации декларативного контракта kafka-домена —
// хендлеры-синглтоны (task etcd-via-worker-api).
builder.Services.AddSingleton(sp => new CreateClusterHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<TimeProvider>()));
builder.Services.AddSingleton(sp => new DeleteClusterHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints));
builder.Services.AddSingleton(sp => new UpdateConfigHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints));
builder.Services.AddSingleton(sp => new UpdateBrokerResourcesHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints));
builder.Services.AddSingleton(sp => new AddBrokerHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints));
builder.Services.AddSingleton(sp => new DeleteBrokerHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints));
builder.Services.AddSingleton(sp => new RotateAppPasswordHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<TimeProvider>()));
builder.Services.AddSingleton(sp => new RotateAdminPasswordHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<TimeProvider>()));
builder.Services.AddSingleton(sp => new RebalanceHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<TimeProvider>()));

// Топиковые мутации (arch/02 §10.2-6,7,9..12; task etcd-via-worker-api).
builder.Services.AddSingleton(sp => new UpdateTopicDesiredHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<TimeProvider>()));
builder.Services.AddSingleton(sp => new DeleteDesiredHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints));
builder.Services.AddSingleton(sp => new CreateTopicHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<TimeProvider>()));
builder.Services.AddSingleton(sp => new DeleteTopicHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<TimeProvider>()));
builder.Services.AddSingleton(sp => new CancelLifecycleHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints));

// Демо-сид kafka-домена (arch/16 §1.1.1; task etcd-via-worker-api).
builder.Services.AddSingleton(sp => new SeedDemoHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Api.EnableSeedEndpoint));

// docker: драйвер по режиму (Plain: таблица Hosts; Swarm: manager endpoint).
builder.Services.AddSingleton<DockerEngineFactory>();
builder.Services.AddSingleton<IClusterDriver>(sp =>
{
    var docker = sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Docker;
    var factory = sp.GetRequiredService<DockerEngineFactory>();
    if (string.Equals(docker.Mode, "Swarm", StringComparison.OrdinalIgnoreCase))
    {
        if (string.IsNullOrWhiteSpace(docker.SwarmManager))
            throw new ApplicationException("KafkaWorker:Docker:Mode=Swarm требует KafkaWorker:Docker:SwarmManager");
        return new SwarmClusterDriver(docker.SwarmManager, factory);
    }

    var hosts = docker.Hosts
        .Select(h => new HostEndpoint(h.Name, h.Endpoint))
        .ToList();
    if (hosts.Count == 0)
        throw new ApplicationException("KafkaWorker:Docker:Mode=Plain требует непустую таблицу KafkaWorker:Docker:Hosts");
    return new PlainClusterDriver(hosts, factory);
});

// Снапшоты P12 (SnapshotLoop-лидер + процессы в точках изменений «до/после»).
builder.Services.AddSingleton(sp =>
{
    var opts = sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value;
    return new SnapshotJob(
        sp.GetRequiredService<IEtcdGateway>(), opts.Etcd.Endpoints,
        opts.Snapshots.Dir, opts.Snapshots.RetentionFiles, opts.Snapshots.MaintenanceIntervalMin);
});

// Kafka AdminClient (seam + Confluent-адаптер): RequestTimeout короткий —
// это пробы/конфиги, длинные ожидания — циклы процессов. Фабрика — sharable
// кэш (t05): клиент per (bootstrap,user,password), не «клиент на тик».
builder.Services.AddSingleton<IKafkaAdminClientFactory>(sp =>
    new KafkaAdminClientFactory(
        TimeSpan.FromSeconds(10),
        sp.GetRequiredService<ILogger<KafkaAdminClientFactory>>(),
        TimeProvider.System));

// Backoff недоступного кластера (t05): DI-синглтон; писатели — supervise-проба
// и коллектор (первые kafka-контакты), читатели — гейты Active-ветки/коллектора.
builder.Services.AddSingleton(sp =>
    new KafkaClusterBackoff(sp.GetRequiredService<TimeProvider>()));

// Кеш серверных сертов нод (R3, arch/16 §2.3): DI-синглтом.
builder.Services.AddSingleton(new BrokerCertificateCache());

// Ensure per-cluster секретов: CA + креды admin/app (arch/16 §4, t03).
builder.Services.AddSingleton<IClusterSecretEnsurer>(sp => new ClusterSecretEnsurer(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints));

// Converge dynamic broker configs (arch/16 §5 E).
builder.Services.AddSingleton<IClusterConfigConverger, ClusterConfigConverger>();

// Процессы-машины состояний (arch/16 §5): снапшот-делегат P12 «до/после».
builder.Services.AddSingleton(sp =>
{
    var opts = sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value;
    return new ProvisioningProcess(
        sp.GetRequiredService<IEtcdGateway>(), opts.Etcd.Endpoints,
        sp.GetRequiredService<IClusterDriver>(),
        sp.GetRequiredService<ClaimStore>(),
        sp.GetRequiredService<WorkJournal>(),
        sp.GetRequiredService<PortAllocLock>(),
        sp.GetRequiredService<PortAllocIndex>(),
        sp.GetRequiredService<IClusterSecretEnsurer>(),
        sp.GetRequiredService<IKafkaAdminClientFactory>(),
        sp.GetRequiredService<IClusterConfigConverger>(),
        ToProvisioningOptions(opts),
        sp.GetRequiredService<BrokerCertificateCache>(),
        SnapshotDelegate(sp.GetRequiredService<SnapshotJob>()));
});
// t91: индекс занятости portalloc чужих кластеров (arch/16 §2.1) — DI-синглтон.
builder.Services.AddSingleton(sp => new PortAllocIndex(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<ILogger<PortAllocIndex>>()));
builder.Services.AddSingleton(sp => new DeprovisioningProcess(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IClusterDriver>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    SnapshotDelegate(sp.GetRequiredService<SnapshotJob>())));
// Лестница E9 самолечения portalloc (t05, arch/17): supervise вызывает её для
// безадресных брокеров ДО любых деструктивных действий.
builder.Services.AddSingleton(sp => new PortAllocHealer(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IClusterDriver>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<PortAllocLock>(),
    sp.GetRequiredService<PortAllocIndex>(),
    ToProvisioningOptions(sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value),
    sp.GetRequiredService<BrokerCertificateCache>()));
builder.Services.AddSingleton(sp => new NodeSupervisor(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IClusterDriver>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<IKafkaAdminClientFactory>(),
    ToProvisioningOptions(sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value),
    sp.GetRequiredService<BrokerCertificateCache>(),
    backoff: sp.GetRequiredService<KafkaClusterBackoff>(),
    healer: sp.GetRequiredService<PortAllocHealer>()));

// Scale-проход и ротация (arch/16 §5 F/G/H): ротатор — со снапшот-делегатом P12.
// Reassignment (I, t02) — перед G: drain TO_REMOVE-брокеров + заявки balance.
builder.Services.AddSingleton(sp =>
{
    var opts = sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value;
    return new PartitionReassignerProcess(
        sp.GetRequiredService<IEtcdGateway>(),
        opts.Etcd.Endpoints,
        sp.GetRequiredService<IClusterDriver>(),
        sp.GetRequiredService<ClaimStore>(),
        sp.GetRequiredService<WorkJournal>(),
        sp.GetRequiredService<IKafkaAdminClientFactory>(),
        new ReassignOptions(
            opts.Loops.ReassignIntervalSec, opts.Loops.ReassignBatchPartitions,
            opts.Thresholds.ReassignExecSec, opts.Thresholds.ReassignRetrySubmitSec),
        sp.GetRequiredService<TimeProvider>());
});
builder.Services.AddSingleton(sp => new RemoveBrokerProcess(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IClusterDriver>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<IKafkaAdminClientFactory>(),
    ToProvisioningOptions(sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value)));
builder.Services.AddSingleton(sp => new AddBrokerProcess(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IClusterDriver>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<PortAllocLock>(),
    sp.GetRequiredService<PortAllocIndex>(),
    sp.GetRequiredService<IKafkaAdminClientFactory>(),
    ToProvisioningOptions(sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value),
    sp.GetRequiredService<BrokerCertificateCache>()));
builder.Services.AddSingleton(sp => new PasswordRotator(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IClusterDriver>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<IKafkaAdminClientFactory>(),
    ToProvisioningOptions(sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value),
    sp.GetRequiredService<BrokerCertificateCache>(),
    SnapshotDelegate(sp.GetRequiredService<SnapshotJob>())));
builder.Services.AddSingleton(sp => new NodeRegenerator(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IClusterDriver>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    ToProvisioningOptions(sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value),
    sp.GetRequiredService<BrokerCertificateCache>()));

// Автосинк топиков (arch/16 §5 D): троттлинг TopicSyncIntervalSec внутри.
builder.Services.AddSingleton(sp => new TopicSyncProcess(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<IKafkaAdminClientFactory>(),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Loops.TopicSyncIntervalSec));

// Converge-миграция премиграционных кластеров в канон t03 (arch/16 §5 M).
builder.Services.AddSingleton(sp => new SecurityMigrator(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IClusterDriver>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<IClusterSecretEnsurer>(),
    sp.GetRequiredService<IKafkaAdminClientFactory>(),
    sp.GetRequiredService<IClusterConfigConverger>(),
    ToProvisioningOptions(sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value),
    sp.GetRequiredService<BrokerCertificateCache>()));

// Циклы: keepalive первым (lease живут до Reconcile), затем снапшоты и reconcile.
builder.Services.AddSingleton<IKafkaClusterProcesses, KafkaClusterProcesses>();
builder.Services.AddSingleton<KeepaliveLoop>();
builder.Services.AddSingleton<SnapshotLoop>();
builder.Services.AddSingleton<ReconcileLoop>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<KeepaliveLoop>());
builder.Services.AddHostedService(sp => sp.GetRequiredService<SnapshotLoop>());
builder.Services.AddHostedService(sp => sp.GetRequiredService<ReconcileLoop>());

// Коллектор лагов/USR (arch/18 §4): read-only сбор вне клэймов; источник кластеров —
// тот же снапшот /kafka/clusters/, что у ReconcileLoop (парсер KafkaSnapshotParser);
// только Active (Config.State == null — arch/15 §2.1, ревью Ф4-6).
builder.Services.AddSingleton(sp => new KafkaMetricsState(
    sp.GetRequiredService<System.Diagnostics.Metrics.Meter>()));
builder.Services.AddHostedService(sp => new KafkaMetricsCollector(
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Metrics.CollectIntervalSec,
    ct => SnapshotClustersAsync(sp, ct),
    sp.GetRequiredService<IKafkaAdminClientFactory>(),
    sp.GetRequiredService<KafkaMetricsState>(),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<ILogger<KafkaMetricsCollector>>(),
    sp.GetRequiredService<KafkaClusterBackoff>()));

// Наблюдаемость (arch/16 §7): агрегированный health + per-loop обёртки.
builder.Services.AddSingleton<ServiceProbes>();
builder.Services.AddSingleton<KafkaWorkerHealth>();
builder.Services.AddSingleton<HealthCheckAbstract<ReconcileLoop>>(
    sp => new(sp.GetRequiredService<ReconcileLoop>()));
builder.Services.AddSingleton<HealthCheckAbstract<KeepaliveLoop>>(
    sp => new(sp.GetRequiredService<KeepaliveLoop>()));
builder.Services.AddSingleton<HealthCheckAbstract<SnapshotLoop>>(
    sp => new(sp.GetRequiredService<SnapshotLoop>()));
builder.Services.AddHealthChecks()
    .AddCheck<KafkaWorkerHealth>("kafkaworker", tags: ["ready"])
    .AddCheck<HealthCheckAbstract<ReconcileLoop>>("reconcile-loop")
    .AddCheck<HealthCheckAbstract<KeepaliveLoop>>("keepalive-loop")
    .AddCheck<HealthCheckAbstract<SnapshotLoop>>("snapshot-loop");

var app = builder.Build();
if (app.Services.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Api.Tls.AllowInsecureHttp)
    app.Logger.LogWarning(
        "KafkaWorker:Api:Tls:AllowInsecureHttp=true — HTTP без TLS (ТОЛЬКО WAF-тесты, arch/16 §1.1)");
app.MapAppMetrics();
app.MapHealthChecks("/healthz");
app.MapWorkerApi();

await app.RunAsync();

// Опции процессов из дерева конфигурации (arch/16 §8).
static ProvisioningOptions ToProvisioningOptions(KafkaWorkerOptions opts) => new(
    opts.Docker.PortRange.From,
    opts.Docker.PortRange.To,
    opts.Thresholds.BrokerBootSec,
    opts.Thresholds.NodeDeadSec,
    opts.AdvertisedClientHost,
    opts.Docker.Images.Node);

// Делегат снапшота для процессов (P12 «до/после» в точках изменений).
static Func<CancellationToken, Task<Result>> SnapshotDelegate(SnapshotJob job)
    => async ct => await job.TakeAsync(ct);

// Источник кластеров для коллектора метрик (arch/18 §4): RangeAsync по
// /kafka/clusters/ c failover по endpoints (паттерн ReconcileLoop воркера)
// → KafkaSnapshotParser; ошибки чтения → Result.Failed (коллектор пропустит тик).
static async Task<Result<IReadOnlyList<KafkaClusterSnapshot>>> SnapshotClustersAsync(
    IServiceProvider sp, CancellationToken ct)
{
    var gateway = sp.GetRequiredService<IEtcdGateway>();
    var endpoints = sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints;
    Result<IReadOnlyList<Kv>>? last = null;
    foreach (var endpoint in endpoints)
    {
        var range = await gateway.RangeAsync(endpoint, "/kafka/clusters/", ct);
        if (!range.IsSuccess)
        {
            last = range;
            continue;
        }

        return KafkaSnapshotParser.Parse(range.Value);
    }

    return Result<IReadOnlyList<KafkaClusterSnapshot>>.Failed(last!.Error!);
}

// WAF-тесты (KafkaWorker.IntegrationTests/Api): точка входа как public partial.
public partial class Program;
