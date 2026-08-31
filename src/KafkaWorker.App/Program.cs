using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Options;
using KafkaWorker.App;
using KafkaWorker.App.Api;
using KafkaWorker.App.Api.Operations;
using KafkaWorker.App.HealthChecks;
using KafkaWorker.App.Loops;
using KafkaWorker.Core;
using KafkaWorker.Docker.Drivers;
using KafkaWorker.Docker.Engine;
using KafkaWorker.Etcd;
using KafkaWorker.Etcd.Client;
using KafkaWorker.Etcd.Coordination;
using KafkaWorker.Provisioning.Kafka;
using KafkaWorker.Provisioning.Processes;

// Точка входа KafkaWorker (arch/16 §8): host-builder с HTTP-гранью /healthz,
// конфигурация appsettings+env, DI всех слоёв (etcd → координация → docker →
// процессы → циклы). Env-секретов per-install НЕТ (единственный секрет —
// per-cluster app_password в etcd). Fail-fast: пустые Etcd:Endpoints/Hosts.

var builder = WebApplication.CreateBuilder(args);

// Конфигурация: appsettings.json + env-оверрайды KafkaWorker__*.
builder.Services.Configure<KafkaWorkerOptions>(builder.Configuration.GetSection("KafkaWorker"));
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<HealthState>();

builder.Services.AddHttpClient("etcd");

// Fail-fast при старте: без etcd-endpoints воркер бессмысленен (hosts — в DI-фабрике драйвера);
// ключ доступа /kafkaworker/api/<id> без URL бессмысленен (arch/16 §1.1).
builder.Services.AddOptions<KafkaWorkerOptions>()
    .Validate(o => o.Etcd.Endpoints is { Length: > 0 }, "KafkaWorker:Etcd:Endpoints не заданы")
    .Validate(o => !string.IsNullOrWhiteSpace(o.Api.AdvertiseUrl),
        "KafkaWorker:Api:AdvertiseUrl не задан (env KFW_API_ADVERTISE_URL)")
    .ValidateOnStart();

// etcd-клиент (HTTP JSON gateway /v3/*) + координация (клэймы/лидерство, журнал).
builder.Services.AddSingleton<IEtcdGateway>(sp =>
    new EtcdGateway(sp.GetRequiredService<IHttpClientFactory>().CreateClient("etcd")));
builder.Services.AddSingleton(sp => new ClaimStore(
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Api.AdvertiseUrl));
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
builder.Services.AddSingleton(sp => new RebalanceHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<TimeProvider>()));

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
// это пробы/конфиги, длинные ожидания — циклы процессов.
builder.Services.AddSingleton<IKafkaAdminClientFactory>(_ =>
    new KafkaAdminClientFactory(TimeSpan.FromSeconds(10)));

// Ensure per-cluster SASL-секрета (arch/16 §4): чтение/txn put-if-absent.
builder.Services.AddSingleton<IAppSecretEnsurer>(sp => new AppSecretEnsurer(
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
        sp.GetRequiredService<IAppSecretEnsurer>(),
        sp.GetRequiredService<IKafkaAdminClientFactory>(),
        sp.GetRequiredService<IClusterConfigConverger>(),
        ToProvisioningOptions(opts),
        SnapshotDelegate(sp.GetRequiredService<SnapshotJob>()));
});
builder.Services.AddSingleton(sp => new DeprovisioningProcess(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IClusterDriver>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    SnapshotDelegate(sp.GetRequiredService<SnapshotJob>())));
builder.Services.AddSingleton(sp => new NodeSupervisor(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IClusterDriver>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<IKafkaAdminClientFactory>(),
    ToProvisioningOptions(sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value)));

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
    sp.GetRequiredService<IKafkaAdminClientFactory>(),
    ToProvisioningOptions(sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value)));
builder.Services.AddSingleton(sp => new AppPasswordRotator(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IClusterDriver>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<IKafkaAdminClientFactory>(),
    ToProvisioningOptions(sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value),
    SnapshotDelegate(sp.GetRequiredService<SnapshotJob>())));

// Автосинк топиков (arch/16 §5 D): троттлинг TopicSyncIntervalSec внутри.
builder.Services.AddSingleton(sp => new TopicSyncProcess(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<IKafkaAdminClientFactory>(),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Loops.TopicSyncIntervalSec));

// Циклы: keepalive первым (lease живут до Reconcile), затем снапшоты и reconcile.
builder.Services.AddSingleton<IKafkaClusterProcesses, KafkaClusterProcesses>();
builder.Services.AddSingleton<KeepaliveLoop>();
builder.Services.AddSingleton<SnapshotLoop>();
builder.Services.AddSingleton<ReconcileLoop>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<KeepaliveLoop>());
builder.Services.AddHostedService(sp => sp.GetRequiredService<SnapshotLoop>());
builder.Services.AddHostedService(sp => sp.GetRequiredService<ReconcileLoop>());

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
app.UseMiddleware<ApiKeyMiddleware>();
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

// WAF-тесты (KafkaWorker.IntegrationTests/Api): точка входа как public partial.
public partial class Program;
