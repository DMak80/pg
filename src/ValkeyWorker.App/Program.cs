using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using ValkeyWorker.App;
using Shared.Metrics;
using ValkeyWorker.App.Api;
using ValkeyWorker.App.Api.Operations;
using Shared.Core.HealthChecks;
using ValkeyWorker.App.HealthChecks;
using ValkeyWorker.App.Loops;
using ValkeyWorker.Docker.Engine;
using ValkeyWorker.Docker.Drivers;
using ValkeyWorker.Core.Valkey;
using ValkeyWorker.Provisioning.Processes;
using Shared.Etcd.Client;

// Точка входа ValkeyWorker (arch/21 §8): host-builder с mTLS-гранью HTTP API
// (вкл. /healthz), конфигурация appsettings+env, DI всех слоёв (etcd →
// координация → циклы). Per-install env-секреты — только TLS HTTP API
// (arch/21 §4); per-cluster секреты (app/admin) — в etcd.
// Fail-fast: пустые Etcd:Endpoints, не-https AdvertiseUrl.
// Каркас t02 (фаза 2): циклы живы, процессы A–E — задача 12, API — задача 13.

var builder = WebApplication.CreateBuilder(args);

// Конфигурация: appsettings.json + env-оверрайды ValkeyWorker__*.
builder.Services.Configure<ValkeyWorkerOptions>(builder.Configuration.GetSection("ValkeyWorker"));
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<HealthState>();

// Метрики (arch/18 §2.2): /metrics на том же mTLS-Kestrel-порту, что /healthz —
// scrape ходит клиентским сертом per-install пакета. Коллектор доменных метрик
// INFO — t05 (в t02 не входит).
builder.Services.AddAppMetrics("ValkeyWorker", builder.Configuration.GetSection("ValkeyWorker:Metrics"));
builder.Services.AddSingleton(sp =>
{
    var m = new Shared.Metrics.Worker.WorkerMetricsInstrumentation(
        sp.GetRequiredService<System.Diagnostics.Metrics.Meter>(),
        sp.GetRequiredService<TimeProvider>());
    // Единый seam фаз/операций (S2, зеркало PgWorker/KafkaWorker): терминальные фазы/
    // first-seen/подавление supervise — внутри OnJournalPhase.
    sp.GetRequiredService<WorkJournal>().PhaseWritten
        += e => m.OnJournalPhase(e.Cluster, e.Op, e.Phase);
    return m;
});

// etcd-клиент: HTTP JSON gateway /v3/*; handler против DNS-флейпа Docker
// embedded DNS (t09; arch/21 §7): PooledConnectionLifetime + IPv4-first резолв.
// EtcdGateway-синглтон захвачен HttpClient навсегда — ротация handler'ов фабрики
// на него не действует, поэтому явный SocketsHttpHandler.
builder.Services.AddHttpClient("etcd")
    .ConfigurePrimaryHttpMessageHandler(EtcdConnectCallback.CreateHandler);

// Fail-fast при старте: без etcd-endpoints воркер бессмысленен (docker-правила —
// в DI-фабрике драйвера, задача 4);
// ключ доступа /valkeyworker/api/<id> без URL бессмысленен (arch/21 §1.1).
builder.Services.AddOptions<ValkeyWorkerOptions>()
    .Validate(o => o.Etcd.Endpoints is { Length: > 0 }, "ValkeyWorker:Etcd:Endpoints не заданы")
    .Validate(o => !string.IsNullOrWhiteSpace(o.Api.AdvertiseUrl),
        "ValkeyWorker:Api:AdvertiseUrl не задан (env VWK_API_ADVERTISE_URL)")
    .Validate(o => o.Api.Tls.AllowInsecureHttp
        || o.Api.AdvertiseUrl.StartsWith("https://", StringComparison.Ordinal),
        "AdvertiseUrl обязан быть https:// (mTLS-only API, arch/21 §1.1)")
    .ValidateOnStart();

// Управляемый серт API (arch/21 §1.1): чтение ключа /workers/api_tls/valkeyworker
// ДО поднятия Kestrel; приоритет etcd > env; битый ключ — fail-fast.
var etcdEndpoints = builder.Configuration.GetSection("ValkeyWorker:Etcd:Endpoints").Get<string[]>() ?? [];
var managedCert = await WorkerApiCertReader.ReadAsync(etcdEndpoints, "valkeyworker", CancellationToken.None);

// mTLS HTTP API (arch/21 §1.1): env-секреты TLS → конфиг, Kestrel c
// серверным сертом и требованием клиентского серта per-install API-CA.
TlsEndpoints.ApplyEnvOverrides(builder.Configuration);
var apiTls = TlsEndpoints.ConfigureMtls(builder, port: 8080, managedCert);

// Thumbprint применённого серта → дискавери-ключ (arch/21 §1.1): панель
// сверяет с целевым → applied/pending restart.
var apiCertThumbprint = apiTls.ServerCert is { } appliedCert
    ? Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(appliedCert.RawData)).ToLowerInvariant()
    : null;

// etcd-клиент (HTTP JSON gateway /v3/*) + координация (клэймы/лидерство, журнал).
// Единое место литерала префикса etcd-ключей: Shared-координация параметризована.
const string KeyPrefix = "/valkeyworker";
builder.Services.AddSingleton<IEtcdGateway>(sp =>
    new EtcdGateway(sp.GetRequiredService<IHttpClientFactory>().CreateClient("etcd")));
builder.Services.AddSingleton(sp => new ClaimStore(
    KeyPrefix,
    sp.GetRequiredService<IOptions<ValkeyWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<IOptions<ValkeyWorkerOptions>>().Value.Api.AdvertiseUrl,
    apiCertThumbprint));
// Глобальный portalloc-клэйм (arch/21 §2) — DI-синглтон, InstanceId единый с
// ClaimStore (сквозная диагностика держателя).
builder.Services.AddSingleton(sp => new PortAllocLock(
    KeyPrefix,
    sp.GetRequiredService<IOptions<ValkeyWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<ClaimStore>().InstanceId));
builder.Services.AddSingleton(sp => new WorkJournal(
    KeyPrefix,
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<ValkeyWorkerOptions>>().Value.Etcd.Endpoints));

// docker-фабрика движков (health-пробы docker-hosts) + драйвер по режиму
// (Plain: таблица Hosts; Swarm: manager endpoint) — fail-fast на противоречивой
// конфигурации (arch/21 §8).
builder.Services.AddSingleton<DockerEngineFactory>();
builder.Services.AddSingleton<IClusterDriver>(sp =>
{
    var docker = sp.GetRequiredService<IOptions<ValkeyWorkerOptions>>().Value.Docker;
    var factory = sp.GetRequiredService<DockerEngineFactory>();
    if (string.Equals(docker.Mode, "Swarm", StringComparison.OrdinalIgnoreCase))
    {
        if (string.IsNullOrWhiteSpace(docker.SwarmManager))
            throw new ApplicationException("ValkeyWorker:Docker:Mode=Swarm требует ValkeyWorker:Docker:SwarmManager");
        return new SwarmClusterDriver(docker.SwarmManager, factory);
    }

    var hosts = docker.Hosts
        .Select(h => new HostEndpoint(h.Name, h.Endpoint))
        .ToList();
    if (hosts.Count == 0)
        throw new ApplicationException("ValkeyWorker:Docker:Mode=Plain требует непустую таблицу ValkeyWorker:Docker:Hosts");
    return new PlainClusterDriver(hosts, factory);
});

// Снапшоты P12 (SnapshotLoop-лидер + процессы в точках изменений «до/после»):
// контроль-плейн /valkey/ + /valkeyworker/ — SnapshotJob снимает etcd целиком.
builder.Services.AddSingleton(sp =>
{
    var opts = sp.GetRequiredService<IOptions<ValkeyWorkerOptions>>().Value;
    return new SnapshotJob(
        sp.GetRequiredService<IEtcdGateway>(), opts.Etcd.Endpoints,
        opts.Snapshots.Dir, opts.Snapshots.RetentionFiles, opts.Snapshots.MaintenanceIntervalMin);
});

// Процессы A–E + вспомогательные (arch/21 §5): снапшот-делегат P12 «до/после»
// у provisioning/deprovisioning; RESP-клиент — короткоживущие пробы.
builder.Services.AddSingleton<IValkeyConnection, ValkeyConnection>();
builder.Services.AddSingleton<IClusterSecretEnsurer>(sp => new ClusterSecretEnsurer(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<ValkeyWorkerOptions>>().Value.Etcd.Endpoints));
builder.Services.AddSingleton(sp => new PortAllocIndex(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<ValkeyWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<ILogger<PortAllocIndex>>()));
builder.Services.AddSingleton(sp => new PortAllocHealer(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<ValkeyWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IClusterDriver>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<PortAllocLock>(),
    sp.GetRequiredService<PortAllocIndex>(),
    ToProvisioningOptions(sp.GetRequiredService<IOptions<ValkeyWorkerOptions>>().Value)));
builder.Services.AddSingleton(sp => new ProvisioningProcess(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<ValkeyWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IClusterDriver>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<PortAllocLock>(),
    sp.GetRequiredService<PortAllocIndex>(),
    sp.GetRequiredService<IClusterSecretEnsurer>(),
    sp.GetRequiredService<IValkeyConnection>(),
    ToProvisioningOptions(sp.GetRequiredService<IOptions<ValkeyWorkerOptions>>().Value),
    SnapshotDelegate(sp.GetRequiredService<SnapshotJob>())));
builder.Services.AddSingleton(sp => new DeprovisioningProcess(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<ValkeyWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IClusterDriver>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    SnapshotDelegate(sp.GetRequiredService<SnapshotJob>())));
builder.Services.AddSingleton(sp => new NodeSupervisor(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<ValkeyWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IClusterDriver>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<IValkeyConnection>(),
    ToProvisioningOptions(sp.GetRequiredService<IOptions<ValkeyWorkerOptions>>().Value),
    sp.GetRequiredService<PortAllocHealer>()));
builder.Services.AddSingleton(sp => new ConfigConverger(
    sp.GetRequiredService<IValkeyConnection>(),
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<ValkeyWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<WorkJournal>()));
builder.Services.AddSingleton(sp => new PasswordRotator(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<ValkeyWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<IValkeyConnection>()));

// HTTP API воркера (arch/21 §1.1): мутации декларативного контракта
// valkey-домена — хендлеры-синглтоны.
builder.Services.AddSingleton(sp => new CreateClusterHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<ValkeyWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<TimeProvider>()));
builder.Services.AddSingleton(sp => new DeleteClusterHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<ValkeyWorkerOptions>>().Value.Etcd.Endpoints));
builder.Services.AddSingleton(sp => new UpdateConfigHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<ValkeyWorkerOptions>>().Value.Etcd.Endpoints));
builder.Services.AddSingleton(sp => new UpdateResourcesHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<ValkeyWorkerOptions>>().Value.Etcd.Endpoints));
builder.Services.AddSingleton(sp => new RotatePasswordHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<ValkeyWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<TimeProvider>()));
builder.Services.AddSingleton(sp => new SeedDemoHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<ValkeyWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<IOptions<ValkeyWorkerOptions>>().Value.Api.EnableSeedEndpoint));
builder.Services.AddSingleton(sp => new RestartHandler(
    sp.GetRequiredService<IHostApplicationLifetime>(),
    sp.GetRequiredService<ILoggerFactory>().CreateLogger<RestartHandler>()));

// Циклы: keepalive первым (lease живут до Reconcile), затем снапшоты и reconcile.
builder.Services.AddSingleton<IValkeyClusterProcesses, ValkeyClusterProcesses>();
builder.Services.AddSingleton<KeepaliveLoop>();
builder.Services.AddSingleton<SnapshotLoop>();
builder.Services.AddSingleton<ReconcileLoop>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<KeepaliveLoop>());
builder.Services.AddHostedService(sp => sp.GetRequiredService<SnapshotLoop>());
builder.Services.AddHostedService(sp => sp.GetRequiredService<ReconcileLoop>());

// Наблюдаемость (arch/21 §7): агрегированный health + per-loop обёртки.
builder.Services.AddSingleton<ServiceProbes>();
builder.Services.AddSingleton<ValkeyWorkerHealth>();
builder.Services.AddSingleton<HealthCheckAbstract<ReconcileLoop>>(
    sp => new(sp.GetRequiredService<ReconcileLoop>()));
builder.Services.AddSingleton<HealthCheckAbstract<KeepaliveLoop>>(
    sp => new(sp.GetRequiredService<KeepaliveLoop>()));
builder.Services.AddSingleton<HealthCheckAbstract<SnapshotLoop>>(
    sp => new(sp.GetRequiredService<SnapshotLoop>()));
builder.Services.AddHealthChecks()
    .AddCheck<ValkeyWorkerHealth>("valkeyworker", tags: ["ready"])
    .AddCheck<HealthCheckAbstract<ReconcileLoop>>("reconcile-loop")
    .AddCheck<HealthCheckAbstract<KeepaliveLoop>>("keepalive-loop")
    .AddCheck<HealthCheckAbstract<SnapshotLoop>>("snapshot-loop");

var app = builder.Build();
if (app.Services.GetRequiredService<IOptions<ValkeyWorkerOptions>>().Value.Api.Tls.AllowInsecureHttp)
    app.Logger.LogWarning(
        "ValkeyWorker:Api:Tls:AllowInsecureHttp=true — HTTP без TLS (ТОЛЬКО WAF-тесты, arch/21 §1.1)");
if (apiTls.Source is { } certSource)
    app.Logger.LogInformation("ValkeyWorker:Api:Tls: серверный серт API — источник {Source}", certSource);
if (apiTls.Warning is { } certWarning)
    app.Logger.LogWarning("ValkeyWorker:Api:Tls: {Warning}", certWarning);
app.MapAppMetrics();
app.MapHealthChecks("/healthz");
app.MapWorkerApi();

await app.RunAsync();

// Опции процессов из дерева конфигурации (arch/21 §8).
static ValkeyWorker.Provisioning.Processes.ValkeyProvisioningOptions ToProvisioningOptions(ValkeyWorkerOptions opts) => new(
    opts.Docker.PortRange.From,
    opts.Docker.PortRange.To,
    opts.Thresholds.NodeBootSec,
    opts.Thresholds.NodeDeadSec,
    opts.AdvertisedClientHost,
    opts.Docker.Images.Node);

// Делегат снапшота для процессов (P12 «до/после» в точках изменений).
static Func<CancellationToken, Task<Result>> SnapshotDelegate(SnapshotJob job)
    => async ct => await job.TakeAsync(ct);

// WAF-тесты (ValkeyWorker.IntegrationTests/Api, задача 13): точка входа как public partial.
public partial class Program;
