using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using S3Worker.App;
using Shared.Metrics;
using S3Worker.App.Api;
using S3Worker.App.HealthChecks;
using S3Worker.App.Loops;
using Shared.Core.HealthChecks;
using Shared.Core.Hosting;
using Shared.Etcd.Client;

// Точка входа S3Worker (arch/24 §8): host-builder с mTLS-гранью HTTP API
// (вкл. /healthz), конфигурация appsettings+env, DI слоёв. Per-install
// env-секреты — TLS API (S3W_API_TLS_*), docker-транспорт (S3W_DOCKER_*),
// профили секретов нод (S3W_NODE_SECRETS__*, arch/23 §6).
// Каркас t32 Task 3: координация/циклы — Task 4, драйвер docker — Task 6,
// процессы — Tasks 10–13, API-эндпоинты — Task 14 (каждая задача правит свой
// блок DI; инкрементальный рост Program).

var builder = WebApplication.CreateBuilder(args);

// Конфигурация: appsettings.json + env-оверрайды S3Worker__*.
builder.Services.Configure<S3WorkerOptions>(builder.Configuration.GetSection("S3Worker"));
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<HealthState>();
// Прогресс-отметки долгих фаз reconcile в глубину процессов (provisioning).
builder.Services.AddSingleton<Shared.Core.Hosting.ILoopProgress>(sp => sp.GetRequiredService<HealthState>());

// Метрики (arch/18 §2.2): /metrics на том же mTLS-Kestrel-порту, что /healthz —
// scrape ходит клиентским сертом per-install пакета. Meter "S3Worker" —
// s3worker_*-метрики домена (arch/24 §7).
builder.Services.AddAppMetrics("S3Worker", builder.Configuration.GetSection("S3Worker:Metrics"));
builder.Services.AddSingleton(sp =>
{
    var m = new Shared.Metrics.Worker.WorkerMetricsInstrumentation(
        sp.GetRequiredService<System.Diagnostics.Metrics.Meter>(),
        sp.GetRequiredService<TimeProvider>());
    // Единый seam фаз/операций (S2, зеркало PgWorker/Kafka/Valkey): терминальные
    // фазы/first-seen/подавление supervise — внутри OnJournalPhase.
    sp.GetRequiredService<WorkJournal>().PhaseWritten
        += e => m.OnJournalPhase(e.Cluster, e.Op, e.Phase);
    return m;
});

// etcd-клиент: HTTP JSON gateway /v3/*; handler против DNS-флейпа Docker
// embedded DNS (t09): PooledConnectionLifetime + IPv4-first резолв.
// Поллинг-инвариант (arch/24 §6): одиночный HTTP-вызов к etcd не молчит дольше
// окна проверки watchdog — таймаут = половина окна (7.5 c при дефолтах).
builder.Services.AddHttpClient("etcd", c => c.Timeout = TimeSpan.FromSeconds(7.5))
    .ConfigurePrimaryHttpMessageHandler(EtcdConnectCallback.CreateHandler);

// env-секреты docker-транспорта → конфиг-дерево (до валидаций).
DockerEnvBindings.ApplyTlsEnvOverrides(builder.Configuration);
DockerEnvBindings.ApplySshEnvOverrides(builder.Configuration);

// Fail-fast при старте (arch/24 §8): чистые предикаты S3StartupChecks —
// endpoints etcd, advertise, docker-топология, пороги, bucket, порт-диапазон.
builder.Services.AddOptions<S3WorkerOptions>()
    .Validate(o => S3StartupChecks.EtcdEndpoints(o), "S3Worker:Etcd:Endpoints не заданы")
    .Validate(o => S3StartupChecks.AdvertiseUrl(o),
        "S3Worker:Api:AdvertiseUrl не задан или не https:// (env S3W_API_ADVERTISE_URL; mTLS-only API — arch/24 §1.1)")
    .Validate(o => S3StartupChecks.DockerTopology(o),
        "S3Worker:Docker: некорректная топология (Swarm → SwarmManager обязателен; Plain → непустая таблица Hosts без дубликатов имён)")
    .Validate(o => S3StartupChecks.Thresholds(o),
        "S3Worker:Thresholds: пороги обязаны быть положительными, NodeBootSec ≤ 100")
    .Validate(o => S3StartupChecks.Bucket(o),
        "S3Worker:Bucket не задан или не соответствует паттерну имени S3-бакета (env S3W_BUCKET)")
    .Validate(o => S3StartupChecks.PortRange(o),
        "S3Worker:Docker:PortRange: 0 < From < To ≤ 65535")
    .ValidateOnStart();

// env-профили секретов нод (arch/23 §6): читатель + fail-fast полупар
// (arch/24 §8 «неполные секретные пары») — воркер с обрезанным env не стартует.
var secretStore = new S3Worker.Provisioning.SecretProfileStore();
foreach (var broken in secretStore.ValidateEnvPairs())
    throw new ApplicationException($"S3W_NODE_SECRETS__{broken}: неполная секретная пара");
builder.Services.AddSingleton(secretStore);

// HTTP API хендлеры (arch/24 §1.1; Task 14) + счётчик операций.
builder.Services.AddSingleton(sp => new S3Worker.App.Api.S3ApiMetrics(
    sp.GetRequiredService<System.Diagnostics.Metrics.Meter>()));
builder.Services.AddSingleton(sp => new S3Worker.App.Api.Operations.CreateClusterHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<S3WorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<S3Worker.Provisioning.SecretProfileStore>(),
    sp.GetRequiredService<TimeProvider>()));
builder.Services.AddSingleton(sp => new S3Worker.App.Api.Operations.DeleteClusterHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<S3WorkerOptions>>().Value.Etcd.Endpoints));
builder.Services.AddSingleton(sp => new S3Worker.App.Api.Operations.AddNodeHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<S3WorkerOptions>>().Value.Etcd.Endpoints));
builder.Services.AddSingleton(sp => new S3Worker.App.Api.Operations.DeleteNodeHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<S3WorkerOptions>>().Value.Etcd.Endpoints));
builder.Services.AddSingleton(sp => new S3Worker.App.Api.Operations.UpdateNodeResourcesHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<S3WorkerOptions>>().Value.Etcd.Endpoints));
builder.Services.AddSingleton(sp => new S3Worker.App.Api.Operations.PostRotationHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<S3WorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<TimeProvider>()));
builder.Services.AddSingleton(sp => new S3Worker.App.Api.Operations.SyncProbeHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<S3WorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<TimeProvider>()));
// Граф репликации и отзыв ключей (arch/24 §1.1, t40): put-links, deny/allow.
builder.Services.AddSingleton(sp => new S3Worker.App.Api.Operations.PutLinksHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<S3WorkerOptions>>().Value.Etcd.Endpoints));
builder.Services.AddSingleton(sp => new S3Worker.App.Api.Operations.DenyHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<S3WorkerOptions>>().Value.Etcd.Endpoints));
builder.Services.AddSingleton(sp => new S3Worker.App.Api.Operations.AllowHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<S3WorkerOptions>>().Value.Etcd.Endpoints));
builder.Services.AddSingleton(sp => new S3Worker.App.Api.Operations.SeedDemoHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<S3WorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<S3Worker.App.Api.Operations.CreateClusterHandler>(),
    sp.GetRequiredService<IOptionsMonitor<S3WorkerOptions>>()));

// Управляемый серт API (arch/24 §1.1): чтение ключа /workers/api_tls/s3worker
// ДО поднятия Kestrel; приоритет etcd > env; битый ключ — fail-fast.
var etcdEndpoints = builder.Configuration.GetSection("S3Worker:Etcd:Endpoints").Get<string[]>() ?? [];
var managedCert = await WorkerApiCertReader.ReadAsync(etcdEndpoints, "s3worker", CancellationToken.None);

// mTLS HTTP API (arch/24 §1.1): env-секреты TLS → конфиг, Kestrel c
// серверным сертом и требованием клиентского серта per-install API-CA.
TlsEndpoints.ApplyEnvOverrides(builder.Configuration);
var apiTls = TlsEndpoints.ConfigureMtls(builder, port: 8080, managedCert);

// Thumbprint применённого серта → дискавери-ключ (arch/24 §1.1): панель
// сверяет с целевым → applied/pending restart.
var apiCertThumbprint = apiTls.ServerCert is { } appliedCert
    ? Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(appliedCert.RawData)).ToLowerInvariant()
    : null;

// etcd-клиент (HTTP JSON gateway /v3/*) + координация (клэймы/лидерство, журнал).
// Единое место литерала префикса etcd-ключей: Shared-координация параметризована
// (arch/23 §8: /s3worker/{leader,claims,work,portalloc,locks/portalloc,instances,api,rotations}).
const string KeyPrefix = "/s3worker";
builder.Services.AddSingleton<IEtcdGateway>(sp =>
    new EtcdGateway(sp.GetRequiredService<IHttpClientFactory>().CreateClient("etcd")));

// Клиент ownS3-ноды (arch/23 §5): health/bucket/usage; таймаут — поллинг-
// инвариант (половина окна watchdog).
builder.Services.AddHttpClient("owns3-node", c => c.Timeout = TimeSpan.FromSeconds(7.5));
builder.Services.AddSingleton<S3Worker.Core.OwnS3.IOwnS3NodeClient>(sp =>
    new S3Worker.Core.OwnS3.OwnS3NodeClient(
        sp.GetRequiredService<IHttpClientFactory>().CreateClient("owns3-node")));
builder.Services.AddSingleton(sp => new ClaimStore(
    KeyPrefix,
    sp.GetRequiredService<IOptions<S3WorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<IOptions<S3WorkerOptions>>().Value.Api.AdvertiseUrl,
    apiCertThumbprint));
// Глобальный portalloc-клэйм (arch/23 §8) — DI-синглтон, InstanceId единый с
// ClaimStore (сквозная диагностика держателя).
builder.Services.AddSingleton(sp => new PortAllocLock(
    KeyPrefix,
    sp.GetRequiredService<IOptions<S3WorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<ClaimStore>().InstanceId));
builder.Services.AddSingleton(sp => new WorkJournal(
    KeyPrefix,
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<S3WorkerOptions>>().Value.Etcd.Endpoints));

// docker-фабрика движков (health-пробы docker-hosts) + драйвер по режиму
// (Plain: таблица Hosts; Swarm: manager endpoint) — fail-fast на противоречивой
// конфигурации уже в S3StartupChecks.DockerTopology (arch/24 §8).
builder.Services.AddSingleton<DockerEngineFactory>();
builder.Services.AddSingleton<S3Worker.Docker.Drivers.IS3ClusterDriver>(sp =>
{
    var docker = sp.GetRequiredService<IOptions<S3WorkerOptions>>().Value.Docker;
    var factory = sp.GetRequiredService<DockerEngineFactory>();
    if (string.Equals(docker.Mode, "Swarm", StringComparison.OrdinalIgnoreCase))
    {
        // Таблица Hosts — и для swarm: endpoint'ы Engine API нод по имени
        // (hostname из /nodes) — том данных нод-локален; пустая таблица
        // (однонодовый контур) — manager.
        return new S3Worker.Docker.Drivers.SwarmClusterDriver(
            docker.SwarmManager!, factory,
            docker.Hosts.Select(h => new S3Worker.Docker.Drivers.HostEndpoint(h.Name, h.Endpoint)).ToList());
    }

    return new S3Worker.Docker.Drivers.PlainClusterDriver(
        docker.Hosts.Select(h => new S3Worker.Docker.Drivers.HostEndpoint(h.Name, h.Endpoint)).ToList(),
        factory);
});

// Снапшоты P12 (SnapshotLoop-лидер + процессы в точках изменений «до/после»):
// контроль-плейн /s3/ + /s3worker/ — SnapshotJob снимает etcd целиком.
builder.Services.AddSingleton(sp =>
{
    var opts = sp.GetRequiredService<IOptions<S3WorkerOptions>>().Value;
    return new SnapshotJob(
        sp.GetRequiredService<IEtcdGateway>(), opts.Etcd.Endpoints,
        opts.Snapshots.Dir, opts.Snapshots.RetentionFiles, opts.Snapshots.MaintenanceIntervalMin);
});

// Процессы A–E + вспомогательные (Tasks 7–13): индекс portalloc, опции
// процессов (плоский срез дерева), provisioning (A); остальные подключаются
// задачами 11–13; агрегатор — каркас Task 4.
builder.Services.AddSingleton(sp => new S3Worker.Provisioning.Processes.PortAllocIndex(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<S3WorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<S3Worker.Provisioning.Processes.PortAllocIndex>>()));
builder.Services.AddSingleton(ToProvisioningOptions(
    builder.Configuration.GetSection("S3Worker").Get<S3WorkerOptions>()!));
// Бюджет boot-ожидания — общий синглтон provisioning/deprovisioning: сброс на
// done/aborted-state-changed выводит из provisioning, демонтаж начинается с нуля.
builder.Services.AddSingleton(sp => new S3Worker.Provisioning.Processes.BootWaitBudget(
    sp.GetRequiredService<TimeProvider>()));
// PKI репликации per-cluster + единая сборка env инстанса (arch/24 §4/§5, t40);
// процесс G — доводка состава mesh (arch/24 §5 G).
builder.Services.AddSingleton(sp => new S3Worker.Provisioning.PkiStore(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<S3WorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<TimeProvider>()));
builder.Services.AddSingleton<S3Worker.Provisioning.InstanceEnvComposer>();
builder.Services.AddSingleton(sp => new S3Worker.Provisioning.Processes.MeshGovernanceProcess(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<S3WorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<S3Worker.Provisioning.SecretProfileStore>(),
    sp.GetRequiredService<S3Worker.Provisioning.PkiStore>(),
    sp.GetRequiredService<S3Worker.Core.OwnS3.IOwnS3NodeClient>(),
    ToProvisioningOptions(sp.GetRequiredService<IOptions<S3WorkerOptions>>().Value),
    sp.GetRequiredService<System.Diagnostics.Metrics.Meter>()));
// Процесс F — зонд связности/синхронности (arch/24 §5 F, t33).
builder.Services.AddSingleton(sp => new S3Worker.Provisioning.Processes.SyncProbeProcess(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<S3WorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<S3Worker.Provisioning.SecretProfileStore>(),
    sp.GetRequiredService<S3Worker.Core.OwnS3.IOwnS3NodeClient>(),
    ToProvisioningOptions(sp.GetRequiredService<IOptions<S3WorkerOptions>>().Value),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<System.Diagnostics.Metrics.Meter>()));
builder.Services.AddSingleton(sp => new S3Worker.Provisioning.Processes.ProvisioningProcess(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<S3WorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<S3Worker.Docker.Drivers.IS3ClusterDriver>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<PortAllocLock>(),
    sp.GetRequiredService<S3Worker.Provisioning.Processes.PortAllocIndex>(),
    sp.GetRequiredService<S3Worker.Provisioning.SecretProfileStore>(),
    sp.GetRequiredService<S3Worker.Provisioning.PkiStore>(),
    sp.GetRequiredService<S3Worker.Provisioning.InstanceEnvComposer>(),
    sp.GetRequiredService<S3Worker.Provisioning.Processes.MeshGovernanceProcess>(),
    sp.GetRequiredService<S3Worker.Core.OwnS3.IOwnS3NodeClient>(),
    ToProvisioningOptions(sp.GetRequiredService<IOptions<S3WorkerOptions>>().Value),
    sp.GetRequiredService<S3Worker.Provisioning.Processes.BootWaitBudget>(),
    SnapshotDelegate(sp.GetRequiredService<SnapshotJob>())));
builder.Services.AddSingleton(sp => new S3Worker.Provisioning.Processes.PortAllocHealer(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<S3WorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<S3Worker.Docker.Drivers.IS3ClusterDriver>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<PortAllocLock>(),
    sp.GetRequiredService<S3Worker.Provisioning.Processes.PortAllocIndex>(),
    ToProvisioningOptions(sp.GetRequiredService<IOptions<S3WorkerOptions>>().Value)));
builder.Services.AddSingleton(sp => new S3Worker.Provisioning.Processes.NodeSupervisor(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<S3WorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<S3Worker.Docker.Drivers.IS3ClusterDriver>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<S3Worker.Core.OwnS3.IOwnS3NodeClient>(),
    sp.GetRequiredService<S3Worker.Provisioning.SecretProfileStore>(),
    sp.GetRequiredService<S3Worker.Provisioning.PkiStore>(),
    sp.GetRequiredService<S3Worker.Provisioning.InstanceEnvComposer>(),
    ToProvisioningOptions(sp.GetRequiredService<IOptions<S3WorkerOptions>>().Value),
    sp.GetRequiredService<S3Worker.Provisioning.Processes.PortAllocHealer>()));
builder.Services.AddSingleton(sp => new S3Worker.Provisioning.Processes.DeprovisioningProcess(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<S3WorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<S3Worker.Docker.Drivers.IS3ClusterDriver>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<S3Worker.Provisioning.Processes.BootWaitBudget>(),
    SnapshotDelegate(sp.GetRequiredService<SnapshotJob>())));
builder.Services.AddSingleton(sp => new S3Worker.Provisioning.Processes.AddNodeProcess(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<S3WorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<S3Worker.Docker.Drivers.IS3ClusterDriver>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<PortAllocLock>(),
    sp.GetRequiredService<S3Worker.Provisioning.Processes.PortAllocIndex>(),
    sp.GetRequiredService<S3Worker.Provisioning.SecretProfileStore>(),
    sp.GetRequiredService<S3Worker.Provisioning.PkiStore>(),
    sp.GetRequiredService<S3Worker.Provisioning.InstanceEnvComposer>(),
    sp.GetRequiredService<S3Worker.Core.OwnS3.IOwnS3NodeClient>(),
    ToProvisioningOptions(sp.GetRequiredService<IOptions<S3WorkerOptions>>().Value),
    SnapshotDelegate(sp.GetRequiredService<SnapshotJob>())));
builder.Services.AddSingleton(sp => new S3Worker.Provisioning.Processes.RemoveNodeProcess(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<S3WorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<S3Worker.Docker.Drivers.IS3ClusterDriver>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<PortAllocLock>(),
    ToProvisioningOptions(sp.GetRequiredService<IOptions<S3WorkerOptions>>().Value),
    SnapshotDelegate(sp.GetRequiredService<SnapshotJob>())));
builder.Services.AddSingleton(sp => new S3Worker.Provisioning.Processes.SecretRotator(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<S3WorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<S3Worker.Docker.Drivers.IS3ClusterDriver>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<S3Worker.Provisioning.SecretProfileStore>(),
    sp.GetRequiredService<S3Worker.Provisioning.PkiStore>(),
    sp.GetRequiredService<S3Worker.Provisioning.InstanceEnvComposer>(),
    sp.GetRequiredService<S3Worker.Core.OwnS3.IOwnS3NodeClient>(),
    ToProvisioningOptions(sp.GetRequiredService<IOptions<S3WorkerOptions>>().Value)));
builder.Services.AddSingleton<IS3ClusterProcesses, S3ClusterProcesses>();

// Циклы: keepalive первым (lease живут до Reconcile), затем снапшоты и reconcile.
builder.Services.AddSingleton<KeepaliveLoop>();
builder.Services.AddSingleton<SnapshotLoop>();
builder.Services.AddSingleton<ReconcileLoop>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<KeepaliveLoop>());
builder.Services.AddHostedService(sp => sp.GetRequiredService<SnapshotLoop>());
builder.Services.AddHostedService(sp => sp.GetRequiredService<ReconcileLoop>());

// Наблюдаемость (arch/24 §7): агрегированный health + per-loop обёртки.
builder.Services.AddSingleton<ServiceProbes>();
builder.Services.AddSingleton<S3WorkerHealth>();
builder.Services.AddSingleton<HealthCheckAbstract<ReconcileLoop>>(
    sp => new(sp.GetRequiredService<ReconcileLoop>()));
builder.Services.AddSingleton<HealthCheckAbstract<KeepaliveLoop>>(
    sp => new(sp.GetRequiredService<KeepaliveLoop>()));
builder.Services.AddSingleton<HealthCheckAbstract<SnapshotLoop>>(
    sp => new(sp.GetRequiredService<SnapshotLoop>()));
builder.Services.AddHealthChecks()
    .AddCheck<S3WorkerHealth>("s3worker", tags: ["ready"])
    .AddCheck<HealthCheckAbstract<ReconcileLoop>>("reconcile-loop")
    .AddCheck<HealthCheckAbstract<KeepaliveLoop>>("keepalive-loop")
    .AddCheck<HealthCheckAbstract<SnapshotLoop>>("snapshot-loop");

// Watchdog зависших циклов (arch/24 §6): staleness активности (тик или
// прогресс-отметка) всех циклов → журнал + метрика + graceful self-stop;
// Enabled=false (секция S3Worker:Loops:Watchdog) — компонент не регистрируется.
builder.Services.AddSingleton<S3WorkerLoopsVitality>();
builder.Services.AddSingleton<Shared.Core.Hosting.ILoopsVitality>(
    sp => sp.GetRequiredService<S3WorkerLoopsVitality>());
var loopsWatchdog = new Shared.Core.Hosting.WatchdogOptions();
builder.Configuration.GetSection("S3Worker:Loops:Watchdog").Bind(loopsWatchdog);
builder.Services.AddLoopWatchdog(
    loopsWatchdog,
    (sp, loop) => sp.GetRequiredService<Shared.Metrics.Worker.WorkerMetricsInstrumentation>()
        .WatchdogRestart(loop));
// fail-fast инварианта живости (arch/24 §8): тики быстрых циклов чаще порога
// сноса (Multiplier × CheckIntervalSec); конфиг watchdog не меняется
var loopsCfg = builder.Configuration.GetSection("S3Worker:Loops").Get<LoopsOptions>() ?? new LoopsOptions();
if (loopsWatchdog.Enabled)
    Shared.Core.Hosting.WatchdogConfigGuard.EnsureFastLoopsBelowStaleThreshold(
        loopsCfg.ScanIntervalSec, loopsCfg.KeepaliveSec, loopsWatchdog);

var app = builder.Build();
if (app.Services.GetRequiredService<IOptions<S3WorkerOptions>>().Value.Api.Tls.AllowInsecureHttp)
    app.Logger.LogWarning(
        "S3Worker:Api:Tls:AllowInsecureHttp=true — HTTP без TLS (ТОЛЬКО WAF-тесты, arch/24 §1.1)");
if (apiTls.Source is { } certSource)
    app.Logger.LogInformation("S3Worker:Api:Tls: серверный серт API — источник {Source}", certSource);
if (apiTls.Warning is { } certWarning)
    app.Logger.LogWarning("S3Worker:Api:Tls: {Warning}", certWarning);
app.MapAppMetrics();
app.MapHealthChecks("/healthz");
app.MapWorkerApi();

await app.RunAsync();

// Опции процессов из дерева конфигурации (arch/24 §8; порт valkey).
static S3Worker.Provisioning.Processes.S3ProvisioningOptions ToProvisioningOptions(S3WorkerOptions opts) => new(
    opts.Docker.PortRange.From,
    opts.Docker.PortRange.To,
    opts.Thresholds.NodeBootSec,
    opts.Thresholds.NodeDeadSec,
    opts.Thresholds.HealthIntervalSec,
    opts.Docker.AdvertisedHost,
    opts.Docker.Images.OwnS3,
    opts.Bucket,
    opts.Docker.ContourNetwork,
    opts.Thresholds.SyncIntervalSec,
    opts.Thresholds.SyncMaxPages);

// Делегат снапшота для процессов (P12 «до/после» в точках изменений).
static Func<CancellationToken, Task<Result>> SnapshotDelegate(SnapshotJob job)
    => async ct => await job.TakeAsync(ct);

// WAF-тесты (S3Worker.IntegrationTests/Api, Task 15): точка входа как public partial.
public partial class Program;
