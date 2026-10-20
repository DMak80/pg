using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using EtcdWorker.App;
using Shared.Metrics;
using EtcdWorker.App.Api;
using EtcdWorker.App.Api.Operations;
using EtcdWorker.App.HealthChecks;
using EtcdWorker.App.Loops;
using Shared.Core.HealthChecks;
using Shared.Core.Hosting;
using Shared.Etcd.Client;

// Точка входа EtcdWorker (arch/25 §8): host-builder с mTLS-гранью HTTP API
// (вкл. /healthz), конфигурация appsettings+env, DI слоёв. Per-install
// env-секреты — TLS API (ETCDW_API_TLS_*), docker-транспорт (ETCDW_DOCKER_*).

var builder = WebApplication.CreateBuilder(args);

// Конфигурация: appsettings.json + env-оверрайды EtcdWorker__*.
builder.Services.Configure<EtcdWorkerOptions>(builder.Configuration.GetSection("EtcdWorker"));
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<HealthState>();
// Прогресс-отметки долгих фаз в глубину процессов (health-ожидание провижининга).
builder.Services.AddSingleton<Shared.Core.Hosting.ILoopProgress>(sp => sp.GetRequiredService<HealthState>());

// Метрики (arch/18 §2.2): /metrics на том же mTLS-Kestrel-порту, что /healthz —
// scrape ходит клиентским сертом per-install пакета. Meter "EtcdWorker" —
// etcdworker_*-метрики домена (arch/25 §7).
builder.Services.AddAppMetrics("EtcdWorker", builder.Configuration.GetSection("EtcdWorker:Metrics"));
builder.Services.AddSingleton(sp =>
{
    var m = new Shared.Metrics.Worker.WorkerMetricsInstrumentation(
        sp.GetRequiredService<System.Diagnostics.Metrics.Meter>(),
        sp.GetRequiredService<TimeProvider>());
    // Единый seam фаз/операций (S2, зеркало PgWorker/Kafka/Valkey): терминальные
    // фазы/first-seen/подавление supervise — внутри OnJournalPhase. Журнал фаз
    // состава один (домен /etcdworker) — кластер-параметр константа "etcd".
    var etcdJournal = sp.GetRequiredService<EtcdWorker.Provisioning.EtcdWorkJournal>();
    etcdJournal.PhaseWritten += e => m.OnJournalPhase("etcd", e.Op, e.Phase);
    return m;
});

// etcd-клиент: HTTP JSON gateway /v3/*; handler против DNS-флейпа Docker
// embedded DNS (t09): PooledConnectionLifetime + IPv4-first резолв.
// Поллинг-инвариант (arch/25 §6): одиночный HTTP-вызов к etcd не молчит дольше
// окна проверки watchdog — таймаут = половина окна (7.5 c при дефолтах).
builder.Services.AddHttpClient("etcd", c => c.Timeout = TimeSpan.FromSeconds(7.5))
    .ConfigurePrimaryHttpMessageHandler(EtcdConnectCallback.CreateHandler);

// env-секреты docker-транспорта → конфиг-дерево (до валидаций).
DockerEnvBindings.ApplyTlsEnvOverrides(builder.Configuration);
DockerEnvBindings.ApplySshEnvOverrides(builder.Configuration);

// Fail-fast при старте (arch/25 §8): чистые предикаты EtcdStartupChecks —
// endpoints etcd, advertise, docker-топология, пороги, порт-диапазон, слепки.
builder.Services.AddOptions<EtcdWorkerOptions>()
    .Validate(o => EtcdStartupChecks.EtcdEndpoints(o),
        "EtcdWorker:Etcd:Endpoints are not configured (env ETCDW_ETCD_ENDPOINT_*)")
    .Validate(o => EtcdStartupChecks.AdvertiseUrl(o),
        "EtcdWorker:Api:AdvertiseUrl is not set or not https:// (env ETCDW_API_ADVERTISE_URL; mTLS-only API — arch/25 §1.1)")
    .Validate(o => EtcdStartupChecks.DockerTopology(o),
        "EtcdWorker:Docker: invalid topology (Swarm → SwarmManager required; Plain → exactly one host in the table and non-empty Network) (env ETCDW_DOCKER_*)")
    .Validate(o => EtcdStartupChecks.Thresholds(o),
        "EtcdWorker:Thresholds: thresholds must be positive, NodeBootSec ≤ 100")
    .Validate(o => EtcdStartupChecks.PortRange(o),
        "EtcdWorker:Docker:PortRange: 0 < From < To ≤ 65535")
    .Validate(o => EtcdStartupChecks.Snapshots(o),
        "EtcdWorker:Snapshots: intervals must be positive, retentions ≥ 1")
    .ValidateOnStart();

// Управляемый серт API (arch/25 §1.1): чтение ключа /workers/api_tls/etcdworker
// ДО поднятия Kestrel; приоритет etcd > env; битый ключ — fail-fast.
var etcdEndpoints = builder.Configuration.GetSection("EtcdWorker:Etcd:Endpoints").Get<string[]>() ?? [];
var managedCert = await WorkerApiCertReader.ReadAsync(etcdEndpoints, "etcdworker", CancellationToken.None);

// mTLS HTTP API (arch/25 §1.1): env-секреты TLS → конфиг, Kestrel c
// серверным сертом и требованием клиентского серта per-install API-CA.
TlsEndpoints.ApplyEnvOverrides(builder.Configuration);
var apiTls = TlsEndpoints.ConfigureMtls(builder, port: 8080, managedCert);

// Thumbprint применённого серта → дискавери-ключ (arch/25 §1.1): панель
// сверяет с целевым → applied/pending restart.
var apiCertThumbprint = apiTls.ServerCert is { } appliedCert
    ? Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(appliedCert.RawData)).ToLowerInvariant()
    : null;

// etcd-клиент (HTTP JSON gateway /v3/*) + координация (клэймы/лидерство, журнал).
// Единое место литерала префикса etcd-ключей: Shared-координация параметризована
// (arch/25 §8: /etcdworker/{leader,claims,work,portalloc,locks/portalloc,instances,api}).
const string KeyPrefix = "/etcdworker";
builder.Services.AddSingleton<IEtcdGateway>(sp =>
    new EtcdGateway(sp.GetRequiredService<IHttpClientFactory>().CreateClient("etcd")));

// Журнал фаз состава (arch/25 §5): /etcdworker/work — БЕЗ суффикса (домен один).
builder.Services.AddSingleton(sp => new EtcdWorker.Provisioning.EtcdWorkJournal(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<EtcdWorkerOptions>>().Value.Etcd.Endpoints));

builder.Services.AddSingleton(sp => new ClaimStore(
    KeyPrefix,
    sp.GetRequiredService<IOptions<EtcdWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<IOptions<EtcdWorkerOptions>>().Value.Api.AdvertiseUrl,
    apiCertThumbprint));
// Глобальный portalloc-клэйм (arch/25 §5) — DI-синглтон, InstanceId единый с
// ClaimStore (сквозная диагностика держателя); потребляется MembershipProcess
// (окно аллокации client-порта + member-add).
builder.Services.AddSingleton(sp => new PortAllocLock(
    KeyPrefix,
    sp.GetRequiredService<IOptions<EtcdWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<ClaimStore>().InstanceId));

// HTTP API хендлеры (arch/25 §1.1) + счётчик операций — зеркало S3Worker.
builder.Services.AddSingleton(sp => new EtcdApiMetrics(
    sp.GetRequiredService<System.Diagnostics.Metrics.Meter>()));
builder.Services.AddSingleton(sp => new PutEtcdConfigHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<EtcdWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<TimeProvider>()));
// Graceful self-stop (arch/25 §1.1): рестарт из панели применяет серт/конфиг.
builder.Services.AddSingleton(sp => new RestartHandler(
    sp.GetRequiredService<IHostApplicationLifetime>(),
    sp.GetRequiredService<ILoggerFactory>().CreateLogger<RestartHandler>()));

// Циклы: keepalive (продление lease'ов + instance/api-ключи, arch/24 §6)
// и слепки (arch/25 §4): SnapshotJob без sink — цикл сам экспортирует;
// цель — живой резолвер etcd-backup кластера, S3-клиент per-попытка (первый
// endpoint списка), CA-bundle/таймаут из Snapshots-опций; пульс конечного
// S3-вызова (arch/14 §6) — активность snapshot-цикла вокруг выгрузки.
builder.Services.AddSingleton<KeepaliveLoop>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<KeepaliveLoop>());
builder.Services.AddSingleton(sp => new Shared.Etcd.Maintenance.SnapshotJob(
    sp.GetRequiredService<IEtcdGateway>(),
    etcdEndpoints,
    sp.GetRequiredService<IOptions<EtcdWorkerOptions>>().Value.Snapshots.Dir,
    sp.GetRequiredService<IOptions<EtcdWorkerOptions>>().Value.Snapshots.RetentionFiles,
    sp.GetRequiredService<IOptions<EtcdWorkerOptions>>().Value.Snapshots.MaintenanceIntervalMin));
builder.Services.AddSingleton(sp => new EtcdWorker.Provisioning.EtcdBackupTargetResolver(
    sp.GetRequiredService<IEtcdGateway>(), etcdEndpoints));
builder.Services.AddSingleton(sp =>
{
    var opts = sp.GetRequiredService<IOptions<EtcdWorkerOptions>>().Value;
    return new EtcdWorker.Provisioning.SnapshotExporter(
        sp.GetRequiredService<IEtcdGateway>(), etcdEndpoints,
        sp.GetRequiredService<EtcdWorker.Provisioning.EtcdBackupTargetResolver>(),
        t => new EtcdWorker.Core.EtcdSnapshotS3(
            t.Endpoints[0], t.Bucket, t.AccessKey, t.SecretKey,
            opts.Snapshots.S3.CaBundlePath, TimeSpan.FromSeconds(opts.Snapshots.TimeoutSec)),
        opts.Snapshots.RetentionObjects, opts.Snapshots.TimeoutSec,
        sp.GetRequiredService<ClaimStore>().InstanceId, opts.Snapshots.IntervalMin,
        sp.GetRequiredService<HealthState>().MarkSnapshotActivity,
        TimeSpan.FromSeconds(Math.Max(1, opts.Loops.Watchdog.CheckIntervalSec) / 2));
});
builder.Services.AddSingleton<SnapshotLoop>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<SnapshotLoop>());

// Состав контура (arch/25 §3): драйвер по Mode (Plain — единственный
// хост; Swarm — manager), порт-механика + провижинер + процессы + цикл.
builder.Services.AddSingleton<EtcdWorker.Docker.Drivers.IEtcdNodeDriver>(sp =>
{
    var o = sp.GetRequiredService<IOptions<EtcdWorkerOptions>>().Value;
    var factory = sp.GetRequiredService<DockerEngineFactory>();
    return string.Equals(o.Docker.Mode, "Swarm", StringComparison.OrdinalIgnoreCase)
        ? new EtcdWorker.Docker.Drivers.SwarmEtcdNodeDriver(
            o.Docker.SwarmManager!, factory, null, o.Docker.Network)
        : new EtcdWorker.Docker.Drivers.PlainEtcdNodeDriver(
            o.Docker.Hosts.Select(h => new EtcdWorker.Docker.Drivers.HostEndpoint(h.Name, h.Endpoint)).ToList(),
            factory, o.Docker.Network);
});
builder.Services.AddSingleton(sp =>
{
    var o = sp.GetRequiredService<IOptions<EtcdWorkerOptions>>().Value;
    return new EtcdWorker.Core.EtcdProvisioningOptions(
        o.Docker.Mode, o.Docker.Network, o.Docker.Images.Etcd, o.Docker.AdvertisedHost,
        o.Docker.PortRange.From, o.Docker.PortRange.To,
        o.Docker.Hosts.Length > 0 ? o.Docker.Hosts[0].Name : "local",
        o.Thresholds.NodeBootSec, o.Thresholds.NodeDeadSec, o.Thresholds.HealthIntervalSec);
});
builder.Services.AddSingleton(sp => new EtcdWorker.Provisioning.EtcdPortAllocStore(
    sp.GetRequiredService<IEtcdGateway>(), etcdEndpoints));
builder.Services.AddSingleton(sp => new EtcdWorker.Provisioning.EtcdNodeProvisioner(
    sp.GetRequiredService<EtcdWorker.Docker.Drivers.IEtcdNodeDriver>(),
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<EtcdWorkerOptions>>().Value.Thresholds.NodeBootSec,
    sp.GetRequiredService<Shared.Core.Hosting.ILoopProgress>()));
builder.Services.AddSingleton(sp => new EtcdWorker.Provisioning.Processes.MembershipProcess(
    sp.GetRequiredService<IEtcdGateway>(), etcdEndpoints,
    sp.GetRequiredService<EtcdWorker.Provisioning.EtcdWorkJournal>(),
    sp.GetRequiredService<EtcdWorker.Docker.Drivers.IEtcdNodeDriver>(),
    sp.GetRequiredService<EtcdWorker.Provisioning.EtcdPortAllocStore>(),
    sp.GetRequiredService<EtcdWorker.Provisioning.EtcdNodeProvisioner>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<EtcdWorker.Core.EtcdProvisioningOptions>(),
    sp.GetRequiredService<PortAllocLock>()));
builder.Services.AddSingleton(sp => new EtcdWorker.Provisioning.Processes.NodeSupervisor(
    sp.GetRequiredService<IEtcdGateway>(), etcdEndpoints,
    sp.GetRequiredService<EtcdWorker.Provisioning.EtcdWorkJournal>(),
    sp.GetRequiredService<EtcdWorker.Docker.Drivers.IEtcdNodeDriver>(),
    sp.GetRequiredService<EtcdWorker.Provisioning.EtcdNodeProvisioner>(),
    sp.GetRequiredService<EtcdWorker.Provisioning.EtcdPortAllocStore>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<EtcdWorker.Core.EtcdProvisioningOptions>(),
    sp.GetRequiredService<TimeProvider>()));
builder.Services.AddSingleton<ReconcileLoop>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<ReconcileLoop>());

// Наблюдаемость (arch/25 §7): агрегированный health (активные пробы
// ServiceProbes) + per-loop обёртка. DockerEngineFactory — docker-побы.
builder.Services.AddSingleton<DockerEngineFactory>();
builder.Services.AddSingleton<ServiceProbes>();
builder.Services.AddSingleton<EtcdWorkerHealth>();
builder.Services.AddSingleton<HealthCheckAbstract<KeepaliveLoop>>(
    sp => new(sp.GetRequiredService<KeepaliveLoop>()));
builder.Services.AddSingleton<HealthCheckAbstract<SnapshotLoop>>(
    sp => new(sp.GetRequiredService<SnapshotLoop>()));
builder.Services.AddSingleton<HealthCheckAbstract<ReconcileLoop>>(
    sp => new(sp.GetRequiredService<ReconcileLoop>()));
builder.Services.AddHealthChecks()
    .AddCheck<EtcdWorkerHealth>("etcdworker", tags: ["ready"])
    .AddCheck<HealthCheckAbstract<KeepaliveLoop>>("keepalive-loop")
    .AddCheck<HealthCheckAbstract<SnapshotLoop>>("snapshot-loop")
    .AddCheck<HealthCheckAbstract<ReconcileLoop>>("reconcile-loop");

// Watchdog зависших циклов (arch/25 §6): staleness активности (тик или
// прогресс-отметка) всех циклов → журнал + метрика + graceful self-stop;
// Enabled=false (секция EtcdWorker:Loops:Watchdog) — компонент не регистрируется.
builder.Services.AddSingleton<EtcdWorkerLoopsVitality>();
builder.Services.AddSingleton<Shared.Core.Hosting.ILoopsVitality>(
    sp => sp.GetRequiredService<EtcdWorkerLoopsVitality>());
var loopsWatchdog = new Shared.Core.Hosting.WatchdogOptions();
builder.Configuration.GetSection("EtcdWorker:Loops:Watchdog").Bind(loopsWatchdog);
builder.Services.AddLoopWatchdog(
    loopsWatchdog,
    (sp, loop) => sp.GetRequiredService<Shared.Metrics.Worker.WorkerMetricsInstrumentation>()
        .WatchdogRestart(loop));
// fail-fast инварианта живости (arch/25 §8): тики быстрых циклов чаще порога
// сноса (Multiplier × CheckIntervalSec); конфиг watchdog не меняется
var loopsCfg = builder.Configuration.GetSection("EtcdWorker:Loops").Get<LoopsOptions>() ?? new LoopsOptions();
if (loopsWatchdog.Enabled)
    Shared.Core.Hosting.WatchdogConfigGuard.EnsureFastLoopsBelowStaleThreshold(
        loopsCfg.ScanIntervalSec, loopsCfg.KeepaliveSec, loopsWatchdog);

var app = builder.Build();
if (app.Services.GetRequiredService<IOptions<EtcdWorkerOptions>>().Value.Api.Tls.AllowInsecureHttp)
    app.Logger.LogWarning(
        "EtcdWorker:Api:Tls:AllowInsecureHttp=true — HTTP without TLS (WAF tests ONLY, arch/25 §1.1)");
if (apiTls.Source is { } certSource)
    app.Logger.LogInformation("EtcdWorker:Api:Tls: API server certificate — source {Source}", certSource);
if (apiTls.Warning is { } certWarning)
    app.Logger.LogWarning("EtcdWorker:Api:Tls: {Warning}", certWarning);
app.MapAppMetrics();
app.MapHealthChecks("/healthz");
app.MapWorkerApi();

await app.RunAsync();

// WAF-тесты: точка входа как public partial.
public partial class Program;
