using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PgWorker.App;
using PgWorker.App.Api;
using PgWorker.App.Api.Operations;
using PgWorker.App.HealthChecks;
using PgWorker.App.Loops;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Docker.Drivers;
using PgWorker.Docker.Engine;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.Moves;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Probes;
using PgWorker.Provisioning.Processes;
using PgWorker.Provisioning.Snapshots;
using PgWorker.Provisioning.Sql;
using Shared.Metrics;
using ProcessThresholds = PgWorker.Provisioning.Processes.ThresholdsOptions;

// Точка входа PgWorker (задача 23–24): host-builder с HTTP-granью /healthz,
// конфигурация appsettings+env, DI всех слоёв (etcd → координация → docker →
// процессы → циклы). Секреты установки — ТОЛЬКО env (Д7), fail-fast при отсутствии.

var builder = WebApplication.CreateBuilder(args);

// Конфигурация: appsettings.json + env-оверрайды PgWorker__* (пример — в корне проекта).
builder.Services.Configure<PgWorkerOptions>(builder.Configuration.GetSection("PgWorker"));
// Fail-fast: ключ доступа /pgworker/api/<id> без URL бессмысленен (arch/14 §1.1).
builder.Services.AddOptions<PgWorkerOptions>()
    .Validate(o => !string.IsNullOrWhiteSpace(o.Api.AdvertiseUrl),
        "PgWorker:Api:AdvertiseUrl не задан (URL API, достижимый панелью; env PGW_API_ADVERTISE_URL)")
    .ValidateOnStart();
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<HealthState>();

// Метрики (arch/18 §3): /metrics на том же Kestrel-порту, что /healthz;
// ApiKeyMiddleware защищает только /api — scrape-грань открыта (доверенная сеть).
builder.Services.AddAppMetrics("PgWorker", builder.Configuration.GetSection("PgWorker:Metrics"));
builder.Services.AddSingleton(sp => new Shared.Metrics.Worker.WorkerMetricsInstrumentation(
    sp.GetRequiredService<System.Diagnostics.Metrics.Meter>(),
    sp.GetRequiredService<TimeProvider>()));

// Секреты per-install (Д7, spec §10): не в git, не в etcd — только env процесса.
builder.Services.AddSingleton(_ => SecretsFromEnv());

builder.Services.AddHttpClient("etcd");
builder.Services.AddHttpClient("patroni");

// etcd-клиент (HTTP JSON gateway /v3/*) + координация (клэймы/лидерство, журнал).
builder.Services.AddSingleton<IEtcdGateway>(sp =>
    new EtcdGateway(sp.GetRequiredService<IHttpClientFactory>().CreateClient("etcd")));
builder.Services.AddSingleton(sp => new ClaimStore(
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Api.AdvertiseUrl));
builder.Services.AddSingleton(sp => new WorkJournal(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints));

// HTTP API воркера (arch/14 §1.1): мутации декларативного контракта — хендлеры-синглтоны.
builder.Services.AddSingleton(sp => new CreateClusterHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<TimeProvider>()));
builder.Services.AddSingleton(sp => new DeleteClusterHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints));
builder.Services.AddSingleton(sp => new AddShardHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints));
builder.Services.AddSingleton(sp => new DeleteShardHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints));
builder.Services.AddSingleton(sp => new MoveBucketsHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<TimeProvider>()));
builder.Services.AddSingleton(sp => new RollbackBucketsHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<TimeProvider>()));
builder.Services.AddSingleton(sp => new FinalizeBucketHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<TimeProvider>()));
builder.Services.AddSingleton(sp => new AbortBucketHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Moves.ToRuntime(
        sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Thresholds)));
builder.Services.AddSingleton(sp => new CancelMoveHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints));
builder.Services.AddSingleton(sp => new RotateAppPasswordHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<TimeProvider>()));
builder.Services.AddSingleton(sp => new RecreateNodeHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints));
// Демо-сид (arch/14 §1.1.1): стендовый эндпоинт за флагом EnableSeedEndpoint.
builder.Services.AddSingleton(sp => new SeedDemoHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Api.EnableSeedEndpoint));

// docker: драйвер по режиму (Plain: таблица Hosts; Swarm: manager endpoint).
// AdvertisedHost (advertised-правило arch/16): только Plain + ровно один хост —
// advertised-имя одно на таблицу, при мульти-хосте порты разных хостов склеились
// бы в один namespace адресов (fail-fast, а не молчаные коллизии).
builder.Services.AddSingleton<DockerEngineFactory>();
builder.Services.AddSingleton<IClusterDriver>(sp =>
{
    var docker = sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Docker;
    var factory = sp.GetRequiredService<DockerEngineFactory>();
    var advertised = docker.AdvertisedHost;
    if (!string.IsNullOrWhiteSpace(advertised))
    {
        if (string.Equals(docker.Mode, "Swarm", StringComparison.OrdinalIgnoreCase))
            throw new ApplicationException(
                "PgWorker:Docker:AdvertisedHost не поддерживается в Mode=Swarm (multi-host адресация)");
        if (docker.Hosts.Length != 1)
            throw new ApplicationException(
                "PgWorker:Docker:AdvertisedHost требует ровно один хост в PgWorker:Docker:Hosts (single-host/tunnel)");
    }

    if (string.Equals(docker.Mode, "Swarm", StringComparison.OrdinalIgnoreCase))
    {
        if (string.IsNullOrWhiteSpace(docker.SwarmManager))
            throw new ApplicationException("PgWorker:Docker:Mode=Swarm требует PgWorker:Docker:SwarmManager");
        return new SwarmClusterDriver(docker.SwarmManager, factory, docker.EnableDoorman, docker.Images.Node);
    }

    var hosts = docker.Hosts
        .Select(h => new HostEndpoint(h.Name, h.Endpoint))
        .ToList();
    if (hosts.Count == 0)
        throw new ApplicationException("PgWorker:Docker:Mode=Plain требует непустую таблицу PgWorker:Docker:Hosts");
    return new PlainClusterDriver(hosts, factory, docker.EnableDoorman, docker.Images.Node, docker.AdvertisedHost);
});

// Пробы Patroni REST и SQL-слой (Npgsql + Polly-ретраи).
builder.Services.AddSingleton(sp =>
    new ShardProbe(sp.GetRequiredService<IHttpClientFactory>().CreateClient("patroni")));
builder.Services.AddSingleton<ISqlExecutor, DatabaseProvisioner>();

// Снапшоты P12 (SnapshotLoop-лидер + процессы в точках изменений).
builder.Services.AddSingleton(sp =>
{
    var opts = sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value;
    return new SnapshotJob(
        sp.GetRequiredService<IEtcdGateway>(), opts.Etcd.Endpoints,
        opts.Snapshots.Dir, opts.Snapshots.RetentionFiles, opts.Snapshots.MaintenanceIntervalMin);
});

// Процессы-машины состояний (§6.4): снапшот передаётся делегатом от SnapshotJob.
// EtcdEndpoints для КОНТЕЙНЕРОВ нод — из AdvertisedEndpoints (ноды ходят в etcd
// через docker-сеть, а не через endpoint'ы самого PgWorker).
builder.Services.AddSingleton(sp => new EtcdEndpoints(
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.AdvertisedEndpoints is { Length: > 0 } advertised
        ? advertised
        : sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints));
// Индекс занятости портов из portalloc всех кластеров (spec §3.3): busy = docker ∪ etcd-записи.
builder.Services.AddSingleton(sp => new PortAllocIndex(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints.ToArray(),
    sp.GetRequiredService<ILogger<PortAllocIndex>>()));
// Глобальный portalloc-клэйм (t90, arch/14 §2.4/§3.3): взаимоисключение секции
// довыделения портов между кластерами/инстансами; instance = InstanceId ClaimStore.
builder.Services.AddSingleton(sp => new PortAllocLock(
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints.ToArray(),
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<ClaimStore>().InstanceId));
builder.Services.AddSingleton(sp =>
{
    var opts = sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value;
    var endpoints = opts.Etcd.Endpoints;
    var job = sp.GetRequiredService<SnapshotJob>();
    return new ProvisioningProcess(
        sp.GetRequiredService<IEtcdGateway>(), endpoints,
        sp.GetRequiredService<IClusterDriver>(), sp.GetRequiredService<ISqlExecutor>(),
        sp.GetRequiredService<ShardProbe>(), sp.GetRequiredService<ClaimStore>(),
        sp.GetRequiredService<WorkJournal>(),
        new PlacementOptions(opts.Docker.PortRange.From, opts.Docker.PortRange.To, opts.Thresholds.PatroniBootSec,
            opts.Thresholds.ProvisionRetryBaseSec, opts.Thresholds.ProvisionRetryMaxSec),
        sp.GetRequiredService<InstallSecrets>(),
        sp.GetRequiredService<IAppSecretEnsurer>(),
        sp.GetRequiredService<IAppParamsEnsurer>(),
        sp.GetRequiredService<EtcdEndpoints>(),
        sp.GetRequiredService<PortAllocIndex>(),
        sp.GetRequiredService<PortAllocLock>(),
        SnapshotDelegate(job));
});
builder.Services.AddSingleton(sp => new DeprovisioningProcess(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IClusterDriver>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    SnapshotDelegate(sp.GetRequiredService<SnapshotJob>())));
builder.Services.AddSingleton(sp => new NodeSupervisor(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IClusterDriver>(),
    sp.GetRequiredService<ShardProbe>(),
    sp.GetRequiredService<ISqlExecutor>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    new ProcessThresholds(sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Thresholds.NodeDeadSec,
        sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Thresholds.ShardDeadSec),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<InstallSecrets>(),
    sp.GetRequiredService<IAppParamsEnsurer>(),
    new MasterKeyReconciler(
        sp.GetRequiredService<IEtcdGateway>(),
        sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
        sp.GetRequiredService<ShardProbe>()),
    sp.GetRequiredService<EtcdEndpoints>()));
// Адресация шардов (t01 задача 9): master-ключ/portalloc + DSN-билдеры —
// общий сервис эвакуатора и процессов переезда (MoveProcess — задача 17).
builder.Services.AddSingleton(sp => new ShardEndpoints(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<ShardProbe>()));

// Усыновление кластеров (adopt-repair spec §3.2 + §3.7 Д2): адреса из HA-контура+docker →
// portalloc; инвариант адресов Active (AD2') + ensure секретов/ролей — общие ensurer'ы выше.
builder.Services.AddSingleton(sp =>
{
    var opts = sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value;
    return new AdoptionProcess(
        sp.GetRequiredService<IEtcdGateway>(),
        opts.Etcd.Endpoints,
        sp.GetRequiredService<IClusterDriver>(),
        sp.GetRequiredService<ShardEndpoints>(),
        sp.GetRequiredService<ISqlExecutor>(),
        sp.GetRequiredService<IAppSecretEnsurer>(),
        sp.GetRequiredService<IAppParamsEnsurer>(),
        sp.GetRequiredService<InstallSecrets>(),
        sp.GetRequiredService<ClaimStore>(),
        sp.GetRequiredService<WorkJournal>(),
        sp.GetRequiredService<PortAllocIndex>(),
        sp.GetRequiredService<PortAllocLock>(),
        new PlacementOptions(opts.Docker.PortRange.From, opts.Docker.PortRange.To, opts.Thresholds.PatroniBootSec,
            opts.Thresholds.ProvisionRetryBaseSec, opts.Thresholds.ProvisionRetryMaxSec),
        sp.GetRequiredService<EtcdEndpoints>(),
        SnapshotDelegate(sp.GetRequiredService<SnapshotJob>()));
});

// Ensure per-cluster app-секрета (spec §4.1): чтение/txn put-if-absent
// /clusters/<C>/{app_user,app_password} — общий для Provisioning/AddShard.
builder.Services.AddSingleton<IAppSecretEnsurer>(sp => new AppSecretEnsurer(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints));

// Ensure per-node app_params (spec §4.2): put-if-absent дефолта — общий для
// Provisioning (P2.5')/AddShard (A5)/надзора (миграция C).
builder.Services.AddSingleton<IAppParamsEnsurer>(sp => new AppParamsEnsurer(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.AppParams.Default));
builder.Services.AddSingleton(sp => new BucketEvacuator(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IClusterDriver>(),
    sp.GetRequiredService<ISqlExecutor>(),
    sp.GetRequiredService<ShardProbe>(),
    sp.GetRequiredService<ShardEndpoints>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<InstallSecrets>(),
    SnapshotDelegate(sp.GetRequiredService<SnapshotJob>())));

// Scale-процессы шардов (t06): подъём/демонтаж отдельного шарда Active-кластера.
builder.Services.AddSingleton(sp =>
{
    var opts = sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value;
    return new AddShardProcess(
        sp.GetRequiredService<IEtcdGateway>(), opts.Etcd.Endpoints,
        sp.GetRequiredService<IClusterDriver>(), sp.GetRequiredService<ISqlExecutor>(),
        sp.GetRequiredService<ShardProbe>(), sp.GetRequiredService<ClaimStore>(),
        sp.GetRequiredService<WorkJournal>(),
        new PlacementOptions(opts.Docker.PortRange.From, opts.Docker.PortRange.To, opts.Thresholds.PatroniBootSec,
            opts.Thresholds.ProvisionRetryBaseSec, opts.Thresholds.ProvisionRetryMaxSec),
        sp.GetRequiredService<InstallSecrets>(),
        sp.GetRequiredService<IAppSecretEnsurer>(),
        sp.GetRequiredService<IAppParamsEnsurer>(),
        sp.GetRequiredService<EtcdEndpoints>(),
        sp.GetRequiredService<PortAllocIndex>(),
        sp.GetRequiredService<PortAllocLock>(),
        SnapshotDelegate(sp.GetRequiredService<SnapshotJob>()));
});
builder.Services.AddSingleton(sp => new RemoveShardProcess(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IClusterDriver>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    SnapshotDelegate(sp.GetRequiredService<SnapshotJob>())));

// Переезды бакетов (t01 задача 17): SQL-слой Npgsql+Polly, DDL через docker exec,
// машина состояний MoveProcess (M0–M6/rollback/finalize/abort); runtime-опции —
// склейка секций Moves + Thresholds; TimeProvider/System — источник unix-времени
// статусов; снапшот-делегат — точки «до/после» P12.
builder.Services.AddSingleton<IMoveSqlExecutor, NpgsqlMoveSqlExecutor>();
builder.Services.AddSingleton(sp => new MoveDdl(
    sp.GetRequiredService<IClusterDriver>(),
    sp.GetRequiredService<IMoveSqlExecutor>()));
builder.Services.AddSingleton(sp =>
{
    var opts = sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value;
    return new MoveProcess(
        sp.GetRequiredService<IEtcdGateway>(), opts.Etcd.Endpoints,
        sp.GetRequiredService<IMoveSqlExecutor>(),
        sp.GetRequiredService<MoveDdl>(),
        sp.GetRequiredService<IClusterDriver>(),
        sp.GetRequiredService<ShardEndpoints>(),
        sp.GetRequiredService<ClaimStore>(),
        sp.GetRequiredService<WorkJournal>(),
        sp.GetRequiredService<InstallSecrets>(),
        opts.Moves.ToRuntime(opts.Thresholds),
        sp.GetRequiredService<TimeProvider>(),
        sp.GetRequiredService<ILoggerFactory>().CreateLogger<MoveProcess>(),
        SnapshotDelegate(sp.GetRequiredService<SnapshotJob>()));
});

// Репарация брошенных переездов (adopt-repair spec §3.5): синтетические заявки
// put-if-absent в существующий MoveProcess; пороги = панельные алерты.
builder.Services.AddSingleton(sp => new MoveRepairProcess(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Moves.ToRuntime(
        sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Thresholds),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<ILoggerFactory>().CreateLogger<MoveRepairProcess>()));

// Ротация app-пароля (spec §4.3, arch/14 §5 I): заявка /pgworker/rotations/<C>;
// Active-ветка цикла зовёт через ClusterProcesses (scale → rotate → evacuate → moves).
builder.Services.AddSingleton(sp => new AppPasswordRotator(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<ISqlExecutor>(),
    sp.GetRequiredService<ShardProbe>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<InstallSecrets>(),
    sp.GetRequiredService<IAppSecretEnsurer>(),
    SnapshotDelegate(sp.GetRequiredService<SnapshotJob>())));

// Циклы (§6.2): keepalive первым (lease живут до Reconcile), затем снапшоты и reconcile.
// Регистрируются синглтонами — health-обёртки читают их состояние напрямую.
builder.Services.AddSingleton<IClusterProcesses, ClusterProcesses>();
builder.Services.AddSingleton<KeepaliveLoop>();
builder.Services.AddSingleton<SnapshotLoop>();
builder.Services.AddSingleton<ReconcileLoop>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<KeepaliveLoop>());
builder.Services.AddHostedService(sp => sp.GetRequiredService<SnapshotLoop>());
builder.Services.AddHostedService(sp => sp.GetRequiredService<ReconcileLoop>());

// Наблюдаемость §8: агрегированный health (etcd/docker/loops/claims/snapshot)
// + per-loop обёртки паттерна Puzzle (Inited/Working/StatusError).
builder.Services.AddSingleton<ServiceProbes>();
builder.Services.AddSingleton<PgWorkerHealth>();
builder.Services.AddSingleton<HealthCheckAbstract<ReconcileLoop>>(
    sp => new(sp.GetRequiredService<ReconcileLoop>()));
builder.Services.AddSingleton<HealthCheckAbstract<KeepaliveLoop>>(
    sp => new(sp.GetRequiredService<KeepaliveLoop>()));
builder.Services.AddSingleton<HealthCheckAbstract<SnapshotLoop>>(
    sp => new(sp.GetRequiredService<SnapshotLoop>()));
builder.Services.AddHealthChecks()
    .AddCheck<PgWorkerHealth>("pgworker", tags: ["ready"])
    .AddCheck<HealthCheckAbstract<ReconcileLoop>>("reconcile-loop")
    .AddCheck<HealthCheckAbstract<KeepaliveLoop>>("keepalive-loop")
    .AddCheck<HealthCheckAbstract<SnapshotLoop>>("snapshot-loop");

var app = builder.Build();
app.UseMiddleware<ApiKeyMiddleware>();
app.MapAppMetrics();
app.MapHealthChecks("/healthz");
app.MapWorkerApi();

await app.RunAsync();

// Секреты из env с fail-fast: отсутствующий секрет — ошибка старта (Д7).
static InstallSecrets SecretsFromEnv()
{
    string Required(string name) =>
        Environment.GetEnvironmentVariable(name)
        ?? throw new ApplicationException(
            $"не задан обязательный env-секрет {name} (Д7: per-install, не в git и не в etcd)");

    return new InstallSecrets(
        Required("PGW_PG_SUPERUSER_PASSWORD"),
        Required("PGW_PG_STANDBY_PASSWORD"),
        Required("PGW_BUCKET_ADMIN_PASSWORD"),
        Required("PGW_BUCKET_MOVER_PASSWORD"));
}

// Делегат снапшота для процессов (P12 «до/после» в точках изменений).
static Func<CancellationToken, Task<Result>> SnapshotDelegate(SnapshotJob job)
    => async ct => await job.TakeAsync(ct);

// WAF-тесты (PgWorker.IntegrationTests/Api): точка входа как public partial.
public partial class Program;
