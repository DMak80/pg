using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PgWorker.App;
using PgWorker.App.Api;
using PgWorker.App.Api.Operations;
using PgWorker.App.HealthChecks;
using PgWorker.App.Loops;
using PgWorker.Backups;
using PgWorker.Backups.Sql;
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

// t03: env-секреты TLS (API / Docker / SSH) → конфиг-дерево до всего остального.
ApiTlsEndpoints.ApplyEnvOverrides(builder.Configuration);
DockerTlsOptions.ApplyEnvOverrides(builder.Configuration);
SshTunnelOptions.ApplyEnvOverrides(builder.Configuration);

// Конфигурация: appsettings.json + env-оверрайды PgWorker__* (пример — в корне проекта).
builder.Services.Configure<PgWorkerOptions>(builder.Configuration.GetSection("PgWorker"));
// Fail-fast: advertise без URL бессмысленен; http-advertise запрещён при mTLS-каноне (arch/14 §1.1).
builder.Services.AddOptions<PgWorkerOptions>()
    .Validate(o => !string.IsNullOrWhiteSpace(o.Api.AdvertiseUrl),
        "PgWorker:Api:AdvertiseUrl не задан (URL API, достижимый панелью; env PGW_API_ADVERTISE_URL)")
    .Validate(o => o.Api.Tls.AllowInsecureHttp
        || o.Api.AdvertiseUrl.StartsWith("https://", StringComparison.Ordinal),
        "PgWorker:Api:AdvertiseUrl обязан быть https:// (mTLS-only API, arch/14 §1.1)")
    // Подсистема бэкапов (arch/19, t01): fail-fast включения без S3-комплекта;
    // default Enabled=false — подсистема не активна, поведение не меняется.
    .Validate(o => o.Backups.IsValid(),
        "PgWorker:Backups: Enabled=true требует непустые PgWorker:Backups:S3:Endpoint/Bucket/AccessKey/SecretKey (env PGW_BACKUP_S3_*) и Backups:Job:Image (arch/19 §7/§9)")
    // Расчёт PGTune (spec.md §4.2): мусорный конфиг виден на старте, а не на
    // первом provision'е. desktop запрещён — его wal_level=minimal/max_wal_senders=0
    // несовместимы с P3 (логическое декодирование, переезды бакетов).
    .Validate(o => o.Pgtune.IsValid(),
        "PgWorker:Pgtune: DbVersion 10..18; DbType web|oltp|dw|mixed (desktop запрещён — несовместим с P3); " +
        "HdType ssd|san|hdd|nvme; DbSize less_ram|mid_ram|greater_ram; Connections 20..999999; " +
        "ExcludeParams — только имена вывода PGTune (§5.2). Память/CPU — не здесь: заявки etcd request_{cpu,mem}")
    .ValidateOnStart();

// mTLS HTTP API (arch/14 §1.1, t03): Kestrel с серверным сертом и требованием
// клиентского серта per-install API-CA (порт — из ASPNETCORE_URLS/urls, иначе 8080).
ApiTlsEndpoints.ConfigureMtls(builder);

builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<HealthState>();

// Метрики (arch/18 §3): /metrics на том же mTLS-Kestrel-порту, что /healthz (t03).
builder.Services.AddAppMetrics("PgWorker", builder.Configuration.GetSection("PgWorker:Metrics"));
builder.Services.AddSingleton(sp =>
{
    var m = new Shared.Metrics.Worker.WorkerMetricsInstrumentation(
        sp.GetRequiredService<System.Diagnostics.Metrics.Meter>(),
        sp.GetRequiredService<TimeProvider>());
    // Единый seam фаз/операций (S2): метрики — наблюдатель журнала, точки вызова
    // процессов не трогаем. Терминальные фазы/first-seen/подавление supervise и
    // evacuate — внутри OnJournalPhase (arch/18 §2.2).
    sp.GetRequiredService<WorkJournal>().PhaseWritten += e => m.OnJournalPhase(e.Cluster, e.Op, e.Phase);
    return m;
});

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
builder.Services.AddSingleton(sp => new RotateClusterSecretsHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<TimeProvider>()));
builder.Services.AddSingleton(sp => new BackupsPolicyHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints));
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
builder.Services.AddSingleton(sp =>
{
    var docker = sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Docker;
    return new DockerEngineFactory(
        docker.Tls, docker.Ssh,
        sp.GetRequiredService<ILoggerFactory>().CreateLogger<DockerEngineFactory>(),
        sp.GetRequiredService<ILoggerFactory>());
});
builder.Services.AddSingleton<IClusterDriver>(sp =>
{
    var opts = sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value;
    var docker = opts.Docker;
    var factory = sp.GetRequiredService<DockerEngineFactory>();
    var advertised = docker.AdvertisedHost;
    // ExcludeParams PGTune — параметр конструктора драйвера (spec.md §4.4):
    // per-call канала для exclude нет (EnsureNodeAsync несёт только tuning),
    // SpiloEnvBuilder вырезает их при сборке YAML.
    var pgtuneExclude = new HashSet<string>(opts.Pgtune.ExcludeParams, StringComparer.Ordinal);
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
        return new SwarmClusterDriver(docker.SwarmManager, factory, docker.EnableDoorman, docker.Images.Node,
            pgtuneExclude: pgtuneExclude);
    }

    var hosts = docker.Hosts
        .Select(h => new HostEndpoint(h.Name, h.Endpoint))
        .ToList();
    if (hosts.Count == 0)
        throw new ApplicationException("PgWorker:Docker:Mode=Plain требует непустую таблицу PgWorker:Docker:Hosts");
    return new PlainClusterDriver(hosts, factory, docker.EnableDoorman, docker.Images.Node, docker.AdvertisedHost,
        pgtuneExclude: pgtuneExclude);
});

// Фабрика входов PGTune (spec.md §4.3): runtime-склейка PgWorker:Pgtune
// (валидированы fail-fast'ом старта); расчёт — per-shard на EnsureNode-путях.
builder.Services.AddSingleton(sp => new PgtuneInputsFactory(
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Pgtune.ToRuntime(),
    sp.GetRequiredService<ILogger<PgtuneInputsFactory>>()));

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
        sp.GetRequiredService<IClusterSecretEnsurer>(),
        sp.GetRequiredService<IAppParamsEnsurer>(),
        sp.GetRequiredService<EtcdEndpoints>(),
        sp.GetRequiredService<PortAllocIndex>(),
        sp.GetRequiredService<PortAllocLock>(),
        sp.GetRequiredService<PgtuneInputsFactory>(),
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
        sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Thresholds.ShardDeadSec,
        sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Thresholds.PatroniBootSec),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<InstallSecrets>(),
    sp.GetRequiredService<IAppParamsEnsurer>(),
    sp.GetRequiredService<PgtuneInputsFactory>(),
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
        sp.GetRequiredService<IClusterSecretEnsurer>(),
        sp.GetRequiredService<IAppParamsEnsurer>(),
        sp.GetRequiredService<InstallSecrets>(),
        sp.GetRequiredService<ClaimStore>(),
        sp.GetRequiredService<WorkJournal>(),
        sp.GetRequiredService<PortAllocIndex>(),
        sp.GetRequiredService<PortAllocLock>(),
        new PlacementOptions(opts.Docker.PortRange.From, opts.Docker.PortRange.To, opts.Thresholds.PatroniBootSec,
            opts.Thresholds.ProvisionRetryBaseSec, opts.Thresholds.ProvisionRetryMaxSec),
        sp.GetRequiredService<EtcdEndpoints>(),
        sp.GetRequiredService<PgtuneInputsFactory>(),
        SnapshotDelegate(sp.GetRequiredService<SnapshotJob>()));
});

// Ensure per-cluster app-секрета (spec §4.1): чтение/txn put-if-absent
// /clusters/<C>/{app_user,app_password} — общий для Provisioning/AddShard.
builder.Services.AddSingleton<IClusterSecretEnsurer>(sp => new ClusterSecretEnsurer(
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
        sp.GetRequiredService<IClusterSecretEnsurer>(),
        sp.GetRequiredService<IAppParamsEnsurer>(),
        sp.GetRequiredService<EtcdEndpoints>(),
        sp.GetRequiredService<PortAllocIndex>(),
        sp.GetRequiredService<PortAllocLock>(),
        sp.GetRequiredService<PgtuneInputsFactory>(),
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

// Ротация per-cluster секретов (t02, arch/14 §5 I): заявка /pgworker/rotations/<C>;
// Active-ветка цикла зовёт через ClusterProcesses (scale → rotate → evacuate → moves).
builder.Services.AddSingleton(sp => new ClusterSecretRotator(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<ISqlExecutor>(),
    sp.GetRequiredService<ShardProbe>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<InstallSecrets>(),
    sp.GetRequiredService<IClusterSecretEnsurer>(),
    SnapshotDelegate(sp.GetRequiredService<SnapshotJob>())));

// Полные бэкапы шардов (t02, arch/19): планировщик + супервизия джобов;
// runtime-опции — склейка секции PgWorker:Backups; процесс — no-op при Enabled=false.
builder.Services.AddSingleton(sp => new PgWorker.Backups.BackupProcess(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IClusterDriver>(),
    sp.GetRequiredService<ShardEndpoints>(),
    sp.GetRequiredService<ISqlExecutor>(),
    sp.GetRequiredService<IClusterSecretEnsurer>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<InstallSecrets>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Backups.ToRuntime(),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<ILoggerFactory>().CreateLogger<PgWorker.Backups.BackupProcess>(),
    SnapshotDelegate(sp.GetRequiredService<SnapshotJob>())));

// Восстановление шарда из бэкапа (t05, arch/19 §3.5): PLANNED→RUNNING→
// REJOINING→COMPLETED; plain-only, Exec в объёме джоба. Runtime-опции —
// статичный срез (врезка цикла и процесс гвардят Enabled).
builder.Services.AddSingleton(sp => new PgWorker.Backups.Process.RestoreProcess(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IClusterDriver>(),
    sp.GetRequiredService<IBackupS3>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Backups.ToRuntime(),
    sp.GetRequiredService<InstallSecrets>(),
    sp.GetRequiredService<EtcdEndpoints>(),
    sp.GetRequiredService<IClusterSecretEnsurer>(),
    sp.GetRequiredService<ShardProbe>(),
    new ProcessThresholds(sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Thresholds.NodeDeadSec,
        sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Thresholds.ShardDeadSec,
        sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Thresholds.PatroniBootSec),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<ILoggerFactory>().CreateLogger<PgWorker.Backups.Process.RestoreProcess>()));

// Заявка restore через API (t05 §3.2): гварды + txn put-if-not-exists
// PLANNED-ключа; исполнение — RestoreProcess (держатель клэйма).
builder.Services.AddSingleton(sp => new PgWorker.App.Api.Operations.RestoreShardHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<TimeProvider>()));

// WAL-архивация (t03, arch/19 §3): слот/агент/контроль цепочки; runtime()==null
// (Backups:Enabled=false) — процесс выполняет стоп-семантику и не активен.
builder.Services.AddSingleton(sp =>
{
    var opts = sp.GetRequiredService<IOptionsMonitor<PgWorkerOptions>>().CurrentValue;
    return new WalStreamProcess(
        sp.GetRequiredService<IEtcdGateway>(),
        opts.Etcd.Endpoints,
        sp.GetRequiredService<IClusterDriver>(),
        sp.GetRequiredService<ShardEndpoints>(),
        sp.GetRequiredService<IWalSqlExecutor>(),
        sp.GetRequiredService<IBackupS3>(),
        new WalStatusWriter(sp.GetRequiredService<IEtcdGateway>(), opts.Etcd.Endpoints),
        sp.GetRequiredService<ClaimStore>(),
        sp.GetRequiredService<WorkJournal>(),
        () => sp.GetRequiredService<IOptionsMonitor<PgWorkerOptions>>().CurrentValue.Backups.Enabled
            ? sp.GetRequiredService<IOptionsMonitor<PgWorkerOptions>>().CurrentValue.Backups.ToRuntime()
            : null,
        sp.GetRequiredService<InstallSecrets>(),
        sp.GetRequiredService<TimeProvider>(),
        sp.GetRequiredService<Shared.Metrics.Worker.WorkerMetricsInstrumentation>().BackupWalLag,
        sp.GetRequiredService<ILoggerFactory>().CreateLogger("WalStreamProcess"));
});
builder.Services.AddSingleton<IWalSqlExecutor, NpgsqlWalSqlExecutor>();
// Ретенция (t06, arch/19 §4): GFS/WAL-чистка/гигиена + монитор хранилища;
// Enabled=false — no-op (гвард в процессе дублирует guard цикла).
builder.Services.AddSingleton(sp => new PgWorker.Backups.RetentionProcess(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IBackupS3>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Backups.ToRuntime(),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<ILoggerFactory>().CreateLogger<PgWorker.Backups.RetentionProcess>()));
// S3-клиент с горячей конфигурацией (ревью Ф7 №3): включение/смена секции
// Backups без рестарта воркера пересоздаёт клиента при первом же вызове
// (асимметрия «выключение работает, включение нет» устранена).
builder.Services.AddSingleton<IBackupS3, ReloadableBackupS3>();

// Проверки полных бэкапов (t04, arch/19 §5): цепочка + pg_verifybackup-джобы;
// verifyObserver — counter pgworker_backup_verify_total{result}.
builder.Services.AddSingleton(sp => new PgWorker.Backups.BackupVerifyProcess(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IClusterDriver>(),
    sp.GetRequiredService<ShardEndpoints>(),
    sp.GetRequiredService<IBackupS3>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<IOptionsMonitor<PgWorkerOptions>>().CurrentValue.Backups.ToRuntime(),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<ILoggerFactory>().CreateLogger<PgWorker.Backups.BackupVerifyProcess>(),
    sp.GetRequiredService<Shared.Metrics.Worker.WorkerMetricsInstrumentation>().BackupVerify));

// Сверка S3↔etcd (t07, arch/19 §4): per-cluster чистка мусора full/<id>/ без
// etcd-ключа; runtime-функция через IOptionsMonitor — Enabled=false → no-op
// (образец WalStreamProcess).
builder.Services.AddSingleton(sp => new PgWorker.Backups.Supervisor.BackupSupervisorProcess(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IBackupS3>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    () => sp.GetRequiredService<IOptionsMonitor<PgWorkerOptions>>().CurrentValue.Backups.Enabled
        ? sp.GetRequiredService<IOptionsMonitor<PgWorkerOptions>>().CurrentValue.Backups.ToRuntime()
        : null,
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<ILoggerFactory>().CreateLogger<PgWorker.Backups.Supervisor.BackupSupervisorProcess>()));

// Глобальный проход сирот S3 (t07, arch/19 §4): реестр /pgworker/backups/orphans
// + TTL-удаление; пишет только лидер /pgworker/leader (BackupOrphanSweeperLoop).
builder.Services.AddSingleton(sp => new PgWorker.Backups.Supervisor.BackupOrphanSweeper(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IBackupS3>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    () => sp.GetRequiredService<IOptionsMonitor<PgWorkerOptions>>().CurrentValue.Backups.Enabled
        ? sp.GetRequiredService<IOptionsMonitor<PgWorkerOptions>>().CurrentValue.Backups.ToRuntime()
        : null,
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<ILoggerFactory>().CreateLogger<PgWorker.Backups.Supervisor.BackupOrphanSweeper>()));
builder.Services.AddSingleton<PgWorker.App.Loops.BackupOrphanSweeperLoop>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<PgWorker.App.Loops.BackupOrphanSweeperLoop>());

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
if (app.Services.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Api.Tls.AllowInsecureHttp)
    app.Logger.LogWarning(
        "PgWorker:Api:Tls:AllowInsecureHttp=true — HTTP без TLS (ТОЛЬКО WAF-тесты, arch/14 §1.1)");
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

// S3-клиент подсистемы бэкапов с горячей конфигурацией (ревью Ф7 №3): секция
// PgWorker:Backups читается CurrentValue при КАЖДОМ вызове; смена опций (record-
// равенство) пересоздаёт клиента (креды/endpoint актуальны сразу после reload).
// Enabled=false → отказ "Backups:Enabled=false" (семантика заглушки: WalStreamProcess
// при runtime()==null уходит в стоп-семантику, не доходя до S3; ошибки list —
// консервативный сигнал, если зовут мимо процесса).
file sealed class ReloadableBackupS3(IOptionsMonitor<PgWorkerOptions> options) : IBackupS3, IAsyncDisposable
{
    private readonly object _gate = new();
    private (BackupsRuntimeOptions Config, BackupS3 Client)? _current;
    private bool _disposed;

    // Клиент под ТЕКУЩУЮ конфигурацию (ленивая инициализация + пересоздание);
    // устаревший клиент диспозится ВНЕ lock (IAsyncDisposable).
    private async ValueTask<IBackupS3> CurrentAsync()
    {
        BackupS3? stale = null;
        IBackupS3 current;
        lock (_gate)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(ReloadableBackupS3));
            var runtime = options.CurrentValue.Backups.Enabled
                ? options.CurrentValue.Backups.ToRuntime()
                : null;

            if (_current is { } pair && (runtime is null || pair.Config != runtime))
            {
                stale = pair.Client; // старый конфиг (креды/endpoint) неактуален
                _current = null;
            }

            if (runtime is null)
                current = DisabledBackupS3.Instance;
            else
            {
                _current ??= (runtime, new BackupS3(runtime));
                current = _current.Value.Client;
            }
        }

        if (stale is not null)
            await stale.DisposeAsync();
        return current;
    }

    public async Task<PgWorker.Core.Result<bool>> BucketExistsAsync(CancellationToken ct)
        => await (await CurrentAsync()).BucketExistsAsync(ct);

    public async Task<PgWorker.Core.Result<IReadOnlyList<PgWorker.Backups.WalObject>>> ListWalAsync(
        string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default)
        => await (await CurrentAsync()).ListWalAsync(cluster, shard, maxKeysPerTest, ct);

    public async Task<PgWorker.Core.Result<IReadOnlyList<PgWorker.Backups.S3ObjectInfo>>> ListPrefixAsync(
        string prefix, int? maxKeysPerTest = null, CancellationToken ct = default)
        => await (await CurrentAsync()).ListPrefixAsync(prefix, maxKeysPerTest, ct);

    public async Task<PgWorker.Core.Result> DeleteKeysAsync(
        IReadOnlyList<string> keys, CancellationToken ct = default)
        => await (await CurrentAsync()).DeleteKeysAsync(keys, ct);

    public async Task<PgWorker.Core.Result<IReadOnlyList<PgWorker.Backups.WalObject>>> ListAsync(
        string cluster, string shard, string prefix, int? maxKeysPerTest = null, CancellationToken ct = default)
        => await (await CurrentAsync()).ListAsync(cluster, shard, prefix, maxKeysPerTest, ct);

    public async Task<PgWorker.Core.Result<string>> GetObjectAsync(
        string cluster, string shard, string key, CancellationToken ct = default)
        => await (await CurrentAsync()).GetObjectAsync(cluster, shard, key, ct);

    public async Task<PgWorker.Core.Result<IReadOnlyList<string>>> ListFullsAsync(
        string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default)
        => await (await CurrentAsync()).ListFullsAsync(cluster, shard, maxKeysPerTest, ct);

    public async Task<PgWorker.Core.Result<string>> DownloadTextAsync(
        string cluster, string shard, string objectKey, CancellationToken ct = default)
        => await (await CurrentAsync()).DownloadTextAsync(cluster, shard, objectKey, ct);

    public async ValueTask DisposeAsync()
    {
        BackupS3? client;
        lock (_gate)
        {
            _disposed = true;
            client = _current?.Client;
            _current = null;
        }

        if (client is not null)
            await client.DisposeAsync();
    }

    // Заглушка выключенной подсистемы: list не зовётся штатным путём (стоп-семантика).
    private sealed class DisabledBackupS3 : IBackupS3
    {
        public static readonly DisabledBackupS3 Instance = new();

        public Task<PgWorker.Core.Result<bool>> BucketExistsAsync(CancellationToken ct)
            => Task.FromResult(PgWorker.Core.Result<bool>.Failed(
                new ApplicationException("Backups:Enabled=false")));

        public Task<PgWorker.Core.Result<IReadOnlyList<PgWorker.Backups.WalObject>>> ListWalAsync(
            string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default)
            => Task.FromResult(PgWorker.Core.Result<IReadOnlyList<PgWorker.Backups.WalObject>>.Failed(
                new ApplicationException("Backups:Enabled=false")));

        public Task<PgWorker.Core.Result<IReadOnlyList<PgWorker.Backups.S3ObjectInfo>>> ListPrefixAsync(
            string prefix, int? maxKeysPerTest = null, CancellationToken ct = default)
            => Task.FromResult(PgWorker.Core.Result<IReadOnlyList<PgWorker.Backups.S3ObjectInfo>>.Failed(
                new ApplicationException("Backups:Enabled=false")));

        public Task<PgWorker.Core.Result> DeleteKeysAsync(
            IReadOnlyList<string> keys, CancellationToken ct = default)
            => Task.FromResult(PgWorker.Core.Result.Failed(
                new ApplicationException("Backups:Enabled=false")));

        public Task<PgWorker.Core.Result<IReadOnlyList<PgWorker.Backups.WalObject>>> ListAsync(
            string cluster, string shard, string prefix, int? maxKeysPerTest = null, CancellationToken ct = default)
            => Task.FromResult(PgWorker.Core.Result<IReadOnlyList<PgWorker.Backups.WalObject>>.Failed(
                new ApplicationException("Backups:Enabled=false")));

        public Task<PgWorker.Core.Result<string>> GetObjectAsync(
            string cluster, string shard, string key, CancellationToken ct = default)
            => Task.FromResult(PgWorker.Core.Result<string>.Failed(
                new ApplicationException("Backups:Enabled=false")));

        public Task<PgWorker.Core.Result<IReadOnlyList<string>>> ListFullsAsync(
            string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default)
            => Task.FromResult(PgWorker.Core.Result<IReadOnlyList<string>>.Failed(
                new ApplicationException("Backups:Enabled=false")));

        public Task<PgWorker.Core.Result<string>> DownloadTextAsync(
            string cluster, string shard, string objectKey, CancellationToken ct = default)
            => Task.FromResult(PgWorker.Core.Result<string>.Failed(
                new ApplicationException("Backups:Enabled=false")));
    }
}

// WAF-тесты (PgWorker.IntegrationTests/Api): точка входа как public partial.
public partial class Program;
