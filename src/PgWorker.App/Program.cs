using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PgWorker.App;
using PgWorker.App.Api;
using PgWorker.App.Api.Operations;
using Shared.Core.HealthChecks;
using Shared.Core.Hosting;
using PgWorker.App.HealthChecks;
using PgWorker.App.Loops;
using PgWorker.Backups;
using PgWorker.Backups.EtcdExport;
using PgWorker.Backups.Sql;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Docker.Drivers;
using PgWorker.Etcd.S3;
using Shared.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.Moves;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Probes;
using PgWorker.Provisioning.Processes;
using PgWorker.Provisioning.Sql;
using Shared.Metrics;
using ProcessThresholds = PgWorker.Provisioning.Processes.ThresholdsOptions;

// Точка входа PgWorker (задача 23–24): host-builder с HTTP-granью /healthz,
// конфигурация appsettings+env, DI всех слоёв (etcd → координация → docker →
// процессы → циклы). Секреты установки — ТОЛЬКО env (Д7), fail-fast при отсутствии.

var builder = WebApplication.CreateBuilder(args);

// t03: env-секреты TLS (API / Docker / SSH) → конфиг-дерево до всего остального
// (t07: PGW_DOCKER_* — pg-биндинги в DockerEnvBindings, модели — в Shared.Docker).
ApiTlsEndpoints.ApplyEnvOverrides(builder.Configuration);
DockerEnvBindings.ApplyTlsEnvOverrides(builder.Configuration);
DockerEnvBindings.ApplySshEnvOverrides(builder.Configuration);
// REST-TLS нод (t22, arch/14 §4 гр.3): PGW_REST_TLS_* — до всего остального.
DockerEnvBindings.ApplyRestTlsEnvOverrides(builder.Configuration);

// Доверие CA https S3-endpoint (t42, spec §5 fallback): PgWorker:Backups:S3:CaBundlePath
// (env PGW_BACKUP_S3_CA_BUNDLE_PATH) доходит до BackupS3 через runtime-опции —
// AWSSDK.Core env AWS_CA_BUNDLE не читает (доказано E2E t42), per-install CA
// доверяется фабрикой HttpClient BackupS3. Хот-релоада пути нет: env деплоя
// меняются recreate контейнера — тот же порядок, что и остальные S3-секреты.

// Конфигурация: appsettings.json + env-оверрайды PgWorker__* (пример — в корне проекта).
builder.Services.Configure<PgWorkerOptions>(builder.Configuration.GetSection("PgWorker"));
// Fail-fast: advertise без URL бессмысленен; http-advertise запрещён при mTLS-каноне (arch/14 §1.1).
builder.Services.AddOptions<PgWorkerOptions>()
    .Validate(o => !string.IsNullOrWhiteSpace(o.Api.AdvertiseUrl),
        "PgWorker:Api:AdvertiseUrl is not set (API URL reachable by the panel; env PGW_API_ADVERTISE_URL)")
    .Validate(o => o.Api.Tls.AllowInsecureHttp
        || o.Api.AdvertiseUrl.StartsWith("https://", StringComparison.Ordinal),
        "PgWorker:Api:AdvertiseUrl must be https:// (mTLS-only API, arch/14 §1.1)")
    // Экспорт etcd-снапшотов в S3 (reliability t08): Enabled=true требует
    // ПОЛНУЮ СОБСТВЕННУЮ пару PgWorker:Snapshots:Export:S3 (t36, env
    // PGW_SNAPSHOTS_S3_* — не креды пулов бэкапов); диапазоны — всегда.
    .Validate(o => o.Snapshots.Export.IsValid(),
        "PgWorker:Snapshots:Export: Enabled=true requires a complete pair PgWorker:Snapshots:Export:S3 (env PGW_SNAPSHOTS_S3_*); RetentionObjects>=1, RetryIntervalSec>0, TimeoutSec>0")
    // Расчёт PGTune (spec.md §4.2): мусорный конфиг виден на старте, а не на
    // первом provision'е. desktop запрещён — его wal_level=minimal/max_wal_senders=0
    // несовместимы с P3 (логическое декодирование, переезды бакетов).
    .Validate(o => o.Pgtune.IsValid(),
        "PgWorker:Pgtune: DbVersion 10..18; DbType web|oltp|dw|mixed (desktop is forbidden — incompatible with P3); " +
        "HdType ssd|san|hdd|nvme; DbSize less_ram|mid_ram|greater_ram; Connections 20..999999; " +
        "ExcludeParams — only PGTune output names (§5.2). Memory/CPU are not set here: etcd requests use request_{cpu,mem}")
    // REST-TLS нод (t22, arch/14 §8): поставка без TLS-пакета считается битой —
    // HTTP-режим провижининга не существует (симметрия запрета Pgtune:DbType=desktop).
    .Validate(o => o.Docker.RestTls.IsComplete(),
        "PgWorker:Docker:RestTls is required: both PGW_REST_TLS_{CA,CA_KEY}[_PATH] " +
        "(per-install CA for REST-TLS nodes; HTTP mode does not exist)")
    .ValidateOnStart();

// Управляемый серт API (spec §3.2 п.1): чтение ключа /workers/api_tls/pgworker
// ДО поднятия Kestrel; приоритет etcd > env; битый ключ — fail-fast.
var etcdEndpoints = builder.Configuration.GetSection("PgWorker:Etcd:Endpoints").Get<string[]>() ?? [];
var managedCert = await WorkerApiCertReader.ReadAsync(etcdEndpoints, "pgworker", CancellationToken.None);

// mTLS HTTP API (arch/14 §1.1, t03): Kestrel с серверным сертом и требованием
// клиентского серта per-install API-CA (порт — из ASPNETCORE_URLS/urls, иначе 8080).
var apiTls = ApiTlsEndpoints.ConfigureMtls(builder, managedCert);

// Thumbprint применённого серта → дискавери-ключ (spec §3.2 п.3): панель
// сверяет с целевым → applied/pending restart.
var apiCertThumbprint = apiTls.ServerCert is { } appliedCert
    ? Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(appliedCert.RawData)).ToLowerInvariant()
    : null;

builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<HealthState>();
// Прогресс-отметки долгих фаз reconcile в глубину процессов (Provisioning/Moves).
builder.Services.AddSingleton<Shared.Core.Hosting.ILoopProgress>(sp => sp.GetRequiredService<HealthState>());

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

// REST-TLS нод (t22, arch/14 §8): per-install CA + ключ выпуска сертов;
// резолв PEM|PATH и разбор на старте — битый/неполный пакет fail-fast.
builder.Services.AddSingleton(sp =>
{
    var tls = sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Docker.RestTls;
    var caPem = tls.CaPem ?? Shared.Tls.TlsMaterial.ReadPemFile(tls.CaPath);
    var caKeyPem = tls.CaKeyPem ?? Shared.Tls.TlsMaterial.ReadPemFile(tls.CaKeyPath);
    if (!RestTlsOptions.IsValidPemPair(caPem, caKeyPem)
        || !RestPki.TryParseCertificate(caPem!, out var ca) || ca is null)
        throw new ApplicationException(
            "PgWorker:Docker:RestTls: per-install CA is not set / cannot be parsed — " +
            "both PGW_REST_TLS_{CA,CA_KEY}[_PATH] are required (REST-TLS nodes; HTTP mode does not exist)");
    return new RestTlsMaterial(caPem!, caKeyPem!, ca);
});

// Поллинг-инвариант (arch/14 §6): одиночный HTTP-вызов к etcd не молчит дольше
// окна проверки watchdog — таймаут = половина окна (7.5 c при дефолтах); зависший
// запрос (протухшее соединение) → исключение → transient-фейл тика, а не молчание
// до дефолтных 100 c HttpClient (разбор E2E-маркера: reconcile молчал 37 c).
builder.Services.AddHttpClient("etcd", c => c.Timeout = TimeSpan.FromSeconds(7.5));
// Клиент Patroni REST (t22, arch/14 §5 C): https к host:patroni-port portalloc;
// верификация цепочки к per-install CA без hostname-проверки (канон P17
// «require, не verify-full»: адрес ноды — из реплицированного portalloc;
// образец — CustomRootTrust панели). Таймауты не меняются (7.5 c клиент).
builder.Services.AddHttpClient("patroni", c => c.Timeout = TimeSpan.FromSeconds(7.5))
    .ConfigurePrimaryHttpMessageHandler(sp =>
    {
        var ca = sp.GetRequiredService<RestTlsMaterial>().Ca;
        return new SocketsHttpHandler
        {
            SslOptions = new System.Net.Security.SslClientAuthenticationOptions
            {
                RemoteCertificateValidationCallback = (_, certificate, _, _) =>
                    Shared.Tls.TlsChain.ValidateChain(certificate, ca),
            },
        };
    });

// etcd-клиент (HTTP JSON gateway /v3/*) + координация (клэймы/лидерство, журнал).
// Единое место литерала префикса etcd-ключей (t09): Shared-координация параметризована.
const string KeyPrefix = "/pgworker";
builder.Services.AddSingleton<IEtcdGateway>(sp =>
    new EtcdGateway(sp.GetRequiredService<IHttpClientFactory>().CreateClient("etcd")));
builder.Services.AddSingleton(sp => new ClaimStore(
    KeyPrefix,
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Api.AdvertiseUrl,
    apiCertThumbprint));
builder.Services.AddSingleton(sp => new WorkJournal(
    KeyPrefix,
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints));
// Журнал эвакуаций (t09): Pg-домен на ключах /pgworker/evacuations/* — выделен
// из WorkJournal при переносе координации в Shared.Etcd.
builder.Services.AddSingleton(sp => new EvacuationJournalStore(
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
builder.Services.AddSingleton(sp => new UpdateClusterConfigHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints)); // t06: PUT config
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
// Сироты бэкапов: hold/unhold/delete-заявки (reliability t04, arch/19 §4) —
// пер-префиксные ключи orphan-holds/orphan-deletes, пишет только API;
// runtime-функция — как у sweeper (Enabled=false → гвард 503).
builder.Services.AddSingleton(sp => new OrphanHoldHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<TimeProvider>(),
    () => sp.GetRequiredService<IOptionsMonitor<PgWorkerOptions>>().CurrentValue.Backups.Enabled
        ? sp.GetRequiredService<IOptionsMonitor<PgWorkerOptions>>().CurrentValue.BackupsRuntime()
        : null));
builder.Services.AddSingleton(sp => new OrphanDeleteHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<TimeProvider>(),
    () => sp.GetRequiredService<IOptionsMonitor<PgWorkerOptions>>().CurrentValue.Backups.Enabled
        ? sp.GetRequiredService<IOptionsMonitor<PgWorkerOptions>>().CurrentValue.BackupsRuntime()
        : null));
builder.Services.AddSingleton(sp => new RecreateNodeHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints));
// Демо-сид (arch/14 §1.1.1): стендовый эндпоинт за флагом EnableSeedEndpoint.
builder.Services.AddSingleton(sp => new SeedDemoHandler(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Api.EnableSeedEndpoint));

// Graceful self-stop (spec §3.2 п.2): рестарт из панели применяет серт/конфиг.
builder.Services.AddSingleton(sp => new RestartHandler(
    sp.GetRequiredService<IHostApplicationLifetime>(),
    sp.GetRequiredService<ILoggerFactory>().CreateLogger<RestartHandler>()));

// Кеш серверных сертов REST-эндпоинтов нод (t22, arch/14 §2.1): из per-install
// CA (RestTlsMaterial) — один на процесс воркера, обе реализации драйвера.
builder.Services.AddSingleton(sp =>
{
    var material = sp.GetRequiredService<RestTlsMaterial>();
    return new RestCertificateCache(material.CaPem, material.CaKeyPem);
});

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
                "PgWorker:Docker:AdvertisedHost is not supported in Mode=Swarm (multi-host addressing)");
        if (docker.Hosts.Length != 1)
            throw new ApplicationException(
                "PgWorker:Docker:AdvertisedHost requires exactly one host in PgWorker:Docker:Hosts (single-host/tunnel)");
    }

    // ScrapeNetwork (t15, arch/18 §5.4): сетевой attach нод реализован для
    // plain-контейнеров; swarm-Ensure — сервисы, второй сети у сервиса нет в
    // MVP — молчаливое игнорирование ключа недопустимо (fail-fast, прецедент
    // AdvertisedHost).
    if (!string.IsNullOrWhiteSpace(docker.ScrapeNetwork)
        && string.Equals(docker.Mode, "Swarm", StringComparison.OrdinalIgnoreCase))
        throw new ApplicationException(
            "PgWorker:Docker:ScrapeNetwork is not supported in Mode=Swarm — node network attach is implemented for plain containers");

    // ContourNetwork (t41, arch/14 §2.1): контур-режим сетей движка — ноды/
    // wal-агенты/джобы бэкапов в одной внешней сети поставки; для swarm-сервисов
    // режима нет (fail-fast, прецедент ScrapeNetwork).
    if (!string.IsNullOrWhiteSpace(docker.ContourNetwork)
        && string.Equals(docker.Mode, "Swarm", StringComparison.OrdinalIgnoreCase))
        throw new ApplicationException(
            "PgWorker:Docker:ContourNetwork is not supported in Mode=Swarm — contour network mode is implemented for plain containers");

    if (string.Equals(docker.Mode, "Swarm", StringComparison.OrdinalIgnoreCase))
    {
        if (string.IsNullOrWhiteSpace(docker.SwarmManager))
            throw new ApplicationException("PgWorker:Docker:Mode=Swarm requires PgWorker:Docker:SwarmManager");
        return new SwarmClusterDriver(docker.SwarmManager, factory, docker.EnableDoorman, docker.Images.Node,
            pgtuneExclude: pgtuneExclude,
            restCertificates: sp.GetRequiredService<RestCertificateCache>());
    }

    var hosts = docker.Hosts
        .Select(h => new HostEndpoint(h.Name, h.Endpoint))
        .ToList();
    if (hosts.Count == 0)
        throw new ApplicationException("PgWorker:Docker:Mode=Plain requires a non-empty PgWorker:Docker:Hosts list");
    return new PlainClusterDriver(hosts, factory, docker.EnableDoorman, docker.Images.Node, docker.AdvertisedHost,
        pgtuneExclude: pgtuneExclude, scrapeNetwork: docker.ScrapeNetwork,
        contourNetwork: docker.ContourNetwork is { Length: > 0 } ? docker.ContourNetwork : null,
        restCertificates: sp.GetRequiredService<RestCertificateCache>());
});



// Фабрика входов PGTune (spec.md §4.3): runtime-склейка PgWorker:Pgtune
// (валидированы fail-fast'ом старта); расчёт — per-shard на EnsureNode-путях.
// PgtuneSettings (runtime-склейка PgWorker:Pgtune) — единый ExcludeParams для
// bootstrap и конвергенции DCS (t11, spec §4.3 п.3).
builder.Services.AddSingleton(sp =>
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Pgtune.ToRuntime());
builder.Services.AddSingleton(sp => new PgtuneInputsFactory(
    sp.GetRequiredService<PgtuneSettings>(),
    sp.GetRequiredService<ILogger<PgtuneInputsFactory>>()));

// Пробы Patroni REST и SQL-слой (Npgsql + Polly-ретраи).
builder.Services.AddSingleton(sp =>
    new ShardProbe(sp.GetRequiredService<IHttpClientFactory>().CreateClient("patroni")));
builder.Services.AddSingleton<ISqlExecutor, DatabaseProvisioner>();

// t08: S3-sink экспорта — один синглтон на процесс; null при выключенной опции
// (креды фиксируются на старте: env-секреты деплоя меняются recreate контейнера).
// null! — осознанный контракт: GetService<EtcdSnapshotSink>() обязан вернуть null
// при Enabled=false (потребители — SnapshotJob/SnapshotLoop).
builder.Services.AddSingleton(sp =>
{
    var opts = sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value;
    return opts.Snapshots.Export.Enabled
        ? new EtcdSnapshotSink(
            // t36: цель слепков — ОТДЕЛЬНАЯ пара PgWorker:Snapshots:Export:S3
            // (env PGW_SNAPSHOTS_S3_*; arch/14 §6/§8 — не креды пулов бэкапов).
            new BackupS3(EtcdSnapshotSinkFactory.Create(opts)),
            sp.GetRequiredService<IEtcdGateway>(),
            opts.Etcd.Endpoints,
            opts.Snapshots.Export.RetentionObjects,
            opts.Snapshots.Export.TimeoutSec,
            sp.GetRequiredService<ClaimStore>().InstanceId,
            opts.Loops.SnapshotIntervalMin,
            // Пульс S3-шагов выгрузки — канал snapshot (MarkSnapshotActivity,
            // не reconcile): путь SnapshotLoop, arch/14 §6; период — полокна
            // проверки watchdog (CheckIntervalSec-производный).
            () => sp.GetRequiredService<HealthState>().MarkSnapshotActivity(),
            TimeSpan.FromSeconds(Math.Max(1, opts.Loops.Watchdog.CheckIntervalSec / 2)))
        : null!;
});

// Снапшоты P12 (SnapshotLoop-лидер + процессы в точках изменений) + опциональный
// S3-sink (t08): Enabled=false → sink null, поведение прежнее (§2.10).
builder.Services.AddSingleton(sp =>
{
    var opts = sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value;
    return new SnapshotJob(
        sp.GetRequiredService<IEtcdGateway>(), opts.Etcd.Endpoints,
        opts.Snapshots.Dir, opts.Snapshots.RetentionFiles, opts.Snapshots.MaintenanceIntervalMin,
        sp.GetService<EtcdSnapshotSink>());
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
    KeyPrefix,
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
        SnapshotDelegate(job),
        sp.GetRequiredService<Shared.Core.Hosting.ILoopProgress>(),
        TimeSpan.FromSeconds(opts.Loops.Watchdog.CheckIntervalSec),
        sp.GetRequiredService<ILogger<ProvisioningProcess>>(),
        opts.Docker.ScrapeNetwork);
});
builder.Services.AddSingleton(sp => new DeprovisioningProcess(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IClusterDriver>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    SnapshotDelegate(sp.GetRequiredService<SnapshotJob>()),
    sp.GetRequiredService<Shared.Core.Hosting.ILoopProgress>()));
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
    sp.GetRequiredService<IClusterSecretEnsurer>(),
    sp.GetRequiredService<IAppParamsEnsurer>(),
    sp.GetRequiredService<PgtuneInputsFactory>(),
    sp.GetRequiredService<PgtuneSettings>(),
    sp.GetRequiredService<ILogger<NodeSupervisor>>(),
    new MasterKeyReconciler(
        sp.GetRequiredService<IEtcdGateway>(),
        sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
        sp.GetRequiredService<ShardProbe>()),
    sp.GetRequiredService<EtcdEndpoints>(),
    sp.GetRequiredService<Shared.Metrics.Worker.WorkerMetricsInstrumentation>().HaDurations));
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
        SnapshotDelegate(sp.GetRequiredService<SnapshotJob>()),
        opts.Docker.ScrapeNetwork);
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
    sp.GetRequiredService<EvacuationJournalStore>(),
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
        SnapshotDelegate(sp.GetRequiredService<SnapshotJob>()),
        sp.GetRequiredService<Shared.Core.Hosting.ILoopProgress>(),
        TimeSpan.FromSeconds(opts.Loops.Watchdog.CheckIntervalSec),
        sp.GetRequiredService<ILogger<AddShardProcess>>(),
        opts.Docker.ScrapeNetwork);
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
        SnapshotDelegate(sp.GetRequiredService<SnapshotJob>()),
        sp.GetRequiredService<Shared.Core.Hosting.ILoopProgress>());
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
    sp.GetRequiredService<IClusterDriver>(),
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
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.BackupsRuntime(),
    sp.GetRequiredService<BackupS3Factory>(),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<ILoggerFactory>().CreateLogger<PgWorker.Backups.BackupProcess>(),
    SnapshotDelegate(sp.GetRequiredService<SnapshotJob>()),
    fullAgeObserver: sp.GetRequiredService<Shared.Metrics.Worker.WorkerMetricsInstrumentation>().BackupFullAge,
    watchdogWindow: TimeSpan.FromSeconds(
        sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Loops.Watchdog.CheckIntervalSec),
    progress: sp.GetRequiredService<Shared.Core.Hosting.ILoopProgress>()));

// Восстановление шарда из бэкапа (t05, arch/19 §3.5): PLANNED→RUNNING→
// REJOINING→COMPLETED; plain-only, Exec в объёме джоба. Runtime-опции —
// статичный срез (врезка цикла и процесс гвардят Enabled).
builder.Services.AddSingleton(sp => new PgWorker.Backups.Process.RestoreProcess(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IClusterDriver>(),
    sp.GetRequiredService<BackupS3Factory>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.BackupsRuntime(),
    sp.GetRequiredService<InstallSecrets>(),
    sp.GetRequiredService<EtcdEndpoints>(),
    sp.GetRequiredService<IClusterSecretEnsurer>(),
    sp.GetRequiredService<ShardProbe>(),
    sp.GetRequiredService<ISqlExecutor>(),
    new ProcessThresholds(sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Thresholds.NodeDeadSec,
        sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Thresholds.ShardDeadSec,
        sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Thresholds.PatroniBootSec),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<ILoggerFactory>().CreateLogger<PgWorker.Backups.Process.RestoreProcess>(),
    sp.GetRequiredService<Shared.Metrics.Worker.WorkerMetricsInstrumentation>().BackupRestore,
    TimeSpan.FromSeconds(
        sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Loops.Watchdog.CheckIntervalSec),
    sp.GetRequiredService<Shared.Core.Hosting.ILoopProgress>()));

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
        sp.GetRequiredService<BackupS3Factory>(),
        new WalStatusWriter(sp.GetRequiredService<IEtcdGateway>(), opts.Etcd.Endpoints),
        sp.GetRequiredService<ClaimStore>(),
        sp.GetRequiredService<WorkJournal>(),
        () => sp.GetRequiredService<IOptionsMonitor<PgWorkerOptions>>().CurrentValue.Backups.Enabled
            ? sp.GetRequiredService<IOptionsMonitor<PgWorkerOptions>>().CurrentValue.BackupsRuntime()
            : null,
        sp.GetRequiredService<InstallSecrets>(),
        sp.GetRequiredService<TimeProvider>(),
        sp.GetRequiredService<Shared.Metrics.Worker.WorkerMetricsInstrumentation>().BackupWalLag,
        sp.GetRequiredService<ILoggerFactory>().CreateLogger("WalStreamProcess"),
        sp.GetRequiredService<Shared.Metrics.Worker.WorkerMetricsInstrumentation>().BackupWalUploadedAge,
        TimeSpan.FromSeconds(opts.Loops.Watchdog.CheckIntervalSec),
        sp.GetRequiredService<Shared.Core.Hosting.ILoopProgress>());
});
builder.Services.AddSingleton<IWalSqlExecutor, NpgsqlWalSqlExecutor>();
// t36: пул S3 бэкапов — чтение /s3/, резолвер policy→списки, фабрика пула-клиента.
builder.Services.AddSingleton<IS3PoolStore>(sp => new PgWorker.Etcd.S3.S3PoolStore(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<ILoggerFactory>().CreateLogger<PgWorker.Etcd.S3.S3PoolStore>()));
builder.Services.AddSingleton(sp => new BackupPoolResolver(
    sp.GetRequiredService<IS3PoolStore>(),
    policyReader: new BackupPolicyReader(
        sp.GetRequiredService<IEtcdGateway>(),
        sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints).ReadAsync));
builder.Services.AddSingleton(sp => new BackupS3Factory(
    sp.GetRequiredService<BackupPoolResolver>(),
    sp.GetRequiredService<IS3PoolStore>(),
    () => sp.GetRequiredService<IOptionsMonitor<PgWorkerOptions>>().CurrentValue.BackupsRuntime().S3Bucket,
    () => sp.GetRequiredService<IOptionsMonitor<PgWorkerOptions>>().CurrentValue.BackupsRuntime().S3CaBundlePath,
    // Пер-попыточный таймаут S3-запроса (arch/19 §5): половина окна watchdog.
    TimeSpan.FromSeconds(sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Loops.Watchdog.CheckIntervalSec / 2.0)));
// Ретенция (t06, arch/19 §4): GFS/WAL-чистка/гигиена + монитор хранилища;
// Enabled=false — no-op (гвард в процессе дублирует guard цикла).
builder.Services.AddSingleton(sp => new PgWorker.Backups.RetentionProcess(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<BackupS3Factory>(),
    sp.GetRequiredService<IS3PoolStore>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.BackupsRuntime(),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<ILoggerFactory>().CreateLogger<PgWorker.Backups.RetentionProcess>(),
    sp.GetRequiredService<Shared.Core.Hosting.ILoopProgress>()));

// Проверки полных бэкапов (t04, arch/19 §5): цепочка + pg_verifybackup-джобы;
// verifyObserver — counter pgworker_backup_verify_total{result}.
builder.Services.AddSingleton(sp => new PgWorker.Backups.BackupVerifyProcess(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IClusterDriver>(),
    sp.GetRequiredService<ShardEndpoints>(),
    sp.GetRequiredService<BackupS3Factory>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<IOptionsMonitor<PgWorkerOptions>>().CurrentValue.BackupsRuntime(),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<ILoggerFactory>().CreateLogger<PgWorker.Backups.BackupVerifyProcess>(),
    sp.GetRequiredService<Shared.Metrics.Worker.WorkerMetricsInstrumentation>().BackupVerify,
    TimeSpan.FromSeconds(sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Loops.Watchdog.CheckIntervalSec),
    sp.GetRequiredService<Shared.Core.Hosting.ILoopProgress>()));

// Дрилл восстановимости (reliability t02, arch/19 §3.6): супервиз drill-джоба,
// отбор/валидация/запуск, доводимый снос; runtime — как у verify.
builder.Services.AddSingleton(sp => new PgWorker.Backups.Process.RestoreDrillProcess(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<IClusterDriver>(),
    sp.GetRequiredService<ShardEndpoints>(),
    sp.GetRequiredService<BackupS3Factory>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.BackupsRuntime(),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<ILoggerFactory>().CreateLogger<PgWorker.Backups.Process.RestoreDrillProcess>(),
    sp.GetRequiredService<Shared.Metrics.Worker.WorkerMetricsInstrumentation>().BackupDrill,
    TimeSpan.FromSeconds(
        sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Loops.Watchdog.CheckIntervalSec),
    sp.GetRequiredService<Shared.Core.Hosting.ILoopProgress>()));

// Сверка S3↔etcd (t07, arch/19 §4): per-cluster чистка мусора full/<id>/ без
// etcd-ключа; runtime-функция через IOptionsMonitor — Enabled=false → no-op
// (образец WalStreamProcess).
builder.Services.AddSingleton(sp => new PgWorker.Backups.Supervisor.BackupSupervisorProcess(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<BackupS3Factory>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    () => sp.GetRequiredService<IOptionsMonitor<PgWorkerOptions>>().CurrentValue.Backups.Enabled
        ? sp.GetRequiredService<IOptionsMonitor<PgWorkerOptions>>().CurrentValue.BackupsRuntime()
        : null,
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<Shared.Core.Hosting.ILoopProgress>(),
    sp.GetRequiredService<ILoggerFactory>().CreateLogger<PgWorker.Backups.Supervisor.BackupSupervisorProcess>()));

// Глобальный проход сирот S3 (t07, arch/19 §4): реестр /pgworker/backups/orphans
// + TTL-удаление; пишет только лидер /pgworker/leader (BackupOrphanSweeperLoop).
builder.Services.AddSingleton(sp => new PgWorker.Backups.Supervisor.BackupOrphanSweeper(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<BackupS3Factory>(),
    sp.GetRequiredService<IS3PoolStore>(),
    sp.GetRequiredService<ClaimStore>(),
    () => sp.GetRequiredService<IOptionsMonitor<PgWorkerOptions>>().CurrentValue.Backups.Enabled
        ? sp.GetRequiredService<IOptionsMonitor<PgWorkerOptions>>().CurrentValue.BackupsRuntime()
        : null,
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<ILoggerFactory>().CreateLogger<PgWorker.Backups.Supervisor.BackupOrphanSweeper>()));
builder.Services.AddSingleton(sp => new PgWorker.App.Loops.BackupOrphanSweeperLoop(
    sp.GetRequiredService<IOptionsMonitor<PgWorkerOptions>>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<PgWorker.Backups.Supervisor.BackupOrphanSweeper>(),
    sp.GetRequiredService<HealthState>(),
    sp.GetRequiredService<ILogger<PgWorker.App.Loops.BackupOrphanSweeperLoop>>()));
builder.Services.AddHostedService(sp => sp.GetRequiredService<PgWorker.App.Loops.BackupOrphanSweeperLoop>());

// Циклы (§6.2): keepalive первым (lease живут до Reconcile), затем снапшоты и reconcile.
// Регистрируются синглтонами — health-обёртки читают их состояние напрямую.
builder.Services.AddSingleton<IClusterProcesses, ClusterProcesses>();
builder.Services.AddSingleton<KeepaliveLoop>();
// t08: явная фабрика SnapshotLoop — последний параметр (sink) может быть null
// при выключенной опции; sp.GetService вернёт null (это рабочий режим).
builder.Services.AddSingleton(sp => new SnapshotLoop(
    sp.GetRequiredService<IOptionsMonitor<PgWorkerOptions>>(),
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<SnapshotJob>(),
    sp.GetRequiredService<ILogger<SnapshotLoop>>(),
    sp.GetRequiredService<HealthState>(),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<Shared.Metrics.Worker.WorkerMetricsInstrumentation>(),
    sp.GetService<EtcdSnapshotSink>()));
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

// Watchdog зависших циклов (arch/14 §6): staleness активности (тик или
// прогресс-отметка) всех циклов → журнал +
// метрика + graceful self-stop (путь POST /api/restart); Enabled=false (секция
// PgWorker:Loops:Watchdog) — компонент не регистрируется.
builder.Services.AddSingleton<PgWorkerLoopsVitality>();
builder.Services.AddSingleton<Shared.Core.Hosting.ILoopsVitality>(
    sp => sp.GetRequiredService<PgWorkerLoopsVitality>());
var loopsWatchdog = new Shared.Core.Hosting.WatchdogOptions();
builder.Configuration.GetSection("PgWorker:Loops:Watchdog").Bind(loopsWatchdog);
builder.Services.AddLoopWatchdog(
    loopsWatchdog,
    (sp, loop) => sp.GetRequiredService<Shared.Metrics.Worker.WorkerMetricsInstrumentation>()
        .WatchdogRestart(loop));
// fail-fast инварианта живости (arch/14 §8): тики быстрых циклов чаще порога
// сноса (Multiplier × CheckIntervalSec); конфиг watchdog не меняется
var loopsCfg = builder.Configuration.GetSection("PgWorker:Loops").Get<LoopsOptions>() ?? new LoopsOptions();
if (loopsWatchdog.Enabled)
    Shared.Core.Hosting.WatchdogConfigGuard.EnsureFastLoopsBelowStaleThreshold(
        loopsCfg.ScanIntervalSec, loopsCfg.KeepaliveSec, loopsWatchdog);

var app = builder.Build();
if (app.Services.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Api.Tls.AllowInsecureHttp)
    app.Logger.LogWarning(
        "PgWorker:Api:Tls:AllowInsecureHttp=true — HTTP without TLS (WAF tests ONLY, arch/14 §1.1)");
if (apiTls.Source is { } certSource)
    app.Logger.LogInformation("PgWorker:Api:Tls: API server certificate — source {Source}", certSource);
if (apiTls.Warning is { } certWarning)
    app.Logger.LogWarning("PgWorker:Api:Tls: {Warning}", certWarning);
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
            $"required env secret {name} is not set (Д7: per-install, not in git and not in etcd)");

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
