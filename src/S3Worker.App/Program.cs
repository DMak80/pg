using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using S3Worker.App;
using Shared.Metrics;
using S3Worker.App.Api;
using Shared.Core.Hosting;

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
    // TODO(Task 4): подписка WorkJournal.PhaseWritten += e => m.OnJournalPhase(...);
    // WorkJournal появляется в Task 4 — в этом шаге сборка без координации.
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

// etcd-клиент (HTTP JSON gateway /v3/*) + координация: ClaimStore/PortAllocLock/
// WorkJournal — Task 4 (единый литерал префикса /s3worker там же).

// docker-фабрика движков (health-пробы docker-hosts; драйвер Plain/Swarm —
// Task 6; fail-fast топологии — валидация выше).
builder.Services.AddSingleton<DockerEngineFactory>();

// TODO(Task 4): SnapshotJob (снапшоты P12), координация, циклы, health-чеки.
// TODO(Tasks 10–13): процессы A–E + ротатор.
// TODO(Task 14): хендлеры API + MapWorkerApi-эндпоинты (каркас — пустой).

// Циклы: keepalive → snapshot → reconcile (Task 4).

// Наблюдаемость (arch/24 §7): агрегированный health + per-loop обёртки — Task 4.
builder.Services.AddHealthChecks();

// Watchdog зависших циклов (arch/24 §6): регистрация с циклами — Task 4.

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

// WAF-тесты (S3Worker.IntegrationTests/Api, Task 15): точка входа как public partial.
public partial class Program;
