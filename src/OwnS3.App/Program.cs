using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using OwnS3.App;
using OwnS3.App.Access;
using OwnS3.App.Pipeline;
using OwnS3.Storage;
using OwnS3.Storage.Erasure;
using Shared.Metrics;
using System.Security.Cryptography.X509Certificates; // X509Certificate2? tlsCert

// Точка входа ownS3 (arch/owns3/01 §3, arch/owns3/05): Kestrel-хост S3-грани,
// конфигурация OwnS3:* / OWNS3_*, fail-fast root-пары, /healthz и /metrics.
// Протокольный конвейер (роутинг/подпись/права) — S3Middleware.

var builder = WebApplication.CreateBuilder(args);

// Env-имена канона arch/owns3/05 §4 (OWNS3_*): стандартный .NET-провайдер
// ждёт OwnS3__Root__User — транслируем канонические имена в ключи OwnS3:*
// (env приоритетнее appsettings; t39, фикс дефекта первого докер-запуска).
foreach (var (env, key) in new[]
         {
             ("OWNS3_ROOT_USER", "OwnS3:Root:User"),
             ("OWNS3_ROOT_PASSWORD", "OwnS3:Root:Password"),
             ("OWNS3_SERVER_PORT", "OwnS3:Server:Port"),
             ("OWNS3_DATA_DIR", "OwnS3:DataDir"),
             ("OWNS3_EC_SET_SIZE", "OwnS3:Ec:SetSize"),
             ("OWNS3_EC_PARITY", "OwnS3:Ec:Parity"),
             ("OWNS3_HOST_ID", "OwnS3:HostId"),
             ("OWNS3_TLS_CERT", "OwnS3:Tls:ServerCertPem"),
             ("OWNS3_TLS_KEY", "OwnS3:Tls:ServerKeyPem"),
             ("OWNS3_TLS_CERT_PATH", "OwnS3:Tls:ServerCertPath"),
             ("OWNS3_TLS_KEY_PATH", "OwnS3:Tls:ServerKeyPath"),
         })
{
    var value = Environment.GetEnvironmentVariable(env);
    if (!string.IsNullOrEmpty(value))
        builder.Configuration[key] = value;
}
// Массив статических ключей OWNS3_ACCESS_KEYS__<i>__* (глава 05 §2:
// индексная нумерация массива в env — конвенция .NET): сканируем до
// первого пропуска индекса.
for (var i = 0; ; i++)
{
    var access = Environment.GetEnvironmentVariable($"OWNS3_ACCESS_KEYS__{i}__ACCESSKEY");
    var secret = Environment.GetEnvironmentVariable($"OWNS3_ACCESS_KEYS__{i}__SECRETKEY");
    if (string.IsNullOrEmpty(access) && string.IsNullOrEmpty(secret))
        break;
    builder.Configuration[$"OwnS3:AccessKeys:{i}:AccessKey"] = access;
    builder.Configuration[$"OwnS3:AccessKeys:{i}:SecretKey"] = secret;
    var policy = Environment.GetEnvironmentVariable($"OWNS3_ACCESS_KEYS__{i}__POLICY");
    if (!string.IsNullOrEmpty(policy))
        builder.Configuration[$"OwnS3:AccessKeys:{i}:Policy"] = policy;
}

builder.Services.Configure<OwnS3Options>(builder.Configuration.GetSection(OwnS3Options.SectionName));
// TryAdd: системные часы — дефолт; тестовый хост (WAF) может подменить время
// своей регистрацией до Program — тогда остаётся подмена (детерминированные
// skew/окно-кейсы интеграционных тестов).
builder.Services.TryAddSingleton(TimeProvider.System);

// Fail-fast root-пары (arch/owns3/05 §1): user >= 3, password >= 8, оба непусты.
builder.Services.AddOptions<OwnS3Options>()
    .Validate(o => !string.IsNullOrWhiteSpace(o.Root.User) && o.Root.User.Length >= 3,
        "OwnS3:Root:User обязателен и не короче 3 символов (env OWNS3_ROOT_USER)")
    .Validate(o => !string.IsNullOrWhiteSpace(o.Root.Password) && o.Root.Password.Length >= 8,
        "OwnS3:Root:Password обязателен и не короче 8 символов (env OWNS3_ROOT_PASSWORD)")
    .ValidateOnStart();

// Метрики (arch/18; arch/owns3/05 §5): имя Meter = ownS3 (строчными — финальные
// серии ownS3_*_total/ownS3_request_duration_seconds совпадают со словарём главы 05).
builder.Services.AddAppMetrics("ownS3", builder.Configuration.GetSection("OwnS3:Metrics"));

// Домен: реестр учёток + объектный слой (xl-хранение, канон 04). Режим — по
// числу путей OwnS3:DataDir (канон 06 §1): 1 путь — XL Single (SingleXlDrive
// над XlVolume); список ≥ 2 — XL Erasure (ErasurePool + ErasureXlDrive + heal).
// Все регистрации ленивы: режим разрешается по итоговому конфигу ПОСЛЕ Build
// (in-memory-инжект WAF-тестов слит к этому моменту), ошибки конфига —
// fail-fast до старта прослушивания Kestrel (ниже, Critical + exit 1).
builder.Services.AddSingleton<AccessKeyRegistry>();
builder.Services.AddSingleton(sp => StorageModeInfo.Resolve(
    sp.GetRequiredService<IOptions<OwnS3Options>>().Value));

// XL Single: том и драйв (строятся только при разрешении в single-режиме).
builder.Services.AddSingleton(sp => new XlVolume(
    sp.GetRequiredService<StorageModeInfo>().SingleRoot
        ?? throw new InvalidOperationException("XlVolume разрешён только в single-режиме"),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<ILogger<XlVolume>>()));
builder.Services.AddSingleton(sp => new SingleXlDrive(
    sp.GetRequiredService<XlVolume>(),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<ILogger<SingleXlDrive>>()));

// XL Erasure: пул, драйв, статус и heal (строятся только в EC-режиме).
builder.Services.AddSingleton(sp =>
{
    var mode = sp.GetRequiredService<StorageModeInfo>();
    return new ErasurePool(mode.DataRoots,
        mode.Topology ?? throw new InvalidOperationException("ErasurePool разрешён только в EC-режиме"),
        sp.GetRequiredService<TimeProvider>());
});
builder.Services.AddSingleton(sp => new ErasureXlDrive(
    sp.GetRequiredService<ErasurePool>(),
    sp.GetRequiredService<StorageModeInfo>().Topology!.Parity,
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<ILogger<ErasureXlDrive>>()));
builder.Services.AddSingleton<ErasureStatusProvider>();
builder.Services.AddSingleton(sp => new ErasureHealService(
    sp.GetRequiredService<ErasurePool>(), sp.GetRequiredService<ErasureXlDrive>(),
    sp.GetRequiredService<ErasureStatusProvider>(), sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<ILogger<ErasureHealService>>()));
builder.Services.AddSingleton<OwnS3EcMetrics>();

// Общий контракт размещения: развилка режима — при разрешении.
builder.Services.AddSingleton<IXlDrive>(sp =>
    sp.GetRequiredService<StorageModeInfo>().IsErasure
        ? sp.GetRequiredService<ErasureXlDrive>()
        : sp.GetRequiredService<SingleXlDrive>());

builder.Services.AddSingleton<IObjectStore>(sp => new XlObjectStore(
    sp.GetRequiredService<IXlDrive>(),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<ILogger<XlObjectStore>>()));
builder.Services.AddHostedService<VolumeCleanupService>();
builder.Services.AddHostedService<ErasureHealHostedService>();
builder.Services.AddSingleton<OwnS3Metrics>();

// 21 хендлер протокольного контура (spec §3.4.1) — диспетчеризация по операции.
builder.Services.AddSingleton<OwnS3.App.Handlers.BucketHandlers.ListBucketsHandler>();
builder.Services.AddSingleton<OwnS3.App.Handlers.BucketHandlers.CreateBucketHandler>();
builder.Services.AddSingleton<OwnS3.App.Handlers.BucketHandlers.DeleteBucketHandler>();
builder.Services.AddSingleton<OwnS3.App.Handlers.BucketHandlers.HeadBucketHandler>();
builder.Services.AddSingleton<OwnS3.App.Handlers.BucketHandlers.GetBucketLocationHandler>();
builder.Services.AddSingleton<OwnS3.App.Handlers.ObjectHandlers.PutObjectHandler>();
builder.Services.AddSingleton<OwnS3.App.Handlers.ObjectHandlers.GetObjectHandler>();
builder.Services.AddSingleton<OwnS3.App.Handlers.ObjectHandlers.HeadObjectHandler>();
builder.Services.AddSingleton<OwnS3.App.Handlers.ObjectHandlers.DeleteObjectHandler>();
builder.Services.AddSingleton<OwnS3.App.Handlers.ObjectHandlers.DeleteObjectsHandler>();
builder.Services.AddSingleton<OwnS3.App.Handlers.ObjectHandlers.CopyObjectHandler>();
builder.Services.AddSingleton<OwnS3.App.Handlers.ObjectHandlers.GetObjectAttributesHandler>();
builder.Services.AddSingleton<OwnS3.App.Handlers.ListHandlers.ListObjectsHandler>();
builder.Services.AddSingleton<OwnS3.App.Handlers.ListHandlers.ListObjectsV2Handler>();
builder.Services.AddSingleton<OwnS3.App.Handlers.ListHandlers.ListObjectVersionsHandler>();
builder.Services.AddSingleton<OwnS3.App.Handlers.MultipartHandlers.CreateMultipartUploadHandler>();
builder.Services.AddSingleton<OwnS3.App.Handlers.MultipartHandlers.UploadPartHandler>();
builder.Services.AddSingleton<OwnS3.App.Handlers.MultipartHandlers.CompleteMultipartUploadHandler>();
builder.Services.AddSingleton<OwnS3.App.Handlers.MultipartHandlers.AbortMultipartUploadHandler>();
builder.Services.AddSingleton<OwnS3.App.Handlers.MultipartHandlers.ListPartsHandler>();
builder.Services.AddSingleton<OwnS3.App.Handlers.MultipartHandlers.ListMultipartUploadsHandler>();
builder.Services.AddSingleton<OwnS3.App.Handlers.IOperationHandler>(sp =>
    sp.GetRequiredService<OwnS3.App.Handlers.BucketHandlers.ListBucketsHandler>());
builder.Services.AddSingleton<OwnS3.App.Handlers.IOperationHandler>(sp =>
    sp.GetRequiredService<OwnS3.App.Handlers.BucketHandlers.CreateBucketHandler>());
builder.Services.AddSingleton<OwnS3.App.Handlers.IOperationHandler>(sp =>
    sp.GetRequiredService<OwnS3.App.Handlers.BucketHandlers.DeleteBucketHandler>());
builder.Services.AddSingleton<OwnS3.App.Handlers.IOperationHandler>(sp =>
    sp.GetRequiredService<OwnS3.App.Handlers.BucketHandlers.HeadBucketHandler>());
builder.Services.AddSingleton<OwnS3.App.Handlers.IOperationHandler>(sp =>
    sp.GetRequiredService<OwnS3.App.Handlers.BucketHandlers.GetBucketLocationHandler>());
builder.Services.AddSingleton<OwnS3.App.Handlers.IOperationHandler>(sp =>
    sp.GetRequiredService<OwnS3.App.Handlers.ObjectHandlers.PutObjectHandler>());
builder.Services.AddSingleton<OwnS3.App.Handlers.IOperationHandler>(sp =>
    sp.GetRequiredService<OwnS3.App.Handlers.ObjectHandlers.GetObjectHandler>());
builder.Services.AddSingleton<OwnS3.App.Handlers.IOperationHandler>(sp =>
    sp.GetRequiredService<OwnS3.App.Handlers.ObjectHandlers.HeadObjectHandler>());
builder.Services.AddSingleton<OwnS3.App.Handlers.IOperationHandler>(sp =>
    sp.GetRequiredService<OwnS3.App.Handlers.ObjectHandlers.DeleteObjectHandler>());
builder.Services.AddSingleton<OwnS3.App.Handlers.IOperationHandler>(sp =>
    sp.GetRequiredService<OwnS3.App.Handlers.ObjectHandlers.DeleteObjectsHandler>());
builder.Services.AddSingleton<OwnS3.App.Handlers.IOperationHandler>(sp =>
    sp.GetRequiredService<OwnS3.App.Handlers.ObjectHandlers.CopyObjectHandler>());
builder.Services.AddSingleton<OwnS3.App.Handlers.IOperationHandler>(sp =>
    sp.GetRequiredService<OwnS3.App.Handlers.ObjectHandlers.GetObjectAttributesHandler>());
builder.Services.AddSingleton<OwnS3.App.Handlers.IOperationHandler>(sp =>
    sp.GetRequiredService<OwnS3.App.Handlers.ListHandlers.ListObjectsHandler>());
builder.Services.AddSingleton<OwnS3.App.Handlers.IOperationHandler>(sp =>
    sp.GetRequiredService<OwnS3.App.Handlers.ListHandlers.ListObjectsV2Handler>());
builder.Services.AddSingleton<OwnS3.App.Handlers.IOperationHandler>(sp =>
    sp.GetRequiredService<OwnS3.App.Handlers.ListHandlers.ListObjectVersionsHandler>());
builder.Services.AddSingleton<OwnS3.App.Handlers.IOperationHandler>(sp =>
    sp.GetRequiredService<OwnS3.App.Handlers.MultipartHandlers.CreateMultipartUploadHandler>());
builder.Services.AddSingleton<OwnS3.App.Handlers.IOperationHandler>(sp =>
    sp.GetRequiredService<OwnS3.App.Handlers.MultipartHandlers.UploadPartHandler>());
builder.Services.AddSingleton<OwnS3.App.Handlers.IOperationHandler>(sp =>
    sp.GetRequiredService<OwnS3.App.Handlers.MultipartHandlers.CompleteMultipartUploadHandler>());
builder.Services.AddSingleton<OwnS3.App.Handlers.IOperationHandler>(sp =>
    sp.GetRequiredService<OwnS3.App.Handlers.MultipartHandlers.AbortMultipartUploadHandler>());
builder.Services.AddSingleton<OwnS3.App.Handlers.IOperationHandler>(sp =>
    sp.GetRequiredService<OwnS3.App.Handlers.MultipartHandlers.ListPartsHandler>());
builder.Services.AddSingleton<OwnS3.App.Handlers.IOperationHandler>(sp =>
    sp.GetRequiredService<OwnS3.App.Handlers.MultipartHandlers.ListMultipartUploadsHandler>());

// Kestrel: any-IP, h1+h2c (HTTP) / h2-by-ALPN (HTTPS) (arch/owns3/03 §6),
// лимит тела отключён — лимит 5 ГБ уровня хендлера (глава 02), не транспорта.
// TLS-опция (t42): сертификат резолвится ниже ПОСЛЕ Build (fail-fast) но ДО
// старта прослушивания — Kestrel строит bindings при RunAsync, замыкание
// читает уже заполненную переменную.
X509Certificate2? tlsCert = null;
builder.WebHost.ConfigureKestrel(o =>
{
    o.Limits.MaxRequestBodySize = null;
    o.ConfigureHttpsDefaults(h => h.ServerCertificate = tlsCert);
});

var app = builder.Build();
var options = app.Services.GetRequiredService<IOptions<OwnS3Options>>().Value;

// TLS-материал (t42, arch/owns3/03 §6 / 05 §4): резолв fail-fast ДО старта
// прослушивания — неполный/битый материал = Critical + exit 1 (как режим/том
// ниже), НЕ «молча на HTTP». Материал задан — HTTPS-only на том же порту.
try
{
    tlsCert = OwnS3Tls.Resolve(options.Tls);
}
catch (Exception ex)
{
    app.Logger.LogCritical(ex, "OwnS3:Tls: невалидный TLS-материал — отказ старта (env OWNS3_TLS_*)");
    Environment.Exit(1);
}

app.Urls.Clear();
app.Urls.Add($"{(tlsCert is null ? "http" : "https")}://*:{options.Server.Port}");

// S3-конвейер: RequestId → OPTIONS → роутер → подпись → права → хендлер →
// ошибки → метрики/лог. Map-эндпоинты исполняются в конце; изоляция служебных
// путей — шаг 0 S3Middleware.
app.UseMiddleware<S3Middleware>();

// Режим и том (arch/owns3/04 §3/§6, 06 §1): fail-fast до старта прослушивания
// Kestrel — к этому моменту конфигурация слита целиком (прод — env/appsettings,
// тесты — in-memory WAF); невалидный конфиг/том = диагностика + ненулевой exit
// (спека §6.1). Драйв уже в DI — тот же инстанс, который получат XlObjectStore
// и VolumeCleanupService.
IXlDrive drive = null!; // присваивается ниже; провал валидации — Environment.Exit
try
{
    _ = app.Services.GetRequiredService<StorageModeInfo>(); // парсинг/валидации режима
    drive = app.Services.GetRequiredService<IXlDrive>();
    drive.Initialize();
}
catch (Exception ex)
{
    app.Logger.LogCritical(ex, "Конфигурация/том ownS3 (DataDir={DataDir}) невалидны — отказ старта",
        options.DataDir);
    Environment.Exit(1);
}

// EC-режим: инструменты OwnS3EcMetrics создаются конструктором (observable-gauge
// + счётчики heal), класс никто не инжектит — без явного инстанса серии не
// экспортировались бы. Meter в DI — только при включённых метриках (05 §5).
if (app.Services.GetRequiredService<StorageModeInfo>().IsErasure
    && app.Services.GetRequiredService<Shared.Metrics.MetricsOptions>().Enabled)
    _ = app.Services.GetRequiredService<OwnS3EcMetrics>();

// Вне S3-конвейера: healthz (том валиден + touch-проба записи, спека §6.2)
// и metrics.
app.MapGet("/healthz", () => drive.CheckHealth()
    ? Results.Ok()
    : Results.StatusCode(StatusCodes.Status503ServiceUnavailable));
app.MapAppMetrics();

await app.RunAsync();

/// <summary>Маркер для WebApplicationFactory интеграционных тестов.</summary>
public partial class Program;
