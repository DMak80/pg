using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using OwnS3.App;
using OwnS3.App.Access;
using OwnS3.App.Pipeline;
using OwnS3.Storage;
using Shared.Metrics;

// Точка входа ownS3 (arch/owns3/01 §3, arch/owns3/05): Kestrel-хост S3-грани,
// конфигурация OwnS3:* / OWNS3_*, fail-fast root-пары, /healthz и /metrics.
// Протокольный конвейер (роутинг/подпись/права) — S3Middleware.

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<OwnS3Options>(builder.Configuration.GetSection(OwnS3Options.SectionName));
builder.Services.AddSingleton(TimeProvider.System);

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

// Домен: реестр учёток + объектный слой t36 — заглушка (реализация xl — t37).
builder.Services.AddSingleton<AccessKeyRegistry>();
builder.Services.AddSingleton<IObjectStore, NotWiredObjectStore>();
builder.Services.AddSingleton<OwnS3Metrics>();

// 22 хендлера протокольного контура (spec §3.4.1) — диспетчеризация по операции.
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
builder.Services.AddSingleton<OwnS3.App.Handlers.MultipartHandlers.UploadPartCopyHandler>();
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
    sp.GetRequiredService<OwnS3.App.Handlers.MultipartHandlers.UploadPartCopyHandler>());
builder.Services.AddSingleton<OwnS3.App.Handlers.IOperationHandler>(sp =>
    sp.GetRequiredService<OwnS3.App.Handlers.MultipartHandlers.CompleteMultipartUploadHandler>());
builder.Services.AddSingleton<OwnS3.App.Handlers.IOperationHandler>(sp =>
    sp.GetRequiredService<OwnS3.App.Handlers.MultipartHandlers.AbortMultipartUploadHandler>());
builder.Services.AddSingleton<OwnS3.App.Handlers.IOperationHandler>(sp =>
    sp.GetRequiredService<OwnS3.App.Handlers.MultipartHandlers.ListPartsHandler>());
builder.Services.AddSingleton<OwnS3.App.Handlers.IOperationHandler>(sp =>
    sp.GetRequiredService<OwnS3.App.Handlers.MultipartHandlers.ListMultipartUploadsHandler>());

// Kestrel: any-IP, h1+h2c (arch/owns3/03 §6), лимит тела отключён — лимит 5 ГБ
// уровня хендлера (глава 02), не транспорта.
builder.WebHost.ConfigureKestrel(o => o.Limits.MaxRequestBodySize = null);

var app = builder.Build();
var options = app.Services.GetRequiredService<IOptions<OwnS3Options>>().Value;
app.Urls.Clear();
app.Urls.Add($"http://*:{options.Server.Port}");

// S3-конвейер: RequestId → OPTIONS → роутер → подпись → права → хендлер →
// ошибки → метрики/лог. Map-эндпоинты исполняются в конце; изоляция служебных
// путей — шаг 0 S3Middleware.
app.UseMiddleware<S3Middleware>();

// Вне S3-конвейера: healthz (в t36 — 200 без валидации тома; том — t37) и metrics.
app.MapGet("/healthz", () => Results.Ok());
app.MapAppMetrics();

await app.RunAsync();

/// <summary>Маркер для WebApplicationFactory интеграционных тестов.</summary>
public partial class Program;
