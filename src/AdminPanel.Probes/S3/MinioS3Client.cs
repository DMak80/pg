using AdminPanel.Core;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace AdminPanel.Probes.S3;

/// <summary>Объект list-v2 (on-demand пагинация — ContinuationToken наружу).</summary>
public sealed record MinioObjectInfo(string Key, long SizeBytes, long LastModifiedUnix);

/// <summary>Страница list-v2.</summary>
public sealed record S3Page(IReadOnlyList<MinioObjectInfo> Items, string? NextContinuationToken);

/// <summary>Read-only клиент MinIO/S3 панели (t08, adminpanel/02 §2.5): контракт
/// уровня кода — только ListBuckets/ListObjectsV2 и публичные health-эндпоинты;
/// пишущих/удаляющих методов и admin-API НЕТ (они — только воркер, arch/19 §4/§5).
/// Осознанный дубль PgWorker.Backups.BackupS3 (панель не ссылается на PgWorker.*;
/// унификация — t08-unify-adminpanel-duplicates).</summary>
public interface IMinioS3
{
    Task<Result<IReadOnlyList<string>>> ListBucketsAsync(CancellationToken ct);

    /// <summary>Один list-objects-v2 (пагинация наружу — IsTruncated → токен);
    /// prefix null — весь bucket.</summary>
    Task<Result<S3Page>> ListPageAsync(
        string? prefix, string? continuationToken, int maxKeys, CancellationToken ct);

    /// <summary>Health: ApiOk — ListBuckets прошёл (API жив + креды валидны),
    /// LiveOk/ClusterOk — GET /minio/health/live|cluster (HttpClient, без SigV4);
    /// ClusterOk null — эндпоинт не отвечает/не поддерживается; drives-поля тела
    /// cluster парсятся толерантно (нет полей/битый JSON → Drives null).</summary>
    Task<MinioHealth> GetHealthAsync(CancellationToken ct);
}

public sealed class MinioS3Client : IMinioS3, IAsyncDisposable
{
    public const string HealthHttpClientName = "minio-health";

    private readonly AmazonS3Client? _client;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly string _endpoint;
    private readonly string _bucket;

    // Единственный ctor (DI): health-пробы идут через именованный HttpClient
    // "minio-health" — фикс интеграции AC2/AC3 (t08): DI выбирал 1-арговый ctor
    // (MinioOptions не резолвится) и клиент жил с _httpClientFactory = null,
    // health-пробы молча не выполнялись (LiveOk всегда false).
    public MinioS3Client(IOptions<MinioOptions> options, IHttpClientFactory httpClientFactory)
        : this(options.Value, httpClientFactory)
    {
    }

    // httpClientFactory — именованный HttpClient "minio-health" (без SigV4).
    public MinioS3Client(MinioOptions options, IHttpClientFactory httpClientFactory)
    {
        _endpoint = options.S3.Endpoint.TrimEnd('/');
        _bucket = options.S3.Bucket;
        _httpClientFactory = httpClientFactory;

        // AC1: пустой Endpoint — грань выключена, панель обязана стартовать;
        // AWSSDK-клиент без ServiceURL не конструируется — строим его ТОЛЬКО
        // при конфигурации (все вызовы гвардятся IsConfigured в loop/handler'е).
        if (string.IsNullOrWhiteSpace(options.S3.Endpoint))
            return;

        // Таймауты HTTP-вызовов S3 — из настроек (<= 0 — дефолт 5 c, образец
        // именованного HttpClient в ModuleExtensions).
        var timeoutSeconds = options.TimeoutSec > 0 ? options.TimeoutSec : 5;
        var config = new AmazonS3Config
        {
            ServiceURL = options.S3.Endpoint,
            ForcePathStyle = options.S3.PathStyle,
            AuthenticationRegion = options.S3.Region,

            // Тик — свой retry-механизм (ConsecutiveFailures, период 60 c):
            // SDK-ретраи с бэкоффом только растягивали сбойный тик на минуты.
            MaxErrorRetry = 0,
            Timeout = TimeSpan.FromSeconds(timeoutSeconds),
        };
        _client = new AmazonS3Client(
            new BasicAWSCredentials(options.S3.AccessKey, options.S3.SecretKey), config);
    }

    // Вызов при выключенной грани — ошибка вызова, не транспорта (callers guard
    // IsConfigured; сюда можно попасть только в обход гварда).
    private static Result<T> NotConfigured<T>() => Result<T>.Failed(
        new InvalidOperationException("S3 endpoint не задан — грань «Хранилище бэкапов» выключена"));

    public async Task<Result<IReadOnlyList<string>>> ListBucketsAsync(CancellationToken ct)
    {
        if (_client is null)
            return NotConfigured<IReadOnlyList<string>>();
        try
        {
            var response = await _client.ListBucketsAsync(ct);
            return Result<IReadOnlyList<string>>.Success(
                response.Buckets.Select(b => b.BucketName).ToList());
        }
        catch (Exception e)
        {
            return Result<IReadOnlyList<string>>.Failed(
                new ApplicationException($"S3 list-buckets: {e.Message}", e));
        }
    }

    public async Task<Result<S3Page>> ListPageAsync(
        string? prefix, string? continuationToken, int maxKeys, CancellationToken ct)
    {
        if (_client is null)
            return NotConfigured<S3Page>();
        try
        {
            var response = await _client.ListObjectsV2Async(new ListObjectsV2Request
            {
                BucketName = _bucket,
                Prefix = prefix,
                ContinuationToken = continuationToken,
                MaxKeys = maxKeys,
            }, ct);
            var items = response.S3Objects
                .Select(o => new MinioObjectInfo(
                    o.Key, o.Size,
                    new DateTimeOffset(o.LastModified.ToUniversalTime(), TimeSpan.Zero)
                        .ToUnixTimeSeconds()))
                .ToList();
            return Result<S3Page>.Success(new S3Page(
                items,
                response.IsTruncated is true ? response.NextContinuationToken : null));
        }
        catch (Exception e)
        {
            return Result<S3Page>.Failed(
                new ApplicationException($"S3 list-v2 (prefix={prefix ?? "<root>"}): {e.Message}", e));
        }
    }

    public async Task<MinioHealth> GetHealthAsync(CancellationToken ct)
    {
        var buckets = await ListBucketsAsync(ct);
        var live = await ProbeHealthAsync("live", withDrives: false, ct);
        var cluster = await ProbeHealthAsync("cluster", withDrives: true, ct);

        // 200 → true; 503 → false (degraded); иного/нет ответа → null (не отвечает).
        bool? clusterOk = cluster.StatusCode is null ? null : cluster.StatusCode == 200;

        return new MinioHealth(
            buckets.IsSuccess,
            buckets.IsSuccess ? null : buckets.Error!.Message,
            live.StatusCode == 200,
            clusterOk,
            cluster.Drives);
    }

    // GET {endpoint}/minio/health/<path>; отсутствие ответа — null-статус;
    // тело (только cluster) парсится толерантно — битый JSON → drives null.
    private async Task<(int? StatusCode, MinioDrives? Drives)> ProbeHealthAsync(
        string path, bool withDrives, CancellationToken ct)
    {
        try
        {
            var client = _httpClientFactory.CreateClient(HealthHttpClientName);
            using var response = await client.GetAsync(
                new Uri($"{_endpoint}/minio/health/{path}"), ct);
            MinioDrives? drives = null;
            if (withDrives)
            {
                var body = await response.Content.ReadAsStringAsync(ct);
                drives = ParseDrives(body);
            }

            return ((int)response.StatusCode, drives);
        }
        catch (Exception)
        {
            // Сетевой сбой/таймаут — «эндпоинт не отвечает», не исключение наружу.
            return (null, null);
        }
    }

    private static MinioDrives? ParseDrives(string? body)
    {
        if (string.IsNullOrWhiteSpace(body))
            return null;
        try
        {
            using var doc = JsonDocument.Parse(body);
            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
                return null;
            return new MinioDrives(
                OptionalLong(root, "healthyDrives"),
                OptionalLong(root, "offlineDrives"),
                OptionalLong(root, "healingDrives"),
                OptionalLong(root, "totalDrives"));
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static long? OptionalLong(JsonElement root, string name)
        => root.TryGetProperty(name, out var v)
           && v.ValueKind == JsonValueKind.Number
           && v.TryGetInt64(out var parsed)
            ? parsed
            : null;

    public ValueTask DisposeAsync()
    {
        _client?.Dispose();
        return ValueTask.CompletedTask;
    }
}
