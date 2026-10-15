using AdminPanel.Core;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace AdminPanel.Probes.S3;

/// <summary>Объект list-v2 (on-demand пагинация — ContinuationToken наружу).</summary>
public sealed record S3ObjectInfo(string Key, long SizeBytes, long LastModifiedUnix);

/// <summary>Страница list-v2.</summary>
public sealed record S3Page(IReadOnlyList<S3ObjectInfo> Items, string? NextContinuationToken);

/// <summary>Read-only клиент MinIO/S3 панели (t08, adminpanel/02 §2.5): контракт
/// уровня кода — только ListBuckets/ListObjectsV2 и публичные health-эндпоинты;
/// пишущих/удаляющих методов и admin-API НЕТ (они — только воркер, arch/19 §4/§5).
/// Осознанный дубль PgWorker.Backups.BackupS3 (панель не ссылается на PgWorker.*;
/// унификация etcd-каркаса — t08, 2026-09-14; MinIO-клиент выведен за её скоуп).</summary>
public interface IS3StorageClient
{
    Task<Result<IReadOnlyList<string>>> ListBucketsAsync(CancellationToken ct);

    /// <summary>Один list-objects-v2 (пагинация наружу — IsTruncated → токен);
    /// prefix null — весь bucket.</summary>
    Task<Result<S3Page>> ListPageAsync(
        string? prefix, string? continuationToken, int maxKeys, CancellationToken ct);

    /// <summary>Health: ApiOk — ListBuckets прошёл (API жив + креды валидны),
    /// LiveOk — GET /s3/health/live, 404/403 → фолбэк GET /healthz (канон
    /// ownS3; HttpClient, без SigV4; 403 — канонический ответ ownS3 на
    /// анонимный запрос вне /healthz и /metrics); ClusterOk — GET
    /// /s3/health/cluster, null — эндпоинт не отвечает/не поддерживается
    /// (вкл. 404/403 — не ownS3-контур); drives-поля тела cluster парсятся
    /// толерантно (нет полей/битый JSON → Drives null).</summary>
    Task<S3Health> GetHealthAsync(CancellationToken ct);
}

public sealed class S3StorageClient : IS3StorageClient, IAsyncDisposable
{
    public const string HealthHttpClientName = "s3-health";

    private readonly bool _configured;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly string _bucket;
    // t36 (02 §2.5): цели инвентаря — TargetList() (Endpoints ?? [Endpoint]);
    // клиенты per-цели — лениво (перебор с первого доступного).
    private readonly string[] _endpoints;
    private readonly string _accessKey;
    private readonly string _secretKey;
    private readonly string? _region;
    private readonly bool _pathStyle;
    private readonly TimeSpan _timeout;
    private readonly Func<string, AmazonS3Client> _targetClientFactory;
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, AmazonS3Client> _targetClients = new();

    // Единственный ctor (DI): health-пробы идут через именованный HttpClient
    // "s3-health" — фикс интеграции AC2/AC3 (t08): DI выбирал 1-арговый ctor
    // (BackupsS3Options не резолвится) и клиент жил с _httpClientFactory = null,
    // health-пробы молча не выполнялись (LiveOk всегда false).
    public S3StorageClient(IOptions<BackupsS3Options> options, IHttpClientFactory httpClientFactory)
        : this(options.Value, httpClientFactory)
    {
    }

    // Юнит-шов (t36): подмена клиента per-цели — тесты перебора строят
    // фейк-подкласс AmazonS3Client (методы виртуальные) вместо транспорта.
    internal S3StorageClient(
        BackupsS3Options options, IHttpClientFactory httpClientFactory,
        Func<string, AmazonS3Client> targetClientFactory)
        : this(options, httpClientFactory)
    {
        _targetClientFactory = targetClientFactory;
    }

    // httpClientFactory — именованный HttpClient "s3-health" (без SigV4).
    public S3StorageClient(BackupsS3Options options, IHttpClientFactory httpClientFactory)
    {
        _bucket = options.S3.Bucket;
        _httpClientFactory = httpClientFactory;
        _accessKey = options.S3.AccessKey;
        _secretKey = options.S3.SecretKey;
        _region = options.S3.Region;
        _pathStyle = options.S3.PathStyle;
        _timeout = TimeSpan.FromSeconds(options.TimeoutSec > 0 ? options.TimeoutSec : 5);
        _endpoints = options.S3.TargetList();
        _targetClientFactory = BuildClient;

        // AC1: пустой Endpoint — грань выключена, панель обязана стартовать;
        // AWSSDK-клиент без ServiceURL не конструируется — цели строятся ТОЛЬКО
        // при конфигурации (все вызовы гвардятся IsConfigured в loop/handler'е).
        _configured = !string.IsNullOrWhiteSpace(options.S3.Endpoint);
        if (!_configured)
            return;
    }

    // Вызов при выключенной грани — ошибка вызова, не транспорта (callers guard
    // IsConfigured; сюда можно попасть только в обход гварда).
    private static Result<T> NotConfigured<T>() => Result<T>.Failed(
        new InvalidOperationException("S3 endpoint не задан — грань «Хранилище бэкапов» выключена"));

    // t36: перебор целей с первого доступного (list-семантика 02 §2.5) —
    // первая Success выигрывает; исчерпание — последняя ошибка (алерт
    // backup-s3-unreachable видит текст).
    private async Task<Result<T>> AnyTargetAsync<T>(
        Func<string, AmazonS3Client, Task<Result<T>>> call, string what)
    {
        Result<T> last = default;
        foreach (var endpoint in _endpoints)
        {
            var client = _targetClients.GetOrAdd(endpoint, _targetClientFactory);
            var result = await call(endpoint, client);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return Result<T>.Failed(new ApplicationException(
            $"{what}: все цели инвентаря недоступны: {last.Error!.Message}"));
    }

    // Клиент одной цели (креды общие per-install, ServiceURL — по цели).
    private AmazonS3Client BuildClient(string endpoint)
        => new(
            new BasicAWSCredentials(_accessKey, _secretKey),
            new AmazonS3Config
            {
                ServiceURL = endpoint,
                ForcePathStyle = _pathStyle,
                AuthenticationRegion = _region,
                MaxErrorRetry = 0,
                Timeout = _timeout,
            });

    public async Task<Result<IReadOnlyList<string>>> ListBucketsAsync(CancellationToken ct)
    {
        if (!_configured)
            return NotConfigured<IReadOnlyList<string>>();
        // t36: первая доступная цель (перебор Endpoints, 02 §2.5).
        return await AnyTargetAsync(async (_, client) =>
        {
            try
            {
                var response = await client.ListBucketsAsync(ct);
                return Result<IReadOnlyList<string>>.Success(
                    response.Buckets.Select(b => b.BucketName).ToList());
            }
            catch (Exception e)
            {
                return Result<IReadOnlyList<string>>.Failed(
                    new ApplicationException($"S3 list-buckets: {e.Message}", e));
            }
        }, "S3 list-buckets");
    }

    public async Task<Result<S3Page>> ListPageAsync(
        string? prefix, string? continuationToken, int maxKeys, CancellationToken ct)
    {
        if (!_configured)
            return NotConfigured<S3Page>();
        // t36: первая доступная цель; токен страницы валиден в рамках ОДНОЙ
        // цели — перебор идёт только до первого успеха (дальше цель фикс).
        return await AnyTargetAsync(async (_, client) =>
        {
            try
            {
                var response = await client.ListObjectsV2Async(new ListObjectsV2Request
                {
                    BucketName = _bucket,
                    Prefix = prefix,
                    ContinuationToken = continuationToken,
                    MaxKeys = maxKeys,
                }, ct);
                var items = response.S3Objects
                    .Select(o => new S3ObjectInfo(
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
        }, "S3 list-v2");
    }

    public async Task<S3Health> GetHealthAsync(CancellationToken ct)
    {
        var buckets = await ListBucketsAsync(ct);
        var live = await ProbeHealthAsync("/s3/health/live", withDrives: false, ct);
        // ownS3-фолбэк: 404/403 = «хранилище не MinIO» (эндпоинт жив, путь
        // не поддерживается): 404 — несуществующий путь, 403 — канонический
        // ответ ownS3 на анонимный запрос вне /healthz и /metrics
        // (arch/owns3/03; пробы идут без SigV4). Повтор — по канону ownS3
        // /healthz (без подписи); 200 → live, 503/иное → down. Фолбэк
        // ТОЛЬКО по статусу ответа: мёртвый endpoint (нет ответа) вторую
        // пробу не крутит.
        if (live.StatusCode is 404 or 403)
            live = await ProbeHealthAsync("/healthz", withDrives: false, ct);
        var cluster = await ProbeHealthAsync("/s3/health/cluster", withDrives: true, ct);

        // 200 → true; 503 → false (degraded); нет ответа или 404/403
        // (эндпоинт не поддерживается — не ownS3-контур) → null; drives
        // при 404/403 нет.
        bool? clusterOk = cluster.StatusCode is null or 404 or 403
            ? null
            : cluster.StatusCode == 200;

        return new S3Health(
            buckets.IsSuccess,
            buckets.IsSuccess ? null : buckets.Error!.Message,
            live.StatusCode == 200,
            clusterOk,
            cluster.StatusCode is 404 or 403 ? null : cluster.Drives);
    }

    // GET {endpoint}{relativePath}; отсутствие ответа — null-статус; тело
    // (только cluster) парсится толерантно — битый JSON → drives null.
    // t36: перебор целей с первого ответившего (list-семантика 02 §2.5) —
    // мёртвая первая цель не роняет live/cluster-грань; фолбэк live→healthz
    // остаётся пробой по статусу (мёртвый endpoint вторую пробу не крутит).
    private async Task<(int? StatusCode, S3Drives? Drives)> ProbeHealthAsync(
        string relativePath, bool withDrives, CancellationToken ct)
    {
        foreach (var endpoint in _endpoints)
        {
            try
            {
                var client = _httpClientFactory.CreateClient(HealthHttpClientName);
                using var response = await client.GetAsync(
                    new Uri($"{endpoint}{relativePath}"), ct);
                S3Drives? drives = null;
                if (withDrives)
                {
                    var body = await response.Content.ReadAsStringAsync(ct);
                    drives = ParseDrives(body);
                }

                return ((int)response.StatusCode, drives);
            }
            catch (Exception)
            {
                // Сетевой сбой/таймаут — «эндпоинт не отвечает», следующая цель.
            }
        }

        return (null, null);
    }

    private static S3Drives? ParseDrives(string? body)
    {
        if (string.IsNullOrWhiteSpace(body))
            return null;
        try
        {
            using var doc = JsonDocument.Parse(body);
            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
                return null;
            return new S3Drives(
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
        foreach (var client in _targetClients.Values)
            client.Dispose();
        return ValueTask.CompletedTask;
    }
}
