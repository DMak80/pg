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

/// <summary>Read-only клиент S3-хранилища (ownS3) панели (adminpanel/02 §2.5): контракт
/// уровня кода — только ListBuckets/ListObjectsV2 и публичные health-эндпоинты;
/// пишущих/удаляющих методов и admin-API НЕТ (они — только воркер, arch/19 §4/§5).
/// Осознанный дубль PgWorker.Backups.BackupS3 (панель не ссылается на PgWorker.*).</summary>
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

    /// <summary>Подмена целей инвентаря (кластер/endpoints/креды из etcd);
    /// null — грань «Backup Storage» выключена. Смена объекта целей сбрасывает
    /// кэш клиентов per-цель.</summary>
    void UpdateTargets(S3StorageTargets? targets);
}

public sealed class S3StorageClient : IS3StorageClient, IAsyncDisposable
{
    public const string HealthHttpClientName = "s3-health";

    // Цели инвентаря (кластер/endpoints/креды) приходят снаружи — UpdateTargets
    // (push из etcd-наблюдения), НЕ из BackupsS3Options; до первого push'а грань
    // «Backup Storage» выключена (все вызовы гвардятся _targets is null).
    private volatile S3StorageTargets? _targets;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly string _bucket;
    private readonly string? _region;
    private readonly bool _pathStyle;
    private readonly TimeSpan _timeout;
    private readonly Func<string, AmazonS3Client> _targetClientFactory;
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, AmazonS3Client> _targetClients = new();

    // Единственный ctor (DI): health-пробы идут через именованный HttpClient
    // "s3-health" — фикс интеграции AC2/AC3 : DI выбирал 1-арговый ctor
    // (BackupsS3Options не резолвится) и клиент жил с _httpClientFactory = null,
    // health-пробы молча не выполнялись (LiveOk всегда false).
    public S3StorageClient(IOptions<BackupsS3Options> options, IHttpClientFactory httpClientFactory)
        : this(options.Value, httpClientFactory)
    {
    }

    // Юнит-шов : подмена клиента per-цели — тесты перебора строят
    // фейк-подкласс AmazonS3Client (методы виртуальные) вместо транспорта.
    internal S3StorageClient(
        BackupsS3Options options, IHttpClientFactory httpClientFactory,
        Func<string, AmazonS3Client> targetClientFactory)
        : this(options, httpClientFactory)
    {
        _targetClientFactory = targetClientFactory;
    }

    // httpClientFactory — именованный HttpClient "s3-health" (без SigV4).
    // Из options — только грань-константы (Bucket/Region/PathStyle/TimeoutSec);
    // креды/endpoints приходят позже через UpdateTargets — до него грань
    // выключена (вызовы гвардятся _targets is null в loop/handler'е).
    public S3StorageClient(BackupsS3Options options, IHttpClientFactory httpClientFactory)
    {
        _bucket = options.S3.Bucket;
        _httpClientFactory = httpClientFactory;
        _region = options.S3.Region;
        _pathStyle = options.S3.PathStyle;
        _timeout = TimeSpan.FromSeconds(options.TimeoutSec > 0 ? options.TimeoutSec : 5);
        _targetClientFactory = BuildClient;
    }

    // Подмена целей инвентаря (кластер/endpoints/креды): null — грань выключена;
    // смена объекта целей сбрасывает кэш клиентов per-цели (старые клиенты
    // освобождаются — держат транспорт и креды; тот же объект — no-op).
    // Креды дальше конструктора AmazonS3Client не идут (ни в исключения, ни в логи).
    public void UpdateTargets(S3StorageTargets? targets)
    {
        if (ReferenceEquals(_targets, targets))
            return;
        _targets = targets;
        foreach (var client in _targetClients.Values)
            client.Dispose();
        _targetClients.Clear();
    }

    // Вызов при выключенной грани — ошибка вызова, не транспорта (callers guard
    // _targets is null; сюда можно попасть только в обход гварда).
    private static Result<T> NotConfigured<T>() => Result<T>.Failed(
        new InvalidOperationException("S3 endpoint not set — the «Backup Storage» facet is disabled"));

    // перебор целей с первого доступного (list-семантика 02 §2.5) —
    // первая Success выигрывает; исчерпание — последняя ошибка (алерт
    // backup-s3-unreachable видит текст).
    private async Task<Result<T>> AnyTargetAsync<T>(
        S3StorageTargets targets, Func<string, AmazonS3Client, Task<Result<T>>> call, string what)
    {
        Result<T> last = default;
        foreach (var endpoint in targets.Endpoints)
        {
            var client = _targetClients.GetOrAdd(endpoint, _targetClientFactory);
            var result = await call(endpoint, client);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return Result<T>.Failed(new ApplicationException(
            $"{what}: all inventory targets are unreachable: {last.Error!.Message}"));
    }

    // Клиент одной цели (креды — из текущих целей, ServiceURL — по цели).
    private AmazonS3Client BuildClient(string endpoint)
    {
        var targets = _targets ?? throw new InvalidOperationException(
            "S3 endpoint not set — the «Backup Storage» facet is disabled");
        return new(
            new BasicAWSCredentials(targets.AccessKey, targets.SecretKey),
            new AmazonS3Config
            {
                ServiceURL = endpoint,
                ForcePathStyle = _pathStyle,
                AuthenticationRegion = _region,
                MaxErrorRetry = 0,
                Timeout = _timeout,
            });
    }

    public async Task<Result<IReadOnlyList<string>>> ListBucketsAsync(CancellationToken ct)
    {
        if (_targets is not { } targets)
            return NotConfigured<IReadOnlyList<string>>();
        // первая доступная цель (перебор Endpoints, 02 §2.5).
        return await AnyTargetAsync(targets, async (_, client) =>
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
        if (_targets is not { } targets)
            return NotConfigured<S3Page>();
        // первая доступная цель; токен страницы валиден в рамках ОДНОЙ
        // цели — перебор идёт только до первого успеха (дальше цель фикс).
        return await AnyTargetAsync(targets, async (_, client) =>
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
        // ownS3-фолбэк: 404/403 = проба ownS3 не поддержана (эндпоинт жив,
        // путь не поддерживается): 404 — несуществующий путь, 403 — канонический
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
    // перебор целей с первого ответившего (list-семантика 02 §2.5) —
    // мёртвая первая цель не роняет live/cluster-грань; фолбэк live→healthz
    // остаётся пробой по статусу (мёртвый endpoint вторую пробу не крутит).
    private async Task<(int? StatusCode, S3Drives? Drives)> ProbeHealthAsync(
        string relativePath, bool withDrives, CancellationToken ct)
    {
        var targets = _targets;
        if (targets is null)
            return (null, null);
        foreach (var endpoint in targets.Endpoints)
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
