using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using PgWorker.Core;

namespace PgWorker.Backups;

/// <summary>Объект WAL-префикса: имя (последний компонент ключа) + факт времени
/// последней модификации (для last_uploaded_unix — S3 истина, arch/19 §3).</summary>
public sealed record WalObject(string Name, DateTimeOffset LastModified);

/// <summary>Объект bucket с размером (ретенционные list'ы t06, arch/19 §5).</summary>
public sealed record S3ObjectInfo(string Key, long SizeBytes, DateTimeOffset LastModified);

/// <summary>Тонкая обёртка S3-клиента (t03): list-objects-v2 с пагинацией по
/// префиксу `<C>/<X>/wal/` + диагностика старта (BucketExists). Загрузку делает
/// mc внутри агента — других операций t03 не требует (spec §3.1). PathStyle —
/// MinIO и облако одним клиентом (ForcePathStyle). Создание bucket — забота
/// стенда/фикстур (прямой AWSSDK-клиент), НЕ интерфейс подсистемы.
/// t06: ListPrefixAsync/DeleteKeysAsync — ретенционные list с размерами и
/// batch-delete (S3-удаления ТОЛЬКО в ретенционных путях, R4).
/// t04: ListAsync/GetObjectAsync — verify-листинг относительно &lt;C&gt;/&lt;X&gt;/
/// и GET history; общий кор пагинации с t06-листом.
/// t05: ListFullsAsync/DownloadTextAsync — DR-поиск полных по CommonPrefixes и
/// чтение маленьких текстовых объектов (backup_label/backup_manifest);
/// restore S3 ТОЛЬКО читает — удаляющих операций тут нет (arch/19 §3.5).</summary>
public interface IBackupS3
{
    Task<Result<bool>> BucketExistsAsync(CancellationToken ct);

    /// <summary>maxKeysPerTest — инъекция размера страницы (тест пагинации); null — максимум.</summary>
    Task<Result<IReadOnlyList<WalObject>>> ListWalAsync(
        string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default);

    /// <summary>list-objects-v2 с пагинацией по произвольному префиксу (full/&lt;id&gt;/,
    /// wal/, "" — весь bucket) с размерами; maxKeysPerTest — инъекция страницы.
    /// Реализация — общий кор листинга с t04 ListAsync (без дублирования пагинации).</summary>
    Task<Result<IReadOnlyList<S3ObjectInfo>>> ListPrefixAsync(
        string prefix, int? maxKeysPerTest = null, CancellationToken ct = default);

    /// <summary>batch-delete (DeleteObjects, чанки ≤1000); идемпотентно —
    /// отсутствие ключа в ответе не ошибка (повтор прохода безопасен).</summary>
    Task<Result> DeleteKeysAsync(IReadOnlyList<string> keys, CancellationToken ct = default);

    /// <summary>Листинг произвольного префикса (t04, verify): prefix — относительно
    /// &lt;C&gt;/&lt;X&gt;/, напр. "wal/" | "full/&lt;id&gt;/pg_wal/"; та же пагинация list-v2
    /// через общий кор с t06 ListPrefixAsync.</summary>
    Task<Result<IReadOnlyList<WalObject>>> ListAsync(
        string cluster, string shard, string prefix, int? maxKeysPerTest = null, CancellationToken ct = default);

    /// <summary>Содержимое маленького объекта (t04: .history для строгих TLI-переходов);
    /// key — относительно &lt;C&gt;/&lt;X&gt;/, напр. "wal/00000002.history".</summary>
    Task<Result<string>> GetObjectAsync(string cluster, string shard, string key, CancellationToken ct = default);

    /// <summary>t05: id полных шарда по префиксу `<C>/<X>/full/` с Delimiter="/"
    /// (CommonPrefixes — каталоги `full/<id>/`), сортировка Ordinal; пагинация по
    /// IsTruncated/NextContinuationToken. DR-путь: etcd-статусов может не быть.</summary>
    Task<Result<IReadOnlyList<string>>> ListFullsAsync(
        string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default);

    /// <summary>t05: маленький текстовый объект (`objectKey` — путь внутри
    /// префикса шарда, напр. `full/&lt;id&gt;/backup_label`); отсутствующий —
    /// Failed (не исключение наружу — валидация restore разведает transien/permanent).</summary>
    Task<Result<string>> DownloadTextAsync(
        string cluster, string shard, string objectKey, CancellationToken ct = default);
}

public sealed class BackupS3 : IBackupS3, IAsyncDisposable
{
    private readonly AmazonS3Client _client;
    private readonly string _bucket;

    public BackupS3(BackupsRuntimeOptions options)
    {
        _bucket = options.S3Bucket;
        var config = new AmazonS3Config
        {
            ServiceURL = options.S3Endpoint,
            ForcePathStyle = options.S3PathStyle,
            AuthenticationRegion = options.S3Region,
        };
        _client = new AmazonS3Client(
            new BasicAWSCredentials(options.S3AccessKey, options.S3SecretKey), config);
    }

    public async Task<Result<bool>> BucketExistsAsync(CancellationToken ct)
    {
        try
        {
            await _client.ListBucketsAsync(ct);
            return Result<bool>.Success(true);
        }
        catch (AmazonS3Exception e) when (e.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return Result<bool>.Success(false);
        }
        catch (Exception e)
        {
            return Result<bool>.Failed(new ApplicationException($"S3 list-buckets: {e.Message}", e));
        }
    }

    public Task<Result<IReadOnlyList<WalObject>>> ListWalAsync(
        string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default)
        => ListAsync(cluster, shard, "wal/", maxKeysPerTest, ct);

    public async Task<Result<IReadOnlyList<WalObject>>> ListAsync(
        string cluster, string shard, string prefix, int? maxKeysPerTest = null, CancellationToken ct = default)
    {
        var fullPrefix = $"{cluster}/{shard}/{prefix}";
        var listed = await ListPrefixCoreAsync(fullPrefix, maxKeysPerTest, ct);
        if (!listed.IsSuccess)
            return Result<IReadOnlyList<WalObject>>.Failed(listed.Error!);
        var result = new List<WalObject>();
        foreach (var obj in listed.Value)
        {
            var name = obj.Key[(obj.Key.LastIndexOf('/') + 1)..];
            if (name.Length > 0)
                result.Add(new WalObject(name, obj.LastModified));
        }

        return Result<IReadOnlyList<WalObject>>.Success(result);
    }

    public async Task<Result<string>> GetObjectAsync(
        string cluster, string shard, string key, CancellationToken ct = default)
    {
        try
        {
            var response = await _client.GetObjectAsync(
                new GetObjectRequest { BucketName = _bucket, Key = $"{cluster}/{shard}/{key}" }, ct);
            using var reader = new StreamReader(response.ResponseStream, System.Text.Encoding.UTF8);
            return Result<string>.Success(await reader.ReadToEndAsync(ct));
        }
        catch (Exception e)
        {
            return Result<string>.Failed(new ApplicationException($"S3 get {key}: {e.Message}", e));
        }
    }

    public async Task<Result<IReadOnlyList<S3ObjectInfo>>> ListPrefixAsync(
        string prefix, int? maxKeysPerTest = null, CancellationToken ct = default)
    {
        var listed = await ListPrefixCoreAsync(prefix, maxKeysPerTest, ct);
        if (!listed.IsSuccess)
            return Result<IReadOnlyList<S3ObjectInfo>>.Failed(listed.Error!);
        return Result<IReadOnlyList<S3ObjectInfo>>.Success(listed.Value
            .Select(o => new S3ObjectInfo(o.Key, o.Size, o.LastModified))
            .ToList());
    }

    /// <summary>Общий кор листинга (t06 ListPrefixAsync / t04 ListAsync): постраничный
    /// list-objects-v2 по абсолютному префиксу bucket'а — сырые S3-объекты,
    /// маппинг в контракт каждого метода — снаружи (без дублирования пагинации).</summary>
    private async Task<Result<IReadOnlyList<S3Object>>> ListPrefixCoreAsync(
        string prefix, int? maxKeysPerTest, CancellationToken ct)
    {
        try
        {
            var result = new List<S3Object>();
            string? token = null;
            do
            {
                var request = new ListObjectsV2Request
                {
                    BucketName = _bucket,
                    Prefix = prefix,
                    ContinuationToken = token,
                };
                if (maxKeysPerTest is { } maxKeys)
                    request.MaxKeys = maxKeys;
                var page = await _client.ListObjectsV2Async(request, ct);
                result.AddRange(page.S3Objects);
                token = page.IsTruncated is true ? page.NextContinuationToken : null;
            }
            while (token is not null);

            return Result<IReadOnlyList<S3Object>>.Success(result);
        }
        catch (Exception e)
        {
            return Result<IReadOnlyList<S3Object>>.Failed(new ApplicationException(
                $"S3 list {prefix}: {e.Message}", e));
        }
    }

    public async Task<Result> DeleteKeysAsync(IReadOnlyList<string> keys, CancellationToken ct = default)
    {
        if (keys.Count == 0)
            return Result.Success();
        try
        {
            foreach (var chunk in keys.Chunk(1000))
            {
                var request = new DeleteObjectsRequest
                {
                    BucketName = _bucket,
                    Objects = chunk.Select(k => new KeyVersion { Key = k }).ToList(),
                };
                var response = await _client.DeleteObjectsAsync(request, ct);
                // Идемпотентность: отсутствующие ключи не приходят в ответ — не ошибка.
                if (response.DeleteErrors is { Count: > 0 } errors)
                    return Result.Failed(new ApplicationException(
                        $"S3 batch-delete: {errors[0].Key}: {errors[0].Message}"));
            }

            return Result.Success();
        }
        catch (Exception e)
        {
            return Result.Failed(new ApplicationException($"S3 batch-delete: {e.Message}", e));
        }
    }

    public async Task<Result<IReadOnlyList<string>>> ListFullsAsync(
        string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default)
    {
        try
        {
            var ids = new SortedSet<string>(StringComparer.Ordinal);
            string? token = null;
            do
            {
                var request = new ListObjectsV2Request
                {
                    BucketName = _bucket,
                    Prefix = $"{cluster}/{shard}/full/",
                    Delimiter = "/",
                    ContinuationToken = token,
                };
                if (maxKeysPerTest is { } maxKeys)
                    request.MaxKeys = maxKeys;
                var page = await _client.ListObjectsV2Async(request, ct);
                foreach (var common in page.CommonPrefixes)
                {
                    // common — каталог "…/full/<id>/": id = последний компонент без хвостового '/'
                    var id = common.TrimEnd('/');
                    id = id[(id.LastIndexOf('/') + 1)..];
                    if (id.Length > 0)
                        ids.Add(id);
                }

                token = page.IsTruncated is true ? page.NextContinuationToken : null;
            }
            while (token is not null);

            return Result<IReadOnlyList<string>>.Success([.. ids]);
        }
        catch (Exception e)
        {
            return Result<IReadOnlyList<string>>.Failed(new ApplicationException(
                $"S3 list {cluster}/{shard}/full/: {e.Message}", e));
        }
    }

    public async Task<Result<string>> DownloadTextAsync(
        string cluster, string shard, string objectKey, CancellationToken ct = default)
    {
        try
        {
            var response = await _client.GetObjectAsync(new GetObjectRequest
            {
                BucketName = _bucket,
                Key = $"{cluster}/{shard}/{objectKey}",
            }, ct);
            using var reader = new StreamReader(response.ResponseStream);
            return Result<string>.Success(await reader.ReadToEndAsync(ct));
        }
        catch (AmazonS3Exception e) when (e.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            // Отсутствующий объект — валидный «нет» (проверка факта manifest/label),
            // а не транспортный сбой: наружу — Failed без исключения.
            return Result<string>.Failed(new ApplicationException(
                $"S3 get {cluster}/{shard}/{objectKey}: not found"));
        }
        catch (Exception e)
        {
            return Result<string>.Failed(new ApplicationException(
                $"S3 get {cluster}/{shard}/{objectKey}: {e.Message}", e));
        }
    }

    public ValueTask DisposeAsync()
    {
        _client.Dispose();
        return ValueTask.CompletedTask;
    }
}
