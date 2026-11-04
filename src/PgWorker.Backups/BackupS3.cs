using System.Security.Cryptography;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using PgWorker.Core;

namespace PgWorker.Backups;

/// <summary>Объект WAL-префикса: имя (последний компонент ключа) + факт времени
/// последней модификации (для last_uploaded_unix — S3 истина, arch/19 §3).</summary>
public sealed record WalObject(string Name, DateTimeOffset LastModified);

/// <summary>Объект bucket с размером (ретенционные list'ы, arch/19 §5).</summary>
public sealed record S3ObjectInfo(string Key, long SizeBytes, DateTimeOffset LastModified);

/// <summary>Тонкая обёртка S3-клиента : list-objects-v2 с пагинацией по
/// префиксу `<C>/<X>/wal/` + диагностика старта (BucketExists). Загрузку делает
/// mc внутри агента — других операций не требует (spec §3.1). PathStyle —
/// ownS3 и облако одним клиентом (ForcePathStyle). Создание bucket — забота
/// стенда/фикстур (прямой AWSSDK-клиент), НЕ интерфейс подсистемы.
/// ListPrefixAsync/DeleteKeysAsync — ретенционные list с размерами и
/// batch-delete (S3-удаления ТОЛЬКО в ретенционных путях, R4).
/// ListAsync/GetObjectAsync — verify-листинг относительно &lt;C&gt;/&lt;X&gt;/
/// и GET history; общий кор пагинации с листом.
/// ListFullsAsync/DownloadTextAsync — DR-поиск полных по CommonPrefixes и
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
    /// Реализация — общий кор листинга с ListAsync (без дублирования пагинации).</summary>
    Task<Result<IReadOnlyList<S3ObjectInfo>>> ListPrefixAsync(
        string prefix, int? maxKeysPerTest = null, CancellationToken ct = default);

    /// <summary>batch-delete (DeleteObjects, чанки ≤1000); идемпотентно —
    /// отсутствие ключа в ответе не ошибка (повтор прохода безопасен).</summary>
    Task<Result> DeleteKeysAsync(IReadOnlyList<string> keys, CancellationToken ct = default);

    /// <summary>put байтов с SHA256-проверкой целостности транспорта (:
    /// экспорт etcd-слепков): ChecksumSHA256 уходит с запросом — сервер сверяет
    /// содержимое и отвергает put при несовпадении (BadDigest); успех = байты
    /// доставлены целыми. sha256=null — без проверки (мелкие служебные объекты).</summary>
    Task<Result> PutObjectAsync(string key, byte[] data, string? sha256, CancellationToken ct = default);

    /// <summary>Листинг произвольного префикса (verify): prefix — относительно
    /// &lt;C&gt;/&lt;X&gt;/, напр. "wal/" | "full/&lt;id&gt;/pg_wal/"; та же пагинация list-v2
    /// через общий кор с ListPrefixAsync.</summary>
    Task<Result<IReadOnlyList<WalObject>>> ListAsync(
        string cluster, string shard, string prefix, int? maxKeysPerTest = null, CancellationToken ct = default);

    /// <summary>Содержимое маленького объекта (.history для строгих TLI-переходов);
    /// key — относительно &lt;C&gt;/&lt;X&gt;/, напр. "wal/00000002.history".</summary>
    Task<Result<string>> GetObjectAsync(string cluster, string shard, string key, CancellationToken ct = default);

    /// <summary>put куска с Content-MD5 (spec §4.4, решение №22): MD5Digest
    /// (base64 БАЙТОВ хэша) уходит с запросом — транспорт отвергает битое;
    /// md5Hex — тот же расчёт, что в имени куска (один хэш на два потребителя).
    /// Таймауты/маршрут put — как у PutObjectAsync.</summary>
    Task<Result> PutObjectMd5Async(string key, byte[] data, string md5Hex, CancellationToken ct = default);

    /// <summary>Байты объекта в память + ETag (сборка fetch-wal, spec §4.4);
    /// key — относительно &lt;C&gt;/&lt;X&gt;, как GetObjectAsync; кавычки ETag MinIO сняты
    /// (ETag одиночного put = md5 содержимого — сверка безномерных legacy-версий).</summary>
    Task<Result<(byte[] Data, string? ETag)>> GetObjectBytesAsync(
        string cluster, string shard, string key, CancellationToken ct = default);

    /// <summary>: id полных шарда по префиксу `<C>/<X>/full/` с Delimiter="/"
    /// (CommonPrefixes — каталоги `full/<id>/`), сортировка Ordinal; пагинация по
    /// IsTruncated/NextContinuationToken. DR-путь: etcd-статусов может не быть.</summary>
    Task<Result<IReadOnlyList<string>>> ListFullsAsync(
        string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default);

    /// <summary>: маленький текстовый объект (`objectKey` — путь внутри
    /// префикса шарда, напр. `full/&lt;id&gt;/backup_label`); отсутствующий —
    /// Failed (не исключение наружу — валидация restore разведает transien/permanent).</summary>
    Task<Result<string>> DownloadTextAsync(
        string cluster, string shard, string objectKey, CancellationToken ct = default);

    /// <summary>: put файла 1:1 объектом (upload полного — arch/19 §2);
    /// key — ПОЛНЫЙ ключ в bucket (&lt;C&gt;/&lt;X&gt;/full/&lt;id&gt;/…). Содержимое —
    /// FileStream (файл не читается в память целиком — наборы бывают большими).</summary>
    Task<Result> PutFileAsync(string key, string filePath, CancellationToken ct = default);

    /// <summary>: скачивание объекта в файл (verify/restore — arch/19 §5/§3.5);
    /// key — относительно &lt;C&gt;/&lt;X&gt;/, как GetObjectAsync; ответ — через
    /// временный &lt;dest&gt;.part с атомарной заменой по успеху (обрыв не оставляет
    /// частичный файл; dest перезаписывается — ретраи идемпотентны).</summary>
    Task<Result> GetObjectToFileAsync(
        string cluster, string shard, string key, string destFilePath, CancellationToken ct = default);
}

/// <summary>Запись WAL-сегмента без разрушения чужой цепочки (arch/19 §3):
/// существующий объект с ИДЕНТИЧНЫМ содержимым — идемпотентный успех (повтор
/// put двух приёмников одного потока — канон идемпотентности); с ДРУГИМ
/// содержимым — отказ с маркером <see cref="WalArchiveConflict.Marker"/>
/// (перезапись запрещена: в общем S3-префиксе одинаковых имён сегментов может
/// лежать цепочка ДРУГОЙ инкарнации — DR-источник, который restore ещё должен
/// прочитать; PUT поверх неё уничтожает восстановимость).
/// Отдельный интерфейс (не метод IBackupS3): тестовые фейки подсистемы,
/// которым гвард не нужен, не обязаны его реализовывать.</summary>
public interface IWalSegmentGuard
{
    Task<Result> PutWalSegmentAsync(
        string key, byte[] data, string? sha256, CancellationToken ct = default);

    /// <summary>Файловая форма (дубль pg_wal полного в общий wal/): та же
    /// сверка содержимого по ETag, файл не читается в память целиком.</summary>
    Task<Result> PutWalSegmentFileAsync(
        string key, string filePath, CancellationToken ct = default);
}

/// <summary>Маркер permanent-конфликта WAL-архива (arch/19 §3): сегмент с тем
/// же именем уже лежит с другим содержимым — это НЕ transient (ретрай ничего
/// не изменит) и НЕ исчерпание пула: приёмник обязан замереть, а не
/// перезаписывать чужую цепочку.</summary>
public static class WalArchiveConflict
{
    public const string Marker = "wal-archive-conflict";

    public static bool Is(Result result)
        => !result.IsSuccess
           && result.Error!.Message.Contains(Marker, StringComparison.Ordinal);

    public static bool Is<T>(Result<T> result)
        => !result.IsSuccess
           && result.Error!.Message.Contains(Marker, StringComparison.Ordinal);
}

public sealed class BackupS3
    : IBackupS3, IWalSegmentGuard, IMultipartS3, IStreamObjectS3, IDeleteObjectS3, IAsyncDisposable
{
    private readonly AmazonS3Client _client;
    private readonly string _bucket;

    // Пер-попыточный таймаут S3-запроса (arch/14 §6 «пульс конечного внешнего
    // вызова», arch/19 §5): короче окна проверки watchdog — симметрия таймаутов
    // etcd/patroni/SQL. Дефолт — полокна (10 с при дефолтных опциях);
    // фабрика передаёт производный от Loops:Watchdog:CheckIntervalSec.
    private static readonly TimeSpan DefaultRequestTimeout = TimeSpan.FromSeconds(10);

    // цель пула (endpoint+креды одного адреса); PathStyle=true — общий
    // ownS3-контур (Region/PathStyle per-цель появятся с external-целями).
    public BackupS3(S3ClientOptions options, TimeSpan? requestTimeout = null, bool pathStyle = true)
    {
        _bucket = options.Bucket;
        var timeout = requestTimeout is { } t && t >= TimeSpan.FromSeconds(1) ? t : DefaultRequestTimeout;
        var config = new AmazonS3Config
        {
            ServiceURL = options.Endpoint,
            ForcePathStyle = pathStyle,
            AuthenticationRegion = options.Region,
            Timeout = timeout,
        };
        if (options.MaxErrorRetry is { } retries)
            config.MaxErrorRetry = retries;
        // Доверие CA https S3-endpoint (spec §5 fallback): AWSSDK.Core env
        // AWS_CA_BUNDLE не читает (доказано E2E) — per-install CA доверяется
        // фабрикой HttpClient. Путь не задан (http-endpoint) — прежнее поведение.
        // Нечитаемый/пустой файл CA — исключение с диагностикой (BackupCaPem).
        if (!string.IsNullOrWhiteSpace(options.CaBundlePath))
            config.HttpClientFactory = new S3CaHttpClientFactory(
                Shared.Tls.TlsMaterial.LoadPem(BackupCaPem.ReadIfConfigured(options.CaBundlePath)!));
        _client = new AmazonS3Client(
            new BasicAWSCredentials(options.AccessKey, options.SecretKey), config);
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

    public async Task<Result> PutObjectMd5Async(
        string key, byte[] data, string md5Hex, CancellationToken ct = default)
    {
        try
        {
            var request = new PutObjectRequest
            {
                BucketName = _bucket,
                Key = key,
                InputStream = new MemoryStream(data),
                // Content-MD5: base64 БАЙТОВ хэша (в имени куска — hex).
                MD5Digest = Convert.ToBase64String(Convert.FromHexString(md5Hex)),
            };
            await _client.PutObjectAsync(request, ct);
            return Result.Success();
        }
        catch (Exception e)
        {
            return Result.Failed(new ApplicationException($"S3 put {key}: {e.Message}", e));
        }
    }

    public async Task<Result<(byte[] Data, string? ETag)>> GetObjectBytesAsync(
        string cluster, string shard, string key, CancellationToken ct = default)
    {
        try
        {
            var response = await _client.GetObjectAsync(
                new GetObjectRequest { BucketName = _bucket, Key = $"{cluster}/{shard}/{key}" }, ct);
            using var buffer = new MemoryStream();
            await response.ResponseStream.CopyToAsync(buffer, ct);
            return Result<(byte[] Data, string? ETag)>.Success(
                (buffer.ToArray(), response.ETag?.Trim('"')));
        }
        catch (AmazonS3Exception e) when (e.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            // 404 — «объекта нет» ОТЛИЧИМ от транспортного сбоя (суффикс ": not
            // found", симметрия GetObjectAsync/GetObjectToFileAsync): fetch-wal по
            // нему решает «куска на этой ноде нет», а не ретраит вызов.
            return Result<(byte[] Data, string? ETag)>.Failed(
                new ApplicationException($"S3 get {cluster}/{shard}/{key}: not found"));
        }
        catch (Exception e)
        {
            return Result<(byte[] Data, string? ETag)>.Failed(
                new ApplicationException($"S3 get {key}: {e.Message}", e));
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

    /// <summary>Общий кор листинга (ListPrefixAsync / ListAsync): постраничный
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

    /// <summary>WAL-сегмент с гвардом чужой цепочки (arch/19 §3): HEAD
    /// существующего объекта, сверка ETag (одиночный PUT ownS3/S3 — md5-hex)
    /// с md5 содержимого. Совпал — байт-в-байт уже в архиве, put НЕ
    /// повторяется (идемпотентность двух приёмников одного потока); не совпал
    /// (или ETag не md5 — доказать идентичность нельзя) — конфликт: объект
    /// НЕ перезаписывается, наружу — маркер WalArchiveConflict. Отсутствует —
    /// обычный put (в т.ч. перебор целей пула).</summary>
    public async Task<Result> PutWalSegmentAsync(
        string key, byte[] data, string? sha256, CancellationToken ct = default)
    {
        GetObjectMetadataResponse? existing;
        try
        {
            existing = await _client.GetObjectMetadataAsync(
                new GetObjectMetadataRequest { BucketName = _bucket, Key = key }, ct);
        }
        catch (AmazonS3Exception e) when (e.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            existing = null; // отсутствует — заливаем
        }
        catch (Exception e)
        {
            return Result.Failed(new ApplicationException($"S3 head {key}: {e.Message}", e));
        }

        if (existing is not null)
        {
            var etag = existing.ETag?.Trim('"');
            var md5 = Convert.ToHexString(MD5.HashData(data)).ToLowerInvariant();
            if (string.Equals(etag, md5, StringComparison.OrdinalIgnoreCase))
                return Result.Success(); // идемпотентный повтор: те же байты
            return Result.Failed(new ApplicationException(
                $"{WalArchiveConflict.Marker}: {key} already archived with different content " +
                $"(etag={etag ?? "?"}, md5={md5}) — refusing to overwrite a foreign WAL chain"));
        }

        return await PutObjectAsync(key, data, sha256, ct);
    }

    /// <summary>Файловая форма гварда (дубль pg_wal полного): HEAD + md5 файла
    /// (потоково) против ETag; идентичность — идемпотентный успех, иное —
    /// WalArchiveConflict, отсутствие — обычный put-file. Обычный путь
    /// перезаписи файла (без гварда) — PutFileAsync.</summary>
    public async Task<Result> PutWalSegmentFileAsync(
        string key, string filePath, CancellationToken ct = default)
    {
        GetObjectMetadataResponse? existing;
        try
        {
            existing = await _client.GetObjectMetadataAsync(
                new GetObjectMetadataRequest { BucketName = _bucket, Key = key }, ct);
        }
        catch (AmazonS3Exception e) when (e.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            existing = null;
        }
        catch (Exception e)
        {
            return Result.Failed(new ApplicationException($"S3 head {key}: {e.Message}", e));
        }

        if (existing is not null)
        {
            string md5;
            try
            {
                await using var input = new FileStream(
                    filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
                md5 = Convert.ToHexString(MD5.HashData(input)).ToLowerInvariant();
            }
            catch (Exception e)
            {
                return Result.Failed(new ApplicationException(
                    $"wal segment file read {filePath}: {e.Message}", e));
            }
            var etag = existing.ETag?.Trim('"');
            if (string.Equals(etag, md5, StringComparison.OrdinalIgnoreCase))
                return Result.Success();
            return Result.Failed(new ApplicationException(
                $"{WalArchiveConflict.Marker}: {key} already archived with different content " +
                $"(etag={etag ?? "?"}, md5={md5}) — refusing to overwrite a foreign WAL chain"));
        }

        return await PutFileAsync(key, filePath, ct);
    }

    public async Task<Result> PutObjectAsync(string key, byte[] data, string? sha256, CancellationToken ct = default)
    {
        try
        {
            var request = new PutObjectRequest
            {
                BucketName = _bucket,
                Key = key,
                InputStream = new MemoryStream(data),
            };
            // Канон sha256 в мете/статусе — hex (spec §3.2); транспорт AWSSDK
            // ChecksumSHA256 ждёт BASE64-дайджест — конвертация на границе.
            if (!string.IsNullOrEmpty(sha256))
                request.ChecksumSHA256 = Convert.ToBase64String(Convert.FromHexString(sha256));
            await _client.PutObjectAsync(request, ct);
            return Result.Success();
        }
        catch (Exception e)
        {
            return Result.Failed(new ApplicationException($"S3 put {key}: {e.Message}", e));
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

    public async Task<Result> PutFileAsync(string key, string filePath, CancellationToken ct = default)
    {
        try
        {
            await using var input = new FileStream(
                filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
            await _client.PutObjectAsync(new PutObjectRequest
            {
                BucketName = _bucket,
                Key = key,
                InputStream = input,
            }, ct);
            return Result.Success();
        }
        catch (Exception e)
        {
            return Result.Failed(new ApplicationException($"S3 put-file {key}: {e.Message}", e));
        }
    }

    public async Task<Result> GetObjectToFileAsync(
        string cluster, string shard, string key, string destFilePath, CancellationToken ct = default)
    {
        try
        {
            var response = await _client.GetObjectAsync(new GetObjectRequest
            {
                BucketName = _bucket,
                Key = $"{cluster}/{shard}/{key}",
            }, ct);
            await WriteDownloadToFileAsync(response.ResponseStream, destFilePath, ct);
            return Result.Success();
        }
        // 404 — «объекта нет» ОТЛИЧИМ от транспортного сбоя (суффикс ": not found",
        // симметрия GetObjectAsync): fetch-wal по нему решает «конец WAL» сразу,
        // транспортный сбой — ретраит (иначе флап сети читается как конец WAL).
        catch (AmazonS3Exception e) when (e.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return Result.Failed(new ApplicationException(
                $"S3 get-to-file {cluster}/{shard}/{key}: not found"));
        }
        catch (Exception e)
        {
            return Result.Failed(new ApplicationException(
                $"S3 get-to-file {cluster}/{shard}/{key}: {e.Message}", e));
        }
    }

    /// <summary>Скачивание в файл ЧЕРЕЗ временный &lt;dest&gt;.part с атомарной
    /// заменой по успеху: обрыв тела ответа (транзиентный сетевой сбой) не
    /// оставляет частичный файл по dest — получатель (fetch-wal: ярлык «файл
    /// уже лежит»; restore: CRC) иначе принял бы обрезанный файл за валидный.
    /// Неуспех гасит .part в catch — по dest либо полный файл, либо ничего.</summary>
    internal static async Task WriteDownloadToFileAsync(
        Stream source, string destFilePath, CancellationToken ct)
    {
        var partPath = destFilePath + ".part";
        try
        {
            await using (var output = File.Create(partPath))
            {
                await source.CopyToAsync(output, ct);
            }
            File.Move(partPath, destFilePath, overwrite: true);
        }
        catch
        {
            try { File.Delete(partPath); } catch { /* уже нет */ }
            throw;
        }
    }

    // ── Multipart-заливка ОДНОГО объекта (spec t01 §3.3): части 16 МиБ с
    // Content-MD5 (транспорт отвергает битое), Complete по partNumber→ETag.
    // Сессия живёт на ОДНОМ endpoint (перебор нод — забота операции: отказ →
    // Abort → рестарт попытки на следующей ноде).

    public async Task<Result<IMultipartUploadSession>> CreateMultipartUploadAsync(
        string key, CancellationToken ct)
    {
        try
        {
            var response = await _client.InitiateMultipartUploadAsync(
                new InitiateMultipartUploadRequest { BucketName = _bucket, Key = key }, ct);
            if (response.UploadId is not { Length: > 0 } uploadId)
                return Result<IMultipartUploadSession>.Failed(
                    new ApplicationException($"S3 multipart {key}: empty upload id"));
            return Result<IMultipartUploadSession>.Success(new MultipartSession(this, key, uploadId));
        }
        catch (Exception e)
        {
            return Result<IMultipartUploadSession>.Failed(
                new ApplicationException($"S3 multipart {key}: {e.Message}", e));
        }
    }

    public async Task<Result<IReadOnlyList<MultipartUploadInfo>>> ListMultipartUploadsAsync(
        string prefix, CancellationToken ct)
    {
        try
        {
            var uploads = new List<MultipartUploadInfo>();
            string? keyMarker = null;
            string? uploadIdMarker = null;
            do
            {
                var page = await _client.ListMultipartUploadsAsync(new ListMultipartUploadsRequest
                {
                    BucketName = _bucket,
                    Prefix = prefix,
                    KeyMarker = keyMarker,
                    UploadIdMarker = uploadIdMarker,
                }, ct);
                foreach (var upload in page.MultipartUploads)
                {
                    uploads.Add(new MultipartUploadInfo(
                        upload.Key, upload.UploadId,
                        new DateTimeOffset(DateTime.SpecifyKind(upload.Initiated, DateTimeKind.Utc))));
                }

                if (page.IsTruncated is true)
                {
                    keyMarker = page.NextKeyMarker;
                    uploadIdMarker = page.NextUploadIdMarker;
                }
                else
                {
                    keyMarker = null;
                }
            }
            while (keyMarker is not null);

            return Result<IReadOnlyList<MultipartUploadInfo>>.Success(uploads);
        }
        catch (Exception e)
        {
            return Result<IReadOnlyList<MultipartUploadInfo>>.Failed(
                new ApplicationException($"S3 list-multipart {prefix}: {e.Message}", e));
        }
    }

    public async Task<Result> AbortMultipartUploadAsync(
        string key, string uploadId, CancellationToken ct)
    {
        try
        {
            await _client.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
            {
                BucketName = _bucket,
                Key = key,
                UploadId = uploadId,
            }, ct);
            return Result.Success();
        }
        catch (Exception e)
        {
            return Result.Failed(
                new ApplicationException($"S3 abort-multipart {key} ({uploadId}): {e.Message}", e));
        }
    }

    // ── Потоковое чтение (restore/verify/дубль WAL) и одиночное удаление ──

    public async Task<Result<(Stream Body, long Size)>> GetObjectStreamAsync(
        string cluster, string shard, string key, CancellationToken ct)
    {
        try
        {
            var response = await _client.GetObjectAsync(
                new GetObjectRequest { BucketName = _bucket, Key = $"{cluster}/{shard}/{key}" }, ct);
            // Стрим диспозит вызывающий (он владеет жизненным циклом чтения).
            return Result<(Stream Body, long Size)>.Success(
                (response.ResponseStream, response.ContentLength));
        }
        catch (AmazonS3Exception e) when (e.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return Result<(Stream Body, long Size)>.Failed(new ApplicationException(
                $"S3 get-stream {cluster}/{shard}/{key}: not found"));
        }
        catch (Exception e)
        {
            return Result<(Stream Body, long Size)>.Failed(
                new ApplicationException($"S3 get-stream {cluster}/{shard}/{key}: {e.Message}", e));
        }
    }

    public async Task<Result<byte[]>> GetObjectRangeAsync(
        string cluster, string shard, string key, long start, long end, CancellationToken ct)
    {
        try
        {
            var response = await _client.GetObjectAsync(new GetObjectRequest
            {
                BucketName = _bucket,
                Key = $"{cluster}/{shard}/{key}",
                ByteRange = new ByteRange(start, end),
            }, ct);
            using var buffer = new MemoryStream();
            await response.ResponseStream.CopyToAsync(buffer, ct);
            return Result<byte[]>.Success(buffer.ToArray());
        }
        catch (AmazonS3Exception e) when (e.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return Result<byte[]>.Failed(new ApplicationException(
                $"S3 get-range {cluster}/{shard}/{key}: not found"));
        }
        catch (Exception e)
        {
            return Result<byte[]>.Failed(
                new ApplicationException($"S3 get-range {cluster}/{shard}/{key}: {e.Message}", e));
        }
    }

    public async Task<Result<(bool Exists, long SizeBytes)>> HeadObjectAsync(
        string cluster, string shard, string key, CancellationToken ct)
    {
        try
        {
            var response = await _client.GetObjectMetadataAsync(new GetObjectMetadataRequest
            {
                BucketName = _bucket,
                Key = $"{cluster}/{shard}/{key}",
            }, ct);
            // Размер — тем же HEAD'ом (без суффиксного Range/Content-Range,
            // spec §6.23): verify нужен полный размер до чтения хвоста.
            return Result<(bool Exists, long SizeBytes)>.Success((true, response.ContentLength));
        }
        catch (AmazonS3Exception e) when (e.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return Result<(bool Exists, long SizeBytes)>.Success((false, 0L));
        }
        catch (Exception e)
        {
            return Result<(bool Exists, long SizeBytes)>.Failed(
                new ApplicationException($"S3 head {cluster}/{shard}/{key}: {e.Message}", e));
        }
    }

    public async Task<Result> DeleteObjectAsync(
        string cluster, string shard, string key, CancellationToken ct)
    {
        try
        {
            await _client.DeleteObjectAsync(new DeleteObjectRequest
            {
                BucketName = _bucket,
                Key = $"{cluster}/{shard}/{key}",
            }, ct);
            return Result.Success();
        }
        catch (Exception e)
        {
            return Result.Failed(
                new ApplicationException($"S3 delete {cluster}/{shard}/{key}: {e.Message}", e));
        }
    }

    /// <summary>Сессия multipart-заливки на ОДНОМ endpoint (ключ и BucketName —
    /// этого клиента). Complete сверяет контигуальность partNumber 1..N
    /// (сервер дополнительно валидирует ETag частей сам).</summary>
    private sealed class MultipartSession(BackupS3 owner, string key, string uploadId)
        : IMultipartUploadSession
    {
        public string UploadId => uploadId;

        public async Task<Result<string>> UploadPartAsync(
            int partNumber, byte[] data, CancellationToken ct)
        {
            try
            {
                var response = await owner._client.UploadPartAsync(new UploadPartRequest
                {
                    BucketName = owner._bucket,
                    Key = key,
                    UploadId = uploadId,
                    PartNumber = partNumber,
                    InputStream = new MemoryStream(data),
                    // Content-MD5: base64 БАЙТОВ хэша (транспорт отвергает битое).
                    MD5Digest = Convert.ToBase64String(MD5.HashData(data)),
                }, ct);
                return Result<string>.Success(response.ETag?.Trim('"') ?? "");
            }
            catch (Exception e)
            {
                return Result<string>.Failed(new ApplicationException(
                    $"S3 upload-part {key} #{partNumber}: {e.Message}", e));
            }
        }

        public async Task<Result<string>> CompleteAsync(
            IReadOnlyDictionary<int, string> partETags, CancellationToken ct)
        {
            if (partETags.Count == 0)
                return Result<string>.Failed(new ApplicationException(
                    $"S3 complete-multipart {key}: no parts uploaded"));
            var ordered = partETags.Keys.OrderBy(number => number).ToList();
            for (var i = 0; i < ordered.Count; i++)
            {
                if (ordered[i] != i + 1)
                    return Result<string>.Failed(new ApplicationException(
                        $"S3 complete-multipart {key}: part numbers are not contiguous from 1"));
            }

            try
            {
                var response = await owner._client.CompleteMultipartUploadAsync(
                    new CompleteMultipartUploadRequest
                    {
                        BucketName = owner._bucket,
                        Key = key,
                        UploadId = uploadId,
                        PartETags = partETags
                            .Select(pair => new PartETag(pair.Key, pair.Value))
                            .ToList(),
                    }, ct);
                return Result<string>.Success(response.ETag?.Trim('"') ?? "");
            }
            catch (Exception e)
            {
                return Result<string>.Failed(new ApplicationException(
                    $"S3 complete-multipart {key}: {e.Message}", e));
            }
        }

        public async Task<Result> AbortAsync(CancellationToken ct)
            => await owner.AbortMultipartUploadAsync(key, uploadId, ct);
    }

    public ValueTask DisposeAsync()
    {
        _client.Dispose();
        return ValueTask.CompletedTask;
    }
}
