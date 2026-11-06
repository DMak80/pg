using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;

namespace EtcdNode.Core;

/// <summary>S3-операции над бакетом etcd-слепков (export/retention): put с
/// SHA256-проверкой целостности транспорта, list по префиксу с пагинацией и
/// batch-delete. Осознанное дублирование механики PgWorker.Backups/BackupS3
/// без зависимости от PgWorker.Core (S3ClientOptions недоступен).</summary>
public interface IEtcdSnapshotS3 : IAsyncDisposable
{
    /// <summary>put байтов с SHA256-проверкой целостности транспорта:
    /// ChecksumSHA256 уходит с запросом — сервер сверяет содержимое и отвергает
    /// put при несовпадении (BadDigest); успех = байты доставлены целыми.
    /// sha256 (канон — hex) = null — без проверки (мелкие служебные объекты).</summary>
    Task<Result> PutObjectAsync(string key, byte[] data, string? sha256, CancellationToken ct);

    /// <summary>list-objects-v2 с пагинацией по префиксу (IsTruncated/
    /// NextContinuationToken до исчерпания): все объекты префикса — ключ +
    /// размер + время модификации (EtcdS3Object).</summary>
    Task<Result<IReadOnlyList<EtcdS3Object>>> ListPrefixAsync(string prefix, CancellationToken ct);

    /// <summary>batch-delete (DeleteObjects, чанки ≤1000); идемпотентно —
    /// отсутствие ключа в ответе не ошибка (повтор прохода безопасен).</summary>
    Task<Result> DeleteKeysAsync(IReadOnlyList<string> keys, CancellationToken ct);
}

/// <summary>Минимальный S3-клиент бакета слепков: PathStyle (ownS3 одним
/// клиентом), перезапросный таймаут, доверие per-install CA через фабрику
/// HttpClient (env AWS_CA_BUNDLE AWSSDK не читает). Конструктор — механика
/// BackupS3 напрямую из аргументов (без S3ClientOptions).</summary>
public sealed class EtcdSnapshotS3 : IEtcdSnapshotS3, IAsyncDisposable
{
    private readonly AmazonS3Client _client;
    private readonly string _bucket;

    // Пер-попыточный таймаут S3-запроса — механика BackupS3 (пульс конечного
    // внешнего вызова): дефолт 10 с, переданный меньше секунды — как не заданный.
    private static readonly TimeSpan DefaultRequestTimeout = TimeSpan.FromSeconds(10);

    public EtcdSnapshotS3(string endpoint, string bucket, string accessKey, string secretKey,
        string? caBundlePath = null, TimeSpan? requestTimeout = null)
    {
        _bucket = bucket;
        var timeout = requestTimeout is { } t && t >= TimeSpan.FromSeconds(1) ? t : DefaultRequestTimeout;
        var config = new AmazonS3Config
        {
            ServiceURL = endpoint,
            ForcePathStyle = true,
            Timeout = timeout,
        };
        // Доверие CA https S3-endpoint: AWSSDK env AWS_CA_BUNDLE не читает —
        // per-install CA доверяется фабрикой HttpClient; путь не задан
        // (http-endpoint) — прежнее поведение; нечитаемый/пустой файл CA —
        // исключение с диагностикой (BackupCaPem).
        if (!string.IsNullOrWhiteSpace(caBundlePath))
            config.HttpClientFactory = new S3CaHttpClientFactory(
                Shared.Tls.TlsMaterial.LoadPem(BackupCaPem.ReadIfConfigured(caBundlePath)!));
        _client = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey), config);
    }

    public async Task<Result> PutObjectAsync(string key, byte[] data, string? sha256, CancellationToken ct)
    {
        try
        {
            var request = new PutObjectRequest
            {
                BucketName = _bucket,
                Key = key,
                InputStream = new MemoryStream(data),
            };
            // Канон sha256 в мете/статусе — hex; транспорт AWSSDK ChecksumSHA256
            // ждёт BASE64-дайджест — конвертация на границе.
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

    public async Task<Result<IReadOnlyList<EtcdS3Object>>> ListPrefixAsync(string prefix, CancellationToken ct)
    {
        var listed = await ListPrefixCoreAsync(prefix, ct);
        if (!listed.IsSuccess)
            return Result<IReadOnlyList<EtcdS3Object>>.Failed(listed.Error!);
        return Result<IReadOnlyList<EtcdS3Object>>.Success(listed.Value
            .Select(o => new EtcdS3Object(o.Key, o.Size, o.LastModified))
            .ToList());
    }

    /// <summary>Общий кор листинга: постраничный list-objects-v2 по абсолютному
    /// префиксу bucket'а — сырые S3-объекты, маппинг в контракт — снаружи.</summary>
    private async Task<Result<IReadOnlyList<S3Object>>> ListPrefixCoreAsync(string prefix, CancellationToken ct)
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

    public async Task<Result> DeleteKeysAsync(IReadOnlyList<string> keys, CancellationToken ct)
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

    public ValueTask DisposeAsync()
    {
        _client.Dispose();
        return ValueTask.CompletedTask;
    }
}
