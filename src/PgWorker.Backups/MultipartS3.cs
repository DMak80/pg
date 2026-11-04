namespace PgWorker.Backups;

/// <summary>Сессия multipart-заливки ОДНОГО объекта (spec t01 §3.3): endpoint
/// ФИКСИРОВАН на сессию (обёртка не перебирает ноды — перебор делает операция
/// рестартом попытки), части 16 МиБ с Content-MD5, Complete со списком
/// partNumber→ETag.</summary>
public interface IMultipartUploadSession
{
    string UploadId { get; }

    /// <summary>Заливка части; Result — ETag части (кавычки S3 сняты).</summary>
    Task<Result<string>> UploadPartAsync(int partNumber, byte[] data, CancellationToken ct);

    /// <summary>Завершение заливки; Result — ETag объекта.</summary>
    Task<Result<string>> CompleteAsync(
        IReadOnlyDictionary<int, string> partETags, CancellationToken ct);

    /// <summary>Отмена сессии (лучшее усилие): части и uploadId исчезают.</summary>
    Task<Result> AbortAsync(CancellationToken ct);
}

/// <summary>Висячая multipart-сессия (гигиена супервизора): ключ, uploadId и
/// время старта (возраст = критерий Abort).</summary>
public sealed record MultipartUploadInfo(string Key, string UploadId, DateTimeOffset Initiated);

/// <summary>Multipart-заливка одного объекта (spec t01 §3.3): создание
/// сессии по ПОЛНОМУ ключу bucket'а (`&lt;C&gt;/&lt;X&gt;/full/&lt;id&gt;.tar` —
/// симметрия PutFileAsync), листинг/отмена висячих сессий по префиксу.</summary>
public interface IMultipartS3
{
    Task<Result<IMultipartUploadSession>> CreateMultipartUploadAsync(
        string key, CancellationToken ct);

    Task<Result<IReadOnlyList<MultipartUploadInfo>>> ListMultipartUploadsAsync(
        string prefix, CancellationToken ct);

    Task<Result> AbortMultipartUploadAsync(string key, string uploadId, CancellationToken ct);
}

/// <summary>Потоковое чтение объектов (restore/verify/дубль WAL): стрим тела,
/// Range-чтение диапазона и HEAD (существование + размер). Ключи —
/// относительные (`full/&lt;id&gt;.tar`), как у прочих GET'ов IBackupS3.</summary>
public interface IStreamObjectS3
{
    /// <summary>Тело объекта стримом + размер; стрим диспозит вызывающий.</summary>
    Task<Result<(Stream Body, long Size)>> GetObjectStreamAsync(
        string cluster, string shard, string key, CancellationToken ct);

    /// <summary>Тело диапазона [start, end]; 404 → Failed(": not found").
    /// Полный размер объекта Range не даёт — его берут из HeadObjectAsync
    /// (spec §6.23).</summary>
    Task<Result<byte[]>> GetObjectRangeAsync(
        string cluster, string shard, string key, long start, long end, CancellationToken ct);

    /// <summary>Существование + размер объекта (ContentLength HEAD'а);
    /// NotFound → Success((false, 0)) — не ошибка.</summary>
    Task<Result<(bool Exists, long SizeBytes)>> HeadObjectAsync(
        string cluster, string shard, string key, CancellationToken ct);
}

/// <summary>Одиночное удаление объекта (ретенция/супервизор: полный — ОДИН
/// ключ `full/&lt;id&gt;.tar`).</summary>
public interface IDeleteObjectS3
{
    Task<Result> DeleteObjectAsync(
        string cluster, string shard, string key, CancellationToken ct);
}
