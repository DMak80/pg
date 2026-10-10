using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace OwnS3.Storage;

// Бакетные операции XlObjectStore (спека §4.6).
public sealed partial class XlObjectStore
{
    // Существует → BucketAlreadyOwnedByYou; иначе каталог + bucket.json
    // (каталог первым — видимость листинга = наличие каталога).
    public Task CreateBucketAsync(string bucket, CancellationToken ct)
    {
        if (BucketExists(bucket))
            throw new ObjectStoreException(ObjectStoreErrorCode.BucketAlreadyOwnedByYou);
        Directory.CreateDirectory(BucketRoot(bucket));
        _bucketMeta.CreateBucketMeta(bucket);
        return Task.CompletedTask;
    }

    // Нет → NoSuchBucket; в поддереве есть xl.meta ИЛИ живые multipart-загрузки →
    // BucketNotEmpty; иначе каталог бакета и служебная мета — в .trash по отдельности.
    public Task DeleteBucketAsync(string bucket, CancellationToken ct)
    {
        EnsureBucket(bucket);
        if (Directory.EnumerateFiles(BucketRoot(bucket), "xl.meta", SearchOption.AllDirectories).Any())
            throw new ObjectStoreException(ObjectStoreErrorCode.BucketNotEmpty);
        // Живые multipart-загрузки бакета блокируют удаление (канон 02 §1/§5):
        // активная запись = запись с существующим каталогом загрузки (М2)
        if (Directory.Exists(volume.MultipartDir))
            foreach (var keyDir in Directory.EnumerateDirectories(volume.MultipartDir))
            {
                List<MultipartJournals.UploadJournalEntry> uploads;
                try { uploads = MultipartJournals.ReadUploads(MultipartJournals.UploadsJsonPath(keyDir)); }
                catch (JsonException ex)
                {
                    _logger.LogWarning(ex, "Битый uploads.json в {KeyDir} при DeleteBucket: пропущен", keyDir);
                    continue;
                }
                if (uploads.Any(e => e.Bucket == bucket
                        && Directory.Exists(MultipartJournals.UploadDirPath(keyDir, e.UploadId))))
                    throw new ObjectStoreException(ObjectStoreErrorCode.BucketNotEmpty);
            }
        volume.MoveToTrash(BucketRoot(bucket));
        var metaDir = Path.Combine(volume.BucketsMetaDir, bucket);
        if (Directory.Exists(metaDir))
            volume.MoveToTrash(metaDir);
        return Task.CompletedTask;
    }

    // Каталог существует и не служебный.
    public Task<bool> BucketExistsAsync(string bucket, CancellationToken ct) =>
        Task.FromResult(BucketExists(bucket));

    // Каталоги первого уровня кроме .owns3.sys, по алфавиту (Utf8ByteOrder —
    // порядок EnumerateDirectories ФС не гарантирован); CreationDate из bucket.json
    // (файла нет → epoch).
    public Task<IReadOnlyList<BucketEntry>> ListBucketsAsync(CancellationToken ct)
    {
        var names = Directory.EnumerateDirectories(volume.Root)
            .Select(Path.GetFileName)
            .Where(name => name is not null && name != ".owns3.sys")
            .Select(name => name!)
            .ToList();
        names.Sort((left, right) => Utf8ByteOrder.Compare(left, right));
        var entries = names
            .Select(name => new BucketEntry(name, _bucketMeta.TryReadCreationDate(name)
                ?? DateTimeOffset.FromUnixTimeMilliseconds(0)))
            .ToList();
        return Task.FromResult<IReadOnlyList<BucketEntry>>(entries);
    }
}
