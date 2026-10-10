using System.Security.Cryptography;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace OwnS3.Storage;

// Multipart-методы XlObjectStore (канон 02 §5): Create/Abort реализованы;
// UploadPart/UploadPartCopy/Complete/ListParts/ListMultipartUploads — до
// своих задач; журналы и раскладка — канон 04 §5 (MultipartJournals).
public sealed partial class XlObjectStore
{
    // Лимиты канона 02 §1: минимальная часть (кроме последней) 5 МиБ;
    // максимальный размер части/копии 5 ГБ (совпадает с MaxCopySize PUT).
    internal const long MinPartSize = 5L * 1024 * 1024;
    internal const long MaxPartSize = 5L * 1024 * 1024 * 1024;

    // CreateMultipartUpload (канон 02 §5/04 §5): каталог загрузки + xl.meta
    // загрузки ДО записи в журнале (каталог без записи — невидим); запись
    // журнала — под _commitLock.
    public Task<string> CreateMultipartUploadAsync(string bucket, string key,
        ObjectUploadMetadata metadata, string initiatorAccessKey, CancellationToken ct)
    {
        EnsureBucket(bucket);
        var uploadId = Guid.NewGuid().ToString("N"); // формат N (канон 04 §5)
        var keyDir = MultipartJournals.KeyDir(volume.MultipartDir, bucket, key);
        var uploadDir = MultipartJournals.UploadDirPath(keyDir, uploadId);
        Directory.CreateDirectory(uploadDir);
        var now = timeProvider.GetUtcNow();
        var record = new XlMetaRecord(Guid.Parse(uploadId), 0, now, "",
            metadata.ContentType, metadata.UserMetadata, EmptyHeaders, ""); // М4
        XlMetaFile.Write(uploadDir, record);
        lock (_commitLock)
        {
            var uploads = MultipartJournals.ReadUploads(MultipartJournals.UploadsJsonPath(keyDir));
            uploads.Add(new MultipartJournals.UploadJournalEntry(uploadId, bucket, key,
                now.ToUnixTimeMilliseconds(), initiatorAccessKey));
            MultipartJournals.WriteUploads(MultipartJournals.UploadsJsonPath(keyDir), uploads);
        }
        return Task.FromResult(uploadId);
    }

    // Abort (канон 02 §5/04 §5): под _commitLock — разрешение, удаление записи,
    // каталог в .trash. Повторный Abort после успешного — NoSuchUpload.
    public Task AbortMultipartUploadAsync(string bucket, string key, string uploadId, CancellationToken ct)
    {
        EnsureBucket(bucket);
        var keyDir = MultipartJournals.KeyDir(volume.MultipartDir, bucket, key);
        lock (_commitLock)
        {
            var uploads = ReadUploadsOrThrow(keyDir);
            var entry = uploads.FirstOrDefault(e =>
                e.UploadId == uploadId && e.Bucket == bucket && e.Key == key)
                ?? throw new ObjectStoreException(ObjectStoreErrorCode.NoSuchUpload);
            if (!Directory.Exists(MultipartJournals.UploadDirPath(keyDir, uploadId)))
                throw new ObjectStoreException(ObjectStoreErrorCode.NoSuchUpload); // М2: призрачная запись
            uploads.Remove(entry);
            MultipartJournals.WriteUploads(MultipartJournals.UploadsJsonPath(keyDir), uploads);
            volume.MoveToTrash(MultipartJournals.UploadDirPath(keyDir, uploadId));
        }
        return Task.CompletedTask;
    }

    // Журнал ключа: битый JSON — warning + NoSuchUpload (М2); файла нет — пусто.
    internal List<MultipartJournals.UploadJournalEntry> ReadUploadsOrThrow(string keyDir)
    {
        try
        {
            return MultipartJournals.ReadUploads(MultipartJournals.UploadsJsonPath(keyDir));
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(ex, "Битый uploads.json в {KeyDir}: загрузки ключа недоступны", keyDir);
            throw new ObjectStoreException(ObjectStoreErrorCode.NoSuchUpload);
        }
    }

    // Разрешение загрузки: запись ∧ каталог (М2); чужая для OwnedBy — NoSuchUpload (М9).
    internal MultipartJournals.UploadJournalEntry ResolveUploadOrThrow(
        string bucket, string key, string uploadId, UploadVisibility? visibility)
    {
        var keyDir = MultipartJournals.KeyDir(volume.MultipartDir, bucket, key);
        var uploads = ReadUploadsOrThrow(keyDir);
        var entry = uploads.FirstOrDefault(e =>
            e.UploadId == uploadId && e.Bucket == bucket && e.Key == key)
            ?? throw new ObjectStoreException(ObjectStoreErrorCode.NoSuchUpload);
        if (!Directory.Exists(MultipartJournals.UploadDirPath(keyDir, uploadId)))
            throw new ObjectStoreException(ObjectStoreErrorCode.NoSuchUpload);
        if (visibility is UploadVisibility.Owned(var owner) && entry.AccessKey != owner)
            throw new ObjectStoreException(ObjectStoreErrorCode.NoSuchUpload);
        return entry;
    }

    // UploadPart (канон 02 §5/04 §5): тело → part.N.tmp БЕЗ лока (fsync), затем под
    // _commitLock — согласованная пара rename части + upsert parts.json (М3).
    public async Task<PutResult> UploadPartAsync(string bucket, string key, string uploadId, int partNumber,
        Stream body, long contentLength, CancellationToken ct)
    {
        EnsureBucket(bucket);
        ResolveUploadOrThrow(bucket, key, uploadId, visibility: null);
        var keyDir = MultipartJournals.KeyDir(volume.MultipartDir, bucket, key);
        var uploadDir = MultipartJournals.UploadDirPath(keyDir, uploadId);
        var tmpPath = Path.Combine(uploadDir, MultipartJournals.PartTmpFileName(partNumber));
        // Тело: MD5-инкремент + счётчик + fsync (длина ≠ contentLength → XlIntegrityException,
        // остаточный случай — конвейер уже проверил тело)
        string etagHex;
        long total;
        using (var md5 = IncrementalHash.CreateHash(HashAlgorithmName.MD5))
        using (var file = new FileStream(tmpPath, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            var buffer = new byte[64 * 1024];
            total = 0;
            int read;
            while ((read = await body.ReadAsync(buffer, ct)) > 0)
            {
                file.Write(buffer, 0, read);
                md5.AppendData(buffer, 0, read);
                total += read;
            }
            file.Flush(flushToDisk: true);
            etagHex = Convert.ToHexString(md5.GetHashAndReset()).ToLowerInvariant();
        }
        if (total != contentLength)
            throw new XlIntegrityException(
                $"Фактическая длина части {total} не совпадает с заявленной {contentLength}");
        var modTime = timeProvider.GetUtcNow();
        await Task.Yield(); // async-контракт при синхронном файловом IO (P4 t37)
        lock (_commitLock)
        {
            CommitPartFile(uploadDir, partNumber, tmpPath, etagHex, total, modTime);
        }
        return new PutResult('"' + etagHex + '"', modTime);
    }

    // Коммит части (UploadPart/UploadPartCopy): rename + upsert parts.json —
    // согласованная пара под _commitLock вызывающего (М3).
    private void CommitPartFile(string uploadDir, int partNumber, string tmpPath,
        string etagHex, long size, DateTimeOffset modTime)
    {
        File.Move(tmpPath, Path.Combine(uploadDir, MultipartJournals.PartFileName(partNumber)),
            overwrite: true);
        var parts = ReadPartsOrThrow(uploadDir);
        parts.RemoveAll(p => p.PartNumber == partNumber);
        parts.Add(new MultipartJournals.PartJournalEntry(partNumber, etagHex, size,
            modTime.ToUnixTimeMilliseconds()));
        parts.Sort((left, right) => left.PartNumber.CompareTo(right.PartNumber));
        MultipartJournals.WriteParts(MultipartJournals.PartsJsonPath(uploadDir), parts);
    }

    // Журнал частей загрузки: битый parts.json (JsonException) — warning + загрузка
    // недоступна (NoSuchUpload) — единообразно с ListParts/Complete (спека §4.1:
    // потеря целостности журнала — загрузка недоступна). Файла нет — пустой список.
    internal List<MultipartJournals.PartJournalEntry> ReadPartsOrThrow(string uploadDir)
    {
        try
        {
            return MultipartJournals.ReadParts(MultipartJournals.PartsJsonPath(uploadDir));
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(ex, "Битый parts.json загрузки в {UploadDir}: загрузка недоступна", uploadDir);
            throw new ObjectStoreException(ObjectStoreErrorCode.NoSuchUpload);
        }
    }

    public Task<PutResult> UploadPartCopyAsync(PartCopyRequest request, CancellationToken ct) =>
        ThrowUnavailable<PutResult>(); // t38, Task 10

    public Task<CompleteResult> CompleteMultipartUploadAsync(string bucket, string key, string uploadId,
        IReadOnlyList<PartEtag> parts, CancellationToken ct) =>
        ThrowUnavailable<CompleteResult>(); // t38, Task 6–7

    public Task<PartsPage> ListPartsAsync(string bucket, string key, string uploadId,
        int? maxParts, int? partNumberMarker, CancellationToken ct) =>
        ThrowUnavailable<PartsPage>(); // t38, Task 5

    public Task<UploadsPage> ListMultipartUploadsAsync(string bucket, UploadsQuery query, CancellationToken ct) =>
        ThrowUnavailable<UploadsPage>(); // t38, Task 5

    private static Task<T> ThrowUnavailable<T>() => throw new ObjectStoreUnavailableException();
}
