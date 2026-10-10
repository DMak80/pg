using System.Security.Cryptography;
using Microsoft.Extensions.Logging;

namespace OwnS3.Storage;

// Объектные операции XlObjectStore: Put/Delete/DeleteObjects — коммит-цикл
// канона 04 §4 (Get/Head — Task 7, Copy/Attributes — Task 8, листинги — Task 9).
public sealed partial class XlObjectStore
{
    private static readonly IReadOnlyDictionary<string, string> EmptyHeaders =
        new Dictionary<string, string>();

    // Сериализация коммит-циклов одного процесса: rename/Write-файлы одного
    // каталога объекта не конфликтуют между конкурентными PUT (per-object
    // словари локов не заводятся — спека §2.3; это один лок всего процесса).
    private readonly object _commitLock = new();

    public async Task<PutResult> PutObjectAsync(string bucket, string key, Stream body, long contentLength,
        ObjectUploadMetadata metadata, CancellationToken ct)
    {
        // 1. Бакет
        EnsureBucket(bucket);
        // 2. Staging + новый versionId (dataDir-имя = versionId, P1)
        var versionId = Guid.NewGuid();
        var staging = Path.Combine(volume.TmpDir, Guid.NewGuid().ToString("N"));
        var dataDir = Path.Combine(staging, versionId.ToString("N"));
        Directory.CreateDirectory(dataDir);
        try
        {
            // 3. Тело → part.1 (MD5+SHA-256 инкрементально, счётчик, fsync файла)
            var (etag, sha256) = await CopyBodyToFileAsync(body, Path.Combine(dataDir, "part.1"), contentLength);
            var record = new XlMetaRecord(versionId, contentLength, timeProvider.GetUtcNow(), etag,
                metadata.ContentType, metadata.UserMetadata, EmptyHeaders, sha256); // ETag hex без кавычек (P8)
            XlMetaFile.Write(staging, record); // fsync tmp-файла — внутри Write
            // 4–6. Коммит + возврат (ETag HTTP-значение — в кавычках, P8)
            return await CommitStagedObjectAsync(bucket, key, record, staging, ct);
        }
        finally
        {
            // Staging удаляется при любом отказе — не ждёт чисток (спека §4.3 п.4)
            if (Directory.Exists(staging))
                Directory.Delete(staging, recursive: true);
        }
    }

    // Коммит-схема канона 04 §4: новый ключ — один rename; перезапись —
    // rename dataDir → атомарная замена xl.meta (КОММИТ) → старый dataDir в .trash.
    internal async Task<PutResult> CommitStagedObjectAsync(string bucket, string key, XlMetaRecord record,
        string stagingDir, CancellationToken ct)
    {
        await Task.Yield(); // async-контракт при синхронном файловом IO (P4)
        var target = ObjectDir(bucket, key);
        var newDataDir = Path.Combine(stagingDir, record.DataDirName);
        lock (_commitLock)
        {
            if (!Directory.Exists(target))
            {
                try
                {
                    Directory.CreateDirectory(Path.GetDirectoryName(target)!);
                    Directory.Move(stagingDir, target); // единый rename = коммит
                }
                catch (IOException) when (Directory.Exists(target))
                {
                    // Гонка создания: другой поток закоммитил первым — схемой перезаписи
                    MoveIntoExistingTarget(target, record, newDataDir, oldDataDir: null);
                }
            }
            else
            {
                // Живой каталог-префикс или существующий объект (канон 04 §4 п.3–4)
                MoveIntoExistingTarget(target, record, newDataDir, oldDataDir: ReadCurrentDataDirOrNull(target));
            }
            // Diagnostic-warning: fsync каталога не выполняется — только BCL
            // (спека §4.3 п.2 best-effort, приказ пользователя)
            _logger.LogWarning("fsync каталога объекта не выполняется (только BCL, приказ пользователя): {Target}", target);
        }
        return new PutResult('"' + record.ETag + '"');
    }

    // Перезапись: rename нового dataDir рядом со старым → замена xl.meta →
    // старый dataDir в .trash строго ПОСЛЕ коммита.
    private void MoveIntoExistingTarget(string target, XlMetaRecord record, string newDataDir, string? oldDataDir)
    {
        Directory.Move(newDataDir, Path.Combine(target, record.DataDirName));
        XlMetaFile.Write(target, record); // bkp → tmp → rename; ЕДИНСТВЕННЫЙ коммит-поинт
        if (oldDataDir is not null)
        {
            var oldPath = Path.Combine(target, oldDataDir);
            if (Directory.Exists(oldPath))
                volume.MoveToTrash(oldPath);
        }
    }

    private static string? ReadCurrentDataDirOrNull(string target)
    {
        try
        {
            return XlMetaFile.Read(target, out _).DataDirName;
        }
        catch (FileNotFoundException)
        {
            return null; // нет записи — нет старого dataDir
        }
    }

    // Копирование тела в файл с инкрементальными MD5+SHA-256 и счётчиком;
    // фактическая длина ≠ заявленной → XlIntegrityException (спека §4.3 п.1);
    // fsync файла — Flush(flushToDisk: true) (чистый BCL).
    private static async Task<(string ETagHex, string Sha256Hex)> CopyBodyToFileAsync(
        Stream body, string filePath, long expectedLength)
    {
        using var md5 = IncrementalHash.CreateHash(HashAlgorithmName.MD5);
        using var sha256 = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        long total = 0;
        using (var file = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            var buffer = new byte[64 * 1024];
            int read;
            while ((read = await body.ReadAsync(buffer)) > 0)
            {
                file.Write(buffer, 0, read);
                md5.AppendData(buffer, 0, read);
                sha256.AppendData(buffer, 0, read);
                total += read;
            }
            file.Flush(flushToDisk: true);
        }
        if (total != expectedLength)
            throw new XlIntegrityException(
                $"Фактическая длина тела {total} не совпадает с заявленной {expectedLength}");
        return (Convert.ToHexString(md5.GetHashAndReset()).ToLowerInvariant(),
            Convert.ToHexString(sha256.GetHashAndReset()).ToLowerInvariant());
    }

    // Delete канона 04 §4 п.5: xl.meta и dataDir — в .trash ПО ОТДЕЛЬНОСТИ
    // (цели <guid>); опустевший каталог объекта удаляется, каталоги-префиксы
    // с подобъектами остаются. Идемпотентность: повторный Delete не находит
    // ни основной, ни страховочной записи — успех (гварды ниже покрывают
    // краш-остатки: чтение из bkp при удалённом основном).
    public Task DeleteObjectAsync(string bucket, string key, CancellationToken ct)
    {
        EnsureBucket(bucket);
        var target = ObjectDir(bucket, key);
        XlMetaRecord meta;
        try
        {
            meta = XlMetaFile.Read(target, out _);
        }
        catch (FileNotFoundException)
        {
            return Task.CompletedTask; // идемпотентность (канон 02 §1)
        }
        var metaPath = Path.Combine(target, "xl.meta");
        if (File.Exists(metaPath))
            volume.MoveToTrash(metaPath);
        var bkpPath = Path.Combine(target, "xl.meta.bkp");
        if (File.Exists(bkpPath))
            volume.MoveToTrash(bkpPath); // страховочная копия уходит вместе с записью
        var dataDir = Path.Combine(target, meta.DataDirName);
        if (Directory.Exists(dataDir))
            volume.MoveToTrash(dataDir);
        if (!Directory.EnumerateFiles(target, "xl.meta", SearchOption.AllDirectories).Any())
            Directory.Delete(target, recursive: true); // записей в поддереве нет
        return Task.CompletedTask;
    }

    // Per-key исходы; quiet фильтрует App (Storage игнорирует).
    public async Task<IReadOnlyList<DeletedKeyResult>> DeleteObjectsAsync(string bucket,
        IReadOnlyList<string> keys, bool quiet, CancellationToken ct)
    {
        EnsureBucket(bucket);
        var results = new List<DeletedKeyResult>(keys.Count);
        foreach (var key in keys)
        {
            try
            {
                await DeleteObjectAsync(bucket, key, ct);
                results.Add(new DeletedKeyResult(key, Deleted: true, null, null));
            }
            catch (ObjectStoreException ex)
            {
                results.Add(new DeletedKeyResult(key, Deleted: false, ex.Code.ToString(), ex.Message));
            }
        }
        return results;
    }

    // — транзитные заглушки до задач 7–9 (P6) —

    public Task<ObjectContent> GetObjectAsync(string bucket, string key, ObjectReadOptions options,
        CancellationToken ct) =>
        ThrowUnavailable<ObjectContent>(); // t37: реализация в Task 7 плана

    public Task<ObjectContent> HeadObjectAsync(string bucket, string key, ObjectReadOptions? options,
        CancellationToken ct) =>
        ThrowUnavailable<ObjectContent>(); // t37: реализация в Task 7 плана

    public Task<PutResult> CopyObjectAsync(CopyRequest request, CancellationToken ct) =>
        ThrowUnavailable<PutResult>(); // t37: реализация в Task 8 плана

    public Task<ObjectAttributesResult> GetObjectAttributesAsync(string bucket, string key,
        IReadOnlyList<ObjectAttributeName> attributes, int? maxParts, int? partNumberMarker,
        ObjectConditions? conditions, CancellationToken ct) =>
        ThrowUnavailable<ObjectAttributesResult>(); // t37: реализация в Task 8 плана

    public Task<ListPage> ListObjectsAsync(string bucket, ListQuery query, CancellationToken ct) =>
        ThrowUnavailable<ListPage>(); // t37: реализация в Task 9 плана
}
