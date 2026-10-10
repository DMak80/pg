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
        return new PutResult('"' + record.ETag + '"', record.ModTime);
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

    // — чтение (спека §4.4): conditional-таблица, Range/If-Range, полная
    // SHA-256 сверка ДО передачи любых байтов клиенту —

    // Чтение каталога объекта; отсутствие записи → NoSuchKey.
    private XlMetaRecord ReadObjectMeta(string bucket, string key)
    {
        EnsureBucket(bucket);
        try
        {
            var meta = XlMetaFile.Read(ObjectDir(bucket, key), out var fromBackup);
            if (fromBackup)
                _logger.LogWarning("xl.meta объекта {Bucket}/{Key} прочитан из страховочной копии", bucket, key);
            return meta;
        }
        catch (FileNotFoundException)
        {
            throw new ObjectStoreException(ObjectStoreErrorCode.NoSuchKey);
        }
    }

    // Доменное представление записи: ETag HTTP-значение — в кавычках (P8).
    private static ObjectMetadata ToMetadata(string key, XlMetaRecord record) =>
        new(key, '"' + record.ETag + '"', record.Size, record.ModTime, record.ContentType, record.UserMetadata);

    public Task<ObjectContent> GetObjectAsync(string bucket, string key, ObjectReadOptions options,
        CancellationToken ct)
    {
        // 1. Запись (с фолбэком bkp)
        var meta = ReadObjectMeta(bucket, key);
        // 2. Conditional (GET-семантика: If-Modified-Since применяется)
        var outcome = ConditionalEvaluator.Evaluate(options.Conditions, meta.ETag, meta.ModTime,
            ifModifiedSinceApplies: true);
        if (outcome == ConditionalOutcome.PreconditionFailed)
            throw new ObjectStoreException(ObjectStoreErrorCode.PreconditionFailed);
        if (outcome == ConditionalOutcome.NotModified)
            return Task.FromResult(new ObjectContent(ToMetadata(key, meta), Stream.Null, NotModified: true, Range: null));
        // 3. Range/If-Range (сверка — с ETag всего объекта, P8)
        var range = RangeResolver.Resolve(options.Range, options.IfRange, meta.ETag, meta.Size);
        // 4. Полная SHA-256 сверка ДО отдачи: Range не влияет на хэш (Q1);
        //    многочастевое тело — последовательный проход по всем part.N (М8)
        var partPaths = EnumerateDataParts(ObjectDir(bucket, key), meta.DataDirName)
            .Select(p => p.Path).ToList();
        VerifyChecksum(partPaths, meta);
        // 5. Тело: составной поток (полный объект или срез Range)
        Stream body = range is null
            ? new MultipartBodyStream(partPaths, 0, meta.Size)
            : new MultipartBodyStream(partPaths, range.Start, range.End - range.Start + 1);
        return Task.FromResult(new ObjectContent(ToMetadata(key, meta), body, NotModified: false, Range: range));
    }

    public Task<ObjectContent> HeadObjectAsync(string bucket, string key, ObjectReadOptions? options,
        CancellationToken ct)
    {
        // Шаги 1–3 те же; данных не читает — сверка не выполняется (канон 04 §2)
        var meta = ReadObjectMeta(bucket, key);
        var outcome = ConditionalEvaluator.Evaluate(options?.Conditions, meta.ETag, meta.ModTime,
            ifModifiedSinceApplies: true);
        if (outcome == ConditionalOutcome.PreconditionFailed)
            throw new ObjectStoreException(ObjectStoreErrorCode.PreconditionFailed);
        if (outcome == ConditionalOutcome.NotModified)
            return Task.FromResult(new ObjectContent(ToMetadata(key, meta), Stream.Null, NotModified: true, Range: null));
        var range = RangeResolver.Resolve(options?.Range, options?.IfRange, meta.ETag, meta.Size);
        return Task.FromResult(new ObjectContent(ToMetadata(key, meta), Stream.Null, NotModified: false, Range: range));
    }

    // Перечисление частей данных записи (М8): part.N в dataDir, номер из имени,
    // размер из файла, по возрастанию номеров; пусто → XlIntegrityException
    // (запись без данных — порча). Простой PUT — ровно part.1 (частный случай).
    internal static List<(int Number, string Path, long Size)> EnumerateDataParts(string objectDir, string dataDirName)
    {
        var dataDir = Path.Combine(objectDir, dataDirName);
        if (!Directory.Exists(dataDir))
            throw new XlIntegrityException($"Каталог данных отсутствует: {dataDir}");
        var parts = new List<(int Number, string Path, long Size)>();
        foreach (var file in Directory.EnumerateFiles(dataDir, "part.*"))
        {
            var name = Path.GetFileName(file);
            if (!int.TryParse(name["part.".Length..], out var number))
                continue; // не-числовые суффиксы — не части
            parts.Add((number, file, new FileInfo(file).Length));
        }
        if (parts.Count == 0)
            throw new XlIntegrityException($"Запись без данных (нет part.N): {dataDir}");
        parts.Sort((left, right) => left.Number.CompareTo(right.Number));
        return parts;
    }

    // Полный проход SHA-256 по всем частям (конкатенация в порядке номеров);
    // расхождение — невосстановимая порча: клиенту байты не отдаются (канон 04 §2).
    private static void VerifyChecksum(IReadOnlyList<string> partPaths, XlMetaRecord meta)
    {
        string actual;
        using (var sha = IncrementalHash.CreateHash(HashAlgorithmName.SHA256))
        {
            var buffer = new byte[64 * 1024];
            foreach (var path in partPaths)
                using (var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read))
                {
                    int read;
                    while ((read = stream.Read(buffer, 0, buffer.Length)) > 0)
                        sha.AppendData(buffer, 0, read);
                }
            actual = Convert.ToHexString(sha.GetHashAndReset()).ToLowerInvariant();
        }
        if (actual != meta.ContentSha256)
            throw new XlIntegrityException(
                $"checksum mismatch: объект {meta.VersionId:N}, ожидается {meta.ContentSha256}, факт {actual}");
    }

    // Тело-срез Range — MultipartBodyStream с параметрами (startOffset, length);
    // простой PUT — частный случай одной части (М8).
}
