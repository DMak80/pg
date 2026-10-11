using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace OwnS3.Storage;

// Multipart-операции XlObjectStore — все 6 канона 02 §5 поверх IXlDrive:
// валидации/видимость/идемпотентность здесь; журналы, каталоги загрузок,
// rename-коммиты — драйв (канон 04 §5).
public sealed partial class XlObjectStore
{
    // Лимиты канона 02 §1: минимальная часть (кроме последней) 5 МиБ;
    // максимальный размер части/копии 5 ГБ (совпадает с MaxCopySize PUT).
    internal const long MinPartSize = 5L * 1024 * 1024;
    internal const long MaxPartSize = 5L * 1024 * 1024 * 1024;

    // Сериализация журнальных пар журнала ключа (append/remove) — один лок
    // уровня store (commit-циклы частей/Complete — лок драйва).
    private readonly object _journalLock = new();

    // CreateMultipartUpload (канон 02 §5/04 §5): каталог загрузки + xl.meta
    // загрузки ДО записи в журнале (каталог без записи — невидим).
    public Task<string> CreateMultipartUploadAsync(string bucket, string key,
        ObjectUploadMetadata metadata, string initiatorAccessKey, CancellationToken ct)
    {
        EnsureBucket(bucket);
        var uploadId = Guid.NewGuid().ToString("N"); // формат N (канон 04 §5)
        var now = timeProvider.GetUtcNow();
        var record = new XlMetaRecord(Guid.Parse(uploadId), 0, now, "",
            metadata.ContentType, metadata.UserMetadata, EmptyHeaders, ""); // М4
        Drive.CreateUploadDir(bucket, key, uploadId, record);
        lock (_journalLock)
        {
            // Битый журнал ключа — как у остальных операций (М2/спека §4.1):
            // warning + NoSuchUpload, не сырой JsonException → 500
            var uploads = ReadUploadsOrThrow(bucket, key);
            uploads.Add(new MultipartJournals.UploadJournalEntry(uploadId, bucket, key,
                now.ToUnixTimeMilliseconds(), initiatorAccessKey));
            Drive.WriteUploads(bucket, key, uploads);
        }

        return Task.FromResult(uploadId);
    }

    // Abort (канон 02 §5/04 §5): под _journalLock — разрешение, удаление записи,
    // каталог в .trash. Повторный Abort после успешного — NoSuchUpload.
    public Task AbortMultipartUploadAsync(string bucket, string key, string uploadId, CancellationToken ct)
    {
        EnsureBucket(bucket);
        lock (_journalLock)
        {
            var uploads = ReadUploadsOrThrow(bucket, key);
            var entry = uploads.FirstOrDefault(e =>
                e.UploadId == uploadId && e.Bucket == bucket && e.Key == key)
                ?? throw new ObjectStoreException(ObjectStoreErrorCode.NoSuchUpload);
            if (!Drive.UploadDirExists(bucket, key, uploadId))
                throw new ObjectStoreException(ObjectStoreErrorCode.NoSuchUpload); // М2: призрачная запись
            uploads.Remove(entry);
            Drive.WriteUploads(bucket, key, uploads);
            Drive.TrashUpload(bucket, key, uploadId);
        }

        return Task.CompletedTask;
    }

    // Журнал ключа: битый JSON — warning + NoSuchUpload (М2); файла нет — пусто.
    internal List<MultipartJournals.UploadJournalEntry> ReadUploadsOrThrow(string bucket, string key)
    {
        try
        {
            return Drive.ReadUploads(bucket, key);
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(ex, "Битый uploads.json ключа {Bucket}/{Key}: загрузки недоступны", bucket, key);
            throw new ObjectStoreException(ObjectStoreErrorCode.NoSuchUpload);
        }
    }

    // Разрешение загрузки: запись ∧ каталог (М2); чужая для OwnedBy — NoSuchUpload (М9).
    internal MultipartJournals.UploadJournalEntry ResolveUploadOrThrow(
        string bucket, string key, string uploadId, UploadVisibility? visibility)
    {
        var uploads = ReadUploadsOrThrow(bucket, key);
        var entry = uploads.FirstOrDefault(e =>
            e.UploadId == uploadId && e.Bucket == bucket && e.Key == key)
            ?? throw new ObjectStoreException(ObjectStoreErrorCode.NoSuchUpload);
        if (!Drive.UploadDirExists(bucket, key, uploadId))
            throw new ObjectStoreException(ObjectStoreErrorCode.NoSuchUpload);
        if (visibility is UploadVisibility.Owned(var owner) && entry.AccessKey != owner)
            throw new ObjectStoreException(ObjectStoreErrorCode.NoSuchUpload);
        return entry;
    }

    // UploadPart (канон 02 §5/04 §5): тело → уникальный tmp в каталоге загрузки
    // (fsync, БЕЗ лока), затем согласованная пара rename части + upsert
    // parts.json под локом драйва (М3).
    public async Task<PutResult> UploadPartAsync(string bucket, string key, string uploadId, int partNumber,
        Stream body, long contentLength, CancellationToken ct)
    {
        EnsureBucket(bucket);
        ResolveUploadOrThrow(bucket, key, uploadId, visibility: null);
        using var staging = Drive.CreateUploadStaging(bucket, key, uploadId);
        // Тело: MD5-инкремент + счётчик + fsync (длина ≠ contentLength →
        // XlIntegrityException; staging-tmp удаляется Dispose при отказе)
        PartDigest digest;
        try
        {
            digest = await Drive.WritePartAsync(staging, partNumber, body, contentLength, ct);
        }
        catch
        {
            staging.Dispose();
            throw;
        }

        var modTime = timeProvider.GetUtcNow();
        await Task.Yield(); // async-контракт при синхронном файловом IO (P4 t37)
        try
        {
            Drive.CommitUploadPart(staging, partNumber, digest.ETagHex, digest.Length, modTime);
        }
        catch (JsonException ex)
        {
            // Битый parts.json — загрузка недоступна (единый исход с ListParts/Complete)
            _logger.LogWarning(ex, "Битый parts.json загрузки {UploadId}: загрузка недоступна", uploadId);
            throw new ObjectStoreException(ObjectStoreErrorCode.NoSuchUpload);
        }

        return new PutResult('"' + digest.ETagHex + '"', modTime);
    }

    // Журнал частей загрузки: битый parts.json (JsonException) — warning + загрузка
    // недоступна (NoSuchUpload) — единообразно с ListParts/Complete (спека §4.1).
    internal List<MultipartJournals.PartJournalEntry> ReadPartsOrThrow(string bucket, string key, string uploadId)
    {
        try
        {
            return Drive.ReadParts(bucket, key, uploadId);
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(ex, "Битый parts.json загрузки {UploadId}: загрузка недоступна", uploadId);
            throw new ObjectStoreException(ObjectStoreErrorCode.NoSuchUpload);
        }
    }

    // Complete (канон 02 §5, спека §4.2): сверка здесь, коммит-цикл (attempt →
    // перенос частей → composite ETag + SHA-256 → КОММИТ xl.meta → зачистка) —
    // драйв под своим commit-локом (идемпотентность попытки М7 — attempt.json).
    public async Task<CompleteResult> CompleteMultipartUploadAsync(string bucket, string key, string uploadId,
        IReadOnlyList<PartEtag> parts, CancellationToken ct)
    {
        EnsureBucket(bucket);
        // 1–2. Разрешение + сверка (ReadPartsOrThrow: битый журнал → NoSuchUpload)
        ResolveUploadOrThrow(bucket, key, uploadId, visibility: null);
        var journal = ReadPartsOrThrow(bucket, key, uploadId);
        ValidateManifestAgainstJournal(parts, journal);
        // Шаблон записи: метаданные загрузки; versionId/size/etag/sha — драйв
        var uploadMeta = Drive.ReadUploadMeta(bucket, key, uploadId);
        var newRecord = new XlMetaRecord(Guid.Empty, 0, timeProvider.GetUtcNow(), "",
            uploadMeta.ContentType, uploadMeta.UserMetadata, EmptyHeaders, "");
        await Task.Yield(); // async-контракт при синхронном файловом IO
        return Drive.CommitMultipart(bucket, key, uploadId, parts, newRecord);
    }

    // Сверка манифеста Complete с parts.json (канон 02 §5): каждая пара манифеста —
    // номер существует ∧ ETag совпал (кавычки снимаются, hex в lowercase); каждая
    // часть кроме последней ≥ MinPartSize. Провал — InvalidPart.
    internal static void ValidateManifestAgainstJournal(IReadOnlyList<PartEtag> manifest,
        List<MultipartJournals.PartJournalEntry> journal)
    {
        var byNumber = journal.ToDictionary(p => p.PartNumber);
        for (var i = 0; i < manifest.Count; i++)
        {
            var (number, etagRaw) = manifest[i];
            if (!byNumber.TryGetValue(number, out var uploaded))
                throw new ObjectStoreException(ObjectStoreErrorCode.InvalidPart);
            var expected = etagRaw.Trim('"').ToLowerInvariant();
            if (!string.Equals(uploaded.ETag, expected, StringComparison.Ordinal))
                throw new ObjectStoreException(ObjectStoreErrorCode.InvalidPart);
            if (i < manifest.Count - 1 && uploaded.Size < MinPartSize)
                throw new ObjectStoreException(ObjectStoreErrorCode.InvalidPart);
        }
    }

    // ListParts (канон 02 §5): части из parts.json по возрастанию; marker — строго после;
    // maxParts null = 1000; битый parts.json при живой записи → NoSuchUpload (спека §4.1,
    // ReadPartsOrThrow — единый исход с UploadPart/Complete).
    public Task<PartsPage> ListPartsAsync(string bucket, string key, string uploadId,
        int? maxParts, int? partNumberMarker, UploadVisibility visibility, CancellationToken ct)
    {
        EnsureBucket(bucket);
        ResolveUploadOrThrow(bucket, key, uploadId, visibility);
        var parts = ReadPartsOrThrow(bucket, key, uploadId);
        var marker = partNumberMarker ?? 0;
        var limit = maxParts ?? 1000;
        // max-parts=0: пустая страница БЕЗ усечения (конвенция ListWalker t37 —
        // не вечный пагинационный цикл из-за IsTruncated без маркера)
        if (limit == 0)
            return Task.FromResult(new PartsPage([], IsTruncated: false, NextPartNumberMarker: null));
        var selected = parts.Where(p => p.PartNumber > marker)
            .OrderBy(p => p.PartNumber)
            .Take(limit)
            .Select(p => new PartEntry(p.PartNumber, '"' + p.ETag + '"', p.Size,
                DateTimeOffset.FromUnixTimeMilliseconds(p.ModTimeMs)))
            .ToList();
        var truncated = parts.Count(p => p.PartNumber > marker) > selected.Count;
        return Task.FromResult(new PartsPage(selected, truncated,
            truncated && selected.Count > 0 ? selected[^1].PartNumber : null));
    }

    // ListMultipartUploads (канон 02 §5, М10): сбор всех записей бакета,
    // сортировка ключ(UTF-8 байты)→initiatedMs→uploadId, фильтры prefix/маркер-пара,
    // свёртка delimiter, max-uploads с подсчётом CommonPrefixes.
    public Task<UploadsPage> ListMultipartUploadsAsync(string bucket, UploadsQuery query, CancellationToken ct)
    {
        EnsureBucket(bucket);
        var all = new List<MultipartJournals.UploadJournalEntry>();
        foreach (var keyDirPath in Drive.ListUploadKeyDirs())
        {
            try
            {
                all.AddRange(Drive.ReadUploadsByKeyDir(keyDirPath)
                    .Where(e => e.Bucket == bucket));
            }
            catch (JsonException ex)
            {
                // Битый журнал одного ключа не валит листинг бакета (спека §4.1)
                _logger.LogWarning(ex, "Битый uploads.json в {KeyDir}: ключ пропущен", keyDirPath);
            }
        }

        // Видимость (М9): read-only — только свои
        all = all.Where(e => query.Visibility is not UploadVisibility.Owned(var owner)
                             || e.AccessKey == owner).ToList();
        // Сортировка М10
        all.Sort((l, r) =>
        {
            var byKey = Utf8ByteOrder.Compare(l.Key, r.Key);
            if (byKey != 0) return byKey;
            var byTime = l.InitiatedMs.CompareTo(r.InitiatedMs);
            return byTime != 0 ? byTime : string.CompareOrdinal(l.UploadId, r.UploadId);
        });
        // prefix
        if (query.Prefix is not null)
            all = all.Where(e => Utf8ByteOrder.StartsWith(e.Key, query.Prefix)).ToList();
        // маркер-пара (М10): строго после записи-маркера в порядке выдачи; маркер-CP
        // (NextKeyMarker = префикс) — при delimiter ключи этого префикса пропускаются
        // ЦЕЛИКОМ: уже выданный CommonPrefix не свёртывается повторно (без дублей)
        if (query.KeyMarker is not null)
        {
            var markerIndex = all.FindIndex(e => Utf8ByteOrder.Compare(e.Key, query.KeyMarker) == 0
                && query.UploadIdMarker is not null && e.UploadId == query.UploadIdMarker);
            // Префикс-пропуск — ТОЛЬКО для маркера-CP (NextKeyMarker = сам префикс,
            // оканчивается на delimiter): уже выданный CommonPrefix не сворачивается
            // повторно. Обычный key-marker без delimiter-суффикса — стандартное
            // «строго после пары» (строковый префикс key-marker НЕ выкидывает
            // соседние ключи: «a/x» не должен пропускать «a/x2», канон 02 §5)
            var markerIsCommonPrefix = query.Delimiter is not null
                && Utf8ByteOrder.EndsWith(query.KeyMarker, query.Delimiter);
            all = markerIndex >= 0
                ? all.Skip(markerIndex + 1).ToList()
                : all.Where(e => Utf8ByteOrder.Compare(e.Key, query.KeyMarker) > 0
                    && (!markerIsCommonPrefix
                        || !Utf8ByteOrder.StartsWith(e.Key, query.KeyMarker))).ToList();
        }

        // Свёртка delimiter + пагинация (uploads + CP вместе ≤ max-uploads, М10)
        var maxUploads = query.MaxUploads ?? 1000;
        // max-uploads=0: пустая страница без усечения (конвенция ListWalker t37)
        if (maxUploads == 0)
            return Task.FromResult(new UploadsPage([], [], IsTruncated: false,
                NextKeyMarker: null, NextUploadIdMarker: null));
        var uploads = new List<UploadEntry>();
        var prefixes = new List<CommonPrefixEntry>();
        bool truncated = false;
        string? nextKey = null, nextUploadId = null;
        foreach (var e in all)
        {
            string? commonPrefix = null;
            if (query.Delimiter is not null)
            {
                var idx = e.Key.IndexOf(query.Delimiter, query.Prefix?.Length ?? 0, StringComparison.Ordinal);
                if (idx >= 0)
                    commonPrefix = e.Key[..(idx + query.Delimiter.Length)];
            }

            var isSeenPrefix = commonPrefix is not null
                && prefixes.Any(p => p.Prefix == commonPrefix) ? (bool?)true : null;
            if (uploads.Count + prefixes.Count >= maxUploads
                && (commonPrefix is null || isSeenPrefix != true))
            {
                truncated = true; // осталась невыданная позиция
                break;
            }

            if (commonPrefix is null)
            {
                uploads.Add(new UploadEntry(e.Key, e.UploadId,
                    DateTimeOffset.FromUnixTimeMilliseconds(e.InitiatedMs)));
                nextKey = e.Key; nextUploadId = e.UploadId;
            }
            else if (isSeenPrefix != true)
            {
                prefixes.Add(new CommonPrefixEntry(commonPrefix));
                nextKey = commonPrefix; nextUploadId = null;
            }
        }

        return Task.FromResult(new UploadsPage(uploads, prefixes, truncated,
            truncated ? nextKey : null, truncated ? nextUploadId : null));
    }
}
