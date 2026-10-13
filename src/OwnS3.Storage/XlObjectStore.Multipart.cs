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
    public async Task<Result<string>> CreateMultipartUploadAsync(string bucket, string key,
        ObjectUploadMetadata metadata, string initiatorAccessKey, CancellationToken ct)
    {
        var ensured = EnsureBucket(bucket);
        if (!ensured.IsSuccess)
            return Result<string>.Failed(ensured.Error!);
        var uploadId = Guid.NewGuid().ToString("N"); // формат N (канон 04 §5)
        var now = timeProvider.GetUtcNow();
        var record = new XlMetaRecord(Guid.Parse(uploadId), 0, now, "",
            metadata.ContentType, metadata.UserMetadata, EmptyHeaders, ""); // М4
        var created = Drive.CreateUploadDir(bucket, key, uploadId, record);
        if (!created.IsSuccess)
            return Result<string>.Failed(created.Error!);
        lock (_journalLock)
        {
            // Битый журнал ключа — как у остальных операций (М2/спека §4.1):
            // warning + NoSuchUpload, не сырой JsonException → 500
            var uploads = ReadUploadsOrFailed(bucket, key);
            if (!uploads.IsSuccess)
                return Result<string>.Failed(uploads.Error!);
            uploads.Value.Add(new MultipartJournals.UploadJournalEntry(uploadId, bucket, key,
                now.ToUnixTimeMilliseconds(), initiatorAccessKey));
            var written = Drive.WriteUploads(bucket, key, uploads.Value);
            if (!written.IsSuccess)
                return Result<string>.Failed(written.Error!);
        }

        await Task.Yield(); // async-контракт
        return Result<string>.Success(uploadId);
    }

    // Abort (канон 02 §5/04 §5): под _journalLock — разрешение, удаление записи,
    // каталог в .trash. Повторный Abort после успешного — NoSuchUpload.
    public async Task<Result> AbortMultipartUploadAsync(string bucket, string key, string uploadId,
        CancellationToken ct)
    {
        var ensured = EnsureBucket(bucket);
        if (!ensured.IsSuccess)
            return ensured;
        lock (_journalLock)
        {
            var uploads = ReadUploadsOrFailed(bucket, key);
            if (!uploads.IsSuccess)
                return uploads;
            var entry = uploads.Value.FirstOrDefault(e =>
                e.UploadId == uploadId && e.Bucket == bucket && e.Key == key);
            if (entry is null)
                return Result.Failed(new ObjectStoreException(ObjectStoreErrorCode.NoSuchUpload));
            if (!Drive.UploadDirExists(bucket, key, uploadId))
                return Result.Failed(new ObjectStoreException(ObjectStoreErrorCode.NoSuchUpload)); // М2: призрачная запись
            uploads.Value.Remove(entry);
            var written = Drive.WriteUploads(bucket, key, uploads.Value);
            if (!written.IsSuccess)
                return written;
            return Drive.TrashUpload(bucket, key, uploadId);
        }
    }

    // Журнал ключа: битый JSON — warning + NoSuchUpload (М2); файла нет — пусто.
    internal Result<List<MultipartJournals.UploadJournalEntry>> ReadUploadsOrFailed(string bucket, string key)
    {
        try
        {
            return Drive.ReadUploads(bucket, key);
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(ex, "Битый uploads.json ключа {Bucket}/{Key}: загрузки недоступны", bucket, key);
            return Result<List<MultipartJournals.UploadJournalEntry>>.Failed(
                new ObjectStoreException(ObjectStoreErrorCode.NoSuchUpload));
        }
    }

    // Разрешение загрузки: запись ∧ каталог (М2); чужая для OwnedBy — NoSuchUpload (М9).
    internal Result<MultipartJournals.UploadJournalEntry> ResolveUploadOrFailed(
        string bucket, string key, string uploadId, UploadVisibility? visibility)
    {
        var uploads = ReadUploadsOrFailed(bucket, key);
        if (!uploads.IsSuccess)
            return Result<MultipartJournals.UploadJournalEntry>.Failed(uploads.Error!);
        var entry = uploads.Value.FirstOrDefault(e =>
            e.UploadId == uploadId && e.Bucket == bucket && e.Key == key);
        if (entry is null)
            return Result<MultipartJournals.UploadJournalEntry>.Failed(
                new ObjectStoreException(ObjectStoreErrorCode.NoSuchUpload));
        if (!Drive.UploadDirExists(bucket, key, uploadId))
            return Result<MultipartJournals.UploadJournalEntry>.Failed(
                new ObjectStoreException(ObjectStoreErrorCode.NoSuchUpload));
        if (visibility is UploadVisibility.Owned(var owner) && entry.AccessKey != owner)
            return Result<MultipartJournals.UploadJournalEntry>.Failed(
                new ObjectStoreException(ObjectStoreErrorCode.NoSuchUpload));
        return Result<MultipartJournals.UploadJournalEntry>.Success(entry);
    }

    // UploadPart (канон 02 §5/04 §5): тело → уникальный tmp в каталоге загрузки
    // (fsync, БЕЗ лока), затем согласованная пара rename части + upsert
    // parts.json под локом драйва (М3).
    public async Task<Result<PutResult>> UploadPartAsync(string bucket, string key, string uploadId, int partNumber,
        Stream body, long contentLength, CancellationToken ct)
    {
        var ensured = EnsureBucket(bucket);
        if (!ensured.IsSuccess)
            return Result<PutResult>.Failed(ensured.Error!);
        var resolved = ResolveUploadOrFailed(bucket, key, uploadId, visibility: null);
        if (!resolved.IsSuccess)
            return Result<PutResult>.Failed(resolved.Error!);
        try
        {
            // staging части ВНУТРИ загрузки; экранированные броски драйва — конвертация
            // на границе слоя операции: ключ не представим xl-путём (сегмент >255 байт,
            // канон 04 §1) — доменный 400; кворумный отказ staging-пути EC (сет ключа
            // без онлайн-дисков, multi-set) — доменный 503
            using var staging = Drive.CreateUploadStaging(bucket, key, uploadId);
            // Тело: MD5-инкремент + счётчик + fsync (длина ≠ contentLength →
            // XlIntegrityException; staging-tmp удаляется Dispose при отказе)
            var digestResult = await Drive.WritePartAsync(staging, partNumber, body, contentLength, ct);
            if (!digestResult.IsSuccess)
                return Result<PutResult>.Failed(digestResult.Error!);
            var digest = digestResult.Value;

            var modTime = timeProvider.GetUtcNow();
            await Task.Yield(); // async-контракт при синхронном файловом IO (P4 t37)
            Result committed;
            try
            {
                committed = Drive.CommitUploadPart(staging, partNumber, digest.ETagHex, digest.Length, modTime);
            }
            catch (JsonException ex)
            {
                // Битый parts.json — загрузка недоступна (единый исход с ListParts/Complete)
                _logger.LogWarning(ex, "Битый parts.json загрузки {UploadId}: загрузка недоступна", uploadId);
                return Result<PutResult>.Failed(new ObjectStoreException(ObjectStoreErrorCode.NoSuchUpload));
            }

            if (!committed.IsSuccess)
                return Result<PutResult>.Failed(committed.Error!);
            return Result<PutResult>.Success(
                new PutResult('"' + digest.ETagHex + '"', modTime, digest.Length, digest.Sha256Hex));
        }
        catch (Exception ex) when (ex is ObjectStoreException or XlInvalidArgumentException)
        {
            return Result<PutResult>.Failed(ex);
        }
    }

    // Журнал частей загрузки: битый parts.json (JsonException) — warning + загрузка
    // недоступна (NoSuchUpload) — единообразно с ListParts/Complete (спека §4.1).
    internal Result<List<MultipartJournals.PartJournalEntry>> ReadPartsOrFailed(string bucket, string key,
        string uploadId)
    {
        try
        {
            return Drive.ReadParts(bucket, key, uploadId);
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(ex, "Битый parts.json загрузки {UploadId}: загрузка недоступна", uploadId);
            return Result<List<MultipartJournals.PartJournalEntry>>.Failed(
                new ObjectStoreException(ObjectStoreErrorCode.NoSuchUpload));
        }
    }

    // Complete (канон 02 §5, спека §4.2): сверка здесь, коммит-цикл (attempt →
    // перенос частей → composite ETag + SHA-256 → КОММИТ xl.meta → зачистка) —
    // драйв под своим commit-локом (идемпотентность попытки М7 — attempt.json).
    public async Task<Result<CompleteResult>> CompleteMultipartUploadAsync(string bucket, string key, string uploadId,
        IReadOnlyList<PartEtag> parts, CancellationToken ct)
    {
        var ensured = EnsureBucket(bucket);
        if (!ensured.IsSuccess)
            return Result<CompleteResult>.Failed(ensured.Error!);
        // 1–2. Разрешение + сверка (ReadPartsOrFailed: битый журнал → NoSuchUpload)
        var resolved = ResolveUploadOrFailed(bucket, key, uploadId, visibility: null);
        if (!resolved.IsSuccess)
            return Result<CompleteResult>.Failed(resolved.Error!);
        var journal = ReadPartsOrFailed(bucket, key, uploadId);
        if (!journal.IsSuccess)
            return Result<CompleteResult>.Failed(journal.Error!);
        var validated = ValidateManifestAgainstJournal(parts, journal.Value);
        if (!validated.IsSuccess)
            return Result<CompleteResult>.Failed(validated.Error!);
        // Шаблон записи: метаданные загрузки; versionId/size/etag/sha — драйв
        var uploadMeta = Drive.ReadUploadMeta(bucket, key, uploadId);
        if (!uploadMeta.IsSuccess)
            return Result<CompleteResult>.Failed(uploadMeta.Error!);
        var newRecord = new XlMetaRecord(Guid.Empty, 0, timeProvider.GetUtcNow(), "",
            uploadMeta.Value.ContentType, uploadMeta.Value.UserMetadata, EmptyHeaders, "");
        await Task.Yield(); // async-контракт при синхронном файловом IO
        return Drive.CommitMultipart(bucket, key, uploadId, parts, newRecord);
    }

    // Сверка манифеста Complete с parts.json (канон 02 §5): каждая пара манифеста —
    // номер существует ∧ ETag совпал (кавычки снимаются, hex в lowercase); каждая
    // часть кроме последней ≥ MinPartSize. Провал — InvalidPart.
    internal static Result ValidateManifestAgainstJournal(IReadOnlyList<PartEtag> manifest,
        List<MultipartJournals.PartJournalEntry> journal)
    {
        var byNumber = journal.ToDictionary(p => p.PartNumber);
        for (var i = 0; i < manifest.Count; i++)
        {
            var (number, etagRaw) = manifest[i];
            if (!byNumber.TryGetValue(number, out var uploaded))
                return Result.Failed(new ObjectStoreException(ObjectStoreErrorCode.InvalidPart));
            var expected = etagRaw.Trim('"').ToLowerInvariant();
            if (!string.Equals(uploaded.ETag, expected, StringComparison.Ordinal))
                return Result.Failed(new ObjectStoreException(ObjectStoreErrorCode.InvalidPart));
            if (i < manifest.Count - 1 && uploaded.Size < MinPartSize)
                return Result.Failed(new ObjectStoreException(ObjectStoreErrorCode.InvalidPart));
        }

        return Result.Success();
    }

    // ListParts (канон 02 §5): части из parts.json по возрастанию; marker — строго после;
    // maxParts null = 1000; битый parts.json при живой записи → NoSuchUpload (спека §4.1,
    // ReadPartsOrFailed — единый исход с UploadPart/Complete).
    public async Task<Result<PartsPage>> ListPartsAsync(string bucket, string key, string uploadId,
        int? maxParts, int? partNumberMarker, UploadVisibility visibility, CancellationToken ct)
    {
        var ensured = EnsureBucket(bucket);
        if (!ensured.IsSuccess)
            return Result<PartsPage>.Failed(ensured.Error!);
        var resolved = ResolveUploadOrFailed(bucket, key, uploadId, visibility);
        if (!resolved.IsSuccess)
            return Result<PartsPage>.Failed(resolved.Error!);
        var parts = ReadPartsOrFailed(bucket, key, uploadId);
        if (!parts.IsSuccess)
            return Result<PartsPage>.Failed(parts.Error!);
        var marker = partNumberMarker ?? 0;
        var limit = maxParts ?? 1000;
        // max-parts=0: пустая страница БЕЗ усечения (конвенция ListWalker t37 —
        // не вечный пагинационный цикл из-за IsTruncated без маркера)
        if (limit == 0)
            return Result<PartsPage>.Success(new PartsPage([], IsTruncated: false, NextPartNumberMarker: null));
        var selected = parts.Value.Where(p => p.PartNumber > marker)
            .OrderBy(p => p.PartNumber)
            .Take(limit)
            .Select(p => new PartEntry(p.PartNumber, '"' + p.ETag + '"', p.Size,
                DateTimeOffset.FromUnixTimeMilliseconds(p.ModTimeMs)))
            .ToList();
        var truncated = parts.Value.Count(p => p.PartNumber > marker) > selected.Count;
        return Result<PartsPage>.Success(new PartsPage(selected, truncated,
            truncated && selected.Count > 0 ? selected[^1].PartNumber : null));
    }

    // ListMultipartUploads (канон 02 §5, М10): сбор всех записей бакета,
    // сортировка ключ(UTF-8 байты)→initiatedMs→uploadId, фильтры prefix/маркер-пара,
    // свёртка delimiter, max-uploads с подсчётом CommonPrefixes.
    public async Task<Result<UploadsPage>> ListMultipartUploadsAsync(string bucket, UploadsQuery query,
        CancellationToken ct)
    {
        var ensured = EnsureBucket(bucket);
        if (!ensured.IsSuccess)
            return Result<UploadsPage>.Failed(ensured.Error!);
        var all = new List<MultipartJournals.UploadJournalEntry>();
        var keyDirs = Drive.ListUploadKeyDirs();
        if (!keyDirs.IsSuccess)
            return Result<UploadsPage>.Failed(keyDirs.Error!);
        foreach (var keyDirPath in keyDirs.Value)
        {
            try
            {
                var read = Drive.ReadUploadsByKeyDir(keyDirPath);
                if (!read.IsSuccess)
                {
                    // Битый журнал одного ключа не валит листинг бакета (спека §4.1)
                    _logger.LogWarning(read.Error, "Битый uploads.json в {KeyDir}: ключ пропущен", keyDirPath);
                    continue;
                }

                all.AddRange(read.Value.Where(e => e.Bucket == bucket));
            }
            catch (JsonException ex)
            {
                // Битый журнал одного ключа не валит листинг бакета (спека §4.1)
                _logger.LogWarning(ex, "Битый uploads.json в {KeyDir}: ключ пропущен", keyDirPath);
            }
            catch (FileNotFoundException)
            {
                // keyDir есть, журнала нет (рассинхрон носителей после отказов) —
                // загрузок этого ключа не видно, не ошибка листинга
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
            return Result<UploadsPage>.Success(new UploadsPage([], [], IsTruncated: false,
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

        return Result<UploadsPage>.Success(new UploadsPage(uploads, prefixes, truncated,
            truncated ? nextKey : null, truncated ? nextUploadId : null));
    }
}
