using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace OwnS3.Storage;

// Multipart-операции XlObjectStore — все 7 канона 02 §5: CreateMultipartUpload,
// UploadPart, UploadPartCopy, CompleteMultipartUpload, AbortMultipartUpload,
// ListParts, ListMultipartUploads. Журналы и раскладка — канон 04 §5
// (MultipartJournals); многочастевое чтение — MultipartBodyStream (Objects).
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
            // Битый журнал ключа — как у остальных операций (М2/спека §4.1):
            // warning + NoSuchUpload, не сырой JsonException → 500
            var uploads = ReadUploadsOrThrow(keyDir);
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
        var tmpPath = Path.Combine(uploadDir, MultipartJournals.PartTmpFileNameUnique(partNumber));
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

    // UploadPartCopy (канон 02 §5, спека §4.2): разрешение приёмника → чтение источника →
    // conditional → резолв/лимит диапазона → срез по границам частей в part.N.tmp →
    // коммит части (механика UploadPart).
    public async Task<PutResult> UploadPartCopyAsync(PartCopyRequest request, CancellationToken ct)
    {
        // 1. Приёмник
        EnsureBucket(request.DestBucket);
        ResolveUploadOrThrow(request.DestBucket, request.DestKey, request.UploadId, visibility: null);
        // 2. Источник
        EnsureBucket(request.SourceBucket);
        XlMetaRecord src;
        try
        {
            src = XlMetaFile.Read(ObjectDir(request.SourceBucket, request.SourceKey), out var fromBackup);
            if (fromBackup)
                _logger.LogWarning("xl.meta источника {Bucket}/{Key} прочитан из страховочной копии",
                    request.SourceBucket, request.SourceKey);
        }
        catch (FileNotFoundException)
        {
            throw new ObjectStoreException(ObjectStoreErrorCode.NoSuchKey);
        }
        // 3. Conditional источника (copy — не GET/HEAD; провал → 412, как CopyObject t37)
        var outcome = ConditionalEvaluator.Evaluate(request.SourceConditions, src.ETag, src.ModTime,
            ifModifiedSinceApplies: false);
        if (outcome != ConditionalOutcome.Proceed)
            throw new ObjectStoreException(ObjectStoreErrorCode.PreconditionFailed);
        // 4. Резолв диапазона: null → весь объект; выход за размер → InvalidArgument;
        //    длина > MaxPartSize → EntityTooLarge (ДО чтения данных)
        long start, end;
        if (request.SourceRange is { } range)
        {
            (start, end) = (range.Start!.Value, range.End!.Value);
            if (start >= src.Size || end >= src.Size)
                throw new XlInvalidArgumentException(
                    $"Диапазон копирования [{start}, {end}] выходит за размер источника {src.Size}");
        }
        else
        {
            (start, end) = (0, src.Size - 1);
        }
        var length = end - start + 1;
        if (length > MaxPartSize)
            throw new ObjectStoreException(ObjectStoreErrorCode.EntityTooLarge);
        // 5. Срез источника → part.N.tmp приёмника (MD5-инкремент + счётчик + fsync)
        var keyDir = MultipartJournals.KeyDir(volume.MultipartDir, request.DestBucket, request.DestKey);
        var uploadDir = MultipartJournals.UploadDirPath(keyDir, request.UploadId);
        var tmpPath = Path.Combine(uploadDir, MultipartJournals.PartTmpFileNameUnique(request.PartNumber));
        var (etagHex, total) = await CopySliceToFileAsync(
            ObjectDir(request.SourceBucket, request.SourceKey), src.DataDirName, start, length, tmpPath);
        // 6. Коммит части (rename + upsert parts.json под _commitLock)
        var modTime = timeProvider.GetUtcNow();
        lock (_commitLock)
        {
            CommitPartFile(uploadDir, request.PartNumber, tmpPath, etagHex, total, modTime);
        }
        // 7. Возврат
        return new PutResult('"' + etagHex + '"', modTime);
    }

    // Срез источника по границам частей (спека §4.3): маппинг offset → части,
    // последовательное чтение ровно length байтов с записью в часть приёмника.
    private static async Task<(string ETagHex, long Total)> CopySliceToFileAsync(
        string sourceObjectDir, string sourceDataDirName, long start, long length, string destPath)
    {
        using var md5 = IncrementalHash.CreateHash(HashAlgorithmName.MD5);
        long copied = 0;
        using (var file = new FileStream(destPath, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            var buffer = new byte[64 * 1024];
            foreach (var (_, partPath, partSize) in EnumerateDataParts(sourceObjectDir, sourceDataDirName))
            {
                if (start >= partSize)
                {
                    start -= partSize; // диапазон начинается дальше этой части
                    continue;
                }
                using var src = new FileStream(partPath, FileMode.Open, FileAccess.Read, FileShare.Read);
                src.Seek(start, SeekOrigin.Begin);
                start = 0;
                while (copied < length)
                {
                    var want = (int)Math.Min(buffer.Length, length - copied);
                    var read = await src.ReadAsync(buffer.AsMemory(0, want));
                    if (read == 0)
                        break; // часть исчерпана — следующая
                    file.Write(buffer, 0, read);
                    md5.AppendData(buffer, 0, read);
                    copied += read;
                }
                if (copied >= length)
                    break;
            }
            file.Flush(flushToDisk: true);
        }
        if (copied != length)
            throw new XlIntegrityException($"Срез источника короче заявленного: {copied} из {length}");
        return (Convert.ToHexString(md5.GetHashAndReset()).ToLowerInvariant(), copied);
    }

    // Тест-хук сбоя между переносом частей и коммитом (по образцу HardLinkProbe t37):
    // задан → вызывается ПОСЛЕ переноса всех частей и ДО записи xl.meta объекта;
    // исключение хука эмулирует крах процесса до коммит-поинта.
    internal Action? CompletePreCommitProbe;

    // Complete (канон 02 §5, спека §4.2 шаги 1–9): весь под _commitLock; сверка →
    // попытка (attempt.json) → перенос частей rename'ами → составной ETag + SHA-256 →
    // КОММИТ xl.meta (единственная точка видимости) → зачистка (старый dataDir,
    // запись журнала, каталог загрузки).
    public Task<CompleteResult> CompleteMultipartUploadAsync(string bucket, string key, string uploadId,
        IReadOnlyList<PartEtag> parts, CancellationToken ct)
    {
        EnsureBucket(bucket);
        var keyDir = MultipartJournals.KeyDir(volume.MultipartDir, bucket, key);
        var uploadDir = MultipartJournals.UploadDirPath(keyDir, uploadId);
        lock (_commitLock)
        {
            // 1–2. Разрешение + сверка (ReadPartsOrThrow: битый журнал → NoSuchUpload)
            ResolveUploadOrThrow(bucket, key, uploadId, visibility: null);
            var journal = ReadPartsOrThrow(uploadDir);
            ValidateManifestAgainstJournal(parts, journal);
            // 3. Попытка (М7): attempt.json — dataDir текущей сборки (идемпотентность
            // повтора); не-Guid маркер = битый/несовпадающий — трактуется как
            // отсутствующий (новая попытка; старый недособранный dataDir — orphan
            // по порогу 1 ч), иначе Guid.Parse даст перманентный 500 повторов
            var attempted = MultipartJournals.ReadAttempt(uploadDir)?.DataDir;
            var dataDirName = attempted is not null && Guid.TryParseExact(attempted, "N", out var parsed)
                ? parsed.ToString("N")
                : Guid.NewGuid().ToString("N");
            MultipartJournals.WriteAttempt(uploadDir, new MultipartJournals.AttemptMarker(dataDirName));
            var target = ObjectDir(bucket, key);
            var dataDir = Path.Combine(target, dataDirName);
            Directory.CreateDirectory(dataDir); // создаёт и каталоги-префиксы
            // 4. Перенос частей (идемпотентность: перенесённое не трогать)
            foreach (var (number, _) in parts)
            {
                var dest = Path.Combine(dataDir, MultipartJournals.PartFileName(number));
                if (File.Exists(dest))
                    continue;
                var src = Path.Combine(uploadDir, MultipartJournals.PartFileName(number));
                if (!File.Exists(src))
                    throw new ObjectStoreException(ObjectStoreErrorCode.InvalidPart);
                File.Move(src, dest);
            }
            // 5. Точка сбоя (тест-хук)
            CompletePreCommitProbe?.Invoke();
            // 6. Составной ETag (М6) + SHA-256 полным проходом по dataDir
            var journalByNumber = journal.ToDictionary(p => p.PartNumber);
            var concat = string.Concat(parts.Select(p => journalByNumber[p.PartNumber].ETag));
            var compositeEtag = parts.Count + "-" + Convert.ToHexString(
                MD5.HashData(Encoding.ASCII.GetBytes(concat))).ToLowerInvariant();
            string sha256Hex;
            using (var sha = IncrementalHash.CreateHash(HashAlgorithmName.SHA256))
            {
                var buffer = new byte[64 * 1024];
                foreach (var (number, _) in parts)
                    using (var stream = new FileStream(
                        Path.Combine(dataDir, MultipartJournals.PartFileName(number)),
                        FileMode.Open, FileAccess.Read, FileShare.Read))
                    {
                        int read;
                        while ((read = stream.Read(buffer, 0, buffer.Length)) > 0)
                            sha.AppendData(buffer, 0, read);
                    }
                sha256Hex = Convert.ToHexString(sha.GetHashAndReset()).ToLowerInvariant();
            }
            // 7–8. xl.meta объекта и КОММИТ (единственная точка видимости)
            var uploadMeta = XlMetaFile.Read(uploadDir, out _);
            var size = parts.Sum(p => journalByNumber[p.PartNumber].Size);
            var record = new XlMetaRecord(Guid.Parse(dataDirName), size, timeProvider.GetUtcNow(),
                compositeEtag, uploadMeta.ContentType, uploadMeta.UserMetadata, EmptyHeaders, sha256Hex);
            var oldDataDir = ReadCurrentDataDirOrNull(target);
            XlMetaFile.Write(target, record);
            // 9. Зачистка после коммита
            if (oldDataDir is not null && oldDataDir != dataDirName)
            {
                var oldPath = Path.Combine(target, oldDataDir);
                if (Directory.Exists(oldPath))
                    volume.MoveToTrash(oldPath);
            }
            var uploads = MultipartJournals.ReadUploads(MultipartJournals.UploadsJsonPath(keyDir));
            uploads.RemoveAll(e => e.UploadId == uploadId && e.Bucket == bucket && e.Key == key);
            MultipartJournals.WriteUploads(MultipartJournals.UploadsJsonPath(keyDir), uploads);
            volume.MoveToTrash(uploadDir);
            return Task.FromResult(new CompleteResult('"' + compositeEtag + '"'));
        }
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
        var uploadDir = MultipartJournals.UploadDirPath(
            MultipartJournals.KeyDir(volume.MultipartDir, bucket, key), uploadId);
        var parts = ReadPartsOrThrow(uploadDir);
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

    // ListMultipartUploads (канон 02 §5, М10): сбор всех записей бакета из sha-каталогов,
    // сортировка ключ(UTF-8 байты)→initiatedMs→uploadId, фильтры prefix/маркер-пара,
    // свёртка delimiter, max-uploads с подсчётом CommonPrefixes.
    public Task<UploadsPage> ListMultipartUploadsAsync(string bucket, UploadsQuery query, CancellationToken ct)
    {
        EnsureBucket(bucket);
        var all = new List<MultipartJournals.UploadJournalEntry>();
        if (Directory.Exists(volume.MultipartDir))
            foreach (var keyDir in Directory.EnumerateDirectories(volume.MultipartDir))
            {
                try
                {
                    all.AddRange(MultipartJournals.ReadUploads(MultipartJournals.UploadsJsonPath(keyDir))
                        .Where(e => e.Bucket == bucket));
                }
                catch (JsonException ex)
                {
                    // Битый журнал одного ключа не валит листинг бакета (спека §4.1)
                    _logger.LogWarning(ex, "Битый uploads.json в {KeyDir}: ключ пропущен", keyDir);
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
