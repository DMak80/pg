using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace OwnS3.Storage;

// Staging объектной записи single-тома: tmp/<guid>/<versionId>/part.N + xl.meta
// (CreateStaging создаёт каталоги; Dispose — удалить staging при любом исходе).
internal sealed class SingleObjectStaging : IXlStaging
{
    public required string StagingDir { get; init; }
    public required string DataDir { get; init; }

    // versionId записи = имя dataDir (P1); XlObjectStore строит запись с
    // Guid.Empty — драйв нормализует в WriteMeta/Commit.
    public Guid VersionId => Guid.ParseExact(Path.GetFileName(DataDir), "N");

    public void Dispose()
    {
        if (Directory.Exists(StagingDir))
            Directory.Delete(StagingDir, recursive: true);
    }
}

// Staging части ВНУТРИ загрузки: тело пишется в уникальный tmp рядом с частью
// (параллельные UploadPart одного partNumber не конфликтуют, ревью t38),
// CommitUploadPart линеаризует rename'ом под _commitLock.
internal sealed class SingleUploadPartStaging : IXlStaging
{
    public required string UploadDir { get; init; }
    public string? TmpPath { get; set; }

    public void Dispose()
    {
        if (TmpPath is { } path && File.Exists(path))
            File.Delete(path);
    }
}

// Single-размещение (канон 04): один корень, данные как есть, xl.meta
// RecordVersion 1. Перенос механики XlVolume/XlObjectStore/BucketMetaStore/
// ListWalker-курсора; логика операций остаётся в XlObjectStore.
public sealed class SingleXlDrive(XlVolume volume, TimeProvider timeProvider, ILogger? logger = null) : IXlDrive
{
    private readonly BucketMetaStore _bucketMeta = new(volume, timeProvider);

    // Сериализация коммит-циклов одного процесса (как в XlObjectStore до
    // рефакторинга): rename/Write-файлы одного каталога не конфликтуют между
    // конкурентными операциями — один лок всего процесса.
    private readonly object _commitLock = new();

    // Тест-хук fallback копии: задан и вернул false — принудительное побайтовое
    // копирование (HardLinkProbe t37).
    internal Func<string, string, bool>? HardLinkProbe;

    // Тест-хук сбоя Complete между переносом частей и коммитом xl.meta.
    internal Action? CompletePreCommitProbe;

    // ── жизненный цикл тома ──

    public void Initialize() => volume.Initialize();

    public Task RunCleanupAsync(CancellationToken ct) => volume.RunCleanupAsync(ct);

    public bool CheckHealth() => volume.CheckHealth();

    // ── бакеты ──

    public bool BucketExists(string bucket) =>
        bucket != ".owns3.sys" && Directory.Exists(BucketRoot(bucket));

    public void CreateBucket(string bucket)
    {
        Directory.CreateDirectory(BucketRoot(bucket));
        _bucketMeta.CreateBucketMeta(bucket);
    }

    public void DeleteBucket(string bucket)
    {
        if (Directory.EnumerateFiles(BucketRoot(bucket), "xl.meta", SearchOption.AllDirectories).Any())
            throw new ObjectStoreException(ObjectStoreErrorCode.BucketNotEmpty);
        // Живые multipart-загрузки бакета блокируют удаление (канон 02 §1/§5):
        // активная запись = запись с существующим каталогом загрузки (М2)
        if (Directory.Exists(volume.MultipartDir))
            foreach (var keyDir in Directory.EnumerateDirectories(volume.MultipartDir))
            {
                List<MultipartJournals.UploadJournalEntry> uploads;
                try
                {
                    uploads = MultipartJournals.ReadUploads(MultipartJournals.UploadsJsonPath(keyDir));
                }
                catch (JsonException ex)
                {
                    logger?.LogWarning(ex, "Битый uploads.json в {KeyDir} при DeleteBucket: пропущен", keyDir);
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
    }

    public IReadOnlyList<BucketEntry> ListBuckets()
    {
        var names = Directory.EnumerateDirectories(volume.Root)
            .Select(Path.GetFileName)
            .Where(name => name is not null && name != ".owns3.sys")
            .Select(name => name!)
            .ToList();
        names.Sort(Utf8ByteOrder.Compare);
        return names
            .Select(name => new BucketEntry(name, _bucketMeta.TryReadCreationDate(name)
                ?? DateTimeOffset.FromUnixTimeMilliseconds(0)))
            .ToList();
    }

    // ── объекты ──

    public XlMetaRecord? TryReadMeta(string bucket, string key)
    {
        try
        {
            var meta = XlMetaFile.Read(ObjectDir(bucket, key), out var fromBackup);
            if (fromBackup)
                logger?.LogWarning("xl.meta объекта {Bucket}/{Key} прочитан из страховочной копии", bucket, key);
            return meta;
        }
        catch (FileNotFoundException)
        {
            return null;
        }
    }

    public IXlStaging CreateStaging()
    {
        var versionId = Guid.NewGuid();
        var stagingDir = Path.Combine(volume.TmpDir, Guid.NewGuid().ToString("N"));
        var dataDir = Path.Combine(stagingDir, versionId.ToString("N"));
        Directory.CreateDirectory(dataDir);
        return new SingleObjectStaging { StagingDir = stagingDir, DataDir = dataDir };
    }

    public async Task<PartDigest> WritePartAsync(IXlStaging staging, int partNumber, Stream body,
        long contentLength, CancellationToken ct)
    {
        if (staging is SingleUploadPartStaging upload)
        {
            // часть загрузки: тело → уникальный tmp в каталоге загрузки
            upload.TmpPath = Path.Combine(upload.UploadDir, MultipartJournals.PartTmpFileNameUnique(partNumber));
            var (uploadEtag, _) = await CopyBodyToFileAsync(body, upload.TmpPath, contentLength, withSha: false, ct);
            return new PartDigest(uploadEtag, string.Empty, contentLength);
        }

        var objectStaging = RequireObjectStaging(staging);
        var (etag, sha256) = await CopyBodyToFileAsync(
            body, Path.Combine(objectStaging.DataDir, MultipartJournals.PartFileName(partNumber)),
            contentLength, withSha: true, ct);
        return new PartDigest(etag, sha256, contentLength);
    }

    public void CopyPartsToStaging(IXlStaging staging, string srcBucket, string srcKey, XlMetaRecord srcMeta)
    {
        var objectStaging = RequireObjectStaging(staging);
        foreach (var (_, partPath, _) in EnumerateDataParts(ObjectDir(srcBucket, srcKey), srcMeta.DataDirName))
            CopyPart(partPath, Path.Combine(objectStaging.DataDir, Path.GetFileName(partPath)));
    }

    public void WriteMeta(IXlStaging staging, XlMetaRecord record)
    {
        var objectStaging = RequireObjectStaging(staging);
        // нормализация: versionId записи = имя dataDir staging (P1)
        XlMetaFile.Write(objectStaging.StagingDir, record with { VersionId = objectStaging.VersionId });
    }

    // Коммит-схема канона 04 §4: новый ключ — один rename; перезапись —
    // rename dataDir → атомарная замена xl.meta (КОММИТ) → старый dataDir в .trash.
    public PutResult Commit(string bucket, string key, XlMetaRecord record, IXlStaging staging)
    {
        var objectStaging = RequireObjectStaging(staging);
        record = record with { VersionId = objectStaging.VersionId }; // P1: dataDir = versionId
        var target = ObjectDir(bucket, key);
        var newDataDir = Path.Combine(objectStaging.StagingDir, record.DataDirName);
        lock (_commitLock)
        {
            if (!Directory.Exists(target))
            {
                try
                {
                    Directory.CreateDirectory(Path.GetDirectoryName(target)!);
                    Directory.Move(objectStaging.StagingDir, target); // единый rename = коммит
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
            logger?.LogWarning(
                "fsync каталога объекта не выполняется (только BCL, приказ пользователя): {Target}", target);
        }

        return new PutResult('"' + record.ETag + '"', record.ModTime);
    }

    public Stream OpenData(string bucket, string key, XlMetaRecord meta, long offset, long length)
    {
        var partPaths = EnumerateDataParts(ObjectDir(bucket, key), meta.DataDirName)
            .Select(p => p.Path).ToList();
        return new MultipartBodyStream(partPaths, offset, length);
    }

    public void VerifyFullChecksum(string bucket, string key, XlMetaRecord meta)
    {
        var partPaths = EnumerateDataParts(ObjectDir(bucket, key), meta.DataDirName)
            .Select(p => p.Path).ToList();
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

    // Delete канона 04 §4 п.5: xl.meta и dataDir — в .trash ПО ОТДЕЛЬНОСТИ;
    // опустевший каталог объекта удаляется, каталоги-префиксы с подобъектами остаются.
    public void Delete(string bucket, string key, XlMetaRecord meta)
    {
        var target = ObjectDir(bucket, key);
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
    }

    public IReadOnlyList<(int Number, long Size)> EnumerateParts(string bucket, string key, XlMetaRecord meta) =>
        EnumerateDataParts(ObjectDir(bucket, key), meta.DataDirName)
            .Select(p => (p.Number, p.Size))
            .ToList();

    // ── листинги ──

    public IXlObjectCursor EnumerateObjects(string bucket) =>
        new SingleObjectCursor(volume, bucket);

    // ── multipart-журналы ──

    public List<MultipartJournals.UploadJournalEntry> ReadUploads(string bucket, string key) =>
        MultipartJournals.ReadUploads(MultipartJournals.UploadsJsonPath(KeyDir(bucket, key)));

    public void WriteUploads(string bucket, string key, List<MultipartJournals.UploadJournalEntry> uploads) =>
        MultipartJournals.WriteUploads(MultipartJournals.UploadsJsonPath(KeyDir(bucket, key)), uploads);

    public IReadOnlyList<string> ListUploadKeyDirs() =>
        Directory.Exists(volume.MultipartDir)
            ? Directory.EnumerateDirectories(volume.MultipartDir).ToList()
            : [];

    public List<MultipartJournals.UploadJournalEntry> ReadUploadsByKeyDir(string keyDirPath) =>
        MultipartJournals.ReadUploads(MultipartJournals.UploadsJsonPath(keyDirPath));

    public bool UploadDirExists(string bucket, string key, string uploadId) =>
        Directory.Exists(MultipartJournals.UploadDirPath(KeyDir(bucket, key), uploadId));

    public void CreateUploadDir(string bucket, string key, string uploadId, XlMetaRecord uploadRecord)
    {
        var uploadDir = MultipartJournals.UploadDirPath(KeyDir(bucket, key), uploadId);
        Directory.CreateDirectory(uploadDir);
        XlMetaFile.Write(uploadDir, uploadRecord);
    }

    public XlMetaRecord ReadUploadMeta(string bucket, string key, string uploadId) =>
        XlMetaFile.Read(MultipartJournals.UploadDirPath(KeyDir(bucket, key), uploadId), out _);

    public void TrashUpload(string bucket, string key, string uploadId) =>
        volume.MoveToTrash(MultipartJournals.UploadDirPath(KeyDir(bucket, key), uploadId));

    public List<MultipartJournals.PartJournalEntry> ReadParts(string bucket, string key, string uploadId) =>
        MultipartJournals.ReadParts(MultipartJournals.PartsJsonPath(
            MultipartJournals.UploadDirPath(KeyDir(bucket, key), uploadId)));

    public IXlStaging CreateUploadStaging(string bucket, string key, string uploadId) =>
        new SingleUploadPartStaging
        {
            UploadDir = MultipartJournals.UploadDirPath(KeyDir(bucket, key), uploadId),
        };

    // Коммит части (UploadPart): rename + upsert parts.json — согласованная
    // пара под _commitLock (М3).
    public void CommitUploadPart(IXlStaging staging, int partNumber, string etagHex, long size, DateTimeOffset modTime)
    {
        var upload = RequireUploadStaging(staging);
        if (upload.TmpPath is null)
            throw new InvalidOperationException("часть не записана в staging (WritePartAsync)");

        lock (_commitLock)
        {
            File.Move(upload.TmpPath, Path.Combine(upload.UploadDir, MultipartJournals.PartFileName(partNumber)),
                overwrite: true);
            var parts = MultipartJournals.ReadParts(
                MultipartJournals.PartsJsonPath(upload.UploadDir));
            parts.RemoveAll(p => p.PartNumber == partNumber);
            parts.Add(new MultipartJournals.PartJournalEntry(partNumber, etagHex, size,
                modTime.ToUnixTimeMilliseconds()));
            parts.Sort((left, right) => left.PartNumber.CompareTo(right.PartNumber));
            MultipartJournals.WriteParts(MultipartJournals.PartsJsonPath(upload.UploadDir), parts);
        }

        upload.TmpPath = null; // закоммичен — Dispose не удаляет
    }

    // Complete (канон 04 §5 шаги 3–9): attempt → перенос частей rename'ами →
    // composite ETag + SHA-256 → КОММИТ xl.meta → зачистка. newRecord — шаблон
    // с метаданными загрузки; versionId/size/etag/sha заполняются здесь.
    public CompleteResult CommitMultipart(string bucket, string key, string uploadId,
        IReadOnlyList<PartEtag> manifest, XlMetaRecord newRecord)
    {
        var keyDir = KeyDir(bucket, key);
        var uploadDir = MultipartJournals.UploadDirPath(keyDir, uploadId);
        lock (_commitLock)
        {
            // Разрешение под коммит-локом: повторный Complete после зачистки
            // первым — NoSuchUpload (поведение XlObjectStore до рефакторинга)
            var currentUploads = MultipartJournals.ReadUploads(MultipartJournals.UploadsJsonPath(keyDir));
            if (currentUploads.All(e => e.UploadId != uploadId || e.Bucket != bucket || e.Key != key)
                || !Directory.Exists(uploadDir))
                throw new ObjectStoreException(ObjectStoreErrorCode.NoSuchUpload);
            // Попытка (М7): attempt.json — dataDir текущей сборки (идемпотентность
            // повтора); не-Guid маркер = битый/несовпадающий — трактуется как
            // отсутствующий (новая попытка; старый недособранный dataDir — orphan
            // по порогу 1 ч)
            var attempted = MultipartJournals.ReadAttempt(uploadDir)?.DataDir;
            var dataDirName = attempted is not null && Guid.TryParseExact(attempted, "N", out var parsed)
                ? parsed.ToString("N")
                : Guid.NewGuid().ToString("N");
            MultipartJournals.WriteAttempt(uploadDir, new MultipartJournals.AttemptMarker(dataDirName));
            var target = ObjectDir(bucket, key);
            var dataDir = Path.Combine(target, dataDirName);
            Directory.CreateDirectory(dataDir); // создаёт и каталоги-префиксы
            // Перенос частей (идемпотентность: перенесённое не трогать)
            foreach (var (number, _) in manifest)
            {
                var dest = Path.Combine(dataDir, MultipartJournals.PartFileName(number));
                if (File.Exists(dest))
                    continue;
                var src = Path.Combine(uploadDir, MultipartJournals.PartFileName(number));
                if (!File.Exists(src))
                    throw new ObjectStoreException(ObjectStoreErrorCode.InvalidPart);
                File.Move(src, dest);
            }

            // Точка сбоя (тест-хук)
            CompletePreCommitProbe?.Invoke();
            // Composite ETag (М6) + SHA-256 полным проходом по dataDir
            var journal = MultipartJournals.ReadParts(MultipartJournals.PartsJsonPath(uploadDir));
            var journalByNumber = journal.ToDictionary(p => p.PartNumber);
            var concat = string.Concat(manifest.Select(p => journalByNumber[p.PartNumber].ETag));
            var compositeEtag = manifest.Count + "-" + Convert.ToHexString(
                MD5.HashData(Encoding.ASCII.GetBytes(concat))).ToLowerInvariant();
            string sha256Hex;
            using (var sha = IncrementalHash.CreateHash(HashAlgorithmName.SHA256))
            {
                var buffer = new byte[64 * 1024];
                foreach (var (number, _) in manifest)
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

            // xl.meta объекта и КОММИТ (единственная точка видимости)
            var size = manifest.Sum(p => journalByNumber[p.PartNumber].Size);
            var record = newRecord with
            {
                VersionId = Guid.Parse(dataDirName), Size = size, ETag = compositeEtag, ContentSha256 = sha256Hex,
            };
            var oldDataDir = ReadCurrentDataDirOrNull(target);
            XlMetaFile.Write(target, record);
            // Зачистка после коммита
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
            return new CompleteResult('"' + compositeEtag + '"');
        }
    }

    // ── внутренняя механика ──

    internal string BucketRoot(string bucket) => Path.Combine(volume.Root, bucket);

    internal string ObjectDir(string bucket, string key) =>
        Path.Combine(BucketRoot(bucket), XlPathEncoder.EncodePath(key));

    internal string KeyDir(string bucket, string key) =>
        MultipartJournals.KeyDir(volume.MultipartDir, bucket, key);

    private static SingleObjectStaging RequireObjectStaging(IXlStaging staging) =>
        staging as SingleObjectStaging
        ?? throw new ArgumentException($"требуется объектный staging single-тома, получен {staging.GetType().Name}",
            nameof(staging));

    private static SingleUploadPartStaging RequireUploadStaging(IXlStaging staging) =>
        staging as SingleUploadPartStaging
        ?? throw new ArgumentException($"требуется staging части загрузки single-тома, получен {staging.GetType().Name}",
            nameof(staging));

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

    // Копирование тела в файл с инкрементальными MD5 (+SHA-256) и счётчиком;
    // фактическая длина ≠ заявленной → XlIntegrityException (спека §4.3 п.1);
    // fsync файла — Flush(flushToDisk: true) (чистый BCL).
    private static async Task<(string ETagHex, string Sha256Hex)> CopyBodyToFileAsync(
        Stream body, string filePath, long expectedLength, bool withSha, CancellationToken ct)
    {
        using var md5 = IncrementalHash.CreateHash(HashAlgorithmName.MD5);
        using var sha256 = withSha ? IncrementalHash.CreateHash(HashAlgorithmName.SHA256) : null;
        long total = 0;
        using (var file = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            var buffer = new byte[64 * 1024];
            int read;
            while ((read = await body.ReadAsync(buffer, ct)) > 0)
            {
                file.Write(buffer, 0, read);
                md5.AppendData(buffer, 0, read);
                sha256?.AppendData(buffer, 0, read);
                total += read;
            }

            file.Flush(flushToDisk: true);
        }

        if (total != expectedLength)
            throw new XlIntegrityException(
                $"Фактическая длина тела {total} не совпадает с заявленной {expectedLength}");
        return (Convert.ToHexString(md5.GetHashAndReset()).ToLowerInvariant(),
            sha256 is null ? string.Empty : Convert.ToHexString(sha256.GetHashAndReset()).ToLowerInvariant());
    }

    // part.N копии: хардлинк — мгновенная копия (Q2); на net10.0 BCL-API хардлинка
    // нет (File.CreateHardLink появился в net11) — fallback-копирование всегда.
    // HardLinkProbe (тест-хук) полностью заменяет попытку линка.
    private void CopyPart(string srcPart, string destPart)
    {
        var linked = HardLinkProbe is null ? TryCreateHardLink(srcPart, destPart)
                                           : HardLinkProbe(srcPart, destPart);
        if (linked)
            return;
        if (HardLinkProbe is null)
            logger?.LogWarning(
                "хардлинк недоступен (нет BCL-API в net10.0) — побайтовое копирование {Src} → {Dst}",
                srcPart, destPart);
        using (var src = new FileStream(srcPart, FileMode.Open, FileAccess.Read, FileShare.Read))
        using (var dst = new FileStream(destPart, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            src.CopyTo(dst);
            dst.Flush(flushToDisk: true);
        }
    }

    // Точка включения хардлинка: на net10.0 стандартного API нет — всегда false;
    // при переходе на net11 включить File.CreateHardLink(source, dest) одной
    // правкой здесь (Q2: копирование станет мгновенным).
    private static bool TryCreateHardLink(string source, string dest) => false;

    // Перечисление частей данных записи (М8): part.N в dataDir, номер из имени,
    // размер из файла, по возрастанию номеров; пусто → XlIntegrityException
    // (запись без данных — порча).
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

    // Курсор ключей бакета поверх DirectoryCursor (механика обхода — ListWalker).
    private sealed class SingleObjectCursor(XlVolume volume, string bucket) : IXlObjectCursor
    {
        private readonly DirectoryCursor _cursor =
            new(Path.Combine(volume.Root, bucket), ownKey: null, prefix: string.Empty, after: null);

        public string? PeekKey => _cursor.PeekKey;

        public void MoveNext() => _cursor.MoveNext();

        public XlMetaRecord ReadCurrentMeta()
        {
            var key = _cursor.PeekKey
                ?? throw new InvalidOperationException("курсор исчерпан — ReadCurrentMeta без ключа");
            var objectDir = Path.Combine([Path.Combine(volume.Root, bucket),
                .. XlPathEncoder.EncodePath(key).Split('/')]);
            return XlMetaFile.Read(objectDir, out _);
        }
    }
}
