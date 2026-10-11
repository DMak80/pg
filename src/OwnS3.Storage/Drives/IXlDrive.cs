namespace OwnS3.Storage;

// Дайджест ИСХОДНОГО потока части (single — из файла; EC — из кодера).
public readonly record struct PartDigest(string ETagHex, string Sha256Hex, long Length);

// Staging одной готовящейся записи (opaque для XlObjectStore):
// Dispose = удалить staging-каталоги (безвреден после успешного Commit).
public interface IXlStaging : IDisposable { }

// Ленивый курсор ключей бакета в Utf8ByteOrder (свёртка/пагинация — ListWalker).
public interface IXlObjectCursor
{
    // Минимальный ещё не эмитнутый ключ; null — обход исчерпан.
    string? PeekKey { get; }

    void MoveNext();

    // Мета ключа PeekKey (битая структура — XlIntegrityException,
    // конкурентное удаление — FileNotFoundException).
    XlMetaRecord ReadCurrentMeta();
}

// Способ размещения (механика — канон 04; EC-отличия — канон 06).
// Реализации: SingleXlDrive (глава 04), ErasureXlDrive (глава 06).
// XlObjectStore держит ВСЮ логику операций (conditional/Range/ETag/листинги/
// multipart-валидации/идемпотентность) поверх этого контракта; реализации
// решают только «где и как лежат байты и меты».
public interface IXlDrive
{
    // ── жизненный цикл тома ──
    void Initialize();                                   // 04 §3 / 06 §2 + старт-чистки
    Task RunCleanupAsync(CancellationToken ct);          // 04 §6 / 06 §10
    bool CheckHealth();                                  // 05 §5: touch-проба

    // ── бакеты (04 §1 / 06 §8) ──
    bool BucketExists(string bucket);
    void CreateBucket(string bucket);                    // каталоги + bucket.json
    void DeleteBucket(string bucket);                    // emptiness-проверка + trash
    IReadOnlyList<BucketEntry> ListBuckets();            // 06 §8: EC — консенсус по любому кворумному сету

    // ── объекты (04 §2/§4 / 06 §8) ──

    /// null — объекта нет; кворумная недоступность — ObjectStoreException(Insufficient*).
    XlMetaRecord? TryReadMeta(string bucket, string key);
    IXlStaging CreateStaging();
    Task<PartDigest> WritePartAsync(IXlStaging staging, int partNumber, Stream body,
        long contentLength, CancellationToken ct);       // single: тело → part.N; EC: шардирование
    void CopyPartsToStaging(IXlStaging staging, string srcBucket, string srcKey,
        XlMetaRecord srcMeta);                           // Copy: побайтовый перенос частей (single: part.N; EC: шардовые part.N)
    void WriteMeta(IXlStaging staging, XlMetaRecord record); // single: xl.meta v1; EC: v2 per-disk (EcIndex диска)
    PutResult Commit(string bucket, string key, XlMetaRecord record, IXlStaging staging); // rename-схема 04 §4 (EC: по-дисково + кворум)
    Stream OpenData(string bucket, string key, XlMetaRecord meta, long offset, long length);
    void VerifyFullChecksum(string bucket, string key, XlMetaRecord meta); // full-SHA исходных данных
    void Delete(string bucket, string key, XlMetaRecord meta);  // trash меты+данных (EC: кворум)
    IReadOnlyList<(int Number, long Size)> EnumerateParts(string bucket, string key,
        XlMetaRecord meta);                              // исходные размеры частей (GetObjectAttributes)

    // ── листинги (04 §1 / 06 §7–8) ──
    IXlObjectCursor EnumerateObjects(string bucket);     // лениво, Utf8ByteOrder (EC: merge сетов + дедуп)

    // ── multipart-журналы (04 §5 / 06 §8) ──
    List<MultipartJournals.UploadJournalEntry> ReadUploads(string bucket, string key);
    void WriteUploads(string bucket, string key, List<MultipartJournals.UploadJournalEntry> uploads);

    /// Каталоги ключей с журналами загрузок (opaque-пути для ListMultipartUploads;
    /// JsonException из ReadUploadsByKeyDir по битому журналу не валит листинг).
    IReadOnlyList<string> ListUploadKeyDirs();
    List<MultipartJournals.UploadJournalEntry> ReadUploadsByKeyDir(string keyDirPath);

    bool UploadDirExists(string bucket, string key, string uploadId);
    void CreateUploadDir(string bucket, string key, string uploadId, XlMetaRecord uploadRecord);

    /// Мета загрузки (xl.meta каталога uploadId) — источник метаданных Complete.
    XlMetaRecord ReadUploadMeta(string bucket, string key, string uploadId);

    void TrashUpload(string bucket, string key, string uploadId);
    List<MultipartJournals.PartJournalEntry> ReadParts(string bucket, string key, string uploadId);
    IXlStaging CreateUploadStaging(string bucket, string key, string uploadId); // staging части ВНУТРИ загрузки
    void CommitUploadPart(IXlStaging staging, int partNumber, string etagHex, long size,
        DateTimeOffset modTime);                         // rename части + upsert parts.json (EC: по-дисково + кворум)

    /// attempt.json + перенос частей + xl.meta + зачистка (04 §5; EC — по-дисково
    /// с кворумом). newRecord — шаблон с метаданными загрузки (ContentType/
    /// UserMetadata/ModTime): versionId (=attempt-dataDir), size (Σ журнала),
    /// etag (composite) и ContentSha256 (проход по частям) заполняет драйв.
    CompleteResult CommitMultipart(string bucket, string key, string uploadId,
        IReadOnlyList<PartEtag> manifest, XlMetaRecord newRecord);
}
