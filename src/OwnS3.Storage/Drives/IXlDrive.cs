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
//
// Доставка исходов — Result-конвенция главы 01 §4: отказ (включая кворумный)
// — Failed с доменным кодом в Error (не летит через границу интерфейса);
// терминальное знание, включая «записи нет» (XlMetaLookup.Missing), — Success.
public interface IXlDrive
{
    // ── жизненный цикл тома ──
    Result Initialize();                                                    // 04 §3 / 06 §2 + старт-чистки; отказ тома — Failed
    Task<Result> RunCleanupAsync(CancellationToken ct);                     // 04 §6 / 06 §10; IO-отказ — Failed
    bool CheckHealth();                                                     // 05 §5: touch-проба: без доменных бросков (сверено)

    // ── бакеты (04 §1 / 06 §8) ──
    Result<bool> BucketExists(string bucket);                               // EC: НЕТ сетов с read-кворумом →
                                                                            // Failed(InsufficientReadQuorum) — «503, не „бакета нет“»,
                                                                            // канон 06 §7

    // Каталоги + bucket.json; creationDate — CreationDate реплики/бэктфила (t35, глава 07),
    // null = текущее время (клиентский CreateBucket).
    Result CreateBucket(string bucket, DateTimeOffset? creationDate = null); // кворумный отказ EC — Failed
    Result DeleteBucket(string bucket);                                     // emptiness-проверка + trash; BucketNotEmpty → Failed(ObjectStoreException)
    Result<IReadOnlyList<BucketEntry>> ListBuckets();                       // 06 §8: EC — консенсус по любому кворумному сету; кворумный отказ → Failed

    // ── объекты (04 §2/§4 / 06 §8) ──

    /// Исход поиска меты: Found | Missing («записи нет» — терминальный Success);
    /// кворумная недоступность EC — Failed(ObjectStoreException(Insufficient*)).
    Result<XlMetaLookup> TryReadMeta(string bucket, string key);

    /// Staging записи ключа (EC: сет ключа определяется bucket/key — SIPMOD).
    /// Каталог tmp без IO-семантики исхода: XlInvalidArgumentException из
    /// XlPathEncoder («Key too long») конвертируется в Failed НА ГРАНИЦЕ
    /// XlObjectStore/ReplicaStore — чистая функция кодирования остаётся исключением.
    IXlStaging CreateStaging(string bucket, string key);
    Task<Result<PartDigest>> WritePartAsync(IXlStaging staging, int partNumber, Stream body,
        long contentLength, CancellationToken ct);       // single: тело → part.N; EC: шардирование; кворум записи — Failed
    Result CopyPartsToStaging(IXlStaging staging, string srcBucket, string srcKey,
        XlMetaRecord srcMeta);                           // Copy: побайтовый перенос частей (single: part.N; EC: шардовые part.N)
    Result WriteMeta(IXlStaging staging, XlMetaRecord record); // single: xl.meta v1; EC: v2 per-disk (EcIndex диска)
    Result<PutResult> Commit(string bucket, string key, XlMetaRecord record, IXlStaging staging); // rename-схема 04 §4 (EC: по-дисково + кворум); кворум — Failed
    Result<Stream> OpenData(string bucket, string key, XlMetaRecord meta,
        long offset, long length);                       // конкурентное удаление/IO-отказ — Failed; кворум чтения EC — Failed
    Result VerifyFullChecksum(string bucket, string key, XlMetaRecord meta); // full-SHA исходных данных (битая структура — XlIntegrityException, вне перечня миграции)
    Result Delete(string bucket, string key, XlMetaRecord meta);  // trash меты+данных (EC: кворум); кворум — Failed
    Result<IReadOnlyList<(int Number, long Size)>> EnumerateParts(string bucket,
        string key, XlMetaRecord meta);                  // исходные размеры частей (GetObjectAttributes)

    // ── листинги (04 §1 / 06 §7–8) ──
    Result<IXlObjectCursor> EnumerateObjects(string bucket); // лениво, Utf8ByteOrder (EC: merge сетов + дедуп);
                                                             // EC-предиктат: онлайн < k хотя бы в одном сете → Failed(InsufficientReadQuorum) —
                                                             // «неполный листинг опаснее недоступности», канон 06 §7; курсорные
                                                             // XlIntegrityException битой структуры — вне перечня миграции,
                                                             // остаются исключениями КУРСОРА (ReadCurrentMeta), не драйва

    // ── multipart-журналы (04 §5 / 06 §8) ──
    Result<List<MultipartJournals.UploadJournalEntry>> ReadUploads(string bucket, string key);
    Result WriteUploads(string bucket, string key, List<MultipartJournals.UploadJournalEntry> uploads);

    /// Каталоги ключей с журналами загрузок (opaque-пути для ListMultipartUploads;
    /// JsonException из ReadUploadsByKeyDir по битому журналу не валит листинг).
    Result<IReadOnlyList<string>> ListUploadKeyDirs();
    Result<List<MultipartJournals.UploadJournalEntry>> ReadUploadsByKeyDir(string keyDirPath);

    bool UploadDirExists(string bucket, string key, string uploadId); // «проверили и знаем»: доменных бросков нет (Directory.Exists — сверено)
    Result CreateUploadDir(string bucket, string key, string uploadId, XlMetaRecord uploadRecord);

    /// Мета загрузки (xl.meta каталога uploadId) — источник метаданных Complete;
    /// нет каталога/меты → Failed(ObjectStoreException(NoSuchUpload)).
    Result<XlMetaRecord> ReadUploadMeta(string bucket, string key, string uploadId);

    Result TrashUpload(string bucket, string key, string uploadId);
    Result<List<MultipartJournals.PartJournalEntry>> ReadParts(string bucket, string key, string uploadId);
    IXlStaging CreateUploadStaging(string bucket, string key, string uploadId); // staging части ВНУТРИ загрузки (XlPathEncoder-бросок конвертируется на границе — как CreateStaging)
    Result CommitUploadPart(IXlStaging staging, int partNumber, string etagHex, long size,
        DateTimeOffset modTime);                         // rename части + upsert parts.json (EC: по-дисково + кворум); кворум — Failed

    /// attempt.json + перенос частей + xl.meta + зачистка (04 §5; EC — по-дисково
    /// с кворумом). newRecord — шаблон с метаданными загрузки (ContentType/
    /// UserMetadata/ModTime): versionId (=attempt-dataDir), size (Σ журнала),
    /// etag (composite) и ContentSha256 (проход по частям) заполняет драйв.
    /// NoSuchUpload/InvalidPart/кворум — Failed.
    Result<CompleteResult> CommitMultipart(string bucket, string key, string uploadId,
        IReadOnlyList<PartEtag> manifest, XlMetaRecord newRecord);
}
