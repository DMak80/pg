using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using OwnS3.Storage.Erasure;

namespace OwnS3.Storage;

// Staging EC-записи: на каждом онлайн-диске сета tmp/<guid>/<dataDir>/;
// шард-потоки части N лежат на позициях distribution[j]. Dispose — удалить
// staging-каталоги всех дисков (безвреден после Commit).
internal sealed class ErasureStaging : IXlStaging
{
    public required Guid VersionId { get; init; }
    public required IReadOnlyList<ErasureDisk> SetDisks { get; init; }
    public required int[] Distribution { get; init; } // перестановка формата тома

    // staging-каталог диска (tmp/<guid>) — коммит переносит его в bucket-дерево
    public required Dictionary<ErasureDisk, string> StagingDirs { get; init; }

    // результаты шардирования по частям (для xl.meta v2 и дайджестов)
    public required Dictionary<int, ErasurePartResult> PartResults { get; init; }

    // xl.meta-запись по дискам (WriteMeta; CommitOnDisk при перезаписи)
    public Dictionary<ErasureDisk, XlMetaRecord> DiskRecords { get; } = new();

    public required int Parity { get; init; } // parity этой записи (per-object)

    public void Dispose()
    {
        foreach (var dir in StagingDirs.Values)
        {
            if (Directory.Exists(dir))
                Directory.Delete(dir, recursive: true);
        }
    }
}

// Драйв EC (канон 06): фасад над ErasurePool; объектные операции — на сете
// PickSetIndex(bucket, key); бакетные/листинги — на всех сетах (Task 10).
// Зоны записи реализуются здесь (Task 8); чтение — Task 9, бакеты/листинги —
// Task 10, multipart — Task 11, полные чистки — Task 12.
public sealed partial class ErasureXlDrive : IXlDrive
{
    private readonly ErasurePool _pool;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<ErasureXlDrive> _logger;
    private readonly int _defaultParity;

    // кэш кодеков по parity (матрица иммутабельна — потокобезопасно)
    private readonly Dictionary<int, ReedSolomonCodec> _codecCache = new();
    private readonly object _codecLock = new();

    // сериализация коммит-циклов (по-дисковые rename одного объекта)
    private readonly object _commitLock = new();

    // общий лок журнальных операций драйва (uploads.json/parts.json/attempt.json/
    // xl.meta загрузок): heal-выравнивание журналов (ErasureHealService) берёт его
    // на цикл «прочитать reference → скопировать» — вне лока микросекундное окно
    // между чтением эталона и записью копии позволяло перезаписать свежую запись
    // клиента устаревшим содержимым (mtime копии закреплял откат на следующем
    // проходе). Порядок вложенности всегда _commitLock → JournalLock
    internal readonly object JournalLock = new();

    public ErasureXlDrive(ErasurePool pool, int defaultParity, TimeProvider timeProvider,
        ILogger<ErasureXlDrive> logger)
    {
        _pool = pool;
        _defaultParity = defaultParity;
        _timeProvider = timeProvider;
        _logger = logger;
    }

    private ErasurePool Pool => _pool;

    // default parity драйва (конвенция parity загрузок — heal Complete-семантики)
    internal int DefaultParity => _defaultParity;

    // Ошибка I/O переводит диск сета в offline до ближайшей успешной
    // перепроверки (тикер 30 с / heal-проход — канон 06 §7): операции не
    // бьются об умерший диск до конца таймера.
    private static void MarkOfflineIfIo(ErasureDisk disk, Exception ex)
    {
        if (ex is IOException or UnauthorizedAccessException or DirectoryNotFoundException)
            disk.Online = false;
    }

    // Кодек сета для parity записи (per-object parity, канон 06 §1).
    private ReedSolomonCodec CodecFor(int parity)
    {
        lock (_codecLock)
        {
            if (_codecCache.TryGetValue(parity, out var codec))
                return codec;

            var setSize = _pool.Topology.SetSize;
            codec = new ReedSolomonCodec(setSize - parity, parity);
            _codecCache[parity] = codec;
            return codec;
        }
    }

    // ── жизненный цикл тома ──

    public void Initialize()
    {
        _pool.Initialize();
        _ = _pool.StartOnlineTicker();
    }

    public void RefreshOnline() => _pool.RefreshOnline();

    // ── объекты: запись (Task 8) ──

    // Staging записи: на каждом онлайн-диске сета ключа tmp/<guid>/<dataDir>/.
    public IXlStaging CreateStaging(string bucket, string key)
    {
        var setIndex = _pool.PickSetIndex(bucket, key);
        var setDisks = _pool.Sets[setIndex];
        var versionId = Guid.NewGuid();
        var stagingGuid = Guid.NewGuid().ToString("N");
        var distribution = ReadDistribution(setDisks);
        var stagingDirs = new Dictionary<ErasureDisk, string>();
        foreach (var disk in setDisks)
        {
            if (!disk.Online)
                continue;
            var stagingDir = Path.Combine(disk.Root, ".owns3.sys", "tmp", stagingGuid);
            Directory.CreateDirectory(Path.Combine(stagingDir, versionId.ToString("N")));
            stagingDirs[disk] = stagingDir;
        }

        return new ErasureStaging
        {
            VersionId = versionId,
            SetDisks = setDisks,
            Distribution = distribution,
            StagingDirs = stagingDirs,
            PartResults = new Dictionary<int, ErasurePartResult>(),
            Parity = _defaultParity,
        };
    }

    // Distribution формата тома (общая для всех сетов; читается с первого валидного).
    private int[] ReadDistribution(IReadOnlyList<ErasureDisk> setDisks)
    {
        foreach (var disk in setDisks.Where(d => d.Online))
        {
            var info = ErasureVolumeInfo.Read(disk.Root);
            return info.Distribution;
        }

        throw new ObjectStoreException(ObjectStoreErrorCode.InsufficientReadQuorum);
    }

    private async Task<PartDigest> WriteObjectPartAsync(IXlStaging staging, int partNumber, Stream body,
        long contentLength, CancellationToken ct)
    {
        var erasureStaging = RequireErasureStaging(staging);
        var codec = CodecFor(erasureStaging.Parity);
        var online = erasureStaging.SetDisks.Where(d => d.Online).ToList();
        var k = codec.DataShards;
        if (online.Count < k)
            throw new ObjectStoreException(ObjectStoreErrorCode.InsufficientWriteQuorum);

        // шард j блока → диск distribution[j]; офлайн-позиция — шард не пишется
        // (канон 06 §8 п.1: потоки только каждого ОНЛАЙН-диска; недостающий шард
        // реконструируется при чтении) — заглушка-поток собирает bitrot-хэши
        var dataDir = erasureStaging.VersionId.ToString("N");
        var streams = new Stream[codec.TotalShards];
        try
        {
            for (var j = 0; j < codec.TotalShards; j++)
            {
                var positionInSet = erasureStaging.Distribution[j];
                var disk = online.FirstOrDefault(d => d.PositionInSet == positionInSet);
                streams[j] = disk is not null
                    ? new FileStream(Path.Combine(erasureStaging.StagingDirs[disk], dataDir, $"part.{partNumber}"),
                        FileMode.Create, FileAccess.Write, FileShare.None)
                    : new MemoryStream(); // офлайн-позиция: шард в никуда
            }

            using var writer = new ErasureBlockWriter(codec, streams);
            await writer.WriteAsync(body, contentLength, ct);
            var result = await writer.CompleteAsync();
            erasureStaging.PartResults[partNumber] = result;
            return new PartDigest(result.ETagHex, result.Sha256Hex, result.SourceLength);
        }
        finally
        {
            foreach (var stream in streams)
                stream?.Dispose();
        }
    }

    public void WriteMeta(IXlStaging staging, XlMetaRecord record)
    {
        var erasureStaging = RequireErasureStaging(staging);
        // v2 per-disk: одинаковая запись, кроме EcIndex (позиция диска в сете)
        foreach (var (disk, stagingDir) in erasureStaging.StagingDirs)
        {
            var diskRecord = record with
            {
                VersionId = erasureStaging.VersionId,
                Ec = BuildEcInfo(erasureStaging, disk.PositionInSet),
            };
            XlMetaFile.Write(stagingDir, diskRecord);
            erasureStaging.DiskRecords[disk] = diskRecord;
        }
    }

    // Коммит: по-дисковые rename staging → bucket-дерево (схема 04 §4 на каждом
    // диске независимо), успехов ≥ writeQuorum; меньше — откат staging в .trash/
    // на всех дисках + InsufficientWriteQuorum.
    public PutResult Commit(string bucket, string key, XlMetaRecord record, IXlStaging staging)
    {
        var erasureStaging = RequireErasureStaging(staging);
        var k = _pool.Topology.SetSize - erasureStaging.Parity;
        var m = erasureStaging.Parity;
        var writeQuorum = ErasureQuorum.Write(k, m);

        lock (_commitLock)
        {
            var successes = new List<ErasureDisk>();
            var oldDataDirs = new Dictionary<ErasureDisk, string?>();
            foreach (var (disk, stagingDir) in erasureStaging.StagingDirs)
            {
                try
                {
                    oldDataDirs[disk] = CommitOnDisk(disk, bucket, key, erasureStaging, stagingDir);
                    successes.Add(disk);
                }
                catch (Exception ex)
                {
                    MarkOfflineIfIo(disk, ex);
                    _logger.LogWarning(ex, "EC: коммит на диске {Position} ({Root}) не прошёл", disk.Position, disk.Root);
                }
            }

            if (successes.Count < writeQuorum)
            {
                // откат: staging → .trash/ на всех дисках
                foreach (var (disk, stagingDir) in erasureStaging.StagingDirs)
                    if (Directory.Exists(stagingDir))
                        MoveToTrash(disk, stagingDir);
                throw new ObjectStoreException(ObjectStoreErrorCode.InsufficientWriteQuorum);
            }

            // кворум достигнут: старые dataDir успешных дисков → .trash/ (после коммита)
            foreach (var disk in successes)
            {
                if (oldDataDirs.TryGetValue(disk, out var old) && old is not null)
                {
                    var oldPath = Path.Combine(ObjectDirOnDisk(disk, bucket, key), old);
                    if (Directory.Exists(oldPath))
                        MoveToTrash(disk, oldPath);
                }
            }

            return new PutResult('"' + record.ETag + '"', record.ModTime);
        }
    }

    // Схема 04 §4 на одном диске; возвращает dataDir ПРЕДЫДУЩЕЙ записи (для trash
    // ПОСЛЕ кворума). Новый ключ — один rename; перезапись — dataDir + xl.meta.
    private string? CommitOnDisk(ErasureDisk disk, string bucket, string key, ErasureStaging staging, string stagingDir)
    {
        var target = ObjectDirOnDisk(disk, bucket, key);
        var newDataDir = Path.Combine(stagingDir, staging.VersionId.ToString("N"));
        if (!Directory.Exists(target))
        {
            Directory.CreateDirectory(Path.GetDirectoryName(target)!);
            Directory.Move(stagingDir, target); // единый rename = коммит диска
            return null;
        }

        // перезапись: старый dataDir читается ДО коммита; новый dataDir рядом →
        // замена xl.meta (КОММИТ диска) → старый в .trash строго после кворума
        string? oldDataDir = null;
        try
        {
            oldDataDir = XlMetaFile.Read(target, out _).DataDirName;
        }
        catch (FileNotFoundException)
        {
            // записи нет — нет старого dataDir
        }

        Directory.Move(newDataDir, Path.Combine(target, staging.VersionId.ToString("N")));
        var recordDisk = staging.DiskRecords.TryGetValue(disk, out var cached)
            ? cached
            : throw new InvalidOperationException("xl.meta не записан в staging (WriteMeta)");
        XlMetaFile.Write(target, recordDisk);
        return oldDataDir == staging.VersionId.ToString("N") ? null : oldDataDir;
    }

    // ── заглушки зон, реализуемых Task 9–12 ──

    // Чистки per-disk (канон 06 §10): на каждом онлайн-диске — возрастные
    // .trash/orphan (1 ч), брошенные загрузки (24 ч), фантом-записи (локальная
    // кворумная сверка отсутствия с онлайн-корнями сета).
    public Task RunCleanupAsync(CancellationToken ct)
    {
        foreach (var set in _pool.Sets)
        {
            var peerRoots = set.Where(d => d.Online).Select(d => d.Root).ToList();
            var k = _pool.Topology.SetSize - _defaultParity;
            foreach (var disk in set.Where(d => d.Online))
            {
                var janitor = new XlDiskJanitor(disk.Root, _timeProvider, _logger);
                janitor.CleanAged();
                janitor.CleanupAbandonedUploads();
                janitor.CleanPhantoms(peerRoots, k);
            }
        }

        return Task.CompletedTask;
    }

    // Формат пула валиден + touch-проба записи на каждом онлайн-диске (канон
    // 05 §5/спека §4.3 — как single XlVolume.CheckHealth); деградация НЕ роняет
    // healthz — она наблюдается в /admin/storage.
    public bool CheckHealth()
    {
        var online = _pool.Disks.Where(d => d.Online).ToList();
        if (online.Count == 0)
            return false;
        foreach (var disk in online)
        {
            try
            {
                ErasureVolumeInfo.Read(disk.Root);
            }
            catch (Exception)
            {
                return false;
            }

            if (!_pool.ProbeDiskAlive(disk))
                return false; // read-only/умерший диск — том не доступен на запись
        }

        return true;
    }

    // ── бакеты: на дисках ВСЕХ сетов (канон 06 §8) ──

    public bool BucketExists(string bucket)
    {
        if (bucket == ".owns3.sys")
            return false;
        // Бакет живёт на всех сетах: мнение любого сета с read-кворумом
        // достаточно. НЕТ сетов с read-кворумом → кворумная недоступность
        // (503 InsufficientReadQuorum, не «бакета нет» — канон 06 §7)
        var threshold = ErasureQuorum.Read(
            _pool.Topology.SetSize - _defaultParity, _defaultParity);
        if (_pool.Sets.All(set => set.Count(d => d.Online) < threshold))
            throw new ObjectStoreException(ObjectStoreErrorCode.InsufficientReadQuorum);
        return _pool.Sets
            .Where(set => set.Count(d => d.Online) >= threshold)
            .SelectMany(set => set)
            .Any(disk => Directory.Exists(Path.Combine(disk.Root, bucket)));
    }

    public void CreateBucket(string bucket)
    {
        // каталоги + bucket.json на дисках всех сетов, кворум ≥ writeQuorum каждого сета
        foreach (var set in _pool.Sets)
        {
            var successes = 0;
            var writeQuorum = ErasureQuorum.Write(_pool.Topology.SetSize - _defaultParity, _defaultParity);
            foreach (var disk in set)
            {
                if (!disk.Online)
                    continue;
                try
                {
                    Directory.CreateDirectory(Path.Combine(disk.Root, bucket));
                    var metaDir = Path.Combine(disk.Root, ".owns3.sys", "buckets", bucket);
                    Directory.CreateDirectory(metaDir);
                    File.WriteAllText(Path.Combine(metaDir, "bucket.json"),
                        "{\"createdAt\":" + _timeProvider.GetUtcNow().ToUnixTimeMilliseconds() + "}");
                    successes++;
                }
                catch (Exception ex)
                {
                    MarkOfflineIfIo(disk, ex);
                    _logger.LogWarning(ex, "EC: CreateBucket на диске {Position} не прошёл", disk.Position);
                }
            }

            if (successes < writeQuorum)
                throw new ObjectStoreException(ObjectStoreErrorCode.InsufficientWriteQuorum);
        }
    }

    public void DeleteBucket(string bucket)
    {
        // emptiness по всем сетам (объекты/живые загрузки), trash per-disk
        foreach (var set in _pool.Sets)
        {
            foreach (var disk in set.Where(d => d.Online))
            {
                var bucketRoot = Path.Combine(disk.Root, bucket);
                if (!Directory.Exists(bucketRoot))
                    continue;
                if (Directory.EnumerateFiles(bucketRoot, "xl.meta", SearchOption.AllDirectories).Any())
                    throw new ObjectStoreException(ObjectStoreErrorCode.BucketNotEmpty);
            }
        }

        // Живые multipart-загрузки бакета блокируют удаление (канон 02 §1/§5):
        // активная запись = запись с существующим каталогом загрузки (М2);
        // журналы реплицированы на диски сета — ListUploadKeyDirs отдаёт один
        // keyDir на ключ
        foreach (var keyDirPath in ListUploadKeyDirs())
        {
            List<MultipartJournals.UploadJournalEntry> uploads;
            try
            {
                uploads = ReadUploadsByKeyDir(keyDirPath);
            }
            catch (JsonException ex)
            {
                _logger.LogWarning(ex, "Битый uploads.json в {KeyDir} при DeleteBucket: пропущен", keyDirPath);
                continue;
            }
            catch (FileNotFoundException)
            {
                continue; // keyDir есть, журнала нет — загрузок этого ключа нет
            }

            if (uploads.Any(e => e.Bucket == bucket && UploadDirExists(e.Bucket, e.Key, e.UploadId)))
                throw new ObjectStoreException(ObjectStoreErrorCode.BucketNotEmpty);
        }

        // Кворум ≥ writeQuorum КАЖДОГО сета (канон 06 §8 — как CreateBucket):
        // трэш только по онлайн-дискам без кворума оставил бы фантомные
        // бакеты/ключи на offline-дисках — воскреснут при возврате, их не
        // вычищают ни heal (только живое-в-консенсусе), ни чистки (порог)
        var deleteQuorum = ErasureQuorum.Write(
            _pool.Topology.SetSize - _defaultParity, _defaultParity);
        foreach (var set in _pool.Sets)
        {
            if (set.Count(d => d.Online) < deleteQuorum)
                throw new ObjectStoreException(ObjectStoreErrorCode.InsufficientWriteQuorum);
        }

        foreach (var disk in _pool.Disks.Where(d => d.Online))
        {
            var bucketRoot = Path.Combine(disk.Root, bucket);
            if (Directory.Exists(bucketRoot))
                MoveToTrash(disk, bucketRoot);
            var metaDir = Path.Combine(disk.Root, ".owns3.sys", "buckets", bucket);
            if (Directory.Exists(metaDir))
                MoveToTrash(disk, metaDir);
        }
    }

    public IReadOnlyList<BucketEntry> ListBuckets()
    {
        // консенсус по любому сету с read-кворумом (канон 06 §8); все сеты < k → 503
        var threshold = _pool.Topology.SetSize - _defaultParity;
        foreach (var set in _pool.Sets)
        {
            var online = set.Where(d => d.Online).ToList();
            if (online.Count < threshold)
                continue;
            var names = online.SelectMany(disk => Directory.EnumerateDirectories(disk.Root))
                .Select(Path.GetFileName)
                .Where(name => name is not null && name != ".owns3.sys")
                .Select(name => name!)
                .Distinct()
                .ToList();
            names.Sort(Utf8ByteOrder.Compare);
            return names.Select(TryReadBucketCreation).ToList();
        }

        throw new ObjectStoreException(ObjectStoreErrorCode.InsufficientReadQuorum);
    }

    // CreationDate из bucket.json первого успешного чтения (файла нет → epoch).
    private BucketEntry TryReadBucketCreation(string name)
    {
        foreach (var disk in _pool.Disks.Where(d => d.Online))
        {
            var path = Path.Combine(disk.Root, ".owns3.sys", "buckets", name, "bucket.json");
            if (!File.Exists(path))
                continue;
            try
            {
                using var doc = System.Text.Json.JsonDocument.Parse(File.ReadAllText(path));
                var created = doc.RootElement.TryGetProperty("createdAt", out var createdAt)
                    && createdAt.TryGetInt64(out var ms)
                    ? DateTimeOffset.FromUnixTimeMilliseconds(ms)
                    : DateTimeOffset.FromUnixTimeMilliseconds(0);
                return new BucketEntry(name, created);
            }
            catch (System.Text.Json.JsonException)
            {
                // битая мета — не роняет листинг
            }
        }

        return new BucketEntry(name, DateTimeOffset.FromUnixTimeMilliseconds(0));
    }
    public XlMetaRecord? TryReadMeta(string bucket, string key)
    {
        var set = _pool.Sets[_pool.PickSetIndex(bucket, key)];
        var online = set.Where(d => d.Online).ToList();
        // порог консенсуса — k дефолтной parity (per-object parity неизвестна до
        // чтения; больший порог консервативно-безопасен, канон 06 §7)
        var threshold = _pool.Topology.SetSize - _defaultParity;
        if (online.Count < threshold)
            throw new ObjectStoreException(ObjectStoreErrorCode.InsufficientReadQuorum);

        // консенсус: мажоритарный versionId (порог ≥ k); битые записи не считаются
        var records = new List<XlMetaRecord>();
        foreach (var disk in online)
        {
            try
            {
                records.Add(XlMetaFile.Read(ObjectDirOnDisk(disk, bucket, key), out _));
            }
            catch (FileNotFoundException)
            {
                // записи на этом диске нет
            }
            catch (XlIntegrityException ex)
            {
                _logger.LogWarning(ex, "EC: битый xl.meta {Bucket}/{Key} на диске {Position} — запись не в консенсусе",
                    bucket, key, disk.Position);
            }
        }

        if (records.Count == 0)
            return null; // объекта нет при кворумном онлайне

        var majority = records.GroupBy(r => r.VersionId)
            .OrderByDescending(g => g.Count())
            .First();
        if (majority.Count() < threshold)
            throw new ObjectStoreException(ObjectStoreErrorCode.InsufficientReadQuorum);
        return majority.First();
    }
    // Copy: перенос частей источника в staging цели (канон 06 §8). Шардовые
    // part.N копируются побайтово с позиций dist сета источника на те же
    // позиции сета цели (байты шарда от сета не зависят) — bitrot-набор
    // наследуется из записи источника. Раскладка несовместима (parity/размер
    // сета) или шард-источник недоступен (офлайн/файла нет) — часть
    // перекодируется: реконструкция исходного потока и шардирование с
    // раскладкой staging.
    public void CopyPartsToStaging(IXlStaging staging, string srcBucket, string srcKey, XlMetaRecord srcMeta)
    {
        var erasureStaging = RequireErasureStaging(staging);
        ArgumentNullException.ThrowIfNull(srcMeta.Ec, nameof(srcMeta));
        var srcEc = srcMeta.Ec;
        var srcSet = _pool.Sets[_pool.PickSetIndex(srcBucket, srcKey)];
        var dataDir = erasureStaging.VersionId.ToString("N");
        var sameLayout = srcEc.EcN == _pool.Topology.SetSize
            && srcEc.EcN - srcEc.EcM == _pool.Topology.SetSize - erasureStaging.Parity
            && srcEc.EcDist.Select(b => (int)b).SequenceEqual(erasureStaging.Distribution);

        var partOffset = 0L;
        for (var i = 0; i < srcEc.Parts.Count; i++)
        {
            var bitrot = srcEc.Parts[i];
            var partNumber = i + 1;
            var transferred = false;
            if (sameLayout)
            {
                transferred = true;
                for (var j = 0; j < srcEc.EcN; j++)
                {
                    var position = srcEc.EcDist[j];
                    var srcDisk = srcSet.FirstOrDefault(d => d.Online && d.PositionInSet == position);
                    var dstDisk = erasureStaging.SetDisks.FirstOrDefault(d => d.Online && d.PositionInSet == position);
                    if (dstDisk is null)
                        continue; // цель-позиция офлайн: шард не переносится (как при записи)
                    if (srcDisk is null)
                    {
                        transferred = false; // позиции нет среди онлайн — реконструкция
                        break;
                    }
                    var srcPath = Path.Combine(ObjectDirOnDisk(srcDisk, srcBucket, srcKey),
                        srcMeta.DataDirName, $"part.{partNumber}");
                    if (!File.Exists(srcPath))
                    {
                        transferred = false; // шард недоступен (деградация записи) — реконструкция
                        break;
                    }
                    var dstPath = Path.Combine(erasureStaging.StagingDirs[dstDisk], dataDir, $"part.{partNumber}");
                    Directory.CreateDirectory(Path.GetDirectoryName(dstPath)!);
                    File.Copy(srcPath, dstPath, overwrite: true);
                }

                if (transferred)
                    erasureStaging.PartResults[partNumber] = new ErasurePartResult(
                        string.Empty, string.Empty, bitrot.SourceSize,
                        bitrot.BlockHashes.Length > 0 ? bitrot.BlockHashes[0].Length : 0,
                        bitrot.BlockHashes);
            }

            if (!transferred)
            {
                // перекодирование: реконструированный поток части → шардирование staging
                using var decoded = OpenData(srcBucket, srcKey, srcMeta, partOffset, bitrot.SourceSize);
                WriteObjectPartAsync(erasureStaging, partNumber, decoded, bitrot.SourceSize, CancellationToken.None)
                    .ConfigureAwait(false).GetAwaiter().GetResult();
            }

            partOffset += bitrot.SourceSize;
        }
    }
    public Stream OpenData(string bucket, string key, XlMetaRecord meta, long offset, long length)
    {
        ArgumentNullException.ThrowIfNull(meta.Ec, nameof(meta));

        var ec = meta.Ec;
        var codec = CodecFor(ec.EcN - ec.EcM);
        var set = _pool.Sets[_pool.PickSetIndex(bucket, key)];
        var windows = new List<ErasureBlockReader>();
        var shardStreams = new List<Stream>(); // владение — ErasureObjectStream
        var windowStart = offset;
        var windowEnd = offset + length;
        var partStart = 0L;
        Stream?[] currentReaders = [];
        try
        {
            for (var i = 0; i < ec.Parts.Count && partStart < windowEnd; i++)
            {
                var part = ec.Parts[i];
                var partEnd = partStart + part.SourceSize;
                if (partEnd > windowStart)
                {
                    var localOffset = Math.Max(0, windowStart - partStart);
                    var localEnd = Math.Min(partEnd, windowEnd);
                    var result = new ErasurePartResult(string.Empty, string.Empty, part.SourceSize,
                        part.BlockHashes.Length > 0 ? part.BlockHashes[0].Length : 0, part.BlockHashes);
                    currentReaders = OpenShardStreams(set, bucket, key, meta, i + 1, ec);

                    // Порог ДО декодирования — по per-object parity записи (TryReadMeta
                    // консервативен по default k): живых источников части < k объекта →
                    // кворумная недоступность 503, не 500 InternalError от ридера
                    if (currentReaders.Count(r => r is not null) < ec.EcM)
                        throw new ObjectStoreException(ObjectStoreErrorCode.InsufficientReadQuorum);

                    shardStreams.AddRange(currentReaders.Where(r => r is not null)!);
                    windows.Add(new ErasureBlockReader(codec, currentReaders, result,
                        localOffset, localEnd - partStart - localOffset));
                    currentReaders = [];
                }

                partStart = partEnd;
            }
        }
        catch
        {
            // 503-путь (и любой сбой сборки окна): уже открытые шард-потоки этой и
            // предыдущих частей никто не закроет — поток-обёртка не создана
            foreach (var stream in currentReaders)
                stream?.Dispose();
            foreach (var stream in shardStreams)
                stream.Dispose();
            throw;
        }

        return new ErasureObjectStream(windows, shardStreams);
    }

    // Шард-потоки части N по позициям EcDist записи: поток j — с диска
    // distribution[j] (null — диск офлайн / файла нет).
    private static Stream?[] OpenShardStreams(IReadOnlyList<ErasureDisk> set, string bucket, string key,
        XlMetaRecord meta, int partNumber, XlMetaEcInfo ec)
    {
        var readers = new Stream?[ec.EcN];
        for (var j = 0; j < ec.EcN; j++)
        {
            var disk = set.FirstOrDefault(d => d.Online && d.PositionInSet == ec.EcDist[j]);
            if (disk is null)
                continue;
            var path = Path.Combine(ObjectDirOnDisk(disk, bucket, key), meta.DataDirName, $"part.{partNumber}");
            if (File.Exists(path))
                readers[j] = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read);
        }

        return readers;
    }
    public void VerifyFullChecksum(string bucket, string key, XlMetaRecord meta)
    {
        string actual;
        using (var sha = System.Security.Cryptography.IncrementalHash
                   .CreateHash(System.Security.Cryptography.HashAlgorithmName.SHA256))
        {
            using var body = OpenData(bucket, key, meta, 0, meta.Size);
            var buffer = new byte[64 * 1024];
            int read;
            while ((read = body.Read(buffer, 0, buffer.Length)) > 0)
                sha.AppendData(buffer, 0, read);
            actual = Convert.ToHexString(sha.GetHashAndReset()).ToLowerInvariant();
        }

        if (actual != meta.ContentSha256)
            throw new XlIntegrityException(
                $"checksum mismatch: объект {meta.VersionId:N}, ожидается {meta.ContentSha256}, факт {actual} (реконструкция)");
    }
    // Delete: трэш ФАЙЛОВ записи (мета+данные) в .trash/ на каждом диске сета
    // (кворум ≥ writeQuorum; недоступные диски выравниваются при возврате: живое-
    // в-консенсусе доливается heal; фантом-записи чистятся по порогу — канон 06 §8).
    public void Delete(string bucket, string key, XlMetaRecord meta)
    {
        var set = _pool.Sets[_pool.PickSetIndex(bucket, key)];
        var k = meta.Ec?.EcM ?? (byte)(_pool.Topology.SetSize - _defaultParity);
        var m = meta.Ec is { } ec ? (byte)(ec.EcN - ec.EcM) : (byte)_defaultParity;
        var writeQuorum = ErasureQuorum.Write(k, m);
        var successes = 0;
        foreach (var disk in set)
        {
            if (!disk.Online)
                continue;
            var target = ObjectDirOnDisk(disk, bucket, key);
            if (!Directory.Exists(target))
                continue; // записи нет (деградированная запись/фантом — heal/чистки)
            try
            {
                // семантика single (04 §4 п.5): трэшатся файлы записи, а не весь
                // объектный каталог — вложенные объекты («a/b» внутри «a»)
                // не затрагиваются; каталог удаляется только без записей в поддереве
                var metaPath = Path.Combine(target, "xl.meta");
                if (File.Exists(metaPath))
                    MoveToTrash(disk, metaPath);
                var bkpPath = Path.Combine(target, "xl.meta.bkp");
                if (File.Exists(bkpPath))
                    MoveToTrash(disk, bkpPath);
                var dataDirPath = Path.Combine(target, meta.DataDirName);
                if (Directory.Exists(dataDirPath))
                    MoveToTrash(disk, dataDirPath);
                if (!Directory.EnumerateFiles(target, "xl.meta", SearchOption.AllDirectories).Any())
                    Directory.Delete(target, recursive: true); // записей в поддереве нет
                successes++;
            }
            catch (Exception ex)
            {
                MarkOfflineIfIo(disk, ex);
                _logger.LogWarning(ex, "EC: Delete на диске {Position} не прошёл", disk.Position);
            }
        }

        if (successes < writeQuorum)
            throw new ObjectStoreException(ObjectStoreErrorCode.InsufficientWriteQuorum);
    }
    public IReadOnlyList<(int Number, long Size)> EnumerateParts(string bucket, string key, XlMetaRecord meta) =>
        meta.Ec?.Parts.Select((part, index) => (index + 1, part.SourceSize)).ToList()
        ?? throw new XlIntegrityException($"запись {bucket}/{key} без EC-полей в EC-томе");
    // Листинг объектов: предусловие — во ВСЕХ сетах онлайн ≥ k (канон 06 §7:
    // неполный листинг опаснее недоступности); merge DirectoryCursor-курсоров
    // корней бакета онлайн-дисков всех сетов (куча по PeekKey), дедуп по ключу,
    // мета — с первого диска нахождения (битая — следующий диск сета).
    public IXlObjectCursor EnumerateObjects(string bucket)
    {
        var threshold = _pool.Topology.SetSize - _defaultParity;
        foreach (var set in _pool.Sets)
        {
            if (set.Count(d => d.Online) < threshold)
                throw new ObjectStoreException(ObjectStoreErrorCode.InsufficientReadQuorum);
        }

        return new ErasureObjectCursor(_pool, bucket);
    }

    // Курсор ключей бакета поверх всех сетов: k-way merge по PeekKey (Utf8ByteOrder),
    // дедуп — по продвижению всех курсоров с равным ключом.
    private sealed class ErasureObjectCursor(ErasurePool pool, string bucket) : IXlObjectCursor
    {
        private readonly List<(ErasureDisk Disk, DirectoryCursor Cursor, string ObjectRoot)> _cursors = pool.Disks
            .Where(d => d.Online)
            .Select(disk => (disk,
                new DirectoryCursor(Path.Combine(disk.Root, bucket), ownKey: null, prefix: string.Empty, after: null),
                Path.Combine(disk.Root, bucket)))
            .ToList();

        private static readonly IComparer<string> KeyComparer =
            Comparer<string>.Create(Uf8Compare);

        private static int Uf8Compare(string left, string right) => Utf8ByteOrder.Compare(left, right);

        public string? PeekKey
        {
            get
            {
                string? best = null;
                foreach (var (_, cursor, _) in _cursors)
                {
                    var key = cursor.PeekKey;
                    if (key is null)
                        continue;
                    if (best is null || KeyComparer.Compare(key, best) < 0)
                        best = key;
                }

                return best;
            }
        }

        public void MoveNext()
        {
            var key = PeekKey;
            if (key is null)
                return;
            // продвижение ВСЕХ курсоров с этим ключом — дедуп
            foreach (var (_, cursor, _) in _cursors)
            {
                if (cursor.PeekKey == key)
                    cursor.MoveNext();
            }
        }

        public XlMetaRecord ReadCurrentMeta()
        {
            var key = PeekKey
                ?? throw new InvalidOperationException("курсор исчерпан — ReadCurrentMeta без ключа");
            // мета с первого диска нахождения; битая — пробуется следующий
            foreach (var (_, cursor, root) in _cursors)
            {
                if (cursor.PeekKey != key)
                    continue;
                var objectDir = Path.Combine([root, .. XlPathEncoder.EncodePath(key).Split('/')]);
                try
                {
                    return XlMetaFile.Read(objectDir, out _);
                }
                catch (XlIntegrityException)
                {
                    // битая копия — следующий диск сета
                }
            }

            throw new XlIntegrityException($"EC: все копии xl.meta ключа {key} битые");
        }
    }
    // ── multipart: журналы на дисках сета ключа (канон 06 §8) ──
    // Журналы пишутся tmp+rename на все онлайн-диски сета, читаются с любого
    // успешного; рассинхрон после отказов выравнивается heal (Task 13).

    private IReadOnlyList<ErasureDisk> SetOf(string bucket, string key) =>
        _pool.Sets[_pool.PickSetIndex(bucket, key)];

    private string UploadDirOnDisk(ErasureDisk disk, string bucket, string key, string uploadId) =>
        Path.Combine(disk.Root, ".owns3.sys", "multipart",
            MultipartJournals.KeyDir(string.Empty, bucket, key).Trim('/'), uploadId);

    private static string KeyDirName(string bucket, string key) =>
        MultipartJournals.KeyDir(string.Empty, bucket, key).Trim('/');

    public List<MultipartJournals.UploadJournalEntry> ReadUploads(string bucket, string key)
    {
        // Журнал реплицирован на диски сета, но НЕДО-реплицирован при отказах:
        // ReadList отсутствие файла трактует как [] — первый опрошенный диск
        // без uploads.json НЕ означает «загрузок нет». Отсутствие файла — «нет
        // мнения у диска»: опрашиваем ВСЕ онлайн-диски и сливаем носители
        // (union по uploadId, содержимое записи неизменно после создания) —
        // пустой журнал не затирает непустые реплики, Create поверх частично
        // реплицированного журнала не теряет записи. Фантомная запись (Abort/
        // Complete при offline-диске) отсеивается uploadDir-гейтом операций и
        // выравнивается heal (свежий reference по mtime).
        var merged = new Dictionary<string, MultipartJournals.UploadJournalEntry>();
        var opinion = false;
        foreach (var disk in SetOf(bucket, key).Where(d => d.Online))
        {
            var path = MultipartJournals.UploadsJsonPath(
                Path.GetDirectoryName(UploadDirOnDisk(disk, bucket, key, "x"))!);
            try
            {
                if (!File.Exists(path))
                {
                    opinion = true; // достижимый диск без файла: «мнение: пусто» —
                    continue;        // итог решают ВСЕ носители, не первый опрошенный
                }

                foreach (var entry in MultipartJournals.ReadUploads(path))
                    merged.TryAdd(entry.UploadId, entry);
                opinion = true;
            }
            catch (JsonException ex)
            {
                _logger.LogWarning(ex, "EC: битый uploads.json на диске {Position} ({Path})", disk.Position, path);
            }
            catch (IOException)
            {
                // следующий диск
            }
        }

        if (opinion)
            return [.. merged.Values];
        throw new ObjectStoreException(ObjectStoreErrorCode.InsufficientReadQuorum);
    }

    public void WriteUploads(string bucket, string key, List<MultipartJournals.UploadJournalEntry> uploads)
    {
        lock (JournalLock)
        {
            foreach (var disk in SetOf(bucket, key).Where(d => d.Online))
            {
                var keyDir = Path.Combine(disk.Root, ".owns3.sys", "multipart", KeyDirName(bucket, key));
                Directory.CreateDirectory(keyDir);
                MultipartJournals.WriteUploads(MultipartJournals.UploadsJsonPath(keyDir), uploads);
            }
        }
    }

    public IReadOnlyList<string> ListUploadKeyDirs()
    {
        // один keyDir на ключ: журналы реплицированы на диски сета — ключ
        // представляем первым онлайн-диском, где каталог существует (дедуп по
        // имени keyDir; пути дисков различны корнями — путь не дедуплит)
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var result = new List<string>();
        foreach (var disk in _pool.Disks.Where(d => d.Online))
        {
            var multipartRoot = Path.Combine(disk.Root, ".owns3.sys", "multipart");
            if (!Directory.Exists(multipartRoot))
                continue;
            foreach (var dir in Directory.EnumerateDirectories(multipartRoot))
            {
                if (seen.Add(Path.GetFileName(dir)))
                    result.Add(dir);
            }
        }
        return result;
    }

    public List<MultipartJournals.UploadJournalEntry> ReadUploadsByKeyDir(string keyDirPath)
    {
        // keyDir мог доехать не до всех носителей (I/O-сбой WriteUploads после
        // CreateDirectory): журнал читаем с первого носителя, где файл есть и
        // валиден, — иначе первый диск без uploads.json скрывал живые загрузки
        // реплик (гейт DeleteBucket пропускал загрузку, листинги пустели)
        var keyDirName = Path.GetFileName(keyDirPath);
        foreach (var disk in _pool.Disks.Where(d => d.Online))
        {
            var jsonPath = MultipartJournals.UploadsJsonPath(
                Path.Combine(disk.Root, ".owns3.sys", "multipart", keyDirName));
            if (!File.Exists(jsonPath))
                continue;
            try
            {
                return MultipartJournals.ReadUploads(jsonPath);
            }
            catch (JsonException)
            {
                // битая реплика — следующая может быть валидной
            }
        }
        return [];
    }

    public bool UploadDirExists(string bucket, string key, string uploadId) =>
        SetOf(bucket, key).Any(disk => disk.Online
            && Directory.Exists(UploadDirOnDisk(disk, bucket, key, uploadId)));

    public void CreateUploadDir(string bucket, string key, string uploadId, XlMetaRecord uploadRecord)
    {
        // xl.meta загрузки — v1 (не объект); каталоги на всех онлайн-дисках сета
        lock (JournalLock)
        {
            foreach (var disk in SetOf(bucket, key).Where(d => d.Online))
            {
                var uploadDir = UploadDirOnDisk(disk, bucket, key, uploadId);
                Directory.CreateDirectory(uploadDir);
                XlMetaFile.Write(uploadDir, uploadRecord);
            }
        }
    }

    public XlMetaRecord ReadUploadMeta(string bucket, string key, string uploadId)
    {
        foreach (var disk in SetOf(bucket, key).Where(d => d.Online))
        {
            var uploadDir = UploadDirOnDisk(disk, bucket, key, uploadId);
            if (Directory.Exists(uploadDir))
                return XlMetaFile.Read(uploadDir, out _);
        }

        throw new ObjectStoreException(ObjectStoreErrorCode.NoSuchUpload);
    }

    public void TrashUpload(string bucket, string key, string uploadId)
    {
        lock (JournalLock)
        {
            foreach (var disk in SetOf(bucket, key).Where(d => d.Online))
            {
                var uploadDir = UploadDirOnDisk(disk, bucket, key, uploadId);
                if (Directory.Exists(uploadDir))
                    MoveToTrash(disk, uploadDir);
            }
        }
    }

    public List<MultipartJournals.PartJournalEntry> ReadParts(string bucket, string key, string uploadId)
    {
        foreach (var disk in SetOf(bucket, key).Where(d => d.Online))
        {
            var uploadDir = UploadDirOnDisk(disk, bucket, key, uploadId);
            if (Directory.Exists(uploadDir))
                return MultipartJournals.ReadParts(MultipartJournals.PartsJsonPath(uploadDir));
        }

        throw new ObjectStoreException(ObjectStoreErrorCode.NoSuchUpload);
    }

    // Staging части ВНУТРИ загрузки: шардирование пишет part.N прямо в
    // каталоги загрузки на дисках сета (tmp не нужен — UploadPart-файл не виден
    // до parts.json), PartResults копятся для Complete.
    private sealed class ErasureUploadStaging : IXlStaging
    {
        public required string Bucket { get; init; }
        public required string Key { get; init; }
        public required string UploadId { get; init; }
        public required Dictionary<int, ErasurePartResult> PartResults { get; init; }
        public required int Parity { get; init; }
        public required IReadOnlyList<ErasureDisk> SetDisks { get; init; }
        public required int[] Distribution { get; init; }

        public void Dispose()
        {
            // части загрузки остаются в каталоге загрузки — их чистит Abort/чистки
        }
    }

    public IXlStaging CreateUploadStaging(string bucket, string key, string uploadId) =>
        new ErasureUploadStaging
        {
            Bucket = bucket,
            Key = key,
            UploadId = uploadId,
            PartResults = new Dictionary<int, ErasurePartResult>(),
            Parity = _defaultParity,
            SetDisks = SetOf(bucket, key),
            Distribution = ReadDistribution(SetOf(bucket, key)),
        };

    public async Task<PartDigest> WriteUploadPartAsync(IXlStaging staging, int partNumber, Stream body,
        long contentLength, CancellationToken ct)
    {
        // тот же путь шардирования, но потоки — сразу в каталоги загрузки
        var upload = staging as ErasureUploadStaging
            ?? throw new ArgumentException("требуется EC upload-staging", nameof(staging));

        // Повторная заливка части: недо-переносы прошлых попыток Complete (part.N
        // в attempt-/orphan-dataDir объекта — каталогах без упоминания в живой
        // xl.meta) обязаны уйти — иначе повторный Complete скипнет устаревший
        // dest при НОВОМ sidecar: SHA-проход упадёт или закоммитится битое
        foreach (var disk in upload.SetDisks.Where(d => d.Online))
        {
            var objectDir = ObjectDirOnDisk(disk, upload.Bucket, upload.Key);
            if (!Directory.Exists(objectDir))
                continue;
            string? liveDataDir = null;
            try
            {
                if (File.Exists(Path.Combine(objectDir, "xl.meta")))
                    liveDataDir = XlMetaFile.Read(objectDir, out _).DataDirName;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "EC: чтение xl.meta {Bucket}/{Key} на диске {Position} при перезаливке части",
                    upload.Bucket, upload.Key, disk.Position);
            }

            foreach (var dataDir in Directory.EnumerateDirectories(objectDir))
            {
                if (Path.GetFileName(dataDir) == liveDataDir)
                    continue; // живая версия объекта — не трогаем
                try
                {
                    var stale = Path.Combine(dataDir, $"part.{partNumber}");
                    if (File.Exists(stale))
                        File.Delete(stale);
                }
                catch (Exception ex)
                {
                    MarkOfflineIfIo(disk, ex);
                    _logger.LogWarning(ex, "EC: удаление устаревшего переноса части на диске {Position} не прошло",
                        disk.Position);
                }
            }
        }

        var codec = CodecFor(upload.Parity);
        var online = upload.SetDisks.Where(d => d.Online).ToList();
        var streams = new Stream[codec.TotalShards];
        try
        {
            for (var j = 0; j < codec.TotalShards; j++)
            {
                var disk = online.FirstOrDefault(d => d.PositionInSet == upload.Distribution[j]);
                if (disk is null)
                {
                    streams[j] = new MemoryStream();
                    continue;
                }
                // фантомная загрузка (Abort при offline-диске + возврат): каталог
                // загрузки жив не на всех носителях — FileStream без каталога даёт
                // необработанный DirectoryNotFoundException (500); пересоздание
                // идемпотентно, итог решает кворум CommitUploadPart
                var uploadDir = UploadDirOnDisk(disk, upload.Bucket, upload.Key, upload.UploadId);
                Directory.CreateDirectory(uploadDir);
                streams[j] = new FileStream(Path.Combine(uploadDir, $"part.{partNumber}"),
                    FileMode.Create, FileAccess.Write, FileShare.None);
            }

            using var writer = new ErasureBlockWriter(codec, streams);
            await writer.WriteAsync(body, contentLength, ct);
            var result = await writer.CompleteAsync();
            upload.PartResults[partNumber] = result;
            return new PartDigest(result.ETagHex, result.Sha256Hex, result.SourceLength);
        }
        finally
        {
            foreach (var stream in streams)
                stream?.Dispose();
        }
    }

    public void CommitUploadPart(IXlStaging staging, int partNumber, string etagHex, long size, DateTimeOffset modTime)
    {
        var upload = staging as ErasureUploadStaging
            ?? throw new ArgumentException("требуется EC upload-staging", nameof(staging));
        var k = _pool.Topology.SetSize - upload.Parity;
        var writeQuorum = ErasureQuorum.Write(k, upload.Parity);
        var successes = 0;
        lock (JournalLock)
        {
            foreach (var disk in upload.SetDisks.Where(d => d.Online))
            {
                var uploadDir = UploadDirOnDisk(disk, upload.Bucket, upload.Key, upload.UploadId);
                if (!Directory.Exists(uploadDir))
                    continue;
                try
                {
                    var parts = MultipartJournals.ReadParts(MultipartJournals.PartsJsonPath(uploadDir));
                    parts.RemoveAll(p => p.PartNumber == partNumber);
                    parts.Add(new MultipartJournals.PartJournalEntry(partNumber, etagHex, size,
                        modTime.ToUnixTimeMilliseconds()));
                    parts.Sort((l, r) => l.PartNumber.CompareTo(r.PartNumber));
                    MultipartJournals.WriteParts(MultipartJournals.PartsJsonPath(uploadDir), parts);
                    WritePartBitrot(uploadDir, partNumber, upload.PartResults);
                    successes++;
                }
                catch (Exception ex)
                {
                    MarkOfflineIfIo(disk, ex);
                    _logger.LogWarning(ex, "EC: CommitUploadPart на диске {Position} не прошёл", disk.Position);
                }
            }
        }

        if (successes < writeQuorum)
            throw new ObjectStoreException(ObjectStoreErrorCode.InsufficientWriteQuorum);
    }

    // Complete: attempt.json → по-дисковые rename частей uploadDir/part.N →
    // dataDir/part.N → xl.meta v2 (per-disk EcIndex) → кворумный коммит → зачистка.
    // Composite ETag — из журнала; SHA-256 — проходом ErasureBlockReader по частям.
    public CompleteResult CommitMultipart(string bucket, string key, string uploadId,
        IReadOnlyList<PartEtag> manifest, XlMetaRecord newRecord)
    {
        lock (_commitLock)
        {
            var set = SetOf(bucket, key);
            // resolve под локом (повтор после зачистки — NoSuchUpload)
            var uploads = ReadUploads(bucket, key);
            if (uploads.All(e => e.UploadId != uploadId) || !UploadDirExists(bucket, key, uploadId))
                throw new ObjectStoreException(ObjectStoreErrorCode.NoSuchUpload);

            // attempt.json — dataDir текущей сборки (идемпотентность повтора, 04 §5)
            var online = set.Where(d => d.Online).ToList();
            var writeQuorum0 = ErasureQuorum.Write(_pool.Topology.SetSize - _defaultParity, _defaultParity);
            if (online.Count < writeQuorum0)
                throw new ObjectStoreException(ObjectStoreErrorCode.InsufficientWriteQuorum);
            // attempt — с ПЕРВОГО НОСИТЕЛЯ сета (частичный WriteAttempt при I/O-сбое:
            // повтор Complete с другим первым диском обязан получить ТОТ ЖЕ dataDir —
            // иначе перенесённые части не находятся и загрузка не Completable);
            // чтение+запись маркера — под JournalLock (heal-выравнивание attempt.json
            // не вклинивается между ними — сериализация журнальных операций)
            string attemptDataDir;
            string? attempted = null;
            lock (JournalLock)
            {
                foreach (var disk in online)
                {
                    try
                    {
                        var marker = MultipartJournals.ReadAttempt(UploadDirOnDisk(disk, bucket, key, uploadId));
                        if (marker is not null)
                        {
                            attempted = marker.DataDir;
                            break;
                        }
                    }
                    catch (IOException ex)
                    {
                        MarkOfflineIfIo(disk, ex);
                        _logger.LogWarning(ex, "EC: чтение attempt на диске {Position} не прошло", disk.Position);
                    }
                }

                var dataDirName = attempted is not null && Guid.TryParseExact(attempted, "N", out var parsed)
                    ? parsed.ToString("N")
                    : Guid.NewGuid().ToString("N");
                foreach (var disk in online)
                {
                    try
                    {
                        MultipartJournals.WriteAttempt(UploadDirOnDisk(disk, bucket, key, uploadId),
                            new MultipartJournals.AttemptMarker(dataDirName));
                    }
                    catch (Exception ex)
                    {
                        MarkOfflineIfIo(disk, ex);
                        _logger.LogWarning(ex, "EC: запись attempt на диске {Position} не прошла", disk.Position);
                    }
                }

                attemptDataDir = dataDirName;
            }

            // composite ETag из журнала
            var journal = ReadParts(bucket, key, uploadId);
            var journalByNumber = journal.ToDictionary(p => p.PartNumber);
            var concat = string.Concat(manifest.Select(p => journalByNumber[p.PartNumber].ETag));
            var compositeEtag = manifest.Count + "-" + Convert.ToHexString(
                System.Security.Cryptography.MD5.HashData(Encoding.ASCII.GetBytes(concat))).ToLowerInvariant();
            var size = manifest.Sum(p => journalByNumber[p.PartNumber].Size);

            // Перенос частей СРАЗУ в dataDir цели per-disk (модель single 04 §5):
            // персистентное место, не tmp-staging — сбой посреди Complete не теряет
            // перенесённое (повтор доезжает: dest существует → skip); uploadDir не
            // опустошается до кворумного коммита. Отсутствие шард-носителя части
            // на отдельном диске — деградация (кворум ниже), не фатально.
            var distribution = ReadDistribution(SetOf(bucket, key));
            var parity = _defaultParity;
            var codec = CodecFor(parity);
            foreach (var disk in online)
            {
                try
                {
                    var uploadDir = UploadDirOnDisk(disk, bucket, key, uploadId);
                    var dataDir = Path.Combine(ObjectDirOnDisk(disk, bucket, key), attemptDataDir);
                    Directory.CreateDirectory(dataDir); // создаёт и каталоги-префиксы ключа
                    foreach (var (number, _) in manifest)
                    {
                        var dest = Path.Combine(dataDir, $"part.{number}");
                        if (File.Exists(dest))
                            continue; // перенесено прошлой попыткой — идемпотентность
                        var src = Path.Combine(uploadDir, $"part.{number}");
                        if (!File.Exists(src))
                            continue; // шард этого диска отсутствует — кворум носителей ниже
                        File.Move(src, dest);
                    }
                }
                catch (Exception ex)
                {
                    MarkOfflineIfIo(disk, ex);
                    _logger.LogWarning(ex, "EC: перенос части Complete на диске {Position} не прошёл", disk.Position);
                }
            }

            // кворум шард-носителей каждой части (после переноса): меньше k —
            // InvalidPart (части сохранены в dataDir — повтор доедет при возврате
            // носителей/heal загрузок)
            var kShards = codec.DataShards;
            foreach (var (number, _) in manifest)
            {
                var carriers = online.Count(d => File.Exists(Path.Combine(
                    ObjectDirOnDisk(d, bucket, key), attemptDataDir, $"part.{number}")));
                if (carriers < kShards)
                    throw new ObjectStoreException(ObjectStoreErrorCode.InvalidPart);
            }

            // bitrot-хэши частей (sidecar остаётся в uploadDir до зачистки):
            // SHA-256 итога — проходом ErasureBlockReader по перенесённым частям
            string sha256Hex;
            var partResults = new Dictionary<int, ErasurePartResult>();
            using (var sha = System.Security.Cryptography.IncrementalHash
                       .CreateHash(System.Security.Cryptography.HashAlgorithmName.SHA256))
            {
                foreach (var (number, _) in manifest)
                {
                    var readers = new Stream?[codec.TotalShards];
                    try
                    {
                        for (var j = 0; j < codec.TotalShards; j++)
                        {
                            var disk = online
                                .FirstOrDefault(d => d.PositionInSet == distribution[j]);
                            if (disk is null)
                                continue;
                            var path = Path.Combine(ObjectDirOnDisk(disk, bucket, key), attemptDataDir, $"part.{number}");
                            if (File.Exists(path))
                                readers[j] = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read);
                        }

                        var hashes = ReadPartBitrot(
                            online.Select(d => UploadDirOnDisk(d, bucket, key, uploadId)).ToList(), number);
                        var partResult = new ErasurePartResult(string.Empty, string.Empty,
                            journalByNumber[number].Size,
                            hashes.Length > 0 ? hashes[0].Length : 0, hashes);
                        partResults[number] = partResult;
                        using var reader = new ErasureBlockReader(codec, readers, partResult, 0,
                            journalByNumber[number].Size);
                        var buffer = new byte[64 * 1024];
                        int read;
                        while ((read = reader.ReadAsync(buffer, 0, buffer.Length, CancellationToken.None)
                                   .GetAwaiter().GetResult()) > 0)
                            sha.AppendData(buffer, 0, read);
                    }
                    finally
                    {
                        foreach (var stream in readers)
                            stream?.Dispose();
                    }
                }

                sha256Hex = Convert.ToHexString(sha.GetHashAndReset()).ToLowerInvariant();
            }

            var record = newRecord with
            {
                VersionId = Guid.Parse(attemptDataDir), Size = size, ETag = compositeEtag,
                ContentSha256 = sha256Hex,
            };

            // Кворумный коммит: xl.meta v2 (свой EcIndex) tmp+rename ПРЯМО в
            // каталог объекта на каждом онлайн-диске (без tmp-staging — его
            // Dispose сносил бы перенесённые части при сбое); старый dataDir
            // прежней версии объекта — в .trash после записи новой меты
            var writeQuorum = ErasureQuorum.Write(kShards, parity);
            var committed = 0;
            foreach (var disk in online)
            {
                try
                {
                    var objectDir = ObjectDirOnDisk(disk, bucket, key);
                    var diskRecord = record with
                    {
                        Ec = BuildEcInfo(partResults, distribution, disk.PositionInSet, parity),
                    };
                    string? oldDataDir = null;
                    var oldMetaPath = Path.Combine(objectDir, "xl.meta");
                    if (File.Exists(oldMetaPath))
                    {
                        var previous = XlMetaFile.Read(objectDir, out _);
                        oldDataDir = previous.DataDirName != attemptDataDir ? previous.DataDirName : null;
                    }

                    XlMetaFile.Write(objectDir, diskRecord);
                    if (oldDataDir is not null && Directory.Exists(Path.Combine(objectDir, oldDataDir)))
                        MoveToTrash(disk, Path.Combine(objectDir, oldDataDir));
                    committed++;
                }
                catch (Exception ex)
                {
                    MarkOfflineIfIo(disk, ex);
                    _logger.LogWarning(ex, "EC: коммит Complete на диске {Position} не прошёл", disk.Position);
                }
            }

            if (committed < writeQuorum)
                throw new ObjectStoreException(ObjectStoreErrorCode.InsufficientWriteQuorum);

            // зачистка: журнал + каталог загрузки
            var updated = ReadUploads(bucket, key);
            updated.RemoveAll(e => e.UploadId == uploadId && e.Bucket == bucket && e.Key == key);
            WriteUploads(bucket, key, updated);
            TrashUpload(bucket, key, uploadId);
            // ETag — quoted (формат PutResult коммит-пути Put/Complete)
            return new CompleteResult('"' + record.ETag + '"');
        }
    }

    // ── sidecar bitrot-файлы частей загрузки ──
    // part.<N>.bitrot: [uint32 ecN][uint32 blockCount][ecN × blockCount × 32 Б] —
    // транзитное состояние загрузки (источник bitrot-записей xl.meta при Complete);
    // после Complete удаляется вместе с каталогом загрузки.

    private static void WritePartBitrot(string uploadDir, int partNumber,
        Dictionary<int, ErasurePartResult> partResults)
    {
        if (!partResults.TryGetValue(partNumber, out var result))
            return;
        var ecN = (uint)result.ShardBlockHashes.Length;
        var blockCount = ecN > 0 ? (uint)result.ShardBlockHashes[0].Length : 0u;
        using var ms = new MemoryStream();
        var header = new byte[8];
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(0, 4), ecN);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(4, 4), blockCount);
        ms.Write(header);
        for (var shard = 0; shard < ecN; shard++)
            for (var block = 0; block < blockCount; block++)
                ms.Write(result.ShardBlockHashes[shard][block]);
        File.WriteAllBytes(Path.Combine(uploadDir, $"part.{partNumber}.bitrot"), ms.ToArray());
    }

    // sidecar части с первого диска-носителя файла (диски сета; рассинхрон
    // носителей после отказов выравнивается heal); нигде нет — InvalidPart.
    internal static byte[][][] ReadPartBitrot(IReadOnlyList<string> uploadDirs, int partNumber)
    {
        byte[]? data = null;
        foreach (var uploadDir in uploadDirs)
        {
            var path = Path.Combine(uploadDir, $"part.{partNumber}.bitrot");
            if (File.Exists(path))
            {
                data = File.ReadAllBytes(path);
                break;
            }
        }

        if (data is null || data.Length < 8)
            throw new ObjectStoreException(ObjectStoreErrorCode.InvalidPart);
        var ecN = (int)System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(0, 4));
        var blockCount = (int)System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(4, 4));
        if (8 + (long)ecN * blockCount * 32 != data.Length)
            throw new ObjectStoreException(ObjectStoreErrorCode.InvalidPart); // битый sidecar
        var hashes = new byte[ecN][][];
        var offset = 8;
        for (var shard = 0; shard < ecN; shard++)
        {
            hashes[shard] = new byte[blockCount][];
            for (var block = 0; block < blockCount; block++)
            {
                hashes[shard][block] = data.AsSpan(offset, 32).ToArray();
                offset += 32;
            }
        }

        return hashes;
    }

    // Перегрузка IXlDrive.WritePartAsync направляет upload-staging на шардирование в загрузку.
    public Task<PartDigest> WritePartAsync(IXlStaging staging, int partNumber, Stream body,
        long contentLength, CancellationToken ct) =>
        staging is ErasureUploadStaging
            ? WriteUploadPartAsync(staging, partNumber, body, contentLength, ct)
            : WriteObjectPartAsync(staging, partNumber, body, contentLength, ct);

    // ── внутренняя механика ──

    private static ErasureStaging RequireErasureStaging(IXlStaging staging) =>
        staging as ErasureStaging
        ?? throw new ArgumentException($"требуется EC-staging, получен {staging.GetType().Name}", nameof(staging));

    // xl.meta-запись для диска (свой EcIndex) — собирается из record + bitrot staging.
    internal XlMetaEcInfo BuildEcInfo(ErasureStaging staging, int positionInSet) =>
        BuildEcInfo(staging.PartResults, staging.Distribution, positionInSet, staging.Parity);

    // xl.meta-EC для записи из результатов шардирования частей (per-disk EcIndex).
    internal XlMetaEcInfo BuildEcInfo(IReadOnlyDictionary<int, ErasurePartResult> partResults,
        int[] distribution, int positionInSet, int parity)
    {
        var m = parity;
        var n = _pool.Topology.SetSize;
        var parts = partResults.OrderBy(p => p.Key)
            .Select(p => new XlMetaPartBitrot(p.Value.SourceLength, p.Value.ShardBlockHashes))
            .ToList();
        return new XlMetaEcInfo(
            "reedsolomon",
            (byte)(n - m),
            (byte)n,
            (uint)ReedSolomonCodec.ErasureBlockMaxSize,
            (byte)positionInSet,
            distribution.Select(d => (byte)d).ToArray(),
            parts);
    }

    // Каталог объекта на диске (кодированный путь канона 04 §1).
    internal static string ObjectDirOnDisk(ErasureDisk disk, string bucket, string key) =>
        Path.Combine(disk.Root, bucket, XlPathEncoder.EncodePath(key));

    // rename каталога в .trash/<guid> диска (trash — от корня диска).
    internal static void MoveToTrash(ErasureDisk disk, string path)
    {
        var trash = Path.Combine(disk.Root, ".owns3.sys", ".trash");
        Directory.CreateDirectory(trash);
        Directory.Move(path, Path.Combine(trash, Guid.NewGuid().ToString("N")));
    }
}
