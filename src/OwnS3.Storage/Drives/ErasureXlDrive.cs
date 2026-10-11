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

    public ErasureXlDrive(ErasurePool pool, int defaultParity, TimeProvider timeProvider,
        ILogger<ErasureXlDrive> logger)
    {
        _pool = pool;
        _defaultParity = defaultParity;
        _timeProvider = timeProvider;
        _logger = logger;
    }

    private ErasurePool Pool => _pool;

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
        foreach (var disk in setDisks)
        {
            var info = ErasureVolumeInfo.Read(disk.Root);
            return info.Distribution;
        }

        throw new InvalidOperationException("EC: сет без валидных дисков");
    }

    public async Task<PartDigest> WritePartAsync(IXlStaging staging, int partNumber, Stream body,
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

    public Task RunCleanupAsync(CancellationToken ct) => throw new NotImplementedException();
    public bool CheckHealth() => throw new NotImplementedException();
    public bool BucketExists(string bucket) => throw new NotImplementedException();
    public void CreateBucket(string bucket) => throw new NotImplementedException();
    public void DeleteBucket(string bucket) => throw new NotImplementedException();
    public IReadOnlyList<BucketEntry> ListBuckets() => throw new NotImplementedException();
    public XlMetaRecord? TryReadMeta(string bucket, string key) => throw new NotImplementedException();
    public void CopyPartsToStaging(IXlStaging staging, string srcBucket, string srcKey, XlMetaRecord srcMeta) =>
        throw new NotImplementedException();
    public Stream OpenData(string bucket, string key, XlMetaRecord meta, long offset, long length) =>
        throw new NotImplementedException();
    public void VerifyFullChecksum(string bucket, string key, XlMetaRecord meta) =>
        throw new NotImplementedException();
    public void Delete(string bucket, string key, XlMetaRecord meta) => throw new NotImplementedException();
    public IReadOnlyList<(int Number, long Size)> EnumerateParts(string bucket, string key, XlMetaRecord meta) =>
        throw new NotImplementedException();
    public IXlObjectCursor EnumerateObjects(string bucket) => throw new NotImplementedException();
    public List<MultipartJournals.UploadJournalEntry> ReadUploads(string bucket, string key) =>
        throw new NotImplementedException();
    public void WriteUploads(string bucket, string key, List<MultipartJournals.UploadJournalEntry> uploads) =>
        throw new NotImplementedException();
    public IReadOnlyList<string> ListUploadKeyDirs() => throw new NotImplementedException();
    public List<MultipartJournals.UploadJournalEntry> ReadUploadsByKeyDir(string keyDirPath) =>
        throw new NotImplementedException();
    public bool UploadDirExists(string bucket, string key, string uploadId) => throw new NotImplementedException();
    public void CreateUploadDir(string bucket, string key, string uploadId, XlMetaRecord uploadRecord) =>
        throw new NotImplementedException();
    public XlMetaRecord ReadUploadMeta(string bucket, string key, string uploadId) =>
        throw new NotImplementedException();
    public void TrashUpload(string bucket, string key, string uploadId) => throw new NotImplementedException();
    public List<MultipartJournals.PartJournalEntry> ReadParts(string bucket, string key, string uploadId) =>
        throw new NotImplementedException();
    public IXlStaging CreateUploadStaging(string bucket, string key, string uploadId) =>
        throw new NotImplementedException();
    public void CommitUploadPart(IXlStaging staging, int partNumber, string etagHex, long size, DateTimeOffset modTime) =>
        throw new NotImplementedException();
    public CompleteResult CommitMultipart(string bucket, string key, string uploadId,
        IReadOnlyList<PartEtag> manifest, XlMetaRecord newRecord) => throw new NotImplementedException();

    // ── внутренняя механика ──

    private static ErasureStaging RequireErasureStaging(IXlStaging staging) =>
        staging as ErasureStaging
        ?? throw new ArgumentException($"требуется EC-staging, получен {staging.GetType().Name}", nameof(staging));

    // xl.meta-запись для диска (свой EcIndex) — собирается из record + bitrot staging.
    internal XlMetaEcInfo BuildEcInfo(ErasureStaging staging, int positionInSet)
    {
        var m = staging.Parity;
        var n = _pool.Topology.SetSize;
        var parts = staging.PartResults.OrderBy(p => p.Key)
            .Select(p => new XlMetaPartBitrot(p.Value.SourceLength, p.Value.ShardBlockHashes))
            .ToList();
        return new XlMetaEcInfo(
            "reedsolomon",
            (byte)(n - m),
            (byte)n,
            (uint)ReedSolomonCodec.ErasureBlockMaxSize,
            (byte)positionInSet,
            staging.Distribution.Select(d => (byte)d).ToArray(),
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
