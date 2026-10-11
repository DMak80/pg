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

    public Task RunCleanupAsync(CancellationToken ct) => throw new NotImplementedException(); // Task 12

    // Формат пула валиден + touch-проба на каждом онлайн-диске (спека §4.3);
    // деградация НЕ роняет healthz — она наблюдается в /admin/storage.
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
        }

        return true;
    }

    // ── бакеты: на дисках ВСЕХ сетов (канон 06 §8) ──

    public bool BucketExists(string bucket) =>
        bucket != ".owns3.sys" && _pool.Sets
            .Where(set => set.Count(d => d.Online) >= ErasureQuorum.Read(
                _pool.Topology.SetSize - _defaultParity, _defaultParity))
            .SelectMany(set => set)
            .Any(disk => Directory.Exists(Path.Combine(disk.Root, bucket)));

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
                // живые multipart-загрузки бакета блокируют удаление (загрузки — Task 11)
            }
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
    public void CopyPartsToStaging(IXlStaging staging, string srcBucket, string srcKey, XlMetaRecord srcMeta) =>
        throw new NotImplementedException();
    public Stream OpenData(string bucket, string key, XlMetaRecord meta, long offset, long length)
    {
        ArgumentNullException.ThrowIfNull(meta.Ec, nameof(meta));

        var ec = meta.Ec;
        var codec = CodecFor(ec.EcN - ec.EcM);
        var set = _pool.Sets[_pool.PickSetIndex(bucket, key)];
        var windows = new List<ErasureBlockReader>();
        var windowStart = offset;
        var windowEnd = offset + length;
        var partStart = 0L;
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
                var readers = OpenShardStreams(set, bucket, key, meta, i + 1, ec);
                windows.Add(new ErasureBlockReader(codec, readers, result, localOffset, localEnd - partStart - localOffset));
            }

            partStart = partEnd;
        }

        return new ErasureObjectStream(windows);
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
    // Delete: rename каталога объекта в .trash/ на каждом диске сета (кворум
    // ≥ writeQuorum; недоступные диски выравниваются при возврате: живое-в-
    // консенсусе доливается heal; фантом-записи чистятся по порогу — канон 06 §8).
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
                MoveToTrash(disk, target);
                successes++;
            }
            catch (Exception ex)
            {
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
