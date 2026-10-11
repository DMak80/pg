using System.Buffers.Binary;
using Microsoft.Extensions.Logging;
using OwnS3.Storage;

namespace OwnS3.Storage.Erasure;

// Фоновый heal-проход (канон 06 §9 + правка Task 0): по сетам → листинг →
// по-дисковая сверка живого-в-консенсусе → дозаписи tmp+rename. `.trash/` и
// orphan heal не сканирует (их удаляют чистки по порогу — §10; ответственность
// не дублируется). RequestPass не создаёт параллельного прохода: идущий
// продолжается.
public sealed class ErasureHealService(ErasurePool pool, ErasureXlDrive drive,
    ErasureStatusProvider status, TimeProvider timeProvider, ILogger<ErasureHealService> logger)
{
    private const int BlockSize = ReedSolomonCodec.ErasureBlockMaxSize;
    private readonly object _gate = new();
    private long _healedObjects;
    private long _healedShards;
    private DateTimeOffset? _lastPassAt;

    public bool IsRunning { get; private set; }

    /// Завершение прохода: число вылеченных объектов (метрики OwnS3EcMetrics).
    public event Action<long>? PassCompleted;

    // POST /admin/heal: запуск внепланового прохода (идущий продолжается —
    // параллельный не создаётся).
    public void RequestPass()
    {
        _ = Task.Run(async () =>
        {
            try
            {
                await RunPassAsync(CancellationToken.None);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "EC heal: внеплановый проход упал");
            }
        });
    }

    public async Task RunPassAsync(CancellationToken ct)
    {
        lock (_gate)
        {
            if (IsRunning)
                return; // идущий проход продолжается — параллельный не создаётся
            IsRunning = true;
        }

        status.UpdateHeal(true, _lastPassAt, _healedObjects, _healedShards);
        try
        {
            pool.RefreshOnline();
            var buckets = drive.ListBuckets();
            foreach (var bucket in buckets)
                await HealBucketAsync(bucket.Name, ct);
            HealUploads();
            _lastPassAt = timeProvider.GetUtcNow();
            PassCompleted?.Invoke(_healedObjects);
        }
        finally
        {
            IsRunning = false;
            status.UpdateHeal(false, _lastPassAt, _healedObjects, _healedShards);
        }
    }

    // ── объекты ──

    private async Task HealBucketAsync(string bucket, CancellationToken ct)
    {
        IXlObjectCursor cursor;
        try
        {
            cursor = drive.EnumerateObjects(bucket);
        }
        catch (ObjectStoreException)
        {
            // сет без read-кворума — heal этого бакета невозможен, не ошибка
            logger.LogWarning("EC heal: листинг {Bucket} недоступен (сет без кворума) — пропуск", bucket);
            return;
        }

        while (cursor.PeekKey is { } key)
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                HealObject(bucket, key);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "EC heal: объект {Bucket}/{Key} не вылечен этим проходом", bucket, key);
            }

            cursor.MoveNext();
        }
    }

    private void HealObject(string bucket, string key)
    {
        var meta = drive.TryReadMeta(bucket, key);
        if (meta?.Ec is null)
            return; // не в консенсусе (или битая) — чистки/heal не трогают
        var ec = meta.Ec;
        var codec = new ReedSolomonCodec(ec.EcM, ec.EcN - ec.EcM);
        var set = pool.Sets[pool.PickSetIndex(bucket, key)];
        var healed = false;

        foreach (var disk in set.Where(d => d.Online))
        {
            var objectDir = ErasureXlDrive.ObjectDirOnDisk(disk, bucket, key);
            var metaOk = false;
            if (File.Exists(Path.Combine(objectDir, "xl.meta")))
            {
                try
                {
                    _ = XlMetaFile.Read(objectDir, out _);
                    metaOk = true;
                }
                catch (XlIntegrityException)
                {
                    // биткая — копия консенсусной с корректным EcIndex
                }
            }

            var dataDir = Path.Combine(objectDir, meta.DataDirName);
            var dataOk = Directory.Exists(dataDir);
            if (!metaOk || !dataOk)
            {
                // мета-копия + гарантия dataDir (части долечиваются ниже)
                XlMetaFile.Write(objectDir, meta with { Ec = ec with { EcIndex = (byte)disk.PositionInSet } });
                Directory.CreateDirectory(dataDir);
                healed = true;
                logger.LogInformation("EC heal: {Bucket}/{Key} — мета/dataDir восстановлены на диске {Position}",
                    bucket, key, disk.Position);
            }

            // шард-файлы частей: отсутствие или bitrot-порча → реконструкция
            for (var i = 0; i < ec.Parts.Count; i++)
            {
                var partNumber = i + 1;
                var shardIndex = Array.IndexOf(ec.EcDist, (byte)disk.PositionInSet);
                var partPath = Path.Combine(dataDir, $"part.{partNumber}");
                var needRestore = !File.Exists(partPath) || IsShardCorrupted(codec, set, ec, meta.DataDirName,
                    bucket, key, partNumber, shardIndex, ec.Parts[i]);
                if (!needRestore)
                    continue;
                if (RestoreShardFile(codec, set, ec, disk,
                        d => Path.Combine(ErasureXlDrive.ObjectDirOnDisk(d, bucket, key), meta.DataDirName,
                            $"part.{partNumber}"),
                        partNumber, ec.Parts[i].SourceSize, ec.Parts[i].BlockHashes))
                {
                    _healedShards++;
                    healed = true;
                }
            }
        }

        if (healed)
            _healedObjects++;
    }

    // bitrot-сверка шарда j части N на диске (все блоки против 2D-хэшей).
    private bool IsShardCorrupted(ReedSolomonCodec codec, IReadOnlyList<ErasureDisk> set, XlMetaEcInfo ec,
        string dataDirName, string bucket, string key, int partNumber, int shardIndex, XlMetaPartBitrot part)
    {
        var disk = set.First(d => d.PositionInSet == ec.EcDist[shardIndex]);
        var partPath = Path.Combine(ErasureXlDrive.ObjectDirOnDisk(disk, bucket, key), dataDirName, $"part.{partNumber}");
        var fullShardLen = CeilDiv(BlockSize, codec.DataShards);
        using var stream = new FileStream(partPath, FileMode.Open, FileAccess.Read, FileShare.Read);
        var blockCount = part.BlockHashes.Length > 0 ? part.BlockHashes[0].Length : 0;
        for (var b = 0; b < blockCount; b++)
        {
            var blockLen = (int)Math.Min(BlockSize, part.SourceSize - (long)b * BlockSize);
            var shardLen = CeilDiv(blockLen, codec.DataShards);
            var buffer = new byte[shardLen];
            var read = stream.Read(buffer, 0, shardLen);
            if (read != shardLen)
                return true; // обрыв файла — порча
            var expected = part.BlockHashes[shardIndex][b];
            var actual = HighwayHash256.Hash(HighwayHash256.MagicKey, buffer);
            if (!actual.AsSpan().SequenceEqual(expected))
                return true;
        }

        return false;
    }

    // Реконструкция шард-файла целевой позиции: по блокам из k живых копий
    // (bitrot-сверка каждого источника), tmp+rename поверх цели.
    private bool RestoreShardFile(ReedSolomonCodec codec, IReadOnlyList<ErasureDisk> set, XlMetaEcInfo ec,
        ErasureDisk target, Func<ErasureDisk, string> pathOf, int partNumber, long sourceSize, byte[][][] hashes)
    {
        var targetShard = Array.IndexOf(ec.EcDist, (byte)target.PositionInSet);
        var blockCount = hashes.Length > 0 ? hashes[0].Length : 0;
        var fullShardLen = CeilDiv(BlockSize, codec.DataShards);
        var tmpDir = Path.Combine(target.Root, ".owns3.sys", "tmp", "heal-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tmpDir);
        var tmpPath = Path.Combine(tmpDir, $"part.{partNumber}");
        try
        {
            using (var output = new FileStream(tmpPath, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                for (var b = 0; b < blockCount; b++)
                {
                    var blockLen = (int)Math.Min(BlockSize, sourceSize - (long)b * BlockSize);
                    var shardLen = CeilDiv(blockLen, codec.DataShards);
                    var shards = new byte[]?[codec.TotalShards];
                    for (var j = 0; j < codec.TotalShards; j++)
                    {
                        var disk = set.FirstOrDefault(d => d.Online && d.PositionInSet == ec.EcDist[j]);
                        if (disk is null || disk.Position == target.Position)
                            continue;
                        var path = pathOf(disk);
                        if (!File.Exists(path))
                            continue;
                        using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read);
                        stream.Seek((long)b * fullShardLen, SeekOrigin.Begin);
                        var buffer = new byte[shardLen];
                        var read = stream.Read(buffer, 0, shardLen);
                        if (read != shardLen)
                            continue;
                        // bitrot-сверка источника
                        if (hashes[j][b] is { } expected)
                        {
                            var actual = HighwayHash256.Hash(HighwayHash256.MagicKey, buffer);
                            if (!actual.AsSpan().SequenceEqual(expected))
                                continue; // битый источник — не参与 реконструкции
                        }

                        shards[j] = buffer;
                    }

                    try
                    {
                        codec.Reconstruct(shards);
                    }
                    catch (InvalidOperationException)
                    {
                        logger.LogWarning(
                            "EC heal: блок {Block} части {Part} — живых источников меньше k — реконструкция невозможна",
                            b, partNumber);
                        return false;
                    }

                    output.Write(shards[targetShard]!, 0, shardLen);
                    output.Flush(flushToDisk: true);
                }
            }

            var dest = pathOf(target);
            Directory.CreateDirectory(Path.GetDirectoryName(dest)!);
            File.Move(tmpPath, dest, overwrite: true);
            return true;
        }
        finally
        {
            if (Directory.Exists(tmpDir))
                Directory.Delete(tmpDir, recursive: true);
        }
    }

    // ── multipart-зурналы/части (рассинхрон после отказов) ──

    private void HealUploads()
    {
        foreach (var set in pool.Sets)
        {
            var online = set.Where(d => d.Online).ToList();
            if (online.Count < 2)
                continue;
            // эталонные журналы/файлы каждого uploadDir — с первого диска, где они есть;
            // недостающее на других — копия (журналы/attempt/меты идентичны по содержанию)
            var multipartRoots = online
                .Select(d => (Disk: d, Root: Path.Combine(d.Root, ".owns3.sys", "multipart")))
                .Where(x => Directory.Exists(x.Root))
                .ToList();
            foreach (var reference in multipartRoots)
            {
                foreach (var peer in multipartRoots.Where(x => x.Disk.Position != reference.Disk.Position))
                {
                    foreach (var file in Directory.EnumerateFiles(reference.Root, "*", SearchOption.AllDirectories))
                    {
                        var relative = Path.GetRelativePath(reference.Root, file);
                        // шардовые части НЕ копируются (каждый диск держит свой шард —
                        // восстановление только реконструкцией); bitrot-sidecar — копируется
                        var name = Path.GetFileName(file);
                        var isReplicable = name is "uploads.json" or "parts.json" or "attempt.json"
                            or "xl.meta" or "xl.meta.bkp" || name.EndsWith(".bitrot");
                        if (!isReplicable)
                            continue;
                        var target = Path.Combine(peer.Root, relative);
                        if (!File.Exists(target))
                        {
                            Directory.CreateDirectory(Path.GetDirectoryName(target)!);
                            File.Copy(file, target);
                        }
                    }
                }
            }
        }
    }

    private static int CeilDiv(int value, int div) => (value + div - 1) / div;
}
