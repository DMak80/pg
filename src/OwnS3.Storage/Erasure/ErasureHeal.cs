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
            // Финальная внутренняя идиома heal (не мост): кворумный отказ
            // пробрасывается вызывающим → LogError, как сегодня
            var buckets = drive.ListBuckets().Throw().Value;
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
        // Сет без read-кворума — heal этого бакета невозможен, не ошибка
        var enumerated = drive.EnumerateObjects(bucket);
        if (!enumerated.IsSuccess)
        {
            logger.LogWarning("EC heal: листинг {Bucket} недоступен (сет без кворума) — пропуск", bucket);
            return;
        }

        var cursor = enumerated.Value;
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
        var lookup = drive.TryReadMeta(bucket, key);
        if (!lookup.IsSuccess)
        {
            // Отказ драйва — пропуск объекта heal-прохода с логом (пер-объектные
            // отказы проход гасит — прецедент catch в HealBucketAsync/HealObject)
            logger.LogWarning(lookup.Error, "EC heal: мета {Bucket}/{Key} недоступна — объект пропущен", bucket, key);
            return;
        }

        if (lookup.Value is not XlMetaLookup.Found found || found.Record.Ec is null)
            return; // Missing — не в консенсусе; без Ec — не трогать: реконструкция нереализуема
        var meta = found.Record;
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

    // ── multipart-журналы/части (рассинхрон после отказов) ──

    private void HealUploads()
    {
        foreach (var set in pool.Sets)
        {
            var online = set.Where(d => d.Online).ToList();
            if (online.Count < 2)
                continue;
            // эталонные журналы/файлы uploadDir: reference-носитель КАЖДОГО файла —
            // свежий по mtime (Abort/Complete при offline-диске оставили свежий
            // журнал на носителях операции; устаревшая копия вернувшегося диска
            // замещается, а не разносится — двусторонняя конвергенция, канон 06 §8
            // «рассинхрон после отказов выравнивается heal»)
            var multipartRoots = online
                .Select(d => (Disk: d, Root: Path.Combine(d.Root, ".owns3.sys", "multipart")))
                .Where(x => Directory.Exists(x.Root))
                .ToList();
            var carriers = new Dictionary<string, List<(string Root, string Path)>>(StringComparer.Ordinal);
            foreach (var (_, root) in multipartRoots)
            {
                foreach (var file in Directory.EnumerateFiles(root, "*", SearchOption.AllDirectories))
                {
                    // шардовые части НЕ копируются (каждый диск держит свой шард —
                    // восстановление только реконструкцией ниже); bitrot-sidecar — копируется
                    var name = Path.GetFileName(file);
                    var isReplicable = name is "uploads.json" or "parts.json" or "attempt.json"
                        or "xl.meta" or "xl.meta.bkp" || name.EndsWith(".bitrot");
                    if (!isReplicable)
                        continue;
                    var relative = Path.GetRelativePath(root, file);
                    if (!carriers.TryGetValue(relative, out var list))
                        carriers[relative] = list = [];
                    list.Add((root, file));
                }
            }

            // Копии журналов — под JournalLock драйва (сериализация с журнальными
            // операциями): вне лока микросекундное окно между чтением эталона и
            // записью копии позволяло перезаписать свежую запись клиента старым
            // содержимым — mtime копии закреплял откат на следующем проходе
            lock (drive.JournalLock)
            {
                foreach (var list in carriers.Values)
                {
                    if (list.Count < 2)
                        continue;
                    // reference — носитель с новейшим mtime файла
                    var (referenceRoot, referencePath) = list
                        .OrderByDescending(f => File.GetLastWriteTimeUtc(f.Path))
                        .First();
                    var referenceBytes = File.ReadAllBytes(referencePath);
                    foreach (var (peerRoot, peerPath) in list)
                    {
                        if (peerRoot == referenceRoot)
                            continue;
                        if (File.Exists(peerPath) && File.ReadAllBytes(peerPath).SequenceEqual(referenceBytes))
                            continue; // синхронизирован
                        Directory.CreateDirectory(Path.GetDirectoryName(peerPath)!);
                        File.Copy(referencePath, peerPath, overwrite: true);
                    }
                }
            }

            HealUploadParts(set, multipartRoots);
        }
    }

    // Реконструкция недостающих шардовых part.N живых загрузок сета (канон 06 §9:
    // «отсутствующие части multipart-загрузок/журналы — аналогично» — часть
    // загрузки тот же шардовый поток): источники — другие диски сета с bitrot-
    // сверкой по sidecar, дозапись tmp+rename (RestoreShardFile). Parity —
    // конвенция загрузок драйва (default parity), исходный размер части —
    // parts.json журнала.
    private void HealUploadParts(IReadOnlyList<ErasureDisk> set,
        List<(ErasureDisk Disk, string Root)> multipartRoots)
    {
        // каталоги загрузок сета (по parts.json; relative от multipart-корня)
        var uploadDirs = new HashSet<string>(StringComparer.Ordinal);
        foreach (var (_, root) in multipartRoots)
            foreach (var partsJson in Directory.EnumerateFiles(root, "parts.json", SearchOption.AllDirectories))
                uploadDirs.Add(Path.GetRelativePath(root, Path.GetDirectoryName(partsJson)!));

        // distribution формата тома — с первого онлайн-диска сета
        var formatDisk = set.FirstOrDefault(d => d.Online);
        byte[] dist;
        try
        {
            dist = ErasureVolumeInfo.Read(formatDisk!.Root).Distribution.Select(d => (byte)d).ToArray();
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "EC heal: формат тома недоступен — реконструкция частей загрузок пропущена");
            return;
        }

        var parity = drive.DefaultParity;
        var codec = new ReedSolomonCodec(set.Count - parity, parity);
        var ec = new XlMetaEcInfo("reedsolomon", (byte)(set.Count - parity), (byte)set.Count,
            (uint)BlockSize, 0, dist, []);

        foreach (var relative in uploadDirs)
        {
            // эталонный журнал частей — с первого носителя
            List<MultipartJournals.PartJournalEntry> entries;
            try
            {
                var partsPath = multipartRoots
                    .Select(r => Path.Combine(r.Root, relative, "parts.json"))
                    .First(File.Exists);
                entries = MultipartJournals.ReadParts(partsPath);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "EC heal: parts.json {Relative} недоступен — пропуск загрузки", relative);
                continue;
            }

            foreach (var entry in entries)
            {
                foreach (var (disk, root) in multipartRoots)
                {
                    var partPath = Path.Combine(root, relative, $"part.{entry.PartNumber}");
                    if (File.Exists(partPath))
                        continue;

                    // bitrot-хэши части — sidecar (реплицирован выше; носители сета)
                    var bitrot = ErasureXlDrive.ReadPartBitrot(
                        multipartRoots.Select(r => Path.Combine(r.Root, relative)).ToList(),
                        entry.PartNumber);
                    if (!bitrot.IsSuccess)
                    {
                        logger.LogWarning(bitrot.Error,
                            "EC heal: sidecar части {Part} {Relative} нигде нет — часть не реконструируется",
                            entry.PartNumber, relative);
                        continue;
                    }

                    var hashes = bitrot.Value;
                    if (RestoreShardFile(codec, set, ec, disk,
                            d => Path.Combine(
                                multipartRoots.First(r => r.Disk.Position == d.Position).Root,
                                relative, $"part.{entry.PartNumber}"),
                            entry.PartNumber, entry.Size, hashes))
                    {
                        _healedShards++;
                        logger.LogInformation(
                            "EC heal: часть {Part} загрузки {Relative} реконструирована на диске {Position}",
                            entry.PartNumber, relative, disk.Position);
                    }
                }
            }
        }
    }

    private static int CeilDiv(int value, int div) => (value + div - 1) / div;
}
