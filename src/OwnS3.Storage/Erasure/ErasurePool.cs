using System.Text;
using Microsoft.Extensions.Logging;

namespace OwnS3.Storage.Erasure;

// Диск позиции пула: корень + принадлежность сету + идентичность + онлайн-трекинг.
public sealed class ErasureDisk
{
    public required string Root { get; init; }     // путь позиции
    public required int SetIndex { get; init; }    // s = position / setSize
    public required int Position { get; init; }    // позиция в списке путей
    public required int PositionInSet { get; init; } // j = position % setSize

    // заполняется Initialize/заменой диска (внутри сборки)
    public string DiskId { get; internal set; } = string.Empty;

    // Каталог доступен и I/O жив; перепроверка тикером 30 с и heal (канон 06 §7).
    public bool Online { get; internal set; }
}

// Пул сетов (канон 06 §2–3, §7): Initialize по правилам старта; SIPMOD;
// трекинг online (лениво + тикер 30 с); замена диска (06 §9).
public sealed class ErasurePool : IDisposable
{
    private const string SysDirName = ".owns3.sys";
    private static readonly string[] SysSubDirs = ["tmp", "multipart", ".trash", "buckets", "config"];
    private static readonly TimeSpan OnlineTickerPeriod = TimeSpan.FromSeconds(30);

    private readonly TimeProvider _timeProvider;
    private readonly ILogger? _logger;
    private readonly object _lock = new();
    private CancellationTokenSource? _tickerCts;

    public ErasurePool(IReadOnlyList<string> roots, ErasureTopology topology,
        TimeProvider timeProvider, ILogger? logger = null)
    {
        if (roots.Count != topology.DriveCount)
            throw new InvalidOperationException(
                $"EC-пул: путей {roots.Count}, топология ожидает {topology.DriveCount}");

        _timeProvider = timeProvider;
        _logger = logger;
        Topology = topology;
        DeploymentId = Guid.NewGuid();
        var disks = new ErasureDisk[roots.Count];
        for (var i = 0; i < roots.Count; i++)
        {
            disks[i] = new ErasureDisk
            {
                Root = roots[i],
                SetIndex = i / topology.SetSize,
                Position = i,
                PositionInSet = i % topology.SetSize,
            };
        }

        Disks = disks;
        Sets = Enumerable.Range(0, topology.SetCount)
            .Select(s => (IReadOnlyList<ErasureDisk>)disks.Skip(s * topology.SetSize).Take(topology.SetSize).ToArray())
            .ToArray();
    }

    public Guid DeploymentId { get; private set; }

    public ErasureTopology Topology { get; }

    public IReadOnlyList<ErasureDisk> Disks { get; }

    public IReadOnlyList<IReadOnlyList<ErasureDisk>> Sets { get; }

    // Индекс сета объекта: SIPMOD (канон 06 §3) — SipHash-2-4(deploymentId-ключ,
    // «bucket/object») % число сетов.
    public int PickSetIndex(string bucket, string key)
    {
        var keyBytes = DeploymentId.ToByteArray().AsSpan(0, 16);
        var data = Encoding.UTF8.GetBytes(bucket + "/" + key);
        var hash = SipHash24.Hash(keyBytes, data);
        return (int)(hash % (ulong)Topology.SetCount);
    }

    // Старт по правилам канона 06 §2 (+ правка Task 0: отсутствующий путь живого
    // тома — offline, не кандидат замены).
    public void Initialize()
    {
        Topology.Validate();
        lock (_lock)
        {
            var states = new DiskState[Disks.Count];
            for (var i = 0; i < Disks.Count; i++)
                states[i] = Probe(i);

            var validIndexes = Enumerable.Range(0, Disks.Count).Where(i => states[i] == DiskState.Valid).ToList();
            if (validIndexes.Count == 0)
            {
                InitializeFresh(states);
                return;
            }

            InitializeExisting(states, validIndexes);
        }
    }

    // Перепроверка всех дисков (тикером 30 с и heal).
    public void RefreshOnline()
    {
        lock (_lock)
        {
            foreach (var disk in Disks)
                disk.Online = ProbeAlive(disk);
        }
    }

    // Единичная touch-проба записи диска (healthz — канон 05 §5: том доступен
    // на запись; наблюдение без мутации online-трекинга — его обновляет тикер).
    public bool ProbeDiskAlive(ErasureDisk disk) => ProbeAlive(disk);

    // Фоновый тикер online-перепроверки (канон 06 §7).
    public IDisposable StartOnlineTicker()
    {
        lock (_lock)
        {
            _tickerCts = new CancellationTokenSource();
            var token = _tickerCts.Token;
            _ = Task.Run(async () =>
            {
                using var timer = new PeriodicTimer(OnlineTickerPeriod, _timeProvider);
                while (!token.IsCancellationRequested)
                {
                    try
                    {
                        if (!await timer.WaitForNextTickAsync(token))
                            break;
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }

                    RefreshOnline();
                }
            }, token);
            return new TickerStop(this);
        }
    }

    public void Dispose()
    {
        lock (_lock)
        {
            _tickerCts?.Cancel();
            _tickerCts?.Dispose();
            _tickerCts = null;
        }
    }

    // ── внутренняя механика старта ──

    private enum DiskState { PathMissing, EmptyDir, Valid, ForeignContent }

    private DiskState Probe(int index)
    {
        var root = Disks[index].Root;
        if (!Directory.Exists(root))
            return DiskState.PathMissing;
        if (File.Exists(ErasureVolumeInfo.VolumeJsonPath(root)))
            return DiskState.Valid;
        // пустой каталог (без записей вообще) — кандидат замены; любое содержимое — чужое
        return Directory.EnumerateFileSystemEntries(root).Any()
            ? DiskState.ForeignContent
            : DiskState.EmptyDir;
    }

    // Свежая инсталляция: все пусты/отсутствуют — создание формата на каждом диске
    // (общие deploymentId/volumeId/distribution, свои diskId, sets по позициям) +
    // полная служебная структура + старт-чистка tmp каждого диска.
    private void InitializeFresh(DiskState[] states)
    {
        foreach (var (state, i) in states.Select((s, i) => (s, i)))
        {
            if (state == DiskState.ForeignContent)
                throw new InvalidOperationException(
                    $"EC-старт: позиция {i} ({Disks[i].Root}) содержит посторонние данные без volume.json — отказ");
        }

        var volumeId = Guid.NewGuid().ToString("N");
        var deploymentId = DeploymentId.ToString("N");
        var distribution = BuildDistribution(DeploymentId, Topology.SetSize);
        var diskIds = Enumerable.Range(0, Disks.Count).Select(_ => Guid.NewGuid().ToString("N")).ToArray();
        var sets = BuildSetsMatrix(diskIds);
        for (var i = 0; i < Disks.Count; i++)
        {
            Directory.CreateDirectory(Disks[i].Root);
            ErasureVolumeInfo.Write(Disks[i].Root, new ErasureVolumeInfo(
                ErasureVolumeInfo.MagicValue, ErasureVolumeInfo.CurrentFormatVersion, volumeId,
                ErasureVolumeInfo.ModeValue, deploymentId, diskIds[i], Topology.SetSize, sets,
                distribution, ErasureVolumeInfo.DistributionAlgoValue));
            CreateSysStructure(Disks[i]);
            WipeTmp(Disks[i]);
            Disks[i].DiskId = diskIds[i];
            Disks[i].Online = true;
        }

        _logger?.LogInformation("EC-пул: создан новый том {VolumeId}, сетов {SetCount}, setSize {SetSize}",
            volumeId, Topology.SetCount, Topology.SetSize);
    }

    // Живой том: эталонный формат — с любого валидного диска; согласованность,
    // offline-пути, кандидаты замены (правила канона 06 §2/§9 + правка Task 0).
    private void InitializeExisting(DiskState[] states, List<int> validIndexes)
    {
        var referenceIndex = validIndexes[0];
        var reference = ErasureVolumeInfo.Read(Disks[referenceIndex].Root);

        ValidateReference(reference, referenceIndex);

        // форматы валидных дисков читаются ОДИН раз (кэш циклов ниже)
        var formats = new Dictionary<int, ErasureVolumeInfo> { [referenceIndex] = reference };
        foreach (var i in validIndexes.Skip(1))
        {
            var info = ErasureVolumeInfo.Read(Disks[i].Root);
            ValidateAgainstReference(info, reference, Disks[i]);
            if (info.DiskId == reference.DiskId)
                throw new InvalidOperationException(
                    $"EC-старт: позиция {i} ({Disks[i].Root}) несёт diskId позиции {referenceIndex} — подмена дисков запрещена");
            formats[i] = info;
        }

        // изменения числа путей против sets формата — отказ
        var expectedDrives = reference.Sets.Length * reference.SetSize;
        if (expectedDrives != Disks.Count)
            throw new InvalidOperationException(
                $"EC-старт: формат ожидает {expectedDrives} дисков, конфигурация даёт {Disks.Count} — расширение тома запрещено");

        // ожидаемый diskId каждой позиции из sets эталона
        var expectedDiskIds = new string[Disks.Count];
        for (var s = 0; s < reference.Sets.Length; s++)
            for (var j = 0; j < reference.SetSize; j++)
                expectedDiskIds[s * reference.SetSize + j] = reference.Sets[s][j];

        foreach (var i in validIndexes)
        {
            if (formats[i].DiskId != expectedDiskIds[i])
                throw new InvalidOperationException(
                    $"EC-старт: позиция {i} ({Disks[i].Root}) несёт diskId вне sets своей позиции — отказ");
        }

        // deploymentId живого тома восстанавливается из формата: SIPMOD-ключ
        // PickSetIndex обязан совпадать между рестартами процесса — «тот же
        // ключ всегда в тот же сет, пока жив формат тома» (канон 06 §3)
        if (!Guid.TryParseExact(reference.DeploymentId, "N", out var volumeDeploymentId))
            throw new InvalidOperationException(
                $"EC-старт: позиция {referenceIndex}: deploymentId формата «{reference.DeploymentId}» не GUID — отказ");
        DeploymentId = volumeDeploymentId;

        // per-set замена кандидатов: живые старые >= k, заменяемых < k
        var replacementDiskIds = new Dictionary<int, string>();
        for (var s = 0; s < Topology.SetCount; s++)
        {
            var setDisks = Sets[s];
            var aliveOld = 0;
            var candidates = new List<int>();
            foreach (var disk in setDisks)
            {
                var state = states[disk.Position];
                if (state == DiskState.Valid)
                    aliveOld++;
                else if (state == DiskState.EmptyDir)
                    candidates.Add(disk.Position);
                else if (state == DiskState.ForeignContent)
                    throw new InvalidOperationException(
                        $"EC-старт: позиция {disk.Position} ({disk.Root}) содержит посторонние данные — отказ");
            }

            if (candidates.Count == 0)
                continue;
            if (candidates.Count >= Topology.DataShards)
                throw new InvalidOperationException(
                    $"EC-старт: сет {s}: заменяемых одновременно {candidates.Count} >= k={Topology.DataShards} — отказ старта");
            if (aliveOld < Topology.DataShards)
                throw new InvalidOperationException(
                    $"EC-старт: сет {s}: живых старых дисков {aliveOld} < k={Topology.DataShards} при кандидатах замены — отказ старта (реконструировать не из чего)");

            // замена: новый diskId каждому кандидату, кворумное обновление sets
            foreach (var position in candidates)
                replacementDiskIds[position] = Guid.NewGuid().ToString("N");
        }

        string[][] effectiveSets;
        if (replacementDiskIds.Count > 0)
        {
            effectiveSets = reference.Sets.Select(set => set.ToArray()).ToArray();
            foreach (var (position, diskId) in replacementDiskIds)
                effectiveSets[position / Topology.SetSize][position % Topology.SetSize] = diskId;
            // пишем обновлённый формат на валидные диски (из кэша чтений)
            // (замена выполняется при старте, heal потом доливает — канон 06 §9);
            // формат кандидата пишет финальный цикл ниже
            foreach (var i in validIndexes)
                ErasureVolumeInfo.Write(Disks[i].Root, formats[i] with { Sets = effectiveSets });
        }
        else
        {
            effectiveSets = reference.Sets.Select(set => set.ToArray()).ToArray();
        }

        // финальное состояние дисков: identity/online + идемпотентная структура + старт-tmp
        for (var i = 0; i < Disks.Count; i++)
        {
            var disk = Disks[i];
            if (states[i] == DiskState.Valid)
            {
                disk.DiskId = formats[i].DiskId;
            }
            else if (replacementDiskIds.TryGetValue(i, out var newDiskId))
            {
                ErasureVolumeInfo.Write(disk.Root, new ErasureVolumeInfo(
                    reference.Magic, reference.FormatVersion, reference.VolumeId, reference.Mode,
                    reference.DeploymentId, newDiskId, reference.SetSize,
                    effectiveSets,
                    reference.Distribution, reference.DistributionAlgo));
                disk.DiskId = newDiskId;
            }
            else
            {
                // отсутствующий путь — offline (правка Task 0): замены нет
                disk.DiskId = expectedDiskIds[i];
                disk.Online = false;
                continue;
            }

            CreateSysStructure(disk);
            WipeTmp(disk);
            disk.Online = true;
        }

        _logger?.LogInformation(
            "EC-пул: живой том {VolumeId}, сетов {SetCount}; заменено дисков {Replaced}, offline {Offline}",
            reference.VolumeId, Topology.SetCount, replacementDiskIds.Count,
            Disks.Count(d => !d.Online));
    }

    // Эталонный формат валиден по своей структуре.
    private void ValidateReference(ErasureVolumeInfo info, int referenceIndex)
    {
        if (info.Magic != ErasureVolumeInfo.MagicValue)
            throw new InvalidOperationException(
                $"EC-старт: позиция {referenceIndex} ({Disks[referenceIndex].Root}): чужой magic «{info.Magic}» — отказ");
        if (info.FormatVersion > ErasureVolumeInfo.CurrentFormatVersion)
            throw new InvalidOperationException(
                $"EC-старт: позиция {referenceIndex}: версия формата {info.FormatVersion} новее поддерживаемой — отказ");
        if (info.FormatVersion < ErasureVolumeInfo.CurrentFormatVersion)
            throw new InvalidOperationException(
                $"EC-старт: позиция {referenceIndex}: версия формата {info.FormatVersion} — не EC-том — отказ");
        if (info.Mode != ErasureVolumeInfo.ModeValue)
            throw new InvalidOperationException(
                $"EC-старт: позиция {referenceIndex}: режим «{info.Mode}» не «{ErasureVolumeInfo.ModeValue}» — отказ");
        if (info.SetSize != Topology.SetSize)
            throw new InvalidOperationException(
                $"EC-старт: setSize формата {info.SetSize} != конфигурации {Topology.SetSize} — отказ (setSize фиксирован форматом)");
        if (info.DistributionAlgo != ErasureVolumeInfo.DistributionAlgoValue)
            throw new InvalidOperationException(
                $"EC-старт: distributionAlgo «{info.DistributionAlgo}» не поддерживается — отказ");
        if (info.Sets.Length * info.SetSize != Disks.Count)
            throw new InvalidOperationException(
                $"EC-старт: формат ожидает {info.Sets.Length * info.SetSize} дисков, конфигурация даёт {Disks.Count} — расширение/изменение числа путей живого тома запрещено");
        if (info.Sets.Length != Topology.SetCount || info.Sets.Any(set => set.Length != Topology.SetSize))
            throw new InvalidOperationException(
                $"EC-старт: sets формата не согласуются с топологией {Topology.DriveCount}/{Topology.SetSize} — отказ");
        if (!IsPermutation(info.Distribution, Topology.SetSize))
            throw new InvalidOperationException("EC-старт: distribution формата не перестановка 0..setSize−1 — отказ");
    }

    // Согласованность валидного диска с эталоном (подмена топологий запрещена).
    private void ValidateAgainstReference(ErasureVolumeInfo info, ErasureVolumeInfo reference, ErasureDisk disk)
    {
        if (info.DeploymentId != reference.DeploymentId)
            throw new InvalidOperationException(
                $"EC-старт: диск {disk.Position}: deploymentId не совпадает с эталоном — смешение топологий запрещено");
        if (info.VolumeId != reference.VolumeId)
            throw new InvalidOperationException(
                $"EC-старт: диск {disk.Position}: volumeId не совпадает с эталоном — смешение томов запрещено");
        if (info.SetSize != reference.SetSize)
            throw new InvalidOperationException($"EC-старт: диск {disk.Position}: setSize не совпадает с эталоном — отказ");
        if (!info.Distribution.SequenceEqual(reference.Distribution))
            throw new InvalidOperationException(
                $"EC-старт: диск {disk.Position}: distribution не совпадает с эталоном — отказ");
        if (info.DistributionAlgo != reference.DistributionAlgo)
            throw new InvalidOperationException(
                $"EC-старт: диск {disk.Position}: distributionAlgo не совпадает с эталоном — отказ");
        if (info.FormatVersion != reference.FormatVersion)
            throw new InvalidOperationException(
                $"EC-старт: диск {disk.Position}: версия формата не совпадает с эталоном — отказ");
    }

    // Служебная структура .owns3.sys/{tmp,multipart,.trash,buckets,config} —
    // идемпотентно (параллель XlVolume.Initialize в single, канон 06 §2).
    private static void CreateSysStructure(ErasureDisk disk)
    {
        foreach (var sub in SysSubDirs)
            Directory.CreateDirectory(Path.Combine(disk.Root, SysDirName, sub));
    }

    // Стартовая безусловная очистка tmp диска (канон 04 §6/06 §10).
    private static void WipeTmp(ErasureDisk disk)
    {
        var tmpDir = Path.Combine(disk.Root, SysDirName, "tmp");
        if (!Directory.Exists(tmpDir))
            return;
        foreach (var entry in Directory.EnumerateFileSystemEntries(tmpDir))
        {
            if (File.Exists(entry))
                File.Delete(entry);
            else
                Directory.Delete(entry, recursive: true);
        }
    }

    // Каталог доступен и I/O жив (touch-проба в tmp).
    private bool ProbeAlive(ErasureDisk disk)
    {
        try
        {
            var tmpDir = Path.Combine(disk.Root, SysDirName, "tmp");
            if (!Directory.Exists(tmpDir))
                return false;
            var probe = Path.Combine(tmpDir, "online-" + Guid.NewGuid().ToString("N"));
            File.WriteAllText(probe, string.Empty);
            File.Delete(probe);
            return true;
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "EC: диск {Position} ({Root}) offline — проба I/O не прошла", disk.Position, disk.Root);
            return false;
        }
    }

    private string[][] BuildSetsMatrix(string[] diskIds) =>
        Enumerable.Range(0, Topology.SetCount)
            .Select(s => diskIds.Skip(s * Topology.SetSize).Take(Topology.SetSize).ToArray())
            .ToArray();

    private static bool IsPermutation(int[] values, int setSize) =>
        values.Length == setSize && values.OrderBy(v => v).SequenceEqual(Enumerable.Range(0, setSize));

    // Detemинированная перестановка distribution из deploymentId: seed — SipHash
    // (deploymentId-ключ, «distribution»), Fisher–Yates по SipHash-потоку.
    // В файле хранится готовая перестановка — воспроизводить при валидации не нужно.
    internal static int[] BuildDistribution(Guid deploymentId, int setSize)
    {
        var key = deploymentId.ToByteArray().AsSpan(0, 16);
        var perm = Enumerable.Range(0, setSize).ToArray();
        for (var j = setSize - 1; j >= 1; j--)
        {
            var data = Encoding.UTF8.GetBytes("distribution/" + j);
            var hash = SipHash24.Hash(key, data);
            var r = (int)(hash % (ulong)(j + 1));
            (perm[j], perm[r]) = (perm[r], perm[j]);
        }

        return perm;
    }

    private sealed class TickerStop(ErasurePool pool) : IDisposable
    {
        public void Dispose() => pool.Dispose();
    }
}
