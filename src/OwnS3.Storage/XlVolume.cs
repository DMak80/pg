using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace OwnS3.Storage;

// Том xl (канон 04 §1/§3/§6). Один экземпляр на процесс; после Initialize —
// служебная структура готова, чистки старта выполнены.
public sealed class XlVolume(string root, TimeProvider timeProvider, ILogger? logger = null)
{
    private const string SysDirName = ".owns3.sys";
    private const string Magic = "OWNS3-VOL";
    private const int CurrentFormatVersion = 1;
    private const string Mode = "XL Single";
    private static readonly TimeSpan AgeThreshold = TimeSpan.FromHours(1);
    private static readonly TimeSpan AbandonedUploadThreshold = TimeSpan.FromHours(24);

    // Модель volume.json (канон 04 §3).
    private sealed record VolumeInfo(string Magic, int FormatVersion, string VolumeId, string Mode);

    // camelCase-поля файла: {"magic","formatVersion","volumeId","mode"}.
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    public string Root { get; } = root;
    public string SysDir { get; } = Path.Combine(root, SysDirName);
    public string TmpDir { get; } = Path.Combine(root, SysDirName, "tmp");
    public string MultipartDir { get; } = Path.Combine(root, SysDirName, "multipart");
    public string TrashDir { get; } = Path.Combine(root, SysDirName, ".trash");
    public string BucketsMetaDir { get; } = Path.Combine(root, SysDirName, "buckets");
    public string ConfigDir { get; } = Path.Combine(root, SysDirName, "config");
    public bool Initialized { get; private set; }

    internal string ConfigVolumeJsonPath => Path.Combine(SysDir, "volume.json");

    // Создание на пустом томе ИЛИ валидация существующего volume.json
    // (отсутствие при непустом томе / чужой magic / версия > 1 / режим не
    // «XL Single» — исключение fail-fast, канон 04 §3). Затем старт-чистки §6:
    // tmp/* — безусловно; .trash/* и orphan-dataDir старше 1 ч (mtime).
    public void Initialize()
    {
        Directory.CreateDirectory(Root);
        Directory.CreateDirectory(SysDir);
        var volumePath = ConfigVolumeJsonPath;
        if (File.Exists(volumePath))
        {
            ValidateVolumeJson(volumePath);
        }
        else
        {
            EnsureVolumeEmpty();
            File.WriteAllText(volumePath, JsonSerializer.Serialize(
                new VolumeInfo(Magic, CurrentFormatVersion, Guid.NewGuid().ToString("N"), Mode), JsonOptions));
        }
        // Служебная структура (идемпотентно)
        Directory.CreateDirectory(TmpDir);
        Directory.CreateDirectory(MultipartDir);
        Directory.CreateDirectory(TrashDir);
        Directory.CreateDirectory(BucketsMetaDir);
        Directory.CreateDirectory(ConfigDir);
        // fsync каталогов опущен: стандартного BCL-API нет, спека §4.3 п.2 —
        // best-effort (механизм без fsync-каталога — не отказ)
        // Старт: tmp/* — БЕЗУСЛОВНО (незакоммиченный staging; закоммиченных
        // данных там не бывает по построению — канон 04 §6), затем возрастные
        // .trash/orphan и брошенные multipart-загрузки.
        WipeTmp();
        CleanAged();
        CleanupAbandonedUploads();
        Initialized = true;
    }

    // Фоновый проход: возрастные .trash и orphan-dataDir (порог 1 ч), брошенные
    // multipart-загрузки (порог 24 ч); tmp НЕ трогается — на старте процесса
    // может идти in-flight PUT со staging (безусловная очистка tmp — только на
    // старте, спека §4.7/канон 04 §6).
    public Task RunCleanupAsync(CancellationToken ct)
    {
        CleanAged();
        CleanupAbandonedUploads();
        return Task.CompletedTask;
    }

    // volume.json валиден + touch-проба записи в tmp (создать+удалить файл).
    public bool CheckHealth()
    {
        try
        {
            ValidateVolumeJson(ConfigVolumeJsonPath);
            var probe = Path.Combine(TmpDir, "healthz-" + Guid.NewGuid().ToString("N"));
            File.WriteAllText(probe, string.Empty);
            File.Delete(probe);
            return true;
        }
        catch (Exception ex)
        {
            logger?.LogWarning(ex, "Проба здоровья тома {Root} не прошла", Root);
            return false;
        }
    }

    // rename пути в .trash/<guid> (создаёт .trash при отсутствии; цели-имена
    // уникальны — повторные удаления одного ключа не конфликтуют, канон 04 §1).
    internal void MoveToTrash(string path)
    {
        Directory.CreateDirectory(TrashDir);
        var target = Path.Combine(TrashDir, Guid.NewGuid().ToString("N"));
        if (File.Exists(path))
        {
            File.Move(path, target);
            return;
        }
        Directory.Move(path, target);
    }

    // Пустой том = нет записей кроме (возможно пустого) .owns3.sys: создание
    // volume.json поверх чужих данных запрещено (fail-fast, канон 04 §3).
    private void EnsureVolumeEmpty()
    {
        foreach (var entry in Directory.EnumerateFileSystemEntries(Root))
        {
            if (string.Equals(Path.GetFileName(entry), SysDirName, StringComparison.Ordinal))
            {
                if (Directory.EnumerateFileSystemEntries(entry).Any())
                    throw new InvalidOperationException(
                        $"Том {Root}: volume.json отсутствует, но .owns3.sys не пуст — отказ старта");
                continue;
            }
            throw new InvalidOperationException(
                $"Том {Root}: volume.json отсутствует при непустом томе — отказ старта");
        }
    }

    private void ValidateVolumeJson(string volumePath)
    {
        VolumeInfo info;
        try
        {
            info = JsonSerializer.Deserialize<VolumeInfo>(File.ReadAllText(volumePath), JsonOptions)
                ?? throw new InvalidOperationException($"Том {Root}: volume.json пуст");
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException($"Том {Root}: volume.json не читается — {ex.Message}");
        }
        if (info.Magic != Magic)
            throw new InvalidOperationException($"Том {Root}: чужой magic «{info.Magic}» — отказ старта");
        if (info.FormatVersion > CurrentFormatVersion)
            throw new InvalidOperationException(
                $"Том {Root}: версия формата {info.FormatVersion} новее поддерживаемой — отказ старта");
        if (info.Mode != Mode)
            throw new InvalidOperationException($"Том {Root}: режим «{info.Mode}» не «{Mode}» — отказ старта");
    }

    // Стартовая безусловная очистка tmp (канон 04 §6: это незакоммиченный
    // staging — при старте процесса in-flight PUT быть не может).
    private void WipeTmp()
    {
        if (!Directory.Exists(TmpDir))
            return;
        foreach (var entry in Directory.EnumerateFileSystemEntries(TmpDir))
            DeleteRecursive(entry);
    }

    // Возрастные чистки (старт и фон — одни и те же пороги, канон 04 §6).
    private void CleanAged()
    {
        // .trash/*: содержимое никогда не читается клиентами — только возраст
        if (Directory.Exists(TrashDir))
            foreach (var entry in Directory.EnumerateFileSystemEntries(TrashDir))
                if (IsAged(entry))
                    DeleteRecursive(entry);

        // orphan-dataDir: Guid-подкаталоги в каталогах объектов, не упомянутые
        // текущим xl.meta (остатки прерванных перезаписей/Delete)
        if (Directory.Exists(Root))
            foreach (var bucketDir in Directory.EnumerateDirectories(Root))
            {
                if (string.Equals(Path.GetFileName(bucketDir), SysDirName, StringComparison.Ordinal))
                    continue;
                CleanupOrphansInSubtree(bucketDir);
            }
    }

    private void CleanupOrphansInSubtree(string directory)
    {
        // Текущий dataDir — из валидного xl.meta этого каталога; отсутствие или
        // битость НЕ блокирует чистку его Guid-подкаталогов (orphan после краша
        // Delete: xl.meta уже в .trash, dataDir-каталог остался).
        string? currentDataDir = null;
        var metaPath = Path.Combine(directory, "xl.meta");
        if (File.Exists(metaPath))
        {
            try
            {
                currentDataDir = XlMetaFile.Read(directory, out _).DataDirName;
            }
            catch (XlIntegrityException ex)
            {
                logger?.LogWarning(ex, "Битый xl.meta в {Directory}: файл не тронут, Guid-подкаталоги чистятся по возрасту", directory);
            }
        }
        foreach (var sub in Directory.EnumerateDirectories(directory))
        {
            var name = Path.GetFileName(sub);
            // Guid-имя БЕЗ xl.meta внутри — dataDir-кандидат (в dataDir лежит
            // лишь part.N). Guid-имя С xl.meta — это каталог объекта с 32-hex
            // ключом-сегментом (валидный ключ, Guid.TryParseExact("N")) — живой
            // объект: рекурсивный обход, никакой чистки самого каталога.
            var looksLikeDataDir = Guid.TryParseExact(name, "N", out _) && !File.Exists(Path.Combine(sub, "xl.meta"));
            if (!looksLikeDataDir)
            {
                CleanupOrphansInSubtree(sub);
                continue;
            }
            if (name == currentDataDir)
                continue;
            if (IsAged(sub))
                MoveToTrash(sub);
        }
    }

    private bool IsAged(string path) => IsAged(path, AgeThreshold);

    // Обобщение на произвольный порог (чистки .trash/orphan — 1 ч, загрузки — 24 ч).
    private bool IsAged(string path, TimeSpan threshold) =>
        timeProvider.GetUtcNow() - File.GetLastWriteTimeUtc(path) > threshold;

    // Брошенные загрузки — 24 ч от initiation (канон 04 §5–6): чистится стартом и
    // каждым фоновым проходом. Без _commitLock XlObjectStore: порог 24 ч исключает
    // коллизию с живой загрузкой; гонка с Complete может оставить «призрачную» запись
    // (каталога уже нет) — она не разрешается как загрузка и уходит следующим проходом.
    private void CleanupAbandonedUploads()
    {
        if (!Directory.Exists(MultipartDir))
            return;
        var nowMs = timeProvider.GetUtcNow().ToUnixTimeMilliseconds();
        foreach (var keyDir in Directory.EnumerateDirectories(MultipartDir))
        {
            var uploadsPath = MultipartJournals.UploadsJsonPath(keyDir);
            List<MultipartJournals.UploadJournalEntry> uploads;
            try
            {
                uploads = MultipartJournals.ReadUploads(uploadsPath);
            }
            catch (JsonException ex)
            {
                // Битый журнал (спека §4.1): warning; записи недоступны — только mtime-прокси
                logger?.LogWarning(ex, "Битый uploads.json в {KeyDir}: записи не читаются, каталоги чистятся по mtime", keyDir);
                uploads = [];
            }
            var kept = new List<MultipartJournals.UploadJournalEntry>();
            foreach (var entry in uploads)
            {
                if (nowMs - entry.InitiatedMs > AbandonedUploadThreshold.TotalMilliseconds)
                    TrashIfExists(MultipartJournals.UploadDirPath(keyDir, entry.UploadId));
                else
                    kept.Add(entry);
            }
            // Каталоги без живой записи (журнал бит/утрачен/запись удалена): mtime-прокси
            foreach (var uploadDir in Directory.EnumerateDirectories(keyDir))
                if (kept.All(e => e.UploadId != Path.GetFileName(uploadDir))
                    && IsAged(uploadDir, AbandonedUploadThreshold))
                    MoveToTrash(uploadDir);
            if (kept.Count != uploads.Count)
                MultipartJournals.WriteUploads(uploadsPath, kept);
            // Опустевший sha-каталог (журнал пуст И каталогов загрузок нет) удаляется
            if (kept.Count == 0 && !Directory.EnumerateDirectories(keyDir).Any())
                Directory.Delete(keyDir, recursive: true); // в нём только uploads.json
        }
    }

    private void TrashIfExists(string path)
    {
        if (Directory.Exists(path))
            MoveToTrash(path);
    }

    private static void DeleteRecursive(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
            return;
        }
        Directory.Delete(path, recursive: true);
    }
}
