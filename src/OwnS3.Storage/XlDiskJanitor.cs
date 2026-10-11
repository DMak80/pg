using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace OwnS3.Storage;

// Джанитор корня диска (канон 04 §6; EC-дополнения — 06 §10): переиспользуемая
// механика чисток одного корня (single-том и каждый диск сета EC).
// Пороги: tmp — только на старте; .trash/ и orphan-dataDir — 1 ч; брошенные
// multipart-загрузки — 24 ч. Фантом-записи EC — локальная кворумная сверка
// отсутствия (канон 06 §10 в правке Task 0).
public sealed class XlDiskJanitor(string root, TimeProvider timeProvider, ILogger? logger = null)
{
    private const string SysDirName = ".owns3.sys";
    private static readonly TimeSpan AgeThreshold = TimeSpan.FromHours(1);
    private static readonly TimeSpan AbandonedUploadThreshold = TimeSpan.FromHours(24);

    private string TrashDir => Path.Combine(root, SysDirName, ".trash");
    private string TmpDir => Path.Combine(root, SysDirName, "tmp");
    private string MultipartDir => Path.Combine(root, SysDirName, "multipart");

    // Стартовая безусловная очистка tmp (канон 04 §6: незакоммиченный staging —
    // при старте процесса in-flight операций быть не может).
    public void WipeTmp()
    {
        if (!Directory.Exists(TmpDir))
            return;
        foreach (var entry in Directory.EnumerateFileSystemEntries(TmpDir))
            DeleteRecursive(entry);
    }

    // Возрастные чистки (старт и фон — одни и те же пороги): .trash/* — только
    // возраст; orphan-dataDir — Guid-подкаталоги, не упомянутые локальной xl.meta.
    public void CleanAged()
    {
        if (Directory.Exists(TrashDir))
            foreach (var entry in Directory.EnumerateFileSystemEntries(TrashDir))
                if (IsAged(entry))
                    DeleteRecursive(entry);

        if (!Directory.Exists(root))
            return;
        foreach (var bucketDir in Directory.EnumerateDirectories(root))
        {
            if (string.Equals(Path.GetFileName(bucketDir), SysDirName, StringComparison.Ordinal))
                continue;
            CleanupOrphansInSubtree(bucketDir);
        }
    }

    // Брошенные загрузки — 24 ч от initiation: чистится стартом и каждым фоновым
    // проходом (механика XlVolume t37 без изменений).
    public void CleanupAbandonedUploads()
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

    // Фантом-записи EC (канон 06 §10 в правке Task 0): ключ с локально валидной
    // xl.meta, не актуальный в консенсусе сета. Локальная сверка: xl.meta того же
    // ключа читается на других онлайн-корнях сета (внутрипроцессно); запись
    // отсутствует на ≥ k корнях И локальная старше 1 ч (mtime xl.meta) → каталог
    // ключа rename в .trash; кворум отсутствия недостижим → консервативно сохранить.
    public void CleanPhantoms(IReadOnlyList<string> peerRoots, int k)
    {
        if (!Directory.Exists(root))
            return;
        foreach (var bucketDir in Directory.EnumerateDirectories(root))
        {
            if (string.Equals(Path.GetFileName(bucketDir), SysDirName, StringComparison.Ordinal))
                continue;
            foreach (var keyDir in EnumerateObjectDirs(bucketDir))
                CleanPhantomKey(keyDir, bucketDir, peerRoots, k);
        }
    }

    // Каталоги объектов поддерева: содержат xl.meta (сами, не dataDir-Guid).
    private static IEnumerable<string> EnumerateObjectDirs(string directory)
    {
        foreach (var dir in Directory.EnumerateDirectories(directory))
        {
            if (File.Exists(Path.Combine(dir, "xl.meta")))
                yield return dir;
            else
                foreach (var sub in EnumerateObjectDirs(dir))
                    yield return sub;
        }
    }

    private void CleanPhantomKey(string keyDir, string bucketDir, IReadOnlyList<string> peerRoots, int k)
    {
        var metaPath = Path.Combine(keyDir, "xl.meta");
        if (!File.Exists(metaPath) || !IsAged(metaPath))
            return; // локально валидна и молода — консервативно не трогаем

        // локальная запись должна читаться (битая — не фантом, её лечит heal)
        try
        {
            _ = XlMetaFile.ReadFile(metaPath);
        }
        catch (XlIntegrityException)
        {
            return;
        }

        // сверка отсутствия: тот же относительный путь ключа на онлайн-корнях сета
        var relative = Path.GetRelativePath(root, keyDir);
        var absent = 0;
        foreach (var peer in peerRoots)
        {
            if (string.Equals(peer, root, StringComparison.Ordinal))
                continue;
            var peerMeta = Path.Combine(peer, relative, "xl.meta");
            if (!File.Exists(peerMeta))
                absent++;
        }

        if (absent >= k)
        {
            logger?.LogInformation("Фантом-запись {KeyDir} отсутствует на {Absent} ≥ k={K} корнях сета — в .trash",
                keyDir, absent, k);
            MoveToTrash(keyDir);
        }
        // кворум отсутствия недостижим — ключ консервативно сохраняется
    }

    // ── внутренняя механика (перенос из XlVolume без изменений поведения) ──

    // Текущий dataDir — из валидного xl.meta этого каталога; отсутствие или
    // битость НЕ блокирует чистку его Guid-подкаталогов (orphan после краха
    // Delete: xl.meta уже в .trash, dataDir-каталог остался).
    private void CleanupOrphansInSubtree(string directory)
    {
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
            // Guid-имя БЕЗ xl.meta внутри — dataDir-кандидат (в dataDir лежит лишь
            // part.N). Guid-имя С xl.meta — каталог объекта с 32-hex ключом-сегментом
            // — живой объект: рекурсивный обход, никакой чистки самого каталога.
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

    private bool IsAged(string path, TimeSpan threshold) =>
        timeProvider.GetUtcNow() - File.GetLastWriteTimeUtc(path) > threshold;

    private void TrashIfExists(string path)
    {
        if (Directory.Exists(path))
            MoveToTrash(path);
    }

    // rename пути в .trash/<guid> (цели-имена уникальны — повторные удаления
    // одного ключа не конфликтуют, канон 04 §1).
    private void MoveToTrash(string path)
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
