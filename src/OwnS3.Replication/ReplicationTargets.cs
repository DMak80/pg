using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace OwnS3.Replication;

// Сосед mesh (глава 07 §2): запись состава — и получатель наших уведомлений,
// и источник наших фетчей; endpoint — advertised (multi-host/гео).
public sealed record TargetRecord(string Name, string Endpoint, string AccessKey, string SecretKey);

// Состав mesh (глава 07 §2): источник истины — файл тома <sysDir>/targets.json
// (JSON camelCase, персист tmp+rename); env — только seed первого старта.
// Потокобезопасный снапшот; горячая замена — Replace (админ-грань, глава 05 §7).
public sealed class ReplicationTargets(string sysDir, ILogger<ReplicationTargets> logger)
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private readonly object _lock = new();
    private IReadOnlyList<TargetRecord> _snapshot = [];

    private string FilePath => Path.Combine(sysDir, "targets.json");

    // Текущий состав соседей (чтение без блокировки — заменяется атомарно).
    public IReadOnlyList<TargetRecord> Snapshot
    {
        get { lock (_lock) return _snapshot; }
    }

    // ≥1 сосед — репликация активна (глава 07 §2).
    public bool Active
    {
        get { lock (_lock) return _snapshot.Count > 0; }
    }

    // Событие замены состава — подписчик координатора (Task 12): перестройка исполнителей.
    public event Action<IReadOnlyList<TargetRecord>>? TargetsChanged;

    // Первый старт (файла нет): env-seed → targets.json; env пуст → файл не создаётся,
    // Active=false. Файл есть → env НЕ перечитывается (источник истины — файл).
    public static ReplicationTargets Load(string sysDir, IEnumerable<TargetOptions> seed,
        ILogger<ReplicationTargets> logger)
    {
        var targets = new ReplicationTargets(sysDir, logger);
        Directory.CreateDirectory(sysDir);
        if (File.Exists(targets.FilePath))
        {
            targets.LoadFromFile();
            return targets;
        }
        var seeded = seed.Select(t => new TargetRecord(t.Name, t.Endpoint, t.AccessKey, t.SecretKey)).ToList();
        Validate(seeded);
        if (seeded.Count > 0)
            targets.Persist(seeded);
        lock (targets._lock)
            targets._snapshot = seeded;
        return targets;
    }

    // Идемпотентная замена полного списка (глава 07 §2): персист tmp+rename,
    // снапшот заменяется атомарно, подписчики получают новый состав.
    public void Replace(IReadOnlyList<TargetRecord> targets)
    {
        Validate(targets);
        lock (_lock)
        {
            Persist(targets);
            _snapshot = targets.ToList().AsReadOnly();
        }
        logger.LogInformation("Состав mesh заменён: {Count} соседей ({Names})",
            targets.Count, string.Join(",", targets.Select(t => t.Name)));
        TargetsChanged?.Invoke(Snapshot);
    }

    // Уникальность имён соседей (глава 07 §2: имя — ключ курсоров/очередей/метрик).
    private static void Validate(IReadOnlyList<TargetRecord> targets)
    {
        var duplicates = targets.GroupBy(t => t.Name, StringComparer.Ordinal).Where(g => g.Count() > 1)
            .Select(g => g.Key).ToList();
        if (duplicates.Count > 0)
            throw new ArgumentException($"Дубликаты имён соседей mesh: {string.Join(",", duplicates)}",
                nameof(targets));
    }

    private void LoadFromFile()
    {
        var text = File.ReadAllText(FilePath);
        var records = JsonSerializer.Deserialize<List<TargetRecord>>(text, JsonOptions) ?? [];
        lock (_lock)
            _snapshot = records.AsReadOnly();
        logger.LogInformation("Состав mesh загружен из targets.json: {Count} соседей", records.Count);
    }

    // Персист атомарно (tmp + rename) — глава 07 §2.
    private void Persist(IReadOnlyList<TargetRecord> targets)
    {
        var tmp = FilePath + ".tmp";
        File.WriteAllText(tmp, JsonSerializer.Serialize(targets, JsonOptions));
        File.Move(tmp, FilePath, overwrite: true);
    }
}
