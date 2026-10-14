using System.Security.Cryptography;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace OwnS3.Replication;

// Сосед mesh (глава 07 §2): запись состава двух видов (поле auth) — и получатель
// наших уведомлений, и источник наших фетчей; endpoint — advertised (multi-host/гео).
// legacy — симметричная SigV4-идентификация (пара кредов, глава 07 §6);
// key — асимметричная: исходящие подписываются приватным ключом identity,
// входящие от соседа верифицируются publicKeyPem записи (схема OWNS3-PEER).
// Auth обязателен; старый targets.json (без auth) читается как legacy.
public sealed record TargetRecord(string Name, string Endpoint, string Auth,
    string? AccessKey = null, string? SecretKey = null, string? PublicKeyPem = null)
{
    public const string AuthLegacy = "legacy";
    public const string AuthKey = "key";
}

// Состав mesh (глава 07 §2): источник истины — файл тома <sysDir>/targets.json
// (env — только seed первого старта; key-записи попадают в состав через PUT
// admin-грани или join, env-seed остаётся legacy-only). Потокобезопасный снапшот;
// горячая замена — Replace/AddOrReplace/Remove (глава 05 §7).
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
        var seeded = seed.Select(t => new TargetRecord(t.Name, t.Endpoint, TargetRecord.AuthLegacy,
            t.AccessKey, t.SecretKey)).ToList();
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

    // Добавление/обновление одного соседа (join, глава 07 §2): merge по имени,
    // персист tmp+rename, горячая замена — подписчики перестраиваются по событию.
    public void AddOrReplace(TargetRecord target)
    {
        lock (_lock)
        {
            var merged = _snapshot.Where(t => t.Name != target.Name).Append(target).ToList();
            Validate(merged);
            Persist(merged);
            _snapshot = merged.AsReadOnly();
        }
        logger.LogInformation("Состав mesh дополнен соседом {Name} (auth={Auth}): {Count} соседей",
            target.Name, target.Auth, Snapshot.Count);
        TargetsChanged?.Invoke(Snapshot);
    }

    // Удаление соседа (delete, глава 07 §2): true — имя было в составе; курсоры
    // не трогаются (возврат соседа продолжится с места); персист + событие.
    // Неизвестное имя — false без IO.
    public bool Remove(string name)
    {
        lock (_lock)
        {
            if (_snapshot.All(t => t.Name != name))
                return false;
            var remaining = _snapshot.Where(t => t.Name != name).ToList();
            Persist(remaining);
            _snapshot = remaining.AsReadOnly();
        }
        logger.LogInformation("Сосед {Name} удалён из состава mesh: {Count} соседей",
            name, Snapshot.Count);
        TargetsChanged?.Invoke(Snapshot);
        return true;
    }

    // Поиск записи по имени (резолв пира на replication-грани, глава 07 §6).
    public bool TryGet(string name, out TargetRecord target)
    {
        lock (_lock)
        {
            var found = _snapshot.FirstOrDefault(t => t.Name == name);
            target = found!; // null при отсутствии — контракт TryGet
            return found is not null;
        }
    }

    // Валидация состава (глава 07 §2): имена уникальны; legacy — пара кредов и
    // уникальность accessKey среди legacy-записей (accessKey — идентификатор
    // источника на приёмнике: дедуп (sourceName, seqNo) при общих кредах гасил бы
    // события второго отправителя); key — валидный PEM публичного ключа ECDSA
    // P-256; endpoint — scheme://host:port.
    private static void Validate(IReadOnlyList<TargetRecord> targets)
    {
        var nameDuplicates = DuplicatesOf(targets.Select(t => t.Name).ToList());
        if (nameDuplicates.Count > 0)
            throw new ArgumentException(
                $"Дубликаты имён соседей mesh: {string.Join(",", nameDuplicates)}", nameof(targets));

        foreach (var target in targets)
        {
            switch (target.Auth)
            {
                case TargetRecord.AuthLegacy:
                    if (string.IsNullOrEmpty(target.AccessKey) || string.IsNullOrEmpty(target.SecretKey))
                        throw new ArgumentException(
                            $"legacy-сосед {target.Name}: обязательна пара accessKey/secretKey",
                            nameof(targets));
                    break;
                case TargetRecord.AuthKey:
                    if (string.IsNullOrWhiteSpace(target.PublicKeyPem))
                        throw new ArgumentException(
                            $"key-сосед {target.Name}: обязателен publicKeyPem", nameof(targets));
                    try
                    {
                        using var key = ECDsa.Create();
                        key.ImportFromPem(target.PublicKeyPem); // SUBJECT PUBLIC KEY ECDSA P-256
                    }
                    catch (Exception ex)
                    {
                        throw new ArgumentException(
                            $"key-сосед {target.Name}: невалидный PEM публичного ключа ECDSA P-256 — {ex.Message}",
                            nameof(targets));
                    }
                    break;
                default:
                    throw new ArgumentException(
                        $"Сосед {target.Name}: неизвестный auth «{target.Auth}» (ожидалось legacy|key)",
                        nameof(targets));
            }
            if (!Uri.TryCreate(target.Endpoint, UriKind.Absolute, out var uri)
                || uri.Scheme is not ("http" or "https")
                || string.IsNullOrEmpty(uri.Host))
                throw new ArgumentException(
                    $"Невалидный endpoint соседа {target.Name}: {target.Endpoint} (ожидалось scheme://host:port)",
                    nameof(targets));
        }

        var accessKeyDuplicates = DuplicatesOf(targets
            .Where(t => t.Auth == TargetRecord.AuthLegacy && !string.IsNullOrEmpty(t.AccessKey))
            .Select(t => t.AccessKey!).ToList());
        if (accessKeyDuplicates.Count > 0)
            throw new ArgumentException(
                $"Дубликаты accessKey соседей mesh: {string.Join(",", accessKeyDuplicates)}", nameof(targets));
    }

    private static List<string> DuplicatesOf(List<string> values) =>
        values.GroupBy(v => v, StringComparer.Ordinal).Where(g => g.Count() > 1)
            .Select(g => g.Key).ToList();

    private void LoadFromFile()
    {
        var text = File.ReadAllText(FilePath);
        var records = (JsonSerializer.Deserialize<List<TargetRecordDto>>(text, JsonOptions) ?? [])
            .Select(FromDto).ToList();
        lock (_lock)
            _snapshot = records.AsReadOnly();
        logger.LogInformation("Состав mesh загружен из targets.json: {Count} соседей", records.Count);
    }

    // Персист атомарно (tmp + rename) — глава 07 §2.
    private void Persist(IReadOnlyList<TargetRecord> targets)
    {
        var tmp = FilePath + ".tmp";
        File.WriteAllText(tmp, JsonSerializer.Serialize(targets.Select(ToDto).ToList(), JsonOptions));
        File.Move(tmp, FilePath, overwrite: true);
    }

    // Дисковый формат записи (глава 07 §2): camelCase; записи без auth (старый
    // формат) читаются как legacy — обратная совместимость дискового формата.
    private sealed record TargetRecordDto(string Name, string Endpoint, string? Auth,
        string? AccessKey, string? SecretKey, string? PublicKeyPem);

    private static TargetRecord FromDto(TargetRecordDto dto) =>
        new(dto.Name, dto.Endpoint, dto.Auth ?? TargetRecord.AuthLegacy,
            dto.AccessKey, dto.SecretKey, dto.PublicKeyPem);

    private static TargetRecordDto ToDto(TargetRecord t) =>
        new(t.Name, t.Endpoint, t.Auth, t.AccessKey, t.SecretKey, t.PublicKeyPem);
}
