using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using AdminPanel.Etcd.Client;

namespace AdminPanel.Etcd.Writing;

// Ошибка валидации одного поля (ProblemDetails errors; дубль pg-объявления —
// pg-часть Writing удалена, kafka уйдёт от него в Task 14).
public sealed record ValidationError(string Field, string Message);

// Тело POST /api/kafka/clusters (arch/02 §10.3; arch/03 §7.2): nullable-поля с
// дефолтами 3/3/2/12/7д/2/2/20 — биндится Minimal API как JSON (camelCase).
public sealed record CreateKafkaClusterRequest(
    string? Name,
    int? Brokers = null,
    int? ReplicationFactor = null,
    int? MinInSyncReplicas = null,
    int? DefaultPartitions = null,
    long? DefaultRetentionMs = null,
    decimal? Cpu = null,
    int? MemGi = null,
    int? DiskGi = null);

// Тело PUT /api/kafka/clusters/{c}/config — хотя бы одно поле (arch/02 §10.2-3).
public sealed record KafkaConfigUpdateRequest(
    int? ReplicationFactor = null,
    int? MinInSyncReplicas = null,
    int? DefaultPartitions = null,
    long? DefaultRetentionMs = null);

// Тело POST /api/kafka/clusters/{c}/brokers — ресурсы нового брокера (arch/02 §10.2-4).
public sealed record AddKafkaBrokerRequest(
    decimal? Cpu = null,
    int? MemGi = null,
    int? DiskGi = null);

// Границы kafka-мутаций — arch/02 §10.3 (константы кода, не конфиг).
public static partial class KafkaLimits
{
    // Как pg-имена: без дефиса (arch/15).
    [GeneratedRegex("^[a-z][a-z0-9_]{0,62}$")]
    public static partial Regex ClusterPattern();

    // Топик: Kafka-паттерн без __-префикса (arch/15; internal-топики вне реестра).
    [GeneratedRegex("^[a-zA-Z0-9._-]{1,249}$")]
    public static partial Regex TopicPattern();

    public const int MinBrokers = 1;
    public const int MaxBrokers = 9;
    public const int MinRf = 1;
    public const int MaxRf = 9;
    public const int MinPartitions = 1;
    public const int MaxPartitions = 1000;
    public const long MinRetentionMs = 1;
    public const long MaxRetentionMs = 2147483647;
    public const decimal MinCpu = 0.01m;
    public const decimal MaxCpu = 64m;
    public const int MinGiB = 1;
    public const int MaxGiB = 65536;

    public const int DefBrokers = 3;
    public const int DefRf = 3;
    public const int DefMinIsr = 2;
    public const int DefPartitions = 12;
    public const long DefRetentionMs = 604800000; // 7 дней
    public const decimal DefCpu = 2m;
    public const int DefMemGi = 2;
    public const int DefDiskGi = 20;

    // Internal-топики Kafka в реестр не попадают (arch/15).
    public static bool IsInternalTopic(string topic)
        => topic.StartsWith("__", StringComparison.Ordinal);
}

// Чистая функция валидации создания: сервер — источник истины (arch/02 §10.3).
public static class KafkaCreateValidator
{
    public static IReadOnlyList<ValidationError> Validate(CreateKafkaClusterRequest request)
    {
        var errors = new List<ValidationError>();
        if (!KafkaLimits.ClusterPattern().IsMatch(request.Name ?? ""))
            errors.Add(new("name", "имя: ^[a-z][a-z0-9_]{0,62}$"));

        Range(request.Brokers ?? KafkaLimits.DefBrokers, KafkaLimits.MinBrokers, KafkaLimits.MaxBrokers,
            "brokers", $"брокеры: целое {KafkaLimits.MinBrokers}..{KafkaLimits.MaxBrokers} (по умолчанию 3)", errors);
        var rf = Range(request.ReplicationFactor ?? KafkaLimits.DefRf, KafkaLimits.MinRf, KafkaLimits.MaxRf,
            "replicationFactor", $"replicationFactor: целое {KafkaLimits.MinRf}..{KafkaLimits.MaxRf} ≤ brokers", errors);
        if ((request.Brokers ?? KafkaLimits.DefBrokers) < rf)
            errors.Add(new("replicationFactor", "replicationFactor не может превышать brokers"));

        var minIsr = request.MinInSyncReplicas ?? KafkaLimits.DefMinIsr;
        if (minIsr < 1 || minIsr > rf)
            errors.Add(new("minInSyncReplicas", $"minInSyncReplicas: целое 1..replicationFactor (={rf})"));

        Range(request.DefaultPartitions ?? KafkaLimits.DefPartitions, KafkaLimits.MinPartitions, KafkaLimits.MaxPartitions,
            "defaultPartitions", $"defaultPartitions: целое {KafkaLimits.MinPartitions}..{KafkaLimits.MaxPartitions}", errors);
        Range(request.DefaultRetentionMs ?? KafkaLimits.DefRetentionMs, KafkaLimits.MinRetentionMs, KafkaLimits.MaxRetentionMs,
            "defaultRetentionMs", $"defaultRetentionMs: {KafkaLimits.MinRetentionMs}..{KafkaLimits.MaxRetentionMs}", errors);

        var cpu = request.Cpu ?? KafkaLimits.DefCpu;
        if (cpu < KafkaLimits.MinCpu || cpu > KafkaLimits.MaxCpu)
            errors.Add(new("cpu", $"cpu: {KafkaLimits.MinCpu}..{KafkaLimits.MaxCpu} ядер"));
        GiB(request.MemGi ?? KafkaLimits.DefMemGi, "memGi", errors);
        GiB(request.DiskGi ?? KafkaLimits.DefDiskGi, "diskGi", errors);
        return errors;
    }

    // Валидация полей config-мутации (границы те же; межполевая проверка minISR ≤ RF
    // выполняется командой на АКТУАЛЬНОМ config — rf может не меняться в запросе).
    public static IReadOnlyList<ValidationError> ValidateUpdate(
        KafkaConfigUpdateRequest request, int currentRf, int currentMinIsr)
    {
        var errors = new List<ValidationError>();
        if (request.ReplicationFactor is null && request.MinInSyncReplicas is null
            && request.DefaultPartitions is null && request.DefaultRetentionMs is null)
            errors.Add(new("", "хотя бы одно поле обновления обязательно"));

        if (request.ReplicationFactor is { } rf && (rf < KafkaLimits.MinRf || rf > KafkaLimits.MaxRf))
            errors.Add(new("replicationFactor", $"replicationFactor: целое {KafkaLimits.MinRf}..{KafkaLimits.MaxRf}"));
        if (request.MinInSyncReplicas is { } isr && isr < 1)
            errors.Add(new("minInSyncReplicas", "minInSyncReplicas: целое ≥ 1"));

        if (request.DefaultPartitions is { } p && (p < KafkaLimits.MinPartitions || p > KafkaLimits.MaxPartitions))
            errors.Add(new("defaultPartitions", $"defaultPartitions: {KafkaLimits.MinPartitions}..{KafkaLimits.MaxPartitions}"));
        if (request.DefaultRetentionMs is { } r && (r < KafkaLimits.MinRetentionMs || r > KafkaLimits.MaxRetentionMs))
            errors.Add(new("defaultRetentionMs", $"defaultRetentionMs: {KafkaLimits.MinRetentionMs}..{KafkaLimits.MaxRetentionMs}"));

        // Межполевая валидация на эффективных значениях (новое ?? текущее).
        var effectiveRf = request.ReplicationFactor ?? currentRf;
        var effectiveIsr = request.MinInSyncReplicas ?? currentMinIsr;
        if (effectiveIsr > effectiveRf)
            errors.Add(new("minInSyncReplicas", $"minInSyncReplicas ({effectiveIsr}) не может превышать replicationFactor ({effectiveRf})"));
        return errors;
    }

    private static int Range(int value, int min, int max, string field, string message, List<ValidationError> errors)
    {
        if (value < min || value > max)
            errors.Add(new(field, message));
        return value;
    }

    private static long Range(long value, long min, long max, string field, string message, List<ValidationError> errors)
    {
        if (value < min || value > max)
            errors.Add(new(field, message));
        return value;
    }

    private static void GiB(int value, string field, List<ValidationError> errors)
    {
        if (value < KafkaLimits.MinGiB || value > KafkaLimits.MaxGiB)
            errors.Add(new(field, $"{field}: целое {KafkaLimits.MinGiB}..{KafkaLimits.MaxGiB} GiB"));
    }
}

// План ключей создания kafka-кластера (arch/02 §10.2-1): чистая функция.
public sealed record KafkaClusterCreatePlan(
    string ConfigKey,
    string ConfigValue,
    IReadOnlyList<KvPut> Puts,
    string CanonicalCpu,
    string CanonicalMem,
    string CanonicalDisk)
{
    public const string NotInitialized = "NOT_INITIALIZED";

    public static KafkaClusterCreatePlan Build(CreateKafkaClusterRequest request, long nowUnix)
    {
        var brokers = request.Brokers ?? KafkaLimits.DefBrokers;
        var cpu = Canonical(request.Cpu ?? KafkaLimits.DefCpu);
        var mem = $"{request.MemGi ?? KafkaLimits.DefMemGi}Gi";
        var disk = $"{request.DiskGi ?? KafkaLimits.DefDiskGi}Gi";

        // config-JSON: канон snake_case arch/15 §2.1, state только у заявки.
        var config = JsonSerializer.Serialize(new KafkaConfigJson(
            brokers,
            request.ReplicationFactor ?? KafkaLimits.DefRf,
            request.MinInSyncReplicas ?? KafkaLimits.DefMinIsr,
            request.DefaultPartitions ?? KafkaLimits.DefPartitions,
            request.DefaultRetentionMs ?? KafkaLimits.DefRetentionMs,
            nowUnix,
            NotInitialized));

        // Пакет PUT: state + resources на КАЖДЫЙ брокер (arch/02 §10.2-1 п.2),
        // абсолютные ключи.
        var puts = new List<KvPut>();
        for (var k = 1; k <= brokers; k++)
        {
            puts.Add(new($"/kafka/clusters/{request.Name}/brokers/broker{k}/state", NotInitialized));
            puts.Add(new($"/kafka/clusters/{request.Name}/brokers/broker{k}/resources",
                JsonSerializer.Serialize(new ResourcesJson(cpu, mem, disk))));
        }

        puts.Sort((a, b) => string.CompareOrdinal(a.Key, b.Key)); // детерминированный порядок
        return new KafkaClusterCreatePlan(
            $"/kafka/clusters/{request.Name}/config", config, puts, cpu, mem, disk);
    }

    // Каноническая decimal-строка invariant ("2", "0.5") — образец pg §9.3.
    public static string Canonical(decimal value)
        => value.ToString("0.#########", CultureInfo.InvariantCulture);
}

// Канонический JSON config-ключа (arch/15 §2): поля snake_case; state — только у заявок.
public sealed record KafkaConfigJson(
    [property: JsonPropertyName("brokers")] int Brokers,
    [property: JsonPropertyName("replication_factor")] int ReplicationFactor,
    [property: JsonPropertyName("min_insync_replicas")] int MinInSyncReplicas,
    [property: JsonPropertyName("default_partitions")] int DefaultPartitions,
    [property: JsonPropertyName("default_retention_ms")] long DefaultRetentionMs,
    [property: JsonPropertyName("created_unix")] long? CreatedUnix,
    [property: JsonPropertyName("state")] string? State)
{
    private static readonly JsonSerializerOptions Json = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    // Толерантный разбор значения config (битый JSON → null).
    public static KafkaConfigJson? TryParse(string raw)
    {
        try
        {
            return JsonSerializer.Deserialize<KafkaConfigJson>(raw, Json);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    public string Serialize() => JsonSerializer.Serialize(this, Json);

    // Перезапись mutable-полей (config-мутация; state/created_unix сохраняются).
    public KafkaConfigJson With(KafkaConfigUpdateRequest update) => new(
        Brokers,
        update.ReplicationFactor ?? ReplicationFactor,
        update.MinInSyncReplicas ?? MinInSyncReplicas,
        update.DefaultPartitions ?? DefaultPartitions,
        update.DefaultRetentionMs ?? DefaultRetentionMs,
        CreatedUnix,
        State);

    // Перевод в TO_REMOVE (удаление кластера): поля сохраняются (arch/02 §10.2-2).
    public KafkaConfigJson WithState(string state) => this with { State = state };
}

// JSON заявки ресурсов брокера: {"cpu":"2","mem":"4Gi","disk":"40Gi"} (arch/15 §2).
public sealed record ResourcesJson(
    [property: JsonPropertyName("cpu")] string Cpu,
    [property: JsonPropertyName("mem")] string Mem,
    [property: JsonPropertyName("disk")] string Disk);

// JSON заявки ротации /kafkaworker/rotations/<C> (arch/15 §4).
public sealed record KafkaRotationTicketJson(
    [property: JsonPropertyName("requested_unix")] long RequestedUnix,
    [property: JsonPropertyName("requested_by")] string RequestedBy)
{
    public string Serialize()
        => JsonSerializer.Serialize(this, new JsonSerializerOptions
        {
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        });
}

// Тело PUT /api/kafka/clusters/{c}/topics/{t} — конфиг-заявка топика (arch/02
// §10.2-7; управляемые поля §3.2): хотя бы одно поле; partitions — только
// увеличение (сверяется с фактом ключа в хендлере).
public sealed record TopicDesiredRequest(
    int? Partitions = null,
    long? RetentionMs = null,
    int? MinInSyncReplicas = null);

// desired-часть ключа topics/<T> (arch/15 §3).
public sealed record KafkaTopicDesiredJson(
    [property: JsonPropertyName("partitions")] int? Partitions,
    [property: JsonPropertyName("configs")] Dictionary<string, string>? Configs);

// Значение заявки topics/<T>/desired.create (arch/15 §3.1).
public sealed record TopicLifecycleCreateJson(
    [property: JsonPropertyName("partitions")] int Partitions,
    [property: JsonPropertyName("replication_factor")] int ReplicationFactor,
    [property: JsonPropertyName("configs")] Dictionary<string, string>? Configs,
    [property: JsonPropertyName("requested_unix")] long RequestedUnix,
    [property: JsonPropertyName("requested_by")] string RequestedBy)
{
    public string Serialize() => JsonSerializer.Serialize(this, new JsonSerializerOptions
    { DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull });
}

// Значение заявки topics/<T>/desired.delete (arch/15 §3.1).
public sealed record TopicLifecycleDeleteJson(
    [property: JsonPropertyName("requested_unix")] long RequestedUnix,
    [property: JsonPropertyName("requested_by")] string RequestedBy)
{
    public string Serialize() => JsonSerializer.Serialize(this);
}

// Тело POST /api/kafka/clusters/{c}/topics (arch/02 §10.2-9): name обязателен;
// partitions/RF дефолтятся из config кластера; retention/minISR опциональны.
public sealed record CreateTopicRequest(
    string? Name,
    int? Partitions = null,
    short? ReplicationFactor = null,
    long? RetentionMs = null,
    short? MinInSyncReplicas = null);

// Чистая валидация создания топика (arch/02 §10.2-9 / §10.3) на эффективных
// значениях (дефолты config кластера); отдельный класс — прецедент KafkaCreateValidator.
public static class KafkaTopicCreateValidator
{
    public static IReadOnlyList<ValidationError> Validate(CreateTopicRequest request, KafkaConfigJson config)
    {
        var errors = new List<ValidationError>();
        if (!KafkaLimits.TopicPattern().IsMatch(request.Name ?? "") || KafkaLimits.IsInternalTopic(request.Name ?? ""))
            errors.Add(new("name", "имя: ^[a-zA-Z0-9._-]{1,249}$ без __-префикса"));

        var partitions = request.Partitions ?? config.DefaultPartitions;
        if (partitions < KafkaLimits.MinPartitions || partitions > KafkaLimits.MaxPartitions)
            errors.Add(new("partitions", $"partitions: целое {KafkaLimits.MinPartitions}..{KafkaLimits.MaxPartitions}"));

        var rf = request.ReplicationFactor ?? (short)config.ReplicationFactor;
        if (rf < KafkaLimits.MinRf || rf > config.Brokers)
            errors.Add(new("replicationFactor", $"replicationFactor: целое {KafkaLimits.MinRf}..{KafkaLimits.MaxRf} и ≤ brokers ({config.Brokers})"));

        if (request.RetentionMs is { } r && (r < KafkaLimits.MinRetentionMs || r > KafkaLimits.MaxRetentionMs))
            errors.Add(new("retentionMs", $"retentionMs: {KafkaLimits.MinRetentionMs}..{KafkaLimits.MaxRetentionMs}"));

        if (request.MinInSyncReplicas is { } isr && (isr < 1 || isr > rf))
            errors.Add(new("minInSyncReplicas", $"minInSyncReplicas: целое 1..replicationFactor (={rf})"));

        return errors;
    }
}

// Построение канонической create-заявки (arch/02 §10.2-9): развёртка дефолтов
// config кластера; прецедент — KafkaClusterCreatePlan.Build.
public static class KafkaTopicCreatePlan
{
    public static TopicLifecycleCreateJson Build(
        CreateTopicRequest request, KafkaConfigJson config, long nowUnix, string by)
    {
        Dictionary<string, string>? configs = null;
        if (request.RetentionMs is not null || request.MinInSyncReplicas is not null)
        {
            configs = new Dictionary<string, string>();
            if (request.RetentionMs is { } r)
                configs["retention.ms"] = r.ToString(CultureInfo.InvariantCulture);
            if (request.MinInSyncReplicas is { } isr)
                configs["min.insync.replicas"] = isr.ToString(CultureInfo.InvariantCulture);
        }

        return new TopicLifecycleCreateJson(
            request.Partitions ?? config.DefaultPartitions,
            request.ReplicationFactor ?? (short)config.ReplicationFactor,
            configs, nowUnix, by);
    }
}

// Значение ключа topics/<T> (arch/15 §3): факт (partitions/RF/configs/
// synced_unix) + заявка desired + missing. Толерантный разбор/каноническая
// запись — панель меняет ТОЛЬКО desired-поля (RMW), факт — территория воркера.
public sealed record KafkaTopicKeyJson(
    [property: JsonPropertyName("partitions")] int Partitions,
    [property: JsonPropertyName("replication_factor")] int? ReplicationFactor,
    [property: JsonPropertyName("configs")] Dictionary<string, string>? Configs,
    [property: JsonPropertyName("desired")] KafkaTopicDesiredJson? Desired,
    [property: JsonPropertyName("desired_unix")] long? DesiredUnix,
    [property: JsonPropertyName("desired_by")] string? DesiredBy,
    [property: JsonPropertyName("synced_unix")] long? SyncedUnix,
    [property: JsonPropertyName("missing")] bool Missing)
{
    private static readonly JsonSerializerOptions Json = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    // Толерантный разбор значения ключа (битый JSON → null → 503 хендлером).
    public static KafkaTopicKeyJson? TryParse(string raw)
    {
        try
        {
            return JsonSerializer.Deserialize<KafkaTopicKeyJson>(raw, Json);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    public string Serialize() => JsonSerializer.Serialize(this, Json);

    // Постановка заявки: факт не трогаем, desired/desired_unix/desired_by.
    public KafkaTopicKeyJson WithDesired(KafkaTopicDesiredJson desired, long unix, string by)
        => this with { Desired = desired, DesiredUnix = unix, DesiredBy = by };

    // Отмена заявки: убрать desired-поля (desire=null → поля не пишутся).
    public KafkaTopicKeyJson WithoutDesired()
        => this with { Desired = null, DesiredUnix = null, DesiredBy = null };
}

// Чистые функции desired-мутаций топика (arch/02 §10.2-7/8): валидация тела
// против факта ключа + построение desired-JSON.
public static class KafkaTopicDesiredPlan
{
    // Валидация: хотя бы одно поле; границы; partitions — строго больше факта
    // (уменьшение Kafka не поддерживает — отсекает панель, spec §3.2).
    public static IReadOnlyList<ValidationError> Validate(TopicDesiredRequest request, int actualPartitions)
    {
        var errors = new List<ValidationError>();
        if (request.Partitions is null && request.RetentionMs is null && request.MinInSyncReplicas is null)
            errors.Add(new("", "хотя бы одно поле заявки обязательно"));

        if (request.Partitions is { } p && p <= actualPartitions)
            errors.Add(new("partitions",
                $"partitions: только увеличение (фактически {actualPartitions})"));
        if (request.Partitions is { } p2 && (p2 < KafkaLimits.MinPartitions || p2 > KafkaLimits.MaxPartitions))
            errors.Add(new("partitions",
                $"partitions: целое {KafkaLimits.MinPartitions}..{KafkaLimits.MaxPartitions}"));

        if (request.RetentionMs is { } r && (r < KafkaLimits.MinRetentionMs || r > KafkaLimits.MaxRetentionMs))
            errors.Add(new("retentionMs",
                $"retentionMs: {KafkaLimits.MinRetentionMs}..{KafkaLimits.MaxRetentionMs}"));

        if (request.MinInSyncReplicas is { } isr && isr < 1)
            errors.Add(new("minInSyncReplicas", "minInSyncReplicas: целое ≥ 1"));

        return errors;
    }

    // desired-JSON из запроса: только управляемые конфиги (§3.2).
    public static KafkaTopicDesiredJson Build(TopicDesiredRequest request)
    {
        Dictionary<string, string>? configs = null;
        if (request.RetentionMs is not null || request.MinInSyncReplicas is not null)
        {
            configs = new Dictionary<string, string>();
            if (request.RetentionMs is { } r)
                configs["retention.ms"] = r.ToString(CultureInfo.InvariantCulture);
            if (request.MinInSyncReplicas is { } isr)
                configs["min.insync.replicas"] = isr.ToString(CultureInfo.InvariantCulture);
        }

        return new KafkaTopicDesiredJson(request.Partitions, configs);
    }
}
