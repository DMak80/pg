using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace KafkaWorker.Core.Writing;

// Топиковые request-модели и планы декларативного контракта (task
// etcd-via-worker-api): перенос топиковой части AdminPanel.Etcd/Writing/
// KafkaWriting.cs + тел запросов lifecycle из панельного KafkaCommands.cs
// (панельный оригинал жил одним файлом; здесь — отдельный по смыслу домена).

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
// запись — API воркера меняет ТОЛЬКО desired-поля (RMW), факт — территория
// процессов воркера.
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
    // (уменьшение Kafka не поддерживает — отсекает API, spec §3.2).
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
