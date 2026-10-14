using System.Text.Json;
using System.Text.Json.Serialization;
using S3Worker.Provisioning.Processes;
using Shared.Core;
using Shared.Etcd.Client;

namespace S3Worker.App.Api.Operations;

// ============================================================
// Исключения API S3-домена (arch/24 §1.1; порт ValkeyExceptions):
// тексты ProblemDetails-маппинга ApiModule.
// ============================================================

// Все etcd-endpoint'ы недоступны — писать некуда — 503.
public sealed class EtcdWriteUnavailableException()
    : Exception("нет активного etcd-endpoint'а (etcd недоступен)");

// Кластер не найден — 404.
public sealed class S3ClusterNotFoundException(string cluster)
    : Exception($"S3-кластер {cluster} не найден");

// Кластер не Active (NOT_INITIALIZED/TO_REMOVE) — 409.
public sealed class S3ClusterNotActiveException(string cluster, string state)
    : Exception($"S3-кластер {cluster} не Active (state={state}) — операция отклонена");

// Валидация: 400 с errors по полям.
public sealed class S3ValidationException(IReadOnlyList<ValidationError> errors)
    : Exception("параметры некорректны")
{
    public IReadOnlyList<ValidationError> Errors { get; } = errors;
}

// Профиль секретов отсутствует в env воркера — 422 (перечень — ТОЛЬКО имена).
public sealed class S3SecretProfileUnavailableException(IReadOnlyList<string> names)
    : Exception($"профиль секретов не найден в env воркера; доступны: {string.Join(", ", names)}")
{
    public IReadOnlyList<string> Names { get; } = names;
}

// Кластер с таким именем уже существует — 409.
public sealed class S3ClusterAlreadyExistsException(string name)
    : Exception($"S3-кластер {name} уже существует");

// Нода кластера не найдена — 404.
public sealed class S3NodeNotFoundException(string cluster, string node)
    : Exception($"нода {node} S3-кластера {cluster} не найдена");

// Предел состава: 9 нод / последняя нода — 409.
public sealed class S3LastNodeException(string message) : Exception(message);

// Живая заявка ротации — 409.
public sealed class S3RotationAlreadyRequestedException(string cluster)
    : Exception($"ротация секретов {cluster} уже запрошена — дождитесь исполнения");

// RMW-compare проигран — повтор запроса клиентом.
public sealed class S3ConcurrentWriteException(string key)
    : Exception($"{key} изменился с момента чтения — повторите запрос");

// Ресурс/эндпоинт воркера не найден или выключен — 404 (напр., seed за флагом).
public sealed class WorkerApiNotFoundException(string message) : Exception(message);

// ============================================================
// DTO (camelCase JSON; spec §6).
// ============================================================

// Заявка размещения (arch/23 §4): минимум один атрибут.
public sealed record S3PlacementDto(
    [property: JsonPropertyName("dockerHost")] string? DockerHost,
    [property: JsonPropertyName("swarmNode")] string? SwarmNode,
    [property: JsonPropertyName("swarmLabels")] IReadOnlyDictionary<string, string>? SwarmLabels)
{
    public bool IsEmpty => string.IsNullOrWhiteSpace(DockerHost)
        && string.IsNullOrWhiteSpace(SwarmNode)
        && SwarmLabels is not { Count: > 0 };

    // Канонический JSON селектора (arch/23 §2 placement — только указанные).
    public string ToJson()
    {
        using var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            if (!string.IsNullOrWhiteSpace(DockerHost))
                writer.WriteString("docker_host", DockerHost);
            if (!string.IsNullOrWhiteSpace(SwarmNode))
                writer.WriteString("swarm_node", SwarmNode);
            if (SwarmLabels is { Count: > 0 })
            {
                writer.WriteStartObject("swarm_labels");
                foreach (var (key, value) in SwarmLabels.OrderBy(p => p.Key, StringComparer.Ordinal))
                    writer.WriteString(key, value);
                writer.WriteEndObject();
            }

            writer.WriteEndObject();
        }

        return System.Text.Encoding.UTF8.GetString(buffer.ToArray());
    }
}

// Ресурсы ноды (pg §9.3): cpu — decimal-строка; memGi/diskGi — целые.
public sealed record S3ResourcesDto(
    [property: JsonPropertyName("cpu")] decimal? Cpu,
    [property: JsonPropertyName("memGi")] int? MemGi,
    [property: JsonPropertyName("diskGi")] int? DiskGi);

public sealed record CreateS3ClusterRequest(
    [property: JsonPropertyName("name")] string? Name,
    [property: JsonPropertyName("platform")] string? Platform,
    [property: JsonPropertyName("nodes")] int? Nodes,
    [property: JsonPropertyName("secretProfile")] string? SecretProfile,
    [property: JsonPropertyName("placements")] IReadOnlyList<S3PlacementDto>? Placements,
    [property: JsonPropertyName("resources")] S3ResourcesDto? Resources);

public sealed record AddNodeRequest(
    [property: JsonPropertyName("placement")] S3PlacementDto? Placement,
    [property: JsonPropertyName("resources")] S3ResourcesDto? Resources);

public sealed record UpdateResourcesRequest(
    [property: JsonPropertyName("cpu")] decimal? Cpu,
    [property: JsonPropertyName("memGi")] int? MemGi,
    [property: JsonPropertyName("diskGi")] int? DiskGi);

public record ValidationError(string Field, string Message);

// ============================================================
// Лимиты/валидатор (spec §6; канон arch/23 §2).
// ============================================================

public static partial class S3Limits
{
    public const string Platform = "owns3";
    public const int MinNodes = 1;
    public const int MaxNodes = 9;
    public const decimal MinCpu = 0.01m;
    public const decimal MaxCpu = 64m;
    public const int MinGiB = 1;
    public const int MaxGiB = 65536;

    [System.Text.RegularExpressions.GeneratedRegex(@"^[a-z][a-z0-9_]{0,62}$")]
    public static partial System.Text.RegularExpressions.Regex ClusterPattern();

    public static string CanonicalCpu(decimal cpu)
        => cpu.ToString(System.Globalization.CultureInfo.InvariantCulture);

    // Ресурсы декларации → канонический JSON arch/23 §2 (строки).
    public static string ResourcesJson(S3ResourcesDto resources)
        => JsonSerializer.Serialize(new S3ResourcesJson(
            CanonicalCpu(resources.Cpu ?? 1m),
            $"{resources.MemGi ?? 1}Gi",
            $"{resources.DiskGi ?? 10}Gi"));
}

// Ресурсы в etcd: {"cpu":"2","mem":"4Gi","disk":"200Gi"} (arch/23 §2).
public sealed record S3ResourcesJson(
    [property: JsonPropertyName("cpu")] string Cpu,
    [property: JsonPropertyName("mem")] string Mem,
    [property: JsonPropertyName("disk")] string Disk);

// ============================================================
// Общие хелперы etcd-операций хендлеров (failover, ключи).
// ============================================================

public static class S3ApiHelpers
{
    public const string ConfigKeyFormat = "/s3/clusters/{0}/config";
    public const string NodesPrefixFormat = "/s3/clusters/{0}/nodes/";

    public static string ConfigKey(string cluster) => string.Format(ConfigKeyFormat, cluster);

    public static string NodeKey(string cluster, string node, string leaf)
        => $"/s3/clusters/{cluster}/nodes/{node}/{leaf}";

    public static string RotationKey(string cluster) => $"/s3worker/rotations/{cluster}";

    // Ключи mesh-деклараций и PKI — делегирование S3ProcessCommon (одна точка
    // истины формата с процессами; arch/23 §2).
    public static string LinksKey(string cluster) => S3ProcessCommon.LinksKey(cluster);

    public static string DenyListKey(string cluster) => S3ProcessCommon.DenyListKey(cluster);

    public static string NodePkiKey(string cluster, string node) => S3ProcessCommon.NodePkiKey(cluster, node);

    // Failover-вызов: первый успешный endpoint выигрывает.
    public static async Task<Result<T>> CallAsync<T>(
        string[] endpoints, Func<string, Task<Result<T>>> call)
    {
        Result<T> last = default;
        foreach (var endpoint in endpoints)
        {
            var result = await call(endpoint);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return Result<T>.Failed(last.Error
            ?? new EtcdWriteUnavailableException());
    }

    // Чтение config (null — ключа нет; 404-исключение решает вызывающий).
    public static Task<Result<Kv?>> GetConfigAsync(
        IEtcdGateway gateway, string[] endpoints, string cluster, CancellationToken ct)
        => CallAsync(endpoints, endpoint => gateway.GetAsync(endpoint, ConfigKey(cluster), ct));

    // Фактический состав нод кластера: имена node<k> из Range nodes/.
    public static async Task<Result<IReadOnlyList<string>>> ListNodeNamesAsync(
        IEtcdGateway gateway, string[] endpoints, string cluster, CancellationToken ct)
    {
        var range = await CallAsync(endpoints, endpoint =>
            gateway.RangeAsync(endpoint, $"/s3/clusters/{cluster}/nodes/", ct));
        if (!range.IsSuccess)
            return Result<IReadOnlyList<string>>.Failed(range.Error!);
        var names = range.Value
            .Select(kv => kv.Key.Split('/')[^2])
            .Where(n => n.StartsWith("node", StringComparison.Ordinal))
            .Distinct()
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToList();
        return Result<IReadOnlyList<string>>.Success(names);
    }
}
