using System.Globalization;
using System.Text.RegularExpressions;
using ValkeyWorker.Core.Writing;
using System.Text.Json;
using Shared.Core;
using Shared.Core.Writing;
using Shared.Etcd.Client;

namespace ValkeyWorker.App.Api.Operations;

// Границы и правила валидации valkey-мутаций (arch/20 §2; pg §9.3 — форматы
// ресурсов: cpu decimal 0.01–64 без суффикса; mem/disk — только целые GiB).
public static partial class ValkeyLimits
{
    // Как pg/kafka-имена: без дефиса (arch/20 §2).
    [GeneratedRegex("^[a-z][a-z0-9_]{0,62}$")]
    public static partial Regex ClusterPattern();

    public const int MinNodes = 1;
    public const int MaxNodes = 1; // v1: standalone nodes=1 (реплики — roadmap)
    public const long MinMaxmemoryBytes = 1;
    public const long MaxMaxmemoryBytes = long.MaxValue;
    public const decimal MinCpu = 0.01m;
    public const decimal MaxCpu = 64m;
    public const int MinGiB = 1;
    public const int MaxGiB = 65536;

    public const int DefNodes = 1;
    public const long DefMaxmemoryBytes = 536870912; // 512 MiB
    public const string DefMaxmemoryPolicy = "allkeys-lru";
    public const decimal DefCpu = 1m;
    public const int DefMemGi = 1;
    public const int DefDiskGi = 10;

    // Инвариант R3: maxmemory_bytes < mem-лимит (иначе OOM-килл).
    public static bool MaxmemoryFitsMem(long maxmemoryBytes, int memGi)
        => maxmemoryBytes < memGi * 1024L * 1024 * 1024;

    public static string Canonical(decimal value)
        => value.ToString("0.#########", CultureInfo.InvariantCulture);

    // Правило валидации config-полей: policy ∈ 8 значений канона, maxmemory > 0.
    public static bool IsKnownPolicy(string? policy)
        => policy is null || ValkeyWriting.KnownPolicies.Contains(policy);
}

// DTO запросов API (JSON camelCase; ресурсы — decimal ядра + int GiB).
public sealed record CreateValkeyClusterRequest(
    string? Name,
    int? Nodes = null,
    long? MaxmemoryBytes = null,
    string? MaxmemoryPolicy = null,
    ValkeyResourcesUpdateRequest? Resources = null);

public sealed record ValkeyResourcesUpdateRequest(
    decimal? Cpu = null,
    int? MemGi = null,
    int? DiskGi = null);

public sealed record ValkeyConfigUpdateRequest(
    long? MaxmemoryBytes = null,
    string? MaxmemoryPolicy = null);

public sealed record RotateValkeyPasswordRequest(string? Role);

// Типизированный config-JSON (канон arch/20 §2.1; сериализация — ValkeyWriting.ConfigJson).
public sealed record ValkeyConfigJson(
    [property: System.Text.Json.Serialization.JsonPropertyName("nodes")] int Nodes,
    [property: System.Text.Json.Serialization.JsonPropertyName("maxmemory_bytes")] long MaxmemoryBytes,
    [property: System.Text.Json.Serialization.JsonPropertyName("maxmemory_policy")] string MaxmemoryPolicy,
    [property: System.Text.Json.Serialization.JsonPropertyName("created_unix")] long? CreatedUnix,
    [property: System.Text.Json.Serialization.JsonPropertyName("state")] string? State)
{
    // Канон arch/20 §2.1: опциональные поля (state у Active-кластера)
    // ОТСУТСТВУЮТ в значении ключа — WhenWritingNull, не "state":null.
    private static readonly System.Text.Json.JsonSerializerOptions Canonical =
        new() { DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull };

    public string Serialize() => System.Text.Json.JsonSerializer.Serialize(this, Canonical);

    public static ValkeyConfigJson? Parse(string raw)
    {
        try
        {
            return System.Text.Json.JsonSerializer.Deserialize<ValkeyConfigJson>(raw);
        }
        catch (JsonException)
        {
            return null;
        }
    }
}
