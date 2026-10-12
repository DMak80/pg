using System.Text.Json;
using S3Worker.Core.Model;

namespace S3Worker.Provisioning.Processes;

/// <summary>Опции процессов S3Worker (плоский срез S3WorkerOptions, порт
/// ValkeyProvisioningOptions; arch/23 §2/§4, arch/24 §8).</summary>
public sealed record S3ProvisioningOptions(
    int PortRangeFrom,
    int PortRangeTo,
    int NodeBootSec,
    int NodeDeadSec,
    int HealthIntervalSec,
    string? AdvertisedHost,
    string OwnS3Image,
    string Bucket)
{
    public static S3ProvisioningOptions Default { get; } =
        new(18000, 18999, 100, 90, 15, null, "owns3:dev", "pgworker-backups");
}

/// <summary>
/// Общие хелперы процессов S3Worker (порт ProcessCommon valkey): ключи etcd
/// домена /s3/ и координации /s3worker/ (arch/23 §8), сериализация portalloc,
/// чтение/запись state ноды (failover). Дополняется задачами 10–13
/// (ParseResources и др. — Task 9).
/// </summary>
public static class S3ProcessCommon
{
    public static string ConfigKey(string cluster)
        => $"/s3/clusters/{cluster}/config";

    public static string EndpointsKey(string cluster)
        => $"/s3/clusters/{cluster}/endpoints";

    public static string NodeStateKey(string cluster, string node)
        => $"/s3/clusters/{cluster}/nodes/{node}/state";

    public static string NodePlacementKey(string cluster, string node)
        => $"/s3/clusters/{cluster}/nodes/{node}/placement";

    public static string NodeResourcesKey(string cluster, string node)
        => $"/s3/clusters/{cluster}/nodes/{node}/resources";

    public static string NodePlacedOnKey(string cluster, string node)
        => $"/s3/clusters/{cluster}/nodes/{node}/placed_on";

    public static string NodeUsageKey(string cluster, string node)
        => $"/s3/clusters/{cluster}/nodes/{node}/usage";

    public static string PortAllocKey(string cluster)
        => $"/s3worker/portalloc/{cluster}";

    public static string RotationKey(string cluster)
        => $"/s3worker/rotations/{cluster}";

    public static string WorkKey(string cluster)
        => $"/s3worker/work/{cluster}";

    public static string ClusterPrefix(string cluster)
        => $"/s3/clusters/{cluster}/";

    // Формат arch/23 §8: {"node<k>":{"host":"h","s3":18001}}.
    public static Dictionary<string, NodeAddress> ParsePortAlloc(string json)
    {
        var addresses = new Dictionary<string, NodeAddress>();
        using var doc = JsonDocument.Parse(json);
        foreach (var node in doc.RootElement.EnumerateObject())
            addresses[node.Name] = new NodeAddress(
                node.Value.GetProperty("host").GetString()!,
                node.Value.GetProperty("s3").GetInt32());
        return addresses;
    }

    public static string SerializePortAlloc(IReadOnlyDictionary<string, NodeAddress> addresses)
    {
        using var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            foreach (var (node, addr) in addresses.OrderBy(p => p.Key, StringComparer.Ordinal))
            {
                writer.WritePropertyName(node);
                writer.WriteStartObject();
                writer.WriteString("host", addr.Host);
                writer.WriteNumber("s3", addr.S3Port);
                writer.WriteEndObject();
            }

            writer.WriteEndObject();
        }

        return System.Text.Encoding.UTF8.GetString(buffer.ToArray());
    }
}
