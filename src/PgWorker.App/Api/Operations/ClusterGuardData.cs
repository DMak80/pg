using System.Text.Json;
using PgWorker.Core;
using PgWorker.Etcd.Client;

namespace PgWorker.App.Api.Operations;

// Разбор префикса /clusters/<C>/ для guard'ов API-хендлеров (task etcd-via-worker-api):
// упрощённый порт панельных снапшот-парсеров (AdminPanel.Etcd/Parsing/ClustersParser)
// на прямое чтение etcd — семантика полей та же, воронка unknown-ключей не нужна.
internal sealed record ClusterGuardData(
    string? ConfigRaw,
    IReadOnlySet<string> Shards,                     // шарды с любыми ключами (как снапшот панели)
    IReadOnlyDictionary<string, string> ShardStates, // shard → state-маркер (TO_REMOVE)
    IReadOnlyDictionary<string, string> NodeStates,  // "<shard>/<node>" → state (QUARANTINED/…)
    IReadOnlyDictionary<int, string> Routing,        // bucket → owner
    IReadOnlyDictionary<int, (string? State, string? Owner, string? Target)> Status)
{
    // Канон статусов бакета (arch/02 §2.1): отсутствие status-ключа/поля state = ACTIVE.
    public const string ActiveState = "ACTIVE";

    /// <summary>Range префикса кластера с разбором нужных guard'ам полей.</summary>
    public static async Task<Result<ClusterGuardData>> ReadAsync(
        IEtcdGateway gateway, string[] endpoints, string cluster, CancellationToken ct)
    {
        var range = await EtcdFailover.CallAsync(endpoints,
            endpoint => gateway.RangeAsync(endpoint, $"/clusters/{cluster}/", ct));
        if (!range.IsSuccess)
            return Result<ClusterGuardData>.Failed(range.Error!);

        string? config = null;
        var shards = new HashSet<string>();
        var shardStates = new Dictionary<string, string>();
        var nodeStates = new Dictionary<string, string>();
        var routing = new Dictionary<int, string>();
        var status = new Dictionary<int, (string?, string?, string?)>();

        foreach (var kv in range.Value)
        {
            // "/clusters/<C>/<...>" → ["", "clusters", <C>, ...]
            var segments = kv.Key.Split('/');
            if (segments.Length < 4)
                continue;

            if (segments.Length == 4 && segments[3] == "config")
            {
                config = kv.Value;
                continue;
            }

            if (segments.Length >= 6 && segments[3] == "shards" && segments[4].Length > 0)
            {
                var shard = segments[4];
                if (segments.Length == 6 && segments[5] is "state")
                {
                    shards.Add(shard);
                    shardStates[shard] = kv.Value;
                }
                else if (segments.Length == 6 && segments[5] is "dsn" or "replicas" or "master")
                {
                    shards.Add(shard);
                }
                else if (segments.Length == 8 && segments[5] == "nodes" && segments[6].Length > 0
                         && segments[7] == "state")
                {
                    shards.Add(shard);
                    nodeStates[$"{shard}/{segments[6]}"] = kv.Value;
                }

                continue;
            }

            if (segments.Length == 6 && segments[3] == "buckets"
                && segments[5].StartsWith("bucket_", StringComparison.Ordinal)
                && int.TryParse(segments[5]["bucket_".Length..], out var id))
            {
                if (segments[4] == "routing")
                    routing[id] = kv.Value;
                else if (segments[4] == "status")
                    status[id] = ParseStatus(kv.Value);
            }
        }

        return Result<ClusterGuardData>.Success(new ClusterGuardData(
            config, shards, shardStates, nodeStates, routing, status));
    }

    // Статус бакета (arch/02 §2.1): state + owner/target переезда; битый JSON —
    // как у панели: поля отсутствуют → guard'ы трактуют бакет консервативно.
    private static (string? State, string? Owner, string? Target) ParseStatus(string raw)
    {
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            string? ReadString(string name) => root.TryGetProperty(name, out var el)
                && el.ValueKind == JsonValueKind.String
                ? el.GetString()
                : null;
            return (ReadString("state") ?? ActiveState, ReadString("owner"), ReadString("target"));
        }
        catch (JsonException)
        {
            return (ActiveState, null, null);
        }
    }
}
