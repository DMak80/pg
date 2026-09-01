using System.Text.Json;
using System.Text.RegularExpressions;
using PgWorker.Core;
using PgWorker.Core.Writing;
using PgWorker.Etcd.Client;

namespace PgWorker.App.Api.Operations;

// Демонтаж шарда через API воркера (task etcd-via-worker-api): one-way маркер
// shards/<X>/state=TO_REMOVE (arch/02 §9.6); очистку выполняет RemoveShardProcess.
// Порт панельного DeleteShardCommandHandler: guards переписаны с панельного
// снапшота на прямые чтения etcd (ClusterGuardData), тексты/порядок — дословно.
// Без txn: конкурентные PUT маркера сходятся к одному значению. Без ретраев.
public sealed partial class DeleteShardHandler(IEtcdGateway gateway, string[] endpoints)
{
    public const string ToRemoveState = "TO_REMOVE"; // канон маркера (§9.6)

    // Паттерн имени шарда (§9.5/pg §4.1): без дефиса.
    [GeneratedRegex("^[a-z][a-z0-9_]{0,30}$")]
    private static partial Regex ShardNamePattern();

    public async Task<Result> HandleAsync(string cluster, string shard, CancellationToken ct)
    {
        // 1) Имена канонические, иначе 404 (такие создать не могли).
        if (!CreateClusterLimits.NamePattern().IsMatch(cluster)
            || !ShardNamePattern().IsMatch(shard))
            return Result.Failed(new ClusterNotFoundException(cluster));

        // 2) Guard-данные кластера одним range (config/shards/routing/status/nodes).
        var data = await ClusterGuardData.ReadAsync(gateway, endpoints, cluster, ct);
        if (!data.IsSuccess)
            return Result.Failed(data.Error!); // 503 (etcd недоступен)
        var info = data.Value;
        if (info.ConfigRaw is null)
            return Result.Failed(new ClusterNotFoundException(cluster));

        // 3) Config: сбой парсинга → 503; state не Active → 409.
        string? state;
        int bucketsCount;
        try
        {
            state = ReadState(info.ConfigRaw);
            bucketsCount = ReadBuckets(info.ConfigRaw);
        }
        catch (JsonException)
        {
            return Result.Failed(new InvalidClusterConfigException(cluster)); // 503
        }
        if (state is not null)
            return Result.Failed(new ClusterNotActiveException(cluster, state));

        // 4) Шард существует (replicas-ключ) иначе 404.
        var replicas = await ReadKeyAsync($"/clusters/{cluster}/shards/{shard}/replicas", ct);
        if (!replicas.IsSuccess)
            return Result.Failed(replicas.Error!); // 503
        if (replicas.Value is null)
            return Result.Failed(new ShardNotFoundException(cluster, shard));

        // 5) Пред-проверки guard'ов (Д4: быстро оператору; гонки ловит RemoveShardProcess).
        //    Переезд: owner ИЛИ target СТАТУСА, плюс routing-owner (зеркало G4:
        //    после flip routing уже уехал, а зависший статус держит старый шард).
        //    Нешардированная БД (arch/03 §2): демонтаж единственного вырожденного
        //    шарда = удаление кластера; guard до бакетов — сообщение про «последний
        //    шард» сбивает.
        if (bucketsCount == 1 && info.Shards.Count <= 1)
            return Result.Failed(new NonShardedClusterException(cluster));
        var owned = info.Routing.Values.Count(owner => owner == shard);
        if (owned > 0)
            return Result.Failed(ShardRemoveBlockedException.Buckets(owned));
        var movingStates = new HashSet<string>(StringComparer.Ordinal) { "SYNCING", "FROZEN", "ABORTING" };
        if (info.Status.Any(s => movingStates.Contains(s.Value.State ?? ClusterGuardData.ActiveState)
                && (s.Value.Owner == shard || s.Value.Target == shard
                    || info.Routing.TryGetValue(s.Key, out var owner) && owner == shard)))
            return Result.Failed(ShardRemoveBlockedException.UnfinishedMove());
        if (info.Shards.Count <= 1)
            return Result.Failed(ShardRemoveBlockedException.LastShard());
        if (info.NodeStates.Any(n => n.Key.StartsWith($"{shard}/", StringComparison.Ordinal)
                && n.Value == "QUARANTINED"))
            return Result.Failed(ShardRemoveBlockedException.Quarantine());

        // 6) PUT маркера; уже TO_REMOVE → идемпотентный успех без записи (§9.6);
        //    сбой чтения state-ключа → 503 (не пишем поверх нечитанного).
        var markerKey = $"/clusters/{cluster}/shards/{shard}/state";
        var marker = await ReadKeyAsync(markerKey, ct);
        if (!marker.IsSuccess)
            return Result.Failed(marker.Error!); // 503
        if (marker.Value == ToRemoveState)
            return Result.Success();

        var put = await EtcdFailover.CallAsync(endpoints,
            endpoint => gateway.PutAsync(endpoint, markerKey, ToRemoveState, null, ct));
        return put.IsSuccess ? Result.Success() : Result.Failed(put.Error!);
    }

    // Точечное чтение ключа через range (образец §9.4): Failed → 503;
    // Success(null) — ровно «ключа нет».
    private async Task<Result<string?>> ReadKeyAsync(string key, CancellationToken ct)
    {
        var range = await EtcdFailover.CallAsync(endpoints,
            endpoint => gateway.RangeAsync(endpoint, key, ct));
        if (!range.IsSuccess)
            return Result<string?>.Failed(range.Error!);
        return Result<string?>.Success(range.Value.FirstOrDefault(kv => kv.Key == key)?.Value);
    }

    private static string? ReadState(string raw)
    {
        using var doc = JsonDocument.Parse(raw);
        return doc.RootElement.TryGetProperty("state", out var s) && s.ValueKind == JsonValueKind.String
            ? s.GetString()
            : null;
    }

    private static int ReadBuckets(string raw)
    {
        using var doc = JsonDocument.Parse(raw);
        return doc.RootElement.TryGetProperty("buckets", out var b) && b.ValueKind == JsonValueKind.Number
            ? b.GetInt32()
            : 0;
    }
}
