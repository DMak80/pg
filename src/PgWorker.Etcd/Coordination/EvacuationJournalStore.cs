using System.Text.Json;
using System.Text.Json.Serialization;
using Shared.Core;
using Shared.Etcd.Client;

namespace PgWorker.Etcd.Coordination;

// Журнал эвакуации шарда (Pg-домен): /pgworker/evacuations/<C>/<X> — ключ жёсткий,
// у Kfw аналога нет (: WorkJournal ушёл в Shared, эвакуации остались доменом).
public sealed record EvacuationJournal(
    [property: JsonPropertyName("buckets")] IReadOnlyDictionary<int, string> Buckets,
    [property: JsonPropertyName("reason")] string Reason,
    [property: JsonPropertyName("evacuated_unix")] long EvacuatedUnix,
    [property: JsonPropertyName("state")] string State,
    [property: JsonPropertyName("returned_unix")] long? ReturnedUnix);

// Обёртка над /pgworker/evacuations/<C>/<X>: чистая etcd-запись журнала эвакуаций
// (: выделен из WorkJournal — Put/Get + JSON + failover).
public sealed class EvacuationJournalStore(IEtcdGateway gateway, string[] endpoints)
{
    private static readonly JsonSerializerOptions Json = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    public Task<Result> WriteAsync(string cluster, string shard, EvacuationJournal j, CancellationToken ct)
        => WithFailoverAsync(endpoint => gateway.PutAsync(
            endpoint, EvacuationKey(cluster, shard), JsonSerializer.Serialize(j, Json), lease: null, ct));

    public async Task<Result<EvacuationJournal?>> ReadAsync(string cluster, string shard, CancellationToken ct)
    {
        var result = await WithFailoverAsync(endpoint => gateway.GetAsync(endpoint, EvacuationKey(cluster, shard), ct));
        if (!result.IsSuccess)
            return Result<EvacuationJournal?>.Failed(result.Error!);

        if (result.Value is not { } kv)
            return Result<EvacuationJournal?>.Success(null);

        try
        {
            return Result<EvacuationJournal?>.Success(JsonSerializer.Deserialize<EvacuationJournal>(kv.Value, Json));
        }
        catch (JsonException e)
        {
            return Result<EvacuationJournal?>.Failed(new ApplicationException(
                $"broken evacuation journal /pgworker/evacuations/{cluster}/{shard}: {e.Message}", e));
        }
    }

    private static string EvacuationKey(string cluster, string shard) => $"/pgworker/evacuations/{cluster}/{shard}";

    // Единая точка failover (arch/17 S7: sticky + per-attempt бюджет + негативный
    // кэш; дубликаты цикла обхода удалены).
    private Task<Result<T>> WithFailoverAsync<T>(Func<string, Task<Result<T>>> call)
        => EtcdEndpointFailover.CallAsync(endpoints, call);

    private Task<Result> WithFailoverAsync(Func<string, Task<Result>> call)
        => EtcdEndpointFailover.CallAsync(endpoints, call);
}
