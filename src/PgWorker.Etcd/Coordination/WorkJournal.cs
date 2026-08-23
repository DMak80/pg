using System.Text.Json;
using System.Text.Json.Serialization;
using PgWorker.Core;
using PgWorker.Etcd.Client;

namespace PgWorker.Etcd.Coordination;

// Журнал текущего процесса кластера (spec §4.3): journal-before-manipulations (P7).
public sealed record WorkState(
    [property: JsonPropertyName("op")] string Op,
    [property: JsonPropertyName("phase")] string Phase,
    [property: JsonPropertyName("instance")] string Instance,
    [property: JsonPropertyName("updated_unix")] long UpdatedUnix,
    [property: JsonPropertyName("last_error")] string? LastError);

// Журнал эвакуации шарда: bucketId → новый владелец, состояние карантина (spec §4.3).
public sealed record EvacuationJournal(
    [property: JsonPropertyName("buckets")] IReadOnlyDictionary<int, string> Buckets,
    [property: JsonPropertyName("reason")] string Reason,
    [property: JsonPropertyName("evacuated_unix")] long EvacuatedUnix,
    [property: JsonPropertyName("state")] string State,
    [property: JsonPropertyName("returned_unix")] long? ReturnedUnix);

// Обёртка над /pgworker/work/<C> и /pgworker/evacuations/<C>/<X>: чистая etcd-запись
// фаз процессов (крах оставляет самодокументирующийся след; takeover продолжает фазу).
public sealed class WorkJournal(IEtcdGateway gateway, string[] endpoints)
{
    private static readonly JsonSerializerOptions Json = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    // /pgworker/work/<C>: {"op","phase","instance","updated_unix","last_error"}.
    public Task<Result> WritePhaseAsync(
        string cluster, string op, string phase, string instance, string? lastError, CancellationToken ct)
    {
        var payload = new WorkState(op, phase, instance, DateTimeOffset.UtcNow.ToUnixTimeSeconds(), lastError);
        return WithFailoverAsync(endpoint => gateway.PutAsync(
            endpoint, WorkKey(cluster), JsonSerializer.Serialize(payload, Json), lease: null, ct));
    }

    public async Task<Result<WorkState?>> ReadAsync(string cluster, CancellationToken ct)
    {
        var result = await WithFailoverAsync(endpoint => gateway.GetAsync(endpoint, WorkKey(cluster), ct));
        if (!result.IsSuccess)
            return Result<WorkState?>.Failed(result.Error!);

        if (result.Value is not { } kv)
            return Result<WorkState?>.Success(null); // процесса не было

        try
        {
            return Result<WorkState?>.Success(JsonSerializer.Deserialize<WorkState>(kv.Value, Json));
        }
        catch (JsonException e)
        {
            return Result<WorkState?>.Failed(new ApplicationException($"битый журнал /pgworker/work/{cluster}: {e.Message}", e));
        }
    }

    // /pgworker/evacuations/<C>/<X> — журнал эвакуации (spec §4.3).
    public Task<Result> WriteEvacuationAsync(string cluster, string shard, EvacuationJournal j, CancellationToken ct)
        => WithFailoverAsync(endpoint => gateway.PutAsync(
            endpoint, EvacuationKey(cluster, shard), JsonSerializer.Serialize(j, Json), lease: null, ct));

    public async Task<Result<EvacuationJournal?>> ReadEvacuationAsync(string cluster, string shard, CancellationToken ct)
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
                $"битый журнал эвакуации /pgworker/evacuations/{cluster}/{shard}: {e.Message}", e));
        }
    }

    private static string WorkKey(string cluster) => $"/pgworker/work/{cluster}";

    private static string EvacuationKey(string cluster, string shard) => $"/pgworker/evacuations/{cluster}/{shard}";

    private async Task<Result<T>> WithFailoverAsync<T>(Func<string, Task<Result<T>>> call)
    {
        Result<T>? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await call(endpoint);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }

    private async Task<Result> WithFailoverAsync(Func<string, Task<Result>> call)
    {
        Result? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await call(endpoint);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }
}
