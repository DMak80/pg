using System.Text.Json;
using System.Text.Json.Serialization;
using KafkaWorker.Core;
using KafkaWorker.Etcd.Client;

namespace KafkaWorker.Etcd.Coordination;

// Журнал текущего процесса кластера (arch/16 §5): journal-before-manipulations —
// крах оставляет самодокументирующийся след; takeover продолжает с записанной фазы.
// Unreachable — трек молчания брокеров надзора (значение brokers/<b>/state —
// плоская строка по контракту arch/15, время живёт здесь): "broker" → first_seen_unix.
public sealed record WorkState(
    [property: JsonPropertyName("op")] string Op,
    [property: JsonPropertyName("phase")] string Phase,
    [property: JsonPropertyName("instance")] string Instance,
    [property: JsonPropertyName("updated_unix")] long UpdatedUnix,
    [property: JsonPropertyName("last_error")] string? LastError,
    [property: JsonPropertyName("unreachable")] IReadOnlyDictionary<string, long>? Unreachable = null);

// Обёртка над /kafkaworker/work/<C>: чистая etcd-запись фаз процессов.
public sealed class WorkJournal(IEtcdGateway gateway, string[] endpoints)
{
    private static readonly JsonSerializerOptions Json = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    // /kafkaworker/work/<C>: {"op","phase","instance","updated_unix","last_error"}.
    public Task<Result> WriteAsync(
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
            return Result<WorkState?>.Failed(new ApplicationException(
                $"битый журнал /kafkaworker/work/{cluster}: {e.Message}", e));
        }
    }

    // Тик надзора: op=supervise + трек недоступности (порог NodeDeadSec);
    // lastError — накопленные warning-ы тика (RF=1-пересоздания и т.п.).
    public Task<Result> WriteSupervisionAsync(
        string cluster, string instance, IReadOnlyDictionary<string, long> unreachable,
        string? lastError, CancellationToken ct)
        => WithFailoverAsync(endpoint => gateway.PutAsync(
            endpoint, WorkKey(cluster),
            JsonSerializer.Serialize(new WorkState("supervise", "supervising", instance,
                DateTimeOffset.UtcNow.ToUnixTimeSeconds(), lastError, unreachable), Json),
            lease: null, ct));

    // Прочитать трек недоступности (null = журнала нет/поля нет).
    public async Task<Result<IReadOnlyDictionary<string, long>>> ReadUnreachableAsync(string cluster, CancellationToken ct)
    {
        var state = await ReadAsync(cluster, ct);
        if (!state.IsSuccess)
            return Result<IReadOnlyDictionary<string, long>>.Failed(state.Error!);

        return Result<IReadOnlyDictionary<string, long>>.Success(
            state.Value?.Unreachable ?? (IReadOnlyDictionary<string, long>)new Dictionary<string, long>());
    }

    private static string WorkKey(string cluster) => $"/kafkaworker/work/{cluster}";

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
