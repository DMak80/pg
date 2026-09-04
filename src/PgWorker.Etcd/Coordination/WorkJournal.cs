using System.Text.Json;
using System.Text.Json.Serialization;
using PgWorker.Core;
using PgWorker.Etcd.Client;

namespace PgWorker.Etcd.Coordination;

// Журнал текущего процесса кластера (spec §4.3): journal-before-manipulations (P7).
// Unreachable — трек недоступности нод надзора (решение плана №4: значение
// nodes/<n>/state — плоская строка по контракту панели, время живёт здесь):
// "shard/node" → first_seen_unix.
public sealed record WorkState(
    [property: JsonPropertyName("op")] string Op,
    [property: JsonPropertyName("phase")] string Phase,
    [property: JsonPropertyName("instance")] string Instance,
    [property: JsonPropertyName("updated_unix")] long UpdatedUnix,
    [property: JsonPropertyName("last_error")] string? LastError,
    [property: JsonPropertyName("unreachable")] IReadOnlyDictionary<string, long>? Unreachable = null,
    [property: JsonPropertyName("fail_count")] int? FailCount = null,
    [property: JsonPropertyName("fail_first_unix")] long? FailFirstUnix = null,
    [property: JsonPropertyName("retry_not_before_unix")] long? RetryNotBeforeUnix = null);

/// <summary>Серия подряд идущих фейлов процесса (бэкофф ретраев, arch/14 §3.3/§5 A):
/// живёт в /pgworker/work/&lt;C&gt;, пишется фейлом, переносится фазами, сбрасывается Done.</summary>
public sealed record RetrySeries(int FailCount, long FailFirstUnix, long RetryNotBeforeUnix);

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

    // Наблюдатели метрик подписываются на событие (WorkerMetricsInstrumentation,
    // arch/18 §2.2 — seam S2). Наблюдатели — пассивные: NotifyPhase глотает
    // исключения подписчиков, журнал от метрик не зависит.
    public sealed record WorkPhaseEntry(string Cluster, string Op, string Phase);

    private event Action<WorkPhaseEntry>? _phaseWritten;

    public event Action<WorkPhaseEntry>? PhaseWritten
    {
        add => _phaseWritten += value;
        remove => _phaseWritten -= value;
    }

    private void NotifyPhase(WorkPhaseEntry entry)
    {
        try
        {
            _phaseWritten?.Invoke(entry); // наблюдатель не влияет на журнал
        }
        catch
        {
            // Метрики — пассивные наблюдатели: исключение подписчика глотается.
        }
    }

    // /pgworker/work/<C>: {"op","phase","instance","updated_unix","last_error"} + поля серии
    // ретраев (fail_count/fail_first_unix/retry_not_before_unix — null опускается).
    // unreachable — трек недоступности надзора (t09: фазовые записи в тике
    // надзора — конвергенция DCS-конфига — обязаны его сохранять, иначе
    // пороги NodeDead/ShardDead сбрасываются каждой фазовой записью).
    public async Task<Result> WritePhaseAsync(
        string cluster, string op, string phase, string instance, string? lastError, CancellationToken ct,
        RetrySeries? series = null, IReadOnlyDictionary<string, long>? unreachable = null)
    {
        // t09 (AC6 фикс-гейта): фазовая запись БЕЗ явного трека сохраняет
        // существующий. Владелец unreachable-трека — supervise (полная
        // перезапись актуальным множеством — только WriteSupervisionAsync);
        // прочие процессы (adopt/rotate/moves/...) пишут фазы в тот же ключ
        // /pgworker/work/<C> каждый тик и раньше стирали трек → supervise
        // перечитывал пустоту и пороги NodeDead/ShardDead не истекали никогда
        // (эвакуация не стартовала вовсе).
        IReadOnlyDictionary<string, long>? track = unreachable;
        if (track is null)
        {
            var current = await ReadAsync(cluster, ct);
            if (!current.IsSuccess)
                return current;
            track = current.Value?.Unreachable;
        }

        var payload = new WorkState(op, phase, instance, DateTimeOffset.UtcNow.ToUnixTimeSeconds(), lastError,
            track, series?.FailCount, series?.FailFirstUnix, series?.RetryNotBeforeUnix);
        var put = await WithFailoverAsync(endpoint => gateway.PutAsync(
            endpoint, WorkKey(cluster), JsonSerializer.Serialize(payload, Json), lease: null, ct));
        if (put.IsSuccess)
            NotifyPhase(new WorkPhaseEntry(cluster, op, phase)); // только ПОСЛЕ успешного Put
        return put;
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

    // Тик надзора: op=supervise + трек недоступности (пороги NodeDead/ShardDead).
    // ВНИМАНИЕ: стационарная запись надзора событие PhaseWritten НЕ эмитит —
    // supervise подавлен в фазовых сериях (arch/18 §2.2, решение ревью Ф4-2).
    public Task<Result> WriteSupervisionAsync(
        string cluster, string instance, IReadOnlyDictionary<string, long> unreachable, CancellationToken ct)
        => WithFailoverAsync(endpoint => gateway.PutAsync(
            endpoint, WorkKey(cluster),
            JsonSerializer.Serialize(new WorkState("supervise", "supervising", instance,
                DateTimeOffset.UtcNow.ToUnixTimeSeconds(), null, unreachable), Json),
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
