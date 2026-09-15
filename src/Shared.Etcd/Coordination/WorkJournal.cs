using System.Text.Json;
using System.Text.Json.Serialization;
using Shared.Core;
using Shared.Etcd.Client;

namespace Shared.Etcd.Coordination;

// Журнал текущего процесса кластера (spec §4.3, arch/16 §5): journal-before-manipulations.
// Unreachable — трек недоступности нод надзора (значение state — плоская строка
// по контракту панели, время живёт здесь): "нода" → first_seen_unix.
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
/// живёт в {prefix}/work/&lt;C&gt;, пишется фейлом, переносится фазами, сбрасывается Done.</summary>
public sealed record RetrySeries(int FailCount, long FailFirstUnix, long RetryNotBeforeUnix);

// Обёртка над {prefix}/work/<C> (t09: общий журнал Pg/Kfw, префикс — параметр ctor):
// чистая etcd-запись фаз процессов (крах оставляет самодокументирующийся след;
// takeover продолжает фазу).
public sealed class WorkJournal(string keyPrefix, IEtcdGateway gateway, string[] endpoints)
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

    // {prefix}/work/<C>: {"op","phase","instance","updated_unix","last_error"} + поля серии
    // ретраев (fail_count/fail_first_unix/retry_not_before_unix — null опускается).
    // unreachable — трек недоступности надзора (t09: фазовые записи в тике
    // надзора — конвергенция DCS/брокеров — обязаны его сохранять, иначе
    // пороги сбрасываются каждой фазовой записью).
    public async Task<Result> WritePhaseAsync(
        string cluster, string op, string phase, string instance, string? lastError, CancellationToken ct,
        RetrySeries? series = null, IReadOnlyDictionary<string, long>? unreachable = null)
    {
        // t09 (унификация с Pg, фикс AC6): фазовая запись БЕЗ явного трека
        // сохраняет существующий (fix сброса порогов NodeDead/BrokerDead;
        // унификация t09). Владелец unreachable-трека — supervise (полная
        // перезапись актуальным множеством — только WriteSupervisionAsync);
        // прочие процессы пишут фазы в тот же ключ {prefix}/work/<C> каждый тик
        // и раньше стирали трек → supervise перечитывал пустоту и пороги не
        // истекали никогда.
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
            return Result<WorkState?>.Failed(new ApplicationException($"битый журнал {WorkKey(cluster)}: {e.Message}", e));
        }
    }

    // Тик надзора: op=supervise + трек недоступности (пороги NodeDead/BrokerDead);
    // lastError — накопленные warning-ы тика (RF=1-пересоздания и т.п.).
    // ВНИМАНИЕ: стационарная запись надзора событие PhaseWritten НЕ эмитит —
    // supervise подавлен в фазовых сериях (arch/18 §2.2, решение ревью Ф4-2).
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

    private string WorkKey(string cluster) => $"{keyPrefix}/work/{cluster}";

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
