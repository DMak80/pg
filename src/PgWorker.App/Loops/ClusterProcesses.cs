using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Moves;
using PgWorker.Provisioning.Processes;

namespace PgWorker.App.Loops;

// Агрегатор процессов для ReconcileLoop (задача 23): цикл не знает конкретных
// машин состояний — только эту грань (мокабельно в unit-тестах цикла).

/// <summary>Точка входа цикла к процессам-машинам состояний (§6.4).</summary>
internal interface IClusterProcesses
{
    Task<Result<ProcessOutcome>> ProvisionAsync(ClusterSnapshot snap, CancellationToken ct);

    Task<Result<ProcessOutcome>> DeprovisionAsync(ClusterSnapshot snap, CancellationToken ct);

    /// <summary>Надзор + список полностью мёртвых шардов (событие эвакуации).</summary>
    Task<Result<SuperviseOutcome>> SuperviseAsync(ClusterSnapshot snap, CancellationToken ct);

    /// <summary>Усыновление: адреса внешних нод в portalloc (spec §3.2, arch/14 §5 J).</summary>
    Task<Result<ProcessOutcome>> AdoptAsync(ClusterSnapshot snap, CancellationToken ct);

    /// <summary>Эвакуация конкретного мёртвого шарда (BucketEvacuator E0–E4).</summary>
    Task<Result<ProcessOutcome>> EvacuateAsync(ClusterSnapshot snap, string deadShard, CancellationToken ct);

    /// <summary>Обработка заявок переездов бакетов /pgworker/moves/&lt;C&gt;/ (t01, spec §5.3).</summary>
    Task<Result<ProcessOutcome>> ProcessMovesAsync(ClusterSnapshot snap, CancellationToken ct);

    /// <summary>Scale-проход Active-ветки (t06, spec §5.1): remove-кандидаты →
    /// add-кандидаты, по одному шард-за-тик (Д13: демонтаж освобождает хосты/порты).</summary>
    Task<Result<ProcessOutcome>> ScaleShardsAsync(ClusterSnapshot snap, CancellationToken ct);

    /// <summary>Ротация app-пароля по заявке /pgworker/rotations/&lt;C&gt; (spec §4.3,
    /// arch/14 §5 I); no-op без заявки.</summary>
    Task<Result<ProcessOutcome>> RotateAppPasswordAsync(ClusterSnapshot snap, CancellationToken ct);

    /// <summary>Репарация брошенных переездов: синтетические заявки в MoveProcess
    /// (adopt-repair spec §3.5, arch/14 §5 K).</summary>
    Task<Result<ProcessOutcome>> RepairAsync(ClusterSnapshot snap, CancellationToken ct);
}

/// <summary>Реализация поверх процессов задач 19–22 + MoveProcess t01 (синглтоны DI).</summary>
internal sealed class ClusterProcesses(
    ProvisioningProcess provision,
    DeprovisioningProcess deprovision,
    NodeSupervisor supervisor,
    AdoptionProcess adopt,
    BucketEvacuator evacuator,
    MoveProcess moves,
    MoveRepairProcess repair,
    AddShardProcess addShards,
    RemoveShardProcess removeShards,
    AppPasswordRotator rotator) : IClusterProcesses
{
    public Task<Result<ProcessOutcome>> ProvisionAsync(ClusterSnapshot snap, CancellationToken ct)
        => provision.TickAsync(snap, ct);

    public Task<Result<ProcessOutcome>> DeprovisionAsync(ClusterSnapshot snap, CancellationToken ct)
        => deprovision.TickAsync(snap, ct);

    // Мёртвые шарды — значением из тика надзора (rework №1): свойство
    // синглтона перезаписывалось параллельными тиками чужих кластеров
    // (шаблонные имена shard1/shard2 совпадают между кластерами).
    public Task<Result<SuperviseOutcome>> SuperviseAsync(ClusterSnapshot snap, CancellationToken ct)
        => supervisor.TickAsync(snap, ct);

    public Task<Result<ProcessOutcome>> AdoptAsync(ClusterSnapshot snap, CancellationToken ct)
        => adopt.TickAsync(snap, ct);

    public Task<Result<ProcessOutcome>> EvacuateAsync(ClusterSnapshot snap, string deadShard, CancellationToken ct)
        => evacuator.TickAsync(snap, deadShard, ct);

    public Task<Result<ProcessOutcome>> ProcessMovesAsync(ClusterSnapshot snap, CancellationToken ct)
        => moves.TickAsync(snap, ct);

    public async Task<Result<ProcessOutcome>> ScaleShardsAsync(ClusterSnapshot snap, CancellationToken ct)
    {
        var candidates = ShardScaleClassifier.Detect(snap);

        // Remove-проход первым (Д13): помеченные демонтируются, недоднятый add
        // отменяется этим же путём (Д5).
        if (candidates.Remove.Count > 0)
        {
            var removed = await removeShards.TickAsync(snap, candidates.Remove[0], ct);
            if (!removed.IsSuccess)
                return removed;
        }

        // Add-проход: только НЕпомеченные кандидаты. Шард из обоих списков
        // (TO_REMOVE + declared без dsn) уже демонтирован remove-проходом выше —
        // снапшот тика ещё видит его declared-ноды, и без фильтра add поднял бы
        // шард заново; AddShardProcess (A1) также guard'ит ToRemove (blocked-removing).
        var addCandidate = candidates.Add.FirstOrDefault(name => !candidates.Remove.Contains(name));
        if (addCandidate is { } shard)
        {
            var added = await addShards.TickAsync(snap, shard, ct);
            if (!added.IsSuccess)
                return added;
        }

        return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
    }

    public Task<Result<ProcessOutcome>> RotateAppPasswordAsync(ClusterSnapshot snap, CancellationToken ct)
        => rotator.TickAsync(snap, ct);

    public Task<Result<ProcessOutcome>> RepairAsync(ClusterSnapshot snap, CancellationToken ct)
        => repair.TickAsync(snap, ct);
}
