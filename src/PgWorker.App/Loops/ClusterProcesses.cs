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

    /// <summary>Эвакуация конкретного мёртвого шарда (BucketEvacuator E0–E4).</summary>
    Task<Result<ProcessOutcome>> EvacuateAsync(ClusterSnapshot snap, string deadShard, CancellationToken ct);

    /// <summary>Обработка заявок переездов бакетов /pgworker/moves/&lt;C&gt;/ (t01, spec §5.3).</summary>
    Task<Result<ProcessOutcome>> ProcessMovesAsync(ClusterSnapshot snap, CancellationToken ct);
}

/// <summary>Реализация поверх процессов задач 19–22 + MoveProcess t01 (синглтоны DI).</summary>
internal sealed class ClusterProcesses(
    ProvisioningProcess provision,
    DeprovisioningProcess deprovision,
    NodeSupervisor supervisor,
    BucketEvacuator evacuator,
    MoveProcess moves) : IClusterProcesses
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

    public Task<Result<ProcessOutcome>> EvacuateAsync(ClusterSnapshot snap, string deadShard, CancellationToken ct)
        => evacuator.TickAsync(snap, deadShard, ct);

    public Task<Result<ProcessOutcome>> ProcessMovesAsync(ClusterSnapshot snap, CancellationToken ct)
        => moves.TickAsync(snap, ct);
}
