using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Provisioning.Processes;

namespace PgWorker.App.Loops;

// Агрегатор процессов для ReconcileLoop (задача 23): цикл не знает конкретных
// машин состояний — только эту грань (мокабельно в unit-тестах цикла).

/// <summary>Исход тика надзора: обычный outcome + мёртвые шарды (эвакуация).</summary>
internal sealed record SuperviseOutcome(ProcessOutcome Outcome, IReadOnlyList<string> DeadShards);

/// <summary>Точка входа цикла к процессам-машинам состояний (§6.4).</summary>
internal interface IClusterProcesses
{
    Task<Result<ProcessOutcome>> ProvisionAsync(ClusterSnapshot snap, CancellationToken ct);

    Task<Result<ProcessOutcome>> DeprovisionAsync(ClusterSnapshot snap, CancellationToken ct);

    /// <summary>Надзор + список полностью мёртвых шардов (событие эвакуации).</summary>
    Task<Result<SuperviseOutcome>> SuperviseAsync(ClusterSnapshot snap, CancellationToken ct);

    /// <summary>Эвакуация конкретного мёртвого шарда (BucketEvacuator E0–E4).</summary>
    Task<Result<ProcessOutcome>> EvacuateAsync(ClusterSnapshot snap, string deadShard, CancellationToken ct);
}

/// <summary>Реализация поверх процессов задач 19–22 (все — синглтоны DI).</summary>
internal sealed class ClusterProcesses(
    ProvisioningProcess provision,
    DeprovisioningProcess deprovision,
    NodeSupervisor supervisor,
    BucketEvacuator evacuator) : IClusterProcesses
{
    public Task<Result<ProcessOutcome>> ProvisionAsync(ClusterSnapshot snap, CancellationToken ct)
        => provision.TickAsync(snap, ct);

    public Task<Result<ProcessOutcome>> DeprovisionAsync(ClusterSnapshot snap, CancellationToken ct)
        => deprovision.TickAsync(snap, ct);

    public async Task<Result<SuperviseOutcome>> SuperviseAsync(ClusterSnapshot snap, CancellationToken ct)
    {
        var outcome = await supervisor.TickAsync(snap, ct);
        if (!outcome.IsSuccess)
            return Result<SuperviseOutcome>.Failed(outcome.Error!);

        // DeadShards — свойство последнего тика надзора (задача 21).
        return Result<SuperviseOutcome>.Success(new SuperviseOutcome(outcome.Value, supervisor.DeadShards));
    }

    public Task<Result<ProcessOutcome>> EvacuateAsync(ClusterSnapshot snap, string deadShard, CancellationToken ct)
        => evacuator.TickAsync(snap, deadShard, ct);
}
