using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Provisioning.Processes;

namespace KafkaWorker.App.Loops;// Агрегатор процессов для ReconcileLoop (порт ClusterProcesses PgWorker):
// цикл не знает конкретных машин состояний — только эту грань (мокабельно
// в unit-тестах цикла).

/// <summary>
/// Точка входа цикла к процессам-машинам состояний (arch/16 §5).
/// </summary>
internal interface IKafkaClusterProcesses
{
    Task<Result> ProvisionAsync(KafkaClusterSnapshot snap, CancellationToken ct);

    Task<Result> DeprovisionAsync(KafkaClusterSnapshot snap, CancellationToken ct);

    /// <summary>
    /// Active-ветка (порядок — arch/16 §5): надзор (C) → converge (E) →
    /// reassignment (I, t02: drain TO_REMOVE + заявки balance) → scale-проход
    /// remove (G) → add (F) → ротация (H) → регенерация (J, t06: автоконверге
    /// лимитов) → автосинк топиков (D, тик TopicSyncIntervalSec —
    /// троттлится внутри процесса).
    /// </summary>
    Task<Result> ActiveAsync(KafkaClusterSnapshot snap, CancellationToken ct);
}

/// <summary>Реализация поверх процессов (синглтоны DI).</summary>
internal sealed class KafkaClusterProcesses(
    ProvisioningProcess provision,
    DeprovisioningProcess deprovision,
    NodeSupervisor supervisor,
    IClusterConfigConverger converger,
    PartitionReassignerProcess reassigner,
    RemoveBrokerProcess removeBroker,
    AddBrokerProcess addBroker,
    AppPasswordRotator rotator,
    NodeRegenerator regenerator,
    TopicSyncProcess topicSync) : IKafkaClusterProcesses
{
    public Task<Result> ProvisionAsync(KafkaClusterSnapshot snap, CancellationToken ct)
        => provision.RunAsync(snap, ct);

    public Task<Result> DeprovisionAsync(KafkaClusterSnapshot snap, CancellationToken ct)
        => deprovision.RunAsync(snap.Cluster, snap.Brokers.Select(b => b.Name).ToList(), ct);

    public async Task<Result> ActiveAsync(KafkaClusterSnapshot snap, CancellationToken ct)
    {
        // Надзор (C) — самовосстановление нод; конвейер Active-ветки останавливать
        // не должен: ошибка надзора — ошибка тика кластера (следующий тик повторит).
        var supervised = await supervisor.RunAsync(snap, ct);
        if (!supervised.IsSuccess)
            return supervised;

        // Converge (E) — только для поднявшегося кластера (endpoints есть).
        if (snap.Endpoints is not null && snap.AppUser is not null && snap.AppPassword is not null)
        {
            var converged = await converger.ApplyAsync(
                snap.Cluster, snap.Endpoints, snap.AppUser, snap.AppPassword, snap.Config, ct);
            if (!converged.IsSuccess)
                return converged;
        }

        // Reassignment (I) перед remove: к моменту G дренируемый брокер пуст
        // (drain TO_REMOVE-кандидатов + заявка balance; arch/16 §5 классификация).
        var reassigned = await reassigner.RunAsync(snap, ct);
        if (!reassigned.IsSuccess)
            return reassigned;

        // Scale-проход: сначала демонтаж (G), затем добавление (F) — endpoints
        // не «прыгает» туда-сюда в одном тике.
        var removed = await removeBroker.RunAsync(snap, ct);
        if (!removed.IsSuccess)
            return removed;

        var added = await addBroker.RunAsync(snap, ct);
        if (!added.IsSuccess)
            return added;

        // Ротация app-пароля (H) — по заявке /kafkaworker/rotations/<C>.
        var rotated = await rotator.RunAsync(snap, ct);
        if (!rotated.IsSuccess)
            return rotated;

        // Регенерация (J, t06): автоконверге лимитов — после ротации (не
        // смешиваем rolling-ы) и перед TopicSync (реестр — к итогу).
        var regenerated = await regenerator.RunAsync(snap, ct);
        if (!regenerated.IsSuccess)
            return regenerated;

        // Автосинк топиков (D) — последним: реестр сходится к итоговому факту
        // кластера (в т.ч. после изменения состава брокеров).
        return await topicSync.RunAsync(snap, ct);
    }
}
