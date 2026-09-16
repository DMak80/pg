using ValkeyWorker.App;

namespace ValkeyWorker.App.Loops;

// Агрегатор процессов для ReconcileLoop (порт ClusterProcesses PgWorker/KafkaWorker):
// цикл не знает конкретных машин состояний — только эту грань (мокабельно в
// unit-тестах цикла). В каркасе (t02 фаза 2) — пустая реализация; наполняется
// процессами A–E в задаче 12.

/// <summary>Один reconcile-тик над всеми кластерами (процессы arch/21 §5).</summary>
public interface IValkeyClusterProcesses
{
    Task TickAsync(CancellationToken ct);
}

/// <summary>Пустая реализация каркаса: тик без работы (наполняется в задаче 12).</summary>
internal sealed class ValkeyClusterProcesses : IValkeyClusterProcesses
{
    public Task TickAsync(CancellationToken ct) => Task.CompletedTask;
}
