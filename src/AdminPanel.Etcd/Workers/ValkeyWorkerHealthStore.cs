using AdminPanel.Core;
using Shared.Core.DI;

namespace AdminPanel.Etcd.Workers;

// Стор результатов опроса /healthz инстансов ValkeyWorker (t03; arch/adminpanel/02
// §2.3.3): poller пишет, valkey-refresher вносит готовым в снапшот (паттерн
// KafkaWorkerHealthStore: volatile-замена, KV-тик не блокируется).
[InjectAsSingleton(typeof(IValkeyWorkerHealthStore))]
public sealed class ValkeyWorkerHealthStore : IValkeyWorkerHealthStore
{
    private volatile IReadOnlyList<WorkerHealth>? _current;

    public IReadOnlyList<WorkerHealth>? Current => _current;

    public void Replace(IReadOnlyList<WorkerHealth> health) => _current = health;
}

// Читатель результатов опроса (refresher вносит их успешным тиком).
public interface IValkeyWorkerHealthStore
{
    IReadOnlyList<WorkerHealth>? Current { get; }

    void Replace(IReadOnlyList<WorkerHealth> health);
}
