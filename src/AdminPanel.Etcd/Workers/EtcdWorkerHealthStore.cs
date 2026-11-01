using AdminPanel.Core;
using Shared.Core.DI;

namespace AdminPanel.Etcd.Workers;

// Стор результатов опроса /healthz инстансов EtcdWorker (arch/adminpanel/02
// §2.3.5): poller пишет, pg-refresher вносит готовым в снапшот (паттерн
// S3WorkerHealthStore: volatile-замена, KV-тик не блокируется).
[InjectAsSingleton(typeof(IEtcdWorkerHealthStore))]
public sealed class EtcdWorkerHealthStore : IEtcdWorkerHealthStore
{
    private volatile IReadOnlyList<WorkerHealth>? _current;

    public IReadOnlyList<WorkerHealth>? Current => _current;

    public void Replace(IReadOnlyList<WorkerHealth> health) => _current = health;
}

// Читатель результатов опроса (refresher вносит их успешным тиком).
public interface IEtcdWorkerHealthStore
{
    IReadOnlyList<WorkerHealth>? Current { get; }

    void Replace(IReadOnlyList<WorkerHealth> health);
}
