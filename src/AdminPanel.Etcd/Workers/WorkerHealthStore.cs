using AdminPanel.Core;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Etcd.Workers;

// Стор результатов опроса /healthz (паттерн ProbeResultsStore): poller пишет,
// refresher вносит готовым в снапшот — KV-тик не блокируется (arch/adminpanel/02 §4).
[InjectAsSingleton(typeof(IWorkerHealthStore))]
public sealed class WorkerHealthStore : IWorkerHealthStore
{
    private volatile IReadOnlyList<WorkerHealth>? _current;

    public IReadOnlyList<WorkerHealth>? Current => _current;

    public void Replace(IReadOnlyList<WorkerHealth> health) => _current = health;
}
