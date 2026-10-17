using AdminPanel.Core;
using Shared.Core.DI;

namespace AdminPanel.Etcd.Workers;

// Стор результатов опроса /healthz инстансов S3Worker (arch/adminpanel/02
// §2.3.4): poller пишет, s3-refresher вносит готовым в снапшот (паттерн
// ValkeyWorkerHealthStore: volatile-замена, KV-тик не блокируется).
[InjectAsSingleton(typeof(IS3WorkerHealthStore))]
public sealed class S3WorkerHealthStore : IS3WorkerHealthStore
{
    private volatile IReadOnlyList<WorkerHealth>? _current;

    public IReadOnlyList<WorkerHealth>? Current => _current;

    public void Replace(IReadOnlyList<WorkerHealth> health) => _current = health;
}

// Читатель результатов опроса (refresher вносит их успешным тиком).
public interface IS3WorkerHealthStore
{
    IReadOnlyList<WorkerHealth>? Current { get; }

    void Replace(IReadOnlyList<WorkerHealth> health);
}
