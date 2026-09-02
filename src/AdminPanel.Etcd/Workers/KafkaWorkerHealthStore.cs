using AdminPanel.Core;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Etcd.Workers;

// Стор результатов опроса /healthz инстансов KafkaWorker (t09; arch/adminpanel/02
// §2.3.2): poller пишет, kafka-refresher вносит готовым в снапшот (паттерн
// WorkerHealthStore: volatile-замена, KV-тик не блокируется).
[InjectAsSingleton(typeof(IKafkaWorkerHealthStore))]
public sealed class KafkaWorkerHealthStore : IKafkaWorkerHealthStore
{
    private volatile IReadOnlyList<WorkerHealth>? _current;

    public IReadOnlyList<WorkerHealth>? Current => _current;

    public void Replace(IReadOnlyList<WorkerHealth> health) => _current = health;
}
