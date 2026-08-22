using AdminPanel.Infrastructure.DI;
using AdminPanel.Infrastructure.HealthChecks;

namespace AdminPanel.Etcd;

// Чек живости etcd-цикла: без собственной логики — по состоянию refresher (spec §7.3).
// Регистрируется без тега live: /api/healthz — liveness самой панели (arch/03 §1).
// HealthCheckAbstract<T> имеет primary-конструктор T service — наследник обязан пробросить аргумент.
[InjectAsTransient]
public sealed class EtcdHealthCheck(SnapshotRefresher service)
    : HealthCheckAbstract<SnapshotRefresher>(service)
{
}
