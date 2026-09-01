using PgWorker.Core.Model;

namespace PgWorker.Core.Planning;

/// <summary>
/// Сходимость плана портов с фактом занятости (spec §3.7 Д1, arch/14 §5 A P1):
/// закрепление, не подтверждённое фактом своего живого контейнера и занятое
/// чужим (docker-биндинг соседа минус свои ∪ portalloc-записи соседей),
/// снимается — PortAllocator выделит ноде свободные порты, EnsureNode создаст
/// контейнер в том же тике. object-записи (усыновлённые) не трогаются (R9).
/// Переиспользуется provision (P1) и adopt (AD2').
/// </summary>
public static class PortPlanConvergence
{
    public static bool DetachColliding(
        Dictionary<string, NodeAddress> existing,
        IReadOnlySet<(string Host, int Port)> selfFact,
        IReadOnlySet<(string Host, int Port)> foreign)
    {
        var colliding = new List<string>();
        foreach (var (key, addr) in existing)
        {
            if (addr.Object is not null)
                continue; // усыновлённая (object) — чужой контейнер, не трогаем (R9)
            var ports = new[] { addr.Ports.Pg, addr.Ports.Patroni, addr.Ports.Doorman };
            if (ports.All(p => selfFact.Contains((addr.Host, p))))
                continue; // подтверждено фактом своего живого контейнера (spec §8.10)
            if (ports.Any(p => foreign.Contains((addr.Host, p))))
                colliding.Add(key);
        }

        foreach (var key in colliding)
            existing.Remove(key);
        return colliding.Count > 0;
    }
}
