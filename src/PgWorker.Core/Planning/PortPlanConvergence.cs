using PgWorker.Core.Model;

namespace PgWorker.Core.Planning;

/// <summary>
/// Сходимость плана портов с фактом занятости (spec §3.7 Д1, arch/14 §5 A P1):
/// закрепление, не подтверждённое фактом СВОЕГО живого контейнера и занятое
/// любой фактической публикацией (docker-биндинг соседа — в т.ч. ноды СВОЕГО
/// кластера: дубликат внутри кластера — такой же конфликт — ∪ portalloc-записи
/// соседей), снимается — PortAllocator выделит ноде свободные порты, EnsureNode
/// создаст контейнер в том же тике. object-записи (усыновлённые) не трогаются
/// (R9). Подтверждение per-node: факт контейнера самой ноды подтверждает только
/// её запись (агрегатное вычитание всех «своих» портов маскировало дубликаты).
/// Переиспользуется provision (P1) и adopt (AD2').
/// </summary>
public static class PortPlanConvergence
{
    /// <summary>Снять коллизионные закрепления. busy — ВСЯ фактическая занятость
    /// (docker-публикации ∪ portalloc соседей), без вычитания своих портов.</summary>
    public static bool DetachColliding(
        Dictionary<string, NodeAddress> existing,
        IReadOnlyDictionary<string, IReadOnlySet<(string Host, int Port)>> selfFactByNode,
        IReadOnlySet<(string Host, int Port)> busy)
    {
        var colliding = new List<string>();
        foreach (var (key, addr) in existing)
        {
            if (addr.Object is not null)
                continue; // усыновлённая (object) — чужой контейнер, не трогаем (R9)
            if (selfFactByNode.TryGetValue(key, out var own) && MatchesFact(addr, own))
                continue; // подтверждено фактом своего живого контейнера (spec §8.10)
            if (Ports(addr).Any(p => busy.Contains((addr.Host, p))))
                colliding.Add(key); // порт фактически занят кем угодно, кроме собственной записи
        }

        foreach (var key in colliding)
            existing.Remove(key);
        return colliding.Count > 0;
    }

    /// <summary>Порты фактов подтверждённых записей (запись совпадает с фактом
    /// своего контейнера): вычитаются из занятости для PortAllocator — иначе
    /// переиспользование валидных записей ломалось и EnsureNode пересоздавал
    /// бы живые контейнеры.</summary>
    public static IReadOnlySet<(string Host, int Port)> ConfirmedFact(
        IReadOnlyDictionary<string, NodeAddress> existing,
        IReadOnlyDictionary<string, IReadOnlySet<(string Host, int Port)>> selfFactByNode)
    {
        var confirmed = new HashSet<(string, int)>();
        foreach (var (key, addr) in existing)
            if (selfFactByNode.TryGetValue(key, out var own) && MatchesFact(addr, own))
                confirmed.UnionWith(own);
        return confirmed;
    }

    /// <summary>Быстрый пред-выход (t90): все wanted-записи закреплены и detach
    /// их снять не может — object-записи не трогаются (R9), прочие подтверждены
    /// фактом своего живого контейнера (MatchesFact). Чтение busy под глобальным
    /// portalloc-клэймом ничего бы не изменило — лок можно не брать (тики
    /// waiting-patroni не соперничают за клэйм, arch/14 §2.4).</summary>
    public static bool AllConfirmed(
        IReadOnlyDictionary<string, NodeAddress> existing,
        IReadOnlyDictionary<string, IReadOnlySet<(string Host, int Port)>> selfFactByNode,
        IReadOnlyCollection<string> wanted)
        => wanted.All(key =>
            existing.TryGetValue(key, out var addr)
            && (addr.Object is not null
                || (selfFactByNode.TryGetValue(key, out var own) && MatchesFact(addr, own))));

    // Запись подтверждена фактом: все её НЕнулевые порты публикует контейнер
    // самой ноды. Нулевые игнорируются: EnableDoorman=false (R1) — запись без
    // пулера (Doorman=0), порт 0 в факт не попадает никогда (merge тем же тиком
    // уже нормализовал запись к факту) — иначе режим без пулера дал бы вечный
    // detach → бесконечный recreate контейнеров.
    private static bool MatchesFact(NodeAddress addr, IReadOnlySet<(string Host, int Port)> own)
        => Ports(addr).Where(p => p > 0).All(p => own.Contains((addr.Host, p)));

    private static int[] Ports(NodeAddress addr)
        => [addr.Ports.Pg, addr.Ports.Patroni, addr.Ports.Doorman];
}
