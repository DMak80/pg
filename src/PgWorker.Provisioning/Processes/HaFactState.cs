using Shared.Etcd.Coordination;

namespace PgWorker.Provisioning.Processes;

/// <summary>
/// Чистые переходы HA-фактов надзора (arch/14 §5 C): решения «открыть/закрыть/
/// сбросить» без etcd и часов (времена приходят параметрами); NodeSupervisor
/// делегирует. Владеет последним фактом каждого вида (без истории):
/// новое событие перезаписывает, повторная детекция того же события — игнор
/// (detected первого тика).
/// </summary>
public sealed class HaFactState
{
    public HaSupervisionFact? Failover { get; private set; }
    public HaSupervisionFact? Rebuild { get; private set; }

    private HaFactState(HaSupervisionFact? failover, HaSupervisionFact? rebuild)
        => (Failover, Rebuild) = (failover, rebuild);

    // takeover-продолжение: открытые события из work-ключа
    public static HaFactState FromStored(HaSupervisionFact? failover, HaSupervisionFact? rebuild)
        => new(failover, rebuild);

    // Открытие failover (dead-ветка надзора/применённое ускорение):
    // уже открытое событие того же (shard, node) не перезаписываем.
    public void FailoverDetected(string shard, string node, string cause, long detectedUnix)
    {
        if (Failover is { } open && open.Shard == shard && open.Node == node)
            return;
        Failover = new HaSupervisionFact(shard, node, cause, detectedUnix);
    }

    // Открытие rebuild (rebuild-ветка/маркер TO_RECREATE).
    public void RebuildDetected(string shard, string node, string cause, long detectedUnix)
    {
        if (Rebuild is { } open && open.Shard == shard && open.Node == node)
            return;
        Rebuild = new HaSupervisionFact(shard, node, cause, detectedUnix);
    }

    // Тик с другим лидером scope: закрытие открытого failover этого шарда.
    public void LeaderChanged(string shard, string? leader, long nowUnix)
    {
        if (Failover is not { } open || open.Shard != shard || open.ResolvedUnix is not null)
            return;
        if (leader is null || leader == open.Node)
            return; // лидер тот же/неизвестен — событие продолжается
        Failover = open with { ResolvedUnix = nowUnix, DurationSec = nowUnix - open.DetectedUnix };
    }

    // Флап-оживание: нода жива и лидерство сохранила — факта НЕ БЫЛО.
    // Сброс применим только к ОТКРЫТОЙ записи: закрытый факт (смена уже была)
    // — реальное событие, возврат лидера его не отменяет.
    public void LeaderRecovered(string shard, string node, string? leader)
    {
        if (Failover is { } open && open.ResolvedUnix is null
            && open.Shard == shard && open.Node == node && leader == node)
            Failover = null;
    }

    // Живость ноды пробой: закрытие открытого rebuild (первый живой тик).
    public void NodeAlive(string shard, string node, long nowUnix)
    {
        if (Rebuild is not { } open || open.Shard != shard || open.Node != node || open.ResolvedUnix is not null)
            return;
        Rebuild = open with { ResolvedUnix = nowUnix, DurationSec = nowUnix - open.DetectedUnix };
    }

    public HaSupervisionFacts ToRecord() => new(Failover, Rebuild);
}
