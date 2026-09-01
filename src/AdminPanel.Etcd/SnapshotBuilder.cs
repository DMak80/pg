using AdminPanel.Core;
using AdminPanel.Etcd.Parsing;

namespace AdminPanel.Etcd;

// Сборка EtcdSnapshot из частей одного тика: чистая функция (spec §6.5).
// Alerts/Probes пусты в t03 (наполняют AlertEngine t04 и пробы t06).
public static class SnapshotBuilder
{
    public static EtcdSnapshot Build(
        TimeProvider time,
        ClustersParseResult clusters,
        ServiceParseResult service,
        IReadOnlyList<StandNode> standNodes,
        MovesParseResult moves,
        WorkerEndpointsParseResult pgWorkerEndpoints,
        IReadOnlyList<EtcdMember> members,
        IReadOnlyList<EtcdAlarm> alarms,
        EtcdStatus etcd)
        => new(
            time.GetUtcNow(),
            etcd,
            clusters.Clusters,
            service.Scopes,
            standNodes,
            moves.Tickets,
            pgWorkerEndpoints.Endpoints,
            [],
            [],
            [.. clusters.Errors, .. service.Errors, .. moves.Errors, .. pgWorkerEndpoints.Errors],
            clusters.UnknownKeyCount + service.UnknownKeyCount);
}
