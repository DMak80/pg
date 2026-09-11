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
        BackupsParseResult backups,
        WorkerEndpointsParseResult pgWorkerEndpoints,
        WorkJournalParseResult work,
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
            backups.Clusters,
            pgWorkerEndpoints.Endpoints,
            work.Items,
            [], // WorkerHealth вносит SnapshotRefresher из IWorkerHealthStore (spec D4)
            [],
            [],
            [.. clusters.Errors, .. service.Errors, .. moves.Errors, .. backups.Errors, .. pgWorkerEndpoints.Errors, .. work.Errors],
            clusters.UnknownKeyCount + service.UnknownKeyCount,
            backups.Storage); // /pgworker/backups/storage (t06)
}
