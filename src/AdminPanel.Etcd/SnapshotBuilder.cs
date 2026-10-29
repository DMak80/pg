using AdminPanel.Core;
using AdminPanel.Etcd.Parsing;
using Shared.Etcd.Client;

namespace AdminPanel.Etcd;

// Сборка EtcdSnapshot из частей одного тика: чистая функция (spec §6.5).
// Alerts/Probes пусты в (наполняют AlertEngine и пробы).
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
        EtcdStatus etcd,
        WorkerCertParseResult? workerCert = null,
        (EtcdSnapshotExportInfo? Info, IReadOnlyList<KeyParseError> Errors)? etcdSnapshots = null,
        (EtcdDomainInfo? Domain, IReadOnlyList<KeyParseError> Errors)? etcdDomain = null,
        WorkerEndpointsParseResult? etcdWorkerEndpoints = null,
        WorkerCertParseResult? etcdWorkerCert = null)
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
            [.. clusters.Errors, .. service.Errors, .. moves.Errors, .. backups.Errors, .. pgWorkerEndpoints.Errors, .. work.Errors,
                .. WorkerCertParser.ErrorsOf(workerCert ?? new WorkerCertParseResult(null, null)),
                .. etcdSnapshots?.Errors ?? [],
                .. etcdDomain?.Errors ?? [],
                .. etcdWorkerEndpoints?.Errors ?? [],
                .. WorkerCertParser.ErrorsOf(etcdWorkerCert ?? new WorkerCertParseResult(null, null))],
            clusters.UnknownKeyCount + service.UnknownKeyCount,
            backups.Storage, // /pgworker/backups/storage 
            backups.Orphans, // /pgworker/backups/orphans 
            null, // S3Storage вносит SnapshotRefresher из стора 
            workerCert?.Cert, // /workers/api_tls/pgworker (adminpanel/02 §9.9)
            etcdSnapshots?.Info, // /etcdworker/snapshots (adminpanel/02 §2.3.5)
            etcdDomain?.Domain, // /etcd/config + /etcd/nodes/ (02 §2.3.5)
            etcdWorkerEndpoints?.Endpoints ?? [], // /etcdworker/api/ (02 §2.3.5)
            [], // EtcdWorkerHealth вносит SnapshotRefresher из стора
            null, // EtcdBackupTarget вносит SnapshotRefresher из S3-домена
            etcdWorkerCert?.Cert); // /workers/api_tls/etcdworker (02 §9.9)
}
