using AdminPanel.Core.Alerting;
using Shared.Core.DI;
using Microsoft.Extensions.Options;

namespace AdminPanel.Core.Alerting.Rules;

// wal-stream-lag (warning, arch/19 §3/§8): ACTIVE-поток отстаёт от мастера
// больше WalLagMaxSegments сегментов либо молчит дольше WalStaleSec (transient —
// воркер ретраит тиками; алерт информирует, не паникует).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class WalStreamLagRule(IOptions<AlertsOptions> options) : IAlertRule
{
    public const string KindName = "wal-stream-lag";

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        var nowUnix = context.NowUtc.ToUnixTimeSeconds();
        foreach (var backups in snapshot.Backups)
        foreach (var (shard, wal) in backups.Shards ?? new Dictionary<string, WalStreamInfo?>())
        {
            if (wal is not { State: WalStreamInfoState.Active })
                continue;

            // только живой Active-кластер: демонтаж сам останавливает поток
            var cluster = snapshot.Clusters.FirstOrDefault(
                c => c.Name == backups.Cluster && c.State == ClusterState.Active
                     && c.Shards.Any(s => s.Name == shard));
            if (cluster is null)
                continue;

            var lagHit = wal.LagSegments is { } lag && lag >= options.Value.WalLagMaxSegments;
            var staleHit = nowUnix - wal.LastUploadedUnix > options.Value.WalStaleSec;
            if (!lagHit && !staleHit)
                continue;

            yield return new Alert(
                $"{KindName}:{backups.Cluster}/{shard}",
                AlertSeverity.Warning,
                KindName,
                $"{backups.Cluster}/{shard}",
                lagHit
                    ? $"WAL stream of shard {shard} of cluster {backups.Cluster} lags by {wal.LagSegments} segments (threshold {options.Value.WalLagMaxSegments})"
                    : $"WAL stream of shard {shard} of cluster {backups.Cluster} has been silent for {nowUnix - wal.LastUploadedUnix} s (threshold {options.Value.WalStaleSec})",
                new Dictionary<string, string>
                {
                    ["lagSegments"] = wal.LagSegments?.ToString() ?? "",
                    ["lagThreshold"] = options.Value.WalLagMaxSegments.ToString(),
                    ["lastUploadedUnix"] = wal.LastUploadedUnix.ToString(),
                    ["staleSec"] = options.Value.WalStaleSec.ToString(),
                },
                null,
                "check the receivers' status keys /pgworker/backups/<C>/<X>/agent/<N> in etcd and the walreceiver log inside the node containers; check S3 availability (transient — self-healing, arch/19 §3)",
                AlertRemedy.WorkerAuto,
                "transient degradation: the receiver retries inside the node container (supervisord); if it persists — check backup_exec credentials / S3 storage availability");
        }
    }
}
