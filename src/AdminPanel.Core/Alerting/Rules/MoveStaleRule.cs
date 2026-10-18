using AdminPanel.Core.Alerting;
using Shared.Core.DI;
using Microsoft.Extensions.Options;

namespace AdminPanel.Core.Alerting.Rules;

// move-stale (warning): status-ключ не-ACTIVE без прогресса дольше StaleMoveSeconds (arch/03 §4).
// Условия каталога независимы: FROZEN/ABORTING старше порога тоже stale (spec §3.12).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class MoveStaleRule(IOptions<AlertsOptions> options) : IAlertRule
{
    public const string KindName = "move-stale";

    // Каталожный дефолт 600 c — фолбэк при опечатке конфига AdminPanel:Alerts (spec §3.11).
    public const int DefaultSeconds = 600;

    public string Kind => KindName;

    private int ThresholdSeconds
        => options.Value.StaleMoveSeconds > 0 ? options.Value.StaleMoveSeconds : DefaultSeconds;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        var nowUnix = context.NowUtc.ToUnixTimeSeconds();
        foreach (var cluster in snapshot.Clusters)
        foreach (var bucket in cluster.Buckets)
        {
            if (bucket.State == BucketState.Active)
                continue;

            if (bucket.State == BucketState.NotInitialized)
                continue; // не переезд: начальное состояние создаваемого кластера (arch/03 §4)

            var stamp = MoveAge.Stamp(bucket);
            if (stamp is null || nowUnix - stamp.Value <= ThresholdSeconds)
                continue; // нет меры возраста (spec §4.2) либо прогресс свежий

            var age = nowUnix - stamp.Value;
            yield return new Alert(
                $"{KindName}:{cluster.Name}/bucket_{bucket.Id}",
                AlertSeverity.Warning,
                KindName,
                $"{cluster.Name}/bucket_{bucket.Id}",
                $"move of bucket_{bucket.Id} of cluster {cluster.Name} ({StateName(bucket.State)}) without progress for {age} s — threshold {ThresholdSeconds} s",
                new Dictionary<string, string>
                {
                    ["state"] = StateName(bucket.State),
                    ["ageSeconds"] = age.ToString(),
                    ["thresholdSeconds"] = ThresholdSeconds.ToString(),
                    ["updatedUnix"] = stamp.Value.ToString(),
                },
                null,
                "move status is not-ACTIVE with no progress beyond the threshold: every SYNCING/FROZEN/ABORTING status must either advance (updated) or be cleared; a stuck status blocks moves and shard decommissioning",
                AlertRemedy.WorkerAuto,
                "PgWorker move reparator (feat-pgworker-adopt-repair) will close it; if it hangs, it is a worker defect");
        }
    }

    // Строка канона статус-ключей для message/details (spec §3.8); Core не зависит от Api (arch/01 §1).
    private static string StateName(BucketState state)
        => state switch
        {
            BucketState.Syncing => "SYNCING",
            BucketState.Frozen => "FROZEN",
            _ => "ABORTING",
        };
}
