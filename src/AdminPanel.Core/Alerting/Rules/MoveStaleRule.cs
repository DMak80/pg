using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;
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

            var stamp = MoveAge.Stamp(bucket);
            if (stamp is null || nowUnix - stamp.Value <= ThresholdSeconds)
                continue; // нет меры возраста (spec §4.2) либо прогресс свежий

            var age = nowUnix - stamp.Value;
            yield return new Alert(
                $"{KindName}:{cluster.Name}/bucket_{bucket.Id}",
                AlertSeverity.Warning,
                KindName,
                $"{cluster.Name}/bucket_{bucket.Id}",
                $"переезд bucket_{bucket.Id} кластера {cluster.Name} ({StateName(bucket.State)}) без прогресса {age} c — порог {ThresholdSeconds} c",
                new Dictionary<string, string>
                {
                    ["state"] = StateName(bucket.State),
                    ["ageSeconds"] = age.ToString(),
                    ["thresholdSeconds"] = ThresholdSeconds.ToString(),
                    ["updatedUnix"] = stamp.Value.ToString(),
                },
                null);
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
