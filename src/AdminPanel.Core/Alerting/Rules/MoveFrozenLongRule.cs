using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;
using Microsoft.Extensions.Options;

namespace AdminPanel.Core.Alerting.Rules;

// move-frozen-long (critical): FROZEN дольше FrozenSeconds — cutover обязан быть секундами (arch/03 §4).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class MoveFrozenLongRule(IOptions<AlertsOptions> options) : IAlertRule
{
    public const string KindName = "move-frozen-long";

    // Каталожный дефолт 60 c — фолбэк при опечатке конфига (spec §3.11).
    public const int DefaultSeconds = 60;

    public string Kind => KindName;

    private int ThresholdSeconds
        => options.Value.FrozenSeconds > 0 ? options.Value.FrozenSeconds : DefaultSeconds;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        var nowUnix = context.NowUtc.ToUnixTimeSeconds();
        foreach (var cluster in snapshot.Clusters)
        foreach (var bucket in cluster.Buckets.Where(b => b.State == BucketState.Frozen))
        {
            var stamp = MoveAge.Stamp(bucket);
            if (stamp is null || nowUnix - stamp.Value <= ThresholdSeconds)
                continue;

            var age = nowUnix - stamp.Value;
            yield return new Alert(
                $"{KindName}:{cluster.Name}/bucket_{bucket.Id}",
                AlertSeverity.Critical,
                KindName,
                $"{cluster.Name}/bucket_{bucket.Id}",
                $"бакет bucket_{bucket.Id} кластера {cluster.Name} в FROZEN {age} c — cutover обязан быть секундами",
                new Dictionary<string, string>
                {
                    ["ageSeconds"] = age.ToString(),
                    ["thresholdSeconds"] = ThresholdSeconds.ToString(),
                    ["updatedUnix"] = stamp.Value.ToString(),
                },
                null,
                "бакет в FROZEN дольше окна: cutover обязан быть секундами (запись остановлена только на переключение), длительный FROZEN — зависание переезда на ровном месте",
                AlertRemedy.WorkerAuto,
                "репаратор переездов PgWorker (feat-pgworker-adopt-repair) закроет; висит — дефект воркера");
        }
    }
}
