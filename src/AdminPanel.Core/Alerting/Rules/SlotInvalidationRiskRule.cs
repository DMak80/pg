using AdminPanel.Core.Alerting;
using AdminPanel.Infrastructure.DI;
using Microsoft.Extensions.Options;

namespace AdminPanel.Core.Alerting.Rules;

// slot-invalidation-risk (warning): остаток safe_wal_size < порога — риск среза
// слота ДО потери (P4, arch/03 §4); null (max_slot_wal_keep_size не задан) — риска нет.
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class SlotInvalidationRiskRule(IOptions<AlertsOptions> options) : IAlertRule
{
    public const string KindName = "slot-invalidation-risk";

    // Каталожный дефолт 1 GiB — фолбэк при опечатке конфига (spec §3.8).
    public const long DefaultBytes = 1024L * 1024 * 1024;

    public string Kind => KindName;

    private long ThresholdBytes
        => options.Value.SlotSafeWalSizeBytes > 0 ? options.Value.SlotSafeWalSizeBytes : DefaultBytes;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        foreach (var (cluster, shard, slot) in SlotLagHighRule.Slots(snapshot))
        {
            var safe = slot.SafeWalSizeBytes;
            if (safe is not > 0 || safe >= ThresholdBytes)
                continue;

            yield return new Alert(
                $"{KindName}:{cluster.Name}/{shard.Name}/{slot.SlotName}",
                AlertSeverity.Warning,
                KindName,
                $"{cluster.Name}/{shard.Name}/{slot.SlotName}",
                $"слоту {slot.SlotName} шарда {cluster.Name}/{shard.Name} осталось {safe} байт WAL до среза (порог {ThresholdBytes} байт, P4)",
                new Dictionary<string, string>
                {
                    ["safeWalSizeBytes"] = safe.Value.ToString(),
                    ["thresholdBytes"] = ThresholdBytes.ToString(),
                },
                null);
        }
    }
}
