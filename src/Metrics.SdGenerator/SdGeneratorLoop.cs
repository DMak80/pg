using Shared.Core;
using Shared.Etcd.Client;

namespace Metrics.SdGenerator;

// Тик генератора (arch/18 §5.4): failover-Range префикса portalloc → маппинг →
// атомарная запись при diff → отметка успеха. Консервативная свежесть: ошибка
// чтения etcd не трогает ни файл таргетов, ни метрику last_success.
public sealed class SdGeneratorLoop(
    IEtcdGateway etcd, string[] endpoints, SdFileWriter writer,
    SdGeneratorMetrics metrics, ILogger<SdGeneratorLoop> logger)
{
    /// <summary>true — успешный тик (пустой префикс = успех); false — ошибка чтения
    /// (warning-лог, файл и last_success НЕ тронуты).</summary>
    public async Task<bool> TickAsync(CancellationToken ct)
    {
        var range = await EtcdFailover.CallAsync(endpoints, ep => etcd.RangeAsync(ep, TargetMapping.PortallocPrefix, ct));
        if (!range.IsSuccess)
        {
            logger.LogWarning("чтение portalloc не удалось: {Error}", range.Error);
            return false;
        }

        var groups = TargetMapping.Map(range.Value, (c, e) => logger.LogWarning("пропуск portalloc {Cluster}: {Error}", c, e));
        writer.WriteIfChanged(TargetMapping.Serialize(groups));
        metrics.MarkSuccess();
        return true;
    }
}
