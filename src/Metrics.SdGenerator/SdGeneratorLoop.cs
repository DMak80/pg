using Shared.Core;
using Shared.Etcd.Client;

namespace Metrics.SdGenerator;

// Тик генератора (arch/18 §5.4): failover-Range префикса portalloc → маппинг →
// атомарная запись при diff → отметка успеха. Два file_sd-файла ОДНИМ проходом
// (t25, arch/18 §5.4/§8): patroni-nodes.json (порт 8008) + walreceiver-nodes.json
// (порт 8080, те же группы). Консервативная свежесть: ошибка чтения etcd не
// трогает ни файлы таргетов, ни метрику last_success.
public sealed class SdGeneratorLoop(
    IEtcdGateway etcd, string[] endpoints, SdFileWriter writer, SdFileWriter walWriter,
    SdGeneratorMetrics metrics, ILogger<SdGeneratorLoop> logger)
{
    /// <summary>true — успешный тик (пустой префикс = успех); false — ошибка чтения
    /// (warning-лог, файлы и last_success НЕ тронуты).</summary>
    public async Task<bool> TickAsync(CancellationToken ct)
    {
        var range = await EtcdEndpointFailover.CallAsync(endpoints, ep => etcd.RangeAsync(ep, TargetMapping.PortallocPrefix, ct));
        if (!range.IsSuccess)
        {
            logger.LogWarning("portalloc read failed: {Error}", range.Error);
            return false;
        }

        var groups = TargetMapping.Map(range.Value, (c, e) => logger.LogWarning("skipping portalloc {Cluster}: {Error}", c, e));
        writer.WriteIfChanged(TargetMapping.Serialize(groups));
        walWriter.WriteIfChanged(TargetMapping.SerializeWal(groups));
        metrics.MarkSuccess();
        return true;
    }
}
