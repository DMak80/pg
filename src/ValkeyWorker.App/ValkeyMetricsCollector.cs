using System.Globalization;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using ValkeyWorker.Core.Model;
using ValkeyWorker.Core.Valkey;

namespace ValkeyWorker.App;

// Коллектор valkey-метрик (t05, arch/18 §4.2; зеркало KafkaMetricsCollector):
// hosted-сервис с тиком ValkeyWorker:Metrics:CollectIntervalSec; по Active-кластерам
// (снапшот /valkey/clusters/) и адресам из /valkeyworker/portalloc/ — ОДНА проба
// INFO all на ноду за тик. Сбор read-only вне клэймов, без backoff (лёгкая
// TCP-проба); ошибка ноды не валит тик — LastSuccess обновляется только при
// полном успехе всех (cluster, node)-проб тика (консервативно, алерт §5.2).
public sealed class ValkeyMetricsCollector(
    int collectIntervalSec,
    Func<CancellationToken, Task<Result<IReadOnlyList<ValkeyClusterSnapshot>>>> clustersSnapshot,
    Func<CancellationToken, Task<Result<IReadOnlyDictionary<string, IReadOnlyDictionary<string, NodeAddress>>>>> portAllocSnapshot,
    IValkeyConnection valkey,
    string? advertisedClientHost,
    ValkeyMetricsState state,
    TimeProvider clock,
    ILogger<ValkeyMetricsCollector> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // <=0 → 30 с лог-предупреждением (паттерн SnapshotRefresher/Kafka-коллектора).
        var interval = EffectiveIntervalSec(collectIntervalSec);
        if (collectIntervalSec <= 0)
            logger.LogWarning(
                "ValkeyWorker:Metrics:CollectIntervalSec={Value} <= 0 — используется дефолт 30 с",
                collectIntervalSec);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await CollectOnceAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break; // штатная остановка host'а
            }
            catch (Exception ex)
            {
                // Исключение тика — лог warning, тик жив (метрики не роняют воркер).
                logger.LogWarning(ex, "тик ValkeyMetricsCollector упал: {Message}", ex.Message);
            }

            await Task.Delay(TimeSpan.FromSeconds(interval), stoppingToken);
        }
    }

    // Ядро тика — публично для unit-тестов без хоста (паттерн Kafka-коллектора).
    public async Task CollectOnceAsync(CancellationToken ct)
    {
        var snapshots = await clustersSnapshot(ct);
        if (!snapshots.IsSuccess)
        {
            logger.LogWarning("снапшот кластеров недоступен: {Message}", snapshots.Error!.Message);
            return; // пропуск тика — LastSuccess не двигается (Stalled сработает)
        }

        var allocs = await portAllocSnapshot(ct);
        if (!allocs.IsSuccess)
        {
            logger.LogWarning("снапшот portalloc недоступен: {Message}", allocs.Error!.Message);
            return;
        }

        var allOk = true;
        foreach (var snap in snapshots.Value)
        {
            // Только Active (Config.State == null — невыполненные заявки не трогаем,
            // зеркало ревью Ф4-6; битый Config — консервативный skip) и полные
            // дискавери-креды (иначе проба невозможна — паттерн NodeSupervisor).
            if (snap.Config is null || snap.Config.State is not null
                || snap.AdminUser is null || snap.AdminPassword is null)
                continue;

            var addresses = allocs.Value.GetValueOrDefault(snap.Cluster) ?? EmptyAddresses;
            if (!await TryCollectClusterAsync(snap, addresses, ct))
                allOk = false;
        }

        if (allOk)
            state.MarkSuccess(clock.GetUtcNow());
    }

    private static readonly IReadOnlyDictionary<string, NodeAddress> EmptyAddresses =
        new Dictionary<string, NodeAddress>();

    // Сбор кластера: одна INFO all на ноду, без ретраев; false — ошибка сбора
    // (тик жив, LastSuccess не обновляется; остальные кластеры тика собираются).
    private async Task<bool> TryCollectClusterAsync(
        ValkeyClusterSnapshot snap, IReadOnlyDictionary<string, NodeAddress> addresses, CancellationToken ct)
    {
        var cluster = snap.Cluster;
        try
        {
            var samples = new List<ValkeyNodeSample>();
            foreach (var node in snap.Nodes.Keys.Order())
            {
                // Нода без portalloc-записи — пропуск (лестница E9 — забота надзора C,
                // коллектор только читает); хост пробы — advertised ?? portalloc.host.
                if (!addresses.TryGetValue(node, out var address))
                    continue;

                var info = await valkey.InfoAllAsync(
                    new ValkeyEndpoint(
                        advertisedClientHost ?? address.Host, address.ClientPort,
                        snap.AdminUser!, snap.AdminPassword!),
                    ct);
                if (!info.IsSuccess)
                {
                    logger.LogWarning("кластер {Cluster}: INFO {Node} не удался: {Message}",
                        cluster, node, info.Error!.Message);
                    return false;
                }

                samples.Add(ToSample(node, info.Value));
            }

            state.UpdateCluster(cluster, samples);
            return true;
        }
        catch (Exception ex)
        {
            // Пассивный наблюдатель: исключение сбора — не роняет тик.
            logger.LogWarning(ex, "кластер {Cluster}: сбор метрик упал: {Message}", cluster, ex.Message);
            return false;
        }
    }

    // INFO-словарь → срез серий §2.6: только поля словаря; отсутствующие/нечисловые
    // → null (серия не эмитится — консервативно); role — только master|slave.
    internal static ValkeyNodeSample ToSample(string node, IReadOnlyDictionary<string, string> info) => new(
        node,
        Long(info, "used_memory"),
        Long(info, "maxmemory"),
        Long(info, "connected_clients"),
        Long(info, "blocked_clients"),
        Long(info, "evicted_keys"),
        Long(info, "expired_keys"),
        Long(info, "keyspace_hits"),
        Long(info, "keyspace_misses"),
        Long(info, "instantaneous_ops_per_sec"),
        Long(info, "total_connections_received"),
        Long(info, "rejected_connections"),
        Long(info, "total_commands_processed"),
        info.GetValueOrDefault("role") is "master" or "slave" ? info["role"] : null,
        Long(info, "connected_slaves"));

    private static long? Long(IReadOnlyDictionary<string, string> info, string key)
        => long.TryParse(info.GetValueOrDefault(key), NumberStyles.Integer,
               CultureInfo.InvariantCulture, out var value)
            ? value
            : null;

    // <=0 → 30 с (internal — юнит-тест дефолта).
    internal static int EffectiveIntervalSec(int configured) => configured <= 0 ? 30 : configured;
}
