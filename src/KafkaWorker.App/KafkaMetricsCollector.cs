using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Provisioning.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace KafkaWorker.App;

// Коллектор kafka-метрик (t04, arch/18 §4): hosted-сервис с тиком
// KafkaWorker:Metrics:CollectIntervalSec; по Active-кластерам (снапшот etcd)
// через IKafkaAdminClientFactory — группы/committed/watermarks → consumer-lag,
// describe топиков → USR. Сбор read-only вне клэймов, без ретраев; ошибка
// кластера не валит тик — LastSuccess обновляется только при полном успехе
// (консервативно, алерт KafkaCollectorStalled §3.7).
public sealed class KafkaMetricsCollector(
    int collectIntervalSec,
    Func<CancellationToken, Task<Result<IReadOnlyList<KafkaClusterSnapshot>>>> clustersSnapshot,
    IKafkaAdminClientFactory adminFactory,
    KafkaMetricsState state,
    TimeProvider clock,
    ILogger<KafkaMetricsCollector> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // <=0 → 30 с лог-предупреждением (паттерн SnapshotRefresher).
        var interval = collectIntervalSec <= 0
            ? 30
            : collectIntervalSec;
        if (collectIntervalSec <= 0)
            logger.LogWarning(
                "KafkaWorker:Metrics:CollectIntervalSec={Value} <= 0 — используется дефолт 30 с", collectIntervalSec);

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
                logger.LogWarning(ex, "тик KafkaMetricsCollector упал: {Message}", ex.Message);
            }

            await Task.Delay(TimeSpan.FromSeconds(interval), stoppingToken);
        }
    }

    // Ядро тика — публично для unit-тестов без хоста (паттерн RefreshOnceAsync панели).
    public async Task CollectOnceAsync(CancellationToken ct)
    {
        var snapshots = await clustersSnapshot(ct);
        if (!snapshots.IsSuccess)
        {
            logger.LogWarning("снапшот кластеров недоступен: {Message}", snapshots.Error!.Message);
            return; // коллектор пропустит тик — KafkaCollectorStalled сработает по свежести
        }

        var allOk = true;
        foreach (var snap in snapshots.Value)
        {
            // Только Active: Config.State == null (невыполненные заявки не трогаем,
            // arch/15 §2.1, ревью Ф4-6); дискавери-поля обязательны (не поднят).
            if (snap.Config.State is not null || snap.Endpoints is null || snap.AppUser is null || snap.AppPassword is null)
                continue;

            if (!await TryCollectClusterAsync(snap, ct))
                allOk = false;
        }

        if (allOk)
            state.MarkSuccess(clock.GetUtcNow());
    }

    // Сбор одного кластера: один AdminClient-коннект за тик, без ретраев (M2/S4).
    // false — ошибка сбора (тик жив, LastSuccess не обновляется).
    private async Task<bool> TryCollectClusterAsync(KafkaClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Cluster;
        try
        {
            await using var admin = adminFactory.Create(snap.Endpoints!, snap.AppUser!, snap.AppPassword!);

            var lag = new List<((string, string, string), long)>();
            var groups = await admin.ListGroupsAsync(ct);
            if (!groups.IsSuccess)
            {
                logger.LogWarning("кластер {Cluster}: ListGroups не удался: {Message}", cluster, groups.Error!.Message);
                return false;
            }

            foreach (var group in groups.Value)
            {
                var committed = await admin.ListConsumerGroupOffsetsAsync(group.Group, ct);
                if (!committed.IsSuccess)
                {
                    logger.LogWarning("кластер {Cluster}: committed {Group} не удался: {Message}",
                        cluster, group.Group, committed.Error!.Message);
                    return false;
                }

                if (committed.Value.Count == 0)
                    continue;

                var watermarks = await admin.ListOffsetsAsync(
                    committed.Value.Select(c => new KafkaTopicPartition(c.Topic, c.Partition)).ToList(), ct);
                if (!watermarks.IsSuccess)
                {
                    logger.LogWarning("кластер {Cluster}: watermarks {Group} не удались: {Message}",
                        cluster, group.Group, watermarks.Error!.Message);
                    return false;
                }

                // Лаг по (group, topic) = Σ max(0, watermark − committed) по партициям.
                var wmByPartition = watermarks.Value
                    .ToDictionary(w => (w.Topic, w.Partition), w => w.Offset);
                lag.AddRange(committed.Value
                    .GroupBy(c => c.Topic)
                    .Select(g => (
                        (cluster, group.Group, g.Key),
                        g.Sum(c => Math.Max(0, wmByPartition.GetValueOrDefault((c.Topic, c.Partition)) - c.Offset)))));
            }

            // USR: партиции топика, у которых ISR уже assignment; ISR null (данных
            // нет у адаптера) — топик пропускаем, консервативно без серии.
            var usr = new List<((string, string), int)>();
            var topics = await admin.DescribeTopicsAsync(includeInternal: false, ct);
            if (!topics.IsSuccess)
            {
                logger.LogWarning("кластер {Cluster}: describe топиков не удался: {Message}", cluster, topics.Error!.Message);
                return false;
            }

            foreach (var topic in topics.Value)
            {
                if (topic.IsrPerPartition is null)
                    continue; // ISR не задан — USR не считаем (паттерн фейков)
                var underReplicated = topic.IsrPerPartition
                    .Select((isr, i) => (Isr: isr, Replicas: topic.ReplicasPerPartition[i]))
                    .Count(p => p.Isr.Count < p.Replicas.Count);
                usr.Add(((cluster, topic.Topic), underReplicated));
            }

            state.UpdateCluster(cluster, lag, usr);
            return true;
        }
        catch (Exception ex)
        {
            // Пассивный наблюдатель: исключение сбора — не роняет тик.
            logger.LogWarning(ex, "кластер {Cluster}: сбор метрик упал: {Message}", cluster, ex.Message);
            return false;
        }
    }
}
