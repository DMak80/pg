using Microsoft.Extensions.Logging;
using OwnS3.Replication.Cursors;
using OwnS3.Replication.Outbox;
using OwnS3.Replication.Lww;

namespace OwnS3.Replication;

// Цикл обслуживания журнала и tombstones (канон 07 §4/§7/§8-а; вызыватель —
// ReplicationCoordinator: при старте и периодически 60 с). Отдельный класс —
// тестируем без хоста.
public sealed class ReplicationMaintenance(OutboxJournal journal, TargetCursors cursors,
    ReplicationTargets targets, TombstoneStore tombstones, ReplicationOptions options,
    TimeProvider timeProvider, ILogger<ReplicationMaintenance> logger)
{
    // Проход обслуживания:
    // 1) штатный тримминг: Trim(min(watermark приёма АКТИВНЫХ соседей)); соседей нет —
    //    тримминг до хвоста ТОЛЬКО при активной репликации (иначе журнал и так пуст);
    // 2) BudgetExceeded → ForceTrimToBudget + лог-событие «потеряны уведомления для
    //    отстающих соседей (принудительный тримминг)» (алерт-метрика — в самом журнале);
    // 3) retention-чистка tombstones: Cleanup(now, retentionHours) + лог при удалённых > 0.
    public Task RunMaintenancePassAsync(CancellationToken ct)
    {
        if (targets.Active)
        {
            var minWatermark = targets.Snapshot
                .Select(t => cursors.GetAccepted(t.Name))
                .DefaultIfEmpty(long.MaxValue - 1)
                .Min();
            // Сосед без файла курсора (−1, новый) не удерживает журнал (он с конца)
            if (minWatermark < 0)
                minWatermark = long.MaxValue - 1;
            if (journal.Trim(minWatermark))
                logger.LogInformation("Журнал доставки подстрижен по watermark {MinAccepted}", minWatermark);

            if (journal.BudgetExceeded)
            {
                journal.ForceTrimToBudget();
                // Лог-событие потерь уведомлений (алерт-метрика budget_exceeded — в журнале)
                logger.LogWarning(
                    "Бюджет журнала превышен — принудительный тримминг: потеряны уведомления для отстающих соседей, дыры закроет reconcile");
            }
        }

        var removed = tombstones.Cleanup(timeProvider.GetUtcNow(), options.TombstoneRetentionHours);
        if (removed > 0)
            logger.LogInformation("Retention-чистка tombstones: удалено {Count} (retention {Hours} ч)",
                removed, options.TombstoneRetentionHours);
        return Task.CompletedTask;
    }
}
