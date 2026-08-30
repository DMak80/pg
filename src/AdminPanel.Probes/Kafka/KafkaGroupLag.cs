namespace AdminPanel.Probes.Kafka;

/// <summary>
/// Чистая функция лага группы (план C3-шаг 1): (endOffsets, committed) →
/// totalLag. Отсутствие коммита по партиции = весь end как лаг (группа ни
/// разу не закоммитилась); отрицательный лаг (коммит после end — сегмент
/// удалён retention'ом) — 0.
/// </summary>
public static class KafkaGroupLag
{
    public static long Total(
        IReadOnlyDictionary<(string Topic, int Partition), long> endOffsets,
        IReadOnlyDictionary<(string Topic, int Partition), long> committed)
    {
        long total = 0;
        foreach (var (partition, end) in endOffsets)
        {
            var offset = committed.GetValueOrDefault(partition, -1);
            total += offset < 0 ? end : Math.Max(0, end - offset);
        }

        return total;
    }
}
