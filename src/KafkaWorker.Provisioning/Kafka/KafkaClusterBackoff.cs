namespace KafkaWorker.Provisioning.Kafka;

/// <summary>
/// Backoff недоступного кластера (t05, spec §3.2; паттерн KafkaProbeLoop t11):
/// сколько kafka-проб подряд упало и когда разрешена следующая. Писатели —
/// supervise-проба и коллектор метрик (первые kafka-контакты конвейера);
/// успех сбрасывает. Чистая политика частоты: НЕ состояние кластера — в etcd
/// ничего не пишет, brokers/&lt;b&gt;/state не трогает (флап ≠ смерть, arch/17 S7).
/// </summary>
public sealed class KafkaClusterBackoff(TimeProvider clock)
{
    private readonly object _gate = new();
    private readonly Dictionary<string, State> _clusters = [];

    public bool IsBlocked(string cluster)
    {
        lock (_gate)
            return _clusters.TryGetValue(cluster, out var s) && clock.GetUtcNow() < s.NextAttemptUtc;
    }

    public void RecordFailure(string cluster, string error)
    {
        lock (_gate)
        {
            var failures = (_clusters.TryGetValue(cluster, out var s) ? s.ConsecutiveFailures : 0) + 1;
            _clusters[cluster] = new State(failures, clock.GetUtcNow() + BackoffAfter(failures), error);
        }
    }

    public void RecordSuccess(string cluster)
    {
        lock (_gate)
        {
            _clusters.Remove(cluster);
        }
    }

    // Кластеры исчезли из снапшота — запись удаляется ЦЕЛИКОМ: и окно, и
    // счётчик (возвращение кластера начинает с первой ступени; t11).
    public void ForgetMissing(IReadOnlySet<string> liveClusters)
    {
        lock (_gate)
        {
            foreach (var gone in _clusters.Keys.Where(c => !liveClusters.Contains(c)).ToList())
                _clusters.Remove(gone);
        }
    }

    // Окно после N-й подряд неудачи: 1-я → 15 с (база kafka-циклов), 2-я →
    // 60 с, дальше 300 с (t11: 15 → 60 → 300, сброс при успехе).
    internal static TimeSpan BackoffAfter(int consecutiveFailures)
        => consecutiveFailures switch
        {
            <= 1 => TimeSpan.FromSeconds(15),
            2 => TimeSpan.FromSeconds(60),
            _ => TimeSpan.FromSeconds(300),
        };

    private sealed record State(int ConsecutiveFailures, DateTimeOffset NextAttemptUtc, string LastError);
}
