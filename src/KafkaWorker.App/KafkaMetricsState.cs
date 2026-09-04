using System.Collections.Frozen;
using System.Diagnostics.Metrics;

namespace KafkaWorker.App;

// Стейт + ObservableGauge-серии коллектора (arch/18 §2.3–§4). Пассивный
// наблюдатель: чтение стейта под lock, серии пустые до первого тика.
public sealed class KafkaMetricsState
{
    private readonly object _lock = new();
    private readonly Dictionary<(string Cluster, string Group, string Topic), long> _lag = [];
    private readonly Dictionary<(string Cluster, string Topic), int> _usr = [];
    private DateTimeOffset? _lastSuccess;

    public KafkaMetricsState(Meter meter)
    {
        meter.CreateObservableGauge(
            "kafka.consumer.lag",
            () => Measure(_lag.Select(kv => new Measurement<long>(kv.Value,
                new KeyValuePair<string, object?>("cluster", kv.Key.Cluster),
                new KeyValuePair<string, object?>("group", kv.Key.Group),
                new KeyValuePair<string, object?>("topic", kv.Key.Topic)))),
            description: "Суммарный consumer-lag по группе и топику (arch/18 §2.3)");

        meter.CreateObservableGauge(
            "kafka.under_replicated_partitions",
            () => Measure(_usr.Select(kv => new Measurement<int>(kv.Value,
                new KeyValuePair<string, object?>("cluster", kv.Key.Cluster),
                new KeyValuePair<string, object?>("topic", kv.Key.Topic)))),
            description: "Число недореплицированных партиций топика");

        meter.CreateObservableGauge(
            "kafka.collector.last_success_timestamp_seconds",
            () => Measure(new[] { ReadLastSuccess() }.OfType<Measurement<long>>()),
            unit: "s", description: "Unix-время последнего успешного сбора коллектора");
    }

    // Обновление стейта тика: лаги/USR кластера (предыдущие записи кластера
    // затираются — ушедшие группы/топики не копятся) + LastSuccess при полном успехе.
    public void UpdateCluster(
        string cluster,
        IReadOnlyCollection<((string Cluster, string Group, string Topic) Key, long Lag)> lag,
        IReadOnlyCollection<((string Cluster, string Topic) Key, int Usr)> usr)
    {
        lock (_lock)
        {
            foreach (var key in _lag.Keys.Where(k => k.Cluster == cluster).ToList())
                _lag.Remove(key);
            foreach (var key in _usr.Keys.Where(k => k.Cluster == cluster).ToList())
                _usr.Remove(key);
            foreach (var (key, value) in lag)
                _lag[key] = value;
            foreach (var (key, value) in usr)
                _usr[key] = value;
        }
    }

    // LastSuccess обновляется ТОЛЬКО при полном успехе всех кластеров (консервативно).
    public void MarkSuccess(DateTimeOffset at)
    {
        lock (_lock)
            _lastSuccess = at;
    }

    // Чтение стейта ТОЛЬКО под lock: конкурентный UpdateCluster (тик коллектора)
    // мутирует _lastSuccess — чтение вне lock даёт порванное значение.
    private Measurement<long>? ReadLastSuccess()
    {
        lock (_lock)
            return _lastSuccess is { } at ? new Measurement<long>(at.ToUnixTimeSeconds()) : null;
    }

    // Материализация (.ToArray) ОБЯЗАТЕЛЬНА под lock: OTel перечисляет результат
    // вне колбэка, а ленивый Select-read по словарям даст InvalidOperationException
    // при конкурентном UpdateCluster — серия пропадёт со scrape (ревью Ф7-4).
    private IEnumerable<Measurement<T>> Measure<T>(IEnumerable<Measurement<T>> read) where T : struct
    {
        lock (_lock)
            return read.ToArray();
    }

    /// <summary>Internal-снимок стейта для юнит-проверок (InternalsVisibleTo).</summary>
    internal DebugSnapshotRecord DebugSnapshot()
    {
        lock (_lock)
        {
            return new DebugSnapshotRecord(
                _lag.ToFrozenDictionary(),
                _usr.ToFrozenDictionary(),
                _lastSuccess);
        }
    }

    internal sealed record DebugSnapshotRecord(
        IReadOnlyDictionary<(string Cluster, string Group, string Topic), long> Lag,
        IReadOnlyDictionary<(string Cluster, string Topic), int> Usr,
        DateTimeOffset? LastSuccess);
}
