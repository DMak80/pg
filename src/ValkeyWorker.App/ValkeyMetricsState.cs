using System.Collections.Frozen;
using System.Diagnostics.Metrics;

namespace ValkeyWorker.App;

// Срез доменных метрик ноды из INFO (arch/18 §2.6): null — поле INFO
// отсутствует/нечислово → серия НЕ эмитится (консервативно, без нулей-фантомов).
public sealed record ValkeyNodeSample(
    string Node,
    long? UsedMemoryBytes,
    long? MaxMemoryBytes,
    long? ConnectedClients,
    long? BlockedClients,
    long? EvictedKeys,
    long? ExpiredKeys,
    long? KeyspaceHits,
    long? KeyspaceMisses,
    long? InstantaneousOpsPerSec,
    long? TotalConnectionsReceived,
    long? RejectedConnections,
    long? TotalCommandsProcessed,
    string? Role,
    long? ConnectedSlaves);

// Стейт + ObservableGauge-серии valkey-коллектора (arch/18 §2.6/§4.2; зеркало
// KafkaMetricsState). Пассивный наблюдатель: чтение стейта под lock, серии пустые
// до первого тика; все доменные серии — gauge (persistence off сбрасывает
// кумулятивы ноды — counter ломал бы rate()/increase()).
public sealed class ValkeyMetricsState
{
    private readonly object _lock = new();
    private readonly Dictionary<(string Cluster, string Node), ValkeyNodeSample> _nodes = [];
    private DateTimeOffset? _lastSuccess;

    public ValkeyMetricsState(Meter meter)
    {
        Gauge(meter, "valkey.memory.used_bytes", s => s.UsedMemoryBytes,
            "Потребление памяти нодой, байты (INFO used_memory)");
        Gauge(meter, "valkey.memory.max_bytes", s => s.MaxMemoryBytes,
            "Предел памяти ноды maxmemory, байты (INFO maxmemory)");
        Gauge(meter, "valkey.connected_clients", s => s.ConnectedClients,
            "Подключённые клиенты (INFO connected_clients)");
        Gauge(meter, "valkey.blocked_clients", s => s.BlockedClients,
            "Заблокированные клиенты BLPOP и т.п. (INFO blocked_clients)");
        Gauge(meter, "valkey.evicted_keys", s => s.EvictedKeys,
            "Кумулятив выселенных ключей (gauge: сброс при рестарте ноды)");
        Gauge(meter, "valkey.expired_keys", s => s.ExpiredKeys,
            "Кумулятив истёкших ключей (gauge: сброс при рестарте ноды)");
        Gauge(meter, "valkey.keyspace_hits", s => s.KeyspaceHits,
            "Кумулятив попаданий (hit-rate = hits/(hits+misses))");
        Gauge(meter, "valkey.keyspace_misses", s => s.KeyspaceMisses,
            "Кумулятив промахов ключей");
        Gauge(meter, "valkey.instantaneous_ops_per_sec", s => s.InstantaneousOpsPerSec,
            "Операции/сек — готовая скорость INFO (без rate())");
        Gauge(meter, "valkey.total_connections_received", s => s.TotalConnectionsReceived,
            "Кумулятив принятых соединений (gauge: сброс при рестарте ноды)");
        Gauge(meter, "valkey.rejected_connections", s => s.RejectedConnections,
            "Кумулятив отклонённых соединений (maxclients)");
        Gauge(meter, "valkey.total_commands_processed", s => s.TotalCommandsProcessed,
            "Кумулятив обработанных команд (gauge: сброс при рестарте ноды)");
        Gauge(meter, "valkey.connected_slaves", s => s.ConnectedSlaves,
            "Число реплик (v1 standalone — всегда 0; заготовка под топологии)");

        // role: enum-паттерн — 1 в серии с лейблом фактической роли (master|slave);
        // прочие значения INFO серию не эмитят (консервативно).
        meter.CreateObservableGauge(
            "valkey.role",
            () => Measure(_nodes
                .Where(kv => kv.Value.Role is "master" or "slave")
                .Select(kv => new Measurement<long>(1,
                    new KeyValuePair<string, object?>("cluster", kv.Key.Cluster),
                    new KeyValuePair<string, object?>("node", kv.Key.Node),
                    new KeyValuePair<string, object?>("role", kv.Value.Role)))),
            description: "Роль ноды: 1 в серии с role=\"master|slave\" (INFO Replication.role)");

        meter.CreateObservableGauge(
            "valkey.collector.last_success_timestamp_seconds",
            () => Measure(new[] { ReadLastSuccess() }.OfType<Measurement<long>>()),
            unit: "s", description: "Unix-время последнего успешного тика коллектора");
    }

    // Обновление стейта кластера тиком: предыдущие записи кластера затираются —
    // ушедшие ноды не копятся; LastSuccess — только через MarkSuccess.
    public void UpdateCluster(string cluster, IReadOnlyCollection<ValkeyNodeSample> samples)
    {
        lock (_lock)
        {
            foreach (var key in _nodes.Keys.Where(k => k.Cluster == cluster).ToList())
                _nodes.Remove(key);
            foreach (var sample in samples)
                _nodes[(cluster, sample.Node)] = sample;
        }
    }

    // LastSuccess обновляется ТОЛЬКО при полном успехе всех проб тика (консервативно;
    // пустой домен = успех — алерт ValkeyCollectorStalled §5.2-правил).
    public void MarkSuccess(DateTimeOffset at)
    {
        lock (_lock)
            _lastSuccess = at;
    }

    // Чтение ТОЛЬКО под lock (зеркало KafkaMetricsState): конкурентный
    // UpdateCluster мутирует _lastSuccess.
    private Measurement<long>? ReadLastSuccess()
    {
        lock (_lock)
            return _lastSuccess is { } at ? new Measurement<long>(at.ToUnixTimeSeconds()) : null;
    }

    // Материализация (.ToArray) ОБЯЗАТЕЛЬНА под lock: OTel перечисляет результат
    // вне колбэка — ленивый Select по словарю даст InvalidOperationException при
    // конкурентном UpdateCluster (урок Ф7-4).
    private IEnumerable<Measurement<T>> Measure<T>(IEnumerable<Measurement<T>> read) where T : struct
    {
        lock (_lock)
            return read.ToArray();
    }

    // Регистрация числовой серии: поле сэмпла null → точка НЕ эмитится.
    private void Gauge(Meter meter, string name, Func<ValkeyNodeSample, long?> field, string description)
        => meter.CreateObservableGauge(
            name,
            () => Measure(_nodes
                .Select(kv => (kv.Key, Value: field(kv.Value)))
                .Where(t => t.Value is not null)
                .Select(t => new Measurement<long>(t.Value!.Value,
                    new KeyValuePair<string, object?>("cluster", t.Key.Cluster),
                    new KeyValuePair<string, object?>("node", t.Key.Node)))),
            description: description);

    /// <summary>Internal-снимок стейта для юнит-проверок (InternalsVisibleTo).</summary>
    internal DebugSnapshotRecord DebugSnapshot()
    {
        lock (_lock)
            return new DebugSnapshotRecord(_nodes.ToFrozenDictionary(), _lastSuccess);
    }

    internal sealed record DebugSnapshotRecord(
        IReadOnlyDictionary<(string Cluster, string Node), ValkeyNodeSample> Nodes,
        DateTimeOffset? LastSuccess);
}
