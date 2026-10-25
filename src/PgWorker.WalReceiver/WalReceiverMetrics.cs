using System.Diagnostics.Metrics;

namespace PgWorker.WalReceiver;

/// <summary>Инструменты телеметрии приёмника WAL (arch/18 §2.8; паттерн
/// Shared.Metrics.Worker.WorkerMetricsInstrumentation): инструменты создаются
/// из Meter `PgWorker.WalReceiver`, марк-методы пассивные — не бросают и не
/// влияют на цикл/стрим. Лейблы cluster/shard/node константны per-процесс
/// (приёмник = одна нода; +role только у role-серии). Серии:
/// gauges `walreceiver_role` (активная роль=1), `walreceiver_streaming`,
/// `walreceiver_backlog_bytes` (позиция потока − подтверждённый хвост),
/// `walreceiver_last_success_timestamp_seconds`; counters
/// `walreceiver_segments_delivered_total`, `walreceiver_bytes_delivered_total`,
/// `walreceiver_put_retries_total`, `walreceiver_put_errors_total`,
/// `walreceiver_reconnects_total`.</summary>
public sealed class WalReceiverMetrics : IDisposable
{
    /// <summary>Имя Meter приёмника (константа — регистрация DI/OTel).</summary>
    public const string MeterName = "PgWorker.WalReceiver";

    private readonly KeyValuePair<string, object?>[] _labels;
    private readonly TimeProvider _clock;
    private readonly object _lock = new();

    // Стейт гейджей (чтение — колбэками ObservableGauge под тем же lock).
    private string? _activeRole;      // "master"|"sync"|"none" — последняя проба
    private bool _streaming;
    private long? _backlogBytes;      // позиция потока − подтверждённый хвост; null — серия исчезает
    private long? _lastSuccessUnix;   // unix последнего успешного put

    private bool _disposed;

    public WalReceiverMetrics(
        Meter meter, string cluster, string shard, string node, TimeProvider? clock = null)
    {
        _clock = clock ?? TimeProvider.System;
        _labels =
        [
            new KeyValuePair<string, object?>("cluster", cluster),
            new KeyValuePair<string, object?>("shard", shard),
            new KeyValuePair<string, object?>("node", node),
        ];

        // Counter-инструменты: пишутся марк-методами напрямую.
        var segments = meter.CreateCounter<long>(
            "walreceiver_segments_delivered_total",
            unit: "{segment}",
            description: "Closed WAL segments delivered to S3 (arch/18 §2.8)");
        var bytes = meter.CreateCounter<long>(
            "walreceiver_bytes_delivered_total",
            unit: "By",
            description: "WAL segment/history bytes delivered to S3 (arch/18 §2.8)");
        var putRetries = meter.CreateCounter<long>(
            "walreceiver_put_retries_total",
            description: "Retry put attempts across pool write targets (arch/18 §2.8)");
        var putErrors = meter.CreateCounter<long>(
            "walreceiver_put_errors_total",
            description: "Failed put attempts incl. write-list exhaustion (arch/18 §2.8)");
        var reconnects = meter.CreateCounter<long>(
            "walreceiver_reconnects_total",
            description: "Reconnects to the replication source (drops/role changes, arch/18 §2.8)");

        // Gauge-серии: по одному ObservableGauge; колбэки читают стейт под lock.
        meter.CreateObservableGauge(
            "walreceiver_role",
            () => Measure(() =>
            {
                var active = _activeRole;
                return new[]
                {
                    new Measurement<long>(active == "master" ? 1 : 0, WithLabel("role", "master")),
                    new Measurement<long>(active == "sync" ? 1 : 0, WithLabel("role", "sync")),
                    new Measurement<long>(active == "none" ? 1 : 0, WithLabel("role", "none")),
                };
            }),
            description: "Source role by REST probe (1 in the active series, arch/18 §2.8)");

        meter.CreateObservableGauge(
            "walreceiver_streaming",
            () => Measure(() => new[] { new Measurement<long>(_streaming ? 1 : 0, _labels) }),
            description: "1 = replication stream active, 0 = waiting (arch/18 §2.8)");

        meter.CreateObservableGauge(
            "walreceiver_backlog_bytes",
            () => Measure(() => _backlogBytes is { } value
                ? new[] { new Measurement<long>(value, _labels) }
                : Array.Empty<Measurement<long>>()),
            unit: "By",
            description: "WAL tail pending delivery: stream position − confirmed tail (arch/18 §2.8)");

        meter.CreateObservableGauge(
            "walreceiver_last_success_timestamp_seconds",
            () => Measure(() => _lastSuccessUnix is { } unix
                ? new[] { new Measurement<long>(unix, _labels) }
                : Array.Empty<Measurement<long>>()),
            unit: "s",
            description: "Unix time of the last successful put (arch/18 §2.8)");

        SegmentDeliveredMark = count =>
        {
            try
            {
                segments.Add(1, _labels);
                bytes.Add(count, _labels);
            }
            catch
            {
                // Пассивный наблюдатель: ошибка инструментария не влияет на стрим.
            }
        };
        BytesDeliveredMark = count =>
        {
            try
            {
                bytes.Add(count, _labels);
            }
            catch
            {
                // Пассивный наблюдатель.
            }
        };
        PutRetryMark = () =>
        {
            try
            {
                putRetries.Add(1, _labels);
            }
            catch
            {
                // Пассивный наблюдатель.
            }
        };
        PutErrorMark = () =>
        {
            try
            {
                putErrors.Add(1, _labels);
            }
            catch
            {
                // Пассивный наблюдатель.
            }
        };
        ReconnectMark = () =>
        {
            try
            {
                reconnects.Add(1, _labels);
            }
            catch
            {
                // Пассивный наблюдатель.
            }
        };
    }

    private readonly Action<long> SegmentDeliveredMark;
    private readonly Action<long> BytesDeliveredMark;
    private readonly Action PutRetryMark;
    private readonly Action PutErrorMark;
    private readonly Action ReconnectMark;

    private KeyValuePair<string, object?>[] WithLabel(string key, string value)
    {
        var all = new KeyValuePair<string, object?>[_labels.Length + 1];
        _labels.CopyTo(all, 0);
        all[^1] = new KeyValuePair<string, object?>(key, value);
        return all;
    }

    // Последняя пробованная роль: "master"|"sync"|"none"; null — серия нулевая.
    public void SetRole(string? role)
    {
        try
        {
            lock (_lock)
                _activeRole = role;
        }
        catch
        {
            // Пассивный наблюдатель.
        }
    }

    // Стрим активен/ожидает.
    public void SetStreaming(bool streaming)
    {
        try
        {
            lock (_lock)
                _streaming = streaming;
        }
        catch
        {
            // Пассивный наблюдатель.
        }
    }

    // Backlog: позиция потока − подтверждённый хвост (байты); null — серия исчезает.
    public void SetBacklog(long? bytes)
    {
        try
        {
            lock (_lock)
                _backlogBytes = bytes;
        }
        catch
        {
            // Пассивный наблюдатель.
        }
    }

    // Доставлен закрытый сегмент (counter сегментов + байт).
    public void SegmentDelivered(long bytes) => SegmentDeliveredMark(bytes);

    // Доставлена `.history` (counter байт; сегменты не трогает — arch/18 §2.8).
    public void HistoryDelivered(long bytes) => BytesDeliveredMark(bytes);

    // Повторная попытка put (перебор целей).
    public void PutRetry() => PutRetryMark();

    // Неудачная попытка put (вкл. исчерпание списка).
    public void PutError() => PutErrorMark();

    // Успешный put: двигает last_success_timestamp_seconds.
    public void PutSuccess()
    {
        try
        {
            lock (_lock)
                _lastSuccessUnix = _clock.GetUtcNow().ToUnixTimeSeconds();
        }
        catch
        {
            // Пассивный наблюдатель.
        }
    }

    // Переподключение к источнику (обрыв/смена роли).
    public void Reconnect() => ReconnectMark();

    private IEnumerable<Measurement<T>> Measure<T>(Func<IEnumerable<Measurement<T>>> read) where T : struct
    {
        lock (_lock)
        {
            if (_disposed)
                return [];
            return read().ToArray();
        }
    }

    public void Dispose()
    {
        lock (_lock)
            _disposed = true;
    }
}
