using System.Collections.Frozen;
using System.Diagnostics.Metrics;

namespace Shared.Metrics.Worker;

// Типизированные инструменты воркер-паттерна (arch/18 §2.2). Метрики — пассивные
// наблюдатели: марк-методы никогда не бросают исключений, не влияют на циклы.
// Единственный источник серий §2.2 — эти марк-методы (вызовы циклов рядом с
// health.Mark* + подписка на фазовые записи журнала); HealthState — источник
// только /healthz (arch/18 §1).
public sealed class WorkerMetricsInstrumentation : IDisposable
{
    // Терминальные фазы — фактический словарь журналов обоих воркеров (arch/18 §2.2).
    public static readonly IReadOnlySet<string> FinalPhases =
        new HashSet<string> { "done", "failed", "crashed", "rejected", "cancelled" };

    // Ops без терминальной фазы: supervise (стационарные записи, часть через
    // WriteSupervisionAsync мимо события) и evacuate (только waiting-*/blocked-moving).
    public static readonly IReadOnlySet<string> SuppressedOps =
        new HashSet<string> { "supervise", "evacuate" };

    private readonly TimeProvider _clock;
    private readonly object _lock = new();

    // Стейт гейджей/счётчиков (чтение — колбэками ObservableGauge под тем же lock).
    private readonly Dictionary<string, long> _lastSuccess = new();
    private readonly Dictionary<string, double> _lastDuration = new();
    private readonly Dictionary<(string Loop, bool Ok), long> _loopTicks = new();
    private readonly Dictionary<(string Cluster, string Process), (string Phase, DateTimeOffset StartedAt)> _phases = new();
    private readonly Dictionary<(string Operation, string Result), long> _operations = new();
    private DateTimeOffset? _lastSnapshotTaken;
    private readonly Dictionary<(string Cluster, string Shard), long> _walLag = new();
    private readonly Dictionary<(string Cluster, string Shard), long> _fullAgeFinishedUnix = new();
    private readonly Dictionary<(string Cluster, string Shard), long> _fullMaxAge = new();
    private readonly Dictionary<(string Cluster, string Shard), long> _haFailoverSec = new();
    private readonly Dictionary<(string Cluster, string Shard), long> _haRebuildSec = new();
    private readonly Dictionary<(string Cluster, string Shard), long> _walUploadedUnix = new();
    private readonly Dictionary<(string Cluster, string Shard, string Result), long> _backupVerify = new();
    private readonly Dictionary<string, long> _watchdogRestarts = new();
    private readonly Dictionary<(string Cluster, string Shard, string Result), long> _backupRestore = new();
    private readonly Dictionary<(string Cluster, string Shard, string Result), long> _backupDrill = new();
    private int _claimsHeld;

    private bool _disposed;

    public WorkerMetricsInstrumentation(Meter meter, TimeProvider clock)
    {
        _clock = clock;

        // Counter-инструменты: пишутся марк-методами напрямую.
        var loopTicks = meter.CreateCounter<long>(
            "worker.loop.ticks", description: "Тики циклов воркера (arch/18 §2.2)");
        var operations = meter.CreateCounter<long>(
            "worker.operation.total", description: "Завершённые операции (result: ok/error)");
        var backupVerify = meter.CreateCounter<long>(
            "pgworker_backup_verify_total", description: "Результаты verify полных бэкапов (arch/19 §5, t04)");
        var backupRestore = meter.CreateCounter<long>(
            "pgworker_backup_restore_total",
            description: "Терминальные исходы restore-заявок (arch/18 §2.7)");
        var backupDrill = meter.CreateCounter<long>(
            "pgworker_backup_drill_total",
            description: "Терминальные исходы дрилов восстановимости (arch/18 §2.7)");
        var watchdogRestarts = meter.CreateCounter<long>(
            "worker_watchdog_restarts_total",
            description: "Инициированные watchdog-остановки по staleness (arch/18 §2.2)");

        // Gauge-серии: по одному ObservableGauge на серию; колбэки читают стейт;
        // длительность фазы вычисляется в колбэке как clock.GetUtcNow() - startedAt.
        meter.CreateObservableGauge(
            "worker.loop.last_success_timestamp_seconds",
            () => Measure(() => _lastSuccess.Select(kv =>
                new Measurement<long>(kv.Value, new KeyValuePair<string, object?>("loop", kv.Key)))),
            unit: "s", description: "Unix-время последнего успешного тика цикла");

        meter.CreateObservableGauge(
            "worker.loop.duration_seconds",
            () => Measure(() => _lastDuration.Select(kv =>
                new Measurement<double>(kv.Value, new KeyValuePair<string, object?>("loop", kv.Key)))),
            unit: "s", description: "Длительность последнего тика цикла");

        meter.CreateObservableGauge(
            "worker.claims_held",
            () => Measure(() => new[] { new Measurement<int>(_claimsHeld) }),
            description: "Число кластеров под клэймом");

        meter.CreateObservableGauge(
            "pgworker_backup_wal_lag_segments",
            () => Measure(() => _walLag.Select(kv =>
                new Measurement<long>(kv.Value,
                    new KeyValuePair<string, object?>("cluster", kv.Key.Cluster),
                    new KeyValuePair<string, object?>("shard", kv.Key.Shard)))),
            unit: "{segment}", description: "Отставание WAL-потока бэкапов, сегментов (arch/19 §3)");

        // t14 (arch/18 §2.7): возрастные серии хранят unix-факт, колбэк пересчитывает
        // now − unix на КАЖДОМ scrape — между тиками процессов значение не замирает
        // (паттерн worker_snapshot_age_seconds).
        meter.CreateObservableGauge(
            "pgworker_backup_full_age_seconds",
            () => Measure(() => _fullAgeFinishedUnix.Select(kv =>
                new Measurement<double>(
                    Math.Max(0, _clock.GetUtcNow().ToUnixTimeSeconds() - kv.Value),
                    new KeyValuePair<string, object?>("cluster", kv.Key.Cluster),
                    new KeyValuePair<string, object?>("shard", kv.Key.Shard)))),
            unit: "s", description: "Возраст последнего валидного полного бэкапа, с (arch/18 §2.7)");

        meter.CreateObservableGauge(
            "pgworker_backup_full_max_age_seconds",
            () => Measure(() => _fullMaxAge.Select(kv =>
                new Measurement<long>(kv.Value,
                    new KeyValuePair<string, object?>("cluster", kv.Key.Cluster),
                    new KeyValuePair<string, object?>("shard", kv.Key.Shard)))),
            unit: "s", description: "Порог full_max_age_sec эффективной политики кластера (arch/18 §2.7)");

        meter.CreateObservableGauge(
            "pgworker_backup_wal_last_uploaded_age_seconds",
            () => Measure(() => _walUploadedUnix.Select(kv =>
                new Measurement<double>(
                    Math.Max(0, _clock.GetUtcNow().ToUnixTimeSeconds() - kv.Value),
                    new KeyValuePair<string, object?>("cluster", kv.Key.Cluster),
                    new KeyValuePair<string, object?>("shard", kv.Key.Shard)))),
            unit: "s", description: "Возраст последней загрузки WAL-сегмента в S3, с (arch/18 §2.7)");

        // t17 (arch/18 §2.7): RTO-длительности — только ЗАВЕРШЁННЫЕ факты надзора
        // (resolved − detected, arch/14 §3.3); открытого события/факта нет — серия
        // не эмитится. Источник — тик NodeSupervisor (наблюдатель haDurations).
        meter.CreateObservableGauge(
            "pgworker_ha_failover_duration_seconds",
            () => Measure(() => _haFailoverSec.Select(kv =>
                new Measurement<long>(kv.Value,
                    new KeyValuePair<string, object?>("cluster", kv.Key.Cluster),
                    new KeyValuePair<string, object?>("shard", kv.Key.Shard)))),
            unit: "s", description: "Длительность последнего завершённого failover, с (arch/14 §3.3)");

        meter.CreateObservableGauge(
            "pgworker_ha_rebuild_duration_seconds",
            () => Measure(() => _haRebuildSec.Select(kv =>
                new Measurement<long>(kv.Value,
                    new KeyValuePair<string, object?>("cluster", kv.Key.Cluster),
                    new KeyValuePair<string, object?>("shard", kv.Key.Shard)))),
            unit: "s", description: "Длительность последнего завершённого rebuild, с (arch/14 §3.3)");

        meter.CreateObservableGauge(
            "worker.process.phase.duration_seconds",
            () => Measure(() =>
            {
                var now = _clock.GetUtcNow();
                return _phases.Select(kv => new Measurement<double>(
                    Math.Max(0, (now - kv.Value.StartedAt).TotalSeconds),
                    new KeyValuePair<string, object?>("cluster", kv.Key.Cluster),
                    new KeyValuePair<string, object?>("process", kv.Key.Process),
                    new KeyValuePair<string, object?>("phase", kv.Value.Phase)));
            }),
            unit: "s", description: "Возраст текущей фазы процесса на кластере");

        meter.CreateObservableGauge(
            "worker.snapshot.age_seconds",
            () => Measure(() => _lastSnapshotTaken is { } taken
                ? new[] { new Measurement<double>(Math.Max(0, (_clock.GetUtcNow() - taken).TotalSeconds)) }
                : []),
            unit: "s", description: "Возраст последнего снапшота P12");

        LoopTickMark = (loop, ok) =>
        {
            try
            {
                loopTicks.Add(1,
                    new KeyValuePair<string, object?>("loop", loop),
                    new KeyValuePair<string, object?>("ok", ok));
            }
            catch
            {
                // Пассивный наблюдатель: ошибка инструментария не влияет на цикл.
            }
        };
        OperationMark = (operation, result) =>
        {
            try
            {
                operations.Add(1,
                    new KeyValuePair<string, object?>("operation", operation),
                    new KeyValuePair<string, object?>("result", result));
            }
            catch
            {
                // Пассивный наблюдатель.
            }
        };
        BackupVerifyMark = (cluster, shard, result) =>
        {
            try
            {
                backupVerify.Add(1,
                    new KeyValuePair<string, object?>("cluster", cluster),
                    new KeyValuePair<string, object?>("shard", shard),
                    new KeyValuePair<string, object?>("result", result));
            }
            catch
            {
                // Пассивный наблюдатель.
            }
        };
        BackupRestoreMark = (cluster, shard, result) =>
        {
            try
            {
                backupRestore.Add(1,
                    new KeyValuePair<string, object?>("cluster", cluster),
                    new KeyValuePair<string, object?>("shard", shard),
                    new KeyValuePair<string, object?>("result", result));
            }
            catch
            {
                // Пассивный наблюдатель.
            }
        };
        BackupDrillMark = (cluster, shard, result) =>
        {
            try
            {
                backupDrill.Add(1,
                    new KeyValuePair<string, object?>("cluster", cluster),
                    new KeyValuePair<string, object?>("shard", shard),
                    new KeyValuePair<string, object?>("result", result));
            }
            catch
            {
                // Пассивный наблюдатель.
            }
        };
        WatchdogRestartMark = loop =>
        {
            try
            {
                watchdogRestarts.Add(1, new KeyValuePair<string, object?>("loop", loop));
            }
            catch
            {
                // Пассивный наблюдатель.
            }
        };
    }

    private readonly Action<string, bool> LoopTickMark;
    private readonly Action<string, string> OperationMark;
    private readonly Action<string, string, string> BackupVerifyMark;
    private readonly Action<string, string, string> BackupRestoreMark;
    private readonly Action<string, string, string> BackupDrillMark;
    private readonly Action<string> WatchdogRestartMark;

    // Колбэк ObservableGauge: чтение стейта под lock; после Dispose — серии пустые.
    // Материализация (.ToArray) ОБЯЗАТЕЛЬНА под lock: OTel перечисляет результат
    // вне колбэка, а ленивый Select-read по словарям даст InvalidOperationException
    // при конкурентной мутации (ProcessFinished/смена фазы) — серия пропадёт со scrape.
    private IEnumerable<Measurement<T>> Measure<T>(Func<IEnumerable<Measurement<T>>> read) where T : struct
    {
        lock (_lock)
        {
            if (_disposed)
                return [];
            return read().ToArray();
        }
    }

    // Тик цикла: counter worker.loop.ticks{loop, ok}; ok=true дополнительно двигает
    // worker.loop.last_success_timestamp_seconds{loop}.
    public void LoopTick(string loop, bool ok)
    {
        try
        {
            LoopTickMark(loop, ok);
            lock (_lock)
            {
                var key = (loop, ok);
                _loopTicks[key] = _loopTicks.TryGetValue(key, out var n) ? n + 1 : 1;
                if (ok)
                    _lastSuccess[loop] = _clock.GetUtcNow().ToUnixTimeSeconds();
            }
        }
        catch
        {
            // Пассивный наблюдатель: ошибка инструментария не влияет на цикл.
        }
    }

    // Длительность последнего тика: gauge worker.loop.duration_seconds{loop}.
    public void LoopDuration(string loop, double seconds)
    {
        try
        {
            lock (_lock)
                _lastDuration[loop] = seconds;
        }
        catch
        {
            // Пассивный наблюдатель.
        }
    }

    // Gauge pgworker_backup_wal_lag_segments{cluster,shard}: лаг WAL-потока шарда
    // (наблюдение контрольного прохода WalStreamProcess; null — серия исчезает).
    public void BackupWalLag(string cluster, string shard, long? segments)
    {
        try
        {
            lock (_lock)
            {
                if (segments is { } value)
                    _walLag[(cluster, shard)] = value;
                else
                    _walLag.Remove((cluster, shard));
            }
        }
        catch
        {
            // Пассивный наблюдатель.
        }
    }

    // Gauge pgworker_backup_full_age_seconds/full_max_age_seconds (t14, arch/18
    // §2.7): ЕДИНЫЙ марк-метод на тик кластера — набор шардов тика замещает
    // стейт кластера ЦЕЛИКОМ (паттерн UpdateCluster §4.2: шард, не пришедший в
    // наборе, серии больше не эмитит). Элемент набора: LastValidUnix — источник
    // возраста (null → age-серия шарда отсутствует), MaxAgeSec — порог (пишется
    // всегда; per-cluster порог алерта age > max_age без хардкода в правиле).
    public void BackupFullAge(
        string cluster, IReadOnlyDictionary<string, (long? LastValidUnix, long MaxAgeSec)> shards)
    {
        try
        {
            lock (_lock)
            {
                foreach (var gone in _fullMaxAge.Keys
                             .Where(k => k.Cluster == cluster && !shards.ContainsKey(k.Shard))
                             .ToList())
                {
                    _fullAgeFinishedUnix.Remove(gone);
                    _fullMaxAge.Remove(gone);
                }

                foreach (var (shard, value) in shards)
                {
                    _fullMaxAge[(cluster, shard)] = value.MaxAgeSec;
                    if (value.LastValidUnix is { } unix)
                        _fullAgeFinishedUnix[(cluster, shard)] = unix;
                    else
                        _fullAgeFinishedUnix.Remove((cluster, shard));
                }
            }
        }
        catch
        {
            // Пассивный наблюдатель.
        }
    }

    // Gauge pgworker_ha_{failover,rebuild}_duration_seconds (arch/18 §2.7):
    // ЕДИНЫЙ марк-метод на тик надзора — набор шардов тика замещает стейт
    // кластера ЦЕЛИКОМ (ушедшие шарды серии не копят; null-факт — серия шарда
    // исчезает до появления завершённого факта).
    public void HaDurations(string cluster, IReadOnlyDictionary<string, (long? FailoverSec, long? RebuildSec)> shards)
    {
        try
        {
            lock (_lock)
            {
                foreach (var gone in _haFailoverSec.Keys.Concat(_haRebuildSec.Keys)
                             .Where(k => k.Cluster == cluster && !shards.ContainsKey(k.Shard))
                             .Distinct().ToList())
                {
                    _haFailoverSec.Remove(gone);
                    _haRebuildSec.Remove(gone);
                }

                foreach (var (shard, value) in shards)
                {
                    if (value.FailoverSec is { } failover)
                        _haFailoverSec[(cluster, shard)] = failover;
                    else
                        _haFailoverSec.Remove((cluster, shard));
                    if (value.RebuildSec is { } rebuild)
                        _haRebuildSec[(cluster, shard)] = rebuild;
                    else
                        _haRebuildSec.Remove((cluster, shard));
                }
            }
        }
        catch
        {
            // Пассивный наблюдатель: ошибка инструментария не влияет на цикл.
        }
    }

    // Gauge pgworker_backup_wal_last_uploaded_age_seconds (t14, arch/18 §2.7):
    // возраст последней доставки закрытого сегмента — наблюдение контрольного
    // прохода WalStreamProcess; null — серия исчезает (симметрия BackupWalLag).
    // Хранится unix-факт (now − age наблюдения), колбэк гейджа пересчитывает
    // возраст на каждом scrape.
    public void BackupWalUploadedAge(string cluster, string shard, long? ageSec)
    {
        try
        {
            lock (_lock)
            {
                if (ageSec is { } age)
                    _walUploadedUnix[(cluster, shard)] = _clock.GetUtcNow().ToUnixTimeSeconds() - age;
                else
                    _walUploadedUnix.Remove((cluster, shard));
            }
        }
        catch
        {
            // Пассивный наблюдатель.
        }
    }

    /// <summary>Counter pgworker_backup_verify_total{cluster,shard,result}:
    /// итог проверки полного (BackupVerifyProcess), result ∈ ok|failed|transient.</summary>
    public void BackupVerify(string cluster, string shard, string result)
    {
        try
        {
            BackupVerifyMark(cluster, shard, result);
            lock (_lock)
            {
                var key = (cluster, shard, result);
                _backupVerify[key] = _backupVerify.TryGetValue(key, out var n) ? n + 1 : 1;
            }
        }
        catch
        {
            // Пассивный наблюдатель.
        }
    }

    /// <summary>Counter pgworker_backup_restore_total{cluster,shard,result}: терминальный
    /// исход restore-заявки (RestoreProcess), result ∈ ok|failed (arch/18 §2.7).</summary>
    public void BackupRestore(string cluster, string shard, string result)
    {
        try
        {
            BackupRestoreMark(cluster, shard, result);
            lock (_lock)
            {
                var key = (cluster, shard, result);
                _backupRestore[key] = _backupRestore.TryGetValue(key, out var n) ? n + 1 : 1;
            }
        }
        catch
        {
            // Пассивный наблюдатель.
        }
    }

    /// <summary>Counter pgworker_backup_drill_total{cluster,shard,result}: чистый терминальный
    /// итог дрилла (RestoreDrillProcess), result ∈ ok|failed (arch/18 §2.7).</summary>
    public void BackupDrill(string cluster, string shard, string result)
    {
        try
        {
            BackupDrillMark(cluster, shard, result);
            lock (_lock)
            {
                var key = (cluster, shard, result);
                _backupDrill[key] = _backupDrill.TryGetValue(key, out var n) ? n + 1 : 1;
            }
        }
        catch
        {
            // Пассивный наблюдатель.
        }
    }

    /// <summary>Counter worker_watchdog_restarts_total{loop}: watchdog инициировал
    /// self-restart по отсутствию активности (тик или прогресс-отметка)
    /// (колбэк LoopWatchdog из Program.cs app).</summary>
    public void WatchdogRestart(string loop)
    {
        try
        {
            WatchdogRestartMark(loop);
            lock (_lock)
            {
                _watchdogRestarts[loop] = _watchdogRestarts.TryGetValue(loop, out var n) ? n + 1 : 1;
            }
        }
        catch
        {
            // Пассивный наблюдатель: ошибка инструментария не влияет на остановку.
        }
    }

    // Число удерживаемых клэймов: gauge worker_claims_held.
    public void ClaimsHeld(int count)
    {
        try
        {
            lock (_lock)
                _claimsHeld = count;
        }
        catch
        {
            // Пассивный наблюдатель.
        }
    }

    // Вход кластера в фазу процесса: gauge worker_process_phase_duration_seconds
    // {cluster, process, phase}; value = now - startedAt при observe; повторная
    // запись той же фазы НЕ сбрасывает startedAt (first-seen); ProcessFinished
    // сбрасывает серию (кардинальность, arch/18 §9 M1).
    public void ProcessPhase(string cluster, string process, string phase, DateTimeOffset startedAt)
    {
        try
        {
            lock (_lock)
            {
                if (_phases.TryGetValue((cluster, process), out var current) && current.Phase == phase)
                    return; // first-seen: журнал пишет ту же фазу каждый тик — не сбрасываем
                _phases[(cluster, process)] = (phase, startedAt);
            }
        }
        catch
        {
            // Пассивный наблюдатель.
        }
    }

    // Завершение процесса: сброс фазовой серии (кардинальность — только активные).
    public void ProcessFinished(string cluster, string process)
    {
        try
        {
            lock (_lock)
                _phases.Remove((cluster, process));
        }
        catch
        {
            // Пассивный наблюдатель.
        }
    }

    // Журнальное событие фазы (подписка WorkJournal.PhaseWritten/WriteAsync):
    //  - ops без терминальной фазы (SuppressedOps: supervise, evacuate) — ИГНОР:
    //    стационарные записи (часть — через WriteSupervisionAsync мимо события),
    //    живость надзора закрывает WorkerLoopStalled (решение ревью Ф4-2);
    //  - терминальные фазы (FinalPhases, фактический словарь журналов обоих
    //    воркеров): done, failed, crashed, rejected, cancelled → ProcessFinished +
    //    Operation(process, ok: phase == "done"); skipped — промежуточная
    //    (AdoptionProcess: skipped → далее обязательно done/failed) — НЕ терминальная;
    //  - прочие → ProcessPhase (startedAt контролируется first-seen внутри).
    public void OnJournalPhase(string cluster, string process, string phase)
    {
        try
        {
            if (SuppressedOps.Contains(process))
                return;
            if (FinalPhases.Contains(phase))
            {
                ProcessFinished(cluster, process);
                Operation(process, ok: phase == "done");
                return;
            }
            ProcessPhase(cluster, process, phase, _clock.GetUtcNow());
        }
        catch
        {
            // Пассивный наблюдатель.
        }
    }

    // Завершённая операция: counter worker_operation_total{operation, result}
    // → worker_operation_total; result ∈ {"ok","error"}.
    public void Operation(string operation, bool ok)
    {
        try
        {
            var result = ok ? "ok" : "error";
            OperationMark(operation, result);
            lock (_lock)
            {
                var key = (operation, result);
                _operations[key] = _operations.TryGetValue(key, out var n) ? n + 1 : 1;
            }
        }
        catch
        {
            // Пассивный наблюдатель.
        }
    }

    // Снапшот снят: источник worker_snapshot_age_seconds (value = now - at).
    public void SnapshotTaken(DateTimeOffset at)
    {
        try
        {
            lock (_lock)
                _lastSnapshotTaken = at;
        }
        catch
        {
            // Пассивный наблюдатель.
        }
    }

    // Dispose — только освобождение собственных подписок (колбэки гейджей
    // перестают отдавать серии); Meter принадлежит DI — не диспозим.
    public void Dispose()
    {
        lock (_lock)
            _disposed = true;
    }

    /// <summary>Internal-снимок стейта для юнит-проверок значений (надёжнее MeterListener).</summary>
    internal DebugState DebugSnapshot()
    {
        lock (_lock)
        {
            var age = _lastSnapshotTaken is { } taken
                ? Math.Max(0, (_clock.GetUtcNow() - taken).TotalSeconds)
                : (double?)null;
            return new DebugState(
                _lastSuccess.ToFrozenDictionary(),
                _loopTicks.ToFrozenDictionary(),
                _phases.ToFrozenDictionary(kv => kv.Key, kv => new DebugPhase(kv.Value.Phase, kv.Value.StartedAt)),
                _operations.ToFrozenDictionary(),
                _claimsHeld,
                _walLag.ToFrozenDictionary(),
                _backupVerify.ToFrozenDictionary(),
                _watchdogRestarts.ToFrozenDictionary(),
                _backupRestore.ToFrozenDictionary(),
                _backupDrill.ToFrozenDictionary(),
                _fullAgeFinishedUnix.ToFrozenDictionary(),
                _fullMaxAge.ToFrozenDictionary(),
                _walUploadedUnix.ToFrozenDictionary(),
                age,
                _haFailoverSec.ToFrozenDictionary(),
                _haRebuildSec.ToFrozenDictionary());
        }
    }

    /// <summary>Immutable-снимок стейта для тестов (InternalsVisibleTo Shared.Metrics.UnitTests).</summary>
    internal sealed record DebugState(
        IReadOnlyDictionary<string, long> LastSuccess,
        IReadOnlyDictionary<(string Loop, bool Ok), long> LoopTicks,
        IReadOnlyDictionary<(string Cluster, string Process), DebugPhase> Phases,
        IReadOnlyDictionary<(string Operation, string Result), long> Operations,
        int ClaimsHeld,
        IReadOnlyDictionary<(string Cluster, string Shard), long> WalLag,
        IReadOnlyDictionary<(string Cluster, string Shard, string Result), long> BackupVerifyTotals,
        IReadOnlyDictionary<string, long> WatchdogRestarts,
        IReadOnlyDictionary<(string Cluster, string Shard, string Result), long> BackupRestoreTotals,
        IReadOnlyDictionary<(string Cluster, string Shard, string Result), long> BackupDrillTotals,
        IReadOnlyDictionary<(string Cluster, string Shard), long> FullAgeFinishedUnix,
        IReadOnlyDictionary<(string Cluster, string Shard), long> FullMaxAge,
        IReadOnlyDictionary<(string Cluster, string Shard), long> WalUploadedUnix,
        double? SnapshotAgeSeconds,
        IReadOnlyDictionary<(string Cluster, string Shard), long> HaFailoverDurations,
        IReadOnlyDictionary<(string Cluster, string Shard), long> HaRebuildDurations);

    internal sealed record DebugPhase(string Phase, DateTimeOffset StartedAt);
}
