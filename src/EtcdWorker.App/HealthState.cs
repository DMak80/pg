namespace EtcdWorker.App;

/// <summary>
/// Пассивные состояния наблюдаемости (arch/etcdworker/06-operability.md §4): циклы пишут тики/события,
/// health-проба /healthz читает снимок. Immutable-снимок + lock — без гонок.
/// Реализует ILoopProgress — доставка прогресс-отметок в процессы ниже App.
/// </summary>
public sealed class HealthState(TimeProvider clock) : Shared.Core.Hosting.ILoopProgress
{
    private readonly object _sync = new();

    private DateTimeOffset? _lastReconcileTick;
    private DateTimeOffset? _lastReconcileActivity;
    private DateTimeOffset? _lastKeepaliveTick;
    private DateTimeOffset? _lastSnapshotTick;
    private DateTimeOffset? _lastSnapshotActivity;
    private DateTimeOffset? _lastSnapshotTaken;
    private int _claimsHeld;

    /// <summary>Тик ReconcileLoop + сколько клэймов удерживаем после него
    /// (тик — тоже активность).</summary>
    public void MarkReconcileTick(bool ok, int claimsHeld)
    {
        lock (_sync)
        {
            _lastReconcileTick = clock.GetUtcNow();
            _lastReconcileActivity = _lastReconcileTick;
            if (ok)
                _claimsHeld = claimsHeld;
        }
    }

    /// <summary>Прогресс-отметка reconcile (heartbeat долгих фаз итерации:
    /// контейнеры нод, health-цикл ожидания готовности) — активность без тика;
    /// читает только watchdog, healthz loops-alive — по тикам.</summary>
    public void MarkReconcileActivity()
    {
        lock (_sync)
        {
            _lastReconcileActivity = clock.GetUtcNow();
        }
    }

    /// <summary>ILoopProgress — доставка прогресс-отметок в процессы ниже App.</summary>
    public void Mark() => MarkReconcileActivity();

    /// <summary>Тик KeepaliveLoop (продление lease'ов + instance-ключ).</summary>
    public void MarkKeepaliveTick()
    {
        lock (_sync)
        {
            _lastKeepaliveTick = clock.GetUtcNow();
        }
    }

    /// <summary>Тик SnapshotLoop (попытка снятия снапшота лидером).</summary>
    public void MarkSnapshotTick()
    {
        lock (_sync)
        {
            _lastSnapshotTick = clock.GetUtcNow();
        }
    }

    /// <summary>Пульс сна snapshot-цикла (активность БЕЗ тика, зеркально
    /// MarkReconcileActivity): законное ожидание длиннее порога сноса watchdog;
    /// читает только watchdog, healthz loops-alive — по тикам.</summary>
    public void MarkSnapshotActivity()
    {
        lock (_sync)
        {
            _lastSnapshotActivity = clock.GetUtcNow();
        }
    }

    /// <summary>Успешно снятый снапшот (snapshot-freshness).</summary>
    public void MarkSnapshotTaken()
    {
        lock (_sync)
        {
            _lastSnapshotTaken = clock.GetUtcNow();
        }
    }

    /// <summary>Immutable-снимок состояний для health-пробы.</summary>
    public HealthSnapshot Snapshot()
    {
        lock (_sync)
        {
            return new HealthSnapshot(
                _lastReconcileTick, _lastKeepaliveTick,
                _lastSnapshotTick, _lastSnapshotTaken, _claimsHeld,
                _lastReconcileActivity, _lastSnapshotActivity);
        }
    }
}

/// <summary>Снимок состояний циклов на момент чтения.</summary>
public sealed record HealthSnapshot(
    DateTimeOffset? LastReconcileTick,
    DateTimeOffset? LastKeepaliveTick,
    DateTimeOffset? LastSnapshotTick,
    DateTimeOffset? LastSnapshotTaken,
    int ClaimsHeld,
    DateTimeOffset? LastReconcileActivity,
    DateTimeOffset? LastSnapshotActivity);
