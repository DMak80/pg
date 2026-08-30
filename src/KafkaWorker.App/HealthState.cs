namespace KafkaWorker.App;

/// <summary>
/// Пассивные состояния наблюдаемости (spec §8): циклы пишут тики/события,
/// health-проба /healthz читает снимок. Immutable-снимок + lock — без гонок.
/// </summary>
public sealed class HealthState(TimeProvider clock)
{
    private readonly object _sync = new();

    private DateTimeOffset? _lastEtcdOk;
    private DateTimeOffset? _lastReconcileTick;
    private DateTimeOffset? _lastKeepaliveTick;
    private DateTimeOffset? _lastSnapshotTick;
    private DateTimeOffset? _lastSnapshotTaken;
    private int _claimsHeld;

    /// <summary>Успешный цикл чтения etcd (Range /clusters/ + /service/).</summary>
    public void MarkEtcdOk()
    {
        lock (_sync)
        {
            _lastEtcdOk = clock.GetUtcNow();
        }
    }

    /// <summary>Тик ReconcileLoop + сколько клэймов удерживаем после него.</summary>
    public void MarkReconcileTick(bool ok, int claimsHeld)
    {
        lock (_sync)
        {
            _lastReconcileTick = clock.GetUtcNow();
            if (ok)
                _claimsHeld = claimsHeld;
        }
    }

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
                _lastEtcdOk, _lastReconcileTick, _lastKeepaliveTick,
                _lastSnapshotTick, _lastSnapshotTaken, _claimsHeld);
        }
    }
}

/// <summary>Снимок состояний циклов на момент чтения.</summary>
public sealed record HealthSnapshot(
    DateTimeOffset? LastEtcdOk,
    DateTimeOffset? LastReconcileTick,
    DateTimeOffset? LastKeepaliveTick,
    DateTimeOffset? LastSnapshotTick,
    DateTimeOffset? LastSnapshotTaken,
    int ClaimsHeld);
