namespace OwnS3.Storage.Erasure;

// Состояние для /admin/storage и метрик (канон 05 §5/§7).
public sealed class ErasureStatusProvider(ErasurePool pool)
{
    private readonly object _lock = new();
    private bool _healRunning;
    private DateTimeOffset? _healLastPassAt;
    private long _healedObjects;
    private long _healedShards;

    public ErasureStorageStatus GetStatus()
    {
        lock (_lock)
        {
            var sets = pool.Sets.Select((set, index) =>
            {
                var k = pool.Topology.SetSize - pool.Topology.Parity;
                var m = pool.Topology.Parity;
                var online = set.Count(d => d.Online);
                return new ErasureSetStatus(
                    index,
                    Readable: online >= ErasureQuorum.Read(k, m),
                    Writable: online >= ErasureQuorum.Write(k, m),
                    set.Select(d => new ErasureDriveStatus(d.Position, d.Root, d.DiskId, d.Online)).ToList());
            }).ToList();
            return new ErasureStorageStatus("XL Erasure", sets,
                new ErasureHealStatus(_healRunning, _healLastPassAt, _healedObjects, _healedShards));
        }
    }

    // Статистику heal пишет ErasureHealService.
    internal void UpdateHeal(bool running, DateTimeOffset? lastPassAt, long healedObjects, long healedShards)
    {
        lock (_lock)
        {
            _healRunning = running;
            _healLastPassAt = lastPassAt;
            _healedObjects = healedObjects;
            _healedShards = healedShards;
        }
    }
}

public sealed record ErasureStorageStatus(string Mode, IReadOnlyList<ErasureSetStatus> Sets, ErasureHealStatus Heal);

public sealed record ErasureSetStatus(int Index, bool Readable, bool Writable, IReadOnlyList<ErasureDriveStatus> Drives);

public sealed record ErasureDriveStatus(int Position, string Path, string DiskId, bool Online);

public sealed record ErasureHealStatus(bool Running, DateTimeOffset? LastPassAt, long HealedObjects, long HealedShards);
