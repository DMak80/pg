namespace AdminPanel.Core;

// Кластер <C> из /clusters/<C>/…: константы, шарды, бакеты, журнал heals (arch/02 §2.1).
public sealed record ClusterInfo(
    string Name,
    string? DbName,
    int BucketsCount,
    long? CreatedUnix,
    IReadOnlyList<ShardInfo> Shards,
    IReadOnlyList<BucketInfo> Buckets,
    IReadOnlyList<HealRecord> Heals)
{
    // Пометка «incomplete» (arch/02 §7): префикс есть, config отсутствует/пуст.
    public bool Incomplete => DbName is null || BucketsCount <= 0;
}

// Шард кластера: dsn, декларативные реплики, master-ключ с lease-семантикой (arch/02 §2.1).
public sealed record ShardInfo(
    string Name,
    string Dsn,
    IReadOnlyList<string> DsnHosts,
    int? Port,
    string? DbName,
    string? User,
    int? ReplicasDeclared,
    string? MasterAddress,
    ShardRuntime? Runtime)
{
    // Lease-семантика master-ключа (arch/02 §1): ключ есть = lease жив.
    public bool MasterLeaseAlive => MasterAddress is not null;
}

// Бакет: id, владелец (routing), состояние переезда (arch/02 §2.1).
public sealed record BucketInfo(
    int Id,
    string? Owner,
    BucketState State,
    MoveInfo? Move);

// Статус бакета: отсутствие status-ключа = ACTIVE (arch/02 §2.1).
public enum BucketState
{
    Active,
    Syncing,
    Frozen,
    Aborting,
}

// Поля статус-ключа переезда (значение /clusters/<C>/buckets/status/bucket_<N>).
public sealed record MoveInfo(
    string? Owner,
    string? Target,
    long? StartedUnix,
    long? UpdatedUnix,
    string? Phase,
    string? LastError);

// Запись журнала авто-починки (значение /clusters/<C>/heals/<bucket>).
public sealed record HealRecord(
    string Bucket,
    string? Was,
    string? Now,
    string? Reason,
    long? TsUnix);
