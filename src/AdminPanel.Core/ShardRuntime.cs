namespace AdminPanel.Core;

// Runtime-обогащение шарда из SQL-пробы (t06): слоты, standby, подписки, инвентарь бакетов.
public sealed record ShardRuntime(
    string Shard,
    IReadOnlyList<ReplicationSlotInfo> Slots,
    IReadOnlyList<StandbyInfo> Standbies,
    IReadOnlyList<SubscriptionInfo> Subscriptions,
    IReadOnlyList<string> BucketSchemas,
    bool? IsInRecovery,
    string? Error);

// Слот репликации (pg_replication_slots, P4).
public sealed record ReplicationSlotInfo(
    string SlotName,
    string SlotType,
    bool Active,
    string? WalStatus,
    long? SafeWalSizeBytes,
    long? LagBytes);

// Физическая реплика (pg_stat_replication, sync_state! — P8).
public sealed record StandbyInfo(
    string ApplicationName,
    string? ClientAddr,
    string State,
    string SyncState,
    long? LagBytes);

// Подписка логической репликации (pg_stat_subscription — прогресс переездов).
public sealed record SubscriptionInfo(
    string Name,
    string? ReceivedLsn,
    string? LatestEndLsn,
    DateTimeOffset? LatestEndTime);
