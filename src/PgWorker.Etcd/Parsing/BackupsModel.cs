namespace PgWorker.Etcd.Parsing;

// Модель подсистемы бэкапов (arch/19 §4, t01): префикс /pgworker/backups/*
// пишет ТОЛЬКО PgWorker (клэйм <C>), панель читает (02 §2.3.1); потребители в
// воркере — t02+. Значения-строки etcd → типизированные records.

/// <summary>Состояние полного бэкапа (etcd state, arch/19 §4).</summary>
public enum FullBackupStatus
{
    Planned,
    Running,
    Uploading,
    Completed,
    Failed,
    Deleting,
}

/// <summary>Источник полного бэкапа: штатно реплика, fallback — мастер
/// (журнал-факт I/O одной операции, arch/19 §6).</summary>
public enum BackupSourceRole
{
    Replica,
    Master,
}

/// <summary>Состояние WAL-потока шарда (arch/19 §4).</summary>
public enum WalStreamStatus
{
    Active,
    Degraded,
    Stopped,
}

/// <summary>Статус проверки полного (pg_verifybackup — t04, arch/19 §8).</summary>
public enum BackupVerifyStatus
{
    Pending,
    Ok,
    Failed,
}

/// <summary>Per-cluster политика бэкапов (ключ
/// /pgworker/backups/&lt;C&gt;/policy); отсутствует → дефолт конфига
/// PgWorker:Backups:Policy у потребителя (t02+).</summary>
/// <param name="RetentionDays">GFS: хранить N дневных (t06).</param>
/// <param name="RetentionWeeks">GFS: N недельных.</param>
/// <param name="RetentionMonths">GFS: N месячных.</param>
/// <param name="FullMaxAgeSec">Окно суточного алерта «нет валидного полного» (t02).</param>
/// <param name="VerifyOnCreate">Проверять полный сразу после создания (t04).</param>
public sealed record BackupPolicy(
    int RetentionDays, int RetentionWeeks, int RetentionMonths,
    long FullMaxAgeSec, bool VerifyOnCreate);

/// <summary>Результат проверки полного: состояние + время последней проверки.</summary>
public sealed record BackupVerify(BackupVerifyStatus State, long? CheckedUnix);

/// <summary>Один полный бэкап шарда (ключ
/// /pgworker/backups/&lt;C&gt;/&lt;X&gt;/full/&lt;id&gt;, id=YYYYMMDDHHMMSSZ
/// сортируемый, коллизия — суффикс -2/-3, arch/19 §2); WalStartSegment —
/// опционален до фазы UPLOADING (t02: заполняется с UPLOADING, §4).</summary>
public sealed record FullBackupState(
    string Id,
    FullBackupStatus State,
    string Node,
    BackupSourceRole Role,
    long StartedUnix,
    long? FinishedUnix,
    string? WalStartSegment,
    long? SizeBytes,
    string? Error,
    BackupVerify? Verify);

/// <summary>WAL-поток шарда (ключ /pgworker/backups/&lt;C&gt;/&lt;X&gt;/wal):
/// агент pg_receivewal (t03); инвариант непрерывности chain_start →
/// last_uploaded без дыр (arch/19 §3).</summary>
public sealed record WalStreamState(
    WalStreamStatus State,
    string Slot,
    string MasterNode,
    string ChainStartSegment,
    string LastReceivedSegment,
    string LastUploadedSegment,
    long? LastUploadedUnix,
    long? LagSegments,
    string? Error);

/// <summary>Бэкапы одного шарда: полные (сортированы по Id) + WAL-поток
/// (null — ключа нет: агент не поднимался, t03).</summary>
public sealed record ShardBackups(
    IReadOnlyList<FullBackupState> Full,
    WalStreamState? Wal);

/// <summary>Бэкапы кластера: политика (null — дефолт конфига) + шарды.</summary>
public sealed record ClusterBackups(
    string Cluster,
    BackupPolicy? Policy,
    IReadOnlyDictionary<string, ShardBackups> Shards);
