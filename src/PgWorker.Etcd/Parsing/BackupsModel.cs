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

/// <summary>Состояние операции восстановления шарда (ключ
/// /pgworker/backups/&lt;C&gt;/&lt;X&gt;/restore/&lt;id&gt;, arch/19 §4).</summary>
public enum RestoreStatus
{
    Planned,
    Running,
    Rejoining,
    Completed,
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
/// <param name="VerifyIntervalSec">Период перепроверки оставшихся полных, c
/// (t04); null — не задан в policy-ключе → дефолт подставляет потребитель.</param>
public sealed record BackupPolicy(
    int RetentionDays, int RetentionWeeks, int RetentionMonths,
    long FullMaxAgeSec, bool VerifyOnCreate, long? VerifyIntervalSec = null);

/// <summary>Результат проверки полного: состояние + время последней проверки;
/// Error — причина провала (t04, только для FAILED).</summary>
public sealed record BackupVerify(BackupVerifyStatus State, long? CheckedUnix, string? Error = null);

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

/// <summary>Операция восстановления шарда из бэкапа (ключ
/// /pgworker/backups/&lt;C&gt;/&lt;X&gt;/restore/&lt;id&gt;, id=YYYYMMDDHHMMSSZ
/// как у полных, arch/19 §4). Source — S3-префикс источника
/// «&lt;srcC&gt;/&lt;srcX&gt;» (default — собственный); Target — «latest»
/// или «time:&lt;RFC3339&gt;»; Phase — фаза джоба (downloading|recovering).</summary>
public sealed record RestoreOperationState(
    string Id,
    RestoreStatus State,
    string BackupId,
    string Source,
    string Target,
    string Node,
    long RequestedUnix,
    string RequestedBy,
    long? StartedUnix = null,
    long? FinishedUnix = null,
    string? Phase = null,
    string? RestoredToLsn = null,
    string? Error = null)
{
    /// <summary>System id восстановленного PGDATA (из result-джоба): щит
    /// /service/&lt;scope&gt;/initialize в rejoin'е — пустые ноды в гонке не
    /// могут initdb'нуться в чужой кластер. null — старый образ джоба.</summary>
    public string? SystemId { get; init; }
}

/// <summary>Бэкапы одного шарда: полные (сортированы по Id) + WAL-поток
/// (null — ключа нет: агент не поднимался, t03) + restore-операции (t05,
/// сортированы по Id; пусто — restore-ключей нет). Опциональный ctor-параметр
/// Restores сохраняет обратную совместимость вызовов new(full, wal) (t02+).</summary>
public sealed record ShardBackups(
    IReadOnlyList<FullBackupState> Full,
    WalStreamState? Wal,
    IReadOnlyList<RestoreOperationState>? Restores = null)
{
    // Не-null инвариант для гвардов t05: пусто, если restore-ключей нет.
    public IReadOnlyList<RestoreOperationState> Restores { get; init; } = Restores ?? [];
}

/// <summary>Бэкапы кластера: политика (null — дефолт конфига) + шарды.</summary>
public sealed record ClusterBackups(
    string Cluster,
    BackupPolicy? Policy,
    IReadOnlyDictionary<string, ShardBackups> Shards);
