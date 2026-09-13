namespace AdminPanel.Core;

// Бэкапы кластера из /pgworker/backups/ (arch/19 §4, t02; дубль воркерной модели
// осознанный — унификация t08-unify-adminpanel-duplicates). FullMaxAgeSec — null,
// если policy-ключа нет (правило берёт панельный дефолт). В словаре — шарды,
// у которых есть ХОТЯ БЫ ОДИН ключ полных: значение null = COMPLETED не было
// («полного никогда не было»); «шарда нет в словаре» = подсистема не включена
// для него → правило молчит.

/// <summary>Последний проваленный verify шарда (t04): полный невалиден — вход
/// правила backup-verify-failed (текст алерта — Error).</summary>
public sealed record ShardVerifyFailure(string Shard, string Id, string Error, long? CheckedUnix);

public sealed record ClusterBackupsInfo(
    string Cluster,
    long? FullMaxAgeSec,
    IReadOnlyDictionary<string, long?> ShardLastCompletedUnix,
    // t03: WAL-статусы шардов (ключи /pgworker/backups/<C>/<X>/wal) — вход
    // правил wal-chain-broken/wal-stream-lag/wal-stream-stopped; null = ключей
    // wal нет (агент не поднимался).
    IReadOnlyDictionary<string, WalStreamInfo?>? Shards = null,
    // t06: DELETING-полные per-shard (застарелые → алерт backup-deleting-stuck).
    IReadOnlyDictionary<string, IReadOnlyList<DeletingFullInfo>>? DeletingFulls = null,
    // t04: последний verify-FAILED по шарду (по checked_unix); пустой словарь =
    // невалидных полных нет.
    IReadOnlyDictionary<string, ShardVerifyFailure>? ShardVerifyFailures = null,
    // t05: restore-заявки per-shard (вход правила restore-failed).
    IReadOnlyDictionary<string, IReadOnlyList<RestoreOperationInfo>>? ShardsRestores = null,
    // t08: все etcd-полные per-shard — вход MinioReconciler; null = парсер t08
    // их не собрал.
    IReadOnlyDictionary<string, IReadOnlyList<BackupFullInfo>>? ShardsFulls = null);

/// <summary>Один etcd-ключ полного /pgworker/backups/&lt;C&gt;/&lt;X&gt;/full/&lt;id&gt; (t08):
/// полный факт state/verify/size для сверки с S3 и деталей шарда.</summary>
public sealed record BackupFullInfo(
    string Id, string State, string? Error,
    long StartedUnix, long? FinishedUnix, long? SizeBytes,
    string? VerifyState, long? VerifyCheckedUnix, string? VerifyError);

/// <summary>DELETING-полный (t06): возраст для backup-deleting-stuck.</summary>
public sealed record DeletingFullInfo(string Id, long StartedUnix, long? FinishedUnix);

/// <summary>Операция восстановления шарда (t05, arch/19 §4): только поля статусов
/// — UI restore-операций t08; RequestedUnix обязателен, остальное — по факту.</summary>
public sealed record RestoreOperationInfo(
    string Cluster,
    string Shard,
    string Id,
    string State,
    string? Error,
    long RequestedUnix,
    long? StartedUnix,
    long? FinishedUnix,
    string? Phase);

/// <summary>Статус одного полного в сверке S3↔etcd (t08, arch/02 §2.5):
/// Ok — объекты+ключ; S3Only — объекты без ключа; EtcdOnly — ключ без объектов
/// (COMPLETED/FAILED); InProgress — активный PLANNED/RUNNING/UPLOADING без
/// объектов (ожидаемо); Deleting — DELETING-ключ + остатки объектов.</summary>
public enum BackupFullReconcileStatus { Ok, S3Only, EtcdOnly, InProgress, Deleting }

public sealed record BackupFullReconcile(
    string Cluster, string Shard, string Id,
    BackupFullReconcileStatus Status,
    long? SizeBytes, long? ObjectCount, long? LastModifiedUnix, // S3-факт (null — объектов нет)
    string? EtcdState, string? VerifyState, long? EtcdSizeBytes); // etcd-факт (null — ключа нет)

/// <summary>Сирота по сверке ПАНЕЛИ (префикс &lt;C&gt;/&lt;X&gt; без владельца в /clusters/):
/// рядом — факт реестра воркера (InWorkerRegistry/RegistryState/FirstSeenUnix).</summary>
public sealed record BackupOrphanPrefix(
    string Prefix, string Kind, long SizeBytes,
    bool InWorkerRegistry, string? RegistryState, long? FirstSeenUnix);

/// <summary>WAL-сверка — только факты, без вердикта (spec §4.4).</summary>
public sealed record BackupWalReconcile(
    string Cluster, string Shard,
    string? EtcdLastSegment, long? EtcdLastUnix,
    string? S3LastObject, long? S3LastModifiedUnix);

/// <summary>Итог MinioReconciler.Reconcile — чистая функция над снапшотом.</summary>
public sealed record BackupReconcileInfo(
    IReadOnlyList<BackupFullReconcile> Fulls,
    IReadOnlyList<BackupOrphanPrefix> OrphanPrefixes,
    IReadOnlyList<BackupWalReconcile> Wal);
