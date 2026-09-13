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
    IReadOnlyDictionary<string, IReadOnlyList<RestoreOperationInfo>>? ShardsRestores = null);

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
