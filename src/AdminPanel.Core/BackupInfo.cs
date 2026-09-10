namespace AdminPanel.Core;

// Бэкапы кластера из /pgworker/backups/ (arch/19 §4, t02; дубль воркерной модели
// осознанный — унификация t08-unify-adminpanel-duplicates). FullMaxAgeSec — null,
// если policy-ключа нет (правило берёт панельный дефолт). В словаре — шарды,
// у которых есть ХОТЯ БЫ ОДИН ключ полных: значение null = COMPLETED не было
// («полного никогда не было»); «шарда нет в словаре» = подсистема не включена
// для него → правило молчит.
public sealed record ClusterBackupsInfo(
    string Cluster,
    long? FullMaxAgeSec,
    IReadOnlyDictionary<string, long?> ShardLastCompletedUnix,
    // t03: WAL-статусы шардов (ключи /pgworker/backups/<C>/<X>/wal) — вход
    // правил wal-chain-broken/wal-stream-lag/wal-stream-stopped; null = ключей
    // wal нет (агент не поднимался).
    IReadOnlyDictionary<string, WalStreamInfo?>? Shards = null);
