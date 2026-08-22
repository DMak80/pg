namespace AdminPanel.Core.Alerting;

// Правило каталога алертов (arch/03 §4): один kind, чистая оценка снапшота.
// Каркас: t05/t06 добавляют правила новыми классами без правки AlertEngine (spec §3.2).
public interface IAlertRule
{
    // Kind каталога, напр. "etcd-unreachable" (arch/03 §4).
    string Kind { get; }

    // Алерты правила по текущему снапшоту (0..N; SinceUnix проставляет AlertEngine).
    IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context);
}

// Параметры оценки вне снапшота: прошлый снапшот (sinceUnix), текущее время и период тика
// (порог snapshot-stale). Core не знает настроек — направление зависимостей arch/01 §1 (spec §3.3).
public sealed record AlertContext(
    EtcdSnapshot? Previous,
    DateTimeOffset NowUtc,
    double RefreshIntervalSeconds);

// Чистая функция Snapshot → Alert[] (arch/01 §2): правила + общая механика (spec §4.1).
public interface IAlertEngine
{
    IReadOnlyList<Alert> Evaluate(
        EtcdSnapshot snapshot,
        EtcdSnapshot? previous,
        DateTimeOffset nowUtc,
        double refreshIntervalSeconds);
}
