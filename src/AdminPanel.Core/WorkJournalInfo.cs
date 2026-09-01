namespace AdminPanel.Core;

// Журнал текущего процесса кластера /pgworker/work/<C> (arch/adminpanel/02 §2.3.1,
// формат arch/14 §3.3): фаза/ошибка процесса + серия фейлов provision (бэкофф).
// Поля серии optional — журналы старого формата читаются с null.
public sealed record WorkJournalInfo(
    string Cluster,
    string Op,
    string Phase,
    string Instance,
    long UpdatedUnix,
    string? LastError,
    int? FailCount,
    long? FailFirstUnix,
    long? RetryNotBeforeUnix);
