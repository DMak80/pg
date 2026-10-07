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
    long? RetryNotBeforeUnix,
    HaSupervisionInfo? LastFailover = null,
    HaSupervisionInfo? LastRebuild = null);

// Последний HA-факт надзора из /pgworker/work/<C> (arch/14 §3.3;
// панельный дубль воркерной модели — осознанный): null = факта нет/старый ключ.
public sealed record HaSupervisionInfo(
    string Shard, string Node, string Cause,
    long DetectedUnix, long? ResolvedUnix, long? DurationSec);
