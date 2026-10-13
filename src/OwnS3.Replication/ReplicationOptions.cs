// Конфигурация репликации (t35, глава 07 §11): секция OwnS3:Replication,
// env OWNS3_REPLICATION_* (таблица главы 07 §11 / глава 05 §4).
// Targets — seed env первого старта, НЕ источник истины (файл targets.json).
namespace OwnS3.Replication;

public sealed class ReplicationOptions
{
    public const string SectionName = "OwnS3:Replication";

    public TargetOptions[] Targets { get; set; } = [];                       // seed env, не источник истины
    public long OutboxBudgetBytes { get; set; } = 2L * 1024 * 1024 * 1024;  // bounded-бюджет журнала (§4)
    public int InflightWindow { get; set; } = 64;                           // окно in-flight notifier'а (§8)
    public int Parallelism { get; set; } = 1;                               // фетчеры: параллелизм на источник (§7)
    public long MaxRateBytes { get; set; } = 0;                             // троттлинг фетчей; 0 — безлимит (§7)
    public int BackoffMaxSec { get; set; } = 60;                            // потолок эксп-бэкоффа (§7/§8)
    public long DiskFreeMinBytes { get; set; } = 1024L * 1024 * 1024;       // watermark места фетчей (§7)
    public int TombstoneRetentionHours { get; set; } = 168;                 // retention tombstones (§7)
    public int ReconcileIntervalSeconds { get; set; } = 60;                 // период reconcile (§9)
    public int FetchQueueMax { get; set; } = 10000;                         // ёмкость fetch-очереди на источник (§7)
    public int ProbeTimeoutMs { get; set; } = 2000;                        // per-target таймаут ping-пробы (§2)
}

// Seed-запись соседа из env (OWNS3_REPLICATION_TARGETS__<i>__*).
public sealed record TargetOptions(string Name, string Endpoint, string AccessKey, string SecretKey);
