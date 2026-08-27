using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Core.Alerting;

// [Config]-POCO порогов алертов: секция AdminPanel:Alerts (arch/01 §6; заводит t05 — t04 §3.6).
// Регистрация — автоскан AddCore().
[Config("AdminPanel:Alerts")]
public class AlertsOptions
{
    // move-stale: не-ACTIVE статус без прогресса дольше N секунд (каталог 03 §4).
    public int StaleMoveSeconds { get; set; } = 600;

    // move-frozen-long: FROZEN дольше N секунд — cutover обязан быть секундами (каталог 03 §4).
    public int FrozenSeconds { get; set; } = 60;

    // replica-lag-high и slot-lag-high: порог лага в байтах (arch/01 §6, каталог 03 §4;
    // один порог лага на оба kind — spec §3.8). <= 0 — дефолт каталога.
    public long ReplicaLagBytes { get; set; } = 16 * 1024 * 1024;

    // slot-invalidation-risk: остаток safe_wal_size ниже порога — риск
    // среза слота (03 §4; отдельная семантика от лага, spec §3.8). <= 0 — дефолт 1 GiB.
    public long SlotSafeWalSizeBytes { get; set; } = 1024L * 1024 * 1024;
}
