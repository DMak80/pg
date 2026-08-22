using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Core.Alerting;

// [Config]-POCO порогов алертов: секция AdminPanel:Alerts (arch/01 §6; заводит t05 — t04 §3.6).
// Регистрация — автоскан AddCore(); ReplicaLagBytes появится в t06 (YAGNI, spec §4.5).
[Config("AdminPanel:Alerts")]
public class AlertsOptions
{
    // move-stale: не-ACTIVE статус без прогресса дольше N секунд (каталог 03 §4).
    public int StaleMoveSeconds { get; set; } = 600;

    // move-frozen-long: FROZEN дольше N секунд — cutover обязан быть секундами (каталог 03 §4).
    public int FrozenSeconds { get; set; } = 60;
}
