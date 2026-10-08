using Shared.Core.DI;

namespace AdminPanel.Core.Valkey.ValkeyAlerting;

// [Config]-POCO порогов valkey-алертов: секция AdminPanel:ValkeyAlerts
// (arch/03 §8.4). Регистрация — автоскан AddCore().
[Config("AdminPanel:ValkeyAlerts")]
public class ValkeyAlertsOptions
{
    // valkey-node-not-running: PROVISIONING младше N секунд не алертится
    // (штатный подъём ноды — critical-шум неуместен; порт KafkaAlertsOptions).
    public int FreshProvisioningSeconds { get; set; } = 60;

    // Stale-ротации (t10): живая ротационная заявка старше N секунд — видимость
    // зависания ДО возрастного снятия воркером. Половина RotationTicketTimeoutSec
    // воркеров (3600/2) — полчаса видимости; связность stale < timeout (spec §2).
    public int RotationStaleSeconds { get; set; } = 1800;
}
