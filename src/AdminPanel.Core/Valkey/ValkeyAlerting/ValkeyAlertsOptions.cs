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
}
