using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Core.Kafka.KafkaAlerting;

// [Config]-POCO порогов kafka-алертов: секция AdminPanel:KafkaAlerts
// (arch/03 §7.4). Регистрация — автоскан AddCore().
[Config("AdminPanel:KafkaAlerts")]
public class KafkaAlertsOptions
{
    // kafka-broker-not-running: PROVISIONING младше N секунд не алертится
    // (штатный подъём брокера занимает десятки секунд — critical-шум неуместен).
    public int FreshProvisioningSeconds { get; set; } = 60;

    // kafka-desired-stale (волна C): desired не снят дольше N секунд — converge буксует.
    public int StaleDesiredSeconds { get; set; } = 600;

    // kafka-group-lag-high (волна C): порог totalLag группы в сообщениях.
    public long GroupLagMessages { get; set; } = 100000;
}
