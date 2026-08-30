using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Etcd;

// [Config]-POCO kafka-домена панели: секция AdminPanel:Kafka (arch/02 §10).
// Endpoints etcd — общие с pg-циклом через AdminPanel:Etcd (EtcdOptions).
[Config("AdminPanel:Kafka")]
public class KafkaPanelOptions
{
    // Тик KafkaSnapshotRefresher. <= 0 — fallback 3 c (симметрия pg, arch/02 §4).
    public double RefreshIntervalSeconds { get; set; } = 3;
}
