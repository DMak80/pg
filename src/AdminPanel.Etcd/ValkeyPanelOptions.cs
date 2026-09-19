using Shared.Core.DI;

namespace AdminPanel.Etcd;

// [Config]-POCO valkey-домена панели: секция AdminPanel:Valkey (arch/02 §11).
// Endpoints etcd — общие с pg-циклом через AdminPanel:Etcd (EtcdOptions).
[Config("AdminPanel:Valkey")]
public class ValkeyPanelOptions
{
    // Тик ValkeySnapshotRefresher. <= 0 — fallback 3 c (симметрия kafka, arch/02 §4).
    public double RefreshIntervalSeconds { get; set; } = 3;
}
