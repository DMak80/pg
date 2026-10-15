using Shared.Core.DI;

namespace AdminPanel.Etcd;

// [Config]-POCO s3-домена панели: секция AdminPanel:S3 (arch/02 §12).
// Endpoints etcd — общие с pg-циклом через AdminPanel:Etcd (EtcdOptions).
[Config("AdminPanel:S3")]
public class S3PanelOptions
{
    // Тик S3SnapshotRefresher. <= 0 — fallback 3 c (симметрия kafka/valkey).
    public double RefreshIntervalSeconds { get; set; } = 3;
}
