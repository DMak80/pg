using Shared.Core.DI;

namespace AdminPanel.Core.S3.S3Alerting;

// [Config]-POCO порогов s3-алертов: секция AdminPanel:S3Alerts (arch/03 §12.4).
// Регистрация — автоскан AddCore().
[Config("AdminPanel:S3Alerts")]
public class S3AlertsOptions
{
    // s3-node-not-running: PROVISIONING младше N секунд не алертится.
    public int FreshProvisioningSeconds { get; set; } = 60;

    // s3-rotation-stale: живая заявка ротации старше N секунд.
    public int RotationStaleSeconds { get; set; } = 1800;

    // s3-sync-stale: connectivity/sync отсутствуют или checked_unix старше N c.
    public int SyncStaleSec { get; set; } = 300;

    // s3-node-disk-usage-high: used/total > N процентов.
    public int DiskUsagePercent { get; set; } = 85;
}
