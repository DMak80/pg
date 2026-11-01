namespace Shared.Core;

/// <summary>Имя фиксированного etcd-слепкового bucket (arch/19 §5, arch/23 §5,
/// arch/25 §4): одинаково на всех установках; создаёт S3Worker на инстансах
/// etcd-backup кластера, выгружает в него EtcdWorker. НЕ конфигурируется.</summary>
public static class EtcdSnapshotBucket
{
    public const string Name = "etcd-backups";
}
