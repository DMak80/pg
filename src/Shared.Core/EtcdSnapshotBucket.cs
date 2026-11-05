namespace Shared.Core;

/// <summary>Имя фиксированного etcd-слепкового bucket (arch/backups/06-s3-layout.md, arch/s3-clusters/05-platform.md,
/// arch/etcdworker/04-snapshots.md): одинаково на всех установках; создаёт S3Worker на инстансах
/// etcd-backup кластера, выгружает в него EtcdWorker. НЕ конфигурируется.</summary>
public static class EtcdSnapshotBucket
{
    public const string Name = "etcd-backups";
}
