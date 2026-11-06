namespace Shared.Core;

/// <summary>Имя фиксированного etcd-слепкового bucket (arch/backups/06-s3-layout.md, arch/s3-clusters/05-platform.md,
/// arch/etcdnode/02-snapshots.md): одинаково на всех установках; создаёт S3Worker на инстансах
/// etcd-backup кластера, выгружают в него агенты EtcdNode. НЕ конфигурируется.</summary>
public static class EtcdSnapshotBucket
{
    public const string Name = "etcd-backups";
}
