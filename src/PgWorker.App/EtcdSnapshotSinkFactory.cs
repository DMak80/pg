using PgWorker.App;

namespace PgWorker.Backups.EtcdExport;

/// <summary>Фабрика цели слепков etcd (arch/14 §6/§8): S3ClientOptions
/// из ОТДЕЛЬНОЙ пары PgWorker:Snapshots:Export:S3 (env PGW_SNAPSHOTS_S3_*) —
/// не креды пулов бэкапов. Тестируемая точка сборки Program-синка.</summary>
public static class EtcdSnapshotSinkFactory
{
    public static S3ClientOptions Create(PgWorkerOptions opts)
        => new(
            opts.Snapshots.Export.S3.Endpoint,
            opts.Snapshots.Export.S3.Bucket,
            opts.Snapshots.Export.S3.AccessKey,
            opts.Snapshots.Export.S3.SecretKey,
            opts.Snapshots.Export.S3.Region,
            opts.Snapshots.Export.S3.CaBundlePath);
}
