using PgWorker.App;

namespace PgWorker.Backups.EtcdExport;

/// <summary>Фабрика цели слепков etcd (arch/14 §6/§8): пул-клиент из
/// ОТДЕЛЬНОЙ пары PgWorker:Snapshots:Export:S3 (env PGW_SNAPSHOTS_S3_* —
/// запятая-список URI) — не креды пулов бэкапов. Перебор целей до первого
/// доступного — канон arch/19 §5 (PoolBackupS3: пер-попыточный таймаут —
/// дефолт BackupS3 10 с, breaker-окно DefaultBreakerWindow 45 с);
/// write=read=разобранный список пары. Тестируемая точка сборки
/// Program-синка.</summary>
public static class EtcdSnapshotSinkFactory
{
    /// <summary>Per-цельные опции пары (чистая функция для тестов): разбор
    /// запятая-списка → S3ClientOptions на каждую цель; bucket/креды/Region/
    /// CA — одни на все цели пары (arch/14 §6/§8).</summary>
    internal static S3ClientOptions[] TargetOptions(SnapshotExportS3Options s3)
        => [.. SnapshotExportS3Options.ParseEndpoints(s3.Endpoints).Select(uri => new S3ClientOptions(
            uri, s3.Bucket, s3.AccessKey, s3.SecretKey, s3.Region, s3.CaBundlePath))];

    public static PoolBackupS3 Create(PgWorkerOptions opts)
    {
        var s3 = opts.Snapshots.Export.S3;
        var perTarget = TargetOptions(s3);
        var byUri = perTarget.ToDictionary(t => t.Endpoint);
        var uris = perTarget.Select(t => t.Endpoint).ToArray();
        // write=read: цель слепочной пары записываема и читаема (arch/19 §5);
        // targetFactory — per-цель пары со СВОИМИ Region/CA (дефолтная фабрика
        // пула их не знает); requestTimeout не задаём — дефолт BackupS3 10 с.
        return new PoolBackupS3(
            uris, uris,
            s3.AccessKey, s3.SecretKey, s3.Bucket, s3.CaBundlePath,
            targetFactory: uri => new BackupS3(byUri[uri]));
    }
}
