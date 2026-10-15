namespace PgWorker.Backups;

/// <summary>Маркер исчерпания пула (t36, arch/19 §4): в Error статусов воркера
/// (wal DEGRADED / full FAILED) и в Failed-исходах пула-клиента; панель зажигает
/// алерт backup-pool-unreachable по строке-литералу. Общий для воркера/
/// приёмника/джобов — не выносить в разные места.</summary>
public static class S3PoolMarkers
{
    public const string Exhausted = "s3-pool-exhausted";
}

/// <summary>Общая запись ОДНОЙ S3-цели (t36): endpoint+bucket+креды+CA;
/// потребитель — BackupS3 (второй конструктор), фабрика пула, слепочная пара
/// (EtcdSnapshotSink), WalReceiver Program. Заменяет завязку BackupS3 на
/// BackupsRuntimeOptions.</summary>
public sealed record S3ClientOptions(
    string Endpoint, string Bucket, string AccessKey, string SecretKey,
    string? Region = null, string? CaBundlePath = null);
