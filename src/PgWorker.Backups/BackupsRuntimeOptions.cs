namespace PgWorker.Backups;

/// <summary>Runtime-опции подсистемы бэкапов (arch/19 §9): склейка секции
/// PgWorker:Backups для процесса WalStreamProcess (образец MovesRuntimeOptions).
/// Достаётся из App через BackupsOptions.ToRuntime().</summary>
/// <param name="AgentImage">Образ агента/джоба (контракт arch/19 §2, сборка t02).</param>
/// <param name="S3Endpoint">S3 endpoint (MinIO/облако) для КЛИЕНТОВ ВОРОКЕРА.</param>
/// <param name="S3AdvertisedEndpoint">Endpoint, как S3 виден ИЗ контейнеров агентов
/// (single-host стенды: host.docker.internal; null → S3Endpoint — паттерн
/// Etcd:AdvertisedEndpoints). Реализация-деталь t03 (адресация env агента, §7) —
/// канон §9 её не перечисляет сознательно; стенд-включение подсистемы — t02.</param>
/// <param name="S3Bucket">Bucket per-install (arch/19 §5).</param>
/// <param name="WalVerifyIntervalSec">Период list S3 + контроля цепочки (§3).</param>
/// <param name="WalLagMaxSegments">Порог отставания в сегментах → DEGRADED.</param>
/// <param name="WalStaleSec">Порог тишины загрузок → DEGRADED.</param>
public sealed record BackupsRuntimeOptions(
    string AgentImage,
    string S3Endpoint,
    string? S3AdvertisedEndpoint,
    string? S3Region,
    string S3Bucket,
    string S3AccessKey,
    string S3SecretKey,
    bool S3PathStyle,
    string StagingDir,
    long? StagingQuotaBytes,
    double? AgentCpu,
    long? AgentMem,
    int WalVerifyIntervalSec,
    int WalLagMaxSegments,
    int WalStaleSec)
{
    /// <summary>Endpoint для env контейнера агента (§7: передача — env, не строка команды).</summary>
    public string AgentS3Endpoint => S3AdvertisedEndpoint is { Length: > 0 } ? S3AdvertisedEndpoint : S3Endpoint;
}
