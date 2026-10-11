namespace PgWorker.Backups;

/// <summary>Материал CA https S3-endpoint для контейнеров джобов/агентов
/// (t42, arch/19 §7): файл CA хоста воркера в мульти-хост контейнерам недоступен —
/// PEM передаётся значением env S3_CA_PEM и материализуется внутри контейнера
/// (mc — SSL_CERT_FILE, AWS SDK — AWS_CA_BUNDLE). null — путь не задан
/// (http-endpoint, прежний env-набор).</summary>
public static class BackupCaPem
{
    public const string EnvName = "S3_CA_PEM";

    /// <summary>null — CaBundlePath не задан; нечитаемый/пустой файл — исключение
    /// (спека джоба/тик воркера падает с диагностикой, а не молча без доверия CA).</summary>
    public static string? ReadIfConfigured(BackupsRuntimeOptions opts)
    {
        if (string.IsNullOrWhiteSpace(opts.S3CaBundlePath))
            return null;
        string pem;
        try
        {
            pem = File.ReadAllText(opts.S3CaBundlePath);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"PgWorker:Backups:S3:CaBundlePath={opts.S3CaBundlePath}: файл CA не читается — {ex.Message}", ex);
        }

        return pem.Trim() is { Length: > 0 } trimmed
            ? trimmed
            : throw new InvalidOperationException(
                $"PgWorker:Backups:S3:CaBundlePath={opts.S3CaBundlePath}: файл CA пуст");
    }
}
