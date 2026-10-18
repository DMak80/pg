namespace PgWorker.Backups;

/// <summary>Материал CA https S3-endpoint для контейнеров джобов/агентов
/// (t42, arch/19 §7): файл CA хоста воркера в мульти-хост контейнерам недоступен —
/// PEM передаётся значением env S3_CA_PEM и материализуется внутри контейнера
/// (mc — SSL_CERT_FILE; SDK-клиенты — фабрика HttpClient с валидацией корня
/// цепочки по отпечатку CA, при заданном CaBundlePath). null — путь не задан
/// (http-endpoint, прежний env-набор).</summary>
public static class BackupCaPem
{
    public const string EnvName = "S3_CA_PEM";

    /// <summary>null — CaBundlePath не задан; нечитаемый/пустой файл — исключение
    /// (спека джоба/тик воркера падает с диагностикой, а не молча без доверия CA).</summary>
    public static string? ReadIfConfigured(BackupsRuntimeOptions opts)
        => ReadIfConfigured(opts.S3CaBundlePath);

    // t36: строковый путь (цель S3ClientOptions — пул/слепки), механика та же.
    public static string? ReadIfConfigured(string? caBundlePath)
    {
        if (string.IsNullOrWhiteSpace(caBundlePath))
            return null;
        string pem;
        try
        {
            pem = File.ReadAllText(caBundlePath);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"PgWorker:Backups:S3:CaBundlePath={caBundlePath}: CA file is not readable — {ex.Message}", ex);
        }

        return pem.Trim() is { Length: > 0 } trimmed
            ? trimmed
            : throw new InvalidOperationException(
                $"PgWorker:Backups:S3:CaBundlePath={caBundlePath}: CA file is empty");
    }
}
