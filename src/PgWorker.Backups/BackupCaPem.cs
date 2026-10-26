namespace PgWorker.Backups;

/// <summary>Материал CA https S3-endpoint (t42, arch/19 §7): воркер читает PEM
/// из CaBundlePath (TLS-пакет установки) — S3-клиенты с фабрикой доверия по
/// корню цепочки; tool-операции получают PEM в POST-команде (материализация на
/// приёмнике), приёмник ноды — env ноды (PGW_BACKUP_S3_CA_PEM).</summary>
public static class BackupCaPem
{
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
