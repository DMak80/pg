namespace EtcdWorker.Core;

/// <summary>Материал CA https S3-endpoint цели слепков (перенос BackupCaPem
/// t08/t42, arch/19 §7): при заданном CaBundlePath PEM читается файлом хоста
/// воркера — SDK-клиент получает фабрику HttpClient с валидацией корня цепочки
/// по CA. null — путь не задан (http-endpoint).</summary>
public static class BackupCaPem
{
    // Строковый путь (механика та же, что в PgWorker.Backups; перегрузка
    // BackupsRuntimeOptions не переносится — тип PgWorker недоступен).
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
                $"EtcdWorker:Snapshots:S3:CaBundlePath={caBundlePath}: CA file is not readable — {ex.Message}", ex);
        }

        return pem.Trim() is { Length: > 0 } trimmed
            ? trimmed
            : throw new InvalidOperationException(
                $"EtcdWorker:Snapshots:S3:CaBundlePath={caBundlePath}: CA file is empty");
    }
}
