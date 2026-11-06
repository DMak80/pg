namespace EtcdNode.Core;

/// <summary>Материал CA https S3-endpoint цели слепков (arch/backups/08-secrets.md,
/// arch/etcdnode/01-agent.md §3): при заданном CaBundlePath PEM читается
/// файлом хоста ВОРКЕРА — содержимое уходит в env узла ETCDNODE_S3_CA_PEM,
/// агент материализует его в temp-файл для фабрики HttpClient с валидацией
/// корня цепочки по CA. null — путь не задан (http-endpoint).</summary>
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
                $"CA bundle {caBundlePath} is not readable — {ex.Message}", ex);
        }

        return pem.Trim() is { Length: > 0 } trimmed
            ? trimmed
            : throw new InvalidOperationException(
                $"CA bundle {caBundlePath} is empty");
    }
}
