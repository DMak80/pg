using PgWorker.Backups;
using PgWorker.Etcd.S3;

namespace PgWorker.WalReceiver.Operations;

/// <summary>S3-контекст restore_command (env процесса postgres — секреты не в
/// argv, Р13; выставляет RestoreOperation при старте postgres — наследуется до
/// restore_command).</summary>
internal sealed record FetchWalContext(
    string[] Endpoints, string Bucket, string Prefix,
    string AccessKey, string SecretKey, string? CaPem);

/// <summary>Sub-режим того же бинаря `walreceiver fetch-wal &lt;seg&gt; &lt;dest&gt;`
/// (arch/19 §3.5, spec t26 §3.4): restore_command postgres — скачивание
/// сегмента/.history из wal/-префикса С ПЕРВОЙ ноды пула, ГДЕ ОН ЕСТЬ (перебор
/// read-списка до найденного — arch/19 §5; лаг mesh допущен); нигде нет →
/// exit 1 — конец WAL (канон §3.5). Регистр: %f приходит ВЕРХНИМ hex
/// (XLogFileName), объекты S3 записаны приёмником строчными, S3-ключи
/// регистрозависимы — нормализация обязательна, иначе первый буквенный сегмент
/// даёт ложный 404 и обрывает recovery до цели. Файл уже лежит по dest
/// (переполучение восстановления) → exit 0 без хода в S3.</summary>
internal static class FetchWalCommand
{
    /// <summary>Чистая функция для тестов: нормализация регистра сегмента.</summary>
    internal static string NormalizeSegment(string segment)
        => segment.ToLowerInvariant();

    /// <summary>Чистая функция для тестов: разбор S3-контекста из env
    /// (порядок endpoints сохраняется — порядок из команды запуска, Р4);
    /// null — обязательные переменные отсутствуют/пусты.</summary>
    internal static FetchWalContext? ParseEnv(Func<string, string?> env)
    {
        var endpoints = (env("PGW_S3_ENDPOINTS") ?? string.Empty)
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var bucket = env("PGW_S3_BUCKET");
        var prefix = env("PGW_S3_PREFIX");
        var accessKey = env("PGW_S3_ACCESS_KEY");
        var secretKey = env("PGW_S3_SECRET_KEY");
        if (endpoints.Length == 0
            || string.IsNullOrWhiteSpace(bucket)
            || string.IsNullOrWhiteSpace(prefix)
            || string.IsNullOrWhiteSpace(accessKey)
            || string.IsNullOrWhiteSpace(secretKey))
            return null;
        return new FetchWalContext(
            endpoints, bucket, prefix, accessKey, secretKey,
            string.IsNullOrWhiteSpace(env("PGW_S3_CA_PEM")) ? null : env("PGW_S3_CA_PEM"));
    }

    /// <summary>Точка входа sub-режима (Program.Main ДО builder): код 0 — объект
    /// записан в dest (или уже был там); 1 — объекта нет нигде/отказ/невалидный
    /// env (для postgres — конец WAL или сбой recovery).</summary>
    public static async Task<int> RunAsync(string[] args)
    {
        if (args is not [{ } segment, { } dest, ..] || args.Length != 2)
        {
            Console.Error.WriteLine("fetch-wal: usage: fetch-wal <segment> <dest>");
            return 1;
        }
        var context = ParseEnv(Environment.GetEnvironmentVariable);
        if (context is null)
        {
            Console.Error.WriteLine("fetch-wal: S3 context env is missing or incomplete");
            return 1;
        }

        // dest — путь из %p postgres (полный путь файла); сегмент нормализуем
        // ТОЛЬКО для S3-ключа (объекты wal/ строчные, %f приходит ВЕРХНИМ).
        var target = dest;
        if (File.Exists(target))
            return 0; // файл уже лежит — переполучение восстановления

        var prefix = context.Prefix.Trim('/');
        var slash = prefix.IndexOf('/');
        var cluster = slash > 0 ? prefix[..slash] : prefix;
        var shard = slash > 0 ? prefix[(slash + 1)..] : string.Empty;

        // CA https (PEM из env): материализация в temp — один раз на вызов.
        string? caBundlePath = null;
        if (context.CaPem is { Length: > 0 } caPem)
        {
            caBundlePath = Path.Combine(Path.GetTempPath(), $"s3-ca-{Guid.NewGuid():N}.pem");
            await File.WriteAllTextAsync(caBundlePath, caPem + "\n");
        }

        try
        {
            foreach (var endpoint in context.Endpoints)
            {
                await using var s3 = new BackupS3(
                    new S3ClientOptions(endpoint, context.Bucket, context.AccessKey,
                        context.SecretKey, Region: null, CaBundlePath: caBundlePath),
                    WalReceiverCore.PutAttemptTimeout);
                var got = await s3.GetObjectToFileAsync(
                    cluster, shard, $"wal/{NormalizeSegment(segment)}", target);
                if (got.IsSuccess)
                    return 0;
            }

            return 1; // объекта нет ни на одной ноде — конец WAL
        }
        catch (Exception e)
        {
            Console.Error.WriteLine($"fetch-wal: {e.Message.Split('\n')[0]}");
            return 1;
        }
        finally
        {
            if (caBundlePath is not null)
                try { File.Delete(caBundlePath); } catch { /* уже нет */ }
        }
    }
}
