using PgWorker.Backups;
using PgWorker.Etcd.S3;
using Shared.Core;

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
/// даёт ложный 404 и обрывает recovery до цели. Транспортный сбой (НЕ «сегмента
/// нет») — ограниченный внутренний ретрай 3×2 с перед exit 1: restore_command
/// зовётся postgres'ом на каждый сегмент, разовый сетевой флап иначе читается
/// как конец WAL → преждевременный promote. Вызов в ЛЮБОМ исходе укладывается в
/// жёсткий дедлайн (CallDeadline): внутренние ретраи S3-клиента выключены
/// (MaxErrorRetry=0 — ретраит сам fetch-wal), иначе клиентские ретраи ×
/// пер-запросный таймаут × перебор мёртвой ноды растягивают один вызов на
/// минуты, целевое восстановление не доходит до «исчерпание архива → promote»
/// в бюджете операции. Длительные моргания сети покрывают ПОВТОРНЫЕ вызовы
/// restore_command самим postgres'ом. Файл уже лежит по dest (переполучение
/// восстановления) → exit 0 без хода в S3.</summary>
internal static class FetchWalCommand
{
    // Бюджет внутреннего ретрая транспортных сбоев: 3 попытки с паузой 2 с —
    // перекрывает секундные флапы, не растягивает restore_command (postgres
    // ждёт завершения restore_command на каждый сегмент).
    internal static readonly int TransportAttempts = 3;
    private static readonly TimeSpan TransportRetryDelay = TimeSpan.FromSeconds(2);

    // Жёсткий дедлайн ВСЕГО вызова (arch/19 §3.5): после него — stderr-факт и
    // exit 1 (для postgres — конец WAL; повторный вызов restore_command продолжит
    // добор). Проверяется МЕЖДУ попытками: текущая доигрывается в своём
    // пер-запросном таймауте, попытки сверх дедлайна не начинаются.
    internal static readonly TimeSpan CallDeadline = TimeSpan.FromSeconds(45);

    // Корень данных ноды (arch/19 §3.5, Spilo-layout): data-volume монтируется
    // сюда, PGDATA — data/ под ним; postgres зовёт restore_command с cwd = PGDATA.
    internal const string PgDataRoot = "/home/postgres/pgdata/pgroot/data";

    /// <summary>Чистая функция для тестов: dest обязан оставаться в границах
    /// корня данных (CWE-22). %p postgres — путь внутри PGDATA: относительный
    /// (cwd = PGDATA) — нормализуется против корня; абсолютный — обязан лежать
    /// под корнем. Возвращает нормализованный полный путь (для проверки);
    /// выход за корень — ArgumentException.</summary>
    internal static string EnsureDestUnderRoot(string dest, string root)
    {
        var full = Path.IsPathRooted(dest)
            ? Path.GetFullPath(dest)
            : Path.GetFullPath(Path.Combine(root, dest));
        var normalizedRoot = Path.GetFullPath(root);
        if (full != normalizedRoot
            && !full.StartsWith(normalizedRoot + Path.DirectorySeparatorChar, StringComparison.Ordinal))
            throw new ArgumentException($"dest outside PGDATA root: {full}");
        return full;
    }

    /// <summary>Чистая функция для тестов: нормализация регистра сегмента.</summary>
    internal static string NormalizeSegment(string segment)
        => segment.ToLowerInvariant();

    /// <summary>Чистая функция для тестов: «объекта нет» (not found) — конец WAL;
    /// прочие ошибки — транспорт (ретрай).</summary>
    internal static bool IsNotFound(Result error)
        => error.Error?.Message.EndsWith(": not found", StringComparison.Ordinal) == true;

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

    /// <summary>Чистая функция для тестов: продолжать ли транспортные попытки —
    /// лимит попыток НЕ исчерпан И жёсткий дедлайн вызова НЕ наступил
    /// (arch/19 §3.5: вызов в любом исходе — секунды, не минуты).</summary>
    internal static bool ShouldRetryTransport(int attempt, long startTick, long nowTick)
        => attempt < TransportAttempts
           && TimeSpan.FromTicks(nowTick - startTick) < CallDeadline;

    /// <summary>Чистая функция для тестов: ОДИН проход по клиентам пула (перебор
    /// read-списка до найденного); transportError — первая транспортная ошибка
    /// прохода (null — «сегмента нет нигде», все ответили not found).</summary>
    internal static async Task<(int ExitCode, string? TransportError)> FetchOnceAsync(
        IReadOnlyList<IBackupS3> clients, string cluster, string shard,
        string segment, string target, CancellationToken ct)
    {
        string? transportError = null;
        foreach (var client in clients)
        {
            var got = await client.GetObjectToFileAsync(
                cluster, shard, $"wal/{NormalizeSegment(segment)}", target, ct);
            if (got.IsSuccess)
                return (0, null);
            if (!IsNotFound(got))
                transportError ??= got.Error!.Message.Split('\n')[0];
        }
        return (1, transportError);
    }

    /// <summary>Точка входа sub-режима (Program.Main ДО builder): код 0 — объект
    /// записан в dest (или уже был там); 1 — объекта нет нигде/исчерпан ретрай/
    /// невалидный env (для postgres — конец WAL или сбой recovery).</summary>
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
        // Граница корня данных (CWE-22): выход за PGDATA — отказ (exit 1).
        try
        {
            EnsureDestUnderRoot(dest, PgDataRoot);
        }
        catch (ArgumentException e)
        {
            Console.Error.WriteLine($"fetch-wal: {e.Message}");
            return 1;
        }
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
            // Ограниченный внутренний ретрай транспортных сбоев (не «сегмента
            // нет» — тот сразу exit 1) ПОД жёстким дедлайном вызова: клиенты
            // пересоздаются на попытку (истёкший/битый коннект не переиспользуется),
            // клиентские ретраи выключены (MaxErrorRetry=0) — одна транспортная
            // попытка ограничена пер-запросным таймаутом клиента на ноду.
            var startTick = Environment.TickCount64;
            for (var attempt = 1; ; attempt++)
            {
                var clients = context.Endpoints.Select(endpoint => (IBackupS3)new BackupS3(
                    new S3ClientOptions(endpoint, context.Bucket, context.AccessKey,
                        context.SecretKey, Region: null, CaBundlePath: caBundlePath,
                        MaxErrorRetry: 0),
                    WalReceiverCore.PutAttemptTimeout)).ToList();
                try
                {
                    var (exitCode, transportError) = await FetchOnceAsync(
                        clients, cluster, shard, segment, target, CancellationToken.None);
                    if (exitCode == 0)
                        return 0;
                    if (transportError is null)
                        return 1; // нигде нет — конец WAL (все endpoints not found)
                    if (!ShouldRetryTransport(attempt, startTick, Environment.TickCount64))
                    {
                        Console.Error.WriteLine($"fetch-wal: {transportError}");
                        return 1;
                    }
                }
                finally
                {
                    foreach (var client in clients)
                        if (client is IAsyncDisposable disposable)
                            await disposable.DisposeAsync();
                }
                await Task.Delay(TransportRetryDelay);
            }
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
