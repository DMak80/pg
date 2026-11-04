using PgWorker.Backups;
using PgWorker.Etcd.S3;
using Shared.Core;

namespace PgWorker.PgNode.Operations;

/// <summary>S3-контекст restore_command (env процесса postgres — секреты не в
/// argv, Р13; выставляет RestoreOperation при старте postgres — наследуется до
/// restore_command).</summary>
internal sealed record FetchWalContext(
    string[] Endpoints, string Bucket, string Prefix,
    string AccessKey, string SecretKey, string? CaPem);

/// <summary>Sub-режим того же бинаря `walreceiver fetch-wal &lt;seg&gt; &lt;dest&gt;`
/// (arch/19 §3.5, spec §3.4): restore_command postgres — скачивание
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

    /// <summary>Чистая функция для тестов: лучший (длиннейший по EndLsn)
    /// .partial-ключ из списка имён; null — версий нет.</summary>
    internal static string? BestPartialKey(IEnumerable<string> names)
    {
        string? best = null;
        ulong bestLsn = 0;
        foreach (var name in names)
            if (PgWorker.Backups.WalPartialName.TryParse(name) is { } v
                && v.EndLsn >= bestLsn)
            {
                bestLsn = v.EndLsn;
                best = name;
            }
        return best;
    }

    /// <summary>Лучший .partial по ВСЕМ нодам пула (лаг mesh допускается —
    /// arch/19 §5): перебор листингов узкого префикса; транспортный отказ ноды
    /// пропускает ноду. Возврат: (ключ, есть ли хоть один ответивший, первая
    /// транспортная ошибка) — никто не ответил ⇒ транспорт, не «конец WAL».</summary>
    private static async Task<(string? Key, bool AnyResponded, string? FirstError)> ListBestPartialAsync(
        IReadOnlyList<IBackupS3> clients, string cluster, string shard,
        string normalizedSegment, CancellationToken ct)
    {
        string? best = null;
        var anyResponded = false;
        string? firstError = null;
        foreach (var client in clients)
        {
            var listed = await client.ListAsync(
                cluster, shard, $"wal/{normalizedSegment}.", ct: ct);
            if (!listed.IsSuccess)
            {
                firstError ??= listed.Error!.Message.Split('\n')[0];
                continue;
            }
            anyResponded = true;
            if (BestPartialKey(listed.Value.Select(o => o.Name)) is not { } candidate)
                continue;
            if (best is null
                || PgWorker.Backups.WalPartialName.TryParse(candidate)!.Value.EndLsn
                   > PgWorker.Backups.WalPartialName.TryParse(best)!.Value.EndLsn)
                best = candidate;
        }
        return (best, anyResponded, firstError);
    }

    /// <summary>true — предпочесть локальную копию набора: архивный `.partial`
    /// не длиннее РЕАЛЬНОГО содержимого копии (её последний ненулевой байт —
    /// верхняя граница конца: копия набора у бэкапа со standby разрежена и
    /// добита), false — отдать `.partial` (он длиннее), null — копии нет/пуста.
    /// postgres при restore_command сначала идёт в архив; exit 1 заставляет его
    /// прочитать layout самому — и остановиться там, где кончается копия набора
    /// (сканировать разреженный файл эвристиками нельзя).</summary>
    internal static bool? LocalSetCoversPartial(
        string normalizedSegment, string? bestPartialKey, string? pgDataRoot = null)
    {
        var root = pgDataRoot ?? PgDataRoot;
        var local = Path.Combine(root, "pg_wal", normalizedSegment);
        if (!File.Exists(local) || new FileInfo(local).Length != WalFileName.SegmentBytes)
            return null;
        if (WalFileName.TryParse(normalizedSegment) is not { } seg)
            return null;

        // Последний ненулевой байт копии (верхняя граница её содержимого).
        long lastNonZero = -1;
        using (var stream = new FileStream(local, FileMode.Open, FileAccess.Read, FileShare.Read))
        {
            var buffer = new byte[64 * 1024];
            long offset = 0;
            int read;
            while ((read = stream.Read(buffer, 0, buffer.Length)) > 0)
            {
                for (var i = read - 1; i >= 0; i--)
                    if (buffer[i] != 0)
                    {
                        lastNonZero = offset + i;
                        break;
                    }
                offset += read;
            }
        }
        if (lastNonZero < 0)
            return null; // пустая копия — не опора

        var segmentStart = ((ulong)seg.Log * WalFileName.SegsPerLog + seg.Seg)
            * (ulong)WalFileName.SegmentBytes;
        var setEnd = (ulong)lastNonZero + 1;
        var partialAbsEnd = bestPartialKey is { } key
            ? WalPartialName.TryParse(key)?.EndLsn ?? 0
            : 0;
        var partialEnd = partialAbsEnd > segmentStart ? partialAbsEnd - segmentStart : 0;
        return setEnd >= partialEnd;
    }

    /// <summary>Дописать валидный префикс `.partial` нулями до полного
    /// `wal_segment_size` и положить в запрошенный путь (arch/19 §2/§3.5):
    /// postgres отвергает архивный файл размера ≠ `wal_segment_size`
    /// («archive file … has wrong size»); нули за концом последней валидной
    /// записи — штатный конец WAL. false — префикс пуст/больше сегмента/ошибка
    /// ФС.</summary>
    internal static bool TryWritePaddedSegment(string sourcePath, string targetPath)
    {
        try
        {
            var prefix = File.ReadAllBytes(sourcePath);
            var segmentBytes = (int)WalFileName.SegmentBytes;
            if (prefix.Length == 0 || prefix.Length > segmentBytes)
                return false;
            using var stream = new FileStream(
                targetPath, FileMode.Create, FileAccess.Write, FileShare.None);
            stream.Write(prefix);
            var zeros = new byte[64 * 1024];
            var remaining = segmentBytes - prefix.Length;
            while (remaining > 0)
            {
                var chunk = Math.Min(remaining, zeros.Length);
                stream.Write(zeros, 0, chunk);
                remaining -= chunk;
            }
            stream.Flush(true);
            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>Чистая функция для тестов: ОДИН проход по клиентам пула (перебор
    /// read-списка до найденного); transportError — первая транспортная ошибка
    /// прохода (null — «сегмента нет нигде», все ответили not found). Финала нет
    /// нигде — лучший .partial версии (валидный префикс живого сегмента).</summary>
    internal static async Task<(int ExitCode, string? TransportError)> FetchOnceAsync(
        IReadOnlyList<IBackupS3> clients, string cluster, string shard,
        string segment, string target, CancellationToken ct)
    {
        string? transportError = null;
        var normalized = NormalizeSegment(segment);
        foreach (var client in clients)
        {
            var got = await client.GetObjectToFileAsync(
                cluster, shard, $"wal/{normalized}", target, ct);
            if (got.IsSuccess)
                return (0, null);
            if (!IsNotFound(got))
                transportError ??= got.Error!.Message.Split('\n')[0];
        }

        // Финала нет нигде — лучший .partial (хвост потока, arch/19 §3.5):
        // префикс валиден по построению, но postgres принимает архивный файл
        // только полного размера — добивается нулями; ничего нет нигде ⇒
        // конец WAL (exit 1).
        var (bestKey, anyResponded, listError) = await ListBestPartialAsync(
            clients, cluster, shard, normalized, ct);
        if (!anyResponded)
            return (1, listError ?? "wal partial listing failed on all nodes");

        // Локальная копия набора полного (`full/<id>/pg_wal/` скачана в PGDATA):
        // restore_command у postgres ПРИОРИТЕТНЕЕ pg_wal — архивный .partial
        // отдаётся, только если он ДЛИННЕЕ копии (верхняя граница содержимого
        // копии — её последний ненулевой байт: у бэкапа со standby копия
        // разрежена и добита — эвристиками префикса её не режем). Копия не
        // короче — exit 1: postgres прочитает layout сам (остановится на его
        // реальном конце), битого хвоста из архива не возьмёт.
        if (LocalSetCoversPartial(normalized, bestKey) is { } preferLayout
            && preferLayout)
            return (1, null);

        if (bestKey is { } partial)
        {
            var temp = target + ".partial";
            try
            {
                foreach (var client in clients)
                {
                    var got = await client.GetObjectToFileAsync(
                        cluster, shard, $"wal/{partial}", temp, ct);
                    if (got.IsSuccess)
                    {
                        if (!TryWritePaddedSegment(temp, target))
                        {
                            transportError ??= $"wal partial {partial}: pad to segment size failed";
                            break;
                        }
                        return (0, null);
                    }
                    if (!IsNotFound(got))
                        transportError ??= got.Error!.Message.Split('\n')[0];
                    // Недокачанный/отсутствующий объект на этой ноде — следующая.
                    try
                    {
                        if (File.Exists(temp))
                            File.Delete(temp);
                    }
                    catch
                    {
                        // Не удалось — следующая попытка перезапишет.
                    }
                }
            }
            finally
            {
                try
                {
                    if (File.Exists(temp))
                        File.Delete(temp);
                }
                catch
                {
                    // Временный файл вне контракта вызова.
                }
            }
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
