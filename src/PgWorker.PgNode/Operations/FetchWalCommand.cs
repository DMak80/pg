using System.Security.Cryptography;
using PgWorker.Backups;
using PgWorker.Etcd.S3;
using Shared.Core;

namespace PgWorker.PgNode.Operations;

/// <summary>S3-контекст restore_command (env процесса postgres — секреты не в
/// argv, Р13; выставляет RestoreOperation при старте postgres — наследуется до
/// restore_command).</summary>
internal sealed record FetchWalContext(
    string[] Endpoints, string Bucket, string Prefix,
    string AccessKey, string SecretKey, string? CaPem, uint? ChainLife = null);

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
        // t15: номер жизни цепочки выбранного полного (spec решение №34/№35);
        // мусор/отсутствие → null — DR-orphan-режим, фильтр по номеру не активен.
        var chainLife = uint.TryParse(env("PGW_WAL_CHAIN_LIFE"), out var life) ? life : (uint?)null;
        return new FetchWalContext(
            endpoints, bucket, prefix, accessKey, secretKey,
            string.IsNullOrWhiteSpace(env("PGW_S3_CA_PEM")) ? null : env("PGW_S3_CA_PEM"),
            chainLife);
    }

    /// <summary>Чистая функция для тестов: продолжать ли транспортные попытки —
    /// лимит попыток НЕ исчерпан И жёсткий дедлайн вызова НЕ наступил
    /// (arch/19 §3.5: вызов в любом исходе — секунды, не минуты).</summary>
    internal static bool ShouldRetryTransport(int attempt, long startTick, long nowTick)
        => attempt < TransportAttempts
           && TimeSpan.FromTicks(nowTick - startTick) < CallDeadline;

    // Параллелизм скачивания кусков (spec решение №23; НЕ таймаут — бюджет
    // CallDeadline не меняется).
    internal static readonly int DeltaDownloadParallelism = 8;

    /// <summary>Кандидат сборки покрытия (spec §4.3): кусок/безномерная t14-версия
    /// с границами в АБСОЛЮТНЫХ LSN (spec решение №2). Md5Hex null — безномерная
    /// t14-версия: сверка по ETag, диапазон [начало сегмента..EndLsn имени).</summary>
    internal sealed record AssemblyCandidate(
        string Name, ulong Start, ulong End, bool IsMaster, string? Md5Hex);

    /// <summary>Чистая функция (spec §4.3 п.2): фильтры инвентаря ДО скачивания —
    /// разбор имени, фильтр номера жизни (чужие не рассматриваются вовсе),
    /// безномерные t14-версии только пока у сегмента нет кусков текущего номера
    /// (решение №39), сверка end−start с размером объекта, диапазон внутри сегмента.
    /// Неразбор/несовпадение — отброс (диагностику печатает вызывающий).</summary>
    internal static IReadOnlyList<AssemblyCandidate> FilterDeltaInventory(
        IReadOnlyList<(string Name, long Size)> objects, uint? chainLife, WalFileName segment)
    {
        var segmentStart = WalDeltaName.SegmentStartOf(segment);
        var segmentEnd = segmentStart + (ulong)WalFileName.SegmentBytes;

        // Есть ли у сегмента кусок ТЕКУЩЕГО номера жизни: безномерные legacy
        // учитываются только пока таких кусков нет (переходное окно t14→t15).
        var hasCurrentPiece = chainLife is { } current
            && objects.Any(o => WalDeltaName.TryParse(o.Name) is { } p && p.ChainLife == current);

        var result = new List<AssemblyCandidate>();
        foreach (var (name, size) in objects)
        {
            if (WalDeltaName.TryParse(name) is { } piece)
            {
                if (chainLife is { } life && piece.ChainLife != life)
                    continue; // чужой номер жизни — невидим вовсе
                if (piece.End - piece.Start != (ulong)size)
                    continue; // битый: end−start ≠ размер из листинга
                if (piece.Start < segmentStart || piece.End > segmentEnd)
                    continue; // вне диапазона сегмента
                result.Add(new AssemblyCandidate(
                    name, piece.Start, piece.End, piece.IsMaster, piece.Md5Hex));
                continue;
            }

            if (WalPartialName.TryParse(name) is { } legacy)
            {
                if (chainLife is not null && hasCurrentPiece)
                    continue; // класс «чужих» после появления кусков текущего номера
                if (legacy.EndLsn <= segmentStart
                    || legacy.EndLsn - segmentStart != (ulong)size
                    || legacy.EndLsn > segmentEnd)
                    continue; // пустая/битая по размеру/вне сегмента версия
                result.Add(new AssemblyCandidate(name, segmentStart, legacy.EndLsn, false, null));
            }
            // прочее (финал, history, мусор) — не кандидат
        }
        return result;
    }

    /// <summary>Чистая функция (spec §4.3 п.4): порядок кандидатов — m над r,
    /// затем более ранний start (база идёт первой в своей группе).</summary>
    internal static IReadOnlyList<AssemblyCandidate> OrderCandidates(
        IReadOnlyList<AssemblyCandidate> candidates)
        => candidates
            .OrderByDescending(c => c.IsMaster)
            .ThenBy(c => c.Start)
            .ThenBy(c => c.Name, StringComparer.Ordinal)
            .ToList();

    /// <summary>Результат сборки: покрытие (конец собранного префикса, абсолютный
    /// LSN), факт конфликта байтов, граница дыры (минимальный start куска выше
    /// покрытия; null — выше покрытия кусков нет) и TrustedEnd — конец последней
    /// применённой КУСКА-БАЗЫ (start == начало сегмента, включая t14-версии; при
    /// отсутствии — начало сегмента). Область [0..TrustedEnd) ДОВЕРЕНА писателю
    /// (выкладывается только валидный префикс по границам записей) и транспорту
    /// (Content-MD5/ETag + md5 в имени); скан-валидации (CRC/границы записей)
    /// подлежит только [TrustedEnd..Coverage) — область дифов
    /// (arch/pgnode/processes/g-wal-delta-assembly.md §5).</summary>
    internal sealed record AssemblyResult(
        ulong Coverage, bool Conflict, ulong? HoleStart, ulong TrustedEnd = 0);

    /// <summary>Штатная сборка (spec §4.3 п.6): жадное применение от начала
    /// сегмента; пересечение с собранным буфером сверяется побайтово, совпало —
    /// union; несовпадение — конфликт, сборка останавливается; кусок ниже
    /// покрытия пропускается молча; кусок выше покрытия — кандидат на дыру.</summary>
    internal static AssemblyResult GreedyAssemble(
        IReadOnlyList<AssemblyCandidate> candidates, IReadOnlyDictionary<string, byte[]> bytes)
        => GreedyAssembleCore(candidates, bytes).Result;

    // Внутренняя форма: результат + собранный буфер (нужен вызывающему для записи).
    internal static (AssemblyResult Result, byte[] Buffer) GreedyAssembleCore(
        IReadOnlyList<AssemblyCandidate> candidates, IReadOnlyDictionary<string, byte[]> bytes)
    {
        if (candidates.Count == 0)
            return (new AssemblyResult(0, false, null), []);
        var segmentStart = SegmentStartOfCandidate(candidates[0]);
        var buffer = new byte[WalFileName.SegmentBytes];
        var coverage = segmentStart;
        var trustEnd = segmentStart; // конец последней применённой базы (доверена)
        var conflict = false;
        // Жадка ИТЕРИРУЕТСЯ до фиксации покрытия (как итеративное покрытие
        // контроля CoverageOf): при порядке «роль → start» кусок с start >
        // coverage мог быть рассмотрен раньше куска с ранним start — повторный
        // проход замыкает его после расширения покрытия, а не теряет навсегда
        // (arch/pgnode/processes/g-wal-delta-assembly.md §2).
        bool applied;
        do
        {
            applied = false;
            foreach (var candidate in candidates)
            {
                if (candidate.End <= coverage)
                    continue; // уже покрыто — молча пропускаем
                if (candidate.Start > coverage)
                    continue; // стык за покрытием — кандидат на дыру/повторный проход
                var data = bytes[candidate.Name];
                var startOffset = (int)(candidate.Start - segmentStart);
                var overlapLen = (int)(Math.Min(candidate.End, coverage) - candidate.Start);
                if (overlapLen > 0
                    && !buffer.AsSpan(startOffset, overlapLen).SequenceEqual(data.AsSpan(0, overlapLen)))
                {
                    conflict = true; // несовпадение байтов пересечения
                    break;           // сборка останавливается на покрытии до конфликта
                }

                var copyLsn = Math.Max(candidate.Start, coverage);
                var copyLen = (int)(candidate.End - copyLsn);
                data.AsSpan((int)(copyLsn - candidate.Start), copyLen)
                    .CopyTo(buffer.AsSpan((int)(copyLsn - segmentStart), copyLen));
                coverage = candidate.End;
                if (candidate.Start == segmentStart)
                    trustEnd = Math.Max(trustEnd, candidate.End); // база — доверена
                applied = true;
            }
        }
        while (applied && !conflict);

        var holeStart = HoleStartOf(candidates, coverage);
        return (new AssemblyResult(coverage, conflict, holeStart, trustEnd),
            buffer[..(int)(coverage - segmentStart)]);
    }

    /// <summary>Режим выживания (spec §4.3 п.7): сборка заново, кандидаты по
    /// возрастанию md5 (Ordinal; безномерные — по ETag, без ETag — в конце);
    /// каждый применённый кусок проверяется сканером (границы записей + CRC
    /// xl_crc) на всём покрытии до его конца — несовпадение пересечения или
    /// битые записи → кусок отбрасывается без ретрая.</summary>
    internal static AssemblyResult SurvivalAssemble(
        IReadOnlyList<AssemblyCandidate> candidates, IReadOnlyDictionary<string, byte[]> bytes,
        IReadOnlyDictionary<string, string?> etags)
        => SurvivalAssembleCore(candidates, bytes, etags).Result;

    internal static (AssemblyResult Result, byte[] Buffer) SurvivalAssembleCore(
        IReadOnlyList<AssemblyCandidate> candidates, IReadOnlyDictionary<string, byte[]> bytes,
        IReadOnlyDictionary<string, string?> etags)
    {
        if (candidates.Count == 0)
            return (new AssemblyResult(0, false, null), []);
        var segmentStart = SegmentStartOfCandidate(candidates[0]);
        var buffer = new byte[WalFileName.SegmentBytes];
        var coverage = segmentStart;
        var trustEnd = segmentStart; // конец последней применённой базы (доверена)

        // Порядок: возрастание md5-строки; безномерные — по ETag; без ETag — в конце.
        var ordered = candidates
            .Select(c => (Candidate: c,
                Key: c.Md5Hex ?? (etags.TryGetValue(c.Name, out var etag) ? etag : null)))
            .OrderBy(x => x.Key is null ? 1 : 0)
            .ThenBy(x => x.Key, StringComparer.Ordinal)
            .ThenBy(x => x.Candidate.Start)
            .Select(x => x.Candidate)
            .ToList();

        // Инкрементальный скан ОБЛАСТИ ДИФОВ: граница известна по построению
        // кусков (концы записей), поэтому скан начинается с неё, а не с начала
        // сегмента; доверенная база и уже проверенные дифы не пересчитываются.
        // Жадка итеративна (§2 канона).
        var validatedUpTo = 0; // offset; 0 — доверенной базы ещё нет
        bool applied;
        do
        {
            applied = false;
            foreach (var candidate in ordered)
            {
                if (candidate.End <= coverage || candidate.Start > coverage)
                    continue; // уже покрыт / за дырой — не применяется
                var data = bytes[candidate.Name];
                var startOffset = (int)(candidate.Start - segmentStart);
                var overlapLen = (int)(Math.Min(candidate.End, coverage) - candidate.Start);
                if (overlapLen > 0
                    && !buffer.AsSpan(startOffset, overlapLen).SequenceEqual(data.AsSpan(0, overlapLen)))
                    continue; // несовпадение пересечения — отброс без ретрая

                var copyLsn = Math.Max(candidate.Start, coverage);
                var copyLen = (int)(candidate.End - copyLsn);
                var copyOffset = (int)(copyLsn - segmentStart);
                var saved = buffer.AsSpan(copyOffset, copyLen).ToArray();
                data.AsSpan((int)(copyLsn - candidate.Start), copyLen)
                    .CopyTo(buffer.AsSpan(copyOffset, copyLen));

                // Скан-валидация — ТОЛЬКО для дифов (start > начала сегмента):
                // база-компак и t14-версия доверены писателю и транспорту
                // (arch/pgnode/processes/g-wal-delta-assembly.md §5). Диф
                // проверяется инкрементально от валидированной границы: всё
                // собранное ниже уже принято (пересечение куска сверено побайтово),
                // поэтому укорочение означает битые записи нового куска.
                var isDelta = candidate.Start > segmentStart;
                var target = (int)(candidate.End - segmentStart);
                if (isDelta
                    && WalPrefixScanner.ValidatedPrefixLength(buffer, target, validatedUpTo) < target)
                {
                    saved.CopyTo(buffer.AsSpan(copyOffset, copyLen)); // откат копии
                    continue; // записи битые — кусок отброшен
                }
                coverage = candidate.End;
                if (!isDelta)
                    trustEnd = Math.Max(trustEnd, candidate.End); // база — доверена
                validatedUpTo = target;
                applied = true;
            }
        }
        while (applied);

        var holeStart = HoleStartOf(ordered, coverage);
        return (new AssemblyResult(coverage, false, holeStart, trustEnd),
            buffer[..(int)(coverage - segmentStart)]);
    }

    // Начало сегмента кандидата (имя начинается с 24 hex-символов сегмента).
    private static ulong SegmentStartOfCandidate(AssemblyCandidate candidate)
        => WalFileName.TryParse(candidate.Name[..24]) is { } segment
            ? WalDeltaName.SegmentStartOf(segment)
            : 0;

    // Граница дыры: минимальный start куска выше итогового покрытия (null — нет).
    private static ulong? HoleStartOf(
        IReadOnlyList<AssemblyCandidate> candidates, ulong coverage)
    {
        ulong? hole = null;
        foreach (var candidate in candidates)
            if (candidate.Start > coverage && (hole is null || candidate.Start < hole))
                hole = candidate.Start;
        return hole;
    }

    /// <summary>true — предпочесть локальную копию набора: собранное Покрытие
    /// кусков не длиннее РЕАЛЬНОГО содержимого копии (её последний ненулевой
    /// байт — верхняя граница конца: копия набора у бэкапа со standby разрежена
    /// и добита), false — отдать покрытие (оно длиннее), null — копии нет/пуста.
    /// postgres при restore_command сначала идёт в архив; exit 1 заставляет его
    /// прочитать layout самому — и остановиться там, где кончается копия набора
    /// (сканировать разреженный файл эвристиками нельзя).</summary>
    internal static bool? LocalSetCoversPartial(
        string segment, ulong coverage, string? pgDataRoot = null)
    {
        var root = pgDataRoot ?? PgDataRoot;
        // PG/pg_basebackup пишут имена ВЕРХНИМ hex, а %f/S3-ключ нормализуются
        // в строчные — копию набора ищем без учёта регистра.
        var local = FindLocalWalFile(root, segment);
        if (local is null || new FileInfo(local).Length != WalFileName.SegmentBytes)
            return null;
        if (WalFileName.TryParse(segment) is not { } seg)
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

        var segmentStart = WalDeltaName.SegmentStartOf(seg);
        var setEnd = (ulong)lastNonZero + 1; // конец содержимого копии (внутри сегмента)
        var coverageEnd = coverage > segmentStart ? coverage - segmentStart : 0;
        return setEnd >= coverageEnd;
    }

    // Локальная копия сегмента в pg_wal набора: точное имя, иначе — без учёта
    // регистра (ВЕРХНИЙ hex у PG/pg_basebackup vs строчный %f/S3-ключ).
    private static string? FindLocalWalFile(string root, string segment)
    {
        var walDir = Path.Combine(root, "pg_wal");
        var exact = Path.Combine(walDir, segment);
        if (File.Exists(exact))
            return exact;
        try
        {
            return Directory.EnumerateFiles(walDir)
                .FirstOrDefault(p => string.Equals(Path.GetFileName(p), segment,
                    StringComparison.OrdinalIgnoreCase));
        }
        catch
        {
            return null; // каталога нет — локальной копии нет
        }
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

    /// <summary>ОДИН проход по клиентам пула (spec §4.3): финал `wal/<seg>` —
    /// первым (перебор read-списка до найденного); финала нет нигде — сборка
    /// покрытия из дельта-кусков по процессу g-wal-delta-assembly. transportError —
    /// первая транспортная ошибка прохода (null — «конца WAL достигли», все
    /// ответили not found).</summary>
    internal static async Task<(int ExitCode, string? TransportError)> FetchOnceAsync(
        IReadOnlyList<IBackupS3> clients, string cluster, string shard,
        string segment, string target, uint? chainLife, CancellationToken ct)
    {
        string? transportError = null;
        var normalized = NormalizeSegment(segment);
        // Мини-breaker нод на ОДИН вызов (spec §4.3, решение №23): нода,
        // ответившая транспортным отказом (НЕ «объекта нет»), исключается из
        // перебора последующих КУСКОВ этого прохода — «висящая» нода иначе стоит
        // свой пер-запросный таймаут на КАЖДОМ из k кусков (k×15 с при пуле 8).
        // Попытка внешнего цикла начинает перебор заново (свежий вызов).
        var failedNodes = new bool[clients.Count];
        for (var i = 0; i < clients.Count; i++)
        {
            var got = await clients[i].GetObjectToFileAsync(
                cluster, shard, $"wal/{normalized}", target, ct);
            if (got.IsSuccess)
                return (0, null);
            if (!IsNotFound(got))
            {
                transportError ??= got.Error!.Message.Split('\n')[0];
                failedNodes[i] = true; // сид breaker'а для фазы кусков
            }
        }

        if (WalFileName.TryParse(normalized) is not { } segmentName)
            return (1, transportError); // мусорное имя — не сегмент WAL

        // (1) Инвентарь узкого префикса ПО ВСЕМ нодам (union, дедуп по имени;
        // размер объекта — из листинга, GET для фильтра не нужен). Ни одна нода
        // не ответила — транспортный отказ вызова, НЕ «конец WAL».
        var inventory = new SortedDictionary<string, long>(StringComparer.Ordinal);
        var anyResponded = false;
        string? listError = null;
        foreach (var client in clients)
        {
            var listed = await client.ListPrefixAsync(
                $"{cluster}/{shard}/wal/{normalized}.", ct: ct);
            if (!listed.IsSuccess)
            {
                listError ??= listed.Error!.Message.Split('\n')[0];
                continue;
            }
            anyResponded = true;
            foreach (var obj in listed.Value)
                inventory[obj.Key[(obj.Key.LastIndexOf('/') + 1)..]] = obj.SizeBytes;
        }
        if (!anyResponded)
            return (1, listError ?? "wal delta listing failed on all nodes");

        // (2) Фильтры ДО скачивания: жизнь, размер (= end−start), диапазон сегмента.
        var filtered = FilterDeltaInventory(
            inventory.Select(kv => (kv.Key, kv.Value)).ToList(), chainLife, segmentName);
        if (filtered.Count == 0)
            return (1, null); // (3) конец WAL: ни финала, ни кусков

        // (4) Порядок кандидатов: m над r → start asc.
        var ordered = OrderCandidates(filtered);

        // (5) Скачивание параллельно (пул 8); сверка md5 байтов с именем куска
        // (безномерная legacy — с ETag одиночного put = md5 содержимого).
        // Перебор нод на кусок — с breaker'ом этого вызова (failedNodes):
        // нода, сдавшая транспортный отказ (не 404) на любом объекте вызова,
        // последующими кусками не перебирается.
        var bytes = new System.Collections.Concurrent.ConcurrentDictionary<string, byte[]>(
            StringComparer.Ordinal);
        var etags = new System.Collections.Concurrent.ConcurrentDictionary<string, string?>(
            StringComparer.Ordinal);
        string? downloadError = null;
        using (var gate = new SemaphoreSlim(DeltaDownloadParallelism))
        {
            var downloads = ordered.Select(async candidate =>
            {
                await gate.WaitAsync(ct);
                try
                {
                    for (var i = 0; i < clients.Count; i++)
                    {
                        if (Volatile.Read(ref failedNodes[i]))
                            continue; // breaker: нода сдала транспортный отказ
                        var got = await clients[i].GetObjectBytesAsync(
                            cluster, shard, $"wal/{candidate.Name}", ct);
                        if (!got.IsSuccess)
                        {
                            if (!IsNotFound(got))
                            {
                                downloadError ??= got.Error!.Message.Split('\n')[0];
                                Volatile.Write(ref failedNodes[i], true); // последующие куски — мимо
                            }
                            continue; // ноды нет объекта — следующая
                        }
                        var (data, etag) = got.Value;
                        etags[candidate.Name] = etag;
                        var actual = Convert.ToHexStringLower(MD5.HashData(data));
                        var expected = candidate.Md5Hex ?? etag;
                        if (expected is null
                            || !string.Equals(actual, expected, StringComparison.OrdinalIgnoreCase))
                        {
                            await Console.Error.WriteLineAsync(
                                $"fetch-wal: delta {candidate.Name}: md5 mismatch");
                            return; // порча при хранении — отброс + факт
                        }
                        bytes[candidate.Name] = data;
                        return;
                    }
                }
                finally
                {
                    gate.Release();
                }
            }).ToList();
            await Task.WhenAll(downloads);
        }

        var downloaded = ordered.Where(c => bytes.ContainsKey(c.Name)).ToList();
        if (downloaded.Count == 0)
            return (1, downloadError ?? transportError); // ни один кусок не скачан

        // (6) Штатная сборка; несовпадение байтов пересечения — факт + выживание.
        var segmentStart = WalDeltaName.SegmentStartOf(segmentName);
        var (greedyResult, greedyBuffer) = GreedyAssembleCore(downloaded, bytes);
        var assembly = greedyResult;
        var coverage = greedyResult.Coverage;
        var holeStart = greedyResult.HoleStart;
        var assembled = greedyBuffer;
        if (greedyResult.Conflict)
        {
            await Console.Error.WriteLineAsync(
                $"fetch-wal: delta conflict at {coverage:x}, survival mode");
            var (survivalResult, survivalBuffer) = SurvivalAssembleCore(downloaded, bytes, etags);
            assembly = survivalResult;
            coverage = survivalResult.Coverage;
            holeStart = survivalResult.HoleStart;
            assembled = survivalBuffer;
        }

        // (8) Дыра — фолбэк последней надежды: закрытое покрытие до её начала.
        if (holeStart is { } hole)
            await Console.Error.WriteLineAsync($"fetch-wal: delta hole at {hole:x}");

        if (coverage <= segmentStart)
            return (1, transportError ?? downloadError); // покрытия нет — «конца WAL»

        // (10) Локальная копия набора: не короче покрытия — postgres читает layout сам.
        if (LocalSetCoversPartial(segment, coverage) is { } preferLayout && preferLayout)
            return (1, null);

        // (9) Финальная скан-валидация ОБЛАСТИ ДИФОВ (arch/pgnode/processes/
        // g-wal-delta-assembly.md §5): базы-компаки и t14-версии доверены
        // писателю и транспорту — скан начинается с конца последней применённой
        // базы; дифов нет (coverage == TrustedEnd) — валидации нет вовсе,
        // усечение невозможно. CRC-аномалия диф-области — усечение + факт.
        var trustedOffset = (int)Math.Max(0L, (long)assembly.TrustedEnd - (long)segmentStart);
        var validated = assembled.Length <= trustedOffset
            ? assembled.Length
            : WalPrefixScanner.ValidatedPrefixLength(
                assembled, assembled.Length, validatedUpTo: trustedOffset);
        if (validated < assembled.Length)
            await Console.Error.WriteLineAsync(
                $"fetch-wal: delta {normalized}: diff region truncated {assembled.Length} → {validated} (crc)");

        // (9) Запись валидного префикса и добивка нулями до wal_segment_size.
        var temp = target + ".delta";
        try
        {
            await File.WriteAllBytesAsync(temp, assembled.AsMemory(0, validated), ct);
            if (!TryWritePaddedSegment(temp, target))
                return (1, transportError ?? $"wal delta {normalized}: pad to segment size failed");
            return (0, null);
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
                // Остаток жёсткого дедлайна вызова — ВНУТРЬ прохода (linked-CTS):
                // листинги/GET/скан прерываются по нему, а не только между
                // попытками (spec §4.3, решение №23: k кусков × пер-запросный
                // таймаут иначе не ограничены). Исчерпание — транспортный исход
                // попытки (НЕ «конец WAL»): ретрай решает внешний цикл.
                // Бюджет CallDeadline НЕ меняется — он лишь распространяется внутрь.
                var remaining = CallDeadline - TimeSpan.FromMilliseconds(
                    Environment.TickCount64 - startTick);
                using var attemptCts = new CancellationTokenSource(
                    remaining > TimeSpan.Zero ? remaining : TimeSpan.FromMilliseconds(1));
                try
                {
                    int exitCode;
                    string? transportError;
                    try
                    {
                        (exitCode, transportError) = await FetchOnceAsync(
                            clients, cluster, shard, segment, target, context.ChainLife,
                            attemptCts.Token);
                    }
                    catch (OperationCanceledException) when (attemptCts.IsCancellationRequested)
                    {
                        // Дедлайн вызова исчерпан внутри прохода — транспортный
                        // исход попытки; внешний цикл ниже решит, ретраить ли.
                        exitCode = 1;
                        transportError = "call deadline exceeded";
                    }

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
