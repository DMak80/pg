using System.Globalization;

namespace PgWorker.Core.Tuning;

// Ядро расчёта параметров PostgreSQL по алгоритму PGTune — чистая функция 1:1
// docs/pgtune-calculation-spec.md (входы §2, формулы §4, формат §5, псевдокод §6,
// контрольные примеры §7 — тест-векторы PgTuneTests один-в-один).
//
// Каналы ядра (spec.md §2 принцип 1/2):
// - без I/O, без NuGet — только BCL;
// - внутренняя единица памяти — килобайт (KB), 1 KB = 1024 байта, все единицы
//   двоичные (1 MB = 1024 KB, 1 GB = 1024 MB, 1 TB = 1024 GB);
// - все деления объёмов — целочисленные с floor (кроме work_mem: вещественное
//   деление, округления после остальных арифметических шагов — §1 п.4);
// - значения вывода — строки в инвариантной культуре;
// - параметр, не вычисленный для данного входа, в вывод не включается (§1 п.5);
//   max_wal_senders = 0 (desktop) — вычисленный, не «отсутствующий» (§8 п.3).
//
// Решение по границе KB-канала (spec.md §4.3, зафиксировано в плане задачи 1):
// §2 спецификации алгоритма ограничивает totalMemory ≤ 999999 в человеко-единицах
// инструмента (MB/GB/TB — ограничение входа PGTune-калькулятора). KB в
// PgTuneMemoryUnit — внутренний канал фабрики (TotalMemoryKb = floor(bytes/1024),
// дефолт 8 GiB = 8 388 608 KB > 999 999), а spec.md §6 требует, чтобы память
// > 100 GB давала только предупреждение и не блокировала расчёт. Поэтому для
// unit = KB применяется только нижняя граница ≥ 1 (иначе расчёт невозможен),
// верхний числовой кап §2 НЕ применяется; для MB — ≥ 512, для GB/TB — ≥ 1,
// верхний кап ≤ 999999 — для всех трёх человеко-единиц.

/// <summary>Операционная система хоста PostgreSQL.</summary>
public enum PgTuneOsType { Linux, Windows, Mac }

/// <summary>Тип рабочей нагрузки (desktop отвергается валидацией старта PgWorker — P3).</summary>
public enum PgTuneDbType { Web, Oltp, Dw, Desktop, Mixed }

/// <summary>Тип дисковой подсистемы.</summary>
public enum PgTuneHdType { Ssd, San, Hdd, Nvme }

/// <summary>Ожидаемый размер базы относительно RAM.</summary>
public enum PgTuneDbSize { LessRam, MidRam, GreaterRam }

/// <summary>Единица входной памяти. KB — внутренний канал фабрики (см. шапку файла).</summary>
public enum PgTuneMemoryUnit { KB, MB, GB, TB }

/// <summary>Вход алгоритма PGTune (9 явных входов — spec.md §2 принцип 2).</summary>
public sealed record PgTuneInput(
    int DbVersion,
    PgTuneOsType OsType,
    PgTuneDbType DbType,
    long TotalMemory,
    PgTuneMemoryUnit TotalMemoryUnit,
    int? CpuNum,
    int? ConnectionNum,
    PgTuneHdType HdType,
    PgTuneDbSize DbSize);

/// <summary>Параметр вывода: значение — строка §5.1/§5.2 спецификации алгоритма.</summary>
public sealed record PgTuneParameter(string Name, string Value);

/// <summary>
/// Результат расчёта: параметры в порядке §5.2 (не вычисленные пропущены) и
/// предупреждения §4.21 (первый элемент "WARNING" при непустом списке).
/// </summary>
public sealed record PgTuneResult(IReadOnlyList<PgTuneParameter> Parameters, IReadOnlyList<string> Warnings)
{
    /// <summary>Значение параметра по имени; null — параметр не выведен для входа.</summary>
    public string? this[string name] =>
        Parameters.FirstOrDefault(p => string.Equals(p.Name, name, StringComparison.Ordinal))?.Value;
}

/// <summary>
/// Ядро PGTune: расчёт (§4/§6 спецификации алгоритма) и два режима вывода (§5.3).
/// Константы PgWorker (dbVersion/dbType/connections/...) на границе — в фабрике входов.
/// </summary>
public static class PgTune
{
    /// <summary>
    /// Все 25 имён вывода в порядке §5.2 спецификации алгоритма — реестр для
    /// валидации PgWorker:Pgtune:ExcludeParams (fail-fast старта).
    /// </summary>
    public static IReadOnlyCollection<string> KnownParameterNames { get; } =
    [
        "max_connections",
        "shared_buffers",
        "effective_cache_size",
        "maintenance_work_mem",
        "checkpoint_completion_target",
        "wal_buffers",
        "default_statistics_target",
        "random_page_cost",
        "effective_io_concurrency",
        "work_mem",
        "huge_pages",
        "jit",
        "wal_compression",
        "autovacuum_max_workers",
        "autovacuum_work_mem",
        "io_method",
        "io_workers",
        "min_wal_size",
        "max_wal_size",
        "max_worker_processes",
        "max_parallel_workers_per_gather",
        "max_parallel_workers",
        "max_parallel_maintenance_workers",
        "wal_level",
        "max_wal_senders",
    ];

    /// <summary>Расчёт параметров по формулам §4.1–§4.21 и псевдокоду §6.</summary>
    public static PgTuneResult Calculate(PgTuneInput input)
    {
        Validate(input);

        // Единицы в KB (двоичные): 1 MB = 1024 KB, 1 GB = 1024 MB (§1 п.1/п.2).
        const long MbKb = 1024;
        const long GbKb = 1024 * 1024;

        long ramKb = input.TotalMemory * UnitBytes(input.TotalMemoryUnit) / 1024;

        // 4.1 max_connections: задан → он; иначе карта по типу БД.
        var maxConn = input.ConnectionNum ?? input.DbType switch
        {
            PgTuneDbType.Web => 200,
            PgTuneDbType.Oltp => 300,
            PgTuneDbType.Dw => 40,
            PgTuneDbType.Desktop => 20,
            PgTuneDbType.Mixed => 100,
            _ => throw new ArgumentOutOfRangeException(nameof(input), input.DbType, "неизвестный тип БД"),
        };

        // 4.2 shared_buffers: floor(RAM/D), D = 16 только у desktop.
        var sb = ramKb / (input.DbType == PgTuneDbType.Desktop ? 16 : 4);
        if (input.DbVersion < 10 && input.OsType == PgTuneOsType.Windows && sb > 512 * MbKb)
            sb = 512 * MbKb; // ограничение Windows — недостижимо на поддерживаемых версиях (§4.2 примечание)

        // 4.3 huge_pages: mac → off; иначе try при SB ≥ 2GB.
        var hugePages = input.OsType == PgTuneOsType.Mac
            ? "off"
            : sb >= 2 * GbKb ? "try" : "off";

        // 4.4 effective_cache_size: floor(RAM×3/4); desktop — floor(RAM/4).
        var ecs = input.DbType == PgTuneDbType.Desktop ? ramKb / 4 : ramKb * 3 / 4;

        // 4.5 maintenance_work_mem: floor(RAM/D), D = 8 у dw; лимит 8GB
        // (Windows ≤ 17 — 2GB, ровно 2GB ломает PG ≤ 17 на Windows → −1MB).
        var mwm = ramKb / (input.DbType == PgTuneDbType.Dw ? 8 : 16);
        var mwmLimit = 8 * GbKb;
        if (input.OsType == PgTuneOsType.Windows && input.DbVersion <= 17)
            mwmLimit = 2 * GbKb;
        if (mwm >= mwmLimit)
            mwm = input.OsType == PgTuneOsType.Windows && input.DbVersion <= 17
                ? mwmLimit - MbKb
                : mwmLimit;

        // 4.6 min_wal_size/max_wal_size: константы по типу БД (KB).
        var (minWal, maxWal) = input.DbType switch
        {
            PgTuneDbType.Web => (1048576L, 4194304L),
            PgTuneDbType.Oltp => (2097152L, 8388608L),
            PgTuneDbType.Dw => (4194304L, 16777216L),
            PgTuneDbType.Desktop => (102400L, 2097152L),
            PgTuneDbType.Mixed => (1048576L, 4194304L),
            _ => throw new ArgumentOutOfRangeException(nameof(input), input.DbType, "неизвестный тип БД"),
        };

        // 4.8 wal_buffers: 3% от SB; кап 16MB ДО «округления вверх», минимум 32 — последним (§8 п.4).
        var wb = 3 * sb / 100;
        if (wb > 16 * MbKb)
            wb = 16 * MbKb;
        if (wb > 14 * MbKb && wb < 16 * MbKb)
            wb = 16 * MbKb;
        if (wb < 32)
            wb = 32;

        // 4.10 random_page_cost: первое совпавшее правило (less_ram сильнее диска).
        var rpc = input.DbSize == PgTuneDbSize.LessRam ? "1.1"
            : input.HdType == PgTuneHdType.Hdd ? "4"
            : input.DbType == PgTuneDbType.Dw ? "4"
            : "1.1";

        // 4.11 effective_io_concurrency: только Linux.
        string? eio = input.OsType == PgTuneOsType.Linux ? input.HdType switch
        {
            PgTuneHdType.Hdd => "2",
            PgTuneHdType.Ssd => "200",
            PgTuneHdType.San => "300",
            PgTuneHdType.Nvme => "1000",
            _ => throw new ArgumentOutOfRangeException(nameof(input), input.HdType, "неизвестный тип диска"),
        } : null;

        // 4.12 параллельные: только при cpuNum ≥ 4; иначе pfw = 8 (§4.12/§8 п.5).
        string? maxWorkerProcesses = null;
        string? maxParallelWorkersPerGather = null;
        string? maxParallelWorkers = null;
        string? maxParallelMaintenanceWorkers = null;
        var parallelForWorkMem = 8L;
        if (input.CpuNum is { } cpuNum and >= 4)
        {
            var wpg = (cpuNum + 1) / 2; // ceil(cpuNum/2)
            if (input.DbType != PgTuneDbType.Dw && wpg > 4)
                wpg = 4; // нет доказательств пользы большего числа воркеров на ядро
            maxWorkerProcesses = cpuNum.ToString(CultureInfo.InvariantCulture);
            maxParallelWorkersPerGather = wpg.ToString(CultureInfo.InvariantCulture);
            parallelForWorkMem = cpuNum; // pfw = max_worker_processes = cpuNum
            if (input.DbVersion >= 10)
                maxParallelWorkers = cpuNum.ToString(CultureInfo.InvariantCulture);
            if (input.DbVersion >= 11)
                maxParallelMaintenanceWorkers = Math.Min(4, (cpuNum + 1) / 2)
                    .ToString(CultureInfo.InvariantCulture);
        }

        // 4.13 work_mem: вещественное деление → floor с множителем типа БД →
        // коррекция dbSize (новый floor) → минимум 4MB → кап Windows (§8 п.1).
        var wmv = (double)(ramKb - sb) / ((maxConn + parallelForWorkMem) * 3);
        var wm = (long)(input.DbType switch
        {
            PgTuneDbType.Web or PgTuneDbType.Oltp => Math.Floor(wmv),
            PgTuneDbType.Dw or PgTuneDbType.Mixed => Math.Floor(wmv / 2),
            PgTuneDbType.Desktop => Math.Floor(wmv / 6),
            _ => throw new ArgumentOutOfRangeException(nameof(input), input.DbType, "неизвестный тип БД"),
        });
        if (input.DbSize == PgTuneDbSize.LessRam)
            wm = (long)Math.Floor(wm * 1.3); // кэш ОС недогружен
        else if (input.DbSize == PgTuneDbSize.GreaterRam)
            wm = (long)Math.Floor(wm * 0.9); // запас против OOM
        if (wm < 4 * MbKb)
            wm = 4 * MbKb;
        if (input.OsType == PgTuneOsType.Windows && input.DbVersion <= 17 && wm > 2 * GbKb - MbKb)
            wm = 2 * GbKb - MbKb;

        // 4.14 wal_level/max_wal_senders: только desktop (нулевое значение — вычисленное, §8 п.3).
        string? walLevel = null;
        string? maxWalSenders = null;
        if (input.DbType == PgTuneDbType.Desktop)
        {
            walLevel = "minimal";
            maxWalSenders = "0";
        }

        // 4.15 jit: off для web/oltp/mixed (PG ≥ 12); dw/desktop — не выводится.
        var jit = input.DbVersion >= 12 && input.DbType is PgTuneDbType.Web or PgTuneDbType.Oltp or PgTuneDbType.Mixed
            ? "off"
            : null;

        // 4.16 wal_compression: lz4 (PG ≥ 15, требует --with-lz4) / on (pglz).
        var walCompression = input.DbVersion >= 15 ? "lz4" : "on";

        // 4.17 autovacuum_max_workers: только при cpuNum ≥ 16.
        string? avw = input.CpuNum switch
        {
            null => null,
            { } c when c >= 32 => "5",
            { } c when c >= 16 => "4",
            _ => null,
        };

        // 4.18 autovacuum_work_mem: кап 2GB на воркер при mwm ≥ 2GB.
        long? avm = mwm >= 2 * GbKb ? 2 * GbKb : null;
        if (avm is not null && input.OsType == PgTuneOsType.Windows && input.DbVersion <= 17
            && avm > 2 * GbKb - MbKb)
            avm = 2 * GbKb - MbKb;

        // 4.19 io_method: PG ≥ 18; io_uring только на Linux (--with-liburing).
        string? iom = input.DbVersion >= 18
            ? input.OsType == PgTuneOsType.Linux ? "io_uring" : "worker"
            : null;

        // 4.20 io_workers: PG ≥ 18, cpuNum задан, не io_uring; > 3 (иначе дефолт).
        string? iow = null;
        if (input.DbVersion >= 18 && input.CpuNum is { } ioCpu && iom != "io_uring")
        {
            var v = Math.Min(32, Math.Max(3, ioCpu / 4)); // ~25% ядер, кап 32
            if (v > 3)
                iow = v.ToString(CultureInfo.InvariantCulture);
        }

        // 4.21 предупреждения: память → lz4 → io_uring → cost-DW, разделители "" между группами.
        var totalMemoryBytes = input.TotalMemory * UnitBytes(input.TotalMemoryUnit);
        var warnings = new List<string>();
        if (totalMemoryBytes < 256L * 1024 * 1024)
            warnings.AddRange(["this tool not being optimal", "for low memory systems"]);
        else if (totalMemoryBytes > 100L * 1024 * 1024 * 1024)
            warnings.AddRange(["this tool not being optimal", "for very high memory systems"]);

        void AddGroup(params string[] lines)
        {
            if (warnings.Count > 0)
                warnings.Add("");
            warnings.AddRange(lines);
        }

        if (walCompression == "lz4")
            AddGroup("wal_compression = lz4 requires PostgreSQL", "to be compiled with --with-lz4");
        if (iom == "io_uring")
            AddGroup("io_method = io_uring requires PostgreSQL", "to be compiled with --with-liburing");
        if (input.DbType == PgTuneDbType.Dw && input.HdType != PgTuneHdType.Hdd
            && input.DbSize != PgTuneDbSize.LessRam)
        {
            var medium = input.HdType switch
            {
                PgTuneHdType.Ssd => "SSDs",
                PgTuneHdType.Nvme => "NVMe drives",
                PgTuneHdType.San => "SAN storage",
                _ => throw new ArgumentOutOfRangeException(nameof(input), input.HdType, "неизвестный тип диска"),
            };
            AddGroup(
                $"Cost parameters for Data Warehouses on {medium} are left at defaults",
                "to avoid catastrophic index scan selections",
                "Monitor query planner behavior and adjust random_page_cost if necessary");
        }

        if (warnings.Count > 0)
            warnings.Insert(0, "WARNING");

        // Выход: пары в порядке §5.2, не вычисленные — пропущены (§1 п.5, §8 п.3).
        var parameters = new List<PgTuneParameter>
        {
            new("max_connections", maxConn.ToString(CultureInfo.InvariantCulture)),
            new("shared_buffers", FormatValue(sb)),
            new("effective_cache_size", FormatValue(ecs)),
            new("maintenance_work_mem", FormatValue(mwm)),
            new("checkpoint_completion_target", "0.9"),
            new("wal_buffers", FormatValue(wb)),
            new("default_statistics_target",
                (input.DbType == PgTuneDbType.Dw ? 500 : 100).ToString(CultureInfo.InvariantCulture)),
            new("random_page_cost", rpc),
        };
        if (eio is not null)
            parameters.Add(new("effective_io_concurrency", eio));
        parameters.Add(new("work_mem", FormatValue(wm)));
        parameters.Add(new("huge_pages", hugePages));
        if (jit is not null)
            parameters.Add(new("jit", jit));
        if (walCompression is not null)
            parameters.Add(new("wal_compression", walCompression));
        if (avw is not null)
            parameters.Add(new("autovacuum_max_workers", avw));
        if (avm is not null)
            parameters.Add(new("autovacuum_work_mem", FormatValue(avm.Value)));
        if (iom is not null)
            parameters.Add(new("io_method", iom));
        if (iow is not null)
            parameters.Add(new("io_workers", iow));
        parameters.Add(new("min_wal_size", FormatValue(minWal)));
        parameters.Add(new("max_wal_size", FormatValue(maxWal)));
        if (maxWorkerProcesses is not null)
            parameters.Add(new("max_worker_processes", maxWorkerProcesses));
        if (maxParallelWorkersPerGather is not null)
            parameters.Add(new("max_parallel_workers_per_gather", maxParallelWorkersPerGather));
        if (maxParallelWorkers is not null)
            parameters.Add(new("max_parallel_workers", maxParallelWorkers));
        if (maxParallelMaintenanceWorkers is not null)
            parameters.Add(new("max_parallel_maintenance_workers", maxParallelMaintenanceWorkers));
        if (walLevel is not null)
            parameters.Add(new("wal_level", walLevel));
        if (maxWalSenders is not null)
            parameters.Add(new("max_wal_senders", maxWalSenders));

        return new PgTuneResult(parameters, warnings);
    }

    /// <summary>Байтов в единице памяти (§2 таблица констант).</summary>
    private static long UnitBytes(PgTuneMemoryUnit unit) => unit switch
    {
        PgTuneMemoryUnit.KB => 1024L,
        PgTuneMemoryUnit.MB => 1024L * 1024,
        PgTuneMemoryUnit.GB => 1024L * 1024 * 1024,
        PgTuneMemoryUnit.TB => 1024L * 1024 * 1024 * 1024,
        _ => throw new ArgumentOutOfRangeException(nameof(unit), unit, "неизвестная единица памяти"),
    };

    /// <summary>Валидация границ входа §2 — до расчёта (§8 п.9). KB — внутренний
    /// канал фабрики: только нижняя граница, без верхнего капа 999999 (шапка файла).</summary>
    private static void Validate(PgTuneInput input)
    {
        if (input.DbVersion is < 10 or > 18)
            throw new ArgumentException(
                $"dbVersion {input.DbVersion} вне допустимых значений 10..18 (§2 спецификации алгоритма)", nameof(input));
        if (input.TotalMemory <= 0)
            throw new ArgumentException(
                $"totalMemory {input.TotalMemory} обязан быть > 0 (§2 спецификации алгоритма)", nameof(input));
        switch (input.TotalMemoryUnit)
        {
            case PgTuneMemoryUnit.KB:
                break; // внутренний канал: кап 999999 не применяется (решение зафиксировано в плане)
            case PgTuneMemoryUnit.MB:
                if (input.TotalMemory < 512)
                    throw new ArgumentException(
                        $"totalMemory {input.TotalMemory} MB < 512 (§2 спецификации алгоритма)", nameof(input));
                if (input.TotalMemory > 999999)
                    throw new ArgumentException(
                        $"totalMemory {input.TotalMemory} MB > 999999 (§2 спецификации алгоритма)", nameof(input));
                break;
            case PgTuneMemoryUnit.GB:
            case PgTuneMemoryUnit.TB:
                if (input.TotalMemory > 999999)
                    throw new ArgumentException(
                        $"totalMemory {input.TotalMemory} {input.TotalMemoryUnit} > 999999 (§2 спецификации алгоритма)", nameof(input));
                break;
            default:
                throw new ArgumentOutOfRangeException(
                    nameof(input), input.TotalMemoryUnit, "неизвестная единица памяти");
        }

        if (input.CpuNum is < 1)
            throw new ArgumentException(
                $"cpuNum {input.CpuNum} вне допустимых значений 1..999999 (§2 спецификации алгоритма)", nameof(input));
        if (input.ConnectionNum is < 20)
            throw new ArgumentException(
                $"connectionNum {input.ConnectionNum} вне допустимых значений 20..999999 (§2 спецификации алгоритма)", nameof(input));
    }

    /// <summary>Форматирование памяти §5.1: кратность GB первой, затем MB, иначе kB.</summary>
    private static string FormatValue(long kb) => kb switch
    {
        _ when kb % (1024 * 1024) == 0 => (kb / (1024 * 1024)).ToString(CultureInfo.InvariantCulture) + "GB",
        _ when kb % 1024 == 0 => (kb / 1024).ToString(CultureInfo.InvariantCulture) + "MB",
        _ => kb.ToString(CultureInfo.InvariantCulture) + "kB",
    };

    /// <summary>Заголовок входных данных §5.2 (только непустые значения; dbSize
    /// в заголовок не включается — §8 п.7). commentPrefix — "#" (conf) или "--" (ALTER SYSTEM).</summary>
    private static IEnumerable<string> HeaderLines(PgTuneInput input, string commentPrefix)
    {
        yield return $"{commentPrefix} DB Version: {input.DbVersion}";
        yield return $"{commentPrefix} OS Type: {input.OsType.ToString().ToLowerInvariant()}";
        yield return $"{commentPrefix} DB Type: {input.DbType.ToString().ToLowerInvariant()}";
        yield return $"{commentPrefix} Total Memory (RAM): {input.TotalMemory} {input.TotalMemoryUnit}";
        if (input.CpuNum is { } cpu)
            yield return $"{commentPrefix} CPUs num: {cpu}";
        if (input.ConnectionNum is { } conn)
            yield return $"{commentPrefix} Connections num: {conn}";
        yield return $"{commentPrefix} Data Storage: {input.HdType.ToString().ToLowerInvariant()}";
    }

    /// <summary>Режим postgresql.conf (§5.3): предупреждения и заголовок — комментариями "#",
    /// параметры "имя = значение". Разделитель строк — "\n" (детерминизм тест-векторов).</summary>
    public static string ToPostgresqlConf(PgTuneInput input, PgTuneResult result)
    {
        var lines = new List<string>();
        foreach (var warning in result.Warnings)
            lines.Add(warning.Length == 0 ? "#" : $"# {warning}");
        lines.AddRange(HeaderLines(input, "#"));
        lines.Add("");
        foreach (var parameter in result.Parameters)
            lines.Add($"{parameter.Name} = {parameter.Value}");
        return string.Join("\n", lines);
    }

    /// <summary>Режим ALTER SYSTEM (§5.3): комментарии "--", значения всегда в одинарных кавычках.</summary>
    public static string ToAlterSystem(PgTuneInput input, PgTuneResult result)
    {
        var lines = new List<string>();
        foreach (var warning in result.Warnings)
            lines.Add(warning.Length == 0 ? "--" : $"-- {warning}");
        lines.AddRange(HeaderLines(input, "--"));
        lines.Add("");
        foreach (var parameter in result.Parameters)
            lines.Add($"ALTER SYSTEM SET {parameter.Name} = '{parameter.Value}';");
        return string.Join("\n", lines);
    }
}
