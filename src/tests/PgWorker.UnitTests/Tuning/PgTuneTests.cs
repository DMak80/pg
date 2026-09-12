using PgWorker.Core.Tuning;

namespace PgWorker.UnitTests.Tuning;

// PgTune: ядро расчёта параметров PostgreSQL — тест-векторы контрольных примеров
// §7 docs/pgtune-calculation-spec.md (один-в-один) + правила/форматы/валидация.
// Предупреждения §4.21 нормативны: в текстах примеров документации они опущены
// «для компактности», в ожиданиях тестов — присутствуют (комментариями перед
// заголовком конфигурации).

public class PgTuneTests
{
    // Контрольный пример 1: PG 15, Linux, web, 4GB, 4 CPU, 300 подключений, SSD, mid_ram.
    private static readonly PgTuneInput Example1 = new(
        15, PgTuneOsType.Linux, PgTuneDbType.Web, 4, PgTuneMemoryUnit.GB,
        4, 300, PgTuneHdType.Ssd, PgTuneDbSize.MidRam);

    // Контрольный пример 2: PG 18, Linux, dw, 64GB, 16 CPU, подключения не заданы, NVMe, mid_ram.
    private static readonly PgTuneInput Example2 = new(
        18, PgTuneOsType.Linux, PgTuneDbType.Dw, 64, PgTuneMemoryUnit.GB,
        16, null, PgTuneHdType.Nvme, PgTuneDbSize.MidRam);

    // Контрольный пример 3: PG 18, Windows, desktop, 8GB, CPU не заданы, HDD, mid_ram.
    private static readonly PgTuneInput Example3 = new(
        18, PgTuneOsType.Windows, PgTuneDbType.Desktop, 8, PgTuneMemoryUnit.GB,
        null, null, PgTuneHdType.Hdd, PgTuneDbSize.MidRam);

    private static string[] ConfLines(string conf) => conf.Split('\n');

    [Fact]
    public void Calculate_Example1_ReproducesReferenceConf()
    {
        // Arrange: контрольный пример 1 спецификации алгоритма.

        // Act: расчёт + режим postgresql.conf.
        var result = PgTune.Calculate(Example1);
        var conf = PgTune.ToPostgresqlConf(Example1, result);

        // Assert: построчное равенство блока «заголовок + параметры» (§7 пример 1)
        // плюс предупреждения §4.21 (lz4-пара) перед заголовком.
        var expected = string.Join("\n",
        [
            "# WARNING",
            "# wal_compression = lz4 requires PostgreSQL",
            "# to be compiled with --with-lz4",
            "# DB Version: 15",
            "# OS Type: linux",
            "# DB Type: web",
            "# Total Memory (RAM): 4 GB",
            "# CPUs num: 4",
            "# Connections num: 300",
            "# Data Storage: ssd",
            "",
            "max_connections = 300",
            "shared_buffers = 1GB",
            "effective_cache_size = 3GB",
            "maintenance_work_mem = 256MB",
            "checkpoint_completion_target = 0.9",
            "wal_buffers = 16MB",
            "default_statistics_target = 100",
            "random_page_cost = 1.1",
            "effective_io_concurrency = 200",
            "work_mem = 4MB",
            "huge_pages = off",
            "jit = off",
            "wal_compression = lz4",
            "min_wal_size = 1GB",
            "max_wal_size = 4GB",
            "max_worker_processes = 4",
            "max_parallel_workers_per_gather = 2",
            "max_parallel_workers = 4",
            "max_parallel_maintenance_workers = 2",
        ]);
        conf.Should().Be(expected);

        // Assert: предупреждения — lz4-пара с ведущим WARNING (§4.21).
        result.Warnings.Should().Equal(
            "WARNING",
            "wal_compression = lz4 requires PostgreSQL",
            "to be compiled with --with-lz4");
    }

    [Fact]
    public void Calculate_Example2_ReproducesReferenceLines()
    {
        // Arrange: контрольный пример 2 (dw, 64GB, 16 CPU, подключения → 40, NVMe).

        // Act: расчёт + режим postgresql.conf.
        var result = PgTune.Calculate(Example2);
        var conf = PgTune.ToPostgresqlConf(Example2, result);

        // Assert: точные строки эталона §7 пример 2.
        conf.Should().Contain("max_connections = 40");
        conf.Should().Contain("shared_buffers = 16GB");
        conf.Should().Contain("effective_cache_size = 48GB");
        conf.Should().Contain("maintenance_work_mem = 8GB");
        conf.Should().Contain("wal_buffers = 16MB");
        conf.Should().Contain("default_statistics_target = 500");
        conf.Should().Contain("random_page_cost = 4");
        conf.Should().Contain("effective_io_concurrency = 1000");
        conf.Should().Contain("work_mem = 149796kB");
        conf.Should().Contain("huge_pages = try");
        conf.Should().Contain("wal_compression = lz4");
        conf.Should().Contain("autovacuum_max_workers = 4");
        conf.Should().Contain("autovacuum_work_mem = 2GB");
        conf.Should().Contain("io_method = io_uring");
        conf.Should().Contain("min_wal_size = 4GB");
        conf.Should().Contain("max_wal_size = 16GB");
        conf.Should().Contain("max_worker_processes = 16");
        conf.Should().Contain("max_parallel_workers_per_gather = 8");
        conf.Should().Contain("max_parallel_workers = 16");
        conf.Should().Contain("max_parallel_maintenance_workers = 4");

        // Assert: jit не выводится (DW); io_workers не выводится при io_uring;
        // подключений во входе нет — строки заголовка «Connections num» нет.
        conf.Should().NotContain("jit");
        conf.Should().NotContain("io_workers");
        conf.Should().NotContain("Connections num");

        // Assert: предупреждения — lz4-пара, пустой разделитель, liburing-пара,
        // пустой разделитель, тройка cost-DW для NVMe (§4.21).
        result.Warnings.Should().Equal(
            "WARNING",
            "wal_compression = lz4 requires PostgreSQL",
            "to be compiled with --with-lz4",
            "",
            "io_method = io_uring requires PostgreSQL",
            "to be compiled with --with-liburing",
            "",
            "Cost parameters for Data Warehouses on NVMe drives are left at defaults",
            "to avoid catastrophic index scan selections",
            "Monitor query planner behavior and adjust random_page_cost if necessary");
    }

    [Fact]
    public void Calculate_Example3_ReproducesReferenceLines()
    {
        // Arrange: контрольный пример 3 (Windows, desktop, 8GB, CPU не заданы, HDD).

        // Act: расчёт + режим postgresql.conf.
        var result = PgTune.Calculate(Example3);
        var conf = PgTune.ToPostgresqlConf(Example3, result);

        // Assert: точные строки эталона §7 пример 3 (parallel_for_work_mem = 8:
        // floor(floor((8388608 − 524288)/((20+8)×3))/6) = 15603 KB).
        conf.Should().Contain("max_connections = 20");
        conf.Should().Contain("shared_buffers = 512MB");
        conf.Should().Contain("effective_cache_size = 2GB"); // кратность GB проверяется первой
        conf.Should().Contain("maintenance_work_mem = 512MB");
        conf.Should().Contain("work_mem = 15603kB");
        conf.Should().Contain("huge_pages = off");
        conf.Should().Contain("wal_compression = lz4");
        conf.Should().Contain("io_method = worker");
        conf.Should().Contain("min_wal_size = 100MB");
        conf.Should().Contain("max_wal_size = 2GB");
        conf.Should().Contain("wal_level = minimal");
        conf.Should().Contain("max_wal_senders = 0");

        // Assert: отсутствуют — effective_io_concurrency (не Linux), jit (desktop),
        // параллельные/autovacuum/io_workers (CPU не заданы).
        conf.Should().NotContain("effective_io_concurrency");
        conf.Should().NotContain("jit");
        conf.Should().NotContain("max_worker_processes");
        conf.Should().NotContain("max_parallel_workers");
        conf.Should().NotContain("autovacuum_max_workers");
        conf.Should().NotContain("autovacuum_work_mem");
        conf.Should().NotContain("io_workers");

        // Assert: предупреждения — только lz4-пара (io_method=worker, не io_uring).
        result.Warnings.Should().Equal(
            "WARNING",
            "wal_compression = lz4 requires PostgreSQL",
            "to be compiled with --with-lz4");
    }

    [Fact]
    public void ToAlterSystem_Example1_ReproducesReferenceSql()
    {
        // Arrange: контрольный пример 4 — тот же расчёт, режим ALTER SYSTEM.

        // Act: расчёт + режим ALTER SYSTEM.
        var result = PgTune.Calculate(Example1);
        var sql = PgTune.ToAlterSystem(Example1, result);

        // Assert: построчное равенство эталона §7 пример 4 — значения всегда
        // в одинарных кавычках, комментарии "--".
        var expected = string.Join("\n",
        [
            "-- WARNING",
            "-- wal_compression = lz4 requires PostgreSQL",
            "-- to be compiled with --with-lz4",
            "-- DB Version: 15",
            "-- OS Type: linux",
            "-- DB Type: web",
            "-- Total Memory (RAM): 4 GB",
            "-- CPUs num: 4",
            "-- Connections num: 300",
            "-- Data Storage: ssd",
            "",
            "ALTER SYSTEM SET max_connections = '300';",
            "ALTER SYSTEM SET shared_buffers = '1GB';",
            "ALTER SYSTEM SET effective_cache_size = '3GB';",
            "ALTER SYSTEM SET maintenance_work_mem = '256MB';",
            "ALTER SYSTEM SET checkpoint_completion_target = '0.9';",
            "ALTER SYSTEM SET wal_buffers = '16MB';",
            "ALTER SYSTEM SET default_statistics_target = '100';",
            "ALTER SYSTEM SET random_page_cost = '1.1';",
            "ALTER SYSTEM SET effective_io_concurrency = '200';",
            "ALTER SYSTEM SET work_mem = '4MB';",
            "ALTER SYSTEM SET huge_pages = 'off';",
            "ALTER SYSTEM SET jit = 'off';",
            "ALTER SYSTEM SET wal_compression = 'lz4';",
            "ALTER SYSTEM SET min_wal_size = '1GB';",
            "ALTER SYSTEM SET max_wal_size = '4GB';",
            "ALTER SYSTEM SET max_worker_processes = '4';",
            "ALTER SYSTEM SET max_parallel_workers_per_gather = '2';",
            "ALTER SYSTEM SET max_parallel_workers = '4';",
            "ALTER SYSTEM SET max_parallel_maintenance_workers = '2';",
        ]);
        sql.Should().Be(expected);
    }

    [Fact]
    public void FormatValue_UsesLargestLosslessUnit()
    {
        // Arrange: значения памяти из контрольных примеров + Windows-вход для капа.

        // Act: расчёты примеров и Windows-капа maintenance_work_mem.
        var r1 = PgTune.Calculate(Example1);
        var r2 = PgTune.Calculate(Example2);
        var r3 = PgTune.Calculate(Example3);
        var windows = PgTune.Calculate(new PgTuneInput(
            17, PgTuneOsType.Windows, PgTuneDbType.Web, 256, PgTuneMemoryUnit.GB,
            4, 20, PgTuneHdType.Ssd, PgTuneDbSize.MidRam));

        // Assert: 1048576 KB → "1GB" (кратность GB проверяется первой — не "1024MB");
        // 262144 KB → "256MB"; 149796 KB → "149796kB"; 2096128 KB → "2047MB"
        // (2096128 % 1048576 = 1047552 ≠ 0 → MB, §5.1).
        r1["shared_buffers"].Should().Be("1GB");
        r1["maintenance_work_mem"].Should().Be("256MB");
        r2["work_mem"].Should().Be("149796kB");
        windows["maintenance_work_mem"].Should().Be("2047MB");
        r3["effective_cache_size"].Should().Be("2GB");
    }

    [Fact]
    public void WalBuffers_AppliesRulesInOrder()
    {
        // Arrange: 3% от shared_buffers в диапазоне (14336; 16384): RAM 1920 MB →
        // SB = 491520 KB, 3% = 14745 → «округление вверх» до 16384 (кап 16MB — до
        // округления, минимум 32 — последним, §4.8/§8 п.4). Маленькая RAM (4096 KB,
        // внутренний KB-канал): 3% от SB=1024 → 30 < 32 → минимум.

        // Act: расчёты обоих входов.
        var roundUp = PgTune.Calculate(new PgTuneInput(
            18, PgTuneOsType.Linux, PgTuneDbType.Oltp, 1920, PgTuneMemoryUnit.MB,
            null, null, PgTuneHdType.Ssd, PgTuneDbSize.MidRam));
        var minimum = PgTune.Calculate(new PgTuneInput(
            18, PgTuneOsType.Linux, PgTuneDbType.Oltp, 4096, PgTuneMemoryUnit.KB,
            null, null, PgTuneHdType.Ssd, PgTuneDbSize.MidRam));

        // Assert: (14336;16384) → 16384 ("16MB", не "14745kB"); минимум 32 → "32kB".
        roundUp["wal_buffers"].Should().Be("16MB");
        minimum["wal_buffers"].Should().Be("32kB");
    }

    [Fact]
    public void RandomPageCost_AppliesRulesInOrder()
    {
        // Arrange: порядок правил §4.10 — less_ram сильнее типа диска.

        // Act: расчёт четырёх входов, покрывающих все ветви.
        var lessRamHdd = PgTune.Calculate(new PgTuneInput(
            18, PgTuneOsType.Linux, PgTuneDbType.Web, 4, PgTuneMemoryUnit.GB,
            null, null, PgTuneHdType.Hdd, PgTuneDbSize.LessRam));
        var hdd = PgTune.Calculate(new PgTuneInput(
            18, PgTuneOsType.Linux, PgTuneDbType.Web, 4, PgTuneMemoryUnit.GB,
            null, null, PgTuneHdType.Hdd, PgTuneDbSize.MidRam));
        var dwNvme = PgTune.Calculate(new PgTuneInput(
            18, PgTuneOsType.Linux, PgTuneDbType.Dw, 4, PgTuneMemoryUnit.GB,
            null, null, PgTuneHdType.Nvme, PgTuneDbSize.MidRam));
        var webSsd = PgTune.Calculate(new PgTuneInput(
            18, PgTuneOsType.Linux, PgTuneDbType.Web, 4, PgTuneMemoryUnit.GB,
            null, null, PgTuneHdType.Ssd, PgTuneDbSize.MidRam));

        // Assert: less_ram+hdd → 1.1 (правило 1 сильнее); hdd → 4; dw+nvme (mid_ram)
        // → 4 (правило 3); web+ssd+mid_ram → 1.1 (иначе).
        lessRamHdd["random_page_cost"].Should().Be("1.1");
        hdd["random_page_cost"].Should().Be("4");
        dwNvme["random_page_cost"].Should().Be("4");
        webSsd["random_page_cost"].Should().Be("1.1");
    }

    [Fact]
    public void CpuNumNotSet_SkipsCpuDerivedParameters()
    {
        // Arrange: пример 3 — cpuNum не задан (параллельные_for_work_mem = 8).

        // Act: расчёт примера 3.
        var result = PgTune.Calculate(Example3);

        // Assert: параллельные, autovacuum_max_workers и io_workers отсутствуют;
        // work_mem = 15603kB фиксирует использование дефолта 8 (не cpuNum).
        result.Parameters.Should().NotContain(p => p.Name == "max_worker_processes");
        result.Parameters.Should().NotContain(p => p.Name == "max_parallel_workers_per_gather");
        result.Parameters.Should().NotContain(p => p.Name == "max_parallel_workers");
        result.Parameters.Should().NotContain(p => p.Name == "max_parallel_maintenance_workers");
        result.Parameters.Should().NotContain(p => p.Name == "autovacuum_max_workers");
        result.Parameters.Should().NotContain(p => p.Name == "io_workers");
        result["work_mem"].Should().Be("15603kB");
    }

    [Fact]
    public void Windows_CapsMaintenanceAndWorkMem()
    {
        // Arrange: PG17/Windows/256GB — лимит maintenance 2GB, капы «2GB − 1MB» (§4.5/§4.13).

        // Act: расчёт входа PG 17, Windows, web, 256GB, 4 CPU, 20 подключений.
        var result = PgTune.Calculate(new PgTuneInput(
            17, PgTuneOsType.Windows, PgTuneDbType.Web, 256, PgTuneMemoryUnit.GB,
            4, 20, PgTuneHdType.Ssd, PgTuneDbSize.MidRam));

        // Assert: maintenance_work_mem = 2047MB (2096128 KB = 2GB − 1MB);
        // work_mem капнут 2096128 → "2047MB"; autovacuum_work_mem не выводится
        // (mwm < 2GB на Windows ≤ 17 — §4.18 примечание); io_method нет (PG < 18).
        result["maintenance_work_mem"].Should().Be("2047MB");
        result["work_mem"].Should().Be("2047MB");
        result["autovacuum_work_mem"].Should().BeNull();
        result["io_method"].Should().BeNull();
    }

    [Fact]
    public void DbSize_CorrectsWorkMem()
    {
        // Arrange: oltp/4GB/60 подключений без CPU: базовый work_mem = 15420 KB
        // ((4194304−1048576)/((60+8)×3) = 15420.23), коррекции §4.13.

        // Act: расчёт less_ram (×1.3) и greater_ram (×0.9).
        var lessRam = PgTune.Calculate(new PgTuneInput(
            18, PgTuneOsType.Linux, PgTuneDbType.Oltp, 4, PgTuneMemoryUnit.GB,
            null, 60, PgTuneHdType.Ssd, PgTuneDbSize.LessRam));
        var greaterRam = PgTune.Calculate(new PgTuneInput(
            18, PgTuneOsType.Linux, PgTuneDbType.Oltp, 4, PgTuneMemoryUnit.GB,
            null, 60, PgTuneHdType.Ssd, PgTuneDbSize.GreaterRam));

        // Assert: floor(15420×1.3) = 20046 → "20046kB"; floor(15420×0.9) = 13878 → "13878kB".
        lessRam["work_mem"].Should().Be("20046kB");
        greaterRam["work_mem"].Should().Be("13878kB");
    }

    [Fact]
    public void WorkMem_FloorsAt4MB()
    {
        // Arrange: пример 1 — work_mem по формуле 3449 KB < 4096.

        // Act: расчёт примера 1.
        var result = PgTune.Calculate(Example1);

        // Assert: минимум 4MB (4096 KB) — против сброса на диск (§4.13).
        result["work_mem"].Should().Be("4MB");
    }

    [Fact]
    public void Calculate_ValidatesInputBounds()
    {
        // Arrange: входы за границами §2 спецификации алгоритма (до расчёта).

        // Act + Assert: каждый невалидный вход — ArgumentException.
        new PgTuneInput(9, PgTuneOsType.Linux, PgTuneDbType.Web, 4, PgTuneMemoryUnit.GB,
                null, null, PgTuneHdType.Ssd, PgTuneDbSize.MidRam)
            .FailValidation();
        new PgTuneInput(19, PgTuneOsType.Linux, PgTuneDbType.Web, 4, PgTuneMemoryUnit.GB,
                null, null, PgTuneHdType.Ssd, PgTuneDbSize.MidRam)
            .FailValidation();
        new PgTuneInput(18, PgTuneOsType.Linux, PgTuneDbType.Web, 0, PgTuneMemoryUnit.GB,
                null, null, PgTuneHdType.Ssd, PgTuneDbSize.MidRam)
            .FailValidation();
        new PgTuneInput(18, PgTuneOsType.Linux, PgTuneDbType.Web, 511, PgTuneMemoryUnit.MB,
                null, null, PgTuneHdType.Ssd, PgTuneDbSize.MidRam)
            .FailValidation();
        new PgTuneInput(18, PgTuneOsType.Linux, PgTuneDbType.Web, 1000000, PgTuneMemoryUnit.MB,
                null, null, PgTuneHdType.Ssd, PgTuneDbSize.MidRam)
            .FailValidation();
        new PgTuneInput(18, PgTuneOsType.Linux, PgTuneDbType.Web, 1000000, PgTuneMemoryUnit.GB,
                null, null, PgTuneHdType.Ssd, PgTuneDbSize.MidRam)
            .FailValidation();
        new PgTuneInput(18, PgTuneOsType.Linux, PgTuneDbType.Web, 4, PgTuneMemoryUnit.GB,
                0, null, PgTuneHdType.Ssd, PgTuneDbSize.MidRam)
            .FailValidation();
        new PgTuneInput(18, PgTuneOsType.Linux, PgTuneDbType.Web, 4, PgTuneMemoryUnit.GB,
                null, 19, PgTuneHdType.Ssd, PgTuneDbSize.MidRam)
            .FailValidation();
    }

    [Fact]
    public void Calculate_KbUnitHasNoUpperCap()
    {
        // Arrange: KB — внутренний канал фабрики: кап 999999 §2 НЕ применяется
        // (решение зафиксировано в плане задачи 1); 8 GiB = 8388608 KB проходит.

        // Act: расчёт входа 8388608 KB.
        var result = PgTune.Calculate(new PgTuneInput(
            18, PgTuneOsType.Linux, PgTuneDbType.Oltp, 8388608, PgTuneMemoryUnit.KB,
            null, 60, PgTuneHdType.Ssd, PgTuneDbSize.MidRam));

        // Assert: расчёт выполнен (max_connections выведен — параметр выводится всегда).
        result["max_connections"].Should().Be("60");
    }

    [Fact]
    public void Result_IndexerReturnsParameterValueByName()
    {
        // Arrange: результат примера 1.

        // Act: чтение по имени выведенного и не выведенного параметра.

        // Assert: значение по имени; отсутствующее имя → null (нужно BuildSpec/doorman).
        var result = PgTune.Calculate(Example1);
        result["max_connections"].Should().Be("300");
        result["io_method"].Should().BeNull(); // PG15 < 18 — параметр не выведен
    }

    [Fact]
    public void KnownParameterNames_ListsAll25OutputNames()
    {
        // Arrange: реестр имён §5.2 (валидация ExcludeParams в конфигурации).

        // Act: чтение реестра.

        // Assert: 25 имён, первое/последнее — по порядку §5.2.
        PgTune.KnownParameterNames.Should().HaveCount(25);
        PgTune.KnownParameterNames.First().Should().Be("max_connections");
        PgTune.KnownParameterNames.Last().Should().Be("max_wal_senders");
        PgTune.KnownParameterNames.Should().Contain("shared_buffers");
        PgTune.KnownParameterNames.Should().Contain("io_workers");
    }
}

// Хелпер валидации: вход обязан ронять Calculate исключением аргумента (AAA-хелпер).
file static class PgTuneInputValidationAssertions
{
    public static void FailValidation(this PgTuneInput input)
    {
        // Arrange: действие расчёта невалидного входа.
        var act = () => PgTune.Calculate(input);

        // Act + Assert: ArgumentException до расчёта (§2/§8 п.9).
        act.Should().Throw<ArgumentException>();
    }
}
