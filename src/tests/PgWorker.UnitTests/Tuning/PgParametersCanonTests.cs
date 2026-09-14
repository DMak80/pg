using PgWorker.Core.Tuning;

namespace PgWorker.UnitTests.Tuning;

// PgParametersCanon (t11, spec §4.1): единый желаемый набор merge(PGTune ∪
// канон) — один источник для bootstrap (SpiloEnvBuilder) и конвергенции
// (DcsConfigConvergence). Значения — СЫРЫЕ строки без YAML/JSON-обвязки.
public class PgParametersCanonTests
{
    // Вход-эталон: oltp/Linux/8GiB/4cpu/60/ssd/mid_ram (как NodeConfigBuildersTests).
    private static PgTuneResult Tuning() => PgTune.Calculate(new PgTuneInput(
        18, PgTuneOsType.Linux, PgTuneDbType.Oltp, 8388608, PgTuneMemoryUnit.KB,
        4, 60, PgTuneHdType.Ssd, PgTuneDbSize.MidRam));

    // AAA: merge — сначала PGTune-вывод в порядке §5.2, затем канон поверх с
    // перезаписью по имени без дубликатов (позиция первого вхождения
    // сохраняется, новые ключи канона — в конец).
    [Fact]
    public void Desired_MergesPgTuneOrderWithCanonOverlay()
    {
        // Arrange — tuning эталонного входа, exclude пуст.
        // Act
        var desired = PgParametersCanon.Desired(Tuning(), null);
        var names = desired.Select(p => p.Name).ToList();

        // Assert: PGTune-порядок §5.2 сохранён для PGTune-имён...
        int Index(string name) => names.FindIndex(n => n == name);
        Index("max_connections").Should().BeLessThan(Index("shared_buffers"));
        Index("shared_buffers").Should().BeLessThan(Index("effective_cache_size"));
        Index("effective_cache_size").Should().BeLessThan(Index("work_mem"));
        // ...канон-ключи, которых нет в PGTune-выводе, — в конце.
        Index("wal_keep_size").Should().BeGreaterThan(Index("work_mem"));
        Index("logging_collector").Should().BeGreaterThan(Index("work_mem"));
        // Дубликатов нет.
        names.Should().OnlyHaveUniqueItems();
        // Имена канона — фиксированный состав (P3 + лог-блок).
        PgParametersCanon.CanonParameters.Select(p => p.Name).Should().Equal(
            "wal_level", "hot_standby", "sync_replication_slots",
            "max_slot_wal_keep_size", "max_wal_senders", "max_replication_slots",
            "wal_keep_size", "checkpoint_timeout", "logging_collector",
            "log_directory", "log_filename", "log_rotation_age", "log_rotation_size");
    }

    // AAA: канон перезаписывает PGTune-значение по имени (desktop-вывод несёт
    // wal_level=minimal/max_wal_senders=0 — PgTune выводит их ТОЛЬКО для
    // desktop, §4.14 алгоритма; канон даёт logical/10 — P3 поверх PGTune всегда).
    [Fact]
    public void Desired_CanonOverwritesPgTuneByName()
    {
        // Arrange — desktop-вход: только эта ветвь выводит имена, общие с
        // каноном (wal_level/max_wal_senders), — проверяется именно ПЕРЕЗАПИСЬ.
        var desktop = PgTune.Calculate(new PgTuneInput(
            18, PgTuneOsType.Linux, PgTuneDbType.Desktop, 8388608, PgTuneMemoryUnit.KB,
            4, 60, PgTuneHdType.Ssd, PgTuneDbSize.MidRam));
        desktop["wal_level"].Should().Be("minimal", "вход обязан нести PGTune-ветвь wal_level");
        desktop["max_wal_senders"].Should().Be("0", "вход обязан нести PGTune-ветвь max_wal_senders");

        // Act
        var desired = PgParametersCanon.Desired(desktop, null);

        // Assert: wal_level один и он канонический (PGTune-значение ПЕРЕЗАПИСАНО).
        desired.Count(p => p.Name == "wal_level").Should().Be(1);
        desired.Single(p => p.Name == "wal_level").RawValue.Should().Be("logical");
        // Канон-значения P3 фиксированы (перезапись по имени, не дубль).
        desired.Count(p => p.Name == "max_wal_senders").Should().Be(1);
        desired.Single(p => p.Name == "max_wal_senders").RawValue.Should().Be("10");
        desired.Single(p => p.Name == "max_replication_slots").RawValue.Should().Be("10");
    }

    // AAA: exclude вырезает параметр из PGTune-вывода ВООБЩЕ (никаких пустых
    // значений); канон exclude не касается.
    [Fact]
    public void Desired_ExcludeCutsParameter()
    {
        // Arrange — вход, выводящий io_method/io_workers (Windows, 72cpu).
        var tuning = PgTune.Calculate(new PgTuneInput(
            18, PgTuneOsType.Windows, PgTuneDbType.Oltp, 8388608, PgTuneMemoryUnit.KB,
            72, 60, PgTuneHdType.Ssd, PgTuneDbSize.MidRam));
        var exclude = new HashSet<string>(["io_method", "io_workers"], StringComparer.Ordinal);

        // Act
        var desired = PgParametersCanon.Desired(tuning, exclude);

        // Assert
        desired.Select(p => p.Name).Should().NotContain("io_method").And.NotContain("io_workers");
        desired.Should().Contain(p => p.Name == "max_connections");
    }

    // AAA: значения — СЫРЫЕ строки без кавычек (цитирование — деталь
    // сериализатора SpiloEnvBuilder; JSON-патч тоже пишет их как строки).
    [Fact]
    public void Desired_ValuesAreRawWithoutQuotes()
    {
        // Arrange/Act
        var desired = PgParametersCanon.Desired(Tuning(), null);

        // Assert
        desired.Single(p => p.Name == "max_connections").RawValue.Should().Be("60");
        desired.Single(p => p.Name == "shared_buffers").RawValue.Should().Be("2GB");
        desired.Single(p => p.Name == "wal_level").RawValue.Should().Be("logical");
        PgParametersCanon.CanonParameters.Single(p => p.Name == "hot_standby").RawValue
            .Should().Be("on");
        PgParametersCanon.CanonParameters.Single(p => p.Name == "wal_keep_size").RawValue
            .Should().Be("2048MB");
        desired.Should().OnlyContain(p => !p.RawValue.Contains('"'));
    }

    // AAA: детерминизм — одинаковый вход даёт эквивалентный набор.
    [Fact]
    public void Desired_Deterministic()
    {
        // Arrange/Act
        var first = PgParametersCanon.Desired(Tuning(), null);
        var second = PgParametersCanon.Desired(Tuning(), null);

        // Assert
        first.Should().Equal(second);
    }
}
