using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using PgWorker.Core.Model;
using PgWorker.Core.Tuning;
using PgWorker.Provisioning.Processes;

namespace PgWorker.UnitTests.Provisioning;

// PgtuneInputsFactory: канал «заявки ресурсов + PgWorker:Pgtune → вход ядра →
// вывод» (spec.md §4.3): floor-преобразования, fallback-память, детерминизм,
// warning-лог; etcd не используется (вызывается держателем клэйма <C>).

public class PgtuneInputsFactoryTests
{
    // Дефолтные настройки PGTune (эквивалент секции PgWorker:Pgtune appsettings).
    private static PgtuneSettings DefaultSettings() => new(
        DbVersion: 18,
        DbType: "oltp",
        HdType: "ssd",
        DbSize: "mid_ram",
        Connections: 60,
        DefaultTotalMemoryBytes: 8589934592,
        ExcludeParams: new HashSet<string>(StringComparer.Ordinal));

    [Fact]
    public void Create_FloorsMemoryBytesToKb()
    {
        // Arrange: фабрика с дефолтными настройками; заявка 4 GiB + 4 CPU.
        var factory = new PgtuneInputsFactory(DefaultSettings(), NullLogger<PgtuneInputsFactory>.Instance);

        // Act: расчёт тюнинга от заявки 4294967296 байт (→ 4194304 KB).
        var result = factory.Create(new NodeResources(4, 4294967296));

        // Assert: floor(bytes/1024) → shared_buffers 1GB; connections — из настроек.
        result["shared_buffers"].Should().Be("1GB");
        result["max_connections"].Should().Be("60");
    }

    [Fact]
    public void Create_FallsBackToDefaultMemoryWhenResourcesMissing()
    {
        // Arrange: фабрика с дефолтными настройками (fallback 8 GiB).

        // Act: расчёт без заявки и с «пустой» заявкой (нечитаемые request_*).
        var factory = new PgtuneInputsFactory(DefaultSettings(), NullLogger<PgtuneInputsFactory>.Instance);
        var noResources = factory.Create(null);
        var emptyResources = factory.Create(new NodeResources(null, null));

        // Assert: DefaultTotalMemoryBytes/1024 = 8388608 KB → shared_buffers 2GB,
        // effective_cache_size 6GB.
        noResources["shared_buffers"].Should().Be("2GB");
        noResources["effective_cache_size"].Should().Be("6GB");
        emptyResources["shared_buffers"].Should().Be("2GB");
    }

    [Fact]
    public void Create_FloorsCpuCores()
    {
        // Arrange: фабрика с дефолтными настройками.
        var factory = new PgtuneInputsFactory(DefaultSettings(), NullLogger<PgtuneInputsFactory>.Instance);

        // Act: расчёты от заявок 4.7 / 2.7 / 0.5 ядер (память — дефолт 8 GiB).
        var fourCores = factory.Create(new NodeResources(4.7, 8589934592));
        var twoCores = factory.Create(new NodeResources(2.7, 8589934592));
        var subCore = factory.Create(new NodeResources(0.5, 8589934592));

        // Assert: floor(4.7)=4 → параллельные выведены (cpuNum ≥ 4, §4.12 алгоритма).
        fourCores["max_worker_processes"].Should().Be("4");

        // Assert: floor(2.7)=2 → cpuNum < 4 — параллельные ОТСУТСТВУЮТ; work_mem
        // 30840kB фиксирует parallel_for_work_mem = 8 (дефолт, не cpuNum).
        twoCores.Parameters.Should().NotContain(p => p.Name == "max_worker_processes");
        twoCores["work_mem"].Should().Be("30840kB");

        // Assert: floor(0.5)=0 < 1 → cpuNum не задан: параллельные,
        // autovacuum_max_workers и io_workers отсутствуют (никаких суррогатных дефолтов).
        subCore.Parameters.Should().NotContain(p => p.Name == "max_worker_processes");
        subCore.Parameters.Should().NotContain(p => p.Name == "autovacuum_max_workers");
        subCore.Parameters.Should().NotContain(p => p.Name == "io_workers");
    }

    [Fact]
    public void Create_IsDeterministic()
    {
        // Arrange: фабрика с дефолтными настройками; одинаковые заявки.
        var factory = new PgtuneInputsFactory(DefaultSettings(), NullLogger<PgtuneInputsFactory>.Instance);
        var resources = new NodeResources(4.5, 4294967296);

        // Act: два расчёта с одним входом.
        var first = factory.Create(resources);
        var second = factory.Create(resources);

        // Assert: равные результаты (детерминированная функция входа — решения в etcd нет).
        second.Parameters.Should().Equal(first.Parameters);
        second.Warnings.Should().Equal(first.Warnings);
    }

    [Fact]
    public void Create_LogsWarningsAndDoesNotBlock()
    {
        // Arrange: фабрика с перехватывающим логгером; заявка 100 MB < 256MB
        // (граница памяти §2 спецификации алгоритма → предупреждение, не блокировка).
        var logger = new CapturingLogger();
        var factory = new PgtuneInputsFactory(DefaultSettings(), logger);

        // Act: расчёт от маленькой заявки.
        var result = factory.Create(new NodeResources(null, 104857600));

        // Assert: расчёт не блокирован (max_connections выведен), предупреждения
        // попали в warning-лог.
        result["max_connections"].Should().NotBeNull();
        logger.Entries.Should().Contain(e => e.Level == LogLevel.Warning
                                             && e.Message.Contains("this tool not being optimal", StringComparison.Ordinal));
    }

    [Fact]
    public void Create_FeedsWorkerOptionsThroughCoreInput()
    {
        // Arrange: НЕ-дефолтные настройки — канал «опции воркера → вход ядра →
        // вывод» (AC spec.md §7 п.3: изменение опций подхватывается следующим
        // созданием контейнера).
        var settings = DefaultSettings() with { Connections = 40, DbType = "dw" };
        var factory = new PgtuneInputsFactory(settings, NullLogger<PgtuneInputsFactory>.Instance);

        // Act: расчёт от дефолтной памяти.
        var result = factory.Create(null);

        // Assert: Connections=40 → max_connections 40 (комплементарно doorman
        // 35 через DoormanConfigBuilder.ServerConnections — Задача 4); DbType=dw
        // → default_statistics_target 500 и random_page_cost 4 (dw+ssd+mid_ram,
        // правило 3 §4.10).
        result["max_connections"].Should().Be("40");
        result["default_statistics_target"].Should().Be("500");
        result["random_page_cost"].Should().Be("4");
    }

    [Fact]
    public void Create_UnknownDomainStringFailsFast()
    {
        // Arrange: настройка DbType вне домена (валидация старта обойдена).
        var settings = DefaultSettings() with { DbType = "nosql" };
        var factory = new PgtuneInputsFactory(settings, NullLogger<PgtuneInputsFactory>.Instance);

        // Act: расчёт с мусорной строкой.
        var act = () => factory.Create(null);

        // Assert: fail-fast расчёта (InvalidOperationException), не тихий дефолт.
        act.Should().Throw<InvalidOperationException>();
    }

    // Перехватывающий логгер: проверка попадания предупреждений PGTune в warning-лог.
    private sealed class CapturingLogger : ILogger<PgtuneInputsFactory>
    {
        public List<(LogLevel Level, string Message)> Entries { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) =>
            Entries.Add((logLevel, formatter(state, exception)));
    }
}
