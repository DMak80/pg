using FluentAssertions;
using ValkeyWorker.App.Api.Operations;
using Xunit;

namespace ValkeyWorker.UnitTests.Api;

// Валидации API (spec §4.7, pg §9.3): имя, nodes=1, 8 policy, maxmemory,
// границы Cpu/MemGi/DiskGi, инвариант maxmemory < mem — на чистых функциях.
public class ValkeyValidationTests
{
    private static List<ValidationError> Validate(
        string? name = "demo", int? nodes = 1, long? maxmemory = 536870912,
        string? policy = "allkeys-lru", decimal? cpu = 1m, int? memGi = 1, int? diskGi = 10)
        => CreateClusterHandler.Validate(new CreateValkeyClusterRequest(
            name, nodes, maxmemory, policy, new ValkeyResourcesUpdateRequest(cpu, memGi, diskGi)));

    private static bool HasError(List<ValidationError> errors, string field) => errors.Any(e => e.Field == field);

    [Fact]
    public void ВалиднаяЗаявка_БезОшибок()
    {
        // Arrange/Act/Assert
        Validate().Should().BeEmpty();
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("Demo")]
    [InlineData("with-dash")]
    [InlineData("_underscore")]
    [InlineData("1digit")]
    [InlineData("a")]
    public void Имя_НеканоническоеОтклоняется(string? name)
    {
        // Arrange/Act: "a" — канон (нижняя граница), остальные — нет.
        var errors = Validate(name: name);

        // Assert
        if (name == "a")
            errors.Should().BeEmpty();
        else
            HasError(errors, "name").Should().BeTrue();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(2)]
    [InlineData(3)]
    public void Ноды_ТолькоОдин(int nodes)
    {
        // Arrange/Act/Assert: v1 standalone — реплики roadmap.
        HasError(Validate(nodes: nodes), "nodes").Should().BeTrue();
    }

    [Fact]
    public void МаксимумПамяти_НольИлиМеньше_Отклоняется()
    {
        // Arrange/Act/Assert
        HasError(Validate(maxmemory: 0), "maxmemoryBytes").Should().BeTrue();
        HasError(Validate(maxmemory: -1), "maxmemoryBytes").Should().BeTrue();
    }

    [Theory]
    [InlineData("allkeys-lru", true)]
    [InlineData("allkeys-lfu", true)]
    [InlineData("volatile-lru", true)]
    [InlineData("volatile-lfu", true)]
    [InlineData("allkeys-random", true)]
    [InlineData("volatile-random", true)]
    [InlineData("volatile-ttl", true)]
    [InlineData("noeviction", true)]
    [InlineData("lru", false)]
    [InlineData("ALLKEYS-LRU", false)]
    public void Политика_ВосемьЗначенийКанона(string policy, bool valid)
    {
        // Arrange/Act/Assert
        var errors = Validate(policy: policy);
        (errors.Count == 0).Should().Be(valid);
    }

    [Theory]
    [InlineData(0.009, false)]
    [InlineData(0.01, true)]
    [InlineData(0.5, true)]
    [InlineData(64, true)]
    [InlineData(64.01, false)]
    public void Процессор_Границы010_64(decimal cpu, bool valid)
    {
        // Arrange/Act/Assert: decimal-ядра без суффикса m.
        var errors = Validate(cpu: cpu);
        (errors.Count == 0).Should().Be(valid);
    }

    [Theory]
    [InlineData(0, false)]
    [InlineData(1, true)]
    [InlineData(65536, true)]
    [InlineData(65537, false)]
    public void ПамятьИДиск_ГраницыГиб(int memGi, bool valid)
    {
        // Arrange/Act/Assert: целые GiB 1..65536.
        var errors = Validate(memGi: memGi, diskGi: memGi);
        (errors.Count == 0).Should().Be(valid);
    }

    [Fact]
    public void ИнвариантMaxmemoryМеньшеПамяти_R3()
    {
        // Arrange: maxmemory == mem-лимиту — OOM-риск.
        // Act
        var equal = Validate(maxmemory: 1024L * 1024 * 1024, memGi: 1);
        var fits = Validate(maxmemory: 1024L * 1024 * 1024 - 1, memGi: 1);

        // Assert: нарушение отклоняется; строго меньше — проходит.
        HasError(equal, "maxmemoryBytes").Should().BeTrue();
        fits.Should().BeEmpty();
    }

    [Fact]
    public void ConfigJson_Sериализация_КанонArch20_21()
    {
        // Arrange: Active-кластер (state отсутствует) и заявка (state задан).

        // Act
        var active = new ValkeyConfigJson(1, 536870912, "allkeys-lru", 1756500000, null).Serialize();
        var requested = new ValkeyConfigJson(1, 536870912, "allkeys-lru", 1756500000, "NOT_INITIALIZED").Serialize();

        // Assert: null-поля ОТСУТСТВУЮТ (не "state":null — arch/20 §2.1);
        // заявка несёт state.
        active.Should().Be("""{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru","created_unix":1756500000}""");
        requested.Should().Contain("\"state\":\"NOT_INITIALIZED\"");
    }
}
