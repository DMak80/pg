using PgWorker.Core.Model;
using PgWorker.Core.Templates;

namespace PgWorker.UnitTests.Templates;

// PatroniTimings (t09, arch/14 §2.1/§5 C): канон таймингов Patroni (полы
// Patroni 4.x: loop_wait≥1, retry_timeout≥3, ttl≥20) и вычисление патча
// конвергенции динамического DCS-конфига (GET /config → PATCH /config).
public class PatroniTimingsTests
{
    // AAA (t09): дефолтный конфиг Patroni (нода/кластер работают без нашего
    // bootstrap-канона) — патч несёт ВСЕ канонические поля.
    [Fact]
    public void Regression_T09_Divergence_DefaultConfig_PatchCarriesCanonical()
    {
        // Arrange — динамический конфиг на Patroni-дефолтах, synchronous_mode нет
        const string config = """{"ttl":30,"loop_wait":10,"retry_timeout":10,"postgresql":{"use_pg_rewind":true}}""";

        // Act: считаем патч конвергенции
        var patch = PatroniTimings.DivergencePatch(config);

        // Assert: все канонические тайминги в патче, чужие поля не тронуты
        patch.Should().NotBeNull();
        patch.Should().Contain("\"ttl\":20").And.Contain("\"loop_wait\":1")
            .And.Contain("\"retry_timeout\":3").And.Contain("\"synchronous_mode\":true");
        patch.Should().NotContain("postgresql");
    }

    // AAA (t09): конфиг, молча скорректированный Patroni 4.1 (наш ttl=5 → 20,
    // loop_wait остался 2) — патч минимальный (только loop_wait), ttl не мигает.
    [Fact]
    public void Regression_T09_Divergence_PatroniAdjustedConfig_MinimalPatch()
    {
        // Arrange — фактический /config из диагностики t09 (sampler3):
        // Patroni поднял ttl 5→20 и записал обратно в DCS
        const string config = """{"ttl":20,"loop_wait":2,"retry_timeout":3,"synchronous_mode":true}""";

        // Act: считаем патч конвергенции
        var patch = PatroniTimings.DivergencePatch(config);

        // Assert: расходится только loop_wait — патч минимальный
        patch.Should().Be("""{"loop_wait":1}""");
    }

    // AAA (t09): канонический конфиг — конвергентно, мутаций нет (патч не
    // должен писаться каждым тиком — не второй регулярный писатель).
    [Fact]
    public void Regression_T09_Divergence_CanonicalConfig_NoPatch()
    {
        // Arrange — конфиг ровно канонический
        const string config = """{"ttl":20,"loop_wait":1,"retry_timeout":3,"synchronous_mode":true}""";

        // Act: считаем патч конвергенции
        var patch = PatroniTimings.DivergencePatch(config);

        // Assert: расхождений нет — null
        patch.Should().BeNull();
    }

    // AAA (t09): пустой/битый/чужой ответ — конвергируем все поля (безопасный
    // исход: непонятный конфиг приводится к канону, а не оставляется как есть).
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("not-json{")]
    [InlineData("""{"foreign":"document"}""")]
    public void Regression_T09_Divergence_GarbageConfig_PatchAllCanonical(string? config)
    {
        // Arrange — отсутствие внятного конфига (null/пусто/битый JSON/чужой документ)

        // Act: считаем патч конвергенции
        var patch = PatroniTimings.DivergencePatch(config);

        // Assert: полный канон в патче
        patch.Should().NotBeNull();
        patch.Should().Contain("\"ttl\":20").And.Contain("\"loop_wait\":1")
            .And.Contain("\"retry_timeout\":3").And.Contain("\"synchronous_mode\":true");
    }

    // AAA (t09): SPILO_CONFIGURATION и конвергенция — из одного источника
    // (PatroniTimings): env ноды несёт ровно канонические значения.
    [Fact]
    public void Regression_T09_SpiloEnv_AndConvergence_SingleCanonicalSource()
    {
        // Arrange: топология шарда из одной ноды.
        var topology = new ShardTopology("shop", "shard1", "shop-shard1",
            new Dictionary<string, NodeAddress> { ["shard1a"] = new("h1", new NodePorts(15432, 18008, 16432)) });

        // Act: генерируем env и прогоняем канон через конвергенцию
        var spilo = SpiloEnvBuilder.Build(
            topology, new EtcdEndpoints(["http://e1:2379"]),
            new InstallSecrets("su", "sb", "adm", "mov"))["SPILO_CONFIGURATION"];
        var selfPatch = PatroniTimings.DivergencePatch(
            $$"""{"ttl":{{PatroniTimings.Ttl}},"loop_wait":{{PatroniTimings.LoopWait}},"retry_timeout":{{PatroniTimings.RetryTimeout}},"synchronous_mode":true}""");

        // Assert: env несёт канон; канон конвергентен сам себе (null-патч);
        // канон удовлетворяет полам и правилу Patroni 4.x
        spilo.Should().Contain($"ttl: {PatroniTimings.Ttl}")
            .And.Contain($"loop_wait: {PatroniTimings.LoopWait}")
            .And.Contain($"retry_timeout: {PatroniTimings.RetryTimeout}");
        selfPatch.Should().BeNull("канон конвергентен сам с собой");
        PatroniTimings.Ttl.Should().BeGreaterThanOrEqualTo(20, "пол Patroni 4.x: ttl≥20");
        PatroniTimings.LoopWait.Should().BeGreaterThanOrEqualTo(1);
        PatroniTimings.RetryTimeout.Should().BeGreaterThanOrEqualTo(3);
        (PatroniTimings.LoopWait + 2 * PatroniTimings.RetryTimeout)
            .Should().BeLessThanOrEqualTo(PatroniTimings.Ttl, "правило loop_wait+2*retry_timeout≤ttl");
    }
}
