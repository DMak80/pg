using PgWorker.Core.Model;
using PgWorker.Core.Templates;

namespace PgWorker.UnitTests.Templates;

// PatroniTimings (t09, arch/14 §2.1/§5 C): канон таймингов Patroni (полы
// Patroni 4.x: loop_wait≥1, retry_timeout≥3, ttl≥20). Построение патча
// конвергенции с t11 живёт в DcsConfigConvergence — кейсы патча см.
// DcsConfigConvergenceTests (кейсы Regression_T09_Divergence_* мигрированы).
public class PatroniTimingsTests
{
    // AAA (t09, дополнен t11): канон таймингов удовлетворяет полам Patroni 4.x;
    // канонический документ конвергентен сам себе на новом API (расширенный
    // self-check с параметрами — DcsConfigConvergenceTests).
    [Fact]
    public void Regression_T09_SpiloEnv_AndConvergence_SingleCanonicalSource()
    {
        // Arrange: топология шарда из одной ноды.
        var topology = new ShardTopology("shop", "shard1", "shop-shard1",
            new Dictionary<string, NodeAddress> { ["shard1a"] = new("h1", new NodePorts(15432, 18008, 16432)) });

        // Act: генерируем env и сверяем канон через DcsConfigConvergence.
        var spilo = SpiloEnvBuilder.Build(
            topology, new EtcdEndpoints(["http://e1:2379"]),
            new InstallSecrets("su", "sb", "adm", "mov"))["SPILO_CONFIGURATION"];
        var selfPatch = DcsConfigConvergence.DivergencePatch(
            $$"""{"ttl":{{PatroniTimings.Ttl}},"loop_wait":{{PatroniTimings.LoopWait}},"retry_timeout":{{PatroniTimings.RetryTimeout}},"synchronous_mode":true}""",
            null);

        // Assert: env несёт канон; канон конвергентен сам себе (null-патч);
        // полы и правило Patroni 4.x соблюдены.
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
