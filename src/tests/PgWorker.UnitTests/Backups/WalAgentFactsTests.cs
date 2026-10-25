using FluentAssertions;
using PgWorker.Backups;
using PgWorker.Etcd.Parsing;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Факты agents wal-ключа из статус-ключей приёмников (t25, arch/19 §4):
// WalStreamProcess.BuildAgentFacts — по нодам portalloc шарда: ключ жив (есть в
// Range) → Running, ключа нет → Absent (exited новыми записями не пишется —
// легаси-значение панельного парсера). Толерантный парсинг значения ключа —
// WalReceiverStatusReader.Parse (Backups не ссылается на WalReceiver).
public class WalAgentFactsTests
{
    private static WalReceiverAgentFacts Facts(string node, string state = "streaming") =>
        new(node, "master", state, false, null, null);

    [Fact]
    public void Build_FromKeys_RunningAndAbsent()
    {
        // Arrange — ключи живы у shard1a; у shard1b ключа нет.
        var facts = new[] { Facts("shard1a") };

        // Act — факты по нодам portalloc шарда.
        var agents = WalStreamProcess.BuildAgentFacts(["shard1a", "shard1b", "shard1c"], facts);

        // Assert — контракт §4: ключ жив → Running, нет → Absent.
        agents.Should().BeEquivalentTo(
        [
            new WalAgentState("shard1a", WalAgentPresence.Running),
            new WalAgentState("shard1b", WalAgentPresence.Absent),
            new WalAgentState("shard1c", WalAgentPresence.Absent),
        ]);
    }

    [Fact]
    public void Build_NoKeys_AllAbsent()
    {
        // Arrange/Act — Range пуст (приёмники не стартовали/умерли).
        var agents = WalStreamProcess.BuildAgentFacts(["shard1a", "shard1b"], null);

        // Assert — все absent (exited новыми записями не пишется).
        agents.Should().BeEquivalentTo(
        [
            new WalAgentState("shard1a", WalAgentPresence.Absent),
            new WalAgentState("shard1b", WalAgentPresence.Absent),
        ]);
    }

    // AAA: толерантный парсинг значения статус-ключа — полный формат, минимум,
    // чужой/битый JSON → null (пропуск записи, не исключение).
    [Fact]
    public void Parse_Tolerant_FullMinimalAndForeign()
    {
        // Arrange/Act/Assert — полный формат со всеми фактами
        var full = WalReceiverStatusReader.Parse(
            """{"node":"s1a","role":"master","state":"streaming","last_delivered_segment":"000000010000000000000002","last_delivered_unix":10,"pool_exhausted":true,"tli_jump":{"from":"000000010000000000000003","to":"000000020000000000000007","tli":2},"error":"boom","updated_unix":99}""");
        full.Should().NotBeNull();
        full!.Node.Should().Be("s1a");
        full.Role.Should().Be("master");
        full.State.Should().Be("streaming");
        full.PoolExhausted.Should().BeTrue();
        full.TliJump.Should().Be(new WalGapState(
            "000000010000000000000003", "000000020000000000000007", 2));
        full.Error.Should().Be("boom");

        // Минимум: только node/state (role/pool_exhausted — дефолты)
        var minimal = WalReceiverStatusReader.Parse("""{"node":"s1b","state":"waiting_role"}""");
        minimal.Should().NotBeNull();
        minimal!.Role.Should().BeNull();
        minimal.PoolExhausted.Should().BeFalse();
        minimal.TliJump.Should().BeNull();

        // Битое/чужое — null (пропуск записи, не исключение)
        WalReceiverStatusReader.Parse("{not json").Should().BeNull();
        WalReceiverStatusReader.Parse("[]").Should().BeNull();
        WalReceiverStatusReader.Parse("""{"role":"master"}""").Should().BeNull();
    }
}
