using FluentAssertions;
using PgWorker.WalReceiver;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// JSON-контракт статус-ключа агента /pgworker/backups/<C>/<X>/agent/<N>
// (arch/19 §4): ToJson — формат значения (имена полей snake_case, node
// по факту identity); Parse — полный round-trip всех полей + толерантность
// к чужому/битому JSON (null, не исключение).
public class WalReceiverAgentStatusJsonTests
{
    [Fact]
    public void ToJson_AllFields_PresentWithCanonicalNames()
    {
        // Arrange — статус со всеми фактами (tli_jump, history_missing_tli, error, прогресс)
        var status = new WalReceiverAgentStatus(
            WalReceiverRole.Master, WalReceiverState.Streaming,
            "000000010000000000000002", 1760000000L, true,
            new WalReceiverTliJump("000000010000000000000001", "000000020000000000000003", 2),
            3,
            "some error", 1760000100L);

        // Act
        var json = WalReceiverAgentStatusJson.ToJson("s1a", status);

        // Assert — канон arch/19 §4: node/role/state/факты; enum'ы — строками
        json.Should().Contain("\"node\":\"s1a\"");
        json.Should().Contain("\"role\":\"master\"");
        json.Should().Contain("\"state\":\"streaming\"");
        json.Should().Contain("\"last_delivered_segment\":\"000000010000000000000002\"");
        json.Should().Contain("\"last_delivered_unix\":1760000000");
        json.Should().Contain("\"pool_exhausted\":true");
        json.Should().Contain("\"tli_jump\":{\"from\":\"000000010000000000000001\"," +
                              "\"to\":\"000000020000000000000003\",\"tli\":2}");
        json.Should().Contain("\"history_missing_tli\":3");
        json.Should().Contain("\"error\":\"some error\"");
        json.Should().Contain("\"updated_unix\":1760000100");
    }

    [Fact]
    public void ToJson_NullableFields_Omitted()
    {
        // Arrange — минимальный статус без фактов
        var status = new WalReceiverAgentStatus(
            WalReceiverRole.None, WalReceiverState.WaitingRole,
            null, null, false, null, null, null, 100);

        // Act
        var json = WalReceiverAgentStatusJson.ToJson("s1b", status);

        // Assert — null-поля опущены (WhenWritingNull), флаги/времена на месте
        json.Should().Contain("\"node\":\"s1b\"");
        json.Should().Contain("\"role\":\"none\"");
        json.Should().Contain("\"state\":\"waiting_role\"");
        json.Should().Contain("\"pool_exhausted\":false");
        json.Should().Contain("\"updated_unix\":100");
        json.Should().NotContain("last_delivered_segment");
        json.Should().NotContain("last_delivered_unix");
        json.Should().NotContain("tli_jump");
        json.Should().NotContain("\"error\"");
    }

    [Fact]
    public void Parse_RoundTrip_AllFields()
    {
        // Arrange — полный статус
        var status = new WalReceiverAgentStatus(
            WalReceiverRole.Sync, WalReceiverState.FailedPermanent,
            "000000010000000000000005", 42, true,
            new WalReceiverTliJump("a", "b", 7),
            5,
            "boom", 99);

        // Act — ToJson → Parse (node из значения ключа)
        var parsed = WalReceiverAgentStatusJson.Parse(
            WalReceiverAgentStatusJson.ToJson("s2a", status));

        // Assert — все поля восстановлены 1:1
        parsed.Should().NotBeNull();
        parsed!.Value.Node.Should().Be("s2a");
        var restored = parsed.Value.Status;
        restored.Role.Should().Be(WalReceiverRole.Sync);
        restored.State.Should().Be(WalReceiverState.FailedPermanent);
        restored.LastDeliveredSegment.Should().Be("000000010000000000000005");
        restored.LastDeliveredUnix.Should().Be(42);
        restored.PoolExhausted.Should().BeTrue();
        restored.TliJump.Should().NotBeNull();
        restored.TliJump!.From.Should().Be("a");
        restored.TliJump.To.Should().Be("b");
        restored.TliJump.Tli.Should().Be(7u);
        restored.HistoryMissingTli.Should().Be(5u);
        restored.Error.Should().Be("boom");
        restored.UpdatedUnix.Should().Be(99);
    }

    [Fact]
    public void Parse_PartialValue_DefaultsTolerant()
    {
        // Arrange — минимальное значение (только node/state) — живой ключ может
        // писаться разными версиями
        const string json = """{"node":"s3a","state":"waiting_password"}""";

        // Act
        var parsed = WalReceiverAgentStatusJson.Parse(json);

        // Assert — дефолты: role=none, флаги false, факты null
        parsed.Should().NotBeNull();
        parsed!.Value.Node.Should().Be("s3a");
        parsed.Value.Status.State.Should().Be(WalReceiverState.WaitingPassword);
        parsed.Value.Status.Role.Should().Be(WalReceiverRole.None);
        parsed.Value.Status.PoolExhausted.Should().BeFalse();
        parsed.Value.Status.TliJump.Should().BeNull();
    }

    [Fact]
    public void Parse_ForeignOrBrokenJson_ReturnsNull()
    {
        // Arrange — чужое/битое значение ключа
        const string foreign = """{"node":"s4a","state":"totally_unknown_state"}""";

        // Act/Assert — незнакомое state → FailedPermanent (не исключение)
        var foreignParsed = WalReceiverAgentStatusJson.Parse(foreign);
        foreignParsed.Should().NotBeNull();
        foreignParsed!.Value.Status.State.Should().Be(WalReceiverState.FailedPermanent);

        // Act/Assert — битый JSON → null (чужое значение, не парсится)
        WalReceiverAgentStatusJson.Parse("{not json").Should().BeNull();
        WalReceiverAgentStatusJson.Parse("[]").Should().BeNull();
    }
}
